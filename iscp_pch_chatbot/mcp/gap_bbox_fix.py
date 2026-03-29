import re
from pathlib import Path
from typing import List, Tuple

from autobots_sdk.base.executors.synopsys.fusion_compiler import fusion_compiler

BBox = Tuple[float, float, float, float]

_LAYER_NAMES_CACHE: List[str] | None = None
_VIA_LAYER_CACHE: dict[str, Tuple[str, str]] = {}


def _run_fc_to_text(query: str, log_name: str) -> str:
    log_dir = Path("/nfs/site/disks/zsc16_ttlpcd_00114/pard2dide/kgoh14/ai_test/WW09.5_copilot/tmp")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_name
    fusion_compiler(query=query, log_path=str(log_path))
    return log_path.read_text(errors="ignore")


def _parse_bboxes(text: str) -> List[BBox]:
    matches = re.findall(r"\{\{([^{}]+)\}\s+\{([^{}]+)\}\}", text)
    bboxes: List[BBox] = []
    for ll, ur in matches:
        ll_parts = ll.split()
        ur_parts = ur.split()
        if len(ll_parts) < 2 or len(ur_parts) < 2:
            continue
        bboxes.append((float(ll_parts[0]), float(ll_parts[1]), float(ur_parts[0]), float(ur_parts[1])))
    return bboxes


def _parse_gap_bboxes(spec: str) -> List[BBox]:
    segments = [segment.strip() for segment in spec.split(";") if segment.strip()]
    result: List[BBox] = []
    for segment in segments:
        nums = re.findall(r"[-+]?\d*\.\d+|\d+", segment)
        if len(nums) < 4:
            continue
        x1, y1, x2, y2 = map(float, nums[:4])
        llx, urx = (x1, x2) if x1 <= x2 else (x2, x1)
        lly, ury = (y1, y2) if y1 <= y2 else (y2, y1)
        result.append((llx, lly, urx, ury))
    return result


def _overlap(a: BBox, b: BBox) -> bool:
    ax1, ay1, ax2, ay2 = a
    bx1, by1, bx2, by2 = b
    return not (ax2 < bx1 or bx2 < ax1 or ay2 < by1 or by2 < ay1)


def _expand(bb: BBox, margin: float) -> BBox:
    x1, y1, x2, y2 = bb
    return (x1 - margin, y1 - margin, x2 + margin, y2 + margin)


def _center(bb: BBox) -> Tuple[float, float]:
    x1, y1, x2, y2 = bb
    return ((x1 + x2) / 2.0, (y1 + y2) / 2.0)


def _overlap_1d(a1: float, a2: float, b1: float, b2: float) -> bool:
    lo = max(min(a1, a2), min(b1, b2))
    hi = min(max(a1, a2), max(b1, b2))
    return hi > lo


def _reason_rank(reason: str | None) -> int:
    if reason is None:
        return 0
    if "fill" in reason.lower():
        return 1
    return 2


def _pick_x_pair(
    candidates: List[Tuple[str, BBox]],
    gap: BBox,
    gcx: float,
    reason_cache: dict[str, str | None],
    prefer_non_fill: bool,
) -> Tuple[Tuple[str, BBox] | None, Tuple[str, BBox] | None]:
    gx1, gy1, gx2, gy2 = gap

    left_side = [
        (name, bb)
        for name, bb in candidates
        if _center(bb)[0] <= gcx and _overlap_1d(bb[1], bb[3], gy1, gy2)
    ]
    right_side = [
        (name, bb)
        for name, bb in candidates
        if _center(bb)[0] >= gcx and _overlap_1d(bb[1], bb[3], gy1, gy2)
    ]

    pair_scores = []
    for left in left_side:
        for right in right_side:
            if left[0] == right[0]:
                continue
            if not _overlap_1d(left[1][1], left[1][3], right[1][1], right[1][3]):
                continue
            left_reason = reason_cache.get(left[0])
            right_reason = reason_cache.get(right[0])
            left_rank = _reason_rank(left_reason)
            right_rank = _reason_rank(right_reason)
            fill_count = int(left_rank == 1) + int(right_rank == 1)
            penalty = left_rank + right_rank
            edge_dist = abs(gx1 - left[1][2]) + abs(right[1][0] - gx2)
            center_dist = abs(_center(left[1])[0] - gcx) + abs(_center(right[1])[0] - gcx)
            if prefer_non_fill:
                score = (penalty, fill_count, edge_dist, center_dist)
            else:
                score = (edge_dist, center_dist, penalty, fill_count)
            pair_scores.append((score, left, right))

    if pair_scores:
        pair_scores.sort(key=lambda item: item[0])
        _, best_left, best_right = pair_scores[0]
        return best_left, best_right

    relaxed_left = [
        (name, bb)
        for name, bb in candidates
        if bb[0] <= gx1 + 1e-9 and _overlap_1d(bb[1], bb[3], gy1, gy2)
    ]
    relaxed_right = [
        (name, bb)
        for name, bb in candidates
        if bb[2] >= gx2 - 1e-9 and _overlap_1d(bb[1], bb[3], gy1, gy2)
    ]

    if not relaxed_left:
        relaxed_left = [
            (name, bb)
            for name, bb in candidates
            if _overlap_1d(bb[1], bb[3], gy1, gy2)
        ]
    if not relaxed_right:
        relaxed_right = [
            (name, bb)
            for name, bb in candidates
            if _overlap_1d(bb[1], bb[3], gy1, gy2)
        ]

    def _left_score(item: Tuple[str, BBox]) -> tuple:
        name, bb = item
        rank = _reason_rank(reason_cache.get(name))
        fill_count = int(rank == 1)
        edge_dist = abs(gx1 - bb[2])
        center_dist = abs(_center(bb)[0] - gcx)
        if prefer_non_fill:
            return (rank, fill_count, edge_dist, center_dist)
        return (edge_dist, center_dist, rank, fill_count)

    def _right_score(item: Tuple[str, BBox]) -> tuple:
        name, bb = item
        rank = _reason_rank(reason_cache.get(name))
        fill_count = int(rank == 1)
        edge_dist = abs(bb[0] - gx2)
        center_dist = abs(_center(bb)[0] - gcx)
        if prefer_non_fill:
            return (rank, fill_count, edge_dist, center_dist)
        return (edge_dist, center_dist, rank, fill_count)

    if relaxed_left and relaxed_right:
        left_sorted = sorted(relaxed_left, key=_left_score)
        right_sorted = sorted(relaxed_right, key=_right_score)
        for left in left_sorted:
            for right in right_sorted:
                if left[0] != right[0]:
                    return left, right

    return None, None


def _pick_y_pair(
    candidates: List[Tuple[str, BBox]],
    gap: BBox,
    gcy: float,
    reason_cache: dict[str, str | None],
    prefer_non_fill: bool,
) -> Tuple[Tuple[str, BBox] | None, Tuple[str, BBox] | None]:
    gx1, gy1, gx2, gy2 = gap

    low_side = [
        (name, bb)
        for name, bb in candidates
        if _center(bb)[1] <= gcy and _overlap_1d(bb[0], bb[2], gx1, gx2)
    ]
    high_side = [
        (name, bb)
        for name, bb in candidates
        if _center(bb)[1] >= gcy and _overlap_1d(bb[0], bb[2], gx1, gx2)
    ]

    pair_scores = []
    for low in low_side:
        for high in high_side:
            if low[0] == high[0]:
                continue
            if not _overlap_1d(low[1][0], low[1][2], high[1][0], high[1][2]):
                continue
            low_reason = reason_cache.get(low[0])
            high_reason = reason_cache.get(high[0])
            low_rank = _reason_rank(low_reason)
            high_rank = _reason_rank(high_reason)
            fill_count = int(low_rank == 1) + int(high_rank == 1)
            penalty = low_rank + high_rank
            edge_dist = abs(gy1 - low[1][3]) + abs(high[1][1] - gy2)
            center_dist = abs(_center(low[1])[1] - gcy) + abs(_center(high[1])[1] - gcy)
            if prefer_non_fill:
                score = (penalty, fill_count, edge_dist, center_dist)
            else:
                score = (edge_dist, center_dist, penalty, fill_count)
            pair_scores.append((score, low, high))

    if pair_scores:
        pair_scores.sort(key=lambda item: item[0])
        _, best_low, best_high = pair_scores[0]
        return best_low, best_high

    relaxed_low = [
        (name, bb)
        for name, bb in candidates
        if bb[1] <= gy1 + 1e-9 and _overlap_1d(bb[0], bb[2], gx1, gx2)
    ]
    relaxed_high = [
        (name, bb)
        for name, bb in candidates
        if bb[3] >= gy2 - 1e-9 and _overlap_1d(bb[0], bb[2], gx1, gx2)
    ]

    if not relaxed_low:
        relaxed_low = [
            (name, bb)
            for name, bb in candidates
            if _overlap_1d(bb[0], bb[2], gx1, gx2)
        ]
    if not relaxed_high:
        relaxed_high = [
            (name, bb)
            for name, bb in candidates
            if _overlap_1d(bb[0], bb[2], gx1, gx2)
        ]

    def _low_score(item: Tuple[str, BBox]) -> tuple:
        name, bb = item
        rank = _reason_rank(reason_cache.get(name))
        fill_count = int(rank == 1)
        edge_dist = abs(gy1 - bb[3])
        center_dist = abs(_center(bb)[1] - gcy)
        if prefer_non_fill:
            return (rank, fill_count, edge_dist, center_dist)
        return (edge_dist, center_dist, rank, fill_count)

    def _high_score(item: Tuple[str, BBox]) -> tuple:
        name, bb = item
        rank = _reason_rank(reason_cache.get(name))
        fill_count = int(rank == 1)
        edge_dist = abs(bb[1] - gy2)
        center_dist = abs(_center(bb)[1] - gcy)
        if prefer_non_fill:
            return (rank, fill_count, edge_dist, center_dist)
        return (edge_dist, center_dist, rank, fill_count)

    if relaxed_low and relaxed_high:
        low_sorted = sorted(relaxed_low, key=_low_score)
        high_sorted = sorted(relaxed_high, key=_high_score)
        for low in low_sorted:
            for high in high_sorted:
                if low[0] != high[0]:
                    return low, high

    return None, None


def _bbox_text(bb: BBox) -> str:
    x1, y1, x2, y2 = bb
    return f"{{{{{x1:.6f} {y1:.6f}}} {{{x2:.6f} {y2:.6f}}}}}"


def _shape_bbox(shape_name: str) -> str:
    text = _run_fc_to_text(
        query=f"get_attribute [get_shapes -quiet {shape_name}] bbox",
        log_name="fc_gap_fix_shape_bbox.txt",
    )
    parsed = _parse_bboxes(text)
    if not parsed:
        return "<NA>"
    return _bbox_text(parsed[0])


def _shape_attr_text(shape_name: str, attr: str) -> str:
    text = _run_fc_to_text(
        query=f"get_attribute [get_shapes -quiet {shape_name}] {attr}",
        log_name=f"fc_gap_fix_{shape_name}_{attr}.txt",
    )
    return text.strip()


def _via_attr_text(via_name: str, attr: str) -> str:
    text = _run_fc_to_text(
        query=f"get_attribute [get_vias -quiet {via_name}] {attr}",
        log_name=f"fc_gap_fix_{via_name}_{attr}.txt",
    )
    return text.strip()


def _via_connected_to_layer(via_name: str, metal_layer: str) -> bool:
    via_key = via_name.strip()
    if via_key not in _VIA_LAYER_CACHE:
        lower = _via_attr_text(via_key, "lower_layer_name").strip().lower()
        upper = _via_attr_text(via_key, "upper_layer_name").strip().lower()
        _VIA_LAYER_CACHE[via_key] = (lower, upper)

    lower, upper = _VIA_LAYER_CACHE[via_key]
    target = metal_layer.strip().lower()
    return lower == target or upper == target


def _edit_block_reason(shape_name: str) -> str | None:
    shape_use = _shape_attr_text(shape_name, "shape_use").lower()
    if "fill" in shape_use:
        return "shape_use=fill"
    if "pin" in shape_use:
        return f"shape_use={shape_use}"

    is_pin = _shape_attr_text(shape_name, "is_pin").lower()
    if re.search(r"\b(true|1|yes)\b", is_pin):
        return "is_pin=true"

    term_type = _shape_attr_text(shape_name, "term_type").lower()
    if "pin" in term_type:
        return f"term_type={term_type}"

    owner_type = _shape_attr_text(shape_name, "owner_type").lower()
    if "macro" in owner_type and "pin" in owner_type:
        return f"owner_type={owner_type}"

    return None


def _is_fill_reason(reason: str | None) -> bool:
    if not reason:
        return False
    return "fill" in reason.lower()


def _get_layer_names() -> List[str]:
    global _LAYER_NAMES_CACHE
    if _LAYER_NAMES_CACHE is not None:
        return _LAYER_NAMES_CACHE

    text = _run_fc_to_text(
        query="get_object_name [get_layers -quiet *]",
        log_name="fc_gap_fix_layer_list.txt",
    )
    _LAYER_NAMES_CACHE = [token.strip() for token in text.split() if token.strip()]
    return _LAYER_NAMES_CACHE


def _metal_index(layer_name: str) -> int | None:
    match = re.search(r"m(\d+)", layer_name.strip().lower())
    if not match:
        return None
    return int(match.group(1))


def _resolve_adjacent_via_layers(metal_layer: str) -> List[str]:
    m_idx = _metal_index(metal_layer)
    if m_idx is None:
        return []

    candidate_indices = [m_idx - 1, m_idx]
    all_layers = _get_layer_names()
    resolved: List[str] = []

    for via_idx in candidate_indices:
        if via_idx <= 0:
            continue
        patterns = [
            re.compile(rf"^via0*{via_idx}$", flags=re.IGNORECASE),
            re.compile(rf"^v0*{via_idx}$", flags=re.IGNORECASE),
            re.compile(rf"^cut0*{via_idx}$", flags=re.IGNORECASE),
            re.compile(rf"^c0*{via_idx}$", flags=re.IGNORECASE),
        ]
        for name in all_layers:
            if any(pattern.match(name) for pattern in patterns):
                resolved.append(name)

    # de-duplicate preserve order
    out: List[str] = []
    seen = set()
    for name in resolved:
        if name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def _local_edge_probe_bbox(
    shape_bb: BBox,
    gap_bb: BBox,
    axis: str,
    side: str,
    inward_um: float = 0.03,
) -> BBox:
    sx1, sy1, sx2, sy2 = shape_bb
    gx1, gy1, gx2, gy2 = gap_bb
    eps = 0.003

    inward = max(0.0, inward_um)

    if axis == "x":
        gcy = (gy1 + gy2) / 2.0
        local_half_span = max(0.005, min((abs(gy2 - gy1) / 2.0) + 0.002, 0.02))
        oy1 = max(min(sy1, sy2), gcy - local_half_span)
        oy2 = min(max(sy1, sy2), gcy + local_half_span)
        if oy2 <= oy1:
            oy1 = max(sy1, gy1)
            oy2 = min(sy2, gy2)
        if oy2 <= oy1:
            oy1 = min(sy1, sy2)
            oy2 = max(sy1, sy2)
        if side == "left":
            x1 = sx2 - inward - eps
            x2 = sx2 + eps
        else:
            x1 = sx1 - eps
            x2 = sx1 + inward + eps
        return (x1, oy1 - eps, x2, oy2 + eps)

    gcx = (gx1 + gx2) / 2.0
    local_half_span = max(0.005, min((abs(gx2 - gx1) / 2.0) + 0.002, 0.02))
    ox1 = max(min(sx1, sx2), gcx - local_half_span)
    ox2 = min(max(sx1, sx2), gcx + local_half_span)
    if ox2 <= ox1:
        ox1 = max(sx1, gx1)
        ox2 = min(sx2, gx2)
    if ox2 <= ox1:
        ox1 = min(sx1, sx2)
        ox2 = max(sx1, sx2)
    if side == "low":
        y1 = sy2 - inward - eps
        y2 = sy2 + eps
    else:
        y1 = sy1 - eps
        y2 = sy1 + inward + eps
    return (ox1 - eps, y1, ox2 + eps, y2)


def _has_local_via_on_side(
    shape_bb: BBox,
    gap_bb: BBox,
    axis: str,
    side: str,
    metal_layer: str,
    via_layers: List[str],
) -> bool:
    px1, py1, px2, py2 = _local_edge_probe_bbox(shape_bb, gap_bb, axis, side)
    text = _run_fc_to_text(
        query=(
            f"get_object_name [get_vias -quiet "
            f"-filter {{bbox_llx<={px2:.6f} && bbox_urx>={px1:.6f} && "
            f"bbox_lly<={py2:.6f} && bbox_ury>={py1:.6f}}}]"
        ),
        log_name="fc_gap_fix_via_probe.txt",
    )

    via_names = re.findall(r"\bVIA_[A-Za-z0-9_./-]+\b", text, flags=re.IGNORECASE)
    for via_name in via_names:
        if _via_connected_to_layer(via_name, metal_layer):
            return True

    return False


def _nearest_non_fill_x(
    candidates: List[Tuple[str, BBox]],
    reason_cache: dict[str, str | None],
    gap: BBox,
    gcx: float,
    side: str,
    exclude_name: str,
) -> Tuple[str, BBox] | None:
    gx1, gy1, gx2, gy2 = gap
    pool: List[Tuple[str, BBox]] = []
    for name, bb in candidates:
        if name == exclude_name:
            continue
        reason = reason_cache.get(name)
        if reason is not None:
            continue
        if not _overlap_1d(bb[1], bb[3], gy1, gy2):
            continue
        cx, _ = _center(bb)
        if side == "left" and cx <= gcx:
            pool.append((name, bb))
        if side == "right" and cx >= gcx:
            pool.append((name, bb))
    if not pool:
        return None
    if side == "left":
        return min(pool, key=lambda item: abs(gx1 - item[1][2]))
    return min(pool, key=lambda item: abs(item[1][0] - gx2))


def _nearest_non_fill_y(
    candidates: List[Tuple[str, BBox]],
    reason_cache: dict[str, str | None],
    gap: BBox,
    gcy: float,
    side: str,
    exclude_name: str,
) -> Tuple[str, BBox] | None:
    gx1, gy1, gx2, gy2 = gap
    pool: List[Tuple[str, BBox]] = []
    for name, bb in candidates:
        if name == exclude_name:
            continue
        reason = reason_cache.get(name)
        if reason is not None:
            continue
        if not _overlap_1d(bb[0], bb[2], gx1, gx2):
            continue
        _, cy = _center(bb)
        if side == "low" and cy <= gcy:
            pool.append((name, bb))
        if side == "high" and cy >= gcy:
            pool.append((name, bb))
    if not pool:
        return None
    if side == "low":
        return min(pool, key=lambda item: abs(gy1 - item[1][3]))
    return min(pool, key=lambda item: abs(item[1][1] - gy2))


def fix_gap_bbox_spacing_impl(
    layer: str,
    target_gap_um: float,
    search_margin_um: float,
    gap_bboxes: str,
    dry_run: bool = True,
    prefer_non_fill: bool = True,
) -> str:
    parsed_gaps = _parse_gap_bboxes(gap_bboxes)
    if not parsed_gaps:
        return "No valid gap bboxes parsed. Use format: x1 y1 x2 y2; x1 y1 x2 y2"

    names_text = _run_fc_to_text(
        query=(
            f"get_object_name [get_shapes -quiet -hierarchical -include_fill "
            f"-filter {{layer_name=={layer}}}]"
        ),
        log_name="fc_gap_fix_layer_names.txt",
    )
    bbox_text = _run_fc_to_text(
        query=(
            f"get_attribute [get_shapes -quiet -hierarchical -include_fill "
            f"-filter {{layer_name=={layer}}}] bbox"
        ),
        log_name="fc_gap_fix_layer_bboxes.txt",
    )

    shape_names = [token for token in names_text.split() if token.strip()]
    shape_bboxes = _parse_bboxes(bbox_text)
    count = min(len(shape_names), len(shape_bboxes))
    shapes = list(zip(shape_names[:count], shape_bboxes[:count]))

    if not shapes:
        return f"No shapes found on layer {layer}."

    lines: List[str] = []
    via_layers = _resolve_adjacent_via_layers(layer)
    lines.append(
        f"layer={layer} target_gap_um={target_gap_um} search_margin_um={search_margin_um} "
        f"dry_run={dry_run} prefer_non_fill={prefer_non_fill}"
    )
    lines.append("adjacent_via_layers=" + (", ".join(via_layers) if via_layers else "<none>"))
    lines.append(f"layer_shape_count={len(shapes)}")

    for gap in parsed_gaps:
        gx1, gy1, gx2, gy2 = gap
        dx = gx2 - gx1
        dy = gy2 - gy1
        axis = "x" if dx <= dy else "y"
        curr_gap = dx if axis == "x" else dy
        need = target_gap_um - curr_gap

        lines.append(f"\nGAP={_bbox_text(gap)} axis={axis} current={curr_gap:.6f} need={need:.6f}")
        if need <= 0:
            lines.append("SKIP current gap already >= target")
            continue

        search_box = _expand(gap, search_margin_um)
        gcx, gcy = _center(gap)
        candidates = [(name, bb) for name, bb in shapes if _overlap(bb, search_box)]
        lines.append(f"search_bbox={_bbox_text(search_box)} candidates={len(candidates)}")

        if not candidates:
            lines.append("FAIL no overlapping shapes found")
            continue

        reason_cache: dict[str, str | None] = {}
        for name, _ in candidates:
            reason_cache[name] = _edit_block_reason(name)

        def ensure_reasons(pool: List[Tuple[str, BBox]]) -> None:
            for shape_name, _ in pool:
                if shape_name not in reason_cache:
                    reason_cache[shape_name] = _edit_block_reason(shape_name)

        half = need / 2.0

        if axis == "x":
            left, right = _pick_x_pair(candidates, gap, gcx, reason_cache, prefer_non_fill)
            if not left or not right:
                fallback_pool = [
                    (name, bb)
                    for name, bb in candidates
                    if _overlap_1d(bb[1], bb[3], gy1, gy2)
                ]

                def _cand_rank(shape_name: str) -> tuple[int, int]:
                    reason = reason_cache.get(shape_name)
                    rank = _reason_rank(reason)
                    return rank, int(rank == 1)

                left_edges = sorted(
                    fallback_pool,
                    key=lambda item: (
                        abs(item[1][2] - gx1),
                        *_cand_rank(item[0]),
                        abs(_center(item[1])[0] - gcx),
                    ),
                )
                right_edges = sorted(
                    fallback_pool,
                    key=lambda item: (
                        abs(item[1][0] - gx2),
                        *_cand_rank(item[0]),
                        abs(_center(item[1])[0] - gcx),
                    ),
                )

                edge_tol = max(0.005, search_margin_um)
                left_candidate = next(
                    (item for item in left_edges if abs(item[1][2] - gx1) <= edge_tol),
                    left_edges[0] if left_edges else None,
                )
                right_candidate = next(
                    (item for item in right_edges if abs(item[1][0] - gx2) <= edge_tol),
                    right_edges[0] if right_edges else None,
                )

                if left_candidate and right_candidate and left_candidate[0] != right_candidate[0]:
                    lines.append("FAIL cannot identify left/right pair")
                    continue

                fallback_side = "left" if left_candidate else ("right" if right_candidate else "")
                fallback_item = left_candidate if left_candidate else right_candidate
                if not fallback_item:
                    lines.append("FAIL cannot identify left/right pair")
                    continue

                fb_name, fb_bb = fallback_item
                fb_block = reason_cache.get(fb_name)
                fb_fill = _is_fill_reason(fb_block)
                fb_pin_like = bool(fb_block and not fb_fill)
                fb_has_via = _has_local_via_on_side(
                    fb_bb,
                    gap,
                    "x",
                    "left" if fallback_side == "left" else "right",
                    layer,
                    via_layers,
                )
                fb_locked = fb_fill or fb_has_via or fb_pin_like

                lines.append("PAIR_FALLBACK=edge_one_side")
                if fallback_side == "left":
                    lines.append(f"LEFT={fb_name} bbox={_bbox_text(fb_bb)}")
                    lines.append("RIGHT=<PIN_OR_UNAVAILABLE>")
                else:
                    lines.append("LEFT=<PIN_OR_UNAVAILABLE>")
                    lines.append(f"RIGHT={fb_name} bbox={_bbox_text(fb_bb)}")

                if fb_locked:
                    lines.append(
                        "ERROR fallback edit blocked: "
                        + f"{fallback_side.upper()}(fill={int(fb_fill)},via={int(fb_has_via)},pin={int(fb_pin_like)})"
                    )
                    continue

                if not dry_run:
                    if fallback_side == "left":
                        lines.append("MODE=one_side LEFT_ONLY (EDGE_FALLBACK)")
                        fusion_compiler(
                            query=(
                                f"resize_objects -delta {{ {{0 0}} {{{-need:.6f} 0}} }} "
                                f"[get_shapes -quiet {fb_name}]"
                            )
                        )
                        lines.append(f"LEFT_AFTER={_shape_bbox(fb_name)}")
                        lines.append("RIGHT_AFTER=<NA>")
                    else:
                        lines.append("MODE=one_side RIGHT_ONLY (EDGE_FALLBACK)")
                        fusion_compiler(
                            query=(
                                f"resize_objects -delta {{ {{{need:.6f} 0}} {{0 0}} }} "
                                f"[get_shapes -quiet {fb_name}]"
                            )
                        )
                        lines.append("LEFT_AFTER=<NA>")
                        lines.append(f"RIGHT_AFTER={_shape_bbox(fb_name)}")
                continue
            left_name, left_bb = left
            right_name, right_bb = right
            orig_left_name, orig_left_bb = left_name, left_bb
            orig_right_name, orig_right_bb = right_name, right_bb

            left_block = reason_cache.get(left_name)
            right_block = reason_cache.get(right_name)
            orig_left_block, orig_right_block = left_block, right_block
            left_fill = _is_fill_reason(left_block)
            right_fill = _is_fill_reason(right_block)
            orig_has_fill = left_fill or right_fill

            if prefer_non_fill and left_fill:
                replacement = _nearest_non_fill_x(candidates, reason_cache, gap, gcx, "left", right_name)
                if not replacement:
                    expanded_box = _expand(gap, max(search_margin_um * 5.0, search_margin_um + 0.5))
                    expanded_pool = [(name, bb) for name, bb in shapes if _overlap(bb, expanded_box)]
                    ensure_reasons(expanded_pool)
                    replacement = _nearest_non_fill_x(expanded_pool, reason_cache, gap, gcx, "left", right_name)
                if replacement:
                    left_name, left_bb = replacement
                    left_block = reason_cache.get(left_name)
                    left_fill = _is_fill_reason(left_block)
                    lines.append(f"REPLACE_LEFT_NON_FILL={left_name}")

            if prefer_non_fill and right_fill:
                replacement = _nearest_non_fill_x(candidates, reason_cache, gap, gcx, "right", left_name)
                if not replacement:
                    expanded_box = _expand(gap, max(search_margin_um * 5.0, search_margin_um + 0.5))
                    expanded_pool = [(name, bb) for name, bb in shapes if _overlap(bb, expanded_box)]
                    ensure_reasons(expanded_pool)
                    replacement = _nearest_non_fill_x(expanded_pool, reason_cache, gap, gcx, "right", left_name)
                if replacement:
                    right_name, right_bb = replacement
                    right_block = reason_cache.get(right_name)
                    right_fill = _is_fill_reason(right_block)
                    lines.append(f"REPLACE_RIGHT_NON_FILL={right_name}")

            lines.append(f"LEFT={left_name} bbox={_bbox_text(left_bb)}")
            lines.append(f"RIGHT={right_name} bbox={_bbox_text(right_bb)}")

            pair_gap = right_bb[0] - left_bb[2]
            pair_need = target_gap_um - pair_gap
            if pair_need <= 0 and orig_has_fill:
                left_name, left_bb = orig_left_name, orig_left_bb
                right_name, right_bb = orig_right_name, orig_right_bb
                left_block, right_block = orig_left_block, orig_right_block
                left_fill = _is_fill_reason(left_block)
                right_fill = _is_fill_reason(right_block)
                pair_gap = right_bb[0] - left_bb[2]
                pair_need = target_gap_um - pair_gap
                lines.append("REVERT_TO_ORIGINAL_PAIR=1")
            lines.append(f"PAIR_GAP={pair_gap:.6f} PAIR_NEED={pair_need:.6f}")
            if pair_need <= 0:
                lines.append("SKIP selected pair already >= target")
                continue

            half = pair_need / 2.0

            left_pin_like = bool(left_block and not left_fill)
            right_pin_like = bool(right_block and not right_fill)

            left_has_via = _has_local_via_on_side(left_bb, gap, "x", "left", layer, via_layers)
            right_has_via = _has_local_via_on_side(right_bb, gap, "x", "right", layer, via_layers)
            lines.append(f"LEFT_HAS_VIA={int(left_has_via)} RIGHT_HAS_VIA={int(right_has_via)}")

            left_locked = left_fill or left_has_via or left_pin_like
            right_locked = right_fill or right_has_via or right_pin_like

            if left_locked and right_locked:
                lines.append(
                    "ERROR edit blocked because both sides are locked: "
                    + f"LEFT(fill={int(left_fill)},via={int(left_has_via)},pin={int(left_pin_like)}), "
                    + f"RIGHT(fill={int(right_fill)},via={int(right_has_via)},pin={int(right_pin_like)})"
                )
                continue

            mode = "two_side"
            mode_reason = ""
            if left_locked and not right_locked:
                mode = "right_only"
                mode_reason = "LEFT locked"
            elif right_locked and not left_locked:
                mode = "left_only"
                mode_reason = "RIGHT locked"

            if not dry_run:
                if mode == "right_only":
                    one_side_need = max(pair_need, need)
                    lines.append(f"MODE=one_side RIGHT_ONLY ({mode_reason})")
                    fusion_compiler(
                        query=(
                            f"resize_objects -delta {{ {{{one_side_need:.6f} 0}} {{0 0}} }} "
                            f"[get_shapes -quiet {right_name}]"
                        )
                    )
                elif mode == "left_only":
                    one_side_need = max(pair_need, need)
                    lines.append(f"MODE=one_side LEFT_ONLY ({mode_reason})")
                    fusion_compiler(
                        query=(
                            f"resize_objects -delta {{ {{0 0}} {{{-one_side_need:.6f} 0}} }} "
                            f"[get_shapes -quiet {left_name}]"
                        )
                    )
                else:
                    lines.append("MODE=two_side SYMMETRIC")
                    fusion_compiler(
                        query=(
                            f"resize_objects -delta {{ {{0 0}} {{{-half:.6f} 0}} }} "
                            f"[get_shapes -quiet {left_name}]"
                        )
                    )
                    fusion_compiler(
                        query=(
                            f"resize_objects -delta {{ {{{half:.6f} 0}} {{0 0}} }} "
                            f"[get_shapes -quiet {right_name}]"
                        )
                    )
                lines.append(f"LEFT_AFTER={_shape_bbox(left_name)}")
                lines.append(f"RIGHT_AFTER={_shape_bbox(right_name)}")
        else:
            low, high = _pick_y_pair(candidates, gap, gcy, reason_cache, prefer_non_fill)
            if not low or not high:
                lines.append("FAIL cannot identify low/high pair")
                continue
            low_name, low_bb = low
            high_name, high_bb = high
            orig_low_name, orig_low_bb = low_name, low_bb
            orig_high_name, orig_high_bb = high_name, high_bb

            low_block = reason_cache.get(low_name)
            high_block = reason_cache.get(high_name)
            orig_low_block, orig_high_block = low_block, high_block
            low_fill = _is_fill_reason(low_block)
            high_fill = _is_fill_reason(high_block)
            orig_has_fill = low_fill or high_fill

            if prefer_non_fill and low_fill:
                replacement = _nearest_non_fill_y(candidates, reason_cache, gap, gcy, "low", high_name)
                if not replacement:
                    expanded_box = _expand(gap, max(search_margin_um * 5.0, search_margin_um + 0.5))
                    expanded_pool = [(name, bb) for name, bb in shapes if _overlap(bb, expanded_box)]
                    ensure_reasons(expanded_pool)
                    replacement = _nearest_non_fill_y(expanded_pool, reason_cache, gap, gcy, "low", high_name)
                if replacement:
                    low_name, low_bb = replacement
                    low_block = reason_cache.get(low_name)
                    low_fill = _is_fill_reason(low_block)
                    lines.append(f"REPLACE_LOW_NON_FILL={low_name}")

            if prefer_non_fill and high_fill:
                replacement = _nearest_non_fill_y(candidates, reason_cache, gap, gcy, "high", low_name)
                if not replacement:
                    expanded_box = _expand(gap, max(search_margin_um * 5.0, search_margin_um + 0.5))
                    expanded_pool = [(name, bb) for name, bb in shapes if _overlap(bb, expanded_box)]
                    ensure_reasons(expanded_pool)
                    replacement = _nearest_non_fill_y(expanded_pool, reason_cache, gap, gcy, "high", low_name)
                if replacement:
                    high_name, high_bb = replacement
                    high_block = reason_cache.get(high_name)
                    high_fill = _is_fill_reason(high_block)
                    lines.append(f"REPLACE_HIGH_NON_FILL={high_name}")

            lines.append(f"LOW={low_name} bbox={_bbox_text(low_bb)}")
            lines.append(f"HIGH={high_name} bbox={_bbox_text(high_bb)}")

            pair_gap = high_bb[1] - low_bb[3]
            pair_need = target_gap_um - pair_gap
            if pair_need <= 0 and orig_has_fill:
                low_name, low_bb = orig_low_name, orig_low_bb
                high_name, high_bb = orig_high_name, orig_high_bb
                low_block, high_block = orig_low_block, orig_high_block
                low_fill = _is_fill_reason(low_block)
                high_fill = _is_fill_reason(high_block)
                pair_gap = high_bb[1] - low_bb[3]
                pair_need = target_gap_um - pair_gap
                lines.append("REVERT_TO_ORIGINAL_PAIR=1")
            lines.append(f"PAIR_GAP={pair_gap:.6f} PAIR_NEED={pair_need:.6f}")
            if pair_need <= 0:
                lines.append("SKIP selected pair already >= target")
                continue

            half = pair_need / 2.0

            low_pin_like = bool(low_block and not low_fill)
            high_pin_like = bool(high_block and not high_fill)

            low_has_via = _has_local_via_on_side(low_bb, gap, "y", "low", layer, via_layers)
            high_has_via = _has_local_via_on_side(high_bb, gap, "y", "high", layer, via_layers)
            lines.append(f"LOW_HAS_VIA={int(low_has_via)} HIGH_HAS_VIA={int(high_has_via)}")

            low_locked = low_fill or low_has_via or low_pin_like
            high_locked = high_fill or high_has_via or high_pin_like

            if low_locked and high_locked:
                lines.append(
                    "ERROR edit blocked because both sides are locked: "
                    + f"LOW(fill={int(low_fill)},via={int(low_has_via)},pin={int(low_pin_like)}), "
                    + f"HIGH(fill={int(high_fill)},via={int(high_has_via)},pin={int(high_pin_like)})"
                )
                continue

            mode = "two_side"
            mode_reason = ""
            if low_locked and not high_locked:
                mode = "high_only"
                mode_reason = "LOW locked"
            elif high_locked and not low_locked:
                mode = "low_only"
                mode_reason = "HIGH locked"

            if not dry_run:
                if mode == "high_only":
                    one_side_need = max(pair_need, need)
                    lines.append(f"MODE=one_side HIGH_ONLY ({mode_reason})")
                    fusion_compiler(
                        query=(
                            f"resize_objects -delta {{ {{0 {one_side_need:.6f}}} {{0 0}} }} "
                            f"[get_shapes -quiet {high_name}]"
                        )
                    )
                elif mode == "low_only":
                    one_side_need = max(pair_need, need)
                    lines.append(f"MODE=one_side LOW_ONLY ({mode_reason})")
                    fusion_compiler(
                        query=(
                            f"resize_objects -delta {{ {{0 0}} {{0 {-one_side_need:.6f}}} }} "
                            f"[get_shapes -quiet {low_name}]"
                        )
                    )
                else:
                    lines.append("MODE=two_side SYMMETRIC")
                    fusion_compiler(
                        query=(
                            f"resize_objects -delta {{ {{0 0}} {{0 {-half:.6f}}} }} "
                            f"[get_shapes -quiet {low_name}]"
                        )
                    )
                    fusion_compiler(
                        query=(
                            f"resize_objects -delta {{ {{0 {half:.6f}}} {{0 0}} }} "
                            f"[get_shapes -quiet {high_name}]"
                        )
                    )
                lines.append(f"LOW_AFTER={_shape_bbox(low_name)}")
                lines.append(f"HIGH_AFTER={_shape_bbox(high_name)}")

    return "\n".join(lines)
