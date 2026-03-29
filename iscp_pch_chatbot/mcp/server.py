#!/usr/bin/env python3
"""
FastMCP Server for fusion_compiler - Various Usage Examples
"""

import re
from pathlib import Path

from autobots_sdk.base.mcp.servers.base_server import AutobotsMCPStdioServer


# Import the connector
from autobots_sdk.base.executors.synopsys.fusion_compiler import fusion_compiler
from macro_report import export_macro_report_impl
from gap_bbox_fix import fix_gap_bbox_spacing_impl

mcp_fc = AutobotsMCPStdioServer(name="fusion_compiler")


def _classify_mcp_fixer_for_rule(rule_name: str) -> tuple[bool, str]:
    """
    Return whether a DRC rule is potentially fixable by current MCP tools,
    and which fixer name is the best match.
    """

    normalized = (rule_name or "").lower()

    if re.search(r"^m6_41(?:$|/)", normalized):
        return True, "fix_metal_MyaORb_41_42_drc"

    if re.search(r"^m9_myb_41(?:$|/)", normalized):
        return True, "fix_m9_Myb_41_drc"

    if re.search(r"^m\d+_my[ab]_4[12](?:$|/)", normalized):
        return True, "fix_metal_MyaORb_41_42_drc"

    return False, ""


def _normalize_rule_root(rule_name: str) -> str:
    """
    Normalize a DRC rule token to a root key used for similarity grouping.
    Examples:
    - m9_Myb_149/m9_Myb_199 -> m9_myb
    - m7_Mya_41 -> m7_mya
    - M6_44 -> m6
    """

    normalized = (rule_name or "").strip().lower()
    first = normalized.split("/")[0]

    myab_match = re.match(r"^(m\d+)_my([ab])_\d+", first)
    if myab_match:
        return f"{myab_match.group(1)}_my{myab_match.group(2)}"

    m_match = re.match(r"^(m\d+)_\d+", first)
    if m_match:
        return m_match.group(1)

    generic_m_match = re.match(r"^(m\d+)_", first)
    if generic_m_match:
        return generic_m_match.group(1)

    return ""


def _derive_indirect_fix_potentials(
    rule_counts: dict[str, int],
) -> dict[str, tuple[str, str]]:
    """
    For rules not directly fixable by MCP, infer whether they are potentially
    cleaned indirectly by a related directly-fixable rule in the same family.

    Returns:
        dict[rule_name, (driver_rule_name, driver_fixer_name)]
    """

    fixable_by_root: dict[str, tuple[str, str]] = {}
    for rule_name in rule_counts:
        is_fixable, fixer_name = _classify_mcp_fixer_for_rule(rule_name)
        if not is_fixable:
            continue
        root = _normalize_rule_root(rule_name)
        if root and root not in fixable_by_root:
            fixable_by_root[root] = (rule_name, fixer_name)

    indirect: dict[str, tuple[str, str]] = {}
    for rule_name in rule_counts:
        is_fixable, _ = _classify_mcp_fixer_for_rule(rule_name)
        if is_fixable:
            continue
        root = _normalize_rule_root(rule_name)
        if root and root in fixable_by_root:
            indirect[rule_name] = fixable_by_root[root]

    return indirect


def _parse_rule_counts_from_error_file(error_file_path: str) -> tuple[dict[str, int], str]:
    """
    Parse ERROR SUMMARY section from TOP_LAYOUT_ERRORS-like file.

    Returns:
        tuple[dict[str, int], str]: (rule_counts, error_message).
        If parsing succeeds, error_message is empty string.
    """

    try:
        with open(error_file_path, "r", encoding="utf-8", errors="ignore") as handle:
            lines = handle.readlines()
    except Exception as exc:
        return {}, f"Failed to read error file: {exc}"

    in_summary = False
    current_rule = ""
    rule_counts: dict[str, int] = {}

    for raw in lines:
        line = raw.rstrip("\n")

        if re.match(r"^\s*ERROR SUMMARY\s*$", line):
            in_summary = True
            continue

        if in_summary and re.match(r"^\s*ERROR DETAILS\s*$", line):
            break

        if not in_summary:
            continue

        rule_match = re.match(
            r"^\s*([mM]\d+_[A-Za-z0-9_]+(?:/[mM]\d+_[A-Za-z0-9_]+)*)\s*:",
            line,
        )
        if rule_match:
            current_rule = rule_match.group(1)
            rule_counts.setdefault(current_rule, 0)
            continue

        viol_match = re.search(r"(\d+)\s+violations?\s+found\.", line)
        if viol_match and current_rule:
            rule_counts[current_rule] += int(viol_match.group(1))

    if not rule_counts:
        return {}, "No parsable rule summary found. Ensure this is a TOP_LAYOUT_ERRORS file with an ERROR SUMMARY section."

    return rule_counts, ""


def _extract_rule_bboxes_from_error_details(error_file_path: str, rule_name: str) -> tuple[list[str], str]:
    """
    Extract violation bboxes for a specific rule from ERROR DETAILS section.

    Returns:
        tuple[list[str], str]: (bbox_list, error_message).
        bbox_list entries are formatted as "x1 y1 x2 y2".
    """

    try:
        with open(error_file_path, "r", encoding="utf-8", errors="ignore") as handle:
            text = handle.read()
    except Exception as exc:
        return [], f"Failed to read error file: {exc}"

    section_pattern = re.compile(
        rf"\n-+\n\s*{re.escape(rule_name)}:.*?\n-+\n(.*?)(?=\n-+\n\s*[A-Za-z0-9_./-]+:|\Z)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    section_match = section_pattern.search(text)
    if not section_match:
        return [], ""

    section_text = section_match.group(1)
    coord_pattern = re.compile(
        r"\(\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\)\s*"
        r"\(\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\)"
    )

    bboxes: list[str] = []
    seen: set[str] = set()
    for match in coord_pattern.finditer(section_text):
        bbox = f"{match.group(1)} {match.group(2)} {match.group(3)} {match.group(4)}"
        if bbox not in seen:
            seen.add(bbox)
            bboxes.append(bbox)

    return bboxes, ""

@mcp_fc.tool()
def get_current_time() -> str:
    """
    Returns the current local time as a string in the format "HH:MM:SS".

    Returns:
        str: The current time formatted as "HH:MM:SS".
    """

    from datetime import datetime

    return datetime.now().strftime("%H:%M:%S")

@mcp_fc.tool()
def find_current_design(log_path: str = None) -> str:
    """
    Find the current design loaded in the linked live Fusion Compiler session.

    Use this tool when the user asks for:
    - current design
    - active/loaded design
    - what design is open in FC
    - check linked Fusion session design

    Args:
        log_path: Optional path to save the output log

    Returns:
        str: Current design name (or FC executor response details).
    """
    # Direct execution - simple and clean!
    result = fusion_compiler(query="current_design", log_path=log_path)
    return result


@mcp_fc.tool()
def detect_current_design_intent(user_text: str = "") -> str:
    """
    Detect whether user text suggests checking the current design
    from the linked live Fusion Compiler session.

    Args:
        user_text: Latest user request text.

    Returns:
        str: Guidance message including whether to ask user confirmation.
    """

    text = (user_text or "").lower()

    design_keywords = [
        "current design",
        "active design",
        "loaded design",
        "what design",
        "which design",
        "open design",
        "current_design",
        "linked fusion session",
        "fusion session",
        "fc session",
    ]

    matched_keywords = [keyword for keyword in design_keywords if keyword in text]

    if matched_keywords:
        ask_line = "Detected current-design intent (keyword(s): " + ", ".join(matched_keywords) + ")."
        ask_line += " Ask user: 'Do you want me to check current_design from the linked Fusion session now?'"
        ask_line += "\nSuggested command: find_current_design()"
        return ask_line

    return (
        "No strong current-design intent detected. "
        "Do not auto-query Fusion session. Wait for explicit user confirmation."
    )


@mcp_fc.tool()
def export_macro_report(
    output_path: str = "/nfs/site/disks/zsc16_ttlpcd_00114/pard2dide/kgoh14/ai_test/WW09.5_copilot/tmp/macro_cells_ref_bbox_inst.txt",
) -> str:
    """
    Dump macro list with boundary box (bbox) report from the current design.

    Use this tool when the user asks for:
    - dump macro list
    - macro list with boundary box
    - macro bbox report
    - export macro report

    Export hard macro info in format:
    ref_name, llx lly urx ury, instance_name

    Args:
        output_path: Full path of output report file.

    Returns:
        str: Summary including output path and macro count.
    """

    return export_macro_report_impl(output_path)


@mcp_fc.tool()
def fix_m9_Myb_41_drc(
    search_margin_um: float = 0.05,
    gap_bboxes: str = "9.0820000 21.8525000 9.1320000 21.8690000;29.0780000 19.6140000 29.1280000 19.6205000",
    dry_run: bool = True,
    prefer_non_fill: bool = True,
) -> str:
    """
    Fix m9_Myb_41 DRC by repairing M9 end-to-end spacing from gap bboxes.

    This tool is specialized for:
    - Rule: m9_Myb_41
    - Layer: m9
    - Target spacing: 0.116 um
        - Shape search includes hierarchical and fill objects
        - Editable geometry only (active routing shapes)

        Guard behavior:
        - If one selected side is FILL geometry, that side is not edited and
            full delta is applied to the non-fill side.
        - If both sides are FILL geometry, edit is blocked.
        - If either selected side is pin-like geometry (for example macro pin),
            edit is blocked.

    Use this tool when user asks to fix from violation bbox window (gap bbox),
    not from exact shape bbox.

    Args:
        search_margin_um: Extra margin around each gap bbox for shape search.
        gap_bboxes: Semicolon-separated bbox list as "x1 y1 x2 y2; x1 y1 x2 y2".
        dry_run: If True, only report selected shape pairs; no resize edit.
        prefer_non_fill: Prefer non-fill candidates when picking repair pair.

    Returns:
        str: Detailed operation summary and selected shapes.
    """

    return fix_gap_bbox_spacing_impl(
        layer="m9",
        target_gap_um=0.116,
        search_margin_um=search_margin_um,
        gap_bboxes=gap_bboxes,
        dry_run=dry_run,
        prefer_non_fill=prefer_non_fill,
    )


@mcp_fc.tool()
def fix_metal_MyaORb_41_42_drc(
    layer: str = "m9",
    search_margin_um: float = 0.05,
    gap_bboxes: str = "9.0820000 21.8525000 9.1320000 21.8690000;29.0780000 19.6140000 29.1280000 19.6205000",
    dry_run: bool = True,
    prefer_non_fill: bool = True,
    target_gap_um: float | None = None,
) -> str:
    """
    Generic fixer for m*_Mya_41/m*_Myb_41 and m*_Mya_42/m*_Myb_42-like
    end-to-end spacing DRC from gap bboxes.

    Works for horizontal or vertical layer orientation automatically,
    based on the gap bbox geometry.

    Current validated layers: m6, m7, m8, m9, m10.

    Supported layers and default target spacing:
    - m6: 0.04 um (M6_41 minimum ETE spacing target)
    - m7, m8 (Mya): 0.12 um
    - m9, m10 (Myb): 0.116 um

    Args:
        layer: Metal routing layer (m6/m7/m8/m9/m10).
        search_margin_um: Extra margin around each gap bbox for shape search.
        gap_bboxes: Semicolon-separated bbox list as "x1 y1 x2 y2; x1 y1 x2 y2".
        dry_run: If True, only report selected shape pairs; no resize edit.
        prefer_non_fill: Prefer non-fill candidates when picking repair pair.
        target_gap_um: Optional override for target gap. If not provided, layer default is used.

    Returns:
        str: Detailed operation summary and selected shapes, or validation error.
    """

    layer_norm = layer.strip().lower()
    default_target_by_layer = {
        "m6": 0.04,
        "m7": 0.12,
        "m8": 0.12,
        "m9": 0.116,
        "m10": 0.116,
    }

    if layer_norm not in default_target_by_layer:
        return "Unsupported layer. Use one of: m6, m7, m8, m9, m10"

    resolved_target_gap = (
        target_gap_um if target_gap_um is not None else default_target_by_layer[layer_norm]
    )

    return fix_gap_bbox_spacing_impl(
        layer=layer_norm,
        target_gap_um=resolved_target_gap,
        search_margin_um=search_margin_um,
        gap_bboxes=gap_bboxes,
        dry_run=dry_run,
        prefer_non_fill=prefer_non_fill,
    )


@mcp_fc.tool()
def fix_metal_MyaORb_41_drc(
    layer: str = "m9",
    search_margin_um: float = 0.05,
    gap_bboxes: str = "9.0820000 21.8525000 9.1320000 21.8690000;29.0780000 19.6140000 29.1280000 19.6205000",
    dry_run: bool = True,
    prefer_non_fill: bool = True,
    target_gap_um: float | None = None,
) -> str:
    """
    Backward-compatible wrapper for fix_metal_MyaORb_41_42_drc.
    """

    return fix_metal_MyaORb_41_42_drc.fn(
        layer=layer,
        search_margin_um=search_margin_um,
        gap_bboxes=gap_bboxes,
        dry_run=dry_run,
        prefer_non_fill=prefer_non_fill,
        target_gap_um=target_gap_um,
    )


@mcp_fc.tool()
def suggest_myaorb_42_fix_from_error_file(
    error_file_path: str,
) -> str:
    """
    Parse a DRC error report and suggest fixer usage when rule names contain
    m*_Mya_* or m*_Myb_* keywords, especially *_41 or *_42.

    Args:
        error_file_path: Full path to TOP_LAYOUT_ERRORS-like report.

    Returns:
        str: Suggested fixer commands for matched rules, or an explanation.
    """

    try:
        with open(error_file_path, "r", encoding="utf-8", errors="ignore") as handle:
            text = handle.read()
    except Exception as exc:
        return f"Failed to read error file: {exc}"

    rule_names = sorted(set(re.findall(r"^\s*([A-Za-z0-9_./-]+):", text, flags=re.MULTILINE)))
    if not rule_names:
        return "No rule names found in file."

    myaorb_rules = [r for r in rule_names if re.search(r"m\d+_my[ab]_\d+", r, flags=re.IGNORECASE)]
    if not myaorb_rules:
        return "No m*_Mya_* or m*_Myb_* rules detected."

    focus_rules = [r for r in myaorb_rules if re.search(r"_(41|42)(?:$|/)", r)]
    candidate_rules = focus_rules if focus_rules else myaorb_rules

    suggested_layers = sorted(
        {
            match.group(1).lower()
            for rule in candidate_rules
            for match in [re.search(r"(m\d+)_my[ab]_", rule, flags=re.IGNORECASE)]
            if match
        }
    )

    lines = []
    lines.append(f"Detected {len(candidate_rules)} similar m*_Mya/Myb rules from: {error_file_path}")
    lines.append("Suggested MCP fixer: fix_metal_MyaORb_41_42_drc")
    lines.append("Suggested layers: " + (", ".join(suggested_layers) if suggested_layers else "(none)"))
    lines.append("Matched rules:")
    lines.extend([f"- {rule}" for rule in candidate_rules])
    lines.append("Example call: fix_metal_MyaORb_41_42_drc(layer=\"m7\", gap_bboxes=\"x1 y1 x2 y2\", dry_run=True)")

    return "\n".join(lines)


@mcp_fc.tool()
def process_drc_error_file(error_file_path: str) -> str:
    """
    Process a TOP_LAYOUT_ERRORS-like file and summarize violations by rule,
    then suggest which error codes are currently honored by existing fixers.

    This tool does NOT apply fixes.

    Args:
        error_file_path: Full path to DRC error report.

    Returns:
        str: Full DRC summary and actionable suggestions:
            - all detected rules with counts
            - rules currently fixable by MCP flow
            - high-impact next candidates for future fixer support
    """

    rule_counts, parse_error = _parse_rule_counts_from_error_file(error_file_path)
    if parse_error:
        return parse_error

    total_violations = sum(rule_counts.values())
    sorted_rules = sorted(rule_counts.items(), key=lambda item: (-item[1], item[0].lower()))

    myaorb_rules = [
        (rule, count)
        for rule, count in sorted_rules
        if re.search(r"m\d+_my[ab]_\d+", rule, flags=re.IGNORECASE)
    ]

    fixable_rules: list[tuple[str, int, str]] = []
    for rule, count in sorted_rules:
        is_fixable, fixer_name = _classify_mcp_fixer_for_rule(rule)
        if is_fixable:
            fixable_rules.append((rule, count, fixer_name))

    non_honored_myaorb_rules = [
        (rule, count)
        for rule, count in myaorb_rules
        if not _classify_mcp_fixer_for_rule(rule)[0]
    ]

    non_myaorb_rules = [
        (rule, count)
        for rule, count in sorted_rules
        if not re.search(r"m\d+_my[ab]_\d+", rule, flags=re.IGNORECASE)
    ]

    indirect_potentials = _derive_indirect_fix_potentials(rule_counts)

    suggested_layers = sorted(
        {
            matched.group(1).lower()
            for rule, _, _ in fixable_rules
            for matched in [re.search(r"(m\d+)_my[ab]_", rule, flags=re.IGNORECASE)]
            if matched
        }
    )

    out: list[str] = []
    out.append(f"Processed: {error_file_path}")
    out.append(f"Total rules: {len(rule_counts)}")
    out.append(f"Total violations: {total_violations}")
    out.append("")
    out.append("All rules (sorted by count):")
    for rule, count in sorted_rules:
        if rule in indirect_potentials:
            driver_rule, driver_fixer = indirect_potentials[rule]
            out.append(
                f"- {rule}: {count} (potentially cleaned by {driver_rule} via {driver_fixer})"
            )
        else:
            out.append(f"- {rule}: {count}")

    out.append("")
    out.append("Potentially fixable now (detected from current MCP fixers):")
    if fixable_rules:
        for rule, count, fixer_name in fixable_rules:
            out.append(f"- {rule}: {count} (fixer: {fixer_name})")
        out.append(
            "Suggested layers: " + (", ".join(suggested_layers) if suggested_layers else "(none)")
        )
    else:
        out.append("- None detected for current MCP fixer scope")

    if non_honored_myaorb_rules:
        out.append("")
        out.append("Mya/Myb rules not yet in honored-fix scope (_41/_42):")
        for rule, count in non_honored_myaorb_rules[:20]:
            out.append(f"- {rule}: {count}")

    if indirect_potentials:
        out.append("")
        out.append("Potentially cleaned indirectly by fixable MCP rules (heuristic):")
        for rule, count in sorted_rules:
            if rule not in indirect_potentials:
                continue
            driver_rule, driver_fixer = indirect_potentials[rule]
            out.append(
                f"- {rule}: {count} -> potential via {driver_rule} ({driver_fixer})"
            )

    out.append("")
    out.append("Suggested next-fix candidates (learning priority):")
    learning_candidates = (non_honored_myaorb_rules + non_myaorb_rules)[:12]
    if learning_candidates:
        for rule, count in learning_candidates:
            out.append(f"- {rule}: {count}")
    else:
        out.append("- None")

    out.append("")
    out.append("Note: This tool only summarizes and suggests. No geometry edit is applied.")

    return "\n".join(out)


@mcp_fc.tool()
def load_drc_and_suggest_fixes(error_file_path: str) -> str:
    """
    Load a TOP_LAYOUT_ERRORS-like file and directly return fixability guidance
    from currently available MCP fixers, without additional confirmation flow.

    Args:
        error_file_path: Full path to DRC error report.

    Returns:
        str: Compact summary with:
            - total rules / violations
            - potentially fixable rules and mapped fixer names
            - ready-to-run fixer command templates
    """

    rule_counts, parse_error = _parse_rule_counts_from_error_file(error_file_path)
    if parse_error:
        return parse_error

    sorted_rules = sorted(rule_counts.items(), key=lambda item: (-item[1], item[0].lower()))
    total_violations = sum(rule_counts.values())

    fixable_rules: list[tuple[str, int, str]] = []
    for rule, count in sorted_rules:
        is_fixable, fixer_name = _classify_mcp_fixer_for_rule(rule)
        if is_fixable:
            fixable_rules.append((rule, count, fixer_name))

    out: list[str] = []
    out.append(f"Loaded DRC: {error_file_path}")
    out.append(f"Total rules: {len(rule_counts)}")
    out.append(f"Total violations: {total_violations}")
    out.append("")
    out.append("Potentially fixable now (by current MCP logic):")

    if not fixable_rules:
        out.append("- None")
        out.append("")
        out.append("Use process_drc_error_file(...) for full prioritization list.")
        return "\n".join(out)

    for rule, count, fixer_name in fixable_rules:
        out.append(f"- {rule}: {count} (fixer: {fixer_name})")

    out.append("")
    out.append("Ready-to-run templates:")

    emitted_generic = False
    emitted_m9_specific = False
    for _, _, fixer_name in fixable_rules:
        if fixer_name == "fix_m9_Myb_41_drc" and not emitted_m9_specific:
            out.append(
                "- fix_m9_Myb_41_drc(gap_bboxes=\"x1 y1 x2 y2;...\", search_margin_um=0.05, dry_run=True, prefer_non_fill=True)"
            )
            emitted_m9_specific = True
        if fixer_name == "fix_metal_MyaORb_41_42_drc" and not emitted_generic:
            out.append(
                "- fix_metal_MyaORb_41_42_drc(layer=\"m7|m8|m9|m10\", gap_bboxes=\"x1 y1 x2 y2;...\", search_margin_um=0.05, dry_run=True, prefer_non_fill=True)"
            )
            emitted_generic = True

    out.append("")
    out.append("No fix is applied by this tool. Use returned templates for dry-run first.")

    return "\n".join(out)


@mcp_fc.tool()
def fix_m6_to_m10_41_from_error_file(
    error_file_path: str,
    dry_run: bool = True,
    search_margin_um: float = 0.05,
    prefer_non_fill: bool = True,
) -> str:
    """
    Batch-fix m6/m7/m8/m9/m10 *_41 end-to-end spacing violations directly from
    a TOP_LAYOUT_ERRORS-like report using fix_metal_MyaORb_41_42_drc.

    This tool:
    - extracts bbox windows from ERROR DETAILS for *_41 rules
    - maps each rule to layer m6/m7/m8/m9/m10
    - runs the generic fixer once per layer with combined bboxes

    Args:
        error_file_path: Full path to DRC error report.
        dry_run: If True, no geometry edits are applied.
        search_margin_um: Search margin around each gap bbox.
        prefer_non_fill: Prefer non-fill candidates when selecting shape pairs.

    Returns:
        str: Per-layer extraction and fixer execution summary.
    """

    target_rules = [
        ("m6", "M6_41/M6_411"),
        ("m7", "m7_Mya_41"),
        ("m8", "m8_Mya_41"),
        ("m9", "m9_Myb_41"),
        ("m10", "m10_Myb_41"),
    ]

    out: list[str] = []
    out.append(f"Batch m6-m10 _41 fixer input: {error_file_path}")
    out.append(f"dry_run={dry_run}, search_margin_um={search_margin_um}, prefer_non_fill={prefer_non_fill}")
    out.append("")

    any_attempted = False
    for layer, rule_name in target_rules:
        bboxes, error_message = _extract_rule_bboxes_from_error_details(error_file_path, rule_name)
        if error_message:
            out.append(f"[{rule_name}] ERROR: {error_message}")
            continue

        if not bboxes:
            out.append(f"[{rule_name}] No bbox found in ERROR DETAILS. Skipped.")
            continue

        any_attempted = True
        gap_bboxes = ";".join(bboxes)
        out.append(f"[{rule_name}] Extracted {len(bboxes)} bbox(es). Running fixer on layer={layer}.")

        result = fix_metal_MyaORb_41_42_drc.fn(
            layer=layer,
            search_margin_um=search_margin_um,
            gap_bboxes=gap_bboxes,
            dry_run=dry_run,
            prefer_non_fill=prefer_non_fill,
        )
        out.append(result)
        out.append("")

    if not any_attempted:
        out.append("No m6/m7/m8/m9/m10 *_41 bbox windows were found. No fixer call was executed.")

    return "\n".join(out)


@mcp_fc.tool()
def fix_m7_to_m10_41_from_error_file(
    error_file_path: str,
    dry_run: bool = True,
    search_margin_um: float = 0.05,
    prefer_non_fill: bool = True,
) -> str:
    """
    Backward-compatible wrapper for fix_m6_to_m10_41_from_error_file.
    """

    return fix_m6_to_m10_41_from_error_file.fn(
        error_file_path=error_file_path,
        dry_run=dry_run,
        search_margin_um=search_margin_um,
        prefer_non_fill=prefer_non_fill,
    )


@mcp_fc.tool()
def detect_drc_error_intent(
    user_text: str = "",
    candidate_file_path: str = "",
    require_confirmation: bool = False,
) -> str:
    """
    Detect whether user intent and/or file name indicates DRC error processing,
    and generate a confirmation question before loading/summarizing the file.

    Trigger conditions:
    - File name includes TOP_LAYOUT_ERRORS
    - User text includes DRC/LV error reading keywords

    Args:
        user_text: Latest user request text.
        candidate_file_path: Optional file path that may be a DRC report.
        require_confirmation: If True, return ask-user wording. If False,
            return auto-process recommendation directly.

    Returns:
        str: Guidance message including whether to ask user confirmation.
    """

    text = (user_text or "").lower()
    file_name = Path(candidate_file_path).name if candidate_file_path else ""
    file_name_lc = file_name.lower()

    drc_keywords = [
        "load lv error",
        "read drc",
        "drc error",
        "layout error",
        "top_layout_errors",
        "lv error",
        "summarize drc",
        "parse drc",
    ]

    matched_keywords = [keyword for keyword in drc_keywords if keyword in text]
    looks_like_top_layout_errors = "top_layout_errors" in file_name_lc

    if looks_like_top_layout_errors or matched_keywords:
        reasons = []
        if looks_like_top_layout_errors:
            reasons.append(f"file name detected: {file_name}")
        if matched_keywords:
            reasons.append("keyword(s): " + ", ".join(matched_keywords))

        ask_line = "Detected DRC-error context (" + "; ".join(reasons) + ")."
        if require_confirmation:
            ask_line += " Ask user: 'Do you want me to load and summarize this DRC error file now?'"
        else:
            ask_line += " Auto-load behavior enabled: proceed to process immediately."

        if candidate_file_path:
            ask_line += f"\nSuggested command: process_drc_error_file(error_file_path=\"{candidate_file_path}\")"
            ask_line += "\nExpected output includes potentially fixable rules based on current MCP fixers."

        return ask_line

    return (
        "No strong DRC-error intent detected. "
        "Do not auto-process. Wait for explicit user confirmation."
    )

if __name__ == "__main__":
    mcp_fc.run()
