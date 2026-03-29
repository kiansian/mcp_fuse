#!/usr/bin/env python3
"""
MCP Fuse Server

Simple consolidation utilities for run-list style logs.
"""

from __future__ import annotations

import csv
import json
import hashlib
import re
import smtplib
import socket
import getpass

from datetime import datetime
from email.message import EmailMessage
from email.utils import formatdate
from html import escape
from pathlib import Path

try:
    from autobots_sdk.base.mcp.servers.base_server import AutobotsMCPStdioServer
except ModuleNotFoundError:
    class AutobotsMCPStdioServer:
        def __init__(self, name: str):
            self.name = name

        def tool(self):
            def _decorator(func):
                if not hasattr(func, "fn"):
                    func.fn = func
                return func

            return _decorator

        def run(self):
            raise RuntimeError("autobots_sdk is not available in this Python environment")

mcp_fuse = AutobotsMCPStdioServer(name="fusion_compiler_fuse")


def _derive_standard_paths(run_path: str) -> dict[str, str]:
    base = (run_path or "").strip().rstrip("/")
    return {
        "apr_fc_outputs_finish": f"{base}/apr_fc/outputs/finish" if base else "",
        "release_latest_finish": f"{base}/release/latest/finish" if base else "",
        "assembly_outputs": f"{base}/assembly/outputs" if base else "",
    }


def _md5sum(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _collect_ext_files(folder: str, exts: tuple[str, ...] = (".oas", ".sp")) -> dict[str, Path]:
    root = Path(folder)
    if not root.exists() or not root.is_dir():
        return {}

    files: dict[str, Path] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in exts:
            continue
        rel = str(path.relative_to(root))
        files[rel] = path
    return files


def _check_run_path_md5(run_path: str) -> dict[str, object]:
    derived = _derive_standard_paths(run_path)
    assembly = derived["assembly_outputs"]
    finish_a = derived["apr_fc_outputs_finish"]
    finish_b = derived["release_latest_finish"]

    asm_files = _collect_ext_files(assembly)
    fin_a_files = _collect_ext_files(finish_a)
    fin_b_files = _collect_ext_files(finish_b)

    keys = sorted(set(asm_files) | set(fin_a_files) | set(fin_b_files))

    rows: list[dict[str, object]] = []
    summary = {
        "total_candidates": len(keys),
        "present_in_assembly": 0,
        "missing_in_finish_a": 0,
        "missing_in_finish_b": 0,
        "md5_match_all": 0,
        "md5_mismatch": 0,
    }

    for rel in keys:
        p_asm = asm_files.get(rel)
        p_a = fin_a_files.get(rel)
        p_b = fin_b_files.get(rel)

        md5_asm = _md5sum(p_asm) if p_asm else ""
        md5_a = _md5sum(p_a) if p_a else ""
        md5_b = _md5sum(p_b) if p_b else ""

        if p_asm:
            summary["present_in_assembly"] += 1
        if p_asm and not p_a:
            summary["missing_in_finish_a"] += 1
        if p_asm and not p_b:
            summary["missing_in_finish_b"] += 1

        match_a = bool(p_asm and p_a and md5_asm == md5_a)
        match_b = bool(p_asm and p_b and md5_asm == md5_b)
        match_all = bool(p_asm and p_a and p_b and md5_asm == md5_a == md5_b)

        if match_all:
            summary["md5_match_all"] += 1
        elif p_asm and (p_a or p_b):
            if (p_a and not match_a) or (p_b and not match_b):
                summary["md5_mismatch"] += 1

        rows.append(
            {
                "relative_path": rel,
                "assembly_exists": bool(p_asm),
                "finish_a_exists": bool(p_a),
                "finish_b_exists": bool(p_b),
                "assembly_md5": md5_asm,
                "finish_a_md5": md5_a,
                "finish_b_md5": md5_b,
                "assembly_vs_finish_a_match": match_a,
                "assembly_vs_finish_b_match": match_b,
                "all_three_match": match_all,
            }
        )

    return {
        "run_path": run_path,
        "assembly_outputs": assembly,
        "apr_fc_outputs_finish": finish_a,
        "release_latest_finish": finish_b,
        "summary": summary,
        "rows": rows,
    }


def _parse_data_list_line(line: str) -> dict[str, str] | None:
    text = (line or "").strip()
    if not text:
        return None
    if text.startswith("#"):
        return None

    parts = text.split()
    if len(parts) < 3:
        return None

    tag = parts[0].upper()
    if tag == "XOR":
        return None

    if tag == "WARD" and len(parts) >= 4:
        project_name = parts[1]
        flow_name = parts[2]
        run_path = " ".join(parts[3:])
        return {
            "project_name": project_name,
            "flow_name": flow_name,
            "run_path": run_path,
            **_derive_standard_paths(run_path),
        }

    project_name = parts[0]
    flow_name = parts[1]
    run_path = " ".join(parts[2:])

    return {
        "project_name": project_name,
        "flow_name": flow_name,
        "run_path": run_path,
        **_derive_standard_paths(run_path),
    }


def _file_timestamp(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return "MISSING"
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


def _file_md5_or_missing(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return "MISSING"
    return _md5sum(path)


def _build_block_artifact_rows(data_list_file: str) -> tuple[list[dict[str, str]], str]:
    records, error = _read_records_from_file(data_list_file)
    if error:
        return [], error
    if not records:
        return [], f"No valid rows in: {data_list_file}"

    rows: list[dict[str, str]] = []
    for record in records:
        project = record["project_name"]
        block = record["flow_name"]
        run_path = record["run_path"]

        assembly_dir = Path(record["assembly_outputs"])
        finish_a_dir = Path(record["apr_fc_outputs_finish"])
        finish_b_dir = Path(record["release_latest_finish"])

        for ext in ("sp", "oas"):
            file_name = f"{block}.{ext}"
            assembly_file = assembly_dir / file_name
            finish_a_file = finish_a_dir / file_name
            finish_b_file = finish_b_dir / file_name

            assembly_md5 = _file_md5_or_missing(assembly_file)
            finish_a_md5 = _file_md5_or_missing(finish_a_file)
            finish_b_md5 = _file_md5_or_missing(finish_b_file)

            rows.append(
                {
                    "project": project,
                    "block": block,
                    "file": file_name,
                    "run_path": run_path,
                    "assembly_dir": str(assembly_dir),
                    "finish_a_dir": str(finish_a_dir),
                    "finish_b_dir": str(finish_b_dir),
                    "assembly_file": str(assembly_file),
                    "finish_a_file": str(finish_a_file),
                    "finish_b_file": str(finish_b_file),
                    "assembly_timestamp": _file_timestamp(assembly_file),
                    "finish_a_timestamp": _file_timestamp(finish_a_file),
                    "finish_b_timestamp": _file_timestamp(finish_b_file),
                    "assembly_md5": assembly_md5,
                    "finish_a_md5": finish_a_md5,
                    "finish_b_md5": finish_b_md5,
                    "assembly_vs_finish_a_match": "YES"
                    if assembly_md5 != "MISSING" and assembly_md5 == finish_a_md5
                    else "NO",
                    "assembly_vs_finish_b_match": "YES"
                    if assembly_md5 != "MISSING" and assembly_md5 == finish_b_md5
                    else "NO",
                    "finish_a_vs_finish_b_match": "YES"
                    if finish_a_md5 != "MISSING" and finish_a_md5 == finish_b_md5
                    else "NO",
                }
            )

    rows.sort(key=lambda row: (row["block"], 0 if row["file"].endswith(".sp") else 1, row["file"]))
    return rows, ""


def _build_paths_summary(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        block = row["block"]
        if block in seen:
            continue
        seen.add(block)
        out.append(
            {
                "block": block,
                "assembly_dir": row["assembly_dir"],
                "finish_a_dir": row["finish_a_dir"],
                "finish_b_dir": row["finish_b_dir"],
            }
        )
    return out


def _build_markdown_split_tables(rows: list[dict[str, str]]) -> str:
    lines: list[str] = []
    lines.append("Paths")
    lines.append("")
    for item in _build_paths_summary(rows):
        lines.append(f"- {item['block']}")
        lines.append(f"  - Assembly: {item['assembly_dir']}")
        lines.append(f"  - Finish A: {item['finish_a_dir']}")
        lines.append(f"  - Finish B: {item['finish_b_dir']}")

    lines.append("")
    lines.append("| Project | Block | File | Assembly Timestamp | Finish A Timestamp | Finish B Timestamp |")
    lines.append("|---|---|---|---|---|---|")
    for row in rows:
        lines.append(
            f"| {row['project']} | {row['block']} | {row['file']} | "
            f"{row['assembly_timestamp']} | {row['finish_a_timestamp']} | {row['finish_b_timestamp']} |"
        )

    lines.append("")
    lines.append(
        "| Project | Block | File | Assembly gmd5sum | Finish A gmd5sum | "
        "Finish B gmd5sum | A vs FA | A vs FB | FA vs FB |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for row in rows:
        lines.append(
            f"| {row['project']} | {row['block']} | {row['file']} | "
            f"{row['assembly_md5']} | {row['finish_a_md5']} | {row['finish_b_md5']} | "
            f"{row['assembly_vs_finish_a_match']} | {row['assembly_vs_finish_b_match']} | "
            f"{row['finish_a_vs_finish_b_match']} |"
        )

    return "\n".join(lines)


def _build_html_split_tables(rows: list[dict[str, str]]) -> str:
    paths = _build_paths_summary(rows)

    ts_headers = [
        "project",
        "block",
        "file",
        "assembly_timestamp",
        "finish_a_timestamp",
        "finish_b_timestamp",
    ]
    md5_headers = [
        "project",
        "block",
        "file",
        "assembly_md5",
        "finish_a_md5",
        "finish_b_md5",
        "assembly_vs_finish_a_match",
        "assembly_vs_finish_b_match",
        "finish_a_vs_finish_b_match",
    ]

    path_html = []
    for item in paths:
        path_html.append(
            "<div>"
            f"<b>{escape(item['block'])}</b><br>"
            f"Assembly path: {escape(item['assembly_dir'])}<br>"
            f"Finish A path: {escape(item['finish_a_dir'])}<br>"
            f"Finish B path: {escape(item['finish_b_dir'])}"
            "</div><br>"
        )

    ts_head = "".join(f"<th>{escape(key)}</th>" for key in ts_headers)
    ts_rows = []
    for row in rows:
        ts_rows.append(
            "<tr>"
            + "".join(f"<td>{escape(str(row.get(key, '')))}</td>" for key in ts_headers)
            + "</tr>"
        )

    md5_head = "".join(f"<th>{escape(key)}</th>" for key in md5_headers)
    md5_rows = []
    for row in rows:
        md5_rows.append(
            "<tr>"
            + "".join(f"<td>{escape(str(row.get(key, '')))}</td>" for key in md5_headers)
            + "</tr>"
        )

    return f"""<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <title>Block Artifact Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; font-size: 13px; color: #222; }}
    h2, h3 {{ margin: 0 0 10px 0; }}
    table {{ border-collapse: collapse; width: 100%; margin: 8px 0 16px; }}
    th, td {{ border: 1px solid #d0d7de; padding: 6px 8px; text-align: left; }}
    th {{ background: #f6f8fa; }}
    tr:nth-child(even) {{ background: #fbfbfb; }}
  </style>
</head>
<body>
  <h2>Block Artifact Report</h2>
  <h3>Paths</h3>
  {''.join(path_html)}
  <h3>Timestamps</h3>
  <table>
    <thead><tr>{ts_head}</tr></thead>
    <tbody>{''.join(ts_rows)}</tbody>
  </table>
  <h3>gmd5sum</h3>
  <table>
    <thead><tr>{md5_head}</tr></thead>
    <tbody>{''.join(md5_rows)}</tbody>
  </table>
</body>
</html>
"""


def _parse_violation_summary_drc_file(file_path: str) -> tuple[list[dict[str, str]], str]:
    path = Path(file_path)
    if not path.exists() or not path.is_file():
        return [], f"File not found: {file_path}"

    rows: list[dict[str, str]] = []
    in_table = False
    in_error_summary = False
    total_errors = ""

    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line_no, raw in enumerate(handle, start=1):
            line = raw.rstrip("\n")

            if not in_error_summary:
                if (
                    "Errors" in line
                    and "Info" in line
                    and "Warnings" in line
                    and "Waived" in line
                    and "Status" in line
                    and "Block" in line
                ):
                    in_error_summary = True
            else:
                summary_text = line.strip()
                if summary_text and set(summary_text) > {"-", "=", "_"} and not total_errors:
                    summary_match = re.match(r"^\s*(\d+)\s+\d+\s+\d+\s+\d+\s+\S+\s+\S+\s+\S+", line)
                    if summary_match:
                        total_errors = summary_match.group(1)

            if not in_table:
                if "Count" in line and "Description" in line:
                    in_table = True
                continue

            text = line.strip()
            if not text:
                continue
            if set(text) <= {"-", "=", "_"}:
                continue

            match = re.match(r"^\s*(\d+)\s+(.+?)\s*$", line)
            if not match:
                continue

            count = match.group(1)
            description = match.group(2).strip()
            if not description:
                continue

            rule_name = description.split(":", 1)[0].strip()
            if not rule_name:
                continue

            rows.append(
                {
                    "line_no": str(line_no),
                    "count": count,
                    "description": description,
                    "rule_name": rule_name,
                    "total_errors": total_errors,
                }
            )

    if not rows:
        return [], (
            f"No rule rows parsed after 'Count Description' in: {file_path}. "
            "Please verify file format."
        )

    return rows, ""


def _parse_violation_summary_total_errors(
    file_path: str,
    block_name: str = "",
    flow_name: str = "drc",
) -> str:
    path = Path(file_path)
    if not path.exists() or not path.is_file():
        return ""

    in_error_summary = False
    block_key = (block_name or "").strip().lower()
    flow_key = (flow_name or "").strip().lower()

    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw in handle:
            line = raw.rstrip("\n")

            if not in_error_summary:
                if (
                    "Errors" in line
                    and "Info" in line
                    and "Warnings" in line
                    and "Waived" in line
                    and "Status" in line
                    and "Block" in line
                ):
                    in_error_summary = True
                continue

            text = line.strip()
            if not text or set(text) <= {"-", "=", "_"}:
                continue

            parts = text.split()
            if len(parts) < 7:
                continue
            if not all(token.isdigit() for token in parts[:4]):
                continue

            errors = parts[0]
            flow = parts[4].lower()
            block = parts[-1].lower()

            if flow_key and flow != flow_key:
                continue
            if block_key and block != block_key:
                continue

            return errors

    return ""


def _collect_drc_rules_from_data_list(data_list_file: str) -> tuple[list[dict[str, str]], str]:
    records, error = _read_records_from_file(data_list_file)
    if error:
        return [], error
    if not records:
        return [], f"No valid rows in: {data_list_file}"

    out_rows: list[dict[str, str]] = []
    for record in records:
        project = record["project_name"]
        block = record["flow_name"]
        run_path = record["run_path"].rstrip("/")
        summary_file = f"{run_path}/lv_icv/reports/{block}.violation_summary_drc"
        block_summary_file = f"{run_path}/lv_icv/reports/{block}.violation_summary"

        parsed, parse_error = _parse_violation_summary_drc_file(summary_file)
        if parse_error:
            total_from_summary = _parse_violation_summary_total_errors(
                summary_file,
                block_name=block,
                flow_name="drc",
            )
            total_from_block_summary = _parse_violation_summary_total_errors(
                block_summary_file,
                block_name=block,
                flow_name="drc",
            )
            total_errors = total_from_summary or total_from_block_summary
            status = "OK" if total_errors == "0" else parse_error
            out_rows.append(
                {
                    "project": project,
                    "block": block,
                    "run_path": run_path,
                    "summary_file": summary_file,
                    "rule_names": "",
                    "rule_count": "0",
                    "total_errors": total_errors,
                    "status": status,
                }
            )
            continue

        seen: set[str] = set()
        unique_rules: list[str] = []
        for row in parsed:
            name = row["rule_name"]
            if name in seen:
                continue
            seen.add(name)
            unique_rules.append(name)
        unique_rules.sort()

        total_from_block_summary = _parse_violation_summary_total_errors(
            block_summary_file,
            block_name=block,
            flow_name="drc",
        )
        total_errors = total_from_block_summary or (parsed[0].get("total_errors", "") if parsed else "")

        out_rows.append(
            {
                "project": project,
                "block": block,
                "run_path": run_path,
                "summary_file": summary_file,
                "rule_names": "\n".join(unique_rules),
                "rule_count": str(len(unique_rules)),
                "total_errors": total_errors,
                "status": "OK",
            }
        )

    out_rows.sort(key=lambda row: (row["block"], row["run_path"]))
    return out_rows, ""


def _read_records_from_file(file_path: str) -> tuple[list[dict[str, str]], str]:
    path = Path(file_path)
    if not path.exists() or not path.is_file():
        return [], f"File not found: {file_path}"

    records: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for idx, raw in enumerate(handle, start=1):
            parsed = _parse_data_list_line(raw)
            if parsed is None:
                continue
            parsed["source_file"] = str(path)
            parsed["line_no"] = str(idx)
            records.append(parsed)

    return records, ""


@mcp_fuse.tool()
def get_current_time() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _contains_any(text: str, keywords: list[str]) -> bool:
    return any(item in text for item in keywords)


def _extract_interval_minutes(text: str) -> int:
    if not text:
        return 0

    if "hourly" in text:
        return 60
    if "daily" in text:
        return 1440

    match = re.search(
        r"(?:every|each)\s+(\d+)\s*(min|mins|minute|minutes|hr|hrs|hour|hours|day|days)",
        text,
    )
    if not match:
        return 0

    value = int(match.group(1))
    unit = match.group(2)
    if unit.startswith("min"):
        return value
    if unit.startswith("hr") or unit.startswith("hour"):
        return value * 60
    if unit.startswith("day"):
        return value * 1440
    return 0


@mcp_fuse.tool()
def resolve_refresh_resend_intent(
    user_text: str,
    me_email: str = "kian.sian.goh@intel.com",
) -> str:
    """
    Resolve natural-language mail intent for refresh/resend style requests.

    Rules:
    - 'refresh'/'resend' are treated as send-mail actions.
    - Recipient scope is inferred from wording:
        * me-only phrases => scope=ME
        * all/team phrases => scope=ALL
        * otherwise => scope=UNSPECIFIED (needs follow-up)
    - Scheduling is inferred from wording:
        * periodic/recursive/every/hourly/daily => DELIVERY_MODE=PERIODIC
        * otherwise => DELIVERY_MODE=ONE_TIME
    """

    text = (user_text or "").strip().lower()
    if not text:
        return "INTENT=UNKNOWN\nACTION=NONE\nSCOPE=UNSPECIFIED\nNEEDS_SCOPE=YES"

    send_keywords = [
        "refresh",
        "resend",
        "send",
        "mail",
        "email",
    ]
    me_keywords = [
        "to me",
        "send me",
        "mail me",
        "me only",
        "only me",
        "my email",
    ]
    all_keywords = [
        "to all",
        "send all",
        "mail all",
        "all recipients",
        "everyone",
        "team",
        "distro",
        "distribution",
    ]
    periodic_keywords = [
        "periodic",
        "recursive",
        "repeat",
        "recurring",
        "every",
        "hourly",
        "daily",
    ]

    action = "REFRESH_AND_RESEND" if _contains_any(text, send_keywords) else "NONE"
    intent = "SEND_MAIL" if action != "NONE" else "UNKNOWN"

    scope = "UNSPECIFIED"
    to_addr = ""
    if _contains_any(text, me_keywords) and not _contains_any(text, all_keywords):
        scope = "ME"
        to_addr = me_email.strip()
    elif _contains_any(text, all_keywords):
        scope = "ALL"

    delivery_mode = "PERIODIC" if _contains_any(text, periodic_keywords) else "ONE_TIME"
    interval_min = _extract_interval_minutes(text) if delivery_mode == "PERIODIC" else 0
    needs_interval = "YES" if delivery_mode == "PERIODIC" and interval_min <= 0 else "NO"

    needs_scope = "YES" if intent == "SEND_MAIL" and scope == "UNSPECIFIED" else "NO"
    aliases_hit = []
    for keyword in ["refresh", "resend"]:
        if keyword in text:
            aliases_hit.append(keyword)

    out = [
        f"INTENT={intent}",
        f"ACTION={action}",
        f"SCOPE={scope}",
        f"TO={to_addr}",
        f"DELIVERY_MODE={delivery_mode}",
        f"INTERVAL_MIN={interval_min}",
        f"NEEDS_INTERVAL={needs_interval}",
        f"NEEDS_SCOPE={needs_scope}",
        f"ALIASES_HIT={','.join(aliases_hit) if aliases_hit else '-'}",
    ]
    return "\n".join(out)


@mcp_fuse.tool()
def mcp_fuse_help() -> str:
    """
    Brief capability + input guide for mcp_fuse.
    """

    return "\n".join(
        [
            "mcp_fuse quick guide",
            "",
            "What I can do:",
            "- Parse run list from data_list.log (WARD rows supported).",
            "- Ignore comment/info lines starting with '#'.",
            "- Skip XOR rows from core run parsing.",
            "- Build consolidated CSV/JSON from one or many data_list files.",
            "- Check OAS/SP md5 and match status across assembly/apr_fc/release.",
            "- Generate block artifact CSV and HTML email content.",
            "- Build partition-based runtime summaries from APR, FEV, STAR_PV, and LV_ICV logs.",
            "- Build StarPV extract-quality summary table from extract_quality.report.",
            "- Parse DRC violation summaries and build DRC rule table.",
            "- Resolve refresh/resend natural-language intent into send-mail action + recipient scope.",
            "- Detect periodic/recursive mail requests and parse repeat interval.",
            "",
            "Data list rules:",
            "- WARD rows define partitions/columns in report matrices.",
            "- XOR rows are optional; commented/removed XOR rows are treated as not ready.",
            "",
            "Common inputs:",
            "- data_list_file: absolute path to data_list.log",
            "- run_path: absolute run folder path",
            "- block_name: block/flow name used in file names",
            "- output_csv_path/output_json_path/output_html_path: absolute output paths",
            "- file_paths: comma-separated absolute file paths",
            "",
            "Typical commands:",
            "- resolve_refresh_resend_intent(user_text, me_email)",
            "- parse_data_list_log(file_path)",
            "- consolidate_data_list_logs(file_paths, output_csv_path, output_json_path)",
            "- dump_block_artifact_csv(data_list_file, output_csv_path)",
            "- format_drc_rule_table_from_data_list(data_list_file, output_csv_path)",
            "- parse_violation_summary_drc_from_run(run_path, block_name, output_csv_path)",
            "",
            "Intent examples:",
            "- 'refresh and resend' => SEND_MAIL + NEEDS_SCOPE=YES",
            "- 'refresh and send to me' => SEND_MAIL + SCOPE=ME",
            "- 'resend to all' => SEND_MAIL + SCOPE=ALL",
            "- 'periodic refresh every 10 minutes to all' => DELIVERY_MODE=PERIODIC + INTERVAL_MIN=10 + SCOPE=ALL",
            "- 'recursive refresh resend' => DELIVERY_MODE=PERIODIC + NEEDS_INTERVAL=YES",
            "",
            "Current email report script:",
            "- autobots_agent_scratch_dir/resend_latest_report.py",
            "- Uses mcp_fuse/data_list.log as source of truth.",
        ]
    )


@mcp_fuse.tool()
def parse_data_list_log(file_path: str) -> str:
    """
    Parse one data_list-like log into rows with 3 columns:
    project_name, flow_name, run_path.
    """

    records, error = _read_records_from_file(file_path)
    if error:
        return error

    out: list[str] = []
    out.append(f"Parsed file: {file_path}")
    out.append(f"Total rows: {len(records)}")

    for idx, row in enumerate(records, start=1):
        out.append(
            f"{idx:03d}. {row['project_name']} | {row['flow_name']} | {row['run_path']}"
        )
        out.append(f"     apr_fc/outputs/finish: {row['apr_fc_outputs_finish']}")
        out.append(f"     release/latest/finish: {row['release_latest_finish']}")
        out.append(f"     assembly/outputs: {row['assembly_outputs']}")

    return "\n".join(out)


@mcp_fuse.tool()
def consolidate_data_list_logs(
    file_paths: str,
    output_csv_path: str,
    output_json_path: str = "",
) -> str:
    """
    Consolidate multiple data_list-like logs to CSV (and optional JSON).

    Args:
        file_paths: Comma-separated absolute file paths.
        output_csv_path: Absolute output CSV path.
        output_json_path: Optional absolute output JSON path.
    """

    inputs = [item.strip() for item in file_paths.split(",") if item.strip()]
    if not inputs:
        return "No input files provided."

    all_rows: list[dict[str, str]] = []
    skipped: list[str] = []

    for file_path in inputs:
        rows, error = _read_records_from_file(file_path)
        if error:
            skipped.append(error)
            continue
        all_rows.extend(rows)

    if not all_rows:
        message = ["No rows parsed from input files."]
        if skipped:
            message.append("Skipped:")
            message.extend(f"- {item}" for item in skipped)
        return "\n".join(message)

    out_csv = Path(output_csv_path)
    if out_csv.parent:
        out_csv.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "project_name",
        "flow_name",
        "run_path",
        "apr_fc_outputs_finish",
        "release_latest_finish",
        "assembly_outputs",
        "source_file",
        "line_no",
    ]

    with out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)

    out: list[str] = []
    out.append(f"Consolidated rows: {len(all_rows)}")
    out.append(f"CSV: {out_csv}")

    if output_json_path.strip():
        out_json = Path(output_json_path)
        if out_json.parent:
            out_json.parent.mkdir(parents=True, exist_ok=True)
        with out_json.open("w", encoding="utf-8") as handle:
            json.dump(all_rows, handle, indent=2)
            handle.write("\n")
        out.append(f"JSON: {out_json}")

    if skipped:
        out.append("Skipped:")
        out.extend(f"- {item}" for item in skipped)

    return "\n".join(out)


@mcp_fuse.tool()
def check_run_artifact_md5(
    run_path: str,
    output_csv_path: str = "",
    output_json_path: str = "",
) -> str:
    """
    Check .oas/.sp md5 between:
    - <run_path>/assembly/outputs
    - <run_path>/apr_fc/outputs/finish
    - <run_path>/release/latest/finish
    """

    report = _check_run_path_md5(run_path)
    summary = report["summary"]
    rows = report["rows"]

    out: list[str] = []
    out.append(f"Run path: {report['run_path']}")
    out.append(f"Assembly: {report['assembly_outputs']}")
    out.append(f"Finish A: {report['apr_fc_outputs_finish']}")
    out.append(f"Finish B: {report['release_latest_finish']}")
    out.append(f"Total candidates: {summary['total_candidates']}")
    out.append(f"Present in assembly: {summary['present_in_assembly']}")
    out.append(f"Missing in finish A: {summary['missing_in_finish_a']}")
    out.append(f"Missing in finish B: {summary['missing_in_finish_b']}")
    out.append(f"All three md5 match: {summary['md5_match_all']}")
    out.append(f"MD5 mismatch rows: {summary['md5_mismatch']}")

    if output_csv_path.strip():
        out_csv = Path(output_csv_path)
        if out_csv.parent:
            out_csv.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "relative_path",
            "assembly_exists",
            "finish_a_exists",
            "finish_b_exists",
            "assembly_md5",
            "finish_a_md5",
            "finish_b_md5",
            "assembly_vs_finish_a_match",
            "assembly_vs_finish_b_match",
            "all_three_match",
        ]
        with out_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        out.append(f"CSV: {out_csv}")

    if output_json_path.strip():
        out_json = Path(output_json_path)
        if out_json.parent:
            out_json.parent.mkdir(parents=True, exist_ok=True)
        with out_json.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
            handle.write("\n")
        out.append(f"JSON: {out_json}")

    return "\n".join(out)


@mcp_fuse.tool()
def check_md5_from_data_list(
    data_list_file: str,
    output_dir: str,
) -> str:
    """
    Parse data_list-like file and run md5 checks for each row run_path.
    Writes one CSV per run plus one summary JSON.
    """

    records, error = _read_records_from_file(data_list_file)
    if error:
        return error
    if not records:
        return f"No valid rows in: {data_list_file}"

    out_root = Path(output_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    summary_rows: list[dict[str, object]] = []

    for idx, row in enumerate(records, start=1):
        run_path = row["run_path"]
        report = _check_run_path_md5(run_path)
        short_name = f"row_{idx:03d}"
        csv_path = out_root / f"{short_name}_md5.csv"
        json_path = out_root / f"{short_name}_md5.json"

        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "relative_path",
                    "assembly_exists",
                    "finish_a_exists",
                    "finish_b_exists",
                    "assembly_md5",
                    "finish_a_md5",
                    "finish_b_md5",
                    "assembly_vs_finish_a_match",
                    "assembly_vs_finish_b_match",
                    "all_three_match",
                ],
            )
            writer.writeheader()
            writer.writerows(report["rows"])

        with json_path.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
            handle.write("\n")

        summary_rows.append(
            {
                "row_no": idx,
                "project_name": row["project_name"],
                "flow_name": row["flow_name"],
                "run_path": run_path,
                "total_candidates": report["summary"]["total_candidates"],
                "md5_match_all": report["summary"]["md5_match_all"],
                "md5_mismatch": report["summary"]["md5_mismatch"],
                "missing_in_finish_a": report["summary"]["missing_in_finish_a"],
                "missing_in_finish_b": report["summary"]["missing_in_finish_b"],
                "csv": str(csv_path),
                "json": str(json_path),
            }
        )

    summary_csv = out_root / "md5_summary.csv"
    with summary_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "row_no",
                "project_name",
                "flow_name",
                "run_path",
                "total_candidates",
                "md5_match_all",
                "md5_mismatch",
                "missing_in_finish_a",
                "missing_in_finish_b",
                "csv",
                "json",
            ],
        )
        writer.writeheader()
        writer.writerows(summary_rows)

    return (
        f"Processed rows: {len(summary_rows)}\n"
        f"Summary CSV: {summary_csv}\n"
        f"Output dir: {out_root}"
    )


@mcp_fuse.tool()
def dump_block_artifact_csv(
    data_list_file: str,
    output_csv_path: str,
) -> str:
    """
    Dump one row per <block>.sp / <block>.oas with paths, timestamps, md5 and match flags.
    """

    rows, error = _build_block_artifact_rows(data_list_file)
    if error:
        return error

    out_csv = Path(output_csv_path)
    if out_csv.parent:
        out_csv.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "project",
        "block",
        "file",
        "run_path",
        "assembly_dir",
        "finish_a_dir",
        "finish_b_dir",
        "assembly_file",
        "finish_a_file",
        "finish_b_file",
        "assembly_timestamp",
        "finish_a_timestamp",
        "finish_b_timestamp",
        "assembly_md5",
        "finish_a_md5",
        "finish_b_md5",
        "finish_a_vs_finish_b_match",
        "assembly_vs_finish_a_match",
        "assembly_vs_finish_b_match",
    ]
    with out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return f"CSV: {out_csv}\nRows: {len(rows)}"


@mcp_fuse.tool()
def format_block_artifact_split_tables(data_list_file: str) -> str:
    """
    Return markdown with:
    1) paths at top
    2) timestamp table
    3) gmd5sum table
    """

    rows, error = _build_block_artifact_rows(data_list_file)
    if error:
        return error
    return _build_markdown_split_tables(rows)


@mcp_fuse.tool()
def generate_block_artifact_email_html(
    data_list_file: str,
    output_html_path: str,
) -> str:
    """
    Generate HTML report with paths on top and split timestamp/gmd5 tables.
    """

    rows, error = _build_block_artifact_rows(data_list_file)
    if error:
        return error

    html = _build_html_split_tables(rows)
    out_html = Path(output_html_path)
    if out_html.parent:
        out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8")

    return f"HTML: {out_html}\nRows: {len(rows)}"


@mcp_fuse.tool()
def send_block_artifact_email(
    data_list_file: str,
    to_email: str,
    subject: str = "Block Artifact Report (Split Tables)",
    from_email: str = "",
    smtp_host: str = "localhost",
    smtp_port: int = 25,
    output_html_path: str = "",
    output_eml_path: str = "",
) -> str:
    """
    Build split-table HTML email and send via SMTP.
    """

    rows, error = _build_block_artifact_rows(data_list_file)
    if error:
        return error

    html = _build_html_split_tables(rows)

    sender = from_email.strip()
    if not sender:
        user = getpass.getuser()
        host = socket.getfqdn() or socket.gethostname()
        sender = f"{user}@{host}"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to_email
    msg["Date"] = formatdate(localtime=True)
    msg.set_content(
        "Please view this email in HTML mode for paths, timestamp table, and gmd5sum table."
    )
    msg.add_alternative(html, subtype="html")

    out: list[str] = []

    if output_html_path.strip():
        html_path = Path(output_html_path)
        if html_path.parent:
            html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(html, encoding="utf-8")
        out.append(f"HTML: {html_path}")

    if output_eml_path.strip():
        eml_path = Path(output_eml_path)
        if eml_path.parent:
            eml_path.parent.mkdir(parents=True, exist_ok=True)
        eml_path.write_bytes(msg.as_bytes())
        out.append(f"EML: {eml_path}")

    with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as smtp:
        smtp.send_message(msg)

    out.append(f"Sent: YES via {smtp_host}:{smtp_port}")
    out.append(f"From: {sender}")
    out.append(f"To: {to_email}")
    out.append(f"Rows: {len(rows)}")
    return "\n".join(out)


@mcp_fuse.tool()
def parse_violation_summary_drc(
    file_path: str,
    output_csv_path: str = "",
) -> str:
    """
    Parse <block>.violation_summary_drc and extract rule_name from the
    Description column (text before first ':') after 'Count Description' header.
    """

    rows, error = _parse_violation_summary_drc_file(file_path)
    if error:
        return error

    unique_rules = sorted({row["rule_name"] for row in rows})

    out: list[str] = []
    out.append(f"File: {file_path}")
    out.append(f"Parsed rows: {len(rows)}")
    out.append(f"Unique rules: {len(unique_rules)}")
    out.append(f"Total DRC errors: {rows[0].get('total_errors', '') or 'UNKNOWN'}")

    if output_csv_path.strip():
        out_csv = Path(output_csv_path)
        if out_csv.parent:
            out_csv.parent.mkdir(parents=True, exist_ok=True)
        with out_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["line_no", "count", "description", "rule_name", "total_errors"],
            )
            writer.writeheader()
            writer.writerows(rows)
        out.append(f"CSV: {out_csv}")

    preview_count = min(30, len(unique_rules))
    out.append(f"Rule preview ({preview_count}):")
    out.extend(f"- {name}" for name in unique_rules[:preview_count])

    return "\n".join(out)


@mcp_fuse.tool()
def parse_violation_summary_drc_from_run(
    run_path: str,
    block_name: str,
    output_csv_path: str = "",
) -> str:
    """
    Parse:
      <run_path>/lv_icv/reports/<block_name>.violation_summary_drc
    and extract DRC rule names from Description column.
    """

    run = (run_path or "").strip().rstrip("/")
    block = (block_name or "").strip()
    if not run or not block:
        return "run_path and block_name are required."

    file_path = f"{run}/lv_icv/reports/{block}.violation_summary_drc"
    return parse_violation_summary_drc.fn(file_path, output_csv_path)


@mcp_fuse.tool()
def format_drc_rule_table_from_data_list(
    data_list_file: str,
    output_csv_path: str = "",
) -> str:
    """
    Build a markdown table listing DRC rule_name for each block/run from:
      <run_path>/lv_icv/reports/<block>.violation_summary_drc
    """

    rows, error = _collect_drc_rules_from_data_list(data_list_file)
    if error:
        return error

    block_to_rules: dict[str, set[str]] = {}
    missing_items: list[str] = []
    path_lines: list[str] = []

    for row in rows:
        block = f"{row['block']} ({row['project']})"
        summary_file = row["summary_file"]
        run_path = row["run_path"]
        total_errors = row.get("total_errors", "")
        total_errors_text = total_errors if total_errors else "UNKNOWN"
        path_lines.append(
            f"- {block} ({run_path}): {summary_file} | total_errors={total_errors_text}"
        )

        if block not in block_to_rules:
            block_to_rules[block] = set()

        if row["status"] == "OK":
            for name in row["rule_names"].split("\n"):
                clean = name.strip()
                if clean:
                    block_to_rules[block].add(clean)
        else:
            missing_items.append(f"{block}: missing/nofound")

    blocks = sorted(set(block_to_rules))

    if output_csv_path.strip():
        out_csv = Path(output_csv_path)
        if out_csv.parent:
            out_csv.parent.mkdir(parents=True, exist_ok=True)
        with out_csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["block", "rule_name", "status", "project", "total_errors"],
            )
            writer.writeheader()
            for block in blocks:
                rules = sorted(block_to_rules.get(block, set()))
                matching_row = next((item for item in rows if f"{item['block']} ({item['project']})" == block), None)
                total_errors = matching_row.get("total_errors", "") if matching_row else ""
                if rules:
                    for rule_name in rules:
                        writer.writerow(
                            {
                                "block": block,
                                "rule_name": rule_name,
                                "status": "OK",
                                "project": block.rsplit("(", 1)[-1].rstrip(")") if "(" in block else "",
                                "total_errors": total_errors,
                            }
                        )
            for item in missing_items:
                writer.writerow(
                    {
                        "block": item.split(":", 1)[0],
                        "rule_name": "",
                        "status": "missing/nofound",
                        "project": "",
                        "total_errors": "",
                    }
                )

    out: list[str] = []
    out.append(f"Data list: {data_list_file}")
    if output_csv_path.strip():
        out.append(f"CSV: {output_csv_path}")
    out.append("")
    out.append("DRC output paths:")
    out.extend(path_lines)

    out.append("")
    header = "| " + " | ".join(blocks + ["missing/nofound"]) + " |"
    sep = "| " + " | ".join("---" for _ in (blocks + ["missing/nofound"])) + " |"

    row_cells: list[str] = []
    for block in blocks:
        items: list[str] = []
        rules = sorted(block_to_rules.get(block, set()))
        if rules:
            items.extend(rules)
        cell = "<br>".join(items) if items else "-"
        row_cells.append(cell)

    missing_cell = "<br>".join(missing_items) if missing_items else "-"
    row_cells.append(missing_cell)

    data_row = "| " + " | ".join(row_cells) + " |"
    out.append(header)
    out.append(sep)
    out.append(data_row)

    return "\n".join(out)


if __name__ == "__main__":
    mcp_fuse.run()
