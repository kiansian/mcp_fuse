import csv
import getpass
import hashlib
import json
import os
import re
import smtplib
import socket
import time
from email.message import EmailMessage
from email.utils import formatdate
from html import escape
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import server

DATA_LIST = str(Path(__file__).resolve().parents[1] / "data_list.log")
ARTIFACT_CSV = Path("/nfs/site/disks/zsc11_nvlpcd_00026/kgoh14/AI_TEST/sample_setup_official/vscode_copilot_fusion_compiler/iscp_pch_chatbot/mcp_fuse/autobots_agent_scratch_dir/block_artifact_report.csv")
DRC_CSV = Path("/nfs/site/disks/zsc11_nvlpcd_00026/kgoh14/AI_TEST/sample_setup_official/vscode_copilot_fusion_compiler/iscp_pch_chatbot/mcp_fuse/autobots_agent_scratch_dir/drc_rule_table.csv")
CONSOLIDATED_CSV = Path("/nfs/site/disks/zsc11_nvlpcd_00026/kgoh14/AI_TEST/sample_setup_official/vscode_copilot_fusion_compiler/iscp_pch_chatbot/mcp_fuse/autobots_agent_scratch_dir/consolidated_data_list.csv")
CONSOLIDATED_JSON = Path("/nfs/site/disks/zsc11_nvlpcd_00026/kgoh14/AI_TEST/sample_setup_official/vscode_copilot_fusion_compiler/iscp_pch_chatbot/mcp_fuse/autobots_agent_scratch_dir/consolidated_data_list.json")
HTML_PATH = Path("/nfs/site/disks/zsc11_nvlpcd_00026/kgoh14/AI_TEST/sample_setup_official/vscode_copilot_fusion_compiler/iscp_pch_chatbot/mcp_fuse/autobots_agent_scratch_dir/block_artifact_report_with_drc_email.html")
EML_PATH = Path("/nfs/site/disks/zsc11_nvlpcd_00026/kgoh14/AI_TEST/sample_setup_official/vscode_copilot_fusion_compiler/iscp_pch_chatbot/mcp_fuse/autobots_agent_scratch_dir/block_artifact_report_with_drc_email_to_kian.eml")
STATE_PATH = Path("/nfs/site/disks/zsc11_nvlpcd_00026/kgoh14/AI_TEST/sample_setup_official/vscode_copilot_fusion_compiler/iscp_pch_chatbot/mcp_fuse/autobots_agent_scratch_dir/.last_send_state.json")
DEDUP_COOLDOWN_SECONDS = 1800
TO_ADDRS = [
    "kian.sian.goh@intel.com",
    "alice.koh.chee@intel.com",
    "Jian.Yan.Yeoh@intel.com",
    "khai.juan.tan@intel.com",
    "Wooi.Phang.Tan@intel.com",
    "george.pek.jing.ting@intel.com",
]

to_addrs_env = os.environ.get("TO_ADDRS", "").strip()
if to_addrs_env:
    TO_ADDRS = [item.strip() for item in to_addrs_env.split(",") if item.strip()]

# Always refresh source data from mcp_fuse/data_list.log
server.consolidate_data_list_logs.fn(DATA_LIST, str(CONSOLIDATED_CSV), str(CONSOLIDATED_JSON))
server.dump_block_artifact_csv.fn(DATA_LIST, str(ARTIFACT_CSV))
server.format_drc_rule_table_from_data_list.fn(DATA_LIST, str(DRC_CSV))

records, err = server._read_records_from_file(DATA_LIST)
if err:
    raise RuntimeError(err)
print(f"DATA_LIST={DATA_LIST}")
print(f"RUN_ROWS={len(records)}")
for idx, row in enumerate(records, start=1):
    print(f"RUN_{idx:02d}={row['run_path']}")

artifact_rows = list(csv.DictReader(ARTIFACT_CSV.open()))
artifact_rows.sort(key=lambda r: (r["block"], r.get("run_path", ""), 0 if r["file"].endswith(".sp") else 1, r["file"]))

seen = set()
path_groups = []
for row in artifact_rows:
    key = (row["block"], row.get("run_path", ""))
    if key in seen:
        continue
    seen.add(key)
    path_groups.append((row["block"], row.get("run_path", ""), row["assembly_dir"], row["finish_a_dir"], row["finish_b_dir"]))

rows, error = server._collect_drc_rules_from_data_list(DATA_LIST)
if error:
    raise RuntimeError(error)

block_to_rules = {}
block_to_total_errors = {}
missing_items = []
path_lines = []
for row in rows:
    block = f"{row['block']} ({row['project']})"
    total_errors = str(row.get("total_errors", "")).strip() or "UNKNOWN"
    path_lines.append(f"{block} ({row['run_path']}): {row['summary_file']} | total_errors={total_errors}")
    block_to_rules.setdefault(block, set())
    block_to_total_errors.setdefault(block, total_errors)
    if row["status"] == "OK":
        for name in row["rule_names"].split("\n"):
            clean = name.strip()
            if clean:
                block_to_rules[block].add(clean)
    else:
        if total_errors != "0":
            missing_items.append(f"{block}: missing/nofound")

blocks = sorted(set(block_to_rules))

ts_headers = ["project", "block", "file", "assembly_timestamp", "finish_a_timestamp", "finish_b_timestamp"]
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


def _cell_class(header, value):
    text = str(value or "").strip()
    text_lc = text.lower()
    if header == "lvs_status" and text_lc == "pass":
        return "pass"
    if header == "fev_status" and text_lc == "design_equal":
        return "clean"
    if header == "fev_status" and text_lc in {"not_equal", "not_clean", "dirty", "fail"}:
        return "mismatch"
    if header == "fev_status" and text_lc == "missing":
        return "missing"
    if header == "total_drc_count" and text == "0":
        return "clean"
    if header == "total_antenna_count" and text == "0":
        return "clean"
    if header == "total_density_count" and text == "0":
        return "clean"
    if header == "total_lu_count" and text == "0":
        return "clean"
    if header == "status" and text_lc == "clean":
        return "clean"
    if text_lc in {"missing", "missing/nofound", "nofound", "not found"}:
        return "missing"
    if header in {
        "assembly_vs_finish_a_match",
        "assembly_vs_finish_b_match",
        "finish_a_vs_finish_b_match",
    } and text == "NO":
        return "mismatch"
    return ""


def build_table(headers, data_rows):
    head_cells = []
    for h in headers:
        if h == "partition":
            head_cells.append(
                "<th style='max-width:320px;width:320px;white-space:normal;overflow-wrap:anywhere;word-break:break-word;'>"
                f"{escape(h)}"
                "</th>"
            )
        else:
            head_cells.append(f"<th>{escape(h)}</th>")
    head = "".join(head_cells)
    body_rows = []
    for row in data_rows:
        tds = []
        for header in headers:
            raw = str(row.get(header, ""))
            cls = _cell_class(header, raw)
            cls_attr = f" class='{cls}'" if cls else ""
            if header == "partition":
                tds.append(
                    "<td"
                    f"{cls_attr}"
                    " style='max-width:320px;width:320px;white-space:normal;overflow-wrap:anywhere;word-break:break-word;'>"
                    f"{escape(raw)}"
                    "</td>"
                )
            elif header == "layer_use_list":
                rendered = escape(raw).replace("\n", "<br>")
                tds.append(
                    "<td"
                    f"{cls_attr}"
                    " style='max-width:720px;overflow-wrap:anywhere;word-break:break-word;'>"
                    f"{rendered}"
                    "</td>"
                )
            else:
                tds.append(f"<td{cls_attr}>{escape(raw)}</td>")
        body_rows.append("<tr>" + "".join(tds) + "</tr>")
    body = "".join(body_rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


drc_head = "".join(
    f"<th>{escape(b)}<br><span style='font-weight:normal'>Errors={escape(block_to_total_errors.get(b, 'UNKNOWN'))}</span></th>"
    for b in blocks
)
drc_cells = []
for block in blocks:
    items = sorted(block_to_rules.get(block, set()))
    total_errors = str(block_to_total_errors.get(block, "")).strip()
    if total_errors == "0":
        drc_cells.append("<td class='clean'>CLEAN</td>")
    elif items:
        cell = "<br>".join(escape(item) for item in items)
        drc_cells.append(f"<td>{cell}</td>")
    else:
        drc_cells.append("<td class='missing'>MISSING</td>")

missing_cell = "<br>".join(escape(item) for item in missing_items) if missing_items else "-"
drc_head = drc_head + "<th>missing/nofound</th>"
missing_attr = " class='missing'" if missing_items else ""
drc_cells.append(f"<td{missing_attr}>{missing_cell}</td>")
drc_table = f"<table><thead><tr>{drc_head}</tr></thead><tbody><tr>{''.join(drc_cells)}</tr></tbody></table>"

path_html = "".join(
    f"<div><b>{escape(block)}</b><br>Run path: {escape(run_path)}<br>Assembly path: {escape(assembly)}<br>Finish A path: {escape(finish_a)}<br>Finish B path: {escape(finish_b)}</div><br>"
    for block, run_path, assembly, finish_a, finish_b in path_groups
)

drc_paths_html = "".join(f"<li>{escape(line)}</li>" for line in path_lines)

lvs_headers = ["project", "block", "partition", "lvs_status", "lvs_errors_file", "details"]
lvs_rows = []
seen_lvs = set()
for row in rows:
    key = (row.get("project", ""), row.get("block", ""), row.get("run_path", ""))
    if key in seen_lvs:
        continue
    seen_lvs.add(key)

    run_path = str(row.get("run_path", "")).rstrip("/")
    block_name = row.get("block", "")
    lvs_errors_file = f"{run_path}/lv_icv/outputs/lvs/{block_name}.LVS_ERRORS"

    lvs_status = "FAIL"
    details = "PASS marker not found"
    lvs_path = Path(lvs_errors_file)
    if not lvs_path.exists() or not lvs_path.is_file():
        lvs_status = "MISSING"
        details = "file not found"
    else:
        lvs_text = lvs_path.read_text(encoding="utf-8", errors="ignore")
        if "Final comparison result:PASS" in lvs_text:
            lvs_status = "PASS"
            details = "found exact marker"

    lvs_rows.append(
        {
            "project": row.get("project", ""),
            "block": block_name,
            "partition": run_path,
            "lvs_status": lvs_status,
            "lvs_errors_file": lvs_errors_file,
            "details": details,
        }
    )

lvs_rows.sort(key=lambda item: (item["project"], item["block"], item["partition"]))
lvs_table = build_table(lvs_headers, lvs_rows)

drc_total_headers = ["project", "block", "partition", "total_drc_count", "status"]
drc_total_rows = []
seen_partitions = set()
for row in rows:
    key = (row.get("project", ""), row.get("block", ""), row.get("run_path", ""))
    if key in seen_partitions:
        continue
    seen_partitions.add(key)
    total_errors = str(row.get("total_errors", "")).strip() or "UNKNOWN"
    summary_status = row.get("status", "")
    if total_errors == "0":
        summary_status = "CLEAN"
    drc_total_rows.append(
        {
            "project": row.get("project", ""),
            "block": row.get("block", ""),
            "partition": row.get("run_path", ""),
            "total_drc_count": total_errors,
            "status": summary_status,
        }
    )

drc_total_rows.sort(key=lambda item: (item["project"], item["block"], item["partition"]))
drc_total_table = build_table(drc_total_headers, drc_total_rows)

antenna_headers = ["project", "block", "partition", "total_antenna_count", "status"]
antenna_rows = []
seen_antenna = set()
for row in rows:
    key = (row.get("project", ""), row.get("block", ""), row.get("run_path", ""))
    if key in seen_antenna:
        continue
    seen_antenna.add(key)

    project = row.get("project", "")
    block_name = row.get("block", "")
    run_path = str(row.get("run_path", "")).rstrip("/")
    antenna_summary_file = f"{run_path}/lv_icv/reports/{block_name}.violation_summary_antenna"

    antenna_total = "UNKNOWN"
    antenna_status = "UNKNOWN"
    summary_path = Path(antenna_summary_file)
    if not summary_path.exists() or not summary_path.is_file():
        antenna_status = "MISSING"
    else:
        parsed_total = server._parse_violation_summary_total_errors(
            antenna_summary_file,
            block_name=block_name,
            flow_name="antenna",
        )
        if parsed_total:
            antenna_total = parsed_total
            antenna_status = "CLEAN" if parsed_total == "0" else "DIRTY"

    antenna_rows.append(
        {
            "project": project,
            "block": block_name,
            "partition": run_path,
            "total_antenna_count": antenna_total,
            "status": antenna_status,
        }
    )

antenna_rows.sort(key=lambda item: (item["project"], item["block"], item["partition"]))
antenna_table = build_table(antenna_headers, antenna_rows)

density_headers = ["project", "block", "partition", "total_density_count", "status"]
density_rows = []
seen_density = set()
for row in rows:
    key = (row.get("project", ""), row.get("block", ""), row.get("run_path", ""))
    if key in seen_density:
        continue
    seen_density.add(key)

    project = row.get("project", "")
    block_name = row.get("block", "")
    run_path = str(row.get("run_path", "")).rstrip("/")
    density_summary_file = f"{run_path}/lv_icv/reports/{block_name}.violation_summary_density"

    density_total = "UNKNOWN"
    density_status = "UNKNOWN"
    summary_path = Path(density_summary_file)
    if not summary_path.exists() or not summary_path.is_file():
        density_status = "MISSING"
    else:
        parsed_total = server._parse_violation_summary_total_errors(
            density_summary_file,
            block_name=block_name,
            flow_name="density",
        )
        if parsed_total:
            density_total = parsed_total
            density_status = "CLEAN" if parsed_total == "0" else "DIRTY"

    density_rows.append(
        {
            "project": project,
            "block": block_name,
            "partition": run_path,
            "total_density_count": density_total,
            "status": density_status,
        }
    )

density_rows.sort(key=lambda item: (item["project"], item["block"], item["partition"]))
density_table = build_table(density_headers, density_rows)

lu_headers = ["project", "block", "partition", "total_lu_count", "status"]
lu_rows = []
seen_lu = set()
for row in rows:
    key = (row.get("project", ""), row.get("block", ""), row.get("run_path", ""))
    if key in seen_lu:
        continue
    seen_lu.add(key)

    project = row.get("project", "")
    block_name = row.get("block", "")
    run_path = str(row.get("run_path", "")).rstrip("/")
    lu_summary_file = f"{run_path}/lv_icv/reports/{block_name}.violation_summary_lu"

    lu_total = "UNKNOWN"
    lu_status = "UNKNOWN"
    summary_path = Path(lu_summary_file)
    if not summary_path.exists() or not summary_path.is_file():
        lu_status = "MISSING"
    else:
        parsed_total = server._parse_violation_summary_total_errors(
            lu_summary_file,
            block_name=block_name,
            flow_name="lu",
        )
        if parsed_total:
            lu_total = parsed_total
            lu_status = "CLEAN" if parsed_total == "0" else "DIRTY"

    lu_rows.append(
        {
            "project": project,
            "block": block_name,
            "partition": run_path,
            "total_lu_count": lu_total,
            "status": lu_status,
        }
    )

lu_rows.sort(key=lambda item: (item["project"], item["block"], item["partition"]))
lu_table = build_table(lu_headers, lu_rows)

xor_entries = []
for raw_line in Path(DATA_LIST).read_text(encoding="utf-8", errors="ignore").splitlines():
    text = raw_line.strip()
    if not text or text.startswith("#"):
        continue
    parts = text.split()
    if len(parts) < 4:
        continue
    if parts[0].upper() != "XOR":
        continue
    xor_entries.append(
        {
            "project": parts[1],
            "block": parts[2],
            "xor_log_file": " ".join(parts[3:]),
        }
    )

xor_rows = []
xor_paths = []
expected_partitions = []
seen_expected_partitions = set()
run_path_by_partition = {}
for rec in records:
    partition_name = f"{rec.get('project_name', '')}_{rec.get('flow_name', '')}"
    if partition_name in seen_expected_partitions:
        continue
    seen_expected_partitions.add(partition_name)
    expected_partitions.append(partition_name)
    run_path_by_partition[partition_name] = str(rec.get("run_path", "")).rstrip("/")

xor_by_partition = {}
for item in xor_entries:
    log_path = Path(item["xor_log_file"])
    partition_name = f"{item['project']}_{item['block']}"
    xor_paths.append(f"{partition_name}: {item['xor_log_file']}")
    if not log_path.exists() or not log_path.is_file():
        xor_by_partition[partition_name] = {
            "partition": partition_name,
            "xor_status": "MISSING",
            "layer_use_count": "0",
            "layer_use_list": "file not found",
        }
        continue

    layer_uses = []
    seen_layer_uses = set()
    for raw in log_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = re.match(r"^\s*#\d+:\s+\d+\s+\d+\s+(\S+)\s*$", raw)
        if not m:
            continue
        layer_use = m.group(1).strip()
        if not layer_use or layer_use in seen_layer_uses:
            continue
        seen_layer_uses.add(layer_use)
        layer_uses.append(layer_use)

    layer_uses.sort()
    xor_by_partition[partition_name] = {
        "partition": partition_name,
        "xor_status": "OK",
        "layer_use_count": str(len(layer_uses)),
        "layer_use_list": "\n".join(layer_uses),
    }

xor_partitions = expected_partitions[:]
if not xor_partitions:
    xor_partitions = sorted(xor_by_partition.keys())

for partition_name in xor_partitions:
    row = xor_by_partition.get(partition_name)
    if row is None:
        row = {
            "partition": partition_name,
            "xor_status": "NOT_READY",
            "layer_use_count": "N/A",
            "layer_use_list": "not ready (no XOR line in data_list.log)",
        }
    xor_rows.append(row)

xor_paths_html = "".join(f"<li>{escape(line)}</li>" for line in xor_paths)

xor_total_row = []
xor_layers_row = []
for row in xor_rows:
    count_text = str(row.get("layer_use_count", "0"))
    layers_text = str(row.get("layer_use_list", ""))
    status_text = str(row.get("xor_status", "")).strip().upper()
    is_unavailable = status_text in {"MISSING", "NOT_READY"}
    count_cls = " class='missing'" if is_unavailable else ""
    layers_cls = " class='missing'" if is_unavailable else ""

    xor_total_row.append(f"<td{count_cls}>Total layer: {escape(count_text)}</td>")
    xor_layers_row.append(
        "<td"
        f"{layers_cls}"
        " style='max-width:720px;overflow-wrap:anywhere;word-break:break-word;'>"
        f"{escape(layers_text).replace('\n', '<br>')}"
        "</td>"
    )

xor_head = "".join(f"<th>{escape(partition)}</th>" for partition in xor_partitions)
xor_table = (
    f"<table><thead><tr>{xor_head}</tr></thead><tbody>"
    f"<tr>{''.join(xor_total_row)}</tr>"
    f"<tr>{''.join(xor_layers_row)}</tr>"
    "</tbody></table>"
)

if xor_rows:
    xor_section_html = f"<ul>{xor_paths_html}</ul>{xor_table}"
else:
    xor_section_html = "<div>No active XOR entries in data_list.log (removed/commented).</div>"

fev_headers = ["project", "block", "partition", "fev_status", "fev_results_file", "details"]
fev_rows = []
seen_fev = set()
for row in rows:
    key = (row.get("project", ""), row.get("block", ""), row.get("run_path", ""))
    if key in seen_fev:
        continue
    seen_fev.add(key)

    project = row.get("project", "")
    block_name = row.get("block", "")
    run_path = str(row.get("run_path", "")).rstrip("/")
    fev_results_file = f"{run_path}/fev_conformal/fev_noconst/fev_results.log"

    fev_status = "UNKNOWN"
    details = "marker not found"
    fev_path = Path(fev_results_file)
    if not fev_path.exists() or not fev_path.is_file():
        fev_status = "MISSING"
        details = "file not found"
    else:
        fev_text = fev_path.read_text(encoding="utf-8", errors="ignore")
        equal_match = re.search(r"(?im)^.*DESIGNS\s+EQUAL.*$", fev_text)
        not_equal_match = re.search(r"(?im)^.*DESIGNS\s+NOT\s+EQUAL.*$", fev_text)
        if equal_match:
            fev_status = "DESIGN_EQUAL"
            details = equal_match.group(0).strip()
        elif not_equal_match:
            fev_status = "NOT_EQUAL"
            details = not_equal_match.group(0).strip()

    fev_rows.append(
        {
            "project": project,
            "block": block_name,
            "partition": run_path,
            "fev_status": fev_status,
            "fev_results_file": fev_results_file,
            "details": details,
        }
    )

fev_rows.sort(key=lambda item: (item["project"], item["block"], item["partition"]))
fev_table = build_table(fev_headers, fev_rows)

runtime_partitions = expected_partitions[:]
if not runtime_partitions:
    runtime_partitions = sorted(run_path_by_partition.keys())

elapsed_pat = re.compile(
    r"Elapsed time for this session:\s*(\d+)\s*seconds\s*\(\s*([0-9.]+)\s*hours\)",
    re.IGNORECASE,
)
star_runtime_pat = re.compile(r"([0-9]+\s*s\s*\([0-9]+:[0-9]{2}:[0-9]{2}\))")
fev_elapse_seconds_pat = re.compile(r"Elapse\s*time\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*seconds?", re.IGNORECASE)
fev_elapsed_seconds_pat = re.compile(r"Elapsed\s*Time\s*\|\s*([0-9]+(?:\.[0-9]+)?)\s*sec", re.IGNORECASE)
lv_icv_overall_time_pat = re.compile(r"Overall\s*engine\s*Time\s*=\s*([^\s].*)", re.IGNORECASE)

apr_finish_elapsed_by_partition = {}
star_pv_runtime_by_partition = {}
fev_lec_runtime_by_partition = {}
lv_icv_runtime_by_partition = {}
lv_icv_run_names = []
seen_lv_icv_run_names = set()

for partition_name in runtime_partitions:
    run_path = run_path_by_partition.get(partition_name, "")
    if not run_path:
        apr_finish_elapsed_by_partition[partition_name] = "MISSING_RUN_PATH"
        star_pv_runtime_by_partition[partition_name] = "MISSING_RUN_PATH"
        fev_lec_runtime_by_partition[partition_name] = "MISSING_RUN_PATH"
        lv_icv_runtime_by_partition[partition_name] = {}
        continue

    finish_candidates = [
        Path(run_path) / "apr_fc/logs/fc.finish.log",
        Path(run_path) / "apr_fc/logs/fc.apreco_finish.log",
    ]
    apr_elapsed_text = "MISSING_LOG"
    for finish_log in finish_candidates:
        if not finish_log.exists() or not finish_log.is_file():
            continue
        apr_elapsed_text = "ELAPSED_NOT_FOUND"
        finish_text = finish_log.read_text(encoding="utf-8", errors="ignore")
        matches = list(elapsed_pat.finditer(finish_text))
        if matches:
            seconds, hours = matches[-1].groups()
            apr_elapsed_text = f"{seconds} seconds ({hours} hours)"
            break
    apr_finish_elapsed_by_partition[partition_name] = apr_elapsed_text

    star_log = Path(run_path) / "extraction/logs/star_pv/star_pv.log"
    if not star_log.exists() or not star_log.is_file():
        star_pv_runtime_by_partition[partition_name] = "MISSING_LOG"
    else:
        star_runtime_text = "KEYWORD_NOT_FOUND"
        star_lines = star_log.read_text(encoding="utf-8", errors="ignore").splitlines()
        for line in reversed(star_lines):
            if "All looks okay" not in line:
                continue
            line_text = line.strip()
            runtime_match = star_runtime_pat.search(line_text)
            if runtime_match:
                star_runtime_text = runtime_match.group(1)
            else:
                star_runtime_text = "RUNTIME_PARSE_FAIL"
            break
        star_pv_runtime_by_partition[partition_name] = star_runtime_text

    fev_lec_log = Path(run_path) / "fev_conformal/fev_noconst/logs/lec.log"
    if not fev_lec_log.exists() or not fev_lec_log.is_file():
        fev_lec_runtime_by_partition[partition_name] = "MISSING_LOG"
    else:
        fev_runtime_text = "ELAPSE_NOT_FOUND"
        fev_lines = fev_lec_log.read_text(encoding="utf-8", errors="ignore").splitlines()

        sec_value = None
        for line in reversed(fev_lines):
            m = fev_elapse_seconds_pat.search(line)
            if m:
                sec_value = float(m.group(1))
                break

        if sec_value is None:
            for line in reversed(fev_lines):
                m = fev_elapsed_seconds_pat.search(line)
                if m:
                    sec_value = float(m.group(1))
                    break

        if sec_value is not None:
            mins = sec_value / 60.0
            sec_text = str(int(sec_value)) if sec_value.is_integer() else f"{sec_value:.2f}"
            fev_runtime_text = f"{mins:.2f} mins ({sec_text} sec)"

        fev_lec_runtime_by_partition[partition_name] = fev_runtime_text

    lv_map = {}
    lv_logs_root = Path(run_path) / "lv_icv/logs"
    if lv_logs_root.exists() and lv_logs_root.is_dir():
        for icv_log in sorted(lv_logs_root.glob("*/icv.log")):
            run_name = icv_log.parent.name
            if run_name not in seen_lv_icv_run_names:
                seen_lv_icv_run_names.add(run_name)
                lv_icv_run_names.append(run_name)

            line_found = None
            for log_line in reversed(icv_log.read_text(encoding="utf-8", errors="ignore").splitlines()):
                if "Overall engine Time" in log_line:
                    line_found = log_line.strip()
                    break

            if line_found is None:
                lv_map[run_name] = "KEYWORD_NOT_FOUND"
            else:
                m = lv_icv_overall_time_pat.search(line_found)
                lv_map[run_name] = m.group(1).strip() if m else line_found

    lv_icv_runtime_by_partition[partition_name] = lv_map

runtime_head = "<th>run_check</th>" + "".join(
    f"<th>{escape(partition)}</th>" for partition in runtime_partitions
)

def _runtime_cell(text: str) -> str:
    value = str(text or "")
    cls = " class='missing'" if value in {
        "MISSING_LOG",
        "MISSING_RUN_PATH",
        "ELAPSED_NOT_FOUND",
        "ELAPSE_NOT_FOUND",
        "KEYWORD_NOT_FOUND",
        "RUNTIME_PARSE_FAIL",
    } else ""
    return f"<td{cls}>{escape(value)}</td>"

runtime_row_apr = "".join(
    _runtime_cell(apr_finish_elapsed_by_partition.get(partition, "MISSING"))
    for partition in runtime_partitions
)
runtime_row_star = "".join(
    _runtime_cell(star_pv_runtime_by_partition.get(partition, "MISSING"))
    for partition in runtime_partitions
)
runtime_row_fev = "".join(
    _runtime_cell(fev_lec_runtime_by_partition.get(partition, "MISSING"))
    for partition in runtime_partitions
)

runtime_table = (
    f"<table><thead><tr>{runtime_head}</tr></thead><tbody>"
    f"<tr><td><b>APR_FC_FINISH_ELAPSED</b></td>{runtime_row_apr}</tr>"
    f"<tr><td><b>FEV_LEC_ELAPSE_MIN</b></td>{runtime_row_fev}</tr>"
    f"<tr><td><b>STAR_PV_ALL_LOOKS_OKAY_RUNTIME</b></td>{runtime_row_star}</tr>"
    "</tbody></table>"
)

extract_quality_metric_order = [
    "Starrcxt Version",
    "Nxtgrd Version",
    "grdgenxo Version",
    "Local ivar Overrides",
    "Starrcxt Command Overrides",
    "Total number of nets",
    "Percent of completely good nets (w/o open/short/smin)",
    "Total number of nets with opens in spef",
    "Total number of nets with opens from StarRC",
    "Total number of nets with shorts",
    "Total number of nets with smin violations",
    "Total number of zones with shorts",
    "Total number of zones with smin violations",
    "Total number of nets with multiple entries",
    "Total number of nets with estimation",
    "Total number of nets with large res values",
    "Total number of nets with zero res values",
    "Total number of nets connected to mpins",
    "Total number of nets connected to long ports",
]

extract_quality_by_partition = {}
extract_quality_line_pat = re.compile(r"^\s*(.+?)\s*:\s*(.*?)\s*$")
for rec in records:
    partition_name = f"{rec.get('project_name', '')}_{rec.get('flow_name', '')}"
    run_path = str(rec.get("run_path", "")).rstrip("/")
    block_name = str(rec.get("flow_name", "")).strip()
    report_path = Path(run_path) / f"extraction/reports/star_pv/{block_name}.extract_quality.report"

    values = {metric: "MISSING_REPORT" for metric in extract_quality_metric_order}
    if report_path.exists() and report_path.is_file():
        text = report_path.read_text(encoding="utf-8", errors="ignore")
        parsed = {}
        for line in text.splitlines():
            m = extract_quality_line_pat.match(line)
            if not m:
                continue
            key = m.group(1).strip()
            val = m.group(2).strip()
            parsed[key] = val
        for metric in extract_quality_metric_order:
            if metric in parsed:
                values[metric] = parsed[metric]

    extract_quality_by_partition[partition_name] = values

extract_quality_head = "<th>metric</th>" + "".join(
    f"<th>{escape(partition)}</th>" for partition in runtime_partitions
)

extract_quality_rows_html = []
for metric in extract_quality_metric_order:
    cells = []
    for partition in runtime_partitions:
        metric_value = extract_quality_by_partition.get(partition, {}).get(metric, "MISSING_REPORT")
        cells.append(_runtime_cell(metric_value))
    extract_quality_rows_html.append(
        f"<tr><td><b>{escape(metric)}</b></td>{''.join(cells)}</tr>"
    )

extract_quality_table = (
    f"<table><thead><tr>{extract_quality_head}</tr></thead><tbody>"
    f"{''.join(extract_quality_rows_html)}"
    "</tbody></table>"
)

lv_icv_head = "<th>run_name</th>" + "".join(
    f"<th>{escape(partition)}</th>" for partition in runtime_partitions
)

lv_icv_rows_html = []
for run_name in lv_icv_run_names:
    row_cells = []
    for partition in runtime_partitions:
        part_map = lv_icv_runtime_by_partition.get(partition, {})
        value = part_map.get(run_name, "MISSING_LOG")
        row_cells.append(_runtime_cell(value))
    lv_icv_rows_html.append(f"<tr><td><b>{escape(run_name)}</b></td>{''.join(row_cells)}</tr>")

if lv_icv_rows_html:
    lv_icv_runtime_table = (
        f"<table><thead><tr>{lv_icv_head}</tr></thead><tbody>"
        f"{''.join(lv_icv_rows_html)}"
        "</tbody></table>"
    )
else:
    lv_icv_runtime_table = "<div>No lv_icv/logs/*/icv.log found.</div>"

html = f"""<!doctype html>
<html><head><meta charset='utf-8' />
<style>
body {{ font-family: Arial, sans-serif; font-size: 13px; color: #222; }}
h2 {{ margin: 0 0 10px 0; }}
h3 {{ margin: 18px 0 10px 0; font-size: 26px; font-weight: 700; color: #1f4e9e; }}
table {{ border-collapse: collapse; width: 100%; margin: 8px 0 24px; }}
th, td {{ border: 1px solid #d0d7de; padding: 6px 8px; text-align: left; vertical-align: top; }}
th {{ background: #f6f8fa; }}
tr:nth-child(even) {{ background: #fbfbfb; }}
.pass {{ color: #067647; font-weight: 900; font-size: 26px; }}
.clean {{ color: #067647; font-weight: 900; font-size: 26px; }}
.missing {{ color: #b42318; font-weight: 600; }}
.mismatch {{ color: #b54708; font-weight: 600; }}
</style></head><body>
<h2>Block Artifact + DRC Rule Report</h2>
<h3>Paths</h3>
{path_html}
<h3>Timestamps</h3>
{build_table(ts_headers, artifact_rows)}
<h3>gmd5sum</h3>
{build_table(md5_headers, artifact_rows)}
<h3>Run-check runtime matrix</h3>
{runtime_table}
<h3>StarPV extract quality summary</h3>
{extract_quality_table}
<h3>LV ICV runtime by run</h3>
{lv_icv_runtime_table}
<h3>DRC output paths</h3>
<ul>{drc_paths_html}</ul>
<h3>LVS status by partition</h3>
{lvs_table}
<h3>DRC total count by partition</h3>
{drc_total_table}
<h3>Antenna total count by partition</h3>
{antenna_table}
<h3>Density total count by partition</h3>
{density_table}
<h3>LU total count by partition</h3>
{lu_table}
<h3>FEV status by partition</h3>
{fev_table}
<h3>DRC errors/rules by block</h3>
{drc_table}
<h3>XOR layer-use by partition</h3>
{xor_section_html}
</body></html>"""

HTML_PATH.write_text(html, encoding="utf-8")


def _normalized_recipients(addrs):
    return sorted(a.strip().lower() for a in addrs if a and a.strip())


def _build_send_signature(subject, recipients, html_content):
    payload = {
        "subject": subject,
        "to": _normalized_recipients(recipients),
        "html_sha256": hashlib.sha256(html_content.encode("utf-8")).hexdigest(),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _read_last_state(path):
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_last_state(path, state):
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


SUBJECT = "NVLPCD* PAR*FUSE metal ECO final status"
force_send = os.environ.get("FORCE_SEND", "").strip().lower() in {"1", "true", "yes", "y"}
send_sig = _build_send_signature(SUBJECT, TO_ADDRS, html)
now_ts = int(time.time())
last_state = _read_last_state(STATE_PATH)
last_sig = str(last_state.get("signature", ""))
last_ts = int(last_state.get("sent_at", 0) or 0)
within_cooldown = (now_ts - last_ts) < DEDUP_COOLDOWN_SECONDS
is_duplicate = (send_sig == last_sig) and within_cooldown

from_addr = f"{getpass.getuser()}@{(socket.getfqdn() or socket.gethostname())}"
msg = EmailMessage()
msg["Subject"] = SUBJECT
msg["From"] = from_addr
msg["To"] = ", ".join(TO_ADDRS)
msg["Date"] = formatdate(localtime=True)
msg.set_content("Please view this email in HTML mode.")
msg.add_alternative(html, subtype="html")
EML_PATH.write_bytes(msg.as_bytes())

if is_duplicate and not force_send:
    print("SENT=SKIPPED_DUPLICATE")
    print(f"SKIP_REASON=same content+recipients within {DEDUP_COOLDOWN_SECONDS}s cooldown")
else:
    with smtplib.SMTP("localhost", 25, timeout=10) as smtp:
        smtp.send_message(msg)
    _write_last_state(
        STATE_PATH,
        {
            "sent_at": now_ts,
            "signature": send_sig,
            "subject": SUBJECT,
            "to": _normalized_recipients(TO_ADDRS),
            "html_sha256": hashlib.sha256(html.encode("utf-8")).hexdigest(),
        },
    )
    print("SENT=YES")

print(f"TO={msg['To']}")
print(f"HTML={HTML_PATH}")
print(f"EML={EML_PATH}")
print(f"DRC_BLOCK_COLUMNS={len(blocks)}")
