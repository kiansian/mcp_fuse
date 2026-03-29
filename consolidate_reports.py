#!/usr/bin/env python3
"""
consolidate_reports.py — MCP FUSE report consolidation tool
SPE ISCK CKT team, metal ECO data mining feature.

Consolidates full reports across all configured partitions (e.g. NVL S and H),
computes md5 checksums (gmd5sum), and writes detailed per-partition and
consolidated summary reports.

Usage:
    python consolidate_reports.py [--config CONFIG] [--output-dir DIR]
                                  [--golden GOLDEN_MD5_FILE] [--groups GROUP ...]
                                  [--partitions PARTITION ...] [--verbose]

Examples:
    # Consolidate all groups defined in config.yaml
    python consolidate_reports.py

    # Consolidate only NVL_S and NVL_H groups
    python consolidate_reports.py --groups NVL_S NVL_H

    # Consolidate with golden md5sum verification
    python consolidate_reports.py --golden golden.md5

    # Use a custom config and output directory
    python consolidate_reports.py --config my_config.yaml --output-dir /tmp/reports
"""

import argparse
import glob
import hashlib
import json
import logging
import os
import sys
import textwrap
from datetime import datetime, timezone
from typing import Dict, List, Optional

try:
    import yaml
except ImportError:
    print("ERROR: PyYAML is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPORT_FILENAME = "consolidated_report.txt"
JSON_FILENAME = "consolidated_report.json"
MD5_FILENAME = "consolidated.md5"
MD5_MISMATCH_MARKER = "MISMATCH"
MD5_MATCH_MARKER = "OK"
MD5_MISSING_MARKER = "MISSING"

# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def compute_md5(path: str) -> str:
    """Return the hex-digest MD5 checksum of the file at *path*."""
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def load_golden_md5(golden_path: str) -> Dict[str, str]:
    """Parse a golden md5sum file (``<hash>  <filename>`` format).

    Returns a mapping of *basename* → *expected_hash*.
    """
    golden: Dict[str, str] = {}
    if not os.path.isfile(golden_path):
        log.warning("Golden md5 file not found: %s", golden_path)
        return golden
    with open(golden_path) as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(None, 1)
            if len(parts) != 2:
                log.warning("Skipping malformed line %d in %s: %r", lineno, golden_path, line)
                continue
            digest, filename = parts
            golden[os.path.basename(filename.strip())] = digest.lower()
    return golden


# ---------------------------------------------------------------------------
# Core consolidation logic
# ---------------------------------------------------------------------------

def collect_partition_files(partition: dict, base_dir: str) -> List[str]:
    """Return sorted list of absolute paths matching *partition*'s report glob."""
    part_path = os.path.join(base_dir, partition["path"])
    pattern = os.path.join(part_path, partition.get("report_glob", "*.rpt"))
    return sorted(glob.glob(pattern))


def process_partition(
    partition: dict,
    base_dir: str,
    golden: Optional[Dict[str, str]],
    verbose: bool,
) -> dict:
    """Collect and hash all report files for a single partition.

    Returns a dict with the partition result summary.
    """
    name = partition["name"]
    files = collect_partition_files(partition, base_dir)

    file_results = []
    for fpath in files:
        basename = os.path.basename(fpath)
        digest = compute_md5(fpath)
        size_bytes = os.path.getsize(fpath)

        md5_status = None
        if golden is not None:
            expected = golden.get(basename)
            if expected is None:
                md5_status = MD5_MISSING_MARKER
            elif expected == digest:
                md5_status = MD5_MATCH_MARKER
            else:
                md5_status = MD5_MISMATCH_MARKER

        entry = {
            "file": basename,
            "path": fpath,
            "md5": digest,
            "size_bytes": size_bytes,
        }
        if md5_status is not None:
            entry["md5_status"] = md5_status

        file_results.append(entry)

        if verbose:
            status_tag = f" [{md5_status}]" if md5_status else ""
            log.info("  %s  %s%s", digest, basename, status_tag)

    mismatches = [f for f in file_results if f.get("md5_status") == MD5_MISMATCH_MARKER]
    missing = [f for f in file_results if f.get("md5_status") == MD5_MISSING_MARKER]

    return {
        "partition": name,
        "path": partition["path"],
        "file_count": len(file_results),
        "files": file_results,
        "mismatch_count": len(mismatches),
        "missing_golden_count": len(missing),
        "status": "FAIL" if (mismatches or missing) else "PASS",
    }


def consolidate(
    config: dict,
    base_dir: str,
    golden: Optional[Dict[str, str]],
    selected_groups: Optional[List[str]],
    selected_partitions: Optional[List[str]],
    verbose: bool,
) -> dict:
    """Run consolidation across all requested groups/partitions.

    Returns the full consolidated result dict.
    """
    groups_cfg = config.get("groups", {})
    if not groups_cfg:
        log.error("No groups defined in configuration.")
        sys.exit(1)

    run_groups = selected_groups if selected_groups else list(groups_cfg.keys())
    unknown = set(run_groups) - set(groups_cfg.keys())
    if unknown:
        log.error("Unknown group(s): %s", ", ".join(sorted(unknown)))
        sys.exit(1)

    timestamp = datetime.now(timezone.utc).isoformat()
    result: dict = {
        "tool": "mcp_fuse consolidate_reports",
        "timestamp": timestamp,
        "groups": {},
        "summary": {},
    }

    total_files = 0
    total_mismatches = 0
    total_missing = 0

    for group_name in run_groups:
        group_cfg = groups_cfg[group_name]
        partitions = group_cfg.get("partitions", [])

        # Filter partitions if --partitions flag was given
        if selected_partitions:
            partitions = [p for p in partitions if p["name"] in selected_partitions]

        log.info("Processing group: %s (%s)", group_name, group_cfg.get("description", ""))

        group_results = []
        for partition in partitions:
            log.info("  Partition: %s", partition["name"])
            part_result = process_partition(partition, base_dir, golden, verbose)
            group_results.append(part_result)
            total_files += part_result["file_count"]
            total_mismatches += part_result["mismatch_count"]
            total_missing += part_result["missing_golden_count"]

        group_status = "FAIL" if any(p["status"] == "FAIL" for p in group_results) else "PASS"
        result["groups"][group_name] = {
            "description": group_cfg.get("description", ""),
            "partition_count": len(group_results),
            "partitions": group_results,
            "status": group_status,
        }

    overall_status = "FAIL" if (total_mismatches > 0 or total_missing > 0) else "PASS"
    result["summary"] = {
        "total_groups": len(run_groups),
        "total_partitions": sum(
            result["groups"][g]["partition_count"] for g in run_groups
        ),
        "total_files": total_files,
        "total_mismatches": total_mismatches,
        "total_missing_golden": total_missing,
        "overall_status": overall_status,
        "golden_md5_used": golden is not None,
    }

    return result


# ---------------------------------------------------------------------------
# Report writers
# ---------------------------------------------------------------------------

def _partition_section(part: dict) -> str:
    """Render a text block for one partition."""
    lines = []
    indent = "    "
    lines.append(f"  Partition : {part['partition']}")
    lines.append(f"  Path      : {part['path']}")
    lines.append(f"  Files     : {part['file_count']}")
    lines.append(f"  Status    : {part['status']}")
    if part.get("mismatch_count"):
        lines.append(f"  Mismatches: {part['mismatch_count']}")
    if part.get("missing_golden_count"):
        lines.append(f"  Missing   : {part['missing_golden_count']}")
    lines.append("")
    if part["files"]:
        lines.append(f"  {'MD5 Checksum':<34} {'Status':<10} {'Size (B)':<12} File")
        lines.append(f"  {'-'*34} {'-'*10} {'-'*12} {'-'*40}")
        for f in part["files"]:
            status_col = f.get("md5_status", "-")
            lines.append(
                f"  {f['md5']:<34} {status_col:<10} {f['size_bytes']:<12,} {f['file']}"
            )
    else:
        lines.append(f"{indent}(no report files found)")
    return "\n".join(lines)


def write_text_report(result: dict, out_path: str) -> None:
    """Write a human-readable consolidated report to *out_path*."""
    sep = "=" * 80
    thin = "-" * 80
    lines = [
        sep,
        "  MCP FUSE — Consolidated Partition Report",
        f"  Generated : {result['timestamp']}",
        sep,
        "",
    ]

    summary = result["summary"]
    lines += [
        "SUMMARY",
        thin,
        f"  Overall status     : {summary['overall_status']}",
        f"  Groups processed   : {summary['total_groups']}",
        f"  Partitions total   : {summary['total_partitions']}",
        f"  Report files total : {summary['total_files']}",
        f"  MD5 mismatches     : {summary['total_mismatches']}",
        f"  Missing golden     : {summary['total_missing_golden']}",
        f"  Golden md5 used    : {summary['golden_md5_used']}",
        "",
    ]

    for group_name, group in result["groups"].items():
        lines += [
            f"GROUP: {group_name}  [{group['status']}]",
            thin,
            f"  Description : {group['description']}",
            f"  Partitions  : {group['partition_count']}",
            "",
        ]
        for part in group["partitions"]:
            lines.append(_partition_section(part))
            lines.append("")
        lines.append("")

    lines += [sep, "  END OF REPORT", sep]

    with open(out_path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    log.info("Text report written: %s", out_path)


def write_json_report(result: dict, out_path: str) -> None:
    """Write the full result as a machine-readable JSON file."""
    with open(out_path, "w") as fh:
        json.dump(result, fh, indent=2)
    log.info("JSON report written: %s", out_path)


def write_md5_file(result: dict, out_path: str) -> None:
    """Write a consolidated md5sum file (``<hash>  <file>`` format)."""
    lines = []
    for group in result["groups"].values():
        for part in group["partitions"]:
            for f in part["files"]:
                lines.append(f"{f['md5']}  {f['path']}")
    with open(out_path, "w") as fh:
        fh.write("\n".join(sorted(lines)) + "\n")
    log.info("MD5 checksum file written: %s", out_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=textwrap.dedent("""\
            MCP FUSE — Consolidate partition reports.

            Collects report files from all configured partitions, computes MD5
            checksums, optionally compares against a golden md5sum file, and
            writes a detailed consolidated report.
        """),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML configuration file (default: config.yaml)",
    )
    parser.add_argument(
        "--base-dir",
        default=".",
        help="Base directory for resolving relative partition paths (default: .)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for output reports (overrides config output_dir)",
    )
    parser.add_argument(
        "--golden",
        default=None,
        metavar="GOLDEN_MD5_FILE",
        help="Path to golden md5sum file for checksum verification",
    )
    parser.add_argument(
        "--groups",
        nargs="+",
        default=None,
        metavar="GROUP",
        help="Restrict consolidation to these groups (default: all groups in config)",
    )
    parser.add_argument(
        "--partitions",
        nargs="+",
        default=None,
        metavar="PARTITION",
        help="Restrict consolidation to these partition names (default: all partitions)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Log each file as it is processed",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    # Load configuration
    if not os.path.isfile(args.config):
        log.error("Configuration file not found: %s", args.config)
        return 1

    with open(args.config) as fh:
        config = yaml.safe_load(fh)
    if not isinstance(config, dict):
        log.error("Invalid configuration file: %s", args.config)
        return 1

    output_dir = args.output_dir or config.get("output_dir", "consolidated_output")
    os.makedirs(output_dir, exist_ok=True)

    # Load golden md5 file if provided
    golden = load_golden_md5(args.golden) if args.golden else None

    # Run consolidation
    result = consolidate(
        config=config,
        base_dir=args.base_dir,
        golden=golden,
        selected_groups=args.groups,
        selected_partitions=args.partitions,
        verbose=args.verbose,
    )

    # Write outputs
    write_text_report(result, os.path.join(output_dir, REPORT_FILENAME))
    write_json_report(result, os.path.join(output_dir, JSON_FILENAME))
    write_md5_file(result, os.path.join(output_dir, MD5_FILENAME))

    # Print summary to stdout
    summary = result["summary"]
    print(f"\nConsolidation complete — {summary['overall_status']}")
    print(f"  Groups     : {summary['total_groups']}")
    print(f"  Partitions : {summary['total_partitions']}")
    print(f"  Files      : {summary['total_files']}")
    if golden is not None:
        print(f"  Mismatches : {summary['total_mismatches']}")
        print(f"  Missing    : {summary['total_missing_golden']}")
    print(f"  Reports    : {output_dir}/")

    return 0 if summary["overall_status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
