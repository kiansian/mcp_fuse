"""
Unit tests for consolidate_reports.py
"""
import hashlib
import json
import os
import tempfile
import textwrap

import pytest

# Ensure the repo root is importable
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from consolidate_reports import (
    MD5_MATCH_MARKER,
    MD5_MISMATCH_MARKER,
    MD5_MISSING_MARKER,
    collect_partition_files,
    compute_md5,
    consolidate,
    load_golden_md5,
    write_json_report,
    write_md5_file,
    write_text_report,
    main,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _write(path: str, content: str) -> str:
    """Write *content* to *path* (creating parent dirs) and return path."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(content)
    return path


def _md5(content: str) -> str:
    return hashlib.md5(content.encode()).hexdigest()


@pytest.fixture()
def workdir(tmp_path):
    """Return a temporary working directory with a minimal partition layout.

    Layout::

        workdir/
          runs/
            NVL_S/
              partition1/  report_a.rpt  report_b.rpt
              partition2/  report_c.rpt
            NVL_H/
              partition1/  report_d.rpt
    """
    base = str(tmp_path)
    _write(f"{base}/runs/NVL_S/partition1/report_a.rpt", "content_a\n")
    _write(f"{base}/runs/NVL_S/partition1/report_b.rpt", "content_b\n")
    _write(f"{base}/runs/NVL_S/partition2/report_c.rpt", "content_c\n")
    _write(f"{base}/runs/NVL_H/partition1/report_d.rpt", "content_d\n")
    return base


@pytest.fixture()
def simple_config(workdir):
    """Minimal YAML config covering two groups with two partitions each."""
    return {
        "output_dir": os.path.join(workdir, "output"),
        "groups": {
            "NVL_S": {
                "description": "NVL S partitions",
                "partitions": [
                    {
                        "name": "NVL_S_P1",
                        "path": "runs/NVL_S/partition1",
                        "report_glob": "*.rpt",
                    },
                    {
                        "name": "NVL_S_P2",
                        "path": "runs/NVL_S/partition2",
                        "report_glob": "*.rpt",
                    },
                ],
            },
            "NVL_H": {
                "description": "NVL H partitions",
                "partitions": [
                    {
                        "name": "NVL_H_P1",
                        "path": "runs/NVL_H/partition1",
                        "report_glob": "*.rpt",
                    },
                ],
            },
        },
    }


# ---------------------------------------------------------------------------
# compute_md5
# ---------------------------------------------------------------------------

class TestComputeMd5:
    def test_known_hash(self, tmp_path):
        path = str(tmp_path / "f.txt")
        content = "hello world\n"
        with open(path, "w") as fh:
            fh.write(content)
        assert compute_md5(path) == hashlib.md5(content.encode()).hexdigest()

    def test_empty_file(self, tmp_path):
        path = str(tmp_path / "empty.txt")
        open(path, "w").close()
        assert compute_md5(path) == hashlib.md5(b"").hexdigest()

    def test_binary_file(self, tmp_path):
        path = str(tmp_path / "bin.rpt")
        data = bytes(range(256))
        with open(path, "wb") as fh:
            fh.write(data)
        assert compute_md5(path) == hashlib.md5(data).hexdigest()


# ---------------------------------------------------------------------------
# load_golden_md5
# ---------------------------------------------------------------------------

class TestLoadGoldenMd5:
    def test_parse_standard_format(self, tmp_path):
        path = str(tmp_path / "golden.md5")
        with open(path, "w") as fh:
            fh.write("abc123  report_a.rpt\n")
            fh.write("def456  /some/path/report_b.rpt\n")
        result = load_golden_md5(path)
        assert result == {"report_a.rpt": "abc123", "report_b.rpt": "def456"}

    def test_comments_and_blank_lines_skipped(self, tmp_path):
        path = str(tmp_path / "golden.md5")
        with open(path, "w") as fh:
            fh.write("# comment\n\nabc123  file.rpt\n")
        result = load_golden_md5(path)
        assert result == {"file.rpt": "abc123"}

    def test_missing_file_returns_empty(self, tmp_path):
        result = load_golden_md5(str(tmp_path / "nonexistent.md5"))
        assert result == {}

    def test_malformed_line_skipped(self, tmp_path):
        path = str(tmp_path / "golden.md5")
        with open(path, "w") as fh:
            fh.write("onlyonetoken\n")
            fh.write("abc123  good.rpt\n")
        result = load_golden_md5(path)
        assert result == {"good.rpt": "abc123"}

    def test_hash_normalized_to_lowercase(self, tmp_path):
        path = str(tmp_path / "golden.md5")
        with open(path, "w") as fh:
            fh.write("ABCDEF  file.rpt\n")
        result = load_golden_md5(path)
        assert result["file.rpt"] == "abcdef"


# ---------------------------------------------------------------------------
# collect_partition_files
# ---------------------------------------------------------------------------

class TestCollectPartitionFiles:
    def test_collects_rpt_files(self, workdir):
        partition = {
            "name": "NVL_S_P1",
            "path": "runs/NVL_S/partition1",
            "report_glob": "*.rpt",
        }
        files = collect_partition_files(partition, workdir)
        basenames = [os.path.basename(f) for f in files]
        assert "report_a.rpt" in basenames
        assert "report_b.rpt" in basenames

    def test_empty_dir_returns_empty_list(self, tmp_path):
        part_dir = str(tmp_path / "empty_part")
        os.makedirs(part_dir)
        partition = {"name": "X", "path": "empty_part", "report_glob": "*.rpt"}
        assert collect_partition_files(partition, str(tmp_path)) == []

    def test_default_glob(self, workdir):
        partition = {
            "name": "NVL_S_P1",
            "path": "runs/NVL_S/partition1",
            # no report_glob key → defaults to *.rpt
        }
        files = collect_partition_files(partition, workdir)
        assert len(files) == 2


# ---------------------------------------------------------------------------
# consolidate (integration-level)
# ---------------------------------------------------------------------------

class TestConsolidate:
    def test_basic_consolidation(self, workdir, simple_config):
        result = consolidate(
            config=simple_config,
            base_dir=workdir,
            golden=None,
            selected_groups=None,
            selected_partitions=None,
            verbose=False,
        )
        assert result["summary"]["total_groups"] == 2
        assert result["summary"]["total_partitions"] == 3
        assert result["summary"]["total_files"] == 4
        assert result["summary"]["overall_status"] == "PASS"
        assert result["summary"]["golden_md5_used"] is False

    def test_selected_groups(self, workdir, simple_config):
        result = consolidate(
            config=simple_config,
            base_dir=workdir,
            golden=None,
            selected_groups=["NVL_S"],
            selected_partitions=None,
            verbose=False,
        )
        assert set(result["groups"].keys()) == {"NVL_S"}
        assert result["summary"]["total_groups"] == 1

    def test_selected_partitions(self, workdir, simple_config):
        result = consolidate(
            config=simple_config,
            base_dir=workdir,
            golden=None,
            selected_groups=["NVL_S"],
            selected_partitions=["NVL_S_P1"],
            verbose=False,
        )
        parts = result["groups"]["NVL_S"]["partitions"]
        assert len(parts) == 1
        assert parts[0]["partition"] == "NVL_S_P1"

    def test_unknown_group_exits(self, workdir, simple_config):
        with pytest.raises(SystemExit):
            consolidate(
                config=simple_config,
                base_dir=workdir,
                golden=None,
                selected_groups=["DOES_NOT_EXIST"],
                selected_partitions=None,
                verbose=False,
            )

    def test_golden_md5_pass(self, workdir, simple_config, tmp_path):
        # Build a correct golden file
        golden_path = str(tmp_path / "golden.md5")
        with open(golden_path, "w") as fh:
            for fname, content in [
                ("report_a.rpt", "content_a\n"),
                ("report_b.rpt", "content_b\n"),
                ("report_c.rpt", "content_c\n"),
                ("report_d.rpt", "content_d\n"),
            ]:
                fh.write(f"{_md5(content)}  {fname}\n")
        golden = load_golden_md5(golden_path)
        result = consolidate(
            config=simple_config,
            base_dir=workdir,
            golden=golden,
            selected_groups=None,
            selected_partitions=None,
            verbose=False,
        )
        assert result["summary"]["total_mismatches"] == 0
        assert result["summary"]["overall_status"] == "PASS"

    def test_golden_md5_mismatch(self, workdir, simple_config, tmp_path):
        golden_path = str(tmp_path / "golden.md5")
        with open(golden_path, "w") as fh:
            fh.write(f"{'0' * 32}  report_a.rpt\n")  # wrong hash
        golden = load_golden_md5(golden_path)
        result = consolidate(
            config=simple_config,
            base_dir=workdir,
            golden=golden,
            selected_groups=["NVL_S"],
            selected_partitions=None,
            verbose=False,
        )
        assert result["summary"]["total_mismatches"] >= 1
        assert result["summary"]["overall_status"] == "FAIL"

    def test_golden_md5_missing_entry(self, workdir, simple_config, tmp_path):
        golden_path = str(tmp_path / "golden.md5")
        # Only provide hash for one of the four files
        with open(golden_path, "w") as fh:
            fh.write(f"{_md5('content_a' + chr(10))}  report_a.rpt\n")
        golden = load_golden_md5(golden_path)
        result = consolidate(
            config=simple_config,
            base_dir=workdir,
            golden=golden,
            selected_groups=None,
            selected_partitions=None,
            verbose=False,
        )
        assert result["summary"]["total_missing_golden"] >= 1
        assert result["summary"]["overall_status"] == "FAIL"

    def test_md5_status_values(self, workdir, simple_config, tmp_path):
        golden_path = str(tmp_path / "golden.md5")
        with open(golden_path, "w") as fh:
            fh.write(f"{_md5('content_a' + chr(10))}  report_a.rpt\n")  # PASS
            fh.write(f"{'0' * 32}  report_b.rpt\n")                     # FAIL/mismatch
            # report_c, report_d not in golden → MISSING
        golden = load_golden_md5(golden_path)
        result = consolidate(
            config=simple_config,
            base_dir=workdir,
            golden=golden,
            selected_groups=None,
            selected_partitions=None,
            verbose=False,
        )
        nvls_p1 = result["groups"]["NVL_S"]["partitions"][0]
        statuses = {f["file"]: f.get("md5_status") for f in nvls_p1["files"]}
        assert statuses["report_a.rpt"] == MD5_MATCH_MARKER
        assert statuses["report_b.rpt"] == MD5_MISMATCH_MARKER


# ---------------------------------------------------------------------------
# Report writers
# ---------------------------------------------------------------------------

class TestWriteTextReport:
    def _make_result(self):
        return {
            "tool": "mcp_fuse",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "groups": {
                "NVL_S": {
                    "description": "NVL S",
                    "partition_count": 1,
                    "status": "PASS",
                    "partitions": [
                        {
                            "partition": "NVL_S_P1",
                            "path": "runs/NVL_S/partition1",
                            "file_count": 1,
                            "status": "PASS",
                            "mismatch_count": 0,
                            "missing_golden_count": 0,
                            "files": [
                                {
                                    "file": "a.rpt",
                                    "path": "/runs/a.rpt",
                                    "md5": "abc",
                                    "size_bytes": 10,
                                }
                            ],
                        }
                    ],
                }
            },
            "summary": {
                "total_groups": 1,
                "total_partitions": 1,
                "total_files": 1,
                "total_mismatches": 0,
                "total_missing_golden": 0,
                "overall_status": "PASS",
                "golden_md5_used": False,
            },
        }

    def test_creates_text_file(self, tmp_path):
        result = self._make_result()
        out = str(tmp_path / "report.txt")
        write_text_report(result, out)
        assert os.path.isfile(out)
        content = open(out).read()
        assert "PASS" in content
        assert "NVL_S" in content

    def test_overall_status_in_report(self, tmp_path):
        result = self._make_result()
        result["summary"]["overall_status"] = "FAIL"
        out = str(tmp_path / "report.txt")
        write_text_report(result, out)
        assert "FAIL" in open(out).read()


class TestWriteJsonReport:
    def test_valid_json(self, tmp_path):
        result = {"key": "value"}
        out = str(tmp_path / "report.json")
        write_json_report(result, out)
        with open(out) as fh:
            data = json.load(fh)
        assert data == result


class TestWriteMd5File:
    def test_md5_file_format(self, tmp_path):
        result = {
            "groups": {
                "NVL_S": {
                    "partitions": [
                        {
                            "files": [
                                {"md5": "abc123", "path": "/runs/a.rpt"},
                                {"md5": "def456", "path": "/runs/b.rpt"},
                            ]
                        }
                    ]
                }
            }
        }
        out = str(tmp_path / "out.md5")
        write_md5_file(result, out)
        lines = open(out).read().splitlines()
        assert any("abc123" in l and "/runs/a.rpt" in l for l in lines)
        assert any("def456" in l and "/runs/b.rpt" in l for l in lines)


# ---------------------------------------------------------------------------
# CLI (main)
# ---------------------------------------------------------------------------

class TestMain:
    def _write_config(self, path: str, config_text: str) -> None:
        with open(path, "w") as fh:
            fh.write(config_text)

    def test_missing_config_returns_1(self, tmp_path):
        rc = main(["--config", str(tmp_path / "nosuchfile.yaml")])
        assert rc == 1

    def test_full_run_no_golden(self, workdir, simple_config, tmp_path):
        import yaml as _yaml

        config_path = str(tmp_path / "cfg.yaml")
        out_dir = str(tmp_path / "out")
        simple_config["output_dir"] = out_dir
        with open(config_path, "w") as fh:
            _yaml.dump(simple_config, fh)

        rc = main(["--config", config_path, "--base-dir", workdir, "--output-dir", out_dir])
        assert rc == 0
        assert os.path.isfile(os.path.join(out_dir, "consolidated_report.txt"))
        assert os.path.isfile(os.path.join(out_dir, "consolidated_report.json"))
        assert os.path.isfile(os.path.join(out_dir, "consolidated.md5"))

    def test_full_run_with_group_filter(self, workdir, simple_config, tmp_path):
        import yaml as _yaml

        config_path = str(tmp_path / "cfg.yaml")
        out_dir = str(tmp_path / "out")
        simple_config["output_dir"] = out_dir
        with open(config_path, "w") as fh:
            _yaml.dump(simple_config, fh)

        rc = main([
            "--config", config_path,
            "--base-dir", workdir,
            "--output-dir", out_dir,
            "--groups", "NVL_S",
        ])
        assert rc == 0
        data = json.load(open(os.path.join(out_dir, "consolidated_report.json")))
        assert list(data["groups"].keys()) == ["NVL_S"]
