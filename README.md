# mcp_fuse

SPE ISCK CKT team — MCP FUSE for metal ECO on data mining feature.

## Report Consolidation Tool

`consolidate_reports.py` collects report files from all configured partitions
(e.g. **NVL S** and **NVL H**), computes MD5 checksums (gmd5sum), optionally
compares against a *golden* reference md5 file, and writes a detailed
consolidated report in both human-readable text and machine-readable JSON.

### Quick start

```bash
# Install dependency
pip install pyyaml

# Consolidate all groups defined in config.yaml
python consolidate_reports.py

# Consolidate only the NVL_S and NVL_H groups
python consolidate_reports.py --groups NVL_S NVL_H

# Verify checksums against a golden md5 file
python consolidate_reports.py --golden golden.md5

# Use a custom config and write outputs to /tmp/reports
python consolidate_reports.py --config my_config.yaml --output-dir /tmp/reports
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--config FILE` | `config.yaml` | YAML configuration file |
| `--base-dir DIR` | `.` | Base directory for resolving relative partition paths |
| `--output-dir DIR` | `consolidated_output` | Output directory for generated reports |
| `--golden FILE` | *(none)* | Golden md5sum file for checksum verification |
| `--groups GROUP …` | all groups | Restrict consolidation to specific groups |
| `--partitions PART …` | all partitions | Restrict consolidation to specific partition names |
| `--verbose` | off | Log each file as it is processed |

### Configuration (`config.yaml`)

```yaml
output_dir: "consolidated_output"

groups:
  NVL_S:
    description: "NVL S partitions"
    partitions:
      - name: "NVL_S_P1"
        path: "runs/NVL_S/partition1"
        report_glob: "*.rpt"
      - name: "NVL_S_P2"
        path: "runs/NVL_S/partition2"
        report_glob: "*.rpt"
      # … add more partitions as needed
  NVL_H:
    description: "NVL H partitions"
    partitions:
      - name: "NVL_H_P1"
        path: "runs/NVL_H/partition1"
        report_glob: "*.rpt"
      # … add more partitions as needed
  # Other groups can be added here
```

### Outputs

After a successful run the `output_dir` contains:

| File | Description |
|---|---|
| `consolidated_report.txt` | Human-readable full report with per-partition detail |
| `consolidated_report.json` | Machine-readable full report (all fields) |
| `consolidated.md5` | MD5 checksum file for every processed report file |

### Running tests

```bash
pip install pyyaml pytest
python -m pytest tests/test_consolidate.py -v
```
