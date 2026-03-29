# MCP Fuse

Purpose:
- Consolidate and mine `data_list.log` style run-list inputs.
- Generate block artifact summaries, DRC rule reports, and email-ready HTML.

Primary report script in this workflow:
- `autobots_agent_scratch_dir/resend_latest_report.py`
- Source-of-truth input: `mcp_fuse/data_list.log`

## Supported `data_list.log` input formats

Core partition rows (recommended):
- `WARD <project> <block> <run_path>`

Legacy rows (still supported):
- `<project> <block> <run_path>`

XOR rows (optional, for XOR-specific reporting paths):
- `XOR <project> <block> <xor_log_path>`

Comments/info rows:
- Any line starting with `#` is ignored.

## Parsing behavior

- `# ...` lines are ignored.
- `XOR ...` lines are skipped from core run parsing (`project/block/run_path` records).
- `WARD ...` and legacy 3-column rows are parsed into:
	- `project_name`
	- `flow_name`
	- `run_path`
- Partition order in report matrices follows `WARD` rows.
- If XOR rows are missing/commented for a partition, report shows not-ready/missing values.

## Current HTML report sections (resend flow)

The refreshed report includes these major tables/sections:
- Paths / timestamps / gmd5sum
- Run-check runtime matrix (partition columns)
	- `APR_FC_FINISH_ELAPSED` from:
		- `<run_path>/apr_fc/logs/fc.finish.log`, fallback `fc.apreco_finish.log`
		- keyword: `Elapsed time for this session`
	- `FEV_LEC_ELAPSE_MIN` from:
		- `<run_path>/fev_conformal/fev_noconst/logs/lec.log`
		- keyword: `Elapse time  : <sec> seconds`
		- converted and shown as minutes + seconds
	- `STAR_PV_ALL_LOOKS_OKAY_RUNTIME` from:
		- `<run_path>/extraction/logs/star_pv/star_pv.log`
		- keyword line containing `All looks okay`
- StarPV extract quality summary (partition columns)
	- source: `<run_path>/extraction/reports/star_pv/<block>.extract_quality.report`
	- rows include versions and quality counters (nets/opens/shorts/smin/etc)
- LV ICV runtime by run
	- source: `<run_path>/lv_icv/logs/*/icv.log`
	- row key = folder name under `lv_icv/logs/`
	- value from keyword `Overall engine Time`
- FEV status, DRC/LVS summaries, XOR summary table

Missing data behavior:
- Missing file/path uses placeholders such as `MISSING_LOG`, `MISSING_REPORT`, `ELAPSED_NOT_FOUND`.

## Quick capability guide tool

- Tool name: `mcp_fuse_help()`
- Returns a brief summary of:
	- what `mcp_fuse` can do,
	- required/common input fields,
	- typical tool calls.
	- current report logic and data source behavior.

## Natural-language mail intent mapping

New helper tool:
- `resolve_refresh_resend_intent(user_text, me_email="kian.sian.goh@intel.com")`

Behavior:
- Keywords like `refresh` / `resend` / `send` / `mail` / `email` are treated as mail-send intent.
- Recipient scope is inferred from phrasing:
	- me-only phrases (`to me`, `me only`, `mail me`) => `SCOPE=ME`
	- all/team phrases (`to all`, `all recipients`, `team`, `distro`) => `SCOPE=ALL`
	- otherwise => `SCOPE=UNSPECIFIED`, `NEEDS_SCOPE=YES`
- Schedule mode is inferred from phrasing:
	- periodic phrases (`periodic`, `recursive`, `recurring`, `every`, `hourly`, `daily`) => `DELIVERY_MODE=PERIODIC`
	- otherwise => `DELIVERY_MODE=ONE_TIME`
	- explicit interval examples are parsed into `INTERVAL_MIN` (for example: `every 10 minutes` => `INTERVAL_MIN=10`)
	- periodic without explicit interval gives `NEEDS_INTERVAL=YES`

Examples:
- `refresh and resend` => send intent, but asks scope
- `refresh and send to me` => send intent + me-only
- `resend to all` => send intent + all recipients
- `periodic refresh every 10 minutes to all` => periodic send + interval=10 minutes + all recipients
- `recursive refresh resend` => periodic send, but asks interval

## Common tools and inputs

- `parse_data_list_log(file_path)`
	- input: absolute path to one `data_list.log`
- `consolidate_data_list_logs(file_paths, output_csv_path, output_json_path)`
	- `file_paths`: comma-separated absolute paths
- `dump_block_artifact_csv(data_list_file, output_csv_path)`
- `format_drc_rule_table_from_data_list(data_list_file, output_csv_path)`
- `parse_violation_summary_drc_from_run(run_path, block_name, output_csv_path)`

Typical input fields:
- `data_list_file`: absolute path to `data_list.log`
- `run_path`: absolute run directory
- `block_name`: block/flow name used in report file names
- `output_csv_path` / `output_json_path` / `output_html_path`: absolute output paths
