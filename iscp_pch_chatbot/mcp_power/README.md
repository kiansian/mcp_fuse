# Power MCP (Starter)

This folder contains a new MCP server focused on power-related queries in Fusion Compiler.

## Files
- `server.py`: MCP server with starter power tools
- `mcp.json`: MCP server launch config

## Starter tools
- `power_current_design()`
- `power_current_block()`
- `power_list_pg_nets()`
- `power_query_fc(command=...)`

## Typical commands via `power_query_fc`
- `report_power`
- `report_pg_nets`
- `report_voltage_areas`
- `report_clock_tree_power`

## Notes
- This is a starter read/query MCP.
- Add editing/fix tools only after power signoff flow rules are defined.
