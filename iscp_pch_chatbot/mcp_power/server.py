#!/usr/bin/env python3
"""
Power MCP Server (starter)

Purpose:
- Provide power-related helper tools through MCP for a live Fusion Compiler session.
"""

from autobots_sdk.base.mcp.servers.base_server import AutobotsMCPStdioServer
from autobots_sdk.base.executors.synopsys.fusion_compiler import fusion_compiler

mcp_fc_power = AutobotsMCPStdioServer(name="fusion_compiler_power")


@mcp_fc_power.tool()
def power_current_design(log_path: str | None = None) -> str:
    """
    Return current design from linked Fusion Compiler session.
    """

    return fusion_compiler(query="current_design", log_path=log_path)


@mcp_fc_power.tool()
def power_current_block(log_path: str | None = None) -> str:
    """
    Return current block from linked Fusion Compiler session.
    """

    return fusion_compiler(query="current_block", log_path=log_path)


@mcp_fc_power.tool()
def power_list_pg_nets(log_path: str | None = None) -> str:
    """
    List power/ground nets in the current design.
    """

    return fusion_compiler(
        query="get_object_name [get_nets -quiet -filter {net_type==power || net_type==ground}]",
        log_path=log_path,
    )


@mcp_fc_power.tool()
def power_query_fc(command: str, log_path: str | None = None) -> str:
    """
    Run a power-related FC query (advanced escape hatch).

    Example:
    - report_power
    - report_pg_nets
    - report_voltage_areas
    - report_clock_tree_power

    Args:
        command: Raw FC command string.
        log_path: Optional path to save output log.
    """

    if not command or not command.strip():
        return "command is required"

    return fusion_compiler(query=command.strip(), log_path=log_path)


if __name__ == "__main__":
    mcp_fc_power.run()
