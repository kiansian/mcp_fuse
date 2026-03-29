import sys
sys.path.insert(0, '/nfs/site/disks/zsc11_nvlpcd_00026/kgoh14/AI_TEST/sample_setup_official/vscode_copilot_fusion_compiler/iscp_pch_chatbot/mcp_fuse')
import server
from pathlib import Path

DATA_LIST = str(Path(__file__).resolve().parents[1] / 'data_list.log')
OUT_DIR = '/nfs/site/disks/zsc11_nvlpcd_00026/kgoh14/AI_TEST/sample_setup_official/vscode_copilot_fusion_compiler/iscp_pch_chatbot/mcp_fuse/autobots_agent_scratch_dir'

print(server.consolidate_data_list_logs.fn(DATA_LIST, f'{OUT_DIR}/consolidated_data_list.csv', f'{OUT_DIR}/consolidated_data_list.json'))
print('---')
print(server.dump_block_artifact_csv.fn(DATA_LIST, f'{OUT_DIR}/block_artifact_report.csv'))
print('---')
summary = server.format_drc_rule_table_from_data_list.fn(DATA_LIST, f'{OUT_DIR}/drc_rule_table.csv')
for line in summary.splitlines()[:12]:
    print(line)
