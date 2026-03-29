from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import server

ROOT = Path(__file__).resolve().parents[1]
DATA_LIST = ROOT / 'data_list.log'
OUT_DIR = Path('/nfs/site/disks/zsc11_nvlpcd_00026/kgoh14/AI_TEST/sample_setup_official/vscode_copilot_fusion_compiler/iscp_pch_chatbot/mcp_fuse/autobots_agent_scratch_dir')

rows = []
for line in DATA_LIST.read_text().splitlines():
    parts = line.strip().split()
    if len(parts) >= 4 and parts[0].upper() == 'WARD':
        rows.append((parts[3].rstrip('/'), parts[2]))
    elif len(parts) >= 3 and parts[0].upper() != 'XOR':
        rows.append((parts[2].rstrip('/'), parts[1]))

for idx, (run_path, block_name) in enumerate(rows, start=1):
    out_csv = OUT_DIR / f"drc_rules_{idx:02d}_{block_name}.csv"
    result = server.parse_violation_summary_drc_from_run.fn(run_path, block_name, str(out_csv))
    print('---')
    print(f'RUN={run_path}')
    print(f'BLOCK={block_name}')
    for line in result.splitlines()[:10]:
        print(line)
