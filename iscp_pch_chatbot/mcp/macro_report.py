import re
from pathlib import Path

from autobots_sdk.base.executors.synopsys.fusion_compiler import fusion_compiler


def export_macro_report_impl(output_path: str) -> str:
    """
    Export hard macro info in format:
    ref_name, llx lly urx ury, instance_name
    """

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    inst_log = out_path.parent / "macro_inst_raw.txt"
    ref_log = out_path.parent / "macro_ref_raw.txt"
    bbox_log = out_path.parent / "macro_bbox_raw.txt"

    base_query = "[get_cells -hier -filter {is_hard_macro==true}]"
    fusion_compiler(query=f"get_object_name {base_query}", log_path=str(inst_log))
    fusion_compiler(query=f"get_attribute {base_query} ref_name", log_path=str(ref_log))
    fusion_compiler(query=f"get_attribute {base_query} boundary_bbox", log_path=str(bbox_log))

    instances = [token for token in inst_log.read_text().split() if token.strip()]
    refs = [token for token in ref_log.read_text().split() if token.strip()]
    bbox_text = bbox_log.read_text()

    bbox_matches = re.findall(r"\{\{([^{}]+)\}\s+\{([^{}]+)\}\}", bbox_text)
    bboxes = []
    for ll, ur in bbox_matches:
        ll_parts = ll.split()
        ur_parts = ur.split()
        if len(ll_parts) >= 2 and len(ur_parts) >= 2:
            bboxes.append((ll_parts[0], ll_parts[1], ur_parts[0], ur_parts[1]))
        else:
            bboxes.append(("NA", "NA", "NA", "NA"))

    count = min(len(instances), len(refs), len(bboxes))
    lines = ["ref_name, llx lly urx ury, instance_name"]

    for idx in range(count):
        ref = refs[idx]
        inst = instances[idx]
        llx, lly, urx, ury = bboxes[idx]
        lines.append(f"{ref}, {llx} {lly} {urx} {ury}, {inst}")

    out_path.write_text("\n".join(lines) + "\n")

    return f"Wrote {count} macros to {out_path}"
