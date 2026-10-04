#!/usr/bin/env python3
"""从 terraform output -json 生成 Ansible INI inventory（CLI 薄壳）。
纯逻辑在 render_inventory_lib.py（可测，tests/test_render_inventory.py）。

用法:
  python3 iac/scripts/render-inventory.py \
    --tf-output /tmp/tf-outputs-slr.json \
    --template iac/terraform/environments/prod/inventory.tmpl \
    --output iac/ansible/inventories/prod/hosts

占位符残留防护：模板中任何未被填充的 ${...} 直接报错退出。
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import render_inventory_lib as ri  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Render Ansible inventory from Terraform outputs")
    parser.add_argument("--tf-output", required=True)
    parser.add_argument("--template", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    with open(args.tf_output) as f:
        tf_data = json.load(f)

    template_vars = ri.build_template_vars(tf_data)
    with open(args.template) as f:
        template = f.read()
    rendered = ri.render(template_vars, template)

    leftover = ri.find_leftover(rendered)
    if leftover:
        print(f"ERROR: 模板占位符未被填充: {leftover}", file=sys.stderr)
        print("  → 检查 tfvars 实例组键 / GROUP_MAP 是否匹配模板", file=sys.stderr)
        sys.exit(1)

    with open(args.output, "w") as f:
        f.write(rendered)

    print(f"Generated: {args.output}")
    for group in sorted(k for k in template_vars if k != "timestamp"):
        content = template_vars[group]
        if content:
            print(f"    [{group}] {len(content.strip().splitlines())} hosts")


if __name__ == "__main__":
    main()
