#!/bin/bash
# ============================================================
# 从 terraform output -json 生成 Ansible inventory（Terraform 是唯一事实来源）
# 用法: ./iac/scripts/generate-inventory.sh prod   [FORCE=1 跳过覆盖确认]
# 前提: 已 terraform apply（或 state 可读）；backend 见 environments/prod/backend.conf
# ============================================================

set -euo pipefail

ENV="${1:?Usage: $0 <environment>（当前仅 prod）}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IAC_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TF_ENV_DIR="${IAC_ROOT}/terraform/environments/${ENV}"
INV_DIR="${IAC_ROOT}/ansible/inventories/${ENV}"
TF_OUTPUT_FILE="/tmp/tf-outputs-slr-${ENV}.json"

case "${ENV}" in
    prod) OUTPUT_FILE="${INV_DIR}/hosts" ;;
    *)    echo "ERROR: 未知环境 '${ENV}'（支持: prod）"; exit 1 ;;
esac

echo "=== Generating Ansible inventory for ${ENV} ==="

if [ ! -d "${TF_ENV_DIR}" ]; then
    echo "ERROR: Environment directory not found: ${TF_ENV_DIR}"
    exit 1
fi

# 1. terraform outputs
echo "[1/3] Extracting terraform outputs..."
if [ ! -d "${TF_ENV_DIR}/.terraform" ]; then
    echo "  Initializing Terraform (backend=false for output extraction)..."
    terraform -chdir="${TF_ENV_DIR}" init -backend=false
fi
terraform -chdir="${TF_ENV_DIR}" output -json > "${TF_OUTPUT_FILE}"

# 覆盖防护：自动备份 + 交互确认（FORCE=1 跳过）
# ★ 备份必须 .bak 结尾：Ansible 目录 inventory 忽略 .bak；若命名 hosts.bak.<ts>，
#   splitext 后缀是时间戳 → 会被当 inventory 解析且字典序靠后覆盖新 hosts（adtrader C1 实测教训）
mkdir -p "${INV_DIR}"
if [ -f "${OUTPUT_FILE}" ]; then
    BACKUP="${OUTPUT_FILE}.$(date +%Y%m%d%H%M%S).bak"
    cp "${OUTPUT_FILE}" "${BACKUP}"
    echo "  已备份: ${BACKUP}"
    if [ "${FORCE:-0}" != "1" ]; then
        read -r -p "  ⚠️  即将覆盖 ${OUTPUT_FILE}（手写内容应放 group_vars/host_vars）。输入 yes 继续: " ANSWER
        [ "${ANSWER}" = "yes" ] || { echo "  已取消"; exit 1; }
    fi
fi

# 2. 渲染
echo "[2/3] Rendering inventory template..."
python3 "${SCRIPT_DIR}/render-inventory.py" \
    --tf-output "${TF_OUTPUT_FILE}" \
    --template "${TF_ENV_DIR}/inventory.tmpl" \
    --output "${OUTPUT_FILE}"

# 3. 验证提示
echo "[3/3] Verification..."
echo "Output file: ${OUTPUT_FILE}"
echo "  ansible -i ${INV_DIR} all -m ping"
echo "  覆盖后校验: diff 备份 vs 新文件 → ansible-inventory --graph 组完整性 → ping 全绿"
echo "=== Done ==="
