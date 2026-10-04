#!/bin/bash
# 从 apache/iggy:0.7.0 镜像一次性提取原生二进制（官方自 0.5.0 起停发发行资产）。
# 产物: <OUT>/iggy-0.7.0-linux-x86_64.tar.gz（iggy-server、iggy CLI、configs/）+ .sha256
# 产物随后由操作者上传至 slr-artifacts-prod OSS 桶（见 docs/runbook-ecs-deploy.md）。
#
# 用法: ./scripts/extract-iggy-binary.sh [输出目录，默认 dist/]
# 环境变量: DRY_RUN=1 只打印将执行的 docker 命令，不实际执行（无 docker 环境校验用）
# 需要本机 docker 守护进程可用。镜像基底 debian:bookworm-slim（glibc），
# 与 Alibaba Cloud Linux 3 运行环境兼容；提取后建议在目标机 `ldd iggy-server` 核验。
set -euo pipefail

IMAGE="apache/iggy:0.7.0"
OUT="${1:-dist}"
NAME="iggy-0.7.0-linux-x86_64"

run() {
  if [ "${DRY_RUN:-0}" = "1" ]; then echo "[dry-run] $*"; else "$@"; fi
}

if [ "${DRY_RUN:-0}" != "1" ] && ! docker info >/dev/null 2>&1; then
  echo "ERROR: docker 守护进程不可用（或设 DRY_RUN=1 仅校验流程）" >&2
  exit 1
fi

# 容器内路径自动探测（不硬编码，防镜像布局差异）
probe() { # $1=文件名
  run docker run --rm --entrypoint sh "$IMAGE" -c \
    "command -v $1 2>/dev/null || find / -maxdepth 4 -type f -name $1 2>/dev/null | head -1"
}

if [ "${DRY_RUN:-0}" = "1" ]; then
  SERVER_BIN=/iggy-server; CLI_BIN=/iggy; CONF_DIR=/configs
  echo "[dry-run] 探测 iggy-server / iggy / configs 路径"
else
  SERVER_BIN=$(probe iggy-server)
  CLI_BIN=$(probe iggy)
  CONF_DIR=$(docker run --rm --entrypoint sh "$IMAGE" -c \
    'find / -maxdepth 4 -type d -name configs 2>/dev/null | head -1')
  for p in "$SERVER_BIN" "$CLI_BIN" "$CONF_DIR"; do
    [ -n "$p" ] || { echo "ERROR: 镜像内未找到必要路径（server=$SERVER_BIN cli=$CLI_BIN conf=$CONF_DIR）" >&2; exit 1; }
  done
fi

mkdir -p "$OUT"
TMP=$(mktemp -d)
CID=""
trap 'rm -rf "$TMP"; [ -n "$CID" ] && docker rm -f "$CID" >/dev/null 2>&1 || true' EXIT

run docker pull "$IMAGE"
CID=$(run docker create "$IMAGE")
if [ "${DRY_RUN:-0}" = "1" ]; then CID=dry-run-cid; fi
run docker cp "$CID:$SERVER_BIN" "$TMP/iggy-server"
run docker cp "$CID:$CLI_BIN" "$TMP/iggy"
run docker cp "$CID:$CONF_DIR" "$TMP/configs"

if [ "${DRY_RUN:-0}" = "1" ]; then
  echo "[dry-run] tar -C \$TMP -czf $OUT/$NAME.tar.gz iggy-server iggy configs"
  echo "[dry-run] shasum -a 256 → $OUT/$NAME.tar.gz.sha256"
  echo "DRY RUN OK"
  exit 0
fi

chmod +x "$TMP/iggy-server" "$TMP/iggy"
tar -C "$TMP" -czf "$OUT/$NAME.tar.gz" iggy-server iggy configs
(cd "$OUT" && shasum -a 256 "$NAME.tar.gz" > "$NAME.tar.gz.sha256")
echo "OK: $OUT/$NAME.tar.gz"
cat "$OUT/$NAME.tar.gz.sha256"
echo "下一步: ossutil cp $OUT/$NAME.tar.gz* oss://slr-artifacts-prod/ （见 runbook）"
