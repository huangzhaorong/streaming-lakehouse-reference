#!/bin/bash
# 部署期渲染 Flink SQL：warehouse 本地路径 → OSS-HDFS，'iggy' 容器主机名 → 实际地址。
# 仓库内 flink/sql/*.sql 保持 Compose 时代写法不动，渲染仅发生在部署目标机。
# 用法: SLR_WAREHOUSE_BASE=oss://<bucket>/warehouse SLR_IGGY_HOST=<ip> \
#       [SLR_FLUSS_BOOTSTRAP=<host:9123>，默认 127.0.0.1:9123] render-sql.sh <src_dir> <out_dir>
# 示例: SLR_WAREHOUSE_BASE=oss://slr-lakehouse-prod/warehouse SLR_IGGY_HOST=127.0.0.1 \
#         ./scripts/render-sql.sh flink/sql /opt/flink/sql
set -euo pipefail
SRC="${1:?用法: render-sql.sh <src_dir> <out_dir>}"
OUT="${2:?用法: render-sql.sh <src_dir> <out_dir>}"
: "${SLR_WAREHOUSE_BASE:?需设置 SLR_WAREHOUSE_BASE，如 oss://slr-lakehouse-prod/warehouse}"
: "${SLR_IGGY_HOST:?需设置 SLR_IGGY_HOST，如 127.0.0.1}"
SLR_FLUSS_BOOTSTRAP="${SLR_FLUSS_BOOTSTRAP:-127.0.0.1:9123}"
mkdir -p "$OUT"
for f in "$SRC"/*.sql; do
  # 顺序重要：先 file:///…/warehouse/（backfill），再裸 /opt/flink/warehouse；
  # pipeline.jars 的 /opt/flink/lib 不含 warehouse 字样，天然不受影响。
  sed -e "s|file:///opt/flink/warehouse/|${SLR_WAREHOUSE_BASE}/|g" \
      -e "s|/opt/flink/warehouse|${SLR_WAREHOUSE_BASE}|g" \
      -e "s|'host'\( *= *\)'iggy'|'host'\1'${SLR_IGGY_HOST}'|" \
      -e "s|fluss-coordinator:9123|${SLR_FLUSS_BOOTSTRAP}|g" \
      "$f" > "$OUT/$(basename "$f")"
done
echo "rendered $(ls "$OUT" | wc -l | tr -d ' ') sql files → $OUT"
