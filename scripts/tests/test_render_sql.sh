#!/bin/bash
# render-sql.sh 门禁测试：渲染产物不得残留本地 warehouse 路径与 'iggy' 主机名，
# 且 pipeline.jars 的 /opt/flink/lib 路径不被误伤。
set -euo pipefail
cd "$(dirname "$0")/../.."
OUT=$(mktemp -d); trap 'rm -rf "$OUT"' EXIT
SLR_WAREHOUSE_BASE="oss://test-bucket/warehouse" SLR_IGGY_HOST="10.0.0.9" SLR_FLUSS_BOOTSTRAP="10.0.0.9:9123" \
  ./scripts/render-sql.sh flink/sql "$OUT"
fail=0
if grep -rl "opt/flink/warehouse" "$OUT"; then echo "FAIL: 残留 warehouse 本地路径"; fail=1; fi
# 注意：'connector' = 'iggy' 是连接器类型名必须保留，只查 host 行
if grep -rlE "'host' *= *'iggy'" "$OUT"; then echo "FAIL: 残留 iggy 主机名"; fail=1; fi
if grep -rl "fluss-coordinator" "$OUT"; then echo "FAIL: 残留 fluss-coordinator 主机名"; fail=1; fi
grep -q "'bootstrap.servers' = '10.0.0.9:9123'" "$OUT/clearing-house.sql" || { echo "FAIL: fluss bootstrap 未替换"; fail=1; }
grep -q "oss://test-bucket/warehouse/paimon" "$OUT/lakehouse-tier.sql" || { echo "FAIL: paimon 未替换"; fail=1; }
grep -q "oss://test-bucket/warehouse/iceberg" "$OUT/lakehouse-tier.sql" || { echo "FAIL: iceberg 未替换"; fail=1; }
grep -q "'host'      = '10.0.0.9'" "$OUT/clearing-house.sql" || { echo "FAIL: iggy host 未替换"; fail=1; }
grep -q "oss://test-bucket/warehouse/backfill/" "$OUT/import-backfill.sql" || { echo "FAIL: backfill 未替换"; fail=1; }
grep -q "file:///opt/flink/lib/flink-connector-iggy.jar" "$OUT/lakehouse-tier.sql" || { echo "FAIL: pipeline.jars 被误伤"; fail=1; }
[ "$(ls "$OUT" | wc -l | tr -d ' ')" = "7" ] || { echo "FAIL: 应渲染 7 个 SQL"; fail=1; }
[ $fail -eq 0 ] && echo "ALL PASS"
