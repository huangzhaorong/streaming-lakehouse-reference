# Clean Slate Guide — 重置与历史回填（ECS 语境）

各层状态的持久性与重置手段（详细部署见 `runbook-ecs-deploy.md`）。

## 重启后什么会丢

| 状态 | 重启后 | 恢复方式 |
|---|---|---|
| Iggy topic/消息（/data/iggy 在数据盘） | 实例重启保留；盘损坏丢 | poller/n8n 启动自动重建 stream/topic |
| Flink 作业 | JM/TM 重启后从 OSS-HDFS checkpoints 自动恢复（ZK HA） | 无需重提交；彻底重置见下 |
| n8n staticData（仓位/现金/killswitch） | 保留（/var/lib/n8n SQLite） | 备份=拷文件 |
| LanceDB | 保留（/data/lancer） | indexer timer 每小时增量；可整库重建 |
| Prometheus/Grafana | 保留（/data/*） | TSDB 7d 滚动 |
| 湖数据（Paimon/Iceberg） | 持久（OSS-HDFS） | — |

## 分级重置

### L1 — 重启单个服务
```bash
systemctl restart <poller|bridge|lancer|query-api|n8n|prometheus|grafana>       # 应用面
systemctl restart <iggy|zookeeper|fluss-coordinator|fluss-tablet|flink-jobmanager|flink-taskmanager|jindofuse-warehouse>  # 数据面
```

### L2 — 清空交易状态（重新开始模拟盘）
```bash
# 1) n8n 决策状态归零：UI 里对 slr-trading-decision 执行 "Reset static data"
#    （或 SSH app-01: sqlite3 /var/lib/n8n/database.sqlite 清 static data 后 restart n8n）
# 2) 湖内账本清零 = 清 balance/trades 表（aggregation 表不能 DELETE 归零，直接删表重建）：
#    数据面执行 seed-balance.sql 前，先删 Paimon 表：
sudo -u flink env JAVA_HOME=/usr/lib/jvm/java-17-openjdk HADOOP_CONF_DIR=/opt/flink/conf/hadoop \
  /opt/flink/bin/sql-client.sh embedded   # DROP TABLE paimon_catalog.crypto.{balance,trades}; 然后重提 clearing-house + seed
rm -f /data/.slr-seed-done                # 清 seed 守卫标记
ansible-playbook playbook_jobs.yaml       # seed 守卫重新放行
```

### L3 — 彻底重置湖仓（全部历史数据）
```bash
# ⚠️ 不可逆。OSS-HDFS 数据删除：
ossutil rm -r oss://slr-lakehouse-prod/warehouse/ --endpoint cn-beijing.oss-dls.aliyuncs.com
# Flink HA 状态（否则作业带着旧 checkpoint 恢复）：
zkCli.sh -server 127.0.0.1:2181 deleteall /streaming-lakehouse
ossutil rm -r oss://slr-lakehouse-prod/flink/
# 之后：重启 flink-jobmanager/taskmanager → playbook_jobs.yaml → backfill（见下）→ lancer-indexer
```

### L4 — 推倒重来（基础设施）
```bash
terraform -chdir=iac/terraform/environments/prod destroy   # 保留 state 桶；OSS-HDFS 桶内数据需先手动清空
# 再走 runbook Phase 1-4
```

## 历史回填（backfill）

lancer 向量匹配与 replay 需要历史深度。在**数据面**（fuse rw）执行：

```bash
cd /opt/slr/scripts
python3 backfill-candles.py --days 60        # Coinbase REST 拉 1m 蜡烛 → parquet → /mnt/warehouse/backfill/
sudo -u flink env JAVA_HOME=/usr/lib/jvm/java-17-openjdk HADOOP_CONF_DIR=/opt/flink/conf/hadoop \
  /opt/flink/bin/sql-client.sh embedded -f /opt/flink/sql/import-backfill.sql   # filesystem → Paimon ohlcv_1m
systemctl start lancer-indexer               # 重建 LanceDB（此后每小时 timer 增量）
```

历史回放（可选）：app 面配 `/etc/slr/replay.env`（REPLAY_SPEED/DATE/PAIRS）→
`systemctl start replay`；回放走 `crypto/replay` topic + replay-pipeline.sql 写独立 Paimon 表。

## 验证重置成功

```bash
curl -s localhost:8081/jobs/overview | python3 -m json.tool | grep -c RUNNING   # ≥3
curl -s localhost:8009/api/balance    # L2/L3 后应回到 seed 的 1000 USD
curl -s localhost:8009/api/state      # n8n staticData 已归零/滚动
```
