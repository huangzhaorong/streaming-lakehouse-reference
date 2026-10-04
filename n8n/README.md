# n8n 决策链（替代 analyst + consensus）

`trading-decision` workflow 承载完整决策链：信号拉取 → LLM 情绪（DashScope Qwen，可切换模型）→ 风控裁决 → Iggy HTTP 发单 → Grafana 注解 → 状态镜像到 query-api。

## 文件

| 文件 | 说明 |
|---|---|
| `workflows/decide.js` | 风控核心**唯一事实源**：纯函数 `decide()/updateEquity()/DEFAULTS` + n8n 包装器。阈值与原 consensus 逐字一致 |
| `build_workflow.py` | 组装 `workflows/trading-decision.json`（RiskDecide 节点内嵌 decide.js 全文） |
| `workflows/trading-decision.json` | 生成物（入库；`n8n import:workflow` 直接消费） |
| `tests/decide.test.mjs` | decide/updateEquity 单测（14 例，覆盖全部 BUY/SELL/BLOCKED/熔断分支） |
| `tests/test_workflow_json.py` | JSON 结构 + decide.js 同步 + 构建可重现门禁 |
| `credentials/*.json.example` | 凭据结构参考；真值由 Ansible vault 渲染后 `n8n import:credentials` 导入 |

**修改 decide.js 后必须**：`python3 n8n/build_workflow.py`（否则 test_workflow_json.py 的同步断言失败）。

## 测试

```bash
node --test n8n/tests/*.mjs
PYTHONPATH=services/query-api/_deps python3 -m pytest n8n/tests/test_workflow_json.py -q
```

## 运行时环境变量（Ansible 渲染进 /etc/n8n/n8n.env，进程 env 经 $env 读取）

| 变量 | 默认 | 说明 |
|---|---|---|
| `SLR_TRADING_PAIRS` | BTC-USD,ETH-USD,SOL-USD,DOGE-USD,AVAX-USD,LINK-USD | 监控 pair |
| `SLR_PROM_URL` | http://localhost:9090 | Prometheus |
| `SLR_LANCER_URL` | http://localhost:8003 | lancer signal API |
| `SLR_QUERY_API_URL` | http://localhost:8009 | query-api（balance/state） |
| `SLR_IGGY_HTTP_URL` | http://127.0.0.1:3000 | **应用面须指向数据面 IP** |
| `SLR_IGGY_USER` / `SLR_IGGY_PASSWORD` | iggy/iggy | Iggy 登录（vault 下发） |
| `SLR_GRAFANA_URL` | http://localhost:3001 | 注解推送 |
| `SLR_LLM_MODEL` | qwen-plus | DashScope 模型（qwen-turbo/plus/max 可切换） |
| `N8N_ENCRYPTION_KEY` | — | vault 下发；import:credentials 与运行实例必须同 key |

DashScope API key 走 credential（`DashScope` httpHeaderAuth），不落 workflow JSON。

## 状态语义

- 仓位/现金/熔断/killswitch 全部存 **workflow staticData**（n8n 仅在整次执行**成功**后持久化进 SQLite——失败轮次不落账，偏保守，等价原 consensus 原子写）
- 每轮末 `POST query-api /api/state` 镜像一份供 Grafana/运维只读
- 持久审计 = Iggy orders → Flink clearing → Paimon trades（湖内不可变）

## killswitch

```bash
curl -X POST http://<app_ip>:5678/webhook/killswitch -H 'Content-Type: application/json' -d '{"killswitch": true}'   # 停止开仓（平仓永远放行）
curl http://<app_ip>:5678/webhook/killswitch                                                                        # 查看状态
curl -X POST .../webhook/killswitch -d '{"killswitch": false}'                                                      # 恢复
```
生产 webhook 走 test/production 前缀由 n8n 决定（publish 后为 `/webhook/killswitch`）。兜底：`n8n unpublish:workflow --id=slr-trading-decision`。

## 部署（Ansible n8n role 自动执行）

```bash
sudo -u n8n n8n import:workflow --separate --input=/opt/n8n/workflows/
sudo -u n8n n8n import:credentials --input=/opt/n8n/credentials/
sudo -u n8n n8n publish:workflow --id=slr-trading-decision
```
owner 账号首启需 Web 建立（带外，SSH 隧道 `ssh -L 5678:localhost:5678 <app_ip>`，见 runbook）。

ponytail: Iggy token 每 pair 每轮重新 login（无缓存/401 重试逻辑）——6 次/分钟 HTTP 登录开销可忽略；若 Iggy 侧成为瓶颈再加 token 缓存。
