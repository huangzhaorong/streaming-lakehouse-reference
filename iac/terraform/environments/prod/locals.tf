# ============================================================
# SLR prod — 安全组规则矩阵（最小公网暴露）
# 组内互通（三台 ECS 全端口）由 inner_access_policy=Accept 承担，无规则条目；
# 跨机端口（8090/3000/2181/9123/9124/8081/9249/8000-8010/9090/3001/5678）因此全通。
# ============================================================

locals {
  sg_rules = flatten([
    # SSH：仅管理员白名单（唯一必备公网入向）
    [
      for cidr in var.admin_cidrs : {
        description = "SSH from admin"
        direction   = "ingress"
        port_range  = "22/22"
        protocol    = "tcp"
        cidr_ip     = cidr
      }
    ],

    # 可选 UI 直连（Grafana/n8n/Flink UI/Prometheus）——默认 var.ui_cidrs=[] 即不开，
    # 日常经 SSH 隧道访问（ssh -L 3001:localhost:3001 等，见 runbook）
    length(var.ui_cidrs) > 0 ? [
      for cidr in var.ui_cidrs : {
        description = "SLR UIs from whitelisted cidr"
        direction   = "ingress"
        port_range  = "3001/3001"
        protocol    = "tcp"
        cidr_ip     = cidr
      }
    ] : [],
    length(var.ui_cidrs) > 0 ? [
      for cidr in var.ui_cidrs : {
        description = "n8n from whitelisted cidr"
        direction   = "ingress"
        port_range  = "5678/5678"
        protocol    = "tcp"
        cidr_ip     = cidr
      }
    ] : [],
    length(var.ui_cidrs) > 0 ? [
      for cidr in var.ui_cidrs : {
        description = "Flink UI from whitelisted cidr"
        direction   = "ingress"
        port_range  = "8081/8081"
        protocol    = "tcp"
        cidr_ip     = cidr
      }
    ] : [],
    length(var.ui_cidrs) > 0 ? [
      for cidr in var.ui_cidrs : {
        description = "Prometheus from whitelisted cidr"
        direction   = "ingress"
        port_range  = "9090/9090"
        protocol    = "tcp"
        cidr_ip     = cidr
      }
    ] : [],

    # Egress 全放行（Coinbase WSS / DashScope / 装机下载 / OSS 内网）
    [{
      description = "Allow all egress"
      direction   = "egress"
      port_range  = "1/65535"
      protocol    = "all"
      cidr_ip     = "0.0.0.0/0"
    }],
  ])
}
