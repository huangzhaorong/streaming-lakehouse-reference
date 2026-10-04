# ============================================================
# ECS 实例模块 — 创建阿里云 ECS 实例
# ============================================================

variable "instance_group" {
  description = "主机组名称（如 flink_jobmanagers）"
  type        = string
}

variable "instance_count" {
  description = "实例数量"
  type        = number
  default     = 1
}

variable "instance_type" {
  description = "ECS 实例规格"
  type        = string
}

variable "image_id" {
  description = "镜像 ID"
  type        = string
}

variable "system_disk_size" {
  description = "系统盘大小 (GB)"
  type        = number
  default     = 100
}

variable "system_disk_category" {
  description = "系统盘类型"
  type        = string
  default     = "cloud_essd"
}

variable "system_disk_performance_level" {
  description = "ESSD 性能等级"
  type        = string
  default     = "PL1"
}

variable "data_disk_size" {
  description = "数据盘大小 (GB)，0 表示不创建"
  type        = number
  default     = 0
}

variable "data_disk_category" {
  description = "数据盘类型"
  type        = string
  default     = "cloud_essd"
}

variable "data_disk_performance_level" {
  description = "数据盘 ESSD 性能等级"
  type        = string
  default     = "PL1"
}

variable "vswitch_id" {
  description = "交换机 ID"
  type        = string
}

variable "security_group_id" {
  description = "安全组 ID"
  type        = string
}

variable "private_ips" {
  description = "指定的内网 IP 列表"
  type        = list(string)
  default     = []
}

variable "ram_role_name" {
  description = "绑定到实例的 RAM 角色"
  type        = string
  default     = ""
}

variable "tags" {
  description = "资源标签"
  type        = map(string)
  default     = {}
}

variable "hostname_prefix" {
  description = "主机名前缀"
  type        = string
}

variable "hostnames" {
  description = "显式主机名列表（可选）。非空时逐台使用（instance_name 与 OS host_name 同步），用于续编命名（如 kafka04/vw07）；为空时回退 hostname_prefix-index 原行为"
  type        = list(string)
  default     = []
}

variable "charge_type" {
  description = "付费模式 (PrePaid | PostPaid)"
  type        = string
  default     = "PostPaid"
}

variable "period" {
  description = "包月时长 (仅 PrePaid)"
  type        = number
  default     = 1
}

variable "period_unit" {
  description = "包月单位 (Month | Year)"
  type        = string
  default     = "Month"
}

variable "spot_strategy" {
  description = "竞价策略 (NoSpot | SpotWithPriceLimit | SpotAsPriceGo)"
  type        = string
  default     = "NoSpot"
}

variable "user_data" {
  description = "cloud-init 初始化脚本"
  type        = string
  default     = ""
}

variable "key_name" {
  description = "绑定的 SSH 密钥对名称（空字符串表示不绑定）"
  type        = string
  default     = ""
}

variable "password" {
  description = "ECS 实例密码"
  type        = string
  default     = ""
  sensitive   = true
}
variable "internet_max_bandwidth_out" {
  description = "公网出带宽 Mbps。>0 时分配公网 IP（PayByTraffic）。SLR 需出向访问 Coinbase WSS/DashScope/装机下载；0=无公网 IP"
  type        = number
  default     = 0
}

variable "internet_charge_type" {
  description = "公网计费方式"
  type        = string
  default     = "PayByTraffic"
}
