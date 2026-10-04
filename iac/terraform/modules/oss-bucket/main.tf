# ============================================================
# OSS Bucket 模块 — 主资源定义
# ============================================================

# 直接管理 bucket。
# ⚠️ 原「data source 查到已存在则 count=0」模式是反模式（T0 实证）：首次 apply 创建后，
# 第二次 plan data source 查到 TF 自己创建的资源 → count=0 → 计划销毁自己 state 里的
# 资源（销毁后下次 plan 又查不到 → 再创建，永久摇摆）。TF 是唯一事实来源，直接管理；
# 若 bucket 已被 state 外创建，apply 会显式报「已存在」而非静默收养（正确行为）。
resource "alicloud_oss_bucket" "this" {
  bucket = var.bucket_name

  storage_class   = "Standard"
  redundancy_type = "LRS"

  tags = merge(var.tags, {
    Name = var.bucket_name
  })

  # 生命周期规则
  dynamic "lifecycle_rule" {
    for_each = var.lifecycle_rules
    content {
      id      = lifecycle_rule.value.id
      prefix  = lifecycle_rule.value.prefix
      enabled = lifecycle_rule.value.status == "Enabled"

      dynamic "expiration" {
        for_each = lifecycle_rule.value.expiration_days > 0 ? [1] : []
        content {
          days = lifecycle_rule.value.expiration_days
        }
      }

      # 存储类型转换（Standard→IA/Archive 等，冷数据降本）
      dynamic "transitions" {
        for_each = lifecycle_rule.value.transitions
        content {
          days          = transitions.value.days
          storage_class = transitions.value.storage_class
        }
      }
    }
  }
}
# ACL 独立资源（provider ≥1.220 弃用 alicloud_oss_bucket.acl 内联字段）。
# private 是桶默认，不建资源；仅 public-read（artifacts 桶）显式声明。
resource "alicloud_oss_bucket_acl" "this" {
  count = var.acl != "private" ? 1 : 0

  bucket = alicloud_oss_bucket.this.bucket
  acl    = var.acl
}
