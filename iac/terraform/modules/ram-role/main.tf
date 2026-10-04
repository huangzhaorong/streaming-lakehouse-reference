# ============================================================
# RAM 角色模块 — 主资源定义
# ============================================================

# 直接管理 RAM 角色（原 data source + count 反模式会计划销毁自己 state 里的角色，
# 同 oss-bucket 模块注释，T0 实证后移除）
resource "alicloud_ram_role" "this" {
  role_name   = var.role_name
  description = var.role_description
  assume_role_policy_document = jsonencode({
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = ["ecs.aliyuncs.com"]
        }
      }
    ]
    Version = "1"
  })

  force = true
}

# OSS 访问策略
resource "alicloud_ram_policy" "oss_access" {
  policy_name = "${var.role_name}-oss-policy"
  description = "OSS 读写权限 — ${var.role_name}"

  policy_document = jsonencode({
    Statement = flatten([
      for bucket in var.oss_bucket_names : [
        {
          Effect = "Allow"
          Action = [
            "oss:GetObject",
            "oss:PutObject",
            "oss:DeleteObject",
            "oss:ListObjects",
            "oss:AbortMultipartUpload",
            "oss:ListParts",
            "oss:GetBucketInfo",
            "oss:GetBucketStat",
          ]
          Resource = [
            "acs:oss:*:*:${bucket}",
            "acs:oss:*:*:${bucket}/*",
          ]
        }
      ]
    ])
    Version = "1"
  })

  force = true
}

# 绑定策略到角色
resource "alicloud_ram_role_policy_attachment" "oss_access" {
  role_name   = var.role_name
  policy_name = alicloud_ram_policy.oss_access.policy_name
  policy_type = "Custom"
}