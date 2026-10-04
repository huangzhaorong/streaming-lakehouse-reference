# ============================================================
# SLR prod 环境 — Terraform 主入口
# spec: docs/specs/2026-10-04-ecs-native-oss-hdfs-n8n-design.md
# 模式承袭 adtrader-iac-infra/terraform/environments/atx_v2（实证裁决随注释保留）：
#   - 不新建网络：VPC/vSwitch data source 只读引用
#   - SG 组内互通用原生 inner_access_policy=Accept（阿里云 API 拒绝 SG 自引用规则，atx_v2 T0 实证）
#   - OSS bucket 直接管理（data source + count 反模式会计划销毁自己 state，atx_v2 注释）
# ============================================================

terraform {
  required_version = ">= 1.9"

  required_providers {
    alicloud = {
      source  = "hashicorp/alicloud"
      version = ">= 1.230"
    }
  }

  backend "oss" {
    # 参数通过 -backend-config=backend.conf 指定
  }
}

provider "alicloud" {
  region = var.region
  # 凭证：ALICLOUD_ACCESS_KEY/SECRET 环境变量或 aliyun CLI profile，不落盘
}

# ─── Data Sources ───

data "alicloud_vpcs" "main" {
  name_regex = var.vpc_name
}

data "alicloud_vswitches" "main" {
  vpc_id     = data.alicloud_vpcs.main.ids[0]
  cidr_block = var.vswitch_cidr
}

data "alicloud_images" "os" {
  name_regex  = var.image_name_regex
  most_recent = true
  owners      = "system"
}

# ─── 模块调用 ───

module "security_group" {
  source = "../../modules/security-group"

  vpc_id         = data.alicloud_vpcs.main.ids[0]
  sg_name        = "sg-slr-prod"
  sg_description = "SLR 原生部署安全组（组内互通 Accept；公网入向仅 SSH/UI 白名单）"
  rules          = local.sg_rules
  # 阿里云 API 拒绝 SG 自引用规则（atx_v2 T0 实证）→ 组内互访用原生 inner_access_policy
  inner_access_policy = "Accept"
  tags                = var.tags
}

# 湖仓桶：Paimon/Iceberg/checkpoints/savepoints/ha + reconcile JSON。
# ⚠️ OSS-HDFS（JindoFS 服务）开通是控制台带外操作（TF 无此能力），见 runbook Phase 1。
module "oss_bucket" {
  source = "../../modules/oss-bucket"

  bucket_name     = var.oss_bucket_name
  region          = var.region
  lifecycle_rules = [] # YAGNI：清理策略跑稳后再加
  tags            = var.tags
}

# Flink state 备用桶（R1 回退路径，承 adtrader 2026-10-04 方案F2 裁决）：
# 主路径 jindo-flink 全走湖仓桶（单一 Jindo 栈无 endpoint 语义冲突）；
# 若 G1 金丝雀失败回退 flink-oss-fs-hadoop 插件，则 checkpoints/savepoints/ha
# 迁本桶（普通 OSS 命名空间），湖仓桶继续 JindoSDK bucket 级 dls 路由——物理分桶
# 消除同一 fs.oss.endpoint 键在插件/Jindo 两层的语义冲突。空桶零成本。
module "oss_bucket_flink_state" {
  source = "../../modules/oss-bucket"

  bucket_name     = "${var.oss_bucket_name}-state"
  region          = var.region
  lifecycle_rules = []
  tags            = var.tags
}

# 部署工件桶：iggy 提取件、离线 Grafana 插件等公开二进制（public-read，无秘密）
module "oss_bucket_artifacts" {
  source = "../../modules/oss-bucket"

  bucket_name     = var.artifacts_bucket_name
  region          = var.region
  acl             = "public-read"
  lifecycle_rules = []
  tags            = var.tags
}

module "ram_role" {
  source = "../../modules/ram-role"

  role_name        = var.ram_role_name
  role_description = "SLR ECS OSS/OSS-HDFS 访问角色（instance metadata 免 AK）"
  oss_bucket_names = [var.oss_bucket_name, "${var.oss_bucket_name}-state", var.artifacts_bucket_name]
  tags             = var.tags
}

resource "alicloud_ecs_key_pair" "main" {
  count = var.ssh_public_key != "" ? 1 : 0

  key_pair_name = "slr-prod-key"
  public_key    = var.ssh_public_key
  tags          = var.tags
}

module "ecs_instances" {
  source   = "../../modules/ecs-instance"
  for_each = var.slr_instances

  instance_group              = each.key
  instance_count              = each.value.count
  instance_type               = each.value.instance_type
  image_id                    = data.alicloud_images.os.images[0].id
  system_disk_size            = each.value.disk_size
  data_disk_size              = each.value.data_disk_size
  data_disk_performance_level = each.value.data_disk_performance_level
  internet_max_bandwidth_out  = each.value.internet_max_bandwidth_out
  vswitch_id                  = data.alicloud_vswitches.main.ids[0]
  security_group_id           = module.security_group.security_group_id
  private_ips                 = each.value.private_ips
  hostnames                   = each.value.hostnames
  hostname_prefix             = each.value.hostname_prefix
  ram_role_name               = var.ram_role_name
  key_name                    = var.ssh_public_key != "" ? alicloud_ecs_key_pair.main[0].key_pair_name : ""
  charge_type                 = each.value.charge_type
  spot_strategy               = each.value.spot_strategy
  period                      = each.value.period
  tags                        = var.tags
}
