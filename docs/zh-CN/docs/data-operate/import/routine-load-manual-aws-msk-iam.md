# Routine Load 使用 AWS MSK IAM 认证

## 背景

AWS Managed Streaming for Apache Kafka (MSK) 支持使用 AWS IAM (Identity and Access Management) 进行身份验证和授权。这提供了一种更安全的方式来访问 Kafka 集群，无需管理用户名和密码，而是使用 IAM 角色和策略。

## 概念说明

### AWS MSK
Amazon Managed Streaming for Apache Kafka (MSK) 是 AWS 提供的完全托管的 Apache Kafka 服务。

### VPC (Virtual Private Cloud)
AWS 的虚拟私有云，提供了网络隔离环境。

### IAM Role
AWS 的身份和访问管理角色，用于授予临时权限。使用 IAM Role 的好处：
- **无需硬编码凭证**：不需要在配置中存储 access key 和 secret key
- **自动轮换凭证**：AWS 自动管理临时凭证的轮换
- **细粒度权限控制**：通过 IAM 策略精确控制访问权限
- **审计追踪**：所有访问都可以通过 AWS CloudTrail 追踪

## 功能说明

Doris Routine Load 现在支持使用 AWS MSK IAM 认证来消费 Kafka 数据。这允许运行在 AWS 环境中的 Doris 集群（如 EC2、ECS、EKS 等）通过 IAM Role 安全地访问 MSK 集群。

### 支持的认证方式

1. **AWS_MSK_IAM 机制**（推荐，需要 aws-msk-iam-auth 库支持）
2. **OAUTHBEARER 机制**（使用 AWS SDK 提供的 OAuth token）

## 前置条件

### 1. AWS 环境配置

确保您的 Doris 集群运行在 AWS 环境中，并且：

- **EC2 实例**：为 EC2 实例附加具有 MSK 访问权限的 IAM 角色
- **ECS/EKS**：为任务或 Pod 配置服务账户并关联 IAM 角色
- **本地开发**：配置 AWS CLI 凭证文件（`~/.aws/credentials`）

### 2. IAM 策略配置

创建 IAM 策略，授予对 MSK 集群的访问权限：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster"
      ],
      "Resource": "arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:ReadData",
        "kafka-cluster:DescribeGroup"
      ],
      "Resource": [
        "arn:aws:kafka:us-east-1:123456789012:topic/my-cluster/*",
        "arn:aws:kafka:us-east-1:123456789012:group/my-cluster/*"
      ]
    }
  ]
}
```

### 3. MSK 集群配置

确保 MSK 集群启用了 IAM 访问控制：
- 在创建 MSK 集群时选择"IAM role-based authentication"
- 或者在现有集群中启用 IAM 认证

### 4. BE 节点要求

- Doris BE 节点使用 librdkafka 2.11.0+ 版本（Doris 1.2+ 自带）
- BE 节点需要能够访问 AWS metadata service（用于获取 IAM 临时凭证）
- 确保 BE 节点具有网络访问 MSK broker 的权限（通常在同一 VPC 内）

## 使用方法

### 方法一：使用 AWS_MSK_IAM 机制（推荐）

这是最直接的方式，但需要 librdkafka 支持 AWS_MSK_IAM SASL 机制（需要额外的 aws-msk-iam-auth 库）。

```sql
CREATE ROUTINE LOAD job_name ON table_name
PROPERTIES
(
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "20",
    "max_batch_rows" = "300000",
    "max_batch_size" = "209715200"
)
FROM KAFKA
(
    "kafka_broker_list" = "b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098,b-2.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098",
    "kafka_topic" = "my_topic",
    "property.group.id" = "my_consumer_group",
    "property.security.protocol" = "SASL_SSL",
    "property.sasl.mechanism" = "AWS_MSK_IAM"
);
```

**可选配置**：如果需要指定特定的 IAM 角色或 AWS 配置文件：

```sql
FROM KAFKA
(
    "kafka_broker_list" = "b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098",
    "kafka_topic" = "my_topic",
    "property.group.id" = "my_consumer_group",
    "property.security.protocol" = "SASL_SSL",
    "property.sasl.mechanism" = "AWS_MSK_IAM",
    "property.aws.msk.iam.role.arn" = "arn:aws:iam::123456789012:role/MyMskRole",
    "property.aws.profile.name" = "default"
);
```

### 方法二：使用 OAUTHBEARER 机制

如果 librdkafka 不支持 AWS_MSK_IAM 机制，可以使用 OAUTHBEARER 机制：

```sql
CREATE ROUTINE LOAD job_name ON table_name
PROPERTIES
(
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "20"
)
FROM KAFKA
(
    "kafka_broker_list" = "b-1.msk-cluster.xxx.kafka.us-east-1.amazonaws.com:9098",
    "kafka_topic" = "my_topic",
    "property.group.id" = "my_consumer_group",
    "property.security.protocol" = "SASL_SSL",
    "property.sasl.mechanism" = "OAUTHBEARER"
);
```

## 完整示例

### 示例 1：基本配置

```sql
-- 创建数据表
CREATE TABLE kafka_data (
    id BIGINT,
    user_name VARCHAR(128),
    timestamp DATETIME,
    message TEXT
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 10
PROPERTIES ("replication_num" = "3");

-- 创建 Routine Load 任务
CREATE ROUTINE LOAD example_db.load_kafka_data ON kafka_data
COLUMNS(id, user_name, timestamp, message)
PROPERTIES
(
    "desired_concurrent_number" = "5",
    "max_batch_interval" = "20",
    "max_batch_rows" = "500000",
    "max_batch_size" = "104857600",
    "strict_mode" = "false"
)
FROM KAFKA
(
    "kafka_broker_list" = "b-1.my-msk.us-east-1.amazonaws.com:9098,b-2.my-msk.us-east-1.amazonaws.com:9098",
    "kafka_topic" = "user_events",
    "property.group.id" = "doris_consumer_group_1",
    "property.security.protocol" = "SASL_SSL",
    "property.sasl.mechanism" = "AWS_MSK_IAM"
);
```

### 示例 2：带分区指定

```sql
CREATE ROUTINE LOAD example_db.load_kafka_partitions ON kafka_data
PROPERTIES
(
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "15"
)
FROM KAFKA
(
    "kafka_broker_list" = "b-1.my-msk.us-east-1.amazonaws.com:9098",
    "kafka_topic" = "user_events",
    "kafka_partitions" = "0,1,2",
    "kafka_offsets" = "OFFSET_BEGINNING,OFFSET_BEGINNING,OFFSET_BEGINNING",
    "property.group.id" = "doris_partition_consumer",
    "property.security.protocol" = "SASL_SSL",
    "property.sasl.mechanism" = "AWS_MSK_IAM"
);
```

### 示例 3：跨账号访问（使用指定 IAM 角色）

```sql
CREATE ROUTINE LOAD example_db.load_cross_account ON kafka_data
FROM KAFKA
(
    "kafka_broker_list" = "b-1.partner-msk.us-west-2.amazonaws.com:9098",
    "kafka_topic" = "shared_topic",
    "property.group.id" = "doris_cross_account_consumer",
    "property.security.protocol" = "SASL_SSL",
    "property.sasl.mechanism" = "AWS_MSK_IAM",
    "property.aws.msk.iam.role.arn" = "arn:aws:iam::999888777666:role/CrossAccountMskRole"
);
```

## 配置参数说明

### 必需参数

| 参数 | 说明 | 示例值 |
|------|------|--------|
| kafka_broker_list | MSK broker 地址列表 | `b-1.xxx.kafka.us-east-1.amazonaws.com:9098` |
| kafka_topic | Kafka 主题名称 | `my_topic` |
| property.security.protocol | 安全协议，使用 IAM 认证必须为 SASL_SSL | `SASL_SSL` |
| property.sasl.mechanism | SASL 认证机制 | `AWS_MSK_IAM` 或 `OAUTHBEARER` |

### 可选参数

| 参数 | 说明 | 示例值 |
|------|------|--------|
| property.group.id | 消费者组 ID | `my_consumer_group` |
| property.aws.msk.iam.role.arn | 指定使用的 IAM 角色 ARN（跨账号访问时需要） | `arn:aws:iam::123456789012:role/MyRole` |
| property.aws.profile.name | AWS 配置文件名称 | `default` |
| property.session.timeout.ms | 会话超时时间（毫秒） | `45000` |
| property.max.poll.interval.ms | 最大轮询间隔（毫秒） | `300000` |

## 常见问题排查

### 问题 1：连接超时

**错误信息**：
```
Failed to connect to broker
```

**解决方案**：
1. 检查网络连接：确保 Doris BE 节点可以访问 MSK broker
2. 验证 VPC 安全组规则允许从 Doris BE 到 MSK 的流量
3. 检查 MSK 端口（通常 IAM 认证使用 9098 端口）

### 问题 2：认证失败

**错误信息**：
```
SASL authentication failed
```

**解决方案**：
1. 验证 IAM 角色已正确附加到 EC2 实例或 ECS/EKS 任务
2. 检查 IAM 策略是否包含必要的 kafka-cluster 权限
3. 确认 MSK 集群已启用 IAM 认证
4. 检查 BE 节点是否可以访问 AWS metadata service (169.254.169.254)

### 问题 3：权限不足

**错误信息**：
```
Not authorized to access topics
```

**解决方案**：
1. 检查 IAM 策略中的资源 ARN 是否正确
2. 确保策略包含对指定 topic 的 `kafka-cluster:ReadData` 权限
3. 验证 group 相关的权限（`kafka-cluster:DescribeGroup`）

### 问题 4：配置错误

**错误信息**：
```
property.security.protocol must be set to SASL_SSL
```

**解决方案**：
这是 Doris 的配置验证错误，确保：
- `property.security.protocol` 设置为 `SASL_SSL`
- `property.sasl.mechanism` 设置为 `AWS_MSK_IAM` 或 `OAUTHBEARER`
- 配置参数名称拼写正确（区分大小写）

### 问题 5：库不支持

**错误信息**：
```
Unknown sasl mechanism: AWS_MSK_IAM
```

**解决方案**：
这表示当前 librdkafka 不支持 AWS_MSK_IAM 机制。解决方案：
1. 改用 `OAUTHBEARER` 机制
2. 或者升级 librdkafka 并添加 aws-msk-iam-auth 库支持

## 与其他认证方式的对比

| 特性 | IAM 认证 | SASL/SCRAM | 无认证 |
|------|----------|------------|--------|
| 安全性 | 高 | 中 | 低 |
| 凭证管理 | AWS 自动管理 | 需手动管理用户名密码 | 无 |
| 权限控制 | IAM 策略 | Kafka ACL | 无 |
| 跨账号访问 | 支持 | 不支持 | 不适用 |
| 审计追踪 | CloudTrail | Kafka 日志 | 无 |
| 配置复杂度 | 中 | 低 | 最低 |

## 最佳实践

1. **使用最小权限原则**：只授予必要的 kafka-cluster 权限
2. **分离环境**：为不同环境（开发、测试、生产）使用不同的 IAM 角色
3. **监控和告警**：使用 CloudWatch 监控 MSK 连接和认证指标
4. **定期审计**：通过 CloudTrail 审查 MSK 访问日志
5. **网络隔离**：将 Doris 和 MSK 部署在同一 VPC 内以提高性能和安全性
6. **使用 VPC 终端节点**：如果跨 VPC 访问，考虑使用 VPC Peering 或 PrivateLink

## 参考资料

- [AWS MSK IAM 认证官方文档](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)
- [Doris Routine Load 文档](https://doris.apache.org/zh-CN/docs/data-operate/import/import-way/routine-load-manual)
- [librdkafka 配置参数](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
