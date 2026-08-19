# Amazon S3 Express One Zone 使用指南

本文介绍如何在 Apache Doris 中使用 Amazon S3 Express One Zone directory bucket 完成以下操作：

- 通过 S3 TVF 或 S3 Load/Broker Load 导入文件。
- 在存算分离集群中创建和使用 S3 Express Storage Vault。

## 功能范围

| 场景 | 支持情况 |
| --- | --- |
| 使用 S3 TVF 查询 directory bucket 中的文件 | 支持 |
| `INSERT INTO ... SELECT ... FROM S3(...)` | 支持 |
| 使用 `WITH S3` 提交 S3 Load/Broker Load | 支持 |
| 在存算分离集群中使用 S3 Express Storage Vault | 支持 |
| S3 Streaming Job | 不支持；CREATE/ALTER 阶段会拒绝 |
| COPY INTO | 不在当前支持范围内 |
| Path-style 访问 | 不支持 |
| Anonymous 凭证 | 不支持 |
| 显式 dualstack/IPv6 endpoint 选择 | 不在当前支持范围内 |

本文只讨论使用 directory bucket API 的 S3 Express One Zone。普通 S3 bucket 和其他
S3-compatible 对象存储继续使用原有 Doris 配置与访问路径。

## 前置条件

### 创建 directory bucket

S3 Express directory bucket 名称必须包含 Zone ID，格式为：

```text
<bucket-base-name>--<zone-id>--x-s3
```

例如：

```text
doris-data--usw2-az1--x-s3
```

directory bucket 只能通过 virtual-hosted-style Zonal endpoint 访问，不支持 path-style。

### 准备 Region 和 endpoint

需要配置 bucket 所在的 AWS Region，例如：

```text
us-west-2
```

建议在 Doris 配置中使用与 bucket Zone 对应的官方 S3 Express Zonal endpoint：

```text
https://s3express-usw2-az1.us-west-2.amazonaws.com
```

Doris 不会将该 endpoint 直接作为对象请求的 endpoint override。AWS SDK 会根据完整的
directory bucket 名称和 Region 自动解析实际的 Zonal endpoint，例如：

```text
doris-data--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com
```

### 配置 IAM 权限

访问 directory bucket 的 IAM 用户或角色至少需要目标 bucket 上的
`s3express:CreateSession` 权限。Doris 使用的 AWS SDK 会自动创建和刷新 S3 Express
session credentials。请根据使用 identity policy 还是 bucket policy，参考 AWS 官方文档配置
资源 ARN 和其他限制条件。

如果 bucket 使用 SSE-KMS，还需要按照 AWS 要求配置相应的 KMS 权限。

## 使用 S3 TVF 导入

### 查询 CSV 文件

推荐显式设置 `provider = S3EXPRESS`：

```sql
SELECT *
FROM S3(
    "uri" = "s3://doris-data--usw2-az1--x-s3/import/orders/*.csv",
    "provider" = "S3EXPRESS",
    "s3.endpoint" = "https://s3express-usw2-az1.us-west-2.amazonaws.com",
    "s3.region" = "us-west-2",
    "s3.access_key" = "<your-access-key>",
    "s3.secret_key" = "<your-secret-key>",
    "use_path_style" = "false",
    "format" = "csv"
);
```

S3 TVF 支持的文件格式与普通 S3 相同，包括：

- `csv`
- `csv_with_names`
- `csv_with_names_and_types`
- `json`
- `parquet`
- `orc`

### 导入到 Doris 表

```sql
INSERT INTO target_orders
SELECT *
FROM S3(
    "uri" = "s3://doris-data--usw2-az1--x-s3/import/orders/*.parquet",
    "provider" = "S3EXPRESS",
    "s3.endpoint" = "https://s3express-usw2-az1.us-west-2.amazonaws.com",
    "s3.region" = "us-west-2",
    "s3.access_key" = "<your-access-key>",
    "s3.secret_key" = "<your-secret-key>",
    "use_path_style" = "false",
    "format" = "parquet"
);
```

### Provider 自动识别

对于导入场景，Doris 根据配置计算 effective provider：

| 用户配置 | Effective provider |
| --- | --- |
| `provider = S3EXPRESS` | `S3EXPRESS` |
| `provider = S3`，endpoint 为 AWS S3 Express endpoint | `S3EXPRESS` |
| 未设置 provider，endpoint 为 AWS S3 Express endpoint | `S3EXPRESS` |
| `provider = S3`，endpoint 为普通 S3 endpoint | `S3` |
| GCS、MinIO 等配置 | 保持对应 provider |

建议新配置始终显式使用 `provider = S3EXPRESS`，避免 endpoint 写错导致 provider 识别不符合预期。

## 使用 S3 Load/Broker Load 导入

```sql
LOAD LABEL demo.s3_express_load
(
    DATA INFILE("s3://doris-data--usw2-az1--x-s3/import/orders/*.csv")
    INTO TABLE target_orders
    FORMAT AS "CSV"
    COLUMNS TERMINATED BY ","
)
WITH S3
(
    "provider" = "S3EXPRESS",
    "AWS_ENDPOINT" = "https://s3express-usw2-az1.us-west-2.amazonaws.com",
    "AWS_REGION" = "us-west-2",
    "AWS_ACCESS_KEY" = "<your-access-key>",
    "AWS_SECRET_KEY" = "<your-secret-key>",
    "use_path_style" = "false"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

S3 Express LIST 只接受以 `/` 结尾的 prefix。Doris 会将文件 glob 扩大到对应目录 prefix，
使用 continuation token 拉取所有页面，然后在本地过滤实际匹配的文件。

## 使用默认凭证链或 Assume Role

如果所有会访问 bucket 的 Doris 组件（FE、BE 和 Recycler）都能访问一致的 AWS 默认凭证链，
可以省略 AK/SK，并设置：

```text
s3.credentials_provider_type = DEFAULT
```

也可以配置 Assume Role：

```text
s3.credentials_provider_type = INSTANCE_PROFILE
s3.role_arn = arn:aws:iam::<account-id>:role/<role-name>
s3.external_id = <external-id>
```

不要将 `s3.credentials_provider_type` 设置为 `ANONYMOUS`。

## 创建 S3 Express Storage Vault

Storage Vault 只适用于 Doris 存算分离部署。与导入场景不同，创建新的 S3 Express
Storage Vault 时必须显式设置 `provider = S3EXPRESS`。

> **重要：** 不要在用于 Doris Storage Vault 的 directory bucket 或 `s3.root.path` 前缀上
> 配置会删除当前对象的 Lifecycle Expiration 规则。directory bucket 不支持 Versioning，
> 过期删除后无法通过历史版本恢复；当前 Doris Checker 也不会主动发现该规则。

```sql
CREATE STORAGE VAULT IF NOT EXISTS s3_express_vault
PROPERTIES (
    "type" = "S3",
    "provider" = "S3EXPRESS",
    "s3.endpoint" = "https://s3express-usw2-az1.us-west-2.amazonaws.com",
    "s3.region" = "us-west-2",
    "s3.bucket" = "doris-data--usw2-az1--x-s3",
    "s3.root.path" = "doris/warehouse",
    "s3.access_key" = "<your-access-key>",
    "s3.secret_key" = "<your-secret-key>",
    "use_path_style" = "false"
);
```

默认情况下，创建时 Doris 会执行对象存储连通性检查，包括 PUT、HEAD、LIST、multipart
upload 和 DELETE。因此配置的凭证和 bucket policy 必须允许这些操作。

### 使用 Assume Role 创建 Storage Vault

```sql
CREATE STORAGE VAULT IF NOT EXISTS s3_express_role_vault
PROPERTIES (
    "type" = "S3",
    "provider" = "S3EXPRESS",
    "s3.endpoint" = "https://s3express-usw2-az1.us-west-2.amazonaws.com",
    "s3.region" = "us-west-2",
    "s3.bucket" = "doris-data--usw2-az1--x-s3",
    "s3.root.path" = "doris/warehouse",
    "s3.credentials_provider_type" = "INSTANCE_PROFILE",
    "s3.role_arn" = "arn:aws:iam::<account-id>:role/<role-name>",
    "s3.external_id" = "<external-id>",
    "use_path_style" = "false"
);
```

使用 Assume Role 时不要同时设置 `s3.access_key` 和 `s3.secret_key`。

### 设置默认 Storage Vault

```sql
SET s3_express_vault AS DEFAULT STORAGE VAULT;
```

### 为表指定 Storage Vault

```sql
CREATE TABLE orders (
    order_id BIGINT,
    amount DECIMAL(18, 2)
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 8
PROPERTIES (
    "storage_vault_name" = "s3_express_vault"
);
```

表创建后不能切换到其他 Storage Vault。

## 修改 S3 Express Storage Vault

### 轮换 AK/SK

AK 和 SK 必须一起修改：

```sql
ALTER STORAGE VAULT s3_express_vault
PROPERTIES (
    "type" = "S3",
    "s3.access_key" = "<new-access-key>",
    "s3.secret_key" = "<new-secret-key>"
);
```

### 切换或轮换 IAM Role

```sql
ALTER STORAGE VAULT s3_express_vault
PROPERTIES (
    "type" = "S3",
    "s3.credentials_provider_type" = "INSTANCE_PROFILE",
    "s3.role_arn" = "arn:aws:iam::<account-id>:role/<new-role-name>",
    "s3.external_id" = "<new-external-id>"
);
```

S3 Express Storage Vault 的 ALTER 操作会拒绝：

- `use_path_style = true`
- `s3.credentials_provider_type = ANONYMOUS`

`provider`、bucket、endpoint、Region 和 root path 不属于可修改属性。如需更换这些配置，
请创建新的 Storage Vault。

## 实现相关说明

- Doris 使用 AWS SDK 管理 S3 Express session credentials，不需要用户手动创建 session token。
- 对象请求由 SDK 根据 directory bucket 名称和 Region 解析 Zonal endpoint。
- 单对象上传和 multipart upload 使用 CRC32C；multipart complete 会携带每个 part 的 CRC32C。
- multipart part number 必须从 1 开始连续编号。
- Recycler 会按照 directory bucket 的 LIST 约束扩大目录 prefix，并在 Doris 内部过滤目标 key。
- directory bucket 不支持 S3 Versioning。当前 Doris Checker 不检查 S3 Express bucket 的
  current-object Lifecycle Expiration，因此必须由管理员保证 Doris 数据前缀没有自动过期规则。

## 限制

### Streaming Job

S3 Streaming Job 使用 key 作为跨批次 offset，需要 `StartAfter` 和按 key 字典序返回的 LIST。
directory bucket 不支持这两项语义，因此 Doris 会在 CREATE 或修改 S3 TVF SQL 的 ALTER 阶段拒绝
effective provider 为 `S3EXPRESS` 的 Streaming Job。

### COPY INTO

COPY INTO 不在当前 S3 Express 支持范围内。

### dualstack endpoint

当前版本不保证将用户配置的 S3 Express dualstack endpoint 传播为 SDK dualstack 选项。需要
IPv6-only 访问时，请先验证运行环境；当前文档只承诺标准非-dualstack endpoint。

### 其他写入路径

本文只承诺 Doris 内部 Storage Vault 写入。Hive 等外部表格式的 deferred multipart commit
路径不在当前支持范围内。

### Lifecycle 与数据保护

不要为 Doris Storage Vault 的 directory bucket 或数据前缀配置 current-object Expiration。
如需使用 `AbortIncompleteMultipartUpload` 清理未完成的 multipart upload，请单独验证规则不会
覆盖已完成的 Doris 数据对象。由于 directory bucket 不支持 Versioning，误删数据无法依赖
noncurrent version 恢复。

## 常见错误

### `S3 Express requires virtual-hosted-style access`

确认设置：

```text
use_path_style = false
```

### `S3 Express does not support anonymous access`

配置 AK/SK、AWS 默认凭证链或 Assume Role，不要使用 `ANONYMOUS`。

### `Region is not set`

显式设置 bucket 所在 Region：

```text
s3.region = us-west-2
```

### 无法创建 session 或返回 AccessDenied

确认 IAM 用户或角色拥有目标 directory bucket 的 `s3express:CreateSession` 权限，并确认
FE、BE 和 Recycler 所在网络能够访问 AWS S3 endpoint。

## 参考资料

- [Apache Doris S3 TVF](https://doris.apache.org/docs/dev/sql-manual/sql-functions/table-valued-functions/s3)
- [Apache Doris Broker Load](https://doris.apache.org/docs/3.x/data-operate/import/import-way/broker-load-manual/)
- [Apache Doris Storage Vault 管理](https://doris.apache.org/docs/dev/compute-storage-decoupled/managing-storage-vault/)
- [AWS directory bucket 命名规则](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-tutorial-create-directory-bucket.html)
- [AWS S3 Express endpoint](https://docs.aws.amazon.com/AmazonS3/latest/userguide/endpoint-directory-buckets-AZ.html)
- [AWS CreateSession 鉴权](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-create-session.html)
- [AWS S3 Express Service Authorization Reference](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3express.html)
- [AWS directory bucket multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-using-multipart-upload.html)
- [AWS directory bucket Lifecycle](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html)
