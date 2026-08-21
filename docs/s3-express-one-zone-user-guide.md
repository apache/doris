# Amazon S3 Express One Zone 使用指南

本文介绍如何在 Apache Doris 中使用 Amazon S3 Express One Zone directory bucket 完成以下操作：

- 通过 S3 TVF 或 S3 Broker Load 导入文件。
- 在存算分离集群中创建和使用 S3 Express Storage Vault。

自 3.1.0 起，Doris 支持访问 S3 Directory Bucket 数据。原有使用方式要求用户显式填写完整的
S3 Express endpoint，具体使用方式请参见
[访问 S3 Directory Bucket](https://doris.apache.org/zh-CN/docs/3.x/lakehouse/storages/s3/#访问-s3-directory-bucket)。

在 `<版本号>` 中，Doris 完善了对 Amazon S3 Express One Zone 的支持：在兼容原有导入配置的
基础上，新增 `S3EXPRESS` provider，并支持在存算分离集群中创建和使用 S3 Express Storage Vault。
显式配置 `provider = S3EXPRESS` 时，用户不需要再配置 `s3.endpoint`。

## 1. 介绍 S3 Express One Zone

Amazon S3 Express One Zone 使用目录存储桶（directory bucket）在单个可用区内存储数据。
directory bucket 的名称包含 Zone ID，格式如下：

```text
<bucket-base-name>--<zone-id>--x-s3
```

例如：

```text
doris-data--usw2-az1--x-s3
```

directory bucket 使用可用区端点（Zonal endpoint）。Doris 根据完整的 directory bucket 名称和
Region 通过 AWS SDK 自动解析实际访问 endpoint，用户不需要配置 `s3.endpoint`。

以位于 `us-west-2` Region、Zone ID 为 `usw2-az1` 的 bucket 为例，实际访问 endpoint 的格式为：

```text
https://doris-data--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com
```



### 1.1 Doris 使用方式

Doris 支持通过以下方式访问 Amazon S3 Express One Zone：


| 场景                                                | 支持情况                      |
| ------------------------------------------------- | ------------------------- |
| 使用 S3 TVF 查询 directory bucket 中的文件                | 支持                        |
| 使用 `INSERT INTO ... SELECT ... FROM S3(...)` 导入文件 | 支持                        |
| 使用 S3 Broker Load 导入文件                            | 支持                        |
| 在存算分离集群中使用 S3 Express Storage Vault               | 支持                        |
| S3 Streaming Job                                  | 不支持，在 CREATE 或 ALTER 阶段拒绝 |
| COPY INTO                                         | 不在当前支持范围内                 |


普通 S3 bucket 和其他 S3-compatible 对象存储继续使用原有 Doris 配置与访问路径。

### 1.2 配置 IAM 权限

访问 directory bucket 的 IAM 用户或角色至少需要目标 bucket 上的
`s3express:CreateSession` 权限。Doris 使用的 AWS SDK 会自动创建和刷新 S3 Express session
credentials，用户不需要手动创建 S3 Express session token。

directory bucket 的 Zonal object API 不需要分别授予 `s3:GetObject`、`s3:PutObject` 或
`s3:DeleteObject` 等普通 S3 权限。`s3express:CreateSession` 创建的 session 决定该 bucket
上对象操作的权限。

当前 Doris 会请求 `ReadWrite` session，包括只读导入场景。为兼容该请求，AWS identity policy
中授予 `s3express:CreateSession` 时，不要设置 `s3express:SessionMode` 条件。如果将 session mode
限制为 `ReadOnly`，Doris 的 CreateSession 请求会被拒绝。

bucket 使用 SSE-KMS 时，还需要按照 AWS 要求授予对应的 KMS 权限。

`s3express:CreateSession` 是 bucket 级权限。identity policy 的 `Resource` 应填写完整的
directory bucket ARN，例如：

```text
arn:aws:s3express:<region>:<account-id>:bucket/<bucket-name>
```

不要在 ARN 后添加 `/*` 或 `s3.root.path`。`s3.root.path` 不能作为 IAM prefix 隔离边界。

Doris 支持以下凭证方式：


| 凭证方式        | 配置说明                                                                       |
| ----------- | -------------------------------------------------------------------------- |
| AK/SK       | 配置 `s3.access_key` 和 `s3.secret_key`，两者必须同时出现；临时凭证可同时配置 `s3.session_token` |
| AWS 默认凭证链   | 配置 `s3.credentials_provider_type = DEFAULT`                                |
| Assume Role | 配置 `s3.role_arn`，并按需配置 `s3.external_id` 和源凭证类型                             |


使用 EC2 Instance Profile 作为 Assume Role 源凭证时，可以使用以下配置：

```text
s3.credentials_provider_type = INSTANCE_PROFILE
s3.role_arn = arn:aws:iam::<account-id>:role/<role-name>
s3.external_id = <external-id>
```

使用 Assume Role 前，需要满足以下 IAM 条件：

- 源身份具有调用 `sts:AssumeRole` 的权限。
- 目标 Role 的 trust policy 信任源身份。
- 目标 Role 具有目标 directory bucket 上的 `s3express:CreateSession` 权限。

使用 Assume Role 时，不要同时配置静态 AK/SK。S3 Express 不支持
`s3.credentials_provider_type = ANONYMOUS`。

如果使用默认凭证链或 Assume Role，需要确保所有实际访问 directory bucket 的 Doris 组件都能
获得相应凭证。导入场景涉及 FE 和 BE；Storage Vault 场景涉及 FE、BE 和 Recycler。

有关 directory bucket 的 IAM 配置，请参见
[AWS S3 Express One Zone IAM 文档](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-security-iam.html)。

### 1.3 注意事项

- bucket 名称中的 Zone ID 和配置的 Region 必须相互匹配。
- 显式配置 `provider = S3EXPRESS` 时不要配置 `s3.endpoint`。Doris 会让 AWS SDK 根据 bucket
名称和 Region 解析 endpoint。
- directory bucket 只支持 virtual-hosted-style 访问，不支持 path-style 访问。请省略
`use_path_style`，或显式设置 `use_path_style = false`。
- S3 Express 不支持匿名访问。请配置 AK/SK、AWS 默认凭证链或 Assume Role。
- 新配置建议显式使用 `provider = S3EXPRESS`。不要将普通 S3 bucket 配置为 `S3EXPRESS`。



## 2. 导入 S3 Express One Zone 数据

Doris 可以使用 S3 TVF 或 S3 Broker Load 读取 directory bucket 中的文件。导入场景兼容
3.1.0 以来通过 S3 Express endpoint 识别 directory bucket 的配置方式。

### 2.1 配置说明

导入时常用参数如下：


| 参数                                | 是否必需      | 说明                                                            |
| --------------------------------- | --------- | ------------------------------------------------------------- |
| `uri` 或 `DATA INFILE`             | 是         | 文件 URI，例如 `s3://<bucket>/<prefix>/*.csv`                      |
| `provider`                        | 推荐        | 新配置建议设置为 `S3EXPRESS`                                          |
| `s3.region` 或 `AWS_REGION`        | 是         | bucket 所在 Region，例如 `us-west-2`                               |
| `s3.access_key` 和 `s3.secret_key` | 条件必需      | 使用静态凭证时必须成对配置                                                 |
| `s3.credentials_provider_type`    | 否         | 不使用静态 AK/SK 时指定凭证来源                                           |
| `use_path_style`                  | 否         | 必须省略或设置为 `false`                                              |
| `format` 或 `FORMAT AS`            | S3 TVF 必需 | S3 TVF 必须设置 `format`；Broker Load 可通过 `FORMAT AS` 指定，也可按文件后缀推断 |


Doris 根据 `provider` 和 endpoint 计算实际生效的 provider：


| 用户配置                                               | 实际生效的 provider | 结果               |
| -------------------------------------------------- | -------------- | ---------------- |
| `provider = S3EXPRESS`                             | `S3EXPRESS`    | 使用 S3 Express 实现 |
| `provider = S3`，endpoint 为 AWS S3 Express endpoint | `S3EXPRESS`    | 兼容原有配置           |
| 未配置 provider，endpoint 为 AWS S3 Express endpoint    | `S3EXPRESS`    | 兼容原有配置           |
| `provider = S3`，endpoint 为普通 S3 endpoint           | `S3`           | 保持普通 S3 行为       |
| GCS、MinIO 等配置                                      | 对应 provider    | 保持原有行为           |


新配置建议始终显式使用 `provider = S3EXPRESS`，并省略 endpoint。只有继续使用
`provider = S3` 或不配置 provider 的原有配置时，才需要通过完整 S3 Express endpoint 识别
directory bucket。

### 2.2 使用 S3 TVF 导入

以下示例直接查询 directory bucket 中的 CSV 文件：

```sql
SELECT *
FROM S3(
    "uri" = "s3://doris-data--usw2-az1--x-s3/import/orders/*.csv",
    "provider" = "S3EXPRESS",
    "s3.region" = "us-west-2",
    "s3.access_key" = "<your-access-key>",
    "s3.secret_key" = "<your-secret-key>",
    "use_path_style" = "false",
    "format" = "csv"
);
```

也可以通过 `INSERT INTO ... SELECT ... FROM S3(...)` 将文件导入 Doris 表：

```sql
INSERT INTO target_orders
SELECT *
FROM S3(
    "uri" = "s3://doris-data--usw2-az1--x-s3/import/orders/*.parquet",
    "provider" = "S3EXPRESS",
    "s3.region" = "us-west-2",
    "s3.access_key" = "<your-access-key>",
    "s3.secret_key" = "<your-secret-key>",
    "use_path_style" = "false",
    "format" = "parquet"
);
```

S3 TVF 支持 CSV、JSON、Parquet 和 ORC 等文件格式。完整参数和格式说明请参见
[S3 TVF](https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/table-valued-functions/s3/)。

### 2.3 使用 S3 Broker Load 导入

以下示例使用 S3 Broker Load 导入 CSV 文件：

> Broker Load 已进入弃用流程。新任务建议优先使用 S3 TVF 配合
> `INSERT INTO ... SELECT ... FROM S3(...)`；已有 Broker Load 任务继续兼容。

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

有关 S3 Broker Load 的完整语法，请参见
[Broker Load](https://doris.apache.org/zh-CN/docs/dev/data-operate/import/import-way/broker-load-manual/)。

### 2.4 注意事项

- S3 TVF 和 S3 Broker Load 支持使用 glob 匹配多个文件。完整语法请参见
[文件路径模式](https://doris.apache.org/zh-CN/docs/dev/sql-manual/basic-element/file-path-pattern/)。
- directory bucket 的 LIST 结果不保证按 key 字典序返回。请不要依赖文件枚举顺序判断最终数据顺序。
- S3 Streaming Job 依赖 `StartAfter` 和按 key 字典序返回的 LIST，而 directory bucket 不支持
这些语义。因此，显式配置 `provider = S3EXPRESS`，或者通过 endpoint 识别为 `S3EXPRESS`
的 Streaming Job，都会在 CREATE 或修改 S3 TVF SQL 的 ALTER 阶段被拒绝。
- COPY INTO 不在当前 S3 Express 支持范围内。



## 3. S3 Express Storage Vault

S3 Express 是 S3 Storage Vault 的一种 provider，只适用于 Doris 存算分离集群。有关
Storage Vault 的创建、设为默认、绑定表和修改凭证等通用操作，请参见
[Storage Vault 管理](https://doris.apache.org/zh-CN/docs/dev/compute-storage-decoupled/managing-storage-vault/)。

创建 S3 Express Storage Vault 时，`type` 仍为 `S3`，并且必须显式配置
`provider = S3EXPRESS`。Doris 会根据 directory bucket 名称和 Region 解析 endpoint，
不需要配置 `s3.endpoint`。

### 3.1 创建示例

```sql
CREATE STORAGE VAULT IF NOT EXISTS s3_express_vault
PROPERTIES (
    "type" = "S3",
    "provider" = "S3EXPRESS",
    "s3.region" = "us-west-2",
    "s3.bucket" = "doris-data--usw2-az1--x-s3",
    "s3.root.path" = "doris/warehouse",
    "s3.access_key" = "<your-access-key>",
    "s3.secret_key" = "<your-secret-key>",
    "use_path_style" = "false"
);
```

使用默认凭证链或 Assume Role 时，按照 [1.2 配置 IAM 权限](#12-配置-iam-权限)配置凭证，
并将示例中的 AK/SK 替换为对应的凭证属性。

### 3.2 注意事项

- 不能通过 S3 Express endpoint 自动识别 Storage Vault，必须显式配置 `provider = S3EXPRESS`。
- 不支持 `use_path_style = true` 或 `s3.credentials_provider_type = ANONYMOUS`。
- AWS identity policy 必须允许 `s3express:CreateSession`，且不要设置
  `s3express:SessionMode` 条件。
- `s3.root.path` 仅是 Doris 使用的对象 key 前缀，不属于 IAM `Resource` ARN。
- directory bucket 不支持 S3 Versioning。不要在 bucket 或 `s3.root.path` 对应前缀上配置
  删除当前对象的 Lifecycle Expiration；Doris 当前不会检测或阻止该配置，误删后无法通过
  历史版本恢复。
- `AbortIncompleteMultipartUpload` 只清理未完成的 multipart upload，不会删除已完成对象。
  如需使用该规则，请单独确认其作用范围。

## 常见错误

以下错误信息可能带有 `Invalid S3 filesystem properties`、`Invalid s3 conf` 或错误码等前后缀。
排查时可以匹配本节列出的稳定错误片段。

### `S3 Express requires virtual-hosted-style access` 或 `S3 Express requires use_path_style=false`

S3 Express 不支持 path-style 访问。请删除 `use_path_style`，或将其设置为：

```text
use_path_style = false
```

### `S3 Express does not support anonymous access`

S3 Express 不支持匿名凭证。请配置 AK/SK、AWS 默认凭证链或 Assume Role，并确认没有设置：

```text
s3.credentials_provider_type = ANONYMOUS
```

### `Region is not set` 或 `Invalid s3 conf, empty region`

未配置 bucket 所在 Region。请显式设置 Region，并确保它与 directory bucket 名称中的 Zone ID
匹配。例如：

```text
s3.region = us-west-2
```

### `S3 Express storage vault requires provider=S3EXPRESS`

创建 S3 Express Storage Vault 时不能依赖 endpoint 自动识别，请显式设置：

```text
provider = S3EXPRESS
```

### `s3.access_key and s3.secret_key must be set together`

静态 AK/SK 必须成对配置。请同时设置 `s3.access_key` 和 `s3.secret_key`，或者同时删除这两个
参数并改用 AWS 默认凭证链或 Assume Role。

### `S3 Express One Zone is not supported for S3 streaming jobs`

S3 Streaming Job 依赖 directory bucket 不支持的 `StartAfter` 和按 key 字典序返回的 LIST。
该错误表示当前场景不受支持，不是临时连接故障。请改用 S3 TVF 或 S3 Broker Load 进行批量导入。

### 错误中包含 `AccessDenied`、`S3 connectivity test failed for bucket` 或 `pingS3 failed`

按以下顺序检查权限和网络：

1. identity policy 是否在完整 directory bucket ARN 上授予 `s3express:CreateSession`。
2. identity policy 是否错误设置了 `s3express:SessionMode` 条件。当前 Doris 请求
   `ReadWrite` session，不应添加该条件。
3. IAM `Resource` ARN 后是否错误添加了 `/*` 或 `s3.root.path`。
4. 使用 Assume Role 时，源身份是否有 `sts:AssumeRole` 权限、目标 Role 的 trust policy 是否正确，
   以及目标 Role 是否有 `s3express:CreateSession` 权限。
5. 使用 SSE-KMS 时，是否具备对应的 KMS 权限。
6. FE、BE 和 Recycler 所在网络是否能够解析并访问 AWS S3 endpoint。

## 参考资料

- [Apache Doris：访问 S3 Directory Bucket](https://doris.apache.org/zh-CN/docs/3.x/lakehouse/storages/s3/#访问-s3-directory-bucket)
- [Apache Doris S3 TVF](https://doris.apache.org/zh-CN/docs/dev/sql-manual/sql-functions/table-valued-functions/s3/)
- [Apache Doris Broker Load](https://doris.apache.org/zh-CN/docs/dev/data-operate/import/import-way/broker-load-manual/)
- [Apache Doris Storage Vault 管理](https://doris.apache.org/zh-CN/docs/dev/compute-storage-decoupled/managing-storage-vault/)
- [AWS directory bucket 概览和命名规则](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-overview.html)
- [AWS S3 Express Zonal endpoint](https://docs.aws.amazon.com/AmazonS3/latest/userguide/endpoint-directory-buckets-AZ.html)
- [AWS S3 Express One Zone IAM](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-security-iam.html)
- [AWS SDK 和 S3 Express session credentials](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-SDKs.html)
- [AWS directory bucket Lifecycle](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-lifecycle.html)
