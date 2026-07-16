# Apache Doris S3 Express One Zone 实现改动梳理

## 1. 文档目的

本文梳理 `goal.md` 设计方案在提交 `8f732dce8a` 中的实际落地情况，重点回答：

- Doris 为支持 S3 Express One Zone 实际修改了哪些模块。
- Directory Bucket 请求与普通 S3 请求分别经过什么调用链。
- checksum、multipart、ListObjectsV2 和失败清理如何处理。
- 哪些 Doris 使用面已经接入原生实现，哪些使用面仍然主动拒绝。
- 当前实现已经具备哪些代码能力，以及还缺少哪些编译、测试和真实 AWS 验证。

本文是实现状态说明，不替代 `goal.md` 中的完整背景、AWS 约束、发布标准和运维方案。

## 2. 实现状态

| 项目 | 内容 |
| --- | --- |
| 设计文档 | `goal.md` |
| 实现提交 | `8f732dce8a`，`[feature](s3) Support S3 Express One Zone directory buckets` |
| 当前阶段 | 代码已实现，尚未编译和执行测试 |
| 原生支持面 | BE S3 FileSystem、FE Java SDK v2 S3 FileSystem、S3 Resource 连通性检查、Hive multipart 提交链路 |
| 主动拒绝面 | Cloud Storage Vault、Hadoop/S3A 外部扫描、presigned URL、Java S3 CopyObject |
| 兼容目标 | 普通 AWS S3 与 MinIO、COS、OSS 等 S3-compatible 服务保持原有行为 |

这里的“代码已实现”表示设计中的主要调用链已经进入代码，不表示已经满足 GA 验收。当前没有执行编译、单元测试、回归测试或真实 Directory Bucket 集成测试。

## 3. 总体设计

本次实现没有把 S3 Express 处理成一个全局开关，而是引入 request-level bucket capability：

```text
bucket + endpoint + region + addressing mode
                    |
                    v
       S3BucketCapabilities resolver
                    |
          +---------+---------+
          |                   |
          v                   v
 GENERAL_PURPOSE          DIRECTORY
          |                   |
 endpoint override       SDK endpoint rules
 Content-MD5             CRC32C
 ordinary list           continuation + local ordering
 ordinary auth           SDK-managed Express session auth
```

Directory Bucket 只有在以下条件同时成立时才会被识别：

1. bucket 是合法的 Availability Zone Directory Bucket 完整名称，例如
   `analytics--use1-az4--x-s3`。
2. endpoint 为空，或者属于官方 AWS S3 endpoint。

因此，在 MinIO 等自定义 endpoint 下，即使 bucket 名以 `--x-s3` 结尾，也仍按普通 S3-compatible bucket 处理。

## 4. 公共能力模型

### 4.1 C++

新增文件：

- `common/cpp/s3_bucket_capabilities.h`

核心类型：

- `S3BucketType`
  - `GENERAL_PURPOSE`
  - `DIRECTORY`
- `S3EndpointMode`
  - `EXPLICIT_OVERRIDE`
  - `AWS_SDK_RULES`
- `S3ChecksumPolicy`
  - `CONTENT_MD5`
  - `CRC32C`
- `S3BucketCapabilities`
  - 是否要求 HTTPS。
  - 是否要求 virtual-hosted addressing。
  - 是否支持 `StartAfter`。
  - 服务端结果是否保证字典序。
  - 是否支持 versioning 和 presign。

resolver 同时提供以下辅助校验：

- Directory Bucket 名称和 zone-id 解析。
- AWS S3、S3 Express Zonal 和 control endpoint 识别。
- endpoint 中 region、zone-id 的解析与冲突检查。
- FIPS、dualstack、accelerate、VPC endpoint 等显式 endpoint mode 识别。

### 4.2 Java

新增文件：

- `fe/fe-filesystem/fe-filesystem-api/src/main/java/org/apache/doris/filesystem/S3BucketCapabilities.java`

Java 侧镜像 C++ 的分类和配置校验规则，供 `fe-core`、`fe-filesystem-s3`、Hadoop FileSystem 和 connector 模块复用。

目前 C++ 和 Java 使用相同规则并分别有测试，但尚未按 `goal.md` 的建议抽取为一份由两端共同读取的测试向量文件。这是后续防止规则漂移的改进项。

## 5. BE 改动

### 5.1 S3ClientConf 与客户端缓存

修改文件：

- `be/src/util/s3_util.h`
- `be/src/util/s3_util.cpp`

主要改动：

1. `S3ClientConf` 增加基于 bucket capability 的 client 行为派生。
2. cache hash 改为有顺序的 hash combine，不再对所有字段直接 XOR。
3. cache key 补充：
   - bucket
   - `need_override_endpoint`
   - addressing mode
   - credentials provider
   - role ARN、external ID、token identity
   - bucket type、endpoint mode、checksum policy
   - 全局 HTTP scheme
4. `to_string()` 不再输出明文 token 和 external ID。
5. 删除全局动态配置 `s3_disable_content_md5`，checksum 完全由 bucket capability 决定。

由于 bucket 进入 client cache key，不同 Directory Bucket 不会复用同一个 Doris cache entry；SDK 内部产生的 bucket-scoped session credentials 也不会进入 Doris 配置、日志或 RPC。

### 5.2 S3ClientFactory

普通 S3 路径保持：

- 原有 endpoint override。
- 原有 virtual/path-style 行为。
- `PayloadSigningPolicy::Never`。
- Content-MD5。

Directory Bucket 路径改为：

- 从 Doris 公共 `ClientConfiguration` 转换为 `S3ClientConfiguration`，保留 CA、timeout 和 retry 配置。
- 不设置 `endpointOverride`。
- 使用 `S3EndpointProvider` 和 SDK endpoint rules。
- `disableS3ExpressAuth=false`，由 SDK 完成 CreateSession、session cache 和刷新。
- `PayloadSigningPolicy::RequestDependent`。
- 要求 region、HTTPS 和 virtual-hosted addressing。
- 拒绝 path-style、HTTP、control endpoint、显式 FIPS/dualstack 等不支持配置，以及 region/zone 冲突。

自定义 S3-compatible endpoint 使用 `disableS3ExpressAuth=true`，避免 SDK 因特殊 bucket 名误启用 Express session auth。

### 5.3 特殊客户端创建入口

`be/src/exprs/function/ai/embed.h` 调整为先解析 URI 和 bucket，再创建 S3 client。Directory Bucket 的 AI media presigned URL 在创建 client 前返回 `NotSupported`。

`be/src/io/tools/file_cache_microbench.cpp` 补充 bucket 到 `S3ClientConf`，使 capability 和 cache key 能正确派生。

### 5.4 PutObject 与 UploadPart checksum

修改文件：

- `be/src/io/fs/s3_obj_storage_client.h`
- `be/src/io/fs/s3_obj_storage_client.cpp`

`S3ObjStorageClient` 不再保存 `_disable_content_md5`，而是保存完整 `S3BucketCapabilities`。

Directory Bucket 写入行为：

- `PutObject` 计算 Base64 CRC32C，设置 algorithm 和 checksum，不发送 Content-MD5。
- `CreateMultipartUpload` 声明 `CRC32C`。
- `UploadPart` 计算并发送 CRC32C。
- `UploadPart` 校验响应中的 CRC32C；缺失或不一致均返回失败。
- `CompleteMultipartUpload` 为每个 part 发送 ETag、part number 和 CRC32C。
- Complete 前要求 Directory Bucket part number 为从 1 开始的连续序列。

普通 S3 继续发送 Content-MD5，不使用 Directory Bucket 分支。

### 5.5 Multipart 元数据与失败清理

修改文件：

- `be/src/io/fs/obj_storage_client.h`
- `be/src/io/fs/s3_file_writer.cpp`

新增数据：

- `ObjectStorageUploadResponse.checksum_crc32c`
- `ObjectCompleteMultiPart.checksum_crc32c`

`S3FileWriter` 会保存每个 part 的 CRC32C，在 Complete 前排序并检查 part 序列。

失败清理流程：

```text
CreateMultipartUpload 成功
          |
          v
并发 UploadPart / Complete / close
          |
          +---- 成功 ----> Complete
          |
          +---- 失败
                  |
                  v
          等待所有异步 part 收敛
                  |
                  v
          AbortMultipartUpload
```

具体行为：

- `ObjStorageClient` 增加 `abort_multipart_upload` 接口。
- S3 实现调用 AWS `AbortMultipartUpload`。
- 404/NoSuchUpload 作为幂等成功处理。
- Abort 失败时保留原业务错误，并附加 cleanup 错误。
- 析构函数不发起网络 Abort，只记录 warning 和 unfinished multipart metric。

### 5.6 DeleteObjects 与列举

Directory Bucket：

- `DeleteObjects` 请求显式使用 CRC32C。
- 批量删除响应中的所有 object error 都进入错误消息，不再只报告第一项。
- 非 `/` 结尾的 list prefix 扩大到父目录 prefix。
- 使用 continuation token 读取全部页面，再按原始 prefix 本地过滤。
- BE 完整 list 结果在客户端按 key 排序。
- 递归删除一旦删除了当前 batch，就丢弃旧 continuation token 并从头扫描，避免删除过程中继续使用失效的 opaque token。

### 5.7 BE 可观测性

新增指标：

- CRC32C 校验失败数。
- multipart Abort 次数和失败数。
- writer 析构时仍存在 multipart upload 的次数。
- Directory Bucket list page、扫描 key 和返回 key 数。

client 创建日志增加 bucket type、endpoint mode 和 checksum policy；credentials token 和 external ID 保持遮蔽。

## 6. Hive BE 上传、FE Complete 链路

Hive 写入不是在同一进程内完成 multipart：BE 上传 part，FE 在事务提交时 Complete。因此本次修改了 Thrift 协议。

修改文件：

- `gensrc/thrift/DataSinks.thrift`
- `be/src/exec/sink/writer/vhive_partition_writer.cpp`
- `fe/fe-core/src/main/java/org/apache/doris/datasource/hive/HMSTransaction.java`
- `fe/fe-filesystem/fe-filesystem-api/src/main/java/org/apache/doris/filesystem/UploadPartResult.java`
- `fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/ObjFileSystem.java`

新增可选字段：

```thrift
enum TObjectStorageChecksumAlgorithm {
    CRC32C = 0
}

5: optional TObjectStorageChecksumAlgorithm checksum_algorithm
6: optional map<i32, string> part_checksums
```

传播链路：

```text
BE S3FileWriter.completed_parts
        |
        v
VHivePartitionWriter
        |
        v
TS3MPUPendingUpload
  etags + algorithm + part_checksums
        |
        v
FE HMSTransaction
        |
        v
ObjFileSystem -> UploadPartResult
        |
        v
S3ObjStorage CompleteMultipartUpload
```

Directory Bucket Complete 缺少任一 part checksum 时会失败，并进入 Abort。普通 S3 和旧消息仍可不设置新字段。

这些字段是 optional，因此协议可以解析旧消息，但混合版本集群仍不能安全启用 Directory Bucket：旧 BE 不产生 checksum，旧 FE 也不会使用 checksum。

## 7. FE 原生 S3 FileSystem 改动

### 7.1 S3ObjStorage 客户端模型

修改文件：

- `fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3ObjStorage.java`

原来的单一 lazy client 改为：

- `generalClient`
- `directoryClient`
- 两者共享同一个长期 `AwsCredentialsProvider`

每个公开对象操作解析当前 URI 的 bucket，并通过 `clientFor(bucket)` 选择 client。bucket type 不会被第一次请求永久缓存。

general client：

- 保留 endpoint override、path-style、signer 和兼容配置。
- 自定义 endpoint 禁用 Express session auth。

directory client：

- 不设置 endpoint override。
- 强制 virtual-hosted addressing。
- 启用 SDK Express session auth。
- 复用原 credentials provider。

Java Directory Bucket 请求增加：

- PutObject CRC32C。
- CreateMultipartUpload CRC32C。
- UploadPart CRC32C，并要求响应包含 `ChecksumCRC32C`。
- CompleteMultipartUpload 传播每个 part 的 CRC32C。
- DeleteObjects CRC32C。
- Abort 404 幂等处理。

Java SDK 负责根据 request body 计算请求 checksum；当前 Java 代码要求 UploadPart 响应 checksum 存在并继续传给 Complete，但没有像 C++ 路径一样单独计算本地 CRC32C 后做值比较。

### 7.2 Directory Bucket glob

修改文件：

- `fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3FileSystem.java`
- `fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/S3CompatibleFileSystem.java`

Directory Bucket glob 算法：

1. 原始非 glob prefix 截断到最后一个 `/`。
2. 请求中不发送 `StartAfter`。
3. 用 opaque continuation token 读取所有服务页面。
4. 本地应用原始 prefix、glob 和 `key > startAfter` 过滤。
5. 使用 Doris UTF-8 binary comparator 排序。
6. 排序后应用 `maxFiles`、`maxBytes` 和 `maxFile` 契约。

递归删除在 Directory Bucket 上删除一个 batch 后重新从空 token 列举。

当前实现为保证语义正确，会收集全部匹配对象后排序；尚未实现仅有 `maxFiles` 时的有界堆优化，也没有新增 Directory Bucket 专用的内存限制和查询取消检查。

### 7.3 S3FileSystemProperties

修改文件：

- `fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3FileSystemProperties.java`

增加 Directory Bucket region、endpoint、HTTPS 和 path-style 校验。将 Directory Bucket 属性转换为 Hadoop S3A 配置时直接拒绝，防止进入未经验证的 S3A client。

## 8. fe-core 改动

### 8.1 S3Util

`fe/fe-core/src/main/java/org/apache/doris/common/util/S3Util.java` 的多个 builder 收敛到统一内部实现：

- 已知 bucket 时先解析 capability。
- Directory Bucket 不设置 endpoint override，不强制 `AwsS3V4Signer`，启用 Express session auth。
- 普通 S3 保留原 builder 行为。
- 自定义 S3-compatible endpoint 禁用 Express session auth。
- HeadBucket connectivity tester 传入完整 bucket。

### 8.2 S3Resource

`fe/fe-core/src/main/java/org/apache/doris/catalog/S3Resource.java` 增加：

- Directory Bucket endpoint 缺少 scheme 时默认补 HTTPS，而不是 HTTP。
- 在网络请求前校验 region、endpoint、path-style 和 HTTPS。
- ping list 使用父目录扫描，并翻页直到确认测试对象存在。
- multipart Complete 失败时执行 Abort。
- Abort 失败作为 suppressed exception 保留在原始错误中。

### 8.3 S3URI

`S3URI.isS3DirectoryBucket` 改为复用统一的 Directory Bucket 名称校验，不再维护一套宽松的 suffix 分割规则。

## 9. 不支持使用面的 fail-fast

### 9.1 Cloud Storage Vault

修改文件：

- `fe/fe-core/src/main/java/org/apache/doris/catalog/StorageVaultMgr.java`
- `cloud/src/meta-service/meta_service_resource.cpp`

FE 在生成 create/alter vault 请求前拒绝 Directory Bucket。Meta Service 在直接 ADD/ALTER object info 或 Storage Vault 时再次校验，并在加密或持久化前返回 `INVALID_ARGUMENT`。

门禁同时要求：

- provider 为 S3。
- endpoint 是官方 AWS service。
- bucket 是合法 Directory Bucket 名称。

因此第三方 S3-compatible 服务中的 `--x-s3` bucket 不会被误拒绝。

### 9.2 Hadoop/S3A 与外部扫描

以下入口在第一个 Hadoop/S3A 网络请求前拒绝 Directory Bucket：

| 使用面 | 拒绝位置 |
| --- | --- |
| 通用 Hadoop FileSystem | `DFSFileSystem.getHadoopFs` |
| 新 Hive connector | `HiveScanPlanProvider` 创建 Hadoop FileSystem 前 |
| Hudi | `HudiScanNode` 创建 Hudi client 前 |
| LakeSoul | `LakeSoulScanNode` 初始化 scanner 前 |
| Paimon | 各 Paimon metastore `initializeCatalog` 前 |
| Avro S3 reader | `S3FileReader` 调用 `FileSystem.get` 前 |

上述门禁都会结合 endpoint 判断官方 AWS 与自定义兼容服务，普通 S3 和自定义 endpoint 保持原路径。

### 9.3 Presign 和 Copy

- Java `S3ObjStorage.getPresignedUrl` 对 Directory Bucket 返回不支持。
- BE AI embed 入口明确返回 `NotSupported`。
- BE 通用 `generate_presigned_url` 接口受限于返回类型，只能记录 warning 并返回空字符串。
- Java `S3ObjStorage.copyObject` 对 Directory Bucket 返回不支持。

当前 BE `S3ObjStorageClient::copy_object` 尚未增加 Directory Bucket 显式门禁；Directory Bucket Access Point 也没有专门的资源标识识别。这两项不能写入 P0 支持声明，需要在合入前补齐或证明没有用户可达调用链。

## 10. 普通 S3 与兼容性

本次实现有意保留以下行为：

- 普通 AWS S3 继续使用 Content-MD5。
- 普通 S3 client 继续使用原 endpoint override 和 path-style 配置。
- MinIO、COS、OSS 等自定义 endpoint 不根据 bucket suffix 启用 Express。
- `UploadPartResult` 保留原两参数构造，OSS、COS、OBS、Azure 不需要 checksum 字段。
- `ObjStorageClient.abort_multipart_upload` 默认是 no-op，避免要求所有已有 object storage 实现同时增加原生 Abort。
- bucket capability 不增加 PB、EditLog 或持久化字段。
- 只有 Hive multipart 的 Thrift 消息增加 optional checksum 字段。

## 11. 配置示例

```sql
CREATE RESOURCE "s3_express_hot"
PROPERTIES
(
    "type" = "s3",
    "provider" = "S3",
    "AWS_ENDPOINT" = "https://s3.us-east-1.amazonaws.com",
    "AWS_REGION" = "us-east-1",
    "AWS_BUCKET" = "analytics-hot--use1-az4--x-s3",
    "AWS_ROOT_PATH" = "hot-data/",
    "AWS_ACCESS_KEY" = "<access-key-id>",
    "AWS_SECRET_KEY" = "<secret-access-key>",
    "use_path_style" = "false"
);
```

关键约束：

- bucket 必须使用完整 Directory Bucket 名称。
- endpoint 推荐配置标准 regional HTTPS endpoint，不需要手工配置 bucket-specific Zonal endpoint。
- region 必须与 bucket 所在 region 一致。
- `use_path_style` 必须为 `false`。
- IAM principal 必须拥有目标 bucket 的 `s3express:CreateSession` 权限。
- Doris 只配置长期 credentials provider，不能配置或持久化 SDK 产生的 Express session token。

## 12. 已增加但未执行的测试

### 12.1 C++

- `be/test/util/s3_util_test.cpp`
  - 官方 AWS Directory Bucket 分类。
  - 自定义 endpoint 负例。
  - endpoint mode、region 和 zone 解析。
- `be/test/io/fs/s3_file_writer_test.cpp`
  - Complete 失败后执行 Abort。

### 12.2 Java

- `S3BucketCapabilitiesTest`
  - 分类、region、zone、dualstack 和 custom endpoint。
- `S3ObjStorageMockTest`
  - Put、Create MPU、UploadPart、Complete 和 DeleteObjects CRC32C request model。
  - Directory Bucket list 不发送 StartAfter。
- `S3FileSystemTest`
  - 无序 list 的本地排序和 `maxFile`。
- `S3FileSystemPropertiesTest`
  - Directory Bucket 拒绝转换为 S3A。
- `DFSFileSystemTest`、`HiveScanPlanProviderTest`、`S3UtilsTest`
  - S3A/Avro fail-fast 和第三方 endpoint 兼容性。

这些测试文件已经进入提交，但遵照开发要求没有执行。

## 13. 尚未完成的验证和风险

以下项目在代码提交后仍然是明确的未完成项：

1. 没有执行 BE、FE 或 Cloud 编译，接口签名和依赖闭包尚未由编译器验证。
2. 没有运行任何单元测试、回归测试或静态分析。
3. C++ clang-format 未完成；当前环境缺少 Homebrew/clang-format。
4. 没有真实 AWS Directory Bucket 测试：
   - CreateSession 权限和 403 错误。
   - 连续运行超过五分钟的 session refresh。
   - 并发首次请求和 refresh stampede。
   - Put、multipart、DeleteObjects 的实际 HTTP checksum header。
   - 无序、多页 ListObjectsV2。
5. Java UploadPart 没有单独计算本地 CRC32C 并和响应值比较，目前依赖 SDK request checksum，并要求响应 checksum 存在。
6. Directory glob 尚未增加专用内存上限、取消检查和 `maxFiles` 有界堆优化。
7. 跨语言分类测试尚未使用同一个测试向量文件。
8. BE CopyObject 和 Directory Bucket Access Point 缺少明确门禁。
9. Cloud Vault 门禁缺少绕过 FE 直调 Meta Service 的自动化测试。
10. 当前未增加 cluster capability bit，混合版本启用限制依靠发布文档和运维流程。

因此，在上述验证完成前，只能表述为“Directory Bucket 支持代码已实现”，不能表述为“功能已经通过验收”或“可以作为 GA 发布”。

## 14. 建议验证顺序

后续验证建议按以下顺序进行：

1. 运行 C++ 和 Java 格式检查，先解决纯工程问题。
2. 编译 Thrift、BE、FE 和 Cloud，确认跨模块接口完整。
3. 运行 capability、S3 object client、file writer、filesystem 和 fail-fast 单测。
4. 运行普通 AWS S3、MinIO/S3-compatible 回归，确认兼容路径没有变化。
5. 使用真实 AWS Directory Bucket 验证小对象、multipart、Abort、DeleteObjects 和多页 list。
6. 让 client 跨越至少一次五分钟 session 过期，并执行并发刷新验证。
7. 验证 Cloud Meta Service 直接 add/alter vault 均在持久化前拒绝。
8. 补齐 BE CopyObject、Access Point 等剩余边界后，再评估 P0 验收。

## 15. 回滚与混合版本

- 本次不改变对象 key 和对象内容格式。
- 旧 Doris binary 不能可靠访问仍位于 Directory Bucket 中的数据，因此不能把二进制回滚等同于数据可用性回滚。
- 回滚前必须停止 Directory Bucket workload，把仍需访问的数据复制到普通 S3，并切换 Resource。
- 混合版本期间只能继续使用普通 S3；所有可能执行该资源 I/O 的 BE 和 FE 升级完成后，才能启用 Directory Bucket Resource。
- Cloud Storage Vault 当前主动拒绝 Directory Bucket，不存在透明切换或回滚路径。

## 16. 结论

本次改动已经完成 S3 Express One Zone 的核心代码闭环：统一能力识别、SDK-managed session auth、CRC32C、multipart checksum 传播、失败 Abort、Directory Bucket list 语义、FE 原生 S3 client，以及未支持 surface 的 fail-fast。

当前最重要的下一步不是继续扩大支持面，而是完成编译、普通 S3 兼容回归和真实 Directory Bucket 集成验证，并补齐本文列出的 BE CopyObject、Access Point、资源限制和 Cloud 直调测试缺口。
