# Apache Doris S3 Express One Zone 落地设计

## 1. 文档信息

| 项目 | 内容 |
| --- | --- |
| 状态 | Proposed |
| 最后核对日期 | 2026-07-16 |
| 实现基线 | take-over-63409，HEAD 0a193178c8 |
| 关联 PR | [apache/doris#63409](https://github.com/apache/doris/pull/63409) |
| 目标读者 | Doris BE、FE、Cloud、测试与发布维护者 |

本文给出 Doris 支持 AWS S3 Express One Zone 的可实施方案。严格来说，S3 Express One Zone 是 storage class，Directory Bucket 是承载它的 bucket 类型；本文 P0 专指 Availability Zone 中使用 S3 Express One Zone 的 Directory Bucket。这里的“支持”不是简单地让一次 PutObject 成功，而是让 endpoint 解析、CreateSession、凭证刷新、checksum、multipart、ListObjectsV2、失败清理、普通 S3 兼容性和可观测性形成完整闭环。

本文中的 AWS 行为以 2026-07-16 的官方文档为准。S3 Directory Bucket 的能力仍在演进，开发和发布前必须重新核对文末的 AWS API 白名单与差异文档。

## 2. 结论先行

最终方案采用以下决策：

1. 把 Directory Bucket 作为一种 S3 客户端能力模型，而不是通过“是否关闭 MD5”或 endpoint 子串做临时分支。
2. Doris 只配置长期 IAM credentials 或既有 credentials provider。CreateSession、五分钟 session credentials 的缓存与刷新全部交给支持 S3 Express 的 AWS SDK，Doris 不自行实现 token manager。
3. Directory Bucket 数据面不设置 endpointOverride，由 SDK 根据完整 bucket 名和 region 解析 Zonal endpoint；强制 HTTPS 和 virtual-hosted addressing，拒绝 path-style。
4. Directory Bucket 写入统一显式使用 CRC32C。PutObject 发送对象 checksum；multipart 完整贯通 CreateMultipartUpload、UploadPart 和 CompleteMultipartUpload 的 part checksum。
5. Directory Bucket 的 ListObjectsV2 不能沿用普通 S3 的 StartAfter 和字典序假设。Doris 使用 continuation token 扫描、客户端过滤和排序来保持现有 FileSystem 接口语义。
6. 首个正式支持范围限定为 Doris 原生 S3 文件系统路径，包括 BE 读写以及与之配套的 FE 校验和列举。Cloud Storage Vault、Hadoop/S3A、预签名 URL、Access Point 不在首期支持声明中。
7. 当前 PR 不能直接作为“Doris 已完整支持 S3 Express”合入。它已经验证了 BE 基础方向，但还缺少 FE、Cloud 边界、multipart 完整性、列举语义、失败 Abort、回归保护和真实 Directory Bucket 自动化测试。

## 3. 产品范围

### 3.1 首期 P0 支持矩阵

| Doris 使用面 | P0 状态 | 说明 |
| --- | --- | --- |
| BE 原生 S3 FileSystem exact-key 读、Head | 支持 | GetObject、HeadObject，由 C++ SDK 自动选择 Zonal endpoint 和 session auth |
| BE 小对象写入 | 支持 | PutObject，显式 CRC32C，不发送 Content-MD5 |
| BE 大对象写入 | 支持 | Create、UploadPart、Complete 全链路 CRC32C；失败时主动 Abort |
| BE 删除 | 支持 | DeleteObject；DeleteObjects 必须使用 flexible checksum |
| 原生 S3 前缀列举和 FE glob | 支持，但有代价 | 使用 continuation token 扫描相关目录；FE 客户端过滤、排序并应用 startAfter、maxFiles、maxBytes |
| FE S3 Resource 连通性检查 | 支持 | Java SDK v2，不强制通用 SigV4 signer，不 override Zonal endpoint |
| FE 新 S3 FileSystem 客户端 | 支持 | Java SDK v2，复用同一 credentials provider 和 bucket 分类规则 |
| 普通 AWS S3 | 保持现状 | endpoint、MD5、path-style 等既有行为不能回归 |
| MinIO、COS、OSS 等 S3-compatible | 保持现状 | 不因 bucket 名恰好以 --x-s3 结尾而切换到 AWS Express 模式 |

### 3.2 P0 明确不支持

| 使用面 | 处理方式 |
| --- | --- |
| Cloud Storage Vault | FE 与 Meta Service 的创建、修改入口都 fail fast；原因见第 12 节 |
| 依赖 Hadoop S3A 的 External Catalog | 在确认所用 Hadoop/AWS SDK 版本支持 Directory Bucket 之前 fail fast |
| AWS Java SDK v1 路径 | 不支持；必须迁移到 Java SDK v2 后才能纳入 |
| Presigned URL | Directory Bucket 模式明确返回 NotSupported，完成独立鉴权和过期时间测试后再开放 |
| Directory Bucket Access Point | 不支持；其 host 和资源标识不同于直接 bucket |
| CopyObject、UploadPartCopy、RenameObject | 不作为 P0 对外承诺，实际调用面需要分别验证 |
| SSE-C、DSSE-KMS、对象 ACL、Object Lock、Requester Pays、对象 tag | Directory Bucket 不支持或不在 P0 |
| Doris 创建、删除 Directory Bucket | 不支持；bucket 由管理员预先创建 |
| Local Zone Directory Bucket | 不在 P0；P0 只声明 Availability Zone 中的 S3 Express One Zone |

### 3.3 非目标

- 不通过 Doris 自研 HTTP 签名或 token 缓存替代 AWS SDK。
- 不把 S3 Express 当成普通 S3 的透明性能开关。
- 不承诺跨 AZ 的低延迟，也不在 Doris 内自动查询 IMDS 判断节点所在 AZ。
- 不把单 AZ Directory Bucket 描述为普通 S3 的同等耐久性替代品。
- 不在本次设计中改变 Doris 对象路径、文件格式或持久化元数据格式。

## 4. AWS 硬约束

### 4.1 Bucket、endpoint 与寻址

Availability Zone Directory Bucket 的完整名称为：

    <bucket-base-name>--<zone-id>--x-s3

例如：

    analytics-hot--use1-az4--x-s3

zone-id 是全局一致的 Availability Zone ID，例如 use1-az4，不是账户内可能映射不同的 Availability Zone 名称。

数据面和控制面的 endpoint 不能混用：

| 平面 | 典型操作 | endpoint | 寻址方式 | 认证 |
| --- | --- | --- | --- | --- |
| Zonal 数据面 | CreateSession、GetObject、PutObject、ListObjectsV2、multipart、Delete | s3express-<zone-id>.<region>.amazonaws.com | 只支持 virtual-hosted | 默认 session auth，由 SDK处理 |
| Regional 控制面 | 创建/删除 bucket、lifecycle、bucket policy 等 | s3express-control.<region>.amazonaws.com | path-style | 长期 IAM credentials 与 SigV4 |

Doris P0 只需要对象数据面。客户端应把 bucket 和 region 交给 SDK endpoint rules，不能把用户填写的 endpoint 直接覆盖成 Zonal host，更不能把 s3express-control 当成对象 endpoint。

### 4.2 CreateSession

- IAM principal 必须拥有目标 Directory Bucket 上的 s3express:CreateSession。
- SDK 根据 --x-s3 bucket 名自动请求 bucket-scoped session credentials。
- session credentials 有效期为五分钟；SDK 负责缓存、并发复用和临近过期刷新。
- Doris 只保存或传递长期 credentials provider 所需的信息，不把 SDK 生成的 session access key、secret key 或 token 写入 FE、Meta Service、配置、日志或 RPC。
- SDK 对少数使用长期 IAM 签名的操作有自己的签名选择逻辑，Doris 不应强制指定 AwsS3V4Signer 或自行指定 service name。

最小读写 IAM 示例：

    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": "s3express:CreateSession",
          "Resource": "arn:aws:s3express:us-east-1:123456789012:bucket/analytics-hot--use1-az4--x-s3"
        }
      ]
    }

生产环境应使用角色和最小权限策略。若需要只读 session，应通过 AWS 支持的 s3express:SessionMode 条件约束 IAM，而不是让 Doris 修改 session token。

### 4.3 Checksum 与 ETag

- Directory Bucket 的 PutObject 和 UploadPart 不支持 Content-MD5。
- CRC32、CRC32C、SHA-1、SHA-256 可用于数据完整性校验；AWS 推荐高性能场景使用 CRC32 或 CRC32C。
- 本设计固定选择 CRC32C，避免不同 SDK 版本的隐式默认值变化，并复用 Doris 已有的硬件优化 CRC32C 实现。
- Directory Bucket 的 ETag 是不透明标识，不能假设为对象 MD5。
- DeleteObjects 的 XML request body 必须带 Content-MD5 或 flexible checksum；Directory Bucket 路径使用 CRC32C。

### 4.4 Multipart

- Directory Bucket 的 part number 必须从 1 开始连续递增；Complete 时不允许空洞。
- part 可以并发上传，Doris 在 Complete 前按 part number 排序并验证连续性。
- 一旦声明 multipart checksum algorithm，各 UploadPart 和 CompleteMultipartUpload 必须携带一致的 part checksum。
- 未完成的 multipart 会持续占用存储并计费。应用必须主动 Complete 或 Abort，lifecycle 只能作为兜底。

### 4.5 ListObjectsV2

Directory Bucket 与普通 S3 的关键差异：

- 返回结果不保证按 key 字典序排序。
- 不支持 StartAfter。
- Prefix 只支持以 / 结尾的值。
- Delimiter 只支持 /。
- 分页必须使用服务端返回的 opaque continuation token。
- 列表中可能出现仅由未完成 multipart 产生的 prefix。

因此，任何依赖“服务端排序 + last key 续扫”的 Doris 代码都必须改造，不能只修改 endpoint 和鉴权。

### 4.6 能力限制

- 不支持 S3 Versioning。
- 当前 AWS API 支持 Directory Bucket lifecycle，但只支持对象过期和取消未完成 multipart；不支持非当前版本过期、transition 和 tag filter。
- P0 使用 bucket 默认 SSE-S3。SSE-KMS 需要单独验证 CreateSession、bucket 默认加密和请求 header 一致性后再开放。
- StorageClass header 应省略或使用 EXPRESS_ONEZONE，不能发送 STANDARD。
- Access Point 已是 AWS 独立能力，但不在 P0；不能误写成 AWS 永久不支持。

## 5. 当前 PR 评估

### 5.1 当前分支

take-over-63409 相对 upstream/master 的 merge-base e05c331da1 有三个提交：

1. 4096f12a4f：PutObject 和 UploadPart 增加 CRC32C，并为 Express 关闭 Content-MD5。
2. 98b7adffee：AWS SDK C++ 1.11.219 升级到 1.11.400，并改用 S3ClientConfiguration。
3. 0a193178c8：集中和收紧 S3 Express 检测。

原 GitHub PR #63409 仍只有前两个提交；第三个检测修复尚未进入原 PR。原 PR 当前为 Changes requested。

### 5.2 已经正确的方向

- be/src/util/s3_util.cpp 开始使用 S3ClientConfiguration 和 S3EndpointProvider。
- Directory Bucket 模式不再设置 endpointOverride。
- disableS3ExpressAuth=false 时可由 SDK 接管 CreateSession。
- be/src/io/fs/s3_obj_storage_client.cpp 已验证显式 CRC32C 的基本写入方向。
- 普通 S3 仍保留 Content-MD5 分支。
- common/cpp/aws_logger.h 已适配新 SDK 的 vaLog 接口。

### 5.3 必须修正的事实描述

AWS SDK C++ 1.11.219 并非完全不支持 S3 Express。AWS SDK C++ 维护者说明 S3 Express customizations 从 1.11.212 已合入；1.11.219 源码中已经存在 CreateSession、S3 Express signer/provider、disableS3ExpressAuth 和 endpoint rules。旧的 S3Client 构造函数在该版本也会创建 endpoint provider 和 Express signer provider。

因此：

- 可以继续采用 1.11.400 作为 Doris 的固定版本，因为 AWS 建议采用包含后续 bug fix 的较新版本。
- PR 描述不能把“1.11.219 没有 S3 Express 能力”作为根因。
- 改成显式 S3ClientConfiguration 的价值是让配置和 endpoint provider 更清楚、可测试，而不是它单独开启了 Express。
- 发布前要用 pinned SDK 的集成测试证明 endpoint、session refresh 和 checksum，而不是依赖版本号推断。

### 5.4 当前实现缺口和回归风险

| 问题 | 具体触发链 | 设计要求 |
| --- | --- | --- |
| 覆盖面只有 BE 基础路径 | FE 和 Cloud 有独立 S3 client builder | 分别改造或明确排除，不能宣称 Doris 全面支持 |
| 检测仍把 bucket/endpoint 字符串作为布尔值 | 第三方 bucket 恰好以 --x-s3 结尾会被跳过自定义 endpoint | 引入 service-aware capability resolver，联合 bucket、官方 endpoint authority 和 override mode |
| disableS3ExpressAuth 与自研检测绑定 | AWS 普通 bucket 被强制关闭 SDK 默认能力，第三方行为与 endpoint 子串耦合 | 官方 AWS service 保持 SDK 默认 false；自定义 endpoint client 明确禁用 |
| 新配置从默认值重建 | be/src/util/s3_util.cpp 直接默认构造 S3ClientConfiguration | 从现有 ClientConfiguration 转换，保留 CA、timeout、retry 等语义 |
| payload signing 策略变化 | 旧构造显式 Never，新构造使用默认 RequestDependent | 普通路径显式保持现状，并做回归与性能测试 |
| s3_disable_content_md5 是动态全局配置 | 配置只在 cached S3ObjStorageClient 构造时快照 | P0 删除该用户开关；checksum 由 bucket capability 决定 |
| cache key 不完整 | S3ClientConf::get_hash() 缺 need_override_endpoint 和新策略 | 所有影响 client 行为的字段进入 key |
| multipart checksum 只有 UploadPart | Create 未声明算法、Complete 只传 ETag | 完整传播 CRC32C |
| 写失败不 Abort | S3FileWriter::_abort 仅声明，底层无接口 | 增加 abort API 并覆盖取消、失败和并发上传收敛 |
| DeleteObjects 未覆盖 | Directory Bucket request body 不接受既有 MD5 假设 | 显式 CRC32C 或验证 SDK 注入，必须有 request capture test |
| List 仍依赖普通 S3 语义 | FE glob 直接下推 StartAfter，Prefix 可不以 / 结尾 | continuation token + 本地过滤、排序 |
| 可能泄露 token | S3ClientConf::to_string() 直接输出 token | token 和 externalId 必须遮蔽或完全移除 |
| 默认 HTTP 风险 | s3_client_http_scheme 和 S3Resource 历史路径可生成 HTTP | Directory Bucket 强制 HTTPS |
| 测试仅覆盖检测 | 当前新增测试只有少量 is_s3_express case | 增加 client、request、multipart、list、refresh 和真实 AWS 测试 |

### 5.5 Doris code-review 关键检查点结论

| 检查点 | 当前 PR 结论 |
| --- | --- |
| 目标是否完成、是否有测试证明 | 仅完成 BE PutObject/UploadPart 的基础尝试；没有证明完整 Doris 调用链，也没有 session refresh、multipart、list 或真实 AWS 自动化测试 |
| 修改是否最小清晰 | 差异本身较小，但用全局 MD5 开关和 bool Express 状态承载多种能力，抽象不足 |
| 并发 | S3FileWriter 并发上传 part；当前没有等待所有 part 后 Abort 的失败收敛设计。SDK session provider 的并发复用也未测试 |
| 生命周期 | 动态配置在 cached client 构造时快照；client、五分钟 session、multipart upload 的创建与销毁生命周期尚未闭环 |
| 新增配置 | s3_disable_content_md5 声明为 mutable，但运行时更新对已有 client 不生效，不能按当前形态保留 |
| 兼容与滚动升级 | 没有 wire/storage format 变化，但重建 S3ClientConfiguration 可能改变 timeout 和 payload signing；旧 BE 不能用于 Directory Bucket |
| 平行调用路径 | FE 两套 Java builder、Hive BE-upload/FE-complete、Cloud recycler/client、特殊 BE client 创建点没有同步覆盖 |
| 特殊条件判断 | bucket/endpoint 字符串判断不能区分官方 AWS 与自定义 S3-compatible endpoint |
| 测试覆盖 | 新增测试只覆盖少量识别输入；请求字段、错误路径、负例和端到端覆盖不足 |
| 可观测性 | 没有 Directory Bucket 专用 metrics；现有 S3ClientConf::to_string() 还会输出明文 token |
| 持久化与协议 | bucket capability 无需持久化；但 BE 上传、FE 完成的 Hive multipart 必须增加可选 Thrift checksum 字段 |
| 数据写入正确性 | 小对象 CRC32C 方向正确，但 multipart checksum、DeleteObjects checksum 和失败 Abort 不完整 |
| 性能 | client/session 复用方向正确；需验证 payload signing、CRC32C CPU 和 Directory Bucket 全量 list 的成本 |

## 6. 统一能力模型

### 6.1 C++ 数据结构

在 BE 和 Cloud 都可依赖的 common/cpp 层定义纯数据类型和无网络副作用的 resolver。名称可按现有构建组织调整，建议语义如下：

    enum class S3BucketType {
        GENERAL_PURPOSE,
        DIRECTORY
    };

    enum class S3EndpointMode {
        EXPLICIT_OVERRIDE,
        AWS_SDK_RULES
    };

    enum class S3ChecksumPolicy {
        CONTENT_MD5,
        CRC32C
    };

    struct S3BucketCapabilities {
        S3BucketType bucket_type;
        S3EndpointMode endpoint_mode;
        S3ChecksumPolicy checksum_policy;
        bool require_https;
        bool require_virtual_addressing;
        bool supports_start_after;
        bool list_is_lexicographic;
        bool supports_versioning;
        bool supports_presign;
    };

resolver 输入必须包含：

- storage provider（若当前 surface 有该信息）
- 完整 bucket 名
- endpoint
- region
- use_path_style 或 use_virtual_addressing
- need_override_endpoint 或等价的“官方 AWS 服务/自定义 S3-compatible 服务”信息
- 当前调用面是否声明支持 Directory Bucket

resolver 输出不可只返回 bool。调用者应基于 capability 做 endpoint、auth、checksum、list 和功能门禁，避免散落多个 is_s3_express 判断。

### 6.2 判定真值表

| 服务意图 | Bucket | Endpoint/override | 结果 |
| --- | --- | --- | --- |
| AWS 服务 | 完整 --x-s3 后缀 | 空、标准 AWS regional endpoint 或匹配的官方 Zonal endpoint | DIRECTORY |
| AWS 服务 | 非 --x-s3 | 官方 s3express Zonal/control endpoint | 配置错误，fail fast |
| 自定义 S3-compatible 服务 | 任意名称，包括 --x-s3 后缀 | 自定义 endpoint，明确需要 override | GENERAL_PURPOSE，不自动切 Express |
| 非 S3 provider | 任意名称 | 官方 s3express endpoint | 配置错误，fail fast |

这里的 AWS 服务不能只由用户属性 provider 或 C++ 的 ObjStorageType::AWS 判断。FE 合法的通用 S3 provider 值是 S3，进入 C++ 后又可能映射成 ObjStorageType::AWS；MinIO 等 S3-compatible 配置会走同一类枚举。resolver 必须结合 need_override_endpoint 和 endpoint authority：明确使用自定义 endpoint override 的客户端按 GENERAL_PURPOSE 处理；只有指向官方 AWS 服务且 bucket 后缀合法时才进入 DIRECTORY。endpoint 仍然不能单独作为 Directory Bucket 的判定依据，不能因为字符串中出现 s3express 就启用 session auth。

官方 endpoint 的识别应复用 AWS partition/endpoint metadata 或集中维护的 parser，不能把 .amazonaws.com 写死后遗漏其他 partition，也不能维护会快速过期的 region/AZ allowlist。

### 6.3 Bucket 解析

- 只接受完整后缀 --x-s3。
- 从最后两个双连字符段解析 zone-id，不维护静态 AZ allowlist。
- region 必填。
- 若 endpoint 同时包含 region 或 zone-id，发现明显不一致时返回包含 bucket、region、endpoint 的可操作错误。
- 不仅检查 endpoint 字符串；最终 endpoint 仍由 SDK rules 生成。

### 6.4 跨语言一致性

FE Java 无法直接复用 C++ 类型，因此应：

1. 在 S3URI/S3Properties/S3FileSystemProperties 附近实现同样的 service-aware resolver。
2. 增加一份跨语言测试向量，覆盖 bucket、provider（若有）、endpoint、override mode、region、path-style 和预期结果。
3. C++ 和 Java 单测读取同一组向量，防止后续规则漂移。

P0 不为 bucket type 新增 protobuf、Thrift 或持久化字段。各 surface 使用已有 bucket、region、endpoint、path-style，并结合本地 endpoint override 规则确定性派生 capability；不能假设 provider 已在所有路径传递，因为 S3FileSystemProperties 和 S3Properties.getS3TStorageParam() 当前不会完整传递它。

multipart checksum 是另一件事：BE 上传、FE 完成的 Hive 写入必须新增可选 Thrift 字段，详见第 8.5 节。可选字段保证旧消息可解析，但不代表混合版本可以启用 Directory Bucket。

### 6.5 FE 的两级解析

S3FileSystemProperties.bucket 在部分调用面可以为空，实际 bucket 来自每次 remotePath；S3ObjStorage 当前也只维护一个 lazy S3Client。因此 FE 不能在 buildClient 时把整个 client 固化成某个 bucket type。

FE 使用两级模型：

1. service-level config 在创建 S3ObjStorage 时确定：
   - AWS_SDK_ROUTED：endpoint 为空或为可安全归一化的标准 regional endpoint；允许 Directory Bucket。
   - AWS_EXPLICIT_ENDPOINT：FIPS、dualstack、accelerate 或其他必须保留的显式 AWS endpoint；P0 只允许普通 bucket。
   - CUSTOM_S3_COMPATIBLE：保留 endpointOverride，禁用 Express session auth。
2. request-level capability 在每个方法解析 remotePath 后，根据实际 bucket、service-level config、region 和 path-style 派生：
   - AWS_SDK_ROUTED + --x-s3 为 DIRECTORY。
   - AWS_SDK_ROUTED + 普通 bucket 为 GENERAL_PURPOSE。
   - AWS_EXPLICIT_ENDPOINT + 普通 bucket 为 GENERAL_PURPOSE；遇到 --x-s3 时 fail fast。
   - CUSTOM_S3_COMPATIBLE 下所有 bucket 都按 GENERAL_PURPOSE。

为保证普通 AWS S3 不回归，S3ObjStorage 将当前单一 lazy client 改成 clientFor(bucket)：

- general client 完整保留已有 endpointOverride、FIPS/dualstack、path-style 和兼容配置。
- directory client 只在 AWS_SDK_ROUTED 的 Directory Bucket 请求出现时惰性创建，固定为无 endpointOverride、HTTPS、virtual-hosted、Express auth enabled。
- 两个 client 复用同一长期 credentials provider，但生命周期和配置缓存分开。
- 一个 directory client 可以服务同 region 的多个 Directory Bucket，SDK 的 Express identity cache 按 bucket 和 credentials identity 隔离 session。

Put、multipart、List、Copy、presign 等方法必须使用当前 URI 的 request-level capability 和 clientFor(bucket)，不能把第一次请求的 bucket type 缓存在 S3ObjStorage 上，也不能为每个对象创建新 client。

## 7. 客户端与请求数据流

    FE properties / URI / storage resource
                  |
                  v
        service-aware capability resolver
                  |
                  +---- unsupported surface: fail fast
                  |
                  v
      existing PB / Thrift storage parameters
      only long-term credentials configuration
                  |
                  v
        BE S3ClientFactory client cache
                  |
                  v
      AWS SDK endpoint rules + credentials provider
                  |
                  +---- CreateSession
                  |        |
                  |        +---- SDK-only five-minute session credentials
                  |
                  v
       bucket-specific Zonal data endpoint
                  |
                  +---- Get / Head / List
                  +---- Put / Multipart / Delete

Hive multipart 的事务提交链路另有一条 BE 到 FE 的 TS3MPUPendingUpload 消息，只传算法、ETag 和 part checksum 等完成上传所需元数据，仍然不传 session credentials。

关键生命周期：

- client 以 bucket、region、service kind、endpoint mode、credentials provider identity 等为 key 长期缓存。
- 首次 Directory Bucket 请求触发 SDK 获取 session。
- 并发请求复用同一个 client 和 session provider。
- SDK 在 session 过期前刷新；Doris 不观察和持久化 token。
- client 销毁时 session credentials 只随进程内对象释放。

## 8. BE 详细改造

### 8.1 S3ClientConf 与 cache key

文件：

- be/src/util/s3_util.h
- be/src/util/s3_util.cpp

改造：

1. 在 S3ClientConf 初始化完成且 bucket 已知后调用 capability resolver。
2. get_hash() 至少加入 need_override_endpoint、bucket type、endpoint mode、checksum policy、scheme、addressing style。
3. 不继续用简单 XOR 拼接易交换字段的 hash；按字段顺序使用 hash combine。
4. credentials provider 类型、role ARN、external ID、session token identity 必须继续参与隔离。
5. to_string() 只能输出遮蔽后的 access key；token、secret key 和 external ID 不输出明文。
6. client cache 不跨不同 bucket 复用 Directory Bucket client，确保 session credentials 的 bucket scope 不被混用。

### 8.2 S3ClientFactory

文件：

- be/src/util/s3_util.cpp

普通 S3 路径：

- 保留当前 endpointOverride 规则。
- 保留 useVirtualAddressing、timeout、CA、retry 和 payload signing 的原有语义。
- S3-compatible provider 不因名称命中 --x-s3 而改变行为。

Directory Bucket 路径：

- 从 S3ClientFactory::getClientConfiguration() 返回的现有公共 ClientConfiguration 通过 converting constructor 构造 S3ClientConfiguration，避免丢失 aws_client_request_timeout_ms 等公共默认值。
- region 使用用户配置。
- scheme 强制 HTTPS；若配置显式要求 HTTP，直接返回配置错误。
- useVirtualAddressing 强制 true；若用户配置 path-style，直接返回配置错误。
- endpointOverride 保持空。
- disableS3ExpressAuth 保持 false。
- 使用 S3EndpointProvider。
- credentials provider 仍是 Doris 已有 static、default chain、web identity、instance profile 或 assume-role provider。
- 对普通路径显式保留 PayloadSigningPolicy::Never；Directory Bucket 路径让 SDK 的 Express customization 决定必要的签名行为，并用性能测试验证没有重复 payload hash。

不要把 disableS3ExpressAuth 设置成 !is_directory。建议规则是：

- 官方 AWS 服务：false，允许 SDK 根据实际 bucket 选择。
- 自定义 S3-compatible endpoint：true，防止 AWS SDK 根据特殊 bucket 名误启用 Express。

### 8.3 特殊构造路径审计

所有在 bucket 未知时创建 S3ClientConf 的路径必须修正。例如 be/src/exprs/function/ai/embed.h 应先解析 URI 并写入 bucket，再创建 client。审计要求：

    rg "S3ClientConf|create_s3_client|S3ClientFactory" be cloud

每个直接构造 client 的路径都必须归入以下之一：

- 使用统一 factory 和 capability resolver。
- 明确仅支持普通 S3，并在 Directory Bucket 输入时 fail fast。
- 有独立的 Directory Bucket 实现和测试。

### 8.4 PutObject

文件：

- be/src/io/fs/s3_obj_storage_client.h
- be/src/io/fs/s3_obj_storage_client.cpp

改造：

- S3ObjStorageClient 持有 S3BucketCapabilities，而不是 is_s3_express 和 _disable_content_md5 两个布尔值。
- DIRECTORY 时：
  - SetChecksumAlgorithm(CRC32C)
  - 计算 Base64 编码的 CRC32C 并 SetChecksumCRC32C
  - 不 SetContentMD5
- GENERAL_PURPOSE 时保持当前 Content-MD5 行为。
- 对空对象、非对齐 buffer、最大单次 PutObject 边界增加已知向量测试。
- 不把 ETag 与本地 MD5 比较。

当前 s3_disable_content_md5 全局动态配置不进入最终 P0。若未来确实需要为第三方存储配置 checksum，应设计为每个 resource 的 AUTO、MD5、CRC32C、NONE 枚举，并贯穿 FE、PB/Thrift、BE 及 cache key，不能复用当前全局布尔值。

### 8.5 Multipart 完整校验

相关文件：

- be/src/io/fs/obj_storage_client.h
- be/src/io/fs/s3_obj_storage_client.cpp
- be/src/io/fs/s3_file_writer.cpp
- be/src/io/fs/s3_file_writer.h

Directory Bucket 流程：

1. CreateMultipartUpload 设置 ChecksumAlgorithm=CRC32C。
2. UploadPart 为每个 part 计算并发送 CRC32C。
3. 校验服务端返回的 ChecksumCRC32C 与本地值一致；缺失或不一致均返回失败。
4. ObjectStorageUploadResponse 增加可选 checksum_crc32c。
5. ObjectCompleteMultiPart 增加可选 checksum_crc32c。
6. S3FileWriter 保存 part number、ETag、CRC32C。
7. CompleteMultipartUpload 的每个 CompletedPart 同时设置 ETag、PartNumber、ChecksumCRC32C。
8. Complete 前排序并断言 part number 为 1..N 连续序列；违反不变量时失败，不能跳过空洞继续提交。

普通 S3FileWriter 在同一 BE 内完成上传时，上述内部结构足够；但 Hive 写入还存在“BE 上传 part，FE 提交事务后 Complete”的跨进程链路：

- gensrc/thrift/DataSinks.thrift 的 TS3MPUPendingUpload 当前只有 bucket、key、upload_id 和 etags。
- be/src/exec/sink/writer/vhive_partition_writer.cpp 只把 part ETag 写入 Thrift。
- fe/fe-core/src/main/java/org/apache/doris/datasource/hive/HMSTransaction.java 在 FE 发起 Complete。
- fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/ObjFileSystem.java 当前把 ETag map 转成 UploadPartResult。

P0 必须给 TS3MPUPendingUpload 增加可选字段：

    5: optional TObjectStorageChecksumAlgorithm checksum_algorithm
    6: optional map<i32, string> part_checksums

其中 P0 枚举至少包含 CRC32C，part_checksums 的 key 与 etags 的 part number 一致。对应改造：

1. VHivePartitionWriter 从 S3FileWriter.completed_parts() 同时填入算法和每个 part 的 CRC32C。
2. HMSTransaction 把两个新字段交给 ObjFileSystem。
3. ObjFileSystem 构造带 checksum 的 UploadPartResult。
4. S3ObjStorage 在 Complete 时为每个 CompletedPart 设置 CRC32C。
5. Directory Bucket 模式若新字段缺失，FE 必须拒绝 Complete 并走 Abort；不能降级成只传 ETag。
6. 普通 S3 和旧消息可以不设置可选字段。

这些改动不改变持久化对象格式，但改变了可选 Thrift 消息。新代码可解析旧消息；语义上仍不允许混合版本启用 Directory Bucket，因为旧 FE 会忽略新字段，旧 BE 也不会生成它们。Azure、OSS、COS、OBS 等实现不使用 checksum 字段，但必须继续兼容现有构造。

若 pinned C++ SDK 的 request model 对 composite checksum 有额外约束，以 SDK 1.11.400 的实际模型和真实 AWS 测试为准；不能退化成“只要 UploadPart 成功就视为完整支持”。

### 8.6 AbortMultipartUpload

当前 S3FileWriter::_abort 只有声明。P0 必须：

1. 给 ObjStorageClient 增加 abort_multipart_upload。
2. S3 实现调用 AWS AbortMultipartUpload；NoSuchUpload 视为幂等成功。
3. 等待所有 in-flight UploadPart future 收敛后再 Abort，避免 part 上传与 Abort 竞态。
4. Create 成功后的任一上传、Complete、取消或 close 失败都进入 Abort。
5. 若业务操作失败且 Abort 也失败，返回保留原始错误并附带 cleanup 错误的 Status。
6. 析构函数不执行可能阻塞或抛异常的网络请求，只记录未正常 close 的 metric 和 warning。
7. 服务端 AbortIncompleteMultipartUpload lifecycle 作为最终兜底，不替代应用主动清理。

### 8.7 DeleteObjects

审计 BE 和 Cloud 的批量删除实现：

- Directory Bucket 使用 CRC32C 校验 XML request body。
- 不设置 Content-MD5。
- request capture 单测必须断言 algorithm 和 checksum header。
- 保留每个 object 的失败明细；部分失败不能静默当作成功。

### 8.8 读取

- GetObject、HeadObject 由 SDK 处理 endpoint 和 session auth。
- P0 不改变 Doris range read 语义。
- 若启用 ChecksumMode，必须验证 ranged GET 的 AWS 行为；不能要求服务端对任意 range 返回完整对象 checksum。
- TLS 是读取传输保护的最低要求。
- 所有使用 ETag 做缓存 identity 的代码只把它当不透明字符串。

## 9. ListObjectsV2 兼容方案

### 9.1 现有冲突

fe-filesystem 的 FileSystem 接口把 startAfter 定义为字典序游标；S3CompatibleFileSystem.globListWithLimit 当前把它直接下推给 ListObjectsV2，并按服务返回顺序计算 maxFiles、maxBytes 和 GlobListing.maxFile。这在 Directory Bucket 上不成立。

相关文件：

- fe/fe-filesystem/fe-filesystem-api/src/main/java/org/apache/doris/filesystem/FileSystem.java
- fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3FileSystem.java
- fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/ObjectListOptions.java
- fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/S3CompatibleFileSystem.java
- fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3ObjStorage.java

仅把 prefix 扩展到父目录还不够，因为：

- StartAfter 请求会被拒绝。
- 返回顺序不稳定，提前应用 maxFiles 或 maxBytes 会漏掉字典序更小的 key，并产生错误的 maxFile 游标。
- 原始最长非通配 prefix 可能不以 / 结尾。

### 9.2 P0 算法

输入：

- originalPrefix：Doris 语义中的最长前缀
- directoryPrefix：originalPrefix 截断到最后一个 /，没有 / 时为空
- globPattern
- startAfter
- maxFiles
- maxBytes

执行：

1. 只向 AWS 发送以 / 结尾的 directoryPrefix。
2. 不发送 StartAfter。
3. 用 NextContinuationToken 读取到 IsTruncated=false。
4. 对每一页在客户端过滤 originalPrefix 和 globPattern。
5. 在客户端应用 key > startAfter。
6. 收集匹配对象的 key、size 和 modification time，并按 Doris 既有 key comparator 排序。
7. 按排序结果依次加入 FileEntry；加入当前对象后，如果 files.size 达到 maxFiles 或 totalSize 达到 maxBytes，则结束当前返回页。这与现有“包含触发阈值的对象”语义一致。
8. GlobListing.maxFile 设置为返回页后的第一个匹配 key；若不存在下一项，则保持现有契约，使用最后一个已返回 key。
9. maxFiles 有值且 maxBytes 未启用时，可以用大小为 maxFiles+1 的最大堆优化内存；只要 maxBytes 启用，阈值依赖排序后对象大小，最坏情况必须收集全部匹配项后排序，不能套用单一 limit 堆算法。
10. 多个服务 prefix 的结果需要去重后再统一排序；Directory Bucket 路径通常只使用扩大的一个 directoryPrefix。
11. 全程响应查询取消；超过现有内存限制时返回 ResourceLimit 错误，不能静默截断。

复杂度：

- 服务请求数与 directoryPrefix 下的总对象数相关。
- 只有 maxFiles 限制时可把内存优化为 O(maxFiles)，CPU 为 O(N log maxFiles)。
- maxBytes 启用或没有限制时，最坏内存为 O(M)，排序 CPU 为 O(M log M)，M 是匹配对象数。

这是保持现有 FileSystem 契约的正确性成本。文档应建议用户为高频列举使用稳定、以 / 结尾且选择性高的目录前缀。

### 9.3 BE 与 Cloud 的不同契约

BE S3ObjStorageClient.list_objects 返回完整 FileInfo 集合，没有 FE GlobListing 的 maxFiles、maxBytes、maxFile 契约。BE 只需要：

- continuation token 翻完所有服务页。
- 任意非目录 prefix 先扩大到父目录，再客户端精确过滤。
- 不承诺或依赖服务返回顺序；只有上层契约要求时才排序。
- 未完成 multipart 产生的 CommonPrefixes 不当成已提交对象。

BE 和 Cloud 的递归删除也不能直接复用 FE 排序算法。尤其不能一边按 opaque continuation token 翻页，一边删除已经列出的对象后继续使用原 token。Directory Bucket 的安全算法是：

1. 从空 continuation token 开始扫描，客户端过滤目标 prefix。
2. 收集最多一个 DeleteObjects batch 的匹配 key；如果前几页没有匹配项，继续翻页，不能提前结束。
3. 删除该 batch 后丢弃 continuation token，从头重新扫描。
4. 重复直到一次完整扫描找不到匹配对象。
5. 每个 DeleteObjects request 使用 CRC32C，并检查部分失败。

Cloud recycler 的 list_prefix、递归删除和 checker 只需要集合完整性和取消/重试语义，不需要 FE 的字典序分页结果。三条调用链共享 AWS 协议约束和 prefix 过滤 helper，但各自保留自己的返回契约。这里的 Cloud 算法只描述第 12.2 节的独立 P1：P0 不让 Directory Bucket 请求进入 Cloud recycler，必须在 Vault 持久化前拒绝。

## 10. FE 详细改造

### 10.1 S3URI 与 S3Properties

相关文件：

- fe/fe-core/src/main/java/org/apache/doris/common/util/S3URI.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/property/storage/S3Properties.java

现有 S3URI 已有 directory bucket 辅助逻辑，可以保留 bucket suffix 解析，但必须升级为 service-aware capability resolver。校验时：

- OFFICIAL_AWS service + --x-s3 才进入 Directory Bucket。
- region 必填。
- path-style=false。
- endpoint 必须是 HTTPS。
- 推荐 endpoint 配置为标准 AWS regional endpoint，例如 https://s3.us-east-1.amazonaws.com；它只作为 region/配置提示，不下传为 Directory Bucket 的 endpoint override。
- 对 control endpoint、region/zone 冲突给出明确错误。

若某个 surface 在校验阶段拿不到 bucket，不能只看 endpoint 猜测，也不能要求 S3FileSystemProperties.bucket 必填。先区分 AWS_SDK_ROUTED、AWS_EXPLICIT_ENDPOINT、CUSTOM_S3_COMPATIBLE，再在每次 remotePath 解析出 bucket 后确定 request-level capability。

### 10.2 fe-filesystem-s3

相关文件：

- fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3ObjStorage.java
- fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3FileSystemProperties.java
- fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3FileSystem.java
- fe/fe-filesystem/fe-filesystem-api/src/main/java/org/apache/doris/filesystem/UploadPartResult.java
- fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/ObjStorage.java

当前代码注释说“仅为非 AWS endpoint 设置 endpointOverride”，但实现对任何非空 endpoint 都 override。不能为支持 Directory Bucket 而一律删除官方 AWS endpointOverride，因为普通 bucket 可能依赖 FIPS、dualstack 或显式 regional endpoint。改造为：

- general client：
  - 完整保留当前 endpointOverride、path-style、FIPS/dualstack 和兼容 signer 行为。
  - CUSTOM_S3_COMPATIBLE 调用 disableS3ExpressSessionAuth(true)。
- directory client：
  - 只允许 AWS_SDK_ROUTED。
  - 不调用 endpointOverride。
  - region 必填。
  - disableS3ExpressSessionAuth(false)。
  - 不设置自定义 signer。
  - 使用 buildCredentialsProvider() 返回的同一 provider。
  - 强制 HTTPS 和 virtual-hosted，让 Java SDK v2 解析 Zonal endpoint/auth。
- AWS_EXPLICIT_ENDPOINT 遇到 Directory Bucket 时 fail fast；P0 不静默丢弃用户指定的 FIPS/dualstack 语义。

每次请求解析 bucket 后由 clientFor(bucket) 选择 general 或 directory client。若当前 remotePath bucket 是 DIRECTORY 且 usePathStyle=true，在发送请求前 fail fast；普通 bucket 保留既有 path-style 行为。

S3ObjStorage 自身也执行 S3Resource ping 的 Put 和 multipart，不能只修 client builder：

- 每个公开方法解析 S3Uri 后调用 capabilitiesFor(uri.bucket())，不缓存第一次请求的 bucket type。
- Directory PutObject 显式选择 CRC32C，让 Java SDK 计算或发送 checksum，并用 request interceptor 测试实际 header。
- Directory CreateMultipartUpload 声明 CRC32C。
- Directory UploadPart 返回并保存 ChecksumCRC32C。
- Java SPI 的 UploadPartResult 增加可选 checksum 字段，并保留现有两参数构造以避免影响 OSS、COS、OBS、Azure。
- CompleteMultipartUpload 为每个 CompletedPart 传递 CRC32C。
- CopyObject、presign 等未纳入 P0 的操作在 Directory Bucket 模式 fail fast。
- 普通 S3-compatible 请求保持现有行为。

general/directory 两个 lazy client 各自使用线程安全的初始化方式。测试要覆盖并发首次请求，确认每种配置只创建一个 client，且两种配置不会串用。

### 10.3 fe-core S3Util

相关文件：

- fe/fe-core/src/main/java/org/apache/doris/common/util/S3Util.java

该文件使用 AWS Java SDK v2，但当前 builder：

- 无条件 endpointOverride。
- 强制 AwsS3V4Signer。
- 可开启 path-style。

把 builder 收敛为接收规范化的 service-level config，并由已知 bucket 选择配置：

- Directory Bucket 使用 AWS_SDK_ROUTED：不 override endpoint、不强制 signer，并启用 Express session auth。
- 普通 AWS 或自定义 S3-compatible 保留现有显式 endpoint、signer 和 chunkedEncoding 兼容设置；自定义 endpoint 禁用 Express session auth。
- AWS_EXPLICIT_ENDPOINT + Directory Bucket 在构建前 fail fast。
- HeadBucket 连通性检查必须使用完整 bucket，并在调用前用 request-level capability 拒绝 Directory Bucket 的 path-style/HTTP 配置。

不建议继续扩散多个 buildS3Client overload。先收敛到一个接收规范化配置对象的内部 builder，再让旧 overload 委托它。

### 10.4 S3Resource

相关文件：

- fe/fe-core/src/main/java/org/apache/doris/catalog/S3Resource.java

改造：

- Directory Bucket 不允许缺省成 http://。
- ping test 的 ListObjects prefix 必须使用父目录 slash prefix，并在客户端过滤目标 key。
- ping test 复用现有 ObjStorage.abortMultipartUpload API；任何失败路径都 Abort，并把 Abort 失败附加到原始错误，不能像当前实现一样吞掉 cleanup 异常。
- 错误信息区分 InvalidDirectoryBucketConfiguration、CreateSessionAccessDenied、UnsupportedS3Surface。

### 10.5 Presigned URL

S3ObjStorage.getPresignedUrl 当前使用 static basic credentials 创建 presigner，和实际 client 的 default chain、session token、assume-role provider 不一致。P0 处理：

- Directory Bucket 直接返回明确 NotSupported。
- 普通 S3 保持现状。
- 后续开放前必须让 presigner 使用统一 credentials provider，并验证 URL 的签名类型、bucket host、session token、最大有效期和 SDK 刷新行为。

### 10.6 Hadoop/S3A 和 Java v1

对所有 External Catalog、Hadoop FileSystem、AWS Java SDK v1 client 做调用面清单。这份清单是 P0 的合入产物和阻断门禁，至少逐项记录：用户入口、最终 I/O 实现及依赖版本、何时能取得完整 bucket、最早可确定拒绝的位置、负责人和自动化测试。初始清单至少覆盖：

| 调用面家族 | 已知入口示例 | P0 处理 |
| --- | --- | --- |
| Doris 原生 S3 FileSystem | S3ObjStorage、S3FileSystem、S3Util | 按本设计适配并支持 |
| Hive BE-upload/FE-complete | VHivePartitionWriter、HMSTransaction、ObjFileSystem | 按第 8.5 节传播 multipart checksum 后支持 |
| Hudi Hadoop FileSystem | HudiScanNode 及其 Hadoop 配置构造 | 未证明所装载 S3A 版本支持前拒绝 |
| LakeSoul Hadoop FileSystem | LakeSoulUtils 及其 Hadoop 配置构造 | 未证明所装载 S3A 版本支持前拒绝 |
| Paimon/Hadoop 配置 | AbstractPaimonProperties 及对应 catalog/scanner | 未证明最终 I/O client 支持前拒绝 |
| be-java-extensions 与其他外部 catalog/scanner | 各扩展自己的 classloader 和 AWS/Hadoop 依赖 | 逐模块锁定实际依赖版本；未验证项拒绝 |
| AWS Java SDK v1 直接客户端 | fe-core 及扩展中引入 aws-java-sdk-s3 的路径 | P0 不支持，迁移或隔离到 Java SDK v2 后再开放 |

P0 规则：

- 若最终 I/O 由已适配的 Doris S3 FileSystem 执行，允许。
- 若最终 I/O 由未经验证的 S3A 或 Java v1 client 执行，在能够取得完整 bucket 的最早确定点拒绝 Directory Bucket URI。bucket 在分析期可知时就在分析期拒绝；只能在运行时 URI 中取得时，必须在 filesystem/scanner factory 发出首个网络请求前拒绝。
- 错误必须指出该 surface 尚未支持，不能在运行时表现为 403、错误 endpoint 或签名失败。
- 每一行清单都必须有正向普通 S3 用例和 Directory Bucket 负例；清单未穷举或缺少测试时，PR 3 不满足完成条件。

## 11. 配置与用户使用

### 11.1 推荐配置

Directory Bucket 已由管理员在 us-east-1 的 use1-az4 创建：

    analytics-hot--use1-az4--x-s3

Doris S3 Resource 示例：

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

对象 URI：

    s3://analytics-hot--use1-az4--x-s3/hot-data/example.parquet

优先使用 instance profile、web identity 或 assume role，避免在 Resource 中保存静态 AK/SK。上例只用于展示已有属性形状。

Doris 当前 S3 属性的合法 provider 值是 S3，不是 AWS。是否连接官方 AWS 服务由 endpoint authority 和 override mode 判断；不要为了本功能新增一个只在部分链路存在的 AWS 枚举。

### 11.2 endpoint 规则

- 用户不需要手工配置 bucket-specific Zonal endpoint。
- 如果当前 Doris surface 强制要求 AWS_ENDPOINT，填写标准 regional S3 HTTPS endpoint。
- Directory Bucket client 不把该值传给 endpointOverride。
- 普通 bucket 继续保留显式 regional、FIPS、dualstack 和 S3-compatible endpoint 行为；P0 的 Directory Bucket 只接受 AWS_SDK_ROUTED，遇到 FIPS、dualstack 或其他显式 override 配置时拒绝，而不是静默改写。
- s3express-control endpoint 不能用于对象 I/O。
- 自定义 proxy 若会改写 Host、DNS 或 TLS SNI，不在支持范围。

### 11.3 部署要求

- 计算节点和 bucket 位于同一 AZ 才能获得设计目标中的低延迟和更低网络成本。
- 跨 AZ 功能上可能可达，但不作为性能目标，且会增加成本和故障域复杂度。
- 私网访问使用 com.amazonaws.<region>.s3express 的 gateway VPC endpoint；普通 S3 endpoint 或 PrivateLink 配置不能直接替代。
- DNS、TLS SNI 和 Host header 必须保留 SDK 生成的 bucket-specific host。

## 12. Cloud Storage Vault 边界

### 12.1 为什么 P0 不支持

Cloud Storage Vault 是 Doris Cloud 的主持久化存储，不是临时 staging 目录。Directory Bucket：

- 只在单个 AZ 内冗余。
- 不支持 Versioning。
- 删除后没有普通 S3 versioning 提供的恢复窗口。

当前 Cloud checker 还明确依赖：

- S3Accessor::check_versioning() 要求 Versioning=Enabled。
- get_life_cycle() 查找 NoncurrentVersionExpiration。
- Checker 使用该保留天数做巡检间隔风险判断。

Directory Bucket 不支持这两个前提。简单地“跳过检查”会移除现有安全门禁；把 current-object Expiration 当成替代又可能删除仍然有效的 Doris 数据。因此 P0 必须在创建或修改 Storage Vault 时拒绝 Directory Bucket，而不是让 BE 写入成功后由 recycler/checker 失败。

只在 FE 校验不够：cloud/src/common/http_helper.cpp 暴露 add_s3_vault、alter_s3_vault 等直接 HTTP 路由，它们进入 cloud/src/meta-service/meta_service_resource.cpp 并可独立持久化 ObjectStoreInfoPB。P0 要在两层都执行同一 service-aware 门禁：

1. FE 在生成 Storage Vault 请求前拒绝 official AWS service + 合法 --x-s3 bucket。
2. Meta Service 在 ADD_S3_VAULT、ALTER_S3_VAULT 以及所有能直接创建或替换 S3 vault/object info 的 RPC/HTTP 路径中，必须在 txn->put 或修改 InstanceInfoPB 前再次拒绝。
3. Meta Service 的判断必须同时使用 provider/service、endpoint authority/override mode 和完整 bucket；第三方 S3-compatible 服务中恰好以 --x-s3 结尾的 bucket 不能被误拒绝。
4. 返回稳定的 INVALID_ARGUMENT/UnsupportedS3Surface 错误，不能先落库再依赖 recycler 或 checker 报错。

这是一项产品范围门禁，不改变现有 Cloud 信任边界或安全定级。

### 12.2 若产品决定支持 Cloud Vault

必须单独通过以下设计门禁后才能进入 P1：

1. 产品和运维明确接受单 AZ 与无 versioning 的数据耐久性、恢复和删除风险。
2. cloud/src/recycler/s3_accessor.cpp 改用 S3ClientConfiguration、S3EndpointProvider、SDK endpoint rules、HTTPS 和 virtual-hosted。
3. S3Conf::from_obj_store_info 当前把 OSS、S3、COS、OBS、BOS 都折叠成 S3Conf::S3；P1 必须保留原始 ObjectStoreInfoPB::Provider 或在折叠前派生 official AWS service kind，不能把第三方 vault 的 --x-s3 bucket 误判为 Directory Bucket。
4. cloud/src/recycler/s3_obj_client.cpp 的 Put、DeleteObjects、List、Abort 使用 Directory Bucket checksum 和分页语义。
5. 不把 session credentials 写入 ObjectStoreInfoPB 或 Meta Service。
6. client/session cache 不能跨 instance、tenant、vault 或 credentials identity 共享。
7. 重做 checker 的安全模型，不能仅 skip versioning/lifecycle：
   - 明确 Directory Bucket 下不可恢复删除的告警和运维门禁。
   - lifecycle 只允许验证 AbortIncompleteMultipartUpload，不允许把通用 current-object Expiration 配在 Doris 数据根目录。
   - 为 checker 设计不依赖 noncurrent version retention 的独立巡检 SLA。
8. recycler 的 mark-before-delete、两阶段删除、重试幂等和 packed-file 顺序保持不变。
9. 完成 Cloud recycler、checker、snapshot、restore、storage vault 创建和滚动升级的端到端测试。

在这套安全模型评审完成前，Cloud 代码可以先复用 capability resolver 用于 fail fast，但不对外开放 Vault。

## 13. 安全设计

### 13.1 信任边界

按照 Doris threat model，外部 S3 配置由管理员信任，但 credentials、网络 endpoint 和 Cloud tenant 隔离仍属于必须保护的边界。

### 13.2 Credentials

- 长期 AK/SK、AWS_TOKEN、role ARN 等继续走 Doris 现有安全配置和 credentials provider。
- CreateSession 返回值只存在于 SDK 进程内存。
- 不新增 session token 的 FE 属性、PB、Thrift、EditLog 或 Meta Service 字段。
- 不记录 Authorization、x-amz-s3session-token、secret key、session token。
- 修复 S3ClientConf::to_string() 对 token 的明文输出；external ID 也按敏感信息处理。
- 日志可记录 bucket mode、region、zone-id、operation、HTTP status、AWS request id，不记录完整 credentials。

### 13.3 网络

- Directory Bucket 强制 HTTPS。
- 不接受 path-style 或任意 endpoint override。
- 保留 Doris 现有 endpoint 安全校验；官方 AWS host 仍需经过 allowlist/SSRF 规则。
- 不跟随会把签名请求重定向到非 AWS host 的重定向。

### 13.4 Cloud tenant 隔离

若未来支持 Cloud：

- cache key 包含 instance/vault/credentials identity。
- 一个 tenant 的 session client 不得用于另一个 tenant，即使 bucket、region 和 endpoint 相同。
- session 刷新失败只影响对应 client，不触发全局 credentials 降级。

## 14. 错误处理

建议新增或规范化以下用户可识别错误：

| 错误 | 示例信息 |
| --- | --- |
| 非法 bucket 名 | AWS Directory Bucket requires a full bucket name ending with --x-s3 |
| 缺少 region | AWS Directory Bucket requires AWS_REGION |
| path-style | Path-style addressing is not supported for AWS Directory Bucket |
| HTTP | AWS Directory Bucket requires HTTPS |
| endpoint 冲突 | Configured endpoint region/zone does not match directory bucket |
| control endpoint | s3express-control endpoint cannot be used for object operations |
| session 权限 | Access denied while creating S3 Express session; grant s3express:CreateSession |
| 不支持的 surface | S3 Express One Zone is not supported by Hadoop S3A/Cloud Storage Vault in this release |
| checksum | CRC32C mismatch for UploadPart, bucket=..., key=..., part=... |
| multipart 空洞 | Directory Bucket multipart parts must be consecutive from 1 |
| list 资源不足 | Directory Bucket listing exceeded query memory limit while preserving ordered result |

实现遵循 Doris 的“错误即失败”原则：

- 配置不一致在创建 client 前失败。
- checksum 不一致不能重试为无 checksum 请求。
- session auth 失败不能回退为匿名或非 TLS。
- Abort 失败不能被静默丢弃。
- 只对 SDK 明确标记可重试的网络、限流和服务端错误使用现有 retry strategy。

## 15. 可观测性

### 15.1 日志

client 创建时记录：

- bucket_type=general_purpose 或 directory
- service kind（official AWS 或 custom S3-compatible）
- region
- zone_id
- endpoint_mode=override 或 sdk_rules
- addressing=virtual 或 path
- checksum_policy

请求失败记录：

- operation
- bucket/key 的现有安全形式
- HTTP status
- SDK exception name
- AWS request id
- 是否处于 Directory Bucket 模式

不在 INFO 级别记录每次成功请求或 session token。

### 15.2 Metrics

建议在现有 S3 bvar 基础上增加低基数维度：

- s3_client_create_total，按 bucket_type
- s3_request_total，按 bucket_type、operation、result
- s3_request_latency，按 bucket_type、operation
- s3_checksum_failure_total，按 operation
- s3_multipart_abort_total，按 result
- s3_directory_list_scanned_keys
- s3_directory_list_returned_keys
- s3_directory_list_pages
- s3_auth_failure_total，按 AccessDenied、ExpiredToken、其他

不要把 bucket 名、key 或 tenant id 作为 metric label。

AWS SDK 内部 CreateSession 不一定暴露独立回调；若不能可靠计数，不应猜测 session refresh 次数。可以通过 SDK debug 日志和长时集成测试验证刷新，通过外层 403/ExpiredToken 统计定位故障。

## 16. 测试方案

### 16.1 Capability resolver 单测

C++ 与 Java 使用同一组测试向量：

- AWS + 合法 --x-s3。
- bucket 中间包含 --x-s3 但后缀不匹配。
- 自定义 S3-compatible endpoint + --x-s3。
- AWS + custom endpoint。
- zonal endpoint 的 region/zone 匹配与不匹配。
- control endpoint。
- 空 region。
- HTTP。
- path-style。
- dualstack 官方 endpoint。

### 16.2 BE client 配置单测

通过可注入 builder 或 request hook 断言：

- Directory Bucket：HTTPS、virtual、无 endpointOverride、disableS3ExpressAuth=false。
- 普通 AWS S3：原 endpoint 和 addressing 行为不变。
- S3-compatible：保留 endpointOverride，Express auth 禁用。
- CA、aws_client_request_timeout_ms、resource timeout、connect timeout、max connections、retry strategy 被保留。
- payload signing policy 与设计一致。
- cache key 能隔离 need_override_endpoint、bucket type、scheme、checksum policy 和 credentials identity。

### 16.3 Checksum 单测

- CRC32C 已知向量：空字符串、123456789、小 buffer、跨 chunk buffer。
- PutObject request：
  - Directory 有 ChecksumAlgorithm=CRC32C 和正确 ChecksumCRC32C。
  - Directory 无 ContentMD5。
  - 普通 S3 仍有原 ContentMD5。
- DeleteObjects request body 使用 CRC32C。
- checksum mismatch 返回失败。

### 16.4 Multipart 单测

- Create 声明 CRC32C。
- 多个并发 part 的 checksum 正确保存。
- Complete 前排序。
- Complete 携带每个 part 的 ETag 和 ChecksumCRC32C。
- Hive BE-upload/FE-complete 路径把 checksum algorithm 和 part checksum 通过 TS3MPUPendingUpload 传到 FE。
- Directory Bucket 下缺少可选 checksum 字段时拒绝 Complete 并 Abort。
- part number 从 1 连续。
- 空洞、重复、从 0 开始均失败。
- UploadPart 失败时等待其他 future 后 Abort。
- Complete 失败时 Abort。
- Abort 的 NoSuchUpload 幂等成功。
- 原始失败与 Abort 失败同时可见。
- 普通 S3 和 Azure writer 不回归。

### 16.5 List 单测

构造无序、多页、超过 1000 个 key 的 mock 响应：

- 不发送 StartAfter。
- 服务 prefix 以 / 结尾。
- 正确使用 NextContinuationToken。
- 对 originalPrefix 和 glob 二次过滤。
- 无序输入得到稳定有序输出。
- startAfter 在本地应用。
- 仅 maxFiles 时最多保留 N+1 个全局字典序最小对象，并生成正确 maxFile。
- maxBytes 时先完整排序，包含触发阈值的对象，并生成正确 maxFile。
- 未完成 multipart prefix 不被当成对象。
- 空页但 IsTruncated=true 时继续翻页。
- 递归删除每批删除后从空 token 重扫，不复用删除前的 opaque continuation token。
- 前几页没有匹配 key 时继续扫描，不能误判删除完成。
- 取消和内存不足返回错误。

### 16.6 FE 单测

- properties + URI 得到与 C++ 一致的 capability。
- AWS_SDK_ROUTED 的 directory client 不 endpointOverride、不强制 signer、Express auth enabled；Directory request 在 path-style=true 时 fail fast。
- general client 保留普通 AWS FIPS/dualstack/显式 endpoint 与 S3-compatible 既有行为。
- AWS_EXPLICIT_ENDPOINT + Directory Bucket fail fast。
- bucket property 为空时，从每次 remotePath 派生 capability；同一 S3ObjStorage 的 general/directory 两个 lazy client 不串配置。
- custom endpoint 下以 --x-s3 结尾的 bucket 仍按 GENERAL_PURPOSE。
- default chain、static session token、web identity、assume role provider 能传给 client。
- Java PutObject、CreateMultipartUpload、UploadPart、CompleteMultipartUpload 的 CRC32C request/response 字段完整。
- HMSTransaction 和 ObjFileSystem 正确消费 TS3MPUPendingUpload 的 checksum 字段；旧消息、缺字段和双失败路径有负例。
- S3Resource ping 使用正确 Head/List 请求。
- S3Resource ping 的业务失败与 Abort 失败同时可见，覆盖双失败测试。
- Directory Bucket presign 返回 NotSupported。
- 第 10.6 节调用面清单中的每一个入口都在“最早能取得完整 bucket 且尚未发出网络请求”的位置覆盖 Directory Bucket 负例，并保留普通 S3 正例；不能统一用一个抽象测试代替各入口门禁。

### 16.7 Cloud P0 门禁单测

- FE 创建和修改 Storage Vault 时拒绝 official AWS Directory Bucket。
- 绕过 FE，直接调用 Meta Service ADD_S3_VAULT 和 ALTER_S3_VAULT 时，在任何持久化写入前返回 INVALID_ARGUMENT。
- 覆盖其他能够直接创建或替换 S3 vault/object info 的 RPC/HTTP 路径，确认无法绕过同一门禁。
- custom S3-compatible endpoint + --x-s3 后缀 bucket 不误拒绝。
- 拒绝后 InstanceInfoPB、StorageVaultPB 和 recycler 可见状态均无变化。

### 16.8 AWS 真实集成测试

使用预创建的 Directory Bucket，凭证和 bucket 名通过 CI secret/environment 注入；测试默认关闭，只在专用 AWS job 运行。

必测：

1. Put/Get/Head/Delete 小对象和空对象。
2. 大对象 multipart，读取后逐字节校验。
3. List 超过 1000 个 key、无序返回、glob、startAfter、maxFiles、maxBytes 和 maxFile。
4. DeleteObjects。
5. 主动 Abort 后确认 upload 不存在。
6. 静态 IAM credentials。
7. AssumeRole 或 web identity。
8. 只读 policy 的读成功、写失败。
9. 缺少 s3express:CreateSession 的明确 403。
10. client 连续运行超过六分钟并跨越 session refresh。
11. 并发首次请求，验证无 session refresh stampede 和请求失败。
12. 错 region、HTTP、path-style、control endpoint 的负例。
13. 同 AZ 和跨 AZ 只记录延迟差异，不设置易抖动的硬性能阈值。

普通 S3 和 MinIO 只能证明兼容性，不能替代 CreateSession 和 Directory Bucket 语义测试。

### 16.9 回归与工程检查

按改动阶段执行：

- ./build.sh --be --fe
- run-be-ut.sh 运行相关 s3_util、s3_obj_storage_client、s3_file_writer 测试
- run-fe-ut.sh 运行 S3URI、S3Properties、S3Util、S3ObjStorage 测试
- run-regression-test.sh 运行 S3 Resource、TVF、外部文件读写相关 case
- build-support/clang-format.sh 格式化修改的 C++ 文件
- build-support/check-format.sh 检查格式
- BE build 生成 compile_commands.json 后，对修改文件运行 build-support/run-clang-tidy.sh

若没有 Directory Bucket AWS job 通过，不得把 feature 标记为 GA。

## 17. 兼容性与滚动升级

### 17.1 既有工作负载

- 不改变普通 AWS S3 和 S3-compatible 的属性名称。
- 不改变对象 key 或文件格式。
- bucket type 不新增持久化字段；TS3MPUPendingUpload 只增加向后可解析的可选 checksum 字段。
- 删除当前全局 s3_disable_content_md5，不让其改变既有 cached client。
- 普通 S3 继续使用既有 Content-MD5 和 endpoint override 策略。

### 17.2 混合版本

旧 BE 无法可靠访问 Directory Bucket。滚动升级规则：

1. 可以在混合版本期间继续使用普通 S3。
2. 必须升级所有可能执行该资源 I/O 的 BE 和 FE 后，才能创建或启用 Directory Bucket Resource。
3. TS3MPUPendingUpload 的新字段虽然是 optional，但旧 FE 会忽略 part checksum，旧 BE 也不会生成它；因此 optional 只保证协议解析，不保证 Directory Bucket 功能兼容。
4. 若未来开放 Cloud Vault，所有 recycler/checker 实例必须先升级。
5. 回滚到不支持版本前必须停止 Directory Bucket workload，并按第 20 节先把仍需访问的数据复制到普通 S3、切换到新资源；对象内容无需改格式，但旧版本不能读取留在 Directory Bucket 中的数据。

可选增强是增加 cluster capability bit，让 FE 在所有 BE 尚未声明 S3_DIRECTORY_BUCKET 时拒绝启用。若 Doris 当前没有适合的能力协商机制，首期通过发布文档和运维检查实现，不为此引入新的持久化协议。

## 18. 分阶段交付

### PR 1：SDK 基线与公共能力模型

内容：

- 固定 AWS SDK C++ 1.11.400，并修正文档中的版本根因。
- 保留 aws_logger vaLog 适配。
- 增加 service-aware capability resolver 和跨语言测试向量。
- 修复敏感 token 日志。
- client config 从现有公共配置转换，补齐 cache key。

完成条件：

- 普通 S3、MinIO 配置 UT 通过。
- Directory Bucket client 配置 UT 通过。
- 无业务 API 行为声明。

### PR 2：BE 读写完整链路

内容：

- Directory Bucket endpoint/auth client。
- PutObject CRC32C。
- multipart CRC32C 全链路。
- Hive BE-upload/FE-complete 的可选 Thrift checksum 传播。
- AbortMultipartUpload。
- DeleteObjects checksum。
- exact-key 读写。

完成条件：

- BE UT、真实 AWS 小对象和 multipart job 通过。
- session refresh 超过六分钟通过。
- 普通 S3、MinIO 回归通过。

### PR 3：List 与 FE

内容：

- S3CompatibleFileSystem 和 S3FileSystem 的 continuation token + 本地过滤排序算法。
- S3ObjStorage、S3FileSystemProperties、S3Util、S3Resource builder 和 ping。
- 完成第 10.6 节所有 Hadoop/S3A、Java v1、external catalog/scanner surface 清单，并逐入口 fail fast。
- FE 与 Meta Service 在所有 Storage Vault 创建、修改入口执行 P0 fail fast。
- Presigned URL 门禁。

完成条件：

- 多页无序 List、glob、startAfter、maxFiles、maxBytes、maxFile 语义测试通过。
- 调用面清单没有“待盘点”项，每个未支持入口都有普通 S3 正例和 Directory Bucket 负例。
- 绕过 FE 直调 Meta Service 的 add/alter vault 测试证明拒绝发生在持久化前，且第三方 --x-s3 bucket 不误判。
- FE UT、regression、真实 AWS 列举通过。

### PR 4：发布、文档与可观测性

内容：

- 用户文档、IAM 示例、部署限制。
- metrics 和错误信息。
- mixed-version 与 rollback 文档。
- AWS 专用 CI job 稳定运行。

完成条件：

- P0 验收清单全部通过。
- 发布说明明确 Cloud Vault、S3A、presign 等边界。

### 独立 P1：Cloud Storage Vault

只有第 12 节的安全模型通过产品、Cloud 和运维评审后才启动，不能作为 P0 PR 的顺手补丁。

## 19. P0 验收标准

以下条件必须全部满足：

1. 官方 AWS service + 合法 --x-s3 bucket 被稳定分类为 Directory Bucket；第三方兼容存储不误判。
2. 线上请求使用 SDK 生成的 bucket-specific Zonal HTTPS virtual-host，不出现 path-style 或自定义 endpointOverride。
3. Doris 未显式调用 CreateSession，但 AWS SDK 能自动创建、复用并刷新 session。
4. client 连续运行跨越至少一次五分钟 session 过期仍能无中断读写。
5. PutObject、UploadPart、CompleteMultipartUpload 和 DeleteObjects checksum 行为符合设计。
6. multipart 任一失败会主动 Abort，不留下由正常失败路径产生的未完成 upload。
7. ListObjectsV2 不发送 StartAfter，不依赖服务排序，多页结果符合 Doris 现有排序、startAfter、maxFiles、maxBytes 和 maxFile 契约。
8. 普通 AWS S3、MinIO/S3-compatible、Azure writer 的现有测试不回归。
9. FE 连通性检查使用相同 bucket 分类和 credentials provider。
10. 第 10.6 节清单中的 Cloud Vault、S3A、Java v1、presign 等未支持路径都在最早确定点、首个网络请求或持久化之前 fail fast；FE 与 Meta Service 的直接 add/alter Vault 路径均有证据，第三方兼容存储不误判。
11. 日志、metrics、PB/Thrift 中不存在 session secret 或明文 token。
12. C++ format、clang-tidy、BE UT、FE UT、regression 和专用 AWS 集成测试通过。
13. 用户文档明确单 AZ 故障域、同 AZ 部署建议、IAM、endpoint 和支持边界。

## 20. 回滚方案

- feature 不改变对象内容格式，但这不等于可以直接回滚：不支持该功能的旧 Doris binary 无法读取仍只存在于 Directory Bucket 的数据。
- “停止流量/关闭新写入”和“恢复旧版本下的数据可用性”是两个不同层次。前者可以立即执行；后者必须完成数据复制和资源迁移后才能执行。
- 运行时关闭方式是停止使用 Directory Bucket Resource，而不是动态切换 checksum 或禁用 session auth。
- 为需要快速回滚的生产 workload，启用前应预先创建普通 S3 bucket、复制账号/权限、容量和校验清单；必要时在切换前安排持续复制或业务双写。P0 不在 Doris 内实现自动双写。
- 若新版本出现 session、endpoint 或 list 问题，按以下顺序回滚：
  1. 停止所有新的 Directory Bucket 写入和会创建对象的作业，记录切流时间与最后成功提交点。
  2. 等待正在进行的 multipart 完成；不能完成的 upload 使用仍受支持的新版本主动 Abort，并确认没有遗留 upload。
  3. 在新版本 Doris/SDK 仍可访问 Directory Bucket 时，使用经过批准的外部复制工具或 Doris 导出路径，把仍需访问的对象复制到预建的普通 S3 bucket/root。保持对象 key，使用对象数量、字节数、清单和 checksum/逐对象读取校验复制完整性。
  4. 新建指向普通 S3 bucket/root 的 S3 Resource 或 Storage Policy；不要试图原地修改旧 S3Resource 的 endpoint、region、root path 或 bucket，这些属性在当前实现中不可修改。
  5. 使用 Doris 已支持的资源/策略/表迁移流程把 workload 切到新资源，执行代表性查询、写入和恢复检查；只有确认不再引用 Directory Bucket 后，才回滚 binary。
  6. 保留 Directory Bucket 为只读观察窗口，达到既定保留期且清单复核完成后再删除数据。
- P0 不支持 Cloud Storage Vault，因此上述切换只适用于本设计支持的外部 S3 Resource/FileSystem workload；不能把它解读为 Cloud 主存储的透明迁移方案。
- 如果故障发生时没有可用的新版本读取路径，也没有预先复制的数据，只能停止流量，不能宣称完成了数据可用性回滚。
- 不允许在同一个 Directory Bucket 上通过强制 endpointOverride 或 disableS3ExpressAuth 临时绕过故障，因为这可能产生错误签名、错误 host 或无效 checksum。

## 21. 待确认事项

下面事项不改变本文的 P0 架构决策，但必须在对应实现 PR 进入合并前关闭：

1. SDK C++ 1.11.400 对 composite CRC32C 的具体 request/response model 字段，以编译和真实 AWS 测试为准。
2. Java SDK v2 当前 pinned 版本在 Doris classloader 场景下的 S3 Express interceptor 加载行为。
3. 第 10.6 节完整调用面清单、实际依赖版本和逐入口测试属于 PR 3 的阻断项，不能以“后续盘点”状态合入。
4. presigned URL 是否有明确业务需求；若有，另写设计。
5. Cloud Storage Vault 是否允许单 AZ、无 versioning 存储作为主数据层；默认答案为不允许。
6. 是否增加 cluster capability bit 阻止混合版本启用 Directory Bucket。

## 22. 官方资料与源码依据

AWS：

- [Differences for directory buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-differences.html)
- [Directory bucket API operations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-APIs.html)
- [Creating directory buckets in an Availability Zone](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-create.html)
- [Networking for directory buckets in an Availability Zone](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-az-networking.html)
- [S3 Express One Zone session authentication](https://docs.aws.amazon.com/sdkref/latest/guide/feature-s3-express.html)
- [Authenticating and authorizing requests](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-authenticating-authorizing.html)
- [CreateSession API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateSession.html)
- [PutObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)
- [DeleteObjects API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)
- [ListObjectsV2 API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
- [Using multipart uploads with directory buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-using-multipart-upload.html)
- [PutBucketLifecycleConfiguration API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html)
- [Optimizing directory bucket performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-optimizing-performance.html)

AWS SDK C++：

- [AWS SDK C++ maintainer discussion: S3 Express customizations merged in 1.11.212](https://github.com/aws/aws-sdk-cpp/discussions/2623)
- [AWS SDK C++ 1.11.219 source tree](https://github.com/aws/aws-sdk-cpp/tree/1.11.219)
- [AWS SDK C++ S3ClientConfiguration](https://docs.aws.amazon.com/sdk-for-cpp/latest/api/aws-cpp-sdk-s3/html/struct_aws_1_1_s3_1_1_s3_client_configuration.html)
- [AWS SDK Java v2 S3BaseClientBuilder](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3BaseClientBuilder.html)

Doris 当前实现入口：

- be/src/util/s3_util.cpp
- be/src/util/s3_util.h
- be/src/io/fs/s3_obj_storage_client.cpp
- be/src/io/fs/s3_file_writer.cpp
- be/src/exec/sink/writer/vhive_partition_writer.cpp
- gensrc/thrift/DataSinks.thrift
- fe/fe-core/src/main/java/org/apache/doris/common/util/S3URI.java
- fe/fe-core/src/main/java/org/apache/doris/common/util/S3Util.java
- fe/fe-core/src/main/java/org/apache/doris/catalog/S3Resource.java
- fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3ObjStorage.java
- fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3FileSystemProperties.java
- fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3FileSystem.java
- fe/fe-filesystem/fe-filesystem-api/src/main/java/org/apache/doris/filesystem/UploadPartResult.java
- fe/fe-filesystem/fe-filesystem-api/src/main/java/org/apache/doris/filesystem/FileSystem.java
- fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/ObjStorage.java
- fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/ObjFileSystem.java
- fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/ObjectListOptions.java
- fe/fe-filesystem/fe-filesystem-spi/src/main/java/org/apache/doris/filesystem/spi/S3CompatibleFileSystem.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/hive/HMSTransaction.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/hudi/source/HudiScanNode.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/lakesoul/LakeSoulUtils.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/AbstractPaimonProperties.java
- cloud/src/common/http_helper.cpp
- cloud/src/meta-service/meta_service_resource.cpp
- cloud/src/recycler/s3_accessor.cpp
- cloud/src/recycler/s3_obj_client.cpp
- cloud/src/recycler/checker.cpp
