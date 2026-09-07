# PR #67470 概览：Azure Iceberg 数据文件原生凭证路由

> 本文基于 PR diff 和本地分支 `codex/azure-native-migration` 的代码核对，对 [apache/doris#67470](https://github.com/apache/doris/pull/67470)（`[fix](iceberg) Route Azure data files through native credentials`）进行逐层梳理。
>
> 本文回答“改了什么、怎么改的”；配套文档 `iceberg-native-azure-capability-matrix.md` 回答“改完之后什么能用、什么不能用”。

## 核心结论

- **普通 Azure ABFS/WASB 已切换到原生客户端**：数据文件和删除向量（DV）统一走 `FILE_S3` wire 值，由 BE 的 Azure Blob C++ 客户端读取。
- **OneLake 保持原有 Hadoop 路径**：通过 `.dfs.fabric.microsoft.com` host 后缀识别，继续走 `FILE_HDFS` 和 `fs.azure.*` 配置。
- **凭证边界已改为 Azure 原生字段**：FE → BE 使用 `AZURE_*` 属性传递 SharedKey、SAS 和 OAuth2 服务主体凭证，同时保留必要的 OneLake Hadoop 配置。
- **安全行为收紧**：SAS token 会校验过期时间并在日志中脱敏；OAuth2 缺少必要字段或使用非法 server URI 时 fail-closed。
- **验证仍有缺口**：FE filesystem 单测和 FE/BE 构建已通过；BE UT、真实 Entra data-plane E2E 和 OneLake 全链路回归尚未完成。

## 1. PR 元信息

| 项 | 值 |
| --- | --- |
| 标题 | `[fix](iceberg) Route Azure data files through native credentials` |
| 作者 | xylaaaaa (Chenjunwei) |
| 状态 | OPEN |
| 基分支 | master |
| head 分支 | `codex/azure-native-migration` |
| 规模 | 42 个文件，+1946 / -270，6 个提交 |
| 创建时间 | 2026-09-03（最新提交于 2026-09-07 增加 OAuth2 支持） |

### 提交列表（PR 内，倒序）

1. `ca8b1abe788` `[feature](azure) Support native Azure OAuth2 data access` — **2026-09-07**：BE 构造 `ClientSecretCredential`，FE 输出 OAuth2 服务主体字段（见第 11 节）
2. `4c88df2b3b9` `[fix](iceberg) Load Azure FileIO in metadata scanner` — metadata scanner 类加载修复
3. `ccf2b2f457b` `[fix](iceberg) Route Azure data files through native credentials` — 主路由提交
4. `a064c877135` `[feature](be) Support native Azure SAS object reads` — BE SAS 读取
5. `bc6efbeba5d` `[feature](fe) Route Azure Iceberg files through native client` — FE 路由
6. `176b4f0c172` `[test](be) Cover native Azure Iceberg delete params` — DV 参数测试

## 2. 背景与目标

### 问题

Azure Iceberg 数据文件读取此前耦合了两类 Hadoop 时代的遗留物：

- **Hadoop ABFS 兼容路径**：`abfss://` URI 被转成 `s3://` 风格，或直接走 Hadoop reader。
- **Hadoop 风格的凭证 map**：FE 把 Azure 凭证塞进 `AWS_*` / `fs.azure.*` 键，借 S3 参数语义传递。

### 目标

1. 保留 Azure account/container/object 原生 URI（不转 `s3://`，不查 Hadoop 配置）；
2. FE → BE 传递 provider 自有的原生 Azure 凭证；
3. `FILE_S3` + `provider=azure` 的 range 读取路由到原生 Azure Blob C++ 客户端。

### 行为变更（Release note）

- Azure SAS 凭证传给 BE 原生 Azure 客户端，支持 range 读取与过期校验；
- native BE OAuth2 已支持服务主体（client-secret）credential 构造；缺失字段或非法 OAuth server URI 仍 **fail-closed**，而不是静默降级；
- Fabric OneLake 的 OAuth2 保留在原有显式 Hadoop 路径上。

## 3. 数据流总览

```
FE：catalog 属性 / URI
    → StorageAdapter(provider=azure, AZURE_* 键)
    → LocationPath 路由判定：
        · OneLake（host 以 .dfs.fabric.microsoft.com 结尾）→ FILE_HDFS（Hadoop）
        · 普通 Azure ABFS / WASB → FILE_S3
    → Thrift：FILE_S3 + properties(provider=azure, AZURE_AUTH_TYPE=...)
BE：FileFactory(FILE_S3 槽位)
    → S3ClientFactory 识别 provider=azure
    → AzureAuthFactory 构建凭证：
        · SHARED_KEY → StorageSharedKeyCredential
        · SAS → 规范化 token + 过期校验
        · OAUTH2 → ClientSecretCredential（服务主体；缺失字段 fail-closed）
    → AzureObjStorageClient（原生 Azure Blob SDK）
```

## 4. 路由规则总表

| 场景 | 路由 | 认证 | 状态 |
| --- | --- | --- | --- |
| Azure ABFS/WASB + SharedKey | `FILE_S3` 原生客户端 | SharedKey | 保持兼容 |
| Azure ABFS/WASB + SAS（含 vended） | `FILE_S3` 原生客户端 | SAS + 过期校验 | 本次新增 |
| Azure ABFS/WASB + OAuth2 | `FILE_S3` 原生客户端 | Azure C++ `ClientSecretCredential` | 需要真实 Entra data-plane E2E；Databricks REST token 不作为 Azure token |
| Fabric OneLake + OAuth2 | `FILE_HDFS` Hadoop ABFS | 走原有 `fs.azure.*` 配置 | 保持不变 |
| 删除向量（DV）读取 | `FILE_S3` + `AZURE_*` 属性 | 同数据文件 | 不重建 HDFS 参数，防重回 Hadoop |

## 5. BE / common（C++）

### 5.1 S3URI — `be/src/util/s3_uri.cpp` / `s3_uri.h`

- 新增 `abfs` / `abfss` / `wasb` / `wasbs` 四种 scheme 的解析；
- authority 形如 `container@account.dfs.core.windows.net`，拆出：
  - `_bucket = container`
  - `_endpoint = account.dfs.core.windows.net`
  - `_account = account`（host 第一个 `.` 之前）
- 保留原生 URI 形态；原生客户端从 endpoint/account 推导连接参数，不查 Hadoop 配置；
- 顺带修复：复用 `S3URI` 实例重新 parse 时清理上次的 Azure 状态（此前会残留 authority）。

### 5.2 S3ClientConf 与工厂 — `be/src/util/s3_util.cpp` / `s3_util.h`

这是改动最大的 BE 文件：

- `S3ClientConf` 新增字段:
  - `token_expiration_time_ms`（vended token 过期时间，Unix 毫秒；0 保持长生命周期凭证的旧行为）
  - `azure_auth_type`（空值：有 token 时推断为 SAS，否则为 SharedKey）
  - `azure_oauth_client_id` / `azure_oauth_client_secret` / `azure_oauth_tenant_id` / `azure_oauth_server_uri`（OAuth2 服务主体四元组；hash 与 `to_string()` 已纳入，**secret 不进日志**）
- `is_s3_conf_valid`（OAuth2 校验）：
  - OAUTH2 不能与 SAS token 组合；
  - OAUTH2 必须提供 client id、client secret、OAuth server URI，否则返回 `InvalidArgument`（fail-closed，便于诊断）；
- `set_azure_auth_type`：解析 `SHARED_KEY` / `SAS` / `OAUTH2`；
- 新增 `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID` / `AZURE_OAUTH_SERVER_URI` / `AZURE_OAUTH_ACCOUNT_HOST` 键（含 `azure.oauth2_*` 别名），并纳入 Azure binding 自描述检测；
- 兼容旧 FE 只发 `fs.azure.account.auth.type.*=OAUTH` 的场景：显式标记为 OAUTH2，并从 `fs.azure.account.oauth2.client.id/secret/endpoint.*` 恢复服务主体字段，而不是静默解释成空 SharedKey 凭证；
- 创建客户端时按 auth type 分派 `AzureAuthFactory`。

### 5.3 AzureAuthFactory — `common/cpp/obj-client/auth/azure_auth_factory.cpp` / `.h`

- `AzureCredentialType` 枚举扩为 `{SHARED_KEY, SAS, OAUTH2}`；`AzureCredentialOptions` 新增 OAuth2 四元组字段；
- `create()`：
  - SAS：规范化 token、校验过期并构建 SAS 凭证；
  - OAUTH2：构造 Azure C++ `Azure::Identity::ClientSecretCredential`：
    - 校验 client id / client secret / OAuth server URI 非空；
    - tenant id 缺省时**从 OAuth server URI 的 path 推导**（取非 `oauth2` / `v2.0` / `token` 的路径段）；
    - `AuthorityHost` 从 URI 推导（`scheme://host[:port]`；非 HTTPS 或无 host 时报告错误）；
    - 构造失败返回具体诊断错误。
- OAUTH2 保留为显式枚举值，保证调用方选择 `ClientSecretCredential`，而不是把 OAuth2 材料误当作 SharedKey。

### 5.4 AzureObjStorageClient — `common/cpp/obj-client/azure_obj_storage_client.cpp`

- **安全修复**：所有错误日志 / 异常消息中的 URL query 部分脱敏为 `?<redacted>`（SAS 签名不再进日志）；路径展示同样裁掉 `?` 之后的部分。
- `generate_presigned_url`：SAS 模式下 `_credential == nullptr` 且 URL 已带 query 时，直接返回 SDK URL 原样。SAS 本身就是签名 URL，没有 account key 无法再扩展，调用方按 token 原始过期时间处理；OAuth2 模式（凭证为空、URL 无 query）返回空串。Entra ID 授权走 SDK pipeline，但无法签 Blob SAS URL，调用方必须走 authenticated client 路径。

### 5.5 FileFactory — `be/src/io/file_factory.cpp` / `file_factory.h`

- 明确 `FILE_S3` 是 Azure 的兼容 wire 值：`TStorageBackendType::AZURE` 恒返回 `FILE_S3`，永不回退 `FILE_HDFS`；
- 注释明确：该分支不涉及 Hadoop reader，provider 自有属性由 `S3ClientFactory` 消费并分派到 `AzureObjStorageClient`；
- reader/writer 创建时把原始 path 透传（用于错误信息展示真实 URI）。

### 5.6 S3FileReader / S3FileWriter / S3FileSystem — `be/src/io/fs/`

- 三个文件处理同一件事：透传 `display_path`，展示 `abfss://...` 而不是拼接的 `s3://bucket/key`；展示前裁掉 `?` 后的 SAS query；
- `ObjClientHolder::reset` 补上 `token_expiration_time_ms` / `azure_auth_type` 的拷贝。

## 6. FE（Java）

### 6.1 fe-core 门面 — `LocationPath` / `StorageAdapter` / `StorageUriUtils`

- `findStorageAdapter`：AZURE 类型**不再回退**通用 S3 binding。回退会丢掉 account@host authority，且可能静默选中不相关的凭证集；
- `validateAndNormalizeAzureUri`：校验 scheme 后保留原生 URI（仅规范化大小写），不再转成 `s3://` 风格；
- `StorageUriUtils.isOneLakeLocation`：从 legacy `AzurePropertyUtils` 移植，正则完全一致；
- OneLake 特判原样保留：`abfs/abfss` + OneLake host → `FILE_HDFS` / `HDFS`；
- OAuth2 仅限 Iceberg REST 的 gate 保留（顺带修复 legacy 大小写敏感导致小写 `oauth2` 绕过 gate 的历史 bug，注释有说明）。

### 6.2 CredentialUtils — `fe/fe-core/src/main/java/org/apache/doris/datasource/credentials/CredentialUtils.java`

- 新增 `adls.sas-token.<account-host>` / `adls.sas-token-expires-at-ms.<account-host>`（Iceberg Unity Catalog / ADLSFileIO 的 vended 凭证形状），规范化为 provider 自有的 `AZURE_*` 键；
- SAS 过期校验：非正整数或已过期时抛出 `AzureSasCredentialException`（**不再 fail-soft**，防止过期 token 静默回退到静态凭证集）；
- 一次 scan 只接受一个 account host，多 host vended token 明确拒绝；
- 规范化时移除所有 Azure 别名（`adls.*` / `azure.*` / `azure_*`），防止无关的 `azure.auth_type=OAuth2` 在别名优先级上压过刚校验的 SAS token。

### 6.3 DefaultConnectorContext — `fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java`

- `vendStorageCredentials`：Azure SAS 校验失败（`AzureSasCredentialException`）向上传播，其余保持 legacy fail-soft；
- `buildVendedStorageMap`：**移除合成 HDFS 绑定**。plugin registry 会给无 HDFS provider 的 map 注入默认 HDFS 项；纯 Azure vended token 不得继承该回退（否则 Azure → Hadoop 通道会在 scan 层复活）。token 显式含 HDFS 配置（`hdfs.*` / `dfs.*` / `hadoop.*` / `fs.defaultfs` 等）时保留真实 HDFS 绑定，支持混合 connector。

### 6.4 fe-filesystem-azure — `AzureFileSystemProperties` / `AzureFileSystemProvider` / `AzureObjStorage`

- 新增 SAS token / expiry 属性（含别名 `azure.sas-token`、`AZURE_SAS_TOKEN` 等），`validate()` 增加 SAS 过期校验；
- backend map 改为 provider 自有的 `AZURE_*` 键（SAS 不是 AWS session token，注释专门强调）；
- OAuth2 的 `toMap()`：**同一 binding 同时输出两套词汇表**——`fs.azure.*` 账号级配置 + Hadoop Configuration dump（供 OneLake 的 `FILE_HDFS` 路径消费），以及 `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID` / `AZURE_OAUTH_SERVER_URI` / `AZURE_OAUTH_ACCOUNT_HOST`（供原生 `FILE_S3` 路径构造 `ClientSecretCredential`）；由 URI 选中的 reader 决定消费哪套；
- `toHadoopConfigurationMap()` 增加 SAS 分支（`fs.azure.sas.fixed.token.<host>`）；
- `AzureFileSystemProvider.supports()` 增加 SAS token 键的识别；
- FE 侧 `BlobServiceClient`（非 scan 路径的 FE 自用客户端）支持 SAS。

### 6.5 Iceberg connector — `IcebergCatalogFactory` / `IcebergScanPlanProvider` / `IcebergWritePlanProvider`

- `selectEffectiveStorages`：AZURE 存在时丢掉无显式 HDFS 配置的合成 HDFS 绑定（防止 Hadoop 默认配置混入原生 Azure scan/write payload）；真实混合 Azure + HDFS catalog 在显式 HDFS 配置存在时保持完整；
- scan / write 两路的 BE 凭证改为输出 `AZURE_*` 键。

### 6.6 BE Java 扩展 — `fe/be-java-extensions/iceberg-metadata-scanner`

- 新增 `iceberg-azure` 依赖（元数据任务需要反序列化表的 Iceberg FileIO，即 ADLSFileIO）；
- **传输层冲突处理**：Doris 使用 Netty 4.2，而 Azure 的 Netty transport 编译于 Netty 4.1，因此排除 `azure-core-http-netty`，改用 JDK HTTP transport（`azure-core-http-jdk-httpclient`）；
- `IcebergSysTableJniScanner`：FileScanTask 反序列化时显式使用扩展 classloader（`resolveClass` + 线程上下文），使 ADLSFileIO 类可见。

## 7. OneLake 兼容性保留（专项核对）

OneLake 是微软 Fabric 的存储层，URI 形如 `abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<path>`，认证只有 OAuth2 一种。它和普通 Azure ABFS URI 长得一模一样，路由逻辑必须按 host 后缀区分。逐项核对结果：

| 检查点 | 结果 |
| --- | --- |
| 检测正则一致 | 分支 `StorageUriUtils.ONELAKE_PATTERN` 与 legacy `AzurePropertyUtils` 完全一致（`abfs[s]?://([^@]+)@([^/]+)\.dfs\.fabric\.microsoft\.com(/.*)?`，大小写不敏感） |
| `FILE_HDFS` 特判保留 | `LocationPath.getTFileTypeForBE()` / `getFileSystemType()` 的 OneLake 特判原样保留，`testOnelakeStorageLocationConvert` 更新后仍断言 OneLake → `FILE_HDFS` |
| OAuth2 Hadoop 配置保留 | `AzureFileSystemProperties.toMap()` 的 OAuth2 分支仍 `putAll(oauth2BackendProperties())`，`fs.azure.*` 配置供 OneLake 的 HDFS 路径使用 |
| BE OAuth2 路由不误伤 | 普通 Azure ABFS 走 native `ClientSecretCredential`；OneLake 仍走 `FILE_HDFS` 兼容路径，不会改变其 Hadoop OAuth2 行为 |
| FE 侧 OAuth2 可配置 | `validate()` 仍接受 OAuth2 auth_type；REST-only gate 保留 |

### 已知风险

1. **无 OneLake 端到端测试**：PR 内只有 FE 单测（构造 adapter map 验证 TFileType 路由），没有真实 OneLake catalog → 属性绑定 → backend props（含 `fs.azure` OAuth2 键）的全链路测试。
2. **边缘配置场景**：`findStorageAdapter` 对 AZURE 不回退 S3 后，依赖 catalog 能绑定出 AZURE adapter。标准 OneLake 配置（AZURE 风格属性）没问题；纯 `fs.azure.*` Hadoop 风格 key 的奇异配置可能绑不出 AZURE adapter 而报错（该场景在 master 上是否真能工作也存疑）。

## 8. 测试覆盖（42 个文件中 16 个是测试）

| 层 | 测试文件 | 覆盖点 |
| --- | --- | --- |
| BE | `be/test/util/s3_uri_test.cpp` | abfs/wasb scheme 解析、authority 拆分 |
| BE | `be/test/io/s3_client_factory_test.cpp` | OAuth2 字段转换、legacy SAS 别名走原生、fs.azure.* legacy 形状识别 |
| BE | `be/test/io/fs/azure_obj_storage_client_test.cpp` | SAS 过期校验 |
| BE | `be/test/format/table/iceberg/iceberg_delete_file_reader_helper_test.cpp` | 原生 Azure delete 参数不构建 HDFS 参数（DV 读取不重回 Hadoop）；非 Azure 调用方保持 HDFS 契约 |
| FE core | `LocationPathTest` | OneLake → FILE_HDFS、普通 Azure → FILE_S3 且保留原生 URI |
| FE core | `DefaultConnectorContextNormalizeUriTest` / `DefaultConnectorContextVendTest` | URI 归一化、vended 凭证覆盖 |
| FE core | `CredentialUtilsTest` | ADLS SAS → AZURE_* 归一化、过期校验 |
| FE azure | `AzureFileSystemPropertiesTest` / `AzureFileSystemProviderTest` / `AzureObjStorageExtensionTest` | SAS 属性、provider 识别 |
| 连接器 | `IcebergAzureFileIoClasspathTest` ×2（fe-connector + metadata-scanner） | ADLSFileIO 可加载、Netty transport 被排除 |

## 9. 验证状态与缺口

| 项 | 状态 |
| --- | --- |
| FE Azure filesystem 模块单测 | ✅ 97 tests，0 failures，0 errors |
| BE 单元测试覆盖 | ⚠️ 代码已加，但 **BE UT 二进制尚未编译运行** |
| 构建 | ✅ `./build.sh --be --fe` 通过，`doris_be` / `doris-fe.jar` 产物已验证 |
| Regression test | ❌ 未做（PR checklist 未勾选） |

### 需要注意的缺口

1. BE UT 未运行仍是验证缺口——OAuth2 提交只做了 BE changed-object 增量编译，UT 二进制仍未见运行记录；
2. **OAuth2 无真实 Entra data-plane E2E**：`ClientSecretCredential` 构造、tenant/authority 推导均有单测，但没有真实服务主体访问 Azure 数据文件的端到端验证；Databricks REST token 不作为 Azure token 使用（表格已标注）；
3. OneLake 全链路（见第 7 节）缺少端到端回归，最划算的补充是一个 FE 测试：标准 OneLake 属性集 → `StorageAdapter.ofAll` → `LocationPath.ofAdapters` → 断言 `FILE_HDFS` 且 backend map 含 `fs.azure.account.oauth2.*` 键。

## 10. 贯穿全文的设计主线

1. **凭证形状去 Hadoop 化**：FE → BE 边界从 `AWS_*` / `fs.azure.*` 改为 provider 自有的 `AZURE_*` 词汇表，防止 Azure 凭证继承 AWS/S3 参数语义；
2. **OAuth2 从 fail-closed 演进为原生支持**：初版提交对原生 OAuth2 显式报错，后续 `ca8b1abe788` 提交补齐 `ClientSecretCredential` 构造；缺失字段 / 非法 OAuth server URI 仍 fail-closed（诊断错误），不静默降级；唯一例外是 OneLake 继续走 Hadoop；
3. **防“回头路”**：删除合成 HDFS 绑定、AZURE 不回退 S3、DV 读取不构建 HDFS 参数——多处逻辑共同阻止 Azure 流量重新掉回 Hadoop 通道；
4. **凭证安全**：SAS 签名在日志 / 错误消息中全程脱敏，OAuth2 client secret 不进 `to_string()`；过期 token 不静默降级。

## 11. 后续提交专项：原生 OAuth2 支持（`ca8b1abe788`）

> 2026-09-07 新增，10 个文件，+282 / -53。该提交解决了初版路由完成后 OAuth2 服务主体访问被回归的问题：此前 `FILE_S3` 路由已就位，但 BE 拒绝 OAUTH2。

### 11.1 BE 侧

- `is_s3_conf_valid`：删除 OAUTH2 的 `NotSupported` 拒绝，改为字段完备性校验（OAuth2 需 client id / client secret / server URI；不能与 SAS token 组合）；
- `S3ClientConf` / `ObjClientHolder::reset`：新增并透传 `azure_oauth_client_id` / `azure_oauth_client_secret` / `azure_oauth_tenant_id` / `azure_oauth_server_uri`；
- 新后端键：`AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID` / `AZURE_OAUTH_SERVER_URI` / `AZURE_OAUTH_ACCOUNT_HOST`（含 `azure.oauth2_*` 小写别名），并入 Azure binding 自描述检测；
- legacy `fs.azure.account.oauth2.client.id/secret/endpoint.*` 形状从“标记后报错”改为“恢复服务主体字段”；
- `AzureAuthFactory::create`：构造 `ClientSecretCredential`；tenant id 缺省时从 OAuth server URI path 推导；`AuthorityHost` 从 URI 推导（强制 HTTPS）；失败返回具体诊断错误；
- `generate_presigned_url`：OAuth2 模式返回空串（Entra ID 无法签 Blob SAS URL），调用方须走 authenticated client 路径。

### 11.2 FE 侧

- `AzureFileSystemProperties.toMap()`：OAuth2 分支同时输出两套词汇表——`fs.azure.*`（OneLake 的 `FILE_HDFS` 路径）+ 5 个 `AZURE_*` OAuth2 字段（原生 `FILE_S3` 路径），由 URI 选中的 reader 决定消费哪套；
- 新增后端键常量与 `AZURE_OAUTH_SERVER_URI` / `AZURE_OAUTH_ACCOUNT_HOST` 输入别名。

### 11.3 测试

| 测试 | 覆盖 |
| --- | --- |
| `AzureAuthFactoryTest.BuildsOAuth2ClientSecretCredential` | ClientSecretCredential 构造成功 |
| `AzureAuthFactoryTest.DerivesOAuth2TenantFromServerUri` | tenant id 从 URI path 推导 |
| `AzureAuthFactoryTest.RejectsIncompleteOAuth2Credential` | 缺失字段拒绝 |
| `S3ClientFactoryTest.ConvertsNativeAzureOAuth2Properties` | OAuth2 属性 → S3ClientConf 转换 |
| `AzureFileSystemPropertiesTest` | backend map 断言 4 个 OAuth2 键、原生别名绑定 |
