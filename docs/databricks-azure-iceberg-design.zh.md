# Doris 访问 Databricks Azure Iceberg：开发目标与整体方案

本文从“让 Doris 访问 Databricks 上、数据存放在 Azure 的 Iceberg 表”这一需求出发，说明问题、目标、设计和验收边界，不按 PR 或 commit 组织。

设计说明对照 2026-09-18 本分支源码 `d88d302bf44`。本文不是已发布版本的支持声明，也不把设计目标当成已经全部通过的测试结果。

## 1. 最初要解决什么问题

**用户希望在 Doris 中连接 Databricks Unity Catalog，查询其中存放在 Azure 上的 Iceberg 表；对于远端允许写入的表，还能使用 Doris 已有的 Iceberg 写入能力。**

这里不是让 Doris 把 SQL 转发给 Databricks 执行，而是通过 Iceberg REST Catalog 获取表信息，再由 Doris 访问底层文件并执行查询。示意位置如下，账号和路径均为占位符：

```text
abfss://container@account.dfs.core.windows.net/table/data/file.parquet
```

因此，连接成功有两个不同层次：

1. **访问目录服务**：Doris 可以向 Databricks 获取表、快照、文件位置等信息。
2. **访问 Azure 文件**：Doris 执行节点可以使用正确的存储凭证读取数据文件和删除文件；写入时还需要相应的存储权限和目录提交权限。

只做到列出库表，甚至生成查询计划，都不代表第二步已经打通。

本场景重点处理 REST 服务下发的临时 Azure SAS，即 vended credentials。SAS 是带权限、有效期等限制的存储访问令牌，不能当作登录 Databricks 的 token，也不能直接当作 AWS session token 使用。

## 2. 为什么不只是增加几个 Azure 配置项

Doris 已有 Azure、Hadoop 和对象存储访问能力，本开发不是“从零支持 Azure”。需要补齐的是 **Iceberg 返回的 Azure 文件位置与凭证，经过 FE 规划后，能够一致地到达 BE 原生文件访问链路**。

| 需要解决的问题 | 仅增加配置项为什么不够 | 本方案的处理 |
|---|---|---|
| 目录认证和存储认证不同 | 能访问 REST 服务，不代表能读取 Azure 文件。 | 分开处理 REST 登录凭证和 Azure 存储凭证。 |
| Azure URI 有自己的含义 | `container@account`、DFS/Blob endpoint、路径编码不能按普通 S3 地址处理。 | 由 Azure provider 解释和校验 URI，保留对象路径语义。 |
| 临时凭证有范围和有效期 | 不能将返回的 SAS 无条件覆盖整个 catalog，也不能永久复用。 | 在请求内选择配置，检查账号、位置范围和有效期。 |
| 同一张表涉及多种文件 | 数据文件、删除文件、metadata 和 manifest 不一定由同一个客户端读取。 | 原生数据访问与 Iceberg FileIO 分开生成执行配置。 |
| 通用层还服务其他存储 | 为 Azure 修改路由时，可能影响 S3/HDFS 等既有访问。 | 保留未被替换的其他存储配置，并验证旧调用路径。 |

## 3. 开发目标与不做的事情

### 3.1 本次要达成的目标

- **先打通主场景**：Databricks REST Catalog 返回 Azure 表及临时 SAS 后，Doris 可以正确读取 Azure 数据，并正确应用 Iceberg 删除语义。
- **数据文件使用 BE 原生 Azure 客户端**：把相关 data/delete 文件访问接到 Azure C++ SDK，而不是将这部分文件 I/O 留在 Hadoop/JNI 路径。
- **保持元数据路径可用**：metadata、manifest 和系统表仍通过适合它们的 Iceberg FileIO 访问，不因数据文件切换客户端而丢失认证。
- **在远端允许的范围内支持写入**：已有 INSERT、DELETE、UPDATE、MERGE、OVERWRITE 能力使用正确的 Azure 文件访问配置；成功还取决于 Iceberg 表能力及远端提交规则。
- **覆盖既有认证方式并避免回归**：除主场景的 SAS 外，原生 Azure 认证模型也承载 SharedKey 和 OAuth2 客户端密钥认证，不能为了接入临时凭证而破坏旧配置。

### 3.2 不应由此推导出的承诺

- 不保证所有 Databricks 表类型、DDL、维护操作都可读写；远端权限和提交限制仍然有效。
- 不重写 Iceberg 格式、Parquet/ORC reader 或事务协议。
- 不将所有 Iceberg metadata 访问都改成 C++ 原生访问。
- 不把 Managed Identity、Workload Identity 等认证方式自动算作已支持或已验收。
- 不将 OneLake 自动切换到普通 Azure Blob 原生路径；它保留独立的 Hadoop 访问边界。
- 不承诺切换为原生客户端就必然获得某个性能提升比例，性能结论需要单独测试。

## 4. 整体方案：目录、文件访问和执行各负其责

```text
用户在 Doris 查询 Iceberg 表
    |
    v
FE Iceberg connector ---- REST 认证 ----> Databricks Unity Catalog
    ^                                        |
    |________ 表元数据、文件位置、存储凭证 _____|
    |
    +-- metadata / manifest
    |       -> Iceberg FileIO 读取并规划
    |       -> 系统表等任务需要时，携带 FileIO 配置进入 BE Java scanner
    |
    +-- data / delete 文件
            -> FE 选择该位置对应的存储配置
            -> 生成 URI + 文件类型 + BE 访问属性
            -> BE 原生 Azure client
            -> 直接访问 Azure Storage
```

FE 负责“访问哪一个文件、使用哪组配置”，BE 负责“用这组配置创建客户端并执行文件读写”。写入时，BE 写出文件不等于事务已成功；仍须由 Iceberg 的提交链路更新表元数据。

### 4.1 区分两类认证

| 认证对象 | 用途 | 本文需要明确的区别 |
|---|---|---|
| Databricks REST Catalog | 获取表信息、执行目录操作或提交 | REST token 不是 Azure 文件访问凭证。 |
| Azure Storage | 实际读取、写入、删除文件 | 使用 SAS、SharedKey 或本方案支持的 OAuth2 客户端密钥认证。 |

**“Azure OAuth2 数据访问通过”不等于“Databricks REST OAuth2 登录通过”。** 它们必须分别配置和验证。

### 4.2 给每个文件选择一套完整配置

将已经解析好的存储配置称为 binding：它包含 provider 所需的账号、endpoint、认证方式等信息。请求级 resolver 根据文件 URI 选择 binding，并一次返回：

```text
provider + 校验/归一化后的 URI + BE 文件类型 + BE 访问属性
```

这样下游不会把一组配置解析出的 URI，与另一组配置的凭证拼在一起。resolver 使用 catalog 静态配置和 connector 已取得的临时凭证，**本身不向 Databricks 或 Azure 申请 SAS**。

新请求可以取得新凭证并建立新的配置快照；一次规划内可以复用自己的快照。快照保证配置选择一致，不保证凭证永久有效，也不等于查询执行期间会自动替换已经下发的 SAS。过期凭证必须报错，不能静默借用另一身份继续访问。

实现依据：[请求级配置选择](../fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java)及 [Azure 配置模型](../fe/fe-filesystem/fe-filesystem-azure/src/main/java/org/apache/doris/filesystem/azure/AzureFileSystemProperties.java)。

### 4.3 数据与元数据不能共用一份未经区分的属性

| 访问对象 | 执行路径 | 配置要求 |
|---|---|---|
| 原生 Azure data/delete 文件 | BE 对象存储 reader/writer + Azure SDK | 生成 `provider=azure` 和 `AZURE_*` 执行字段。 |
| Iceberg metadata/manifest | Iceberg FileIO；部分任务经 JNI 执行 | 使用 FileIO/Hadoop 能识别的认证配置，必要时携带固定的凭证有效期。 |

当前方案复用 `FILE_S3` 作为原生对象存储的 FE/BE 传递入口，通过 `provider=azure` 选择 Azure 客户端。**这里的 `FILE_S3` 不表示用 AWS 客户端访问 Azure。**

FileIO 也不能随意替换：例如，不能让只配置了旧静态身份的 HadoopFileIO 静默接管新下发的 Azure FileIO 凭证。具体选择与校验见 [Iceberg FileIO 属性适配](../fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergRestFileIOProperties.java)。

## 5. 为什么改到通用存储层，其他存储要不要接入

**需求来自 Azure，但“根据文件位置一致地选择存储配置”是通用问题。** 如果只在 Iceberg 中增加 Azure 特判，扫描、写入和元数据操作容易各自维护一套不一致的 URI 与凭证选择规则。

因此职责分成三层：

1. **通用存储层**：组织配置快照、选择 provider、返回完整访问结果，不硬编码 Azure 认证规则。
2. **Azure provider**：理解 Azure URI、SAS、SharedKey、OAuth2，并生成不同客户端所需的属性。
3. **Iceberg connector**：在自己的扫描、写入和元数据生命周期中使用这些能力。

其他存储不需要使用 Azure 的凭证字段，也不需要为了这个需求新增一种认证方式。S3/HDFS 等已有 binding 应保持自己的行为，Azure 凭证更新不能清掉它们。

但通用层变化仍需做兼容性测试。旧 connector 不会因为新接口存在就自动迁移；若希望使用完整访问结果，需要显式接入 resolver。**“没有强制迁移”不等于“通用改动对旧路径没有影响”。**

## 6. 如何判断这个开发目标完成

验收应围绕真实用户操作，而不是以编译通过或新增接口为终点。

| 验收层次 | 需要证明什么 |
|---|---|
| 目录连接 | 能通过 REST 认证发现目标库表并加载 Iceberg 元数据。 |
| 实际查询 | FE/BE 可以访问真实 Azure 文件，查询结果正确；不能只看 SHOW TABLES 或 EXPLAIN。 |
| 删除语义 | 已删除的数据不会重新出现，覆盖目标版本适用的 position delete / deletion vector。 |
| 元数据操作 | manifest 和目标系统表使用正确的 FileIO 配置，跨 JNI 后仍可访问。 |
| 原生路径 | 关闭相关缓存后，以 profile/客户端证据确认文件 I/O 确实经过原生 Azure 路径。 |
| 写入闭环 | 在允许写入的表上执行目标 DML，提交后重新读取并校验结果。 |
| 凭证边界 | 过期、账号不匹配、scope 不匹配等情况按预期失败；新凭证不混用旧身份或其他 provider 的配置。 |
| 兼容性 | 既有 SharedKey、S3/HDFS 路径和约定的升级顺序得到验证。 |

SAS、SharedKey、OAuth2，以及 Iceberg 版本和文件格式需要分别记录结果。一个组合通过，不能代替整个矩阵。单测可以覆盖解析、路由和异常边界，但不能代替真实 Databricks/Azure 环境验收；普通外表 CI 通过也不等于专用 Azure suite 已执行。

真实验收需要预先准备可访问的 REST catalog、Azure 存储及相应权限，凭证通过外部配置提供，不写入测试代码。专用 suite 的环境开关、运行方法与输出基线生成方式见[验收说明](azure-native-iceberg-acceptance.md)。

部署时先升级 BE，再启用新 FE 的原生 SAS/OAuth2 数据访问；这些模式不能安全地交给不认识新字段的旧 BE。完整滚动升级约定也见上述验收说明。

## 7. 阅读入口

- **理解开发目标和方案**：本文。
- **搭建验收环境并运行测试**：[Native Azure Iceberg 验收说明](azure-native-iceberg-acceptance.md)。
- **追溯评审问题的处理**：[Azure native review 修复记录](azure-native-review-resolution.md)，其中验证状态应按记录的范围理解，不作为实时 CI 状态。

本文不维护逐次流水线状态和完整云端测试结果；这两类记录与设计分开，避免把某次环境结果误写成长期产品承诺。
