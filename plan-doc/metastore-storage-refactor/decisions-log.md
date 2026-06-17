# 决策日志（ADR，append-only，时间倒序）

> 编号 `D-NNN` **仅在本子项目内有效**，与上层 `../decisions-log.md` 独立。
> 新决策写在顶部。修改设计文档某节时，在该节加 `（D-NNN）` 脚注。

---

## D-009 — bind-all 机制 + 白名单 +1（FileSystemPluginManager）（应对 DV-001）
- **日期**：2026-06-17 ｜ **决策者**：用户（应对 P0-T01 证伪 P0-1 预期）
- **内容**：实现 `ConnectorContext.getStorageProperties()`（返回 fe-filesystem typed `StorageProperties`）所需的 raw map → `List<StorageProperties>` 绑定，落在 fe-core `FileSystemPluginManager` 新增 additive `public List<StorageProperties> bindAll(Map)`（镜像现有 `createFileSystem` 的 provider 循环，但用 `provider.bind(props)` 全量收集所有 `supports()` 命中者，而非首个命中 `create`）。`DefaultConnectorContext.getStorageProperties()` 调它；raw map 经现有 `storagePropertiesSupplier` 值的 `getOrigProps()` 取（fe-core `ConnectionProperties` 公有 getter），**不改构造点**（`PluginDrivenExternalCatalog` 零改动）。
- **理由**：守 D-003「连接器只见 fe-filesystem-api」架构（否决 C「ctx 返回 Map」=放弃该目标边）；bindAll 放 fe-core（非 fe-filesystem-spi 静态）契合设计 §2.1「fe-core 用 providers 全量绑定」且能见 directory 插件（否决 B）。fe-core 改动 = `DefaultConnectorContext` + `FileSystemPluginManager` 两文件、均纯新增。
- **影响**：WORKFLOW §4.1 白名单 +`FileSystemPluginManager.java`（仅新增 bindAll）；risks R-004 改「唯一」为「两处 fe-core 新增」；设计 §4 P0-1/P0-2 回写（+DV-001 脚注）；tasks P0-T02/P1-T02。**fe-core 旧 storage 包仍零改动**（bindAll 用 fe-filesystem providers，不碰 `datasource.property.storage`）。

## D-008 — Vended creds 边界：连接器只「抽取」，fe-core 单点「归一」
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：vended creds 处理边界 = **连接器只负责 SDK 特定的原始 token 抽取**（paimon 已落地于 `PaimonScanPlanProvider.extractVendedToken(table)`，从活的 paimon SDK 表对象抠出任意 shape 的 `s3.*`/`oss.*` token）；**raw-token → 统一 BE map** 的归一（`CredentialUtils.filterCloudStorageProperties` + `StorageProperties.createAll`（provider 重绑、派生 region/endpoint/调优默认）+ `getBackendPropertiesFromStorageMap` → `AWS_ACCESS_KEY/SECRET_KEY/TOKEN/ENDPOINT/REGION`）**仍由 fe-core `ConnectorContext.vendStorageCredentials(rawToken)` 单点实现**。fe-core 旧 `Paimon/IcebergVendedCredentialsProvider` 的抽取逻辑后续随各连接器迁移正式下沉到连接器（paimon 已下沉），本次不删 fe-core 旧类（D-005）。
- **理由**：raw-token→BE-map 必须套**后端特定**默认值（region/endpoint 推导、S3=50/3000/1000 vs OSS/COS/OBS=100/10000/10000 调优分叉），需 storage providers（ServiceLoader 发现）；而连接器按 D-003 **只见 fe-filesystem-api 接口、不持有 providers**，物理上无法独立完成全程归一。延续 D-003「动态/provider 发现留 fe-core」的精确边界：连接器最轻、归一单点、无漂移。**备选 B（放宽红线让连接器依赖 fe-filesystem-spi/providers 自做端到端）被否**：加重连接器 classpath + 破坏「fe-connector 仅依赖 fe-filesystem-api」红线。
- **影响**：设计 §2.3（细化边界）；tasks `P1-T04` 已符合（vended 路径不动，仍走 `ctx.vendStorageCredentials`），**无新增 task**；为后续 hive/iceberg 迁移确立统一 vended 模式。

## D-007 — Kerberos 抽到新叶子模块 fe-kerberos（单一真相源）
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：新建叶子模块 **`fe-kerberos`**（依赖**仅** hadoop-auth/hadoop-common；把唯一外部依赖 trino `KerberosTicketUtils` 用 JDK `javax.security.auth.kerberos` 替掉），把 fe-common `org.apache.doris.common.security.authentication.*` 整套搬入作**唯一真相源**；`fe-filesystem-hdfs` **删掉自有的** `KerberosHadoopAuthenticator`、改依赖 fe-kerberos；统一两个打架的 `HadoopAuthenticator` 接口（fe-common 用 `PrivilegedExceptionAction` vs fe-filesystem-spi 用 `IOCallable`）为单接口 + 消费侧 adapter。`fe-common`/`fe-core`/`fe-filesystem-*`/`fe-connector-*` 共用。`fe-kerberos` **置于顶层**（与 `fe-foundation`/`fe-common` 平级的中立叶子），无环。
- **归属/命名（用户 2026-06-17 二次确认）**：**否决** `fe-connector-auth`（置于 fe-connector 组）——会破坏两条规则：(1) `fe-filesystem-* ──╳──► fe-connector` gate（fe-filesystem-hdfs 无法依赖它删除自有副本）；(2) `fe-common`（低层）反向依赖 fe-connector 子模块=层级倒挂。故必须是**顶层中立叶子**才能被 fe-filesystem-hdfs + fe-common 共用（=满足原始需求「HMS 与 HDFS 都能用」）。**模块名定 `fe-kerberos`**（用户 2026-06-17 确认）。
- **理由**：现状 kerberos **三处实现**——(1) fe-common `security.authentication.*`（fe-core/HMS/HDFS 用）；(2) fe-filesystem-hdfs **自抄一份** `KerberosHadoopAuthenticator`（为避免依赖 fe-common，一年前拷贝、TGT 刷新可能已漂移）；(3) paimon 手抄 HMS 的 kerberos HiveConf 键 + 回调 `ctx.executeAuthenticated`。改一处要改三处。auth 类 import 干净（仅 JDK/hadoop/log4j/commons/guava + 1 个 trino），fe-common 不依赖 fe-core → 抽取无阻力；`fe-foundation` 现为纯净（零 hadoop）的 `@ConnectorProperty` 叶子，**不应**被 hadoop 污染（故否「折进 fe-foundation」）；也否「fe-filesystem/fe-connector 直接依赖较重的 fe-common」。
- **范围（分两步，用户 2026-06-17 确认）**：(a) **P3a 纳入本次** = 先建 `fe-kerberos` + 让 **paimon** 的 HMS kerberos facts 走它（paimon-local、纯新增，**不碰** fe-common/fe-filesystem-hdfs 既有路径，符合 D-005）；(b) **P3b = follow-up（本次不做）** = 全量去重（删 fe-filesystem-hdfs 副本、fe-common 重指向 fe-kerberos、统一两个 `HadoopAuthenticator` 接口），与 hive/iceberg 迁移同批——此步改 fe-common + fe-filesystem-hdfs，超出 D-005，故独立。
- **影响**：设计 §3.5（新增 Kerberos 节）+ 依赖图（加 fe-kerberos 叶子）；tasks 新增 P3；risks 补「kerberos 三处漂移」项。

## D-006 — MetaStore 后端「类型」用 Provider 自识别，api 层不放 per-backend 枚举
- **日期**：2026-06-17 ｜ **决策者**：用户（修正 D-001/设计 §3.1 初版的 `MetaStoreType` 枚举）
- **内容**：`fe-connector-metastore-api` 的 `MetaStoreProperties` 接口**不放 per-backend `MetaStoreType` 枚举**；后端标识用 **`String providerName()`**，横切行为用**能力方法**（如 `boolean needsStorage()` / `needsVendedCredentials()`）表达。新增 `fe-connector-metastore-spi` 的 **`MetaStoreProvider<P extends MetaStoreProperties>` SPI**（`boolean supports(Map)` 自识别 + `bind(raw, storageList)`），经 `META-INF/services` + ServiceLoader 发现，**镜像 `FileSystemProvider`**。新增后端（Glue/S3Tables/自定义）= 新模块 + 新 provider + 一行 services 文件，**api/spi 零改动、无中心 switch**。唯一旧消费者 `VendedCredentialsFactory:61` 的 `getType()` switch 用能力方法替代。
- **理由**：把 per-backend 枚举放进 api 会**原样继承**旧 `MetastoreProperties.Type` 的脆性（每加后端改 api 枚举 + 找全散落 switch、漏一个无编译期报错）；fe-filesystem 已用 provider 模式干净解决同一问题，连 `FileSystemType` per-backend 枚举顶上都挂着官方「加类型要改多处、易错」的反模式 TODO。**高层 category 枚举（如 `StorageKind`）可留**（封闭小集 + 承载横切行为），但 metastore 当前无此需要，能力方法足矣。
- **影响**：设计 §3.1（重写 type() 部分为 provider 模型）/ §3.2（加 `MetaStoreProvider` SPI + ServiceLoader）；tasks `P2-T01` 改写（去枚举、加 provider）；**修正 D-001** 措辞中的「MetaStoreType 枚举」。

## D-005 — 本次任务范围：纯新增/迁移，只动 paimon
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：本次任务 **不删除** fe-core `datasource.property.{storage,metastore}` 任何类；**不修改** hive/hudi/iceberg/es/jdbc/mc/trino 连接器；`fe-property` 仅断开 paimon 依赖边（变孤儿），**不物理删**；import gate 不收紧。Phase 3+ 及 fe-core 两包的最终删除属范围外（后续全连接器迁完再做）。
- **理由**：保持改动外科化、可回滚；隔离 paimon 的迁移风险，不波及在用的 hive/hudi/iceberg。
- **影响**：设计文档 §0.1 / §4（Phase 1/2 改为 additive、删除 Phase 3+ 与 fe-core 删除步骤）/ §6。

## D-004 — typed MetaStore 属性复用 fe-foundation @ConnectorProperty 绑定
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：新 MetaStore 属性模型用 `@ConnectorProperty` + `ConnectorPropertiesUtils` 注解绑定（别名优先级/required/sensitive/matchedProperties 全免费），镜像 fe-filesystem StorageProperties 做法。
- **理由**：消除 paimon 手抄的 `String[]` 别名数组 + `firstNonBlank`；单一真相源。
- **影响**：设计 §3.2（typed holder）。

## D-003 — fe-core 绑定 Storage，经 ConnectorContext 下发已解析的 StorageProperties
- **日期**：2026-06-17 ｜ **决策者**：用户（修正 agent 初版"混合 Option C"）
- **内容**：CREATE CATALOG 入口在 fe-core；fe-core 用 fe-filesystem（全量+providers）绑定 `StorageProperties`，经 `ConnectorContext.getStorageProperties(): List<StorageProperties>`（fe-filesystem-api 类型）传给连接器；连接器只见 api 接口，调 `toHadoopProperties()/toBackendProperties()`。动态/RPC（vended creds、URI 归一、thrift `TS3StorageParam`）留 fe-core 经 ConnectorContext 委托。
- **理由**：provider 发现（ServiceLoader）集中 fe-core；连接器 classpath 无需 providers、只依赖 fe-filesystem-api 接口——精确满足"fe-connector 仅依赖 fe-filesystem-api"；比"连接器自调静态门面"（agent 初版 Option C）更干净。
- **影响**：设计 §2 / §3.2 / §4 P1-1,P1-2。**取代** agent 初版的静态门面方案（无需给 fe-filesystem-api 加 `buildObjectStorageHadoopConfig` 等价静态门面）。

## D-002 — 去重策略：混合（共享后端 fact 解析器 + 薄 per-connector adapter）
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：HMS/DLF/Glue/REST/JDBC 的"连接事实解析器"在 `fe-connector-metastore-spi` 实现**一次**（含 JDBC DriverShim）；每个连接器只写薄 catalog adapter 消费 facts 建各自 SDK catalog。
- **理由**：最大化去重（HMS 后端现被复制约 4 次、DLF 8-key 块 3 次、DriverShim 2 次），又不把引擎 SDK 塞进共享层。
- **影响**：设计 §3.2 / §3.3。

## D-001 — 新建 fe-connector-metastore-api + fe-connector-metastore-spi 模块对
- **日期**：2026-06-17 ｜ **决策者**：用户
- **内容**：MetaStore Property SPI/API 放在新建的模块对，依赖 fe-foundation + fe-filesystem-api，镜像 fe-filesystem 的 api/spi 拆分；不折叠进现有 fe-connector-api（其极简，仅 fe-thrift provided）。
- **理由**：metastore 模型需要 @ConnectorProperty 绑定（fe-foundation）+ StorageProperties 入参（fe-filesystem-api），不应污染 fe-connector-api 的消费契约层。
- **影响**：设计 §0 / §3.1 / §3.2。
