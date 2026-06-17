# 偏差日志（DV，append-only，时间倒序）

> 编号 `DV-NNN` **仅在本子项目内有效**，与上层 `../deviations-log.md` 独立。
> 规则（沿用 ../README §4.3）：原设计在落地中发现不可行/不必要时，**先**在此顶部记录，**再**改设计文档；禁止 silently 改设计。
> 每条格式：`DV-NNN`、日期、原计划位置（设计 §x / task Pn-Tnn）、为何不可行、新方案、影响范围。

---

## DV-005 — FU-T02 不新增 `credentialsProviderType` 字段（镜像 S3 的写法），改为内联镜像 legacy 基类条件
- **日期**：2026-06-18 ｜ **原计划位置**：task `FU-T02` / D-011（「给 `Oss/Cos/ObsFileSystemProperties` 加 `credentialsProviderType` 字段（镜像 `S3FileSystemProperties`）」）。
- **为何不可行/不必要**：现场 recon（对照 `fe-core .../datasource/property/storage`）证伪「镜像 S3 字段」是正确做法——
  1. **legacy OSS/COS/OBS 没有可配置的 provider type**：`OSSProperties/COSProperties/OBSProperties` 均**不** override `AbstractS3CompatibleProperties.getAwsCredentialsProviderTypeForBackend()`（:124-129），该基类仅在 **ak/sk 皆空**时返回 `ANONYMOUS`、否则 `null`（省略）。只有 `S3Properties` override（:308 恒非空，故 S3 typed 才有字段）。加可配置字段会引入 legacy 不存在的旋钮，并可能对**有凭据** catalog 误发 `DEFAULT`（D-011/FU-T02 验收明确「**非**无条件 DEFAULT」）。
  2. **`S3CredentialsProviderType` 枚举在 `fe-filesystem-s3`**，而 `fe-filesystem-{oss,cos,obs}` **不依赖** s3 模块（仅 `fe-filesystem-spi`）→ 复用须移类到 api/spi = 扩白名单 + AskUserQuestion。recon 结论是**反过来**：根本不需要共享类型。
- **新方案**：在每个 `toBackendKv()` 末尾**内联**镜像 legacy `doBuildS3Configuration`(:117-120)——`StringUtils.isBlank(accessKey) && StringUtils.isBlank(secretKey)` 时 `kv.put("AWS_CREDENTIALS_PROVIDER_TYPE", "ANONYMOUS")`，否则省略；字面量 `"ANONYMOUS"` == `AwsCredentialsProviderMode.ANONYMOUS.name()`。**仅** BE map（`toBackendKv`），**不**碰 `toHadoopConfigurationMap`（legacy 该键只进 `getBackendConfigProperties`）。无字段、无枚举、无跨模块依赖、无白名单扩展、无 AskUserQuestion。
- **影响范围**：FU-T02 实现仅落 `fe-filesystem-{oss,cos,obs}/src/main` 各 +5 行 + test 各 +2 断言；与原 D-011 **功能等价且更贴 legacy**（更简、更外科，CLAUDE.md Rule 2/3/7）；不影响 S3 typed（已有字段，行为不变）、不影响 R-006/FU-T03。**R-008 闭环**。

## DV-004 — P1-T04 BE 凭据全量切 typed 路会丢 HDFS BE 键（fe-filesystem 无 HDFS typed BE model）→ 用户定「按原计划全切、接受 HDFS 回归、follow-up 补 fe-filesystem HdfsFileSystemProperties」
- **日期**：2026-06-17 ｜ **原计划位置**：设计 §5 T1 / WORKFLOW §5.2 T1 / **DV-002**（「P1-T03/T04 全量切换 fe-filesystem，含 P1-T04 BE 凭据也切 `toBackendProperties().toMap()`」，隐含与 P1-T03 同源等价）；task `P1-T04`。
- **为何偏差（现场 recon 取证，对照真实代码）**：新 typed 路对 **HDFS 物理上产不出 BE 键**——
  - **fe-filesystem 无 HDFS typed BE model**：`HdfsFileSystemProvider implements FileSystemProvider<FileSystemProperties>`（基接口泛型,**无**具体 `HdfsFileSystemProperties` 类),**未** override `bind()` → 用 `FileSystemProvider.bind()` 默认实现 `throw UnsupportedOperationException("...does not support typed FileSystemProperties binding yet")`;`FileSystemPluginManager.bindAll` **catch 该异常并跳过** → `getStorageProperties()` 对 HDFS-warehouse catalog 返回**空** → `toBackendProperties().toMap()` 链无 HDFS 项。
  - **legacy 路有 HDFS**：`getBackendStorageProperties()`(`DefaultConnectorContext:203` → `CredentialUtils.getBackendPropertiesFromStorageMap` → fe-core `HdfsProperties.getBackendConfigProperties:198`)发 HDFS 的 `hadoop.*/dfs.*/HA/kerberos` 键。
  - **这些键 load-bearing**:经 `PluginDrivenScanNode.getLocationProperties`(去 `location.` 前缀) → `FileQueryScanNode.setLocationPropertiesIfNecessary` → `HdfsResource.generateHdfsParam(locationProperties)` → `THdfsParams`(namenode/HA/kerberos)。故全量切丢 HDFS BE 配置 → **HDFS-warehouse paimon 原生读回归**(解析不了 HA nameservice / 无 kerberos)。对象存储侧两路等价(typed 为超集,DV-002)。
- **关键事实**：`getBackendStorageProperties()` 是 **`ConnectorContext` SPI 方法、不依赖 fe-property**(连接器只 import `connector.spi.ConnectorContext`),故 **P1-T05 断 paimon→fe-property 边并不需要本切换**;切换纯为 D-003「连接器只见 fe-filesystem-api 的统一 typed 消费」架构收益,而该收益对 HDFS 物理做不到(除非动 fe-filesystem,超 P1 白名单)。
- **新方案（用户 2026-06-17 定）**：**按原计划全切**——对象存储走 typed 路 `getStorageProperties()→toBackendProperties().toMap()`(`.ifPresent` 跳过无 BE 项,镜像 P1-T03 风格),HDFS 暂丢;**接受 HDFS BE 回归**,后续由用户**补 fe-filesystem `HdfsFileSystemProperties` typed BE model** 修复(记 **follow-up FU-T01** + **R-007**)。代码注释标 `KNOWN GAP (DV-004 / R-007)`。**被否**:(a) 保留 `getBackendStorageProperties`(最安全、零回归,但放弃 D-003 统一);(b) 混合两路(对象存储 typed + HDFS legacy,多代码、要管叠加顺序/防重复,无功能收益)。
- **影响范围**：P1-T04 实现(`PaimonScanPlanProvider` 静态凭据块 + 注释)与测试(`scanContext` 改喂 `getStorageProperties()` 的 fake `StorageProperties` + 新 `fakeBackendStorage` helper);新增 **R-007** + **follow-up FU-T01**(tasks 占位);**P1-T06 docker HDFS flavor 会暴露此回归**(须知晓为**已接受、非新 bug**);不影响对象存储 flavor、不影响 P2/P3a。`getBackendStorageProperties()` SPI default 方法**保留**(连接器停止调用,移除非「新增」、超 P1-T04 范围,留作 follow-up 清理)。

## DV-003 — T1 自动等价测试不可在 UT 落地 → 改「connector-local 契约 UT + docker 兜底」；并连带 P1-T03 提前删 fe-property import + 删 ~23 canonical 测试
- **日期**：2026-06-17 ｜ **原计划位置**：设计 §5 T1 / WORKFLOW §5.2 T1（DV-002 框架：「新 `toHadoopConfigurationMap()` 与旧 `buildObjectStorageHadoopConfig` 在常见静态凭据路径 key/value **全等**」作为切换回归闸）；task P1-T03、P1-T05。
- **DV-003-a（T1 落地形态，用户 2026-06-17 选 Option C）**：
  - **为何不可行（现场 recon 取证）**：算 T1「新产物」须绑真 fe-filesystem 对象存储 `StorageProperties`，而其 impl 模块（`fe-filesystem-{s3,oss,cos,obs}`）是 `Env.loadPlugins` **运行时目录插件**——`fe-core` pom 注「impl 运行时依赖在 Phase 4 P4.1 移除」（仅留 `fe-filesystem-local` test-scope）、paimon 从无。故 **fe-core 与 paimon 任一单测 classpath 都绑不出**真 S3/OSS/COS/OBS fe-filesystem 实例 → 字面 key/value 等价测试无法在 UT 写。强行把 impl 拖进单测 classpath 既破"impl 仅运行时"架构，又冒本仓历史反复出现的 paimon 跨 loader / classpath 中毒风险。
  - **新方案（Option C）**：paimon UT **只钉 connector-local 契约**（合成 storage map → 落 conf/HiveConf + `paimon.*` 改键 + 原始 `fs./dfs./hadoop.` 透传 + **last-write-wins** + kerberos-在-storage-叠加-之后）；**真 key/value 等价由 P1-T06 docker 5-flavor 兜底**；P0-T01 4-agent recon + DV-002 的 code-read 等价（fe-filesystem ⊇ fe-property 超集差异已记）为依据。**修订 DV-002 的「自动 key/value 全等 UT」→「契约 UT + docker 闸」**。
  - **被否选项**：(B) paimon 内加 fe-filesystem impl test-scope 依赖自建等价测试 = transient（P1-T05 paimon 弃 fe-property 即须删）+ 重复 SDK 依赖 + 与 paimon 自带 hadoop-aws classpath 冲突风险；(A) fe-core companion 等价测试 = 同样须把运行时-only impl 拖进 fe-core test classpath + 扩 fe-core 白名单（新测试文件 + test-scope pom）。两者都把运行时插件拖进单测，user 否。
- **DV-003-b（import 顺序连带）**：`PaimonCatalogFactory` 的 `org.apache.doris.property.storage.StorageProperties` import **仅** :393 一处用（`buildObjectStorageHadoopConfig`）。P1-T03 删该 call 即孤立 import → checkstyle 报未用 import → **P1-T03 必同删 import**（原 P1-T05 计划"删 :20 import"）。**P1-T05 退化为仅删 pom `fe-property` 依赖边 + `grep org.apache.doris.property` 归零闸**（call/import 已在 T03 清）。
- **覆盖核对（删 canonical 测试）**：现 `PaimonCatalogFactoryTest` ~23 个 S3/OSS/COS/OBS/MinIO canonical 翻译测试测的是 fe-filesystem 现职责。**对抗 review（`wf_76df09a4-c2f`，8 agent，1 BLOCKER+3 MAJOR+2 MINOR；verify 推翻 BLOCKER[删 buildHmsHiveConf 重载=唯 paimon 调用方全已改]+2 MAJOR[endpoint-pattern/OSS-derivation 经核实 fe-filesystem 已覆盖])+ 直接核实**：fe-filesystem 覆盖 **canonical 键翻译 + endpoint-from-region 派生**（`S3FileSystemPropertiesTest.toHadoopConfigurationMap`、`OssFileSystemPropertiesTest:108-110`、Cos/Obs），**但 NOT 覆盖调优默认值**（S3 50/3000/1000、OSS/COS/OBS 100/10000/10000）→ 删 paimon tuning 测试丢了**显式 UT 守护**（功能今日正确=fe-filesystem 字段默认真发；测试健壮性缺口）→ **记 R-006**（docker P1-T06 兜底 + fe-filesystem 加断言 follow-up，超白名单）。**初判「已全覆盖」修正为「键翻译+派生已覆盖、调优默认未守护」。**
- **影响范围**：P1-T03 实现与测试改造、P1-T05 范围缩减；设计 §5 T1 / WORKFLOW §5.2 T1 待回写（DV-003 脚注）；risks R-001 缓解更新（自动 UT 闸 → docker 闸）。不影响 P2/P3a。

## DV-002 — T1 等价性从「全等」放宽为「常见静态凭据路径全等 + 文档记超集」
- **日期**：2026-06-17 ｜ **原计划位置**：设计 §5 T1 / §6.4 验收 item 4 / WORKFLOW §5.2 T1（"新 == 旧 key/value **全等**"）。
- **为何不可行（P0-T01 取证）**：fe-filesystem `toHadoopConfigurationMap()`/`toBackendProperties().toMap()` 是 paimon 现走 fe-property 路（`buildObjectStorageHadoopConfig`）的**超集**，非全等：
  - **S3**：fe-filesystem 加 assume-role 分支（`fs.s3a.assumed.role.*`）+ 无 AK 时 anonymous/default `fs.s3a.aws.credentials.provider`；fe-property base 二者皆无。
  - **OSS/COS/OBS**：配置齐时一致（jindo/cosn/obs 块都在），但 fe-filesystem `fs.s3a.endpoint`/`.region` **无条件**发（`cfg.put`）vs fe-property **懒发**（仅非空时）。
  - **BE map**：fe-filesystem `toMap()` 多 `AWS_BUCKET`/`AWS_ROOT_PATH`/`AWS_CREDENTIALS_PROVIDER_TYPE`。
  - 均为 fe-filesystem 更完整的**有意设计**，非 bug。故字面「全等」测试必红。
- **新方案（用户 2026-06-17 定 A）**：认 fe-filesystem 为**新事实源**。**T1 = 常见静态凭据路径**（S3/OSS/COS/OBS 配齐 endpoint/region/AK/SK，无 role、无 vended）下各后端 key/value **全等**（含调优默认分叉 S3=50/3000/1000 vs 其它 100/10000/10000）+ **文档明记超集差异为「有意、更完整」**。P1-T03/T04 全量切换 fe-filesystem（含 P1-T04 BE 凭据也切 `toBackendProperties().toMap()`）。
- **影响**：设计 §5 T1 / §6.4 / WORKFLOW §5.2 T1 加（DV-002 修订）脚注；risks R-001 缓解更新；P1-T03/T04 的 T1 测试钉常见路径全等 + 注释超集（对照 fe-property 现产物）。

## DV-001 — P0-1 预期「fe-filesystem-api 已够用、无需门面」被证伪：缺 raw map → List<StorageProperties> 的 bind-all 入口
- **日期**：2026-06-17 ｜ **原计划位置**：设计 §4 P0-1 / §2.1 / 决策 D-003；task P0-T01；WORKFLOW §4.1 路径白名单（"唯一 fe-core 改动 = DefaultConnectorContext"）。
- **为何不可行（取证）**：
  - fe-filesystem `org.apache.doris.filesystem.properties.StorageProperties` 是**纯接口、无静态工厂**（无 `createAll`）。绑定靠各 `FileSystemProvider.bind(Map)`。
  - 仓内**不存在**任何「raw map → `List<fe-filesystem StorageProperties>`」聚合入口：`FileSystemPluginManager.providers` 私有，唯一出口是**首个命中**的 `createFileSystem`（返回 `FileSystem`，不是 StorageProperties，且非全量）；`FileSystemFactory.getProviders()` 包级私有且仅 ServiceLoader。
  - `DefaultConnectorContext` 当前**只持有 fe-core typed map 的 supplier**（`Map<fe-core StorageProperties.Type, fe-core StorageProperties>`），不持有 raw map；fe-filesystem 是**另一族** StorageProperties。raw map 可经现有 supplier 值的 `getOrigProps()`（fe-core `ConnectionProperties` 公有 getter）取回，**无需改构造点**；但**绑定步骤**仍需新代码。
  - 结论：实现 `getStorageProperties()`（返回 fe-filesystem 类型）**至少需要在 DefaultConnectorContext 之外再加一个 additive `bindAll(...)`**（fe-core `FileSystemPluginManager` 或 fe-filesystem-spi），无法塞进 `DefaultConnectorContext` 单文件 → 白名单需最小扩张。
  - 另：F1 等价性——fe-filesystem `toHadoopConfigurationMap()` 与 paimon 现走的 fe-property `buildObjectStorageHadoopConfig` 在**静态凭据常见路径全等**（COS/OSS/OBS 的 jindo/cosn/obs 块都在）；fe-filesystem 为**超集**（S3 assume-role/anon 分支额外键 + OSS/COS/OBS endpoint/region 无条件 vs 懒发）。非阻塞，但确认 fe-filesystem 为新事实源，T1 钉常见路径全等 + 记超集差异。
- **新方案（用户 2026-06-17 定向 A，记 D-009；已回写）**：在 fe-core `FileSystemPluginManager` 加 additive `public List<StorageProperties> bindAll(Map)`（镜像 `createFileSystem` 的 provider 循环，但 `bind` 全量收集而非首个命中 `create`）；`DefaultConnectorContext.getStorageProperties()` 调它，raw map 经现有 supplier 值的 `getOrigProps()` 取（不碰构造点）。已回写：设计 §4 P0-1/P0-2、WORKFLOW §4.1 白名单（+FileSystemPluginManager）、decisions D-009、risks R-004、tasks P0-T01/P0-T02。
  - **A（荐）**：守 D-003 架构（连接器消费 fe-filesystem-api typed StorageProperties）。在 fe-core `FileSystemPluginManager` 加 additive `public List<StorageProperties> bindAll(Map)`（镜像 `createFileSystem`），`DefaultConnectorContext.getStorageProperties()` 调它（raw map 经 `getOrigProps()` 取，不碰构造点）。fe-core 改动 = DefaultConnectorContext + FileSystemPluginManager 两文件、均纯新增。
  - **B**：同架构，但 `bindAll` 放 fe-filesystem-spi 静态（ServiceLoader）→ fe-core 仅改 DefaultConnectorContext；代价=改 fe-filesystem-spi（同样白名单外）+ 仅见内置 provider（storage 足够）。
  - **C（更简、偏离 D-003）**：不下发 typed 对象；加 `ConnectorContext.getStorageHadoopConfig(): Map<String,String>`，fe-core 用现有 typed map 单点算（与 hive/iceberg 同源、零漂移），paimon 调它。改动**确可**局限 DefaultConnectorContext 单文件；但连接器**不再**依赖 fe-filesystem-api（放弃 D-003 的「fe-connector → 仅 fe-filesystem-api」目标边）。
- **影响范围**：P0-T01 结论、P0-T02 / P1-T02 / P1-T03 / P1-T04 的绑定机制与白名单；不影响 P2/P3a。
