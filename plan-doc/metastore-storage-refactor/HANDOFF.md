# HANDOFF — Session 间接力（每完成一个阶段/任务即更新并 commit）

> **下次 agent 接手流程（强制，用户 2026-06-17 立规）**：
> 1. 先读 `PROGRESS.md` → 本文件 → `WORKFLOW.md` → 下一 task 在 `tasks.md` 的对应块 → `decisions-log.md`/`deviations-log.md` 相关条。
> 2. **对照真实代码 review 下一步方案**（不照搬本文件里的旧计划——代码可能已变；先 grep/读真实调用流，确认方案仍成立）。
> 3. 一句话复述确认 + 必要时 AskUserQuestion 定边界 → 开始实施（严格按 `WORKFLOW.md §2` 单任务 TDD 循环）。

---

**更新时间**：2026-06-18（实现 session：**P2-T03 ✅ paimon adapter cutover 到共享 metastore-spi**[commit `3c1e118dcfa`]；下一步 = **P2-T04**[paimon pom + gate 核对]，然后 **P2-T05** docker 真闸）
**更新人**：Claude（Opus 4.8）

> **本 session P2 进度补注（最新在最前）**：
> - **P2-T03 ✅（commit `3c1e118dcfa`）**：paimon 元存储**连接逻辑** cutover 到 P2-T02 建的共享 spi（paimon SDK Options 组装 + filesystem/jdbc 存储 Configuration **留连接器**，非连接事实）。**2 边界经 AskUserQuestion 定**：**D-014**（采用 spi 的 **legacy-faithful validate**——CREATE CATALOG 比当前 paimon 更严：HMS case-sensitive forbidIf(simple)/requireIf(kerberos)、REST case-sensitive `"dlf".equals`、DLF 在 CREATE 要求 OSS；故意向真 legacy 收敛）、**D-015**（JDBC **注册副作用留连接器**，仅纯 `resolveDriverUrl` 共享；不下移=单消费方+守 spi SDK/JVM-free，Rule 2）。**改动**（白名单内 5 main+2 test+pom，净 +318/−847）：`validateProperties`→`MetaStoreProviders.bind(props,{}).validate()`；`createCatalog` HMS/DLF→`bind`+新薄 `PaimonCatalogFactory.assembleHiveConf(base,overrides)`（HMS seed `ctx.loadHiveConfResources` base 再叠 `toHiveConfOverrides`；DLF `assembleHiveConf(null,toDlfCatalogConf())`）、删 build-time `requireOssStorageForDlf`；两处 driver-url→`JdbcDriverSupport.resolveDriverUrl`；`PaimonCatalogFactory` 删 6 法+`KNOWN_FLAVORS`+加 `assembleHiveConf`；`PaimonConnectorProperties` 删 `DLF_*`/`REST_TOKEN_PROVIDER`/`REST_DLF_*`（**DV-008**：别名数组只**部分**删——`HMS_URI`/`REST_URI`/`JDBC_*` 仍被保留的 `buildCatalogOptions` 用）。**TDD**：新 `PaimonConnectorValidatePropertiesTest` 13/0（3 tightening RED→GREEN 实证）+ 删 28 旧 builder/validate 测（content parity 已由 spi `Hms/DlfMetaStorePropertiesTest` 13+7 覆盖）+ 2 `assembleHiveConf` 测（F2 layering）。**验证 paimon 全模块 278/0/1skip**（skip=live gated）、checkstyle 0、import-gate 0、白名单干净。**recon `wf_9437dd4e-06d`** verify=SOUND/READY（逐键 parity 通过）；**对抗 review `wf_dd78ec4b-da5`** verify=READY/0 真 finding（唯一 MAJOR「kerberos.principal alias 未测」证伪=该键走 verbatim passthrough→测它恒真 tautology 违 Rule 9；隔离 binding 的 `service.principal`→`kerberos.principal` 方向已被 spi line72/80 覆盖）。⚠️ **docker e2e 未跑**（HMS/DLF live metastore=hive + 插件 zip ServiceLoader 发现 5 provider 在子优先 loader=P2-T05 真闸）。
> - **决策补**：D-014（采用 legacy-faithful validate）｜D-015（JDBC 注册留连接器）｜DV-008（别名数组部分删 + `bind` 取代 `parse` + 新 `assembleHiveConf` 助手）。
> - **P2-T02 ✅（commit `7ea63528bc4`）**：新建 `fe-connector-metastore-spi`（22 文件 = 15 main + 7 test）。**3 边界经 AskUserQuestion 定**：**DV-006**（fe-kerberos = compile-dep only，**零新代码**——recon 三重证伪 HANDOFF 旧写「增量补 authenticator 机制」：产出 `KerberosAuthSpec` 纯 String→值对象不需 hadoop，真 doAs 留 FE 侧 `ctx.executeAuthenticated`）、**DV-007**（parser storage 入参 = 中立 `Map<String,String> storageHadoopConfig`，**非** `List<StorageProperties>`；spi **不**依赖 fe-filesystem-api，保持 hadoop/fs-free；parser 拥有 storage-overlay 以守 kerberos-after-storage 序）、全 5 后端一次 commit。**内容**：`MetaStoreProvider<P> extends PluginFactory`（`supports`+abstract `bind(props,storageHadoopConfig)`）+ `MetaStoreProviders.bind` first-hit ServiceLoader 派发 + `MetaStoreParseUtils`（firstNonBlank/copyIfPresent/applyStorageConfig/matchedProperties + `CATALOG_TYPE_KEY=paimon.catalog.type`）+ `JdbcDriverSupport.resolveDriverUrl`（**仅纯 resolver**；driver 注册/DriverShim JVM 副作用无调用方 → 留 P2-T03，Rule 2）+ `AbstractMetaStoreProperties`（共享 raw/warehouse/matchedProperties）+ 5 `*MetaStorePropertiesImpl`（`@ConnectorProperty` 绑定，消灭 `PaimonConnectorProperties` 手抄别名）+ 5 provider（`sensitivePropertyKeys` 暴露 sensitive 键，镜像 `S3FileSystemProvider`）+ 单 `META-INF/services`（5 行）。pom = metastore-api + fe-extension-spi + fe-foundation + fe-kerberos + commons-lang3（copy-plugin-deps phase=none）。**来源 = 上移 paimon `PaimonCatalogFactory` 手抄逻辑去 fe-core 化**（HiveConf→中立 Map、authenticator→facts）；**fe-core 旧 `Paimon*MetaStoreProperties` 不动**。**HMS D-4 补回** legacy `HMSBaseProperties.buildRules` 的 forbidIf-simple/requireIf-kerberos（paimon 手抄 validate 漏；**CASE-SENSITIVE `Objects.equals` 对齐 ParamRules**，与 `buildHmsHiveConf` 的 `equalsIgnoreCase` 不对称**保留**）。验证：spi **41/0**、checkstyle 0、import-gate exit 0、无 fe-core 禁包 import、白名单干净、**3 mutation RED→GREEN**（HMS 大小写敏感·kerberos-after-storage clobber·REST 大小写敏感）。**对抗 review `wf_2ddae04d-cf9`（4 lens + verify）**：0 BLOCKER；真 MAJOR=**REST token-provider `equalsIgnoreCase`→`"dlf".equals`**（paimon 手抄 latent bug，legacy ParamRules 才权威）已修；FS `supports()` 改 `type==null||equalsIgnoreCase`（去 trim 不对称 + 对齐 legacy reject-on-malformed）；trim/accessPublic-proxyMode divergence 经核证「对齐权威 legacy contract、仅偏离非权威 paimon 手抄」→不改；补 12 测（storage re-key/clobber-via-storage-channel/alias-first-wins/username-overlay/DLF-S3-reject/dispatch-instanceof…）。**API 旁改 2 javadoc**（`getDriverUrl`「raw，consumer-resolves」+ `needsStorage` FS 准确性，诚实订正，白名单内）。⚠️ **docker 未跑**（T2 真闸 P2-T05）。
> - **决策补**：D-013（fe-kerberos 先建）｜DV-006（kerberos compile-dep-only）｜DV-007（storage 中立 Map，spi 不依赖 fe-filesystem-api）。
> - **P2-T01 ✅（commit `44d1fec4dcb`）**：新建 `fe-connector-metastore-api`（`org.apache.doris.connector.metastore`）= `MetaStoreProperties`（`providerName()`+能力方法 `needsStorage()`/`needsVendedCredentials()` 默认 false+`validate()` no-op+`rawProperties()`/`matchedProperties()`，**无 `MetaStoreType` 枚举** D-006）+ 5 子接口 HMS/DLF/REST/JDBC/FileSystem（中立 Map/标量；`HmsMetaStoreProperties` 用 fe-kerberos `AuthType`+`Optional<KerberosAuthSpec>`）。**依赖仅 fe-kerberos**（D-013；fe-foundation/fe-filesystem-api api 纯接口未用→留 spi）。pom 镜像 fe-connector-api（copy-plugin-deps none）；注册 fe-connector/pom.xml。**未建 Glue/S3Tables**（留扩展）。`MetaStorePropertiesContractTest` 3/0、checkstyle 0、import-gate exit 0、无 fe-core 禁包 import。
> - **P3a-T01 facts-carrier ✅（commit `51df4fccd01`，D-013）**：新顶层叶子 `fe-kerberos`（**零生产依赖**）facts 切片 `AuthType`(SIMPLE/KERBEROS, `fromString` 仅 "kerberos" 命中余皆 SIMPLE) + `KerberosAuthSpec`(client principal+keytab 不可变值对象, `hasCredentials()` 需两者非空；HMS service principal 不在此=HiveConf override)。6 测绿、checkstyle 0。**authenticator 机制子集（hadoop 依赖 + trino KerberosTicketUtils→JDK）= 待 P2-T02 增量补**。
> - **决策**：D-012（跳过/推迟 P1-T06 docker，验证折进 P2-T05）｜D-013（kerberos facts 归 fe-kerberos、先建；metastore-api 依赖 fe-kerberos）。
> - ⚠️ **docker e2e 全程未跑**（留 P2-T05）。

<details><summary>更早本 session（FU-T02 + FU-T03，已完成）</summary>

## 这次 session 完成了什么（FU-T02 + FU-T03）

**FU-T02 ✅（R-008 闭环，commit `e5b088b14e7`）** — fe-filesystem typed OSS/COS/OBS BE map 补 `AWS_CREDENTIALS_PROVIDER_TYPE`：
- 在 `Oss/Cos/ObsFileSystemProperties.toBackendKv()` 末尾**内联**镜像 legacy `AbstractS3CompatibleProperties.doBuildS3Configuration`(storage 包 :117-120)：`StringUtils.isBlank(accessKey) && StringUtils.isBlank(secretKey)` → `kv.put("AWS_CREDENTIALS_PROVIDER_TYPE", "ANONYMOUS")`，否则省略。仅 BE map，不碰 `toHadoopConfigurationMap`（legacy 该键只进 `getBackendConfigProperties`）。
- **DV-005（偏差，已记）**：原 D-011 说「加 `credentialsProviderType` 字段镜像 S3」——recon 证伪：legacy OSS/COS/OBS **不** override `getAwsCredentialsProviderTypeForBackend()`（只 `S3Properties` override 恒非空），即**无可配置 provider type**；加字段会引入 legacy 没有的旋钮 + 可能对有凭据 catalog 误发 `DEFAULT`（D-011 验收明确「非无条件 DEFAULT」）；且 `S3CredentialsProviderType` 在 `fe-filesystem-s3`、`fe-filesystem-{oss,cos,obs}` 不依赖 s3 → 复用须扩白名单。故改内联条件（更简、更贴 legacy，符合用户本轮「处理逻辑一致」指令；无字段/枚举/跨模块依赖/白名单扩展/AskUserQuestion）。
- **TDD**：3 个 `toBackendProperties_emitsAnonymousProviderTypeWhenNoStaticCredentials`（RED `expected <ANONYMOUS> but was <null>` → GREEN）+ 3 个有凭据测试加 `assertNull(AWS_CREDENTIALS_PROVIDER_TYPE)` 守「有凭据时省略」。OSS 13/0·COS 12/0·OBS 12/0 + 全模块绿、checkstyle 0。

**FU-T03 ✅（R-006 闭环，本次 commit）** — fe-filesystem 调优默认 UT 守护（纯 test-only，不动 main）：
- `S3/Oss/Cos/ObsFileSystemPropertiesTest` 各加 1 个 `toMaps_emit*TuningDefaultsWhenNotConfigured`：不显式设调优键时断 **BE map**（`AWS_MAX_CONNECTIONS`/`AWS_REQUEST_TIMEOUT_MS`/`AWS_CONNECTION_TIMEOUT_MS`）+ **Hadoop map**（`fs.s3a.connection.maximum`/`...request.timeout`/`...timeout`）= S3 `50/3000/1000`、OSS/COS/OBS `100/10000/10000`。
- **关键**：期望值用**字面量**非 `DEFAULT_*` 常量（否则改常量两侧同步=测试恒绿，守不住）。已核 legacy parity：`S3Properties.Env`(50/3000/1000)、`OSS/COS/OBSProperties`(各 100/10000/10000)。
- **mutation 证**：sed 改 4 个 `DEFAULT_MAX_CONNECTIONS` → 4 测全红（`<50> but was <99>` / `<100> but was <999>`），revert 后全绿。S3 15/0·OSS 14/0·COS 13/0·OBS 13/0 + 全 sibling suite 绿、checkstyle 0×4。

**红线/守门**：`git diff --name-only` 全程仅落 `fe-filesystem-{oss,cos,obs}/{main,test}`（FU-T02）+ 4 个 `*PropertiesTest.java`（FU-T03）+ 本跟踪目录；mutation 用的 main 改动经 `git checkout` 还原（post-revert status 仅余 test 文件）。⚠️ **docker e2e 未跑**（本 session 仅 compile + UT + mutation）。

<details><summary>上一个 session（FU-T01，已完成）</summary>

**FU-T01 ✅（D-010 授权，提升为 active）**：给 `fe-filesystem-hdfs` 新建 **HDFS typed BE model**，修复 P1-T04 全量切 typed BE 路引入的 HDFS BE 配置回归（**DV-004 / R-007 闭环**）。

**做了什么（仅 fe-filesystem-hdfs 核心 + 3 个已白名单文件的微改/注释）**：
1. **`HdfsFileSystemProperties.java`（新）**：`implements FileSystemProperties, BackendStorageProperties`（**BE-only，不实现 HadoopStorageProperties**——catalog/Hadoop 路保持 P1-T03 后的 raw passthrough，零新行为）。`toMap()` = **忠实移植 legacy `HdfsProperties.initBackendConfigProperties()`**（XML 资源 + `hadoop./dfs./fs./juicefs.` 透传 + 恒发 `ipc.client.fallback…`/`hdfs.security.authentication` + kerberos 块 + `hadoop.username`）；`validate()` = kerberos required-check + `checkHaConfig`（inline 移植 `HdfsPropertiesUtils`）。`backendKind()=HDFS`、`type()=HDFS`、`kind()=HDFS_COMPATIBLE`。**移植源 = fe-property `HdfsProperties`（依赖轻 BE-key-only 孪生）→ parity by construction**。
2. **`HdfsConfigFileLoader.java`（新）**：XML `hadoop.config.resources` 加载（移植 fe-property `PropertyConfigLoader`）。**F1 接线**：dir 经 `resolveHadoopConfigDir()` 读 sysprop `doris.hadoop.config.dir`（fe-core 设），默认 `$DORIS_HOME/plugins/hadoop_conf/`（与 `Config.hadoop_config_dir` 默认相同）。
3. **`HdfsFileSystemProvider.java`（改）**：re-type 为 `FileSystemProvider<HdfsFileSystemProperties>` + 新增 `bind()`/`create(P)`；**`create(Map)`/`supports()` 字节不变**（hive/iceberg/broker FE filesystem 路零回归——既有 `DFSFileSystemTest` 25/0 证）。
4. **`pom.xml`（改）**：+`fe-foundation`+`commons-lang3`（镜像 sibling s3；packaging 经 review 证无跨 loader 风险）。
5. **F1 接线（用户选「现在接好」）**：fe-core `FileSystemFactory.bindAllStorageProperties`（**项目 P1-T02 加的方法**，+1 行 `System.setProperty("doris.hadoop.config.dir", Config.hadoop_config_dir)`）→ leaf 读 sysprop → 非默认 `hadoop_config_dir` 安装也对齐 legacy。
6. **stale 注释修**（本改动作废）：`FileSystemPluginManager.bindAll` javadoc 去 HDFS skip-list（项目 P0-T02 加的方法）、paimon `PaimonScanPlanProvider` `KNOWN GAP 1`→标 CLOSED。
7. **kerberos = K1**（用户 AskUserQuestion 选）：BE-key 字符串内联发射，**不建 fe-kerberos**、**不碰** fe-filesystem-hdfs 现有 create()-side `KerberosHadoopAuthenticator`。recon 证 BE model 仅需字符串、不需 fe-kerberos（真 `UGI.doAs` 留 fe-core/ctx + 现有 DFSFileSystem，§5 不变量 4）。

**TDD/验证**：25 golden parity UT 钉 `toMap()`==legacy BE 键集（simple/kerberos/kerberos-via-Doris-alias/HA+3 负例/username/uri-derive/viewfs-jfs derive vs ofs-oss no-derive/allowFallback-blank/multi-uri/malformed-uri-fail-loud/XML/sysprop）。**fe-filesystem-hdfs 全模块 78/0/0** + checkstyle 0 + **RED/GREEN 经 mutation 证**（关 kerberos 块→`kerberosViaDorisAlias` 红）+ **fe-core `-pl fe-core -am compile` 绿**（验 FileSystemFactory/PluginManager 改）+ `git diff` 白名单干净。

**对抗 review（`wf_5db99e32-2ad`，27 agent，4 lens + verify）**：清场——packaging 无跨 loader 风险、独立 agent 逐键复核 byte-level parity、BE-only 无新 catalog 路回归、强 oss-hdfs-wrong-keys 断言被 verify **推翻**、`new Configuration()` 默认 bloat 是 legacy-faithful。**3 实质修**：①malformed-`uri` swallow→**fail-loud**（对齐 legacy）；②2 stale 注释；③+11 测试。**F1**（config-dir 未接 `Config.hadoop_config_dir`）→ 用户选「现在接好」=sysprop 桥。
</details>
</details>

## 当前状态
- 阶段：Research ✅ / Design ✅（**15 决策 D-001..D-015**）/ **Implement 🚧（P1 storage 5/6 P1-T06 docker 推迟[D-012]；P2: 3/5 = P2-T01 + P2-T02 + P2-T03 ✅；P3a facts-carrier ✅）**。
- 任务计数 **11/14**（P0: 2/2 ✅ ｜ P1: 5/6，**P1-T06 推迟** ｜ **P2: 3/5（P2-T01 + P2-T02 + P2-T03 ✅）** ｜ P3a: 0/1，facts-carrier 切片 ✅ 机制待续[DV-006 推迟到 P3b]）｜ follow-up FU-T01/02/03 ✅｜ P3b 占位。
- **新增 3 模块**：顶层叶子 `fe-kerberos`（facts 切片）+ `fe-connector-metastore-api`（5 子接口）+ `fe-connector-metastore-spi`（5 解析器 + Provider SPI，22 文件）。**paimon 连接器已 cutover 到共享 spi**（P2-T03：连接逻辑走 `MetaStoreProviders.bind`，手抄 `build*HiveConf`/`validate`/`resolveDriverUrl`/别名数组已删）。**R-006/R-007/R-008 已闭环**（UT/mutation 层）。
- ⚠️ **e2e/docker 全程未跑**（P1 storage 等价 T1 + P2 metastore T2/5-flavor 闸 一并留 P2-T05 docker 跑；D-012）。

## 下一步（明确）：P2-T04（paimon pom + gate 核对），然后 P2-T05（docker 真闸）
> **务必先按顶部流程：读文档 + 对照真实代码 review 方案再动手；实施前 WORKFLOW §2 单任务 TDD + 一句话复述。**

**P2-T04（paimon pom + gate 核对）**：
- **做什么**：核 `fe-connector-paimon/pom.xml` 依赖集 = `fe-connector-{api,spi}` + **`fe-connector-metastore-spi`（P2-T03 已加，transitively 带 metastore-api + fe-kerberos）** + `fe-filesystem-api` + `fe-thrift(provided)` + paimon SDK + hadoop/aws/…；`grep` 确认 paimon 无 fe-core import（`org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`）；`tools/check-connector-imports.sh` PASS（P2-T03 已验 exit 0，复核即可）。
- **⚠️ P2-T03 recon 揪出、P2-T04/T05 必核（packaging）**：**插件 zip 必须含 metastore-spi 的 `META-INF/services/org.apache.doris.connector.metastore.spi.MetaStoreProvider`（5 行）**，否则运行时 `MetaStoreProviders.load()` 经 ServiceLoader 在**子优先插件 loader** 下发现不到 5 provider → `bind` 抛「No MetaStoreProvider supports」→ 所有 paimon CREATE/读 挂。UT（单 flat loader）已证 services 可发现（278/0），但 **plugin-zip 子优先 loader 是 docker-gated**。核 paimon 的 assembly/copy-plugin-deps 是否把 metastore-spi（含 services 文件 + 其 transitive metastore-api/fe-kerberos）打进 zip。
- **依赖**：P2-T03 ✅。设计 §4 P2-4。

**P2-T05（docker 真闸，合并 P1-T06 的 T1 + P2 的 T2）**：paimon 5 flavor（filesystem/hms/rest/jdbc/dlf）+ vended(REST/DLF) + Kerberos HMS，`enablePaimonTest=true`。**P2-T03 未离线验的**：①HMS/DLF live `metastore=hive`（IMetaStoreClient 从 host hive-catalog-shade 解析 + 子优先 Configuration/HiveConf 跨 loader identity 危害，见 `PaimonConnector` HMS/DLF 分支 NOTE）；②上面 P2-T04 的 plugin-zip ServiceLoader 发现；③T1 storage 等价（S3/OSS/COS/OBS/HDFS + 无凭据对象存储 + 调优默认）。**D-014 行为变更**（CREATE 更严）也在此真验。
- **依赖**：P2-T03 ✅, P2-T04。设计 §4 P2-5 / §5 T2,T4。

## 未决 / 需注意
- ✅ 已闭环：R-006（FU-T03）、R-007（FU-T01）、R-008（FU-T02）。
- 📌 **残留已知（非本批引入，独立 FU）**：**oss-hdfs**（`oss://` warehouse + JindoFS）在 typed 路缺 oss 凭据键——P1-T04 已起（HDFS-family typed 缺口），彻底修需 fe-filesystem **OssHdfs typed model**（独立大动作，超白名单）。FU-T01 让 HDFS provider 对 bare-`oss://` fs.defaultFS 发无凭据 HDFS 键（review F3 MINOR，latent 误配曝露，非 working catalog 回归）。
- 📌 **scan-time 重 validate**：`getStorageProperties()` 每次 scan 经 `bindAll`→`bind()`→`of().validate()`（无 memoization）——valid catalog 内禀 dormant；是 typed-路通性（P1-T02/D-009），非 FU-T01 专有。
- ⚠️ e2e 全程未跑；P1-T06 前如不部署 docker，明确标「未跑 e2e」（CLAUDE.md Rule 12）。

## 红线提醒（WORKFLOW §4）
- **可动**（白名单）：`fe-connector-metastore-api/**` + **`fe-connector-metastore-spi/**`（新建）** + `fe-kerberos/**`（新建叶子）、`fe-connector-paimon/**`、`fe-connector-spi/**`、fe-core **仅** `connector/DefaultConnectorContext.java` + `fs/FileSystemPluginManager.java` + `fs/FileSystemFactory.java`（均**仅新增方法 / 对本项目所加方法的微改+注释**）、**`fe-filesystem/fe-filesystem-hdfs/**`（D-010，FU-T01）**、**`fe-filesystem/fe-filesystem-{s3,oss,cos,obs}/**`（D-011，FU-T02/FU-T03；main+test）**、相关 pom（`fe-connector/pom.xml`/`fe/pom.xml` 仅新增模块声明）、本跟踪目录。
- **P2-T02 额外触碰**（透明，白名单内）：`fe-connector-metastore-api` 的 `MetaStoreProperties.java`/`JdbcMetaStoreProperties.java` 各 1 处 javadoc 诚实订正（`needsStorage` FS 准确性 + `getDriverUrl` raw 语义）——非改契约方法签名。
- **P2-T03 触碰**（透明，白名单内）：`fe-connector-paimon/**` 5 main（`PaimonConnectorProvider`/`PaimonConnector`/`PaimonCatalogFactory`/`PaimonConnectorProperties`/`PaimonScanPlanProvider`）+ 2 test + `fe-connector-paimon/pom.xml`（加 `fe-connector-metastore-spi` 依赖，属 `fe-connector-paimon/**`）。**fe-core 旧 `Paimon*MetaStoreProperties` 不动；metastore-spi/api 未改**（只新增消费方）。
- **禁碰**：fe-core `datasource.property.{storage,metastore}` 包、构造点 `PluginDrivenExternalCatalog`、其它连接器（hive/hudi/iceberg/es/jdbc/mc/trino）、**其它 fe-filesystem 模块**（`-{api,spi,azure,broker,local}`，含其 test——R-008 若须给 api/spi 加共享 credentials-provider-type 须先 AskUserQuestion）、`fe-property` 模块删除。
- **FU-T01 额外触碰**（已记 D-010 + tasks，透明）：fe-core `FileSystemFactory.java`（F1 +1 行 setProperty，项目 P1-T02 加的方法）、`FileSystemPluginManager.java`（bindAll javadoc，项目 P0-T02 加的方法）、fe-connector-paimon `PaimonScanPlanProvider.java`（注释）——均 project-owned 微改/注释，非碰 pre-existing fe-core 方法。
- paimon 连接器 + fe-filesystem-hdfs **允许** import `org.apache.doris.foundation.*`（fe-foundation 叶子）、`org.apache.doris.filesystem.*`；**禁** import fe-core/fe-connector（fe-filesystem 侧 gate）。
- 每次提交前 `git diff --name-only` 对照白名单。

## 关键链接
- 设计：[`../designs/metastore-storage-property-refactor-design-2026-06-17.md`](../designs/metastore-storage-property-refactor-design-2026-06-17.md)
- 流程：[`WORKFLOW.md`](./WORKFLOW.md) ｜ 任务：[`tasks.md`](./tasks.md) ｜ 决策：[`decisions-log.md`](./decisions-log.md) ｜ 偏差：[`deviations-log.md`](./deviations-log.md) ｜ 风险：[`risks.md`](./risks.md)
- 对抗 review（FU-T01）：workflow `wf_5db99e32-2ad`（27 agent，4 lens + verify；3 实质修 + F1 接线）｜recon：`wf_de5f54be-668`（4-agent：legacy parity / fe-filesystem-hdfs / api+s3 / kerberos）
- **P2-T02**：recon `wf_187e052d-230`（4 reader + synth；证 DV-006/007）｜对抗 review `wf_2ddae04d-cf9`（4 lens + verify；REST case-sens MAJOR 修 + 12 测补 + hive.conf.resources/doAs-契约 P2-T03 follow-up）
