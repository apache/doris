# HANDOFF — Session 间接力（每完成一个阶段/任务即更新并 commit）

> **下次 agent 接手流程（强制，用户 2026-06-17 立规）**：
> 1. 先读 `PROGRESS.md` → 本文件 → `WORKFLOW.md` → 下一 task 在 `tasks.md` 的对应块 → `decisions-log.md`/`deviations-log.md` 相关条。
> 2. **对照真实代码 review 下一步方案**（不照搬本文件里的旧计划——代码可能已变；先 grep/读真实调用流，确认方案仍成立）。
> 3. 一句话复述确认 + 必要时 AskUserQuestion 定边界 → 开始实施（严格按 `WORKFLOW.md §2` 单任务 TDD 循环）。

---

**更新时间**：2026-06-18（实现 session：**FU-T02 + FU-T03 完成** — fe-filesystem 对象存储补齐，**R-008 + R-006 闭环**；下一步 P1-T06）
**更新人**：Claude（Opus 4.8）

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

## 当前状态
- 阶段：Research ✅ / Design ✅（**11 决策 D-001..D-011**）/ **Implement 🚧（P1 5/6 + FU-T01/T02/T03 ✅，仅剩 P1-T06 验证）**。
- 任务计数 **8/14**（P0: 2/2 ✅ ｜ P1: 5/6 ｜ P2: 0/5 ｜ P3a: 0/1）｜ follow-up **FU-T01 ✅ + FU-T02 ✅ + FU-T03 ✅**（均已完成）｜ P3b 占位。
- **R-006 / R-007 / R-008 全部已闭环**。typed BE 路现对 S3/OSS/COS/OBS/**HDFS** 全产 BE 键 + OSS/COS/OBS 无凭据补 `ANONYMOUS` + 调优默认有 UT 守护。
- ⚠️ **e2e/docker 未跑**（本 session 仅 compile + UT + mutation 证）。

## 下一步（明确）：P1-T06（P1 验证收口）
> **R-006/R-007/R-008 已全闭环** → P1-T06 应为**干净全绿验收**（不带已知漂移）。
> **务必先按顶部流程：读文档 + 对照真实代码 review 方案再动手；实施前 WORKFLOW §2 单任务 TDD + 一句话复述。**

**P1-T06（P1 验证收口）**：paimon UT 全绿（已 293/0/1skip）+ docker `enablePaimonTest=true` **5 flavor**（filesystem/hms/rest/jdbc/dlf）+ vended(REST/DLF) + Kerberos HMS + **真 T1 等价闸 Option C**。
- **HDFS-warehouse flavor（含 HA / kerberized）应通过**（R-007 闭环验证点，FU-T01）；**无凭据 OSS/COS/OBS 应通过**（R-008 闭环，FU-T02）；**调优默认**由 FU-T03 UT 守护（R-006）→ 干净全绿。
- **现场 recon 必做**：确认 docker paimon 测试套件入口（`enablePaimonTest=true` 如何起、5 flavor 的 regression-conf）+ 当前分支 jar 打包要点（paimon 模块需 `-am package -Dassembly.skipAssembly=true`，shade jar 携带 HiveConf）。**不部署 docker 则明确标「未跑 e2e」**（CLAUDE.md Rule 12），不得把「编译过」当「验证过」。
- 之后 P2（metastore SPI：P2-T01 新建 fe-connector-metastore-api …）+ P3a（fe-kerberos 叶子）。

## 未决 / 需注意
- ✅ 已闭环：R-006（FU-T03）、R-007（FU-T01）、R-008（FU-T02）。
- 📌 **残留已知（非本批引入，独立 FU）**：**oss-hdfs**（`oss://` warehouse + JindoFS）在 typed 路缺 oss 凭据键——P1-T04 已起（HDFS-family typed 缺口），彻底修需 fe-filesystem **OssHdfs typed model**（独立大动作，超白名单）。FU-T01 让 HDFS provider 对 bare-`oss://` fs.defaultFS 发无凭据 HDFS 键（review F3 MINOR，latent 误配曝露，非 working catalog 回归）。
- 📌 **scan-time 重 validate**：`getStorageProperties()` 每次 scan 经 `bindAll`→`bind()`→`of().validate()`（无 memoization）——valid catalog 内禀 dormant；是 typed-路通性（P1-T02/D-009），非 FU-T01 专有。
- ⚠️ e2e 全程未跑；P1-T06 前如不部署 docker，明确标「未跑 e2e」（CLAUDE.md Rule 12）。

## 红线提醒（WORKFLOW §4）
- **可动**（白名单）：`fe-connector-paimon/**`、`fe-connector-spi/**`、fe-core **仅** `connector/DefaultConnectorContext.java` + `fs/FileSystemPluginManager.java` + `fs/FileSystemFactory.java`（均**仅新增方法 / 对本项目所加方法的微改+注释**）、**`fe-filesystem/fe-filesystem-hdfs/**`（D-010，FU-T01）**、**`fe-filesystem/fe-filesystem-{s3,oss,cos,obs}/**`（D-011，FU-T02/FU-T03；main+test）**、相关 pom、本跟踪目录。
- **禁碰**：fe-core `datasource.property.{storage,metastore}` 包、构造点 `PluginDrivenExternalCatalog`、其它连接器（hive/hudi/iceberg/es/jdbc/mc/trino）、**其它 fe-filesystem 模块**（`-{api,spi,azure,broker,local}`，含其 test——R-008 若须给 api/spi 加共享 credentials-provider-type 须先 AskUserQuestion）、`fe-property` 模块删除。
- **FU-T01 额外触碰**（已记 D-010 + tasks，透明）：fe-core `FileSystemFactory.java`（F1 +1 行 setProperty，项目 P1-T02 加的方法）、`FileSystemPluginManager.java`（bindAll javadoc，项目 P0-T02 加的方法）、fe-connector-paimon `PaimonScanPlanProvider.java`（注释）——均 project-owned 微改/注释，非碰 pre-existing fe-core 方法。
- paimon 连接器 + fe-filesystem-hdfs **允许** import `org.apache.doris.foundation.*`（fe-foundation 叶子）、`org.apache.doris.filesystem.*`；**禁** import fe-core/fe-connector（fe-filesystem 侧 gate）。
- 每次提交前 `git diff --name-only` 对照白名单。

## 关键链接
- 设计：[`../designs/metastore-storage-property-refactor-design-2026-06-17.md`](../designs/metastore-storage-property-refactor-design-2026-06-17.md)
- 流程：[`WORKFLOW.md`](./WORKFLOW.md) ｜ 任务：[`tasks.md`](./tasks.md) ｜ 决策：[`decisions-log.md`](./decisions-log.md) ｜ 偏差：[`deviations-log.md`](./deviations-log.md) ｜ 风险：[`risks.md`](./risks.md)
- 对抗 review（FU-T01）：workflow `wf_5db99e32-2ad`（27 agent，4 lens + verify；3 实质修 + F1 接线）｜recon：`wf_de5f54be-668`（4-agent：legacy parity / fe-filesystem-hdfs / api+s3 / kerberos）
