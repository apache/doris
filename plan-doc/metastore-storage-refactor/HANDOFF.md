# HANDOFF — Session 间接力（每完成一个阶段/任务即更新并 commit）

> **下次 agent 接手流程（强制，用户 2026-06-17 立规）**：
> 1. 先读 `PROGRESS.md` → 本文件 → `WORKFLOW.md` → 下一 task 在 `tasks.md` 的对应块 → `decisions-log.md`/`deviations-log.md` 相关条。
> 2. **对照真实代码 review 下一步方案**（不照搬本文件里的旧计划——代码可能已变；先 grep/读真实调用流，确认方案仍成立）。
> 3. 一句话复述确认 + 必要时 AskUserQuestion 定边界 → 开始实施（严格按 `WORKFLOW.md §2` 单任务 TDD 循环）。

---

**更新时间**：2026-06-17（实现 session：**FU-T01 完成** — HDFS typed BE model，R-007 闭环）
**更新人**：Claude（Opus 4.8）

## 这次 session 完成了什么（FU-T01）

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

## 当前状态
- 阶段：Research ✅ / Design ✅（**10 决策 D-001..D-010**）/ **Implement 🚧（P1 5/6 + FU-T01 ✅，仅剩 P1-T06 验证）**。
- 任务计数 **8/14**（P0: 2/2 ✅ ｜ P1: 5/6 ｜ **FU-T01 ✅** ｜ P2: 0/5 ｜ P3a: 0/1）｜ follow-up 占位 FU-T02 + P3b。
- **R-007 已闭环**（HDFS typed BE model 落地）。typed BE 路现对 S3/OSS/COS/OBS/**HDFS** 全产 BE 键。
- ⚠️ **e2e/docker 未跑**（本 session 仅 compile + UT + 对抗 review）。

## 下一步（明确）：P1-T06（P1 验证收口）
> **务必先按顶部流程：读文档 + 对照真实代码 review 方案再动手。**

**P1-T06**：paimon UT 全绿（已 293/0/1skip）+ docker `enablePaimonTest=true` **5 flavor**（filesystem/hms/rest/jdbc/dlf）+ vended(REST/DLF) + Kerberos HMS + **真 T1 等价闸 Option C**。
- **FU-T01 已补 → HDFS-warehouse flavor（含 HA / kerberized HMS+HDFS）应转通过**（R-007 闭环验证点；之前是已接受回归）。
- R-008（无凭据 OSS/COS/OBS 缺 `AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS`）docker 会暴露 → **仍 FU-T02 独立**（超白名单）。
- R-006（fe-filesystem 调优默认无 UT 守护）docker 兜底。
- **不部署 docker 则明确标「未跑 e2e」**（CLAUDE.md Rule 12）。
- 之后 P2（metastore SPI：P2-T01 新建 fe-connector-metastore-api …）+ P3a（fe-kerberos 叶子）。

## 未决 / 需注意
- ✅ 已闭环：R-007（FU-T01）。
- ❗ **R-008（已触发，用户接受类）**：fe-filesystem typed OSS/COS/OBS 缺 `AWS_CREDENTIALS_PROVIDER_TYPE`（无凭据 catalog 的 legacy `ANONYMOUS` 丢）。**follow-up FU-T02**（超白名单——`fe-filesystem-{oss,cos,obs}`）。
- ❗ **R-006（confirm，需用户定夺）**：fe-filesystem 调优默认值无显式 UT 守护；docker P1-T06 兜底；fe-filesystem 加断言 follow-up（超白名单）。
- 📌 **残留已知（非 FU-T01 引入，独立 FU）**：**oss-hdfs**（`oss://` warehouse + JindoFS）在 typed 路缺 oss 凭据键——P1-T04 已起（HDFS-family typed 缺口），彻底修需 fe-filesystem **OssHdfs typed model**（独立大动作，超白名单）。FU-T01 让 HDFS provider 对 bare-`oss://` fs.defaultFS 发无凭据 HDFS 键（review F3 MINOR，latent 误配曝露，非 working catalog 回归）。
- 📌 **scan-time 重 validate**：`getStorageProperties()` 每次 scan 经 `bindAll`→`bind()`→`of().validate()`（无 memoization）——valid catalog 内禀 dormant；是 typed-路通性（P1-T02/D-009），非 FU-T01 专有。
- ⚠️ e2e 全程未跑；P1-T06 前如不部署 docker，明确标「未跑 e2e」（CLAUDE.md Rule 12）。

## 红线提醒（WORKFLOW §4）
- **可动**（白名单）：`fe-connector-paimon/**`、`fe-connector-spi/**`、fe-core **仅** `connector/DefaultConnectorContext.java` + `fs/FileSystemPluginManager.java` + `fs/FileSystemFactory.java`（均**仅新增方法 / 对本项目所加方法的微改+注释**）、**`fe-filesystem/fe-filesystem-hdfs/**`（D-010 授权，仅 FU-T01）**、相关 pom、本跟踪目录。
- **禁碰**：fe-core `datasource.property.{storage,metastore}` 包、构造点 `PluginDrivenExternalCatalog`、其它连接器（hive/hudi/iceberg/es/jdbc/mc/trino）、**其它 fe-filesystem 模块**（`-{api,spi,s3,oss,cos,obs,azure,broker,local}`，含其 test）、`fe-property` 模块删除。
- **FU-T01 额外触碰**（已记 D-010 + tasks，透明）：fe-core `FileSystemFactory.java`（F1 +1 行 setProperty，项目 P1-T02 加的方法）、`FileSystemPluginManager.java`（bindAll javadoc，项目 P0-T02 加的方法）、fe-connector-paimon `PaimonScanPlanProvider.java`（注释）——均 project-owned 微改/注释，非碰 pre-existing fe-core 方法。
- paimon 连接器 + fe-filesystem-hdfs **允许** import `org.apache.doris.foundation.*`（fe-foundation 叶子）、`org.apache.doris.filesystem.*`；**禁** import fe-core/fe-connector（fe-filesystem 侧 gate）。
- 每次提交前 `git diff --name-only` 对照白名单。

## 关键链接
- 设计：[`../designs/metastore-storage-property-refactor-design-2026-06-17.md`](../designs/metastore-storage-property-refactor-design-2026-06-17.md)
- 流程：[`WORKFLOW.md`](./WORKFLOW.md) ｜ 任务：[`tasks.md`](./tasks.md) ｜ 决策：[`decisions-log.md`](./decisions-log.md) ｜ 偏差：[`deviations-log.md`](./deviations-log.md) ｜ 风险：[`risks.md`](./risks.md)
- 对抗 review（FU-T01）：workflow `wf_5db99e32-2ad`（27 agent，4 lens + verify；3 实质修 + F1 接线）｜recon：`wf_de5f54be-668`（4-agent：legacy parity / fe-filesystem-hdfs / api+s3 / kerberos）
