# PROGRESS — 属性体系重构（paimon 优先）

> 人类 + agent 入口。每完成 task / 阶段切换 / 重要变更后更新。上次更新：**2026-06-17**。

---

## 总体状态

| 阶段 | 进度 | 状态 |
|---|---|---|
| Research（调研） | ██████████ 100% | ✅ 完成（8-agent + grep；+ 3-agent recon 复核 D-006/7/8） |
| Design（设计） | ██████████ 100% | ✅ 完成（设计文档 + **7 决策** D-001..D-008，范围已收窄） |
| **Implement（实现）** | ███████░░░ ~50% | 🚧 **进行中**（范围 P0+P1 已获批；P0 ✅；P1 5/6，storage+BE 凭据切 fe-filesystem-api typed + paimon→fe-property 依赖边已断） |

任务计数：**7 / 14** 完成（P0: 2/2 ✅ ｜ P1: 5/6 ｜ P2: 0/5 ｜ **P3a: 0/1**）｜ + **P3b / FU-T01 / FU-T02**（follow-up，范围外占位）。仅剩 **P1-T06**（验证）即 P1 收口。

---

## 当前活跃 task
- **FU-T01 ✅ 完成（2026-06-17，D-010 授权，commit 待提交）**：给 `fe-filesystem-hdfs` 新建 HDFS typed BE model（`HdfsFileSystemProperties` + `HdfsConfigFileLoader` + provider bind），修复 P1-T04 的 HDFS BE 回归（DV-004/**R-007 闭环**）。移植源 = fe-property `HdfsProperties`（parity by construction）；kerberos=K1（BE-key 字符串、不建 fe-kerberos）；BE-only（不实现 HadoopStorageProperties）。验证：fe-filesystem-hdfs 全模块 **78/0**（含 25 golden parity；既有 create() 路零回归）+ checkstyle 0 + RED/GREEN(mutation) + fe-core 编译绿。**对抗 review `wf_5db99e32-2ad`（27 agent）**清场，3 实质修（malformed-uri fail-loud + 2 stale 注释 + 11 测试）+ **F1 接线**（config-dir sysprop 桥 `Config.hadoop_config_dir`，用户选「现在接好」）。⚠️ docker e2e 未跑。
- **下一个：`P1-T06`**（P1 验证收口）：paimon UT 全绿（已 293/0/1skip）+ docker `enablePaimonTest=true` 5 flavor（filesystem/hms/rest/jdbc/dlf）+ vended(REST/DLF) + Kerberos HMS；**真 T1 等价闸 Option C**；**FU-T01 已补 → HDFS flavor 应通过（R-007 已闭环）**。**若不部署 docker → 明确标「未跑 e2e」**（CLAUDE.md Rule 12）。
- P0-T01 ✅｜P0-T02 ✅（bindAll）｜P1-T01 ✅（getStorageProperties 默认方法 + 边）｜P1-T02 ✅（getStorageProperties 实现 + FileSystemFactory accessor）｜P1-T03 ✅（paimon storage 配置 `applyStorageConfig` 改走 `toHadoopConfigurationMap()`）｜P1-T04 ✅（paimon BE 静态凭据改走 `getStorageProperties().toBackendProperties().toMap()`，全量切）｜**P1-T05 ✅**（删 paimon→fe-property pom 依赖边 + grep 归零闸）。
- ✅ **连接器 storage + BE 凭据路全切 fe-filesystem-api typed，且 paimon→fe-property 依赖边已断**：catalog 路 `PaimonConnector.buildStorageHadoopConfig()→toHadoopConfigurationMap()`；BE 扫描分片路 `PaimonScanPlanProvider` 遍历 `getStorageProperties()→toBackendProperties().toMap()`→`location.*`（vended overlays static 保序不动）。paimon 已零 `org.apache.doris.property/datasource` import + pom 无 fe-property 依赖（fe-property 变 0 消费者孤儿,本次不物理删 D-005）。
- ⚠️ **已知接受回归（fe-filesystem typed BE model 不全,超 P1 白名单)**：HDFS-warehouse paimon BE 配置丢（DV-004/R-007/FU-T01）；无凭据 OSS/COS/OBS 缺 `AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS`（R-008/FU-T02）。均用户接受、follow-up 修、docker P1-T06 会暴露（非新 bug）。
- ▶ **下一步**：P1-T06（docker 5-flavor 真等价闸 Option C；验 R-006 调优默认 + R-007/R-008 已知回归边界）→ P1 收口 → 后续 P2（metastore SPI）。

## 阻塞 / 待决
- ✅ 范围已获批（2026-06-17）= **P0+P1（storage 收口），做到 P1-T06 gate 停**。
- ✅ **DV-001/D-009（2026-06-17）**：P0-T01 recon 证伪「fe-filesystem-api 已够、唯一 fe-core 改动」——产出 fe-filesystem typed StorageProperties 须新增 bind-all（仓内不存在）。用户定 **机制 A**：fe-core `FileSystemPluginManager` 加 additive `bindAll`，`getStorageProperties()` 经 `getOrigProps()` 取 raw map、不碰构造点。**fe-core 改动 = 2 文件**（DefaultConnectorContext + FileSystemPluginManager，均纯新增），白名单已 +1。
- ⚠️ **R-001 等价性**：fe-filesystem 为新事实源，较 fe-property 略**超集**（S3 role/anon；OSS/COS/OBS endpoint 无条件）；T1 须钉常见路径全等 + 记超集差异。

---

## 最近动态（最近 7 天）
- 2026-06-17 **FU-T01 ✅**（D-010 授权，HDFS typed BE model 修 DV-004/R-007）：新建 `fe-filesystem-hdfs` 的 `HdfsFileSystemProperties`（BE-only，忠实移植 legacy `initBackendConfigProperties`）+ `HdfsConfigFileLoader`（XML 资源）+ provider `bind()`/`create(P)`（`create(Map)`/`supports()` 不动）+ pom `fe-foundation`/`commons-lang3`。kerberos=**K1**（BE-key 字符串内联，不建 fe-kerberos，不碰 create()-side authenticator；用户 AskUserQuestion 选）。**真 parity 在 UT 落地**（非 paimon Option C）：25 golden parity 钉 `toMap()`==legacy BE 键集（simple/kerberos/HA/username/uri-derive/XML/sysprop…）。验证 fe-filesystem-hdfs **78/0** + checkstyle 0 + RED/GREEN(mutation 关 kerberos 块→红) + fe-core `-am compile` 绿 + `git diff` 白名单干净。**对抗 review `wf_5db99e32-2ad`（27 agent，4 lens+verify）**：清场（packaging 无跨 loader、parity byte-level 复核、BE-only 无新 catalog 路回归、强 oss-hdfs 断言被 verify 推翻），3 实质修（①malformed-uri swallow→fail-loud 对齐 legacy；②2 处 stale 注释[bindAll javadoc/paimon KNOWN GAP 1]；③+11 测试）。**F1**（XML config-dir 未接 `Config.hadoop_config_dir`）用户选「**现在接好**」=fe-core `FileSystemFactory` setProperty 桥（leaf 读 sysprop）。**额外触碰 3 已白名单文件**（FileSystemFactory/FileSystemPluginManager/PaimonScanPlanProvider，均 project-owned 微改/注释）。残留 oss-hdfs JindoFS 凭据=独立 FU。⚠️ docker e2e 未跑（HA/kerberized 真闸 P1-T06）。
- 2026-06-17 **P1-T05 ✅**（断开 paimon→fe-property 依赖边）：删 `fe-connector-paimon/pom.xml` 的 `fe-property` 依赖块（仅删 pom 边——import/call 已在 P1-T03 清 DV-003-b）。recon 确认 paimon src（main+test）`org.apache.doris.property` 已 ZERO、唯一物理耦合是 pom :72，其余 `fe-property` 字样皆历史注释（不动）。**RED/GREEN=构建闸**（无 UT 可写）：删后全模块编译+全 UT 仍绿=证无隐藏 transitive 断裂。验证：paimon 全模块 **293/0/0/1skip**、grep 归零、pom 无 fe-property、checkstyle 0、import-gate PASS、白名单干净（仅 pom）。**fe-property 变 0 消费者孤儿（本次不物理删,D-005）**。⚠️ docker e2e 未跑。仅剩 P1-T06 验证即 P1 收口。
- 2026-06-17 **P1-T04 ✅**（paimon `PaimonScanPlanProvider` BE 静态凭据全量切 `getStorageProperties().toBackendProperties().ifPresent(putAll(toMap()))`→`location.*`；vended 不动、叠后保序）：现场 recon 揪出 **DV-002 未覆盖的 HDFS 缺口**——fe-filesystem 无 HDFS typed BE model（`HdfsFileSystemProvider.bind` 抛→`bindAll` 跳过）,legacy `getBackendStorageProperties()` 经 fe-core 发的 HDFS `hadoop/dfs/HA/kerberos`→`THdfsParams` 是 load-bearing,全量切会丢→HDFS paimon 原生读回归;`getBackendStorageProperties()` 是 ConnectorContext 方法不依赖 fe-property→P1-T05 不需此切换,纯 D-003 统一。**用户定全量切 + 接受 HDFS 回归 + follow-up 补 HDFS typed BE 类**（DV-004/R-007/FU-T01）。TDD RED（`expected ak was null`）→GREEN;52/0 + 全模块 292/0/1skip + checkstyle 0 + import-gate PASS + 白名单干净（2 文件）。**对抗 review `wf_09745716-d48`**（10 agent）confirm 4：MAJOR=R-008（OSS/COS/OBS typed 缺 `AWS_CREDENTIALS_PROVIDER_TYPE` ANONYMOUS,fe-filesystem 超白名单→FU-T02,仅无凭据 catalog）+ 3 test-gap 已修（新增 Optional.empty 跳过 + 多 entry merge 测试）;推翻 3 假 finding（含实测 mutation 证「测试钉了新 seam」）。⚠️ docker e2e 未跑。
- 2026-06-17 **P1-T03 ✅**（commit `[P1-T03]`；连接器侧首个 task；paimon `applyStorageConfig` 改走 `ctx.getStorageProperties().toHadoopConfigurationMap()`）：recon 证 ctx 在 `PaimonConnector.createCatalog()` 可达 → `buildStorageHadoopConfig()` 合并下发；保留 paimon.*/raw 覆盖 last-write-wins。**T1 = Option C**（用户选；fe-filesystem 对象存储 impl 是运行时插件不在单测 classpath → paimon UT 只钉 connector-local 契约，真等价由 docker P1-T06 兜底；DV-003）。TDD RED（neuter forEach → 3 测红）→GREEN；删 ~23 canonical 测试（fe-filesystem 职责）+ 6 新契约测试；**292/0/0/1skip + checkstyle 0 + import-gate PASS + 白名单干净**。**对抗 review `wf_76df09a4-c2f`** 推翻假 1B+2M、confirm 1M=**R-006**（调优默认 50/3000/1000、100/10000/10000 fe-filesystem 无显式 UT 守护；功能正确，docker 兜底，fe-filesystem 加断言 follow-up 超白名单）。⚠️ docker e2e 未跑。
- 2026-06-17 **P1-T02 ✅**（`DefaultConnectorContext.getStorageProperties()` + `FileSystemFactory.bindAllStorageProperties`，D-009 二次确认 3 文件）：TDD 4 绿（factory 委托/fallback + ctx 空/全量绑定捕获 raw map）+ 回归 2 绿；checkstyle 0；raw map 经 `getOrigProps()` 取。**fe-core 侧管线打通**。
- 2026-06-17 **P1-T01 ✅**（`ConnectorContext.getStorageProperties()` 默认空列表 + `fe-connector-spi→fe-filesystem-api` pom 边）：TDD（RED assertNotNull→GREEN 1/1）+ checkstyle 0 + import-gate PASS；新建首个 fe-connector-spi 测试。
- 2026-06-17 **P0-T02 ✅**（`FileSystemPluginManager.bindAll`，D-009）：TDD（RED 5 错→GREEN 5 绿）+ checkstyle 0；纯新增 34 行不动既有方法。实证发现真对象存储 providers 是运行时目录插件（非 fe-core 单测 classpath）→ 删 real-S3 集成测试移交 P1-T06；并发现 P1-T02 须经 `FileSystemFactory` static accessor 取 live manager（第 3 fe-core 文件，待 AskUserQuestion）。
- 2026-06-17 **进入 Implement（范围 P0+P1 获批）**；**P0-T01 ✅**（4-agent recon 取证三套 StorageProperties + 连接器 seam）：(1) F1 等价性=非阻塞（fe-filesystem 与 paimon 现 fe-property 路常见静态凭据键全等、为超集）；(2) F2 可行性=阻塞（无 bind-all 入口，证伪白名单「唯一 fe-core 改动」）→ **DV-001**；用户定 **机制 A** → **D-009**（fe-core `FileSystemPluginManager.bindAll` + `getStorageProperties()` 经 `getOrigProps()`，白名单 +1）。已回写设计/WORKFLOW/decisions/risks/tasks。
- 2026-06-17 **3 设计点定稿（D-006/7/8）**（3-agent recon + 直读复核）：**D-006** MetaStore 后端用 `MetaStoreProvider.supports()` 自识别 + ServiceLoader（镜像 `FileSystemProvider`），api 层**去掉** `MetaStoreType` 枚举；**D-007** Kerberos 抽**顶层中立叶子 `fe-kerberos`**（否决 fe-connector-auth：破 fe-filesystem↛fe-connector gate + fe-common 层级倒挂），分 P3a（paimon-local）/P3b（全量去重 follow-up）；**D-008** vended 边界=连接器只抽取、fe-core 单点归一（现状已符合）。设计文档 §0/§2.3/§3.1/§3.2/§3.3/§3.5/依赖图已更新。
- 2026-06-17 调研完成（current state：paimon metastore 已与 fe-core 解耦、仅剩 storage 对 fe-property 一条边；三套同名 StorageProperties；fe-core metastore 28 文件 3624 LOC 矩阵；kerberos 三处实现）。
- 2026-06-17 设计定稿 + 4 决策（①新建 metastore-api/spi ②混合去重 ③fe-core 绑定下发 typed storage ④复用 @ConnectorProperty）。
- 2026-06-17 范围收窄（用户）：纯新增/迁移、只动 paimon、不删 fe-core 两包、不动其它连接器、fe-property 不物理删。
- 2026-06-17 建立本跟踪目录 + 开发流程（WORKFLOW.md）+ 任务清单（13 tasks）。

---

## 关键链接
- 设计：[`../designs/metastore-storage-property-refactor-design-2026-06-17.md`](../designs/metastore-storage-property-refactor-design-2026-06-17.md)
- 背景（fe-filesystem StorageProperties 现状评审）：[`../reviews/fe-filesystem-storage-spi-review-2026-06-17.md`](../reviews/fe-filesystem-storage-spi-review-2026-06-17.md)
- 流程：[`WORKFLOW.md`](./WORKFLOW.md) ｜ 任务：[`tasks.md`](./tasks.md) ｜ 决策：[`decisions-log.md`](./decisions-log.md)
