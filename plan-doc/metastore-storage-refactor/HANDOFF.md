# HANDOFF — Session 间接力（每完成一个阶段/任务即更新并 commit）

> **下次 agent 接手流程（强制，用户 2026-06-17 立规）**：
> 1. 先读 `PROGRESS.md` → 本文件 → `WORKFLOW.md` → 下一 task 在 `tasks.md` 的对应块 → `decisions-log.md`/`deviations-log.md` 相关条。
> 2. **对照真实代码 review 下一步方案**（不照搬本文件里的旧计划——代码可能已变；先 grep/读真实调用流，确认方案仍成立）。
> 3. 一句话复述确认 + 必要时 AskUserQuestion 定边界 → 开始实施（严格按 `WORKFLOW.md §2` 单任务 TDD 循环）。

---

**更新时间**：2026-06-17（实现 session：P1-T04 + **P1-T05** 完成）
**更新人**：Claude（Opus 4.8）

## 这次 session 完成了什么（P1-T04 + P1-T05）

**P1-T05 ✅（最新）**：删 `fe-connector-paimon/pom.xml` 的 `fe-property` 依赖块（**仅删 pom 边**——import/call 已在 P1-T03 清，DV-003-b；fe-property 模块本身不删 D-005 → 变 0 消费者孤儿）。recon 证 paimon src（main+test）`org.apache.doris.property` 已 **ZERO**、唯一物理耦合 = pom :72，其余 `fe-property` 字样皆历史注释（不动，surgical）。**RED/GREEN = 构建闸**（无 UT 可写）：删后全模块编译 + 全 UT 仍绿 = 证无隐藏 transitive 断裂。验证：paimon 全模块 **293/0/0/1skip**（含 P1-T04 新增 1 测试）、grep 归零、pom 无 fe-property、checkstyle 0、import-gate PASS、白名单干净（仅 pom 1 文件）。**P1 storage 收口的依赖边正式断开**（trivial 机械改,未跑对抗 review）。

---

**P1-T04 ✅（本 session 较早）**：
1. **paimon BE 静态凭据切 typed 路**：`PaimonScanPlanProvider.getScanNodeProperties()` 的 BE 静态凭据块从 `ctx.getBackendStorageProperties()` 改为遍历 `ctx.getStorageProperties()` 调 `sp.toBackendProperties().ifPresent(b → backendStorageProps.putAll(b.toMap()))` → 发 `location.<key>`（镜像 P1-T03 `.ifPresent` 风格）。**vended 块（`ctx.vendStorageCredentials`）不动、仍叠在静态块之后**→vended overlays static 保序。加 `org.apache.doris.filesystem.properties.StorageProperties` import；pom 无需改（fe-filesystem-api 依赖 P1-T03 已加）。**仅改 `PaimonScanPlanProvider.java` 1 主文件 + 其测试**。
2. **关键 recon 发现（DV-002 未覆盖）+ 用户定向**：新 typed 路对 **HDFS 物理上产不出 BE 键**（fe-filesystem **无 HDFS typed BE model**：`HdfsFileSystemProvider` 未 override `bind()`→默认抛 `UnsupportedOperationException`→`FileSystemPluginManager.bindAll` catch 跳过→`getStorageProperties()` 对 HDFS catalog 返回空）。legacy `getBackendStorageProperties()`（fe-core `HdfsProperties.getBackendConfigProperties`）发的 HDFS `hadoop/dfs/HA/kerberos` 键经 `PluginDrivenScanNode→FileQueryScanNode.setLocationPropertiesIfNecessary→HdfsResource.generateHdfsParam→THdfsParams` 是 **load-bearing**→全量切会丢→HDFS paimon 原生读回归。又：`getBackendStorageProperties()` 是 **ConnectorContext 方法、不依赖 fe-property**→**P1-T05 并不需要本切换**,切换纯为 D-003 统一。**用户 2026-06-17 定：按原计划全量切 + 接受 HDFS BE 回归 + follow-up 补 fe-filesystem `HdfsFileSystemProperties`**（记 **DV-004 / R-007 / FU-T01**）。
3. **TDD**：`scanContext` helper 改喂 `getStorageProperties()` 的 fake `StorageProperties`（删 `getBackendStorageProperties` override）→ `getScanNodePropertiesNormalizesStaticCreds` RED（`expected ak was null`）→ 切产线 GREEN。
4. **对抗 review confirm 修**：新增 1 测试 `...SkipsStoragePropsWithoutBackendMappingAndMergesRest`（混 `Optional.empty()`-无-BE 项[HDFS-like] + 真对象存储项 → 钉 `.ifPresent` 跳过 + 多 entry `putAll` merge；mutation `.ifPresent→.get()`→RED）+ 2 helper（`scanContextWithStorage`/`fakeStorageWithoutBackend`）。
5. **验证**：`PaimonScanPlanProviderTest` **52/0**、paimon 全模块 **292/0/0/1skip**、**checkstyle 0**、`tools/check-connector-imports.sh` PASS、`git diff --name-only` 白名单干净（2 文件）、零 `org.apache.doris.property/datasource` import。
6. **对抗 review**（`wf_09745716-d48`，10 agent，3 lens + verify）：7 finding confirm 4。**confirm 1 MAJOR=R-008**（fe-filesystem `Oss/Cos/ObsFileSystemProperties.toBackendKv()` 缺 `AWS_CREDENTIALS_PROVIDER_TYPE`,legacy 对无凭据 OSS/COS/OBS 发 `ANONYMOUS`；S3 typed 有；**fix 在 fe-filesystem 超 P1 白名单**→记 R-008 + **FU-T02**；仅影响无 ak/sk 的 OSS/COS/OBS,带 IAM-role 主机会误取 instance 凭据,公开桶仍 anonymous 非硬失败）+ **3 test-gap 已修**（上条测试）。verify **推翻 3 假 finding**：AWS_BUCKET/ROOT_PATH 超集=DV-002 已接受非回归;「测试没钉新 seam」被**实测 mutation 推翻**（回退旧 seam→RED）;OverlaysVended 静态缺失由 sibling NormalizesStaticCreds 覆盖。

## 当前状态
- 阶段：Research ✅ / Design ✅（**9 决策 D-001..D-009**）/ **Implement 🚧（P1 5/6，仅剩 P1-T06 验证）**。
- 任务计数 **7/14**（P0: 2/2 ✅ ｜ P1: 5/6 ｜ P2: 0/5 ｜ P3a: 0/1）｜ follow-up 占位 P3b/FU-T01/FU-T02。
- **连接器 storage + BE 凭据路全切 fe-filesystem-api typed，且 paimon→fe-property 依赖边已断**：catalog 配置 `PaimonConnector.buildStorageHadoopConfig()→toHadoopConfigurationMap()`（P1-T03）；BE 扫描分片 `PaimonScanPlanProvider`→`getStorageProperties().toBackendProperties().toMap()`→`location.*`（P1-T04，vended overlays static 不动）。
- paimon 已**零** `org.apache.doris.property/datasource` import **+ pom 无 fe-property 依赖**（P1-T05）；fe-property 变 0 消费者孤儿（本次不物理删，D-005）。
- ⚠️ **已知接受回归（fe-filesystem typed BE model 不全,fix 超 P1 白名单)**：①HDFS-warehouse paimon BE 配置丢（DV-004/R-007/FU-T01）；②无凭据 OSS/COS/OBS 缺 `AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS`（R-008/FU-T02）。用户接受、follow-up 修、docker P1-T06 会暴露（**非新 bug**）。
- ⚠️ **e2e/docker 未跑**（本 session 仅 compile + UT + 对抗 review）。

## 下一步（明确）：P1-T06（P1 验证收口）
> **务必先按顶部流程：读文档 + 对照真实代码 review 方案再动手。** 下面是已知方案，但须现场核实。

**目标**：P1 收口验证。(1) paimon UT 全绿（**已 293/0/1skip**）；(2) docker `enablePaimonTest=true` 跑 paimon **5 flavor**（filesystem/hms/rest/jdbc/dlf）+ vended(REST/DLF) + Kerberos HMS；(3) **真 T1 等价闸 Option C**——docker 真 fe-filesystem 对象存储 impl 在 classpath，端到端读私有对象存储桶验 storage 配置/凭据真发（UT 因 impl 是运行时插件无法验，故 docker 兜底）。

**重点验已知边界（本 session 引入，须区分「已知接受」vs「真新回归」）**：
- **R-007（HDFS 回归，已接受）**：HDFS-warehouse paimon flavor 的 BE 原生读会因缺 `THdfsParams` 失败/降级（fe-filesystem 无 HDFS typed BE model，DV-004/FU-T01）。docker HDFS flavor **应暴露此回归**——确认它就是 R-007、**非新 bug**（勿误判为本次破坏）。若 docker filesystem flavor 用 HDFS warehouse → 预期失败；用 local/对象存储 → 应正常。
- **R-008（无凭据 OSS/COS/OBS ANONYMOUS 漂移，已接受）**：仅无 ak/sk 的 OSS/COS/OBS catalog 受影响（FU-T02）；带 ak/sk 的不受影响。
- **R-006（调优默认无 UT 守护）**：docker 运行期兜底确认 S3 50/3000/1000、OSS/COS/OBS 100/10000/10000 真发。

**先做 recon（关键未知）**：读 docker 编排 + regression suite 的 paimon flavor 覆盖（`enablePaimonTest` gate 位置、5 flavor 如何起、各 flavor 用何 warehouse 后端=对象存储 or HDFS）；确认哪些 flavor 真跑对象存储（验 P1-T03/T04 storage 路）、哪些跑 HDFS（触发 R-007）。可用技能 `doris-docker-regression` / `regression_doris`。

**若本环境不部署 docker → 明确标「未跑 e2e」**（CLAUDE.md Rule 12），不得把「UT 过 + 编译过」当「e2e 验过」。P1-T06 不跑 docker 即不能算真正收口,需在 PROGRESS/HANDOFF 标注待补。

**之后**：P1 收口 → P2（metastore SPI：P2-T01 新建 fe-connector-metastore-api …）；follow-up FU-T01/FU-T02/R-006 与 fe-filesystem 收口批次同做。

## 未决 / 需注意
- ✅ 已定：范围 P0+P1（到 P1-T06）｜机制 A（D-009）｜T1=Option C（DV-003）｜P1-T04 全量切 + 接受 fe-filesystem typed BE model 缺口回归（用户 2026-06-17）。
- ❗ **R-007（已触发，用户接受）**：HDFS-warehouse paimon BE 配置丢（fe-filesystem 无 HDFS typed BE model）→ HDFS（尤 HA/kerberized）原生读回归。**follow-up FU-T01** 补 fe-filesystem `HdfsFileSystemProperties`（超白名单）。docker P1-T06 HDFS flavor 会暴露——**已知、非新 bug**。
- ❗ **R-008（已触发，用户接受类）**：fe-filesystem typed OSS/COS/OBS 缺 `AWS_CREDENTIALS_PROVIDER_TYPE`（无凭据 catalog 的 legacy `ANONYMOUS` 丢）→ 带 IAM-role 云主机误取 instance 凭据（公开桶仍 anonymous 非硬失败）。**follow-up FU-T02**（超白名单）。仅影响无 ak/sk 的 OSS/COS/OBS。
- ❗ **R-006（confirm，需用户定夺）**：fe-filesystem 对调优默认值（S3 50/3000/1000、OSS/COS/OBS 100/10000/10000）**无显式 UT 守护**。**功能今日正确**，docker P1-T06 兜底。修法（超 P1 白名单）=在 `S3/Oss/Cos/ObsFileSystemPropertiesTest` 加 test-only 断言，作 follow-up。
- 📌 **R-006/R-007/R-008 + FU-T01/FU-T02 同源**：均「fe-filesystem typed storage 模型对 legacy 不完整（BE model 缺 HDFS、缺 OSS/COS/OBS provider-type、缺调优默认断言），fix 全在 fe-filesystem（超 P1 白名单）」→ 宜与 fe-filesystem 收口/迁移批次或经用户批准的小补丁同做。**下次 session 可向用户确认是否纳入。**
- ⚠️ e2e 全程未跑；P1-T06 前如不部署 docker，明确标「未跑 e2e」（CLAUDE.md Rule 12）。

## 红线提醒（WORKFLOW §4）
- **可动**（白名单）：`fe-connector-paimon/**`（P1-T04+ 改造）、`fe-connector-spi/**`（勿再扩）、fe-core **仅** `connector/DefaultConnectorContext.java` + `fs/FileSystemPluginManager.java` + `fs/FileSystemFactory.java`（均**仅新增方法**）、相关 pom（仅依赖增删）、本跟踪目录。
- **禁碰**：fe-core `datasource.property.{storage,metastore}` 包、构造点 `PluginDrivenExternalCatalog`、其它连接器（hive/hudi/iceberg/es/jdbc/mc/trino）、**fe-filesystem 各模块**（含其 test——R-006 的 fe-filesystem 断言须经用户批准才动）、`fe-property` 模块删除。
- paimon 连接器现**允许** import `org.apache.doris.filesystem.properties.*`（fe-filesystem-api，目标边）；**禁** `org.apache.doris.{property,catalog,common,datasource,qe,...}`（import-gate 守）。
- 每次提交前 `git diff --name-only` 对照白名单。

## 关键链接
- 设计：[`../designs/metastore-storage-property-refactor-design-2026-06-17.md`](../designs/metastore-storage-property-refactor-design-2026-06-17.md)
- 流程：[`WORKFLOW.md`](./WORKFLOW.md) ｜ 任务：[`tasks.md`](./tasks.md) ｜ 决策：[`decisions-log.md`](./decisions-log.md) ｜ 偏差：[`deviations-log.md`](./deviations-log.md) ｜ 风险：[`risks.md`](./risks.md)
- 对抗 review（P1-T04）：workflow `wf_09745716-d48`（10 agent，confirm R-008 + 3 test-gap；transcript 在 session subagents 目录）｜P1-T03：`wf_76df09a4-c2f`
