# HANDOFF — Session 间接力（每完成一个阶段/任务即更新并 commit）

> **下次 agent 接手流程（强制，用户 2026-06-17 立规）**：
> 1. 先读 `PROGRESS.md` → 本文件 → `WORKFLOW.md` → 下一 task 在 `tasks.md` 的对应块 → `decisions-log.md`/`deviations-log.md` 相关条。
> 2. **对照真实代码 review 下一步方案**（不照搬本文件里的旧计划——代码可能已变；先 grep/读真实调用流，确认方案仍成立）。
> 3. 一句话复述确认 + 必要时 AskUserQuestion 定边界 → 开始实施（严格按 `WORKFLOW.md §2` 单任务 TDD 循环）。

---

**更新时间**：2026-06-17（实现 session：P1-T03 完成）
**更新人**：Claude（Opus 4.8）

## 这次 session 完成了什么
1. **P1-T03 ✅**（commit `[P1-T03]`，连接器侧首个 task）：paimon `PaimonCatalogFactory.applyStorageConfig` 从 fe-property `StorageProperties.buildObjectStorageHadoopConfig(props)` 改为吃**预算好的** `storageHadoopConfig` 入参；保留其后 `paimon.*/fs./dfs./hadoop.` 覆盖块（last-write-wins）。`PaimonConnector` 新增 `buildStorageHadoopConfig()`，遍历 `ctx.getStorageProperties()` 调 `toHadoopProperties().toHadoopConfigurationMap()` 合并，传入 3 个 `buildXxx`（REST 不用）。pom 加 `fe-filesystem-api` 直接依赖（fe-property 依赖**留** P1-T05 删）。
2. **TDD**：neuter `storageHadoopConfig.forEach(setter)` → 3 个 Applies/Overlays 测试 RED（`expected <ak/oak> was <null>`）→ 恢复 → GREEN。
3. **测试改造**：删 ~23 canonical-translation 测试（S3/OSS/COS/OBS/MinIO 翻译=fe-filesystem 职责）+ adapt 保留测试 + 新增 6 契约测试（storage map 落 conf×3 builder + explicit-fs.s3a-overrides-storage + paimon-prefix-overrides-storage + kerberos-survives-storage-overlay）。
4. **验证**：paimon 全模块 **292/0/0/1skip**（docker-gated `PaimonLiveConnectivityTest`），`PaimonCatalogFactoryTest` 42/0，**checkstyle 0**、`tools/check-connector-imports.sh` PASS、`git diff --name-only` 白名单干净。
5. **对抗 review**（`wf_76df09a4-c2f`，8 agent，4 lens + verify）：1 BLOCKER+3 MAJOR+2 MINOR；verify 推翻假 BLOCKER（删 buildHmsHiveConf 重载=唯 paimon 调用方全已改）+2 MAJOR（endpoint-pattern/OSS-derivation 经直接核实 fe-filesystem 已覆盖）；**confirm 1 MAJOR=R-006**（调优默认 50/3000/1000、100/10000/10000 在 fe-filesystem **无显式 UT 守护**；**功能今日正确**=字段默认真发，仅测试健壮性缺口）→ 记 R-006 + 加 1 个 in-scope kerberos-storage 测试。
6. **决策/偏差**：**DV-003**（T1 落地=Option C：connector-local 契约 UT + docker P1-T06 兜底，因 fe-filesystem 对象存储 impl 是运行时插件不在任何单测 classpath；并 DV-003-b：fe-property import 已在 T03 删，P1-T05 退化为仅删 pom 边 + grep 闸）。回写：tasks P1-T03、deviations DV-003、risks R-001(修订)/R-006(新)、PROGRESS。

## 当前状态
- 阶段：Research ✅ / Design ✅（**9 决策 D-001..D-009**）/ **Implement 🚧（P1 3/6）**。
- 任务计数 **5/14**（P0: 2/2 ✅ ｜ P1: 3/6 ｜ P2: 0/5 ｜ P3a: 0/1）。
- **连接器 storage 配置路已切**：`CREATE CATALOG raw map` →（fe-core）`bindAll` → `ctx.getStorageProperties()`（fe-filesystem typed）→（连接器）`PaimonConnector.buildStorageHadoopConfig()` 合并 `toHadoopConfigurationMap()` → 3 个 `buildXxx` 叠加（+paimon.*/raw 覆盖 last-write-wins）。
- paimon main 已**零** `org.apache.doris.property` import（grep 归零）；`fe-property` pom 依赖仍在（变 0 import 消费者，P1-T05 删边）。
- ⚠️ **e2e/docker 未跑**（本 session 仅 compile + UT + 对抗 review）。

## 下一步（明确）：P1-T04（BE 静态凭据改走 toBackendProperties().toMap()）
> **务必先按顶部流程：读文档 + 对照真实代码 review 方案再动手。** 下面是已知方案，但须现场核实。

**目标**：paimon `PaimonScanPlanProvider` 的 BE 静态凭据从 `context.getBackendStorageProperties()`（`PaimonScanPlanProvider.java:606`）改为「遍历 `ctx.getStorageProperties()` 调 `toBackendProperties().orElseThrow().toMap()`」合并出 AWS_* map。**vended 动态路径不动**（仍 `ctx.vendStorageCredentials`，D-008；用户定 A=全量切静态、vended 不碰）。

**先做 recon（关键未知）**：
- 读 `PaimonScanPlanProvider.java:600-620` 现有 `getBackendStorageProperties()` 消费点（合并进 scan range location props 的循环）；确认 `ctx`（`context` 字段）在该方法可达（应可达，与 catalog 路同 ctx）。
- `StorageProperties.toBackendProperties()` 返回 `Optional<BackendStorageProperties>`，`.toMap()` 出 `AWS_*`。注意 fe-filesystem BE map 是**超集**（多 `AWS_BUCKET`/`AWS_ROOT_PATH`/`AWS_CREDENTIALS_PROVIDER_TYPE`，DV-002）——T1 钉常见路径全等 + 记超集（同 P1-T03 的 Option C：真等价 docker 兜底）。
- vended 叠加顺序：legacy vended REPLACE/overlay 静态（见 catalog-spi P5 FIX-1 记忆）；确认改后 vended 仍后叠（精确保序）。

**编译/测**：`mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-connector/fe-connector-paimon -am package -Dassembly.skipAssembly=true -Dmaven.build.cache.enabled=false`（看 `BUILD SUCCESS` 行）；`-Dtest=PaimonScanPlanProviderTest` 聚焦。checkstyle 0、import-gate PASS。

**之后**：P1-T05（删 paimon→fe-property **pom 依赖边** + `grep org.apache.doris.property` 归零闸；import 已在 T03 删，DV-003-b）→ P1-T06（paimon UT + docker `enablePaimonTest=true` 5 flavor，**真 T1 等价闸 + 验 R-006 调优默认**；不跑则标「未跑 e2e」）。

## 未决 / 需注意
- ✅ 已定：范围 P0+P1（到 P1-T06）｜机制 A（D-009）｜T1=Option C（DV-003）。
- ❗ **R-006（confirm，需用户定夺）**：fe-filesystem 对调优默认值（S3 50/3000/1000、OSS/COS/OBS 100/10000/10000）**无显式 UT 守护**（删 paimon canonical 测试暴露）。**功能今日正确**（字段默认真发），docker P1-T06 运行期兜底。**修法（超 P1 白名单，禁碰 fe-filesystem）**=在 `S3/Oss/Cos/ObsFileSystemPropertiesTest` 加 test-only 断言 → **建议作 follow-up / 经用户批准的小补丁**，不在 paimon 重复断言（Option C：paimon 无 fe-filesystem impl 于测试 classpath，合成 map 断言同义反复）。**下次 session 可向用户确认是否纳入。**
- ⚠️ e2e 全程未跑；P1-T06 前如不部署 docker，明确标「未跑 e2e」（CLAUDE.md Rule 12）。

## 红线提醒（WORKFLOW §4）
- **可动**（白名单）：`fe-connector-paimon/**`（P1-T04+ 改造）、`fe-connector-spi/**`（勿再扩）、fe-core **仅** `connector/DefaultConnectorContext.java` + `fs/FileSystemPluginManager.java` + `fs/FileSystemFactory.java`（均**仅新增方法**）、相关 pom（仅依赖增删）、本跟踪目录。
- **禁碰**：fe-core `datasource.property.{storage,metastore}` 包、构造点 `PluginDrivenExternalCatalog`、其它连接器（hive/hudi/iceberg/es/jdbc/mc/trino）、**fe-filesystem 各模块**（含其 test——R-006 的 fe-filesystem 断言须经用户批准才动）、`fe-property` 模块删除。
- paimon 连接器现**允许** import `org.apache.doris.filesystem.properties.*`（fe-filesystem-api，目标边）；**禁** `org.apache.doris.{property,catalog,common,datasource,qe,...}`（import-gate 守）。
- 每次提交前 `git diff --name-only` 对照白名单。

## 关键链接
- 设计：[`../designs/metastore-storage-property-refactor-design-2026-06-17.md`](../designs/metastore-storage-property-refactor-design-2026-06-17.md)
- 流程：[`WORKFLOW.md`](./WORKFLOW.md) ｜ 任务：[`tasks.md`](./tasks.md) ｜ 决策：[`decisions-log.md`](./decisions-log.md) ｜ 偏差：[`deviations-log.md`](./deviations-log.md) ｜ 风险：[`risks.md`](./risks.md)
- 对抗 review（P1-T03）：workflow `wf_76df09a4-c2f`（transcript 在 session subagents 目录）
