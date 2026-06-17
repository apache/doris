# HANDOFF — Session 间接力（每完成一个阶段/任务即更新并 commit）

> **下次 agent 接手流程（强制，用户 2026-06-17 立规）**：
> 1. 先读 `PROGRESS.md` → 本文件 → `WORKFLOW.md` → 下一 task 在 `tasks.md` 的对应块 → `decisions-log.md`/`deviations-log.md` 相关条。
> 2. **对照真实代码 review 下一步方案**（不照搬本文件里的旧计划——代码可能已变；先 grep/读真实调用流，确认方案仍成立）。
> 3. 一句话复述确认 + 必要时 AskUserQuestion 定边界 → 开始实施（严格按 `WORKFLOW.md §2` 单任务 TDD 循环）。

---

**更新时间**：2026-06-17（实现 session：P0 全部 + P1-T01/T02）
**更新人**：Claude（Opus 4.8）

## 这次 session 完成了什么
1. **进入 Implement**，用户批准范围 = **P0 + P1（storage 收口），做到 P1-T06 gate 停**。
2. **完成 P0（2/2）+ P1-T01/T02**，共 **5 个 commit**（均 TDD + checkstyle 0 + 红线核对）：
   - `5bf6cee` **P0-T01**：4-agent recon 三套 StorageProperties + 连接器 seam → 结论 + 定向。
   - `0f50a13` **P0-T02**：`FileSystemPluginManager.bindAll(rawMap)`（raw map → List<fe-filesystem StorageProperties>，全量收集 supporting providers、skip 未迁移 bind 的 legacy）。TDD 5/5。
   - `ffd5466` **P1-T01**：`ConnectorContext.getStorageProperties()` 默认空列表 + `fe-connector-spi → fe-filesystem-api` pom 边。TDD 1/1，import-gate PASS。
   - `5520975` **P1-T02**：`DefaultConnectorContext.getStorageProperties()`（`getOrigProps()` 取 raw map → factory）+ `FileSystemFactory.bindAllStorageProperties()`（取 live plugin manager）。TDD 4/4 + 回归 2/2。
   - `4d190a7` **DV-002 决策记录**（T1 框架）。
3. **3 个决策定稿**（均用户拍板）：
   - **D-009**：bind-all 机制 A = fe-core `FileSystemPluginManager.bindAll` + `DefaultConnectorContext.getStorageProperties()` 经 `getOrigProps()`；二次确认 **3 个 fe-core 文件**（+`FileSystemFactory` static accessor 取 live manager，因对象存储 provider 是运行时目录插件）。
   - **DV-001**：P0-1「fe-filesystem-api 已够、唯一 fe-core 改动」预期被证伪（缺 bind-all 入口）。
   - **DV-002**：T1「全等」放宽为 **常见静态凭据路径全等 + 文档记超集**（fe-filesystem 是 fe-property 路的超集：S3 role/anon、OSS/COS/OBS endpoint 无条件、BE map 多 AWS_BUCKET/ROOT_PATH/CREDENTIALS_PROVIDER_TYPE）。
4. 同步回写：设计 §4 P0-1/P0-2 + §2.1 + §5 T1、WORKFLOW §4.1 白名单（+2 fe-core 文件）+ §5.2 T1、decisions D-009、deviations DV-001/DV-002、risks R-004（3 处）/R-001、tasks、PROGRESS。

## 当前状态
- 阶段：Research ✅ / Design ✅（**9 决策 D-001..D-009**）/ **Implement 🚧（P1 2/6）**。
- 任务计数 **4/14**（P0: 2/2 ✅ ｜ P1: 2/6 ｜ P2: 0/5 ｜ P3a: 0/1）。
- **fe-core/spi 侧管线已通且全绿**：`CatalogProperty.getProperties()`(raw) →（`DefaultConnectorContext.getStorageProperties()` 内）任一 typed 值 `getOrigProps()` → `FileSystemFactory.bindAllStorageProperties()` → live `FileSystemPluginManager.bindAll()` → `List<fe-filesystem StorageProperties>` → `ConnectorContext.getStorageProperties()`（连接器待 P1-T03 消费）。
- fe-core 改动 = **3 文件全 additive**：`DefaultConnectorContext`(+getStorageProperties)、`fs/FileSystemPluginManager`(+bindAll)、`fs/FileSystemFactory`(+bindAllStorageProperties)。
- ⚠️ **e2e/docker 未跑**（本 session 仅 compile + 单测）。

## 下一步（明确）：P1-T03（连接器侧首个 task）
> **务必先按顶部流程：读文档 + 对照真实代码 review 方案再动手。** 下面是已知方案，但 paimon 连接器调用流须现场核实。

**目标**：paimon `PaimonCatalogFactory.applyStorageConfig` 改走 `ctx.getStorageProperties()` 的 `toHadoopProperties().toHadoopConfigurationMap()`，取代 `fe-property StorageProperties.buildObjectStorageHadoopConfig(props)`；**保留**其后 `paimon.*/fs./dfs./hadoop.` 覆盖块（保序 last-write-wins，有历史 bug 注释为证）。

**先做 recon（关键未知）**：
- `PaimonCatalogFactory` 是**纯静态 util**（无 ctx 字段，只吃 raw `Map props`）；`applyStorageConfig(props, setter)` 现调静态 fe-property 方法。3 调用方：`buildHadoopConfiguration`(:367)、`buildHmsHiveConf`(:478)、`buildDlfHiveConf`(:589)。
- **须查清**：连接器在哪里调这些 `PaimonCatalogFactory.buildXxx`？那里 `ConnectorContext`（→`getStorageProperties()`）/`List<StorageProperties>` 能否拿到？→ 把 storage list 线程进 `applyStorageConfig`（签名重构），或在调用点先算好 hadoop map 传入。grep `PaimonCatalogFactory.` 找调用方（大概率在 `PaimonConnector`/catalog 创建路径，且在 `ctx.executeAuthenticated` 内）。
- 注意 fe-property 现路是 object-store-only（HDFS 不贡献，靠 overlay 的 raw passthrough）；fe-filesystem 同理（bindAll skip HDFS）。两边对齐。

**T1 等价性测试（R-001 闸，DV-002 框架）**：fe-filesystem `toHadoopConfigurationMap()` 产物 vs fe-property `buildObjectStorageHadoopConfig` 现产物，在**常见静态凭据路径**（S3/OSS/COS/OBS 配齐 endpoint/region/AK/SK、无 role、无 vended）下 key/value **全等**；超集差异（S3 role/anon、endpoint 无条件、BE 多键）写注释记录，不当漂移。

**编译/测**：paimon 模块需 `mvn ... -am package -Dassembly.skipAssembly=true`（shade jar 携带 HiveConf）；`-Dmaven.build.cache.enabled=false` 确保 surefire 真跑；后台 task 看 `BUILD SUCCESS/FAILURE` 行非 echo exit code。

**之后**：P1-T04（BE 凭据切 `getStorageProperties().toBackendProperties().toMap()`，用户定 A=全量切，vended 路不动）→ P1-T05（删 paimon→fe-property pom 依赖 + import；`grep org.apache.doris.property` 归零，**不删 fe-property 模块**）→ P1-T06（UT + T1 + docker 5 flavor，不跑则标「未跑 e2e」）。

## 未决 / 需注意
- ✅ 已定：范围 P0+P1（到 P1-T06）｜机制 A（D-009，3 fe-core 文件）｜T1 框架 A（DV-002）。
- ❓ P1-T03 唯一现场未知 = **连接器调 `PaimonCatalogFactory.buildXxx` 处 ctx/storageList 的可达性**（决定签名重构形态）——recon 后若发现需改连接器其它文件或有阻碍，停下 AskUserQuestion。
- ⚠️ e2e 全程未跑；P1-T06 前如不部署 docker，明确标「未跑 e2e」（CLAUDE.md Rule 12）。

## 红线提醒（WORKFLOW §4，本 session 已扩张 2 次）
- **可动**（白名单）：`fe-connector-paimon/**`（P1-T03+ 改造）、`fe-connector-spi/**`（已加 getStorageProperties，勿再扩）、fe-core **仅** `connector/DefaultConnectorContext.java` + `fs/FileSystemPluginManager.java` + `fs/FileSystemFactory.java`（均**仅新增方法**，勿碰既有方法）、相关 pom（仅依赖增删）、本跟踪目录。
- **禁碰**：fe-core `datasource.property.{storage,metastore}` 包、构造点 `PluginDrivenExternalCatalog`、其它连接器（hive/hudi/iceberg/es/jdbc/mc/trino）、fe-filesystem 各模块、`fe-property` 模块删除。
- 每次提交前 `git diff --name-only` 对照白名单；3 个 fe-core 文件 `git diff` 须只见新增。

## 关键链接
- 设计：[`../designs/metastore-storage-property-refactor-design-2026-06-17.md`](../designs/metastore-storage-property-refactor-design-2026-06-17.md)
- 流程：[`WORKFLOW.md`](./WORKFLOW.md) ｜ 任务：[`tasks.md`](./tasks.md) ｜ 决策：[`decisions-log.md`](./decisions-log.md) ｜ 偏差：[`deviations-log.md`](./deviations-log.md) ｜ 风险：[`risks.md`](./risks.md)
