# HANDOFF — Session 间接力（每次 session 结束覆盖）

> 下次 agent 接手：先读 `PROGRESS.md` → 本文件 → `WORKFLOW.md` → （如指定 task）`tasks.md` 对应块 → 一句话复述确认 → 用户确认后开始。

---

**更新时间**：2026-06-17（设计补充 session：D-006/7/8）
**更新人**：Claude（3 设计点定稿 session）

## 这次 session 完成了什么
1. 用户提的 **3 个设计讨论点**经 3-agent recon + 直读复核后定稿，记为 **D-006 / D-007 / D-008**：
   - **D-006**：MetaStore 后端用 `MetaStoreProvider.supports()` 自识别 + ServiceLoader（镜像 `FileSystemProvider`），`fe-connector-metastore-api` **不放** `MetaStoreType` 枚举；标识用 `String providerName()` + 能力方法。
   - **D-007**：Kerberos 抽**顶层中立叶子 `fe-kerberos`**（**否决** `fe-connector-auth`——会破 `fe-filesystem↛fe-connector` gate + fe-common 层级倒挂）。**P3a（paimon-local）纳入本次**、**P3b（全量去重）= follow-up 范围外**（均用户 2026-06-17 确认）；模块名定 **`fe-kerberos`**。
   - **D-008**：vended creds 边界=连接器只「抽取」（paimon 已落地 `extractVendedToken`）、fe-core 单点「归一」（`vendStorageCredentials`）——**现状已符合**，无新增 task。
2. 同步更新：设计文档（§0 表 + 依赖图 + §2.3 + §3.1 + §3.2 + §3.3 + 新增 §3.5）、decisions-log（D-006/7/8）、tasks（P2-T01/T02 改写 + 新增 P3a/P3b）、PROGRESS。

## 当前状态
- 阶段：Research ✅ / Design ✅（**9 决策 D-001..D-009**）/ **Implement 🚧 进行中**。
- **范围已获批（2026-06-17 用户确认）= P0 + P1（storage 收口），做到 P1-T06 gate 停**。
- **P0-T01 ✅**（recon + 定向）→ **DV-001 / D-009**：缺 bind-all 入口，定机制 A（fe-core `FileSystemPluginManager.bindAll` + `getStorageProperties()` 经 `getOrigProps()`，白名单 +`FileSystemPluginManager.java`）。
- **P0-T02 ✅**（`FileSystemPluginManager.bindAll`）｜ **P1-T01 ✅**（`ConnectorContext.getStorageProperties()` 默认空 + 边）｜ **P1-T02 ✅**（`DefaultConnectorContext.getStorageProperties()` + `FileSystemFactory.bindAllStorageProperties`，D-009 二次确认 3 fe-core 文件全 additive，TDD 4 绿 + 2 回归绿）。
- **fe-core/spi 侧管线已通**：getOrigProps→bindAll(live manager)→ConnectorContext.getStorageProperties()。
- **下一个：P1-T03**（连接器侧首个 task）。T1 框架已定 **A（DV-002）**：认 fe-filesystem 新事实源，T1 = 常见静态凭据路径全等 + 文档记超集。实现要点：
  1. paimon `PaimonCatalogFactory.applyStorageConfig` 改走 `ctx.getStorageProperties()` 的 `toHadoopProperties().toHadoopConfigurationMap()`（取代 `fe-property StorageProperties.buildObjectStorageHadoopConfig`），**保留**其后 `paimon.*/fs./dfs./hadoop.` 覆盖块（保序 last-write-wins）。
  2. **先 recon**：`PaimonCatalogFactory` 是纯静态 util（无 ctx），`applyStorageConfig` 3 调用方（buildHadoopConfiguration/buildHmsHiveConf/buildDlfHiveConf）；须找到连接器调 factory 处 ctx/storageList 从哪来，把 `List<StorageProperties>` 线程进去（签名重构）。
  3. T1 等价性测试：fe-filesystem 产物 vs fe-property 现产物，常见路径全等 + 注释超集（R-001 闸）。
  4. 编译/测 paimon 模块需 `-am package -Dassembly.skipAssembly=true`（shade jar 带 HiveConf）。
- 代码 commit：P0-T01（plan-doc）+ P0-T02 + P1-T01 + P1-T02 + DV-002 决策记录。

## 下一步（明确）
1. **等待用户批准 `tasks.md`（14 task，含 P3a）** 后进入 Implement。
2. 获批后从 **P1-T01**（`ConnectorContext.getStorageProperties()`）开始；`P0-T01/T02` 可并行。Kerberos `fe-kerberos`（P3a-T01）依赖 P2-T02。
3. 严格按 `WORKFLOW.md §2` 单任务循环。

## 未决 / 需用户确认
- ~~P3a 是否纳入本次~~ → **已确认纳入**（2026-06-17）。~~模块名~~ → **定 `fe-kerberos`**。
- `P1-T02` 是本项目**唯一**的 fe-core 改动（`DefaultConnectorContext` 新增 `getStorageProperties()`，限 paimon 路径、不碰 property 包）。用户已倾向接受。
- ⚠️ **红线扩展**：P3a 新增 `fe-kerberos` 顶层模块属本次合法改动；但 fe-common / fe-filesystem-hdfs 的既有 kerberos 路径**本次零改动**（P3b follow-up）——提交前 `git diff` 须确认未碰这两处。

## 红线提醒（WORKFLOW §4）
- 只动：metastore-api/spi（新建）、paimon、ConnectorContext（仅新增）、DefaultConnectorContext（仅新增）、相关 pom、本跟踪目录。
- 禁碰：fe-core `datasource.property.{storage,metastore}` 包、其它连接器、fe-property 删除。
