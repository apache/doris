# 🤝 Session Handoff —— 每语句表加载归属者 · 移植/重构

> **滚动文档**：每 session 结束覆盖式更新，只留下一个 session 必须的上下文。
> 开场必读顺序、模板、铁律见 [`README.md`](./README.md)；进度见 [`progress.md`](./progress.md)；状态见 [`tasklist.md`](./tasklist.md)。

---

# 🆕 本轮（2026-07-19 session 4）已完成：**落地读取侧改道 + 后台读穿纠正（读取键石主体收官）**

## 一句话结果
- 动手前先过对抗式核对关（workflow `wf_6e2967a9-1a2`），把改道表逐条对当前代码行号重核 → `designs/C3-read-reroute-verified-checklist.md`：fe-core 连接器 `getMetadata` 工厂缝**共 66 处**完整切四类（相加=66、零遗漏/重叠/未分类）。两处偏差经用户拍板。
- **已实现（全绿，未提交）**：
  - **助手** `PluginDrivenExternalCatalog.buildCrossStatementSession()`：镜像 `buildConnectorSession`（保留凭证），只多强制 `withStatementScope(NONE)`。
  - **9 处后台读穿**（含名字映射两缝 `fromRemoteDatabaseName`/`fromRemoteTableName` 经拍板归入）：改用助手 + 走统一入口 `PluginDrivenMetadata.get`；**含 `fetchRowCount` ANALYZE 隐患修复**（不必单改统计模块）。源码证实「NONE 走漏斗」与裸直调**字节等价**。
  - **49 处改道**：读/DDL/命令/多版本从 `connector.getMetadata(session)` 改调 `PluginDrivenMetadata.get(session, connector)`。
  - **扫描节点存字段**：`volatile cachedMetadata` + `metadata()`；8 处 per-method 改调它，静态 `create` 直调入口。
  - **测试适配**（workflow `wf_fbb60841-365`，11 agents 并行）：补 `getStatementScope()→NONE` stub + 测试替身补 `buildCrossStatementSession` override；新守门 `ConnectorSessionImplTest.explicitNoneStatementScopeWinsOverLiveContext`。
- **验证**：目标测试 247 全绿（先红 130 error+30 fail）+ ConnectorSessionImplTest 18 绿 + fe-core checkstyle 0 违规 + 主编译 SUCCESS。

---

# ➡️ 下一个 session = **读取键石收官（防漂移门禁）→ 然后 HMS 异构网关兄弟扇出**

## 第一件事（先读）
1. 读 `designs/C3-read-reroute-verified-checklist.md`（已实施；§1 四类分区、§2 强制 NONE 机制、用户两处拍板）。
2. 读 `designs/P1-implementation-design.md` §5（HMS sibling 扇出蓝图：三连接器共享 catalogId → key 加属主 label）+ §8（arch 门禁蓝图）。
3. 读架构记忆 `iceberg-table-resolution-cache-scoping`、`catalog-spi-hms-hiveversionutil-gate-false-positive`（门禁误报先例）。

## A. 收官：防漂移门禁（读取键石最后一步，小）
- 目标：加一道构建期门禁（bash grep，仿 `tools/check-connector-imports.sh` 风格 + 自测），确保 fe-core 里裸 `connector.getMetadata(` **只允许出现在** `PluginDrivenMetadata.java`（漏斗自身的 factory lambda）。
- **本轮已把读穿 loader 也走统一入口 → 门禁零例外**（无需白名单）。当前 fe-core 里合法裸调用只剩：`PluginDrivenMetadata.java:54`（漏斗 lambda）+ 写路径 8 处（`PhysicalPlanTranslator`×2 / `BindSink`×2 / `IcebergRowLevelDmlTransform` / `PhysicalIcebergMergeSink` / `PluginDrivenInsertExecutor` / `PluginDrivenExternalTable.resolveWriteTargetHandle:133`）。
- **决策点（交用户）**：写路径 8 处现仍裸直调（留写入共用步骤）。门禁要么(a)现在只锁"非写文件"、写文件暂豁免（写入共用步骤再收）；要么(b)推迟整个门禁到写入共用步骤后一次落。建议(a)：现在就锁读侧防漂移，写文件显式列为"暂待写入步骤"豁免。落点见 P1-design §8（checkstyle Regexp 或 fe-connector pom 的独立 exec）。

## B. 下一大步：HMS 异构网关兄弟扇出（STEP 2 / P1-design §5）
- 网关已 LIVE（hms 在 `SPI_READY_TYPES`）。三连接器（hive/iceberg/hudi）**共享一个 catalogId** → 若统一入口 key 只用 catalogId 会把三者塌成一个 metadata、派错。
- **修**：①入口 key 加属主 label（`"metadata:"+catalogId+":"+owner`，owner∈{hive,iceberg,hudi}，在 `resolveSiblingOwner` 命中臂后取 label）；②**兄弟 getMetadata 也进入口**（`siblingMetadata`/`icebergSiblingMetadata`/`hudiSiblingMetadata`）；③只存/返回 `ConnectorMetadata` 接口不 cast；④补异构网关 e2e（`external_table_p2/refactor_catalog_param`，对齐 `hms-iceberg-delegation-needs-e2e`）。
- 注意：sibling 扇出在**连接器内部**（fe-connector-hive），fe-core arch 门禁看不见 → 由连接器侧单测守（仿 `HiveConnectorSiblingTest`）。

## 后续步（详见分期定稿 §4）
- **STEP 3 写入共用**：3a 无状态写点（含 `resolveWriteTargetHandle`）改道进入口；3b `ConnectorTransaction` 归属上移 `CatalogStatementTransaction`。闸门=读写身份等价 + 保留 hive 起写刷新 + 保留 tx↔session 绑定。纠缠点：iceberg 起写复用 `IcebergStatementScope.sharedTable`（P2 计划删）→ 3b 时定先后。
- **STEP 4 缓存隔离（安全）**：`getIdentityShardKey()` SPI → iceberg 三投影缓存分片 + fe-core 表结构缓存 bypass(先)/分片(后) + 防漂移门禁；随 STEP 2 属主键覆盖异构网关；越权 e2e + 威胁模型签字。

## 关闭接线残留风险（carry-forward，详见分期定稿 §6）
1. 取消/超时非硬栅栏（`SplitAssignment.stop` 只置标志）——既有共担，P1 no-op 关闭下无实害，**关闭做实事前须硬化**。
2. arrow-flight 异常断连 → 注册表条目留存（无 TTL）——既有共担。
3. **待确认**：走协调器但从不走 getSplits 的纯 information_schema / 某元数据 TVF 可能漏关。
4. **🔴 TCCL 自钉扎（硬前置）**：连接器 `close()` 做实事（P2+）后，主关闭回调 + 兜底两处都须把 TCCL 钉到插件 classloader（连接器 `close()` 自钉扎首选）。

## 铁律 / 闸门提醒
- 用户 2026-07-19 已豁免铁律 A（fe-core 只减不增）。仍守：连接器 connector-agnostic（作用域值 Object）；作用域跨用户即泄漏（STEP 4 威胁模型）。
- **动码前探测并发活动**（git log/status + maven 进程 + 近 90s mtime），发现活跃即停手（`concurrent-sessions-shared-worktree-hazard`）。

---

# 🗂 遗留 / 关联
- 分期定稿（现行主线）：`designs/expanded-scope-phasing-and-security-decisions.md`（含 §6 进展 + 残留风险）。
- STEP 1 蓝图：`designs/P1-implementation-design.md`（§2 改道表；§4 两层关闭；§5 HMS sibling；§8 arch 门禁）。
- C3 核对清单（已实施）：`designs/C3-read-reroute-verified-checklist.md`。
- 目标架构全景：`designs/trino-parity-metadata-redesign-design.md`。
- 本轮 workflow：`wf_6e2967a9-1a2`（C3 逐缝核对+对抗）、`wf_fbb60841-365`（测试适配 11 文件）。前轮 `wf_8b907b93-e9f`/`wf_9250330b-e81`。
- 架构记忆：`iceberg-table-resolution-cache-scoping`。
- e2e 一律留连接器进 `SPI_READY_TYPES` 切换阶段统一补；异构网关 e2e 随 STEP 2/4。
