# 🤝 Session Handoff —— 每语句表加载归属者 · 移植/重构

> **滚动文档**：每 session 结束覆盖式更新，只留下一个 session 必须的上下文。
> 开场必读顺序、模板、铁律见 [`README.md`](./README.md)；进度见 [`progress.md`](./progress.md)；状态见 [`tasklist.md`](./tasklist.md)。

---

# 🆕 本轮（2026-07-19 session 5）已完成：**防漂移门禁落地 → 读取键石（STEP 1 / RD-1）完全收官**

## 一句话结果
- 用户拍板方案 A（现在就上门禁锁死读取侧、写入 8 处显式豁免）。
- **已实现（全绿，已提交 `b2d147998d1`）**：
  - 新增 `tools/check-fecore-metadata-funnel.sh`（bash grep 门禁，仿 `check-connector-imports.sh`）+ 自测 `.test.sh`：扫 `fe/fe-core/src/main/java`，禁裸 `Connector#getMetadata(session)`；放行=①funnel 文件 `PluginDrivenMetadata.java`（含 javadoc）②带 `getMetadata-funnel-exempt` 标记的行（call 行**或其上一行**）③无参 `getMetadata()`（异方法）④注释行。正则双形（同行参数 + 换行参数），不误伤 `getMetadataTableRows`/API 定义。
  - 8 处写入裸调各加一行上置标记注释（103 字）；挂入 `fe/fe-core/pom.xml` validate 阶段 exec（与 fe-connector 门禁同深度同范式）。
- **验证**：自测 PASS（10 项含核心/换行/白名单/同行+上行标记/无参/边界/注释/退出码/标记承重）；门禁真实树 exit 0、8 处未标记 exit 1（都证过）；fe-core checkstyle 0 违规；`mvn -pl fe-core validate` 实跑触发 exec + BUILD SUCCESS。
- **读取键石收官**：C1 地基 + C2 关闭 + C3 改道 + 扫描存字段 + 后台读穿 + 防漂移门禁全落地（RD-1 = ✅）。

---

# ➡️ 下一个 session = **HMS 异构网关兄弟扇出（STEP 2 / RD-2）**

## 第一件事（先读）
1. 读 `designs/expanded-scope-phasing-and-security-decisions.md` §1（读写共用忠实 Trino）+ §2（后台读穿），尤其 **§1 末：兄弟入口须覆盖 `beginTransaction` 路由，不仅 getMetadata**。
2. 读 `designs/P1-implementation-design.md` §5（HMS sibling 扇出蓝图：三连接器共享 catalogId → key 加属主 label；~43 处 per-handle 转发；`resolveSiblingOwner` 三路派发；DCL sibling 身份稳定）。
3. 读架构记忆 `catalog-spi-plugin-tccl-classloader-gotcha`（ThriftHmsClient/HiveConf split-brain 四 locus）、`hms-iceberg-delegation-needs-e2e`（每项 iceberg-on-HMS 新能力都要配 e2e）、`iceberg-table-resolution-cache-scoping`。

## A. 动码前先 grounding + 出中文方案待确认（本任务铁律：设计先行）
- 网关已 LIVE（hms 在 `SPI_READY_TYPES`，commit `83585fd5097`）。sibling 扇出全在**连接器内部**（fe-connector-hive）→ fe-core arch 门禁看不见，由连接器侧单测守（仿 `HiveConnectorSiblingTest`）。
- 对当前代码逐点核实（会漂移）：`HiveConnectorMetadata.getTableHandle` 按格式转发 + `siblingMetadata`/`icebergSiblingMetadata`/`hudiSiblingMetadata`（各转发点当前行号）；`HiveConnector.resolveSiblingOwner`（三路 `ownsHandle`）；`beginTransaction` 路由点（`HiveConnectorMetadata:1854` 附近，核实）；`createSiblingConnector` 传 `this` context → 三连接器同 catalogId 的证据（`DefaultConnectorContext`）。
- 出中文方案（不引任务代号）：为何 catalogId 单独做 key 会把三连接器塌成一个 metadata、派错；修法四点（见 B）；e2e 落点。**待用户确认后再改代码。**

## B. 实现要点（分期定稿 §1 末 + P1-design §5）
1. **funnel key 加属主 label**：`"metadata:"+catalogId+":"+owner`，`owner∈{hive,iceberg,hudi}`，**在 `resolveSiblingOwner` 命中臂后取 label**（非预解析、非 identityHashCode）。
2. **兄弟 getMetadata 及 beginTransaction 都进同一入口**：`siblingMetadata`/`icebergSiblingMetadata`/`hudiSiblingMetadata` + 写事务 mint 点，先解析属主 Connector（照旧可 fail-loud），再 `scope.getOrCreateMetadata(key(catalogId,ownerLabel), () -> owner.getMetadata(session))`；getTableHandle 的 by-TYPE 转发与后续 per-handle 转发共享一个 sibling metadata。
3. **只存/返回 `ConnectorMetadata` 接口，绝不 cast 具体类型**（跨 loader CCE）。
4. **补异构网关 e2e**（`external_table_p2/refactor_catalog_param`，对齐 `hms-iceberg-delegation-needs-e2e`）：异构 HMS 目录跑 INSERT/DELETE/MERGE/ALTER/EXECUTE 断言与独立 iceberg 目录同表同结果。
- 连接器侧的 funnel 入口 = fe-core 的 `PluginDrivenMetadata.get`？注意：sibling 扇出在 fe-connector-hive 内，**不能 import fe-core**（`check-connector-imports` 门禁会挂）。→ 入口须是**连接器可见的 SPI**（`ConnectorStatementScope.getOrCreateMetadata` 已在 fe-connector-api，sibling 直接用 `session.getStatementScope().getOrCreateMetadata(key, factory)`，不经 fe-core 的 `PluginDrivenMetadata`）。**grounding 时确认这条路径**（label key 构造放连接器侧）。

## 后续步（详见分期定稿 §4）
- **STEP 3 写入共用（RD-3）**：3a 无状态写点（含 `resolveWriteTargetHandle`）改道进入口（删对应 `getMetadata-funnel-exempt` 标记 → 门禁自动收紧）；3b `ConnectorTransaction` 归属上移 `CatalogStatementTransaction`。闸门=读写身份等价 + 保留 hive 起写刷新（`HiveConnectorTransaction.beginWrite` 当场 getTable）+ 保留 tx↔session 绑定。纠缠点：iceberg 起写复用 `IcebergStatementScope.sharedTable`（P2 计划删）→ 3b 时定先后。
- **STEP 4 缓存隔离（安全，RD-4）**：`getIdentityShardKey()` SPI → iceberg 三投影缓存分片 + fe-core 表结构缓存 bypass(先)/分片(后) + 防漂移门禁；随 STEP 2 属主键覆盖异构网关；越权 e2e + 威胁模型签字。

## 关闭接线残留风险（carry-forward，详见分期定稿 §6）
1. 取消/超时非硬栅栏（`SplitAssignment.stop` 只置标志）——既有共担，P1 no-op 关闭下无实害，**关闭做实事前须硬化**。
2. arrow-flight 异常断连 → 注册表条目留存（无 TTL）——既有共担。
3. **待确认**：走协调器但从不走 getSplits 的纯 information_schema / 某元数据 TVF 可能漏关。
4. **🔴 TCCL 自钉扎（硬前置）**：连接器 `close()` 做实事（P2+）后，主关闭回调 + 兜底两处都须把 TCCL 钉到插件 classloader（连接器 `close()` 自钉扎首选）。

## 铁律 / 闸门提醒
- 用户 2026-07-19 已豁免铁律 A（fe-core 只减不增）。仍守：连接器 connector-agnostic（作用域值 Object）；作用域跨用户即泄漏（STEP 4 威胁模型）；**fe-connector-hive 不得 import fe-core**（门禁）。
- **动码前探测并发活动**（git log/status + maven 进程 + 近 90s mtime），发现活跃即停手（`concurrent-sessions-shared-worktree-hazard`）。
- 本任务设计先行：调研设计阶段结束、正式改代码前，先把方案用中文详述待用户确认。

---

# 🗂 遗留 / 关联
- 分期定稿（现行主线）：`designs/expanded-scope-phasing-and-security-decisions.md`（含 §1 读写共用、§2 后台、§4 分期、§6 残留风险）。
- STEP 1 蓝图：`designs/P1-implementation-design.md`（§2 改道表；§4 两层关闭；§5 HMS sibling；§8 arch 门禁）。
- C3 核对清单（已实施）：`designs/C3-read-reroute-verified-checklist.md`。
- 目标架构全景：`designs/trino-parity-metadata-redesign-design.md`。
- 门禁：`tools/check-fecore-metadata-funnel.sh`（+ `.test.sh`），fe-core pom validate exec。
- 架构记忆：`catalog-spi-plugin-tccl-classloader-gotcha`、`hms-iceberg-delegation-needs-e2e`、`iceberg-table-resolution-cache-scoping`。
- e2e 一律留连接器进 `SPI_READY_TYPES` 切换阶段统一补；异构网关 e2e 随 STEP 2/4。
