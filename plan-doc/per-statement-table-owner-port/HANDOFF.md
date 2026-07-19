# 🤝 Session Handoff —— 每语句表加载归属者 · 移植/重构

> **滚动文档**：每 session 结束覆盖式更新，只留下一个 session 必须的上下文。
> 开场必读顺序、模板、铁律见 [`README.md`](./README.md)；进度见 [`progress.md`](./progress.md)；状态见 [`tasklist.md`](./tasklist.md)。

---

# 🆕 本轮（2026-07-19 session 3）已完成：**定稿分期 + 落地读取键石的 C1 地基 + C2 关闭接线**

## 一句话结果
- 用户拍板扩展范围并定**分期推进**：读写共用、缓存隔离都做，但排序 + 独立提交；后台"传句柄到后台线程"被取证证伪 → 改"读穿 + 修隐患"；缓存"list ≠ load"已确认 → 缓存隔离是**真实安全修复**。定稿见 `designs/expanded-scope-phasing-and-security-decisions.md`。
- **已实现并提交（均编译通过 + checkstyle 0 + 单测全绿）**：
  - **C1 地基** `5b7312f9d1f`：SPI 加 `getOrCreateMetadata`/`closeAll` 默认方法 + `ConnectorStatementScopeImpl.closeAll`(幂等) + 静态漏斗 `PluginDrivenMetadata.get`。字节中性（无生产路径接进漏斗）。
  - **C2 关闭接线** `12f3e95239b`：**两层关闭**（主=getSplits 注册 `scope::closeAll`；兜底=`StatementContext.close` 守卫 closeAll + `proxyExecute` finally；reset closeAll-before-null；retry 每次重置）。P1 关闭仍 no-op，行为中性。
- 取证 workflow：`wf_8b907b93-e9f`（扩展范围三块）、`wf_9250330b-e81`（关闭接线生命周期，**推翻了原"scope 创建处注册"会泄漏**）。

---

# ➡️ 下一个 session = **STEP 1 的 C3：读取侧改道 + 后台读穿纠正（本阶段最大一步，动手前先过清单 + 配对抗复核）**

## 第一件事（先读）
1. 读 `designs/expanded-scope-phasing-and-security-decisions.md`（分期定稿 + §6 C1/C2 进展 + 关闭接线残留风险）。
2. 读 `designs/P1-implementation-design.md`：§2 的 66 缝改道表（C3 的清单）；§3 扫描节点存字段；**§4 已更正为两层关闭**（C2 已实现，勿再按旧"scope 创建处注册"）。
3. 读架构记忆 `iceberg-table-resolution-cache-scoping`。

## C3 要做的（读取侧改道 + 后台纠正）
- **读/扫描/DDL/MVCC/misc 改道**（~55 处 on-thread）：把 `connector.getMetadata(session)` 改调 `PluginDrivenMetadata.get(session, connector)`。清单见 §2 组 A/B/C/E/misc。
- **扫描节点存字段**（组 C）：`PluginDrivenScanNode` 加懒字段 `cachedMetadata` + `metadata()`，8 处 per-method 重取改调它（`volatile` 稳态）。
- **后台读穿纠正**（用户拍板）：7 处跨语句 loader（`listDatabaseNames`/`listTableNamesFromRemote`/`initSchema`/`getColumnStatistic`/`getChunkSizes`/`fetchRowCount`/`MetadataGenerator.dealPluginDrivenCatalog`）**显式建 NONE session 读穿**（`ConnectorSessionBuilder.withStatementScope(NONE)` 或新增 `buildCrossStatementSession()`）；**修 `fetchRowCount` 在 ANALYZE 线程（`AnalysisManager.buildAnalysisJobInfo`）被错误绑定到语句作用域的隐患**。
- **⚠ 别误改道 `PluginDrivenExternalTable.resolveWriteTargetHandle`**（藏在读文件里的**写专用**点）——标记为写 seam，留 STEP 3。
- 守门：一语句一表加载计数=1（对照 NONE=N）；跨 catalog/queryId 隔离。
- **动手前**：先把 §2 改道表逐条核对当前代码行号（可能漂移），列清单给用户过一遍，并计划一轮对抗式复核（clean-room 偏好）。

## 后续步（STEP 2/3/4，详见分期定稿 §4）
- **STEP 2 HMS 兄弟**：键含属主 label + 兄弟 getMetadata **及 beginTransaction** 进漏斗 + 异构网关 e2e。
- **STEP 3 写入共用**：3a 无状态写点改道（含被标记的 `resolveWriteTargetHandle`）；3b `ConnectorTransaction` 归属上移 `CatalogStatementTransaction`。闸门=读写身份等价 + 保留 hive 起写刷新 + 保留 tx↔session 绑定。纠缠点：iceberg 起写复用表读 `IcebergStatementScope.sharedTable`（P2 计划删）——3b 时定"先接旧 vs 先做 iceberg 最小 P2 前置"。
- **STEP 4 缓存隔离（安全）**：`getIdentityShardKey()` SPI → iceberg 三投影缓存分片 + fe-core 表结构缓存 bypass(先)/分片(后) + 防漂移门禁；随 STEP 2 属主键覆盖异构网关；越权 e2e + 威胁模型签字。

## 关闭接线残留风险（carry-forward，详见分期定稿 §6）
1. 取消/超时非硬栅栏（`SplitAssignment.stop` 只置标志不 join pump）——既有共担，P1 no-op 下无实害，**关闭做实事前须硬化**。
2. arrow-flight 异常断连 → 注册表条目留存（无 TTL）——既有共担。
3. **待确认**：走协调器但从不走 getSplits 的外部目录查询（纯 information_schema / 某元数据 TVF）可能漏关——STEP 3/后续确认。
4. **🔴 TCCL 自钉扎（硬前置）**：一旦连接器 `close()` 做实事（P2+），主关闭回调 + 兜底两处都须把 TCCL 钉到插件 classloader（连接器 `close()` 自钉扎首选）。

## 铁律 / 闸门提醒
- 用户 2026-07-19 已豁免铁律 A（fe-core 只减不增）——本重构的 fe-core 净增已获授权。仍守：连接器 connector-agnostic（作用域值 Object）；作用域跨用户即泄漏（STEP 4 威胁模型）。
- **动码前探测并发活动**（git log/status + maven 进程 + 近 90s mtime），发现活跃即停手（`concurrent-sessions-shared-worktree-hazard`）。

---

# 🗂 遗留 / 关联
- 分期定稿（现行主线）：`designs/expanded-scope-phasing-and-security-decisions.md`（含 §6 进展 + 残留风险）。
- STEP 1 蓝图：`designs/P1-implementation-design.md`（§2 改道表；§4 已更正为两层关闭）。
- 目标架构全景：`designs/trino-parity-metadata-redesign-design.md`。
- 蓝本 iceberg：`../perf-hotpath-iceberg/`（PERF-07）；代码范本 `IcebergStatementScope`(commit `ea7fd1f6e7a`)+地基 `97bdcd6bdbe`。
- 架构记忆：`iceberg-table-resolution-cache-scoping`。
- 本轮 workflow journal：`wf_8b907b93-e9f`（扩展范围）、`wf_9250330b-e81`（关闭接线生命周期）；前轮 `wf_72d1e505-75c`/`wf_62cc379e-c6e`/`wf_7e537094-44f`。
- e2e 一律留连接器进 `SPI_READY_TYPES` 切换阶段统一补；异构网关 e2e 随 STEP 2/4（对齐 `hms-iceberg-delegation-needs-e2e`）。
