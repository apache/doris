# 修复设计：时间旅行读被改名列丢列导致 BE 崩溃（列句柄无快照维度）

> 对应 TASKLIST 第 2 条（B1，唯一 crash 风险）。姊妹的统计无快照问题（第 15 条）单列一节，随后处理。

## 1. 问题与根因（已实证）

- 通用扫描节点 `PluginDrivenScanNode.buildColumnHandles()` 在 **pin 快照之前**用未 pin 的 handle 调 `getColumnHandles(session, handle)`，拿到的是 **latest schema** 建的 name→handle map。
- 时间旅行查询（`FOR VERSION AS OF <old>`）的 slot 绑的是**钉住的旧 schema** 的列名。若某列在钉住快照之后被 `RENAME`，slot 带旧名、latest map 只有新名 → `allHandles.get(旧名)` 命中 null → **静默丢列**（`if (ch != null)`）。
- 混合投影（被改名列 + 存活列同现）下，丢列后 columns 非空；paimon 的 field-id dict 的 `-1` 目标条目按 columns 建，缺该列 → BE `StructNode children.at()` `std::out_of_range` → **SIGABRT**。
- iceberg 幸免：其扫描 provider 在 `hasSnapshotPin()` 下用**完整钉住 schema** 重建 dict，绕过丢列；paimon 无此自防。
- **实证**：已用现有 `sc_parquet`（snapshot 1 = 改名前 schema）跑 `SELECT * FROM sc_parquet FOR VERSION AS OF 1` 复现 BE 崩溃。

**SPI 层根因**：取表结构接口有带快照的重载 `getTableSchema(session, handle, snapshot)`（连接器据此返回钉住 schema），但**取列句柄接口只有不带快照的两参版本** —— 二者不对称，列句柄看不到时间点。

## 2. 方案（用户已拍板：框架级重载 + 通用兜底）

对齐 Trino 精神（列解析发生在已钉快照的 handle 上）。Trino 的 handle 天然携带 snapshot，但 Doris 各连接器的 `getColumnHandles` 写死读 latest（reorder 无用），故落到 Doris 已有代码上，等价且最省事的做法是**补一个带快照的重载**，与已有 `getTableSchema` 三参重载同构。

### 2.1 connector-api（新增，向后兼容）
`ConnectorTableOps`：
- 新增默认重载 `getColumnHandles(session, handle, ConnectorMvccSnapshot snapshot)`，默认转调两参（latest）。
- 新增能力位 `supportsColumnHandleSnapshotPin(session)` 默认 `false`。

### 2.2 fe-core（`PluginDrivenScanNode.buildColumnHandles`）
- 用与 `pinMvccSnapshot` **相同**的 version-aware selector 解析本次快照；
- **仅当 `getPinnedSchema() != null`**（即钉住了一个**不同的**历史 schema）时走三参重载，把列句柄建在钉住 schema 上；否则（普通查询/同 schema 的时间旅行）保持两参 latest —— **逐字节不变**。
- **通用兜底（fail-loud）**：当连接器 `supportsColumnHandleSnapshotPin()==true` 且某 slot 的列**存在于钉住 schema** 却在句柄 map 缺失时，抛 `UserException`（而非静默丢 → BE 崩）。用能力位门控，使 iceberg（靠自己的 dict 重建兜底，能力位 false）保持原静默跳过路径不变；钉住 schema 里没有的合成列仍照旧跳过。
- `buildColumnHandles` 因此声明 `throws UserException`；4 处调用点：`getSplits`（本就 `throws`）直接透传；`startSplit`/`startStreamingSplit`/`getOrLoadPropertiesResult` 三处把 `buildColumnHandles`+投影/过滤+`pinMvccSnapshot` 一并纳入已有的 `try/catch(UserException)`（沿用既有错误通道，不新增行为）。

### 2.3 paimon（`PaimonConnectorMetadata`）
- 实现三参 `getColumnHandles`：`schemaId<0` 回退两参；否则用**同一 memo 化的 `schemaAt(schemaId)`**（与三参 `getTableSchema` 共用），按钉住列名建句柄。这样列句柄名与 slot 绑定的钉住 Doris schema 一致 → 不再丢列。
- `supportsColumnHandleSnapshotPin()` 返回 `true`。
- 无需改投影/dict：paimon 扫描侧 `resolveScanTable` 已 pin（`table.rowType()` 为钉住 rowType），投影按**列名** `indexOf`、dict 的 `selectCurrentSchemaFields` 已**优先钉住 schema**；上游喂对（钉住）列名后，两者自然正确。`PaimonColumnHandle.fieldIndex` 读路径无消费者，只有 `getName()` 生效。

### 2.4 iceberg
- 不改。其连接器侧 dict 重建已兜住崩溃；三参默认转调两参 latest，行为不变；能力位 false → 不受 fail-loud 影响。

## 3. 为什么这样安全 / 收敛

- **改动面最小**：只有"时间旅行到不同历史 schema"这一路径改走三参；其余全部逐字节不变。
- **不违铁律**：接口新增在 connector-api（非 fe-core 源）；fe-core 改动是连接器无关地把已解析的快照往下传，非把连接器逻辑塞进 fe-core。
- **不回归 iceberg**：fail-loud 用能力位门控，iceberg 能力位 false。
- **根因修复**：未来任何连接器只需实现三参重载即可获得正确的时间旅行列解析。

## 4. 验证

- e2e：`regression-test/suites/external_table_p0/paimon/test_paimon_time_travel_rename.groovy`（用现有 `sc_parquet`，无需重建 warehouse）——混合投影 `SELECT k, vVV FOR VERSION AS OF 1` 及 `SELECT *`，native + jni 两路；对照单列改名投影（空集回退，不崩）。修复前 BE 崩溃、用例挂；修复后返回钉住 snapshot 1 数据。
- 构建：fe-connector-api / fe-connector-paimon / fe-core `compile` + `checkstyle:check` 全绿。
- paimon 连接器无单测脚手架（`src/test` 空、metadata 难裸构造），故以 e2e 为权威验证。

## 5. 姊妹问题（统计行数无快照，第 15 条）——已完成

- 现象：`getTableStatistics(session, handle)` 亦无快照维度；时间旅行下 CBO 取 latest 行数、扫描读钉住快照 → 基数估算偏斜（**只影响代价估算，不影响结果正确性、不崩溃**）。
- 关键差异：统计走**跨语句缓存会话**（`ConnectorStatementScope.NONE`），`ExternalRowCountCache` 只按 `{catalogId,dbId,tableId}` 建 key，且**缓存 loader 在后台线程、拿不到查询时间点**。故不能复用 #2 的"扫描线程内 pin"入口。

**实现（已落地）**：
- **connector-api**：`ConnectorStatisticsOps` 增默认三参 `getTableStatistics(session, handle, snapshot)`，默认转调两参（latest）。
- **fe-core `StatementContext`**：新增 `getVersionedSnapshot(TableIf)` —— 仅返回**在非默认 version key（真正的 FOR VERSION/TIME AS OF / @branch/@tag）下钉住**的快照；普通/最新引用（version key 为 `""`）与歧义（`t@v1` join `t@v2`）返回空。这是"是否真的时间旅行"的判据（普通查询行数偏斜发生在**任意**跨快照时间旅行，故不能像 #2 用 `getPinnedSchema()!=null` 判 schema 演进）。
- **fe-core `PluginDrivenExternalTable`**：override `getRowCount()`。仅当本语句为该表钉住了真正的 versioned 快照时，在**查询线程内**按钉住快照直算行数（`fetchRowCountAtSnapshot` → 三参 `getTableStatistics`），**绕过**latest-keyed 跨语句缓存（历史行数不值得跨语句缓存）；否则/无上下文 → `super.getRowCount()` 走原缓存路径，**逐字节不变**。连接器算不出时优雅回退 latest。
- **iceberg**：三参用 `table.snapshot(snapshotId).summary()`（复用同一 total-records/position/equality-delete 公式），非 `currentSnapshot()`。
- **paimon**：三参用与扫描路径**同一** `applySnapshot` 生成 scan options，`resolveTable` + `table.copy(scanOptions)` 后 `rowCount` 求和；任何异常回退空（→ 上层回退 latest）。

**验证**：`test_paimon_time_travel_rowcount.groovy` —— `EXPLAIN SELECT * FROM tbl_time_travel FOR VERSION AS OF 1` 的 `cardinality` 应为钉住 snapshot 1 的 3（非 latest 18），并断言 time-travel 基数 < latest 基数（修复前二者相等）。compile + checkstyle 全绿。

**范围说明**：普通查询、SHOW TABLE / information_schema（走 `getCachedRowCount`，未 override）保持原缓存路径不变；本修复只在真正时间旅行时改走钉住快照直算。
