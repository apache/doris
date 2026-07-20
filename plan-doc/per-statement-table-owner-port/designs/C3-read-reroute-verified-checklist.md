# C3 读取侧改道 —— 逐缝核对清单（动手前，行号已按当前代码校准）

> 承接 `P1-implementation-design.md` §2/§3 与 `expanded-scope-phasing-and-security-decisions.md` §2。
> 本文是 C3（读取侧改道 + 后台读穿纠正）**动手前的核对关**产物：把设计里的改道表逐条对当前代码行号重新核实（会漂移），并经一轮对抗式复核（workflow `wf_6e2967a9-1a2`，10 agents：7 census reader + 3 adversarial verifier）。**本文不动代码。**

---

## ✅ 实施状态（2026-07-19 session 4）

已全部落地并验证：`buildCrossStatementSession` 助手 + 9 处 NONE 读穿（含名字映射两缝，含 fetchRowCount ANALYZE 修复）+ 49 处改道 + 扫描节点 `cachedMetadata`/`metadata()`。测试适配 11 文件 + 新守门 `ConnectorSessionImplTest.explicitNoneStatementScopeWinsOverLiveContext`。验证：目标 247 测全绿 + checkstyle 0 + 主编译 SUCCESS。**剩防漂移门禁**（读取键石收官，见 HANDOFF）。

## 0. 结论一句话

- fe-core 里连接器 `getMetadata(` 工厂缝 = **66 处**（80 raw grep 减 9 处测试静态 + 4 处漏斗自身 + 1 处 `RuntimeProfile.node.getMetadata()`）。**完整切成四类，相加正好 66，无遗漏/无重叠/无未分类缝。**
- 全部设计命名的行号**零漂移**（例外：扫描节点 9 处 + 尾部两处漂 +13，纯位置漂移，方法未变）。
- 揪出两处需拍板的偏差（§4）。

## 1. 四类分区（66 = 49/51 改道 + 7/9 读穿 + 8 写延后 + 4 漏斗自身在 66 之外）

> 注：4 处漏斗自身（`PluginDrivenMetadata` 内 3 javadoc + 1 factory lambda）不计入 66。

### A. 改道进漏斗（`connector.getMetadata(session)` → `PluginDrivenMetadata.get(session, connector)`）

| 文件 | 缝 | 当前行 | 线程 |
|---|---|---|---|
| PluginDrivenExternalCatalog（DDL 19 + tableExist 1 = 20 处） | createTable/createDb/dropDb/dropTable/renameTable/truncateTable/addColumn(s)/dropColumn/renameColumn/modifyColumn/reorderColumns/createOrReplaceBranch/createOrReplaceTag/dropBranch/dropTag/addPartitionField/dropPartitionField/replacePartitionField + tableExist | 332,422,501,550,598,670,714,807,823,838,853,868,884,915,931,947,963,988,1003,1018 | on-thread |
| PluginDrivenExternalTable（读 12 处） | getSyntheticScanPredicates(159)/resolveWriteCapabilityHandle(189,读——探写能力但读元数据)/resolveIsView(576)/getViewText(594)/getShowCreateTableDdl(617)/fetchSyntheticWriteColumns(727,读——getFullSchema 路径)/getNameToPartitionItems(820)/getNameToPartitionValues(881)/getMetadataTableRows(918)/getComment(950)/getSupportedSysTables(981)/（第 12 处 1304） | 159,189,576,594,617,727,820,881,918,950,981,1304 | on-thread/planning |
| PluginDrivenExternalDatabase.getLocation | 1 | 84 | on-thread |
| PluginDrivenScanNode（扫描 9 处，**存字段而非裸改**） | create(静态工厂,206) + convertPredicate(806)/tryPushDownLimit(846)/tryPushDownProjection(865)/pinMvccSnapshot(910)/pinRewriteFileScope(1055)/pinTopnLazyMaterialize(1074)/buildColumnHandles(1902)/buildRemainingFilter(1939) | 见左 | planning-thread |
| PluginDrivenMvccExternalTable（3 处） | materializeLatest(143,mixed)/loadSnapshot(358,on-thread)/resolveFreshnessProbe(725,background) | 143,358,725 | 见左 |
| misc（4 处命令/TVF） | ShowPartitionsCommand(285)/ConnectorExecuteAction(128)/CallExecuteStmtFunc(101)/JdbcQueryTableValueFunction(52) | 见左 | on-thread |

**扫描节点存字段细节**：加懒字段 `private volatile ConnectorMetadata cachedMetadata;` + `metadata()` 访问器（`if(cachedMetadata==null) cachedMetadata = PluginDrivenMetadata.get(connectorSession, connector);`），8 处 per-method 全改调 `metadata()`。**create(206) 是静态工厂无实例** → 直接调 `PluginDrivenMetadata.get(session, connector)`（不能用实例访问器）。字段现状：`connector:148`、`connectorSession:149`、`currentHandle:152`；镜像现有 `volatile resolvedScanProvider:184`。8 处均在单规划线程（并发 appendBatch 之前）。

### B. 后台读穿加载器（跨语句缓存 loader，**强制 NONE 会话**，绝不绑定语句作用域）

| 文件 | 缝 | 当前行 | 线程 |
|---|---|---|---|
| PluginDrivenExternalCatalog.listDatabaseNames | 1 | 295 | background |
| PluginDrivenExternalCatalog.listTableNamesFromRemote | 1 | 311 | background |
| PluginDrivenExternalTable.initSchema | 1 | 460 | background(schema-cache) |
| PluginDrivenExternalTable.getColumnStatistic | 1 | 1052 | background(stats-cache) |
| PluginDrivenExternalTable.getChunkSizes | 1 | 1087 | analyze-thread |
| PluginDrivenExternalTable.fetchRowCount | 1 | 1153 | **mixed**（含 ANALYZE 执行线程，见 §3） |
| MetadataGenerator.dealPluginDrivenCatalog | 1 | 1329 | background(BE-driven) |

### C. 写入路径（**留后续步骤，C3 不碰**，共 8 处）

`resolveWriteTargetHandle(PluginDrivenExternalTable:133)` + PhysicalPlanTranslator(612,660) + PluginDrivenInsertExecutor(206) + BindSink(673,712) + IcebergRowLevelDmlTransform(112) + PhysicalIcebergMergeSink(303)。

---

## 2. 强制 NONE 的机制（已源码证实为「与裸直调字节等价」）

- `ConnectorSessionBuilder.withStatementScope(ConnectorStatementScope.NONE)` 显式短路 `captureStatementScope()` 的第一道 `!= null` 守卫 → 与线程 ConnectContext 状态**完全无关地强制 NONE**（`ConnectorSessionBuilder.java:190-203`）。
- `ConnectorStatementScope.NONE.computeIfAbsent(key,loader) = loader.get()`（每次跑工厂、零缓存）；`getOrCreateMetadata` 默认走它；`closeAll` 默认 no-op → NONE 无可关之物。
- 故 `PluginDrivenMetadata.get(NONE会话, connector)` = **每次新建、零留存**，SPI 文档原话「byte-identical to building metadata every time」。
- 7 处 loader 现状全是 `<catalog>.buildConnectorSession()`（读 ConnectContext，有 ctx 即绑活作用域=泄漏隐患）。**修法 = 新增 `PluginDrivenExternalCatalog.buildCrossStatementSession()`**：镜像 `buildConnectorSession()` 两分支，末尾统一 `.withStatementScope(ConnectorStatementScope.NONE)`。7 处改调它。

---

## 3. ANALYZE 隐患（已证实，修法并入 fetchRowCount 强制 NONE）

- `AnalyzeTableCommand.doRun` → `AnalysisManager.buildAnalysisJobInfo:349` → `:418` 在 `getRowCount()<=0` 时**同步直调** `table.fetchRowCount()`，全程在 ANALYZE 语句执行线程（ConnectContext + StatementContext 均活）。
- `fetchRowCount:1152` 经 `buildConnectorSession()` 捕获该活作用域。
- **今日无害**：fetchRowCount 现走**裸直调** `connector.getMetadata(session)`（不经漏斗），故什么都没 memo 进作用域，会话只是持了个引用。隐患是**前瞻性**的：一旦 fetchRowCount 改走漏斗（且 metadata 挂真实可关资源），会把资源钉在整个 ANALYZE 生命周期、并在语句末误关。
- **修法**：fetchRowCount 用 `buildCrossStatementSession()` 强制 NONE。**无需单独改 AnalysisManager**——强制 NONE 后无论走不走漏斗都读穿。

---

## 用户拍板（2026-07-19）

- **偏差①**：名字映射两缝 `fromRemoteDatabaseName(1106)`/`fromRemoteTableName(1112)` → **归入强制 NONE 读穿**（→ 读穿 9 处、改道 49 处）。
- **偏差②**：读穿 loader → **走统一入口 + 传 NONE 会话**（甲案，门禁零例外）。

## 4. 两处需拍板的偏差（对抗复核揪出）

### 偏差①：名字映射两缝 `fromRemoteDatabaseName(1106)`/`fromRemoteTableName(1112)`
- 设计原判：on-request 的 DDL 改道（进 A 类改道）。
- 复核实证（两 agent 独立）：其**全部调用方跑在离请求线程的名字缓存 loader / buildDbForInit / buildTableForInit / metastore-event-sync 路径**（`ExternalCatalog:586,970`；`ExternalDatabase:224,277,659`），与后台 loader 同族。裸改道进漏斗 → 若某次同步首载落在有活 ConnectContext 的请求线程，会把 memo 值绑进**活得比语句久的名字缓存**。
- **建议**：与其它 7 处 loader 同等对待——**强制 NONE**（→ 读穿 9 处、改道 49 处）。

### 偏差②：读穿 loader 的落地形态（漏斗+NONE vs 裸直调+门禁白名单）
- 旧设计 §8 选「保留裸直调 + 防漂移门禁白名单」（当时理由「更清晰、避免塞残留 scope」，该理由在强制 NONE 后已失效——NONE 无 scope 可塞）。
- 已证实「漏斗+NONE」与「裸直调」**字节等价**，且让防漂移门禁**零例外**（fe-core 里不再有合法的裸 `connector.getMetadata(`）。
- **建议**：读穿 loader 也走漏斗（传 NONE 会话），门禁更干净。

---

## 5. 落地子提交建议（C3 内部，每步独立可编译/回退）

1. 新增 `buildCrossStatementSession()` 助手（连接器无关，强制 NONE）。
2. 后台读穿：7（或 9）处 loader 改用助手 + 走漏斗；含 fetchRowCount ANALYZE 隐患修复。守门：加载计数=1(对照 NONE=N)、跨 catalog/queryId 隔离、ANALYZE 首次不绑活作用域。
3. 读/DDL/misc/MVCC 改道：49（或 51）处裸改道进漏斗。
4. 扫描节点存字段：加 `cachedMetadata`+`metadata()`，9 处改调。
- 防漂移门禁（bash grep，仿 `check-connector-imports.sh`）择机随 STEP 1 收尾补（形态取决于偏差②）。HMS 兄弟扇出 = 下一大步，不在 C3。

## 6. 已核实为「不动」

- 写入 8 处（§1.C）；`getMetadataTableRows()` 数据调用（PluginDrivenExternalTable:923，非工厂缝）；`TestExternalCatalog` 9 处测试静态 Map；`RuntimeProfile.node.getMetadata():260`；`PluginDrivenMetadata` 漏斗自身 4 处。
