# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 C3b-core 实现（① + ②[Option A] + ③ + commit-bridge，1 个 coherent commit）**

**设计 ✅ + step 1+2 实现 ✅ DONE 本 session（[D-072]，详 `tasks/designs/P6.6-C3-ws-write-design.md` §10）**。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6 翻闸进行中：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core 设计 ✅ + step 1+2（② SPI + ConnectorColumn invisibility）✅ → C3b-core 耦合核心+bridge 实现待办**。翻闸 = 5 commit-stream（C1/C2/C3/C4/C5），**C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`。

## ✅ 本 session 完成 = 设计（§10）+ **C3b-core step 1+2（② SPI carrier + ConnectorColumn invisibility infra；additive、dormant、TDD+mutation 绿）**
- 设计起步对抗 recon 把 **scope 改写**：用户三裁 ②=完整中立 SPI / commit-bridge=现在做 / 不拆。recon **推翻「暴露 native 表」前提**（classloader 隔离）→ 真桥 = **路由 DML 提交到连接器事务**（连接器 `IcebergConnectorTransaction` 已全建 + 通用 `RowLevelDmlCommand` SPI commit 链已 dormant）。详 §10 + [D-072]。
- **step 1+2 已实现（dormant additive infra，2 commit：doc `0e3cd0a6634` + 本码 commit）**：
  - **② SPI**：新出向 carrier `ConnectorWritePartitionField`/`ConnectorWritePartitionSpec`（`connector.api.write`，仿 `ConnectorWriteSortColumn`）+ `ConnectorWritePlanProvider.getWritePartitioning(session,handle)` default null + iceberg `IcebergWritePlanProvider.getWritePartitioning` override（native spec walk，carries transform/param/sourceColumnName/fieldName/sourceId+specId）。`IcebergWritePlanProviderTest` **26/0/0**（+3：identity/unpartitioned-null/bucket）+ **mutation**（swap sourceColumnName↔fieldName → 仅 bucket test 红、identity 绿=证 bucket test 非 trivially-pass）。
  - **③-infra（部分）**：`ConnectorColumn` 加 `invisible()`/`isVisible()` 标志（仿 `withTimeZone`）+ `ConnectorColumnConverter.convertColumn` 重应用 `setIsVisible(false)`。`ConnectorColumnConverterTest` **13/0/0**（+2：default-visible / propagate-invisible；RED 见证=mutation 证）。
  - **验证**：全 connector-iceberg 套件 **556/0/0+1 live-skip**（无回归）；import-gate PASS；新 SPI grep 无 `org.apache.iceberg`；`SPI_READY_TYPES` 未改。
- **剩余（fresh session，仍是大活 ~12-15 文件 coupled）**：③-infra 余下（**中立 format-version 信号** O4 / **ctx 中立重命名** — 故意推迟到与消费方同 session，避免 orphan）→ ① handles + 全 cast 放宽 + `getIcebergPartitioning` dual-mode helper（消费本 session 的 `getWritePartitioning`）+ ③ `PluginDrivenExternalTable.getFullSchema`/injector 注入（消费本 session 的 `ConnectorColumn` invisibility）→ commit-bridge（translator dual-mode + executor 改道 connector tx + V3 DeleteFile seam）。**注：PR squash-merge，故 step 1+2 单独 commit 无碍「1 coherent commit」终态**。

## C3b-core 实现起步指引（必读顺序）
1. **读 `tasks/designs/P6.6-C3-ws-write-design.md` §10**（session-3 全量设计：§10.1 recon CL-1~4 / §10.2 commit-bridge=Option(a) / §10.3 plan-time 工作集含 §9.5 修正 / §10.4 dual-mode+测试 / §10.5 工作分解 / §10.6 开放项）。**§9 仍参考但被 §10 superseded 处以 §10 为准**。
2. **§10.6 开放项 impl 期首核**（动码前先证）：**O1** 连接器 `IcebergWritePlanProvider.planWrite`(MERGE/DELETE) 的合成 `$operation`/`$row_id` 列索引从何来（engine 经 handle 传 vs 连接器独算）；**O2** post-flip scan 走 `PluginDrivenScanNode` → V3 DeleteFile 收集源 `IcebergScanNode` 是否还在（bridge 最深点）；**O3** `getWritePartitioning` plan-time（分布）可同步取（连接器 catalog live）；**O4** format-version 中立信号落点 + v3 lineage uniqueId carrier。
3. **建议实现顺序（§10.5；coupled 必须一个 commit、但 sub-step 内逐 green）**：
   - **✅ step 1+2 DONE（本 session）**：`ConnectorColumn` invisibility 标志 + converter ✅；`ConnectorWritePartitionField`/`Spec` carrier + `getWritePartitioning` default-null + iceberg override ✅。
   - **③-infra 余（待）**：中立 **format-version** 信号（O4）+ **ctx 中立重命名**（`icebergRowIdTargetTableId`→中立，~8 站点+`IcebergDDLAndDMLPlanTest`；故意与消费方同 session 做避免 orphan）。
   - **耦合核心（待）**：① `handles` 泛化 + 全 cast 放宽 + `getIcebergPartitioning` dual-mode helper（**消费本 session 的 `getWritePartitioning`**：pre-flip native walk / post-flip carrier→`DistributionSpecMerge.IcebergPartitionField(transform,exprId,param,name,sourceId)`，本地 name→exprId）+ ③ `PluginDrivenExternalTable.getFullSchema`/injector 注入（**消费本 session 的 `ConnectorColumn.invisible()`**）+ **pre-flip parity 回归 byte-identical**。
   - **commit-bridge（待）**：translator `visitPhysicalIcebergMergeSink/DeleteSink` dual-mode 路由 + executor 经 connector `ConnectorTransaction` + V3 DeleteFile iceberg-only seam。
4. **每 sub-step green + mutation**（Rule 9/12，dormant 路易 trivially-pass 必变异验真）；**import-gate PASS**；新 SPI grep 无 `org.apache.iceberg`；**`SPI_READY_TYPES` 切忌动（C5 才动）**。全 green 才 commit（1 个 coherent commit）。

## ⚠️⚠️ 用户铁律（C2 确立）：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经 SPI。**C3b-core 新代码全用中立能力/描述符/carrier**；保留的 `PhysicalIcebergMergeSink`/`DeleteSink`/`IcebergRowLevelDmlTransform`/`Iceberg*Command`/`IcebergNereidsUtils`/`IcebergTransaction` = **legacy 豁免**（dual-mode helper + `instanceof IcebergExternalTable` pre-flip 分支落这些豁免类合法）；V3 DeleteFile seam 的 `instanceof IcebergConnectorTransaction` downcast 在 finalize 钩子（iceberg-only seam，不入中立 SPI）。

---

# 🔴 C3b-core 设计要点速查（详 §10）

- **commit-bridge = Option (a)**：post-flip `PhysicalIcebergMergeSink/DeleteSink` 保留在 nereids 级（合成列+分布=Option C），但 `visitPhysicalIcebergMergeSink/DeleteSink` **dual-mode**：pre-flip 自建 fe-core thrift sink（byte-identical）；post-flip 建 `PluginDrivenTableSink`+连接器 `planWrite`（`WriteOperation=MERGE/DELETE`，触发 `beginWrite` 载 native 表，连接器产 byte-identical `TIcebergMergeSink/DeleteSink`）。DML executor post-flip = plugin-driven（开 connector `ConnectorTransaction` + 经 `RowLevelDmlCommand` SPI commit）；**legacy `IcebergTransaction`/`IcebergDeleteExecutor`/`IcebergMergeExecutor` 仅 pre-flip**。
- **② Option A**：新出向 carrier `ConnectorWritePartitionField`（`connector.api.write`，仿 `ConnectorWriteSortColumn`；**非**扩 inbound `ConnectorPartitionField`）= {transform, param, sourceColumnName, fieldName, sourceId}+specId；`ConnectorWritePlanProvider.getWritePartitioning(session,handle)` default null；iceberg override 从 native spec 算（同 `buildMergeSortFields`）；fe-core dual-mode helper `getIcebergPartitioning` 替 `PhysicalIcebergMergeSink.buildInsertPartitionFields`（pre-flip native walk / post-flip carrier，本地 name→exprId 构 `DistributionSpecMerge.IcebergPartitionField(transform,exprId,param,name,sourceId)`）。
- **③ row-id 双注入**：(1) STRUCT `__DORIS_ICEBERG_ROWID_COL__`（请求级 gate）+ (2) v3 lineage `_row_id`/`_last_updated_sequence_number`（format-version≥3 gate）。STRUCT **已能过 SPI**（`ConnectorType.structOf`+`ConnectorColumnConverter`）；缺 `ConnectorColumn` visibility 标志（仿 `withTimeZone` 范式）+ 中立 format-version 信号 + ctx 中立重命名（`icebergRowIdTargetTableId`→中立，~8 站点+1 测试，0 行为）+ `PluginDrivenExternalTable.getFullSchema/needInternalHiddenColumns` override 动态注入 + `IcebergRowIdInjector:159` guard 放宽。
- **① handles 泛化**：`IcebergRowLevelDmlTransform.handles:69`（唯一 live gate）→ `instanceof IcebergExternalTable || (PluginDrivenExternalTable && connector.supportsDelete()||supportsMerge())`，模板 `InsertOverwriteTableCommand.allowInsertOverwrite:320-329`。per-op 命令 guard 死、无需动；transform 4 cast lockstep。

---

# 🔴🔴 开放问题 — P6.6 翻闸（C1+C2+C3a+C3b-pre 已闭，C3b-core 设计闭、实现待办，C4–C5 待办）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP）。

- **[C1 ✅]** sys 表时间旅行 pin-feed（[D-068]）。
- **[C2 ✅]** 合成列读路径 classifyColumn SPI 化（[D-069]）。
- **[C3a ✅]** INSERT OVERWRITE 能力 + 完整接通 @branch（[D-070]，闭 DV-T06-branch）。
- **[C3b-pre ✅]** partition_columns emit + ④b parallel-write（[D-071]，Option A 随机真 parity）。
- **[C3b-core 设计 ✅ / 实现待办]**（[D-072]）：① handles + ②[Option A getWritePartitioning] + ③ row-id 双注入 + commit-bridge[Option(a) 路由连接器]，1 个 coherent commit。详 §10。
- **[C4 待办，最重]** rewrite_data_files 执行半接 PluginDriven（连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册去 `IcebergExecuteActionFactory:89` throw）；唯一深水 = per-group 读端子设计。`IcebergRewriteDataFilesAction:173,196` 翻闸即 CCE（不能纯 defer）。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory:137 case` / `pluginCatalogTypeToEngine` iceberg / **GSON compat 8 catalog+db+table** / mvcc+partition-stats capability 核 / Show* parity。**C5 是唯一不可逆点**；C5 前须 C1–C4 全绿 + 用户二签。

## 🆕 C2/C3 recon 挖出的翻闸前置项（RFC 漏，已登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类 `PluginDrivenMvccExternalTable`（与 paimon 同类）掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`（`:58-63` 精确类白名单）→ iceberg lazy-top-N 静默失效。修须 capability/engine 判别（非 class）。
- **[GAP-B = C3b-core ③]** 隐藏列注入：翻闸后 `PluginDrivenExternalTable` 不注入 row-id/v3-lineage → DML 绑定失败/show_hidden 不暴露/v3 空。C3b-core ③ 处理（§10.3）。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049。C1/C2/C3a/C3b-pre 无新 DV。**C3b-core 预计 0 新 DV**（dormant + pre-flip byte-identical parity；commit-bridge 是 post-flip 接通、pre-flip 走 legacy 短路）。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**P6.4 T07–T09 + P6.5 T01–T08 + P6.6-RFC + C1 + C2 + C3a + C3b-pre + 本 session C3b-core 设计 已 commit / 未 push**（确切数 `git rev-list --count origin/catalog-spi-10-iceberg..HEAD`）。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 = ✅ DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core 设计 ✅ → C3b-core 实现待办**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。

## 本 session 完成 — recon/设计细节（补充顶部 §12 的 step 1+2 摘要）

- **2 对抗 recon wf**：`wf_77a255c5-ef9`（6-slice 锚点核 + 2 adversarial verify，§9.5 cast 全量核对 + translator order TRUE + native-table-access 死路 TRUE）；`wf_e9e5f1a7-00b`（4-slice commit-bridge，1 slice API-overload 失败但问题被其余 3 回答）+ 主 session 亲核 classloader/executor-txn/sink-translator 全链。
- **用户 AskUserQuestion 三裁**：②=完整中立 SPI（A）/ commit-bridge=现在做 / 不拆。
- **决定性发现**：classloader 隔离推翻「暴露 native 表」前提；连接器 commit 机器（`IcebergConnectorTransaction` ~1010 行）已全建好；fe-core 通用 row-level DML SPI commit 链路已 dormant → bridge=Option(a) 路由 DML 提交到连接器。
- **产出**：① doc — 设计 §10（全量 + §10.5 工作分解 + §10.6 开放项 O1-O4）+ §8 TODO + decisions-log [D-072]（commit `0e3cd0a6634`）；② 码 — **C3b-core step 1+2**（② SPI + ConnectorColumn invisibility，详 §12；本 commit）。**0 新 DV、`SPI_READY_TYPES` 未改、dormant**。

---

# 🗺️ 代码脚手架（iceberg read/sys/write，全 DONE/dormant + C3b-core 实现锚点）

- **C3b-core 实现锚点（待动；行号见 §10.3 / §9.5，impl 期先 re-grep 防漂移）**：
  - ① `IcebergRowLevelDmlTransform.handles:69`（+transform cast :74/90/110/166）；模板 `InsertOverwriteTableCommand.allowInsertOverwrite:320-329`+helper `:338-342`。
  - ② sink cast：`PhysicalIcebergMergeSink`（2 ctor + with* :101/114/122/129 + **:188 冗余删** + **:271/273 唯一真 native = `getIcebergTable()` → dual-mode helper**）/`DeleteSink`/`TableSink`（零 native，全删）；`Iceberg*Command` 3 reachable DB cast（Delete:219/Update:240/Merge:464）；executor（Delete 57/74/88/100、Merge 47/62/76/88、Insert 42/52/57，txn 层已 ExternalTable）；新 carrier `ConnectorWritePartitionField`+`ConnectorWritePlanProvider.getWritePartitioning`。
  - ③ `ConnectorColumn`（加 visibility 标志，仿 `withTimeZone`）+`ConnectorColumnConverter`+中立 format-version 信号；`ConnectContext.icebergRowIdTargetTableId:290`/`needIcebergRowIdForTable:1128`（中立重命名 ~8 站点+`IcebergDDLAndDMLPlanTest`）；`PluginDrivenExternalTable.getFullSchema`(base `ExternalTable:176`)/`needInternalHiddenColumns:181` override；`IcebergRowId.createHiddenColumn`（STRUCT 定义）/`IcebergUtils.appendRowLineageColumnsForV3`；`IcebergNereidsUtils.IcebergRowIdInjector:159` guard。
  - bridge：translator `visitPhysicalIcebergMergeSink:601-627`/`visitPhysicalIcebergDeleteSink:588-598` dual-mode（post-flip→`PluginDrivenTableSink`，仿 `visitPhysicalConnectorTableSink:629-681`）；`PluginDrivenInsertExecutor.beginTransaction:74`/`finalizeSink:91`+`getConnectorTransactionOrNull:86` 范式；`RowLevelDmlCommand.run:98-100` SPI commit 链；连接器 `IcebergConnectorTransaction`（已全建）；`IcebergRewritableDeletePlanner.collectForDelete/Merge`（V3 DeleteFile，iceberg-only seam）。
- **partition_columns + parallel-write（C3b-pre，dormant）**：连接器 `IcebergConnectorMetadata.buildTableSchema` emit `partition_columns` CSV（current-spec 源列名 lowercased，legacy `loadTableSchemaCacheValue` 语义）+ `IcebergConnector.getCapabilities` +`SUPPORTS_PARALLEL_WRITE`。
- **写 @branch + INSERT OVERWRITE（C3a，dormant）**：SPI `ConnectorWriteOps.supportsWriteBranch()`+`ConnectorWriteHandle.getBranchName()`；连接器 `supportsInsertOverwrite/supportsWriteBranch=true` + `IcebergWritePlanProvider:197`；fe-core `PluginDrivenInsertCommandContext.branchName` + 透传 + 两 guard 泛化。
- **合成列分类（C2，dormant）**：`ConnectorColumnCategory` + `ConnectorScanPlanProvider.classifyColumn` + `PluginDrivenScanNode.classifyColumn` + `IcebergScanPlanProvider.classifyColumn:222`（已 SPI 化 row-id/lineage 三名 scan-side identity）。
- **sys 时间旅行 pin-feed（C1，dormant）**：`PluginDrivenScanNode.pinMvccSnapshot`。
- **连接器**：`IcebergConnector`〔`getOrCreateCatalog` 自有 live native catalog / `getWritePlanProvider`〕/`IcebergConnectorMetadata`〔`supportsDelete:469/supportsMerge:474/supportsInsertOverwrite:492/supportsWriteBranch:508` / `beginTransaction:455`〕/`IcebergConnectorTransaction`〔INSERT/OVERWRITE/DELETE/UPDATE/MERGE commit 全建〕/`IcebergWritePlanProvider`〔`planWrite`/`beginWrite`/`buildMergeSortFields`〕/`IcebergScanPlanProvider`/`IcebergCatalogOps`。
- **fe-core legacy 豁免（pre-flip 路 + P6.7 删）**：`datasource/iceberg/IcebergTransaction`〔beginDelete/Merge/Insert 已 ExternalTable 签名 / getIcebergTable post-flip 死〕 + `IcebergDeleteExecutor`/`IcebergMergeExecutor`/`IcebergInsertExecutor` + `IcebergNereidsUtils` + `IcebergRewritableDeletePlanner` + `IcebergExternalTable.getFullSchema:293-303`。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard/scan/sink/command/executor/transaction=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`；单类/多类 `test -Dtest=A,B`。**fe-core -am 单类/多类 ~2.5min**。后台 task 通知的 exit code 是 echo 的非 maven 的，读 surefire XML。fe-core 构建尾常有 antlr `->`/`super::` "mismatched input" 噪音（exit 0、测全绿），非本改动。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `catalog|common|datasource|qe|analysis|nereids|planner`）。**连接器须本地字面量复制 Doris 常量**（如 `__DORIS_ICEBERG_ROWID_COL__`）+ fe-core contract UT pin 值防漂移（C2 范式）。**classloader 隔离**：native `org.apache.iceberg.Table` 跨连接器↔fe-core 必 CCE（边界只走中立 SPI 类型）。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔`CALLS_REAL_METHODS` + stub 包私 seam〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow 测易 trivially-pass → 必变异验真（C2/C3a/C3b-pre 已逐条做）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔勿混淆，真文件在 `fe/fe-connector/...`〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。本 session **2 commit**：
  - **commit A（doc，已提交 `0e3cd0a6634`）**：`plan-doc/tasks/designs/P6.6-C3-ws-write-design.md`〔+§10〕· `plan-doc/decisions-log.md`〔+D-072〕· `plan-doc/HANDOFF.md`。
  - **commit B（step 1+2 码，本次）= 6 改 + 2 新 + HANDOFF**：connector-api `connector/api/write/ConnectorWritePartitionField.java`〔新〕·`.../write/ConnectorWritePartitionSpec.java`〔新〕·`.../write/ConnectorWritePlanProvider.java`〔+getWritePartitioning default〕·`connector/api/ConnectorColumn.java`〔+invisible()/isVisible()〕；connector-iceberg `connector/iceberg/IcebergWritePlanProvider.java`〔+getWritePartitioning override+parseTransformParam〕·`src/test/.../IcebergWritePlanProviderTest.java`〔+3 UT+bucket helper〕；fe-core `datasource/ConnectorColumnConverter.java`〔+setIsVisible 重应用〕·`src/test/.../ConnectorColumnConverterTest.java`〔+2 UT〕；+ `plan-doc/HANDOFF.md`。
- commit A message（已用）。**commit B message** `[refactor](catalog) P6.6 iceberg: C3b-core step 1+2 — ② getWritePartitioning SPI + ConnectorColumn invisibility（additive dormant；TDD+mutation；26/13 UT+556 套件绿）` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计/RFC 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep+实证）再信文档。**本 session 又一次：C3 设计的「executor→txn typed IcebergExternalTable」错（早已 ExternalTable）；「暴露 native 表」用户前提物理不可行（classloader 隔离）；连接器 commit 机器已全建（设计未察）**。
- **🆕 用户铁律：fe-core 不得 `if (iceberg)` / `instanceof Iceberg*`（新 seam）** —— iceberg 逻辑落连接器经 SPI；dual-mode helper 的 `instanceof IcebergExternalTable` pre-flip 分支落 legacy 豁免类合法。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗 + 先 code 独立判断、后交叉核历史结论。
- **C3b-core 是大活、coupled atomic commit、跨 session**：fresh session 全 budget 做；逐 sub-step green+mutation；全 green 才 1 个 coherent commit；**C5 前切忌动 `SPI_READY_TYPES`**。impl 首核 §10.6 开放项 O1-O4。
