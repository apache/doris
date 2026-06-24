# 设计偏差日志

> **Append-only**：实施中发现原计划/RFC 设计**不可行 / 不必要 / 需要重新设计**时记入本文件。
> 与"决策"的区别见 [README §3.1](./README.md)：
> - 决策（D-NNN）= **事前**确定的选择
> - 偏差（DV-NNN）= **事后**对原计划的修正
>
> 编号规则：`DV-NNN` 三位数字，从 001 起单调递增，永不复用。
>
> 维护规则见 [README §4.3](./README.md)：**先记偏差再改文档**，不要 silent edit。

---

## 📋 索引

> 时间倒序；当前共 **49** 项（最新 DV-049；本轮 P6.5-T07 对抗 byte-parity 审计〔8 area finder + refute-by-default skeptic + completeness critic，22 finding / 19 confirmed〕把 P6.5 sys-table 残留 deviation 批化中央登记为 DV-048〔correctness-bearing〕/049〔perf-cosmetic/display/internal〕——**审计揭出的 2 项主动偏差〔sys 时间旅行 guard 拒绝 + hms 大小写〕用户裁「现修」故 NOT-DV，见 [D-067]**；二层镜像 P6.4-T08 DV-045..047）。

| 编号 | 偏差主题 | 原计划位置 | 日期 | 当前状态 |
|---|---|---|---|---|
| DV-049 | **P6.5 iceberg sys-table perf-cosmetic/display/internal 批汇总**（结果恒等/展示/内部枚举；镜像 DV-047/044 style）：**①sys split self-weight 丢**〔audit `T05-sys-split-weight`：legacy `IcebergScanNode.createIcebergSysSplit:900`+`IcebergSplit.newSysTableSplit:78` 设 `selfSplitWeight=Math.max(recordCount,1L)`；连接器 `IcebergScanRange` 无 `getSelfSplitWeight()` override → SPI 默认 -1 → `PluginDrivenSplit` `SplitWeight.standard()` 均匀。**result-equivalent**——查询结果/thrift 字段/serialized_split 字节皆不变，仅 BE `FederationBackendPolicy` 调度权重差；镜像 [DV-033] native-subsplit weight 不移植〕·**②内部 `TableIf.TableType=PLUGIN_EXTERNAL_TABLE`**〔legacy `IcebergSysExternalTable:57`=`ICEBERG_EXTERNAL_TABLE`〕——但 **user-visible 全 parity**：`getMysqlType`→`information_schema.tables.TABLE_TYPE` 两枚举皆 fall-through "BASE TABLE"〔`TableIf:319/324-325`〕、engine name 经 [D-066] T06-F1="iceberg"、descriptor 经 T06-C1=HIVE/ICEBERG_TABLE → 残留**仅内部枚举**〔无 user 可观测面〕·**③position_deletes 文案**〔Q2 用户签字：连接器 `listSupportedSysTables` 去 `POSITION_DELETES`+`getSysTableHandle`→empty → 通用 fe-core not-found（"Unknown sys table"）vs legacy bespoke "is not supported yet"（`IcebergSysTable:74`）；两侧 support boolean 同〔皆不可查〕仅文案分叉〕。全部**非正确性** | T07 对抗审计 / [task 表 §P6.5](./tasks/P6-iceberg-migration.md) | 2026-06-25 | 🟢 已登记（accept；结果恒等/内部/展示；P6.6 docker/live 真值闸）|
| DV-048 | **P6.5 iceberg sys-table correctness-bearing 但 UT 不可见**（parity-by-construction / 用户签字；docker 闸）：**①F2 paimon SHOW CREATE priv loosening（LIVE）**〔audit `F2-paimon-showcreate-priv-loosened-live` + critic：[D-066] T06-F2 给 `ShowCreateTableCommand.validate():120-124` 加 `PluginDrivenSysExternalTable`→`getSourceTable().getName()` 解包；**对 iceberg 是 parity**〔legacy `IcebergSysExternalTable` 分支 `:118-119` 已解包，且 iceberg pre-flip dormant〕，但 **paimon 在 SPI_READY_TYPES** → live 行为变更：sys `$`-表 SHOW CREATE 现按 **base** 表授权（pre-T06 按合成 'tbl$snapshots' 名）；严格**更宽松**、同向，与 `Env.getDdlStmt`/`UserAuthentication` 既有 output 解包一致、破坏风险近零；用户 T06-Q2 签字。**⚠️ 无隔离 UT**〔`validate()` 依赖 `Env`/`ConnectContext`/`AccessManager` 全局单例〕→ P6.8 e2e 兜底〕·**②serialized `FileScanTask` 字节潜伏（T05）**〔FE `SerializationUtil` deserialize-round-trip UT 已在**同进程 iceberg 1.10.1** 核「可消费+asDataTask+meta schema」，但与 BE `IcebergSysTableJniScanner` 的**跨版本/classloader interop** FE 不可及 → P6.8 docker e2e 兜底〕。**别于 DV-041**：本条**已建待 docker 实证**，DV-041 是未接线翻闸阻塞 | T07 对抗审计 / [task 表 §P6.5](./tasks/P6-iceberg-migration.md) | 2026-06-25 | 🟢 已登记（accept；P6.6 docker/Kerberos 真值闸）|
| DV-047 | **P6.4 iceberg procedures perf/cosmetic/behaviour-equiv/dispatch-order 批汇总**（结果恒等/dormant/接缝/幂等；镜像 DV-044 style）：cache 失效搬 dispatch+短路多失效〔仅 context!=null〕·executeAction 加 ConnectorSession 参〔内部接缝〕·**DV-T08-loadwrap**〔新 "Failed to load iceberg table" 串,引擎再裹 "Failed to execute action:"〕·**DV-T08-factory-advertise**〔rewrite_data_files 广告-但-`createAction` 拒,dormant,canary UT 钉〕·DV-T06r-{scanpool〔丢 scanManifestsWith〕,zone〔复用 DV-T04-f〕,rollback〔不清列表中性〕}·DV-T07-{where〔WHERE 拒 fail-loud dormant〕,name-order〔priv-first 有意发散〕,exc-contract〔IllegalStateException 逃逸 byte-parity 边界〕}·PARTITION(*) 拒不对称〔low,dormant〕·null-row 编码〔low〕·per-conjunct filter 形状〔结构等价〕。全部**非正确性** | T04/T06/T07 设计 §10 / [task 表 §P6.4](./tasks/P6-iceberg-migration.md) | 2026-06-24 | 🟢 已登记（accept；结果恒等/展示/dormant/接缝；P6.6 docker 真值闸）|
| DV-046 | **P6.4 iceberg procedures correctness-bearing 但 UT 不可见**（parity-by-construction 或 dormant+用户签字；docker/Kerberos 闸）：**auth-add**〔8 snapshot mutator 的 loadTable+commit 现裹 `executeAuthenticated`〔仅 context!=null〕,legacy 缺=潜伏 Kerberized auth bug,加非丢〕·**DV-T05r-where**〔rewrite_data_files WHERE 走 conflict-mode:不可转节点静默丢〔legacy 抛〕→ planner scan 变宽/极端全表 + conflict-matrix 收窄;rewrite 语境 over-approx **不安全**〔安全性反号 vs O5-2〕;conflict-matrix 是 legacy 严格子集→只变宽绝不反向;用户签字 Option A;**经 EXECUTE 派发不可达**〔双闸:factory 无 case〔DV-T08-factory-advertise〕+ WHERE 拒〔DV-T07-where〕→ DV-045 R-B 接线后才激活〕〕。别于 DV-045=未接线翻闸阻塞 | T04/T05 设计 §10 / [task 表 §P6.4](./tasks/P6-iceberg-migration.md) | 2026-06-24 | 🟢 已登记（accept；P6.6 docker/Kerberos 真值闸）|
| DV-045 | **P6.4 `rewrite_data_files` 执行半翻闸阻塞 BLOCKER**（R-B 推后专门写路径 RFC；与 DV-041 写路径阻塞同族）：① 事务半〔连接器 `WriteOperation.REWRITE` 变体,T06 已建 dormant 净0新verb〕+ 规划半〔`RewriteDataFilePlanner`,T05 已建 dormant〕；**②③④ 执行半留 fe-core**〔`RewriteDataFileExecutor`/`RewriteGroupTask`/`RewriteTableCommand`/`IcebergRewriteExecutor`〕。recon 证伪设计 §5/D-062 R-A「pinned snapshot+WHERE 重规划」前提〔连接器 scan SPI 无法表达 bin-pack「分区内任意文件子集」→ over-scan 破 rewrite 正确性;`FileScanTask` 侧信道翻闸后死;SPI 模块边界禁连接器 carrier 跨进 fe-core;multi-sink-per-txn 生命周期重设计〕。用户裁 Option 1（2026-06-24）| T05/T06 设计 §5 / [task 表 §P6.4 T06](./tasks/P6-iceberg-migration.md) / [HANDOFF 🔴🔴](./HANDOFF.md) | 2026-06-24 | 🔴 翻闸前(P6.6)必接线（专门写路径 RFC）|
| DV-044 | **P6.3 iceberg 写路径 perf/cosmetic/EXPLAIN-diff/等价结构 批汇总**（结果恒等；镜像 DV-040/DV-035 style）：jdbc txn 全局注册生命周期变更·writeOperation 移 T03/beginTransaction throwing 默认（DV-T01-b/c）·jdbc EXPLAIN 头标签 `WRITE TYPE:JDBC_WRITE`→`WRITE:plan-provider`（窄化，INSERT SQL 经 appendExplainInfo 保，DV-T02-b）·appendExplainInfo EXPLAIN 期读元数据（净优于 legacy 每-INSERT 查，DV-T02-c）·异常型 `DorisConnectorException`/`IllegalStateException`（消息字节同，DV-T03-a/T04-b/T05-b/T06-c）·`scanManifestsWith` 丢→SDK 默认池（DV-T04-a/T05-f）·partition_data_json Jackson vs Gson（DV-T04-d）·单 `beginWrite`+`commit` switch（DV-T04-e）·显式 ZoneId 形参（DV-T04-f/T05-d）·O5-2 惰性转/私有 formatVersion 重复（DV-T05-a/e）·double loadTable 只读重 I/O（DV-T06-d）·新 `getBackendFileType`/`getWriteSortColumns` SPI 接缝 + `SINK_REQUIRE_FULL_SCHEMA_ORDER`（DV-T06-e）·EXPLAIN sink 标签 `PLUGIN-DRIVEN TABLE SINK` vs `ICEBERG TABLE SINK`（plan-shape 不变，OQ-3）·jdbc thrift 移位（OQ-1→DV-T02-a 实现）。全部**非正确性** | T01–T07 设计 §6 / [task 表 §P6.3](./tasks/P6-iceberg-migration.md) | 2026-06-24 | 🟢 已登记（accept；结果恒等/展示/性能；P6.6 docker 真值闸）|
| DV-043 | **P6.3 iceberg 写路径 parity-忠实 correctness-bearing 但 UT 不可见 批汇总**（parity-by-construction / widening-safe，各有 P6.6 docker 闸）：jdbc affected-rows `-1` 哨兵 + `DPP_NORMAL_ALL`（BE 真实计数离线不可验，DV-T01-a）·jdbc `TJdbcTableSink` thrift 由 fe-core 移连接器 `planWrite`（OQ-1 字节-parity 移位，§4.1 逐字段 + UT，DV-T02-a）·`beginWrite` 的 `newTransaction()` auth-wrap（Kerberized HMS `doRefresh`，离线 InMemoryCatalog 不可分辨，DV-T03-d）·TIMESTAMP/identity 分区值连接器-本地解析 + 显式 zone（BE canonical fmt，DV-T04-c）·`IcebergPredicateConverter` conflict-mode 丢不可转/NullSafeEqual/Cast/col-col/NE → 冲突 filter **widens**（no-missed-conflict 安全，忠实 legacy 冲突路 Option A，用户签字 2026-06-24；DV-T05-c/T07b-matrix/T07b-literal）·连接器 hadoopConfig 经 fe-filesystem `toHadoopConfigurationMap` vs legacy fe-core（默认口径微差，P6.6 docker 断字节，DV-T06-hadoopconfig）。**别于 DV-041**：本条**已修待 docker 实证**，DV-041 是**未接线翻闸阻塞** | T01–T07 设计 §6 / [task 表 §P6.3](./tasks/P6-iceberg-migration.md) | 2026-06-24 | 🟢 已登记（accept；P6.6 docker 真值闸必逐项验）|
| DV-042 | **P6.3 北极星 (iii) 有界架构偏差：iceberg DML plan 合成 fe-resident（Route B / option (i)，PMC 签字）**：iceberg DELETE/UPDATE/MERGE 的 plan 合成（`$row_id` 注入 / branch-label 投影代数 / nereids→iceberg expr）**暂留 fe-core**，经连接器-键控 `RowLevelDmlTransform` 注册表调用；合成内反向 `instanceof IcebergExternalTable`。**有界 intentional**——保 EXPLAIN parity，**只在北极星 (iii) 通用化**（Trino 式：连接器 0 优化器 import、引擎核心全 DML 合成）**关闭**，触发 = 第二个行级-DML 消费者（hive P7 / paimon），后续专门 RFC。含等价结构项：T07c 冲突-filter 顺序（provably-equivalent reorder）/单 table resolve（删 legacy 冗余 re-resolve）/`IcebergXCommand.run()` 循环 transitional-dead（P6.7 随类删）·conflict-mode 合成列排除经注入 Predicate（DV-T07b-exclusion）·`rewritableDeleteFileSets` 经 T07c executor finalize seam（DV-T07-rewritable）| RFC §5.3/§10 / T07 设计 §4.5.8 / [task 表 §P6.3](./tasks/P6-iceberg-migration.md) | 2026-06-24 | 🟢 已登记（accept；有界、PMC 签字、保 EXPLAIN parity；北极星 iii 后续 RFC 关闭）|
| DV-041 | **P6.3 写路径翻闸阻塞 BLOCKER：通用 `visitPhysicalConnectorTableSink` 缺合成列物化 + 分布（DV-038 同主题新面）+ 休眠-至-翻闸激活集**。**主阻塞（DV-T07-materialize）**=通用 `visitPhysicalConnectorTableSink` 无合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` 分布，仅 legacy `visitPhysicalIcebergMergeSink` 有 → iceberg DELETE/MERGE 经通用 sink 真正走通前须先长出，否则上游列被丢 → BE `iceberg_reader.cpp` StructNode DCHECK（**同 DV-038 崩溃类**）；T07 有意不碰 `PhysicalPlanTranslator:589-627`。**休眠-至-翻闸激活集（P6.6 必接线，全有或全无）**：写分布 `getRequirePhysicalProperties` 分区-hash 延后（DV-T06-a）·branch-INSERT thread-through（DV-T06-branch）·REST 对象存储 vended overlay（不接翻闸后 403，DV-T07-vended）·O5-2 `getConnectorTransactionOrNull()`→null 休眠（翻闸激活，DV-T07c-o5seam）·FILE_BROKER 地址（DV-T06-broker/T07-broker）| T06 §6 / T07 §1.1/§6 / RFC §5 / [task 表 §P6.3](./tasks/P6-iceberg-migration.md) | 2026-06-24 | 🔴 翻闸前（P6.6）必修/必接线（与 DV-038 同 holistic）|
| DV-040 | **P6.2 iceberg scan perf/observability/EXPLAIN-drop + lenient-validation + benign superset 批汇总**（~36 项，镜像 DV-035 style）：profile/`planWith` drop·manifest 统计 drop·空表 COUNT EXPLAIN `(-1)`·COUNT `>10000` 并行 trim drop·typed-vs-string carriers·per-file format·Jackson vs Gson·predicate over-approx（reversed/IsNull/Like/Between/LARGEINT/edge-literal，BE residual 兜底）·ZoneId alias-map·`delete_files` unset·fail-loud 异常型·`Locale.ROOT`·INCREMENTAL fail-loud·TIMESTAMP epoch-millis·vended `io().properties()`/非-fail-soft/PROVIDER_CHAIN gap·**🔵 split-package shadowing**（vendored `DeleteFileIndex` 与 iceberg-core 共存，T08 extractor 漏报本条补登，跨引用 P6.1 R-004/#973270）。全部**非正确性**（结果恒等/展示/源不同值同/安全超集/BE 兜底） | T02–T09 设计 / [T11 汇总](./designs/P6-T11-iceberg-scan-summary-design.md) | 2026-06-23 | 🟢 已登记（accept；P6.6 docker 真值闸）|
| DV-039 | **P6.2 iceberg scan parity-忠实 HIGH/MEDIUM correctness-bearing 但 UT 不可见 deviation 批汇总**（已连接器内缓解、单项不阻塞翻闸、各有 P6.6 docker 闸）：HIGH=columns-from-path unset-then-set(#968880 防双填)·`isPartitionBearing()` 空分区崩修（**0-新-SPI 唯一例外**，非破坏默认）·主数据路径 `normalizeStorageUri`(path/originalPath 拆)·Option A 全 pinned-schema 字典(time-travel 防 `iceberg_reader.cpp:181` DCHECK)·静态 `location.*` 凭据发射(T09 前 403)；MEDIUM=latest-snapshot 二元组·name-mapping 回退·fail-loud 竞态窗·vended live round-trip·Kerberized `doAs`(跨引用 DV-031)·tag/branch REF-pin·1-arg normalize delete 路。**别于 DV-038**：本条**已修待 docker 实证**，DV-038 是**未修共享 fe-core 崩溃** | T03/T04/T06/T07/T08/T09 设计 / [T11 汇总](./designs/P6-T11-iceberg-scan-summary-design.md) | 2026-06-23 | 🟢 已登记（accept；P6.6 docker 真值闸必逐项验）|
| DV-038 | **P6.2 iceberg 翻闸阻塞 BLOCKER：共享 fe-core field-id 路径 BE StructNode DCHECK 崩溃（1 主题/2 面，CI #969249 类）**。**面 1**=`GLOBAL_ROWID` 被通用 `classifyColumn` 误归 REGULAR→不在 field-id 字典→`iceberg_reader.cpp` DCHECK→整 BE 崩（连接器无法修，须改共享 fe-core `classifyColumn`→SYNTHESIZED，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）。**面 2**=`getColumnHandles` 无 snapshot 重载→rename+time-travel 丢被重命名 slot field-id→同一 DCHECK（iceberg 侧 T07 Option A 已闭合，但**共享 seam 仍潜伏 PAIMON** snapshot-id time-travel+rename）。审计 critic 实证 blocker 计数=**2**（同主题），合并单条但显式记两面（Rule 12）。**P6.6 翻闸前必 holistic 修 + paimon 影响分析（可能 BE 协同）** | T06 §6 / T07 §6 / T10 audit / [HANDOFF 🔴🔴](./HANDOFF.md) | 2026-06-23 | 🔴 翻闸前必修（面 1 用户签字延后 2026-06-22 / 面 2 待 P6.6 holistic）|
| DV-037 | P6-C2 FIX-C2-HDFS-XML：legacy HDFS `getHadoopStorageConfig()` 的 `fs.hdfs.impl.disable.cache=true` 未进 typed FE Configuration（pre-existing，非 C2 引入；Hadoop FS-cache benign） | [FIX-C2-HDFS-XML-design §Risk](./designs/FIX-C2-HDFS-XML-design.md) | 2026-06-19 | 🟢 已登记（accept / 可转 follow-up）|
| DV-036 | P6-C2 FIX-C2-HDFS-XML：DLF catalog 若另绑 HDFS storage，HDFS keys 会进 DLF HiveConf（legacy DLF 只 overlay OSS/OSS_HDFS）；结果 additive/inert defaults-free，纯-OSS DLF byte-unchanged | [FIX-C2-HDFS-XML-design §Risk/Open Q1](./designs/FIX-C2-HDFS-XML-design.md) | 2026-06-19 | 🟢 已登记（accept）|
| DV-035 | P4 MINOR/NIT cleanup：**15 项 accept-as-deviation**（review §5/§7，用户签字 [D-057]，2026-06-12；2 项已修 = N10.1 `bcee91dcb52` + sentinel `4b2c2190dc2`，不在本条）。read-only 对抗 recon `wf_6884d37b-8ef` 逐项对当前代码复核。**(a) M5.1（FUNCTIONAL/transient-only）**：bridge `getSupportedSysTables` 经远端 handle 预探列 sys-table，`getTableHandle` swallow-非NotExist-为-empty → 瞬时 metastore blip 致已存在 sys-table 报 phantom「table not found」（legacy 静态无条件列）。**无 surgical 修**：swallow→empty 是有意+有测契约（`PaimonConnectorMetadataReadAuthTest:150` `failAuth→empty`）且共享 existence 谓词（含 P3 createTable `remoteExists`）；干净修需 SPI 加法或破契约。transient-only → accept。**(b) 假前提 ×2**：M9.1（HDFS `ipc.client.fallback-to-simple-auth` 等 default「丢」）、M9.2（hive.* metastore 键推 BE）——recon 证伪：连接器 `getBackendStorageProperties` 跑**同** `CredentialUtils.getBackendPropertiesFromStorageMap` over 同 storage map，无 drop。**(c) display-only**：M10.1（CREATE 嵌套 struct comment 丢）、M10.2（read isKey=false，无 planner gate，仅 DESCRIBE）、M10.3（LTZ `WITH_TIMEZONE` extraInfo 丢，仅 DESCRIBE Extra）、M7.1（`PluginDrivenScanNode` 不 override `getDeleteFiles`+不调 super→EXPLAIN VERBOSE 缺 DV/per-backend 计账，DV 仍正确达 BE）。**(d) perf-only**：M6.1（live-Table handle cache 丢，SDK CachingCatalog 仍缓）、M6.2（schema-at-snapshot 不按 schemaId 缓，结果同仅重算）。**(e) text-only**：N2.1（"Paimon"→"Plugin" 拒绝文案）、M3.1/N4.1（not-found 文案丢 earliest-snapshot hint / "Failed to get Paimon..." 前缀，条件+异常类两侧同）、C2（ALTER BRANCH/TAG 抛 `DdlException` vs `UnsupportedOperationException`，两侧都拒）。**(f) inert no-op**：N3.1（@incr 丢 `scan.snapshot-id=null` 防御性 reset，fresh base table 上 no-op）、M2.1（@incr BE-serialized table 是 incremental-window-copied，BE 只用作 read-builder/rowType 工厂、不重 plan，inert）。**(g) 连接器更 correct**：M4.1（branch schema 解析对 branch 自身 schemaManager vs legacy base 表）、M1.3（CAST 谓词不下推——除掉 legacy source-side over-prune 数据丢 bug）。**(h) diagnostic**：M1.1（`ignore_split_type` 调试 var 忽略，须 fe-core SessionVariable 类型）。跨连接器：hudi/iceberg full-adopter 多项同复发，归本条批量考量 | [task-list §P4](./task-list-P5-rereview2-fixes.md) / [D-057](./decisions-log.md) | 2026-06-12 | 🟢 已登记（accept；M5.1=transient-only FUNCTIONAL，余 display/perf/text/inert/连接器-更-correct/假前提；live-e2e 真值闸）|
| DV-034 | P3-fix FIX-CREATE-TABLE-LOCAL-CONFLICT：**plugin DDL op 把 typed MySQL error-code 收敛成 generic `DdlException`**（pre-existing 跨全 4 DDL op，P4 cleanup defer）。`FIX-CREATE-TABLE-LOCAL-CONFLICT`（[D-056]）仅恢复 createTable 的 **case-B correctness**（local-only 冲突 + `!IF NOT EXISTS`→改抛 typed `ERR_TABLE_EXISTS_ERROR` 1050），**未** retype：**case-A**（createTable remote-hit + `!IF NOT EXISTS`）仍 fall-through 由连接器（paimon `TableAlreadyExistException`）→`DorisConnectorException`→桥 re-wrap 成 generic `DdlException`「already exists」，legacy `PaimonMetadataOps:195` 在 FE 层先抛 typed 1050；**createDatabase/dropDatabase/dropTable** 同样 `catch(Exception)`→generic `DdlException`（`PaimonConnectorMetadata:731/798/832/756`+桥 re-wrap），collapse 掉 legacy 1007/1008/1109。**非本 P3 finding**（finding=case-B silent-create correctness）、P3 audit 标 error-code parity=cosmetic/AGREE（error class + user-visible「already exists」文本两侧同、仅 numeric code 丢）。修它须每 op 在桥/连接器边界统一 typed-code 透传，属跨全 op + 跨连接器（hudi/iceberg 同）的 **P4 cleanup 批量**。真值闸=无功能影响，仅 MySQL numeric-error-code-sensitive 客户端脚本理论可感知 | [task-list §P3/§P4](./task-list-P5-rereview2-fixes.md) / [D-056](./decisions-log.md) | 2026-06-12 | 🟢 已登记（cosmetic/error-code-only，pre-existing 跨全 DDL op；P4 cleanup defer）|
| DV-033 | P5-fix#9 FIX-NATIVE-SUBSPLIT：**split-weight / target-size 调度 nicety 不移植**（用户签字采纯连接器实现，2026-06-12）。legacy `fileSplitter.splitFile` 经 `splitCreator.create(...,targetFileSplitSize,...)` 在每个 `FileSplit` 上设 split weight + targetSplitSize，供 `FederationBackendPolicy` 做 backend 分配均衡。连接器 native sub-range（`buildNativeRange`）**不设** `selfSplitWeight`/targetSplitSize——但这是 **pre-existing**：翻闸后单-range native 路本就没设（`buildNativeRange` 从未设 weight，仅 JNI 路 `buildJniScanRange` 经 `computeSplitWeight` 设）。#9 **不引入**该缺口，只是把一个整文件 range 变成多个 sub-range（并行度本身已恢复，这是 #9 的目的）。纯调度均衡质量、非正确性、非并行度。连接器 SPI 无 per-range weight 喂入 FileSplit 的通道（`PaimonScanRange` 无 targetSplitSize 字段）。跨连接器：hudi/iceberg full-adopter 若要 weight-均衡可后续在 SPI/`PaimonScanRange` 加 weight 字段批量补（与既有 native-path weight 缺口一并）。真值闸=live-e2e（观察 backend 分配均衡，非正确性） | [task-list #9](./task-list-P5-rereview2-fixes.md) / [P5-fix-NATIVE-SUBSPLIT 设计](./tasks/designs/P5-fix-NATIVE-SUBSPLIT-design.md) / [D-055](./decisions-log.md) | 2026-06-12 | 🟢 已登记（perf/调度-only，pre-existing；live-e2e 真值闸）|
| DV-032 | P5-fix#8 FIX-COUNT-PUSHDOWN：**collapse-to-one 丢 legacy `>10000` 并行 count-split trim**（用户签字采 collapse-to-one，2026-06-12）。legacy `PaimonScanNode:484-495` 收齐 count-eligible split 后按 `pushDownCountSum` 分流——`>COUNT_WITH_PARALLEL_SPLITS(10000)` 时 trim 到 `parallelExecInstanceNum * numBackends` 个 split 并 `assignCountToSplits` 把 total 均摊（BE 每 split CountReader 再求和回 total）；`<=10000` 则 `singletonList(first)` 收一 split 携全 total。连接器**始终 collapse-to-one**（无论 countSum 大小），因连接器无 `numBackends`/`parallelExecInstanceNum`（fe-core scan-node-only，`getSplits(int numBackends)` 才有）。**纯 perf 偏差、结果恒等**：单 CountReader 在一个 fragment emit `countSum` 个空行（无 IO）而非 N 个并行——对超大 count 不并行化 count-emit。CountReader 不读数据故影响小。**未采 full-parity**（连接器发 per-split + fe-core 按 numBackends trim+redistribute）以避免把 count 语义耦进通用 `ConnectorScanRange` + 多 fe-core 代码。跨连接器：hudi/iceberg full-adopter 若要 `>10000` 并行可后续在 fe-core 加 trim hook（与 [DV-028]/[DV-030]/[DV-031]「新连接器读法 vs fe-core 既有约定」类缝同批考量）。真值闸=live-e2e（超大 PK 表 `COUNT(*)` 仍正确、仅观察 fragment 并行度差异） | [task-list #8](./task-list-P5-rereview2-fixes.md) / [P5-fix-COUNT-PUSHDOWN 设计](./tasks/designs/P5-fix-COUNT-PUSHDOWN-design.md) / [D-054](./decisions-log.md) | 2026-06-12 | 🟢 已登记（perf-only，结果恒等；live-e2e 真值闸）|
| DV-031 | P5-fix#6 FIX-KERBEROS-DOAS 两接受项：① **真 doAs 端到端 = live-Kerberos-e2e only**——M-8（filesystem/jdbc over Kerberized HDFS）+ M-11（Kerberos HMS read RPC）的 FE-unit 测只覆盖 **wiring**（M-8 断言 `getExecutionAuthenticator()` 返 `HadoopExecutionAuthenticator` 类型、不调 initializeCatalog；M-11 用 `RecordingConnectorContext.failAuth`/`authCount` 断言 read 经 `executeAuthenticated`），**无 paimon-kerberos regression 套件**（现有 `regression-test/.../kerberos/` 4 套仅 hive+iceberg、gated by `enableKerberosTest`）→ 真 KDC doAs 留给 live-e2e 门（翻闸前必验）。fail-safe：非 Kerberos 部署 no-op authenticator 与真 authenticator 行为一致（`ExecutionAuthenticator.execute`=`task.call()`）、无回归。② **跨连接器 follow-up**：read-vs-DDL doAs 缺口（M-11）+ 翻闸-authenticator-wiring 缺口（M-8，`initializeCatalog` 死代码）在 hudi/iceberg full-adopter **同样复发**（`cutover-fe-dispatch-gap` 姊妹）；与 [DV-028]（#4 CREATE-time-only 校验）/[DV-030]（#5 mapping-flag 键）同属「新连接器读法/翻闸 vs fe-core 既有约定」类缝，将来可批量 close。**M-8 新增 fe-core `MetastoreProperties.initExecutionAuthenticator` hook 是 fe-core 内部扩展、非连接器 SPI**（`ConnectorContext`/`Connector` 表面未改）→ 01-spi-extensions-rfc.md 无须改 | [task-list #6](./task-list-P5-rereview2-fixes.md) / [P5-fix-KERBEROS-DOAS 设计](./tasks/designs/P5-fix-KERBEROS-DOAS-design.md) / [D-052](./decisions-log.md) / [D-053](./decisions-log.md) | 2026-06-11 | 🟢 已登记（live-e2e 真值闸 + 跨连接器 follow-up）|
| DV-030 | P5-fix#5 FIX-MAPPING-FLAG-KEYS 跨连接器 follow-up（用户定本轮 paimon-only）：**新 hive + iceberg 连接器同根因**——读**下划线** mapping-flag 键而 fe-core 只写/读/藏**点分** catalog 键（`CatalogProperty:50,52`），`PluginDrivenExternalCatalog.createConnectorFromProperties` 喂原始 catalog map、中间无点分→下划线归一化 → 用户在 CREATE CATALOG 开 `enable.mapping.varbinary`/`enable.mapping.timestamp_tz` 对 hive/iceberg 亦**静默失效**（BINARY→STRING、LTZ→DATETIMEV2）。**iceberg** = `enable_mapping_varbinary`/`enable_mapping_timestamp_tz`（`IcebergConnectorProperties:46,47`→`IcebergConnectorMetadata:151,154`），仅分隔符差、语义不反转。**hive** = `enable_mapping_binary_as_string`/`enable_mapping_timestamp_tz`（`HiveConnectorProperties:52,53`→`HiveConnectorMetadata:317,319`），binary 键既改分隔符又改 token，但 `binary_as_string` 是**误名非语义反转**（`HmsTypeMapping:90-93` true→VARBINARY，喂 `mapBinaryToVarbinary` 字段）。JDBC 是唯一正确的新连接器（点分）。legacy hive/iceberg 经 `getCatalog().getEnableMappingVarbinary()` 读点分（`HMSExternalTable:791`/`IcebergUtils:1083`）→ 翻闸回归。**用户签 [D-051] = 本轮只修 paimon**（保 commit surgical、单任务）；**follow-up（close 时）**：hive+iceberg 两常量重指 canonical 点分键（hive `binary_as_string` token 复原为 `varbinary`，**勿**反转 boolean）+ 各加 dotted-key honor UT；与 paimon #5 同形修。scope 经验证 workflow `wf_a3626c54-0db`（g5 + synthesizer，静态 trace 未 live） | [task-list #5](./task-list-P5-rereview2-fixes.md) / [P5-fix-MAPPING-FLAG-KEYS 设计](./tasks/designs/P5-fix-MAPPING-FLAG-KEYS-design.md) / [D-051](./decisions-log.md) | 2026-06-11 | 🟡 待修（跨连接器 follow-up，用户定本轮 paimon-only）|
| DV-028 | P5-fix#4 FIX-JDBC-DRIVER-URL：driver_url 安全校验**仅 CREATE CATALOG**（`PaimonConnector.preCreateValidation`→`ConnectorValidationContext.validateAndResolveDriverPath`），**FE-restart reload / ALTER CATALOG / scan-time 不复校**——与 legacy 分歧（legacy `getBackendPaimonOptions`→`JdbcResource.getFullDriverUrl` 每 scan 复校 format/whitelist/secure-path）。根因 = pre-existing **fe-core 架构缝**、非本 fix/非 paimon 专属：`CatalogFactory:164` replay(`isReplay=true`) 跳 `checkWhenCreating`→`preCreateValidation` 不跑；`PluginDrivenExternalCatalog.checkProperties`(ALTER 路) 只调 `validateProperties`(无 driver 校验)、不调 `preCreateValidation`；`getBackendPaimonOptions` 仅 resolve 不 validate（连接器 scan-time 只有 `ConnectorContext`、无 driver-path 校验 hook）。**与 JDBC 参考连接器 `JdbcDorisConnector` 完全 parity**（其亦 CREATE-time-only）。**用户定接受**（[D-050]）：默认配置 permissive（`secure_path="*"`/whitelist 空）无可绕，唯一暴露 = 硬化部署后**收紧** whitelist/secure-path 又**不重建** catalog。**复评/follow-up（跨连接器）**：若需 close，须 fe-core 改（ALTER 路 `checkProperties`→`preCreateValidation`，注意会触发 JDBC 连接器的 BE 连通测）+ scan-time 校验须新 `ConnectorContext` SPI hook——影响全 plugin 连接器、独立工单 | [task-list #4](./task-list-P5-rereview2-fixes.md) / [P5-fix-JDBC-DRIVER-URL 设计](./tasks/designs/P5-fix-JDBC-DRIVER-URL-design.md) / [D-050](./decisions-log.md) | 2026-06-11 | 🟢 已登记（CREATE-time parity，用户接受+跨连接器 follow-up）|
| DV-029 | P5-fix#4 FIX-JDBC-DRIVER-URL 两 scope-out（surgical）：① 连接器 `PaimonCatalogFactory.resolveDriverUrl` 是 legacy `JdbcResource.getFullDriverUrl` 的**简化子集**——只做 scheme 解析（裸名→`file://{jdbc_drivers_dir}/{name}`），**不**做文件存在性 / legacy 旧 `jdbc_drivers/` 回退 / 云下载。常见情形（`mysql.jar`+默认 dir）两者等价；仅装旧 dir 的 jar 会 BE 找不到（pre-existing 简化、FE 注册路本就如此、复用未改）。② **BE-side `paimon.jdbc.{user,password,uri}` 别名丢弃不修**——同 `startsWith("jdbc.")` filter 也丢这些别名键，但 **BE 不需要**：`PaimonJniScanner.initTable` 从 `serialized_table` 反序列化整表、**不**从 options_json 重建 JdbcCatalog；BE 唯一消费 jdbc 选项处 `PaimonJdbcDriverUtils.registerDriverIfNeeded` 只读 driver_url/driver_class。legacy `getBackendPaimonOptions` 亦仅发 driver_url+driver_class（窄）。故 B-8a 只修 driver_url/class 即 parity（scope-critic lens LGTM 确认） | [task-list #4](./task-list-P5-rereview2-fixes.md) / [P5-fix-JDBC-DRIVER-URL 设计](./tasks/designs/P5-fix-JDBC-DRIVER-URL-design.md) / [D-050](./decisions-log.md) | 2026-06-11 | 🟢 已登记（surgical scope-out，BE 经 trace 确认安全）|
| DV-027 | P5-fix#3 FIX-SCHEMA-EVOLUTION：history_schema_info 用 **eager 全量** `SchemaManager.listAllIds()`+`schema(id)`（每 scan、**无 cache**），非 legacy 的 per-split 引用 schema 懒读+缓存（`PaimonScanNode.putHistorySchemaInfo`→`PaimonUtils.getSchemaCacheValue`）。理由：Design C 的 scan 级缝 `populateScanLevelParams` 拿不到 split 集（那是 `planScan` 才有），故无法只读引用到的 schema；listAllIds() 全集**保证**覆盖任意 native 文件的 `schema_id`（BE `table_schema_change_helper.h:259-263` 缺 entry 会 fail-loud `InternalError`，全集即杜绝）。**两点接受**：① perf——K 个 schema 版本= K 次小 JSON 读/scan（props 每 node 缓存一次、非 per-split）；② 鲁棒性微回归——某**未被引用**的 schema-N JSON 瞬时不可读会令本 scan 失败（fail-loud 传播，镜像 legacy `putHistorySchemaInfo` 不吞异常），而 legacy 因只读引用 schema 不碰它、可完成。correctness-safe（全集是 legacy 引用集的超集、绝不触发 BE InternalError）；review 评 MINOR。未来优化=引用集（需 split-aware 缝）或连接器侧 cache | [task-list #3](./task-list-P5-rereview2-fixes.md) / [P5-fix-SCHEMA-EVOLUTION 设计](./tasks/designs/P5-fix-SCHEMA-EVOLUTION-design.md) / [D-049](./decisions-log.md) | 2026-06-11 | 🟢 已登记（MINOR perf+鲁棒性，接受 fail-loud）|
| DV-026 | P5-fix#3：**M-10（`Column.uniqueId=-1`）deferred 不修**（task-list #3 原含 M-10）。Design C 直接从 paimon `DataField.id()` 建 `history_schema_info` 的 `TField.id`，B-1a（field-id 匹配）**完全独立于** Doris `Column.uniqueId` → M-10 对 B-1a correctness 无关。rereview2 §4 已 majority-refute M-10 standalone repro（BE field-id 路不读 tuple descriptor、唯一 legacy `Column.uniqueId` 消费者 `ExternalUtil.initSchemaInfo` 经 legacy scan node 翻闸后已死）→ 无 demonstrated user-visible 消费者。故 deferred（非本 fix 必需、Design C 不穿 ConnectorColumn/ConnectorType field-id channel）。**复评触发**：若未来出现 field-id 消费者（如 SPI-on iceberg/hudi 经 `ExternalUtil` 从 Doris 列建 history schema），须重启 M-10（穿 `ConnectorColumn.fieldId`+`ConnectorType` 嵌套 id+`ConnectorColumnConverter.setUniqueId` 递归）| [task-list #3](./task-list-P5-rereview2-fixes.md) / [P5-fix-SCHEMA-EVOLUTION 设计](./tasks/designs/P5-fix-SCHEMA-EVOLUTION-design.md) / [D-049](./decisions-log.md) | 2026-06-11 | 🟢 已登记（M-10 deferred，无消费者）|
| DV-025 | P5-fix-FIX-URI-NORMALIZE：`normalizeStorageUri` 用 catalog **静态** `getStoragePropertiesMap()` 做 scheme 归一化，**非** legacy `PaimonScanNode:171` 的 vended-overlay 版（`VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials`）。理由：scheme 归一化（oss/cos/obs/s3a→s3、bucket.endpoint→bucket）与 vended 凭据正交——vended 只改 `AWS_*` 键、不改 scheme/bucket 形；只要 warehouse endpoint 静态配置（OSS/COS/OBS 绝大多数情形必配，否则连不上）静态 map 即含该 type entry，归一化与 legacy 等价。唯一分歧 = *纯-vended、无静态存储配* 的 REST catalog：静态 map 可能缺 entry → `LocationPath.of` fail-loud 抛（legacy vended-overlay 版不抛）。该边角**与凭据缝重叠、本 fix 显式不收**，归 task-list #2 `FIX-STATIC-CREDS-BE` / `FIX-REST-VENDED`（review §9.3 三道凭据缝之一）。fail-loud 优于静默送裸 `oss://`（后者 DV 错行）| [task-list #1](./task-list-P5-rereview2-fixes.md) / [P5-fix-URI-NORMALIZE 设计](./tasks/designs/P5-fix-URI-NORMALIZE-design.md) / [SPI RFC §21](./01-spi-extensions-rfc.md) | 2026-06-11 | 🟢 已登记（scope 决策，凭据边角归 #2/#3）|
| DV-024 | P5-B4 揭出并修复 B2 遗留缺陷（普通 paimon plugin 表 BE 描述符错型）：`PaimonConnectorMetadata` 不 override `buildTableDescriptor`（SPI default 返 null）→ `PluginDrivenExternalTable.toThrift` 走 fallback `SCHEMA_TABLE`（BE `descriptors.cpp:635` 建 `SchemaTableDescriptor`），而 legacy `PaimonExternalTable.toThrift` + sys 表须 `HIVE_TABLE`（`:644` `HiveTableDescriptor`）。B4/T19 加 `buildTableDescriptor` override（`HIVE_TABLE`+`THiveTable`，镜像 legacy + MC `MaxComputeConnectorMetadata.buildTableDescriptor`），**一处修同时正普通表+sys 表**。inert until 翻闸（paimon 未入 `SPI_READY_TYPES`），真值闸=live-e2e BE 描述符 | [tasks/P5 T19](./tasks/P5-paimon-migration.md) / [D-039](./decisions-log.md) | 2026-06-10 | 🟢 已修正（T19，live-e2e 待验）|
| DV-023 | RFC §10（E7 Sys Tables）设计被 P5-B4 取代：RFC §10 的「sys-table = `$`-后缀普通表 + 连接器 `getTableHandle` 内解析后缀 + `listSysTableSuffixes`」**从未实现**；live fe-core 实为 `SysTableResolver`+`NativeSysTable`+`TableIf.getSupportedSysTables/findSysTable`（iceberg + legacy-paimon 共用）。B4 按 [D-039](./decisions-log.md) 复用该 live 机制（连接器 `listSupportedSysTables`+`getSysTableHandle`，fe-core 通用 `PluginDrivenSysExternalTable`），RFC §10 加脚注标 superseded | [01-spi-extensions-rfc.md §10](./01-spi-extensions-rfc.md) / [D-039](./decisions-log.md) | 2026-06-10 | 🟢 已修正（RFC §10 脚注 + D-039）|
| DV-022 | P4-T09 §8：fe-common 去 odps 暴露隐藏传递依赖（依赖卫生，非缺陷）——`odps-sdk-core` 此前**传递**为 fe-common 自身 `DorisHttpException`(io.netty) / `GsonUtilsBase`(com.google.protobuf) 提供 jar；删 odps-sdk-core 后编译暴露缺失，故 fe-common/pom 显式补 `netty-all`+`protobuf-java`（parent dependencyManagement 管版本）。设计 §8 原假设「odps 仅服务 MCUtils」不全 | [Batch-D 设计 §8](./tasks/designs/P4-batchD-maxcompute-removal-design.md) / [D-027] | 2026-06-09 | 🟢 已修正（显式声明，`409300a75b8`）|
| DV-021 | P4-T3：Batch-D 删除后 4 条 Tier-3 接受项（minor，legacy 已删故现为既定行为，非丢数据，用户定接受不修）——**GAP3** CREATE DB 非-IFNE 远端已存→本地预抛 `ERR_DB_CREATE_EXISTS`(1007)；**GAP4** DROP TABLE 非-IF-EXISTS+远端缺→通用 `ERR_UNKNOWN_TABLE`(1109)；**GAP9** SHOW PARTITIONS `LIMIT`：sort-then-paginate（vs legacy paginate-then-sort，更合 ORDER-BY-LIMIT）；**GAP10** partitions() TVF schema-分区零实例表→返 0 行（vs legacy 抛，in-code 注释声明 intentional） | [Batch-D 红线](./task-list-batchD-redline-gaps.md) | 2026-06-09 | 🟢 已登记（Tier-3 接受）|
| DV-020 | P4-T06e FIX-CAST-PUSHDOWN：getSplits 的 limit-suppress wiring + MC 端到端 CAST-strip 无 fe-core 单测（KNOWN-LIMITATION）+ JDBC applyLimit 同类 under-return（OUT-OF-SCOPE 备查）。**① harness gap**：纯静态 `effectiveSourceLimit(limit,stripped)` 已 UT 2 + mutation 2/2（drop-suppression/always-suppress）向红 pin；连接器 `supportsCastPredicatePushdown=false` 已 UT + mutation(false→true 红) pin；但「`getSplits` 据 `filteredToOriginalIndex!=null` 调 `effectiveSourceLimit`」+「`buildRemainingFilter` 对 MC 真剥 CAST conjunct 并保留 BE-only」的端到端 wiring **无 offline 直测**（构造 `PluginDrivenScanNode` 需 harness、本模块缺，同 [DV-015]）。覆盖经：strip-when-false 是 fe-core 共享逻辑（JDBC false 分支既覆盖）+ 纯 helper UT/mutation + **live e2e 真值闸**（STRING 列存 `"5"/"05"/" 5"`，`WHERE CAST(code AS INT)=5` 返回全部 3 行 / limit-opt ON+CAST+LIMIT 不 under-return；EXPLAIN 证 CAST 谓词不在下推 filter）。**② OUT-OF-SCOPE（Rule 12 surface）**：JDBC 若 session 关 cast-pushdown 且经 `applyLimit` 推 limit，理论同类 under-return；但 MaxCompute 不 override `applyLimit`（no-op）、F9 的 getSplits limit-param 抑制对 MC 完整，JDBC `applyLimit` 路径非本修范围（pre-existing、非 MC），登记备查、待评估。fail-safe：误关下推退化为多读行交 BE（非丢数据） | [FIX-CAST-PUSHDOWN 设计](./tasks/designs/P4-T06e-FIX-CAST-PUSHDOWN-design.md) / [D-036] | 2026-06-08 | 🟢 已登记（helper+capability UT/mutation；wiring 待 live e2e；JDBC applyLimit 备查）|
| DV-019 | P4-T06e FIX-BATCH-MODE-SPLIT 异步 batch wiring + `computeBatchMode` null-guard 无 fe-core 单测（KNOWN-LIMITATION，NG-7）：纯静态四闸 `shouldUseBatchMode` 已 UT 9 + mutation 5/5 向红 pin；但 ① `computeBatchMode` 的 SF-1 `scanProvider != null` null-guard（provider-less full-adopter 防 NPE，跑 dispatch+explain 两路径）与 ② `startSplit` 的 async 分批循环（`getScheduleExecutor` outer/inner CompletableFuture + `SplitAssignment` `needMoreSplit/addToQueue/finishSchedule/setException/isStop` 契约 + init 30s 首-split）+ ③ `numApproximateSplits` 取值——三处 wiring **无 offline 直测**：构造 `PluginDrivenScanNode`（`FileQueryScanNode` 子类）需绕 ctor + stub connector/session/handle/desc/sessionVariable/splitAssignment，本模块无现成轻量 spy/analyze harness（同 [DV-015]/[DV-014] 因）。覆盖经：逐字镜像 legacy `MaxComputeScanNode:214-298`（已验 parity）+ 纯 helper UT/mutation + **大分区 live e2e 真值闸**（EXPLAIN/profile 证 batched/streamed split、规划耗时/内存 ≪ 同步路；阈值边界 `num_partitions_in_batch_mode`=0/大于选中数→回退非-batch；全空选/单分区）。impl-review `wve7y1jst` TQ-1 已据此把测试 javadoc 的「null-provider 已覆盖」声明诚实降级。fail-safe：去 batch 退化为同步 `getSplits`（非丢数据） | [FIX-BATCH-MODE-SPLIT 设计](./tasks/designs/P4-T06e-FIX-BATCH-MODE-SPLIT-design.md) / [D-035] | 2026-06-08 | 🟢 已登记（helper UT+mutation，wiring 待外表 scan harness / live e2e）|
| DV-018 | P4-T06e FIX-POSTCOMMIT-REFRESH cutover post-commit 刷新 swallow 有意分歧于 legacy（无产线逻辑改动，NG-8/F15=F21 minor，regression=no）：`PluginDrivenInsertExecutor.doAfterCommit()` 用 try/catch 吞 `super.doAfterCommit()`（=`handleRefreshTable`）刷新失败、INSERT 报 OK；legacy `MCInsertExecutor` 不 override → 异常传播 → 报 FAILED。**cutover 更安全**：按生命周期序数据已落 ODPS/远端、FE 无法回滚，`handleRefreshTable` 只刷 FE 缓存 + 写 external-table refresh editlog（follower 失效提示、非数据真相源）、不碰已提交数据 → 报 FAILED 诱发重试→重复写。**用户定（2026-06-08）接受 + Javadoc 泛化（[D-034]）、不回退**。改 = 仅 Javadoc(`:164-176`) 从「只讲 JDBC_WRITE」泛化到覆盖 MC connector-transaction 路径（两路径数据均已持久；swallow 最坏只瞬时缓存 stale 自愈；显式注明分歧 legacy）。对抗性安全核查：master 先本地刷新(`RefreshManager:152`)后写 editlog(`:155`)，丢 editlog 仅 follower 缓存暂 stale 自愈、无正确性损失/无主从分裂。swallow 路径无新增 UT（注释 only、无可 pin 逻辑变化；异常吞行为 offline 直测受同类 harness 缺位限制，同 [DV-015]）；真值闸=CI-skip live e2e（MC INSERT 后人为令 refresh 失败→断言报 OK + warn）。守门 checkstyle 0、import-gate 净 | [FIX-POSTCOMMIT-REFRESH 设计](./tasks/designs/P4-T06e-FIX-POSTCOMMIT-REFRESH-design.md) / [D-034] | 2026-06-08 | 🟢 已登记（无逻辑改动，行为收敛接受；live 真值闸待跑）|
| DV-017 | P4-T06e FIX-ISKEY-METADATA `getTableSchema→buildColumn` wiring 无连接器内单测（KNOWN-LIMITATION）：`buildColumn` 助手 isKey=true 不变式已 UT+mutation pin，但两 `getTableSchema` 调用点经 `buildColumn` 的 wiring 无 offline 测——`getTableSchema` deref live `com.aliyun.odps.Table`（唯一 ctor package-private）、模块无 Mockito（同 [DV-014]/[DV-015]/[DV-016] 类）；唯一 offline 变通=`com.aliyun.odps` 包内 fixture 子类 override `getSchema()`，repo 无先例（sibling `getColumnHandles` 同样未测）。绕过 `buildColumn`（回退 5 参 ctor）的回归仅由 CI-skip live e2e `DESCRIBE <mc_table>` 显 Key=YES 捕获（load-bearing gate）。**作用域注**：`information_schema.columns.COLUMN_KEY` 受 `FrontendServiceImpl:962-965` OlapTable 门控、MC 前后皆空、已 parity、out-of-scope（不可断言其变非空）；isKey 非纯展示（亦喂 `UnequalPredicateInfer`/BE descriptor），但 legacy 即喂 true → 本修恢复既有值 | [FIX-ISKEY-METADATA 设计](./tasks/designs/P4-T06e-FIX-ISKEY-METADATA-design.md) / [D-033] | 2026-06-08 | 🟢 已登记（helper UT+mutation，wiring 待 live DESCRIBE）|
| DV-016 | P4-T06e FIX-LIMIT-SPLIT-DEFAULT 三点（均 opt-in 默认 OFF、非丢行/非回归）：① **CAST-unwrap 致 limit-opt 资格略宽于 legacy**——converter `convert(CastExpr)→convert(child)` 在所有位置剥 CAST（左列/右 literal/IN 元素），故 `CAST(partcol AS T)=lit`、`partcol=CAST(lit AS T)`、`partcol IN (CAST(lit,…))` 经 `checkOnlyPartitionEquality` 判资格，legacy 见原始 `CastExpr` 子节点 instanceof 失败→false；② **嵌套-AND-作单 conjunct 略宽**——converter `flattenAnd` 把单 conjunct `(pt=1 AND region=cn)` 摊平成 flat `ConnectorAnd`→资格，legacy 见 `CompoundPredicate` conjunct→false（与①同安全类，且 conjunct 拆分通常上游已分）；③ **`LIMIT 0` 路径差**——本 fix `limit<=0` 拒 limit-opt 走标准多 split 路，legacy `hasLimit()`(`limit>-1`) 走 limit-opt 路；两者皆 0 行、且 `LIMIT 0` 被 Nereids 折成 EmptySet 不可达。①②均纯分区、correctness-safe（裁剪 Nereids `SelectedPartitions` 同算 + 转换后 `filterPredicate` 仍下推 read session 作 backstop，`:191/:208/:353`；LIMIT 无 ORDER BY 无序）。**另**：planScan 两行 wiring（`isLimitOptEnabled(session.getSessionProperties())` + `shouldUseLimitOptimization(...)` 收 live filter/partitionColumnNames）无连接器内单测——`planScan` 需 live odps `Table`、模块无 fe-core/Mockito（同 [DV-014]/[DV-015] 因）；纯 helper 全 UT(26)+mutation(8 向红) pin，wiring 半由 CI-skip live E2E 守。**附**：本 fix 实 `checkOnlyPartitionEquality` 同闭 F2/F12（旧恒 false stub minors）| [FIX-LIMIT-SPLIT-DEFAULT 设计](./tasks/designs/P4-T06e-FIX-LIMIT-SPLIT-DEFAULT-design.md) / [D-032] | 2026-06-08 | 🟢 已登记（opt-in 非回归 + 逻辑 UT/mutation，wiring 待 live E2E）|
| DV-015 | P4-T06e FIX-PRUNE-PUSHDOWN 端到端裁剪下推 wiring 无 fe-core 单测（KNOWN-LIMITATION）：`getSplits()` pruned-to-zero 短路 + translator `setSelectedPartitions` 注入 + `getSplits→planScan` 6 参 threading 无 fe-core 端到端 UT（连接器 scan 无轻量 analyze/spy harness，同 [DV-014] 因）。逻辑半（`PluginDrivenScanNode.resolveRequiredPartitions` 三态 + `MaxComputeScanPlanProvider.toPartitionSpecs` 转换）已 UT+mutation pin；wiring 半 + 真实裁剪生效由 p2 live `test_max_compute_partition_prune.groovy` 覆盖（真值=EXPLAIN/profile 仅扫目标分区 + `WHERE pt='不存在'`→0 行不建全分区 session）。与既有约定一致（`HiveScanNodeTest` 亦直构 node 测 setter、不经 translator）| [FIX-PRUNE-PUSHDOWN 设计](./tasks/designs/P4-T06e-FIX-PRUNE-PUSHDOWN-design.md) / [D-031] | 2026-06-08 | 🟢 已登记（逻辑 UT+mutation，wiring 待 live；外表 scan analyze/spy harness 落地后补）|
| DV-014 | P4-T06e FIX-BIND-STATIC-PARTITION bind 期投影无 fe-core 单测（KNOWN-LIMITATION）：`bindConnectorTableSink` 的 full-schema 投影（NULL 填充 + 分区列在末尾 + 按位置投影）未被 connector-path 单测直接 pin——`bind()` 走 `RelationUtil.getDbAndTable` 真 Env 解析，外表 PluginDriven catalog 需连接器插件,无现成轻量 analyze harness（OLAP analyze 测仅覆盖 `createTable` 内表）。覆盖经：①与 legacy `bindMaxComputeTableSink` 及 Iceberg 路径**共享** helper `getColumnToOutput`/`getOutputProjectByCoercion`（被既有 OLAP/Hive/Iceberg insert 测充分覆盖）；②列选择 helper `selectConnectorSinkBindColumns` 单测 + 分布 full-schema 索引测（要求 child full-schema 序方过）；③p2 live `test_mc_write_insert` Test 3/3b（部分/重排列名）+ `test_mc_write_static_partitions`。capability 声明/reader 按既有约定不单测（既有 readers 亦仅被 mock）| [FIX-BIND-STATIC-PARTITION 设计](./tasks/designs/P4-T06e-FIX-BIND-STATIC-PARTITION-design.md) / [D-030] | 2026-06-07 | 🟢 已登记（无 harness,parity+p2 覆盖；待外表 analyze harness 落地补）|
| DV-013 | P4-T06e FIX-WRITE-DISTRIBUTION 两处 planner 写分发 parity 微差（均非回归，default `strict` 下与 legacy MC 同果）：① `ShuffleKeyPruner` connector 分支缺 `enableStrictConsistencyDml` 短路 → non-strict 下少剪 shuffle-key（更保守 missed optimization）；② `enable_strict_consistency_dml=false` 下动态分区 local-sort 被丢（legacy MC 亦丢）| [FIX-WRITE-DISTRIBUTION 设计](./tasks/designs/P4-T06e-FIX-WRITE-DISTRIBUTION-design.md) / [D-029] | 2026-06-07 | 🟢 已登记（非回归，接受）|
| DV-012 | P4-T04 `TMaxComputeTableSink.partition_columns`(field 14) 源：legacy `MaxComputeTableSink` 取 `targetTable.getPartitionColumns()`（fe-core Doris `Column`）；连接器 `MaxComputeWritePlanProvider.planWrite` 取 `odpsTable.getSchema().getPartitionColumns()`（odps-sdk 列）——**源不同、值同**（分区列名）| [tasks/P4 P4-T04](./tasks/P4-maxcompute-migration.md) / [P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md) | 2026-06-06 | 🟢 已落地（P4-T04，值等价）|
| DV-011 | P4-T03 连接器事务 block 上限源：legacy fe-core `Config.max_compute_write_max_block_count`（fe.conf 可调，默认 20000）→ 连接器常量 `MAX_BLOCK_COUNT=20000L`（import-gate 禁 `common.Config`，丢可调性）；附 legacy `throws UserException`→`DorisConnectorException`（unchecked，SPI 面无 checked throws）| [tasks/P4 P4-T03](./tasks/P4-maxcompute-migration.md) / [P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md) | 2026-06-06 | 🟢 已修正（P4-T03 硬编 → GC1 经 session-property 透传恢复 fe.conf 可调，`95575a4954d`）|
| DV-010 | P4-T01 修共享 fe-core `ConnectorColumnConverter.toConnectorType` 丢 CHAR/VARCHAR 长度（写 `precision=0`；长度存 `len` 非 `precision`）→ CREATE TABLE 经 SPI 丢长度。特判 CHAR/VARCHAR 把 `getLength()` 写入 precision 字段（与逆 `convertScalarType`+`MCTypeMapping` 约定一致）| [tasks/P4 P4-T01](./tasks/P4-maxcompute-migration.md) / `ConnectorColumnConverter` | 2026-06-06 | 🟢 已修正（P4-T01）|
| DV-009 | W5 写 sink 收口位置：RFC/handoff「route 3 个 visitPhysicalXxxTableSink + 新建 PluginDrivenTableSink」与代码不符；plugin-driven 写经 `visitPhysicalConnectorTableSink` + 既有 `PluginDrivenTableSink`，W5 改为在其上 layer `planWrite()` | [写 RFC §5.5/§12 W5](./tasks/designs/connector-write-spi-rfc.md) / [HANDOFF W5](./HANDOFF.md) | 2026-06-06 | 🟢 已修正（W5 `9ebe5e27fa4`）|
| DV-008 | P3-T07 parity 两处 SPI↔legacy 偏差：列名 casing 当场修；Hudi meta-field 推迟批 E | [tasks/P3 §批C/T07](./tasks/P3-hudi-migration.md) | 2026-06-05 | 🟢 已修正 |
| DV-007 | P3 批 B scope 校正：T05 `listPartitions*` override 推迟批 E（零 live caller、Hive 不 override）；T06 MVCC 保持 default opt-out（非抛异常 override）| [HANDOFF 未完成 #1/#2](./HANDOFF.md) / [tasks/P3 T05/T06](./tasks/P3-hudi-migration.md) | 2026-06-05 | 🟢 已修正（T05 裁剪已落地；list*/MVCC 入批 E）|
| DV-006 | P3-T03 schema_id/history 非批 A 可修（连接器缺 field-id/InternalSchema/type→thrift；裸基线会回归）；推迟批 E | [HANDOFF 1b ①](./HANDOFF.md) / [tasks/P3 T03](./tasks/P3-hudi-migration.md) | 2026-06-05 | 🟡 推迟（批 E）|
| DV-005 | P3 hudi「HMS-over-SPI 前置依赖」与代码不符；真阻塞=catalog 模型错配 | [connectors/hudi.md](./connectors/hudi.md) / [master plan §3.4](./00-connector-migration-master-plan.md) / D-005 | 2026-06-04 | 🟡 待修正（P3 模型决策）|
| DV-004 | T13 用户向安装文档不在本代码仓（在 doris-website 仓） | [tasks/P2 T13](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟢 已修正 |
| DV-003 | T12 回归测试引用不存在的先例/目录且本地不可运行 | [tasks/P2 T12](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟡 推迟 |
| DV-002 | T11 无法 mock Trino plugin；JsonSerializer 非纯单元 | [tasks/P2 T11](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟢 已修正 |
| DV-001 | 批 D 范围遗漏 ExternalCatalog db 路由 + legacy test | [tasks/P2 T08-T10](./tasks/P2-trino-connector-migration.md) | 2026-06-04 | 🟢 已修正 |

---

## 详细记录（时间倒序）

### DV-047 — P6.4 iceberg procedures：perf / cosmetic / behaviour-equivalent / dispatch-order（结果恒等 / dormant / 行为等价；镜像 DV-044 批 style）
- **状态**：🟢 已登记（accept；非正确性——结果恒等/展示/dormant/接缝/幂等）｜**日期**：2026-06-24｜**签字**：各项已随 T04/T06/T07 task 用户签字，本条为 P6.4-T08 中央汇总
- **原计划位置**：T04/T06/T07 设计 §10（[procedure-spi-design](./tasks/designs/P6.4-T01-procedure-spi-design.md)）+ [task 表 §P6.4](./tasks/P6-iceberg-migration.md)
- **范围**：P6.4 procedure 迁移共 ~10 项 UT 不可见但**非正确性**的偏差，批化为一条。要点分类：
  - **cache 失效搬 dispatch 级 + 短路多失效（T04）**：各 action body 内 fe-core-only `ExtMetaCacheMgr.invalidateTableCache` 删，搬 `IcebergProcedureOps.runInAuthScope` 经 `context.getMetaInvalidator().invalidateTable`（default NOOP）。正常返回即失效（**含 no-op 短路**，legacy 短路 early-return 前不失效）⇒ 幂等于未变表，pre-flip 微差。**精确语义**：失效（与 auth-wrap）只在 `context != null` 分支（`IcebergProcedureOps.java:104-117`）；`context == null`（仅离线测试路）跑 body **不**裹 auth 且 **跳过**失效。生产接线 context 恒非 null。UT 钉：`rollbackToCurrentSnapshotStillInvalidates`（短路仍失效）/`bodyFailureAfterSuccessfulLoadDoesNotInvalidate`（body 失败不失效）。
  - **`executeAction` 加 `ConnectorSession` 参（T04）**：legacy 读 thread-local `ConnectContext` 取会话 TZ；连接器够不到，`BaseIcebergAction.execute(Table)`→`execute(Table,ConnectorSession)`、`executeAction` 同（7 个非 TZ body 忽略，仅 `rollback_to_timestamp` 消费）。SPI `ConnectorProcedureOps` 签名 + factory `createAction` 签名都不动（纯连接器内部接缝）。
  - **DV-T08-loadwrap（新错误串，无 legacy 对应）**：`IcebergProcedureOps.runInAuthScope:112-114` 把 `catalogOps.loadTable` 的非-`DorisConnectorException` 失败裹为 `"Failed to load iceberg table <db>.<tbl>: <cause>"`——legacy 在表解析期（`IcebergExternalTable.getIcebergTable()`）加载、action 外，无此 wrapper；auth-add 把 SDK load 移进 `executeAuthenticated` 才产生。结果等价：引擎命令再裹 `"Failed to execute action:"`（`ExecuteActionCommand`），且仅 catalog-load 失败触发（ported body 自身失败已 re-wrap `DorisConnectorException` 在 `:110-111` 原样 re-throw）；镜像写路径 `IcebergWritePlanProvider` 同款 wrapper。UT 钉：`IcebergProcedureOpsTest.wrapsLoadTableFailure`。
  - **DV-T08-factory-advertise（factory 列表/switch 不一致，dormant）**：连接器 `IcebergExecuteActionFactory.getSupportedActions()`/`getSupportedProcedures()` 广告 9 名（含 `rewrite_data_files`），但 `createAction` switch 只 8 case（无 `rewrite_data_files`）→ 拒 `"Unsupported Iceberg procedure: rewrite_data_files. Supported procedures: ..., rewrite_data_files, ..."`（自指）。pre-flip 偏差 vs legacy（legacy 有真 case 派发 `rewrite_data_files`），**dormant**（EXECUTE on iceberg pre-flip 走 legacy；整连接器 factory 翻闸前不可达）+ 有意（body 待 T05/T06 写半接线，= DV-045 翻闸阻塞）。UT canary：`IcebergExecuteActionFactoryTest.rewriteDataFilesIsAdvertisedButNotYetExecutable`（接线时转红）。
  - **DV-T06r-scanpool（T06, perf-only）**：`commitRewriteTxn` 丢 legacy `scanManifestsWith(threadPool)`（`IcebergTransaction:258`）→ SDK 默认 worker 池，对齐连接器 append 路；提交结果字节等价。REWRITE-scoped 同 DV-044 的 INSERT-路 DV-T04-a/T05-f。
  - **DV-T06r-zone（T06, benign）**：rewrite-added 文件分区值经 session-TZ 解析（`convertCommitDataToFilesToAdd`→`IcebergWriterHelper.convertToWriterResult(...,zone)`），复用 INSERT 的 zone-aware 路（= 既有 DV-T04-f）；行为等价（timestamptz 分区值会话 TZ、plain timestamp UTC），仅显式线程化 zone 而非 thread-local。rewrite 路新触发，UT 不单测（共享 INSERT/overwrite 分区路覆盖）。
  - **DV-T06r-rollback（T06, 中性）**：`rollback()` 不清 `filesToDelete`/`filesToAdd`（legacy `isRewriteMode` 清，`IcebergTransaction:562-572`）。单 txn/语句生命周期中性（ConnectorTransaction 单用一语句、rollback 后丢弃；陈旧列表永不被读——唯一读者 `getFilesTo*Count/Size` 在 commit 前、`commitRewriteTxn` rollback 后不可达）。
  - **DV-T07-where（T07, fail-loud dormant）**：连接器 EXECUTE 派发 WHERE 拒——`ConnectorExecuteAction.execute:121-123` 对任何 present WHERE 抛 `DdlException`（连接器前，恒收 null WHERE）。三处发散 vs legacy（消息文案 action-specific vs 统一；时序 validate-内 vs execute-内；范围 8 pure-SDK vs 全部，因 rewrite_data_files 写半未接线 R-B）；两侧皆 `UserException` 子类 ⇒ run() 前缀保。fail-loud 胜静默丢（Rule 12），dormant。UT 钉：fe-core `executeRejectsWhereConditionUntilLoweringLands`。
  - **DV-T07-name-order（T07, 有意发散 priv-first）**：未知 procedure 名校验时序——legacy `createAction` 期抛（priv 前）vs 连接器路 priv 后由 connector 拒（adapter `isSupported()` 恒 true，名校验在 `IcebergProcedureOps.execute`→factory）。priv-first 更安全（不向无权用户泄漏 procedure 存在性）+ 不在 authz 前碰连接器；「引擎保 priv／连接器拥含名派发」分工支持。
  - **DV-T07-exc-contract（T07, byte-parity 边界）**：adapter 只 `catch(DorisConnectorException)`；连接器契约=arg 失败已 re-wrap `DorisConnectorException`（`BaseIcebergAction.validate:93-94`），唯一逃逸的非-`DorisConnectorException` = 单行 `Preconditions.checkState` 的 `IllegalStateException`——与 legacy `BaseExecuteAction.execute:113-114` 同（其 checkState 也非 `UserException`、也不被 run() 前缀）。UT 钉：fe-core `executeEnforcesSingleRowWidthInvariant` + 连接器 `BaseIcebergActionTest.executeFailsLoudWhenRowWidthDoesNotMatchSchema`（含 message 字节）。
  - **present-empty / 星号 `PARTITION(*)` 拒绝不对称（low, dormant）**：legacy `validateNoPartitions` 按 `isPresent()` 拒（含星号 spec）；连接器 adapter 把 `Optional`→`getPartitionNames`（星号→空）后连接器 `validateNoPartitions(!isEmpty())` **不**拒 ⇒ `EXECUTE proc(...) PARTITIONS(*)` 在无分区 procedure 上 legacy 拒、连接器接受。文法可达（`DorisParser` `alterTableExecute` `partitionSpec?` 含星号 alt）；EXECUTE+分区对这些 procedure 无意义 + dormant。无修，P6.6 docker 闸。
  - **null-body-row 编码不对称（low）**：连接器把 null body-row 编码为 `(declared-schema, emptyRows)` vs legacy 丢元数据返 null；parity 由 `ConnectorExecuteAction.wrapResult` 的 `rows.isEmpty()→null` 恢复（三态 OR vs legacy 两态）。潜伏（现 9 procedure 无返 null-row）。UT 钉：fe-core `executeReturnsNullResultSetWhenConnectorReturnsNoRows`。
  - **per-conjunct scan.filter() 形状（T05, 结构等价）**：planner 把 WHERE 降为 `List<Expression>` 逐合取 `scan.filter()`（iceberg 内部 AND）vs legacy 单 `Expressions.and()` 合并 filter——扫描结果字节同，纯结构。UT 钉：`RewriteDataFilePlannerTest.whereTopLevelAndAppliesEveryConjunct`。
- **为何可接受**：全部**非正确性**——结果恒等（scanpool/zone/per-conjunct）、连接器内部接缝（session 参）、dormant（factory-advertise/WHERE 拒/星号分区）、有意更安全（priv-first）、byte-parity 边界（exc-contract）、幂等（cache 短路）、潜伏（null-row）、新串结果等价（loadwrap）。
- **真值闸**：P6.6 docker（翻闸后回归）逐项确认无害。
- **关联**：[DV-044]（P6.3 同 style 批）、[DV-040]/[DV-035]（同 style）、[DV-045]（DV-T08-factory-advertise 接线 = DV-045）、[DV-046]、T04/T06/T07 task record。

### DV-046 — P6.4 iceberg procedures：parity-忠实 correctness-bearing 但 UT 不可见 deviation（auth-add Kerberos + rewrite WHERE conflict-mode；docker / dormant 闸）
- **状态**：🟢 已登记（accept；parity-by-construction 或 dormant+用户签字；docker/Kerberos 真值闸）｜**日期**：2026-06-24｜**签字**：auth-add 随 T04；DV-T05r-where = 用户签字 Option A（2026-06-24）
- **原计划位置**：T04 设计 §10（auth）+ T05 设计 §5/§10 + [task 表 §P6.4 T04/T05](./tasks/P6-iceberg-migration.md)
- **范围**：与 [DV-047] 区别 = 本条各项**承载正确性**，但 parity-by-construction（auth）或 dormant+用户签字+常见路零差异（rewrite WHERE）。
  - **auth-add（T04, Kerberos）**：8 个 snapshot mutator 的 `loadTable`+body 的 SDK `manageSnapshots()/...commit()` 现裹 `context.executeAuthenticated`（`IcebergProcedureOps.runInAuthScope:108-109`，**当 `context != null`**——见 DV-047 精确语义）。legacy fe-core action 提交 snapshot mutator **未** authenticate（潜伏 Kerberized-catalog auth bug）。**auth 是加非丢**、修向 parity；离线 InMemoryCatalog 无 auth 不可分辨真 KDC `doAs` ⇒ 留 docker/live。UT 钉 wiring：`runsBodyInAuthScopeAndInvalidatesTableAfterCommit`（`authCount==1`）/`argumentValidationRunsBeforeTouchingTheCatalog`（validate 先于 auth）。跨引用 [DV-031]/DV-043 同类 auth-wrap。
  - **DV-T05r-where（T05, 用户签字 Option A 2026-06-24）**：`rewrite_data_files` WHERE 走 P6.3 conflict-mode 通路（`new IcebergPredicateConverter(schema, zone, true)`）vs legacy `IcebergNereidsUtils.convertNereidsToIcebergExpression`。两处有意发散：① **不可转节点静默丢**（legacy **抛**）→ planner `scan.filter()` 变宽 → 重写**比 WHERE 指定更多**文件（极端：整条 WHERE 不可转 → 重写全表），不报错；② conflict-matrix 收窄跨列 OR/非-`IS NULL` 的 NOT/NE。**关键认知**：设计 §5 「safe over-approximation」对**扫描下推**成立（BE 残差再过滤）但对 **rewrite 不成立**（planner `scan.filter()` 直接即重写集，无下游再过滤）→ 同一 P6.3 管道在 rewrite 语境安全性反号（vs O5-2 冲突检测「变宽=更保守」）。conflict-matrix 是 legacy 接受节点集的**严格子集** ⇒ 连接器只会相对 legacy 变宽、绝不反向收窄（无隐藏反向发散）。常见 WHERE（等值/范围/IN/BETWEEN）两路一致、零差异；发散只在罕见 WHERE（函数/NE/跨列 OR/NOT）。**reachability（T08 critic）**：当前 wiring 下 DV-T05r-where over-scan **经 EXECUTE 派发不可达**——双重闸：`rewrite_data_files` 无 factory `createAction` case（DV-T08-factory-advertise）且 `ConnectorExecuteAction` 拒/置 null WHERE（DV-T07-where）；over-scan 经 `ALTER TABLE EXECUTE` 只在 P6.6 同时（加 factory case + WHERE-lowering 落地 = DV-045 R-B 接线）后才能触发，今仅经 dormant 直连 planner 路（`RewriteDataFilePlannerTest`）被覆盖。UT 钉 Option A：`unconvertibleCrossColumnOrWidensScan`（整表变宽）/`whereBetweenPrunesViaConflictMode`（conflict-mode 钉，scan-mode 会丢→红）/`whereTopLevelAndAppliesEveryConjunct`（多合取交集）。
- **为何不单独阻塞翻闸**：auth-add = parity-by-construction（只加 auth）；DV-T05r-where 完全 dormant（rewrite 执行半 R-B 未接线 = DV-045）+ 用户签字 + 常见路零差异 + UT 钉逻辑半。
- **别于 [DV-045]**：DV-045 = 未接线翻闸阻塞；本条 = 已修（auth）/已签待 docker（rewrite WHERE）。
- **真值闸**：P6.6 docker——Kerberized EXECUTE auth round-trip + `rewrite_data_files` WHERE 各形（等值/范围/函数/NE/OR）文件集 parity。
- **关联**：[DV-043]（P6.3 同类 correctness-bearing）、[DV-031]（Kerberos）、[DV-045]（DV-T05r-where 是其规划半，本条 over-scan 经 EXECUTE 在 DV-045 接线后才可达）、T04/T05 task record。

### DV-045 — P6.4 `rewrite_data_files` 执行半**翻闸阻塞 BLOCKER**：执行半 fe-resident（R-B 推后专门写路径 RFC）
- **状态**：🔴 **翻闸前（P6.6）必接线**（专门写路径 RFC）——未接则 `rewrite_data_files` 经 plugin-driven 端到端断｜**日期**：2026-06-24｜**签字**：用户裁 Option 1（2026-06-24，T06 recon 证伪设计 §5 R-A 前提）
- **原计划位置**：T05/T06 设计 §5（[procedure-spi-design](./tasks/designs/P6.4-T01-procedure-spi-design.md)）+ T06 实现记录 + [task 表 §P6.4 T06](./tasks/P6-iceberg-migration.md) + [HANDOFF 🔴🔴](./HANDOFF.md)
- **偏差描述**（写路径耦合长杆，**与 [DV-041] 写路径翻闸阻塞同族**）：`rewrite_data_files` 分三半——**① 事务半**（连接器 `IcebergConnectorTransaction` 的 `WriteOperation.REWRITE` 变体，T06 已建，dormant，净 0 新事务 verb）；**规划半**（连接器 `RewriteDataFilePlanner`/`RewriteDataGroup`/`RewriteResult`，T05 已建，dormant）；**②③④ 执行半留 fe-core**（`RewriteDataFileExecutor`/`RewriteGroupTask`/`RewriteTableCommand`/`IcebergRewriteExecutor`，**R-B 推后**）。
  - **recon 证伪设计 §5 / D-062 R-A 前提**「连接器从 pinned snapshot+WHERE 重规划」：(a) 连接器 scan SPI 只能按 snapshot/谓词/分区收窄，**无法表达** legacy bin-pack 的「分区内任意文件子集」→ 重规划 **over-scan** → 重写组外文件 → **破坏 rewrite 正确性**（非仅成本）；(b) `FileScanTask` 侧信道（`RewriteGroupTask:117`→`IcebergScanNode.getFileScanTasksFromContext`〔def `:492` / rewrite-consuming caller `:929`，T09 faithfulness 校正 stale `:498`〕）翻闸后走 `PluginDrivenScanNode` 端到端**死**；(c) **SPI 模块边界**：`fe-core` 只依赖 `fe-connector-api/-spi`，连接器 `RewriteDataGroup`（裹 iceberg `FileScanTask`/`DataFile`）**不能**跨进 fe-core 执行半；(d) multi-sink-per-txn 生命周期（一 txn 跨 N 组 INSERT-SELECT）须重设计、只能翻闸（P6.6）验。**= D-062「超预算→R-B」预设回退被实证触发**，用户签字 Option 1。
- **翻闸阻塞 checklist（P6.6 前必清）**：
  - [ ] (i) 每组 **file-level 扫描范围**（新中立 scan-范围 SPI；pinned-snapshot+WHERE 不足/over-scan）
  - [ ] (ii) `BindSink.bind(UnboundIcebergTableSink):1057` 对 PluginDriven 抛错 → 改绑 `UnboundConnectorTableSink`→`visitPhysicalConnectorTableSink`
  - [ ] (iii) `RewriteGroupTask:175` `instanceof IcebergRewriteExecutor` + executor 选 `instanceof PhysicalIcebergTableSink` → 连接器 sink
  - [ ] (iv) `RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转 → 经通用 `PluginDrivenTransactionManager` 取连接器 REWRITE txn〔① 已建〕+ `setTxnId` 喂 commit-fragment
  - [ ] (v) multi-sink-per-txn 生命周期（一 begin、N 组 INSERT 不重 begin/commit、一 commit）
  - [ ] (vi) 接线同步：DV-T08-factory-advertise（加 `createAction` rewrite_data_files case，canary 转红）+ DV-T07-where（WHERE-lowering 落地，连接器收真 WHERE = DV-T05r-where 激活）
- **为何登记为 BLOCKER（Rule 12）**：与 [DV-041] 写路径翻闸阻塞同需 holistic 写路径 RFC（scan-范围 SPI + bind + executor + multi-sink 生命周期）；现 iceberg **不在** `SPI_READY_TYPES`，rewrite 写路径 behind gate dormant 故未触发；① 事务半/规划半 dormant（无 live caller，`planWrite` 无 REWRITE case→default-throw / factory `rewrite_data_files`→default-throw）。
- **真值闸**：专门写路径 RFC + P6.6 docker（`rewrite_data_files` 端到端：文件子集精确、bin-pack 正确性、OCC 冲突检测）。
- **关联**：[DV-041]（写路径翻闸阻塞同族 holistic）、[DV-042]（rewrite 执行半 fe-resident 同北极星 iii 域）、[DV-046]（DV-T05r-where 规划半，gated 在本条接线后才可触发）、T06 task record、[HANDOFF 🔴🔴](./HANDOFF.md)。

### DV-044 — P6.3 iceberg 写路径：perf / cosmetic / EXPLAIN-diff / 等价结构（结果恒等；镜像 DV-040/DV-035 批 style）
- **状态**：🟢 已登记（accept；结果恒等/展示/性能/等价结构）｜**日期**：2026-06-24｜**签字**：各项已随 T01–T07 task 用户签字（见各设计 §6），本条为 P6.3-T08 中央汇总
- **原计划位置**：T01–T07〔a/b/c〕各设计文档 §6 + [task 表 §P6.3 line 239](./tasks/P6-iceberg-migration.md)
- **范围**：P6.3 写框架统一 + iceberg 写路径共 ~20 项 UT 不可见但**非正确性**的偏差，批化为一条。要点分类：
  - **生命周期 / 时序变更（行为等价）**：jdbc 退化 no-op txn 现经通用 `PluginDrivenTransactionManager.begin`/`putTxnById` 全局注册（连接器 0 注册码，DV-T01-b）；`writeOperation` 枚举移 T03、`beginTransaction` 默认保 throwing（非 RFC 字面 no-op，由 `NoOpConnectorTransaction` 退化，DV-T01-c）；`writeOperation` 产品消费者落 T04/T06、T03 仅默认值契约测（DV-T03-b）；txn-id 双注册由通用 manager 完成（同 maxcompute，DV-T03-c）。
  - **EXPLAIN / observability drop（cosmetic）**：jdbc EXPLAIN 头标签 `WRITE TYPE: JDBC_WRITE`→`WRITE: plan-provider`（窄化，INSERT SQL 经 `appendExplainInfo` 保，DV-T02-b）；`appendExplainInfo` 在 EXPLAIN-string 期触发连接器读元数据（**净优于** legacy 每-INSERT 查；纯 INSERT 为 0，DV-T02-c）；sink EXPLAIN 标签 `PLUGIN-DRIVEN TABLE SINK`+detail vs `ICEBERG TABLE SINK`（plan-shape 不变，OQ-3，非回归）。
  - **fail-loud 异常型 / 源不同值同**：begin/commit/guard 失败抛 `DorisConnectorException`/`IllegalStateException`/SDK `ValidationException`/`VerifyException` vs legacy `UserException`/`AnalysisException`/`IllegalArgumentException`/`RuntimeException`（消息字节同义，DV-T03-a/T04-b/T05-b/T06-c）；`partition_data_json` 经 iceberg Jackson 非 fe-core Gson（`List<String>` 字节同，DV-T04-d）；私有 `formatVersion(Table)` 与 `IcebergConnectorMetadata.getFormatVersion` 重复（避跨类编辑，DV-T05-e）。
  - **perf / scale-only（结果恒等）**：op 的 `scanManifestsWith(pool)` 丢 → SDK 默认 worker pool（提交结果字节等价，DV-T04-a/T05-f）；op 选择经单 `beginWrite`+`commit()` switch 而非 legacy 3 begin/finish 方法名、`CommonStatistics` 内联（DV-T04-e）；`getWriteSortColumns`（translator 期）+ `beginWrite`（bind 期）double loadTable 只读重 I/O（DV-T06-d）。
  - **SPI 接缝 / 参数化（thrift-free，等价）**：TIMESTAMP/identity 解析取显式 `ZoneId` 形参非 thread-local（DV-T04-f/T05-d；correctness 半在 DV-043）；O5-2 `applyWriteConstraint` 暂存中立 `ConnectorPredicate`、commit 时惰性转（DV-T05-a）；新 `ConnectorContext.getBackendFileType`（scheme→`TFileType` 的 thrift-free enum-name 接缝）+ `getWriteSortColumns`（null=无 sort / 非 null=有）+ 声明 `SINK_REQUIRE_FULL_SCHEMA_ORDER`（DV-T06-e）。
- **为何可接受**：全部**非正确性**——生命周期/异常型行为等价、仅展示（EXPLAIN）、源不同值同、perf/scale 结果恒等、SPI 接缝 thrift-free 等价。jdbc/maxcompute/paimon 写字节 parity 经其各自回归门守（T01–T06 无回归）。
- **真值闸**：P6.6 docker（翻闸后回归套件）逐项确认无害 + assembled-Thrift vs legacy。
- **关联**：[DV-040]（P6.2 同 style 批）、[DV-035]（P4 同 style 批）、[DV-041]（翻闸阻塞）、[DV-042]（北极星 iii）、[DV-043]（correctness-bearing）、T01–T07 设计 §6。

### DV-043 — P6.3 iceberg 写路径：parity-忠实 HIGH/MEDIUM correctness-bearing 但 UT 不可见 deviation（parity-by-construction / widening-safe，各有 P6.6 docker 闸）
- **状态**：🟢 已登记（accept；各项 parity-by-construction 或 widening-safe + P6.6 docker 真值闸必验）｜**日期**：2026-06-24｜**签字**：各项随 T01–T07 task 用户签字（Option A 冲突矩阵 2026-06-24）
- **原计划位置**：T01/T02/T03/T04/T05/T06/T07b 各设计文档 §6 + [task 表 §P6.3](./tasks/P6-iceberg-migration.md)
- **范围**：与 [DV-044] 区别 = 本条各项**承载正确性**（误则错结果），但**parity-by-construction 或只-widening（绝不漏冲突）** 故单项不阻塞翻闸；每项有具体 P6.6 docker 闸。
  - **byte-parity 移位（OQ-1）**：jdbc `TJdbcTableSink` thrift 由 fe-core planner 移连接器 `JdbcWritePlanProvider.planWrite`（DV-T02-a）——14 字段逐字段 parity（设计 §4.1）+ 连接池 `getInt`/`DEFAULT_POOL_*` 非 bind 硬编 fallback 陷阱已避；UT 断字节，docker 终验。
  - **affected-rows 哨兵**：jdbc 退化 txn 的 `NoOpConnectorTransaction.getUpdateCnt` 返 `-1` 哨兵 + executor `if(cnt>=0)` 守 `DPP_NORMAL_ALL`，否则默认 0 clobber 回归（DV-T01-a）——correct-by-construction，BE 真实计数离线 UT 不可验。
  - **auth-wrap 离线不可见**：`beginWrite` 的 `loadTable`+`newTransaction()` 须在 `executeAuthenticated` 内（Kerberized HMS `BaseTable.newTransaction` 触发无条件远程 `doRefresh`），离线 InMemoryCatalog 无 auth 不可分辨（DV-T03-d，跨引用 [DV-031]）。
  - **zone-aware 分区值解析**：TIMESTAMP/identity 分区值经连接器-本地 parser + 显式 `ZoneId`（非 nereids `DateLiteral`+thread-local），BE 发 canonical 格式（DV-T04-c）——实务等价，docker 断字节。
  - **conflict-mode widening（Option A，用户签字 2026-06-24）**：O5-2 写约束经 `IcebergPredicateConverter` conflict-mode 转换，**忠实** legacy 真实冲突路——legacy 不处理的形式（NullSafeEqual / Cast 包裹列 / col-col / 裸 bool / NE）一律丢弃 → 冲突 filter **widens**（更保守，**no-missed-conflict 安全**）；字面量经扫描侧 `extractIcebergLiteral` 矩阵（边缘 UUID/FIXED/GEO 分歧只放宽）；合成列排除经 T07c 注入 Predicate（DV-T05-c/T07b-matrix/T07b-literal）。**本轮 T08 gap-fill UT 已补 per-conjunct drop（O5-2-GAP-001）+ OR all-or-nothing（O5-2-GAP-006）+ 分区冲突 filter 端到端（OP-SEL-01/VAL-T05-*）**，逻辑半已 UT 锁。
  - **hadoopConfig 口径**：连接器 `hadoopConfig` 经 fe-filesystem `StorageProperties.toHadoopConfigurationMap()` vs legacy fe-core `getBackendConfigProperties()`，默认口径可能微差（DV-T06-hadoopconfig）——P6.6 docker 断字节 parity。
- **为何各项不单独阻塞翻闸**：每项 **parity-by-construction**（thrift 逐字段 / 哨兵守 / auth-wrap 镜像 legacy）或 **widening-only**（冲突 filter 只放宽、绝不漏冲突）；UT 已覆盖逻辑/wiring（T08 gap-fill 补全），剩余仅「真 BE/Kerberized/live 行为」须 docker。**别于 [DV-041]**：DV-041 是**未接线翻闸阻塞**，本条是**已修待 docker 实证**。
- **真值闸**：P6.6 docker——INSERT/DELETE/UPDATE/MERGE 写 parity + 事务提交/冲突检测 + jdbc affected-rows + Kerberized auth + assembled-Thrift vs legacy。
- **关联**：[DV-041]（翻闸阻塞）、[DV-044]（perf/cosmetic）、[DV-039]（P6.2 同类 correctness-bearing）、[DV-031]（Kerberos）、T01–T07 设计。

### DV-042 — P6.3 北极星 (iii) 有界架构偏差：iceberg DML plan 合成 fe-resident（Route B / option (i)，PMC 签字）
- **状态**：🟢 已登记（accept；有界 intentional、PMC/RFC §4 签字、保 EXPLAIN parity）｜**日期**：2026-06-24｜**签字**：RFC §4 Q1 = Route B / option (i)（PMC 评审通过）
- **原计划位置**：[06-iceberg-write-path-rfc.md §5.3/§10](./06-iceberg-write-path-rfc.md) + [T07 设计 §4.5.8](./tasks/designs/P6.3-T07-rowlevel-dml-unification-design.md) + [task 表 §P6.3 范围外](./tasks/P6-iceberg-migration.md)
- **偏差描述**：iceberg DELETE/UPDATE/MERGE 的 **plan 合成**（`$row_id` 注入 / branch-label 投影代数 / nereids→iceberg expr）**暂留 fe-core**，经连接器-键控 `RowLevelDmlTransform` 注册表（`RowLevelDmlCommand` 壳 + capability 派发）调用；合成内仍有反向 `instanceof IcebergExternalTable` cast（fe-resident 合成内属本条，其余 catalog/statistics 读侧 cast 归 P6.7）。这是 RFC §4 Q1 用户/PMC 裁定的 **Route B / option (i) 务实路径**（拒 option (ii) 新 nereids-spi 模块），与北极星 **(iii) Trino 式通用化**（连接器 0 优化器 import、引擎核心全 DML 合成、连接器供 3 声明式 SPI = row-id handle + RowChangeParadigm + merge sink）有界偏离。
- **范围（含 T07c 等价结构项）**：
  - **DV-04x（本条核心）**：DML plan 合成 fe-resident + 连接器-键控变换注册表 + 合成内反向 instanceof。
  - T07c **冲突-filter 计算顺序**从 T04 分支内挪到 `newExecutor` 后（单 merged call，provably-equivalent reorder；T04 冲突已 stable、新 executor 无副作用、结果字节同，DV-T07c-conflict-order）。
  - T07c 壳做**单 table resolve**，删 legacy 冗余 re-resolve + instanceof throw（dispatcher 已解析、吞/抛纪律保，harmless，DV-T07c-resolve）。
  - `IcebergXCommand.run()`/`executeMergePlan()` 循环保留但不再被路由 = **transitional dead**，P6.7 随类删；live 路径仅壳一份循环（DV-T07c-dormant-loop）。
  - conflict-mode 合成列排除按名排扫描侧 row-lineage 列，合成列**生产**排除由 T07c `WriteConstraintExtractor` 注入 `Predicate<SlotReference>` 完成（DV-T07b-exclusion）。
  - `rewritableDeleteFileSets`（fv≥3 DV rewrite）经 T07c executor finalize seam 注入连接器 opaque sink（critic 定点 option b vs c，DV-T07-rewritable）。
- **为何可接受**：option (i) 保 **EXPLAIN/执行 parity**（oracle `IcebergDDLAndDMLPlanTest` 14/0 byte-parity 铁证，本轮 T08 又补 DELETE/UPDATE operation-literal 值断言）、surgical（不新建 nereids-spi 模块）；等价结构项 provably-equivalent 或 transitional-dead。**有界**——不随意扩散，仅在北极星 (iii) 专门 RFC 时关闭。
- **真值闸**：oracle byte-parity（已绿）+ P6.6 docker EXPLAIN/执行不回归；北极星 (iii) 触发 = 第二个行级-DML 消费者（hive P7 / paimon 第二消费者）→ 后续专门 RFC 彻底关闭本条。
- **关联**：[06-iceberg-write-path-rfc.md §10 北极星](./06-iceberg-write-path-rfc.md)、[DV-041]（翻闸阻塞，DV-T07-materialize 是其物化半）、[DV-009]（写 sink 收口位置）、T07 设计。

### DV-041 — P6.3 写路径**翻闸阻塞 BLOCKER**：通用 `visitPhysicalConnectorTableSink` 缺合成列物化 + 分布（DV-038 同主题新面）+ 休眠-至-翻闸激活集
- **状态**：🔴 **翻闸前（P6.6）必修/必接线**——未接则 iceberg DELETE/MERGE 经通用 sink 挂 BE / REST 读 403｜**日期**：2026-06-24｜**签字**：T07 §1.1 critic finding 5 + 激活集各项随 T06/T07 task 用户签字延后
- **原计划位置**：[T06 设计 §6](./tasks/designs/P6.3-T06-iceberg-sink-unification-design.md) + [T07 设计 §1.1/§6](./tasks/designs/P6.3-T07-rowlevel-dml-unification-design.md) + [RFC §5](./06-iceberg-write-path-rfc.md) + [HANDOFF 🔴🔴](./HANDOFF.md)
- **偏差描述**（**主阻塞与 [DV-038] 同一 BE StructNode DCHECK 崩溃类，新面**）：
  - **主阻塞 — DV-T07-materialize（合成列物化 + 分布）**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator`）**无**合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` 分布——这两者**仅** legacy `visitPhysicalIcebergMergeSink` 有。iceberg DELETE/MERGE 经通用 sink 真正走通前，通用 translator 须**先长出**合成列物化 + 分布，否则上游合成列被丢 → BE `iceberg_reader.cpp` field-id 路 StructNode `DCHECK` → 整 BE 崩（**同 DV-038 崩溃签名**）。T07 **有意不碰** `PhysicalPlanTranslator:589-627`，把它登记为 P6.6 翻闸阻塞。
- **休眠-至-翻闸激活集**（**P6.6 必接线，翻闸全有或全无**；不接则对应 catalog/查询断，但非 BE 崩）：
  - 写分布 `getRequirePhysicalProperties`（分区-hash）延后——capability 模型错配（mc 强制 local-sort，iceberg 必须不），dormant（DV-T06-a）。
  - branch-INSERT thread-through——通用 `PluginDrivenWriteHandle` 不带 branch 字段，T06 折出 `branch=Optional.empty()`，须 P6.6 经 `PluginDrivenInsertCommandContext` 加字段（DV-T06-branch）。
  - REST 对象存储 **vended overlay**——delete/merge sink 的 `hadoop_config` 静态、无 vended overlay；翻闸后 REST 对象存储读 **403**（DV-T07-vended，同 P6.2 [DV-039] vended 族）。
  - O5-2 `BaseExternalTableInsertExecutor.getConnectorTransactionOrNull()` → **null 休眠**（iceberg 走 legacy txn），翻闸（iceberg 进 plugin-driven）后激活（DV-T07c-o5seam）。
  - FILE_BROKER 写地址（`broker_addresses`）解析缺（broker 写少见，P6.6 确认需求后补，DV-T06-broker/T07-broker）。
- **为何登记为 BLOCKER（Rule 12）**：主阻塞与 [DV-038] **同一** BE field-id 路 StructNode DCHECK 崩溃类、同需 holistic 共享 fe-core/translator 修；激活集是翻闸**全有或全无**的必接线项（`CatalogFactory:104-113`）。现 iceberg **不在** `SPI_READY_TYPES`，写路径 behind gate dormant，故未触发。
- **真值闸**：P6.6 docker——翻闸前必接线主阻塞（合成列物化 + 分布）+ 激活集，跑 iceberg DELETE/MERGE（防 BE 崩）+ REST 对象存储读（防 403）+ branch-INSERT。**须先接线再翻闸**。
- **关联**：[DV-038]（同 BE StructNode DCHECK 主题，DV-041 是写路径新面；翻闸前须一并 holistic 修）、[DV-042]（DML 合成 fe-resident，DV-T07-materialize 是其物化半）、[DV-009]（写 sink 收口）、T06/T07 设计、[HANDOFF 🔴🔴 块](./HANDOFF.md)。
- **后续动作（翻闸阻塞 checklist，P6.6 前必清）**：
  - [ ] 通用 `visitPhysicalConnectorTableSink`：长出合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge`（与 [DV-038] 一并 holistic）
  - [ ] 写分布 `getRequirePhysicalProperties` 接线（DV-T06-a）+ branch/broker thread-through
  - [ ] REST 对象存储 vended overlay 接 delete/merge sink（DV-T07-vended）
  - [ ] O5-2 `getConnectorTransactionOrNull()` 翻闸激活核对（DV-T07c-o5seam）

### DV-040 — P6.2 iceberg scan：perf / observability / EXPLAIN-profile drop + lenient-validation + benign superset（结果恒等 / cosmetic / scale-only；镜像 DV-035 批 style）
- **状态**：🟢 已登记（accept；结果恒等/展示/性能/良性超集）｜**日期**：2026-06-23｜**签字**：各项已随 T02–T09 task 用户签字（见各设计文档 §deviation），本条为中央汇总
- **原计划位置**：T02–T09 各设计文档 §deviation + [P6.2-T11 汇总设计](./designs/P6-T11-iceberg-scan-summary-design.md)
- **范围**：T02–T09 共 ~36 项 UT 不可见但**非正确性**的偏差，统一为一条（逐项对各设计文档核对）。要点分类：
  - **profile / EXPLAIN drop（同 paimon 族）**：`metricsReporter`+`planWith` 丢（规划指标缺、用 SDK 默认 worker pool，T02）；manifest cache hit/miss 统计从 EXPLAIN VERBOSE 丢、无 `cacheHitRecorder`（T08）；空表 COUNT EXPLAIN 显 `(-1)` vs legacy `(0)`（BE 结果同为 0，T05）。
  - **perf / scale-only（结果恒等）**：COUNT 塌缩单 range 丢 legacy `>10000` 并行 count-split trim（BE 求和等价，T05）；count range 携惰性 delete/partition carriers（CountReader 忽略，T05）；`scan.snapshot()` vs legacy `getSpecifiedSnapshot/currentSnapshot`（非 time-travel 等价，T05）；`beginQuerySnapshot` live 读无 cache、跨查询漂移窗（T07，T08 加 cache 缓解）；schema memo 跳过（iceberg 历史 schema 内存即得、零 I/O 收益，T08）；`getMatchingManifest` HashMap vs Caffeine LoadingCache（T08）；manifest gate `ttl-second`/`capacity` 仅喂 enable 公式不 size cache（legacy quirk，T08）；`ManifestFiles.dropCache` 不调（SDK ContentCache 资源释放 gap、非 stale-read，T08）；batch mode 延后（manifest-计数 vs 分区-计数轴不匹配、0-新-SPI 不变量，T02/T03/T05/T08）。
  - **typed-vs-string / 源不同值同**：typed carriers vs paimon string-props（`IcebergScanRange`，T03）；per-file `dataFile.format()` vs legacy table-uniform reader 选择（混格式表更正确，T03）；`partition_data_json` Jackson/`JsonUtil` vs Gson 字节形（BE 重解析值同，T03）；单 `-1` field-id 字典条目 vs HANDOFF「枚举全 schema-id」（iceberg field-id 永久不变 + BE 从文件读 file field-id，T06）。
  - **predicate over-approximation（BE residual 兜底）**：reversed `literal OP col` / col-col 丢（Nereids 已规范化故不可达，T02）；`IsNull`/`Like`/`Between`/`FunctionCall` 丢（legacy 无此 case，IS NULL 仍经 EQ_FOR_NULL，T02）；LARGEINT(String) 不下推 int64（legacy parity，T02）；edge 字面量串形 best-effort（datetime/decimal vs STRING 列，T02）；ZoneId alias-map vs 裸 `ZoneId.of(String)`（修 CST 8h 偏移误裁，T02/T03）。
  - **lenient-validation / fail-loud 字节同**：`delete_files` v2+ unset vs legacy 空 ArrayList（BE unset==empty，T03）；Bug1 非-orc/parquet fail-loud `IllegalStateException` vs `DdlException`（消息字节同，T03）；DV `contentOffset/contentSizeInBytes` auto-unbox to long（legacy parity，T04）；manifest-cache 3 键不进 `validateProperties`（恶值静默回落、非正确性，T08）；`is_optional` 恒 true vs iceberg required flag（BE field-id 路不读、inert，T06）；STRING scalar 占位符（BE 只用 type.type 当 nested-vs-scalar 判别，T06）；`Locale.ROOT` 顶层小写 vs paimon 默认 locale（T06）；INCREMENTAL @incr fail-loud vs legacy 静默读 latest（Rule 12，T07）；TIMESTAMP digital 当 epoch-millis（安全超集，T07）。
  - **vended 边角（同 paimon 族）**：`extractVendedToken` 无条件读 `io().properties()`（非云键被滤，T09）；`extractVendedToken` 非 fail-soft（paimon 同款不修，T09）；REST `PROVIDER_CHAIN` 非-DEFAULT signing-cred gap（T05 族、与 T09 数据路无关，T09）。
  - **🔵 classloader / split-package（category a，T08 extractor 漏报、本条补登）**：vendored `org.apache.iceberg.DeleteFileIndex`（906 行，包私有 1.10.1）与 iceberg-core jar 的包私有副本 **split-package 共存**，jar 序定胜者；fe-core 同款已上线 proven、字节相同→预期 benign，但**首次** direct-iceberg 连接器 + 该 vendored 类，UT/打包不可见，须 P6.6 docker plugin-zip e2e 实证。**跨引用 P6.1 同类 classloader 风险**（[risks.md R-004]、CI #973270 ServiceLoader-empty 类、hive-catalog-shade + direct iceberg-core child-first 共存，均 P6.1 packaging 层、HANDOFF「🔴 关键认知」已登记）。
- **为何可接受**：以上全部**非正确性**——结果恒等（perf/scale）、仅展示（profile/EXPLAIN）、源不同值同、安全超集、或 BE residual 兜底；split-package 是 fe-core 已 proven 的 benign 模式。
- **真值闸**：P6.6 docker（翻闸后回归套件）逐项确认无害；split-package 走 plugin-zip e2e。
- **关联**：[DV-035]（P4 同 style 批）、[DV-032]/[DV-033]（COUNT/native perf 族）、[DV-025]（normalizeStorageUri）、[risks.md R-004]、T02–T09 设计文档。

### DV-039 — P6.2 iceberg scan：parity-忠实 HIGH/MEDIUM correctness-bearing 但 UT 不可见 deviation（各有 P6.6 docker 闸、已连接器内缓解，单项不阻塞翻闸）
- **状态**：🟢 已登记（accept；各项已连接器内缓解 + P6.6 docker 真值闸必验）｜**日期**：2026-06-23｜**签字**：各项随 T03–T10 task 用户签字
- **原计划位置**：T03/T04/T06/T07/T08/T09 设计文档 + [P6.2-T11 汇总设计](./designs/P6-T11-iceberg-scan-summary-design.md)
- **范围**：与 [DV-040] 区别 = 本条各项**承载正确性**（误则错结果/崩溃），但**已在连接器内修/缓解**故单项不阻塞翻闸；每项有具体 P6.6 docker 闸，翻闸前必逐项确认。
  - **HIGH（BE-slot 分类 / scheme 派发 / time-travel 超集 / 凭据发射）**：
    - columns-from-path **unset-then-set**、无 `HIVE_DEFAULT` 哨兵、null 经并行 `is_null` list（T03）——BE OrcReader/parquet slot 分类、防双填/DCHECK（CI #968880 类）。
    - `isPartitionBearing()` 空分区值 Hive-路径-parse 崩修（一个**非破坏 SPI 默认方法**，paimon 字节不变，T03 Bug2）——P6.2「0 新 SPI」唯一例外。
    - 主数据文件路径经 `context.normalizeStorageUri` 归一化（`path`=归一化 BE-open / `originalPath`=raw `original_file_path`）vs raw `oss://` scheme-派发打不开（T04）。
    - **Option A 全 pinned-schema field-id 字典**（time-travel 下超集覆盖所有 BE slot、防 `iceberg_reader.cpp:181` 重命名列 DCHECK，T07）。
    - 静态 `location.*` 凭据仅 scan/BE-open 时校验——T09 前发**零** `location.*`→403；T09 发 BE-canonical `AWS_*`/hadoop 键 + vended overlay（T09）。
  - **MEDIUM（cache/名映射/竞态/vended/auth/ref-pin）**：
    - latest-snapshot `(snapshotId, schemaId)` 二元组原子钉 vs paimon 单 long——schema-only-ALTER 漂移防御（T08）。
    - 老文件缺内嵌 field-id 的 name-mapping 回退（`by_parquet_field_id_with_name_mapping`，T06）。
    - 无 paimon-style `latest()` 回退；fail-loud + T07 build-vs-pin ordering 竞态窗（T06 §7，T07 闭合 iceberg 侧）。
    - vended overlay vs legacy 整-map 替换、live REST round-trip、无 `validToken()` 刷新（T09，每查询 `loadTable` 新鲜）。
    - Kerberized auth 真-KDC `doAs` 仅 live-e2e 验（跨引用 [DV-031]；read-vs-DDL `doAs` + 翻闸-authenticator-wiring 在 iceberg 复发，T10）。
    - tag/branch 钉 REF 名（`useRef` 跟后续 commit）vs 冻结 snapshot id（legacy parity，T07）。
    - 1-arg static-map `normalizeStorageUri` 用于 delete 路径直到 T09 接 vended token（T04）。
- **为何各项不单独阻塞翻闸**：每项**已在连接器内修/缓解**（Option A 字典、isPartitionBearing 默认、路径归一化、静态+vended 凭据发射），UT 已覆盖 wiring/逻辑；剩余仅「真 BE/live 行为」须 docker 确认。**别于 [DV-038]**：DV-038 是**未修的共享 fe-core 崩溃**，本条是**已修待 docker 实证**。
- **真值闸**：P6.6 docker——scan parity（分区裁剪行数 / native·JNI / position+equality delete / 静态+vended 凭据 round-trip）+ MVCC time-travel（AS OF / rename）+ Kerberized live。
- **关联**：[DV-038]（同 field-id 路族但 DV-038 未修）、[DV-031]（Kerberos）、[DV-025]（normalizeStorageUri）、[DV-027]（schema-evolution 字典 paimon 姊妹）、T03/T04/T06/T07/T08/T09 设计。

### DV-038 — P6.2 iceberg **翻闸阻塞 BLOCKER**：共享 fe-core field-id 路径 BE StructNode DCHECK 崩溃（1 主题 / 2 面，CI #969249 类）
- **状态**：🔴 **翻闸前（P6.6）必修**——未修则 iceberg top-N / rename+time-travel 查询挂 BE｜**日期**：2026-06-23｜**签字**：面 1 GLOBAL_ROWID 用户签字延后（2026-06-22）；面 2 待 P6.6 holistic
- **原计划位置**：T06 设计 §6 + T07 设计 §6 + T10 audit（line 69）+ [HANDOFF 顶部 🔴🔴 块](./HANDOFF.md) + [P6.2-T11 汇总设计 §翻闸阻塞](./designs/P6-T11-iceberg-scan-summary-design.md)
- **偏差描述**（**两面同一崩溃类**：BE `iceberg_reader.cpp` field-id 路径 `children_column_exists` StructNode `DCHECK` → 整 BE 崩）：
  - **面 1 — GLOBAL_ROWID 误分类（T06）**：top-N 延迟物化合成列 `GLOBAL_ROWID` 在 SPI 路径被通用 `PluginDrivenScanNode.classifyColumn` 归 **REGULAR**（非 SYNTHESIZED）→ field-id 字典让 BE 走 field-id 路径 → 该列不在字典 → DCHECK。连接器**无法修**（GLOBAL_ROWID 在连接器前被滤、无 iceberg field-id）；修在**共享 fe-core** `classifyColumn`（GLOBAL_ROWID→SYNTHESIZED），但 BE `paimon_reader.cpp` **无** SYNTHESIZED-GLOBAL_ROWID 处理器 → **盲改可能破 paimon top-N**。
  - **面 2 — `getColumnHandles` 无 snapshot 重载（T07，paimon 潜伏）**：rename + time-travel pin 下，从 current schema 建 pruned 列字典会丢被重命名 slot 的 field-id → 同一 DCHECK。**iceberg 侧已由 T07 Option A**（全 pinned-schema 超集字典）**连接器内闭合**，但**共享 fe-core seam `getColumnHandles(handle)` 仍无 snapshot-aware 重载** → **PAIMON 的 snapshot-id time-travel + rename 仍潜伏同一崩溃**。
- **为何 1 主题 / 2 面合并登记（Rule 12，不得静默丢面 2）**：两面是**同一** BE field-id 路径 StructNode DCHECK 崩溃类、**同需** holistic 共享 fe-core 修 + **paimon 影响分析**（可能 BE 协同）；T07 文档明确把面 2 比作面 1（「like the GLOBAL_ROWID blocker」）。审计 critic 实证 `isGateFlipBlocker=true` 计数为 **2**（非 1），故合并为单条 BLOCKER 但**显式记两面**。
- **影响范围**：iceberg 翻闸（P6.6）**前**必修，否则面 1（任意 iceberg top-N 延迟物化查询）/ 面 2（paimon·iceberg snapshot-id time-travel + 列重命名查询）挂 BE。**跨 paimon**（面 2 是既有 paimon 潜伏，本次首次显式登记）。
- **真值闸**：P6.6 docker——翻闸前必跑 iceberg top-N + time-travel+rename；须**先** holistic 修 + paimon 影响分析再翻闸。
- **关联**：[DV-026]（field-id 消费者复评触发，已预言此类 SPI-on iceberg/hudi 经 `ExternalUtil` 的崩溃）、[DV-027]（schema-evolution 字典 paimon 姊妹）、T06/T07/T10 设计、CI #969249。
- **后续动作（翻闸阻塞 checklist，P6.6 前必清）**：
  - [ ] fe-core `PluginDrivenScanNode.classifyColumn`：`GLOBAL_ROWID`→SYNTHESIZED（**先** paimon 影响分析）
  - [ ] BE `iceberg_reader.cpp` / `paimon_reader.cpp` SYNTHESIZED-`GLOBAL_ROWID` 处理器核对（可能 BE 协同）
  - [ ] fe-core `getColumnHandles(handle)` snapshot-aware 重载（关闭面 2 的 paimon 潜伏）
  - [ ] 翻闸冒烟：iceberg `SELECT ... ORDER BY ... LIMIT k`（top-N）+ paimon/iceberg `FOR TIME AS OF` + `ALTER ... RENAME COLUMN` 后查询

### DV-037 — P6-C2 FIX-C2-HDFS-XML：legacy HDFS `getHadoopStorageConfig()` 的 `fs.hdfs.impl.disable.cache=true` 未进 typed FE Configuration（pre-existing，非 C2 引入）
- **状态**：🟢 已登记（accept / 可转 follow-up）｜**日期**：2026-06-19｜**签字**：待用户
- **原计划位置**：[FIX-C2-HDFS-XML-design.md §Risk](./designs/FIX-C2-HDFS-XML-design.md)
- **偏差描述**：legacy `HdfsProperties` 的 FE `getHadoopStorageConfig()` 带 `fs.hdfs.impl.disable.cache=true`（`StorageProperties.ensureDisableCache`），typed `HdfsFileSystemProperties.backendConfigProperties` 从不加它（该键只在 `HdfsConfigBuilder` 运行期 `create()` 路上，`:44-48`）。
- **触发场景**：任何 paimon HDFS catalog 的 FE catalog-create Configuration——**与 C2 无关**：翻闸后 HDFS 本就对 `storageHadoopConfig` 零贡献，C2 前后该键都缺；C2 只补 XML 子项，不动此键。
- **新方案**：accept。Hadoop FS-cache 按 scheme+authority+ugi 缓存，benign；不在 C2（XML-resource gap）scope 内。
- **影响范围**：代码无（pre-existing）。可转独立 follow-up（若将来报 FS-cache 串扰）。
- **关联**：[task-list §P6-C2](./task-list-P6-fixes.md)、[DV-036]

### DV-036 — P6-C2 FIX-C2-HDFS-XML：DLF catalog 若另绑 HDFS storage，HDFS keys 会进 DLF HiveConf（legacy DLF 只 overlay OSS/OSS_HDFS）
- **状态**：🟢 已登记（accept）｜**日期**：2026-06-19｜**签字**：待用户
- **原计划位置**：[FIX-C2-HDFS-XML-design.md §Risk / Open Q1](./designs/FIX-C2-HDFS-XML-design.md)
- **偏差描述**：`HdfsFileSystemProperties` 实现 `HadoopStorageProperties` 后，`buildStorageHadoopConfig()` 对所有 flavor 共享；legacy DLF（`PaimonAliyunDLFMetaStoreProperties:90-96`）只 overlay OSS/OSS_HDFS storage，新 `DlfMetaStorePropertiesImpl.toDlfCatalogConf:141` 无条件 overlay 整个 `storageHadoopConfig`。故 DLF catalog 若也绑了 `HdfsFileSystemProperties`，HDFS keys 会进 DLF HiveConf。
- **触发场景**：DLF catalog 的 raw props 触发 `HdfsFileSystemProvider.supports()`（`dfs.nameservices` / `hdfs|viewfs|ofs|jfs`-scheme 裸 `fs.defaultFS` / `_STORAGE_TYPE_=HDFS` / `hadoop.kerberos.principal`）——对 Aliyun-OSS 的 DLF 是 nonsensical 配置（真 DLF 用 `oss.*`/`dlf.*` + `oss.hdfs.fs.defaultFS=oss://…`，非裸 `fs.defaultFS`）。
- **新方案**：accept。结果是 additive/inert 的 defaults-free HDFS keys，绝不破凭据/正确性；纯-OSS DLF（真实场景）byte-unchanged。修它需动 out-of-scope 的 DLF 路加 HDFS filter 去守一个 nonsensical 配置，不值。
- **替代方案**：DLF 路按 storage 类型 filter——拒（C2 不含 DLF；增复杂度守不可达场景）。
- **影响范围**：代码无（accept）。文档：本条 + 设计 Open Q1。
- **关联**：[task-list §P6-C2](./task-list-P6-fixes.md)、[DV-037]

### DV-035 — P4 MINOR/NIT cleanup：15 项 accept-as-deviation（M5.1 transient-only + 14 display/perf/text/inert/连接器-更-correct/假前提）
- **状态**：🟢 已登记（accept）｜**日期**：2026-06-12｜**签字**：用户 [D-057]
- **范围**：review §5/§7 去重 ~17 项 P4 MINOR/NIT 中，2 项已修（N10.1 `bcee91dcb52`、sentinel `4b2c2190dc2`，见 [D-057]），余 15 项 accept。完整逐项分类见索引表 DV-035 行；要点：
  - **M5.1（唯一 FUNCTIONAL，transient-only）**：sys-table 列举的远端 handle 预探，瞬时失败 → phantom「table not found」。**accept 理由（实现层证伪 critic 的「cheap fallback」）**：`getTableHandle` 的 swallow-非NotExist-为-empty 是有意+有测契约（`PaimonConnectorMetadataReadAuthTest:150` `failAuth→empty`）且是共享 existence 谓词（`PluginDrivenExternalCatalog:239/295/446`，含 P3 createTable remoteExists）；`listSupportedSysTables` 忽略 handle。无 surgical 零成本修，唯一干净修 = SPI no-handle list（surface churn + 重引「为不存在表列 sys-table」legacy quirk）或 broad retype（破契约 + 触 P3 fix）。
  - **假前提 ×2（M9.1/M9.2）**：review 声称连接器丢 HDFS default / 推 hive.* 到 BE——recon 证伪（连接器跑同 `CredentialUtils.getBackendPropertiesFromStorageMap` over 同 storage map，无 drop；hive.* 仅 FE-side HiveConf，never location.*）。**非偏差、是 review 误判**，登记以免重复追查。
  - **其余 12**：display（M10.1/M10.2/M10.3/M7.1）、perf（M6.1/M6.2）、text（N2.1/M3.1/N4.1/C2）、inert no-op（N3.1/M2.1）、连接器**更** correct（M4.1/M1.3）、diagnostic（M1.1）。均非 correctness regression。
- **meta**：completeness critic 的 fix 建议须落到代码契约/测试层再判 effort——M5.1 照单转 FIX 会做无 sanction 的 broad/SPI 改；实现层核查把它 flip 回 accept。
- **跨连接器**：hudi/iceberg full-adopter 多项同复发（display/text/假前提类），将来批量 close（与 [DV-028]/[DV-030]/[DV-031]/[DV-032]/[DV-033]/[DV-034] 同批考量）。
- **关联**：[D-057](./decisions-log.md)、[task-list §P4](./task-list-P5-rereview2-fixes.md)、recon `wf_6884d37b-8ef`

### DV-034 — P3-fix FIX-CREATE-TABLE-LOCAL-CONFLICT：plugin DDL op typed error-code 收敛成 generic DdlException（COSMETIC/error-code-ONLY，pre-existing 跨全 DDL op）

`FIX-CREATE-TABLE-LOCAL-CONFLICT`（[D-056](./decisions-log.md)）恢复了 createTable 的 case-B **correctness**（local-only 冲突 + `!IF NOT EXISTS` 改抛 typed `ERR_TABLE_EXISTS_ERROR` 1050），但**有意不动** error-code 残留：

- **case-A（createTable，remote-hit + `!IF NOT EXISTS`）**：plugin 仍 fall-through 到 `metadata.createTable`，由连接器（paimon SDK `TableAlreadyExistException`）→`DorisConnectorException`→桥 re-wrap 成 **generic `DdlException`(e.getMessage())**「Table 't1' already exists…」；legacy `PaimonMetadataOps:195` 在 FE 层先抛 **typed** `ERR_TABLE_EXISTS_ERROR`(1050)。outcome（拒绝 + 「already exists」文本）同，仅 numeric code 丢。
- **createDatabase/dropDatabase/dropTable**：同样 `catch(Exception)`→generic `DdlException`（`PaimonConnectorMetadata:731/798/832/756` + 桥 re-wrap），collapse 掉 legacy 的 1007/1008/1050/1109 typed code。
- **为何不在本 fix 收口**：① 非本 P3 finding（finding=case-B silent-create correctness）；② P3 audit 把 error-code parity 标 **cosmetic/AGREE**（error class + user-visible 文本两侧一致）；③ 修它须对每 DDL op 在桥/连接器边界统一 typed-code 透传机制，属跨全 op + 跨连接器（hudi/iceberg full-adopter 同）的 **P4 cleanup 批量**，非外科单点。
- **真值闸**：无功能影响；仅对 MySQL numeric-error-code-sensitive 的客户端脚本理论可感知。
- **跨连接器**：与 [DV-028]/[DV-030]/[DV-031] 同属翻闸后通用桥/连接器边界的语义收敛，将来批量 close。

### DV-033 — P5-fix#9 FIX-NATIVE-SUBSPLIT：split-weight / target-size 调度 nicety 不移植（PERF/调度-ONLY，pre-existing）

- **发现日期**：2026-06-12
- **发现 session / agent**：#9 recon（`wf_ad764bf6-1c9`，synthesizer 标 "parity nicety, not a blocker"）。
- **当前状态**：🟢 已登记（perf/调度-only，pre-existing；live-e2e 真值闸）
- **原计划位置**：[P5-fix-NATIVE-SUBSPLIT 设计](./tasks/designs/P5-fix-NATIVE-SUBSPLIT-design.md) §Out of scope
- **偏差描述**：legacy `fileSplitter.splitFile` 经 `splitCreator.create(path, start, length, fileLength, targetFileSplitSize, ...)` 在每个 `FileSplit` 上设 per-split weight + targetSplitSize，供 `FederationBackendPolicy` 做 backend 分配均衡。连接器 native sub-range（`buildNativeRange`）不设 `selfSplitWeight`/targetSplitSize。
- **为何可接受**：① **pre-existing**——翻闸后单-range native 路本就没设 weight（`buildNativeRange` 从未设、仅 JNI 路 `buildJniScanRange` 经 `computeSplitWeight` 设）；#9 不引入该缺口，只是把整文件 range 切成多 sub-range。② 纯**调度均衡质量**、非正确性、非并行度——#9 的目的（文件内并行）已达成（发多个 sub-range）。③ 连接器 SPI 无 per-range weight 喂入 FileSplit 的通道（`PaimonScanRange` 无 targetSplitSize 字段）。
- **影响范围**：仅 backend 分配的负载均衡质量；读结果与并行度均正确。
- **关联**：[D-055]、native-path weight 既有缺口
- **后续动作**：
  - [ ] 跨连接器：若要 weight-均衡，后续在 SPI/`PaimonScanRange` 加 weight 字段，与既有 native-path weight 缺口一并补（hudi/iceberg full-adopter 同需）。
  - [ ] live-e2e：观察大文件多 sub-range 的 backend 分配（均衡差异为预期、非正确性问题）。

### DV-032 — P5-fix#8 FIX-COUNT-PUSHDOWN：collapse-to-one 丢 legacy `>10000` 并行 count-split trim（PERF-ONLY，用户签字）

- **发现日期**：2026-06-12
- **发现 session / agent**：#8 recon（5-scout + 对抗 synthesizer workflow `wf_1ce48c93-325`，legacy-parity lens），用户在 scope 决策中明确选 collapse-to-one。
- **当前状态**：🟢 已登记（perf-only，结果恒等；live-e2e 真值闸）
- **原计划位置**：[P5-fix-COUNT-PUSHDOWN 设计](./tasks/designs/P5-fix-COUNT-PUSHDOWN-design.md) §Deviation
- **偏差描述**：legacy `PaimonScanNode:484-495` 收齐 count-eligible split 后按 `pushDownCountSum` 分流——`> COUNT_WITH_PARALLEL_SPLITS(10000)` 时 trim 到 `parallelExecInstanceNum * numBackends` 个 split 并 `assignCountToSplits` 把 total 均摊（BE 每 split 的 CountReader 再求和回 total）；`<=10000` 则 `singletonList(first)` 收一 split 携全 total。连接器 `PaimonScanPlanProvider.planScanInternal` **始终 collapse-to-one**（无论 `countSum` 大小都只发一个 count range 携全 total），因连接器**无** `numBackends`/`parallelExecInstanceNum`——它们是 fe-core scan-node-only（`PluginDrivenScanNode.getSplits(int numBackends)` 才有，连接器 SPI `planScan` 无）。**附（cosmetic）**：legacy 还在 post-loop 调 `setPushDownCount(sum)` 让 EXPLAIN 显示 `pushdown agg=COUNT (N)`（`FileScanNode` 节点级、display-only）；collapse-to-one **无 fe-core post-loop** 故 plugin 路 EXPLAIN 不显示该计数行。**纯展示差异**：count 经 per-range `table_level_row_count` 走另一条路达 BE（与 EXPLAIN 显示无关），结果与性能均不受影响。review 判为 non-blocking（adversarial workflow `wf_6ead7c2c-b58`，display-only、pre-existing override 模式）。
- **为何可接受**：纯 perf 偏差、**结果恒等**——单 CountReader 在一个 fragment emit `countSum` 个空行（无 IO、不读数据文件）而非 N 个并行；只在超大 count 时不并行化 count-emit。对比 legacy 的并行 trim 本身也只是优化（CountReader 极廉）。**fail-safe**：collapse-to-one 是 legacy `<=10000` 路径的普遍化，非新行为。
- **未采替代**：full-parity（连接器发 per-split + fe-core 按 numBackends trim+redistribute）——会把 count 语义耦进通用 `ConnectorScanRange`（fe-core 须读/改写各 range 的 row_count）、多 fe-core 代码、blast 大；per-split（不 collapse）——比 legacy 多 fragment。collapse-to-one 是连接器自包含、零 fe-core post-loop 数学的最简正确解。
- **影响范围**：仅 count 查询的 split 并行度（fragment 数）；count 结果与全表行数均正确。
- **关联**：[D-054]、[第二轮 review report](./reviews/P5-paimon-rereview2-2026-06-11.md)（M-2）、[DV-028]/[DV-030]/[DV-031]（同属「新连接器读法/翻闸 vs fe-core 既有约定」类缝）
- **后续动作**：
  - [ ] **live e2e（必经）**：超大 PK 表 `COUNT(*)` 验结果正确；EXPLAIN/profile 观察 count fragment 并行度（与 legacy 差异为预期、非回归）。
  - [ ] 跨连接器：hudi/iceberg full-adopter 若需 `>10000` 并行 count-split，可在 fe-core 加通用 trim hook 批量 close（与 DV-028/030/031 同批考量）。

### DV-015 — P4-T06e FIX-PRUNE-PUSHDOWN：端到端裁剪下推 wiring 无 fe-core 单测（KNOWN-LIMITATION）

- **发现日期**：2026-06-08
- **发现 session / agent**：FIX-PRUNE-PUSHDOWN clean-room review（workflow `w31i0vfo5`，test-quality lens，4 finding 全 verifier 判 minor/非 must-fix）
- **当前状态**：🟢 已登记（逻辑半 UT+mutation 守门，wiring 半 + 真实裁剪生效待 live e2e）
- **原计划位置**：[FIX-PRUNE-PUSHDOWN 设计](./tasks/designs/P4-T06e-FIX-PRUNE-PUSHDOWN-design.md) §Test Plan
- **偏差描述**：本 fix 三处产线点无 fe-core 端到端 UT：① `PluginDrivenScanNode.getSplits()` 的 pruned-to-zero 短路（`requiredPartitions!=null && isEmpty()→return emptyList()`）；② `PhysicalPlanTranslator` plugin 分支 `setSelectedPartitions(fileScan.getSelectedPartitions())` 注入；③ `getSplits→planScan` 6 参 requiredPartitions threading。原因：`PluginDrivenScanNode` 是 `FileQueryScanNode` 子类，裸构造需绕 ctor 链 + stub `getScanPlanProvider`/`buildColumnHandles`/`buildRemainingFilter`/`applyLimit`（无现成轻量 analyze/spy harness；同 [DV-014] 外表 bind harness 缺位）。
- **覆盖经**：① 最易错的三态映射逻辑（NOT_PRUNED→null / pruned-非空→names / pruned-空→空 list）由 `PluginDrivenScanNodePartitionPruningTest`（5 测）+ mutation（去 `!isPruned` 守卫双红）pin；② 名→PartitionSpec 转换由 `MaxComputeScanPlanProviderTest`（3 测）+ mutation（恒 emptyList 红）pin；③ wiring 半（短路/注入/threading 单变量直线流）+ **真实裁剪生效** 由 p2 live `test_max_compute_partition_prune.groovy` 覆盖——真值证据 = EXPLAIN/profile 仅扫目标分区（split 数/规划耗时 ≪ 全表）+ `WHERE pt='不存在'`→0 行且不建全分区 session。
- **为何可接受**：与既有约定一致（`HiveScanNodeTest`/legacy-MC/Hudi 的 translator 注入均无 translator 级测，`HiveScanNodeTest:99-115` 直构 node 调 setter）；fail-safe（默认 `selectedPartitions=NOT_PRUNED`→`resolveRequiredPartitions`→null→scan all，去 wiring 退化为修前全表扫**非丢数据**）。
- **影响范围**：仅测试覆盖层；产线行为正确。
- **关联**：[D-031]、[review-rounds](./reviews/P4-T06e-FIX-PRUNE-PUSHDOWN-review-rounds.md)、[复审 §B DG-1](./reviews/P4-maxcompute-full-rereview-2026-06-07.md)、[DV-014]（同类 harness 缺位）
- **后续动作**：
  - [ ] 待外表 scan 的 fe-core spy/analyze harness 落地（`MaxComputeScanNodeTest`/`PaimonScanNodeTest` 用 `Mockito.spy`+反射，可借鉴），补 `getSplits()` 短路 + threading 的 CI 级测，把 correctness 不变式从 live-only 提到 CI。
  - [ ] **live e2e（必经）**：真实 ODPS 跑 `test_max_compute_partition_prune.groovy`，并核 EXPLAIN/profile 证裁剪真正下推（行正确不足以证——修前行已正确）。

### DV-014 — P4-T06e FIX-BIND-STATIC-PARTITION：bind 期 full-schema 投影无 fe-core 单测（KNOWN-LIMITATION）

> 补登：本条索引行（见上）此前已录，详细记录段遗漏，现补齐（doc-sync 横切债）。

- **发现日期**：2026-06-07
- **发现 session / agent**：FIX-BIND-STATIC-PARTITION clean-room review（workflow `wi3mnjymb`/`wy299gtsh`/`wlwpw0b2s`，test-quality lens）
- **当前状态**：🟢 已登记（无 harness，parity + p2 覆盖；待外表 analyze harness 落地补）
- **原计划位置**：[FIX-BIND-STATIC-PARTITION 设计](./tasks/designs/P4-T06e-FIX-BIND-STATIC-PARTITION-design.md) / [D-030]
- **偏差描述**：`bindConnectorTableSink` 的 full-schema 投影（NULL 填充 + 分区列末尾 + 按位置投影）未被 connector-path 单测直接 pin——`bind()` 经 `RelationUtil.getDbAndTable` 真 Env 解析，外表 PluginDriven catalog 需连接器插件，无现成轻量 analyze harness（OLAP analyze 测仅覆盖 `createTable` 内表）。
- **覆盖经**：① 与 legacy `bindMaxComputeTableSink` 及 Iceberg 路径**共享** helper `getColumnToOutput`/`getOutputProjectByCoercion`（被既有 OLAP/Hive/Iceberg insert 测覆盖）；② 列选择 helper `selectConnectorSinkBindColumns` 单测 + 分布 full-schema 索引测；③ p2 live `test_mc_write_insert` Test 3/3b + `test_mc_write_static_partitions`。
- **关联**：[D-030]、[review-rounds](./reviews/P4-T06e-FIX-BIND-STATIC-PARTITION-review-rounds.md)、[DV-015]（同类 harness 缺位）
- **后续动作**：[ ] 待外表 analyze harness 落地补 bind 投影 CI 级测。

### DV-013 — P4-T06e FIX-WRITE-DISTRIBUTION：两处 planner 写分发 parity 微差（均非回归）

- **发现日期**：2026-06-07
- **发现 session / agent**：FIX-WRITE-DISTRIBUTION clean-room review（workflow `ww1g95bba`，Phase A parity/delivery lens）
- **当前状态**：🟢 已登记（非回归，接受；default `enable_strict_consistency_dml=true` 下与 legacy MC 同果）
- **原计划位置**：[FIX-WRITE-DISTRIBUTION 设计](./tasks/designs/P4-T06e-FIX-WRITE-DISTRIBUTION-design.md)（§"Known minor divergence — ShuffleKeyPruner" + §"Why no change in RequestPropertyDeriver"）
- **偏差描述**：
  - **① ShuffleKeyPruner**：`ShuffleKeyPruner.visitPhysicalConnectorTableSink`（通用 connector 分支，`:286-295`）缺 legacy `visitPhysicalMaxComputeTableSink`（`:272-283`）的 `enableStrictConsistencyDml==false → childAllowShuffleKeyPrune=true` 短路；通用分支恒 `required.equals(ANY)?true:false`。
  - **② local-sort under non-strict**：`enable_strict_consistency_dml=false` 时 `RequestPropertyDeriver` 对 connector sink（required≠GATHER）下推 `ANY` → 动态分区 hash+local-sort 需求被丢。
- **为何非回归**：default `enable_strict_consistency_dml=`**`true`**（`SessionVariable.java:1566`）下——① 两路均 `required≠ANY → prune=false`（**同果**）；② `RequestPropertyDeriver` 下推 `getRequirePhysicalProperties()` = hash+local-sort（**enforce**，与 legacy MC 同）。仅 non-strict（用户显式关）时分歧：① 通用分支**少剪**（更保守 = missed optimization，无正确性损）；② local-sort 被丢——但 **legacy MC 在 non-strict 下亦丢**（`visitPhysicalMaxComputeTableSink` 同样下推 ANY）→ parity，非本 fix 引入。clean-room review Phase B 把 ① 多数 refute 为 non-regression。
- **影响范围**：仅 `enable_strict_consistency_dml=false` 的 MaxCompute 动态分区写；default 不触及。① 纯性能（少剪 shuffle-key）；② 与 legacy 同行为。
- **关联**：[D-029]、[review-rounds](./reviews/P4-T06e-FIX-WRITE-DISTRIBUTION-review-rounds.md)、[复审 §A.NG-2/NG-4](./reviews/P4-maxcompute-full-rereview-2026-06-07.md)
- **后续动作**：
  - [ ] 如需 non-strict 下完全 parity：给 `ShuffleKeyPruner` 通用 connector 分支补 `enableStrictConsistencyDml` 短路（影响 jdbc/es 共享分支，超本 fix scope）

### DV-012 — P4-T04：`partition_columns` 取 ODPS 表列（源不同、值同）

- **发现日期**：2026-06-06
- **发现 session / agent**：P4 Batch B session（P4-T04 写计划实现，核读 legacy `MaxComputeTableSink.bindDataSink`）
- **当前状态**：🟢 已落地（P4-T04，值等价）
- **原计划位置**：[P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md)（港 legacy `MaxComputeTableSink` 静态字段）
- **偏差描述**：legacy `MaxComputeTableSink.bindDataSink` 填 `TMaxComputeTableSink.partition_columns`(field 14) 取 `targetTable.getPartitionColumns()`（fe-core Doris `Column` 名）。连接器 import-gate 禁 fe-core `catalog.Column`，且 planWrite 持的是 `MaxComputeTableHandle`（携 odps-sdk `Table`）非 fe-core 表。
- **新方案**：连接器 `MaxComputeWritePlanProvider.planWrite` 取 `mcHandle.getOdpsTable().getSchema().getPartitionColumns()`（odps-sdk `com.aliyun.odps.Column` 名）。**源不同（ODPS schema vs fe-core Column）、值同（分区列名字符串）**——BE 经 field 14 收到相同分区列名 list。同源亦用于静态分区串的列序（`MCTransaction.beginInsert` 用 fe-core 列序，连接器用 ODPS 列序，序同）。
- **影响范围**：连接器 `MaxComputeWritePlanProvider`（dormant，gate 关，零 live）。行为等价：BE 收到的 `partition_columns` 内容不变。
- **关联**：P4-T04、[P4-T04 设计](./tasks/designs/P4-T04-write-plan-design.md)、[D-025]

---

### DV-011 — P4-T03：连接器事务 block 上限 + 异常类型（import-gate 禁 fe-core common）

- **发现日期**：2026-06-06
- **发现 session / agent**：P4 Batch B session（P4-T03 写前核实 import-gate 边界：`org.apache.doris.common.{Config,UserException}` 均在禁列）
- **当前状态**：🟢 已修正（P4-T03 硬编 → GC1 经 session-property 透传恢复 fe.conf 可调性，`95575a4954d`）
- **原计划位置**：[P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md)（港 legacy `MCTransaction` block 分配 + commit）
- **偏差描述**：legacy `MCTransaction.allocateBlockIdRange` 用 fe-core `Config.max_compute_write_max_block_count`（默认 20000，fe.conf 可调）作上限、并 `throws UserException`。连接器 import-gate 禁 `org.apache.doris.common.*`（含 `Config`/`UserException`），二者均不可 import。
- **新方案**：① 上限改连接器常量 `MaxComputeConnectorTransaction.MAX_BLOCK_COUNT = 20000L`（镜像 legacy 默认值，**丢 fe.conf 可调性**；Rule 2 不投机，如需再经 `MCConnectorProperties` 暴露）。② 校验失败抛 `DorisConnectorException`（unchecked；SPI `ConnectorTransaction.allocateWriteBlockRange` 面无 checked throws，W4 `PluginDrivenTransaction` 适配）。
- **影响范围**：连接器 `MaxComputeConnectorTransaction`（dormant，gate 关，零 live）。行为：block 上限值不变（20000），仅来源 Config→常量；异常类型 UserException→DorisConnectorException（语义等价的写失败）。
- **关联**：P4-T03、[P4-T03 设计](./tasks/designs/P4-T03-write-txn-design.md)、[D-024]
- **后续动作**：
  - [x] 已恢复 fe.conf 可调（GC1 FIX-BLOCKID-CAP-CONFIG，`95575a4954d`）：经 **session-property 透传**——fe-core `ConnectorSessionBuilder.extractSessionProperties` 注入 `Config.max_compute_write_max_block_count`（镜像既有 `lower_case_table_names`），连接器 `MaxComputeConnectorMetadata.resolveMaxBlockCount` 读 `ConnectorSession.getSessionProperties()` 透传 ctor。**非**原拟 `MCConnectorProperties`（那是 catalog-scoped、错 scope）；本机制读 fe-core 全局 Config = true legacy parity。

### DV-010 — P4-T01：共享 fe-core ConnectorColumnConverter 丢 CHAR/VARCHAR 长度，特判修复（用户签字）

- **发现日期**：2026-06-06
- **发现 session / agent**：P4 Batch A session（P4-T01 启动前 code-grounded 核读 `ConnectorColumnConverter.toConnectorType` + `ScalarType`：CHAR/VARCHAR 长度存 `len`、`getScalarPrecision()` 返 `precision`=0；既有 `ConnectorColumnConverterTest` 无 CHAR/VARCHAR 断言）
- **当前状态**：🟢 已修正（P4-T01；fe-core `ConnectorColumnConverter` 特判 + 回归测 `testCharVarcharLengthPreserved`，Tests run 9/0F0E）
- **原计划位置**：P4-T01 原框定「连接器-only、gate 关」；`ConnectorColumnConverter.toConnectorType`（P0-T15 期建）ScalarType 分支统一用 `getScalarPrecision()`/`getScalarScale()`
- **偏差描述**：连接器 `createTable` 消费的 `ConnectorCreateTableRequest` 列类型经 `ConnectorColumnConverter.toConnectorType(Type)` 产生；其 ScalarType 分支对 CHAR/VARCHAR 用 `getScalarPrecision()`（=`precision` 字段，CHAR/VARCHAR 默认 0），而长度实存 `len`（`getLength()`）→ 请求里 CHAR(n)/VARCHAR(n) **丢长度**（legacy `dorisScalarTypeToMcType` 用 `getLength()` 保留）。这是 P0 转换器的**逆一致性 bug**（其逆向 `convertScalarType` + 连接器 `MCTypeMapping` 约定「CHAR/VARCHAR 长度在 precision 字段」），是 CHAR/VARCHAR DDL 经 SPI 真正达 parity 的唯一路径。
- **新方案**（用户 AskUserQuestion 签字「修 fe-core 转换器」）：`toConnectorType` 特判 CHAR/VARCHAR，把 `getLength()` 写入 ConnectorType precision 字段（与逆向约定一致）；其余类型不变；加回归测 `ConnectorColumnConverterTest#testCharVarcharLengthPreserved`。
- **替代方案**：连接器侧对 CHAR/VARCHAR 缺长度 fail-loud + 记 OQ 推迟（保 Batch A 连接器-only 边界，但 CHAR/VARCHAR DDL 暂不可用）——用户否决。
- **影响范围**：
  - 代码：fe-core `ConnectorColumnConverter.toConnectorType`（+ import `PrimitiveType`）+ test。**触碰共享 P0 代码**：对 live 的 jdbc/es CREATE TABLE CHAR/VARCHAR 行为变更（「丢长度」→「保留长度」，严格更正确，低风险）。
  - 文档：本条 + [tasks/P4](./tasks/P4-maxcompute-migration.md) + [PROGRESS](./PROGRESS.md)（§四/§六计数）。
  - 计划：P4-T01 范围从「连接器-only」微扩至含 1 处 fe-core 转换器修复。
- **关联**：P4-T01、P0-T15（converter）、[D-023]
- **后续动作**：
  - [x] 修 `toConnectorType` + 回归测（P4-T01）
  - [ ] Batch E：连接器 DDL parity 测覆盖 CHAR/VARCHAR 端到端

### DV-009 — W5 写 sink 收口位置与 RFC/handoff 措辞不符：plugin-driven 写已有专路，改为 layer planWrite

- **发现日期**：2026-06-06
- **发现 session / agent**：W-phase 实现 session（W5 启动前 2 路 Explore code-grounded recon：sink 入参 + nereids 写 sink 接线；主线 firsthand 核读 `PhysicalPlanTranslator.visitPhysicalConnectorTableSink` / `planner/PluginDrivenTableSink`）
- **当前状态**：🟢 已修正（W5 commit `9ebe5e27fa4`；用户 AskUserQuestion 签字「Corrected W5 (layer planWrite)」）
- **原计划位置**：[写 RFC §5.5 / §12 W5](./tasks/designs/connector-write-spi-rfc.md)、[HANDOFF W5 锚点](./HANDOFF.md)——原措辞：「新建 fe-core `PluginDrivenTableSink` + `PhysicalPlanTranslator` 各 `visitPhysicalXxxTableSink`（hive/iceberg/mc）→ `planWrite()`，保 PhysicalXxxSink fallback」。
- **偏差描述**：RFC/handoff 写于不知既有路径之时。实测（recon + firsthand 核读）：
  1. `PluginDrivenTableSink` **已存在**（`planner/PluginDrivenTableSink.java`，P0/P1 JDBC 期建），非新建。
  2. plugin-driven 写 INSERT **不**走 `visitPhysicalHive/Iceberg/MaxComputeTableSink`（那 3 个服务 legacy 非 plugin-driven 表）；走专路 `UnboundConnectorTableSink → LogicalConnectorTableSink → PhysicalConnectorTableSink → visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:644`），已据 `ConnectorWriteConfig`（config-bag）建 `PluginDrivenTableSink`。mc/hive/iceberg 迁 plugin-driven 后走此专路 → 在那 3 个 concrete 方法加 planWrite 路由是**死代码**。
  3. 两写-sink 模型并存：既有 **config-bag**（连接器返 `ConnectorWriteConfig` 属性包，fe-core 建 `THiveTableSink`/`TJdbcTableSink`；表达不了 mc/iceberg）⊥ 新 **opaque-sink**（W1 `ConnectorWritePlanProvider.planWrite()` 连接器自建 `TDataSink`，RFC §5.5 E 决策，可泛化）。RFC 未察 config-bag 已存在，故未调和二者。
- **新方案**（用户签字）：在既有 `visitPhysicalConnectorTableSink` + `PluginDrivenTableSink.bindDataSink` 上 **layer** `planWrite()` 为优先路径（`connector.getWritePlanProvider() != null` 时），config-bag 为 fallback。**不动** 3 个 concrete visit 方法。零行为变更（无连接器 override `getWritePlanProvider`，jdbc 仍走 config-bag）。`ConnectorWriteHandle`/`ConnectorSinkPlan`（W1）形状经使用确认充分，无需改。
- **缩界（R12 不静默）**：overwrite / 静态分区 / writePath 等 connector-specific write context 的 handle 填充留 P4 adopter（base `InsertCommandContext` 为空 marker，无通用 overwrite；强行 instanceof 子类会再耦合 fe-core）。W5 仅建 seam（空 context）。

---

### DV-008 — P3-T07 parity 暴露两处 SPI↔legacy 偏差：列名 casing 当场修；Hudi meta-field 纳入推迟批 E

- **发现日期**：2026-06-05
- **发现 session / agent**：P3 批 C session（T07 启动前 5-agent code-grounded recon workflow `p3-t07-recon`：cow-mor / legacy-types / spi-types / hms-surface / hive-surface + 主线核读 `HudiConnectorMetadata`/`HudiTypeMapping`/`HMSExternalTable.initHudiSchema`/`ThriftHmsClient`）
- **当前状态**：🟢 已修正（gap-1 casing 已修 + 测；gap-2 meta-field 推迟批 E 实证）
- **原计划位置**：[tasks/P3 §批 C/T07](./tasks/P3-hudi-migration.md)（「parity 测试——SPI `HudiConnectorMetadata` schema/partition 输出 vs legacy `getHudiTableSchema`」）——原计划隐含假定 SPI schema 输出与 legacy parity，仅需写测试验证
- **偏差描述**：parity recon 实证 SPI avro→column 变换与 legacy `HMSExternalTable.initHudiSchema` 有两处偏差（其余逐类型一致，见设计备忘矩阵）：
  1. **gap-1 列名 casing**：SPI `HudiConnectorMetadata.avroSchemaToColumns` 用 `field.name()` 原样；legacy 在 `HMSExternalTable.java:745` `toLowerCase(Locale.ROOT)`（**仅顶层列名**；嵌套 struct 字段名两侧均不降）。mixed-case avro 列名时 SPI 保留原 case → 破 parity（BE name-match 大小写敏感，见 DV-006 / T03）。
  2. **gap-2 Hudi meta-field 纳入**：SPI `getSchemaFromMetaClient` 调无参 `TableSchemaResolver.getTableAvroSchema()`；legacy `getHudiTableSchema:852` 调 `getTableAvroSchema(true)`。`true` 很可能强制纳入 `_hoodie_*` meta 列，无参默认随 Hudi 版本/表配置（`populateMetaFields`）变 → 可能改变列集合。无真实 metaclient 不可单测判定（同 T03 族）。
- **触发场景**：T07 parity recon（golden-value 法，因 fe-core 只依赖 fe-connector-api/-spi、不依赖具体连接器模块，无跨模块编译路径）+ 用户 AskUserQuestion 签字（2026-06-05，「Also fix casing now」+「Focused baseline」）。
- **新方案**：
  - **gap-1 当场修**（用户签字）：`avroSchemaToColumns` 顶层列名改 `toLowerCase(Locale.ROOT)`，镜像 legacy:745（仅顶层；嵌套 struct 名保持 raw，两侧一致）。已核安全：`ThriftHmsClient.convertFieldSchemas:303` 用 `fs.getName()` 不防御降字，但 Hive Metastore 自身存小写标识符 → 降 avro 路径列名与小写 HMS partition key 对齐（改善 `getColumnHandles` 匹配），无回归。`avroSchemaToColumns` 由 `private`→package-private `static`（零行为变更，使可单测）。
  - **gap-2 推迟批 E**（DV-006 同族）：无真实 fixture 不可判定 + 属 schema-evolution/meta-field 机制，与 hive/HMS migration 一并实证。T07 parity 测不依赖该差异（测纯 avro→column 变换）。
  - **缩界（R12 不静默）**：`ThriftHmsClient` 源头防御性降字（与 hive 模块共享）**不在 T07 改**——触碰 hive 行为属 P7/批 E。
- **替代方案**：(gap-1) 不修、仅 pin 现状 + 记 DV 推批 E（precedent T03/T05）——用户否决，选当场修（trivially-correct，对齐 legacy + 小写 HMS）；(gap-2) 当场加 `(true)`——否决（无真实 metaclient 不可验证语义，脆测）。
- **影响范围**：
  - 文档：本条 + [tasks/P3](./tasks/P3-hudi-migration.md)（T07 ✅ + 验收 + 阶段日志）+ [PROGRESS](./PROGRESS.md)（§一/二/三/四/六/七）+ [connectors/hudi.md](./connectors/hudi.md)（概况 + playbook 12 + 进度日志）+ [HANDOFF](./HANDOFF.md)。
  - 代码：gap-1 `HudiConnectorMetadata.avroSchemaToColumns`（降字 + 可见性）+ 6 测试文件（hudi 3 改/新 + hms 1 + hive 2）；gap-2 零代码。
  - 计划：批 C = {三模块测试基线 ✅, COW/MOR schema parity ✅, gap-1 casing 修 ✅}；gap-2 meta-field 入批 E。
- **关联**：P3-T07、DV-006（同族 schema-evolution 推批 E）、P3-T10/T11（批 E）、[D-019](./decisions-log.md)（hybrid）、[`designs/P3-T07-test-baseline-design.md`](./tasks/designs/P3-T07-test-baseline-design.md)
- **后续动作**：
  - [x] gap-1 casing 修 + `HudiSchemaParityTest` casing pin（顶层降、嵌套 struct 名保留）
  - [x] 三模块测试基线（hms `HmsTypeMappingTest` 12 / hive `HiveFileFormatTest` 6 + `HiveConnectorMetadataPartitionPruningTest` 8 / hudi `HudiTypeMappingTest`+7 + `HudiSchemaParityTest` 3 + `HudiTableTypeTest` 4 = 33 全绿）
  - [ ] 批 E：gap-2 meta-field 纳入（`getTableAvroSchema(true)` vs 无参）真实 fixture 实证
  - [ ] 批 E/P7：`ThriftHmsClient` 源头防御性降字（与 hive 共享）

### DV-007 — P3 批 B scope 校正：T05 `listPartitions*` override 推迟批 E；T06 MVCC 保持 default opt-out（非抛异常 override）

- **发现日期**：2026-06-05
- **发现 session / agent**：P3 批 B session（T05/T06 启动前 5-reader code-grounded recon workflow：hudi-current / hudi-resolve / hive-ref / spi-invoke / mvcc-t06 + 主线核读 `HudiConnectorMetadata`/`HiveConnectorMetadata` 全文 + grep fe-core 调用方）
- **当前状态**：🟢 已修正（T05 applyFilter EQ/IN 裁剪已落地 commit `10b72d4`；list*/MVCC 完整实现入批 E）
- **原计划位置**：[HANDOFF.md 未完成 #1/#2](./HANDOFF.md)（「T05：`listPartitions/listPartitionNames/listPartitionValues` override + 真实 applyFilter EQ/IN 分区裁剪」；「T06：大概率**显式 unsupported**（与 T04 fail-loud 一致）」）+ [tasks/P3 §T05/T06](./tasks/P3-hudi-migration.md)
- **偏差描述**：原计划把 T05 的「`listPartitions*` override」与「applyFilter 裁剪」并列为批 B 交付；并暗示 T06 应**新增抛异常的 MVCC override**。recon 实测两点前提失真：
  1. **T05 `listPartitions*` 零 live caller + Hive 不 override**：SPI `ConnectorMetadata.listPartitionNames/listPartitions/listPartitionValues` 在 fe-core **无任何调用方**——`PluginDrivenScanNode` 不调用（分区经 `applyFilter`→`prunedPartitionPaths`→`resolvePartitions` 链路）；`ShowPartitionsCommand`/`HudiExternalMetaCache`/`MetadataGenerator` 调的是 **legacy** metastore 路径（`dorisTable.getRemoteName()`），非 SPI。对标 `HiveConnectorMetadata`（批 B 基准）**也不 override** 这三方法。→ 现 override = 不可测的死代码（违 R2 nothing speculative / R9 测意图）。
  2. **T06「显式 unsupported」违 SPI opt-out 约定**：三个 MVCC 方法 default 即 `Optional.empty()`（= 不支持），`FakeConnectorPluginTest` 有显式断言；`Iceberg`/`Paimon`/`Hive`/`Trino` **全部依赖 default**，无一 override；MVCC 方法**无 production caller**（仅测试用 adapter）；且 T04 已在唯一可触发点（time-travel）`visitPhysicalHudiScan` 抛 `AnalysisException`。→ 新增抛异常 override = 唯一打破约定 + 不可达死代码（违 R11 conformance / R3 surgical）。
- **触发场景**：T05/T06 启动前 recon + grep fe-core 调用方；用户 AskUserQuestion 签字（2026-06-05，「Pruning only, defer list*」+「Keep defaults + document」）。
- **新方案**：
  - **T05** = 仅 applyFilter 真实 EQ/IN 裁剪（忠实镜像 Hive 7 步 + 7 helper，保留 `List<String>` 路径表示与 `-1` 上限）；`listPartitions*` override **推迟批 E**（届时 fe-core 长出 SPI 消费 + `SHOW PARTITIONS` 改走 SPI 时一并做）。已落地 `10b72d4`（8 单测、checkstyle 0、import-gate 通过）。
  - **T06** = **不 override，保持 default `Optional.empty()` opt-out + 文档化**（零代码）；正确的 fail-loud 已在 T04 的 translator 守卫。完整 MVCC（`HudiMvccSnapshot`、snapshot 透传、增量时序）入批 E。见 [`designs/P3-T06-mvcc-design.md`](./tasks/designs/P3-T06-mvcc-design.md)。
- **替代方案**：(T05) 现 override 三方法委托 HMS——否决（死代码、无可测意图、Hive 无先例）；(T06) 新增抛异常 override——否决（破 opt-out 约定、不可达、与全体连接器分叉、T04 已覆盖）。
- **影响范围**：
  - 文档：本条 + [tasks/P3](./tasks/P3-hudi-migration.md)（T05 ✅ 裁剪 + T06 ✅ 决策 + 验收标准 + 阶段日志）+ [PROGRESS](./PROGRESS.md)（§一 P3 / §三 / §四 / §六计数）+ [connectors/hudi.md](./connectors/hudi.md)（E5/E10 + 进度日志）。
  - 代码：T05 已合入 `10b72d4`（applyFilter 裁剪 + 单测）；T06 零代码。
  - 计划：批 B 范围由 {T05 裁剪+list* override, T06 throwing override} 收为 {T05 裁剪 ✅, T06 keep-defaults ✅}；list*/完整 MVCC 与 T03/T09–T11 同批 E。
- **关联**：[DV-005](#dv-005--p3-hudi-的hms-over-spi-前置依赖与代码实际状态不符真正阻塞是-catalog-模型错配)（其后续动作「listPartitions override + 真实 applyFilter 裁剪」本条落地裁剪部分）、P3-T05、P3-T06、P3-T10/T11（批 E）、[D-019](./decisions-log.md)（hybrid）、[P3-T04](./tasks/designs/P3-T04-fail-loud-design.md)
- **后续动作**：
  - [x] T05 applyFilter EQ/IN 裁剪 + 单测（`10b72d4`）
  - [ ] 批 E：`listPartitions*` override（fe-core SPI 消费就绪 + `SHOW PARTITIONS` 走 SPI 后）
  - [ ] 批 E：完整 MVCC（`HudiMvccSnapshot` + snapshot 透传 + 增量时序），time-travel 从 T04 fail-loud 转为正确快照

### DV-006 — P3-T03（schema_id / history_schema_info）不是 model-agnostic 的批 A SPI-surface 修复；推迟到批 E

- **发现日期**：2026-06-05
- **发现 session / agent**：P3 批 A session（T03 启动前 code-grounded recon：4-reader workflow 读 SPI hook + Paimon/ES 参照 + legacy 路径 + thrift/BE 消费端；主线对 BE `table_schema_change_helper.h` 二次核读）
- **当前状态**：🟡 推迟（批 E，并入 hive/HMS migration）
- **原计划位置**：[HANDOFF.md 关键认知 1b HIGH ①](./HANDOFF.md) + [DV-005 后续动作 ①](#dv-005--p3-hudi-的hms-over-spi-前置依赖与代码实际状态不符真正阻塞是-catalog-模型错配) + [tasks/P3 §P3-T03](./tasks/P3-hudi-migration.md)：「schema_id/history 缺→退化名匹配；可经现有 SPI hook `populateScanLevelParams`（Paimon/ES 已 override）+ `HudiScanRange` 设 schema_id 修复，**无需 fe-core 改动**」
- **偏差描述**：原评估认为 ① 是「多在 SPI surface 内可修」的 model-agnostic 修复。recon 实测发现**前提不成立**：
  1. **BE 语义**（`be/src/format/table/table_schema_change_helper.h:219-267`）：`history_schema_info` **unset** → `by_parquet_name`/`by_orc_name`（**鲁棒名匹配**，处理大小写 / 缺列）——**即当前 SPI hudi 路径行为**；`current_schema_id == file_schema_id` → **`ConstNode`**（`:92-121`）= **纯 identity-by-name**、**大小写敏感**、假设精确匹配（其注释自陈需注意大小写）；id 不同 → `by_table_field_id`（**唯一**做 field-id / 改名 / evolution 的路径）。
  2. **「Paimon/ES 已 override」前提失真**：二者 override `populateScanLevelParams` 是为 **predicate / docvalue**，**并不设** schema evolution 元数据（recon 实证）——**无任何 SPI 先例**发 schema_id/history。
  3. **连接器缺料**：`HudiColumnHandle` **无 field id**（仅 `name`/`typeName` 串/`isPartitionKey`）；SPI hudi 连接器**无 Hudi `InternalSchema` 版本跟踪**（legacy 走 `getCommitInstantInternalSchema`）；连接器模块**无 type→`TColumnType` thrift 转换**（legacy 在 fe-core `ExternalUtil.getExternalSchema`，import gate 禁止复用）。
  4. **裸基线会回归**：若仅设 `current==file==-1`（→ ConstNode）= identity-by-name 大小写敏感，**严格弱于**当前名匹配（丢大小写 / 缺列处理）——**净回归**；而真正的 field-id evolution 路径需上述全部缺料。
- **触发场景**：T03 启动前 recon + 主线核读 BE `gen_table_info_node_by_field_id` / `ConstNode` / `StructNode`。
- **新方案**：**T03 推迟到批 E**，与 hive/HMS migration 一次性建齐机制（column-handle field id + Hudi `InternalSchema` 版本 + Avro/ConnectorType→`TColumnType` thrift + `populateScanLevelParams` 设 current+history + 每-split `THudiFileDesc.schema_id`）。批 A 不发任何 schema 元数据（保持现状名匹配，**零回归**），不 ship 裸 ConstNode 基线。用户已签字（2026-06-05，AskUserQuestion「Defer T03 to batch E」）。
- **替代方案**：(a) 批 A 内建全套 field-id/InternalSchema/type→thrift 机制——否决（大、与批 E 重叠、触碰 live 可读 schema 路径、回归风险）；(b) 裸 ConstNode 基线——否决（净回归大小写/缺列）。
- **影响范围**：
  - 文档：本条 + [tasks/P3](./tasks/P3-hudi-migration.md)（T03 移入批 E、备注现状名匹配 + evolution gap）+ [PROGRESS](./PROGRESS.md)（§三 parity 行 / §六计数）+ [connectors/hudi.md](./connectors/hudi.md)。
  - 代码：无（recon + 决策，零改动）。
  - 计划：批 A 范围由 {T02,T03,T04} 收为 {T02 ✅, T04}；T03 与 T09–T11 同批 E。
- **关联**：[DV-005](#dv-005--p3-hudi-的hms-over-spi-前置依赖与代码实际状态不符真正阻塞是-catalog-模型错配)（其后续 ① 本条修正）、P3-T03、P3-T10/T11（批 E）、[D-019](./decisions-log.md)（hybrid）、R-001
- **后续动作**：
  - [ ] 批 E：连接器 schema field-id + InternalSchema 版本 + type→thrift + `populateScanLevelParams` + per-split `schema_id`（faithful field-id evolution parity）
  - [x] 现状行为登记：SPI hudi 走 BE 名匹配（`by_parquet_name`/`by_orc_name`），common 无 evolution 可用；改名 / reorder-with-evolution 退化（非崩溃）

### DV-005 — P3 hudi 的「HMS-over-SPI 前置依赖」与代码实际状态不符；真正阻塞是 catalog 模型错配

- **发现日期**：2026-06-04
- **发现 session / agent**：P3 启动 recon session（8-agent code-grounded workflow + 2 路对抗验证；verdict `hmsMetadataOverSpiReady=false`, high confidence）
- **当前状态**：🟡 待修正（P3 catalog 模型决策，待用户签字）
- **原计划位置**：[connectors/hudi.md](./connectors/hudi.md)（「P3 启动前必须 P5 paimon 或 P7 hive 进入到至少完成 hms metadata 路径」）、[master plan §3.4/§3.8](./00-connector-migration-master-plan.md)、决策 D-005（用 `tableFormatType` 区分 DLA）
- **偏差描述**：原计划假设 HMS-over-SPI 元数据读路径要等 P5/P7 才落地、是 P3 的前置硬依赖。recon 实测（`branch-catalog-spi` HEAD `0793f032662`）发现该读路径**代码早已存在且非 stub**（源自更早的 #62183/#62821，一直 dormant 在 gate 后）：
  - `fe-connector-hms` = 共享 **HMS Thrift 客户端库**（`HmsClient`/`ThriftHmsClient`，**不是** ConnectorMetadata）；
  - `fe-connector-hive` `HiveConnectorMetadata`(type `"hms"`) 真实读路径 + applyFilter 真分区裁剪；
  - `fe-connector-hudi` `HudiConnectorMetadata`(type `"hudi"`) 从 Hudi Avro MetaClient 读 schema（HMS fallback）+ COW/MOR 探测 + `HudiScanPlanProvider` 快照扫描；
  - D-005 区分符 `ConnectorTableSchema.tableFormatType`(`:33/:58`) 已存在并被各连接器写入。

  但全部 **dormant**：`CatalogFactory.SPI_READY_TYPES = {jdbc, es, trino-connector}`(`CatalogFactory.java:52`) 不含 hms/hudi → HMS 系 catalog 永远走 legacy `HMSExternalCatalog`（零 live caller）。**真正阻塞不是缺 HMS 读码，而是 catalog 模型错配**：现存连接器注册独立 `"hudi"` catalog type（`HudiConnectorProvider.getType()=="hudi"`），而 Doris 真实模型是 hudi 寄生在 `"hms"` catalog 内、以 `HMSExternalTable.DLAType.HUDI` 暴露；fe-core 无 `"hudi"` catalog type，且 `PluginDrivenExternalTable` 从不消费 `tableFormatType`（只读 `getColumns()`，按 catalog TYPE 字串路由）→ 单个 `"hms"` 连接器没有 per-table HUDI/HIVE/ICEBERG 分流的 SPI 机制。附带确认缺口：增量读无 SPI 表示（P1-T04 `visitPhysicalHudiScan` SPI 分支丢弃 `getIncrementalRelation()`；MVCC trio 未实现；4 个 `*IncrementalRelation` 仍在 fe-core）；hive/hudi 未 override `listPartitions*`（Hudi applyFilter 列全部分区不裁剪，Hive applyFilter 做 EQ/IN 裁剪）；三模块零测试。**已验证非阻塞**：SPI scan/split 通用链路（`PluginDrivenScanNode.planScan`→BE）已被合入的 trino-connector 走通；hudi-specific 的「单 ScanNode 混合 COW-native + MOR-JNI 每-split 格式」正确性才是待验证项。
- **触发场景**：用户准备启动 P3，要求 code-grounded 确认 HMS 就绪情况。
- **新方案**：P3 不再以「等 P5/P7 交付 HMS-over-SPI」为前提；改为 (1) recon SPI scan/split 路径（hudi-specific 正确性），(2) 写 catalog 模型决策备忘（见下），用户签字后再编码。**不要直接 flip `SPI_READY_TYPES`**。
- **替代方案（catalog 模型，待用户决策）**：
  - **(a) hms-first**：`HiveConnectorProvider(type="hms")` 接入 `PluginDrivenExternalCatalog` + fe-core 消费 `tableFormatType` 分流，hudi 作薄增量。一次命中真正架构阻塞、契合现存 `type="hms"` 设计；但把 P7(hive/HMS) 范围拉进 P3、触碰 live 重度使用的 HMS 路径、零测试网，回归风险大。
  - **(b) gate 后建脚手架**：先做 format-dispatch / 增量 SPI hook / MVCC + 补测试（design+stub，不动 live 路径、零回归）；但 hudi 不单独端到端可用，推迟模型决策。
  - **(c) 直接 flip gate** —— **否决**（模型错配下 `"hudi"` provider 不可达；live hms catalog 推到未测 SPI；增量丢失；高回归）。
- **影响范围**：
  - 文档：本条 + [connectors/hudi.md](./connectors/hudi.md)（已加更正注）+ [PROGRESS.md](./PROGRESS.md)（§一 P3 / §二看板 / §四 / §六 / §七 已同步）+ [HANDOFF.md](./HANDOFF.md)（P3 起点）✅；master plan / hudi.md 章节正文待 P3 按选定模型重写。
  - 代码：无（recon only）。
  - 计划：P3 性质从「等依赖」变为「先定模型 + 补 SPI 分流/增量/测试」；可能与 P7(hive/HMS) 部分合并或重排序——待模型决策。
- **关联**：D-005、P1-T04（incrementalRelation gap）、R-001（image 兼容）、P3、master plan §3.4/§3.8
- **后续动作**：
  - [x] P3 session：recon SPI scan/split —— **完成**（verdict：混合 COW-native/MOR-JNI 非问题、与 legacy 结构等价；plumbing 正确；parity gap 见下，详见 HANDOFF 1b）
  - [ ] scan 侧 HIGH 修复（与模型无关、多在 SPI surface 内）：①`HudiScanPlanProvider` override `populateScanLevelParams` 设 current_schema_id+history_schema_info + `HudiScanRange` 设 `THudiFileDesc.schema_id`；②column_types 改发完整 Hive 类型串（弃 `getTypeName()`）+ 停止逗号 join/split（typed list 端到端）；③time-travel 透传 snapshot 否则 fail-loud；④增量读 fail-loud
  - [x] 写 catalog 模型决策备忘（a/b），用户签字 —— **完成**：定 **hybrid**（[D-019](./decisions-log.md)），建 [tasks/P3](./tasks/P3-hudi-migration.md)（批 A 现做 b、批 E 推迟 a）
  - [ ] 选定后：补 `tableFormatType` 分流消费、增量 SPI hook、`listPartitions` override + 真实 applyFilter 裁剪、三模块测试

### DV-004 — T13 用户向安装文档不在本代码仓（在 doris-website 仓）

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟢 已修正
- **原计划位置**：[tasks/P2 §P2-T13](./tasks/P2-trino-connector-migration.md)：「`docs-next/` 加 trino-connector 插件安装步骤」
- **偏差描述**：原计划假设本代码仓有 `docs-next/`；实际本仓只有 `docs/`，用户向文档（docs-next / i18n）在独立的 doris-website 仓。
- **新方案**：T13 在本 PR 内只同步 plan-doc 跟踪文档；用户向安装文档另在 doris-website 仓提交。
- **影响范围**：文档 — 本仓只更新 plan-doc；website 仓待办。代码/计划 — 无。
- **关联**：P2-T13
- **后续动作**：[ ] 在 doris-website 仓补 trino-connector 插件安装文档

### DV-003 — T12 迁移兼容回归测试：先例与目标目录均不存在，且本地不可运行

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟡 推迟
- **原计划位置**：[tasks/P2 §P2-T12](./tasks/P2-trino-connector-migration.md)：「类似 P0 的 ES/JDBC migration compat；放入 `regression-test/suites/external_catalog/`」
- **偏差描述**：(1) 不存在「P0 ES/JDBC migration_compat」先例套件；(2) 不存在 `external_catalog/` 目录（实际为 `external_table_p0/` 与 `external_table_p2/`）；(3) 该测试需真实 Trino plugin + 外部数据源 + 运行集群，本开发环境无 docker/集群，无法编写后验证。
- **触发场景**：批 E 启动 T12 时 recon 发现。
- **新方案**：推迟到有 Trino plugin + docker/集群的环境再编写并验证；不往本 PR 加无法验证的套件。
- **替代方案**：盲写 groovy 放 `external_table_p0/trino_connector/` 但本地不可验证——否决（违反"测试要可验证"）。
- **影响范围**：测试 — 迁移 image 兼容回归缺位（现有 trino_connector 功能套件仍在）。代码/计划 — 无。
- **关联**：P2-T12、R-001（image 兼容回归风险）
- **后续动作**：[ ] 集群/CI 环境补 `trino_connector_migration_compat`（CREATE CATALOG→image→重启读回 + 旧 image 含 `TRINO_CONNECTOR` 枚举反序列化）

### DV-002 — T11 单测无法 mock Trino plugin；`TrinoJsonSerializer` 非纯单元

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟢 已修正（commit `9bba12a44b2`）
- **原计划位置**：[tasks/P2 §P2-T11](./tasks/P2-trino-connector-migration.md)：「最少 4 个 test class（schema / predicate / type-map / json）；mock Trino plugin」
- **偏差描述**：(1) fe-connector-trino 仅依赖 junit-jupiter，无 Mockito；(2) `TrinoJsonSerializer` 构造需 `HandleResolver` + Trino `TypeRegistry`（来自已加载 plugin 的 `TrinoBootstrap`），非纯单元；(3) schema / applyFilter / preCreateValidation 需活的 connector。无 plugin 无法在单测覆盖。
- **触发场景**：T11 启动、读 3 个 SUT 源码时发现。
- **新方案**：写 3 个纯转换器 JUnit5 测试（`TrinoPredicateConverterTest` 14 / `TrinoTypeMappingTest` 11 / `TrinoConnectorProviderTest`=validateProperties 4 = 29 测试），本地 `mvn test` 全绿、不需 plugin；砍掉 json/schema，用 `validateProperties`（批 A T01）替补第 3 类。plugin 依赖路径由现有 `external_table_p0/p2` trino_connector regression 套件覆盖。
- **替代方案**：引 Mockito mock Trino connector 测 pushdown/metadata——否决（偏离 module 现有约定、脆弱、费时）。
- **影响范围**：测试 — 单测覆盖纯转换逻辑；集成路径靠 regression。代码/计划 — 无。
- **关联**：P2-T11、P2-T02
- **后续动作**：（无；plugin 路径覆盖见 T12 follow-up）

### DV-001 — 批 D（删 legacy）范围遗漏 `ExternalCatalog` db 路由与 legacy 测试

- **发现日期**：2026-06-04
- **发现 session / agent**：P2 批 C+D+E session
- **当前状态**：🟢 已修正（commit `ed81a063fe8`）
- **原计划位置**：[tasks/P2 §P2-T08..T10](./tasks/P2-trino-connector-migration.md) / HANDOFF：批 D 只列 T08（translator 分支）+ T09（CatalogFactory case）+ T10（删目录）
- **偏差描述**：recon 发现还有两处引用 legacy 目录、计划未列：(1) `ExternalCatalog.java:948` enum switch `case TRINO_CONNECTOR` 实例化 `TrinoConnectorExternalDatabase`；(2) 测试 `fe-core/.../trinoconnector/TrinoConnectorPredicateTest.java` 测被删的 `TrinoConnectorPredicateConverter`。删目录后两者编译失败。另：原 T10 描述「删 GsonUtils 3 个 class-token 注册」已过时（批 B/T03 已 atomic-replace，T10 不碰 GsonUtils）。
- **触发场景**：批 D 删目录前 `grep datasource.trinoconnector` 全仓 recon。
- **新方案**：(1) `case TRINO_CONNECTOR` 改返 `PluginDrivenExternalDatabase`（照搬已迁移的 JDBC case line 936）+ 删 import；(2) 删该 legacy 测试（新测试见 T11）。**有意保留** `MetastoreProperties.Type.TRINO_CONNECTOR` + `TrinoConnectorPropertiesFactory`（在 `property/metastore/` 子系统，不引用被删目录，SPI 路径可能仍需）。
- **替代方案**：`case TRINO_CONNECTOR` 整删落 default 返 null——否决（JDBC 先例显式返 PluginDrivenExternalDatabase，SPI 需要）。
- **影响范围**：代码 — 已合入批 D commit `ed81a063fe8`。文档 — 本条 + tasks/P2 T10 备注已更正。计划 — 无。
- **关联**：P2-T08、P2-T09、P2-T10
- **后续动作**：[ ] 评估 `MetastoreProperties` trino 条目是否真被 SPI 路径使用（若纯死代码可后续清）

---

## 附录：偏差模板

发现偏差时复制以下模板到 §详细记录 顶部，并更新 §📋 索引表。

```markdown
### DV-NNN — <一句话主题>

- **发现日期**：YYYY-MM-DD
- **发现 session / agent**：（哪次 session 发现的）
- **当前状态**：🟢 已修正 / 🟡 待修正 / 🔴 阻塞中
- **原计划位置**：[文档名 §章节](./xxx.md)，引用原句或代码片段
- **偏差描述**：原计划说 X，实施中发现 Y
- **触发场景**：什么操作 / 什么连接器 / 什么 corner case 引发的
- **新方案**：现在的处理方式
- **替代方案**：考虑过的其他修正
- **影响范围**：
  - 文档：哪些文件需要同步修改（已修改的标 ✅）
  - 代码：哪些已合 PR / 待提 PR
  - 计划：是否影响阶段时长 / 顺序
- **关联**：[task ID]、[PR #]、[decision D-NNN（如果偏差催生了新决策）]
- **后续动作**：
  - [ ] 同步修改文档 X
  - [ ] 提 PR 调整代码 Y
  - [ ] 通知相关 task owner
```

---

## 何时应该写偏差日志（典型场景）

1. RFC 中某 SPI 方法签名在实际实现时发现参数不够 / 太多
2. 原计划某阶段时长估算严重偏差（如 2 周变 4 周）
3. 实施中发现某连接器有未预料的特殊性（如 Iceberg 某 catalog flavor 不支持某操作）
4. 原计划的某 task 拆分粒度太粗 / 太细，重新拆分
5. 原计划假设某个三方库行为 X，实际是 Y
6. 决策（D-NNN）在落地时发现执行不了，需要重新评估
7. 跨连接器假设的一致性被打破（如某 SPI 默认行为对 connector A 合理但对 B 不合理）

## 何时**不**应该写偏差日志

- 普通 bug 修复（写 commit message）
- task 的子步骤微调（在 task 文件里加备注）
- 文档错别字 / 链接错误（直接改）
- 命名重构 / 重命名（直接改）
- 已知的实施细节决策（如选用 `HashMap` vs `LinkedHashMap`）
