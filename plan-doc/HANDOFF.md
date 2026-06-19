# 🤝 Session Handoff

> 滚动文档：每次 session 结束**直接覆盖**（不保留历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围**：本文件 = catalog-spi **主线** handoff。metastore/storage 抽取是**独立子线**，单独跟踪在
> [`metastore-storage-refactor/`](./metastore-storage-refactor/)（其 HANDOFF/PROGRESS/tasks/decisions 自洽，本文件不复述细节）。

---

# 🎯 下一个 session 的任务 — **P6-DEVIATIONS 余项 accept-as-deviation 签字 → 见 `task-list-P6-fixes.md` backlog 0 末项**（5 个 deviation→fix **全部完成**：A3 ✅ `5fa47c27eb8` / A2 ✅ `1935748d6c3` / B-MC2 ✅ `10284edbf88` / A1 ✅ `9d687145a28` / B-R2-be ✅ `60ed665c4dc`。**本批清零。** 下一步=把未转 fix 的剩余 MINOR/NIT 刻意偏离 + wave2 新增 + `PluginDrivenExternalCatalog:140` 吞 authenticator-wiring 异常逐条记入新建 `deviations-log.md`（含用户签字）；之后 B8 legacy 删除/元存储子线 P2-T04/05）

> **进度（2026-06-19）**：P6 发现项按 `task-list-P6-fixes.md` 的 prioritized list 逐个修（单任务循环：
> design → 红队 → 实现 → impl 验证 → build+UT → commit）。
> **✅ C1 (MinIO, MAJOR) `9967846ef64`** / **✅ C2 (HDFS XML, MAJOR) `e95128aed5b`** / **✅ R3-residual (MINOR) `44499f073e8`**（详见 git log + 各自 design/summary）。
> **✅ A3 (NIT, profile-parity, deviation 1/5) `5fa47c27eb8`**：`PaimonScanRange` ctor 发 `paimon.self_split_weight` 的闸由
> 值判断 `selfSplitWeight > 0` 改为 JNI 标记 `paimonSplit != null`——weight=0 的 JNI split（rowCount-0 系统表 split /
> fileSize-0 DataSplit）现也发 0，BE 不再读 -1 哨兵（profile counter `_max_time_split_weight_counter`）；= legacy
> `PaimonScanNode.setPaimonParams:274`（JNI 臂无条件、native 臂从不发）parity。新 `PaimonScanRangeSelfSplitWeightTest`（3，
> **RED→GREEN 两次独立跑验证**：未修代码 weight-0 测失败 1，修后 3/0）；283/0/0/1skip + checkstyle0 + import0；design 红队
> `wf_3f2cd605-2a8`（9 候选→0-actionable on code）、impl-verify **APPROVE**；e2e gated 未跑。详见
> `designs/FIX-A3-SELF-SPLIT-WEIGHT-{design,summary}.md`。
> **✅ B-R2-be (NIT, intentional, deviation 5/5 = LAST, ⚠️NO PERF REGRESSION) `60ed665c4dc`**：schema-evolution
> 字典的 per-schema-id 读做 memo。**收窄方案被否（架构上连接器内做不到 + BE 崩风险）**：props 常先于 split 构建、
> `getScanPlanProvider()` 每次 new provider 故 planScan 的 schema_id 到不了字典构建、引用集是 per-scan、通用桥不能收
> `paimon.schema_id`、props 里重 plan=新 I/O、收窄漏发即 BE 硬崩（CI 969249）。**用户选 Option A=memo 读、保贪婪全集发射**：
> 字典发射**字节不变**（全 `listAllIds()`→永覆盖→零 BE 崩险），只把 per-schema-id 字段**读**走连接器级不可变 memo——
> **复用 B-MC2 `PaimonSchemaAtMemo`**（已缓存同一 write-once 事实 `(handle,schemaId)→fields`）。新 package-private 4-arg
> provider ctor（2/3-arg 委托 fresh memo→~25 处不变）；`buildSchemaEvolutionParam` 收 handle + memo 包 loop（loader 走
> **直读** `schemaManager.schema(id)` 非 `catalogOps.schemaAt`，real-table+fake-catalogOps 测不破）；`getScanPlanProvider()`
> 注入共享 memo。一致性（同 key 同值）经 write-once 不变式 + B-MC2 从不写 $ro/sys key 验证。+5 UT（memo-populated/
> sentinel-HIT/byte-identical/force-jni/wiring）各 RED→GREEN；303/0/1skip；checkstyle0+import0；design 红队
> `wf_222e1abd-655`（4 lens sound、荐复用、6 actionable 折叠）、impl-verify COMMIT_AS_IS（0 defect）；e2e gated 未跑。
> 详见 `designs/FIX-B-R2-BE-SCHEMA-DICT-MEMO-{design,summary}.md`。
> **✅ A1 (MINOR, FE 调度回归, deviation 4/5) `9d687145a28`**：把连接器算好的比例 split weight 接到 FE `FileSplit`
> 调度字段（`FederationBackendPolicy` 按大小分配，legacy parity；FE BE-分配 only，不改行/路由/BE读/结果）。SPI
> `ConnectorScanRange` 加默认 `getSelfSplitWeight()`/`getTargetSplitSize()`（哨兵 -1）；`PluginDrivenSplit` ctor
> 仅当 `weight>=0 && target>0` 时填字段（通用，别的连接器继承 -1→保持 `standard()` 无回退）；`PaimonScanRange` 携
> `targetSplitSize`（Builder 默认 -1）+ `@Override` 两 getter；`PaimonScanPlanProvider` 算 `resolveSplitWeightDenominator`
> （= legacy `fileSplitSize>0 ? : max_file_split_size` 64MB）一次并把 `weightDenominator` 穿到每个 builder。**task-list 漏的
> 关键缺口（靠追 legacy 抓出）**：native 范围从不设 `selfSplitWeight`（默认 0）——legacy 用 `length(+DV)`（`PaimonSplit:72,112`），
> native 是默认路径,weight=0 会 clamp 0.01（均匀）使修复失效；故 `buildNativeRange` 现设 `length+DV`+denominator。FE-only
> （BE-thrift `paimon.self_split_weight` 仍 A3-gated on `paimonSplit`）。新 `PluginDrivenSplitWeightTest`(fe-core,5)+
> `ConnectorScanRangeWeightDefaultsTest`(api)+5 `PaimonScanPlanProviderTest`+6 改签名调用点；各由独立 mutation
> **RED→GREEN 验证**（ctor-gate/native-weight/sentinel/denominator/swap）；api 44/0+paimon 298/0/1skip+fe-core 5/0;
> checkstyle0+import0；design 红队 `wf_c8345c28-ee6`（4 lens sound,6 actionable 已折叠）、impl-verify `wf_3381cfaa-205`
> （2 lens 全 COMMIT_AS_IS,0 actionable）；e2e gated 未跑。详见 `designs/FIX-A1-SPLIT-WEIGHT-{design,summary}.md`。
> **✅ B-MC2 (NIT, CACHE-P1, deviation 3/5, ⚠️NO PERF REGRESSION) `10284edbf88`**：恢复 time-travel
> schema-at-snapshot 的跨查询二级缓存（SPI cutover CACHE-P1 丢弃）。新连接器侧 `PaimonSchemaAtMemo`
> （`ConcurrentHashMap<MemoKey, PaimonSchemaSnapshot>`，loader 在锁外 + best-effort clear-on-overflow 界），
> 由**长寿命 per-catalog `PaimonConnector`** 持有（REFRESH→onClose connector=null→重建→空 memo）并经**新
> package-private 4-arg ctor** 注入每查询的 metadata（public 3-arg 委托一个 fresh per-instance memo→~15 处构造点不变）。
> `getTableSchema(snapshot)` schemaId>=0 臂：`resolveTable` 在 loader 外**只调一次**（branch handle 的 getTable 仍 1/查询），
> memo 只包 `schemaAt` 读，`ConnectorTableSchema` 每查询**重建**。**design 红队 MAJOR 已采纳**：缓存原始
> `PaimonSchemaSnapshot`（key 的纯函数），**非**built `ConnectorTableSchema`（它嵌 live `coreOptions`→陈旧属性风险）。
> `MemoKey`=抽取的 handle 身份(db,table,sysName,branch)+schemaId（不留 handle 引用→不钉住已加载的 paimon Table，
> 偏离红队「delegate to handle.equals」是为避免钉住 Table）。+3 `PaimonConnectorMetadataMvccTest` + 新
> `PaimonSchemaAtMemoTest`(3)，各由独立 mutation 跑 **RED→GREEN 验证**（RED-1 memo禁/RED-2 key丢字段/RED-3 界禁）；
> 293/0/0/1skip + checkstyle0 + import0；design 红队 `wf_903bf4e9-3a4`、impl-verify `wf_67804f35-d5e`
> （2×COMMIT_AS_IS+1×FIX_THEN_COMMIT=仅 verifier 自留 scratch，产品码干净）；e2e gated 未跑。详见
> `designs/FIX-B-MC2-SCHEMA-AT-MEMO-{design,summary}.md`。
> **✅ A2 (MINOR, missing-port, deviation 2/5) `1935748d6c3`**：`appendExplainInfo` 反序列化已在 props 里的
> `paimon.predicate`（即推给 SDK 的 `List<Predicate>`）重发 legacy `predicatesFromPaimon:` 块，置于
> `paimonNativeReadSplits=` 与 VERBOSE `PaimonSplitStats` 之间（legacy 序 `PaimonScanNode:657-671`）。不重跑 converter
> （filter 不在 seam、provider 每次新建）；absent≠empty 跳过（保 exact-equality 旧测）；decode 失败 LOG.warn+跳过；
> 不改 SPI、无 BE 影响（`populateScanLevelParams` 逐键读，新键也到不了 BE）。4 新 `PaimonScanExplainTest`（**RED→GREEN
> 分跑**：未修 3 失→修后 0）；287/0/0/1skip + checkstyle0 + import0；design 红队 `wf_c67cb558-ff4`（13 候选→0-actionable
> on code，已折叠文档/测试细化）、impl-verify APPROVE。详见 `designs/FIX-A2-PREDICATES-FROM-PAIMON-{design,summary}.md`。
> **✅ R3-residual (MINOR) 已完成**：去 `PluginDrivenScanNode.getNodeExplainString` 的 `"paimon".equals(getType())`
> gate，VERBOSE backends 块改无条件 emit（gate 变 `VERBOSE && !isBatchMode()`，与父 `FileScanNode` 完全一致）+ 重写假注释。
> **红队纠正了 scope**（比 review 的「maxcompute」更广）：`SPI_READY_TYPES={jdbc,es,trino-connector,max_compute,paimon}` 全走
> 此 node → paimon 不变、maxcompute/trino **恢复** pre-cutover 块、es/jdbc 获得**新增**（NPE-safe、合规则）VERBOSE 输出
> （`PluginDrivenSplit extends FileSplit` 恒有 FileScanRange + `getDeleteFiles` null-guard；es/jdbc 渲染合成路径
> `es://idx/shard`、`jdbc://virtual`）。新 `PluginDrivenScanNodeVerboseExplainTest`（3 测，**RED→GREEN 突变验证**：
> 重加 gate → 非-paimon 测变红）；45/0/0 `PluginDrivenScanNode*` UT + checkstyle 干净；**e2e gated/未跑**。
> es_http `ES terminate_after:` gate 作**独立残留**留下（R3-LAYER-2，键 file-format-type 非 getType()，规则字面不违反）。
> 设计/红队结论详见 `designs/FIX-R3-RESIDUAL-{design,summary}.md`（design 红队 3 lens finder→verifier `wf_3518653b-3cb`）。
> **✅ R1-table (MINOR) 已完成**：`PluginDrivenExternalCatalog.createTable`（**通用桥**，全 SPI 连接器）去 `if (localExists)`
> 守卫 → 存在分支无条件报 `ERR_TABLE_EXISTS_ERROR`（MySQL **1050**/42S01），在 `metadata.createTable` 前短路。修「表只远端存在、
> 本 FE 缓存缺」（陈旧缓存/他 FE/外部建）+ 无 IF NOT EXISTS 时丢 1050 退化成泛化 DdlException(errno 0)。精确 legacy parity
> （paimon `:195/:212` + maxcompute `:184/:195`，remote+local 两臂皆 1050）。es/jdbc/trino 对已存在表 CREATE 现报「already exists」(NIT)。
> 改写 remote 测 + 强化 local 测加 errno 断言（**RED→GREEN 突变验证**：重加守卫→remote 测红）；26/0/0 DdlRouting + 12/0/0 Engine +
> checkstyle 干净；**e2e gated**。design 红队 `wf_19fd7785-165`（0 actionable）。详见 `designs/FIX-R1-TABLE-{design,summary}.md`。
> **✅ C4 / R2-catalog / R3-catalog（3 MINOR，合一）已完成 `82b6de0de98`**：**C4** 透传
> `Config.hive_metastore_client_timeout_second`（env key `hive_metastore_client_timeout_second` → `HmsMetaStoreProperties
> .toHiveConfOverrides(String)`，去硬编码 `"10"`；fe.conf 未设时 byte-parity，恢复 `HMSBaseProperties:204-208`）。**R2-catalog**
> 改 **warn-only**（非 strip，用户拍板）在 `PaimonConnectorProvider.validateProperties` 提示死键 `meta.cache.paimon.table.*`——
> 经 `getMetaCacheEngine()=="default"`（PluginDriven 不 override）证实 plugin 路从不碰 `PaimonExternalMetaCache`，键确死；
> warn 落连接器（非 connector-agnostic 桥，report 引的位置=错层）。**R3-catalog** 改 **rethrow**（用户拍板，非仅加 catalog 名）——
> `listDatabaseNames` 抛 `RuntimeException("Failed to list databases names, catalog name: <name>")` 与 legacy
> `PaimonMetadataOps:340` 完全一致（且所有其它连接器都 propagate），原先吞成 emptyList 且注释谎称 parity。280/0 paimon(+1
> gated skip)+16/0+3/0+14/0+12/0；fe-core 编译过；checkstyle 0；import-check 干净；design+impl 两道红队均 0-actionable；e2e gated。
> 详见 `designs/FIX-C4-R2-R3-CATALOG-{design,summary}.md`（design 红队 `wf_444e33b9-5c6`、impl 红队 `wf_b3d35e64-6b9`）。
> **5 个 deviation→fix 全部完成**（A3 `5fa47c27eb8` / A2 `1935748d6c3` / B-MC2 `10284edbf88` / A1 `9d687145a28`
> / B-R2-be `60ed665c4dc`）。**下一个 = P6-DEVIATIONS 余项 accept-as-deviation 签字**：未转 fix 的剩余 MINOR/NIT
> 刻意偏离 + wave2 新增 + `PluginDrivenExternalCatalog:140` 吞 authenticator-wiring 异常 → 逐条记入新建
> `deviations-log.md`（含用户签字）。**注**：B-R2-be 的「收窄」方案经分析架构不可行（见上方 ✅ B-R2-be 条 + 设计文档），
> 已与用户确认改用 memo（Option A），亦应在 deviations-log 记一条「R2-be 收窄不可行→memo」的决策。之后才是 P6-fixes 批清零。

paimon connector 全功能路径 clean-room 对抗 review（6 维度 + 7 缺口线，2 波，零历史先验）**已完成**。
报告：[`reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md`](./reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md)（未跟踪，待 vet+commit）。
统计：**2 BLOCKER · 2 MAJOR · 16 MINOR · 10 NIT**（27 confirmed / 3 partial / 3 refuted）。方法：wave1 = 9 finder 线归 6 维度
（read×2/write/ddl×2+config/replay/cache/residual），wave2 = 补 7 缺口线（show-partitions / partitions-TVF / 统计-ANALYZE /
@branch / MTMV / auth-UGI / config→BE），每线 finder→对抗 verifier；fresh subagent 仅喂代码+维度问题（成功挡住历史先验）。

**核心结论（详见报告）**：
- **2 BLOCKER 都是 B8 删除护栏、非运行时 bug**：R1 = legacy `property/metastore/Paimon*MetaStoreProperties` + `PaimonExternalCatalog`
  **常量**仍 LIVE（cutover 的 `initPreExecutionAuthenticator`→Kerberos 装配经它）；R2 = `property/storage/{S3,OSS,COS,OBS,Minio}Properties`
  是**跨连接器共享**（~26 消费者 iceberg/hive/glue/dlf/storage-vault/load/cloud/policy）。→ **B8 不能整包删，必须分阶段**。
- **2 MAJOR 是真活读路回归**（不挡 B8，应随 cutover 修）：**C1** = `minio.*`-keyed catalog 整条不可用（FE 建表 + BE 读，两波独立证实；
  fe-filesystem 无 MinIO provider，S3 provider 不认 `minio.*`；2026-06-14 的 applyCanonicalMinioConfig 未进本分支）；**C2** = HDFS
  `hadoop.config.resources` XML 未注入 FE 建表 Configuration（filesystem/jdbc flavor）→ XML-only HA 拓扑解析不到 nameservice。
  **C2 的 kerberos-by-alias 子项被 wave2 证伪**（per-FS Configuration 的 auth marker 非负载性：JVM-global `UGI.setConfiguration` 主导 SASL）→ 只修 XML。
- **其余全 parity**：replay/GSON 干净（0 缺陷）、scan→BE 契约（历史 double-fill / `file_format=jni` / schema-evo `-1` bug 均已修）、
  write（无写路、两侧都 loud-reject）、cache pin 模型、SHOW PARTITIONS（critic 的 `VARCHAR(60→300)` 担忧被证伪：master 早已 300）、
  partitions-TVF、统计/ANALYZE（row-count 一致、column-stat 两侧空、ANALYZE 走 generic）、@branch、MTMV 新鲜度、auth/UGI（split-plan
  等不裹 `executeAuthenticated` 与 legacy 完全一致 → 非回归，了结 HANDOFF 旧 open item）。MINOR/NIT 多为 EXPLAIN/profile/错误码
  parity 或刻意更安全的偏离。

**下一步**：本轮是 review、**未改任何代码**（除报告本身 + 我修正了 writer 的计数）。发现项各自另起 fix task（见下方 backlog 0 + 报告
§Coverage gaps & follow-ups 的 prioritized fix-task list）。**AGENT-PLAYBOOK 单任务循环：先 review 方案后实现**。

---

# 🔭 主线 backlog（P6 review 已出报告，按此排）

0. **修复 P6 发现项**（报告 §Coverage gaps & follow-ups → prioritized fix-task list；每个独立 fix task；
   逐项进度见 `task-list-P6-fixes.md`）：
   - ✅ **C1 MinIO**（MAJOR）— **DONE `9967846ef64`**（minio.* 别名进共享 fe-filesystem-s3 + 保留 tuning 默认；28/0/0 UT）。
   - ✅ **C2 HDFS XML**（MAJOR）— **DONE `e95128aed5b`**（`HdfsFileSystemProperties implements HadoopStorageProperties`；
     FE `toHadoopConfigurationMap()` 返 **defaults-free** 图避免多后端 `fs.s3a.*` clobber，BE `toMap()` 仍 defaults-laden；
     DLF=DV-036、disable-cache=DV-037；28/0+279/0/1skip+glue test）。
   - ✅ **R3 residual**（MINOR）— **DONE**：去 `PluginDrivenScanNode.getNodeExplainString` 的 `"paimon".equals(getType())`
     gate，VERBOSE backends 块无条件 emit（与父 `FileScanNode` gate 一致）+ 重写假注释。红队纠正 scope=全 5 个 SPI 连接器
     （paimon 不变 / maxcompute+trino 恢复 / es+jdbc 新增 NPE-safe 输出）；新 UT 3 测 RED→GREEN；45/0/0 + checkstyle 干净。
     es_http gate 留作 R3-LAYER-2 独立残留。详见 `designs/FIX-R3-RESIDUAL-{design,summary}.md`。
   - ✅ **R1 table**（MINOR）— **DONE `44499f073e8` 之后**：`PluginDrivenExternalCatalog.createTable`（通用桥）去 `if (localExists)`
     守卫 → 存在分支无条件报 `ERR_TABLE_EXISTS_ERROR`(1050)，在 `metadata.createTable` 前短路；精确 legacy parity（paimon+maxcompute
     remote+local 两臂皆 1050）；改写 remote 测 + 强化 local 测 errno 断言（RED→GREEN）；26/0/0+12/0/0+checkstyle 干净；红队 0 actionable。
     详见 `designs/FIX-R1-TABLE-{design,summary}.md`。
   - ✅ **C4 / R2-catalog / R3-catalog**（3 MINOR，合一）— **DONE `82b6de0de98`**：C4 透传
     `Config.hive_metastore_client_timeout_second`（去硬编码 `"10"`，fe.conf 未设 byte-parity）；R2-catalog **warn-only**
     （非 strip，用户拍板）提示死键 `meta.cache.paimon.table.*`（`getMetaCacheEngine()=="default"` 证实 plugin 路不碰
     `PaimonExternalMetaCache`，键确死）；R3-catalog **rethrow**（用户拍板）`RuntimeException` 带 catalog 名，与 legacy
     `PaimonMetadataOps:340` 一致（原吞成 emptyList）。280/0+16/0+3/0+14/0+12/0；checkstyle 0；两道红队 0-actionable。
     详见 `designs/FIX-C4-R2-R3-CATALOG-{design,summary}.md`。
   - ✅ **A3**（NIT profile-parity）— **DONE `5fa47c27eb8`**：`PaimonScanRange` ctor `self_split_weight` 闸 `>0`→
     `paimonSplit != null`（emit-iff-JNI = legacy `PaimonScanNode:274` parity）；weight-0 JNI 现发 0；新 UT 3 RED→GREEN；
     283/0/0/1skip。详见 `designs/FIX-A3-SELF-SPLIT-WEIGHT-{design,summary}.md`。
   - ✅ **A2**（MINOR missing-port）— **DONE `1935748d6c3`**：`appendExplainInfo` 反序列化 `paimon.predicate` 重发 legacy
     `predicatesFromPaimon:`（置于 `paimonNativeReadSplits=` 与 VERBOSE `PaimonSplitStats` 间，legacy 序）；不重跑 converter；
     4 新 UT RED→GREEN；287/0/0/1skip。详见 `designs/FIX-A2-PREDICATES-FROM-PAIMON-{design,summary}.md`。
   - ✅ **B-MC2**（NIT CACHE-P1，⚠️NO PERF REGRESSION）— **DONE `10284edbf88`**：连接器侧 `PaimonSchemaAtMemo`
     （`ConcurrentHashMap`，loader 锁外 + clear-on-overflow 界）由 per-catalog `PaimonConnector` 持有并注入每查询 metadata；
     `getTableSchema(snapshot)` schemaId>=0 臂 memo 只包 `schemaAt` 读、`ConnectorTableSchema` 每查询重建（红队 MAJOR：缓存
     原始 `PaimonSchemaSnapshot` 非 built schema 以保 live coreOptions）；+3 Mvcc UT + 新 `PaimonSchemaAtMemoTest`(3) 各 RED→GREEN；
     293/0/0/1skip。详见 `designs/FIX-B-MC2-SCHEMA-AT-MEMO-{design,summary}.md`（design 红队 `wf_903bf4e9-3a4`、impl-verify `wf_67804f35-d5e`）。
   - ✅ **A1**（MINOR FE 调度回归）— **DONE `9d687145a28`**：SPI `ConnectorScanRange` 加 `getSelfSplitWeight`/
     `getTargetSplitSize`（哨兵 -1）；`PluginDrivenSplit` ctor 仅 `weight>=0 && target>0` 填 FileSplit 权重；
     `PaimonScanRange` 携 `targetSplitSize`（默认 -1）；`PaimonScanPlanProvider` 算 denominator（legacy 64MB 公式）
     穿到每 builder + **native 范围补 `selfSplitWeight=length+DV`**（task-list 漏的关键缺口，native 是默认路径否则均匀）。
     fe-core 5 + api 1 + 连接器 5 新 UT 各 RED→GREEN；298/0/1skip+5/0+44/0；两道红队全过。详见 `designs/FIX-A1-SPLIT-WEIGHT-*.md`。
   - ✅ **B-R2-be**（NIT intentional，⚠️NO PERF REGRESSION）— **DONE `60ed665c4dc`**：收窄方案架构不可行→
     用户选 Option A=memo per-schema-id 读、保贪婪全集发射（字节不变→零 BE 崩险）；复用 B-MC2 `PaimonSchemaAtMemo`；
     +5 UT RED→GREEN；303/0/1skip；红队+impl-verify 全过。详见 `designs/FIX-B-R2-BE-SCHEMA-DICT-MEMO-*.md`。
   - **5 个 deviation→fix 全部完成（本批清零）；下一个 = 下面这条 P6-DEVIATIONS 余项签字。**
   - **P6-DEVIATIONS 余项（5 项之后，本批最后一项）**：未转 fix 的剩余 MINOR/NIT 刻意偏离 + wave2 新增 +
     `PluginDrivenExternalCatalog:140` 吞 authenticator-wiring 异常（R3/R4/R5/R6 residual 属 B8 清理、2 BLOCKER 属 B8 护栏，
     均不在此）。逐条记入新建 `deviations-log.md` accept-as-deviation（含用户签字）。
1. **B8 legacy 删除（review 已解锁；须分阶段，按报告 §B8 deletion readiness 的 DEAD vs STILL-CONSUMED ledger）**：
   - **可删（DEAD，成单元同删）**：`datasource/paimon/*`（PaimonExternalCatalog/Factory、ExternalDatabase/Table、HMS/DLF/File/Rest 子类、
     SysExternalTable、MetaCache 等）、`systable/PaimonSysTable`、`metacache/paimon/*` + `ExternalMetaCacheMgr.paimon()/ENGINE_PAIMON`、
     `ShowPartitionsCommand`/`Env`/`ExternalCatalog.buildDbForInit`/`UserAuthentication`/`ExternalMetaCacheRouteResolver` 的死 legacy 分支+import。
   - **删除前置（硬）**：① 先把 `PaimonExternalCatalog` 的常量（`PAIMON_FILESYSTEM`/`PAIMON_HMS`）迁出到 metastore-props 模块（5 个 live 类 import 它）；
     ② scrub 悬空 javadoc `{@link PaimonSysTable}`（`PluginDrivenSysTable:27`、`NativeSysTable:36`）否则 strict checkstyle/javadoc 挂；
     ③ 保 load-bearing dispatch ordering（`ShowPartitionsCommand` PluginDriven 分支先于 legacy）。
   - **不可删（STILL-CONSUMED）**：`property/metastore/Paimon*MetaStoreProperties`+`PaimonPropertiesFactory`+`AbstractPaimonProperties`（cutover
     Kerberos 装配 LIVE，R1）、`property/storage/{S3,OSS,COS,OBS,Minio}Properties`（跨连接器共享，R2）。**B8 scope 不含这两树。**
   - 逐子树删 + 每批跑 fe-core 编译 + 连接器测 + regression-gated。与元存储子线 D-016 一致（那两包不碰）。
2. **元存储子线收尾**（[`metastore-storage-refactor/`](./metastore-storage-refactor/)）：P2-T04（paimon pom + gate，
   ⚠️ `MetaStoreProviders` ServiceLoader 改 2-arg 显式 loader 防子优先 loader 下发现不到 provider）→ P2-T05（docker
   5-flavor 真闸 + vended(REST/DLF) + Kerberos HMS + storage 等价，合并原 P1-T06；`enablePaimonTest=true`）。
3. **D-057 re-scope**（第三轮报告 §D.3）：deferred `TablePartitionValues:162` prune-path sentinel residue **不影响
   paimon**（MVCC override 绕过）→ re-scope 到非-MVCC 插件连接器（maxcompute/es/jdbc）。
4. **accepted-deviation 用户签字**（task-list「NOT in this fix scope」）：~10 MINOR + ~12 NIT + C-1 observability +
   uncheckedFallbacks（REFRESH cache invalidation / partitions-TVF auth / split-plan RPC 在 `executeAuthenticated` 外 /
   `PluginDrivenExternalCatalog:140` 吞 authenticator-wiring 异常）。逐条 accept-as-deviation 或转 fix。

---

# 📦 仓库 / 进度状态
- **HEAD = `60ed665c4dc`**（FIX-B-R2-be schema-dict memo；前序 `9d687145a28` A1、`10284edbf88` B-MC2、`1935748d6c3` A2、`5fa47c27eb8` A3、`82b6de0de98` C4/R2/R3、`f652b40d210` R1-table、`44499f073e8` R3-residual、`e95128aed5b` C2 HDFS XML、`9967846ef64` C1 MinIO）。当前分支 **`catalog-spi-07-paimon`**（非 master）；
  remote `master-catalog-spi-07-paimon`（= PR [#64445](https://github.com/apache/doris/pull/64445) head）仍在 `82b6de0de98`，
  **本地领先：A3 起的 deviation fix commits 尚未 push** → 待本批 deviation fix 做完，session 收尾一次性
  force-with-lease push + PR 评论 `run buildall`（见 §Commit 须知 / memory `catalog-spi-07-paimon-branch-pr-workflow`）。
- **主线（P0–P5）**：paimon connector SPI cutover + round-3 clean-room review 的 4 个 user-approved fix 全完成
  （FIX-1 `c376aba1264` rest-vended-uri / FIX-2 `2e845e88bf9` jni-file-format / FIX-3 `f08bc22b9bd` incr-scan-reset /
  FIX-4 `f0210b51871` feconf-storage-parity）。详见 `task-list-P5-rereview3-fixes.md` + `reviews/P5-paimon-rereview3-2026-06-12.md`。
- **元存储/storage 子线**（独立目录，本 session 推进）：storage 收口到 `fe-filesystem-api` typed（P1）+ 新建
  `fe-connector-metastore-{api,spi}` + `fe-kerberos`（P2-T01..T03，paimon 已 cutover 到共享 metastore SPI）+
  **fe-property 模块已物理删除**（P1-T07，0 消费者孤儿）。剩 P2-T04/T05（见 backlog）。**注**：fe-core
  `datasource.property.{storage,metastore}` 两包仍在（子线 D-016 不碰；B8 才考虑删其 paimon-only 部分）。
- ⚠️ `regression-test/conf/regression-conf.groovy` 仍 modified 未 commit 且含**明文 Aliyun key** → commit 前继续
  path-whitelist，**严禁 `git add -A`**；`regression-conf.groovy.bak` 同理排除。
- 未 commit/未跟踪：scratch（`.audit-scratch/` `conf.cmy/` `META-INF/`）；`reviews/P5-paimon-rereview3-2026-06-12.md`
  （第三轮 review 报告）；**`reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md`（本轮全路径 clean-room review 报告，502 行，本 session 产物）**。
  HANDOFF.md 本身已更新（review 完成态）。三者未跟踪——下次方便时 vet + path-whitelist commit 或保留本地。

## 🗺️ 代码脚手架
- **Plugin connector**：`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/`
  （`PaimonConnector` / `PaimonConnectorProvider` / 存储+HiveConf 装配 `PaimonCatalogFactory`[现 cutover 到
  `MetaStoreProviders.bind` + 薄 `assembleHiveConf`] / scan `PaimonScanPlanProvider` / @incr `PaimonIncrementalScanParams`）。
- **共享 SPI / 叶子**：`fe/fe-connector/fe-connector-{api,spi}/` + `fe-connector-metastore-{api,spi}/`（metastore 解析器 +
  `MetaStoreProvider` SPI/ServiceLoader）+ 顶层叶子 `fe/fe-kerberos/`（kerberos facts）+ `fe/fe-filesystem/`（typed
  storage，含 `-hdfs` BE model）。
- **fe-core 桥**：`fe/fe-core/.../connector/DefaultConnectorContext.java`、`.../datasource/PluginDriven*.java`、
  `.../fs/FileSystem{Factory,PluginManager}.java`；nereids scan-node 分发。
- **Legacy 对照基准（＝ review 对照 + B8 删除目标）**：fe-core `.../datasource/paimon/`、
  `.../datasource/property/storage/` 下 `{OSS,COS,OBS,S3,Minio}Properties`、`.../property/metastore/HMSBaseProperties`。
- **BE 消费端**：`be/src/format/table/`（`paimon_cpp_reader.cpp`、`paimon_reader.cpp`、`partition_column_filler.h`）。

## ⚠️ Commit 须知（任何 `git add` 前必读）
- **硬前置**：scrub `regression-test/conf/regression-conf.groovy`（明文 key）+ 清 scratch（`.audit-scratch/` `conf.cmy/`
  `META-INF/` `*.bak`）。**path-whitelist `git add`，严禁 `git add -A`。**
- 每个 fix 独立 commit；message = `fix: <ID>` / `[Pn-Tnn] <subj>` + 根因 + 解法 + 测试，末尾带
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`。fix commit 带其 design doc（repo 惯例）。
- **收尾推送惯例**（见 memory `catalog-spi-07-paimon-branch-pr-workflow`）：push `catalog-spi-07-paimon`(ff) +
  **force-with-lease** `master-catalog-spi-07-paimon`（PR #64445 head）+ 在 PR #64445 评论 `run buildall`。⚠️ 两分支
  历史曾发散；force 前先 fetch 对比、用 `--force-with-lease`。⚠️ remote URL 明文嵌 GitHub PAT（`git remote -v` 会打印）。

## ⚙️ 操作须知（复用）
- maven 绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false
  -DfailIfNoTests=false`；验证读 surefire XML + `BUILD SUCCESS`（memory `doris-build-verify-gotchas`）。**漏 `-am` →
  `could not resolve … ${revision}` 假错**。paimon 模块需 `-am package -Dassembly.skipAssembly=true`（shade jar 携带
  HiveConf）。**checkstyle 在 `validate` phase（编译前）跑**。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅允许 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- cwd 跨 Bash 调用持久，`cd` 破相对路径 → 一律绝对路径。
- 测试 harness：`PaimonCatalogFactoryTest`（纯 Map→Configuration/HiveConf）/`PaimonScanPlanProviderTest`(real-table
  `FileSystemCatalog`)/`PaimonIncrementalScanParamsTest`/`RecordingConnectorContext`/`RecordingPaimonCatalogOps`/
  `FakePaimonTable`（`.copy` 是 no-op recorder，reset/merge fail-before 须 real table）/ metastore-spi 的
  `*MetaStorePropertiesTest` / `DefaultConnectorContextNormalizeUriTest`(fe-core)。live-e2e CI-gated
  （`enablePaimonTest` 默认 false）→ 注明 gated，勿谎称跑过。

## 🧠 给下一个 agent 的 meta
- **本轮是 review、不是改码**：先出 review 报告，发现项各自另起 fix task；**review 须 clean-room、零历史先验**（见上「关键约束」）。
- **review 必须先于 B8**（legacy ＝ 对照基线）；B8 scope 须经 review dim-6 确认真 dead（别误删仍被 hive/hudi/iceberg 消费的类）。
- **改 handle/分区/scan/storage/auth 流必 grep 全调用方 + 确认实际实例类（base vs MVCC 子类）**；storage/auth 装配注意 raw
  `hadoop.*`/`fs.*` passthrough 跑最后会 clobber 之前 authoritative 设置（FIX-4 4d/4e 亲证）。
- **design red-team（写码前）+ impl verification（写码后）两道**历史证有效（修复阶段照用，但 review 阶段保持 clean-room）。
- **元存储子线**细节不在本文件——读 `metastore-storage-refactor/HANDOFF.md`。
