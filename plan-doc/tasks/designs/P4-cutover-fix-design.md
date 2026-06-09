# P4 — MaxCompute 翻闸缺口修复设计 (review 后续)

> 来源: 翻闸对抗 review 报告 `plan-doc/reviews/P4-cutover-review-findings.md`(41 存活发现)。
> 本设计**只覆盖用户选定的 6 个 blocker/核心-阻断 major**; 其余存活 major/minor 见文末「本批次外(待定)」。
> 状态: **设计待审 —— 未写码**。建议 task id: P4-T06d(cutover gap-fix, 续 T06a/b/c)。生成方式: 每 issue 1 设计 agent + 1 对抗 critic。
> 前置关系: 本批修复落 + live 验证全绿 = 翻闸真正完成门 → 才解锁 Batch D。日期: 2026-06-07。

## 0. 范围与阶段

| 阶段 | issue | severity | 层 | 依赖 | 一句话 |
|---|---|---|---|---|---|
| 阶段 1 | FIX-READ-DESC | blocker | fe-connector-maxcompute | — | MaxComputeConnectorMetadata 缺 buildTableDescriptor override,导致翻闸后 toThrift 走 null 兜底产 SCHEMA_TABLE(无 mcTable),BE file_scanner 无条件 static_cast 到 MaxComputeTableDescriptor 类型混淆崩溃;修法为在 MC connector 补 override 产出 MAX_COMPUTE_TABLE+TMCTable,并把 endpoint/quota/properties 透传进 metadata。 |
| 阶段 1 | FIX-READ-SPLIT | blocker | fe-connector-maxcompute | — | byte_size split 在翻闸 connector 用 .length(splitByteSize) 回填 rangeDesc.size,丢失 legacy 的 -1 sentinel,使 BE 把 byte-size split 误判为 row-offset → 默认路径静默读出错误数据;改 MaxComputeScanPlanProvider.java:268 为 .length(-1) 恢复 sentinel。 |
| 阶段 2 | FIX-DDL-ENGINE | blocker | fe-core | P4-batchD-maxcompute-removal-design.md:100 计划删 CreateTableInfo:~390/:912 的 instanceof MaxComputeExternalCatalog 分支 —— 须改为先落 PluginDriven 分支、Batch D 仅删 legacy MC 分支(顺序依赖,否则坐实回归) | paddingEngineName/checkEngineWithCatalog 在 MC instanceof 分支后新增 PluginDrivenExternalCatalog 分支(keyed on getType()=="max_compute"→ENGINE_MAXCOMPUTE,经 helper 通用化),纯 fe-core 最小改动,镜像 legacy 自动补 engine=maxcompute 行为;须先于 Batch D 删 legacy MC 分支落地。 |
| 阶段 2 | FIX-DDL-REMOTE | major | fe-core | DDL-P1 | 在 PluginDrivenExternalCatalog 的 createTable/dropTable override 内先用 getRemoteName/getRemoteDbName 把本地名解析成 ODPS 远端真名再交给连接器，mirror legacy MaxComputeMetadataOps，纯 FE 改动、不扩 SPI、不动连接器。 |
| 阶段 3 | FIX-PART-GATES | major | fe-core | — | 给 PluginDrivenExternalTable 加 isPartitionedTable/getPartitionColumns override(keyed on connector 的 partition_columns 声明),并在 PartitionsTableValuedFunction.analyze 双网关补 PluginDriven 分支,打通 T06c 已接好的 SHOW PARTITIONS / partitions() TVF BE handler;不删 Batch-D 红线分支。 |
| 阶段 4 | FIX-WRITE-ROWS | major | fe-core | — | 在 PluginDrivenInsertExecutor.doBeforeCommit() 的事务模型分支(connectorTx != null)补一行 loadedRows = connectorTx.getUpdateCnt(),回填翻闸丢失的 affected-rows,镜像 legacy MCInsertExecutor;getUpdateCnt 全链路已就绪,纯 fe-core 一处赋值。 |

阶段排序理由:
- **阶段 1 — 恢复读路径可用 (gate live SELECT)**: 两 blocker 直接决定 SELECT 能否工作; BE 不改(BE 仍按 max_compute 期望 MC 描述符), 修在 FE+connector。先修这层, 否则任何 live 读验证都不可信。
- **阶段 2 — 恢复 DDL 可用**: engine 门阻断无 ENGINE 的 CREATE TABLE(blocker, 分析期即报错); 远端名映射保 DDL 在 name-mapping 下的数据正确性(major)。
- **阶段 3 — 恢复分区可见 (partitions TVF / SHOW PARTITIONS)**: analyze 网关 + 分区元数据 override, 打通 T06c 已接但当前不可达的 BE handler; 含 Batch-D 红线守护。
- **阶段 4 — 写回正确性 (affected rows)**: 数据已写对, 仅修客户端/audit 报告行数; 独立小改, 可与任意阶段并行。

> 提交纪律(项目硬约定): **每 issue 独立 commit**, 改 fe-core 带 `-pl :fe-core -am`, 改连接器带对应 `-pl`, 读真实 BUILD/MVN_EXIT/CS_EXIT, import-gate 从 repo 根跑。

## 1. 🔴 Batch-D 红线(修复期必须守住)

- **勿删** `PartitionsTableValuedFunction.java:173` 的 `MaxComputeExternalCatalog` 分支。Batch D 设计 §2 称「T06c 已加 PluginDriven 分支, Batch D 删 MC 分支」—— 前提经本轮 git 核实为**假**(commit `2cf7dfa81ad` 只改 `MetadataGenerator.java`, 从未触该 TVF)。照删 = partitions() 对 MC 永久不可用。正确动作 = FIX-PART-GATES 先**新增** PluginDriven 分支, Batch D 再仅删残留 legacy MC 引用。

## 2. 逐 issue 修复设计

### 阶段 1 — 恢复读路径可用 (gate live SELECT)

### FIX-READ-DESC — 读路径 TableDescriptor 类型混淆 — 补 buildTableDescriptor override 产 TMCTable

- **Problem**: 翻闸后(catalog 为 `PluginDrivenExternalCatalog`/type=`max_compute`)对 MaxCompute 外表的任意 `SELECT` 在 BE 端非法向下转型崩溃或读出垃圾数据。触发条件:任何走 JNI scanner 的 MC 读(`range.table_format_params.table_format_type == "max_compute"`)。用户可见症状:查询崩(段错误/未定义行为)或返回错误数据 + 无鉴权(endpoint/project/quota/凭证全为越界内存)。legacy 路径正常,翻闸即坏 —— 回归=是,severity=blocker。

- **Root Cause**: 精确链路:
  - FE: `fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalTable.java:249-258` —— `toThrift()` 调 `metadata.buildTableDescriptor(...)`,MC connector 未 override → 命中 SPI 默认 `fe/fe-connector/fe-connector-api/.../ConnectorTableOps.java:146-151` 返 `null` → 走 `:257` 兜底产出 `TTableType.SCHEMA_TABLE` 描述符,且**不含** `mcTable` 字段。
  - BE descriptor 工厂 `be/src/runtime/descriptors.cpp:635-636` 据 `SCHEMA_TABLE` 创建 `SchemaTableDescriptor`(非 `MaxComputeTableDescriptor`,后者仅 `:653-654` 的 `MAX_COMPUTE_TABLE` 分支创建)。
  - BE scanner `be/src/exec/scan/file_scanner.cpp:1069-1070` 在 `table_format_type=="max_compute"` 时**无条件** `static_cast<const MaxComputeTableDescriptor*>(_real_tuple_desc->table_desc())` —— 把一个实际是 `SchemaTableDescriptor*` 的指针向下转成 `MaxComputeTableDescriptor*`,随后 `mc_desc->init_status()` 及 reader 读 `_endpoint/_project/_quota/_props` 全是越界/垃圾内存。
  - 注:`MaxComputeTableDescriptor` 构造函数 `be/src/runtime/descriptors.cpp:289-320` 直接读 `tdesc.mcTable.region/project/table/...`(部分字段无 `__isset` 守卫),即便侥幸进了该分支也要求 `mcTable` 必须被 set;只有 `endpoint`/`quota` 缺失才走 `init_status` 报错路径,其余字段缺失即 UB。
  - 直接根因:`fe/fe-connector/fe-connector-maxcompute/.../MaxComputeConnectorMetadata.java` 缺 `buildTableDescriptor` override(对比已 override 的 `JdbcConnectorMetadata.java:182-217` / `EsConnectorMetadata.java:121-131`)。

- **Design**: 在 `MaxComputeConnectorMetadata` 新增 `buildTableDescriptor` override,产出 `TTableType.MAX_COMPUTE_TABLE` 的 `TTableDescriptor` 并 `setMcTable(TMCTable)`,mirror legacy `MaxComputeExternalTable.toThrift()`(`fe/fe-core/.../maxcompute/MaxComputeExternalTable.java:305-322`)的可观察行为。
  - 方法签名(与 SPI default 完全一致,override 即可,**SPI 无需扩展**):
    `public org.apache.doris.thrift.TTableDescriptor buildTableDescriptor(ConnectorSession session, long tableId, String tableName, String dbName, String remoteName, int numCols, long catalogId)`
  - 要 set 的 `TMCTable` 字段(对照 `gensrc/thrift/Descriptors.thrift:455-467` 与 legacy):`setEndpoint(...)`、`setQuota(...)`、`setProject(...)`、`setTable(...)`、`setProperties(...)`。其余 thrift 字段(region/access_key/secret_key/public_access/odps_url/tunnel_url)legacy 也未 set(已 `// deprecated`),保持不 set —— 凭证经 `properties` map 下传,与 legacy 一致,BE `descriptors.cpp:313` 走 `__isset.properties` 的 likely 分支。
  - 字段取值来源:connector 已在 `MaxComputeDorisConnector` 持有 `getEndpoint()` / `getQuota()` / `getProperties()` / `getDefaultProject()`(`MaxComputeDorisConnector.java:194-211`),但 `MaxComputeConnectorMetadata` 当前 ctor 只接 `odps/structureHelper/defaultProject`(`:72-78`),**缺 endpoint/quota/properties**。最小改动:扩 `MaxComputeConnectorMetadata` 构造参数,在 `MaxComputeDorisConnector.getMetadata`(`:160-161`)把 `endpoint/quota/properties` 透传进去(prop 源已现成,无需 re-resolve)。
  - **`project`/`table` 取值是关键 parity 判定点(显式标注)**:legacy 用 `tMcTable.setProject(dbName)` 其中 `dbName=db.getFullName()`(本地名)、`setTable(name)`(本地名)—— 因 MC 历史路径 local==remote。SPI `toThrift` 调用点已传 `dbName=db.getRemoteName()`、`remoteName=getRemoteName()`(`PluginDrivenExternalTable.java:247,250`)。BE 用 `_project/_table` 去 ODPS 建读 session,**必须是 ODPS 真实可寻址名(remote)**。故 override 应 `setProject(dbName 参数)`(已是 remote)、`setTable(remoteName 参数)`(remote),而非 legacy 的本地名。这在 `meta_names_mapping`/`lower_case_meta_names` 生效时与 legacy 行为有别,但属"翻闸更正确"(legacy 用本地名在映射开启时本就会寻址错表),与 review 中 DDL-P3/DDL-C2 同源;此处取 remote 是有意修正,不算回归。建议在 commit message / OQ 登记。
  - **通用性判定**:这是 MC 专有(MC connector 缺 override),非通用插件层缺口 —— `PluginDrivenExternalTable.toThrift` 的 dispatch + SPI hook + null 兜底机制本身正确且已 keyed on connector(每个 connector 自带 typed descriptor,jdbc/es 已证);修法落在 MC connector,无需碰 fe-core dispatch、无需 hardcode maxcompute。fe-core 的 `getEngineTableTypeName`(`:232-233`)已用 `getType()=="max_compute"` 而非 hardcode,符合既有约定。

- **Implementation Plan**(逐文件逐方法,均为单 issue 独立 commit):
  1. [fe-connector-maxcompute] `MaxComputeConnectorMetadata.java`:扩构造函数,新增 `private final String endpoint; private final String quota; private final Map<String,String> properties;` 三字段(import 复用现有 `java.util.Map`),ctor 增对应参数并赋值。
  2. [fe-connector-maxcompute] `MaxComputeConnectorMetadata.java`:新增 `@Override public org.apache.doris.thrift.TTableDescriptor buildTableDescriptor(...)`:new `TMCTable`,`setEndpoint(endpoint)`/`setQuota(quota)`/`setProject(dbName)`/`setTable(remoteName)`/`setProperties(properties)`;new `TTableDescriptor(tableId, TTableType.MAX_COMPUTE_TABLE, numCols, 0, tableName, "")`,`setMcTable(...)`,return。全程用全限定 `org.apache.doris.thrift.*`(对齐 jdbc/es override 的写法,避免新 import 触 import-gate;若改用 import 须同步 checkstyle import 顺序)。
  3. [fe-connector-maxcompute] `MaxComputeDorisConnector.java:160-161` `getMetadata`:`new MaxComputeConnectorMetadata(odps, structureHelper, defaultProject, endpoint, quota, properties)`(字段已在 ctor/doInit 就绪)。
  4. [be] 无需改动(`MAX_COMPUTE_TABLE` 分支与 `MaxComputeTableDescriptor` 已存在,本修法令 FE 重新走该分支)。
  5. [thrift] 无需改动(`TMCTable` 字段集已满足,见 `Descriptors.thrift:455-467`)。
  6. [fe-core] 无需改动(`PluginDrivenExternalTable.toThrift` 兜底逻辑保留作其他无 typed-descriptor 的 connector 的安全网;本修法令 MC 不再走兜底)。
  - 守门:仅改连接器 → `mvn ... -pl :fe-connector-maxcompute`(不触 fe-core,无需 `-am`/`-pl :fe-core`)。

- **Risk**:
  - 回归风险低:纯新增 override + ctor 透传,不改 fe-core dispatch、不改 BE、不改 thrift、不改其他 connector。jdbc/es/trino 走各自 override 或 null 兜底,零影响。
  - 不触 keep 集(legacy `MaxComputeExternalTable.toThrift` 仍在,翻闸下不被调用;Batch D 删除 legacy 时一并移除,与本 fix 无序约束冲突)。
  - checkstyle/import-gate:用全限定名规避新 import;若团队约定要 import,则需校 import 顺序与 unused。
  - 唯一语义差异点:`project`/`table` 取 remote 名(上文已标注,有意修正,与 DDL 远端名修复同源);若 reviewer 坚持严格 mirror legacy 本地名,会在映射开启时寻址错表 —— 应选 remote。
  - BE `descriptors.cpp:289-320` 对 region/access_key 等字段无 `__isset` 守卫:本 fix 不 set 这些字段,thrift 默认空串 → BE 读到空串而非 UB(因为现在 mcTable 整体被 set),与 legacy 完全一致(legacy 同样不 set 这些)。

- **Test Plan**:
  - UT(放 `fe/fe-connector/fe-connector-maxcompute/src/test/java/org/apache/doris/connector/maxcompute/`):新增纯 Java 单测(无需 live ODPS、无 fe-core 依赖,沿用该模块 child-first loader 约定,见 `OdpsClassloaderIsolationTest.java`)直接 new `MaxComputeConnectorMetadata`(传入构造好的 endpoint/quota/properties stub),调 `buildTableDescriptor`,断言:① 返回非 null;② `getTableType()==TTableType.MAX_COMPUTE_TABLE`;③ `isSetMcTable()==true`;④ `getMcTable().getEndpoint()/getQuota()/getProject()(==dbName 参数=remote)/getTable()(==remoteName 参数)/getProperties()` 与输入一致。该测试 encode WHY:断 thrift 类型与 mcTable 存在性,正是 BE `static_cast` 与 `descriptors.cpp` 凭证读取所依赖的契约(Rule 9)。jdbc/es 当前无对应 UT,本测试补齐该 connector 的描述符契约门。
  - E2E(`regression-test/suites/external_table_p2/maxcompute/`,需 live ODPS,user-run):在翻闸开关下跑 `test_external_catalog_maxcompute.groovy` 与 `test_max_compute_all_type.groovy` 的 `SELECT`(断言点:查询不崩、行数/数据与 `.out` 基线一致 —— 验证 BE 拿到合法 `MaxComputeTableDescriptor` + endpoint/project/quota/凭证正确,即不再走 SCHEMA_TABLE 兜底);`test_max_compute_partition_prune.groovy` 的基础整表 `SELECT count(*)` 验证读路径打通(注:分区裁剪本身是 READ-P3 另一 issue,此处只断"读得出正确全量数据")。断言锚点为既有 `.out` 文件(`regression-test/data/external_table_p2/maxcompute/*.out`)。

**Open questions**: project/table 取 remote 名(dbName/remoteName 参数)而非 legacy 的本地名:在 meta_names_mapping/lower_case_meta_names 开启时与 legacy 行为有别。判定为有意修正(与 DDL-P3/DDL-C2 远端名修复同源),但需 reviewer 确认是否接受作为翻闸基线,或要求严格 mirror legacy 本地名。 · BE descriptors.cpp:289-320 对 region/access_key/secret_key/public_access/odps_url/tunnel_url 无 __isset 守卫;本 fix 不 set 这些(同 legacy)→ thrift 默认空串。需确认无任何 BE 路径依赖这些 deprecated 字段非空(凭证全经 properties map,已核 descriptors.cpp:313 走 properties 分支)。 · MaxComputeConnectorMetadata 改用全限定 thrift 类名(对齐 jdbc/es)还是新增 import —— 取决于该模块 checkstyle import-gate 约定,需在实现时确认。

#### 🔎 对抗 critic — verdict: `sound`

**需修正(corrections)**:
- 设计 Risk/Design 里把'project/table 取 remote 名'描述成与 legacy 有别的、需 reviewer 容忍的语义差异,论证迂回。实际核查更强:SPI 读 session 本身就用 remote 名构建——PluginDrivenScanNode.java:130-131 用 db.getRemoteName()/table.getRemoteName() 调 getTableHandle,MaxComputeScanPlanProvider 据此 handle 的 getTableIdentifier 建 TableReadSession,JNI scanner(MaxComputeJniScanner:136-148)对 project requireNonNull 并 odps.setDefaultProject(project)。故 descriptor 用 remote 名是与 SPI 读 session 一致的唯一正确选择;若按 reviewer 建议改回 legacy 本地名,反而会和 SPI 读 session 的 project 不一致。设计结论(取 remote)正确,但其'legacy 本地名因 local==remote 才侥幸工作'的根因解释不完整:legacy 读 session 也用本地名(MaxComputeExternalTable:167 getOdpsTableIdentifier(dbName,name)),legacy 整条链都是本地名,所以无映射时本就一致——这与 descriptor 修复无因果,设计把它说成'此处取 remote 是有意修正 legacy 寻址错误'略夸大:descriptor 的 project/table 对实际数据读几乎是 vestigial(真正寻址靠 FE 端预建的序列化 scan session)。

**遗漏(gaps)**:
- TTableDescriptor 6th 构造参数(dbName 字段)与 legacy 不一致,设计未 surface(违反 Rule 7): 设计 Implementation Plan step 2 写 `new TTableDescriptor(tableId, MAX_COMPUTE_TABLE, numCols, 0, tableName, "")` 用空串,而 legacy MaxComputeExternalTable.toThrift:318-319 用 `new TTableDescriptor(getId(), MAX_COMPUTE_TABLE, schema.size(), 0, getName(), dbName)` 传 dbName。该 6th 参映射到 thrift TTableDescriptor.dbName(field 8),BE descriptors.cpp:219 读入 TableDescriptor::_database。MC 读路径不用 _database(JNI scanner 用 TMCTable.project/table),故空串无害,但设计选了 jdbc 约定(jdbc override 也用 "")却与它声称要 mirror 的 legacy 行为分歧——设计文档把这点说成完全 mirror legacy,实际未 mirror 该参数,应显式登记此偏差。
- UT 覆盖边界:设计的连接器内 UT 直接 new MaxComputeConnectorMetadata 调 buildTableDescriptor,只验证 override 自身产出,完全不覆盖 fe-core 侧 PluginDrivenExternalTable.toThrift(:249) 是否真的 CALL 该 override。若 toThrift dispatch/null 兜底逻辑回归(例如把 schema.size() 传错、或 remoteName/dbName 实参传反),该 UT 零感知。设计自述'补齐 descriptor 契约门'但 contract 的另一半(调用方正确传 remote 名 dbName=db.getRemoteName()、remoteName=getRemoteName())无任何门禁。
- E2E 计划里 test_max_compute_partition_prune.groovy 仅跑 `SELECT count(*)` 整表读,断言'读得出全量数据'。但 count(*) 可能被优化为 BE meta/统计路径或不实际拉列数据,未必触发 file_scanner.cpp:1069 的 MaxComputeTableDescriptor static_cast。要真正验证 descriptor 修复,E2E 必须断言至少一个带列投影/带数据行的 SELECT(test_external_catalog_maxcompute.groovy 的 SELECT 列查询已覆盖,但 partition_prune 那条 count(*) 作为'读路径打通'证据偏弱)。

**额外风险**:
- prompt 质疑的 time_zone 缺失:已核实为非问题,但设计完全没提到 time_zone,说明设计者可能没意识到 JNI scanner(MaxComputeJniScanner:139)对 time_zone 做 requireNonNull。它之所以不崩,是因为 BE JNI 框架在 jni_reader.cpp:151 `_scanner_params.emplace("time_zone", _state->timezone())` 对所有 JNI scanner 通用注入,不走 descriptor。建议设计显式记录此依赖,否则后续若有人改 descriptor properties 覆盖逻辑(BE max_compute_jni_reader.cpp:62 先 mc_desc->properties() 再覆盖固定 key,但 time_zone 不在覆盖集),可能误删 time_zone 来源而不自知。
- BE max_compute_jni_reader.cpp:62-66 的 properties 合并顺序:先 `auto properties = mc_desc->properties()`(=TMCTable.properties),再硬覆盖 endpoint/quota/project/table。意味着若 catalog properties map 里恰好含 key 'endpoint'/'quota'/'project'/'table'(裸 key,非 mc.*),会被 descriptor 字段覆盖——与 legacy 行为一致(legacy 也 setProperties(同一 map)),无回归;但设计未提 properties map 与这些保留 key 的交互,属隐含假设。
- MaxComputeConnectorMetadata 当前 ctor(:72-78)被 MaxComputeDorisConnector.getMetadata(:160) 每次调用 new 一个新实例。设计扩 ctor 加 endpoint/quota/properties 透传后,需确保 getMetadata 处 endpoint/quota 已 doInit 就绪(getEndpoint/getQuota 内含 ensureInitialized,但 getMetadata 已先 ensureInitialized,且设计建议直接传字段而非 getter)。若改传裸字段 endpoint/quota 而非 getEndpoint()/getQuota(),需确认 getMetadata 调用时这俩字段非 null——getMetadata:159 已 ensureInitialized(),字段在 doInit 赋值,OK;此为低风险但设计 step 3 写 `new MaxComputeConnectorMetadata(odps, structureHelper, defaultProject, endpoint, quota, properties)` 直接引裸字段,依赖 ensureInitialized 已跑,需保证调用序不变。
- checkstyle:设计建议全限定 org.apache.doris.thrift.* 规避新 import,符合 jdbc/es 既有写法;但新增 `private final Map<String,String> properties` 复用现有 java.util.Map import 即可,这点 OK。无额外 import-gate 风险。

---

### FIX-READ-SPLIT — byte_size split size sentinel — 默认 split 回填 size=-1

- **Problem**
  - 用户可见症状:在默认配置下查询 MaxCompute(翻闸后 PluginDriven 路径)外表会读出**错误/损坏的数据**(行数、列值与真实表内容不符,而非报错)。`select count(*) from <mc_table>`、`select *` 等都会命中。
  - 触发条件:`mc.split_strategy` 取默认值 `byte_size`(`MCConnectorProperties.DEFAULT_SPLIT_STRATEGY = SPLIT_BY_BYTE_SIZE_STRATEGY`),即不显式配置 split 策略时的默认路径。`row_offset` 策略和 limit-optimization 单 split 路径不受影响(它们本就走真实 offset/count)。
  - 严重性 blocker:即便绕过本 review 的另一个 blocker(READ-P1),默认读路径仍产出错误数据,且不报错——静默错误,最危险。

- **Root Cause**
  - BE 用 `split_size == -1` 这一 sentinel 来区分两种 split 类型,这是唯一判别依据:
    - BE C++ 把 `range.size` 透传给 Java scanner:`be/src/format/table/max_compute_jni_reader.cpp:70` → `properties["split_size"] = std::to_string(range.size)`。
    - Java scanner 据此判型:`fe/be-java-extensions/max-compute-connector/.../MaxComputeJniScanner.java:125-128` → `if (splitSize == -1) splitType = BYTE_SIZE; else splitType = ROW_OFFSET;`,再在 `open()`(:207-210)分别建 `new IndexedInputSplit(sessionId, (int) startOffset)`(BYTE_SIZE)或 `new RowRangeInputSplit(sessionId, startOffset, splitSize)`(ROW_OFFSET)。注意:scanner **完全不读** range 里携带的 `split_type` 属性/`getPath()` 的 `/byte_size`,只看 `split_size` 数值。
  - legacy 基线对 byte_size split 显式回填 `size = -1`:`fe/fe-core/.../maxcompute/source/MaxComputeScanNode.java:657-662` → `new MaxComputeSplit(BYTE_SIZE_PATH, splitIndex, /*length=*/-1, /*fileLength=*/splitByteSize, ...)`(构造器签名 `MaxComputeSplit(path, start, length, fileLength, ...)`,见 MaxComputeSplit.java:40)——第 3 参 `length=-1`,`splitByteSize` 进第 4 参 `fileLength`(BE 不读)。随后 `:153 rangeDesc.setSize(maxComputeSplit.getLength())` ⇒ `setSize(-1)`。
  - 翻闸 connector **没有**回填 sentinel:`fe/fe-connector/fe-connector-maxcompute/.../MaxComputeScanPlanProvider.java:268` 对 byte_size split 用 `.length(splitByteSize)`(= 默认 268435456),而 `MaxComputeScanRange.populateRangeParams`(MaxComputeScanRange.java:122)做 `rangeDesc.setSize(getLength())` ⇒ `setSize(splitByteSize)`。于是 BE 收到 `split_size = 268435456 ≠ -1`,把 byte-size split **误判为 ROW_OFFSET**,用 `new RowRangeInputSplit(sessionId, startOffset=splitIndex, rowCount=splitByteSize)` 去读 → 错误数据。
  - 精确根因点:`MaxComputeScanPlanProvider.java:268`(`.length(splitByteSize)` 应等价为 sentinel,使 size 落到 -1)。

- **Design**
  - 最小、最忠于 legacy 可观察行为的修法:**在 byte_size split 分支把传给 range 的 length 设为 -1**,使 `getLength()→setSize(-1)` 恢复 sentinel。等价于 legacy 的 `MaxComputeSplit(..., length=-1, fileLength=splitByteSize, ...)`。
    - 改 `MaxComputeScanPlanProvider.java:268`:`.length(splitByteSize)` → `.length(-1)`。
    - **[T06d 实施修正 — 原"只流向两处"声称有误]** `getLength()` 在该 byte_size range 里实际流向**三处**(非两处):`setPath` cosmetic 字符串(:120)、`setSize`(:122,BE sentinel,load-bearing),以及 `PluginDrivenSplit.java:42` 传入 `FileSplit.length`(再被 `FederationBackendPolicy.java:499` 一致性哈希分配、`FileQueryScanNode.java:430` `totalFileSize += getLength()` 消费)。结论不变(改后第三处看到 -1 而非 268435456,**良性且改善 legacy parity**——legacy 也是 -1),但原文"grep 全证实只两处"是事实错误。完整修正影响分析见 `P4-T06d-FIX-READ-SPLIT-design.md` §Risk(含 (a) 一致性哈希 split→BE 落点会与当前 buggy build 不同=良性、对齐 legacy,勿误判为回归;(b) byte_size 扫描 `totalFileSize` 转负,pre-existing legacy 行为,仅 stats/cost/explain)。BE 端从不消费 byte_size split 的真实字节数(legacy 把它塞进未读的 fileLength);split 的字节切分早已在 `SplitOptions.SplitByByteSize(splitByteSize)`(:131)阶段完成,session 自带该信息,BE 用 `IndexedInputSplit(sessionId, splitIndex)` 复原,不需要 size。
    - 副作用对齐:改后 `setPath` 字符串变为 `[ splitIndex , -1 ]`,这与 legacy 完全一致(legacy `getStart()=splitIndex`、`getLength()=-1` ⇒ 同样的 `[ splitIndex , -1 ]`)。即 path 字符串也是精确 mirror,无新增偏差。
  - 不需要扩展 SPI、不需要新增 override、不改 thrift:`TFileRangeDesc.size` 字段与 `populateRangeParams` seam 均已存在,sentinel 是纯数值约定。
  - 关于"通用插件层 vs MC 专有":此 sentinel(`split_size == -1` ⇒ IndexedInputSplit)是 **MaxCompute 连接器与其 BE-side `MaxComputeJniScanner` 之间私有的、per-range 的语义契约**,经由 `MaxComputeScanRange.populateRangeParams`(连接器自有代码,getTableFormatType="max_compute" 专属分支)实现,**不**经过 `PluginDrivenScanNode` 的通用逻辑(后者只调用 `scanRange.populateRangeParams(...)` 委派,见 PluginDrivenScanNode.java:392)。因此修复**就该 keyed 在 MaxCompute 连接器自己的 range 实现里**,这正是 SPI 设计的"per-range 契约由 provider 负责、与 legacy 等价"原则(P3-T08-tableformat-dispatch-design.md §结论 4:per-range 契约不变)。无须、也不应在 fe-core 通用层 hardcode maxcompute。无历史决策被推翻(review 已确认"历史零记载");本设计反而补齐了 P4-T05-T06-cutover-design 未显式记录的 per-range size 契约。

- **Implementation Plan**
  - [fe-connector-maxcompute] `MaxComputeScanPlanProvider.java:268`:把 byte_size 分支的 `.length(splitByteSize)` 改为 `.length(-1L)`。加一行简短注释说明 -1 是 BE 区分 BYTE_SIZE/ROW_OFFSET 的 sentinel(mirror legacy MaxComputeScanNode.java:659 的 length=-1)。row_offset 分支(:286 `.length(count)`)和 limit-optimization 分支(:334 `.length(rowsToRead)`)**不动**——它们正确发送真实 rowCount。
  - [fe-connector-maxcompute] 不改 `MaxComputeScanRange.java`:`populateRangeParams` 的 `setSize(getLength())` 保持原样,fix 后自然回填 -1。`Builder.length` 默认值已是 -1(:134),与意图一致。
  - 守门:本 issue 独立 commit;只触连接器,构建带 `-pl :fe-connector-maxcompute`(连带其依赖 `-am` 视根 pom 而定)。不需 `-pl :fe-core`,不需 BE 重编,不动 thrift。

- **Risk**
  - 回归风险:极低且收敛。仅改默认 byte_size 路径的一个常量,使其与 legacy 字节对齐;row_offset / limit 路径不变。改后默认查询从"静默错误"变为"正确",方向单一。
  - 对其他连接器/插件影响:**零**。sentinel 是 MaxCompute connector ↔ MaxComputeJniScanner 私有契约,改动局限于 MC 的 range 构造分支;Hive/Hudi/ES 等其他 provider 各自的 `populateRangeParams` 与 size 语义不受影响(它们的 size 是真实文件字节,与本 sentinel 无关)。
  - keep 集:本 fix **不**触碰 legacy `MaxComputeScanNode.java`(keep 基线,只读对照);只改翻闸 connector。符合"legacy 保留、cutover 对齐 legacy 可观察行为"。
  - checkstyle / import-gate:仅改一个字面量参数,不新增 import、不新增类型;`-1L` 与既有 long 字面量风格一致。无 import-gate 影响。
  - 潜在隐患排查:**[T06d 修正]** `getLength()` 在 MC range 中除 setPath/setSize 外还有第三消费方 `PluginDrivenSplit.java:42 → FileSplit.length`(被 `FederationBackendPolicy.java:499` / `FileQueryScanNode.java:430` 消费),原文"无其它消费方(grep 已证)"有误;但该三处改后均看到 -1,良性且对齐 legacy,详见 `P4-T06d-FIX-READ-SPLIT-design.md` §Risk。`setPath` 字符串与 legacy 同步变为 `[ splitIndex , -1 ]`,不破坏任何 BE 解析(BE 不解析该 path 字符串内容,只用作显示/定位)。

- **Test Plan**
  - UT(放 `fe/fe-connector/fe-connector-maxcompute/src/test/java/org/apache/doris/connector/maxcompute/`,新增轻量 `MaxComputeScanRangeTest`,无网络、无 odps 依赖,符合该模块既有 CI-runnable 单测约定):
    - 断言 1(回归红点,Rule 9 编码 WHY):用 `MaxComputeScanRange.builder().start(splitIndex).length(-1).splitType(SPLIT_TYPE_BYTE_SIZE)...build()` 构造,调用 `populateRangeParams(formatDesc, rangeDesc)`,断言 `rangeDesc.getSize() == -1`。注释写明:size 必须是 -1 sentinel,否则 BE 把 byte_size split 误判为 ROW_OFFSET → 损坏读(链接 MaxComputeJniScanner.java:125-128)。该断言在 fix 前必然失败(当前会是 splitByteSize)。
    - 断言 2(对照):row_offset range(`.length(count).splitType(SPLIT_TYPE_ROW_OFFSET)`)断言 `rangeDesc.getSize() == count`(真实值,非 -1),锁住"只有 byte_size 用 sentinel"的意图。
    - 断言 3(path mirror,可选):断言 byte_size range 的 `rangeDesc.getPath()` == `"[ <splitIndex> , -1 ]"`,与 legacy 字符串对齐。
  - E2E(`regression-test/suites/external_table_p2/maxcompute/`,默认 byte_size 策略,即不显式设 `mc.split_strategy` 的常规 catalog):
    - 复用 `test_external_catalog_maxcompute.groovy` 的既有读断言(order_qt_q1 `select count(*) from web_site`、order_qt_q2 `select *`、int_types / mc_parts 系列)——这些查询在默认 byte_size 路径下,fix 前读出错误数据(行数/列值与 .out 基线不符),fix 后应与 legacy `.out` 基线一致。关键断言点:`count(*)` 行数 与 全列 `select *` 的逐行值。
    - 若 CI 有 legacy↔cutover 对照机制,断言两者结果集逐行相等(本 fix 的核心目标即"cutover 默认路径 == legacy")。
    - 不新增 suite:该 blocker 是默认路径读正确性,既有读套件已是最直接的覆盖面;新增反而偏离最小改动。

**Open questions**: E2E 严格落地依赖一个真实 MaxCompute 端点(external_table_p2 需凭证),CI 中**默认跳过**该套件;唯一 CI-runnable 守门是 UT。T06d 采用的 UT 直接驱动 provider 的 byte_size 分支(`buildSplitsFromSession` 反射 + 离线 fake session),断言 `rangeDesc.getSize()==-1`,**provider 分支真实回退会令其失败**(已验证 expected:<-1> but was:<268435456>);E2E 作为人工/带凭证回归。 · 是否存在依赖现有 byte_size 错误 size 的隐性消费方?**[T06d 修正]** 实际有第三消费方 `FileSplit.length`(一致性哈希 + totalFileSize),改后看到 -1=良性且对齐 legacy(legacy 同为 -1),非 setPath/setSize 两处,详见 `P4-T06d-FIX-READ-SPLIT-design.md` §Risk;explain/profile 的 totalFileSize 会转负(pre-existing legacy 行为,仅 stats/cost)。

#### 🔎 对抗 critic — verdict: `sound`

**需修正(corrections)**:
- Factual correction to the design's grep claim: getLength() for a byte_size MaxComputeScanRange has THREE consumers, not two -- setPath (MaxComputeScanRange.java:120), setSize (:122), AND PluginDrivenSplit.java:42 -> FileSplit.length (further read by FederationBackendPolicy.java:499 and FileQueryScanNode.java:430). The conclusion (fix is legacy-equivalent and safe) is unchanged and verified, but the supporting statement 'grep 全证实 ... 只 流向两处' is wrong and should be corrected.
- Minor: the design says the fix mirrors legacy MaxComputeScanNode.java:659 length=-1; verified accurate -- legacy constructor MaxComputeSplit(BYTE_SIZE_PATH, splitIndex, -1, splitByteSize, ...) puts -1 in arg3 (length) and splitByteSize in arg4 (fileLength, unread by BE). Connector after fix is byte-exact (setSize=-1, setStartOffset=splitIndex, path='[ splitIndex , -1 ]'). No correction needed to the core claim.

**遗漏(gaps)**:
- Risk analysis omits a third consumer of getLength(): PluginDrivenSplit.java:42 passes scanRange.getLength() into the FileSplit.length field. The design's repeated claim that splitByteSize/getLength flows ONLY into setPath(:120) and setSize(:122) ('grep fully confirms') is factually incomplete. FileSplit.length is consumed downstream by FederationBackendPolicy.java:499 (primitiveSink.putLong(split.getLength()) in consistent-hash backend assignment) and FileQueryScanNode.java:430 (totalFileSize += split.getLength()). After the fix these see -1 instead of 268435456 -- which is BENIGN because legacy MaxComputeSplit also used length=-1 (the current buggy cutover diverges from legacy here too), so the fix actually improves parity. But the design must update the grep claim and add these consumers to the impact analysis instead of asserting 'only two places'.
- Test Plan does not acknowledge that the named E2E suite (regression-test/suites/external_table_p2/maxcompute/test_external_catalog_maxcompute.groovy) is an external_table_p2 suite requiring live MaxCompute/ODPS credentials, so it will be skipped in normal CI. The design frames it as 'the most direct coverage', but the only CI-runnable automated guard is the proposed UT. This should be stated explicitly so the fix is not merged believing E2E runs unattended.
- The design scopes out (correctly) the broader read-path descriptor population, but for the reviewer's checklist: the JNI scanner requires TIME_ZONE (MaxComputeJniScanner.java:139 Objects.requireNonNull on 'time_zone'), and BE max_compute_jni_reader.cpp:62-77 does NOT set time_zone in the properties map -- it must arrive via mc_desc->properties()/endpoint. Whether the cutover descriptor carries time_zone/project/quota/endpoint correctly is the separate READ-P1 blocker; this fix neither helps nor regresses it, but the split-size fix alone will NOT yield correct reads if READ-P1 is unfixed. The design states this dependency but does not call out the time_zone requirement specifically.

**额外风险**:
- Backend-assignment determinism: FederationBackendPolicy.java:499 hashes split.getLength() into the consistent-hash placement. Changing length from 268435456 to -1 for every byte_size split changes which backend each split lands on (vs the current buggy cutover). This is invisible/benign for correctness and matches legacy, but means a before/after A-B comparison of split-to-BE placement on the SAME cutover build will differ -- worth noting so it is not mistaken for a regression during validation.
- FileQueryScanNode.java:430 accumulates totalFileSize += getLength(); with length=-1 per split this drives totalFileSize negative for byte_size scans (one -1 per split). This is a pre-existing legacy behavior (legacy also had -1) used only for stats/cost/logging, not correctness, but it propagates to profile/explain numbers and any cost-based heuristic keyed on totalFileSize. Low risk, pre-existing, but the design does not mention it.
- Other PluginDriven connectors (jdbc/es/trino/hive/hudi): the fix is strictly inside MaxComputeScanRange's byte_size branch in MaxComputeScanPlanProvider, so zero cross-connector impact is confirmed -- the design's claim holds. No additional risk here, but it is worth recording that the -1 sentinel semantics are private to the MaxCompute connector <-> MaxComputeJniScanner contract and any future generic use of ConnectorScanRange.getLength()==-1 by other code paths would need re-examination.
- No follower-replay/master sync concern: verified the change is purely in query-plan-time scan-range construction (planScan/populateRangeParams), not persisted to the edit log, so no replay/HA implications. (Confirms one of the prompt's checklist items as a non-issue.)

---

### 阶段 2 — 恢复 DDL 可用

### FIX-DDL-ENGINE — 无 ENGINE 的 CREATE TABLE — paddingEngineName/checkEngineWithCatalog 识别 PluginDriven

- **Problem**
  翻闸(T06b)后 `max_compute` catalog 实例化为 `PluginDrivenExternalCatalog`(`CatalogFactory.java:52` SPI_READY_TYPES 含 `max_compute` → `:112` new PluginDrivenExternalCatalog)。用户在该 catalog 下执行**不写 `ENGINE=maxcompute` 子句**的 `CREATE TABLE`(legacy 下完全可用、且是 MC 最常见写法,见 `regression-test/suites/external_table_p2/maxcompute/test_max_compute_create_table.groovy` Test1/Test2/Test3 均无 ENGINE 子句)时,在**分析期**直接抛 `AnalysisException: Current catalog does not support create table: <ctl>`,根本到不了 `PluginDrivenExternalCatalog.createTable` override(`PluginDrivenExternalCatalog.java:264`)。触发条件:catalog 类型走 SPI(当前仅 `max_compute` 是 full-adopter;jdbc/es/trino 也走 PluginDriven 但本身不支持 CREATE TABLE,故主要可见于 MC,但缺口是通用插件层缺口)。这是 legacy 可用、翻闸即坏的 blocker 级回归,且 T06c 回归矩阵把 CREATE TABLE 一律误标 PASS、未覆盖此子场景。

- **Root Cause**
  `CreateTableInfo.paddingEngineName`(`fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/CreateTableInfo.java:896-918`)在 `engineName` 为空时按 catalog **具体子类** `instanceof` 推断 engine:`:912 else if (catalog instanceof MaxComputeExternalCatalog) engineName = ENGINE_MAXCOMPUTE`,无匹配则 `:914-915 throw "Current catalog does not support create table"`。翻闸后 catalog 不再是 `MaxComputeExternalCatalog` 而是 `PluginDrivenExternalCatalog`,既非 HMS/Iceberg/Paimon 也非 MC → 落 else 抛错。
  同一缺陷存在于 `checkEngineWithCatalog`(`:376-393`),其 `:390 else if (catalog instanceof MaxComputeExternalCatalog && !engineName.equals(ENGINE_MAXCOMPUTE)) throw` —— 若用户**显式写** `ENGINE=maxcompute`,翻闸后该 catalog-engine 一致性校验被静默绕过(漏 throw,非崩溃),属同源的镜像缺口,应一并修以保持 parity。
  根因层面:这两处 dispatch keyed on legacy 具体子类(`MaxComputeExternalCatalog`),而 PluginDriven SPI 把所有 SPI 连接器收敛到单一 `PluginDrivenExternalCatalog`,catalog 的真实类型只剩 `getType()`(返回 props 里的 catalog type 字符串,如 `"max_compute"`,见 `PluginDrivenExternalCatalog.java:235-239`)。这是与 [catalog-spi-cutover-fe-dispatch-gap] 同族的"FE 分发未接 SPI"缺口。

- **Design**
  仿照仓内既有约定 `PluginDrivenExternalTable.getEngine()`(`PluginDrivenExternalTable.java:196-219`,switch on `((PluginDrivenExternalCatalog) catalog).getType()` 映射到各 engine 名)—— 在 `paddingEngineName` / `checkEngineWithCatalog` 增加 `PluginDrivenExternalCatalog` 分支,keyed on `getType()` 而非 hardcode `maxcompute`,使其通用惠及所有 full-adopter 连接器,且 Batch D 删 legacy MC 引用后仍成立。

  1. **`paddingEngineName`**:在 `:912` MC 分支之后、`:914 else` 之前,插入:
     `else if (catalog instanceof PluginDrivenExternalCatalog) { engineName = pluginCatalogTypeToEngine((PluginDrivenExternalCatalog) catalog); }`
     新增 private helper `pluginCatalogTypeToEngine(PluginDrivenExternalCatalog c)`:`switch (c.getType())` → `case "max_compute": return ENGINE_MAXCOMPUTE;`(其余 SPI 类型如 jdbc/es/trino 在 CREATE TABLE 上下文不会到这里,或可 default 落入"does not support create table" throw 以镜像它们 SPI 不支持建表的现状)。**关键映射点**:`getType()` 返回 `"max_compute"`(CatalogFactory key,带下划线),须映射到 `ENGINE_MAXCOMPUTE = "maxcompute"`(`:125`,无下划线)。**不可**复用 `TableType.MAX_COMPUTE_EXTERNAL_TABLE.toEngineName()` —— 该枚举无 case → 返回 null(确认见 `TableIf.java:225-269`,MC 不在 switch 内),会把 engineName 置 null 触发后续 NPE。
  2. **`checkEngineWithCatalog`**:在 `:390` MC 分支后插入对称分支:
     `else if (catalog instanceof PluginDrivenExternalCatalog && !engineName.equals(pluginCatalogTypeToEngine((PluginDrivenExternalCatalog) catalog))) throw new AnalysisException("MaxCompute type catalog can only use \`maxcompute\` engine.");`
     (msg 以连接器声明 engine 为准;最小改动可复用同一 helper。)
  3. **mirror 的 legacy 可观察行为**:legacy `MaxComputeExternalCatalog` → `ENGINE_MAXCOMPUTE`(`:912-913`),padding 后 engineName=`maxcompute`,顺利通过下游白名单 `checkEngineName:944`(含 ENGINE_MAXCOMPUTE)与 `analyzeEngine:1121-1127`(MC 允许 distribution/partition desc)。本修法令 PluginDriven(type=max_compute)产出同一 `maxcompute` 字符串,下游零改动即与 legacy 完全等价。
  4. **SPI 是否需扩展**:**不需**。`Connector` SPI(`fe-connector-api/.../Connector.java`)无 engine-name 声明,引入它属过度设计;`getType()` 已足够 key。本修法纯 fe-core 内,不触 SPI/connector/thrift/BE。
  5. **import**:`CreateTableInfo.java` 已 import `MaxComputeExternalCatalog`(`:52`)等;需新增 `import org.apache.doris.datasource.PluginDrivenExternalCatalog;`(同包 `org.apache.doris.datasource`,与既有 `CatalogIf`/`InternalCatalog` 同级)。
  6. **与历史决策的关系(显式标注)**:Batch D removal 设计(`P4-batchD-maxcompute-removal-design.md:100`)计划"删 CreateTableInfo ~:390/:912 的 2× `instanceof MaxComputeExternalCatalog`"。本修法不与之冲突但**修正其前提**:Batch D 不应直接删除这两个分支,而应在删 legacy MC 分支的同时**保留/已由本 fix 落地的 PluginDriven 分支**(keyed on getType()),否则删完会把无 ENGINE 的 CREATE TABLE 永久坐实为报错(正是 review 综合总结 §二.4 警告的"amendment 自触发"模式)。建议在 decisions-log 标注:DDL-P1 fix 先落 PluginDriven 分支,Batch D 退化为"仅删 legacy MC 的 2 个 instanceof 分支 + import"。

- **Implementation Plan**(逐文件逐方法,均 **fe-core** 层)
  1. [fe-core] `CreateTableInfo.java` 顶部 import 区新增 `import org.apache.doris.datasource.PluginDrivenExternalCatalog;`(放在 `:51 InternalCatalog` 与 `:52 maxcompute.MaxComputeExternalCatalog` 之间,按字母序)。
  2. [fe-core] `CreateTableInfo.java:896-918 paddingEngineName`:在 `:913` 之后、`:914 else` 之前插入 `else if (catalog instanceof PluginDrivenExternalCatalog)` 分支,调用新 helper。
  3. [fe-core] `CreateTableInfo.java:376-393 checkEngineWithCatalog`:在 `:391` 之后插入对称的 `else if (catalog instanceof PluginDrivenExternalCatalog && !engineName.equals(...))` 分支。
  4. [fe-core] `CreateTableInfo.java` 新增 private static helper `pluginCatalogTypeToEngine(PluginDrivenExternalCatalog)`:`switch(getType())` → `"max_compute"`→`ENGINE_MAXCOMPUTE`,default 抛"does not support create table"(或对 jdbc/es/trino 显式拒,保持其现状)。
  5. 守门:改 fe-core 用 `-pl :fe-core -am`;`fe-code-style`(Checkstyle) + import-gate(新 import 须真用到);本 issue 独立 commit `[P4-DDL-P1]`。

- **Risk**
  - **回归面极窄**:仅在 `engineName` 为空(无 ENGINE 子句)且 catalog 为 PluginDriven 时新增一条分支;HMS/Iceberg/Paimon/Internal/legacy-MC 路径字节级不变(分支顺序在 MC 之后、else 之前)。
  - **对其他连接器/插件**:helper default 分支保留对 jdbc/es/trino-connector 的"不支持建表"语义(它们 SPI 本就不支持 CREATE TABLE,落 default throw 与现状一致),无新增可用性也无新增破坏。`checkEngineWithCatalog` 的新分支仅在用户显式写错 ENGINE 时 throw,对正确写法无影响。
  - **keep 集**:本 fix **依赖** `MaxComputeExternalCatalog` import 仍在 keep 集(Batch D 删它前 DDL-P1 必须先修),需在 commit message / decisions-log 标注顺序依赖,避免 Batch D 误删 PluginDriven 分支。
  - **checkstyle/import-gate**:新增 1 个 import,helper 方法须有 Javadoc 或保持 private 简短;switch 默认分支不可漏。
  - **getType() 字符串脆性**:依赖 `"max_compute"` 字面量(CatalogFactory key),与 `PluginDrivenExternalTable.getEngine():212` 同一约定,风险已被既有代码承担;若未来改 key 两处需同步(可在 helper 注释引用 CatalogFactory.SPI_READY_TYPES)。

- **Test Plan**
  - **UT(fe-core)**:在 `fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/info/CreateTableInfoTest.java`(已存在)新增用例,或就近放 `PluginDrivenExternalCatalog` 相关测试目录。断言 WHY(Rule 9):mock/构造一个 `PluginDrivenExternalCatalog`(参 T06c `TestablePluginCatalog`:反射注入 connector + stub buildConnectorSession)使其 `getType()=="max_compute"`,对一个 `engineName=null` 的 `CreateTableInfo` 调 `paddingEngineName` 后断言 `getEngineName()==ENGINE_MAXCOMPUTE`(编码:翻闸后无 ENGINE 的 CREATE TABLE 必须自动补 maxcompute,而非抛错);对显式 `engineName="hive"` 调 `checkEngineWithCatalog` 断言抛 AnalysisException(编码:catalog-engine 一致性校验在 PluginDriven 下仍生效)。helper default 分支:type="jdbc" 时 padding 抛"does not support create table"。
  - **E2E(regression-test)**:`regression-test/suites/external_table_p2/maxcompute/test_max_compute_create_table.groovy` Test1(`:62-71` 无 ENGINE 的 Basic CREATE TABLE)即为天然断言点 —— 翻闸后必须仍 `CREATE TABLE` 成功、`show tables like` 命中、`SHOW CREATE TABLE`(`qt_test1_show_create_table`)回显 engine 为 maxcompute/无报错。无需新增套件,本 fix 的成功标准 = 该既有套件在翻闸态下由 FAIL 转 PASS。可补一条断言:无 ENGINE CREATE TABLE 后 `SHOW CREATE TABLE` 的输出包含 `ENGINE=maxcompute`(对齐 legacy 回显)。

**Open questions**: helper default 分支对 jdbc/es/trino-connector(其 SPI 不支持 CREATE TABLE)应保持现状抛 throw 还是显式更友好报错 —— 建议保持与现状一致(落 does-not-support 分支),待各连接器 full-adopt 时再各自补 · checkEngineWithCatalog 新分支的 AnalysisException 文案:沿用 legacy 'MaxCompute type catalog can only use maxcompute engine' 还是按 connector 声明的 engine 名通用化 —— 倾向通用化(显示 getType() 推导的 engine 名),但需确认无回归测试断言旧文案 · 是否需要把 'max_compute'→'maxcompute' 的映射约定抽到 PluginDrivenExternalCatalog/单一常量,避免与 PluginDrivenExternalTable.getEngine():212 的字面量重复(最小改动下暂不抽,仅加注释引用)

#### 🔎 对抗 critic — verdict: `needs-revision`

**需修正(corrections)**:
- Import placement instruction is wrong and will fail Checkstyle. Step 1 says insert the new import 'between :51 InternalCatalog and :52 maxcompute.MaxComputeExternalCatalog, 按字母序'. Actual lines are 48-53 (off by two), and ASCII-case-sensitive ordering puts uppercase 'PluginDrivenExternalCatalog' (P) BEFORE lowercase sub-package imports 'hive.' / 'iceberg.' / 'maxcompute.' / 'paimon.'. Correct position is immediately after line 49 (org.apache.doris.datasource.InternalCatalog) and BEFORE line 50 (org.apache.doris.datasource.hive.HMSExternalCatalog) — i.e. grouped with the top-level datasource.* classes, not after the sub-packages. The stated placement would put it after hive/iceberg and Checkstyle CustomImportOrder would reject it.
- Line-number anchors throughout are off by two for the import region (design cites :51/:52 for InternalCatalog/MaxComputeExternalCatalog; actual is :49/:52). The method/branch anchors (paddingEngineName 896-918, MC branch 912, checkEngineWithCatalog 376-393, MC branch 390) are accurate; only the import-region anchors drift. Minor but the import-region drift directly produces the wrong-placement error above.

**遗漏(gaps)**:
- E2E assertion is factually wrong and would FAIL even with a correct fix. The design's proposed supplementary assertion 'SHOW CREATE TABLE 输出包含 ENGINE=maxcompute' contradicts actual rendering. SHOW CREATE TABLE renders ENGINE= + table.getEngineTableTypeName() (Env.java:4283-4284), and PluginDrivenExternalTable.getEngineTableTypeName() (PluginDrivenExternalTable.java:232-233) returns TableType.MAX_COMPUTE_EXTERNAL_TABLE.name() == 'MAX_COMPUTE_EXTERNAL_TABLE'. The recorded baseline regression-test/data/.../test_max_compute_create_table.out line 3 confirms 'ENGINE=MAX_COMPUTE_EXTERNAL_TABLE', not 'ENGINE=maxcompute'. The design conflates analysis-time engineName ('maxcompute', used for DDL padding/validation) with display-time getEngineTableTypeName ('MAX_COMPUTE_EXTERNAL_TABLE'). The existing qt_test1_show_create_table already covers the regression correctly; the proposed extra assertion must be dropped.
- UT feasibility detail omitted: both paddingEngineName (line 899) and checkEngineWithCatalog (line 383) re-fetch the catalog via Env.getCurrentEnv().getCatalogMgr().getCatalog(ctlName) by NAME — they ignore any directly-constructed catalog object. The UT plan says 'construct a PluginDrivenExternalCatalog so getType()==max_compute' but never states it must be registered into CatalogMgr (or CatalogMgr mocked) for the by-name lookup to return it. As written the UT would hit the real CatalogMgr and not find the test catalog.
- CTAS path benefits but is unmentioned: validateCreateTableAsSelect (line 926) also calls paddingEngineName, so CTAS into a max_compute PluginDriven catalog is equally broken pre-fix and equally fixed. The design scopes only plain CREATE TABLE and never lists CTAS as a covered scenario or a test target, leaving a verification gap.
- No UT/E2E asserts the checkEngineWithCatalog mirror actually had a behavior change. The design claims the explicit-ENGINE consistency check is 'silently bypassed' pre-fix, but provides no failing-then-passing test that a wrong explicit ENGINE (e.g. ENGINE=hive on a max_compute catalog) is rejected only after the fix. Without it the mirror branch is untested against its WHY (violates Rule 9: the test could pass with the branch absent).

**额外风险**:
- Root-cause analysis, central type-string mapping, and both target sites are otherwise CORRECT and verified: getType() returns lowercase 'max_compute' (CatalogFactory.java:90 toLowerCase + :100 putIfAbsent, :235-239 getType), the same key PluginDrivenExternalTable.getEngine()/getEngineTableTypeName() switch on; ENGINE_MAXCOMPUTE='maxcompute' (:125); and the warning against reusing TableType.MAX_COMPUTE_EXTERNAL_TABLE.toEngineName() is valid — that enum is NOT in the toEngineName() switch (TableIf.java) and returns null. The fix's destination is real: MaxComputeConnectorMetadata.createTable IS implemented (line 283), so padding the engine genuinely reaches a working createTable, not just a deferred failure.
- default-branch behavior for jdbc/es/trino is correctly non-regressive: pre-fix those catalogs already hit the same 'Current catalog does not support create table' throw at line 915, and ConnectorTableOps.createTable default also throws 'CREATE TABLE not supported' (line 66). So the helper's default-throw preserves their status quo — no new breakage, as claimed.
- Follower replay / master sync is NOT a concern for this fix (prompt flagged it): engine padding is analysis-time on the receiving FE; persistence uses logCreateTable edit log (PluginDrivenExternalCatalog.java:279) independent of engineName. No replay change needed.
- Batch-D ordering dependency is real and correctly flagged: P4-batchD-maxcompute-removal-design.md:100 plans to delete both instanceof MaxComputeExternalCatalog branches in CreateTableInfo; if Batch D runs without first landing the PluginDriven branch, no-ENGINE CREATE TABLE is permanently broken. The keep-set / commit-ordering note is warranted. Confirmed UnboundTableSinkCreator (CTAS/INSERT sink) already has PluginDrivenExternalCatalog branches (:68/:108/:149) from T06c — so CreateTableInfo really is the last unwired analysis-time CREATE TABLE gate, supporting the design's scoping.
- Latent fragility (acknowledged by design): two now-parallel switch-on-getType() tables (CreateTableInfo helper + PluginDrivenExternalTable.getEngine/getEngineTableTypeName) must stay in sync if SPI_READY_TYPES keys change. Acceptable given existing code already accepts this risk, but a future jdbc/es full-adopter will require touching both — worth the cross-reference comment the design suggests.

---

### FIX-DDL-REMOTE — DDL 远端名解析 — CREATE/DROP TABLE 用 getRemoteName/getRemoteDbName 再发 connector

- **Problem**: 翻闸到 `PluginDrivenExternalCatalog` 后，对启用了名映射的 catalog（`lower_case_meta_names=true` / `lower_case_database_names=1` 或 `2` / `meta_names_mapping`，使本地展示名 ≠ ODPS 远端真名）执行 `CREATE TABLE` / `DROP TABLE` 时，FE 把**本地名**原样透传给连接器，连接器再原样喂给 ODPS SDK。用户可见症状：
  - `CREATE TABLE`：在错误大小写/映射后的库名下建表，或建到不存在的库报错。
  - `DROP TABLE`：`getTableHandle` 用本地小写/映射名查 ODPS 定位不到真实表 → `IF EXISTS` 静默不删（残表）、非 `IF EXISTS` 误报“表不存在”；极端情况删错对象。
  - 触发条件：catalog 属性开启上述任一名映射，且本地名与远端名不一致。未开映射时本地名==远端名，行为无差异（解释为何 gate/默认 e2e 未暴露）。这是 legacy 可用、翻闸即坏的**数据正确性回归**。

- **Root Cause**:
  - CREATE：`fe/fe-core/.../PluginDrivenExternalCatalog.java:267-268` `CreateTableInfoToConnectorRequestConverter.convert(createTableInfo, createTableInfo.getDbName())` 传**本地** dbName；converter `fe/fe-core/.../connector/ddl/CreateTableInfoToConnectorRequestConverter.java:63-64` 用该 dbName 并直接 `info.getTableName()`（本地表名）。连接器 `fe/fe-connector/.../MaxComputeConnectorMetadata.java:285-286` 把 `request.getDbName()/getTableName()` 原样喂 `structureHelper.tableExist`/`createTableCreator`→ODPS。
  - DROP：`PluginDrivenExternalCatalog.java:357-359` 用本地 `dbName`/`tableName` 调 `metadata.getTableHandle`；连接器 `MaxComputeConnectorMetadata.java:104,346-347` 把本地名原样喂 SDK。
  - Legacy 基线（须 mirror）：`fe/fe-core/.../datasource/maxcompute/MaxComputeMetadataOps.java` — createTableImpl `:179`/`:219` 用 `db.getRemoteName()` 作 dbName（表名保持原始 `createTableInfo.getTableName()`，**legacy CREATE 不对表名做 remote 解析**，因为表尚不存在、无本地→远端映射）；dropTableImpl `:266-267` 用 `dorisTable.getRemoteDbName()`（= `db.getRemoteName()`）与 `dorisTable.getRemoteName()`。
  - 名映射来源：`fe/fe-core/.../ExternalCatalog.java:548-560` buildMetaCache 令 localName≠remoteName；`ExternalDatabase.getRemoteName():407-409`、`ExternalTable.getRemoteName():166-168`、`ExternalTable.getRemoteDbName():535-536`。
  - 注意 createDb/dropDb 不在本 issue 范围：legacy 的实际 SDK 调用对库名也用**原始本地名**（createDbImpl `:122`、dropDbImpl 实删 `:156`），仅 dropDbImpl 的 force 级联枚举用 `getRemoteName()`（属另一发现 DDL-P2）。故本 fix 只动 CREATE TABLE 的 db 名 + DROP TABLE 的 db/table 名。

- **Design**: remote 解析放 **FE（`PluginDrivenExternalCatalog`）**，与现有读路径 `getRemoteName` 用法、与 base `ExternalCatalog.dropTable:1119-1131`（先 `getDbNullable` 再 `db.getTableNullable` 取 dorisTable）一致；**不扩展 SPI**、不改连接器（连接器继续把 handle 里的名字当“已是远端名”原样发 SDK，契约保持“FE 负责 local→remote”）。这是通用插件层缺口（任何 full-adopter 都需），但实现 **keyed on PluginDriven 的通用 `ExternalDatabase`/`ExternalTable` getRemoteName API，非 hardcode maxcompute**。
  - createTable override：解析 db 远端名后传给 converter。最小改动用现有 converter 第二参（`convert(info, dbName)` 注释已写“caller may normalize case”）——
    `ExternalDatabase<? extends ExternalTable> db = getDbNullable(createTableInfo.getDbName());`（db==null 抛 `DdlException("Failed to get database ...")`，mirror legacy `MaxComputeMetadataOps:172-176` 与 base `ExternalCatalog:1120-1122`），随后 `convert(createTableInfo, db.getRemoteName())`。表名保持 converter 内 `info.getTableName()` 原始值（mirror legacy：CREATE 不解析远端表名）。
  - dropTable override：先 `ExternalDatabase db = getDbNullable(dbName)`；db==null 时按 ifExists 干净返回 / 否则抛（mirror base `:1120-1128`、legacy 经 `getTableNullable`）。再 `ExternalTable dorisTable = db.getTableNullable(tableName)`；dorisTable==null 时按 ifExists 返回 / 否则抛（mirror legacy `dropTableImpl` 的“表不存在”分支与 base `:1124-1128`）。然后用 `dorisTable.getRemoteDbName()` 与 `dorisTable.getRemoteName()` 调 `metadata.getTableHandle(session, remoteDb, remoteTbl)`；后续 `metadata.dropTable(handle)` 不变。editlog 与缓存失效仍用**本地** dbName/tableName（mirror base `:1132` 与 legacy `afterDropTable` 用本地名）。
  - 须 mirror 的 legacy 可观察行为：建/删命中正确远端对象；IF EXISTS 在表不存在时静默成功；非 IF EXISTS 抛明确 `DdlException`；editlog/缓存键沿用本地名（保持 follower replay 一致）。
  - 通用性说明：解析仅依赖 `ExternalCatalog.getDbNullable` + `ExternalDatabase.getRemoteName` + `ExternalTable.getRemoteDbName/getRemoteName`，对所有 PluginDriven 连接器一致；未开名映射时 `getRemoteName()` 回落为本地名（`:408`/`:167` 的 `Strings.isNullOrEmpty` 兜底），行为不变。

- **Implementation Plan**（单 issue 独立 commit；仅触 fe-core；编译 `-pl :fe-core -am`）：
  1. [fe-core] `PluginDrivenExternalCatalog.createTable`（:264-287）：在 `convert(...)` 前加 `getDbNullable(createTableInfo.getDbName())` 取 db、null 校验抛 `DdlException`，把第二参由 `createTableInfo.getDbName()` 改为 `db.getRemoteName()`。editlog `org.apache.doris.persist.CreateTableInfo`(:274-278) 与 `getDbForReplay(...).resetMetaCacheNames()`(:283) 维持本地名不变。
  2. [fe-core] `PluginDrivenExternalCatalog.dropTable`（:353-374）：在 `getTableHandle` 前加 `getDbNullable(dbName)` + `db.getTableNullable(tableName)` 解析；db/table 为 null 时按 ifExists 返回否则抛（mirror base 语义，同时附带修 DDL-C7 的库存在校验，但仅为达成正确寻址的必要前置，不扩范围）；`getTableHandle(session, dorisTable.getRemoteDbName(), dorisTable.getRemoteName())`。editlog `DropInfo`(:371) 与 `unregisterTable`(:372) 维持本地名。
  3. [fe-connector-maxcompute] 无改动（连接器契约保持“接收即远端名”）。
  4. [fe-connector-api] 无改动（无需扩 SPI）。
  5. [thrift] / [be] 无改动。
  - import-gate：fe-core 已 import `ExternalDatabase`/`ExternalTable`（同包/已用），无新增第三方 import；如缺则补 `org.apache.doris.datasource.ExternalDatabase`/`ExternalTable`。

- **Risk**:
  - 回归面小且收敛：仅改两个 override 的名解析；未开名映射时 `getRemoteName()==本地名`，行为与现状逐字节一致。
  - DROP override 现状**未做库/表存在性校验**直接 `getTableHandle`（DDL-C7）；本 fix 补上 `getDbNullable` 预检会改变“库不存在”路径的异常类型（由连接器 `OdpsException→RuntimeException` 变为 FE `DdlException`），更贴 base/legacy，属改进；须在 UT 固化该行为防回退。
  - 对其他连接器/插件：纯增益——任何 full-adopter 走 PluginDriven DDL 都会因此正确解析远端名；无破坏（未开映射不变）。
  - keep 集：不删除、不触 legacy `datasource/maxcompute/`（Batch D 才删）；不动连接器 keep 文件。
  - checkstyle/import-gate：仅 fe-core 内既有类型，风险低；按 fe-code-style 跑 Checkstyle。
  - 反例提醒（Batch D 协同）：本 fix 不依赖、也不引入连接器侧 local→remote 解析；Batch D 删 legacy 时勿据“连接器内部解析 remote”这一**已被证伪的** T06c §5:187 假定行事。

- **Test Plan**:
  - UT（fe-core，扩 `fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenExternalCatalogDdlRoutingTest.java`，复用既有 Mockito + `TestablePluginCatalog` stub）：
    - `testCreateTableUsesRemoteDbName`：stub `dbNullableResult.getRemoteName()` 返回与本地名不同的远端名（如 local `db1`→remote `DB1`），`createTable` 后 `Mockito.verify(metadata).createTable(eq(session), argThat(req -> req.getDbName().equals("DB1") && req.getTableName().equals(<本地表名>)))`；断言表名**未**被改写（mirror legacy CREATE 不解析远端表名）。
    - `testCreateTableMissingDbThrows`：`dbNullableResult=null` → 期望 `DdlException`，且 `verifyNoInteractions(metadata.createTable)`。
    - `testDropTableUsesRemoteDbAndTableName`：stub `db.getTableNullable(...)` 返回一个 mock `ExternalTable`，其 `getRemoteDbName()`→`DB1`、`getRemoteName()`→`TBL1`；`dropTable` 后 `verify(metadata).getTableHandle(session, "DB1", "TBL1")`；editlog `logDropTable` 的 `DropInfo` 仍用本地名。
    - `testDropTableIfExistsMissingTableIsNoop` / `testDropTableMissingTableWithoutIfExistsThrows`：覆盖表不存在的 ifExists 语义（mirror legacy）。
  - E2E（regression-test）：在 `regression-test/suites/external_table_p2/maxcompute/`（现有 `test_max_compute_create_table.groovy` 同目录）新增/扩一支，catalog 创建时设 `"lower_case_meta_names"="true"`（或 `lower_case_database_names=1`），断言点：
    - `CREATE TABLE` 后 ODPS 侧真名（混合大小写库）存在、可 `SELECT`；
    - `DROP TABLE IF EXISTS` 命中真实远端表后 `SHOW TABLES` 不再含该表（验证未走“本地名查不到→静默不删→残表”路径）；
    - 对照同套件未开映射场景行为不变。sql 断言聚焦“建/删后的可见性”而非内部名，符合 Rule 9（编码 why：名映射下寻址正确性）。

**Open questions**: E2E 需真实 ODPS 环境且 catalog 开 lower_case_meta_names；若 CI 无真实 MaxCompute，远端名解析正确性只能由 fe-core UT(verify getTableHandle/req.getDbName 收到远端名)兜底，E2E 标记为需 live MC 环境运行。 · DROP override 补 getDbNullable 库存在校验顺带修了 DDL-C7(库不存在时异常类型对齐 base/DdlException);确认是否将 DDL-C7 合并入本 commit(寻址正确性的必要前置)还是仅最小解析、留 DDL-C7 单独修——倾向合并以免 dropTable override 被改两次。 · CREATE TABLE 须先修 DDL-P1(paddingEngineName 只认 MaxComputeExternalCatalog，翻闸后分析期即抛错根本到不了本 override);本 fix 的 CREATE 部分在 DDL-P1 修复前不可达，故 depends_on=DDL-P1。

#### 🔎 对抗 critic — verdict: `needs-revision`

**需修正(corrections)**:
- The design's Risk claim 'DROP override 现状未做库/表存在性校验直接 getTableHandle' is correct, but the framing that adding getDbNullable 'changes the库不存在 exception type from connector OdpsException→RuntimeException to FE DdlException, 更贴 base/legacy, 属改进' is only partly right: for max_compute the connector's getTableHandle does NOT throw on a missing DB — it calls structureHelper.tableExist which returns false → Optional.empty() → current code already throws FE DdlException 'Failed to get table' (line 364), NOT a RuntimeException. So the 'before' behavior described (OdpsException→RuntimeException) is not what actually happens for a missing DB on the drop path today; the improvement is real (clearer 'database' vs 'table' message) but the stated before-state is inaccurate.
- The design asserts CREATE must NOT remote-resolve the table name and cites legacy as authority. Verified correct (MaxComputeMetadataOps.java:219 passes createTableInfo.getTableName() = local literal; only db uses getRemoteName at :219). No correction to the decision itself — but note legacy createTableImpl ALSO does two FE-side existence checks (tableExist on remote db at :179 and getTableNullable at :189) that the plugin createTable override does NOT replicate and the fix does NOT add. The fix only adds the db null-check, leaving the connector to do the existence/IF-NOT-EXISTS check. This is a pre-existing divergence the fix neither closes nor flags; acceptable for scope but should be stated as an explicit non-goal rather than implied parity.

**遗漏(gaps)**:
- EXISTING TESTS WILL BREAK, not just 'extend' — the design's test plan says 扩既有 but omits a mandatory rewrite. In /mnt/disk1/yy/.../PluginDrivenExternalCatalogDdlRoutingTest.java the stub TestablePluginCatalog.getDbNullable returns dbNullableResult which DEFAULTS TO null. After the fix, dropTable calls getDbNullable FIRST, so all 4 existing drop tests (testDropTableResolvesHandleRoutesAndUnregisters:176, testDropTableIfExistsWhenMissingIsNoop:190, testDropTableMissingWithoutIfExistsThrows:200, testDropTableWrapsConnectorException:209) will now throw 'Failed to get database' before ever reaching getTableHandle — they currently stub only getTableHandle, never getDbNullable/getTableNullable. Likewise testCreateTableInvalidatesDbCache:223 stubs only getDbForReplay, not getDbNullable, so the new createTable null-check throws DdlException and the test fails. The plan must explicitly list these 5 tests as REQUIRING rewrite (stub dbNullableResult + db.getTableNullable), otherwise the suite goes red. This is a Rule-12 'fail loud' omission.
- SHARED-OVERRIDE BLAST RADIUS understated. CatalogFactory.java:51-52 SPI_READY_TYPES = {jdbc, es, trino-connector, max_compute}. createTable/dropTable in PluginDrivenExternalCatalog are inherited by ALL FOUR, not just max_compute (verified: EsConnectorMetadata/JdbcConnectorMetadata/TrinoConnectorDorisMetadata do NOT override createTable/dropTable). The design repeatedly says '任何 full-adopter' but never names jdbc/es/trino concretely nor adds a UT proving the resolution is benign for a connector whose createTable throws 'not supported'. For DROP on jdbc/es/trino the new getDbNullable+getTableNullable adds a remote getTableNullable round-trip (ExternalDatabase.getTableNullable can hit the remote system, line 270-302) that the current code path skips — behavior end-state is still a throw so no functional regression, but the added remote call on a code path that previously short-circuited is unflagged.
- DROP path adds a getTableNullable() remote round-trip that the CURRENT plugin dropTable does not make (it goes straight to getTableHandle). This matches base ExternalCatalog.dropTable:1123 and legacy, so it is correct parity — but the design's Risk section claims '回归面小' / '逐字节一致' which is false for the unmapped case too: even WITHOUT name mapping, the fix changes the control flow (extra getDbNullable+getTableNullable resolution + potential remote validation, plus changed exception type for missing-db) for every drop on every PluginDriven catalog. The '逐字节一致' claim only holds for the SDK-bound names, not for the FE-side control flow.
- No coverage for the case where FE resolves the table exists locally but getTableHandle(remoteDb,remoteTbl) returns empty (table dropped out-of-band remotely). The existing handle-absent ifExists/throw branch (line 360-365) is preserved, but the test plan adds no case asserting it still fires AFTER the new getTableNullable resolution succeeds — i.e. a table present in FE cache but absent remotely.
- Line numbers and package paths in the design are stale/wrong (it cites fe-core/.../connector/ddl and MaxComputeMetadataOps line refs that don't all line up; actual converter is org.apache.doris.connector.ddl, CREATE override is at PluginDrivenExternalCatalog.java:263-287 with the local-dbName at :268). Cosmetic, but indicates the design was not re-derived against the current tree before writing the plan.

**额外风险**:
- getDbNullable / getTableNullable on the master can trigger lazy metaCache build / remote round-trips (ExternalDatabase.getTableNullable Step 2-3, lines 270-302) the moment a DDL fires. If the remote (ODPS) is slow/unreachable, CREATE/DROP now blocks on metadata resolution before the SDK call, whereas the current plugin createTable path reaches the converter without that resolution. Minor latency/failure-surface change on the master write path, unmentioned.
- getRemoteDbName() on ExternalTable delegates to db.getRemoteName() (ExternalTable.java:536), and the design resolves db separately via getDbNullable then table via db.getTableNullable. There is a latent assumption that dorisTable.getRemoteDbName() (== its parent db's remoteName) equals the remoteName of the db just fetched via getDbNullable. They should be the same object, but if cache invalidation races between the getDbNullable call and getTableNullable (concurrent refresh), the two could momentarily diverge. Legacy base dropTable has the identical structure so this is not a new risk, but it is unaddressed by any concurrency note.
- The E2E plan proposes lower_case_meta_names=true on a max_compute catalog and asserts post-create visibility. But ODPS project/db naming under name-mapping is environment-specific (mixed-case real DB must already exist on the ODPS side). If the test infra's ODPS project has no mixed-case database, the E2E silently can't exercise the mapping divergence and degenerates to local==remote, giving a green test that does NOT prove the fix (Rule 9 violation). The plan does not specify how the mixed-case remote object is provisioned, so the E2E may not actually fail pre-fix.
- Per Rule 9, the proposed UT 'testCreateTableUsesRemoteDbName' must use a real CreateTableInfoToConnectorRequestConverter (or assert on the dbName actually passed) — but the existing test mocks the converter statically (MockedStatic at line 227). If the new UT keeps mocking the converter, verify(metadata).createTable(argThat(req -> req.getDbName().equals('DB1'))) cannot work because the mocked converter returns a stub req unaffected by the dbName argument. The UT must capture the SECOND argument passed to convert() (the dbName) via the static-mock invocation, not the resulting request's getDbName(). The test-plan wording 'argThat(req -> req.getDbName()...)' is unimplementable against the existing mocking style and would either not compile against intent or pass vacuously.

---

### 阶段 3 — 恢复分区可见 (partitions TVF / SHOW PARTITIONS)

### FIX-PART-GATES — partitions() TVF + SHOW PARTITIONS analyze 网关 + 分区元数据 override

**Scope**: review 发现 DDL-C1 / CACHE-C1 / CACHE-C2(severity major,regression=yes,对抗存活 3✓/0✗ ×3),含 ⚠️ Batch-D 红线。本 section 只设计、不写码。

#### Problem
翻闸(cutover)后 MaxCompute catalog 变成 `PluginDrivenExternalCatalog`、其表是 `PLUGIN_EXTERNAL_TABLE`。对一张真实分区的 MC 表执行两条用户命令在 FE **analyze 阶段直接抛错**,legacy 可用、翻闸即坏:
- `SELECT * FROM partitions('catalog'='mc','database'='d','table'='t')` → 抛 `AnalysisException("Catalog of type 'max_compute' is not allowed in ShowPartitionsStmt")`(若补了 catalog 网关,下一步又因表类型不在 allow-list 抛 `MetaNotFound`)。
- `SHOW PARTITIONS FROM <mc 分区表>` → 抛 `Table X is not a partitioned table`。

触发条件:翻闸后(`SPI_READY_TYPES` 含 max_compute、CatalogFactory 走 PluginDriven)对任意真实分区的 MC 表跑上述两命令。两条命令的 BE 取数支路 / dispatch / handler 都已由 T06c 接好,但因 analyze 网关挡在前面,这些 handler 是**不可达死代码**(`MetadataGenerator.dealPluginDrivenCatalog`、`ShowPartitionsCommand.handleShowPluginDrivenTablePartitions`)。

#### Root Cause
三个独立缺口,均为 T06c "FE 分发接线" 漏接 analyze 网关:

1. **DDL-C1 / CACHE-C1(partitions() TVF 双重网关)** — `fe/fe-core/.../tablefunction/PartitionsTableValuedFunction.java:172-176` 的 catalog allow-list 只认 `internal / HMSExternalCatalog / MaxComputeExternalCatalog`,无 `PluginDrivenExternalCatalog`;`:184-185` 的 `getTableOrMetaException(...)` 允许类型只到 `OLAP/HMS_EXTERNAL_TABLE/MAX_COMPUTE_EXTERNAL_TABLE`,无 `PLUGIN_EXTERNAL_TABLE`。构造器 `:149` 即 eager `analyze()`,故双重挡死。已接好的 BE handler `MetadataGenerator.java:1317-1318`(dispatch)+`:1359-1377`(`dealPluginDrivenCatalog`,走 SPI + remote 名解析)永不可达。
   - 注:历史(commit 2cf7dfa81ad ③ / HANDOFF:42,61 / Batch-D 设计:72)声称 T06c 已给本文件加 PluginDriven 分支 —— **证伪**,本文件 git 全文无 `PluginDrivenExternalCatalog`,T06c 只改了 `MetadataGenerator.java`。

2. **CACHE-C2(SHOW PARTITIONS 的 isPartitionedTable 门)** — `fe/fe-core/.../commands/ShowPartitionsCommand.java:263-266`,对非 internal catalog 调 `table.isPartitionedTable()`,默认实现 `TableIf.java:364-366` 返 `false`。T06c 已接 allow-list(:208)、表类型(:261)、handler(:312)、dispatch(:460-461),唯独 `isPartitionedTable()` 门未过 —— `PluginDrivenExternalTable.java` 全类无此 override(已逐行读 52-260 确认),故真实分区 MC 表在 `:265` 先抛 "is not a partitioned table"。T06c 设计 §4.3:162 自己把 `isPartitionedTable` 标"验证项"却未落实。

3. **根因汇聚于一处缺失** — `PluginDrivenExternalTable` 缺分区元数据 override(`isPartitionedTable` / `getPartitionColumns`,以及 supportInternalPartitionPruned/getNameToPartitionItems 见 Risk 边界说明)。legacy `MaxComputeExternalTable.java:331-335`(`isPartitionedTable=getOdpsTable().isPartitioned()`)、`:88-97`(`getPartitionColumns`)是要 mirror 的可观察行为基线。

#### Design
通用插件层缺口(非 MC 专有,任何有分区的 full-adopter 连接器都触发),修法 **keyed on PluginDriven / connector 声明,不 hardcode maxcompute**。连接器 SPI 已足够,**无需扩展 thrift / fe-connector-api**:
- 连接器在 `getTableSchema()` 的 `ConnectorTableSchema` props 里写 `partition_columns`(`MaxComputeConnectorMetadata.java:149-153`,`ConnectorTableSchema.getProperties()`);分区名/项已有 `listPartitionNames/listPartitions/listPartitionValues`(`ConnectorTableOps.java:158/169/181`,default `emptyList()` → 非分区连接器优雅返 0 行)。

A. **`PluginDrivenExternalTable` 新增分区元数据 override(fe-core,核心修复)**
- `@Override public boolean isPartitionedTable()` —— 经 connector 声明判定:`makeSureInitialized()` 后读 `getTableSchema` 暴露的 `partition_columns` prop(等价:`!getPartitionColumns().isEmpty()`),非空即 partitioned。mirror legacy `isPartitionedTable()=odpsTable.isPartitioned()`。
- `@Override public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot)` —— 返回 schema 里被标为分区列的 `Column`。数据源:connector 已在 `initSchema()`(`PluginDrivenExternalTable.java:78-109`)把分区列也并入 columns;分区列名取自 `ConnectorTableSchema` 的 `partition_columns` prop。最小实现:用该 prop 的列名集合从 `getFullSchema()` 过滤出分区列(保持 ConnectorColumnConverter 已转好的 Doris `Column`,避免重复转换)。mirror legacy `getPartitionColumns()`。
- 不在 SPI 层硬编码 MC:判定一律走 `ConnectorTableSchema` props,任何 full-adopter 复用。
- 这一处同时打通 SHOW PARTITIONS 的 `isPartitionedTable` 门(CACHE-C2)与 TVF 的"是否分区表"语义。

B. **`PartitionsTableValuedFunction.analyze()` 双网关补 PluginDriven 分支(fe-core,DDL-C1/CACHE-C1)**
- catalog allow-list `:172-176`:追加 `|| catalog instanceof PluginDrivenExternalCatalog`(**新增分支,不动既有 MaxCompute 分支** —— Batch-D 红线)。
- 表类型 `getTableOrMetaException` `:184-185`:追加 `TableType.PLUGIN_EXTERNAL_TABLE`。
- "非分区表"守卫:在现有 `if (table instanceof MaxComputeExternalTable)`(`:200-204`,检查 `getOdpsTable().getPartitions().isEmpty()`)**旁加**一个 `else if (table instanceof PluginDrivenExternalTable && !table.isPartitionedTable())` → 抛 "Table X is not a partitioned table",mirror legacy MC 对空分区表的可观察行为。依赖 A 的 `isPartitionedTable()`。
- 新增 import `org.apache.doris.datasource.PluginDrivenExternalCatalog`、`PluginDrivenExternalTable`。

C. **SHOW PARTITIONS 侧无需改 ShowPartitionsCommand.java** —— allow-list/表类型/dispatch/handler 已由 T06c 接好;`:263-266` 的 `isPartitionedTable()` 门由 A 的 override 自然放行。零改动该文件即修复 CACHE-C2。

D. **Batch-D 红线(显式推翻历史前提,不删码)** — Batch-D 设计 `:70-77,:102` 的 amendment 假设"T06c 已在 `PartitionsTableValuedFunction` 加 PluginDriven 分支",前提**错误**:本 fix 落地前文件根本无该分支。本 fix(B)使该假设**首次成真**。Batch-D 执行删 `:173` 的 MaxCompute 分支,**必须在 B 已 merge、确认 PluginDriven 分支存在后**进行;否则会删掉唯一放行分支、永久坐实 partitions() 对 MC 不可用。设计须在 decisions/Batch-D 文档显式标注此 ordering 依赖(更新 D-028 / Batch-D amendment 措辞由"T06c adds"改为"FIX-PART-GATES adds")。

#### Implementation Plan
逐文件逐方法(每条标层)。约束:每 issue 独立 commit;改 fe-core 带 `-pl :fe-core -am`;不改连接器(connector 已就绪)。建议拆 2 commit:
1. **commit ①(fe-core)**:`PluginDrivenExternalTable` override + TVF 网关。
   - `[fe-core]` `fe/fe-core/.../datasource/PluginDrivenExternalTable.java`:新增 `isPartitionedTable()`、`getPartitionColumns(Optional<MvccSnapshot>)` 两个 override(读 `ConnectorTableSchema` props 的 `partition_columns`,keyed on connector 声明)。
   - `[fe-core]` `fe/fe-core/.../tablefunction/PartitionsTableValuedFunction.java`:`:172-176` catalog allow-list 加 `|| instanceof PluginDrivenExternalCatalog`;`:184-185` 加 `TableType.PLUGIN_EXTERNAL_TABLE`;`:200-204` 旁加 PluginDriven 非分区守卫;补 2 import。
2. **commit ②(docs)**:更新 `plan-doc/tasks/designs/P4-batchD-maxcompute-removal-design.md:70-77,102` amendment 措辞 + decisions-log D-028,标注 Batch-D 删 `:173` 须排在本 fix 之后(Batch-D 红线)。
- **不涉及**层:fe-connector-maxcompute(connector 已 expose partition_columns/listPartition*,零改)、fe-connector-api(SPI 充分,零改)、be、thrift。
- 守门:`isPartitionedTable` 等 override 须 `makeSureInitialized()` 后取 schema;checkstyle 扫 test 源同样适用。

#### Risk
- **回归风险(低-中)**:`getPartitionColumns` 若返回值与 legacy `MaxComputeSchemaCacheValue.getPartitionColumns()` 顺序/类型不一致,DESCRIBE / SHOW PARTITIONS 列名展示会偏。须以 legacy 输出为基线核对(connector `initSchema` 已按 partition columns 追加,顺序应一致)。
- **对其他连接器/插件影响(正向)**:override keyed on `partition_columns` prop —— 不声明分区列的连接器(JDBC/ES)`isPartitionedTable()` 仍返 false、`getPartitionColumns()` 返空,行为不变;SHOW PARTITIONS 对其继续抛 "not a partitioned table"(与 legacy 一致)。无连接器特判。
- **keep 集 / Batch-D**:本 fix **新增** PluginDriven 分支,**不触碰** `PartitionsTableValuedFunction:173` 的 MaxComputeExternalCatalog keep 分支(翻闸后仍可能有遗留 MC 表/Batch-D 未跑)。是修复 Batch-D 红线前提的必要前置。
- **边界 / 已知降级(fail loud,需显式登记,非本 section 修)**:本 fix 只恢复 `isPartitionedTable`/`getPartitionColumns`(满足 SHOW PARTITIONS + partitions() TVF 显示)。**未** override `supportInternalPartitionPruned()` / `getNameToPartitionItems()` → FE 侧内部分区裁剪(legacy 有,带 partition_values 二级 cache)仍缺失,即 review 独立发现 READ-P3 / CACHE-C-SELECT / CACHE-P1(分区裁剪丢失 → 整表扫)。须在 deviations-log 显式记为已知降级,勿在本 fix 误标"分区能力已全恢复"。若决策要求一并恢复裁剪,则追加 `supportInternalPartitionPruned()=true`(经 connector capability) + `getNameToPartitionItems()`(经 `listPartitions` 构 `PartitionItem`),属更大改动,单独评估。
- **import-gate / checkstyle**:仅加标准 doris import,无新依赖。

#### Test Plan
- **UT(fe-core,`-pl :fe-core -am`)**:
  - 新增/扩展 `fe/fe-core/src/test/.../datasource/PluginDrivenExternalTableEngineTest.java`(或同包新建):构造一张 connector 声明 `partition_columns` 的 PluginDriven 表,断言 `isPartitionedTable()==true`、`getPartitionColumns()` 非空且列名匹配;再构造无分区列的表,断言 `false`/空 → 锁住 keyed-on-connector 语义(Rule 9:测的是"为何分区表必须放行",非仅 handler 形状)。
  - **扩展 `ShowPartitionsCommandPluginDrivenTest.java`**:现有 testHandlerRoutesToSpiWithRemoteNames 用反射**直调 handler、跳过了 analyze 网关**(正是 CACHE-C2 逃逸的原因)。须新增一条**驱动 `analyze()`/validate gate** 的用例,在分区表上断言不抛 "not a partitioned table"、在非分区表上断言抛 —— 让该 UT 能在 `isPartitionedTable` 回归时失败。
  - 新增 `PartitionsTableValuedFunctionPluginDrivenTest`(或扩展 `MetadataGeneratorPluginDrivenTest.java`):断言 PluginDriven catalog + PLUGIN_EXTERNAL_TABLE 通过 `analyze()` 双网关(不抛 "not allowed" / MetaNotFound),且空分区表抛 "not a partitioned table"。
- **E2E(regression-test/suites,p2 真实 ODPS)**:这些套件翻闸后跑在 PluginDriven catalog 上,本 fix 让它们恢复绿:
  - `regression-test/suites/external_table_p2/maxcompute/test_external_catalog_maxcompute.groovy:395/428/437`(`show partitions from multi_partitions / other_db_mc_parts / mc_parts`)—— 断言分区行非空、与 `.out` 基线一致。
  - `regression-test/suites/external_table_p2/maxcompute/test_max_compute_schema.groovy:127/128`(`show partitions from default.order_detail / analytics.web_log`)。
  - `regression-test/suites/external_table_p2/maxcompute/test_max_compute_partition_prune.groovy:69-71`(`show partitions one/two/three_partition_tb`)。
  - **新增 partitions() TVF 断言点**:在上述某 MC 分区表套件加 `order_qt_partitions_tvf """ SELECT * FROM partitions('catalog'=...,'database'=...,'table'=<分区表>) """`,断言返回分区名集合(覆盖 DDL-C1/CACHE-C1,现有套件无 TVF 用例)。
  - 断言点统一:行数 > 0、分区名格式 `k=v[/k2=v2]`、排序稳定(用 `order_qt_`)。

**Open questions**: 分区裁剪(supportInternalPartitionPruned/getNameToPartitionItems + partition_values 二级 cache)是否要在本 fix 一并恢复,还是仅做 isPartitionedTable/getPartitionColumns 最小修、把裁剪丢失作为已知降级登记 deviations-log(对应独立发现 READ-P3/CACHE-C-SELECT/CACHE-P1)? 建议本 section 只做最小修,裁剪另起。 · getPartitionColumns 的实现是直接从 getFullSchema() 按 partition_columns prop 名集过滤(复用已转 Column),还是要求 connector 在 ConnectorTableSchema 显式标记每列 isPartition? 现状 prop 只给逗号分隔列名,过滤可行;若日后多连接器需更强契约,可议是否给 ConnectorColumn 加 isPartition 标志(SPI 扩展,本 fix 不做)。 · partitions() TVF 对空分区/非分区 PluginDriven 表的报错文案是否需与 legacy MC 完全逐字一致('Table X is not a partitioned table'),还是允许沿用通用文案? 影响是否需在 TVF 单独加守卫(B 已按 mirror legacy 设计加)。 · Batch-D 删除 PartitionsTableValuedFunction:173 MaxCompute 分支的 ordering:本 fix 必须先 merge;需确认 Batch-D 文档/decisions-log 已据此更新 amendment 措辞,否则红线仍在。

#### 🔎 对抗 critic — verdict: `needs-revision`

**需修正(corrections)**:
- Design section A's data-source description is internally contradictory and partly wrong: 'connector 已在 initSchema() 把分区列也并入 columns;分区列名取自 ConnectorTableSchema 的 partition_columns prop。最小实现:用该 prop 的列名集合从 getFullSchema() 过滤'. getFullSchema() does include the partition Columns (connector appends them at :141, mirrored by legacy at :196), BUT the prop needed to identify which columns are partition columns is not available from getFullSchema() nor from the cache. The 'equivalent: !getPartitionColumns().isEmpty()' phrasing for isPartitionedTable() is circular if getPartitionColumns itself depends on the prop. Correct the design to specify the exact prop-sourcing mechanism (re-fetch vs. cache-subclass).
- The Batch-D red-line (part D) is correct AND the existing Batch-D doc is factually wrong as the design states: the amendment at P4-batchD-maxcompute-removal-design.md:70-77 asserts 'P4-T06c adds a PluginDrivenExternalCatalog branch' for PartitionsTableValuedFunction — verified FALSE (the file contains no PluginDrivenExternalCatalog reference at all). The design's instruction to reword 'T06c adds' -> 'FIX-PART-GATES adds' and to gate the :173 MC-branch deletion behind this fix is a valid and necessary correction. No error here; this part is sound.

**遗漏(gaps)**:
- PROP-SOURCING (load-bearing, unaddressed): The design's getPartitionColumns()/isPartitionedTable() both depend on the connector's `partition_columns` prop, but that prop is NOT persisted anywhere reachable at call time. Verified: PluginDrivenExternalTable.initSchema():108 stores `new SchemaCacheValue(columns)` (base class), and base SchemaCacheValue.java only holds `List<Column> schema` (no properties field). There is NO PluginDriven SchemaCacheValue subclass (grep confirms only Iceberg/Paimon/HMS/MaxCompute subclasses exist). ConnectorColumnConverter.convertColumn():67 drops all partition-key markers (it only carries isKey, and MaxComputeConnectorMetadata:141-146 builds partition ConnectorColumns with isKey=false). Therefore the design's stated 'minimal impl: filter getFullSchema() by the prop's name set' is impossible as written — getFullSchema() returns only Columns with no way to identify partition columns, and the prop is not in the cache. The override MUST either (a) re-call metadata.getTableSchema() via the connector SPI on every isPartitionedTable()/getPartitionColumns() call (a remote ODPS metadata round-trip), or (b) introduce a PluginDrivenSchemaCacheValue subclass that persists partition_columns and have initSchema() populate it. The design picks neither and the two design bullets contradict (one says 'read getTableSchema-exposed prop' = re-fetch; the other says 'filter getFullSchema()' = impossible). This must be resolved before implementation.
- PERF/BEHAVIOR DEVIATION not flagged: if the chosen sourcing is per-call getTableSchema() re-fetch (the only option without a new cache subclass), then isPartitionedTable() — called inside ShowPartitionsCommand.validate() at :264 and potentially in planner partition paths — issues a live remote metadata fetch each time, whereas legacy MaxComputeExternalTable.getPartitionColumns():92-97 reads from the cached MaxComputeSchemaCacheValue. Design does not register this as a deviation.
- TEST INFEASIBILITY for the proposed UT not acknowledged: the new 'assert isPartitionedTable()==true' test on PluginDrivenExternalTable requires stubbing the connector's getTableSchema() to return the partition_columns prop AND ensuring the schema-cache/init path is reachable (the existing PluginDrivenExternalTableEngineTest helper never triggers initSchema(); it only exercises getEngine/getEngineTableTypeName which don't touch schema). The test plan doesn't state how the prop is fed to the table under test, which is non-trivial given the prop is not cached.
- COLUMN-NAME MAPPING mismatch for the generalized claim: initSchema():98-105 remaps column names via metadata.fromRemoteColumnName() (e.g., JDBC lowercases), but MaxComputeConnectorMetadata writes the RAW remote partition names into the `partition_columns` prop at :140 BEFORE any mapping. So 'filter getFullSchema() (mapped names) by prop names (raw names)' breaks for any connector that remaps identifiers. MC itself does NOT override fromRemoteColumnName (verified — default returns name unchanged), so MC works today, but the design's central 'keyed on connector, any full-adopter reuses' claim is unsound for remapping connectors and must be either narrowed or fixed by mapping the prop names through fromRemoteColumnName.
- partition_values() TVF gate not mentioned: PartitionValuesTableValuedFunction.java:114-115 has the identical missing-PluginDriven catalog gate and :127 lacks PLUGIN_EXTERNAL_TABLE, and Batch-D's delete list includes its MC branch (~:115). The design scopes out partition_values entirely. This is DEFENSIBLE (verified :132-134 only ever supported HMS tables — 'Currently only support hive table's partition values meta table' — so MC tables always hit that throw even in legacy, meaning no regression and no Batch-D red-line equivalent), but the design should explicitly note it distinguished this case rather than silently omitting a file in the same Batch-D delete list.

**额外风险**:
- Ordering fragility beyond Batch-D: getPartitionColumns() correctness relies on the prop's comma-separated order matching the schema-append order. Verified this holds today (MaxComputeConnectorMetadata:137-147 builds partitionColumnNames and appends columns in the same loop; legacy MaxComputeExternalTable.initSchema:181-197 appends in odpsTable partition-column order). But if a connector ever builds the prop and the column list in different orders, getPartitionColumns() would silently misorder — there's no invariant enforcing this. Worth a guard/assert or a doc note in the SPI contract for partition_columns.
- isPartitionedTable() is a TableIf default returning false and is consumed in more places than SHOW PARTITIONS (planner, DESCRIBE, partition-prune entry checks). Flipping it to true for MC PluginDriven tables WITHOUT also overriding supportInternalPartitionPruned()/getNameToPartitionItems() (which the design explicitly defers) can produce an inconsistent state: a table that reports isPartitionedTable()==true but supportInternalPartitionPruned()==false and getNameToPartitionItems()=={} (the ExternalTable defaults at :458/:478). Verified ExternalTable.initSchemaAndPartitionPrune-style logic uses these together (PartitionValuesTableValuedFunction/ExternalTable:440-446 gate on supportInternalPartitionPruned + getPartitionColumns + getNameToPartitionItems). The design registers the pruning loss as a known degradation, but should also verify no code path assumes isPartitionedTable()==true IMPLIES non-empty getNameToPartitionItems(), which could NPE/empty-prune-to-full-scan inconsistently rather than cleanly.
- follower/replay and gsonPostProcess: PluginDrivenExternalTable.gsonPostProcess():157-165 only fixes table type; the new overrides read live connector schema, so on a follower FE (or after replay) the first isPartitionedTable() triggers connector init. If the connector/session isn't ready on a follower at validate() time this could throw where legacy (cache-backed) did not. Not analyzed by the design; worth confirming follower behavior for the re-fetch path.
- The new 'non-partitioned guard' in PartitionsTableValuedFunction (design B: `else if (table instanceof PluginDrivenExternalTable && !table.isPartitionedTable())`) will, under per-call re-fetch sourcing, perform a remote getTableSchema() during TVF analyze for every partitions() call on a PluginDriven table — including non-MC connectors that declared no partition_columns (they'll re-fetch, get empty, and throw 'not a partitioned table'). Behavior is correct but adds a remote call to the analyze hot path that legacy MC avoided (legacy used cached getOdpsTable().getPartitions()).

---

### 阶段 4 — 写回正确性 (affected rows)

### FIX-WRITE-ROWS — INSERT affected rows 恒 0 — doBeforeCommit 补 loadedRows=getUpdateCnt()

- **Problem**: 翻闸(SPI 事务模型,当前唯一 adopter = MaxCompute)后,对 PluginDriven 外表执行 `INSERT INTO ...` 数据被正确写入,但客户端返回 / `SHOW INSERT RESULT` / `fe.audit.log` 的 returnRows 恒为 `affected rows: 0`。触发条件:catalog 走 SPI 事务模型(`writeOps.usesConnectorTransaction()==true`,即 `connectorTx != null`)的任意 INSERT。JDBC / auto-commit handle 模型(`connectorTx==null`)不受影响。属可观察输出回归(数据不丢,但行数判读错误,影响用户、审计、上层工具)。

- **Root Cause**: 精确定位
  - `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/insert/PluginDrivenInsertExecutor.java:146-150` —— `doBeforeCommit()` 只在 `insertHandle != null` 时调 `writeOps.finishInsert(...)`。事务模型下 `insertHandle` 恒为 null(handle 仅在 `beforeExec()` 的 JDBC 分支创建,而事务模型在 `:109-113` 早退),整段被跳过,`loadedRows` 永不赋值。
  - `loadedRows` 字段定义于 `AbstractInsertExecutor.java:69`(`protected long loadedRows = 0;`)。事务模型下,BE 的 MaxCompute sink 只通过 `TMCCommitData.row_count` 上报行数,从不更新 `num_rows_load_success`(DPP_NORMAL_ALL),故 `AbstractInsertExecutor.java:221-222` 取回 0,`loadedRows` 停在默认 0。
  - 下游 `BaseExternalTableInsertExecutor.java:197/201/203` 用 `loadedRows` 设 `setOk` / `setOrUpdateInsertResult` / `updateReturnRows` → 全部为 0。
  - legacy 基线 `MCInsertExecutor.java:74-78`:`doBeforeCommit()` 在 `finishInsert()` 之外还有 load-bearing 的一行 `loadedRows = transaction.getUpdateCnt();`。翻闸 restructure 只镜像了 `finishInsert` 的等价物(`connectorTx.commit` 经 txn manager),漏镜像 `loadedRows` 赋值。
  - 历史误判:`plan-doc/tasks/designs/P4-T05-T06-cutover-design.md:114`(W-c / gap G2)称 `doBeforeCommit ... → null for MC ⇒ correctly skipped`,把"跳过 doBeforeCommit"当作正确——本设计显式推翻该结论(见 Risk)。

- **Design**: 在 `PluginDrivenInsertExecutor.doBeforeCommit()` 的事务模型分支回填 `loadedRows`,镜像 legacy 可观察行为。
  - 不扩展任何 SPI:`getUpdateCnt()` 全链路已实现且仅差调用方 —— `ConnectorTransaction.getUpdateCnt()`(default,`fe-connector-api/.../ConnectorTransaction.java:96`)→ `MaxComputeConnectorTransaction.getUpdateCnt()`(`fe-connector-maxcompute/.../MaxComputeConnectorTransaction.java:158-160`,= `sum(TMCCommitData.getRowCount())`)→ 经 `PluginDrivenTransaction.getUpdateCnt()`(`PluginDrivenTransactionManager.java:183-184`)暴露 → `Transaction.getUpdateCnt()`(`Transaction.java:65` default)。`transactionManager.getTransaction(long)` 已声明 `throws UserException`(`TransactionManager.java:30`),与 `doBeforeCommit()` 现有签名 `throws UserException` 兼容。
  - 通用插件层修法,keyed on `connectorTx != null`(SPI 事务模型),非 hardcode maxcompute —— 任何未来事务模型 connector 自动受益;`connectorTx == null` 的 JDBC/auto-commit 路径保持原状(沿用 coordinator/DPP_NORMAL_ALL 取到的 `loadedRows`,与 legacy JdbcInsertExecutor 一致,不回填)。
  - 镜像 legacy 的 mirror 方式:legacy 用 `(MCTransaction) transactionManager.getTransaction(txnId)` 取 txn 再 `getUpdateCnt()`;翻闸已持有 `connectorTx` 字段且 `txnId == connectorTx.getTransactionId()`。两种等价取法:(a) 直接 `connectorTx.getUpdateCnt()`(`connectorTx` 是 executor 现有字段,最少耦合,无需 throws/lookup);(b) `transactionManager.getTransaction(txnId).getUpdateCnt()`(与 legacy 取法逐字一致,但引入 `throws UserException` 的 lookup)。推荐 (a):`connectorTx` 已在手、语义等价、不引入可失败的 manager lookup,改动最小;最终值与 legacy 一致(同一 `TMCCommitData.row_count` 累加链)。
  - 现有 `if (writeOps != null && insertHandle != null)` 的 `finishInsert` 分支不动(JDBC handle 模型仍需);新增逻辑作为事务模型独立分支。

- **Implementation Plan**: 逐文件逐方法
  - [fe-core] `fe/fe-core/.../insert/PluginDrivenInsertExecutor.java` `doBeforeCommit()`(:146-150):在现有 `finishInsert` guard 之外,新增事务模型回填分支 —— `if (connectorTx != null) { loadedRows = connectorTx.getUpdateCnt(); }`。两分支互斥(`connectorTx != null` ⇔ `insertHandle == null`),顺序无关;`loadedRows` 继承自 `AbstractInsertExecutor`(可直接赋值)。无新增 import(`ConnectorTransaction` 已 import 于 :30)。
  - 不改 fe-connector-maxcompute / fe-connector-api / be / thrift —— `getUpdateCnt()` 链路全已就绪,本 issue 纯 fe-core 一处赋值。

- **Risk**:
  - 回归风险:极低。仅在 `connectorTx != null` 分支新增一次纯读取赋值;`getUpdateCnt()` 是无副作用的累加器读取(`commitDataList` 求和),在 `doBeforeCommit()`(commit 前、BE 回传 commitData 之后)调用时点正确,与 legacy 一致。`connectorTx == null` 的 JDBC/ES 路径字节级不变。
  - 对其他连接器/插件影响:正向。修法 keyed on `connectorTx`,任何事务模型 connector 通用;非事务模型不触达。无 hardcode maxcompute。
  - keep 集:本改动在翻闸侧 `PluginDrivenInsertExecutor`(SPI 路线 keep),不触碰 legacy `MCInsertExecutor`/`MCTransaction`(removal 集,batchD 将删)。需推翻历史决策:`P4-T05-T06-cutover-design.md:114` 的 "doBeforeCommit ... correctly skipped" 结论 —— 本设计显式标注该结论错误(它只覆盖"能否写成功",漏了"写成功后报告的行数",`loadedRows` 是独立于 G1–G5 的被遗漏 gap)。建议在 deviations-log / decisions-log 补一条更正记录(文档侧,非本 commit 代码范围)。
  - checkstyle / import-gate:无新 import,无 wildcard,单行赋值符合既有风格;不引入跨模块依赖(`connectorTx.getUpdateCnt()` 走已 import 的 SPI 接口)。

- **Test Plan**:
  - UT(放 fe-core):扩 `fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/insert/PluginDrivenInsertExecutorTest.java`。复用现成 `newUnconstructedExecutor()`(Mockito CALLS_REAL_METHODS + Objenesis)+ `Deencapsulation` 注字段的范式。在内部 `StubConnectorTransaction` 加一个可返回固定行数的 `getUpdateCnt()` override(覆盖 SPI default)。新增 `doBeforeCommitBackfillsLoadedRowsFromUpdateCnt`:注入 `connectorTx = StubConnectorTransaction(returns N)`,调 `exec.doBeforeCommit()`,断言 `Deencapsulation.getField(exec, "loadedRows") == N`(编码 WHY:事务模型下 affected rows 必须取自 connector txn 的 getUpdateCnt,而非默认 0)。可补一条 `doBeforeCommitLeavesLoadedRowsForHandleModel`:`connectorTx == null` + 预置 `loadedRows`,断言 `doBeforeCommit()` 不覆盖(JDBC 路径不回填)。该 UT 不需 fe-core 之外依赖。
  - E2E:沿用 `regression-test/suites/external_table_p2/maxcompute/write/test_mc_write_insert.groovy`(gated by `enableMaxComputeTest`)。当前仅用 `order_qt_*` 验数据,无 affected-rows 断言。在 Test 1 的 `INSERT INTO ${tb1} VALUES (...3 行...)` 后捕获 affected rows(如 `def res = sql "INSERT ..."; assertEquals(3, res[0][0])` 或检查 SHOW INSERT RESULT / returnRows),断言点 = 写入行数 N 而非 0;并对 `INSERT ... SELECT`(Test 2)同样断言 N>0。断言点直击本回归:数据写对(order_qt 已保证)且行数报告正确。
  - 守门:改 fe-core 带 `-pl :fe-core -am`;本 issue 独立 commit(只动 `PluginDrivenInsertExecutor.java` + 该 UT)。

**Open questions**: 回填取法二选一:推荐 (a) connectorTx.getUpdateCnt()(字段已在手、无 throws、最小改动);(b) transactionManager.getTransaction(txnId).getUpdateCnt() 与 legacy 取法逐字一致但引入 UserException lookup。两者最终值等价,需 owner 拍板风格偏好。 · 是否同步补 deviations-log/decisions-log 一条更正,推翻 P4-T05-T06-cutover-design.md:114 'doBeforeCommit correctly skipped' 的历史结论(文档侧,非本代码 commit 范围)。 · E2E affected-rows 断言的具体取值方式(sql 返回的 res[0][0] vs SHOW INSERT RESULT vs returnRows)需按 regression 框架对 external INSERT 的实际返回形态确认;gated by enableMaxComputeTest,需真 MC 环境跑。

#### 🔎 对抗 critic — verdict: `sound`

**需修正(corrections)**:
- Minor imprecision in the Root Cause's claim that legacy 'doBeforeCommit() 在 finishInsert() 之外还有一行 loadedRows = transaction.getUpdateCnt()'. Verified the ORDER in legacy MCInsertExecutor.java:75-78 is: getTransaction -> `loadedRows = transaction.getUpdateCnt()` (line 76) THEN `transaction.finishInsert()` (line 77). i.e. legacy reads the count BEFORE finishInsert commits. The fix's recommended approach (a) reads `connectorTx.getUpdateCnt()` in doBeforeCommit BEFORE PluginDrivenTransactionManager.commit() (called later in onComplete:105). Order is preserved — but note getUpdateCnt() reads commitDataList which is independent of commit(), so order is immaterial here; the design's 'order 无关' claim is correct. No behavioral error, just confirming the mirror is faithful.
- The design says approach (b) `transactionManager.getTransaction(txnId).getUpdateCnt()` is 'with legacy 取法逐字一致'. Not quite literal: legacy casts to `(MCTransaction)` and calls the concrete getUpdateCnt; the SPI path returns a `PluginDrivenTransaction` whose getUpdateCnt delegates to connectorTx (PluginDrivenTransactionManager.java:183-184). Semantically equivalent, but (b) is NOT a byte-for-byte mirror. This does not affect the recommendation — (a) is the right choice and the design picks it — but the '逐字一致' characterization of (b) is slightly overstated.

**遗漏(gaps)**:
- E2E affected-rows capture shape unverified. The design proposes `def res = sql "INSERT ..."; assertEquals(3, res[0][0])`, but no existing external-table write suite (hive/iceberg/mc) uses this pattern — they all verify via `order_qt_*` on a follow-up SELECT. Whether the Doris regression `sql` helper returns INSERT affected-rows as `res[0][0]` (vs. needing `SHOW INSERT RESULT` / a JDBC updateCount path) is unconfirmed in-repo; implementation should pin the exact accessor before claiming the E2E asserts the regression. This is a test-mechanics gap, not a design flaw.
- Multi-statement / multi-fragment accumulation not explicitly covered by the proposed UT. The real value comes from summing N `TMCCommitData.row_count` fed by multiple BE fragment reports; the proposed StubConnectorTransaction returns a single fixed N, so it does not exercise the `commitDataList.stream().sum()` accumulation path (that lives in MaxComputeConnectorTransaction, in fe-connector-maxcompute, which the fe-core UT cannot reach). E2E Test 4 (multi-batch, 3 separate INSERTs of 1 row each) is the only place real accumulation across the feed path is exercised, and the design only proposes asserting Test 1/Test 2 — adding an affected-rows assertion to Test 4 would close this.
- Design does not state whether `filteredRows` should also be backfilled. It correctly mirrors legacy MCInsertExecutor (which only sets loadedRows), and MC never populates DPP_ABNORMAL_ALL so filteredRows legitimately stays 0 — but the design should explicitly note this as an intentional non-change for completeness, since `afterExec`/`setOk` also report a filtered count.
- No mention of the empty-insert path. Verified independently it is safe (empty insert skips executeSingleInsert entirely, so doBeforeCommit never runs and loadedRows=0 is correct), but the design's risk section should have named it since `beginTransaction`/`connectorTx` are skipped there.

**额外风险**:
- Strict-mode interaction is benign but undocumented. AbstractInsertExecutor.checkStrictModeAndFilterRatio (line 232-246) runs in executeSingleInsert BEFORE onComplete->doBeforeCommit, so it evaluates with loadedRows still 0. For MC this is harmless (filteredRows=0 too, so the ratio guard `filteredRows > ratio*(filteredRows+loadedRows)` is `0 > 0` = false). The backfill happening afterward cannot retroactively affect the strict-mode check — which matches legacy exactly. Worth a one-line note in the design so a future reader doesn't 'fix' the ordering and accidentally make the filter-ratio denominator non-zero.
- getUpdateCnt() is read off connectorTx without holding the synchronized lock that addCommitData/commit use (MaxComputeConnectorTransaction.addCommitData synchronizes on `this`, but getUpdateCnt streams commitDataList unsynchronized). At doBeforeCommit time all BE fragment reports have completed (coordinator.join returned) so no concurrent addCommitData is in flight — same as legacy MCTransaction.getUpdateCnt which is also unsynchronized. Low risk, but it relies on the join->doBeforeCommit happens-before edge; if a future change moves commit-data feed off the join path this read could race. Pre-existing in legacy, not introduced by this fix.
- If a future SPI transaction-model connector returns a stateful ConnectorTransaction but does NOT override getUpdateCnt(), the backfill will silently write 0 (SPI default returns 0) — re-introducing the exact symptom for that connector with no fail-loud. The fix is generic and correct for MC, but the 'any future transaction-model connector automatically benefits' claim is conditional on that connector implementing getUpdateCnt(). Worth flagging in the connector-author contract / SPI javadoc rather than relying on the default.
- The design proposes a doc correction to P4-T05-T06-cutover-design.md:114 and decisions-log but scopes it out of the code commit. Confirmed line 114 does say 'doBeforeCommit ... null for MC => correctly skipped' and is genuinely wrong about loadedRows. Risk: if the doc correction is deferred and forgotten, the stale 'correctly skipped' rationale could mislead a future reviewer into re-removing the backfill during batchD legacy cleanup. Recommend bundling the deviations/decisions-log note into the same change.

---

## 3. 守门 / commit 计划

| issue | commit 标题(建议) | 守门(模块) |
|---|---|---|
| FIX-READ-DESC | `[P4-T06d] 读路径 TableDescriptor 类型混淆 — 补 buildTableDescriptor override 产 TMCTable` | mvn ... -pl :fe-connector-maxcompute ... + import-gate |
| FIX-READ-SPLIT | `[P4-T06d] byte_size split size sentinel — 默认 split 回填 size=-1` | mvn ... -pl :fe-connector-maxcompute ... + import-gate |
| FIX-DDL-ENGINE | `[P4-T06d] 无 ENGINE 的 CREATE TABLE — paddingEngineName/checkEngineWithCatalog 识别 PluginDriven` | mvn -f .../fe/pom.xml -pl :fe-core -am ... test-compile + checkstyle:check |
| FIX-DDL-REMOTE | `[P4-T06d] DDL 远端名解析 — CREATE/DROP TABLE 用 getRemoteName/getRemoteDbName 再发 connector` | mvn -f .../fe/pom.xml -pl :fe-core -am ... test-compile + checkstyle:check |
| FIX-PART-GATES | `[P4-T06d] partitions() TVF + SHOW PARTITIONS analyze 网关 + 分区元数据 override` | mvn -f .../fe/pom.xml -pl :fe-core -am ... test-compile + checkstyle:check |
| FIX-WRITE-ROWS | `[P4-T06d] INSERT affected rows 恒 0 — doBeforeCommit 补 loadedRows=getUpdateCnt()` | mvn -f .../fe/pom.xml -pl :fe-core -am ... test-compile + checkstyle:check |

## 4. 合并 TODO(执行时勾选)

**阶段 1 — 恢复读路径可用 (gate live SELECT)**
- [ ] FIX-READ-DESC — MaxComputeConnectorMetadata 缺 buildTableDescriptor override,导致翻闸后 toThrift 走 null 兜底产 SCHEMA_TABLE(无 mcTable),BE file_scanner 无条件 static_cast 到 MaxComputeTableDescriptor 类型混淆崩溃;修法为在 MC connector 补 override 产出 MAX_COMPUTE_TABLE+TMCTable,并把 endpoint/quota/properties 透传进 metadata。
    - [ ] test: fe-connector-maxcompute UT: MaxComputeConnectorMetadata.buildTableDescriptor (新增,放 fe-connector-maxcompute/src/test)
    - [ ] test: regression-test/suites/external_table_p2/maxcompute/test_external_catalog_maxcompute.groovy
    - [ ] test: regression-test/suites/external_table_p2/maxcompute/test_max_compute_all_type.groovy
    - [ ] test: regression-test/suites/external_table_p2/maxcompute/test_max_compute_partition_prune.groovy
- [ ] FIX-READ-SPLIT — byte_size split 在翻闸 connector 用 .length(splitByteSize) 回填 rangeDesc.size,丢失 legacy 的 -1 sentinel,使 BE 把 byte-size split 误判为 row-offset → 默认路径静默读出错误数据;改 MaxComputeScanPlanProvider.java:268 为 .length(-1) 恢复 sentinel。
    - [ ] test: fe/fe-connector/fe-connector-maxcompute/src/test/java/org/apache/doris/connector/maxcompute/MaxComputeScanRangeTest.java (new UT)
    - [ ] test: regression-test/suites/external_table_p2/maxcompute/test_external_catalog_maxcompute.groovy (default byte_size read path)
**阶段 2 — 恢复 DDL 可用**
- [ ] FIX-DDL-ENGINE — paddingEngineName/checkEngineWithCatalog 在 MC instanceof 分支后新增 PluginDrivenExternalCatalog 分支(keyed on getType()=="max_compute"→ENGINE_MAXCOMPUTE,经 helper 通用化),纯 fe-core 最小改动,镜像 legacy 自动补 engine=maxcompute 行为;须先于 Batch D 删 legacy MC 分支落地。
    - [ ] test: fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/info/CreateTableInfoTest.java (UT: paddingEngineName/checkEngineWithCatalog PluginDriven 分支)
    - [ ] test: regression-test/suites/external_table_p2/maxcompute/test_max_compute_create_table.groovy (E2E: Test1/Test2/Test3 无 ENGINE 的 CREATE TABLE 翻闸态由 FAIL 转 PASS, qt_test*_show_create_table 断言)
- [ ] FIX-DDL-REMOTE — 在 PluginDrivenExternalCatalog 的 createTable/dropTable override 内先用 getRemoteName/getRemoteDbName 把本地名解析成 ODPS 远端真名再交给连接器，mirror legacy MaxComputeMetadataOps，纯 FE 改动、不扩 SPI、不动连接器。
    - [ ] test: fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenExternalCatalogDdlRoutingTest.java
    - [ ] test: regression-test/suites/external_table_p2/maxcompute/test_max_compute_create_table.groovy
**阶段 3 — 恢复分区可见 (partitions TVF / SHOW PARTITIONS)**
- [ ] FIX-PART-GATES — 给 PluginDrivenExternalTable 加 isPartitionedTable/getPartitionColumns override(keyed on connector 的 partition_columns 声明),并在 PartitionsTableValuedFunction.analyze 双网关补 PluginDriven 分支,打通 T06c 已接好的 SHOW PARTITIONS / partitions() TVF BE handler;不删 Batch-D 红线分支。
    - [ ] test: external_table_p2/maxcompute/test_external_catalog_maxcompute
    - [ ] test: external_table_p2/maxcompute/test_max_compute_schema
    - [ ] test: external_table_p2/maxcompute/test_max_compute_partition_prune
**阶段 4 — 写回正确性 (affected rows)**
- [ ] FIX-WRITE-ROWS — 在 PluginDrivenInsertExecutor.doBeforeCommit() 的事务模型分支(connectorTx != null)补一行 loadedRows = connectorTx.getUpdateCnt(),回填翻闸丢失的 affected-rows,镜像 legacy MCInsertExecutor;getUpdateCnt 全链路已就绪,纯 fe-core 一处赋值。
    - [ ] test: external_table_p2/maxcompute/write/test_mc_write_insert
- [ ] 全部落地后 → 用户跑 live 验证矩阵(SELECT/分区表 SELECT/SHOW PARTITIONS/partitions() TVF/无 ENGINE CREATE TABLE/INSERT affected rows/DROP TABLE/DB) 全绿 → 解锁 Batch D

## 5. 本批次外(其余存活发现, 待用户定)

> 以下为 review 存活但**未纳入本批**的 major/minor; 不在本设计, 列此以免静默遗漏(fail loud)。

- **READ-P3 / CACHE-P1** (major/minor): FE 侧内部分区裁剪 + partition_values cache 丢失(退化为 connector 每查询直连 ODPS)。性能向, 待定。
- **READ-P4** (major): datetime 谓词下推 ISO-8601 解析失败被静默吞 + 源时区取 endpoint region。
- **READ-P5** (major): limit-split 优化忽略 `enable_mc_limit_split_optimization`(默认 OFF), 默认行为与 legacy 相反。
- **READ-C6** (question): CAST 谓词下推语义与 legacy 不同(剥 CAST 下推 vs 保守不下推)。
- **DDL-P4** (major): CREATE TABLE 列约束(auto-increment/聚合)校验被静默绕过。
- **DDL-P2 / CACHE-P2/C3** (question): DROP DATABASE FORCE 级联不复刻(force 不转发)。
- **WRITE-P2/P3, READ-C7/C8, REPLAY-P1, DDL-C5** (minor): block 上限硬编码 / isKey 标记 / split 缺字段 / post-commit 吞错 / editlog-cache 顺序反转 / IF NOT EXISTS 冗余 editlog。详见报告。
- **READ-C9**: legacy NOT IN 取反 bug, 翻闸已修正 —— 回归用例须以**正确**语义为基线, 勿误判。
