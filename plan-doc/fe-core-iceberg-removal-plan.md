# fe-core Iceberg 代码与 Maven 依赖移除计划（v2，实证重写）

> 目标：fe-core 逐步不再包含任何 iceberg 特有能力代码（`instanceof Iceberg*`、`import ...datasource.iceberg.*`、iceberg SDK import）与 iceberg 专属 maven 依赖。
> 生成：2026-07-04。方法：39 个分析 agent 分两阶段（7 路并行清点 → 对每个"可删"结论做双镜头对抗反驳 + 高风险死臂能力孪生抽查 + 8 个存活集群移除路径设计 + 独立完备性复扫）。全部结论带 file:line 实证。
> **v1 草稿（同名文件旧版）记载失实**：其声称 broker/ 与 fileio/ 已删除——实际两目录仍在、无对应提交。本 v2 为全量重写，v1 结论一律以本文为准。

---

## 0. 结论速览

- 基线（完备性复扫 fresh grep）：fe-core `src/main` 含 iceberg 的文件 **236 个**（其中 `datasource/iceberg/` 74 个 + 目录外 165 个引用点文件 + vendored 拆包类 `src/main/java/org/apache/iceberg/DeleteFileIndex.java`）；`src/test` **106 个**。
- **翻闸已生效**（复核）：`CatalogFactory.java:49-50` SPI_READY_TYPES = {jdbc, es, trino-connector, max_compute, paimon, **iceberg**}；**hive/hms 不在其中** → HMS 目录仍走 fe-core 原生栈，"HMS 目录下的 iceberg 表"是真实存活路径，也是最大的删除阻塞项。
- **GSON 持久化兼容不阻塞任何删除**：`GsonUtils.java:395-411,464-466,491-494` 只注册字符串标签映射到 PluginDriven*，零原生类引用；`IcebergGsonCompatReplayTest` 构造的是 PluginDrivenExternalCatalog + 字符串替换 clazz 判别符（:52-58,64-68），删原生类后原样存活，且必须保绿（它是升级兼容的守卫）。
- `iceberg_meta` TVF 已在上游删除（eea082b4540），fe-core `tablefunction/`、`FrontendServiceImpl` 零 iceberg 引用。
- 连接器覆盖度实证：**原生 iceberg catalog 的全部运行时能力已由 fe-connector-iceberg 完整承接**（元数据/DDL/scan+count 下推/sys 表/写入+事务+OCC/全部 9 个 EXECUTE 过程含 rewrite_data_files（经引擎中立的 ConnectorRewriteDriver 执行，非原生执行器）/统计/时间旅行/MTMV/vended 凭据/缓存/kerberos+TCCL）。**但"完整移植"≠"fe-core 可清零"**，三大存活根：HMS-iceberg、行级 DML 计划合成（签字的临时架构）、属性/鉴权接线（见 §4）。

| 分类 | 规模 | 处置 |
|---|---|---|
| 硬死码孤岛（阶段一） | `datasource/iceberg/` 内 ~36 文件 + 若干目录外死执行器 | 立即删（个别需先削臂/搬常量） |
| AWS 依赖簇（阶段二） | 属性/连通性/AWS helper ~16 文件 + 3 个 maven 依赖 | 外科手术式剥离后摘依赖 |
| 死臂 + 实体类（阶段三） | ~30 处 `instanceof Iceberg*` 死臂 + 3 个实体类，波及 ~75-100 文件 | 逐臂孪生审计后清剿 |
| 架构迁移（阶段四） | HMS-iceberg 全栈 + 行级 DML 合成 | 需架构决策（Trino 参照见 §6） |
| 永久保留标识面 | thrift/枚举/配置/列名常量/GSON 标签 | 不删（§7） |

---

## 1. Maven 依赖裁决（fe-core/pom.xml）

| 依赖 | 裁决 | 依据 |
|---|---|---|
| `org.apache.iceberg:iceberg-core` (pom:537-541) | **暂留**，阶段四后可摘 | 被钉：① HMS-iceberg（`HMSExternalCatalog.java:40,242-249` → `IcebergUtils.createIcebergHiveCatalog:1307-1322`）；② 行级 DML 合成（`BindSink.java:119-121`、`StatementContext.java:323` 持 `List<FileScanTask>`）；③ vendored 拆包类 `org/apache/iceberg/DeleteFileIndex.java`；④ 属性簇存活方法。fe/pom.xml 的 dependencyManagement + `${iceberg.version}` **永久保留**（fe-connector-iceberg 与 be-java-extensions/iceberg-metadata-scanner 自带副本仍需版本管理） |
| `org.apache.iceberg:iceberg-aws` (pom:542-546) | **阶段二摘除** | `org.apache.iceberg.aws.*` 仅 9 文件 import，全部属可剥离簇（§4）；hive/nereids/statistics 零命中。⚠️ 例外见 §4 决策点 |
| `software.amazon.awssdk:s3tables` (pom:737-740) | **阶段二摘除** | 唯一 main 引用 = `IcebergS3TablesMetaStoreProperties.java:30-31` |
| `software.amazon.s3tables:s3-tables-catalog-for-iceberg` (pom:742-745) | **阶段二摘除** | 唯一 main 引用 = 同上文件 :32-34 |
| `org.apache.avro:avro` (pom:589) | **保留** | pom 注释"For Iceberg"有误导：hudi 直接使用（`HudiUtils.java:45-48`、`HMSExternalTable.java:743,748`），且是 iceberg-core 传递需求。fe/pom.xml:345-347 记载 avro.version 与 iceberg.version 耦合——摘 iceberg-core 前不可动 |
| `software.amazon.awssdk:s3-transfer-manager` (pom:562-565) | **保留** | 纯运行时依赖，hadoop-aws 的 S3A IO 路径需要（与 iceberg 无关的消费者） |

连接器自包含性已核实：fe-connector-iceberg pom 自带 compile 级 iceberg-core/iceberg-aws/AWS SDK v2 闭包/s3tables，经 plugin-zip 子加载器打包；be-java-extensions/iceberg-metadata-scanner 亦自带。**删 fe-core 副本不影响插件。**

---

## 2. 现状引用图（74 文件分簇，import 精确 + 控制流核验）

### 死簇（阶段一目标，~36 文件）
| 簇 | 文件 | 备注 |
|---|---|---|
| broker IO | broker/ 3 文件 | 零入向引用（main/test/目录内全核实；注意 `common/parquet/BrokerInputFile` 是同名不同类，勿混） |
| fileio 委托 | fileio/ 4 文件 | 静态零引用；⚠️ 对抗核验发现**配置注入反射活路**：HMS-iceberg 目录上用户可设 `io-impl=...DelegateFileIO`（`IcebergUtils.java:1311-1321` 把用户属性原样透传 `HiveCatalog.initialize`），且有 p2 回归测试在用 → 删除需决策（§8-Q1） |
| 6 个非 REST catalog flavor + 工厂 | 7 文件 | 仅互引；工厂零引用（CatalogFactory legacy case 已删）。GSON/路由/统计侧只引 base 类不引 flavor |
| DLF 实现 | dlf/ + HiveCompatibleCatalog 5 文件 | 死；删除需同步编辑 `IcebergAliyunDLFMetaStoreProperties.initCatalog`（唯一编译引用点） |
| 原生 EXECUTE actions | action/ 11 文件 | 运行时死（插件走连接器 factory）；编译被 `ExecuteActionFactory.java:66` 的 IcebergExternalTable 死臂钉住 → 先削臂 |
| 原生 rewrite 执行半 | rewrite/ 6 文件 | 运行时死。**重要更正**：插件路径的 rewrite_data_files 执行半 = 引擎中立的 `ConnectorRewriteDriver`/`ConnectorRewriteExecutor`（fe-core 但 iceberg-free），原生 rewrite/ 目录只是死掉的孪生兄弟，**可删** |
| 写事务 + delete-plan helper | IcebergTransaction 等 4 文件 | 运行时死；被目录外死执行器（IcebergDeleteExecutor/IcebergMergeExecutor/IcebergInsertExecutor/IcebergRewriteExecutor）编译钉住，后者又被 `IcebergRowLevelDmlTransform.java:193-197`、`InsertIntoTableCommand.java:568-583` 的死臂钉住 → 同批削臂后连锁删除（抽查已确认这些臂的插件孪生存活：PluginDrivenInsertExecutor + 连接器事务） |

对抗反驳（每簇双镜头：隐藏引用 + 运行时可达）：以上除 fileio 外全部通过（refuted=false, high confidence）。

### 存活簇（不可立即删）
| 簇 | 文件数 | 存活根 |
|---|---|---|
| HMS 元数据/schema/快照/分区网 | 13（IcebergUtils、IcebergMetadataOps、IcebergExternalMetaCache、IcebergMvccSnapshot、Iceberg*CacheKey/Value、IcebergSnapshot、IcebergPartition*、DorisTypeToIcebergType…）+ hive/IcebergDlaTable | HMS-iceberg（hive 未翻闸） |
| scan source | source/ 7 文件（IcebergScanNode 1235 行…） | HMS-iceberg 读路径（`PhysicalPlanTranslator.java:860-864` 对 DlaType.ICEBERG 构造 IcebergScanNode） |
| manifest 缓存 | cache/ 2 + IcebergManifestEntryKey | 唯一消费者 = IcebergScanNode:718,723 |
| scan profile | profile/IcebergMetricsReporter | 唯一消费者 = IcebergScanNode:574 |
| 行级 DML 计划合成工具 | IcebergNereidsUtils、IcebergMergeOperation、IcebergRowId、IcebergMetadataColumn（4 活/5） | 行级 DML 根：`RowLevelDmlRegistry.java:38` → `IcebergRowLevelDmlTransform`（**对插件表同样生效**，:78-101 处理 PluginDrivenExternalTable；其 :96,:131 的"dormant"注释已过时）→ 合成 IcebergDelete/Update/MergeCommand；提交侧已走 SPI（PluginDrivenInsertExecutor + 连接器事务），非原生 IcebergTransaction |
| vended 凭据 | IcebergVendedCredentialsProvider | `VendedCredentialsFactory.java:62-63` Type.ICEBERG 分支（插件路径每个 iceberg 目录都执行） |
| catalog base 类 | IcebergExternalCatalog（MIXED） | 实例翻闸后不再构造，但**常量活着**：`IcebergUtils.java:1876-1881` 读 MANIFEST_CACHE_*、`IcebergMetadataOps.java:122,253,491`、`IcebergScanNode.java:197-203` switch ICEBERG_HMS/REST/…。删除前须把常量搬家（对抗核验修正了 v1"随 flavor 一起删"的误判） |
| 实体类 | IcebergExternalDatabase/Table/SysExternalTable（MIXED） | 运行时零构造路径，但编译扇入最重（~25 个活文件的死臂钉住）→ 阶段三。⚠️ 一个回放边缘：`ExternalCatalog.buildDbForInit` switch case ICEBERG（`ExternalCatalog.java:972-973`）——升级场景下**翻闸前写的 InitDatabaseLog(Type.ICEBERG) 回放**会在 GSON 迁移后的 PluginDriven 目录上构造原生 IcebergExternalDatabase → 删实体类前必须把该 case 改为构造 PluginDriven 数据库（与 GSON 标签迁移同型的兼容处理） |

### 目录外死执行器/sink 车道（完备性复扫补盲，v1 完全遗漏）
`LogicalIcebergTableSink`、`LogicalIcebergMergeSink`、`planner/IcebergDeleteSink`、`planner/IcebergMergeSink`、`planner/IcebergTableSink`、`IcebergDeleteExecutor`、`IcebergMergeExecutor`、`IcebergInsertExecutor`、`IcebergRewriteExecutor`、`IcebergTransactionManager` + `TransactionManagerFactory.java:21`（createIcebergTransactionManager）——与实体类/写事务簇同命运，阶段一/三连锁处理。

---

## 3. 阶段一：硬死码删除（可立即执行，无行为影响）

1. 删 broker/（3 文件）——零耦合，直接删。
2. 削 `ExecuteActionFactory.java:66` IcebergExternalTable 死臂 → 删 action/（11）+ rewrite/（6）。
3. 削 `IcebergRowLevelDmlTransform.java:193-197`、`InsertIntoTableCommand.java:568-583` 死臂 → 删 IcebergDeleteExecutor/IcebergMergeExecutor/IcebergInsertExecutor/IcebergRewriteExecutor、IcebergTransactionManager（+ TransactionManagerFactory 对应方法）、IcebergTransaction、IcebergConflictDetectionFilterUtils、helper/IcebergRewritableDeletePlanner*。
4. 6 个 catalog flavor + 工厂（7 文件）：先把 IcebergExternalCatalog 里被活代码读取的常量搬家（IcebergUtils 或属性层），flavor 引用的常量随删；改造 4 个测试（`ExternalMetaCacheRouteResolverTest:76-78` 换 PluginDriven 构造、StatisticsUtilTest、DbsProcDirTest、IcebergDLFExternalCatalogTest）；IcebergGsonCompatReplayTest 不动、保绿。
5. dlf/ + HiveCompatibleCatalog（5 文件）+ 编辑 `IcebergAliyunDLFMetaStoreProperties.initCatalog`。
6. fileio/（4 文件）——**待 §8-Q1 决策后执行**。

测试爆炸半径（103 个含 iceberg 的测试文件已逐个定性）：随码删 1、需改写 7、随阶段保留 39、不受影响 56。

---

## 4. 阶段二：AWS 依赖簇剥离（摘 3 个 maven 依赖）

**关键更正（对抗核验推翻 v1 与首轮清点的两处误判）：**
- fe-core iceberg 属性簇**翻闸后仍活**：`PluginDrivenExternalCatalog.java:147`（initPreExecutionAuthenticator）→ `CatalogProperty.getMetastoreProperties:251,260` → `MetastoreProperties.create`（Type.ICEBERG 注册于 :90）→ `IcebergPropertiesFactory` 8 flavor → 每个插件路径 iceberg 目录首次访问都构造属性对象并跑 `initNormalizeAndCheckProps`（kerberos/鉴权接线用）。
- `property/common/IcebergAws{ClientCredentials,AssumeRole}Properties` **不是死码**：`IcebergRestProperties.java:353,356` 的调用点在 `addGlueRestCatalogProperties()`（:345-361），调用链 initNormalizeAndCheckProps→initIcebergRestCatalogProperties（:219→:289）**在插件路径上活着**（REST + `iceberg.rest.signing-name=glue|s3tables` 的正常受支持配置）。
- vended 凭据在插件路径同样经 fe-core `IcebergVendedCredentialsProvider`（工厂 Type.ICEBERG 分支），但该类只 import iceberg-core 类型，不钉 aws 依赖。

**剥离步骤**（每步独立可落地）：
1. 删 5 个连通性 tester（AbstractIcebergConnectivityTester + 4 子类）+ `CatalogConnectivityTestCoordinator.java:284-304` 死臂——test_connection 已由连接器承接（实证 FULLY），tester 不 import aws，纯清理。
2. 剥属性类中的死方法：AbstractIcebergProperties.toFileIOProperties/buildIcebergCatalog、IcebergGlue/S3Tables/DLF 各自的 initCatalog 及 helper（org.apache.iceberg.aws 与 s3tables import 全部集中于此；插件路径只用 initNormalizeAndCheckProps，initCatalog 只有死掉的原生 catalog 构建路径调用）。
3. 处理唯一活的 aws 引用：`addGlueRestCatalogProperties` 的 glue/s3tables REST 签名属性规范化——就地内联所需常量（几行字符串键）或下沉连接器侧，消除对 iceberg-aws 类型的 import。
4. 删 IcebergS3TablesMetaStoreProperties（+2 测试 + tester）后同 commit 摘 `s3tables` + `s3-tables-catalog-for-iceberg`；删 IcebergAws* helper、DLFCatalog 后摘 `iceberg-aws`。
5. ⚠️ 决策点 §8-Q2：摘 iceberg-aws 后，HMS-iceberg 目录上用户手工配置 `io-impl=org.apache.iceberg.aws.s3.S3FileIO` 的极端场景会反射失败（属性透传见 §2 fileio 行）。

**属性簇整体删除**（9 文件全删 + MetastoreProperties.java:51,90 注册注销）是更远一步，前置 = 把 initPreExecutionAuthenticator 的鉴权接线与 vended 凭据 Type.ICEBERG 分支改走连接器（Trino 参照：目录配置完全插件内，引擎零 per-connector 属性类）。阶段二不强求，摘依赖只需上面 1-4。

---

## 5. 阶段三：死臂清剿 + 实体类删除

- 30 处翻闸后死臂（`instanceof Iceberg*`）分布于 ~20-30 个共享活文件：PhysicalPlanTranslator:885、StatisticsUtil:1001、StatisticsAutoCollector:152、RefreshManager:243、Env.getDdlStmt 两处 ~25 行臂、ExternalMetaCacheRouteResolver:63、UserAuthentication、ShowCreate*/ShowPartitions/CreateTableInfo、BindRelation/BindSink(:727-836 大臂)/LogicalFileScan:213,263/SlotTypeReplacer/MaterializeProbeVisitor、RewriteTableCommand:190-201 等（完整 82 条臂清单含 LIVE/DEAD/IDENTIFIER_ONLY 定性见分析工作流归档）。
- **能力孪生抽查（12 条最高风险死臂）：12/12 全部 COVERED**（通用/插件路径有实证等价承接）。删臂执行时仍须对余下死臂逐条做同款孪生核对（历史上嵌套列裁剪臂曾漏承接致静默回归）。
- 实体类删除顺序：先修 `ExternalCatalog.buildDbForInit` case ICEBERG 的回放兼容（改构造 PluginDriven 数据库，见 §2），再清剿死臂，最后删 IcebergExternalDatabase/Table/SysExternalTable + IcebergExternalCatalog（常量已于阶段一搬家）。规模 ~75-100 文件。

---

## 6. 阶段四：架构迁移（Trino 参照）

### 6a. HMS-iceberg（最大阻塞，钉住 iceberg-core + ~24 文件）
Trino 方案：**hive 插件零 iceberg 代码**——引擎提供通用表重定向钩子 `ConnectorMetadata#redirectTable(session, tableName) → Optional<CatalogSchemaTableName>`，hive 元数据层检测表属性 `table_type=ICEBERG` 后把表重定向到配置的 iceberg 目录（`hive.iceberg-catalog-name`），后续全部规划/读写走 iceberg 连接器。
Doris 可借鉴的两条路：
- **路 A（Trino 式重定向）**：fe-core 加中立"表重定向"接缝，HMS 目录检测 DlaType.ICEBERG 后把表委给同集群的插件 iceberg 目录处理。优点：不必等 hive 整体迁 SPI；缺点：需要用户侧有/隐式建一个对应 iceberg 目录，跨目录语义（权限/缓存/SHOW）要设计。
- **路 B（随 hive 整体迁移）**：hive 进 SPI_READY_TYPES 时 HMS-iceberg 自然进插件（fe-connector-hive/fe-connector-hms 方向）。优点：无新架构面；缺点：时间表最远。
无论哪条，落地后连锁解放：IcebergUtils、IcebergMetadataOps、IcebergExternalMetaCache、source/ 全部、cache/、profile/、IcebergDlaTable、IcebergTransaction 残留、vendored DeleteFileIndex（+ checkstyle suppressions.xml:72-73）、IcebergRestExternalCatalog，然后才能摘 `iceberg-core`。

### 6b. 行级 DML 计划合成（签字的临时架构：留引擎侧，直至出现第二个行级 DML 消费者）
Trino 方案：引擎为所有连接器合成**一份通用 MERGE 计划**——`ConnectorMetadata#getMergeRowIdColumnHandle`（不透明行标识列）+ `RowChangeParadigm`（iceberg=DELETE_ROW_AND_INSERT_ROW）+ worker 侧 `ConnectorMergeSink` 吃统一的操作码行流；引擎不知道 iceberg 存在。
Doris 对应改造（不必推翻签字决策，可先"去 iceberg SDK 化"）：把 IcebergMergeOperation 的操作码、IcebergRowId/IcebergMetadataColumn 的行标识抽象改为 fe-connector-api 中立类型（两个小增项：中立 merge 操作码枚举 + 行标识列描述，SPI 带默认实现），IcebergNereidsUtils 中 SDK 表达式转换下沉连接器；`StatementContext.java:323` 的 `List<FileScanTask>` 暂存改为不透明句柄。做完后该车道虽仍在 fe-core，但**不再 import org.apache.iceberg**——iceberg-core 摘除不再被它阻塞（只剩 6a）。

### 6c. 目录属性/鉴权（远期收尾）
Trino：目录配置=插件内 ConnectorFactory#create(props)，引擎零 per-connector 属性类。Doris 对应：initPreExecutionAuthenticator/vended 凭据的 Type.ICEBERG 分支改由连接器提供（现有 ConnectorContext#vendStorageCredentials 接缝已在），之后 MetastoreProperties 注销 Type.ICEBERG、删属性簇余量。

---

## 7. 永久保留（标识面，与 SDK/原生类无关，删了反而破坏兼容）

- thrift 契约：`TIcebergCommitData`/`TIcebergColumnStats` 等（DataSinks.thrift 等 9 文件）——**插件写路径同样使用**（BE 回报 → `LoadProcessor.java:230-236` feed 连接器事务），FE-BE 线协议。
- `InitDatabaseLog.Type.ICEBERG` 枚举常量——旧 editlog 回放需要（配合 §5 的 buildDbForInit 兼容改造）。
- GSON 字符串标签注册（8 个旧 catalog 名 + db/table 标签）。
- `Column.ICEBERG_ROWID_COL`（fe-catalog）——活 SPI 消费者在用；fe-core 死臂删除后 `ColumnType` 的 iceberg 方法成孤儿可顺手清。
- 存储属性中的 iceberg 条件逻辑（AzureProperties:155-156,306-320、OSSProperties:74,164 等 isIcebergRestCatalog 判断）与 fe-filesystem 的 `iceberg.rest.*` 凭据别名键——插件路径与 HMS 路径都在用的字符串面。
- fe-common Config：`enable_query_iceberg_views` 等开关及消费者（BindRelation:623,643）。
- fe/pom.xml 的 `${iceberg.version}`/`${avro.version}` 版本管理（连接器与 BE 扩展模块仍需）。
- be-java-extensions/iceberg-metadata-scanner 整模块（BE 侧 JNI sys-table 扫描，独立于 fe-core）。

---

## 8. 用户决策（2026-07-04 已裁定）

- **Q1+Q2（io-impl 极端配置）= A 接受失效**：fileio/ 4 文件并入阶段一删除（同步改掉在用的 p2 回归）；iceberg-aws 阶段二照常摘，文档注明 HMS-iceberg 上手配 `io-impl=...S3FileIO`/内部 FQCN 的极端配置不再支持。
- **Q3（HMS-iceberg 方向）= B 随 hive 整体迁移**：不建 Trino 式重定向接缝；§6a 走路 B，iceberg-core 摘除时间表挂靠 hive 目录迁插件框架的进度。
- **Q4（行级 DML 去 SDK 化）= A 现在做**：§6b 提前启动，签字的引擎侧留驻决策不变，只消除 SDK import；**设计先行**（见 Q5）。
- **Q5（执行范围）= 继续只分析**：暂不动码。下一轮先产出行级 DML 去 SDK 化的详细设计（新增中立 SPI 面的精确形状、`StatementContext` 暂存句柄化、连接器侧下沉点、兼容与验收），设计签字后再按 阶段一 → 二 → 三 → 6b 的顺序动码。

## 9. 验收（每阶段）

fe-core `test-compile` 过 + 波及单测按 §3 处置表逐个交代 + checkstyle 净 + import-gate 净 + IcebergGsonCompatReplayTest 保绿；行为敏感项（削臂）逐条附能力孪生证据；docker/e2e 项如未跑须显式标注。
