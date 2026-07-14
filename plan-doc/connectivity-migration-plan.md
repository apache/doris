# 方案：移除 fe-core 的 `datasource/connectivity` 包

> 目标（用户）：把 `fe/fe-core/src/main/java/org/apache/doris/datasource/connectivity/` 迁出 fe-core，
> 使 fe-core 不再出现与具体 meta 类型（HMS/Glue/REST）或具体 fs 类型（S3/Minio/HDFS）相关的逻辑。
>
> 结论先行：**这个包没有东西可"搬"。它在本分支上已是死代码（对唯一可达的两种目录零效果）。
> 正确做法是"删空壳 + 在插件侧补回被删掉的能力"，而不是把死代码搬进插件模块。**

---

## 一、现状：整包已空转（FULLY_DEAD）

### 1.1 唯一入口，且被插件目录完全绕开

- 入口：`ExternalCatalog.checkWhenCreating()`（`fe-core/.../datasource/ExternalCatalog.java:322-334`），
  仅当 `test_connection=true` 时构造 `CatalogConnectivityTestCoordinator` 并 `runTests()`。
  它的唯一调用者是 `CatalogFactory.java:164`（且在 `if (!isReplay)` 内 → 回放/GSON 永不触发）。
- `PluginDrivenExternalCatalog.checkWhenCreating()`（`PluginDrivenExternalCatalog.java:206-237`）
  **完全覆盖**基类实现、不调 `super`：改走 `connector.testConnection(session)` +
  `validationCtx.executePendingBeTests()`。
- `SPI_READY_TYPES = {jdbc, es, trino-connector, max_compute, paimon, iceberg, hms}`
  （`CatalogFactory.java:54-55`）全部由 `PluginDrivenExternalCatalog` 承接。

⇒ 还继承基类 `checkWhenCreating()` 的具体子类只有 3 个，其中 1 个已废：

| 子类 | 是否走协调器 |
|---|---|
| `PluginDrivenExternalCatalog` | 否（override） |
| `RemoteDorisExternalCatalog`（`type=doris`） | **是** |
| `TestExternalCatalog`（`type=test`） | **是**（仅单测，`FeConstants.runningUnitTest` 门禁） |
| `LakeSoulExternalCatalog` | 否 —— `CatalogFactory.java:141-142` 直接抛 `"Lakesoul catalog is no longer supported"`，根本构造不出来 |

### 1.2 协调器本身已被掏空 → 三条支路全是 no-op

- **meta 探针**：`createMetaTester()`（`CatalogConnectivityTestCoordinator.java:265-273`）
  无条件返回匿名 no-op（10 个真探针在删 fe-core hive/iceberg 时被删，见 §2）。
  ⇒ `testConnection()` 空跑；`getTestLocation()` 返回 `null`。
- **S3/Minio 存储探针**：入口是 `getTestObjectStorageProperties():109-124`，
  第一行就是 `if (StringUtils.isBlank(this.warehouseLocation)) return null;`。
  而 `warehouseLocation` 只可能来自上面那个恒返回 `null` 的 `getTestLocation()`。
  ⇒ `S3ConnectivityTester` / `MinioConnectivityTester` / `AbstractS3CompatibleConnectivityTester`
  **不可达**（无反射、无 ServiceLoader、无单测直接构造；fe-core 测试树对本包零引用）。
- **HDFS 探针**：`HdfsCompatibleConnectivityTester.testFeConnection/testBeConnection:46-56`
  是空 TODO（**upstream 也一样**）。⇒ 空跑。

⇒ **删掉这 8 个文件，对 `type=doris` / `type=test` 逐字节零行为变化。**

---

## 二、真正的问题：能力已经回退了（本次删除只是把它暴露出来）

upstream master 的同名包有 **18 个文件**，本分支只剩 8 个。被删的 10 个全是 meta 探针
（引入自 upstream PR **#57004**，2025-10-30）：

`HMSBaseConnectivityTester` / `AbstractHiveConnectivityTester` / `HiveHMSConnectivityTester` /
`HiveGlueMetaStoreConnectivityTester` / `AWSGlueMetaStoreBaseConnectivityTester` /
`AbstractIcebergConnectivityTester` / `IcebergHMSConnectivityTester` /
`IcebergGlueMetaStoreConnectivityTester` / `IcebergRestConnectivityTester` /
`IcebergS3TablesMetaStoreConnectivityTester`

删除发生在 `ec3a0cd55f4`（P6 iceberg 迁移）和 `10290e02933`（原子删除 hive/hudi/iceberg 死码）。

### 2.1 能力对照表（upstream vs 本分支）

| upstream 能力 | 本分支现状 | 归属 |
|---|---|---|
| Iceberg **REST** meta 探针 | ✅ 已在插件侧 `IcebergConnector.testConnection():240-248` | iceberg 连接器 |
| Iceberg 存储 **S3 FE 侧**探针 | ✅ 已在插件侧 `IcebergConnector.probeStorage():263-324`（自建 S3FileIO HEAD） | iceberg 连接器 |
| Iceberg **HMS / Glue / S3Tables** meta 探针 | ❌ **丢失**（`testConnection()` 只对 `catalog.type=rest` 探测） | iceberg 连接器 |
| Hive **HMS / Glue** meta 探针 | ❌ **丢失**（`HiveConnector` 未 override `testConnection()` → SPI 默认 `success()`，静默通过） | hive 连接器 |
| S3/Minio 存储 **BE 侧**探针（thrift `test_storage_connectivity`） | ❌ **丢失**（FE 侧唯一调用点就是这个死包） | 见 §4 决策 3 |
| HDFS 探针 | —（upstream 本就是空 TODO） | 不做 |
| Paimon / Hudi | —（upstream 也没有） | 不做 |

> **关键细节**：upstream 里 `AbstractHiveConnectivityTester` **不 override** `getTestLocation()`（返回 `null`），
> 只有 iceberg 系探针返回 warehouse（`s3://...`）。所以**即便在 upstream，S3/Minio 存储探针也只有 iceberg 目录触发得到**
> （p2 套件里那句注释 "S3 is not tested for Hive HMS without warehouse" 正是此意）。
> 这意味着：存储探针的**全部活语义**，iceberg 连接器已经自行覆盖了 FE 侧那一半。

### 2.2 受影响的回归套件

- `external_table_p0/test_connection/test_iceberg_rest_minio_connectivity.groovy`（**p0，CI 门禁**）：
  断言 `"Iceberg REST"` + `"connectivity test failed"` + MinIO 错密钥失败 → **今天由 IcebergConnector 覆盖，OK**。
- `external_table_p2/test_connection/test_connectivity.groovy`（p2，需外部环境）：
  Test 1.1 / 2.1 断言坏 HMS uri 抛 `"connectivity test failed"` + `"HMS"` → 本分支 hive/iceberg-on-HMS
  已无 meta 探针 → **CREATE CATALOG 会成功、不抛异常 → 这两个 case 应当已经挂了**（高置信；p2 需外部环境，未实跑证实）。

---

## 三、方案

### 阶段 1（必做）：fe-core 归零 —— 纯删除，零行为变化

1. 删除整包 8 个文件：`fe/fe-core/src/main/java/org/apache/doris/datasource/connectivity/`
   （`CatalogConnectivityTestCoordinator`、`MetaConnectivityTester`、`StorageConnectivityTester`、
   `AbstractS3CompatibleConnectivityTester`、`S3ConnectivityTester`、`MinioConnectivityTester`、
   `HdfsConnectivityTester`、`HdfsCompatibleConnectivityTester`）。
2. `ExternalCatalog.java`：删 `import`（:42）+ `checkWhenCreating()`（:322-334）体内的协调器调用。
   基类 `checkWhenCreating()` 保留为空实现（`PluginDrivenExternalCatalog` 要 override 它）；
   `TEST_CONNECTION` 常量保留（`PluginDrivenExternalCatalog.java:221` 在用）。
3. 结果：fe-core 就"连通性"这条线**不再 import 任何** `S3Properties` / `MinioProperties` /
   `HdfsProperties` / `MetastoreProperties` / `TStorageBackendType`。符合铁律"fe-core 源相关代码只减不增"。

> 不做的事：**不把这 8 个文件搬进 fe-connector / fe-filesystem**。搬过去仍然是死代码
> （没有任何调用者），只会把污染换个地方（违反 Rule 2「不留投机性代码」）。

### 阶段 2（建议）：能力回填 —— 用**现成 SPI**，fe-core 不加一行

全部落在插件侧，无需新增 fe-core 代码，也无需新 SPI：

- **A. Hive meta 探针**（恢复 `HiveHMS` + `HiveGlue`）：
  `HiveConnector` 新增 `testConnection(session)` → `getMetadata(session).listDatabaseNames(session)`
  （`ConnectorSchemaOps.java:30`；等价 upstream 的 `IMetaStoreClient.getAllDatabases()`）。
  失败文案须含 `"connectivity test failed"` + `"HMS"`（p2 套件断言依赖）。约 15 行。
- **B. Iceberg 非 REST meta 探针**（恢复 `IcebergHMS` / `IcebergGlue` / `IcebergS3Tables`）：
  `IcebergConnector.testConnection():240` 去掉 `catalog.type=rest` 的 guard，
  改为所有 catalog type 都 `listDatabaseNames`（REST 分支保留额外的 warehouse 解析）。约 5 行。
- **C. 存储 FE 侧探针**：iceberg 已自建（`probeStorage`），hive 侧 upstream 本就没有 → **不做**。

### 阶段 3：测试

- 新增 `HiveConnectorTestConnectionTest`（仿 `IcebergConnectorTestConnectionTest`，断言文案稳定）。
- p2 `test_connectivity.groovy`：补完能力后应重新变绿（需外部环境实跑确认）。

---

## 四、决策与落地结果（2026-07-14，用户已拍板并已实施）

| 决策 | 选定 | 落地 commit |
|---|---|---|
| 范围 | 阶段 1 + 2（删死码 + 补 meta 探针） | `8057f45d0f2` |
| 基类 `checkWhenCreating()` | 留空实现 | `8057f45d0f2` |
| BE 存储探针 | 回填；挂 `ConnectorContext` **同步**回调（非 `ConnectorValidationContext` 延迟注册——后者注册时机早于 `testConnection()`，REST catalog 的 warehouse 尚未解析，覆盖面比 upstream 窄） | `c8f05143dc1` |
| BE `test_location` 空指针 | 顺手修（`Status::InvalidArgument`） | `c8f05143dc1` |

### 实施中发现的两个"本来会出事"的点

1. **`ExternalCatalog.checkWhenCreating()` 的构造参数是有副作用的**（对抗 agent 抓到，我原判"零行为变化"是错的）：
   `catalogProperty.getMetastoreProperties()` 对 `type=doris|test` 抛 **`IllegalArgumentException`**（非 `UserException`，
   `CatalogProperty` 只 catch 后者）→ 今天 `CREATE CATALOG ... type=doris, test_connection=true` 是**硬失败**的。
   删除后该 DDL 成功。这是**唯一**的行为变化，本质是潜伏 bug 的修复；无任何回归套件覆盖。
2. **`TcclPinningConnectorContext` 装饰器会静默吞掉新 default 方法**：两个装饰器逐方法显式委派，
   新增的 `testBackendStorageConnectivity` 若不加委派，就会落到接口的 no-op 默认实现 →
   BE 探针在**生产**里永不执行（单测抓到）。iceberg + paimon 两个装饰器均已补委派。

### 平价边界（对齐 upstream，不多不少）

iceberg 的 meta 探针范围钉死在 upstream `createMetaTester()` 的分支集合：**HMS / Glue / REST / S3Tables**。
`hadoop`（文件系统型）catalog upstream 走 no-op → 这里也不探（`IcebergConnector.probesMetastore()`，有单测钉住）。
一开始我改成"全类型都探"，被既有单测 `testConnectionSucceedsWhenNothingToProbe` 挡下——那测试是对的。

---

## 附：原始决策选项（存档）

**决策 1 — 本次范围**
- (a) 只做阶段 1（fe-core 归零），能力回填另开任务 → 最小、零风险、立即可提交
- (b) 阶段 1 + 2（推荐）→ 顺手把已发生的回退补回来，p2 套件恢复
- (c) 1 + 2 + 3 全套（含 BE 存储探针，见决策 3）

**决策 2 — 基类 `checkWhenCreating()` 怎么处理**
- (a) 留空方法（推荐）：`PluginDrivenExternalCatalog` 需要 override 点，且 `type=doris/test` 未来可能要加自己的校验
- (b) 连同基类方法一起删，把 `checkWhenCreating()` 下沉为 `PluginDrivenExternalCatalog` 独有 → 需改 `CatalogFactory.java:164` 的调用点

**决策 3 — BE 侧存储连通性探针（thrift `test_storage_connectivity`）**
它是唯一"真丢了、且插件侧没补"的能力。BE handler 仍在（`be/src/service/backend_service.h:127`），
删掉 FE 调用点后它变孤儿。
- (a) 暂不回填，单开 issue 跟踪（推荐：影响面小——它只在 iceberg + s3 warehouse 且 BE 存活时才跑过）
- (b) 用现成范式回填：给 `ConnectorValidationContext` 加 `requestBeStorageConnectivityTest(int type, Map props)`
  回调（引擎侧 `DefaultConnectorValidationContext` 发 thrift；签名保持 thrift-free，与已有的
  `requestBeConnectivityTest(byte[], int, String)` 同构），iceberg 连接器在 `preCreateValidation` 注册。
  → fe-core 只加**通用**回调（不含 fs 类型分支），符合架构目标
- (c) 连 BE handler + thrift 定义一起删 → 跨 FE/BE 改动，不建议

---

## 五、风险 / 未验证项

- p2 `test_connectivity.groovy` 的现状（是否已挂）需要外部环境实跑确认；本文按代码推断为"已挂"。
- 若选决策 1(a)（只删不补），p2 套件仍是红的，且属于**先前提交引入的回退**，不是本次删除引入 —— 但必须在 PR 描述里写明，不能默认它是"本来就那样"。
