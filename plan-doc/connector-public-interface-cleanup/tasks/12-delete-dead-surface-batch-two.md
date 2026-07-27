# 12. 删除第二批死接口面（需要连带修改连接器）

> **优先级**：第三优先级（删死面） ｜ **风险**：中 ｜ **前置依赖**：11 号任务（第一批删除，只动公共模块内部；先做它可以避开同文件的改动冲突，不是逻辑依赖）
> **影响模块**：`fe-connector-api`、`fe-connector-hive`、`fe-connector-hudi`、`fe-connector-iceberg`、`fe-connector-jdbc`、`fe-connector-paimon`、`fe-connector-maxcompute`、`fe-core`（**只改测试**）
> **预计改动规模**：约 18 个文件，净减少 200～260 行；其中约一半是单测的删除与改写
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

把四组「引擎从来不调用」的公共接口面从 `fe-connector-api` 删掉，并连带删掉各连接器为它们写的实现和单测；其中建表的旧宽度重载不只是死代码，它的降级默认会**静默丢掉分区信息**，是留给下一个新连接器的陷阱。

## 二、背景：现在的代码是怎么写的

### 2.1 连接器级属性取得器 `ConnectorMetadata.getProperties`

`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorMetadata.java:53-56`：

```java
    /** Returns connector-level properties. */
    default Map<String, String> getProperties() {
        return Collections.emptyMap();
    }
```

五个连接器覆写了它：`HiveConnectorMetadata.java:591`、`HudiConnectorMetadata.java:393`、`IcebergConnectorMetadata.java:806`、`JdbcConnectorMetadata.java:186`、`PaimonConnectorMetadata.java:452`（paimon 的覆写直接 `return Collections.emptyMap();`，即为满足接口而写的空覆写）。

引擎侧没有任何调用方。`fe-core` 主源里 `getProperties()` 的全部命中都属于**别的对象**：`ConnectorTableSchema`（`PluginDrivenExternalTable.java:521`）、`ConnectorDatabaseMetadata`（`PluginDrivenExternalDatabase.java:86`）、`CatalogProperty`、`CreateTableInfo` 等。名字撞车正是这一条难被发现的原因。

`ConnectorMetadata` 是**每语句/每次取用**构造出来的对象（`PluginDrivenMetadata.get(session, connector)`），而这个方法叫「连接器级属性」——挂错了层次。

### 2.2 分区值枚举 `ConnectorTableOps.listPartitionValues`

`ConnectorTableOps.java:499`（上方注释写着 "Used by the `partition_values()` TVF and by column-distinct-value optimizations"）：

```java
    default List<List<String>> listPartitionValues(ConnectorSession session,
            ConnectorTableHandle handle,
            List<String> partitionColumns) {
        return Collections.emptyList();
    }
```

三个连接器实现了它：`HudiConnectorMetadata.java:690`、`PaimonConnectorMetadata.java:1146`、`MaxComputeConnectorMetadata.java:300`。三份实现都在做同一件事：拿到分区列表，再按调用方给的列顺序把值投影成二维表。

而注释里说的两个用途，代码上都不经过它：

- `partition_values()` 表函数：`MetadataGenerator.java:2035` → `PluginDrivenExternalTable.getNameToPartitionValues`（`PluginDrivenExternalTable.java:882`）→ **`metadata.listPartitions(...)`**（`:898-899`），FE 侧再按分区列名投影。
- 分区系统表：`MetadataGenerator.dealPluginDrivenCatalog` → **`metadata.listPartitionNames(...)`**（`MetadataGenerator.java:1270`）。

也就是说「列分区」在公共接口上有三套（`listPartitions` / `listPartitionNames` / `listPartitionValues`），真正被引擎用的是前两套。

### 2.3 建表与删库各一个旧宽度重载

`ConnectorTableOps.java:222-228` 是窄形态，`:231-249` 是宽形态，宽形态的默认实现会**降级**到窄形态：

```java
    /** Creates a new table with the given schema and properties. */
    default void createTable(ConnectorSession session,
            ConnectorTableSchema schema, Map<String, String> properties) {
        throw new DorisConnectorException("CREATE TABLE not supported");
    }
    ...
    default void createTable(ConnectorSession session, ConnectorCreateTableRequest request) {
        ConnectorTableSchema schema = new ConnectorTableSchema(
                request.getTableName(), request.getColumns(), null, request.getProperties());
        createTable(session, schema, request.getProperties());   // 分区/分桶/EXTERNAL/IF NOT EXISTS 就在这里蒸发
    }
```

`ConnectorSchemaOps.java:69-75` 与 `:76-86` 是同一形状：三参 `dropDatabase(session, dbName, ifExists)` 抛「不支持」，四参 `dropDatabase(..., force)` 默认丢掉 `force` 再转给三参。

实际实现与调用情况（已全仓核实）：

| | 窄形态实现方 | 宽形态实现方 | 引擎调用的形态 |
|---|---|---|---|
| `createTable` | **零** | hive `:1569`、iceberg `:949`、paimon `:831`、maxcompute `:379` | 宽形态（`PluginDrivenExternalCatalog.java:455`） |
| `dropDatabase` | **零** | hive `:1537`、iceberg `:895`、paimon `:945`、maxcompute `:468` | 宽形态（`PluginDrivenExternalCatalog.java:553`） |

### 2.4 主键：两套并存的机制，两套都没有读取方

- `ConnectorTableOps.getPrimaryKeys`（`:416-420`）：只有 jdbc 实现（`JdbcConnectorMetadata.java:259-261`，转给连接器内部的 `JdbcConnectorClient.getPrimaryKeys`）。引擎零调用，唯一调用点是 `fe-core` 的默认值测试 `FakeConnectorPluginTest.java:123`。
- `ConnectorTableSchema.PRIMARY_KEYS_KEY`（`ConnectorTableSchema.java:80`，值为内部前缀 + `primary_keys`）：只有 paimon 写入（`PaimonConnectorMetadata.java:356-358`）。它同时被登记在 `RESERVED_CONTROL_KEYS`（`ConnectorTableSchema.java:118-120`）里，而 `fe-core` 会把这个集合里的键从 `SHOW CREATE TABLE` 的 `PROPERTIES(...)` 里**全部剥掉**——所以这条链路是「写进去，然后被删掉」，没有第三个消费者。

补充两个必须交代清楚的事实：

1. **流式作业的主键不走这套接口**：它用的是 `fe-core` 自己的遗留 JDBC 客户端（`StreamingJobUtils.java:405` → `JdbcClient.java:426`），与连接器 SPI 无关，删除不影响它。
2. **paimon 用户可见的主键属性另有一行**：`PaimonConnectorMetadata.java:341` 把 paimon 自己的 `primary-key` 选项写进表属性（这是 `SHOW CREATE TABLE` 要显示的东西），**这一行不动**。

## 三、为什么这是个问题

1. **死方法在向每个新连接器收税。** 一个新连接器作者读接口时要判断这四组要不要实现；`listPartitionValues` 还带一条「内层列表顺序必须与入参列顺序一致」的契约，三个连接器各自认真实现了一遍（注释里互相引用对方），产出的结果没有任何人读。
2. **文档把人指向错的地方。** `listPartitionValues` 的注释说它服务 `partition_values()` 表函数，实际那条路走 `listPartitions`。照文档实现的连接器会发现「我实现了但功能不生效」，然后去引擎里找不到调用点。
3. **建表的降级默认是一个正确性陷阱。** 它今天不触发（没人实现窄签名），但它是留给下一个连接器的地雷：只实现窄签名，`CREATE TABLE ... PARTITION BY ...` 会**建表成功且不报错**，分区、分桶、`EXTERNAL`、`IF NOT EXISTS` 全部静默丢失。删库那条同理：`force` 被默认丢掉后，`DROP DATABASE ... FORCE` 会变成非级联删除。
4. **主键有两条并行通道且都没有读取方。** 新连接器作者要在「实现 `getPrimaryKeys`」和「写 `PRIMARY_KEYS_KEY`」之间猜，而两条都不通。
5. **命名把层次搞错了。** 「连接器级属性」挂在每会话重建的元数据对象上，且与三个同名不同义的取得器混在一起。

### 顺带暴露的一个既存事实（本任务不修，需要拍板）

hudi 连接器的缓存契约注释与单测把「`partition_values()` 表函数」标成走 `listPartitionValues` 并要求**绕过缓存**取最新（`HudiConnectorMetadata.java:692`、`HudiConnectorHmsCacheTest.java:38-45` 与 `:80-91`）。但真实的表函数路径走 `listPartitions`，而 hudi 的 `listPartitions` 是**读缓存**的（`HudiConnectorMetadata.java:671-674`）。所以 hudi 的 `partition_values()` 今天最多可能落后一个缓存 TTL —— 这是删除动作揭出来的既有行为，与本次删除无因果关系。本任务只负责**不要把错的映射留在注释和测试里**，是否要把这条路径改成取最新，另开一项、由人决定。

## 四、用一个最小例子说明

假设明天有人新增一个连接器 X，他读接口时看到两个 `createTable`，选了参数少的那个实现（这是最自然的选择：窄签名的文档是 "Creates a new table with the given schema and properties"，看不出它缺什么）。用户执行：

```sql
CREATE TABLE x_catalog.db1.orders (
    id   INT,
    dt   DATE
)
PARTITION BY LIST (dt) ()
PROPERTIES ("file_format" = "parquet");
```

| 用户写了什么 | 今天实际发生什么 | 应该发生什么 |
|---|---|---|
| 带 `PARTITION BY LIST (dt)` 建表 | 建表**成功**，返回 OK；远端表**没有分区**（宽形态默认把 `partitionSpec` 丢在半路），`SHOW PARTITIONS` 空 | 要么按分区建表，要么明确报错 |
| `IF NOT EXISTS` / `EXTERNAL` | 同样被静默丢弃：重复建表报「表已存在」而不是静默返回 | 按语义生效 |
| `DROP DATABASE db1 FORCE` | `force` 被默认丢弃 → 走非级联删除 → 库非空时远端报错，用户看到的是「删不掉」 | 按 `FORCE` 级联删除，或明确报「不支持」 |

删掉窄签名之后，连接器 X 的作者在编译期就只看到一个入口，参数里明摆着 `partitionSpec` / `bucketSpec` / `isIfNotExists`；他不实现就会得到清晰的 `CREATE TABLE not supported`，而不是一张少了分区的表。

## 五、解决方案

### 5.1 目标状态

`fe-connector-api` 上四处删除 + 两处「把抛出点搬进保留的宽形态」：

```java
// ConnectorMetadata：整段删除 getProperties（连注释）

// ConnectorTableOps：删除 listPartitionValues、删除窄 createTable、删除 getPrimaryKeys；
// 宽形态自己抛出，不再降级：
    /**
     * Creates a table with full DDL semantics (partition, bucket, external, IF NOT EXISTS).
     * Connectors that support CREATE TABLE override this.
     * @throws DorisConnectorException if the connector cannot create tables
     */
    default void createTable(ConnectorSession session, ConnectorCreateTableRequest request) {
        throw new DorisConnectorException("CREATE TABLE not supported");
    }

// ConnectorSchemaOps：删除三参 dropDatabase；四参自己抛出：
    default void dropDatabase(ConnectorSession session,
            String dbName, boolean ifExists, boolean force) {
        throw new DorisConnectorException("DROP DATABASE not supported");
    }

// ConnectorTableSchema：删除 PRIMARY_KEYS_KEY 常量，并从 RESERVED_CONTROL_KEYS 的列表里摘掉
```

异常文案保持与今天**逐字一致**（`"CREATE TABLE not supported"` / `"DROP DATABASE not supported"`），这样既有的错误路径断言不会因为措辞而变化。

### 5.2 改动清单

| 文件 | 做什么 |
|---|---|
| `fe-connector-api/.../ConnectorMetadata.java:53-56` | 删除 `getProperties`（如 `Map` / `Collections` 因此不再被引用，同步清 import；checkstyle 有 `UnusedImports`） |
| `fe-connector-api/.../ConnectorTableOps.java:222-228` | 删除窄 `createTable`；把「不支持」抛出移入 `:241` 的宽形态并去掉降级构造 |
| `fe-connector-api/.../ConnectorTableOps.java:416-420` | 删除 `getPrimaryKeys` |
| `fe-connector-api/.../ConnectorTableOps.java:492-503` | 删除 `listPartitionValues` 及其注释 |
| `fe-connector-api/.../ConnectorSchemaOps.java:69-75` | 删除三参 `dropDatabase`；把抛出移入 `:82` 的四参形态 |
| `fe-connector-api/.../ConnectorTableSchema.java:76-80, 118-120` | 删除 `PRIMARY_KEYS_KEY` 常量与它在 `RESERVED_CONTROL_KEYS` 里的登记 |
| `fe-connector-api/.../ddl/ConnectorCreateTableRequest.java:28-35` | 类注释里「相对旧签名多带了哪些信息」的表述改写（旧签名将不存在） |
| `fe-connector-hive/.../HiveConnectorMetadata.java:591-593` + `:204` + `:278` | 删覆写；此处 `properties` 字段除该取得器外**无人读取**，同时删字段与 `this.properties = properties;`。**构造函数签名一律不动**（全仓 31 处构造点），最宽那个构造器的 `properties` 形参因此成为未用形参——这是刻意的取舍，见 5.3 |
| `fe-connector-hudi/.../HudiConnectorMetadata.java:393-395` | 删覆写（`properties` 字段在增量读、`use_hive_sync_partition` 等处仍在用，**保留**） |
| `fe-connector-iceberg/.../IcebergConnectorMetadata.java:806-808` | 删覆写（`properties` 字段在 `:835` 等处仍在用，**保留**） |
| `fe-connector-jdbc/.../JdbcConnectorMetadata.java:186-188` | 删覆写（`properties` 字段在构造 thrift 描述符等处大量在用，**保留**） |
| `fe-connector-jdbc/.../JdbcConnectorMetadata.java:259-261` | 删 `getPrimaryKeys` 覆写（内部客户端的同名方法**不动**，见 5.3） |
| `fe-connector-paimon/.../PaimonConnectorMetadata.java:452-454` | 删空覆写 |
| `fe-connector-paimon/.../PaimonConnectorMetadata.java:356-358` | 删 `PRIMARY_KEYS_KEY` 写入；`:341` 的 `CoreOptions.PRIMARY_KEY` 写入**保留** |
| `fe-connector-paimon/.../PaimonConnectorMetadata.java:1146-1161` | 删 `listPartitionValues`；同步修 `:105`、`:1080`、`:1094`、`:1165` 与 `:318-322` 注释里对它的引用（含「三个枚举钩子共享一份缓存」改为两个） |
| `fe-connector-hudi/.../HudiConnectorMetadata.java:689-706` | 删 `listPartitionValues`；同步修 `:709-711`、`:726` 注释（`collectPartitions` 的服务对象从三个变两个，且**不要**再把 `partition_values()` 写成走这条路） |
| `fe-connector-maxcompute/.../MaxComputeConnectorMetadata.java:299-315` | 删 `listPartitionValues`；同步修 `MaxComputePartitionCache.java:41-43` 注释（三个消费者 → 两个） |

单测改动集中列在第六节（它们是本任务的验收面，不只是「跟着改」）。

### 5.3 明确不要顺手做的事

- **不要动 jdbc 连接器内部客户端的 `getPrimaryKeys`**（`JdbcConnectorClient.java:433`、`JdbcMySQLConnectorClient.java:215`、`JdbcOceanBaseConnectorClient.java:134`）。它们是连接器内部对 JDBC `DatabaseMetaData` 的封装（含 MySQL 的 `KEY_SEQ` 重排等方言处理），不属于公共接口面。删掉 SPI 覆写后它们暂时没有调用者，是否清理属于 jdbc 连接器自己的事，另开一项。
- **不要动任何连接器构造函数的签名。** `HiveConnectorMetadata` 有 31 处构造点，为消掉一个未用形参去改签名，风险远大于收益。
- **不要动 paimon 写给用户看的 `primary-key` 属性**（`PaimonConnectorMetadata.java:341`）——它是 `SHOW CREATE TABLE` 的输出内容。
- **不要顺手改 hudi `partition_values()` 的新鲜度语义**（第三节末的那条）。本任务只修注释与测试里的错映射，不改缓存路由。
- **不要动 `ConnectorCreateTableRequest` 的字段。** 其中 `isExternal` 的删除属于第一批（11 号任务）。
- **不要给保留下来的宽签名新增 `supportsXxx()` 能力位。** 本任务是纯删除，不新增接口面；建表能力的声明方式是 18 号任务的事。
- **不要改 `fe-core` 的生产代码。** 本任务在 `fe-core` 只改测试（`fe-core` 只出不进）。
- **不要为「零调用方」这个结论加静态门禁。** 本仓库已有结论：shell/正则门禁只适合存在性与前缀类不变量。

## 六、怎么验证

### 6.1 编译门禁（最强单一信号）

```bash
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -T1C test-compile
```

必须含测试源，**禁用 `-Dmaven.test.skip=true`**。删除公共接口方法后，任何漏改的连接器实现（`@Override` 找不到父方法）与漏改的测试调用点都在这里报错——这正是本任务的兜底。

### 6.2 单元测试要断言什么

跑测试一律加 `-Dmaven.build.cache.enabled=false`（否则 surefire 会被静默跳过，`BUILD SUCCESS` 是空的）。

**`fe-core`（只改测试）**

| 测试 | 怎么改 |
|---|---|
| `FakeConnectorPluginTest.java:117-126`（`tableOpsListDefaults`） | 去掉 `getPrimaryKeys` 那一行断言 |
| `FakeConnectorPluginTest.java:128-139`（`partitionListingDefaultsToEmpty`） | 去掉 `listPartitionValues` 断言，注释里的「三个枚举默认值」改成两个 |
| `FakeConnectorPluginTest.java:140-157`（`createTableRequestDefaultDegradesToLegacy`） | **改写而非删除**：改名为「未实现建表的连接器收到宽形态请求时直接抛出」，断言消息为 `CREATE TABLE not supported`，并写明为什么不再有降级（降级会静默丢分区）。这是本任务唯一的行为断言 |
| 同文件，新增一条 | 四参 `dropDatabase` 默认抛 `DROP DATABASE not supported`（今天没有这条覆盖，删掉三参之后必须补上，否则抛出点搬家没有测试托底） |
| `PluginDrivenExternalTablePartitionTest.java:245, 261` 与 `PluginDrivenExternalTableTest.java:934, 951` | 这两处用 `PRIMARY_KEYS_KEY` 作为「保留键会被剥掉」的样本。**不要整段删测试**，把样本换成另一个仍存在的保留键（如 `DISTRIBUTION_COLUMNS_KEY`），保持原意图不变 |

**paimon**

| 测试 | 怎么改 |
|---|---|
| `PaimonConnectorMetadataPartitionTest.java:212`（`listPartitionValuesUsesRequestedColumnOrderWithRenderedValues`） | 删除整个测试方法 |
| 同文件 `:391` | 删掉那一行 `listPartitionValues` 断言，保留 `listPartitions` / `listPartitionNames` 的「未分区表不碰远端」断言 |
| `PaimonConnectorMetadataPartitionViewCacheTest.java:288`（`listPartitionValuesCachesAcrossQueries`） | 删除整个测试方法 |
| 同文件 `:311`（`allThreeHooksShareOneCacheEntry`） | 改成两个钩子共享一份缓存条目：删掉 values 相关断言并改方法名与注释；**`loadCount == 1` 这条断言必须保留**（它是分区视图缓存的核心不变量） |
| 同文件 `:338`（`unpartitionedNamesAndValuesBypassCacheWithoutTouchingSnapshotSeam`） | 去掉 values 那一行，其余不动 |

**hudi**

| 测试 | 怎么改 |
|---|---|
| `HudiConnectorPartitionListingTest.java:174-182`（`listPartitionValuesProjectsRequestedColumnOrder`） | 删除整个测试方法 |
| `HudiConnectorHmsCacheTest.java:80-91`（`partitionValuesTvfListsFresh`） | 删除该测试，并把类注释 `:38-45` 里「`partition_values()` 表函数 = `listPartitionValues`，必须取最新」这句改成据实描述：用户面枚举取最新的是 `listPartitionNames`（`SHOW PARTITIONS`），`partition_values()` 实际走 `listPartitions`（读缓存）。**注释要留下这个事实，不要一删了之**，否则下一个人会重新写回错的映射 |

**maxcompute**：无测试引用 `listPartitionValues`（已全仓核实），只改主源与注释。

### 6.3 变异验证（确认新增/改写的断言真的能红）

- 把宽 `createTable` 的抛出改回「构造一个 schema 后什么都不做」→ `fe-core` 那条改写后的断言必须变红。
- 把四参 `dropDatabase` 的抛出去掉 → 新增的那条断言必须变红。
- 把 paimon 的某个枚举钩子改成绕过共享缓存直接列举 → 改写后的缓存测试 `loadCount == 1` 必须变红。

### 6.4 端到端回归

本任务不改变任何用户可见行为（删掉的都是零调用方；`PRIMARY_KEYS_KEY` 本来写进去就被剥掉），**不需要新增 e2e**。建议在有集群的时机顺带跑一遍既有的分区枚举与建表用例确认零变化：`regression-test/suites/external_table_p0/hive/test_hive_partition_values_tvf.groovy`、`auth_p0/test_partition_values_tvf_auth.groovy`，以及 hive / iceberg / paimon 的建表与 `SHOW CREATE TABLE` 用例。e2e 本地跑不了，需要真集群。

## 七、风险与回退

- **回退**：单个 commit，纯删除 + 测试改写，`git revert` 即可完整回退，没有数据面或元数据面的残留。
- **不涉及持久化与有线格式**：删掉的都是 FE 内部的接口方法与一个 FE 内部属性键（该键写入后就被 `fe-core` 剥掉，从不出现在 `SHOW CREATE TABLE`，也不下发 BE），与 Gson 持久化的类型标签、thrift 字段无关。
- **主要风险是测试改写误伤意图**：`PRIMARY_KEYS_KEY` 在两个 `fe-core` 测试里是「保留键会被剥掉」的样本，必须换样本而不是删测试；paimon 的缓存测试必须保住 `loadCount == 1`。这两点在第六节已点名。
- **次要风险是漏改**：由全反应堆含测试源的 `test-compile` 兜住（删除接口方法会让漏改处编译失败），这是删除类任务最可靠的信号。
- **遗留的未用形参**：hive 最宽构造器的 `properties` 形参在删掉字段后不再被使用。选择保留是为了不动 31 处构造点；如果评审要求清掉，应作为独立改动做，不要塞进本任务。

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md`
  - 第七章 7.1 节：死接口面为什么值得删（三种真实伤害）。
  - 第七章 7.3 节：本任务的四组条目（另两组——分片类型枚举族、推模型缓存失效接口——分别是 13 与 14 号任务）。
  - 第七章 7.4 节：为什么不走「先加过时标注、下个版本再删」，以及 `getPrimaryKeys` 属于「判断题不是事实题」的那一条——若最终决定保留主键接口，则**必须**同时补契约文档并让至少一个连接器真正消费它，不允许维持「零消费 + 零文档」的现状。
- 相邻任务：11 号（第一批删除，建议先做以避开同文件冲突）、13 号、14 号（同为删死面）、24 号（连接器自声明属性的决策文档，与 2.1 节的「连接器级属性」命名问题相邻但不重叠）。
- `plan-doc/connector-public-interface-cleanup/HANDOFF.md`：构建与验证的坑（maven build cache 静默跳过测试、绝对路径 `-f`、禁止 `git add -A` 等）。
