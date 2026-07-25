# 24. 待拍板：连接器自声明属性——接线还是删除

> **优先级**：待用户拍板（决定之后才知道归入哪一批） ｜ **风险**：选项一 高 / 选项二 低 ｜ **前置依赖**：无。但**它反过来卡住 11 号任务**——11 号任务改动清单的第 1 项就是删掉本文讨论的这三个死接口，本文如果拍成「接线」，11 号必须把那一项摘出去。
> **影响模块**：选项二只动 `fe-connector-api`（删 1 个类 + 2 个默认方法）与 `fe-core`（**仅测试**，删两行断言）；选项一要动 `fe-connector-api`、`fe-core`（新增引擎侧校验与可能的新语法）、以及每个想声明属性的连接器。
> **预计改动规模**：选项二约 3 个文件、净减约 130 行；选项一保守估计 15～25 个文件、净增 600 行以上（不含语法层）。
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

公共接口 `fe-connector-api` 里躺着一套「连接器自己声明它接受哪些配置项」的机制（`ConnectorPropertyMetadata` 这个类，加上 `Connector` 上的两个取得器），它是从 Trino 直译过来的，在 Doris 这边**一个实现、一个调用点都没有**；本文把「把它接线成真机制」和「删掉它」两条路的代价、影响面、以及它到底能不能解决我们真正的痛点摊开，请你拍板选一条——**不允许的第三条是维持现状，让它继续在公共接口里当装饰**。

## 二、背景：现在的代码是怎么写的

### 2.1 这三个接口长什么样

`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorPropertyMetadata.java`，共 120 行，一个泛型不可变值对象，字段是 `name` / `description` / `type` / `defaultValue` / `required`（`:29-33`），对外只给四个静态工厂：`stringProperty`（`:46`）、`intProperty`（`:53`）、`booleanProperty`（`:60`）、`requiredStringProperty`（`:67`），另有 getter、`equals`、`hashCode`、`toString`。

`Connector.java:234-242` 上挂着两个默认方法把它返回出来：

```java
/** Returns the table-level property descriptors. */
default List<ConnectorPropertyMetadata<?>> getTableProperties() {
    return Collections.emptyList();
}

/** Returns the session-level property descriptors. */
default List<ConnectorPropertyMetadata<?>> getSessionProperties() {
    return Collections.emptyList();
}
```

全仓库对 `ConnectorPropertyMetadata` 这个类名只有 15 处命中：13 处在它自己的文件里，2 处就是上面那两行返回类型。本仓库的 8 个连接器（es / hive / hudi / iceberg / jdbc / maxcompute / paimon / trino）一个都没有覆写这两个方法。两个方法唯一的调用点在一个测试里——`fe/fe-core/src/test/java/org/apache/doris/connector/fake/FakeConnectorPluginTest.java:177-178`，断言的内容正是「它俩返回空」：

```java
Assertions.assertTrue(connector.getTableProperties().isEmpty());
Assertions.assertTrue(connector.getSessionProperties().isEmpty());
```

在 Trino 里同名机制是活的：连接器声明一批属性描述符，引擎据此校验 `CREATE TABLE ... WITH (...)` 的键是否合法、支持 `SET SESSION 目录.属性 = 值`，还能通过系统表把「这个目录接受哪些旋钮」列出来给用户看。连接器加一个可调参数完全不碰引擎。

### 2.2 撞名警告：仓库里有三个 `get*Properties`，含义各不相同

在讨论之前必须先把名字理清，否则一定会误判：

| 符号 | 位置 | 返回 | 状态 |
|---|---|---|---|
| `Connector.getTableProperties()` | `Connector.java:235` | `List<ConnectorPropertyMetadata<?>>` | **本文讨论的死接口** |
| `PluginDrivenExternalTable.getTableProperties()` | `fe-core/.../datasource/plugin/PluginDrivenExternalTable.java:768` | `Map<String,String>` | **活的**，是 `SHOW CREATE TABLE` 渲染 `PROPERTIES(...)` 的数据源（`Env.java:4881` 消费） |
| `Connector.getSessionProperties()` | `Connector.java:240` | `List<ConnectorPropertyMetadata<?>>` | **本文讨论的死接口** |
| `ConnectorSession.getSessionProperties()` | `ConnectorSession.java:89` | `Map<String,String>` | **活的且用得很重**，hive / iceberg / paimon / hudi 都在读它取会话变量 |

删除本文这两个方法，跟上面两个活的同名方法毫无关系；反过来说，看到 grep 里一堆 `getSessionProperties` 命中就以为它是活的，也是错的。

### 2.3 Doris 今天真实存在的三条属性通道

**通道一：按目录的属性（已经是连接器完全自有的，公共模块零改动）。** `ConnectorProvider.create(Map<String,String> properties, ConnectorContext context)`（`fe-connector-spi/.../ConnectorProvider.java:64`）把 `CREATE CATALOG ... PROPERTIES(...)` 的整张属性表原样交给连接器；同一张表也通过 `ConnectorSession.getCatalogProperties()`（`ConnectorSession.java:78`）在查询期可见。校验钩子也已经在连接器一侧：`ConnectorProvider.validateProperties(Map)`（同文件 `:74`，默认空实现），fe-core 在 `PluginDrivenExternalCatalog.java:212` 经 `ConnectorFactory.validateProperties` 调它，hive / iceberg / trino 三个连接器已经覆写。

这条通道**今天已经在被当作「连接器私有旋钮」用**，三个键为证，键名前缀、解析、默认值全在 hive 连接器里，fe-core 全树对这三个键字符串零命中：

| 键 | 声明处 | 读取处 |
|---|---|---|
| `hive.ignore_absent_partitions` | `HiveConnectorProperties.java:116` | `HiveScanPlanProvider.java:544-545` |
| `hive.enable_hms_events_incremental_sync` | `HiveConnectorProperties.java:102-103` | `HiveConnector.java:459-460` |
| `hive.hms_events_batch_size_per_rpc` | `HiveConnectorProperties.java:106` | `HiveConnector.java:466-468` |

**通道二：FE 全局配置（`fe.conf`）→ 逐键手工转发。** `fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java:568-596` 的 `buildEnvironment()` 把 9 个键塞进一张 map，连接器经 `ConnectorContext.getEnvironment()` 读回。其中 7 个是数据源专属的：

| 环境键 | 转发处 | `fe.conf` 字段 | 归属 |
|---|---|---|---|
| `jdbc_drivers_dir` | `:574` | `Config.java:157` | jdbc（iceberg/paimon 的 jdbc 元存储也用） |
| `force_sqlserver_jdbc_encrypt_false` | `:575-576` | `Config.java:176` | jdbc（连数据库品牌都写进键名了） |
| `jdbc_driver_secure_path` | `:577` | `Config.java:163` | jdbc |
| `hive_metastore_client_timeout_second` | `:581-582` | `Config.java:2140` | hive 元存储 |
| `hive_default_file_format` | `:587` | `Config.java:2561` | hive 建表 |
| `enable_create_hive_bucket_table` | `:588` | `Config.java:2558` | hive 建表 |
| `trino_connector_plugin_dir` | `:595` | `Config.java:2895` | trino |

另外两个 `doris_home`（`:572`）与 `doris_version`（`:591`）是中立的部署信息，不在讨论范围。`:586` 那条注释明写着这条通道的脆弱点：

```
// Keys must stay byte-identical to the reads in HiveConnectorProperties.
```

对应的读取端确实是逐字抄的常量：`HiveConnectorProperties.java:77-79` 的 `ENV_HIVE_DEFAULT_FILE_FORMAT` / `ENV_ENABLE_CREATE_HIVE_BUCKET_TABLE` / `ENV_DORIS_VERSION`。改错一个字母不报编译错，只会静默取默认值。

**通道三：会话变量。** `fe/fe-core/src/main/java/org/apache/doris/connector/ConnectorSessionBuilder.java:222-233` 的 `extractSessionProperties`：主体是 `VariableMgr.toMap(ctx.getSessionVariable())`（`:223`，整表倒出，不维护白名单，所以**新增一个会话变量本身不需要改这里**），后面另有两个手工 `put`：

- `lower_case_table_names`（`:225-226`）：是注册过的变量，但作用域是全局（`fe-common/.../qe/GlobalVariable.java:109-110`，`GLOBAL | READ_ONLY`），不在会话变量表里，所以要单独补；
- `max_compute_write_max_block_count`（`:231-232`）：**根本不是变量**，是 `fe.conf` 字段 `Config.java:2190`，被塞进了这条「会话属性」通道，注释自己承认这是借道（"same as lower_case_table_names above"）。

这与取得器自己的文档冲突：`ConnectorSession.java:81-88` 的 javadoc 说这里装的是「来自用户会话的按查询设置（例如 SET 语句），键名取自 FE 会话变量注册表」。第二个键两条都不满足。

## 三、为什么这是个问题

**第一，死接口本身的代价。** 公共接口上的每个方法都是对 8 个连接器作者的一次要求。一个零实现零消费的方法，会让读接口的人以为「原来加旋钮该走这里」，照着做完发现无人读取；而 11 号任务清单里其它死接口的教训已经说明，留着不删的接口迟早会被人当真。

**第二，也是必须纠正最初那轮调研结论的一点：这套接口即使接线，也修不了我们真正的痛点。** 「今天所有数据源专属旋钮只能落到 `fe-common` 全局配置与 `fe-core` 会话变量这条封闭路径上」这句话**是过宽的**——通道一已经存在，`hive.ignore_absent_partitions` 这三个键就是「连接器加旋钮、公共模块零改动」的既成事实。真正必须改公共模块的只剩两种情形，而它们的成因都不是「缺少描述符」：

- 旋钮的值住在 `fe.conf`（一个 `fe-common` 的部署文件），连接器无法 import `Config`，所以必须有人从 `fe-common` 搬到 map 里——搬运动作本身就是那行手工转发。补上描述符不会让这行消失，除非把旋钮**从 `fe.conf` 挪到目录属性**，而那才是真正的兼容变更。
- 旋钮想按会话生效，而 Doris 的 `SET` 语法只认注册在 `SessionVariable` 里的变量名，没有「按目录设置连接器属性」这种语法（`ALTER CATALOG ... SET PROPERTIES` 有，`AlterCatalogPropertiesCommand.java:31-36`，但那是改持久化的目录属性，不是按会话覆盖）。

**第三，真正缺的那一块（声明式校验与可发现性）今天也已经有连接器侧入口。** 目录属性的键名今天**拼错不报错**：多写一个字母静默失效，用户只会看到「设了没用」。这确实是个缺陷，但补它并不需要描述符——`ConnectorProvider.validateProperties` 就是为此存在的钩子，连接器自己在里面比对键名即可，零公共模块改动。描述符能带来的额外价值只有「统一形状」和「引擎能把可用旋钮列出来给用户看」，而后者要新增一张 fe-core 系统表，与「fe-core 只出不进」的现阶段纪律正面相撞。

**用户可见后果**：现状本身没有正确性缺陷。选项一如果做，会有用户可见后果（配置项位置变化）；选项二没有。

## 四、用一个最小例子说明

场景：**hive 连接器作者想加一个旋钮，控制「列举分区目录时用几个线程」。**

先看三种作用域今天各要动什么：

| 我希望这个旋钮怎么设 | 今天必须动的文件 | 涉及模块数 |
|---|---|---|
| 按目录（写在 `CREATE CATALOG` 里） | 只有 `HiveConnectorProperties.java`（加常量）+ 读取处 | **1 个（连接器自己）** |
| 全 FE 一份（写在 `fe.conf` 里） | `fe-common/Config.java` 加字段、`DefaultConnectorContext.buildEnvironment` 加一行转发、连接器加常量与读取 | 3 个 |
| 按会话（`SET`） | `fe-core/SessionVariable` 加字段（`VariableMgr.toMap` 会自动带上）、连接器加常量与读取 | 2 个 |

也就是说，**「零公共模块改动」这个世界在按目录这一档已经到手了**：

```sql
-- 今天就能这么用：键名、默认值、解析全在 hive 连接器里，fe-core 完全不知道这个键存在
CREATE CATALOG hive_a PROPERTIES ("type" = "hms", "hive.ignore_absent_partitions" = "false");
ALTER CATALOG hive_a SET PROPERTIES ("hive.hms_events_batch_size_per_rpc" = "1000");
```

今天做不到的是这两件事：

```sql
-- ① 拼错键名：少写一个 s，语句成功、没有任何提示，旋钮静默失效
CREATE CATALOG hive_b PROPERTIES ("type" = "hms", "hive.ignore_absent_partition" = "false");

-- ② 按会话临时覆盖某个目录的连接器旋钮：Doris 没有这个语法，直接报语法错误
SET SESSION hive_a.ignore_absent_partitions = false;
```

两个选项各自能带来什么：

| | 选项一（接线成声明式属性） | 选项二（删掉三个死接口） |
|---|---|---|
| 上表第一行「按目录」 | 已经零改动，接线后再加一行描述符声明 | 不变，仍然零改动 |
| ① 拼错键名静默失效 | 引擎按描述符统一拒绝未知键 | 仍可修，但走 `validateProperties`，由连接器自己拒 |
| ② 按会话覆盖目录旋钮 | 需要**同时**新增 SQL 语法与 fe-core 解析，描述符只是其中一环 | 不做 |
| 那 7 个 `fe.conf` 键的手工转发 | **不会消失**，除非把键搬到目录属性（兼容变更） | 不变 |

## 五、解决方案

### 5.1 目标状态

**选项一：接线成 Trino 那样的声明式属性。**

保留 `ConnectorPropertyMetadata`，把两个取得器接成真通道。要成立至少需要三件事同时落地：

1. **描述符成为目录属性的合法键来源**。签名可以不变，但语义要从「表级/会话级」纠正为「目录级」，因为 Doris 的对应物是 `CREATE CATALOG ... PROPERTIES`：
   ```java
   /** 本连接器接受的目录级属性；引擎据此拒绝未知键并填默认值。 */
   default List<ConnectorPropertyMetadata<?>> getCatalogPropertyMetadata() {
       return Collections.emptyList();
   }
   ```
   注意：校验发生在 `CREATE CATALOG`，那时 `Connector` 实例还没建好，只有 `ConnectorProvider`，所以这个取得器**必须挂在 `ConnectorProvider` 上而不是 `Connector` 上**——这意味着两个现有方法本来的位置就是错的，接线也要搬家。
2. **未知键的处置要是可开关的**。存量目录里可能已经存了拼错的或第三方工具塞进去的键，直接改成硬拒会让 FE 重启后加载不了既有目录。必须是「先告警、可配置升级为拒绝」的两段式。
3. **兼容承诺**：那 7 个 `fe.conf` 键一个都不能改名、不能失效。如果同时想把它们变成目录属性，只能是「目录属性优先、缺失时回落到 `fe.conf` 值」的叠加语义，`fe.conf` 键至少保留若干个版本并在文档标记为过时。

至于「按会话覆盖某目录的属性」（`SET SESSION 目录.属性`），需要新增 SQL 语法、解析、会话态存储与生命周期，**属于引擎语法层的独立工程，不应塞进这个决策里**；建议明确排除在本次范围之外。

**选项二：删掉三个死接口，把入口形状写进设计文档。**

`ConnectorPropertyMetadata.java` 整文件删除，`Connector.java:234-242` 的两个默认方法删除，`FakeConnectorPluginTest.java:177-178` 两行断言删除（该测试方法其余断言保留）。同时在 7 号任务产出的包级说明里补一段「连接器旋钮该放哪」的规则：按目录的旋钮走 `CREATE CATALOG` 属性 + `ConnectorProvider.validateProperties` 自校验（首选）；必须全 FE 一份的走 `fe.conf` + `buildEnvironment` 转发，并在两侧注释里互指键名；未来若要做声明式属性，正确的入口是 `ConnectorProvider` 上的目录级描述符取得器，而不是 `Connector` 上的表级/会话级取得器。

**我的推荐：选项二。** 理由三条。第一，这套接口即使接线也不解决我们真实的两个卡点（值住在 `fe.conf`、没有按会话覆盖语法），它承诺的东西和我们缺的东西不重合；第二，它现在挂错了位置——目录属性校验发生在 `Connector` 存在之前，照现有签名接线一定要重新设计，那已经是「新做一套」而不是「接线」，把死接口留在原地并不能省下这份设计；第三，它现在挡住的那个真缺陷（拼错键静默失效）有一个成本低得多的现成入口 `validateProperties`，可以独立立项，不需要引入引擎侧的属性校验框架。

如果你倾向选项一，我的建议是**先只做第 1 件事（目录级描述符 + 未知键告警）**，把 `fe.conf` 键的搬迁和按会话覆盖语法各自单独立项，避免一次改动同时动到用户可见的配置位置和 SQL 语法。

### 5.2 改动清单

**选项二（推荐）的改动清单：**

| 序号 | 文件 | 做什么 |
|---|---|---|
| 1 | `fe-connector-api/.../api/ConnectorPropertyMetadata.java` | 整文件删除 |
| 2 | `fe-connector-api/.../api/Connector.java`（`234-242`） | 删两个默认方法；`List` / `Collections` 的 import 视文件内其它用法决定去留（该文件其它方法仍在用，预计都保留） |
| 3 | `fe-core/src/test/.../connector/fake/FakeConnectorPluginTest.java`（`177-178`） | 删两行断言，`connectorTopLevelDefaults` 其余断言保留 |
| 4 | 7 号任务的包级说明文档 | 补一段「连接器旋钮该放哪」的规则与「未来入口形状」的记录 |

选完这条后，请把 11 号任务改动清单的第 1 项标注为「由 24 号任务落地」或反之，两者只做一次，避免重复删除造成冲突。

**选项一的改动清单（只列骨架，正式做之前需要单独出一份施工文档）：**

| 序号 | 文件 | 做什么 |
|---|---|---|
| 1 | `fe-connector-spi/.../spi/ConnectorProvider.java` | 新增目录级描述符取得器（默认空列表） |
| 2 | `fe-connector-api/.../api/Connector.java` | 删掉现有两个挂错位置的取得器 |
| 3 | `fe-connector-api/.../api/ConnectorPropertyMetadata.java` | 保留；`description` 的用途要落地（否则它仍是死字段） |
| 4 | `fe-core/.../connector/ConnectorPluginManager.java`（`161-170`） | 在既有 `validateProperties` 调用链上加一段「按描述符检查未知键」，默认只告警 |
| 5 | `fe-common/.../Config.java` | 新增一个开关，把未知键从告警升级为拒绝 |
| 6 | 各连接器的 `*ConnectorProperties` / `*ConnectorProvider` | 逐个把已有的目录属性键改写成描述符声明（hive 至少 3 个键，iceberg / paimon / jdbc / trino / es 待清点） |
| 7 | 各连接器与 `fe-connector-api` 的测试 | 新增「未知键被拒/被告警」「默认值生效」的断言 |
| 8 | 用户文档 | 说明新的校验行为与开关 |

### 5.3 明确不要顺手做的事

- **不要顺手改那两个活的同名方法**（`PluginDrivenExternalTable.getTableProperties()`、`ConnectorSession.getSessionProperties()`）。前者是 `SHOW CREATE TABLE` 的数据源，后者 8 个连接器在读，与本文毫无关系。
- **不要顺手清理 `buildEnvironment` 的 7 个转发键**。它们是迁移前既有的 `fe.conf` 名字，删任何一个都是用户可见的兼容破坏；把它们改成目录属性是独立议题，需要单独的兼容设计与文档。
- **不要顺手把 `max_compute_write_max_block_count` 从会话通道搬到环境通道**——虽然它确实放错了地方（它是 `fe.conf` 字段却走了会话属性通道）。搬家会改变 maxcompute 连接器的读取位置，属于独立的一次修正，且要连带修 maxcompute 侧的读取常量与测试；本文只负责把这件事记录下来。
- **不要在本任务里做「按会话覆盖目录属性」的 SQL 语法**。那是引擎语法层的独立工程，混进来会让这个决策无法评估。
- **不要为「描述符必须被声明」写 shell 或正则静态门禁**。判断一个连接器是否声明了描述符属于语言语义范畴，本仓库已有结论：这类门禁误报比漏报更毒。

## 六、怎么验证

**选项二的验证：**

1. **全反应堆含测试源编译**（唯一能一次证明「引用全清」的动作，禁止任何跳过测试编译的参数）：
   ```
   mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -T 1C test-compile
   ```
   `BUILD SUCCESS` 之外任何 symbol not found 当场处理，不许注释掉测试绕过。
2. **单测**（必须关掉构建缓存，否则测试会被静默跳过而仍报 `BUILD SUCCESS`）：
   ```
   mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -Dmaven.build.cache.enabled=false \
       -pl fe-core test -Dtest=FakeConnectorPluginTest
   ```
   删掉两行断言后，`connectorTopLevelDefaults` 必须仍然覆盖 `getScanPlanProvider()` 为空、`getCapabilities()` 为空、`defaultTestConnection()` 为 false、`testConnection()` 成功这几条——如果删完只剩一两条，就是把测试意图一起删掉了。
3. **删除彻底性自查**（人工一次，不做成门禁）：全仓 grep `ConnectorPropertyMetadata` 期望零命中；grep `getTableProperties` / `getSessionProperties` 期望**只剩** 2.2 节表里那两个活的方法及其调用点。
4. **端到端回归：不需要。** 删掉的路径在生产上恒为空列表，无任何运行时行为。
5. **不需要变异验证。** 没有被保护的行为可供变异。

**选项一的验证（若拍这条，正式施工文档里要展开）：**

- 单测要断言的核心不变量是三条：声明过的键被接受并按 `defaultValue` 填默认；未声明的键在告警模式下**目录仍能创建成功**（这条最重要，它保护的是存量目录能被加载）；开关打开后未声明的键被拒且报错文案里带上键名。
- 必须做一次变异验证：手工把校验改成恒通过，「未知键被拒」那条用例必须变红；如果不红，说明测试没有真正走到校验。
- 端到端回归是**硬性要求**（需本地集群）：至少覆盖「用旧 `fe.conf` 键的既有目录重启 FE 后仍能加载并查询」，以及 hive 那三个既有目录属性键的行为不变。这是选项一与选项二在验证成本上的关键差别。

## 七、风险与回退

**选项二的风险：低。** 删的是恒空列表，运行时零行为。唯一的实质风险是**判断失误**——如果将来确实要做声明式属性，得重新写这个类。但 5.1 节的分析表明现有签名挂错了位置（校验时点没有 `Connector` 实例），将来那次工作无论如何都要重新设计入口，删除并不增加成本。回退就是 `git revert`，无数据、无持久化、无有线格式牵连。

**选项一的风险：高**，且集中在两处：

- **存量目录加载**。如果未知键处置一步到位改成硬拒，任何一个既有目录里存着连接器没声明的键（历史遗留、拼错、第三方工具写入），FE 重启后就加载不了这个目录。这是「重启才炸」的类型，测试环境不一定复现。所以两段式（默认告警、开关升级为拒绝）不是可选项而是必需项。
- **配置项位置的兼容承诺**。7 个 `fe.conf` 键一旦对外宣布「已改为目录属性」，就不能反悔。回退代价远高于代码回退——文档、用户脚本、运维手册都会跟着走。

无论选哪条，本文档的决定都要回写到 `plan-doc/connector-public-interface-cleanup/HANDOFF.md` 的待拍板段落与 `README.md` 的任务表，并同步调整 11 号任务的改动清单第 1 项。

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md`
  - 第 4.5 节「一个尚未排期、需要先拍板的落点：连接器自声明属性」——本文的出处；注意其中「所有数据源专属旋钮只能落到全局配置与会话变量」的判断经本文核实**过宽**，按目录的通道已经存在。
  - 第 7.2 节「可以直接删」表格第一行——把这三个接口列入直接删除项，与本文选项二一致。
  - 附录 A 第 43 / 47 / 55 / 91 条——同一处发现的四次独立命中（属性描述符体系零实现零消费），其中 55 与 91 已经注意到了 2.2 节说的撞名问题。
- `plan-doc/connector-public-interface-cleanup/tasks/11-delete-dead-surface-batch-one.md` 的改动清单第 1 项——**与本文选项二是同一件事，只能做一次**。
- `plan-doc/connector-public-interface-cleanup/tasks/07-write-down-the-design-rules.md`——选项二要补的那段「连接器旋钮该放哪」的规则应落在这份包级说明里。
- `plan-doc/connector-public-interface-cleanup/tasks/06-fix-engine-context-forwarding-gap.md`——同样在讨论 `ConnectorContext` 这条通道（那份讲的是转发漏抄，本文讲的是转发内容），两者不冲突。
