# 15. 删除目录类型白名单，让注册了插件的连接器真的能被路由到

> **优先级**：第四优先级（兑现承诺） ｜ **风险**：中 ｜ **前置依赖**：无（本任务不依赖前面任何一号任务；它只改目录创建这条路径，与公共接口的删除批次没有文件重叠）
> **影响模块**：`fe-connector-spi`（加一个带默认实现的方法）、`fe-connector-hudi`（覆写它）、`fe-core`（删白名单 + 改路由顺序 + 加重名冲突检测）、`fe-connector-hive`（仅改两行过时注释）
> **预计改动规模**：6 个生产文件，净删约 15 行、新增约 60 行；新增/改动测试约 3 个文件、约 120 行
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

引擎在决定「一个 `CREATE CATALOG` 要不要去问连接器插件」时，先查一张写死在 `fe-core` 里的七个类型名的集合；不在这张集合里的类型，哪怕插件已经装好、已经被插件加载器成功装配、启动日志里都能看到它，`CREATE CATALOG` 依然会失败。本任务删掉这张集合，把它承担的唯一一件真实职责（排除 hudi）上移成连接器自己的一句声明，让「注册了插件就能被路由到」这句承诺在代码上第一次成立。

## 二、背景：现在的代码是怎么写的

### 白名单本体

`fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java:56-57`：

```java
private static final Set<String> SPI_READY_TYPES =
        ImmutableSet.of("jdbc", "es", "trino-connector", "max_compute", "paimon", "iceberg", "hms");
```

它上面有一段 9 行的注释（`:47-55`），说明了两件事：这七个类型走插件（SPI）路径，别的类型掉到下面的内建 `switch`；以及**「不要把 hudi 加进这个集合」**。

### 判定点

同文件 `createCatalog`（`:76`）里，`catalogType` 从 `type` 属性（或 resource）解析出来后（`:79-96`），有三段判定：

```java
Connector spiConnector = null;
if (SPI_READY_TYPES.contains(catalogType)) {                       // :110  第一次查集合
    spiConnector = ConnectorFactory.createConnector(
            catalogType, props, new DefaultConnectorContext(name, catalogId));
}
if (spiConnector != null) {                                        // :114
    catalog = new PluginDrivenExternalCatalog(...);                // :117
} else if (SPI_READY_TYPES.contains(catalogType)) {                // :119  第二次查集合
    if (isReplay) {
        catalog = new PluginDrivenExternalCatalog(..., null);      // :128  降级注册
    } else {
        throw new DdlException("No connector plugin loaded for catalog type '" + ...);  // :131
    }
}
if (catalog == null) {                                             // :138  内建类型兜底
    switch (catalogType) {
        case "lakesoul": throw new DdlException("Lakesoul catalog is no longer supported");  // :143
        case "doris":  ...                                          // :146
        case "test":   ...                                          // :153（仅单测）
        default: throw new DdlException("Unknown catalog type: " + catalogType);           // :157
    }
}
```

所以现在的形状是：**先查集合 → 集合里才问插件 → 集合里但没插件就（建目录时）报错或（重放时）降级 → 集合外一律落到内建 `switch`，最后抛「Unknown catalog type」**。

### 插件侧的自描述发现机制其实已经完备

`fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorProvider.java`：

- `:46` `String getType()` —— 连接器自报类型名，注释写着「对应 `CREATE CATALOG` 里的 `type` 属性」。
- `:52-54` `default boolean supports(String catalogType, Map<String, String> properties)` —— 默认按类型名比较，但**留了按属性判定的口子**。

`fe/fe-core/src/main/java/org/apache/doris/connector/ConnectorPluginManager.java:126-144` 的 `createConnector` 就是照这个机制做的：遍历已注册 provider，第一个 `supports(...)` 返回 true 的胜出，检查 API 版本后建连接器；一个都不匹配就返回 `null`。也就是说**动态分派早就有了，白名单是叠在它上面的一层编译期硬门禁**。

仓库里一共 8 个 provider（各连接器模块的 `META-INF/services/org.apache.doris.connector.spi.ConnectorProvider` 都已核实）：`hms`、`iceberg`、`paimon`、`jdbc`、`es`、`max_compute`、`trino-connector`、`hudi`。白名单正好等于「这 8 个减去 hudi」。**换句话说，白名单今天的全部效果就是两件事：排除 hudi，以及挡住所有第三方连接器。**

### hudi 为什么必须被排除

`fe/fe-connector/fe-connector-hudi/src/main/java/org/apache/doris/connector/hudi/HudiConnectorProvider.java:31-39` 的类注释已经把理由写透了：`"hudi"` 是一个**只用于兄弟连接器查找的类型串，不是用户可见的目录类型**。一张 hudi 表寄生在 Hive 元存储目录上，运行时由 hms 网关通过 `ConnectorContext.createSiblingConnector("hudi", ...)` 构造成内嵌兄弟连接器；`fe-core` 没有、也不该有 hudi 的目录类。真给 `type=hudi` 建一个独立目录，就会造出一个没有引擎侧目录语义支撑的空壳。

兄弟连接器走的是另一条门：`fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java:174-183` 的 `createSiblingConnector` 直接调 `ConnectorFactory.createConnector(...)`，**根本不经过 `CatalogFactory` 的白名单**。这条门必须保持能查到 hudi。

### 类型名唯一性只写在文档里，没人保证

`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorStatementScopes.java:39-43` 在论证「语句级作用域的命名空间跨连接器不会撞」时，明确把 `getType()` 当成前提：

> the connector's connector-type name (its `ConnectorProvider.getType()` …) Because `getType()` is a connector's unique identity, source-prefixing makes these namespaces distinct across connectors *by construction*

但注册路径并不检查这件事：

- `ConnectorPluginManager.loadBuiltins()`（`:74-80`，类路径 ServiceLoader 批次）**完全不去重**，同名类型两个 provider 会一起进 `providers` 列表，谁胜出由插入顺序决定。
- `loadPlugins()`（`:88-112`，生产用的插件目录批次）依赖 `DirectoryPluginRuntimeManager`：`fe/fe-extension-loader/src/main/java/org/apache/doris/extension/loader/DirectoryPluginRuntimeManager.java:131-139` 按 `factory.name()`（对 `ConnectorProvider` 默认就是 `getType()`，见 `ConnectorProvider.java:84-86` 与 `DirectoryPluginRuntimeManager.java:257-259`）判重，重名的那个被跳过并记一条加载失败，由 `ConnectorPluginManager.java:100-104` 打成 WARN —— **不抛异常，第一个胜出**。
- 两个批次**之间**没有任何去重：一个插件目录里的 provider 和一个类路径上的 provider 声明同一个类型名，双方都会进列表。

## 三、为什么这是个问题

**第一，它让「新增连接器不需要修改公共模块」这句承诺在代码上是假的。** 一个第三方连接器把 `ConnectorProvider` 写对、`META-INF/services` 注册对、插件目录放对，FE 启动日志里 `ConnectorPluginManager initialized ... registered types: [..., acme-lake]` 都打出来了，用户执行 `CREATE CATALOG ... "type" = "acme-lake"` 依然得到「Unknown catalog type: acme-lake」。要让它能用，唯一办法是改 `CatalogFactory.java:57` 那一行、重新编译并发布整个 FE —— 这恰好是插件化想消掉的事情。这是整轮整治里唯一能真正兑现这条承诺的改动。

**第二，它让 `supports(catalogType, properties)` 这个能力事实上不可用。** 按属性（而不是仅按类型名）分派的口子留在了公共接口上，但因为外层先按类型名过一遍白名单，任何「类型名不在名单里、只有属性能识别」的分派都不可能发生。目前全仓 0 个连接器覆写 `supports` —— 不是因为没人需要，是因为覆写了也不会被调用。删掉白名单，这个能力才第一次真正接通。

**第三，它顺带造成一个现存缺陷：不认识的目录类型会让 FE 起不来。**（这是本任务顺带修掉的净改善）
`CatalogMgr.replayCreateCatalog`（`fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogMgr.java:546-549`）在编辑日志重放时调 `CatalogFactory.createFromLog`。一旦它抛异常，`fe/fe-core/src/main/java/org/apache/doris/persist/EditLog.java:1150-1154` 的 `OP_CREATE_CATALOG` 分支把异常交给 `:1524-1538` 的兜底 `catch`，那里除非该操作码被列进 `Config.skip_operation_types_on_replay_exception`（`fe/fe-common/src/main/java/org/apache/doris/common/Config.java:1311`，默认 `{-1, -1}` 即空），就直接 `System.exit(-1)`。

现在的代码只对**白名单内**的类型做了重放降级保护（`:119-129`）。白名单外的类型在重放时会走到 `:157` 抛异常 → FE 进程退出。用户观察到的现象是：**一次本来只该让某个目录不可用的问题，变成整个 FE 起不来**，而且日志里只有一行「Unknown catalog type」。

顺带说明：从元数据镜像恢复的那条路径**已经**没有这个问题 —— 镜像里目录是 Gson 反序列化成 `PluginDrivenExternalCatalog`（`fe/fe-core/src/main/java/org/apache/doris/persist/gson/GsonUtils.java:358-407`），连接器是首次访问时由 `PluginDrivenExternalCatalog.createConnectorFromProperties()`（`:192-205`）惰性创建的，那里**不查白名单**。所以本任务的另一个价值是把两条恢复路径的口径拉齐：第三方连接器的目录一旦能建出来，重启后无需任何 Gson 改动就能正确恢复（`clazz` 就是 `PluginDrivenExternalCatalog`）。

## 四、用一个最小例子说明

假设我要新增一个连接器 `acme-lake`，插件已装好并被成功加载。

```sql
CREATE CATALOG my_lake PROPERTIES ("type" = "acme-lake", "acme.uri" = "http://acme:8080");
```

| 用户写了什么 | 现在实际发生什么 | 应该发生什么 |
|---|---|---|
| 上面这条 `CREATE CATALOG` | 报错 `Unknown catalog type: acme-lake`。引擎连插件都没问 —— `"acme-lake"` 不在 `CatalogFactory` 那七个字符串里，直接掉进内建 `switch` 的 `default` | 引擎向已注册的 provider 逐个问 `supports("acme-lake", props)`，`AcmeConnectorProvider` 认领，建出 `PluginDrivenExternalCatalog` |
| 同一条语句，但插件**没**装 | 报错 `Unknown catalog type: acme-lake`（看不出是插件缺失还是类型写错） | 报错「没有插件认领类型 `acme-lake`，当前已注册类型：\[hms, iceberg, ...\]」 |
| 目录已建好，此后重启 FE，且这条建目录日志还没被 checkpoint 掉，而插件被运维误删 | 重放抛异常 → `System.exit(-1)`，**FE 起不来** | 降级注册该目录；只有真去访问它时才报「未找到插件，请确认插件已安装」 |
| `CREATE CATALOG h PROPERTIES ("type" = "hudi")` | 报错（白名单里没有 hudi）—— 这是**正确**行为，必须保住 | 依然报错。理由不再是「不在白名单里」，而是 hudi 的 provider 自己声明了 `isStandaloneCatalogType() == false` |

「我今天必须动哪些文件」这个问题的答案就是这张表的第一行：**`fe/fe-core/.../CatalogFactory.java` 一行，然后重编译发布 FE**。本任务之后答案变成：一个文件都不用动。

## 五、解决方案

### 5.1 目标状态

**（1）连接器自己声明「我能不能作为一个独立目录出现」**，在 `fe-connector-spi` 的 `ConnectorProvider` 上加一个带默认实现的方法（默认 true，对齐仓库既有的 `supportsXxx()` opt-in 惯例；这里语义是「默认允许、少数否认」，所以默认值取 true）：

```java
/**
 * 本 provider 的类型能否作为一个独立目录出现在 CREATE CATALOG 的 type 属性里。
 *
 * 返回 false 表示这个连接器只以内嵌兄弟身份存在（由另一个连接器通过
 * ConnectorContext.createSiblingConnector 构造并持有），引擎不会为它建独立目录；
 * 它仍然正常参与服务发现与兄弟查找。默认 true。
 */
default boolean isStandaloneCatalogType() {
    return true;
}
```

**（2）区分两条查询入口。** 兄弟连接器查找**必须**仍然能查到非独立类型，所以这个开关只能作用在「建独立目录」这条路径上，绝不能塞进 `ConnectorPluginManager.createConnector`：

| 入口 | 用途 | 是否过滤非独立类型 |
|---|---|---|
| `ConnectorPluginManager.createConnector` / `ConnectorFactory.createConnector` | 兄弟连接器查找（`DefaultConnectorContext.createSiblingConnector`） | **否**（hudi 靠它） |
| 新增 `createStandaloneCatalogConnector`（两个类上各一个同名入口） | 建独立目录 | 是 |

实现上让两个入口共用一个私有方法、只差一个 `standaloneOnly` 布尔量即可，不要复制遍历逻辑。

**（3）`CatalogFactory.createCatalog` 改成三段式**，删掉 `SPI_READY_TYPES`：

```
① 无条件问插件：createStandaloneCatalogConnector(catalogType, props, ctx)
     命中 -> PluginDrivenExternalCatalog(带连接器)
② 没命中 -> 问引擎内建类型（lakesoul / doris / test 这个 switch 原样保留）
     命中 -> 原样处理
③ 都没命中：
     isReplay == true  -> 降级注册 PluginDrivenExternalCatalog(connector = null)，打 WARN
     isReplay == false -> DdlException，文案说明「没有插件认领该类型」并列出 ConnectorFactory.getRegisteredTypes()
```

第 ③ 步就是白名单内那段降级逻辑（`:119-135`）的去白名单版本：**降级/报错的判据从「类型在名单里」变成「引擎也不认识它」**。降级目录首次访问时的报错文案不用改，`PluginDrivenExternalCatalog.initLocalObjectsImpl` 已有（`:162-165`）：`No ConnectorProvider found for plugin-driven catalog: <name>, type: <type>. Ensure the connector plugin is installed.`

**关于顺序**：插件优先于内建类型，是为了保持现有七个类型的行为逐字不变（它们今天就是插件优先）。代价是一个插件若声明 `getType()` 为 `doris` / `test` / `lakesoul` 会遮蔽内建类型 —— 这一点写进 `getType()` 的契约文档（见下），并由第（4）条的冲突检测兜住其中的插件对插件冲突。

**（4）注册时检测类型名冲突。** 在 `ConnectorPluginManager` 里维护一个「已被认领的类型名」集合（小写比较），两个加载批次都过这一关：

- `loadBuiltins()`：撞名直接抛 `IllegalStateException`。类路径上出现两个同名类型是构建期错误，不是部署事故，应该当场炸。
- `loadPlugins()`：撞名**跳过后来者**并打 `LOG.error`（把两个 provider 的类名都打出来）。这里保持该方法既有的「部分成功」契约 —— 一个坏插件目录不该阻止 FE 启动。
- `registerProvider(provider)`（`:177-179`，测试用的最高优先级插队）**不参与去重**，它就是为了遮蔽而存在的，多个测试依赖它。

同时把类型名唯一性契约写进 `ConnectorProvider.getType()` 的 javadoc：全局唯一（不区分大小写）、不得与引擎内建类型名相同、并且是语句级作用域命名空间前缀的锚点（呼应 `ConnectorStatementScopes.java:39-43` 已经写下的那段论证）。

### 5.2 改动清单

| 文件 | 做什么 |
|---|---|
| `fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorProvider.java` | 在 `getType()`（`:46`）之后加 `isStandaloneCatalogType()` 默认方法；补强 `getType()` 的 javadoc（唯一性契约 + 不得撞内建类型名 + 命名空间前缀锚点） |
| `fe/fe-connector/fe-connector-hudi/src/main/java/org/apache/doris/connector/hudi/HudiConnectorProvider.java` | 覆写 `isStandaloneCatalogType()` 返回 `false`；把类注释（`:31-39`）与 `getType()` 里那两行注释（`:45-46`）中「NEVER add "hudi" to `SPI_READY_TYPES`」改写成指向这个覆写 —— 白名单已不存在，留着会把后人指向一个不存在的符号 |
| `fe/fe-core/src/main/java/org/apache/doris/connector/ConnectorPluginManager.java` | 加 `createStandaloneCatalogConnector`（与 `createConnector` 共用私有遍历，差一个布尔量）；加类型名冲突检测（见 5.1 第 4 条）；`registerProvider` 不动 |
| `fe/fe-core/src/main/java/org/apache/doris/connector/ConnectorFactory.java` | 加对应的静态入口 `createStandaloneCatalogConnector`（照 `:66-75` 的 `createConnector` 写法，插件管理器为 null 时返回 `null`） |
| `fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java` | 删 `SPI_READY_TYPES`（`:56-57`）与它上面那段注释（`:47-55`）；`:107-135` 改成 5.1 第（3）条的三段式；`:140-142` 那条提到 `SPI_READY_TYPES` 的注释改写；`:157` 的 `default` 分支不再是唯一出口，改由第 ③ 步统一处理 |
| `fe/fe-core/src/main/java/org/apache/doris/datasource/plugin/PluginDrivenExternalCatalog.java` | `createConnectorFromProperties()`（`:204`）改用 `createStandaloneCatalogConnector`。这是建独立目录的**第二道门**（镜像恢复后的惰性创建），两道门口径一致才不会留下歧义 |
| `fe/fe-connector/fe-connector-hive/src/main/java/org/apache/doris/connector/hive/HiveConnector.java` | `:73` 与 `:78` 两条注释提到 `CatalogFactory.SPI_READY_TYPES`，改成指向新机制。仅注释 |

### 5.3 明确不要顺手做的事

- **不要动 `lakesoul` 那条硬失败**（`CatalogFactory.java:143`）。它在重放时同样会让 FE 退出，是一个**相邻但独立**的缺陷；它属于「有意下线某类型」的语义，改它等于改用户可见的下线策略，应该单独立项讨论。本任务只保证「引擎不认识的类型」在重放时降级。
- **不要把 `isStandaloneCatalogType` 铺到 `validateProperties` 上**（`ConnectorPluginManager.java:161-174` / `ConnectorFactory.java:97-103`）。它只被 `PluginDrivenExternalCatalog.checkProperties`（`:212`）调用，而非独立类型永远建不出这样的目录，加过滤没有可达的行为差异，只增加要维护的分支。
- **不要顺手清理仓库里其它二十来处提到 `SPI_READY_TYPES` 的注释**。除了 5.2 表里点名的那几处（它们承载「不要给 hudi 建独立目录」这条真实契约、必须跟着改），其余大多是各连接器里叙述迁移历史的文字（例如 iceberg 那批「切换前是惰性的」说明），跟着一起改会把这个补丁摊成几十个文件、淹掉真正的改动。
- **不要顺手把 `supports` 的按属性分派用起来**。本任务只负责把这条路接通，不负责给任何连接器写第一个覆写。
- **不要给类型名唯一性加 shell/正则构建门禁**。类型名是方法返回值，判定它需要理解 Java 语义；本仓库已有结论：那类门禁只适合存在性与前缀类不变量。这里用运行时的注册冲突检测 + 单测就够了。
- **不要顺手改 `Env.changeCatalog` 里那句按 `"es"` 硬编码的默认库设置**（`fe/fe-core/src/main/java/org/apache/doris/catalog/Env.java:6509-6512`）。它是另一处按数据源名分支，属于别的任务。

## 六、怎么验证

**（1）单元测试：兄弟查找与独立目录必须分道扬镳**（扩 `fe/fe-core/src/test/java/org/apache/doris/connector/ConnectorPluginManagerTest.java`）

- 注册一个 `isStandaloneCatalogType()` 返回 false 的假 provider：`createStandaloneCatalogConnector(它的类型)` 必须返回 `null`，而 `createConnector(同一类型)` 必须返回非 `null`。**断言要写清 WHY**：后者是 hms 网关构造 hudi 兄弟连接器的唯一通路，前者返回非 null 就等于允许建出一个没有引擎侧目录语义的空壳目录。
- 注册一个类型名是任意第三方串（例如 `"acme-lake"`）的普通假 provider：`createStandaloneCatalogConnector` 必须命中。这条就是「删掉白名单」的行为断言 —— 它在改动前必然失败（因为那时压根没有这个方法），改动后必须通过。

**（2）单元测试：注册冲突**（同一文件）

- 两个 provider 声明同一个类型名（大小写不同也算撞）：类路径批次抛 `IllegalStateException`；插件目录批次跳过后来者且 `getRegisteredTypes()` 里该类型只出现一次。
- 为了能直接测到，建议把「登记一个已发现的 provider」抽成一个包内可见的小方法，让两个批次都调它，测试直接打这个方法 —— 不要为了测试去伪造 `META-INF/services` 文件。
- 一条断言保住 `registerProvider` 的遮蔽语义：先 `loadPlugins` 装一个 `"iceberg"`，再 `registerProvider` 一个同名的，后者必须胜出且不报冲突（多个既有测试依赖这条，见 `DefaultConnectorContextSiblingTest.java:77`）。

**（3）单元测试：重放不再让 FE 退出**（扩 `fe/fe-core/src/test/java/org/apache/doris/datasource/ExternalCatalogTest.java`，那里已有现成的 `registerCatalogViaReplay` 私有助手，`:169-177`）

- 用一个引擎和插件都不认识的类型（例如 `"acme-lake"`）走 `mgr.replayCreateCatalog`：**必须不抛异常**，且目录能在 `mgr` 里查到。断言注释里要写清 WHY：这条路径上抛异常会经由 `EditLog` 的兜底 `catch` 走到 `System.exit(-1)`，代价是整个 FE 起不来。
- 同一个目录上触发一次会走到 `makeSureInitialized` 的访问，断言报错文案含 `No ConnectorProvider found for plugin-driven catalog`。
- 反向断言：非重放路径（`CREATE CATALOG`）对同一个类型必须报错，且文案里能看到已注册类型列表。**这条不能漏** —— 否则「删白名单」会退化成「任何拼错的 type 都静默建出一个坏目录」。

**（4）不要破坏的既有测试。** `ExternalCatalogTest.testShowCreateCatalogMasksSensitiveProperties`（`:126-167`）刻意用「重放降级」这条路注册一个 `type=iceberg` 的目录（fe-core 单测里加载不到 iceberg 插件）。改动后它走的是新的第 ③ 步而不是原来的白名单分支，行为必须完全一样：目录注册成功、`SHOW CREATE CATALOG` 能打出被脱敏的属性。跑测试时**必须禁用 maven build cache**，否则 surefire 会被静默跳过、`BUILD SUCCESS` 是空的。

**（5）编译门禁。** 全反应堆**含测试源**的 `test-compile`（禁用任何跳过测试编译的参数）。这是本任务最强的单一信号：`SPI_READY_TYPES` 是私有字段，只有注释引用它，所以编译不会替你发现漏改的注释 —— 但它会替你发现所有漏改的调用点。maven 用绝对路径的 `-f`。

**（6）端到端回归。** 需要跑一轮既有的外部目录建目录用例（hms / iceberg / paimon / jdbc / es 各至少一个 `CREATE CATALOG` + 一次查询），确认这七个类型的建目录行为逐字不变。**必须专门跑一次 hudi 读取用例**（hudi 表寄生在 hms 目录上），确认兄弟连接器构造这条路没有被新开关误伤 —— 这是本任务最需要端到端兜底的一点，单测只能证明路由，证不了整条读取链。

**（7）不需要变异验证。** 本任务的核心断言（非独立类型在两个入口上一个通一个不通）本身就是双向的，改动前后行为差异明确。

## 七、风险与回退

| 风险 | 说明与对策 |
|---|---|
| 新开关塞错位置，把 hudi 的兄弟查找也挡掉 | 这是本任务**唯一的高危错误**：hudi 表会整体读不出来，而且 fe-core 单测未必能发现。对策是 5.1 第（2）条那张表（开关只作用于建独立目录的入口）+ 第（1）（6）条的双向断言与 hudi 端到端用例 |
| 打错字的 `type` 从「明确报错」变成「静默建坏目录」 | 只可能发生在实现漏掉第 ③ 步的非重放分支时。第（3）条的反向断言专门守这个 |
| 第三方插件遮蔽内建类型名（`doris` / `test` / `lakesoul`） | 插件优先的顺序带来的固有代价，换取现有七个类型行为不变。已写进 `getType()` 契约文档；插件之间的撞名由注册冲突检测挡住 |
| 重放语义从抛异常改成降级注册 | 这是本任务有意为之的净改善。已核实全仓（含 `regression-test/`）没有任何测试断言 `Unknown catalog type` 或 `No connector plugin loaded` 这两条文案，没有测试依赖旧行为 |
| Gson 持久化兼容 | **无影响**。第三方目录持久化为 `PluginDrivenExternalCatalog`，`GsonUtils.java:362-363` 早已注册该子类型；本任务不新增、不删除、不重命名任何持久化类型标签 |

**回退**：改动集中在 6 个文件、彼此无跨阶段耦合，`git revert` 单个提交即可完整回退。`isStandaloneCatalogType()` 是带默认实现的新增方法，回退它不会让任何连接器编译失败（唯一的覆写在 hudi，随同一提交一起回退）。

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md`
  - 第 4.1 节（1)「目录类型白名单——最该删的一处」：本任务的直接来源，包含 `isStandaloneCatalogType` 的原始草案。
  - 附录 A.1 第 1 条与第 7 条：同一问题的两次独立记录（一条按可扩展性归类、一条按路由归类），复核结论均为「部分成立」。
  - 落地批次表里的「5. 删类型白名单」一行，以及排期建议中「这是唯一能真正兑现『新增连接器不需要修改公共模块』的一批；在它合入之前，这个承诺在代码上是假的」。
- `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorStatementScopes.java:22-51`：类型名唯一性契约的下游消费者，本任务第（4）条冲突检测要保护的正是它的前提。
- `fe/fe-connector/fe-connector-hudi/src/main/java/org/apache/doris/connector/hudi/HudiConnectorProvider.java:25-39`：hudi 为什么没有独立目录概念，写得比本文更细，改注释前先读它。
- 相邻但不在本任务范围内的同类问题（都在审计报告里各有条目）：`CreateTableInfo` 里建表能力的四处协同硬编码、`PluginDrivenExternalTable` 里两份重复的 engine 展示名 `switch`、`FileQueryScanNode` 的文件缓存准入类型白名单。它们和本任务是同一个病根的不同发作点，但各自独立可做。

---

## 九、落地记录（2026-07-25，两个提交）

**提交**：`[refactor](catalog) remove the catalog type allow-list so a registered connector is reachable`（生产改动 + 测试）、`[doc](catalog) stop pointing at the deleted catalog type allow-list`（纯注释）。

### 动手前按符号复核的结论

任务文档的核心事实全部成立：8 个 provider（`hms` `iceberg` `paimon` `jdbc` `es` `max_compute` `trino-connector` `hudi`）、白名单 = 8 减 hudi、兄弟查找不经白名单、插件目录批次已按名字判重而类路径批次与跨批次都不判重、全仓（含 `regression-test/`）无任何测试断言 `Unknown catalog type` 或 `No connector plugin loaded` 两条文案。

**六处需要修正或补充**：

1. **文案变化面比文档写的大。** 不只是「引擎不认识的类型」——原白名单内 7 个类型在**插件缺失**时的报错文案也变了（旧文案专门提到 `connector_plugin_root`）。已核实无测试依赖，写进了提交信息。
2. **删字段留下 20 处悬空注释**（分布 15 个文件），文档只点了必须改的 4 处。已按第二个提交处理。其中 **6 处在本轮之前就已经是错的**（iceberg 那批仍写着「iceberg 还没进白名单、所以代码不可达」，有一处甚至说「没有 iceberg 分片到达 BE」），这些直接删除而不是改写。
3. **删字段后 `ImmutableSet` / `Set` 两个 import 会孤立**，`test-compile` 不报、`checkstyle` 报。本轮因为保留了 `BUILTIN_CATALOG_TYPES` 仍在用，未触发。
4. **三段式不需要新增数据结构**：把原 `switch` 的 `default` 分支改写成第三段即可，文档描述得像要再维护一个集合。
5. **`CREATE CATALOG` 是目录级 CREATE 权限、不是管理员权限**，所以「报错列出已注册类型」确实是对非管理员暴露插件清单。已拍板列出（对齐 Trino 同类报错），并**过滤掉非独立类型**——把一个建不出来的名字列给用户会误导。
6. **文档把「插件可遮蔽内建类型名」当固有代价接受，实际有更好解法**：Trino 的做法是单一注册表 + 重名直接拒绝。本轮借了后半段——`doris` / `test` / `lakesoul` 成为保留字，在**注册期**拒绝，于是「遮蔽」这个风险不存在，路由顺序也不再影响正确性。

### 与文档方案的差异

- 新增 `CatalogFactory.BUILTIN_CATALOG_TYPES`（包内可见）+ `isBuiltinCatalogType()`，`ConnectorPluginManager` 用它做保留字判定。这是文档没有的一项。
- 类型名检查统一到一个包内可见的 `registerDiscovered(provider, failFast)`，两个加载批次都过它（文档建议如此），顺带覆盖了文档未提的「空白类型名」与「跨批次重名」。
- `registerProvider`（测试插队）不参与检查，注释写清了原因。

### 验证结果

- 全反应堆**含测试源** `test-compile` + `checkstyle` 通过（两个提交各跑一次）。
- 52 个单测通过：`ConnectorPluginManagerTest`（13，新增 8）、`CatalogFactoryPluginRoutingTest`（5，新建）、`HudiConnectorProviderTest`（1，新建）、`ExternalCatalogTest`（3，含刻意走重放降级那条）、`DefaultConnectorContextSiblingTest` / `StoragePropsTest`（各 3）、`PluginDrivenExternalTableEngineTest`（16）、`CreateTableInfoEngineCatalogTest`（9）。
- **做了三次变异验证**（文档原说不需要，但把开关放错入口是本任务唯一高危错误，值得实证）：
  1. 把独立过滤搬到兄弟查找入口（两个入口对调）→ **两个方向同时变红**：兄弟查找返回 null，且非独立类型能建出目录。
  2. 把 hudi 的声明改成 `true` → `HudiConnectorProviderTest` 变红。
  3. 删掉保留字检查 → `providerClaimingAnEngineBuiltinCatalogTypeIsRefused` 变红。
- **未执行**：端到端（需真集群）。7 个类型各一次建目录+查询、以及**一次 hudi 读取**仍待有集群时跑——单测只能证明路由，证不了整条读取链。

### 本轮踩到的坑（供后续批次复用）

- **`-pl <单模块>` 会从本地仓库解析兄弟模块的旧 jar**。给 `fe-connector-spi` 加了方法后，用 `-pl fe-connector/fe-connector-hudi` 跑测试会报「method does not override」——那是旧 jar，不是代码错。跑连接器模块的测试一律走全反应堆 + `-Dtest=` 过滤。
- **checkstyle 的方法名正则是 `^[a-z][a-z0-9][a-zA-Z0-9_]*$`**：第二个字符也必须小写，`aTypeThatIsCreatable` 这种测试名会红。
- **`PluginDrivenExternalCatalog.getConnector()` 会触发 `makeSureInitialized()`**，纯单测里用不了（需要真 Env）。判断「目录是插件建出来的还是降级注册的」改用「假 provider 记录自己被问了几次」，而且这样断言的正是「引擎真的问了插件」这个不变量。
- **`ConnectorMetadata` 的冻结基线没有被牵动**：本轮加的方法在 `ConnectorProvider`（`fe-connector-spi`），不在那份基线的覆盖范围内，无需重新生成。
