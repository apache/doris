# 4.1.3 → branch-catalog-spi 升级兼容性验证方案（FE UT only，待 review）

> **唯一目标**：4.1.3 集群升级后，之前创建的 catalog 能被迁移到 connector 框架上并正常使用。
> **决定**：D1=`4.1.3`（annotated tag，commit `7126cf65d96`）／D2=不覆盖 resource-backed（**见 §0.1，建议重看**）／D3=cloud 在范围内／**全部在 FE UT 完成，不做 L2 离线回放、不做 L3 真实升级**。
> 日期 2026-07-29　HEAD=`31ae926565c`　上一版方案中的 L2/L3/L4 已按决定删除。

---

## 0. 结论先行

### 0.0 可行性已实测，不是推演
在本机离线完成，全程 `-o` 无网络：

```bash
git worktree add --detach /mnt/disk1/yy/git/doris-413 4.1.3^{commit}      # 5.4s
cd /mnt/disk1/yy/git/doris-413
DORIS_HOME=$PWD DORIS_THIRDPARTY=/mnt/disk1/yy/git/doris/thirdparty bash generated-source.sh   # 10.4s
mvn -o -f /mnt/disk1/yy/git/doris-413/fe/pom.xml -pl fe-core -am -DskipTests \
    -Dcheckstyle.skip=true -Dlicense.skip=true -Drat.skip=true compile    # BUILD SUCCESS 3m23s
```

用 4.1.3 的**真实字节码**生成 `CatalogMgr` 字节，喂给 HEAD 的 `CatalogMgr.read`，结果正确：

```
0     -> InternalCatalog              internal
10001 -> PluginDrivenExternalCatalog  hms_ctl   logType=PLUGIN type=hms      taap 保真
10002 -> PluginDrivenExternalCatalog  ice_ctl   logType=PLUGIN type=iceberg
10003 -> PluginDrivenExternalCatalog  jdbc_ctl  logType=PLUGIN type=jdbc
10004 -> PluginDrivenExternalCatalog  pai_ctl   logType=PLUGIN type=paimon
10005 -> PluginDrivenExternalCatalog  es_ctl    logType=PLUGIN type=es
```

> ☠ **绝对不要在 4.1.3 worktree 里 `mvn install`**。两棵树都解析 `${revision}=1.2-SNAPSHOT`，4.1.3 的 jar 会静默覆盖 HEAD 在 `~/.m2` 里的同名产物。只 `compile`。`-am` 是必需的（单模块会挂在 `${revision}` 上）。

### 0.1 ❗ D2 请重新裁决：新证据推翻了当初的成本假设

当初否掉 resource-backed（props 里没有 `type`）的理由是"要跨版本升级链，成本高"。**这个前提现在不成立**——同一个生成器里直接调 4.1.3 的具体构造函数即可，多写 5 行。

而 agent 做了变异测试，**实测**结果是：

> 把 `PluginDrivenExternalCatalog.java:1422-1432` 的整个类型反填块删掉重编译，用 props 里带 `type` 的夹具跑，**测试依然全绿**（BASELINE OK / MUTANT OK）。

原因很直白：`type` 已经在 JSON 里，`getType()` 直接读得到；`instanceof PluginDrivenExternalCatalog` 来自 `registerCompatibleSubtype`。所以**只有 `type` 的夹具能测到的是"标签重映射"，测不到本分支为迁移新增的任何一行逻辑**。

- 只测带 `type` 的（当前 D2）：覆盖标签重映射 + `logType→PLUGIN` 改写 + 属性传递保真。**不覆盖类型反填**。
- 加上 3 个不带 `type` 的（es / jdbc / **trino-connector**）：类型反填被真正测到。trino 不可省——它是 `logType.name().toLowerCase()`（`trino_connector`）与所需值（`trino-connector`）唯一不一致的类型。

**建议**：加这 3 个，成本≈0。若你坚持 D2，方案照跑，但 §5 的残留风险表里 R2 要从"已覆盖"降级为"不覆盖"，并在 PROVENANCE 里写明。**我按"加"来写，你划掉即可。**

### 0.2 UT 层的诚实天花板

fe-core 只依赖 `fe-connector-api` + `fe-connector-spi`，`ServiceLoader` 找不到真实 connector。UT 能证明的是「**路由到正确的 provider + 属性逐项完整传递**」，证明不了「真实后端上查得动」。

**其中一条残留风险有廉价解，建议做**（详见 §4）：`legacyLogTypeToCatalogType()` 返回的是字符串字面量，必须等于**实际发布的** provider 的 `getType()`。把 `TrinoConnectorProvider.getType()` 改成 `"trino"`，**整套测试依然全绿，而所有迁移过来的 trino catalog 永久失效**。fe-core 的 pom 里已经有 14 个 `fe-filesystem-*` 的 `<scope>test</scope>` 先例，加 connector 模块的 test 依赖即可闭合。

---

## 1. 夹具：生成，绝不手写

### 1.1 为什么不能手写（4 个已实测的坑 + 1 个我自己踩的）

| 坑 | 手写必错的地方 |
|---|---|
| **HTML 转义是开的** | jdbc catalog 的 `jdbc_url` 落盘成 `...?yearIsDateType=false&tinyInt1isBit=false`。手写一定写成 `?`/`=`/`&` |
| **`serializeNulls` 是关的** | `catalogProperty.resource` 为 null 时**整个 key 不出现**；非 null 时才有 |
| **`taap` 形状是多态的** | 空 map → `"taap":{}`（JSON 对象）；非空 → `"taap":[[{"first":"db1","second":"tbl1"},"enable"],...]`（二元组数组）。这是 `enableComplexMapKeySerialization` 的 `hasComplexKeys` 分支 |
| **`clazz` 在最前** | `RuntimeTypeAdapterFactory.write` 先写 `clazz` 再写委托字段，然后是 `ExternalCatalog` 的 8 个字段**按声明序** |
| **建 catalog 时 props 会被改写** | `setDefaultPropsIfMissing` 给**每个** catalog 注入 `use_meta_cache` / `enable.mapping.varbinary` / `enable.mapping.timestamp_tz`；HMS 另加 `ipc.client.fallback-to-simple-auth-allowed`；JDBC 重写 `jdbc_url`。**落盘的不是用户敲的** |
| **（我自己踩的）** | 我第一次抽 4.1.3 标签集时漏了 `IcebergS3TablesExternalCatalog`，因为类名带数字 `3` 而正则写的是 `[A-Za-z]+`。**正确是 23 个注册类**（21 外部 + `InternalCatalog` + `CloudInternalCatalog`），HEAD 全覆盖 |

**结论**：`labels.txt` 必须由脚本从 4.1.3 源码抽取并 commit，附抽取脚本；任何人工誊写的清单都不可信。

### 1.2 三族夹具

| 族 | 内容 | 生成方式 |
|---|---|---|
| **G1** 带 `type` | hms / iceberg-rest / jdbc / paimon / es | 4.1.3 的 `CatalogFactory.createFromLog(CatalogLog)` |
| **G2** 不带 `type`（§0.1，建议保留） | es / jdbc / **trino-connector** | 直接调 4.1.3 具体构造函数 + `setDefaultPropsIfMissing(true)`——这正是 4.1.3 那个 switch 做的事，绕开需要 `ResourceMgr` 的 resource 分支 |
| **G3** 4.1.3 造不出来但 4.1 image 里可能有的标签 | `LakeSoulExternalCatalog`、`Paimon{HMS,File,Rest,DLF}ExternalCatalog`（4.1.3 的工厂永远只返回基类 `PaimonExternalCatalog`；`IcebergExternalCatalog` 是 abstract） | 只能由锚定过的 writer 构造；**必须在 PROVENANCE.txt 里写明来源不同** |

### 1.3 保真锚（整套夹具可信度的唯一来源）

一次性生成真实 4.1.3 字节 commit 进仓库，同时在仓库里放一个 writer 供日常构造，用**字节相等**把 writer 钉死在真实字节上：

```java
byte[] golden = Files.readAllBytes(res("/upgrade/413/datasource.module.bin"));
ByteArrayOutputStream bos = new ByteArrayOutputStream();
try (DataOutputStream out = new DataOutputStream(bos)) {
    Text.writeString(out, GsonUtils.GSON.toJson(Legacy413Fixtures.buildFixtureModule()));
}
Assert.assertArrayEquals(golden, bos.toByteArray());   // 字节相等，绝不用 JsonElement 比
```

这一条断言同时钉住 §1.1 的全部 5 个坑，且不需要把它们各写一遍。**它红了，其余所有测试的结论都作废**。

### 1.4 落盘布局

```
fe/fe-core/src/test/resources/upgrade/413/
  datasource.module.bin          G1+G2，带 Text 帧（int32 长度 + UTF-8），即 image 的 datasource 模块字节
  datasource.module.cloud.bin    同上，deploy_mode="cloud" 生成
  catalogMgr.json                不带帧，供人肉 diff
  editlog/op<NNN>-<name>.bin     每 op 一个文件：writeShort(opCode) ++ Text(json)
  labels.txt                     从 4.1.3 GsonUtils 机器抽取，禁止手写
  PROVENANCE.txt                 4.1.3^{commit}=7126cf65d96、gson 2.10.1、生成器源码、sha256、日期、G3 来源说明、降级不可逆声明
fe/fe-core/src/test/java/org/apache/doris/datasource/upgrade/
  Legacy413Fixtures.java  RecordingConnectorProvider.java  + 下表各测试类
```

---

## 2. 测试用例

### A. image 路径（`CatalogMgr.read`，纯 GSON，不构建 connector）

| # | 类 | 断言 | 覆盖 |
|---|---|---|---|
| **1** | `Legacy413WireAnchorTest` | §1.3 字节相等 | 夹具可信度本身 |
| **2** | `Legacy413ImageBlobTest` | `CatalogMgr.read(DataInput)` 后每个 G1 条目是 `PluginDrivenExternalCatalog`；id/name/comment/`taap` 保真。不需要 `Env`/`MetaContext`/provider——`gsonPostProcess` 只是重建 `nameToCatalog`，不调 `addCatalog` | 标签重映射 |
| **3** | `Legacy413BackfillTest`（须放 `org.apache.doris.datasource` 包，`logType` 是 protected） | 对 G2 每个：`getType()` 正确（**含 `trino-connector` 连字符**）、`type` 已被**写回** props、`logType == PLUGIN`。`getLastUpdateTime()` 必须在任何 `makeSureInitialized()` **之前**断言（`ExternalCatalog.java:423` 会覆盖它）。**不要断言 `initialized`**——`gsonPostProcess` 无条件置 false | **迁移逻辑本体**（D2 若不加 G2 则此类不存在） |
| **4** | `Legacy413FullImageTest` | 手工拼一张真 `image.N`（`MetaHeader.write` + header 记录 + `datasource` 索引 + `resources` 索引 + `MetaFooter.write`），用 `MetaReader.read(File, new Env(false))` 读。`MetaWriter` 无法逐模块驱动（`WriteMethod`/`Delegate` 是 private 嵌套接口），需内联其 body。附带断言：未知模块 → `IOException`（除非 `ignore_unknown_metadata_module`）、废弃模块被跳过、`MODULE_NAMES.indexOf("datasource") < indexOf("resources")` | 容器/footer/模块分发/读取顺序不变式 |
| **5** | `Legacy413LabelMatrixTest` | 数据驱动读 `labels.txt`，逐标签过三个 factory。**表标签必须用 `assertSame(Class, x.getClass())`，绝不能用 `instanceof`**——`PluginDrivenMvccExternalTable extends PluginDrivenExternalTable`，`instanceof` 抓不到静默降级 | 23 个标签字面量 |

> ⚠ 陷阱：`assertNotNull(env.getResourceMgr())` 是空断言（`Env` 构造函数里就建好了）。要证明 `resources` 模块真被读，须 `assertNotSame(preLoadInstance, env.getResourceMgr())`。
> ⚠ `MetaContext` 必须在 `new Env(false)` **之前** `mockStatic`（`Env.java:810-811` 会装一个 version=0 的）。

### B. edit log 路径（`EditLog.loadJournal`，**connector 会被真正构建**）

wire 形态是 `writeShort(opCode)` + `Text.writeString(GSON.toJson(payload))`——**类名不上 wire**，5 个 payload 类相对 4.1.3 只有包移动，所以 4.1.3 与 HEAD 的字节相同，**差别只在回放行为**。

| # | 类 | 断言 | 覆盖 |
|---|---|---|---|
| **6** | `Legacy413JournalReplayTest` | 每条一个独立 `byte[]`（**绝不拼接**：`OP_INIT_EXTERNAL_TABLE` / `OP_*_EXTERNAL_PARTITIONS` 会 `isRead=true` 而不消费，共享流会错位），喂 `EditLog.loadJournal(Env, Long, JournalEntity)`。覆盖 OP_CREATE/DROP/ALTER_NAME/ALTER_PROPS/ALTER_COMMENT/REFRESH_CATALOG、OP_INIT_*、OP_REFRESH_EXTERNAL_DB/TABLE、OP_ADD_META_ID_MAPPINGS | 滚动升级中 HEAD follower 回放 4.1.3 master 日志 |
| **7** | `Legacy413ReplayDegradeTest` | provider 的 `create()` 抛异常 / 类型无人认领时，`CatalogFactory.createFromLog` 必须返回 `PluginDrivenExternalCatalog` 且 `getType()` 正确 | **R1**：回放路径同步建 connector 且无 try/catch |

> ⚠ 必须在 setUp 设 `Config.skip_operation_types_on_replay_exception` 并在 tearDown 还原，否则回放缺陷直接 `System.exit(-1)`，surefire 报 "Tests run: 0 / VM terminated"。
> ⚠ **但设了它之后，没有断言的 `replay()` 永远不会失败**——所以每条回放的 op 都必须带一条 post-state 断言，否则删掉。
> ⚠ **不要**用 `assertDoesNotThrow`：它接受 `null` 返回，而 `null` 会在 `CatalogMgr.java:547` NPE 再落回 `System.exit(-1)`。必须断言对象本身。
> ⚠ 整卷回放不可行：`LocalJournal.getMaxJournalId()` 恒返回 0，`EditLogFileOutputStream` 不写长度前缀。只能逐 op。
> 用例 7 **预期是红的**，这正是它的价值。若产品侧修复，`isReplay` 分支应 catch `Throwable`——半装好的插件抛的是 `NoClassDefFoundError`，不是 `RuntimeException`。

### C. 「能用」的最大化近似

把「能用」定义为：**正确的 provider 被恰好咨询一次，且它收到的属性 map 与 4.1.3 落盘的逐项相等**。

```java
c.makeSureInitialized();
assertEquals(1, p.calls.size(), "认领该类型的 provider 必须被咨询恰好一次");
assertEquals(expectedProps, p.lastProps(), "从 image 字节到 connector，属性不得增、删、改");
assertEquals("c413", p.lastContext().getCatalogName());
assertEquals(10086L, p.lastContext().getCatalogId());
```

- 桩 provider 必须**记录**收到的 props（`new TreeMap<>(properties)` 防御性快照）。真 connector 反而做不到——你没有办法观察它收到了什么。
- 安装/复位必须在 `@BeforeEach` **和** `@AfterEach` 都做：`ConnectorFactory.pluginManager` 是进程级静态。`registerProvider` 走 `providers.add(0, p)`，会遮蔽真 provider。
- **`use_meta_cache` 期望值恒为 `true`**：`setDefaultPropsIfMissing` 在**加载时**无条件覆写它，4.1.3 和 HEAD 的方法体逐字节相同。所以在 4.1.3 上 `ALTER` 成 `false` 的 catalog，在 4.1.3 自己读回来也是 `true`——**不是迁移回归**。必须写注释，否则下一个人会来"修"它。
- 陷阱：`assertDoesNotThrow(() -> ConnectorFactory.validateProperties(...))` 在无人认领该类型时是**空断言**。必须配 `assertTrue(ConnectorFactory.findProvider(type, props).isPresent())`。

---

## 3. cloud 模式（D3）

**一个约 40 行的测试类即可，不需要 forked surefire、不需要 profile、不需要 `-D`。**

理由：整条 catalog 路径上唯一的 cloud 条件分支是 `dsTypeAdapterFactory` 的尾巴，且**只换 `InternalCatalog` ↔ `CloudInternalCatalog`**。19 个外部 compat 标签、8 个 db 标签、8 个 table 标签全是无条件注册。`ExternalCatalog`/`Database`/`Table`/`CatalogProperty`/`CatalogFactory`/`datasource/plugin/*` 里 grep 不到任何 `CloudMode`/`CloudEnv`。生成器用 `deploy_mode="cloud"` 跑出来的外部 catalog JSON 与非 cloud **字节相同**（已实测）。

必须做对的一件事：

```java
public class Legacy413CloudImageTest {
    static { Config.deploy_mode = "cloud"; }   // 必须是类的第一个成员，不能放 @BeforeClass
```

`GsonUtils` 的注册在**静态初始化块**里跑，`RuntimeTypeAdapterFactory.create()` 会快照 `labelToSubtype`，晚一步翻转就是**静默无效**。`reuseForks=false`（`fe/fe-core/pom.xml:844`，每个测试类独立 JVM）保证了安全。

两个用例：`{"clazz":"InternalCatalog"}` 必须解成 `CloudInternalCatalog`（这同时是"闩锁真的生效了"的探针）；G2 在 cloud JVM 里的 `getType()` 与非 cloud 一致。

**明确不要写**：断言 `Config.isCloudMode()` 的"守卫用例"——它重读可变静态量，永远观察不到 Gson 的陈旧闩锁（已实测：把翻转移到首次触碰 `GsonUtils` 之后，4 个用例里 3 个照样通过）。也不要断言 `{"clazz":"CloudInternalCatalog"}` 能存活——那个标签在两种模式下都注册，不可能因 cloud 原因失败。**不要为 cloud 复制整个外部矩阵**，一条回归守卫足够。

---

## 4. 建议追加：闭合 provider `getType()` 漂移（C1）

这是 UT-only 下**唯一有廉价解的高危残留**。

- 无循环依赖：没有任何 `fe-connector/*/pom.xml` 依赖 `fe-core`。
- 现有守门脚本够不着：`check-connector-imports.sh` 只扫 `fe/fe-connector/*/src`，`check-fecore-metadata-funnel.sh` 只扫 `${ROOT}/src/main/java`，都不读 pom。
- 先例就在要改的文件里：`fe/fe-core/pom.xml:112-226` 已有 14 个 `fe-filesystem-*` 的 `<scope>test</scope>`。
- 断言：对每个 `InitCatalogLog.Type`，`legacyLogTypeToCatalogType(X)` == 实际 provider 的 `getType()`。**两侧对拍，而不是把被测文件里的字面量抄一遍**。
- 代价（都是真的，需权衡）：① Dependency License Review 的 `fail-on-scopes` 含 `development`，maven test scope 属之——`fe-connector-hive → fe-connector-hms → hive-shade` 这条边**必须在一次性 PR 上实测**，不能假设；② nearest-wins 会把 caffeine 从 connector 钉的 `2.9.3` 翻成 fe-core 的 `3.2.3`（仅 UT）；③ 每个启动 `Env` 的测试类会开始加载 8 个 provider。

**建议**：只为这一条断言引入依赖；其余一律用桩 provider（真 connector 无法观察它收到的属性）。若引入，顺带加 `fe-connector-hudi`——它是唯一 `isStandaloneCatalogType()==false` 的 provider，能钉住"sibling-only 连接器永远不能成为可创建的 catalog 类型"。

---

## 5. 覆盖天花板（UT 测不到的，明说）

| # | 测不到 | 残留 |
|---|---|---|
| **C1** | provider `getType()` 漂移（见 §4，有解） | **高**，除非做 §4 |
| **C2** | 发布包缺 `plugins/connector/` → 升级后所有 catalog 死掉而 FE 报健康（`errorMsg` 只在访问时才写，启动时无人扫描） | **高**。UT 外补：`build.sh` 打包后 `test -d`；`loadImage` 后统计"类型无 provider"的 catalog 数并按 ERROR 打日志（`findProvider` 不会触发初始化，是字节惰性的） |
| **C3** | 8 个 iceberg flavor 全部构造同一个 `IcebergConnector`，只有 `validateProperties` 区分，且只看属性不连后端。`iceberg.catalog.type=dlf` / `paimon.catalog.type=dlf` 是**有意移除**——4.1.3 的 DLF catalog 能加载但首次访问失败 | 中。UT 能钉报错文案，钉不住"用户有迁移路径" |
| **C4** | MTMV 新鲜度：翻闸后的 `PluginDrivenMvccExternalTable` 产出的 `MTMVSnapshotIf` 是否与 4.1 基表持久化的可比 | 中，静默错判，无报错 |
| **C5** | classloader 隔离：生产是 child-first，测试类路径是平的，iceberg 与 paimon 的 `MetaStoreProviders` 注册表会合并 | 低（对迁移而言） |
| **C6** | `initialized` 被读取端强制置 false、`lastUpdateTime` 被首次 `makeSureInitialized()` 重写 | 低——初始化前断言，或不断言 |
| **C7** | `System.eixt(-1)` 无法在不杀 fork 的前提下断言，只能低一帧断言 | 低 |
| **C8** | **从未读过一张真实的 4.1 生产 image**。所有"4.1 catalog 的 props 长什么样"都推自 4.1.3 源码；G3 完全是构造的 | **中**。一张客户 dump 出来的 image 就能把它归零——若拿得到，强烈建议加 |
| **C9** | 降级不可逆：HEAD 写出 `"clazz":"PluginDrivenExternalCatalog"`，4.1.3 读不了 | 已决定出范围。但它不是"没测"而是"不可能"，**必须写进 PROVENANCE.txt**，别让人在回滚时才发现 |

---

## 6. 反空跑：变异清单（**不做这节，整套测试可能永远绿**）

已实测：agent 起草的测试里，**3 个在功能被删掉后照样全绿**。变异一律在 scratchpad 复制类并前置到 classpath，**禁止 `git checkout` 还原**。

| 测试 | 必须让它变红的一行改动 |
|---|---|
| 1 锚 | `GsonUtils` 加 `.disableHtmlEscaping()` → 红；加 `.serializeNulls()` → 红；删 `.enableComplexMapKeySerialization()` → 红 |
| 2 blob | 把 compat 标签 `"HMSExternalCatalog"` 改名 → 红 |
| 3 反填 | (a) 删反填块 → 红（`getType()` 变 `"plugin"`）；(b) 删 `case TRINO_CONNECTOR:` → 红（变 `"trino_connector"`）；(c) 把 `logType = PLUGIN` 改成空操作 → **只有直接读 protected `logType` 才红**，读 `getType()` 抓不到 |
| 4 整图 | (a) `MODULE_NAMES` 里把 `datasource` 挪到 `resources` 之后 → 红；(b) 删 `MetaReader` 的 DEPRECATED 跳过分支 → 红；(c) 夹具里去掉 `resources` 索引 → **只有 `assertNotSame` 形式才红**，`assertNotNull` 活下来 |
| 5 标签 | (a) 删任一 `registerCompatibleSubtype` → 红；(b) 把 `IcebergExternalTable` 的目标从 `PluginDrivenMvccExternalTable` 改成 `PluginDrivenExternalTable` → **只有 `assertSame` 才红**，`instanceof` 活下来 |
| 6 回放 | (a) 注释掉 `registerProvider(...)` → **必须靠"provider 被咨询次数"计数器才红**；只断言类名永远绿（两个分支返回同一个类）；(b) 删 `CatalogFactory` 的 `props.putIfAbsent(type)` → 创建路径断言红；(c) 把任一无断言的 `replay()` 指向不存在的 catalog id → **必须红**，否则说明该 op 没带 post-state 断言，应删 |
| 7 降级 | 还原 `CatalogFactory` 的 try/catch → 红。**"吞掉异常返回 null"的修法也必须红**——这要求断言对象类型而非 `assertDoesNotThrow` |
| cloud | (a) 删 cloud 分支的 `registerCompatibleSubtype(CloudInternalCatalog, "InternalCatalog")` → 红；(b) 删测试自己的 `static { deploy_mode="cloud" }` → 红（这是证明闩锁真的落下的唯一手段） |

---

## 7. 验收标准

**必过**：用例 1–7 + cloud 类全绿；§6 每条变异**实际执行**并留日志证明变红；`labels.txt` 由脚本生成且与 4.1.3 源码一致。

**允许记账**：用例 7（回放降级）预期红，作为产品侧待修项显式记账，不得静默跳过。

**门禁写法**（两个已实证的假绿陷阱）：
- `run-fe-ut.sh --run` 在选择器匹配不到类时报 BUILD SUCCESS 且零测试（`-DfailIfNoTests=false`）。门禁必须断言"每个类都出现一行 `Tests run: N ... - in <FQCN>` 且行数==类数"，并**故意跑一个不存在的类名**确认能被抓。根治：加 `-Dsurefire.failIfNoSpecifiedTests=true`。
- 不要用退出码判断任何 image 相关工具。

---

## 8. 待办前置

1. ~~`fe/fe-core/src/test/java/org/apache/doris/datasource/upgrade/` 下有 3 个 agent 写的类，其中 `LegacyFullImageUpgradeTest` 调用了不存在的方法，导致 fe-core testCompile 整体失败~~ → 已移至 scratchpad `agent-written-drafts/`，工作区已恢复。其中的思路可复用，但需按本方案重写。
2. worktree `/mnt/disk1/yy/git/doris-413`（`7126cf65d96`）已建好，是夹具生成的基座。用完 `git worktree remove` 即可。
3. 待你裁决：§0.1 的 D2 是否改判、§4 的 connector test 依赖是否引入、C8 能否搞到一张真实 4.1 生产 image。
