# 19. 把 Elasticsearch 专有的逃生门与通用扫描节点里的 ES 分支归位

> **优先级**：第五优先级（中立化） ｜ **风险**：中 ｜ **前置依赖**：无
> **影响模块**：`fe-connector-api`（删一个方法、加一个可选能力接口）、`fe-connector-es`（承接两处 ES 逻辑）、`fe-core`（净删 ES 专有分支，改一个 REST 端点的取用方式）
> **预计改动规模**：6 个主源文件 + 2 个测试文件；新增 1 个接口文件约 40 行，`fe-core` 主源净减约 8 行、净增约 12 行中立代码，`fe-connector-es` 净增约 45 行，新增单测约 120 行
> **行号说明**：本文行号以 `7ff51a106f0` 为准，核对时以符号名为准，不要以行号为准。

## 一、一句话说明这个任务要解决什么

有两处 Elasticsearch 专有的东西长在了所有连接器共用的公共位置上：一个是所有连接器都继承的入口接口 `Connector` 上挂着一个只有 ES 会实现的 REST 透传方法；另一个是通用扫描节点 `PluginDrivenScanNode` 里还留着两段按 ES 格式名硬判的分支（EXPLAIN 打印 `ES terminate_after`、往 ES 专属 thrift 属性里塞 `limit`）。本任务把第一处摘成一个可选能力接口交给 ES 连接器实现，把第二处经既有的两个委派钩子搬进 ES 连接器，引擎侧只以中立的合成键提供三个事实（下推的 limit、过滤是否已全部下推、BE 每批行数）。

## 二、背景：现在的代码是怎么写的

### 2.1 REST 透传方法挂在入口接口上

`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/Connector.java:292-305`：

```java
    /**
     * Execute a REST passthrough request against the underlying data source.
     *
     * <p>Connectors that expose HTTP endpoints (e.g., Elasticsearch) can
     * override this to proxy REST requests from FE REST APIs.</p>
     * ...
     */
    default String executeRestRequest(String path, String body) {          // :303
        throw new UnsupportedOperationException("REST passthrough not supported by this connector");
    }
```

全仓库核实结果（`grep -rn executeRestRequest --include=*.java`）：

- 唯一实现方：`fe/fe-connector/fe-connector-es/src/main/java/org/apache/doris/connector/es/EsConnector.java:77-80`，一行转发给 `EsConnectorRestClient.executePassthrough`；
- 唯一调用方：`fe/fe-core/src/main/java/org/apache/doris/httpv2/restv2/ESCatalogAction.java:86` 与 `:100`，两个 REST 端点 `/rest/v2/api/es_catalog/get_mapping`、`/rest/v2/api/es_catalog/search`，分别拼出 `<table>/_mapping` 与 `<table>/_search` 两个 ES 路径；
- 该端点在 `ESCatalogAction.java:67-70` 已经按类型名收窄：`!"es".equals(((PluginDrivenExternalCatalog) catalog).getType())` 就直接回 `badRequest("unknown ES Catalog: ...")`。

也就是说：路径与响应形状都是 ES 的，收窄判定在端点里，而方法本身却挂在每个连接器都继承的入口接口上，其他 7 个连接器继承到的是一个「调用即抛异常」的方法（本仓库共 8 个 `Connector` 实现：es / hive / hudi / iceberg / jdbc / maxcompute / paimon / trino）。

同一个 `Connector` 接口里已经有现成的可选能力写法（`Connector.java:339-349`），javadoc 里还明确写了不要用 `instanceof`：

```java
    /**
     * Returns this connector's incremental metadata-change source, or {@code null} if it has none.
     * A capability-probe getter ... never via {@code instanceof}. ...
     */
    default ConnectorEventSource getEventSource() {                        // :347
        return null;
    }
```

引擎侧对应的取用方式在 `fe/fe-core/src/main/java/org/apache/doris/datasource/MetastoreEventSyncDriver.java:132`（`getConnector().getEventSource()` 判空）。

### 2.2 通用扫描节点里的两段 ES 分支

文件：`fe/fe-core/src/main/java/org/apache/doris/datasource/scan/PluginDrivenScanNode.java`。

第一段，EXPLAIN 输出（`:559-564`）：

```java
            // Show ES terminate_after optimization when limit is pushed to ES
            if (limit > 0 && conjuncts.isEmpty()
                    && "es_http".equals(props.get(PROP_FILE_FORMAT_TYPE))) {
                output.append(prefix).append("ES terminate_after: ")
                        .append(limit).append("\n");
            }
```

第二段，往 thrift 参数里塞 ES 属性（`:1815-1836`）：

```java
    public void createScanRangeLocations() throws UserException {
        super.createScanRangeLocations();
        ConnectorScanPlanProvider scanProvider = resolveScanProvider();
        if (scanProvider != null) {
            Map<String, String> props = getOrLoadScanNodeProperties();
            onPluginClassLoader(scanProvider, () -> {
                scanProvider.populateScanLevelParams(params, props);       // :1823
                return null;
            });
        }
        pruneConjunctsFromNodeProperties();                                // :1827
        // Push down limit to ES via terminate_after optimization. ...
        if (limit > 0 && limit <= sessionVariable.batchSize && conjuncts.isEmpty()
                && params.isSetEsProperties()) {                            // :1832-1833
            params.getEsProperties().put("limit", String.valueOf(limit));
        }
    }
```

几个已核实的关键事实：

1. `"es_http"` 这个格式名是 ES 连接器自己写进扫描节点属性的（`EsScanPlanProvider.java:184` 的 `nodeProps.put("file_format_type", "es_http")`，以及 `EsScanRange.java:91`）；
2. `es_properties` 里的 `"limit"` 键是 BE 契约：`be/src/format/table/es/es_scan_reader.h:46` 的 `KEY_TERMINATE_AFTER = "limit"`，被 `es_scan_reader.cpp:79` 与 `es_scroll_query.cpp:123` 读取，拼成 ES 的 `terminate_after=` 查询串。这个字符串不能改；
3. `params.setEsProperties(...)` 本来就是 ES 连接器自己做的（`EsScanPlanProvider.java:371`），也就是引擎在 `:1834` 是往连接器刚设进去的那张 map 里补写一个键；
4. 两个承接钩子都已存在并已被 ES 实现：`ConnectorScanPlanProvider.java:474 populateScanLevelParams` / `:490 appendExplainInfo`，ES 侧实现在 `EsScanPlanProvider.java:362` 与 `:410`；
5. 引擎向连接器传递「引擎侧事实」的合成键机制已有先例：`PluginDrivenScanNode.java:132-146` 定义了 `__native_read_splits` / `__total_read_splits` / `__explain_verbose` 三个键，`:538-543` 在委派前拷一份属性 map 注入，paimon 连接器按字面量消费并有单测锚定（`PaimonScanExplainTest.java:63` 等）；
6. **`conjuncts.isEmpty()` 的时机是有讲究的**：`:1832` 读到的 `conjuncts` 是 `:1827` 剪枝**之后**的，剪枝逻辑（`pruneConjunctsFromNodeProperties`，`:1873-1882`）会把因为含 CAST 而从未交给连接器的谓词**保留**下来，所以「conjuncts 空」等价于「所有过滤都真的被连接器接走了」。这是正确性判据，不是顺手写的条件；
7. 同一个文件在 `:490-499` 自己写下了这条规则：`NO source-name branch belongs in this generic node ... Connector-SPECIFIC EXPLAIN stays delegated to ConnectorScanPlanProvider.appendExplainInfo`。

## 三、为什么这是个问题

**违反的原则**：通用接口层与通用扫描节点里禁止出现数据源专有代码。第二处尤其明显——违规代码和写着「这里不许有源名分支」的注释在同一个文件里相隔 60 行。

**真实后果**：

- 通用节点里硬编码了某个连接器的格式标签 `"es_http"` 与某个连接器的 thrift 字段 `es_properties`。任何一个新连接器想要「LIMIT 提示」这类能力，今天都必须回到 `fe-core` 加一段 `"xxx_http".equals(...)` 分支——这正是本次整治要终结的模式；
- 一半判据在引擎、一半渲染在引擎，导致两半已经不一致：EXPLAIN 那段（`:560`）**没有** `limit <= sessionVariable.batchSize` 这个判据，而真正下推那段（`:1832`）有。于是当 `batch_size` 小于 LIMIT 时，用户在 EXPLAIN 里看到 `ES terminate_after: 5000`，实际却没有任何 `limit` 被发给 BE。这是既有缺陷，不是本任务引入的（本任务按原样搬迁，见第七节与最后的待拍板问题）；
- `Connector` 那个方法让 7 个与 ES 无关的连接器都继承到一个「一调用就抛 `UnsupportedOperationException`」的方法，能力的有无只能靠异常发现，而不是像 `getEventSource()` 那样能判空探测。

## 四、用一个最小例子说明

一条最普通的 ES 查询：

```sql
select * from es_catalog.my_db.my_index limit 5;
```

| 用户写了什么 | 现在实际发生什么 | 应该发生什么 |
|---|---|---|
| `... limit 5`，然后看 `EXPLAIN` | 通用扫描节点先问连接器要 ES 的那几行（`ES index:` 等），随后自己判断 `file_format_type == "es_http"`，再自己打印 `ES terminate_after: 5` | 引擎只告诉连接器两个中立事实：这个扫描的 LIMIT 是 5、过滤已全部下推；`ES terminate_after: 5` 这一行由 ES 连接器自己打印 |
| `... limit 5` 真正执行 | ES 连接器先把 `es_properties` 设进 thrift，引擎随后往这张 map 里补一个 `limit=5` | ES 连接器在设 `es_properties` 时自己决定要不要加 `limit=5`（引擎额外告诉它 BE 每批行数是多少） |
| `curl '.../rest/v2/api/es_catalog/get_mapping?catalog=es_catalog&table=my_index'` | 端点先判 `type == "es"`（保留），再调所有连接器都继承的 `Connector.executeRestRequest` | 端点仍先判 `type == "es"`，再取 `getRestPassthrough()`，只有 ES 连接器返回非空 |

再换个角度：**假设我要新增一个连接器 X，它也想在 EXPLAIN 里报告自己把 LIMIT 下推给了远端**。今天我必须打开 `fe-core` 的 `PluginDrivenScanNode`，在 `:559` 旁边加一段 `"x_native".equals(props.get("file_format_type"))` 的分支；改完之后，`fe-core` 就多知道了一个连接器的格式名。本任务做完之后，我什么公共代码都不用碰：三个合成键对每个连接器都注入，我在自己的 `appendExplainInfo` 里读就行。

## 五、解决方案

### 5.1 目标状态

**（一）REST 透传变成可选能力接口。** 新增 `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/rest/ConnectorRestPassthrough.java`：

```java
/** A connector that can proxy an HTTP REST request to its underlying source. */
public interface ConnectorRestPassthrough {
    /**
     * @param path 相对路径（由调用端按该数据源的 REST 形状拼好）
     * @param body 请求体，GET 风格请求可为 null
     * @return 响应体原文
     */
    String executeRestRequest(String path, String body);
}
```

`Connector.java` 删掉 `executeRestRequest`（`:292-305`），换成与 `getEventSource()` 同形的能力探针：

```java
    /**
     * Returns this connector's REST passthrough capability, or {@code null} if it has none.
     * A capability-probe getter (mirrors {@link #getEventSource()}): the caller probes for null,
     * never via {@code instanceof}. Default null, so no connector inherits a throwing method.
     */
    default ConnectorRestPassthrough getRestPassthrough() {
        return null;
    }
```

`EsConnector` 改成 `implements Connector, ConnectorRestPassthrough`，保留原来的一行方法体，另加 `getRestPassthrough() { return this; }`。

`ESCatalogAction` 把内部 `handleRequest` 的函数参数从 `BiFunction<CatalogIf, String, String>` 换成 `BiFunction<ConnectorRestPassthrough, String, String>`，在既有的 `"es"` 类型判定之后取一次能力、判空后再调；两个端点的 lambda 从 `((PluginDrivenExternalCatalog) catalog).getConnector().executeRestRequest(...)` 简化为 `rest.executeRestRequest(...)`。**`"es".equals(...)` 那句一个字不动。**

**（二）扫描节点的两段 ES 分支搬进 ES 连接器。** `PluginDrivenScanNode` 新增三个合成键（形态、注释风格照抄 `:132-146`）：

```java
    private static final String PUSHDOWN_LIMIT_KEY = "__pushdown_limit";
    private static final String ALL_CONJUNCTS_PUSHED_KEY = "__all_conjuncts_pushed";
    private static final String SESSION_BATCH_SIZE_KEY = "__session_batch_size";
```

两条委派路径都注入这三个键（EXPLAIN 路径在 `:538` 的 `explainProps` 上追加；thrift 路径新拷一份 map）。为此 `createScanRangeLocations` 里必须把 `pruneConjunctsFromNodeProperties()` 提到 `populateScanLevelParams` 委派**之前**，因为「过滤是否已全部下推」这个事实只有剪枝后才成立。

顺序调换的安全性依据（已逐个核实）：现存五个 `populateScanLevelParams` 实现——`HiveScanPlanProvider.java:449`、`HudiScanPlanProvider.java:409`、`IcebergScanPlanProvider.java:1777`、`PaimonScanPlanProvider.java:1355`、`EsScanPlanProvider.java:362`——都只按固定键读传入的属性 map 并写 `params`，没有任何一个读 `conjuncts`；而 `pruneConjunctsFromNodeProperties` 只改 `conjuncts` 并读已缓存的属性结果。因此交换顺序对 thrift 参数逐字节不变。

ES 连接器侧：`EsScanPlanProvider.populateScanLevelParams` 在把 `esProperties` 设进 params 之前，按 `limit > 0 && batchSize > 0 && limit <= batchSize && "true".equals(allPushed)` 决定是否 `esProperties.put("limit", ...)`；`appendExplainInfo` 在现有三行 ES 输出之后，按 `limit > 0 && "true".equals(allPushed)` 追加 `ES terminate_after: <limit>`（**故意不加 batch 判据**，逐字保留既有行为，并写 ATTN 注释说明这一不对称是既有状态）。

### 5.2 改动清单

| 文件 | 做什么 |
|---|---|
| `fe-connector-api/.../connector/api/rest/ConnectorRestPassthrough.java` | **新增**。单方法可选能力接口，javadoc 中立、不点名任何数据源 |
| `fe-connector-api/.../connector/api/Connector.java` | 删 `:292-305` 的 `executeRestRequest`；加 `getRestPassthrough()` 默认返回 null（放在 `getEventSource()` 附近，注释对齐其写法） |
| `fe-connector-es/.../es/EsConnector.java` | `implements Connector, ConnectorRestPassthrough`；保留 `:77-80` 方法体并加 `@Override`；新增 `getRestPassthrough() { return this; }` |
| `fe-core/.../httpv2/restv2/ESCatalogAction.java` | `handleRequest` 的函数参数类型换成 `ConnectorRestPassthrough`；`:71` 之后探测能力、为 null 返回 `badRequest`；两个 lambda 去掉重复的 catalog 强转。**不动 `:67-70` 的类型判定** |
| `fe-core/.../datasource/scan/PluginDrivenScanNode.java` | 新增三个合成键常量；`:538-543` 的 `explainProps` 追加注入；删 `:559-564` 整段 ES EXPLAIN 分支；`createScanRangeLocations` 把剪枝提前、委派时传注入后的属性副本、删 `:1829-1835` 整段 ES limit 分支；抽一个包内可见的纯静态注入辅助方法便于单测 |
| `fe-connector-es/.../es/EsScanPlanProvider.java` | 新增三个与引擎侧逐字节相同的键常量；`populateScanLevelParams` 承接 `limit` 写入；`appendExplainInfo` 末尾承接 `ES terminate_after` 一行 |
| `fe-connector-es/src/test/.../EsScanPlanProviderTest.java` | 新增 5 个用例（见第六节） |
| `fe-core/src/test/.../datasource/scan/` 下新增一个测试类 | 断言合成键注入辅助方法的取值映射（照 `PluginDrivenScanNodeLimitStripTest` / `PluginDrivenScanNodeExplainStatsTest` 的纯静态断言写法） |

### 5.3 明确不要顺手做的事

- **不要中立化 `ESCatalogAction` 里的 `"es".equals(...)` 判定。** 这个端点本身就是 ES 兼容 API（路径拼 `/_mapping`、`/_search`，响应形状是 ES 的），按类型收窄是正确边界；一旦中立化，就等于把这个端点开给所有声明了 REST 直通能力的连接器，白扩攻击面。
- **不要动 `mapFileFormatType` 里的 `case "es_http": return TFileFormatType.FORMAT_ES_HTTP;`（`PluginDrivenScanNode.java:1970`）。** 那是格式名到 thrift 枚举的通用查表，和 `parquet` / `orc` / `text` 并列，thrift 枚举还受有线格式约束；它不是源专有分支。
- **不要顺手修 EXPLAIN 与实际下推的 batch 判据不一致**（第三节第二条）。本任务的验收基线是 EXPLAIN 文本逐字不变；要修就另立一项，同时改 `external_table_p2` 的用例并想清楚 `batch_size` 变化时的用户预期。
- **不要顺手给 `ESCatalogAction` 补插件类加载器钉扎**。当前 REST 线程直接调进插件、未 pin TCCL，这是既有状态，与本任务无关；要修单独立项评估。
- **不要动 `pruneConjunctsFromNodeProperties` 的剪枝算法本身**（含 CAST 谓词保留那段），本任务只调它的调用位置。
- **不要为「通用节点里不许出现源名」这条不变量加 shell/正则门禁**。本仓已有结论：这类门禁只适合存在性与前缀类不变量，判断语言语义时误报比漏报更毒。用注释 + 单测 + 评审。

## 六、怎么验证

**ES 连接器单测**（`EsScanPlanProviderTest`，现有 `testAppendExplainInfoShowsEsIndex` 就是模板）：

1. `populateScanLevelParams`：属性含 `__pushdown_limit=5`、`__session_batch_size=1024`、`__all_conjuncts_pushed=true` → `params.getEsProperties().get("limit")` 等于 `"5"`。这条同时锚定 BE 契约字符串 `"limit"`；
2. `__all_conjuncts_pushed=false` → `es_properties` 里**没有** `limit` 键（这是正确性用例：过滤没全下推却限量，会少返回行）；
3. `__pushdown_limit=5000`、`__session_batch_size=1024` → 没有 `limit` 键；
4. 三个键都缺失（老调用方/纯单测 map）→ 没有 `limit` 键；
5. `appendExplainInfo`：`__pushdown_limit=5` + `__all_conjuncts_pushed=true` → 输出**逐字**含 `ES terminate_after: 5\n`（前缀、冒号后一个空格）；`__all_conjuncts_pushed=false` 或键缺失 → `notContains "ES terminate_after"`。

**变异验证**（改实现应让上述用例变红）：把用例 1 的比较写成 `limit >= batchSize` → 用例 3 失败；去掉 `__all_conjuncts_pushed` 判据 → 用例 2、5 的负例失败；把 `"limit"` 键名写成 `"terminate_after"` → 用例 1 失败。

**引擎侧单测**（新增类，照 `PluginDrivenScanNodeLimitStripTest` 的纯静态写法）：断言注入辅助方法在 `limit=-1` / `limit=5`、`allPushed` 真假、`batchSize` 各取值下写出的键值字符串正确，且键名字面量与 ES 连接器侧一致（两侧各写死同一批字面量，这与 paimon 现有合成键的做法同构）。剪枝先于委派这一顺序无法用纯静态方法覆盖，用 ATTN 注释写清依据（5.1 节列的五个实现均不读 `conjuncts`），交评审把关。

**编译门禁**（最强单一信号，必须全反应堆含测试源）：

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -T 1C test-compile
```

不得加任何跳过测试编译的参数。这一步能一次性抓出「有没有别的模块还在调 `Connector.executeRestRequest`」。

**跑单测**（必须禁用构建缓存，否则 surefire 会被静默跳过）：

```
mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -Dmaven.build.cache.enabled=false \
    -pl fe-connector/fe-connector-es,fe-core -am test \
    -Dtest='EsScanPlanProviderTest,PluginDrivenScanNode*Test'
```

**端到端回归**：`regression-test/suites/external_table_p2/es/test_es_query_predicate_correctness.groovy:94-135` 六处断言（`contains "ES terminate_after: 5"` / `"3"`、`notContains "ES terminate_after"`，ES 7 与 ES 8 各三处）必须仍通过。这些断言用 `contains`，对行的相对位置不敏感——本任务会让 `ES terminate_after` 从 `pushdown agg=` 之后移到 ES 其它几行之后（即 `pushdown agg=` 之前）。已核实仓库内没有把整段 EXPLAIN 存成 golden 文件的 ES 用例（`grep -rn terminate_after regression-test --include=*.out` 无命中），所以只有这一个 p2 套件受影响。该套件需要 ES 7/8 容器，本地无集群时须显式标注为「待集群验证」，不得当作已通过。

**REST 端点手工验证**（无自动化覆盖，`ESCatalogAction` 在仓库里既无单测也无回归用例）：部署后各调一次 `get_mapping` 与 `search`，确认响应体与改动前一致；另外用一个非 ES 目录名调一次，确认仍返回 `unknown ES Catalog`。

## 七、风险与回退

- **EXPLAIN 文本漂移（主要风险）**：`ES terminate_after: ` 这个前缀连空格都不能变，否则 p2 用例失败。对策是连接器单测断言整行，且搬迁时用复制粘贴而不是重写字符串拼接。
- **行位置变化**：如上所述，这一行在 EXPLAIN 里的位置会前移。当前所有断言都是包含式，风险可接受；但若后续有人加了整段比对，会暴露。
- **剪枝与委派顺序调换**：依据已在 5.1 列出（五个实现都不读 `conjuncts`）。若担心，可分两个 commit：先只搬 EXPLAIN 那半（不需要调顺序），确认 p2 通过后再搬 thrift 那半。
- **合成键字符串两侧漂移**：引擎与 ES 各自定义常量，靠单测里硬编码字面量锚定——与 paimon 现有三个合成键同风险同对策。
- **新旧版本混搭部署**：删掉 `Connector.executeRestRequest` 后，若用旧的 ES 插件包配新 `fe-core`，插件里的 `EsConnector` 多出一个不再属于任何接口的方法（运行期无害），但 `getRestPassthrough()` 会返回 null，两个 REST 端点会返回 `badRequest` 而不是原来的 500。插件与 FE 同版本部署是既有约定，此处只需在提交说明里写明。
- **一个需要用户拍板的取舍**：EXPLAIN 那半缺少 `limit <= batch_size` 判据（第三节第二条），会让用户在 `batch_size` 较小时看到一个并未真正生效的 `ES terminate_after`。本文默认**逐字保留**这个不一致（验收基线是文本不变）；若允许在同一轮里把连接器侧 EXPLAIN 判据补齐成与实际下推一致，则需接受 `batch_size` 小于 LIMIT 时 EXPLAIN 输出发生变化（现有 p2 用例的 `limit 5` / `limit 3` 远小于默认 `batch_size`，不会变红）。
- **回退**：三处改动彼此独立（REST 能力接口 / EXPLAIN 一行 / `es_properties` 的 `limit`）。建议至少分两个 commit：一个只做 REST 能力接口，一个只做扫描节点归位，便于单独 revert。

## 八、相关背景

- `plan-doc/connector-public-interface-cleanup/audit-report.md`：附录 A.2「公共模块里的数据源专有语义」下第 18 条——通用扫描节点里的 ES 分支（EXPLAIN 的 `terminate_after` 与 `esProperties` 的 limit），严重度高；第 19 条——`executeRestRequest` 这个 ES 专用逃生门，严重度中。同一报告第 8.1 节的清单表格里 `Connector.executeRestRequest` 那一行明确写了「那个 REST 端点自己的类型判定不动」。
- `plan-doc/connector-api-spi-design-review-2026-07-25.md:159`、`:193`、`:646`：把 REST 透传与 SQL 透传一并移出通用入口接口的原始建议（本任务只做 REST 那半；SQL 透传 `executeStmt` + `getColumnsFromQuery` 不在本任务范围）。
- 合成键先例：`PluginDrivenScanNode.java:132-146`（定义）、`:538-543`（注入）、`PaimonScanPlanProvider.java:1393` 起（消费）、`PaimonScanExplainTest.java`（按字面量锚定的单测）。
- 能力探针先例：`Connector.java:339-349`（`getEventSource`，javadoc 明确「never via instanceof」）、`MetastoreEventSyncDriver.java:56` 与 `:132`（引擎侧判空取用）。
- 通用节点里禁止源名分支的规则原文：`PluginDrivenScanNode.java:470-472` 与 `:490-499`。
