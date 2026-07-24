# 设计 — `fe/fe-connector` 剥离 `hive-catalog-shade`（迁自带精简局部 shade）

> 稳定参考文档。状态/进度看 `tasklist.md` + `progress.md`；下一步看 `HANDOFF.md`。
> 基线 2026-07-16，分支 `catalog-spi-11-hive`。所有 `file:line` 以 HEAD `grep` 为准。

---

## 1. 背景：`hive-catalog-shade` 是什么、为什么存在

`org.apache.doris:hive-catalog-shade`（源码库 `doris-shade`）是一个用 `maven-shade-plugin` 把整个 Hive 访问闭包
（`hive-metastore:3.1.3` + `hive-serde` + `hive-exec:core` + `iceberg-hive-metastore:1.10.1` + `paimon-hive-connector:1.3.1` + DLF）
打成一个 jar 的胖包，核心动作是**把 `org.apache.thrift` 重定位到 `shade.doris.hive.org.apache.thrift`**。

- **为什么要重定位**：Doris 内部 thrift = **0.16.0**（`fe/pom.xml:295`），doris-gen 桩（`TFileScanRangeParams`/`TIcebergFileDesc` 等）按 0.16.0 编译；而 Hive 3.1.3 的 metastore 客户端桩按 **thrift ~0.9.x** 编译，二进制不兼容（`TFramedTransport` 换包到 `.transport.layered`、`TBase`/scheme 契约漂移）。同一个 `org.apache.thrift` 命名空间容不下两个版本 → 给 Hive 那套单独换私有包。
- **为什么把 iceberg/paimon 的 hive-metastore 也打进去**：`maven-shade` 只改写它**打进 jar 的类**的字节码；iceberg 的 `HiveCatalog`、paimon 的 `HiveCatalog` 都经 `HiveMetaStoreClient` 访问 HMS、字节码带 `org.apache.thrift.*` 引用，必须一起打进来才会被统一重定位。
- **为什么胖**：122 MB 里真正为它而存在的（Hive 客户端 4 MB + 重定位 thrift 0.5 MB）不到 1.6%；其余是 paimon-bundle（~94 MB，内部又重打包了 hadoop/guava/fastutil）、完整 hadoop（58 MB）、古董 fastutil 6.5.x（37 MB）、多平台原生库、datanucleus/derby 服务端等——大量**重复**和**无用**。

**这就是要脱离它的动机**：胖、有版本冲突隐患（fastutil 6.5.x child-first 遮蔽）、且插件化后一个插件被迫背另一个连接器的全套依赖。

---

## 2. 现状依赖图（2026-07-16 核实）

`fe/fe-connector/` 下对 `hive-catalog-shade` 的**真实(非注释)直接依赖只有两个**：

```
hive-catalog-shade  (org.apache.doris, 版本由 fe/pom.xml dependencyManagement 钉)
      ▲                                   ▲
      │ 直接 (fe-connector-iceberg:93)    │ 直接 (fe-connector-hms:43, compile 无 scope → 传递)
      │                                   │
fe-connector-iceberg               fe-connector-hms   ◄── 共享 plain-HMS 客户端模块
   (要 iceberg-hive-metastore)             │
                                           │ compile 传递给
                          ┌────────────────┼────────────────┐
                    fe-connector-hive  fe-connector-hudi  fe-connector-iceberg
                                        (都 depend fe-connector-hms)
```

- `fe-connector-hms/pom.xml:43` 直接依赖，**无 `<scope>` = compile = 传递**。它是被 hive / hudi / iceberg-on-HMS **共用**的 plain-HMS 客户端。
- `fe-connector-iceberg/pom.xml:93` 另有一条**直接**依赖，用途是 `iceberg-hive-metastore`（iceberg→HMS 的胶水），外加它链接的重定位 thrift 基座。
- 结论：**只迁 iceberg 只摘掉它自己那条直接依赖；hms（及经 hms 的 hive/hudi/iceberg）仍被 shade 牵着。要整个 fe/fe-connector 脱钩，必须先迁 `fe-connector-hms`。**

repo 全局(HEAD)真实消费者共 4 个 + 版本钉：`fe-connector-hms`、`fe-connector-iceberg`、`be-java-extensions/avro-scanner`、`be-java-extensions/java-udf`、`fe/pom.xml`（钉）。**后三者不在本任务范围**。

---

## 3. 方案：镜像 `fe-connector-paimon-hive-shade`

paimon 已经走通这条路（`fe/fe-connector/fe-connector-paimon-hive-shade/pom.xml`，324 行；设计 `plan-doc/fix-c-hms-thrift-design.md`）：建一个插件私有 shade 模块，bundle metastore 客户端闭包 + `libthrift`，**重定位 `org.apache.thrift` 到插件私有前缀**（paimon 用 `org.apache.doris.paimon.shaded.thrift`），consumer 换依赖。要点原样照搬：

- **只 shade metastore-client 闭包**，不 shade 连接器主模块（否则会把连接器里对 host 0.16.0 `TSerializer`/`TBase` 的调用也重定位，序列化 doris-gen 结构就断了）。
- **libthrift 排除出 consumer 插件**（保持 `org.apache.thrift` 对 host 0.16.0 **parent-first**，doris-gen 路径不动），只在 shade 模块里 bundle + 重定位。
- **artifactSet 排除** host/插件已提供的库：`org.apache.hadoop:*`、guava、protobuf、slf4j、log4j、commons-*、gson、jackson、caffeine（避免 child-first 重复类）。
- **防御性重定位 fastutil**（`it.unimi.dsi.fastutil` → 私有前缀），躲开 6.5.x 遮蔽。
- **`org.apache.paimon.* / org.apache.hadoop.*` 绝不重定位**（SPI 发现 + `HiveConf`/`Configuration` 类同一性）。
- consumer 侧还要在 `plugin-zip.xml` 保留对 `org.apache.thrift:libthrift` 和 `org.apache.doris:fe-thrift` 的 exclude。

目标 jar 体积预估 ≈ **13–15 MB**（对比 122 MB）。**Phase 0 核实的精确 bundle 清单**（`javap` + `jar tf` + 核查 agent 交叉验证，2026-07-16）：

| bundle 进 shade（重定位 thrift） | 版本 | 谁需要 | 为什么 |
|---|---|---|---|
| `org.apache.hive:hive-standalone-metastore` | **3.1.3** | 全部 | 提供 `org.apache.hadoop.hive.metastore.api.*`（`ThriftHiveMetastore` + ~200 结构体）+ `IMetaStoreClient`/`MetastoreConf`/`MetaStoreUtils`/`HadoopThriftAuthBridge`/`txn.TxnUtils`。**⚠️ 是 standalone-metastore（真实现），不是 `hive-metastore:3.1.3`（13 类空壳）** |
| `org.apache.thrift:libthrift` | **0.9.3**（内联钉，非 managed 0.16.0） | 全部 | 补丁客户端直接 import 的 9 个 thrift 类；重定位到 `shade.doris.hive.org.apache.thrift` |
| `org.apache.thrift:libfb303` | **0.9.3** | 全部 | `ThriftHiveMetastore.{Iface,Client}` 继承 `com.facebook.fb303.FacebookService`，不链接则生成客户端 link 失败 |
| `org.apache.hive:hive-common` | **3.1.3** | 全部 | `org.apache.hadoop.hive.conf.HiveConf`（`HmsConfHelper.createHiveConf` / iceberg `IcebergCatalogFactory` 用）；带 `hive-shims` |
| `org.apache.hive:hive-storage-api` | 2.7.0 | 全部 | `ValidWriteIdList` 等（hms txn / write-ACID 路径），hive-common/standalone-metastore 都不含 |
| `org.apache.hive:hive-serde` | **3.1.3** | 仅 iceberg | iceberg `HiveSchemaUtil` 建表/提交时用 `serde2.typeinfo.*`/`objectinspector.*`（118 处 byte-ref） |
| `org.apache.iceberg:iceberg-hive-metastore` | 1.10.1 | 仅 iceberg | `org.apache.iceberg.hive.HiveCatalog`（hms flavor）；排除 iceberg-core（插件已直接带 1.10.1） |

**关键 exclude**：`org.apache.paimon:*`（自有 shade）、完整 `org.apache.hadoop:*`（插件自带 child-first）、`it.unimi.dsi:fastutil`（防御性重定位而非 bundle）、`org.apache.hive:hive-exec`（见下）、`com.aliyun:*`/DLF、derby/datanucleus/bonecp/HikariCP/orc、bouncycastle、jersey/jaxb、arrow/parquet/avro（hive-serde 重传递）、guava/protobuf/jackson/slf4j/log4j/commons（host parent-first）、iceberg-core/api/caffeine（插件已带）。

**两处原设计假设已纠正**：
1. **hive-exec 不进插件**。connector 源码里 `org.apache.hadoop.hive.ql.*` 全是**字符串常量**（ORC/Parquet 格式类名写进 HMS `StorageDescriptor`，FE 从不加载该类；BE 原生读）。`HmsWriteConverter:270-279`、`HiveScanPlanProvider:92`、`HiveConnectorMetadata:139/141`、`HiveTableFormatDetector:43/44` 均为字符串，零 `import`。commit `e7eae85faa4` 的 host `hive-exec:core` 是 CREATE FUNCTION 的 `hive.ql.exec.UDF` 契约，主机侧独立事，与插件无关。
2. **DLF 已是死代码**，直接不装。`iceberg.catalog.type=dlf` / `metastore.type=dlf` 已在 `IcebergCatalogFactory.resolveCatalogImpl` + `HmsClientConfig.REMOVED_METASTORE_TYPES` 移除并有守卫测试拦截。⚠️ `fe-connector-iceberg/pom.xml:138-145` 那段"支持 DLF / port-now"注释**过期**，动码时顺手订正（别让它误导 shade 的 artifactSet 去保留 DLF）。

---

## 4. 设计决策 —— **已定案（Phase 0，2026-07-16，用户签字）**

> **决策速查**（下方各 D 的详细背景保留；此处为最终结论）：
> - **D1 = A（共享一个 shade）** ✅ 用户签字。iceberg `HiveCatalog` 按类名建 `HiveMetaStoreClient` → 命中补丁客户端 → 二者须共用同一份重定位 thrift + 元数据 API 类身份 → 必须一个共享 shade。代价：hive/hudi 多背 ~1.1MB 永不加载的 iceberg 类（非泄漏，仅字节）。
> - **D2 = 3.1.3** ✅ 用户签字。保持与全局 shade 行为一致。
> - **D3 = 复用 `shade.doris.hive.org.apache.thrift`** ✅ 用户签字。补丁客户端 + `ThriftHmsClient` 本就 import 此前缀 → **零源码改动**；过渡期两包同名同版本（libthrift 0.9.3）字节一致、谁生效都不撕裂（新前缀反而会在过渡期让新旧两份 `HiveCatalog` 绑不同前缀 → 旧份被优先加载即 ClassCastException）。
> - **D4 = KEEP_IN_HMS**（`javap` 定案）。补丁客户端字节码对 thrift 的全部 48 处引用**已全部是重定位名** `shade.doris.hive.org.apache.thrift.*`，零 raw、零按名反射。故它留在 `fe-connector-hms`、不搬进 shade、不改写，只需精简 shade 在其 classpath 上复现同名 thrift 即可。
>
> **配套计划微调（红队条件 1）**：iceberg **摘全局 shade** 与 **接精简 shade** 须放**同一次提交（原子切换）**，消除"过渡期两份 `HiveCatalog` 并存"的不确定（实测全局 37821B vs 精简 1.10.1 37853B，谁生效随 classpath 序 → 顺带把 iceberg.hive 对齐插件自带 iceberg-core 1.10.1）。
> **验证条件（红队条件 2）**：Phase 1/4 须跑重部署类加载冒烟（每插件 `metastore.api.Table` 与 `shade.doris.hive.org.apache.thrift.TException` **各仅一份**）+ hive/hudi/iceberg-on-HMS e2e + Kerberos/filter-hook 按名反射路径（D5）。

### 4.x 决策原始背景（存档，勿据此再议——结论以上方速查为准）

### D1 — 一个共享 shade 还是两个独立 shade？
- **选项 A（推荐，共享）**：一个 `fe-connector-hms-hive-shade`，bundle `hive-metastore` + `hive-serde` + `hive-common` + `libthrift`（重定位）**外加 `iceberg-hive-metastore`**。`fe-connector-hms` 和 `fe-connector-iceberg` 都依赖它。iceberg-hive-metastore 随包搭车 → hive/hudi 多背 ~1 MB 用不到的 iceberg 类，但**只需一个 thrift 私有命名空间**、最省事、就是「全局 shade 去肥」版。
- **选项 B（分开）**：`fe-connector-hms-hive-shade`（客户端+thrift，hive/hudi/iceberg 共用）+ iceberg 的 iceberg-hive-metastore 单独 shade。**难点**：iceberg-hive-metastore 的 `HiveCatalog` 必须和客户端用**同一份**重定位 thrift，而 `libthrift` 不能在两个 shade 各 bundle 一份（child-first 重复类）。要么 iceberg-hive-metastore 进同一个 jar，要么引用 hms shade 的重定位 thrift 而不重打包 → 复杂。
- **建议**：选 A。Phase 0 交用户签字。

### D2 — bundle 的 hive-metastore 版本？
全局 shade 用 **3.1.3**；paimon 局部 shade 用 **2.3.7**。plain-HMS 插件当前经全局 shade 跑的是 **3.1.3**。
**默认取 3.1.3 以保持行为不变**（尤其 vendored 补丁客户端 + `HiveVersionUtil` 对 Hive 版本敏感——见 D4）。Phase 0 确认 vendored 客户端所依赖的版本后签字。

### D3 — thrift 重定位前缀
新 hms 私有前缀 **`org.apache.doris.hms.shaded.thrift`**（区别于 paimon 的 `org.apache.doris.paimon.shaded.thrift`、全局的 `shade.doris.hive.org.apache.thrift`，三者永不撞）。

### D4 — 🔴 vendored 补丁 `HiveMetaStoreClient.java`（**本任务最大未知**）
`fe/fe-connector/fe-connector-hms/src/main/java/org/apache/hadoop/hive/metastore/HiveMetaStoreClient.java` 是 **Doris 源码写的、版本感知的**补丁客户端（`import org.apache.doris.datasource.hive.HiveVersionUtil`），当前靠 jar 排序 overlay/shadow 掉 shade 里那份未打补丁的同名类（iceberg pom 注释：修 Hive-3 `@cat#` db 标记对 Hive-1/2 metastore 的兼容）。

- **问题**：它编译在 fe-connector-hms 里（**不会被 shade 重定位**）。若它的字节码引用 `org.apache.thrift.transport.*` 等被重定位掉的类，重定位后：shade 里的基类引用 `org.apache.doris.hms.shaded.thrift.*`，而这个补丁子类仍引用原包 `org.apache.thrift.*` → **命名空间撕裂**。paimon **没有**这个问题（它的客户端全来自 SDK jar，可整体 shade）。
- **Phase 0（FCL-02）必须**：`javap` 出 vendored 客户端对 `org.apache.thrift.*` 的全部引用面；据此决定：
  - 若它只碰高层 API（不碰 transport/被搬走的类）→ 可能可保留在原包；
  - 若它碰被重定位的类 → 需把它的**源码也纳入 shade 模块**（在 shade 里编译再重定位），或改写它避免直接触 thrift transport。
- **这是 Phase 0 的头号交付物**，方案定不下来别进 Phase 1。

### D5 — TCCL classloader pin（**别回归**）
plain-hive HMS 客户端创建须钉插件 classloader：`ThriftHmsClient.doAs` 钉 plugin loader（非 `getSystemClassLoader()`）、`HmsConfHelper.createHiveConf` 须 `setClassLoader(插件loader)`（否则 `loadFilterHooks` 经 conf 缓存 CL 反射 `MetaStoreFilterHook` split-brain）。
参考 memory `catalog-spi-plugin-tccl-classloader-gotcha`（TeamCity #991951，commit `92004ef1d0d`；`test_string_dict_filter` 2026-07-11 踩坑）。**重定位不能破坏这套按名反射**——Phase 4 e2e 必须覆盖 filter hook / string dict filter / kerberos 路径。

### D6 — 其它库共存
mirror paimon 的 artifactSet 排除（hadoop/guava/slf4j/log4j/commons/caffeine），并核对打包后插件 zip 无重复 `HiveConf`/`libthrift`/`hive-metastore`。

---

## 5. 风险清单

| # | 风险 | 触发 | 缓解 |
|---|---|---|---|
| R1 | vendored 补丁客户端命名空间撕裂（D4） | 它引用被重定位的 thrift 类 | Phase 0 先 `javap` 定面；必要时把它纳入 shade 编译 |
| R2 | TCCL 反射回归（D5） | 重定位后 filter hook / doAs 按名反射失效 | e2e 覆盖 string dict filter / filter hook / kerberos |
| R3 | iceberg-on-HMS 与 hms 客户端 thrift 命名空间不一致 | 分开 shade 各 bundle thrift | 选 D1-A（共享一份重定位 thrift） |
| R4 | 打包出现两份 `paimon-hive`/`hive-metastore`/`libthrift`（child-first 重复类） | consumer 同时保留旧 raw 依赖 | 照 paimon：raw 依赖 `<optional>true`，plugin-zip exclude，打包后 `unzip -l` 断言唯一 |
| R5 | hive/hudi 静默受影响（它们经 hms 传递） | 只测 hms、iceberg 漏测 hive/hudi | Phase 1 gate 必须连 hive+hudi build+UT；Phase 4 e2e 三连 |
| R6 | 行为变更（Caffeine/hive 版本漂移） | 换版本或换传递依赖 | 默认锁 3.1.3；FE 启动 + 缓存冒烟（FCL-41） |

---

## 6. 铁律（继承主线，勿违）

1. **fe-core / fe-connector 源码只出不进**：不为「编译过」把逻辑挪进 fe-core util；shade 模块只装第三方 jar + （必要时）vendored 客户端源码。
2. **禁 deletion-scaffolding 式搬迁**：遇到「删依赖导致编译不过」，停手重分析真实归属，别就近搬。
3. **commit / PR 文案全英文**（上游社区看）；plan-doc / 本空间中文。
4. **每完成一个 Phase 就更新 HANDOFF + commit**（不只阶段边界）。
5. **大改动用 clean-room 对抗 review**（多 agent 先独立判断、后交叉核对历史结论）。
6. **iceberg-on-HMS 新能力必须配 e2e**（异构 HMS 目录跑 INSERT/DELETE/MERGE/read 断言与独立 iceberg 目录同结果）。

---

## 7. 参考

- 范式模块：`fe/fe-connector/fe-connector-paimon-hive-shade/pom.xml`
- 范式设计：`plan-doc/fix-c-hms-thrift-design.md`
- 兄弟任务：`plan-doc/hive-catalog-shade-removal/`（fe-core 版，已完成阶段 1–5）
- shade 机制/体积拆解：本 session 分析（见 `progress.md` 开篇「起源」）
- 相关 memory：`catalog-spi-plugin-tccl-classloader-gotcha`、`fe-core-source-isolation-iron-rules`、`catalog-spi-connector-cache-framework-caffeine-coherence`、`hms-iceberg-delegation-needs-e2e`
