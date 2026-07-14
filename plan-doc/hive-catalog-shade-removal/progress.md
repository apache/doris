# 📜 进度记录 — 剔除 `hive-catalog-shade`

> **Append-only 日志**：只往下追加，**不覆盖不删改**（覆盖式的「下一步」在 [`HANDOFF.md`](./HANDOFF.md)）。
> 每条格式：`## YYYY-MM-DD — 标题` + 做了什么 / commit / 结论 / 踩坑。

---

## 2026-07-14 — 立项 + 事实基线（零代码改动）

**做了什么**
- 10-agent 工作流：6 路侦察（fe-core 静态用法 / 运行时需求 / 引入历史 / 其它消费者 / 与 paimon-hive-shade 的关系 /
  连带 hack）+ **3 路对抗验证**（compile-break / runtime-NoClassDefFound / packaging-and-scope）+ 综合。
- 主 session **亲验**了所有要落进持久文档的关键事实（可复现命令见 `design.md §7`）。
- 建立本任务文档空间（README / design / tasklist / progress / HANDOFF）。

**结论（推翻了立项时的假设）**
- ❌ **假设「hive/iceberg/paimon 都迁到插件了 → shade 已冗余可删」= 错。**
  3 路对抗验证**全部**推翻了「可以直接删」：
  - **编译**：fe-core `src/main` **26 个文件**仍 import hive 类（Glue 38 文件树 / DLF 客户端 / 5 个 HiveConf 属性类 /
    `DefaultConnectorContext` / `RangerHiveAuditHandler`）；测试 4 个。
  - **运行时**：`fe/lib` 是所有插件的**父加载器**，且 `AWSCatalogMetastoreClient`（**shade jar 里 0 个条目**，唯一
    定义 = fe-core vendored 源码）与 paimon 的 `ProxyMetaStoreClient` 是**按名反射加载、只存在于父**的。
  - **打包**：`fe-common` 的 shade 是 **`provided`** → 删 fe-core 依赖后 fe-common **编译绿、运行炸**，
    且它在**每个外表 catalog** 的路径上。
- ✅ **假设「fe-core 是从 fe-common 传递拿到 shade」= 错**（实跑 maven 否掉）：Maven 从不传递 `provided`；
  fe-core 只有**一条 depth-1 compile 边**。所以删 `fe-core:437-440` **确实**能把 jar 踢出 `fe/lib` ——
  这正是它现在**不能**删的原因。
- ✅ **`fe-connector-paimon-hive-shade` 与 `hive-catalog-shade` 无依赖关系** —— 是同一招数的插件私有版重新实现
  （改名前缀故意不撞：`org.apache.doris.paimon.shaded.thrift` vs `shade.doris.hive.org.apache.thrift`；
  hive 2.3.7 vs 3.1.3）。paimon 当年（P5）明确**拒绝**了全局 shade；iceberg 反而按 **D-060** 签字复用它。

**关键踩坑（写下来防止重复踩）**
- ⚠️ **验证陷阱**：`doris-shade/.../target/hive-catalog-shade-3.1.2-SNAPSHOT.jar`（本地构建产物）**已经**改名了
  fastutil；拿它验证会误判「fastutil hack 已死」。**构建实际解析的是 3.1.1**，那份里 fastutil 是**裸的 10653 个类**。
- ⚠️ `mvn dependency:tree` 在本 reactor 里**必须加 `-am`**，否则 `${revision}` 解析失败报假错
  （侦察 agent 一度据此断言「这里跑不了 dependency:tree」——是错的）。
- ⚠️ 别信 `dependency:analyze`：`AliyunDLFBaseProperties.java:71` 把 `DataLakeConfig.CATALOG_PROXY_MODE` 当**注解
  String 常量**用，javac 会**内联**进字节码 → 字节码扫描报「未使用」，但 `javac` 仍会因 `import` 失败。

**被验证阶段纠正的侦察结论**（记下来，别回退到错版本）
- 侦察说「fe-core 里 ~40 个文件 import hive」→ 实测 **26**（main）+ 4（test）。
- 侦察说「shade 是 `org.apache.hadoop.hive.*` 的唯一提供者」→ **错**：`hudi-hadoop-mr → hudi-hadoop-common →
  hive-storage-api:2.8.1` 也往 `fe/lib` 塞 hive 类。删完 shade，`fe/lib` **仍不是** hive-free
  → 半填充命名空间（风险 R-4，比干净缺失更难查）。

**未决 / 待用户拍板**：D-1（Glue 归属）· D-2（paimon DLF，**需先 spike**）· D-3（Ranger `HiveOperationType`）。

**commit**：`1a9104ee85f`（文档立项）

---

## 2026-07-14（补） — 用户定调「按需要保留设计」+ 由此挖出的机制推演

**用户指示**：*「Glue-on-HMS 和 paimon-on-DLF 现在我无法验证，你先按照『需要保留』来设计任务。」*

⇒ 方案基调固定（写进 `design.md §5 D-4` / `tasklist.md 阶段 0` / `HANDOFF.md`）：
1. 禁止「反正可能坏了 → 删功能」；两条路径**必须**改完后仍可用。
2. 禁止无论证的静默换实现（R-2）。
3. **e2e 不可得 ⇒ 用离线 classloader 测试代偿（HCS-01）**；阶段 6 是**单向门**，归不了 0 就停在降级档
   （新增 `design.md §4.1` 档 0/1/2/3），**不许为了删而删**。

**为定这个基调而做的探查，挖出三条硬事实（全部亲验）**：
1. `hive-catalog-shade-3.1.1.jar`（**127 MB**）**确实被打进 hudi / iceberg 的 plugin-zip**（`unzip -l` 实查）
   → 插件里有**自己那份** `IMetaStoreClient` / `RetryingMetaStoreClient`。
2. `ThriftHmsClient.java:644` 把 **TCCL 钉到插件（child）loader**，`:910` 才调 `RetryingMetaStoreClient.getProxy`。
3. fe-core vendored 的 `ProxyMetaStoreClient` 有 **2193 行**，且它**自己还 import 阿里云 DLF SDK 的其余部分**
   （`com.aliyun.datalake.metastore.common.*` / `.hive.common.utils.*`）—— 那 **1963 个 `com/aliyun` 类只在
   shade jar 里**。搬 DLF ≠ 搬一个文件，而是要搬**整个 SDK 闭包**。

**⚠️ 由此得出的机制推演（`design.md §3.2b`）—— 可能是本任务最重要的发现**：

`ChildFirstClassLoader` 的 parent-first 名单**不含** `org.apache.hadoop.hive` / `com.amazonaws`
（实读 `ChildFirstClassLoader.java` + `ConnectorPluginManager.java:64`）。于是：
- `IMetaStoreClient` / `RetryingMetaStoreClient` → **child 定义**（插件 zip 里有 shade）
- `AWSCatalogMetastoreClient` → 插件 lib 里**没有**（shade jar 里 Glue 类 = **0 个条目**）→ 父回退 → **app 定义**
- `getProxy` 内部 `Class.forName(name, TCCL).asSubclass(IMetaStoreClient.class)`，其中 `IMetaStoreClient` 来自 **child**
- ⇒ app-loaded 的 Glue 客户端实现的是 **app 的** `IMetaStoreClient` ≠ **child 的** → `asSubclass` 失败
  → **`ClassCastException`**

**⇒ Glue-on-HMS 与 paimon-on-DLF 今天很可能就是坏的**（paimon 更糟：child 是 hive **2.3.7**，父是 **3.1.3**）。
**这不是猜测，是可以离线证实/证伪的** → 这就是新的 **HCS-01**（本任务最高优先级）。

**⇒ 对方案的意义（两头都成立，所以方案是严格改进）**：把 Glue/DLF 客户端**搬进插件**会让 caller
（`RetryingMetaStoreClient`）与 callee（客户端）落到**同一个 child loader、同一份 `IMetaStoreClient` 身份**上。
- 若 HCS-01 证明今天已坏 → 搬迁 = **修复一个线上 bug**；
- 若 HCS-01 证明今天能跑（推演有误）→ 搬迁 = **行为保持**，且由 HCS-01 的测试守住。

**新增/改写的 task**：HCS-01（改为离线 classloader 测试，替代原「用户真集群 e2e 探测」）·
**HCS-30a（DLF 搬迁 spike，新增）** · HCS-30b/33（新增）· HCS-22/71（验收改为「离线必过 + 显式声明未 e2e 覆盖项」）。

**commit**：（本次文档更新 commit 待填）
