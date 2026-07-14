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

**commit**：`7b726e224e0`

---

## 2026-07-14（三） — 🚨 离线实验跑出事实：**三条外部 metastore 路径今天全是坏的**（推演证实 + 修正）

**做了什么**
- 主 session **亲手写并跑了离线 classloader 实验**（`loader-probe-reproduction.java.txt`，本目录）：
  真实 `output/fe/lib`（576 jar，`doris-fe.jar` 按 `start_fe.sh` 钉最前）当父 + 真实
  `output/fe/plugins/connector/{hive,iceberg,hudi,paimon}` 的 jar 组 **真实的**
  `ChildFirstClassLoader`（fe-extension-loader 那份）+ 真实 parent-first 名单。**不是模拟，跑的是生产拓扑。**
- 6 路调研 + 6 路对抗验证（12 agent）。**对抗验证推翻了 4/6 路的关键结论**，见下。

### 🔴 实验结论（全部实测，非推演）

| 路径 | 结果 | 失败机理 |
|---|---|---|
| **Glue**（hive/iceberg/hudi/paimon **全部 4 个**） | ❌ **坏** | Glue 类**不在任何插件 zip**（shade jar 里 `com/amazonaws` = **0** 条目）→ 父回退 → **app loader** 定义 → 它实现的是 **app 的** `IMetaStoreClient`；而 `RetryingMetaStoreClient` 由 **child** 定义 → 其 ctor 的 **`checkcast IMetaStoreClient`（child 的）** → **ClassCastException** |
| **DLF via hive/iceberg/hudi** | ❌ **坏** | 类身份**是对的**（shade 打进了插件 zip → child 定义，`isAssignableFrom`=**true**）；但 shade 里那份 = **上游原版**，**缺** ctor `(Configuration, HiveMetaHookLoader, Boolean)`（实测只有 `(HiveConf)` / `(HiveConf,…)`）→ `getProxy` 内 `getDeclaredConstructor` **精确匹配** → **NoSuchMethodException** |
| **DLF via paimon** | ❌ **坏** | paimon zip **既无 aliyun jar 也无 hive-catalog-shade** → 父回退 → app 的（hive **3.1.3**）；而 paimon 私有 shade 是 hive **2.3.7** → 身份 + 版本**双重**不匹配 |

### ⚠️ 由此**修正** `design.md` 的两处错误（结论方向不变，机理写错了）

1. **§3.2b 说失败在 `getProxy` 的 `asSubclass`** → **错**。字节码实证：
   `JavaUtils.getClass` 只做 `Class.forName(name, true, TCCL)` 后 `areturn`（泛型擦除，**无** asSubclass）。
   真正的失败点是 `RetryingMetaStoreClient` 私有 ctor **offset 127 的 `checkcast IMetaStoreClient`**。
2. **R-2 说「今天生效的 DLF 客户端是 fe-core vendored 那份」** → 仅对 **app loader** 成立。
   **插件路径上 child-first 挑中的是 shade 里那份**（hive/iceberg/hudi 的 zip 里都有）→
   **「静默换实现」这件事已经在 cutover 时发生了**，不是将来的风险。

### 🚨 最重要：**这是本分支 cutover 引入的回归，不是历史遗留**

- **master 是好的**：`fe/fe-core/.../hive/ThriftHMSCachedClient.java`（本分支**已删**）
  `:29-30` **直接 import** 两个客户端，`:676/:678` 用 **`ProxyMetaStoreClient.class.getName()` /
  `AWSCatalogMetastoreClient.class.getName()`** —— **编译期类引用**，编译器保证类在调用方 classpath 上；
  且全在 fe-core → **app loader** → caller/callee/`IMetaStoreClient` 三者自洽 → 能跑。
- **本分支坏了**：`fe-connector-hms/.../ThriftHmsClient.java:917/:922` 把它们改写成了**字符串字面量**
  → 编译期保证**静默消失** → 落到插件 child loader → 三种坏法。
- **DLF 那个 ctor 的来历**：`fe/fe-core/.../ProxyMetaStoreClient.java:182` 注释
  **`// morningman: add this constructor to avoid NoSuchMethod exception`** —— Doris 自己给上游打的补丁。
  master 靠 `start_fe.sh:355` 把 `doris-fe.jar` 钉最前让**打了补丁那份赢**；插件 child-first 让**原版赢** → 补丁失效。
- **影响面**：`hive.metastore.type` 是用户写在 `CREATE CATALOG` 里的普通属性
  （`HmsClientConfig.java:37/:43/:46`）→ **任何 Glue/DLF catalog 首次使用必然撞**，非边角路径。

### 🔬 完整证明链（字节码级，全部亲验）

```
ThriftHmsClient:644   TCCL := 插件 child loader
ThriftHmsClient:910   RetryingMetaStoreClient.getProxy(conf, hook, "类名字符串")
  └─ 3-arg 重载 offset 7/12/17: 组 Class[]{Configuration, HiveMetaHookLoader, Boolean}
  └─ JavaUtils.getClass(name, IMetaStoreClient.class)     ← #149 常量池，从 child 解析
       └─ Class.forName(name, true, TCCL)                 ← child-first；找不到→父回退
  └─ JavaUtils.newInstance offset 94: Class.getDeclaredConstructor(...)  ← 精确匹配，不做子类型适配
  └─ ctor offset 127: checkcast IMetaStoreClient (child 的)
```

### 🧭 六路调研结论（对抗验证后）

| 路 | 结论 | 验证判定 |
|---|---|---|
| **Ranger `HiveOperationType`** | ✅ **是纯死代码**！唯一用途是填一个全仓**零读取**的 `ROLE_OPS`（`RangerHiveAuditHandler.java:55,57-63`）。真编译**双向**验证：删 12 行后摘掉 shade jar 仍 `javac` 通过；不删则恰好报 1 错就是该 import。**→ D-3 决策消失，直接删** | SOUND |
| **DLF spike** | ❌ **路线 A/B 双双证伪**：`ProxyMetaStoreClient` 是 **hive3-only**（javap 实证实现了 `createCatalog`/`createISchema`/`getValidWriteIds` 等 hive3 专有方法），而 paimon 私有 shade 是 hive **2.3.7**（那些 api 类 grep 计数=0）。包名 `hive2` 是**误导性遗留名** | MOSTLY_SOUND |
| **零引用依赖** | ⚠️ 前轮「5 项全零引用」**错 3 项**。真可删只有 `hudi-hadoop-mr`。**`kryo-shaded` 绝不可删**（`WorkloadSchedPolicy.java:32,287,298` 经 minlog 真实调用）。`avro` 删声明但 jar 删不掉（三方传递） | MOSTLY_SOUND |
| **Glue 搬迁** | 可行但 3 个 blocker：① `ConfigurationAWSCredentialsProvider.java:89` 读 `Config.aws_credentials_provider_version`（fe-common，插件不依赖它）② fe-core `HiveGlueMetaStoreProperties.java:24` **反向** import vendored Glue ③ **3 个文件 import 裸 `org.apache.thrift.TException`**，而 fe-connector-hms 编译期**无 libthrift** → 直接搬**必编译失败**（改 shade 前缀即可；**绝不能加 libthrift**，那正是 shade 要防的冲突）。checkstyle **零工作量**（`suppressions.xml:65` 是路径正则，自动跟着走） | MOSTLY_SOUND |
| **5 个属性类** | ✅ 已是**死代码**可直接删（"hms" 走 `SPI_READY_TYPES` → plugin supplier → `getMetastoreProperties()` 永不被调）。但 `HiveTable.java`/`HMSResource.java` 两个 **GSON 持久化活类**只为拿 String 常量而 import 它们 | MOSTLY_SOUND |
| **fe-common** | 可解：`CatalogConfigFileUtils` 只删 `loadHiveConfFromHiveConfDir` 那一半（另一半 `loadConfigurationFromHadoopConfDir` 服务**所有**外表 catalog，必须留）。⚠️ `loadHiveConfResources` **不是死代码**（IcebergConnector:707 / PaimonConnector:376 真调），且现语义是把 `new HiveConf()` 的**上千条默认值**灌进插件 HiveConf → 换实现**必须显式签字**（硬约束 2） | MOSTLY_SOUND |

**未决**：见 HANDOFF「待用户签字」。**尚未动任何生产代码。**

**commit**：（本次文档更新）

---

## 2026-07-14（四） — 🔀 用户拍板：**删掉 thrift 一代 Glue/DLF**，任务定性改变

**用户指示**：*「把 glue 和 dlf 这两个功能先删掉，不再支持了，不作为需要迁移的内容。」*
**范围签字**：**只删「走 HMS thrift 协议的那一代」**，保留 iceberg 原生 Glue + DLF 2.0 REST。

**为什么翻案（且这次翻案是合规的）**：`design.md §4.1` / 原 tasklist 立过一条硬约束
「**禁止**『反正可能已经坏了 → 干脆删功能』」。那条约束成立的前提是**没人知道它到底坏没坏**。
本轮离线实验**实测证明四条路径全坏**（见上一条），用户在**信息充分**的前提下决策不修直接删 ——
这正是那条约束要保护的东西，不是绕过它。

**⇒ 方案塌缩为【纯删除】**：不做守门测试 · 不做 paimon-on-DLF · 不做客户端搬迁 · 不需要降级档 ·
总判据自然归零 · fe-core 全程只减不增（铁律 A/B 天然满足）。原「阶段 2/3 搬迁」全部作废。

**用户否决的（记下来别回头做）**：守门测试「不做」· paimon-on-DLF「不做」。

### 🔴 本轮新发现：**第四个实例 —— iceberg 原生 Glue 的 AK/SK 路径也是坏的**

主 session 先前告知用户「iceberg 原生 Glue 今天是好的」——**这个说法不完整，已当场更正**。
只查了 `GlueCatalog` 类本身在插件包里（在），**漏查了它按名加载的凭证类**：

`IcebergCatalogFactory.java:559-560` 在 AK/SK 分支把 `client.credentials-provider` 设为
`com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x`（**住 fe-core，不在插件包**）
→ iceberg 的 `AwsClientProperties` 用 child loader 按名加载 + `asSubclass` 到 **child 的**
`software.amazon.awssdk...AwsCredentialsProvider` → **CCE**。**实测 `isAssignableFrom = false`。**

- ✋ IAM-role 分支（`AssumeRoleAwsClientFactory.class.getName()`，**编译期类引用**）**没病** —— 又一次印证
  「编译期类引用 vs 字符串字面量」就是这一整类 bug 的分水岭。
- ✋ 默认凭证链分支（不填 AK/SK）**没病**。
- **修法 = T-20**：把这 **50 行**搬进 `fe-connector-iceberg`（是**出** fe-core，合铁律 A），进插件包即自愈。

### 本轮调研新增的关键事实（4 路清单 + 4 路对抗验证）

- **Glue 树 38 个文件里，只有 37 个可删** —— `ConfigurationAWSCredentialsProvider2x.java` 归**保留路径**（T-20 搬走）。
  同目录的 `ConfigurationAWSCredentialsProvider.java`(v1) + `...Factory.java` 归 thrift 一代（删）。
- 🔴 **「删分支」会静默走错路**：`getMetastoreClientClassName` 是 `if(dlf)…else if(glue)…else→HiveMetaStoreClient`。
  光删分支 ⇒ `hive.metastore.type=glue` 落到 `else` ⇒ **静默去连普通 HMS**。**必须改成显式抛异常。**
- `aws-java-sdk-glue` 为 vendored 树**独占**（树内 712 处引用，树外 0）→ 可随树删。
- `createV1` 的**唯一调用点**是 `ConfigurationAWSCredentialsProvider.java:78`（本次删除对象）→ 删后成死代码。
  ✋ `createV2`/`getV2ClassName` 仍活（`S3Properties.java:350,363,391,406`）。
- 删 `AWSGlueMetaStoreBaseProperties` 对 `SHOW CREATE CATALOG` **脱敏是字节中性的**：其唯一敏感字段
  `glueSecretKey` 的三个别名被**保留的** `S3Properties.java:104-106` 逐字覆盖。
- iceberg 的 `aws.catalog.credentials.provider.factory.class` 这个 key **只被被删的 thrift 树读**
  （`AWSGlueClientFactory.java:114-118` / `AWSGlueConfig.java:33-34`）；iceberg 原生 `AwsClientProperties`
  **从不读**它（javap 已证）→ 删除**行为中性**。

### ⚠️ 落刀前必须先解决的两个未决问题（对抗验证挖出）

1. **T-54 删 `hudi-hadoop-mr`**：`orc-core-1.8.4`（`fe/lib` 实装 jar）的**公开 API 直接依赖
   `hive-storage-api` 的类** → 删了会不会炸 orc？**查清前不许删。**
   （前一轮只查了「fe-core 自己的源码引用」，方法论有系统性缺口。）
2. **T-51 删 `HMSBaseProperties`**：「已是死代码」证据链**有洞** —— `PluginDrivenExternalCatalog.java:156`
   设 supplier，但 `createConnectorFromProperties()` 在 **:129** 就被调（**早于** :156）→ 有无真实调用窗口？

**commit**：（本次文档更新）
