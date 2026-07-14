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

---

## 2026-07-14（四）— 阶段 1 完成：iceberg 原生 Glue 凭证类搬出 fe-core + 改包

**commit**：`2cd01ada8df`（code，独立）· 基线 `570bbc89cf8`
**方法**：10-agent 侦察（源码 / 全仓引用 / 插件 pom+dependency:tree / 真实 jar javap / Trino 对照）
+ 4 路对抗验证（每路独立复核，不采信侦察结论）。10/10 完成，0 error。

### 做了什么

`ConfigurationAWSCredentialsProvider2x`（50 行）
`fe/fe-core/.../com/amazonaws/glue/catalog/credentials/`
→ `fe/fe-connector/fe-connector-iceberg/.../org/apache/doris/connector/iceberg/glue/`

**用户 2026-07-14 拍板「改成 Doris 自己的包」**（非原样搬）。理由：老 FQN 今天 100% 坏 →
无「工作中的用户配置」会被打破；而 `com.amazonaws.glue.catalog.**` 整棵树马上删光，
插件里留同名包会让后人 grep 时误判「没删干净」。

改动面 4 文件（git 识别为 rename）：新类 · fe-core 删除 · `IcebergConnectorProperties.java:142` 常量值 ·
`IcebergCatalogFactoryTest.java:556` 断言。**pom 零改动**。

### 验证（信实测不信推演）

- `mvn -pl fe-connector/fe-connector-iceberg -am test`：**BUILD SUCCESS**，`IcebergCatalogFactoryTest`
  **63/63 通过**，checkstyle 0
- `mvn -pl fe-core -am test-compile`：**BUILD SUCCESS**，checkstyle 0（证明 fe-core 删掉该类无编译面破坏）
- `tools/check-connector-imports.sh`：**exit 0**（HiveVersionUtil 两条是已知误报）
- **决定性探针**（真实 `ChildFirstClassLoader` + 真实插件 jar 183 个 + app-model 父加载器 576 jar，
  同一次运行的前后对照）：

```
--- BEFORE (旧 FQN，仍在 stale doris-fe.jar 里) ---
  resolved from : app-model
  VERDICT native-Glue creds (old): isAssignableFrom = false
--- AFTER (搬进插件后) ---
  resolved from : ChildFirstClassLoader
  VERDICT native-Glue creds: isAssignableFrom = true   [FIXED]
  create(Map) returned : org.apache.doris.connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x
  resolveCredentials() : accessKeyId=AKIA_PROBE
RESULT: PASS
```

### 侦察挖出的事实更正（**文档此前是错的**）

1. **模块路径是嵌套的**：`fe/fe-connector/fe-connector-iceberg`，**不是** `fe/fe-connector-iceberg`。
   `-pl fe-connector/fe-connector-iceberg -am`（`-am` 必填，否则 `${revision}` 假错）。
2. **真实报错不是 CCE**：是 `IllegalArgumentException "Cannot initialize ..., it does not implement
   software.amazon.awssdk.auth.credentials.AwsCredentialsProvider"` —— iceberg `AwsClientProperties`
   的 `isAssignableFrom` 门禁（`Preconditions.checkArgument`）在 `checkcast` **之前**就拦下了。修法不受影响。
3. **`auth:2.29.52` 早在插件 compile classpath 上**（经**未声明**的传递路径：declared `s3` → `auth`）。
   反向对照实验：从 classpath 剥掉 `auth-2.29.52.jar` → `package ... does not exist`，证明确是它在起作用。
   全 classpath 扫描 `AwsCredentialsProvider.class` 只有 1 份 → 无 shaded 副本污染。
4. **新包命中 parent-first 前缀**：`org.apache.doris.connector.` 在 `CONNECTOR_PARENT_FIRST_PREFIXES` 里
   → 子加载器先问父。**成立靠「父无此类 → CNFE → 回退子加载器 findClass」**，已实测；
   与插件内其它所有类同一模式（`IcebergCatalogFactory` 等皆如此）。

### Trino 对照（架构层）

Trino **不走「传类名 → 反射加载」这一跳**：自己实现 `TrinoGlueCatalog`，在 Guice module 里
**直接构造实例** `StaticCredentialsProvider.create(AwsBasicCredentials.create(k, s))` 交给
`GlueClient.builder().credentialsProvider(instance)`。实例自带 `Class`，在插件加载器里解析一次 → 天然无 split-brain。

**未照搬的理由**：我们用 iceberg 官方 `GlueCatalog`，它只接受属性 map，唯一注入点就是类名字符串；
要绕开须自实现 `AwsClientFactory` —— 而 iceberg 加载 `client.factory` **同样按名反射**，同样一跳，代码更多。
搬 50 行是最小修法。

**另证**（javap，排除「根本不需要这个类」的可能）：iceberg 的 `AwsClientProperties` **确有**内建静态 AK/SK
阶梯（`credentialsProvider(ak, sk, token)` → `StaticCredentialsProvider`），但 Glue client 走的
`applyClientCredentialConfigurations` **只调 `credentialsProvider(String)`**（25 条指令，全扫描确认
3-arg 重载只被 `AwsProperties.restCredentialsProvider` 和 `S3FileIOProperties` 引用）
→ **自定义类是真必需**，不能用 iceberg 原生属性替掉。

### 🆕 侦察挖出的两个遗留项（**清单里原本没有**）

1. **T-21（待用户签字）**：`create()` **静音丢弃 session token**。`IcebergCatalogFactory.java:565-566`
   发 `client.credentials-provider.glue.session_token`、单测 `:559` 断言它，但 `create()` 只造
   `AwsBasicCredentials`，**从不造 `AwsSessionCredentials`** → 临时 STS 凭证认证必失败。
   既有 bug，非搬迁引入；属**行为变更** → 未动。
2. **T-30 落刀前必查**：`IcebergCatalogFactory.java:563-564` 发的
   `aws.catalog.credentials.provider.factory.class` → `ConfigurationAWSCredentialsProviderFactory`
   **指向一个即将被 T-30 删掉的 fe-core 类**（第二条 fe-core→插件按名耦合）。
   T-34 称「iceberg 原生从不读该 key，删除中性」→ **落刀前 javap 复核**。

---

## 2026-07-14（五）— 阶段 2 完成：删 thrift 一代 Glue

**commit**：`e43173eca67`（code，独立）· 基线 `5e6351e6d8a`
**方法**：6-agent 侦察（阻塞项/树闭包/分派/属性类/iceberg 切面/regression 面）+ 5 路对抗验证
+ 1 个专项 agent 查 FE 生命周期。11/11 完成，0 error。

### 规模

**53 文件改动，-11,245 行 / +139 行。** fe-core main 的 `com.amazonaws.*` 引用 **归零**。
总判据 26 → **7**（余下为 DLF/hive 部分）。

### 阻塞项已解除（原 HANDOFF 的「落刀前必查」）

`aws.catalog.credentials.provider.factory.class` 的 `opts.put` 删除**行为中性 = CONFIRMED**，字节码级：
- 拆插件目录**全部 183 个 jar**（侦察只查了 6 个，验证者扩到全量）、**6603 个 class** → 该 key **0 命中**
- **正对照**（关键）：同一 grep、同一语料，`appendGlueProperties` 发的**其它每一个** key 都能命中
  （`client.credentials-provider`→4、`client.factory`→7、`client.region`→2 …）→ 空结果是真信号不是坏 grep
- 唯一读者 `AWSGlueClientFactory.java:115`（`conf.getClass(...)`）在**被删树内**
- `AwsClientProperties.<init>(Map)` 的 javap 显示它读的是**封闭集合**，不含该 key

### 🔴 本轮最重要的发现：拒绝逻辑放错地方会让 FE 起不来

原计划只说「必须改成显式抛异常」，没说放哪。查 FE 生命周期后发现**三个位置后果完全不同**：

| 位置 | 新建 catalog | **edit-log 回放** |
|---|---|---|
| `validateProperties` | ✅ 报错 | ✅ **不跑**（`CatalogMgr:551-553` 有 `!isReplay` 门）→ 安全 |
| `create()` / 连接器构造函数 | ✅ 报错 | 💀 **会跑**（`CatalogFactory:105-113` 两条路径都建 connector）→ 抛异常 → `EditLog.loadJournal:1521-1539` catch-all **`System.exit(-1)`** → **FE 起不来**（`OP_CREATE_CATALOG=320` 不在默认 skip 列表；恢复需运维手改 `fe.conf`） |
| `createClient()` | ✅ 报错 | ✅ 懒加载，不跑 → 安全 |

⚠️ **代码库已有的 lakesoul「已移除」先例（`CatalogFactory.java:141-142`）恰好放在没有 isReplay 门的地方**
→ checkpoint 之后建的 lakesoul catalog 会让 FE 启动失败。**那是个未修的潜在 bug，别照抄它的位置。**

### 另一个发现：光在分派处抛异常根本不会生效

`HiveConnector.createClient:538-546` 的「HMS URI 必填」检查**跑在分派之前**。真实 glue catalog
**不配** `hive.metastore.uris`（只配 `glue.endpoint`/`glue.access_key`），所以：

| 配置 | 只删 glue 分支后的结局 |
|---|---|
| `type=glue`，无 uris（**正常的 glue catalog**） | 报 `"HMS URI ('hive.metastore.uris') is required"` —— 响，但**与 glue 毫无关系**，误导 |
| `type=glue` + 多余的 uris | **静默连普通 HMS** —— 原记录的风险，真实存在但更窄 |

两种结局都不告诉用户「glue 被移除了」→ 都指向同一个修法。**拒绝必须放在 URI 检查之前。**

### 用户 2026-07-14 拍板：两处都加

- `HiveConnectorProvider.validateProperties` → 抛 **`IllegalArgumentException`**
  （`PluginDrivenExternalCatalog:193-199` **只解包这一种**并 `new DdlException(e.getMessage())` 原样透出；
  抛别的类型文案会被包烂。先例 `CacheSpec.checkLongProperty` 同样抛它）
- `HiveConnector.createClient` → 抛 **`DorisConnectorException`**（邻居惯例；fe-core 有 5+ 处专门 catch 它，
  改抛 IAE 会绕过那些 handler），**放在 URI 检查之前**
- 共享的是**文案**（`HmsClientConfig.removedMetastoreTypeError`），不是 throw —— 两处异常类型必须不同

**偏离原计划**：`METASTORE_TYPE_GLUE` 原计划「无消费者→删」，实际**改名保留**为
`METASTORE_TYPE_GLUE_REMOVED` —— 它有了新消费者（识别并拒绝该已移除类型），比内联字面量清楚。

### 覆盖了老用户升级场景

老 glue catalog 从 image 反序列化（`CatalogMgr.read` → GSON 直接建对象，**`CatalogFactory` 全程不参与**），
**永不经** `validateProperties`。所以：重启不受影响（不阻塞启动），第一次查询时由 `createClient`
那处拒绝给出同一句话——而不是 jar 删掉后的 `ClassNotFoundException`。

### 测试

新增 `HmsClientConfigRemovedTypeTest`（2 个用例）。**动机**：侦察发现
`getMetastoreClientClassName` **全仓零测试覆盖** → 拒绝逻辑被误删不会有任何东西变红。
两个用例分别钉「glue 必须被拒且文案点名」与「存活类型 hms/dlf 路由不变」（后者防拒绝逻辑误伤面过大）。

**已做变异验证**：把拒绝条件改成 `if (false)` → 测试**确实转红**
（`AssertionFailedError: hive.metastore.type=glue must be rejected, never silently ignored`）。

⚠️ **自己踩坑并纠正**：变异验证**第一次漏了 `-am`** → `BUILD FAILURE` 是
`org.apache.doris:fe-connector:pom:${revision}` 解析假错、**根本没编译**到测试 → 首次「变异验证通过」的
结论**作废**，加 `-am` 重做才是真的（memory `doris-build-verify-gotchas` 早有记载，仍复发）。

### 验证汇总（信 LOG 不信 exit）

- `mvn -pl fe-core -am test-compile` → **BUILD SUCCESS**，checkstyle 0
- `mvn -pl fe-connector/{fe-connector-hms,fe-connector-hive,fe-connector-iceberg} -am test` → **BUILD SUCCESS**，
  零 Failures/Errors，checkstyle 0
- `tools/check-connector-imports.sh` → **exit 0**
- `grep -rn "com\.amazonaws" fe/fe-core/src/main/java` → **空**

### 对抗验证的 2 个 REFUTED（都是**命题写漏**，非计划有错）

1. 「树自洽，除属性类外无外部引用」→ **REFUTED**：漏列了 `ThriftHmsClient` 的 `GLUE_CLIENT_CLASS`
   反射字符串（存活模块内）。**计划本就要删它**，是命题的豁免清单写漏。
2. 「删树+属性类+createV1 后 fe-core 零 com.amazonaws 且仍能编译」→ **REFUTED**：零引用**成立**，
   但「仍能编译」不成立 —— 会悬空两条硬编译边（`HivePropertiesFactory:38` 的**构造器引用**、
   `DatasourcePrintableMap:61` 的**类字面量**）。二者**本就在计划的删除清单里**，是我的命题把删除集写小了。
   → 实际删除时二者均已处理，`test-compile` 实测绿。

### 阶段 2 遗留

见 `tasklist.md` 「阶段 2 遗留」：regression-test 清理（含**文件级 guard 钉在 glue 专有 conf key** 的坑）·
`test_connection=true` 顺序坑 · 文案里 `Supported types: hms, dlf` 待阶段 3 去掉 `dlf`。

---

## 2026-07-14（六）— 阶段 3 完成：删 thrift 一代 DLF（DLF 1.0）

**commit**：`a0f65c353a9`（code，独立）· 基线 `ca1f0c4f427`
**方法**：6-agent 侦察（含 1 路**安全专项**）+ 5 路对抗验证。11/11 完成，0 error。

### 规模

**43 文件改动，-4,095 / +184。** fe-core 的 `com.aliyun.datalake` **归零**；总判据 **7 → 4**。

### 🔴 本轮最重要的发现：照原计划删会**明文泄漏用户凭证**

原计划 T-44 只写「删 `AliyunDLFBaseProperties`」。但它同时挂着 `DatasourcePrintableMap` 的
**`SHOW CREATE CATALOG` 凭证脱敏注册**。Glue 那次删掉是安全的（S3 属性类逐字覆盖同样别名），
**DLF 不成立**——javap 逐字段确认，4 个敏感别名的覆盖**不均匀**：

| 别名 | 删掉注册后是否仍脱敏 |
|---|---|
| `dlf.secret_key` | ✅ `OSSProperties`/`OSSHdfsProperties` 逐字覆盖 |
| `dlf.catalog.accessKeySecret` | ❌ **无人覆盖** |
| `dlf.session_token` | ❌ **无人覆盖**（OSS 的 sessionToken 别名里没有它） |
| `dlf.catalog.sessionToken` | ❌ **无人覆盖** |

**为什么升级后真的会泄漏**（验证者独立复核确认）：
- 脱敏是 `SENSITIVE_KEY.contains(key)` 的**原始 key 匹配**，**不按 catalog 类型**，
  且 `showCreateCatalog` 这条路上**没有兜底启发式**（`MetadataGenerator` 的后缀启发式只服务 `catalogs()` TVF，
  且它也漏 `dlf.catalog.accessKeySecret`/`dlf.catalog.sessionToken`）。
- 老 DLF catalog **回放时不被拒绝**（阶段 2 刻意设计：拒绝只在 CREATE + createClient，回放放行否则 FE 起不来）
  → 它**仍在目录里、仍可 `SHOW CREATE CATALOG`**（该路径**从不调** createClient）。
- ⇒ 删掉注册 = 那些存着的 token/密钥**从打码变明文**。

**修法（用户 2026-07-14 拍板）**：照同文件 **iceberg REST 块的既有范式**，把 4 个 key 显式列出。
讽刺的是那段注释早就写着警告：*"the overlap with S3Properties above is uneven and must NOT be relied on ...
omitting it here would silently unmask it"* —— 同一个陷阱，第二次。

### ✅ DLF 2.0 REST 与被删代码完全不相干（字节码级）

`PaimonRestMetaStoreProperties` `extends AbstractMetaStoreProperties`（**不是** `AbstractDlfMetaStoreProperties`），
常量池**零** DLF 引用；key 是 `paimon.rest.dlf.*`，与被删的 `dlf.catalog.*` **结构性隔离**；
token provider 是 **Paimon 自己的**（由 `token.provider=dlf` 选项字符串选中），**Doris 侧无对应类**。
`AbstractDlfMetaStoreProperties` 的消费者只有被删的 Paimon/Iceberg 两个 DLF 属性类 —— REST 不在其中。

### 侦察挖出的、原计划没有的事

1. **跨模块硬引用**：原以为 fe-core 那棵树只被一个反射字符串引用；实际 **5 处**，其中
   `DLFClientPool.java:20` 是**硬 import**（iceberg 插件直接编不过）、`PaimonCatalogFactory` 是 prod 字符串。
   两者都在本阶段范围内 → 一起删即闭合。
2. **该树寄生在 hive-catalog-shade 上**：它 import 的 `com.aliyun.datalake.metastore.common.*` 就住在那个
   127MB jar 里（paimon 侧注释亦自证：*"host-provided via hive-catalog-shade at cutover"*）。**删它正是目标。**
3. **`fe-filesystem-oss` 的 dlf 是良性的**：只是 OSS 凭证的 `@ConnectorProperty` **别名字符串**，零 DLF 类依赖，
   且**非 DLF 的 OSS catalog 也可能用到** → **不动**（删它有风险无收益）。

### 复用阶段 2 基础设施（零新增落点）

拒绝逻辑直接扩展 `HmsClientConfig.removedMetastoreTypeError`：两个已移除类型收敛成一张
`REMOVED_METASTORE_TYPES` 表，文案自动收敛为 `Supported types: hms.`。两个调用点
（`validateProperties` + `createClient`）**原样复用**。iceberg/paimon 侧 default 分支**本就 fail-loud**，
不需要新增拒绝点。

**偏离原计划**：dlf 分支删光后 `getMetastoreClientClassName` 成了恒返回同一值的空壳 → **连方法一并内联**。

### 测试：7 处「反转」

把「dlf 可被分派/路由」的断言全部反转为「dlf 必须被拒」（含「provider 必须从 ServiceLoader 消失」
—— 陈旧的 services 条目会复活一个客户端已不存在的后端）。`restDlfTokenProviderRequiresAkSk` **保留**（测 REST）。

### ⚙️ 两个构建坑（**新踩，务必带走**）

1. **maven build cache 静默跳过 surefire**：日志 `Skipping plugin execution (cached): surefire:test` →
   **BUILD SUCCESS 但 0 个测试执行**。我第一次就被骗了，靠数 `Tests run:` 行数才发现（0 行）。
   必须 `-Dmaven.build.cache.enabled=false`。**只 grep BUILD SUCCESS 是不够的，要数测试数。**
2. **依赖 shade 模块的模块必须跑到 `package`**：`fe-connector-paimon` 的 `HiveConf` 来自
   `fe-connector-paimon-hive-shade`，shade 产物只在 `package` 阶段生成；`test-compile`/`test` 会让 maven 用
   该模块**空的 target/classes** → 假报 `package org.apache.hadoop.hive.conf does not exist`（**不是代码错**）。
   `install` 亦不可用（`fe-type` 撞预存 quirk）。**用 `-am package`。**
3. 又一次漏 `-am` 撞 `${revision}` 假错（memory 早有记载，本 session 第二次）。

### 验证汇总

- `mvn -pl <8 连接器> -am package -Dmaven.build.cache.enabled=false` → **BUILD SUCCESS**，
  **229 个测试类、零 Failures/Errors**，checkstyle 0
- `mvn -pl fe-core -am test-compile` → BUILD SUCCESS
- `tools/check-connector-imports.sh` → exit 0
- `grep -rn "com\.aliyun\.datalake" fe/fe-core/src/main/java` → **空**
- 脱敏 4 个 key 逐条核对在位

### 过程中自我纠正 2 次

1. 我给新测试写的断言 `assertFalse(msg.contains("dlf."))` **本身是错的** —— 报错里的 `dlf.` 来自**回显用户输入**
   （`type: dlf. Supported types:`），不是「supported 列表含 dlf」。测试自己红了才发现 → 改成只截取
   `Supported types:` 之后的子串再断言。
2. `git add -A fe/` 把非本线程的 `fe/.mvn/maven.config` 卷了进来 → `git restore --staged` 剔除。
   **这正是纪律禁止 `git add -A` 的原因**（本 session 亲自复现）。
