# ✅ Task List — 删除 thrift 一代 Glue/DLF ⇒ 剔除 `hive-catalog-shade`

> **本任务的唯一进度清单**。完成一项即把 `[ ]` 勾成 `[x]`（随 commit 更新）。
> **「怎么做」看 [`design.md`](./design.md)，「下一步做什么」看 [`HANDOFF.md`](./HANDOFF.md)。**
> **⚠️ 行号信 HEAD 不信文档**（基线 = 2026-07-14 / `657417bec32`）。

---

## 🚩 用户 2026-07-14 拍板（**任务定性已改变**）

**原方案（搬迁 Glue/DLF 客户端进插件）已作废。** 实测证明 thrift 一代的 Glue/DLF 在本分支**已全部损坏**
（cutover 引入的回归，见 `progress.md` 2026-07-14（三）），用户决策：

> **「把 glue 和 dlf 这两个功能先删掉，不再支持了，不作为需要迁移的内容。」**
> 范围 = **只删「走 HMS thrift 协议的那一代」**（见下表）。

⇒ **本任务从「搬迁 + 守门 + 降级档」塌缩为【纯删除】**：
- **不做**守门测试（功能没了，无可守）
- **不做** paimon-on-DLF
- **不做**任何客户端搬迁（唯一例外见 **T-20**，50 行，且是**出** fe-core）
- 总判据**自然归零** ⇒ 127MB jar 直接可出 `fe/lib`，**不再需要降级档**
- 全程 fe-core **只减不增** ⇒ 铁律 A/B 天然满足

### 删除边界（**误删比漏删更严重**）

| | ❌ 删（thrift 一代，实测已坏） | ✋ 留（今天可用，与 shade 无关） |
|---|---|---|
| **Glue** | `hive.metastore.type=glue` → vendored `AWSCatalogMetastoreClient` 树 | `iceberg.catalog.type=glue` → iceberg 官方 `org.apache.iceberg.aws.glue.GlueCatalog`（AWS SDK **v2**，在 iceberg 插件包 `iceberg-aws-1.10.1.jar` 内） |
| **DLF** | `hive.metastore.type=dlf` · `iceberg.catalog.type=dlf`（`DLFCatalog`/`DLFClientPool`）· paimon `paimon.catalog.type=dlf` —— 全部经 `ProxyMetaStoreClient` | **DLF 2.0 REST**：paimon `paimon.catalog.type=rest` + dlf token provider（`PaimonRestMetaStoreProperties`） |
| 其它 | — | `hive.metastore.type=hms`（主路径）· `fe/pom.xml:801-805` 版本钉 · `start_fe.sh` 钉序 · `fastutil-core` · BE `java-udf`/`avro-scanner` pom · `doris-shade` 仓库 |

**总判据（唯一的「做完了没」标准）**：
```bash
grep -rlE '^import (org\.apache\.hadoop\.hive\.|shade\.doris\.hive\.|com\.aliyun\.datalake\.)' \
     fe/fe-core/src/main/java | wc -l     # 基线 26  →  目标 0
grep -rn 'hadoop.hive' fe/fe-common/src/main/java   # 基线 1 处 → 目标 空
```

---

## 阶段 0 — 调研（已完成）

- [x] **T-00** 事实基线（10-agent 侦察 + 3 路对抗验证）→ `design.md`
- [x] **T-01** ⭐ **离线 classloader 实验**（真实 `output/fe/lib` + 真实插件包 + 真实 `ChildFirstClassLoader`）
      → **实测四条路径全坏**，机理各不相同 → `progress.md`；复现程序 `loader-probe-reproduction.java.txt`
- [x] **T-02** 六路删除清单调研 + 六路对抗验证
- [x] **T-03** 用户签字：**删 thrift 一代**（范围见上表）

---

## 阶段 1 — 🔴 先修 iceberg 原生 Glue 的凭证类（**这是保留路径上的 bug，不是删除**）

> **⚠️ 这条容易被漏**：iceberg 原生 Glue 在**用户填了 AK/SK 时**也是坏的 —— 第四个实例。
> `IcebergCatalogFactory.java:559-560` 把 `client.credentials-provider` 设为
> `com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x`（**住在 fe-core**），
> iceberg 的 `AwsClientProperties` 用**自己（child）的** loader 按名加载并 `asSubclass`
> 到 **child 的** `software.amazon.awssdk...AwsCredentialsProvider` → app-loaded 的 Provider2x 对不上 → **CCE**。
> **实测**：`child AwsCredentialsProvider.isAssignableFrom(Provider2x) = false`。
> IAM-role 分支（`AssumeRoleAwsClientFactory.class.getName()`，**编译期类引用**）与默认凭证链分支**不受影响**。

- [x] **T-20** ✅ **已完成**（commit `2cd01ada8df`）。把 `ConfigurationAWSCredentialsProvider2x.java` 从 fe-core
      搬进 **`fe/fe-connector/fe-connector-iceberg`**（⚠️ 模块真实路径是**嵌套**的，文档旧路径 `fe/fe-connector-iceberg` 错误），
      并按用户 2026-07-14 拍板**改包**到 `org.apache.doris.connector.iceberg.glue`（原 `com.amazonaws.glue.catalog.credentials`
      是 AWS 官方命名空间，且与即将删光的 thrift 老树同包 → 留着会让人误判「没删干净」）。
      - 改动面 = 4 文件：新类 + fe-core 删除 + `IcebergConnectorProperties.java:142` 常量值 + `IcebergCatalogFactoryTest.java:556` 断言
      - ✅ 合铁律 A（**出** fe-core）；`auth:2.29.52` 经 `s3` 传递已在插件 compile classpath，**pom 零改动**
      - ✅ **验收达成**：probe 同一次运行的前后对照 —— 旧位置解析自 `app-model` → `isAssignableFrom = false`；
        新位置解析自 `ChildFirstClassLoader` → **`true`**；`create(Map)` + `resolveCredentials()` 返回正确 AK/SK
      - 📌 **实测更正**：真实报错是 `IllegalArgumentException "... does not implement AwsCredentialsProvider"`
        （iceberg `isAssignableFrom` 门禁先拦），**不是** CCE。不影响修法
      - 📌 新包命中 `org.apache.doris.connector.` 这条 parent-first 前缀 → 靠「父加载器无此类 → 回退子加载器」成立，
        **已实测**，与插件内其它所有类同一模式
      - ✋ 同目录的 `ConfigurationAWSCredentialsProvider.java`（v1）与 `ConfigurationAWSCredentialsProviderFactory.java`
        **未动**，属 thrift 一代 → 随 T-30 删

- [ ] **T-21** 🆕 **待用户拍板**：`create()` **静音丢弃 session token**（侦察挖出的既有 bug，非本次搬迁引入）。
      `IcebergCatalogFactory.java:565-566` 发 `client.credentials-provider.glue.session_token`、单测 `:559` 也断言它，
      但 `create()` 只读 `glue.access_key`/`glue.secret_key` 造 `AwsBasicCredentials`，**从不造 `AwsSessionCredentials`**
      → 用 `aws.glue.session-token` 传临时 STS 凭证的用户**认证必失败**。
      修法就一处：AK/SK/token 三者齐全时改造 `AwsSessionCredentials`（插件已 import 该类）。
      ⚠️ 属**行为变更**（超出「纯搬迁」范围）→ **必须用户签字后再动**。

---

## 阶段 2 — 删 Glue（thrift 一代）✅ **已完成**（commit `e43173eca67`）

> **判据达成**：fe-core main 的 `com.amazonaws.*` 引用 **归零**。
> 总判据（hive/dlf import 数）从基线 **26 → 7**（余下为 DLF/hive 部分，阶段 3/4 处理）。
> 验证：fe-core `test-compile` 绿 · hms/hive/iceberg 三连接器 `test` 全绿 · checkstyle 0 ·
> `check-connector-imports.sh` exit 0。

- [x] **T-30** 删 `fe/fe-core/src/main/java/com/amazonaws/glue/**` —— **37 个文件 / 10,467 行**全删。
      对抗验证证实树自洽：跨边界的编译期边只有 1 条（`HiveGlueMetaStoreProperties`），其余皆字符串；
      `com.amazonaws.services.glue`(v1 SDK) 的 23 个使用者全在树内。
- [x] **T-31** 删 `ThriftHmsClient` 的 `GLUE_CLIENT_CLASS` + glue 分派分支。
      🔴 **显式拒绝已实现（用户 2026-07-14 拍板「两处都加」）**：
      - `HiveConnectorProvider.validateProperties` —— 拦 CREATE/ALTER。**必须抛 `IllegalArgumentException`**
        （catalog 层只解包这一种，文案才能原样透出；抛别的会被包烂）。
      - `HiveConnector.createClient` —— 拦**升级上来的老 catalog**（它们从 image 反序列化，**永不经**
        `validateProperties`）。**必须放在「HMS URI 必填」检查之前**，否则被遮蔽。抛 `DorisConnectorException`
        （该处邻居惯例；fe-core 有多处专门 catch 它）。
      - 💀 **绝不能放** `ConnectorProvider.create()` / `HiveConnector` 构造函数 —— **edit-log 回放时会跑** →
        抛异常 → `EditLog.loadJournal` 的 catch-all `System.exit(-1)` → **FE 起不来**。
        （代码库里 lakesoul 的「已移除」先例恰好就放错了位置，是个潜在启动 bug —— **别照抄它**。）
      - 常量 `METASTORE_TYPE_GLUE` **未删而是改名** `METASTORE_TYPE_GLUE_REMOVED`（偏离原计划）：
        它现在有了新消费者 = 识别并拒绝该已移除类型，比内联字符串字面量更清楚。
- [x] **T-32** 删 `HiveGlueMetaStoreProperties` + `AWSGlueMetaStoreBaseProperties`（整文件）+
      `HivePropertiesFactory` 的 `register("glue", ...)` 与 javadoc + `DatasourcePrintableMap` 的 import/`addAll`。
      脱敏字节中性已由对抗验证用 **javap 字节码**证实（非仅读源码）：该类唯一 `sensitive=true` 字段是
      `glueSecretKey`（3 个别名），`S3Properties` 逐字覆盖同样 3 个别名。
- [x] **T-33** 删 `AwsCredentialsProviderFactory.createV1` + `createDefaultV1`；**保留** `createV2`/`getV2ClassName`。
      `isWebIdentityConfigured`/`isContainerCredentialsConfigured` 两个私有 helper **保留**（V2 也在用）。
- [x] **T-34** 删 iceberg 侧 factory-key：`IcebergCatalogFactory` 的那条 `opts.put` +
      `IcebergConnectorProperties` 的 `GLUE_CREDENTIALS_PROVIDER_FACTORY_KEY`/`_FACTORY` 两常量。
      ✋ `_KEY`/`_2X`/AK/SK/session-token 全部保留（阶段 1 之后它们是好的）。
      单测断言从「断言发出 factory-class」改为**断言不发出**（带 WHY）。
      > **行为中性已用字节码锁死**：拆插件目录**全部 183 个 jar**（侦察只查了 6 个）、**6603 个 class**
      > 搜该 key → **0 命中**；正对照证明搜索有效（其它每个 emit 的 key 均命中）。唯一读者
      > `AWSGlueClientFactory:115` 在被删树内。
- [x] **T-35** 删 `HMSGlueMetaStorePropertiesTest`(108) · `AWSGlueMetaStoreBasePropertiesTest`(139) ·
      `GlueCatalogTest`(111)。
- [x] **T-36** 🆕 新增 `HmsClientConfigRemovedTypeTest`（该分派**此前零测试覆盖** → 不加测试则拒绝逻辑
      被误删也不会有东西变红）。**已做变异验证**：把拒绝改坏 → 测试确实转红（报
      `"hive.metastore.type=glue must be rejected, never silently ignored"`）。
      ⚠️ 踩坑复现：变异验证第一次漏了 `-am` → `BUILD FAILURE` 是 `${revision}` 假错、**根本没编译**，
      结论作废后重做（memory `doris-build-verify-gotchas`）。

### 📌 阶段 2 遗留（**不在本阶段，勿忘**）

- **regression-test 未动**（按计划归「用户可见面」阶段）。已定位待删：
  `aws_iam_role_p0/test_catalog_instance_profile.groovy:67-95`（3 块 hive-on-glue）+ 死变量 `:26-27`；
  `aws_iam_role_p0/test_catalog_with_role.groovy:82-90` + 死变量 `:49`；
  `external_table_p2/refactor_catalog_param/iceberg_and_hive_on_glue.groovy:369-372`（**已是死代码**，定义了从不引用）。
  ✋ **保留**：`test_catalog_with_role.groovy:56-60` 的 `awsGlueProperties`（iceberg-glue 分支 `:78` 还在用）、
  `:62-81`、`iceberg_and_hive_on_glue.groovy:367`。
  ⚠️ `test_catalog_instance_profile.groovy:22-24` 的**文件级 guard 钉在 glue 专有 conf key 上** ——
  删 glue 块后须改钉 iceberg 的 key，否则存活的 iceberg 分支**静默永不运行**。
  ⚠️ 假阳性别碰：`external_table_p2/hive/test_external_catalog_glue_table.groovy`（名字是陷阱，实为普通 HMS，
  且 `:20-24` 硬关）· `test_iceberg_predicate_conversion.groovy`（唯一 p0 命中，glue 只是列名）·
  `test_s3tables_glue_*`（iceberg REST + glue signing）。
- **`test_connection=true` 顺序坑**：`checkWhenCreating` 跑在 `checkProperties` **之前**。当前
  `HiveConnector` 不 override `defaultTestConnection()`（继承 `false`）故不触发；但显式配
  `"test_connection"="true"` 的 glue catalog 会先撞别的错。已由 `createClient` 那处拒绝兜住。
- **文案里 `Supported types: hms, dlf`** —— 阶段 3 删 DLF 后须同步去掉 `dlf`。

## 阶段 3 — 删 DLF（thrift 一代 = DLF 1.0）✅ **已完成**（commit `a0f65c353a9`）

> **判据达成**：fe-core 的 `com.aliyun.datalake` 引用 **归零**。总判据 **7 → 4**（余下 4 个为 hive 部分，阶段 4 处理）。
> 验证：**229 个测试类零失败** · fe-core `test-compile` 绿 · checkstyle 0 · `check-connector-imports.sh` exit 0。
> ✋ **DLF 2.0 REST 完好**（`paimon.catalog.type=rest` + dlf token）—— 字节码级证实与被删代码不相干。

- [x] **T-40** 删 `fe/fe-core/src/main/java/com/aliyun/datalake/**`（`ProxyMetaStoreClient`，2193 行）。
      > 侦察副产品：该树 import 的 `com.aliyun.datalake.metastore.common.*` **就住在 hive-catalog-shade 里**
      > —— 它本身就寄生在待剔除的那个 jar 上，删它正是本任务目标。
- [x] **T-41** 删 `ThriftHmsClient` 的 `DLF_CLIENT_CLASS` + dlf 分支。
      **偏离原计划**：分支删光后 `getMetastoreClientClassName` 成了恒返回同一值的空壳 → **连方法一并内联**到调用点。
      拒绝逻辑**无需新增落点**，直接扩展阶段 2 建好的 `HmsClientConfig.removedMetastoreTypeError`：
      两个已移除类型收敛成一张 `REMOVED_METASTORE_TYPES` 表，文案自动变为 `Supported types: hms.`。
      连带删 `HiveConnectorMetadata` 的 DLF 默认值守卫（已成死代码）。
- [x] **T-42** iceberg 侧：`dlf/` 整包（`DLFCatalog`/`DLFTableOperations`/`HiveCompatibleCatalog`/
      `DLFCachedClientPool`/`DLFClientPool`）· `TYPE_DLF` · dispatch arm · `createDlfCatalog` ·
      `buildDlfConfiguration` · **5 处 DDL 守卫**（dlf catalog 现在建不出来 → 守卫永不可达 = 死代码）。
      ✋ `Supported types` 文案去掉 `dlf`、**保留 `glue`**。
      > `HiveCompatibleCatalog` 虽是抽象基类但**唯一子类就是 `DLFCatalog`**（grep 证实）→ 随包删。
- [x] **T-43** paimon 侧：`DLF` case · `appendDlfOptions` · `DLF` 常量 · 相关 javadoc。
      ✋ **`REST` 全链路未动**；`restDlfTokenProviderRequiresAkSk` 测试**保留**（它测的是 REST 路径）。
- [x] **T-44** 删 fe-core `AliyunDLFBaseProperties` + `HiveAliyunDLFMetaStoreProperties` + `HivePropertiesFactory`
      的 dlf 注册。**⚠️ 脱敏另行处置，见 T-46。**
- [x] **T-45** 连接器侧 DLF 属性类逐个判定完毕，**全部 DELETE**（无一被 REST 共用）：
      `metastore-api/DlfMetaStoreProperties` · `metastore-spi/AbstractDlfMetaStoreProperties` ·
      `metastore-iceberg/dlf/*` · `metastore-paimon/dlf/*` + **两处 SPI `services` 注册行**。
      ✋ **`fe-filesystem-oss` 不动**：那里的 `dlf.*` 只是 OSS 凭证的 `@ConnectorProperty` **别名字符串**，
      不依赖任何 DLF 类，且**非 DLF 的 OSS catalog 也可能用到** → 删它有风险、无收益。
- [x] **T-46** 🆕🔴 **安全修复（本轮最重要，原计划没有）**：`DatasourcePrintableMap` 的脱敏注册
      **不能随属性类一起删**，否则**明文泄漏**。用户 2026-07-14 拍板「显式列出那 4 个 key」。
      > **为什么 Glue 那次能直接删、DLF 不能**：脱敏靠**逐字对齐的别名字符串**，重叠**不均匀**——
      > `dlf.secret_key` 被 OSS 属性类覆盖，但 `dlf.catalog.accessKeySecret` / `dlf.session_token` /
      > `dlf.catalog.sessionToken` **无人覆盖**（javap 逐字段确认）。
      > **为什么升级后仍会泄漏**：脱敏按**原始 key 匹配、不按 catalog 类型**；而老 DLF catalog **回放时不被拒绝**
      > （刻意设计，否则 FE 起不来）→ 仍在目录里、仍可 `SHOW CREATE CATALOG` → 存的 token 从打码变明文。
      > **范式来自同文件的 iceberg REST 块**，其注释早就警告过这个「重叠不均匀」的陷阱。
- [x] **T-47** 🆕 测试：**新增/改造 7 处**，把「dlf 可被分派/路由」的断言**反转**为「dlf 必须被拒」：
      `HmsClientConfigRemovedTypeTest`（重写，4 用例，含「文案只许宣传 hms」）·
      paimon/iceberg 两个 `MetaStoreProvidersDispatchTest`（+「provider 必须从 ServiceLoader 消失」）·
      `PaimonCatalogFactoryTest` · `PaimonConnectorValidatePropertiesTest` · `IcebergCatalogFactoryTest` ·
      `IcebergConnectorTest`。删除只覆盖已删路径的测试 5 个文件 + 若干方法。

### 📌 阶段 3 遗留

- **regression-test 未动**（归「用户可见面」阶段，与 Glue 的一并做）。
- **报错文案**：iceberg/paimon 侧走各自 default 分支报 `Unknown ...type: dlf. Supported types: ...` ——
  **loud 且准确，但没说「已移除」**。归「用户可见面」阶段统一措辞（该阶段本就要求 dlf 报错说明已移除）。

### ⚙️ 阶段 3 踩到的两个构建坑（**下轮务必带上**）

1. **maven build cache 会静默跳过 surefire** → 日志出现 `Skipping plugin execution (cached): surefire:test`，
   **BUILD SUCCESS 但零测试执行**。必须 `-Dmaven.build.cache.enabled=false`，并**数一下 `Tests run:` 的行数**。
2. **依赖 shade 模块的模块必须跑到 `package`**：`fe-connector-paimon` 的 `HiveConf` 来自
   `fe-connector-paimon-hive-shade`，shade 内容只在 `package` 阶段产出。用 `test-compile`/`test` 会假报
   `package org.apache.hadoop.hive.conf does not exist`（**不是代码错**）。用 `-am package`。
   ⚠️ `install` 不行：会在 `fe-type` 撞预存的 `did not assign a main file` quirk。

## 阶段 4 — fe-core 去 hive 化 ✅ **已完成**（4 个独立 commit）

> **判据达成**：fe-core 的 hive/shade/dlf import **26 → 0**。
> 通用 SPI 上的 hive 专有方法整个消失（铁律 C）。fe-core 全程**只减不增**（铁律 A）。
> 验证：fe-core + SPI test-compile 绿 · 两插件 `-am package` 182 个测试类真跑零失败
> （已数 `Tests run:` 行，surefire 未被 build cache 跳过）· checkstyle 绿 ·
> `check-connector-imports.sh` exit 0。

**⚠️ 侦察（5 路 + 20 路对抗验证）推翻了本阶段原计划的 3 个前提**，明细见 `progress.md`。

- [x] **T-50** Ranger 死代码 —— commit `d8c121b7f21`。净减 **12** 行（原计划写 13，实测 12）。
      ROLE_OPS 全仓零读取点。**诚实表述更正**：本类**是**可被反射到达的（经持久化的
      `access_controller.class` → `RangerHiveAccessController`）；安全的理由是"ROLE_OPS 零读取点
      且类名不变"，不是"没人能到达这个类"。
      ⚠️ **原计划预录的"恰好 1 个编译错误"证明不可信**（对抗验证复现出 9 个，且其 harness
      无法产出宣称的干净对照）→ 判据以实跑编译+checkstyle 为准。
- [x] **T-51** 删 HMS 属性簇 —— commit `22461468e7c`。**作用面是原计划的 2 倍**：
      **4 文件 + 1 行反注册**（原计划只说 2 文件）。
      🔴 **原计划"它们是死代码"被证伪**：`MetastoreProperties.java:88` 至今仍有
      `register(Type.HMS, new HivePropertiesFactory())` → 是"能到达但没人走"。真实删除闭包 =
      `HMSBaseProperties` + `AbstractHiveProperties` + `HiveHMSProperties`（不在原计划）+
      `HivePropertiesFactory`（不在原计划），**任何中间态都编译不过**。
      常量归属：**用户拍板纯内联字面量**（fe-core 一行不增，字面满足铁律 A；不把铁律冲突用
      "实质满足"平均掉）。删 `HMSPropertiesTest`（诚实记录被放弃的覆盖 → D-1/D-2）。
      ✅ 不丢脱敏（两条独立证据）：4 文件源码级 `grep sensitive` 全 0；`DatasourcePrintableMap`
      只注册 S3/GCS/Azure/OSS/OSSHdfs/COS/OBS/Minio，对 hive 零引用。
- [x] **T-53** hive 配置加载去 hive 化 —— commit `7ed266c677a`。**判据归零的那一刀。**
      🔴 **原计划/SPI 注释的核心前提"插件不可能自己解析 hadoop_config_dir"被证伪**：
      `fe-filesystem-hdfs` 是真 leaf（pom 无 fe-core/fe-common），**已经**通过
      `doris.hadoop.config.dir` 系统属性桥（`FileSystemFactory:124` 设值）解析同一个目录，且有测试。
      **用户拍板"插件自解析"**（照抄该既有范式），非"改名为通用 hadoop 加载"。
      行为变化真实但有界：iceberg 可证明零变化；paimon 28 个 ConfVars 默认值回到自带的 2.3.9
      （方向是**变正确**）。测试从 mock 握手改写为驱动真实文件→HiveConf，**已做变异验证**。
- [x] **T-54** 删 `hudi-hadoop-mr` —— commit `8d6fe9f9736`。
      🔴 **原计划的"ORC blocker"是假警报**：全仓 `org.apache.orc` 引用 **0**；且 shade 里逐字节
      相同地捆了全部 122 个 `hive-storage-api` 类（`IDENTICAL=122`）。
      ⚠️ **该结论条件于 shade 还在** → 阶段 5 必须**重跑** ORC 分析（见 D-3）。
- [ ] **T-52** fe-common 去 hive 化 —— **改排到阶段 5**（🔴 见下）。**门禁已解锁**：
      `loadHiveConfFromHiveConfDir` 现在**零调用点**（只剩声明本身）。

### 🔴 T-52 为什么必须与摘 jar 同批（**OUTAGE 级排期更正**）

原计划把它排成"阶段 4 的一件事、随便什么时候做"。**错**。`CatalogConfigFileUtils` 服务
**所有**外表 catalog（不只 hive）。若 fe-core 先摘 jar 而 fe-common 仍 import `HiveConf`：
**编译绿**（fe-common 自带 `provided` 声明）但 FE 启动时**每个** catalog 都 `NoClassDefFoundError`。
⇒ 它的排期是一个**窗口**（两个调用点消失后 ✅ 已达成、摘 jar 之前），**最好同一个 commit**。

### 📌 阶段 4 立项的既有缺陷（**用户 2026-07-14 拍板：三个全记录，本轮不修**）

| ID | 缺陷 | 性质 |
|---|---|---|
| **D-1** | **普通 hive 路径 socket timeout 失效**：`Config.hive_metastore_client_timeout_second`(默认 10s) 从不到达 metastore 客户端 —— `HmsConfHelper.createHiveConf` 从不设 `hive.metastore.client.socket.timeout` → 用户静默拿 HiveConf 内建的 **600s**。（iceberg/paimon 的 HMS 后端经 `AbstractHmsMetaStoreProperties` 读 env，是好的；**只有 plain-hive 漏**。） | pre-existing，非本次引入。唯一记录它的 `HMSPropertiesTest:141-150` 已随 T-51 删除 |
| **D-2** | **两个用户可见属性空转**：`hive.enable_hms_events_incremental_sync` / `hive.hms_events_batch_size_per_rpc` **零生产消费者**；且 live 的 `HiveConnectorProperties.getInt()` 用 `catch (NumberFormatException) { return defaultVal; }` **静默吞掉**非法值（旧类是 loud reject） | pre-existing。无运行时变化，**丢的是最后的记录** |
| **D-3** | **摘 jar 后 `orc-core` 变孤儿**：`orc-core-1.8.4` 经 `hudi-common` 进来，而其 `hive-storage-api` 在 `fe/pom.xml:1507-1510` 被排除 → 结构上不可满足。今天无害**纯因无人加载 ORC** | ⛔ **阶段 5 必须重跑分析**；别拿 T-54 的"ORC 没事"当先例 |

## 阶段 5 — pom 终局（fe-core 判据已 = 0 ✅，可以动了）

- [ ] **T-52** fe-common 去 hive 化（从阶段 4 挪来）—— **必须与 T-60 同批/同 commit**（见上方 🔴）

- [ ] **T-60** 删 `fe/fe-core/pom.xml:437-440`（`hive-catalog-shade` 本体）
- [ ] **T-61** 删 `fe/fe-core/pom.xml:217-223`（`commons-lang` 2.6 runtime）
      ⚠️ 风险 R-7：Ranger 降到 2.8.0 以下会因**无关原因**复活它
- [ ] **T-62** 删 `fe/fe-core/pom.xml:926-967`（`bundle-fastutil-into-doris-fe` shade execution）
      ✋ **`fastutil-core` 依赖本身要留**（`:766-774`）；按风险 R-5 在该依赖上**留注释**记录「为什么这里曾需要 shade 覆盖」
- [ ] **T-63** 删 `fe/fe-core/pom.xml:776-782`（`<repositories>` 的 `central`，注释写着 `for hive-catalog-shade`）
- [ ] **T-64** 删 `fe/fe-common/pom.xml:87-91`（`provided` shade）
- [ ] **T-65** 删 fe-core 的 `aws-java-sdk-glue` / `aws-java-sdk-sts`（T-30/T-33 后零引用）；
      **`aws-java-sdk-core` 是否还需要**按 T-33 结论定
- [ ] **T-66** **不许动**复核：`fe/pom.xml:801-805` ✋ · `start_fe.sh` 钉序 ✋ · `fastutil-core` ✋ ·
      BE `java-udf`/`avro-scanner` ✋ · `doris-shade` ✋。**独立 commit**

---

## 阶段 6 — 用户可见面 + 守门

- [ ] **T-70** **regression-test 清理**（只删 thrift 一代的）：
      `aws_iam_role_p0/test_catalog_with_role.groovy:82-90`（`hive.metastore.type=glue` 块 + `:49` 的
      `hiveGlueTableName` 变量）✋ **`:73-81` 的 `iceberg.catalog.type="glue"` 保留**；
      `aws_iam_role_p0/test_catalog_instance_profile.groovy:67-95`（三块 hive-on-glue）
      ⚠️ 注意 `:22` 的 guard 是否随之失去意义。其余 glue/dlf case 逐个判「thrift 一代 vs 保留路径」。
- [ ] **T-71** 文档 / 报错文案：`hive.metastore.type=glue|dlf`、`iceberg.catalog.type=dlf`、
      `paimon.catalog.type=dlf` 的报错要清楚说明**已移除**；仓库内 .md 同步。
- [ ] **T-72** 静态守门：
      - 总判据两条 grep → 0 / 空
      - `mvn -o -f <abs>/fe/pom.xml -pl fe-core -am test-compile` BUILD SUCCESS（**漏 `-am` → 假错**）
      - fe-common + 全连接器 `test-compile` 绿 · checkstyle 0 · `tools/check-connector-imports.sh` exit 0
      - `mvn -o dependency:tree -Dverbose -Dincludes=org.apache.doris:hive-catalog-shade -pl fe-core -am` → **fe-core 段为空**
      - `unzip -l fe/fe-core/target/doris-fe-lib.zip | grep hive` → 记录删前/删后差异
- [ ] **T-73** **e2e**：① 普通 HMS catalog 读写（回归基线）② **iceberg 原生 Glue + AK/SK**（T-20 的验收，若可跑）
      ③ Ranger hive 鉴权（T-50 动了它）
- [ ] **T-74** 收尾：`progress.md` 结项 + `../decisions-log.md` 补 D-NNN + PR（base = `branch-catalog-spi`，squash）。
      **PR 描述必须显式列出移除的用户可见能力**（见下）。

### PR 必须声明「本 PR 移除了以下能力」
- `hive.metastore.type = glue`（AWS Glue as HMS-thrift metastore）
- `hive.metastore.type = dlf`（阿里云 DLF 1.0 as HMS-thrift metastore）
- `iceberg.catalog.type = dlf`
- `paimon.catalog.type = dlf`
- **仍然支持**：`iceberg.catalog.type = glue`（iceberg 原生）· paimon `rest` + DLF token（DLF 2.0）· `hive.metastore.type = hms`

---

## 📌 Commit / 分支纪律

- 工作分支 `catalog-spi-11-hive`；PR base = `branch-catalog-spi`，**squash**。
- **每个阶段 = 独立 commit**；文档与 code **分开 commit**。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`** —— 工作树有大量历史遗留 scratch（`.audit-scratch/` /
  `conf.cmy/` / `META-INF/` / `*.bak` / `failed-cases.out` / `.claude/` …），**非本线程产物，勿混入**。
- commit message：`[refactor|fix|doc](catalog) …` + `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
