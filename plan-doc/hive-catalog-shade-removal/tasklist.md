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

## 阶段 3 — 删 DLF（thrift 一代 = DLF 1.0）

> ✋ **边界**：**只删走 thrift 的老一代**。**DLF 2.0 REST 必须保留**（`PaimonRestMetaStoreProperties`）。
> ⚠️ 落刀前先确认 `AbstractDlfMetaStoreProperties` 是否被 **REST 那代共用** —— 共用则**不能删**，只能删其 thrift 消费者。

- [ ] **T-40** 删 `fe/fe-core/src/main/java/com/aliyun/datalake/**`（`ProxyMetaStoreClient.java`，2193 行）
- [ ] **T-41** `ThriftHmsClient.java`：删 `DLF_CLIENT_CLASS`(:917-918) + `getMetastoreClientClassName` 的 dlf 分支
      (:926-927) + `HmsClientConfig.java:43` 的 `METASTORE_TYPE_DLF`。
      🔴 **同 T-31：必须显式拒绝，不能静默落到 `HiveMetaStoreClient`。**
      连带 `HiveConnectorMetadata.java:1562` 的 dlf 判断（先查清它做什么）。
- [ ] **T-42** iceberg 侧删 dlf 类型：`IcebergCatalogFactory.java:307`(TYPE_DLF 分支) ·
      `fe-connector-iceberg/.../dlf/` 整目录（`DLFCatalog` / `DLFClientPool`）·
      `IcebergConnectorProperties.java:37`(TYPE_DLF) · `IcebergConnector.java:692,841,845` ·
      `IcebergConnectorMetadata.java:1268`。
      ⚠️ `:310-313` 的 `Supported types:` 文案要同步去掉 `dlf`。
- [ ] **T-43** paimon 侧删 dlf 类型：`PaimonCatalogFactory.java:119,158,217` · `PaimonConnector.java:388-408` ·
      `PaimonConnectorProperties.java:65`。✋ **`REST`(:63) 必须保留。**
- [ ] **T-44** 删 fe-core DLF 属性类：`AliyunDLFBaseProperties.java` · `HiveAliyunDLFMetaStoreProperties.java`
      + `HivePropertiesFactory` 的 dlf 注册。测试 `HMSAliyunDLFMetaStorePropertiesTest.java`。
- [ ] **T-45** 连接器侧 Dlf 属性类按「是否被 REST 共用」逐个判定（`fe-connector-metastore-{spi,hms,iceberg,paimon}`）。
      **独立 commit**

---

## 阶段 4 — 残余清理（fe-core / fe-common 去 hive 化）

- [ ] **T-50** **Ranger 死代码**：`RangerHiveAuditHandler.java` 删 `:22`(HiveOperationType import) ·
      `:36`(EnumSet) · `:38`(HashSet) · `:55-63`(`ROLE_OPS` 声明 + static 块)。**净减 ~12 行，纯删除。**
      > 已用真编译**双向**验证：删后把 shade 摘出 classpath 仍 `javac` 通过（EXIT=0）；不删则恰好报 1 错就是该 import。
      > `ROLE_OPS` 全仓**零读取点**，从未流入任何 audit event 字段 → **用户可见行为零变更**。
      > ⚠️ 必须同删 EnumSet/HashSet 两个 import，否则 checkstyle `UnusedImports`(`checkstyle.xml:167`) 挂。
- [ ] **T-51** 删 5 个属性类中剩余的（`HMSBaseProperties` · `AbstractHiveProperties`；Glue/DLF 那 3 个已由阶段 2/3 删）。
      > 据称已是死代码（"hms" 走 `CatalogFactory.java:55` 的 `SPI_READY_TYPES` → `PluginDrivenExternalCatalog`
      > → `CatalogProperty.java:223` 的 `getMetastoreProperties()` 永不被调）。
      > ⚠️ **对抗验证指出证据链有洞，必须复验**：`PluginDrivenExternalCatalog.java:156` 设 supplier，但
      > `createConnectorFromProperties()` 在 **:129** 就被调用（**早于** :156）→ 查清中间有无真实调用窗口。
      > ⚠️ `HiveTable.java:21,107-119` 与 `HMSResource.java:22,66-67` 是 **GSON 持久化活类**，只为拿 String
      > 常量而 import `HMSBaseProperties` → 删它会让这两个编译不过。**处置方案需符合铁律 A/B**（别就近塞 util）。
- [ ] **T-52** **fe-common 去 hive 化**：删 `CatalogConfigFileUtils.java:23`(HiveConf import) + `:95`
      (`loadHiveConfFromHiveConfDir`)。✋ **同文件 `:79` 的 `loadConfigurationFromHadoopConfDir` 服务所有外表
      catalog，必须留**（`ConnectionProperties.loadConfigFromFile:83` 在用）。判据：`grep -rn 'hadoop.hive' fe/fe-common/src` → 空。
- [ ] **T-53** `DefaultConnectorContext` 的 `loadHiveConfResources`：**不是死代码**
      （`IcebergConnector:707` / `PaimonConnector:376` 真调）。⚠️ 现语义 = 把 `new HiveConf()` 的**上千条默认值**
      返回给插件、插件再 `base.forEach(hiveConf::set)` 灌进自己的 HiveConf。
      **Glue/DLF 删掉后这条链还需要吗？** 查清 iceberg/paimon 拿这些默认值做什么。
      ⚠️ `ConnectorContext.java:150-166` 上出现 hive 专有方法**本身违反**「通用 SPI 禁 source-specific」。
      ⚠️ **换实现必须显式论证**（不得静默改变那个默认值集合）。**这是阶段 4 最难的一点。**
- [ ] **T-54** 删 `hudi-hadoop-mr`（`fe/fe-core/pom.xml:600-604`）。✋ `hudi-common`(:595-598) **要留**
      （`HttpProperties`/`StatisticsCache` 各一处 import，已证实）。
      > 🔴 **落刀前必查**：对抗验证指出 **`orc-core-1.8.4`（`fe/lib` 里的实装 jar）的公开 API 直接依赖
      > `hive-storage-api` 的类** → 删 `hive-storage-api` 是否炸 orc？**未解决前不许删。**
      ⚠️ 别信前一轮的「5 项全零引用」：**`kryo-shaded` 绝不可删**（`WorkloadSchedPolicy.java:32,287,298`
      经 minlog 传递依赖真实调用）；`avro` 删声明但 jar 删不掉（hadoop-client/iceberg-core/parquet-avro 三方传递）。
      **独立 commit**

---

## 阶段 5 — pom 终局（⛔ 只有总判据 = 0 才能动）

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
