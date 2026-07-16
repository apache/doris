# 🧭 设计文档 — 剔除 `hive-catalog-shade` 冗余依赖

> **基线日期**：2026-07-14 · **分支**：`catalog-spi-11-hive` · **基线 HEAD**：`669602f079d`
> **证据来源**：10-agent 工作流（6 路侦察 + 3 路对抗验证 + 综合），关键事实**主 session 二次亲验**（见 §7 附录）。
> **稳定文档**：状态变化写 `progress.md`，别改这里；结论要改先在 progress 留痕。

---

## 1. `hive-catalog-shade` 是什么，为什么当初要有它

`org.apache.doris:hive-catalog-shade` 是一个**外部仓库**产出的 uber-jar：

- 源码：`/mnt/disk1/yy/git/doris-shade/hive-catalog-shade`（**不在本仓库**）
- 版本：**3.1.1**（`fe/pom.xml:238` 属性 + `fe/pom.xml:801-805` dependencyManagement）
- 体积：**127 MB**

它是一道**二进制兼容性防火墙**，不是「图省事的打包」。

### 1.1 冲突本体：两个不兼容的 thrift

| 谁 | libthrift 版本 |
|---|---|
| Doris 自己的 FE↔BE RPC 桩代码 | **0.16.0**（`fe/pom.xml:294`） |
| Hive 3.1.3 的 `HiveMetaStoreClient` | **0.9.3**（hive-3.1.3.pom） |

JVM 的一个 classloader 把 `org.apache.thrift.TProtocol` 这个名字映射到**唯一一份**字节码。两份 jar 抢同一个
包名，只有一个能赢，输的那一方在**运行时**炸 `NoSuchMethodError` —— **编译期完全看不出来**。

引入 commit：**`5f981b0b1f0`** ——「[fix](catalog) Use hive-catalog-shade to solve thrift version compatibility
issues (#18504)」，原话：*"Hive 3 uses the thrift-0.9.3 package, and Doris uses the thrift-0.16.0 package.
These two packages are not compatible... This jar package renames the thrift class."*

### 1.2 shade + relocation 干了什么

maven-shade-plugin 把 hive 全家桶打进一个 jar，并**改包名**（relocation）：

```
org.apache.thrift.*  ──改名──▶  shade.doris.hive.org.apache.thrift.*
（连同引用它们的 HiveMetaStoreClient 字节码一起被改写）
```

于是 classpath 上：`org.apache.thrift.*` 归 Doris 的 0.16.0 独占；hive 用的 0.9.3 换了个名字，撞不上。

**jar 内容实证（3.1.1，主 session 亲验）**：

| 包 | 类数 |
|---|---|
| `org/apache/thrift/**` | **0** ✅ 全部改过名 |
| `shade/doris/hive/org/apache/thrift/**` | 179 |
| `it/unimi/dsi/fastutil/**`（**未**改名，6.5.6 古董） | **10653** ⚠️ |
| `org/apache/iceberg/hive/**` | 36 |
| `org/apache/paimon/hive/**` | 81 |
| `com/amazonaws/glue/**` | **0** ⚠️（见 §3.2） |
| `ProxyMetaStoreClient`（阿里云 DLF） | 2 |

**为什么 iceberg / paimon / 阿里云 DLF 也被塞进同一个 jar**：它们都通过**同一个** HMS thrift 客户端跟
Hive Metastore 说话。客户端的包名被改了，它们的字节码必须在**同一次 shade 里**一起被改写。

### 1.3 连带的三个 hack（都只因这个 jar 而存在）

| fe-core/pom.xml | 是什么 | 为什么存在 |
|---|---|---|
| `:217-223` | `commons-lang:commons-lang` 2.6 `scope=runtime` | shade 把 commons-lang 标 `provided` 没打进去，但它的 `HiveConf` 要调 `org.apache.commons.lang.StringUtils` |
| `:926-967` | `maven-shade-plugin` 的 `bundle-fastutil-into-doris-fe` execution | shade 里那 **10653 个未改名的 fastutil 6.5.6** 会盖住真 fastutil；靠把 fastutil-core 8.5.18 打进 `doris-fe.jar`、再靠 `bin/start_fe.sh:355` 把 `doris-fe.jar` **钉在 classpath 最前**来抢赢 |
| `:776-782` | `<repositories>` 的 `central` 条目 | 注释直接写着 `<!-- for hive-catalog-shade -->` |

> **⚠️ 验证陷阱**：`doris-shade/.../target/hive-catalog-shade-3.1.2-SNAPSHOT.jar`（本地构建产物）**已经**把
> fastutil 改名了。拿它验证会误判「fastutil hack 已经没用了」。**构建实际解析的是 3.1.1**，那份里 fastutil 是裸的。

---

## 2. 和 `fe-connector-paimon-hive-shade` 的关系：**无依赖关系**

它是同一招数的**插件私有版重新实现**，不是 fork、不是消费者、不是替代品。

| | `hive-catalog-shade`（外部） | `fe-connector-paimon-hive-shade`（本仓库内） |
|---|---|---|
| 改名前缀 | `shade.doris.hive.org.apache.thrift` | `org.apache.doris.paimon.shaded.thrift`（**故意不撞**） |
| hive 版本 | 3.1.3 | hive-metastore **2.3.7** + hive-common 2.3.9 + libthrift 0.9.3 |
| 装了什么 | hive 全家桶 + iceberg-hive + paimon-hive + 阿里云 DLF，127 MB | 只有 paimon 那一片，4 个 artifact |
| 古董 fastutil | **未**改名（泄漏） | **已**改名（关掉了这个坑） |
| 谁在用 | fe-core、fe-common、fe-connector-hms、fe-connector-iceberg、java-udf、avro-scanner | **只有** fe-connector-paimon |

**历史**：paimon **从来没用过**全局 shade —— P5 阶段因「127 MB + fastutil 6.5.6 污染」拒绝
（`plan-doc/tasks/P5-paimon-migration.md:332`），改自带裸 hive jar，撞上
`NoClassDefFoundError: TFramedTransport`，才有了 FIX-C 这个私有 shade（`plan-doc/fix-c-hms-thrift-design.md`）。
iceberg 走了相反的路：决策 **D-060**（`plan-doc/decisions-log.md:31`，用户 2026-06-22 签字）复用全局 shade。

**⚠️ 由此推出一条重要约束**：「让每个连接器各自 shade，从而解放 fe-core」这条路**对 hive 家族不成立**——
`fe-connector-hms` 的**源码里直接 import 了那个改名前缀**
（`fe/fe-connector/fe-connector-hms/src/main/java/org/apache/hadoop/hive/metastore/HiveMetaStoreClient.java`
import `shade.doris.hive.org.apache.thrift.*`），且依赖 jar 名排序（`f` < `h`）让补丁版客户端盖住 shade 里的原版。
换 shade = 改源码 + 可能**静默**破坏这个覆盖技巧。

> **本任务的范围因此是**：让 **fe-core / fe-common** 不再依赖它（→ 从 `fe/lib` 剔除）。
> **不是**消灭这个 artifact —— fe-connector-hms / fe-connector-iceberg / java-udf / avro-scanner **继续用**，
> `fe/pom.xml:801-805` 的 dependencyManagement 版本钉**保留不动**。

---

## 3. 为什么今天删不掉：三层阻塞

### 3.1 编译期 —— fe-core `src/main` 还有 26 个文件 import hive 类

`fe/fe-core/pom.xml:437-440` **没有 `<scope>`** ⇒ compile scope。fe-core **没有任何其它 hive artifact**。

**最不可辩驳的证据**是那个改过名的包 `shade.doris.hive.org.apache.thrift.TException` —— **全世界只有这个
shade jar 能提供它**，而 fe-core 里有 7 个 main 文件 import 了它。

| 组 | 文件 | 数量 |
|---|---|---|
| **A. vendored AWS Glue 客户端** | `fe/fe-core/src/main/java/com/amazonaws/glue/**` | 38 个 `.java`，其中 **19** 个 import hive |
| **B. vendored 阿里云 DLF 客户端** | `com/aliyun/datalake/metastore/hive2/ProxyMetaStoreClient.java` | 1 |
| **C. HiveConf 属性解析** | `org/apache/doris/datasource/property/metastore/{HMSBaseProperties, AbstractHiveProperties, AliyunDLFBaseProperties, HiveAliyunDLFMetaStoreProperties, HiveGlueMetaStoreProperties}` | 5 |
| **D. 连接器上下文** | `org/apache/doris/connector/DefaultConnectorContext.java`（`:208` 调 `loadHiveConfFromHiveConfDir`） | 1 |
| **E. Ranger hive 审计** | `org/apache/doris/catalog/authorizer/ranger/hive/RangerHiveAuditHandler.java`（用 hive-exec 的 `HiveOperationType`） | 1 |
| **测试** | `HMSIntegrationTest` / `HMSPropertiesTest` / `HMSGlueMetaStorePropertiesTest` / `HMSAliyunDLFMetaStorePropertiesTest` | 4 |

> C 组正是 memory `catalog-spi-no-property-parsing-in-fecore`（**fe-core 不持有任何属性解析**）的**未完成尾巴**。

### 3.2 运行时 —— `fe/lib` 是所有插件的**父加载器**，且有两个类**只**存在于父

**`fe/lib` 只由 fe-core 的 assembly 喂**：
`fe/fe-core/src/main/assembly/fe-lib.xml:28-34`（`<dependencySet><scope>runtime</scope>`，**排除 provided**）
→ `build.sh:1011-1012` 解压 `doris-fe-lib.zip` + 拷 `doris-fe.jar` 进 `output/fe/lib/`。
连接器插件另走 `output/fe/plugins/connector/`（`build.sh:1069`），**不喂 `fe/lib`**。

连接器插件是 **child-first + 父回退**（`fe-extension-loader` 的 `ChildFirstClassLoader`），parent-first 前缀名单
（`fe/fe-core/src/main/java/org/apache/doris/connector/ConnectorPluginManager.java:64`）**不含**
`org.apache.hadoop.hive` / `com.aliyun` / `com.amazonaws`。

**两个按名字反射加载、且只存在于父的类**：

| 客户端 | 谁按名字加载 | 定义在哪 | 删 shade 的后果 |
|---|---|---|---|
| **AWS Glue** `com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient` | `fe-connector-hms/.../ThriftHmsClient.java:920-922`（字符串传给 `RetryingMetaStoreClient.getProxy`） | **shade jar 里 0 个条目**！唯一定义 = fe-core vendored 源码 → `doris-fe.jar` | Glue catalog 首用 `NoClassDefFoundError` |
| **阿里云 DLF** `com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient` | **paimon**：`PaimonCatalogFactory.java:217`（paimon zip **没有** shade / aliyun jar；`PaimonConnector.java:398` 注释自认 *"host-provided via hive-catalog-shade at cutover, not bundled"*） | fe-core vendored **+** shade jar（2 个条目） | paimon-on-DLF 首用 `NoClassDefFoundError` |

> **iceberg-on-DLF 不受影响**：`fe-connector-iceberg/.../DLFClientPool.java:20` 直接 import，而 iceberg 的
> plugin-zip **自带** shade（内含 `ProxyMetaStoreClient`）→ child-first 自满足。

> **⚠️ 静默换实现风险**：`bin/start_fe.sh:355` 把 `doris-fe.jar` 钉在 classpath **最前**。所以今天走父回退时
> 实际生效的 `ProxyMetaStoreClient` 是 **fe-core vendored 的那份**，不是 shade 里那份。删掉 fe-core 副本 =
> **悄悄换了 DLF 实现**。必须有意识地处理，不能顺手删。

### 3.2b ⚠️ 机制推演：Glue-on-HMS / paimon-on-DLF **今天很可能就是坏的**（可离线证实，见 HCS-01）

把 classloader 的实际代码路径走一遍（**全部行号已亲验**）：

```
ChildFirstClassLoader.loadClass(name)                    # fe-extension-loader
  ├─ isParentFirst(name)? parent-first 名单 =
  │     java./javax./sun./com.sun./org.slf4j./org.apache.logging./
  │     org.apache.doris.extension.spi./org.apache.doris.connector.api.
  │     + 连接器追加 org.apache.doris.connector./org.apache.doris.filesystem.
  │     → org.apache.hadoop.hive.* 和 com.amazonaws.* 都【不在】名单里
  ├─ findClass(name)                                     # 只看插件自己的 lib/
  └─ CNFE → super.loadClass(name)                        # 父回退
```

于是：

| 类 | 在插件 `lib/` 里吗 | 由谁定义 |
|---|---|---|
| `org.apache.hadoop.hive.metastore.IMetaStoreClient` | ✅ 在（hudi/iceberg zip 里**实查到** `lib/hive-catalog-shade-3.1.1.jar`，127 MB） | **child**（插件） |
| `RetryingMetaStoreClient` | ✅ 同上 | **child** |
| `com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient` | ❌ **不在**（shade jar 里 0 个条目） | 父回退 → **app loader** |

而 `ThriftHmsClient.java:644` 把 **TCCL 钉到插件 loader**（`getClass().getClassLoader()`），`:910` 才调
`RetryingMetaStoreClient.getProxy(conf, hookLoader, "com.amazonaws...AWSCatalogMetastoreClient")`。
hive 的 `getProxy` 内部走 `JavaUtils.getClass(name, IMetaStoreClient.class)` = `Class.forName(name, TCCL)`
+ **`.asSubclass(IMetaStoreClient.class)`**，而这里的 `IMetaStoreClient.class` 是从
`RetryingMetaStoreClient`（**child**）的定义 loader 解析的。

⇒ **app-loaded 的 `AWSCatalogMetastoreClient` 实现的是「app 的 `IMetaStoreClient`」，与「child 的
`IMetaStoreClient`」是两个不同的 `Class` 对象**（同名同版本，不同 loader ⇒ 不同类身份）
⇒ `asSubclass` 失败 ⇒ **`ClassCastException` / `LinkageError`**。

**paimon-on-DLF 更糟**：paimon 的私有 shade 提供的是 **hive 2.3.7** 的 `IMetaStoreClient`
（jar 内该 class 日期 2020-04-07），而父回退拿到的 fe-core vendored `ProxyMetaStoreClient` 实现的是
**hive 3.1.3** 的 —— **版本都不一样**。

> **📌 这不是猜测，是可以离线证实/证伪的**：写一个单测，用真实 plugin-zip 的 `lib/*.jar` 组一个
> `ChildFirstClassLoader`（parent = app loader），`Class.forName(客户端类名, false, child)` 后断言
> `childIMetaStoreClient.isAssignableFrom(...)`。**不需要真集群**。这就是 **HCS-01**。

**⇒ 对本任务的意义（关键）**：把 Glue / DLF 客户端**搬进插件**，会让调用方（`RetryingMetaStoreClient`）和
被调方（客户端）落在**同一个 child loader、同一份 `IMetaStoreClient` 身份**上 —— 这**不只是为了删 shade，
它本身就是在修一个大概率存在的线上 bug**。因此本方案**无论 HCS-01 结果如何都是严格改进**：
- 若 HCS-01 证明今天已坏 → 搬迁 = **修复**；
- 若 HCS-01 证明今天能跑（说明我推演错了某处）→ 搬迁 = **行为保持**（且必须靠 HCS-01 的测试守住）。

### 3.3 `fe-common` 的 `provided` 陷阱 —— 编译绿、运行炸

`fe/fe-common/pom.xml:89-91` 声明 shade 为 **`<scope>provided</scope>`**。

> **`provided` = 「编译时给我，打包时别带，运行时有别人提供」**。Maven **从不传递 provided**。

**用实跑 maven 否掉了「fe-core 是从 fe-common 传递拿到 shade」这个假设**：

```bash
cd fe && mvn -o dependency:tree -Dverbose \
  -Dincludes=org.apache.doris:hive-catalog-shade -pl fe-core -am
# （漏 -am 会因 ${revision} 假错）
```
→ fe-core 只有**一条 depth-1 的 compile 边**，零传递路径。**所以删 `fe-core:437-440` 确实能把 jar 踢出 `fe/lib`。**

**坑在**：`fe/fe-common/src/main/java/org/apache/doris/common/CatalogConfigFileUtils.java:23,95` 有
`public static HiveConf loadHiveConfFromHiveConfDir(...)`。fe-core 删依赖后，fe-common **照样编译通过**
（它自己 `provided` 着），但运行时 `fe/lib` 已无 shade → 加载 `CatalogConfigFileUtils` 时
`NoClassDefFoundError: org/apache/hadoop/hive/conf/HiveConf`。而这个类在**每一个外表 catalog** 的路径上
→ **炸的不只是 hive，是所有 catalog**。

**⇒ 这是一个两模块改动，fe-common 必须与 fe-core 同批（或更早）去 hive 化。**

---

## 4. 方案：五步走（详见 `tasklist.md`）

```
阶段 0  前置探测 + 决策拍板         ← 阻塞后续
阶段 1  独立冗余清理（不依赖任何决策，可立即做）
阶段 2  Glue 客户端归位  → fe-connector-hms
阶段 3  DLF 客户端归位  → fe-connector-paimon（iceberg 已自满足）
阶段 4  HiveConf 属性解析下沉（fe-core + fe-common 去 hive 化）
阶段 5  Ranger hive 审计去 hive-exec
阶段 6  pom 终局清理（4 处 fe-core + 1 处 fe-common）
阶段 7  守门 + e2e
```

**终局要删的 pom 块**（**只有跑完阶段 2–5 才能动**）：

| 文件:行 | 内容 | 备注 |
|---|---|---|
| `fe/fe-core/pom.xml:437-440` | `hive-catalog-shade` 本体 | |
| `fe/fe-core/pom.xml:217-223` | `commons-lang` 2.6 runtime | 已验证 fe-core 源码 0 处用 lang2；Ranger 2.8.0 也 0 处 |
| `fe/fe-core/pom.xml:926-967` | `bundle-fastutil-into-doris-fe` shade execution | **`fastutil-core` 依赖本身要留**（`:766-774`，`TabletInvertedIndex` 等 3 个文件在用），死的只是这个 plugin 块 |
| `fe/fe-core/pom.xml:776-782` | `<repositories>` 的 `central` | |
| `fe/fe-common/pom.xml:87-91` | `provided` shade | **必须与 fe-core 同批或更早** |

**不许动**：`fe/pom.xml:801-805` 版本钉 · `bin/start_fe.sh:355` 的 `doris-fe.jar` 钉序（通用机制，不是 hive 专属）
· `fastutil-core` 依赖 · BE 侧 `java-udf` / `avro-scanner` 的 pom（**独立供应链**，memory
`catalog-spi-be-java-ext-shared-classpath`：动它炸过 JNI scanner） · `doris-shade` 仓库本身。

### 4.1 ⛑ 降级路线（**必须事先约定**：e2e 不可得，不许硬上）

用户**无法 e2e 验证** Glue-on-HMS 与 paimon-on-DLF，且要求这两条路径**必须保留可用**。因此：

> **阶段 6（删 pom）是一道单向门 —— 只要阶段 2/3 里有任何一条客户端搬不动，就【不删】shade，收在降级档。**

| 档位 | 达成条件 | 交付物 | 价值 |
|---|---|---|---|
| **档 0（保底，无条件可交）** | 阶段 1 独立清理完成 | 删 `hudi-hadoop-mr` 等零引用依赖；`fe/lib` 少一个 `hive-storage-api` | 真实瘦身，零风险 |
| **档 1** | HCS-01 离线测试落地 | 一个**能证明 Glue/DLF 客户端在插件 loader 下能否被正确解析**的守门单测 | 把一个「谁也说不清」的问题变成 CI 里的红绿灯，**本身就有独立价值** |
| **档 2** | 阶段 2（Glue 搬迁）+ 阶段 4/5 完成，但 **D-2 spike 失败** | fe-core 去掉 Glue/HiveConf/Ranger，**但 `ProxyMetaStoreClient` 与 shade 依赖保留** | fe-core 大幅瘦身；shade 仍在 `fe/lib`，**目标未达成** |
| **档 3（完全体）** | 阶段 2–5 全绿 + 总判据 = 0 | 删 `fe-core:437-440` + 三个连带 hack + `fe-common:87-91` | **`fe/lib` 少 127 MB** |

**判据**：`grep` 总判据（见 `tasklist.md` 顶部）必须归 0 才允许进阶段 6。**归不了 0 就停在档 2，不许为了删而删。**

---

## 5. ⚠️ 待拍板决策（阶段 0，需要用户签字）

### D-1｜Glue 客户端（38 个 vendored 文件）搬去哪？

- **A（推荐）搬进 `fe-connector-hms`**：随 hive/iceberg/hudi 插件 zip 走 child-first，`ThriftHmsClient` 按名加载在插件内自满足。合「源相关代码归插件」的架构目标。代价：38 文件搬迁 + checkstyle。
- **B 换 upstream `aws-glue-datacatalog-client` jar**：省掉 vendored 副本，但 upstream 是否有匹配 hive 3.1.3 + 我们改名前缀的版本**未调研**，风险高。
- **C 先不动 Glue，只做阶段 1/4/5**：那样 shade 仍删不掉（Glue 需要 `IMetaStoreClient` 父类），**目标不达成**。

### D-2｜DLF 客户端 for paimon 怎么给？（**最高风险项，需要 spike**）

⚠️ **这一条不是「用户选一个」就完事的，它有真实的技术未知数**（HCS-30a spike）：

- fe-core 那份 vendored `ProxyMetaStoreClient` 有 **2193 行**，而且它**自己还依赖阿里云 DLF SDK 的其余部分**
  （`com.aliyun.datalake.metastore.common.*` / `.hive.common.utils.*`）—— 那 **1963 个 `com/aliyun` 类只在
  shade jar 里**。所以搬 DLF 到 paimon 插件 = 要同时给它 **客户端 + SDK 闭包**。
- 而且**版本口径对不上**：阿里云 artifact 是 `com.aliyun.datalake:metastore-client-hive3:0.2.14`（hive **3** API），
  paimon 的私有 shade 是 hive **2.3.7**。
  📎 **线索**：那个类的包名却叫 `...metastore.**hive2**.ProxyMetaStoreClient` —— 需要 spike 确认它到底实现
  哪个版本的 `IMetaStoreClient` 接口。

候选路线（spike 后再定）：
- **A** 把 `metastore-client-hive3` 加进 `fe-connector-paimon-hive-shade` 的 artifactSet，随 paimon 的 thrift 前缀
  一起 relocate（最干净，**前提是 hive API 版本对得上**）
- **B** 把 fe-core 那份 vendored 源码搬进 paimon 插件并改写 import 到 paimon 的前缀（工作量大，2193 行）
- **C** 让 paimon 也依赖 `hive-catalog-shade`（P5 当年明确拒绝：127 MB + fastutil 污染；且会和 paimon 私有 shade
  的 `IMetaStoreClient` 撞成两份 → **不推荐**）

### D-3｜Ranger hive 审计的 `HiveOperationType`（hive-exec）怎么去？

需要单独看一眼 `RangerHiveAuditHandler` 用了它什么。选项大致是：换成自建枚举 / 下沉到插件 / 保留（则 shade 删不掉）。
**阶段 0 需要一次小调研**（~1 个 subagent）。

### D-4｜~~是否先跑 e2e 前置探测~~ → **已由用户定调（2026-07-14）**

**用户无法验证 Glue-on-HMS 与 paimon-on-DLF，指示：一律按「需要保留、必须继续能用」设计。**

⇒ 方案基调随之固定：
1. **不得**因为「反正可能已经坏了」就删功能 —— 两条路径**必须**在改完后仍可用（且更正确）。
2. **不得**做无法验证的静默换实现（风险 R-2）—— DLF 客户端**必须逐字节保行为**或有明确论证。
3. **e2e 不可得 ⇒ 用离线的 classloader 复现测试代偿**（**HCS-01**，见 §3.2b）。这个测试完全离线可跑，
   而且比 e2e 更早、更便宜地把「客户端类能不能被插件 loader 正确解析成 `IMetaStoreClient`」钉死。
   **它同时是搬迁前的现状快照和搬迁后的验收门禁。**

---

## 6. 风险登记

| ID | 风险 | 说明 | 缓解 |
|---|---|---|---|
| **R-1** | **Glue-on-HMS / paimon-on-DLF 今天很可能就是坏的**（机制推演，见 §3.2b） | 客户端由**父**加载器定义（实现父的 `IMetaStoreClient`），调用方 `RetryingMetaStoreClient` 在**插件**里（child，另一份 `IMetaStoreClient` 身份）→ `asSubclass` 失败 → `ClassCastException`。paimon 更糟：child 是 hive **2.3.7**，父是 **3.1.3** | **HCS-01 离线 classloader 复现测试**（不需要真集群）。⚠️ **用户无法 e2e，故一律按「必须保留可用」设计**：搬进插件后 caller/callee 同 loader → 若今天已坏则是**修复**，若今天能跑则是**行为保持** |
| **R-2** | 静默换 DLF 实现 | `start_fe.sh:355` 钉 `doris-fe.jar` 最前 → 今天生效的是 fe-core vendored 副本（2193 行），**不是** shade 里那份。删 fe-core 副本 = 悄悄换实现 | ⚠️ **用户无法 e2e ⇒ 禁止无论证的静默换实现**。HCS-31 必须先 **diff 两份实现**并把差异写进 `progress.md`，无法论证等价就**保留 fe-core 那份的语义**（搬源码而非换 jar） |
| **R-2b** | **DLF 搬迁存在真实技术未知数** | vendored `ProxyMetaStoreClient`（2193 行）还依赖 shade 里**另外 1963 个** `com/aliyun` SDK 类；且阿里云 artifact 是 `metastore-client-hive3`（hive 3 API）而 paimon 私有 shade 是 hive **2.3.7** | **HCS-30a spike 先行**（D-2）。⚠️ **若 spike 判定不可安全搬迁 → 走 §4.1 降级路线，不硬上** |
| **R-3** | Glue 回归无人接得住 | 删 shade 但留 glue 树 → `NoClassDefFoundError`；删 glue 树但不搬 → `ThriftHmsClient:922` 按名 `ClassNotFoundException`。**编译器不报、单测不报** | HCS-01 的离线 classloader 测试**就是**这条的守门（e2e 不可得时的代偿） |
| **R-4** | 删完 `fe/lib` 仍非 hive-free | `hudi-hadoop-mr:1.0.2 → hudi-hadoop-common → hive-storage-api:2.8.1` 还会塞一个 hive jar 进 `fe/lib`，`org.apache.hadoop.hive.*` 变成**半填充命名空间**（比干净缺失更难查；文件系统插件对 `org.apache.hadoop.` 整个前缀是 parent-first） | 阶段 1 顺手删 `hudi-hadoop-mr`（fe-core 源码零引用） |
| **R-5** | fastutil 去 hack 后的重复 | shade 走后，`fastutil-core:8.5.18`（直接依赖）与完整 `fastutil:8.5.12`（fe-common → trino-main → clearspring）都进 `lib/`，都含 `Long2ObjectOpenHashMap`。**今天是良性的**（两份都有需要的方法），但从此靠 `lib/` 顺序而非显式钉 | 在 `fastutil-core` 依赖上留注释记录此事，否则将来一次版本 bump 会复现原 bug 而无人知道为什么 |
| **R-6** | 别信 `dependency:analyze` | `AliyunDLFBaseProperties.java:71` 把 `DataLakeConfig.CATALOG_PROXY_MODE` 当**注解 String 常量**用，javac 会**内联**进字节码 → 纯字节码扫描会报「依赖未使用」，但 `javac` 仍会因 `import` 失败 | 一律用 `grep import` + 真编译判定 |
| **R-7** | Ranger 版本耦合 | 若有人把 `ranger-plugins-common` 降到 2.8.0 以下，`commons-lang` 2.x 会因**无关原因**重新变成必需（2.7.0 有 171 处 lang2 引用） | Ranger 每次 bump 后重跑 verbose dependency:tree |

---

## 7. 附录：主 session 亲验的事实（可复现命令）

```bash
cd /mnt/disk1/yy/git/wt-catalog-spi

# fe-core main 里 import hive/shade/datalake 的文件（基线 = 26）
grep -rlE '^import (org\.apache\.hadoop\.hive\.|shade\.doris\.hive\.|com\.aliyun\.datalake\.)' \
     fe/fe-core/src/main/java | wc -l            # → 26   ⚠️ 这就是「删完了没」的判据，目标 = 0
grep -rlE '^import (org\.apache\.hadoop\.hive\.|shade\.doris\.hive\.|com\.aliyun\.datalake\.)' \
     fe/fe-core/src/test/java | wc -l            # → 4
find fe/fe-core/src/main/java/com/amazonaws/glue -name '*.java' | wc -l   # → 38

# fe-common 的 provided 陷阱
grep -rn 'hadoop.hive' fe/fe-common/src/main/java   # → CatalogConfigFileUtils.java:23
grep -rn 'loadHiveConfFromHiveConfDir' --include=*.java fe/
#   → fe-common CatalogConfigFileUtils.java:95（定义）
#     fe-core  HMSBaseProperties.java:192 / DefaultConnectorContext.java:208（调用）

# 按名字反射加载 Glue/DLF 的地方
grep -rn 'AWSCatalogMetastoreClient\|ProxyMetaStoreClient' --include=*.java fe/fe-connector/

# shade 3.1.1 jar 实证（注意：别用 doris-shade/target 里的 3.1.2-SNAPSHOT，它已改名 fastutil → 会误判）
J=~/.m2/repository/org/apache/doris/hive-catalog-shade/3.1.1/hive-catalog-shade-3.1.1.jar
unzip -l "$J" | awk '{print $4}' | grep -c '^org/apache/thrift/.*\.class$'                 # → 0
unzip -l "$J" | awk '{print $4}' | grep -c '^shade/doris/hive/org/apache/thrift/.*\.class$' # → 179
unzip -l "$J" | awk '{print $4}' | grep -c '^it/unimi/dsi/fastutil/.*\.class$'              # → 10653
unzip -l "$J" | grep -c 'AWSCatalogMetastoreClient'                                         # → 0  ⚠️
unzip -l "$J" | grep -c 'ProxyMetaStoreClient'                                              # → 2

# 依赖图真相（漏 -am 会因 ${revision} 假错）
cd fe && mvn -o dependency:tree -Dverbose \
  -Dincludes=org.apache.doris:hive-catalog-shade -pl fe-core -am
```

**未亲验、来自 agent 的次级事实**（用前请自行确认）：hudi 解析版本 = 1.0.2 且经
`hudi-hadoop-mr → hudi-hadoop-common → hive-storage-api:2.8.1` 进 `fe/lib`；Ranger 2.7.0 有 171 处 lang2 引用。

---

## 8. 铁律（继承主线，本任务同样适用）

- **【铁律 A｜fe-core 只出不进】** 本任务期间 fe-core 源码**只减不增**，不得新增/搬入任何数据源直接相关代码。
- **【铁律 B｜禁 deletion-scaffolding 式搬迁】** 不得为「删 A 能编译过」把 A 的逻辑就近挪进 fe-core util。
  遇此情形**停手**，重分析真实归属，出方案交用户 review。
- （memory `fe-core-source-isolation-iron-rules` · `catalog-spi-no-property-parsing-in-fecore`）
