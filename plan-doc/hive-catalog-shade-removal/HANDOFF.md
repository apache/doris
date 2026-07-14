# 🤝 Session Handoff — 删除 thrift 一代 Glue/DLF ⇒ 剔除 `hive-catalog-shade`

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**。
> 完成的明细**不落这里**（在 `git log` + [`progress.md`](./progress.md) 里）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) · 清单 [`tasklist.md`](./tasklist.md)

---

# 🆕 下一个 session = **阶段 1（T-20）起，直接开工**

## 状态：**调研已完成，方案已用户签字，尚未动任何生产代码**

**基线 HEAD** = `657417bec32`（`catalog-spi-11-hive`）。本任务至今**零生产代码改动**，只有文档 + 一个复现程序。

---

## 🚩 最重要的一件事：**任务定性变了，别按旧方案做**

**原方案（把 Glue/DLF 客户端搬进插件）已作废。** 用户 2026-07-14 拍板：

> **「把 glue 和 dlf 这两个功能先删掉，不再支持了，不作为需要迁移的内容。」**
> 范围 = **只删「走 HMS thrift 协议的那一代」**。

**为什么会翻案**：上一轮跑了离线 classloader 实验，**实测证明 thrift 一代已经全坏**（不是「可能坏」）——
是本分支 cutover 引入的回归。用户在信息充分的前提下决定**不修、直接删**。

⇒ 本任务塌缩为**纯删除**：**不做**守门测试 · **不做** paimon-on-DLF · **不做**降级档 ·
总判据自然归零 · fe-core 全程只减不增（铁律 A/B 天然满足）。

---

## 🔴 删除边界 —— **误删比漏删严重得多**

| | ❌ **删**（thrift 一代，实测已坏） | ✋ **留**（今天可用，与 shade 无关） |
|---|---|---|
| **Glue** | `hive.metastore.type=glue` → vendored `AWSCatalogMetastoreClient` 树 | `iceberg.catalog.type=glue` → iceberg 官方 `org.apache.iceberg.aws.glue.GlueCatalog`（AWS SDK **v2**，已在 iceberg 插件包内） |
| **DLF** | `hive.metastore.type=dlf` · `iceberg.catalog.type=dlf` · paimon `paimon.catalog.type=dlf`（全经 `ProxyMetaStoreClient`） | **DLF 2.0 REST**：paimon `rest` + dlf token（`PaimonRestMetaStoreProperties`） |

**同名不同物，别按关键字 grep 一把梭删。**

---

## ▶️ 第一件事：**T-20 —— 修 iceberg 原生 Glue 的凭证类**（不是删除，是修 bug）

这条**最容易被漏**，且它在**保留路径**上：

`IcebergCatalogFactory.java:559-560` 在用户填了 **AK/SK** 时，把 `client.credentials-provider` 设成
`com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x` —— 这个类**住在 fe-core**，
**不在 iceberg 插件包里**。iceberg 的 `AwsClientProperties` 用**自己（child）的** loader 按名加载它，再
`asSubclass` 到 **child 的** `software.amazon.awssdk...AwsCredentialsProvider` → app-loaded 的 Provider2x
实现的是 **app 的**那个接口 → **ClassCastException**。

**实测**（`loader-probe-reproduction.java.txt`）：
```
software.amazon.awssdk.auth.credentials.AwsCredentialsProvider  -> ChildFirstClassLoader
com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x -> app-model
VERDICT native-Glue creds: isAssignableFrom = false   [BROKEN]
```

**修法（T-20）**：把这 **50 行**从 fe-core 搬进 **`fe-connector-iceberg`**。进了插件包就是 child 加载
→ 与 child 的 `AwsCredentialsProvider` 同 loader → **自愈**。合铁律 A（是**出** fe-core）。

⚠️ 同目录的 `ConfigurationAWSCredentialsProvider.java`(v1) 与 `ConfigurationAWSCredentialsProviderFactory.java`
**属 thrift 一代 → 随 T-30 删**，别一起搬。
⚠️ IAM-role 分支（`AssumeRoleAwsClientFactory.class.getName()`，**编译期类引用**）与默认凭证链分支**没病**。

---

## 🔴 两个「删了会静默走错路」的坑（T-31 / T-41）

`ThriftHmsClient.getMetastoreClientClassName` 是 `if(dlf) … else if(glue) … else → HiveMetaStoreClient`。
**光删分支** ⇒ `hive.metastore.type=glue` 落到 `else` ⇒ **静默去连普通 HMS**（用户以为连的是 Glue）。

⇒ **必须改成显式抛异常**。文案范式：`IcebergCatalogFactory.java:310-313` 的
`"Unknown ...: X. Supported types: ..."`。

---

## ⚠️ 落刀前必须先解决的两个未决问题

1. **T-54（删 `hudi-hadoop-mr`）**：对抗验证指出 **`orc-core-1.8.4`（`fe/lib` 里的实装 jar）的公开 API
   直接依赖 `hive-storage-api` 的类**。删 `hive-storage-api` 会不会炸 orc？**查清前不许删。**
2. **T-51（删 `HMSBaseProperties` 等）**：「已是死代码」的证据链**有洞** ——
   `PluginDrivenExternalCatalog.java:156` 设 supplier，但 `createConnectorFromProperties()` 在 **:129**
   就被调用（**早于** :156）。**先复验有无真实调用窗口。**
   且 `HiveTable.java` / `HMSResource.java` 是 **GSON 持久化活类**，只为拿 String 常量而 import 它。

---

## 🚫 别做的事

- **别按关键字 grep 一把梭删 glue/dlf** —— 保留路径同名（见上表）
- **别做守门测试 / paimon-on-DLF / 降级档** —— 用户已明确「不做」
- 别信前一轮的「5 项依赖全零引用」：**`kryo-shaded` 绝不可删**（`WorkloadSchedPolicy.java:32,287,298`
  经 minlog 传递依赖真实调用）；`avro` 删声明但 jar 删不掉（三方传递）
- 别拿 `doris-shade/.../target/hive-catalog-shade-3.1.2-SNAPSHOT.jar` 做验证 —— 它**已经**改名了 fastutil，
  会让你误判「fastutil hack 已死」。**构建实际解析的是 3.1.1**（`~/.m2/.../3.1.1/`）
- 别为「删得动」把 HiveConf 逻辑挪进 fe-core util（**铁律 B**，遇到就停手交 review）
- 别动：`fe/pom.xml:801-805` 版本钉 · `bin/start_fe.sh` 钉序 · `fastutil-core` 依赖 ·
  BE `java-udf`/`avro-scanner` pom · `doris-shade` 仓库

## ⚙️ 操作须知

- maven 必须绝对路径 + `-am`：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile`
  （**漏 `-am` → 假错 `${revision}`**）
- **验证信 LOG 不信 exit**：后台 task 的 exit code 是 wrapper 的；重定向到文件跑，grep
  `BUILD SUCCESS`/`BUILD FAILURE`/`[ERROR].*\.java:`/`Checkstyle`（memory `doris-build-verify-gotchas`）
- **⚠️ `git add` 用 path-whitelist，严禁 `git add -A`**（工作树大量非本线程 scratch）
- 动码前先探并发（memory `concurrent-sessions-shared-worktree-hazard`）—— 主线 session 可能也在改 pom
- 复现实验：`loader-probe-reproduction.java.txt`（本目录）。跑法：
  `javac -cp fe/fe-extension-loader/target/fe-extension-loader-1.2-SNAPSHOT.jar -d . LoaderProbe.java`
  然后 `java -cp ".:<该jar>" LoaderProbe`。**依赖 `output/fe/` 已构建。**
