# 🤝 Session Handoff — 删除 thrift 一代 Glue/DLF ⇒ 剔除 `hive-catalog-shade`

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**。
> 完成的明细**不落这里**（在 `git log` + [`progress.md`](./progress.md) 里）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) · 清单 [`tasklist.md`](./tasklist.md)

---

# 🆕 下一个 session = **阶段 2（T-30）起，开删 Glue thrift 一代**

## 状态：**阶段 1 已完成并提交**（`2cd01ada8df`），保留路径的凭证 bug 已修复并实测

**基线 HEAD** = `2cd01ada8df`（`catalog-spi-11-hive`）。

---

## 🚩 最重要的一件事：**任务定性 = 纯删除，别按旧方案做**

用户 2026-07-14 拍板：

> **「把 glue 和 dlf 这两个功能先删掉，不再支持了，不作为需要迁移的内容。」**
> 范围 = **只删「走 HMS thrift 协议的那一代」**。

**为什么**：离线 classloader 实验**实测证明 thrift 一代已全坏**（本分支 cutover 引入的回归）。
用户在信息充分下决定**不修、直接删**。

⇒ **不做**守门测试 · **不做** paimon-on-DLF · **不做**降级档 · 总判据自然归零 · fe-core 全程只减不增。

---

## 🔴 删除边界 —— **误删比漏删严重得多**

| | ❌ **删**（thrift 一代，实测已坏） | ✋ **留**（今天可用，与 shade 无关） |
|---|---|---|
| **Glue** | `hive.metastore.type=glue` → vendored `AWSCatalogMetastoreClient` 树 | `iceberg.catalog.type=glue` → iceberg 官方 `GlueCatalog`（AWS SDK **v2**，在插件包内）**← 阶段 1 刚修好，别碰坏** |
| **DLF** | `hive.metastore.type=dlf` · `iceberg.catalog.type=dlf` · paimon `paimon.catalog.type=dlf`（全经 `ProxyMetaStoreClient`） | **DLF 2.0 REST**：paimon `rest` + dlf token（`PaimonRestMetaStoreProperties`） |

**同名不同物，别按关键字 grep 一把梭删。**

---

## ▶️ 第一件事：**T-30 —— 删 `fe/fe-core/src/main/java/com/amazonaws/glue/**`**

**38 个 .java 中删 37 个**。第 38 个（`ConfigurationAWSCredentialsProvider2x.java`）**已由阶段 1 搬走**
（现在在 `fe/fe-connector/fe-connector-iceberg/.../connector/iceberg/glue/`）—— 那个目录现在只剩
v1 的 `ConfigurationAWSCredentialsProvider.java` + `ConfigurationAWSCredentialsProviderFactory.java`，**两个都删**。

树内自洽；`aws-java-sdk-glue` 为该树独占（树内 712 处引用，树外 0）。

### 🔴 落 T-30 前必须先查的一件事（阶段 1 侦察挖出，**清单里原本没有**）

`IcebergCatalogFactory.java:563-564` 会往 iceberg 参数里发
`aws.catalog.credentials.provider.factory.class` → `com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProviderFactory`
—— **指向一个即将被 T-30 删掉的 fe-core 类**，是第二条 fe-core→插件的按名耦合。

T-34 的结论是「iceberg 原生 `AwsClientProperties` **从不读**这个 key（javap 已证），只有**被删的** thrift 树读它
（`AWSGlueClientFactory.java:114-118`）→ 删除行为中性」。**落刀前用 javap 复核一遍这个结论**，
确认删掉那条 `opts.put` 不改变任何存活路径的行为。

---

## 🔴 两个「删了会静默走错路」的坑（T-31 / T-41）

`ThriftHmsClient.getMetastoreClientClassName` 是 `if(dlf) … else if(glue) … else → HiveMetaStoreClient`。
**光删分支** ⇒ `hive.metastore.type=glue` 落到 `else` ⇒ **静默去连普通 HMS**（用户以为连的是 Glue）。

⇒ **必须改成显式抛异常**。文案范式：`IcebergCatalogFactory.java:311-313` 的
`"Unknown ...: X. Supported types: ..."`（⚠️ 该文案里的 `glue` 要留、`dlf` 要去掉）。

---

## ⚠️ 落刀前必须先解决的两个未决问题（**沿用，未动**）

1. **T-54（删 `hudi-hadoop-mr`）**：对抗验证指出 **`orc-core-1.8.4`（`fe/lib` 里的实装 jar）的公开 API
   直接依赖 `hive-storage-api` 的类**。删 `hive-storage-api` 会不会炸 orc？**查清前不许删。**
2. **T-51（删 `HMSBaseProperties` 等）**：「已是死代码」的证据链**有洞** ——
   `PluginDrivenExternalCatalog.java:156` 设 supplier，但 `createConnectorFromProperties()` 在 **:129**
   就被调用（**早于** :156）。**先复验有无真实调用窗口。**
   且 `HiveTable.java` / `HMSResource.java` 是 **GSON 持久化活类**，只为拿 String 常量而 import 它。

---

## 🆕 一个待用户拍板的事（**别自作主张改**）

**T-21：`create()` 静音丢弃 session token**（既有 bug，非本分支引入）。
`IcebergCatalogFactory.java:565-566` 发 `client.credentials-provider.glue.session_token`、单测 `:559` 断言它，
但 `ConfigurationAWSCredentialsProvider2x.create()` 只造 `AwsBasicCredentials`，**从不造 `AwsSessionCredentials`**
→ 用临时 STS 凭证的用户认证必失败。修法一处，但属**行为变更** → **必须签字**。已在 `tasklist.md` 记为 T-21。

---

## 🚫 别做的事

- **别按关键字 grep 一把梭删 glue/dlf** —— 保留路径同名（见上表）
- **别碰阶段 1 刚修好的东西**：`org.apache.doris.connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x`
  与 `IcebergConnectorProperties.java:141-142` 的常量、`IcebergCatalogFactory.java:558-562`、`:565-566`
- **别做守门测试 / paimon-on-DLF / 降级档** —— 用户已明确「不做」
- 别信前一轮的「5 项依赖全零引用」：**`kryo-shaded` 绝不可删**（`WorkloadSchedPolicy.java:32,287,298`
  经 minlog 传递依赖真实调用）；`avro` 删声明但 jar 删不掉（三方传递）
- 别拿 `doris-shade/.../target/hive-catalog-shade-3.1.2-SNAPSHOT.jar` 做验证 —— 它**已经**改名了 fastutil，
  会让你误判「fastutil hack 已死」。**构建实际解析的是 3.1.1**（`~/.m2/.../3.1.1/`）
- 别为「删得动」把 HiveConf 逻辑挪进 fe-core util（**铁律 B**，遇到就停手交 review）
- 别动：`fe/pom.xml:801-805` 版本钉 · `bin/start_fe.sh` 钉序 · `fastutil-core` 依赖 ·
  BE `java-udf`/`avro-scanner` pom · `doris-shade` 仓库

## ⚙️ 操作须知

- **⚠️ 模块真实路径是嵌套的**：`fe/fe-connector/fe-connector-iceberg`（**不是** `fe/fe-connector-iceberg`）。
  `-pl` 要写 `fe-connector/fe-connector-iceberg`，**且 `-am` 必填**（漏了 → 假错 `${revision}`）
- maven 必须绝对路径：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile`
- **验证信 LOG 不信 exit**：后台 task 的 exit code 是 wrapper 的；重定向到文件跑，grep
  `BUILD SUCCESS`/`BUILD FAILURE`/`[ERROR].*\.java:`/`Checkstyle`（memory `doris-build-verify-gotchas`）
- **⚠️ `git add` 用 path-whitelist，严禁 `git add -A`**（工作树大量非本线程 scratch）
- 动码前先探并发（memory `concurrent-sessions-shared-worktree-hazard`）—— 主线 session 可能也在改 pom
- 复现实验：`loader-probe-reproduction.java.txt`（本目录）。**依赖 `output/fe/` 已构建。**
  阶段 1 的凭证类专用探针见 `progress.md` 2026-07-14（四），跑法同上，判据 `isAssignableFrom`
