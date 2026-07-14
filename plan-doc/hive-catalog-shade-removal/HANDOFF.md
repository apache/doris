# 🤝 Session Handoff — 删除 thrift 一代 Glue/DLF ⇒ 剔除 `hive-catalog-shade`

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**。
> 完成的明细**不落这里**（在 `git log` + [`progress.md`](./progress.md) 里）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) · 清单 [`tasklist.md`](./tasklist.md)

---

# 🆕 下一个 session = **阶段 3（T-40）起，删 DLF thrift 一代**

## 状态：**阶段 1 + 阶段 2 已完成并提交**

**基线 HEAD** = `e43173eca67`（`catalog-spi-11-hive`）。

| 阶段 | commit | 结果 |
|---|---|---|
| 1 — 修 iceberg 原生 Glue 凭证类 | `2cd01ada8df` | 搬出 fe-core + 改包；probe 判据 false→**true** 实测 |
| 2 — 删 Glue thrift 一代 | `e43173eca67` | **-11,245 行**；fe-core `com.amazonaws.*` **归零** |

**总判据**：`26 → 7`（余下 7 个是 hive/DLF import，阶段 3/4 处理）。

---

## 🚩 任务定性 = **纯删除**（别按旧方案做）

用户 2026-07-14 拍板：**「把 glue 和 dlf 这两个功能先删掉，不再支持了」**，
范围 = **只删「走 HMS thrift 协议的那一代」**。理由：实测证明该代已全坏（本分支 cutover 引入的回归）。

⇒ **不做**守门测试 · **不做** paimon-on-DLF · **不做**降级档 · fe-core 全程只减不增。

---

## 🔴 删除边界 —— **误删比漏删严重得多**

| | ❌ **删**（thrift 一代，实测已坏） | ✋ **留**（今天可用） |
|---|---|---|
| **DLF** | `hive.metastore.type=dlf` · `iceberg.catalog.type=dlf`（`DLFCatalog`/`DLFClientPool`）· paimon `paimon.catalog.type=dlf` —— 全经 `ProxyMetaStoreClient` | **DLF 2.0 REST**：paimon `paimon.catalog.type=rest` + dlf token（`PaimonRestMetaStoreProperties`） |
| **Glue** | *(阶段 2 已删光)* | `iceberg.catalog.type=glue`（iceberg 官方 `GlueCatalog`）**← 阶段 1 刚修好，别碰坏** |

**同名不同物，别按关键字 grep 一把梭删。**

---

## ▶️ 第一件事：**T-40 —— 删 `fe/fe-core/src/main/java/com/aliyun/datalake/**`**

（`ProxyMetaStoreClient.java`，2193 行）。**建议先照阶段 2 的方式跑一轮侦察+对抗验证再落刀**——
阶段 2 的经验是：侦察挖出的东西（生命周期坑、URI 遮蔽）**全都不在原计划里**，且都是会出事的。

### 🔴 阶段 2 已趟平的路，阶段 3 直接复用（**别重新踩**）

**1. 拒绝逻辑的位置 —— 放错会让 FE 起不来**

| 位置 | 新建 catalog | **edit-log 回放** |
|---|---|---|
| `validateProperties` | ✅ 报错 | ✅ **不跑**（`!isReplay` 门）→ 安全 |
| `create()` / 连接器构造函数 | ✅ 报错 | 💀 **会跑** → 抛异常 → **`System.exit(-1)`，FE 起不来** |
| `createClient()` | ✅ 报错 | ✅ 懒加载 → 安全 |

**DLF 的拒绝直接加在阶段 2 已建好的 `HmsClientConfig.removedMetastoreTypeError` 里**（同一个方法，
加一个 dlf 分支即可），两个调用点（`HiveConnectorProvider.validateProperties` +
`HiveConnector.createClient`）**已经就位，无须新增**。

- `validateProperties` 处**必须** `IllegalArgumentException`（catalog 层只解包这一种，文案才原样透出）
- `createClient` 处**必须** `DorisConnectorException`（fe-core 有 5+ 处专门 catch 它）
- ⚠️ **别照抄 lakesoul 先例**（`CatalogFactory.java:141-142`）—— 它的 throw 没有 isReplay 门，是潜在启动 bug

**2. 文案要同步**：`HmsClientConfig.removedMetastoreTypeError` 现在写的是
`Supported types: hms, dlf` —— **删 DLF 后要去掉 `dlf`**。
`IcebergCatalogFactory:311-313` 的 `Supported types: rest, hms, glue, hadoop, jdbc, s3tables, dlf`
也要去掉 `dlf`（✋ **`glue` 要留**）。

**3. 常量处置范式**：`METASTORE_TYPE_GLUE` 没删，改名 `METASTORE_TYPE_GLUE_REMOVED` 供拒绝逻辑使用。
`METASTORE_TYPE_DLF` 同理 —— 删了 dlf 分派后它仍要留给拒绝逻辑。

**4. 测试**：`getMetastoreClientClassName` 此前零覆盖，阶段 2 已建
`HmsClientConfigRemovedTypeTest`。DLF 的拒绝**往这个类里加用例**即可。
⚠️ 该类现有用例 `survivingTypesAreNotRejectedAndStillRouteToTheirClients` **断言 dlf 仍路由到
`ProxyMetaStoreClient`** —— 删 DLF 时这个用例要改。

### 阶段 3 的清单（tasklist 里的 T-40~T-45）

- **T-42 前先查清** `HiveConnectorMetadata.java:1562` 的 dlf 判断到底做什么
- **T-45 最需谨慎**：连接器侧 Dlf 属性类**按「是否被 REST 那代共用」逐个判定** ——
  共用则**不能删**，只能删其 thrift 消费者。`AbstractDlfMetaStoreProperties` 尤其要先确认。

---

## ⚠️ 后续阶段仍未解决的两个问题（**沿用**）

1. **删 `hudi-hadoop-mr`**：对抗验证指出 **`orc-core-1.8.4`（`fe/lib` 实装 jar）的公开 API 直接依赖
   `hive-storage-api` 的类**。删 `hive-storage-api` 会不会炸 orc？**查清前不许删。**
2. **删 `HMSBaseProperties` 等**：「已是死代码」的证据链**有洞** —— `PluginDrivenExternalCatalog.java:156`
   设 supplier，但 `createConnectorFromProperties()` 在 **:129** 就被调用（**早于** :156）。**先复验。**
   且 `HiveTable.java` / `HMSResource.java` 是 **GSON 持久化活类**，只为拿 String 常量而 import 它。

---

## 🆕 待用户拍板（**别自作主张改**）

**`create()` 静音丢弃 session token**（既有 bug，非本分支引入）。
`IcebergCatalogFactory` 发 `client.credentials-provider.glue.session_token`、单测断言它，但
`ConfigurationAWSCredentialsProvider2x.create()` 只造 `AwsBasicCredentials`，**从不造 `AwsSessionCredentials`**
→ 用临时 STS 凭证的用户认证必失败。修法一处，但属**行为变更** → **必须签字**。见 `tasklist.md` T-21。

---

## 🚫 别做的事

- **别按关键字 grep 一把梭删 dlf** —— DLF 2.0 REST 是保留路径，同名
- **别碰阶段 1/2 的成果**：`org.apache.doris.connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x` ·
  `IcebergConnectorProperties` 的 `GLUE_CREDENTIALS_PROVIDER_KEY`/`_2X`/AK/SK/token ·
  `IcebergCatalogFactory.appendGlueProperties` 的其余 put · `HmsClientConfigRemovedTypeTest`
- **别做守门测试 / paimon-on-DLF / 降级档** —— 用户已明确「不做」
- 别信「5 项依赖全零引用」：**`kryo-shaded` 绝不可删**（`WorkloadSchedPolicy.java:32,287,298` 经 minlog
  传递依赖真实调用）；`avro` 删声明但 jar 删不掉（三方传递）
- 别拿 `doris-shade/.../target/hive-catalog-shade-3.1.2-SNAPSHOT.jar` 做验证 —— 它**已经**改名了 fastutil，
  会误判「fastutil hack 已死」。**构建实际解析的是 3.1.1**（`~/.m2/.../3.1.1/`）
- 别为「删得动」把 HiveConf 逻辑挪进 fe-core util（**铁律 B**，遇到就停手交 review）
- 别动：`fe/pom.xml:801-805` 版本钉 · `bin/start_fe.sh` 钉序 · `fastutil-core` 依赖 ·
  BE `java-udf`/`avro-scanner` pom · `doris-shade` 仓库

## ⚙️ 操作须知

- **⚠️ 连接器模块路径是嵌套的**：`fe/fe-connector/fe-connector-XXX`（**不是** `fe/fe-connector-XXX`）。
  `-pl fe-connector/fe-connector-hms`，**且 `-am` 必填**
- **`-am` 漏了会出假错**：报 `org.apache.doris:fe-connector:pom:${revision}` 无法解析 →
  **那是没编译，不是代码错**。阶段 2 在变异验证时踩了这个坑、误判过一次结论
- maven 必须绝对路径：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile`
- **验证信 LOG 不信 exit**：重定向到文件跑，grep `BUILD SUCCESS`/`BUILD FAILURE`/`[ERROR].*\.java:`/`Checkstyle`
- **⚠️ `git add` 用 path-whitelist，严禁 `git add -A`**（工作树大量非本线程 scratch）
- 动码前先探并发（memory `concurrent-sessions-shared-worktree-hazard`）
- 复现实验：`loader-probe-reproduction.java.txt`（本目录）。**依赖 `output/fe/` 已构建。**
