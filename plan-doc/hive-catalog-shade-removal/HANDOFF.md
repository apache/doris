# 🤝 Session Handoff — 删除 thrift 一代 Glue/DLF ⇒ 剔除 `hive-catalog-shade`

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**。
> 完成的明细**不落这里**（在 `git log` + [`progress.md`](./progress.md) 里）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) · 清单 [`tasklist.md`](./tasklist.md)

---

# 🆕 下一个 session = **阶段 4（T-50）起，fe-core / fe-common 去 hive 化**

## 状态：**阶段 1 / 2 / 3 已完成并提交**

**基线 HEAD** = `a0f65c353a9`（`catalog-spi-11-hive`）。

| 阶段 | commit | 结果 |
|---|---|---|
| 1 — 修 iceberg 原生 Glue 凭证类 | `2cd01ada8df` | 搬出 fe-core + 改包；probe 判据 false→**true** 实测 |
| 2 — 删 Glue thrift 一代 | `e43173eca67` | **-11,245 行**；fe-core `com.amazonaws.*` **归零** |
| 3 — 删 DLF thrift 一代 | `a0f65c353a9` | **-4,095 行**；fe-core `com.aliyun.datalake` **归零** |

**总判据**：`26 → 4`。剩下的 4 个文件就是**阶段 4 的全部工作面**：

```bash
grep -rlE '^import (org\.apache\.hadoop\.hive\.|shade\.doris\.hive\.|com\.aliyun\.datalake\.)' \
     fe/fe-core/src/main/java     # 现在 4 → 目标 0
grep -rn 'hadoop.hive' fe/fe-common/src/main/java    # 仍 1 处 → 目标 空
```

**⛔ 阶段 5（动 pom / 删 127MB jar）只有总判据归零才能开始。**

---

## ▶️ 阶段 4 的四件事（`tasklist.md` T-50~T-54）

**先跑一轮侦察+对抗验证再落刀** —— 前三个阶段的教训：**侦察挖出的东西全都不在原计划里，且都会出事**
（阶段 2 的 `System.exit(-1)` 坑、阶段 3 的**明文泄漏**坑）。

- **T-50 Ranger 死代码**：纯删除 ~12 行，已用真编译双向验证过。**最简单，可先做。**
  ⚠️ 必须同删 `EnumSet`/`HashSet` 两个 import，否则 checkstyle `UnusedImports` 挂。
- **T-51 删 `HMSBaseProperties` / `AbstractHiveProperties`**：🔴 **证据链有洞，必须先复验** ——
  `PluginDrivenExternalCatalog.java:156` 设 supplier，但 `createConnectorFromProperties()` 在 **:129**
  就被调用（**早于** :156）。且 `HiveTable.java` / `HMSResource.java` 是 **GSON 持久化活类**，
  只为拿 String 常量而 import 它 → **处置须合铁律 A/B**（别就近塞 fe-core util）。
- **T-52 fe-common 去 hive 化**：删 `CatalogConfigFileUtils` 的 `HiveConf` import + `loadHiveConfFromHiveConfDir`。
  ✋ 同文件的 `loadConfigurationFromHadoopConfDir` **服务所有外表 catalog，必须留**。
- **T-53 `loadHiveConfResources`**：**不是死代码**（iceberg/paimon 真调）。现语义 = 把 `new HiveConf()` 的
  **上千条默认值**返回给插件。**Glue/DLF 删完后这条链还需要吗？** ⚠️ **换实现必须显式论证**
  （不得静默改变那个默认值集合）。⚠️ `ConnectorContext` 上出现 hive 专有方法**本身违反**「通用 SPI 禁 source-specific」。
  **这是阶段 4 最难的一点。**
- **T-54 删 `hudi-hadoop-mr`**：🔴 **落刀前必查** —— 对抗验证指出 **`orc-core-1.8.4`（`fe/lib` 实装 jar）的
  公开 API 直接依赖 `hive-storage-api` 的类**。删 `hive-storage-api` 是否炸 orc？**未解决前不许删。**
  ✋ `hudi-common` 要留（`HttpProperties`/`StatisticsCache` 各一处 import）。

---

## 🔴 前三阶段趟平的路（**别重新踩**）

**1. 拒绝逻辑的位置 —— 放错会让 FE 起不来**

| 位置 | 新建 catalog | **edit-log 回放** |
|---|---|---|
| `validateProperties` | ✅ 报错 | ✅ **不跑**（`!isReplay` 门）→ 安全 |
| `create()` / 连接器构造函数 | ✅ 报错 | 💀 **会跑** → 抛异常 → **`System.exit(-1)`，FE 起不来** |
| `createClient()` / 懒加载路径 | ✅ 报错 | ✅ 不跑 → 安全 |

现成设施：`HmsClientConfig.removedMetastoreTypeError` + `REMOVED_METASTORE_TYPES` 表，
两个调用点（`HiveConnectorProvider.validateProperties` 抛 `IllegalArgumentException`、
`HiveConnector.createClient` 抛 `DorisConnectorException`）已就位。**再有类型要移除，往表里加一行即可。**
⚠️ 别照抄 lakesoul 先例（`CatalogFactory.java:141-142`）—— 它的 throw 没有 isReplay 门，是潜在启动 bug。

**2. 🔴 删属性类前先查它是不是脱敏注册的来源**（阶段 3 血的教训）

`DatasourcePrintableMap` 用**反射**从属性类取 sensitive key。删类 = 删脱敏。
**别假设「别的类覆盖了同样的 key」** —— 阶段 3 实测重叠**不均匀**（4 个别名只 1 个被覆盖），
而**老 catalog 回放后仍可 `SHOW CREATE CATALOG`** → 会**明文泄漏**。
判法：`javap -v -p` 逐字段列出 sensitive 别名，再逐字对比其它注册类。
修法范式：把 key **显式列出**（同文件的 iceberg REST 块 + 阶段 3 的 DLF 块都是这么做的）。

**3. 老 catalog 升级路径**：从 image 反序列化（`CatalogMgr.read` → GSON 直接建对象，**`CatalogFactory` 全程不参与**），
**永不经** `validateProperties`。所以任何「移除某能力」的改动都要想清楚老 catalog 会怎样。

---

## 🚫 别做的事

- **别按关键字 grep 一把梭删** —— 保留路径同名：`iceberg.catalog.type=glue`（阶段 1 刚修好）·
  `paimon.catalog.type=rest` + dlf token（DLF 2.0）· `fe-filesystem-oss` 的 `dlf.*` 别名（OSS 凭证用）
- **别碰阶段 1/2/3 的成果**：`connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x` ·
  `HmsClientConfig.removedMetastoreTypeError` + 两个调用点 · `DatasourcePrintableMap` 的 4 个 dlf 脱敏 key ·
  `HmsClientConfigRemovedTypeTest`
- **别做守门测试 / paimon-on-DLF / 降级档** —— 用户已明确「不做」
- 别信「5 项依赖全零引用」：**`kryo-shaded` 绝不可删**（`WorkloadSchedPolicy.java:32,287,298` 经 minlog
  传递依赖真实调用）；`avro` 删声明但 jar 删不掉（三方传递）
- 别拿 `doris-shade/.../target/hive-catalog-shade-3.1.2-SNAPSHOT.jar` 做验证 —— 它**已经**改名了 fastutil，
  会误判「fastutil hack 已死」。**构建实际解析的是 3.1.1**（`~/.m2/.../3.1.1/`）
- 别为「删得动」把 HiveConf 逻辑挪进 fe-core util（**铁律 B**，遇到就停手交 review）
- 别动：`fe/pom.xml:801-805` 版本钉 · `bin/start_fe.sh` 钉序 · `fastutil-core` 依赖 ·
  BE `java-udf`/`avro-scanner` pom · `doris-shade` 仓库

## ⚙️ 操作须知（**本 session 全踩过一遍**）

- **⚠️ maven build cache 会静默跳过 surefire** → `Skipping plugin execution (cached): surefire:test`，
  **BUILD SUCCESS 但 0 个测试执行**。必须 `-Dmaven.build.cache.enabled=false`，
  且**数一下 `grep -c "^\[INFO\] Tests run:"`** —— 只看 BUILD SUCCESS 会被骗。
- **⚠️ 依赖 shade 模块的模块必须跑到 `package`**：`fe-connector-paimon` 的 `HiveConf` 来自
  `fe-connector-paimon-hive-shade`。用 `test-compile`/`test` 会假报
  `package org.apache.hadoop.hive.conf does not exist`（**不是代码错**）。用 `-am package`。
  `install` 不行（`fe-type` 撞预存 `did not assign a main file` quirk）。
- **⚠️ 连接器模块路径是嵌套的**：`fe/fe-connector/fe-connector-XXX`，**且 `-am` 必填**
  （漏了报 `fe-connector:pom:${revision}` 无法解析 —— **那是没编译，不是代码错**；本 session 踩了 2 次）
- maven 必须绝对路径：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile`
- **⚠️ `git add` 用 path-whitelist，严禁 `git add -A`** —— 本 session 亲测：`git add -A fe/` 会把
  `fe/.mvn/maven.config` 等非本线程 scratch 卷进来
- 动码前先探并发（memory `concurrent-sessions-shared-worktree-hazard`）

## 🆕 待用户拍板（**别自作主张改**）

**`create()` 静音丢弃 session token**（既有 bug，非本分支引入）。
`IcebergCatalogFactory` 发 `client.credentials-provider.glue.session_token`、单测断言它，但
`ConfigurationAWSCredentialsProvider2x.create()` 只造 `AwsBasicCredentials`，**从不造 `AwsSessionCredentials`**
→ 用临时 STS 凭证的用户认证必失败。修法一处，但属**行为变更** → **必须签字**。见 `tasklist.md` T-21。
