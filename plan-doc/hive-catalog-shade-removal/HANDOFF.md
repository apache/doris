# 🤝 Session Handoff — 删除 thrift 一代 Glue/DLF ⇒ 剔除 `hive-catalog-shade`

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**。
> 完成的明细**不落这里**（在 `git log` + [`progress.md`](./progress.md) 里）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) · 清单 [`tasklist.md`](./tasklist.md)

---

# 🆕 下一个 session = **阶段 5（pom 终局）**：摘 127MB jar

## 状态：**阶段 1 / 2 / 3 / 4 已完成并提交**

**基线 HEAD** = `7ed266c677a`（`catalog-spi-11-hive`）。

| 阶段 | commit | 结果 |
|---|---|---|
| 1 — 修 iceberg 原生 Glue 凭证类 | `2cd01ada8df` | 搬出 fe-core + 改包；probe 判据 false→**true** 实测 |
| 2 — 删 Glue thrift 一代 | `e43173eca67` | **-11,245 行**；fe-core `com.amazonaws.*` **归零** |
| 3 — 删 DLF thrift 一代 | `a0f65c353a9` | **-4,095 行**；fe-core `com.aliyun.datalake` **归零** |
| 4 — fe-core 去 hive 化 | `8d6fe9f9736` `d8c121b7f21` `22461468e7c` `7ed266c677a` | **判据 4 → 0** |

## 🎯 总判据现状

```bash
# ① fe-core —— 已达标 ✅
grep -rlE '^import (org\.apache\.hadoop\.hive\.|shade\.doris\.hive\.|com\.aliyun\.datalake\.)' \
     fe/fe-core/src/main/java | wc -l          # 基线 26 → 现在 0 ✅

# ② fe-common —— 剩最后 1 处，**故意留到阶段 5**（见下方 🔴 S1）
grep -rn 'hadoop.hive' fe/fe-common/src/main/java
#   → CatalogConfigFileUtils.java:23  import org.apache.hadoop.hive.conf.HiveConf;
```

**fe-common 的门禁已解锁**：`loadHiveConfFromHiveConfDir` 现在**零调用点**（只剩声明本身，已实测）。

---

## 🔴 阶段 5 第一件事：**fe-common 与摘 jar 必须同批**（**OUTAGE 级，别排成"最后"**）

`CatalogConfigFileUtils` 服务的是**所有**外表 catalog（`ConnectionProperties.loadConfigFromFile:83`），
不只是 hive。如果 fe-core 先把 jar 从 `fe/lib` 拿掉、而 fe-common 还 import 着 `HiveConf`：
**编译照样绿**（fe-common 自己有一份 `provided` 声明）→ 但 FE 一启动，**每一个** catalog 都
`NoClassDefFoundError: org/apache/hadoop/hive/conf/HiveConf`。

⇒ fe-common 的排期是一个**窗口**（两个调用点消失之后 ✅ 已达成、摘 jar 之前），**最好同一个 commit**。

**fe-common 的 edit list**（已侦察确认，行号信 HEAD 不信本文档）：
- `CatalogConfigFileUtils.java`：删 `:23` 的 HiveConf import + `:87-102`（空行 + javadoc + 方法本体）
  ⚠️ 用 **87**-102，不是 88-102（否则留下孤儿空行）
- 同文件 javadoc 去 hive 化：`:32` / `:37` / `:39` 三处 `(e.g., Hadoop or Hive)` 之类
- ✋ **必须留** `:79-86 loadConfigurationFromHadoopConfDir` + `:43-70 loadConfigFromDir`
  （后者虽降到单调用者，**别内联**——删除提交里别做无关重构）
- `fe/fe-common/pom.xml`：删 `:87-91`（`hive-catalog-shade`, scope=provided）
- ✋ **别碰** `fe/pom.xml:801-805`（版本钉，仍被 fe-core / hms / iceberg / hudi / paimon /
  paimon-hive-shade / avro-scanner / java-udf 消费）· `fe-common/pom.xml:108-122`（hadoop-common provided）
- 验证判据用**大小写不敏感**：`grep -rniE 'org\.apache\.(hadoop\.)?hive' fe/fe-common/src/` → 空
  （原判据是大小写敏感的，抓不到自己 javadoc 里的 `HiveConf`）

---

## ⚠️ 阶段 5 的三个真实地雷（侦察挖出，**都不在原计划里**）

**1. 🔴 fe-core 掉 shade 会连带掉 14 个 compile-scope 传递依赖 —— endgame 最可能炸的地方**

shade 的 pom 有 41 个依赖，**只有 27 个是 provided**，**14 个无 scope（= compile = 传递）**：
`ivy` · `parquet-hadoop-bundle` · `guava` · `gson` · `hive-serde` · `hive-storage-api` ·
`hive-metastore` · `hive-exec` · `iceberg-hive-metastore` · `hive-standalone-metastore` ·
`hive-classification` · `paimon-hive-connector-3.1` · `paimon-hive-common` · `metastore-client-hive3`。

⇒ fe-core 掉 shade 时这些**一起从 compile classpath 消失**。**逐个 re-declare 或证明未用。**
⚠️ **别套用 fe-common 的结论**：fe-common 之所以安全，是因为它只用 guava/gson 且两者都已直接声明；
**fe-core 很可能真的在用其中几个**。

**2. 🔴 fe-core 的测试侧还有 1 个 hive importer —— 这是个真正的岔路口**

`fe/fe-core/src/test/java/.../metastore/HMSIntegrationTest.java`（import `HiveConf` +
`HiveMetaStoreClient` + 4 个 metastore api 类）。`provided` scope **在本模块自己的 test classpath 上**
⇒ 删 pom 那 4 行会**炸 fe-core 测试编译**。两条路，**必须显式选**：
- **降级为 test scope** —— 测试能编，但 **127MB 仍被拉进来**（收益归零，只是换个 scope）
- **迁走 / 删掉该测试** —— 包才真正消失
（`HMSPropertiesTest` 已随阶段 4 删除，所以只剩这 1 个。）

**3. 🔴 摘 jar 后 `orc-core` 会变孤儿 —— 必须重跑 ORC 分析**

阶段 4 删 `hudi-hadoop-mr` 时证明了"ORC 没事"，**但那个结论条件于 shade 还在**
（shade 里逐字节相同地捆了全部 122 个 `hive-storage-api` 类）。摘掉 shade 后：
`orc-core-1.8.4` 仍经 `hudi-common` 进来，而 `hudi-common` 的 `hive-storage-api` 在
`fe/pom.xml:1507-1510` 被排除 → **结构上不可满足**。今天无害**纯粹因为没人加载 ORC**。
**⛔ 别把阶段 4 的"ORC 没事"当先例引用。**

---

## 📋 阶段 5 剩余清单（`tasklist.md` T-60~T-66）

- `fe/fe-core/pom.xml:437-440` —— `hive-catalog-shade` 本体（**主目标**）
- `fe/fe-core/pom.xml:217-223` —— `commons-lang` 2.6 runtime
  ⚠️ 注释原文写着"hive-catalog-shade's HiveConf needs org.apache.commons.lang.StringUtils"，
  **但项目记忆 `catalog-spi-be-java-ext-shared-classpath` 说它对 `preload-extensions` 另有承重** → 别顺手删
- `fe/fe-core/pom.xml:926-967` —— `bundle-fastutil-into-doris-fe` shade execution
  ✋ `fastutil-core` 依赖本身（`:766-774`）**要留**，在其上留注释记录"为什么这里曾需要 shade 覆盖"
- `fe/fe-core/pom.xml:776-782` —— `<repositories>` 的 `central`（注释写着 for hive-catalog-shade）
- `aws-java-sdk-glue` / `aws-java-sdk-sts` —— 阶段 2/3 后零引用；`aws-java-sdk-core` 是否还需要按结论定
- LICENSE / NOTICE 复核（shade 的 bundled-third-party 足迹正是那两个文件记录的）
  📌 `dist/LICENSE-dist.txt:650` / `NOTICE-dist.txt:4200` 是手工维护且**早已过期**，
  `license-maven-plugin` 只在 `release` profile 跑 `add-third-party` → **不 gate 构建**
- **不许动**复核：`fe/pom.xml:801-805` ✋ · `start_fe.sh` 钉序 ✋ · `fastutil-core` ✋ ·
  BE `java-udf`/`avro-scanner` ✋ · `doris-shade` ✋

---

## 🚫 别做的事（前四阶段趟平的路）

- **别按关键字 grep 一把梭删** —— 保留路径同名：`iceberg.catalog.type=glue`（阶段 1 刚修好）·
  `paimon.catalog.type=rest` + dlf token（DLF 2.0）· `fe-filesystem-oss` 的 `dlf.*` 别名（OSS 凭证用）
- **别碰前四阶段的成果**：`connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x` ·
  `HmsClientConfig.removedMetastoreTypeError` + 两个调用点 · `DatasourcePrintableMap` 的 4 个 dlf 脱敏 key ·
  `HmsClientConfigRemovedTypeTest` · 两个插件的 `addConfResources` + `resolveHadoopConfigDir`
- 别信「5 项依赖全零引用」：**`kryo-shaded` 绝不可删**（`WorkloadSchedPolicy.java:32,287,298` 经 minlog
  传递依赖真实调用）；`avro` 删声明但 jar 删不掉（三方传递）
- 别拿 `doris-shade/.../target/hive-catalog-shade-3.1.2-SNAPSHOT.jar` 做验证 —— 它**已经**改名了 fastutil，
  会误判「fastutil hack 已死」。**构建实际解析的是 3.1.1**（`~/.m2/.../3.1.1/`）
- 别为「删得动」把 HiveConf 逻辑挪进 fe-core util（**铁律 B**，遇到就停手交 review）
- **别做守门测试 / paimon-on-DLF / 降级档** —— 用户已明确「不做」

## ⚙️ 操作须知（**前四阶段全踩过**）

- **⚠️ maven build cache 会静默跳过 surefire** → `Skipping plugin execution (cached): surefire:test`，
  **BUILD SUCCESS 但 0 个测试执行**。必须 `-Dmaven.build.cache.enabled=false`，
  且**数一下 `grep -c "^\[INFO\] Tests run:"`** —— 只看 BUILD SUCCESS 会被骗。
- **⚠️ `-am` 必填**：漏了报 `org.apache.doris:fe:pom:${revision}` 无法解析 —— **那是没编译，不是代码错**。
  本 session 又踩了 1 次（记忆里明明写着）。
- **⚠️ 依赖 shade 模块的模块必须跑到 `package`**：`fe-connector-paimon` 的 `HiveConf` 来自
  `fe-connector-paimon-hive-shade`。用 `test-compile`/`test` 会假报
  `package org.apache.hadoop.hive.conf does not exist`（**不是代码错**）。用 `-am package`。
  `install` 不行（`fe-type` 撞预存 `did not assign a main file` quirk）。
- **⚠️ 连接器模块路径是嵌套的**：`fe/fe-connector/fe-connector-XXX`
- maven 必须绝对路径：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile`
- **⚠️ 变异验证要确认红在断言上**：本 session 第一次变异验证红在 **checkstyle**（`LeftCurly`）而非断言，
  **测试根本没跑**，结论作废后重做。变异代码也要合 checkstyle。
- **⚠️ `git add` 用 path-whitelist，严禁 `git add -A`**
- **动码前先探并发** —— 本 session 实测有并发 session 在同一工作树提交（`1ae046f82cc`，iceberg scan 修复，
  与本线工作面不相交）。小步快提交。

## 🆕 待用户拍板（**别自作主张改**）

**`create()` 静音丢弃 session token**（既有 bug，非本分支引入）。
`IcebergCatalogFactory` 发 `client.credentials-provider.glue.session_token`、单测断言它，但
`ConfigurationAWSCredentialsProvider2x.create()` 只造 `AwsBasicCredentials`，**从不造 `AwsSessionCredentials`**
→ 用临时 STS 凭证的用户认证必失败。修法一处，但属**行为变更** → **必须签字**。见 `tasklist.md` T-21。
