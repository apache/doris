# 🤝 Session Handoff — 删除 thrift 一代 Glue/DLF ⇒ 剔除 `hive-catalog-shade`

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**。
> 完成的明细**不落这里**（在 `git log` + [`progress.md`](./progress.md) 里）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) · 清单 [`tasklist.md`](./tasklist.md)

---

# 🆕 下一个 session = **阶段 6（用户可见面 + 守门）**：T-70 ~ T-74

> T-21（Glue session token）**已完成**（用户 2026-07-15 指定优先处理并签字）。

## 状态：**阶段 1–5 全部完成。127MB 的 jar 已经出了 `fe/lib`。**

**基线 HEAD** = 见 `git log`（`catalog-spi-11-hive`）。

| 阶段 | commit | 结果 |
|---|---|---|
| 1 — 修 iceberg 原生 Glue 凭证类 | `2cd01ada8df` | 搬出 fe-core + 改包；probe 判据 false→**true** 实测 |
| 2 — 删 Glue thrift 一代 | `e43173eca67` | **-11,245 行**；fe-core `com.amazonaws.*` **归零** |
| 3 — 删 DLF thrift 一代 | `a0f65c353a9` | **-4,095 行**；fe-core `com.aliyun.datalake` **归零** |
| 4 — fe-core 去 hive 化 | `8d6fe9f9736` `d8c121b7f21` `22461468e7c` `7ed266c677a` | **判据 26 → 0** |
| 5 — pom 终局 | `0c35090a4f3` + 卫星项提交 | **jar 出 `fe/lib`**；fe-core 净少 38 个 jar |

## 🎯 总判据 —— **全部达成 ✅**

```bash
grep -rlE '^import (org\.apache\.hadoop\.hive\.|shade\.doris\.hive\.|com\.aliyun\.datalake\.)' \
     fe/fe-core/src/main/java | wc -l              # 26 → 0 ✅（test 侧亦为 0）
grep -rniE 'org\.apache\.(hadoop\.)?hive' fe/fe-common/src/   # → 空 ✅
mvn -o -f <abs>/fe/pom.xml -pl fe-core -am dependency:tree \
    -Dincludes=org.apache.doris:hive-catalog-shade            # fe-core 段为空 ✅
```

shade 的真依赖只剩 **4 个正当消费者**：`fe-connector-hms` · `fe-connector-iceberg` ·
`avro-scanner` · `java-udf`。`fe/pom.xml` 的版本钉**必须留**。

---

## 📋 阶段 6 清单（`tasklist.md` T-70 ~ T-74）

- **T-70 regression-test 清理**（只删 thrift 一代的，**误删比漏删严重**）：
  - `aws_iam_role_p0/test_catalog_with_role.groovy:82-90`（`hive.metastore.type=glue` 块）+ 死变量 `:49`
    ✋ **`:73-81` 的 `iceberg.catalog.type="glue"` 保留**（那条路径阶段 1 刚修好）
  - `aws_iam_role_p0/test_catalog_instance_profile.groovy:67-95`（3 块 hive-on-glue）+ 死变量 `:26-27`
    ⚠️ `:22-24` 的**文件级 guard 钉在 glue 专有 conf key 上** —— 删 glue 块后须改钉 iceberg 的 key，
    否则存活的 iceberg 分支**静默永不运行**
  - `external_table_p2/refactor_catalog_param/iceberg_and_hive_on_glue.groovy:369-372`（**已是死代码**）
    ✋ 保留 `:367`
  - ⚠️ **假阳性别碰**：`external_table_p2/hive/test_external_catalog_glue_table.groovy`（名字是陷阱，
    实为普通 HMS 且 `:20-24` 硬关）· `test_iceberg_predicate_conversion.groovy`（glue 只是列名）·
    `test_s3tables_glue_*`（iceberg REST + glue signing）
- **T-71 文档 / 报错文案**：`hive.metastore.type=glue|dlf`、`iceberg.catalog.type=dlf`、
  `paimon.catalog.type=dlf` 的报错要清楚说明**已移除**（现在只说 `Unknown type`，loud 但没说「已移除」）；
  仓库内 .md 同步。
- **T-72 静态守门**：判据两条 grep · `-pl fe-core -am test-compile`（**漏 `-am` = 假错**）·
  全连接器 `-am package` · checkstyle · `tools/check-connector-imports.sh` ·
  `unzip -l fe/fe-core/target/doris-fe-lib.zip | grep -i hive` 记录删前/删后差异
- **T-73 e2e**：① 普通 HMS catalog 读写 ② iceberg 原生 Glue + AK/SK ③ Ranger hive 鉴权
  🔴 **本阶段新增必跑项**：④ **FE 起得来 + 各类缓存正常**（Caffeine 由 2.x 翻到 3.2.3，见下）
  ⑤ **OBS/BOS 访问**（commons-lang 结论的反面验证）
  ⑥ **glue catalog + 临时 STS 凭证**（T-21 的验收；需真实 STS 凭证，单测覆盖不到）
- **T-74 收尾**：`progress.md` 结项 + `../decisions-log.md` 补 D-NNN + PR（base = `branch-catalog-spi`，squash）

---

## 🔴 阶段 6 必须知道的三件事（阶段 5 的产物，**别当成无关背景**）

**1. Caffeine 从 2.x 翻到了 3.2.3 —— 这是本分支唯一的运行期行为变更，e2e 必须覆盖。**
那个 jar 捆着 Caffeine 2.x，且 `start_fe.sh` **倒序**拼 classpath 使它排在 `caffeine-3.2.3.jar`
**之前** ⇒ **FE 一直跑的是 hive jar 里那份 2.x**，pom 声明的 3.2.3 从未生效。摘 jar 后现实与声明对齐。
- 已做的证明：自写字节码链接检查器（沿继承链解析）扫 `output/fe/lib` 全部 caffeine 消费者 → **零缺失**；
  `CacheBulkLoaderTest` 变异验证红在断言上。
- **未做的**：真启动 FE 验缓存（`MetaCache` / `StatisticsCache` / `FileSystemCache` /
  `ExternalRowCountCache`）—— **归 T-73**。

**2. `commons-lang` 2.6 必须留着 —— 别再有人"顺手删"它。**
原 tasklist 写着删它，是**错的**（已翻转）。它与 shade 无关：真正需要它的是
`hadoop-huaweicloud` 的 `OBSFileSystem`（**反射加载**）与 `bce-java-sdk`。
`OBSProperties` 用 `Class.forName(..., initialize=false)` 探测 ⇒ **删了编译绿、探测仍成功、
S3A 降级不触发**，等 Hadoop 实例化才炸 ⇒ **OBS 静默崩**。pom 注释已改正。

**3. `fe/lib` 里仍有两份 fastutil（良性，已实测）。**
`fastutil-core-8.5.18` 与经 `fe-common → trino-main` 进来的完整 `fastutil-8.5.12` 并存。
二者 API 相同（只有 jar 里那份 2013 年的 6.5.6 缺 `computeIfAbsent`），谁赢都正确 ⇒ 曾经的
shade execution 已删。**别把这当成新 bug 去"修"。**

---

## ✅ 全量 UT 已绿（阶段 5 收尾，独占跑）

| 模块 | 结果 |
|---|---|
| 其余 15 个模块 | 776 用例 **全绿** |
| **fe-core** | **5197 用例 · Failures 0 · Errors 0 · Skipped 35** |

校验：失败标记 0 · surefire 被 build cache 跳过 **0 次**（真跑）· 实跑 877 个测试类。

**两个既有失败已在本阶段一并修掉**（`e2fa286b0a5`，均非本分支引入）：
`AuthenticationPluginManagerTest`（断言一个全仓从未实现的 `oidc` 插件）·
`PluginDrivenMvccExternalTableTest`（另一 session 改分区值契约时漏更新被改类自己的测试）。

## 🔴 跑 UT 的两条铁律（本 session 血的教训）

1. **绝不可在全量测试跑的同时另起 maven** —— 它们写同一个 `target/classes`。本 session 这么干过，
   结果 **22 个 `NoClassDefFoundError` 全指向 Doris 自己的类**（class 明明在 target 里），
   失败散落在互不相干的 nereids 类上；**更阴的是覆盖面被腰斩**（2845 vs 干净跑的 5197）
   ⇒ 连「只有这几个类失败」都不可信。**判定失败前先确认无并发构建。**
2. **必须 `-Dmaven.test.failure.ignore=true`**：否则某个前置模块一红，反应堆就在**抵达 fe-core 之前中止**，
   「跑了全量」是假象。跑完只看各模块的 `Results:` 段。

## 🚫 别做的事

- **别删 `commons-lang`**（见上）· 别"修"两份 fastutil（见上）
- **别按关键字 grep 一把梭删** —— 保留路径同名：`iceberg.catalog.type=glue`（阶段 1 刚修好）·
  `paimon.catalog.type=rest` + dlf token（DLF 2.0）· `fe-filesystem-oss` 的 `dlf.*` 别名（OSS 凭证用）
- **别碰前五阶段的成果**：`connector.iceberg.glue.ConfigurationAWSCredentialsProvider2x` ·
  `HmsClientConfig.removedMetastoreTypeError` + 两个调用点 · `DatasourcePrintableMap` 的 4 个 dlf 脱敏 key ·
  `HmsClientConfigRemovedTypeTest` · 两个插件的 `addConfResources` + `resolveHadoopConfigDir`
- **别删 `fe/pom.xml` 的 shade 版本钉** —— 4 个正当消费者还在用
- 别信「5 项依赖全零引用」：**`kryo-shaded` 绝不可删**（`WorkloadSchedPolicy.java` 经 minlog 真实调用）
- **别做守门测试 / paimon-on-DLF / 降级档** —— 用户已明确「不做」

## ⚙️ 操作须知（**前五阶段全踩过**）

- **⚠️ maven build cache 会静默跳过 surefire** → `Skipping plugin execution (cached): surefire:test`，
  **BUILD SUCCESS 但 0 个测试执行**。必须 `-Dmaven.build.cache.enabled=false`，
  且**数 `grep -c "^\[INFO\] Running org.apache.doris"`**。
- **⚠️ `-am` 必填**：漏了报 `org.apache.doris:fe:pom:${revision}` 无法解析 —— **那是没编译，不是代码错**。
- **⚠️ `git add` 遇到已 `git rm` 的路径会中断整条命令** → 该次 commit 只包含先前暂存的内容。
  **commit 后必看 `git show --stat` 的文件数**（本 session 踩过，靠 `--amend` 补回）。
- **⚠️ 依赖 shade 模块的模块必须跑到 `package`**：`fe-connector-paimon` 的 `HiveConf` 来自
  `fe-connector-paimon-hive-shade`。用 `test-compile`/`test` 会假报
  `package org.apache.hadoop.hive.conf does not exist`（**不是代码错**）。`install` 不行（`fe-type` quirk）。
- **⚠️ 连接器模块路径是嵌套的**：`fe/fe-connector/fe-connector-XXX`；认证模块同理：
  `fe/fe-authentication/fe-authentication-handler`（`-pl fe-authentication-handler` 会报"不在 reactor"）
- maven 必须绝对路径：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile`
- **⚠️ 变异验证要确认红在断言上**（不是 checkstyle/编译），变异代码也要合 checkstyle
- **⚠️ `git add` 用 path-whitelist，严禁 `git add -A`**（工作树有大量非本线程的历史 scratch）
- **动码前先探并发**（`git log`/`status` + maven 进程 + 近 90s mtime）

---

# ✅ T-21 — Glue session token 已修（用户签字，行为变更）

**改完之后**：用临时 STS 凭证的 glue catalog 从「必然认证失败」变成「能用」。

🔴 **侦察证伪了原计划的「修法就一处」——实为两个模块、两处独立缺陷**（只修一处仍不可用）：
- **A（iceberg 插件）** `ConfigurationAWSCredentialsProvider2x.create()` 只读 ak/sk，token 就在收到的 map 里
  没人读（iceberg 剥掉 `client.credentials-provider.` 前缀后 key = `glue.session_token`）。
- **B（`fe-filesystem`）** `S3FileSystemProperties` 别名表不对称：accessKey/secretKey 收了 glue 的三个别名，
  **sessionToken 一个都没有** ⇒ token 到不了 S3 store ⇒ 发出 `s3.session-token=""` ⇒ iceberg 走空 token 分支。

**两条给后人的经验**：
1. **判空必须 `isNotBlank` 而非 `!= null`**：`AwsSessionCredentials.create(ak,sk,"")` **不报错**，
   会造出带空 token 的凭证，等到 AWS 才炸出莫名其妙的 4xx。
2. **只断言"发射"抓不到这类 bug**：既有用例断言了 `client.credentials-provider.glue.session_token` 被发出，
   却挡不住读取侧忽略它。新加的是**探针式**测试——驱动真的
   `new AwsClientProperties(opts).credentialsProvider(null,null,null)`，把
   「发射 → 剥前缀 → 反射 → 读取」串进一个测试。两处均已变异验证（红在断言上）。

📌 **e2e（归 T-73）**：真临时凭证跑通 glue catalog 需要真实 STS，本地无法覆盖 —— 保留为待验项。

## 📌 侦察挖出、**本轮有意不动**的独立问题（阶段 6 可择机记进 decisions-log）

- `aws-java-sdk-dynamodb` / `aws-java-sdk-logs` 在 fe-core **零引用**
  （logs 的唯一理由「ranger audit 需要」在 Ranger 2.8.0 已过期：audit 模块换成了 `ranger-audit-core`，
  其 pom 零 AWS 依赖）—— 与本任务无关，属独立清理。
- `fe/pom.xml` 的 `snapshots` 仓库仍挂着 `<!--todo waiting hive-catalog release-->` 的过期注释
  （仓库本身可能仍服务其它 snapshot 依赖，**别顺手删仓库**）。
- `dist/LICENSE-dist.txt` / `NOTICE-dist.txt` 手工维护且**早已过期**；`license-maven-plugin` 只在
  `release` profile 跑 `add-third-party` → **不 gate 构建**。本轮新增的 `com.tdunning:json:1.8`
  **早已在 LICENSE-dist:981-982 记着**（因为那个 jar 一直在捆它），故无需改动。
