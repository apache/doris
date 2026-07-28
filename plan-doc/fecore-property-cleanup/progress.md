# 📜 进度记录（append-only）

> **只追加，不覆盖**。新条目写在**底部**（时间正序）。
> 每条格式：日期 /（commit）/ 做了什么 / 结论 / 踩了什么坑。
> 「下一步做什么」不写这里（在 [`HANDOFF.md`](./HANDOFF.md)）；「勾到哪了」不写这里（在 [`tasklist.md`](./tasklist.md)）。

---

## 2026-07-28 — 任务空间建立 + 调研完成（FPC-00 / FPC-01a）

**基线**：`3468d905eb3`，分支 `catalog-spi-review-21`。**代码零改动。**

### 起因

用户提出独立调研任务：fe-core `datasource/property/` 下的 `common` 和 `metastore` 两个目录，
能不能删掉、或者迁进 `fe/fe-connector`？给出方案或给出不能的理由。
背景原则：元数据服务（Glue 之类）归 `fe-connector-metastore-api`，存储（hdfs/s3 之类）归 `fe-filesystem`。
用户明确要求「不要局限于当前逻辑，必要时可增删任意模块的接口」。

### 做法

Workflow 编排：8 路并行侦察（产出 172 条 finding）→ 3 路独立设计
（minimal / architectural / risk 三种视角，**结论并不一致**）→ 6 项对抗验证（每项被推翻后再走第二轮独立复核）
→ 综合。之后由本 session 对**承重结论逐条亲自复核代码**（不只采信 agent）。

三份设计的裁决分歧本身是有信息量的：

| 视角 | `common/` | `metastore/` |
|---|---|---|
| minimal | KEEP | DELETE |
| architectural | DELETE_AND_REDIRECT | DELETE |
| risk | KEEP | DELETE_AND_REDIRECT |

⇒ `metastore/` **三路一致要删**；`common/` **2:1 主张留**，且主张删的那路（architectural）
自己也承认需要用户先拍两个板。最终采纳「留」。

### 结论

- **`metastore/`（4 文件 333 行）→ 整体删除**，连带孤儿 `ConnectionProperties.java`（140 行）。
  运行期两道门都不可达；包里本来就零解析（唯一注册的 `TrinoConnectorPropertiesFactory` 连
  `initNormalizeAndCheckProps()` 都不调）；持久化无坑；接班人
  （`fe-connector-metastore-api` 的 `MetaStoreProperties` + `MetaStoreProviders.bind` +
  `Connector.deriveStorageProperties`）早已在生产路径上跑着 ⇒ **没有任何东西需要搬**。
- **`common/`（2 文件 237 行）→ 留在 fe-core，只砍死的一半（~146 行）**。
  它服务的是**内部存储**（冷存 StoragePolicy / 云上 StorageVault / TVF / backup / export），
  零个 fe-connector、零个 fe-filesystem 模块 import 它 —— **根本不在这次迁移的射程内**。

### 🔴 踩坑记录（最有价值的部分）

**坑 1 —— 「重复造轮子」的误判，被对抗验证两轮推翻。**
我的初判是：`common/` 和 `fe-filesystem-s3-base` 的
`S3CredentialsProviderType`/`S3CredentialsProviderFactory` 是同一套逻辑的两份副本，
应该「删除 + 把消费者指向现成实现」。**错。** 两条活的行为差异：
① 发给 hadoop 的凭证串会多出 `ProfileCredentialsProvider`；
② 模式串接受面放宽（空串 / `ENVIRONMENT` / `WEB_IDENTITY_TOKEN_FILE` 从抛异常变成接受，
而 `StorageAdapter.java:169-170` 注释明说这个严格是**故意的**）。
**且全仓没有任何测试钉住那个串** ⇒ 换掉会绿着上线一个回归。
**通用教训**：「两个类长得像 ⇒ 可以合并」是个高频误判；判定等价必须**逐字段比对输出**
（尤其是会发到 BE / 写进 hadoop conf 的**字符串**），而不是比对结构。
顺带一提，仓库里其实有**四份**这套逻辑（第三份在 `fe-connector-iceberg`，第四份在
`fe-connector-metastore-iceberg`），而且第三份的语义又和前两份都不同（未知模式回退 DEFAULT 而非抛）。

**坑 2 —— 对抗验证抓到「两份候选设计会挂在 checkstyle 上」。**
`fe/pom.xml:177-183` 把 checkstyle `check` 绑到**每个模块的 validate 阶段**，
`checkstyle.xml:167` 开着 `UnusedImports` 且 `severity=error` ⇒
**漏删一个 import，不带 flag 的 `mvn test` 会在跑任何测试之前就中止**。
三份设计里两份的 import 清单是错的（漏了 `CatalogProperty.java:25` 的 `Preconditions`；
另有一份把 `StorageAdapter` 的 import 错记到 `AwsCredentialsProviderFactory` 头上）。
`tasklist.md` 里是**修正后**的精确清单。

**坑 3 —— 调研报告自己给的 HIGH 风险缓解方案，复核后是无效的。**
报告主张「把 `setPluginDerivedStorageDefaultsSupplier` 语句提前到造连接器之前」来关闭
null-supplier 窗口。复核发现**修不干净**：提前之后 lambda 捕获的 `connector` 字段仍是旧值/null
（`connector = newConnector` 发生在 `createConnectorFromProperties()` 返回**之后**），
窗口内照样得到 `emptyMap`。
更进一步，我逐个复核了四个连接器的构造函数（paimon `:151-166` / iceberg `:215+` / hive / hudi），
**没有任何一个在构造期碰 storage**（都只传惰性方法引用 `this::pluginAuthenticator`），
`validateProperties` 还显式传 `Collections.emptyMap()` ⇒ **这个窗口今天根本不可达**。
所以它不是 blocker，而是「删完之后要 fail-loud 还是 fail-silent」的选择题 → 收敛成 **OD-1**。
**通用教训**：**调研报告（哪怕是自己多轮对抗产出的）的「风险 + 缓解」也要复核**，
高危标注可能是理论可达性而非真实可达性，配的解法可能治标不治本。

### 产出

本任务空间 `plan-doc/fecore-property-cleanup/`：
`README.md` · `design.md` · `tasklist.md` · `open-decisions.md` · `HANDOFF.md` · `progress.md`。

### 卡点

**OD-1 待用户拍板**（null-supplier 分支 fail-loud vs fail-silent），阻塞 FPC-03。

---

## 2026-07-28（二）— FPC-01 + FPC-03 落地

**用户指示**：文档空间单独 commit（`938d38c7425`），然后直接开始编码。

### FPC-01 —— OD-1 未获表态，按推荐值 A 执行

用户说「直接开始编码」但未就 OD-1 表态。按文档推荐值 **A（fail-loud，`throw`）** 落地，
理由是它**精确保留今天的行为**（今天走到这里就是抛），且**翻成 B 只需改一行 + 删一个用例**
——即使追认时被推翻，代价也极小。已在 `open-decisions.md` 标为「⏳ 待用户追认」。

### FPC-03 —— 主删除完成

**删 5 文件**（`metastore/` 四个 + `ConnectionProperties.java`）**共 473 行**，
`CatalogProperty` 净减 ~45 行。**零 pom / 零连接器业务代码 / 零 fe-filesystem 改动。**

`resolveDerivedStorageDefaults()` 的 null 分支落地为
`throw new IllegalStateException("Storage properties were accessed before the connector-derived
storage defaults were wired ...")`。

**顺带清掉 3 处悬空注释引用**：`StorageAdapter`（javadoc 散文，非编译依赖）、
paimon `TcclPinningConnectorContext:49`、`DatasourcePrintableMap:69`
（最后这处是全仓 grep 才揪出来的，初次清单里漏了）。

**测试改动**（守 Rule 9）：`CatalogPropertyPluginStorageDerivationTest`
- 类 javadoc + 两处 MUTATION 注释**改钉到仍然存在的变异上**（原来钉的
  「改回走 `getMetastoreProperties()`」已无法表达）
- **新增**第 4 个用例 `unwiredSupplierFailsLoudInsteadOfDerivingNothing`，钉住 fail-loud 不变量

### 验证（全部实跑，非预期）

| 项 | 结果 |
|---|---|
| 全仓残留 grep（`fe`/`regression-test`/`tools`/`gensrc`） | **0** |
| 全反应堆 `clean test-compile -Dcheckstyle.skip=true`（含测试源） | **BUILD SUCCESS**（2:16） |
| `-pl fe-core checkstyle:check` | **0 violations** ⇒ import 修剪精确 |
| fe-core 定向单测（含 3 个 Gson 回放兼容测试） | **95 run / 0 fail** |
| `-pl fe-connector/fe-connector-api -am test`（录制基线） | **110 run / 0 fail**；`connector-metadata-methods.txt` **未被改动**（与预判一致） |
| 变异验证（`throw` → `return emptyMap()`） | 新用例**变红**，其余三例不受影响；改回**复绿** |

### 🔴 踩坑记录

**坑 4 —— 我自己写进 tasklist 的验证命令有两处是错的，实跑才发现。**
① `mvn -pl fe-core test` **漏了 `-am`** → 兄弟模块的 `${revision}` 解析不了，报的是
「Could not resolve dependencies / fe-authentication:pom:${revision}」这种**看起来像真错的假错**
（本仓库 `hive-catalog-shade-removal` 的 T-72 早就记过「漏 `-am` = 假错」，我还是踩了）。
② surefire 2.22.2 认的是 **`-DfailIfNoTests=false`**，我写的 `-DfailIfNoSpecifiedTests=false` 无效，
导致 `-am` 带起来的 `fe-foundation` 因「No tests were executed」直接 FAILURE。
两条都已修进 `tasklist.md` 的「四条纪律」。
**通用教训**：**文档里的验证命令在实跑过之前都只是草稿**，别当成已验证的资产传给下一个 session。

### 状态

- `metastore/` 目录已不存在；`property/` 下只剩 `common` / `constants` / `fileformat`。
- 下一步：**FPC-02**（删 AWS 死构造臂）——先按 OD-2 grep 一次上游 master。

---

## 2026-07-28（三）— FPC-02 停手：OD-2 前置条件不成立

按 HANDOFF 的指示，动 FPC-02 前先执行 OD-2 的前置检查「grep 一次上游 master」。

**结果与预设相反。** `upstream-apache/master` @ `2faf819fa89` 上
`StorageAdapter.getAwsCredentialsProvider()` **有两个活调用者**：

```
datasource/connectivity/AbstractS3CompatibleConnectivityTester.java:71   adapter.getAwsCredentialsProvider()
datasource/property/common/IcebergAwsClientCredentialsProperties.java:84 s3Adapter.getAwsCredentialsProvider()
```

本分支之所以判它「零调用者」，是因为迁移**已经把这两个消费者连同整个
`datasource/connectivity/` 包删光了** —— 即 **上游活、本分支死**。

**为什么这翻转了结论**：`StorageAdapter.java` 本身两边都在，会走 rebase 三方合并。
今天上游改动该区域能干净合入；删掉方法后，这些改动就变成**每次 rebase 的人工冲突 hunk**，
而收益只是 146 行本就不执行的代码 —— 对一个**定期 rebase 到 force-push 上游**的分支，
这笔账不划算。

⇒ **已停手，未执行 FPC-02**，`open-decisions.md` OD-2 推荐值改为 **B（不做）**，等用户拍板。
若用户判断 `StorageAdapter` 本身后续要整体退役，则冲突面是虚的，A（删）更好。

**通用教训（补强坑 1）**：判「死代码」必须**声明口径是哪个 ref**。
「本分支零调用者」和「上游零调用者」是两件事；对**长期 rebase 型分支**，
删除上游仍在用的代码是在**给自己制造持续的合并债**，不是在清理。

---

## 2026-07-28（四）— OD-1/OD-2 拍板 + FPC-02 落地

**用户拍板**：OD-1 = **抛异常**（与已落地实现一致，无需改码）；OD-2 = **直接删**（**推翻我的推荐 B**）。

OD-2 被推翻是合理的：我的推荐建立在「为零功能收益承担 rebase 摩擦不划算」上，
但**该文件后续是否整体退役**只有 owner 知道，这正是我停手问的原因。既然拍板要删，
代价（上游改动该区域时的 modify/delete 冲突）已明确记入 OD-2，**处置方式=保留删除**。

### FPC-02 —— 实删 159 行（大于文档估的 ~146）

- `StorageAdapter`：`getAwsCredentialsProvider()` + `staticAwsCredentialsProvider()` +
  `s3AwsCredentialsProvider()`，共 **85 行**（含 8 个孤儿 import）
- `AwsCredentialsProviderFactory`：`createV2` + `createDefaultV2` + 单参 `getV2ClassName`，
  共 **74 行**（含 2 个孤儿 import）
- 另清 3 处点名已删方法的连接器注释

**没有照抄文档的 import 清单**，而是逐符号统计「正文使用数」再判孤儿——结果与文档一致
（`StorageAdapter` 8 个、工厂 2 个），且确认了两个**易误删项**：
`InstanceProfileCredentialsProvider` 尚有 1 处正文使用、`Config` 尚有 12 处。

### 验证（全部实跑）

| 项 | 结果 |
|---|---|
| 残留 grep `getAwsCredentialsProvider()\|createV2\|createDefaultV2` | 仅剩 iceberg 测试里的 `createV2Unpartitioned`（表格式 v2 同名**误报**，无关） |
| 全反应堆 `clean test-compile -Dcheckstyle.skip=true` | **BUILD SUCCESS** |
| `-pl fe-core checkstyle:check` | **0 violations** |
| fe-core 存储适配定向单测 | **52 run / 0 fail / 1 skipped** |

`1 skipped` = `LocationPathTest:115` 的 `@Disabled("not support in master")`，**既有**，
该文件本次未改动。

### 🔴 踩坑记录

**坑 5 —— `-am test` 对依赖链经过 shade 模块的连接器跑不通。**
想跑 `-pl fe-connector/fe-connector-iceberg -am test` 验证 iceberg 侧的注释改动，
结果在 `fe-connector-hms` 炸「package org.apache.hadoop.hive.metastore.api does not exist」。
**没有靠推断归因**，而是 `git stash push -u -- fe/` 回到干净 HEAD 跑同一条命令
—— **一模一样地失败** ⇒ 既有 reactor 怪癖（shaded jar 只在 `package` 阶段产出，
`test` 阶段够不着），与本次改动无关。已记为第 5 条纪律。
**通用教训**：`-am` 不是万能的；**归因失败要用 stash 复现，不要用「我没碰那个模块」的推理**
——推理会漏掉传递性影响，stash 不会。
（对比：`fe-connector-api` 不在 shade 链上，`-am test` 正常，早先跑出 110/0。）

### 状态

`property/` 下 `common` 只剩真正在用的部分：`AwsCredentialsProviderMode` 全保留，
`AwsCredentialsProviderFactory` 只剩 `getV2ClassName(mode, boolean)` + 两个 env 探针
（喂 `StorageAdapter:645/:654` 那条**活的** hadoop/BE 串）。
**本任务空间的核心工作（FPC-01/02/03）已全部完成**，只剩可选的 FPC-04。
