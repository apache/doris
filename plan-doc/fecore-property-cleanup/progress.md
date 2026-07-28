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
