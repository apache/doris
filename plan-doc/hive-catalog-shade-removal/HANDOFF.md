# 🤝 Session Handoff — 剔除 `hive-catalog-shade`

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**。
> 完成的明细**不落这里**（在 `git log` + [`progress.md`](./progress.md) 里）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) · 清单 [`tasklist.md`](./tasklist.md)

---

# 🆕 下一个 session = **阶段 0（决策）+ 阶段 1（独立清理，可立即开工）**

## 状态：调研完成，**尚未动任何代码**

**基线 HEAD** = `669602f079d`（`catalog-spi-11-hive`）。本任务**零代码改动**，只产出了本文档空间。

## 🚩 必须先知道的一个反直觉结论

**`fe/fe-core/pom.xml:437-440` 不是冗余，直接删会 `javac` 失败。**
用户最初的假设（「都迁到插件了，这个 shade 应该没用了」）被 3 路对抗验证**全部推翻**：

- fe-core `src/main` 还有 **26 个文件** import hive 类（38 个 vendored Glue 文件 + 1 个 DLF 客户端 +
  5 个 HiveConf 属性类 + `DefaultConnectorContext` + `RangerHiveAuditHandler`）
- `fe-common` 还有 1 处 `HiveConf`，而且是 **`provided`** → 删了 fe-core 的依赖后它**照样编译通过，运行时炸**，
  且它在**每个外表 catalog** 的路径上 → 炸的不只是 hive
- shade jar 里 **`AWSCatalogMetastoreClient` 有 0 个条目** —— 唯一定义就是 fe-core 那 38 个 vendored 文件

**pom 那 4 行只是症状；真正的工作量是把残留源码搬回插件。** 详见 `design.md §3`。

## ▶️ 下一个 session 的第一件事（二选一，看用户）

### 路线 A（推荐）：先做**阶段 1 独立清理** —— 便宜、无风险、不等任何决策

第一条命令：
```bash
cd /mnt/disk1/yy/git/wt-catalog-spi
# HCS-10：逐条验证 agent 报告的「fe-core 源码零引用」是否属实（含反射/字符串用法，不能只看 import）
for a in hudi-hadoop-mr iceberg-aws s3-tables-catalog-for-iceberg avro kryo-shaded; do echo "== $a =="; done
```
然后按 `tasklist.md` **HCS-10 → HCS-13** 走。重点是 **HCS-11 删 `hudi-hadoop-mr`**
（`fe/fe-core/pom.xml:600-604`）—— 它是 `hive-storage-api:2.8.1` 混进 `fe/lib` 的唯一来源（风险 R-4）。
⚠️ `hudi-common` **要留**（`HttpProperties` / `StatisticsCache` 各一处 import）。

### 路线 B：先推**阶段 0 决策**（`tasklist.md` HCS-01~03）

需要用户拍板 3 件事（背景+选项见 `design.md §5`，用中文讲清楚再问）：
- **D-1** 38 个 vendored Glue 文件搬去哪？（推荐：搬进 `fe-connector-hms`）
- **D-2** paimon 的 DLF 客户端怎么给？（推荐：插件自带）
- **D-3** `RangerHiveAuditHandler` 的 hive-exec `HiveOperationType` 怎么去？（**需要先做 HCS-02 小调研**）
- **D-4** 要不要先跑 **e2e 前置探测**（HCS-01）——**强烈建议**，见下。

## ⚠️ 一个可能颠覆方案的未知数（风险 R-1）

`AWSCatalogMetastoreClient` / `ProxyMetaStoreClient` 由**父加载器**定义（实现父的 `IMetaStoreClient`，hive 3.1.3），
而调用它们的 `RetryingMetaStoreClient` 在**插件里**（child-first，**另一份类身份**）。
→ **Glue-on-HMS 和 paimon-on-DLF 今天可能就已经是坏的**（`ClassCastException`/`LinkageError`），跟本任务无关。

**如果本来就坏**，那么「留着 shade 保平安」是幻觉，插件侧自带客户端是唯一正解，方案可以更激进。
**这个只能在用户真集群 e2e 探测（HCS-01）。建议在动 Glue/DLF 之前先探。**

## 🚫 别做的事

- 别直接删 `fe/fe-core/pom.xml:437-440`（编译就挂，26 个文件）
- 别拿 `doris-shade/.../target/hive-catalog-shade-3.1.2-SNAPSHOT.jar` 做验证 —— 它**已经**改名了 fastutil，
  会让你误判「fastutil hack 已死」。**构建实际解析的是 3.1.1**（`~/.m2/.../3.1.1/`），那份里 fastutil 是**裸的 10653 个类**
- 别为「删得动」把 HiveConf 逻辑挪进 fe-core util（**铁律 B**，遇到就停手交 review）
- 别动：`fe/pom.xml:801-805` 版本钉 · `bin/start_fe.sh:355` 钉序 · `fastutil-core` 依赖 ·
  BE `java-udf`/`avro-scanner` pom · `doris-shade` 仓库

## ⚙️ 操作须知（复用主线）

- maven 必须绝对路径 + `-am`：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile`
  （**漏 `-am` → 假错 `${revision}`**）
- **验证信 LOG 不信 exit**：后台 task 的 exit code 是 wrapper 的；重定向到文件跑，grep
  `BUILD SUCCESS`/`BUILD FAILURE`/`[ERROR].*\.java:`/`Checkstyle`（memory `doris-build-verify-gotchas`）
- **⚠️ `git add` 用 path-whitelist，严禁 `git add -A`**（工作树大量非本线程 scratch）
- 动码前先探并发（memory `concurrent-sessions-shared-worktree-hazard`）—— 主线 session 可能也在改 pom
