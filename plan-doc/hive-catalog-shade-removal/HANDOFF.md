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

## 🚩 用户定调（2026-07-14，**必须遵守**）

> **「Glue-on-HMS 和 paimon-on-DLF 现在我无法验证，你先按照『需要保留』来设计任务。」**

⇒ 三条硬约束：
1. **禁止**「反正可能已经坏了 → 干脆删功能」。两条路径**必须**在改完后仍可用。
2. **禁止**无论证的静默换实现（风险 R-2：今天生效的 DLF 客户端是 fe-core vendored 那份，不是 shade 里那份）。
3. **e2e 不可得 ⇒ 用 HCS-01 的离线 classloader 测试代偿**；**阶段 6 是单向门**，归不了 0 就停在降级档
   （`design.md §4.1`），**不许为了删而删**。

## ▶️ 下一个 session 的第一件事：**HCS-01（离线 classloader 复现测试）**

这是本任务现在的**最高优先级**，因为它把一个「谁也说不清、又没法 e2e」的问题变成了**离线可跑的红绿灯**。

**为什么它重要 —— 机制推演说今天大概率就是坏的**（`design.md §3.2b`，行号全部亲验）：

```
ThriftHmsClient:644  把 TCCL 钉到【插件 child loader】
ThriftHmsClient:910  调 RetryingMetaStoreClient.getProxy(..., "com.amazonaws...AWSCatalogMetastoreClient")
                     └─ 内部 Class.forName(name, TCCL).asSubclass(IMetaStoreClient.class)
                                                        └─ 这个 IMetaStoreClient 来自【child】
                                                           （插件 zip 里实查到 hive-catalog-shade-3.1.1.jar, 127MB）
Class.forName 走 child-first：
   com.amazonaws...AWSCatalogMetastoreClient 不在 parent-first 名单 → findClass(插件 lib) 
      → ❌ 找不到（shade jar 里 Glue 类 = 0 个条目！）→ 父回退 → 【app loader】定义
   ⇒ app-loaded 的 Glue 客户端实现的是【app 的】IMetaStoreClient
   ⇒ 与【child 的】IMetaStoreClient 是两个不同 Class 对象
   ⇒ asSubclass 失败 ⇒ ClassCastException
```
paimon-on-DLF 更糟：child 是 hive **2.3.7**，父是 **3.1.3**，版本都不一样。

**HCS-01 要写的测试**（完全离线，不要真集群）：
```java
// 用真实 plugin-zip 的 lib/*.jar 组 ChildFirstClassLoader（parent-first 前缀照抄 ConnectorPluginManager:64）
Class<?> childIMSC = Class.forName("org.apache.hadoop.hive.metastore.IMetaStoreClient", false, child);
Class<?> glue      = Class.forName("com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient", false, child);
childIMSC.isAssignableFrom(glue);   // ← RetryingMetaStoreClient.getProxy 内部 asSubclass 的判据
```
对 Glue（hms/hive/iceberg/hudi）与 DLF（paimon）各做一遍。**先跑，用事实说话，别先信推演。**

⚠️ **无论结果如何这个测试都是交付物**：它是阶段 2/3 搬迁后的**唯一验收门禁**（e2e 不可得）。
而且把 Glue/DLF 客户端**搬进插件**会让 caller/callee 落到同一个 loader ——
**若今天已坏 = 这是修复；若今天能跑 = 这是行为保持。方案两头都成立。**

## ▶️ 可并行开工（不等任何决策）：**阶段 1 独立清理**（HCS-10 → HCS-13）

重点 **HCS-11 删 `hudi-hadoop-mr`**（`fe/fe-core/pom.xml:600-604`）—— 它是 `hive-storage-api:2.8.1`
混进 `fe/lib` 的唯一来源（风险 R-4）。⚠️ `hudi-common` **要留**（`HttpProperties`/`StatisticsCache` 各一处 import）。
这是**降级档 0（保底可交）**，零风险。

## ⏳ 待用户签字（`design.md §5`，用中文讲清背景再问）

- **D-1** 38 个 vendored Glue 文件搬去哪？（推荐：搬进 `fe-connector-hms`）
- **D-3** `RangerHiveAuditHandler` 的 hive-exec `HiveOperationType` 怎么去？（**先做 HCS-02 小调研再问**）
- **D-2 先别问** —— 它有真实技术未知数（DLF 客户端 2193 行 + 依赖 shade 里另外 1963 个 aliyun SDK 类；
  且 artifact 是 `metastore-client-hive3` 而 paimon 私有 shade 是 hive 2.3.7）。**先做 HCS-30a spike。**

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
