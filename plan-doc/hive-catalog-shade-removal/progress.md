# 📜 进度记录 — 剔除 `hive-catalog-shade`

> **Append-only 日志**：只往下追加，**不覆盖不删改**（覆盖式的「下一步」在 [`HANDOFF.md`](./HANDOFF.md)）。
> 每条格式：`## YYYY-MM-DD — 标题` + 做了什么 / commit / 结论 / 踩坑。

---

## 2026-07-14 — 立项 + 事实基线（零代码改动）

**做了什么**
- 10-agent 工作流：6 路侦察（fe-core 静态用法 / 运行时需求 / 引入历史 / 其它消费者 / 与 paimon-hive-shade 的关系 /
  连带 hack）+ **3 路对抗验证**（compile-break / runtime-NoClassDefFound / packaging-and-scope）+ 综合。
- 主 session **亲验**了所有要落进持久文档的关键事实（可复现命令见 `design.md §7`）。
- 建立本任务文档空间（README / design / tasklist / progress / HANDOFF）。

**结论（推翻了立项时的假设）**
- ❌ **假设「hive/iceberg/paimon 都迁到插件了 → shade 已冗余可删」= 错。**
  3 路对抗验证**全部**推翻了「可以直接删」：
  - **编译**：fe-core `src/main` **26 个文件**仍 import hive 类（Glue 38 文件树 / DLF 客户端 / 5 个 HiveConf 属性类 /
    `DefaultConnectorContext` / `RangerHiveAuditHandler`）；测试 4 个。
  - **运行时**：`fe/lib` 是所有插件的**父加载器**，且 `AWSCatalogMetastoreClient`（**shade jar 里 0 个条目**，唯一
    定义 = fe-core vendored 源码）与 paimon 的 `ProxyMetaStoreClient` 是**按名反射加载、只存在于父**的。
  - **打包**：`fe-common` 的 shade 是 **`provided`** → 删 fe-core 依赖后 fe-common **编译绿、运行炸**，
    且它在**每个外表 catalog** 的路径上。
- ✅ **假设「fe-core 是从 fe-common 传递拿到 shade」= 错**（实跑 maven 否掉）：Maven 从不传递 `provided`；
  fe-core 只有**一条 depth-1 compile 边**。所以删 `fe-core:437-440` **确实**能把 jar 踢出 `fe/lib` ——
  这正是它现在**不能**删的原因。
- ✅ **`fe-connector-paimon-hive-shade` 与 `hive-catalog-shade` 无依赖关系** —— 是同一招数的插件私有版重新实现
  （改名前缀故意不撞：`org.apache.doris.paimon.shaded.thrift` vs `shade.doris.hive.org.apache.thrift`；
  hive 2.3.7 vs 3.1.3）。paimon 当年（P5）明确**拒绝**了全局 shade；iceberg 反而按 **D-060** 签字复用它。

**关键踩坑（写下来防止重复踩）**
- ⚠️ **验证陷阱**：`doris-shade/.../target/hive-catalog-shade-3.1.2-SNAPSHOT.jar`（本地构建产物）**已经**改名了
  fastutil；拿它验证会误判「fastutil hack 已死」。**构建实际解析的是 3.1.1**，那份里 fastutil 是**裸的 10653 个类**。
- ⚠️ `mvn dependency:tree` 在本 reactor 里**必须加 `-am`**，否则 `${revision}` 解析失败报假错
  （侦察 agent 一度据此断言「这里跑不了 dependency:tree」——是错的）。
- ⚠️ 别信 `dependency:analyze`：`AliyunDLFBaseProperties.java:71` 把 `DataLakeConfig.CATALOG_PROXY_MODE` 当**注解
  String 常量**用，javac 会**内联**进字节码 → 字节码扫描报「未使用」，但 `javac` 仍会因 `import` 失败。

**被验证阶段纠正的侦察结论**（记下来，别回退到错版本）
- 侦察说「fe-core 里 ~40 个文件 import hive」→ 实测 **26**（main）+ 4（test）。
- 侦察说「shade 是 `org.apache.hadoop.hive.*` 的唯一提供者」→ **错**：`hudi-hadoop-mr → hudi-hadoop-common →
  hive-storage-api:2.8.1` 也往 `fe/lib` 塞 hive 类。删完 shade，`fe/lib` **仍不是** hive-free
  → 半填充命名空间（风险 R-4，比干净缺失更难查）。

**未决 / 待用户拍板**：D-1（Glue 归属）· D-2（paimon DLF）· D-3（Ranger `HiveOperationType`）·
D-4（是否先跑 e2e 前置探测 —— **强烈建议**，风险 R-1：Glue-on-HMS / paimon-on-DLF **今天可能已经是坏的**）。

**commit**：（文档 commit 待填）
