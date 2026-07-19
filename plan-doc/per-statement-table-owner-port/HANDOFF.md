# 🤝 Session Handoff —— 每语句表加载归属者 · 移植到其它连接器

> **滚动文档**：每 session 结束覆盖式更新，只留下一个 session 必须的上下文。
> 开场必读顺序、模板、铁律见 [`README.md`](./README.md)；进度见 [`progress.md`](./progress.md)；状态见 [`tasklist.md`](./tasklist.md)。

---

# 🆕 本轮（2026-07-19 session 1）已完成：**全部连接器 recon + 架构统一性调研 → 结论全部 🔬**

## 一句话结果
- **逐连接器复核（双签）：没有一个连接器现在值得移植**——iceberg 独特在它是**唯一迁移了行级写（DELETE/MERGE）**的连接器；别的连接器只读 / 仅追加写，没有多臂重复加载风暴，现有跨查询缓存又已把加载压到≈1。四项全部标 🔬。
- **"统一接口标准"其实已存在**（中性 `getStatementScope()` + `ConnectorStatementScope`，新连接器天生继承）；**推荐高度 L0**（写下约定 + 登记触发点，生产逻辑零改动）。
- **用户拍板**：本轮先把结论记成**单独文档**（已落 `designs/recon-findings-and-trino-refactor-groundwork.md`）；**重构成 Trino 架构（L2/L3）留到下个 session 专题讨论**。
- **未动任何产品代码**（纯 plan-doc）。

---

# ➡️ 下一个 session = **专题讨论"重构成 Trino 架构"（L2/L3）**

## 第一件事（先读，别急着给方案）
1. 读 **`designs/recon-findings-and-trino-refactor-groundwork.md`**（本轮全部结论：逐连接器复核 §2、iceberg 为何独特 §3、统一性/Trino 参照/高度分级 §4、paimon 触发点 §5、共享 helper 落点 §6、**Trino 重构预备材料 §7**）。
2. 读架构记忆 `iceberg-table-resolution-cache-scoping`（缓存作用域纪律 + "全高度留远期"）。

## 讨论要点（种子，详见结论文档 §7）
- **Trino 到底要引入什么**：每语句/每事务 `ConnectorMetadata` 实例（现状是每 catalog 共享单例）+ 一个 span 生命周期管理器（Doris 现成 span 宿主=`StatementContext`）+ planner 改成经 span 取 metadata + 逐连接器把缓存迁到 per-statement 实例。
- **必须先摆平的硬冲突**：
  - **铁律 A**（删旧代码期 fe-core 只减不增）——L2/L3 几乎全是 fe-core 净增 + planner 改造 → 是否等删旧代码期结束？是否需用户单独签字？
  - 读热路径刚稳定（PERF-01~06/11），per-statement metadata 会重排读热缝、回归面大。
  - **正面收益**：per-statement 实例天然按语句/用户隔离 → 顺带解决"跨查询缓存对 session=user/vended 关闭"的历史包袱（这是 L3 相对现状的真正架构收益）。
- **建议讨论产出**：一份"Doris 每语句/每事务 metadata 重构"的**可行性 + 分期**设计（先 span 宿主 → 逐连接器迁移 → 退役共享单例，避免一次性大爆炸）+ 明确触发条件。

## 关键已知（省得重新发现）
- **地基已就位、勿再改**：`ConnectorStatementScope` + `getStatementScope()`(fe-connector-api) + `ConnectorStatementScopeImpl` + `StatementContext` 懒建/重置 + `ConnectorSessionImpl` 构造期捕获 + `ExecuteCommand` 重置(fe-core)。
- **目前只有 iceberg 用了该 SPI**（`IcebergStatementScope` + `IcebergWritePlanProvider`）。
- **paimon = 唯一真实的将来 L1 候选**：写未迁移（老 JNI-writer INSERT 写栈在删旧代码期被删、未搬进新连接器）；已有胖句柄 `PaimonTableHandle.paimonTable` + 4 加载 seam；**加行级 UPDATE/DELETE 时才值得移植**，届时抽共享 helper（落 `fe-connector-api`，签名/6 处迁移见结论文档 §6）。

## 铁律提醒
- 地基勿再改；移植（若做）只写连接器侧；暴露地基缺口先停手交 review。
- L2/L3 碰铁律 A（fe-core 净增）——**属独立立项 + 需用户签字**，不在本"纯连接器侧移植"任务的默认范围内。
- 作用域跨用户即泄漏——凡有 session=user/凭证语义的连接器，复核共享安全（本轮已确认现有各连接器均目录级单一身份、不泄漏）。

---

# 🗂 遗留 / 关联
- 本轮全部结论：`designs/recon-findings-and-trino-refactor-groundwork.md`。
- iceberg 蓝本任务空间：`../perf-hotpath-iceberg/`（PERF-07 权威设计/小结）。
- 架构记忆：`iceberg-table-resolution-cache-scoping`。
- 本轮调研原始返回：workflow journal `wf_e89cf92e-ff3`（逐连接器）+ `wf_4802a3d2-1c9`（统一性三专项），路径见结论文档 §8。
- e2e 一律留各连接器进 `SPI_READY_TYPES` 切换阶段统一补（对齐 `hms-iceberg-delegation-needs-e2e`）。
