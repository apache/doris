# 📦 任务空间 —— 把「每语句表加载归属者」范式从 iceberg 移植到其它连接器

> **独立任务空间**。目标：把 iceberg 上已落地的「一条语句里同一张表只加载一次、读/扫描/写共享」范式（PERF-07 蓝本），移植到其它 catalog-backed 连接器。
> **重要前提**：这套范式依赖的**中性 fe-core / fe-connector-api 地基已经就位**（PERF-07 建）——移植是**纯连接器侧工作，无需再改 fe-core**。
> 开场必读顺序、单连接器立项流程、约束铁律见下。协作规范沿用 [`../AGENT-PLAYBOOK.md`](../AGENT-PLAYBOOK.md)。

---

## 🚩 一句话背景

新框架下，一条 DML（尤其 DELETE/MERGE）里同一张表会被**反复远端加载**：读元数据、扫描规划、写成形、开事务各自 `loadTable`，最糟一条语句 3~5 次。iceberg 已通过 PERF-07 修掉（读/扫描/写/beginWrite 共享同一「每语句表加载归属者」，整条语句一张表只加载一次）。本空间把同一范式推广到其它同样"metastore/catalog 反复加载同表"的连接器。

> ⚠️ 诚实定位（对齐 iceberg 蓝本）：这类改动**性能收益有限**（写侧 ≈0，读侧靠既有缓存已覆盖一部分），**主价值 = 架构连贯 + 删重复加载/单例/胖句柄**。是否对某连接器值得做，取决于它真实的多载程度——**逐连接器复核后再定，不是照单全上**。

---

## 🧱 已就位的可复用地基（**勿再改，直接复用**）

PERF-07 已在 fe-core / fe-connector-api 落地一套**连接器无关**的每语句作用域基建（commit `97bdcd6bdbe`）：

| 组件 | 位置 | 作用 |
|---|---|---|
| `ConnectorStatementScope`（SPI + `NONE`） | `fe-connector-api/.../connector/api/ConnectorStatementScope.java` | 中性接口：`<T> T computeIfAbsent(String key, Supplier<T>)`；`NONE`=不缓存(逐次加载)，离线/无上下文默认 |
| `ConnectorSession.getStatementScope()` | `fe-connector-api/.../connector/api/ConnectorSession.java` | 默认方法→`NONE`；连接器经它够到每语句作用域 |
| `ConnectorStatementScopeImpl` | `fe-core`（CHM 背书） | 生产实现，挂 `StatementContext` |
| `StatementContext` 懒建字段 + `resetConnectorStatementScope()` | `fe-core` | 唯一贯穿整条语句的对象；不在 close/release 清、随 GC |
| `ConnectorSessionImpl`/`Builder` **构造期捕获** | `fe-core` | 因扫描流式/分批 off-thread 复用同一 session、实时读 thread-local 会失效，故构造期捕获作用域引用 |
| `ExecuteCommand` 一行重置 | `fe-core` | 预编译 EXECUTE 复用同一 `StatementContext`，每执行重置作用域 |

**移植一个连接器 = 只写连接器侧代码**（不再碰 fe-core，即不再触"fe-core 只减不增"铁律 A）。

---

## 🧩 连接器侧移植模板（以 `IcebergStatementScope` 为范本）

对每个目标连接器，镜像 iceberg 的做法（`fe-connector-iceberg/.../IcebergStatementScope.java` 是唯一现成范本）：

1. **加连接器私有 `XStatementScope` helper**：`sharedTable(session, db, tbl, loader)`，键形如 `x.table:catalogId:db:tbl:queryId`；`session==null`（NONE）时逐次加载（等价无缓存）。值存 RAW 表对象（fe-core 不认连接器类型，保持 connector-agnostic）。
2. **四处表解析全走 `sharedTable`**：读元数据 / 扫描规划 / 写成形 / `beginWrite`——同一语句同一表命中同一次加载。每处 loader 各保留原授权（`newXBackedOps(session)`，一语句=一用户=一凭证）。
3. **（若有）拆胖句柄**：若该连接器像 iceberg 那样在 handle 上挂了 `resolvedTable` 之类的"同 handle 内 memo"，评估是否随之下沉到作用域、让句柄回归纯坐标。**没有就不做**（surgical）。
4. **（若有）把每语句暂存下沉到作用域**：iceberg 把删除清单暂存（`IcebergRewritableDeleteStash`）从单例下沉到作用域同键 map 并整删该类。目标连接器若有类似"扫描填、写读"的跨臂暂存，同样下沉；没有就跳过。
5. **响亮失败守卫**：若存在"作用域缺失=静默错误"的路径（iceberg 的 v3 行级 DML + NONE），加 fail-loud（NONE 下抛，消息含 "per-statement scope"）。生产恒有 `StatementContext`，仅离线触发。

**测试守门**（镜像 iceberg）：
- 每语句加载计数守门：读+扫描（+写）共享作用域 → 远端 `loadTable` 计数 **1**；对照 NONE → N。
- 作用域隔离：同键读写同实例 / 跨 queryId 隔离（预编译重执行）/ 跨 catalogId 隔离（跨-catalog 语句）/ NONE 逐次。
- e2e：留连接器进入 `SPI_READY_TYPES` 的切换阶段统一补（对齐 iceberg 的 e2e 欠账惯例）。

---

## 🎯 候选连接器（2026-07-19 初摸，**范围待下 session 复核确认**）

| 连接器 | 写路径 | `loadTable`/`getTable` 触点 | 候选度 | 备注（待 recon 核实） |
|---|---|---|---|---|
| **paimon** | 有（MTMV/写；`getWritePlanProvider` 未在连接器层 override，写路径结构待确认） | **4 文件**（最多） | **高** | 读侧多载最像 iceberg；`fe-connector-cache` 已复制框架副本 |
| **hive / hms** | 有（`HiveWritePlanProvider`） | 3 文件 | **中-高** | plain-hive 读写活；hms 网关委派 iceberg/hudi 兄弟（休眠，未进 `SPI_READY_TYPES`）；**网关按 handle 选 sibling 的特殊性要单独设计** |
| **hudi** | 有（`HudiConnectorMetadata` 写面） | 1 文件 | **中** | 读为主（MTMV 新鲜度已提升）；写臂/多载程度待确认 |
| maxcompute / es / jdbc / trino | 部分有写 | **0**（不走 metastore `loadTable` fan-out） | **低 / 大概率不适用** | 解析模型不同（表对象连接器侧缓存 / 无每语句重载）；下 session 确认后**排除**居多 |

> **第一步不是动码，是逐连接器 recon**：确认每个候选真实的"一条语句加载同表几次"、现有缓存覆盖哪段、是否有胖句柄/跨臂暂存。复核可能把某连接器**判为不必做**（如 iceberg PERF-05/10 就被复核缩小/暂缓过）。

---

## 🔁 单连接器立项流程（一次一个连接器，对齐 `step-by-step-fix`）

1. **recon（动码前）**：grep 该连接器读/扫描/写/beginWrite 的表解析调用链，数"一条 DML 加载同表几次"、现有缓存边界、有无胖句柄/跨臂暂存。产出该连接器的现状图。
2. **写设计** `designs/PORT-<connector>-design.md`：Problem / 现状调用链 / 移植方案（按上面 5 步模板裁剪）/ 与 iceberg 蓝本差异 / Risk / Test Plan（含加载计数守门）。
3. **设计红队**（对抗 review，clean-room 偏好）：至少一个独立视角挑刺——重点核**授权/凭证隔离**（作用域跨用户是否泄漏，参照 iceberg 的 session=user/vended 判定）与**快照/OCC 一致性**。
4. **实现**：纯连接器侧、最小改动、镜像 `IcebergStatementScope`。
5. **验证**：连接器 UT 全绿 + 加载计数守门；纯连接器改动**免 fe-core 两段验**（地基已在）。
6. **独立 commit** + **写小结** `designs/PORT-<connector>-summary.md` + 勾 `tasklist.md` + 追加 `progress.md` + 覆盖 `HANDOFF.md`。

---

## 🧱 约束铁律

1. **地基勿再改**：`ConnectorStatementScope`/`getStatementScope()`/`ConnectorStatementScopeImpl`/`StatementContext` 已就位，移植**只写连接器侧**——不再碰 fe-core（不重新触铁律 A）。若某连接器暴露地基缺口，先停手交 review，别顺手改 fe-core。
2. **作用域跨用户即泄漏**：作用域按 `catalogId+queryId`（+db/tbl）建键、存 RAW、随 session 生死；但**缓存命中会绕过 per-user loadTable 里的授权**——凡有 `session=user`/凭证语义的连接器，复核作用域共享是否安全（参照 iceberg 判据：授权发生在 load 调用里，缓存命中绕过它=元数据泄漏）。
3. **连接器不解析属性 / 通用节点 connector-agnostic**：沿用本项目一贯铁律；作用域值为 `Object`，fe-core 不认连接器类型。
4. **surgical**：模板 5 步里"拆胖句柄""下沉暂存"仅当该连接器真有对应物才做；没有就不做。

---

## 🔗 与 iceberg 蓝本的关系

- **权威范本**：`plan-doc/perf-hotpath-iceberg/designs/FIX-PERF-07-unified-per-statement-table-owner-{design,summary}.md`；架构结论落 memory `iceberg-table-resolution-cache-scoping`。
- **代码范本**：`fe-connector-iceberg` 的 `IcebergStatementScope` + 四处 `resolveTable*` 走它 + fail-loud（commit `ea7fd1f6e7a`）。
- 本空间是那套范式的**推广执行区**，与 `perf-hotpath-iceberg` 任务空间平行、不混流。
