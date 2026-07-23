# Progress Log — 连接器缓存框架统一 (connector cache unification)

> **append-only**：每 session 追加一条（日期 / 做了什么 / 结论 / 下一步）；不覆盖、不删旧条。
> 滚动上下文在 [`HANDOFF.md`](./HANDOFF.md)，进度总览在 [`tasklist.md`](./tasklist.md)。

---

## 2026-07-23 — 搭建伞形追踪空间（未启动执行）

- **做了什么**：读 `00-research-report.md` + `data/connector-audits.json`（hive/hudi/paimon 全文）+ 参考空间 `perf-hotpath-iceberg/`（README/HANDOFF/tasklist）；据此在本目录建 `HANDOFF.md` + `tasklist.md` + 本 `progress.md`。
- **结论**：
  - 本空间定位为**伞形协调空间**——追踪 workstream 级（WS-HUDI / WS-MC / WS-ES / WS-DOC / WS-P2）+ 4 个 owner 决策（D1–D4）；逐连接器执行各自另开 `perf-hotpath-<c>/` 兄弟空间（报告 §9）。
  - 未改 `README.md`（它是完成态的调研交付物）；稳定流程/铁律/build 坑放进 HANDOFF 底部稳定区。
  - **未做任何拍板、未启动任何执行、未改产品代码。**
- **下一步（交下个 session）**：先向用户讲清 D1–D4 拿签字（中文，见 HANDOFF「下一步」），默认推荐先启动 WS-HUDI（唯一 P1 真缺口）并新开 `plan-doc/perf-hotpath-hudi/`。动码前按 HEAD 重侦察（行号信 grep）。

---

## 2026-07-23 (2) — owner 4 决策签字 + 设计定稿（仍 0 产品代码改动）

- **做了什么**：
  1. 向 owner 讲清 D1–D4 并拿签字：D1 hudi+mc+es 都做；D2 先建底座；D3 现在就通用化门禁；D4 原选"提炼进 fe-core"。
  2. 跑 11-agent 只读设计调研 workflow（6 路 HEAD 侦察 + Trino 参考 → 设计综合 → 3 路对抗评审 → 终稿）。产物存 `designs/`（`foundation-design-FINAL.md` + draft + review-1..3）。
     - 注：首跑综合步因"大嵌套结构化输出被截断"失败；改成 prose 输出 + 断点续跑（6 侦察命中缓存），成功。
  3. 设计定稿后向 owner 二次确认，两处更正/确认拍板。
- **结论（关键）**：
  - **D4 前提被侦察推翻**：per-statement memo 底座**早已在 `fe-connector-api`**（`ConnectorStatementScope.computeIfAbsent`/`ConnectorSession.getStatementScope`；iceberg/hudi 均依赖该层、均不依赖 fe-core）→ helper 只是该层一个小静态方法 → **fe-core 0 行、铁律 A 不碰**。owner 二次确认接受、删掉"往 fe-core 塞"备选（含 mc 的 B2）。又一次"侦察推翻已签字蓝图"（memory `execution-blueprint-overestimates-recon-first`）。
  - **底座几乎现成**：通用缓存封装 = 升格已存在的 `ConnectorPartitionViewCache[V]`（iceberg/hive/paimon 已用）→ `ConnectorMetadataCache[V]`；key `PartitionViewCacheKey`→`ConnectorTableKey` 四元组。**整套 A/B/C + hudi/mc/es = 0 行 fe-core 改动。**
  - **门禁原方案有 BLOCKER**：iceberg 缓存字段也在 metadata 类上、hive 网关无标记缓存 → 评审重设计为"模块内扫缓存构造点 + 断言凭证置空 + 网关纳入 + 零声明者硬失败"。
  - owner 确认：**iceberg 本轮就改挂**（真正先建底座、证 parity）。
  - 评审强制若干安全约束：hudi 记忆"不可关闭投影"、instant 每语句重取只缓存分区列表、iceberg `invalidateDb` parity 测试、es 分片路由拆开保持每语句。
- **下一步（交下个 session）**：进入实施，按 HANDOFF「实施路线」8-PR（PR-0 先验 → PR-1 封装 → PR-2 helper+iceberg 改挂 → PR-3 iceberg 收敛 → PR-4 hudi → PR-5 mc → PR-6 es → PR-7 门禁）。**动每个文件前按 HEAD 重侦察**；全程守 fe-core 0 行，遇"不得不碰 fe-core"停手交 review。

---

## 2026-07-23 (3) — PR-0 完成：预编译执行的连接器作用域重置回归守门（含外表可达性侦察）

- **做了什么**：
  1. 进实施前按 HEAD 重侦察 PR-0/1/2 承重事实（6 路只读 workflow + 综合 drift 报告）：**设计成立、有更正、更正均缩小工作量**。
  2. 用户质疑"预编译语句是否支持外表"→ 专项 Explore 侦察**查实：外表确经预编译执行**——走普通 `executor.execute()` 全量规划路、每次执行重新经连接器解析外表；**永进不了 OLAP 短路点查快路**（短路规则只匹配 `logicalOlapScan()`，`LogicalResultSinkToShortCircuitPointQuery.java:88,97`）。故 `ExecuteCommand.java:95` 的 `resetConnectorStatementScope()` 真实可达、承重，只是此前**零测试守门**（现有测试只覆盖 reset 原语，不证 `ExecuteCommand` 调它）。
  3. 加 FE 单测 `ConnectorStatementScopeTest.executeCommandResetsConnectorScopePerExecution`：往复用的语句上下文放哨兵值 → 驱动 `ExecuteCommand.run()`（执行器 stub 空操作、`enableGroupCommitFullPrepare=false` 走普通路）→ 断言执行后作用域被替换、哨兵不残留。
  4. 改正 FINAL 设计里被证伪的机制描述（"每次执行拿新 `StatementContext`" → "复用上下文 + 每次执行显式 `resetConnectorStatementScope()`"，并记录外表可达性侦察结论）。
- **验证**：`mvn test -pl fe-core -am -Dtest=ConnectorStatementScopeTest -Dmaven.build.cache.enabled=false`：`Tests run: 9, Failures: 0`，BUILD SUCCESS，checkstyle 过。**变异验证**（注释掉 `ExecuteCommand.java:95` 的 reset）：`Failures: 1` 且仅新测试变红（`expected: not same`），其余 8 测试仍绿 → 证明测试能失败且精确针对该 reset（Rule 9）。变异后已**逐字节还原** `ExecuteCommand.java`（`git diff` 空）。
- **侦察更正（供后续 PR，动码前仍须再 grep 确认）**：
  - ①**无"兼容子类"可删**——连接器直接构造 `ConnectorPartitionViewCache`（iceberg 构造两次 `:281/:284`，hive `:134`，paimon `:158`），`git grep "extends ConnectorPartitionViewCache"` 空 → PR-1 删掉"删兼容子类"这一步；勿把 `IcebergPartitionCache`（独立 PERF-02 层）误当子类。
  - ②`ttl≤0→CACHE_TTL_DISABLE_CACHE` 映射复制在 **6** 处（设计写 5）：`IcebergComment/Format/LatestSnapshot/Partition/Table` + `PaimonLatestSnapshotCache` → PR-1 收进 `CacheSpec` 动 6 处。
  - ③`formatCache` 挂在 `IcebergScanPlanProvider`（`IcebergConnector.java:782` 注入）**非** metadata 对象 → PR-3 触 format 缓存须对扫描规划器。
  - ④iceberg 5 缓存**全独立 `final class`**、均建于 `MetaCacheEntry`、**无一** extends `ConnectorPartitionViewCache`；entry 名 hyphen（`iceberg-table` 等，非 `iceberg.table`）→ PR-3 钉死 legacy 名。
  - ⑤`ConnectorStatementScopeImpl` 在 **fe-core**（`org.apache.doris.connector`，引用 fe-core `CatalogStatementTransaction`），interface 在 fe-connector-api；iceberg 经 fe-connector-spi **传递**依赖 api、hudi 直接依赖 → PR-2 的 `ConnectorStatementScopes` helper 放 fe-connector-api 仍**0 行 fe-core**。
- **下一步**：PR-1 通用缓存封装升格（`ConnectorPartitionViewCache[V]`→`ConnectorMetadataCache[V]`、`PartitionViewCacheKey`→`ConnectorTableKey`、6 处 ttl 映射收进 `CacheSpec`、修 stale javadoc "no consumers yet"、iceberg/hive/paimon 改挂）；纯加+改名+删，反应堆 test-compile + 现有 partition-view 测试证零变化。**动每个文件前按 HEAD 重侦察**。
