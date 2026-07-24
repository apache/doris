# Progress — perf-hotpath-maxcompute（append-only）

## 2026-07-24 — PERF-MC01 每语句表句柄记忆化（commit `58daadd10e0`）

**做了什么**：`MaxComputeConnectorMetadata.getTableHandle` 改为经 per-statement 实例上的 `Map<List<String>, MaxComputeTableHandle> tableHandleMemo`（`ConcurrentHashMap`）`computeIfAbsent` 解析。收敛 MC-1（冗余 ODPS `exists()` 探测 k×/语句 → 1×）+ MC-2（每表 Table reload 收敛为 1）。**改 1 个连接器文件、+28/-9 行、0 fe-core**。

**流程**：
1. **HEAD 重侦察**（6-agent 只读 workflow）：确认 metadata 每语句单例（funnel `PluginDrivenMetadata.get`）、`getTableHandle` 是唯一 handle 生产点且覆盖 fe-core 13 + translator 2 + BindSink 2 = 17 解析点、handle 无 equals/hashCode（须按 (db,table) 值 key）、跨线程可达（off-thread scan 池复用同 session）→ CHM；`createTable`/`tableExists` 直调 helper 不经 memo；partitionCache 正交。
2. **设计**（`designs/round-1-handle-memo-design.md`）：present-only memo、保留 exists() 只去重、CHM、`List.of` 值 key；owner 中文讲清后确认。
3. **实现 + 守门单测** `MaxComputeConnectorMetadataHandleMemoTest`（仿 `DropDbTest` 手写记录 fake、无 Mockito、null odps 离线）。
4. **验证**：全 `MaxCompute*` 120 测试绿、checkstyle 0 违规。
5. **变异验证（Rule 9）两处**：① 去 memo（`memo.clear()`）→ `sameTableResolvedTwice...` 变红（探测计数 1→2）；② db-blind key（`List.of(tableName)`）→ `sameTableNameInDifferentDatabasesDoNotCollide` 变红（`assertNotSame` 失败）。
6. **净室对抗复审**（4 lens + 2 verify workflow）：parity `PARITY_HOLDS`、staleness `PARITY_HOLDS`；concurrency CONCERN（共享 Table 并发 reload）经 verify **REFUTED**（off-thread 任务用 ctor 一次解析的 `currentHandle`、不调 getTableHandle，共享 Table 是 baseline 既有属性，memo 反而降 reload 争用）；test-quality CONCERN（无跨 db 用例）经 verify **CONFIRMED_REAL** → **已补** `sameTableNameInDifferentDatabasesDoNotCollide` 并变异验证。

**下一步**：PERF-MC02（doc-only 陈旧注释，随手/并 WS-DOC）、PERF-MC03（可选跨查询 Table 缓存，热点触发）。**e2e** 需集群，本地未跑（异构 + 独立 max_compute catalog 的 SELECT/分区裁剪/写路径解析计数），留标注。
