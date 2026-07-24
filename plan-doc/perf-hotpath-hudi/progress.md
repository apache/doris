# Progress Log — perf-hotpath-hudi

> append-only：每 session 追加一条（日期 / 做了什么 / 结论 / 下一步）。滚动上下文在 HANDOFF.md。

---

## 2026-07-24 — 立项 + 调研 + 设计 + 红队复核（未动产品代码）

- **做了什么**：
  1. owner 选定旗舰 hudi 为下一个连接器（连接器缓存统一伞形 WS-HUDI）。
  2. 动码前按 HEAD（`f2e9706df5a`）重侦察：13-agent workflow（`wf_3a0b9aca-966`）——9 路并行核对 5 个热点真实倍数/当前行号 + 框架接入缝 + 授权/生命周期 → 综合设计 → 3 路对抗红队。
  3. 自测独立核实 4 个 crux：`ConnectorStatementScopeImpl` 语句末关 AutoCloseable；`HoodieTableMetaClient`(1.0.2) 非 AutoCloseable（memo 活对象 close-pass-safe）；`CachingHmsClient`/fresh/flush API + hive fresh/cached 模板；hudi 三分区入口 + pom 已依赖 hms/api（无 pom 改动）。
  4. 写设计蓝图 `designs/round-1-memo-hms-cache-design.md`（3 块 + 红队 3 修正 + 验证 + 延后清单）。
- **结论（关键）**：
  - **侦察更正**：metaClient build ~5（非 5-6），schema 3×（非 4），过滤查询分区已去重~1（复核成立）。
  - **旗舰 = 每语句"不可变投影" memo**（{instant, 最新 schema, allHistoricalSchemas}），复用现成 `ConnectorStatementScopes.resolveInStatement`（iceberg 同款）→ **0 fe-core**。红队 3 修正：①不 memo Configuration（可变+非线程安全）②planScan 只读 memo schema、保留自己 lastInstant（文档化字节语义）③latest-only，at-instant 延后。~5 build→~2-3、schema 3×→1×、字节不变。
  - **HMS 缓存层 owner 拍板做全套**：wrap `CachingHmsClient` + 按 hive 补 fresh/cached 拆分（SHOW/TVF→`listPartitionNamesFresh`、剪枝/MTMV→cached）+ override `HudiConnector.invalidate*` flush（网关已 forEachBuiltSibling 转发 REFRESH）——否则 hive-sync 表 SHOW PARTITIONS 陈旧最多 24h 且 REFRESH 清不掉（红队 BLOCKER）。
  - **文档清理**：清 stale "dormant hms" 注释一批。
- **下一步**：实施 3 块（序 A→C→B，各独立 commit），动每个文件前按 HEAD 重 grep，守红队 3 修正 + 铁律 A（0 fe-core）+ 无 pom 改动。
