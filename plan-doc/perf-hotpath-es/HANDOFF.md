# 🤝 Session Handoff — perf-hotpath-es

> 滚动文档：顶部「下一步」覆盖式更新；底部稳定区勿覆盖。
> 完成明细进 `git log` + 本空间 `progress.md`；回填伞形 `plan-doc/connector-cache-unification/`。

---

# 🆕 下一步（覆盖区）

## 当前状态（2026-07-24）

- **Round 1 三件全做：✅ 完成**。commits `7d74ba1161b`（per-scan hoist + per-statement schema memo）、`7466b354901`（cross-path mapping via 每语句作用域，0 fe-core）。
- 每语句远程 `~4× getMapping + 2× search_shards + 2× _nodes` → **1×/1×/1×**。全 `Es*` 94 测试绿 + checkstyle 0 + 三处变异验证 + 4-lens 净室复审（parity/concurrency/freshness HOLD，补分片-不入作用域门禁）。
- 设计权威：[`designs/round-1-design.md`](./designs/round-1-design.md)；日志 [`progress.md`](./progress.md)。

## 之后

- **PERF-ES04**（低优先，热点触发）：`getTableHandle→existIndex` 的 `_mapping` GET 与首次 schema getMapping 同 endpoint，可再去重。
- **死代码清理**（随手）：`EsConnectorMetadata.fetchMetadataState` 两个重载无调用者（被 provider 版取代），本轮保留、穿 session 行为中性。
- **e2e**（需集群）：独立 es catalog 过滤 SELECT 断言远程计数 + 分片再平衡后新鲜度。
- 伞形层面：mc/es 两小 PR 均落地 → 剩 P2 backlog（热点触发）+ 门禁通用化 + 陈旧注释清理 + 各连接器 e2e 统一补。

---

# 🧱 稳定区（勿覆盖）

## 铁律（继承伞形）
1. **fe-core 源只出不进**：本轮 memo 在连接器侧（provider 字段 / metadata CHM）；F2 命名空间常量在 `fe-connector-api`（iceberg/hudi 既定模式），**0 fe-core**。
2. **es 硬约束**：分片路由 + 节点拓扑**每 scan/每语句重解析、绝不跨查询缓存**（ES rebalance/refresh）；只有**原始 mapping**（语句内稳定）可进每语句作用域。`ShardRoutingNeverSharedViaScope` 门禁钉死。
3. **authz**：es 单一 catalog Basic 凭证、无 per-user vending → 名字为 key 的 memo 安全。
4. **parity 只减不改**：NONE scope / null session 下逐次加载（byte-identical）。

## build / 验证坑（继承伞形）
1. maven 绝对 `-f <abs>/fe/pom.xml`；`${revision}` → `-am`；F2 动 api → `-pl :fe-connector-api,:fe-connector-es -am`。
2. 测试必加 `-Dmaven.build.cache.enabled=false`；别 `-q`；grep `BUILD SUCCESS`+`Tests run:`。
3. checkstyle 绑 `validate`（`install`/`test` 都跑），扫 test 源。
4. 后台跑直接 `mvn … >> log 2>&1`，读日志非通知 exit code。

## 并发探测（动码前）
- 本 worktree `find fe -newermt '-120 sec'`（滤 `/target/`）+ `pgrep -a -f maven`；`/mnt/disk1/gq` 是别的用户 worktree（独立 ~/.m2），不冲突。
- 提交只精确 `git add` 本任务文件，禁 `git add -A`。

---

# 🔗 关系
- **伞形**：[`plan-doc/connector-cache-unification/`](../connector-cache-unification/)（WS-ES 行）。
- **模板**：[`perf-hotpath-maxcompute/`](../perf-hotpath-maxcompute/)（同期 mc）/ [`perf-hotpath-iceberg/`](../perf-hotpath-iceberg/) / [`perf-hotpath-hudi/`](../perf-hotpath-hudi/)。
