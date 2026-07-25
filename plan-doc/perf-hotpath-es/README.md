# 📦 任务空间 — fe-connector-es 热路径元数据抓取去重

> 连接器缓存框架统一的兄弟空间（伞形 = `plan-doc/connector-cache-unification/`，行 WS-ES）。
> 镜像 `perf-hotpath-iceberg/` / `perf-hotpath-maxcompute/` 布局。协作规范沿用 `../AGENT-PLAYBOOK.md`。

## 一句话背景
es **已 live、读路径为主、无写、无分区、单一凭证**，连接器自身**无扫描元数据缓存**。一条普通过滤 SELECT 约发 ~4× getMapping + 2× search_shards + 2× _nodes 远程往返（多为冗余）。**非 iceberg 那种 per-file 循环放大**——是常数倍（2–4×）冗余。硬约束：**分片路由/节点拓扑绝不跨查询缓存**（ES rebalance）。

## 本空间文件
| 文件 | 用途 |
|---|---|
| [`HANDOFF.md`](./HANDOFF.md) | 下一步 + 稳定区规则（覆盖式更新） |
| [`tasklist.md`](./tasklist.md) | PERF-ESxx 勾选 + 状态 |
| [`progress.md`](./progress.md) | append-only 日志 |
| [`designs/`](./designs/) | per-round 设计蓝图（`round-1-design.md` = 当前） |

## 流程（对齐 step-by-step-fix + iceberg 立项流程）
侦察（HEAD 重核）→ 设计 + 红队 → 实现（最小改动/守铁律/连接器侧）→ 验证（parity + build-count 守门 + 变异）→ 独立 commit → 更新 tasklist/HANDOFF/progress + 回填伞形。
**行号信 HEAD 不信文档**（设计里 file:line 是 2026-07-24 快照）。
