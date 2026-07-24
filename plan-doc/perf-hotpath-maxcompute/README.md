# 📦 任务空间 — fe-connector-maxcompute 热路径缓存修复

> 连接器缓存框架统一的兄弟空间（伞形 = `plan-doc/connector-cache-unification/`，行 WS-MC）。
> 镜像 `plan-doc/perf-hotpath-iceberg/` / `perf-hotpath-hudi/` 布局。协作规范沿用 `../AGENT-PLAYBOOK.md`。

## 一句话背景
maxcompute **已 live、已相当好地缓存**（跨查询分区缓存 + 干净 split 枚举 + per-statement 写事务 + 复用 fe-core 跨查询 schema 缓存），**不是 iceberg 那样的 P0 目标**。唯一真正适用的框架件 = **每语句表句柄记忆化**：`getTableHandle` 每次解析都发一次冗余 ODPS `tables().exists()` 远程探测并新建惰性表，funnel 只 memo metadata 不 memo handle → 一条语句 ~17 个解析点各付一次探测（冷统计逐列放大）。

## 本空间文件
| 文件 | 用途 |
|---|---|
| [`HANDOFF.md`](./HANDOFF.md) | 下一步 + 稳定区规则（每 session 覆盖式更新） |
| [`tasklist.md`](./tasklist.md) | PERF-MCxx 勾选 + 状态 |
| [`progress.md`](./progress.md) | append-only 日志 |
| [`designs/`](./designs/) | per-round 设计蓝图（`round-1-handle-memo-design.md` = 当前） |

## 流程（对齐 step-by-step-fix + iceberg 立项流程）
侦察（HEAD 重核）→ 设计 + 红队 → 实现（最小改动/守铁律/连接器侧）→ 验证（parity + build-count 守门）→ 独立 commit → 更新 tasklist/HANDOFF/progress + 回填伞形。
**行号信 HEAD 不信文档**（设计里 file:line 是 2026-07-24 快照）。
