# 📦 任务空间 — fe-connector-hudi 热路径缓存修复

> 连接器缓存框架统一的**旗舰连接器**兄弟空间（伞形 = `plan-doc/connector-cache-unification/`，行 WS-HUDI）。
> 镜像 `plan-doc/perf-hotpath-iceberg/` 布局。协作规范沿用 `../AGENT-PLAYBOOK.md`。

## 一句话背景
hudi 是所有 SPI 连接器里**缓存最薄**的一个：零跨查询元数据缓存、零语句内 metaClient/schema memo、HMS 客户端未包缓存层。一条普通过滤 SELECT 的规划里，同表 metaClient 重建 ~5 次、schema 重解析 3 次。iceberg 框架模式直接适用。

## 本空间文件
| 文件 | 用途 |
|---|---|
| [`HANDOFF.md`](./HANDOFF.md) | 下一步 + 稳定区规则（每 session 覆盖式更新） |
| [`tasklist.md`](./tasklist.md) | PERF-Hxx 勾选 + 状态 |
| [`progress.md`](./progress.md) | append-only 日志 |
| [`designs/`](./designs/) | per-round 设计蓝图（`round-1-memo-hms-cache-design.md` = 当前） |

## 流程（对齐 step-by-step-fix + iceberg 立项流程）
侦察（HEAD 重核）→ 设计 + 红队 → 实现（最小改动/守铁律/连接器侧）→ 验证（parity + build-count 守门）→ 独立 commit → 更新 tasklist/HANDOFF/progress + 回填伞形。
**行号信 HEAD 不信文档**（设计里 file:line 是 2026-07-24 快照）。
