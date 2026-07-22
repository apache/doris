# fe-core 数据源依赖 & 残留代码清理 — 任务空间

独立于 catalog-SPI 主线的一个小任务空间，专治一件事：**把 fe-core 里残留的数据源特有依赖与代码清理/迁走**。
分支 `catalog-spi-review-17`，创建于 2026-07-21。

## 文档导航

| 文件 | 作用 | 何时读 |
|---|---|---|
| [`HANDOFF.md`](./HANDOFF.md) | **会话交接**：起步步骤、已定决策速览、架构铁律、构建/验证方法、风险、进度日志 | **每个 session 开头先读**，每轮结束更新 |
| [`TASKLIST.md`](./TASKLIST.md) | **可勾选的分批任务清单**（Batch 0–5），每项带 `file:line`、判据、验证、commit | 干活时对照，完成即勾选 |
| [`fe-core-datasource-deps-and-code-cleanup-2026-07-21.md`](./fe-core-datasource-deps-and-code-cleanup-2026-07-21.md) | **完整分析报告**：逐依赖判定 + 逐数据源源码扫描的全部证据与依据 | 需要证据/背景时查 |

## 一句话现状

分析已完成、**尚未动代码**。依赖层面 fe-core 主源码对数据源库的编译依赖已近清零，真正的删除门槛在 5 个 iceberg 测试类的归属；代码层面 iceberg 行级 DML、legacy engine=hive 簇等仍 LIVE，属迁移而非删除。下一个 session 从 [`TASKLIST.md`](./TASKLIST.md) 的 **Batch 1** 起步。

## 工作纪律（详见 HANDOFF §2 / §3）

- fe-core 源码**只减不增**；禁为编译过而把逻辑就近搬进 fe-core。
- 行号是快照，按符号名/内容重新定位再改。
- 每批独立 commit（英文信息）；每轮更新 HANDOFF 进度日志。
- Maven 用绝对 `-f`；`-DskipTests` 仍编译测试，验证要跑到测试编译。
