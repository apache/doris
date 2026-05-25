# 📊 项目进度仪表盘

> 最后更新：**2026-05-24（晚）** | 当前阶段：**P0 SPI 缺口补齐**（批 0 SPI 接口完成，待 fe-core 桥接） | 项目总进度：**8%**
> [README](./README.md) · [Master Plan](./00-connector-migration-master-plan.md) · [SPI RFC](./01-spi-extensions-rfc.md) · [Decisions](./decisions-log.md) · [Deviations](./deviations-log.md) · [Risks](./risks.md) · [Agent Playbook](./AGENT-PLAYBOOK.md) · [Handoff](./HANDOFF.md)

---

## 一、阶段进度（P0–P8）

| 阶段 | 范围 | 估时 | 进度 | 状态 | 任务文档 |
|---|---|---|---|---|---|
| **P0** | SPI 缺口补齐 | 2 周 | ▰▰▰▱▱▱▱▱▱▱ 30% | 🚧 进行中（批 0 SPI 接口完成；待 fe-core 桥接 T09-T12） | [tasks/P0](./tasks/P0-spi-foundation.md) |
| P1 | scan-node 收口 + 重复清理 | 1 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动（被 P0 阻塞）| — |
| P2 | trino-connector 迁移 | 2 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |
| P3 | hudi 迁移 | 2 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |
| P4 | maxcompute 迁移 | 2 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |
| P5 | paimon 迁移 | 3 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |
| P6 | iceberg 迁移 | 5 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |
| P7 | hive (+HMS) 迁移 | 6 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |
| P8 | 收尾清理 | 2 周 | ▱▱▱▱▱▱▱▱▱▱ 0% | ⏸ 待启动 | — |

**全局进度：5%**（25 周计划中处于第 1 周）

---

## 二、连接器迁移看板

> 维度："SPI 设计" = RFC 中该连接器涉及的 SPI 是否定稿；"实现" = fe-connector 模块中代码完成度；"SPI_READY" = 是否已加入 `CatalogFactory.SPI_READY_TYPES`；"删除旧代码" = fe-core/datasource/<name>/ 是否清空；"反向 instanceof" = nereids/planner 等热区中 `instanceof XExternal*` 是否清理。

| 连接器 | SPI 设计 | 实现完成度 | SPI_READY | 删除旧代码 | 反向 instanceof | 状态 | 详细 |
|---|---|---|---|---|---|---|---|
| **jdbc** | ✅ | ✅ 100% | ✅ | 🟡 (13 个旧 client，P1 删) | n/a | **95%** | [详情](./connectors/jdbc.md) |
| **es** | ✅ | ✅ 100% | ✅ | ✅ | ✅ | **100%** | [详情](./connectors/es.md) |
| trino-connector | 🟡 (P0 待完成) | 🟨 70% | ❌ | ❌ | 0/2 | **30%** | [详情](./connectors/trino-connector.md) |
| hudi | 🟡 | 🟨 50% | ❌ | ❌ | 0/0（寄生 hive） | **20%** | [详情](./connectors/hudi.md) |
| maxcompute | 🟡 | 🟨 60% | ❌ | ❌ | 0/12 | **25%** | [详情](./connectors/maxcompute.md) |
| paimon | 🟡 | 🟨 50% | ❌ | ❌ | 0/10 | **20%** | [详情](./connectors/paimon.md) |
| iceberg | 🟡 | 🟥 10% | ❌ | ❌ | 0/19 | **5%** | [详情](./connectors/iceberg.md) |
| hive (+hms) | 🟡 | 🟥 20% | ❌ | ❌ | 0/31 | **10%** | [详情](./connectors/hive.md) |

---

## 三、当前活跃 task

> 状态非 ✅ 的项，按阶段聚合。详细见各阶段 task 文件。

### P0 — SPI 缺口补齐
| ID | Task | Owner | 状态 | 启动 | 备注 |
|---|---|---|---|---|---|
| P0-T01 | RFC §16.2 决策点闭环 | @me | ✅ | 2026-05-24 | 全部 18 条决策已敲定 |
| P0-T02 | 项目跟踪机制建立 | @me | ✅ | 2026-05-24 | commit 63159837043 |
| P0-T03 | E3：`ConnectorMetaInvalidator` 接口 | @me | ✅ | 2026-05-24 | spi 包 / 5 invalidate 方法 |
| P0-T04 | E3：`ConnectorContext.getMetaInvalidator()` default | @me | ✅ | 2026-05-24 | 返回 NOOP |
| P0-T05 | E4：`ConnectorTransaction` 继承 `ConnectorTransactionHandle` | @me | ✅ | 2026-05-24 | 新增不替换 |
| P0-T06 | E4：`ConnectorWriteOps.beginTransaction` default | @me | ✅ | 2026-05-24 | throws unsupported |
| P0-T07 | E4：`ConnectorSession.getCurrentTransaction` default | @me | ✅ | 2026-05-24 | Optional.empty() |
| P0-T08 | E5：`ConnectorMvccSnapshot` 类型 + 3 default 方法 | @me | ✅ | 2026-05-24 | mvcc 包 + ConnectorMetadata 3 default |
| **批 0 fe-core 桥接（next session）** | | | | | |
| P0-T09 | `DefaultConnectorContext.getMetaInvalidator()` impl | — | ⏳ | — | fe-core 侧 |
| P0-T10 | `ExternalMetaCacheInvalidator`（fe-core 新类） | — | ⏳ | — | 包装 ExternalMetaCacheMgr |
| P0-T11 | `PluginDrivenTransactionManager` 通用化 | — | ⏳ | — | 删 type-specific 分支 |
| P0-T12 | `ConnectorMvccSnapshotAdapter`（fe-core 新类） | — | ⏳ | — | impl MvccSnapshot |
| **批 1 DDL + Partition SPI** | | | | | |
| P0-T13..T20 | E1 CreateTableRequest + E10 listPartitions | — | ⏳ | — | 见 tasks/P0 |
| P0-T21..T27 | 守门脚本 + 回归测试 | — | ⏳ | — | 见 tasks/P0 |

完整 P0 任务清单：[tasks/P0-spi-foundation.md](./tasks/P0-spi-foundation.md)

---

## 四、最近 14 天动态

> 倒序，新内容置顶；超过 14 天的条目移除（git log 保留历史）。

- **2026-05-24（晚）** ✅ **P0 批 0 SPI 接口三件套完成**（T03-T08）：`ConnectorMetaInvalidator`、`ConnectorTransaction`、`ConnectorMvccSnapshot` 共 3 个新类型 + 4 个 default 方法；JDBC/ES clean compile 通过，零下游修改
- **2026-05-24** ✅ 项目跟踪机制建立（README、PROGRESS、decisions-log、deviations-log、risks、tasks/、connectors/、AGENT-PLAYBOOK、HANDOFF）
- **2026-05-24** ✅ SPI RFC §16.2 6 个未决问题（U1-U6）全部决议（D-013..D-018）
- **2026-05-24** ✅ SPI RFC v1 落地（[01-spi-extensions-rfc.md](./01-spi-extensions-rfc.md)）
- **2026-05-24** ✅ Master Plan §5 12 个项目决策点（D1-D12）全部确认（D-001..D-012）
- **2026-05-24** ✅ Master Plan v1 落地（[00-connector-migration-master-plan.md](./00-connector-migration-master-plan.md)）
- **2026-05-24** ✅ 初步代码侦察（177 个 fe-connector 文件、408 个 fe-core/datasource 文件、96 处反向 instanceof）

---

## 五、风险监控（active risks）

| ID | 风险 | 影响 | 当前状态 | 触发阶段 | Owner |
|---|---|---|---|---|---|
| R-001 | Image 反序列化兼容回归 | High | 🟢 监控中 | P2-P7 每个迁移 | @me |
| R-002 | Hive ACID 写路径数据不一致 | High | 🟡 待启动 | P7.3 | TBD |
| R-003 | Iceberg Procedure SPI 抽象失败 | Med | 🟢 监控中 | P6.4 | @me |
| R-004 | classloader 隔离打破 SDK 单例 | Med | 🟢 监控中 | P5/P6 | @me |
| R-005 | nereids 写命令深度耦合 | Med | 🟡 待 P6.3 评估 | P6.3 | TBD |
| R-006 | 通过 SPI 性能回归 | Low | ⏸ 未启动 | P0 末加 benchmark | TBD |
| R-007 | FE/BE 共享 jar 冲突 | Low | ⏸ 未启动 | P5/P6 | TBD |
| R-008 | 文档与流程脱节 | Low | 🟢 缓解中 | 全周期 | @me |

完整列表见 [risks.md](./risks.md)（含 R-009..R-014 从 RFC §16.1 迁入的 Q1-Q6 类技术风险）

---

## 六、决策与偏差快速跳转

| 类型 | 总数 | 最新条目 | 文档 |
|---|---|---|---|
| **决策**（D-NNN） | 18 | D-018（U6: ConnectorColumnStatistics 类型契约） | [decisions-log.md](./decisions-log.md) |
| **偏差**（DV-NNN） | 0 | — | [deviations-log.md](./deviations-log.md) |
| **风险**（R-NNN） | 14 | R-014（thrift sink 选择灵活性） | [risks.md](./risks.md) |

---

## 七、Session 协作状态（Agent / Human）

> 当本项目通过 Claude Code 这类 LLM agent 推进时，跟踪当前 session 状态、handoff 状况和 context 健康度。

- **本 session 已完成**：P0 批 0 SPI 接口三件套（T02 闭环 + T03..T08 实现），clean compile + 下游 connector 兼容性验证通过
- **下一个 session 应做**：fe-core 侧桥接（P0-T09..T12）—— 把新 SPI 接通现有 fe-core 实现（`DefaultConnectorContext`、`ExternalMetaCacheInvalidator`、`PluginDrivenTransactionManager` 通用化、`ConnectorMvccSnapshotAdapter`）；**先让用户 review 本次 SPI 改动再开始**（见 tasks/P0 注意事项 #1）
- **是否需要 handoff**：是，已写新 [HANDOFF.md](./HANDOFF.md)
- **协作规范**：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)（context 预算、subagent 使用、handoff 触发条件）

---

## 八、维护规则速记

| 何时更新本文件 | 改什么 |
|---|---|
| 完成一个 task | §三表中删除 / 标 ✅；§四加一行 |
| 完成一个阶段 | §一进度条 + §三整体清理 + §四加里程碑 |
| 新增决策 | §四加一行 + §六计数 +1 |
| 发现偏差 | §四加一行 + §六计数 +1 |
| 每周一例行 | §四清过期、§五状态滚动、§七 session 状态 review |

📖 详细规则见 [README.md §4 维护规则](./README.md)
