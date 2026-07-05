# 风险登记册

> **滚动状态**：与 decisions / deviations 不同，本文件中**每个风险条目允许更新状态**（监控中 → 缓解中 → 已闭环 / 已触发）。
> 编号规则：`R-NNN` 三位数字。原 master plan §6 的 R1-R8 + RFC §16.1 的 Q1-Q6 已迁入映射到 R-001..R-014。
> 维护规则见 [README §4.4](./README.md)：每周一例行扫一遍。
>
> 模板见文末 §附录。

---

## 📋 风险矩阵

> 横轴 = 概率，纵轴 = 影响。颜色：🔴 必须缓解 / 🟠 应该缓解 / 🟡 监控 / ⚪ 可接受

| | **概率：低** | **概率：中** | **概率：高** |
|---|---|---|---|
| **影响：High** | 🟠 R-006 | 🔴 R-001 | 🔴 R-002 |
| **影响：Med** | 🟡 R-007、R-011 | 🟠 R-004、R-005 | 🟠 R-003、R-009、R-010、R-012 |
| **影响：Low** | ⚪ — | 🟡 R-008、R-013、R-014 | 🟡 — |

---

## 📋 索引（当前 active）

| 编号 | 别名 | 风险 | 影响 | 概率 | 状态 | Owner | 触发阶段 |
|---|---|---|---|---|---|---|---|
| R-001 | R1 | Image 反序列化兼容回归 | High | 中 | 🟢 监控中 | @me | P2-P7 每个迁移 |
| R-002 | R2 | Hive ACID 写路径数据不一致 | High | 高 | 🟡 待启动 | TBD | P7.3 |
| R-003 | R3 | Iceberg Procedure SPI 抽象失败 | Med | 高 | 🟢 监控中 | @me | P6.4 |
| R-004 | R4 | classloader 隔离打破 SDK 单例 | Med | 中 | 🟢 监控中 | @me | P5/P6 |
| R-005 | R5 | nereids 写命令对外部表深度耦合 | Med | 中 | 🟢 RFC 评估定（O5-2 + Route B；DV-04x） | P6.3 实现 | P6.3 |
| R-006 | R6 | 通过 SPI 性能回归 | Low | 低 | ⏸ 未启动 | TBD | P0 末 benchmark |
| R-007 | R7 | FE/BE 共享 jar 冲突 | Low | 低 | ⏸ 未启动 | TBD | P5/P6 |
| R-008 | R8 | 文档与流程脱节 | Low | 中 | 🟢 缓解中 | @me | 全周期 |
| R-009 | Q1 | `ConnectorProcedureSpec.arguments` 类型不安全 | Med | 中 | 🟢 监控中 | @me | P6.4 |
| R-010 | Q2 | `ConnectorMetaInvalidator` 异常路径 leak listener thread | Med | 中 | 🟢 监控中 | @me | P7.2 |
| R-011 | Q3 | `ConnectorTransaction.commit` 跨 BE 分片复杂性 | Med | 低 | 🟢 监控中 | @me | P5-P7 |
| R-012 | Q4 | `ConnectorMvccSnapshot.snapshotId` long 不适配 string-id 系统 | Med | 中 | 🟢 监控中 | @me | P5/P6（未来） |
| R-013 | Q5 | `ConnectorPartitionField.transform` 字符串约定漂移 | Low | 中 | 🟢 监控中 | @me | P5/P6/P7 |
| R-014 | Q6 | E9 的 thrift sink 选择与 connector 演化脱节 | Low | 中 | 🟢 监控中 | @me | P6.3 |

---

## 详细记录

### R-001 (R1) — Image 反序列化兼容回归

- **首次提出**：2026-05-24（master plan §6）
- **影响**：High — 用户从旧 FE 升级时 catalog 元数据丢失，最坏情况无法启动
- **概率**：中 — 每个连接器迁移都需要处理，工作量大
- **当前状态**：🟢 监控中
- **缓解措施**：
  1. 每个连接器迁移加 image 兼容测试（regression-test 中新增 `<connector>_migration_compat` 套件）
  2. `PluginDrivenExternalCatalog.gsonPostProcess` 中保留 logType 迁移分支至少 2 个大版本
  3. `ExternalCatalog.registerCompatibleSubtype` 注册每个旧子类的 GSON 兼容映射
- **拥有者**：@me
- **关联 task**：所有 P2-P7 迁移 task
- **更新日志**：
  - 2026-05-24：初始登记；ES/JDBC 已用此模式验证可行

---

### R-002 (R2) — Hive ACID 写路径数据不一致

- **首次提出**：2026-05-24（master plan §6）
- **影响**：High — 数据正确性问题，最严重的失败模式
- **概率**：高 — `HMSTransaction` 1866 行复杂逻辑、重构难度大
- **当前状态**：🟡 待启动（P7.3 才相关）
- **缓解措施**：
  1. P7.3 启动前必建独立 ACID test suite（覆盖 INSERT OVERWRITE PARTITION、UPDATE、DELETE、MERGE 各 corner case）
  2. 用旧实现产生 baseline 数据 → 新实现 bit-for-bit 比对
  3. 增加 chaos test（commit 中途杀 FE / 杀 BE）
- **拥有者**：TBD（P7 启动前指派）
- **关联 task**：P7.3
- **更新日志**：
  - 2026-05-24：初始登记

---

### R-003 (R3) — Iceberg Procedure SPI 抽象失败

- **首次提出**：2026-05-24（master plan §6）
- **影响**：Med — 10 个 action 用不了，但用户可绕过用 Trino/Spark 调用
- **概率**：高 — Action 行为多样，统一抽象有挑战
- **当前状态**：🟢 监控中
- **缓解措施**：
  1. 已参考 Trino Iceberg connector 的 Procedure SPI 设计（RFC §5）
  2. P6.4 启动前先实现 2 个最简单的（`expire_snapshots`、`rollback_to_snapshot`）验证抽象
  3. 若发现抽象不行，按 deviation 流程调整 SPI
- **拥有者**：@me
- **关联 task**：P6.4
- **更新日志**：
  - 2026-05-24：初始登记；RFC §5 已设计 `ConnectorProcedureOps` 草案

---

### R-004 (R4) — classloader 隔离打破 SDK 单例

- **首次提出**：2026-05-24（master plan §6）
- **影响**：Med — Iceberg/Paimon/Trino SDK 加载错误，连接器初始化失败
- **概率**：中 — 已有 ES/JDBC 验证基础可行性，但复杂 SDK 未试
- **当前状态**：🟢 监控中
- **缓解措施**：
  1. `ConnectorPluginManager.CONNECTOR_PARENT_FIRST_PREFIXES` 已含 `org.apache.doris.connector.` 和 `org.apache.doris.filesystem.`
  2. 每个连接器加入 SPI 路径时确认 parent-first 列表覆盖所有共享 SDK 接口
  3. P5/P6 跑集成测试验证多 catalog 实例共存
- **拥有者**：@me
- **关联 task**：P5、P6
- **更新日志**：
  - 2026-05-24：初始登记

---

### R-005 (R5) — nereids 写命令对外部表深度耦合

- **首次提出**：2026-05-24（master plan §6）
- **影响**：Med — Iceberg DML 命令（DELETE/MERGE/UPDATE）改造工作量难估
- **概率**：中 — `IcebergUpdateCommand` 等 305-行级别复杂逻辑
- **当前状态**：🟢 RFC 评估定（2026-06-23，`06-iceberg-write-path-rfc.md` 评审通过 `a49720820f9`）
- **缓解措施（RFC 裁定）**：
  1. ✅ `plan-doc/06-iceberg-write-path-rfc.md` 已写 + PMC 评审通过；**O5 = O5-2**（`ConnectorTransaction.applyWriteConstraint(ConnectorPredicate)` default-no-op，复用 P6.2-T02 `IcebergPredicateConverter`，非 `getMergeMode()` hint API）。
  2. **Route B / option (i)**：通用 `RowLevelDmlCommand` 壳 + capability 派发消路由 instanceof；iceberg `$row_id`/branch-label/投影代数**暂留 fe-core**（连接器禁 import nereids，import-gate 墙）= **有界 deviation DV-04x**，保 EXPLAIN parity。
  3. **北极星 = Trino 式 (iii) 通用化**（连接器 0 优化器 import、引擎核心全 DML 合成）→ 后续专门 RFC 关闭 DV-04x；演进触发 = hive P7/paimon 第二行级-DML 消费者。残余实现风险 = P6.3-T07（通用命令壳抽取）+ jdbc/maxcompute 框架统一 parity（T01/T02）。
- **拥有者**：P6.3 实现（T01–T09，`tasks/P6-iceberg-migration.md` §P6.3 拆解）
- **关联 task**：P6.3
- **更新日志**：
  - 2026-05-24：初始登记

---

### R-006 (R6) — 通过 SPI 性能回归

- **首次提出**：2026-05-24（master plan §6）
- **影响**：Low — < 5% 损失可接受
- **概率**：低 — 反射开销小、SPI 抽象层很薄
- **当前状态**：⏸ 未启动
- **缓解措施**：
  1. P0 末新增 benchmark：1k 个 catalog × `listTableNames` / `getSchema` 路径
  2. 接受 < 5% 性能损失
- **拥有者**：TBD
- **关联 task**：P0-T（待加）
- **更新日志**：
  - 2026-05-24：初始登记

---

### R-007 (R7) — FE/BE 共享 jar 冲突

- **首次提出**：2026-05-24（master plan §6）
- **影响**：Low — 影响特定部署
- **概率**：低
- **当前状态**：⏸ 未启动
- **缓解措施**：
  1. `plugin-zip.xml` 的 exclude 列表必须包含 BE 侧 jar
  2. 每个连接器打包后用 `unzip -l plugin.zip` 人工 review
- **拥有者**：TBD
- **关联 task**：每个 Pn 的打包子任务
- **更新日志**：
  - 2026-05-24：初始登记

---

### R-008 (R8) — 文档与流程脱节

- **首次提出**：2026-05-24（master plan §6）
- **影响**：Low — 单次延误；长期累积可能误导
- **概率**：中 — 文档维护人皆有惰性
- **当前状态**：🟢 缓解中
- **缓解措施**：
  1. 建立本跟踪机制（README / PROGRESS / 各 log / tasks / connectors）
  2. AGENT-PLAYBOOK §5 强制纪律
  3. 后续可加 `tools/check-tracking-freshness.sh` 自动检测过期
- **拥有者**：@me
- **关联 task**：跨周期
- **更新日志**：
  - 2026-05-24：跟踪机制建立后状态从 🟡 变 🟢

---

### R-009 (Q1) — `ConnectorProcedureSpec.arguments` 类型不安全

- **首次提出**：2026-05-24（RFC §16.1）
- **影响**：Med — 运行时类型错误，但 fail-fast 可接受
- **概率**：中 — connector 实现者可能用错类型
- **当前状态**：🟢 监控中
- **缓解措施**：
  1. 限定允许的类型：`String / Long / Double / Boolean / Instant / List / Map`
  2. `ConnectorProcedureSpec` 构造时校验
  3. 用 `IllegalArgumentException` 兜底（同 D-018 模式）
- **拥有者**：@me
- **关联 task**：P0-T（procedure SPI 实现时）
- **更新日志**：
  - 2026-05-24：初始登记

---

### R-010 (Q2) — `ConnectorMetaInvalidator` 异常路径 leak listener thread

- **首次提出**：2026-05-24（RFC §16.1）
- **影响**：Med — FD 泄漏 / 线程泄漏 / OOM
- **概率**：中 — 异常处理代码容易写漏
- **当前状态**：🟢 监控中
- **缓解措施**：
  1. `Connector.close()` 中必须明确停止 listener thread
  2. 加 fe-core 侧 daemon 监控：catalog 已 unregister 但 listener 线程还在 → 告警
- **拥有者**：@me
- **关联 task**：P7.2
- **更新日志**：
  - 2026-05-24：初始登记

---

### R-011 (Q3) — `ConnectorTransaction.commit` 跨 BE 分片复杂性

- **首次提出**：2026-05-24（RFC §16.1）
- **影响**：Med — 写路径事务一致性
- **概率**：低 — 已有 `ConnectorWriteOps.finishInsert(handle, fragments)` 覆盖
- **当前状态**：🟢 监控中（设计已避开此风险）
- **缓解措施**：
  1. `beginTransaction` 只负责开/关，**不负责 commit 数据**
  2. 数据 commit 通过现有 `finishInsert/finishDelete/finishMerge(handle, fragments)`
  3. 在 RFC §7.6 说明此分工
- **拥有者**：@me
- **关联 task**：P0（SPI 设计）已规避，P5-P7（实施）需复核
- **更新日志**：
  - 2026-05-24：初始登记

---

### R-012 (Q4) — `ConnectorMvccSnapshot.snapshotId` long 不适配 string-id 系统

- **首次提出**：2026-05-24（RFC §16.1）
- **影响**：Med — Delta Lake 等未来连接器无法表达
- **概率**：中 — 长期看一定会遇到
- **当前状态**：🟢 监控中（接受 v1 用 long）
- **缓解措施**：
  1. v1 用 long
  2. 未来需要时加 `String getSnapshotIdAsString()` default 方法（向后兼容）
- **拥有者**：@me
- **关联 task**：本计划范围内不处理
- **更新日志**：
  - 2026-05-24：初始登记

---

### R-013 (Q5) — `ConnectorPartitionField.transform` 字符串约定漂移

- **首次提出**：2026-05-24（RFC §16.1）
- **影响**：Low — connector 间不互认，但用户视角隔离
- **概率**：中 — 文档约束 vs 工程纪律
- **当前状态**：🟢 监控中
- **缓解措施**：
  1. RFC §19 附录 B 列出全部允许的 transform 字符串
  2. `ConnectorPartitionSpec` 构造时校验
  3. 未列出的字符串视为 `CUSTOM`，由 connector 内部识别
- **拥有者**：@me
- **关联 task**：P5-P7
- **更新日志**：
  - 2026-05-24：初始登记

---

### R-014 (Q6) — E9 的 thrift sink 选择与 connector 演化脱节

- **首次提出**：2026-05-24（RFC §16.1）
- **影响**：Low — 新 sink 类型需要 fe-core 与 connector 协同改动
- **概率**：低 — 现有 4 类 sink 覆盖大部分场景
- **当前状态**：🟢 监控中
- **缓解措施**：
  1. `ConnectorWriteConfig.properties` 留 `"thrift_sink_type"` 自定义字段
  2. `CUSTOM` ConnectorWriteType 走 generic sink 兜底
  3. 文档说明扩展机制
- **拥有者**：@me
- **关联 task**：P6.3
- **更新日志**：
  - 2026-05-24：初始登记

---

## 附录：风险模板

新增风险时复制以下模板到 §详细记录 末尾，并更新 §📋 索引和 §📋 风险矩阵。

```markdown
### R-NNN — <一句话主题>

- **首次提出**：YYYY-MM-DD（来源文档）
- **影响**：High / Med / Low
- **概率**：高 / 中 / 低
- **当前状态**：🟢 监控中 / 🟡 待启动 / 🟠 缓解中 / 🔴 已触发 / ✅ 已闭环 / ⏸ 未启动
- **缓解措施**：
  1. ...
- **拥有者**：@me / TBD
- **关联 task**：...
- **更新日志**：
  - YYYY-MM-DD：状态变化描述
```

## 状态流转图

```
[新增] ──→ 🟡 待启动 ──→ 🟢 监控中 ──→ 🟠 缓解中 ──→ ✅ 已闭环
                              ↓
                           🔴 已触发 ──→ 事故响应 + 改 mitigation
```

⏸ 未启动 = 该风险触发条件还很远（如 P6 的风险在 P0 阶段），可以晚一些指派 owner。
