# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-05-24（同日两次更新）
- **本 session 主导者**：Claude Opus 4.7（1M context）
- **本 session 主题**：建立项目跟踪机制（完整版）
- **预估 context 使用**：~70%（进入"警觉"区，已停止接新任务）

---

## ✅ 本 session 完成项

### 1. 决策闭环（前半段）
- ✅ Master plan §5 — 12 个项目决策点（D1-D12）全部按推荐确认
- ✅ SPI RFC §16.2 — 6 个未决问题（U1-U6）全部决议（U4 改批量化）

### 2. 跟踪机制建立（后半段，全部完成）
- ✅ `plan-doc/README.md` — 跟踪机制使用指南 + 文档索引
- ✅ `plan-doc/PROGRESS.md` — 全局仪表盘（阶段进度、连接器看板、活跃 task、风险监控、session 状态）
- ✅ `plan-doc/AGENT-PLAYBOOK.md` — Agent 协作规范（context 预算、subagent 使用、handoff 触发、强制纪律、anti-patterns）
- ✅ `plan-doc/HANDOFF.md` — 本文件（滚动）
- ✅ `plan-doc/decisions-log.md` — 18 条 ADR（D-001..D-018）
- ✅ `plan-doc/deviations-log.md` — 空模板（DV-NNN 待用）
- ✅ `plan-doc/risks.md` — 14 个风险条目（R-001..R-014），含状态矩阵
- ✅ `plan-doc/tasks/_template.md` — 阶段任务模板
- ✅ `plan-doc/tasks/P0-spi-foundation.md` — P0 全部 27 个子任务清单
- ✅ `plan-doc/connectors/_template.md` — 连接器跟踪模板
- ✅ `plan-doc/connectors/{jdbc,es,trino-connector,hudi,maxcompute,paimon,iceberg,hive}.md` — 8 个连接器跟踪文件
- ✅ `plan-doc/00-connector-migration-master-plan.md` 顶部加入跟踪体系入口链接

总计 **17 个文件**，220K，覆盖项目战略 + 进度 + 决策 + 风险 + 任务 + 连接器 + agent 协作 6 个维度。

---

## 🚧 本 session 进行中 / 未完成

**无**。本 session 工作完整收尾，跟踪机制已就位且自洽。

---

## 📝 关键认知 / 临时发现

（沿用上一版 HANDOFF 的认知，本次 session 未产生新代码层面发现）

1. **`fe-connector/` 反向边界当前是干净的**（0 处禁用 import）—— grep 守门脚本只需维护现状即可。
2. **`PluginDrivenExternalCatalog.gsonPostProcess` 已实现 ES/JDBC 兼容范本**（line 274-297）—— 后续连接器迁移直接复制该模式。
3. **`PhysicalPlanTranslator.visitPhysicalFileScan` line 734-790 是 7-way switch 的单点收口** —— P1 首要清理目标。
4. **`ConnectorTransactionHandle` 是 24 行空 marker 接口** —— `ConnectorTransaction` 计划继承它，不破坏现有引用。
5. **`ConnectorPartitionInfo` 已存在** —— RFC E10 复用并扩展 3 个 long 字段（向后兼容构造器）。
6. **`SPI_READY_TYPES` 白名单当前只含 `jdbc`, `es`** —— 后续连接器加入这个 ImmutableSet 即可生效。
7. **`fe-connector-hms` 是共享库不是插件** —— 无 `META-INF/services/...ConnectorProvider`，被 hive / hudi / iceberg-HMS / paimon-HMS 依赖。

### 本 session 新增认知
8. **跟踪机制的"决策 vs 偏差"区分是必须**：先前混在一起会让审查者无法判断"事前想清楚 vs 事后被现实纠正"。
9. **`AGENT-PLAYBOOK` §2.1 的 context 预算分级**对当前 session 已生效——我自己在 ~70% 时停止接新任务。后续 session 应严格执行。
10. **未来 agent 切 session 时的强制开场流程在 README §7.3 和 PLAYBOOK §4.5** —— **不读 HANDOFF 直接问"上次到哪了"是失败模式**。

---

## 🎯 下一个 session 第一件事

**两种路径，由 user 决定：**

### Track A（推荐）：开 P0 编码

第一件事：
```
1. Read plan-doc/PROGRESS.md + plan-doc/HANDOFF.md
2. Read plan-doc/tasks/P0-spi-foundation.md（找批 0 第一个 task = P0-T03）
3. Read plan-doc/01-spi-extensions-rfc.md §6（E3 MetaInvalidator 设计）
4. 实现：
   - 新建 fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorMetaInvalidator.java
   - 修改 ConnectorContext.java 加 getMetaInvalidator() default 方法
5. 编译：mvn -pl fe/fe-connector/fe-connector-spi compile
6. 完成后：
   - 更新 tasks/P0-spi-foundation.md 中 P0-T03 状态为 ✅
   - 更新 PROGRESS.md §三和§四
   - 写新 HANDOFF.md
```

### Track B：建 git commit 沉淀本次工作

第一件事：
```
1. cd /Users/morningman/workspace/git/wt-fs-spi
2. git status
3. git add plan-doc/
4. git commit -m "[plan-doc] establish project tracking system with decision/deviation/risk logs"
5. 然后进入 Track A
```

**强烈推荐 Track B → Track A**：本次 session 创建了 17 个文档但都没提交；先 commit 沉淀，否则一旦本地文件意外丢失，所有跟踪机制要重做。

---

## ⚠️ 开放问题 / 风险提示

1. **跟踪机制本身从未被实际"使用"过**——所有文件都是预期模板，实际产生 deviation / 周维护时是否好用还要看。后续 session 第一次 append decision-log 或 deviation-log 时如果发现模板缺字段，按 DV 流程改 README §3。
2. **`tools/check-connector-imports.sh` 守门脚本仍未实现**（RFC §15.4 + tasks/P0 P0-T21）—— P0 末必须完成。
3. **`maven enforcer` 接入方式未敲定**——技术决策，留 P0 实施时定。
4. **本 session 大量决策（D-001..D-018）尚未进入 git history** —— 见 Track B 推荐。
5. **本跟踪机制没有 PMC review**——单人推进风险。建议在开 P0 编码前至少让一位 reviewer 看一遍 README + AGENT-PLAYBOOK。

---

## 📂 当前 plan-doc/ 目录全景

```
plan-doc/  (220K, 17 文件)
├── 00-connector-migration-master-plan.md         ← 战略
├── 01-spi-extensions-rfc.md                      ← SPI 详细设计
├── README.md                                     ← 跟踪机制指南
├── PROGRESS.md                                   ← 全局仪表盘 ★
├── AGENT-PLAYBOOK.md                             ← Agent 协作规范 ★
├── HANDOFF.md                                    ← 本文件（滚动）
├── decisions-log.md                              ← 18 条决策
├── deviations-log.md                             ← 0 条偏差（空）
├── risks.md                                      ← 14 个风险
├── tasks/
│   ├── _template.md
│   └── P0-spi-foundation.md                      ← 27 个子任务
└── connectors/
    ├── _template.md
    ├── jdbc.md            ← 95% (P1 清理残留)
    ├── es.md              ← 100% ✅
    ├── trino-connector.md ← 30% (P2)
    ├── hudi.md            ← 20% (P3)
    ├── maxcompute.md      ← 25% (P4)
    ├── paimon.md          ← 20% (P5)
    ├── iceberg.md         ← 5%  (P6)
    └── hive.md            ← 10% (P7)
```

---

## 🧠 给下一个 agent 的 meta 建议

- 本项目所有"事实陈述"（代码行数、文件位置、import 引用关系）基于 2026-05-24 这天的 `catalog-spi-2` 分支状态。如 session 跨多天且分支有更新，先 `git log --oneline catalog-spi-2 -10` 确认 base。
- 用户偏好简洁、第一性原理、不绕弯。直接给推荐方案，等他说"这里改一下"再调整。**不要列 6 个选项让他选**——除非真的有 trade-off。
- 用户经常在工作中途插入新需求（本次 session 加了 "context 管理" 要求）——用 PLAYBOOK §2.2 的"主动报告 context 占用"应对，不要默默吞掉。
- 用户已确认 18 个决策（D-001..D-018），**不要重新打开**这些讨论，除非有强证据原决策不可行（此时走 DV 流程）。
- 本次 session 的"建立跟踪机制"是一次性投资。后续 session 不要 re-design，**只用、不改**——除非走 DV 流程明确改进。
- **必读 AGENT-PLAYBOOK 全文**再开始动手——特别是 §6 anti-patterns。
