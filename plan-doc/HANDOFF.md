# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-05-24（晚）
- **本 session 主导者**：Claude Opus 4.7（1M context）
- **本 session 主题**：P0 批 0 SPI 接口三件套实现（E3 MetaInvalidator + E4 Transaction + E5 MvccSnapshot）
- **预估 context 使用**：~50%（健康范围；§7.2 "代码实现 session" 模板预算 60%）

---

## ✅ 本 session 完成项

### 1. P0-T02 闭环（doc 维护）

上一 session 完成 17 文件跟踪机制并已 commit `63159837043`，但 `tasks/P0-spi-foundation.md` 中 P0-T02 仍是 🚧。**本场 session 翻状态为 ✅** —— 一处文档/§5.1 同步纪律的修复，无代码改动。

### 2. P0 批 0：SPI 接口三件套（T03–T08）

| ID | 任务 | 文件 | 备注 |
|---|---|---|---|
| T03 ✅ | E3 `ConnectorMetaInvalidator` 接口 | **新** `fe-connector-spi/.../spi/ConnectorMetaInvalidator.java` | 5 个 invalidate default + `NOOP` 常量；放 spi 包以便 `ConnectorContext` 引用 |
| T04 ✅ | `ConnectorContext.getMetaInvalidator()` default | edit `fe-connector-spi/.../spi/ConnectorContext.java` | 返回 `ConnectorMetaInvalidator.NOOP` |
| T05 ✅ | `ConnectorTransaction` 接口 | **新** `fe-connector-api/.../api/handle/ConnectorTransaction.java` | `extends ConnectorTransactionHandle, Closeable`；保留旧 24 行 marker 不破坏现有引用 |
| T06 ✅ | `ConnectorWriteOps.beginTransaction` default | edit `fe-connector-api/.../api/ConnectorWriteOps.java` | 默认抛 `DorisConnectorException("Transactions not supported")` |
| T07 ✅ | `ConnectorSession.getCurrentTransaction` default | edit `fe-connector-api/.../api/ConnectorSession.java` | 默认 `Optional.empty()` |
| T08 ✅ | `ConnectorMvccSnapshot` 类型 + 3 default 方法 | **新** `fe-connector-api/.../api/mvcc/ConnectorMvccSnapshot.java` + edit `fe-connector-api/.../api/ConnectorMetadata.java` | final value class + Builder；3 default：`beginQuerySnapshot` / `getSnapshotAt` / `getSnapshotById` 全返回 `Optional.empty()` |

### 3. 验证

- `mvn -pl fe-connector/fe-connector-api,spi -am clean compile -DskipTests` → **BUILD SUCCESS**（4 spi 源文件、checkstyle 0 violations）
- `mvn -pl fe-connector/fe-connector-jdbc,fe-connector-es -am compile` → **BUILD SUCCESS**（26 JDBC 源 + ES 全编译，零下游修改）
- 即 P0 验收清单中 **「JDBC、ES 现有 compile 通过」基线成立**（regression test 留 P0-T24/T25 单独验）

### 4. 文档同步（§5.1 五步纪律）

- ✅ `tasks/P0-spi-foundation.md`：T02-T08 状态翻 ✅，新增 2026-05-24（晚）日志条目
- ✅ `PROGRESS.md`：§一 P0 进度条 10% → 30%；§三 P0 表更新；§四加 2026-05-24（晚）条目；§七 session 状态滚动
- ✅ 本 HANDOFF.md 覆写
- N/A `connectors/<name>.md`（本次工作不属任何具体连接器）
- N/A `decisions-log.md` / `deviations-log.md`（本次完全遵循 RFC §6/§7/§8，未产生新决策或偏差）

---

## 🚧 本 session 进行中 / 未完成

**无**。批 0 SPI 接口 + 编译验证 + 文档同步全部收尾。

---

## 📝 关键认知 / 临时发现

继承上版 HANDOFF 7 条认知不变。**本场新增**：

1. **`fe-connector-spi` 模块 `pom.xml` 已依赖 `fe-connector-api`** —— `ConnectorContext.java` 早就 import `org.apache.doris.connector.api.ConnectorHttpSecurityHook`，所以 spi → api 方向可用。`ConnectorMetaInvalidator` 放 spi 包没有循环依赖问题。
2. **`ConnectorTransactionHandle` 与 `ConnectorTransaction` 共存策略验证可行** —— 旧 24 行 marker 保留；`ConnectorTransaction extends ConnectorTransactionHandle` 让现有所有 `*Handle` 占位代码（fe-core / fe-connector 中）无须修改即可继续编译。这条与 HANDOFF 旧认知 #4 一致，本场实操确认。
3. **Maven `build-cache-extension` 默认会按 checksum 复用 jar，可能导致"看似 BUILD SUCCESS 但实际没编译我的新源文件"** —— 验证编译时必须加 `-Dmaven.build.cache.enabled=false` 或显式 `clean`，否则会看到 `Skipping plugin execution (cached): compiler:compile` 误导。**这条要进 PLAYBOOK** —— 见下方"开放问题 #1"。
4. **`ConnectorMvccSnapshot` 用 Builder 模式** —— RFC §8.2 写的 "builder + getters" 我做成了 final class + 静态嵌套 `Builder`，符合 immutable value 类惯例。Map 用 `Collections.unmodifiableMap(new HashMap<>(...))` 拷贝。
5. **`ConnectorMetadata` 上的 3 个 MVCC default 方法**直接放在 `ConnectorMetadata` 自己（不放在子接口）——因为它们是横切关注，且 RFC §8.3 明确放这里。

---

## 🎯 下一个 session 第一件事

### 强烈建议先 review 再继续

`tasks/P0-spi-foundation.md` 注意事项 #1：**「批 0 三个 SPI 是后续所有连接器迁移的 baseline。一旦合入主线，每个连接器都开始用，调整成本急剧上升。先在批 0 完成后让用户 review，再开始批 1。」**

→ 新 session 第一步：用户人工/agent 评审本次 6 个文件（3 新 + 3 改）。如有调整建议，走 DV 流程登记。

### Track A（review 后推荐）：本次批 SPI 接口先 commit

```
1. cd /Users/morningman/workspace/git/wt-fs-spi
2. git status
3. git add fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/handle/ConnectorTransaction.java \
           fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/mvcc/ \
           fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorMetaInvalidator.java \
           fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorMetadata.java \
           fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorSession.java \
           fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorWriteOps.java \
           fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorContext.java \
           plan-doc/PROGRESS.md plan-doc/HANDOFF.md plan-doc/tasks/P0-spi-foundation.md
4. git commit -m "[P0-T03..T08][spi] add MetaInvalidator/Transaction/MvccSnapshot baseline"
```

### Track B：fe-core 桥接（P0-T09..T12）

只在 Track A commit 后启动：

```
1. Read plan-doc/PROGRESS.md + plan-doc/HANDOFF.md
2. Read plan-doc/01-spi-extensions-rfc.md §6.4 / §7.4 / §8.4
3. 找到现有 fe-core 类：
   - DefaultConnectorContext（实现 ConnectorContext）
   - ExternalMetaCacheMgr（被包装目标）
   - PluginDrivenTransactionManager（要通用化）
   - MvccSnapshot 接口（fe-core 侧）
4. 实现：
   - DefaultConnectorContext.getMetaInvalidator() override → new ExternalMetaCacheInvalidator(catalogId)
   - ExternalMetaCacheInvalidator (fe-core 新类) 包装 ExternalMetaCacheMgr 的 5 个 invalidate*
   - PluginDrivenTransactionManager 删 type-specific 分支，改用 ConnectorTransaction map
   - ConnectorMvccSnapshotAdapter (fe-core 新类) implements MvccSnapshot
5. mvn -pl fe-core -am compile（注意 fe-core 体量，预估 2-3 分钟）
6. 跑 JDBC 简单测试验证回归
7. 更新 tasks / PROGRESS / HANDOFF
```

预计 context 用量 ~70%（fe-core 体量大，会涉及多文件读）。**如果上手就觉得读 ExternalMetaCacheMgr / PluginDrivenTransactionManager 单文件超过 800 行，先 grep 定位再 offset+limit 精读**（PLAYBOOK §2.3 #1）。

---

## ⚠️ 开放问题 / 风险提示

1. **Maven build cache 误导问题**（本场新发现）：默认开启的 `build-cache-extension` 会按 input checksum 复用 jar，可能产生 "BUILD SUCCESS 但实际没编译"。建议：
   - **短期**：所有 SPI 改动验证步骤强制加 `-Dmaven.build.cache.enabled=false` 或 `clean`
   - **长期**：在 `AGENT-PLAYBOOK.md` §五新加一条纪律 / 在 P0-T22（maven enforcer 接入）任务中评估是否禁掉 cache 用于 CI
   - 暂未走 DV 登记（不是 RFC 偏差，是工具链 finding）；若决定写 PLAYBOOK 改动，走 DV 流程。
2. **本次 6 文件改动尚未 commit**（同上一 session 模式）—— 见 Track A。
3. **`PluginDrivenTransactionManager` 通用化（P0-T11）需要回归测试保证 JDBC auto-commit 不退化** —— 已在 tasks/P0 注意事项 #2 标注；下一 session 实操时务必跑 JDBC regression。
4. **`ConnectorMvccSnapshot` 的 `properties` Map 是否需要类型化字段**（如 Iceberg 用 `manifest-list-location`、`schema-id`）—— 暂用 `Map<String, String>` 通用承接，待 Iceberg/Paimon 接入时（P5/P6）发现具体需求再补强类型 / 走 DV。
5. **`getCurrentTransaction()` default 的 fe-core 侧填充逻辑**（RFC §7.6 末："fe-core 用 `ConnectorSessionImpl` 在事务期间填入"）—— 留到 P0-T11 同步实现。

---

## 📂 当前关键文件清单

### 本场新增 / 修改

```
NEW  fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorMetaInvalidator.java
NEW  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/handle/ConnectorTransaction.java
NEW  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/mvcc/ConnectorMvccSnapshot.java
MOD  fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorContext.java
MOD  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorMetadata.java
MOD  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorSession.java
MOD  fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorWriteOps.java
MOD  plan-doc/PROGRESS.md
MOD  plan-doc/tasks/P0-spi-foundation.md
MOD  plan-doc/HANDOFF.md（本文件）
```

### 跟踪体系（上场建立，沿用不变）

```
plan-doc/  (~220K, 17 文件)
├── 00-connector-migration-master-plan.md / 01-spi-extensions-rfc.md
├── README.md / PROGRESS.md / AGENT-PLAYBOOK.md / HANDOFF.md
├── decisions-log.md (18) / deviations-log.md (0) / risks.md (14)
├── tasks/{_template.md, P0-spi-foundation.md}
└── connectors/{_template.md, jdbc, es, trino-connector, hudi, maxcompute, paimon, iceberg, hive}.md
```

---

## 🧠 给下一个 agent 的 meta 建议

- **当前分支是 `catalog-spi-00`**（HANDOFF 历史版本写 `catalog-spi-2`，那是 git status 快照失准；本场 session 全程在 `catalog-spi-00` 上工作并验证）。新 session 开场 `git branch --show-current` 确认。
- 接 Track B（fe-core 桥接）前**强烈建议人工 review 本场 6 文件改动**——这是后续所有连接器迁移的 baseline，错了影响面巨大。
- 用户在 Q&A 中纠正过我"为什么只 T03/T04 不做完整批 0"——**说明用户读 task 文件比 HANDOFF 仔细，scope 判定以 tasks/Pn-*.md 为准，HANDOFF 只是建议起点**。下一 session 启动时不要再被 HANDOFF 的"下一个 session 第一件事"窄化思维误导，先看 tasks 文件确认完整 batch。
- 本场没动 RFC 一个字，无新 decision / deviation。**沿用上版 meta 建议第 4 条："不要重新打开 D-001..D-018"**。
- **必读 AGENT-PLAYBOOK §六 anti-patterns** 再开始动手（特别是 "把 RFC 当 PROGRESS 用" 和 "跨 session 凭记忆继续工作"）。
