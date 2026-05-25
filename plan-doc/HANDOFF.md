# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-05-24（夜 ③）
- **本 session 主导者**：Claude Opus 4.7（1M context）
- **本 session 主题**：P0 批 2 守门 + 单测（T21-T23, T26-T27；T24-T25 转交用户在本地跑）—— **已 commit**（用户人工 review 通过；hash 见 `git log --oneline -3`，subject `[feat](connector) add P0 batch 2 gate + unit tests (T21-T23, T26-T27)`）
- **预估 context 使用**：~55%（健康）

---

## ✅ 本 session 完成项

### 1. P0 批 2：守门 + 单测（T21-T23, T26-T27）

| ID | 任务 | 文件 | 备注 |
|---|---|---|---|
| T21 ✅ | `tools/check-connector-imports.sh` | **新** `tools/check-connector-imports.sh` | grep 守门；接受可选 ROOT 参数；正负冒烟均通过 |
| T22 ✅ | exec-maven-plugin 接入脚本 | edit `fe-connector/pom.xml` | 绑 `validate` 阶段；`inherited=false`；用 `${project.basedir}/../../tools/...` 避开 `fe.dir` 解析时序 |
| T23 ✅ | `FakeConnectorPlugin` + 默认行为测试 | **新** `fe-core/src/test/java/.../connector/fake/{FakeConnectorPlugin,FakeConnectorPluginTest}.java` | 11 个 @Test；零 override 的 `FakeMetadata` 验证所有 default 路径 |
| T24 ⏳ | JDBC regression-test | — | **转交用户**在本地跑 |
| T25 ⏳ | ES regression-test | — | **转交用户**在本地跑 |
| T26 ✅ | `ExternalMetaCacheInvalidator` 路由测试 | **新** `fe-core/src/test/java/.../connector/ExternalMetaCacheInvalidatorTest.java` | 5 个 @Test；`MockedStatic<Env>` + `mock(ExternalMetaCacheMgr)`；pin partition fallback & stats no-op |
| T27 ✅ | converter 单测 | **新** `fe-core/src/test/java/.../connector/ddl/CreateTableInfoToConnectorRequestConverterTest.java` | 7 个 @Test；`mock(CreateTableInfo)` 绕开 18-arg ctor；4 partition style + 2 bucket + 列穿透 |

### 2. 验证

- `tools/check-connector-imports.sh` 正/负冒烟测试通过
- `mvn -pl fe-connector validate -Dmaven.build.cache.enabled=false` → **BUILD SUCCESS**（exec-maven-plugin 调起脚本）
- `mvn -pl fe-core -am test -Dtest='FakeConnectorPluginTest,ExternalMetaCacheInvalidatorTest,CreateTableInfoToConnectorRequestConverterTest,ConnectorPluginManagerTest,ConnectorSessionImplTest' -DfailIfNoTests=false -Dmaven.build.cache.enabled=false` → **39/39 tests green**
- `mvn -pl fe-core checkstyle:check` → **0 violations**

### 3. 文档同步（§5.1 五步纪律）

- ✅ `tasks/P0-spi-foundation.md`：T21-T23, T26-T27 状态翻 ✅；T24-T25 owner 改 @用户；新增 2026-05-24（夜 ③）日志条目（含 4 项 trade-off 说明）；顶部验收清单 5 项翻 [x]
- ✅ `PROGRESS.md`：§一 P0 进度条 74% → 93%；§三 P0 表追加批 2 7 行；§四加 2026-05-24（夜 ③）条目；§七 session 状态滚动
- ✅ 本 HANDOFF.md 覆写
- N/A `connectors/<name>.md`（本场不属任何具体连接器）
- N/A `decisions-log.md` / `deviations-log.md`（trade-off 都在 RFC §15 范围内，未升 DV）

### 4. Commit（用户人工 review 通过后）

- ✅ `[feat](connector) add P0 batch 2 gate + unit tests (T21-T23, T26-T27)`（hash 见 `git log --oneline -3`）
- 9 files changed：1 个 pom edit（fe-connector）+ 5 个新文件（1 脚本 + 4 测试相关）+ 3 个 plan-doc 更新
- 工作树 clean

---

## 🚧 本 session 进行中 / 未完成

- **T24/T25**：JDBC + ES regression-test 转交用户在本地跑（containers / docker 在本地更稳）。任务状态保持 ⏳，owner 改为 @用户。完成后用户在 PROGRESS / tasks 上翻 ✅ 即可
- **本 HANDOFF 在 commit 内**——内容写的是 post-commit 状态，与 batch 2 代码、plan-doc 更新一并 commit。不需要后续 amend

---

## 📝 关键认知 / 临时发现

继承上版认知不变。**本场新增**：

1. **maven-enforcer-plugin 不能原生 exec shell**——RFC §15.4 原文写"挂到 maven enforcer plugin"，但 enforcer 只有 `requireXxx` 系列 rule 和 `EvaluateBeanshell`，没有内置的 shell-exec rule。要么写 Java 自定义 Rule 类（重）要么走 `EvaluateBeanshell`（不直观）。**最终选择 `exec-maven-plugin`**——fe-common 已用它跑 make + protoc，零新依赖；脚本 non-zero exit 即触发 `BUILD FAILURE`，效果等价
2. **`directory-maven-plugin` 的 `fe.dir` 属性在 `validate` 阶段还没 set**——它绑 `initialize` 阶段（晚于 validate）。第一次写 pom 用了 `${doris.home}/tools/...`（`doris.home=${fe.dir}/../`），结果路径解析为字面值 `${fe.dir}/..//tools/...`。改用 `${project.basedir}/../../tools/...`（fe-connector aggregator basedir → workspace root → tools）避开属性时序问题
3. **exec-maven-plugin 在 aggregator pom 的继承默认是 `inherited=true`**——会让 11 个 fe-connector-* 子模块每次都重跑同一份扫描。本场设 `inherited=false`，只在 aggregator 自身 lifecycle 跑一次。Trade-off：dev 跑单个子模块 `mvn -pl fe-connector/fe-connector-iceberg compile` 时不会自动触发守门，但顶层 `mvn install` 必扫
4. **`ConnectorMetaInvalidator` 的方法名是 `invalidateAll()` 不是 `invalidateCatalog()`**——第一稿测试写错卡了一次 test-compile。SPI 接口侧明确写 `invalidateAll`（"Invalidates the entire catalog's metadata caches"），fe-core 侧 `ExternalMetaCacheInvalidator.invalidateAll() → mgr.invalidateCatalog(catalogId)` 这才是路由
5. **`Mockito.mockStatic(Env.class)`** 模式在 fe-core 已有先例（`BDBDebuggerTest:115`），mockito-inline 是 fe 顶层 pom 已声明的 test dep，新测试可以直接用，无需修改任何 pom
6. **`Mockito.mock(CreateTableInfo.class)`** 比真正构造 18-arg `CreateTableInfo` 更便捷——converter 只读 8 个 getter，全部 stub 即可。如未来 converter 用到更多 getter，在 `stubInfo` helper 加新 stub
7. **`mvn -pl fe-core test` 不带 `-am` 失败**（缺 fe-grpc / fe-filesystem-* 等本地未 install 的 SNAPSHOT）。本场所有 fe-core 测试运行都用 `mvn -pl fe-core -am test -Dtest=... -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`。`-DfailIfNoTests=false` 是必须的——`-am` 会带上 fe-foundation 等 upstream，它们没有匹配 `-Dtest=` 的测试就会爆 surefire 错
8. **fe-connector 模块当前 import 现状**：`grep -rEn "^import org\.apache\.doris\." fe/fe-connector/*/src/main/java | awk` → 仅 4 个根包 `connector / extension / thrift / trinoconnector`。所有禁词包（catalog/common/datasource/qe/analysis/nereids/planner）都被守门，baseline 已经合规

---

## 🎯 下一个 session 第一件事

### Track A：等 T24/T25 收尾

```
1. 用户跑完 JDBC + ES regression-test 后
2. tasks/P0-spi-foundation.md 把 T24/T25 翻 ✅
3. PROGRESS.md 进度条 93% → 100%；状态 🚧 → ✅
4. 写 P0 阶段收尾 commit（如果 T24/T25 有微调代码）
```

### Track B：选 P0 末加项 vs 直接进 P1

- **选项 B1**：P0-T28 benchmark（R-006 缓解，1k catalog × `listTableNames` 性能基线）。原列入 P1，可前置到 P0 末加，让 P0 出阶段干净
- **选项 B2**：直接进 P1（scan-node 收口 + 重复清理）。P0 既然 93% 接近收尾，T24/T25 跑完即关阶段
- 推荐 B2（B1 在 P1 阶段开题更自然，benchmark 跟 scan-node 工作正好同期）

### ~~Track C：commit 批 2~~（已收尾）

批 2 已合入 `catalog-spi-00`；无需再开 Track C。

---

## ⚠️ 开放问题 / 风险提示

继承上版 7 项不变（删了"未 commit batch 1"项；增加本场 trade-off）：

1. **守门挂 `exec-maven-plugin` 而非 `maven-enforcer-plugin`**：RFC §15.4 原文写后者。本场用前者（等价实现，0 新依赖）。是否在 RFC §15.4 加脚注说明这个偏差？**判断**：trade-off 在 RFC 范围内，不升 DV；若有 reviewer 强烈要求 enforcer 写 Java Rule 类再重做
2. **守门 `inherited=false`**：dev 跑单连接器 `mvn -pl fe-connector/fe-connector-iceberg compile` 时不会触发。是否要改 `inherited=true`？**判断**：现状没人手动跑这条命令日常迭代，重复扫的成本（11 × ~50ms）也不大；如未来某个连接器开发体感差再改
3. **`invalidatePartition` 测试 pin 当前 fallback**：一旦 SPI 在该方法签名上加 column 名携带能力，bridge 和测试必须同步更新。测试已留 inline comment 描述意图
4. **`CreateTableInfo` 用 mock**：converter 改用 mock 之外的 getter 时，需在 `stubInfo` helper 加新 stub。Trade-off：测试更聚焦但代价是输入对象不"真实"
5. **partition 风格的 IDENTITY vs TRANSFORM 判别**：测试覆盖了"全 UnboundSlot → IDENTITY"和"含 UnboundFunction → TRANSFORM"两路径，但没覆盖"UnboundSlot + UnboundFunction 混合"——按 converter 当前实现，只要有任意一个 UnboundFunction 就走 TRANSFORM 路径，UnboundSlot 在 `convertFields()` 里也会被识别为 `identity` transform。这个混合场景的语义是否符合预期？**判断**：RFC §4.2 未明确混合用法，留待 P5/P6 Iceberg 真正用到时评估
6. （沿用）`ColumnDefinition.defaultValue` SPI 缺位
7. （沿用）LIST/RANGE `initialValues` flatten 缺位
8. （沿用）`PluginDrivenExternalCatalog.createTable` 返回值丢失"已存在"信息
9. （沿用）bucket 算法名 `"doris_default"` / `"doris_random"` 占位
10. （沿用）Maven build cache 误导问题；`mvn -pl fe-core` 必须 cwd=`fe/`
11. （沿用）`PluginDrivenTransactionManager.begin(ConnectorTransaction)` 暂无 caller
12. （沿用）`invalidatePartition` fallback；`invalidateStatistics` no-op
13. （沿用，本场强化）**`mvn -pl fe-core test` 不带 `-am` 失败**：必须 `-am -DfailIfNoTests=false`

---

## 📂 当前关键文件清单

### 本场新增 / 修改（已 commit）

```
NEW  tools/check-connector-imports.sh                                            (gate script)
MOD  fe/fe-connector/pom.xml                                                     (exec-maven-plugin)
NEW  fe/fe-core/src/test/java/org/apache/doris/connector/fake/FakeConnectorPlugin.java
NEW  fe/fe-core/src/test/java/org/apache/doris/connector/fake/FakeConnectorPluginTest.java        (11 tests)
NEW  fe/fe-core/src/test/java/org/apache/doris/connector/ExternalMetaCacheInvalidatorTest.java    (5 tests)
NEW  fe/fe-core/src/test/java/org/apache/doris/connector/ddl/CreateTableInfoToConnectorRequestConverterTest.java  (7 tests)
MOD  plan-doc/PROGRESS.md
MOD  plan-doc/tasks/P0-spi-foundation.md
MOD  plan-doc/HANDOFF.md（本文件）
```

### 跟踪体系（沿用不变）

```
plan-doc/  (~225K, 17 文件)
├── 00-connector-migration-master-plan.md / 01-spi-extensions-rfc.md
├── README.md / PROGRESS.md / AGENT-PLAYBOOK.md / HANDOFF.md
├── decisions-log.md (18) / deviations-log.md (0) / risks.md (14)
├── tasks/{_template.md, P0-spi-foundation.md}
└── connectors/{_template.md, jdbc, es, trino-connector, hudi, maxcompute, paimon, iceberg, hive}.md
```

---

## 🧠 给下一个 agent 的 meta 建议

- **当前分支是 `catalog-spi-00`**。新 session 开场 `git branch --show-current` 确认
- **批 2（T21-T23, T26-T27）已合入 `catalog-spi-00`**（subject `[feat](connector) add P0 batch 2 gate + unit tests (T21-T23, T26-T27)`），无需 review 老代码；直接读最新源即可。如果对 6 个新/改文件有调整建议，走 DV 流程登记后再改，不要 silent edit
- **T24/T25 owner 是用户**，不要自己尝试跑 docker regression-test
- **Maven build 的 cwd 必须是 `fe/`**，不是 workspace 根；`mvn -pl fe-core` 需要 `-am`；运行 `-Dtest=` 时务必带 `-DfailIfNoTests=false`，否则 upstream 模块（fe-foundation 等）找不到匹配 test 会爆 surefire 错
- 本场没产生新 decision / deviation——所有 trade-off 在 RFC §15 范围内，由代码注释 + 本 HANDOFF "开放问题" 列出
- 本场用 `Mockito.mockStatic` + `Mockito.mock(CreateTableInfo)` 两个套路绕开了重度 fe-core bootstrap——批 1 的 `CreateTableInfoToConnectorRequestConverter` 同样可以这样测，套路通用。后续 P1/P2 写 unit-test 可以复用
- **必读 AGENT-PLAYBOOK §六 anti-patterns** 再开始动手
- **本 HANDOFF 不内嵌 commit hash**——hash 通过 `git log --grep="P0 batch 2"` 或 `git log --oneline -3` 定位。本场无 amend，HANDOFF 与代码同 commit 落盘
