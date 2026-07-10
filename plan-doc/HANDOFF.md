# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里，见下「起步必读」）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = catalog-spi **主线**（HMS 翻闸）。

---

# 🎯 当前状态（2026-07-10）

catalog-SPI 迁移的剩余工作 = **HMS 翻闸**。权威计划 = **`tasks/hms-cutover-execution-plan-2026-07-10.md`**（4 阶段 + DONE 账本 + 已签字决策 + 硬门；**起步必读 #1，行号信 HEAD 不信文档**）。四阶段：

- **Phase 0 连接器休眠补齐** ✅ **DONE** — 读 SPI + iceberg/hudi 兄弟委派（S0–S6 / W1–W5 / WC1–WC4）+ 整条 hudi 线（HD-A0…HD-D1 + 武装 pivot HD-B2）。
- **Phase 1 翻闸前 fe-core 建设（进行中，真正剩余主活）** — 连接器自持缓存(D2) → 事件管道 Model B → 3 个 loud-break 耦合缝 → W6 写路径 TCCL 验证。
- **Phase 2 原子翻闸**（`SPI_READY += hms` + 3 处 GSON 重映射 + 死臂/INC-5/D5 删除 + 4 守卫改接新子系统）→ **Phase 3 删除旧代码（最后做）** → **Phase 4 e2e/硬门（R-002）**。

**✅ 本轮完成 = Phase 1 首项「Hive 连接器自持缓存(D2)」6 个休眠 commit + 净室对抗复审 + 2 处复审修复**：
D2 = `f742651990d` C-a(pom+Caffeine) → `4fe55d88fab` C-b(`CachingHmsClient` 装饰器) → `7b05df6e55e` C-c(接线进 client) → `7c0ee1ffb2a` C-d(`HiveFileListingCache`) → `7bf90a7fe3c` C-e(`invalidateTable/invalidateAll`) → `12e0c9177c2` C-f(fe-core `getNewestUpdateVersionOrTime` last-modified 分支)。设计 = **`tasks/hive-connector-cache-step-design-2026-07-10.md`**。全休眠（hms 未进 `SPI_READY_TYPES`；paimon/iceberg/jdbc/hudi 字节不变；对齐 Trino 双层）。
**净室对抗复审**（9 维独立盲评 → 逐条对抗验证 → 交叉核对历史结论）= 7 疑点 2 坐实、5 误报/干净；报告 = **`plan-doc/reviews/hive-connector-cache-cleanroom-review-2026-07-10.md`**。两处坐实（用户拍板即修，均已落地）：`fda344e6022`（恢复 `FileSystem.get` 系统性失败大声报错、仅单目录失败 skip——修静默空结果回归）+ `cdc837563a7`（新增通用 `Connector.invalidateDb` SPI + 接 `RefreshManager.refreshDbInternal`，让 REFRESH DATABASE 清连接器两层缓存）。复审确认干净项：§2.6 单调性=与旧实现平价（删最新分区致 max 下降在新旧都抛，非新回归）、无空指针面、装饰器 pass-through 完整、TCCL 只在调用线程、Caffeine 单版本。**未做的低危建议**：`fe-connector-hms/pom.xml` 把 `fe-connector-cache` 标 `optional`（免那个空 Caffeine jar 流进 hudi 包，现无害但去掉未来隐患）——待用户点头。

**⭐ 下一步 = Phase 1 第二项「事件管道 Model B」**（薄 fe-core 角色驱动 + 插件 `pollOnce` SPI，翻闸后重新武装 `MetastoreEventsProcessor` 的增量失效，权威 = `tasks/hms-event-pipeline-findings-2026-07-07.md`）。它**依赖** D2 缓存（在其之上加 `Connector.invalidatePartitions(db,table,names)`）。起步先读 findings + 对照 HEAD。

**⚠ 翻闸/e2e 欠账（非静默，勿丢）**：所有连接器休眠步（读 / 写-拒 / schema-evolution / 缓存 / 跨加载器委派）只在翻闸后 live，须异构 HMS docker e2e 断言（清单见 execution plan §4/§5 + memory `hms-iceberg-delegation-needs-e2e`）。**删除排序最硬约束**：`datasource/hive|hudi|iceberg/` 的 ~90 个 HMS 支撑类删不掉，直到翻闸把消费者切到连接器路径（详见 execution plan §2.4/§3 + `tasks/iceberg-on-hms-delegation-findings-2026-07-07.md`）。

---

# 🧠 起步必读（读文档，别炒 git log 历史）

1. **权威翻闸计划** = `tasks/hms-cutover-execution-plan-2026-07-10.md`（4 阶段 + DONE 账本 + 已签字决策 + 原子翻闸集 + 硬门）。**SUPERSEDES** 07-07 doc §4/§5 的状态。
2. **本步（Hive 缓存）设计** = `tasks/hive-connector-cache-step-design-2026-07-10.md`（TODO 全勾）。
3. **补充权威**：`tasks/hms-cutover-retype-design-2026-07-07.md`（原子翻闸模型 + 能力孪生 + 已签字 §6 D1–D6 决策，仍有效）；`tasks/hms-cutover-sibling-connector-decomposition-2026-07-08.md`（兄弟委派 S0–S6 + CCE/TCCL 硬约束）；两份 findings（`iceberg-on-hms-delegation-findings-2026-07-07.md` + `hms-event-pipeline-findings-2026-07-07.md`）；hudi = `tasks/hudi-on-hms-delegation-plan-2026-07-08.md` + `tasks/hudi-schema-evolution-step-design-2026-07-09.md`（均 DONE）。
4. **样板**：`tasks/P5-paimon-migration.md`（翻闸+删 legacy 全流程）、`tasks/P6-iceberg-migration.md`（净室复审 + 能力孪生 + GSON replay 范式）。
5. 完成工作明细 = `git log`（commit message 详尽）；勿在 HANDOFF 里重述。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi`）。PR base = `branch-catalog-spi`，**squash 合并**。打包/复审策略（单 PR vs 分 PR）= 翻闸阶段的开放决策。
- **公开 tracking issue = apache/doris#65185**（进度按已合入 `branch-catalog-spi` PR 口径）。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**：工作树有大量历史遗留 scratch（`*.bak` / `regression-test/conf/regression-conf.groovy` 明文 key / `.audit-scratch/` / `conf.cmy/` / `META-INF/` / `docker/...` / `plan-doc/reviews/P5-*` / `.claude/` / `failed-cases.out`——**非本线程产物，勿混入任何 commit**）。
- commit message：`[feat|fix|doc](catalog) …` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。**每子步/每条 fix = 独立 commit**；HANDOFF + 设计文档单独 commit（与 code 分开）。上下文超 30% 找干净节点交接（memory `session-handoff-at-30pct-context`）。

# ⚙️ 操作须知（构建/测试，复用）

- maven：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-connector/<mod> **-am** test -Dmaven.build.cache.enabled=false`（**漏 `-am`→假错 `${revision}`**；.m2 里有坏 `${revision}` 目录）。单测加 `-Dtest=<Class>`。禁 build-cache 才真跑 surefire。
- **checkstyle 单独跑**：`mvn -o ... -pl fe-connector/<mod> -am checkstyle:check`（module `test` 阶段已含 checkstyle，BUILD SUCCESS 即过；仍建议显式确认「0 violations」——Rule 12）。
- **import gate**（连接器禁 import fe-core）：`bash tools/check-connector-imports.sh`（**repo 根**跑）。**HMS `HiveVersionUtil` 命中 = 误报非违规**（memory `catalog-spi-hms-hiveversionutil-gate-false-positive`）。
- **⚠️ bash 工具默认 timeout 120s**：fe-core 全量编译 ~2–3min、`-am` 更久 → `timeout` 调到 ~580000+ 或后台跑；**后台/管道 exit 不可信**——读 LOG 的 `BUILD SUCCESS` + surefire `Tests run=/Failures=/Errors=`（memory `doris-build-verify-gotchas`）。
- **连接器测试无 Mockito**（真 recording fake）；**fe-core 测有 Mockito**（`Mockito.mock(接口)` **不跑 default 方法**返 null；`mockStatic(Env)` 是本仓惯用法）。checkstyle **禁 static import**、**扫 test 源**（memory `catalog-spi-fe-core-test-infra`）。
- **cwd 会被 harness 重置** → 一律绝对路径。**⚠️ `/mnt/disk1` 盘紧（~82%）；勿用 worktree 隔离编译 agent**（复制整仓，盘不够）。

# 🔒 铁律（fe-core 约束，翻闸靠"表类=通用类 + 网关按句柄委派"）

- fe-core **不得**新增 `if(hive/iceberg/hudi)` / `instanceof HMSExternal*` / `switch(dlaType)` / 引擎名判别；通用 SPI 节点保持 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）。
- fe-core **不解析属性**（storage→fe-filesystem、meta→fe-connector，均插件侧；memory `catalog-spi-no-property-parsing-in-fecore`）。
- 跨插件/跨边界**须 pin TCCL** 到连接器 classloader（扫描线程 / 写-DDL 引擎线程 / iceberg 内部 worker 池 / 事件轮询后台线程；memory `catalog-spi-plugin-tccl-classloader-gotcha`）。
- `history_schema_info` 嵌套字段名逐层 lowercase（否则 BE SIGABRT；memory `catalog-spi-history-schema-info-lowercase-nested-names`）。
- `PluginDrivenMvccExternalTable` 是 paimon/iceberg **实时**基类：改其共享方法须对二者字节+成本双不变（memory `plugindriven-mvcc-table-is-live-not-dormant`）。

# 🗂 memory 相关项

`handoff-discipline-per-phase` · `clean-room-adversarial-review-pref` · `ask-user-explain-in-chinese-first` · `session-handoff-at-30pct-context` · `doris-build-verify-gotchas` · `catalog-spi-fe-core-test-infra` · `catalog-spi-tracking-issue` · `hms-iceberg-delegation-needs-e2e` · `catalog-spi-connector-cache-framework-caffeine-coherence` · `memory-keep-only-general-or-requested`。
