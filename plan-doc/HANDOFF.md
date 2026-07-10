# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里，见下「起步必读」）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = catalog-spi **主线**（HMS 翻闸）。

---

# 🎯 当前状态（2026-07-10）

**⭐ 本轮 = Phase 2 原子翻闸完成：catalog 类型 `hms` 已接入插件 SPI，所有生产流程改走新连接器代码（plain-hive + iceberg-on-HMS + hudi-on-HMS）。未删任何旧代码（Phase 3 才删）。** 4 个独立 commit（均构建+靶向单测验证；净室对抗复核见下）：

- **✅ A** `1af7f063c2d`：事件同步·跟随者游标改接新驱动。`ExternalMetaIdMgr.replayMetaIdMappingsLog` 原无条件喂旧 `MetastoreEventsProcessor`；改双臂——`PluginDrivenExternalCatalog`→新 `MetastoreEventSyncDriver`，否则旧 processor（都按 catalogId keying，不 cast HMS）。翻闸前休眠。**不改则翻闸后从库增量元数据同步静默永久停摆。**
- **✅ B** `eef6fe7b8c2`：事件同步·主/从强制初始化钩子。`MetastoreEventSyncDriver.realRun` 对未初始化目录按 `getType()=="hms"`（读 catalogProperty，不触发 init）强制 `makeSureInitialized()`，每 FE（无 isMaster 门，从库也要）、`!isInitialized()` 一次性、异常吞掉。翻闸前休眠。**不改则从没被查过的翻闸 hms 目录永不同步。**
- **✅ C（原子翻闸本体）** `fb1624be757`：`CatalogFactory` SPI_READY_TYPES += `"hms"` + 删死 `case "hms"`+import；`GsonUtils` 三处 registerSubtype→registerCompatibleSubtype（catalog→PluginDrivenExternalCatalog、db→PluginDrivenExternalDatabase、**table→PluginDrivenMvccExternalTable**，hive 连接器声明 SUPPORTS_MVCC_SNAPSHOT，对齐 paimon/iceberg）+ 删 3 个 orphaned import；`ExternalCatalog.buildDbForInit` HMS 臂→PluginDrivenExternalDatabase（镜像 ICEBERG 臂）；新增 `HmsGsonCompatReplayTest`（3 绿）；**禁用 3 个遗留测试**（`HmsCatalogTest`/`HmsQueryCacheTest`/`HiveDDLAndDMLPlanTest`——它们经路由 create 建 type=hms 目录、fe-core 测试无 hms 插件→抛错，且断言遗留 HMSExternal* 行为；`@Disabled` 指向 Phase 3）。
- **✅ D（D5 视图收尾）** `320702b8166`：翻闸后 hive 视图=PLUGIN_EXTERNAL_TABLE 走共享插件视图臂。删 `enable_query_iceberg_views` 门（视图无条件服务）+ 中立化 “iceberg view not supported…”→“view not supported with snapshot time/version travel” + 两个 @ConfField 标记 `@Deprecated` no-op + 改 iceberg 视图回归 16 处断言。**对已上线 iceberg 是可见行为变更，随翻闸一起发。**

**已签字决策（用户 2026-07-10）**：① 3 个遗留测试=禁用+延后 Phase 3 删除（非改造）；② D5 视图收尾=本轮一起做。

**验证**：`mvn -pl fe-core -am test-compile` BUILD SUCCESS + 0 checkstyle；靶向 `mvn test` 17 跑 0 败 0 错 1 skip（HmsGson 3/3、Iceberg/PaimonGson 各 3/3、ExternalMetaCacheRouteResolver 7/7、HmsCatalog skip）。净室对抗复核（`wf_728cad25-62a`，4 独立审查 + 逐条对抗验证）= **CLEAN**（1 发现 0 确认缺陷；唯一 minor=翻闸 hive 表失去 Nereids SQL 结果缓存资格 @ `BindRelation:729`，经验证 by-design：fail-safe、`enable_hive_sql_cache` 默认关、与 paimon/iceberg-native 一致、被禁用的 `HmsQueryCacheTest` 已明记该遗留缓存 dead-for-hms）。

**⭐ 下一步（下个 session）：**
1. **e2e 回归（用户自跑，勿丢，非静默）**：异构 `type=hms` 目录跑 hive/iceberg-on-HMS/hudi-on-HMS 读/写/DDL/procedure/MTMV/time-travel/@incr；**从库事件同步陈旧**（新跟随者游标喂路从未在任何目录跑过，hms 是第一个事件消费者）；Kerberos-HMS 冒烟；升级镜像 GSON replay；耦合缝行（partition_values/hudi_meta/sample-analyze/auto-analyze）；hive 视图（现无条件服务）。完整矩阵：`hms-cutover-execution-plan-2026-07-10.md` §4/§5 + `hms-spi-cutover-flip-2026-07-10.md` §5。
2. **Phase 3 删除旧代码（最后做）**：~90 类循环单元（`datasource/hive|hudi|iceberg`）+ 死 Nereids 臂（PhysicalPlanTranslator HMS/hudi 臂、INC-5 stale throws、CheckPolicy hudi 臂、遗留 BindRelation HMS_EXTERNAL_TABLE 臂）+ 删除解锁抽取（HiveUtil/HiveSplit/IcebergUtils）+ 那 3 个 @Disabled 测试。拓扑顺序+清单：execution plan §2.4/§3/§4。

**⚠ 关键纠正（execution plan §3 已过时，本轮已核实纠正，见 `hms-spi-cutover-flip-2026-07-10.md` §2）**：§3.7「rewire 4 个 gate」**错**——两个 instanceof gate（MetastoreEventsProcessor:116、ExternalMetaCacheRouteResolver:66）须**保留**（自动排除翻闸目录、对未翻闸旧目录仍正确；删则破坏旧目录同步/失效）；缓存路自动接管（连接器 CachingHmsClient），事件路靠上面 A/B 两个 ADD-feed（非删 gate）。死 Nereids 臂**翻闸不删**（对齐 iceberg 翻闸留死臂的先例，Phase 3 统删）。

**⚠ 并行 session 风险**：起步先查 `git log`/`git status`/运行中 maven/近 90s mtime 再动手（memory `concurrent-sessions-shared-worktree-hazard`）。

---

# 🧠 起步必读（读文档，别炒 git log 历史）

> **路径**：设计/计划在 `plan-doc/tasks/`，复审报告在 `plan-doc/reviews/`。

1. **本轮翻闸设计（下个 session 起步必读）** = `plan-doc/tasks/hms-spi-cutover-flip-2026-07-10.md`（做了什么 + 对 execution plan 的纠正 + instanceof 全分类 + 验证 + e2e 欠账 + Phase 3 清单）。**行号信 HEAD 不信文档。**
2. **权威翻闸计划（历史，§3 清单本轮已纠正）** = `plan-doc/tasks/hms-cutover-execution-plan-2026-07-10.md`（4 阶段 + DONE 账本 + 硬门；其 §2「Phase 1 未建」已过时，Phase 1 早已 DONE）。
3. **样板**：`plan-doc/tasks/P5-paimon-migration.md`、`P6-iceberg-migration.md`（净室复审 + 能力孪生 + GSON replay 范式；iceberg 翻闸=加 SPI_READY + GSON compat + 留死臂到 Phase 3）。
4. 完成工作明细 = `git log`（commit message 详尽）；勿在 HANDOFF 里重述。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi`）。PR base = `branch-catalog-spi`，**squash 合并**。
- **公开 tracking issue = apache/doris#65185**（进度按已合入 `branch-catalog-spi` PR 口径）。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**：工作树大量历史遗留 scratch（`*.bak` / `regression-conf.groovy` 明文 key / `.audit-scratch/` / `conf.cmy/` / `META-INF/` / `docker/...` / `plan-doc/reviews/P5-*` / `.claude/` / `failed-cases.out`——**非本线程产物，勿混入任何 commit**）。
- commit message：`[feat|fix|doc](catalog) …` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。**每子步/每条 fix = 独立 commit**；HANDOFF + 设计文档单独 commit（与 code 分开）。上下文超 30% 找干净节点交接（memory `session-handoff-at-30pct-context`）。

# ⚙️ 操作须知（构建/测试，复用）

- maven：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile -Dmaven.build.cache.enabled=false`（**漏 `-am`→假错 `${revision}`**）。连接器：`-pl :fe-connector-<mod> -am`。**靶向单测**加 `-Dtest=<Class> -DfailIfNoTests=false`（`-am` + `-Dtest` 会因上游模块无匹配测试报 “No tests were executed!” 假失败）。
- **⚠️ paimon 模块必须用 `install`（不是 `test`）验证**（shade jar 绑 `package` 阶段）；hms/hive 无此坑。
- **验证信 LOG 不信 exit**：后台 task 通知的 exit code 是 wrapper 的（本轮见过 rc=1 但通知报 exit 0）；重定向到文件跑（不加 `-q`），grep `BUILD SUCCESS`/`BUILD FAILURE`/`[ERROR].*\.java:`/`Tests run:`/`You have N Checkstyle`（memory `doris-build-verify-gotchas`）。
- **⚠️ bash 默认 timeout 120s**：全量编译 ~6min → 后台跑 + 读 LOG。**⚠️ `/mnt/disk1` 盘紧；勿用 worktree 隔离编译 agent**。cwd 会被重置 → 绝对路径。
- **连接器测试无 Mockito**（真 recording fake）；**fe-core 测有 Mockito**。checkstyle 禁 static import、扫 test 源、`UnusedImports` 会 fail build。

# 🔒 铁律（fe-core 约束）

- fe-core **不得**新增 `if(hive/iceberg/hudi)` / `instanceof HMSExternal*` / `switch(dlaType)` / 引擎名判别；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）。本轮 B 的 `getType()=="hms"` 门是**事件源类型探测（对齐旧 poller 的 instanceof HMSExternalCatalog）**，非源判别式违规。
- fe-core **不解析属性**（storage→fe-filesystem、meta→fe-connector；memory `catalog-spi-no-property-parsing-in-fecore`）。
- 跨插件/跨边界**须 pin TCCL**（memory `catalog-spi-plugin-tccl-classloader-gotcha`）。
- `history_schema_info` 嵌套字段名逐层 lowercase（memory `catalog-spi-history-schema-info-lowercase-nested-names`）。
- `PluginDrivenMvccExternalTable`/`PluginDrivenExternalTable` 是 paimon/iceberg/**翻闸后 hms** 实时基类（memory `plugindriven-mvcc-table-is-live-not-dormant`）。

# 🗂 memory 相关项

`handoff-discipline-per-phase` · `clean-room-adversarial-review-pref` · `ask-user-explain-in-chinese-first` · `session-handoff-at-30pct-context` · `doris-build-verify-gotchas` · `catalog-spi-fe-core-test-infra` · `catalog-spi-plugindriven-no-source-specific-code` · `plugindriven-mvcc-table-is-live-not-dormant` · `catalog-spi-tracking-issue` · `hms-iceberg-delegation-needs-e2e` · `concurrent-sessions-shared-worktree-hazard` · `memory-keep-only-general-or-requested`。
