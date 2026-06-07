# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期**：2026-06-07（第 5 次 handoff）
- **本 session 主题**：落地 **P4-T06d 翻闸缺口修复 6/6**（FIX-DDL-REMOTE / FIX-PART-GATES / FIX-WRITE-ROWS 本 session 完成；READ-DESC / READ-SPLIT / DDL-ENGINE 前序 session）。每 issue：设计→实现→编译+UT+mutation→对抗 review 收敛→独立 commit + hash 回填 commit。
- **分支**：`catalog-spi-05`（本地,未 push）。P4-T06d 共 12 commit（见 `git log --oneline | grep P4-T06d`）。HEAD = `e89ce146cee`。
- **已验**：fe-core UT 全绿 / Checkstyle 0 / 每处改动 mutation 自证。**未验**：live e2e（真实 ODPS,CI 默认跳,需凭证人工跑）= 翻闸真正完成门。
- **状态详情**：见 `tasks/task-list.md`(P4-T06d 进度表 + 累计结论) 与各 `tasks/designs/P4-T06d-*.md` / `reviews/P4-T06d-*-review-rounds.md`。

---

# 🎯 下一 session = MaxCompute 全路径「clean-room 对抗复审」

> **用户指令(原话)**：采用对抗 review 的方式,再一次分别 review MaxCompute 的各个功能路径(1.读取 2.写入 3.DDL 4.元数据回放 5.元数据 cache 6.是否还有走旧逻辑/fallback 到旧逻辑的地方)。**目的**:重新审阅所有流程,从**设计与实现交付**两面检查是否有问题,并**对照原先(legacy)逻辑看差异**。
> **⚠️ 用户硬约束**：这轮 review **不要注入开发过程中已有的先验知识**,以避免历史记忆限制 review 的公正性与开放性。

## 🔒 Clean-room 铁律(本轮 review 的第一原则)

1. **Phase A/B 审阅者只读代码**:`fe/`、`be/`、`gensrc/` 的**源码**(cutover 实现 + legacy 基线 + BE),**禁止**读任何 `plan-doc/`(包括本 HANDOFF 的「先验隔离区」、所有 `designs/`、`reviews/`、`decisions-log`、`deviations-log`、`PROGRESS`)与 auto-memory。理由:这些携带开发结论,会预设"已修/正确",污染独立判断。
2. **结论从代码自证**:每条发现须 `file:line` 证据,并明确 **cutover vs legacy 的可观察行为差异**(回归 yes/no/unsure)。"设计声称 X" 不算证据——验代码是否真做了 X。
3. **历史只在 Phase C 解禁**:形成独立结论后,才允许对照「先验隔离区」做交叉核对——判定每条发现是 (i) 开发遗漏的**真新问题** / (ii) 已登记的已知降级 / (iii) 已正确处理(发现有误) / (iv) **与历史结论分歧**(须显式 surface)。
4. **复审对象含 P4-T06d 的修复本身**:本轮要以"全新眼光"看**当前代码**(已含 6 处修复),不预设它们正确。是否做过修复、是否 mutation-proven,都是先验,Phase A/B 不告知审阅者。
5. **正交多视角**:每个功能路径派多个独立 lens,彼此盲审;对每条发现派 ≥3 个 refute-by-default 验证者(≥2 票存活)。参 [[clean-room-adversarial-review-pref]] 的 Phase A→B→C→D 结构(本 session 已用此法多次,workflow 脚本可复用)。

## 🧭 复审方法(建议 workflow 编排)

> **✅ workflow 脚本已预写**:`plan-doc/reviews/maxcompute-full-rereview.workflow.js`(已 syntax-check)。下一 session 直接:
> `Workflow({ scriptPath: "plan-doc/reviews/maxcompute-full-rereview.workflow.js" })`(可选 `args: { verifyVotes: 3, lensesPerDomain: 2, includeBe: true }`)。
> 脚本已内建 clean-room 纪律:Phase A/B 子 agent 的 prompt **只含 6 域的代码指针 + cutover↔legacy 对照**,显式禁读 plan-doc;先验仅在 Phase C(脚本内 `QUARANTINE` 常量)解禁做交叉核对。脚本**返回结构化数据**(parityAssessments / newGaps / disagreements / confirmed),编排者据此写报告 `reviews/P4-maxcompute-full-rereview-<date>.md`(写时填日期)。
> ⚠️ 跑前仍建议**人工扫一眼脚本里 6 域的 review prompt**,确认无开发结论泄漏(脚本里的 `DOMAINS[].scope/questions` 与 `LENS_ANGLES` 应只有代码指针与中立问题)。脚本规模:6 域 × `lensesPerDomain` lens(默认 2 = 12 个 Phase-A agent)→ 每 finding × `verifyVotes`(默认 3)refute → 存活项各 1 Phase-C;可用 args 调小。

> 以下是脚本背后的方法(也是给编排者的心智模型);编排者可读全文,但**喂给 Phase A/B 子 agent 的 prompt 必须只含代码指针,不含本文件「先验隔离区」与任何结论**。

- **Phase A — 独立审阅(并行,按 6 域 × 多 lens)**:每 agent 拿到「某功能路径的 cutover 入口 + legacy 基线 + BE 锚点」,自行 `git`/读码,产出结构化 findings(severity / category / cutover-vs-legacy 差异 / 回归判定 / file:line)+ 该路径的「cutover↔legacy 行为差异清单」。**clean-room prompt 模板见本 session 跑过的 `reviews/P4-T06d-*` workflow 脚本**(在 session workflows/scripts 目录;可复用其 CLEANROOM 常量结构,但把 scope 换成下面 6 域)。
- **Phase B — 对抗验证**:每条 finding 派 3 个 skeptic(默认 refuted=true,除非代码铁证),≥2 票存活。
- **Phase C — 历史交叉核对(解禁先验)**:存活 findings 对照「先验隔离区」分类(真新问题 / 已登记降级 / 已处理 / 与历史分歧)。
- **Phase D — 综合**:出报告 `reviews/P4-maxcompute-full-rereview-<date>.md`:按 6 域分节,每条 finding 带 verdict + cutover↔legacy diff + 处置建议;末尾「与历史结论的分歧」专节。

## 📂 6 审查域 — 中立 scope + 入口锚点(无结论,自行判断)

> 下列只给"去哪看 + 对照谁 + 问什么",**不给答案**。cutover 实现普遍在 `PluginDriven*` + `fe/fe-connector/fe-connector-maxcompute/` + `fe/be-java-extensions/max-compute-connector/`;legacy 基线在 `fe/fe-core/src/main/java/org/apache/doris/datasource/maxcompute/`(+ `.../insert/MCInsertExecutor`、`.../transaction/`)。翻闸后 `max_compute` catalog 实例为 `PluginDrivenExternalCatalog`、表为 `PLUGIN_EXTERNAL_TABLE`。

### 1. 读取(Read / SELECT)
- **cutover 入口**:`datasource/PluginDrivenExternalTable`(toThrift / initSchema / getFullSchema)、`datasource/PluginDrivenScanNode`、`fe-connector-maxcompute/.../MaxComputeScanPlanProvider` `MaxComputeScanRange` `MaxComputeConnectorMetadata`(buildTableDescriptor / getTableSchema / split)、`be-java-extensions/max-compute-connector/.../MaxComputeJniScanner`。
- **BE**:`be/src/exec/scan/file_scanner.cpp`、`be/src/runtime/descriptors.cpp`、`be/src/format/table/max_compute_jni_reader.cpp`、`gensrc/thrift/Descriptors.thrift`(TMCTable)。
- **legacy 基线**:`datasource/maxcompute/MaxComputeExternalTable`(toThrift)、`maxcompute/source/MaxComputeScanNode` `MaxComputeSplit`。
- **问(中立)**:cutover 的 table descriptor 产出何种类型/字段?BE 如何消费?与 legacy 一致吗?split 的 size/offset 语义?谓词下推(含 CAST / datetime / 时区)与 legacy 是否等价?分区裁剪行为?列属性(isKey 等)在 DESCRIBE/information_schema 的呈现?limit 优化触发条件?凭证/endpoint/project/quota 如何到达 BE?

### 2. 写入(Write / INSERT)
- **cutover 入口**:`.../insert/PluginDrivenInsertExecutor`、`planner/PluginDrivenTableSink`、`transaction/PluginDrivenTransactionManager`、`fe-connector-maxcompute/.../MaxComputeConnectorTransaction` + write-plan/sink、BE MC writer + block-alloc RPC(`FrontendServiceImpl.getMaxComputeBlockIdRange`/`TMaxComputeBlockId*`)。
- **legacy 基线**:`.../insert/MCInsertExecutor`、`transaction/.../MCTransaction`、legacy MC sink。
- **问**:事务生命周期(begin/finalizeSink/beforeExec/commit/abort/rollback)与 legacy 是否等价?affected-rows 来源?block 上限是否尊重 `Config.max_compute_write_max_block_count`?提交协议(TBinaryProtocol / TMCCommitData)?post-commit 缓存刷新的失败处理?并发/并行写?

### 3. DDL
- **cutover 入口**:`datasource/PluginDrivenExternalCatalog`(createTable/createDb/dropDb/dropTable)、`nereids/.../info/CreateTableInfo`(paddingEngineName/checkEngineWithCatalog/analyzeEngine/CTAS)、`connector/ddl/CreateTableInfoToConnectorRequestConverter`、`fe-connector-maxcompute/.../MaxComputeConnectorMetadata`(DDL)。
- **legacy 基线**:`datasource/maxcompute/MaxComputeMetadataOps`。
- **问**:本地名↔远端名解析(create/drop,name-mapping 开/关)?engine 推断与一致性校验?列约束(auto-increment/聚合)、分区/分布描述校验?ifExists/ifNotExists 语义?CREATE 的存在性预检?DROP DB FORCE 级联?editlog 写入与 cache 失效的内容/顺序?各连接器(jdbc/es/trino 共享 override)是否被波及?

### 4. 元数据回放(Metadata replay / editlog / image)
- **cutover 入口**:`datasource/ExternalCatalog`(replayCreateTable/replayDropTable/replayCreateDb/replayDropDb 及 metadataOps==null 分支)、`persist/CreateTableInfo` `DropInfo` `CreateDbInfo` `DropDbInfo`、`PluginDrivenExternalCatalog.gsonPostProcess` `PluginDrivenExternalTable.gsonPostProcess`、`CatalogFactory`/`GsonUtils` 的 `registerCompatibleSubtype` / `InitCatalogLog.Type`。
- **legacy 基线**:`MaxComputeExternalCatalog` + `MaxComputeMetadataOps.afterCreateDb/afterDropDb/afterCreateTable/afterDropTable`、legacy gson 注册。
- **问**:翻闸后(无 metadataOps)replay 路径是否正确重建 cache?follower FE 行为?image 反序列化旧 resource-backed / 迁移 catalog(ES/JDBC→PluginDriven)是否正确?editlog 与 cache 失效的**执行顺序**(master 侧)与 legacy 是否一致?用本地名还是远端名作 replay key?GSON 三注册(catalog/db/table)compat 是否齐?

### 5. 元数据 cache
- **cutover 入口**:`datasource/ExternalMetaCacheMgr`、`SchemaCache` `SchemaCacheValue` `PluginDrivenSchemaCacheValue`、`ExternalCatalog`/`ExternalDatabase` 的 metaCache + `makeSureInitialized`/`resetMetaCacheNames`/`unregister*`/invalidate、分区值取数路径。
- **legacy 基线**:`maxcompute/MaxComputeExternalMetaCache`、`maxcompute/MaxComputeSchemaCacheValue`。
- **问**:schema cache value 类型与字段(分区列/分区值/类型)?分区值是否有二级 cache(legacy 有,cutover 呢?每查询直连?)?失效/刷新/TTL 时机与 legacy 是否等价?cast 安全(`(PluginDrivenSchemaCacheValue)` 等)?row-count / statistics cache?cache key(NameMapping 本地名)?

### 6. 旧逻辑残留 / fallback(走旧路 或 静默回退)
- **怎么查(自行 grep + 读)**:`instanceof MaxComputeExternalCatalog` / `instanceof MaxComputeExternalTable` / `MAX_COMPUTE_EXTERNAL_TABLE` 的所有 dispatch 点;`TableType` 驱动的路由;`toThrift` 的 null/兜底分支(产 `SCHEMA_TABLE` 等);`BindRelation`/`getEngine`/`getEngineTableTypeName` 路由;`GsonUtils.registerCompatibleSubtype` 注册;翻闸后仍可达地 **构造/调用 legacy `maxcompute.*` 类** 的路径;keep 集(image/plan/thrift compat)。
- **问**:翻闸后(catalog=PluginDriven),哪些路径仍命中 legacy MaxCompute 逻辑、或**静默 fallback** 到通用/旧路径(而非 fail-loud)?哪些 keep 集项是必要 compat、哪些是真残留?有无"分发只接了一半"(如某命令的 BE handler 接了但 analyze 网关没接,或反之)?

## 📦 交付物
`reviews/P4-maxcompute-full-rereview-<date>.md`:6 域分节,每 finding = {severity, category, cutover-vs-legacy diff, 回归 yes/no/unsure, file:line, 处置建议};+「与历史结论分歧」专节;+ 每域一句"独立判定:该路径 cutover 是否达成 legacy parity / 是否有 design-vs-impl 落差"。

---

## 🔒 先验隔离区(QUARANTINE)——仅 Phase C 交叉核对用,**Phase A/B 禁读**

> 下列是开发过程产生的结论/记忆。**编排者勿将其内容放进 Phase A/B 审阅者 prompt**。仅在独立结论形成后(Phase C)用于"开发是否遗漏/是否分歧"。
- P4-T06d 设计 + 轮次:`tasks/designs/P4-T06d-*-design.md`、`reviews/P4-T06d-*-review-rounds.md`、`tasks/designs/P4-cutover-fix-design.md`、`reviews/P4-cutover-review-findings.md`(上一轮 41 发现 + 16 分歧)。
- 翻闸设计 / 移除设计:`tasks/designs/P4-T05-T06-cutover-design.md`、`tasks/designs/P4-batchD-maxcompute-removal-design.md`、`tasks/designs/P4-T06c-fe-dispatch-wiring-design.md`。
- 决策/偏差账:`decisions-log.md`、`deviations-log.md`、`tasks/task-list.md`(P4-T06d 累计结论)。
- auto-memory:`catalog-spi-*`(尤其 `catalog-spi-cutover-fe-dispatch-gap` 含历次结论 + 已修登记)。
- 这些里有"已修/已登记/已证伪/mutation-proven"等强先验——正是本轮要避免被其框住的内容。

---

## ⚙️ 操作须知(无结论,纯工程)
- **maven 必绝对 `-f` + `-pl :artifactId`**:改 fe-core 带 `:fe-core -am`;改连接器带 `:fe-connector-maxcompute`。读真实 `BUILD SUCCESS/FAILURE` 与尾部 `echo "MVN_EXIT=$?"`;**勿信**后台 task-notification 的 exit code。
- **build cache 坑**:守门/跑测带 `-Dmaven.build.cache.enabled=false`,否则会 restore 旧 build 且 **surefire XML 可能 stale**(本 session 多次踩到:mutation 跑出 BUILD FAILURE 但读到旧 XML 显示 0 fail)。直接读 mvn 输出的 `Tests run:` 行,别只读 XML。
- **checkstyle**:`-pl :fe-core checkstyle:check`;`CustomImportOrder`(doris→第三方[com.*/org.* 非 doris]→java)/`UnusedImports`/`LineLength 120`;扫 test 源。
- **import-gate**:`bash tools/check-connector-imports.sh`(repo 根跑)。
- **分支**:`catalog-spi-05`,本地;未跟踪 `.audit-scratch/` `conf.cmy/` `regression-conf.groovy.bak`(勿提交)。
- **mutation 验证技巧**:改产线一处→跑相关 UT→确认对应 test 变红→还原。本 session 用 `cp` 备份产线文件做 mutation(比 perl 删块安全——perl 易匹配到首个同名 `if` 误删方法)。

## 🧠 给下一个 agent 的 meta 建议
- 先按上面「6 审查域 + clean-room 铁律」搭 review workflow;**先写 Phase A/B 的 clean-room prompt(只放代码指针),确认其中不含任何 plan-doc 结论再跑**。
- live e2e 仍是翻闸真正完成门(本 review 之外,用户跑;runbook 见 `git show` 历史 HANDOFF 或 decisions-log)。
- doc-sync(prior-session WIP,未 commit)+ Batch-D 删 legacy 是 review 之后的事;**Batch-D 红线**(勿删 `PartitionsTableValuedFunction` 的 MaxCompute 分支)——但这条本身是先验,若 review 要独立判定 Batch-D 安全性,也应从代码重新核。
