# P4-T06d · FIX-DDL-REMOTE — 对抗 review 轮次记录

> issue 4 / 6。设计: `plan-doc/tasks/designs/P4-T06d-FIX-DDL-REMOTE-design.md`。
> 流程: clean-room 多 agent 对抗(Phase A 仅读代码独立判断 → Phase B 3 票 refute-by-default → Phase C 交叉核对设计/parent critic)→ 有 real-new-gap 则回设计循环(≤5 轮)。
> 改动文件: `PluginDrivenExternalCatalog.java`(createTable/dropTable 两 override)+ `PluginDrivenExternalCatalogDdlRoutingTest.java`。

## Round 1 — verdict: `needs-revision`(3 findings,全 test-quality,production code CLEAN)

review 配置: 4 lens clean-room reviewer(correctness-parity / regression-blast / test-quality / edge-spi)→ 每 finding 3 skeptic refute-by-default(≥2 confirm 存活)→ Phase C 对照设计交叉核对。raw findings 经对抗后 3 条存活且 Phase C 判 `real-new-gap`。

**关键结论(Phase B/C 一致)**: **production code 正确**,无需改源码。三条全是 test-quality(Rule 9):测试只锁住了不变式的 **REMOTE 名一半**(连接器收到 remote-resolved 名),未锁 **LOCAL 名一半**(editlog `persist.CreateTableInfo`/`DropInfo` + `getDbForReplay` 查询键有意用本地名,保 follower replay 一致)。

| id | sev | 标题 | 处置 |
|---|---|---|---|
| F3 | minor | editlog/`getDbForReplay` 的 LOCAL 名只由注释声明、无测试锁(CREATE 侧 `logCreateTable` 实参从未校验;`getDbForReplay` stub 对任意 arg 返回同一 replay db) | ✅ 已修 |
| F6 | minor | DROP 侧 `logDropTable` 仅 `Mockito.any()` 校验,未断言 `DropInfo` 携带本地名 | ✅ 已修 |
| F12 | nit | drop happy-path 用同一 db mock 兼作 resolution + replay,无法捕获 "unregister 走 resolution db 而非 getDbForReplay" 的退化(create 侧已正确分离) | ✅ 已修 |

**修复(test-only,零源码改动)**:
1. F3/F6 — 加 `ArgumentCaptor`:`logCreateTable` 捕 `persist.CreateTableInfo` 断言 `getDbName()=="db1"`/`getTblName()=="t1"`(本地);`logDropTable` 捕 `DropInfo` 断言 `getDb()=="db1"`/`getTableName()=="t1"`(本地)。`TestablePluginCatalog` 加 `lastGetDbForReplayArg` 记录 `getDbForReplay` 实参,断言 == 本地名。CREATE 缓存用例硬化为 remote `DB1` ≠ local `db1`,使本地名断言有判别力。
2. F12 — drop happy-path 用 **distinct** resolution db vs replayDb;断言 `verify(replayDb).unregisterTable("t1")` + `verify(db, never()).unregisterTable(...)`。

**mutation 自证(Round-1 修复)**: 把 4 处本地名用法翻成 remote(`createTableInfo.getDbName()`/`dbName`→`db.getRemoteName()`/`dorisTable.getRemote*()`,见 `PluginDrivenExternalCatalog.java:288/296/406/407)→ `testCreateTableInvalidatesDbCacheUsingLocalNames` 与 `testDropTableResolvesRemoteNamesRoutesAndUnregisters` **双红**。复原后 17/17 绿。

**Round-1 基础 mutation(remote-name 解析 + db-null 闸,修复前已验)**: 翻 createTable `db.getRemoteName()`→local + dropTable remote→local + db==null 改 ifExists-gate → 5 红(`testCreateTablePassesRemoteDbNameToConverter` / `testDropTableResolvesRemoteNamesRoutesAndUnregisters` / `testDropTableHandleAbsentAfterLocalResolveIsNoopWithIfExists` / `testDropTableWrapsConnectorException` / `testDropTableMissingDbThrowsEvenWithIfExists`),证 remote 解析与 db-null 无条件抛均 load-bearing。

**Phase C 对照(parent critic 既有约束)**: 本批 review 未重复 parent 的 corrections(逐字节一致/blast-radius/non-goal 等)—— 那些在设计 §"须显式登记的偏差/non-goal" 已折入并 clean,Phase C 未将其判为 new-gap,符合预期(不跨轮矛盾)。

## Round 2 — focused recheck(test delta)

review 配置: 3 lens 独立 reviewer(captor 非 vacuous / 无新覆盖回退 / 编译·mock-soundness)judge round-1 的 F3/F6/F12 是否真解决 + 是否引入新 test 缺陷;新 finding 走 3 票 refute-by-default。

**verdict: `converged`**(workflow `w8u1xi1jg`,6 agent)。三 lens 一致:F3/F6/F12 全 resolved(`[true,true,true]`×3),`confirmedNew=[]`(verify 阶段一度浮出的候选新发现被 3 票 refute,未存活)。
逐条复核(仅读代码):
- F3 — captor `ArgumentCaptor.forClass(org.apache.doris.persist.CreateTableInfo.class)` 与 `EditLog.logCreateTable(CreateTableInfo)` 参数类型精确匹配(FQN 用法避开与 nereids `CreateTableInfo` import 冲突);remote `DB1`≠local `db1` 使本地名断言有判别力,remote-mutation→`getDbName()=="DB1"`→红。
- F6 — captor `DropInfo` 匹配 `logDropTable(DropInfo)`;`getDb()/getTableName()` 真实 getter,remote-mutation→红。
- F12 — resolution db vs replayDb 真分离;`verify(replayDb).unregisterTable("t1")` + `verify(db, never()).unregisterTable(...)`。
- 非 vacuous 核验:所有被 stub/verify 的方法(`getRemoteName`/`getRemoteDbName`/`getTableNullable`/`unregisterTable`/`resetMetaCacheNames`)均 public/non-final → Mockito 可拦截;converter 断言捕 `convert()` 第二参而非 mocked req,避开 vacuous 陷阱;`testDropTableMissingDbThrowsEvenWithIfExists` 精确编码 base `ExternalCatalog.dropTable:1119-1129` 语义(缺库无条件抛、缺表才 ifExists)。

## 收敛结论
Round 1(needs-revision,3 test-quality)→ 修(test-only)→ Round 2(converged)。**2 轮收敛**。production code 自始 CLEAN(两轮 reviewer 一致),改动仅强化测试对 follower-replay LOCAL-name 不变式的 mutation 锁。
最终守门(restored clean source,cache 关):UT 17/17 绿;Checkstyle 0;BUILD SUCCESS。
- mutation 总账:remote-name 解析 + db-null 无条件抛(round-1,5 红)+ editlog/getDbForReplay LOCAL-name(round-2,2 红)—— 各业务点均有测试 bite。
