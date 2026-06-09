# P4-T06d · FIX-DDL-REMOTE — DDL 远端名解析(CREATE/DROP TABLE)

> issue 4 / 6,phase 2 DDL,sev=major,layer=fe-core,depends_on=DDL-P1(FIX-DDL-ENGINE,已落 `0d95d837924`,CREATE 分析期网关已通,本 override 现可达)。
> 来源: `P4-cutover-fix-design.md` §FIX-DDL-REMOTE(:227-294,verdict=needs-revision)+ review DDL-P3/DDL-C2。
> 本文档已折入 parent critic 的全部 corrections/gaps/额外风险(逐条标 ✅),并据**当前代码树重新推导**(parent 的行号/包路径偏差已校正)。

## Problem

翻闸到 `PluginDrivenExternalCatalog` 后,对**启用名映射**的 catalog(`lower_case_meta_names=true` / `lower_case_database_names=1|2` / `meta_names_mapping`,使本地展示名 ≠ ODPS 远端真名)执行 `CREATE TABLE` / `DROP TABLE` 时,FE 把**本地名**原样透传给连接器 → 连接器原样喂 ODPS SDK:

- `CREATE TABLE`:在错误大小写/映射后的库名下建表,或建到不存在的库报错。
- `DROP TABLE`:用本地名查 ODPS 定位不到真实表 → `IF EXISTS` 静默不删(残表)/ 非 `IF EXISTS` 误报"表不存在"。

触发条件:catalog 开启上述任一名映射且本地名≠远端名。未开映射时本地名==远端名(`getRemoteName()` 的 `Strings.isNullOrEmpty` 兜底,`ExternalDatabase.java:408` / `ExternalTable.java:167`),行为不变 —— 这解释为何默认 gate/e2e 未暴露。legacy 可用、翻闸即坏的**数据正确性回归**(review DDL-P3/DDL-C2,regression=yes)。

## Root Cause(行号据当前树校正 ✅ parent gap-5)

- **CREATE**:`PluginDrivenExternalCatalog.java:267-268` `convert(createTableInfo, createTableInfo.getDbName())` 传**本地** dbName;converter `connector/ddl/CreateTableInfoToConnectorRequestConverter.java:60-64` 用该 dbName 作 `.dbName(dbName)`,表名恒 `info.getTableName()`(本地)。连接器 `MaxComputeConnectorMetadata` 原样喂 SDK。
- **DROP**:`PluginDrivenExternalCatalog.java:359` 用本地 `dbName`/`tableName` 直调 `metadata.getTableHandle(session, dbName, tableName)`,零 local→remote 解析。
- **Legacy 基线(须 mirror)**:
  - `MaxComputeMetadataOps.createTableImpl:172-176` db null→`UserException("Failed to get database ...")`;`:179`/`:219` 用 `db.getRemoteName()` 作 dbName;表名保持 `createTableInfo.getTableName()`(**CREATE 不解析远端表名** —— 表尚不存在,无本地→远端映射)。
  - `MaxComputeMetadataOps.dropTableImpl:266-267` 用 `dorisTable.getRemoteDbName()` 与 `dorisTable.getRemoteName()`;该 `dorisTable` 由 **base `ExternalCatalog.dropTable:1119-1128`** 预解析(getDbNullable→db null 无条件抛;db.getTableNullable→table null 时 ifExists 返回否则抛)后传入 —— 即 legacy MC DROP 的可观察行为 == base.dropTable 的控制流。

## Design

remote 解析放 **FE(`PluginDrivenExternalCatalog`)**,**不扩 SPI、不改连接器**(连接器契约保持"接收即远端名,原样发 SDK")。keyed on 通用 `ExternalDatabase.getRemoteName` / `ExternalTable.getRemoteDbName/getRemoteName` API,非 hardcode maxcompute → 任何 full-adopter 复用。

### createTable override(`:263-287`)
在 `convert(...)` 前插入 db 解析:
```java
ExternalDatabase<? extends ExternalTable> db = getDbNullable(createTableInfo.getDbName());
if (db == null) {
    throw new DdlException("Failed to get database: '" + createTableInfo.getDbName()
            + "' in catalog: " + getName());
}
... convert(createTableInfo, db.getRemoteName());   // 第二参由本地名→远端名
```
- 表名保持 converter 内 `info.getTableName()` 原始值。**CREATE 不解析远端表名**(legacy parity)。✅ parent correction-2 / RESUME 约束 4:**显式登记为 non-goal**。
- editlog(`persist.CreateTableInfo`,本地名)与缓存失效(`getDbForReplay(...).ifPresent`,本地名)**不变**。
- ⚠️ **变量遮蔽**:既有 `getDbForReplay(...).ifPresent(db -> db.resetMetaCacheNames())` 的 lambda 形参 `db` 与新 local `db` 冲突 → lambda 形参改名 `d`。

### dropTable override(`:353-374`)—— 精确 mirror base `ExternalCatalog.dropTable:1114-1138`
```java
makeSureInitialized();
ExternalDatabase<? extends ExternalTable> db = getDbNullable(dbName);
if (db == null) {
    throw new DdlException("Failed to get database: '" + dbName + "' in catalog: " + getName());  // 无条件抛
}
ExternalTable dorisTable = db.getTableNullable(tableName);
if (dorisTable == null) {
    if (ifExists) { return; }
    throw new DdlException("Failed to get table: '" + tableName + "' in database: " + dbName);
}
ConnectorSession session = buildConnectorSession();
ConnectorMetadata metadata = connector.getMetadata(session);
Optional<ConnectorTableHandle> handle = metadata.getTableHandle(
        session, dorisTable.getRemoteDbName(), dorisTable.getRemoteName());   // 远端名
if (!handle.isPresent()) {            // 保留:FE 缓存有表但远端已被带外删除
    if (ifExists) { return; }
    throw new DdlException("Failed to get table: '" + tableName + "' in database: " + dbName);
}
... metadata.dropTable(session, handle.get());
... logDropTable(new DropInfo(getName(), dbName, tableName));   // 本地名
... getDbForReplay(dbName).ifPresent(d -> d.unregisterTable(tableName));  // 本地名,lambda 形参 d
```

**🔴 Rule-7 决策(surface conflict)**:parent 设计文本说"db==null 时按 ifExists 干净返回 / 否则抛"。**与 base 实际不符**:base `ExternalCatalog.dropTable:1120-1122` 对 db==null **无条件抛**(不看 ifExists),只有 table==null 才 ifExists-gated。legacy MC DROP 走的正是此 base 方法 → **精确 legacy 可观察行为 = db==null 无条件抛**。本设计取 base/legacy(无条件抛),推翻 parent 文本的 ifExists-gate 描述。理由:更 tested(base 是权威已测路径)+ 精确 parity。`dropDb` override(`:327`)对 db==null 做 ifExists-gate 是另一回事(mirror 的是 legacy `dropDbImpl:133-141`,语义不同,不混淆)。

- 三道闸全保留:① db==null(无条件抛)② dorisTable==null(ifExists)③ handle 远端不存在(ifExists)。第③道是现状已有、本 fix 保留 —— 覆盖"FE 缓存有表/远端带外已删"。✅ parent gap-4。
- `getDbNullable`+`getTableNullable` 移到 `buildConnectorSession()` 之前:table 不存在时连 `connector.getMetadata` 都不调(测试可 `verifyNoInteractions(metadata)`)。

## 须显式登记的偏差 / non-goal(Rule 12 fail loud)

1. ✅ **parent correction-1**:parent Risk 称"加 getDbNullable 把库不存在异常从连接器 OdpsException→RuntimeException 变 FE DdlException,属改进"——**before-state 描述不准**。max_compute 的 `getTableHandle` 对缺库不抛,走 `structureHelper.tableExist`→false→`Optional.empty()`→现状已抛 FE `DdlException "Failed to get table"`(`:364`),非 RuntimeException。本 fix 的改进是真实的(报错从"table"细化为"database"层级 + 命中正确远端对象),但**纠正**:before 不是 OdpsException/RuntimeException。
2. ✅ **parent correction-2 / RESUME 约束 4**:CREATE 不解析远端表名,**显式 non-goal**。且 legacy createTableImpl 还有两道 FE 侧存在性校验(`tableExist` 远端 db `:179` + `getTableNullable` `:189`)本 override **不复刻**(交连接器自己的 ifNotExists/存在性校验)—— pre-existing divergence,本 fix 不闭合不扩范围,显式登记为 non-goal(非 DDL-C6 范围)。
3. ✅ **parent gap-2 / RESUME 约束 2 — SHARED-OVERRIDE blast radius**:`CatalogFactory SPI_READY_TYPES={jdbc, es, trino-connector, max_compute}`,createTable/dropTable 由**四者共享**(EsConnectorMetadata/JdbcConnectorMetadata/TrinoConnectorDorisMetadata 均不 override)。对 jdbc/es/trino:
   - DROP:新增 `getDbNullable`+`getTableNullable`(可触远端往返,`ExternalDatabase.getTableNullable:476` → makeSureInitialized + 可能 listTableNames),随后 `metadata.dropTable` 仍走 `ConnectorTableOps.dropTable` default **throw "not supported"**。**end-state 仍 throw,无功能回归**(它们本就不支持 DROP),但控制流 + 可能的报错文案 + 一次远端往返为新增 —— **登记,不 guard**(guard = 过度设计,失败路径上的额外往返无害)。
   - CREATE:新增 `getDbNullable`(缺库改抛"Failed to get database"),库存在则 `createTable` 仍 throw "not supported"。end-state 仍 throw。
4. ✅ **parent gap-3 / RESUME 约束 3 — "逐字节一致"不成立**:即便**未开名映射**,本 fix 也改变了**FE 侧控制流**(新增 getDbNullable+getTableNullable 解析、可能远端校验、db 缺失异常层级变化)。parent Risk 的"逐字节一致"**仅对发往 SDK 的名字成立**,对 FE 控制流不成立 —— 纠正措辞。
5. ✅ **parent 额外风险-1 — master 写路径延迟/失败面**:`getDbNullable`/`getTableNullable` 在 master 上可触发 lazy metaCache build / 远端往返;ODPS 慢/不可达时 CREATE/DROP 会在 SDK 调用前 block 于元数据解析。轻微延迟/失败面变化,登记。
6. ✅ **parent 额外风险-2**:`dorisTable.getRemoteDbName()` == 其 parent db 的 `getRemoteName()`(`ExternalTable.java:536`);与单独 `getDbNullable` 取的 db 应同对象,并发刷新理论上瞬时分歧 —— 与 base dropTable 结构相同,非新增风险,登记不处理。
7. **READ-only 影响面**:本 fix 不触 BE/thrift/连接器/SPI;editlog/缓存键仍用本地名 → follower replay 一致(`replayDropTable` 走本地名分支,不受影响)。

## Implementation Plan

| # | 层 | 文件 | 改动 |
|---|---|---|---|
| 1 | fe-core | `PluginDrivenExternalCatalog.java` createTable(:264-287) | 插 getDbNullable+null 校验;`convert` 第二参→`db.getRemoteName()`;cache-invalidation lambda 形参 `db`→`d` |
| 2 | fe-core | `PluginDrivenExternalCatalog.java` dropTable(:353-374) | 插 getDbNullable(无条件抛)+getTableNullable(ifExists);getTableHandle 用 remote 名;保留 handle-absent 闸;unregister lambda 形参 `db`→`d` |
| — | — | imports | `ExternalDatabase`/`ExternalTable` 同包 `org.apache.doris.datasource`,**无需 import**;`ConnectorMetadata`/`ConnectorTableHandle`/`Optional` 已 import |

不触:fe-connector-maxcompute / fe-connector-api / be / thrift。守门:`-pl :fe-core -am` + `fe-code-style`(Checkstyle)。本 issue 独立 commit `[P4-T06d] ... [FIX-DDL-REMOTE]`。

## Test Plan(UT,fe-core,`-pl :fe-core -am`)

扩 `PluginDrivenExternalCatalogDdlRoutingTest.java`。**✅ parent gap-1 / RESUME 约束 1 — 既有 5 用例必 rewrite(非"扩"),否则套件变红(Rule 12 fail loud)**:`getDbNullable` 默认返回 `dbNullableResult`(默认 null);新前置令 4 drop 用例(`testDropTableResolvesHandleRoutesAndUnregisters:176` / `IfExistsWhenMissing:190` / `MissingWithoutIfExistsThrows:200` / `WrapsConnectorException:209`)+ 1 createTable 用例(`testCreateTableInvalidatesDbCache:223`)在 getDbNullable/getTableNullable 阶段即抛/改道 → 须 stub `dbNullableResult` + `db.getTableNullable(...)`。

新增/重写用例(每条编码 WHY,mutation 自证):

**CREATE**
- `testCreateTablePassesRemoteDbNameToConverter`(新)—— stub `db.getRemoteName()="DB1"`(local `db1`);**✅ parent 额外风险-4 / RESUME 约束 5**:**不能**用 `argThat(req->req.getDbName()...)`(converter 被 mock,返回 stub req 与 dbName 无关 → vacuous)。改为 `conv.verify(() -> convert(info, "DB1"))` **捕 convert() 第二参**。mutation(传 `createTableInfo.getDbName()` 本地名)令其红。
- `testCreateTableMissingDbThrows`(新)—— `dbNullableResult=null` → DdlException + `verifyNoInteractions(metadata)`。
- `testCreateTableInvalidatesDbCache`(重写)—— 补 `dbNullableResult`(stub getRemoteName)+ `dbForReplayResult`,断言 `createTable(session, req)` + `resetMetaCacheNames()`。

**DROP**
- `testDropTableResolvesRemoteNamesRoutesAndUnregisters`(重写 :176)—— local `db1.t1` → remote `DB1.TBL1`;断言 `getTableHandle(session, "DB1", "TBL1")`(远端名)+ `dropTable` + `logDropTable` + `unregisterTable("t1")`(本地名)。同时满足 critic 的 `testDropTableUsesRemoteDbAndTableName` 需求。mutation(用本地名调 getTableHandle)令其红。
- `testDropTableMissingDbThrowsEvenWithIfExists`(新)—— `dbNullableResult=null`,`ifExists=true` → 仍 DdlException(编码 Rule-7 决策:db 缺失无条件抛,mirror base)。mutation(ifExists-gate db==null)令其红。
- `testDropTableIfExistsWhenMissingTableIsNoop`(重写 :190)—— db 在、`getTableNullable→null`、ifExists → no-op + `verifyNoInteractions(metadata)`。
- `testDropTableMissingTableWithoutIfExistsThrows`(重写 :200)—— db 在、table null、!ifExists → throw + `verifyNoInteractions(metadata)`。
- `testDropTableHandleAbsentAfterLocalResolveIsNoopWithIfExists` / `...ThrowsWithoutIfExists`(新)—— **✅ parent gap-4**:db+table 本地解析成功,但 `getTableHandle(remote)→empty`(带外远端删除),ifExists→no-op、!ifExists→throw;断言 `getTableHandle` 被调、`dropTable` 不被调。
- `testDropTableWrapsConnectorException`(重写 :209)—— 远端名解析成功 + handle 在 + `dropTable` 抛 DorisConnectorException → 包成 DdlException。

E2E(需 live ODPS + `lower_case_meta_names`,user-run,CI 默认跳过):
- ✅ parent 额外风险-3:E2E 仅在 ODPS 端真实存在混合大小写库时才能证伪 pre-fix(否则 local==remote 退化为 green 假证)。**登记:CI-runnable 守门仅 UT;E2E 标记需 live MC + 预置混合大小写远端对象**。
- `regression-test/suites/external_table_p2/maxcompute/` 扩一支:开 `"lower_case_meta_names"="true"`,断言 CREATE 后 ODPS 真名库存在可 SELECT、`DROP TABLE IF EXISTS` 后 `SHOW TABLES` 不含该表、对照未开映射不变。

## 成功标准
编译过 + Checkstyle=0 + 新/改 UT 全绿且 mutation 自证(用本地名调 getTableHandle/convert 令对应 test 红)+ 对抗 review 收敛(≤5 轮)。

## Review 轮次(2 轮收敛)
详见 `plan-doc/reviews/P4-T06d-FIX-DDL-REMOTE-review-rounds.md`。
- **Round 1** `needs-revision`: 3 findings,全 test-quality(F3/F6/F12),production code CLEAN —— 测试只锁 REMOTE 名半边,未锁 editlog/`getDbForReplay` 的 LOCAL 名半边(follower-replay 不变式)。修法 test-only:`ArgumentCaptor` 断言 `persist.CreateTableInfo`/`DropInfo` 携本地名 + `lastGetDbForReplayArg` 断言 + drop happy-path 分离 resolution/replay db。
- **Round 2** `converged`: 3 lens 一致 resolved,无新缺陷。
- mutation 总账:round-1(remote 解析 + db-null 无条件抛)5 红 + round-2(editlog/getDbForReplay LOCAL 名)2 红。最终 UT 17/17、CS=0、BUILD SUCCESS。
