# FIX-READ-DESC — 对抗 review 轮次记录

> 目的: 记录每轮 review 的 finding + verdict + 处置,防跨轮结论矛盾。max 5 轮。
> 设计: `plan-doc/tasks/designs/P4-T06d-FIX-READ-DESC-design.md`。

## Round 1 (3 clean-room reviewers,distinct lenses)

**R1 — 正确性 / BE parity: ✅ CLEAN**
- TMCTable 字段集与 legacy `MaxComputeExternalTable.toThrift` 逐一致(endpoint/quota/project/table/properties),无 BE 读取字段遗漏。
- BE 无 `__isset` 守卫的 deprecated 字段(region/access_key/...) 未 set → thrift 默认空串(非 UB),与 legacy 一致;endpoint/quota 有守卫,已 set。
- 产出 `MAX_COMPUTE_TABLE` + mcTable 满足 `file_scanner.cpp:1069` static_cast 与 `max_compute_jni_reader.cpp` 消费;凭证经 properties map(同 legacy)。
- project/table 用 remote 名与 SPI 读 session(`TableIdentifier.of(remoteDb,remoteTbl)`)一致;MC 无 name-mapping → 实际 local==remote,等价 legacy 且映射开启时更正确(OQ-7 有意修正)。
- BE 不读 descriptor 的 _database 作 MC 读 → 6th ctor 参 benign。

**R3 — 回归 / blast-radius / build: ✅ CLEAN (0 blocking, 2 info)**
- ctor 全仓仅 2 调用点(`MaxComputeDorisConnector.getMetadata` + 新 UT),均已改 6 参;无残留 3 参调用。
- `getMetadata` 先 `ensureInitialized()`(:159) 再构造(:160);endpoint/quota 在 doInit 赋值、properties final → 无 read-before-init。
- properties map 非 init 后变更,与 legacy 同 by-ref posture,thrift 序列化 copy → 无 aliasing。
- keep-set 干净:仅 2 连接器文件 + 1 新测试 + docs;未触 BE/thrift/fe-core/legacy。
- gates 独立重跑: **MVN_EXIT=0 / CS_EXIT=0 / IMPORTS_EXIT=0**,新 UT 实跑 `Tests run: 1`。

**R2 — 测试有效性 (Rule 9): ⚠️ ISSUES FOUND (3)**
- **[medium] 调用点 wiring 无测试守门 + 设计 doc 过度声明**: 连接器 UT(正确地)够不到 fe-core 调用点 `PluginDrivenExternalTable.toThrift:247-251`(传 `db.getRemoteName()`/`getRemoteName()`/`schema.size()`)。设计称该缺口"仅 e2e 可覆盖",但 `fe/fe-core/src/test/.../PluginDrivenExternalTableEngineTest.java` 已有 mock Connector/ConnectorMetadata/ExternalDatabase 的 harness → 可用 Mockito ArgumentCaptor 廉价补 fe-core 调用点测试,断言 dbName/remoteName/numCols。caveat: toThrift 调 makeSureInitialized()+getFullSchema() → 比 engine-name 测试多些 setup,但远低于 e2e。
- **[low] in-module 测试对陈旧 ~/.m2 connector-api jar 脆弱**: 不带 `-am` 跑会 NoClassDefFoundError(ConnectorTransaction);带 `-am` 通过。非测试代码缺陷,属已知 build gotcha(坑6:改连接器须 -am)。
- **[low] numCols/catalogId 在 in-module 不可观测**: TTableDescriptor 无 numCols getter,connector UT 无法断言 numCols 转发 → 被 [medium] 的调用点测试覆盖即解决;catalogId legacy 本就忽略(正确)。

**Round 1 处置决定**:
- [medium] → **Round 2 修复**: 尝试在 fe-core 补调用点测试(ArgumentCaptor 断言 remote dbName/remoteName + numCols=schema.size());若 Env 单例 scaffolding 过重/脆,则回退为"修正设计 doc 过度声明 + 代码静读验证 numCols 转发",并 fail-loud 登记残留 gap。
- [low ×2] → 文档登记(-am 要求已是坑6;numCols 由 [medium] 解决)。非阻塞。
- R1/R3 无 code 缺陷 → 生产代码本轮不改。

## Round 2 (修复 Round-1 [medium])

- **处置**: 在 fe-core 新增调用点测试 `PluginDrivenExternalTableEngineTest#testToThriftPassesRemoteNamesAndNumColsToBuildTableDescriptor`,用 Mockito ArgumentCaptor 捕获 `metadata.buildTableDescriptor(...)` 实参,断言 `dbName=="REMOTE_DB"`(=db.getRemoteName)、`remoteName=="REMOTE_TBL"`(=table.getRemoteName)、`numCols==3`(=schema.size)。复用既有 `TestablePluginCatalog` harness(扩 ctor 注入可控 ConnectorMetadata + override `getConnector()` 绕过 Env init)。
- **可行性**: 反例预期(Round-1 caveat 担心 Env 单例过重)未成立 —— toThrift 仅经 makeSureInitialized/getFullSchema/getConnector 触 Env,三者均可在测试子类/TestablePluginCatalog override,无需起真 Env/CatalogMgr。
- **Rule 9 mutation 自证**: 临时把调用点改成 `db.getFullName()`/`getName()`/`schema.size()+1` → 测试 FAIL(`expected <REMOTE_DB> but was <mydb>`),恢复生产文件。
- **产物**: 仅 test + design doc;**生产代码本轮零改**。设计 doc 删除"e2e-only"过度声明,改为"调用点已由 fe-core 测试覆盖";补 build note(`-am` + `-DfailIfNoTests=false`)。
- **gates**: MVN_EXIT=0(Tests run: 10)/CS_EXIT=0。

## Round 3 (独立验证 Round-2 test,非作者)

- **R-verify: ✅ CLEAN**
  - git diff 确认 `fe/fe-core/src/main` 本轮零改;唯一 fe-core 改动为该测试文件;Round-1 连接器 fix 仍在。
  - 测试**非 vacuous**: 三个 override 只 stub Env/cache/init plumbing,被断言的实参全经真实 `toThrift()` body(:247-251)流出;`buildConnectorSession()` 未 override,走真实 ctx==null 路径。
  - 独立复现 mutation: 改本地名 → `Tests run:1, Failures:1`(dbName 断言先挂),恢复后 `git diff src/main` 空。
  - 扩展的 TestablePluginCatalog ctor 未破坏其余 9 测试(`Tests run: 10` 全过)。
  - MVN_EXIT=0 / CS_EXIT=0;working tree 完整(生产 clean + Round-2 test + Round-1 fix)。

## 收敛结论

Round-1 唯一实质 finding([medium] 调用点无测试守门 + doc 过度声明)已在 Round-2 修复、Round-3 独立验证 CLEAN。**review 不再产生新问题 → FIX-READ-DESC 收敛,可 commit**。R1/R3 的正确性/回归/build 维度本就 CLEAN。

无跨轮矛盾:Round-1 R2 的 [medium] 在 Round-2 关闭、Round-3 确认;两个 [low](-am 要求 / numCols 不可观测)分别由 build note 登记、由调用点测试覆盖。

