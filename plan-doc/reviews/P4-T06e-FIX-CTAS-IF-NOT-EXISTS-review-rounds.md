# P4-T06e · FIX-CTAS-IF-NOT-EXISTS — review 轮次记录

> issue=`P2-7 FIX-CTAS-IF-NOT-EXISTS`（DG-6 / F33, minor→**major**, regression）
> design=`plan-doc/tasks/designs/P4-T06e-FIX-CTAS-IF-NOT-EXISTS-design.md`
> 方向：FE-only，无 SPI 变更。`createTable` 区分新建 vs 已存在，IFNE 命中返回 true 短路 CTAS。

## 设计对抗验证（design workflow `weepgfhwu`）

verdict = **approve-with-nits**（0 mustFix，parityCorrect=true，blastRadiusComplete=true，**testPlanRule9Compliant=false**）。test-quality 旗标：① 设计原 Test 1 的 `resetMetaCacheNames` 断言真空（生产经 `getDbForReplay(...).resetMetaCacheNames()` 在 replay-db 对象上 reset，非 `catalog.resetMetaCacheNames()`）；② 缺 `exists && !isIfNotExists()` 测。**两者实现时已纳入**（见下）。

## 实现

**改 1 生产文件 + 1 测试文件**（无 SPI、无签名变更）：
- `PluginDrivenExternalCatalog.createTable`：hoist `ConnectorMetadata metadata` 局部；加存在性预检 `boolean exists = metadata.getTableHandle(session, db.getRemoteName(), tableName).isPresent() || db.getTableNullable(tableName) != null;`（镜像 legacy `createTableImpl:178-197` 双探）；`if (exists && isIfNotExists()) return true;`（跳连接器 create + editlog + resetMetaCacheNames）；否则原逻辑不变（return false）。Javadoc 更正（删"conservatively assumes creation"陈述）。
- `PluginDrivenExternalCatalogDdlRoutingTest` +3 测：① IFNE+远端已存在→true+跳全副作用（`verify(replayDb, never()).resetMetaCacheNames()` 非真空断言）；② IFNE+本地 cache 已存在（远端空、local arm）→true；③ 已存在+非-IFNE→连接器抛→DdlException 传播+createTable 被调（钉"非-IFNE 不误短路"）。

**契约确认**：`Env.createTable:3749-3752` 直接回传 override 返回值 → `CreateTableCommand:103 if(createTable(...))return;` CTAS 短路。返回 true 即阻止 INSERT 入已存在表（DG-6 数据变更 bug）。

**守门**：编译 fe-core 绿；UT RoutingTest 25/25；checkstyle 0；mutation 三向红：(A') `return true`→`false`→测 1&2 红；(B) 去 `&& isIfNotExists()`→测 3 红；(C) 去 `|| db.getTableNullable(...) != null`→**仅**测 2 红（钉 local arm）。（注：checkstyle 绑 validate 阶段随 build 跑——删整块致 `exists` unused 会先 checkstyle 红，故用 `return true→false` 作 mutA'。）

## Round 1（impl 对抗 review，workflow `wh4ja0geq`，4 lens）

2 candidate（同一问题）入 verify，**均证伪（isReal=false）**，0 mustFix：
- **REFUTED（已记 known pre-existing gap）**：`已存在+非-IFNE` 且**仅本地 cache 命中（远端缺）**时——legacy `createTableImpl:189-195` 抛 `ERR_TABLE_EXISTS_ERROR`，cutover（P2-7 前后皆然）静默远端建表（连接器 `createTable:337` 只探远端、远端缺→不抛→建表）。证伪理由：**非 P2-7 引入**——HEAD（P2-7 前）该 override 无任何 FE 侧存在预检，非-IFNE 直落 `connector.createTable`，此子case 字节一致；P2-7 的预检**只** gate IFNE 短路。P2-7 范围=DG-6（IFNE-CTAS 静默 INSERT 数据变更）已修。设计 §157-175 明确将非-IFNE 错误码/文案分歧记为 pre-existing out-of-scope。且远端确缺时建表（FE-cache 陈旧）outcome 可争议地比 legacy 抛错更对。
- 其余 lens（parity / blast-roundtrip / test-quality）finding 全 nit（含正面确认：override 仅 plugin catalog 可达、getTableHandle 为既有 SPI default、new-table 路径既有测覆盖）。

**收敛**：0 mustFix。

## ⚠️ KNOWN PRE-EXISTING GAP（非本 fix 引入、out-of-scope、待用户定）

`CREATE TABLE <t>`（**无 IF NOT EXISTS**）当 `<t>` 在 FE cache 存在但远端 ODPS 已不存在（cache 陈旧 / drop-out-of-band）时：legacy 抛 `ERR_TABLE_EXISTS_ERROR`（基于 local cache），cutover 静默在远端建表。**P2-7 未改变此子case**（pre-existing on cutover）。严重度可争议（远端确缺，建表 outcome 未必错）。若要全 legacy parity + fail-loud，可在 `exists && !isIfNotExists()` 加 FE 侧 `ErrorReport.reportDdlException(ERR_TABLE_EXISTS_ERROR, tableName)`——但属 DG-6 之外、且会改远端-hit 路径错误文案。建议留 P3/backlog 由用户定，不在本 fix 扩 scope（Rule 3）。

## 累计结论

- **根因**（DG-6/F33）：override 恒 `return false` + 恒写 editlog → `CreateTableCommand:103` 不短路 → `CREATE TABLE IF NOT EXISTS ... AS SELECT` 对已存在表执行 INSERT（静默数据变更，非仅 editlog 冗余）。
- **修**：FE 侧存在预检（远端 getTableHandle OR 本地 getTableNullable，镜像 legacy 双探）+ IFNE 命中 return true 跳 create/editlog/cache-reset。无 SPI 变更（复用既有 `getTableHandle` default Optional.empty()，其余连接器零影响——本 override 仅 plugin catalog 可达）。
- **真值闸**：UT 25/25 + mutation 三向红。live e2e（CREATE TABLE IF NOT EXISTS ... AS SELECT 对已存在表 → 行数不变 / 新表 → 建+填）CI 跳。
- **doc-sync 随后续**：DDL-C5 从 minor 上调 major、cutover-fix-design CTAS 语义更正、上方 KNOWN GAP 入 deviations-log（待用户定）。
