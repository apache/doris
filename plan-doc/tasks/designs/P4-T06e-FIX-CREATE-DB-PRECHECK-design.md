# P4-T06e · FIX-CREATE-DB-PRECHECK — CREATE DATABASE IF NOT EXISTS 恢复远端存在性预检

> issueId=`P2-6 FIX-CREATE-DB-PRECHECK` ｜ DG-4 / F26 / F23 ｜ sev=major ｜ regression=yes ｜ layer=fe-core + SPI（additive `supportsCreateDatabase` 能力门闸）
> 来源：`plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md` §B DG-4（:106-111）；历史处方 `P4-cutover-review-findings.md` DDL-C4(major,"✗否决→修")+DDL-P5(minor)，曾被 P4-T06d 排除（`cutover-fix-design.md:239` "createDb/dropDb 不在本 issue 范围"），现重开。
> 全部 file:line 已据当前代码树（branch `catalog-spi-05`）逐条核对。

> **⚠️ 决策更新（2026-06-08，用户拍板 OQ-1）**：采用 OQ-1 的**替代方案 = 能力门闸**，非本文档原推荐的"接受行为变化+登记 deviation"。新增 additive SPI `ConnectorSchemaOps.supportsCreateDatabase()`（default `false`，MaxCompute override `true`），远端预检 gate 在该能力位上，使 **jdbc/es/trino 字节不变**（它们 `supportsCreateDatabase()==false` → 预检短路跳过 → 仍走 `createDatabase` 抛 "not supported"，与翻闸前一致）。下方 Design/Implementation/Test 的"不扩 SPI / 接受 R6"段以本决策为准更正：见 §决策更新-实现。同 P2-5/P0/P1 的 additive-default 形态。

---

## Problem

翻闸到 `PluginDrivenExternalCatalog` 后，`CREATE DATABASE IF NOT EXISTS <db>` 对一个**远端 ODPS 已存在、但尚未进 FE 元数据缓存**的库会**报错失败**，而 legacy 路径会干净 no-op。

触发条件：库存在于远端 ODPS，但本 FE 的 `getDbNullable(dbName)` 返回 null（典型：FE 重启后 db-name cache 尚未填充该库 / 该库由其它 FE 或外部工具刚建、本 FE cache 未刷新 / `meta_names_mapping` 下本地名查不中）。此时 `CREATE DATABASE IF NOT EXISTS` 的语义本应是"已存在则跳过"，cutover 却让请求穿透到 ODPS `schemas().create()` 抛 "already exists"——`IF NOT EXISTS` 的承诺被违背。这是 legacy 可用、翻闸即坏的**语义回归**（review DG-4，confirms 3/3）。

---

## Root Cause（cutover vs legacy，行号据当前树）

### Cutover（坏）
`fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalCatalog.java:312-326` `createDb(dbName, ifNotExists, properties)`：

```java
public void createDb(String dbName, boolean ifNotExists, Map<String, String> properties) throws DdlException {
    makeSureInitialized();
    if (ifNotExists && getDbNullable(dbName) != null) {   // :314  ← 只查 FE-cache
        return;
    }
    ConnectorSession session = buildConnectorSession();
    try {
        connector.getMetadata(session).createDatabase(session, dbName, properties);  // :319
    } catch (DorisConnectorException e) {
        throw new DdlException(e.getMessage(), e);
    }
    Env.getCurrentEnv().getEditLog().logCreateDb(new CreateDbInfo(getName(), dbName, null));  // :323
    resetMetaCacheNames();  // :324
}
```

短路条件 `:314` **只**查 FE-cache（`getDbNullable`）。FE-cache miss（远端存在但未缓存）时，落到 `:319` `connector.createDatabase` → `MaxComputeConnectorMetadata.java:409-413` `createDatabase(...)` → `structureHelper.createDb(odps, dbName, false)`（第三参 `ifNotExists` 硬编码 **false**，`:411`）→ `mcClient.schemas().create()` 在已存在库上抛 "already exists"，经 `:320` 包成 `DdlException` 上抛。**`ifNotExists` 在到达连接器前被丢弃**。

### Legacy（对的，须 mirror）
`fe/fe-core/src/main/java/org/apache/doris/datasource/maxcompute/MaxComputeMetadataOps.java:110-124` `createDbImpl`：

```java
public boolean createDbImpl(String dbName, boolean ifNotExists, Map<String, String> properties)
        throws DdlException {
    ExternalDatabase<?> dorisDb = dorisCatalog.getDbNullable(dbName);  // FE-cache
    boolean exists = databaseExist(dbName);                            // :113 ← REMOTE 查询
    if (dorisDb != null || exists) {                                   // :114 ← FE-cache OR 远端
        if (ifNotExists) {
            LOG.info("create database[{}] which already exists", dbName);
            return true;                                               // :117 已存在 → no-op
        } else {
            ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, dbName);  // :119
        }
    }
    dorisCatalog.getMcStructureHelper().createDb(odps, dbName, ifNotExists);  // :122
    return false;                                                            // :123 真正新建
}
```

legacy 同时查 **FE-cache（`getDbNullable`）AND 远端（`databaseExist`，`:113`）**：任一命中 + `ifNotExists` → 返回 true（已存在），上层 `ExternalMetadataOps.createDb:48-52` 看到 `res==true` 就**跳过 `afterCreateDb()`**，`ExternalCatalog.createDb:1008-1013` 看到 `res==true` 就**跳过 `logCreateDb`**。即 legacy 已存在路径 = 不建库、不写 editlog、不刷 cache。非 `ifNotExists` + 存在 → `ERR_DB_CREATE_EXISTS`（清晰 FE 错）。

**差异核心**：cutover 丢了 `:113` 的远端 `databaseExist` 这一半。

---

## Parity Reference（逐字镜像对象）

`MaxComputeMetadataOps.createDbImpl:110-124`（上文已引）。本 fix 把其 `dorisDb != null || exists` 双查 + `ifNotExists → return(no-op)` 的控制流，在 `PluginDrivenExternalCatalog.createDb` 内**用通用 SPI 等价物**复刻：
- legacy `dorisCatalog.getDbNullable(dbName)` ≙ cutover `getDbNullable(dbName)`（已有，`:314`）。
- legacy `databaseExist(dbName)` ≙ `MaxComputeMetadataOps.java:93-95` → `MaxComputeConnectorMetadata.databaseExists(session, dbName)`（`MaxComputeConnectorMetadata.java:95` 实现，`structureHelper.databaseExist(odps, dbName)`），cutover 经通用 SPI `connector.getMetadata(session).databaseExists(session, dbName)` 调到同一实现。
- legacy `return true`（no-op，跳 afterCreateDb/logCreateDb）≙ cutover 提前 `return`（跳 createDatabase + logCreateDb + resetMetaCacheNames）。

SPI 面：`fe/fe-connector/fe-connector-api/.../ConnectorSchemaOps.java:34-38` `default boolean databaseExists(session, dbName){ return false; }`；`ConnectorMetadata extends ConnectorSchemaOps`（`ConnectorMetadata.java:37-38`）。MaxCompute 在 `MaxComputeConnectorMetadata.java:94-97` override。**SPI 已暴露此方法，无需任何 SPI 变更。**

---

## Design（已选方向 + WHY）

**用户已定方向：不改 SPI。** 在 FE 侧 `createDb` override 内，把现有"FE-cache 短路"扩成"FE-cache **或** 远端"双查，复刻 legacy `createDbImpl:112-114` 的存在性判定。

具体：当 `ifNotExists && getDbNullable(dbName) == null`（FE-cache 未命中、但用户写了 IF NOT EXISTS）时，构建 session 并查 `connector.getMetadata(session).databaseExists(session, dbName)`；若为 true（远端已存在）→ 提前 `return`（跳过 `createDatabase` + `logCreateDb` + `resetMetaCacheNames`），镜像 legacy "已存在 → no-op"。保留既有 `:314` 的 FE-cache 短路作为**快路径**（cache 命中时连 session 都不必建，与 legacy `dorisDb != null` 短路同义）。

**WHY 此形 vs 其它**：
- **WHY 不扩 SPI**：`databaseExists` 已是 `ConnectorMetadata`/`ConnectorSchemaOps` 的 `default` 方法且 MaxCompute 已 override（`:95`），FE 直接可调。扩签名（如给 `createDatabase` 加 `ifNotExists` 参）违反 Rule 2/Rule 3，且会波及其它 6 连接器与 P0/P1 已确立的 additive-default 约定。
- **WHY 复用 FE-cache 快路径**：legacy `:114` 本就 `dorisDb != null || exists` 短路，FE-cache 命中时不查远端。保留 `:314` 完全等价，且省一次 ODPS 往返。
- **WHY 只在 `ifNotExists` 分支查远端**：非 `ifNotExists` 时的远端存在性见下「非 ifNotExists 路径决策」——保持最小改动，不主动加查询。

### 非 ifNotExists + 远端已存在 路径决策（必须显式记载）

- **legacy**：`createDbImpl:118-119` 抛 `ERR_DB_CREATE_EXISTS`（"Can't create database '%s'; database exists"，`ErrorCode.java:27`，errno 1007 / SQLSTATE HY000）——FE 侧 fail-loud。
- **cutover 现状**：穿透到 ODPS `schemas().create()` 抛 "already exists"，经 `:320` 包 `DdlException` 上抛。
- **本 fix 决策：保持 cutover 现状（连接器/ODPS 抛），不在 FE 侧补 `ERR_DB_CREATE_EXISTS`。** 理由（Rule 2 最小 + 文档化）：
  1. 两者都是 fail-loud（都抛 `DdlException` 终止建库），用户均得到"已存在"错误——Rule 12 不被违反。差异仅在**错误文案 + errno**（legacy 1007/HY000 标准 SQLSTATE vs ODPS 透传文案）。
  2. 让 FE 在非-IFNE 时也主动查远端，会引入一次额外 ODPS 往返且需新分支，属于为"错误文案逐字对齐"付出的非最小代价；ODPS `schemas().create(false)` 本就会权威拒绝。
  3. 本 issue 的回归本质是 **IF NOT EXISTS 误报失败**（合法语句被拒）；非-IFNE 在两条路径下都是"正确地失败"，仅文案不同，属可接受偏差。
- **登记**：此处文案/errno 偏差登记为 known-deviation（见 Risk Analysis R3），不作为 fix 范围。若后续要求逐字 SQLSTATE 对齐，可在连接器 `createDatabase` 捕获 ODPS "already exists" 重抛为带 `ERR_DB_CREATE_EXISTS` 文案的 `DorisConnectorException`——但那是连接器侧改动，超出本 FE-only 最小修。

---

## Implementation Plan（逐文件、含签名）

### 1. 生产代码（唯一一处）
**文件**：`fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalCatalog.java`
**方法**：`createDb(String dbName, boolean ifNotExists, Map<String, String> properties)`（`:311-326`），签名不变。

把 `:314-322` 改为先建 session、对 IF-NOT-EXISTS 在 FE-cache miss 时补远端预检：

```java
@Override
public void createDb(String dbName, boolean ifNotExists, Map<String, String> properties) throws DdlException {
    makeSureInitialized();
    // Fast path: FE-cache hit + IF NOT EXISTS => no-op (legacy createDbImpl: dorisDb != null).
    if (ifNotExists && getDbNullable(dbName) != null) {
        return;
    }
    ConnectorSession session = buildConnectorSession();
    // FE-cache miss but the db may already exist REMOTELY (e.g. created on another FE / before
    // this FE's db-name cache was populated). Legacy MaxComputeMetadataOps.createDbImpl consulted
    // BOTH getDbNullable AND the remote databaseExist; IF NOT EXISTS then no-oped. Mirror that
    // remote check here so CREATE DATABASE IF NOT EXISTS does not surface ODPS "already exists".
    // (Other connectors keep the SPI default databaseExists()==false, so this is a pure no-op
    // fall-through for them -- zero behavior change.)
    if (ifNotExists && connector.getMetadata(session).databaseExists(session, dbName)) {
        LOG.info("create database[{}] which already exists remotely, skip", dbName);
        return;
    }
    try {
        connector.getMetadata(session).createDatabase(session, dbName, properties);
    } catch (DorisConnectorException e) {
        throw new DdlException(e.getMessage(), e);
    }
    Env.getCurrentEnv().getEditLog().logCreateDb(new CreateDbInfo(getName(), dbName, null));
    resetMetaCacheNames();
    LOG.info("finished to create database {}.{}", getName(), dbName);
}
```

要点：
- 保留 `:314` FE-cache 快路径（cache 命中不建 session、不查远端，与 legacy `dorisDb != null` 短路等价）。
- session 构建上移到远端预检之前（远端预检需要 session）。非-IFNE 路径下 session 构建时机较原来略早，但 `buildConnectorSession()` 无副作用（仅读 `ConnectContext`），等价。
- 远端预检只在 `ifNotExists` 时触发；非-IFNE 不查远端，沿用现状（见 Design 决策）。
- 更新方法 Javadoc（`:302-310`）一行，说明现在 IF NOT EXISTS 同时查 FE-cache 与远端。
- 无新 import（`connector`/`session`/`LOG` 均已在用）。

### 2. 测试代码
**文件**：`fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenExternalCatalogDdlRoutingTest.java`
新增 2 个 `@Test`（见 Test Plan），无需改 helper（`TestablePluginCatalog` 已 override `getDbNullable`/`buildConnectorSession`/`resetMetaCacheNames`，`metadata`/`mockEditLog` 已是 mock）。

### 3. 账本 / 文档（plan-doc，非代码）
- `P4-cutover-fix-design.md` / task-list：登记 DDL-C4 重开 + 本 fix commit。
- deviations-log：登记 R3（非-IFNE 文案/errno 偏差）。
- review-rounds：更正 DG-4 状态。
（这些不影响编译/CI，按本仓 doc-sync 惯例随 commit 回填。）

**无签名变更，无调用点变更**（`createDb` 仅由 `ExternalCatalog.createDb:1002` / 命令层调用，签名不动）。

---

## Blast Radius

> ⚠️ **重要更正（orchestrator 的 "仅 MaxCompute override databaseExists" 假设经核码证伪）**：实测 **全部 7 个连接器都 override 了 `databaseExists`**（`EsConnectorMetadata:59` / `HiveConnectorMetadata:87` / `HudiConnectorMetadata:90` / `IcebergConnectorMetadata:84` / `JdbcConnectorMetadata:94` / `PaimonConnectorMetadata:74` / `TrinoConnectorDorisMetadata:93` / `MaxComputeConnectorMetadata:95`），不是只有 MaxCompute。因此本 fix **不是** P0/P1 那种"default 返回 false → 其它连接器零行为变化"的纯 additive-default 形态——必须按"哪些连接器实际走 `PluginDrivenExternalCatalog.createDb`"重新界定影响面。

### 谁实际走 `PluginDrivenExternalCatalog.createDb`
`CatalogFactory.java:51-52` `SPI_READY_TYPES = {"jdbc", "es", "trino-connector", "max_compute"}` —— **这 4 类**在本分支被路由到 `PluginDrivenExternalCatalog`（`:112`/`:123`），其余（hms/iceberg/paimon/hudi/doris）仍用各自 built-in `ExternalCatalog` + 传统 `metadataOps`，**不经过本 override**，完全不受影响。

### 对 jdbc / es / trino-connector 的行为变化（须如实登记）
这 3 类也走本 `createDb` override，且其 `databaseExists` 是**真实实现**（非 default false）：
- jdbc：`client.getDatabaseNameList().contains(dbName)`；es：`DEFAULT_DB.equals(dbName)`；trino：`listDatabaseNames(session).contains(dbName)`。
- **关键**：这 3 类连接器**均未 override `createDatabase`**（`grep` 证实 jdbc/es/trino 无 `createDatabase`），继承 `ConnectorSchemaOps.createDatabase` 的 default → 抛 `"CREATE DATABASE not supported"`（`ConnectorSchemaOps.java:48-52`）。即翻闸后这 3 类本就**不支持** `CREATE DATABASE`。
- 行为差（仅 `CREATE DATABASE IF NOT EXISTS <db>` 且 FE-cache miss 这一窄路径）：
  - **修前**：落到 `createDatabase` → 抛 `"CREATE DATABASE not supported"`（无论该 db 远端是否存在）。
  - **修后**：先查 `databaseExists`。若**远端已存在** → 静默 no-op（成功返回）；若远端不存在 → 仍落到 `createDatabase` 抛 "not supported"（不变）。
- 评估：远端已存在时 `CREATE DATABASE IF NOT EXISTS` no-op 是 SQL 标准语义（IF NOT EXISTS 对已存在对象应成功），**修后更正确**；且这 3 类此前就不支持建库，没有"真的建出库"的语义可破坏。但这是**可观察行为变化**（原本抛错→现在静默成功），不属 MaxCompute 范畴，**必须登记**（见 OQ-1 与 R6）。FE-cache 命中分支（`:314`）对这 3 类行为完全不变。

### fe-core 调用者
- `createDb` 的唯一上游 `ExternalCatalog.createDb`（`:1002-1018`）。本 override 完全替换基类对 plugin catalog 的行为（plugin catalog `metadataOps==null`，基类 `:1004-1005` 抛 "not supported"，故必须 override）。**签名不动 → 上游零改、无调用点变更。**
- 新增的 `databaseExists` FE 调用是 fe-core 首个调用方（`grep` 证实 fe-core 此前无 `.databaseExists(` 调用），不影响任何既有调用点。

### 现有测试断言：是否需改
- `PluginDrivenExternalCatalogDdlRoutingTest`：
  - `testCreateDbRoutesToConnectorAndInvalidatesCache`（`:97-108`）：`ifNotExists=false` → 远端预检（仅 `ifNotExists` 触发）**不执行** → 行为不变，断言不改。
  - `testCreateDbIfNotExistsShortCircuitsWhenDbExists`（`:110-119`）：stub `dbNullableResult != null` → 命中 FE-cache 快路径 `:314` 提前 return，远端预检**不触发**，`databaseExists` 不被调 → 现有断言全部仍成立，**不改**。
  - `testCreateDbWrapsConnectorException`（`:121-129`）：`ifNotExists=false` → 远端预检不触发，仍直达 `createDatabase`（stub 抛 `DorisConnectorException`）→ 断言不改。
- **结论：无现有断言需要修改。** 仅新增 2 个测试。
- 校验命令（确认 override 面）：`grep -rn "boolean databaseExists" fe/fe-connector/*/src/main/java | grep -v fe-connector-api`（应命中全部 7 连接器）。

---

## Risk Analysis

- **R1（低）多一次 ODPS 往返**：IF-NOT-EXISTS + FE-cache miss 时新增一次 `schemas().exists()`。仅在 cache miss 的 IF-NOT-EXISTS DDL 上发生，DDL 低频；legacy 本就每次 `createDbImpl` 都查 `databaseExist`（`:113`），故相对 legacy 是**减少**往返（cache 命中时本 fix 跳过远端查询，legacy 不跳）。无性能回退。
- **R2（低）远端预检异常语义**：`databaseExists` 在 MaxCompute 内可能抛 `RuntimeException`（`McStructureHelper.databaseExist` 包 `OdpsException`，`:140-145`）。本 fix 不捕获它——与 legacy `createDbImpl` 一致（legacy `databaseExist:93-95` 同样直接传播）。Rule 12 fail-loud：远端不可达时建库应失败而非静默继续。
- **R3（已登记 deviation）非-IFNE 已存在错误文案差异**：见 Design 决策。legacy `ERR_DB_CREATE_EXISTS`（1007/HY000）vs cutover ODPS 透传文案。两者都 fail-loud，仅文案/errno 不同。登记 deviations-log，非 fix 范围。
- **R4（无）GSON/replay**：本 fix 只改 create 期控制流，不碰序列化/editlog 结构（IF-NOT-EXISTS 已存在时本就不写 editlog，与 legacy 一致），replay 不受影响。
- **R5（低）session 构建时机前移**：非-IFNE 路径 session 现在在 try 之外、调 `createDatabase` 前构建（原本也是如此，仅相对短路位置变化）。`buildConnectorSession()` 仅读 `ConnectContext` 无副作用，无风险。
- **R6（中，须 surface）jdbc/es/trino 的 IF-NOT-EXISTS 静默化**：见 Blast Radius。这 3 类同走本 override 且 `databaseExists` 为真实现，故 `CREATE DATABASE IF NOT EXISTS <远端已存在 db>` 从"抛 not supported"变为"静默 no-op"。判定：更贴合 SQL 标准、无数据语义破坏，但属可观察行为变化，登记 deviations-log（见 OQ-1）。若要求保守（仅 MaxCompute 受影响、jdbc/es/trino 行为字节不变），可把远端预检 gate 在连接器能力位上（仅当连接器实际支持 createDatabase 才查远端），但那需引入能力判定、非最小改动——倾向接受行为变化 + 登记，待用户定（OQ-1）。

---

## Open Questions

- **OQ-1（行为变化处置）— ✅ RESOLVED 2026-06-08（用户选「替代：能力门闸」）**：jdbc/es/trino-connector 同走本 `createDb` override（`CatalogFactory` `SPI_READY_TYPES`），且它们的 `databaseExists` 是真实现 + 不支持 `createDatabase`。原推荐"接受+登记"会令这 3 类 `CREATE DATABASE IF NOT EXISTS <远端已存在 db>` 从"抛 not supported"变"静默 no-op"。**用户拍板：能力门闸**——新增 additive `supportsCreateDatabase()`，预检仅对声明能力者（MaxCompute）生效，jdbc/es/trino 字节不变。实现见 §决策更新-实现。

## §决策更新-实现（能力门闸，权威版，覆盖上方"不扩 SPI"段）

### 1b. SPI：加 additive `supportsCreateDatabase()`
**文件**：`fe/fe-connector/fe-connector-api/.../ConnectorSchemaOps.java`，在 `createDatabase` default 旁加：
```java
/**
 * Whether this connector supports CREATE DATABASE. Defaults to false so the FE
 * CREATE DATABASE IF NOT EXISTS remote precheck applies only to connectors that
 * can actually create databases; connectors that cannot keep their existing
 * "CREATE DATABASE not supported" behavior unchanged.
 */
default boolean supportsCreateDatabase() {
    return false;
}
```
additive default false → 其余 6 连接器（含 jdbc/es/trino）零行为变化（同 P2-5 的 dropDatabase 4 参 / P0-1/2/3 capability）。

### 1c. 连接器：MaxCompute override → true
**文件**：`fe/fe-connector/fe-connector-maxcompute/.../MaxComputeConnectorMetadata.java`，在 `createDatabase` 旁加 `@Override public boolean supportsCreateDatabase() { return true; }`（MaxCompute 真支持建库）。

### 1（更正）. fe-core：预检 gate 在能力位上
`PluginDrivenExternalCatalog.createDb` 的远端预检条件加 `supportsCreateDatabase()` 前置，且把 `connector.getMetadata(session)` 提为局部变量复用（避免 3 次 getMetadata；MaxCompute getMetadata 轻量无副作用，但 hoist 更清晰）：
```java
public void createDb(String dbName, boolean ifNotExists, Map<String, String> properties) throws DdlException {
    makeSureInitialized();
    // Fast path: FE-cache hit + IF NOT EXISTS => no-op (legacy createDbImpl: dorisDb != null).
    if (ifNotExists && getDbNullable(dbName) != null) {
        return;
    }
    ConnectorSession session = buildConnectorSession();
    ConnectorMetadata metadata = connector.getMetadata(session);
    // FE-cache miss but the db may already exist REMOTELY (created on another FE / before this
    // FE's db-name cache was populated). Legacy MaxComputeMetadataOps.createDbImpl consulted BOTH
    // getDbNullable AND the remote databaseExist; IF NOT EXISTS then no-oped. Mirror that here.
    // Gated on supportsCreateDatabase() so connectors that cannot create databases (jdbc/es/trino)
    // keep their prior behavior (fall through to createDatabase -> "not supported"), unchanged.
    if (ifNotExists && metadata.supportsCreateDatabase() && metadata.databaseExists(session, dbName)) {
        LOG.info("create database[{}] which already exists remotely, skip", dbName);
        return;
    }
    try {
        metadata.createDatabase(session, dbName, properties);
    } catch (DorisConnectorException e) {
        throw new DdlException(e.getMessage(), e);
    }
    Env.getCurrentEnv().getEditLog().logCreateDb(new CreateDbInfo(getName(), dbName, null));
    resetMetaCacheNames();
    LOG.info("finished to create database {}.{}", getName(), dbName);
}
```
需加 import `org.apache.doris.connector.api.ConnectorMetadata`（fe-core 该文件已 import 之，见现 :28，无新 import）。`&&` 短路保证：能力位 false 时连 `databaseExists` 都不查（jdbc/es/trino 零额外 ODPS/远端往返，行为完全不变）。

### Blast Radius（更正）
- SPI `supportsCreateDatabase` additive default false → 7 连接器零编译/行为变化，唯 MaxCompute override true。
- jdbc/es/trino 走本 override：`supportsCreateDatabase()==false` → 预检短路（不查 databaseExists）→ 落 `createDatabase` 抛 "not supported"，与翻闸前**字节一致**。R6 行为变化**消除**，无需 deviation 登记。
- 其余同上方 Blast Radius（仅 4 类 SPI_READY 走 override；hms/iceberg/paimon/hudi 不经过）。

### Test Plan（更正：3 测）
新增到 `PluginDrivenExternalCatalogDdlRoutingTest` CREATE DATABASE 区块：
1. `testCreateDbIfNotExistsSkipsWhenRemoteExistsAndConnectorSupportsCreate`：`dbNullableResult=null`、`when(metadata.supportsCreateDatabase()).thenReturn(true)`、`when(metadata.databaseExists(session,"db1")).thenReturn(true)`、ifNotExists=true → `verify(metadata).databaseExists(...)`、`verify(metadata,never()).createDatabase(...)`、`verify(mockEditLog,never()).logCreateDb(...)`、`resetMetaCacheNamesCount==0`。WHY：DG-4 回归——远端已存在+IFNE 须 FE 侧 no-op。
2. `testCreateDbIfNotExistsCreatesWhenRemoteAbsent`：`supportsCreateDatabase=true`、`databaseExists=false` → `verify(metadata).createDatabase(...)`、`logCreateDb` 写、`resetMetaCacheNamesCount==1`。WHY：远端不存在仍建库（证明没退化成永不建）。
3. `testCreateDbIfNotExistsBypassesPrecheckWhenConnectorLacksCreateSupport`：`supportsCreateDatabase=false`（默认）、ifNotExists=true、dbNullableResult=null → `verify(metadata).createDatabase(...)`（落 createDatabase）、`verify(metadata,never()).databaseExists(...)`（&& 短路不查远端）。WHY：守 jdbc/es/trino 字节不变——能力门闸防止预检对不支持建库的连接器静默 no-op。
**MUTATION**：(a) 删整条预检行 → 测 1&2 红（databaseExists 未被调 + createDatabase/logCreateDb 被调）；(b) 去掉 `metadata.supportsCreateDatabase() &&` → 测 3 红（gate 去掉后 databaseExists 被查 → `never().databaseExists` 断言违反；createDatabase 仍被调，因测 3 的 databaseExists 默认 false——gate 的职责是跳过远端探测，非阻止建库）。（实测：mutA→测1&2 红、测3 绿；mutB→测3 红；mutC 连接器 true→false→CapabilityTest 红。）

## Test Plan

### Unit Tests

**文件**：`fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenExternalCatalogDdlRoutingTest.java`
**类**：`PluginDrivenExternalCatalogDdlRoutingTest`（既有，新增 2 测试到 CREATE DATABASE 区块 `:95` 后）

1. `testCreateDbIfNotExistsSkipsWhenRemoteDbExists`
   - **Arrange**：`catalog.dbNullableResult = null`（FE-cache miss）；`Mockito.when(metadata.databaseExists(session, "db1")).thenReturn(true)`；`ifNotExists=true`。
   - **Act**：`catalog.createDb("db1", true, new HashMap<>())`。
   - **Assert**：
     - `verify(metadata).databaseExists(session, "db1")`（远端预检确被执行）；
     - `verify(metadata, never()).createDatabase(any(), any(), any())`（不建库）；
     - `verify(mockEditLog, never()).logCreateDb(any())`（不写 editlog）；
     - `assertEquals(0, catalog.resetMetaCacheNamesCount)`（不刷 cache）。
   - **WHY（Rule 9）**：legacy `createDbImpl:113-117` 对"远端已存在 + IF NOT EXISTS"干净 no-op（返回 true → 上层跳 logCreateDb/afterCreateDb）；cutover 丢了远端这一半，会让请求穿透到 ODPS `schemas().create()` 抛 "already exists"。本测试锁定"远端存在即 FE 侧 no-op"，守住 DG-4 回归——`IF NOT EXISTS` 对远端已存在库不得报错、不得产生 editlog/cache 副作用。

2. `testCreateDbIfNotExistsCreatesWhenRemoteDbAbsent`
   - **Arrange**：`catalog.dbNullableResult = null`（FE-cache miss）；`metadata.databaseExists` 默认（Mockito boolean 默认 `false`，等价远端不存在）；`ifNotExists=true`。
   - **Act**：`catalog.createDb("db1", true, props)`。
   - **Assert**：
     - `verify(metadata).databaseExists(session, "db1")`（预检执行且返回 false）；
     - `verify(metadata).createDatabase(session, "db1", props)`（确实建库）；
     - `verify(mockEditLog).logCreateDb(any())`（写 editlog）；
     - `assertEquals(1, catalog.resetMetaCacheNamesCount)`（刷 cache）。
   - **WHY（Rule 9）**：守住"远端不存在时仍正常建库 + 写 editlog + 刷 cache"——证明 fix 没有把所有 IF-NOT-EXISTS 都误判成已存在、退化成永不建库。与测试 1 构成"存在↔不存在"对照，编码 legacy `:114` 分支的两侧语义。

   **MUTATION 检查**：把生产代码新增的远端预检整行
   `if (ifNotExists && connector.getMetadata(session).databaseExists(session, dbName)) { ... return; }`
   删除（即一行 revert 回 cutover 现状）后：
   - 测试 1（`testCreateDbIfNotExistsSkipsWhenRemoteDbExists`）**变红**——`createDatabase`/`logCreateDb`/`resetMetaCacheNames` 会被调用，`never()` 断言失败。
   - 测试 2 仍绿（remote==false 时本就该建库）。
   即测试 1 是该 fix 的"杀手测试"，精确钉住被删除的那行业务逻辑。

   补充：现有 `testCreateDbIfNotExistsShortCircuitsWhenDbExists`（FE-cache 命中）继续守"快路径不查远端"——若 mutation 误把快路径 `getDbNullable` 短路也删了，它会红。

### E2E Tests

- **CI 注记**：UT-only，CI 跳 live ODPS（与本批所有 MC fix 同）。
- 真值闸（手动 / live ODPS）：在远端 ODPS 预建 schema `db_x`，确保本 FE `getDbNullable("db_x")==null`（新 FE 或未刷 cache），执行 `CREATE DATABASE IF NOT EXISTS <catalog>.db_x`：
  - 修前：报 ODPS "already exists" 失败；
  - 修后：静默成功（no-op），且未产生重复建库 / editlog。
- 若 `regression-test/suites/` 下有 MaxCompute DDL 套件（依赖 live ODPS 环境变量、CI 默认 skip），可加一条 IF-NOT-EXISTS-on-existing-remote-db 断言；否则保持 UT 覆盖 + 手动 e2e。

### 构建 / 守门（informational，不在本设计执行）
- `mvn -f <abs>/fe/pom.xml -pl :fe-core -am test -Dtest=PluginDrivenExternalCatalogDdlRoutingTest -Dmaven.build.cache.enabled=false`（fe-core 改动）。
- `mvn -f <abs>/fe/pom.xml -pl :fe-core checkstyle:check`（CustomImportOrder/UnusedImports/LineLength 120；扫 test 源）。
- import-gate 不涉（无连接器改动）：`bash tools/check-connector-imports.sh` 仍应过。
- 无 SPI（fe-connector-api）改动 → 无需 api+maxcompute+fe-core 全量重建。
