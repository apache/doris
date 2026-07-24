# Piece B (旗舰 memo) — 实现级蓝图（动码前按 HEAD 再 grep 行号）

> 前置：Piece A（文档）+ Piece C（HMS 缓存）已完成 + 提交 + 全模块 183 测试绿。
> 本文件把 `round-1-memo-hms-cache-design.md` §1-B 细化到可直接编码，含**已侦察的测试交互坑**。
> 铁律：0 fe-core、不 memo Configuration、planScan 一律不碰、latest-only（at-instant 保持独立 build）。

## 决定：**Scope B — planScan 完全不碰**

planScan 本就必须建自己的 metaClient（fsView + 逐文件 schema_id resolver），故它读不读 memo 只省"schema 读"不省 build，且它是刚稳定的读热路径 + 红队 Issue-2 focus。**本轮 planScan 一行不改**（连 schema 也不从 memo 取）。build 收敛同样达成（见下），风险最低。

## memo 收敛的 3 个元数据侧消费点

一条普通过滤 SELECT 今天建 ~5 个 metaClient。本轮把**元数据侧 3 个**收敛到 1 个 loader：

| 消费点 | 今天 | 改后 |
|---|---|---|
| `getColumnHandles`→`getTableSchema(2-arg)`→`getSchemaFromMetaClient(basePath,null)` build@802 | 建 | 读 memo.latestColumns |
| `beginQuerySnapshot`→`latestInstant(handle)` build@751 | 建 | 读 memo.instant |
| `getScanNodeProperties`（plain, `!force_jni`）build@363 | 建 | 读 memo.{resolvedInternalSchema, historicalSchemas} |
| `planScan` build@148 | 建 | **不改**（fsView 必需） |
| `applyFilter`（过滤时）partition build@292 | 建 | **不改**（分区，Piece 无关） |

→ **~5 build → ~3**（memo-loader 1 + planScan 1 + applyFilter 1）。全部冗余 schema/instant 解析消除。

## 新增（0 fe-core）

### 1. `ConnectorStatementScopes`（fe-connector-api）加常量
- 加 `public static final String HUDI_METACLIENT = "hudi.metaclient";`（紧跟 `ICEBERG_TABLE`）。
- 更新类 javadoc：把 `hudi.metaclient` 从"Reserved"行移到"Declared today"（现写 `{@link #ICEBERG_TABLE}`；改成 `{@link #ICEBERG_TABLE}、{@link #HUDI_METACLIENT}`）。
- 这是 fe-connector-**api** 非 fe-core → 铁律 A 不碰。

### 2. `HudiStatementScope`（新，fe-connector-hudi，包私有 final class）
镜像 `IcebergStatementScope.sharedTable`：
```java
static HudiTableFacts sharedTableFacts(ConnectorSession session, String db, String table,
        Supplier<HudiTableFacts> loader) {
    return ConnectorStatementScopes.resolveInStatement(
            session, ConnectorStatementScopes.HUDI_METACLIENT, db, table, loader);
}
```
- 值类型 = `HudiTableFacts`（不可变投影，**非** AutoCloseable → 不被 scope close-pass 关）。
- null session / NONE scope → loader 每次跑（byte-identical 到今天逐次）。

### 3. `HudiTableFacts`（新，fe-connector-hudi，不可变）
字段（全 final）：
- `long latestCompletedInstant`
- `List<ConnectorColumn> latestColumns`（已 attach field-id 的最新列）
- `HudiSchemaUtils.ResolvedInternalSchema resolvedInternalSchema`（mode-aware base + evolution flag）
- `Collection<InternalSchema> historicalSchemas`（evolution 时的历史版本，否则 emptyList）
- **不含** Configuration、活 metaClient、latestAvro（中间量）、at-instant schema。

### 4. loader（`HudiConnectorMetadata` 私有方法）
```java
private HudiTableFacts loadTableFacts(String basePath) {
    return metaClientExecutor.execute(() -> {
        HoodieTableMetaClient mc = HudiScanPlanProvider.buildMetaClient(buildHadoopConf(), basePath);
        TableSchemaResolver r = new TableSchemaResolver(mc);
        long instant = HudiScanPlanProvider.latestCompletedInstant(mc);
        Schema latestAvro = r.getTableAvroSchema(true);
        List<ConnectorColumn> cols = attachHudiFieldIds(r, latestAvro, avroSchemaToColumns(latestAvro));
        HudiSchemaUtils.ResolvedInternalSchema resolved = HudiSchemaUtils.resolveTableInternalSchema(r, latestAvro);
        Collection<InternalSchema> hist = resolved.enableSchemaEvolution
                ? HudiSchemaUtils.allHistoricalSchemas(mc) : Collections.emptyList();
        return new HudiTableFacts(instant, cols, resolved, hist);
    });
}

private HudiTableFacts sharedFacts(ConnectorSession session, HudiTableHandle h) {
    return HudiStatementScope.sharedTableFacts(session, h.getDbName(), h.getTableName(),
            () -> loadTableFacts(h.getBasePath()));
}
```
- **parity**：loader 每个 fact 的算法与今天各消费点逐字节相同，只是共享一个 mc（并发下更一致，红队 approved）。
- 需把 `HudiSchemaUtils.allHistoricalSchemas` 由 `private` 改 **package-private** static（第 442 行）。`resolveTableInternalSchema` 已是 package static。

## 改挂（消费点，全部有 session）

### A. `getTableSchema(2-arg)` @315 → 走 memo（latest）
今天：`return buildTableSchema((HudiTableHandle) handle, null);`
改：basePath 非空时读 `sharedFacts(session, h).latestColumns`；basePath 空走原 `getSchemaFromHms`；然后组装 `ConnectorTableSchema`。把 `buildTableSchema` 尾部（357-368 的 tableProperties + ConnectorTableSchema 组装）抽成 `assembleTableSchema(handle, columns)` 供两处复用。
- `getTableSchema(3-arg, at-instant)` @335 **不改**：仍走 `buildTableSchema(handle, queryInstant)` → `getSchemaFromMetaClient`（at-instant 独立 build）。
- `getSchemaFromMetaClient` 方法**保留**（3-arg at-instant 仍用它）；只是 2-arg latest 不再经它。

### B. `beginQuerySnapshot` @~418 → 读 `sharedFacts(session, h).latestCompletedInstant`（替 `latestInstant(handle)`）
- `latestInstant(handle)` 方法**保留**（collectPartitions hive-sync 分支 @699 仍用它；那条不在本轮收敛范围，Piece C 已定 collectPartitions 独立）。

### C. `getScanNodeProperties` @~361-391 → plain 路径走 memo
把 metaClient build（@363）挪进 `if (pin != null)` 分支；plain（pin==null）读 memo：
```java
if (!isForceJniScannerEnabled(session)) {
    try {
        String pin = hudiHandle.getQueryInstant();
        Optional<String> dict;
        if (pin == null) {
            HudiTableFacts f = sharedFacts(session, hudiHandle);
            dict = HudiSchemaUtils.buildSchemaEvolutionProp(
                    f.resolvedInternalSchema, f.historicalSchemas, castHudiColumns(columns));
        } else {
            // 原 at-instant 逻辑原样（build mc / resolveInternalSchemaAtInstant / buildSchemaEvolutionDictAtInstant
            // / 非 evolution 回退 buildSchemaEvolutionProp(mc,resolver,latestAvro,columns)）
        }
        dict.ifPresent(v -> props.put(SCHEMA_EVOLUTION_PROP, v));
    } catch (Exception e) { /* 原样 */ }
}
```

### D. `HudiSchemaUtils.buildSchemaEvolutionProp` 拆重载
保留旧 `(HoodieTableMetaClient, TableSchemaResolver, Schema, List<HudiColumnHandle>)`（at-instant 非 evolution 回退用），改为**委派**新重载：
```java
static Optional<String> buildSchemaEvolutionProp(ResolvedInternalSchema resolved,
        Collection<InternalSchema> historical, List<HudiColumnHandle> requestedColumns) {
    for (HudiColumnHandle h : requestedColumns) { if (h.getFieldId() < 0) return Optional.empty(); }
    Collection<InternalSchema> eff = resolved.enableSchemaEvolution ? historical : Collections.emptyList();
    return Optional.of(buildSchemaEvolutionDict(requestedColumns, resolved.internalSchema,
            resolved.enableSchemaEvolution, eff));
}
// 旧重载 body 改为：resolved=resolveTableInternalSchema(r,latestAvro); hist=evolution?allHistoricalSchemas(mc):empty;
//                    return buildSchemaEvolutionProp(resolved, hist, requestedColumns);
```
（byte-identical：旧 body 逻辑不变，只是抽出。）

## ⚠ 测试交互（已侦察，动码前复核）

- **`HudiSchemaAtInstantTest`（会挂，须改）**：`RecordingMetadata extends HudiConnectorMetadata` override `getSchemaFromMetaClient(String,String)`。其 **2-arg 测试**（`m.getTableSchema(null, handle(null))` → 断 `lastInstant==null`，即"2-arg 走 getSchemaFromMetaClient(null)"）在改挂后**不再经 getSchemaFromMetaClient**（走 memo）→ 该断言失效。**改法**：2-arg control 改成断 memo 路径（RecordingMetadata 另 stub executor 返回 canned `HudiTableFacts`，或断 2-arg 返回 latest 列）；**3-arg at-instant 测试原样保留**（仍经 getSchemaFromMetaClient）。
- **`HudiColumnFieldIdTest`（不受影响）**：测 `avroSchemaToColumns`/field-id 纯函数（line 161 注释明确 getColumnHandles 需 live metaClient、未在此单测）。不经 getTableSchema。**复核确认后无需改**。
- **`HudiSchemaParityTest`（大概率不受影响）**：测 schema 列派生纯函数（无 `new HudiConnectorMetadata`/`getTableSchema(session,...)` 调用命中）。**动码前 grep 确认**它不驱动 live getTableSchema(2-arg)；若驱动则同 AtInstant 处理。

## 新增守门测试

- **build-count 守门**（新 `HudiStatementMemoTest`，Rule 9）：注入计数版 executor（或计数 `buildMetaClient`——注意 planScan@148 inline build 不经 `buildMetaClient`，本轮 planScan 不改故不计），在**真（非 NONE）** `ConnectorStatementScope` 下驱动 `getColumnHandles`+`beginQuerySnapshot`+`getScanNodeProperties`，断言**元数据侧 loader 只跑 1 次**（memo 命中）；NONE-scope 对照跑 3 次（load-every-time）；两路输出（schema 列 / instant / dict）**逐字节相同**。变异（把 memo key 去掉 queryId 或让 loader 每次跑）须让计数断言变红。
- 需要真 scope：用 `ConnectorStatementScope` 的 CHM 背书实现（api 层有 NONE；真实现见 iceberg 测试怎么造 scope——`IcebergStatementScopeTest` 有先例，镜像它构造一个 test scope + session stub 返回 getStatementScope）。

## 验证
- `mvn install -pl :fe-connector-api,:fe-connector-hudi -am -Dmaven.build.cache.enabled=false -Dtest='Hudi*,ConnectorStatementScopes*' -DfailIfNoTests=false -f <abs>/fe/pom.xml` → BUILD SUCCESS + Tests run 0 fail。
- fe-core `git diff` 必空。
- clean-room 对抗净室复审（parity）：重点核 loader 各 fact 与今天逐字节等价 + at-instant 未退化 + planScan 未碰。
- e2e 交集群（分区/时间旅行/schema 演进 parity）。

## commit
`[perf](catalog) fe-connector-hudi: per-statement metaClient/schema projection memo`
正文写：root cause（元数据侧 3 build/pass 冗余）+ 修法（不可变投影 memo，复用 ConnectorStatementScopes，0 fe-core，planScan 不碰）+ 度量（~5→~3 build，schema 3×→1×）+ 守门（build-count + parity + NONE 对照）+ 红队 3 修正落实。
