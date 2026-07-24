# Round 1 设计 — maxcompute 每语句表句柄记忆化

> 兄弟空间 = `plan-doc/perf-hotpath-maxcompute/`（伞形 = `plan-doc/connector-cache-unification/`）。
> 镜像 `plan-doc/perf-hotpath-iceberg/` 布局。铁律：0 fe-core、纯连接器侧、parity 只减不改。
> **行号是 2026-07-24 HEAD `b505b3d7c89` 侦察快照**（6-agent 只读侦察，见伞形 `connectors/maxcompute.md` + 本轮侦察）。

## 1. 病灶（root cause）

`MaxComputeConnectorMetadata.getTableHandle(session, db, table)`（`:118-130`，改前）每次调用都：
1. `structureHelper.tableExist(odps, db, table)` → ODPS `tables().exists()` **远程 HTTP 探测**（`McStructureHelper.java:129-137,218-225`，两个 helper 实现都是裸远程、无缓存）；不存在返回 `Optional.empty()`；
2. `structureHelper.getOdpsTable(...)` → `tables().get(...)` 返回一个**惰性 `com.aliyun.odps.Table`**（get 本身不发网络，首次 `getSchema()/getFileNum()/isExternalTable()` 才触发一次远程 reload）；
3. 新建 `MaxComputeTableHandle` 携带该惰性 Table。

funnel 保证一条语句 + 一个 catalog **恰有一个** `MaxComputeConnectorMetadata` 实例（`PluginDrivenMetadata.get` 按 `"metadata:"+catalogId` memo 在 per-statement `ConnectorStatementScope` 上，`:53-71`），但该 metadata **不 memo 已解析的表**。于是一条语句里 `resolveConnectorTableHandle`（fe-core `PluginDrivenExternalTable.java:116-120`，无缓存）的 **13 个站点** + `PhysicalPlanTranslator`（`:624,676`）+ `BindSink`（`:675,714`）共 **~17 个解析点**各调一次 `getTableHandle`：
- 每次一个冗余 `exists()` 远程往返（**冷统计逐列解析放大到 O(列数)**：`getColumnStatistic` fe-core `:1026` 每列解析一次新 handle）；
- 每次新建一个惰性 Table，各自首次访问 schema 时各触发一次远程 reload。

这就是伞形审计的 **MC-1（P1，冗余 exists 探测）+ MC-2（P2，冗余 Table reload）**。表在 SQL 分析期早已解析、确定存在，这些重复全是浪费。

## 2. 修复（fix）

在 per-statement `MaxComputeConnectorMetadata` 实例上加一个 `Map<List<String>, MaxComputeTableHandle> tableHandleMemo`（`ConcurrentHashMap`），`getTableHandle` 改为经它 `computeIfAbsent`：

```java
MaxComputeTableHandle handle = tableHandleMemo.computeIfAbsent(
        List.of(dbName, tableName), k -> {
            if (!structureHelper.tableExist(odps, dbName, tableName)) {
                return null;            // 缺表：不记录映射 → 每次重探、语义不变
            }
            Table odpsTable = structureHelper.getOdpsTable(odps, dbName, tableName);
            TableIdentifier tableId = structureHelper.getTableIdentifier(dbName, tableName);
            return new MaxComputeTableHandle(dbName, tableName, odpsTable, tableId);
        });
return Optional.ofNullable(handle);
```

- 第一次解析：照常 `exists()` + build，存进 memo；第 2..N 次同表：命中 memo，**零远程往返**。→ MC-1：k×/语句 → 1×。
- memo 命中返回**同一个 handle（同一个惰性 Table）**，首次 reload 后后续所有消费点（读扫描 `MaxComputeScanPlanProvider`、写 `MaxComputeWritePlanProvider`、`getTableSchema`/`getColumnHandles`）复用已 reload 的 schema。→ MC-2 **随 memo 自然消解，无需单独改动**。
- **改动只 1 个连接器文件、约 20 行**；0 fe-core。

## 3. 关键取舍（诚实）

| # | 取舍 | 决定 | 理由 / 证据 |
|---|---|---|---|
| A | 只 memo 命中（present）还是也 memo 缺表（`Optional.empty()`）? | **只 memo present**（mapping fn 返回 `null` → CHM 不记录 → 缺表每次重探） | 缺表路径**逐字节等价今天**（每次探测、返回 empty），零行为变化；单语句内 create-then-resolve 同名今天不存在，memo 负结果是"没必要的脆弱"（侦察一致建议）。 |
| B | 是否顺手**删除** `exists()` 探测（更激进的 MC-1）? | **不删**，只去重第 2..N 次 | `tables().get()` 惰性、不验存在性；删 `exists()` 会把今天干净的 `Optional.empty()`（"table not found"）退化成后续 `getSchema()` 时抛的底层 `OdpsException`——**改错误语义**，超范围。目标 = k→1，非 k→0。 |
| C | `HashMap` vs `ConcurrentHashMap`? | **ConcurrentHashMap** | 同一 metadata 实例**跨线程可达**：`PluginDrivenScanNode` 在请求线程建 1 个 `connectorSession`（`:149`）解析 metadata 一次（`:206`），再把**同一 session** 丢进 off-thread 池任务（`scheduleExecutor` runAsync，分区批 `:1615-1628` / 流式 `:1700-1704`）→ 同 session=同 scope=同 metadata。memo 是该实例上**第一个可变跨线程字段**（其余 7 字段全 final），plain HashMap = 数据竞争。对齐 `ConnectorStatementScopeImpl`（CHM）+ iceberg FIX-PERF-07（CHM）先例。`computeIfAbsent` 原子性安全：supplier（tableExist+lazy get）**不回环** memo（两步都落到 ODPS SDK，无 Doris 回调），CHM 单键 computeIfAbsent 唯一禁忌=重入同 map，此处不触发。 |
| D | key 形态? | **`List.of(db, table)`**（List 值相等，无分隔符碰撞） | handle 无 `equals/hashCode`（只有 `serialVersionUID`），不能按 handle identity 做 key；`List.of` 值相等、无 import 新增（`List` 已导入）、非空实参安全；避免 `db+sep+table` 字符串分隔符碰撞推理。 |

## 4. 为何无需失效 / authz / 一致性工作

- **失效**：memo 是 per-statement，随 metadata 实例语句末 GC；无语句内 DDL 改已解析 handle 身份（DDL/DML 是分开的 Nereids 语句）。`createTable`（`:360-374`）/ `tableExists`（`:113-116`）/ `dropDatabase` **各自直调** `structureHelper.tableExist`、**不经 memo**（保持 DDL 存在性检查权威、不被读 memo 短路）。跨查询 `MaxComputePartitionCache`（长寿命 connector 上、TTL 600s、REFRESH 失效）与 memo **生命周期/key/载荷全正交**，无协调代码。
- **authz**：MaxCompute 一 catalog 一套**静态凭证**（AK/SK / RAM-Role-ARN / ECS-RAM-Role，`MCConnectorClientFactory:86-127`），无 per-user 凭证下发、无 session=user；名字为 key 的 memo **不跨用户泄漏**；`PluginDrivenMetadata` 另钉一语句=一身份。Layer-3 授权隔离不适用（伞形铁律 4 已核）。
- **写一致性**：写路径不自解析 handle——`planWrite` 复用上游已解析、经 memo 的同一 `MaxComputeTableHandle`（`MaxComputeWritePlanProvider:91-92,105`）；memo 令读/写共享**同一 Table 对象** → **严格提升**语句内一致性，非放松。

## 5. 验证

- **守门单测**（新 `MaxComputeConnectorMetadataHandleMemoTest`，仿 `DropDbTest` 手写记录 fake、无 Mockito、null odps 离线）：
  1. `sameTableResolvedTwiceProbesAndBuildsOnce`：同表解析 2 次 → `exists()` 探测 1 次 + `getOdpsTable` build 1 次 + 两 handle `assertSame`（MC-1+MC-2 守门；去掉 memo → 计数变 2 + handle 不同 → 红）。
  2. `distinctTablesAreResolvedIndependently`：不同表各解析一次、handle 不同（守 List 值相等 key）。
  3. `absentTableReprobesEachCallAndIsNeverMemoized`：缺表每次重探、恒 empty、从不 build（守取舍 A；若 memo 负结果 → 探测计数变 1 → 红）。
- **回归**：`mvn install -pl :fe-connector-maxcompute -am -Dmaven.build.cache.enabled=false -Dtest='MaxCompute*' -f <abs>/fe/pom.xml` → BUILD SUCCESS + 全 MaxCompute* 测试 0 失败。
- **净室对抗复审**：改动逐字节 parity（只减不改）+ 取舍 A/B/C/D 无坑。
- **e2e**：需集群，本地不跑，留标注（异构 + 独立 max_compute catalog 的 SELECT/分区裁剪/写路径解析计数）。

## 6. commit

`[perf](catalog) fe-connector-maxcompute: memoize resolved table handle per statement`

正文：root cause（~17 解析点各付一次冗余 ODPS `exists()` + 各自 Table reload）+ 修法（per-statement metadata 实例 CHM handle memo，computeIfAbsent，present-only）+ 度量（exists k×→1×、Table reload 每表 1×）+ 守门（build-count 单测 + parity + NONE 缺表对照）+ 0 fe-core。
