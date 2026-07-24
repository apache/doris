# perf-hotpath-jdbc — HP-1 + HP-2 (per-statement column-fetch dedup)

> 兄弟空间（镜像 `perf-hotpath-iceberg` 布局，两项同根合并为一页）。
> 权威病灶分析在伞形空间 `plan-doc/connector-cache-unification/connectors/jdbc.md`。

## 一句话
本地→远端列名映射靠 `client.getJdbcColumnsInfo`（真实 `DatabaseMetaData.getColumns` 远程往返）。一条语句内多处重复取：读侧 `getColumnHandles`（每 scan node ~2 次）+ 缓存 miss 的 `getTableSchema`；写侧 `buildInsertSql` 新建实例再取（`EXPLAIN INSERT` 触发 2 次）。全都没记忆化。修法 = 把两处远程取列收进**每语句作用域**记忆化，读写两条路自动共享。

## 动码前重侦察（HEAD d8e2541e567）
- `JdbcConnectorMetadata.getTableSchema` :109 / `getColumnHandles` :152 都收 `session`，都调同一 `client.getJdbcColumnsInfo(db,table)`（:119/:162）。
- 元数据实例每语句经漏斗单例共享（`PluginDrivenMetadata.get`）→ 读侧多次调用共享同一实例。
- 写侧 `JdbcWritePlanProvider.buildInsertSql` :136 `new JdbcConnectorMetadata(...).getColumnHandles(session, handle)`——**新建实例绕漏斗**，但实例无状态、取列恒远程。
- 关键洞见：记忆化放**语句作用域**（非实例），则写侧新建实例的 `getColumnHandles` 也命中同一条目 → **一个改动点修两处**。owner 拍板"语句作用域统一去重"（对齐 es 的 cross-path 先例，非 mc 的实例记忆化）。

## 实现（commit `7df22cd1c71`，连接器侧，0 fe-core）
- `fe-connector-api/ConnectorStatementScopes`：加命名空间常量 `JDBC_COLUMNS`（与 `ICEBERG_TABLE`/`HUDI_*`/`ES_INDEX_MAPPING` 并列；连接器 SPI，非 fe-core）。
- `JdbcConnectorMetadata`：两处 `client.getJdbcColumnsInfo(db,table)` 包进 `ConnectorStatementScopes.resolveInStatement(session, JDBC_COLUMNS, db, table, loader)`，记忆化**原始** `List<JdbcFieldInfo>`（session 无关），键 `(catalogId,db,table,queryId)`。各消费者按语义各自再套变换（handle 走 identifier mapper、schema 走类型转换）。
- `JdbcWritePlanProvider.buildInsertSql`：**仅 javadoc**——写路径 `getColumnHandles` 现共享该作用域条目（`EXPLAIN INSERT` 不再双取），无逻辑改动。
- NONE 作用域（离线/fe-core 跨查询 schema 缓存 loader）下 loader 每次跑 → 逐字节与改前一致。

## 净室复审发现（已在提交前修复）
`getTableSchema` 的 `jdbcTypeToConnectorType` 对某些日期类型**原地** `setAllowNull(true)`；现在共享同一 raw list → 修正 `JDBC_COLUMNS` javadoc 如实描述"幂等原地改写、仅 `getTableSchema` 读 allowNull 且总先重导"，并加一个**会变换的类型转换 double** 测试钉住该不变量（共享 memo 上两次 `getTableSchema` + 中间 `getColumnHandles` 输出稳定）。

## 验证
- `JdbcConnectorMetadataTest` +3（读侧去重、NONE 每次取、共享-mutable 不变量）。
- `JdbcWritePlanProviderTest` +2（写侧 planWrite+appendExplainInfo 去重、scan+write cross-path 去重证"作用域键非实例键"）；既有 NONE-作用域字节 parity 测试原样绿。
- 全模块 **199/199 绿**，checkstyle 0，BUILD SUCCESS。
- 3-lens 净室对抗复审：session-safety CLEAN、key/scope/composition CLEAN、iron-rules/tests 仅上述 mutation 不变量项（已修）。
- **e2e 需集群本地未跑**（留标注：jdbc 目录 INSERT 端到端 SQL 不变）。
