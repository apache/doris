# HANDOFF — fe-core 数据源依赖与残留代码清理

> 独立任务空间，仅覆盖"清理 fe-core 数据源依赖 + 残留代码"这一件事。与仓库根 `HANDOFF.md`（catalog-SPI 主线）无关，勿混。
> **本文件是活文档**：每完成一轮就更新"进度日志" + commit（对齐 HANDOFF 纪律）。

- 创建：2026-07-21 · 分支：`catalog-spi-review-17`
- 当前状态：**Batch 1 + Batch 2 已完成并提交**（fe-core test-compile 绿）。下一个 session 从 **Batch 3**（iceberg-AWS 依赖簇整体移除，前置=迁走 5 个测试类）起步；Batch 4（依赖定性）可随时穿插；Batch 5（LIVE 迁移）是独立设计工程。

---

## 0. 下一个 session 怎么起步（必读）

1. **先读三份文档**（顺序）：本 `HANDOFF.md` → [`TASKLIST.md`](./TASKLIST.md) → [分析文档](./fe-core-datasource-deps-and-code-cleanup-2026-07-21.md)。
2. **别信行号，信内容**：分析文档/任务里的 `file:line` 是 2026-07-21 快照，代码会漂移。动手前用 grep 按**符号名/内容**重新定位，对照真实代码 review 一遍再改。
3. **并发踩踏探测**（本仓是 linked worktree，可能有并行 session）：动码前查 `git log --oneline -5` + `git status` + 有无活跃 maven 进程 + 近 90s 内是否有文件被改；发现活跃就只写新文件、小步快提交。
4. **建基线**：先对 fe-core（及将碰的模块）跑一次编译，确认起点是绿的，再改。
5. 从 **Batch 1** 开始（零风险），逐批推进；每批完成即更新本文件进度日志 + 独立 commit。

---

## 1. 已确定的决策速览

> 完整证据、判定依据、`file:line` 全在[分析文档](./fe-core-datasource-deps-and-code-cleanup-2026-07-21.md)。这里只给"要做什么"。

### 依赖（`fe/fe-core/pom.xml`）

| 动作 | 依赖 | 备注 |
|---|---|---|
| **直接删** | `lakesoul-io-java`、`scala-library`（provided） | 废弃 lakesoul，零引用 |
| **删（随 iceberg 批）** | `iceberg-core`、`iceberg-aws`、`glue`、`s3tables`、`s3-tables-catalog-for-iceberg`、`aws-json-protocol` | **前置=先迁走 5 个 iceberg 测试类** |
| **换/删（随 iceberg 批）** | `parquet-avro`→`parquet-hadoop(+parquet-column)`；删 `avro` 显式声明 | avro runtime 仍由 hive-exec 供给，可接受 |
| **只改注释（保留）** | `kryo-shaded`（"for hudi catalog"→`WorkloadSchedPolicy`）；`avro`/`parquet-avro`（"For Iceberg"→parquet reader） | 注释错，依赖对 |
| **保留（S3/凭证/UDF 需要）** | `s3-transfer-manager`、`sts`、`url-connection-client`、`protocol-core`、`sdk-core`、`hive-exec(runtime)`、`commons-lang(runtime)`、`mariadb`、`ranger-plugins-common`、`HikariCP`、`okhttp` | 非数据源用途，别当"数据源清理"删 |
| **先调查再定** | `aws-java-sdk-dynamodb`、`aws-java-sdk-logs`、`bce-java-sdk`、`postgresql(provided)` | 见 Batch 4 |

### 代码（fe-core 残留数据源特有类/逻辑）

- **真·死代码（可直接删）**：iceberg `StatisticsUtil.getIcebergColumnStats`+`getColId`、`UnboundIcebergTableSink`+分支、`IcebergInsertCommandContext`；hive `HiveInsertCommandContext`。
- **LIVE 源特有逻辑（需迁移设计，勿删）**：iceberg 行级 DML 簇（~15 文件）、legacy `engine=hive` 簇、`ranger-hive` 授权包、hudi `hudi_meta` TVF、paimon/es 的 `CreateTableInfo` 分支、`Coordinator` 按源 if-链、`AzureProperties.isIcebergRestCatalog`、`DatasourcePrintableMap` maxcompute 遮蔽、es 兼容桩。
- **trino**：已迁干净，无需动。

---

## 2. 架构铁律（改代码时必须守）

1. **fe-core 源相关代码只减不增**。清理期不得往 fe-core 加逻辑。
2. **禁"就近搬迁"**：为"删 A 能编译过"而把逻辑挪进 fe-core util，是违规。遇到这种依赖，停手重新分析真实归属（源特有→连接器 SPI 委派；真通用→留框架），交 review。
3. **fe-core 不解析属性**：storage 属性→fe-filesystem、meta 属性→fe-connector。所以 `HiveTable`/`HMSResource` 的属性解析属于"迁移"而非"删除"。
4. **LIVE 源特有逻辑走 SPI 委派，不是删**：iceberg 行级 DML 那一大块是未迁移特性，Batch 5 是独立设计工作，别当死代码处理。
5. 通用 SPI 节点保持 connector-agnostic：按源名分支跑源特有逻辑=违规；按名 dispatch 到插件=允许。

---

## 3. 构建 / 验证方法

- **Maven 用绝对 `-f`**（cwd 跨调用持久、`cd` 会破相对路径 & 触发权限提示）。例：
  `mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am compile`
- **`-DskipTests` 仍会编译测试**：删测试依赖（如 iceberg 簇）后，务必跑到**测试编译**阶段才算验证通过（`cannot find symbol` 会在这里暴露）。
- 后台 task 通知里的 "exit code" 是 echo 的、不是 maven 的——要读输出里的 `BUILD SUCCESS/FAILURE` 行。
- **门禁**（validate 阶段会跑，别误触）：`check-fecore-metadata-funnel.sh`、`check-connector-imports`、`check-authz-cache-sharding.sh`。删代码一般不碰，但若报错先看是不是已知误报（如 HMS `HiveVersionUtil` gate 误报）。
- 每批的"验证"栏见 [`TASKLIST.md`](./TASKLIST.md)。

---

## 4. 风险与注意

- **iceberg 依赖删除的真门槛是 5 个测试类**（`AWSTest`/`IcebergGlueRestCatalogTest`/`IcebergUnityCatalogRestCatalogTest`/`IcebergDlfRestCatalogTest`/`S3TablesTest`），不是主源码。迁走/删掉它们之前，删依赖会挂测试编译。
  - **2026-07-21 复核纠正原分析定性**：这 5 个类**不是** "连接器 property 解析测试"——它们 `import org.apache.doris` **零命中**，直接 `new` iceberg SDK 的 `GlueCatalog`/`RESTCatalog`/`S3TablesCatalog` 打**真实外部服务**，测的是 iceberg 库本身、**不测任何 Doris 代码**。其中 4 个 `@Disabled`（需真 AWS/Databricks/DLF 凭证，CI 零覆盖）；第 5 个 `S3TablesTest` 未 @Disabled 但**无任何断言**、吞掉所有异常、连的是开发者私有 bucket（`yy-s3-table-bucket`）→ CI 里恒"通过"的 no-op。故**迁移价值≈0**，倾向**直接删**（连接器已自带 iceberg-aws/rest/s3tables 全部依赖，无需迁 scaffolding）。**待用户拍板 migrate-vs-delete**。
- **`avro` 删除有耦合**：受 `iceberg-core`(compile) 与 `hive-exec`(runtime) 两处传递依赖约束，须排在 iceberg 移除**之后**、与 parquet-avro 替换**一起**做；且 avro 永远会留在 runtime（hive-exec），删的只是"给 iceberg"的显式声明。
- **`DatasourcePrintableMap` maxcompute**：不能直接删 import，会让老 MaxCompute catalog 的 `SHOW CREATE CATALOG` 泄露 `mc.secret_key`；须改成字符串字面量 `"mc.secret_key"`（仿 DLF/iceberg-REST）。
- **es 兼容桩**（`EsTable`/`EsResource`）碰持久化镜像反序列化，放最后或长期保留。
- **postgresql(provided)** 删前要处理 `JdbcResourceTest`。

---

## 5. 进度日志（每轮追加，勿删历史）

| 日期 | 批次/任务 | 结果 | commit | 备注 |
|---|---|---|---|---|
| 2026-07-21 | 分析 + 建任务空间 | ✅ 完成分析文档 + HANDOFF/TASKLIST/README | 48323416d5c | 尚未动代码 |
| 2026-07-21 | Batch 1 零风险依赖删除 | ✅ 删 lakesoul/scala/**postgresql**（provided），test-compile 绿 | 76e6d5fcf2d | **偏差**：postgresql 不再"暂缓"——实证它在 fe-core 里只是 `JdbcResourceTest` 的字符串字面量 `"org.postgresql.Driver"`，无 `import org.postgresql`，资源创建/校验/回放路径零 `Class.forName`/`DriverManager`；provided scope 本就不进生产 runtime，故随本批一起删。 |
| 2026-07-21 | Batch 2 死代码 + 注释纠错 | ✅ 删 iceberg/hive 死写路径 + `getIcebergColumnStats`；改 kryo 注释；test-compile 绿 | 0102a022341 | **偏差 1**：TASKLIST 漏了 `SinkVisitor.visitUnboundIcebergTableSink`（`UnboundIcebergTableSink` 的第 4 个引用者），已一并删（无 override）。**偏差 2**：删死方法后 `ColumnStatisticBuilder`/`java.util.Optional` 变未用（checkstyle 报），一并删。**偏差 3**：`avro`/`parquet-avro` 的"For Iceberg"注释**未**在本批改——那两个依赖 Batch 3 会删/换，注释随之改，避免立即被推翻的 churn。**保留**：`PlanType.LOGICAL_UNBOUND_ICEBERG_TABLE_SINK` 枚举常量留着（删枚举项会移位 ordinal，风险>收益）。 |
| 2026-07-21 | Batch 1+2 对抗复核 | ✅ 3 个目标全 `DEAD_CONFIRMED`（0 可达） | — | 3 个对抗 agent 逐通道反证：parser/工厂、反射、枚举 ordinal、GSON/thrift、visitor override、ServiceLoader 均无命中。关键旁证：删后 `rg org.apache.iceberg fe/fe-core/src/main` = **空**（fe-core 主源码对 iceberg 零编译引用，只剩 5 个测试类钉住依赖）；`PlanType.LOGICAL_UNBOUND_ICEBERG_TABLE_SINK` 经 JSON explain 按**名**用非 ordinal，留着惰性无害；live 写路径 `PluginDrivenInsertCommandContext` 覆盖已删 context 的全部字段。 |
