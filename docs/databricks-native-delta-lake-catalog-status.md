# Databricks Native Delta 当前进度与待办

更新时间：2026-10-02

最新状态：已 rebase 到本次拉取的 `origin/master`（`0c29961f45e`），并恢复原合并提交中未被普通 rebase 保留的 Delta SPI 迁移。已完成静态检查；新基线的 FE/BE 编译、单测和 SQL 回归尚未执行，仍待验收。下面的运行结果是历史证据，不代表本次代码已通过相同测试。

## 1. 当前结论

Doris 已经有一个可运行的实验性 native Delta vertical slice，核心路径是：

```text
Doris Delta connector
  ├── Path adapter       -> 直接读取用户指定的 Delta 路径
  └── Unity adapter      -> 按 catalog.schema.table 访问 Unity Catalog
       └── Delta Kernel   -> 解析 Delta log、checkpoint 和 snapshot
```

当前结论不是“已经完整支持 Databricks Delta”，而是：

1. path Delta 的读写和主要 copy-on-write DML 已有本地可运行实现。
2. Unity Catalog 的表发现、临时凭证、catalog-managed snapshot 和部分写入路径已有本地协议 fixture 验证。
3. 真实 Databricks workspace、三云存储、权限、ABAC 和长查询凭证续期还没有完成 E2E 验收。
4. 因此当前代码应视为实验性能力，不能直接宣称生产级 Databricks Delta 支持。

2026-09-09 已完成 FE 写协议修复、TRUNCATE 回归补充和 FileScannerV2 非空列范围谓词修复；当时 BE 构建被范围外的 ORC 工作区语法错误阻塞。该记录仅说明当时的阻塞，不能把旧扫描器下的 Unity 通过结果当作新扫描器已通过。

## 2. 已完成能力

状态标记：

- **代码/单测**：代码已实现，并有对应单测。
- **本地 fixture**：使用本地 Delta 文件或 Unity Catalog HTTP fixture 验证。
- **隔离集群**：在本地 FE/BE 隔离集群执行过 SQL 回归。
- **真实 Databricks**：使用真实 workspace 验证。

| 能力 | 当前状态 | 证据与边界 |
| --- | --- | --- |
| Delta catalog 入口 | 代码/单测 | 新增 `type = delta`，支持 `path` 和 `unity` 两种 adapter。 |
| Delta 格式解析 | 代码/单测 | 使用 Delta Kernel 4.3.1 解析 protocol、log、checkpoint、snapshot。 |
| Path Delta 读取 | 本地 fixture、隔离集群 | 复用 Doris Parquet reader；支持分区裁剪、删除向量和 column mapping 的受限路径。 |
| Unity external/managed 读取 | 本地 fixture | 按表名调用 Unity Delta REST；不绕过 Unity Catalog 猜测对象存储路径。 |
| catalog-managed snapshot | 本地 fixture | 通过 Unity catalog-held commit/log tail 读取，不退化为普通 path 扫描。 |
| 版本/时间点查询 | 本地 fixture、隔离集群 | 支持 `FOR VERSION AS OF` 和 `FOR TIME AS OF`；跨 schema/分区演进当前 fail-closed。 |
| Unity 表发现 | 本地 fixture | 支持 Databricks 官方 `securable_kind_manifest.capabilities`，Tables API 分页使用 `max_results=50`。 |
| 临时凭证 | 本地 fixture | AWS、Azure SAS、GCS OAuth 的初始凭证映射已验证；凭证过期时间会参与 FE 侧检查。 |
| Path Delta 创建 | 本地 fixture、隔离集群 | 支持空表 version 0 创建、直接列名分区和后续 append。 |
| Unity managed 创建 | 本地 fixture | 已验证 staging table、version 0 初始提交和 catalog finalize；真实 Databricks 创建仍未验证。 |
| INSERT | 本地 fixture、隔离集群 | 支持 path 和 Unity external/catalog-managed 的实验性 append。 |
| INSERT OVERWRITE | 本地 fixture、隔离集群 | 通过 Delta remove+add 原子替换 active files，并检查并发版本。 |
| DELETE | 本地 fixture、隔离集群 | copy-on-write；当前支持整表范围和 `WHERE`，不支持分区语法、CTE、子查询、`ORDER BY/LIMIT`。 |
| UPDATE | 本地 fixture、隔离集群 | copy-on-write；支持确定性表达式、别名、`IS NULL`，不支持 `FROM`、CTE、子查询和 `ORDER BY/LIMIT`。 |
| MERGE | 本地 fixture、隔离集群 | 支持 matched DELETE/UPDATE 和 not-matched INSERT；source 当前要求 Doris UNIQUE KEY 表，`ON` 覆盖全部 source key。 |
| TRUNCATE TABLE | 本地 fixture、隔离集群 | 复用空 overwrite transaction 清理全部 active files，只支持整表；Path 在默认扫描器、Unity 在旧扫描器下通过，Unity 默认 FileScannerV2 全流程仍待验证。 |
| Unity DROP TABLE | 本地 fixture、代码/单测 | 通过 Unity Delta API 删除 catalog 注册项；path catalog 不删除对象存储目录。 |
| Unity write capability | 本地 fixture、代码/单测 | 只有 `HAS_DIRECT_EXTERNAL_ENGINE_WRITE_SUPPORT` 的 Unity 表才进入 `READ_WRITE` credential/writer 路径。 |
| ABAC 安全边界 | 本地 fixture | 发现 `row_filter`、顶层 `column_masks` 或 `columns[].mask` 时 fail-closed；尚未实现 ABAC 正向执行。 |

## 3. 历史测试证据与本次验证

下表及 3.1、3.2 是 2026-09-09 的验证记录。本次 rebase 的结果见 3.3。

| 测试 | 结果 | 说明 |
| --- | --- | --- |
| Delta connector 单测 | 67/67（2026-09-09） | 覆盖 snapshot、feature、凭证、创建、写入、DML 和 Unity fixture；新增 TRUNCATE 历史保留、重复清空、旧 snapshot 拒绝和 writer protocol 检查。 |
| FE catalog TRUNCATE 定向测试 | 5/5（2026-09-09） | 覆盖 capability、远端 handle、分区拒绝、缺失远端表和 replay 缓存清理。 |
| FE catalog 其他定向测试 | 已通过 | 覆盖 create/drop、MERGE、overwrite snapshot 和 plugin capability。 |
| Native Delta path 回归 | 已通过（2026-09-09，默认 FileScannerV2） | 完整套件生成 `.out` 后以普通比较模式通过，包含读写、copy-on-write DML、TRUNCATE、历史读取和清空后再写入。 |
| Native Delta Unity 回归 | 已通过（2026-09-09，仅旧扫描器） | 设置会话级 `enable_file_scanner_v2=false` 后，完整套件生成 `.out` 并以普通比较模式通过；包含连续提交、清空、历史读取、拒绝提交和重试。 |
| FileScannerV2 非空列范围查询 | 修复待验收 | 默认 V2 下已复现 `declared=BOOL, actual=Nullable(BOOL)`；BE 修复及三个定向单测已补充，构建/执行未完成。 |
| 标准 `./build.sh --fe` | 已通过（2026-09-09） | 使用 `MAVEN_OPTS='-Xmx4g -Xms512m -XX:+UseSerialGC'`、单 Maven 线程完成 FE 编译和 Checkstyle，输出 `Successfully build Doris`。 |
| 标准 ASAN BE 构建及定向 BE 单测 | 阻塞，未通过 | 旧 PCH 缓存问题已处理。两路构建随后都停在本轮范围外的 `be/src/format_v2/orc/orc_search_argument.cpp:1253`：工作区改动删掉了 `normalized_expression` 函数名；该文件保持原样，未擅自恢复。新增 BE 单测尚未执行。 |
| 本轮三个 C++ 文件的格式检查 | 已通过（clang-format 16） | 使用仓库格式化/检查脚本检查隔离副本，并逐一确认与实际工作区文件内容相同；未格式化其他工作区改动。 |
| C++ 静态检查 | 已执行，未全绿 | 修正了同组已有单测的 COW 所有权编译错误和新增 helper 的嵌套三目。编译诊断/相关可读性规则定向复查不再报告本轮代码问题，但公共 `core/types.h` 的 `NOLINTEND` 及其他既有诊断仍在，不能据此宣称 BE 编译或单测通过。 |
| 隔离 FE/BE 集群 | 已通过（2026-09-09） | 新 FE 已启动，原有 BE、metadata 和 cluster ID 保留；实际查询结果为 45，查询端口为 19030。 |

### 3.1 2026-09-09 修复记录

- **连续写入**：Kernel 会在 catalog-managed 提交中启用依赖 feature `inCommitTimestamp`。原 Doris writer 白名单不接受它，导致表写入一次后不能继续写；现已补齐白名单，其他未支持 feature 仍拒绝。
- **TRUNCATE 协议检查**：TRUNCATE 不构造 BE writer，原来绕过了 `getWriteConfig` 的 writer protocol 检查。现在 overwrite/TRUNCATE 与普通写入共用协议检查；旧 snapshot、只读表和分区语法的拒绝仍保留。
- **非空列读取（待 BE 验收）**：FileScannerV2 将非空列的比较谓词改写成针对可空文件列的谓词，却保留原非空返回类型。修复让这类谓词先经过 TableReader 的可空性校验，再执行过滤；真实 NULL 仍必须报错。

BE 测试保留了所有非空约束和结果断言。两个矩阵测试因 GTest 宏展开产生复杂度告警，已添加注明原因、仅针对该规则的局部 `NOLINTNEXTLINE`；没有屏蔽编译或生产代码告警。静态检查记录为 `/tmp/doris-native-delta-mapper-tidy-20260909-resource-dir.log`、`/tmp/doris-native-delta-test-tidy-20260909-focused.log` 和 `/tmp/doris-native-delta-table-reader-test-tidy-20260909-final.log`。

Unity 测试样本也已校正：使用官方 capability manifest；初始 Parquet 确实包含 BIGINT 列 `id` 和非空值 1、2，而不是借用没有该列的 customer 文件。Parquet 物理列仍可能标记为可空，这正是 FileScannerV2 需要正确处理的映射。只有 mock catalog 接受的 staged commit 才会变成可见版本；这是本地提交可见性测试，不是完整 Unity 服务模拟或真实 Databricks 验收。

对应代码和测试：

- [Delta 写协议检查](../fe/fe-connector/fe-connector-delta/src/main/java/org/apache/doris/connector/delta/DeltaConnectorMetadata.java)、[TRUNCATE 单测](../fe/fe-connector/fe-connector-delta/src/test/java/org/apache/doris/connector/delta/DeltaConnectorTruncateTest.java)
- [Path SQL 回归](../regression-test/suites/external_table_p0/delta/test_native_delta_path.groovy)、[Unity SQL 回归](../regression-test/suites/external_table_p0/delta/test_native_delta_unity.groovy)
- [FileScannerV2 列映射](../be/src/format_v2/column_mapper.cpp)、[谓词映射单测](../be/test/format_v2/column_mapper_test.cpp)、[TableReader 单测](../be/test/format_v2/table_reader_test.cpp)

### 3.2 2026-09-09 验收阻塞记录

当时需要先修正 ORC 缺失函数名，再继续标准 ASAN BE 构建和 `ColumnMapperCastTest.*:TableReaderTest.*` 单测。当前 rebase 后源码已包含该函数名，但尚未重新构建，不能据此认定 BE 已通过。之后仍需部署新 BE，在默认 `enable_file_scanner_v2=true` 下运行 Path、Unity 两个完整回归套件；不能将关闭 V2 作为验收方案。

本机日志位于 `/tmp/`：`doris-native-delta-unit-green-20260909.log`、`doris-native-delta-build-20260909.log` 是 FE 成功记录；`doris-native-delta-be-build-20260909-current.log`、`doris-native-delta-be-unit-20260909-current.log` 是本次 BE 阻塞记录。回归比较记录分别为 `doris-native-delta-path-20260909-final-verify.log` 和 `doris-native-delta-unity-20260909-v1-verify.log`。

### 3.3 2026-10-02 master 迁移收尾

本次只完成兼容迁移，没有扩展 Delta 的产品能力：

- 恢复 Delta 到 `fe-connector-spi` 的接入，删除旧 `fe-connector-api` 残留；SPI 从上游 12.0 升至 13.0，并补齐 API 基线。
- 保留新版 master 的行级 DML 路由，在它之前处理 Delta 全表 copy-on-write；扫描、计数、覆盖写入复用同一基准快照。
- 文件提交回报接入新版集中序列化和最终回报确认流程，保留上游 opaque commit data；补齐空 overwrite、回报缺失和重试相关测试。
- 避让上游 Fluss 等新增 Thrift 字段编号；恢复 BE 当前/历史 schema 的 Delta field-ID 列查找。

已通过 FE connector 导入限制、FE metadata 访问限制和 BE header hygiene 检查。SPI、Delta connector、FE core 的 Checkstyle 已通过；三个修改过的 Thrift 文件已成功生成 Java 代码；相对上游变更的 43 个 C++ 文件已通过 clang-format 16 检查。静态检查不等于编译或测试通过。

`mvn validate` 整体未通过：FE core 依赖的新 `1.2-SNAPSHOT` 模块尚未在本地构建安装，依赖解析失败。随后独立运行 `checkstyle:check` 通过，没有绕过风格检查。记录位于 `/tmp/doris-delta-rebase-validate-20261001.log`、`/tmp/doris-delta-rebase-checkstyle-20261001.log`；最终补充测试后的检查记录为 `/tmp/doris-delta-rebase-checkstyle-20261002.log`、`/tmp/doris-delta-rebase-cpp-format-20261002.log`。

下一步先由用户执行标准 FE 编译（保留 Checkstyle、关闭 Maven 构建缓存）：

```bash
DISABLE_JAVA_CHECK_STYLE=OFF FE_MAVEN_THREADS=1 \
MVN_OPT='-Dmaven.build.cache.enabled=false' ./build.sh --fe
```

之后使用 `run-fe-ut.sh` 验证 SPI/Delta connector、COW 路由与快照、DDL、写入及 commit report 测试；BE 构建/单测和默认 FileScannerV2 下的 Path、Unity SQL 回归仍未执行。真实 Databricks workspace 验收仍是独立待办。

## 4. 待开发功能

这些项目是代码能力缺口，不应通过当前本地 fixture 的绿色结果推断为已支持。

### 4.1 长查询自动凭证续期

当前行为是：FE 检查 vended credential 是否覆盖 `query_timeout`/`insert_timeout` 加安全余量；覆盖不了就 fail-closed。BE 扫描过程中还没有从 Unity Catalog 重新申请并热更新凭证的完整链路。

需要补齐：

- FE/BE 的凭证刷新协议和生命周期管理；
- AWS 临时密钥、Azure SAS、GCS OAuth 的统一刷新行为；
- 正在读取的文件请求如何切换到新凭证；
- 刷新失败、并发刷新和 token 不写日志的处理。

### 4.2 Unity cross-engine ABAC 正向执行

当前对带 row filter/column mask 的表直接隐藏。后续如果要支持这类表，需要把 Unity 策略转换成 Doris 可执行的过滤/掩码表达式，并验证列权限、表达式语义和错误行为。不能只放开 discovery 而继续使用普通 Parquet 扫描。

### 4.3 Delta protocol 和 table features 扩展

当前只接受明确审核过的 feature。以下类型仍有不同程度限制或拒绝：

- generated/default/constraint columns；
- type widening、variant 和其他物理行变换 feature；
- schema evolution 与历史 schema 不一致的 time travel；
- 更高 reader/writer protocol；
- 新版本 checkpoint、deletion vector 或未来 Delta feature 的兼容性。

### 4.4 DML 语法和语义扩展

当前 DELETE/UPDATE/MERGE 是受限 copy-on-write 切片，不是完整 Delta SQL DML。后续可评估：

- 分区级 DELETE/TRUNCATE；
- 更完整的 MERGE source、CTE、子查询和表达式；
- 更丰富的 UPDATE/DELETE 语法；
- 更完整的 affected-row、schema evolution 和并发冲突语义。

### 4.5 Unity catalog 生命周期和维护操作

当前明确支持/不支持的边界：

- 支持实验性的 Unity DROP TABLE；
- path DROP 不会删除对象存储目录；
- table rename、属性修改、OPTIMIZE、VACUUM、ANALYZE 等外部 Delta client 能力尚未产品化；
- 不应把 Unity Catalog OSS/实验 API 的能力直接当成 Databricks 稳定承诺。

### 4.6 正式产品化

当前仍缺少稳定的配置兼容策略、版本兼容矩阵、升级/回滚说明、监控指标和真实云环境验收，因此不能把实验性 `delta` connector 直接标为完整生产支持。

## 5. 待验证功能

### 5.1 Databricks workspace 验证

至少需要一个启用 Unity Catalog 的真实 workspace，以及一个具有外部访问权限的测试 principal。需要验证：

- `EXTERNAL USE SCHEMA`、`SELECT`、`MODIFY`、`CREATE` 等权限；
- external Delta、managed Delta、catalog-managed Delta；
- 只读 capability 和可写 capability 的差异；
- Unity API 返回的官方 capability manifest、table metadata 和 credential response；
- catalog commit 与并发 writer 冲突；
- CREATE、append、overwrite、DELETE、UPDATE、MERGE、TRUNCATE、DROP 的实际结果。

### 5.2 三云存储验证

分别验证 AWS S3、Azure ADLS Gen2、GCS：

- storage firewall、Private Link/VNet、Doris FE/BE 出口网络；
- temporary credential 的实际格式和有效期；
- Parquet、Delta log、checkpoint、deletion vector 的读取；
- credential 到期前刷新和到期后失败行为。

### 5.3 互操作验证

用 Databricks/Spark 写入或修改表，再用 Doris 读取；再用 Doris 写入后由 Databricks/Spark 读取。重点比较：

- snapshot version 和 active file 集合；
- null、分区值、时间戳、decimal、复杂类型；
- concurrent commit 冲突和重试；
- catalog-managed 表是否始终读取到 Unity Catalog 发布的最新 snapshot。

## 6. 明确不应宣称的能力

在真实 Databricks E2E 通过前，不应对外宣称以下内容：

- “完整支持 Databricks managed Delta”；
- “支持所有 Unity Catalog Delta 表”；
- “支持带 row filter/column mask 的表”；
- “支持长查询自动 credential refresh”；
- “支持完整 Delta SQL DML 和所有 table features”；
- “path Delta 与 Unity Catalog Delta 具有相同治理语义”。

## 7. 关联文档

- [Native Delta 方向性调研](databricks-native-delta-lake-catalog-research.md)
- [Databricks Azure Iceberg 联调环境清单](databricks-azure-iceberg-e2e-setup.md)
- [Databricks Tables API](https://docs.databricks.com/api/workspace/tables/list)
- [Databricks Unity REST 外部 Delta client](https://docs.databricks.com/aws/en/external-access/unity-rest)
- [Databricks credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)
- [Delta Kernel](https://docs.delta.io/delta-kernel/)
