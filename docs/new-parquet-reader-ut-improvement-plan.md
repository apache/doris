# New Parquet Reader UT Improvement Plan

本文档评估 Doris new parquet reader 当前 UT 覆盖方式，并给出更合理的测试分层、数据构造方法和落地优先级。

目标不是追求形式上的 100% 行覆盖率，而是让测试能够发现 new parquet reader 最容易出错的真实问题：schema 兼容、definition/repetition level 物化、投影/过滤交互、row group/page pruning、delete predicate 以及 schema evolution 组合。

## 当前覆盖方式评估

当前测试分层大体合理：

| 层级 | 代表文件 | 当前价值 |
|---|---|---|
| Schema resolver UT | `be/test/format_v2/parquet/parquet_schema_test.cpp` | 直接构造 Parquet schema node，验证 `ParquetColumnSchema` 的 kind、type、level 和非法 schema 拒绝。速度快，适合覆盖 schema 分支。 |
| Type resolver UT | `be/test/format_v2/parquet/parquet_type_test.cpp` | 覆盖 physical/logical/converted type 到 Doris type 的映射。 |
| Leaf value UT | `be/test/format_v2/parquet/parquet_leaf_reader_test.cpp` | 覆盖 nullable spacing、binary/fixed/bool/float16 等 leaf append 细节。 |
| Column reader UT | `be/test/format_v2/parquet/parquet_column_reader_test.cpp` | 用 Arrow writer 生成真实 parquet 文件，覆盖 scalar/struct/list/map 的 read、skip、select、overflow。 |
| File reader UT | `be/test/format_v2/parquet/parquet_reader_test.cpp` | 覆盖 open/read、多 row group、predicate selection、statistics/dictionary/page index pruning、row position、delete predicate。 |
| Table reader UT | `be/test/format_v2/table_reader_test.cpp` | 覆盖 table schema 到 file schema mapping、aggregate pushdown、default value、Iceberg delete/virtual column 等跨层行为。 |

这个方向是正确的，但目前有三个明显缺口：

1. Schema 兼容测试和真实读取测试之间缺少桥接。`parquet_schema_test.cpp` 可以证明 legacy LIST/MAP schema 被解析成期望的 tree，但不能证明 `ListColumnReader`、`MapColumnReader` 可以正确消费对应 def/rep levels。
2. 真实 parquet 文件主要由 Arrow writer 生成。Arrow 生成的文件通常符合标准 layout，不能充分代表 Hive、Spark、old parquet-mr、旧 Doris 或其它 legacy writer 的 schema 形态。
3. 异常路径和组合路径覆盖不足。比如 optional map key 被 schema 接受后，真实数据中 key 为 null 必须在 materialize 阶段报错；key/value stream 不对齐、invalid repeated level、non-nullable complex column 读到 null 等 corruption 路径需要专门测试。

## 改进原则

1. 按风险分层测试，不用单一大 fixture 覆盖所有逻辑。
2. Schema resolver 只验证 schema 归一化，不承担真实读取正确性的证明。
3. Def/rep level materialization 要有直接单测，避免所有边界都依赖真实 parquet 文件构造。
4. 对 legacy layout 使用 golden parquet corpus，而不是只用 Arrow writer 动态生成。
5. Reader 集成测试覆盖跨模块行为，避免在 SQL regression 中验证过多 BE 内部细节。
6. SQL regression 只保留用户可见和跨层最关键路径，避免回归测试过慢。

## 推荐测试分层

### L0: Schema Resolver Table-Driven UT

位置：`be/test/format_v2/parquet/parquet_schema_test.cpp`

职责：覆盖 `parquet_column_schema.cpp` 的 schema 归一化规则。建议把 LIST/MAP case 整理成 table-driven 形式，每个 case 明确：

- 输入 schema layout
- 是否成功
- top-level kind/type/nullability
- child kind/name/type/nullability
- definition/repetition level
- error message 关键字

必须覆盖的 schema 形态：

| 类别 | Case |
|---|---|
| LIST 标准格式 | Standard 3-level list: `optional group a (LIST) { repeated group list { optional int32 element; } }` |
| LIST legacy | repeated primitive, repeated group named `array`, repeated group named `<list_name>_tuple`, repeated group with multiple children |
| LIST wrapper 判定 | repeated group with logical annotation, repeated group whose only child is repeated, repeated group whose only child is optional scalar |
| Bare repeated | repeated primitive field, repeated group field inside struct |
| MAP 标准格式 | required/optional outer map, required/optional value |
| MAP 兼容格式 | optional key accepted at schema level, `MAP_KEY_VALUE` converted annotation |
| Invalid schema | LIST outer has zero/multiple children, non-repeated LIST child, MAP outer has zero/multiple children, primitive MAP entry, non-repeated MAP entry, entry child count not equal to 2, repeated outer LIST/MAP in normal mode |
| Unsupported type | UTC TIME rejection, unsupported physical/logical type |

L0 的验收标准：schema branch 新增或修改时，必须有对应 table-driven case；但 L0 通过不代表 reader 行为充分。

### L1: Def/Rep Level Materializer UT

位置建议：

- `be/test/format_v2/parquet/parquet_nested_materializer_test.cpp`
- 或拆分为 `parquet_list_column_reader_test.cpp`、`parquet_map_column_reader_test.cpp`

职责：用 fake child reader 直接喂 definition levels、repetition levels 和 leaf values，验证 `ListColumnReader` / `MapColumnReader` 的 offsets、nullmap、child values、cursor 和错误路径。

这种方式比构造真实 parquet 文件更适合覆盖边界，因为 def/rep level 是复杂类型 reader 的核心输入。

建议增加测试工具：

```cpp
class FakeNestedColumnReader final : public ParquetColumnReader {
public:
    Status load_nested_batch(int64_t rows) override;
    Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                               int64_t* values_read) override;
    const std::vector<int16_t>& nested_definition_levels() const override;
    const std::vector<int16_t>& nested_repetition_levels() const override;
    int64_t nested_levels_written() const override;
};
```

必须覆盖的 materialize case：

| 类别 | Case |
|---|---|
| LIST 正常路径 | null list, empty list, list with values, list with null element, consecutive repeated elements |
| LIST 操作 | read 分批、skip 后 read、select 非连续行、select 跨 overflow 边界 |
| LIST 异常 | first level has `rep_level == list.repetition_level`, non-nullable LIST 读到 null, child value count 不匹配 |
| MAP 正常路径 | null map, empty map, one entry, multiple entries, nullable value, complex value |
| MAP 操作 | read 分批、skip 后 read、select 非连续行、value scalar path 和 complex value path |
| MAP 异常 | null key, value stream ended before key stream, key/value repetition level 不对齐, key count 不匹配, value count 不匹配, non-nullable MAP 读到 null |

L1 的验收标准：`ListColumnReader::build_nested_column()` 和 `MapColumnReader::build_nested_column()` 的主要分支必须有直接 UT；corruption path 不能只靠真实文件偶然触发。

### L2: Golden Parquet Corpus UT

位置建议：

- 数据文件：`be/test/exec/test_data/parquet_v2_compat/`
- 测试文件：`be/test/format_v2/parquet/parquet_compat_corpus_test.cpp`

职责：保存小型真实 parquet 文件，覆盖非 Arrow 标准 writer 或难以用 Arrow writer 生成的 legacy layout。每个文件控制在几十行以内，配套记录 schema 来源和 expected output。

建议文件来源：

| 来源 | 覆盖目标 |
|---|---|
| Arrow writer | 标准 LIST/MAP、page v2、dictionary/plain、不同 row group/page size |
| Spark | Spark nested list/map schema、nullable struct/list/map 混合 |
| Hive/parquet-mr | legacy two-level list、optional map key、`array` / `bag` / `key_value` 等命名兼容 |
| 手工生成 | malformed-but-parseable def/rep level edge case，或特殊 converted annotation |

Golden 文件命名建议：

```text
be/test/exec/test_data/parquet_v2_compat/
  list_two_level_repeated_primitive.parquet
  list_tuple_struct_element.parquet
  list_repeated_group_with_logical_map_element.parquet
  map_optional_key_no_null.parquet
  map_optional_key_with_null.parquet
  map_value_list_nullable.parquet
  nested_list_struct_map_list.parquet
  README.md
```

每个 corpus case 至少验证：

- `get_schema()` 输出是否符合预期
- full read 输出是否符合预期
- projection read 输出是否符合预期
- skip/select 后输出是否符合预期
- 预期失败文件是否返回明确错误

L2 的验收标准：每一个 schema compatibility rule 至少有一个真实 parquet 文件证明 reader 可以消费该 layout。

### L3: New Parquet Reader Integration UT

位置：`be/test/format_v2/parquet/parquet_reader_test.cpp`

职责：覆盖 file reader 层的组合行为，不重复 L1 的低层 def/rep 细节。

建议补充或保留以下组合：

| 类别 | Case |
|---|---|
| Projection + predicate | `SELECT s.b WHERE s.a > x` 对应 file-local projection 与 predicate projection 合并 |
| Complex non-predicate select | predicate 过滤后，非谓词复杂列通过 selection vector 读取 |
| Row group/page pruning + complex projection | page index 缩小 row ranges 后，list/map/struct 输出行数和 offsets 正确 |
| Dictionary/statistics pruning | nested scalar leaf predicate 可 prune，但 repeated leaf 不做错误 aggregate/pruning |
| Delete predicate | delete predicate 和 query predicate 同时作用时 row position、selection、输出列一致 |
| Timestamp TZ | timestamp tz mapping 后 schema、read、min/max pushdown 一致 |
| Reopen split | 同一个 reader reopen 不残留 selection、cast、predicate projection、page skip state |

L3 的验收标准：跨 reader state 的行为必须有 UT，尤其是 reopen、filter 后 selection、page skip 后 output column 不 double skip。

### L4: Table Reader And SQL Regression

位置：

- `be/test/format_v2/table_reader_test.cpp`
- `regression-test/suites/external_table_p*_parquet/` 或现有 parquet 外表相关目录

职责：覆盖用户可见行为和 FE/BE 接口组合，不在 regression 中验证 BE 内部 offset/nullmap 细节。

建议保留少量高价值 SQL regression：

| 场景 | SQL 覆盖 |
|---|---|
| Legacy LIST/MAP 文件可读 | `SELECT *`, `SELECT nested_child`, `WHERE nested_child predicate` |
| Schema evolution | missing nested child with default, reordered/renamed nested field |
| Predicate pushdown 正确性 | row group/page pruning 开关开启时结果与关闭时一致 |
| Aggregate pushdown 正确性 | `count`, `min`, `max` 对 flat leaf 和 supported nested single leaf 正确；repeated leaf fallback |
| Iceberg/Paimon delete | delete vector / position delete / equality delete 与 parquet reader 组合结果正确 |

L4 的验收标准：新增用户可见兼容能力时必须有 SQL regression；纯内部 refactor 不强制补 SQL regression，但需要 L0-L3 覆盖。

## 覆盖矩阵

下面的矩阵用于判断新改动应该补哪一层测试。

| 逻辑区域 | L0 Schema | L1 Def/Rep | L2 Corpus | L3 Reader | L4 SQL |
|---|---:|---:|---:|---:|---:|
| Parquet type mapping | 必须 | 不需要 | 可选 | 可选 | 可选 |
| LIST/MAP schema compatibility | 必须 | 可选 | 必须 | 可选 | 必须覆盖用户可见新增能力 |
| Bare repeated field | 必须 | 必须 | 必须 | 可选 | 可选 |
| List offsets/nullmap | 不足 | 必须 | 必须 | 必须 | 可选 |
| Map offsets/nullmap/key validation | 不足 | 必须 | 必须 | 必须 | 可选 |
| Projection pruning | 可选 | 可选 | 必须 | 必须 | 必须覆盖用户可见路径 |
| Predicate selection | 不需要 | 可选 | 可选 | 必须 | 必须覆盖关键路径 |
| Statistics/dictionary/page pruning | 不需要 | 不需要 | 可选 | 必须 | 结果一致性必须 |
| Aggregate pushdown | 不需要 | 不需要 | 可选 | 必须 | 必须 |
| Delete predicate / row position | 不需要 | 不需要 | 可选 | 必须 | Iceberg/Paimon 必须 |
| Error/corruption path | 必须覆盖 schema error | 必须覆盖 materialize error | 必须覆盖真实坏文件 | 可选 | 可选 |

## 推荐优先级

### P0: 立即补齐的正确性保护

1. 为 legacy LIST schema 增加真实读取 corpus：
   - repeated primitive list
   - `<list_name>_tuple` struct element
   - repeated group with multiple children
2. 为 optional MAP key 增加两类真实读取：
   - optional key 但所有 key 非 null，读取成功
   - optional key 且存在 null key，读取失败并包含 `contains null key`
3. 增加 fake def/rep level materializer UT：
   - list null/empty/null element/multi element
   - map null/empty/null value/multi entry/null key
4. 增加 skip/select 覆盖：
   - legacy list corpus 上执行 skip/select
   - map value list 或 list struct map list 上执行 select

### P1: 组合路径保护

1. Projection + predicate 同时命中同一 nested struct 的不同 child。
2. Page index pruning 后读取 complex output column，验证没有 double skip。
3. Row group statistics/dictionary pruning 后从后续 row group 读取 nested column。
4. Reopen split 后 predicate projection、selection vector、page skip plan 不残留。

### P2: 完整性和长期质量

1. 建立 `parquet_v2_compat` corpus README，记录文件生成方式、writer 版本、schema、预期行为。
2. 对 changed files 定期跑 coverage，关注 branch coverage，不只看 line coverage。
3. 对 schema resolver 增加 table-driven case，减少散落 assert。
4. 对 materializer 增加 fuzz/property-style 小范围测试：随机生成合法 list/map rows，转换为 def/rep levels 后读回比较原始 logical rows。

## 测试数据构造建议

### 动态生成数据

适合：

- Arrow 标准 schema
- row group/page size 控制
- dictionary/plain/page index/statistics 行为
- type mapping 常规 case

优点是无需维护二进制文件，case 可读性高。

缺点是不能覆盖大量 legacy writer layout。

### Golden parquet 文件

适合：

- Hive/Spark/parquet-mr legacy LIST/MAP schema
- Arrow writer 不容易生成的 converted annotation
- malformed-but-parseable 文件
- 兼容性回归保护

要求：

1. 文件尽量小，通常 3 到 20 行。
2. 配套 README 说明生成命令、writer 版本、schema、逻辑数据。
3. 不在 UT 中依赖外部网络或外部服务。
4. 预期结果在 C++ UT 中直接断言，SQL regression 的 `.out` 仍由 regression 脚本生成。

### Fake reader 数据

适合：

- def/rep level 边界
- corruption path
- cursor/overflow 状态
- non-nullable output 遇到 null

要求：

1. fake reader 只模拟 `ParquetColumnReader` 必需接口。
2. 每个 case 明确输入 levels 和 expected logical rows。
3. 错误 case 检查 `Status` 类型和关键错误文本。

## 验收标准

一个 new parquet reader 改动合入前，建议满足：

1. 改动 schema resolver：至少补 L0；如果新增兼容能力，补 L2；如果用户可见，补 L4。
2. 改动 list/map/struct reader：至少补 L1 和 L3；涉及 legacy layout 时补 L2。
3. 改动 pruning/predicate/aggregate：至少补 L3；用户可见 SQL 语义补 L4。
4. 改动 table reader mapping/schema evolution：至少补 `table_reader_test.cpp`，必要时补 L4。
5. 新增 error handling：必须有负向 UT，不能只依赖代码审查。

推荐执行命令：

```bash
./run-be-ut.sh --run '--filter=ParquetSchemaTest.*'
./run-be-ut.sh --run '--filter=ParquetColumnReaderTest.*:NewParquetReaderTest.*:ParquetScanTest.*'
./run-be-ut.sh --run '--filter=TableReaderTest.*'
```

对重要重构或发布前验证，建议执行：

```bash
./run-be-ut.sh --run '--filter=Parquet*:*TableReaderTest*' --coverage
```

如果本地工具链无法执行 UT，需要在提交说明或 PR 中明确说明失败原因，并在 CI 或可用环境补跑。

## 不建议的方式

1. 不建议用更多 schema-only case 替代真实读取 case。schema 正确不等于 reader 正确。
2. 不建议只用 Arrow writer 动态生成文件证明 compatibility。兼容性问题通常来自非 Arrow writer。
3. 不建议把所有复杂类型组合塞进一个巨大 fixture 后只断言少量输出。失败定位困难，覆盖意图不清晰。
4. 不建议把内部 def/rep level 边界全部放到 SQL regression。执行慢、定位差、难覆盖异常路径。
5. 不建议用 100% line coverage 作为合入门槛。更合理的是 changed branch coverage + 风险矩阵覆盖。

## 最小落地计划

第一阶段只需要完成 P0：

1. 新增 `parquet_nested_materializer_test.cpp`，覆盖 list/map def/rep 核心正常和异常路径。
2. 新增 `be/test/exec/test_data/parquet_v2_compat/README.md` 和 4 到 6 个小型 golden parquet 文件。
3. 新增 `parquet_compat_corpus_test.cpp`，对 golden 文件做 schema/full read/projection/skip/select 断言。
4. 将现有 `parquet_schema_test.cpp` 中 LIST/MAP schema case 整理为 table-driven 或至少按类别分组。

完成第一阶段后，才能较有信心地说 new parquet reader 的关键逻辑有有效测试保护；否则当前 UT 只能证明主路径和部分 schema 分支，不能充分发现 legacy compatibility 和 complex materialization 的问题。
