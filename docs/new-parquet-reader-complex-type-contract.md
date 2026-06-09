# New Parquet Reader 复杂类型完整功能契约

本文定义 Doris new parquet reader 最终完整复杂类型能力的实现契约。它承接
`complex-column-predicate-and-stats-filtering.md` 已落地的第一阶段方案，以及
`new-parquet-reader-complex-type-gaps.md` 中梳理的功能缺口。

完整复杂类型能力的核心不是扩大单点 case，而是建立统一的 Dremel shape/value
模型，使复杂列读取、nested projection、schema evolution、file-layer pruning 和 lazy
materialization 都遵守同一套语义。

## 1. 总体目标

最终 new parquet reader 应支持以下能力：

- 完整读取 `STRUCT`、`LIST`、`MAP` 及其任意合法嵌套组合。
- 支持 nullable complex node 的 parent null shape，即使该 node 没有直接 scalar child。
- 支持 nested projection，包括 output child、filter-only child、schema evolution child 的合并。
- 支持 nested row-level predicate localization，但不改变 Doris file block 的 top-level complex
  column layout。
- 过渡期只支持 `STRUCT` / nested `STRUCT` 下 non-repeated primitive leaf 的 nested file-layer
  pruning。
- 支持 complex column lazy materialization，predicate child 和 payload child 能按 selected rows
  对齐。
- 支持 nested schema evolution，包括 child rename、reorder、missing/default child，以及多文件
  split-local schema 不一致。

## 2. 基本不变量

### 2.1 reader 边界

`FileReader` / `ParquetReader` 只能消费 file-local request：

- 不理解 FE slot id、FE column unique id。
- 不理解 table/global schema。
- 不理解 partition/default/generated column 的 table 层语义。
- 不重新解释 table-side nested field id/name。

table/global schema 到 file-local schema 的递归映射必须在 `TableColumnMapper` 完成。

### 2.2 block layout

file-local block 仍只为 top-level file column 分配 block slot：

- top-level `STRUCT` 输出为一个 `ColumnStruct`。
- top-level `ARRAY` 输出为一个 `ColumnArray`。
- top-level `MAP` 输出为一个 `ColumnMap`。
- nested child 不注册成独立 block slot。
- `struct_element(...)` 不改写成 child `VSlotRef`。

这保证 `TableReader` finalize 阶段仍通过 `ResultColumnMapping` 从 top-level complex column 中
取 child，而不是依赖额外 hidden slot。

### 2.3 filter 语义

row-level filter 和 file-layer pruning 必须分离：

- row-level filter 使用 localized `Expr` / `VExprContext`。
- `ColumnPredicate` 只作为 file-layer pruning hint。
- file-layer pruning 只能在能证明不会漏读时生效。
- 不能用 `ColumnPredicate` 替代 complex row-level filtering。
- 不生成 file-layer pruning hint 不代表不支持 row-level filter；复杂表达式仍通过
  predicate projection 读取并执行。

### 2.4 nested predicate 保守原则

过渡期 nested predicate 只处理 `STRUCT` / nested `STRUCT` 下的 primitive leaf。

任何不能解析为 struct field 链的表达式都不生成 file-layer pruning hint，包括动态 field
selector、复杂函数包装、非 AND 必要条件，以及经过 repeated path 的表达式。

无法证明安全时直接保留 row group/page，由 row-level filter 执行最终判断。

### 2.5 过渡期 nested predicate 范围

Doris 当前没有 DuckDB 风格的通用 `TableFilter` / `StructFilter` filter tree。
`ColumnPredicate` 仍是 primitive column predicate，不能自身表达 nested target。

因此过渡期 nested predicate 由 `TableColumnMapper` 识别 `struct_element(...)` 链，再生成
`FileColumnPredicateFilter`：

- 支持 `STRUCT` / nested `STRUCT`。
- 支持 resolved target 是 non-repeated primitive leaf。
- 支持 comparison 和 `IN_LIST`。
- 只从 AND 语义下提取。

这是一层 mapper-side extension，用于在 `ColumnPredicate` 重构前承接 struct primitive leaf 的
file-layer pruning。

## 3. 完整数据模型

### 3.1 schema node

Parquet 内部 schema tree 继续由 `ParquetColumnSchema` 表达，但最终必须能稳定描述每个 node：

- file-local node id：当前 parent 下的 local id。
- schema identity：field id 或 name，只用于 mapper 匹配。
- physical leaf id：primitive leaf column ordinal，complex node 为 `-1`。
- logical kind：primitive、struct、list、map。
- Dremel levels：max definition level、max repetition level、node nullable level、node repeated level。
- normalized child layout：标准化后的 struct fields、list element、map key/value。

legacy LIST/MAP encoding 应在 schema 解析阶段 normalization，reader tree 消费统一 layout。

### 3.2 struct predicate target

过渡期 nested pruning target 使用 DuckDB `StructFilter` 类似的 struct-only 形态：

```cpp
struct FileStructPredicateTarget {
    int32_t file_local_id = -1;
    std::string file_child_name;
    std::unique_ptr<FileStructPredicateTarget> child;
};

struct FileNestedPredicateTarget {
    LocalColumnId file_column_id;
    std::unique_ptr<FileStructPredicateTarget> struct_target;
};
```

其中：

- `file_column_id` 是 top-level file column id。
- `struct_target` 是 file-local struct field 链。
- `file_local_id` 是当前 struct 下的 file-local child id。
- `file_child_name` 只用于 debug 和可读性，不作为读取 id。
- `child == nullptr` 表示当前 field 是 primitive leaf target。

当前实现可以继续用 `file_child_id_path` 作为兼容存储，但它只表示 struct field 链。后续如果
重构 `ColumnPredicate` / nested filter target，应优先把它替换为上面的 struct-only target，
而不是在 `file_child_id_path` 上继续扩展额外语义。

### 3.3 read projection

`LocalColumnIndex` 表示 file-local read projection：

- root `index` 是 top-level `LocalColumnId`。
- nested `index` 是当前 parent 下的 file-local child id。
- `project_all_children=true` 表示读取整个 subtree。
- `project_all_children=false` 表示只读取列出的 child paths。

完整实现中，projection 需要区分三类来源：

- output projection：最终需要输出的 child。
- predicate projection：row-level filter 需要读取但不输出的 child。
- shape projection：为了构造 parent shape 必须额外读取的 hidden descendant。

三者可以合并到同一个 top-level file column 的 read projection，但只有 output projection 进入
`ResultColumnMapping`。

### 3.4 output mapping

`IndexMapping` / `ResultColumnMapping` 表示 table/global output shape：

- 只描述输出列，不包含 filter-only child。
- 支持 child rename/reorder。
- 支持 missing/default child。
- nested mapping 的 key 是 table child ordinal，value 是 file-local child mapping。

同一个 file-local child 可能同时服务 output 和 predicate；同一个 predicate-only child 不能改变
最终 `STRUCT/LIST/MAP` 输出 shape。

## 4. Shape / Value 契约

### 4.1 核心抽象

完整 reader 应引入统一的 nested shape 抽象：

```cpp
struct NestedShapeBatch {
    int64_t records_read;
    int64_t levels_written;
    std::vector<int16_t> def_levels;
    std::vector<int16_t> rep_levels;
    // Optional derived shape:
    // - parent null bitmap
    // - array/map offsets
    // - row/value membership
};

struct NestedValueBatch {
    NestedShapeBatch shape;
    MutableColumnPtr values;
    std::vector<int64_t> value_indices;
};
```

shape 表示 Dremel row/level 语义；value 表示 primitive leaf payload。

### 4.2 shape source

每个 complex node 必须能选择 shape source：

- primitive child 可以提供 leaf level stream。
- nested complex child 可以通过其 descendant leaf 提供 shape。
- nullable struct only complex children 必须能使用复杂 child 的 descendant leaf 构造 parent null。
- 如果投影中没有任何 physical descendant，但需要 parent shape，reader 必须追加 hidden shape
  projection。
- 如果 file schema 中不存在可用 physical descendant，则只能按 schema evolution 规则生成
  constant/default/null shape，不能伪造 file 中不存在的 null bitmap。

shape source 选择应由 reader tree 内部完成，调用方只表达需要读取的 subtree。

### 4.3 shape validation

同一 complex node 的多个 child shape 必须一致：

- 同一 struct 下的 sibling child 必须返回相同 record count。
- 对 non-repeated struct，sibling child 每个 row 的 parent null 必须一致。
- 对 repeated container，sibling child 必须能按 repeated level 对齐到同一个 parent row/entry。
- 不一致时返回 corruption，不做静默修复。

### 4.4 value materialization

value materialization 必须依附 shape：

- parent null 时 child 插入 default/null placeholder。
- optional child 缺值时插入 null。
- required child 缺值时返回 corruption。
- list/map offsets 由 shape 推导，不由 value count 反推。

这条规则避免 `STRUCT/LIST/MAP` reader 各自维护不兼容的状态机。

## 5. Reader Tree 契约

### 5.1 统一接口

所有 parquet column reader 最终应支持：

- `read(rows, column, rows_read)`：读取并 materialize。
- `skip(rows)`：按 table row 跳过。
- `select(sel, selected_rows, batch_rows, column)`：按 table row selection 读取。
- `read_shape(rows, shape)`：读取 shape，不 materialize payload。
- `read_value(rows, value_batch)`：读取 shape + value。

其中 `read_shape` / `read_value` 可以阶段性内部实现，不必立即暴露到 public API。

### 5.2 StructColumnReader

`STRUCT` reader 的最终职责：

- 从 shape source 构造 parent null bitmap。
- 按 projection 创建 selected children reader。
- output child 和 predicate-only child 可以同时读取，但只有 output child 写入 final struct column。
- 不要求存在 scalar child。
- nested struct 作为 list/map/struct child 时使用同一套 shape 对齐逻辑。

### 5.3 ListColumnReader

`LIST` reader 的最终职责：

- 支持标准三层 list，以及 normalization 后的 legacy list。
- element 可以是 primitive、struct、list、map。
- offsets 由 repeated level 推导。
- empty list、null list、list with null element 必须区分。
- element projection 递归传给 element reader。

### 5.4 MapColumnReader

`MAP` reader 的最终职责：

- 支持标准 `map -> repeated key_value -> key,value`。
- key 必须按 Parquet 语义为 required。
- value 可以是 primitive、struct、list、map。
- key/value 必须按 entry shape 对齐。
- map offsets 由 repeated key_value level 推导。
- value projection 递归传给 value reader。

## 6. Predicate 契约

### 6.1 row-level predicate

row-level predicate localization 只改写 top-level slot：

```text
struct_element(VSlotRef(table_s), 'id') > 5
```

改写为：

```text
struct_element(VSlotRef(file_s), 'id') > 5
```

不允许改写为：

```text
VSlotRef(file_s_id) > 5
```

### 6.2 predicate projection

任何 row-level predicate 需要访问的 nested child 都必须进入 read projection：

- output child 和 filter-only child 合并到同一个 top-level complex read projection。
- filter-only child 不进入 output mapping。
- row-level predicate 可以引用任意已支持读取的复杂类型表达式；是否能生成 file-layer
  pruning hint 是独立判断。
- 如果 nested path 能解析到 file-local child，读取精确 child。
- 如果 path 无法解析但 parent column 存在，必须保守读取足够 subtree 以保证 row-level Expr 可执行。
- 如果 parent column 缺失，则按 table reader constant/default/missing 规则处理。

### 6.3 pruning target extraction

file-layer pruning hint 只从安全表达式中提取：

- AND 语义下的必要条件。
- nested path op literal。
- literal op nested path。
- nested path IN literal list。
- 后续可扩展 deterministic cast/function domain translator。

不从以下表达式中提取：

- OR/NOT 中无法证明为必要条件的子表达式。
- dynamic field selector。
- non-deterministic function。
- 非 struct field 链表达式。

## 7. File-layer Pruning 契约

### 7.1 non-repeated primitive leaf

对 top-level primitive 或 struct/nested struct 下 non-repeated primitive leaf，允许：

- row group min/max pruning。
- dictionary pruning。
- bloom filter pruning。
- page index row range pruning。

前提：

- target 解析到 primitive leaf。
- `max_repetition_level == 0`。
- literal 可以安全转换到 file leaf type。
- predicate 类型被对应 pruning 方法支持。

### 7.2 null pruning

`IS NULL` / `IS NOT NULL` 需要区分：

- leaf value null。
- parent struct null。
- missing child default null。

non-repeated struct primitive leaf 可以先支持 parent/leaf null 的统计推导。

## 8. Schema Evolution 契约

### 8.1 recursive mapping

`TableColumnMapper` 必须递归匹配 complex children：

- BY_FIELD_ID：按 field id。
- BY_NAME：按 name。
- BY_INDEX：按 ordinal。

匹配结果生成：

- file-local read projection。
- output `IndexMapping`。
- filter localization entry。
- file-layer pruning target。

### 8.2 missing/default child

missing/default child 由 table reader finalize 处理：

- root missing column：生成 root default/null。
- struct missing child：按 parent row shape 生成 child default/null。
- list element missing field：按 element shape 生成 default/null field。
- map value missing field：按 map entry/value shape 生成 default/null field。

如果生成 nested default/null 需要 parent shape，而输出 projection 中没有 physical child，read
projection 必须追加 hidden shape source。

### 8.3 filter-only under schema evolution

filter-only child 必须参与 split-local read projection 重建：

- output mapping 不包含 filter-only child。
- filter localization 能访问 filter-only child。
- pruning target 使用 file child type，而不是 table child type。
- 多文件 schema 不一致时，每个 split 重新解析 filter path。

path 解析失败时保留 row group/page，并保证 row-level filter 仍按 missing/default 语义执行。

## 9. Lazy Materialization 契约

### 9.1 两阶段扫描

complex lazy materialization 的最终模式：

1. predicate phase：读取 predicate columns 和必要 shape，执行 row-level filter，生成 selected rows。
2. payload phase：对 non-predicate columns 执行 selected read。
3. finalize phase：移除 filter-only child，按 output mapping 构造 table/global block。

predicate phase 必须覆盖所有 row-level filter 需要的 complex projection。即使某个 predicate
不能生成 file-layer pruning hint，也必须读取足够的 predicate columns 和 shape，保证
`VExprContext` 可以在行级别正确求值。

### 9.2 shape reuse

同一个 top-level complex column 中 predicate child 和 output child 可能共享 parent shape。

最终实现应允许：

- predicate phase 读取 shape 后缓存 batch 内 shape。
- payload phase 复用 shape 或用 selected rows 重新读取等价 shape。
- 不因 filter-only child 改变 output child 顺序或 output struct field count。

阶段性实现可以继续使用 `skip()+read()` 实现 `select()`，但完整方案应收敛到 decoder-level direct
select。

### 9.3 selected rows 对齐

selected read 必须按 table row 选择，而不是按 leaf value 选择：

- struct child 按 parent row selection 对齐。
- list/map 按 selected parent rows 保留完整 container shape。
- repeated leaf 的 selected value 范围由 selected parent rows 推导。
- page skip、row skip、reader skip 的 accounting 必须一致。

## 10. 测试契约

完整复杂类型能力必须用以下矩阵验收：

- complex read：
  - nullable struct only complex children。
  - nested struct only complex children。
  - array of primitive/struct/list/map。
  - map value as primitive/struct/list/map。
  - empty/null list、null element、empty/null map、null map value。
- nested projection：
  - output child only。
  - filter-only child only。
  - output child + filter child sibling。
  - repeated parent 下的 nested projection。
- schema evolution：
  - rename。
  - reorder。
  - missing/default child。
  - filter-only child under renamed/reordered file schema。
  - multi split schema mismatch。
- pruning：
  - non-repeated struct primitive stats/dictionary/bloom/page index positive and negative case。
  - 非 struct field 链表达式不应触发 nested pruning。
  - literal conversion failure 保留 row group/page。
  - missing stats/dictionary/bloom metadata 保留 row group/page。
- lazy materialization：
  - predicate child 和 output child 属于同一 top-level struct。
  - predicate phase 读取不产生 pruning hint 的 complex filter columns。
  - selected ranges + page skip + nested projection。
  - filter-only child 不出现在 final output。

## 11. 阶段落地顺序

### Phase 0: 契约和安全测试

- 固化本文契约。
- 补非 struct field 链表达式不产生 pruning hint 的 negative tests。
- 补 filter-only projection 和 schema evolution regression tests。

### Phase 1: Nested Shape Engine

- 引入统一 shape/value batch。
- `STRUCT` 不再依赖 scalar child。
- 支持 nullable struct only complex children。
- 收敛 sibling shape validation。

### Phase 2: 完整 reader 组合

- 支持 `array<map<...>>`。
- 支持 `map<K,array<struct<...>>>`。
- 支持 `map<K,map<...>>`。
- 标准化 legacy LIST/MAP schema。

### Phase 3: 完整 nested schema evolution

- 递归 child rename/reorder。
- missing/default child materialization。
- filter-only child split-local path 重建。
- 多文件 schema mismatch 覆盖。

### Phase 4: ColumnPredicate / nested filter target 重构

- 明确是否引入 DuckDB 风格的 `StructFilter` / nested filter tree。
- 或将 `FileColumnPredicateFilter` 扩展为 struct-only nested target。
- 统一 nested target、literal cast、schema mapping、file-layer pruning 的职责边界。

### Phase 5: 扩展 predicate 和 pruning

- nested `IS NULL` / `IS NOT NULL`。
- deterministic cast/function pruning。
- runtime filter on nested primitive leaf。

### Phase 6: decoder-level lazy materialization

- shape-only scan。
- value-late materialization。
- decoder-level direct select。

## 12. 禁止实现

- 不把 nested child 注册成独立 file block slot。
- 不把 `struct_element(...)` 改写成 child `VSlotRef`。
- 不让 `ColumnPredicate` 参与 row-level filtering。
- 不在非 struct field 链表达式上生成 nested pruning hint。
- 不让 filter-only child 改变 final output mapping。
- 不在 file reader 中重新解释 table/global schema。
