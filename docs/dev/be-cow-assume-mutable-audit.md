# BE COW assume_mutable audit

Snapshot: 2026-05-09, branch `cow`.

`assume_mutable()` is now an ownership assertion. A valid call must have a
nearby proof that the referenced `ColumnPtr` is exclusive. If exclusivity is not
local and obvious, mutate the owning handle and write it back:

```cpp
auto column = IColumn::mutate(std::move(block.get_by_position(i).column));
// mutate column
block.replace_by_position(i, std::move(column));
```

For hot paths that append on every row, do not call `mutate()` per row. Keep a
real mutable owner such as `MutableBlock`/`MutableColumnPtr`, then materialize a
`Block` view only at the boundary.

## Scan Commands

Active call-site inventory:

```bash
rg -n '\bassume_mutable(_ref)?\s*\(' be/src -S
```

Current result: 127 raw matches. These include API definitions, comments, and
the active call sites listed below. The current unique path list from the command
is fully covered by the table in this document.

High-risk alias scan:

```bash
rg -n 'get_columns\(\).*assume_mutable|assume_mutable\(\).*get_columns|const_cast<.*IColumn.*>\([^\n]*get_columns|get_columns\(\)\[[^\n]*\]\.get\(\)' be/src be/test -S
```

Current result: no remaining direct hit. This pattern matters because
`Block::get_columns()` copies `ColumnPtr`s and can introduce a temporary alias in
the same function before the mutable assertion.

The direct owner-slot pattern was also scanned:

```bash
rg -n '\.get_by_position\(.*\)\.column->assume_mutable' be/src -S
```

Those calls are not automatically safe; each one is classified below by its
real ownership evidence.

## Lessons From The 2026-05-09 Recheck

- The earlier file-level `OWNED_BLOCK` classification was too coarse. A current
  scanner output block is not itself a proof if the same function first copies
  its `ColumnPtr`s.
- `be/src/format/json/new_json_reader.cpp` had exactly that bug, confirmed by
  the `test_hive_openx_json` external regression on the OpenX
  `ignore.malformed.json=true` table:
  the malformed-json helper iterated `block.get_columns()`, creating aliases,
  then called `assume_mutable()`. It is now `append_null_for_malformed_json()`,
  which mutates each owner slot and writes it back. Rollback paths now use
  `truncate_block_to_rows()` with the same owner-writeback pattern.
- `be/src/exec/rowid_fetcher.cpp` had two unsafe patterns: appending into an
  externally supplied output block with `assume_mutable()`, and writing into
  `result_block.get_columns()` through `const_cast`. Both were changed to mutate
  the owning slot and write back. Source scan-block columns are copied only into
  stable read-only vectors.
- `be/src/core/block/block.cpp` should not be described as `OWNED_BLOCK`.
  These helpers are safe because they branch on `is_exclusive()` or explicitly
  clone/mutate before writeback.

## Classification Legend

- `LOCAL_RESULT`: the column is created locally or freshly cloned before the
  call and is not published before mutation.
- `MUTATED_FIRST`: the owning `ColumnPtr` was moved through `IColumn::mutate()`
  or another COW-safe producer before typed mutable access.
- `OWNED_OUTPUT`: the call writes into a scanner/operator/internal output block
  whose caller contract is exclusive ownership, and this function does not
  introduce another local alias before the call.
- `SUBCOLUMN_EXCLUSIVE`: the parent complex column is already exclusive, so its
  nested columns are exclusive as part of the parent mutation path.
- `EXCLUSIVE_BRANCH`: the call is guarded by `is_exclusive()` or `use_count()==1`
  and has a clone/mutate fallback for the shared case.
- `HELPER_CONTRACT`: API/helper accessors. Callers must prove ownership or use a
  COW-safe mutate/writeback path.
- `COMMENT_ONLY`: not an active call site.
- `CHECKLIST_ONLY`: local checklist text, not code.

## Active Call-Site Audit

| File | Lines | Classification | Evidence / action |
| --- | --- | --- | --- |
| `be/src/core/AGENTS.md` | 18 | CHECKLIST_ONLY | Local review checklist. |
| `be/src/core/cow.h` | 312, 319, 326, 349, 355 | HELPER_CONTRACT | COW primitive API definitions and proxy operators. |
| `be/src/core/column/column_nullable.h` | 283, 391 | HELPER_CONTRACT | Mutable nested/null-map accessors assert subcolumn exclusivity. Callers that only own the parent through a shared `ColumnPtr` must mutate/write back first. |
| `be/src/core/column/column_array.cpp` | 66 | COMMENT_ONLY | Documents const access to avoid mutable assertion during construction. |
| `be/src/core/column/column_map.cpp` | 553 | COMMENT_ONLY | Documents const access after offsets writeback. |
| `be/src/core/column/column_nullable.cpp` | 380 | COMMENT_ONLY | Documents const nested access to avoid mutable assertion. |
| `be/src/core/block/block.cpp` | 659, 674, 737, 745, 775, 804, 823, 1106 | EXCLUSIVE_BRANCH | In-place block helpers clear/filter/shrink in place only when the column is exclusive; otherwise they clone, filter-return, or mutate/write back. This is the correct pattern for shared blocks. |

| File | Lines | Classification | Evidence / action |
| --- | --- | --- | --- |
| `be/src/exprs/vruntimefilter_wrapper.cpp` | 126 | LOCAL_RESULT | `filter_column` is the runtime-filter result column passed to `change_null_to_true`. |
| `be/src/exprs/vtopn_pred.h` | 121 | LOCAL_RESULT | `result_column` is a freshly produced predicate column. |
| `be/src/exprs/vexpr_context.cpp` | 332, 375 | LOCAL_RESULT | Temporary expression result columns are cleared before reuse in the expression context. |
| `be/src/exprs/lambda_function/varray_sort_function.cpp` | 145 | OWNED_OUTPUT | The lambda block is built for the current lambda evaluation and no local `ColumnPtr` alias is introduced before mutation. |
| `be/src/exprs/lambda_function/varray_map_function.cpp` | 233, 242 | OWNED_OUTPUT | Lambda evaluation columns are local to the lambda block for this call. |
| `be/src/exprs/function/function_other_types_to_date.cpp` | 150, 154, 164, 168, 301, 305, 545, 615, 620, 1058, 1133 | LOCAL_RESULT | Function result columns are created by the execute path and filled before being returned. |
| `be/src/exprs/function/cast/cast_to_variant.h` | 97, 109, 121, 124 | LOCAL_RESULT | `col_to` is created by this cast path before defaults/null wrapping are inserted. |
| `be/src/exprs/function/function_variant_element.cpp` | 266, 268, 293 | LOCAL_RESULT | `result` is the newly created variant extraction output. |
| `be/src/exprs/function/function_variadic_arguments.h` | 64, 68, 74 | LOCAL_RESULT | `column` is created locally and assigned to the result only after writes complete. |
| `be/src/exprs/function/dictionary_util.h` | 63 | EXCLUSIVE_BRANCH | In-place filter is used only after `column->is_exclusive()`; otherwise the function replaces `column` with the clone-returning `filter()` result. |
| `be/src/exprs/function/array/function_array_with_constant.cpp` | 102 | LOCAL_RESULT | `clone` comes from `value->clone_empty()` and is filled before publication. |
| `be/src/exprs/function/array/function_array_aggregation.cpp` | 219, 231, 237, 443, 456, 462 | LOCAL_RESULT | Aggregate result column is the local destination passed by the array aggregation function. |
| `be/src/exprs/aggregate/aggregate_function_null_v2.h` | 217, 276 | LOCAL_RESULT | Destination nullable columns are newly created serialize/aggregate output. A former read-only source nested-column assertion was removed in this branch. |

| File | Lines | Classification | Evidence / action |
| --- | --- | --- | --- |
| `be/src/exec/operator/operator.cpp` | 351, 358 | EXCLUSIVE_BRANCH | Projection helper steals/mutates input columns only after checking the source is exclusive; otherwise it materializes a replacement. |
| `be/src/exec/operator/aggregation_source_operator.cpp` | 549 | LOCAL_RESULT | `ptr = make_nullable(ptr, ...)` creates the nullable result before moving it into the output column list. |
| `be/src/exec/operator/distinct_streaming_aggregation_operator.cpp` | 210, 243 | LOCAL_RESULT | Key columns are locally materialized aggregate output columns. |
| `be/src/exec/operator/hashjoin_build_sink.cpp` | 189, 591 | LOCAL_RESULT | Join build helper constructs nullable columns locally before null-map mutation. |
| `be/src/exec/operator/join/process_hash_table_probe_impl.h` | 883 | OWNED_OUTPUT | Hash-join probe writes into its current output block. No `get_columns()` alias is introduced in this mutation path. |
| `be/src/exec/operator/nested_loop_join_probe_operator.h` | 52 | OWNED_OUTPUT | Macro clears local nested-loop join probe output columns. |
| `be/src/exec/operator/assert_num_rows_operator.cpp` | 94 | LOCAL_RESULT | Assertion operator creates and fills its output column locally. |
| `be/src/exec/sort/sorter.cpp` | 235, 240 | OWNED_OUTPUT | Sorter appends into its internal unsorted block. This is a long-lived sorter-owned block, not a per-row mutate path. |
| `be/src/exec/common/util.hpp` | 248 | SUBCOLUMN_EXCLUSIVE | Recursive helper receives a `MutableColumnPtr`; the `ColumnConst` wrapper is already mutable, so its data column is part of the same exclusive path. |
| `be/src/exec/rowid_fetcher.cpp` | 464 | LOCAL_RESULT | `result_block` is built locally from `Block(slots, request.row_locs().size())` before rowid reads. Former shared-output merge and `get_columns()`/`const_cast` paths were fixed to mutate/write back. |

| File | Lines | Classification | Evidence / action |
| --- | --- | --- | --- |
| `be/src/format/json/new_json_reader.cpp` | 1016, 1022, 1062, 1500, 1610, 1625 | OWNED_OUTPUT | Normal JSON write/skip-bitmap paths append to the current scanner output block. The function does not copy the owner column before these calls. The previous malformed/rollback paths were changed to owner mutate/writeback helpers. |
| `be/src/format/json/new_json_reader.cpp` | 1270, 1278 | SUBCOLUMN_EXCLUSIVE | Map keys/values are subcolumns of the current exclusive map column passed into `_simdjson_write_data_to_column()`. |
| `be/src/format/column_type_convert.cpp` | 117 | LOCAL_RESULT | `_cached_src_column` is converter-owned cache state and is cleared before reuse. |
| `be/src/format/parquet/vparquet_column_reader.cpp` | 334, 377, 416, 424, 665, 673, 721, 729, 798, 806, 995 | MUTATED_FIRST | Parquet column readers mutate the destination handle or operate on reader-owned destination columns before typed mutable access. |
| `be/src/format/parquet/vparquet_column_reader.h` | 486 | OWNED_OUTPUT | Nested map reader destination column is owned by the active parquet read path. |
| `be/src/format/parquet/vparquet_group_reader.cpp` | 1061, 1071 | LOCAL_RESULT | Dictionary temporary block is local to the group reader. |
| `be/src/format/parquet/vparquet_reader.h` | 190 | OWNED_OUTPUT | TopN rowid synthesized column writes into the current parquet reader output block. |
| `be/src/format/parquet/parquet_column_convert.h` | 199, 239, 350 | MUTATED_FIRST | Conversion helpers mutate owning destination handles or converter-owned columns before typed access. The nullable null-map slice bug was fixed separately by copying from the appended source slice. |
| `be/src/format/parquet/parquet_column_convert.cpp` | 121 | LOCAL_RESULT | `_cached_src_physical_column` is converter-owned cache state. |
| `be/src/format/orc/vorc_reader.h` | 229 | OWNED_OUTPUT | TopN rowid synthesized column writes into the current ORC reader output block. |
| `be/src/format/orc/vorc_reader.cpp` | 2254 | MUTATED_FIRST | Schema-change conversion operates on the destination column for the current conversion path. |
| `be/src/format/orc/vorc_reader.cpp` | 3092, 3100 | LOCAL_RESULT | Dictionary temporary block is local to the ORC reader. |
| `be/src/format/table/table_format_reader.h` | 71, 106 | OWNED_OUTPUT | Partition/missing columns are filled into the current scanner output block. No local `get_columns()` alias is introduced. |
| `be/src/format/table/table_format_reader.h` | 113 | EXCLUSIVE_BRANCH | Default expression result is mutated only when `use_count()==1`; the shared case is not passed to `assume_mutable()`. |
| `be/src/format/table/es/es_http_reader.cpp` | 153 | OWNED_OUTPUT | ES reader materializes directly into the current output block column slots. |
| `be/src/format/table/iceberg_reader_mixin.h` | 162, 183 | OWNED_OUTPUT | Position delete helper writes synthesized columns into the current delete/output block. The old equality-delete `MutableBlock(&block)` missing-writeback bug has no remaining `assume_mutable()` call and is handled by `to_block()` writeback. |

| File | Lines | Classification | Evidence / action |
| --- | --- | --- | --- |
| `be/src/storage/schema_change/schema_change.cpp` | 176, 183, 233, 383, 408 | LOCAL_RESULT | New schema-change columns/blocks are created locally before insertion or conversion. |
| `be/src/storage/partial_update_info.cpp` | 1010 | LOCAL_RESULT | `tmp_block` is freshly created for the sequence column before moving it into the target block. |
| `be/src/storage/segment/vertical_segment_writer.cpp` | 887, 890 | LOCAL_RESULT | Encoded default sequence value block is created locally and filled immediately. |
| `be/src/storage/segment/segment_iterator.h` | 269 | OWNED_OUTPUT | Segment iterator writes selected rows into the caller-provided read output block. This path does not create a local `ColumnPtr` alias before mutation; type-cast branch writes through `cast_column()` replacement instead. |
| `be/src/storage/segment/segment_iterator.cpp` | 2647, 2650 | LOCAL_RESULT | `_current_return_columns` is iterator-owned current batch state; converted columns replace that state after `cast_column()`. |
| `be/src/storage/segment/virtual_column_iterator.cpp` | 157 | LOCAL_RESULT | `res_col` is produced by `filter()` and immediately becomes the destination column. |
| `be/src/storage/iterator/vcollect_iterator.cpp` | 422 | LOCAL_RESULT | A clone-empty column is created from the source block before being pushed into the local mutable block. |
| `be/src/storage/iterator/vgeneric_iterators.cpp` | 174 | OWNED_OUTPUT | Merge iterator appends into its destination block for the current read. No local alias is introduced. |
| `be/src/storage/iterator/vertical_merge_iterator.cpp` | 330, 353 | OWNED_OUTPUT | Vertical merge appends into its destination block for the current read. No local alias is introduced. |
| `be/src/storage/iterator/olap_data_convertor.h` | 184 | LOCAL_RESULT | Padding column is the local conversion destination. |
| `be/src/storage/segment/variant/variant_column_writer_impl.cpp` | 1224 | COMMENT_ONLY | Documents avoiding a repeated mutable assertion. |
| `be/src/storage/segment/variant/variant_column_reader.cpp` | 1535 | COMMENT_ONLY | Commented-out code only. |

## Mutate / MutableBlock Recheck

Baseline command:

```bash
rg -n -C 3 'MutableBlock::build_mutable_block\(|MutableBlock\s+\w+\s*\(&' be/src -S
```

Current conclusions:

- `be/src/exprs/aggregate/aggregate_function_sort.h`: aggregate sort state owns
  a long-lived `MutableBlock`. `add()` and `merge()` append directly to mutable
  columns; `Block` is materialized only for `serialize()` and `sort_block()`.
  This is the correct hot-path pattern.
- `be/src/format/table/iceberg_reader_mixin.h`: equality-delete cache merge now
  writes back with `eq_file_block = mutable_block.to_block()`.
- `be/src/information_schema/*_scanner.cpp`, scanner helpers, group commit,
  partial update, and tablet helper paths write back with `set_columns()`,
  `swap(...to_block())`, or equivalent owner replacement.
- `be/src/load/memtable/memtable.cpp`, iterator setup, and hash/set build paths
  use local clones or long-lived mutable owners; moved-from local blocks are not
  read afterward.

No second definite `MutableBlock(&block)` missing-writeback bug was found in the
current scan.

Owner-slot mutate command:

```bash
rg -n -C 2 'IColumn::mutate\(std::move\([^\n]*(block|Block|_result_block|result_block|out_block|output_block|in_block|tmp_block).*get_by_position' be/src -S
```

Current conclusions:

- Direct helper cases such as `schema_scanner_helper.cpp`,
  `schema_scanner.cpp`, skip-bitmap helpers, rowid fetcher, point query, and
  file-reader resize paths mutate the owner slot and then write it back with
  `replace_by_position()` or direct owner assignment.
- Mem-reuse operator paths that move several columns out at once collect them
  into `MutableColumns` and then restore the block with `set_columns()` or
  `swap(Block(...))`.
- Long-lived mutable-owner paths such as set-source local state deliberately
  move the block columns into state-owned mutable columns; the moved-from block
  is not read as the owner afterward in that function.
- No definite `IColumn::mutate(std::move(block->get_by_position(...).column))`
  site was found where the mutated owner is later forgotten.

## Hot-Path Mutate Audit

Baseline commands:

```bash
rg -n '\bIColumn::mutate\s*\(|\.mutate\s*\(\)' be/src -S
rg -n '(->|\.)mutate_columns\s*\(' be/src -S
rg -n '\bIColumn::mutate\s*\(|\.mutate\s*\(\)|(->|\.)mutate_columns\s*\(' be/src -S
```

Current result: 171 `IColumn::mutate()` / `std::move(*column).mutate()`
matches, plus 47 `Block::mutate_columns()` matches. `be/src/exprs/aggregate`
has no remaining COW `mutate()` matches; aggregate `add()` hot paths therefore
do not mutate per row. `AggregateFunctionSortData` remains the intended model:
keep a long-lived `MutableBlock`, append directly in `add()`, and materialize a
`Block` view only when sorting or serializing.

The only definite hot-path issue found in this pass was JSONB row-store
deserialization:

- `JsonbSerializeUtil::jsonb_to_block(char*)` was a single-row helper that
  moved/mutated destination block columns per JSONB field.
- It is called from point query and rowid fetcher row loops.
- It now exposes `jsonb_to_columns(...)`, so hot callers mutate the destination
  block once into `MutableColumns` outside the row loop and append rows through
  those mutable owners.
- The old single-row `jsonb_to_block(char*)` wrapper remains for non-hot callers,
  but current hot call sites use `jsonb_to_columns(...)`.

`be/src/format/json/new_json_reader.cpp` also keeps Hive duplicate-key rollback
on an owner-slot mutate/writeback helper. This branch only executes after the
same key has already been written once in the current JSON object, so it is not
part of the normal per-column write path, and it avoids relying on a scanner
output-block ownership assumption across a rollback/rewrite operation.

Detailed grouped audit:

| File | Lines | Hot-path conclusion |
| --- | --- | --- |
| `be/src/core/block/block.cpp` | 584, 1108 | Block-level helper. `mutate_columns()` is the API boundary; shrink path mutates only after exclusivity branch. Not row-by-row. |
| `be/src/core/block/block.h` | 392, 396, 529 | `MutableBlock` constructors and const-column recursion helper. Block-level ownership transfer, not row-by-row. |
| `be/src/core/block/column_with_type_and_name.cpp` | 131 | Column wrapper conversion helper. Not row-by-row. |
| `be/src/core/column/column.h` | 191, 586, 596 | Column COW helper implementations. API boundary. |
| `be/src/core/column/column_const.cpp` | 113 | Const-column internal mutation of owned data. Not row-by-row. |
| `be/src/core/column/column_const.h` | 298, 326 | Const-column internal materialization helpers. Not row-by-row. |
| `be/src/core/column/column_map.cpp` | 522, 523, 526, 529, 539, 540, 541, 560, 563, 566, 662, 665 | Map internal filter helpers mutate subcolumns once per column operation. Recursive value-map dedup detaches the value owner before mutation. Not row-by-row. |
| `be/src/core/column/column_map.h` | 65, 66 | Map shared-column factory preserves immutable subcolumns and validates through const access. Not row-by-row. |
| `be/src/core/column/column_nullable.cpp` | 118 | Nullable constructor internal subcolumn ownership. Not row-by-row. |
| `be/src/core/column/column_nullable.h` | 68, 70 | Nullable shared-column factory preserves immutable subcolumns and validates through const access. Not row-by-row. |
| `be/src/core/column/column_variant.cpp` | 319, 487, 495, 502, 504, 2071, 2129, 2348, 2356, 2816, 2836, 2837 | Variant internal finalize/filter/serialization helpers mutate subcolumns during a column operation. Not row-by-row COW. |
| `be/src/core/column/column_variant.h` | 328, 444 | Variant helper/finalize path. Not row-by-row. |
| `be/src/core/cow.h` | 71 | COW primitive example/comment path. API boundary. |
| `be/src/core/data_type/data_type_array.cpp` | 123 | Array type helper mutates nested column once. Not row-by-row. |
| `be/src/core/data_type/data_type_map.cpp` | 138, 139 | Map type helper mutates key/value nested columns once. Not row-by-row. |
| `be/src/core/data_type/data_type_struct.cpp` | 217 | Struct type helper mutates children once per column operation. Not row-by-row. |
| `be/src/exprs/function/array/function_array_utils.cpp` | 64 | Function execution block-level variant/nullable conversion. Not row-by-row. |
| `be/src/exprs/function/cast/cast_to_variant.h` | 41, 44, 170 | Cast execution mutates result/source once per vectorized block. Not aggregate-row hot. |
| `be/src/exprs/function/comparison_equal_for_null.cpp` | 194, 233 | Temporary block result extraction. Not row-by-row. |
| `be/src/exprs/function/function.cpp` | 70 | Function null-map merge mutates result null-map once per execute. Not row-by-row. |
| `be/src/exprs/function/function_bitmap.cpp` | 684 | Bitmap function mutates a local/source column once per vectorized execute. Not row-by-row. |
| `be/src/exprs/function/function_variant_element.cpp` | 325 | Variant element execution creates mutable root once. Not row-by-row. |
| `be/src/exprs/function/if.cpp` | 252, 283 | IF function reuses one selected result column per vectorized execute. Not row-by-row. |
| `be/src/exprs/table_function/udf_table_function.cpp` | 127 | UDF table-function result column ownership transfer once per produced block. Not row-by-row. |
| `be/src/exprs/table_function/vexplode.cpp` | 48 | Table-function variant column conversion once per input column. Not row-by-row. |
| `be/src/exprs/table_function/vexplode_v2.cpp` | 54 | Same as `vexplode.cpp`. |
| `be/src/exprs/vcompound_pred.h` | 212, 233, 237 | Compound predicate reuses one input/result column per vectorized execute. Not row-by-row. |
| `be/src/exprs/vcondition_expr.cpp` | 206, 235 | CASE/condition expression reuses selected result column per vectorized execute. Not row-by-row. |
| `be/src/exec/common/arrow_column_to_doris_column.cpp` | 103 | Arrow conversion mutates the destination column once per converted column. Not row-by-row. |
| `be/src/exec/common/data_gen_functions/vnumbers_tvf.cpp` | 52 | Data-gen source mutates output columns once per output block when memory is reused. Not row-by-row. |
| `be/src/exec/common/partition_sort_utils.cpp` | 32 | Partition-sort utility converts one stored block to mutable columns before appending. Not row-by-row. |
| `be/src/exec/common/variant_util.cpp` | 438, 2157, 2219, 2222 | Variant utility column conversion/finalization paths. Not row-by-row COW. |
| `be/src/exec/exchange/vdata_stream_sender.cpp` | 332 | Exchange sender keeps a mutable block owner while serializing/sending one block. Not row-by-row. |
| `be/src/exec/operator/aggregation_sink_operator.cpp` | 311, 500 | Aggregation sink normalizes key float values once per input block/key column. Not aggregate `add()`. |
| `be/src/exec/operator/aggregation_source_operator.cpp` | 116, 141, 246, 304, 313 | Aggregation source output path mutates reused output columns once per output batch. Not row-by-row. |
| `be/src/exec/operator/bucketed_aggregation_sink_operator.cpp` | 179 | Bucketed aggregation sink normalizes key float values once per input block/key column. Not aggregate `add()`. |
| `be/src/exec/operator/bucketed_aggregation_source_operator.cpp` | 332, 387, 475, 558 | Bucketed aggregation source mutates reused output columns once per output batch. Not row-by-row. |
| `be/src/exec/operator/dict_sink_operator.cpp` | 47 | Sink block column overflow conversion once per column. Not row-by-row. |
| `be/src/exec/operator/distinct_streaming_aggregation_operator.cpp` | 166, 220, 225, 232 | Distinct streaming aggregation mutates expression/output/cache columns once per processed block/split. Not row-by-row. |
| `be/src/exec/operator/exchange_sink_operator.cpp` | 513 | Exchange sink transfers current block into mutable block for serialization. Not row-by-row. |
| `be/src/exec/operator/hashjoin_build_sink.cpp` | 575, 577 | Hash-join build converts/finalizes one block column before build. Not row-by-row. |
| `be/src/exec/operator/join/process_hash_table_probe_impl.h` | 168, 657, 726 | Hash-join probe mutates output/lazy materialized columns once per output block. Not row-by-row. |
| `be/src/exec/operator/nested_loop_join_probe_operator.cpp` | 82, 104, 145, 401, 515 | Nested-loop join probe creates mutable output columns once per output block/batch section. Not row-by-row. |
| `be/src/exec/operator/partitioned_aggregation_sink_operator.cpp` | 515, 516 | Partitioned aggregation state owns key/value mutable blocks for later appends. Correct long-lived owner pattern. |
| `be/src/exec/operator/schema_scan_operator.cpp` | 261 | Schema scan copies source columns into output columns once per block. Low-volume metadata path. |
| `be/src/exec/operator/set_sink_operator.cpp` | 134 | Set sink overflow conversion once per block column. Not row-by-row. |
| `be/src/exec/operator/set_source_operator.cpp` | 117 | Set source transfers output block columns into local mutable columns once per block. Not row-by-row. |
| `be/src/exec/operator/streaming_aggregation_operator.cpp` | 334, 376, 405, 472, 496, 599 | Streaming aggregation source/sink output paths mutate reused block columns once per block/output batch. Not aggregate `add()`. |
| `be/src/exec/rowid_fetcher.cpp` | 167, 1082 | Fixed in this pass: row-store JSONB loops now mutate result columns once outside the row loop and append through `jsonb_to_columns(...)`. |
| `be/src/exec/rowid_fetcher.cpp` | 196, 943, 1103 | Non-row-store merge/read paths mutate once per destination column, then append many rows. Not row-by-row. |
| `be/src/exec/scan/file_scanner.cpp` | 441, 785 | File scanner mutates partition-prune/skip-bitmap helper columns once per block. Not row-by-row. |
| `be/src/exec/scan/meta_scanner.cpp` | 115 | Meta scanner output columns once per block. Low-volume metadata path. |
| `be/src/exec/scan/scanner.cpp` | 219 | Scanner materialization mutates a prepared column pointer once per projection column. Not row-by-row. |
| `be/src/exec/sink/vtablet_block_convertor.cpp` | 285, 289 | Tablet sink conversion mutates temporary/result columns once per block. Not row-by-row. |
| `be/src/exec/sink/writer/vtablet_writer.cpp` | 1765 | Restores a temporary block after merge. Not row-by-row. |
| `be/src/exec/sink/writer/vtablet_writer_v2.cpp` | 625 | Same as v1 writer. |
| `be/src/format/arrow/arrow_stream_reader.cpp` | 97 | Arrow stream reader mutates output block once per batch. Not row-by-row. |
| `be/src/format/count_reader.h` | 61 | Count reader creates/mutates output columns once per batch. Not row-by-row. |
| `be/src/format/csv/csv_reader.cpp` | 446, 452 | CSV reader mutates output columns once per batch when filling rows. Not row-by-row. |
| `be/src/format/jni/jni_data_bridge.cpp` | 108 | JNI bridge mutates one destination column before writing a batch. Not row-by-row. |
| `be/src/format/json/new_json_reader.cpp` | 462, 472, 482 | Malformed-row append, row rollback, and Hive duplicate-key rollback use owner-slot mutate/writeback helpers. The duplicate-key helper is a rare rollback path, not the normal per-field write path. |
| `be/src/format/lance/lance_rust_reader.cpp` | 233 | Lance reader mutates output block once per batch. Not row-by-row. |
| `be/src/format/orc/vorc_reader.cpp` | 2055, 2073, 2145, 2216, 2218, 2253, 2263, 2860 | ORC complex/schema conversion and block resize paths mutate once per column/batch. Not row-by-row. |
| `be/src/format/parquet/parquet_column_convert.h` | 198, 238, 346 | Parquet conversion helpers mutate destination handles once per converted column. Not row-by-row. |
| `be/src/format/parquet/vparquet_column_reader.cpp` | 331, 413, 663, 719, 796, 994 | Parquet column reader mutates destination handles once per column chunk/batch. Not row-by-row. |
| `be/src/format/parquet/vparquet_column_reader.h` | 485 | Nested parquet column reader mutates destination once per read call. Not row-by-row. |
| `be/src/format/parquet/vparquet_group_reader.cpp` | 668 | Parquet group reader temporary resize once per block. Not row-by-row. |
| `be/src/format/table/paimon_cpp_reader.cpp` | 77, 120 | Paimon reader mutates output columns once per batch. Not row-by-row. |
| `be/src/format/table/paimon_jni_reader.cpp` | 108 | Paimon JNI reader mutates output columns once per batch. Not row-by-row. |
| `be/src/format/table/parquet_metadata_reader.cpp` | 815 | Metadata reader output reuse once per batch. Low-volume metadata path. |
| `be/src/format/table/remote_doris_reader.cpp` | 75 | Remote Doris reader mutates output columns once per batch. Not row-by-row. |
| `be/src/format/transformer/merge_partitioner.cpp` | 213 | Merge partitioner mutates a block once before partitioning. Not row-by-row. |
| `be/src/information_schema/schema_scanner.cpp` | 104, 314, 473 | Information-schema insertion helpers can run per cell, but this is a metadata path, not BE data hot path. They write back correctly. |
| `be/src/information_schema/schema_scanner_helper.cpp` | 36, 47, 59, 75, 85, 95, 105 | Same metadata-path per-cell helper pattern as `schema_scanner.cpp`; writeback is present. |
| `be/src/runtime/result_block_buffer.cpp` | 217 | Result buffer merges/mutates one block when appending query results. Not row-by-row. |
| `be/src/service/point_query_executor.cpp` | 503 | Fixed in this pass: point query now mutates result columns once outside the row loop and appends row-store/missing-column values through those mutable owners. |
| `be/src/storage/iterator/block_reader.cpp` | 171, 347, 480, 537, 587 | Storage reader prepares target/delete-filter columns per batch. Not row-by-row. |
| `be/src/storage/iterator/olap_data_convertor.h` | 310, 314 | OLAP convertor captures column data once per conversion batch. Not row-by-row. |
| `be/src/storage/iterator/vcollect_iterator.cpp` | 881 | Collect iterator mutates target columns once per batch. Not row-by-row. |
| `be/src/storage/iterator/vertical_block_reader.cpp` | 190, 401, 487, 488, 555 | Vertical reader prepares target/delete-filter columns per batch. Not row-by-row. |
| `be/src/storage/iterator/vgeneric_iterators.cpp` | 67 | Generic iterator mutates output columns once per batch. Not row-by-row. |
| `be/src/storage/partial_update_info.cpp` | 45, 342, 389, 418, 497, 565 | Partial-update block construction/merge mutates columns once per block. Not row-by-row. |
| `be/src/storage/segment/column_reader.cpp` | 997, 1012, 1013, 1084, 1169, 1170, 1417, 1782, 1794 | Segment complex-column readers mutate offsets/items/subcolumns once per read batch. Not row-by-row. |
| `be/src/storage/segment/segment_iterator.cpp` | 2185, 2907 | Segment iterator mutates current return columns / temporary mock column once per batch. Not row-by-row. |
| `be/src/storage/segment/variant/hierarchical_data_iterator.cpp` | 206, 228, 249, 290, 546 | Variant hierarchical reader mutates subcolumns once per read/finalize batch. Not row-by-row. |
| `be/src/storage/segment/variant/hierarchical_data_iterator.h` | 141 | Variant iterator helper mutates destination once per helper call. Not row-by-row. |
| `be/src/storage/segment/variant/variant_column_writer_impl.cpp` | 1229 | Variant writer finalization helper. Not row-by-row. |
| `be/src/storage/segment/variant/variant_streaming_compaction_writer.cpp` | 146 | Variant streaming compaction finalization helper. Not row-by-row. |
| `be/src/storage/segment/vertical_segment_writer.cpp` | 96 | Skip-bitmap helper mutates one block column then writes back. Not row-by-row. |
| `be/src/storage/tablet/base_tablet.cpp` | 962, 986, 1194 | Tablet row reconstruction/partial update mutates full output columns once per block. Not row-by-row. |
| `be/src/storage/tablet_info.cpp` | 563 | Partition-key helper mutates a temporary column once. Not row-by-row. |
| `be/src/util/jsonb/serialize.cpp` | 83, 159 | Wrapper paths now mutate destination columns once, call `jsonb_to_columns(...)`, and restore with `set_columns()`. Known hot callers bypass the single-row wrapper and hold `MutableColumns` across the row loop. |

## Tests Added For Real COW Violations

- `NewJsonReaderCowTest.AppendNullForMalformedJsonMutatesOwnerColumn` builds a
  nullable column with an extra `ColumnPtr` alias, calls the malformed-json
  helper, and verifies the block receives a new mutated owner while the original
  shared column remains unchanged.
- `NewJsonReaderCowTest.TruncateBlockToRowsMutatesOwnerColumn` builds a shared
  two-row nullable column, truncates the block, and verifies the original alias
  still has two rows.
- `NewJsonReaderCowTest.PopBackLastInsertedValueMutatesOwnerColumn` builds a
  shared destination column, removes the last inserted value through the JSON
  rollback helper, and verifies the block gets a mutated owner while the
  original alias still has both rows.
- `BlockSerializeCowTest.JsonbToBlockMutatesDestinationOwnerColumn` builds a
  shared destination column, decodes JSONB rows, and verifies the destination
  block gets its own mutated owner while the original shared column remains
  empty.

These tests cover the exact alias mode that external JSON regression exposed:
mutating a block-owned column while another `ColumnPtr` reference to the same
column still exists.

## Fixed During This Audit Series

- `AggregateFunctionSortData::{add,merge}`: removed hot-path
  `assume_mutable()` calls by making the aggregate state own a `MutableBlock`.
- `AggregateFunctionNullUnary::streaming_agg_serialize_to_column`: replaced a
  read-only source nested-column mutable assertion with const access.
- ORC schema-change nullable converter: `align_orc_null_map` now copies from the
  appended source null-map slice instead of offset `0`.
- Parquet schema-change nullable converter: logical-source null maps with an old
  destination prefix now copy from the appended logical-source slice.
- `ColumnArray::create(ColumnPtr...)`: shared-column construction keeps immutable
  subcolumns shared but now reuses the same const-safe offset type and
  nested-size validation as the mutable constructor.
- `ColumnNullable::create(ColumnPtr...)` and `ColumnMap::create(ColumnPtr...)`:
  shared-column construction no longer deep-mutates input subcolumns, avoiding
  unnecessary clones in block wrapping paths while keeping invariant checks.
- `ColumnMap::deduplicate_keys(true)`: recursive nested-map value dedup now
  detaches and writes back the value owner instead of const-casting through a
  shared nullable/value subcolumn.
- `ColumnMap::filter(const Filter&, ...)` and `ColumnMap::permute(...)`: these
  return new columns and therefore keep input subcolumns shared/const instead of
  pre-cloning whole key/value/offset columns.
- Variant materialization for nullable scalar variants: the nested variant is
  taken from the already-detached nullable owner, so root finalization/conversion
  cannot mutate aliases of the original nullable wrapper.
- JSON malformed/rollback paths: changed from `get_columns()` plus
  `assume_mutable()` to owner-slot mutate/writeback helpers with focused BE UTs.
- Rowid fetcher merge and external-row readback: changed shared-output mutation
  and `get_columns()`/`const_cast` destination writes to owner-slot
  mutate/writeback.
- JSONB row-store decode: changed point-query and rowid-fetcher row loops from
  per-row/per-field block mutation to a `MutableColumns` owner held across the
  loop, with a COW unit test for shared destination columns.
