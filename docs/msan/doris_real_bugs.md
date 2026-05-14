# Doris 真实 Bug 清单 — MSAN 发现

**生成日期**：2026-04-21
**数据来源**：variant_p0 回归测试下 `.deploy/output/be/log/msan.1888366`（735MB）
**总 warning 数**：~46,000 条，**全部指向 Doris 自身代码**（0 条三方库）

## 前置背景

经过一天的 MSAN ignorelist 缩减（74 → 20 条目），所有参与 BE 运行时的三方库（包括 CLucene、brpc、OpenSSL、protobuf、rocksdb、arrow 等 44 个库）都已通过 MSAN instrumentation。这次跑 variant_p0 收集到的 46k warning **全部是 Doris 自身代码的真实 bug**（或疑似 bug）。

ignorelist 剩余 20 条目皆为物理不可 instrument 的库：SIMD（hyperscan/simdjson/xsimd/...）、JVM 桥接（hadoop/libhdfs3/paimon/jemalloc）、libunwind、glibc-compatibility。

## Bug 清单汇总

| # | 位置 | 触发次数 | 严重程度 | 类型 |
|---|------|---------|---------|------|
| 1 | `vexpr.h:211 VExpr::op()` | 4,947 | 🔴 高 | 未初始化成员读取 |
| 2 | `segment_iterator.cpp:3068/3105/3112` | 28,140 | 🔴 高 | Bug #1 连锁触发 |
| 3 | `vexpr_context.h:178-179 IndexExecContext::set_true_for_index_status` | 1,416 | 🟡 中 | unordered_map 未初始化 VExpr* |
| 4 | `id_manager.h:174/179 IdFileMap::get_file_mapping_id` | 1,028 | 🟡 中 | StorageReadOptions 字段未初始化 |
| 5 | `writev` (libc interceptor) | 74 | 🔴 **安全** | **信息泄漏：iovec offset 8735 未初始化** |
| 6 | `memcmp` 在 `ScalarColumnWriter::finish_current_page` | 5 | 🟡 中 | segment page 有未初始化字节 |
| 7 | `string_ref.h:157 string_compare` | 2 | 🟡 中 | ZoneMap index StringRef 未初始化 |
| 8 | `column_variant.cpp:812 ColumnVariant::try_insert` | 22 | 🔴 **严重** | **Use-after-destroy（Field）** |
| 9 | `aggregate_function_product.h:175` | 15 | 🟢 低 | 聚合 state 未初始化 |

---

## Bug #1：`VExpr::op()` 读取未初始化 `_opcode`

### 基本信息
- **触发**：4,947 次
- **位置**：`be/src/exprs/vexpr.h:211`
- **代码**：
  ```cpp
  TExprOpcode::type op() const { return _opcode; }
  ```
- **成员声明**（`be/src/exprs/vexpr.h:419-421`）：
  ```cpp
  TExprNodeType::type _node_type {};
  TExprOpcode::type _opcode {TExprOpcode::INVALID_OPCODE};
  ```
  即使加了默认值，仍然触发——说明有构造路径绕过。

### 触发栈
```
#0 VExpr::op()                                                    vexpr.h:211
#1 SegmentIterator::_calculate_expr_in_remaining_conjunct_root()  segment_iterator.cpp:3103
#2 SegmentIterator::_initialize_predicate_results()               segment_iterator.cpp:445
#3 SegmentIterator::_init_impl()                                  segment_iterator.cpp:434
#4 SegmentIterator::init()                                        segment_iterator.cpp:343
#5 Segment::new_iterator()                                        segment.cpp:328
... → OlapScanner::_open_impl → Scanner::open → ScannerScheduler
```

### 分析
`_opcode` 已加默认值 `INVALID_OPCODE`，但仍然触发。可能的漏网路径：
- 从 Thrift 反序列化（`TExprNode`）创建但走了其他构造函数
- `shared_ptr<VExpr>` 的 control block 指向的 VExpr 对象没走我们修过的构造函数
- 某处用 `operator new` 分配 VExpr 但没调用构造函数
- 拷贝构造 `VExpr(const VExpr&) = default` 从源 `_opcode` 未初始化的实例拷贝

**下一步排查**：grep 所有 `VExpr` 的创建路径，看是否有 placement new 或 memset 后没 construct 的情况。

---

## Bug #2：`SegmentIterator::_calculate_expr_in_remaining_conjunct_root` 连锁触发

### 基本信息
- **触发**：28,140 次（3068 + 3105 + 3112 合计）
- **位置**：`be/src/storage/segment/segment_iterator.cpp:3060-3118`

### 代码片段
```cpp
std::stack<VExprSPtr> stack;
stack.emplace(root_expr);
while (!stack.empty()) {
    const auto& expr = stack.top();
    stack.pop();
    for (const auto& child : expr->children()) {                    // line 3068
        if (child->is_virtual_slot_ref()) { ... }
        auto expr_without_cast = VExpr::expr_without_cast(child);
        if (expr_without_cast->is_slot_ref()
            && expr->op() != TExprOpcode::CAST) {                   // line 3103
            // ...
            _common_expr_index_exec_status[...][expr.get()] = false; // line 3105
            _common_expr_to_slotref_map[...][...] = expr.get();     // line 3106
        }
    }
    const auto& children = expr->children();                         // line 3112
    for (int i = cast_set<int>(children.size()) - 1; i >= 0; --i) {
        // ...
    }
}
```

### 触发栈（line 3068）
```
#0 shared_ptr<VExpr>::operator->  (shadow inconsistent)
#1 SegmentIterator::_calculate_expr_in_remaining_conjunct_root   segment_iterator.cpp:3068
```

### Origin
```
Uninitialized value was stored to memory at
#0 shared_ptr<VExpr>::operator->                    shared_ptr.h:634
#1 SegmentIterator::_calculate_expr_in_remaining_conjunct_root  segment_iterator.cpp:3068
```

### 分析
origin 是 `shared_ptr<VExpr>::operator->`。真正的 uninit value 是 **shared_ptr 指向的 VExpr 的内部成员**（即 Bug #1 的 `_opcode`）。修 Bug #1 后此 bug 的大部分会消失。

3105/3112 是同一问题的不同访问点。

---

## Bug #3：`IndexExecContext::set_true_for_index_status` unordered_map 未初始化 VExpr*

### 基本信息
- **触发**：1,416 次（178:47 + 179:17）
- **位置**：`be/src/exprs/vexpr_context.h:172-182`

### 代码
```cpp
void set_true_for_index_status(const VExpr* expr, int column_index) {
    if (column_index < 0 || column_index >= _col_ids.size()) {
        return;
    }
    const auto& column_id = _col_ids[column_index];
    if (_expr_index_status.contains(column_id)) {
        if (_expr_index_status[column_id].contains(expr)) {          // line 178
            _expr_index_status[column_id][expr] = true;              // line 179
        }
    }
}
```

### 触发栈
```
#0 unordered_map::find/contains (hash 计算 expr 指针)
#1 IndexExecContext::set_true_for_index_status      vexpr_context.h:178
#2 VExpr::_evaluate_inverted_index                  vexpr.cpp:975
#3 VMatchPredicate::evaluate_inverted_index         vmatch_predicate.cpp:152
#4 VExprContext::evaluate_inverted_index            vexpr_context.cpp:185
#5 SegmentIterator::_apply_index_expr               segment_iterator.cpp:1172
#6 SegmentIterator::_get_row_ranges_by_column_conditions  segment_iterator.cpp:765
#7 SegmentIterator::_lazy_init                      segment_iterator.cpp:468
```

### Origin
```
Uninitialized value was stored to memory at
#0 __hash_node::__hash_node       __hash_table:139
... (libc++ 正常的 hash_node 构造)
#5 unordered_map::operator[]      unordered_map:1751
#6 SegmentIterator::_calculate_expr_in_remaining_conjunct_root  segment_iterator.cpp:3105
```

### 分析
`_expr_index_status` 是 `unordered_map<column_id, unordered_map<VExpr*, bool>>`。出问题的是 map 中存储的 `VExpr*` key —— 往 map 里 insert 时 VExpr 指针的某些字节未初始化（其实指针 void* 本身应该是有效的，但 shared_ptr<VExpr>.get() 返回的某些语义下可能还是触发 shadow）。

**实际根因**：当 `IndexExecContext` 调用链从 `_calculate_expr_in_remaining_conjunct_root` 传过来时，`expr.get()` 里的值虽然是有效指针但 MSAN 认为 shadow 未初始化 —— 是 Bug #1 的延伸。

---

## Bug #4：`IdFileMap::get_file_mapping_id` —— StorageReadOptions 字段未初始化

### 基本信息
- **触发**：1,028 次（174:34 + 179:9）
- **位置**：`be/src/storage/id_manager.h:170-182`

### 代码
```cpp
int64_t get_file_mapping_id(const std::shared_ptr<FileMapping>& mapping) {
    DCHECK(mapping.get() != nullptr);
    auto value = mapping->file_mapping_info_to_string();          // line 171
    std::unique_lock lock(_mtx);
    auto it = _mapping_to_id.find(value);                         // line 174
    if (it != _mapping_to_id.end()) {
        return it->second;
    }
    _id_map[_init_id++] = mapping;                                // line 178
    _mapping_to_id[value] = _init_id - 1;                         // line 179
    return _init_id - 1;
}
```

### Origin（关键！）
```
Uninitialized value was stored to memory at
#0 __msan_memcpy
#1 StorageReadOptions::StorageReadOptions(const&)    iterators.h:47
#2 LazyInitSegmentIterator::LazyInitSegmentIterator  lazy_init_segment_iterator.cpp:31
#3 make_unique<LazyInitSegmentIterator>
#4 BetaRowsetReader::get_segment_iterators           beta_rowset_reader.cpp:290
#5 BetaRowsetReader::_init_iterator                  beta_rowset_reader.cpp:317
```

### 分析
**Origin 指向 `StorageReadOptions` 的拷贝构造函数**（`iterators.h:47`）。说明 `StorageReadOptions` 有**某个字段未初始化**，被 memcpy 拷贝时传播到下游。下游 `FileMapping` 读到这个未初始化字段，`file_mapping_info_to_string()` 返回的 string 带上这些字节，导致 `_mapping_to_id.find(value)` 触发 MSAN。

### 修法建议
1. 检查 `StorageReadOptions`（`be/src/storage/iterators.h` 附近）所有字段默认初始化
2. 检查 `FileMapping` 类字段
3. `file_mapping_info_to_string()` 如果用到 padding 或 union，考虑 memset 或 `__msan_unpoison`

---

## Bug #5：`writev` 传入未初始化 iovec（⚠️ 信息泄漏安全问题）

### 基本信息
- **触发**：74 次
- **libc interceptor**：compiler-rt 的 `writev` interceptor

### 栈（极简）
```
==1888366==WARNING: MemorySanitizer: use-of-uninitialized-value
Uninitialized bytes in read_iovec at offset 8735 inside [0x730000880000, 65536)
#0 writev  (doris_be+0xf98ba07)
SUMMARY: (doris_be+0xf98ba07) in writev
ORIGIN: invalid (0). Might be a bug in MemorySanitizer origin tracking.
```

### 分析
- `fast_unwind_on_fatal=1` 让我们只看到 interceptor 顶层，没有上游 Doris 代码栈。需要改配置重跑获取完整栈
- `offset 8735 inside [0x..., 65536)`：iovec 第一块是 64KB，其中第 8735 字节未初始化
- 这是**将未初始化内存通过 socket 发送出去**的行为，违反 ISO C++ UB，也是**信息泄漏**

### 修法建议
1. 改 MSAN options 为 `fast_unwind_on_fatal=0` 重跑，抓到完整调用栈
2. 定位到具体调用 `writev` 的代码（可能是 brpc 的 socket 写入，或 Doris 自己的 disk I/O）
3. 该路径写入前应该已知数据长度；如果 buffer 尾部有 padding，要么 zero-fill 要么 truncate iovec 长度

### 安全意义
可能把进程内存内容泄漏给：
- RPC 对端（其他 BE / FE）
- Stream load client
- 磁盘文件

---

## Bug #6：`memcmp` 在 `ScalarColumnWriter::finish_current_page`

### 基本信息
- **触发**：5 次
- **libc interceptor**：compiler-rt `memcmp`

### 栈
```
#0 memcmp  (libc interceptor)
#1 ScalarColumnWriter::finish_current_page                    column_writer.cpp:834
#2 ScalarColumnWriter::append_data                            column_writer.cpp:619
#3 ScalarColumnWriter::append_nullable                        column_writer.cpp:708
#4 ColumnWriter::append                                       column_writer.cpp:466
#5 VerticalSegmentWriter::write_batch                         vertical_segment_writer.cpp:1046
#6 SegmentFlusher::_add_rows                                  segment_creator.cpp:99
#7 SegmentFlusher::flush_single_block                         segment_creator.cpp:72
#8 SegmentCreator::flush_single_block                         segment_creator.cpp:402
#9 BaseBetaRowsetWriter::flush_memtable                       beta_rowset_writer.cpp:816
#10 FlushToken::_do_flush_memtable                            memtable_flush_executor.cpp:210
#11 FlushToken::_flush_memtable                               memtable_flush_executor.cpp:267
#12 MemtableFlushTask::run                                    memtable_flush_executor.cpp:63
```

### 分析
在 memtable flush 路径中 `finish_current_page()` 用 `memcmp` 比较 page 数据时发现未初始化字节。可能是：
- Column buffer padding 区域没清零
- 某些 null row 位置对应的 value 区未初始化（应该不影响逻辑但 memcmp 看到了）

### 修法建议
查看 `column_writer.cpp:834` 的 `memcmp` 究竟在比较什么。可能是 dict page 和 data page 的比较。

---

## Bug #7：`string_compare` / ZoneMap minmax_element 未初始化 StringRef

### 基本信息
- **触发**：2 次
- **位置**：`be/src/core/string_ref.h:157`

### 栈
```
#0 string_compare                       string_ref.h:157
#1 StringRef::compare                   string_ref.h:260
#2 StringRef::lt                        string_ref.h:285
#3 StringRef::operator<                 string_ref.h:295
#4 std::__less<>::operator()            libc++ comp.h:41
#5 _MinmaxElementLessFunc::operator()   minmax_element.h:37
#6 __minmax_element_impl                minmax_element.h:65
#7 std::minmax_element
#8 TypedZoneMapIndexWriter<STRING>::add_values  zone_map_index.cpp:164
#9 ScalarColumnWriter::_internal_append_data_in_current_page  column_writer.cpp:629
... (同 Bug #6 的 memtable flush 路径)
```

### 分析
ZoneMap 索引给 string 列算 min/max 时，传入的 `StringRef` 数组中某个的 `data` 或 `size` 字段未初始化。

### 修法建议
- 检查 `zone_map_index.cpp:164` 上游传入的 StringRef 数组
- 可能是 null row 对应的 StringRef 没设默认值

---

## Bug #8：`ColumnVariant::try_insert` — **Use-After-Destroy（严重）**

### 基本信息
- **触发**：22 次
- **位置**：`be/src/core/column/column_variant.cpp:812`
- **⚠️ 这不是未初始化，是 use-after-destroy！**

### 代码
```cpp
void ColumnVariant::try_insert(const Field& field) {
    size_t old_size = size();
    const auto& object = field.get<TYPE_VARIANT>();
    for (const auto& [key, value] : object) {              // line 812 — 这里访问的 field 已经被析构过
        ...
    }
}
```

### Origin（关键！）
```
Member fields were destroyed             ← 不是"uninitialized"，是"destroyed"
#0 __sanitizer_dtor_callback_fields
#1 doris::Field::destroy()               field.cpp:388
#2 doris::Field::operator=(Field&&)      field.h:251
#3 ColumnVariant::get(..., Field&)       column_variant.cpp:1024
#4 ColumnVariant::operator[]             column_variant.cpp:982
#5 ColumnVariant::insert_from            column_variant.cpp:805
```

### 触发栈
```
#0 ColumnVariant::try_insert             column_variant.cpp:812
#1 ColumnVariant::insert_from            column_variant.cpp:805
#2 ColumnNullable::insert_from           column_nullable.cpp:344
#3 IColumn::insert_from_multi_column_impl<ColumnNullable>  column.h:750
#4 COWHelper::insert_from_multi_column   cow.h:444
#5 VSortedRunMerger::get_next            vsorted_run_merger.cpp:174/197
#6 LocalMergeSortSourceOperatorX::main_source_get_block  local_merge_sort_source_operator.cpp:140
#7 LocalMergeSortSourceOperatorX::get_block
#8 OperatorXBase::get_block_after_projects
#9 PipelineTask::execute
```

### 分析（严重 bug）
`MSAN` + `poison_in_dtor=1` 检测到：
- **`ColumnVariant::insert_from(col, n)`** 里通过 `operator[](n)` 得到一个 `Field` 临时变量（`col[n]` 返回 `Field`）
- 调用链：`operator[]` → `get(n, field)` → `field = std::move(rhs)` → **`Field::destroy()` 析构了原来的 field members**
- 返回后 `field` 对象看似合法，实际 members 已被析构（poison）
- `try_insert(field)` 里 `field.get<TYPE_VARIANT>()` 访问到的 VariantField 是 poisoned

### 这是真实 bug
- 在 sort merge 路径上调用
- 影响：variant 列参与 order by 时可能 crash 或产生错误数据
- ASan 下也应该能抓到（如果跑过相同路径）

### 修法建议
`Field::operator=(Field&&)` 应该先检查 self-assignment，或者 `ColumnVariant::get(n, field)` 应该先构造新 Field 再赋值，而不是 move-assign 到已有 field。具体需要看 `field.h:251` 和 `column_variant.cpp:1024` 的交互。

---

## Bug #9：`AggregateFunctionProduct::add` state 未初始化

### 基本信息
- **触发**：15 次（9 个 T=INT64，6 个 T=INT32）
- **位置**：`be/src/exprs/aggregate/aggregate_function_product.h:175`

### 代码
```cpp
void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num, Arena&) const override {
    const auto& column = assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(*columns[0]);
    this->data(place).add(                                              // line 175
        typename PrimitiveTypeTraits<TResult>::CppType(column.get_data()[row_num]),
        multiplier);
}
```

### 栈
```
#0 AggregateFunctionProduct<T>::add                        aggregate_function_product.h:175
#1 AggregateFunctionNullUnaryInline<Product, true>::add    aggregate_function_null.h:339
#2 AggregateFunctionNullUnaryInline<Product, true>::add_batch_range  aggregate_function_null.h:408
#3 ArrayAggregateImpl<product>::execute_type_impl          function_array_aggregation.cpp:235
#4 ArrayAggregateImpl<product>::execute_type               function_array_aggregation.cpp:279
#5 ArrayAggregateImpl<product>::execute                    function_array_aggregation.cpp:181
#6 FunctionArrayMapped<product>::execute_impl              function_array_mapped.h:62
...
```

### Origin（关键！）
```
Uninitialized value was created by a heap allocation
#0 operator new(unsigned long)
#1 make_unique<AggregateFunctionProduct<...>>              __memory/unique_ptr.h:767
#2 creator_without_type::create_unary_arguments<Product>   helpers.h:272
```

### 分析
- `AggregateFunctionProductData` 的 heap 分配实例（通过 `make_unique`）—— 构造函数没有初始化 `result` 字段
- `add` 时 `this->data(place).add(...)` 做乘法，读取未初始化的 `result`
- 在 `array_product()` 函数下触发（`ArrayAggregateImpl<AggregateOperation::product>`）

### 修法建议
查看 `AggregateFunctionProductData`（`aggregate_function_product.h` 附近）的成员，给 `result` 默认初始化为 1（乘法单位元）或 0。

---

## 统计

| 指标 | 数值 |
|------|------|
| 独立 bug 类型 | 9 |
| 总 warning 次数 | ~46,000 |
| 信息泄漏级别（需立即修） | 1（Bug #5 writev） |
| Use-after-destroy 级别 | 1（Bug #8 ColumnVariant） |
| 未初始化读取 | 7 |

## 修复优先级建议

| 顺序 | Bug | 理由 | 预期消除 warning |
|------|-----|------|-----------------|
| 1 | **#8** ColumnVariant UAD | 安全性：UAD 是 UB，可能导致 crash | ~22 |
| 2 | **#5** writev 信息泄漏 | 安全性：可能泄漏进程内存 | ~74 |
| 3 | **#1+#2** VExpr + segment_iterator | 量最大，一次修改可能消除 ~33000 条 | ~33,087 |
| 4 | **#4** StorageReadOptions 字段未初始化 | 拷贝时传播很广 | ~1,028 |
| 5 | **#3** IndexExecContext（大概率 Bug #1 传播） | Bug #1 修完可能自愈 | ~1,416 |
| 6 | **#9** AggregateFunctionProduct | 简单 | ~15 |
| 7 | **#6** memcmp in finish_current_page | 需要深入分析 | ~5 |
| 8 | **#7** string_compare in ZoneMap | 需要深入分析 | ~2 |

## 引用

- 完整 MSAN 日志：`.deploy/output/be/log/msan.1888366`（735MB）
- 提取的单个 bug 栈：`/tmp/bug_traces/{3068,3105,3112,vexpr,vexpr_ctx_178,vexpr_ctx_179,id_mgr_174,id_mgr_179,writev,col_variant,agg_prod,memcmp,string_cmp}.txt`
- MSAN 整体进度：`.claude/projects/.../memory/project_msan_progress.md`

## 验证修复

每次修完一个 bug 重跑：
```bash
./build.sh --be --msan -j 64
cp -f output/be/lib/doris_be .deploy/output/be/lib/doris_be
.deploy/output/be/bin/start_be.sh --daemon
bash run-regression-test.sh --run -d variant_p0
grep "SUMMARY:" .deploy/output/be/log/msan.* | sort | uniq -c | sort -rn
```

对应 bug 类型的 warning 应下降/归零。
