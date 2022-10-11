// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "exec/olap_common.h"
#include "exec/scan_node.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/function_filter.h"
#include "exprs/in_predicate.h"
#include "exprs/runtime_filter.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/tablet.h"
#include "util/progress_updater.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {
class ObjectPool;
class TPlanNode;
class DescriptorTbl;
class RowBatch;
namespace vectorized {

class VOlapScanner;

class VOlapScanNode final : public ScanNode {
public:
    VOlapScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    friend class VOlapScanner;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override {
        return Status::NotSupported("Not Implemented VOlapScanNode Node::get_next scalar");
    }
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;

    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

    Status get_hints(TabletSharedPtr table, const TPaloScanRange& scan_range, int block_row_count,
                     bool is_begin_include, bool is_end_include,
                     const std::vector<std::unique_ptr<OlapScanRange>>& scan_key_range,
                     std::vector<std::unique_ptr<OlapScanRange>>* sub_scan_range,
                     RuntimeProfile* profile);

    std::string get_name() override;

private:
    // In order to ensure the accuracy of the query result
    // only key column conjuncts will be remove as idle conjunct
    bool is_key_column(const std::string& key_name);

    Status start_scan(RuntimeState* state);
    Status normalize_conjuncts();
    Status build_key_ranges_and_filters();

    template <bool IsFixed, PrimitiveType PrimitiveType, typename ChangeFixedValueRangeFunc>
    static Status change_value_range(ColumnValueRange<PrimitiveType>& range, void* value,
                                     const ChangeFixedValueRangeFunc& func,
                                     const std::string& fn_name, int slot_ref_child = -1);

    void transfer_thread(RuntimeState* state);
    void scanner_thread(VOlapScanner* scanner);
    Status start_scan_thread(RuntimeState* state);

    Status _add_blocks(std::vector<Block*>& block);
    int _start_scanner_thread_task(RuntimeState* state, int block_per_scanner);
    Block* _alloc_block(bool& get_free_block);

    void _init_counter(RuntimeState* state);
    // OLAP_SCAN_NODE profile layering: OLAP_SCAN_NODE, OlapScanner, and SegmentIterator
    // according to the calling relationship
    void init_scan_profile();
    const std::vector<TRuntimeFilterDesc>& runtime_filter_descs() const {
        return _runtime_filter_descs;
    }

    template <PrimitiveType T>
    Status _normalize_in_and_eq_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                          SlotDescriptor* slot, ColumnValueRange<T>& range,
                                          bool* push_down);
    template <PrimitiveType T>
    Status _normalize_not_in_and_not_eq_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                                  SlotDescriptor* slot, ColumnValueRange<T>& range,
                                                  bool* push_down);

    template <PrimitiveType T>
    Status _normalize_noneq_binary_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                             SlotDescriptor* slot, ColumnValueRange<T>& range,
                                             bool* push_down);

    template <PrimitiveType T>
    Status _normalize_is_null_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                        SlotDescriptor* slot, ColumnValueRange<T>& range,
                                        bool* push_down);

    Status _normalize_bloom_filter(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                   SlotDescriptor* slot, bool* push_down);

    void eval_const_conjuncts(VExpr* vexpr, VExprContext* expr_ctx, bool* push_down);

    vectorized::VExpr* _normalize_predicate(RuntimeState* state,
                                            vectorized::VExpr* conjunct_expr_root);

    Status _normalize_function_filters(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                       SlotDescriptor* slot, bool* push_down);

    template <bool IsNotIn>
    bool _should_push_down_in_predicate(VInPredicate* in_pred, VExprContext* expr_ctx);

    bool _is_predicate_acting_on_slot(VExpr* expr,
                                      const std::function<bool(const std::vector<VExpr*>&,
                                                               const VSlotRef**, VExpr**)>& checker,
                                      SlotDescriptor** slot_desc, ColumnValueRangeType** range);

    bool _should_push_down_binary_predicate(
            VectorizedFnCall* fn_call, VExprContext* expr_ctx, StringRef* constant_val,
            int* slot_ref_child, const std::function<bool(const std::string&)>& fn_checker);

    bool _should_push_down_function_filter(VectorizedFnCall* fn_call, VExprContext* expr_ctx,
                                           StringVal* constant_str,
                                           doris_udf::FunctionContext** fn_ctx);

    Status _append_rf_into_conjuncts(RuntimeState* state, std::vector<VExpr*>& vexprs);

    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;
    // doris scan node used to scan doris
    TOlapScanNode _olap_scan_node;
    // tuple descriptors
    const TupleDescriptor* _tuple_desc;
    // tuple index
    int _tuple_idx;
    // string slots
    std::vector<SlotDescriptor*> _string_slots;
    // collection slots
    std::vector<SlotDescriptor*> _collection_slots;

    bool _eos;

    // column -> ColumnValueRange map
    std::map<std::string, ColumnValueRangeType> _column_value_ranges;

    OlapScanKeys _scan_keys;

    std::vector<std::unique_ptr<TPaloScanRange>> _scan_ranges;

    std::vector<TCondition> _olap_filter;
    // push down bloom filters to storage engine.
    // 1. std::pair.first :: column name
    // 2. std::pair.second :: shared_ptr of BloomFilterFuncBase
    std::vector<std::pair<std::string, std::shared_ptr<BloomFilterFuncBase>>>
            _bloom_filters_push_down;

    // push down functions to storage engine
    // only support scalar functions, now just support like / not like
    std::vector<FunctionFilter> _push_down_functions;

    // Pool for storing allocated scanner objects.  We don't want to use the
    // runtime pool to ensure that the scanner objects are deleted before this
    // object is.
    ObjectPool _scanner_pool;

    std::shared_ptr<std::thread> _transfer_thread;

    // Keeps track of total splits and the number finished.
    ProgressUpdater _progress;

    // to limit _materialized_row_batches_bytes < _max_scanner_queue_size_bytes / 2
    std::atomic_size_t _materialized_row_batches_bytes = 0;

    std::atomic_int _running_thread = 0;
    std::condition_variable _scan_thread_exit_cv;

    // to limit _scan_row_batches_bytes < _max_scanner_queue_size_bytes / 2
    std::atomic_size_t _scan_row_batches_bytes = 0;

    std::list<VOlapScanner*> _olap_scanners;

    int _max_materialized_row_batches;
    // to limit _materialized_row_batches_bytes and _scan_row_batches_bytes
    size_t _max_scanner_queue_size_bytes;
    bool _start;
    // Used in Scan thread to ensure thread-safe
    std::atomic_bool _scanner_done;
    std::atomic_bool _transfer_done;

    int _total_assign_num;
    int _nice;

    // protect _status, for many thread may change _status
    SpinLock _status_mutex;
    Status _status;
    RuntimeState* _runtime_state;

    RuntimeProfile::Counter* _scan_timer;
    RuntimeProfile::Counter* _scan_cpu_timer = nullptr;
    RuntimeProfile::Counter* _tablet_counter;
    RuntimeProfile::Counter* _rows_pushed_cond_filtered_counter = nullptr;
    RuntimeProfile::Counter* _reader_init_timer = nullptr;
    RuntimeProfile::Counter* _scanner_sched_counter = nullptr;
    TResourceInfo* _resource_info;

    int64_t _buffered_bytes;
    // Count the memory consumption of Rowset Reader and Tablet Reader in OlapScanner.
    std::unique_ptr<MemTracker> _scanner_mem_tracker;
    EvalConjunctsFn _eval_conjuncts_fn;

    // the max num of scan keys of this scan request.
    // it will set as BE's config `doris_max_scan_key_num`,
    // or be overwritten by value in TQueryOptions
    int32_t _max_scan_key_num = 1024;
    // The max number of conditions in InPredicate  that can be pushed down
    // into OlapEngine.
    // If conditions in InPredicate is larger than this, all conditions in
    // InPredicate will not be pushed to the OlapEngine.
    // it will set as BE's config `max_pushdown_conditions_per_column`,
    // or be overwritten by value in TQueryOptions
    int32_t _max_pushdown_conditions_per_column = 1024;

    struct RuntimeFilterContext {
        RuntimeFilterContext() : apply_mark(false), runtimefilter(nullptr) {}
        bool apply_mark;
        IRuntimeFilter* runtimefilter;
    };
    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    std::vector<RuntimeFilterContext> _runtime_filter_ctxs;
    std::vector<bool> _runtime_filter_ready_flag;
    std::shared_mutex _rf_lock;
    std::map<int, RuntimeFilterContext*> _conjunctid_to_runtime_filter_ctxs;

    std::unique_ptr<RuntimeProfile> _scanner_profile;
    std::unique_ptr<RuntimeProfile> _segment_profile;

    // Counters
    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _read_compressed_counter = nullptr;
    RuntimeProfile::Counter* _decompressor_timer = nullptr;
    RuntimeProfile::Counter* _read_uncompressed_counter = nullptr;
    RuntimeProfile::Counter* _raw_rows_counter = nullptr;

    RuntimeProfile::Counter* _rows_vec_cond_counter = nullptr;
    RuntimeProfile::Counter* _vec_cond_timer = nullptr;
    RuntimeProfile::Counter* _short_cond_timer = nullptr;
    RuntimeProfile::Counter* _output_col_timer = nullptr;

    RuntimeProfile::Counter* _stats_filtered_counter = nullptr;
    RuntimeProfile::Counter* _bf_filtered_counter = nullptr;
    RuntimeProfile::Counter* _del_filtered_counter = nullptr;
    RuntimeProfile::Counter* _conditions_filtered_counter = nullptr;
    RuntimeProfile::Counter* _key_range_filtered_counter = nullptr;

    RuntimeProfile::Counter* _block_fetch_timer = nullptr;
    RuntimeProfile::Counter* _block_load_timer = nullptr;
    RuntimeProfile::Counter* _block_load_counter = nullptr;
    // Not used any more, will be removed after non-vectorized code is removed
    RuntimeProfile::Counter* _block_seek_timer = nullptr;
    // Not used any more, will be removed after non-vectorized code is removed
    RuntimeProfile::Counter* _block_seek_counter = nullptr;
    // Add more detail seek timer and counter profile
    // Read process is split into 3 stages: init, first read, lazy read
    RuntimeProfile::Counter* _block_init_timer = nullptr;
    RuntimeProfile::Counter* _block_init_seek_timer = nullptr;
    RuntimeProfile::Counter* _block_init_seek_counter = nullptr;
    RuntimeProfile::Counter* _first_read_timer = nullptr;
    RuntimeProfile::Counter* _first_read_seek_timer = nullptr;
    RuntimeProfile::Counter* _first_read_seek_counter = nullptr;
    RuntimeProfile::Counter* _lazy_read_timer = nullptr;
    RuntimeProfile::Counter* _lazy_read_seek_timer = nullptr;
    RuntimeProfile::Counter* _lazy_read_seek_counter = nullptr;

    RuntimeProfile::Counter* _block_convert_timer = nullptr;

    RuntimeProfile::Counter* _index_load_timer = nullptr;

    // total pages read
    // used by segment v2
    RuntimeProfile::Counter* _total_pages_num_counter = nullptr;
    // page read from cache
    // used by segment v2
    RuntimeProfile::Counter* _cached_pages_num_counter = nullptr;

    // row count filtered by bitmap inverted index
    RuntimeProfile::Counter* _bitmap_index_filter_counter = nullptr;
    // time fro bitmap inverted index read and filter
    RuntimeProfile::Counter* _bitmap_index_filter_timer = nullptr;
    // number of created olap scanners
    RuntimeProfile::Counter* _num_scanners = nullptr;

    // number of segment filtered by column stat when creating seg iterator
    RuntimeProfile::Counter* _filtered_segment_counter = nullptr;
    // total number of segment related to this scan node
    RuntimeProfile::Counter* _total_segment_counter = nullptr;

    RuntimeProfile::Counter* _scanner_wait_batch_timer = nullptr;
    RuntimeProfile::Counter* _scanner_wait_worker_timer = nullptr;

    RuntimeProfile::Counter* _olap_wait_batch_queue_timer = nullptr;

    // for debugging or profiling, record any info as you want
    RuntimeProfile::Counter* _general_debug_timer[GENERAL_DEBUG_COUNT] = {};

    std::vector<Block*> _scan_blocks;
    std::vector<Block*> _materialized_blocks;
    std::mutex _blocks_lock;
    std::condition_variable _block_added_cv;
    std::condition_variable _block_consumed_cv;

    std::mutex _scan_blocks_lock;
    std::condition_variable _scan_block_added_cv;

    std::vector<Block*> _free_blocks;
    std::mutex _free_blocks_lock;

    std::list<VOlapScanner*> _volap_scanners;
    std::mutex _volap_scanners_lock;

    int _max_materialized_blocks;

    size_t _block_size = 0;

    phmap::flat_hash_set<VExpr*> _rf_vexpr_set;
    std::vector<std::unique_ptr<VExprContext*>> _stale_vexpr_ctxs;
    int64_t _limit_per_scanner = -1;
    phmap::flat_hash_map<int, std::pair<SlotDescriptor*, ColumnValueRangeType>>
            _id_to_slot_column_value_range;
};
} // namespace vectorized
} // namespace doris
