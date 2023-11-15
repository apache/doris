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

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <parallel_hashmap/phmap.h>
#include <stdint.h>

#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "exec/olap_common.h"
#include "exprs/function_filter.h"
#include "runtime/define_primitive_type.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/lock.h"
#include "util/runtime_profile.h"
#include "vec/exec/runtime_filter_consumer.h"
#include "vec/exec/scan/scanner_context.h"
#include "vec/exec/scan/vscanner.h"
#include "vec/runtime/shared_scanner_controller.h"

namespace doris {
class BitmapFilterFuncBase;
class BloomFilterFuncBase;
class DescriptorTbl;
class FunctionContext;
class HybridSetBase;
class IRuntimeFilter;
class SlotDescriptor;
class TScanRangeParams;
class TupleDescriptor;

namespace vectorized {
class Block;
class VExpr;
class VExprContext;
class VInPredicate;
class VectorizedFnCall;
} // namespace vectorized
struct StringRef;
} // namespace doris

namespace doris::pipeline {
class ScanOperator;
}

namespace doris::vectorized {

class VScanner;
class VSlotRef;

struct FilterPredicates {
    // Save all runtime filter predicates which may be pushed down to data source.
    // column name -> bloom filter function
    std::vector<std::pair<std::string, std::shared_ptr<BloomFilterFuncBase>>> bloom_filters;

    std::vector<std::pair<std::string, std::shared_ptr<BitmapFilterFuncBase>>> bitmap_filters;

    std::vector<std::pair<std::string, std::shared_ptr<HybridSetBase>>> in_filters;
};

class VScanNode : public ExecNode, public RuntimeFilterConsumer {
public:
    VScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs),
              RuntimeFilterConsumer(id(), tnode.runtime_filters, ExecNode::_row_descriptor,
                                    ExecNode::_conjuncts) {
        if (!tnode.__isset.conjuncts || tnode.conjuncts.empty()) {
            // Which means the request could be fullfilled in a single segment iterator request.
            if (tnode.limit > 0 && tnode.limit < 1024) {
                _should_run_serial = true;
            }
        }
    }
    ~VScanNode() override = default;

    friend class VScanner;
    friend class NewOlapScanner;
    friend class VFileScanner;
    friend class NewJdbcScanner;
    friend class ScannerContext;
    friend class doris::pipeline::ScanOperator;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    virtual void set_scan_ranges(RuntimeState* state,
                                 const std::vector<TScanRangeParams>& scan_ranges) {}

    void set_shared_scan(RuntimeState* state, bool shared_scan) {
        _shared_scan_opt = shared_scan;
        if (_is_pipeline_scan) {
            if (_shared_scan_opt) {
                _shared_scanner_controller =
                        state->get_query_ctx()->get_shared_scanner_controller();
                auto [should_create_scanner, queue_id] =
                        _shared_scanner_controller->should_build_scanner_and_queue_id(id());
                _should_create_scanner = should_create_scanner;
                _context_queue_id = queue_id;
            } else {
                _should_create_scanner = true;
                _context_queue_id = 0;
            }
        }
    }

    TPushAggOp::type get_push_down_agg_type() { return _push_down_agg_type; }

    int64_t get_push_down_count() const { return _push_down_count; }

    // Get next block.
    // If eos is true, no more data will be read and block should be empty.
    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    Status close(RuntimeState* state) override;

    // Clone current _conjuncts to conjuncts, if exists.
    Status clone_conjunct_ctxs(VExprContextSPtrs& conjuncts);

    int runtime_filter_num() const { return (int)_runtime_filter_ctxs.size(); }

    TupleId output_tuple_id() const { return _output_tuple_id; }
    const TupleDescriptor* output_tuple_desc() const { return _output_tuple_desc; }

    Status alloc_resource(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;

    Status try_close(RuntimeState* state);

    bool should_run_serial() const {
        return _should_run_serial || _state->enable_scan_node_run_serial();
    }
    bool ready_to_open() { return _shared_scanner_controller->scanner_context_is_ready(id()); }
    bool ready_to_read() { return !_scanner_ctx->empty_in_queue(_context_queue_id); }

    enum class PushDownType {
        // The predicate can not be pushed down to data source
        UNACCEPTABLE,
        // The predicate can be pushed down to data source
        // and the data source can fully evaludate it
        ACCEPTABLE,
        // The predicate can be pushed down to data source
        // but the data source can not fully evaluate it.
        PARTIAL_ACCEPTABLE
    };

    RuntimeProfile* scanner_profile() { return _scanner_profile.get(); }

protected:
    // Different data sources register different profiles by implementing this method
    virtual Status _init_profile();

    // Process predicates, extract the predicates in the conjuncts that can be pushed down
    // to the data source, and convert them into common expressions structure ColumnPredicate.
    // There are currently 3 types of predicates that can be pushed down to data sources:
    //
    // 1. Simple predicate, with column on left and constant on right, such as "a=1", "b in (1,2,3)" etc.
    // 2. Bloom Filter, predicate condition generated by runtime filter
    // 3. Function Filter, some data sources can accept function conditions, such as "a like 'abc%'"
    //
    // Predicates that can be fully processed by the data source will be removed from conjuncts
    virtual Status _process_conjuncts() {
        RETURN_IF_ERROR(_normalize_conjuncts());
        return Status::OK();
    }

    // Create a list of scanners.
    // The number of scanners is related to the implementation of the data source,
    // predicate conditions, and scheduling strategy.
    // So this method needs to be implemented separately by the subclass of ScanNode.
    // Finally, a set of scanners that have been prepared are returned.
    virtual Status _init_scanners(std::list<VScannerSPtr>* scanners) { return Status::OK(); }

    //  Different data sources can implement the following 3 methods to determine whether a predicate
    //  can be pushed down to the data source.
    //  3 types:
    //      1. binary predicate
    //      2. in/not in predicate
    //      3. function predicate
    //  TODO: these interfaces should be change to become more common.
    virtual Status _should_push_down_binary_predicate(
            VectorizedFnCall* fn_call, VExprContext* expr_ctx, StringRef* constant_val,
            int* slot_ref_child, const std::function<bool(const std::string&)>& fn_checker,
            PushDownType& pdt);

    virtual PushDownType _should_push_down_in_predicate(VInPredicate* in_pred,
                                                        VExprContext* expr_ctx, bool is_not_in);

    virtual Status _should_push_down_function_filter(VectorizedFnCall* fn_call,
                                                     VExprContext* expr_ctx,
                                                     StringRef* constant_str,
                                                     doris::FunctionContext** fn_ctx,
                                                     PushDownType& pdt) {
        pdt = PushDownType::UNACCEPTABLE;
        return Status::OK();
    }

    virtual bool _should_push_down_common_expr() { return false; }

    virtual bool _storage_no_merge() { return false; }

    virtual PushDownType _should_push_down_bloom_filter() { return PushDownType::UNACCEPTABLE; }

    virtual PushDownType _should_push_down_bitmap_filter() { return PushDownType::UNACCEPTABLE; }

    virtual PushDownType _should_push_down_is_null_predicate() {
        return PushDownType::UNACCEPTABLE;
    }

    // Return true if it is a key column.
    // Only predicate on key column can be pushed down.
    virtual bool _is_key_column(const std::string& col_name) { return false; }

    Status _prepare_scanners(const int query_parallel_instance_num);

    bool _is_pipeline_scan = false;
    bool _shared_scan_opt = false;

    // the output tuple of this scan node
    TupleId _output_tuple_id = -1;
    const TupleDescriptor* _output_tuple_desc = nullptr;

    doris::Mutex _block_lock;

    // These two values are from query_options
    int _max_scan_key_num;
    int _max_pushdown_conditions_per_column;

    // Each scan node will generates a ScannerContext to manage all Scanners.
    // See comments of ScannerContext for more details
    std::shared_ptr<ScannerContext> _scanner_ctx;

    // indicate this scan node has no more data to return
    bool _eos = false;
    bool _opened = false;

    FilterPredicates _filter_predicates {};

    // Save all function predicates which may be pushed down to data source.
    std::vector<FunctionFilter> _push_down_functions;

    // slot id -> ColumnValueRange
    // Parsed from conjuncts
    phmap::flat_hash_map<int, std::pair<SlotDescriptor*, ColumnValueRangeType>>
            _slot_id_to_value_range;
    // column -> ColumnValueRange
    // We use _colname_to_value_range to store a column and its conresponding value ranges.
    std::unordered_map<std::string, ColumnValueRangeType> _colname_to_value_range;
    /**
     * _colname_to_value_range only store the leaf of and in the conjunct expr tree,
     * we use _compound_value_ranges to store corresponding value ranges
     * in the one compound relationship except the leaf of and node,
     * such as `where a > 1 or b > 10 and c < 200`, the expr tree like: 
     *     or
     *   /   \ 
     *  a     and
     *       /   \
     *      b     c
     * the value ranges of column a,b,c will all store into _compound_value_ranges
     */
    std::vector<ColumnValueRangeType> _compound_value_ranges;
    // But if a col is with value range, eg: 1 < col < 10, which is "!is_fixed_range",
    // in this case we can not merge "1 < col < 10" with "col not in (2)".
    // So we have to save "col not in (2)" to another structure: "_not_in_value_ranges".
    // When the data source try to use the value ranges, it should use both ranges in
    // "_colname_to_value_range" and in "_not_in_value_ranges"
    std::vector<ColumnValueRangeType> _not_in_value_ranges;

    // If the query like select * from table limit 10; then the query should run in
    // single scanner to avoid too many scanners which will cause lots of useless read.
    bool _should_run_serial = false;

    // Every time vconjunct_ctx_ptr is updated, the old ctx will be stored in this vector
    // so that it will be destroyed uniformly at the end of the query.
    VExprContextSPtrs _stale_expr_ctxs;
    VExprContextSPtrs _common_expr_ctxs_push_down;

    // If sort info is set, push limit to each scanner;
    int64_t _limit_per_scanner = -1;

    std::shared_ptr<vectorized::SharedScannerController> _shared_scanner_controller;
    bool _should_create_scanner = false;
    int _context_queue_id = -1;

    std::shared_ptr<RuntimeProfile> _scanner_profile;

    // rows read from the scanner (including those discarded by (pre)filters)
    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _byte_read_counter;
    // Wall based aggregate read throughput [rows/sec]
    RuntimeProfile::Counter* _total_throughput_counter;
    RuntimeProfile::Counter* _num_scanners;

    RuntimeProfile::Counter* _get_next_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _alloc_resource_timer = nullptr;
    RuntimeProfile::Counter* _acquire_runtime_filter_timer = nullptr;
    // time of get block from scanner
    RuntimeProfile::Counter* _scan_timer = nullptr;
    RuntimeProfile::Counter* _scan_cpu_timer = nullptr;
    // time of prefilter input block from scanner
    RuntimeProfile::Counter* _prefilter_timer = nullptr;
    // time of convert input block to output block from scanner
    RuntimeProfile::Counter* _convert_block_timer = nullptr;
    // time of filter output block from scanner
    RuntimeProfile::Counter* _filter_timer = nullptr;

    RuntimeProfile::Counter* _scanner_sched_counter = nullptr;
    RuntimeProfile::Counter* _scanner_ctx_sched_counter = nullptr;
    RuntimeProfile::Counter* _scanner_ctx_sched_time = nullptr;
    RuntimeProfile::Counter* _scanner_wait_batch_timer = nullptr;
    RuntimeProfile::Counter* _scanner_wait_worker_timer = nullptr;
    // Num of newly created free blocks when running query
    RuntimeProfile::Counter* _newly_create_free_blocks_num = nullptr;
    // Max num of scanner thread
    RuntimeProfile::Counter* _max_scanner_thread_num = nullptr;

    RuntimeProfile::Counter* _memory_usage_counter;
    RuntimeProfile::HighWaterMarkCounter* _queued_blocks_memory_usage;
    RuntimeProfile::HighWaterMarkCounter* _free_blocks_memory_usage;

    std::unordered_map<std::string, int> _colname_to_slot_id;
    std::vector<int> _col_distribute_ids;

    TPushAggOp::type _push_down_agg_type;

    // Record the value of the aggregate function 'count' from doris's be
    int64_t _push_down_count = -1;

private:
    Status _normalize_conjuncts();
    Status _normalize_predicate(const VExprSPtr& conjunct_expr_root, VExprContext* context,
                                VExprSPtr& output_expr);
    Status _eval_const_conjuncts(VExpr* vexpr, VExprContext* expr_ctx, PushDownType* pdt);

    Status _normalize_bloom_filter(VExpr* expr, VExprContext* expr_ctx, SlotDescriptor* slot,
                                   PushDownType* pdt);

    Status _normalize_bitmap_filter(VExpr* expr, VExprContext* expr_ctx, SlotDescriptor* slot,
                                    PushDownType* pdt);

    Status _normalize_function_filters(VExpr* expr, VExprContext* expr_ctx, SlotDescriptor* slot,
                                       PushDownType* pdt);

    bool _is_predicate_acting_on_slot(
            VExpr* expr,
            const std::function<bool(const VExprSPtrs&, std::shared_ptr<VSlotRef>&, VExprSPtr&)>&
                    checker,
            SlotDescriptor** slot_desc, ColumnValueRangeType** range);

    template <PrimitiveType T>
    Status _normalize_in_and_eq_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                          SlotDescriptor* slot, ColumnValueRange<T>& range,
                                          PushDownType* pdt);
    template <PrimitiveType T>
    Status _normalize_not_in_and_not_eq_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                                  SlotDescriptor* slot, ColumnValueRange<T>& range,
                                                  PushDownType* pdt);

    template <PrimitiveType T>
    Status _normalize_noneq_binary_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                             SlotDescriptor* slot, ColumnValueRange<T>& range,
                                             PushDownType* pdt);

    Status _normalize_compound_predicate(
            vectorized::VExpr* expr, VExprContext* expr_ctx, PushDownType* pdt,
            bool is_runtimer_filter_predicate,
            const std::function<bool(const VExprSPtrs&, std::shared_ptr<VSlotRef>&, VExprSPtr&)>&
                    in_predicate_checker,
            const std::function<bool(const VExprSPtrs&, std::shared_ptr<VSlotRef>&, VExprSPtr&)>&
                    eq_predicate_checker);

    template <PrimitiveType T>
    Status _normalize_binary_in_compound_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                                   SlotDescriptor* slot, ColumnValueRange<T>& range,
                                                   PushDownType* pdt);

    template <PrimitiveType T>
    Status _normalize_match_in_compound_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                                  SlotDescriptor* slot, ColumnValueRange<T>& range,
                                                  PushDownType* pdt);

    template <PrimitiveType T>
    Status _normalize_is_null_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                        SlotDescriptor* slot, ColumnValueRange<T>& range,
                                        PushDownType* pdt);

    template <PrimitiveType T>
    Status _normalize_match_predicate(vectorized::VExpr* expr, VExprContext* expr_ctx,
                                      SlotDescriptor* slot, ColumnValueRange<T>& range,
                                      PushDownType* pdt);

    template <bool IsFixed, PrimitiveType PrimitiveType, typename ChangeFixedValueRangeFunc>
    static Status _change_value_range(ColumnValueRange<PrimitiveType>& range, void* value,
                                      const ChangeFixedValueRangeFunc& func,
                                      const std::string& fn_name, int slot_ref_child = -1);

    // Submit the scanner to the thread pool and start execution
    Status _start_scanners(const std::list<VScannerSPtr>& scanners,
                           const int query_parallel_instance_num);
};

} // namespace doris::vectorized
