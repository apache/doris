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

#include <stdint.h>

#include <cstdint>
#include <string>

#include "common/status.h"
#include "exprs/function_filter.h"
#include "operator.h"
#include "pipeline/common/runtime_filter_consumer.h"
#include "pipeline/dependency.h"
#include "runtime/descriptors.h"
#include "vec/exec/scan/vscan_node.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
class ScannerDelegate;
}

namespace doris::pipeline {

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

struct FilterPredicates {
    // Save all runtime filter predicates which may be pushed down to data source.
    // column name -> bloom filter function
    std::vector<std::pair<std::string, std::shared_ptr<BloomFilterFuncBase>>> bloom_filters;

    std::vector<std::pair<std::string, std::shared_ptr<BitmapFilterFuncBase>>> bitmap_filters;

    std::vector<std::pair<std::string, std::shared_ptr<HybridSetBase>>> in_filters;
};

class ScanLocalStateBase : public PipelineXLocalState<>, public RuntimeFilterConsumer {
public:
    ScanLocalStateBase(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<>(state, parent),
              RuntimeFilterConsumer(parent->node_id(), parent->runtime_filter_descs(),
                                    parent->row_descriptor(), _conjuncts) {}
    ~ScanLocalStateBase() override = default;

    [[nodiscard]] virtual bool should_run_serial() const = 0;

    virtual RuntimeProfile* scanner_profile() = 0;

    [[nodiscard]] virtual const TupleDescriptor* input_tuple_desc() const = 0;
    [[nodiscard]] virtual const TupleDescriptor* output_tuple_desc() const = 0;

    virtual int64_t limit_per_scanner() = 0;

    [[nodiscard]] virtual int runtime_filter_num() const = 0;

    virtual Status clone_conjunct_ctxs(vectorized::VExprContextSPtrs& conjuncts) = 0;
    virtual void set_scan_ranges(RuntimeState* state,
                                 const std::vector<TScanRangeParams>& scan_ranges) = 0;

    virtual TPushAggOp::type get_push_down_agg_type() = 0;

    virtual int64_t get_push_down_count() = 0;

    [[nodiscard]] std::string get_name() { return _parent->get_name(); }

protected:
    friend class vectorized::ScannerContext;
    friend class vectorized::VScanner;

    virtual Status _init_profile() = 0;

    std::atomic<bool> _opened {false};

    DependencySPtr _scan_dependency = nullptr;

    std::shared_ptr<RuntimeProfile> _scanner_profile;
    RuntimeProfile::Counter* _scanner_sched_counter = nullptr;
    RuntimeProfile::Counter* _scanner_ctx_sched_time = nullptr;
    RuntimeProfile::Counter* _scanner_wait_batch_timer = nullptr;
    RuntimeProfile::Counter* _scanner_wait_worker_timer = nullptr;
    // Num of newly created free blocks when running query
    RuntimeProfile::Counter* _newly_create_free_blocks_num = nullptr;
    // Max num of scanner thread
    RuntimeProfile::Counter* _max_scanner_thread_num = nullptr;
    // time of get block from scanner
    RuntimeProfile::Counter* _scan_timer = nullptr;
    RuntimeProfile::Counter* _scan_cpu_timer = nullptr;
    // time of convert input block to output block from scanner
    RuntimeProfile::Counter* _convert_block_timer = nullptr;
    // time of filter output block from scanner
    RuntimeProfile::Counter* _filter_timer = nullptr;
    RuntimeProfile::Counter* _memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _free_blocks_memory_usage = nullptr;
    RuntimeProfile::Counter* _scale_up_scanners_counter = nullptr;
    // rows read from the scanner (including those discarded by (pre)filters)
    RuntimeProfile::Counter* _rows_read_counter = nullptr;

    // Wall based aggregate read throughput [rows/sec]
    RuntimeProfile::Counter* _total_throughput_counter = nullptr;
    RuntimeProfile::Counter* _num_scanners = nullptr;

    RuntimeProfile::Counter* _wait_for_rf_timer = nullptr;
};

template <typename LocalStateType>
class ScanOperatorX;
template <typename Derived>
class ScanLocalState : public ScanLocalStateBase {
    ENABLE_FACTORY_CREATOR(ScanLocalState);
    ScanLocalState(RuntimeState* state, OperatorXBase* parent)
            : ScanLocalStateBase(state, parent) {}
    ~ScanLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    std::string debug_string(int indentation_level) const final;

    [[nodiscard]] bool should_run_serial() const override;

    RuntimeProfile* scanner_profile() override { return _scanner_profile.get(); }

    [[nodiscard]] const TupleDescriptor* input_tuple_desc() const override;
    [[nodiscard]] const TupleDescriptor* output_tuple_desc() const override;

    int64_t limit_per_scanner() override;

    [[nodiscard]] int runtime_filter_num() const override {
        return (int)_runtime_filter_ctxs.size();
    }

    Status clone_conjunct_ctxs(vectorized::VExprContextSPtrs& conjuncts) override;
    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override {}

    TPushAggOp::type get_push_down_agg_type() override;

    int64_t get_push_down_count() override;

    std::vector<Dependency*> filter_dependencies() override {
        if (_filter_dependencies.empty()) {
            return {};
        }
        std::vector<Dependency*> res;
        res.resize(_filter_dependencies.size());
        for (size_t i = 0; i < _filter_dependencies.size(); i++) {
            res[i] = _filter_dependencies[i].get();
        }
        return res;
    }

    std::vector<Dependency*> dependencies() const override { return {_scan_dependency.get()}; }

    std::vector<int> get_topn_filter_source_node_ids(RuntimeState* state, bool push_down) {
        std::vector<int> result;
        for (int id : _parent->cast<typename Derived::Parent>().topn_filter_source_node_ids) {
            if (!state->get_query_ctx()->has_runtime_predicate(id)) {
                // compatible with older versions fe
                continue;
            }

            const auto& pred = state->get_query_ctx()->get_runtime_predicate(id);
            if (!pred.enable()) {
                continue;
            }
            if (_push_down_topn(pred) == push_down) {
                result.push_back(id);
            }
        }
        return result;
    }

protected:
    template <typename LocalStateType>
    friend class ScanOperatorX;
    friend class vectorized::ScannerContext;
    friend class vectorized::VScanner;

    Status _init_profile() override;
    virtual Status _process_conjuncts(RuntimeState* state) {
        RETURN_IF_ERROR(_normalize_conjuncts(state));
        return Status::OK();
    }
    virtual bool _should_push_down_common_expr() { return false; }

    virtual bool _storage_no_merge() { return false; }
    virtual bool _push_down_topn(const vectorized::RuntimePredicate& predicate) { return false; }
    virtual bool _is_key_column(const std::string& col_name) { return false; }
    virtual PushDownType _should_push_down_bloom_filter() { return PushDownType::UNACCEPTABLE; }
    virtual PushDownType _should_push_down_bitmap_filter() { return PushDownType::UNACCEPTABLE; }
    virtual PushDownType _should_push_down_is_null_predicate() {
        return PushDownType::UNACCEPTABLE;
    }
    virtual Status _should_push_down_binary_predicate(
            vectorized::VectorizedFnCall* fn_call, vectorized::VExprContext* expr_ctx,
            StringRef* constant_val, int* slot_ref_child,
            const std::function<bool(const std::string&)>& fn_checker, PushDownType& pdt);

    virtual PushDownType _should_push_down_in_predicate(vectorized::VInPredicate* in_pred,
                                                        vectorized::VExprContext* expr_ctx,
                                                        bool is_not_in);

    virtual Status _should_push_down_function_filter(vectorized::VectorizedFnCall* fn_call,
                                                     vectorized::VExprContext* expr_ctx,
                                                     StringRef* constant_str,
                                                     doris::FunctionContext** fn_ctx,
                                                     PushDownType& pdt) {
        pdt = PushDownType::UNACCEPTABLE;
        return Status::OK();
    }

    // Create a list of scanners.
    // The number of scanners is related to the implementation of the data source,
    // predicate conditions, and scheduling strategy.
    // So this method needs to be implemented separately by the subclass of ScanNode.
    // Finally, a set of scanners that have been prepared are returned.
    virtual Status _init_scanners(std::list<vectorized::VScannerSPtr>* scanners) {
        return Status::OK();
    }

    Status _normalize_conjuncts(RuntimeState* state);
    Status _normalize_predicate(const vectorized::VExprSPtr& conjunct_expr_root,
                                vectorized::VExprContext* context,
                                vectorized::VExprSPtr& output_expr);
    Status _eval_const_conjuncts(vectorized::VExpr* vexpr, vectorized::VExprContext* expr_ctx,
                                 PushDownType* pdt);

    Status _normalize_bloom_filter(vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx,
                                   SlotDescriptor* slot, PushDownType* pdt);

    Status _normalize_bitmap_filter(vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx,
                                    SlotDescriptor* slot, PushDownType* pdt);

    Status _normalize_function_filters(vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx,
                                       SlotDescriptor* slot, PushDownType* pdt);

    bool _is_predicate_acting_on_slot(
            vectorized::VExpr* expr,
            const std::function<bool(const vectorized::VExprSPtrs&,
                                     std::shared_ptr<vectorized::VSlotRef>&,
                                     vectorized::VExprSPtr&)>& checker,
            SlotDescriptor** slot_desc, ColumnValueRangeType** range);

    template <PrimitiveType T>
    Status _normalize_in_and_eq_predicate(vectorized::VExpr* expr,
                                          vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
                                          ColumnValueRange<T>& range, PushDownType* pdt);
    template <PrimitiveType T>
    Status _normalize_not_in_and_not_eq_predicate(vectorized::VExpr* expr,
                                                  vectorized::VExprContext* expr_ctx,
                                                  SlotDescriptor* slot, ColumnValueRange<T>& range,
                                                  PushDownType* pdt);

    template <PrimitiveType T>
    Status _normalize_noneq_binary_predicate(vectorized::VExpr* expr,
                                             vectorized::VExprContext* expr_ctx,
                                             SlotDescriptor* slot, ColumnValueRange<T>& range,
                                             PushDownType* pdt);

    void _normalize_compound_predicate(
            vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx, PushDownType* pdt,
            bool is_runtimer_filter_predicate,
            const std::function<bool(const vectorized::VExprSPtrs&,
                                     std::shared_ptr<vectorized::VSlotRef>&,
                                     vectorized::VExprSPtr&)>& in_predicate_checker,
            const std::function<bool(const vectorized::VExprSPtrs&,
                                     std::shared_ptr<vectorized::VSlotRef>&,
                                     vectorized::VExprSPtr&)>& eq_predicate_checker);

    template <PrimitiveType T>
    Status _normalize_binary_compound_predicate(vectorized::VExpr* expr,
                                                vectorized::VExprContext* expr_ctx,
                                                SlotDescriptor* slot, ColumnValueRange<T>& range,
                                                PushDownType* pdt);

    template <PrimitiveType T>
    Status _normalize_in_and_not_in_compound_predicate(vectorized::VExpr* expr,
                                                       vectorized::VExprContext* expr_ctx,
                                                       SlotDescriptor* slot,
                                                       ColumnValueRange<T>& range,
                                                       PushDownType* pdt);

    template <PrimitiveType T>
    Status _normalize_match_compound_predicate(vectorized::VExpr* expr,
                                               vectorized::VExprContext* expr_ctx,
                                               SlotDescriptor* slot, ColumnValueRange<T>& range,
                                               PushDownType* pdt);

    template <PrimitiveType T>
    Status _normalize_is_null_predicate(vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx,
                                        SlotDescriptor* slot, ColumnValueRange<T>& range,
                                        PushDownType* pdt);

    template <PrimitiveType T>
    Status _normalize_match_predicate(vectorized::VExpr* expr, vectorized::VExprContext* expr_ctx,
                                      SlotDescriptor* slot, ColumnValueRange<T>& range,
                                      PushDownType* pdt);

    bool _ignore_cast(SlotDescriptor* slot, vectorized::VExpr* expr);

    template <bool IsFixed, PrimitiveType PrimitiveType, typename ChangeFixedValueRangeFunc>
    Status _change_value_range(ColumnValueRange<PrimitiveType>& range, void* value,
                               const ChangeFixedValueRangeFunc& func, const std::string& fn_name,
                               int slot_ref_child = -1);

    Status _prepare_scanners();

    // Submit the scanner to the thread pool and start execution
    Status _start_scanners(const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners);

    // For some conjunct there is chance to elimate cast operator
    // Eg. Variant's sub column could eliminate cast in storage layer if
    // cast dst column type equals storage column type
    void get_cast_types_for_variants();
    void _filter_and_collect_cast_type_for_variant(
            const vectorized::VExpr* expr,
            phmap::flat_hash_map<std::string, std::vector<PrimitiveType>>& colname_to_cast_types);

    Status _get_topn_filters(RuntimeState* state);

    // Every time vconjunct_ctx_ptr is updated, the old ctx will be stored in this vector
    // so that it will be destroyed uniformly at the end of the query.
    vectorized::VExprContextSPtrs _stale_expr_ctxs;
    vectorized::VExprContextSPtrs _common_expr_ctxs_push_down;

    std::shared_ptr<vectorized::ScannerContext> _scanner_ctx = nullptr;

    FilterPredicates _filter_predicates {};

    // Save all function predicates which may be pushed down to data source.
    std::vector<FunctionFilter> _push_down_functions;

    // colname -> cast dst type
    std::map<std::string, PrimitiveType> _cast_types_for_variants;

    // slot id -> ColumnValueRange
    // Parsed from conjuncts
    phmap::flat_hash_map<int, std::pair<SlotDescriptor*, ColumnValueRangeType>>
            _slot_id_to_value_range;
    // column -> ColumnValueRange
    // We use _colname_to_value_range to store a column and its conresponding value ranges.
    std::unordered_map<std::string, ColumnValueRangeType> _colname_to_value_range;

    /**
     * _colname_to_value_range only store the leaf of and in the conjunct expr tree,
     * we use _compound_value_ranges to store conresponding value ranges
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

    std::atomic<bool> _eos = false;

    std::mutex _block_lock;

    std::vector<std::shared_ptr<RuntimeFilterDependency>> _filter_dependencies;

    // ScanLocalState owns the ownership of scanner, scanner context only has its weakptr
    std::list<std::shared_ptr<vectorized::ScannerDelegate>> _scanners;
};

template <typename LocalStateType>
class ScanOperatorX : public OperatorX<LocalStateType> {
public:
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override { return OperatorXBase::prepare(state); }
    Status open(RuntimeState* state) override;
    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;
    Status get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                    bool* eos) override {
        return get_block(state, block, eos);
    }
    [[nodiscard]] bool is_source() const override { return true; }

    [[nodiscard]] virtual bool is_file_scan_operator() const { return false; }

    const std::vector<TRuntimeFilterDesc>& runtime_filter_descs() override {
        return _runtime_filter_descs;
    }

    TPushAggOp::type get_push_down_agg_type() { return _push_down_agg_type; }

    DataDistribution required_data_distribution() const override {
        if (OperatorX<LocalStateType>::ignore_data_distribution()) {
            // `ignore_data_distribution()` returns true means we ignore the distribution.
            return {ExchangeType::NOOP};
        }
        return {ExchangeType::BUCKET_HASH_SHUFFLE};
    }

    int64_t get_push_down_count() const { return _push_down_count; }
    using OperatorX<LocalStateType>::node_id;
    using OperatorX<LocalStateType>::operator_id;
    using OperatorX<LocalStateType>::get_local_state;

protected:
    using LocalState = LocalStateType;
    ScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                  const DescriptorTbl& descs, int parallel_tasks = 0);
    virtual ~ScanOperatorX() = default;
    template <typename Derived>
    friend class ScanLocalState;
    friend class OlapScanLocalState;

    // For load scan node, there should be both input and output tuple descriptor.
    // For query scan node, there is only output_tuple_desc.
    TupleId _input_tuple_id = -1;
    TupleId _output_tuple_id = -1;
    const TupleDescriptor* _input_tuple_desc = nullptr;
    const TupleDescriptor* _output_tuple_desc = nullptr;

    phmap::flat_hash_map<int, SlotDescriptor*> _slot_id_to_slot_desc;
    std::unordered_map<std::string, int> _colname_to_slot_id;

    // These two values are from query_options
    int _max_scan_key_num;
    int _max_pushdown_conditions_per_column;

    // If the query like select * from table limit 10; then the query should run in
    // single scanner to avoid too many scanners which will cause lots of useless read.
    bool _should_run_serial = false;

    // Every time vconjunct_ctx_ptr is updated, the old ctx will be stored in this vector
    // so that it will be destroyed uniformly at the end of the query.
    vectorized::VExprContextSPtrs _stale_expr_ctxs;
    vectorized::VExprContextSPtrs _common_expr_ctxs_push_down;

    // If sort info is set, push limit to each scanner;
    int64_t _limit_per_scanner = -1;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;

    TPushAggOp::type _push_down_agg_type;

    // Record the value of the aggregate function 'count' from doris's be
    int64_t _push_down_count = -1;
    const int _parallel_tasks = 0;

    std::vector<int> topn_filter_source_node_ids;
};

} // namespace doris::pipeline
