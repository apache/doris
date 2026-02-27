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

#include <cstdint>
#include <mutex>
#include <string>

#include "common/status.h"
#include "exprs/function_filter.h"
#include "olap/filter_olap_param.h"
#include "operator.h"
#include "pipeline/dependency.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "runtime_filter/runtime_filter_consumer_helper.h"
#include "vec/exec/scan/scan_node.h"
#include "vec/exec/scan/scanner_context.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class ScannerDelegate;
class OlapScanner;
} // namespace doris::vectorized

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

class ScanLocalStateBase : public PipelineXLocalState<> {
public:
    ScanLocalStateBase(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<>(state, parent), _helper(parent->runtime_filter_descs()) {}
    ~ScanLocalStateBase() override = default;

    [[nodiscard]] virtual bool should_run_serial() const = 0;

    virtual RuntimeProfile* scanner_profile() = 0;

    [[nodiscard]] virtual const TupleDescriptor* input_tuple_desc() const = 0;
    [[nodiscard]] virtual const TupleDescriptor* output_tuple_desc() const = 0;

    virtual int64_t limit_per_scanner() = 0;

    virtual void set_scan_ranges(RuntimeState* state,
                                 const std::vector<TScanRangeParams>& scan_ranges) = 0;
    virtual TPushAggOp::type get_push_down_agg_type() = 0;

    // If scan operator is serial operator(like topn), its real parallelism is 1.
    // Otherwise, its real parallelism is query_parallel_instance_num.
    // query_parallel_instance_num of olap table is usually equal to session var parallel_pipeline_task_num.
    // for file scan operator, its real parallelism will be 1 if it is in batch mode.
    // Related pr:
    // https://github.com/apache/doris/pull/42460
    // https://github.com/apache/doris/pull/44635
    [[nodiscard]] virtual int max_scanners_concurrency(RuntimeState* state) const;
    [[nodiscard]] virtual int min_scanners_concurrency(RuntimeState* state) const;
    [[nodiscard]] virtual vectorized::ScannerScheduler* scan_scheduler(RuntimeState* state) const;

    [[nodiscard]] std::string get_name() { return _parent->get_name(); }

    uint64_t get_condition_cache_digest() const { return _condition_cache_digest; }

    Status update_late_arrival_runtime_filter(RuntimeState* state, int& arrived_rf_num);

    Status clone_conjunct_ctxs(vectorized::VExprContextSPtrs& scanner_conjuncts);

protected:
    friend class vectorized::ScannerContext;
    friend class vectorized::Scanner;

    virtual Status _init_profile() = 0;

    std::atomic<bool> _opened {false};

    DependencySPtr _scan_dependency = nullptr;

    std::shared_ptr<RuntimeProfile> _scanner_profile;
    RuntimeProfile::Counter* _scanner_wait_worker_timer = nullptr;
    // Num of newly created free blocks when running query
    RuntimeProfile::Counter* _newly_create_free_blocks_num = nullptr;
    // Max num of scanner thread
    RuntimeProfile::Counter* _max_scan_concurrency = nullptr;
    RuntimeProfile::Counter* _min_scan_concurrency = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _peak_running_scanner = nullptr;
    // time of get block from scanner
    RuntimeProfile::Counter* _scan_timer = nullptr;
    RuntimeProfile::Counter* _scan_cpu_timer = nullptr;
    // time of filter output block from scanner
    RuntimeProfile::Counter* _filter_timer = nullptr;
    // rows read from the scanner (including those discarded by (pre)filters)
    RuntimeProfile::Counter* _rows_read_counter = nullptr;

    RuntimeProfile::Counter* _num_scanners = nullptr;

    RuntimeProfile::Counter* _wait_for_rf_timer = nullptr;

    RuntimeProfile::Counter* _scan_rows = nullptr;
    RuntimeProfile::Counter* _scan_bytes = nullptr;

    std::mutex _conjuncts_lock;
    RuntimeFilterConsumerHelper _helper;
    // magic number as seed to generate hash value for condition cache
    uint64_t _condition_cache_digest = 0;
};

template <typename LocalStateType>
class ScanOperatorX;
template <typename Derived>
class ScanLocalState : public ScanLocalStateBase {
    ENABLE_FACTORY_CREATOR(ScanLocalState);
    ScanLocalState(RuntimeState* state, OperatorXBase* parent)
            : ScanLocalStateBase(state, parent) {}
    ~ScanLocalState() override = default;

    virtual Status init(RuntimeState* state, LocalStateInfo& info) override;

    virtual Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;
    std::string debug_string(int indentation_level) const final;

    [[nodiscard]] bool should_run_serial() const override;

    RuntimeProfile* scanner_profile() override { return _scanner_profile.get(); }

    [[nodiscard]] const TupleDescriptor* input_tuple_desc() const override;
    [[nodiscard]] const TupleDescriptor* output_tuple_desc() const override;

    int64_t limit_per_scanner() override;

    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override {}

    TPushAggOp::type get_push_down_agg_type() override;

    std::vector<Dependency*> execution_dependencies() override {
        if (_filter_dependencies.empty()) {
            return {};
        }
        std::vector<Dependency*> res(_filter_dependencies.size());
        std::transform(_filter_dependencies.begin(), _filter_dependencies.end(), res.begin(),
                       [](DependencySPtr dep) { return dep.get(); });
        return res;
    }

    std::vector<Dependency*> dependencies() const override { return {_scan_dependency.get()}; }

    std::vector<int> get_topn_filter_source_node_ids(RuntimeState* state, bool push_down) {
        std::vector<int> result;
        for (int id : _parent->cast<typename Derived::Parent>()._topn_filter_source_node_ids) {
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
    friend class vectorized::Scanner;

    Status _init_profile() override;
    virtual Status _process_conjuncts(RuntimeState* state) { return _normalize_conjuncts(state); }
    virtual bool _should_push_down_common_expr() { return false; }

    virtual bool _storage_no_merge() { return false; }
    virtual bool _push_down_topn(const vectorized::RuntimePredicate& predicate) { return false; }
    virtual bool _is_key_column(const std::string& col_name) { return false; }
    virtual PushDownType _should_push_down_bloom_filter() const {
        return PushDownType::UNACCEPTABLE;
    }
    virtual PushDownType _should_push_down_topn_filter() const {
        return PushDownType::UNACCEPTABLE;
    }
    virtual PushDownType _should_push_down_bitmap_filter() const {
        return PushDownType::UNACCEPTABLE;
    }
    virtual PushDownType _should_push_down_is_null_predicate(
            vectorized::VectorizedFnCall* fn_call) const {
        return PushDownType::UNACCEPTABLE;
    }
    virtual PushDownType _should_push_down_in_predicate() const {
        return PushDownType::UNACCEPTABLE;
    }
    virtual PushDownType _should_push_down_binary_predicate(
            vectorized::VectorizedFnCall* fn_call, vectorized::VExprContext* expr_ctx,
            vectorized::Field& constant_val, const std::set<std::string> fn_name) const {
        return PushDownType::UNACCEPTABLE;
    }

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
    virtual Status _init_scanners(std::list<vectorized::ScannerSPtr>* scanners) {
        return Status::OK();
    }

    Status _normalize_conjuncts(RuntimeState* state);
    // Normalize a conjunct and try to convert it to column predicate recursively.
    Status _normalize_predicate(vectorized::VExprContext* context,
                                const vectorized::VExprSPtr& root,
                                vectorized::VExprSPtr& output_expr);
    bool _is_predicate_acting_on_slot(const vectorized::VExprSPtrs& children,
                                      SlotDescriptor** slot_desc, ColumnValueRangeType** range);
    Status _eval_const_conjuncts(vectorized::VExprContext* expr_ctx, PushDownType* pdt);

    template <PrimitiveType T>
    Status _normalize_in_predicate(vectorized::VExprContext* expr_ctx,
                                   const vectorized::VExprSPtr& root, SlotDescriptor* slot,
                                   std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                   ColumnValueRange<T>& range, PushDownType* pdt);
    template <PrimitiveType T>
    Status _normalize_binary_predicate(vectorized::VExprContext* expr_ctx,
                                       const vectorized::VExprSPtr& root, SlotDescriptor* slot,
                                       std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                       ColumnValueRange<T>& range, PushDownType* pdt);
    Status _normalize_bloom_filter(vectorized::VExprContext* expr_ctx,
                                   const vectorized::VExprSPtr& root, SlotDescriptor* slot,
                                   std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                   PushDownType* pdt);
    Status _normalize_topn_filter(vectorized::VExprContext* expr_ctx,
                                  const vectorized::VExprSPtr& root, SlotDescriptor* slot,
                                  std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                  PushDownType* pdt);

    Status _normalize_bitmap_filter(vectorized::VExprContext* expr_ctx,
                                    const vectorized::VExprSPtr& root, SlotDescriptor* slot,
                                    std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                    PushDownType* pdt);

    Status _normalize_function_filters(vectorized::VExprContext* expr_ctx, SlotDescriptor* slot,
                                       PushDownType* pdt);

    template <PrimitiveType T>
    Status _normalize_is_null_predicate(vectorized::VExprContext* expr_ctx,
                                        const vectorized::VExprSPtr& root, SlotDescriptor* slot,
                                        std::vector<std::shared_ptr<ColumnPredicate>>& predicates,
                                        ColumnValueRange<T>& range, PushDownType* pdt);

    template <PrimitiveType PrimitiveType, typename ChangeFixedValueRangeFunc>
    Status _change_value_range(bool is_equal_op, ColumnValueRange<PrimitiveType>& range,
                               const vectorized::Field& value,
                               const ChangeFixedValueRangeFunc& func, const std::string& fn_name);

    Status _prepare_scanners();

    // Submit the scanner to the thread pool and start execution
    Status _start_scanners(const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners);

    // For some conjunct there is chance to elimate cast operator
    // Eg. Variant's sub column could eliminate cast in storage layer if
    // cast dst column type equals storage column type
    void get_cast_types_for_variants();
    void _filter_and_collect_cast_type_for_variant(
            const vectorized::VExpr* expr,
            std::unordered_map<std::string, std::vector<vectorized::DataTypePtr>>&
                    colname_to_cast_types);

    Status _get_topn_filters(RuntimeState* state);

    // Stores conjuncts that have been fully pushed down to the storage layer as predicate columns.
    // These expr contexts are kept alive to prevent their FunctionContext and constant strings
    // from being freed prematurely.
    vectorized::VExprContextSPtrs _stale_expr_ctxs;
    vectorized::VExprContextSPtrs _common_expr_ctxs_push_down;

    atomic_shared_ptr<vectorized::ScannerContext> _scanner_ctx;

    // Save all function predicates which may be pushed down to data source.
    std::vector<FunctionFilter> _push_down_functions;

    // colname -> cast dst type
    std::map<std::string, vectorized::DataTypePtr> _cast_types_for_variants;

    // slot id -> ColumnValueRange
    // Parsed from conjuncts
    phmap::flat_hash_map<int, ColumnValueRangeType> _slot_id_to_value_range;
    phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>> _slot_id_to_predicates;
    std::vector<std::shared_ptr<MutilColumnBlockPredicate>> _or_predicates;

    std::atomic<bool> _eos = false;

    std::vector<std::shared_ptr<Dependency>> _filter_dependencies;

    // ScanLocalState owns the ownership of scanner, scanner context only has its weakptr
    std::list<std::shared_ptr<vectorized::ScannerDelegate>> _scanners;
    vectorized::Arena _arena;
};

template <typename LocalStateType>
class ScanOperatorX : public OperatorX<LocalStateType> {
public:
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;
    Status get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                    bool* eos) override {
        Status status = get_block(state, block, eos);
        if (status.ok()) {
            if (auto rows = block->rows()) {
                auto* local_state = state->get_local_state(operator_id());
                COUNTER_UPDATE(local_state->_rows_returned_counter, rows);
                COUNTER_UPDATE(local_state->_blocks_returned_counter, 1);
            }
        }
        return status;
    }
    [[nodiscard]] bool is_source() const override { return true; }

    [[nodiscard]] size_t get_reserve_mem_size(RuntimeState* state) override;

    const std::vector<TRuntimeFilterDesc>& runtime_filter_descs() override {
        return _runtime_filter_descs;
    }

    [[nodiscard]] virtual int get_column_id(const std::string& col_name) const { return -1; }

    TPushAggOp::type get_push_down_agg_type() { return _push_down_agg_type; }

    DataDistribution required_data_distribution(RuntimeState* /*state*/) const override {
        if (OperatorX<LocalStateType>::is_serial_operator()) {
            // `is_serial_operator()` returns true means we ignore the distribution.
            return {ExchangeType::NOOP};
        }
        return {ExchangeType::BUCKET_HASH_SHUFFLE};
    }

    void set_low_memory_mode(RuntimeState* state) override {
        auto& local_state = get_local_state(state);

        if (auto ctx = local_state._scanner_ctx.load()) {
            ctx->clear_free_blocks();
        }
    }

    using OperatorX<LocalStateType>::node_id;
    using OperatorX<LocalStateType>::operator_id;
    using OperatorX<LocalStateType>::get_local_state;

#ifdef BE_TEST
    ScanOperatorX() = default;
#endif

protected:
    using LocalState = LocalStateType;
    friend class vectorized::OlapScanner;
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
    int _max_scan_key_num = 48;
    int _max_pushdown_conditions_per_column = 1024;

    // If the query like select * from table limit 10; then the query should run in
    // single scanner to avoid too many scanners which will cause lots of useless read.
    bool _should_run_serial = false;

    vectorized::VExprContextSPtrs _common_expr_ctxs_push_down;

    // If sort info is set, push limit to each scanner;
    int64_t _limit_per_scanner = -1;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;

    TPushAggOp::type _push_down_agg_type;

    // Record the value of the aggregate function 'count' from doris's be
    int64_t _push_down_count = -1;
    const int _parallel_tasks = 0;

    std::vector<int> _topn_filter_source_node_ids;
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline
