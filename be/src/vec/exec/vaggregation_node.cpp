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

#include "vec/exec/vaggregation_node.h"

#include <memory>

#include "exec/exec_node.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

// Here is an empirical value.
static constexpr size_t HASH_MAP_PREFETCH_DIST = 16;

/// The minimum reduction factor (input rows divided by output rows) to grow hash tables
/// in a streaming preaggregation, given that the hash tables are currently the given
/// size or above. The sizes roughly correspond to hash table sizes where the bucket
/// arrays will fit in  a cache level. Intuitively, we don't want the working set of the
/// aggregation to expand to the next level of cache unless we're reducing the input
/// enough to outweigh the increased memory latency we'll incur for each hash table
/// lookup.
///
/// Note that the current reduction achieved is not always a good estimate of the
/// final reduction. It may be biased either way depending on the ordering of the
/// input. If the input order is random, we will underestimate the final reduction
/// factor because the probability of a row having the same key as a previous row
/// increases as more input is processed.  If the input order is correlated with the
/// key, skew may bias the estimate. If high cardinality keys appear first, we
/// may overestimate and if low cardinality keys appear first, we underestimate.
/// To estimate the eventual reduction achieved, we estimate the final reduction
/// using the planner's estimated input cardinality and the assumption that input
/// is in a random order. This means that we assume that the reduction factor will
/// increase over time.
struct StreamingHtMinReductionEntry {
    // Use 'streaming_ht_min_reduction' if the total size of hash table bucket directories in
    // bytes is greater than this threshold.
    int min_ht_mem;
    // The minimum reduction factor to expand the hash tables.
    double streaming_ht_min_reduction;
};

// TODO: experimentally tune these values and also programmatically get the cache size
// of the machine that we're running on.
static constexpr StreamingHtMinReductionEntry STREAMING_HT_MIN_REDUCTION[] = {
        // Expand up to L2 cache always.
        {0, 0.0},
        // Expand into L3 cache if we look like we're getting some reduction.
        // At present, The L2 cache is generally 1024k or more
        {1024 * 1024, 1.1},
        // Expand into main memory if we're getting a significant reduction.
        // The L3 cache is generally 16MB or more
        {16 * 1024 * 1024, 2.0},
};

static constexpr int STREAMING_HT_MIN_REDUCTION_SIZE =
        sizeof(STREAMING_HT_MIN_REDUCTION) / sizeof(STREAMING_HT_MIN_REDUCTION[0]);

AggregationNode::AggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _intermediate_tuple_desc(NULL),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _output_tuple_desc(NULL),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_merge(false),
          _agg_data(),
          _build_timer(nullptr),
          _serialize_key_timer(nullptr),
          _exec_timer(nullptr),
          _merge_timer(nullptr),
          _expr_timer(nullptr),
          _get_results_timer(nullptr),
          _serialize_data_timer(nullptr),
          _serialize_result_timer(nullptr),
          _deserialize_data_timer(nullptr),
          _hash_table_compute_timer(nullptr),
          _hash_table_iterate_timer(nullptr),
          _insert_keys_to_column_timer(nullptr),
          _streaming_agg_timer(nullptr),
          _hash_table_size_counter(nullptr),
          _hash_table_input_counter(nullptr) {
    if (tnode.agg_node.__isset.use_streaming_preaggregation) {
        _is_streaming_preagg = tnode.agg_node.use_streaming_preaggregation;
        if (_is_streaming_preagg) {
            DCHECK(!tnode.agg_node.grouping_exprs.empty()) << "Streaming preaggs do grouping";
            DCHECK(_limit == -1) << "Preaggs have no limits";
        }
    } else {
        _is_streaming_preagg = false;
    }

    _is_first_phase = tnode.agg_node.__isset.is_first_phase && tnode.agg_node.is_first_phase;
    _use_fixed_length_serialization_opt =
            tnode.agg_node.__isset.use_fixed_length_serialization_opt &&
            tnode.agg_node.use_fixed_length_serialization_opt;
}

AggregationNode::~AggregationNode() = default;

Status AggregationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(
            VExpr::create_expr_trees(_pool, tnode.agg_node.grouping_exprs, &_probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(AggFnEvaluator::create(_pool, tnode.agg_node.aggregate_functions[i],
                                               tnode.agg_node.agg_sort_infos[i], &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    const auto& agg_functions = tnode.agg_node.aggregate_functions;
    _is_merge = std::any_of(agg_functions.cbegin(), agg_functions.cend(),
                            [](const auto& e) { return e.nodes[0].agg_expr.is_merge_agg; });
    return Status::OK();
}

void AggregationNode::_init_hash_method(std::vector<VExprContext*>& probe_exprs) {
    DCHECK(probe_exprs.size() >= 1);
    if (probe_exprs.size() == 1) {
        auto is_nullable = probe_exprs[0]->root()->is_nullable();
        switch (probe_exprs[0]->root()->result_type()) {
        case TYPE_TINYINT:
        case TYPE_BOOLEAN:
            _agg_data.init(AggregatedDataVariants::Type::int8_key, is_nullable);
            return;
        case TYPE_SMALLINT:
            _agg_data.init(AggregatedDataVariants::Type::int16_key, is_nullable);
            return;
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DATEV2:
            if (_is_first_phase)
                _agg_data.init(AggregatedDataVariants::Type::int32_key, is_nullable);
            else
                _agg_data.init(AggregatedDataVariants::Type::int32_key_phase2, is_nullable);
            return;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_DATETIMEV2:
            if (_is_first_phase)
                _agg_data.init(AggregatedDataVariants::Type::int64_key, is_nullable);
            else
                _agg_data.init(AggregatedDataVariants::Type::int64_key_phase2, is_nullable);
            return;
        case TYPE_LARGEINT: {
            if (_is_first_phase)
                _agg_data.init(AggregatedDataVariants::Type::int128_key, is_nullable);
            else
                _agg_data.init(AggregatedDataVariants::Type::int128_key_phase2, is_nullable);
            return;
        }
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128: {
            DataTypePtr& type_ptr = probe_exprs[0]->root()->data_type();
            TypeIndex idx = is_nullable ? assert_cast<const DataTypeNullable&>(*type_ptr)
                                                  .get_nested_type()
                                                  ->get_type_id()
                                        : type_ptr->get_type_id();
            WhichDataType which(idx);
            if (which.is_decimal32()) {
                if (_is_first_phase)
                    _agg_data.init(AggregatedDataVariants::Type::int32_key, is_nullable);
                else
                    _agg_data.init(AggregatedDataVariants::Type::int32_key_phase2, is_nullable);
            } else if (which.is_decimal64()) {
                if (_is_first_phase)
                    _agg_data.init(AggregatedDataVariants::Type::int64_key, is_nullable);
                else
                    _agg_data.init(AggregatedDataVariants::Type::int64_key_phase2, is_nullable);
            } else {
                if (_is_first_phase)
                    _agg_data.init(AggregatedDataVariants::Type::int128_key, is_nullable);
                else
                    _agg_data.init(AggregatedDataVariants::Type::int128_key_phase2, is_nullable);
            }
            return;
        }
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            _agg_data.init(AggregatedDataVariants::Type::string_key, is_nullable);
            break;
        }
        default:
            _agg_data.init(AggregatedDataVariants::Type::serialized);
        }
    } else {
        bool use_fixed_key = true;
        bool has_null = false;
        int key_byte_size = 0;

        _probe_key_sz.resize(_probe_expr_ctxs.size());
        for (int i = 0; i < _probe_expr_ctxs.size(); ++i) {
            const auto vexpr = _probe_expr_ctxs[i]->root();
            const auto& data_type = vexpr->data_type();

            if (!data_type->have_maximum_size_of_value()) {
                use_fixed_key = false;
                break;
            }

            auto is_null = data_type->is_nullable();
            has_null |= is_null;
            _probe_key_sz[i] = data_type->get_maximum_size_of_value_in_memory() - (is_null ? 1 : 0);
            key_byte_size += _probe_key_sz[i];
        }

        if (std::tuple_size<KeysNullMap<UInt256>>::value + key_byte_size > sizeof(UInt256)) {
            use_fixed_key = false;
        }

        if (use_fixed_key) {
            if (has_null) {
                if (std::tuple_size<KeysNullMap<UInt64>>::value + key_byte_size <= sizeof(UInt64)) {
                    if (_is_first_phase)
                        _agg_data.init(AggregatedDataVariants::Type::int64_keys, has_null);
                    else
                        _agg_data.init(AggregatedDataVariants::Type::int64_keys_phase2, has_null);
                } else if (std::tuple_size<KeysNullMap<UInt128>>::value + key_byte_size <=
                           sizeof(UInt128)) {
                    if (_is_first_phase)
                        _agg_data.init(AggregatedDataVariants::Type::int128_keys, has_null);
                    else
                        _agg_data.init(AggregatedDataVariants::Type::int128_keys_phase2, has_null);
                } else {
                    if (_is_first_phase)
                        _agg_data.init(AggregatedDataVariants::Type::int256_keys, has_null);
                    else
                        _agg_data.init(AggregatedDataVariants::Type::int256_keys_phase2, has_null);
                }
            } else {
                if (key_byte_size <= sizeof(UInt64)) {
                    if (_is_first_phase)
                        _agg_data.init(AggregatedDataVariants::Type::int64_keys, has_null);
                    else
                        _agg_data.init(AggregatedDataVariants::Type::int64_keys_phase2, has_null);
                } else if (key_byte_size <= sizeof(UInt128)) {
                    if (_is_first_phase)
                        _agg_data.init(AggregatedDataVariants::Type::int128_keys, has_null);
                    else
                        _agg_data.init(AggregatedDataVariants::Type::int128_keys_phase2, has_null);
                } else {
                    if (_is_merge)
                        _agg_data.init(AggregatedDataVariants::Type::int256_keys, has_null);
                    else
                        _agg_data.init(AggregatedDataVariants::Type::int256_keys_phase2, has_null);
                }
            }
        } else {
            _agg_data.init(AggregatedDataVariants::Type::serialized);
        }
    }
} // namespace doris::vectorized

Status AggregationNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _serialize_key_timer = ADD_TIMER(runtime_profile(), "SerializeKeyTime");
    _exec_timer = ADD_TIMER(runtime_profile(), "ExecTime");
    _merge_timer = ADD_TIMER(runtime_profile(), "MergeTime");
    _expr_timer = ADD_TIMER(runtime_profile(), "ExprTime");
    _get_results_timer = ADD_TIMER(runtime_profile(), "GetResultsTime");
    _serialize_data_timer = ADD_TIMER(runtime_profile(), "SerializeDataTime");
    _serialize_result_timer = ADD_TIMER(runtime_profile(), "SerializeResultTime");
    _deserialize_data_timer = ADD_TIMER(runtime_profile(), "DeserializeDataTime");
    _hash_table_compute_timer = ADD_TIMER(runtime_profile(), "HashTableComputeTime");
    _hash_table_iterate_timer = ADD_TIMER(runtime_profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(runtime_profile(), "InsertKeysToColumnTime");
    _streaming_agg_timer = ADD_TIMER(runtime_profile(), "StreamingAggTime");
    _hash_table_size_counter = ADD_COUNTER(runtime_profile(), "HashTableSize", TUnit::UNIT);
    _hash_table_input_counter = ADD_COUNTER(runtime_profile(), "HashTableInputCount", TUnit::UNIT);

    _data_mem_tracker = std::make_unique<MemTracker>("AggregationNode:Data");
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc()));

    _mem_pool = std::make_unique<MemPool>();

    int j = _probe_expr_ctxs.size();
    for (int i = 0; i < j; ++i) {
        auto nullable_output = _output_tuple_desc->slots()[i]->is_nullable();
        auto nullable_input = _probe_expr_ctxs[i]->root()->is_nullable();
        if (nullable_output != nullable_input) {
            DCHECK(nullable_output);
            _make_nullable_keys.emplace_back(i);
        }
    }
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[j];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[j];
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(state, child(0)->row_desc(),
                                                          _mem_pool.get(), intermediate_slot_desc,
                                                          output_slot_desc));
    }

    // set profile timer to evaluators
    for (auto& evaluator : _aggregate_evaluators) {
        evaluator->set_timer(_exec_timer, _merge_timer, _expr_timer);
    }

    _offsets_of_aggregate_states.resize(_aggregate_evaluators.size());

    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
        _offsets_of_aggregate_states[i] = _total_size_of_aggregate_states;

        const auto& agg_function = _aggregate_evaluators[i]->function();
        // aggreate states are aligned based on maximum requirement
        _align_aggregate_states = std::max(_align_aggregate_states, agg_function->align_of_data());
        _total_size_of_aggregate_states += agg_function->size_of_data();

        // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
        if (i + 1 < _aggregate_evaluators.size()) {
            size_t alignment_of_next_state =
                    _aggregate_evaluators[i + 1]->function()->align_of_data();
            if ((alignment_of_next_state & (alignment_of_next_state - 1)) != 0) {
                return Status::RuntimeError("Logical error: align_of_data is not 2^N");
            }

            /// Extend total_size to next alignment requirement
            /// Add padding by rounding up 'total_size_of_aggregate_states' to be a multiplier of alignment_of_next_state.
            _total_size_of_aggregate_states =
                    (_total_size_of_aggregate_states + alignment_of_next_state - 1) /
                    alignment_of_next_state * alignment_of_next_state;
        }
    }

    if (_probe_expr_ctxs.empty()) {
        _agg_data.init(AggregatedDataVariants::Type::without_key);

        _agg_data.without_key = reinterpret_cast<AggregateDataPtr>(
                _mem_pool->allocate(_total_size_of_aggregate_states));

        if (_is_merge) {
            _executor.execute = std::bind<Status>(&AggregationNode::_merge_without_key, this,
                                                  std::placeholders::_1);
        } else {
            _executor.execute = std::bind<Status>(&AggregationNode::_execute_without_key, this,
                                                  std::placeholders::_1);
        }

        if (_needs_finalize) {
            _executor.get_result = std::bind<Status>(&AggregationNode::_get_without_key_result,
                                                     this, std::placeholders::_1,
                                                     std::placeholders::_2, std::placeholders::_3);
        } else {
            _executor.get_result = std::bind<Status>(&AggregationNode::_serialize_without_key, this,
                                                     std::placeholders::_1, std::placeholders::_2,
                                                     std::placeholders::_3);
        }

        _executor.update_memusage =
                std::bind<void>(&AggregationNode::_update_memusage_without_key, this);
        _executor.close = std::bind<void>(&AggregationNode::_close_without_key, this);
    } else {
        _init_hash_method(_probe_expr_ctxs);

        std::visit(
                [&](auto&& agg_method) {
                    using HashTableType = std::decay_t<decltype(agg_method.data)>;
                    using KeyType = typename HashTableType::key_type;

                    /// some aggregate functions (like AVG for decimal) have align issues.
                    _aggregate_data_container.reset(new AggregateDataContainer(
                            sizeof(KeyType),
                            ((_total_size_of_aggregate_states + _align_aggregate_states - 1) /
                             _align_aggregate_states) *
                                    _align_aggregate_states));
                },
                _agg_data._aggregated_method_variant);
        if (_is_merge) {
            _executor.execute = std::bind<Status>(&AggregationNode::_merge_with_serialized_key,
                                                  this, std::placeholders::_1);
        } else {
            _executor.execute = std::bind<Status>(&AggregationNode::_execute_with_serialized_key,
                                                  this, std::placeholders::_1);
        }

        if (_is_streaming_preagg) {
            runtime_profile()->append_exec_option("Streaming Preaggregation");
            _executor.pre_agg =
                    std::bind<Status>(&AggregationNode::_pre_agg_with_serialized_key, this,
                                      std::placeholders::_1, std::placeholders::_2);
        }

        if (_needs_finalize) {
            _executor.get_result = std::bind<Status>(
                    &AggregationNode::_get_with_serialized_key_result, this, std::placeholders::_1,
                    std::placeholders::_2, std::placeholders::_3);
        } else {
            _executor.get_result = std::bind<Status>(
                    &AggregationNode::_serialize_with_serialized_key_result, this,
                    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
        }
        _executor.update_memusage =
                std::bind<void>(&AggregationNode::_update_memusage_with_serialized_key, this);
        _executor.close = std::bind<void>(&AggregationNode::_close_with_serialized_key, this);

        _should_limit_output = _limit != -1 &&        // has limit
                               !_vconjunct_ctx_ptr && // no having conjunct
                               _needs_finalize;       // agg's finalize step
    }

    return Status::OK();
}

Status AggregationNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "AggregationNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state));
    }

    RETURN_IF_ERROR(_children[0]->open(state));

    // Streaming preaggregations do all processing in GetNext().
    if (_is_streaming_preagg) return Status::OK();
    // move _create_agg_status to open not in during prepare,
    // because during prepare and open thread is not the same one,
    // this could cause unable to get JVM
    if (_probe_expr_ctxs.empty()) {
        _create_agg_status(_agg_data.without_key);
        _agg_data_created_without_key = true;
    }
    bool eos = false;
    Block block;
    while (!eos) {
        RETURN_IF_CANCELLED(state);
        release_block_memory(block);
        RETURN_IF_ERROR_AND_CHECK_SPAN(_children[0]->get_next_after_projects(state, &block, &eos),
                                       _children[0]->get_next_span(), eos);
        if (block.rows() == 0) {
            continue;
        }
        RETURN_IF_ERROR(_executor.execute(&block));
        _executor.update_memusage();
    }

    return Status::OK();
}

Status AggregationNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented Aggregation Node::get_next scalar");
}

Status AggregationNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "AggregationNode::get_next");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    if (_is_streaming_preagg) {
        bool child_eos = false;

        RETURN_IF_CANCELLED(state);
        do {
            release_block_memory(_preagg_block);
            RETURN_IF_ERROR_AND_CHECK_SPAN(
                    _children[0]->get_next_after_projects(state, &_preagg_block, &child_eos),
                    _children[0]->get_next_span(), child_eos);
        } while (_preagg_block.rows() == 0 && !child_eos);

        if (_preagg_block.rows() != 0) {
            RETURN_IF_ERROR(_executor.pre_agg(&_preagg_block, block));
        } else {
            RETURN_IF_ERROR(_executor.get_result(state, block, eos));
        }
        // pre stream agg need use _num_row_return to decide whether to do pre stream agg
        _num_rows_returned += block->rows();
        _make_nullable_output_key(block);
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    } else {
        RETURN_IF_ERROR(_executor.get_result(state, block, eos));
        _make_nullable_output_key(block);
        // dispose the having clause, should not be execute in prestreaming agg
        RETURN_IF_ERROR(VExprContext::filter_block(_vconjunct_ctx_ptr, block, block->columns()));
        reached_limit(block, eos);
    }

    _executor.update_memusage();
    return Status::OK();
}

Status AggregationNode::close(RuntimeState* state) {
    if (is_closed()) return Status::OK();
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "AggregationNode::close");

    for (auto* aggregate_evaluator : _aggregate_evaluators) aggregate_evaluator->close(state);
    VExpr::close(_probe_expr_ctxs, state);
    if (_executor.close) _executor.close();

    /// _hash_table_size_counter may be null if prepare failed.
    if (_hash_table_size_counter) {
        std::visit(
                [&](auto&& agg_method) {
                    COUNTER_SET(_hash_table_size_counter, int64_t(agg_method.data.size()));
                },
                _agg_data._aggregated_method_variant);
    }

    return ExecNode::close(state);
}

Status AggregationNode::_create_agg_status(AggregateDataPtr data) {
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->create(data + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggregationNode::_destroy_agg_status(AggregateDataPtr data) {
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->function()->destroy(data + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggregationNode::_get_without_key_result(RuntimeState* state, Block* block, bool* eos) {
    DCHECK(_agg_data.without_key != nullptr);
    block->clear();

    *block = VectorizedUtils::create_empty_columnswithtypename(_row_descriptor);
    int agg_size = _aggregate_evaluators.size();

    MutableColumns columns(agg_size);
    std::vector<DataTypePtr> data_types(agg_size);
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        data_types[i] = _aggregate_evaluators[i]->function()->get_return_type();
        columns[i] = data_types[i]->create_column();
    }

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        auto column = columns[i].get();
        _aggregate_evaluators[i]->insert_result_info(
                _agg_data.without_key + _offsets_of_aggregate_states[i], column);
    }

    const auto& block_schema = block->get_columns_with_type_and_name();
    DCHECK_EQ(block_schema.size(), columns.size());
    for (int i = 0; i < block_schema.size(); ++i) {
        const auto column_type = block_schema[i].type;
        if (!column_type->equals(*data_types[i])) {
            if (!is_array(remove_nullable(column_type))) {
                DCHECK(column_type->is_nullable());
                DCHECK(!data_types[i]->is_nullable());
                DCHECK(remove_nullable(column_type)->equals(*data_types[i]));
            }

            ColumnPtr ptr = std::move(columns[i]);
            // unless `count`, other aggregate function dispose empty set should be null
            // so here check the children row return
            ptr = make_nullable(ptr, _children[0]->rows_returned() == 0);
            columns[i] = std::move(*ptr).mutate();
        }
    }

    block->set_columns(std::move(columns));
    *eos = true;
    return Status::OK();
}

Status AggregationNode::_serialize_without_key(RuntimeState* state, Block* block, bool* eos) {
    // 1. `child(0)->rows_returned() == 0` mean not data from child
    // in level two aggregation node should return NULL result
    //    level one aggregation node set `eos = true` return directly
    SCOPED_TIMER(_serialize_result_timer);
    if (UNLIKELY(_children[0]->rows_returned() == 0)) {
        *eos = true;
        return Status::OK();
    }
    block->clear();

    DCHECK(_agg_data.without_key != nullptr);
    int agg_size = _aggregate_evaluators.size();

    MutableColumns value_columns(agg_size);
    std::vector<DataTypePtr> data_types(agg_size);
    // will serialize data to string column
    if (_use_fixed_length_serialization_opt) {
        auto serialize_string_type = std::make_shared<DataTypeString>();
        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            data_types[i] = _aggregate_evaluators[i]->function()->get_serialized_type();
            value_columns[i] = _aggregate_evaluators[i]->function()->create_serialize_column();
        }

        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            _aggregate_evaluators[i]->function()->serialize_without_key_to_column(
                    _agg_data.without_key + _offsets_of_aggregate_states[i], value_columns[i]);
        }
    } else {
        std::vector<VectorBufferWriter> value_buffer_writers;
        auto serialize_string_type = std::make_shared<DataTypeString>();
        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            data_types[i] = serialize_string_type;
            value_columns[i] = serialize_string_type->create_column();
            value_buffer_writers.emplace_back(
                    *reinterpret_cast<ColumnString*>(value_columns[i].get()));
        }

        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            _aggregate_evaluators[i]->function()->serialize(
                    _agg_data.without_key + _offsets_of_aggregate_states[i],
                    value_buffer_writers[i]);
            value_buffer_writers[i].commit();
        }
    }
    {
        ColumnsWithTypeAndName data_with_schema;
        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            ColumnWithTypeAndName column_with_schema = {nullptr, data_types[i], ""};
            data_with_schema.push_back(std::move(column_with_schema));
        }
        *block = Block(data_with_schema);
    }

    block->set_columns(std::move(value_columns));
    *eos = true;
    return Status::OK();
}

Status AggregationNode::_execute_without_key(Block* block) {
    DCHECK(_agg_data.without_key != nullptr);
    SCOPED_TIMER(_build_timer);
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->execute_single_add(
                block, _agg_data.without_key + _offsets_of_aggregate_states[i], &_agg_arena_pool);
    }
    return Status::OK();
}

Status AggregationNode::_merge_without_key(Block* block) {
    SCOPED_TIMER(_merge_timer);
    DCHECK(_agg_data.without_key != nullptr);
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        if (_aggregate_evaluators[i]->is_merge()) {
            int col_id = _get_slot_column_id(_aggregate_evaluators[i]);
            auto column = block->get_by_position(col_id).column;
            if (column->is_nullable()) {
                column = ((ColumnNullable*)column.get())->get_nested_column_ptr();
            }

            SCOPED_TIMER(_deserialize_data_timer);
            if (_use_fixed_length_serialization_opt) {
                _aggregate_evaluators[i]->function()->deserialize_and_merge_from_column(
                        _agg_data.without_key + _offsets_of_aggregate_states[i], *column,
                        &_agg_arena_pool);
            } else {
                const int rows = block->rows();
                for (int j = 0; j < rows; ++j) {
                    VectorBufferReader buffer_reader(
                            ((ColumnString*)(column.get()))->get_data_at(j));

                    _aggregate_evaluators[i]->function()->deserialize_and_merge(
                            _agg_data.without_key + _offsets_of_aggregate_states[i], buffer_reader,
                            &_agg_arena_pool);
                }
            }
        } else {
            _aggregate_evaluators[i]->execute_single_add(
                    block, _agg_data.without_key + _offsets_of_aggregate_states[i],
                    &_agg_arena_pool);
        }
    }
    return Status::OK();
}

void AggregationNode::_update_memusage_without_key() {
    _data_mem_tracker->consume(_agg_arena_pool.size() - _mem_usage_record.used_in_arena);
    _mem_usage_record.used_in_arena = _agg_arena_pool.size();
}

void AggregationNode::_close_without_key() {
    //because prepare maybe failed, and couldn't create agg data.
    //but finally call close to destory agg data, if agg data has bitmapValue
    //will be core dump, it's not initialized
    if (_agg_data_created_without_key) {
        _destroy_agg_status(_agg_data.without_key);
        _agg_data_created_without_key = false;
    }
    release_tracker();
}

void AggregationNode::_make_nullable_output_key(Block* block) {
    if (block->rows() != 0) {
        for (auto cid : _make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

bool AggregationNode::_should_expand_preagg_hash_tables() {
    if (!_should_expand_hash_table) return false;

    return std::visit(
            [&](auto&& agg_method) -> bool {
                auto& hash_tbl = agg_method.data;
                auto [ht_mem, ht_rows] =
                        std::pair {hash_tbl.get_buffer_size_in_bytes(), hash_tbl.size()};

                // Need some rows in tables to have valid statistics.
                if (ht_rows == 0) return true;

                // Find the appropriate reduction factor in our table for the current hash table sizes.
                int cache_level = 0;
                while (cache_level + 1 < STREAMING_HT_MIN_REDUCTION_SIZE &&
                       ht_mem >= STREAMING_HT_MIN_REDUCTION[cache_level + 1].min_ht_mem) {
                    ++cache_level;
                }

                // Compare the number of rows in the hash table with the number of input rows that
                // were aggregated into it. Exclude passed through rows from this calculation since
                // they were not in hash tables.
                const int64_t input_rows = _children[0]->rows_returned();
                const int64_t aggregated_input_rows = input_rows - _num_rows_returned;
                // TODO chenhao
                //  const int64_t expected_input_rows = estimated_input_cardinality_ - num_rows_returned_;
                double current_reduction = static_cast<double>(aggregated_input_rows) / ht_rows;

                // TODO: workaround for IMPALA-2490: subplan node rows_returned counter may be
                // inaccurate, which could lead to a divide by zero below.
                if (aggregated_input_rows <= 0) return true;

                // Extrapolate the current reduction factor (r) using the formula
                // R = 1 + (N / n) * (r - 1), where R is the reduction factor over the full input data
                // set, N is the number of input rows, excluding passed-through rows, and n is the
                // number of rows inserted or merged into the hash tables. This is a very rough
                // approximation but is good enough to be useful.
                // TODO: consider collecting more statistics to better estimate reduction.
                //  double estimated_reduction = aggregated_input_rows >= expected_input_rows
                //      ? current_reduction
                //      : 1 + (expected_input_rows / aggregated_input_rows) * (current_reduction - 1);
                double min_reduction =
                        STREAMING_HT_MIN_REDUCTION[cache_level].streaming_ht_min_reduction;

                //  COUNTER_SET(preagg_estimated_reduction_, estimated_reduction);
                //    COUNTER_SET(preagg_streaming_ht_min_reduction_, min_reduction);
                //  return estimated_reduction > min_reduction;
                _should_expand_hash_table = current_reduction > min_reduction;
                return _should_expand_hash_table;
            },
            _agg_data._aggregated_method_variant);
}

size_t AggregationNode::_get_hash_table_size() {
    return std::visit([&](auto&& agg_method) { return agg_method.data.size(); },
                      _agg_data._aggregated_method_variant);
}

void AggregationNode::_emplace_into_hash_table(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                                               const size_t num_rows) {
    std::visit(
            [&](auto&& agg_method) -> void {
                SCOPED_TIMER(_hash_table_compute_timer);
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using HashTableType = std::decay_t<decltype(agg_method.data)>;
                using AggState = typename HashMethodType::State;
                AggState state(key_columns, _probe_key_sz, nullptr);

                _pre_serialize_key_if_need(state, agg_method, key_columns, num_rows);

                if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                    if (_hash_values.size() < num_rows) _hash_values.resize(num_rows);
                    if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                                          AggState>::value) {
                        for (size_t i = 0; i < num_rows; ++i) {
                            _hash_values[i] = agg_method.data.hash(agg_method.keys[i]);
                        }
                    } else {
                        for (size_t i = 0; i < num_rows; ++i) {
                            _hash_values[i] =
                                    agg_method.data.hash(state.get_key_holder(i, _agg_arena_pool));
                        }
                    }
                }

                auto creator = [this](const auto& ctor, const auto& key) {
                    using KeyType = std::decay_t<decltype(key)>;
                    if constexpr (HashTableTraits<HashTableType>::is_string_hash_table &&
                                  !std::is_same_v<StringRef, KeyType>) {
                        StringRef string_ref = to_string_ref(key);
                        ArenaKeyHolder key_holder {string_ref, _agg_arena_pool};
                        key_holder_persist_key(key_holder);
                        auto mapped = _aggregate_data_container->append_data(key_holder.key);
                        _create_agg_status(mapped);
                        ctor(key, mapped);
                    } else {
                        auto mapped = _aggregate_data_container->append_data(key);
                        _create_agg_status(mapped);
                        ctor(key, mapped);
                    }
                };

                auto creator_for_null_key = [this](auto& mapped) {
                    mapped = _agg_arena_pool.aligned_alloc(_total_size_of_aggregate_states,
                                                           _align_aggregate_states);
                    _create_agg_status(mapped);
                };

                /// For all rows.
                COUNTER_UPDATE(_hash_table_input_counter, num_rows);
                for (size_t i = 0; i < num_rows; ++i) {
                    AggregateDataPtr mapped = nullptr;
                    if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                        if (LIKELY(i + HASH_MAP_PREFETCH_DIST < num_rows)) {
                            if constexpr (HashTableTraits<HashTableType>::is_parallel_phmap) {
                                agg_method.data.prefetch_by_key(state.get_key_holder(
                                        i + HASH_MAP_PREFETCH_DIST, _agg_arena_pool));
                            } else
                                agg_method.data.prefetch_by_hash(
                                        _hash_values[i + HASH_MAP_PREFETCH_DIST]);
                        }

                        if constexpr (ColumnsHashing::IsSingleNullableColumnMethod<
                                              AggState>::value) {
                            mapped = state.lazy_emplace_key(agg_method.data, _hash_values[i], i,
                                                            _agg_arena_pool, creator,
                                                            creator_for_null_key);
                        } else {
                            mapped = state.lazy_emplace_key(agg_method.data, _hash_values[i], i,
                                                            _agg_arena_pool, creator);
                        }
                    } else {
                        if constexpr (ColumnsHashing::IsSingleNullableColumnMethod<
                                              AggState>::value) {
                            mapped = state.lazy_emplace_key(agg_method.data, i, _agg_arena_pool,
                                                            creator, creator_for_null_key);
                        } else {
                            mapped = state.lazy_emplace_key(agg_method.data, i, _agg_arena_pool,
                                                            creator);
                        }
                    }

                    places[i] = mapped;
                    assert(places[i] != nullptr);
                }
            },
            _agg_data._aggregated_method_variant);
}

void AggregationNode::_find_in_hash_table(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                                          size_t rows) {
    std::visit(
            [&](auto&& agg_method) -> void {
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using HashTableType = std::decay_t<decltype(agg_method.data)>;
                using AggState = typename HashMethodType::State;
                AggState state(key_columns, _probe_key_sz, nullptr);

                _pre_serialize_key_if_need(state, agg_method, key_columns, rows);

                if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                    if (_hash_values.size() < rows) _hash_values.resize(rows);
                    if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                                          AggState>::value) {
                        for (size_t i = 0; i < rows; ++i) {
                            _hash_values[i] = agg_method.data.hash(agg_method.keys[i]);
                        }
                    } else {
                        for (size_t i = 0; i < rows; ++i) {
                            _hash_values[i] =
                                    agg_method.data.hash(state.get_key_holder(i, _agg_arena_pool));
                        }
                    }
                }

                /// For all rows.
                for (size_t i = 0; i < rows; ++i) {
                    auto find_result = [&]() {
                        if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                            if (LIKELY(i + HASH_MAP_PREFETCH_DIST < rows)) {
                                if constexpr (HashTableTraits<HashTableType>::is_parallel_phmap) {
                                    agg_method.data.prefetch_by_key(state.get_key_holder(
                                            i + HASH_MAP_PREFETCH_DIST, _agg_arena_pool));
                                } else
                                    agg_method.data.prefetch_by_hash(
                                            _hash_values[i + HASH_MAP_PREFETCH_DIST]);
                            }

                            return state.find_key(agg_method.data, _hash_values[i], i,
                                                  _agg_arena_pool);
                        } else {
                            return state.find_key(agg_method.data, i, _agg_arena_pool);
                        }
                    }();

                    if (find_result.is_found()) {
                        places[i] = find_result.get_mapped();
                    } else
                        places[i] = nullptr;
                }
            },
            _agg_data._aggregated_method_variant);
}

Status AggregationNode::_pre_agg_with_serialized_key(doris::vectorized::Block* in_block,
                                                     doris::vectorized::Block* out_block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_probe_expr_ctxs.empty());

    size_t key_size = _probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(in_block, &result_column_id));
            in_block->get_by_position(result_column_id).column =
                    in_block->get_by_position(result_column_id)
                            .column->convert_to_full_column_if_const();
            key_columns[i] = in_block->get_by_position(result_column_id).column.get();
        }
    }

    int rows = in_block->rows();
    if (_places.size() < rows) {
        _places.resize(rows);
    }

    // Stop expanding hash tables if we're not reducing the input sufficiently. As our
    // hash tables expand out of each level of cache hierarchy, every hash table lookup
    // will take longer. We also may not be able to expand hash tables because of memory
    // pressure. In either case we should always use the remaining space in the hash table
    // to avoid wasting memory.
    // But for fixed hash map, it never need to expand
    bool ret_flag = false;
    std::visit(
            [&](auto&& agg_method) -> void {
                if (auto& hash_tbl = agg_method.data; hash_tbl.add_elem_size_overflow(rows)) {
                    // do not try to do agg, just init and serialize directly return the out_block
                    if (!_should_expand_preagg_hash_tables()) {
                        SCOPED_TIMER(_streaming_agg_timer);
                        ret_flag = true;

                        // will serialize value data to string column
                        bool mem_reuse = out_block->mem_reuse();

                        std::vector<DataTypePtr> data_types;
                        MutableColumns value_columns;
                        if (_use_fixed_length_serialization_opt) {
                            for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
                                auto data_type =
                                        _aggregate_evaluators[i]->function()->get_serialized_type();
                                if (mem_reuse) {
                                    value_columns.emplace_back(
                                            std::move(*out_block->get_by_position(i + key_size)
                                                               .column)
                                                    .mutate());
                                } else {
                                    // slot type of value it should always be string type
                                    value_columns.emplace_back(_aggregate_evaluators[i]
                                                                       ->function()
                                                                       ->create_serialize_column());
                                }
                                data_types.emplace_back(data_type);
                            }

                            for (int i = 0; i != _aggregate_evaluators.size(); ++i) {
                                SCOPED_TIMER(_serialize_data_timer);
                                _aggregate_evaluators[i]->streaming_agg_serialize_to_column(
                                        in_block, value_columns[i], rows, &_agg_arena_pool);
                            }
                        } else {
                            std::vector<VectorBufferWriter> value_buffer_writers;
                            auto serialize_string_type = std::make_shared<DataTypeString>();
                            for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
                                if (mem_reuse) {
                                    value_columns.emplace_back(
                                            std::move(*out_block->get_by_position(i + key_size)
                                                               .column)
                                                    .mutate());
                                } else {
                                    // slot type of value it should always be string type
                                    value_columns.emplace_back(
                                            serialize_string_type->create_column());
                                }
                                data_types.emplace_back(serialize_string_type);
                                value_buffer_writers.emplace_back(
                                        *reinterpret_cast<ColumnString*>(value_columns[i].get()));
                            }

                            for (int i = 0; i != _aggregate_evaluators.size(); ++i) {
                                SCOPED_TIMER(_serialize_data_timer);
                                _aggregate_evaluators[i]->streaming_agg_serialize(
                                        in_block, value_buffer_writers[i], rows, &_agg_arena_pool);
                            }
                        }

                        if (!mem_reuse) {
                            ColumnsWithTypeAndName columns_with_schema;
                            for (int i = 0; i < key_size; ++i) {
                                columns_with_schema.emplace_back(
                                        key_columns[i]->clone_resized(rows),
                                        _probe_expr_ctxs[i]->root()->data_type(),
                                        _probe_expr_ctxs[i]->root()->expr_name());
                            }
                            for (int i = 0; i < value_columns.size(); ++i) {
                                columns_with_schema.emplace_back(std::move(value_columns[i]),
                                                                 data_types[i], "");
                            }
                            out_block->swap(Block(columns_with_schema));
                        } else {
                            for (int i = 0; i < key_size; ++i) {
                                std::move(*out_block->get_by_position(i).column)
                                        .mutate()
                                        ->insert_range_from(*key_columns[i], 0, rows);
                            }
                        }
                    }
                }
            },
            _agg_data._aggregated_method_variant);

    if (!ret_flag) {
        _emplace_into_hash_table(_places.data(), key_columns, rows);

        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            _aggregate_evaluators[i]->execute_batch_add(in_block, _offsets_of_aggregate_states[i],
                                                        _places.data(), &_agg_arena_pool,
                                                        _should_expand_hash_table);
        }
    }

    return Status::OK();
}

Status AggregationNode::_execute_with_serialized_key(Block* block) {
    if (_reach_limit) {
        return _execute_with_serialized_key_helper<true>(block);
    } else {
        return _execute_with_serialized_key_helper<false>(block);
    }
}

Status AggregationNode::_get_with_serialized_key_result(RuntimeState* state, Block* block,
                                                        bool* eos) {
    bool mem_reuse = block->mem_reuse();
    auto column_withschema = VectorizedUtils::create_columns_with_type_and_name(_row_descriptor);
    int key_size = _probe_expr_ctxs.size();

    MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (!mem_reuse) {
            key_columns.emplace_back(column_withschema[i].type->create_column());
        } else {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        }
    }
    MutableColumns value_columns;
    for (int i = key_size; i < column_withschema.size(); ++i) {
        if (!mem_reuse) {
            value_columns.emplace_back(column_withschema[i].type->create_column());
        } else {
            value_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        }
    }

    SCOPED_TIMER(_get_results_timer);
    std::visit(
            [&](auto&& agg_method) -> void {
                auto& data = agg_method.data;
                agg_method.init_once();
                const auto size = std::min(data.size(), size_t(state->batch_size()));
                using KeyType = std::decay_t<decltype(agg_method.iterator->get_first())>;
                std::vector<KeyType> keys(size);
                if (_values.size() < size) {
                    _values.resize(size);
                }

                size_t num_rows = 0;
                _aggregate_data_container->init_once();
                auto& iter = _aggregate_data_container->iterator;

                {
                    SCOPED_TIMER(_hash_table_iterate_timer);
                    while (iter != _aggregate_data_container->end() &&
                           num_rows < state->batch_size()) {
                        keys[num_rows] = iter.get_key<KeyType>();
                        _values[num_rows] = iter.get_aggregate_data();
                        ++iter;
                        ++num_rows;
                    }
                }

                {
                    SCOPED_TIMER(_insert_keys_to_column_timer);
                    agg_method.insert_keys_into_columns(keys, key_columns, num_rows, _probe_key_sz);
                }

                for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
                    _aggregate_evaluators[i]->insert_result_info_vec(
                            _values, _offsets_of_aggregate_states[i], value_columns[i].get(),
                            num_rows);
                }

                if (iter == _aggregate_data_container->end()) {
                    if (agg_method.data.has_null_key_data()) {
                        // only one key of group by support wrap null key
                        // here need additional processing logic on the null key / value
                        DCHECK(key_columns.size() == 1);
                        DCHECK(key_columns[0]->is_nullable());
                        if (key_columns[0]->size() < state->batch_size()) {
                            key_columns[0]->insert_data(nullptr, 0);
                            auto mapped = agg_method.data.get_null_key_data();
                            for (size_t i = 0; i < _aggregate_evaluators.size(); ++i)
                                _aggregate_evaluators[i]->insert_result_info(
                                        mapped + _offsets_of_aggregate_states[i],
                                        value_columns[i].get());
                            *eos = true;
                        }
                    } else {
                        *eos = true;
                    }
                }
            },
            _agg_data._aggregated_method_variant);

    if (!mem_reuse) {
        *block = column_withschema;
        MutableColumns columns(block->columns());
        for (int i = 0; i < block->columns(); ++i) {
            if (i < key_size) {
                columns[i] = std::move(key_columns[i]);
            } else {
                columns[i] = std::move(value_columns[i - key_size]);
            }
        }
        block->set_columns(std::move(columns));
    }

    return Status::OK();
}

Status AggregationNode::_serialize_with_serialized_key_result(RuntimeState* state, Block* block,
                                                              bool* eos) {
    SCOPED_TIMER(_serialize_result_timer);
    int key_size = _probe_expr_ctxs.size();
    int agg_size = _aggregate_evaluators.size();
    MutableColumns value_columns(agg_size);
    DataTypes value_data_types(agg_size);

    bool mem_reuse = block->mem_reuse();

    MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (mem_reuse) {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        } else {
            key_columns.emplace_back(_probe_expr_ctxs[i]->root()->data_type()->create_column());
        }
    }

    std::visit(
            [&](auto&& agg_method) -> void {
                agg_method.init_once();
                auto& data = agg_method.data;
                const auto size = std::min(data.size(), size_t(state->batch_size()));
                using KeyType = std::decay_t<decltype(agg_method.iterator->get_first())>;
                std::vector<KeyType> keys(size);
                if (_values.size() < size + 1) {
                    _values.resize(size + 1);
                }

                size_t num_rows = 0;
                _aggregate_data_container->init_once();
                auto& iter = _aggregate_data_container->iterator;

                {
                    SCOPED_TIMER(_hash_table_iterate_timer);
                    while (iter != _aggregate_data_container->end() &&
                           num_rows < state->batch_size()) {
                        keys[num_rows] = iter.get_key<KeyType>();
                        _values[num_rows] = iter.get_aggregate_data();
                        ++iter;
                        ++num_rows;
                    }
                }

                {
                    SCOPED_TIMER(_insert_keys_to_column_timer);
                    agg_method.insert_keys_into_columns(keys, key_columns, num_rows, _probe_key_sz);
                }

                if (iter == _aggregate_data_container->end()) {
                    if (agg_method.data.has_null_key_data()) {
                        // only one key of group by support wrap null key
                        // here need additional processing logic on the null key / value
                        DCHECK(key_columns.size() == 1);
                        DCHECK(key_columns[0]->is_nullable());
                        if (agg_method.data.has_null_key_data()) {
                            key_columns[0]->insert_data(nullptr, 0);
                            _values[num_rows] = agg_method.data.get_null_key_data();
                            ++num_rows;
                            *eos = true;
                        }
                    } else {
                        *eos = true;
                    }
                }

                if (_use_fixed_length_serialization_opt) {
                    SCOPED_TIMER(_serialize_data_timer);
                    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
                        value_data_types[i] =
                                _aggregate_evaluators[i]->function()->get_serialized_type();
                        if (mem_reuse) {
                            value_columns[i] =
                                    std::move(*block->get_by_position(i + key_size).column)
                                            .mutate();
                        } else {
                            value_columns[i] =
                                    _aggregate_evaluators[i]->function()->create_serialize_column();
                        }
                        _aggregate_evaluators[i]->function()->serialize_to_column(
                                _values, _offsets_of_aggregate_states[i], value_columns[i],
                                num_rows);
                    }
                } else {
                    SCOPED_TIMER(_serialize_data_timer);
                    std::vector<VectorBufferWriter> value_buffer_writers;
                    auto serialize_string_type = std::make_shared<DataTypeString>();
                    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
                        value_data_types[i] = serialize_string_type;
                        if (mem_reuse) {
                            value_columns[i] =
                                    std::move(*block->get_by_position(i + key_size).column)
                                            .mutate();
                        } else {
                            value_columns[i] = serialize_string_type->create_column();
                        }
                        value_buffer_writers.emplace_back(
                                *reinterpret_cast<ColumnString*>(value_columns[i].get()));
                    }
                    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
                        _aggregate_evaluators[i]->function()->serialize_vec(
                                _values, _offsets_of_aggregate_states[i], value_buffer_writers[i],
                                num_rows);
                    }
                }
            },
            _agg_data._aggregated_method_variant);

    if (!mem_reuse) {
        ColumnsWithTypeAndName columns_with_schema;
        for (int i = 0; i < key_size; ++i) {
            columns_with_schema.emplace_back(std::move(key_columns[i]),
                                             _probe_expr_ctxs[i]->root()->data_type(),
                                             _probe_expr_ctxs[i]->root()->expr_name());
        }
        for (int i = 0; i < agg_size; ++i) {
            columns_with_schema.emplace_back(std::move(value_columns[i]), value_data_types[i], "");
        }
        *block = Block(columns_with_schema);
    }

    return Status::OK();
}

Status AggregationNode::_merge_with_serialized_key(Block* block) {
    if (_reach_limit) {
        return _merge_with_serialized_key_helper<true>(block);
    } else {
        return _merge_with_serialized_key_helper<false>(block);
    }
}

void AggregationNode::_update_memusage_with_serialized_key() {
    std::visit(
            [&](auto&& agg_method) -> void {
                auto& data = agg_method.data;
                _data_mem_tracker->consume(_agg_arena_pool.size() -
                                           _mem_usage_record.used_in_arena);
                _data_mem_tracker->consume(data.get_buffer_size_in_bytes() -
                                           _mem_usage_record.used_in_state);
                _mem_usage_record.used_in_state = data.get_buffer_size_in_bytes();
                _mem_usage_record.used_in_arena = _agg_arena_pool.size();
            },
            _agg_data._aggregated_method_variant);
}

void AggregationNode::_close_with_serialized_key() {
    std::visit(
            [&](auto&& agg_method) -> void {
                auto& data = agg_method.data;
                data.for_each_mapped([&](auto& mapped) {
                    if (mapped) {
                        _destroy_agg_status(mapped);
                        mapped = nullptr;
                    }
                });
            },
            _agg_data._aggregated_method_variant);
    release_tracker();
}

void AggregationNode::release_tracker() {
    _data_mem_tracker->release(_mem_usage_record.used_in_state + _mem_usage_record.used_in_arena);
}

} // namespace doris::vectorized
