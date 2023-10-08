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

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <opentelemetry/nostd/shared_ptr.h>

#include <array>
#include <atomic>
#include <memory>
#include <string>

#include "common/status.h"
#include "exec/exec_node.h"
#include "runtime/block_spill_manager.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/telemetry/telemetry.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_table_key_holder.h"
#include "vec/common/hash_table/hash_table_utils.h"
#include "vec/common/hash_table/string_hash_table.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/block.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris {
class ObjectPool;
} // namespace doris

namespace doris::vectorized {
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
          _hash_table_compute_timer(nullptr),
          _hash_table_input_counter(nullptr),
          _build_timer(nullptr),
          _expr_timer(nullptr),
          _exec_timer(nullptr),
          _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
          _intermediate_tuple_desc(nullptr),
          _output_tuple_id(tnode.agg_node.output_tuple_id),
          _output_tuple_desc(nullptr),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_merge(false),
          _serialize_key_timer(nullptr),
          _merge_timer(nullptr),
          _get_results_timer(nullptr),
          _serialize_data_timer(nullptr),
          _serialize_result_timer(nullptr),
          _deserialize_data_timer(nullptr),
          _hash_table_iterate_timer(nullptr),
          _insert_keys_to_column_timer(nullptr),
          _streaming_agg_timer(nullptr),
          _hash_table_size_counter(nullptr),
          _max_row_size_counter(nullptr) {
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
    _agg_data = std::make_unique<AggregatedDataVariants>();
    _agg_arena_pool = std::make_unique<Arena>();
}

AggregationNode::~AggregationNode() = default;

Status AggregationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    // ignore return status for now , so we need to introduce ExecNode::init()
    RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.agg_node.grouping_exprs, _probe_expr_ctxs));

    // init aggregate functions
    _aggregate_evaluators.reserve(tnode.agg_node.aggregate_functions.size());
    // In case of : `select * from (select GoodEvent from hits union select CounterID from hits) as h limit 10;`
    // only union with limit: we can short circuit query the pipeline exec engine.
    _can_short_circuit =
            tnode.agg_node.aggregate_functions.empty() && state->enable_pipeline_exec();

    TSortInfo dummy;
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        AggFnEvaluator* evaluator = nullptr;
        RETURN_IF_ERROR(AggFnEvaluator::create(
                _pool, tnode.agg_node.aggregate_functions[i],
                tnode.agg_node.__isset.agg_sort_infos ? tnode.agg_node.agg_sort_infos[i] : dummy,
                &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    const auto& agg_functions = tnode.agg_node.aggregate_functions;
    _external_agg_bytes_threshold = state->external_agg_bytes_threshold();

    if (_external_agg_bytes_threshold > 0) {
        size_t spill_partition_count_bits = 4;
        if (state->query_options().__isset.external_agg_partition_bits) {
            spill_partition_count_bits = state->query_options().external_agg_partition_bits;
        }

        _spill_partition_helper =
                std::make_unique<SpillPartitionHelper>(spill_partition_count_bits);
    }

    _is_merge = std::any_of(agg_functions.cbegin(), agg_functions.cend(),
                            [](const auto& e) { return e.nodes[0].agg_expr.is_merge_agg; });
    return Status::OK();
}

void AggregationNode::_init_hash_method(const VExprContextSPtrs& probe_exprs) {
    DCHECK(probe_exprs.size() >= 1);

    using Type = AggregatedDataVariants::Type;
    Type t(Type::serialized);

    if (probe_exprs.size() == 1) {
        auto is_nullable = probe_exprs[0]->root()->is_nullable();
        PrimitiveType type = probe_exprs[0]->root()->result_type();
        switch (type) {
        case TYPE_TINYINT:
        case TYPE_BOOLEAN:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DATEV2:
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_DATETIMEV2:
        case TYPE_LARGEINT:
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128I: {
            size_t size = get_primitive_type_size(type);
            if (size == 1) {
                t = Type::int8_key;
            } else if (size == 2) {
                t = Type::int16_key;
            } else if (size == 4) {
                t = Type::int32_key;
            } else if (size == 8) {
                t = Type::int64_key;
            } else if (size == 16) {
                t = Type::int128_key;
            } else {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "meet invalid type size, size={}, type={}", size,
                                type_to_string(type));
            }
            break;
        }
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            t = Type::string_key;
            break;
        }
        default:
            t = Type::serialized;
        }

        _agg_data->init(get_hash_key_type_with_phase(t, !_is_first_phase), is_nullable);
    } else {
        bool use_fixed_key = true;
        bool has_null = false;
        size_t key_byte_size = 0;
        size_t bitmap_size = get_bitmap_size(_probe_expr_ctxs.size());

        _probe_key_sz.resize(_probe_expr_ctxs.size());
        for (int i = 0; i < _probe_expr_ctxs.size(); ++i) {
            const auto& expr = _probe_expr_ctxs[i]->root();
            const auto& data_type = expr->data_type();

            if (!data_type->have_maximum_size_of_value()) {
                use_fixed_key = false;
                break;
            }

            auto is_null = data_type->is_nullable();
            has_null |= is_null;
            _probe_key_sz[i] = data_type->get_maximum_size_of_value_in_memory() - (is_null ? 1 : 0);
            key_byte_size += _probe_key_sz[i];
        }

        if (!has_null) {
            bitmap_size = 0;
        }

        if (bitmap_size + key_byte_size > sizeof(UInt256)) {
            use_fixed_key = false;
        }

        if (use_fixed_key) {
            if (bitmap_size + key_byte_size <= sizeof(UInt64)) {
                t = Type::int64_keys;
            } else if (bitmap_size + key_byte_size <= sizeof(UInt128)) {
                t = Type::int128_keys;
            } else if (bitmap_size + key_byte_size <= sizeof(UInt136)) {
                t = Type::int136_keys;
            } else {
                t = Type::int256_keys;
            }
            _agg_data->init(get_hash_key_type_with_phase(t, !_is_first_phase), has_null);
        } else {
            _agg_data->init(Type::serialized);
        }
    }
}

Status AggregationNode::prepare_profile(RuntimeState* state) {
    _memory_usage_counter = ADD_LABEL_COUNTER(runtime_profile(), "MemoryUsage");
    _hash_table_memory_usage =
            ADD_CHILD_COUNTER(runtime_profile(), "HashTable", TUnit::BYTES, "MemoryUsage");
    _serialize_key_arena_memory_usage = runtime_profile()->AddHighWaterMarkCounter(
            "SerializeKeyArena", TUnit::BYTES, "MemoryUsage");

    _build_timer = ADD_TIMER_WITH_LEVEL(runtime_profile(), "BuildTime", 1);
    _build_table_convert_timer = ADD_TIMER(runtime_profile(), "BuildConvertToPartitionedTime");
    _serialize_key_timer = ADD_TIMER(runtime_profile(), "SerializeKeyTime");
    _exec_timer = ADD_TIMER_WITH_LEVEL(runtime_profile(), "ExecTime", 1);
    _merge_timer = ADD_TIMER(runtime_profile(), "MergeTime");
    _expr_timer = ADD_TIMER(runtime_profile(), "ExprTime");
    _get_results_timer = ADD_TIMER(runtime_profile(), "GetResultsTime");
    _serialize_data_timer = ADD_TIMER(runtime_profile(), "SerializeDataTime");
    _serialize_result_timer = ADD_TIMER(runtime_profile(), "SerializeResultTime");
    _deserialize_data_timer = ADD_TIMER(runtime_profile(), "DeserializeAndMergeTime");
    _hash_table_compute_timer = ADD_TIMER(runtime_profile(), "HashTableComputeTime");
    _hash_table_emplace_timer = ADD_TIMER(runtime_profile(), "HashTableEmplaceTime");
    _hash_table_iterate_timer = ADD_TIMER(runtime_profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(runtime_profile(), "InsertKeysToColumnTime");
    _streaming_agg_timer = ADD_TIMER(runtime_profile(), "StreamingAggTime");
    _hash_table_size_counter = ADD_COUNTER(runtime_profile(), "HashTableSize", TUnit::UNIT);
    _hash_table_input_counter = ADD_COUNTER(runtime_profile(), "HashTableInputCount", TUnit::UNIT);
    _max_row_size_counter = ADD_COUNTER(runtime_profile(), "MaxRowSizeInBytes", TUnit::UNIT);
    COUNTER_SET(_max_row_size_counter, (int64_t)0);
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc()));

    _agg_profile_arena = std::make_unique<Arena>();

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
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(
                state, child(0)->row_desc(), intermediate_slot_desc, output_slot_desc));
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
        _agg_data->init(AggregatedDataVariants::Type::without_key);

        _agg_data->without_key = reinterpret_cast<AggregateDataPtr>(
                _agg_profile_arena->alloc(_total_size_of_aggregate_states));

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
                _agg_data->method_variant);
        if (_is_merge) {
            _executor.execute = std::bind<Status>(&AggregationNode::_merge_with_serialized_key,
                                                  this, std::placeholders::_1);
        } else {
            _executor.execute = std::bind<Status>(&AggregationNode::_execute_with_serialized_key,
                                                  this, std::placeholders::_1);
        }

        if (_is_streaming_preagg) {
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

        _should_limit_output = _limit != -1 &&       // has limit
                               _conjuncts.empty() && // no having conjunct
                               _needs_finalize;      // agg's finalize step
    }

    fmt::memory_buffer msg;
    fmt::format_to(msg,
                   "(_is_merge: {}, _needs_finalize: {}, Streaming Preaggregation: {}, agg size: "
                   "{}, limit: {})",
                   _is_merge ? "true" : "false", _needs_finalize ? "true" : "false",
                   _is_streaming_preagg ? "true" : "false",
                   std::to_string(_aggregate_evaluators.size()), std::to_string(_limit));
    runtime_profile()->add_info_string("AggInfos:", fmt::to_string(msg));
    return Status::OK();
}

Status AggregationNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_ERROR(ExecNode::prepare(state));
    RETURN_IF_ERROR(prepare_profile(state));
    return Status::OK();
}

Status AggregationNode::alloc_resource(doris::RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));

    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state));
        _aggregate_evaluators[i]->set_version(state->be_exec_version());
    }

    // move _create_agg_status to open not in during prepare,
    // because during prepare and open thread is not the same one,
    // this could cause unable to get JVM
    if (_probe_expr_ctxs.empty()) {
        // _create_agg_status may acquire a lot of memory, may allocate failed when memory is very few
        RETURN_IF_CATCH_EXCEPTION(static_cast<void>(_create_agg_status(_agg_data->without_key)));
        _agg_data_created_without_key = true;
    }

    return Status::OK();
}

Status AggregationNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(_children[0]->open(state));

    // Streaming preaggregations do all processing in GetNext().
    if (_is_streaming_preagg) {
        return Status::OK();
    }
    bool eos = false;
    Block block;
    while (!eos) {
        RETURN_IF_CANCELLED(state);
        release_block_memory(block);
        RETURN_IF_ERROR(_children[0]->get_next_after_projects(
                state, &block, &eos,
                std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                  ExecNode::get_next,
                          _children[0], std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));
        RETURN_IF_ERROR(sink(state, &block, eos));
    }
    static_cast<void>(_children[0]->close(state));

    return Status::OK();
}

Status AggregationNode::do_pre_agg(vectorized::Block* input_block,
                                   vectorized::Block* output_block) {
    RETURN_IF_ERROR(_executor.pre_agg(input_block, output_block));

    // pre stream agg need use _num_row_return to decide whether to do pre stream agg
    _num_rows_returned += output_block->rows();
    _make_nullable_output_key(output_block);
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    _executor.update_memusage();
    return Status::OK();
}

Status AggregationNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (_is_streaming_preagg) {
        RETURN_IF_CANCELLED(state);
        release_block_memory(_preagg_block);
        while (_preagg_block.rows() == 0 && !_child_eos) {
            RETURN_IF_ERROR(_children[0]->get_next_after_projects(
                    state, &_preagg_block, &_child_eos,
                    std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                      ExecNode::get_next,
                              _children[0], std::placeholders::_1, std::placeholders::_2,
                              std::placeholders::_3)));
        };
        {
            if (_preagg_block.rows() != 0) {
                RETURN_IF_ERROR(do_pre_agg(&_preagg_block, block));
            } else {
                RETURN_IF_ERROR(pull(state, block, eos));
            }
        }
    } else {
        RETURN_IF_ERROR(pull(state, block, eos));
    }
    return Status::OK();
}

Status AggregationNode::pull(doris::RuntimeState* state, vectorized::Block* block, bool* eos) {
    RETURN_IF_ERROR(_executor.get_result(state, block, eos));
    _make_nullable_output_key(block);
    // dispose the having clause, should not be execute in prestreaming agg
    RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, block, block->columns()));
    reached_limit(block, eos);

    return Status::OK();
}

Status AggregationNode::sink(doris::RuntimeState* state, vectorized::Block* in_block, bool eos) {
    if (in_block->rows() > 0) {
        RETURN_IF_ERROR(_executor.execute(in_block));
        RETURN_IF_ERROR(_try_spill_disk());
        _executor.update_memusage();
    }
    if (eos) {
        if (_spill_context.has_data) {
            static_cast<void>(_try_spill_disk(true));
            RETURN_IF_ERROR(_spill_context.prepare_for_reading());
        }
        _can_read = true;
    }
    return Status::OK();
}

void AggregationNode::release_resource(RuntimeState* state) {
    for (auto* aggregate_evaluator : _aggregate_evaluators) {
        aggregate_evaluator->close(state);
    }
    if (_executor.close) {
        _executor.close();
    }

    /// _hash_table_size_counter may be null if prepare failed.
    if (_hash_table_size_counter) {
        std::visit(
                [&](auto&& agg_method) {
                    COUNTER_SET(_hash_table_size_counter, int64_t(agg_method.data.size()));
                },
                _agg_data->method_variant);
    }
    _release_mem();
    ExecNode::release_resource(state);
}

Status AggregationNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

Status AggregationNode::_create_agg_status(AggregateDataPtr data) {
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        try {
            _aggregate_evaluators[i]->create(data + _offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                _aggregate_evaluators[j]->destroy(data + _offsets_of_aggregate_states[j]);
            }
            throw;
        }
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
    DCHECK(_agg_data->without_key != nullptr);
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
                _agg_data->without_key + _offsets_of_aggregate_states[i], column);
    }

    const auto& block_schema = block->get_columns_with_type_and_name();
    DCHECK_EQ(block_schema.size(), columns.size());
    for (int i = 0; i < block_schema.size(); ++i) {
        const auto column_type = block_schema[i].type;
        if (!column_type->equals(*data_types[i])) {
            if (!is_array(remove_nullable(column_type))) {
                if (!column_type->is_nullable() || data_types[i]->is_nullable() ||
                    !remove_nullable(column_type)->equals(*data_types[i])) {
                    return Status::InternalError(
                            "column_type not match data_types, column_type={}, data_types={}",
                            column_type->get_name(), data_types[i]->get_name());
                }
            }

            if (column_type->is_nullable() && !data_types[i]->is_nullable()) {
                ColumnPtr ptr = std::move(columns[i]);
                // unless `count`, other aggregate function dispose empty set should be null
                // so here check the children row return
                ptr = make_nullable(ptr, _children[0]->rows_returned() == 0);
                columns[i] = ptr->assume_mutable();
            }
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

    DCHECK(_agg_data->without_key != nullptr);
    int agg_size = _aggregate_evaluators.size();

    MutableColumns value_columns(agg_size);
    std::vector<DataTypePtr> data_types(agg_size);
    // will serialize data to string column
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        data_types[i] = _aggregate_evaluators[i]->function()->get_serialized_type();
        value_columns[i] = _aggregate_evaluators[i]->function()->create_serialize_column();
    }

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->function()->serialize_without_key_to_column(
                _agg_data->without_key + _offsets_of_aggregate_states[i], *value_columns[i]);
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
    DCHECK(_agg_data->without_key != nullptr);
    SCOPED_TIMER(_build_timer);
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_single_add(
                block, _agg_data->without_key + _offsets_of_aggregate_states[i],
                _agg_arena_pool.get()));
    }
    return Status::OK();
}

Status AggregationNode::_merge_without_key(Block* block) {
    SCOPED_TIMER(_merge_timer);
    DCHECK(_agg_data->without_key != nullptr);
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        if (_aggregate_evaluators[i]->is_merge()) {
            int col_id = _get_slot_column_id(_aggregate_evaluators[i]);
            auto column = block->get_by_position(col_id).column;
            if (column->is_nullable()) {
                column = ((ColumnNullable*)column.get())->get_nested_column_ptr();
            }

            SCOPED_TIMER(_deserialize_data_timer);
            _aggregate_evaluators[i]->function()->deserialize_and_merge_from_column(
                    _agg_data->without_key + _offsets_of_aggregate_states[i], *column,
                    _agg_arena_pool.get());
        } else {
            RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_single_add(
                    block, _agg_data->without_key + _offsets_of_aggregate_states[i],
                    _agg_arena_pool.get()));
        }
    }
    return Status::OK();
}

void AggregationNode::_update_memusage_without_key() {
    auto arena_memory_usage = _agg_arena_pool->size() - _mem_usage_record.used_in_arena;
    mem_tracker()->consume(arena_memory_usage);
    _serialize_key_arena_memory_usage->add(arena_memory_usage);
    _mem_usage_record.used_in_arena = _agg_arena_pool->size();
}

void AggregationNode::_close_without_key() {
    //because prepare maybe failed, and couldn't create agg data.
    //but finally call close to destory agg data, if agg data has bitmapValue
    //will be core dump, it's not initialized
    if (_agg_data_created_without_key) {
        static_cast<void>(_destroy_agg_status(_agg_data->without_key));
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
    if (!_should_expand_hash_table) {
        return false;
    }

    return std::visit(
            [&](auto&& agg_method) -> bool {
                auto& hash_tbl = agg_method.data;
                auto [ht_mem, ht_rows] =
                        std::pair {hash_tbl.get_buffer_size_in_bytes(), hash_tbl.size()};

                // Need some rows in tables to have valid statistics.
                if (ht_rows == 0) {
                    return true;
                }

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
                if (aggregated_input_rows <= 0) {
                    return true;
                }

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
            _agg_data->method_variant);
}

size_t AggregationNode::_memory_usage() const {
    size_t usage = 0;
    std::visit(
            [&](auto&& agg_method) {
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                                      HashMethodType>::value) {
                    usage += agg_method.keys_memory_usage;
                }
                usage += agg_method.data.get_buffer_size_in_bytes();
            },
            _agg_data->method_variant);

    if (_agg_arena_pool) {
        usage += _agg_arena_pool->size();
    }

    if (_aggregate_data_container) {
        usage += _aggregate_data_container->memory_usage();
    }

    return usage;
}

Status AggregationNode::_reset_hash_table() {
    return std::visit(
            [&](auto&& agg_method) {
                auto& hash_table = agg_method.data;
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using HashTableType = std::decay_t<decltype(hash_table)>;

                if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                                      HashMethodType>::value) {
                    agg_method.reset();
                }

                hash_table.for_each_mapped([&](auto& mapped) {
                    if (mapped) {
                        static_cast<void>(_destroy_agg_status(mapped));
                        mapped = nullptr;
                    }
                });

                _aggregate_data_container.reset(new AggregateDataContainer(
                        sizeof(typename HashTableType::key_type),
                        ((_total_size_of_aggregate_states + _align_aggregate_states - 1) /
                         _align_aggregate_states) *
                                _align_aggregate_states));
                hash_table = HashTableType();
                _agg_arena_pool.reset(new Arena);
                return Status::OK();
            },
            _agg_data->method_variant);
}

size_t AggregationNode::_get_hash_table_size() {
    return std::visit([&](auto&& agg_method) { return agg_method.data.size(); },
                      _agg_data->method_variant);
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

                auto creator = [this](const auto& ctor, const auto& key) {
                    using KeyType = std::decay_t<decltype(key)>;
                    if constexpr (HashTableTraits<HashTableType>::is_string_hash_table &&
                                  !std::is_same_v<StringRef, KeyType>) {
                        StringRef string_ref = to_string_ref(key);
                        ArenaKeyHolder key_holder {string_ref, *_agg_arena_pool};
                        key_holder_persist_key(key_holder);
                        auto mapped = _aggregate_data_container->append_data(key_holder.key);
                        static_cast<void>(_create_agg_status(mapped));
                        ctor(key, mapped);
                    } else {
                        auto mapped = _aggregate_data_container->append_data(key);
                        static_cast<void>(_create_agg_status(mapped));
                        ctor(key, mapped);
                    }
                };

                auto creator_for_null_key = [this](auto& mapped) {
                    mapped = _agg_arena_pool->aligned_alloc(_total_size_of_aggregate_states,
                                                            _align_aggregate_states);
                    static_cast<void>(_create_agg_status(mapped));
                };

                if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                    const auto& keys = state.get_keys();
                    if (_hash_values.size() < num_rows) {
                        _hash_values.resize(num_rows);
                    }
                    for (size_t i = 0; i < num_rows; ++i) {
                        _hash_values[i] = agg_method.data.hash(keys[i]);
                    }

                    SCOPED_TIMER(_hash_table_emplace_timer);
                    if constexpr (ColumnsHashing::IsSingleNullableColumnMethod<AggState>::value) {
                        for (size_t i = 0; i < num_rows; ++i) {
                            if (LIKELY(i + HASH_MAP_PREFETCH_DIST < num_rows)) {
                                agg_method.data.prefetch_by_hash(
                                        _hash_values[i + HASH_MAP_PREFETCH_DIST]);
                            }

                            places[i] = state.lazy_emplace_key(agg_method.data, i, *_agg_arena_pool,
                                                               _hash_values[i], creator,
                                                               creator_for_null_key);
                        }
                    } else {
                        state.lazy_emplace_keys(agg_method.data, keys, _hash_values, creator,
                                                places);
                    }
                } else {
                    SCOPED_TIMER(_hash_table_emplace_timer);
                    for (size_t i = 0; i < num_rows; ++i) {
                        AggregateDataPtr mapped = nullptr;
                        if constexpr (ColumnsHashing::IsSingleNullableColumnMethod<
                                              AggState>::value) {
                            mapped = state.lazy_emplace_key(agg_method.data, i, *_agg_arena_pool,
                                                            creator, creator_for_null_key);
                        } else {
                            mapped = state.lazy_emplace_key(agg_method.data, i, *_agg_arena_pool,
                                                            creator);
                        }
                        places[i] = mapped;
                    }
                }

                COUNTER_UPDATE(_hash_table_input_counter, num_rows);
            },
            _agg_data->method_variant);
}

void AggregationNode::_find_in_hash_table(AggregateDataPtr* places, ColumnRawPtrs& key_columns,
                                          size_t num_rows) {
    std::visit(
            [&](auto&& agg_method) -> void {
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using HashTableType = std::decay_t<decltype(agg_method.data)>;
                using AggState = typename HashMethodType::State;
                AggState state(key_columns, _probe_key_sz, nullptr);

                _pre_serialize_key_if_need(state, agg_method, key_columns, num_rows);
                const auto& keys = state.get_keys();
                if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                    _hash_values.resize(num_rows);
                    for (size_t i = 0; i < num_rows; ++i) {
                        _hash_values[i] = agg_method.data.hash(keys[i]);
                    }
                }

                /// For all rows.
                for (size_t i = 0; i < num_rows; ++i) {
                    auto find_result = [&]() {
                        if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                            if (LIKELY(i + HASH_MAP_PREFETCH_DIST < num_rows)) {
                                agg_method.data.prefetch_by_hash(
                                        _hash_values[i + HASH_MAP_PREFETCH_DIST]);
                            }

                            return state.find_key_with_hash(agg_method.data, _hash_values[i],
                                                            keys[i]);
                        } else {
                            return state.find_key(agg_method.data, i, *_agg_arena_pool);
                        }
                    }();

                    if (find_result.is_found()) {
                        places[i] = find_result.get_mapped();
                    } else {
                        places[i] = nullptr;
                    }
                }
            },
            _agg_data->method_variant);
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
    RETURN_IF_ERROR(std::visit(
            [&](auto&& agg_method) -> Status {
                if (auto& hash_tbl = agg_method.data; hash_tbl.add_elem_size_overflow(rows)) {
                    /// If too much memory is used during the pre-aggregation stage,
                    /// it is better to output the data directly without performing further aggregation.
                    const bool used_too_much_memory =
                            (_external_agg_bytes_threshold > 0 &&
                             _memory_usage() > _external_agg_bytes_threshold);
                    // do not try to do agg, just init and serialize directly return the out_block
                    if (!_should_expand_preagg_hash_tables() || used_too_much_memory) {
                        SCOPED_TIMER(_streaming_agg_timer);
                        ret_flag = true;

                        // will serialize value data to string column.
                        // non-nullable column(id in `_make_nullable_keys`)
                        // will be converted to nullable.
                        bool mem_reuse = _make_nullable_keys.empty() && out_block->mem_reuse();

                        std::vector<DataTypePtr> data_types;
                        MutableColumns value_columns;
                        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
                            auto data_type =
                                    _aggregate_evaluators[i]->function()->get_serialized_type();
                            if (mem_reuse) {
                                value_columns.emplace_back(
                                        std::move(*out_block->get_by_position(i + key_size).column)
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
                            RETURN_IF_ERROR(
                                    _aggregate_evaluators[i]->streaming_agg_serialize_to_column(
                                            in_block, value_columns[i], rows,
                                            _agg_arena_pool.get()));
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
                return Status::OK();
            },
            _agg_data->method_variant));

    if (!ret_flag) {
        RETURN_IF_CATCH_EXCEPTION(_emplace_into_hash_table(_places.data(), key_columns, rows));

        for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
            RETURN_IF_ERROR(_aggregate_evaluators[i]->execute_batch_add(
                    in_block, _offsets_of_aggregate_states[i], _places.data(),
                    _agg_arena_pool.get(), _should_expand_hash_table));
        }
    }

    return Status::OK();
}

template <typename HashTableCtxType, typename HashTableType, typename KeyType>
Status AggregationNode::_serialize_hash_table_to_block(HashTableCtxType& context,
                                                       HashTableType& hash_table, Block& block,
                                                       std::vector<KeyType>& keys_) {
    int key_size = _probe_expr_ctxs.size();
    int agg_size = _aggregate_evaluators.size();

    MutableColumns value_columns(agg_size);
    DataTypes value_data_types(agg_size);
    MutableColumns key_columns;

    for (int i = 0; i < key_size; ++i) {
        key_columns.emplace_back(_probe_expr_ctxs[i]->root()->data_type()->create_column());
    }

    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
        value_data_types[i] = _aggregate_evaluators[i]->function()->get_serialized_type();
        value_columns[i] = _aggregate_evaluators[i]->function()->create_serialize_column();
    }

    context.init_once();
    const auto size = hash_table.size();
    std::vector<KeyType> keys(size);
    if (_values.size() < size) {
        _values.resize(size);
    }

    size_t num_rows = 0;
    _aggregate_data_container->init_once();
    auto& iter = _aggregate_data_container->iterator;

    {
        while (iter != _aggregate_data_container->end()) {
            keys[num_rows] = iter.get_key<KeyType>();
            _values[num_rows] = iter.get_aggregate_data();
            ++iter;
            ++num_rows;
        }
    }

    { context.insert_keys_into_columns(keys, key_columns, num_rows, _probe_key_sz); }

    if (hash_table.has_null_key_data()) {
        // only one key of group by support wrap null key
        // here need additional processing logic on the null key / value
        CHECK(key_columns.size() == 1);
        CHECK(key_columns[0]->is_nullable());
        key_columns[0]->insert_data(nullptr, 0);

        // Here is no need to set `keys[num_rows]`, keep it as default value.
        _values[num_rows] = hash_table.get_null_key_data();
        ++num_rows;
    }

    for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->function()->serialize_to_column(
                _values, _offsets_of_aggregate_states[i], value_columns[i], num_rows);
    }

    ColumnsWithTypeAndName columns_with_schema;
    for (int i = 0; i < key_size; ++i) {
        columns_with_schema.emplace_back(std::move(key_columns[i]),
                                         _probe_expr_ctxs[i]->root()->data_type(),
                                         _probe_expr_ctxs[i]->root()->expr_name());
    }
    for (int i = 0; i < agg_size; ++i) {
        columns_with_schema.emplace_back(std::move(value_columns[i]), value_data_types[i],
                                         _aggregate_evaluators[i]->function()->get_name());
    }

    block = columns_with_schema;
    keys_.swap(keys);
    return Status::OK();
}

template <typename HashTableCtxType, typename HashTableType>
Status AggregationNode::_spill_hash_table(HashTableCtxType& agg_method, HashTableType& hash_table) {
    Block block;
    std::vector<typename HashTableType::key_type> keys;
    RETURN_IF_ERROR(_serialize_hash_table_to_block(agg_method, hash_table, block, keys));
    CHECK_EQ(block.rows(), hash_table.size());
    CHECK_EQ(keys.size(), block.rows());

    if (!_spill_context.has_data) {
        _spill_context.has_data = true;
        _spill_context.runtime_profile = _runtime_profile->create_child("Spill", true, true);
    }

    BlockSpillWriterUPtr writer;
    RETURN_IF_ERROR(ExecEnv::GetInstance()->block_spill_mgr()->get_writer(
            std::numeric_limits<int32_t>::max(), writer, _spill_context.runtime_profile));
    Defer defer {[&]() {
        // redundant call is ok
        static_cast<void>(writer->close());
    }};
    _spill_context.stream_ids.emplace_back(writer->get_id());

    std::vector<size_t> partitioned_indices(block.rows());
    std::vector<size_t> blocks_rows(_spill_partition_helper->partition_count);

    // The last row may contain a null key.
    const size_t rows = hash_table.has_null_key_data() ? block.rows() - 1 : block.rows();
    for (size_t i = 0; i < rows; ++i) {
        const auto index = _spill_partition_helper->get_index(hash_table.hash(keys[i]));
        partitioned_indices[i] = index;
        blocks_rows[index]++;
    }

    if (hash_table.has_null_key_data()) {
        // Here put the row with null key at the last partition.
        const auto index = _spill_partition_helper->partition_count - 1;
        partitioned_indices[rows] = index;
        blocks_rows[index]++;
    }

    for (size_t i = 0; i < _spill_partition_helper->partition_count; ++i) {
        Block block_to_write = block.clone_empty();
        if (blocks_rows[i] == 0) {
            /// Here write one empty block to ensure there are enough blocks in the file,
            /// blocks' count should be equal with partition_count.
            static_cast<void>(writer->write(block_to_write));
            continue;
        }

        MutableBlock mutable_block(std::move(block_to_write));

        for (auto& column : mutable_block.mutable_columns()) {
            column->reserve(blocks_rows[i]);
        }

        size_t begin = 0;
        size_t length = 0;
        for (size_t j = 0; j < partitioned_indices.size(); ++j) {
            if (partitioned_indices[j] != i) {
                if (length > 0) {
                    mutable_block.add_rows(&block, begin, length);
                }
                length = 0;
                continue;
            }

            if (length == 0) {
                begin = j;
            }
            length++;
        }

        if (length > 0) {
            mutable_block.add_rows(&block, begin, length);
        }

        CHECK_EQ(mutable_block.rows(), blocks_rows[i]);
        RETURN_IF_ERROR(writer->write(mutable_block.to_block()));
    }
    RETURN_IF_ERROR(writer->close());

    return Status::OK();
}

Status AggregationNode::_try_spill_disk(bool eos) {
    if (_external_agg_bytes_threshold == 0) {
        return Status::OK();
    }
    return std::visit(
            [&](auto&& agg_method) -> Status {
                auto& hash_table = agg_method.data;
                if (!eos && _memory_usage() < _external_agg_bytes_threshold) {
                    return Status::OK();
                }

                if (_get_hash_table_size() == 0) {
                    return Status::OK();
                }

                RETURN_IF_ERROR(_spill_hash_table(agg_method, hash_table));
                return _reset_hash_table();
            },
            _agg_data->method_variant);
}

Status AggregationNode::_execute_with_serialized_key(Block* block) {
    if (_reach_limit) {
        return _execute_with_serialized_key_helper<true>(block);
    } else {
        return _execute_with_serialized_key_helper<false>(block);
    }
}

Status AggregationNode::_merge_spilt_data() {
    CHECK(!_spill_context.stream_ids.empty());

    for (auto& reader : _spill_context.readers) {
        CHECK_LT(_spill_context.read_cursor, reader->block_count());
        reader->seek(_spill_context.read_cursor);
        Block block;
        bool eos;
        RETURN_IF_ERROR(reader->read(&block, &eos));

        if (!block.empty()) {
            auto st = _merge_with_serialized_key_helper<false /* limit */, true /* for_spill */>(
                    &block);
            RETURN_IF_ERROR(st);
        }
    }
    _spill_context.read_cursor++;
    return Status::OK();
}

Status AggregationNode::_get_result_with_spilt_data(RuntimeState* state, Block* block, bool* eos) {
    CHECK(!_spill_context.stream_ids.empty());
    CHECK(_spill_partition_helper != nullptr) << "_spill_partition_helper should not be null";
    _aggregate_data_container->init_once();
    while (_aggregate_data_container->iterator == _aggregate_data_container->end()) {
        if (_spill_context.read_cursor == _spill_partition_helper->partition_count) {
            break;
        }
        RETURN_IF_ERROR(_reset_hash_table());
        RETURN_IF_ERROR(_merge_spilt_data());
        _aggregate_data_container->init_once();
    }

    RETURN_IF_ERROR(_get_result_with_serialized_key_non_spill(state, block, eos));
    if (*eos) {
        *eos = _spill_context.read_cursor == _spill_partition_helper->partition_count;
    }
    CHECK(!block->empty() || *eos);
    return Status::OK();
}

Status AggregationNode::_get_with_serialized_key_result(RuntimeState* state, Block* block,
                                                        bool* eos) {
    if (_spill_context.has_data) {
        return _get_result_with_spilt_data(state, block, eos);
    } else {
        return _get_result_with_serialized_key_non_spill(state, block, eos);
    }
}

Status AggregationNode::_get_result_with_serialized_key_non_spill(RuntimeState* state, Block* block,
                                                                  bool* eos) {
    // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
    bool mem_reuse = _make_nullable_keys.empty() && block->mem_reuse();

    auto columns_with_schema = VectorizedUtils::create_columns_with_type_and_name(_row_descriptor);
    int key_size = _probe_expr_ctxs.size();

    MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (!mem_reuse) {
            key_columns.emplace_back(columns_with_schema[i].type->create_column());
        } else {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        }
    }
    MutableColumns value_columns;
    for (int i = key_size; i < columns_with_schema.size(); ++i) {
        if (!mem_reuse) {
            value_columns.emplace_back(columns_with_schema[i].type->create_column());
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
                            for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
                                _aggregate_evaluators[i]->insert_result_info(
                                        mapped + _offsets_of_aggregate_states[i],
                                        value_columns[i].get());
                            }
                            *eos = true;
                        }
                    } else {
                        *eos = true;
                    }
                }
            },
            _agg_data->method_variant);

    if (!mem_reuse) {
        *block = columns_with_schema;
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
    if (_spill_context.has_data) {
        return _serialize_with_serialized_key_result_with_spilt_data(state, block, eos);
    } else {
        return _serialize_with_serialized_key_result_non_spill(state, block, eos);
    }
}

Status AggregationNode::_serialize_with_serialized_key_result_with_spilt_data(RuntimeState* state,
                                                                              Block* block,
                                                                              bool* eos) {
    CHECK(!_spill_context.stream_ids.empty());
    CHECK(_spill_partition_helper != nullptr) << "_spill_partition_helper should not be null";
    _aggregate_data_container->init_once();
    while (_aggregate_data_container->iterator == _aggregate_data_container->end()) {
        if (_spill_context.read_cursor == _spill_partition_helper->partition_count) {
            break;
        }
        RETURN_IF_ERROR(_reset_hash_table());
        RETURN_IF_ERROR(_merge_spilt_data());
        _aggregate_data_container->init_once();
    }

    RETURN_IF_ERROR(_serialize_with_serialized_key_result_non_spill(state, block, eos));
    if (*eos) {
        *eos = _spill_context.read_cursor == _spill_partition_helper->partition_count;
    }
    CHECK(!block->empty() || *eos);
    return Status::OK();
}
Status AggregationNode::_serialize_with_serialized_key_result_non_spill(RuntimeState* state,
                                                                        Block* block, bool* eos) {
    SCOPED_TIMER(_serialize_result_timer);
    int key_size = _probe_expr_ctxs.size();
    int agg_size = _aggregate_evaluators.size();
    MutableColumns value_columns(agg_size);
    DataTypes value_data_types(agg_size);

    // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
    bool mem_reuse = _make_nullable_keys.empty() && block->mem_reuse();

    MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (mem_reuse) {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        } else {
            key_columns.emplace_back(_probe_expr_ctxs[i]->root()->data_type()->create_column());
        }
    }

    SCOPED_TIMER(_get_results_timer);
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

                {
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
                }
            },
            _agg_data->method_variant);

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
        return _merge_with_serialized_key_helper<true, false>(block);
    } else {
        return _merge_with_serialized_key_helper<false, false>(block);
    }
}

void AggregationNode::_update_memusage_with_serialized_key() {
    std::visit(
            [&](auto&& agg_method) -> void {
                auto& data = agg_method.data;
                auto arena_memory_usage = _agg_arena_pool->size() +
                                          _aggregate_data_container->memory_usage() -
                                          _mem_usage_record.used_in_arena;
                mem_tracker()->consume(arena_memory_usage);
                mem_tracker()->consume(data.get_buffer_size_in_bytes() -
                                       _mem_usage_record.used_in_state);
                _serialize_key_arena_memory_usage->add(arena_memory_usage);
                COUNTER_UPDATE(_hash_table_memory_usage,
                               data.get_buffer_size_in_bytes() - _mem_usage_record.used_in_state);
                _mem_usage_record.used_in_state = data.get_buffer_size_in_bytes();
                _mem_usage_record.used_in_arena =
                        _agg_arena_pool->size() + _aggregate_data_container->memory_usage();
            },
            _agg_data->method_variant);
}

void AggregationNode::_close_with_serialized_key() {
    std::visit(
            [&](auto&& agg_method) -> void {
                auto& data = agg_method.data;
                data.for_each_mapped([&](auto& mapped) {
                    if (mapped) {
                        static_cast<void>(_destroy_agg_status(mapped));
                        mapped = nullptr;
                    }
                });
                if (data.has_null_key_data()) {
                    static_cast<void>(_destroy_agg_status(data.get_null_key_data()));
                }
            },
            _agg_data->method_variant);
    release_tracker();
}

void AggregationNode::release_tracker() {
    mem_tracker()->release(_mem_usage_record.used_in_state + _mem_usage_record.used_in_arena);
}

void AggregationNode::_release_mem() {
    _agg_data = nullptr;
    _aggregate_data_container = nullptr;
    _agg_profile_arena = nullptr;
    _agg_arena_pool = nullptr;
    _preagg_block.clear();

    PODArray<AggregateDataPtr> tmp_places;
    _places.swap(tmp_places);

    std::vector<char> tmp_deserialize_buffer;
    _deserialize_buffer.swap(tmp_deserialize_buffer);

    std::vector<size_t> tmp_hash_values;
    _hash_values.swap(tmp_hash_values);

    std::vector<AggregateDataPtr> tmp_values;
    _values.swap(tmp_values);
}

Status AggSpillContext::prepare_for_reading() {
    if (readers_prepared) {
        return Status::OK();
    }
    readers_prepared = true;

    readers.resize(stream_ids.size());
    auto* manager = ExecEnv::GetInstance()->block_spill_mgr();
    for (size_t i = 0; i != stream_ids.size(); ++i) {
        RETURN_IF_ERROR(manager->get_reader(stream_ids[i], readers[i], runtime_profile, true));
    }
    return Status::OK();
}

} // namespace doris::vectorized
