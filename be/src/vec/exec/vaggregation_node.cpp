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
#include "util/defer_op.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

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
        {256 * 1024, 1.1},
        // Expand into main memory if we're getting a significant reduction.
        {2 * 1024 * 1024, 2.0},
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
          _exec_timer(nullptr),
          _merge_timer(nullptr) {
    if (tnode.agg_node.__isset.use_streaming_preaggregation) {
        _is_streaming_preagg = tnode.agg_node.use_streaming_preaggregation;
        if (_is_streaming_preagg) {
            DCHECK(_conjunct_ctxs.empty()) << "Preaggs have no conjuncts";
            DCHECK(!tnode.agg_node.grouping_exprs.empty()) << "Streaming preaggs do grouping";
            DCHECK(_limit == -1) << "Preaggs have no limits";
        }
    } else {
        _is_streaming_preagg = false;
    }
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
        RETURN_IF_ERROR(
                AggFnEvaluator::create(_pool, tnode.agg_node.aggregate_functions[i], &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }

    const auto& agg_functions = tnode.agg_node.aggregate_functions;
    _is_merge = std::any_of(agg_functions.cbegin(), agg_functions.cend(),
                            [](const auto& e) { return e.nodes[0].agg_expr.is_merge_agg; });
    return Status::OK();
}

Status AggregationNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _exec_timer = ADD_TIMER(runtime_profile(), "ExecTime");
    _merge_timer = ADD_TIMER(runtime_profile(), "MergeTime");
    _expr_timer = ADD_TIMER(runtime_profile(), "ExprTime");

    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());
    RETURN_IF_ERROR(
            VExpr::prepare(_probe_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));

    _mem_pool = std::make_unique<MemPool>(mem_tracker().get());

    int j = _probe_expr_ctxs.size();
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[j];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[j];
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(state, child(0)->row_desc(),
                                                          _mem_pool.get(), intermediate_slot_desc,
                                                          output_slot_desc, mem_tracker()));
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
                return Status::RuntimeError(fmt::format("Logical error: align_of_data is not 2^N"));
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

        _create_agg_status(_agg_data.without_key);

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
        _agg_data.init(AggregatedDataVariants::Type::serialized);
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
            _max_size_of_stream_pre_agg_buffer = state->batch_size();
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
    }

    return Status::OK();
}

Status AggregationNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_ERROR(VExpr::open(_probe_expr_ctxs, state));

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state));
    }

    RETURN_IF_ERROR(_children[0]->open(state));

    // Streaming preaggregations do all processing in GetNext().
    if (_is_streaming_preagg) return Status::OK();

    bool eos = false;
    while (!eos) {
        Block block;
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(_children[0]->get_next(state, &block, &eos));
        if (block.rows() == 0) {
            continue;
        }
        RETURN_IF_ERROR(_executor.execute(&block));
        _executor.update_memusage();
        RETURN_IF_LIMIT_EXCEEDED(state, "aggregator, while execute open.");
    }

    return Status::OK();
}

Status AggregationNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented Aggregation Node::get_next scalar");
}

Status AggregationNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    block->clear();

    if (_is_streaming_preagg) {
        bool child_eos = false;

        Block in_block;
        RETURN_IF_CANCELLED(state);
        do {
            RETURN_IF_ERROR(_children[0]->get_next(state, &in_block, &child_eos));
        } while (in_block.rows() == 0 && !child_eos);

        if (in_block.rows() != 0) {
            RETURN_IF_ERROR(_executor.pre_agg(&in_block, block));
        } else {
            RETURN_IF_ERROR(_executor.get_result(state, block, eos));
        }
    } else {
        RETURN_IF_ERROR(_executor.get_result(state, block, eos));
        // dispose the having clause, should not be execute in prestreaming agg
        if (_vconjunct_ctx_ptr) {
            int result_column_id = -1;
            int orig_columns = block->columns();
            (*_vconjunct_ctx_ptr)->execute(block, &result_column_id);
            Block::filter_block(block, result_column_id, orig_columns);
        }
    }

    _executor.update_memusage();
    RETURN_IF_LIMIT_EXCEEDED(state, "aggregator, while execute get_next.");
    _num_rows_returned += block->rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    return Status::OK();
}

Status AggregationNode::close(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::close(state));
    VExpr::close(_probe_expr_ctxs, state);
    if (_executor.close) _executor.close();
    return Status::OK();
}

Status AggregationNode::_create_agg_status(AggregateDataPtr data) {
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->create(data + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggregationNode::_destory_agg_status(AggregateDataPtr data) {
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->function()->destroy(data + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggregationNode::_get_without_key_result(RuntimeState* state, Block* block, bool* eos) {
    DCHECK(_agg_data.without_key != nullptr);

    *block = VectorizedUtils::create_empty_columnswithtypename(row_desc());

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
            DCHECK(column_type->is_nullable());
            DCHECK(((DataTypeNullable*)column_type.get())
                           ->get_nested_type()
                           ->equals(*data_types[i]));
            DCHECK(!data_types[i]->is_nullable());
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
    if (UNLIKELY(_children[0]->rows_returned() == 0)) {
        *eos = true;
        return Status::OK();
    }

    DCHECK(_agg_data.without_key != nullptr);
    int agg_size = _aggregate_evaluators.size();

    MutableColumns value_columns(agg_size);
    std::vector<DataTypePtr> data_types(agg_size);

    // will serialize data to string column
    std::vector<VectorBufferWriter> value_buffer_writers;
    auto serialize_string_type = std::make_shared<DataTypeString>();
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        data_types[i] = serialize_string_type;
        value_columns[i] = serialize_string_type->create_column();
        value_buffer_writers.emplace_back(*reinterpret_cast<ColumnString*>(value_columns[i].get()));
    }

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->function()->serialize(
                _agg_data.without_key + _offsets_of_aggregate_states[i], value_buffer_writers[i]);
        value_buffer_writers[i].commit();
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
                block, _agg_data.without_key + _offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggregationNode::_merge_without_key(Block* block) {
    DCHECK(_agg_data.without_key != nullptr);
    std::unique_ptr<char[]> deserialize_buffer(new char[_total_size_of_aggregate_states]);
    int rows = block->rows();
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        if (_aggregate_evaluators[i]->is_merge()) {
            auto column = block->get_by_position(i).column;
            if (column->is_nullable()) {
                column = ((ColumnNullable *) column.get())->get_nested_column_ptr();
            }

            for (int j = 0; j < rows; ++j) {
                VectorBufferReader buffer_reader(((ColumnString *) (column.get()))->get_data_at(j));
                _create_agg_status(deserialize_buffer.get());

                _aggregate_evaluators[i]->function()->deserialize(
                        deserialize_buffer.get() + _offsets_of_aggregate_states[i], buffer_reader,
                        &_agg_arena_pool);

                _aggregate_evaluators[i]->function()->merge(
                        _agg_data.without_key + _offsets_of_aggregate_states[i],
                        deserialize_buffer.get() + _offsets_of_aggregate_states[i], &_agg_arena_pool);

                _destory_agg_status(deserialize_buffer.get());
            }
        } else {
            _aggregate_evaluators[i]->execute_single_add(
                block, _agg_data.without_key + _offsets_of_aggregate_states[i]);
        }
    }
    return Status::OK();
}

void AggregationNode::_update_memusage_without_key() {
    mem_tracker()->Consume(_agg_arena_pool.size() - _mem_usage_record.used_in_arena);
    _mem_usage_record.used_in_arena = _agg_arena_pool.size();
}

void AggregationNode::_close_without_key() {
    _destory_agg_status(_agg_data.without_key);
    release_tracker();
}

bool AggregationNode::_should_expand_preagg_hash_tables() {
    if (!_should_expand_hash_table) return false;

    auto& hash_tbl = _agg_data.serialized->data;
    auto [ht_mem, ht_rows] = std::pair{hash_tbl.get_buffer_size_in_bytes(), hash_tbl.size()};

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
    double min_reduction = STREAMING_HT_MIN_REDUCTION[cache_level].streaming_ht_min_reduction;

    //  COUNTER_SET(preagg_estimated_reduction_, estimated_reduction);
    //    COUNTER_SET(preagg_streaming_ht_min_reduction_, min_reduction);
    //  return estimated_reduction > min_reduction;
    _should_expand_hash_table = current_reduction > min_reduction;
    return _should_expand_hash_table;
}

Status AggregationNode::_pre_agg_with_serialized_key(doris::vectorized::Block* in_block,
                                                     doris::vectorized::Block* out_block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_probe_expr_ctxs.empty());
    // now we only support serialized key
    // TODO:
    DCHECK(_agg_data.serialized != nullptr);

    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    auto& method = *_agg_data.serialized;

    size_t key_size = _probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(in_block, &result_column_id));
            key_columns[i] = in_block->get_by_position(result_column_id).column.get();
        }
    }

    int rows = in_block->rows();
    PODArray<AggregateDataPtr> places(rows);
    AggState state(key_columns, {}, nullptr);

    // Stop expanding hash tables if we're not reducing the input sufficiently. As our
    // hash tables expand out of each level of cache hierarchy, every hash table lookup
    // will take longer. We also may not be able to expand hash tables because of memory
    // pressure. In either case we should always use the remaining space in the hash table
    // to avoid wasting memory.
    if (auto& hash_tbl = _agg_data.serialized->data; hash_tbl.add_elem_size_overflow(rows)) {
        // do not try to do agg, just init and serialize directly return the out_block
        if (!_should_expand_preagg_hash_tables()) {
            if (_streaming_pre_agg_buffer == nullptr) {
                _streaming_pre_agg_buffer = _agg_arena_pool.aligned_alloc(
                        _total_size_of_aggregate_states * _max_size_of_stream_pre_agg_buffer,
                        _align_aggregate_states);
            }

            auto aggregate_data = _streaming_pre_agg_buffer;
            for (size_t i = 0; i < rows; ++i) {
                _create_agg_status(aggregate_data);
                places[i] = aggregate_data;
                aggregate_data += _total_size_of_aggregate_states;
            }
            for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
                _aggregate_evaluators[i]->execute_batch_add(
                        in_block, _offsets_of_aggregate_states[i], places.data(), &_agg_arena_pool);
            }

            out_block->clear();
            auto column_withschema = VectorizedUtils::create_columns_with_type_and_name(row_desc());

            MutableColumns value_columns;
            for (int i = key_size; i < column_withschema.size(); ++i) {
                value_columns.emplace_back(column_withschema[i].type->create_column());
            }
            aggregate_data = _streaming_pre_agg_buffer;
            for (size_t j = 0; j < rows; ++j) {
                for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
                    _aggregate_evaluators[i]->insert_result_info(
                            aggregate_data + _offsets_of_aggregate_states[i],
                            value_columns[i].get());
                }
                aggregate_data += _total_size_of_aggregate_states;
            }

            *out_block = column_withschema;
            MutableColumns columns(out_block->columns());
            for (int i = 0; i < out_block->columns(); ++i) {
                if (i < key_size) {
                    columns[i] = std::move(*key_columns[i]).mutate();
                } else {
                    columns[i] = std::move(value_columns[i - key_size]);
                }
            }
            out_block->set_columns(std::move(columns));
            return Status::OK();
        }
    }

    /// For all rows.
    for (size_t i = 0; i < rows; ++i) {
        AggregateDataPtr aggregate_data = nullptr;

        auto emplace_result = state.emplace_key(method.data, i, _agg_arena_pool);

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (emplace_result.is_inserted()) {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            emplace_result.set_mapped(nullptr);

            aggregate_data = _agg_arena_pool.aligned_alloc(_total_size_of_aggregate_states,
                                                           _align_aggregate_states);
            _create_agg_status(aggregate_data);

            emplace_result.set_mapped(aggregate_data);
        } else
            aggregate_data = emplace_result.get_mapped();

        places[i] = aggregate_data;
        assert(places[i] != nullptr);
    }

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->execute_batch_add(in_block, _offsets_of_aggregate_states[i],
                                                    places.data(), &_agg_arena_pool);
    }

    return Status::OK();
}

Status AggregationNode::_execute_with_serialized_key(Block* block) {
    SCOPED_TIMER(_build_timer);
    DCHECK(!_probe_expr_ctxs.empty());
    SCOPED_TIMER(_build_timer);
    // now we only support serialized key
    // TODO:
    DCHECK(_agg_data.serialized != nullptr);

    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    auto& method = *_agg_data.serialized;

    size_t key_size = _probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);
    {
        SCOPED_TIMER(_expr_timer);
        for (size_t i = 0; i < key_size; ++i) {
            int result_column_id = -1;
            RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(block, &result_column_id));
            key_columns[i] = block->get_by_position(result_column_id).column.get();
        }
    }

    int rows = block->rows();
    PODArray<AggregateDataPtr> places(rows);

    AggState state(key_columns, {}, nullptr);

    /// For all rows.
    for (size_t i = 0; i < rows; ++i) {
        AggregateDataPtr aggregate_data = nullptr;

        auto emplace_result = state.emplace_key(method.data, i, _agg_arena_pool);

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (emplace_result.is_inserted()) {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            emplace_result.set_mapped(nullptr);

            aggregate_data = _agg_arena_pool.aligned_alloc(_total_size_of_aggregate_states,
                                                           _align_aggregate_states);
            _create_agg_status(aggregate_data);

            emplace_result.set_mapped(aggregate_data);
        } else
            aggregate_data = emplace_result.get_mapped();

        places[i] = aggregate_data;
        assert(places[i] != nullptr);
    }

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->execute_batch_add(block, _offsets_of_aggregate_states[i],
                                                    places.data(), &_agg_arena_pool);
    }

    return Status::OK();
}

Status AggregationNode::_get_with_serialized_key_result(RuntimeState* state, Block* block,
                                                        bool* eos) {
    DCHECK(_agg_data.serialized != nullptr);
    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    _agg_data.serialized->init_once();

    auto& method = *_agg_data.serialized;
    auto& data = _agg_data.serialized->data;
    auto& iter = _agg_data.serialized->iterator;

    block->clear();
    auto column_withschema = VectorizedUtils::create_columns_with_type_and_name(row_desc());

    int key_size = _probe_expr_ctxs.size();

    MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        key_columns.emplace_back(column_withschema[i].type->create_column());
    }
    MutableColumns value_columns;
    for (int i = key_size; i < column_withschema.size(); ++i) {
        value_columns.emplace_back(column_withschema[i].type->create_column());
    }

    while (iter != data.end() && key_columns[0]->size() < state->batch_size()) {
        const auto& key = iter->get_first();
        auto& mapped = iter->get_second();
        method.insertKeyIntoColumns(key, key_columns, {});
        for (size_t i = 0; i < _aggregate_evaluators.size(); ++i)
            _aggregate_evaluators[i]->insert_result_info(mapped + _offsets_of_aggregate_states[i],
                                                         value_columns[i].get());

        ++iter;
    }

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
    if (iter == data.end()) {
        *eos = true;
    }
    return Status::OK();
}

Status AggregationNode::_serialize_with_serialized_key_result(RuntimeState* state, Block* block,
                                                              bool* eos) {
    DCHECK(_agg_data.serialized != nullptr);
    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    _agg_data.serialized->init_once();

    auto& method = *_agg_data.serialized;
    auto& data = _agg_data.serialized->data;
    auto& iter = _agg_data.serialized->iterator;

    int key_size = _probe_expr_ctxs.size();
    int agg_size = _aggregate_evaluators.size();
    MutableColumns value_columns(agg_size);
    DataTypes value_data_types(agg_size);

    MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        key_columns.emplace_back(_probe_expr_ctxs[i]->root()->data_type()->create_column());
    }

    // will serialize data to string column
    std::vector<VectorBufferWriter> value_buffer_writers;
    auto serialize_string_type = std::make_shared<DataTypeString>();
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        value_data_types[i] = serialize_string_type;
        value_columns[i] = serialize_string_type->create_column();
        value_buffer_writers.emplace_back(*reinterpret_cast<ColumnString*>(value_columns[i].get()));
    }

    while (iter != data.end() && key_columns[0]->size() < state->batch_size()) {
        const auto& key = iter->get_first();
        auto& mapped = iter->get_second();
        // insert keys
        method.insertKeyIntoColumns(key, key_columns, {});

        // serialize values
        for (size_t i = 0; i < _aggregate_evaluators.size(); ++i) {
            _aggregate_evaluators[i]->function()->serialize(
                    mapped + _offsets_of_aggregate_states[i], value_buffer_writers[i]);
            value_buffer_writers[i].commit();
        }
        ++iter;
    }

    ColumnsWithTypeAndName columns_with_schema;
    for (int i = 0; i < key_size; ++i) {
        columns_with_schema.emplace_back(std::move(key_columns[i]),
                                         _probe_expr_ctxs[i]->root()->data_type(), "");
    }

    for (int i = 0; i < agg_size; ++i) {
        columns_with_schema.emplace_back(std::move(value_columns[i]), value_data_types[i], "");
    }

    *block = Block(columns_with_schema);
    if (iter == data.end()) {
        *eos = true;
    }
    return Status::OK();
}

Status AggregationNode::_merge_with_serialized_key(Block* block) {
    DCHECK(!_probe_expr_ctxs.empty());
    // now we only support serialized key
    // TODO:
    DCHECK(_agg_data.serialized != nullptr);

    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    auto& method = *_agg_data.serialized;

    size_t key_size = _probe_expr_ctxs.size();
    ColumnRawPtrs key_columns(key_size);

    for (size_t i = 0; i < key_size; ++i) {
        int result_column_id = -1;
        RETURN_IF_ERROR(_probe_expr_ctxs[i]->execute(block, &result_column_id));
        key_columns[i] = block->get_by_position(result_column_id).column.get();
    }

    int rows = block->rows();
    PODArray<AggregateDataPtr> places(rows);

    AggState state(key_columns, {}, nullptr);

    /// For all rows.
    for (size_t i = 0; i < rows; ++i) {
        AggregateDataPtr aggregate_data = nullptr;

        auto emplace_result = state.emplace_key(method.data, i, _agg_arena_pool);

        /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
        if (emplace_result.is_inserted()) {
            /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
            emplace_result.set_mapped(nullptr);

            aggregate_data = _agg_arena_pool.aligned_alloc(_total_size_of_aggregate_states,
                                                           _align_aggregate_states);
            _create_agg_status(aggregate_data);

            emplace_result.set_mapped(aggregate_data);
        } else
            aggregate_data = emplace_result.get_mapped();

        places[i] = aggregate_data;
        assert(places[i] != nullptr);
    }

    std::unique_ptr<char[]> deserialize_buffer(new char[_total_size_of_aggregate_states]);

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        if (_aggregate_evaluators[i]->is_merge()) {
            auto column = block->get_by_position(i + key_size).column;
            if (column->is_nullable()) {
                column = ((ColumnNullable *) column.get())->get_nested_column_ptr();
            }

            for (int j = 0; j < rows; ++j) {
                VectorBufferReader buffer_reader(((ColumnString *) (column.get()))->get_data_at(j));
                _create_agg_status(deserialize_buffer.get());

                _aggregate_evaluators[i]->function()->deserialize(
                        deserialize_buffer.get() + _offsets_of_aggregate_states[i], buffer_reader,
                        &_agg_arena_pool);

                _aggregate_evaluators[i]->function()->merge(
                        places.data()[j] + _offsets_of_aggregate_states[i],
                        deserialize_buffer.get() + _offsets_of_aggregate_states[i], &_agg_arena_pool);

                _destory_agg_status(deserialize_buffer.get());
            }
        } else {
            _aggregate_evaluators[i]->execute_batch_add(block, _offsets_of_aggregate_states[i],
                                                    places.data(), &_agg_arena_pool);
        }
    }
    return Status::OK();
}

void AggregationNode::_update_memusage_with_serialized_key() {
    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    auto& data = _agg_data.serialized->data;
    mem_tracker()->Consume(_agg_arena_pool.size() - _mem_usage_record.used_in_arena);
    mem_tracker()->Consume(data.get_buffer_size_in_bytes() - _mem_usage_record.used_in_state);
    _mem_usage_record.used_in_state = data.get_buffer_size_in_bytes();
    _mem_usage_record.used_in_arena = _agg_arena_pool.size();
}

void AggregationNode::_close_with_serialized_key() {
    DCHECK(_agg_data.serialized != nullptr);

    using Method = AggregationMethodSerialized<AggregatedDataWithStringKey>;
    using AggState = Method::State;

    auto& data = _agg_data.serialized->data;

    data.for_each_mapped([&](auto& mapped) {
        if (mapped) {
            _destory_agg_status(mapped);
            mapped = nullptr;
        }
    });
    release_tracker();
}

void AggregationNode::release_tracker() {
    mem_tracker()->Release(_mem_usage_record.used_in_state + _mem_usage_record.used_in_arena);
}

} // namespace doris::vectorized
