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

#include "exec/common/ungroupby_agg_context.h"

#include "common/exception.h"
#include "exec/common/agg_context_utils.h"
#include "exec/common/util.hpp"
#include "exprs/vectorized_agg_fn.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

// ==================== Constructor / Destructor ====================

UngroupByAggContext::UngroupByAggContext(std::vector<AggFnEvaluator*> agg_evaluators,
                                         Sizes agg_state_offsets, size_t total_agg_state_size,
                                         size_t agg_state_alignment)
        : AggContext(std::move(agg_evaluators), std::move(agg_state_offsets),
                     total_agg_state_size, agg_state_alignment) {}

UngroupByAggContext::~UngroupByAggContext() = default;

// ==================== Profile ====================

void UngroupByAggContext::init_profile(RuntimeProfile* profile) {
    _build_timer = ADD_TIMER(profile, "BuildTime");
    _merge_timer = ADD_TIMER(profile, "MergeTime");
    _deserialize_data_timer = ADD_TIMER(profile, "DeserializeAndMergeTime");
    _get_results_timer = ADD_TIMER(profile, "GetResultsTime");

    auto* memory_usage =
            profile->create_child("MemoryUsage", true, true);
    _memory_used_counter = profile->get_counter("MemoryUsage");
    _memory_usage_arena = ADD_COUNTER(memory_usage, "Arena", TUnit::BYTES);
}

// ==================== Agg state management ====================

Status UngroupByAggContext::_create_agg_state() {
    DCHECK(!_agg_state_created);
    _agg_state_data = reinterpret_cast<AggregateDataPtr>(
            _alloc_arena.aligned_alloc(_total_agg_state_size, _agg_state_alignment));

    for (int i = 0; i < _agg_evaluators.size(); ++i) {
        try {
            _agg_evaluators[i]->create(_agg_state_data + _agg_state_offsets[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                _agg_evaluators[j]->destroy(_agg_state_data + _agg_state_offsets[j]);
            }
            throw;
        }
    }

    _agg_state_created = true;
    return Status::OK();
}

void UngroupByAggContext::_destroy_agg_state() {
    if (!_agg_state_created) {
        return;
    }
    for (int i = 0; i < _agg_evaluators.size(); ++i) {
        _agg_evaluators[i]->function()->destroy(_agg_state_data + _agg_state_offsets[i]);
    }
    _agg_state_created = false;
}

void UngroupByAggContext::close() {
    _destroy_agg_state();
}

// ==================== Aggregation execution (Sink side) ====================

Status UngroupByAggContext::update(Block* block) {
    // Create agg state on first call (lazy init to match original behavior, which creates
    // state in open() - here we ensure it's created before first use).
    if (!_agg_state_created) {
        RETURN_IF_ERROR(_create_agg_state());
    }
    input_num_rows += block->rows();

    DCHECK(_agg_state_data != nullptr);
    SCOPED_TIMER(_build_timer);
    memory_usage_last_executing = 0;
    SCOPED_PEAK_MEM(&memory_usage_last_executing);

    for (int i = 0; i < _agg_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_agg_evaluators[i]->execute_single_add(
                block, _agg_state_data + _agg_state_offsets[i], _agg_arena));
    }
    return Status::OK();
}

Status UngroupByAggContext::merge(Block* block) {
    if (!_agg_state_created) {
        RETURN_IF_ERROR(_create_agg_state());
    }
    input_num_rows += block->rows();

    SCOPED_TIMER(_merge_timer);
    DCHECK(_agg_state_data != nullptr);
    memory_usage_last_executing = 0;
    SCOPED_PEAK_MEM(&memory_usage_last_executing);

    for (int i = 0; i < _agg_evaluators.size(); ++i) {
        if (_agg_evaluators[i]->is_merge()) {
            int col_id = _get_slot_column_id(_agg_evaluators[i]);
            auto column = block->get_by_position(col_id).column;

            SCOPED_TIMER(_deserialize_data_timer);
            _agg_evaluators[i]->function()->deserialize_and_merge_from_column(
                    _agg_state_data + _agg_state_offsets[i], *column, _agg_arena);
        } else {
            RETURN_IF_ERROR(_agg_evaluators[i]->execute_single_add(
                    block, _agg_state_data + _agg_state_offsets[i], _agg_arena));
        }
    }
    return Status::OK();
}

// ==================== Result output (Source side) ====================

Status UngroupByAggContext::serialize(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_get_results_timer);

    // Ensure agg state exists even if no data flowed through the sink.
    if (!_agg_state_created) {
        RETURN_IF_ERROR(_create_agg_state());
    }

    // If no data was ever fed, return empty result.
    if (UNLIKELY(input_num_rows == 0)) {
        *eos = true;
        return Status::OK();
    }
    block->clear();

    DCHECK(_agg_state_data != nullptr);
    size_t agg_size = _agg_evaluators.size();

    MutableColumns value_columns(agg_size);
    std::vector<DataTypePtr> data_types(agg_size);

    for (int i = 0; i < agg_size; ++i) {
        data_types[i] = _agg_evaluators[i]->function()->get_serialized_type();
        value_columns[i] = _agg_evaluators[i]->function()->create_serialize_column();
    }

    for (int i = 0; i < agg_size; ++i) {
        _agg_evaluators[i]->function()->serialize_without_key_to_column(
                _agg_state_data + _agg_state_offsets[i], *value_columns[i]);
    }

    {
        ColumnsWithTypeAndName data_with_schema;
        for (int i = 0; i < agg_size; ++i) {
            ColumnWithTypeAndName column_with_schema = {nullptr, data_types[i], ""};
            data_with_schema.push_back(std::move(column_with_schema));
        }
        *block = Block(data_with_schema);
    }

    block->set_columns(std::move(value_columns));
    *eos = true;
    return Status::OK();
}

void UngroupByAggContext::set_finalize_output(const RowDescriptor& row_desc) {
    _finalize_row_desc = &row_desc;
}

Status UngroupByAggContext::finalize(RuntimeState* state, Block* block, bool* eos) {
    // Ensure agg state exists even if no data flowed through the sink.
    // Without GROUP BY, aggregation always produces one row (e.g., COUNT(*) → 0).
    if (!_agg_state_created) {
        RETURN_IF_ERROR(_create_agg_state());
    }
    DCHECK(_agg_state_data != nullptr);
    DCHECK(_finalize_row_desc != nullptr);
    block->clear();

    *block = VectorizedUtils::create_empty_columnswithtypename(*_finalize_row_desc);
    size_t agg_size = _agg_evaluators.size();

    MutableColumns columns(agg_size);
    std::vector<DataTypePtr> data_types(agg_size);
    for (int i = 0; i < agg_size; ++i) {
        data_types[i] = _agg_evaluators[i]->function()->get_return_type();
        columns[i] = data_types[i]->create_column();
    }

    for (int i = 0; i < agg_size; ++i) {
        auto column = columns[i].get();
        _agg_evaluators[i]->insert_result_info(_agg_state_data + _agg_state_offsets[i], column);
    }

    const auto& block_schema = block->get_columns_with_type_and_name();
    DCHECK_EQ(block_schema.size(), columns.size());
    for (int i = 0; i < block_schema.size(); ++i) {
        const auto column_type = block_schema[i].type;
        if (!column_type->equals(*data_types[i])) {
            if (column_type->get_primitive_type() != TYPE_ARRAY) {
                if (!column_type->is_nullable() || data_types[i]->is_nullable() ||
                    !remove_nullable(column_type)->equals(*data_types[i])) {
                    return Status::InternalError(
                            "column_type not match data_types, column_type={}, data_types={}",
                            column_type->get_name(), data_types[i]->get_name());
                }
            }

            // Result of operator is nullable, but aggregate function result is not nullable.
            // This happens when: 1) no group by, 2) input empty, 3) all input columns not nullable.
            if (column_type->is_nullable() && !data_types[i]->is_nullable()) {
                ColumnPtr ptr = std::move(columns[i]);
                // Unless count, other aggregate functions on empty set should produce null.
                ptr = make_nullable(ptr, input_num_rows == 0);
                columns[i] = ptr->assume_mutable();
            }
        }
    }

    block->set_columns(std::move(columns));
    *eos = true;
    return Status::OK();
}

// ==================== Utilities ====================

void UngroupByAggContext::update_memusage() {
    int64_t arena_memory_usage = _agg_arena.size();
    if (_memory_used_counter) {
        COUNTER_SET(_memory_used_counter, arena_memory_usage);
    }
    if (_memory_usage_arena) {
        COUNTER_SET(_memory_usage_arena, arena_memory_usage);
    }
}

size_t UngroupByAggContext::memory_usage() const {
    return _agg_arena.size();
}

int UngroupByAggContext::_get_slot_column_id(const AggFnEvaluator* evaluator) {
    return agg_context_utils::get_slot_column_id(evaluator);
}

} // namespace doris
