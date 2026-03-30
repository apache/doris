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

#include "common/exception.h"
#include "core/block/block.h"
#include "exec/common/agg_utils.h"
#include "exec/common/template_helpers.hpp"
#include "exprs/vectorized_agg_fn.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"

/// Utility functions for aggregation context result output.
/// Eliminates duplicate column-preparation and block-assembly patterns
/// across GroupByAggContext, InlineCountAggContext, and UngroupByAggContext.
namespace doris::agg_context_utils {

/// Visit the hash table method variant, throwing on uninitialized (monostate).
/// For void-returning functors: visit_agg_method(data, [&](auto& m) { ... });
/// For non-void: visit_agg_method<RetType>(data, [&](auto& m) -> RetType { ... });
template <typename ReturnType = void, typename Func>
ReturnType visit_agg_method(AggregatedDataVariants& data, Func&& func) {
    return std::visit(Overload {[](std::monostate&) -> ReturnType {
                                    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                           "uninited hash table");
                                },
                                std::forward<Func>(func)},
                      data.method_variant);
}

template <typename ReturnType = void, typename Func>
ReturnType visit_agg_method(const AggregatedDataVariants& data, Func&& func) {
    return std::visit(Overload {[](const std::monostate&) -> ReturnType {
                                    throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                           "uninited hash table");
                                },
                                std::forward<Func>(func)},
                      data.method_variant);
}

/// Null-safe COUNTER_SET: only calls set() when counter is non-null.
inline void set_counter_if(RuntimeProfile::Counter* counter, int64_t val) {
    if (counter) {
        COUNTER_SET(counter, val);
    }
}

/// Take existing columns from block [start, start+count) if mem_reuse,
/// otherwise create new columns via create_fn(index).
///
/// @param block       the output block
/// @param mem_reuse   whether the block supports memory reuse
/// @param start       starting column position in the block
/// @param count       number of columns to take or create
/// @param create_fn   callable(size_t i) -> MutableColumnPtr for non-reuse path
/// @return MutableColumns with count elements
template <typename CreateFn>
MutableColumns take_or_create_columns(Block* block, bool mem_reuse, size_t start, size_t count,
                                      CreateFn&& create_fn) {
    MutableColumns columns;
    columns.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        if (mem_reuse) {
            columns.emplace_back(std::move(*block->get_by_position(start + i).column).mutate());
        } else {
            columns.emplace_back(create_fn(i));
        }
    }
    return columns;
}

/// Assemble a finalized output block from schema + key/value columns (non mem_reuse path).
///
/// @param block                the output block to overwrite
/// @param columns_with_schema  the target schema
/// @param key_columns          key columns to place at [0, key_size)
/// @param value_columns        value columns to place at [key_size, ...)
/// @param key_size             number of key columns
inline void assemble_finalized_output(Block* block,
                                      const ColumnsWithTypeAndName& columns_with_schema,
                                      MutableColumns& key_columns, MutableColumns& value_columns,
                                      size_t key_size) {
    *block = columns_with_schema;
    MutableColumns columns(block->columns());
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i < key_size) {
            columns[i] = std::move(key_columns[i]);
        } else {
            columns[i] = std::move(value_columns[i - key_size]);
        }
    }
    block->set_columns(std::move(columns));
}

/// Build a serialized output block from key expr types + value data types (non mem_reuse path).
///
/// @param block          the output block to overwrite
/// @param key_columns    key columns (moved into the new block)
/// @param key_exprs      groupby expression contexts (for type and name)
/// @param value_columns  value columns (moved into the new block)
/// @param value_types    data types for value columns
inline void build_serialized_output_block(Block* block, MutableColumns& key_columns,
                                          const VExprContextSPtrs& key_exprs,
                                          MutableColumns& value_columns,
                                          const DataTypes& value_types) {
    ColumnsWithTypeAndName schema;
    schema.reserve(key_columns.size() + value_columns.size());
    for (size_t i = 0; i < key_columns.size(); ++i) {
        schema.emplace_back(std::move(key_columns[i]), key_exprs[i]->root()->data_type(),
                            key_exprs[i]->root()->expr_name());
    }
    for (size_t i = 0; i < value_columns.size(); ++i) {
        schema.emplace_back(std::move(value_columns[i]), value_types[i], "");
    }
    *block = Block(schema);
}

/// Overload for streaming agg passthrough: keys come from ColumnRawPtrs (clone + resize).
///
/// @param block          the output block to overwrite (via swap)
/// @param key_columns    raw key column pointers (will be clone_resized)
/// @param rows           number of rows to clone
/// @param key_exprs      groupby expression contexts (for type and name)
/// @param value_columns  value columns (moved into the new block)
/// @param value_types    data types for value columns
inline void build_serialized_output_block(Block* block, ColumnRawPtrs& key_columns, uint32_t rows,
                                          const VExprContextSPtrs& key_exprs,
                                          MutableColumns& value_columns,
                                          const DataTypes& value_types) {
    ColumnsWithTypeAndName schema;
    schema.reserve(key_columns.size() + value_columns.size());
    for (size_t i = 0; i < key_columns.size(); ++i) {
        schema.emplace_back(key_columns[i]->clone_resized(rows), key_exprs[i]->root()->data_type(),
                            key_exprs[i]->root()->expr_name());
    }
    for (size_t i = 0; i < value_columns.size(); ++i) {
        schema.emplace_back(std::move(value_columns[i]), value_types[i], "");
    }
    block->swap(Block(schema));
}

/// Get the input column id from an evaluator's single SlotRef input expression.
/// Unified version used by both GroupByAggContext and UngroupByAggContext.
inline int get_slot_column_id(const AggFnEvaluator* evaluator) {
    auto ctxs = evaluator->input_exprs_ctxs();
    DCHECK(ctxs.size() == 1 && ctxs[0]->root()->is_slot_ref())
            << "input_exprs_ctxs is invalid, input_exprs_ctx[0]="
            << ctxs[0]->root()->debug_string();
    return static_cast<VSlotRef*>(ctxs[0]->root().get())->column_id();
}

} // namespace doris::agg_context_utils
