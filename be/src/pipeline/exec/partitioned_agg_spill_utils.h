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

#include <vector>

#include "common/status.h"
#include "pipeline/dependency.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::pipeline {

// Null key hash value used for partitioning null keys consistently.
inline constexpr uint32_t kAggSpillNullKeyHash = kSpillFanout - 1;

// Common utilities for partitioned aggregation spill operations.
// Used by both PartitionedAggSinkLocalState and PartitionedAggLocalState (Source).
namespace agg_spill_utils {

// Serialize aggregation hash table data (keys + values) into a Block.
// The output block format: [__spill_hash (Int32), key columns..., value columns...]
//
// Parameters:
//   - context: Hash table context for inserting keys into columns
//   - keys: Vector of hash table keys
//   - hashes: Pre-computed hash values for each key
//   - values: Aggregate data pointers for each key
//   - null_key_data: Optional null key's aggregate data (nullptr if no null key)
//   - in_mem_state: Shared state containing aggregate evaluators and probe expressions
//   - key_columns: Mutable columns to receive key data (will be moved into output block)
//   - value_columns: Mutable columns to receive serialized aggregate values (will be moved)
//   - value_data_types: Data types for value columns
//   - out_block: Output block to receive serialized data
//
// Note: After calling this function, key_columns and value_columns will be empty
//       (their contents moved into out_block).
template <typename HashTableCtxType, typename KeyType>
Status serialize_agg_data_to_block(
        HashTableCtxType& context, std::vector<KeyType>& keys, std::vector<uint32_t>& hashes,
        std::vector<vectorized::AggregateDataPtr>& values,
        const vectorized::AggregateDataPtr null_key_data, AggSharedState* in_mem_state,
        vectorized::MutableColumns& key_columns, vectorized::MutableColumns& value_columns,
        const vectorized::DataTypes& value_data_types, vectorized::Block& out_block) {
    DCHECK_EQ(keys.size(), hashes.size());

    // Step 1: Insert keys into key columns
    context.insert_keys_into_columns(keys, key_columns, static_cast<uint32_t>(keys.size()));

    if (null_key_data) {
        // Only one key column can wrap null key
        CHECK(key_columns.size() == 1);
        CHECK(key_columns[0]->is_nullable());
        key_columns[0]->insert_data(nullptr, 0);
        values.emplace_back(null_key_data);
    }

    // Step 2: Add __spill_hash column for multi-level partitioning
    auto hash_col = vectorized::ColumnInt32::create();
    auto& hash_data = assert_cast<vectorized::ColumnInt32&>(*hash_col).get_data();
    hash_data.reserve(hashes.size() + (null_key_data ? 1 : 0));
    for (auto h : hashes) {
        hash_data.emplace_back(h);
    }
    if (null_key_data) {
        hash_data.emplace_back(kAggSpillNullKeyHash);
    }

    out_block.insert(vectorized::ColumnWithTypeAndName {
            std::move(hash_col), std::make_shared<vectorized::DataTypeInt32>(), "__spill_hash"});

    // Step 3: Serialize aggregate values into value columns
    for (size_t i = 0; i < in_mem_state->aggregate_evaluators.size(); ++i) {
        in_mem_state->aggregate_evaluators[i]->function()->serialize_to_column(
                values, in_mem_state->offsets_of_aggregate_states[i], value_columns[i],
                values.size());
    }

    // Step 4: Build key block with schema
    vectorized::ColumnsWithTypeAndName key_columns_with_schema;
    for (size_t i = 0; i < key_columns.size(); ++i) {
        key_columns_with_schema.emplace_back(std::move(key_columns[i]),
                                             in_mem_state->probe_expr_ctxs[i]->root()->data_type(),
                                             in_mem_state->probe_expr_ctxs[i]->root()->expr_name());
    }

    // Step 5: Build value block with schema
    vectorized::ColumnsWithTypeAndName value_columns_with_schema;
    for (size_t i = 0; i < value_columns.size(); ++i) {
        value_columns_with_schema.emplace_back(
                std::move(value_columns[i]), value_data_types[i],
                in_mem_state->aggregate_evaluators[i]->function()->get_name());
    }

    // Step 6: Assemble final block: __spill_hash + keys + values
    for (const auto& column : key_columns_with_schema) {
        out_block.insert(column);
    }
    for (const auto& column : value_columns_with_schema) {
        out_block.insert(column);
    }

    return Status::OK();
}

// Initialize spill columns based on in-memory shared state.
// Creates empty mutable columns for keys and values.
inline void init_spill_columns(AggSharedState* in_mem_state,
                               vectorized::MutableColumns& key_columns,
                               vectorized::MutableColumns& value_columns,
                               vectorized::DataTypes& value_data_types) {
    // Initialize key columns
    key_columns.resize(in_mem_state->probe_expr_ctxs.size());
    for (size_t i = 0; i < in_mem_state->probe_expr_ctxs.size(); ++i) {
        key_columns[i] = in_mem_state->probe_expr_ctxs[i]->root()->data_type()->create_column();
    }

    // Initialize value columns and data types
    value_columns.resize(in_mem_state->aggregate_evaluators.size());
    value_data_types.resize(in_mem_state->aggregate_evaluators.size());
    for (size_t i = 0; i < in_mem_state->aggregate_evaluators.size(); ++i) {
        value_data_types[i] =
                in_mem_state->aggregate_evaluators[i]->function()->get_serialized_type();
        value_columns[i] =
                in_mem_state->aggregate_evaluators[i]->function()->create_serialize_column();
    }
}

// Reset spill columns after each batch serialization.
// Recreates empty mutable columns for the next batch.
inline void reset_spill_columns(AggSharedState* in_mem_state,
                                vectorized::MutableColumns& key_columns,
                                vectorized::MutableColumns& value_columns,
                                const vectorized::DataTypes& value_data_types,
                                vectorized::Block& block) {
    block.clear();

    // Recreate key columns
    key_columns.clear();
    key_columns.resize(in_mem_state->probe_expr_ctxs.size());
    for (size_t i = 0; i < in_mem_state->probe_expr_ctxs.size(); ++i) {
        key_columns[i] = in_mem_state->probe_expr_ctxs[i]->root()->data_type()->create_column();
    }

    // Recreate value columns
    value_columns.clear();
    value_columns.resize(in_mem_state->aggregate_evaluators.size());
    for (size_t i = 0; i < in_mem_state->aggregate_evaluators.size(); ++i) {
        value_columns[i] =
                in_mem_state->aggregate_evaluators[i]->function()->create_serialize_column();
    }
}

} // namespace agg_spill_utils
} // namespace doris::pipeline
