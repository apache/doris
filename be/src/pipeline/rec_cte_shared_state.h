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

#include "dependency.h"
#include "pipeline/common/distinct_agg_utils.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

struct RecCTESharedState : public BasicSharedState {
    std::vector<vectorized::Block> blocks;
    vectorized::IColumn::Selector distinct_row;
    Dependency* source_dep = nullptr;
    vectorized::Arena arena;
    RuntimeProfile::Counter* hash_table_compute_timer = nullptr;
    RuntimeProfile::Counter* hash_table_emplace_timer = nullptr;
    RuntimeProfile::Counter* hash_table_input_counter = nullptr;

    std::unique_ptr<DistinctDataVariants> agg_data = nullptr;

    int current_round = 0;
    int last_round_offset = 0;
    int max_recursion_depth = 0;
    bool ready_to_return = false;

    void update_ready_to_return() {
        if (last_round_offset == blocks.size() || current_round + 1 >= max_recursion_depth) {
            ready_to_return = true;
        }
    }

    Status emplace_block(RuntimeState* state, vectorized::Block&& block) {
        if (agg_data) {
            auto num_rows = uint32_t(block.rows());
            vectorized::ColumnRawPtrs raw_columns;
            std::vector<vectorized::ColumnPtr> columns = block.get_columns_and_convert();
            for (auto& col : columns) {
                raw_columns.push_back(col.get());
            }

            std::visit(vectorized::Overload {
                               [&](std::monostate& arg) -> void {
                                   throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                                          "uninited hash table");
                               },
                               [&](auto& agg_method) -> void {
                                   SCOPED_TIMER(hash_table_compute_timer);
                                   using HashMethodType = std::decay_t<decltype(agg_method)>;
                                   using AggState = typename HashMethodType::State;

                                   AggState agg_state(raw_columns);
                                   agg_method.init_serialized_keys(raw_columns, num_rows);
                                   distinct_row.clear();

                                   size_t row = 0;
                                   auto creator = [&](const auto& ctor, auto& key, auto& origin) {
                                       HashMethodType::try_presis_key(key, origin, arena);
                                       ctor(key);
                                       distinct_row.push_back(row);
                                   };
                                   auto creator_for_null_key = [&]() {
                                       distinct_row.push_back(row);
                                   };

                                   SCOPED_TIMER(hash_table_emplace_timer);
                                   for (; row < num_rows; ++row) {
                                       agg_method.lazy_emplace(agg_state, row, creator,
                                                               creator_for_null_key);
                                   }
                                   COUNTER_UPDATE(hash_table_input_counter, num_rows);
                               }},
                       agg_data->method_variant);

            if (distinct_row.size() == block.rows()) {
                blocks.emplace_back(std::move(block));
            } else if (!distinct_row.empty()) {
                auto distinct_block = vectorized::MutableBlock(block.clone_empty());
                RETURN_IF_ERROR(block.append_to_block_by_selector(&distinct_block, distinct_row));
                blocks.emplace_back(distinct_block.to_block());
            }
        } else {
            blocks.emplace_back(std::move(block));
        }
        return Status::OK();
    }
};

#include "common/compile_check_end.h"
} // namespace doris::pipeline
