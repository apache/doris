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

#include <algorithm>
#include <cstddef>

namespace doris {

// Lightweight value type that captures the dual row+byte budget for block
// output sizing.  Every operator that needs to respect the adaptive batch
// size feature can construct a BlockBudget from RuntimeState's batch_size()
// and preferred_block_size_bytes() and use the helper methods instead of
// reimplementing the same row/byte logic inline.
//
// Typical usage:
//   BlockBudget budget(state->batch_size(), state->preferred_block_size_bytes());
//   size_t eff = budget.effective_max_rows(estimated_row_bytes);
//   while (budget.within_budget(block.rows(), block.bytes())) { ... }
//
struct BlockBudget {
    size_t max_rows;
    size_t max_bytes; // byte budget from preferred_block_size_bytes(), 0 means disabled

    BlockBudget(size_t max_rows_, size_t max_bytes_) : max_rows(max_rows_), max_bytes(max_bytes_) {}

    // Pre-compute effective row limit from an estimated average row byte size.
    // When max_bytes == 0 or estimated_row_bytes == 0, returns max_rows.
    // Always returns at least 1.
    size_t effective_max_rows(size_t estimated_row_bytes) const {
        if (max_bytes > 0 && estimated_row_bytes > 0) {
            size_t bytes_limit = max_bytes / estimated_row_bytes;
            return std::max(size_t(1), std::min(max_rows, bytes_limit));
        }
        return max_rows;
    }

    // Check if a block with the given rows/bytes is still within budget.
    // Use this in loop *continuation* conditions (while/for).
    bool within_budget(size_t rows, size_t bytes) const {
        return rows < max_rows && (max_bytes == 0 || bytes < max_bytes);
    }

    // Check if a block with the given rows/bytes has exceeded the budget.
    // Use this in loop *break* conditions.
    bool exceeded(size_t rows, size_t bytes) const {
        return rows >= max_rows || (max_bytes > 0 && bytes >= max_bytes);
    }

    // Compute how many more rows can be added to a block that currently
    // has current_rows rows and current_bytes bytes, respecting both the
    // row cap and the byte budget.
    // The 3-arg overload accepts an explicit estimated_row_bytes (useful when
    // the estimate comes from a different source, e.g. a child block).
    // The 2-arg overload derives the estimate from current_bytes / current_rows.
    // Returns 0 when the block is already at or over budget.
    size_t remaining_rows(size_t current_rows, size_t current_bytes,
                          size_t estimated_row_bytes) const {
        size_t row_capacity = (current_rows < max_rows) ? (max_rows - current_rows) : 0;
        if (max_bytes > 0 && estimated_row_bytes > 0) {
            if (current_bytes >= max_bytes) {
                return 0;
            }
            size_t byte_capacity = (max_bytes - current_bytes) / estimated_row_bytes;
            row_capacity = std::min(row_capacity, byte_capacity);
        }
        return row_capacity;
    }

    size_t remaining_rows(size_t current_rows, size_t current_bytes) const {
        size_t estimated =
                (current_rows > 0 && current_bytes > 0) ? (current_bytes / current_rows) : 0;
        return remaining_rows(current_rows, current_bytes, estimated);
    }
};

} // namespace doris
