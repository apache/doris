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

#include <cstddef>

#include "core/column/column.h"

namespace doris {

// Size limits of one output Block of a read, shared by the iterators that fill output Blocks
// row by row or run by run.
struct OutputBlockLimits {
    // Row ceiling of an output Block.
    size_t max_rows = 0;
    // Byte budget of an output Block. The budget is only consulted every
    // BYTE_BUDGET_CHECK_INTERVAL_ROWS rows because measuring the columns costs a full walk.
    size_t preferred_bytes = 0;

    static constexpr size_t BYTE_BUDGET_CHECK_INTERVAL_ROWS = 64;

    // Whether `columns` holds at least `preferred_bytes`. Always false when adaptive batch
    // sizing is disabled or no budget is configured.
    bool reached_byte_budget(const MutableColumns& columns) const;

    // reached_byte_budget() at the row-count checkpoints only, so the callers all agree on
    // the measuring cadence.
    bool reached_byte_budget_at(size_t rows, const MutableColumns& columns) const {
        return rows % BYTE_BUDGET_CHECK_INTERVAL_ROWS == 0 && reached_byte_budget(columns);
    }
};

} // namespace doris
