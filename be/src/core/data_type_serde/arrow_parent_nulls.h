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
#include <functional>
#include <memory>
#include <utility>

#include "common/status.h"

namespace arrow {
class Array;
} // namespace arrow

namespace doris {

// Returns `child` with the slots that a NULL row of `parent` hides marked NULL as well, for the
// parent rows [start, end); `child_slots(row)` gives the [begin, end) child indexes of a row.
// Arrow leaves the child values under a NULL list, fixed-size-list, map or struct slot
// unspecified, but element SerDes validate every non-NULL value they decode. For example, a
// FIXED_SIZE_LIST<DATE32, 1> with validity [1, 0, 1] may hold the days [0, 2932897, 1]: its rows
// are [1970-01-01], NULL, [1970-01-02], yet 2932897 (10000-01-01) is outside the Doris DATE range
// and would fail the whole batch. `child` itself is returned when no parent row in the range is
// NULL, so batches without NULL parents allocate nothing.
Status mask_arrow_children_hidden_by_null_parents(
        const arrow::Array& parent, int64_t start, int64_t end,
        const std::shared_ptr<arrow::Array>& child,
        const std::function<std::pair<int64_t, int64_t>(int64_t)>& child_slots,
        std::shared_ptr<arrow::Array>* masked);

} // namespace doris
