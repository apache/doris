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
#include <cstdint>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "storage/index/index_writer.h"
#include "storage/segment/array_index_input.h"

namespace doris::segment_v2 {

// Feeds the rows of an array column to an index writer the way
// ArrayColumnWriter does: the elements through add_array(), the rows the
// array-level null map marks NULL through add_array_nulls().
inline Status feed_array_rows(IndexColumnWriter& writer, const IColumn& array_column,
                              size_t num_rows) {
    const uint8_t* outer_nullmap = nullptr;
    const IColumn& nested = peel_nullable(array_column, 0, &outer_nullmap);
    const auto* col_array = check_and_get_column<ColumnArray>(&nested);
    if (col_array == nullptr) {
        return Status::InternalError("expected ColumnArray, got {}", nested.get_name());
    }
    auto offsets = ColumnOffset64::create();
    rebase_offsets(col_array->get_offsets(), 0, num_rows, 0, offsets.get());
    return feed_array_index(&writer, col_array->get_data(), 0, *offsets, outer_nullmap);
}

} // namespace doris::segment_v2
