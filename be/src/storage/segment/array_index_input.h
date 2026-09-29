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
#include "core/column/column_vector.h"

namespace doris::segment_v2 {

class IndexColumnWriter;

// Rewrites the offsets of rows [row_pos, row_pos + num_rows) of an array
// column into `out`, the column the offset writer takes: num_rows + 1 entries
// with out[0] == base and out[i + 1] - out[i] row i's element count; returns
// the element count of the batch. One owner for every writer that stores or
// indexes array rows.
size_t rebase_offsets(const IColumn::Offsets64& offsets, size_t row_pos, size_t num_rows,
                      uint64_t base, ColumnOffset64* out);

// Feeds the array rows `offsets` describes (rebased: one entry more than the
// rows) to an index writer: the elements through add_array(), then the rows
// the array-level null map marks NULL through add_array_nulls(). `items` is
// the array's item column and `first_item` the index in it of the batch's
// first element.
Status feed_array_index(IndexColumnWriter* writer, const IColumn& items, size_t first_item,
                        const ColumnOffset64& offsets, const uint8_t* outer_nullmap);

} // namespace doris::segment_v2
