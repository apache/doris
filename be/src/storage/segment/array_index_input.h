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
#include "storage/field_type.h"
#include "storage/storage_layout.h"

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
// rows) to the raw index interface as one batch of documents. The storage
// cells of the elements are prepared in `buffers` when they cannot be borrowed
// and consumed before this returns. `first_item` locates the first element in
// `items`; `char_length` is the length CHAR items are padded to, as the segment
// stores them.
Status feed_array_index(IndexColumnWriter* writer, FieldType item_type, const IColumn& items,
                        size_t first_item, const ColumnOffset64& offsets,
                        const uint8_t* outer_nullmap, StorageBuffers& buffers,
                        size_t char_length = 0);

} // namespace doris::segment_v2
