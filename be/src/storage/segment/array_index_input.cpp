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

#include "storage/segment/array_index_input.h"

#include "common/cast_set.h"
#include "storage/index/index_writer.h"

namespace doris::segment_v2 {

size_t rebase_offsets(const IColumn::Offsets64& offsets, size_t row_pos, size_t num_rows,
                      uint64_t base, ColumnOffset64* out) {
    // Row i starts at offsets[i - 1]; offsets[-1] is 0 by the array's padding.
    const auto start_of = [&](size_t row) {
        return offsets[cast_set<ssize_t, size_t, false>(row) - 1];
    };
    const uint64_t first = start_of(row_pos);
    auto& data = out->get_data();
    data.clear();
    data.reserve(num_rows + 1);
    for (size_t i = 0; i <= num_rows; ++i) {
        data.push_back(start_of(row_pos + i) - first + base);
    }
    return data.back() - base;
}

Status feed_array_index(IndexColumnWriter* writer, const IColumn& items, size_t first_item,
                        const ColumnOffset64& offsets, const uint8_t* outer_nullmap) {
    DCHECK_GE(offsets.size(), 1);
    const size_t num_rows = offsets.size() - 1;
    RETURN_IF_ERROR(writer->add_array(items, first_item, offsets.get_data().data(), num_rows));
    if (outer_nullmap != nullptr) {
        RETURN_IF_ERROR(writer->add_array_nulls(outer_nullmap, num_rows));
    }
    return Status::OK();
}

} // namespace doris::segment_v2
