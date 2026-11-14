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

#include "storage/sparse_flush_output.h"

#include <algorithm>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"

namespace doris {

Status SparseFlushOutput::_validate(const Columns& columns) {
    for (size_t cid = 0; cid < columns.size(); ++cid) {
        if (!columns[cid]) {
            continue;
        }
        const auto& sparse = *columns[cid];
        if (!sparse.values || sparse.rowids.size() != sparse.values->size()) {
            return Status::InvalidArgument(
                    "invalid sparse flush column cid={}: rowids={}, values={}", cid,
                    sparse.rowids.size(), sparse.values ? sparse.values->size() : 0);
        }
        uint32_t previous = 0;
        bool first = true;
        for (uint32_t rowid : sparse.rowids) {
            if (rowid >= sparse.row_count || (!first && rowid <= previous)) {
                return Status::InvalidArgument("invalid sparse rowid cid={}, row={}, rows={}", cid,
                                               rowid, sparse.row_count);
            }
            previous = rowid;
            first = false;
        }
    }
    return Status::OK();
}

Status SparseFlushOutput::create(Columns columns,
                                 std::shared_ptr<const SparseFlushOutput>* result) {
    if (result == nullptr) {
        return Status::InvalidArgument("sparse flush output result is null");
    }
    result->reset();
    RETURN_IF_ERROR(_validate(columns));
    *result = std::shared_ptr<const SparseFlushOutput>(new SparseFlushOutput(std::move(columns)));
    return Status::OK();
}

Status SparseFlushOutput::materialize(size_t cid, size_t row_pos, size_t num_rows,
                                      const IColumn& prototype, MutableColumnPtr* result) const {
    if (!has_column(cid)) {
        return Status::InvalidArgument("column {} has no sparse flush attachment", cid);
    }
    const auto& sparse = *_columns[cid];
    if (!prototype.is_nullable()) {
        return Status::InvalidArgument("sparse flush prototype is not nullable, cid={}", cid);
    }
    if (row_pos > sparse.row_count || num_rows > sparse.row_count - row_pos) {
        return Status::InvalidArgument(
                "invalid sparse flush range cid={}, pos={}, rows={}, total={}", cid, row_pos,
                num_rows, sparse.row_count);
    }
    auto dense = prototype.clone_empty();
    auto& nullable = assert_cast<ColumnNullable&>(*dense);
    nullable.reserve(num_rows);
    const uint64_t range_end = static_cast<uint64_t>(row_pos) + num_rows;
    auto value_it = std::ranges::lower_bound(sparse.rowids, row_pos);
    size_t value_idx = std::distance(sparse.rowids.begin(), value_it);
    for (uint64_t row = row_pos; row < range_end; ++row) {
        if (value_idx < sparse.rowids.size() && sparse.rowids[value_idx] == row) {
            nullable.get_nested_column().insert_from(*sparse.values, value_idx++);
            nullable.get_null_map_data().push_back(0);
        } else {
            nullable.insert_default();
        }
    }
    *result = std::move(dense);
    return Status::OK();
}

} // namespace doris
