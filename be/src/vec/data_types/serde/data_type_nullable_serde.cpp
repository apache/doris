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

#include "data_type_nullable_serde.h"

#include "vec/columns/column_nullable.h"

namespace doris {

namespace vectorized {

Status DataTypeNullableSerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                                 int end) const {
    int row_count = end - start;
    auto& nullable_col = assert_cast<const ColumnNullable&>(column);
    auto& null_col = nullable_col.get_null_map_column();
    if (nullable_col.has_null(row_count)) {
        result.set_has_null(true);
        auto* null_map = result.mutable_null_map();
        null_map->Reserve(row_count);
        const auto* col = check_and_get_column<ColumnUInt8>(null_col);
        auto& data = col->get_data();
        null_map->Add(data.begin() + start, data.begin() + end);
    }
    return nested_serde->write_column_to_pb(nullable_col.get_nested_column(), result, start, end);
}

// read from PValues to column
Status DataTypeNullableSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& col = reinterpret_cast<ColumnNullable&>(column);
    auto& null_map_data = col.get_null_map_data();
    auto& nested = col.get_nested_column();
    if (Status st = nested_serde->read_column_from_pb(nested, arg); st != Status::OK()) {
        return st;
    }
    null_map_data.resize(nested.size());
    if (arg.has_null()) {
        for (int i = 0; i < arg.null_map_size(); ++i) {
            null_map_data[i] = arg.null_map(i);
        }
    } else {
        for (int i = 0; i < nested.size(); ++i) {
            null_map_data[i] = false;
        }
    }
    return Status::OK();
}
} // namespace vectorized
} // namespace doris
