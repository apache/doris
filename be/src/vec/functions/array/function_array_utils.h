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

#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

struct ColumnArrayMutableData {
public:
    MutableColumnPtr array_nested_col = nullptr;
    ColumnUInt8::Container* nested_nullmap_data = nullptr;
    MutableColumnPtr offsets_col = nullptr;
    ColumnArray::Offsets64* offsets_ptr = nullptr;
    IColumn* nested_col = nullptr;
};

struct ColumnArrayExecutionData {
public:
    void reset() {
        array_nullmap_data = nullptr;
        array_col = nullptr;
        offsets_ptr = nullptr;
        nested_nullmap_data = nullptr;
        nested_col = nullptr;
    }

public:
    const UInt8* array_nullmap_data = nullptr;
    const ColumnArray* array_col = nullptr;
    const ColumnArray::Offsets64* offsets_ptr = nullptr;
    const UInt8* nested_nullmap_data = nullptr;
    const IColumn* nested_col = nullptr;

    ColumnArrayMutableData to_mutable_data() const {
        ColumnArrayMutableData dst;
        dst.offsets_col = ColumnArray::ColumnOffsets::create();
        dst.offsets_ptr =
                &reinterpret_cast<ColumnArray::ColumnOffsets*>(dst.offsets_col.get())->get_data();
        dst.array_nested_col =
                ColumnNullable::create(nested_col->clone_empty(), ColumnUInt8::create());
        auto* nullable_col = reinterpret_cast<ColumnNullable*>(dst.array_nested_col.get());
        dst.nested_nullmap_data = &nullable_col->get_null_map_data();
        dst.nested_col = nullable_col->get_nested_column_ptr().get();
        for (size_t row = 0; row < offsets_ptr->size(); ++row) {
            dst.offsets_ptr->push_back((*offsets_ptr)[row]);
            size_t off = (*offsets_ptr)[row - 1];
            size_t len = (*offsets_ptr)[row] - off;
            for (int start = off; start < off + len; ++start) {
                if (nested_nullmap_data && nested_nullmap_data[start]) {
                    dst.nested_col->insert_default();
                    dst.nested_nullmap_data->push_back(1);
                } else {
                    dst.nested_col->insert_from(*nested_col, start);
                    dst.nested_nullmap_data->push_back(0);
                }
            }
        }
        return dst;
    }
};
bool extract_column_array_info(const IColumn& src, ColumnArrayExecutionData& data);

ColumnArrayMutableData create_mutable_data(const IColumn* nested_col, bool is_nullable);

MutableColumnPtr assemble_column_array(ColumnArrayMutableData& data);

// array[offset:length]
void slice_array(ColumnArrayMutableData& dst, ColumnArrayExecutionData& src,
                 const ColumnInt64& offset_column, const ColumnInt64* length_column);

using ColumnArrayExecutionDatas = std::vector<ColumnArrayExecutionData>;
} // namespace doris::vectorized
