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
#include "vec/data_types/data_type_array.h"

namespace doris::vectorized {

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
};

struct ColumnArrayMutableData {
public:
    MutableColumnPtr array_nested_col = nullptr;
    ColumnUInt8::Container* nested_nullmap_data = nullptr;
    MutableColumnPtr offsets_col = nullptr;
    ColumnArray::Offsets64* offsets_ptr = nullptr;
    IColumn* nested_col = nullptr;
};

bool extract_column_array_info(const IColumn& src, ColumnArrayExecutionData& data);

ColumnArrayMutableData create_mutable_data(const IColumn* nested_col, bool is_nullable);

MutableColumnPtr assemble_column_array(ColumnArrayMutableData& data);

// array[offset:length]
void slice_array(ColumnArrayMutableData& dst, ColumnArrayExecutionData& src,
                 const IColumn& offset_column, const IColumn* length_column);

} // namespace doris::vectorized
