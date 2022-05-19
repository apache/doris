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

#include "global_dict.h"

#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

GlobalDict::GlobalDict(const std::vector<std::string>& data) : Dict(data) {
    _dict_data = data;
    for (const auto& val : _dict_data) {
        StringValue v {val.data(), (int)val.size()};
        insert_value(v);
    }
}

bool GlobalDict::encode(ColumnWithTypeAndName& col) {
    // encode method is only used by unit test
    assert(col.column && col.type);
    size_t cardin = cardinality();

    DataTypePtr type;
    if (cardin <= UINT8_MAX) {
        type = std::make_shared<DataTypeUInt8>();
    } else if (cardin <= UINT16_MAX) {
        type = std::make_shared<DataTypeUInt16>();
    } else {
        type = std::make_shared<DataTypeInt32>();
    }

    const ColumnString* column_str;
    const UInt8* null_map = nullptr;
    if (col.type->is_nullable()) {
        auto nullable_column = assert_cast<const vectorized::ColumnNullable*>(col.column.get());
        column_str =
                assert_cast<const ColumnString*>(nullable_column->get_nested_column_ptr().get());
        null_map = nullable_column->get_null_map_data().data();
    } else {
        column_str = assert_cast<const ColumnString*>(col.column.get());
    }

    const char* chars = (const char*)column_str->get_chars().data();
    const int32_t* offsets = (const int32_t*)column_str->get_offsets().data();
    std::vector<int32_t> indices;
    int32_t dict_code;
    size_t row_num = col.column->size();

    for (size_t i = 0; i < row_num; ++i) {
        dict_code = find_code({chars + offsets[i - 1], offsets[i] - offsets[i - 1] - 1});
        if (dict_code < 0) {
            if (!null_map || !null_map[i]) {
                return false;
            } else {
                //use 0 for null value
                dict_code = 0;
            }
        }
        indices.push_back(dict_code);
    }

    MutableColumnPtr encoded_column = type->create_column();
    encoded_column->resize(row_num);
    auto encoded_column_data = encoded_column->get_raw_data().data;
    if (cardin <= UINT8_MAX) {
        UInt8* p = (UInt8*)encoded_column_data;
        for (auto index : indices) {
            assert(index <= UINT8_MAX);
            *p = (UInt8)index;
            ++p;
        }
    } else if (cardin <= UINT16_MAX) {
        UInt16* p = (UInt16*)encoded_column_data;
        for (auto index : indices) {
            assert(index <= UINT16_MAX);
            *p = (UInt16)index;
            ++p;
        }
    } else {
        Int32* p = (Int32*)encoded_column_data;
        for (auto index : indices) {
            *p = index;
            ++p;
        }
    }

    if (null_map) {
        auto nullable_column = assert_cast<const ColumnNullable*>(col.column.get());
        col.column = ColumnNullable::create(std::move(encoded_column),
                                            nullable_column->get_null_map_column().get_ptr());
        col.type = std::make_shared<DataTypeNullable>(type);
    } else {
        col.column = std::move(encoded_column);
        col.type = type;
    }

    return true;
}

bool GlobalDict::decode(ColumnWithTypeAndName& col) {
    assert(col.column && col.type);

    const char* encoded_column_data = nullptr;
    if (col.type->is_nullable()) {
        auto nullable_column = assert_cast<const vectorized::ColumnNullable*>(col.column.get());
        encoded_column_data = nullable_column->get_nested_column_ptr()->get_raw_data().data;
    } else {
        encoded_column_data = col.column->get_raw_data().data;
    }

    size_t row_num = col.column->size();
    size_t value_num = dict_value_num();
    auto decoded_column = ColumnString::create();
    DataTypePtr type(std::make_shared<DataTypeString>());
    assert((col.type->is_nullable() &&
            col.type->equals(DataTypeNullable(std::make_shared<DataTypeInt16>()))) ||
           (!col.type->is_nullable() && col.type->equals(DataTypeInt16())));
    Int16* p = (Int16*)encoded_column_data;
    for (size_t i = 0; i < row_num; ++i) {
        //in nullable column, if the item is null, *p can be any value, skip it
        if (UNLIKELY(*p >= value_num || *p < 0)) {
            continue;
        }
        const StringValue& val = get_value(*p);
        decoded_column->insert_data(val.ptr, val.len);
        ++p;
    }

    if (col.type->is_nullable()) {
        auto nullable_column = assert_cast<const ColumnNullable*>(col.column.get());
        col.column = ColumnNullable::create(std::move(decoded_column),
                                            nullable_column->get_null_map_column().get_ptr());
        col.type = std::make_shared<DataTypeNullable>(type);
    } else {
        col.column = std::move(decoded_column);
        col.type = type;
    }

    return true;
}

} // namespace doris::vectorized