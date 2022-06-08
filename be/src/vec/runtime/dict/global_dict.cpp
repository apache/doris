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
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

GlobalDict::GlobalDict(const std::vector<std::string>& data) : Dict(data) {
    _dict_data = data;
    for (const auto& val : _dict_data) {
        StringValue v {val.data(), (int)val.size()};
        insert_value(v);
    }
}

bool GlobalDict::decode(const ColumnWithTypeAndName& src, ColumnWithTypeAndName& dst) {
    assert(src.column && src.type);

    const char* encoded_column_data = nullptr;
    if (src.type->is_nullable()) {
        auto nullable_column = assert_cast<const vectorized::ColumnNullable*>(src.column.get());
        encoded_column_data = nullable_column->get_nested_column_ptr()->get_raw_data().data;
    } else {
        encoded_column_data = src.column->get_raw_data().data;
    }

    size_t row_num = src.column->size();
    size_t value_num = dict_value_num();
    auto decoded_column = ColumnString::create();
    DataTypePtr type(std::make_shared<DataTypeString>());
    assert((src.type->is_nullable() &&
            src.type->equals(DataTypeNullable(std::make_shared<DataTypeInt16>()))) ||
           (!src.type->is_nullable() && src.type->equals(DataTypeInt16())));
    Int16* p = (Int16*)encoded_column_data;
    if (src.type->is_nullable()) {
        auto nullable_column = assert_cast<const vectorized::ColumnNullable*>(src.column.get());
        auto _nullmap = nullable_column->get_null_map_data().data();
        for (size_t i = 0; i < row_num; ++i) {
            //in nullable column, if the item is null, *p can be any value
            if (UNLIKELY(*p >= value_num || *p < 0)) {
                if (LIKELY(_nullmap[i])) {
                    // it's null
                    decoded_column->insert_default();
                } else {
                    // data corrupt
                    return false;
                }
            } else {
                const StringValue& val = get_value(*p);
                decoded_column->insert_data(val.ptr, val.len);
            }
            ++p;
        }
    } else {
        for (size_t i = 0; i < row_num; ++i) {
            if (UNLIKELY(*p >= value_num || *p < 0)) {
                return false;
            }
            const StringValue& val = get_value(*p);
            decoded_column->insert_data(val.ptr, val.len);
            ++p;
        }
    }

    if (src.type->is_nullable()) {
        auto nullable_column = assert_cast<const ColumnNullable*>(src.column.get());
        dst.column = ColumnNullable::create(std::move(decoded_column),
                                            nullable_column->get_null_map_column().get_ptr());
        dst.type = std::make_shared<DataTypeNullable>(type);
    } else {
        dst.column = std::move(decoded_column);
        dst.type = type;
    }
    dst.name = src.name;

    return true;
}

} // namespace doris::vectorized