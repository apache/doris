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

#include "olap/tablet_schema_helper.h"

#include <string.h>

#include "olap/tablet_schema.h"
#include "util/slice.h"
#include "vec/common/arena.h"

namespace doris {

TabletColumnPtr create_int_key(int32_t id, bool is_nullable, bool is_bf_column,
                               bool has_bitmap_index) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_INT;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 4;
    column->_index_length = 4;
    column->_is_bf_column = is_bf_column;
    column->_has_bitmap_index = has_bitmap_index;
    return column;
}

TabletColumnPtr create_int_value(int32_t id, FieldAggregationMethod agg_method, bool is_nullable,
                                 const std::string default_value, bool is_bf_column,
                                 bool has_bitmap_index) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_INT;
    column->_is_key = false;
    column->_aggregation = agg_method;
    column->_is_nullable = is_nullable;
    column->_length = 4;
    column->_index_length = 4;
    if (default_value != "") {
        column->_has_default_value = true;
        column->_default_value = default_value;
    }
    column->_is_bf_column = is_bf_column;
    column->_has_bitmap_index = has_bitmap_index;
    return column;
}

TabletColumnPtr create_char_key(int32_t id, bool is_nullable) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_CHAR;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 8;
    column->_index_length = 1;
    return column;
}

TabletColumnPtr create_varchar_key(int32_t id, bool is_nullable) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_VARCHAR;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 65533;
    column->_index_length = 4;
    return column;
}

TabletColumnPtr create_string_key(int32_t id, bool is_nullable) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_STRING;
    column->_is_key = true;
    column->_is_nullable = is_nullable;
    column->_length = 2147483643;
    column->_index_length = 4;
    return column;
}

void set_column_value_by_type(FieldType fieldType, int src, char* target, vectorized::Arena* pool,
                              size_t _length) {
    if (fieldType == FieldType::OLAP_FIELD_TYPE_CHAR) {
        std::string s = std::to_string(src);
        const char* src_value = s.c_str();
        int src_len = s.size();

        auto* dest_slice = (Slice*)target;
        dest_slice->size = _length;
        dest_slice->data = pool->alloc(dest_slice->size);
        memcpy(dest_slice->data, src_value, src_len);
        memset(dest_slice->data + src_len, 0, dest_slice->size - src_len);
    } else if (fieldType == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
        std::string s = std::to_string(src);
        const char* src_value = s.c_str();
        int src_len = s.size();

        auto* dest_slice = (Slice*)target;
        dest_slice->size = src_len;
        dest_slice->data = pool->alloc(src_len);
        memcpy(dest_slice->data, src_value, src_len);
    } else if (fieldType == FieldType::OLAP_FIELD_TYPE_STRING) {
        std::string s = std::to_string(src);
        const char* src_value = s.c_str();
        int src_len = s.size();

        auto* dest_slice = (Slice*)target;
        dest_slice->size = src_len;
        dest_slice->data = pool->alloc(src_len);
        memcpy(dest_slice->data, src_value, src_len);
    } else {
        *(int*)target = src;
    }
}
void set_column_value_by_type(FieldType fieldType, const std::string& src, char* target,
                              vectorized::Arena* pool, size_t _length) {
    if (fieldType == FieldType::OLAP_FIELD_TYPE_CHAR) {
        const char* src_value = src.c_str();
        int src_len = src.size();

        auto* dest_slice = (Slice*)target;
        dest_slice->size = _length;
        dest_slice->data = pool->alloc(dest_slice->size);
        memcpy(dest_slice->data, src_value, src_len);
        memset(dest_slice->data + src_len, 0, dest_slice->size - src_len);
    } else if (fieldType == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
        const char* src_value = src.c_str();
        int src_len = src.size();

        auto* dest_slice = (Slice*)target;
        dest_slice->size = src_len;
        dest_slice->data = pool->alloc(src_len);
        memcpy(dest_slice->data, src_value, src_len);
    } else if (fieldType == FieldType::OLAP_FIELD_TYPE_STRING) {
        const char* src_value = src.c_str();
        int src_len = src.size();

        auto* dest_slice = (Slice*)target;
        dest_slice->size = src_len;
        dest_slice->data = pool->alloc(src_len);
        memcpy(dest_slice->data, src_value, src_len);
    } else {
        *(int*)target = std::stoi(src);
    }
}

} // namespace doris
