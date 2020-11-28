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

#include <string>

#include "olap/tablet_schema.h"
#include "runtime/mem_pool.h"

namespace doris {

TabletColumn create_int_key(int32_t id, bool is_nullable = true, bool is_bf_column = false,
                            bool has_bitmap_index = false) {
    TabletColumn column;
    column._unique_id = id;
    column._col_name = std::to_string(id);
    column._type = OLAP_FIELD_TYPE_INT;
    column._is_key = true;
    column._is_nullable = is_nullable;
    column._length = 4;
    column._index_length = 4;
    column._is_bf_column = is_bf_column;
    column._has_bitmap_index = has_bitmap_index;
    return column;
}

TabletColumn create_int_value(int32_t id,
                              FieldAggregationMethod agg_method = OLAP_FIELD_AGGREGATION_SUM,
                              bool is_nullable = true, const std::string default_value = "",
                              bool is_bf_column = false, bool has_bitmap_index = false) {
    TabletColumn column;
    column._unique_id = id;
    column._col_name = std::to_string(id);
    column._type = OLAP_FIELD_TYPE_INT;
    column._is_key = false;
    column._aggregation = agg_method;
    column._is_nullable = is_nullable;
    column._length = 4;
    column._index_length = 4;
    if (default_value != "") {
        column._has_default_value = true;
        column._default_value = default_value;
    }
    column._is_bf_column = is_bf_column;
    column._has_bitmap_index = has_bitmap_index;
    return column;
}

TabletColumn create_char_key(int32_t id, bool is_nullable = true) {
    TabletColumn column;
    column._unique_id = id;
    column._col_name = std::to_string(id);
    column._type = OLAP_FIELD_TYPE_CHAR;
    column._is_key = true;
    column._is_nullable = is_nullable;
    column._length = 8;
    column._index_length = 1;
    return column;
}

TabletColumn create_varchar_key(int32_t id, bool is_nullable = true) {
    TabletColumn column;
    column._unique_id = id;
    column._col_name = std::to_string(id);
    column._type = OLAP_FIELD_TYPE_VARCHAR;
    column._is_key = true;
    column._is_nullable = is_nullable;
    column._length = 8;
    column._index_length = 4;
    return column;
}

template <FieldType type>
TabletColumn create_with_default_value(std::string default_value) {
    TabletColumn column;
    column._type = type;
    column._is_nullable = true;
    column._aggregation = OLAP_FIELD_AGGREGATION_NONE;
    column._has_default_value = true;
    column._default_value = default_value;
    column._length = 4;
    return column;
}

void set_column_value_by_type(FieldType fieldType, int src, char* target, MemPool* pool,
                              size_t _length = 0) {
    if (fieldType == OLAP_FIELD_TYPE_CHAR) {
        std::string s = std::to_string(src);
        char* src_value = &s[0];
        int src_len = s.size();

        auto* dest_slice = (Slice*)target;
        dest_slice->size = _length;
        dest_slice->data = (char*)pool->allocate(dest_slice->size);
        memcpy(dest_slice->data, src_value, src_len);
        memset(dest_slice->data + src_len, 0, dest_slice->size - src_len);
    } else if (fieldType == OLAP_FIELD_TYPE_VARCHAR) {
        std::string s = std::to_string(src);
        char* src_value = &s[0];
        int src_len = s.size();

        auto* dest_slice = (Slice*)target;
        dest_slice->size = src_len;
        dest_slice->data = (char*)pool->allocate(src_len);
        memcpy(dest_slice->data, src_value, src_len);
    } else {
        *(int*)target = src;
    }
}

} // namespace doris
