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

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>

#include "olap/olap_common.h"
#include "olap/tablet_schema.h"

namespace doris {
namespace vectorized {
class Arena;
} // namespace vectorized

TabletColumnPtr create_int_key(int32_t id, bool is_nullable = true, bool is_bf_column = false,
                               bool has_bitmap_index = false);

TabletColumnPtr create_int_value(
        int32_t id,
        FieldAggregationMethod agg_method = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
        bool is_nullable = true, const std::string default_value = "", bool is_bf_column = false,
        bool has_bitmap_index = false);

TabletColumnPtr create_char_key(int32_t id, bool is_nullable = true);

TabletColumnPtr create_varchar_key(int32_t id, bool is_nullable = true);

TabletColumnPtr create_string_key(int32_t id, bool is_nullable = true);

template <FieldType type>
TabletColumnPtr create_with_default_value(std::string default_value) {
    auto column = std::make_shared<TabletColumn>();
    column->_type = type;
    column->_is_nullable = true;
    column->_aggregation = FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE;
    column->_has_default_value = true;
    column->_default_value = default_value;
    column->_length = 4;
    return column;
}

void set_column_value_by_type(FieldType fieldType, int src, char* target, vectorized::Arena* pool,
                              size_t _length = 8);

void set_column_value_by_type(FieldType fieldType, const std::string& src, char* target,
                              vectorized::Arena* pool, size_t _length = 8);

} // namespace doris
