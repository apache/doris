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

#include "vec/columns/column_filter_helper.h"

namespace doris::vectorized {
ColumnFilterHelper::ColumnFilterHelper(IColumn& column_)
        : _column(assert_cast<ColumnNullable&>(column_)),
          _value_column(assert_cast<ColumnUInt8&>(_column.get_nested_column())),
          _null_map_column(_column.get_null_map_column()) {}

void ColumnFilterHelper::resize_fill(size_t size, doris::vectorized::UInt8 value) {
    _value_column.get_data().resize_fill(size, value);
    _null_map_column.get_data().resize_fill(size, 0);
}

void ColumnFilterHelper::insert_value(doris::vectorized::UInt8 value) {
    _value_column.get_data().push_back(value);
    _null_map_column.get_data().push_back(0);
}

void ColumnFilterHelper::insert_null() {
    _value_column.insert_default();
    _null_map_column.get_data().push_back(1);
}

void ColumnFilterHelper::reserve(size_t size) {
    _value_column.reserve(size);
    _null_map_column.reserve(size);
}

} // namespace doris::vectorized