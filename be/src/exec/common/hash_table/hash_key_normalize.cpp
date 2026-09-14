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

#include "exec/common/hash_table/hash_key_normalize.h"

#include <algorithm>
#include <utility>

#include "core/assert_cast.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"

namespace doris {

bool contains_float_or_double(const DataTypePtr& type) {
    const auto inner = remove_nullable(type);
    switch (inner->get_primitive_type()) {
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        return true;
    case TYPE_ARRAY:
        return contains_float_or_double(
                assert_cast<const DataTypeArray&>(*inner).get_nested_type());
    case TYPE_MAP: {
        const auto& map = assert_cast<const DataTypeMap&>(*inner);
        return contains_float_or_double(map.get_key_type()) ||
               contains_float_or_double(map.get_value_type());
    }
    case TYPE_STRUCT:
        return std::ranges::any_of(assert_cast<const DataTypeStruct&>(*inner).get_elements(),
                                   contains_float_or_double);
    default:
        return false;
    }
}

void normalize_float_hash_key(ColumnPtr& column, const DataTypePtr& type) {
    if (!contains_float_or_double(type)) {
        return;
    }
    auto mutable_column = IColumn::mutate(std::move(column));
    mutable_column->replace_float_special_values();
    column = std::move(mutable_column);
}

} // namespace doris
