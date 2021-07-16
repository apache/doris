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

#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/call_on_type_index.h"
#include "vec/core/column_numbers.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

class IFunction;

/// Methods, that helps dispatching over real column types.

template <typename ... Type>
bool check_data_type(const IDataType* data_type) {
    return ((typeid_cast<const Type*>(data_type)) || ...);
}

template <typename Type>
const Type* check_and_get_data_type(const IDataType* data_type) {
    return typeid_cast<const Type*>(data_type);
}

template <typename Type>
const ColumnConst* check_and_get_column_const(const IColumn* column) {
    if (!column || !is_column_const(*column)) return {};

    const ColumnConst* res = assert_cast<const ColumnConst*>(column);

    if (!check_column<Type>(&res->get_data_column())) return {};

    return res;
}

template <typename Type>
const Type* check_and_get_column_constData(const IColumn* column) {
    const ColumnConst* res = check_and_get_column_const<Type>(column);

    if (!res) return {};

    return static_cast<const Type*>(&res->get_data_column());
}

template <typename Type>
bool check_column_const(const IColumn* column) {
    return check_and_get_column_const<Type>(column);
}

/// Returns non-nullptr if column is ColumnConst with ColumnString or ColumnFixedString inside.
const ColumnConst* check_and_get_column_constStringOrFixedString(const IColumn* column);

/// Transform anything to Field.
template <typename T>
inline std::enable_if_t<!IsDecimalNumber<T>, Field> to_field(const T& x) {
    return Field(NearestFieldType<T>(x));
}

template <typename T>
inline std::enable_if_t<IsDecimalNumber<T>, Field> to_field(const T& x, UInt32 scale) {
    return Field(NearestFieldType<T>(x, scale));
}

Columns convert_const_tuple_to_constant_elements(const ColumnConst& column);

/// Returns the copy of a given block in which each column specified in
/// the "arguments" parameter is replaced with its respective nested
/// column if it is nullable.
Block create_block_with_nested_columns(const Block& block, const ColumnNumbers& args);

/// Similar function as above. Additionally transform the result type if needed.
Block create_block_with_nested_columns(const Block& block, const ColumnNumbers& args, size_t result);

/// Checks argument type at specified index with predicate.
/// throws if there is no argument at specified index or if predicate returns false.
void validate_argument_type(const IFunction& func, const DataTypes& arguments, size_t argument_index,
                            bool (*validator_func)(const IDataType&),
                            const char* expected_type_description);

} // namespace doris::vectorized
