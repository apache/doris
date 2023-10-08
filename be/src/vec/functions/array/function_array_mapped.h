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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/FunctionArrayMapped.h
// and modified by Doris

#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"

namespace doris {
namespace vectorized {

/** Higher-order functions for arrays.
  * These functions optionally apply a map (transform) to array (or multiple arrays of identical size) by lambda function,
  *  and return some result based on that transformation.
  *
  * Examples:
  * arrayMap(x1,...,xn -> expression, array1,...,arrayn) - apply the expression to each element of the array (or set of parallel arrays).
  * arrayFilter(x -> predicate, array) - leave in the array only the elements for which the expression is true.
  *
  * For some functions arrayCount, arrayExists, arrayAll, an overload of the form f(array) is available,
  *  which works in the same way as f(x -> x, array).
  *
  * See the example of Impl template parameter in arrayMap.cpp
  */
template <typename Impl, typename Name>
class FunctionArrayMapped : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionArrayMapped>(); }

    String get_name() const override { return name; }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto& typed_column = block.get_by_position(arguments[0]);
        auto ptr = typed_column.column->convert_to_full_column_if_const();
        const typename Impl::column_type* column_array;
        if (ptr->is_nullable()) {
            column_array = check_and_get_column<const typename Impl::column_type>(
                    static_cast<const ColumnNullable*>(ptr.get())->get_nested_column_ptr().get());
        } else {
            column_array = check_and_get_column<const typename Impl::column_type>(ptr.get());
        }
        const auto* data_type_array =
                static_cast<const DataTypeArray*>(remove_nullable(typed_column.type).get());
        return Impl::execute(block, arguments, result, data_type_array, *column_array);
    }

    bool is_variadic() const override { return Impl::_is_variadic(); }

    size_t get_number_of_arguments() const override { return Impl::_get_number_of_arguments(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type(arguments);
    }
};

} // namespace vectorized
} // namespace doris
