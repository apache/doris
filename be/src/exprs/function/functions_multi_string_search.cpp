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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsMultiStringSearch.h
// and modified by Doris

#include "exprs/function/functions_multi_string_search.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/pod_array_fwd.h"
#include "core/types.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

template <typename Impl>
class FunctionsMultiStringSearch : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionsMultiStringSearch>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto haystack_column = block.get_by_position(arguments[0]).column;
        auto needles_column = block.get_by_position(arguments[1]).column;

        auto haystack_ptr = remove_nullable(haystack_column);
        auto needles_ptr = remove_nullable(needles_column);

        const auto* col_haystack_vector = check_and_get_column<ColumnString>(&*haystack_ptr);
        const ColumnConst* col_haystack_const =
                check_and_get_column_const<ColumnString>(&*haystack_ptr);

        const auto* col_needles_vector = check_and_get_column<ColumnArray>(needles_ptr.get());
        const ColumnConst* col_needles_const =
                check_and_get_column_const<ColumnArray>(needles_ptr.get());

        if (!col_needles_const && !col_needles_vector) {
            return Status::InvalidArgument(
                    "function '{}' encountered unsupported needles column, found {}", name,
                    needles_column->get_name());
        }

        if (col_haystack_const && col_needles_vector) {
            return Status::InvalidArgument(
                    "function '{}' doesn't support search with non-constant needles "
                    "in constant haystack",
                    name);
        }

        auto col_res = ColumnVector<Impl::ResultPType>::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        auto& vec_res = col_res->get_data();
        auto& offsets_res = col_offsets->get_data();

        Status status;
        if (col_needles_const) {
            status = Impl::vector_constant(
                    col_haystack_vector->get_chars(), col_haystack_vector->get_offsets(),
                    col_needles_const->get_value<TYPE_ARRAY>(), vec_res, offsets_res,
                    allow_hyperscan_, max_hyperscan_regexp_length_,
                    max_hyperscan_regexp_total_length_);
        } else {
            status = Impl::vector_vector(
                    col_haystack_vector->get_chars(), col_haystack_vector->get_offsets(),
                    col_needles_vector->get_data(), col_needles_vector->get_offsets(), vec_res,
                    offsets_res, allow_hyperscan_, max_hyperscan_regexp_length_,
                    max_hyperscan_regexp_total_length_);
        }

        if (!status.ok()) {
            return status;
        }

        handle_nullable_column(haystack_column, vec_res, input_rows_count);
        handle_nullable_column(needles_column, vec_res, input_rows_count);

        block.replace_by_position(result, std::move(col_res));

        return status;
    }

private:
    using ResultType = typename Impl::ResultType;

    constexpr static bool allow_hyperscan_ = true;
    constexpr static size_t max_hyperscan_regexp_length_ = 0;       // not limited
    constexpr static size_t max_hyperscan_regexp_total_length_ = 0; // not limited

    /// Handles nullable column by setting result to 0 if the input is null
    void handle_nullable_column(const ColumnPtr& column, PaddedPODArray<ResultType>& vec_res,
                                size_t input_rows_count) const {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
            const auto& null_map = nullable->get_null_map_data();
            for (size_t i = 0; i != input_rows_count; ++i) {
                if (null_map[i]) {
                    vec_res[i] = 0;
                }
            }
        } else if (const auto* const_column = check_and_get_column<ColumnConst>(column.get());
                   const_column && is_column_nullable(const_column->get_data_column())) {
            const auto& const_nullable =
                    assert_cast<const ColumnNullable&>(const_column->get_data_column());
            if (const_nullable.get_null_map_data()[0]) {
                std::fill(vec_res.begin(), vec_res.begin() + input_rows_count, 0);
            }
        }
    }
};

using FunctionMultiMatchAny = FunctionsMultiStringSearch<FunctionMultiMatchAnyImpl<
        TYPE_TINYINT, MultiMatchTraits::Find::Any, /*WithEditDistance*/ false>>;

void register_function_multi_string_search(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMultiMatchAny>();
}

} // namespace doris
