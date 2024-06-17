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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsLogical.cpp
// and modified by Doris

#include "vec/functions/functions_logical.h"

#include <glog/logging.h>

#include <algorithm>
#include <ranges>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "gutil/integral_types.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

namespace {
using namespace FunctionsLogicalDetail;

template <class Op>
void vector_const(const IColumn* left, const ColumnConst* right, IColumn* res, size_t rows) {
    const auto* __restrict l_datas = assert_cast<const ColumnUInt8*>(left)->get_data().data();
    auto r_data = (uint8_t)right->get_bool(0);
    auto* __restrict res_datas = assert_cast<ColumnUInt8*>(res)->get_data().data();

    for (size_t i = 0; i < rows; ++i) {
        res_datas[i] = Op::apply(l_datas[i], r_data);
    }
}

template <class Op>
void vector_vector(const IColumn* left, const IColumn* right, IColumn* res, size_t rows) {
    const auto* __restrict l_datas = assert_cast<const ColumnUInt8*>(left)->get_data().data();
    const auto* __restrict r_datas = assert_cast<const ColumnUInt8*>(right)->get_data().data();
    auto* __restrict res_datas = assert_cast<ColumnUInt8*>(res)->get_data().data();

    for (size_t i = 0; i < rows; ++i) {
        res_datas[i] = Op::apply(l_datas[i], r_datas[i]);
    }
}

std::pair<const IColumn*, ColumnPtr> get_nested_and_null_column(const IColumn* column) {
    auto null_column = check_and_get_column<const ColumnNullable>(column);
    if (null_column) {
        return {null_column->get_nested_column_ptr().get(), null_column->get_null_map_column_ptr()};
    } else {
        return {column, ColumnUInt8::create(column->size(), 0)};
    }
}

template <class Op>
void vector_const_null(const IColumn* left, const ColumnConst* right, IColumn* res, IColumn* nulls,
                       size_t rows) {
    auto [data_column, null_column_ptr] = get_nested_and_null_column(left);
    const auto* __restrict l_datas =
            assert_cast<const ColumnUInt8*>(data_column)->get_data().data();
    const auto* __restrict l_nulls =
            assert_cast<const ColumnUInt8*>(null_column_ptr.get())->get_data().data();

    auto* __restrict res_datas = assert_cast<ColumnUInt8*>(res)->get_data().data();
    auto* __restrict res_nulls = assert_cast<ColumnUInt8*>(nulls)->get_data().data();

    auto r_data_ptr = right->get_data_at(0);

    if (r_data_ptr.data == nullptr) {
        for (size_t i = 0; i < rows; ++i) {
            res_nulls[i] = Op::apply_null(l_datas[i], l_nulls[i], 1, true);
            res_datas[i] = Op::apply(l_datas[i], 1);
        }
    } else {
        UInt8 r_data = *(UInt8*)r_data_ptr.data;
        for (size_t i = 0; i < rows; ++i) {
            res_nulls[i] = Op::apply_null(l_datas[i], l_nulls[i], r_data, false);
            res_datas[i] = Op::apply(l_datas[i], r_data);
        }
    }
}

template <class Op>
void vector_vector_null(const IColumn* left, const IColumn* right, IColumn* res, IColumn* nulls,
                        size_t rows) {
    auto [l_datas_ptr, l_nulls_ptr] = get_nested_and_null_column(left);
    auto [r_datas_ptr, r_nulls_ptr] = get_nested_and_null_column(right);

    const auto* __restrict l_datas =
            assert_cast<const ColumnUInt8*>(l_datas_ptr)->get_data().data();
    const auto* __restrict r_datas =
            assert_cast<const ColumnUInt8*>(r_datas_ptr)->get_data().data();
    const auto* __restrict l_nulls =
            assert_cast<const ColumnUInt8*>(l_nulls_ptr.get())->get_data().data();
    const auto* __restrict r_nulls =
            assert_cast<const ColumnUInt8*>(r_nulls_ptr.get())->get_data().data();

    auto* __restrict res_datas = assert_cast<ColumnUInt8*>(res)->get_data().data();
    auto* __restrict res_nulls = assert_cast<ColumnUInt8*>(nulls)->get_data().data();

    for (size_t i = 0; i < rows; ++i) {
        res_nulls[i] = Op::apply_null(l_datas[i], l_nulls[i], r_datas[i], r_nulls[i]);
        res_datas[i] = Op::apply(l_datas[i], r_datas[i]);
    }
}

template <class Op>
void basic_execute_impl(ColumnRawPtrs arguments, ColumnWithTypeAndName& result_info,
                        size_t input_rows_count) {
    auto col_res = ColumnUInt8::create(input_rows_count);
    if (auto l = check_and_get_column<ColumnConst>(arguments[0])) {
        vector_const<Op>(arguments[1], l, col_res, input_rows_count);
    } else if (auto r = check_and_get_column<ColumnConst>(arguments[1])) {
        vector_const<Op>(arguments[0], r, col_res, input_rows_count);
    } else {
        vector_vector<Op>(arguments[0], arguments[1], col_res, input_rows_count);
    }
    result_info.column = std::move(col_res);
}

template <class Op>
void null_execute_impl(ColumnRawPtrs arguments, ColumnWithTypeAndName& result_info,
                       size_t input_rows_count) {
    auto col_nulls = ColumnUInt8::create(input_rows_count);
    auto col_res = ColumnUInt8::create(input_rows_count);
    if (auto l = check_and_get_column<ColumnConst>(arguments[0])) {
        vector_const_null<Op>(arguments[1], l, col_res, col_nulls, input_rows_count);
    } else if (auto r = check_and_get_column<ColumnConst>(arguments[1])) {
        vector_const_null<Op>(arguments[0], r, col_res, col_nulls, input_rows_count);
    } else {
        vector_vector_null<Op>(arguments[0], arguments[1], col_res, col_nulls, input_rows_count);
    }
    result_info.column = ColumnNullable::create(std::move(col_res), std::move(col_nulls));
}

} // namespace

template <typename Impl, typename Name>
DataTypePtr FunctionAnyArityLogical<Impl, Name>::get_return_type_impl(
        const DataTypes& arguments) const {
    if (arguments.size() < 2) {
        LOG(FATAL) << fmt::format(
                "Number of arguments for function \"{}\" should be at least 2: passed {}",
                get_name(), arguments.size());
    }

    bool has_nullable_arguments = false;
    for (size_t i = 0; i < arguments.size(); ++i) {
        const auto& arg_type = arguments[i];

        if (!has_nullable_arguments) {
            has_nullable_arguments = arg_type->is_nullable();
            if (has_nullable_arguments && !Impl::special_implementation_for_nulls()) {
                LOG(WARNING) << fmt::format(
                        "Logical error: Unexpected type of argument for function \"{}\" argument "
                        "{} is of type {}",
                        get_name(), i + 1, arg_type->get_name());
            }
        }

        if (!(is_native_number(arg_type) || (Impl::special_implementation_for_nulls() &&
                                             is_native_number(remove_nullable(arg_type))))) {
            LOG(FATAL) << fmt::format("Illegal type ({}) of {} argument of function {}",
                                      arg_type->get_name(), i + 1, get_name());
        }
    }

    auto result_type = std::make_shared<DataTypeUInt8>();
    return has_nullable_arguments ? make_nullable(result_type) : result_type;
}

template <typename Impl, typename Name>
Status FunctionAnyArityLogical<Impl, Name>::execute_impl(FunctionContext* context, Block& block,
                                                         const ColumnNumbers& arguments,
                                                         size_t result_index,
                                                         size_t input_rows_count) const {
    ColumnRawPtrs args_in;
    for (const auto arg_index : arguments)
        args_in.push_back(block.get_by_position(arg_index).column.get());

    auto& result_info = block.get_by_position(result_index);
    if constexpr (Impl::special_implementation_for_nulls()) {
        if (result_info.type->is_nullable()) {
            null_execute_impl<Impl>(std::move(args_in), result_info, input_rows_count);
        } else {
            basic_execute_impl<Impl>(std::move(args_in), result_info, input_rows_count);
        }
    } else {
        DCHECK(std::ranges::all_of(args_in, [](const auto& arg) { return !arg->is_nullable(); }));
        basic_execute_impl<Impl>(std::move(args_in), result_info, input_rows_count);
    }
    return Status::OK();
}

template <typename A, typename Op>
struct UnaryOperationImpl {
    using ResultType = typename Op::ResultType;
    using ArrayA = typename ColumnVector<A>::Container;
    using ArrayC = typename ColumnVector<ResultType>::Container;

    static void NO_INLINE vector(const ArrayA& a, ArrayC& c) {
        std::transform(a.cbegin(), a.cend(), c.begin(), [](const auto x) { return Op::apply(x); });
    }
};

template <template <typename> class Impl, typename Name>
DataTypePtr FunctionUnaryLogical<Impl, Name>::get_return_type_impl(
        const DataTypes& arguments) const {
    if (!is_native_number(arguments[0])) {
        LOG(FATAL) << fmt::format("Illegal type ({}) of argument of function {}",
                                  arguments[0]->get_name(), get_name());
    }

    return std::make_shared<DataTypeUInt8>();
}

template <template <typename> class Impl, typename T>
bool functionUnaryExecuteType(Block& block, const ColumnNumbers& arguments, size_t result) {
    if (auto col = check_and_get_column<ColumnVector<T>>(
                block.get_by_position(arguments[0]).column.get())) {
        auto col_res = ColumnUInt8::create();

        typename ColumnUInt8::Container& vec_res = col_res->get_data();
        vec_res.resize(col->get_data().size());
        UnaryOperationImpl<T, Impl<T>>::vector(col->get_data(), vec_res);

        block.replace_by_position(result, std::move(col_res));
        return true;
    }

    return false;
}

template <template <typename> class Impl, typename Name>
Status FunctionUnaryLogical<Impl, Name>::execute_impl(FunctionContext* context, Block& block,
                                                      const ColumnNumbers& arguments, size_t result,
                                                      size_t /*input_rows_count*/) const {
    if (!functionUnaryExecuteType<Impl, UInt8>(block, arguments, result)) {
        LOG(FATAL) << fmt::format("Illegal column {} of argument of function {}",
                                  block.get_by_position(arguments[0]).column->get_name(),
                                  get_name());
    }

    return Status::OK();
}

void register_function_logical(SimpleFunctionFactory& instance) {
    instance.register_function<FunctionAnd>();
    instance.register_function<FunctionOr>();
    instance.register_function<FunctionNot>();
    instance.register_function<FunctionXor>();
}

} // namespace doris::vectorized
