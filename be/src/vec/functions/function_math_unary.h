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

#include "vec/columns/column_decimal.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

template <typename Impl>
class FunctionMathUnary : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionMathUnary>(); }

private:
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const auto& arg = arguments.front();
        if (!is_number(arg)) {
            return nullptr;
        }
        return std::make_shared<DataTypeFloat64>();
    }

    template <typename T, typename ReturnType>
    static void execute_in_iterations(const T* src_data, ReturnType* dst_data, size_t size) {
        if constexpr (Impl::rows_per_iteration == 0) {
            /// Process all data as a whole and use FastOps implementation

            /// If the argument is integer, convert to Float64 beforehand
            if constexpr (!std::is_floating_point_v<T>) {
                PODArray<Float64> tmp_vec(size);
                for (size_t i = 0; i < size; ++i) tmp_vec[i] = src_data[i];

                Impl::execute(tmp_vec.data(), size, dst_data);
            } else {
                Impl::execute(src_data, size, dst_data);
            }
        } else {
            const size_t rows_remaining = size % Impl::rows_per_iteration;
            const size_t rows_size = size - rows_remaining;

            for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                Impl::execute(&src_data[i], &dst_data[i]);

            if (rows_remaining != 0) {
                T src_remaining[Impl::rows_per_iteration];
                memcpy(src_remaining, &src_data[rows_size], rows_remaining * sizeof(T));
                memset(src_remaining + rows_remaining, 0,
                       (Impl::rows_per_iteration - rows_remaining) * sizeof(T));
                ReturnType dst_remaining[Impl::rows_per_iteration];

                Impl::execute(src_remaining, dst_remaining);

                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(ReturnType));
            }
        }
    }

    template <typename T, typename ReturnType>
    static bool execute(Block& block, const ColumnVector<T>* col, const size_t result) {
        const auto& src_data = col->get_data();
        const size_t size = src_data.size();

        auto dst = ColumnVector<ReturnType>::create();
        auto& dst_data = dst->get_data();
        dst_data.resize(size);

        execute_in_iterations(src_data.data(), dst_data.data(), size);

        block.replace_by_position(result, std::move(dst));
        return true;
    }

    template <typename T, typename ReturnType>
    static bool execute(Block& block, const ColumnDecimal<T>* col, const size_t result) {
        const auto& src_data = col->get_data();
        const size_t size = src_data.size();
        UInt32 scale = src_data.get_scale();

        auto dst = ColumnVector<ReturnType>::create();
        auto& dst_data = dst->get_data();
        dst_data.resize(size);

        for (size_t i = 0; i < size; ++i)
            dst_data[i] = convert_from_decimal<DataTypeDecimal<T>, DataTypeNumber<ReturnType>>(
                    src_data[i], scale);

        execute_in_iterations(dst_data.data(), dst_data.data(), size);

        block.replace_by_position(result, std::move(dst));
        return true;
    }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t /*input_rows_count*/) override {
        const ColumnWithTypeAndName& col = block.get_by_position(arguments[0]);

        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using Type = typename Types::RightType;
            using ReturnType = std::conditional_t<
                    Impl::always_returns_float64 || !std::is_floating_point_v<Type>, Float64, Type>;
            using ColVecType = std::conditional_t<IsDecimalNumber<Type>, ColumnDecimal<Type>,
                                                  ColumnVector<Type>>;

            const auto col_vec = check_and_get_column<ColVecType>(col.column.get());
            return execute<Type, ReturnType>(block, col_vec, result);
        };

        if (!call_on_basic_type<void, true, true, true, false>(col.type->get_type_id(), call)) {
            return Status::InvalidArgument("Illegal column " + col.column->get_name() +
                                           " of argument of function " + get_name());
        }
        return Status::OK();
    }
};

template <typename Name, Float64(Function)(Float64)>
struct UnaryFunctionPlain {
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;
    static constexpr bool always_returns_float64 = true;

    template <typename T>
    static void execute(const T* src, Float64* dst) {
        dst[0] = static_cast<Float64>(Function(static_cast<Float64>(src[0])));
    }
};

#define UnaryFunctionVectorized UnaryFunctionPlain

} // namespace doris::vectorized