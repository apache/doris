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

#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/columns_number.h"
#include "vec/core/call_on_type_index.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

template <typename Impl>
class FunctionMathBinaryFloat64 : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionMathBinaryFloat64>(); }
    static_assert(Impl::rows_per_iteration > 0, "Impl must process at least one row per iteration");

    bool use_default_implementation_for_constants() const override { return true; }

private:
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const auto check_argument_type = [this](const IDataType* arg) -> bool {
            if (!is_native_number(arg)) {
                LOG(ERROR) << "Illegal type " << arg->get_name() << " of argument of function "
                           << get_name();
                return false;
            }
            return true;
        };

        if (check_argument_type(arguments.front().get()) &&
            check_argument_type(arguments.back().get())) {
            return std::make_shared<DataTypeFloat64>();
        } else {
            return nullptr;
        }
    }

    template <typename LeftType, typename RightType>
    bool execute_typed(Block& block, const size_t result, const ColumnConst* left_arg,
                       const IColumn* right_arg) {
        if (const auto right_arg_typed = check_and_get_column<ColumnVector<RightType>>(right_arg)) {
            auto dst = ColumnVector<Float64>::create();

            LeftType left_src_data[Impl::rows_per_iteration];
            std::fill(std::begin(left_src_data), std::end(left_src_data),
                      left_arg->template get_value<LeftType>());
            const auto& right_src_data = right_arg_typed->get_data();
            const auto src_size = right_src_data.size();
            auto& dst_data = dst->get_data();
            dst_data.resize(src_size);

            const auto rows_remaining = src_size % Impl::rows_per_iteration;
            const auto rows_size = src_size - rows_remaining;

            for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                Impl::execute(left_src_data, &right_src_data[i], &dst_data[i]);

            if (rows_remaining != 0) {
                RightType right_src_remaining[Impl::rows_per_iteration];
                memcpy(right_src_remaining, &right_src_data[rows_size],
                       rows_remaining * sizeof(RightType));
                memset(right_src_remaining + rows_remaining, 0,
                       (Impl::rows_per_iteration - rows_remaining) * sizeof(RightType));
                Float64 dst_remaining[Impl::rows_per_iteration];

                Impl::execute(left_src_data, right_src_remaining, dst_remaining);

                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
            }

            block.replace_by_position(result, std::move(dst));
            return true;
        }

        return false;
    }

    template <typename LeftType, typename RightType>
    bool execute_typed(Block& block, const size_t result, const ColumnVector<LeftType>* left_arg,
                       const IColumn* right_arg) {
        if (const auto right_arg_typed = check_and_get_column<ColumnVector<RightType>>(right_arg)) {
            auto dst = ColumnVector<Float64>::create();

            const auto& left_src_data = left_arg->get_data();
            const auto& right_src_data = right_arg_typed->get_data();
            const auto src_size = left_src_data.size();
            auto& dst_data = dst->get_data();
            dst_data.resize(src_size);

            const auto rows_remaining = src_size % Impl::rows_per_iteration;
            const auto rows_size = src_size - rows_remaining;

            for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                Impl::execute(&left_src_data[i], &right_src_data[i], &dst_data[i]);

            if (rows_remaining != 0) {
                LeftType left_src_remaining[Impl::rows_per_iteration];
                memcpy(left_src_remaining, &left_src_data[rows_size],
                       rows_remaining * sizeof(LeftType));
                memset(left_src_remaining + rows_remaining, 0,
                       (Impl::rows_per_iteration - rows_remaining) * sizeof(LeftType));
                RightType right_src_remaining[Impl::rows_per_iteration];
                memcpy(right_src_remaining, &right_src_data[rows_size],
                       rows_remaining * sizeof(RightType));
                memset(right_src_remaining + rows_remaining, 0,
                       (Impl::rows_per_iteration - rows_remaining) * sizeof(RightType));
                Float64 dst_remaining[Impl::rows_per_iteration];

                Impl::execute(left_src_remaining, right_src_remaining, dst_remaining);

                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
            }

            block.replace_by_position(result, std::move(dst));
            return true;
        }
        if (const auto right_arg_typed =
                    check_and_get_column_const<ColumnVector<RightType>>(right_arg)) {
            auto dst = ColumnVector<Float64>::create();

            const auto& left_src_data = left_arg->get_data();
            RightType right_src_data[Impl::rows_per_iteration];
            std::fill(std::begin(right_src_data), std::end(right_src_data),
                      right_arg_typed->template get_value<RightType>());
            const auto src_size = left_src_data.size();
            auto& dst_data = dst->get_data();
            dst_data.resize(src_size);

            const auto rows_remaining = src_size % Impl::rows_per_iteration;
            const auto rows_size = src_size - rows_remaining;

            for (size_t i = 0; i < rows_size; i += Impl::rows_per_iteration)
                Impl::execute(&left_src_data[i], right_src_data, &dst_data[i]);

            if (rows_remaining != 0) {
                LeftType left_src_remaining[Impl::rows_per_iteration];
                memcpy(left_src_remaining, &left_src_data[rows_size],
                       rows_remaining * sizeof(LeftType));
                memset(left_src_remaining + rows_remaining, 0,
                       (Impl::rows_per_iteration - rows_remaining) * sizeof(LeftType));
                Float64 dst_remaining[Impl::rows_per_iteration];

                Impl::execute(left_src_remaining, right_src_data, dst_remaining);

                memcpy(&dst_data[rows_size], dst_remaining, rows_remaining * sizeof(Float64));
            }

            block.replace_by_position(result, std::move(dst));
            return true;
        }

        return false;
    }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t /*input_rows_count*/) override {
        const ColumnWithTypeAndName& col_left = block.get_by_position(arguments[0]);
        const ColumnWithTypeAndName& col_right = block.get_by_position(arguments[1]);

        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using LeftType = typename Types::LeftType;
            using RightType = typename Types::RightType;
            using ColVecLeft = ColumnVector<LeftType>;

            const IColumn* left_arg = col_left.column.get();
            const IColumn* right_arg = col_right.column.get();

            if (const auto left_arg_typed = check_and_get_column<ColVecLeft>(left_arg)) {
                if (execute_typed<LeftType, RightType>(block, result, left_arg_typed, right_arg)) {
                    return true;
                }
                DCHECK(false) << "Illegal column " << right_arg->get_name()
                              << " of second argument of function " << get_name();
            }
            if (const auto left_arg_typed = check_and_get_column_const<ColVecLeft>(left_arg)) {
                if (execute_typed<LeftType, RightType>(block, result, left_arg_typed, right_arg)) {
                    return true;
                }

                DCHECK(false) << "Illegal column " << right_arg->get_name()
                              << " of second argument of function " << get_name();
            }

            return false;
        };

        TypeIndex left_index = col_left.type->get_type_id();
        TypeIndex right_index = col_right.type->get_type_id();

        if (!call_on_basic_types<true, true, false, false>(left_index, right_index, call)) {
            return Status::InvalidArgument("Illegal column " + col_left.column->get_name() +
                                           " of argument of function " + get_name());
        }
        return Status::OK();
    }
};

template <typename Name, Float64(Function)(Float64, Float64)>
struct BinaryFunctionPlain {
    static constexpr auto name = Name::name;
    static constexpr auto rows_per_iteration = 1;

    template <typename T1, typename T2>
    static void execute(const T1* src_left, const T2* src_right, Float64* dst) {
        dst[0] = static_cast<Float64>(
                Function(static_cast<Float64>(src_left[0]), static_cast<Float64>(src_right[0])));
    }
};

#define BinaryFunctionVectorized BinaryFunctionPlain

} // namespace doris::vectorized
