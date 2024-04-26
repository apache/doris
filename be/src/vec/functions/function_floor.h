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

#include <cstddef>
#include <functional>
#include <type_traits>
#include <utility>

#include "common/exception.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "round.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/call_on_type_index.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

struct FloorFloatOneArgImpl {
    static constexpr auto name = "floor";
    static DataTypes get_variadic_argument_types() { return {std::make_shared<DataTypeFloat64>()}; }
};

struct FloorFloatTwoArgImpl {
    static constexpr auto name = "floor";
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeInt32>()};
    }
};

struct FloorDecimalOneArgImpl {
    static constexpr auto name = "floor";
    static DataTypes get_variadic_argument_types() {
        // All Decimal types are named Decimal, and real scale will be passed as type argument for execute function
        // So we can just register Decimal32 here
        return {std::make_shared<DataTypeDecimal<Decimal32>>(9, 0)};
    }
};

struct FloorDecimalTwoArgImpl {
    static constexpr auto name = "floor";
    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeDecimal<Decimal32>>(9, 0),
                std::make_shared<DataTypeInt32>()};
    }
};

template <typename Impl>
class FunctionFloor : public FunctionRounding<Impl, RoundingMode::Floor, TieBreakingMode::Auto> {
public:
    static FunctionPtr create() { return std::make_shared<FunctionFloor>(); }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {}; }
    // SELECT number, floor(123.345, 1) FROM numbers("number"="10")
    // should NOT behave like two column arguments, so we can not use const column default implementation
    bool use_default_implementation_for_constants() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnWithTypeAndName& column_general = block.get_by_position(arguments[0]);
        ColumnPtr res;

        // potential argument types:
        // 0. floor(ColumnConst, ColumnConst)
        // 1. floor(Column), floor(Column, ColumnConst)
        // 2. floor(Column, Column)
        // 3. floor(ColumnConst, Column)

        if (arguments.size() == 2 && is_column_const(*block.get_by_position(arguments[0]).column) &&
            is_column_const(*block.get_by_position(arguments[1]).column)) {
            // floor(ColumnConst, ColumnConst)
            auto col_general =
                    assert_cast<const ColumnConst&>(*column_general.column).get_data_column_ptr();
            Int16 scale_arg = 0;
            RETURN_IF_ERROR(FunctionFloor<Impl>::get_scale_arg(
                    block.get_by_position(arguments[1]), &scale_arg));

            auto call = [&](const auto& types) -> bool {
                using Types = std::decay_t<decltype(types)>;
                using DataType = typename Types::LeftType;

                if constexpr (IsDataTypeNumber<DataType> || IsDataTypeDecimal<DataType>) {
                    using FieldType = typename DataType::FieldType;
                    res = Dispatcher<FieldType, RoundingMode::Floor,
                                     TieBreakingMode::Auto>::apply_vec_const(col_general,
                                                                             scale_arg);
                    return true;
                }

                return false;
            };

#if !defined(__SSE4_1__) && !defined(__aarch64__)
            /// In case of "nearbyint" function is used, we should ensure the expected rounding mode for the Banker's rounding.
            /// Actually it is by default. But we will set it just in case.

            if constexpr (rounding_mode == RoundingMode::Round) {
                if (0 != fesetround(FE_TONEAREST)) {
                    return Status::InvalidArgument("Cannot set floating point rounding mode");
                }
            }
#endif

            if (!call_on_index_and_data_type<void>(column_general.type->get_type_id(), call)) {
                return Status::InvalidArgument("Invalid argument type {} for function {}",
                                               column_general.type->get_name(), "floor");
            }
            // Important, make sure the result column has the same size as the input column
            res = ColumnConst::create(std::move(res), input_rows_count);
        } else if (arguments.size() == 1 ||
                   (arguments.size() == 2 &&
                    is_column_const(*block.get_by_position(arguments[1]).column))) {
            // floor(Column) or floor(Column, ColumnConst)
            Int16 scale_arg = 0;
            if (arguments.size() == 2) {
                RETURN_IF_ERROR(FunctionFloor<Impl>::get_scale_arg(
                        block.get_by_position(arguments[1]), &scale_arg));
            }

            auto call = [&](const auto& types) -> bool {
                using Types = std::decay_t<decltype(types)>;
                using DataType = typename Types::LeftType;

                if constexpr (IsDataTypeNumber<DataType> || IsDataTypeDecimal<DataType>) {
                    using FieldType = typename DataType::FieldType;
                    res = Dispatcher<FieldType, RoundingMode::Floor, TieBreakingMode::Auto>::
                            apply_vec_const(column_general.column.get(), scale_arg);
                    return true;
                }

                return false;
            };
#if !defined(__SSE4_1__) && !defined(__aarch64__)
            /// In case of "nearbyint" function is used, we should ensure the expected rounding mode for the Banker's rounding.
            /// Actually it is by default. But we will set it just in case.

            if constexpr (rounding_mode == RoundingMode::Round) {
                if (0 != fesetround(FE_TONEAREST)) {
                    return Status::InvalidArgument("Cannot set floating point rounding mode");
                }
            }
#endif

            if (!call_on_index_and_data_type<void>(column_general.type->get_type_id(), call)) {
                return Status::InvalidArgument("Invalid argument type {} for function {}",
                                               column_general.type->get_name(), "floor");
            }

        } else if (is_column_const(*block.get_by_position(arguments[0]).column)) {
            // floor(ColumnConst, Column)
            const ColumnWithTypeAndName& column_scale = block.get_by_position(arguments[1]);
            const ColumnConst& const_col_general =
                    assert_cast<const ColumnConst&>(*column_general.column);

            auto call = [&](const auto& types) -> bool {
                using Types = std::decay_t<decltype(types)>;
                using DataType = typename Types::LeftType;

                if constexpr (IsDataTypeNumber<DataType> || IsDataTypeDecimal<DataType>) {
                    using FieldType = typename DataType::FieldType;
                    res = Dispatcher<FieldType, RoundingMode::Floor, TieBreakingMode::Auto>::
                            apply_const_vec(&const_col_general, column_scale.column.get());
                    return true;
                }

                return false;
            };

#if !defined(__SSE4_1__) && !defined(__aarch64__)
            /// In case of "nearbyint" function is used, we should ensure the expected rounding mode for the Banker's rounding.
            /// Actually it is by default. But we will set it just in case.

            if constexpr (rounding_mode == RoundingMode::Round) {
                if (0 != fesetround(FE_TONEAREST)) {
                    return Status::InvalidArgument("Cannot set floating point rounding mode");
                }
            }
#endif

            if (!call_on_index_and_data_type<void>(column_general.type->get_type_id(), call)) {
                return Status::InvalidArgument("Invalid argument type {} for function {}",
                                               column_general.type->get_name(), "floor");
            }
        } else {
            // floor(Column, Column)
            const ColumnWithTypeAndName& column_scale = block.get_by_position(arguments[1]);

            auto call = [&](const auto& types) -> bool {
                using Types = std::decay_t<decltype(types)>;
                using DataType = typename Types::LeftType;

                if constexpr (IsDataTypeNumber<DataType> || IsDataTypeDecimal<DataType>) {
                    using FieldType = typename DataType::FieldType;
                    res = Dispatcher<FieldType, RoundingMode::Floor, TieBreakingMode::Auto>::
                            apply_vec_vec(column_general.column.get(), column_scale.column.get());
                    return true;
                }
                return false;
            };

#if !defined(__SSE4_1__) && !defined(__aarch64__)
            /// In case of "nearbyint" function is used, we should ensure the expected rounding mode for the Banker's rounding.
            /// Actually it is by default. But we will set it just in case.

            if constexpr (rounding_mode == RoundingMode::Round) {
                if (0 != fesetround(FE_TONEAREST)) {
                    return Status::InvalidArgument("Cannot set floating point rounding mode");
                }
            }
#endif

            if (!call_on_index_and_data_type<void>(column_general.type->get_type_id(), call)) {
                return Status::InvalidArgument("Invalid argument type {} for function {}",
                                               column_general.type->get_name(), "floor");
            }
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

} // namespace doris::vectorized
