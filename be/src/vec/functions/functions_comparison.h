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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsComparison.h
// and modified by Doris

#pragma once

#include <limits>
#include <type_traits>

#include "common/logging.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/memcmp_small.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/decimal_comparison.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/functions_logical.h"
#include "vec/runtime/vdatetime_value.h"
namespace doris::vectorized {

/** Comparison functions: ==, !=, <, >, <=, >=.
  * The comparison functions always return 0 or 1 (UInt8).
  *
  * You can compare the following types:
  * - numbers and decimals;
  * - strings and fixed strings;
  * - dates;
  * - datetimes;
  *   within each group, but not from different groups;
  * - tuples (lexicographic comparison).
  *
  * Exception: You can compare the date and datetime with a constant string. Example: EventDate = '2015-01-01'.
  */

template <typename A, typename B, typename Op>
struct NumComparisonImpl {
    /// If you don't specify NO_INLINE, the compiler will inline this function, but we don't need this as this function contains tight loop inside.
    static void NO_INLINE vector_vector(const PaddedPODArray<A>& a, const PaddedPODArray<B>& b,
                                        PaddedPODArray<UInt8>& c) {
        /** GCC 4.8.2 vectorized a loop only if it is written in this form.
          * In this case, if you loop through the array index (the code will look simpler),
          *  the loop will not be vectorized.
          */

        size_t size = a.size();
        const A* a_pos = a.data();
        const B* b_pos = b.data();
        UInt8* c_pos = c.data();
        const A* a_end = a_pos + size;

        while (a_pos < a_end) {
            *c_pos = Op::apply(*a_pos, *b_pos);
            ++a_pos;
            ++b_pos;
            ++c_pos;
        }
    }

    static void NO_INLINE vector_constant(const PaddedPODArray<A>& a, B b,
                                          PaddedPODArray<UInt8>& c) {
        size_t size = a.size();
        const A* a_pos = a.data();
        UInt8* c_pos = c.data();
        const A* a_end = a_pos + size;

        while (a_pos < a_end) {
            *c_pos = Op::apply(*a_pos, b);
            ++a_pos;
            ++c_pos;
        }
    }

    static void constant_vector(A a, const PaddedPODArray<B>& b, PaddedPODArray<UInt8>& c) {
        NumComparisonImpl<B, A, typename Op::SymmetricOp>::vector_constant(b, a, c);
    }

    static void constant_constant(A a, B b, UInt8& c) { c = Op::apply(a, b); }
};

/// Generic version, implemented for columns of same type.
template <typename Op>
struct GenericComparisonImpl {
    static void NO_INLINE vector_vector(const IColumn& a, const IColumn& b,
                                        PaddedPODArray<UInt8>& c) {
        for (size_t i = 0, size = a.size(); i < size; ++i)
            c[i] = Op::apply(a.compare_at(i, i, b, 1), 0);
    }

    static void NO_INLINE vector_constant(const IColumn& a, const IColumn& b,
                                          PaddedPODArray<UInt8>& c) {
        auto b_materialized = b.clone_resized(1)->convert_to_full_column_if_const();
        for (size_t i = 0, size = a.size(); i < size; ++i)
            c[i] = Op::apply(a.compare_at(i, 0, *b_materialized, 1), 0);
    }

    static void constant_vector(const IColumn& a, const IColumn& b, PaddedPODArray<UInt8>& c) {
        GenericComparisonImpl<typename Op::SymmetricOp>::vector_constant(b, a, c);
    }

    static void constant_constant(const IColumn& a, const IColumn& b, UInt8& c) {
        c = Op::apply(a.compare_at(0, 0, b, 1), 0);
    }
};

struct NameEquals {
    static constexpr auto name = "eq";
};
struct NameNotEquals {
    static constexpr auto name = "ne";
};
struct NameLess {
    static constexpr auto name = "lt";
};
struct NameGreater {
    static constexpr auto name = "gt";
};
struct NameLessOrEquals {
    static constexpr auto name = "le";
};
struct NameGreaterOrEquals {
    static constexpr auto name = "ge";
};

template <template <typename, typename> class Op, typename Name>
class FunctionComparison : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionComparison>(); }

    FunctionComparison() = default;

private:
    template <typename T0, typename T1>
    bool execute_num_right_type(Block& block, size_t result, const ColumnVector<T0>* col_left,
                                const IColumn* col_right_untyped) {
        if (const ColumnVector<T1>* col_right =
                    check_and_get_column<ColumnVector<T1>>(col_right_untyped)) {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container& vec_res = col_res->get_data();
            vec_res.resize(col_left->get_data().size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_vector(col_left->get_data(),
                                                                 col_right->get_data(), vec_res);

            block.replace_by_position(result, std::move(col_res));
            return true;
        } else if (auto col_right_const =
                           check_and_get_column_const<ColumnVector<T1>>(col_right_untyped)) {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container& vec_res = col_res->get_data();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::vector_constant(
                    col_left->get_data(), col_right_const->template get_value<T1>(), vec_res);

            block.replace_by_position(result, std::move(col_res));
            return true;
        }

        return false;
    }

    template <typename T0, typename T1>
    bool execute_num_const_right_type(Block& block, size_t result, const ColumnConst* col_left,
                                      const IColumn* col_right_untyped) {
        if (const ColumnVector<T1>* col_right =
                    check_and_get_column<ColumnVector<T1>>(col_right_untyped)) {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container& vec_res = col_res->get_data();
            vec_res.resize(col_left->size());
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constant_vector(
                    col_left->template get_value<T0>(), col_right->get_data(), vec_res);

            block.replace_by_position(result, std::move(col_res));
            return true;
        } else if (auto col_right_const =
                           check_and_get_column_const<ColumnVector<T1>>(col_right_untyped)) {
            UInt8 res = 0;
            NumComparisonImpl<T0, T1, Op<T0, T1>>::constant_constant(
                    col_left->template get_value<T0>(), col_right_const->template get_value<T1>(),
                    res);

            block.replace_by_position(
                    result, DataTypeUInt8().create_column_const(col_left->size(), to_field(res)));
            return true;
        }

        return false;
    }

    template <typename T0>
    bool execute_num_left_type(Block& block, size_t result, const IColumn* col_left_untyped,
                               const IColumn* col_right_untyped) {
        if (const ColumnVector<T0>* col_left =
                    check_and_get_column<ColumnVector<T0>>(col_left_untyped)) {
            if (execute_num_right_type<T0, UInt8>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, UInt16>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, UInt32>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, UInt64>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, Int8>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, Int16>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, Int32>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, Int64>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, Int128>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, Float32>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, Float64>(block, result, col_left, col_right_untyped))
                return true;
            else {
                LOG(FATAL) << "Illegal column " << col_right_untyped->get_name()
                           << " of second argument of function " << get_name();
            }

        } else if (auto col_left_const =
                           check_and_get_column_const<ColumnVector<T0>>(col_left_untyped)) {
            if (execute_num_const_right_type<T0, UInt8>(block, result, col_left_const,
                                                        col_right_untyped) ||
                execute_num_const_right_type<T0, UInt16>(block, result, col_left_const,
                                                         col_right_untyped) ||
                execute_num_const_right_type<T0, UInt32>(block, result, col_left_const,
                                                         col_right_untyped) ||
                execute_num_const_right_type<T0, UInt64>(block, result, col_left_const,
                                                         col_right_untyped) ||
                execute_num_const_right_type<T0, Int8>(block, result, col_left_const,
                                                       col_right_untyped) ||
                execute_num_const_right_type<T0, Int16>(block, result, col_left_const,
                                                        col_right_untyped) ||
                execute_num_const_right_type<T0, Int32>(block, result, col_left_const,
                                                        col_right_untyped) ||
                execute_num_const_right_type<T0, Int64>(block, result, col_left_const,
                                                        col_right_untyped) ||
                execute_num_const_right_type<T0, Int128>(block, result, col_left_const,
                                                         col_right_untyped) ||
                execute_num_const_right_type<T0, Float32>(block, result, col_left_const,
                                                          col_right_untyped) ||
                execute_num_const_right_type<T0, Float64>(block, result, col_left_const,
                                                          col_right_untyped))
                return true;
            else {
                LOG(FATAL) << "Illegal column " << col_right_untyped->get_name()
                           << " of second argument of function " << get_name();
            }
        }

        return false;
    }

    Status execute_decimal(Block& block, size_t result, const ColumnWithTypeAndName& col_left,
                           const ColumnWithTypeAndName& col_right) {
        TypeIndex left_number = col_left.type->get_type_id();
        TypeIndex right_number = col_right.type->get_type_id();

        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            DecimalComparison<LeftDataType, RightDataType, Op, false>(block, result, col_left,
                                                                      col_right);
            return true;
        };

        if (!call_on_basic_types<true, false, true, false>(left_number, right_number, call)) {
            return Status::RuntimeError("Wrong call for {} with {} and {}", get_name(),
                                        col_left.type->get_name(), col_right.type->get_name());
        }
        return Status::OK();
    }

    void execute_generic_identical_types(Block& block, size_t result, const IColumn* c0,
                                         const IColumn* c1) {
        bool c0_const = is_column_const(*c0);
        bool c1_const = is_column_const(*c1);

        if (c0_const && c1_const) {
            UInt8 res = 0;
            GenericComparisonImpl<Op<int, int>>::constant_constant(*c0, *c1, res);
            block.replace_by_position(
                    result, DataTypeUInt8().create_column_const(c0->size(), to_field(res)));
        } else {
            auto c_res = ColumnUInt8::create();
            ColumnUInt8::Container& vec_res = c_res->get_data();
            vec_res.resize(c0->size());

            if (c0_const)
                GenericComparisonImpl<Op<int, int>>::constant_vector(*c0, *c1, vec_res);
            else if (c1_const)
                GenericComparisonImpl<Op<int, int>>::vector_constant(*c0, *c1, vec_res);
            else
                GenericComparisonImpl<Op<int, int>>::vector_vector(*c0, *c1, vec_res);

            block.replace_by_position(result, std::move(c_res));
        }
    }

    Status execute_generic(Block& block, size_t result, const ColumnWithTypeAndName& c0,
                           const ColumnWithTypeAndName& c1) {
        execute_generic_identical_types(block, result, c0.column.get(), c1.column.get());
        return Status::OK();
    }

public:
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        const auto& col_with_type_and_name_left = block.get_by_position(arguments[0]);
        const auto& col_with_type_and_name_right = block.get_by_position(arguments[1]);
        const IColumn* col_left_untyped = col_with_type_and_name_left.column.get();
        const IColumn* col_right_untyped = col_with_type_and_name_right.column.get();

        const DataTypePtr& left_type = col_with_type_and_name_left.type;
        const DataTypePtr& right_type = col_with_type_and_name_right.type;

        /// The case when arguments are the same (tautological comparison). Return constant.
        /// NOTE: Nullable types are special case. (BTW, this function use default implementation for Nullable, so Nullable types cannot be here. Check just in case.)
        /// NOTE: We consider NaN comparison to be implementation specific (and in our implementation NaNs are sometimes equal sometimes not).
        if (left_type->equals(*right_type) && !left_type->is_nullable() &&
            col_left_untyped == col_right_untyped) {
            /// Always true: =, <=, >=
            // TODO: Return const column in the future
            if constexpr (std::is_same_v<Op<int, int>, EqualsOp<int, int>> ||
                          std::is_same_v<Op<int, int>, LessOrEqualsOp<int, int>> ||
                          std::is_same_v<Op<int, int>, GreaterOrEqualsOp<int, int>>) {
                block.get_by_position(result).column =
                        DataTypeUInt8()
                                .create_column_const(input_rows_count, 1u)
                                ->convert_to_full_column_if_const();
                return Status::OK();
            } else {
                block.get_by_position(result).column =
                        DataTypeUInt8()
                                .create_column_const(input_rows_count, 0u)
                                ->convert_to_full_column_if_const();
                return Status::OK();
            }
        }

        WhichDataType which_left {left_type};
        WhichDataType which_right {right_type};

        const bool left_is_num = col_left_untyped->is_numeric();
        const bool right_is_num = col_right_untyped->is_numeric();

        // Compare date and datetime direct use the Int64 compare. Keep the comment
        // may we should refactor the code.
        //        bool date_and_datetime = (left_type != right_type) && which_left.is_date_or_datetime() &&
        //                                 which_right.is_date_or_datetime();

        if (left_is_num && right_is_num) {
            if (!(execute_num_left_type<UInt8>(block, result, col_left_untyped,
                                               col_right_untyped) ||
                  execute_num_left_type<UInt16>(block, result, col_left_untyped,
                                                col_right_untyped) ||
                  execute_num_left_type<UInt32>(block, result, col_left_untyped,
                                                col_right_untyped) ||
                  execute_num_left_type<UInt64>(block, result, col_left_untyped,
                                                col_right_untyped) ||
                  execute_num_left_type<Int8>(block, result, col_left_untyped, col_right_untyped) ||
                  execute_num_left_type<Int16>(block, result, col_left_untyped,
                                               col_right_untyped) ||
                  execute_num_left_type<Int32>(block, result, col_left_untyped,
                                               col_right_untyped) ||
                  execute_num_left_type<Int64>(block, result, col_left_untyped,
                                               col_right_untyped) ||
                  execute_num_left_type<Int128>(block, result, col_left_untyped,
                                                col_right_untyped) ||
                  execute_num_left_type<Float32>(block, result, col_left_untyped,
                                                 col_right_untyped) ||
                  execute_num_left_type<Float64>(block, result, col_left_untyped,
                                                 col_right_untyped)))

                return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                            col_left_untyped->get_name(), get_name());
        } else if (is_decimal_v2(left_type) || is_decimal_v2(right_type)) {
            if (!allow_decimal_comparison(left_type, right_type)) {
                return Status::RuntimeError("No operation {} between {} and {}", get_name(),
                                            left_type->get_name(), right_type->get_name());
            }
            return execute_decimal(block, result, col_with_type_and_name_left,
                                   col_with_type_and_name_right);
        } else if (is_decimal(left_type) || is_decimal(right_type)) {
            if (!allow_decimal_comparison(left_type, right_type)) {
                return Status::RuntimeError("No operation {} between {} and {}", get_name(),
                                            left_type->get_name(), right_type->get_name());
            }
            return execute_decimal(block, result, col_with_type_and_name_left,
                                   col_with_type_and_name_right);
        } else {
            // TODO: varchar and string maybe need a quickly way
            return execute_generic(block, result, col_with_type_and_name_left,
                                   col_with_type_and_name_right);
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
