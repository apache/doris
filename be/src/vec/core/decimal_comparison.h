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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/DecimalComparison.h
// and modified by Doris

#pragma once

#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/block.h"
#include "vec/core/call_on_type_index.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/functions/function_helpers.h" /// todo core should not depend on function"

namespace doris::vectorized {

inline bool allow_decimal_comparison(const DataTypePtr& left_type, const DataTypePtr& right_type) {
    if (is_decimal(left_type)) {
        if (is_decimal(right_type) || is_not_decimal_but_comparable_to_decimal(right_type))
            return true;
    } else if (is_not_decimal_but_comparable_to_decimal(left_type) && is_decimal(right_type))
        return true;
    return false;
}

template <size_t>
struct ConstructDecInt {
    using Type = Int32;
};
template <>
struct ConstructDecInt<8> {
    using Type = Int64;
};
template <>
struct ConstructDecInt<16> {
    using Type = Int128;
};
template <>
struct ConstructDecInt<32> {
    using Type = wide::Int256;
};

template <typename T, typename U>
struct DecCompareInt {
    using Type = typename ConstructDecInt<
            (!IsDecimalNumber<U> || sizeof(T) > sizeof(U)) ? sizeof(T) : sizeof(U)>::Type;
    using TypeA = Type;
    using TypeB = Type;
};

///
template <typename A, typename B, template <typename, typename> typename Operation,
          bool _check_overflow = true, bool _actual = IsDecimalNumber<A> || IsDecimalNumber<B>>
class DecimalComparison {
public:
    using CompareInt = typename DecCompareInt<A, B>::Type;
    using Op = Operation<CompareInt, CompareInt>;
    using ColVecA = std::conditional_t<IsDecimalNumber<A>, ColumnDecimal<A>, ColumnVector<A>>;
    using ColVecB = std::conditional_t<IsDecimalNumber<B>, ColumnDecimal<B>, ColumnVector<B>>;
    using ArrayA = typename ColVecA::Container;
    using ArrayB = typename ColVecB::Container;

    DecimalComparison(Block& block, size_t result, const ColumnWithTypeAndName& col_left,
                      const ColumnWithTypeAndName& col_right) {
        if (!apply(block, result, col_left, col_right)) {
            LOG(FATAL) << fmt::format("Wrong decimal comparison with {} and {}",
                                      col_left.type->get_name(), col_right.type->get_name());
        }
    }

    static bool apply(Block& block, size_t result [[maybe_unused]],
                      const ColumnWithTypeAndName& col_left,
                      const ColumnWithTypeAndName& col_right) {
        if constexpr (_actual) {
            ColumnPtr c_res;
            Shift shift = getScales<A, B>(col_left.type, col_right.type);

            c_res = apply_with_scale(col_left.column, col_right.column, shift);
            if (c_res) {
                block.replace_by_position(result, std::move(c_res));
            }
            return true;
        }
        return false;
    }

    static bool compare(A a, B b, UInt32 scale_a, UInt32 scale_b) {
        static const UInt32 max_scale = max_decimal_precision<Decimal256>();
        if (scale_a > max_scale || scale_b > max_scale) {
            LOG(FATAL) << "Bad scale of decimal field";
        }

        Shift shift;
        if (scale_a < scale_b) {
            shift.a = DataTypeDecimal<B>(max_decimal_precision<B>(), scale_b)
                              .get_scale_multiplier(scale_b - scale_a)
                              .value;
        }
        if (scale_a > scale_b) {
            shift.b = DataTypeDecimal<A>(max_decimal_precision<A>(), scale_a)
                              .get_scale_multiplier(scale_a - scale_b)
                              .value;
        }

        return apply_with_scale(a, b, shift);
    }

private:
    struct Shift {
        CompareInt a = 1;
        CompareInt b = 1;

        bool none() const { return a == 1 && b == 1; }
        bool left() const { return a != 1; }
        bool right() const { return b != 1; }
    };

    template <typename T, typename U>
    static auto apply_with_scale(T a, U b, const Shift& shift) {
        if (shift.left())
            return apply<true, false>(a, b, shift.a);
        else if (shift.right())
            return apply<false, true>(a, b, shift.b);
        return apply<false, false>(a, b, 1);
    }

    template <typename T, typename U>
        requires IsDecimalNumber<T> && IsDecimalNumber<U>
    static Shift getScales(const DataTypePtr& left_type, const DataTypePtr& right_type) {
        const DataTypeDecimal<T>* decimal0 = check_decimal<T>(*left_type);
        const DataTypeDecimal<U>* decimal1 = check_decimal<U>(*right_type);

        Shift shift;
        if (decimal0 && decimal1) {
            using Type = std::conditional_t<sizeof(T) >= sizeof(U), T, U>;
            auto type_ptr = decimal_result_type(*decimal0, *decimal1, false, false, false);
            const DataTypeDecimal<Type>* result_type = check_decimal<Type>(*type_ptr);
            shift.a = result_type->scale_factor_for(*decimal0, false).value;
            shift.b = result_type->scale_factor_for(*decimal1, false).value;
        } else if (decimal0) {
            shift.b = decimal0->get_scale_multiplier().value;
        } else if (decimal1) {
            shift.a = decimal1->get_scale_multiplier().value;
        }

        return shift;
    }

    template <typename T, typename U>
        requires(IsDecimalNumber<T> && !IsDecimalNumber<U>)
    static Shift getScales(const DataTypePtr& left_type, const DataTypePtr&) {
        Shift shift;
        const DataTypeDecimal<T>* decimal0 = check_decimal<T>(*left_type);
        if (decimal0) {
            shift.b = decimal0->get_scale_multiplier().value;
        }
        return shift;
    }

    template <typename T, typename U>
        requires(!IsDecimalNumber<T> && IsDecimalNumber<U>)
    static Shift getScales(const DataTypePtr&, const DataTypePtr& right_type) {
        Shift shift;
        const DataTypeDecimal<U>* decimal1 = check_decimal<U>(*right_type);
        if (decimal1) {
            shift.a = decimal1->get_scale_multiplier().value;
        }
        return shift;
    }

    template <bool scale_left, bool scale_right>
    static ColumnPtr apply(const ColumnPtr& c0, const ColumnPtr& c1, CompareInt scale) {
        auto c_res = ColumnUInt8::create();

        if constexpr (_actual) {
            bool c0_is_const = is_column_const(*c0);
            bool c1_is_const = is_column_const(*c1);

            if (c0_is_const && c1_is_const) {
                const ColumnConst* c0_const = check_and_get_column_const<ColVecA>(c0.get());
                const ColumnConst* c1_const = check_and_get_column_const<ColVecB>(c1.get());

                A a = c0_const->template get_value<A>();
                B b = c1_const->template get_value<B>();
                UInt8 res = apply<scale_left, scale_right>(a, b, scale);
                return DataTypeUInt8().create_column_const(c0->size(), to_field(res));
            }

            ColumnUInt8::Container& vec_res = c_res->get_data();
            vec_res.resize(c0->size());

            if (c0_is_const) {
                const ColumnConst* c0_const = check_and_get_column_const<ColVecA>(c0.get());
                A a = c0_const->template get_value<A>();
                if (const ColVecB* c1_vec = check_and_get_column<ColVecB>(c1.get()))
                    constant_vector<scale_left, scale_right>(a, c1_vec->get_data(), vec_res, scale);
                else {
                    LOG(FATAL) << "Wrong column in Decimal comparison";
                }
            } else if (c1_is_const) {
                const ColumnConst* c1_const = check_and_get_column_const<ColVecB>(c1.get());
                B b = c1_const->template get_value<B>();
                if (const ColVecA* c0_vec = check_and_get_column<ColVecA>(c0.get()))
                    vector_constant<scale_left, scale_right>(c0_vec->get_data(), b, vec_res, scale);
                else {
                    LOG(FATAL) << "Wrong column in Decimal comparison";
                }
            } else {
                if (const ColVecA* c0_vec = check_and_get_column<ColVecA>(c0.get())) {
                    if (const ColVecB* c1_vec = check_and_get_column<ColVecB>(c1.get()))
                        vector_vector<scale_left, scale_right>(c0_vec->get_data(),
                                                               c1_vec->get_data(), vec_res, scale);
                    else {
                        LOG(FATAL) << "Wrong column in Decimal comparison";
                    }
                } else {
                    LOG(FATAL) << "Wrong column in Decimal comparison";
                }
            }
        }

        return c_res;
    }

    template <bool scale_left, bool scale_right>
    static NO_INLINE UInt8 apply(A a, B b, CompareInt scale [[maybe_unused]]) {
        CompareInt x = a;
        CompareInt y = b;

        if constexpr (_check_overflow) {
            bool overflow = false;

            if constexpr (sizeof(A) > sizeof(CompareInt)) overflow |= (A(x) != a);
            if constexpr (sizeof(B) > sizeof(CompareInt)) overflow |= (B(y) != b);
            if constexpr (std::is_unsigned_v<A>) overflow |= (x < 0);
            if constexpr (std::is_unsigned_v<B>) overflow |= (y < 0);

            if constexpr (scale_left) overflow |= common::mul_overflow(x, scale, x);
            if constexpr (scale_right) overflow |= common::mul_overflow(y, scale, y);

            if (overflow) {
                LOG(FATAL) << "Can't compare";
            }
        } else {
            if constexpr (scale_left) x *= scale;
            if constexpr (scale_right) y *= scale;
        }

        return Op::apply(x, y);
    }

    template <bool scale_left, bool scale_right>
    static void NO_INLINE vector_vector(const ArrayA& a, const ArrayB& b, PaddedPODArray<UInt8>& c,
                                        CompareInt scale) {
        size_t size = a.size();
        const A* a_pos = a.data();
        const B* b_pos = b.data();
        UInt8* c_pos = c.data();
        const A* a_end = a_pos + size;

        while (a_pos < a_end) {
            *c_pos = apply<scale_left, scale_right>(*a_pos, *b_pos, scale);
            ++a_pos;
            ++b_pos;
            ++c_pos;
        }
    }

    template <bool scale_left, bool scale_right>
    static void NO_INLINE vector_constant(const ArrayA& a, B b, PaddedPODArray<UInt8>& c,
                                          CompareInt scale) {
        size_t size = a.size();
        const A* a_pos = a.data();
        UInt8* c_pos = c.data();
        const A* a_end = a_pos + size;

        while (a_pos < a_end) {
            *c_pos = apply<scale_left, scale_right>(*a_pos, b, scale);
            ++a_pos;
            ++c_pos;
        }
    }

    template <bool scale_left, bool scale_right>
    static void NO_INLINE constant_vector(A a, const ArrayB& b, PaddedPODArray<UInt8>& c,
                                          CompareInt scale) {
        size_t size = b.size();
        const B* b_pos = b.data();
        UInt8* c_pos = c.data();
        const B* b_end = b_pos + size;

        while (b_pos < b_end) {
            *c_pos = apply<scale_left, scale_right>(a, *b_pos, scale);
            ++b_pos;
            ++c_pos;
        }
    }
};

} // namespace doris::vectorized
