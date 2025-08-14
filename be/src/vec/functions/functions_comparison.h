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
#include "olap/rowset/segment_v2/index_reader_helper.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/memcmp_small.h"
#include "vec/core/accurate_comparison.h"
#include "vec/core/decimal_comparison.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/functions_logical.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

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
        size_t size = a.size();
        const A* __restrict a_pos = a.data();
        const B* __restrict b_pos = b.data();
        UInt8* __restrict c_pos = c.data();
        const A* __restrict a_end = a_pos + size;

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
        const A* __restrict a_pos = a.data();
        UInt8* __restrict c_pos = c.data();
        const A* __restrict a_end = a_pos + size;

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
    static void vector_vector(const IColumn& a, const IColumn& b, PaddedPODArray<UInt8>& c) {
        for (size_t i = 0, size = a.size(); i < size; ++i) {
            c[i] = Op::apply(a.compare_at(i, i, b, 1), 0);
        }
    }

    static void vector_constant(const IColumn& a, const IColumn& b, PaddedPODArray<UInt8>& c) {
        const auto& col_right = assert_cast<const ColumnConst&>(b).get_data_column();
        for (size_t i = 0, size = a.size(); i < size; ++i) {
            c[i] = Op::apply(a.compare_at(i, 0, col_right, 1), 0);
        }
    }

    static void constant_vector(const IColumn& a, const IColumn& b, PaddedPODArray<UInt8>& c) {
        GenericComparisonImpl<typename Op::SymmetricOp>::vector_constant(b, a, c);
    }

    static void constant_constant(const IColumn& a, const IColumn& b, UInt8& c) {
        c = Op::apply(a.compare_at(0, 0, b, 1), 0);
    }
};

template <typename Op>
struct StringComparisonImpl {
    static void NO_INLINE string_vector_string_vector(const ColumnString::Chars& a_data,
                                                      const ColumnString::Offsets& a_offsets,
                                                      const ColumnString::Chars& b_data,
                                                      const ColumnString::Offsets& b_offsets,
                                                      PaddedPODArray<UInt8>& c) {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;
        ColumnString::Offset prev_b_offset = 0;
        const auto* a_pos = a_data.data();
        const auto* b_pos = b_data.data();

        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::apply(memcmp_small_allow_overflow15(
                                     a_pos + prev_a_offset, a_offsets[i] - prev_a_offset,
                                     b_pos + prev_b_offset, b_offsets[i] - prev_b_offset),
                             0);

            prev_a_offset = a_offsets[i];
            prev_b_offset = b_offsets[i];
        }
    }

    static void NO_INLINE string_vector_constant(const ColumnString::Chars& a_data,
                                                 const ColumnString::Offsets& a_offsets,
                                                 const ColumnString::Chars& b_data,
                                                 ColumnString::Offset b_size,
                                                 PaddedPODArray<UInt8>& c) {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;
        const auto* a_pos = a_data.data();
        const auto* b_pos = b_data.data();

        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::apply(
                    memcmp_small_allow_overflow15(a_pos + prev_a_offset,
                                                  a_offsets[i] - prev_a_offset, b_pos, b_size),
                    0);

            prev_a_offset = a_offsets[i];
        }
    }

    static void constant_string_vector(const ColumnString::Chars& a_data,
                                       ColumnString::Offset a_size,
                                       const ColumnString::Chars& b_data,
                                       const ColumnString::Offsets& b_offsets,
                                       PaddedPODArray<UInt8>& c) {
        StringComparisonImpl<typename Op::SymmetricOp>::string_vector_constant(b_data, b_offsets,
                                                                               a_data, a_size, c);
    }
};

template <bool positive>
struct StringEqualsImpl {
    static void NO_INLINE string_vector_string_vector(const ColumnString::Chars& a_data,
                                                      const ColumnString::Offsets& a_offsets,
                                                      const ColumnString::Chars& b_data,
                                                      const ColumnString::Offsets& b_offsets,
                                                      PaddedPODArray<UInt8>& c) {
        size_t size = a_offsets.size();
        ColumnString::Offset prev_a_offset = 0;
        ColumnString::Offset prev_b_offset = 0;
        const auto* a_pos = a_data.data();
        const auto* b_pos = b_data.data();

        for (size_t i = 0; i < size; ++i) {
            auto a_size = a_offsets[i] - prev_a_offset;
            auto b_size = b_offsets[i] - prev_b_offset;

            c[i] = positive == memequal_small_allow_overflow15(a_pos + prev_a_offset, a_size,
                                                               b_pos + prev_b_offset, b_size);

            prev_a_offset = a_offsets[i];
            prev_b_offset = b_offsets[i];
        }
    }

    static void NO_INLINE string_vector_constant(const ColumnString::Chars& a_data,
                                                 const ColumnString::Offsets& a_offsets,
                                                 const ColumnString::Chars& b_data,
                                                 ColumnString::Offset b_size,
                                                 PaddedPODArray<UInt8>& c) {
        size_t size = a_offsets.size();
        if (b_size == 0) {
            auto* __restrict data = c.data();
            auto* __restrict offsets = a_offsets.data();

            ColumnString::Offset prev_a_offset = 0;
            for (size_t i = 0; i < size; ++i) {
                data[i] = positive ? (offsets[i] == prev_a_offset) : (offsets[i] != prev_a_offset);
                prev_a_offset = offsets[i];
            }
        } else {
            ColumnString::Offset prev_a_offset = 0;
            const auto* a_pos = a_data.data();
            const auto* b_pos = b_data.data();
            for (size_t i = 0; i < size; ++i) {
                auto a_size = a_offsets[i] - prev_a_offset;
                c[i] = positive == memequal_small_allow_overflow15(a_pos + prev_a_offset, a_size,
                                                                   b_pos, b_size);
                prev_a_offset = a_offsets[i];
            }
        }
    }

    static void NO_INLINE constant_string_vector(const ColumnString::Chars& a_data,
                                                 ColumnString::Offset a_size,
                                                 const ColumnString::Chars& b_data,
                                                 const ColumnString::Offsets& b_offsets,
                                                 PaddedPODArray<UInt8>& c) {
        string_vector_constant(b_data, b_offsets, a_data, a_size, c);
    }
};

template <PrimitiveType A, PrimitiveType B>
struct StringComparisonImpl<EqualsOp<A, B>> : StringEqualsImpl<true> {};

template <PrimitiveType A, PrimitiveType B>
struct StringComparisonImpl<NotEqualsOp<A, B>> : StringEqualsImpl<false> {};

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

template <template <PrimitiveType, PrimitiveType> class Op, typename Name>
class FunctionComparison : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionComparison>(); }

    FunctionComparison() = default;

private:
    template <PrimitiveType T0, PrimitiveType T1>
    bool execute_num_right_type(Block& block, uint32_t result, const ColumnVector<T0>* col_left,
                                const IColumn* col_right_untyped) const {
        if (const ColumnVector<T1>* col_right =
                    check_and_get_column<ColumnVector<T1>>(col_right_untyped)) {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container& vec_res = col_res->get_data();
            vec_res.resize(col_left->get_data().size());
            NumComparisonImpl<typename PrimitiveTypeTraits<T0>::ColumnItemType,
                              typename PrimitiveTypeTraits<T1>::ColumnItemType,
                              Op<T0, T1>>::vector_vector(col_left->get_data(),
                                                         col_right->get_data(), vec_res);

            block.replace_by_position(result, std::move(col_res));
            return true;
        } else if (auto col_right_const =
                           check_and_get_column_const<ColumnVector<T1>>(col_right_untyped)) {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container& vec_res = col_res->get_data();
            vec_res.resize(col_left->size());
            NumComparisonImpl<typename PrimitiveTypeTraits<T0>::ColumnItemType,
                              typename PrimitiveTypeTraits<T1>::ColumnItemType, Op<T0, T1>>::
                    vector_constant(col_left->get_data(),
                                    col_right_const->template get_value<
                                            typename PrimitiveTypeTraits<T1>::ColumnItemType>(),
                                    vec_res);

            block.replace_by_position(result, std::move(col_res));
            return true;
        }

        return false;
    }

    template <PrimitiveType T0, PrimitiveType T1>
    bool execute_num_const_right_type(Block& block, uint32_t result, const ColumnConst* col_left,
                                      const IColumn* col_right_untyped) const {
        if (const ColumnVector<T1>* col_right =
                    check_and_get_column<ColumnVector<T1>>(col_right_untyped)) {
            auto col_res = ColumnUInt8::create();

            ColumnUInt8::Container& vec_res = col_res->get_data();
            vec_res.resize(col_left->size());
            NumComparisonImpl<typename PrimitiveTypeTraits<T0>::ColumnItemType,
                              typename PrimitiveTypeTraits<T1>::ColumnItemType, Op<T0, T1>>::
                    constant_vector(col_left->template get_value<
                                            typename PrimitiveTypeTraits<T0>::ColumnItemType>(),
                                    col_right->get_data(), vec_res);

            block.replace_by_position(result, std::move(col_res));
            return true;
        } else if (auto col_right_const =
                           check_and_get_column_const<ColumnVector<T1>>(col_right_untyped)) {
            UInt8 res = 0;
            NumComparisonImpl<typename PrimitiveTypeTraits<T0>::ColumnItemType,
                              typename PrimitiveTypeTraits<T1>::ColumnItemType, Op<T0, T1>>::
                    constant_constant(col_left->template get_value<
                                              typename PrimitiveTypeTraits<T0>::ColumnItemType>(),
                                      col_right_const->template get_value<
                                              typename PrimitiveTypeTraits<T1>::ColumnItemType>(),
                                      res);

            block.replace_by_position(
                    result, DataTypeUInt8().create_column_const(col_left->size(),
                                                                to_field<TYPE_BOOLEAN>(res)));
            return true;
        }

        return false;
    }

    template <PrimitiveType T0>
    bool execute_num_left_type(Block& block, uint32_t result, const IColumn* col_left_untyped,
                               const IColumn* col_right_untyped) const {
        if (const ColumnVector<T0>* col_left =
                    check_and_get_column<ColumnVector<T0>>(col_left_untyped)) {
            if (execute_num_right_type<T0, TYPE_BOOLEAN>(block, result, col_left,
                                                         col_right_untyped) ||
                execute_num_right_type<T0, TYPE_DATE>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, TYPE_DATEV2>(block, result, col_left,
                                                        col_right_untyped) ||
                execute_num_right_type<T0, TYPE_DATETIME>(block, result, col_left,
                                                          col_right_untyped) ||
                execute_num_right_type<T0, TYPE_DATETIMEV2>(block, result, col_left,
                                                            col_right_untyped) ||
                execute_num_right_type<T0, TYPE_TINYINT>(block, result, col_left,
                                                         col_right_untyped) ||
                execute_num_right_type<T0, TYPE_SMALLINT>(block, result, col_left,
                                                          col_right_untyped) ||
                execute_num_right_type<T0, TYPE_INT>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, TYPE_BIGINT>(block, result, col_left,
                                                        col_right_untyped) ||
                execute_num_right_type<T0, TYPE_LARGEINT>(block, result, col_left,
                                                          col_right_untyped) ||
                execute_num_right_type<T0, TYPE_IPV4>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, TYPE_IPV6>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, TYPE_FLOAT>(block, result, col_left,
                                                       col_right_untyped) ||
                execute_num_right_type<T0, TYPE_DOUBLE>(block, result, col_left,
                                                        col_right_untyped) ||
                execute_num_right_type<T0, TYPE_TIME>(block, result, col_left, col_right_untyped) ||
                execute_num_right_type<T0, TYPE_TIMEV2>(block, result, col_left,
                                                        col_right_untyped)) {
                return true;
            } else {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "Illegal column ({}) of second argument of function {}",
                                       col_right_untyped->get_name(), get_name());
            }

        } else if (auto col_left_const =
                           check_and_get_column_const<ColumnVector<T0>>(col_left_untyped)) {
            if (execute_num_const_right_type<T0, TYPE_BOOLEAN>(block, result, col_left_const,
                                                               col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_DATE>(block, result, col_left_const,
                                                            col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_DATEV2>(block, result, col_left_const,
                                                              col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_DATETIME>(block, result, col_left_const,
                                                                col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_DATETIMEV2>(block, result, col_left_const,
                                                                  col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_TINYINT>(block, result, col_left_const,
                                                               col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_SMALLINT>(block, result, col_left_const,
                                                                col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_INT>(block, result, col_left_const,
                                                           col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_BIGINT>(block, result, col_left_const,
                                                              col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_LARGEINT>(block, result, col_left_const,
                                                                col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_IPV4>(block, result, col_left_const,
                                                            col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_IPV6>(block, result, col_left_const,
                                                            col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_FLOAT>(block, result, col_left_const,
                                                             col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_DOUBLE>(block, result, col_left_const,
                                                              col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_TIME>(block, result, col_left_const,
                                                            col_right_untyped) ||
                execute_num_const_right_type<T0, TYPE_TIMEV2>(block, result, col_left_const,
                                                              col_right_untyped)) {
                return true;
            } else {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "Illegal column ({}) f second argument of function {}",
                                       col_right_untyped->get_name(), get_name());
            }
        }

        return false;
    }

    Status execute_decimal(Block& block, uint32_t result, const ColumnWithTypeAndName& col_left,
                           const ColumnWithTypeAndName& col_right) const {
        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            DecimalComparison<LeftDataType::PType, RightDataType::PType, Op, false>(
                    block, result, col_left, col_right);
            return true;
        };

        if (!call_on_basic_types<true, false, true, false>(col_left.type->get_primitive_type(),
                                                           col_right.type->get_primitive_type(),
                                                           call)) {
            return Status::RuntimeError("Wrong call for {} with {} and {}", get_name(),
                                        col_left.type->get_name(), col_right.type->get_name());
        }
        return Status::OK();
    }

    Status execute_string(Block& block, uint32_t result, const IColumn* c0,
                          const IColumn* c1) const {
        const ColumnString* c0_string = check_and_get_column<ColumnString>(c0);
        const ColumnString* c1_string = check_and_get_column<ColumnString>(c1);
        const ColumnConst* c0_const = check_and_get_column_const_string_or_fixedstring(c0);
        const ColumnConst* c1_const = check_and_get_column_const_string_or_fixedstring(c1);
        if (!((c0_string || c0_const) && (c1_string || c1_const))) {
            return Status::NotSupported("Illegal columns {}, {} of argument of function {}",
                                        c0->get_name(), c1->get_name(), name);
        }

        if (c0_const && c1_const) {
            execute_generic_identical_types(block, result, c0, c1);
            return Status::OK();
        }

        const ColumnString::Chars* c0_const_chars = nullptr;
        const ColumnString::Chars* c1_const_chars = nullptr;
        ColumnString::Offset c0_const_size = 0;
        ColumnString::Offset c1_const_size = 0;

        if (c0_const) {
            const ColumnString* c0_const_string =
                    check_and_get_column<ColumnString>(&c0_const->get_data_column());

            if (c0_const_string) {
                c0_const_chars = &c0_const_string->get_chars();
                c0_const_size = c0_const_string->get_offsets()[0];
            } else {
                return Status::NotSupported("Illegal columns {}, of argument of function {}",
                                            c0->get_name(), name);
            }
        }

        if (c1_const) {
            const ColumnString* c1_const_string =
                    check_and_get_column<ColumnString>(&c1_const->get_data_column());

            if (c1_const_string) {
                c1_const_chars = &c1_const_string->get_chars();
                c1_const_size = c1_const_string->get_offsets()[0];
            } else {
                return Status::NotSupported("Illegal columns {}, of argument of function {}",
                                            c1->get_name(), name);
            }
        }

        using StringImpl = StringComparisonImpl<Op<TYPE_INT, TYPE_INT>>;

        auto c_res = ColumnUInt8::create();
        ColumnUInt8::Container& vec_res = c_res->get_data();
        vec_res.resize(c0->size());

        if (c0_string && c1_string) {
            StringImpl::string_vector_string_vector(
                    c0_string->get_chars(), c0_string->get_offsets(), c1_string->get_chars(),
                    c1_string->get_offsets(), vec_res);
        } else if (c0_string && c1_const) {
            StringImpl::string_vector_constant(c0_string->get_chars(), c0_string->get_offsets(),
                                               *c1_const_chars, c1_const_size, vec_res);
        } else if (c0_const && c1_string) {
            StringImpl::constant_string_vector(*c0_const_chars, c0_const_size,
                                               c1_string->get_chars(), c1_string->get_offsets(),
                                               vec_res);
        } else {
            return Status::NotSupported("Illegal columns {}, {} of argument of function {}",
                                        c0->get_name(), c1->get_name(), name);
        }
        block.replace_by_position(result, std::move(c_res));
        return Status::OK();
    }

    void execute_generic_identical_types(Block& block, uint32_t result, const IColumn* c0,
                                         const IColumn* c1) const {
        bool c0_const = is_column_const(*c0);
        bool c1_const = is_column_const(*c1);

        if (c0_const && c1_const) {
            UInt8 res = 0;
            GenericComparisonImpl<Op<TYPE_INT, TYPE_INT>>::constant_constant(*c0, *c1, res);
            block.replace_by_position(result, DataTypeUInt8().create_column_const(
                                                      c0->size(), to_field<TYPE_BOOLEAN>(res)));
        } else {
            auto c_res = ColumnUInt8::create();
            ColumnUInt8::Container& vec_res = c_res->get_data();
            vec_res.resize(c0->size());

            if (c0_const) {
                GenericComparisonImpl<Op<TYPE_INT, TYPE_INT>>::constant_vector(*c0, *c1, vec_res);
            } else if (c1_const) {
                GenericComparisonImpl<Op<TYPE_INT, TYPE_INT>>::vector_constant(*c0, *c1, vec_res);
            } else {
                GenericComparisonImpl<Op<TYPE_INT, TYPE_INT>>::vector_vector(*c0, *c1, vec_res);
            }

            block.replace_by_position(result, std::move(c_res));
        }
    }

    Status execute_generic(Block& block, uint32_t result, const ColumnWithTypeAndName& c0,
                           const ColumnWithTypeAndName& c1) const {
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

    Status evaluate_inverted_index(
            const ColumnsWithTypeAndName& arguments,
            const std::vector<vectorized::IndexFieldNameAndTypePair>& data_type_with_names,
            std::vector<segment_v2::IndexIterator*> iterators, uint32_t num_rows,
            segment_v2::InvertedIndexResultBitmap& bitmap_result) const override {
        DCHECK(arguments.size() == 1);
        DCHECK(data_type_with_names.size() == 1);
        DCHECK(iterators.size() == 1);
        auto* iter = iterators[0];
        auto data_type_with_name = data_type_with_names[0];
        if (iter == nullptr) {
            return Status::OK();
        }
        if (!segment_v2::IndexReaderHelper::has_string_or_bkd_index(iter)) {
            return Status::OK();
        }
        segment_v2::InvertedIndexQueryType query_type;
        std::string_view name_view(name);
        if (name_view == NameEquals::name || name_view == NameNotEquals::name) {
            query_type = segment_v2::InvertedIndexQueryType::EQUAL_QUERY;
        } else if (name_view == NameLess::name) {
            query_type = segment_v2::InvertedIndexQueryType::LESS_THAN_QUERY;
        } else if (name_view == NameLessOrEquals::name) {
            query_type = segment_v2::InvertedIndexQueryType::LESS_EQUAL_QUERY;
        } else if (name_view == NameGreater::name) {
            query_type = segment_v2::InvertedIndexQueryType::GREATER_THAN_QUERY;
        } else if (name_view == NameGreaterOrEquals::name) {
            query_type = segment_v2::InvertedIndexQueryType::GREATER_EQUAL_QUERY;
        } else {
            return Status::InvalidArgument("invalid comparison op type {}", Name::name);
        }

        if (segment_v2::is_range_query(query_type) &&
            iter->get_reader(segment_v2::InvertedIndexReaderType::STRING_TYPE)) {
            // untokenized strings exceed ignore_above, they are written as null, causing range query errors
            return Status::OK();
        }
        Field param_value;
        arguments[0].column->get(0, param_value);
        auto param_type = arguments[0].type->get_primitive_type();
        std::unique_ptr<segment_v2::InvertedIndexQueryParamFactory> query_param = nullptr;
        RETURN_IF_ERROR(segment_v2::InvertedIndexQueryParamFactory::create_query_value(
                param_type, &param_value, query_param));
        RETURN_IF_ERROR(segment_v2::InvertedIndexQueryParamFactory::create_query_value(
                param_type, &param_value, query_param));

        segment_v2::InvertedIndexParam param;
        param.column_name = data_type_with_name.first;
        param.column_type = data_type_with_name.second;
        param.query_value = query_param->get_value();
        param.query_type = query_type;
        param.num_rows = num_rows;
        param.roaring = std::make_shared<roaring::Roaring>();
        RETURN_IF_ERROR(iter->read_from_index(&param));
        std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
        if (iter->has_null()) {
            segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
            RETURN_IF_ERROR(iter->read_null_bitmap(&null_bitmap_cache_handle));
            null_bitmap = null_bitmap_cache_handle.get_bitmap();
        }
        segment_v2::InvertedIndexResultBitmap result(param.roaring, null_bitmap);
        bitmap_result = result;
        bitmap_result.mask_out_null();

        if (name_view == NameNotEquals::name) {
            roaring::Roaring full_result;
            full_result.addRange(0, num_rows);
            bitmap_result.op_not(&full_result);
        }

        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
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
            // TODO: Return const column in the future. But seems so far to do. We need a unified approach for passing const column.
            if constexpr (std::is_same_v<Op<TYPE_INT, TYPE_INT>, EqualsOp<TYPE_INT, TYPE_INT>> ||
                          std::is_same_v<Op<TYPE_INT, TYPE_INT>,
                                         LessOrEqualsOp<TYPE_INT, TYPE_INT>> ||
                          std::is_same_v<Op<TYPE_INT, TYPE_INT>,
                                         GreaterOrEqualsOp<TYPE_INT, TYPE_INT>>) {
                block.get_by_position(result).column =
                        DataTypeUInt8()
                                .create_column_const(input_rows_count,
                                                     Field::create_field<TYPE_BOOLEAN>(1))
                                ->convert_to_full_column_if_const();
                return Status::OK();
            } else {
                block.get_by_position(result).column =
                        DataTypeUInt8()
                                .create_column_const(input_rows_count,
                                                     Field::create_field<TYPE_BOOLEAN>(0))
                                ->convert_to_full_column_if_const();
                return Status::OK();
            }
        }

        auto can_compare = [](PrimitiveType t) -> bool {
            return is_int_or_bool(t) || is_float_or_double(t) || t == TYPE_IPV4 || t == TYPE_IPV6 ||
                   t == TYPE_DATEV2 || t == TYPE_DATETIMEV2;
        };
        const bool left_is_num_can_compare = can_compare(left_type->get_primitive_type());
        const bool right_is_num_can_compare = can_compare(right_type->get_primitive_type());

        const bool left_is_string = is_string_type(left_type->get_primitive_type());
        const bool right_is_string = is_string_type(right_type->get_primitive_type());

        if (left_is_num_can_compare && right_is_num_can_compare) {
            if (!(execute_num_left_type<TYPE_BOOLEAN>(block, result, col_left_untyped,
                                                      col_right_untyped) ||
                  execute_num_left_type<TYPE_DATE>(block, result, col_left_untyped,
                                                   col_right_untyped) ||
                  execute_num_left_type<TYPE_DATEV2>(block, result, col_left_untyped,
                                                     col_right_untyped) ||
                  execute_num_left_type<TYPE_DATETIME>(block, result, col_left_untyped,
                                                       col_right_untyped) ||
                  execute_num_left_type<TYPE_DATETIMEV2>(block, result, col_left_untyped,
                                                         col_right_untyped) ||
                  execute_num_left_type<TYPE_TINYINT>(block, result, col_left_untyped,
                                                      col_right_untyped) ||
                  execute_num_left_type<TYPE_SMALLINT>(block, result, col_left_untyped,
                                                       col_right_untyped) ||
                  execute_num_left_type<TYPE_INT>(block, result, col_left_untyped,
                                                  col_right_untyped) ||
                  execute_num_left_type<TYPE_BIGINT>(block, result, col_left_untyped,
                                                     col_right_untyped) ||
                  execute_num_left_type<TYPE_LARGEINT>(block, result, col_left_untyped,
                                                       col_right_untyped) ||
                  execute_num_left_type<TYPE_IPV4>(block, result, col_left_untyped,
                                                   col_right_untyped) ||
                  execute_num_left_type<TYPE_IPV6>(block, result, col_left_untyped,
                                                   col_right_untyped) ||
                  execute_num_left_type<TYPE_FLOAT>(block, result, col_left_untyped,
                                                    col_right_untyped) ||
                  execute_num_left_type<TYPE_DOUBLE>(block, result, col_left_untyped,
                                                     col_right_untyped) ||
                  execute_num_left_type<TYPE_TIME>(block, result, col_left_untyped,
                                                   col_right_untyped) ||
                  execute_num_left_type<TYPE_TIMEV2>(block, result, col_left_untyped,
                                                     col_right_untyped))) {
                return Status::RuntimeError("Illegal column {} of first argument of function {}",
                                            col_left_untyped->get_name(), get_name());
            }
            return Status::OK();
        }
        if (left_type->get_primitive_type() == TYPE_DECIMALV2 ||
            right_type->get_primitive_type() == TYPE_DECIMALV2) {
            if (!allow_decimal_comparison(left_type, right_type)) {
                return Status::RuntimeError("No operation {} between {} and {}", get_name(),
                                            left_type->get_name(), right_type->get_name());
            }
            return execute_decimal(block, result, col_with_type_and_name_left,
                                   col_with_type_and_name_right);
        }

        if (is_decimal(left_type->get_primitive_type()) ||
            is_decimal(right_type->get_primitive_type())) {
            if (!allow_decimal_comparison(left_type, right_type)) {
                return Status::RuntimeError("No operation {} between {} and {}", get_name(),
                                            left_type->get_name(), right_type->get_name());
            }
            return execute_decimal(block, result, col_with_type_and_name_left,
                                   col_with_type_and_name_right);
        }

        // Types from left and right hand should be same (char/varchar/string are string type.)
        if (!(left_type->get_primitive_type() == right_type->get_primitive_type() ||
              (is_string_type(left_type->get_primitive_type()) &&
               is_string_type(right_type->get_primitive_type())))) {
            return Status::InternalError(
                    "comparison must input two same type column or column type is "
                    "decimalv3/numeric, lhs={}, rhs={}",
                    col_with_type_and_name_left.type->get_name(),
                    col_with_type_and_name_right.type->get_name());
        }

        if (left_is_string && right_is_string) {
            return execute_string(block, result, col_with_type_and_name_left.column.get(),
                                  col_with_type_and_name_right.column.get());
        } else {
            // TODO: varchar and string maybe need a quickly way
            return execute_generic(block, result, col_with_type_and_name_left,
                                   col_with_type_and_name_right);
        }
        return Status::OK();
    }
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized