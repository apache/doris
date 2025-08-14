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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/bitAnd.cpp
// and modified by Doris

#include <utility>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/function_unary_arithmetic.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename Impl>
class FunctionBit : public IFunction {
public:
    static constexpr auto name = Impl::name;

    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionBit<Impl>>(); }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<typename Impl::DataType>(),
                std::make_shared<typename Impl::DataType>()};
    }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<typename Impl::DataType>();
    }

    size_t get_number_of_arguments() const override { return 2; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto& column_left = block.get_by_position(arguments[0]).column;
        auto& column_right = block.get_by_position(arguments[1]).column;
        bool is_const_left = is_column_const(*column_left);
        bool is_const_right = is_column_const(*column_right);

        ColumnPtr column_result = nullptr;
        if (is_const_left && is_const_right) {
            column_result = constant_constant(column_left, column_right);
        } else if (is_const_left) {
            column_result = constant_vector(column_left, column_right);
        } else if (is_const_right) {
            column_result = vector_constant(column_left, column_right);
        } else {
            column_result = vector_vector(column_left, column_right);
        }
        block.replace_by_position(result, std::move(column_result));

        return Status::OK();
    }

private:
    ColumnPtr constant_constant(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_left_ptr = assert_cast<const ColumnConst*>(column_left.get());
        const auto* column_right_ptr = assert_cast<const ColumnConst*>(column_right.get());
        ColumnPtr column_result = nullptr;

        auto res = Impl::ColumnType::create(1);
        res->get_element(0) =
                Impl::apply(column_left_ptr->template get_value<typename Impl::Arg>(),
                            column_right_ptr->template get_value<typename Impl::Arg>());
        column_result = std::move(res);
        return ColumnConst::create(std::move(column_result), column_left->size());
    }

    ColumnPtr vector_constant(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_right_ptr = assert_cast<const ColumnConst*>(column_right.get());
        const auto* column_left_ptr =
                assert_cast<const typename Impl::ColumnType*>(column_left.get());
        auto column_result = Impl::ColumnType::create(column_left->size());

        auto& a = column_left_ptr->get_data();
        auto& c = column_result->get_data();
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Impl::apply(a[i], column_right_ptr->template get_value<typename Impl::Arg>());
        }
        return column_result;
    }

    ColumnPtr constant_vector(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_left_ptr = assert_cast<const ColumnConst*>(column_left.get());

        const auto* column_right_ptr =
                assert_cast<const typename Impl::ColumnType*>(column_right.get());
        auto column_result = Impl::ColumnType::create(column_right->size());

        auto& b = column_right_ptr->get_data();
        auto& c = column_result->get_data();
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Impl::apply(column_left_ptr->template get_value<typename Impl::Arg>(), b[i]);
        }
        return column_result;
    }

    ColumnPtr vector_vector(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_left_ptr =
                assert_cast<const typename Impl::ColumnType*>(column_left->get_ptr().get());
        const auto* column_right_ptr =
                assert_cast<const typename Impl::ColumnType*>(column_right->get_ptr().get());

        auto column_result = Impl::ColumnType::create(column_left->size());

        auto& a = column_left_ptr->get_data();
        auto& b = column_right_ptr->get_data();
        auto& c = column_result->get_data();
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Impl::apply(a[i], b[i]);
        }
        return column_result;
    }
};

template <PrimitiveType PType>
struct BitAndImpl {
    static_assert(is_int(PType));
    using Arg = typename PrimitiveTypeTraits<PType>::CppNativeType;
    using DataType = typename PrimitiveTypeTraits<PType>::DataType;
    using ColumnType = typename PrimitiveTypeTraits<PType>::ColumnType;
    static constexpr auto name = "bitand";
    static constexpr bool is_nullable = false;
    static inline Arg apply(Arg a, Arg b) { return a & b; }
};

template <PrimitiveType PType>
struct BitOrImpl {
    static_assert(is_int(PType));
    using Arg = typename PrimitiveTypeTraits<PType>::CppNativeType;
    using DataType = typename PrimitiveTypeTraits<PType>::DataType;
    using ColumnType = typename PrimitiveTypeTraits<PType>::ColumnType;
    static constexpr auto name = "bitor";
    static constexpr bool is_nullable = false;
    static inline Arg apply(Arg a, Arg b) { return a | b; }
};

template <PrimitiveType PType>
struct BitXorImpl {
    static_assert(is_int(PType));
    using Arg = typename PrimitiveTypeTraits<PType>::CppNativeType;
    using DataType = typename PrimitiveTypeTraits<PType>::DataType;
    using ColumnType = typename PrimitiveTypeTraits<PType>::ColumnType;
    static constexpr auto name = "bitxor";
    static constexpr bool is_nullable = false;
    static inline Arg apply(Arg a, Arg b) { return a ^ b; }
};

struct NameBitNot {
    static constexpr auto name = "bitnot";
};

template <typename A>
struct BitNotImpl {
    static constexpr PrimitiveType ResultType = NumberTraits::ResultOfBitNot<A>::Type;

    static inline typename PrimitiveTypeTraits<ResultType>::ColumnItemType apply(A a) {
        return ~static_cast<typename PrimitiveTypeTraits<ResultType>::ColumnItemType>(a);
    }
};

struct NameBitLength {
    static constexpr auto name = "bit_length";
};

struct BitLengthImpl {
    using ReturnType = DataTypeInt32;
    static constexpr auto PrimitiveTypeImpl = PrimitiveType::TYPE_STRING;
    using Type = String;
    using ReturnColumnType = ColumnInt32;

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         PaddedPODArray<Int32>& res) {
        auto size = offsets.size();
        res.resize(size);
        for (int i = 0; i < size; ++i) {
            int str_size = offsets[i] - offsets[i - 1];
            res[i] = (str_size * 8);
        }
        return Status::OK();
    }
};

using FunctionBitNot = FunctionUnaryArithmetic<BitNotImpl, NameBitNot>;
using FunctionBitLength = FunctionUnaryToType<BitLengthImpl, NameBitLength>;

void register_function_bit(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBit<BitAndImpl<TYPE_TINYINT>>>();
    factory.register_function<FunctionBit<BitAndImpl<TYPE_SMALLINT>>>();
    factory.register_function<FunctionBit<BitAndImpl<TYPE_INT>>>();
    factory.register_function<FunctionBit<BitAndImpl<TYPE_BIGINT>>>();
    factory.register_function<FunctionBit<BitAndImpl<TYPE_LARGEINT>>>();

    factory.register_function<FunctionBit<BitOrImpl<TYPE_TINYINT>>>();
    factory.register_function<FunctionBit<BitOrImpl<TYPE_SMALLINT>>>();
    factory.register_function<FunctionBit<BitOrImpl<TYPE_INT>>>();
    factory.register_function<FunctionBit<BitOrImpl<TYPE_BIGINT>>>();
    factory.register_function<FunctionBit<BitOrImpl<TYPE_LARGEINT>>>();

    factory.register_function<FunctionBit<BitXorImpl<TYPE_TINYINT>>>();
    factory.register_function<FunctionBit<BitXorImpl<TYPE_SMALLINT>>>();
    factory.register_function<FunctionBit<BitXorImpl<TYPE_INT>>>();
    factory.register_function<FunctionBit<BitXorImpl<TYPE_BIGINT>>>();
    factory.register_function<FunctionBit<BitXorImpl<TYPE_LARGEINT>>>();

    factory.register_function<FunctionBitNot>();
    factory.register_function<FunctionBitLength>();
}
} // namespace doris::vectorized
