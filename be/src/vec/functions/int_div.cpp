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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/IntDiv.cpp
// and modified by Doris

#include <libdivide.h>

#include <utility>

#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename Impl>
class FunctionIntDiv : public IFunction {
public:
    static constexpr auto name = "int_divide";

    static FunctionPtr create() { return std::make_shared<FunctionIntDiv>(); }

    FunctionIntDiv() = default;

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr type_res =
                std::make_shared<typename PrimitiveTypeTraits<Impl::ResultType>::DataType>();
        return make_nullable(type_res);
    }

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
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        ColumnPtr column_result = nullptr;

        column_result =
                Impl::constant_constant(column_left_ptr->template get_value<typename Impl::Arg>(),
                                        column_right_ptr->template get_value<typename Impl::Arg>());

        return ColumnConst::create(std::move(column_result), column_left->size());
    }

    ColumnPtr vector_constant(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_right_ptr = assert_cast<const ColumnConst*>(column_right.get());
        DCHECK(column_right_ptr != nullptr);

        return Impl::vector_constant(column_left->get_ptr(),
                                     column_right_ptr->template get_value<typename Impl::Arg>());
    }

    ColumnPtr constant_vector(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_left_ptr = assert_cast<const ColumnConst*>(column_left.get());
        DCHECK(column_left_ptr != nullptr);

        return Impl::constant_vector(column_left_ptr->template get_value<typename Impl::Arg>(),
                                     column_right->get_ptr());
    }

    ColumnPtr vector_vector(ColumnPtr column_left, ColumnPtr column_right) const {
        return Impl::vector_vector(column_left->get_ptr(), column_right->get_ptr());
    }
};

template <PrimitiveType Type>
struct DivideIntegralImpl {
    using Arg = typename PrimitiveTypeTraits<Type>::ColumnItemType;
    using ColumnType = typename PrimitiveTypeTraits<Type>::ColumnType;
    static constexpr PrimitiveType ResultType = Type;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<Type>::DataType>(),
                std::make_shared<typename PrimitiveTypeTraits<Type>::DataType>()};
    }

    static void apply(const typename ColumnType::Container& a, Arg b,
                      typename PrimitiveTypeTraits<ResultType>::ColumnType::Container& c,
                      PaddedPODArray<UInt8>& null_map) {
        size_t size = c.size();
        UInt8 is_null = b == 0;
        memset(null_map.data(), is_null, size);

        if (!is_null) {
            if constexpr (!std::is_floating_point_v<Arg> && !std::is_same_v<Arg, Int128> &&
                          !std::is_same_v<Arg, Int8> && !std::is_same_v<Arg, UInt8>) {
                const auto divider = libdivide::divider<Arg>(Arg(b));
                for (size_t i = 0; i < size; i++) {
                    c[i] = a[i] / divider;
                }
            } else {
                for (size_t i = 0; i < size; i++) {
                    c[i] = typename PrimitiveTypeTraits<ResultType>::ColumnItemType(a[i] / b);
                }
            }
        }
    }

    static inline typename PrimitiveTypeTraits<ResultType>::ColumnItemType apply(Arg a, Arg b,
                                                                                 UInt8& is_null) {
        is_null = b == 0;
        return typename PrimitiveTypeTraits<ResultType>::ColumnItemType(a / (b + is_null));
    }

    static ColumnPtr constant_constant(Arg a, Arg b) {
        auto column_result = ColumnType ::create(1);

        auto null_map = ColumnUInt8::create(1, 0);
        column_result->get_element(0) = apply(a, b, null_map->get_element(0));
        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }

    static ColumnPtr vector_constant(ColumnPtr column_left, Arg b) {
        const auto* column_left_ptr = assert_cast<const ColumnType*>(column_left.get());
        auto column_result = ColumnType::create(column_left->size());
        DCHECK(column_left_ptr != nullptr);

        auto null_map = ColumnUInt8::create(column_left->size(), 0);
        apply(column_left_ptr->get_data(), b, column_result->get_data(), null_map->get_data());
        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }

    static ColumnPtr constant_vector(Arg a, ColumnPtr column_right) {
        const auto* column_right_ptr = assert_cast<const ColumnType*>(column_right.get());
        auto column_result = ColumnType::create(column_right->size());
        DCHECK(column_right_ptr != nullptr);

        auto null_map = ColumnUInt8::create(column_right->size(), 0);
        auto& b = column_right_ptr->get_data();
        auto& c = column_result->get_data();
        auto& n = null_map->get_data();
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a, b[i], n[i]);
        }
        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }

    static ColumnPtr vector_vector(ColumnPtr column_left, ColumnPtr column_right) {
        const auto* column_left_ptr = assert_cast<const ColumnType*>(column_left.get());
        const auto* column_right_ptr = assert_cast<const ColumnType*>(column_right.get());

        auto column_result = ColumnType::create(column_left->size());
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        auto null_map = ColumnUInt8::create(column_result->size(), 0);
        auto& a = column_left_ptr->get_data();
        auto& b = column_right_ptr->get_data();
        auto& c = column_result->get_data();
        auto& n = null_map->get_data();
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a[i], b[i], n[i]);
        }
        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }
};

void register_function_int_div(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionIntDiv<DivideIntegralImpl<TYPE_TINYINT>>>();
    factory.register_function<FunctionIntDiv<DivideIntegralImpl<TYPE_SMALLINT>>>();
    factory.register_function<FunctionIntDiv<DivideIntegralImpl<TYPE_INT>>>();
    factory.register_function<FunctionIntDiv<DivideIntegralImpl<TYPE_BIGINT>>>();
    factory.register_function<FunctionIntDiv<DivideIntegralImpl<TYPE_LARGEINT>>>();
}

} // namespace doris::vectorized
