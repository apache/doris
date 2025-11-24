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

#include <bitset>
#include <cstdint>
#include <exception>
#include <type_traits>

#include "common/compiler_util.h"
#include "common/status.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename Impl>
class FunctionBitShift : public IFunction {
public:
    static constexpr auto name = Impl::name;

    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionBitShift<Impl>>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt64>();
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

        auto res = ColumnInt64::create(1);
        res->get_element(0) = Impl::apply(column_left_ptr->template get_value<int64_t>(),
                                          column_right_ptr->template get_value<int8_t>());
        column_result = std::move(res);
        return ColumnConst::create(std::move(column_result), column_left->size());
    }

    ColumnPtr vector_constant(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_right_ptr = assert_cast<const ColumnConst*>(column_right.get());
        const auto* column_left_ptr = assert_cast<const ColumnInt64*>(column_left.get());
        auto column_result = ColumnInt64::create(column_left->size());

        auto& a = column_left_ptr->get_data();
        auto& c = column_result->get_data();
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Impl::apply(a[i], column_right_ptr->template get_value<int8_t>());
        }
        return column_result;
    }

    ColumnPtr constant_vector(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_left_ptr = assert_cast<const ColumnConst*>(column_left.get());
        const auto* column_right_ptr = assert_cast<const ColumnInt8*>(column_right.get());
        auto column_result = ColumnInt64::create(column_right->size());

        auto& b = column_right_ptr->get_data();
        auto& c = column_result->get_data();
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Impl::apply(column_left_ptr->template get_value<int64_t>(), b[i]);
        }
        return column_result;
    }

    ColumnPtr vector_vector(ColumnPtr column_left, ColumnPtr column_right) const {
        const auto* column_left_ptr = assert_cast<const ColumnInt64*>(column_left->get_ptr().get());
        const auto* column_right_ptr =
                assert_cast<const ColumnInt8*>(column_right->get_ptr().get());

        auto column_result = ColumnInt64::create(column_left->size());

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

struct BitShiftLeftImpl {
    static constexpr auto name = "bit_shift_left";

    static inline Int64 apply(Int64 a, Int8 b) {
        // return zero if b < 0, keep consistent with mysql
        // cast to unsigned so that we can do logical shift by default, keep consistent with mysql
        if (UNLIKELY(b >= 64 || b < 0)) {
            return 0;
        }
        return static_cast<typename std::make_unsigned<Int64>::type>(a) << static_cast<Int64>(b);
    }
};

struct BitShiftRightImpl {
    static constexpr auto name = "bit_shift_right";

    static inline Int64 apply(Int64 a, Int8 b) {
        // return zero if b < 0, keep consistent with mysql
        // cast to unsigned so that we can do logical shift by default, keep consistent with mysql
        if (UNLIKELY(b >= 64 || b < 0)) {
            return 0;
        }
        return static_cast<typename std::make_unsigned<Int64>::type>(a) >> static_cast<Int64>(b);
    }
};

void register_function_bit_shift(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBitShift<BitShiftRightImpl>>();
    factory.register_function<FunctionBitShift<BitShiftLeftImpl>>();
}

} // namespace doris::vectorized
