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

#include <fmt/format.h>
#include <glog/logging.h>
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "runtime/thread_context.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

// array_apply([1, 2, 3, 10], ">=", 5) -> [10]
// This function is temporary, use it to meet the requirement before implementing the lambda function.
class FunctionArrayApply : public IFunction {
public:
    static constexpr auto name = "array_apply";

    static FunctionPtr create() { return std::make_shared<FunctionArrayApply>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 3; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1, 2}; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& src_column_array = check_and_get_column<ColumnArray>(*src_column);
        if (!src_column_array) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name()));
        }
        const auto& src_offsets = src_column_array->get_offsets();
        const auto* src_nested_column = &src_column_array->get_data();
        DCHECK(src_nested_column != nullptr);

        DataTypePtr src_column_type = block.get_by_position(arguments[0]).type;
        auto nested_type = assert_cast<const DataTypeArray&>(*src_column_type).get_nested_type();
        const std::string& condition =
                block.get_by_position(arguments[1]).column->get_data_at(0).to_string();

        const ColumnConst& rhs_value_column =
                static_cast<const ColumnConst&>(*block.get_by_position(arguments[2]).column.get());
        ColumnPtr result_ptr;
        RETURN_IF_CATCH_EXCEPTION(
                RETURN_IF_ERROR(_execute(*src_nested_column, nested_type, src_offsets, condition,
                                         rhs_value_column, &result_ptr)));
        block.replace_by_position(result, std::move(result_ptr));
        return Status::OK();
    }

private:
    enum class ApplyOp {
        UNKNOWN = 0,
        EQ = 1,
        NE = 2,
        LT = 3,
        LE = 4,
        GT = 5,
        GE = 6,
    };
    template <typename T, ApplyOp op>
    bool apply(T data, T comp) const {
        if constexpr (op == ApplyOp::EQ) {
            return data == comp;
        }
        if constexpr (op == ApplyOp::NE) {
            return data != comp;
        }
        if constexpr (op == ApplyOp::LT) {
            return data < comp;
        }
        if constexpr (op == ApplyOp::LE) {
            return data <= comp;
        }
        if constexpr (op == ApplyOp::GT) {
            return data > comp;
        }
        if constexpr (op == ApplyOp::GE) {
            return data >= comp;
        }
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }

    // need exception safety
    template <typename T, ApplyOp op>
    ColumnPtr _apply_internal(const IColumn& src_column, const ColumnArray::Offsets64& src_offsets,
                              const ColumnConst& cmp) const {
        T rhs_val = *reinterpret_cast<const T*>(cmp.get_data_at(0).data);
        auto column_filter = ColumnUInt8::create(src_column.size(), 0);
        auto& column_filter_data = column_filter->get_data();
        const char* src_column_data_ptr = nullptr;
        if (!src_column.is_nullable()) {
            src_column_data_ptr = src_column.get_raw_data().data;
        } else {
            src_column_data_ptr = check_and_get_column<ColumnNullable>(src_column)
                                          ->get_nested_column()
                                          .get_raw_data()
                                          .data;
        }
        const T* src_column_data_t_ptr = reinterpret_cast<const T*>(src_column_data_ptr);
        const size_t src_column_size = src_column.size();
        for (size_t i = 0; i < src_column_size; ++i) {
            column_filter_data[i] = apply<T, op>(src_column_data_t_ptr[i], rhs_val);
        }
        const IColumn::Filter& filter = column_filter_data;
        ColumnPtr filtered = src_column.filter(filter, src_column.size());
        auto column_offsets = ColumnArray::ColumnOffsets::create(src_offsets.size());
        ColumnArray::Offsets64& dst_offsets = column_offsets->get_data();
        size_t in_pos = 0;
        size_t out_pos = 0;
        for (size_t i = 0; i < src_offsets.size(); ++i) {
            for (; in_pos < src_offsets[i]; ++in_pos) {
                if (filter[in_pos]) {
                    ++out_pos;
                }
            }
            dst_offsets[i] = out_pos;
        }
        return ColumnArray::create(filtered, std::move(column_offsets));
    }

// need exception safety
#define APPLY_ALL_TYPES(src_column, src_offsets, OP, cmp, dst)                     \
    do {                                                                           \
        WhichDataType which(remove_nullable(nested_type));                         \
        if (which.is_uint8()) {                                                    \
            *dst = _apply_internal<UInt8, OP>(src_column, src_offsets, cmp);       \
        } else if (which.is_int8()) {                                              \
            *dst = _apply_internal<Int8, OP>(src_column, src_offsets, cmp);        \
        } else if (which.is_int16()) {                                             \
            *dst = _apply_internal<Int16, OP>(src_column, src_offsets, cmp);       \
        } else if (which.is_int32()) {                                             \
            *dst = _apply_internal<Int32, OP>(src_column, src_offsets, cmp);       \
        } else if (which.is_int64()) {                                             \
            *dst = _apply_internal<Int64, OP>(src_column, src_offsets, cmp);       \
        } else if (which.is_int128()) {                                            \
            *dst = _apply_internal<Int128, OP>(src_column, src_offsets, cmp);      \
        } else if (which.is_float32()) {                                           \
            *dst = _apply_internal<Float32, OP>(src_column, src_offsets, cmp);     \
        } else if (which.is_float64()) {                                           \
            *dst = _apply_internal<Float64, OP>(src_column, src_offsets, cmp);     \
        } else if (which.is_date()) {                                              \
            *dst = _apply_internal<Int64, OP>(src_column, src_offsets, cmp);       \
        } else if (which.is_date_time()) {                                         \
            *dst = _apply_internal<Int64, OP>(src_column, src_offsets, cmp);       \
        } else if (which.is_date_v2()) {                                           \
            *dst = _apply_internal<UInt32, OP>(src_column, src_offsets, cmp);      \
        } else if (which.is_date_time_v2()) {                                      \
            *dst = _apply_internal<UInt64, OP>(src_column, src_offsets, cmp);      \
        } else if (which.is_date_time_v2()) {                                      \
            *dst = _apply_internal<UInt64, OP>(src_column, src_offsets, cmp);      \
        } else if (which.is_decimal32()) {                                         \
            *dst = _apply_internal<Decimal32, OP>(src_column, src_offsets, cmp);   \
        } else if (which.is_decimal64()) {                                         \
            *dst = _apply_internal<Decimal64, OP>(src_column, src_offsets, cmp);   \
        } else if (which.is_decimal128()) {                                        \
            *dst = _apply_internal<Decimal128, OP>(src_column, src_offsets, cmp);  \
        } else if (which.is_decimal128i()) {                                       \
            *dst = _apply_internal<Decimal128I, OP>(src_column, src_offsets, cmp); \
        } else if (which.is_decimal256()) {                                        \
            *dst = _apply_internal<Decimal256, OP>(src_column, src_offsets, cmp);  \
        } else {                                                                   \
            LOG(FATAL) << "unsupported type " << nested_type->get_name();          \
        }                                                                          \
    } while (0)

    // need exception safety
    Status _execute(const IColumn& nested_src, DataTypePtr nested_type,
                    const ColumnArray::Offsets64& offsets, const std::string& condition,
                    const ColumnConst& rhs_value_column, ColumnPtr* dst) const {
        if (condition == "=") {
            APPLY_ALL_TYPES(nested_src, offsets, ApplyOp::EQ, rhs_value_column, dst);
        } else if (condition == "!=") {
            APPLY_ALL_TYPES(nested_src, offsets, ApplyOp::NE, rhs_value_column, dst);
        } else if (condition == ">=") {
            APPLY_ALL_TYPES(nested_src, offsets, ApplyOp::GE, rhs_value_column, dst);
        } else if (condition == "<=") {
            APPLY_ALL_TYPES(nested_src, offsets, ApplyOp::LE, rhs_value_column, dst);
        } else if (condition == "<") {
            APPLY_ALL_TYPES(nested_src, offsets, ApplyOp::LT, rhs_value_column, dst);
        } else if (condition == ">") {
            APPLY_ALL_TYPES(nested_src, offsets, ApplyOp::GT, rhs_value_column, dst);
        } else {
            return Status::RuntimeError(
                    fmt::format("execute failed, unsupported op {} for function {})", condition,
                                "array_apply"));
        }
        return Status::OK();
    }
};

void register_function_array_apply(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayApply>();
}

} // namespace doris::vectorized
