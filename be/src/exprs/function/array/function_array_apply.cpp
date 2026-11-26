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

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/call_on_type_index.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/thread_context.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

// array_apply([1, 2, 3, 10], ">=", 5) -> [10]
// This function is temporary, use it to meet the requirement before implementing the lambda function.
class FunctionArrayApply : public IFunction {
public:
    static constexpr auto name = "array_apply";

    static FunctionPtr create() { return std::make_shared<FunctionArrayApply>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(remove_nullable(arguments[0])->get_primitive_type() == TYPE_ARRAY)
                << "first argument for function: " << name << " should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        // use_default_implementation_for_nulls() is disabled below, so, unlike the usual
        // get_return_type_impl contract, arguments here keep their original nullability and the
        // wrapping is not automatic: replicate it by hand from every argument.
        bool nullable = arguments[0]->is_nullable() || arguments[1]->is_nullable() ||
                        arguments[2]->is_nullable();
        DataTypePtr base = remove_nullable(arguments[0]);
        return nullable ? make_nullable(base) : base;
    }

    // op is validated below before the NULL check propagates it like an ordinary value, so the
    // generic nullable-argument shortcut must not run first and silently return NULL for it.
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        if (input_rows_count == 0) {
            block.replace_by_position(result, block.get_by_position(result).type->create_column());
            return Status::OK();
        }

        // op is a constant checked in FE: either a literal or a value only BE can evaluate. A
        // literal NULL is rejected by FE, so a BE-evaluated NULL is rejected here as well, instead
        // of silently propagating NULL through the generic nullable-argument shortcut below.
        if (block.get_by_position(arguments[1]).column->is_null_at(0)) {
            return Status::InvalidArgument(
                    "array_apply(arr, op, val): op support const value only.");
        }

        const ColumnWithTypeAndName& src_with_type = block.get_by_position(arguments[0]);
        const ColumnWithTypeAndName& val_with_type = block.get_by_position(arguments[2]);
        bool src_nullable = src_with_type.type->is_nullable();
        bool val_nullable = val_with_type.type->is_nullable();
        // op is nullable-typed when one branch of a BE-only expression (e.g. an IF) is NULL, even
        // though the branch actually taken, and thus the row-0 value read below, is not: the NULL
        // check above already rejects an actual NULL, so op never contributes to the null map.
        bool op_nullable = block.get_by_position(arguments[1]).type->is_nullable();
        NullableColumnInfo src_info =
                src_nullable ? src_with_type.get_nullable_column_info() : NullableColumnInfo {};
        NullableColumnInfo val_info =
                val_nullable ? val_with_type.get_nullable_column_info() : NullableColumnInfo {};
        if ((src_nullable && src_info.only_null) || (val_nullable && val_info.only_null)) {
            block.get_by_position(result).column =
                    block.get_by_position(result).type->create_column_const(input_rows_count,
                                                                            Field());
            return Status::OK();
        }

        ColumnWithTypeAndName unnested_src =
                src_nullable ? src_with_type.unnest_nullable(src_info, false) : src_with_type;
        const auto& [src_column, src_const] = unpack_if_const(unnested_src.column);
        const auto& src_column_array = check_and_get_column<ColumnArray>(*src_column);
        if (!src_column_array) {
            return Status::RuntimeError(fmt::format("unsupported types for function {}({})",
                                                    get_name(), src_with_type.type->get_name()));
        }
        const auto& src_offsets = src_column_array->get_offsets();
        const auto* src_nested_column = &src_column_array->get_data();
        DCHECK(src_nested_column != nullptr);

        auto nested_type = assert_cast<const DataTypeArray&>(*unnested_src.type).get_nested_type();
        // op and val are constants checked in FE, so the first row holds their values. A constant
        // expression such as an IF one is not a ColumnConst.
        const std::string& condition =
                block.get_by_position(arguments[1]).column->get_data_at(0).to_string();
        const IColumn& rhs_value_column = *val_with_type.column;
        ColumnPtr result_ptr;
        RETURN_IF_CATCH_EXCEPTION(
                RETURN_IF_ERROR(_execute(*src_nested_column, nested_type, src_offsets, condition,
                                         rhs_value_column, &result_ptr)));
        ColumnPtr dense_result = src_const ? ColumnConst::create(result_ptr, input_rows_count)
                                           : std::move(result_ptr);
        block.replace_by_position(
                result, (src_nullable || op_nullable || val_nullable)
                                ? wrap_in_nullable(dense_result, block, arguments, input_rows_count)
                                : std::move(dense_result));
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
        throw Exception(Status::FatalError("__builtin_unreachable"));
    }

    // need exception safety
    template <typename T, ApplyOp op>
    ColumnPtr _apply_internal(const IColumn& src_column, const ColumnArray::Offsets64& src_offsets,
                              const IColumn& cmp) const {
        T rhs_val = *reinterpret_cast<const T*>(cmp.get_data_at(0).data);
        auto column_filter = ColumnUInt8::create(src_column.size(), 0);
        auto& column_filter_data = column_filter->get_data();
        const char* src_column_data_ptr = nullptr;
        const uint8_t* null_map_data = nullptr;
        if (!is_column_nullable(src_column)) {
            src_column_data_ptr = src_column.get_raw_data().data;
        } else {
            const auto* nullable_col = assert_cast<const ColumnNullable*>(&src_column);
            src_column_data_ptr = nullable_col->get_nested_column().get_raw_data().data;
            null_map_data = nullable_col->get_null_map_data().data();
        }
        const T* src_column_data_t_ptr = reinterpret_cast<const T*>(src_column_data_ptr);
        const size_t src_column_size = src_column.size();
        for (size_t i = 0; i < src_column_size; ++i) {
            if (null_map_data && null_map_data[i]) {
                continue; // null elements should not pass the filter
            }
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

    template <ApplyOp OP>
    void dispatch_array_scalar(DataTypePtr nested_type, const IColumn& src_column,
                               const ColumnArray::Offsets64& src_offsets, const IColumn& cmp,
                               ColumnPtr* dst) const {
        auto call = [&](const auto& type) -> bool {
            using DispatchType = std::decay_t<decltype(type)>;
            constexpr PrimitiveType PType = DispatchType::PType;
            *dst = _apply_internal<typename PrimitiveTypeTraits<PType>::CppType, OP>(
                    src_column, src_offsets, cmp);
            return true;
        };

        if (!dispatch_switch_scalar(nested_type->get_primitive_type(), call)) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "array_apply only accept array with nested type which is "
                                   "uint/int/decimal/float/date but got : " +
                                           nested_type->get_name());
        }
    }
    // need exception safety
    Status _execute(const IColumn& nested_src, DataTypePtr nested_type,
                    const ColumnArray::Offsets64& offsets, const std::string& condition,
                    const IColumn& rhs_value_column, ColumnPtr* dst) const {
        if (condition == "=") {
            dispatch_array_scalar<ApplyOp::EQ>(nested_type, nested_src, offsets, rhs_value_column,
                                               dst);
        } else if (condition == "!=") {
            dispatch_array_scalar<ApplyOp::NE>(nested_type, nested_src, offsets, rhs_value_column,
                                               dst);
        } else if (condition == "<") {
            dispatch_array_scalar<ApplyOp::LT>(nested_type, nested_src, offsets, rhs_value_column,
                                               dst);
        } else if (condition == "<=") {
            dispatch_array_scalar<ApplyOp::LE>(nested_type, nested_src, offsets, rhs_value_column,
                                               dst);
        } else if (condition == ">") {
            dispatch_array_scalar<ApplyOp::GT>(nested_type, nested_src, offsets, rhs_value_column,
                                               dst);
        } else if (condition == ">=") {
            dispatch_array_scalar<ApplyOp::GE>(nested_type, nested_src, offsets, rhs_value_column,
                                               dst);
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

} // namespace doris
