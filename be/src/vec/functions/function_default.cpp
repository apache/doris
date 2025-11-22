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

#include "vec/functions/function_default.h"

#include <mysql/binary_log_types.h>

#include <string>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class FunctionDefault : public IFunction {
public:
    static constexpr auto name = "default";
    static FunctionPtr create() { return std::make_shared<FunctionDefault>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(arguments[0]);
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status prepare_with_children(FunctionContext* fn_context, RuntimeState* state,
                                 const RowDescriptor& desc, VExprContext* context,
                                 const std::vector<VExprSPtr>& children) override {
        if (!children.empty()) {
            auto default_state = std::make_shared<DefaultFunctionState>();
            auto* slot_ref = dynamic_cast<VSlotRef*>(children[0].get());
            if (slot_ref != nullptr && slot_ref->slot_id() != -1) {
                const auto* slot_desc = state->desc_tbl().get_slot_descriptor(slot_ref->slot_id());

                default_state->slot_id = slot_ref->slot_id();
                default_state->is_nullable = slot_desc->is_nullable();
                default_state->has_default_value = slot_desc->has_default_value();
                if (default_state->has_default_value) {
                    default_state->default_value = slot_desc->col_default_value();
                }
            }
            fn_context->set_function_state(FunctionContext::FRAGMENT_LOCAL, default_state);
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        ColumnWithTypeAndName& result_info = block.get_by_position(result);
        auto res_nested_type = remove_nullable(result_info.type);
        PrimitiveType res_primitive_type = res_nested_type->get_primitive_type();

        ColumnWithTypeAndName& input_column_info = block.get_by_position(arguments[0]);
        const std::string& col_name = input_column_info.name;

        std::string default_value;
        bool has_default_value = false;
        bool is_nullable = true;
        const auto* slot_state = static_cast<const DefaultFunctionState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (slot_state != nullptr && slot_state->slot_id != -1) {
            default_value = slot_state->default_value;
            has_default_value = slot_state->has_default_value;
            is_nullable = slot_state->is_nullable;
        }

        // For date types, if the default value is `CURRENT_TIMESTAMP` or `CURRENT_DATE`.
        // Reference the behavior in MySQL:
        // if column is NULLABLE, return all NULLs
        // else return zero datetime values like `0000-00-00 00:00:00` or `0000-00-00`
        // Because Doris has a range limit for date & datetime, `0000-00-00` cannot be represented.
        // So here we use the smallest representable value instead
        if (is_date_type(res_primitive_type) && has_default_value &&
            (default_value == "CURRENT_TIMESTAMP" || default_value == "CURRENT_DATE")) {
            if (is_nullable) {
                return return_with_all_null(block, result, res_nested_type, input_rows_count);
            } else {
                return return_with_zero_datetime(block, result, res_nested_type, res_primitive_type,
                                                 input_rows_count);
            }
        }

        // Agg state(HLL, BITMAPA, QUANTILE_STATE) error only
        if (is_var_len_object(res_primitive_type)) {
            return Status::InvalidArgument(
                    "Agg type(HLL, BITMAP, QUANTILE_STATE) cannot be used for the DEFAULT "
                    "function");
        }

        // 1. specified default value when creating table -> default_value
        // 2. no specified default value && column is NOT NULL -> error
        // 3. no specified default value && column is NULLABLE -> NULL
        if (has_default_value) {
            MutableColumnPtr res_col = res_nested_type->create_column();
            auto null_map = ColumnUInt8::create(input_rows_count, 0);
            Field default_field;

            auto temp_column = res_nested_type->create_column();
            auto serde = res_nested_type->get_serde();
            StringRef default_str_ref(default_value.data(), default_value.size());
            DataTypeSerDe::FormatOptions options;
            Status parse_status = serde->from_string(default_str_ref, *temp_column, options);

            if (parse_status.ok() && temp_column->size() > 0) {
                temp_column->get(0, default_field);
                res_col->insert(default_field);
                block.replace_by_position(
                        result, ColumnNullable::create(
                                        ColumnConst::create(std::move(res_col), input_rows_count),
                                        std::move(null_map)));
            } else [[unlikely]] {
                return Status::FatalError("Failed to parse default value for column '{}'",
                                          col_name);
            }
        } else {
            if (is_nullable) {
                return return_with_all_null(block, result, res_nested_type, input_rows_count);
            } else {
                return Status::InvalidArgument("Column '{}' is NOT NULL but has no default value",
                                               col_name);
            }
        }
        return Status::OK();
    }

private:
    static Status return_with_all_null(Block& block, uint32_t result,
                                       const DataTypePtr& nested_type, size_t input_rows_count) {
        MutableColumnPtr res_col = nested_type->create_column();
        res_col->insert_default();
        auto null_map = ColumnUInt8::create(input_rows_count, 1);
        block.replace_by_position(
                result,
                ColumnNullable::create(ColumnConst::create(std::move(res_col), input_rows_count),
                                       std::move(null_map)));
        return Status::OK();
    }

    static Status return_with_zero_datetime(Block& block, uint32_t result,
                                            const DataTypePtr& nested_type,
                                            PrimitiveType primitive_type, size_t input_rows_count) {
        MutableColumnPtr res_col = nested_type->create_column();

        switch (primitive_type) {
        case TYPE_DATEV2:
            insert_min_datetime_value<TYPE_DATEV2>(res_col);
            break;
        case TYPE_DATETIMEV2:
            insert_min_datetime_value<TYPE_DATETIMEV2>(res_col);
            break;
        default:
            return Status::InternalError("Unsupported date/time type for zero datetime: {}",
                                         nested_type->get_name());
        }

        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        block.replace_by_position(
                result,
                ColumnNullable::create(ColumnConst::create(std::move(res_col), input_rows_count),
                                       std::move(null_map)));
        return Status::OK();
    }

    template <PrimitiveType Type>
    static void insert_min_datetime_value(MutableColumnPtr& res_col) {
        DCHECK(Type == TYPE_DATEV2 || Type == TYPE_DATETIMEV2);
        using ItemType = typename PrimitiveTypeTraits<Type>::ColumnItemType;
        ItemType min_value;

        if constexpr (Type == TYPE_DATEV2) {
            min_value = MIN_DATE_V2;
        } else if constexpr (Type == TYPE_DATETIMEV2) {
            min_value = MIN_DATETIME_V2;
        }

        res_col->insert_data(reinterpret_cast<const char*>(&min_value), sizeof(ItemType));
    }
};
#include "common/compile_check_end.h"

void register_function_default(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDefault>();
}
} // namespace doris::vectorized