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

#pragma once

#include "core/column/column_nullable.h"
#include "core/data_type/data_type_variant.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_to_string.h"

namespace doris::CastWrapper {

// shared implementation for casting from variant to arbitrary non-nullable target type
inline Status cast_from_variant_impl(FunctionContext* context, Block& block,
                                     const ColumnNumbers& arguments, uint32_t result,
                                     size_t input_rows_count, const NullMap::value_type* null_map,
                                     const DataTypePtr& data_type_to) {
    auto& col_with_type_and_name = block.get_by_position(arguments[0]);
    auto& col_from = col_with_type_and_name.column;
    const IColumn* variant_column = col_from.get();
    const auto* nullable = check_and_get_column<ColumnNullable>(*variant_column);
    if (nullable != nullptr) {
        variant_column = &nullable->get_nested_column();
    }
    const auto* variant = assert_cast<const ColumnVariant*>(variant_column);
    ColumnPtr col_to = data_type_to->create_column();

    ColumnPtr finalized_input_column;
    if (!variant->is_finalized()) {
        // Local exchange can share the same input block across multiple downstream tasks.
        // Finalize a private copy so variant casts never mutate shared input columns.
        auto finalized_variant = variant->clone_finalized();
        variant = assert_cast<const ColumnVariant*>(finalized_variant.get());
        if (nullable != nullptr) {
            finalized_input_column = ColumnNullable::create(std::move(finalized_variant),
                                                            nullable->get_null_map_column_ptr());
        } else {
            finalized_input_column = std::move(finalized_variant);
        }
    }
    auto execute_on_finalized_input = [&](auto&& executor) -> Status {
        if (!finalized_input_column) {
            return executor(block);
        }
        Block finalized_block = block;
        finalized_block.replace_by_position(arguments[0], finalized_input_column);
        RETURN_IF_ERROR(executor(finalized_block));
        block.replace_by_position(result, finalized_block.get_by_position(result).column);
        return Status::OK();
    };

    // It's important to convert as many elements as possible in this context. For instance,
    // if the root of this variant column is a number column, converting it to a number column
    // is acceptable. However, if the destination type is a string and root is none scalar root, then
    // we should convert the entire tree to a string.
    bool is_root_valuable = variant->is_scalar_variant() ||
                            (!variant->is_null_root() &&
                             variant->get_root_type()->get_primitive_type() != INVALID_TYPE &&
                             !is_string_type(data_type_to->get_primitive_type()) &&
                             data_type_to->get_primitive_type() != TYPE_JSONB);

    if (is_root_valuable) {
        ColumnPtr nested = variant->get_root();
        auto nested_from_type = variant->get_root_type();
        // DCHECK(nested_from_type->is_nullable());
        DCHECK(!data_type_to->is_nullable());
        auto new_context = context == nullptr ? nullptr : context->clone();
        if (new_context != nullptr) {
            new_context->set_jsonb_string_as_string(true);
            // Disable strict mode for the inner JSONB→target conversion.
            // The variant root column may contain null/empty JSONB entries for rows
            // where the subcolumn doesn't exist (e.g., mixed-schema variant data).
            // In strict mode (INSERT context), these null entries cause the ENTIRE
            // cast to fail and return all NULLs. Since this is an internal type
            // conversion within variant, not user-provided INSERT data validation,
            // strict mode should not apply here.
            new_context->set_enable_strict_mode(false);
        }
        // dst type nullable has been removed, so we should remove the inner nullable of root column
        auto wrapper =
                prepare_impl(new_context.get(), remove_nullable(nested_from_type), data_type_to);
        Block tmp_block {{remove_nullable(nested), remove_nullable(nested_from_type), ""}};
        tmp_block.insert({nullptr, data_type_to, ""});
        /// Perform the requested conversion.
        Status st = wrapper(new_context.get(), tmp_block, {0}, 1, input_rows_count, nullptr);
        if (!st.ok()) {
            // Fill with default values, which is null
            col_to->assert_mutable()->insert_many_defaults(input_rows_count);
            col_to = make_nullable(col_to, true);
        } else {
            col_to = tmp_block.get_by_position(1).column;
            col_to = wrap_in_nullable(col_to,
                                      Block({{nested, nested_from_type, ""},
                                             {col_from, col_with_type_and_name.type, ""},
                                             {col_to, data_type_to, ""}}),
                                      {0, 1}, input_rows_count);
        }
    } else {
        if (variant->only_have_default_values()) {
            col_to->assert_mutable()->insert_many_defaults(input_rows_count);
            col_to = make_nullable(col_to, true);
        } else if (is_string_type(data_type_to->get_primitive_type())) {
            // serialize to string
            return execute_on_finalized_input([&](Block& finalized_block) {
                return CastToStringFunction::execute_impl(context, finalized_block, arguments,
                                                          result, input_rows_count);
            });
        } else if (data_type_to->get_primitive_type() == TYPE_JSONB) {
            // serialize to json by parsing
            return execute_on_finalized_input([&](Block& finalized_block) {
                return cast_from_generic_to_jsonb(context, finalized_block, arguments, result,
                                                  input_rows_count);
            });
        } else if (!data_type_to->is_nullable() &&
                   !is_string_type(data_type_to->get_primitive_type())) {
            // other types
            col_to->assert_mutable()->insert_many_defaults(input_rows_count);
            col_to = make_nullable(col_to, true);
        } else {
            assert_cast<ColumnNullable&>(*col_to->assert_mutable())
                    .insert_many_defaults(input_rows_count);
        }
    }

    if (null_map == nullptr) {
        if (const auto* nullable_result = check_and_get_column<ColumnNullable>(*col_to);
            nullable_result != nullptr && !nullable_result->has_null()) {
            col_to = nullable_result->get_nested_column_ptr();
        }
    }

    if (col_to->size() != input_rows_count) {
        return Status::InternalError("Unmatched row count {}, expected {}", col_to->size(),
                                     input_rows_count);
    }

    block.replace_by_position(result, std::move(col_to));
    return Status::OK();
}

struct CastFromVariant {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          const NullMap::value_type* null_map = nullptr) {
        auto& data_type_to = block.get_by_position(result).type;
        return cast_from_variant_impl(context, block, arguments, result, input_rows_count, null_map,
                                      data_type_to);
    }
};

struct CastToVariant {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          const NullMap::value_type* null_map = nullptr) {
        // auto& data_type_to = block.get_by_position(result).type;
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const auto& from_type = col_with_type_and_name.type;
        const auto& col_from = col_with_type_and_name.column;
        // set variant root column/type to from column/type
        const auto& data_type_to = block.get_by_position(result).type;
        const auto* variant_type =
                typeid_cast<const DataTypeVariant*>(remove_nullable(data_type_to).get());
        auto variant = ColumnVariant::create(
                variant_type ? variant_type->variant_max_subcolumns_count() : 0,
                variant_type ? variant_type->enable_doc_mode() : false);
        variant->create_root(from_type, IColumn::mutate(col_from));
        block.replace_by_position(result, std::move(variant));
        return Status::OK();
    }
};

// create corresponding variant value to wrap from_type
WrapperType create_cast_to_variant_wrapper(const DataTypePtr& from_type,
                                           const DataTypeVariant& to_type) {
    if (from_type->get_primitive_type() == TYPE_VARIANT) {
        // variant_max_subcolumns_count is not equal
        return create_unsupport_wrapper(from_type->get_name(), to_type.get_name());
    }
    return &CastToVariant::execute;
}

// create corresponding type convert from variant
WrapperType create_cast_from_variant_wrapper(const DataTypeVariant& from_type,
                                             const DataTypePtr& to_type) {
    if (to_type->get_primitive_type() == TYPE_VARIANT) {
        // variant_max_subcolumns_count is not equal
        return create_unsupport_wrapper(from_type.get_name(), to_type->get_name());
    }
    // Capture explicit target type to make the cast independent from Block[result].type.
    DataTypePtr captured_to_type = to_type;
    return [captured_to_type](FunctionContext* context, Block& block,
                              const ColumnNumbers& arguments, uint32_t result,
                              size_t input_rows_count,
                              const NullMap::value_type* null_map) -> Status {
        return cast_from_variant_impl(context, block, arguments, result, input_rows_count, null_map,
                                      captured_to_type);
    };
}

} // namespace doris::CastWrapper
