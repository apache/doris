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

#include "cast_base.h"
#include "cast_to_string.h"
#include "vec/data_types/data_type_variant.h"
namespace doris::vectorized::CastWrapper {

struct CastFromVariant {
    static Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          const NullMap::value_type* null_map = nullptr) {
        auto& data_type_to = block.get_by_position(result).type;
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const auto& col_from = col_with_type_and_name.column;
        const auto& variant = assert_cast<const ColumnVariant&>(*col_from);
        ColumnPtr col_to = data_type_to->create_column();
        if (!variant.is_finalized()) {
            // ColumnVariant should be finalized before parsing, finalize maybe modify original column structure
            variant.assume_mutable()->finalize();
        }
        // It's important to convert as many elements as possible in this context. For instance,
        // if the root of this variant column is a number column, converting it to a number column
        // is acceptable. However, if the destination type is a string and root is none scalar root, then
        // we should convert the entire tree to a string.
        bool is_root_valuable = variant.is_scalar_variant() ||
                                (!variant.is_null_root() &&
                                 variant.get_root_type()->get_primitive_type() != INVALID_TYPE &&
                                 !is_string_type(data_type_to->get_primitive_type()) &&
                                 data_type_to->get_primitive_type() != TYPE_JSONB);
        if (is_root_valuable) {
            ColumnPtr nested = variant.get_root();
            auto nested_from_type = variant.get_root_type();
            // DCHECK(nested_from_type->is_nullable());
            DCHECK(!data_type_to->is_nullable());
            auto new_context = context->clone();
            new_context->set_jsonb_string_as_string(true);
            // dst type nullable has been removed, so we should remove the inner nullable of root column
            auto wrapper = prepare_impl(new_context.get(), remove_nullable(nested_from_type),
                                        data_type_to);
            Block tmp_block {{remove_nullable(nested), remove_nullable(nested_from_type), ""}};
            tmp_block.insert({nullptr, data_type_to, ""});
            /// Perform the requested conversion.
            Status st = wrapper(new_context.get(), tmp_block, {0}, 1, input_rows_count, nullptr);
            if (!st.ok()) {
                // Fill with default values, which is null
                col_to->assume_mutable()->insert_many_defaults(input_rows_count);
                col_to = make_nullable(col_to, true);
            } else {
                col_to = tmp_block.get_by_position(1).column;
                // Note: here we should return the nullable result column
                col_to = wrap_in_nullable(
                        col_to, Block({{nested, nested_from_type, ""}, {col_to, data_type_to, ""}}),
                        {0}, 1, input_rows_count);
            }
        } else {
            if (variant.empty()) {
                // TODO not found root cause, a tmp fix
                col_to->assume_mutable()->insert_many_defaults(input_rows_count);
                col_to = make_nullable(col_to, true);
            } else if (is_string_type(data_type_to->get_primitive_type())) {
                // serialize to string
                return CastToStringFunction::execute_impl(context, block, arguments, result,
                                                          input_rows_count);
            } else if (data_type_to->get_primitive_type() == TYPE_JSONB) {
                // serialize to json by parsing
                return cast_from_generic_to_jsonb(context, block, arguments, result,
                                                  input_rows_count);
            } else if (!data_type_to->is_nullable() &&
                       !is_string_type(data_type_to->get_primitive_type())) {
                // other types
                col_to->assume_mutable()->insert_many_defaults(input_rows_count);
                col_to = make_nullable(col_to, true);
            } else {
                assert_cast<ColumnNullable&>(*col_to->assume_mutable())
                        .insert_many_defaults(input_rows_count);
            }
        }
        if (col_to->size() != input_rows_count) {
            return Status::InternalError("Unmatched row count {}, expected {}", col_to->size(),
                                         input_rows_count);
        }

        block.replace_by_position(result, std::move(col_to));
        return Status::OK();
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
        auto variant = ColumnVariant::create(true /*always nullable*/);
        variant->create_root(from_type, col_from->assume_mutable());
        block.replace_by_position(result, std::move(variant));
        return Status::OK();
    }
};

// create cresponding variant value to wrap from_type
WrapperType create_cast_to_variant_wrapper(const DataTypePtr& from_type,
                                           const DataTypeVariant& to_type) {
    return &CastToVariant::execute;
}

// create cresponding type convert from variant
WrapperType create_cast_from_variant_wrapper(const DataTypeVariant& from_type,
                                             const DataTypePtr& to_type) {
    return &CastFromVariant::execute;
}

} // namespace doris::vectorized::CastWrapper