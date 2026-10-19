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

#include <span>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/string_ref.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/function_variant_element_v2.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

class FunctionVariantElement : public IFunction {
public:
    static constexpr auto name = "element_at";
    static FunctionPtr create() { return std::make_shared<FunctionVariantElement>(); }

    // Get function name.
    String get_name() const override { return name; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeVariant>(), std::make_shared<DataTypeString>()};
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK_EQ(arguments[0]->get_primitive_type(), TYPE_VARIANT)
                << "First argument for function: " << name
                << " should be DataTypeVariant but it has type " << arguments[0]->get_name() << ".";
        const PrimitiveType index_type = remove_nullable(arguments[1])->get_primitive_type();
        DCHECK(is_string_type(index_type) || is_int_or_bool(index_type))
                << "Second argument for function: " << name
                << " should be String or Integer but it has type " << arguments[1]->get_name()
                << ".";
        auto arg_variant = remove_nullable(arguments[0]);
        return make_nullable(std::move(arg_variant));
    }

    // Keep physical-column dispatch in one entry point so nullable handling stays shared.
    // NOLINTNEXTLINE(readability-function-size)
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr materialized =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const IColumn* physical = materialized.get();
        std::span<const uint8_t> outer_nulls;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
            outer_nulls = nullable->get_null_map_data();
            physical = &nullable->get_nested_column();
        }
        if (const auto* variant_v2 = check_and_get_column<ColumnVariantV2>(physical)) {
            if (block.empty()) {
                block.replace_by_position(result, ColumnNullable::create(ColumnVariantV2::create(),
                                                                         ColumnUInt8::create()));
                return Status::OK();
            }

            auto replace_with_all_null_result = [&]() {
                auto null_values = ColumnVariantV2::create();
                null_values->insert_many_defaults(variant_v2->size());
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(null_values),
                                                       ColumnUInt8::create(variant_v2->size(), 1)));
            };
            const auto& index_argument = block.get_by_position(arguments[1]);
            const ColumnPtr materialized_index =
                    index_argument.column->convert_to_full_column_if_const();
            const IColumn* index_column = materialized_index.get();
            if (index_column->is_null_at(0)) {
                replace_with_all_null_result();
                return Status::OK();
            }
            if (const auto* nullable = check_and_get_column<ColumnNullable>(*index_column)) {
                index_column = &nullable->get_nested_column();
            }

            std::optional<VariantElementV2PathSegment> segment;
            const PrimitiveType index_type =
                    remove_nullable(index_argument.type)->get_primitive_type();
            if (is_string_type(index_type)) {
                segment = VariantElementV2PathSegment::object_key(index_column->get_data_at(0));
            } else if (is_int_or_bool(index_type)) {
                const int64_t sql_index = index_column->get_int(0);
                if (sql_index == 0) {
                    replace_with_all_null_result();
                    return Status::OK();
                }
                segment = VariantElementV2PathSegment::array_index(sql_index > 0 ? sql_index - 1
                                                                                 : sql_index);
            } else {
                return Status::RuntimeError("unsupported index type {} for function {}",
                                            index_argument.type->get_name(), get_name());
            }
            std::unique_ptr<ResolvedVariantElementV2Path> path;
            RETURN_IF_ERROR(resolve_variant_element_v2_path(std::span(&*segment, 1), &path));
            ColumnPtr result_column;
            RETURN_IF_ERROR(
                    extract_variant_element_v2(*variant_v2, *path, outer_nulls, &result_column));
            block.replace_by_position(result, std::move(result_column));
            return Status::OK();
        }

        return Status::RuntimeError("element_at requires ColumnVariantV2, got {}",
                                    physical->get_name());
    }
};

class FunctionVariantElementByInteger final : public FunctionVariantElement {
public:
    static constexpr auto name = FunctionVariantElement::name;
    static FunctionPtr create() { return std::make_shared<FunctionVariantElementByInteger>(); }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeVariant>(), std::make_shared<DataTypeInt64>()};
    }
};

void register_function_variant_element(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionVariantElement>();
    factory.register_function<FunctionVariantElementByInteger>();
}

} // namespace doris
