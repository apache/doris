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
#include <map>
#include <span>
#include <string_view>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "exprs/function/simple_function_factory.h"
#include "util/string_util.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

namespace {

std::string_view encoded_variant_type_word(VariantRef value) {
    switch (value.basic_type()) {
    case VariantBasicType::SHORT_STRING:
        return "string";
    case VariantBasicType::OBJECT:
        return "object";
    case VariantBasicType::ARRAY:
        return "array";
    case VariantBasicType::PRIMITIVE:
        break;
    }

    switch (value.primitive_id()) {
    case VariantPrimitiveId::NULL_VALUE:
        return "null";
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        return "bool";
    case VariantPrimitiveId::INT8:
        return "tinyint";
    case VariantPrimitiveId::INT16:
        return "smallint";
    case VariantPrimitiveId::INT32:
        return "int";
    case VariantPrimitiveId::INT64:
        return "bigint";
    case VariantPrimitiveId::DOUBLE:
        return "double";
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16:
        static_cast<void>(value.get_decimal());
        return "decimal";
    case VariantPrimitiveId::DATE:
        return "date";
    case VariantPrimitiveId::TIMESTAMP_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
        return "timestamp";
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        return "timestamp_ntz";
    case VariantPrimitiveId::FLOAT:
        return "float";
    case VariantPrimitiveId::BINARY:
        return "binary";
    case VariantPrimitiveId::STRING:
        return "string";
    case VariantPrimitiveId::TIME_NTZ_MICROS:
        return "time";
    case VariantPrimitiveId::UUID:
        return "uuid";
    }
    DORIS_CHECK(false) << "validated Variant primitive id has no type word";
    return {};
}

ColumnPtr execute_variant_type_v2(const ColumnVariantV2& source,
                                  std::span<const NullMap::value_type> outer_nulls) {
    auto values = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    values->reserve(source.size());
    nulls->reserve(source.size());

    visit_variant_v2_values(
            source, 0, source.size(), outer_nulls,
            [&](size_t) {
                values->insert_default();
                nulls->insert_value(1);
            },
            [&](size_t, VariantRef value) {
                const std::string_view word = encoded_variant_type_word(value);
                values->insert_data(word.data(), word.size());
                nulls->insert_value(0);
            });
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

Status variant_v2_exception_status(const Exception& exception) {
    if (exception.code() == ErrorCode::CORRUPTION) {
        return Status::InvalidArgument("Invalid Variant V2 input: {}", exception.message());
    }
    return exception.to_status();
}

} // namespace

// get data type of variant column
class FunctionVariantType : public IFunction {
public:
    static constexpr auto name = "variant_type";
    static FunctionPtr create() { return std::make_shared<FunctionVariantType>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr materialized =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const IColumn* physical = materialized.get();
        std::span<const NullMap::value_type> outer_nulls;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
            outer_nulls = nullable->get_null_map_data();
            physical = &nullable->get_nested_column();
        }
        const auto* variant_v2 = check_and_get_column<ColumnVariantV2>(physical);
        if (variant_v2 == nullptr) {
            return Status::RuntimeError("variant_type requires ColumnVariantV2, got {}",
                                        physical->get_name());
        }
        try {
            block.replace_by_position(result, execute_variant_type_v2(*variant_v2, outer_nulls));
            return Status::OK();
        } catch (const Exception& exception) {
            return variant_v2_exception_status(exception);
        }
    }
};

void register_function_variant_type(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionVariantType>();
}

} // namespace doris
