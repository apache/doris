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

#include <string>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/typeid_cast.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

namespace {

std::string type_name(const DataTypePtr& input) {
    const DataTypePtr type = remove_nullable(input);
    // TYPE_NULL is represented by a UInt8 with the null-literal marker in BE plans.
    if (type->is_null_literal() || typeid_cast<const DataTypeNothing*>(type.get())) {
        return "unknown";
    }
    switch (type->get_primitive_type()) {
    case TYPE_NULL:
        return "unknown";
    case TYPE_BOOLEAN:
        return "boolean";
    case TYPE_TINYINT:
        return "tinyint";
    case TYPE_SMALLINT:
        return "smallint";
    case TYPE_INT:
        return "integer";
    case TYPE_BIGINT:
        return "bigint";
    case TYPE_LARGEINT:
        return "decimal(38,0)";
    case TYPE_FLOAT:
        return "real";
    case TYPE_DOUBLE:
        return "double";
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        const auto& string_type = assert_cast<const DataTypeString&>(*type);
        const int length = string_type.len();
        const char* name = type->get_primitive_type() == TYPE_CHAR ? "char" : "varchar";
        return length >= 0 ? fmt::format("{}({})", name, length) : name;
    }
    case TYPE_STRING:
        return "varchar";
    case TYPE_DATE:
    case TYPE_DATEV2:
        return "date";
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2:
    case TYPE_TIMESTAMP_NS:
        return "timestamp";
    case TYPE_TIMESTAMPTZ:
        return "timestamp with time zone";
    case TYPE_TIMEV2:
        return "time";
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        return fmt::format("decimal({},{})", type->get_precision(), type->get_scale());
    case TYPE_DECIMALV2: {
        const auto& decimal = assert_cast<const DataTypeDecimalV2&>(*type);
        return fmt::format("decimal({},{})", decimal.get_original_precision(),
                           decimal.get_original_scale());
    }
    case TYPE_ARRAY: {
        const auto& array = assert_cast<const DataTypeArray&>(*type);
        return fmt::format("array({})", type_name(array.get_nested_type()));
    }
    case TYPE_MAP: {
        const auto& map = assert_cast<const DataTypeMap&>(*type);
        return fmt::format("map({}, {})", type_name(map.get_key_type()),
                           type_name(map.get_value_type()));
    }
    case TYPE_STRUCT: {
        const auto& struct_type = assert_cast<const DataTypeStruct&>(*type);
        std::string result = "row(";
        for (size_t i = 0; i < struct_type.get_elements().size(); ++i) {
            if (i != 0) {
                result += ", ";
            }
            if (struct_type.get_have_explicit_names()) {
                result += fmt::format("\"{}\" ", struct_type.get_element_name(i));
            }
            result += type_name(struct_type.get_element(i));
        }
        result += ')';
        return result;
    }
    case TYPE_VARBINARY:
        return "varbinary";
    case TYPE_IPV4:
        return "ipv4";
    case TYPE_IPV6:
        return "ipv6";
    case TYPE_JSONB:
        return "json";
    case TYPE_VARIANT:
        return "variant";
    case TYPE_BITMAP:
        return "bitmap";
    case TYPE_HLL:
        return "hll";
    case TYPE_QUANTILE_STATE:
        return "quantile_state";
    case TYPE_AGG_STATE:
        return "agg_state";
    default:
        return type->get_family_name();
    }
}

class FunctionTypeOf final : public IFunction {
public:
    static constexpr auto name = "typeof";

    static FunctionPtr create() { return std::make_shared<FunctionTypeOf>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return false; }

    bool use_default_implementation_for_constants() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers&,
                        uint32_t result, size_t input_rows_count) const override {
        // Storage columns can omit declared CHAR/VARCHAR lengths. Use the analyzed argument type.
        const std::string type = type_name(context->get_arg_type(0));
        auto value = ColumnString::create();
        value->insert_data(type.data(), type.size());
        block.replace_by_position(result, ColumnConst::create(std::move(value), input_rows_count));
        return Status::OK();
    }
};

} // namespace

void register_function_type_of(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionTypeOf>();
}

} // namespace doris
