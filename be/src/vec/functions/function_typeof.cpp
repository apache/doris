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

#include "vec/functions/function_typeof.h"

#include <string>

#include "runtime/primitive_type.h"
#include "util/string_util.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_time.h"
#include "vec/data_types/data_type_timestamptz.h"
#include "vec/data_types/data_type_varbinary.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

namespace {

std::string format_decimal(const std::string& name, UInt32 precision, UInt32 scale) {
    return name + "(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
}

std::string format_type_for_typeof(const IDataType& type) {
    if (const auto* nullable = typeid_cast<const DataTypeNullable*>(&type)) {
        const auto& nested = nullable->get_nested_type();
        if (typeid_cast<const DataTypeNothing*>(nested.get()) != nullptr) {
            return "null";
        }
        return format_type_for_typeof(*nested);
    }

    if (typeid_cast<const DataTypeNothing*>(&type) != nullptr) {
        return "null";
    }

    switch (type.get_primitive_type()) {
    case TYPE_BOOLEAN:
        return "boolean";
    case TYPE_TINYINT:
        return "tinyint";
    case TYPE_SMALLINT:
        return "smallint";
    case TYPE_INT:
        return "int";
    case TYPE_BIGINT:
        return "bigint";
    case TYPE_LARGEINT:
        return "largeint";
    case TYPE_FLOAT:
        return "float";
    case TYPE_DOUBLE:
        return "double";
    case TYPE_DATE:
        return "date";
    case TYPE_DATETIME:
        return "datetime";
    case TYPE_DATEV2:
        return "datev2";
    case TYPE_DATETIMEV2: {
        const auto& datetimev2 = assert_cast<const DataTypeDateTimeV2&>(type);
        return "datetimev2(" + std::to_string(datetimev2.get_scale()) + ")";
    }
    case TYPE_TIMEV2: {
        const auto& timev2 = assert_cast<const DataTypeTimeV2&>(type);
        return "timev2(" + std::to_string(timev2.get_scale()) + ")";
    }
    case TYPE_TIMESTAMPTZ: {
        const auto& timestamptz = assert_cast<const DataTypeTimeStampTz&>(type);
        return "timestamptz(" + std::to_string(timestamptz.get_scale()) + ")";
    }
    case TYPE_DECIMALV2: {
        const auto& decimalv2 = assert_cast<const DataTypeDecimalV2&>(type);
        return format_decimal("decimalv2", decimalv2.get_original_precision(),
                              decimalv2.get_original_scale());
    }
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        return format_decimal("decimal", type.get_precision(), type.get_scale());
    case TYPE_STRING:
        return "varchar";
    case TYPE_CHAR: {
        const auto& string_type = assert_cast<const DataTypeString&>(type);
        return string_type.len() > 0 ? "char(" + std::to_string(string_type.len()) + ")" : "char";
    }
    case TYPE_VARCHAR: {
        const auto& string_type = assert_cast<const DataTypeString&>(type);
        return string_type.len() > 0 ? "varchar(" + std::to_string(string_type.len()) + ")"
                                     : "varchar";
    }
    case TYPE_VARBINARY:
        return "varbinary";
    case TYPE_ARRAY: {
        const auto& array = assert_cast<const DataTypeArray&>(type);
        return "array(" + format_type_for_typeof(*array.get_nested_type()) + ")";
    }
    case TYPE_MAP: {
        const auto& map = assert_cast<const DataTypeMap&>(type);
        return "map(" + format_type_for_typeof(*map.get_key_type()) + "," +
               format_type_for_typeof(*map.get_value_type()) + ")";
    }
    case TYPE_STRUCT: {
        const auto& struct_type = assert_cast<const DataTypeStruct&>(type);
        std::string result = "struct<";
        for (size_t i = 0; i < struct_type.get_elements().size(); ++i) {
            if (i != 0) {
                result += ",";
            }
            result += struct_type.get_element_name(i);
            result += ":";
            result += format_type_for_typeof(*struct_type.get_element(i));
        }
        result += ">";
        return result;
    }
    case INVALID_TYPE:
        return "null";
    default:
        return to_lower(type_to_string(type.get_primitive_type()));
    }
}

} // namespace

DataTypePtr FunctionTypeOf::get_return_type_impl(const DataTypes& arguments) const {
    return std::make_shared<DataTypeString>();
}

Status FunctionTypeOf::execute_impl(FunctionContext* context, Block& block,
                                    const ColumnNumbers& arguments, uint32_t result,
                                    size_t input_rows_count) const {
    const auto& argument_type = *block.get_by_position(arguments[0]).type;
    block.get_by_position(result).column = std::make_shared<DataTypeString>()->create_column_const(
            input_rows_count,
            Field::create_field<TYPE_STRING>(format_type_for_typeof(argument_type)));
    return Status::OK();
}

void register_function_typeof(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionTypeOf>();
}

} // namespace doris::vectorized
