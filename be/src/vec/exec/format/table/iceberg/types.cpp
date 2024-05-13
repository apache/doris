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

#include "types.h"

#include <optional>

namespace doris {
namespace iceberg {

std::unique_ptr<MapType> MapType::of_optional(int key_id, int value_id,
                                              std::unique_ptr<Type> key_type,
                                              std::unique_ptr<Type> value_type) {
    auto key_field =
            std::make_unique<NestedField>(false, key_id, "key", std::move(key_type), std::nullopt);
    auto value_field = std::make_unique<NestedField>(true, value_id, "value", std::move(value_type),
                                                     std::nullopt);
    return std::make_unique<MapType>(std::move(key_field), std::move(value_field));
}
std::unique_ptr<MapType> MapType::of_required(int key_id, int value_id,
                                              std::unique_ptr<Type> key_type,
                                              std::unique_ptr<Type> value_type) {
    auto key_field =
            std::make_unique<NestedField>(false, key_id, "key", std::move(key_type), std::nullopt);
    auto value_field = std::make_unique<NestedField>(false, value_id, "value",
                                                     std::move(value_type), std::nullopt);
    return std::make_unique<MapType>(std::move(key_field), std::move(value_field));
}

Type* MapType::key_type() const {
    return _key_field->field_type();
}
Type* MapType::value_type() const {
    return _value_field->field_type();
}
int MapType::key_id() const {
    return _key_field->field_id();
}
int MapType::value_id() const {
    return _value_field->field_id();
}
bool MapType::is_value_required() const {
    return !_value_field->is_optional();
}
bool MapType::is_value_optional() const {
    return _value_field->is_optional();
}

std::string MapType::to_string() const {
    return "map<" + _key_field->field_type()->to_string() + ", " +
           _value_field->field_type()->to_string() + ">";
}

std::unique_ptr<ListType> ListType::of_optional(int element_id,
                                                std::unique_ptr<Type> element_type) {
    NestedField field(true, element_id, "element", std::move(element_type), std::nullopt);
    return std::make_unique<ListType>(std::move(field));
}
std::unique_ptr<ListType> ListType::of_required(int element_id,
                                                std::unique_ptr<Type> element_type) {
    NestedField field(false, element_id, "element", std::move(element_type), std::nullopt);
    return std::make_unique<ListType>(std::move(field));
}

std::unique_ptr<PrimitiveType> Types::from_primitive_string(const std::string& type_string) {
    std::string lower_type_string;
    std::transform(type_string.begin(), type_string.end(), std::back_inserter(lower_type_string),
                   [](unsigned char c) { return std::tolower(c); });

    if (lower_type_string == "boolean") {
        return std::make_unique<BooleanType>();
    } else if (lower_type_string == "int") {
        return std::make_unique<IntegerType>();
    } else if (lower_type_string == "long") {
        return std::make_unique<LongType>();
    } else if (lower_type_string == "float") {
        return std::make_unique<FloatType>();
    } else if (lower_type_string == "double") {
        return std::make_unique<DoubleType>();
    } else if (lower_type_string == "date") {
        return std::make_unique<DateType>();
    } else if (lower_type_string == "time") {
        return std::make_unique<TimeType>();
    } else if (lower_type_string == "timestamptz") {
        return std::make_unique<TimestampType>(true);
    } else if (lower_type_string == "timestamp") {
        return std::make_unique<TimestampType>(false);
    } else if (lower_type_string == "string") {
        return std::make_unique<StringType>();
    } else if (lower_type_string == "uuid") {
        return std::make_unique<UUIDType>();
    } else if (lower_type_string == "binary") {
        return std::make_unique<BinaryType>();
    } else {
        std::regex fixed("fixed\\[\\s*(\\d+)\\s*\\]");
        std::regex decimal("decimal\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\]");

        std::smatch match;
        if (std::regex_match(lower_type_string, match, fixed)) {
            int length = std::stoi(match[1]);
            return std::make_unique<FixedType>(length);
        }

        if (std::regex_match(lower_type_string, match, decimal)) {
            int precision = std::stoi(match[1]);
            int scale = std::stoi(match[2]);
            return std::make_unique<DecimalType>(precision, scale);
        }

        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Cannot parse type string to primitive: {}.", type_string);
    }
}

} // namespace iceberg
} // namespace doris
