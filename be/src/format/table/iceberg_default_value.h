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

#include <gen_cpp/ExternalTableSchema_types.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cstddef>
#include <deque>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "util/string_util.h"
#include "util/url_coding.h"

namespace doris::iceberg {

namespace detail {

inline const schema::external::TField* get_field_ptr(const schema::external::TFieldPtr& field_ptr) {
    if (!field_ptr.__isset.field_ptr || field_ptr.field_ptr == nullptr) {
        return nullptr;
    }
    return field_ptr.field_ptr.get();
}

inline const schema::external::TField* find_struct_child(
        const schema::external::TStructField& struct_field, const std::string& name) {
    if (!struct_field.__isset.fields) {
        return nullptr;
    }
    for (const auto& child_ptr : struct_field.fields) {
        const auto* child = get_field_ptr(child_ptr);
        if (child != nullptr && child->__isset.name && iequal(child->name, name)) {
            return child;
        }
    }
    for (const auto& child_ptr : struct_field.fields) {
        const auto* child = get_field_ptr(child_ptr);
        if (child == nullptr || !child->__isset.name_mapping) {
            continue;
        }
        for (const auto& alias : child->name_mapping) {
            if (iequal(alias, name)) {
                return child;
            }
        }
    }
    return nullptr;
}

inline int hex_value(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

inline Status decode_hex(std::string_view encoded, std::string* decoded) {
    DORIS_CHECK(decoded != nullptr);
    if ((encoded.size() & 1U) != 0) {
        return Status::InvalidArgument("Invalid odd-length Iceberg binary default");
    }
    decoded->resize(encoded.size() / 2);
    for (size_t index = 0; index < encoded.size(); index += 2) {
        const int high = hex_value(encoded[index]);
        const int low = hex_value(encoded[index + 1]);
        if (high < 0 || low < 0) {
            return Status::InvalidArgument("Invalid hexadecimal Iceberg binary default");
        }
        (*decoded)[index / 2] = static_cast<char>((high << 4) | low);
    }
    return Status::OK();
}

inline Status decode_json_binary(std::string_view encoded, std::string* decoded) {
    DORIS_CHECK(decoded != nullptr);
    const bool is_uuid = encoded.size() == 36 && encoded[8] == '-' && encoded[13] == '-' &&
                         encoded[18] == '-' && encoded[23] == '-';
    if (is_uuid) {
        std::string uuid_hex;
        uuid_hex.reserve(32);
        for (size_t index = 0; index < encoded.size(); ++index) {
            if (index != 8 && index != 13 && index != 18 && index != 23) {
                uuid_hex.push_back(encoded[index]);
            }
        }
        return decode_hex(uuid_hex, decoded);
    }
    return decode_hex(encoded, decoded);
}

inline std::string json_scalar_text(const rapidjson::Value& value) {
    if (value.IsString()) {
        return {value.GetString(), value.GetStringLength()};
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
    return {buffer.GetString(), buffer.GetSize()};
}

inline void normalize_timestamp_for_doris(PrimitiveType primitive_type, std::string* value) {
    if (primitive_type != TYPE_DATETIME && primitive_type != TYPE_DATETIMEV2 &&
        primitive_type != TYPE_TIMESTAMPTZ) {
        return;
    }
    if (const size_t separator = value->find('T'); separator != std::string::npos) {
        (*value)[separator] = ' ';
    }
    if (primitive_type == TYPE_TIMESTAMPTZ) {
        return;
    }
    if (value->ends_with('Z')) {
        value->pop_back();
        return;
    }
    const size_t time_start = value->find(' ');
    if (time_start == std::string::npos) {
        return;
    }
    const size_t offset = value->find_first_of("+-", time_start + 1);
    if (offset != std::string::npos) {
        value->erase(offset);
    }
}

inline Status make_null_field(const schema::external::TField& field, const DataTypePtr& data_type,
                              Field* result) {
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(result != nullptr);
    if (field.__isset.is_optional && !field.is_optional) {
        return Status::InvalidArgument("Required Iceberg field '{}' has a null default",
                                       field.name);
    }
    if (!data_type->is_nullable()) {
        return Status::InternalError(
                "Optional Iceberg field '{}' has a null default, but its Doris type '{}' is not "
                "nullable",
                field.name, data_type->get_name());
    }
    *result = Field();
    return Status::OK();
}

inline Status build_initial_default_field(const schema::external::TField& field,
                                          const DataTypePtr& data_type,
                                          std::deque<std::string>* binary_storage, Field* result);

inline Status build_json_default_field(const schema::external::TField& field,
                                       const DataTypePtr& data_type,
                                       const rapidjson::Value& json_value,
                                       std::deque<std::string>* binary_storage, Field* result);

inline Status build_json_struct_default(const schema::external::TField& field,
                                        const DataTypePtr& value_type,
                                        const rapidjson::Value& json_value,
                                        std::deque<std::string>* binary_storage, Field* result) {
    if (!json_value.IsObject() || !field.__isset.nestedField ||
        !field.nestedField.__isset.struct_field || !field.nestedField.struct_field.__isset.fields) {
        return Status::InvalidArgument("Invalid Iceberg struct default for field '{}'", field.name);
    }

    const auto& struct_type = assert_cast<const DataTypeStruct&>(*value_type);
    Struct struct_value;
    struct_value.reserve(struct_type.get_elements().size());
    for (size_t index = 0; index < struct_type.get_elements().size(); ++index) {
        const auto& child_name = struct_type.get_element_name(index);
        const auto* child = find_struct_child(field.nestedField.struct_field, child_name);
        if (child == nullptr || !child->__isset.id) {
            return Status::InvalidArgument(
                    "Iceberg struct default for field '{}' is missing metadata for projected "
                    "child '{}'",
                    field.name, child_name);
        }

        const std::string child_id = std::to_string(child->id);
        const auto member = json_value.FindMember(child_id.c_str());
        Field child_value;
        if (member == json_value.MemberEnd()) {
            RETURN_IF_ERROR(build_initial_default_field(*child, struct_type.get_element(index),
                                                        binary_storage, &child_value));
        } else {
            RETURN_IF_ERROR(build_json_default_field(*child, struct_type.get_element(index),
                                                     member->value, binary_storage, &child_value));
        }
        struct_value.push_back(std::move(child_value));
    }
    *result = Field::create_field<TYPE_STRUCT>(std::move(struct_value));
    return Status::OK();
}

// The recursive item TField describes the element schema and its field-level default metadata. It
// cannot represent a particular list literal's length or per-position values, so the parent
// initial-default keeps those values in Iceberg's single-value JSON array.
inline Status build_json_array_default(const schema::external::TField& field,
                                       const DataTypePtr& value_type,
                                       const rapidjson::Value& json_value,
                                       std::deque<std::string>* binary_storage, Field* result) {
    if (!json_value.IsArray() || !field.__isset.nestedField ||
        !field.nestedField.__isset.array_field ||
        !field.nestedField.array_field.__isset.item_field) {
        return Status::InvalidArgument("Invalid Iceberg list default for field '{}'", field.name);
    }
    const auto* element = get_field_ptr(field.nestedField.array_field.item_field);
    if (element == nullptr) {
        return Status::InvalidArgument(
                "Iceberg list default for field '{}' has incomplete element metadata", field.name);
    }

    const auto& array_type = assert_cast<const DataTypeArray&>(*value_type);
    Array array_value;
    array_value.reserve(json_value.Size());
    for (const auto& json_element : json_value.GetArray()) {
        Field element_value;
        RETURN_IF_ERROR(build_json_default_field(*element, array_type.get_nested_type(),
                                                 json_element, binary_storage, &element_value));
        array_value.push_back(std::move(element_value));
    }
    *result = Field::create_field<TYPE_ARRAY>(std::move(array_value));
    return Status::OK();
}

// The recursive key/value TFields describe entry schemas and field-level default metadata. They
// cannot represent the number, order, or concrete values of map entries, so the parent
// initial-default keeps the entries in Iceberg's single-value JSON key/value arrays.
inline Status build_json_map_default(const schema::external::TField& field,
                                     const DataTypePtr& value_type,
                                     const rapidjson::Value& json_value,
                                     std::deque<std::string>* binary_storage, Field* result) {
    if (!json_value.IsObject() || !json_value.HasMember("keys") || !json_value["keys"].IsArray() ||
        !json_value.HasMember("values") || !json_value["values"].IsArray() ||
        !field.__isset.nestedField || !field.nestedField.__isset.map_field ||
        !field.nestedField.map_field.__isset.key_field ||
        !field.nestedField.map_field.__isset.value_field) {
        return Status::InvalidArgument("Invalid Iceberg map default for field '{}'", field.name);
    }
    const auto& keys = json_value["keys"];
    const auto& values = json_value["values"];
    if (keys.Size() != values.Size()) {
        return Status::InvalidArgument(
                "Iceberg map default for field '{}' has {} keys but {} values", field.name,
                keys.Size(), values.Size());
    }

    const auto* key = get_field_ptr(field.nestedField.map_field.key_field);
    const auto* value = get_field_ptr(field.nestedField.map_field.value_field);
    if (key == nullptr || value == nullptr) {
        return Status::InvalidArgument(
                "Iceberg map default for field '{}' has incomplete key/value metadata", field.name);
    }

    const auto& map_type = assert_cast<const DataTypeMap&>(*value_type);
    Array key_fields;
    Array value_fields;
    key_fields.reserve(keys.Size());
    value_fields.reserve(values.Size());
    for (rapidjson::SizeType index = 0; index < keys.Size(); ++index) {
        Field key_value;
        Field mapped_value;
        RETURN_IF_ERROR(build_json_default_field(*key, map_type.get_key_type(), keys[index],
                                                 binary_storage, &key_value));
        RETURN_IF_ERROR(build_json_default_field(*value, map_type.get_value_type(), values[index],
                                                 binary_storage, &mapped_value));
        key_fields.push_back(std::move(key_value));
        value_fields.push_back(std::move(mapped_value));
    }
    Map map_value;
    map_value.push_back(Field::create_field<TYPE_ARRAY>(std::move(key_fields)));
    map_value.push_back(Field::create_field<TYPE_ARRAY>(std::move(value_fields)));
    *result = Field::create_field<TYPE_MAP>(std::move(map_value));
    return Status::OK();
}

inline Status build_json_scalar_default(const schema::external::TField& field,
                                        const DataTypePtr& value_type,
                                        const rapidjson::Value& json_value,
                                        std::deque<std::string>* binary_storage, Field* result) {
    const auto primitive_type = value_type->get_primitive_type();
    std::string serialized_value = json_scalar_text(json_value);
    const bool binary_like = (field.__isset.initial_default_value_is_base64 &&
                              field.initial_default_value_is_base64) ||
                             primitive_type == TYPE_VARBINARY;
    if (binary_like) {
        if (!json_value.IsString()) {
            return Status::InvalidArgument(
                    "Iceberg binary default for field '{}' is not a JSON string", field.name);
        }
        binary_storage->emplace_back();
        RETURN_IF_ERROR(decode_json_binary(serialized_value, &binary_storage->back()));
        if (primitive_type == TYPE_VARBINARY) {
            *result = Field::create_field<TYPE_VARBINARY>(StringView(binary_storage->back()));
        } else if (is_string_type(primitive_type)) {
            *result = Field::create_field<TYPE_STRING>(binary_storage->back());
        } else {
            return Status::InvalidArgument(
                    "Iceberg binary default for field '{}' has incompatible Doris type '{}'",
                    field.name, value_type->get_name());
        }
        return Status::OK();
    }

    if (is_string_type(primitive_type)) {
        if (!json_value.IsString()) {
            return Status::InvalidArgument("Iceberg string default for field '{}' is not a string",
                                           field.name);
        }
        *result = Field::create_field<TYPE_STRING>(std::move(serialized_value));
        return Status::OK();
    }
    normalize_timestamp_for_doris(primitive_type, &serialized_value);
    RETURN_IF_ERROR(value_type->get_serde()->from_fe_string(serialized_value, *result));
    return Status::OK();
}

inline Status build_json_default_field(const schema::external::TField& field,
                                       const DataTypePtr& data_type,
                                       const rapidjson::Value& json_value,
                                       std::deque<std::string>* binary_storage, Field* result) {
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(binary_storage != nullptr);
    DORIS_CHECK(result != nullptr);
    if (json_value.IsNull()) {
        return make_null_field(field, data_type, result);
    }

    const auto value_type = remove_nullable(data_type);
    switch (value_type->get_primitive_type()) {
    case TYPE_STRUCT:
        return build_json_struct_default(field, value_type, json_value, binary_storage, result);
    case TYPE_ARRAY:
        return build_json_array_default(field, value_type, json_value, binary_storage, result);
    case TYPE_MAP:
        return build_json_map_default(field, value_type, json_value, binary_storage, result);
    default:
        return build_json_scalar_default(field, value_type, json_value, binary_storage, result);
    }
}

inline Status build_initial_default_field(const schema::external::TField& field,
                                          const DataTypePtr& data_type,
                                          std::deque<std::string>* binary_storage, Field* result) {
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(binary_storage != nullptr);
    DORIS_CHECK(result != nullptr);
    if (!field.__isset.initial_default_value) {
        if (field.__isset.is_optional && !field.is_optional) {
            return Status::InvalidArgument(
                    "Required Iceberg field '{}' is missing from the data file and has no initial "
                    "default",
                    field.name);
        }
        return make_null_field(field, data_type, result);
    }

    const auto value_type = remove_nullable(data_type);
    const auto primitive_type = value_type->get_primitive_type();
    if (is_complex_type(primitive_type)) {
        rapidjson::Document document;
        document.Parse(field.initial_default_value.data(), field.initial_default_value.size());
        if (document.HasParseError()) {
            return Status::InvalidArgument("Invalid Iceberg JSON initial default for field '{}'",
                                           field.name);
        }
        if (primitive_type == TYPE_STRUCT &&
            (!document.IsObject() || document.MemberCount() != 0)) {
            return Status::InvalidArgument(
                    "Iceberg struct field '{}' has a non-empty initial default", field.name);
        }
        return build_json_default_field(field, data_type, document, binary_storage, result);
    }

    const bool default_is_base64 = (field.__isset.initial_default_value_is_base64 &&
                                    field.initial_default_value_is_base64) ||
                                   primitive_type == TYPE_VARBINARY;
    if (default_is_base64) {
        binary_storage->emplace_back();
        if (!base64_decode(field.initial_default_value, &binary_storage->back())) {
            return Status::InvalidArgument("Invalid Base64 Iceberg initial default for field '{}'",
                                           field.name);
        }
        if (primitive_type == TYPE_VARBINARY) {
            *result = Field::create_field<TYPE_VARBINARY>(StringView(binary_storage->back()));
        } else if (is_string_type(primitive_type)) {
            *result = Field::create_field<TYPE_STRING>(binary_storage->back());
        } else {
            return Status::InvalidArgument(
                    "Iceberg field '{}' marks its initial default as Base64, but Doris type '{}' "
                    "cannot contain binary data",
                    field.name, value_type->get_name());
        }
        return Status::OK();
    }

    RETURN_IF_ERROR(value_type->get_serde()->from_fe_string(field.initial_default_value, *result));
    return Status::OK();
}

} // namespace detail

// Builds an owned one-row column for an Iceberg field that is absent from an old data file.
// Complex values follow Iceberg's JSON single-value encoding. Struct members omitted from the
// encoded value are recursively populated from the child field's own initial default.
inline Status create_initial_default_column(const schema::external::TField& field,
                                            const DataTypePtr& data_type, ColumnPtr* result) {
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(result != nullptr);

    auto column = data_type->create_column();
    std::deque<std::string> binary_storage;
    Field value;
    RETURN_IF_ERROR(detail::build_initial_default_field(field, data_type, &binary_storage, &value));
    // The column copies every String/StringView leaf before binary_storage is destroyed.
    column->insert(value);

    *result = std::move(column);
    return Status::OK();
}

inline ColumnPtr repeat_initial_default_column(const ColumnPtr& default_column, size_t rows) {
    DORIS_CHECK(default_column);
    DORIS_CHECK_EQ(default_column->size(), 1);

    auto repeated_column = default_column->clone_empty();
    repeated_column->insert_many_from(*default_column, 0, rows);
    return repeated_column;
}

inline Status append_initial_default(
        const schema::external::TField& field, const DataTypePtr& data_type, size_t rows,
        std::unordered_map<int32_t, std::pair<DataTypePtr, ColumnPtr>>* prepared_values,
        ColumnPtr* destination) {
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(prepared_values != nullptr);
    DORIS_CHECK(destination != nullptr);
    DORIS_CHECK(field.__isset.id);

    auto prepared_value = prepared_values->find(field.id);
    if (prepared_value == prepared_values->end()) {
        ColumnPtr default_column;
        RETURN_IF_ERROR(create_initial_default_column(field, data_type, &default_column));
        prepared_value =
                prepared_values
                        ->emplace(field.id, std::make_pair(data_type, std::move(default_column)))
                        .first;
    } else {
        // One Iceberg field ID resolves to one query type. Hold the first DataTypePtr so equivalent
        // complex types reconstructed for later Blocks reuse the same prepared value.
        DORIS_CHECK(prepared_value->second.first->equals(*data_type));
    }

    auto mutable_destination = IColumn::mutate(std::move(*destination));
    mutable_destination->insert_many_from(*prepared_value->second.second, 0, rows);
    *destination = std::move(mutable_destination);
    return Status::OK();
}

} // namespace doris::iceberg
