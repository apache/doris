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

#include "vec/exec/format/table/iceberg/schema_parser.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include <optional>
#include <unordered_set>

#include "vec/exec/format/table/iceberg/schema.h"
#include "vec/exec/format/table/iceberg/types.h"

namespace doris {
namespace iceberg {

const char* SchemaParser::SCHEMA_ID = "schema-id";
const char* SchemaParser::IDENTIFIER_FIELD_IDS = "identifier-field-ids";
const char* SchemaParser::TYPE = "type";
const char* SchemaParser::STRUCT = "struct";
const char* SchemaParser::LIST = "list";
const char* SchemaParser::MAP = "map";
const char* SchemaParser::FIELDS = "fields";
const char* SchemaParser::ELEMENT = "element";
const char* SchemaParser::KEY = "key";
const char* SchemaParser::VALUE = "value";
const char* SchemaParser::DOC = "doc";
const char* SchemaParser::NAME = "name";
const char* SchemaParser::ID = "id";
const char* SchemaParser::ELEMENT_ID = "element-id";
const char* SchemaParser::KEY_ID = "key-id";
const char* SchemaParser::VALUE_ID = "value-id";
const char* SchemaParser::REQUIRED = "required";
const char* SchemaParser::ELEMENT_REQUIRED = "element-required";
const char* SchemaParser::VALUE_REQUIRED = "value-required";

std::unique_ptr<Type> SchemaParser::_type_from_json(const rapidjson::Value& value) {
    if (value.IsString()) {
        return Types::from_primitive_string(value.GetString());
    } else if (value.IsObject()) {
        if (value.HasMember(TYPE)) {
            std::string type = value[TYPE].GetString();
            if (type == STRUCT) {
                return _struct_from_json(value);
            } else if (type == LIST) {
                return _list_from_json(value);
            } else if (type == MAP) {
                return _map_from_json(value);
            }
        }
    }
    throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Cannot parse type from json.");
}

std::unique_ptr<StructType> SchemaParser::_struct_from_json(const rapidjson::Value& value) {
    if (!value.HasMember("fields") || !value["fields"].IsArray()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Cannot parse struct fields from non-array.");
    }

    const rapidjson::Value& field_array = value["fields"];
    std::vector<NestedField> fields;
    fields.reserve(field_array.Size());

    for (size_t i = 0; i < field_array.Size(); ++i) {
        const rapidjson::Value& field = field_array[i];
        if (!field.IsObject()) {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "Cannot parse struct field from non-object.");
        }

        int id = field[ID].GetInt();
        std::string name = field[NAME].GetString();
        std::unique_ptr<Type> type = _type_from_json(field[TYPE]);

        std::optional<std::string> doc = std::nullopt;
        if (field.HasMember(DOC)) {
            doc = field[DOC].GetString();
        }

        bool is_required = field[REQUIRED].GetBool();

        fields.emplace_back(!is_required, id, name, std::move(type), doc);
    }

    return std::make_unique<StructType>(std::move(fields));
}

std::unique_ptr<ListType> SchemaParser::_list_from_json(const rapidjson::Value& value) {
    int element_id = value[ELEMENT_ID].GetInt();
    std::unique_ptr<Type> element_type = _type_from_json(value[ELEMENT]);
    bool is_required = value[ELEMENT_REQUIRED].GetBool();

    if (is_required) {
        return ListType::of_required(element_id, std::move(element_type));
    } else {
        return ListType::of_optional(element_id, std::move(element_type));
    }
}

std::unique_ptr<MapType> SchemaParser::_map_from_json(const rapidjson::Value& value) {
    int key_id = value[KEY_ID].GetInt();
    std::unique_ptr<Type> key_type = _type_from_json(value[KEY]);

    int value_id = value[VALUE_ID].GetInt();
    std::unique_ptr<Type> value_type = _type_from_json(value[VALUE]);

    bool is_required = value[VALUE_REQUIRED].GetBool();

    if (is_required) {
        return MapType::of_required(key_id, value_id, std::move(key_type), std::move(value_type));
    } else {
        return MapType::of_optional(key_id, value_id, std::move(key_type), std::move(value_type));
    }
}

std::unique_ptr<Schema> SchemaParser::from_json(const std::string& json) {
    rapidjson::Document doc;
    doc.Parse(json.c_str());

    if (doc.HasParseError()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Failed to parse JSON: {}.",
                               std::string(GetParseError_En(doc.GetParseError())));
    }
    std::unique_ptr<Type> type = _type_from_json(doc);
    return std::make_unique<Schema>(type->as_nested_type()->as_struct_type()->move_fields());
}

std::unordered_set<int> SchemaParser::_get_integer_set(const char* key,
                                                       const rapidjson::Value& value) {
    std::unordered_set<int> integer_set;

    if (value.HasMember(key) && value[key].IsArray()) {
        const rapidjson::Value& arr = value[key];
        for (size_t i = 0; i < arr.Size(); i++) {
            if (arr[i].IsInt()) {
                integer_set.insert(arr[i].GetInt());
            } else {
                throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                       "Unexpected non-integer element in the array.");
            }
        }
    }
    return integer_set;
}

} // namespace iceberg
} // namespace doris