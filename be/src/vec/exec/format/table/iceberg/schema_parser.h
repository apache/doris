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

#include <rapidjson/document.h>

#include <memory>
#include <unordered_set>

namespace doris {
namespace iceberg {

class Type;
class StructType;
class ListType;
class MapType;
class NestedField;
class Schema;

class SchemaParser {
public:
    static const char* SCHEMA_ID;
    static const char* IDENTIFIER_FIELD_IDS;
    static const char* TYPE;
    static const char* STRUCT;
    static const char* LIST;
    static const char* MAP;
    static const char* FIELDS;
    static const char* ELEMENT;
    static const char* KEY;
    static const char* VALUE;
    static const char* DOC;
    static const char* NAME;
    static const char* ID;
    static const char* ELEMENT_ID;
    static const char* KEY_ID;
    static const char* VALUE_ID;
    static const char* REQUIRED;
    static const char* ELEMENT_REQUIRED;
    static const char* VALUE_REQUIRED;

    static std::unique_ptr<Schema> from_json(const std::string& json);

private:
    static std::unique_ptr<Type> _type_from_json(const rapidjson::Value& value);
    static std::unique_ptr<StructType> _struct_from_json(const rapidjson::Value& value);
    static std::unique_ptr<ListType> _list_from_json(const rapidjson::Value& value);
    static std::unique_ptr<MapType> _map_from_json(const rapidjson::Value& value);
    static std::unordered_set<int> _get_integer_set(const char* key, const rapidjson::Value& value);
};

} // namespace iceberg
} // namespace doris
