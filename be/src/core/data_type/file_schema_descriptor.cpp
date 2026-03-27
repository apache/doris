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

#include "core/data_type/file_schema_descriptor.h"

#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"

namespace doris {

namespace {

constexpr auto OBJECT_URI = "object_uri";             // s3://bucket/path/to/file.txt
constexpr auto FILE_NAME = "file_name";               // file.txt
constexpr auto FILE_EXTENSION = "file_extension";     // .txt
constexpr auto SIZE = "size";                         // 12345
constexpr auto ETAG = "etag";                         // abc123
constexpr auto LAST_MODIFIED_AT = "last_modified_at"; // 2022-01-01T00:00:00Z

} // namespace

const FileSchemaDescriptor& FileSchemaDescriptor::instance() {
    static const FileSchemaDescriptor descriptor;
    return descriptor;
}

FileSchemaDescriptor::FileSchemaDescriptor() {
    auto add_field = [this](const char* name, DataTypePtr type, bool nullable) {
        _fields.emplace_back(FileFieldDesc {
                .name = name,
                .type = std::move(type),
                .nullable = nullable,
        });
    };

    add_field(OBJECT_URI, std::make_shared<DataTypeString>(4096, TYPE_VARCHAR), false);
    add_field(FILE_NAME, std::make_shared<DataTypeString>(512, TYPE_VARCHAR), false);
    add_field(FILE_EXTENSION, std::make_shared<DataTypeString>(64, TYPE_VARCHAR), false);
    add_field(SIZE, std::make_shared<DataTypeInt64>(), false);
    add_field(ETAG, make_nullable(std::make_shared<DataTypeString>(256, TYPE_VARCHAR)), true);
    add_field(LAST_MODIFIED_AT, std::make_shared<DataTypeDateTimeV2>(3), false);

    DataTypes types;
    Strings names;
    types.reserve(_fields.size());
    names.reserve(_fields.size());
    for (const auto& field : _fields) {
        types.push_back(field.type);
        names.emplace_back(field.name);
    }
    _physical_type = std::make_shared<DataTypeStruct>(types, names);
}

std::optional<size_t> FileSchemaDescriptor::try_get_position(std::string_view name) const {
    for (size_t i = 0; i < _fields.size(); ++i) {
        if (name == _fields[i].name) {
            return i;
        }
    }
    return std::nullopt;
}

} // namespace doris
