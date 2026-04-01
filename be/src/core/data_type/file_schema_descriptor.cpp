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

const FileSchemaDescriptor& FileSchemaDescriptor::instance() {
    static const FileSchemaDescriptor descriptor;
    return descriptor;
}

FileSchemaDescriptor::FileSchemaDescriptor() {
    auto add_field = [this](const char* name, DataTypePtr type) {
        _fields.emplace_back(FileFieldDesc {
                .name = name,
                .type = std::move(type),
        });
    };

    add_field(FILE_FIELD_OBJECT_URI.data(), std::make_shared<DataTypeString>(4096, TYPE_VARCHAR));
    add_field(FILE_FIELD_FILE_NAME.data(), std::make_shared<DataTypeString>(512, TYPE_VARCHAR));
    add_field(FILE_FIELD_FILE_EXTENSION.data(), std::make_shared<DataTypeString>(64, TYPE_VARCHAR));
    add_field(FILE_FIELD_SIZE.data(), std::make_shared<DataTypeInt64>());
    add_field(FILE_FIELD_ETAG.data(), make_nullable(std::make_shared<DataTypeString>(256, TYPE_VARCHAR)));
    add_field(FILE_FIELD_LAST_MODIFIED_AT.data(), std::make_shared<DataTypeDateTimeV2>(3));
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
