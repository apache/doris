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

#include <optional>
#include <string_view>

#include "core/data_type/data_type.h"

namespace doris {

struct FileFieldDesc {
    const char* name;
    DataTypePtr type;
};

inline constexpr std::string_view FILE_FIELD_OBJECT_URI = "object_uri";
inline constexpr std::string_view FILE_FIELD_FILE_NAME = "file_name";
inline constexpr std::string_view FILE_FIELD_FILE_EXTENSION = "file_extension";
inline constexpr std::string_view FILE_FIELD_SIZE = "size";
inline constexpr std::string_view FILE_FIELD_ETAG = "etag";
inline constexpr std::string_view FILE_FIELD_LAST_MODIFIED_AT = "last_modified_at";

class FileSchemaDescriptor final {
public:
    enum class Field : uint8_t {
        OBJECT_URI = 0,
        FILE_NAME = 1,
        FILE_EXTENSION = 2,
        SIZE = 3,
        ETAG = 4,
        LAST_MODIFIED_AT = 5,
    };

    static const FileSchemaDescriptor& instance();
    const FileFieldDesc& field(size_t idx) const { return _fields[idx]; }
    const FileFieldDesc& field(Field field_id) const {
        return _fields[static_cast<size_t>(field_id)];
    }
    std::string_view field_name(Field field_id) const { return field(field_id).name; }
    std::optional<size_t> try_get_position(std::string_view name) const;

private:
    FileSchemaDescriptor();
    std::vector<FileFieldDesc> _fields;
};

} // namespace doris
