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
#include <string>
#include <string_view>

#include "core/data_type/data_type.h"
#include "util/jsonb_writer.h"

namespace doris {

struct FileFieldDesc {
    const char* name;
    DataTypePtr type;
};

struct FileMetadata {
    std::string uri;
    std::string file_name;
    std::string content_type;
    int64_t size = 0;
    std::string region;
    std::string endpoint;
    std::string ak;
    std::string sk;
    std::string role_arn;
    std::string external_id;
};

// now struct FileInfo only contains file_name and file_size, 
// and if we want to get ETAG and LAST_MODIFIED_AT, need refactor FileInfo and the underlying file system client to support them.
class FileSchemaDescriptor final {
public:
    enum class Field : uint8_t {
        URI = 0,
        FILE_NAME = 1,
        CONTENT_TYPE = 2,
        SIZE = 3,
        REGION = 4,
        ENDPOINT = 5,
        AK = 6,
        SK = 7,
        ROLE_ARN = 8,
        EXTERNAL_ID = 9,
    };

    static const FileSchemaDescriptor& instance();
    const FileFieldDesc& field(size_t idx) const { return _fields[idx]; }
    const FileFieldDesc& field(Field field_id) const {
        return _fields[static_cast<size_t>(field_id)];
    }
    std::string_view field_name(Field field_id) const { return field(field_id).name; }

    // Shared utilities for FILE type serialization.
    static std::string extract_file_name(std::string_view uri);
    // Returns the lowercased file extension including the dot (e.g., ".jpg", ".csv").
    static std::string extract_file_extension(const std::string& file_name);
    // Maps file extension to MIME content type (e.g., ".jpg" → "image/jpeg").
    static std::string extension_to_content_type(const std::string& ext);
    static void write_jsonb_string(JsonbWriter& writer, const std::string& value);
    static void write_jsonb_key(JsonbWriter& writer, std::string_view key);
    // Serializes a FileMetadata into a complete JSONB object.
    static void write_file_jsonb(JsonbWriter& writer, const FileMetadata& metadata);

private:
    FileSchemaDescriptor();
    std::vector<FileFieldDesc> _fields;
};

} // namespace doris
