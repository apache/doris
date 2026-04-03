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

#include <algorithm>
#include <cctype>
#include <cstring>
#include <unordered_map>

#include "common/cast_set.h"
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

    add_field("uri", std::make_shared<DataTypeString>(4096, TYPE_VARCHAR));
    add_field("file_name", std::make_shared<DataTypeString>(512, TYPE_VARCHAR));
    add_field("content_type", std::make_shared<DataTypeString>(128, TYPE_VARCHAR));
    add_field("size", std::make_shared<DataTypeInt64>());
    add_field("region", make_nullable(std::make_shared<DataTypeString>(64, TYPE_VARCHAR)));
    add_field("endpoint", make_nullable(std::make_shared<DataTypeString>(256, TYPE_VARCHAR)));
    add_field("ak", make_nullable(std::make_shared<DataTypeString>(256, TYPE_VARCHAR)));
    add_field("sk", make_nullable(std::make_shared<DataTypeString>(256, TYPE_VARCHAR)));
    add_field("role_arn", make_nullable(std::make_shared<DataTypeString>(256, TYPE_VARCHAR)));
    add_field("external_id", make_nullable(std::make_shared<DataTypeString>(256, TYPE_VARCHAR)));
}

std::string FileSchemaDescriptor::extract_file_name(std::string_view uri) {
    // Strip query string / fragment
    size_t end = uri.find_first_of("?#");
    std::string_view path = (end == std::string_view::npos) ? uri : uri.substr(0, end);
    size_t pos = path.find_last_of('/');
    if (pos == std::string_view::npos) {
        return std::string(path);
    }
    if (pos + 1 >= path.size()) {
        return "";
    }
    return std::string(path.substr(pos + 1));
}

std::string FileSchemaDescriptor::extract_file_extension(const std::string& file_name) {
    size_t pos = file_name.find_last_of('.');
    if (pos == std::string::npos) {
        return "";
    }
    std::string extension = file_name.substr(pos);
    std::transform(extension.begin(), extension.end(), extension.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return extension;
}

std::string FileSchemaDescriptor::extension_to_content_type(const std::string& ext) {
    static const std::unordered_map<std::string, std::string> mime_map = {
            {".csv", "text/csv"},
            {".tsv", "text/tab-separated-values"},
            {".json", "application/json"},
            {".jsonl", "application/x-ndjson"},
            {".parquet", "application/x-parquet"},
            {".orc", "application/x-orc"},
            {".avro", "application/avro"},
            {".txt", "text/plain"},
            {".log", "text/plain"},
            {".tbl", "text/plain"},
            {".xml", "application/xml"},
            {".html", "text/html"},
            {".htm", "text/html"},
            {".pdf", "application/pdf"},
            {".jpg", "image/jpeg"},
            {".jpeg", "image/jpeg"},
            {".png", "image/png"},
            {".gif", "image/gif"},
            {".bmp", "image/bmp"},
            {".svg", "image/svg+xml"},
            {".webp", "image/webp"},
            {".mp3", "audio/mpeg"},
            {".wav", "audio/wav"},
            {".mp4", "video/mp4"},
            {".avi", "video/x-msvideo"},
            {".gz", "application/gzip"},
            {".bz2", "application/x-bzip2"},
            {".zst", "application/zstd"},
            {".lz4", "application/x-lz4"},
            {".snappy", "application/x-snappy"},
            {".zip", "application/zip"},
            {".tar", "application/x-tar"},
    };
    if (auto it = mime_map.find(ext); it != mime_map.end()) {
        return it->second;
    }
    return "application/octet-stream";
}

void FileSchemaDescriptor::write_jsonb_string(JsonbWriter& writer, const std::string& value) {
    writer.writeStartString();
    writer.writeString(value.data(), cast_set<uint32_t>(value.size()));
    writer.writeEndString();
}

void FileSchemaDescriptor::write_jsonb_key(JsonbWriter& writer, std::string_view key) {
    writer.writeKey(key.data(), cast_set<uint8_t>(key.size()));
}

void FileSchemaDescriptor::write_file_jsonb(JsonbWriter& writer, const FileMetadata& metadata) {
    const auto& schema = instance();
    auto write_nullable_str = [&](Field field, const std::string& s) {
        write_jsonb_key(writer, schema.field_name(field));
        if (s.empty()) {
            writer.writeNull();
        } else {
            write_jsonb_string(writer, s);
        }
    };

    writer.writeStartObject();
    write_jsonb_key(writer, schema.field_name(Field::URI));
    write_jsonb_string(writer, metadata.uri);
    write_jsonb_key(writer, schema.field_name(Field::FILE_NAME));
    write_jsonb_string(writer, metadata.file_name);
    write_jsonb_key(writer, schema.field_name(Field::CONTENT_TYPE));
    write_jsonb_string(writer, metadata.content_type);
    write_jsonb_key(writer, schema.field_name(Field::SIZE));
    writer.writeInt64(metadata.size);
    write_nullable_str(Field::REGION, metadata.region);
    write_nullable_str(Field::ENDPOINT, metadata.endpoint);
    write_nullable_str(Field::AK, metadata.ak);
    write_nullable_str(Field::SK, metadata.sk);
    write_nullable_str(Field::ROLE_ARN, metadata.role_arn);
    write_nullable_str(Field::EXTERNAL_ID, metadata.external_id);
    writer.writeEndObject();
}
} // namespace doris
