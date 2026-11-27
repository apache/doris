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

#include "format_v2/lance/lance_blob_column_reader.h"

#include <arrow/array.h>
#include <fmt/format.h>
#include <rapidjson/document.h>

#include <algorithm>
#include <cctype>
#include <limits>
#include <string>
#include <utility>

#include "core/data_type/data_type_file.h"
#include "core/field.h"
#include "core/value/file_value.h"
#include "util/url_coding.h"

namespace doris::format::lance {

bool lance_blob_storage_is_local(std::string_view uri) {
    const auto end = uri.find("://");
    if (end == std::string_view::npos || end == 0) {
        return true;
    }
    std::string scheme(uri.substr(0, end));
    std::transform(scheme.begin(), scheme.end(), scheme.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return scheme == "file" || scheme == "memory";
}

Status lance_blob_base_paths_have_local_storage(std::string_view json, bool* has_local_storage) {
    rapidjson::Document document;
    document.Parse(json.data(), json.size());
    if (document.HasParseError() || !document.IsObject()) {
        return Status::Corruption("Invalid Lance base-path registry");
    }
    bool local = false;
    for (auto it = document.MemberBegin(); it != document.MemberEnd(); ++it) {
        if (!it->value.IsObject() || !it->value.HasMember("path") ||
            !it->value["path"].IsString()) {
            return Status::Corruption("Invalid path in Lance base-path registry");
        }
        const auto& path = it->value["path"];
        local |= lance_blob_storage_is_local({path.GetString(), path.GetStringLength()});
    }
    *has_local_storage = local;
    return Status::OK();
}

Status build_lance_blob_descriptors(const std::shared_ptr<arrow::Array>& descriptors,
                                    const std::vector<uint64_t>& row_ids,
                                    const ColumnDefinition& column, uint32_t source_id,
                                    MutableColumnPtr* output) {
    if (source_id == 0) {
        return Status::InvalidArgument("Lance BLOB source ID was not assigned by FE");
    }
    const auto values = std::dynamic_pointer_cast<arrow::StructArray>(descriptors);
    if (!values || values->length() != row_ids.size()) {
        return Status::Corruption("Missing or misaligned Lance BLOB descriptors for '{}'",
                                  column.name);
    }
    const auto kinds = std::dynamic_pointer_cast<arrow::UInt8Array>(values->GetFieldByName("kind"));
    const auto sizes =
            std::dynamic_pointer_cast<arrow::UInt64Array>(values->GetFieldByName("size"));
    const auto base_ids =
            std::dynamic_pointer_cast<arrow::UInt32Array>(values->GetFieldByName("blob_id"));
    const auto uris =
            std::dynamic_pointer_cast<arrow::StringArray>(values->GetFieldByName("blob_uri"));
    if (!kinds || !sizes || !base_ids || !uris) {
        return Status::Corruption("Invalid Lance Blob v2 descriptor schema for '{}'", column.name);
    }
    auto result = column.type->create_column();
    result->reserve(row_ids.size());
    for (size_t row = 0; row < row_ids.size(); ++row) {
        // Native Blob v2 treats either the struct or its kind child being NULL as a NULL Blob.
        if (values->IsNull(row) || kinds->IsNull(row)) {
            if (!column.type->is_nullable()) {
                return Status::Corruption("NULL in required Lance BLOB column '{}'", column.name);
            }
            result->insert_default();
            continue;
        }
        const auto kind = kinds->Value(row);
        if (kind > 3 || sizes->IsNull(row)) {
            return Status::Corruption("Invalid Lance BLOB descriptor for '{}'", column.name);
        }
        const bool external = kind == 3;
        if (external) {
            if (base_ids->IsNull(row) || uris->IsNull(row)) {
                return Status::Corruption("Invalid external Lance BLOB descriptor");
            }
            // Registered bases were inspected once by the table reader. An absolute external
            // reference (base ID 0) can additionally point to a BE-local object.
            if (base_ids->Value(row) == 0 && lance_blob_storage_is_local(uris->GetView(row))) {
                return Status::NotSupported("Local external Lance BLOB objects are not supported");
            }
        }
        File file(DataTypeFile::FIELD_COUNT);
        auto uri = fmt::format("lance-blob://{}", row_ids[row]);
        if (!uris->IsNull(row) && uris->value_length(row) > 0) {
            std::string encoded_blob_uri;
            url_encode(uris->GetView(row), &encoded_blob_uri);
            uri.append("?blob_uri=").append(encoded_blob_uri);
        }
        file[DataTypeFile::URI_FIELD_INDEX] = Field::create_field<TYPE_STRING>(std::move(uri));
        const auto size = sizes->Value(row);
        // External size=0 is a native "resolve object length" marker, not necessarily empty.
        // The physical position is deliberately not exposed as FILE.offset.
        if (!external || size != 0) {
            if (size > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
                return Status::Corruption("Lance BLOB size exceeds BIGINT");
            }
            file[DataTypeFile::SIZE_FIELD_INDEX] =
                    Field::create_field<TYPE_BIGINT>(static_cast<int64_t>(size));
        }
        file.blob_source_id = source_id;
        file.blob_row_id = row_ids[row];
        RETURN_IF_ERROR(validate_file(file));
        result->insert(Field::create_field<TYPE_FILE>(std::move(file)));
    }
    *output = std::move(result);
    return Status::OK();
}

} // namespace doris::format::lance
