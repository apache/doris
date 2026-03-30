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

#include <curl/curl.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/binary_cast.hpp"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/file_schema_descriptor.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "service/http/http_client.h"
#include "service/http/http_headers.h"
#include "util/jsonb_writer.h"
#include "util/jsonb_utils.h"

namespace doris {

namespace {

void write_jsonb_string(JsonbWriter& writer, const char* data, uint32_t size) {
    CHECK(writer.writeStartString());
    CHECK(writer.writeString(data, size));
    CHECK(writer.writeEndString());
}

struct FileViewdata {
    int64_t size;
    std::optional<std::string> etag;
    std::string last_modified_at;
};

std::string strip_url_suffix(std::string_view uri) {
    size_t end = uri.find_first_of("?#");
    if (end == std::string_view::npos) {
        return std::string(uri);
    }
    return std::string(uri.substr(0, end));
}

std::string extract_file_name(std::string_view uri) {
    std::string normalized = strip_url_suffix(uri);
    size_t pos = normalized.find_last_of('/');
    if (pos == std::string::npos) {
        return normalized;
    }
    if (pos + 1 >= normalized.size()) {
        return "";
    }
    return normalized.substr(pos + 1);
}

std::string extract_file_extension(const std::string& file_name) {
    size_t pos = file_name.find_last_of('.');
    if (pos == std::string::npos) {
        return "";
    }
    std::string extension = file_name.substr(pos);
    std::transform(extension.begin(), extension.end(), extension.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return extension;
}

std::string extract_url_scheme(std::string_view uri) {
    size_t pos = uri.find("://");
    if (pos == std::string_view::npos) {
        return "";
    }
    std::string scheme(uri.substr(0, pos));
    std::transform(scheme.begin(), scheme.end(), scheme.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return scheme;
}

std::string normalize_etag(std::string_view etag) {
    if (etag.size() >= 2 && etag.front() == '"' && etag.back() == '"') {
        return std::string(etag.substr(1, etag.size() - 2));
    }
    if (etag.size() >= 4 && etag.substr(0, 2) == "W/" && etag[2] == '"' && etag.back() == '"') {
        return std::string(etag.substr(0, 2)) + std::string(etag.substr(3, etag.size() - 4));
    }
    return std::string(etag);
}

std::string format_datetime_v2(const DateV2Value<DateTimeV2ValueType>& dt) {
    char buf[64];
    char* pos = dt.to_string(buf);
    return std::string(buf, pos - buf - 1);
}

Result<FileViewdata> fetch_file_metadata(std::string_view url) {
    const std::string scheme = extract_url_scheme(url);
    if (scheme != "http" && scheme != "https") {
        return ResultError(Status::InvalidArgument(
                "to_file(url) only supports HTTP(S) object URLs with embedded auth info, got: {}",
                url));
    }

    auto read_common_headers =
            [&](HttpClient& client,
                std::optional<uint64_t> fallback_length) -> Result<FileViewdata> {
        uint64_t content_length = fallback_length.value_or(0);
        if (!fallback_length.has_value()) {
            RETURN_IF_ERROR_RESULT(client.get_content_length(&content_length));
        }

        std::string etag;
        RETURN_IF_ERROR_RESULT(client.get_header(HttpHeaders::ETAG, &etag));
        if (!etag.empty()) {
            etag = normalize_etag(etag);
        }

        std::string last_modified;
        RETURN_IF_ERROR_RESULT(client.get_header(HttpHeaders::LAST_MODIFIED, &last_modified));
        if (last_modified.empty()) {
            return ResultError(Status::InvalidArgument(
                    "to_file(url) requires Last-Modified header from object storage, url={}", url));
        }

        time_t ts = curl_getdate(last_modified.c_str(), nullptr);
        if (ts < 0) {
            return ResultError(Status::InvalidArgument(
                    "failed to parse Last-Modified header '{}' for url={}", last_modified, url));
        }

        DateV2Value<DateTimeV2ValueType> dt;
        dt.from_unixtime(ts, cctz::utc_time_zone());
        return FileViewdata {
                .size = static_cast<int64_t>(content_length),
                .etag = etag.empty() ? std::nullopt : std::optional<std::string>(std::move(etag)),
                .last_modified_at = format_datetime_v2(dt),
        };
    };

    auto parse_total_size_from_content_range =
            [&](std::string_view content_range) -> Result<uint64_t> {
        const size_t slash_pos = content_range.rfind('/');
        if (slash_pos == std::string_view::npos || slash_pos + 1 >= content_range.size()) {
            return ResultError(Status::InvalidArgument(
                    "invalid Content-Range header '{}' for url={}", content_range, url));
        }
        std::string_view total_size_str = content_range.substr(slash_pos + 1);
        uint64_t total_size = 0;
        const auto [ptr, ec] = std::from_chars(
                total_size_str.data(), total_size_str.data() + total_size_str.size(), total_size);
        if (ec != std::errc() || ptr != total_size_str.data() + total_size_str.size()) {
            return ResultError(Status::InvalidArgument(
                    "invalid Content-Range total size '{}' for url={}", total_size_str, url));
        }
        return total_size;
    };

    HttpClient client;
    RETURN_IF_ERROR_RESULT(client.init(std::string(url), false));
    client.set_method(HEAD);
    client.set_unrestricted_auth(1);
    RETURN_IF_ERROR_RESULT(client.execute());

    const long head_status = client.get_http_status();
    if (head_status >= 200 && head_status < 300) {
        return read_common_headers(client, std::nullopt);
    }

    RETURN_IF_ERROR_RESULT(client.init(std::string(url), false));
    client.set_method(GET);
    client.set_unrestricted_auth(1);
    client.set_range(0, 1);
    RETURN_IF_ERROR_RESULT(client.execute([](const void*, size_t) { return true; }));

    const long get_status = client.get_http_status();
    if (get_status < 200 || get_status >= 300) {
        return ResultError(Status::HttpError(
                "failed to fetch file metadata, HEAD status={}, GET status={}, url={}", head_status,
                get_status, url));
    }

    std::optional<uint64_t> total_size;
    std::string content_range;
    RETURN_IF_ERROR_RESULT(client.get_header(HttpHeaders::CONTENT_RANGE, &content_range));
    if (!content_range.empty()) {
        auto total_size_result = parse_total_size_from_content_range(content_range);
        if (!total_size_result.has_value()) {
            return ResultError(total_size_result.error());
        }
        total_size = total_size_result.value();
    }

    if (!total_size.has_value()) {
        uint64_t content_length = 0;
        RETURN_IF_ERROR_RESULT(client.get_content_length(&content_length));
        total_size = content_length;
    }

    return read_common_headers(client, total_size);
}

} // namespace

class FunctionToFile : public IFunction {
public:
    static constexpr auto name = "to_file";

    static FunctionPtr create() { return std::make_shared<FunctionToFile>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeFile>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 1);

        ColumnPtr uri_col_ptr =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnString* uri_col = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(uri_col_ptr.get())) {
            uri_col = &assert_cast<const ColumnString&>(nullable->get_nested_column());
        } else {
            uri_col = &assert_cast<const ColumnString&>(*uri_col_ptr);
        }

        const auto& schema = FileSchemaDescriptor::instance();
        auto result_col = ColumnFile::create(schema);
        auto& jsonb_col = assert_cast<ColumnString&>(result_col->get_jsonb_column());
        jsonb_col.reserve(input_rows_count);
        JsonbWriter writer;

        for (size_t row = 0; row < input_rows_count; ++row) {
            StringRef uri_ref = uri_col->get_data_at(row);
            std::string uri = uri_ref.to_string();
            std::string file_name = extract_file_name(uri);
            std::string file_ext = extract_file_extension(file_name);
            FileViewdata metadata = DORIS_TRY(fetch_file_metadata(uri));

            writer.reset();
            writer.writeStartObject();
            writer.writeKey(schema.field(0).name,
                            static_cast<uint8_t>(strlen(schema.field(0).name)));
            write_jsonb_string(writer, uri.data(), cast_set<uint32_t>(uri.size()));
            writer.writeKey(schema.field(1).name,
                            static_cast<uint8_t>(strlen(schema.field(1).name)));
            write_jsonb_string(writer, file_name.data(), cast_set<uint32_t>(file_name.size()));
            writer.writeKey(schema.field(2).name,
                            static_cast<uint8_t>(strlen(schema.field(2).name)));
            write_jsonb_string(writer, file_ext.data(), cast_set<uint32_t>(file_ext.size()));
            writer.writeKey(schema.field(3).name,
                            static_cast<uint8_t>(strlen(schema.field(3).name)));
            writer.writeInt64(metadata.size);
            writer.writeKey(schema.field(4).name,
                            static_cast<uint8_t>(strlen(schema.field(4).name)));
            if (metadata.etag.has_value()) {
                write_jsonb_string(writer, metadata.etag->data(),
                                   cast_set<uint32_t>(metadata.etag->size()));
            } else {
                writer.writeNull();
            }
            writer.writeKey(schema.field(5).name,
                            static_cast<uint8_t>(strlen(schema.field(5).name)));
            write_jsonb_string(writer, metadata.last_modified_at.data(),
                               cast_set<uint32_t>(metadata.last_modified_at.size()));
            writer.writeEndObject();
            jsonb_col.insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
            LOG(INFO) << "to_file serialized row=" << row << ", json="
                       << JsonbToJson::jsonb_to_json_string(writer.getOutput()->getBuffer(),
                                                            writer.getOutput()->getSize());
        }

        block.replace_by_position(result, std::move(result_col));
        return Status::OK();
    }
};

void register_function_file(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionToFile>();
}

} // namespace doris
