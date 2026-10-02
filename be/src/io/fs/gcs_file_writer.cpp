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

#include "io/fs/gcs_file_writer.h"

#include <arpa/inet.h>
#include <fmt/format.h>

#include <charconv>
#include <string_view>
#include <utility>

#include "common/logging.h"
#include "service/http/http_client.h"
#include "service/http/http_method.h"
#include "util/s3_uri.h"
#include "util/time.h"

namespace doris::io {
namespace {

constexpr std::string_view GCS_PROVIDER = "GCP";
constexpr std::string_view PROVIDER = "provider";
constexpr std::string_view ENDPOINT = "uri";
constexpr std::string_view AUTHORIZATION = "http.header.Authorization";
constexpr std::string_view TOKEN_EXPIRATION = "AWS_TOKEN_EXPIRATION_TIME_MS";
constexpr int MAX_UPLOAD_ATTEMPTS = 3;

struct GcsConfig {
    std::string bucket;
    std::string key;
    std::string endpoint;
    std::string authorization;
    int64_t token_expiration_time_ms = 0;
};

std::string require_property(const std::map<std::string, std::string>& properties,
                             std::string_view key) {
    auto it = properties.find(std::string(key));
    if (it == properties.end() || it->second.empty()) {
        return {};
    }
    return it->second;
}

Result<int64_t> parse_expiration(const std::string& value) {
    int64_t expiration = 0;
    auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), expiration);
    if (error != std::errc {} || end != value.data() + value.size() || expiration <= 0) {
        return ResultError(Status::InvalidArgument("Invalid GCS vended-token expiration time"));
    }
    if (expiration <= UnixMillis()) {
        return ResultError(Status::InvalidArgument("GCS vended token is expired"));
    }
    return expiration;
}

std::string strip_trailing_slashes(std::string value) {
    while (value.size() > 1 && value.ends_with('/')) {
        value.pop_back();
    }
    return value;
}

bool is_secure_or_loopback_endpoint(std::string_view endpoint) {
    constexpr std::string_view HTTPS = "https://";
    constexpr std::string_view HTTP = "http://";
    const bool secure = endpoint.starts_with(HTTPS);
    if (secure) {
        endpoint.remove_prefix(HTTPS.size());
    } else if (endpoint.starts_with(HTTP)) {
        endpoint.remove_prefix(HTTP.size());
    } else {
        return false;
    }
    std::string_view authority = endpoint.substr(0, endpoint.find_first_of("/?#"));
    if (authority.empty() || authority.find('@') != std::string_view::npos) {
        return false;
    }
    std::string host;
    if (authority.starts_with('[')) {
        size_t closing = authority.find(']');
        if (closing == std::string_view::npos) {
            return false;
        }
        host = authority.substr(1, closing - 1);
    } else {
        host = authority.substr(0, authority.find(':'));
    }
    if (host.empty()) {
        return false;
    }
    if (secure) {
        return true;
    }
    if (host == "localhost") {
        return true;
    }
    in_addr ipv4 {};
    if (inet_pton(AF_INET, host.c_str(), &ipv4) == 1) {
        return (ntohl(ipv4.s_addr) & 0xFF000000U) == 0x7F000000U;
    }
    in6_addr ipv6 {};
    return inet_pton(AF_INET6, host.c_str(), &ipv6) == 1 && IN6_IS_ADDR_LOOPBACK(&ipv6);
}

std::string encoded(std::string_view value) {
    constexpr char HEX[] = "0123456789ABCDEF";
    std::string result;
    result.reserve(value.size());
    for (unsigned char ch : value) {
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
            ch == '-' || ch == '_' || ch == '.' || ch == '~') {
            result.push_back(static_cast<char>(ch));
        } else {
            result.push_back('%');
            result.push_back(HEX[ch >> 4]);
            result.push_back(HEX[ch & 0x0F]);
        }
    }
    return result;
}

Result<GcsConfig> parse_config(const Path& path,
                               const std::map<std::string, std::string>& properties) {
    const std::string provider = require_property(properties, PROVIDER);
    if (provider != GCS_PROVIDER) {
        return ResultError(Status::InvalidArgument("GCS OAuth writer requires provider=GCP"));
    }
    if (!path.native().starts_with("gs://")) {
        return ResultError(Status::InvalidArgument("GCS OAuth writer requires a gs:// path"));
    }
    S3URI uri(path.native());
    RETURN_IF_ERROR_RESULT(uri.parse());

    GcsConfig config {.bucket = uri.get_bucket(),
                      .key = uri.get_key(),
                      .endpoint = require_property(properties, ENDPOINT),
                      .authorization = require_property(properties, AUTHORIZATION)};
    std::string expiration_value = require_property(properties, TOKEN_EXPIRATION);
    if (config.endpoint.empty() || config.authorization.empty() || expiration_value.empty()) {
        return ResultError(Status::InvalidArgument(
                "GCS OAuth writer requires uri, Authorization, and token expiration properties"));
    }
    if (!config.authorization.starts_with("Bearer ") ||
        config.authorization.size() == std::string_view("Bearer ").size()) {
        return ResultError(Status::InvalidArgument("GCS OAuth writer requires a Bearer token"));
    }
    if (config.authorization.find_first_of("\r\n") != std::string::npos) {
        return ResultError(Status::InvalidArgument("GCS OAuth token contains a line break"));
    }
    if (!is_secure_or_loopback_endpoint(config.endpoint)) {
        return ResultError(
                Status::InvalidArgument("GCS OAuth endpoint must use HTTPS unless it is loopback"));
    }
    if (config.endpoint.find_first_of("?#") != std::string::npos) {
        return ResultError(
                Status::InvalidArgument("GCS OAuth endpoint must not contain query or fragment"));
    }
    auto expiration = parse_expiration(expiration_value);
    if (!expiration.has_value()) {
        return unexpected(std::move(expiration).error());
    }
    config.token_expiration_time_ms = std::move(expiration).value();
    config.endpoint = strip_trailing_slashes(std::move(config.endpoint));
    return config;
}

Result<int64_t> parse_committed_offset(std::string_view range) {
    if (range.empty()) {
        return -1;
    }
    constexpr std::string_view PREFIX = "bytes=0-";
    if (!range.starts_with(PREFIX)) {
        return ResultError(Status::HttpError("Invalid GCS resumable Range header"));
    }
    range.remove_prefix(PREFIX.size());
    int64_t offset = -1;
    auto [end, error] = std::from_chars(range.data(), range.data() + range.size(), offset);
    if (error != std::errc {} || end != range.data() + range.size() || offset < 0) {
        return ResultError(Status::HttpError("Invalid GCS resumable Range header"));
    }
    return offset;
}

} // namespace

Result<FileWriterPtr> GcsFileWriter::create(const Path& path,
                                            const std::map<std::string, std::string>& properties,
                                            size_t chunk_size) {
    if (chunk_size < MIN_CHUNK_SIZE || chunk_size % MIN_CHUNK_SIZE != 0) {
        return ResultError(Status::InvalidArgument(
                "GCS resumable upload chunk size must be a multiple of 256 KiB"));
    }
    auto config = parse_config(path, properties);
    if (!config.has_value()) {
        return unexpected(std::move(config).error());
    }
    GcsConfig values = std::move(config).value();
    return FileWriterPtr(new GcsFileWriter(
            path, std::move(values.bucket), std::move(values.key), std::move(values.endpoint),
            std::move(values.authorization), values.token_expiration_time_ms, chunk_size));
}

GcsFileWriter::GcsFileWriter(Path path, std::string bucket, std::string key, std::string endpoint,
                             std::string authorization, int64_t token_expiration_time_ms,
                             size_t chunk_size)
        : _path(std::move(path)),
          _bucket(std::move(bucket)),
          _key(std::move(key)),
          _endpoint(std::move(endpoint)),
          _authorization(std::move(authorization)),
          _token_expiration_time_ms(token_expiration_time_ms),
          _chunk_size(chunk_size) {
    _buffer.reserve(_chunk_size + MIN_CHUNK_SIZE);
}

GcsFileWriter::~GcsFileWriter() {
    if (_state == State::OPENED) {
        _cancel_upload();
    }
}

Status GcsFileWriter::appendv(const Slice* data, size_t data_cnt) {
    if (_state != State::OPENED) {
        return Status::InternalError("append to closed GCS file: {}", _path.native());
    }
    for (size_t i = 0; i < data_cnt; ++i) {
        _buffer.append(data[i].data, data[i].size);
        _bytes_appended += data[i].size;
        while (_buffer.size() > _chunk_size) {
            RETURN_IF_ERROR(_upload_buffer(false));
        }
    }
    return Status::OK();
}

Status GcsFileWriter::close(bool /*non_block*/) {
    if (_state != State::OPENED) {
        return Status::InternalError("GCS file writer already closed: {}", _path.native());
    }
    RETURN_IF_ERROR(_start_upload());
    if (_bytes_appended == 0) {
        RETURN_IF_ERROR(_upload_empty_object());
    } else {
        RETURN_IF_ERROR(_upload_buffer(true));
    }
    _state = State::CLOSED;
    return Status::OK();
}

Status GcsFileWriter::_start_upload() {
    if (!_session_uri.empty()) {
        return Status::OK();
    }
    const std::string url = _endpoint + "/upload/storage/v1/b/" + encoded(_bucket) +
                            "/o?uploadType=resumable&name=" + encoded(_key);
    Status last_status;
    for (int attempt = 0; attempt < MAX_UPLOAD_ATTEMPTS; ++attempt) {
        HttpClient client;
        RETURN_IF_ERROR(_prepare_client(&client, url));
        client.set_content_type("application/json");
        client.set_header("X-Upload-Content-Type", "application/octet-stream");
        client.set_header("Expect", "");
        std::string response;
        last_status = client.execute_post_request("", &response);
        if (!last_status.ok()) {
            continue;
        }
        const long status = client.get_http_status();
        if (status != 200 && status != 201) {
            last_status =
                    Status::HttpError("GCS resumable upload initiation returned HTTP {}", status);
            if (status < 500) {
                return last_status;
            }
            continue;
        }
        RETURN_IF_ERROR(client.get_response_header("Location", &_session_uri));
        if (_session_uri.empty()) {
            return Status::HttpError("GCS resumable upload response has no Location header");
        }
        if (!is_secure_or_loopback_endpoint(_session_uri)) {
            _session_uri.clear();
            return Status::HttpError("GCS resumable upload returned an insecure session URI");
        }
        return Status::OK();
    }
    return last_status;
}

Status GcsFileWriter::_upload_buffer(bool final_chunk) {
    RETURN_IF_ERROR(_start_upload());
    const size_t size = final_chunk ? _buffer.size() : _chunk_size;
    std::string payload = _buffer.substr(0, size);
    RETURN_IF_ERROR(_send_chunk(std::move(payload), final_chunk));
    _buffer.erase(0, size);
    return Status::OK();
}

Status GcsFileWriter::_upload_empty_object() {
    HttpClient client;
    RETURN_IF_ERROR(_prepare_client(&client, _session_uri));
    client.set_header("Content-Range", "bytes */0");
    client.set_header("Expect", "");
    std::string response;
    RETURN_IF_ERROR(client.execute_put_request("", &response));
    const long status = client.get_http_status();
    if (status != 200 && status != 201) {
        return Status::HttpError("GCS empty resumable upload returned HTTP {}", status);
    }
    return Status::OK();
}

Status GcsFileWriter::_send_chunk(std::string payload, bool final_chunk) {
    const size_t chunk_start = _bytes_uploaded;
    const size_t chunk_end = chunk_start + payload.size() - 1;
    size_t next = chunk_start;
    Status last_status;
    for (int attempt = 0; attempt < MAX_UPLOAD_ATTEMPTS; ++attempt) {
        HttpClient client;
        RETURN_IF_ERROR(_prepare_client(&client, _session_uri));
        const std::string total = final_chunk ? std::to_string(_bytes_appended) : "*";
        client.set_header("Content-Range", fmt::format("bytes {}-{}/{}", next, chunk_end, total));
        client.set_content_type("application/octet-stream");
        client.set_header("Expect", "");
        std::string response;
        last_status = client.execute_put_request(payload.substr(next - chunk_start), &response);
        const long http_status = last_status.ok() ? client.get_http_status() : 0;
        if (last_status.ok() && http_status < 500) {
            auto progress = _progress_from_response(client, final_chunk);
            if (!progress.has_value()) {
                return std::move(progress).error();
            }
            if (progress->complete) {
                _bytes_uploaded = chunk_end + 1;
                return Status::OK();
            }
            if (progress->committed_offset >= static_cast<int64_t>(chunk_end)) {
                if (!final_chunk) {
                    _bytes_uploaded = chunk_end + 1;
                    return Status::OK();
                }
                auto final_progress = _query_progress(true);
                if (final_progress.has_value() && final_progress->complete) {
                    _bytes_uploaded = chunk_end + 1;
                    return Status::OK();
                }
            } else if (progress->committed_offset >= static_cast<int64_t>(next) - 1) {
                next = static_cast<size_t>(progress->committed_offset + 1);
                continue;
            } else {
                return Status::HttpError("GCS resumable upload returned an invalid offset");
            }
        } else {
            auto progress = _query_progress(final_chunk);
            if (!progress.has_value()) {
                continue;
            }
            if (progress->complete) {
                _bytes_uploaded = chunk_end + 1;
                return Status::OK();
            }
            if (progress->committed_offset >= static_cast<int64_t>(next) - 1 &&
                progress->committed_offset <= static_cast<int64_t>(chunk_end)) {
                next = static_cast<size_t>(progress->committed_offset + 1);
                if (next > chunk_end && !final_chunk) {
                    _bytes_uploaded = chunk_end + 1;
                    return Status::OK();
                }
            }
        }
    }
    return last_status.ok() ? Status::HttpError("GCS resumable upload did not complete for {}",
                                                _path.native())
                            : last_status;
}

Result<GcsFileWriter::UploadProgress> GcsFileWriter::_query_progress(bool final_chunk) {
    HttpClient client;
    RETURN_IF_ERROR_RESULT(_prepare_client(&client, _session_uri));
    const std::string total = final_chunk ? std::to_string(_bytes_appended) : "*";
    client.set_header("Content-Range", "bytes */" + total);
    client.set_header("Expect", "");
    std::string response;
    RETURN_IF_ERROR_RESULT(client.execute_put_request("", &response));
    return _progress_from_response(client, final_chunk);
}

Result<GcsFileWriter::UploadProgress> GcsFileWriter::_progress_from_response(
        const HttpClient& client, bool final_chunk) const {
    const long status = client.get_http_status();
    if (status == 200 || status == 201) {
        if (!final_chunk) {
            return ResultError(
                    Status::HttpError("GCS resumable upload completed before the final chunk"));
        }
        return UploadProgress {.complete = true,
                               .committed_offset = static_cast<int64_t>(_bytes_appended) - 1};
    }
    if (status != 308) {
        return ResultError(Status::HttpError("GCS resumable upload returned HTTP {}", status));
    }
    std::string range;
    RETURN_IF_ERROR_RESULT(client.get_response_header("Range", &range));
    auto committed_offset = parse_committed_offset(range);
    if (!committed_offset.has_value()) {
        return unexpected(std::move(committed_offset).error());
    }
    return UploadProgress {.complete = false,
                           .committed_offset = std::move(committed_offset).value()};
}

Status GcsFileWriter::_prepare_client(HttpClient* client, const std::string& url) const {
    RETURN_IF_ERROR(_validate_token());
    RETURN_IF_ERROR(client->init(url, false, false));
    client->set_authorization(_authorization);
    return Status::OK();
}

Status GcsFileWriter::_validate_token() const {
    if (_token_expiration_time_ms <= UnixMillis()) {
        return Status::InvalidArgument("GCS vended token is expired");
    }
    return Status::OK();
}

void GcsFileWriter::_cancel_upload() {
    if (_session_uri.empty() || !_validate_token().ok()) {
        return;
    }
    HttpClient client;
    if (!_prepare_client(&client, _session_uri).ok()) {
        return;
    }
    client.set_method(DELETE);
    client.set_header("Content-Length", "0");
    client.set_header("Expect", "");
    std::string response;
    static_cast<void>(client.execute(&response));
}

Status GcsFileWriter::delete_object(const Path& path,
                                    const std::map<std::string, std::string>& properties) {
    auto config = parse_config(path, properties);
    if (!config.has_value()) {
        return std::move(config).error();
    }
    GcsConfig values = std::move(config).value();
    const std::string url = values.endpoint + "/storage/v1/b/" + encoded(values.bucket) + "/o/" +
                            encoded(values.key);
    HttpClient client;
    RETURN_IF_ERROR(client.init(url, false, false));
    client.set_authorization(values.authorization);
    client.set_method(DELETE);
    client.set_header("Content-Length", "0");
    client.set_header("Expect", "");
    std::string response;
    RETURN_IF_ERROR(client.execute(&response));
    const long status = client.get_http_status();
    if (status != 200 && status != 204 && status != 404) {
        return Status::HttpError("GCS object delete returned HTTP {}", status);
    }
    return Status::OK();
}

} // namespace doris::io
