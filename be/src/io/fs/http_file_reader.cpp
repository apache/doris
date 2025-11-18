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

#include "io/fs/http_file_reader.h"

#include <curl/curl.h>
#include <curl/easy.h>

#include <algorithm>

#include "common/logging.h"

namespace doris::io {

Result<FileReaderSPtr> HttpFileReader::create(const std::string& url,
                                              const std::map<std::string, std::string>& props,
                                              const FileReaderOptions& opts,
                                              RuntimeProfile* /*profile*/) {
    OpenFileInfo ofi;
    ofi.path = Path(url);
    ofi.extend_info = props;

    auto reader = std::make_shared<HttpFileReader>(ofi, url);

    // Open the file to detect Range support and validate configuration
    RETURN_IF_ERROR_RESULT(reader->open(opts));

    return reader;
}

HttpFileReader::HttpFileReader(const OpenFileInfo& fileInfo, std::string url)
        : _extend_kv(fileInfo.extend_info),
          _path(fileInfo.path),
          _url(std::move(url)),
          _client(std::make_unique<HttpClient>()) {
    auto etag_iter = _extend_kv.find("etag");
    if (etag_iter != _extend_kv.end()) {
        _etag = etag_iter->second;
    }

    auto lm_iter = _extend_kv.find("last_modified");
    if (lm_iter != _extend_kv.end()) {
        _last_modified = std::stoll(lm_iter->second);
    }

    auto size_iter = _extend_kv.find("file_size");
    if (size_iter != _extend_kv.end()) {
        _file_size = std::stoull(size_iter->second);
        _initialized = true;
    }

    // Parse configuration for non-Range request handling
    auto enable_range_iter = _extend_kv.find("http.enable.range.request");
    if (enable_range_iter != _extend_kv.end()) {
        // Convert to lowercase for case-insensitive comparison
        std::string value = enable_range_iter->second;
        std::transform(value.begin(), value.end(), value.begin(), ::tolower);
        _enable_range_request = (value != "false" && value != "0");
    }

    auto max_size_iter = _extend_kv.find("http.max.request.size.bytes");
    if (max_size_iter != _extend_kv.end()) {
        try {
            _max_request_size_bytes = std::stoull(max_size_iter->second);
        } catch (const std::exception& _) {
            LOG(WARNING) << "Invalid http.max.request.size.bytes value: " << max_size_iter->second
                         << ", using default: " << DEFAULT_MAX_REQUEST_SIZE;
            _max_request_size_bytes = DEFAULT_MAX_REQUEST_SIZE;
        }
    }

    _read_buffer = std::make_unique<char[]>(READ_BUFFER_SIZE);
}

HttpFileReader::~HttpFileReader() {
    static_cast<void>(close());
}

Status HttpFileReader::open(const FileReaderOptions& opts) {
    if (_initialized) {
        return Status::OK();
    }

    // Step 1: HEAD request to get file metadata
    RETURN_IF_ERROR(prepare_client(/*set_fail_on_error=*/true));
    _client->set_method(HttpMethod::HEAD);
    RETURN_IF_ERROR(_client->execute());

    uint64_t content_length = 0;
    RETURN_IF_ERROR(_client->get_content_length(&content_length));

    _file_size = content_length;
    _size_known = true;

    // Step 2: Check if Range request is disabled by configuration
    if (!_enable_range_request) {
        // User explicitly disabled Range requests, use non-Range mode directly
        _range_supported = false;
        LOG(INFO) << "Range requests disabled by configuration for " << _url
                  << ", using non-Range mode. File size: " << _file_size << " bytes";

        // Check if file size exceeds limit for non-Range mode
        if (_file_size > _max_request_size_bytes) {
            return Status::InternalError(
                    "Non-Range mode: file size ({} bytes) exceeds maximum allowed size ({} bytes, "
                    "configured by http.max.request.size.bytes). URL: {}",
                    _file_size, _max_request_size_bytes, _url);
        }

        LOG(INFO) << "Non-Range mode validated for " << _url << ", file size: " << _file_size
                  << " bytes, max allowed: " << _max_request_size_bytes << " bytes";
    } else {
        // Step 3: Range request is enabled (default), detect Range support
        VLOG(1) << "Detecting Range support for URL: " << _url;
        RETURN_IF_ERROR(detect_range_support());

        // Step 4: Validate Range support detection result
        if (!_range_supported) {
            // Server does not support Range and Range is required
            return Status::NotSupported(
                    "HTTP server does not support Range requests (RFC 7233), which is required "
                    "for reading files. File size: {} bytes, URL: {}. "
                    "To allow reading without Range support, set "
                    "'http.enable.range.request'='false' "
                    "in properties and configure 'http.max.request.size.bytes' appropriately "
                    "(note: this may cause high memory usage for large files).",
                    _file_size, _url);
        }

        LOG(INFO) << "HTTP server supports Range requests for " << _url;
    }

    _initialized = true;
    return Status::OK();
}

Status HttpFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                    const IOContext* /*io_ctx*/) {
    VLOG(2) << "HttpFileReader::read_at_impl offset=" << offset << " size=" << result.size
            << " url=" << _url << " range_supported=" << _range_supported;

    if (!_read_buffer) {
        _read_buffer = std::make_unique<char[]>(READ_BUFFER_SIZE);
    }

    size_t to_read = result.size;
    size_t buffer_offset = 0;

    if (_size_known && offset >= _file_size) {
        *bytes_read = 0;
        return Status::OK();
    }

    // Try to serve from buffer cache
    if (offset >= _buffer_start && offset < _buffer_end) {
        size_t buffer_idx = offset - _buffer_start;
        size_t available = _buffer_end - offset;
        size_t copy_len = std::min(available, to_read);

        DCHECK(buffer_idx + copy_len <= READ_BUFFER_SIZE)
                << "Buffer overflow: buffer_idx=" << buffer_idx << " copy_len=" << copy_len
                << " READ_BUFFER_SIZE=" << READ_BUFFER_SIZE;

        std::memcpy(result.data, _read_buffer.get() + buffer_idx, copy_len);
        buffer_offset += copy_len;
        to_read -= copy_len;
        offset += copy_len;

        VLOG(2) << "Buffer cache hit: copied " << copy_len << " bytes";
    } else {
        // Buffer miss, invalidate cache
        _buffer_start = 0;
        _buffer_end = 0;
        VLOG(2) << "Buffer cache miss";
    }

    if (to_read == 0) {
        *bytes_read = buffer_offset;
        return Status::OK();
    }

    size_t remaining = to_read;
    if (_size_known) {
        uint64_t left = (_file_size > offset) ? (_file_size - offset) : 0;
        if (left == 0) {
            *bytes_read = buffer_offset;
            return Status::OK();
        }
        remaining = std::min<uint64_t>(to_read, left);
    }
    size_t req_len = (remaining > READ_BUFFER_SIZE) ? remaining : READ_BUFFER_SIZE;

    VLOG(2) << "Issuing HTTP GET request: offset=" << offset << " req_len=" << req_len
            << " with_range=" << _range_supported;

    // Prepare and initialize the HTTP client for GET request
    RETURN_IF_ERROR(prepare_client(/*set_fail_on_error=*/false));
    _client->set_method(HttpMethod::GET);

    _client->set_header("Expect", "");
    _client->set_header("Connection", "close");

    bool with_range = _range_supported;
    if (with_range) _client->set_range(offset, req_len);

    std::string buf;
    buf.reserve(req_len);
    size_t total_received = 0;
    bool size_limit_exceeded = false;

    auto cb = [&](const void* data, size_t len) {
        total_received += len;

        // If using non-Range mode, enforce size limit to prevent OOM
        if (!_range_supported && total_received > _max_request_size_bytes) {
            size_limit_exceeded = true;
            VLOG(1) << "Stopping download: received " << total_received << " bytes, exceeds limit "
                    << _max_request_size_bytes;
            return false; // Stop receiving - this will cause CURL to return an error
        }

        buf.append(reinterpret_cast<const char*>(data), len);
        return true;
    };

    Status exec_status = _client->execute(cb);

    // Check if we stopped due to size limit - this is expected behavior
    if (size_limit_exceeded) {
        return Status::InternalError(
                "HTTP response too large: received {} bytes, exceeds maximum allowed size {} "
                "bytes (configured by max.request.size.bytes). URL: {}",
                total_received, _max_request_size_bytes, _url);
    }

    // If there's an error and it's not due to our size limit check, return it
    RETURN_IF_ERROR(exec_status);

    long http_status = _client->get_http_status();
    VLOG(2) << "HTTP response: status=" << http_status << " received_bytes=" << buf.size();

    if (buf.empty()) {
        *bytes_read = buffer_offset;
        return Status::OK();
    }

    // Defensive check: if we sent Range but server returned 200 instead of 206
    // This should rarely happen since we detect Range support in open()
    if (with_range && offset > 0 && http_status == 200) {
        LOG(ERROR) << "HTTP server unexpectedly does not support Range requests for " << _url
                   << " (this should have been detected in open()). HTTP status: " << http_status
                   << ", received: " << buf.size()
                   << " bytes. This indicates a server behavior change.";

        return Status::InternalError(
                "HTTP server does not support Range requests but this was not detected during "
                "file open. This may indicate the server behavior has changed. "
                "HTTP status: {}, received: {} bytes. URL: {}",
                http_status, buf.size(), _url);
    }

    // Handle non-Range mode: when _range_supported is false, we download full file
    if (!_range_supported && offset > 0) {
        // We're in non-Range mode and need data from middle of file
        // The full file should have been downloaded
        if (offset >= buf.size()) {
            *bytes_read = buffer_offset;
            return Status::OK();
        }

        size_t slice_len = std::min<size_t>(remaining, buf.size() - offset);
        std::memcpy(result.data + buffer_offset, buf.data() + offset, slice_len);
        buffer_offset += slice_len;

        size_t cached = std::min(slice_len, (size_t)READ_BUFFER_SIZE);
        std::memcpy(_read_buffer.get(), buf.data() + offset, cached);
        _buffer_start = offset;
        _buffer_end = offset + cached;

        *bytes_read = buffer_offset;
        return Status::OK();
    }

    if (to_read > READ_BUFFER_SIZE) {
        if (buf.size() > remaining) {
            return Status::InternalError("HTTP response larger than requested buffer");
        }
        std::memcpy(result.data + buffer_offset, buf.data(), buf.size());
        buffer_offset += buf.size();
    } else {
        size_t cached = std::min(buf.size(), (size_t)READ_BUFFER_SIZE);
        std::memcpy(_read_buffer.get(), buf.data(), cached);
        _buffer_start = offset;
        _buffer_end = offset + cached;

        size_t copy_len = std::min(remaining, cached);
        std::memcpy(result.data + buffer_offset, _read_buffer.get(), copy_len);
        buffer_offset += copy_len;
    }

    if (!_size_known && with_range && buf.size() < req_len) {
        _size_known = true;
        _file_size = offset + buf.size();
    }

    *bytes_read = buffer_offset;
    return Status::OK();
}

Status HttpFileReader::close() {
    if (_closed.exchange(true)) {
        return Status::OK();
    }

    // Release buffer memory (1MB)
    _read_buffer.reset();
    _buffer_start = 0;
    _buffer_end = 0;

    // Release HttpClient resources
    _client.reset();

    return Status::OK();
}

Status HttpFileReader::prepare_client(bool set_fail_on_error) {
    if (!_client) {
        return Status::InternalError("HttpClient is not initialized");
    }

    // Initialize the HTTP client with URL
    RETURN_IF_ERROR(_client->init(_url, set_fail_on_error));

    // Set custom headers from extend_kv
    for (const auto& kv : _extend_kv) {
        if (kv.first.rfind("http.header.", 0) == 0) {
            _client->set_header(kv.first.substr(strlen("http.header.")), kv.second);
        }
    }

    return Status::OK();
}

Status HttpFileReader::detect_range_support() {
    // Send a small Range request to test if the server supports it
    // We request only the first byte to minimize data transfer
    RETURN_IF_ERROR(prepare_client(/*set_fail_on_error=*/false));
    _client->set_method(HttpMethod::GET);
    _client->set_range(0, 1); // Request only the first byte

    std::string test_buf;
    size_t received = 0;
    constexpr size_t MAX_TEST_SIZE = 10240; // 10KB max for test
    bool stopped_by_limit = false;

    auto cb = [&](const void* data, size_t len) {
        received += len;
        // Limit test data to prevent downloading too much
        if (received > MAX_TEST_SIZE) {
            stopped_by_limit = true;
            VLOG(2) << "Stopping Range detection test after receiving " << received << " bytes";
            return false; // This will cause CURL to stop with an error
        }
        test_buf.append(reinterpret_cast<const char*>(data), len);
        return true;
    };

    Status exec_status = _client->execute(cb);

    // If we stopped because of size limit, it's not a real error
    if (!exec_status.ok() && stopped_by_limit) {
        VLOG(1) << "Range detection stopped at size limit (expected): " << exec_status.to_string();
        // Continue processing - this is expected behavior
    } else if (!exec_status.ok()) {
        // Real error
        return exec_status;
    }

    long http_status = _client->get_http_status();

    if (http_status == 206) {
        // HTTP 206 Partial Content - server supports Range requests
        _range_supported = true;
        VLOG(1) << "Range support detected (HTTP 206) for " << _url << ", received "
                << test_buf.size() << " bytes";
    } else if (http_status == 200) {
        // HTTP 200 OK - server does not support Range requests
        // It returned the full file (or a large portion)
        _range_supported = false;
        VLOG(1) << "Range not supported (HTTP 200) for " << _url << ", received " << test_buf.size()
                << " bytes in test";

        // If we received a lot of data, it's likely the full file
        if (test_buf.size() >= MAX_TEST_SIZE || stopped_by_limit) {
            LOG(WARNING) << "Server returned " << received << "+ bytes for Range test, "
                         << "indicating no Range support for " << _url;
        }
    } else {
        // Unexpected status code
        LOG(WARNING) << "Unexpected HTTP status " << http_status << " during Range detection for "
                     << _url << ", assuming Range is not supported";
        _range_supported = false;
    }

    return Status::OK();
}

} // namespace doris::io
