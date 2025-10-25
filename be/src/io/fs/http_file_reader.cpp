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

namespace doris::io {

Result<FileReaderSPtr> HttpFileReader::create(const std::string& url,
                                              const std::map<std::string, std::string>& props,
                                              const FileReaderOptions& opts,
                                              RuntimeProfile* /*profile*/) {
    OpenFileInfo ofi;
    ofi.path = Path(url);
    ofi.extend_info = props;

    auto reader = std::make_shared<HttpFileReader>(ofi, url);
    return reader;
}

HttpFileReader::HttpFileReader(const OpenFileInfo& fileInfo, std::string url)
        : _extend_kv(fileInfo.extend_info), _path(fileInfo.path), _url(std::move(url)) {
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
    _read_buffer = std::make_unique<char[]>(READ_BUFFER_SIZE);
}

HttpFileReader::~HttpFileReader() {
    static_cast<void>(close());
}

Status HttpFileReader::open(const FileReaderOptions& opts) {
    if (_initialized) {
        return Status::OK();
    }

    auto client = get_client();
    if (!client) {
        return Status::InternalError("Failed to get HttpClient");
    }

    client->set_method(HttpMethod::HEAD);

    uint64_t content_length = 0;

    RETURN_IF_ERROR(client->get_content_length(&content_length));

    _file_size = content_length;

    _initialized = true;
    return Status::OK();
}

Status HttpFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                    const IOContext* /*io_ctx*/) {
    if (!_read_buffer) {
        _read_buffer = std::make_unique<char[]>(READ_BUFFER_SIZE);
    }

    size_t to_read = result.size;
    size_t buffer_offset = 0;

    if (_size_known && offset >= _file_size) {
        *bytes_read = 0;
        return Status::OK();
    }

    if (offset >= _buffer_start && offset < _buffer_end) {
        size_t buffer_idx = offset - _buffer_start;
        size_t available = _buffer_end - offset;
        size_t copy_len = std::min(available, to_read);
        std::memcpy(result.data, _read_buffer.get() + buffer_idx, copy_len);
        buffer_offset += copy_len;
        to_read -= copy_len;
        offset += copy_len;
    } else {
        _buffer_start = 0;
        _buffer_end = 0;
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

    auto client = get_client();
    RETURN_IF_ERROR(client->init(_url, /*set_fail_on_error=*/false));
    client->set_method(HttpMethod::GET);

    for (const auto& kv : _extend_kv) {
        if (kv.first.rfind("http.header.", 0) == 0) {
            client->set_header(kv.first.substr(strlen("http.header.")), kv.second);
        }
    }

    client->set_header("Expect", "");
    client->set_header("Connection", "close");

    bool with_range = _range_supported;
    if (with_range) client->set_range(offset, req_len);

    std::string buf;
    buf.reserve(req_len);
    auto cb = [&](const void* data, size_t len) {
        buf.append(reinterpret_cast<const char*>(data), len);
        return true;
    };
    RETURN_IF_ERROR(client->execute(cb));

    if (buf.empty()) {
        *bytes_read = buffer_offset;
        return Status::OK();
    }

    bool range_ignored = with_range && (offset > 0) && (buf.size() > req_len / 2);
    if (range_ignored) {
        _range_supported = false;
        _size_known = true;
        _file_size = buf.size();

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
    return Status::OK();
}

Status HttpFileReader::read_at(size_t offset, void* buf, size_t nbytes, size_t* bytes_read) {
    Slice slice((char*)buf, nbytes);
    return read_at_impl(offset, slice, bytes_read, nullptr);
}

std::shared_ptr<HttpClient> HttpFileReader::get_client() {
    static std::shared_ptr<HttpClient> client = std::make_shared<HttpClient>();
    return client;
}

Status HttpFileReader::read_range(size_t offset, size_t length, char* buffer) {
    auto client = get_client();
    client->set_method(HttpMethod::GET);
    client->set_range(offset, length);

    size_t total = 0;
    auto on_data = [&](const void* data, size_t len) -> bool {
        size_t can_copy = std::min(len, length - total);
        memcpy(buffer + total, data, can_copy);
        total += can_copy;
        return total < length;
    };

    RETURN_IF_ERROR(client->execute(on_data));

    if (total == 0) {
        return Status::InternalError("empty http response for url {}", _url);
    }

    LOG(INFO) << "HttpFileReader::read_range read " << total << " bytes from " << _url;
    return Status::OK();
}

} // namespace doris::io