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

HttpFileReader::HttpFileReader(const OpenFileInfo& openFileInfo, std::string url)
        : _path(openFileInfo.path), _url(std::move(url)) {
    auto etag_iter = openFileInfo.extend_info.find("etag");
    if (etag_iter != openFileInfo.extend_info.end()) {
        _etag = etag_iter->second;
    }

    auto lm_iter = openFileInfo.extend_info.find("last_modified");
    if (lm_iter != openFileInfo.extend_info.end()) {
        _last_modified = std::stoll(lm_iter->second);
    }

    auto size_iter = openFileInfo.extend_info.find("file_size");
    if (size_iter != openFileInfo.extend_info.end()) {
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

    auto client = getClient();
    if (!client) {
        return Status::InternalError("Failed to create HttpClient");
    }

    uint64_t content_length = 0;

    RETURN_IF_ERROR(client->get_content_length(&content_length));

    _file_size = content_length;

    storeClient(std::move(client));
    _initialized = true;
    return Status::OK();
}

Status HttpFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                    const IOContext* io_ctx) {
    if (!_read_buffer) {
        _read_buffer = std::make_unique<char[]>(READ_BUFFER_SIZE);
    }

    size_t to_read = result.size;
    size_t buffer_offset = 0;
    auto client = getClient();
    if (offset >= _buffer_start && offset < _buffer_end) {
        size_t buffer_idx = offset - _buffer_start;
        size_t available = _buffer_end - offset;
        size_t copy_len = std::min(available, to_read);
        memcpy(result.data, _read_buffer.get() + buffer_idx, copy_len);
        buffer_offset += copy_len;
        to_read -= copy_len;
        offset += copy_len;
    } else {
        _buffer_start = 0;
        _buffer_end = 0;
    }

    if (to_read > 0) {
        if (to_read > READ_BUFFER_SIZE) {
            client->set_range(offset, to_read);
            std::string buffer;
            auto callback = [&](const void* data, size_t len) {
                buffer.append(reinterpret_cast<const char*>(data), len);
                return true;
            };
            RETURN_IF_ERROR(client->execute(callback));

            if (buffer.size() > to_read) {
                return Status::InternalError("HTTP response larger than requested buffer");
            }
            memcpy(result.data + buffer_offset, buffer.data(), buffer.size());
            buffer_offset += buffer.size();
        } else {
            // 否则先读入缓存区
            client->set_range(offset, READ_BUFFER_SIZE);
            std::string buffer;
            auto callback = [&](const void* data, size_t len) {
                buffer.append(reinterpret_cast<const char*>(data), len);
                return true;
            };
            RETURN_IF_ERROR(client->execute(callback));

            if (buffer.size() > READ_BUFFER_SIZE) {
                return Status::InternalError("HTTP response larger than buffer");
            }

            memcpy(_read_buffer.get(), buffer.data(), buffer.size());
            _buffer_start = offset;
            _buffer_end = offset + buffer.size();

            // 把用户需要的部分复制过去
            size_t copy_len = std::min(to_read, buffer.size());
            memcpy(result.data + buffer_offset, _read_buffer.get(), copy_len);
            buffer_offset += copy_len;
        }
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

void HttpFileReader::storeClient(std::shared_ptr<HttpClient> client) {
    client_cache.StoreClient(client);
}

std::shared_ptr<HttpClient> HttpFileReader::getClient() {
    auto cached_client = client_cache.GetClient();
    if (cached_client) {
        return cached_client;
    }
    return createClient();
}

std::shared_ptr<HttpClient> HttpFileReader::createClient() {
    auto client = std::make_shared<HttpClient>();
    auto st = client->init(_url);
    if (!st.ok()) {
        return nullptr;
    }
    storeClient(client);
    return client;
}

Status HttpFileReader::read_at(size_t offset, void* buf, size_t nbytes, size_t* bytes_read) {
    Slice slice((char*)buf, nbytes);
    return read_at_impl(offset, slice, bytes_read, nullptr);
}

Status HttpFileReader::read_range(size_t offset, size_t length, char* buffer) {
    auto client = getClient();
    return client->read_range(_url, offset, length, buffer);
}

} // namespace doris::io