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

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "common/status.h"
#include "http/http_client.h"
#include "io/fs/file_reader.h"
#include "io/fs/http_client_cache.h"

namespace doris::io {
typedef struct OpenFileInfo {
    Path path;
    std::map<std::string, std::string> extend_info;
} OpenFileInfo;

class HttpFileReader final : public FileReader {
public:
    explicit HttpFileReader(const OpenFileInfo& fileInfo, std::string url);
    ~HttpFileReader() override;

    Status open(const FileReaderOptions& opts);
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

    Status read_at(size_t offset, void* buf, size_t nbytes, size_t* bytes_read);

    Status close() override;
    const Path& path() const override { return _path; }
    bool closed() const override { return _closed.load(std::memory_order_acquire); }
    size_t size() const override { return _file_size; }

    std::unique_ptr<HttpClient> getClient();
    std::unique_ptr<HttpClient> createClient();
    void storeClient(std::unique_ptr<HttpClient> client);
    Status read_range(size_t offset, size_t length, char* buffer);

private:
    HttpClientCache client_cache;
    std::unique_ptr<char[]> _read_buffer;
    static constexpr size_t READ_BUFFER_SIZE = 4096;
    size_t _buffer_start = 0;
    size_t _buffer_end = 0;
    size_t _buffer_pos = 0;
    size_t _file_offset = 0;
    size_t _buffer_available = 0;
    std::string _etag;
    time_t _last_modified = 0;
    bool _initialized = false;
    // int64_t _pos  = 0;
    size_t _file_size = -1;
    Path _path;
    std::string _url;
    std::atomic<bool> _closed = false;
};

} // namespace doris::io
