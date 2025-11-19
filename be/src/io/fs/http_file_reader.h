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
#include "io/fs/file_handle_cache.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace doris::io {
typedef struct OpenFileInfo {
    Path path;
    std::map<std::string, std::string> extend_info;
} OpenFileInfo;
class HttpFileReader final : public FileReader {
public:
    static Result<FileReaderSPtr> create(const std::string& url,
                                         const std::map<std::string, std::string>& props,
                                         const FileReaderOptions& opts, RuntimeProfile* profile);

    explicit HttpFileReader(const OpenFileInfo& fileInfo, std::string url);
    ~HttpFileReader() override;

    Status open(const FileReaderOptions& opts);
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx = nullptr) override;
    Status close() override;
    const Path& path() const override { return _path; }
    bool closed() const override { return _closed.load(std::memory_order_acquire); }
    size_t size() const override { return _file_size; }

private:
    // Prepare and initialize the HTTP client for a new request
    Status prepare_client(bool set_fail_on_error = true);

    // Detect if the HTTP server supports Range requests
    // Returns OK on success with _range_supported set appropriately
    Status detect_range_support();

    std::unique_ptr<char[]> _read_buffer;
    static constexpr size_t READ_BUFFER_SIZE = 1 << 20; // 1MB
    // Default maximum file size for servers that don't support Range requests
    static constexpr size_t DEFAULT_MAX_REQUEST_SIZE = 100 << 20; // 100MB

    size_t _buffer_start = 0;
    size_t _buffer_end = 0;
    bool _size_known = false;
    bool _range_supported = true;
    std::string _etag;
    bool _initialized = false;
    std::map<std::string, std::string> _extend_kv;
    size_t _file_size = static_cast<size_t>(-1);
    Path _path;
    std::string _url;
    int64_t _last_modified = 0;
    std::atomic<bool> _closed = false;
    std::unique_ptr<HttpClient> _client;

    // Configuration for non-Range request handling
    bool _enable_range_request = true;                         // Whether Range request is required
    size_t _max_request_size_bytes = DEFAULT_MAX_REQUEST_SIZE; // Max size for non-Range downloads
};

} // namespace doris::io
