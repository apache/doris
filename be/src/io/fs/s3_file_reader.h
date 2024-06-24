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
#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_system.h"
#include "util/slice.h"

namespace doris {
class RuntimeProfile;

namespace io {
struct IOContext;

class S3FileReader final : public FileReader {
public:
    static Result<FileReaderSPtr> create(std::shared_ptr<const ObjClientHolder> client,
                                         std::string bucket, std::string key, int64_t file_size,
                                         RuntimeProfile* profile);

    S3FileReader(std::shared_ptr<const ObjClientHolder> client, std::string bucket, std::string key,
                 size_t file_size, RuntimeProfile* profile);

    ~S3FileReader() override;

    Status close() override;

    const Path& path() const override { return _path; }

    size_t size() const override { return _file_size; }

    bool closed() const override { return _closed.load(std::memory_order_acquire); }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

    void _collect_profile_before_close() override;

private:
    struct S3Statistics {
        int64_t total_get_request_counter = 0;
        int64_t too_many_request_err_counter = 0;
        int64_t too_many_request_sleep_time_ms = 0;
        int64_t total_bytes_read = 0;
    };
    Path _path;
    size_t _file_size;

    std::string _bucket;
    std::string _key;
    std::shared_ptr<const ObjClientHolder> _client;

    std::atomic<bool> _closed = false;

    RuntimeProfile* _profile = nullptr;
    S3Statistics _s3_stats;
};

} // namespace io
} // namespace doris
