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

#include <bthread/countdown_event.h>

#include <cstddef>
#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_bufferpool.h"

namespace Aws::S3 {
namespace Model {
class CompletedPart;
}
class S3Client;
} // namespace Aws::S3

namespace doris {
namespace io {
struct S3FileBuffer;
class S3FileSystem;

class S3FileWriter final : public FileWriter {
public:
    S3FileWriter(std::string key, std::shared_ptr<S3FileSystem> fs, const FileWriterOptions* opts);
    ~S3FileWriter() override;

    Status close() override;

    Status abort() override;
    Status appendv(const Slice* data, size_t data_cnt) override;
    Status finalize() override;
    Status write_at(size_t offset, const Slice& data) override {
        return Status::NotSupported("not support");
    }

private:
    void _wait_until_finish(std::string_view task_name);
    Status _complete();
    Status _create_multi_upload_request();
    void _put_object(UploadFileBuffer& buf);
    void _upload_one_part(int64_t part_num, UploadFileBuffer& buf);

    std::string _bucket;
    std::string _key;
    bool _closed = false;
    bool _aborted = false;

    std::shared_ptr<Aws::S3::S3Client> _client;
    std::string _upload_id;
    size_t _index_offset {0};

    // Current Part Num for CompletedPart
    int _cur_part_num = 1;
    std::mutex _completed_lock;
    std::vector<std::unique_ptr<Aws::S3::Model::CompletedPart>> _completed_parts;

    IFileCache::Key _cache_key;
    IFileCache* _cache;
    // **Attention** call add_count() before submitting buf to async thread pool
    bthread::CountdownEvent _countdown_event {0};

    std::atomic_bool _failed = false;
    Status _st;
    size_t _bytes_written = 0;

    std::shared_ptr<FileBuffer> _pending_buf;
    int64_t _expiration_time;
    bool _is_cold_data;
    bool _write_file_cache;
};

} // namespace io
} // namespace doris
