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
struct S3Conf;

namespace io {
struct S3FileBuffer;
class S3FileSystem;

class S3FileWriter final : public FileWriter {
public:
    S3FileWriter(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket, std::string key,
                 const FileWriterOptions* opts);
    ~S3FileWriter() override;

    Status close() override;

    Status appendv(const Slice* data, size_t data_cnt) override;
    Status finalize() override;

    const Path& path() const override { return _path; }
    size_t bytes_appended() const override { return _bytes_appended; }
    bool closed() const override { return _closed; }

    FileCacheAllocatorBuilder* cache_builder() const override {
        return _cache_builder == nullptr ? nullptr : _cache_builder.get();
    }

    const std::vector<std::unique_ptr<Aws::S3::Model::CompletedPart>>& completed_parts() const {
        return _completed_parts;
    }

    const std::string& key() const { return _key; }
    const std::string& bucket() const { return _bucket; }
    const std::string& upload_id() const { return _upload_id; }

private:
    Status _abort();
    [[nodiscard]] std::string _dump_completed_part() const;
    void _wait_until_finish(std::string_view task_name);
    Status _complete();
    Status _create_multi_upload_request();
    Status _set_upload_to_remote_less_than_buffer_size();
    void _put_object(UploadFileBuffer& buf);
    void _upload_one_part(int64_t part_num, UploadFileBuffer& buf);

    Path _path;
    std::string _bucket;
    std::string _key;

    std::shared_ptr<Aws::S3::S3Client> _client;
    std::string _upload_id;

    // Current Part Num for CompletedPart
    int _cur_part_num = 1;
    std::mutex _completed_lock;
    std::vector<std::unique_ptr<Aws::S3::Model::CompletedPart>> _completed_parts;

    // **Attention** call add_count() before submitting buf to async thread pool
    bthread::CountdownEvent _countdown_event {0};

    std::atomic_bool _failed = false;

    bool _closed = false;
    Status _st;
    size_t _bytes_appended = 0;

    std::shared_ptr<FileBuffer> _pending_buf;
    std::unique_ptr<FileCacheAllocatorBuilder>
            _cache_builder; // nullptr if disable write file cache

    // S3 committer will start multipart uploading all files on BE side,
    // and then complete multipart upload these files on FE side.
    // If you do not complete multi parts of a file, the file will not be visible.
    // So in this way, the atomicity of a single file can be guaranteed. But it still cannot
    // guarantee the atomicity of multiple files.
    // Because hive committers have best-effort semantics,
    // this shortens the inconsistent time window.
    bool _used_by_s3_committer;
};

} // namespace io
} // namespace doris
