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

#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <bthread/countdown_event.h>

#include <cstddef>
#include <list>
#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "util/s3_util.h"
#include "util/slice.h"

namespace Aws::S3 {
namespace Model {
class CompletedPart;
}
class S3Client;
} // namespace Aws::S3

namespace doris {
namespace io {
struct S3FileBuffer;

class S3FileWriter final : public FileWriter {
public:
    S3FileWriter(Path path, std::shared_ptr<Aws::S3::S3Client> client, const S3Conf& s3_conf,
                 FileSystemSPtr fs);
    ~S3FileWriter() override;

    Status close() override;

    Status abort() override;
    Status appendv(const Slice* data, size_t data_cnt) override;
    Status finalize() override;
    Status write_at(size_t offset, const Slice& data) override {
        return Status::NotSupported("not support");
    }

    [[nodiscard]] int64_t upload_cost_ms() const { return *_upload_cost_ms; }

private:
    void _wait_until_finish(std::string_view task_name);
    Status _complete();
    Status _create_multi_upload_request();
    void _put_object(S3FileBuffer& buf);
    void _upload_one_part(int64_t part_num, S3FileBuffer& buf);

    std::string _bucket;
    std::string _key;
    bool _closed = false;
    bool _aborted = false;

    std::unique_ptr<int64_t> _upload_cost_ms;

    std::shared_ptr<Aws::S3::S3Client> _client;
    std::string _upload_id;

    // Current Part Num for CompletedPart
    int _cur_part_num = 1;
    std::mutex _completed_lock;
    std::vector<std::unique_ptr<Aws::S3::Model::CompletedPart>> _completed_parts;

    // **Attention** call add_count() before submitting buf to async thread pool
    bthread::CountdownEvent _countdown_event {0};

    std::atomic_bool _failed = false;
    Status _st = Status::OK();
    size_t _bytes_written = 0;

    std::shared_ptr<S3FileBuffer> _pending_buf = nullptr;
};

} // namespace io
} // namespace doris
