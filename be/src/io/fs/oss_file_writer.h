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

#include <alibabacloud/oss/model/Part.h>
#include <bthread/countdown_event.h>

#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "io/fs/oss_file_system.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_bufferpool.h"

namespace doris::io {

struct AsyncCloseStatusPack {
    std::promise<Status> promise;
    std::shared_future<Status> future;
};

class OSSFileWriter final : public FileWriter {
public:
    OSSFileWriter(std::shared_ptr<OSSClientHolder> client, std::string bucket, std::string key,
                  const FileWriterOptions* opts);
    ~OSSFileWriter() override;

    Status appendv(const Slice* data, size_t data_cnt) override;

    const Path& path() const override { return _path; }
    size_t bytes_appended() const override { return _bytes_appended; }
    State state() const override { return _state; }

    Status close(bool non_block = false) override;

private:
    Status _close_impl();
    Status _create_multipart_upload();

    Status _upload_part(int part_num, const char* data, size_t size);

    void _upload_one_part(int64_t part_num, UploadFileBuffer& buf);

    Status _complete_multipart_upload();
    Status _abort_multipart_upload();
    Status _put_object(UploadFileBuffer& buf);

    Status _build_upload_buffer();

    bool _complete_part_task_callback(Status s);

    Status _set_upload_to_remote_less_than_buffer_size();

    void _wait_until_finish(std::string_view task_name);

    Path _path;
    std::string _bucket;
    std::string _key;
    std::shared_ptr<OSSClientHolder> _client;

    std::string _upload_id;
    int64_t _cur_part_num = 1;
    std::vector<AlibabaCloud::OSS::Part> _completed_parts;
    std::mutex _completed_lock;

    std::shared_ptr<FileBuffer> _pending_buf;
    size_t _buffer_size;
    size_t _bytes_appended = 0;

    bthread::CountdownEvent _countdown_event {0};

    std::unique_ptr<AsyncCloseStatusPack> _async_close_pack;

    std::mutex _close_lock;

    // OSS committer mode: FE completes multipart upload
    bool _used_by_oss_committer;

    State _state {State::OPENED};
    std::atomic_bool _failed {false};
    Status _st;
};

} // namespace doris::io
