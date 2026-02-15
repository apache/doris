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

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "io/fs/oss_file_system.h"
#include "io/fs/path.h"

namespace doris::io {

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
    Status _complete_multipart_upload();
    Status _abort_multipart_upload();
    Status _put_object(const char* data, size_t size);
    Status _flush_buffer();

    Path _path;
    std::string _bucket;
    std::string _key;
    std::shared_ptr<OSSClientHolder> _client;

    std::string _upload_id;
    int _cur_part_num = 1;
    std::vector<AlibabaCloud::OSS::Part> _completed_parts;
    std::mutex _completed_lock;

    std::vector<char> _pending_buf;
    size_t _buffer_size;
    size_t _bytes_appended = 0;

    State _state {State::OPENED};
    std::atomic_bool _failed {false};
    Status _st;
};

} // namespace doris::io
