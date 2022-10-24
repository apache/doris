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

#include <cstddef>
#include <list>

#include "io/fs/file_writer.h"
#include "util/s3_util.h"

namespace Aws::S3 {
namespace Model {
class CompletedPart;
}
class S3Client;
} // namespace Aws::S3

namespace doris {
namespace io {

class S3FileWriter final : public FileWriter {
public:
    S3FileWriter(Path path, std::shared_ptr<Aws::S3::S3Client> client,
                 const S3Conf& s3_conf);
    ~S3FileWriter() override;

    Status close() override;

    Status abort() override;

    Status append(const Slice& data) override;

    Status appendv(const Slice* data, size_t data_cnt) override;

    Status write_at(size_t offset, const Slice& data) override;

    Status finalize() override;

    size_t bytes_appended() const override { return _bytes_appended; }

private:
    Status _close(bool sync);

    Status _open();

    Status _upload_part(const std::string& str);

private:

    // max size of each part when uploading: 5MB
    static const int MAX_SIZE_EACH_PART = 5 * 1024 * 1024;

    std::shared_ptr<Aws::S3::S3Client> _client;
    S3Conf _s3_conf;
    std::string _upload_id;
    bool _is_open = false;
    bool _closed = false;
    size_t _bytes_appended = 0;

    // _cur_data is used to collect data in function appendv(const Slice* data, size_t data_cnt),
    // a _cur_data may contain data in several appendv() until _cur_data is full.
    // Then_cur_data_offset will be set when data is appended to _cur_data,
    // next data will be memcpy to _cur_data_offset position.
    // _appended_data_offset will be set when data in one appendv() is split, which is
    // used in next part to be uploaded.
    int8_t _cur_data[MAX_SIZE_EACH_PART];
    int _cur_data_offset = 0;
    int _appended_data_offset = 0;
    // Current Part Num for CompletedPart
    int _cur_part_num = 0;
    std::list<std::shared_ptr<Aws::S3::Model::CompletedPart>> _completed_parts;
};

} // namespace io
} // namespace doris
