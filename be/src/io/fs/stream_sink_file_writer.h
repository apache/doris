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

#include <brpc/stream.h>
#include <gen_cpp/internal_service.pb.h>

#include "io/fs/file_writer.h"

namespace doris {
namespace io {
class StreamSinkFileWriter : public FileWriter {
public:
    StreamSinkFileWriter(brpc::StreamId stream);
    ~StreamSinkFileWriter() override;

    void init(Path path, int64_t load_id, int64_t index_id, int64_t tablet_id, int64_t rowset_id,
              int64_t segment_id);

    Status appendv(const Slice* data, size_t data_cnt) override;

    Status finalize() override;

private:
    PStreamHeader _stream_header_builder();

    Status _stream_sender(butil::IOBuf buf);

    brpc::StreamId _stream;

    int64_t _data = 1;
    int64_t _close = 2;

    std::string _path;
    int64_t _load_id;
    int64_t _index_id;
    int64_t _tablet_id;
    int64_t _rowset_id;
    int64_t _segment_id;
};
} // namespace io
} // namespace doris
