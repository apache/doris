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

#include <queue>

#include "io/fs/file_writer.h"
#include "util/uid_util.h"

namespace doris {

class LoadStreamStub;

struct RowsetId;
struct SegmentStatistics;

namespace io {
class StreamSinkFileWriter : public FileWriter {
public:
    StreamSinkFileWriter(std::vector<std::shared_ptr<LoadStreamStub>>& streams)
            : _streams(streams) {}

    void init(PUniqueId load_id, int64_t partition_id, int64_t index_id, int64_t tablet_id,
              int32_t segment_id);

    Status appendv(const Slice* data, size_t data_cnt) override;

    Status finalize() override;

    Status close() override;

    Status abort() override {
        return Status::NotSupported("StreamSinkFileWriter::abort() is not supported");
    }

    Status write_at(size_t offset, const Slice& data) override {
        return Status::NotSupported("StreamSinkFileWriter::write_at() is not supported");
    }

private:
    template <bool eos>
    Status _flush();

    std::vector<std::shared_ptr<LoadStreamStub>> _streams;

    PUniqueId _load_id;
    int64_t _partition_id;
    int64_t _index_id;
    int64_t _tablet_id;
    int32_t _segment_id;
};

} // namespace io
} // namespace doris
