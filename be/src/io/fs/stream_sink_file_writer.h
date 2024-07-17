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
#include <gen_cpp/olap_common.pb.h>

#include <queue>

#include "io/fs/file_writer.h"
#include "util/uid_util.h"

namespace doris {

class LoadStreamStub;

struct RowsetId;
struct SegmentStatistics;

namespace io {
struct FileCacheAllocatorBuilder;
class StreamSinkFileWriter final : public FileWriter {
public:
    StreamSinkFileWriter(std::vector<std::shared_ptr<LoadStreamStub>> streams)
            : _streams(std::move(streams)) {}

    void init(PUniqueId load_id, int64_t partition_id, int64_t index_id, int64_t tablet_id,
              int32_t segment_id, FileType file_type = FileType::SEGMENT_FILE);

    Status appendv(const Slice* data, size_t data_cnt) override;

    size_t bytes_appended() const override { return _bytes_appended; }

    State state() const override { return _state; }

    // FIXME(plat1ko): Maybe it's an inappropriate abstraction?
    const Path& path() const override {
        static Path dummy;
        return dummy;
    }

    FileCacheAllocatorBuilder* cache_builder() const override { return nullptr; }

    Status close(bool non_block = false) override;

private:
    Status _finalize();
    std::vector<std::shared_ptr<LoadStreamStub>> _streams;

    PUniqueId _load_id;
    int64_t _partition_id;
    int64_t _index_id;
    int64_t _tablet_id;
    int32_t _segment_id;
    size_t _bytes_appended = 0;
    State _state {State::OPENED};
    FileType _file_type {FileType::SEGMENT_FILE};
};

} // namespace io
} // namespace doris
