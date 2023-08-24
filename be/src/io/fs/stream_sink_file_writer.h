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

struct RowsetId;
struct SegmentStatistics;

namespace io {
class StreamSinkFileWriter : public FileWriter {
public:
    StreamSinkFileWriter(int sender_id, std::vector<brpc::StreamId> stream_id)
            : _sender_id(sender_id), _streams(stream_id) {}

    static void deleter(void* data) { ::free(data); }

    static Status send_with_retry(brpc::StreamId stream, butil::IOBuf buf);

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
    Status _stream_sender(butil::IOBuf buf) const {
        for (auto stream : _streams) {
            LOG(INFO) << "writer flushing, load_id: " << UniqueId(_load_id).to_string()
                      << ", index_id: " << _index_id << ", tablet_id: " << _tablet_id
                      << ", segment_id: " << _segment_id << ", stream id: " << stream
                      << ", buf size: " << buf.size();

            RETURN_IF_ERROR(send_with_retry(stream, buf));
        }
        return Status::OK();
    }

    void _append_header();

private:
    PStreamHeader _header;
    butil::IOBuf _buf;

    std::queue<Slice> _pending_slices;
    size_t _max_pending_bytes = config::brpc_streaming_client_batch_bytes;
    size_t _pending_bytes;

    int _sender_id;
    std::vector<brpc::StreamId> _streams;

    PUniqueId _load_id;
    int64_t _partition_id;
    int64_t _index_id;
    int64_t _tablet_id;
    int32_t _segment_id;
};

} // namespace io
} // namespace doris
