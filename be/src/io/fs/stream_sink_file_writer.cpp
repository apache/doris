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

#include "io/fs/stream_sink_file_writer.h"

#include <gen_cpp/internal_service.pb.h>

#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "util/debug_points.h"
#include "util/uid_util.h"
#include "vec/sink/load_stream_stub.h"

namespace doris {
namespace io {

void StreamSinkFileWriter::init(PUniqueId load_id, int64_t partition_id, int64_t index_id,
                                int64_t tablet_id, int32_t segment_id) {
    VLOG_DEBUG << "init stream writer, load id(" << UniqueId(load_id).to_string()
               << "), partition id(" << partition_id << "), index id(" << index_id
               << "), tablet_id(" << tablet_id << "), segment_id(" << segment_id << ")";
    _load_id = load_id;
    _partition_id = partition_id;
    _index_id = index_id;
    _tablet_id = tablet_id;
    _segment_id = segment_id;
}

Status StreamSinkFileWriter::appendv(const Slice* data, size_t data_cnt) {
    size_t bytes_req = 0;
    for (int i = 0; i < data_cnt; i++) {
        bytes_req += data[i].get_size();
    }

    VLOG_DEBUG << "writer appendv, load_id: " << print_id(_load_id) << ", index_id: " << _index_id
               << ", tablet_id: " << _tablet_id << ", segment_id: " << _segment_id
               << ", data_length: " << bytes_req;

    std::span<const Slice> slices {data, data_cnt};
    for (auto& stream : _streams) {
        RETURN_IF_ERROR(stream->append_data(_partition_id, _index_id, _tablet_id, _segment_id,
                                            _bytes_appended, slices));
    }
    _bytes_appended += bytes_req;
    return Status::OK();
}

Status StreamSinkFileWriter::finalize() {
    VLOG_DEBUG << "writer finalize, load_id: " << print_id(_load_id) << ", index_id: " << _index_id
               << ", tablet_id: " << _tablet_id << ", segment_id: " << _segment_id;
    // TODO(zhengyu): update get_inverted_index_file_size into stat
    for (auto& stream : _streams) {
        RETURN_IF_ERROR(stream->append_data(_partition_id, _index_id, _tablet_id, _segment_id,
                                            _bytes_appended, {}, true));
    }
    return Status::OK();
}

Status StreamSinkFileWriter::close() {
    return Status::OK();
}

} // namespace io
} // namespace doris
