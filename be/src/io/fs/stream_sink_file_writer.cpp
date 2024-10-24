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

namespace doris::io {

void StreamSinkFileWriter::init(PUniqueId load_id, int64_t partition_id, int64_t index_id,
                                int64_t tablet_id, int32_t segment_id, FileType file_type) {
    VLOG_DEBUG << "init stream writer, load id(" << UniqueId(load_id).to_string()
               << "), partition id(" << partition_id << "), index id(" << index_id
               << "), tablet_id(" << tablet_id << "), segment_id(" << segment_id << ")"
               << ", file_type(" << file_type << ")";
    _load_id = load_id;
    _partition_id = partition_id;
    _index_id = index_id;
    _tablet_id = tablet_id;
    _segment_id = segment_id;
    _file_type = file_type;
}

Status StreamSinkFileWriter::appendv(const Slice* data, size_t data_cnt) {
    size_t bytes_req = 0;
    for (int i = 0; i < data_cnt; i++) {
        bytes_req += data[i].get_size();
    }

    VLOG_DEBUG << "writer appendv, load_id: " << print_id(_load_id) << ", index_id: " << _index_id
               << ", tablet_id: " << _tablet_id << ", segment_id: " << _segment_id
               << ", data_length: " << bytes_req << "file_type" << _file_type;

    std::span<const Slice> slices {data, data_cnt};
    size_t fault_injection_skipped_streams = 0;
    bool ok = false;
    Status st;
    for (auto& stream : _streams) {
        DBUG_EXECUTE_IF("StreamSinkFileWriter.appendv.write_segment_failed_one_replica", {
            if (fault_injection_skipped_streams < 1) {
                fault_injection_skipped_streams++;
                continue;
            }
        });
        DBUG_EXECUTE_IF("StreamSinkFileWriter.appendv.write_segment_failed_two_replica", {
            if (fault_injection_skipped_streams < 2) {
                fault_injection_skipped_streams++;
                continue;
            }
        });
        DBUG_EXECUTE_IF("StreamSinkFileWriter.appendv.write_segment_failed_all_replica",
                        { continue; });
        st = stream->append_data(_partition_id, _index_id, _tablet_id, _segment_id, _bytes_appended,
                                 slices, false, _file_type);
        ok = ok || st.ok();
        if (!st.ok()) {
            LOG(WARNING) << "failed to send segment data to backend " << stream->dst_id()
                         << ", load_id: " << print_id(_load_id) << ", index_id: " << _index_id
                         << ", tablet_id: " << _tablet_id << ", segment_id: " << _segment_id
                         << ", data_length: " << bytes_req << ", reason: " << st;
        }
    }
    if (!ok) {
        std::stringstream ss;
        for (auto& stream : _streams) {
            ss << " " << stream->dst_id();
        }
        LOG(WARNING) << "failed to send segment data to any replicas, load_id: "
                     << print_id(_load_id) << ", index_id: " << _index_id
                     << ", tablet_id: " << _tablet_id << ", segment_id: " << _segment_id
                     << ", data_length: " << bytes_req << ", backends:" << ss.str();
        return Status::InternalError(
                "failed to send segment data to any replicas, tablet_id={}, segment_id={}",
                _tablet_id, _segment_id);
    }
    _bytes_appended += bytes_req;
    return Status::OK();
}

Status StreamSinkFileWriter::close(bool non_block) {
    if (_state == State::CLOSED) {
        return Status::InternalError("StreamSinkFileWriter already closed, load id {}",
                                     print_id(_load_id));
    }
    if (_state == State::ASYNC_CLOSING) {
        if (non_block) {
            return Status::InternalError("Don't submit async close multi times");
        }
        // Actucally the first time call to close(true) would return the value of _finalize, if it returned one
        // error status then the code would never call the second close(true)
        _state = State::CLOSED;
        return Status::OK();
    }
    if (non_block) {
        _state = State::ASYNC_CLOSING;
    } else {
        _state = State::CLOSED;
    }
    return _finalize();
}

Status StreamSinkFileWriter::_finalize() {
    VLOG_DEBUG << "writer finalize, load_id: " << print_id(_load_id) << ", index_id: " << _index_id
               << ", tablet_id: " << _tablet_id << ", segment_id: " << _segment_id;
    // TODO(zhengyu): update get_inverted_index_file_size into stat
    size_t fault_injection_skipped_streams = 0;
    bool ok = false;
    for (auto& stream : _streams) {
        DBUG_EXECUTE_IF("StreamSinkFileWriter.appendv.write_segment_failed_one_replica", {
            if (fault_injection_skipped_streams < 1) {
                fault_injection_skipped_streams++;
                continue;
            }
        });
        DBUG_EXECUTE_IF("StreamSinkFileWriter.appendv.write_segment_failed_two_replica", {
            if (fault_injection_skipped_streams < 2) {
                fault_injection_skipped_streams++;
                continue;
            }
        });
        DBUG_EXECUTE_IF("StreamSinkFileWriter.appendv.write_segment_failed_all_replica",
                        { continue; });
        auto st = stream->append_data(_partition_id, _index_id, _tablet_id, _segment_id,
                                      _bytes_appended, {}, true, _file_type);
        ok = ok || st.ok();
        if (!st.ok()) {
            LOG(WARNING) << "failed to send segment eos to backend " << stream->dst_id()
                         << ", load_id: " << print_id(_load_id) << ", index_id: " << _index_id
                         << ", tablet_id: " << _tablet_id << ", segment_id: " << _segment_id
                         << ", reason: " << st;
        }
    }
    DBUG_EXECUTE_IF("StreamSinkFileWriter.finalize.finalize_failed", { ok = false; });
    if (!ok) {
        std::stringstream ss;
        for (auto& stream : _streams) {
            ss << " " << stream->dst_id();
        }
        LOG(WARNING) << "failed to send segment eos to any replicas, load_id: "
                     << print_id(_load_id) << ", index_id: " << _index_id
                     << ", tablet_id: " << _tablet_id << ", segment_id: " << _segment_id
                     << ", backends:" << ss.str();
        return Status::InternalError(
                "failed to send segment eos to any replicas, tablet_id={}, segment_id={}",
                _tablet_id, _segment_id);
    }
    return Status::OK();
}

} // namespace doris::io
