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

    _header.set_src_id(_sender_id);
    *_header.mutable_load_id() = _load_id;
    _header.set_partition_id(_partition_id);
    _header.set_index_id(_index_id);
    _header.set_tablet_id(_tablet_id);
    _header.set_segment_id(_segment_id);
    _header.set_opcode(doris::PStreamHeader::APPEND_DATA);
    _append_header();
}

Status StreamSinkFileWriter::appendv(const Slice* data, size_t data_cnt) {
    size_t bytes_req = 0;
    for (int i = 0; i < data_cnt; i++) {
        bytes_req += data[i].get_size();
        _buf.append(data[i].get_data(), data[i].get_size());
    }
    _pending_bytes += bytes_req;
    _bytes_appended += bytes_req;

    VLOG_DEBUG << "writer appendv, load_id: " << UniqueId(_load_id).to_string()
               << ", index_id: " << _index_id << ", tablet_id: " << _tablet_id
               << ", segment_id: " << _segment_id << ", data_length: " << bytes_req
               << ", current batched bytes: " << _pending_bytes;

    if (_pending_bytes >= _max_pending_bytes) {
        RETURN_IF_ERROR(_stream_sender(_buf));
        _buf.clear();
        _append_header();
        _pending_bytes = 0;
    }

    return Status::OK();
}

Status StreamSinkFileWriter::finalize() {
    VLOG_DEBUG << "writer finalize, load_id: " << UniqueId(_load_id).to_string()
               << ", index_id: " << _index_id << ", tablet_id: " << _tablet_id
               << ", segment_id: " << _segment_id;
    // TODO(zhengyu): update get_inverted_index_file_size into stat
    Status status = _stream_sender(_buf);
    // send eos
    _buf.clear();
    _header.set_segment_eos(true);
    _append_header();
    status = _stream_sender(_buf);
    return status;
}

void StreamSinkFileWriter::_append_header() {
    size_t header_len = _header.ByteSizeLong();
    _buf.append(reinterpret_cast<uint8_t*>(&header_len), sizeof(header_len));
    _buf.append(_header.SerializeAsString());
}

Status StreamSinkFileWriter::send_with_retry(brpc::StreamId stream, butil::IOBuf buf) {
    while (true) {
        int ret = brpc::StreamWrite(stream, buf);
        if (ret == EAGAIN) {
            const timespec time = butil::seconds_from_now(60);
            int wait_result = brpc::StreamWait(stream, &time);
            if (wait_result == 0) {
                continue;
            } else {
                return Status::InternalError("fail to send data when wait stream");
            }
        } else if (ret == EINVAL) {
            return Status::InternalError("fail to send data when stream write");
        } else {
            return Status::OK();
        }
    }
}

Status StreamSinkFileWriter::close() {
    return Status::OK();
}

} // namespace io
} // namespace doris
