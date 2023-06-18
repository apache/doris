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

namespace doris {
namespace io {

StreamSinkFileWriter::StreamSinkFileWriter(brpc::StreamId stream_id) : _stream(stream_id) {}

StreamSinkFileWriter::~StreamSinkFileWriter() {}

Status StreamSinkFileWriter::init(Path path, PUniqueId load_id, int64_t index_id, int64_t tablet_id,
                                  int64_t rowset_id, int32_t segment_id, bool is_last_segment) {
    _path = path;
    _load_id = load_id;
    _index_id = index_id;
    _tablet_id = tablet_id;
    _rowset_id = rowset_id;
    _segment_id = segment_id;
    _is_last_segment = is_last_segment;

    butil::IOBuf buf;
    PStreamHeader header;
    header.set_opcode(PStreamHeader::OPEN_FILE);
    buf.append(path);
    return _stream_sender(buf);
}

Status StreamSinkFileWriter::appendv(const Slice* data, size_t data_cnt) {
    butil::IOBuf buf;
    PStreamHeader header;
    header.set_allocated_load_id(&_load_id);
    header.set_index_id(_index_id);
    header.set_tablet_id(_tablet_id);
    header.set_rowset_id(_rowset_id);
    header.set_segment_id(_segment_id);
    header.set_is_last_segment(_is_last_segment);
    header.set_opcode(header.APPEND_DATA);

    buf.append(header.SerializeAsString());
    buf.append_user_data((void*)data->get_data(), data_cnt, deleter);
    Status status = _stream_sender(buf);
    header.release_load_id();
    return status;
}

Status StreamSinkFileWriter::finalize() {
    butil::IOBuf buf;
    PStreamHeader header;
    header.set_allocated_load_id(&_load_id);
    header.set_index_id(_index_id);
    header.set_tablet_id(_tablet_id);
    header.set_rowset_id(_rowset_id);
    header.set_segment_id(_segment_id);
    header.set_is_last_segment(_is_last_segment);
    header.set_opcode(header.CLOSE_FILE);

    buf.append(header.SerializeAsString());
    Status status = _stream_sender(buf);
    header.release_load_id();
    return status;
}

Status StreamSinkFileWriter::_stream_sender(butil::IOBuf buf) {
    while (true) {
        int ret = brpc::StreamWrite(_stream, buf);
        if (ret == EAGAIN) {
            const timespec time = butil::seconds_from_now(60);
            int wait_result = brpc::StreamWait(_stream, &time);
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

Status StreamSinkFileWriter::abort() {
    return Status::OK();
}

Status StreamSinkFileWriter::close() {
    return Status::OK();
}

Status StreamSinkFileWriter::write_at(size_t offset, const Slice& data) {
    return Status::OK();
}
} // namespace io
} // namespace doris