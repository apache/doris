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

#include "sink_stream_mgr.h"
#include "util/uid_util.h"
#include "common/config.h"
#include <gen_cpp/internal_service.pb.h>
#include <runtime/exec_env.h>

namespace doris {

bool TargetSegmentComparator::operator()(const TargetSegmentPtr& lhs, const TargetSegmentPtr& rhs) const {
    if (lhs->target_rowset != rhs->target_rowset) {
        return false;
    }
    if (lhs->segmentid != rhs->segmentid) {
        return false;
    }
    return true;
}

bool TargetRowsetComparator::operator()(const TargetRowsetPtr& lhs, const TargetRowsetPtr& rhs) const {
    if (lhs->loadid.hi != rhs->loadid.hi || lhs->loadid.lo != rhs->loadid.lo) {
        return false;
    }
    if (lhs->indexid != rhs->indexid) {
        return false;
    }
    if (lhs->tabletid != rhs->tabletid) {
        return false;
    }
    return true;
}


SinkStreamHandler::SinkStreamHandler() {
    ThreadPoolBuilder("SinkStreamHandler")
            .set_min_threads(20) // TODO
            .set_max_threads(20) // TODO
            .build(&_workers);
}

SinkStreamHandler::~SinkStreamHandler() {
    if (_workers) {
        _workers->shutdown();
    }
}

Status SinkStreamHandler::_create_and_open_file(TargetSegmentPtr target_segment, std::string path) {
    std::shared_ptr<std::ofstream> file = std::make_shared<std::ofstream>();
    file->open(path.c_str(), std::ios::out | std::ios::app);
    {
        std::lock_guard<std::mutex> l(_file_map_lock);
        _file_map[target_segment] = file; // TODO: better not so global
    }
    return Status::OK();
}

Status SinkStreamHandler::_append_data(TargetSegmentPtr target_segment, std::shared_ptr<butil::IOBuf> message) {
    // TODO: wait if file is opening
    // TODO: data reorder
    // TODO: single file binding to single thread?
    // give me a iterator of _file_map
    auto itr = _file_map.end();
    {
        std::lock_guard<std::mutex> l(_file_map_lock);
        itr = _file_map.find(target_segment);
    }
    if (itr == _file_map.end()) {
        return Status::InternalError("file not found");
    }
    *(itr->second) << message->to_string();
    return Status::OK();
}

Status SinkStreamHandler::_close_file(TargetSegmentPtr target_segment, bool is_last_segment) {
    std::shared_ptr<std::ofstream> file = nullptr;
    {
        std::lock_guard<std::mutex> l(_file_map_lock);
        auto itr = _file_map.find(target_segment);
        if (itr == _file_map.end()) {
            return Status::InternalError("close file error");
        }
        file = itr->second;
        _file_map.erase(itr);
    }
    file->close();
    return Status::OK();
}

void SinkStreamHandler::_report_status(StreamId stream, TargetRowsetPtr target_rowset, int is_success, std::string error_msg) {
    LOG(INFO) << "OOXXOO report status " << is_success << " " << error_msg;
    std::shared_ptr<butil::IOBuf> buf = std::make_shared<butil::IOBuf>();
    PWriteStreamSinkReponse response;
    response.set_success(is_success);
    response.set_error_msg(error_msg);
    response.set_index_id(target_rowset->indexid);
    response.set_tablet_id(target_rowset->tabletid);
    int ret = brpc::StreamWrite(stream, *buf);
    if (ret == EAGAIN) {
        std::cerr << "OOXXOO report status EAGAIN" << std::endl;
    } else if (ret == EINVAL) {
        std::cerr << "OOXXOO report status EINVAL" << std::endl;
    } else {
        std::cerr << "OOXXOO report status " << ret << std::endl;
    }
}

void SinkStreamHandler::_parse_header(butil::IOBuf *const message, int *opcode, UniqueId *loadid, int64_t *indexid,
                  int64_t *tabletid, RowsetId *rowsetid, int64_t *segmentid, bool *is_last_segment) {
    butil::IOBufAsZeroCopyInputStream wrapper(*message);
    PStreamHeader hdr;
    hdr.ParseFromZeroCopyStream(&wrapper);
    *opcode = hdr.opcode();
    *loadid = hdr.load_id();
    *indexid = hdr.index_id();
    *tabletid = hdr.tablet_id();
    // *rowsetid = hdr.rowset_id(); // TODO not needed?
    *segmentid = hdr.segment_id();
    *is_last_segment = hdr.is_last_segment();
    if (hdr.has_is_last_segment()) {
        *is_last_segment = hdr.is_last_segment();
    }
}

uint64_t SinkStreamHandler::get_next_segmentid(TargetRowsetPtr target_rowset, int64_t segmentid, bool is_open) {
    // TODO: need support concurrent flush memtable
    {
        std::lock_guard<std::mutex> l(_tablet_segment_next_id_lock);
        if (_tablet_segment_next_id.find(target_rowset) == _tablet_segment_next_id.end()) {
            _tablet_segment_next_id[target_rowset] = 0;
            return 0;
        }
        if (is_open) {
            return _tablet_segment_next_id[target_rowset]++;
        } else {
            return _tablet_segment_next_id[target_rowset];
        }
    }
}

void SinkStreamHandler::_handle_message(StreamId stream, std::shared_ptr<butil::IOBuf> message) {
    Status s = Status::OK();
    int opcode = -1;
    UniqueId loadid;
    int64_t indexid = -1;
    int64_t tabletid = -1;
    RowsetId rowsetid; //TODO useless probabily
    int64_t segmentid = -1;
    bool is_last_segment = false;
    std::string path;
    std::string remote_ip_port; // TODO: not needed?
    size_t hdr_len = 0;
    message->cutn((void*)&hdr_len, sizeof(size_t));

    butil::IOBuf hdr_buf;
    message->cutn(&hdr_buf, hdr_len);
    _parse_header(&hdr_buf, &opcode, &loadid, &indexid, &tabletid, &rowsetid, &segmentid, &is_last_segment);
    TargetRowsetPtr target_rowset = std::make_shared<TargetRowset>();
    target_rowset->loadid = loadid;
    target_rowset->indexid = indexid;
    target_rowset->tabletid = tabletid;
    uint64_t final_segmentid = get_next_segmentid(target_rowset, segmentid, opcode == PStreamHeader::OPEN_FILE);
    TargetSegmentPtr target_segment = std::make_shared<TargetSegment>();
    target_segment->target_rowset = target_rowset;
    target_segment->segmentid = final_segmentid;

    // TODO: make it VLOG
    LOG(INFO) << "header parse result:"
              << " loadid = "  << loadid
              << ", indexid = " << indexid
              << ", tabletid = " << tabletid
              << ", segmentid = " << segmentid
              << ", final segmentid = " << final_segmentid;

    switch(opcode) {
    case PStreamHeader::OPEN_FILE:
        path = message->to_string();
        s = _create_and_open_file(target_segment, path);
        break;
    case PStreamHeader::APPEND_DATA:
        s= _append_data(target_segment, message);
        break;
    case PStreamHeader::CLOSE_FILE:
        s = _close_file(target_segment, is_last_segment);
        if (is_last_segment) {
            return _report_status(stream, target_rowset, s.ok(), s.to_string());
        }
        break;
    default:
        DCHECK(false);
    }
    if (!s.ok()) {
        LOG(WARNING) << "streaming error! streamid = " << stream << "msg:" << s.to_string();
        _report_status(stream, target_rowset, s.code(), s.to_string());
    }
}

//TODO trigger build meta when last segment of all cluster is closed

int SinkStreamHandler::on_received_messages(StreamId id, butil::IOBuf *const messages[], size_t size) {
    for (size_t i = 0; i < size; ++i) {
        // TODO make sure that memory will be released at right time!
        std::shared_ptr<butil::IOBuf> messageBuf = std::make_shared<butil::IOBuf>(messages[i]->movable()); // hold the data
        _workers->submit_func([this, id, messageBuf]() { // make it shared_ptr to release data when it is done
            _handle_message(id, messageBuf);
        });
    }
    return 0; // TODO
}

void SinkStreamHandler::on_idle_timeout(StreamId id) {

}

void SinkStreamHandler::on_closed(StreamId id) {
    auto env = doris::ExecEnv::GetInstance();
    StreamIdPtr id_ptr = std::make_shared<StreamId>(id);
    env->get_sink_stream_mgr()->release_stream_id(id_ptr);
}

SinkStreamMgr::SinkStreamMgr() {
    for (int i = 0; i < 1000; ++i) {
        StreamIdPtr stream_id = std::make_shared<StreamId>();
        _free_stream_ids.push_back(stream_id);
    }
    _handler = std::make_shared<SinkStreamHandler>();
}

SinkStreamMgr::~SinkStreamMgr() {
    for (auto& ptr : _free_stream_ids) {
        ptr.reset();
    }
}

StreamIdPtr SinkStreamMgr::get_free_stream_id() {
    StreamIdPtr ptr = nullptr;
    {
        std::lock_guard<std::mutex> l(_lock);
        ptr = _free_stream_ids.back();
        _free_stream_ids.pop_back();
    }
    return ptr;
}

void SinkStreamMgr::release_stream_id(StreamIdPtr id) {
    {
        std::lock_guard<std::mutex> l(_lock);
        _free_stream_ids.push_back(id);
    }
}

} // namespace doris