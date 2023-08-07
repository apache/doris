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

#include "runtime/load_stream.h"

#include <brpc/stream.h>
#include <olap/rowset/rowset_factory.h>
#include <olap/rowset/rowset_meta.h>
#include <olap/storage_engine.h>
#include <olap/tablet_manager.h>
#include <runtime/exec_env.h>

#include "exec/tablet_info.h"
#include "gutil/ref_counted.h"
#include "runtime/load_channel.h"
#include "runtime/load_stream_mgr.h"
#include "runtime/rowset_builder.h"
#include "util/uid_util.h"

namespace doris {

TabletStream::TabletStream(PUniqueId load_id, int64_t id, int64_t txn_id, uint32_t num_senders)
        : _id(id), _next_segid(0), _load_id(load_id), _txn_id(txn_id) {
    for (int i = 0; i < 10; i++) {
        _flush_tokens.emplace_back(ExecEnv::GetInstance()->get_load_stream_mgr()->new_token());
    }

    _segids_mapping.resize(num_senders);
    _failed_st = std::make_shared<Status>();
}

inline std::ostream& operator<<(std::ostream& ostr, const TabletStream& tablet_stream) {
    ostr << "load_id=" << tablet_stream._load_id << ", txn_id=" << tablet_stream._txn_id
         << ", tablet_id=" << tablet_stream._id << ", status=" << *tablet_stream._failed_st;
    return ostr;
}

Status TabletStream::init(OlapTableSchemaParam* schema, int64_t index_id, int64_t partition_id) {
    BuildContext context;
    context.tablet_id = _id;
    // TODO schema_hash
    context.index_id = index_id;
    context.txn_id = _txn_id;
    context.partition_id = partition_id;
    context.load_id = _load_id;
    // TODO tablet_schema
    context.table_schema_param = schema;

    _rowset_builder = std::make_shared<RowsetBuilder>(&context, _load_id);
    auto st = _rowset_builder->init();
    if (!st.ok()) {
        _failed_st = std::make_shared<Status>(st);
        LOG(INFO) << "failed to init rowset builder due to " << *this;
    }
    return st;
}

Status TabletStream::append_data(const PStreamHeader& header, butil::IOBuf* data) {
    // TODO failed early

    // dispatch add_segment request
    if (header.opcode() == PStreamHeader::ADD_SEGMENT) {
        return add_segment(header, data);
    }

    uint32_t sender_id = header.sender_id();
    // We don't need a lock protecting _segids_mapping, because it is written once.
    if (sender_id >= _segids_mapping.size()) {
        LOG(WARNING) << "sender id is out of range, sender_id=" << sender_id
                     << ", num_senders=" << _segids_mapping.size() << *this;
        std::lock_guard lock_guard(_lock);
        _failed_st = std::make_shared<Status>(Status::Error<ErrorCode::INVALID_ARGUMENT>(
                "sender id is out of range {}/{}", sender_id, _segids_mapping.size()));
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("unknown sender_id {}", sender_id);
    }

    uint32_t segid = header.segment_id();
    // Ensure there are enough space and mapping are built.
    if (segid + 1 > _segids_mapping[sender_id].size()) {
        // TODO: Each sender lock is enough.
        std::lock_guard lock_guard(_lock);
        ssize_t origin_size = _segids_mapping[sender_id].size();
        if (segid + 1 > origin_size) {
            _segids_mapping[sender_id].resize(segid + 1, std::numeric_limits<uint32_t>::max());
            // handle concurrency.
            for (size_t index = origin_size; index <= segid; index++) {
                _segids_mapping[sender_id][index] = _next_segid;
                _next_segid++;
                LOG(INFO) << "sender_id=" << sender_id << ", segid=" << index << " to "
                          << " segid=" << _next_segid - 1;
            }
        }
    }

    // Each sender sends data in one segment sequential, so we also do not
    // need a lock here.
    bool eos = header.segment_eos();
    uint32_t new_segid = _segids_mapping[sender_id][segid];
    DCHECK(new_segid != std::numeric_limits<uint32_t>::max());
    butil::IOBuf buf = data->movable();
    auto flush_func = [this, new_segid, eos, buf, header]() {
        auto st = _rowset_builder->append_data(new_segid, buf);
        if (eos && st.ok()) {
            st = _rowset_builder->close_segment(new_segid);
        }
        if (!st.ok() && _failed_st->ok()) {
            _failed_st = std::make_shared<Status>(st);
            LOG(INFO) << "write data failed " << *this;
        }
    };
    return _flush_tokens[segid % _flush_tokens.size()]->submit_func(flush_func);
}

Status TabletStream::add_segment(const PStreamHeader& header, butil::IOBuf* data) {
    DCHECK(header.has_segment_statistics());
    SegmentStatistics stat(header.segment_statistics());

    uint32_t sender_id = header.sender_id();
    uint32_t segid = header.segment_id();
    uint32_t new_segid = _segids_mapping[sender_id][segid];
    DCHECK(new_segid != std::numeric_limits<uint32_t>::max());

    return _rowset_builder->add_segment(new_segid, stat);
}

Status TabletStream::close() {
    for (auto& token : _flush_tokens) {
        token->wait();
    }
    if (!_failed_st->ok()) {
        return *_failed_st;
    }
    return _rowset_builder->close();
}

Status IndexStream::append_data(const PStreamHeader& header, butil::IOBuf* data) {
    int64_t tablet_id = header.tablet_id();
    TabletStreamSharedPtr tablet_stream;
    {
        std::lock_guard lock_guard(_lock);
        auto it = _tablet_streams_map.find(tablet_id);
        if (it == _tablet_streams_map.end()) {
            tablet_stream =
                    std::make_shared<TabletStream>(_load_id, tablet_id, _txn_id, _num_senders);
            _tablet_streams_map[tablet_id] = tablet_stream;
            RETURN_IF_ERROR(tablet_stream->init(_schema.get(), _id, header.partition_id()));
        } else {
            tablet_stream = it->second;
        }
    }

    return tablet_stream->append_data(header, data);
}

void IndexStream::close(std::vector<int64_t>* success_tablet_ids,
                        std::vector<int64_t>* failed_tablet_ids) {
    std::lock_guard lock_guard(_lock);
    for (auto& it : _tablet_streams_map) {
        auto st = it.second->close();
        if (st.ok()) {
            success_tablet_ids->push_back(it.second->id());
        } else {
            LOG(INFO) << "close tablet stream " << *it.second << ", status=" << st;
            failed_tablet_ids->push_back(it.second->id());
        }
    }
    failed_tablet_ids->insert(failed_tablet_ids->end(), _failed_tablet_ids.begin(),
                              _failed_tablet_ids.end());
}

LoadStream::LoadStream(PUniqueId id) : _id(id) {}

LoadStream::~LoadStream() {
    LOG(INFO) << "load stream is deconstructed " << *this;
}

Status LoadStream::init(const POpenStreamSinkRequest* request) {
    _num_senders = request->num_senders();
    _num_working_senders = request->num_senders();
    // TODO: how to ensure the num_stream_per_sender is the same for each sender from different BEs.
    _num_stream_per_sender = request->num_stream_per_sender();
    _senders_closed_streams.resize(_num_senders, 0);
    _senders_status.resize(_num_senders, true);
    _txn_id = request->txn_id();

    _schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_schema->init(request->schema()));
    for (auto& index : request->schema().indexes()) {
        _index_streams_map[index.id()] =
                std::make_shared<IndexStream>(_id, index.id(), _txn_id, _num_senders, _schema);
    }
    LOG(INFO) << "succeed to init load stream " << *this;
    return Status::OK();
}

Status LoadStream::close(uint32_t sender_id, std::vector<int64_t>* success_tablet_ids,
                         std::vector<int64_t>* failed_tablet_ids) {
    if (sender_id >= _senders_status.size()) {
        LOG(WARNING) << "out of range sender id " << sender_id << "  num "
                     << _senders_status.size();
        std::lock_guard lock_guard(_lock);
        failed_tablet_ids->insert(failed_tablet_ids->end(), _failed_tablet_ids.begin(),
                                  _failed_tablet_ids.end());
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("unknown sender_id {}", sender_id);
    }

    std::lock_guard lock_guard(_lock);

    // we do nothing until recv CLOSE_LOAD from all stream to ensure all data are handled before ack
    if ((++_senders_closed_streams[sender_id]) < _num_stream_per_sender) {
        LOG(INFO) << fmt::format("OOXXOO {} out of {} stream received CLOSE_LOAD from sender {}",
                                 _senders_closed_streams[sender_id], _num_stream_per_sender,
                                 sender_id);
        return Status::OK();
    }

    if (!_senders_status[sender_id]) {
        return Status::OK();
    }
    _senders_status[sender_id] = false;
    _num_working_senders--;
    if (_num_working_senders == 0) {
        for (auto& it : _index_streams_map) {
            it.second->close(success_tablet_ids, failed_tablet_ids);
        }
        failed_tablet_ids->insert(failed_tablet_ids->end(), _failed_tablet_ids.begin(),
                                  _failed_tablet_ids.end());
        LOG(INFO) << "close load " << *this << ", failed_tablet_num=" << failed_tablet_ids->size()
                  << ", success_tablet_num=" << success_tablet_ids->size();
        return Status::OK();
    }

    // do not return commit info for non-last one.
    return Status::OK();
}

void LoadStream::_report_result(StreamId stream, Status& st,
                                std::vector<int64_t>* success_tablet_ids,
                                std::vector<int64_t>* failed_tablet_ids) {
    LOG(INFO) << "OOXXOO report result, success tablet num " << success_tablet_ids->size()
              << ", failed tablet num " << failed_tablet_ids->size();
    // TODO
    butil::IOBuf buf;
    PWriteStreamSinkResponse response;
    st.to_protobuf(response.mutable_status());
    for (auto& id : *success_tablet_ids) {
        response.add_success_tablet_ids(id);
    }

    for (auto& id : *failed_tablet_ids) {
        response.add_failed_tablet_ids(id);
    }

    buf.append(response.SerializeAsString());
    int ret = brpc::StreamWrite(stream, buf);
    // TODO: handle eagain
    if (ret == EAGAIN) {
        LOG(WARNING) << "OOXXOO report status EAGAIN";
    } else if (ret == EINVAL) {
        LOG(WARNING) << "OOXXOO report status EINVAL";
    } else {
        LOG(INFO) << "OOXXOO report status " << ret;
    }
}

void LoadStream::_parse_header(butil::IOBuf* const message, PStreamHeader& hdr) {
    butil::IOBufAsZeroCopyInputStream wrapper(*message);
    hdr.ParseFromZeroCopyStream(&wrapper);
    // TODO: make it VLOG
    LOG(INFO) << "header parse result: " << hdr.DebugString();
}

Status LoadStream::_append_data(const PStreamHeader& header, butil::IOBuf* data) {
    IndexStreamSharedPtr index_stream;

    uint32_t sender_id = header.sender_id();
    int64_t tablet_id = header.tablet_id();
    if (sender_id >= _senders_status.size()) {
        LOG(WARNING) << "out of range sender id " << sender_id << "  num "
                     << _senders_status.size();
        std::lock_guard lock_guard(_lock);
        _failed_tablet_ids.insert(tablet_id);
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("unknown sender_id {}", sender_id);
    }

    int64_t index_id = header.index_id();
    auto it = _index_streams_map.find(index_id);
    if (it == _index_streams_map.end()) {
        // TODO ERROR
        LOG(WARNING) << "try to append data to unknown index, index_id " << index_id;
        _failed_tablet_ids.insert(tablet_id);
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("unknown index_id {}", index_id);
    } else {
        index_stream = it->second;
    }

    return index_stream->append_data(header, data);
}

//TODO trigger build meta when last segment of all cluster is closed
int LoadStream::on_received_messages(StreamId id, butil::IOBuf* const messages[], size_t size) {
    std::shared_ptr<SegmentStatistics> stat = nullptr;
    LOG(INFO) << "OOXXOO on_received_messages " << id << " " << size;
    for (size_t i = 0; i < size; ++i) {
        // step 1: parse header
        size_t hdr_len = 0;
        messages[i]->cutn((void*)&hdr_len, sizeof(size_t));
        butil::IOBuf hdr_buf;
        PStreamHeader hdr;
        messages[i]->cutn(&hdr_buf, hdr_len);
        _parse_header(&hdr_buf, hdr);

        // step 2: dispatch
        switch (hdr.opcode()) {
        case PStreamHeader::ADD_SEGMENT: // ADD_SEGMENT will be dispatched inside TabletStream
        case PStreamHeader::APPEND_DATA: {
            auto st = _append_data(hdr, messages[i]);
            if (!st.ok()) {
                std::vector<int64_t> success_tablet_ids;
                std::vector<int64_t> failed_tablet_ids;
                failed_tablet_ids.push_back(hdr.tablet_id());
                _report_result(id, st, &success_tablet_ids, &failed_tablet_ids);
            }
        } break;
        case PStreamHeader::CLOSE_LOAD: {
            std::vector<int64_t> success_tablet_ids;
            std::vector<int64_t> failed_tablet_ids;
            auto st = close(hdr.sender_id(), &success_tablet_ids, &failed_tablet_ids);
            _report_result(id, st, &success_tablet_ids, &failed_tablet_ids);
        } break;
        default:
            LOG(WARNING) << "unexpected stream message " << hdr.opcode();
            DCHECK(false);
        }
    }
    return 0;
}

void LoadStream::on_idle_timeout(StreamId id) {
    brpc::StreamClose(id);
}

void LoadStream::on_closed(StreamId id) {
    auto remaining_rpc_stream = remove_rpc_stream();
    LOG(INFO) << "stream closed " << id << ", remaining_rpc_stream=" << remaining_rpc_stream;
    if (remaining_rpc_stream == 0) {
        ExecEnv::GetInstance()->get_load_stream_mgr()->clear_load(_id);
    }
}

inline std::ostream& operator<<(std::ostream& ostr, const LoadStream& load_stream) {
    ostr << "load_id=" << UniqueId(load_stream._id) << ", txn_id=" << load_stream._txn_id
         << ", num_senders=" << load_stream._num_senders;
    return ostr;
}

} // namespace doris
