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
#include "runtime/load_stream_writer.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"

namespace doris {

TabletStream::TabletStream(PUniqueId load_id, int64_t id, int64_t txn_id,
                           LoadStreamMgr* load_stream_mgr, RuntimeProfile* profile)
        : _id(id), _next_segid(0), _load_id(load_id), _txn_id(txn_id) {
    for (int i = 0; i < 10; i++) {
        _flush_tokens.emplace_back(load_stream_mgr->new_token());
    }

    _failed_st = std::make_shared<Status>();
    _profile = profile->create_child(fmt::format("TabletStream {}", id), true, true);
    _append_data_timer = ADD_TIMER(_profile, "AppendDataTime");
    _add_segment_timer = ADD_TIMER(_profile, "AddSegmentTime");
    _close_wait_timer = ADD_TIMER(_profile, "CloseWaitTime");
}

inline std::ostream& operator<<(std::ostream& ostr, const TabletStream& tablet_stream) {
    ostr << "load_id=" << tablet_stream._load_id << ", txn_id=" << tablet_stream._txn_id
         << ", tablet_id=" << tablet_stream._id << ", status=" << *tablet_stream._failed_st;
    return ostr;
}

Status TabletStream::init(OlapTableSchemaParam* schema, int64_t index_id, int64_t partition_id) {
    WriteRequest req;
    req.tablet_id = _id;
    req.index_id = index_id;
    req.txn_id = _txn_id;
    req.partition_id = partition_id;
    req.load_id = _load_id;
    req.table_schema_param = schema;

    _load_stream_writer = std::make_shared<LoadStreamWriter>(&req, _profile);
    auto st = _load_stream_writer->init();
    if (!st.ok()) {
        _failed_st = std::make_shared<Status>(st);
        LOG(INFO) << "failed to init rowset builder due to " << *this;
    }
    return st;
}

Status TabletStream::append_data(const PStreamHeader& header, butil::IOBuf* data) {
    if (!_failed_st->ok()) {
        return *_failed_st;
    }

    // dispatch add_segment request
    if (header.opcode() == PStreamHeader::ADD_SEGMENT) {
        return add_segment(header, data);
    }

    SCOPED_TIMER(_append_data_timer);

    int64_t src_id = header.src_id();
    uint32_t segid = header.segment_id();
    // Ensure there are enough space and mapping are built.
    SegIdMapping* mapping = nullptr;
    {
        std::lock_guard lock_guard(_lock);
        if (!_segids_mapping.contains(src_id)) {
            _segids_mapping[src_id] = std::make_unique<SegIdMapping>();
        }
        mapping = _segids_mapping[src_id].get();
    }
    if (segid + 1 > mapping->size()) {
        // TODO: Each sender lock is enough.
        std::lock_guard lock_guard(_lock);
        ssize_t origin_size = mapping->size();
        if (segid + 1 > origin_size) {
            mapping->resize(segid + 1, std::numeric_limits<uint32_t>::max());
            for (size_t index = origin_size; index <= segid; index++) {
                mapping->at(index) = _next_segid;
                _next_segid++;
                LOG(INFO) << "src_id=" << src_id << ", segid=" << index << " to "
                          << " segid=" << _next_segid - 1;
            }
        }
    }

    // Each sender sends data in one segment sequential, so we also do not
    // need a lock here.
    bool eos = header.segment_eos();
    uint32_t new_segid = mapping->at(segid);
    DCHECK(new_segid != std::numeric_limits<uint32_t>::max());
    butil::IOBuf buf = data->movable();
    auto flush_func = [this, new_segid, eos, buf, header]() {
        auto st = _load_stream_writer->append_data(new_segid, buf);
        if (eos && st.ok()) {
            st = _load_stream_writer->close_segment(new_segid);
        }
        if (!st.ok() && _failed_st->ok()) {
            _failed_st = std::make_shared<Status>(st);
            LOG(INFO) << "write data failed " << *this;
        }
    };
    return _flush_tokens[segid % _flush_tokens.size()]->submit_func(flush_func);
}

Status TabletStream::add_segment(const PStreamHeader& header, butil::IOBuf* data) {
    SCOPED_TIMER(_add_segment_timer);
    DCHECK(header.has_segment_statistics());
    SegmentStatistics stat(header.segment_statistics());

    int64_t src_id = header.src_id();
    uint32_t segid = header.segment_id();
    uint32_t new_segid;
    {
        std::lock_guard lock_guard(_lock);
        if (!_segids_mapping.contains(src_id)) {
            LOG(WARNING) << "No segid mapping for src_id " << src_id
                         << " when ADD_SEGMENT, ignored";
            return Status::OK();
        }
        new_segid = _segids_mapping[src_id]->at(segid);
    }
    DCHECK(new_segid != std::numeric_limits<uint32_t>::max());

    return _load_stream_writer->add_segment(new_segid, stat);
}

Status TabletStream::close() {
    SCOPED_TIMER(_close_wait_timer);
    for (auto& token : _flush_tokens) {
        token->wait();
    }
    if (!_failed_st->ok()) {
        return *_failed_st;
    }
    return _load_stream_writer->close();
}

IndexStream::IndexStream(PUniqueId load_id, int64_t id, int64_t txn_id,
                         std::shared_ptr<OlapTableSchemaParam> schema,
                         LoadStreamMgr* load_stream_mgr, RuntimeProfile* profile)
        : _id(id),
          _load_id(load_id),
          _txn_id(txn_id),
          _schema(schema),
          _load_stream_mgr(load_stream_mgr) {
    _profile = profile->create_child(fmt::format("IndexStream {}", id), true, true);
    _append_data_timer = ADD_TIMER(_profile, "AppendDataTime");
    _close_wait_timer = ADD_TIMER(_profile, "CloseWaitTime");
}

Status IndexStream::append_data(const PStreamHeader& header, butil::IOBuf* data) {
    SCOPED_TIMER(_append_data_timer);
    int64_t tablet_id = header.tablet_id();
    TabletStreamSharedPtr tablet_stream;
    {
        std::lock_guard lock_guard(_lock);
        auto it = _tablet_streams_map.find(tablet_id);
        if (it == _tablet_streams_map.end()) {
            RETURN_IF_ERROR(_init_tablet_stream(tablet_stream, tablet_id, header.partition_id()));
        } else {
            tablet_stream = it->second;
        }
    }

    return tablet_stream->append_data(header, data);
}

Status IndexStream::_init_tablet_stream(TabletStreamSharedPtr& tablet_stream, int64_t tablet_id,
                                        int64_t partition_id) {
    tablet_stream = std::make_shared<TabletStream>(_load_id, tablet_id, _txn_id, _load_stream_mgr,
                                                   _profile);
    RETURN_IF_ERROR(tablet_stream->init(_schema.get(), _id, partition_id));
    _tablet_streams_map[tablet_id] = tablet_stream;
    return Status::OK();
}

Status IndexStream::close(const std::vector<PTabletID>& tablets_to_commit,
                          std::vector<int64_t>* success_tablet_ids,
                          std::vector<int64_t>* failed_tablet_ids) {
    std::lock_guard lock_guard(_lock);
    SCOPED_TIMER(_close_wait_timer);
    // open all need commit tablets
    for (const auto& tablet : tablets_to_commit) {
        TabletStreamSharedPtr tablet_stream;
        auto it = _tablet_streams_map.find(tablet.tablet_id());
        if (it == _tablet_streams_map.end() && _id == tablet.index_id()) {
            RETURN_IF_ERROR(
                    _init_tablet_stream(tablet_stream, tablet.tablet_id(), tablet.partition_id()));
        }
    }

    for (auto& it : _tablet_streams_map) {
        auto st = it.second->close();
        if (st.ok()) {
            success_tablet_ids->push_back(it.second->id());
        } else {
            LOG(INFO) << "close tablet stream " << *it.second << ", status=" << st;
            failed_tablet_ids->push_back(it.second->id());
        }
    }
    return Status::OK();
}

LoadStream::LoadStream(PUniqueId id, LoadStreamMgr* load_stream_mgr, bool enable_profile)
        : _id(id), _enable_profile(enable_profile), _load_stream_mgr(load_stream_mgr) {
    _profile = std::make_unique<RuntimeProfile>("LoadStream");
    _append_data_timer = ADD_TIMER(_profile, "AppendDataTime");
    _close_wait_timer = ADD_TIMER(_profile, "CloseWaitTime");
}

LoadStream::~LoadStream() {
    LOG(INFO) << "load stream is deconstructed " << *this;
}

Status LoadStream::init(const POpenStreamSinkRequest* request) {
    _txn_id = request->txn_id();

    _schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_schema->init(request->schema()));
    for (auto& index : request->schema().indexes()) {
        _index_streams_map[index.id()] = std::make_shared<IndexStream>(
                _id, index.id(), _txn_id, _schema, _load_stream_mgr, _profile.get());
    }
    LOG(INFO) << "succeed to init load stream " << *this;
    return Status::OK();
}

Status LoadStream::close(int64_t src_id, const std::vector<PTabletID>& tablets_to_commit,
                         std::vector<int64_t>* success_tablet_ids,
                         std::vector<int64_t>* failed_tablet_ids) {
    std::lock_guard lock_guard(_lock);
    SCOPED_TIMER(_close_wait_timer);

    // we do nothing until recv CLOSE_LOAD from all stream to ensure all data are handled before ack
    _open_streams[src_id]--;
    LOG(INFO) << "received CLOSE_LOAD from sender " << src_id << ", remaining "
              << _open_streams[src_id] << " streams";
    if (_open_streams[src_id] == 0) {
        _open_streams.erase(src_id);
    }

    Status st = Status::OK();
    if (_open_streams.size() == 0) {
        bthread::Mutex mutex;
        std::unique_lock<bthread::Mutex> lock(mutex);
        bthread::ConditionVariable cond;
        bool ret = _load_stream_mgr->heavy_work_pool()->try_offer([this, &success_tablet_ids,
                                                                   &failed_tablet_ids,
                                                                   &tablets_to_commit, &mutex,
                                                                   &cond, &st]() {
            for (auto& it : _index_streams_map) {
                st = it.second->close(tablets_to_commit, success_tablet_ids, failed_tablet_ids);
                if (!st.ok()) {
                    std::unique_lock<bthread::Mutex> lock(mutex);
                    cond.notify_one();
                    return;
                }
            }
            LOG(INFO) << "close load " << *this
                      << ", failed_tablet_num=" << failed_tablet_ids->size()
                      << ", success_tablet_num=" << success_tablet_ids->size();
            std::unique_lock<bthread::Mutex> lock(mutex);
            cond.notify_one();
        });
        if (ret) {
            cond.wait(lock);
        } else {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "there is not enough thread resource for close load");
        }
    }

    // do not return commit info for non-last one.
    return st;
}

void LoadStream::_report_result(StreamId stream, Status& st,
                                std::vector<int64_t>* success_tablet_ids,
                                std::vector<int64_t>* failed_tablet_ids) {
    LOG(INFO) << "report result, success tablet num " << success_tablet_ids->size()
              << ", failed tablet num " << failed_tablet_ids->size();
    butil::IOBuf buf;
    PWriteStreamSinkResponse response;
    st.to_protobuf(response.mutable_status());
    for (auto& id : *success_tablet_ids) {
        response.add_success_tablet_ids(id);
    }

    for (auto& id : *failed_tablet_ids) {
        response.add_failed_tablet_ids(id);
    }

    if (_enable_profile) {
        TRuntimeProfileTree tprofile;
        ThriftSerializer ser(false, 4096);
        uint8_t* buf = nullptr;
        uint32_t len = 0;
        std::unique_lock<bthread::Mutex> l(_lock);

        _profile->to_thrift(&tprofile);
        auto st = ser.serialize(&tprofile, &len, &buf);
        if (st.ok()) {
            response.set_load_stream_profile(buf, len);
        } else {
            LOG(WARNING) << "load channel TRuntimeProfileTree serialize failed, errmsg=" << st;
        }
    }

    buf.append(response.SerializeAsString());
    int ret = brpc::StreamWrite(stream, buf);
    // TODO: handle EAGAIN
    if (ret != 0) {
        LOG(INFO) << "stream write report status " << ret << ": " << std::strerror(ret);
    }
}

void LoadStream::_parse_header(butil::IOBuf* const message, PStreamHeader& hdr) {
    butil::IOBufAsZeroCopyInputStream wrapper(*message);
    hdr.ParseFromZeroCopyStream(&wrapper);
    VLOG_DEBUG << "header parse result: " << hdr.DebugString();
}

Status LoadStream::_append_data(const PStreamHeader& header, butil::IOBuf* data) {
    SCOPED_TIMER(_append_data_timer);
    IndexStreamSharedPtr index_stream;

    int64_t index_id = header.index_id();
    auto it = _index_streams_map.find(index_id);
    if (it == _index_streams_map.end()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("unknown index_id {}", index_id);
    } else {
        index_stream = it->second;
    }

    Status st = Status::OK();
    {
        bthread::Mutex mutex;
        std::unique_lock<bthread::Mutex> lock(mutex);
        bthread::ConditionVariable cond;
        bool ret = _load_stream_mgr->heavy_work_pool()->try_offer(
                [&index_stream, &header, &data, &mutex, &cond, &st] {
                    st = index_stream->append_data(header, data);
                    std::unique_lock<bthread::Mutex> lock(mutex);
                    cond.notify_one();
                });
        if (ret) {
            cond.wait(lock);
        } else {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "there is not enough thread resource for append data");
        }
    }
    return st;
}

int LoadStream::on_received_messages(StreamId id, butil::IOBuf* const messages[], size_t size) {
    std::shared_ptr<SegmentStatistics> stat = nullptr;
    VLOG_DEBUG << "on_received_messages " << id << " " << size;
    for (size_t i = 0; i < size; ++i) {
        // step 1: parse header
        size_t hdr_len = 0;
        messages[i]->cutn((void*)&hdr_len, sizeof(size_t));
        butil::IOBuf hdr_buf;
        PStreamHeader hdr;
        messages[i]->cutn(&hdr_buf, hdr_len);
        _parse_header(&hdr_buf, hdr);

        VLOG_DEBUG << PStreamHeader_Opcode_Name(hdr.opcode()) << " from " << hdr.src_id()
                   << " with tablet " << hdr.tablet_id();

        {
            std::lock_guard lock_guard(_lock);
            if (!_open_streams.contains(hdr.src_id())) {
                std::vector<int64_t> success_tablet_ids;
                std::vector<int64_t> failed_tablet_ids;
                if (hdr.has_tablet_id()) {
                    failed_tablet_ids.push_back(hdr.tablet_id());
                }
                Status st = Status::Error<ErrorCode::INVALID_ARGUMENT>(
                        "no open stream from source {}", hdr.src_id());
                _report_result(id, st, &success_tablet_ids, &failed_tablet_ids);
                continue;
            }
        }

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
            std::vector<PTabletID> tablets_to_commit(hdr.tablets_to_commit().begin(),
                                                     hdr.tablets_to_commit().end());
            auto st =
                    close(hdr.src_id(), tablets_to_commit, &success_tablet_ids, &failed_tablet_ids);
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
        _load_stream_mgr->clear_load(_id);
    }
}

inline std::ostream& operator<<(std::ostream& ostr, const LoadStream& load_stream) {
    ostr << "load_id=" << UniqueId(load_stream._id) << ", txn_id=" << load_stream._txn_id;
    return ostr;
}

} // namespace doris
