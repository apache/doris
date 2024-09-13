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
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <olap/rowset/rowset_factory.h>
#include <olap/rowset/rowset_meta.h>
#include <olap/storage_engine.h>
#include <olap/tablet_manager.h>
#include <runtime/exec_env.h>

#include <memory>
#include <sstream>

#include "bvar/bvar.h"
#include "common/signal_handler.h"
#include "exec/tablet_info.h"
#include "gutil/ref_counted.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_schema.h"
#include "runtime/fragment_mgr.h"
#include "runtime/load_channel.h"
#include "runtime/load_stream_mgr.h"
#include "runtime/load_stream_writer.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "util/debug_points.h"
#include "util/runtime_profile.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"

#define UNKNOWN_ID_FOR_TEST 0x7c00

namespace doris {

bvar::Adder<int64_t> g_load_stream_cnt("load_stream_count");
bvar::LatencyRecorder g_load_stream_flush_wait_ms("load_stream_flush_wait_ms");
bvar::Adder<int> g_load_stream_flush_running_threads("load_stream_flush_wait_threads");

TabletStream::TabletStream(PUniqueId load_id, int64_t id, int64_t txn_id,
                           LoadStreamMgr* load_stream_mgr, RuntimeProfile* profile)
        : _id(id),
          _next_segid(0),
          _load_id(load_id),
          _txn_id(txn_id),
          _load_stream_mgr(load_stream_mgr) {
    load_stream_mgr->create_tokens(_flush_tokens);
    _status = Status::OK();
    _profile = profile->create_child(fmt::format("TabletStream {}", id), true, true);
    _append_data_timer = ADD_TIMER(_profile, "AppendDataTime");
    _add_segment_timer = ADD_TIMER(_profile, "AddSegmentTime");
    _close_wait_timer = ADD_TIMER(_profile, "CloseWaitTime");
}

inline std::ostream& operator<<(std::ostream& ostr, const TabletStream& tablet_stream) {
    ostr << "load_id=" << tablet_stream._load_id << ", txn_id=" << tablet_stream._txn_id
         << ", tablet_id=" << tablet_stream._id << ", status=" << tablet_stream._status;
    return ostr;
}

Status TabletStream::init(std::shared_ptr<OlapTableSchemaParam> schema, int64_t index_id,
                          int64_t partition_id) {
    WriteRequest req {
            .tablet_id = _id,
            .txn_id = _txn_id,
            .index_id = index_id,
            .partition_id = partition_id,
            .load_id = _load_id,
            .table_schema_param = schema,
            // TODO(plat1ko): write_file_cache
            .storage_vault_id {},
    };

    _load_stream_writer = std::make_shared<LoadStreamWriter>(&req, _profile);
    DBUG_EXECUTE_IF("TabletStream.init.uninited_writer", {
        _status = Status::Uninitialized("fault injection");
        return _status;
    });
    _status = _load_stream_writer->init();
    if (!_status.ok()) {
        LOG(INFO) << "failed to init rowset builder due to " << *this;
    }
    return _status;
}

Status TabletStream::append_data(const PStreamHeader& header, butil::IOBuf* data) {
    if (!_status.ok()) {
        return _status;
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
                VLOG_DEBUG << "src_id=" << src_id << ", segid=" << index << " to "
                           << " segid=" << _next_segid - 1 << ", " << *this;
            }
        }
    }

    // Each sender sends data in one segment sequential, so we also do not
    // need a lock here.
    bool eos = header.segment_eos();
    FileType file_type = header.file_type();
    uint32_t new_segid = mapping->at(segid);
    DCHECK(new_segid != std::numeric_limits<uint32_t>::max());
    butil::IOBuf buf = data->movable();
    auto flush_func = [this, new_segid, eos, buf, header, file_type]() mutable {
        signal::set_signal_task_id(_load_id);
        g_load_stream_flush_running_threads << -1;
        auto st = _load_stream_writer->append_data(new_segid, header.offset(), buf, file_type);
        if (eos && st.ok()) {
            DBUG_EXECUTE_IF("TabletStream.append_data.unknown_file_type",
                            { file_type = static_cast<FileType>(-1); });
            if (file_type == FileType::SEGMENT_FILE || file_type == FileType::INVERTED_INDEX_FILE) {
                st = _load_stream_writer->close_writer(new_segid, file_type);
            } else {
                st = Status::InternalError(
                        "appent data failed, file type error, file type = {}, "
                        "segment_id={}",
                        file_type, new_segid);
            }
        }
        DBUG_EXECUTE_IF("TabletStream.append_data.append_failed",
                        { st = Status::InternalError("fault injection"); });
        if (!st.ok() && _status.ok()) {
            _status = st;
            LOG(WARNING) << "write data failed " << st << ", " << *this;
        }
    };
    auto& flush_token = _flush_tokens[new_segid % _flush_tokens.size()];
    auto load_stream_flush_token_max_tasks = config::load_stream_flush_token_max_tasks;
    auto load_stream_max_wait_flush_token_time_ms =
            config::load_stream_max_wait_flush_token_time_ms;
    DBUG_EXECUTE_IF("TabletStream.append_data.long_wait", {
        load_stream_flush_token_max_tasks = 0;
        load_stream_max_wait_flush_token_time_ms = 1000;
    });
    MonotonicStopWatch timer;
    timer.start();
    while (flush_token->num_tasks() >= load_stream_flush_token_max_tasks) {
        if (timer.elapsed_time() / 1000 / 1000 >= load_stream_max_wait_flush_token_time_ms) {
            _status = Status::Error<true>(
                    "wait flush token back pressure time is more than "
                    "load_stream_max_wait_flush_token_time {}",
                    load_stream_max_wait_flush_token_time_ms);
            return _status;
        }
        bthread_usleep(2 * 1000); // 2ms
    }
    timer.stop();
    int64_t time_ms = timer.elapsed_time() / 1000 / 1000;
    g_load_stream_flush_wait_ms << time_ms;
    g_load_stream_flush_running_threads << 1;
    Status st = Status::OK();
    DBUG_EXECUTE_IF("TabletStream.append_data.submit_func_failed",
                    { st = Status::InternalError("fault injection"); });
    if (st.ok()) {
        st = flush_token->submit_func(flush_func);
    }
    if (!st.ok()) {
        _status = st;
    }
    return _status;
}

Status TabletStream::add_segment(const PStreamHeader& header, butil::IOBuf* data) {
    if (!_status.ok()) {
        return _status;
    }

    SCOPED_TIMER(_add_segment_timer);
    DCHECK(header.has_segment_statistics());
    SegmentStatistics stat(header.segment_statistics());
    TabletSchemaSPtr flush_schema;
    if (header.has_flush_schema()) {
        flush_schema = std::make_shared<TabletSchema>();
        flush_schema->init_from_pb(header.flush_schema());
    }

    int64_t src_id = header.src_id();
    uint32_t segid = header.segment_id();
    uint32_t new_segid;
    DBUG_EXECUTE_IF("TabletStream.add_segment.unknown_segid", { segid = UNKNOWN_ID_FOR_TEST; });
    {
        std::lock_guard lock_guard(_lock);
        if (!_segids_mapping.contains(src_id)) {
            _status = Status::InternalError(
                    "add segment failed, no segment written by this src be yet, src_id={}, "
                    "segment_id={}",
                    src_id, segid);
            return _status;
        }
        DBUG_EXECUTE_IF("TabletStream.add_segment.segid_never_written",
                        { segid = _segids_mapping[src_id]->size(); });
        if (segid >= _segids_mapping[src_id]->size()) {
            _status = Status::InternalError(
                    "add segment failed, segment is never written, src_id={}, segment_id={}",
                    src_id, segid);
            return _status;
        }
        new_segid = _segids_mapping[src_id]->at(segid);
    }
    DCHECK(new_segid != std::numeric_limits<uint32_t>::max());

    auto add_segment_func = [this, new_segid, stat, flush_schema]() {
        signal::set_signal_task_id(_load_id);
        auto st = _load_stream_writer->add_segment(new_segid, stat, flush_schema);
        DBUG_EXECUTE_IF("TabletStream.add_segment.add_segment_failed",
                        { st = Status::InternalError("fault injection"); });
        if (!st.ok() && _status.ok()) {
            _status = st;
            LOG(INFO) << "add segment failed " << *this;
        }
    };
    auto& flush_token = _flush_tokens[new_segid % _flush_tokens.size()];
    Status st = Status::OK();
    DBUG_EXECUTE_IF("TabletStream.add_segment.submit_func_failed",
                    { st = Status::InternalError("fault injection"); });
    if (st.ok()) {
        st = flush_token->submit_func(add_segment_func);
    }
    if (!st.ok()) {
        _status = st;
    }
    return _status;
}

Status TabletStream::close() {
    if (!_status.ok()) {
        return _status;
    }

    SCOPED_TIMER(_close_wait_timer);
    bthread::Mutex mu;
    std::unique_lock<bthread::Mutex> lock(mu);
    bthread::ConditionVariable cv;
    auto wait_func = [this, &mu, &cv] {
        signal::set_signal_task_id(_load_id);
        for (auto& token : _flush_tokens) {
            token->wait();
        }
        std::lock_guard<bthread::Mutex> lock(mu);
        cv.notify_one();
    };
    bool ret = _load_stream_mgr->heavy_work_pool()->try_offer(wait_func);
    if (ret) {
        cv.wait(lock);
    } else {
        _status = Status::Error<ErrorCode::INTERNAL_ERROR>(
                "there is not enough thread resource for close load");
        return _status;
    }

    DBUG_EXECUTE_IF("TabletStream.close.segment_num_mismatch", { _num_segments++; });
    if (_next_segid.load() != _num_segments) {
        _status = Status::Corruption(
                "segment num mismatch in tablet {}, expected: {}, actual: {}, load_id: {}", _id,
                _num_segments, _next_segid.load(), print_id(_load_id));
        return _status;
    }

    // it is necessary to check status after wait_func,
    // for create_rowset could fail during add_segment when loading to MOW table,
    // in this case, should skip close to avoid submit_calc_delete_bitmap_task which could cause coredump.
    if (!_status.ok()) {
        return _status;
    }

    auto close_func = [this, &mu, &cv]() {
        signal::set_signal_task_id(_load_id);
        auto st = _load_stream_writer->close();
        if (!st.ok() && _status.ok()) {
            _status = st;
        }
        std::lock_guard<bthread::Mutex> lock(mu);
        cv.notify_one();
    };
    ret = _load_stream_mgr->heavy_work_pool()->try_offer(close_func);
    if (ret) {
        cv.wait(lock);
    } else {
        _status = Status::Error<ErrorCode::INTERNAL_ERROR>(
                "there is not enough thread resource for close load");
    }
    return _status;
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
            _init_tablet_stream(tablet_stream, tablet_id, header.partition_id());
        } else {
            tablet_stream = it->second;
        }
    }

    return tablet_stream->append_data(header, data);
}

void IndexStream::_init_tablet_stream(TabletStreamSharedPtr& tablet_stream, int64_t tablet_id,
                                      int64_t partition_id) {
    tablet_stream = std::make_shared<TabletStream>(_load_id, tablet_id, _txn_id, _load_stream_mgr,
                                                   _profile);
    _tablet_streams_map[tablet_id] = tablet_stream;
    auto st = tablet_stream->init(_schema, _id, partition_id);
    if (!st.ok()) {
        LOG(WARNING) << "tablet stream init failed " << *tablet_stream;
    }
}

void IndexStream::close(const std::vector<PTabletID>& tablets_to_commit,
                        std::vector<int64_t>* success_tablet_ids, FailedTablets* failed_tablets) {
    std::lock_guard lock_guard(_lock);
    SCOPED_TIMER(_close_wait_timer);
    // open all need commit tablets
    for (const auto& tablet : tablets_to_commit) {
        if (_id != tablet.index_id()) {
            continue;
        }
        TabletStreamSharedPtr tablet_stream;
        auto it = _tablet_streams_map.find(tablet.tablet_id());
        if (it == _tablet_streams_map.end()) {
            _init_tablet_stream(tablet_stream, tablet.tablet_id(), tablet.partition_id());
            tablet_stream->add_num_segments(tablet.num_segments());
        } else {
            it->second->add_num_segments(tablet.num_segments());
        }
    }

    for (auto& [_, tablet_stream] : _tablet_streams_map) {
        auto st = tablet_stream->close();
        if (st.ok()) {
            success_tablet_ids->push_back(tablet_stream->id());
        } else {
            LOG(INFO) << "close tablet stream " << *tablet_stream << ", status=" << st;
            failed_tablets->emplace_back(tablet_stream->id(), st);
        }
    }
}

// TODO: Profile is temporary disabled, because:
// 1. It's not being processed by the upstream for now
// 2. There are some problems in _profile->to_thrift()
LoadStream::LoadStream(PUniqueId load_id, LoadStreamMgr* load_stream_mgr, bool enable_profile)
        : _load_id(load_id), _enable_profile(false), _load_stream_mgr(load_stream_mgr) {
    g_load_stream_cnt << 1;
    _profile = std::make_unique<RuntimeProfile>("LoadStream");
    _append_data_timer = ADD_TIMER(_profile, "AppendDataTime");
    _close_wait_timer = ADD_TIMER(_profile, "CloseWaitTime");
    TUniqueId load_tid = ((UniqueId)load_id).to_thrift();
#ifndef BE_TEST
    std::shared_ptr<QueryContext> query_context =
            ExecEnv::GetInstance()->fragment_mgr()->get_or_erase_query_ctx_with_lock(load_tid);
    if (query_context != nullptr) {
        _query_thread_context = {load_tid, query_context->query_mem_tracker,
                                 query_context->workload_group()};
    } else {
        _query_thread_context = {load_tid, MemTrackerLimiter::create_shared(
                                                   MemTrackerLimiter::Type::LOAD,
                                                   fmt::format("(FromLoadStream)Load#Id={}",
                                                               ((UniqueId)load_id).to_string()))};
    }
#else
    _query_thread_context = {load_tid, MemTrackerLimiter::create_shared(
                                               MemTrackerLimiter::Type::LOAD,
                                               fmt::format("(FromLoadStream)Load#Id={}",
                                                           ((UniqueId)load_id).to_string()))};
#endif
}

LoadStream::~LoadStream() {
    g_load_stream_cnt << -1;
    LOG(INFO) << "load stream is deconstructed " << *this;
}

Status LoadStream::init(const POpenLoadStreamRequest* request) {
    _txn_id = request->txn_id();
    _total_streams = request->total_streams();
    _is_incremental = (_total_streams == 0);

    _schema = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(_schema->init(request->schema()));
    for (auto& index : request->schema().indexes()) {
        _index_streams_map[index.id()] = std::make_shared<IndexStream>(
                _load_id, index.id(), _txn_id, _schema, _load_stream_mgr, _profile.get());
    }
    LOG(INFO) << "succeed to init load stream " << *this;
    return Status::OK();
}

void LoadStream::close(int64_t src_id, const std::vector<PTabletID>& tablets_to_commit,
                       std::vector<int64_t>* success_tablet_ids, FailedTablets* failed_tablets) {
    std::lock_guard<bthread::Mutex> lock_guard(_lock);
    SCOPED_TIMER(_close_wait_timer);

    // we do nothing until recv CLOSE_LOAD from all stream to ensure all data are handled before ack
    _open_streams[src_id]--;
    if (_open_streams[src_id] == 0) {
        _open_streams.erase(src_id);
    }
    _close_load_cnt++;
    LOG(INFO) << "received CLOSE_LOAD from sender " << src_id << ", remaining "
              << _total_streams - _close_load_cnt << " senders, " << *this;

    _tablets_to_commit.insert(_tablets_to_commit.end(), tablets_to_commit.begin(),
                              tablets_to_commit.end());

    if (_close_load_cnt < _total_streams) {
        // do not return commit info if there is remaining streams.
        return;
    }

    for (auto& [_, index_stream] : _index_streams_map) {
        index_stream->close(_tablets_to_commit, success_tablet_ids, failed_tablets);
    }
    LOG(INFO) << "close load " << *this << ", success_tablet_num=" << success_tablet_ids->size()
              << ", failed_tablet_num=" << failed_tablets->size();
}

void LoadStream::_report_result(StreamId stream, const Status& status,
                                const std::vector<int64_t>& success_tablet_ids,
                                const FailedTablets& failed_tablets, bool eos) {
    LOG(INFO) << "report result " << *this << ", success tablet num " << success_tablet_ids.size()
              << ", failed tablet num " << failed_tablets.size();
    butil::IOBuf buf;
    PLoadStreamResponse response;
    response.set_eos(eos);
    status.to_protobuf(response.mutable_status());
    for (auto& id : success_tablet_ids) {
        response.add_success_tablet_ids(id);
    }
    for (auto& [id, st] : failed_tablets) {
        auto pb = response.add_failed_tablets();
        pb->set_id(id);
        st.to_protobuf(pb->mutable_status());
    }

    if (_enable_profile && _close_load_cnt == _total_streams) {
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
            LOG(WARNING) << "TRuntimeProfileTree serialize failed, errmsg=" << st << ", " << *this;
        }
    }

    buf.append(response.SerializeAsString());
    auto wst = _write_stream(stream, buf);
    if (!wst.ok()) {
        LOG(WARNING) << " report result failed with " << wst << ", " << *this;
    }
}

void LoadStream::_report_schema(StreamId stream, const PStreamHeader& hdr) {
    butil::IOBuf buf;
    PLoadStreamResponse response;
    Status st = Status::OK();
    for (const auto& req : hdr.tablets()) {
        BaseTabletSPtr tablet;
        if (auto res = ExecEnv::get_tablet(req.tablet_id()); res.has_value()) {
            tablet = std::move(res).value();
        } else {
            st = std::move(res).error();
            break;
        }
        auto* resp = response.add_tablet_schemas();
        resp->set_index_id(req.index_id());
        resp->set_enable_unique_key_merge_on_write(tablet->enable_unique_key_merge_on_write());
        tablet->tablet_schema()->to_schema_pb(resp->mutable_tablet_schema());
    }
    st.to_protobuf(response.mutable_status());

    buf.append(response.SerializeAsString());
    auto wst = _write_stream(stream, buf);
    if (!wst.ok()) {
        LOG(WARNING) << " report result failed with " << wst << ", " << *this;
    }
}

Status LoadStream::_write_stream(StreamId stream, butil::IOBuf& buf) {
    for (;;) {
        int ret = 0;
        DBUG_EXECUTE_IF("LoadStream._write_stream.EAGAIN", { ret = EAGAIN; });
        if (ret == 0) {
            ret = brpc::StreamWrite(stream, buf);
        }
        switch (ret) {
        case 0:
            return Status::OK();
        case EAGAIN: {
            const timespec time = butil::seconds_from_now(config::load_stream_eagain_wait_seconds);
            int wait_ret = brpc::StreamWait(stream, &time);
            if (wait_ret != 0) {
                return Status::InternalError("StreamWait failed, err={}", wait_ret);
            }
            break;
        }
        default:
            return Status::InternalError("StreamWrite failed, err={}", ret);
        }
    }
    return Status::OK();
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
    DBUG_EXECUTE_IF("TabletStream._append_data.unknown_indexid",
                    { index_id = UNKNOWN_ID_FOR_TEST; });
    auto it = _index_streams_map.find(index_id);
    if (it == _index_streams_map.end()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("unknown index_id {}", index_id);
    } else {
        index_stream = it->second;
    }

    return index_stream->append_data(header, data);
}

int LoadStream::on_received_messages(StreamId id, butil::IOBuf* const messages[], size_t size) {
    VLOG_DEBUG << "on_received_messages " << id << " " << size;
    for (size_t i = 0; i < size; ++i) {
        while (messages[i]->size() > 0) {
            // step 1: parse header
            size_t hdr_len = 0;
            messages[i]->cutn((void*)&hdr_len, sizeof(size_t));
            butil::IOBuf hdr_buf;
            PStreamHeader hdr;
            messages[i]->cutn(&hdr_buf, hdr_len);
            _parse_header(&hdr_buf, hdr);

            // step 2: cut data
            size_t data_len = 0;
            messages[i]->cutn((void*)&data_len, sizeof(size_t));
            butil::IOBuf data_buf;
            PStreamHeader data;
            messages[i]->cutn(&data_buf, data_len);

            // step 3: dispatch
            _dispatch(id, hdr, &data_buf);
        }
    }
    return 0;
}

void LoadStream::_dispatch(StreamId id, const PStreamHeader& hdr, butil::IOBuf* data) {
    VLOG_DEBUG << PStreamHeader_Opcode_Name(hdr.opcode()) << " from " << hdr.src_id()
               << " with tablet " << hdr.tablet_id();
    SCOPED_ATTACH_TASK(_query_thread_context);
    // CLOSE_LOAD message should not be fault injected,
    // otherwise the message will be ignored and causing close wait timeout
    if (hdr.opcode() != PStreamHeader::CLOSE_LOAD) {
        DBUG_EXECUTE_IF("LoadStream._dispatch.unknown_loadid", {
            PUniqueId& load_id = const_cast<PUniqueId&>(hdr.load_id());
            load_id.set_hi(UNKNOWN_ID_FOR_TEST);
            load_id.set_lo(UNKNOWN_ID_FOR_TEST);
        });
        DBUG_EXECUTE_IF("LoadStream._dispatch.unknown_srcid", {
            PStreamHeader& t_hdr = const_cast<PStreamHeader&>(hdr);
            t_hdr.set_src_id(UNKNOWN_ID_FOR_TEST);
        });
    }
    if (UniqueId(hdr.load_id()) != UniqueId(_load_id)) {
        Status st = Status::Error<ErrorCode::INVALID_ARGUMENT>(
                "invalid load id {}, expected {}", print_id(hdr.load_id()), print_id(_load_id));
        _report_failure(id, st, hdr);
        return;
    }

    {
        std::lock_guard lock_guard(_lock);
        if (!_open_streams.contains(hdr.src_id())) {
            Status st = Status::Error<ErrorCode::INVALID_ARGUMENT>("no open stream from source {}",
                                                                   hdr.src_id());
            _report_failure(id, st, hdr);
            return;
        }
    }

    switch (hdr.opcode()) {
    case PStreamHeader::ADD_SEGMENT: // ADD_SEGMENT will be dispatched inside TabletStream
    case PStreamHeader::APPEND_DATA: {
        auto st = _append_data(hdr, data);
        if (!st.ok()) {
            _report_failure(id, st, hdr);
        }
    } break;
    case PStreamHeader::CLOSE_LOAD: {
        std::vector<int64_t> success_tablet_ids;
        FailedTablets failed_tablets;
        std::vector<PTabletID> tablets_to_commit(hdr.tablets().begin(), hdr.tablets().end());
        close(hdr.src_id(), tablets_to_commit, &success_tablet_ids, &failed_tablets);
        _report_result(id, Status::OK(), success_tablet_ids, failed_tablets, true);
        brpc::StreamClose(id);
    } break;
    case PStreamHeader::GET_SCHEMA: {
        _report_schema(id, hdr);
    } break;
    default:
        LOG(WARNING) << "unexpected stream message " << hdr.opcode() << ", " << *this;
        DCHECK(false);
    }
}

void LoadStream::on_idle_timeout(StreamId id) {
    LOG(WARNING) << "closing load stream on idle timeout, " << *this;
    brpc::StreamClose(id);
}

void LoadStream::on_closed(StreamId id) {
    // `this` may be freed by other threads after increasing `_close_rpc_cnt`,
    // format string first to prevent use-after-free
    std::stringstream ss;
    ss << *this;
    auto remaining_streams = _total_streams - _close_rpc_cnt.fetch_add(1) - 1;
    LOG(INFO) << "stream " << id << " on_closed, remaining streams = " << remaining_streams << ", "
              << ss.str();
    if (remaining_streams == 0) {
        _load_stream_mgr->clear_load(_load_id);
    }
}

inline std::ostream& operator<<(std::ostream& ostr, const LoadStream& load_stream) {
    ostr << "load_id=" << print_id(load_stream._load_id) << ", txn_id=" << load_stream._txn_id;
    return ostr;
}

} // namespace doris
