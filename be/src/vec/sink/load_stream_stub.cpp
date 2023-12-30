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

#include "vec/sink/load_stream_stub.h"

#include <sstream>

#include "olap/rowset/rowset_writer.h"
#include "util/brpc_client_cache.h"
#include "util/debug_points.h"
#include "util/network_util.h"
#include "util/thrift_util.h"
#include "util/uid_util.h"

namespace doris {

int LoadStreamStub::LoadStreamReplyHandler::on_received_messages(brpc::StreamId id,
                                                                 butil::IOBuf* const messages[],
                                                                 size_t size) {
    for (size_t i = 0; i < size; i++) {
        butil::IOBufAsZeroCopyInputStream wrapper(*messages[i]);
        PWriteStreamSinkResponse response;
        response.ParseFromZeroCopyStream(&wrapper);

        Status st = Status::create(response.status());

        std::stringstream ss;
        ss << "on_received_messages, load_id=" << _load_id << ", backend_id=" << _dst_id;
        if (response.success_tablet_ids_size() > 0) {
            ss << ", success tablet ids:";
            for (auto tablet_id : response.success_tablet_ids()) {
                ss << " " << tablet_id;
            }
            std::lock_guard<bthread::Mutex> lock(_success_tablets_mutex);
            for (auto tablet_id : response.success_tablet_ids()) {
                _success_tablets.push_back(tablet_id);
            }
        }
        if (response.failed_tablet_ids_size() > 0) {
            ss << ", failed tablet ids:";
            for (auto tablet_id : response.failed_tablet_ids()) {
                ss << " " << tablet_id;
            }
            std::lock_guard<bthread::Mutex> lock(_failed_tablets_mutex);
            for (auto tablet_id : response.failed_tablet_ids()) {
                _failed_tablets.push_back(tablet_id);
            }
        }
        ss << ", status: " << st;
        LOG(INFO) << ss.str();

        if (response.has_load_stream_profile()) {
            TRuntimeProfileTree tprofile;
            const uint8_t* buf =
                    reinterpret_cast<const uint8_t*>(response.load_stream_profile().data());
            uint32_t len = response.load_stream_profile().size();
            auto status = deserialize_thrift_msg(buf, &len, false, &tprofile);
            if (status.ok()) {
                // TODO
                //_sink->_state->load_channel_profile()->update(tprofile);
            } else {
                LOG(WARNING) << "load stream TRuntimeProfileTree deserialize failed, errmsg="
                             << status;
            }
        }

        if (response.tablet_schemas_size() > 0) {
            std::vector<PTabletSchemaWithIndex> schemas(response.tablet_schemas().begin(),
                                                        response.tablet_schemas().end());
            _stub->add_schema(schemas);
        }
    }
    return 0;
}

void LoadStreamStub::LoadStreamReplyHandler::on_closed(brpc::StreamId id) {
    LOG(INFO) << "on_closed, load_id=" << _load_id << ", stream_id=" << id;
    std::lock_guard<bthread::Mutex> lock(_mutex);
    DBUG_EXECUTE_IF("LoadStreamStub::LoadStreamReplyHandler::on_closed.close_wait", { return; });
    _is_closed.store(true);
    _close_cv.notify_all();
}

LoadStreamStub::LoadStreamStub(PUniqueId load_id, int64_t src_id, int num_use, RuntimeState* state)
        : _state(state),
          _use_cnt(num_use),
          _load_id(load_id),
          _src_id(src_id),
          _tablet_schema_for_index(std::make_shared<IndexToTabletSchema>()),
          _enable_unique_mow_for_index(std::make_shared<IndexToEnableMoW>()) {};

LoadStreamStub::LoadStreamStub(LoadStreamStub& stub, RuntimeState* state)
        : _state(state),
          _use_cnt(stub._use_cnt.load()),
          _load_id(stub._load_id),
          _src_id(stub._src_id),
          _tablet_schema_for_index(stub._tablet_schema_for_index),
          _enable_unique_mow_for_index(stub._enable_unique_mow_for_index) {};

LoadStreamStub::~LoadStreamStub() = default;

Status LoadStreamStub::close_stream() {
    if (_is_init.load() && !_handler.is_closed()) {
        LOG(INFO) << "closing stream, load_id=" << print_id(_load_id) << ", src_id=" << _src_id
                  << ", dst_id=" << _dst_id << ", stream_id=" << _stream_id;
        auto ret = brpc::StreamClose(_stream_id);
        if (ret != 0) {
            return Status::InternalError("StreamClose failed, err={}", ret);
        }
    }
    return Status::OK();
}

// open_load_stream
Status LoadStreamStub::open(BrpcClientCache<PBackendService_Stub>* client_cache,
                            const NodeInfo& node_info, int64_t txn_id,
                            const OlapTableSchemaParam& schema,
                            const std::vector<PTabletID>& tablets_for_schema, int total_streams,
                            bool enable_profile) {
    std::unique_lock<bthread::Mutex> lock(_mutex);
    if (_is_init.load()) {
        return Status::OK();
    }
    _dst_id = node_info.id;
    _handler.set_dst_id(_dst_id);
    _handler.set_load_id(_load_id);
    std::string host_port = get_host_port(node_info.host, node_info.brpc_port);
    brpc::StreamOptions opt;
    opt.max_buf_size = config::load_stream_max_buf_size;
    opt.idle_timeout_ms = config::load_stream_idle_timeout_ms;
    opt.messages_in_batch = config::load_stream_messages_in_batch;
    opt.handler = &_handler;
    brpc::Controller cntl;
    if (int ret = StreamCreate(&_stream_id, cntl, &opt)) {
        return Status::Error<true>(ret, "Failed to create stream");
    }
    cntl.set_timeout_ms(config::open_load_stream_timeout_ms);
    POpenLoadStreamRequest request;
    *request.mutable_load_id() = _load_id;
    request.set_src_id(_src_id);
    request.set_txn_id(txn_id);
    request.set_enable_profile(enable_profile);
    request.set_total_streams(total_streams);
    schema.to_protobuf(request.mutable_schema());
    for (auto& tablet : tablets_for_schema) {
        *request.add_tablets() = tablet;
    }
    POpenLoadStreamResponse response;
    // use "pooled" connection to avoid conflicts between streaming rpc and regular rpc,
    // see: https://github.com/apache/brpc/issues/392
    const auto& stub = client_cache->get_new_client_no_cache(host_port, "baidu_std", "pooled");
    stub->open_load_stream(&cntl, &request, &response, nullptr);
    for (const auto& resp : response.tablet_schemas()) {
        auto tablet_schema = std::make_unique<TabletSchema>();
        tablet_schema->init_from_pb(resp.tablet_schema());
        _tablet_schema_for_index->emplace(resp.index_id(), std::move(tablet_schema));
        _enable_unique_mow_for_index->emplace(resp.index_id(),
                                              resp.enable_unique_key_merge_on_write());
    }
    if (cntl.Failed()) {
        return Status::InternalError("Failed to connect to backend {}: {}", _dst_id,
                                     cntl.ErrorText());
    }
    LOG(INFO) << "open load stream " << _stream_id << " load_id=" << print_id(_load_id)
              << " for backend " << _dst_id << " (" << host_port << ")";
    _is_init.store(true);
    return Status::OK();
}

// APPEND_DATA
Status LoadStreamStub::append_data(int64_t partition_id, int64_t index_id, int64_t tablet_id,
                                   int64_t segment_id, uint64_t offset, std::span<const Slice> data,
                                   bool segment_eos) {
    PStreamHeader header;
    header.set_src_id(_src_id);
    *header.mutable_load_id() = _load_id;
    header.set_partition_id(partition_id);
    header.set_index_id(index_id);
    header.set_tablet_id(tablet_id);
    header.set_segment_id(segment_id);
    header.set_segment_eos(segment_eos);
    header.set_offset(offset);
    header.set_opcode(doris::PStreamHeader::APPEND_DATA);
    return _encode_and_send(header, data);
}

// ADD_SEGMENT
Status LoadStreamStub::add_segment(int64_t partition_id, int64_t index_id, int64_t tablet_id,
                                   int64_t segment_id, const SegmentStatistics& segment_stat,
                                   TabletSchemaSPtr flush_schema) {
    PStreamHeader header;
    header.set_src_id(_src_id);
    *header.mutable_load_id() = _load_id;
    header.set_partition_id(partition_id);
    header.set_index_id(index_id);
    header.set_tablet_id(tablet_id);
    header.set_segment_id(segment_id);
    header.set_opcode(doris::PStreamHeader::ADD_SEGMENT);
    segment_stat.to_pb(header.mutable_segment_statistics());
    if (flush_schema != nullptr) {
        flush_schema->to_schema_pb(header.mutable_flush_schema());
    }
    return _encode_and_send(header);
}

// CLOSE_LOAD
Status LoadStreamStub::close_load(const std::vector<PTabletID>& tablets_to_commit) {
    {
        std::lock_guard<std::mutex> lock(_tablets_to_commit_mutex);
        _tablets_to_commit.insert(_tablets_to_commit.end(), tablets_to_commit.begin(),
                                  tablets_to_commit.end());
    }
    if (--_use_cnt > 0) {
        return Status::OK();
    }
    PStreamHeader header;
    *header.mutable_load_id() = _load_id;
    header.set_src_id(_src_id);
    header.set_opcode(doris::PStreamHeader::CLOSE_LOAD);
    {
        std::lock_guard<std::mutex> lock(_tablets_to_commit_mutex);
        for (const auto& tablet : _tablets_to_commit) {
            *header.add_tablets() = tablet;
        }
    }
    return _encode_and_send(header);
}

// GET_SCHEMA
Status LoadStreamStub::get_schema(const std::vector<PTabletID>& tablets) {
    PStreamHeader header;
    *header.mutable_load_id() = _load_id;
    header.set_src_id(_src_id);
    header.set_opcode(doris::PStreamHeader::CLOSE_LOAD);
    std::ostringstream oss;
    oss << "fetching tablet schema from stream " << _stream_id
        << ", load id: " << print_id(_load_id) << ", tablet id:";
    for (const auto& tablet : tablets) {
        *header.add_tablets() = tablet;
        oss << " " << tablet.tablet_id();
    }
    if (tablets.size() == 0) {
        oss << " none";
    }
    LOG(INFO) << oss.str();
    return _encode_and_send(header);
}

void LoadStreamStub::add_schema(const std::vector<PTabletSchemaWithIndex>& schemas) {
    std::lock_guard<bthread::Mutex> lock(_mutex);
    for (const auto& schema : schemas) {
        auto tablet_schema = std::make_unique<TabletSchema>();
        tablet_schema->init_from_pb(schema.tablet_schema());
        _tablet_schema_for_index->emplace(schema.index_id(), std::move(tablet_schema));
        _enable_unique_mow_for_index->emplace(schema.index_id(),
                                              schema.enable_unique_key_merge_on_write());
    }
    _schema_cv.notify_all();
}

Status LoadStreamStub::wait_for_schema(int64_t partition_id, int64_t index_id, int64_t tablet_id,
                                       int64_t timeout_ms) {
    if (_tablet_schema_for_index->contains(index_id)) {
        return Status::OK();
    }
    PTabletID tablet;
    tablet.set_partition_id(partition_id);
    tablet.set_index_id(index_id);
    tablet.set_tablet_id(tablet_id);
    RETURN_IF_ERROR(get_schema({tablet}));

    MonotonicStopWatch watch;
    watch.start();
    while (!_tablet_schema_for_index->contains(index_id) &&
           watch.elapsed_time() / 1000 / 1000 < timeout_ms) {
        static_cast<void>(wait_for_new_schema(100));
    }

    if (!_tablet_schema_for_index->contains(index_id)) {
        return Status::TimedOut("timeout to get tablet schema for index {}", index_id);
    }
    return Status::OK();
}

Status LoadStreamStub::_encode_and_send(PStreamHeader& header, std::span<const Slice> data) {
    butil::IOBuf buf;
    size_t header_len = header.ByteSizeLong();
    buf.append(reinterpret_cast<uint8_t*>(&header_len), sizeof(header_len));
    buf.append(header.SerializeAsString());
    size_t data_len = std::transform_reduce(data.begin(), data.end(), 0, std::plus(),
                                            [](const Slice& s) { return s.get_size(); });
    buf.append(reinterpret_cast<uint8_t*>(&data_len), sizeof(data_len));
    for (const auto& slice : data) {
        buf.append(slice.get_data(), slice.get_size());
    }
    bool eos = header.opcode() == doris::PStreamHeader::CLOSE_LOAD;
    bool get_schema = header.opcode() == doris::PStreamHeader::GET_SCHEMA;
    return _send_with_buffer(buf, eos || get_schema);
}

Status LoadStreamStub::_send_with_buffer(butil::IOBuf& buf, bool sync) {
    butil::IOBuf output;
    std::unique_lock<decltype(_buffer_mutex)> buffer_lock(_buffer_mutex);
    _buffer.append(buf);
    if (!sync && _buffer.size() < config::brpc_streaming_client_batch_bytes) {
        return Status::OK();
    }
    output.swap(_buffer);
    // acquire send lock while holding buffer lock, to ensure the message order
    std::lock_guard<decltype(_send_mutex)> send_lock(_send_mutex);
    buffer_lock.unlock();
    VLOG_DEBUG << "send buf size : " << output.size() << ", sync: " << sync;
    return _send_with_retry(output);
}

Status LoadStreamStub::_send_with_retry(butil::IOBuf& buf) {
    for (;;) {
        int ret;
        {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
            ret = brpc::StreamWrite(_stream_id, buf);
        }
        DBUG_EXECUTE_IF("LoadStreamStub._send_with_retry.stream_write_failed", { ret = EPIPE; });
        switch (ret) {
        case 0:
            return Status::OK();
        case EAGAIN: {
            const timespec time = butil::seconds_from_now(config::load_stream_eagain_wait_seconds);
            int wait_ret = brpc::StreamWait(_stream_id, &time);
            if (wait_ret != 0) {
                return Status::InternalError("StreamWait failed, err={}", wait_ret);
            }
            break;
        }
        default:
            return Status::InternalError("StreamWrite failed, err={}", ret);
        }
    }
}

} // namespace doris
