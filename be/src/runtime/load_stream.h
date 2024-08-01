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

#include <bthread/mutex.h>
#include <gen_cpp/olap_common.pb.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "brpc/stream.h"
#include "butil/iobuf.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/load_stream_writer.h"
#include "util/runtime_profile.h"

namespace doris {

class LoadStreamMgr;
class ThreadPoolToken;
class OlapTableSchemaParam;

// origin_segid(index) -> new_segid(value in vector)
using SegIdMapping = std::vector<uint32_t>;
using FailedTablets = std::vector<std::pair<int64_t, Status>>;
class TabletStream {
public:
    TabletStream(PUniqueId load_id, int64_t id, int64_t txn_id, LoadStreamMgr* load_stream_mgr,
                 RuntimeProfile* profile);

    Status init(std::shared_ptr<OlapTableSchemaParam> schema, int64_t index_id,
                int64_t partition_id);

    Status append_data(const PStreamHeader& header, butil::IOBuf* data);
    Status add_segment(const PStreamHeader& header, butil::IOBuf* data);
    void add_num_segments(int64_t num_segments) { _num_segments += num_segments; }
    Status close();
    int64_t id() const { return _id; }

    friend std::ostream& operator<<(std::ostream& ostr, const TabletStream& tablet_stream);

private:
    int64_t _id;
    LoadStreamWriterSharedPtr _load_stream_writer;
    std::vector<std::unique_ptr<ThreadPoolToken>> _flush_tokens;
    std::unordered_map<int64_t, std::unique_ptr<SegIdMapping>> _segids_mapping;
    std::atomic<uint32_t> _next_segid;
    int64_t _num_segments = 0;
    bthread::Mutex _lock;
    std::shared_ptr<Status> _failed_st;
    PUniqueId _load_id;
    int64_t _txn_id;
    RuntimeProfile* _profile = nullptr;
    RuntimeProfile::Counter* _append_data_timer = nullptr;
    RuntimeProfile::Counter* _add_segment_timer = nullptr;
    RuntimeProfile::Counter* _close_wait_timer = nullptr;
    LoadStreamMgr* _load_stream_mgr = nullptr;
};

using TabletStreamSharedPtr = std::shared_ptr<TabletStream>;

class IndexStream {
public:
    IndexStream(PUniqueId load_id, int64_t id, int64_t txn_id,
                std::shared_ptr<OlapTableSchemaParam> schema, LoadStreamMgr* load_stream_mgr,
                RuntimeProfile* profile);

    Status append_data(const PStreamHeader& header, butil::IOBuf* data);

    Status close(const std::vector<PTabletID>& tablets_to_commit,
                 std::vector<int64_t>* success_tablet_ids, FailedTablets* failed_tablet_ids);

private:
    Status _init_tablet_stream(TabletStreamSharedPtr& tablet_stream, int64_t tablet_id,
                               int64_t partition_id);

private:
    int64_t _id;
    std::unordered_map<int64_t /*tabletid*/, TabletStreamSharedPtr> _tablet_streams_map;
    bthread::Mutex _lock;
    PUniqueId _load_id;
    int64_t _txn_id;
    std::shared_ptr<OlapTableSchemaParam> _schema;
    std::unordered_map<int64_t, int64_t> _tablet_partitions;
    RuntimeProfile* _profile = nullptr;
    RuntimeProfile::Counter* _append_data_timer = nullptr;
    RuntimeProfile::Counter* _close_wait_timer = nullptr;
    LoadStreamMgr* _load_stream_mgr = nullptr;
};
using IndexStreamSharedPtr = std::shared_ptr<IndexStream>;

using StreamId = brpc::StreamId;
class LoadStream : public brpc::StreamInputHandler {
public:
    LoadStream(PUniqueId load_id, LoadStreamMgr* load_stream_mgr, bool enable_profile);
    ~LoadStream() override;

    Status init(const POpenLoadStreamRequest* request);

    void add_source(int64_t src_id) {
        std::lock_guard lock_guard(_lock);
        _open_streams[src_id]++;
        if (_is_incremental) {
            _total_streams++;
        }
    }

    Status close(int64_t src_id, const std::vector<PTabletID>& tablets_to_commit,
                 std::vector<int64_t>* success_tablet_ids, FailedTablets* failed_tablet_ids);

    // callbacks called by brpc
    int on_received_messages(StreamId id, butil::IOBuf* const messages[], size_t size) override;
    void on_idle_timeout(StreamId id) override;
    void on_closed(StreamId id) override;

    friend std::ostream& operator<<(std::ostream& ostr, const LoadStream& load_stream);

private:
    void _parse_header(butil::IOBuf* const message, PStreamHeader& hdr);
    void _dispatch(StreamId id, const PStreamHeader& hdr, butil::IOBuf* data);
    Status _append_data(const PStreamHeader& header, butil::IOBuf* data);

    void _report_result(StreamId stream, const Status& status,
                        const std::vector<int64_t>& success_tablet_ids,
                        const FailedTablets& failed_tablets, bool eos);
    void _report_schema(StreamId stream, const PStreamHeader& hdr);

    // report failure for one message
    void _report_failure(StreamId stream, const Status& status, const PStreamHeader& header) {
        FailedTablets failed_tablets;
        if (header.has_tablet_id()) {
            failed_tablets.emplace_back(header.tablet_id(), status);
        }
        _report_result(stream, status, {}, failed_tablets, false);
    }

    Status _write_stream(StreamId stream, butil::IOBuf& buf);

private:
    PUniqueId _load_id;
    std::unordered_map<int64_t, IndexStreamSharedPtr> _index_streams_map;
    int32_t _total_streams = 0;
    int32_t _close_load_cnt = 0;
    std::atomic<int32_t> _close_rpc_cnt = 0;
    std::vector<PTabletID> _tablets_to_commit;
    bthread::Mutex _lock;
    std::unordered_map<int64_t, int32_t> _open_streams;
    int64_t _txn_id = 0;
    std::shared_ptr<OlapTableSchemaParam> _schema;
    bool _enable_profile = false;
    std::unique_ptr<RuntimeProfile> _profile;
    RuntimeProfile::Counter* _append_data_timer = nullptr;
    RuntimeProfile::Counter* _close_wait_timer = nullptr;
    LoadStreamMgr* _load_stream_mgr = nullptr;
    QueryThreadContext _query_thread_context;
    bool _is_incremental = false;
};

using LoadStreamPtr = std::unique_ptr<LoadStream>;

} // namespace doris
