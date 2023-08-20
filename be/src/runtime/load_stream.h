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

#include <gen_cpp/internal_service.pb.h>
#include <stdint.h>

#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include <runtime/load_stream_writer.h>

#include "brpc/stream.h"
#include "butil/iobuf.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "util/runtime_profile.h"

namespace doris {

// origin_segid(index) -> new_segid(value in vector)
using SegIdMapping = std::vector<uint32_t>;
class TabletStream {
public:
    TabletStream(PUniqueId load_id, int64_t id, int64_t txn_id, uint32_t num_senders,
                 RuntimeProfile* profile);

    Status init(OlapTableSchemaParam* schema, int64_t index_id, int64_t partition_id);

    Status append_data(const PStreamHeader& header, butil::IOBuf* data);
    Status add_segment(const PStreamHeader& header, butil::IOBuf* data);
    Status close();
    int64_t id() { return _id; }

    friend std::ostream& operator<<(std::ostream& ostr, const TabletStream& tablet_stream);

private:
    int64_t _id;
    LoadStreamWriterSharedPtr _load_stream_writer;
    std::vector<std::unique_ptr<ThreadPoolToken>> _flush_tokens;
    std::vector<SegIdMapping> _segids_mapping;
    std::atomic<uint32_t> _next_segid;
    bthread::Mutex _lock;
    std::shared_ptr<Status> _failed_st;
    PUniqueId _load_id;
    int64_t _txn_id;
    RuntimeProfile* _profile;
    RuntimeProfile::Counter* _append_data_timer = nullptr;
    RuntimeProfile::Counter* _add_segment_timer = nullptr;
    RuntimeProfile::Counter* _close_wait_timer = nullptr;
};

using TabletStreamSharedPtr = std::shared_ptr<TabletStream>;

class IndexStream {
public:
    IndexStream(PUniqueId load_id, int64_t id, int64_t txn_id, uint32_t num_senders,
                std::shared_ptr<OlapTableSchemaParam> schema, RuntimeProfile* profile);

    Status append_data(const PStreamHeader& header, butil::IOBuf* data);

    void flush(uint32_t sender_id);
    Status close(std::vector<int64_t>* success_tablet_ids, std::vector<int64_t>* failed_tablet_ids,
               std::vector<PTabletWithPartition>* need_commit_tablet_info);

private:
    int64_t _id;
    uint32_t _num_senders;
    std::unordered_map<int64_t /*tabletid*/, TabletStreamSharedPtr> _tablet_streams_map;
    bthread::Mutex _lock;
    PUniqueId _load_id;
    int64_t _txn_id;
    std::shared_ptr<OlapTableSchemaParam> _schema;
    std::unordered_map<int64_t, int64_t> _tablet_partitions;
    std::vector<int64_t> _failed_tablet_ids;
    RuntimeProfile* _profile;
    RuntimeProfile::Counter* _append_data_timer = nullptr;
    RuntimeProfile::Counter* _close_wait_timer = nullptr;
};
using IndexStreamSharedPtr = std::shared_ptr<IndexStream>;

using StreamId = brpc::StreamId;
class LoadStream : public brpc::StreamInputHandler {
public:
    LoadStream(PUniqueId id);
    ~LoadStream();

    Status init(const POpenStreamSinkRequest* request);

    uint32_t add_rpc_stream() { return ++_num_rpc_streams; }
    uint32_t remove_rpc_stream() { return --_num_rpc_streams; }

    Status close(uint32_t sender_id, std::vector<int64_t>* success_tablet_ids,
                 std::vector<int64_t>* failed_tablet_ids,
                 std::vector<PTabletWithPartition>* need_commit_tablet_info);

    // callbacks called by brpc
    int on_received_messages(StreamId id, butil::IOBuf* const messages[], size_t size) override;
    void on_idle_timeout(StreamId id) override;
    void on_closed(StreamId id) override;

    friend std::ostream& operator<<(std::ostream& ostr, const LoadStream& load_stream);

private:
    void _parse_header(butil::IOBuf* const message, PStreamHeader& hdr);
    Status _append_data(const PStreamHeader& header, butil::IOBuf* data);
    void _report_result(StreamId stream, Status& st, std::vector<int64_t>* success_tablet_ids,
                        std::vector<int64_t>* failed_tablet_ids);

private:
    PUniqueId _id;
    std::unordered_map<int64_t, IndexStreamSharedPtr> _index_streams_map;
    std::atomic<uint32_t> _num_rpc_streams;
    std::vector<bool> _senders_status;
    std::vector<uint32_t> _senders_closed_streams;
    bthread::Mutex _lock;
    uint32_t _num_working_senders;
    uint32_t _num_stream_per_sender;
    uint32_t _num_senders;
    int64_t _txn_id;
    std::shared_ptr<OlapTableSchemaParam> _schema;
    std::set<int64_t> _failed_tablet_ids;
    std::unique_ptr<RuntimeProfile> _profile;
    RuntimeProfile::Counter* _append_data_timer = nullptr;
    RuntimeProfile::Counter* _close_wait_timer = nullptr;
};

using LoadStreamSharedPtr = std::shared_ptr<LoadStream>;

} // namespace doris
