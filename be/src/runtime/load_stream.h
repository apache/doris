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
#include <runtime/rowset_builder.h>

#include "brpc/stream.h"
#include "butil/iobuf.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"

namespace doris {

// origin_segid(index) -> new_segid(value in vector)
using SegIdMapping = std::vector<uint32_t>;
class TabletStream {
public:
    TabletStream(PUniqueId load_id, int64_t id, int64_t txn_id, uint32_t num_senders);

    Status init(OlapTableSchemaParam* schema, int64_t index_id, int64_t partition_id);

    Status append_data(const PStreamHeader& header, butil::IOBuf* data);
    Status add_segment(const PStreamHeader& header, butil::IOBuf* data);
    Status close();
    int64_t id() { return _id; }

    friend std::ostream& operator<<(std::ostream& ostr, const TabletStream& tablet_stream);

private:
    int64_t _id;
    RowsetBuilderSharedPtr _rowset_builder;
    std::vector<std::unique_ptr<ThreadPoolToken>> _flush_tokens;
    std::vector<SegIdMapping> _segids_mapping;
    std::atomic<uint32_t> _next_segid;
    bthread::Mutex _lock;
    std::shared_ptr<Status> _failed_st;
    PUniqueId _load_id;
    int64_t _txn_id;
};

using TabletStreamSharedPtr = std::shared_ptr<TabletStream>;

class IndexStream {
public:
    IndexStream(PUniqueId load_id, int64_t id, int64_t txn_id, uint32_t num_senders,
                std::shared_ptr<OlapTableSchemaParam> schema)
            : _id(id),
              _num_senders(num_senders),
              _load_id(load_id),
              _txn_id(txn_id),
              _schema(schema) {}

    Status append_data(const PStreamHeader& header, butil::IOBuf* data);

    void flush(uint32_t sender_id);
    void close(std::vector<int64_t>* success_tablet_ids, std::vector<int64_t>* failed_tablet_ids);

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
                 std::vector<int64_t>* failed_tablet_ids);

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
};

using LoadStreamSharedPtr = std::shared_ptr<LoadStream>;

} // namespace doris
