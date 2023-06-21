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

#include <fstream>
#include <iostream>
#include <brpc/stream.h>
#include "butil/iobuf.h"
#include "util/threadpool.h"
#include "olap/olap_common.h"
#include <gen_cpp/internal_service.pb.h>

namespace doris {
using namespace brpc;
using StreamIdPtr = std::shared_ptr<StreamId>;

// locate a rowset
struct TargetRowset {
    UniqueId loadid;
    int64_t indexid;
    int64_t tabletid;
    RowsetId rowsetid; // TODO probably not needed

    std::string to_string();
};
using TargetRowsetPtr = std::shared_ptr<TargetRowset>;
struct TargetRowsetComparator {
    bool operator()(const TargetRowsetPtr& lhs, const TargetRowsetPtr& rhs) const;
};

// locate a segment file
struct TargetSegment {
    TargetRowsetPtr target_rowset;
    int64_t segmentid;
    // std::string ip_port or BE id?

    std::string to_string();
};
using TargetSegmentPtr = std::shared_ptr<TargetSegment>;
struct TargetSegmentComparator {
    bool operator()(const TargetSegmentPtr& lhs, const TargetSegmentPtr& rhs) const;
};

class SinkStreamHandler : public StreamInputHandler {
public:
    SinkStreamHandler();
    ~SinkStreamHandler();

    int on_received_messages(StreamId id, butil::IOBuf *const messages[], size_t size) override;
    void on_idle_timeout(StreamId id) override;
    void on_closed(StreamId id) override;

private:
    void _handle_message(StreamId id, PStreamHeader hdr, TargetRowsetPtr rowset,
                         TargetSegmentPtr segment, std::shared_ptr<butil::IOBuf> message);
    void _parse_header(butil::IOBuf *const message, PStreamHeader& hdr);
    Status _create_and_open_file(TargetSegmentPtr target_segment, std::string path);
    Status _append_data(TargetSegmentPtr target_segment, std::shared_ptr<butil::IOBuf> message);
    Status _close_file(TargetSegmentPtr target_segment, bool is_last_segment);
    void _report_status(StreamId stream, TargetRowsetPtr target_rowset, int is_success, std::string error_msg);
    uint64_t get_next_segmentid(TargetRowsetPtr target_rowset, int64_t segmentid, bool is_open);
    Status _build_rowset(TargetRowsetPtr target_rowset, const RowsetMetaPB& rowset_meta);

private:
    std::unique_ptr<ThreadPool> _workers;
    // TODO: make it per load
    std::map<TargetSegmentPtr, std::shared_ptr<std::ofstream>, TargetSegmentComparator> _file_map;
    std::mutex _file_map_lock;
    // TODO: make it per load
    std::map<TargetRowsetPtr, size_t, TargetRowsetComparator> _tablet_segment_next_id;
    std::mutex _tablet_segment_next_id_lock;
    // TODO: make it per load
    std::map<TargetSegmentPtr, std::shared_ptr<ThreadPoolToken>> _segment_token_map; // accessed in single thread, safe
};

// managing stream_id allocation and release
class SinkStreamMgr {
public:
    SinkStreamMgr();
    ~SinkStreamMgr();

    StreamIdPtr get_free_stream_id();
    void release_stream_id(StreamIdPtr id);
    SinkStreamHandler* get_sink_stream_handler() { return _handler.get(); };

private:
    std::vector<StreamIdPtr> _free_stream_ids;
    std::mutex _lock;
    std::shared_ptr<SinkStreamHandler> _handler;
};

}
