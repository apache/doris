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
#include <brpc/controller.h>
#include <bthread/types.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <parallel_hashmap/phmap.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <set>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/tablet_info.h"
#include "gutil/ref_counted.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "util/countdown_latch.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/columns/column.h"
#include "vec/common/allocator.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class TabletSchema;
class LoadStreamStub;

struct SegmentStatistics;

using IndexToTabletSchema = phmap::parallel_flat_hash_map<
        int64_t, std::shared_ptr<TabletSchema>, std::hash<int64_t>, std::equal_to<int64_t>,
        std::allocator<phmap::Pair<const int64_t, std::shared_ptr<TabletSchema>>>, 4, std::mutex>;

using IndexToEnableMoW =
        phmap::parallel_flat_hash_map<int64_t, bool, std::hash<int64_t>, std::equal_to<int64_t>,
                                      std::allocator<phmap::Pair<const int64_t, bool>>, 4,
                                      std::mutex>;

class LoadStreamStub {
private:
    class LoadStreamReplyHandler : public brpc::StreamInputHandler {
    public:
        LoadStreamReplyHandler(LoadStreamStub* stub) : _stub(stub) {}

        int on_received_messages(brpc::StreamId id, butil::IOBuf* const messages[],
                                 size_t size) override;

        void on_idle_timeout(brpc::StreamId id) override {}

        void on_closed(brpc::StreamId id) override;

        bool is_closed() { return _is_closed.load(); }

        Status close_wait(int64_t timeout_ms) {
            std::unique_lock<bthread::Mutex> lock(_mutex);
            if (_is_closed) {
                return Status::OK();
            }
            if (timeout_ms > 0) {
                int ret = _close_cv.wait_for(lock, timeout_ms * 1000);
                return ret == 0 ? Status::OK()
                                : Status::Error<true>(ret, "stream close_wait timeout");
            }
            _close_cv.wait(lock);
        };

    private:
        std::atomic<bool> _is_closed;
        bthread::Mutex _mutex;
        bthread::ConditionVariable _close_cv;
    };

public:
    // construct new stub
    LoadStreamStub(PUniqueId load_id, int64_t src_id);

    // copy constructor, shared_ptr members are shared
    LoadStreamStub(LoadStreamStub& stub);

// for mock this class in UT
#ifdef BE_TEST
    virtual
#endif
            ~LoadStreamStub();

    // open_stream_sink
    Status open(BrpcClientCache<PBackendService_Stub>* client_cache, const NodeInfo& node_info,
                int64_t txn_id, const OlapTableSchemaParam& schema,
                const std::vector<PTabletID>& tablets_for_schema, bool enable_profile);

// for mock this class in UT
#ifdef BE_TEST
    virtual
#endif
            // APPEND_DATA
            Status
            append_data(int64_t partition_id, int64_t index_id, int64_t tablet_id,
                        int64_t segment_id, std::span<const Slice> data, bool segment_eos = false);

    // ADD_SEGMENT
    Status add_segment(int64_t partition_id, int64_t index_id, int64_t tablet_id,
                       int64_t segment_id, SegmentStatistics& segment_stat);

    // CLOSE_LOAD
    Status close_load(const std::vector<PTabletID>& tablets_to_commit);

    // wait remote to close stream,
    // remote will close stream when it receives CLOSE_LOAD
    Status close_wait(int64_t timeout_ms = 0) {
        if (!_is_init.load() || _handler.is_closed()) {
            return Status::OK();
        }
        return _handler.close_wait(timeout_ms);
    }

    std::shared_ptr<TabletSchema> tablet_schema(int64_t index_id) const {
        return _tablet_schema_for_index->at(index_id);
    }

    bool enable_unique_mow(int64_t index_id) const {
        return _enable_unique_mow_for_index->at(index_id);
    }

    std::vector<int64_t> success_tablets() {
        std::lock_guard<bthread::Mutex> lock(_success_tablets_mutex);
        return _success_tablets;
    }

    std::vector<int64_t> failed_tablets() {
        std::lock_guard<bthread::Mutex> lock(_failed_tablets_mutex);
        return _failed_tablets;
    }

    brpc::StreamId stream_id() const { return _stream_id; }

    int64_t src_id() const { return _src_id; }

    int64_t dst_id() const { return _dst_id; }

private:
    Status _encode_and_send(PStreamHeader& header, std::span<const Slice> data = {});
    Status _send_with_buffer(butil::IOBuf& buf, bool eos = false);
    Status _send_with_retry(butil::IOBuf& buf);

protected:
    std::atomic<bool> _is_init;
    bthread::Mutex _mutex;

    std::atomic<int> _num_open;

    std::mutex _buffer_mutex;
    std::mutex _send_mutex;
    butil::IOBuf _buffer;

    PUniqueId _load_id;
    brpc::StreamId _stream_id;
    int64_t _src_id = -1; // source backend_id
    int64_t _dst_id = -1; // destination backend_id
    LoadStreamReplyHandler _handler {this};

    bthread::Mutex _success_tablets_mutex;
    bthread::Mutex _failed_tablets_mutex;
    std::vector<int64_t> _success_tablets;
    std::vector<int64_t> _failed_tablets;

    std::shared_ptr<IndexToTabletSchema> _tablet_schema_for_index;
    std::shared_ptr<IndexToEnableMoW> _enable_unique_mow_for_index;
};

} // namespace doris
