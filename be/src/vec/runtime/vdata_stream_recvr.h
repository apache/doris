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

#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "runtime/query_statistics.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/materialize_block.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
class MemTracker;
class PBlock;
class MemTrackerLimiter;
class PQueryStatistics;
class RuntimeState;

namespace pipeline {
struct ExchangeDataDependency;
class LocalExchangeChannelDependency;
class LocalExchangeMemLimitDependency;
class ExchangeLocalState;
} // namespace pipeline

namespace vectorized {
class VDataStreamMgr;
class VSortedRunMerger;

class VDataStreamRecvr;

class VDataStreamRecvr {
public:
    class SenderQueue;
    VDataStreamRecvr(VDataStreamMgr* stream_mgr, RuntimeState* state, const RowDescriptor& row_desc,
                     const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
                     int num_senders, bool is_merging, RuntimeProfile* profile,
                     std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr);

    virtual ~VDataStreamRecvr();

    Status create_merger(const VExprContextSPtrs& ordering_expr,
                         const std::vector<bool>& is_asc_order,
                         const std::vector<bool>& nulls_first, size_t batch_size, int64_t limit,
                         size_t offset);

    std::vector<SenderQueue*> sender_queues() const { return _sender_queues; }

    Status add_block(const PBlock& pblock, int sender_id, int be_number, int64_t packet_seq,
                     ::google::protobuf::Closure** done);

    void add_block(Block* block, int sender_id, bool use_move);

    bool sender_queue_empty(int sender_id);

    bool ready_to_read();

    Status get_next(Block* block, bool* eos);

    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    PlanNodeId dest_node_id() const { return _dest_node_id; }
    const RowDescriptor& row_desc() const { return _row_desc; }

    void add_sub_plan_statistics(const PQueryStatistics& statistics, int sender_id) {
        _sub_plan_query_statistics_recvr->insert(statistics, sender_id);
    }

    // Indicate that a particular sender is done. Delegated to the appropriate
    // sender queue. Called from DataStreamMgr.
    void remove_sender(int sender_id, int be_number, Status exec_status);

    void remove_sender(int sender_id, int be_number, QueryStatisticsPtr statistics,
                       Status exec_status);

    void cancel_stream(Status exec_status);

    void close();

    // Careful: stream sender will call this function for a local receiver,
    // accessing members of receiver that are allocated by Object pool
    // in this function is not safe.
    bool exceeds_limit(int batch_size) {
        return _blocks_memory_usage_current_value + batch_size >
               config::exchg_node_buffer_size_bytes;
    }

    bool is_closed() const { return _is_closed; }

    std::shared_ptr<pipeline::LocalExchangeChannelDependency> get_local_channel_dependency(
            int sender_id);

    void create_mem_limit_dependency(int id, int node_id, QueryContext* query_ctx);

    auto get_mem_limit_dependency() { return _exchange_sink_mem_limit_dependency; }

private:
    void update_blocks_memory_usage(int64_t size);
    class PipSenderQueue;

    friend struct BlockSupplierSortCursorImpl;

    // DataStreamMgr instance used to create this recvr. (Not owned)
    VDataStreamMgr* _mgr = nullptr;

#ifdef USE_MEM_TRACKER
    std::shared_ptr<MemTrackerLimiter> _query_mem_tracker = nullptr;
    TUniqueId _query_id;
#endif

    // Fragment and node id of the destination exchange node this receiver is used by.
    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // Row schema, copied from the caller of CreateRecvr().
    RowDescriptor _row_desc;

    // True if this reciver merges incoming rows from different senders. Per-sender
    // row batch queues are maintained in this case.
    bool _is_merging;
    bool _is_closed;

    std::unique_ptr<MemTracker> _mem_tracker;
    // Managed by object pool
    std::vector<SenderQueue*> _sender_queues;

    std::unique_ptr<VSortedRunMerger> _merger;

    ObjectPool _sender_queue_pool;
    RuntimeProfile* _profile = nullptr;

    RuntimeProfile::Counter* _bytes_received_counter = nullptr;
    RuntimeProfile::Counter* _local_bytes_received_counter = nullptr;
    RuntimeProfile::Counter* _deserialize_row_batch_timer = nullptr;
    RuntimeProfile::Counter* _first_batch_wait_total_timer = nullptr;
    RuntimeProfile::Counter* _buffer_full_total_timer = nullptr;
    RuntimeProfile::Counter* _data_arrival_timer = nullptr;
    RuntimeProfile::Counter* _decompress_timer = nullptr;
    RuntimeProfile::Counter* _decompress_bytes = nullptr;
    RuntimeProfile::Counter* _memory_usage_counter = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _blocks_memory_usage = nullptr;
    std::atomic<int64_t> _blocks_memory_usage_current_value = 0;
    RuntimeProfile::Counter* _peak_memory_usage_counter = nullptr;

    // Number of rows received
    RuntimeProfile::Counter* _rows_produced_counter = nullptr;
    // Number of blocks received
    RuntimeProfile::Counter* _blocks_produced_counter = nullptr;

    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr;

    bool _enable_pipeline;
    std::vector<std::shared_ptr<pipeline::LocalExchangeChannelDependency>>
            _sender_to_local_channel_dependency;

    // use to limit sink write
    std::shared_ptr<pipeline::LocalExchangeMemLimitDependency> _exchange_sink_mem_limit_dependency;
};

class ThreadClosure : public google::protobuf::Closure {
public:
    void Run() override { _cv.notify_one(); }
    void wait(std::unique_lock<std::mutex>& lock) { _cv.wait(lock); }

private:
    std::condition_variable _cv;
};

class VDataStreamRecvr::SenderQueue {
public:
    SenderQueue(VDataStreamRecvr* parent_recvr, int num_senders, RuntimeProfile* profile);

    virtual ~SenderQueue();

    void set_local_channel_dependency(
            std::shared_ptr<pipeline::LocalExchangeChannelDependency> local_channel_dependency) {
        _local_channel_dependency = local_channel_dependency;
    }

    virtual bool should_wait();

    virtual Status get_batch(Block* next_block, bool* eos);

    Status add_block(const PBlock& pblock, int be_number, int64_t packet_seq,
                     ::google::protobuf::Closure** done);

    virtual void add_block(Block* block, bool use_move);

    void decrement_senders(int sender_id);

    void cancel(Status cancel_status);

    void close();

    bool queue_empty() {
        std::unique_lock<std::mutex> l(_lock);
        return _block_queue.empty();
    }

    void set_dependency(std::shared_ptr<pipeline::ExchangeDataDependency> dependency) {
        _dependency = dependency;
    }

protected:
    friend class pipeline::ExchangeLocalState;
    friend struct pipeline::ExchangeDataDependency;
    Status _inner_get_batch_without_lock(Block* block, bool* eos);

    void try_set_dep_ready_without_lock();

    // To record information about several variables in the event of a DCHECK failure.
    //  DCHECK(_is_cancelled || !_block_queue.empty() || _num_remaining_senders == 0)
#ifndef NDEBUG
    constexpr static auto max_record_number = 128;
    std::list<size_t> _record_block_queue;
    std::list<int> _record_num_remaining_senders;
#else
#endif

    // only in debug
    ALWAYS_INLINE inline void _record_debug_info() {
#ifndef NDEBUG
        if (_record_block_queue.size() > max_record_number) {
            _record_block_queue.pop_front();
        }
        if (_record_num_remaining_senders.size() > max_record_number) {
            _record_num_remaining_senders.pop_front();
        }
        _record_block_queue.push_back(_block_queue.size());
        _record_num_remaining_senders.push_back(_num_remaining_senders);
#else
#endif
    }

    ALWAYS_INLINE inline std::string _debug_string_info() {
#ifndef NDEBUG
        std::stringstream out;
        DCHECK_EQ(_record_block_queue.size(), _record_num_remaining_senders.size());
        out << "record_debug_info [  \n";

        auto it1 = _record_block_queue.begin();
        auto it2 = _record_num_remaining_senders.begin();
        for (; it1 != _record_block_queue.end(); it1++, it2++) {
            out << "( "
                << "_block_queue size : " << *it1 << " , _num_remaining_senders : " << *it2
                << " ) \n";
        }
        out << "  ]\n";
        return out.str();
#else
#endif
        return "";
    }

    // Not managed by this class
    VDataStreamRecvr* _recvr = nullptr;
    std::mutex _lock;
    bool _is_cancelled;
    Status _cancel_status;
    int _num_remaining_senders;
    std::condition_variable _data_arrival_cv;
    std::condition_variable _data_removal_cv;
    std::list<std::pair<BlockUPtr, size_t>> _block_queue;

    bool _received_first_batch;
    // sender_id
    std::unordered_set<int> _sender_eos_set;
    // be_number => packet_seq
    std::unordered_map<int, int64_t> _packet_seq_map;
    std::deque<std::pair<google::protobuf::Closure*, MonotonicStopWatch>> _pending_closures;
    std::unordered_map<std::thread::id, std::unique_ptr<ThreadClosure>> _local_closure;

    std::shared_ptr<pipeline::ExchangeDataDependency> _dependency;
    std::shared_ptr<pipeline::LocalExchangeChannelDependency> _local_channel_dependency;
};

class VDataStreamRecvr::PipSenderQueue : public SenderQueue {
public:
    PipSenderQueue(VDataStreamRecvr* parent_recvr, int num_senders, RuntimeProfile* profile)
            : SenderQueue(parent_recvr, num_senders, profile) {}

    Status get_batch(Block* block, bool* eos) override {
        std::lock_guard<std::mutex> l(_lock); // protect _block_queue
        DCHECK(_is_cancelled || !_block_queue.empty() || _num_remaining_senders == 0)
                << " _is_cancelled: " << _is_cancelled
                << ", _block_queue_empty: " << _block_queue.empty()
                << ", _num_remaining_senders: " << _num_remaining_senders << "\n"
                << _debug_string_info();
        return _inner_get_batch_without_lock(block, eos);
    }

    void add_block(Block* block, bool use_move) override;
};

} // namespace vectorized
} // namespace doris
