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

#include <atomic>
#include <condition_variable>
#include <deque>
#include <list>
#include <thread>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/query_fragments_ctx.h"
#include "runtime/query_statistics.h"
#include "util/runtime_profile.h"

namespace google {
namespace protobuf {
class Closure;
}
} // namespace google

namespace doris {
class MemTracker;
class RuntimeProfile;
class PBlock;

namespace vectorized {
class Block;
class VDataStreamMgr;
class VSortedRunMerger;
class VExprContext;

class VDataStreamRecvr {
public:
    VDataStreamRecvr(VDataStreamMgr* stream_mgr, RuntimeState* state, const RowDescriptor& row_desc,
                     const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id,
                     int num_senders, bool is_merging, int total_buffer_limit,
                     RuntimeProfile* profile,
                     std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr);

    ~VDataStreamRecvr();

    Status create_merger(const std::vector<VExprContext*>& ordering_expr,
                         const std::vector<bool>& is_asc_order,
                         const std::vector<bool>& nulls_first, size_t batch_size, int64_t limit,
                         size_t offset);

    void add_block(const PBlock& pblock, int sender_id, int be_number, int64_t packet_seq,
                   ::google::protobuf::Closure** done);

    void add_block(Block* block, int sender_id, bool use_move);

    Status get_next(Block* block, bool* eos);

    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    PlanNodeId dest_node_id() const { return _dest_node_id; }
    const RowDescriptor& row_desc() const { return _row_desc; }

    void add_sub_plan_statistics(const PQueryStatistics& statistics, int sender_id) {
        _sub_plan_query_statistics_recvr->insert(statistics, sender_id);
    }

    // Indicate that a particular sender is done. Delegated to the appropriate
    // sender queue. Called from DataStreamMgr.
    void remove_sender(int sender_id, int be_number);

    void cancel_stream();

    void close();

private:
    class SenderQueue;
    friend struct ReceiveQueueSortCursorImpl;

    bool exceeds_limit(int batch_size) {
        return _num_buffered_bytes + batch_size > _total_buffer_limit;
    }

    // DataStreamMgr instance used to create this recvr. (Not owned)
    VDataStreamMgr* _mgr;

    RuntimeState* _state;

    // Fragment and node id of the destination exchange node this receiver is used by.
    TUniqueId _fragment_instance_id;
    PlanNodeId _dest_node_id;

    // soft upper limit on the total amount of buffering allowed for this stream across
    // all sender queues. we stop acking incoming data once the amount of buffered data
    // exceeds this value
    int _total_buffer_limit;

    // Row schema, copied from the caller of CreateRecvr().
    RowDescriptor _row_desc;

    // True if this reciver merges incoming rows from different senders. Per-sender
    // row batch queues are maintained in this case.
    bool _is_merging;
    bool _is_closed;

    std::atomic<int> _num_buffered_bytes;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::vector<SenderQueue*> _sender_queues;

    std::unique_ptr<VSortedRunMerger> _merger;

    ObjectPool _sender_queue_pool;
    RuntimeProfile* _profile;

    RuntimeProfile::Counter* _bytes_received_counter;
    RuntimeProfile::Counter* _deserialize_row_batch_timer;
    RuntimeProfile::Counter* _first_batch_wait_total_timer;
    RuntimeProfile::Counter* _buffer_full_total_timer;
    RuntimeProfile::Counter* _data_arrival_timer;
    RuntimeProfile::Counter* _decompress_timer;
    RuntimeProfile::Counter* _decompress_bytes;

    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr;
};

class ThreadClosure : public google::protobuf::Closure {
public:
    void Run() { _cv.notify_one(); }
    void wait(std::unique_lock<std::mutex>& lock) { _cv.wait(lock); }

private:
    std::condition_variable _cv;
};

class VDataStreamRecvr::SenderQueue {
public:
    SenderQueue(VDataStreamRecvr* parent_recvr, int num_senders, RuntimeProfile* profile);

    ~SenderQueue();

    Status get_batch(Block** next_block);

    void add_block(const PBlock& pblock, int be_number, int64_t packet_seq,
                   ::google::protobuf::Closure** done);

    void add_block(Block* block, bool use_move);

    void decrement_senders(int sender_id);

    void cancel();

    void close();

    Block* current_block() const { return _current_block.get(); }

private:
    VDataStreamRecvr* _recvr;
    std::mutex _lock;
    bool _is_cancelled;
    int _num_remaining_senders;
    std::condition_variable _data_arrival_cv;
    std::condition_variable _data_removal_cv;

    using VecBlockQueue = std::list<std::pair<int, Block*>>;
    VecBlockQueue _block_queue;

    std::unique_ptr<Block> _current_block;

    bool _received_first_batch;
    // sender_id
    std::unordered_set<int> _sender_eos_set;
    // be_number => packet_seq
    std::unordered_map<int, int64_t> _packet_seq_map;
    std::deque<std::pair<google::protobuf::Closure*, MonotonicStopWatch>> _pending_closures;
    std::unordered_map<std::thread::id, std::unique_ptr<ThreadClosure>> _local_closure;
};
} // namespace vectorized
} // namespace doris
