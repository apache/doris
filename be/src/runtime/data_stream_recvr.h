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

#ifndef DORIS_BE_SRC_RUNTIME_DATA_STREAM_RECVR_H
#define DORIS_BE_SRC_RUNTIME_DATA_STREAM_RECVR_H

#include <mutex>

#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/Types_types.h" // for TUniqueId
#include "runtime/descriptors.h"
#include "runtime/query_statistics.h"
#include "util/tuple_row_compare.h"

namespace google {
namespace protobuf {
class Closure;
}
} // namespace google

namespace doris {

class DataStreamMgr;
class SortedRunMerger;
class MemTracker;
class RowBatch;
class RuntimeProfile;
class PRowBatch;

// Single receiver of an m:n data stream.
// DataStreamRecvr maintains one or more queues of row batches received by a
// DataStreamMgr from one or more sender fragment instances.
// Receivers are created via DataStreamMgr::CreateRecvr().
// Ownership of a stream recvr is shared between the DataStreamMgr that created it and
// the caller of DataStreamMgr::CreateRecvr() (i.e. the exchange node)
//
// The _is_merging member determines if the recvr merges input streams from different
// sender fragment instances according to a specified sort order.
// If _is_merging = false : Only one batch queue is maintained for row batches from all
// sender fragment instances. These row batches are returned one at a time via
// get_batch().
// If _is_merging is true : One queue is created for the batches from each distinct
// sender. A SortedRunMerger instance must be created via create_merger() prior to
// retrieving any rows from the receiver. Rows are retrieved from the receiver via
// get_next(RowBatch* output_batch, int limit, bool eos). After the final call to
// get_next(), transfer_all_resources() must be called to transfer resources from the input
// batches from each sender to the caller's output batch.
// The receiver sets deep_copy to false on the merger - resources are transferred from
// the input batches from each sender queue to the merger to the output batch by the
// merger itself as it processes each run.
//
// DataStreamRecvr::close() must be called by the caller of CreateRecvr() to remove the
// recvr instance from the tracking structure of its DataStreamMgr in all cases.
class DataStreamRecvr {
public:
    ~DataStreamRecvr();

    // Returns next row batch in data stream; blocks if there aren't any.
    // Retains ownership of the returned batch. The caller must acquire data from the
    // returned batch before the next call to get_batch(). A nullptr returned batch indicated
    // eos. Must only be called if _is_merging is false.
    // TODO: This is currently only exposed to the non-merging version of the exchange.
    // Refactor so both merging and non-merging exchange use get_next(RowBatch*, bool* eos).
    Status get_batch(RowBatch** next_batch);

    void add_batch(RowBatch* batch, int sender_id, bool use_move);

    // Deregister from DataStreamMgr instance, which shares ownership of this instance.
    void close();

    // Create a SortedRunMerger instance to merge rows from multiple sender according to the
    // specified row comparator. Fetches the first batches from the individual sender
    // queues. The exprs used in less_than must have already been prepared and opened.
    Status create_merger(const TupleRowComparator& less_than);

    Status create_parallel_merger(const TupleRowComparator& less_than, uint32_t batch_size);
    // Fill output_batch with the next batch of rows obtained by merging the per-sender
    // input streams. Must only be called if _is_merging is true.
    Status get_next(RowBatch* output_batch, bool* eos);

    // Transfer all resources from the current batches being processed from each sender
    // queue to the specified batch.
    void transfer_all_resources(RowBatch* transfer_batch);

    const TUniqueId& fragment_instance_id() const { return _fragment_instance_id; }
    PlanNodeId dest_node_id() const { return _dest_node_id; }
    const RowDescriptor& row_desc() const { return _row_desc; }

    void add_sub_plan_statistics(const PQueryStatistics& statistics, int sender_id) {
        _sub_plan_query_statistics_recvr->insert(statistics, sender_id);
    }

    // Indicate that a particular sender is done. Delegated to the appropriate
    // sender queue. Called from DataStreamMgr.
    void remove_sender(int sender_id, int be_number);

private:
    friend class DataStreamMgr;
    class SenderQueue;

    DataStreamRecvr(DataStreamMgr* stream_mgr, const RowDescriptor& row_desc,
                    const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
                    bool is_merging, int total_buffer_limit, RuntimeProfile* profile,
                    std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr);

    // If receive queue is full, done is enqueue pending, and return with *done is nullptr
    void add_batch(const PRowBatch& batch, int sender_id, int be_number, int64_t packet_seq,
                   ::google::protobuf::Closure** done);

    // Empties the sender queues and notifies all waiting consumers of cancellation.
    void cancel_stream();

    // Return true if the addition of a new batch of size 'batch_size' would exceed the
    // total buffer limit.
    bool exceeds_limit(int batch_size) {
        return _num_buffered_bytes + batch_size > _total_buffer_limit;
    }

    // DataStreamMgr instance used to create this recvr. (Not owned)
    DataStreamMgr* _mgr;

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

    // total number of bytes held across all sender queues.
    std::atomic<int> _num_buffered_bytes;

    // Memtracker for batches in the sender queue(s).
    std::shared_ptr<MemTracker> _mem_tracker;

    // One or more queues of row batches received from senders. If _is_merging is true,
    // there is one SenderQueue for each sender. Otherwise, row batches from all senders
    // are placed in the same SenderQueue. The SenderQueue instances are owned by the
    // receiver and placed in _sender_queue_pool.
    std::vector<SenderQueue*> _sender_queues;

    // SortedRunMerger used to merge rows from different senders.
    std::unique_ptr<SortedRunMerger> _merger;

    std::vector<std::unique_ptr<SortedRunMerger>> _child_mergers;

    // Pool of sender queues.
    ObjectPool _sender_queue_pool;

    // Runtime profile storing the counters below.
    RuntimeProfile* _profile;

    // Number of bytes received
    RuntimeProfile::Counter* _bytes_received_counter;

    // Time series of number of bytes received, samples _bytes_received_counter
    // RuntimeProfile::TimeSeriesCounter* _bytes_received_time_series_counter;
    RuntimeProfile::Counter* _deserialize_row_batch_timer;

    // Time spent waiting until the first batch arrives across all queues.
    // TODO: Turn this into a wall-clock timer.
    RuntimeProfile::Counter* _first_batch_wait_total_timer;

    // Total time (summed across all threads) spent waiting for the
    // recv buffer to be drained so that new batches can be
    // added. Remote plan fragments are blocked for the same amount of
    // time.
    RuntimeProfile::Counter* _buffer_full_total_timer;

    // Sub plan query statistics receiver.
    std::shared_ptr<QueryStatisticsRecvr> _sub_plan_query_statistics_recvr;

    // Total time spent waiting for data to arrive in the recv buffer
    RuntimeProfile::Counter* _data_arrival_timer;
};

} // end namespace doris

#endif // end DORIS_BE_SRC_RUNTIME_DATA_STREAM_RECVR_H
