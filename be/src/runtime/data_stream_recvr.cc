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

#include "runtime/data_stream_recvr.h"

#include <google/protobuf/stubs/common.h>

#include <deque>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "gen_cpp/data.pb.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/row_batch.h"
#include "runtime/sorted_run_merger.h"
#include "util/debug_util.h"
#include "util/logging.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"

using std::list;
using std::vector;
using std::pair;
using std::make_pair;

using std::condition_variable;
using std::mutex;
using boost::scoped_ptr;
using std::unique_lock;
using std::try_lock;
using std::lock_guard;
using std::mem_fn;

namespace doris {

class ThreadClosure : public google::protobuf::Closure {
public:
    void Run() { _cv.notify_one(); }
    void wait(unique_lock<std::mutex>& lock) { _cv.wait(lock); }

private:
    condition_variable _cv;
};
// Implements a blocking queue of row batches from one or more senders. One queue
// is maintained per sender if _is_merging is true for the enclosing receiver, otherwise
// rows from all senders are placed in the same queue.
class DataStreamRecvr::SenderQueue {
public:
    SenderQueue(DataStreamRecvr* parent_recvr, int num_senders, RuntimeProfile* profile);

    ~SenderQueue() {}

    // Return the next batch form this sender queue. Sets the returned batch in _cur_batch.
    // A returned batch that is not filled to capacity does *not* indicate
    // end-of-stream.
    // The call blocks until another batch arrives or all senders close
    // their channels. The returned batch is owned by the sender queue. The caller
    // must acquire data from the returned batch before the next call to get_batch().
    Status get_batch(RowBatch** next_batch);

    // Adds a row batch to this sender queue if this stream has not been cancelled;
    // blocks if this will make the stream exceed its buffer limit.
    // If the total size of the batches in this queue would exceed the allowed buffer size,
    // the queue is considered full and the call blocks until a batch is dequeued.
    void add_batch(const PRowBatch& pb_batch, int be_number, int64_t packet_seq,
                   ::google::protobuf::Closure** done);

    void add_batch(RowBatch* batch, bool use_move);

    // Decrement the number of remaining senders for this queue and signal eos ("new data")
    // if the count drops to 0. The number of senders will be 1 for a merging
    // DataStreamRecvr.
    void decrement_senders(int sender_id);

    // Set cancellation flag and signal cancellation to receiver and sender. Subsequent
    // incoming batches will be dropped.
    void cancel();

    // Must be called once to cleanup any queued resources.
    void close();

    // Returns the current batch from this queue being processed by a consumer.
    RowBatch* current_batch() const {
        { return _current_batch.get(); }
    }

private:
    // Receiver of which this queue is a member.
    DataStreamRecvr* _recvr;

    // protects all subsequent data.
    mutex _lock;

    // if true, the receiver fragment for this stream got cancelled
    bool _is_cancelled;

    // number of senders which haven't closed the channel yet
    // (if it drops to 0, end-of-stream is true)
    int _num_remaining_senders;

    // signal arrival of new batch or the eos/cancelled condition
    condition_variable _data_arrival_cv;

    // signal removal of data by stream consumer
    condition_variable _data_removal_cv;

    // queue of (batch length, batch) pairs.  The SenderQueue block owns memory to
    // these batches. They are handed off to the caller via get_batch.
    typedef list<pair<int, RowBatch*>> RowBatchQueue;
    RowBatchQueue _batch_queue;

    // The batch that was most recently returned via get_batch(), i.e. the current batch
    // from this queue being processed by a consumer. Is destroyed when the next batch
    // is retrieved.
    scoped_ptr<RowBatch> _current_batch;

    // Set to true when the first batch has been received
    bool _received_first_batch;

    std::unordered_set<int> _sender_eos_set;          // sender_id
    std::unordered_map<int, int64_t> _packet_seq_map; // be_number => packet_seq
    std::deque<std::pair<google::protobuf::Closure*, MonotonicStopWatch>> _pending_closures;
    std::unordered_map<std::thread::id, std::unique_ptr<ThreadClosure>> _local_closure;
};

DataStreamRecvr::SenderQueue::SenderQueue(DataStreamRecvr* parent_recvr, int num_senders,
                                          RuntimeProfile* profile)
        : _recvr(parent_recvr),
          _is_cancelled(false),
          _num_remaining_senders(num_senders),
          _received_first_batch(false) {}

Status DataStreamRecvr::SenderQueue::get_batch(RowBatch** next_batch) {
    unique_lock<mutex> l(_lock);
    // wait until something shows up or we know we're done
    while (!_is_cancelled && _batch_queue.empty() && _num_remaining_senders > 0) {
        VLOG_ROW << "wait arrival fragment_instance_id=" << _recvr->fragment_instance_id()
                 << " node=" << _recvr->dest_node_id();
        // Don't count time spent waiting on the sender as active time.
        CANCEL_SAFE_SCOPED_TIMER(_recvr->_data_arrival_timer, &_is_cancelled);
        CANCEL_SAFE_SCOPED_TIMER(
                _received_first_batch ? NULL : _recvr->_first_batch_wait_total_timer,
                &_is_cancelled);
        _data_arrival_cv.wait(l);
    }

    // _cur_batch must be replaced with the returned batch.
    _current_batch.reset();
    *next_batch = NULL;
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    if (_batch_queue.empty()) {
        DCHECK_EQ(_num_remaining_senders, 0);
        return Status::OK();
    }

    _received_first_batch = true;

    DCHECK(!_batch_queue.empty());
    RowBatch* result = _batch_queue.front().second;
    _recvr->_num_buffered_bytes -= _batch_queue.front().first;
    VLOG_ROW << "fetched #rows=" << result->num_rows();
    _batch_queue.pop_front();
    // _data_removal_cv.notify_one();
    _current_batch.reset(result);
    *next_batch = _current_batch.get();

    if (!_pending_closures.empty()) {
        auto closure_pair = _pending_closures.front();
        closure_pair.first->Run();
        _pending_closures.pop_front();

        closure_pair.second.stop();
        _recvr->_buffer_full_total_timer->update(closure_pair.second.elapsed_time());
    }

    return Status::OK();
}

void DataStreamRecvr::SenderQueue::add_batch(const PRowBatch& pb_batch, int be_number,
                                             int64_t packet_seq,
                                             ::google::protobuf::Closure** done) {
    unique_lock<mutex> l(_lock);
    if (_is_cancelled) {
        return;
    }
    auto iter = _packet_seq_map.find(be_number);
    if (iter != _packet_seq_map.end()) {
        if (iter->second >= packet_seq) {
            LOG(WARNING) << "packet already exist [cur_packet_id= " << iter->second
                         << " receive_packet_id=" << packet_seq << "]";
            return;
        }
        iter->second = packet_seq;
    } else {
        _packet_seq_map.emplace(be_number, packet_seq);
    }

    int batch_size = RowBatch::get_batch_size(pb_batch);
    COUNTER_UPDATE(_recvr->_bytes_received_counter, batch_size);

    // Following situation will match the following condition.
    // Sender send a packet failed, then close the channel.
    // but closed packet reach first, then the failed packet.
    // Then meet the assert
    // we remove the assert
    // DCHECK_GT(_num_remaining_senders, 0);
    if (_num_remaining_senders <= 0) {
        DCHECK(_sender_eos_set.end() != _sender_eos_set.find(be_number));
        return;
    }

    // We always accept the batch regardless of buffer limit, to avoid rpc pipeline stall.
    // If exceed buffer limit, we just do not response ACK to client, so the client won't
    // send data until receive ACK.
    // Note that if this be needs to receive data from N BEs, the size of buffer
    // may reach as many as (buffer_size + n * buffer_size)
    //
    // Note: It's important that we enqueue thrift_batch regardless of buffer limit if
    //  the queue is currently empty. In the case of a merging receiver, batches are
    //  received from a specific queue based on data order, and the pipeline will stall
    //  if the merger is waiting for data from an empty queue that cannot be filled
    //  because the limit has been reached.
    if (_is_cancelled) {
        return;
    }

    RowBatch* batch = NULL;
    {
        SCOPED_TIMER(_recvr->_deserialize_row_batch_timer);
        // Note: if this function makes a row batch, the batch *must* be added
        // to _batch_queue. It is not valid to create the row batch and destroy
        // it in this thread.
        batch = new RowBatch(_recvr->row_desc(), pb_batch, _recvr->mem_tracker().get());
    }

    VLOG_ROW << "added #rows=" << batch->num_rows() << " batch_size=" << batch_size << "\n";
    _batch_queue.emplace_back(batch_size, batch);
    // if done is nullptr, this function can't delay this response
    if (done != nullptr && _recvr->exceeds_limit(batch_size)) {
        MonotonicStopWatch monotonicStopWatch;
        monotonicStopWatch.start();
        DCHECK(*done != nullptr);
        _pending_closures.emplace_back(*done, monotonicStopWatch);
        *done = nullptr;
    }
    _recvr->_num_buffered_bytes += batch_size;
    _data_arrival_cv.notify_one();
}

void DataStreamRecvr::SenderQueue::add_batch(RowBatch* batch, bool use_move) {
    unique_lock<mutex> l(_lock);
    if (_is_cancelled) {
        return;
    }
    RowBatch* nbatch =
            new RowBatch(_recvr->row_desc(), batch->capacity(), _recvr->mem_tracker().get());
    if (use_move) {
        nbatch->acquire_state(batch);
    } else {
        batch->deep_copy_to(nbatch);
    }
    int batch_size = nbatch->total_byte_size();
    _batch_queue.emplace_back(batch_size, nbatch);
    _data_arrival_cv.notify_one();
    if (_recvr->exceeds_limit(batch_size)) {
        std::thread::id tid = std::this_thread::get_id();
        MonotonicStopWatch monotonicStopWatch;
        monotonicStopWatch.start();
        auto iter = _local_closure.find(tid);
        if (iter == _local_closure.end()) {
            _local_closure.emplace(tid, new ThreadClosure);
            iter = _local_closure.find(tid);
        }
        _pending_closures.emplace_back(iter->second.get(), monotonicStopWatch);
        iter->second->wait(l);
    }
    _recvr->_num_buffered_bytes += batch_size;
}

void DataStreamRecvr::SenderQueue::decrement_senders(int be_number) {
    lock_guard<mutex> l(_lock);
    if (_sender_eos_set.end() != _sender_eos_set.find(be_number)) {
        return;
    }
    _sender_eos_set.insert(be_number);
    DCHECK_GT(_num_remaining_senders, 0);
    _num_remaining_senders--;
    VLOG_FILE << "decremented senders: fragment_instance_id=" << _recvr->fragment_instance_id()
              << " node_id=" << _recvr->dest_node_id() << " #senders=" << _num_remaining_senders;
    if (_num_remaining_senders == 0) {
        _data_arrival_cv.notify_one();
    }
}

void DataStreamRecvr::SenderQueue::cancel() {
    {
        lock_guard<mutex> l(_lock);
        if (_is_cancelled) {
            return;
        }
        _is_cancelled = true;
        VLOG_QUERY << "cancelled stream: _fragment_instance_id=" << _recvr->fragment_instance_id()
                   << " node_id=" << _recvr->dest_node_id();
    }
    // Wake up all threads waiting to produce/consume batches.  They will all
    // notice that the stream is cancelled and handle it.
    _data_arrival_cv.notify_all();
    // _data_removal_cv.notify_all();
    // PeriodicCounterUpdater::StopTimeSeriesCounter(
    //         _recvr->_bytes_received_time_series_counter);

    {
        std::lock_guard<std::mutex> l(_lock);
        for (auto closure_pair : _pending_closures) {
            closure_pair.first->Run();
        }
        _pending_closures.clear();
    }
}

void DataStreamRecvr::SenderQueue::close() {
    {
        // If _is_cancelled is not set to true, there may be concurrent send
        // which add batch to _batch_queue. The batch added after _batch_queue
        // is clear will be memory leak
        std::lock_guard<std::mutex> l(_lock);
        _is_cancelled = true;

        for (auto closure_pair : _pending_closures) {
            closure_pair.first->Run();
        }
        _pending_closures.clear();
    }

    // Delete any batches queued in _batch_queue
    for (RowBatchQueue::iterator it = _batch_queue.begin(); it != _batch_queue.end(); ++it) {
        delete it->second;
    }

    _current_batch.reset();
}

Status DataStreamRecvr::create_merger(const TupleRowComparator& less_than) {
    DCHECK(_is_merging);
    vector<SortedRunMerger::RunBatchSupplier> child_input_batch_suppliers;
    // Create the merger that will a single stream of sorted rows.
    _merger.reset(new SortedRunMerger(less_than, &_row_desc, _profile, false));

    for (int i = 0; i < _sender_queues.size(); ++i) {
        child_input_batch_suppliers.emplace_back(
                bind(mem_fn(&SenderQueue::get_batch), _sender_queues[i], std::placeholders::_1));
    }
    RETURN_IF_ERROR(_merger->prepare(child_input_batch_suppliers));
    return Status::OK();
}

Status DataStreamRecvr::create_parallel_merger(const TupleRowComparator& less_than,
                                               uint32_t batch_size, MemTracker* mem_tracker) {
    DCHECK(_is_merging);
    vector<SortedRunMerger::RunBatchSupplier> child_input_batch_suppliers;

    // Create the merger that will a single stream of sorted rows.
    _merger.reset(new SortedRunMerger(less_than, &_row_desc, _profile, false));

    // There we chose parallel merge, we should make thread execute more parallel
    // to minimized the computation of top merger
    // top merger: have child merger to supplier data
    //	child merger: have sender queue to supplier data, each merger start a thread to merge data firstly
    //		sender queue: the data from other node
    // Before parallel merge, if we have 81 sender queue, data is 1000, the computation is 1000 * log(81)
    // After parallel merge, the computation is MAX(1000 * log(2), 500 * log(41))
    // Now we only support max 3 merge child, because:
    // we have N _sender_queue, M merge child. the best way is log(N / M) = M * log(M)
    // So if N = 8, M = 2
    // 	     N = 81, M = 3
    //       N = 1024, M = 4
    // normally the N is lower than 1024, so we chose 8 <= N < 81, M = 2
    // N >= 81, M = 3
    auto parallel_thread = _sender_queues.size() < 81 ? 2 : 3;
    auto step = _sender_queues.size() / parallel_thread + 1;
    for (int i = 0; i < _sender_queues.size(); i += step) {
        // Create the merger that will a single stream of sorted rows.
        std::unique_ptr<SortedRunMerger> child_merger(new ChildSortedRunMerger(
                less_than, &_row_desc, _profile, mem_tracker, batch_size, false));
        vector<SortedRunMerger::RunBatchSupplier> input_batch_suppliers;
        for (int j = i; j < std::min((size_t)i + step, _sender_queues.size()); ++j) {
            input_batch_suppliers.emplace_back(bind(mem_fn(&SenderQueue::get_batch),
                                                    _sender_queues[j], std::placeholders::_1));
        }
        child_merger->prepare(input_batch_suppliers);

        child_input_batch_suppliers.emplace_back(bind(mem_fn(&SortedRunMerger::get_batch),
                                                      child_merger.get(), std::placeholders::_1));
        _child_mergers.emplace_back(std::move(child_merger));
    }
    RETURN_IF_ERROR(_merger->prepare(child_input_batch_suppliers, true));

    return Status::OK();
}

void DataStreamRecvr::transfer_all_resources(RowBatch* transfer_batch) {
    // _child_mergers is not empty, means use parallel merge need transfer resource from
    // _sender queue.
    // the need transfer resources from child_merger input_row_batch
    if (!_child_mergers.empty()) {
        _merger->transfer_all_resources(transfer_batch);
    } else {
        for (SenderQueue* sender_queue : _sender_queues) {
            if (sender_queue->current_batch() != NULL) {
                sender_queue->current_batch()->transfer_resource_ownership(transfer_batch);
            }
        }
    }
}

DataStreamRecvr::DataStreamRecvr(
        DataStreamMgr* stream_mgr, const std::shared_ptr<MemTracker>& parent_tracker,
        const RowDescriptor& row_desc, const TUniqueId& fragment_instance_id,
        PlanNodeId dest_node_id, int num_senders, bool is_merging, int total_buffer_limit,
        RuntimeProfile* profile,
        std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr)
        : _mgr(stream_mgr),
          _fragment_instance_id(fragment_instance_id),
          _dest_node_id(dest_node_id),
          _total_buffer_limit(total_buffer_limit),
          _row_desc(row_desc),
          _is_merging(is_merging),
          _num_buffered_bytes(0),
          _profile(profile),
          _sub_plan_query_statistics_recvr(sub_plan_query_statistics_recvr) {
    _mem_tracker = MemTracker::CreateTracker(
            _profile, -1, "DataStreamRecvr", parent_tracker);

    // Create one queue per sender if is_merging is true.
    int num_queues = is_merging ? num_senders : 1;
    _sender_queues.reserve(num_queues);
    int num_sender_per_queue = is_merging ? 1 : num_senders;
    for (int i = 0; i < num_queues; ++i) {
        SenderQueue* queue =
                _sender_queue_pool.add(new SenderQueue(this, num_sender_per_queue, profile));
        _sender_queues.push_back(queue);
    }

    // Initialize the counters
    _bytes_received_counter = ADD_COUNTER(_profile, "BytesReceived", TUnit::BYTES);
    _deserialize_row_batch_timer = ADD_TIMER(_profile, "DeserializeRowBatchTimer");
    _data_arrival_timer = ADD_TIMER(_profile, "DataArrivalWaitTime");
    _buffer_full_total_timer = ADD_TIMER(_profile, "SendersBlockedTotalTimer(*)");
    _first_batch_wait_total_timer = ADD_TIMER(_profile, "FirstBatchArrivalWaitTime");
}

Status DataStreamRecvr::get_next(RowBatch* output_batch, bool* eos) {
    DCHECK(_merger.get() != NULL);
    return _merger->get_next(output_batch, eos);
}

void DataStreamRecvr::add_batch(const PRowBatch& batch, int sender_id, int be_number,
                                int64_t packet_seq, ::google::protobuf::Closure** done) {
    int use_sender_id = _is_merging ? sender_id : 0;
    // Add all batches to the same queue if _is_merging is false.
    _sender_queues[use_sender_id]->add_batch(batch, be_number, packet_seq, done);
}

void DataStreamRecvr::add_batch(RowBatch* batch, int sender_id, bool use_move) {
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->add_batch(batch, use_move);
}

void DataStreamRecvr::remove_sender(int sender_id, int be_number) {
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->decrement_senders(be_number);
}

void DataStreamRecvr::cancel_stream() {
    for (int i = 0; i < _sender_queues.size(); ++i) {
        _sender_queues[i]->cancel();
    }
}

void DataStreamRecvr::close() {
    for (int i = 0; i < _sender_queues.size(); ++i) {
        _sender_queues[i]->close();
    }
    // Remove this receiver from the DataStreamMgr that created it.
    // TODO: log error msg
    _mgr->deregister_recvr(fragment_instance_id(), dest_node_id());
    _mgr = NULL;
    _merger.reset();
    // TODO: Maybe shared tracker doesn't need to be reset manually
    _mem_tracker.reset();
}

DataStreamRecvr::~DataStreamRecvr() {
    DCHECK(_mgr == NULL) << "Must call close()";
}

Status DataStreamRecvr::get_batch(RowBatch** next_batch) {
    DCHECK(!_is_merging);
    DCHECK_EQ(_sender_queues.size(), 1);
    return _sender_queues[0]->get_batch(next_batch);
}

} // namespace doris
