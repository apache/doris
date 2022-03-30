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

#include "vec/runtime/vdata_stream_recvr.h"

#include "gen_cpp/data.pb.h"
#include "runtime/mem_tracker.h"
#include "util/uid_util.h"

#include "vec/core/block.h"
#include "vec/core/materialize_block.h"
#include "vec/core/sort_cursor.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vsorted_run_merger.h"

namespace doris::vectorized {

VDataStreamRecvr::SenderQueue::SenderQueue(VDataStreamRecvr* parent_recvr, int num_senders,
                                           RuntimeProfile* profile)
        : _recvr(parent_recvr),
          _is_cancelled(false),
          _num_remaining_senders(num_senders),
          _received_first_batch(false) {}

VDataStreamRecvr::SenderQueue::~SenderQueue() = default;

Status VDataStreamRecvr::SenderQueue::get_batch(Block** next_block) {
    std::unique_lock<std::mutex> l(_lock);
    // wait until something shows up or we know we're done
    while (!_is_cancelled && _block_queue.empty() && _num_remaining_senders > 0) {
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
    _current_block.reset();
    *next_block = nullptr;
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    if (_block_queue.empty()) {
        DCHECK_EQ(_num_remaining_senders, 0);
        return Status::OK();
    }

    _received_first_batch = true;

    DCHECK(!_block_queue.empty());
    Block* result = _block_queue.front().second;
    _recvr->_num_buffered_bytes -= _block_queue.front().first;
    VLOG_ROW << "fetched #rows=" << result->rows();
    _block_queue.pop_front();

    _current_block.reset(result);
    *next_block = _current_block.get();

    if (!_pending_closures.empty()) {
        auto closure_pair = _pending_closures.front();
        closure_pair.first->Run();
        _pending_closures.pop_front();

        closure_pair.second.stop();
        _recvr->_buffer_full_total_timer->update(closure_pair.second.elapsed_time());
    }

    return Status::OK();
}

void VDataStreamRecvr::SenderQueue::add_block(const PBlock& pblock, int be_number,
                                              int64_t packet_seq,
                                              ::google::protobuf::Closure** done) {
    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return;
    }
    auto iter = _packet_seq_map.find(be_number);
    if (iter != _packet_seq_map.end()) {
        if (iter->second >= packet_seq) {
            LOG(WARNING) << fmt::format(
                    "packet already exist [cur_packet_id= {} receive_packet_id={}]", iter->second,
                    packet_seq);
            return;
        }
        iter->second = packet_seq;
    } else {
        _packet_seq_map.emplace(be_number, packet_seq);
    }
    auto block_byte_size = pblock.ByteSizeLong();
    COUNTER_UPDATE(_recvr->_bytes_received_counter, block_byte_size);

    if (_num_remaining_senders <= 0) {
        DCHECK(_sender_eos_set.end() != _sender_eos_set.find(be_number));
        return;
    }

    if (_is_cancelled) {
        return;
    }

    Block* block = nullptr;
    {
        SCOPED_TIMER(_recvr->_deserialize_row_batch_timer);
        block = new Block(pblock);
    }
    _recvr->_block_mem_tracker->consume(block->bytes());

    VLOG_ROW << "added #rows=" << block->rows() << " batch_size=" << block_byte_size << "\n";
    _block_queue.emplace_back(block_byte_size, block);
    // if done is nullptr, this function can't delay this response
    if (done != nullptr && _recvr->exceeds_limit(block_byte_size)) {
        MonotonicStopWatch monotonicStopWatch;
        monotonicStopWatch.start();
        DCHECK(*done != nullptr);
        _pending_closures.emplace_back(*done, monotonicStopWatch);
        *done = nullptr;
    }
    _recvr->_num_buffered_bytes += block_byte_size;
    _data_arrival_cv.notify_one();
}

void VDataStreamRecvr::SenderQueue::add_block(Block* block, bool use_move) {
    std::unique_lock<std::mutex> l(_lock);
    if (_is_cancelled) {
        return;
    }
    Block* nblock = new Block(block->get_columns_with_type_and_name());
    nblock->info = block->info;

    // local exchange should copy the block contented if use move == false
    if (use_move) {
        block->clear();
    } else {
        auto rows = block->rows();
        for (int i = 0; i < nblock->columns(); ++i) {
            nblock->get_by_position(i).column =
                    nblock->get_by_position(i).column->clone_resized(rows);
        }
    }
    materialize_block_inplace(*nblock);

    size_t block_size = nblock->bytes();
    _block_queue.emplace_back(block_size, nblock);
    _recvr->_block_mem_tracker->consume(nblock->bytes());
    _data_arrival_cv.notify_one();

    if (_recvr->exceeds_limit(block_size)) {
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

    _recvr->_num_buffered_bytes += block_size;
}

void VDataStreamRecvr::SenderQueue::decrement_senders(int be_number) {
    std::lock_guard<std::mutex> l(_lock);
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

void VDataStreamRecvr::SenderQueue::cancel() {
    {
        std::lock_guard<std::mutex> l(_lock);
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

void VDataStreamRecvr::SenderQueue::close() {
    {
        // If _is_cancelled is not set to true, there may be concurrent send
        // which add batch to _block_queue. The batch added after _block_queue
        // is clear will be memory leak
        std::lock_guard<std::mutex> l(_lock);
        _is_cancelled = true;

        for (auto closure_pair : _pending_closures) {
            closure_pair.first->Run();
        }
        _pending_closures.clear();
    }

    // Delete any batches queued in _block_queue
    for (auto it = _block_queue.begin(); it != _block_queue.end(); ++it) {
        delete it->second;
    }

    _current_block.reset();
}

VDataStreamRecvr::VDataStreamRecvr(
        VDataStreamMgr* stream_mgr, const RowDescriptor& row_desc,
        const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
        bool is_merging, int total_buffer_limit, RuntimeProfile* profile,
        std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr)
        : _mgr(stream_mgr),
          _fragment_instance_id(fragment_instance_id),
          _dest_node_id(dest_node_id),
          _total_buffer_limit(total_buffer_limit),
          _row_desc(row_desc),
          _is_merging(is_merging),
          _is_closed(false),
          _num_buffered_bytes(0),
          _profile(profile),
          _sub_plan_query_statistics_recvr(sub_plan_query_statistics_recvr) {
    _mem_tracker =
            MemTracker::create_tracker(-1, "VDataStreamRecvr:" + print_id(_fragment_instance_id),
                                       nullptr, MemTrackerLevel::VERBOSE, _profile);
    _block_mem_tracker = MemTracker::create_virtual_tracker(
            -1, "VDataStreamRecvr:block:" + print_id(_fragment_instance_id), _mem_tracker);

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

VDataStreamRecvr::~VDataStreamRecvr() {
    DCHECK(_mgr == nullptr) << "Must call close()";
    MemTracker::memory_leak_check(_block_mem_tracker.get(), false);
}

Status VDataStreamRecvr::create_merger(const std::vector<VExprContext*>& ordering_expr,
                                       const std::vector<bool>& is_asc_order,
                                       const std::vector<bool>& nulls_first, size_t batch_size,
                                       int64_t limit, size_t offset) {
    DCHECK(_is_merging);
    std::vector<BlockSupplier> child_block_suppliers;
    // Create the merger that will a single stream of sorted rows.
    _merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size, limit,
                                       offset, _profile));

    for (int i = 0; i < _sender_queues.size(); ++i) {
        child_block_suppliers.emplace_back(std::bind(std::mem_fn(&SenderQueue::get_batch),
                                                     _sender_queues[i], std::placeholders::_1));
    }
    RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
    return Status::OK();
}

void VDataStreamRecvr::add_block(const PBlock& pblock, int sender_id, int be_number,
                                 int64_t packet_seq, ::google::protobuf::Closure** done) {
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->add_block(pblock, be_number, packet_seq, done);
}

void VDataStreamRecvr::add_block(Block* block, int sender_id, bool use_move) {
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->add_block(block, use_move);
}

Status VDataStreamRecvr::get_next(Block* block, bool* eos) {
    if (!_is_merging) {
        Block* res = nullptr;
        RETURN_IF_ERROR(_sender_queues[0]->get_batch(&res));
        if (res != nullptr) {
            block->swap(*res);
        } else {
            *eos = true;
            return Status::OK();
        }
    } else {
        RETURN_IF_ERROR(_merger->get_next(block, eos));
    }

    if (LIKELY(_block_mem_tracker->consumption() >= block->bytes())) {
        _block_mem_tracker->release(block->bytes());
    } else {
        _block_mem_tracker->release(_block_mem_tracker->consumption());
    }
    return Status::OK();
}

void VDataStreamRecvr::remove_sender(int sender_id, int be_number) {
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->decrement_senders(be_number);
}

void VDataStreamRecvr::cancel_stream() {
    for (int i = 0; i < _sender_queues.size(); ++i) {
        _sender_queues[i]->cancel();
    }
}

void VDataStreamRecvr::close() {
    if (_is_closed) {
        return;
    }
    _is_closed = true;
    for (int i = 0; i < _sender_queues.size(); ++i) {
        _sender_queues[i]->close();
    }
    // Remove this receiver from the DataStreamMgr that created it.
    // TODO: log error msg
    _mgr->deregister_recvr(fragment_instance_id(), dest_node_id());
    _mgr = nullptr;

    _merger.reset();
    _block_mem_tracker->release(_block_mem_tracker->consumption());
}

} // namespace doris::vectorized
