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

#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>

#include <algorithm>
#include <functional>
#include <string>

#include "common/logging.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/exchange_source_operator.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
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
          _received_first_batch(false) {
    _cancel_status = Status::OK();
}

VDataStreamRecvr::SenderQueue::~SenderQueue() {
    // Check pending closures, if it is not empty, should clear it here. but it should not happen.
    // closure will delete itself during run method. If it is not called, brpc will memory leak.
    DCHECK(_pending_closures.empty());
    for (auto closure_pair : _pending_closures) {
        closure_pair.first->Run();
    }
    _pending_closures.clear();
}

bool VDataStreamRecvr::SenderQueue::should_wait() {
    std::unique_lock<std::mutex> l(_lock);
    return !_is_cancelled && _block_queue.empty() && _num_remaining_senders > 0;
}

Status VDataStreamRecvr::SenderQueue::get_batch(Block* block, bool* eos) {
    std::unique_lock<std::mutex> l(_lock);
    // wait until something shows up or we know we're done
    while (!_is_cancelled && _block_queue.empty() && _num_remaining_senders > 0) {
        VLOG_ROW << "wait arrival fragment_instance_id=" << print_id(_recvr->fragment_instance_id())
                 << " node=" << _recvr->dest_node_id();
        // Don't count time spent waiting on the sender as active time.
        CANCEL_SAFE_SCOPED_TIMER(_recvr->_data_arrival_timer, &_is_cancelled);
        CANCEL_SAFE_SCOPED_TIMER(
                _received_first_batch ? nullptr : _recvr->_first_batch_wait_total_timer,
                &_is_cancelled);
        _data_arrival_cv.wait(l);
    }
    return _inner_get_batch_without_lock(block, eos);
}

Status VDataStreamRecvr::SenderQueue::_inner_get_batch_without_lock(Block* block, bool* eos) {
    if (_is_cancelled) {
        RETURN_IF_ERROR(_cancel_status);
        return Status::Cancelled("Cancelled");
    }

    if (_block_queue.empty()) {
        DCHECK_EQ(_num_remaining_senders, 0);
        *eos = true;
        return Status::OK();
    }

    _received_first_batch = true;

    DCHECK(!_block_queue.empty());
    auto [next_block, block_byte_size] = std::move(_block_queue.front());
    _recvr->update_blocks_memory_usage(-block_byte_size);
    _block_queue.pop_front();
    if (_block_queue.size() == 0 && _dependency) {
        if (!_is_cancelled && _num_remaining_senders > 0) {
            _dependency->block_reading();
        }
        for (auto& it : _local_channel_dependency) {
            it->set_ready_for_write();
        }
    }

    if (!_pending_closures.empty()) {
        auto closure_pair = _pending_closures.front();
        closure_pair.first->Run();
        _pending_closures.pop_front();

        closure_pair.second.stop();
        _recvr->_buffer_full_total_timer->update(closure_pair.second.elapsed_time());
    }
    block->swap(*next_block);
    *eos = false;
    return Status::OK();
}

Status VDataStreamRecvr::SenderQueue::add_block(const PBlock& pblock, int be_number,
                                                int64_t packet_seq,
                                                ::google::protobuf::Closure** done) {
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_is_cancelled) {
            return Status::OK();
        }
        auto iter = _packet_seq_map.find(be_number);
        if (iter != _packet_seq_map.end()) {
            if (iter->second >= packet_seq) {
                LOG(WARNING) << fmt::format(
                        "packet already exist [cur_packet_id= {} receive_packet_id={}]",
                        iter->second, packet_seq);
                return Status::OK();
            }
            iter->second = packet_seq;
        } else {
            _packet_seq_map.emplace(be_number, packet_seq);
        }
        auto pblock_byte_size = pblock.ByteSizeLong();
        COUNTER_UPDATE(_recvr->_bytes_received_counter, pblock_byte_size);

        DCHECK(_num_remaining_senders >= 0);
        if (_num_remaining_senders == 0) {
            DCHECK(_sender_eos_set.end() != _sender_eos_set.find(be_number));
            return Status::OK();
        }
    }

    BlockUPtr block = nullptr;
    int64_t deserialize_time = 0;
    {
        SCOPED_RAW_TIMER(&deserialize_time);
        block = Block::create_unique();
        RETURN_IF_ERROR(block->deserialize(pblock));
    }

    auto block_byte_size = block->allocated_bytes();
    VLOG_ROW << "added #rows=" << block->rows() << " batch_size=" << block_byte_size << "\n";

    std::lock_guard<std::mutex> l(_lock);
    if (_is_cancelled) {
        return Status::OK();
    }

    COUNTER_UPDATE(_recvr->_deserialize_row_batch_timer, deserialize_time);
    COUNTER_UPDATE(_recvr->_decompress_timer, block->get_decompress_time());
    COUNTER_UPDATE(_recvr->_decompress_bytes, block->get_decompressed_bytes());
    COUNTER_UPDATE(_recvr->_rows_produced_counter, block->rows());
    COUNTER_UPDATE(_recvr->_blocks_produced_counter, 1);

    bool empty = !block->rows();

    if (!empty) {
        _block_queue.emplace_back(std::move(block), block_byte_size);
        if (_dependency) {
            _dependency->set_ready_for_read();
        }
    }
    // if done is nullptr, this function can't delay this response
    if (done != nullptr && _recvr->exceeds_limit(block_byte_size)) {
        MonotonicStopWatch monotonicStopWatch;
        monotonicStopWatch.start();
        DCHECK(*done != nullptr);
        _pending_closures.emplace_back(*done, monotonicStopWatch);
        *done = nullptr;
    }
    _recvr->update_blocks_memory_usage(block_byte_size);
    if (!empty) {
        _data_arrival_cv.notify_one();
    }
    return Status::OK();
}

void VDataStreamRecvr::SenderQueue::add_block(Block* block, bool use_move) {
    {
        std::unique_lock<std::mutex> l(_lock);
        if (_is_cancelled || !block->rows()) {
            return;
        }
    }

    auto block_bytes_received = block->bytes();
    // Has to use unique ptr here, because clone column may failed if allocate memory failed.
    BlockUPtr nblock = Block::create_unique(block->get_columns_with_type_and_name());

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

    size_t block_mem_size = nblock->allocated_bytes();
    std::unique_lock<std::mutex> l(_lock);
    if (_is_cancelled) {
        return;
    }
    COUNTER_UPDATE(_recvr->_local_bytes_received_counter, block_bytes_received);
    COUNTER_UPDATE(_recvr->_rows_produced_counter, block->rows());
    COUNTER_UPDATE(_recvr->_blocks_produced_counter, 1);

    bool empty = !nblock->rows();

    if (!empty) {
        _block_queue.emplace_back(std::move(nblock), block_mem_size);
        if (_dependency) {
            _dependency->set_ready_for_read();
        }
        _data_arrival_cv.notify_one();
    }

    // Careful: Accessing members of _recvr that are allocated by Object pool
    // should be done before the following logic, because the _lock will be released
    // by `iter->second->wait(l)`, after `iter->second->wait(l)` returns, _recvr may
    // have been closed and resouces in _recvr are released;
    _recvr->update_blocks_memory_usage(block_mem_size);
    if (_recvr->exceeds_limit(0)) {
        // yiguolei
        // It is too tricky here, if the running thread is bthread then the tid may be wrong.
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
}

void VDataStreamRecvr::SenderQueue::decrement_senders(int be_number) {
    std::lock_guard<std::mutex> l(_lock);
    if (_sender_eos_set.end() != _sender_eos_set.find(be_number)) {
        return;
    }
    _sender_eos_set.insert(be_number);
    DCHECK_GT(_num_remaining_senders, 0);
    _num_remaining_senders--;
    VLOG_FILE << "decremented senders: fragment_instance_id="
              << print_id(_recvr->fragment_instance_id()) << " node_id=" << _recvr->dest_node_id()
              << " #senders=" << _num_remaining_senders;
    if (_num_remaining_senders == 0) {
        if (_dependency) {
            _dependency->set_always_done();
        }
        _data_arrival_cv.notify_one();
    }
}

void VDataStreamRecvr::SenderQueue::cancel(Status cancel_status) {
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_is_cancelled) {
            return;
        }
        _is_cancelled = true;
        _cancel_status = cancel_status;
        if (_dependency) {
            _dependency->set_always_done();
        }
        VLOG_QUERY << "cancelled stream: _fragment_instance_id="
                   << print_id(_recvr->fragment_instance_id())
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
        if (_dependency) {
            _dependency->set_always_done();
        }

        for (auto closure_pair : _pending_closures) {
            closure_pair.first->Run();
        }
        _pending_closures.clear();
    }

    // Delete any batches queued in _block_queue
    _block_queue.clear();
}

VDataStreamRecvr::VDataStreamRecvr(
        VDataStreamMgr* stream_mgr, RuntimeState* state, const RowDescriptor& row_desc,
        const TUniqueId& fragment_instance_id, PlanNodeId dest_node_id, int num_senders,
        bool is_merging, RuntimeProfile* profile,
        std::shared_ptr<QueryStatisticsRecvr> sub_plan_query_statistics_recvr)
        : _mgr(stream_mgr),
#ifdef USE_MEM_TRACKER
          _query_mem_tracker(state->query_mem_tracker()),
          _query_id(state->query_id()),
#endif
          _fragment_instance_id(fragment_instance_id),
          _dest_node_id(dest_node_id),
          _row_desc(row_desc),
          _is_merging(is_merging),
          _is_closed(false),
          _profile(profile),
          _peak_memory_usage_counter(nullptr),
          _sub_plan_query_statistics_recvr(sub_plan_query_statistics_recvr),
          _enable_pipeline(state->enable_pipeline_exec()) {
    // DataStreamRecvr may be destructed after the instance execution thread ends.
    _mem_tracker =
            std::make_unique<MemTracker>("VDataStreamRecvr:" + print_id(_fragment_instance_id));
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());

    if (state->enable_pipeline_x_exec()) {
        _sender_to_local_channel_dependency.resize(num_senders);
        for (size_t i = 0; i < num_senders; i++) {
            _sender_to_local_channel_dependency[i] =
                    pipeline::LocalExchangeChannelDependency::create_shared(_dest_node_id);
        }
    }

    // Create one queue per sender if is_merging is true.
    int num_queues = is_merging ? num_senders : 1;
    _sender_queues.reserve(num_queues);
    int num_sender_per_queue = is_merging ? 1 : num_senders;
    for (int i = 0; i < num_queues; ++i) {
        SenderQueue* queue = nullptr;
        if (_enable_pipeline) {
            queue = _sender_queue_pool.add(new PipSenderQueue(this, num_sender_per_queue, profile));
            if (state->enable_pipeline_x_exec()) {
                auto dependencies =
                        is_merging
                                ? std::vector<std::shared_ptr<
                                          pipeline::
                                                  LocalExchangeChannelDependency>> {_sender_to_local_channel_dependency
                                                                                            [i]}
                                : _sender_to_local_channel_dependency;
                queue->set_local_channel_dependency(dependencies);
            }
        } else {
            queue = _sender_queue_pool.add(new SenderQueue(this, num_sender_per_queue, profile));
        }
        _sender_queues.push_back(queue);
    }

    // Initialize the counters
    _memory_usage_counter = ADD_LABEL_COUNTER(_profile, "MemoryUsage");
    _blocks_memory_usage = _profile->AddHighWaterMarkCounter("Blocks", TUnit::BYTES, "MemoryUsage");
    _peak_memory_usage_counter =
            _profile->AddHighWaterMarkCounter("PeakMemoryUsage", TUnit::BYTES, "MemoryUsage");
    _bytes_received_counter = ADD_COUNTER(_profile, "BytesReceived", TUnit::BYTES);
    _local_bytes_received_counter = ADD_COUNTER(_profile, "LocalBytesReceived", TUnit::BYTES);

    _deserialize_row_batch_timer = ADD_TIMER(_profile, "DeserializeRowBatchTimer");
    _data_arrival_timer = ADD_TIMER(_profile, "DataArrivalWaitTime");
    _buffer_full_total_timer = ADD_TIMER(_profile, "SendersBlockedTotalTimer(*)");
    _first_batch_wait_total_timer = ADD_TIMER(_profile, "FirstBatchArrivalWaitTime");
    _decompress_timer = ADD_TIMER(_profile, "DecompressTime");
    _decompress_bytes = ADD_COUNTER(_profile, "DecompressBytes", TUnit::BYTES);
    _rows_produced_counter = ADD_COUNTER(_profile, "RowsProduced", TUnit::UNIT);
    _blocks_produced_counter = ADD_COUNTER(_profile, "BlocksProduced", TUnit::UNIT);
}

VDataStreamRecvr::~VDataStreamRecvr() {
    DCHECK(_mgr == nullptr) << "Must call close()";
}

Status VDataStreamRecvr::create_merger(const VExprContextSPtrs& ordering_expr,
                                       const std::vector<bool>& is_asc_order,
                                       const std::vector<bool>& nulls_first, size_t batch_size,
                                       int64_t limit, size_t offset) {
    DCHECK(_is_merging);
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    std::vector<BlockSupplier> child_block_suppliers;
    // Create the merger that will a single stream of sorted rows.
    _merger.reset(new VSortedRunMerger(ordering_expr, is_asc_order, nulls_first, batch_size, limit,
                                       offset, _profile));

    for (int i = 0; i < _sender_queues.size(); ++i) {
        child_block_suppliers.emplace_back(std::bind(std::mem_fn(&SenderQueue::get_batch),
                                                     _sender_queues[i], std::placeholders::_1,
                                                     std::placeholders::_2));
    }
    _merger->set_pipeline_engine_enabled(_enable_pipeline);
    RETURN_IF_ERROR(_merger->prepare(child_block_suppliers));
    return Status::OK();
}

Status VDataStreamRecvr::add_block(const PBlock& pblock, int sender_id, int be_number,
                                   int64_t packet_seq, ::google::protobuf::Closure** done) {
    SCOPED_ATTACH_TASK_WITH_ID(_query_mem_tracker, _query_id, _fragment_instance_id);
    int use_sender_id = _is_merging ? sender_id : 0;
    return _sender_queues[use_sender_id]->add_block(pblock, be_number, packet_seq, done);
}

void VDataStreamRecvr::add_block(Block* block, int sender_id, bool use_move) {
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->add_block(block, use_move);
}

bool VDataStreamRecvr::sender_queue_empty(int sender_id) {
    int use_sender_id = _is_merging ? sender_id : 0;
    return _sender_queues[use_sender_id]->queue_empty();
}

std::shared_ptr<pipeline::LocalExchangeChannelDependency>
VDataStreamRecvr::get_local_channel_dependency(int sender_id) {
    DCHECK_GT(_sender_to_local_channel_dependency.size(), sender_id);
    DCHECK(_sender_to_local_channel_dependency[sender_id] != nullptr);
    return _sender_to_local_channel_dependency[sender_id];
}

bool VDataStreamRecvr::ready_to_read() {
    for (const auto& queue : _sender_queues) {
        if (queue->should_wait()) {
            return false;
        }
    }
    return true;
}

Status VDataStreamRecvr::get_next(Block* block, bool* eos) {
    _peak_memory_usage_counter->set(_mem_tracker->peak_consumption());
    if (!_is_merging) {
        block->clear();
        return _sender_queues[0]->get_batch(block, eos);
    } else {
        return _merger->get_next(block, eos);
    }
}

void VDataStreamRecvr::remove_sender(int sender_id, int be_number, Status exec_status) {
    if (!exec_status.ok()) {
        cancel_stream(exec_status);
        return;
    }
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->decrement_senders(be_number);
}

void VDataStreamRecvr::remove_sender(int sender_id, int be_number, QueryStatisticsPtr statistics,
                                     Status exec_status) {
    if (!exec_status.ok()) {
        cancel_stream(exec_status);
        return;
    }
    int use_sender_id = _is_merging ? sender_id : 0;
    _sender_queues[use_sender_id]->decrement_senders(be_number);
    _sub_plan_query_statistics_recvr->insert(statistics, sender_id);
}

void VDataStreamRecvr::cancel_stream(Status exec_status) {
    VLOG_QUERY << "cancel_stream: fragment_instance_id=" << print_id(_fragment_instance_id)
               << exec_status;

    for (int i = 0; i < _sender_queues.size(); ++i) {
        _sender_queues[i]->cancel(exec_status);
    }
}

void VDataStreamRecvr::update_blocks_memory_usage(int64_t size) {
    _blocks_memory_usage->add(size);
    _blocks_memory_usage_current_value = _blocks_memory_usage->current_value();
    if (_sender_to_local_channel_dependency.size() > 0 && size > 0 &&
        _blocks_memory_usage_current_value > config::exchg_node_buffer_size_bytes && !_is_closed) {
        for (size_t i = 0; i < _sender_to_local_channel_dependency.size(); i++) {
            if (!sender_queue_empty(i)) {
                _sender_to_local_channel_dependency[i]->block_writing();
            }
        }
    } else if (_sender_to_local_channel_dependency.size() > 0 && size < 0 &&
               _blocks_memory_usage_current_value <= config::exchg_node_buffer_size_bytes) {
        for (auto& it : _sender_to_local_channel_dependency) {
            it->set_ready_for_write();
        }
    }
}

void VDataStreamRecvr::close() {
    if (_is_closed) {
        return;
    }
    _is_closed = true;
    for (auto& it : _sender_to_local_channel_dependency) {
        it->set_ready_for_write();
    }
    for (int i = 0; i < _sender_queues.size(); ++i) {
        _sender_queues[i]->close();
    }
    // Remove this receiver from the DataStreamMgr that created it.
    // TODO: log error msg
    static_cast<void>(_mgr->deregister_recvr(fragment_instance_id(), dest_node_id()));
    _mgr = nullptr;

    _merger.reset();
    if (_peak_memory_usage_counter) {
        _peak_memory_usage_counter->set(_mem_tracker->peak_consumption());
    }
}

void VDataStreamRecvr::PipSenderQueue::add_block(Block* block, bool use_move) {
    if (block->rows() == 0) {
        return;
    }
    {
        std::unique_lock<std::mutex> l(_lock);
        if (_is_cancelled) {
            return;
        }
    }
    BlockUPtr nblock = Block::create_unique(block->get_columns_with_type_and_name());

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

    auto block_mem_size = nblock->allocated_bytes();
    {
        std::unique_lock<std::mutex> l(_lock);
        if (_is_cancelled) {
            return;
        }
        _block_queue.emplace_back(std::move(nblock), block_mem_size);
        if (_dependency) {
            _dependency->set_ready_for_read();
        }
        COUNTER_UPDATE(_recvr->_local_bytes_received_counter, block_mem_size);
        _recvr->update_blocks_memory_usage(block_mem_size);
        _data_arrival_cv.notify_one();
    }
}

} // namespace doris::vectorized
