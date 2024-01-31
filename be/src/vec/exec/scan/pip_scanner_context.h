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

#include "pipeline/exec/scan_operator.h"
#include "runtime/descriptors.h"
#include "scanner_context.h"

namespace doris::pipeline {

class PipScannerContext final : public vectorized::ScannerContext {
    ENABLE_FACTORY_CREATOR(PipScannerContext);

public:
    PipScannerContext(RuntimeState* state, vectorized::VScanNode* parent,
                      const TupleDescriptor* output_tuple_desc,
                      const RowDescriptor* output_row_descriptor,
                      const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners,
                      int64_t limit_, int64_t max_bytes_in_blocks_queue,
                      const int num_parallel_instances)
            : vectorized::ScannerContext(state, parent, output_tuple_desc, output_row_descriptor,
                                         scanners, limit_, max_bytes_in_blocks_queue,
                                         num_parallel_instances) {}

    Status get_block_from_queue(RuntimeState* state, vectorized::BlockUPtr* block, bool* eos,
                                int id) override {
        {
            std::unique_lock l(_transfer_lock);
            if (state->is_cancelled()) {
                set_status_on_error(Status::Cancelled("cancelled"), false);
            }

            if (!status().ok()) {
                return _process_status;
            }
        }

        std::vector<vectorized::BlockUPtr> merge_blocks;
        {
            std::unique_lock<std::mutex> l(*_queue_mutexs[id]);
            // The pipeline maybe wake up by scanner.done. If there are still any data
            // in the queue, should read the data first and then check if the scanner.done
            // if done, then eos is returned to indicate that the scan operator finished.
            if (_blocks_queues[id].empty()) {
                *eos = done();
                return Status::OK();
            }
            if (_process_status.is<ErrorCode::CANCELLED>()) {
                *eos = true;
                return Status::OK();
            }
            *block = std::move(_blocks_queues[id].front());
            _blocks_queues[id].pop_front();

            auto rows = (*block)->rows();
            while (!_blocks_queues[id].empty()) {
                const auto add_rows = (*_blocks_queues[id].front()).rows();
                if (rows + add_rows < state->batch_size()) {
                    rows += add_rows;
                    merge_blocks.emplace_back(std::move(_blocks_queues[id].front()));
                    _blocks_queues[id].pop_front();
                } else {
                    break;
                }
            }

            if (_blocks_queues[id].empty()) {
                this->reschedule_scanner_ctx();
            }
        }

        _current_used_bytes -= (*block)->allocated_bytes();
        if (!merge_blocks.empty()) {
            vectorized::MutableBlock m(block->get());
            for (auto& merge_block : merge_blocks) {
                _current_used_bytes -= merge_block->allocated_bytes();
                static_cast<void>(m.merge(*merge_block));
                return_free_block(std::move(merge_block));
            }
            (*block)->set_columns(std::move(m.mutable_columns()));
        }

        // after return free blocks, should try to reschedule the scanner
        if (should_be_scheduled()) {
            this->reschedule_scanner_ctx();
        }

        return Status::OK();
    }

    void append_blocks_to_queue(std::vector<vectorized::BlockUPtr>& blocks) override {
        const int queue_size = _blocks_queues.size();
        const int block_size = blocks.size();
        if (block_size == 0) {
            return;
        }
        int64_t local_bytes = 0;

        for (const auto& block : blocks) {
            auto st = validate_block_schema(block.get());
            if (!st.ok()) {
                set_status_on_error(st, false);
            }
            local_bytes += block->allocated_bytes();
        }

        for (int i = 0; i < queue_size && i < block_size; ++i) {
            int queue = _next_queue_to_feed;
            {
                std::lock_guard<std::mutex> l(*_queue_mutexs[queue]);
                for (int j = i; j < block_size; j += queue_size) {
                    _blocks_queues[queue].emplace_back(std::move(blocks[j]));
                }
            }
            _next_queue_to_feed = queue + 1 < queue_size ? queue + 1 : 0;
        }
        _current_used_bytes += local_bytes;
    }

    bool empty_in_queue(int id) override {
        std::unique_lock<std::mutex> l(*_queue_mutexs[id]);
        return _blocks_queues[id].empty();
    }

    Status init() override {
        for (int i = 0; i < _num_parallel_instances; ++i) {
            _queue_mutexs.emplace_back(std::make_unique<std::mutex>());
            _blocks_queues.emplace_back(std::list<vectorized::BlockUPtr>());
        }
        return ScannerContext::init();
    }

    std::string debug_string() override {
        auto res = ScannerContext::debug_string();
        for (int i = 0; i < _blocks_queues.size(); ++i) {
            res += " queue " + std::to_string(i) + ":size " +
                   std::to_string(_blocks_queues[i].size());
        }
        return res;
    }

protected:
    int _next_queue_to_feed = 0;
    std::vector<std::unique_ptr<std::mutex>> _queue_mutexs;
    std::vector<std::list<vectorized::BlockUPtr>> _blocks_queues;
    std::atomic_int64_t _current_used_bytes = 0;
};

class PipXScannerContext final : public vectorized::ScannerContext {
    ENABLE_FACTORY_CREATOR(PipXScannerContext);

public:
    PipXScannerContext(RuntimeState* state, ScanLocalStateBase* local_state,
                       const TupleDescriptor* output_tuple_desc,
                       const RowDescriptor* output_row_descriptor,
                       const std::list<std::shared_ptr<vectorized::ScannerDelegate>>& scanners,
                       int64_t limit_, int64_t max_bytes_in_blocks_queue,
                       std::shared_ptr<pipeline::ScanDependency> dependency)
            : vectorized::ScannerContext(state, output_tuple_desc, output_row_descriptor, scanners,
                                         limit_, max_bytes_in_blocks_queue, 1, local_state,
                                         dependency) {}
    Status get_block_from_queue(RuntimeState* state, vectorized::BlockUPtr* block, bool* eos,
                                int id) override {
        if (_blocks_queue_buffered.empty()) {
            std::unique_lock l(_transfer_lock);
            if (state->is_cancelled()) {
                set_status_on_error(Status::Cancelled("cancelled"), false);
            }

            if (!status().ok()) {
                return _process_status;
            }

            if (_blocks_queue.empty()) {
                *eos = done();
                return Status::OK();
            }
            if (_process_status.is<ErrorCode::CANCELLED>()) {
                *eos = true;
                return Status::OK();
            }

            _blocks_queue_buffered = std::move(_blocks_queue);
        }

        // `get_block_from_queue` should not be called concurrently from multiple threads,
        // so here no need to lock.
        *block = std::move(_blocks_queue_buffered.front());
        _blocks_queue_buffered.pop_front();

        std::vector<vectorized::BlockUPtr> merge_blocks;
        auto rows = (*block)->rows();
        while (!_blocks_queue_buffered.empty()) {
            const auto add_rows = (*_blocks_queue_buffered.front()).rows();
            if (rows + add_rows < state->batch_size()) {
                rows += add_rows;
                merge_blocks.emplace_back(std::move(_blocks_queue_buffered.front()));
                _blocks_queue_buffered.pop_front();
            } else {
                break;
            }
        }

        if (_blocks_queue_buffered.empty()) {
            std::unique_lock l(_transfer_lock);
            if (_blocks_queue.empty()) {
                this->reschedule_scanner_ctx();
                _dependency->block();
            } else {
                _blocks_queue_buffered = std::move(_blocks_queue);
            }
        }

        _cur_bytes_in_queue -= (*block)->allocated_bytes();
        if (!merge_blocks.empty()) {
            vectorized::MutableBlock m(block->get());
            for (auto& merge_block : merge_blocks) {
                _cur_bytes_in_queue -= merge_block->allocated_bytes();
                static_cast<void>(m.merge(*merge_block));
                if (merge_block->mem_reuse()) {
                    _free_blocks_buffered.emplace_back(std::move(merge_block));
                }
            }
            (*block)->set_columns(std::move(m.mutable_columns()));
        }
        return_free_blocks();

        // after return free blocks, should try to reschedule the scanner
        if (should_be_scheduled()) {
            this->reschedule_scanner_ctx();
        }

        return Status::OK();
    }

    void reschedule_scanner_ctx() override {
        if (done()) {
            return;
        }
        auto state = _scanner_scheduler->submit(shared_from_this());
        //todo(wb) rethinking is it better to mark current scan_context failed when submit failed many times?
        if (state.ok()) {
            _num_scheduling_ctx++;
        } else {
            set_status_on_error(state, false);
        }
    }

private:
    void return_free_blocks() {
        if (_free_blocks_buffered.empty()) {
            return;
        }

        size_t total_bytes = 0;
        for (auto& block : _free_blocks_buffered) {
            const auto bytes = block->allocated_bytes();
            block->clear_column_data();
            _estimated_block_bytes = std::max(bytes, (size_t)16);
            total_bytes += bytes;
        }
        _free_blocks_memory_usage->add(total_bytes);
        const auto count = _free_blocks_buffered.size();
        _free_blocks.enqueue_bulk(std::make_move_iterator(_free_blocks_buffered.begin()), count);
        _free_blocks_buffered.clear();
        _serving_blocks_num -= count;
    }

    std::vector<vectorized::BlockUPtr> _free_blocks_buffered;
    std::list<vectorized::BlockUPtr> _blocks_queue_buffered;
};

} // namespace doris::pipeline
