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
                      const std::vector<int>& col_distribute_ids, const int num_parallel_instances)
            : vectorized::ScannerContext(state, parent, output_tuple_desc, output_row_descriptor,
                                         scanners, limit_, max_bytes_in_blocks_queue,
                                         num_parallel_instances),
              _col_distribute_ids(col_distribute_ids),
              _need_colocate_distribute(!_col_distribute_ids.empty()) {}

    Status get_block_from_queue(RuntimeState* state, vectorized::BlockUPtr* block, bool* eos,
                                int id, bool wait = false) override {
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

        return Status::OK();
    }

    void append_blocks_to_queue(std::vector<vectorized::BlockUPtr>& blocks) override {
        const int queue_size = _blocks_queues.size();
        const int block_size = blocks.size();
        if (block_size == 0) {
            return;
        }
        int64_t local_bytes = 0;

        if (_need_colocate_distribute) {
            std::vector<uint32_t> hash_vals;
            for (const auto& block : blocks) {
                auto st = validate_block_schema(block.get());
                if (!st.ok()) {
                    set_status_on_error(st, false);
                }
                // vectorized calculate hash
                int rows = block->rows();
                const auto element_size = _num_parallel_instances;
                hash_vals.resize(rows);
                std::fill(hash_vals.begin(), hash_vals.end(), 0);
                auto* __restrict hashes = hash_vals.data();

                for (int j = 0; j < _col_distribute_ids.size(); ++j) {
                    block->get_by_position(_col_distribute_ids[j])
                            .column->update_crcs_with_value(
                                    hash_vals.data(),
                                    _output_tuple_desc->slots()[_col_distribute_ids[j]]
                                            ->type()
                                            .type,
                                    rows);
                }
                for (int i = 0; i < rows; i++) {
                    hashes[i] = hashes[i] % element_size;
                }

                std::vector<uint32_t> channel2rows[element_size];
                for (uint32_t i = 0; i < rows; i++) {
                    channel2rows[hashes[i]].emplace_back(i);
                }

                for (int i = 0; i < element_size; ++i) {
                    if (!channel2rows[i].empty()) {
                        _add_rows_colocate_blocks(block.get(), i, channel2rows[i]);
                    }
                }
            }
        } else {
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
        RETURN_IF_ERROR(ScannerContext::init());
        if (_need_colocate_distribute) {
            _init_colocate_block();
        }
        return Status::OK();
    }

    void _init_colocate_block() {
        int real_block_size =
                limit == -1 ? _batch_size : std::min(static_cast<int64_t>(_batch_size), limit);
        int64_t free_blocks_memory_usage = 0;
        for (int i = 0; i < _num_parallel_instances; ++i) {
            auto block = vectorized::Block::create_unique(
                    _output_tuple_desc->slots(), real_block_size, true /*ignore invalid slots*/);
            free_blocks_memory_usage += block->allocated_bytes();
            _colocate_mutable_blocks.emplace_back(
                    vectorized::MutableBlock::create_unique(block.get()));
            _colocate_blocks.emplace_back(std::move(block));
            _colocate_block_mutexs.emplace_back(new std::mutex);
        }
        _free_blocks_memory_usage->add(free_blocks_memory_usage);
    }

    void _dispose_coloate_blocks_not_in_queue() override {
        if (_need_colocate_distribute) {
            for (int i = 0; i < _num_parallel_instances; ++i) {
                std::scoped_lock s(*_colocate_block_mutexs[i], *_queue_mutexs[i]);
                if (_colocate_blocks[i] && !_colocate_blocks[i]->empty()) {
                    _current_used_bytes += _colocate_blocks[i]->allocated_bytes();
                    _blocks_queues[i].emplace_back(std::move(_colocate_blocks[i]));
                    _colocate_mutable_blocks[i]->clear();
                }
            }
        }
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

    const std::vector<int> _col_distribute_ids;
    const bool _need_colocate_distribute;
    std::vector<vectorized::BlockUPtr> _colocate_blocks;
    std::vector<std::unique_ptr<vectorized::MutableBlock>> _colocate_mutable_blocks;
    std::vector<std::unique_ptr<std::mutex>> _colocate_block_mutexs;

    void _add_rows_colocate_blocks(vectorized::Block* block, int loc,
                                   const std::vector<uint32_t>& rows) {
        int row_wait_add = rows.size();
        const int batch_size = _batch_size;
        const uint32_t* begin = rows.data();
        std::lock_guard<std::mutex> l(*_colocate_block_mutexs[loc]);

        while (row_wait_add > 0) {
            int row_add = 0;
            int max_add = batch_size - _colocate_mutable_blocks[loc]->rows();
            if (row_wait_add >= max_add) {
                row_add = max_add;
            } else {
                row_add = row_wait_add;
            }

            _colocate_mutable_blocks[loc]->add_rows(block, begin, begin + row_add);
            row_wait_add -= row_add;
            begin += row_add;

            if (row_add == max_add) {
                _current_used_bytes += _colocate_blocks[loc]->allocated_bytes();
                {
                    std::lock_guard<std::mutex> queue_l(*_queue_mutexs[loc]);
                    _blocks_queues[loc].emplace_back(std::move(_colocate_blocks[loc]));
                }
                _colocate_blocks[loc] = get_free_block();
                _colocate_mutable_blocks[loc]->set_mutable_columns(
                        _colocate_blocks[loc]->mutate_columns());
            }
        }
    }
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
                                int id, bool wait = false) override {
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
