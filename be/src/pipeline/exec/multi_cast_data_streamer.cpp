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

#include "multi_cast_data_streamer.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <iterator>
#include <memory>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/multi_cast_data_stream_source.h"
#include "pipeline/exec/spill_utils.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/pretty_printer.h"
#include "util/uid_util.h"
#include "vec/core/block.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
MultiCastBlock::MultiCastBlock(vectorized::Block* block, int un_finish_copy, size_t mem_size)
        : _un_finish_copy(un_finish_copy), _mem_size(mem_size) {
    _block = vectorized::Block::create_unique(block->get_columns_with_type_and_name());
    block->clear();
}

Status MultiCastDataStreamer::pull(RuntimeState* state, int sender_idx, vectorized::Block* block,
                                   bool* eos) {
    MultiCastBlock* multi_cast_block = nullptr;
    {
        INJECT_MOCK_SLEEP(std::lock_guard l(_mutex));
        for (auto it = _spill_readers[sender_idx].begin();
             it != _spill_readers[sender_idx].end();) {
            if ((*it)->all_data_read) {
                it = _spill_readers[sender_idx].erase(it);
            } else {
                it++;
            }
        }

        if (!_cached_blocks[sender_idx].empty()) {
            *block = std::move(_cached_blocks[sender_idx].front());
            _cached_blocks[sender_idx].erase(_cached_blocks[sender_idx].begin());

            /** Eos:
                * 1. `_eos` is true means no more data will be added into queue.
                * 2. `_cached_blocks[sender_idx]` blocks recovered from spill.
                * 3. `_spill_readers[sender_idx].empty()` means there are no blocks on disk.
                * 4. `_sender_pos_to_read[sender_idx] == _multi_cast_blocks.end()` means no more blocks in queue.
            */
            *eos = _eos && _cached_blocks[sender_idx].empty() &&
                   _spill_readers[sender_idx].empty() &&
                   _sender_pos_to_read[sender_idx] == _multi_cast_blocks.end();
            return Status::OK();
        }

        if (!_spill_readers[sender_idx].empty()) {
            auto reader_item = _spill_readers[sender_idx].front();
            if (!reader_item->stream->ready_for_reading()) {
                return Status::OK();
            }

            auto& reader = reader_item->reader;
            RETURN_IF_ERROR(reader->open());
            if (reader_item->block_offset != 0) {
                reader->seek(reader_item->block_offset);
                reader_item->block_offset = 0;
            }

            auto spill_func = [this, reader_item, sender_idx]() {
                vectorized::Block block;
                bool spill_eos = false;
                size_t read_size = 0;
                while (!spill_eos) {
                    RETURN_IF_ERROR(reader_item->reader->read(&block, &spill_eos));
                    if (!block.empty()) {
                        std::lock_guard l(_mutex);
                        read_size += block.allocated_bytes();
                        _cached_blocks[sender_idx].emplace_back(std::move(block));
                        if (_cached_blocks[sender_idx].size() >= 32 ||
                            read_size > 2 * 1024 * 1024) {
                            break;
                        }
                    }
                }

                if (spill_eos || !_cached_blocks[sender_idx].empty()) {
                    reader_item->all_data_read = spill_eos;
                    _set_ready_for_read(sender_idx);
                }
                return Status::OK();
            };

            auto catch_exception_func = [spill_func = std::move(spill_func)]() {
                RETURN_IF_CATCH_EXCEPTION(return spill_func(););
            };

            _spill_read_dependencies[sender_idx]->block();
            auto spill_runnable = std::make_shared<SpillRecoverRunnable>(
                    state, _spill_read_dependencies[sender_idx],
                    _source_operator_profiles[sender_idx], _shared_state->shared_from_this(),
                    catch_exception_func);
            auto* thread_pool =
                    ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();
            RETURN_IF_ERROR(thread_pool->submit(std::move(spill_runnable)));
            return Status::OK();
        }

        auto& pos_to_pull = _sender_pos_to_read[sender_idx];
        const auto end = _multi_cast_blocks.end();
        if (pos_to_pull == end) {
            _block_reading(sender_idx);
            VLOG_DEBUG << "Query: " << print_id(state->query_id())
                       << ", pos_to_pull end: " << (void*)(_write_dependency);
            *eos = _eos;
            return Status::OK();
        }

        DCHECK_GT(pos_to_pull->_un_finish_copy, 0);
        DCHECK_LE(pos_to_pull->_un_finish_copy, _cast_sender_count);
        *block = *pos_to_pull->_block;

        multi_cast_block = &(*pos_to_pull);
        _copying_count.fetch_add(1);

        pos_to_pull++;

        if (pos_to_pull == end) {
            _block_reading(sender_idx);
            *eos = _eos;
        }
    }

    return _copy_block(state, sender_idx, block, *multi_cast_block);
}

Status MultiCastDataStreamer::_copy_block(RuntimeState* state, int32_t sender_idx,
                                          vectorized::Block* block,
                                          MultiCastBlock& multi_cast_block) {
    const auto rows = block->rows();
    for (int i = 0; i < block->columns(); ++i) {
        block->get_by_position(i).column = block->get_by_position(i).column->clone_resized(rows);
    }

    INJECT_MOCK_SLEEP(std::lock_guard l(_mutex));
    multi_cast_block._un_finish_copy--;
    auto copying_count = _copying_count.fetch_sub(1) - 1;
    if (multi_cast_block._un_finish_copy == 0) {
        DCHECK_EQ(_multi_cast_blocks.front()._un_finish_copy, 0);
        DCHECK_EQ(&(_multi_cast_blocks.front()), &multi_cast_block);
        _multi_cast_blocks.pop_front();
        _write_dependency->set_ready();
    } else if (copying_count == 0) {
        bool spilled = false;
        RETURN_IF_ERROR(_trigger_spill_if_need(state, &spilled));
    }

    return Status::OK();
}

Status MultiCastDataStreamer::_trigger_spill_if_need(RuntimeState* state, bool* triggered) {
    if (!state->enable_spill()) {
        *triggered = false;
        return Status::OK();
    }

    vectorized::SpillStreamSPtr spill_stream;
    *triggered = false;
    if (_cumulative_mem_size.load() >= config::exchg_node_buffer_size_bytes &&
        _multi_cast_blocks.size() >= 4) {
        _write_dependency->block();

        if (_copying_count.load() != 0) {
            return Status::OK();
        }

        bool has_reached_end = false;
        std::vector<int64_t> distances(_cast_sender_count);
        size_t total_count = _multi_cast_blocks.size();
        for (int i = 0; i < _sender_pos_to_read.size(); ++i) {
            distances[i] = std::distance(_multi_cast_blocks.begin(), _sender_pos_to_read[i]);
            if (_sender_pos_to_read[i] == _multi_cast_blocks.end()) {
                has_reached_end = true;
                CHECK_EQ(distances[i], total_count);
            }

            if (!_spill_readers[i].empty()) {
                CHECK_EQ(distances[i], 0);
            }
        }

        if (has_reached_end) {
            RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                    state, spill_stream, print_id(state->query_id()), "MultiCastSender", _node_id,
                    std::numeric_limits<int32_t>::max(), std::numeric_limits<size_t>::max(),
                    _sink_operator_profile));
            for (int i = 0; i < _sender_pos_to_read.size(); ++i) {
                if (distances[i] < total_count) {
                    auto reader = spill_stream->create_separate_reader();
                    reader->set_counters(_source_operator_profiles[i]);
                    auto reader_item = std::make_shared<SpillingReader>(
                            std::move(reader), spill_stream, distances[i], false);
                    _spill_readers[i].emplace_back(std::move(reader_item));
                }

                _block_reading(i);
            }

            RETURN_IF_ERROR(_submit_spill_task(state, spill_stream));
            DCHECK_EQ(_multi_cast_blocks.size(), 0);

            for (auto& pos : _sender_pos_to_read) {
                pos = _multi_cast_blocks.end();
            }
            _cumulative_mem_size = 0;
            *triggered = true;
        }
    }

    return Status::OK();
}

Status MultiCastDataStreamer::_submit_spill_task(RuntimeState* state,
                                                 vectorized::SpillStreamSPtr spill_stream) {
    std::vector<vectorized::Block> blocks;
    for (auto& block : _multi_cast_blocks) {
        DCHECK_GT(block._block->rows(), 0);
        blocks.emplace_back(std::move(*block._block));
    }

    _multi_cast_blocks.clear();

    auto spill_func = [state, blocks = std::move(blocks),
                       spill_stream = std::move(spill_stream)]() mutable {
        const auto blocks_count = blocks.size();
        while (!blocks.empty() && !state->is_cancelled()) {
            auto block = std::move(blocks.front());
            blocks.erase(blocks.begin());

            RETURN_IF_ERROR(spill_stream->spill_block(state, block, false));
        }
        VLOG_DEBUG << "Query: " << print_id(state->query_id()) << " multi cast write "
                   << blocks_count << " blocks";
        return spill_stream->spill_eof();
    };

    auto exception_catch_func = [spill_func = std::move(spill_func),
                                 query_id = print_id(state->query_id()), this]() mutable {
        auto status = [&]() { RETURN_IF_CATCH_EXCEPTION(return spill_func()); }();
        _write_dependency->set_ready();

        if (!status.ok()) {
            LOG(WARNING) << "Query: " << query_id
                         << " multi cast write failed: " << status.to_string()
                         << ", dependency: " << (void*)_spill_dependency.get();
        } else {
            for (int i = 0; i < _sender_pos_to_read.size(); ++i) {
                _set_ready_for_read(i);
            }
        }
        return status;
    };

    auto spill_runnable = std::make_shared<SpillSinkRunnable>(
            state, nullptr, _spill_dependency, _sink_operator_profile,
            _shared_state->shared_from_this(), exception_catch_func);

    _spill_dependency->block();

    auto* thread_pool = ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool();
    return thread_pool->submit(std::move(spill_runnable));
}

Status MultiCastDataStreamer::push(RuntimeState* state, doris::vectorized::Block* block, bool eos) {
    auto rows = block->rows();
    COUNTER_UPDATE(_process_rows, rows);

    const auto block_mem_size = block->allocated_bytes();

    {
        INJECT_MOCK_SLEEP(std::lock_guard l(_mutex));
        if (_pending_block) {
            DCHECK_GT(_pending_block->rows(), 0);
            const auto pending_size = _pending_block->allocated_bytes();
            _cumulative_mem_size += pending_size;
            _multi_cast_blocks.emplace_back(_pending_block.get(), _cast_sender_count, pending_size);
            _pending_block.reset();

            auto last_elem = std::prev(_multi_cast_blocks.end());
            for (int i = 0; i < _sender_pos_to_read.size(); ++i) {
                if (_sender_pos_to_read[i] == _multi_cast_blocks.end()) {
                    _sender_pos_to_read[i] = last_elem;
                    _set_ready_for_read(i);
                }
            }
        }

        _cumulative_mem_size += block_mem_size;
        COUNTER_SET(_peak_mem_usage,
                    std::max(_cumulative_mem_size.load(), _peak_mem_usage->value()));

        if (rows > 0) {
            if (!eos) {
                bool spilled = false;
                RETURN_IF_ERROR(_trigger_spill_if_need(state, &spilled));
                if (spilled) {
                    _pending_block = vectorized::Block::create_unique(
                            block->get_columns_with_type_and_name());
                    block->clear();
                    return Status::OK();
                }
            }

            _multi_cast_blocks.emplace_back(block, _cast_sender_count, block_mem_size);

            // last elem
            auto end = std::prev(_multi_cast_blocks.end());
            for (int i = 0; i < _sender_pos_to_read.size(); ++i) {
                if (_sender_pos_to_read[i] == _multi_cast_blocks.end()) {
                    _sender_pos_to_read[i] = end;
                    _set_ready_for_read(i);
                }
            }
        } else if (eos) {
            for (int i = 0; i < _sender_pos_to_read.size(); ++i) {
                if (_sender_pos_to_read[i] == _multi_cast_blocks.end()) {
                    _set_ready_for_read(i);
                }
            }
        }

        _eos = eos;
    }

    if (_eos) {
        for (auto* read_dep : _dependencies) {
            read_dep->set_always_ready();
        }
    }
    return Status::OK();
}

void MultiCastDataStreamer::_set_ready_for_read(int sender_idx) {
    if (_dependencies.empty()) {
        return;
    }
    auto* dep = _dependencies[sender_idx];
    DCHECK(dep);
    dep->set_ready();
}

void MultiCastDataStreamer::_block_reading(int sender_idx) {
    if (_dependencies.empty()) {
        return;
    }
    auto* dep = _dependencies[sender_idx];
    DCHECK(dep);
    dep->block();
}

std::string MultiCastDataStreamer::debug_string() {
    size_t read_ready_count = 0;
    size_t read_spill_ready_count = 0;
    size_t pos_at_end_count = 0;
    size_t blocks_count = 0;
    {
        std::unique_lock l(_mutex);
        blocks_count = _multi_cast_blocks.size();
        for (int32_t i = 0; i != _cast_sender_count; ++i) {
            if (!_dependencies[i]->is_blocked_by()) {
                read_ready_count++;
            }

            if (!_spill_read_dependencies[i]->is_blocked_by()) {
                read_spill_ready_count++;
            }

            if (_sender_pos_to_read[i] == _multi_cast_blocks.end()) {
                pos_at_end_count++;
            }
        }
    }

    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(
            debug_string_buffer,
            "MemSize: {}, blocks: {}, sender count: {}, pos_at_end_count: {}, copying_count: {} "
            "read_ready_count: {}, read_spill_ready_count: {}, write spill dependency blocked: {}",
            PrettyPrinter::print_bytes(_cumulative_mem_size), blocks_count, _cast_sender_count,
            pos_at_end_count, _copying_count.load(), read_ready_count, read_spill_ready_count,
            _spill_dependency->is_blocked_by() != nullptr);
    return fmt::to_string(debug_string_buffer);
}

} // namespace doris::pipeline