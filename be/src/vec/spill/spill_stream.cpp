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

#include "vec/spill/spill_stream.h"

#include <glog/logging.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>

#include "vec/core/block.h"
#include "vec/spill/spill_reader.h"
#include "vec/spill/spill_writer.h"

namespace doris {

namespace vectorized {
Status SpillStream::prepare() {
    writer_.reset(new SpillWriter(stream_id_, batch_rows_, spill_dir_, profile_));
    RETURN_IF_ERROR(writer_->open());
    fd_ = writer_->get_fd();

    reader_.reset(new SpillReader(stream_id_, spill_dir_, profile_));
    return Status::OK();
}

void SpillStream::close() {}
Status SpillStream::add_rows(Block* block, const std::vector<int>& rows, bool pin, bool eos) {
    if (mutable_block_ == nullptr) {
        mutable_block_ = MutableBlock::create_unique(block->clone_empty());
    }

    total_rows_ += rows.size();

    auto pre_bytes_ = mutable_block_->bytes();
    RETURN_IF_CATCH_EXCEPTION(mutable_block_->add_rows(block, &rows[0], &rows[0] + rows.size()));
    auto new_bytes = mutable_block_->bytes();
    total_bytes_ += new_bytes - pre_bytes_;
    if (_block_reach_limit() || eos) {
        auto new_block = mutable_block_->to_block();
        {
            std::lock_guard l(lock_);
            if (pin) {
                in_mem_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(new_block)));
            } else {
                dirty_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(new_block)));
            }
        }
        mutable_block_ = MutableBlock::create_unique(block->clone_empty());
    }
    return Status::OK();
}

Status SpillStream::add_blocks(std::vector<Block>&& blocks, bool pin) {
    std::lock_guard l(lock_);
    if (pin) {
        for (auto& block : blocks) {
            in_mem_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(block)));
        }
    } else {
        for (auto& block : blocks) {
            dirty_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(block)));
        }
    }
    return Status::OK();
}

Status SpillStream::done_write() {
    if (mutable_block_ && mutable_block_->rows() > 0) {
        auto new_block = mutable_block_->to_block();
        {
            std::lock_guard l(lock_);
            in_mem_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(new_block)));
        }
    }
    mutable_block_.reset();
    return Status::OK();
}

void SpillStream::unpin() {
    std::lock_guard l(lock_);
    dirty_blocks_.insert(dirty_blocks_.end(), in_mem_blocks_.cbegin(), in_mem_blocks_.cend());
    in_mem_blocks_.clear();
}

void SpillStream::spill() {
    DCHECK(!spill_promise_);
    spilled_ = true;
    spill_promise_ = std::make_unique<std::promise<Status>>();
    std::lock_guard l(lock_);
    for (auto& block : dirty_blocks_) {
        block->spilled_ = true;
        block->fd_ = fd_;
        block->offset_ = writer_->get_written_bytes();
        auto st = writer_->write(block->block_, block->spill_data_size_);
        if (!st.ok()) {
            spill_promise_->set_value(st);
            return;
        }
        block->block_.swap(Block());
        spilled_blocks_.emplace_back(block);
    }
    spill_promise_->set_value(Status::OK());
}

bool SpillStream::is_spilling() {
    if (spill_promise_) {
        auto future = spill_promise_->get_future();
        auto status = future.wait_for(std::chrono::milliseconds(10));
        if (status == std::future_status::ready) {
            spill_status_ = future.get();
            spill_promise_ = nullptr;
            return true;
        } else {
            return false;
        }
    }
    return false;
}
size_t SpillStream::spillable_data_size() {
    size_t size = 0;
    std::lock_guard l(lock_);
    for (const auto& block : in_mem_blocks_) {
        size += block->block_.allocated_bytes();
    }
    for (const auto& block : dirty_blocks_) {
        size += block->block_.allocated_bytes();
    }
    return size;
}

Status SpillStream::get_next(Block* block, bool* eos) {
    std::lock_guard l(lock_);
    if (in_mem_blocks_.empty() && dirty_blocks_.empty() && spilled_blocks_.empty()) {
        *eos = true;
        eos_ = true;
        return Status::OK();
    }
    if (!in_mem_blocks_.empty()) {
        *block = std::move(in_mem_blocks_.front()->get_block());
        in_mem_blocks_.pop_front();
        return Status::OK();
    } else if (!dirty_blocks_.empty()) {
        *block = std::move(dirty_blocks_.front()->get_block());
        dirty_blocks_.pop_front();
        return Status::OK();
    } else {
        // initiate async read
        RETURN_IF_ERROR(_read_async());
        return Status::WaitForIO("reading spilled blocks");
    }
}

SpillableBlockSPtr SpillStream::_get_next_spilled_block() {
    std::lock_guard l(lock_);
    if (spilled_blocks_.empty()) {
        return nullptr;
    }
    auto block = spilled_blocks_.front();
    spilled_blocks_.pop_front();
    return block;
}

Status SpillStream::_read_async() {
#define CHECK_STATUS(st)    \
    do {                    \
        _update_status(st); \
        if (!st.ok()) {     \
            return;         \
        }                   \
    } while (0)
    auto status = io_thread_pool_->submit_func([this] {
        Status st;
        auto spilled_block = _get_next_spilled_block();
        while (spilled_block) {
            st = reader_->read_at_offset(spilled_block->offset_, spilled_block->spill_data_size_,
                                         &spilled_block->block_);
            CHECK_STATUS(st);
            {
                std::lock_guard l(lock_);
                in_mem_blocks_.push_back(spilled_block);
            }
            spilled_block = _get_next_spilled_block();
        }
    });
    return status;
}

bool SpillStream::has_in_memory_blocks() {
    std::lock_guard l(lock_);
    return !in_mem_blocks_.empty() || !dirty_blocks_.empty();
}

} // namespace vectorized
} // namespace doris