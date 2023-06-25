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
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "common/status.h"
#include "runtime/exec_env.h"
#include "vec/core/block.h"
#include "vec/spill/spill_reader.h"
#include "vec/spill/spill_writer.h"

namespace doris {
class RuntimeProfile;

namespace vectorized {

class Block;

using flush_stream_callback = std::function<void(const Status&)>;

class SpillStream;

class SpillableBlock {
public:
    SpillableBlock(Block&& block) : block_(block), spilled_(false) {}
    Block& get_block() { return block_; }

private:
    friend class SpillStream;

    Block block_;
    std::atomic_bool spilled_ = false;
    std::atomic_bool pin_in_flight = false;
    std::string file_path_;
    int fd_ = -1;
    size_t spill_data_size_ = 0;
    int64_t offset_ = -1;
};
using SpillableBlockSPtr = std::shared_ptr<SpillableBlock>;
class SpillStream {
public:
    SpillStream(int64_t stream_id, std::string spill_dir, size_t batch_rows, size_t batch_bytes,
                RuntimeProfile* profile)
            : stream_id_(stream_id),
              spill_dir_(spill_dir),
              batch_rows_(batch_rows),
              batch_bytes_(batch_bytes),
              profile_(profile) {
        io_thread_pool_ = ExecEnv::GetInstance()->spill_io_pool();
    }

    Status prepare();

    void close();

    void unpin();

    // add rows in block to the spill stream
    Status add_rows(Block* block, const std::vector<int>& rows, bool pin, bool eos);

    // add blocks to blocks_
    Status add_blocks(std::vector<Block>&& blocks, bool pin);

    Status done_write();

    bool has_next() const { return eos_; }

    Status get_next(Block* block, bool* eos);

    // write blocks_ to disk asynchronously
    // Status flush(const flush_stream_callback& cb);

    void spill();

    Status restore();

    bool is_spilling();

    bool is_spilled() const { return spilled_; }

    int64_t id() const { return stream_id_; }

    int64_t total_bytes() const { return total_bytes_; }

    size_t spillable_data_size();

    bool has_in_memory_blocks();

private:
    bool _block_reach_limit() const {
        return mutable_block_->rows() > batch_rows_ || mutable_block_->bytes() > batch_bytes_;
    }

    SpillableBlockSPtr _get_next_spilled_block();

    void _update_status(Status status) {
        if (!status.ok()) {
            std::lock_guard guard(status_lock_);
            status_ = std::move(status);
        }
    }
    Status _status() {
        std::lock_guard guard(status_lock_);
        return status_;
    }

    Status _flush_internal();

    Status _read_async();

    ThreadPool* io_thread_pool_;
    std::unique_ptr<MutableBlock> mutable_block_;
    int64_t stream_id_;
    std::string spill_dir_;
    int fd_ = -1;
    size_t batch_rows_;
    size_t batch_bytes_;
    int64_t total_rows_ = 0;
    int64_t total_bytes_ = 0;
    std::atomic_bool spilled_ = false;
    std::atomic_bool eos_ = false;

    // protect in_mem_blocks_, dirty_blocks_ and write_in_flight_blocks_
    std::mutex lock_;
    std::unique_ptr<std::promise<Status>> spill_promise_;
    Status spill_status_;
    std::condition_variable write_complete_cv_;
    std::deque<SpillableBlockSPtr> in_mem_blocks_;
    std::deque<SpillableBlockSPtr> dirty_blocks_;
    std::deque<SpillableBlockSPtr> spilled_blocks_;
    // std::deque<SpillableBlockSPtr> write_in_flight_blocks_;

    SpillWriterUPtr writer_;
    SpillReaderUPtr reader_;
    // std::promise<Status> io_status_;

    std::mutex status_lock_;
    Status status_;

    RuntimeProfile* profile_ = nullptr;
};
using SpillStreamSPtr = std::shared_ptr<SpillStream>;
} // namespace vectorized
} // namespace doris