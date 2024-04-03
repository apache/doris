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
#include <future>
#include <memory>

#include "vec/spill/spill_reader.h"
#include "vec/spill/spill_writer.h"

namespace doris {
class RuntimeProfile;
class ThreadPool;

namespace vectorized {

class Block;
class SpillDataDir;

class SpillStream {
public:
    // to avoid too many small file writes
    static constexpr int MIN_SPILL_WRITE_BATCH_MEM = 32 * 1024;
    SpillStream(RuntimeState* state, int64_t stream_id, SpillDataDir* data_dir,
                std::string spill_dir, size_t batch_rows, size_t batch_bytes,
                RuntimeProfile* profile);

    int64_t id() const { return stream_id_; }

    SpillDataDir* get_data_dir() const { return data_dir_; }
    const std::string& get_spill_root_dir() const;

    const std::string& get_spill_dir() const { return spill_dir_; }

    size_t get_written_bytes() const { return writer_->get_written_bytes(); }

    Status prepare_spill();

    Status spill_block(const Block& block, bool eof);

    void end_spill(const Status& status);

    Status spill_eof();

    Status wait_spill();

    Status read_next_block_sync(Block* block, bool* eos);

    void set_write_counters(RuntimeProfile::Counter* serialize_timer,
                            RuntimeProfile::Counter* write_block_counter,
                            RuntimeProfile::Counter* write_bytes_counter,
                            RuntimeProfile::Counter* write_timer,
                            RuntimeProfile::Counter* wait_io_timer) {
        writer_->set_counters(serialize_timer, write_block_counter, write_bytes_counter,
                              write_timer);
        write_wait_io_timer_ = wait_io_timer;
    }

    void set_read_counters(RuntimeProfile::Counter* read_timer,
                           RuntimeProfile::Counter* deserialize_timer,
                           RuntimeProfile::Counter* read_bytes,
                           RuntimeProfile::Counter* wait_io_timer) {
        reader_->set_counters(read_timer, deserialize_timer, read_bytes);
        read_wait_io_timer_ = wait_io_timer;
    }

private:
    friend class SpillStreamManager;

    Status prepare();

    void close();

    RuntimeState* state_ = nullptr;
    ThreadPool* io_thread_pool_;
    int64_t stream_id_;
    std::atomic_bool closed_ = false;
    SpillDataDir* data_dir_ = nullptr;
    std::string spill_dir_;
    size_t batch_rows_;
    size_t batch_bytes_;

    std::unique_ptr<std::promise<Status>> spill_promise_;
    std::future<Status> spill_future_;
    std::unique_ptr<std::promise<Status>> read_promise_;
    std::future<Status> read_future_;

    SpillWriterUPtr writer_;
    SpillReaderUPtr reader_;

    RuntimeProfile* profile_ = nullptr;
    RuntimeProfile::Counter* write_wait_io_timer_ = nullptr;
    RuntimeProfile::Counter* read_wait_io_timer_ = nullptr;
};
using SpillStreamSPtr = std::shared_ptr<SpillStream>;
} // namespace vectorized
} // namespace doris