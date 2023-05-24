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
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/status.h"
#include "runtime/exec_env.h"
#include "vec/spill/spill_reader.h"
#include "vec/spill/spill_writer.h"

namespace doris {
class RuntimeProfile;

namespace vectorized {

class Block;

class SpillStream {
public:
    SpillStream(int64_t stream_id, std::string spill_dir, int32_t batch_size,
                RuntimeProfile* profile)
            : stream_id_(stream_id),
              spill_dir_(spill_dir),
              batch_size_(batch_size),
              profile_(profile) {
        io_thread_pool_ = ExecEnv::GetInstance()->spill_io_pool();
    }

    Status prepare();

    // add a block to blocks_
    Status add_block(Block& block) {
        blocks_.push_back(block);
        return Status::OK();
    }

    Status get_next(Block* block, bool* eos);

    // write blocks_ to disk asynchronously
    Status flush();

    int64 id() const { return stream_id_; }

private:
    ThreadPool* io_thread_pool_;
    int64_t stream_id_;
    std::string spill_dir_;
    int32_t batch_size_;
    bool spilled_ = false;
    bool io_running_ = false;
    std::deque<Block> blocks_;
    SpillWriterUPtr writer_;
    SpillReaderUPtr reader_;
    std::promise<Status> io_status_;

    RuntimeProfile* profile_ = nullptr;
};
using SpillStreamSPtr = std::shared_ptr<SpillStream>;
} // namespace vectorized
} // namespace doris