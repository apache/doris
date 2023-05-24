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

#include "vec/core/block.h"
#include "vec/spill/spill_reader.h"
#include "vec/spill/spill_writer.h"

namespace doris {

namespace vectorized {
Status SpillStream::prepare() {
    writer_.reset(new SpillWriter(stream_id_, batch_size_, spill_dir_, profile_));
    RETURN_IF_ERROR(writer_->open());

    reader_.reset(new SpillReader(stream_id_, spill_dir_, profile_));
    return Status::OK();
}

Status SpillStream::get_next(Block* block, bool* eos) {
    DCHECK(!io_running_);
    io_running_ = true;
    Status status;
    if (!spilled_) {
        *block = std::move(blocks_.front());
        blocks_.pop_front();
    } else {
        status = io_thread_pool_->submit_func([this, block, eos] {
            auto st = reader_->read(block, eos);
            io_status_.set_value(st);
            io_running_ = false;
        });
    }
    return status;
}

Status SpillStream::flush() {
    spilled_ = true;
    DCHECK(!io_running_);
    io_running_ = true;
    auto status = io_thread_pool_->submit_func([this] {
        for (auto it = blocks_.begin(); it != blocks_.end();) {
            auto st = writer_->write(*it);
            if (!st.ok()) {
                io_status_.set_value(st);
                break;;
            }
            it = blocks_.erase(it);
        }
        blocks_.clear();
        io_status_.set_value(Status::OK());
        io_running_ = false;
    });
    // RETURN_IF_ERROR(thread_status.get_future().get());
    return status;
}

} // namespace vectorized
} // namespace doris