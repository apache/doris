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

#include <memory>
#include <mutex>
#include <utility>

#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "vec/core/block.h"
#include "vec/spill/spill_reader.h"
#include "vec/spill/spill_stream_manager.h"
#include "vec/spill/spill_writer.h"

namespace doris::vectorized {
SpillStream::SpillStream(RuntimeState* state, int64_t stream_id, SpillDataDir* data_dir,
                         std::string spill_dir, size_t batch_rows, size_t batch_bytes,
                         RuntimeProfile* profile)
        : state_(state),
          stream_id_(stream_id),
          data_dir_(data_dir),
          spill_dir_(std::move(spill_dir)),
          batch_rows_(batch_rows),
          batch_bytes_(batch_bytes),
          profile_(profile) {
    io_thread_pool_ =
            ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool(data_dir->path());
}

Status SpillStream::prepare() {
    writer_ = std::make_unique<SpillWriter>(stream_id_, batch_rows_, data_dir_, spill_dir_);

    reader_ = std::make_unique<SpillReader>(stream_id_, writer_->get_file_path());
    return Status::OK();
}

void SpillStream::close() {
    if (closed_) {
        return;
    }
    VLOG_ROW << "closing: " << stream_id_;
    closed_ = true;
    if (spill_promise_) {
        spill_future_.wait();
        spill_promise_.reset();
    }
    if (read_promise_) {
        read_future_.wait();
        read_promise_.reset();
    }

    (void)writer_->close();
    (void)reader_->close();
}

const std::string& SpillStream::get_spill_root_dir() const {
    return data_dir_->path();
}
Status SpillStream::prepare_spill() {
    DCHECK(!spill_promise_);
    RETURN_IF_ERROR(writer_->open());

    spill_promise_ = std::make_unique<std::promise<Status>>();
    spill_future_ = spill_promise_->get_future();
    return Status::OK();
}
void SpillStream::end_spill(const Status& status) {
    spill_promise_->set_value(status);
}

Status SpillStream::wait_spill() {
    if (spill_promise_) {
        auto status = spill_future_.get();
        spill_promise_.reset();
        return status;
    }
    return Status::OK();
}

Status SpillStream::spill_block(const Block& block, bool eof) {
    size_t written_bytes = 0;
    RETURN_IF_ERROR(writer_->write(block, written_bytes));
    if (eof) {
        return writer_->close();
    }
    return Status::OK();
}

Status SpillStream::spill_eof() {
    return writer_->close();
}

Status SpillStream::read_next_block_sync(Block* block, bool* eos) {
    DCHECK(!read_promise_);
    DCHECK(reader_ != nullptr);
    Status status;
    read_promise_ = std::make_unique<std::promise<Status>>();
    read_future_ = read_promise_->get_future();
    // use thread pool to limit concurrent io tasks
    status = io_thread_pool_->submit_func([this, block, eos] {
        SCOPED_ATTACH_TASK(state_);
        Status st;
        Defer defer {[&]() { read_promise_->set_value(st); }};
        st = reader_->open();
        if (!st.ok()) {
            return;
        }
        st = reader_->read(block, eos);
    });
    if (!status.ok()) {
        LOG(WARNING) << "read spill data failed: " << status;
        read_promise_.reset();
        return status;
    }

    status = read_future_.get();
    read_promise_.reset();
    return status;
}

} // namespace doris::vectorized
