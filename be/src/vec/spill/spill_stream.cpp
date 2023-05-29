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

Status SpillStream::flush(const flush_stream_callback& cb) {
    spilled_ = true;
    DCHECK(!is_flushing_);
    is_flushing_ = true;
    if (cb) {
        auto status = io_thread_pool_->submit_func([this, cb] {
            Status st = _flush_internal();
            io_status_.set_value(st);
            cb(st);
        });
        return status;
    } else {
        return _flush_internal();
    }
}

Status SpillStream::_flush_internal() {
    Status st;
    for (auto it = blocks_.begin(); it != blocks_.end();) {
        st = writer_->write(*it);
        if (!st.ok()) {
            break;
        }
        it = blocks_.erase(it);
    }
    blocks_.clear();
    is_flushing_ = false;
    return st;
}
Status SpillStream::restore() {
    DCHECK(is_restoring_);
    is_restoring_ = true;
    auto status = io_thread_pool_->submit_func([this] {
        Block block;
        bool eos = false;
        while (!eos) {
            auto st = reader_->read(&block, &eos);
            if (!st.ok()) {
                io_status_.set_value(st);
                break;
            }
            blocks_.push_back(std::move(block));
        }
        is_restoring_ = false;
    });
    return Status::OK();
}

Status SpillStream::get_next(Block* block, bool* eos) {
    if (!spilled_) {
        if (blocks_.empty()) {
            *eos = true;
            return Status::OK();
        }
        *block = std::move(blocks_.front());
        blocks_.pop_front();
        if (blocks_.empty()) {
            *eos = true;
        }
        return Status::OK();
    } else {
        DCHECK(!is_reading_);
        is_reading_ = true;
        /*
        status = io_thread_pool_->submit_func([this, block, eos] {
            auto st = reader_->read(block, eos);
            io_status_.set_value(st);
            is_reading_ = false;
        });
        */
        return reader_->read(block, eos);
    }
}

} // namespace vectorized
} // namespace doris