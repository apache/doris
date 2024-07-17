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
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/debug_points.h"
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
          query_id_(state->query_id()),
          profile_(profile) {}

SpillStream::~SpillStream() {
    gc();
}

void SpillStream::gc() {
    bool exists = false;
    auto status = io::global_local_filesystem()->exists(spill_dir_, &exists);
    if (status.ok() && exists) {
        auto query_gc_dir = data_dir_->get_spill_data_gc_path(print_id(query_id_));
        status = io::global_local_filesystem()->create_directory(query_gc_dir);
        DBUG_EXECUTE_IF("fault_inject::spill_stream::gc", {
            status = Status::Error<INTERNAL_ERROR>("fault_inject spill_stream gc failed");
        });
        if (status.ok()) {
            auto gc_dir = fmt::format("{}/{}", query_gc_dir,
                                      std::filesystem::path(spill_dir_).filename().string());
            status = io::global_local_filesystem()->rename(spill_dir_, gc_dir);
        }
        if (!status.ok()) {
            LOG_EVERY_T(WARNING, 1) << fmt::format("failed to gc spill data, dir {}, error: {}",
                                                   query_gc_dir, status.to_string());
        }
    }
    // If QueryContext is destructed earlier than PipelineFragmentContext,
    // spill_dir_ will be already moved to spill_gc directory.

    // decrease spill data usage anyway, since in ~QueryContext() spill data of the query will be
    // clean up as a last resort
    data_dir_->update_spill_data_usage(-total_written_bytes_);
    total_written_bytes_ = 0;
}

Status SpillStream::prepare() {
    writer_ = std::make_unique<SpillWriter>(stream_id_, batch_rows_, data_dir_, spill_dir_);

    reader_ = std::make_unique<SpillReader>(stream_id_, writer_->get_file_path());
    return Status::OK();
}

const TUniqueId& SpillStream::query_id() const {
    return query_id_;
}

const std::string& SpillStream::get_spill_root_dir() const {
    return data_dir_->path();
}
Status SpillStream::prepare_spill() {
    DBUG_EXECUTE_IF("fault_inject::spill_stream::prepare_spill", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_stream prepare_spill failed");
    });
    return writer_->open();
}

Status SpillStream::spill_block(RuntimeState* state, const Block& block, bool eof) {
    size_t written_bytes = 0;
    DBUG_EXECUTE_IF("fault_inject::spill_stream::spill_block", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_stream spill_block failed");
    });
    RETURN_IF_ERROR(writer_->write(state, block, written_bytes));
    if (eof) {
        RETURN_IF_ERROR(writer_->close());
        total_written_bytes_ = writer_->get_written_bytes();
        writer_.reset();
    } else {
        total_written_bytes_ = writer_->get_written_bytes();
    }
    return Status::OK();
}

Status SpillStream::spill_eof() {
    DBUG_EXECUTE_IF("fault_inject::spill_stream::spill_eof", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_stream spill_eof failed");
    });
    RETURN_IF_ERROR(writer_->close());
    total_written_bytes_ = writer_->get_written_bytes();
    writer_.reset();
    return Status::OK();
}

Status SpillStream::read_next_block_sync(Block* block, bool* eos) {
    DCHECK(reader_ != nullptr);
    DCHECK(!_is_reading);
    _is_reading = true;
    Defer defer([this] { _is_reading = false; });

    DBUG_EXECUTE_IF("fault_inject::spill_stream::read_next_block", {
        return Status::Error<INTERNAL_ERROR>("fault_inject spill_stream read_next_block failed");
    });
    RETURN_IF_ERROR(reader_->open());
    return reader_->read(block, eos);
}

} // namespace doris::vectorized
