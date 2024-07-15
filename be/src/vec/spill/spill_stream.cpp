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
    bool exists = false;
    auto status = io::global_local_filesystem()->exists(spill_dir_, &exists);
    if (status.ok() && exists) {
        auto query_dir = fmt::format("{}/{}/{}", get_data_dir()->path(), SPILL_GC_DIR_PREFIX,
                                     print_id(query_id_));
        (void)io::global_local_filesystem()->create_directory(query_dir);
        auto gc_dir = fmt::format("{}/{}", query_dir,
                                  std::filesystem::path(spill_dir_).filename().string());
        (void)io::global_local_filesystem()->rename(spill_dir_, gc_dir);
    }
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
    return writer_->open();
}

Status SpillStream::spill_block(RuntimeState* state, const Block& block, bool eof) {
    size_t written_bytes = 0;
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

    RETURN_IF_ERROR(reader_->open());
    return reader_->read(block, eos);
}

void SpillStream::decrease_spill_data_usage() {
    data_dir_->update_spill_data_usage(-total_written_bytes_);
}

} // namespace doris::vectorized
