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

#include "exec/spill/spill_file.h"

#include <glog/logging.h>

#include <filesystem>
#include <memory>
#include <utility>

#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/debug_points.h"

namespace doris {
SpillFile::SpillFile(SpillDataDir* data_dir, std::string relative_path)
        : _data_dir(data_dir),
          _query_dir(relative_path.substr(0, relative_path.find('/'))),
          _spill_dir(data_dir->get_spill_data_path() + "/" + std::move(relative_path)) {}

SpillFile::~SpillFile() {
    gc();
}

void SpillFile::gc() {
    if (_dir_created) {
        // Delete the spill directory (or object key prefix) directly instead of moving it to a
        // GC directory. No existence check: for object storage a "directory" never exists as an
        // object, while deleting a missing local directory or an empty prefix is a no-op.
        auto fs = _data_dir->fs();
        Status status = fs != nullptr ? fs->delete_directory(_spill_dir)
                                      : Status::InternalError("spill store {} is not ready",
                                                              _data_dir->path());
        DBUG_EXECUTE_IF("fault_inject::spill_file::gc", {
            status = Status::Error<INTERNAL_ERROR>("fault_inject spill_file gc failed");
        });
        if (!status.ok()) {
            LOG_EVERY_T(WARNING, 1) << fmt::format("failed to delete spill data, dir {}, error: {}",
                                                   _spill_dir, status.to_string());
        }
        _dir_created = false;
    }
    // Decrease spill data usage even if per-file cleanup failed. QueryContext teardown deletes the
    // whole query spill directory and retains failures for later retries.
    _data_dir->release(_total_written_bytes);
    _total_written_bytes = 0;
}

Status SpillFile::create_writer(RuntimeState* state, RuntimeProfile* profile,
                                SpillFileWriterSPtr& writer) {
    writer = std::make_shared<SpillFileWriter>(shared_from_this(), state, profile, _data_dir,
                                               _spill_dir);
    // _active_writer is set in SpillFileWriter constructor via the shared_ptr
    return Status::OK();
}

SpillFileReaderSPtr SpillFile::create_reader(RuntimeState* state, RuntimeProfile* profile) const {
    // It's a programming error to create a reader while a writer is still active.
    DCHECK(_active_writer == nullptr) << "create_reader() called while writer still active";
    return std::make_shared<SpillFileReader>(state, profile, _data_dir, _spill_dir, _part_sizes);
}

void SpillFile::finish_writing() {
    _ready_for_reading = true;
    // writer finished; clear active writer pointer
    _active_writer = nullptr;
}

void SpillFile::update_written_bytes(int64_t delta_bytes) {
    _total_written_bytes += delta_bytes;
}

void SpillFile::add_part(int64_t part_bytes) {
    _part_sizes.push_back(part_bytes);
}

} // namespace doris
