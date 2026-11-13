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

#include <fmt/format.h>
#include <glog/logging.h>

#include <filesystem>
#include <memory>
#include <utility>

#include "exec/spill/spill_file_manager.h"
#include "exec/spill/spill_file_reader.h"
#include "exec/spill/spill_file_writer.h"
#include "exec/spill/spill_s3_file_reader.h"
#include "exec/spill/spill_s3_file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "io/fs/s3_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/debug_points.h"

namespace doris {
SpillFile::SpillFile(SpillDataDir* data_dir, io::FileSystemSPtr fs, std::string relative_path)
        : _data_dir(data_dir),
          _fs(std::move(fs)),
          _query_id(relative_path.substr(0, relative_path.find('/'))),
          _spill_dir(data_dir->get_spill_data_path() + "/" + std::move(relative_path)),
          _log_path(_spill_dir) {
    if (_data_dir->is_remote()) {
        auto* s3_fs = dynamic_cast<io::S3FileSystem*>(_fs.get());
        DORIS_CHECK(s3_fs != nullptr);
        _log_path = fmt::format("s3://{}/{}", s3_fs->bucket(),
                                (io::Path(s3_fs->prefix()) / _spill_dir).native());
    }
    _data_dir->record_spill_file_created();
    LOG(INFO) << "[spill][create] " << _log_context() << ", status=OK";
}

SpillFile::SpillFile(SpillDataDir* data_dir, std::string relative_path)
        : SpillFile(data_dir, data_dir->fs(), std::move(relative_path)) {
    DORIS_CHECK(!data_dir->is_remote());
}

SpillFile::~SpillFile() {
    gc();
}

bool SpillFile::is_remote() const {
    return _data_dir->is_remote();
}

void SpillFile::gc() {
    if (_gc_started) {
        return;
    }
    _gc_started = true;
    // Preserve synchronous writer teardown: uploads must finish before queuing deletion.
    // Only physical cleanup and deletion retries are moved to the GC thread.
    if (_active_writer != nullptr) {
        _active_writer->_discard(this);
    }
    const std::string file_context = _log_context();
    const int64_t written_bytes = std::exchange(_total_written_bytes, 0);
    if (!_dir_created) {
        _data_dir->release(written_bytes);
        _data_dir->record_spill_file_deleted();
        LOG(INFO) << "[spill][delete] " << file_context << ", cleanup=unwritten, status=OK";
        return;
    }
    _dir_created = false;
    auto* manager = ExecEnv::GetInstance()->spill_file_mgr();
    DORIS_CHECK(manager != nullptr);
    manager->queue_spill_directory_deletion(_data_dir, _fs, _spill_dir, written_bytes,
                                            std::exchange(_persisted_bytes, 0), _query_id,
                                            {file_context});
}

std::string SpillFile::_log_context() const {
    return fmt::format("query_id={}, storage={}, fs_id={}, path={}, bytes={}, parts={}", _query_id,
                       _data_dir->is_remote() ? "remote" : "local", _fs->id(), _log_path,
                       _total_written_bytes, _part_sizes.size());
}

Status SpillFile::create_writer(RuntimeState* state, RuntimeProfile* profile,
                                SpillFileWriterSPtr& writer) {
    if (_data_dir->is_remote()) {
        writer = std::make_shared<SpillS3FileWriter>(shared_from_this(), state, profile, _data_dir,
                                                     _fs, _spill_dir);
    } else {
        writer = std::make_shared<SpillFileWriter>(shared_from_this(), state, profile, _data_dir,
                                                   _fs, _spill_dir);
    }
    // _active_writer is set in SpillFileWriter constructor via the shared_ptr
    return Status::OK();
}

SpillFileReaderSPtr SpillFile::create_reader(RuntimeState* state, RuntimeProfile* profile) const {
    // It's a programming error to create a reader while a writer is still active.
    DCHECK(_active_writer == nullptr) << "create_reader() called while writer still active";
    if (_data_dir->is_remote()) {
        return std::make_shared<SpillS3FileReader>(state, profile, _data_dir, _fs, _spill_dir,
                                                   _part_sizes);
    }
    return std::make_shared<SpillFileReader>(state, profile, _data_dir, _fs, _spill_dir,
                                             _part_sizes);
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
    if (_data_dir->is_remote()) {
        _data_dir->record_persisted_bytes(part_bytes);
        _persisted_bytes += part_bytes;
    }
}

void SpillFile::account_potential_part(int64_t part_bytes) {
    DORIS_CHECK(_data_dir->is_remote());
    _data_dir->record_persisted_bytes(part_bytes);
    _persisted_bytes += part_bytes;
    auto status = reconcile_remote_spill_bytes(_data_dir, _fs, _spill_dir, &_persisted_bytes);
    if (!status.ok()) {
        LOG_EVERY_T(WARNING, 60) << "failed to reconcile spill bytes after uncertain upload: "
                                 << status;
    }
}

} // namespace doris
