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

#include "exec/spill/spill_s3_file_writer.h"

#include <chrono>
#include <thread>
#include <utility>

#include "common/config.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_file_manager.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile_counter_names.h"
#include "runtime/workload_management/io_context.h"
#include "util/defer_op.h"
#include "util/stopwatch.hpp"
#include "util/threadpool.h"

namespace doris {

SpillS3FileWriter::SpillS3FileWriter(const std::shared_ptr<SpillFile>& spill_file,
                                     RuntimeState* state, RuntimeProfile* profile,
                                     SpillDataDir* data_dir, io::FileSystemSPtr fs,
                                     std::string spill_dir)
        : SpillFileWriter(spill_file, state, profile, data_dir, std::move(fs),
                          std::move(spill_dir)) {
    RuntimeProfile* custom_profile = profile->get_child("CustomCounters");
    _remote_upload_bytes = custom_profile->get_counter(profile::SPILL_REMOTE_UPLOAD_BYTES);
    _remote_upload_timer = custom_profile->get_counter(profile::SPILL_REMOTE_UPLOAD_TIME);
    _remote_upload_wait_timer = custom_profile->get_counter(profile::SPILL_REMOTE_UPLOAD_WAIT_TIME);
}

SpillS3FileWriter::~SpillS3FileWriter() {
    // Base destruction cannot dispatch virtual hooks to this class. Close while the S3
    // counters and queue admission hook are still alive, then let the base clear registration.
    Status st = close();
    if (!st.ok()) {
        LOG(WARNING) << "SpillS3FileWriter::~SpillS3FileWriter() failed: " << st.to_string()
                     << ", spill_dir=" << _spill_directory();
    }
}

Status SpillS3FileWriter::_wait_for_upload_queue() {
    auto is_cancelled = [this]() {
        return _resource_ctx != nullptr && _resource_ctx->task_controller()->is_cancelled();
    };
    // A cancelled query must not start another (billed) upload.
    if (is_cancelled()) {
        return Status::Cancelled("query cancelled before uploading spill data");
    }
    auto* upload_pool = ExecEnv::GetInstance()->s3_file_upload_thread_pool();
    auto queue_full = [upload_pool]() {
        return static_cast<int64_t>(upload_pool->get_queue_size()) * config::s3_write_buffer_size >=
               config::spill_s3_max_inflight_upload_bytes;
    };
    if (!queue_full()) {
        return Status::OK();
    }
    MonotonicStopWatch wait_watch;
    wait_watch.start();
    Defer record_wait {
            [&]() { COUNTER_UPDATE(_remote_upload_wait_timer, wait_watch.elapsed_time()); }};
    // Queued uploads always finish, so the wait ends without help from other writers.
    while (queue_full()) {
        if (is_cancelled()) {
            return Status::Cancelled("query cancelled while waiting for the spill upload queue");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return Status::OK();
}

Status SpillS3FileWriter::_append_to_part(const Slice& data) {
    RETURN_IF_ERROR(_part_append_status);
    // S3FileWriter splits `data` into upload buffers and queues every full one without waiting,
    // so the queue is checked once per append: `data` is already in memory, and the wait keeps
    // the next append from producing faster than object storage takes the data.
    _part_append_status = _wait_for_upload_queue();
    RETURN_IF_ERROR(_part_append_status);
    MonotonicStopWatch append_watch;
    append_watch.start();
    _part_append_status = _file_writer->append(data);
    _part_upload_ns += append_watch.elapsed_time();
    return _part_append_status;
}

void SpillS3FileWriter::_record_spill_write_bytes(int64_t bytes) {
    if (_resource_ctx) {
        _resource_ctx->io_context()->update_spill_write_bytes_to_remote_storage(bytes);
    }
}

void SpillS3FileWriter::_on_part_closed(const std::shared_ptr<SpillFile>& spill_file, bool closed,
                                        int64_t close_ns) {
    _part_upload_ns += close_ns;
    // A successful close publishes the whole part, including its footer. Failed uploads
    // can still be in flight, so their bytes are not counted as successfully uploaded.
    const int64_t uploaded_bytes = closed ? _part_written_bytes : 0;
    if (_remote_upload_bytes != nullptr) {
        COUNTER_UPDATE(_remote_upload_bytes, uploaded_bytes);
        COUNTER_UPDATE(_remote_upload_timer, _part_upload_ns);
    }
    ExecEnv::GetInstance()->spill_file_mgr()->update_spill_remote_write(uploaded_bytes);
    // A failed close may have published an object despite returning an error. The caller
    // has already drained its writer, so reconcile the prefix before charging the file.
    if (!closed && spill_file) {
        spill_file->account_potential_part(_part_written_bytes);
    }
    _part_upload_ns = 0;
}

} // namespace doris
