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
#include <memory>
#include <string>

#include "io/fs/file_writer.h"
#include "runtime/workload_management/resource_context.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace vectorized {
class SpillDataDir;
class SpillWriter {
public:
    SpillWriter(std::shared_ptr<ResourceContext> resource_context, RuntimeProfile* profile,
                int64_t id, size_t batch_size, SpillDataDir* data_dir, const std::string& dir)
            : data_dir_(data_dir),
              stream_id_(id),
              batch_size_(batch_size),
              _resource_ctx(std::move(resource_context)) {
        // Directory path format specified in SpillStreamManager::register_spill_stream:
        // storage_root/spill/query_id/partitioned_hash_join-node_id-task_id-stream_id/0
        file_path_ = dir + "/0";
        RuntimeProfile* common_profile = profile->get_child("CommonCounters");
        DCHECK(common_profile != nullptr);
        _memory_used_counter = common_profile->get_counter("MemoryUsage");
    }

    Status open();

    Status close();

    Status write(RuntimeState* state, const Block& block, size_t& written_bytes);

    int64_t get_id() const { return stream_id_; }

    int64_t get_written_bytes() const { return total_written_bytes_; }

    const std::string& get_file_path() const { return file_path_; }

    void set_counters(RuntimeProfile* operator_profile) {
        RuntimeProfile* custom_profile = operator_profile->get_child("CustomCounters");
        _write_file_timer = custom_profile->get_counter("SpillWriteFileTime");
        _serialize_timer = custom_profile->get_counter("SpillWriteSerializeBlockTime");
        _write_block_counter = custom_profile->get_counter("SpillWriteBlockCount");
        _write_block_bytes_counter = custom_profile->get_counter("SpillWriteBlockBytes");
        _write_file_total_size = custom_profile->get_counter("SpillWriteFileBytes");
        _write_file_current_size = custom_profile->get_counter("SpillWriteFileCurrentBytes");
        _write_rows_counter = custom_profile->get_counter("SpillWriteRows");
    }

private:
    Status _write_internal(const Block& block, size_t& written_bytes);

    // not owned, point to the data dir of this rowset
    // for checking disk capacity when write data to disk.
    SpillDataDir* data_dir_ = nullptr;
    std::atomic_bool closed_ = false;
    int64_t stream_id_;
    size_t batch_size_;
    size_t max_sub_block_size_ = 0;
    std::string file_path_;
    std::unique_ptr<doris::io::FileWriter> file_writer_;

    size_t written_blocks_ = 0;
    int64_t total_written_bytes_ = 0;
    std::string meta_;

    RuntimeProfile::Counter* _write_file_timer = nullptr;
    RuntimeProfile::Counter* _serialize_timer = nullptr;
    RuntimeProfile::Counter* _write_block_counter = nullptr;
    RuntimeProfile::Counter* _write_block_bytes_counter = nullptr;
    RuntimeProfile::Counter* _write_file_total_size = nullptr;
    RuntimeProfile::Counter* _write_file_current_size = nullptr;
    RuntimeProfile::Counter* _write_rows_counter = nullptr;
    RuntimeProfile::Counter* _memory_used_counter = nullptr;

    std::shared_ptr<ResourceContext> _resource_ctx = nullptr;
};
using SpillWriterUPtr = std::unique_ptr<SpillWriter>;
} // namespace vectorized
} // namespace doris

#include "common/compile_check_end.h"
