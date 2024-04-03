
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
#include <mutex>
#include <unordered_map>
#include <vector>

#include "olap/options.h"
#include "util/threadpool.h"
#include "vec/spill/spill_stream.h"
namespace doris {
class RuntimeProfile;

namespace vectorized {

class SpillDataDir {
public:
    SpillDataDir(const std::string& path, int64_t capacity_bytes = -1,
                 TStorageMedium::type storage_medium = TStorageMedium::HDD);

    Status init();

    const std::string& path() const { return _path; }

    bool is_ssd_disk() const { return _storage_medium == TStorageMedium::SSD; }

    TStorageMedium::type storage_medium() const { return _storage_medium; }

    // check if the capacity reach the limit after adding the incoming data
    // return true if limit reached, otherwise, return false.
    // TODO(cmy): for now we can not precisely calculate the capacity Doris used,
    // so in order to avoid running out of disk capacity, we currently use the actual
    // disk available capacity and total capacity to do the calculation.
    // So that the capacity Doris actually used may exceeds the user specified capacity.
    bool reach_capacity_limit(int64_t incoming_data_size);

    Status update_capacity();

    double get_usage(int64_t incoming_data_size) const {
        return _disk_capacity_bytes == 0
                       ? 0
                       : (_disk_capacity_bytes - _available_bytes + incoming_data_size) /
                                 (double)_disk_capacity_bytes;
    }

private:
    std::string _path;

    // the actual available capacity of the disk of this data dir
    size_t _available_bytes;
    // the actual capacity of the disk of this data dir
    size_t _disk_capacity_bytes;
    TStorageMedium::type _storage_medium;
};
class SpillStreamManager {
public:
    SpillStreamManager(const std::vector<StorePath>& paths);

    Status init();

    void stop() {
        _stop_background_threads_latch.count_down();
        if (_spill_gc_thread) {
            _spill_gc_thread->join();
        }
    }

    // 创建SpillStream并登记
    Status register_spill_stream(RuntimeState* state, SpillStreamSPtr& spill_stream,
                                 std::string query_id, std::string operator_name, int32_t node_id,
                                 int32_t batch_rows, size_t batch_bytes, RuntimeProfile* profile);

    // 标记SpillStream需要被删除，在GC线程中异步删除落盘文件
    void delete_spill_stream(SpillStreamSPtr spill_stream);

    void gc(int64_t max_file_count);

    void update_usage(const std::string& path, int64_t incoming_data_size) {
        path_to_spill_data_size_[path] += incoming_data_size;
    }

    static bool reach_capacity_limit(size_t size, size_t incoming_data_size) {
        return size + incoming_data_size > config::spill_storage_limit;
    }

    int64_t spilled_data_size(const std::string& path) { return path_to_spill_data_size_[path]; }

    ThreadPool* get_spill_io_thread_pool(const std::string& path) const {
        const auto it = path_to_io_thread_pool_.find(path);
        DCHECK(it != path_to_io_thread_pool_.end());
        return it->second.get();
    }
    ThreadPool* get_async_task_thread_pool() const { return async_task_thread_pool_.get(); }

private:
    Status _init_spill_store_map();
    void _spill_gc_thread_callback();
    std::vector<SpillDataDir*> _get_stores_for_spill(TStorageMedium::type storage_medium);

    std::vector<StorePath> _spill_store_paths;
    std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> _spill_store_map;

    CountDownLatch _stop_background_threads_latch;
    std::unique_ptr<ThreadPool> async_task_thread_pool_;
    std::unordered_map<std::string, std::unique_ptr<ThreadPool>> path_to_io_thread_pool_;
    std::unordered_map<std::string, std::atomic_int64_t> path_to_spill_data_size_;
    scoped_refptr<Thread> _spill_gc_thread;

    std::atomic_uint64_t id_ = 0;
};
} // namespace vectorized
} // namespace doris