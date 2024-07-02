
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
#include "util/metrics.h"
#include "util/threadpool.h"
#include "vec/spill/spill_stream.h"
namespace doris {
class RuntimeProfile;

namespace vectorized {

class SpillStreamManager;
class SpillDataDir {
public:
    SpillDataDir(std::string path, int64_t capacity_bytes,
                 TStorageMedium::type storage_medium = TStorageMedium::HDD);

    Status init();

    const std::string& path() const { return _path; }

    std::string get_spill_data_path(const std::string& query_id = "") const;

    std::string get_spill_data_gc_path(const std::string& sub_dir_name = "") const;

    TStorageMedium::type storage_medium() const { return _storage_medium; }

    // check if the capacity reach the limit after adding the incoming data
    // return true if limit reached, otherwise, return false.
    bool reach_capacity_limit(int64_t incoming_data_size);

    Status update_capacity();

    void update_spill_data_usage(int64_t incoming_data_size) {
        std::lock_guard<std::mutex> l(_mutex);
        _spill_data_bytes += incoming_data_size;
        spill_disk_data_size->set_value(_spill_data_bytes);
    }

    int64_t get_spill_data_bytes() {
        std::lock_guard<std::mutex> l(_mutex);
        return _spill_data_bytes;
    }

    int64_t get_spill_data_limit() {
        std::lock_guard<std::mutex> l(_mutex);
        return _spill_data_limit_bytes;
    }

    std::string debug_string();

private:
    bool _reach_disk_capacity_limit(int64_t incoming_data_size);
    double _get_disk_usage(int64_t incoming_data_size) const {
        return _disk_capacity_bytes == 0
                       ? 0
                       : (_disk_capacity_bytes - _available_bytes + incoming_data_size) /
                                 (double)_disk_capacity_bytes;
    }

    friend class SpillStreamManager;
    std::string _path;

    // protect _disk_capacity_bytes, _available_bytes, _spill_data_limit_bytes, _spill_data_bytes
    std::mutex _mutex;
    // the actual capacity of the disk of this data dir
    size_t _disk_capacity_bytes;
    int64_t _spill_data_limit_bytes = 0;
    // the actual available capacity of the disk of this data dir
    size_t _available_bytes = 0;
    int64_t _spill_data_bytes = 0;
    TStorageMedium::type _storage_medium;

    std::shared_ptr<MetricEntity> spill_data_dir_metric_entity;
    IntGauge* spill_disk_capacity = nullptr;
    IntGauge* spill_disk_limit = nullptr;
    IntGauge* spill_disk_avail_capacity = nullptr;
    IntGauge* spill_disk_data_size = nullptr;
    // for test
    IntGauge* spill_disk_has_spill_data = nullptr;
    IntGauge* spill_disk_has_spill_gc_data = nullptr;
};
class SpillStreamManager {
public:
    SpillStreamManager(std::unordered_map<std::string, std::unique_ptr<vectorized::SpillDataDir>>&&
                               spill_store_map);

    Status init();

    void stop() {
        _stop_background_threads_latch.count_down();
        if (_spill_gc_thread) {
            _spill_gc_thread->join();
        }
    }

    // 创建SpillStream并登记
    Status register_spill_stream(RuntimeState* state, SpillStreamSPtr& spill_stream,
                                 const std::string& query_id, const std::string& operator_name,
                                 int32_t node_id, int32_t batch_rows, size_t batch_bytes,
                                 RuntimeProfile* profile);

    // 标记SpillStream需要被删除，在GC线程中异步删除落盘文件
    void delete_spill_stream(SpillStreamSPtr spill_stream);

    void async_cleanup_query(TUniqueId query_id);

    void gc(int32_t max_work_time_ms);

    ThreadPool* get_spill_io_thread_pool() const { return _spill_io_thread_pool.get(); }

private:
    Status _init_spill_store_map();
    void _spill_gc_thread_callback();
    std::vector<SpillDataDir*> _get_stores_for_spill(TStorageMedium::type storage_medium);

    std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> _spill_store_map;

    CountDownLatch _stop_background_threads_latch;
    std::unique_ptr<ThreadPool> _spill_io_thread_pool;
    scoped_refptr<Thread> _spill_gc_thread;

    std::atomic_uint64_t id_ = 0;
};
} // namespace vectorized
} // namespace doris