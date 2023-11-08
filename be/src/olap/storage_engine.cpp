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

#include "olap/storage_engine.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/container/detail/std_fwd.hpp>
#include <chrono>
#include <filesystem>
#include <iterator>
#include <list>
#include <new>
#include <ostream>
#include <random>
#include <set>
#include <thread>
#include <unordered_set>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "io/fs/local_file_system.h"
#include "olap/base_compaction.h"
#include "olap/binlog.h"
#include "olap/cumulative_compaction.h"
#include "olap/data_dir.h"
#include "olap/full_compaction.h"
#include "olap/memtable_flush_executor.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/unique_rowset_id_generator.h"
#include "olap/schema_cache.h"
#include "olap/segment_loader.h"
#include "olap/single_replica_compaction.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/task/engine_task.h"
#include "olap/txn_manager.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/stream_load/stream_load_recorder.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/spinlock.h"
#include "util/stopwatch.hpp"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/trace.h"
#include "util/uid_util.h"
#include "util/work_thread_pool.hpp"

using std::filesystem::directory_iterator;
using std::filesystem::path;
using std::map;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {
using namespace ErrorCode;

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(unused_rowsets_count, MetricUnit::ROWSETS);

static Status _validate_options(const EngineOptions& options) {
    if (options.store_paths.empty()) {
        return Status::InternalError("store paths is empty");
    }
    return Status::OK();
}

Status StorageEngine::open() {
    RETURN_IF_ERROR(_validate_options(_options));
    LOG(INFO) << "starting backend using uid:" << _options.backend_uid.to_string();
    RETURN_NOT_OK_STATUS_WITH_WARN(_open(), "open engine failed");
    LOG(INFO) << "success to init storage engine.";
    return Status::OK();
}

StorageEngine::StorageEngine(const EngineOptions& options)
        : _options(options),
          _available_storage_medium_type_count(0),
          _effective_cluster_id(-1),
          _is_all_cluster_id_exist(true),
          _stopped(false),
          _segcompaction_mem_tracker(std::make_shared<MemTracker>("SegCompaction")),
          _segment_meta_mem_tracker(std::make_shared<MemTracker>("SegmentMeta")),
          _stop_background_threads_latch(1),
          _tablet_manager(new TabletManager(config::tablet_map_shard_size)),
          _txn_manager(new TxnManager(config::txn_map_shard_size, config::txn_shard_size)),
          _rowset_id_generator(new UniqueRowsetIdGenerator(options.backend_uid)),
          _memtable_flush_executor(nullptr),
          _calc_delete_bitmap_executor(nullptr),
          _default_rowset_type(BETA_ROWSET),
          _heartbeat_flags(nullptr),
          _stream_load_recorder(nullptr) {
    REGISTER_HOOK_METRIC(unused_rowsets_count, [this]() {
        // std::lock_guard<std::mutex> lock(_gc_mutex);
        return _unused_rowsets.size();
    });

    _broken_paths = options.broken_paths;
}

StorageEngine::~StorageEngine() {
    stop();
    _clear();
}

Status StorageEngine::load_data_dirs(const std::vector<DataDir*>& data_dirs) {
    std::vector<std::thread> threads;
    std::vector<Status> results(data_dirs.size());
    for (size_t i = 0; i < data_dirs.size(); ++i) {
        threads.emplace_back(
                [&results, data_dir = data_dirs[i]](size_t index) {
                    results[index] = data_dir->load();
                    if (!results[index].ok()) {
                        LOG(WARNING) << "io error when init load tables. res=" << results[index]
                                     << ", data dir=" << data_dir->path();
                    }
                },
                i);
    }
    for (auto& thread : threads) {
        thread.join();
    }
    for (const auto& result : results) {
        if (!result.ok()) {
            return result;
        }
    }
    return Status::OK();
}

Status StorageEngine::_open() {
    // init store_map
    RETURN_NOT_OK_STATUS_WITH_WARN(_init_store_map(), "_init_store_map failed");

    _effective_cluster_id = config::cluster_id;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_all_root_path_cluster_id(), "fail to check cluster id");

    _update_storage_medium_type_count();

    RETURN_NOT_OK_STATUS_WITH_WARN(_check_file_descriptor_number(), "check fd number failed");

    auto dirs = get_stores<false>();
    RETURN_IF_ERROR(load_data_dirs(dirs));

    _memtable_flush_executor.reset(new MemTableFlushExecutor());
    _memtable_flush_executor->init(dirs);

    _calc_delete_bitmap_executor.reset(new CalcDeleteBitmapExecutor());
    _calc_delete_bitmap_executor->init();

    _parse_default_rowset_type();

    return Status::OK();
}

Status StorageEngine::_init_store_map() {
    std::vector<DataDir*> tmp_stores;
    std::vector<std::thread> threads;
    SpinLock error_msg_lock;
    std::string error_msg;
    for (auto& path : _options.store_paths) {
        DataDir* store = new DataDir(path.path, path.capacity_bytes, path.storage_medium,
                                     _tablet_manager.get(), _txn_manager.get());
        tmp_stores.emplace_back(store);
        threads.emplace_back([store, &error_msg_lock, &error_msg]() {
            auto st = store->init();
            if (!st.ok()) {
                {
                    std::lock_guard<SpinLock> l(error_msg_lock);
                    error_msg.append(st.to_string() + ";");
                }
                LOG(WARNING) << "Store load failed, status=" << st.to_string()
                             << ", path=" << store->path();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    if (!error_msg.empty()) {
        for (auto store : tmp_stores) {
            delete store;
        }
        return Status::InternalError("init path failed, error={}", error_msg);
    }

    for (auto store : tmp_stores) {
        _store_map.emplace(store->path(), store);
    }

    std::string stream_load_record_path;
    if (!tmp_stores.empty()) {
        stream_load_record_path = tmp_stores[0]->path();
    }

    RETURN_NOT_OK_STATUS_WITH_WARN(_init_stream_load_recorder(stream_load_record_path),
                                   "init StreamLoadRecorder failed");

    return Status::OK();
}

Status StorageEngine::_init_stream_load_recorder(const std::string& stream_load_record_path) {
    LOG(INFO) << "stream load record path: " << stream_load_record_path;
    // init stream load record rocksdb
    _stream_load_recorder = StreamLoadRecorder::create_unique(stream_load_record_path);
    if (_stream_load_recorder == nullptr) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::MemoryAllocFailed("allocate memory for StreamLoadRecorder failed"),
                "new StreamLoadRecorder failed");
    }
    auto st = _stream_load_recorder->init();
    if (!st.ok()) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError("open StreamLoadRecorder rocksdb failed, path={}",
                                stream_load_record_path),
                "init StreamLoadRecorder failed");
    }
    return Status::OK();
}

void StorageEngine::_update_storage_medium_type_count() {
    set<TStorageMedium::type> available_storage_medium_types;

    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        if (it.second->is_used()) {
            available_storage_medium_types.insert(it.second->storage_medium());
        }
    }

    _available_storage_medium_type_count = available_storage_medium_types.size();
}

Status StorageEngine::_judge_and_update_effective_cluster_id(int32_t cluster_id) {
    if (cluster_id == -1 && _effective_cluster_id == -1) {
        // maybe this is a new cluster, cluster id will get from heartbeat message
        return Status::OK();
    } else if (cluster_id != -1 && _effective_cluster_id == -1) {
        _effective_cluster_id = cluster_id;
        return Status::OK();
    } else if (cluster_id == -1 && _effective_cluster_id != -1) {
        // _effective_cluster_id is the right effective cluster id
        return Status::OK();
    } else {
        if (cluster_id != _effective_cluster_id) {
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    Status::Corruption("multiple cluster ids is not equal. one={}, other={}",
                                       _effective_cluster_id, cluster_id),
                    "cluster id not equal");
        }
    }

    return Status::OK();
}

template <bool include_unused>
std::vector<DataDir*> StorageEngine::get_stores() {
    std::vector<DataDir*> stores;
    stores.reserve(_store_map.size());

    std::lock_guard<std::mutex> l(_store_lock);
    if constexpr (include_unused) {
        for (auto& it : _store_map) {
            stores.push_back(it.second);
        }
    } else {
        for (auto& it : _store_map) {
            if (it.second->is_used()) {
                stores.push_back(it.second);
            }
        }
    }
    return stores;
}

template std::vector<DataDir*> StorageEngine::get_stores<false>();
template std::vector<DataDir*> StorageEngine::get_stores<true>();

Status StorageEngine::get_all_data_dir_info(std::vector<DataDirInfo>* data_dir_infos,
                                            bool need_update) {
    Status res = Status::OK();
    data_dir_infos->clear();

    MonotonicStopWatch timer;
    timer.start();

    // 1. update available capacity of each data dir
    // get all root path info and construct a path map.
    // path -> DataDirInfo
    std::map<std::string, DataDirInfo> path_map;
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& it : _store_map) {
            if (need_update) {
                static_cast<void>(it.second->update_capacity());
            }
            path_map.emplace(it.first, it.second->get_dir_info());
        }
    }

    // 2. get total tablets' size of each data dir
    size_t tablet_count = 0;
    _tablet_manager->update_root_path_info(&path_map, &tablet_count);

    // 3. update metrics in DataDir
    for (auto& path : path_map) {
        std::lock_guard<std::mutex> l(_store_lock);
        auto data_dir = _store_map.find(path.first);
        DCHECK(data_dir != _store_map.end());
        data_dir->second->update_local_data_size(path.second.local_used_capacity);
        data_dir->second->update_remote_data_size(path.second.remote_used_capacity);
    }

    // add path info to data_dir_infos
    for (auto& entry : path_map) {
        data_dir_infos->emplace_back(entry.second);
    }

    timer.stop();
    LOG(INFO) << "get root path info cost: " << timer.elapsed_time() / 1000000
              << " ms. tablet counter: " << tablet_count;

    return res;
}

int64_t StorageEngine::get_file_or_directory_size(const std::string& file_path) {
    if (!std::filesystem::exists(file_path)) {
        return 0;
    }
    if (!std::filesystem::is_directory(file_path)) {
        return std::filesystem::file_size(file_path);
    }
    int64_t sum_size = 0;
    for (const auto& it : std::filesystem::directory_iterator(file_path)) {
        sum_size += get_file_or_directory_size(it.path());
    }
    return sum_size;
}

void StorageEngine::_start_disk_stat_monitor() {
    for (auto& it : _store_map) {
        it.second->health_check();
    }

    _update_storage_medium_type_count();

    _exit_if_too_many_disks_are_failed();
}

// TODO(lingbin): Should be in EnvPosix?
Status StorageEngine::_check_file_descriptor_number() {
    struct rlimit l;
    int ret = getrlimit(RLIMIT_NOFILE, &l);
    if (ret != 0) {
        LOG(WARNING) << "call getrlimit() failed. errno=" << strerror(errno)
                     << ", use default configuration instead.";
        return Status::OK();
    }
    if (l.rlim_cur < config::min_file_descriptor_number) {
        LOG(ERROR) << "File descriptor number is less than " << config::min_file_descriptor_number
                   << ". Please use (ulimit -n) to set a value equal or greater than "
                   << config::min_file_descriptor_number;
        return Status::InternalError("file descriptors limit is too small");
    }
    return Status::OK();
}

Status StorageEngine::_check_all_root_path_cluster_id() {
    int32_t cluster_id = -1;
    for (auto& it : _store_map) {
        int32_t tmp_cluster_id = it.second->cluster_id();
        if (it.second->cluster_id_incomplete()) {
            _is_all_cluster_id_exist = false;
        } else if (tmp_cluster_id == cluster_id) {
            // both have right cluster id, do nothing
        } else if (cluster_id == -1) {
            cluster_id = tmp_cluster_id;
        } else {
            RETURN_NOT_OK_STATUS_WITH_WARN(
                    Status::Corruption("multiple cluster ids is not equal. one={}, other={}",
                                       cluster_id, tmp_cluster_id),
                    "cluster id not equal");
        }
    }

    // judge and get effective cluster id
    RETURN_IF_ERROR(_judge_and_update_effective_cluster_id(cluster_id));

    // write cluster id into cluster_id_path if get effective cluster id success
    if (_effective_cluster_id != -1 && !_is_all_cluster_id_exist) {
        static_cast<void>(set_cluster_id(_effective_cluster_id));
    }

    return Status::OK();
}

Status StorageEngine::set_cluster_id(int32_t cluster_id) {
    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        RETURN_IF_ERROR(it.second->set_cluster_id(cluster_id));
    }
    _effective_cluster_id = cluster_id;
    _is_all_cluster_id_exist = true;
    return Status::OK();
}

std::vector<DataDir*> StorageEngine::get_stores_for_create_tablet(
        TStorageMedium::type storage_medium) {
    struct DirInfo {
        DataDir* data_dir;

        size_t disk_available;
        //if disk_available is high, then available_level is small
        int available_level;

        int tablet_num;

        bool operator<(const DirInfo& other) const {
            if (available_level != other.available_level) {
                return available_level < other.available_level;
            }
            if (tablet_num != other.tablet_num) {
                return tablet_num < other.tablet_num;
            }
            return data_dir->path_hash() < other.data_dir->path_hash();
        }
    };
    std::map<size_t, int> available_levels;
    std::vector<DirInfo> dir_infos;
    int next_index = 0;
    size_t max_disk_capacity = 0;
    {
        std::lock_guard<std::mutex> l(_store_lock);
        next_index = _store_next_index[storage_medium]++;
        if (next_index < 0) {
            next_index = 0;
            _store_next_index[storage_medium] = next_index + 1;
        }
        for (auto& it : _store_map) {
            DataDir* data_dir = it.second;
            if (data_dir->is_used()) {
                if (_available_storage_medium_type_count == 1 ||
                    data_dir->storage_medium() == storage_medium) {
                    size_t disk_available = data_dir->disk_available();
                    DirInfo dir_info;
                    dir_info.data_dir = data_dir;
                    dir_info.available_level = disk_available;
                    dir_infos.push_back(dir_info);
                    available_levels[disk_available] = 0;
                    size_t disk_capacity = data_dir->disk_capacity();
                    if (max_disk_capacity < disk_capacity) {
                        max_disk_capacity = disk_capacity;
                    }
                }
            }
        }
    }

    std::vector<DataDir*> stores;
    if (dir_infos.empty()) {
        return stores;
    }

    // if two disk available diff not exceeds 20% capacity, then they are the same available level.
    size_t same_level_available_diff = std::max<size_t>(max_disk_capacity / 5, 1);
    int level = 0;
    size_t level_start_available = available_levels.rbegin()->first;
    for (auto rit = available_levels.rbegin(); rit != available_levels.rend(); rit++) {
        if (level_start_available - rit->first >= same_level_available_diff) {
            level_start_available = rit->first;
            level++;
        }
        rit->second = level;
    }

    for (auto& dir_info : dir_infos) {
        dir_info.tablet_num = dir_info.data_dir->tablet_num();
        dir_info.available_level = available_levels[dir_info.disk_available];
    }

    std::sort(dir_infos.begin(), dir_infos.end());

    // Suppose there are five data dirs (D1, D2, D3, D4, D5).
    // D1/D2/D3 contain 1 tablet,  D4/D5 contain 2 tablets.
    // If three creating tablets threads simultaneously invoke this function to get stores,
    // then the return stores will be as below:
    // thread 1: (D1, D2, D3, D4, D5)
    // thread 2: (D2, D3, D1, D5, D4)
    // thread 3: (D3, D1, D2, D4, D5)
    stores.reserve(dir_infos.size());
    for (size_t i = 0; i < dir_infos.size();) {
        size_t end = i + 1;
        while (end < dir_infos.size() && dir_infos[i].tablet_num == dir_infos[end].tablet_num &&
               dir_infos[i].available_level == dir_infos[end].available_level) {
            end++;
        }
        // data dirs [i, end) have the same tablet size, round robin range [i, end)
        size_t count = end - i;
        for (size_t k = 0; k < count; k++) {
            size_t index = i + (k + next_index) % count;
            stores.push_back(dir_infos[index].data_dir);
        }
        i = end;
    }

    return stores;
}

DataDir* StorageEngine::get_store(const std::string& path) {
    // _store_map is unchanged, no need to lock
    auto it = _store_map.find(path);
    if (it == _store_map.end()) {
        return nullptr;
    }
    return it->second;
}

static bool too_many_disks_are_failed(uint32_t unused_num, uint32_t total_num) {
    return ((total_num == 0) ||
            (unused_num * 100 / total_num > config::max_percentage_of_error_disk));
}

void StorageEngine::_exit_if_too_many_disks_are_failed() {
    uint32_t unused_root_path_num = 0;
    uint32_t total_root_path_num = 0;

    {
        // TODO(yingchun): _store_map is only updated in main and ~StorageEngine, maybe we can remove it?
        std::lock_guard<std::mutex> l(_store_lock);
        if (_store_map.empty()) {
            return;
        }

        for (auto& it : _store_map) {
            ++total_root_path_num;
            if (it.second->is_used()) {
                continue;
            }
            ++unused_root_path_num;
        }
    }

    if (too_many_disks_are_failed(unused_root_path_num, total_root_path_num)) {
        LOG(FATAL) << "meet too many error disks, process exit. "
                   << "max_ratio_allowed=" << config::max_percentage_of_error_disk << "%"
                   << ", error_disk_count=" << unused_root_path_num
                   << ", total_disk_count=" << total_root_path_num;
        exit(0);
    }
}

void StorageEngine::stop() {
    if (_stopped) {
        LOG(WARNING) << "Storage engine is stopped twice.";
        return;
    }
    // trigger the waiting threads
    notify_listeners();

    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& store_pair : _store_map) {
            store_pair.second->stop_bg_worker();
        }
    }

    _stop_background_threads_latch.count_down();
#define THREAD_JOIN(thread) \
    if (thread) {           \
        thread->join();     \
    }

    THREAD_JOIN(_compaction_tasks_producer_thread);
    THREAD_JOIN(_update_replica_infos_thread);
    THREAD_JOIN(_unused_rowset_monitor_thread);
    THREAD_JOIN(_garbage_sweeper_thread);
    THREAD_JOIN(_disk_stat_monitor_thread);
    THREAD_JOIN(_cache_clean_thread);
    THREAD_JOIN(_tablet_checkpoint_tasks_producer_thread);
    THREAD_JOIN(_async_publish_thread);
    THREAD_JOIN(_cold_data_compaction_producer_thread);
    THREAD_JOIN(_cooldown_tasks_producer_thread);
#undef THREAD_JOIN

#define THREADS_JOIN(threads)            \
    for (const auto& thread : threads) { \
        if (thread) {                    \
            thread->join();              \
        }                                \
    }

    THREADS_JOIN(_path_gc_threads);
    THREADS_JOIN(_path_scan_threads);
#undef THREADS_JOIN

    if (_base_compaction_thread_pool) {
        _base_compaction_thread_pool->shutdown();
    }
    if (_cumu_compaction_thread_pool) {
        _cumu_compaction_thread_pool->shutdown();
    }
    if (_single_replica_compaction_thread_pool) {
        _single_replica_compaction_thread_pool->shutdown();
    }

    if (_seg_compaction_thread_pool) {
        _seg_compaction_thread_pool->shutdown();
    }
    if (_tablet_meta_checkpoint_thread_pool) {
        _tablet_meta_checkpoint_thread_pool->shutdown();
    }
    if (_cold_data_compaction_thread_pool) {
        _cold_data_compaction_thread_pool->shutdown();
    }

    _memtable_flush_executor.reset(nullptr);
    _calc_delete_bitmap_executor.reset(nullptr);

    _stopped = true;
    LOG(INFO) << "Storage engine is stopped.";
}

void StorageEngine::_clear() {
    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& store_pair : _store_map) {
        delete store_pair.second;
        store_pair.second = nullptr;
    }
    _store_map.clear();
}

void StorageEngine::clear_transaction_task(const TTransactionId transaction_id) {
    // clear transaction task may not contains partitions ids, we should get partition id from txn manager.
    std::vector<int64_t> partition_ids;
    StorageEngine::instance()->txn_manager()->get_partition_ids(transaction_id, &partition_ids);
    clear_transaction_task(transaction_id, partition_ids);
}

void StorageEngine::clear_transaction_task(const TTransactionId transaction_id,
                                           const std::vector<TPartitionId>& partition_ids) {
    LOG(INFO) << "begin to clear transaction task. transaction_id=" << transaction_id;

    for (const TPartitionId& partition_id : partition_ids) {
        std::map<TabletInfo, RowsetSharedPtr> tablet_infos;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
                transaction_id, partition_id, &tablet_infos);

        // each tablet
        for (auto& tablet_info : tablet_infos) {
            // should use tablet uid to ensure clean txn correctly
            TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_info.first.tablet_id,
                                                                 tablet_info.first.tablet_uid);
            // The tablet may be dropped or altered, leave a INFO log and go on process other tablet
            if (tablet == nullptr) {
                LOG(INFO) << "tablet is no longer exist. tablet_id=" << tablet_info.first.tablet_id
                          << ", tablet_uid=" << tablet_info.first.tablet_uid;
                continue;
            }
            static_cast<void>(StorageEngine::instance()->txn_manager()->delete_txn(
                    partition_id, tablet, transaction_id));
        }
    }
    LOG(INFO) << "finish to clear transaction task. transaction_id=" << transaction_id;
}

Status StorageEngine::start_trash_sweep(double* usage, bool ignore_guard) {
    Status res = Status::OK();

    std::unique_lock<std::mutex> l(_trash_sweep_lock, std::defer_lock);
    if (!l.try_lock()) {
        LOG(INFO) << "trash and snapshot sweep is running.";
        if (ignore_guard) {
            _need_clean_trash.store(true, std::memory_order_relaxed);
        }
        return res;
    }

    LOG(INFO) << "start trash and snapshot sweep. is_clean=" << ignore_guard;

    const int32_t snapshot_expire = config::snapshot_expire_time_sec;
    const int32_t trash_expire = config::trash_file_expire_time_sec;
    // the guard space should be lower than storage_flood_stage_usage_percent,
    // so here we multiply 0.9
    // if ignore_guard is true, set guard_space to 0.
    const double guard_space =
            ignore_guard ? 0 : config::storage_flood_stage_usage_percent / 100.0 * 0.9;
    std::vector<DataDirInfo> data_dir_infos;
    RETURN_NOT_OK_STATUS_WITH_WARN(get_all_data_dir_info(&data_dir_infos, false),
                                   "failed to get root path stat info when sweep trash.")
    std::sort(data_dir_infos.begin(), data_dir_infos.end(), DataDirInfoLessAvailability());

    time_t now = time(nullptr); //获取UTC时间
    tm local_tm_now;
    local_tm_now.tm_isdst = 0;
    if (localtime_r(&now, &local_tm_now) == nullptr) {
        return Status::Error<OS_ERROR>("fail to localtime_r time. time={}", now);
    }
    const time_t local_now = mktime(&local_tm_now); //得到当地日历时间

    double tmp_usage = 0.0;
    for (DataDirInfo& info : data_dir_infos) {
        LOG(INFO) << "Start to sweep path " << info.path;
        if (!info.is_used) {
            continue;
        }

        double curr_usage = (double)(info.disk_capacity - info.available) / info.disk_capacity;
        tmp_usage = std::max(tmp_usage, curr_usage);

        Status curr_res = Status::OK();
        auto snapshot_path = fmt::format("{}/{}", info.path, SNAPSHOT_PREFIX);
        curr_res = _do_sweep(snapshot_path, local_now, snapshot_expire);
        if (!curr_res.ok()) {
            LOG(WARNING) << "failed to sweep snapshot. path=" << snapshot_path
                         << ", err_code=" << curr_res;
            res = curr_res;
        }

        auto trash_path = fmt::format("{}/{}", info.path, TRASH_PREFIX);
        curr_res = _do_sweep(trash_path, local_now, curr_usage > guard_space ? 0 : trash_expire);
        if (!curr_res.ok()) {
            LOG(WARNING) << "failed to sweep trash. path=" << trash_path
                         << ", err_code=" << curr_res;
            res = curr_res;
        }
    }

    if (usage != nullptr) {
        *usage = tmp_usage; // update usage
    }

    // clear expire incremental rowset, move deleted tablet to trash
    static_cast<void>(_tablet_manager->start_trash_sweep());

    // clean rubbish transactions
    _clean_unused_txns();

    // clean unused rowset metas in OlapMeta
    _clean_unused_rowset_metas();

    // clean unused binlog metas in OlapMeta
    _clean_unused_binlog_metas();

    // cleand unused delete bitmap for deleted tablet
    _clean_unused_delete_bitmap();

    // cleand unused pending publish info for deleted tablet
    _clean_unused_pending_publish_info();

    // clean unused rowsets in remote storage backends
    for (auto data_dir : get_stores()) {
        data_dir->perform_remote_rowset_gc();
        data_dir->perform_remote_tablet_gc();
        data_dir->update_trash_capacity();
    }

    return res;
}

void StorageEngine::_clean_unused_rowset_metas() {
    std::vector<RowsetMetaSharedPtr> invalid_rowset_metas;
    auto clean_rowset_func = [this, &invalid_rowset_metas](TabletUid tablet_uid, RowsetId rowset_id,
                                                           const std::string& meta_str) -> bool {
        // return false will break meta iterator, return true to skip this error
        RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
        bool parsed = rowset_meta->init(meta_str);
        if (!parsed) {
            LOG(WARNING) << "parse rowset meta string failed for rowset_id:" << rowset_id;
            invalid_rowset_metas.push_back(rowset_meta);
            return true;
        }
        if (rowset_meta->tablet_uid() != tablet_uid) {
            LOG(WARNING) << "tablet uid is not equal, skip the rowset"
                         << ", rowset_id=" << rowset_meta->rowset_id()
                         << ", in_put_tablet_uid=" << tablet_uid
                         << ", tablet_uid in rowset meta=" << rowset_meta->tablet_uid();
            invalid_rowset_metas.push_back(rowset_meta);
            return true;
        }

        TabletSharedPtr tablet = _tablet_manager->get_tablet(rowset_meta->tablet_id());
        if (tablet == nullptr) {
            // tablet may be dropped
            // TODO(cmy): this is better to be a VLOG, because drop table is a very common case.
            // leave it as INFO log for observation. Maybe change it in future.
            LOG(INFO) << "failed to find tablet " << rowset_meta->tablet_id()
                      << " for rowset: " << rowset_meta->rowset_id() << ", tablet may be dropped";
            invalid_rowset_metas.push_back(rowset_meta);
            return true;
        }
        if (tablet->tablet_uid() != rowset_meta->tablet_uid()) {
            // In this case, we get the tablet using the tablet id recorded in the rowset meta.
            // but the uid in the tablet is different from the one recorded in the rowset meta.
            // How this happened:
            // Replica1 of Tablet A exists on BE1. Because of the clone task, a new replica2 is createed on BE2,
            // and then replica1 deleted from BE1. After some time, we created replica again on BE1,
            // which will creates a new tablet with the same id but a different uid.
            // And in the historical version, when we deleted the replica, we did not delete the corresponding rowset meta,
            // thus causing the original rowset meta to remain(with same tablet id but different uid).
            LOG(WARNING) << "rowset's tablet uid " << rowset_meta->tablet_uid()
                         << " does not equal to tablet uid: " << tablet->tablet_uid();
            invalid_rowset_metas.push_back(rowset_meta);
            return true;
        }
        if (rowset_meta->rowset_state() == RowsetStatePB::VISIBLE &&
            (!tablet->rowset_meta_is_useful(rowset_meta))) {
            LOG(INFO) << "rowset meta is not used any more, remove it. rowset_id="
                      << rowset_meta->rowset_id();
            invalid_rowset_metas.push_back(rowset_meta);
        }
        return true;
    };
    auto data_dirs = get_stores();
    for (auto data_dir : data_dirs) {
        static_cast<void>(
                RowsetMetaManager::traverse_rowset_metas(data_dir->get_meta(), clean_rowset_func));
        for (auto& rowset_meta : invalid_rowset_metas) {
            static_cast<void>(RowsetMetaManager::remove(
                    data_dir->get_meta(), rowset_meta->tablet_uid(), rowset_meta->rowset_id()));
        }
        LOG(INFO) << "remove " << invalid_rowset_metas.size()
                  << " invalid rowset meta from dir: " << data_dir->path();
        invalid_rowset_metas.clear();
    }
}

void StorageEngine::_clean_unused_binlog_metas() {
    std::vector<std::string> unused_binlog_key_suffixes;
    auto unused_binlog_collector = [this, &unused_binlog_key_suffixes](const std::string& key,
                                                                       const std::string& value,
                                                                       bool need_check) -> bool {
        if (need_check) {
            BinlogMetaEntryPB binlog_meta_pb;
            if (UNLIKELY(!binlog_meta_pb.ParseFromString(value))) {
                LOG(WARNING) << "parse rowset meta string failed for binlog meta key: " << key;
            } else if (_tablet_manager->get_tablet(binlog_meta_pb.tablet_id()) == nullptr) {
                LOG(INFO) << "failed to find tablet " << binlog_meta_pb.tablet_id()
                          << " for binlog rowset: " << binlog_meta_pb.rowset_id()
                          << ", tablet may be dropped";
            } else {
                return false;
            }
        }

        unused_binlog_key_suffixes.emplace_back(key.substr(kBinlogMetaPrefix.size()));
        return true;
    };
    auto data_dirs = get_stores();
    for (auto data_dir : data_dirs) {
        static_cast<void>(RowsetMetaManager::traverse_binlog_metas(data_dir->get_meta(),
                                                                   unused_binlog_collector));
        for (const auto& suffix : unused_binlog_key_suffixes) {
            static_cast<void>(RowsetMetaManager::remove_binlog(data_dir->get_meta(), suffix));
        }
        LOG(INFO) << "remove " << unused_binlog_key_suffixes.size()
                  << " invalid binlog meta from dir: " << data_dir->path();
        unused_binlog_key_suffixes.clear();
    }
}

void StorageEngine::_clean_unused_delete_bitmap() {
    std::unordered_set<int64_t> removed_tablets;
    auto clean_delete_bitmap_func = [this, &removed_tablets](int64_t tablet_id, int64_t version,
                                                             const std::string& val) -> bool {
        TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_id);
        if (tablet == nullptr) {
            if (removed_tablets.insert(tablet_id).second) {
                LOG(INFO) << "clean ununsed delete bitmap for deleted tablet, tablet_id: "
                          << tablet_id;
            }
        }
        return true;
    };
    auto data_dirs = get_stores();
    for (auto data_dir : data_dirs) {
        static_cast<void>(TabletMetaManager::traverse_delete_bitmap(data_dir->get_meta(),
                                                                    clean_delete_bitmap_func));
        for (auto id : removed_tablets) {
            static_cast<void>(
                    TabletMetaManager::remove_old_version_delete_bitmap(data_dir, id, INT64_MAX));
        }
        LOG(INFO) << "removed invalid delete bitmap from dir: " << data_dir->path()
                  << ", deleted tablets size: " << removed_tablets.size();
        removed_tablets.clear();
    }
}

void StorageEngine::_clean_unused_pending_publish_info() {
    std::vector<std::pair<int64_t, int64_t>> removed_infos;
    auto clean_pending_publish_info_func = [this, &removed_infos](int64_t tablet_id,
                                                                  int64_t publish_version,
                                                                  const string& info) -> bool {
        TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_id);
        if (tablet == nullptr) {
            removed_infos.emplace_back(tablet_id, publish_version);
        }
        return true;
    };
    auto data_dirs = get_stores();
    for (auto data_dir : data_dirs) {
        static_cast<void>(TabletMetaManager::traverse_pending_publish(
                data_dir->get_meta(), clean_pending_publish_info_func));
        for (auto& [tablet_id, publish_version] : removed_infos) {
            static_cast<void>(TabletMetaManager::remove_pending_publish_info(data_dir, tablet_id,
                                                                             publish_version));
        }
        LOG(INFO) << "removed invalid pending publish info from dir: " << data_dir->path()
                  << ", deleted pending publish info size: " << removed_infos.size();
        removed_infos.clear();
    }
}

void StorageEngine::gc_binlogs(const std::unordered_map<int64_t, int64_t>& gc_tablet_infos) {
    for (auto [tablet_id, version] : gc_tablet_infos) {
        LOG(INFO) << fmt::format("start to gc binlogs for tablet_id: {}, version: {}", tablet_id,
                                 version);

        TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_id);
        if (tablet == nullptr) {
            LOG(WARNING) << fmt::format("tablet_id: {} not found", tablet_id);
            continue;
        }
        tablet->gc_binlogs(version);
    }
}

void StorageEngine::_clean_unused_txns() {
    std::set<TabletInfo> tablet_infos;
    _txn_manager->get_all_related_tablets(&tablet_infos);
    for (auto& tablet_info : tablet_infos) {
        TabletSharedPtr tablet =
                _tablet_manager->get_tablet(tablet_info.tablet_id, tablet_info.tablet_uid, true);
        if (tablet == nullptr) {
            // TODO(ygl) :  should check if tablet still in meta, it's a improvement
            // case 1: tablet still in meta, just remove from memory
            // case 2: tablet not in meta store, remove rowset from meta
            // currently just remove them from memory
            // nullptr to indicate not remove them from meta store
            _txn_manager->force_rollback_tablet_related_txns(nullptr, tablet_info.tablet_id,
                                                             tablet_info.tablet_uid);
        }
    }
}

Status StorageEngine::_do_sweep(const std::string& scan_root, const time_t& local_now,
                                const int32_t expire) {
    Status res = Status::OK();
    bool exists = true;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(scan_root, &exists));
    if (!exists) {
        // dir not existed. no need to sweep trash.
        return res;
    }

    int curr_sweep_batch_size = 0;
    try {
        // Sort pathes by name, that is by delete time.
        std::vector<path> sorted_pathes;
        std::copy(directory_iterator(scan_root), directory_iterator(),
                  std::back_inserter(sorted_pathes));
        std::sort(sorted_pathes.begin(), sorted_pathes.end());
        for (const auto& sorted_path : sorted_pathes) {
            string dir_name = sorted_path.filename().string();
            string str_time = dir_name.substr(0, dir_name.find('.'));
            tm local_tm_create;
            local_tm_create.tm_isdst = 0;
            if (strptime(str_time.c_str(), "%Y%m%d%H%M%S", &local_tm_create) == nullptr) {
                res = Status::Error<OS_ERROR>("fail to strptime time. time={}", str_time);
                continue;
            }

            int32_t actual_expire = expire;
            // try get timeout in dir name, the old snapshot dir does not contain timeout
            // eg: 20190818221123.3.86400, the 86400 is timeout, in second
            size_t pos = dir_name.find('.', str_time.size() + 1);
            if (pos != string::npos) {
                actual_expire = std::stoi(dir_name.substr(pos + 1));
            }
            VLOG_TRACE << "get actual expire time " << actual_expire << " of dir: " << dir_name;

            string path_name = sorted_path.string();
            if (difftime(local_now, mktime(&local_tm_create)) >= actual_expire) {
                res = io::global_local_filesystem()->delete_directory(path_name);
                if (!res.ok()) {
                    continue;
                }

                curr_sweep_batch_size++;
                if (config::garbage_sweep_batch_size > 0 &&
                    curr_sweep_batch_size >= config::garbage_sweep_batch_size) {
                    curr_sweep_batch_size = 0;
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            } else {
                // Because files are ordered by filename, i.e. by create time, so all the left files are not expired.
                break;
            }
        }
    } catch (...) {
        res = Status::Error<IO_ERROR>("Exception occur when scan directory. path_desc={}",
                                      scan_root);
    }

    return res;
}

// invalid rowset type config will return ALPHA_ROWSET for system to run smoothly
void StorageEngine::_parse_default_rowset_type() {
    std::string default_rowset_type_config = config::default_rowset_type;
    boost::to_upper(default_rowset_type_config);
    if (default_rowset_type_config == "BETA") {
        _default_rowset_type = BETA_ROWSET;
    } else if (default_rowset_type_config == "ALPHA") {
        _default_rowset_type = ALPHA_ROWSET;
        LOG(WARNING) << "default_rowset_type in be.conf should be set to beta, alpha is not "
                        "supported any more";
    } else {
        LOG(FATAL) << "unknown value " << default_rowset_type_config
                   << " in default_rowset_type in be.conf";
    }
}

void StorageEngine::start_delete_unused_rowset() {
    std::unordered_map<std::string, RowsetSharedPtr> unused_rowsets_copy;
    {
        std::lock_guard<std::mutex> lock(_gc_mutex);
        for (auto it = _unused_rowsets.begin(); it != _unused_rowsets.end();) {
            uint64_t now = UnixSeconds();
            if (it->second.use_count() == 1 && it->second->need_delete_file() &&
                // We delay the GC time of this rowset since it's maybe still needed, see #20732
                now > it->second->delayed_expired_timestamp()) {
                if (it->second->is_local()) {
                    unused_rowsets_copy[it->first] = it->second;
                }
                // remote rowset data will be reclaimed by `remove_unused_remote_files`
                evict_querying_rowset(it->second->rowset_id());
                it = _unused_rowsets.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (auto it = unused_rowsets_copy.begin(); it != unused_rowsets_copy.end(); ++it) {
        VLOG_NOTICE << "start to remove rowset:" << it->second->rowset_id()
                    << ", version:" << it->second->version().first << "-"
                    << it->second->version().second;
        auto tablet_id = it->second->rowset_meta()->tablet_id();
        auto tablet = _tablet_manager->get_tablet(tablet_id);
        // delete delete_bitmap of unused rowsets
        if (tablet != nullptr && tablet->enable_unique_key_merge_on_write()) {
            tablet->tablet_meta()->delete_bitmap().remove({it->second->rowset_id(), 0, 0},
                                                          {it->second->rowset_id(), UINT32_MAX, 0});
        }
        Status status = it->second->remove();
        VLOG_NOTICE << "remove rowset:" << it->second->rowset_id()
                    << " finished. status:" << status;
    }
}

void StorageEngine::add_unused_rowset(RowsetSharedPtr rowset) {
    if (rowset == nullptr) {
        return;
    }

    VLOG_NOTICE << "add unused rowset, rowset id:" << rowset->rowset_id()
                << ", version:" << rowset->version();

    auto rowset_id = rowset->rowset_id().to_string();

    std::lock_guard<std::mutex> lock(_gc_mutex);
    auto it = _unused_rowsets.find(rowset_id);
    if (it == _unused_rowsets.end()) {
        rowset->set_need_delete_file();
        rowset->close();
        _unused_rowsets[rowset_id] = rowset;
        release_rowset_id(rowset->rowset_id());
    }
}

// TODO(zc): refactor this funciton
Status StorageEngine::create_tablet(const TCreateTabletReq& request, RuntimeProfile* profile) {
    // Get all available stores, use ref_root_path if the caller specified
    std::vector<DataDir*> stores;
    {
        SCOPED_TIMER(ADD_TIMER(profile, "GetStores"));
        stores = get_stores_for_create_tablet(request.storage_medium);
    }
    if (stores.empty()) {
        return Status::Error<CE_CMD_PARAMS_ERROR>(
                "there is no available disk that can be used to create tablet.");
    }
    return _tablet_manager->create_tablet(request, stores, profile);
}

Status StorageEngine::obtain_shard_path(TStorageMedium::type storage_medium, int64_t path_hash,
                                        std::string* shard_path, DataDir** store) {
    LOG(INFO) << "begin to process obtain root path. storage_medium=" << storage_medium;

    if (shard_path == nullptr) {
        return Status::Error<CE_CMD_PARAMS_ERROR>(
                "invalid output parameter which is null pointer.");
    }

    auto stores = get_stores_for_create_tablet(storage_medium);
    if (stores.empty()) {
        return Status::Error<NO_AVAILABLE_ROOT_PATH>(
                "no available disk can be used to create tablet.");
    }

    *store = nullptr;
    if (path_hash != -1) {
        for (auto data_dir : stores) {
            if (data_dir->path_hash() == path_hash) {
                *store = data_dir;
                break;
            }
        }
    }
    if (*store == nullptr) {
        *store = stores[0];
    }

    Status res = Status::OK();
    uint64_t shard = 0;
    res = (*store)->get_shard(&shard);
    if (!res.ok()) {
        LOG(WARNING) << "fail to get root path shard. res=" << res;
        return res;
    }

    std::stringstream root_path_stream;
    root_path_stream << (*store)->path() << "/" << DATA_PREFIX << "/" << shard;
    *shard_path = root_path_stream.str();

    LOG(INFO) << "success to process obtain root path. path=" << shard_path;
    return res;
}

Status StorageEngine::load_header(const string& shard_path, const TCloneReq& request,
                                  bool restore) {
    LOG(INFO) << "begin to process load headers."
              << "tablet_id=" << request.tablet_id << ", schema_hash=" << request.schema_hash;
    Status res = Status::OK();

    DataDir* store = nullptr;
    {
        // TODO(zc)
        try {
            auto store_path =
                    std::filesystem::path(shard_path).parent_path().parent_path().string();
            store = get_store(store_path);
            if (store == nullptr) {
                return Status::Error<INVALID_ROOT_PATH>("invalid shard path, path={}", shard_path);
            }
        } catch (...) {
            return Status::Error<INVALID_ROOT_PATH>("invalid shard path, path={}", shard_path);
        }
    }

    std::stringstream schema_hash_path_stream;
    schema_hash_path_stream << shard_path << "/" << request.tablet_id << "/" << request.schema_hash;
    // not surely, reload and restore tablet action call this api
    // reset tablet uid here

    string header_path = TabletMeta::construct_header_file_path(schema_hash_path_stream.str(),
                                                                request.tablet_id);
    res = _tablet_manager->load_tablet_from_dir(store, request.tablet_id, request.schema_hash,
                                                schema_hash_path_stream.str(), false, restore);
    if (!res.ok()) {
        LOG(WARNING) << "fail to process load headers. res=" << res;
        return res;
    }

    LOG(INFO) << "success to process load headers.";
    return res;
}

void StorageEngine::register_report_listener(TaskWorkerPool* listener) {
    std::lock_guard<std::mutex> l(_report_mtx);
    CHECK(_report_listeners.find(listener) == _report_listeners.end());
    _report_listeners.insert(listener);
}

void StorageEngine::deregister_report_listener(TaskWorkerPool* listener) {
    std::lock_guard<std::mutex> l(_report_mtx);
    _report_listeners.erase(listener);
}

void StorageEngine::notify_listeners() {
    std::lock_guard<std::mutex> l(_report_mtx);
    for (auto& listener : _report_listeners) {
        listener->notify_thread();
    }
}

void StorageEngine::notify_listener(TaskWorkerPool::TaskWorkerType task_worker_type) {
    std::lock_guard<std::mutex> l(_report_mtx);
    for (auto& listener : _report_listeners) {
        if (listener->task_worker_type() == task_worker_type) {
            listener->notify_thread();
        }
    }
}

Status StorageEngine::execute_task(EngineTask* task) {
    RETURN_IF_ERROR(task->execute());
    return task->finish();
}

// check whether any unused rowsets's id equal to rowset_id
bool StorageEngine::check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id) {
    std::lock_guard<std::mutex> lock(_gc_mutex);
    auto search = _unused_rowsets.find(rowset_id.to_string());
    return search != _unused_rowsets.end();
}

void StorageEngine::create_cumulative_compaction(
        TabletSharedPtr best_tablet, std::shared_ptr<CumulativeCompaction>& cumulative_compaction) {
    cumulative_compaction.reset(new CumulativeCompaction(best_tablet));
}

void StorageEngine::create_base_compaction(TabletSharedPtr best_tablet,
                                           std::shared_ptr<BaseCompaction>& base_compaction) {
    base_compaction.reset(new BaseCompaction(best_tablet));
}

void StorageEngine::create_full_compaction(TabletSharedPtr best_tablet,
                                           std::shared_ptr<FullCompaction>& full_compaction) {
    full_compaction.reset(new FullCompaction(best_tablet));
}

void StorageEngine::create_single_replica_compaction(
        TabletSharedPtr best_tablet,
        std::shared_ptr<SingleReplicaCompaction>& single_replica_compaction,
        CompactionType compaction_type) {
    single_replica_compaction.reset(new SingleReplicaCompaction(best_tablet, compaction_type));
}

bool StorageEngine::get_peer_replica_info(int64_t tablet_id, TReplicaInfo* replica,
                                          std::string* token) {
    TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "tablet is no longer exist: tablet_id=" << tablet_id;
        return false;
    }
    std::unique_lock<std::mutex> lock(_peer_replica_infos_mutex);
    if (_peer_replica_infos.count(tablet_id) &&
        _peer_replica_infos[tablet_id].replica_id != tablet->replica_id()) {
        *replica = _peer_replica_infos[tablet_id];
        *token = _token;
        return true;
    }
    return false;
}

bool StorageEngine::should_fetch_from_peer(int64_t tablet_id) {
    TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "tablet is no longer exist: tablet_id=" << tablet_id;
        return false;
    }
    std::unique_lock<std::mutex> lock(_peer_replica_infos_mutex);
    if (_peer_replica_infos.count(tablet_id)) {
        return _peer_replica_infos[tablet_id].replica_id != tablet->replica_id();
    }
    return false;
}

// Return json:
// {
//   "CumulativeCompaction": {
//          "/home/disk1" : [10001, 10002],
//          "/home/disk2" : [10003]
//   },
//   "BaseCompaction": {
//          "/home/disk1" : [10001, 10002],
//          "/home/disk2" : [10003]
//   }
// }
Status StorageEngine::get_compaction_status_json(std::string* result) {
    rapidjson::Document root;
    root.SetObject();

    std::unique_lock<std::mutex> lock(_tablet_submitted_compaction_mutex);
    const std::string& cumu = "CumulativeCompaction";
    rapidjson::Value cumu_key;
    cumu_key.SetString(cumu.c_str(), cumu.length(), root.GetAllocator());

    // cumu
    rapidjson::Document path_obj;
    path_obj.SetObject();
    for (auto& it : _tablet_submitted_cumu_compaction) {
        const std::string& dir = it.first->path();
        rapidjson::Value path_key;
        path_key.SetString(dir.c_str(), dir.length(), path_obj.GetAllocator());

        rapidjson::Document arr;
        arr.SetArray();

        for (auto& tablet_id : it.second) {
            rapidjson::Value key;
            const std::string& key_str = std::to_string(tablet_id);
            key.SetString(key_str.c_str(), key_str.length(), path_obj.GetAllocator());
            arr.PushBack(key, root.GetAllocator());
        }
        path_obj.AddMember(path_key, arr, path_obj.GetAllocator());
    }
    root.AddMember(cumu_key, path_obj, root.GetAllocator());

    // base
    const std::string& base = "BaseCompaction";
    rapidjson::Value base_key;
    base_key.SetString(base.c_str(), base.length(), root.GetAllocator());
    rapidjson::Document path_obj2;
    path_obj2.SetObject();
    for (auto& it : _tablet_submitted_base_compaction) {
        const std::string& dir = it.first->path();
        rapidjson::Value path_key;
        path_key.SetString(dir.c_str(), dir.length(), path_obj2.GetAllocator());

        rapidjson::Document arr;
        arr.SetArray();

        for (auto& tablet_id : it.second) {
            rapidjson::Value key;
            const std::string& key_str = std::to_string(tablet_id);
            key.SetString(key_str.c_str(), key_str.length(), path_obj2.GetAllocator());
            arr.PushBack(key, root.GetAllocator());
        }
        path_obj2.AddMember(path_key, arr, path_obj2.GetAllocator());
    }
    root.AddMember(base_key, path_obj2, root.GetAllocator());

    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *result = std::string(strbuf.GetString());
    return Status::OK();
}

void StorageEngine::add_quering_rowset(RowsetSharedPtr rs) {
    std::lock_guard<std::mutex> lock(_quering_rowsets_mutex);
    _querying_rowsets.emplace(rs->rowset_id(), rs);
}

RowsetSharedPtr StorageEngine::get_quering_rowset(RowsetId rs_id) {
    std::lock_guard<std::mutex> lock(_quering_rowsets_mutex);
    auto it = _querying_rowsets.find(rs_id);
    if (it != _querying_rowsets.end()) {
        return it->second;
    }
    return nullptr;
}

void StorageEngine::evict_querying_rowset(RowsetId rs_id) {
    std::lock_guard<std::mutex> lock(_quering_rowsets_mutex);
    _querying_rowsets.erase(rs_id);
}

bool StorageEngine::add_broken_path(std::string path) {
    std::lock_guard<std::mutex> lock(_broken_paths_mutex);
    auto success = _broken_paths.emplace(path).second;
    if (success) {
        static_cast<void>(_persist_broken_paths());
    }
    return success;
}

bool StorageEngine::remove_broken_path(std::string path) {
    std::lock_guard<std::mutex> lock(_broken_paths_mutex);
    auto count = _broken_paths.erase(path);
    if (count > 0) {
        static_cast<void>(_persist_broken_paths());
    }
    return count > 0;
}

Status StorageEngine::_persist_broken_paths() {
    std::string config_value;
    for (const std::string& path : _broken_paths) {
        config_value += path + ";";
    }

    if (config_value.length() > 0) {
        auto st = config::set_config("broken_storage_path", config_value, true);
        LOG(INFO) << "persist broken_storae_path " << config_value << st;
        return st;
    }

    return Status::OK();
}

} // namespace doris
