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

#include <rapidjson/document.h>
#include <signal.h>
#include <sys/syscall.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <cstdio>
#include <filesystem>
#include <new>
#include <queue>
#include <random>
#include <set>

#include "agent/cgroups_mgr.h"
#include "agent/task_worker_pool.h"
#include "env/env.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/data_dir.h"
#include "olap/fs/file_block_manager.h"
#include "olap/lru_cache.h"
#include "olap/memtable_flush_executor.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/unique_rowset_id_generator.h"
#include "olap/schema_change.h"
#include "olap/segment_loader.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "util/doris_metrics.h"
#include "util/file_utils.h"
#include "util/pretty_printer.h"
#include "util/scoped_cleanup.h"
#include "util/time.h"
#include "util/trace.h"

using apache::thrift::ThriftDebugString;
using std::filesystem::canonical;
using std::filesystem::directory_iterator;
using std::filesystem::path;
using std::filesystem::recursive_directory_iterator;
using std::back_inserter;
using std::copy;
using std::inserter;
using std::list;
using std::map;
using std::nothrow;
using std::pair;
using std::set;
using std::set_difference;
using std::string;
using std::stringstream;
using std::vector;
using strings::Substitute;

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(unused_rowsets_count, MetricUnit::ROWSETS);
DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(compaction_mem_consumption, MetricUnit::BYTES, "",
                                   mem_consumption, Labels({{"type", "compaction"}}));

StorageEngine* StorageEngine::_s_instance = nullptr;

static Status _validate_options(const EngineOptions& options) {
    if (options.store_paths.empty()) {
        return Status::InternalError("store paths is empty");
    }
    return Status::OK();
}

Status StorageEngine::open(const EngineOptions& options, StorageEngine** engine_ptr) {
    RETURN_IF_ERROR(_validate_options(options));
    LOG(INFO) << "starting backend using uid:" << options.backend_uid.to_string();
    std::unique_ptr<StorageEngine> engine(new StorageEngine(options));
    RETURN_NOT_OK_STATUS_WITH_WARN(engine->_open(), "open engine failed");
    *engine_ptr = engine.release();
    LOG(INFO) << "success to init storage engine.";
    return Status::OK();
}

StorageEngine::StorageEngine(const EngineOptions& options)
        : _options(options),
          _available_storage_medium_type_count(0),
          _effective_cluster_id(-1),
          _is_all_cluster_id_exist(true),
          _index_stream_lru_cache(nullptr),
          _file_cache(nullptr),
          _compaction_mem_tracker(MemTracker::CreateTracker(-1, "AutoCompaction", nullptr, false,
                                                            false, MemTrackerLevel::OVERVIEW)),
          _tablet_mem_tracker(MemTracker::CreateTracker(-1, "TabletHeader", nullptr, false, false,
                                                        MemTrackerLevel::OVERVIEW)),
          _convert_rowset_mem_tracker(MemTracker::CreateTracker(-1, "ConvertRowset", nullptr, false,
                                                                false, MemTrackerLevel::OVERVIEW)),
          _stop_background_threads_latch(1),
          _tablet_manager(new TabletManager(config::tablet_map_shard_size)),
          _txn_manager(new TxnManager(config::txn_map_shard_size, config::txn_shard_size)),
          _rowset_id_generator(new UniqueRowsetIdGenerator(options.backend_uid)),
          _memtable_flush_executor(nullptr),
          _default_rowset_type(BETA_ROWSET),
          _heartbeat_flags(nullptr),
          _stream_load_recorder(nullptr) {
    if (_s_instance == nullptr) {
        _s_instance = this;
    }
    REGISTER_HOOK_METRIC(unused_rowsets_count, [this]() {
        MutexLock lock(&_gc_mutex);
        return _unused_rowsets.size();
    });
    REGISTER_HOOK_METRIC(compaction_mem_consumption, [this]() {
        return _compaction_mem_tracker->consumption();
        // We can get each compaction's detail usage
        // LOG(INFO) << _compaction_mem_tracker=>LogUsage(2);
    });
}

StorageEngine::~StorageEngine() {
    DEREGISTER_HOOK_METRIC(unused_rowsets_count);
    DEREGISTER_HOOK_METRIC(compaction_mem_consumption);
    _clear();

    if (_base_compaction_thread_pool) {
        _base_compaction_thread_pool->shutdown();
    }
    if (_cumu_compaction_thread_pool) {
        _cumu_compaction_thread_pool->shutdown();
    }
    if (_convert_rowset_thread_pool) {
        _convert_rowset_thread_pool->shutdown();
    }
    if (_tablet_meta_checkpoint_thread_pool) {
        _tablet_meta_checkpoint_thread_pool->shutdown();
    }
}

void StorageEngine::load_data_dirs(const std::vector<DataDir*>& data_dirs) {
    std::vector<std::thread> threads;
    for (auto data_dir : data_dirs) {
        threads.emplace_back([data_dir] {
            auto res = data_dir->load();
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "io error when init load tables. res=" << res
                             << ", data dir=" << data_dir->path();
                // TODO(lingbin): why not exit progress, to force OP to change the conf
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

Status StorageEngine::_open() {
    // init store_map
    RETURN_NOT_OK_STATUS_WITH_WARN(_init_store_map(), "_init_store_map failed");

    _effective_cluster_id = config::cluster_id;
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_all_root_path_cluster_id(), "fail to check cluster id");

    _update_storage_medium_type_count();

    RETURN_NOT_OK_STATUS_WITH_WARN(_check_file_descriptor_number(), "check fd number failed");

    _index_stream_lru_cache =
            new_lru_cache("SegmentIndexCache", config::index_stream_cache_capacity);

    _file_cache.reset(new_lru_cache("FileHandlerCache", config::file_descriptor_cache_capacity));

    auto dirs = get_stores<false>();
    load_data_dirs(dirs);

    _memtable_flush_executor.reset(new MemTableFlushExecutor());
    _memtable_flush_executor->init(dirs);

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
                                     path.remote_path, _tablet_manager.get(), _txn_manager.get());
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
        return Status::InternalError(strings::Substitute("init path failed, error=$0", error_msg));
    }

    for (auto store : tmp_stores) {
        _store_map.emplace(store->path(), store);
    }

    std::string stream_load_record_path = "";
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
    _stream_load_recorder.reset(new StreamLoadRecorder(stream_load_record_path));
    if (_stream_load_recorder == nullptr) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::MemoryAllocFailed("allocate memory for StreamLoadRecorder failed"),
                "new StreamLoadRecorder failed");
    }
    auto st = _stream_load_recorder->init();
    if (!st.ok()) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError(Substitute("open StreamLoadRecorder rocksdb failed, path=$0",
                                           stream_load_record_path)),
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
                    Status::Corruption(
                            strings::Substitute("multiple cluster ids is not equal. one=$0, other=",
                                                _effective_cluster_id, cluster_id)),
                    "cluster id not equal");
        }
    }

    return Status::OK();
}

void StorageEngine::set_store_used_flag(const string& path, bool is_used) {
    std::lock_guard<std::mutex> l(_store_lock);
    auto it = _store_map.find(path);
    if (it == _store_map.end()) {
        LOG(WARNING) << "store not exist, path=" << path;
    }

    it->second->set_is_used(is_used);
    _update_storage_medium_type_count();
}

template <bool include_unused>
std::vector<DataDir*> StorageEngine::get_stores() {
    std::vector<DataDir*> stores;
    stores.reserve(_store_map.size());

    std::lock_guard<std::mutex> l(_store_lock);
    if (include_unused) {
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

OLAPStatus StorageEngine::get_all_data_dir_info(std::vector<DataDirInfo>* data_dir_infos,
                                                bool need_update) {
    OLAPStatus res = OLAP_SUCCESS;
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
                it.second->update_capacity();
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
        data_dir->second->update_user_data_size(path.second.data_used_capacity);
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

int64_t StorageEngine::get_file_or_directory_size(std::filesystem::path file_path) {
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
    bool some_tablets_were_dropped = _delete_tablets_on_unused_root_path();
    // If some tablets were dropped, we should notify disk_state_worker_thread and
    // tablet_worker_thread (see TaskWorkerPool) to make them report to FE ASAP.
    if (some_tablets_were_dropped) {
        notify_listeners();
    }
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
                    Status::Corruption(strings::Substitute(
                            "multiple cluster ids is not equal. one=$0, other=", cluster_id,
                            tmp_cluster_id)),
                    "cluster id not equal");
        }
    }

    // judge and get effective cluster id
    RETURN_IF_ERROR(_judge_and_update_effective_cluster_id(cluster_id));

    // write cluster id into cluster_id_path if get effective cluster id success
    if (_effective_cluster_id != -1 && !_is_all_cluster_id_exist) {
        set_cluster_id(_effective_cluster_id);
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
    std::vector<DataDir*> stores;
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& it : _store_map) {
            if (it.second->is_used()) {
                if (_available_storage_medium_type_count == 1 ||
                    it.second->storage_medium() == storage_medium) {
                    stores.push_back(it.second);
                }
            }
        }
    }
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(stores.begin(), stores.end(), g);
    // Two random choices
    for (int i = 0; i < stores.size(); i++) {
        int j = i + 1;
        if (j < stores.size()) {
            if (stores[i]->tablet_size() > stores[j]->tablet_size()) {
                std::swap(stores[i], stores[j]);
            }
            std::shuffle(stores.begin() + j, stores.end(), g);
        } else {
            break;
        }
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

bool StorageEngine::_delete_tablets_on_unused_root_path() {
    std::vector<TabletInfo> tablet_info_vec;
    uint32_t unused_root_path_num = 0;
    uint32_t total_root_path_num = 0;

    {
        // TODO(yingchun): _store_map is only updated in main and ~StorageEngine, maybe we can remove it?
        std::lock_guard<std::mutex> l(_store_lock);
        if (_store_map.empty()) {
            return false;
        }

        for (auto& it : _store_map) {
            ++total_root_path_num;
            if (it.second->is_used()) {
                continue;
            }
            it.second->clear_tablets(&tablet_info_vec);
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

    _tablet_manager->drop_tablets_on_error_root_path(tablet_info_vec);
    // If tablet_info_vec is not empty, means we have dropped some tablets.
    return !tablet_info_vec.empty();
}

void StorageEngine::stop() {
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
    if (_alpha_rowset_scan_thread) {
        THREAD_JOIN(_alpha_rowset_scan_thread);
    }
    THREAD_JOIN(_unused_rowset_monitor_thread);
    THREAD_JOIN(_garbage_sweeper_thread);
    THREAD_JOIN(_disk_stat_monitor_thread);
    THREAD_JOIN(_fd_cache_clean_thread);
    THREAD_JOIN(_tablet_checkpoint_tasks_producer_thread);
#undef THREAD_JOIN

#define THREADS_JOIN(threads)           \
    for (const auto& thread : threads) {\
        if (thread) {                   \
            thread->join();             \
        }                               \
    }

    THREADS_JOIN(_path_gc_threads);
    THREADS_JOIN(_path_scan_threads);
#undef THREADS_JOIN
}

void StorageEngine::_clear() {
    SAFE_DELETE(_index_stream_lru_cache);
    _file_cache.reset();

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
                                                                 tablet_info.first.schema_hash,
                                                                 tablet_info.first.tablet_uid);
            // The tablet may be dropped or altered, leave a INFO log and go on process other tablet
            if (tablet == nullptr) {
                LOG(INFO) << "tablet is no longer exist. tablet_id=" << tablet_info.first.tablet_id
                          << ", schema_hash=" << tablet_info.first.schema_hash
                          << ", tablet_uid=" << tablet_info.first.tablet_uid;
                continue;
            }
            StorageEngine::instance()->txn_manager()->delete_txn(partition_id, tablet,
                                                                 transaction_id);
        }
    }
    LOG(INFO) << "finish to clear transaction task. transaction_id=" << transaction_id;
}

void StorageEngine::_start_clean_cache() {
    _file_cache->prune();
    SegmentLoader::instance()->prune();
}

OLAPStatus StorageEngine::start_trash_sweep(double* usage, bool ignore_guard) {
    OLAPStatus res = OLAP_SUCCESS;

    std::unique_lock<std::mutex> l(_trash_sweep_lock, std::defer_lock);
    if (!l.try_lock()) {
        LOG(INFO) << "trash and snapshot sweep is running.";
        return res;
    }

    LOG(INFO) << "start trash and snapshot sweep.";

    const int32_t snapshot_expire = config::snapshot_expire_time_sec;
    const int32_t trash_expire = config::trash_file_expire_time_sec;
    // the guard space should be lower than storage_flood_stage_usage_percent,
    // so here we multiply 0.9
    // if ignore_guard is true, set guard_space to 0.
    const double guard_space =
            ignore_guard ? 0 : config::storage_flood_stage_usage_percent / 100.0 * 0.9;
    std::vector<DataDirInfo> data_dir_infos;
    RETURN_NOT_OK_LOG(get_all_data_dir_info(&data_dir_infos, false),
                      "failed to get root path stat info when sweep trash.")
    std::sort(data_dir_infos.begin(), data_dir_infos.end(), DataDirInfoLessAvailability());

    time_t now = time(nullptr); //获取UTC时间
    tm local_tm_now;
    local_tm_now.tm_isdst = 0;
    if (localtime_r(&now, &local_tm_now) == nullptr) {
        LOG(WARNING) << "fail to localtime_r time. time=" << now;
        return OLAP_ERR_OS_ERROR;
    }
    const time_t local_now = mktime(&local_tm_now); //得到当地日历时间

    double tmp_usage = 0.0;
    for (DataDirInfo& info : data_dir_infos) {
        LOG(INFO) << "Start to sweep path " << info.path_desc.filepath;
        if (!info.is_used) {
            continue;
        }

        double curr_usage = (double)(info.disk_capacity - info.available) / info.disk_capacity;
        tmp_usage = std::max(tmp_usage, curr_usage);

        OLAPStatus curr_res = OLAP_SUCCESS;
        string snapshot_path = info.path_desc.filepath + SNAPSHOT_PREFIX;
        curr_res = _do_sweep(snapshot_path, local_now, snapshot_expire);
        if (curr_res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to sweep snapshot. path=" << snapshot_path
                         << ", err_code=" << curr_res;
            res = curr_res;
        }

        string trash_path = info.path_desc.filepath + TRASH_PREFIX;
        curr_res = _do_sweep(trash_path, local_now, curr_usage > guard_space ? 0 : trash_expire);
        if (curr_res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to sweep trash. [path=%s" << trash_path
                         << ", err_code=" << curr_res;
            res = curr_res;
        }
    }

    if (usage != nullptr) {
        *usage = tmp_usage; // update usage
    }

    // clear expire incremental rowset, move deleted tablet to trash
    _tablet_manager->start_trash_sweep();

    // clean rubbish transactions
    _clean_unused_txns();

    // clean unused rowset metas in OlapMeta
    _clean_unused_rowset_metas();

    return res;
}

void StorageEngine::_clean_unused_rowset_metas() {
    std::vector<RowsetMetaSharedPtr> invalid_rowset_metas;
    auto clean_rowset_func = [this, &invalid_rowset_metas](TabletUid tablet_uid, RowsetId rowset_id,
                                                           const std::string& meta_str) -> bool {
        // return false will break meta iterator, return true to skip this error
        RowsetMetaSharedPtr rowset_meta(new AlphaRowsetMeta());
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

        TabletSharedPtr tablet = _tablet_manager->get_tablet(
                rowset_meta->tablet_id(), rowset_meta->tablet_schema_hash());
        if (tablet == nullptr) {
            // tablet may be dropped
            // TODO(cmy): this is better to be a VLOG, because drop table is a very common case.
            // leave it as INFO log for observation. Maybe change it in future.
            LOG(INFO) << "failed to find tablet " << rowset_meta->tablet_id() << " for rowset: " << rowset_meta->rowset_id()
                      << ", tablet may be dropped";
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
            LOG(WARNING) << "rowset's tablet uid " << rowset_meta->tablet_uid() << " does not equal to tablet uid: " << tablet->tablet_uid();
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
        if (data_dir->is_remote()) {
            continue;
        }
        RowsetMetaManager::traverse_rowset_metas(data_dir->get_meta(), clean_rowset_func);
        for (auto& rowset_meta : invalid_rowset_metas) {
            RowsetMetaManager::remove(data_dir->get_meta(), rowset_meta->tablet_uid(),
                    rowset_meta->rowset_id());
        }
        LOG(INFO) << "remove " << invalid_rowset_metas.size() << " invalid rowset meta from dir: " << data_dir->path();
        invalid_rowset_metas.clear();
    }
}

void StorageEngine::_clean_unused_txns() {
    std::set<TabletInfo> tablet_infos;
    _txn_manager->get_all_related_tablets(&tablet_infos);
    for (auto& tablet_info : tablet_infos) {
        TabletSharedPtr tablet = _tablet_manager->get_tablet(
                tablet_info.tablet_id, tablet_info.schema_hash, tablet_info.tablet_uid, true);
        if (tablet == nullptr) {
            // TODO(ygl) :  should check if tablet still in meta, it's a improvement
            // case 1: tablet still in meta, just remove from memory
            // case 2: tablet not in meta store, remove rowset from meta
            // currently just remove them from memory
            // nullptr to indicate not remove them from meta store
            _txn_manager->force_rollback_tablet_related_txns(nullptr, tablet_info.tablet_id,
                                                             tablet_info.schema_hash,
                                                             tablet_info.tablet_uid);
        }
    }
}

OLAPStatus StorageEngine::_do_sweep(const string& scan_root, const time_t& local_now,
                                    const int32_t expire) {
    OLAPStatus res = OLAP_SUCCESS;
    if (!FileUtils::check_exist(scan_root)) {
        // dir not existed. no need to sweep trash.
        return res;
    }

    try {
        // Sort pathes by name, that is by delete time.
        std::vector<path> sorted_pathes;
        std::copy(directory_iterator(path(scan_root)), directory_iterator(),
                  std::back_inserter(sorted_pathes));
        std::sort(sorted_pathes.begin(), sorted_pathes.end());
        for (const auto& sorted_path : sorted_pathes) {
            string dir_name = sorted_path.filename().string();
            string str_time = dir_name.substr(0, dir_name.find('.'));
            tm local_tm_create;
            local_tm_create.tm_isdst = 0;
            if (strptime(str_time.c_str(), "%Y%m%d%H%M%S", &local_tm_create) == nullptr) {
                LOG(WARNING) << "fail to strptime time. [time=" << str_time << "]";
                res = OLAP_ERR_OS_ERROR;
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
                Status ret = FileUtils::remove_all(path_name);
                if (!ret.ok()) {
                    LOG(WARNING) << "fail to remove file or directory. path=" << path_name
                                 << ", error=" << ret.to_string();
                    res = OLAP_ERR_OS_ERROR;
                    continue;
                }
            } else {
                // Because files are ordered by filename, i.e. by create time, so all the left files are not expired.
                break;
            }
        }
    } catch (...) {
        LOG(WARNING) << "Exception occur when scan directory. path=" << scan_root;
        res = OLAP_ERR_IO_ERROR;
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
    MutexLock lock(&_gc_mutex);
    for (auto it = _unused_rowsets.begin(); it != _unused_rowsets.end();) {
        if (it->second.use_count() != 1) {
            ++it;
        } else if (it->second->need_delete_file()) {
            VLOG_NOTICE << "start to remove rowset:" << it->second->rowset_id()
                        << ", version:" << it->second->version().first << "-"
                        << it->second->version().second;
            OLAPStatus status = it->second->remove();
            VLOG_NOTICE << "remove rowset:" << it->second->rowset_id()
                        << " finished. status:" << status;
            it = _unused_rowsets.erase(it);
        }
    }
}

void StorageEngine::add_unused_rowset(RowsetSharedPtr rowset) {
    if (rowset == nullptr) {
        return;
    }

    VLOG_NOTICE << "add unused rowset, rowset id:" << rowset->rowset_id()
                << ", version:" << rowset->version().first << "-" << rowset->version().second
                << ", unique id:" << rowset->unique_id();

    auto rowset_id = rowset->rowset_id().to_string();

    MutexLock lock(&_gc_mutex);
    auto it = _unused_rowsets.find(rowset_id);
    if (it == _unused_rowsets.end()) {
        rowset->set_need_delete_file();
        rowset->close();
        _unused_rowsets[rowset_id] = rowset;
        release_rowset_id(rowset->rowset_id());
    }
}

// TODO(zc): refactor this funciton
OLAPStatus StorageEngine::create_tablet(const TCreateTabletReq& request) {
    // Get all available stores, use ref_root_path if the caller specified
    std::vector<DataDir*> stores;
    stores = get_stores_for_create_tablet(request.storage_medium);
    if (stores.empty()) {
        LOG(WARNING) << "there is no available disk that can be used to create tablet.";
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }
    TRACE("got data directory for create tablet");
    return _tablet_manager->create_tablet(request, stores);
}

OLAPStatus StorageEngine::obtain_shard_path(TStorageMedium::type storage_medium,
                                            std::string* shard_path, DataDir** store) {
    LOG(INFO) << "begin to process obtain root path. storage_medium=" << storage_medium;

    if (shard_path == nullptr) {
        LOG(WARNING) << "invalid output parameter which is null pointer.";
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    auto stores = get_stores_for_create_tablet(storage_medium);
    if (stores.empty()) {
        LOG(WARNING) << "no available disk can be used to create tablet.";
        return OLAP_ERR_NO_AVAILABLE_ROOT_PATH;
    }

    OLAPStatus res = OLAP_SUCCESS;
    uint64_t shard = 0;
    res = stores[0]->get_shard(&shard);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to get root path shard. res=" << res;
        return res;
    }

    std::stringstream root_path_stream;
    root_path_stream << stores[0]->path() << DATA_PREFIX << "/" << shard;
    *shard_path = root_path_stream.str();
    *store = stores[0];

    LOG(INFO) << "success to process obtain root path. path=" << shard_path;
    return res;
}

OLAPStatus StorageEngine::load_header(const string& shard_path, const TCloneReq& request,
                                      bool restore) {
    LOG(INFO) << "begin to process load headers."
              << "tablet_id=" << request.tablet_id << ", schema_hash=" << request.schema_hash;
    OLAPStatus res = OLAP_SUCCESS;

    DataDir* store = nullptr;
    {
        // TODO(zc)
        try {
            auto store_path =
                    std::filesystem::path(shard_path).parent_path().parent_path().string();
            store = get_store(store_path);
            if (store == nullptr) {
                LOG(WARNING) << "invalid shard path, path=" << shard_path;
                return OLAP_ERR_INVALID_ROOT_PATH;
            }
        } catch (...) {
            LOG(WARNING) << "invalid shard path, path=" << shard_path;
            return OLAP_ERR_INVALID_ROOT_PATH;
        }
    }

    std::stringstream schema_hash_path_stream;
    schema_hash_path_stream << shard_path << "/" << request.tablet_id << "/" << request.schema_hash;
    // not surely, reload and restore tablet action call this api
    // reset tablet uid here

    string header_path = TabletMeta::construct_header_file_path(schema_hash_path_stream.str(),
                                                                request.tablet_id);
    res = TabletMeta::reset_tablet_uid(header_path);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail reset tablet uid file path = " << header_path << " res=" << res;
        return res;
    }
    res = _tablet_manager->load_tablet_from_dir(store, request.tablet_id, request.schema_hash,
                                                schema_hash_path_stream.str(), false, restore);
    if (res != OLAP_SUCCESS) {
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

OLAPStatus StorageEngine::execute_task(EngineTask* task) {
    {
        std::vector<TabletInfo> tablet_infos;
        task->get_related_tablets(&tablet_infos);
        sort(tablet_infos.begin(), tablet_infos.end());
        std::vector<TabletSharedPtr> related_tablets;
        std::vector<UniqueWriteLock> wrlocks;
        for (TabletInfo& tablet_info : tablet_infos) {
            TabletSharedPtr tablet =
                    _tablet_manager->get_tablet(tablet_info.tablet_id, tablet_info.schema_hash);
            if (tablet != nullptr) {
                related_tablets.push_back(tablet);
                wrlocks.push_back(UniqueWriteLock(tablet->get_header_lock()));
            } else {
                LOG(WARNING) << "could not get tablet before prepare tabletid: "
                             << tablet_info.tablet_id;
            }
        }
        // add write lock to all related tablets
        OLAPStatus prepare_status = task->prepare();
        if (prepare_status != OLAP_SUCCESS) {
            return prepare_status;
        }
    }

    // do execute work without lock
    OLAPStatus exec_status = task->execute();
    if (exec_status != OLAP_SUCCESS) {
        return exec_status;
    }

    {
        std::vector<TabletInfo> tablet_infos;
        // related tablets may be changed after execute task, so that get them here again
        task->get_related_tablets(&tablet_infos);
        sort(tablet_infos.begin(), tablet_infos.end());
        std::vector<TabletSharedPtr> related_tablets;
        std::vector<UniqueWriteLock> wrlocks;
        for (TabletInfo& tablet_info : tablet_infos) {
            TabletSharedPtr tablet =
                    _tablet_manager->get_tablet(tablet_info.tablet_id, tablet_info.schema_hash);
            if (tablet != nullptr) {
                related_tablets.push_back(tablet);
                wrlocks.push_back(UniqueWriteLock(tablet->get_header_lock()));
            } else {
                LOG(WARNING) << "could not get tablet before finish tabletid: "
                             << tablet_info.tablet_id;
            }
        }
        // add write lock to all related tablets
        OLAPStatus fin_status = task->finish();
        return fin_status;
    }
}

// check whether any unused rowsets's id equal to rowset_id
bool StorageEngine::check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id) {
    MutexLock lock(&_gc_mutex);
    auto search = _unused_rowsets.find(rowset_id.to_string());
    return search != _unused_rowsets.end();
}

void StorageEngine::create_cumulative_compaction(
        TabletSharedPtr best_tablet, std::shared_ptr<CumulativeCompaction>& cumulative_compaction) {
    std::string tracker_label =
            "StorageEngine:CumulativeCompaction:" + std::to_string(best_tablet->tablet_id());
    cumulative_compaction.reset(
            new CumulativeCompaction(best_tablet, tracker_label, _compaction_mem_tracker));
}

void StorageEngine::create_base_compaction(TabletSharedPtr best_tablet,
                                           std::shared_ptr<BaseCompaction>& base_compaction) {
    std::string tracker_label =
            "StorageEngine:BaseCompaction:" + std::to_string(best_tablet->tablet_id());
    base_compaction.reset(new BaseCompaction(best_tablet, tracker_label, _compaction_mem_tracker));
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

} // namespace doris
