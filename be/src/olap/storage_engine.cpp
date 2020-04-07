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

#include <signal.h>

#include <algorithm>
#include <cstdio>
#include <new>
#include <queue>
#include <set>
#include <random>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <rapidjson/document.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "env/env.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/lru_cache.h"
#include "olap/memtable_flush_executor.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/schema_change.h"
#include "olap/data_dir.h"
#include "olap/utils.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/olap_snapshot_converter.h"
#include "olap/rowset/unique_rowset_id_generator.h"
#include "olap/fs/file_block_manager.h"
#include "util/time.h"
#include "util/doris_metrics.h"
#include "util/pretty_printer.h"
#include "util/file_utils.h"
#include "agent/cgroups_mgr.h"

using apache::thrift::ThriftDebugString;
using boost::filesystem::canonical;
using boost::filesystem::directory_iterator;
using boost::filesystem::path;
using boost::filesystem::recursive_directory_iterator;
using std::back_inserter;
using std::copy;
using std::inserter;
using std::list;
using std::map;
using std::nothrow;
using std::pair;
using std::priority_queue;
using std::set;
using std::set_difference;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {

StorageEngine* StorageEngine::_s_instance = nullptr;

static Status _validate_options(const EngineOptions& options) {
    if (options.store_paths.empty()) {
        return Status::InternalError("store paths is empty");;
    }
    return Status::OK();
}

Status StorageEngine::open(const EngineOptions& options, StorageEngine** engine_ptr) {
    RETURN_IF_ERROR(_validate_options(options));
    LOG(INFO) << "starting backend using uid:" << options.backend_uid.to_string();
    std::unique_ptr<StorageEngine> engine(new StorageEngine(options));
    auto st = engine->_open();
    if (st != OLAP_SUCCESS) {
        LOG(WARNING) << "engine open failed, res=" << st;
        return Status::InternalError("open engine failed");
    }
    st = engine->_start_bg_worker();
    if (st != OLAP_SUCCESS) {
        LOG(WARNING) << "engine start background failed, res=" << st;
        return Status::InternalError("open engine failed");
    }
    *engine_ptr = engine.release();
    LOG(INFO) << "success to init storage engine.";
    return Status::OK();
}

StorageEngine::StorageEngine(const EngineOptions& options)
        : _options(options),
        _available_storage_medium_type_count(0),
        _effective_cluster_id(-1),
        _is_all_cluster_id_exist(true),
        _index_stream_lru_cache(NULL),
        _tablet_manager(new TabletManager(config::tablet_map_shard_size)),
        _txn_manager(new TxnManager()),
        _rowset_id_generator(new UniqueRowsetIdGenerator(options.backend_uid)),
        _memtable_flush_executor(nullptr),
        _block_manager(nullptr),
        _default_rowset_type(ALPHA_ROWSET),
        _heartbeat_flags(nullptr) {
    if (_s_instance == nullptr) {
        _s_instance = this;
    }
}

StorageEngine::~StorageEngine() {
    _clear();
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

OLAPStatus StorageEngine::_open() {
    // init store_map
    RETURN_NOT_OK(_init_store_map());

    _effective_cluster_id = config::cluster_id;
    RETURN_NOT_OK_LOG(_check_all_root_path_cluster_id(), "fail to check cluster info.");

    _update_storage_medium_type_count();

    RETURN_NOT_OK(_check_file_descriptor_number());

    auto cache = new_lru_cache(config::file_descriptor_cache_capacity);
    FileHandler::set_fd_cache(cache);

    _index_stream_lru_cache = new_lru_cache(config::index_stream_cache_capacity);

    auto dirs = get_stores<false>();
    load_data_dirs(dirs);

    _memtable_flush_executor.reset(new MemTableFlushExecutor());
    _memtable_flush_executor->init(dirs);

    fs::BlockManagerOptions bm_opts;
    bm_opts.read_only = false;
    _block_manager.reset(new fs::FileBlockManager(Env::Default(), std::move(bm_opts)));

    _parse_default_rowset_type();

    return OLAP_SUCCESS;
}

OLAPStatus StorageEngine::_init_store_map() {
    std::vector<DataDir*> tmp_stores;
    std::vector<std::thread> threads;
    std::atomic<bool> init_error{false};
    for (auto& path : _options.store_paths) {
        DataDir* store = new DataDir(path.path, path.capacity_bytes, path.storage_medium,
                                     _tablet_manager.get(), _txn_manager.get());
        tmp_stores.emplace_back(store);
        threads.emplace_back([store, &init_error]() {
            auto st = store->init();
            if (!st.ok()) {
                init_error = true;
                LOG(WARNING) << "Store load failed, status="<< st.to_string() << ", path=" << store->path();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    if (init_error) {
        for (auto store : tmp_stores) {
            delete store;
        }
        return OLAP_ERR_INVALID_ROOT_PATH;
    }

    for (auto store : tmp_stores) {
        _store_map.emplace(store->path(), store);
    }
    return OLAP_SUCCESS;
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

OLAPStatus StorageEngine::_judge_and_update_effective_cluster_id(int32_t cluster_id) {
    OLAPStatus res = OLAP_SUCCESS;

    if (cluster_id == -1 && _effective_cluster_id == -1) {
        // maybe this is a new cluster, cluster id will get from heartbeat message
        return res;
    } else if (cluster_id != -1 && _effective_cluster_id == -1) {
        _effective_cluster_id = cluster_id;
        return res;
    } else if (cluster_id == -1 && _effective_cluster_id != -1) {
        // _effective_cluster_id is the right effective cluster id
        return res;
    } else {
        if (cluster_id != _effective_cluster_id) {
            LOG(WARNING) << "multiple cluster ids is not equal. id1=" << _effective_cluster_id
                    << " id2=" << cluster_id;
            return OLAP_ERR_INVALID_CLUSTER_INFO;
        }
    }

    return res;
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

template<bool include_unused>
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

OLAPStatus StorageEngine::get_all_data_dir_info(vector<DataDirInfo>* data_dir_infos,
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

    // add path info to data_dir_infos
    for (auto& entry : path_map) {
        data_dir_infos->emplace_back(entry.second);
    }

    timer.stop();
    LOG(INFO) << "get root path info cost: " << timer.elapsed_time() / 1000000
            << " ms. tablet counter: " << tablet_count;

    return res;
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
        trigger_report();
    }
}

// TODO(lingbin): Should be in EnvPosix?
OLAPStatus StorageEngine::_check_file_descriptor_number() {
    struct rlimit l;
    int ret = getrlimit(RLIMIT_NOFILE , &l);
    if (ret != 0) {
        LOG(WARNING) << "call getrlimit() failed. errno=" << strerror(errno)
                     << ", use default configuration instead.";
        return OLAP_SUCCESS;
    }
    if (l.rlim_cur < config::min_file_descriptor_number) {
        LOG(ERROR) << "File descriptor number is less than " << config::min_file_descriptor_number
                   << ". Please use (ulimit -n) to set a value equal or greater than "
                   << config::min_file_descriptor_number;
        return OLAP_ERR_TOO_FEW_FILE_DESCRITPROR;
    }
    return OLAP_SUCCESS;
}

OLAPStatus StorageEngine::_check_all_root_path_cluster_id() {
    int32_t cluster_id = -1;
    for (auto& it : _store_map) {
        int32_t tmp_cluster_id = it.second->cluster_id();
        if (tmp_cluster_id == -1) {
            _is_all_cluster_id_exist = false;
        } else if (tmp_cluster_id == cluster_id) {
            // both hava right cluster id, do nothing
        } else if (cluster_id == -1) {
            cluster_id = tmp_cluster_id;
        } else {
            LOG(WARNING) << "multiple cluster ids is not equal. one=" << cluster_id
                << ", other=" << tmp_cluster_id;
            return OLAP_ERR_INVALID_CLUSTER_INFO;
        }
    }

    // judge and get effective cluster id
    OLAPStatus res = OLAP_SUCCESS;
    RETURN_NOT_OK(_judge_and_update_effective_cluster_id(cluster_id));

    // write cluster id into cluster_id_path if get effective cluster id success
    if (_effective_cluster_id != -1 && !_is_all_cluster_id_exist) {
        set_cluster_id(_effective_cluster_id);
    }

    return res;
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
                if (_available_storage_medium_type_count == 1
                    || it.second->storage_medium() == storage_medium) {
                    stores.push_back(it.second);
                }
            }
        }
    }
    //  TODO(lingbin): should it be a global util func?
    std::random_device rd;
    srand(rd());
    std::random_shuffle(stores.begin(), stores.end());
    return stores;
}

DataDir* StorageEngine::get_store(const std::string& path) {
    // _store_map is unchanged, no need to lock
    auto it = _store_map.find(path);
    if (it == std::end(_store_map)) {
        return nullptr;
    }
    return it->second;
}

static bool too_many_disks_are_failed(uint32_t unused_num, uint32_t total_num) {
    return ((total_num == 0)
            || (unused_num * 100 / total_num > config::max_percentage_of_error_disk));
}

bool StorageEngine::_delete_tablets_on_unused_root_path() {
    vector<TabletInfo> tablet_info_vec;
    uint32_t unused_root_path_num = 0;
    uint32_t total_root_path_num = 0;

    std::lock_guard<std::mutex> l(_store_lock);
    if (_store_map.size() == 0) {
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

void StorageEngine::_clear() {
    // 删除lru中所有内容,其实进程退出这么做本身意义不大,但对单测和更容易发现问题还是有很大意义的
    delete FileHandler::get_fd_cache();
    FileHandler::set_fd_cache(nullptr);
    SAFE_DELETE(_index_stream_lru_cache);

    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& store_pair : _store_map) {
        store_pair.second->stop_bg_worker();
        delete store_pair.second;
        store_pair.second = nullptr;
    }
    _store_map.clear();

    _stop_bg_worker = true;
}

void StorageEngine::clear_transaction_task(const TTransactionId transaction_id) {
    // clear transaction task may not contains partitions ids, we should get partition id from txn manager.
    std::vector<int64_t> partition_ids;
    StorageEngine::instance()->txn_manager()->get_partition_ids(transaction_id, &partition_ids);
    clear_transaction_task(transaction_id, partition_ids);
}

void StorageEngine::clear_transaction_task(const TTransactionId transaction_id,
        const vector<TPartitionId>& partition_ids) {
    LOG(INFO) << "begin to clear transaction task. transaction_id=" <<  transaction_id;

    for (const TPartitionId& partition_id : partition_ids) {
        std::map<TabletInfo, RowsetSharedPtr> tablet_infos;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(transaction_id, partition_id, &tablet_infos);

        // each tablet
        for (auto& tablet_info : tablet_infos) {
            // should use tablet uid to ensure clean txn correctly
            TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_info.first.tablet_id,
                tablet_info.first.schema_hash, tablet_info.first.tablet_uid);
            // The tablet may be dropped or altered, leave a INFO log and go on process other tablet
            if (tablet == nullptr) {
                LOG(INFO) << "tablet is no longer exist. tablet_id=" << tablet_info.first.tablet_id
                    << ", schema_hash=" << tablet_info.first.schema_hash
                    << ", tablet_uid=" << tablet_info.first.tablet_uid;
                continue;
            }
            StorageEngine::instance()->txn_manager()->delete_txn(partition_id, tablet, transaction_id);
        }
    }
    LOG(INFO) << "finish to clear transaction task. transaction_id=" << transaction_id;
}

void StorageEngine::_start_clean_fd_cache() {
    VLOG(10) << "start clean file descritpor cache";
    FileHandler::get_fd_cache()->prune();
    VLOG(10) << "end clean file descritpor cache";
}

void StorageEngine::_perform_cumulative_compaction(DataDir* data_dir) {
    TabletSharedPtr best_tablet = _tablet_manager->find_best_tablet_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, data_dir);
    if (best_tablet == nullptr) {
        return;
    }

    DorisMetrics::cumulative_compaction_request_total.increment(1);
    CumulativeCompaction cumulative_compaction(best_tablet);

    OLAPStatus res = cumulative_compaction.compact();
    if (res != OLAP_SUCCESS) {
        best_tablet->set_last_cumu_compaction_failure_time(UnixMillis());
        if (res != OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS) {
            DorisMetrics::cumulative_compaction_request_failed.increment(1);
            LOG(WARNING) << "failed to do cumulative compaction. res=" << res
                        << ", table=" << best_tablet->full_name();
        }
        return;
    }
    best_tablet->set_last_cumu_compaction_failure_time(0);
}

void StorageEngine::_perform_base_compaction(DataDir* data_dir) {
    TabletSharedPtr best_tablet = _tablet_manager->find_best_tablet_to_compaction(
            CompactionType::BASE_COMPACTION, data_dir);
    if (best_tablet == nullptr) {
        return;
    }

    DorisMetrics::base_compaction_request_total.increment(1);
    BaseCompaction base_compaction(best_tablet);
    OLAPStatus res = base_compaction.compact();
    if (res != OLAP_SUCCESS) {
        best_tablet->set_last_base_compaction_failure_time(UnixMillis());
        if (res != OLAP_ERR_BE_NO_SUITABLE_VERSION) {
            DorisMetrics::base_compaction_request_failed.increment(1);
            LOG(WARNING) << "failed to init base compaction. res=" << res
                        << ", table=" << best_tablet->full_name();
        }
        return;
    }
    best_tablet->set_last_base_compaction_failure_time(0);
}

void StorageEngine::get_cache_status(rapidjson::Document* document) const {
    return _index_stream_lru_cache->get_cache_status(document);
}

OLAPStatus StorageEngine::_start_trash_sweep(double* usage) {
    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "start trash and snapshot sweep.";

    const int32_t snapshot_expire = config::snapshot_expire_time_sec;
    const int32_t trash_expire = config::trash_file_expire_time_sec;
    const double guard_space = config::storage_flood_stage_usage_percent / 100.0;
    std::vector<DataDirInfo> data_dir_infos;
    RETURN_NOT_OK_LOG(get_all_data_dir_info(&data_dir_infos, false),
                      "failed to get root path stat info when sweep trash.")

    time_t now = time(nullptr); //获取UTC时间
    tm local_tm_now;
    if (localtime_r(&now, &local_tm_now) == nullptr) {
        LOG(WARNING) << "fail to localtime_r time. time=" << now;
        return OLAP_ERR_OS_ERROR;
    }
    const time_t local_now = mktime(&local_tm_now); //得到当地日历时间

    for (DataDirInfo& info : data_dir_infos) {
        if (!info.is_used) {
            continue;
        }

        double curr_usage = (double) (info.disk_capacity - info.available) / info.disk_capacity;
        *usage = *usage > curr_usage ? *usage : curr_usage;

        OLAPStatus curr_res = OLAP_SUCCESS;
        string snapshot_path = info.path + SNAPSHOT_PREFIX;
        curr_res = _do_sweep(snapshot_path, local_now, snapshot_expire);
        if (curr_res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to sweep snapshot. path=" << snapshot_path
                    << ", err_code=" << curr_res;
            res = curr_res;
        }

        string trash_path = info.path + TRASH_PREFIX;
        curr_res = _do_sweep(trash_path, local_now,
                curr_usage > guard_space ? 0 : trash_expire);
        if (curr_res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to sweep trash. [path=%s" << trash_path
                    << ", err_code=" << curr_res;
            res = curr_res;
        }
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

        RowsetMetaSharedPtr rowset_meta(new AlphaRowsetMeta());
        bool parsed = rowset_meta->init(meta_str);
        if (!parsed) {
            LOG(WARNING) << "parse rowset meta string failed for rowset_id:" << rowset_id;
            // return false will break meta iterator, return true to skip this error
            return true;
        }
        if (rowset_meta->tablet_uid() != tablet_uid) {
            LOG(WARNING) << "tablet uid is not equal, skip the rowset"
                         << ", rowset_id=" << rowset_meta->rowset_id()
                         << ", in_put_tablet_uid=" << tablet_uid
                         << ", tablet_uid in rowset meta=" << rowset_meta->tablet_uid();
            return true;
        }

        TabletSharedPtr tablet = _tablet_manager->get_tablet(rowset_meta->tablet_id(), rowset_meta->tablet_schema_hash(), tablet_uid);
        if (tablet == nullptr) {
            return true;
        }
        if (rowset_meta->rowset_state() == RowsetStatePB::VISIBLE && (!tablet->rowset_meta_is_useful(rowset_meta))) {
            LOG(INFO) << "rowset meta is useless any more, remote it. rowset_id=" << rowset_meta->rowset_id();
            invalid_rowset_metas.push_back(rowset_meta);
        }
        return true;
    };
    auto data_dirs = get_stores();
    for (auto data_dir : data_dirs) {
        RowsetMetaManager::traverse_rowset_metas(data_dir->get_meta(), clean_rowset_func);
        for (auto& rowset_meta : invalid_rowset_metas) {
            RowsetMetaManager::remove(data_dir->get_meta(), rowset_meta->tablet_uid(), rowset_meta->rowset_id());
        }
        invalid_rowset_metas.clear();
    }
}

void StorageEngine::_clean_unused_txns() {
    std::set<TabletInfo> tablet_infos;
    _txn_manager->get_all_related_tablets(&tablet_infos);
    for (auto& tablet_info : tablet_infos) {
        TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_info.tablet_id, tablet_info.schema_hash, tablet_info.tablet_uid, true);
        if (tablet == nullptr) {
            // TODO(ygl) :  should check if tablet still in meta, it's a improvement
            // case 1: tablet still in meta, just remove from memory
            // case 2: tablet not in meta store, remove rowset from meta
            // currently just remove them from memory
            // nullptr to indicate not remove them from meta store
            _txn_manager->force_rollback_tablet_related_txns(nullptr, tablet_info.tablet_id, tablet_info.schema_hash,
                tablet_info.tablet_uid);
        }
    }
}

OLAPStatus StorageEngine::_do_sweep(
        const string& scan_root, const time_t& local_now, const int32_t expire) {
    OLAPStatus res = OLAP_SUCCESS;
    if (!FileUtils::check_exist(scan_root)) {
        // dir not existed. no need to sweep trash.
        return res;
    }

    try {
        path boost_scan_root(scan_root);
        directory_iterator item(boost_scan_root);
        directory_iterator item_end;
        for (; item != item_end; ++item) {
            string path_name = item->path().string();
            string dir_name = item->path().filename().string();
            string str_time = dir_name.substr(0, dir_name.find('.'));
            tm local_tm_create;
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
            VLOG(10) << "get actual expire time " << actual_expire << " of dir: " << dir_name;

            if (difftime(local_now, mktime(&local_tm_create)) >= actual_expire) {
                Status ret = FileUtils::remove_all(path_name);
                if (!ret.ok()) {
                    LOG(WARNING) << "fail to remove file or directory. path=" << path_name
                                 << ", error=" << ret.to_string();
                    res = OLAP_ERR_OS_ERROR;
                    continue;
                }
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
    } else {
        _default_rowset_type = ALPHA_ROWSET;
    }
}

void StorageEngine::start_delete_unused_rowset() {
    MutexLock lock(&_gc_mutex);
    for (auto it = _unused_rowsets.begin(); it != _unused_rowsets.end();) {
        if (it->second.use_count() != 1) {
            ++it;
        } else if (it->second->need_delete_file()) {
            VLOG(3) << "start to remove rowset:" << it->second->rowset_id()
                    << ", version:" << it->second->version().first << "-"
                    << it->second->version().second;
            OLAPStatus status = it->second->remove();
            VLOG(3) << "remove rowset:" << it->second->rowset_id()
                    << " finished. status:" << status;
            it = _unused_rowsets.erase(it);
        }
    }
}

void StorageEngine::add_unused_rowset(RowsetSharedPtr rowset) {
    if (rowset == nullptr) {
        return;
    }

    VLOG(3) << "add unused rowset, rowset id:" << rowset->rowset_id()
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
    return _tablet_manager->create_tablet(request, stores);
}

OLAPStatus StorageEngine::recover_tablet_until_specfic_version(
        const TRecoverTabletReq& recover_tablet_req) {
    TabletSharedPtr tablet = _tablet_manager->get_tablet(recover_tablet_req.tablet_id,
                                   recover_tablet_req.schema_hash);
    if (tablet == nullptr) { return OLAP_ERR_TABLE_NOT_FOUND; }
    RETURN_NOT_OK(tablet->recover_tablet_until_specfic_version(recover_tablet_req.version,
                                                        recover_tablet_req.version_hash));
    return OLAP_SUCCESS;
}

OLAPStatus StorageEngine::obtain_shard_path(
        TStorageMedium::type storage_medium, std::string* shard_path, DataDir** store) {
    LOG(INFO) << "begin to process obtain root path. storage_medium=" << storage_medium;

    if (shard_path == NULL) {
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

    stringstream root_path_stream;
    root_path_stream << stores[0]->path() << DATA_PREFIX << "/" << shard;
    *shard_path = root_path_stream.str();
    *store = stores[0];

    LOG(INFO) << "success to process obtain root path. path=" << shard_path;
    return res;
}

OLAPStatus StorageEngine::load_header(
        const string& shard_path,
        const TCloneReq& request) {
    LOG(INFO) << "begin to process load headers."
              << "tablet_id=" << request.tablet_id
              << ", schema_hash=" << request.schema_hash;
    OLAPStatus res = OLAP_SUCCESS;

    DataDir* store = nullptr;
    {
        // TODO(zc)
        try {
            auto store_path =
                boost::filesystem::path(shard_path).parent_path().parent_path().string();
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

    stringstream schema_hash_path_stream;
    schema_hash_path_stream << shard_path
                            << "/" << request.tablet_id
                            << "/" << request.schema_hash;
    // not surely, reload and restore tablet action call this api
    // reset tablet uid here

    string header_path = TabletMeta::construct_header_file_path(schema_hash_path_stream.str(), request.tablet_id);
    res = TabletMeta::reset_tablet_uid(header_path);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail reset tablet uid file path = " << header_path
                     << " res=" << res;
        return res;
    }
    res = _tablet_manager->load_tablet_from_dir(
            store,
            request.tablet_id, request.schema_hash,
            schema_hash_path_stream.str(), false);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to process load headers. res=" << res;
        return res;
    }

    LOG(INFO) << "success to process load headers.";
    return res;
}

OLAPStatus StorageEngine::execute_task(EngineTask* task) {
    // 1. add wlock to related tablets
    // 2. do prepare work
    // 3. release wlock
    {
        vector<TabletInfo> tablet_infos;
        task->get_related_tablets(&tablet_infos);
        sort(tablet_infos.begin(), tablet_infos.end());
        vector<TabletSharedPtr> related_tablets;
        for (TabletInfo& tablet_info : tablet_infos) {
            TabletSharedPtr tablet = _tablet_manager->get_tablet(
                tablet_info.tablet_id, tablet_info.schema_hash);
            if (tablet != nullptr) {
                related_tablets.push_back(tablet);
                tablet->obtain_header_wrlock();
            } else {
                LOG(WARNING) << "could not get tablet before prepare tabletid: "
                             << tablet_info.tablet_id;
            }
        }
        // add write lock to all related tablets
        OLAPStatus prepare_status = task->prepare();
        for (TabletSharedPtr& tablet : related_tablets) {
            tablet->release_header_lock();
        }
        if (prepare_status != OLAP_SUCCESS) {
            return prepare_status;
        }
    }

    // do execute work without lock
    OLAPStatus exec_status = task->execute();
    if (exec_status != OLAP_SUCCESS) {
        return exec_status;
    }

    // 1. add wlock to related tablets
    // 2. do finish work
    // 3. release wlock
    {
        vector<TabletInfo> tablet_infos;
        // related tablets may be changed after execute task, so that get them here again
        task->get_related_tablets(&tablet_infos);
        sort(tablet_infos.begin(), tablet_infos.end());
        vector<TabletSharedPtr> related_tablets;
        for (TabletInfo& tablet_info : tablet_infos) {
            TabletSharedPtr tablet = _tablet_manager->get_tablet(
                tablet_info.tablet_id, tablet_info.schema_hash);
            if (tablet != nullptr) {
                related_tablets.push_back(tablet);
                tablet->obtain_header_wrlock();
            } else {
                LOG(WARNING) << "could not get tablet before finish tabletid: "
                             << tablet_info.tablet_id;
            }
        }
        // add write lock to all related tablets
        OLAPStatus fin_status = task->finish();
        for (TabletSharedPtr& tablet : related_tablets) {
            tablet->release_header_lock();
        }
        return fin_status;
    }
}

// check whether any unused rowsets's id equal to rowset_id
bool StorageEngine::check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id) {
    MutexLock lock(&_gc_mutex);
    auto search = _unused_rowsets.find(rowset_id.to_string());
    return search != _unused_rowsets.end();
}

}  // namespace doris
