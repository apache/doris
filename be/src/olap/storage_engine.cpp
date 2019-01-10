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

#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/lru_cache.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/schema_change.h"
#include "olap/data_dir.h"
#include "olap/utils.h"
#include "olap/rowset/column_data_writer.h"
#include "util/time.h"
#include "util/doris_metrics.h"
#include "util/pretty_printer.h"

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
        return Status("store paths is empty");;
    }
    return Status::OK;
}

Status StorageEngine::open(const EngineOptions& options, StorageEngine** engine_ptr) {
    RETURN_IF_ERROR(_validate_options(options));
    std::unique_ptr<StorageEngine> engine(new StorageEngine(options));
    auto st = engine->open();
    if (st != OLAP_SUCCESS) {
        LOG(WARNING) << "engine open failed, res=" << st;
        return Status("open engine failed");
    }
    st = engine->_start_bg_worker();
    if (st != OLAP_SUCCESS) {
        LOG(WARNING) << "engine start background failed, res=" << st;
        return Status("open engine failed");
    }
    *engine_ptr = engine.release();
    return Status::OK;
}

StorageEngine::StorageEngine(const EngineOptions& options)
        : _options(options),
        _available_storage_medium_type_count(0),
        _effective_cluster_id(-1),
        _is_all_cluster_id_exist(true),
        _is_drop_tables(false),
        _index_stream_lru_cache(NULL),
        _is_report_disk_state_already(false),
        _is_report_tablet_already(false){
    if (_s_instance == nullptr) {
        _s_instance = this;
    }
}

StorageEngine::~StorageEngine() {
    clear();
}

OLAPStatus StorageEngine::_load_data_dir(DataDir* data_dir) {
    std::string data_dir_path = data_dir->path();
    LOG(INFO) <<"start to load tablets from data_dir_path:" << data_dir_path;

    bool is_header_converted = false;
    OLAPStatus res = TabletMetaManager::get_header_converted(data_dir, is_header_converted);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "get convert flag from meta failed";
        return res;
    }

    // load rowset meta from metaenv and create rowset
    // COMMITTED: add to txn manager
    // VISIBLE: add to tablet
    // if one rowset load failed, then the total data dir will not be loaded
    std::vector<std::shared_ptr<RowsetMeta>> dir_rowset_metas;
    if (is_header_converted) {
        LOG(INFO) << "begin loading rowset from meta";
        bool has_error = false;
        auto load_rowset_func = [this, data_dir, &has_error, &dir_rowset_metas](RowsetId rowset_id, const std::string& meta_str) -> bool {
            std::shared_ptr<RowsetMeta> rowset_meta(new RowsetMeta());
            bool parsed = rowset_meta->init(meta_str);
            if (!parsed) {
                LOG(WARNING) << "parse rowset meta string failed for rowset_id:" << rowset_id;
                has_error = true;
                // return false will break meta iterator
                return false;
            }
            dir_rowset_metas.push_back(rowset_meta);
            return true;
        };
        OLAPStatus s = RowsetMetaManager::traverse_rowset_metas(data_dir->get_meta(), load_rowset_func);
        if (has_error) {
            LOG(WARNING) << "errors when load rowset meta from meta env, skip this data dir:" << data_dir_path;
            return OLAP_ERR_META_ITERATOR;
        }
        LOG(INFO) << "load header from meta finished";
        if (s != OLAP_SUCCESS) {
            LOG(WARNING) << "errors when load rowset meta from meta env, skip this data dir:" << data_dir_path;
            return s;
        } else {
            return OLAP_SUCCESS;
        }
    }
    // load tablet
    // create tablet from tablet meta and add it to tablet mgr
    if (is_header_converted) {
        LOG(INFO) << "load header from meta";
        auto load_tablet_func = [this, data_dir](long tablet_id,
            long schema_hash, const std::string& value) -> bool {
            
            OLAPStatus status = TabletManager::instance()->load_tablet_from_header(data_dir, tablet_id, schema_hash, value);
            if (status != OLAP_SUCCESS) {
                LOG(WARNING) << "load tablet from header failed. status:" << status
                    << "tablet=" << tablet_id << "." << schema_hash;
            };
            return true;
        };
        OLAPStatus s = TabletMetaManager::traverse_headers(data_dir->get_meta(), load_tablet_func);
        LOG(INFO) << "load header from meta finished";
        if (s != OLAP_SUCCESS) {
            LOG(WARNING) << "there is failure when loading tablet headers, path:" << data_dir_path;
            return s;
        } else {
            return OLAP_SUCCESS;
        }
    } else {
        // ygl: could not be compatable with old doris data. User has to use previous version to parse
        // header file into meta env first.
        LOG(WARNING) << "header is not converted to tablet meta yet, could not use this Doris version. " 
                    << "[dir path =" << data_dir_path << "]";
        return OLAP_ERR_INIT_FAILED;
    }

    for (auto rowset_meta : dir_rowset_metas) {
        // TODO(ygl)
        // 1. build rowset from meta
        // 2. add committed rowset to txn
        // 3. add visible rowset to tablet
    }
}

void StorageEngine::load_data_dirs(const std::vector<DataDir*>& stores) {
    std::vector<std::thread> threads;
    for (auto store : stores) {
        threads.emplace_back([this, store] {
            auto res = _load_data_dir(store);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "io error when init load tables. res=" << res
                    << ", store=" << store->path();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

OLAPStatus StorageEngine::open() {
    // init store_map
    for (auto& path : _options.store_paths) {
        DataDir* store = new DataDir(path.path, path.capacity_bytes);
        auto st = store->init();
        if (!st.ok()) {
            LOG(WARNING) << "Store load failed, path=" << path.path;
            return OLAP_ERR_INVALID_ROOT_PATH;
        }
        _store_map.emplace(path.path, store);
    }
    _effective_cluster_id = config::cluster_id;
    auto res = check_all_root_path_cluster_id();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to check cluster info. res=" << res;
        return res;
    }

    _update_storage_medium_type_count();

    auto cache = new_lru_cache(config::file_descriptor_cache_capacity);
    if (cache == nullptr) {
        OLAP_LOG_WARNING("failed to init file descriptor LRUCache");
        TabletManager::instance()->clear();
        return OLAP_ERR_INIT_FAILED;
    }
    FileHandler::set_fd_cache(cache);

    // 初始化LRUCache
    // cache大小可通过配置文件配置
    _index_stream_lru_cache = new_lru_cache(config::index_stream_cache_capacity);
    if (_index_stream_lru_cache == NULL) {
        OLAP_LOG_WARNING("failed to init index stream LRUCache");
        TabletManager::instance()->clear();
        return OLAP_ERR_INIT_FAILED;
    }

    // 初始化CE调度器
    int32_t cumulative_compaction_num_threads = config::cumulative_compaction_num_threads;
    int32_t base_compaction_num_threads = config::base_compaction_num_threads;
    uint32_t file_system_num = get_file_system_count();
    _max_cumulative_compaction_task_per_disk = (cumulative_compaction_num_threads + file_system_num - 1) / file_system_num;
    _max_base_compaction_task_per_disk = (base_compaction_num_threads + file_system_num - 1) / file_system_num;

    auto dirs = get_stores();
    load_data_dirs(dirs);
    // 取消未完成的SchemaChange任务
    TabletManager::instance()->cancel_unfinished_schema_change();

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
    TabletManager::instance()->update_storage_medium_type_count(_available_storage_medium_type_count);
}


OLAPStatus StorageEngine::_judge_and_update_effective_cluster_id(int32_t cluster_id) {
    OLAPStatus res = OLAP_SUCCESS;

    if (cluster_id == -1 && _effective_cluster_id == -1) {
        // maybe this is a new cluster, cluster id will get from heartbeate
        return res;
    } else if (cluster_id != -1 && _effective_cluster_id == -1) {
        _effective_cluster_id = cluster_id;
    } else if (cluster_id == -1 && _effective_cluster_id != -1) {
        // _effective_cluster_id is the right effective cluster id
        return res;
    } else {
        if (cluster_id != _effective_cluster_id) {
            OLAP_LOG_WARNING("multiple cluster ids is not equal. [id1=%d id2=%d]",
                             _effective_cluster_id, cluster_id);
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

void StorageEngine::get_all_available_root_path(std::vector<std::string>* available_paths) {
    available_paths->clear();
    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        if (it.second->is_used()) {
            available_paths->push_back(it.first);
        }
    }
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

OLAPStatus StorageEngine::get_all_data_dir_info(vector<DataDirInfo>* data_dir_infos) {
    OLAPStatus res = OLAP_SUCCESS;
    data_dir_infos->clear();

    MonotonicStopWatch timer;
    timer.start();
    int tablet_counter = 0;

    // get all root path info and construct a path map.
    // path -> DataDirInfo
    std::map<std::string, DataDirInfo> path_map;
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& it : _store_map) {
            std::string path = it.first;
            path_map.emplace(path, it.second->get_dir_info());
            // if this path is not used, init it's info
            if (!path_map[path].is_used) {
                path_map[path].capacity = 1;
                path_map[path].data_used_capacity = 0;
                path_map[path].available = 0;
                path_map[path].storage_medium = TStorageMedium::HDD;
            } else {
                path_map[path].storage_medium = it.second->storage_medium();
            }
        }
    }

    // for each tablet, get it's data size, and accumulate the path 'data_used_capacity'
    // which the tablet belongs to.
    TabletManager::instance()->update_root_path_info(&path_map, &tablet_counter);

    // add path info to data_dir_infos
    for (auto& entry : path_map) {
        data_dir_infos->emplace_back(entry.second);
    }

    // get available capacity of each path
    for (auto& info: *data_dir_infos) {
        if (info.is_used) {
            _get_path_available_capacity(info.path,  &info.available);
        }
    }
    timer.stop();
    LOG(INFO) << "get root path info cost: " << timer.elapsed_time() / 1000000
            << " ms. tablet counter: " << tablet_counter;

    return res;
}

void StorageEngine::start_disk_stat_monitor() {
    for (auto& it : _store_map) {
        it.second->health_check();
    }
    _update_storage_medium_type_count();
    _delete_tables_on_unused_root_path();
    
    // if drop tables
    // notify disk_state_worker_thread and tablet_worker_thread until they received
    if (_is_drop_tables) {
        report_notify(true);

        bool is_report_disk_state_expected = true;
        bool is_report_tablet_expected = true;
        bool is_report_disk_state_exchanged =
                _is_report_disk_state_already.compare_exchange_strong(is_report_disk_state_expected, false);
        bool is_report_tablet_exchanged =
                _is_report_tablet_already.compare_exchange_strong(is_report_tablet_expected, false);
        if (is_report_disk_state_exchanged && is_report_tablet_exchanged) {
            _is_drop_tables = false;
        }
    }
}

bool StorageEngine::_used_disk_not_enough(uint32_t unused_num, uint32_t total_num) {
    return ((total_num == 0) || (unused_num * 100 / total_num > _min_percentage_of_error_disk));
}

OLAPStatus StorageEngine::check_all_root_path_cluster_id() {
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
    res = _judge_and_update_effective_cluster_id(cluster_id);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to judge and update effective cluster id. [res=%d]", res);
        return res;
    }

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
    return Status::OK;
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
    std::random_device rd;
    srand(rd());
    std::random_shuffle(stores.begin(), stores.end());
    return stores;
}

DataDir* StorageEngine::get_store(const std::string& path) {
    std::lock_guard<std::mutex> l(_store_lock);
    auto it = _store_map.find(path);
    if (it == std::end(_store_map)) {
        return nullptr;
    }
    return it->second;
}

void StorageEngine::_delete_tables_on_unused_root_path() {
    vector<TabletInfo> tablet_info_vec;
    uint32_t unused_root_path_num = 0;
    uint32_t total_root_path_num = 0;

    std::lock_guard<std::mutex> l(_store_lock);

    for (auto& it : _store_map) {
        total_root_path_num++;
        if (it.second->is_used()) {
            continue;
        }
        it.second->clear_tablets(&tablet_info_vec);
    }

    if (_used_disk_not_enough(unused_root_path_num, total_root_path_num)) {
        LOG(FATAL) << "engine stop running, because more than " << _min_percentage_of_error_disk
                   << " disks error. total_disks=" << total_root_path_num
                   << ", error_disks=" << unused_root_path_num;
        exit(0);
    }

    if (!tablet_info_vec.empty()) {
        _is_drop_tables = true;
    }
    
    TabletManager::instance()->drop_tablets_on_error_root_path(tablet_info_vec);
}

OLAPStatus StorageEngine::_get_path_available_capacity(
        const string& root_path,
        int64_t* disk_available) {
    OLAPStatus res = OLAP_SUCCESS;

    try {
        boost::filesystem::path path_name(root_path);
        boost::filesystem::space_info path_info = boost::filesystem::space(path_name);
        *disk_available = path_info.available;
    } catch (boost::filesystem::filesystem_error& e) {
        LOG(WARNING) << "get space info failed. path: " << root_path << " erro:" << e.what();
        return OLAP_ERR_STL_ERROR;
    }

    return res;
}

OLAPStatus StorageEngine::clear() {
    // 删除lru中所有内容,其实进程退出这么做本身意义不大,但对单测和更容易发现问题还是有很大意义的
    delete FileHandler::get_fd_cache();
    FileHandler::set_fd_cache(nullptr);
    SAFE_DELETE(_index_stream_lru_cache);

    return OLAP_SUCCESS;
}

void StorageEngine::delete_transaction(
    TPartitionId partition_id, TTransactionId transaction_id,
    TTabletId tablet_id, SchemaHash schema_hash, bool delete_from_tablet) {

    // call txn manager to delete txn from memory
    OLAPStatus res = TxnManager::instance()->delete_txn(partition_id, transaction_id, 
        tablet_id, schema_hash);
    // delete transaction from tablet
    if (res == OLAP_SUCCESS && delete_from_tablet) {
        TabletSharedPtr tablet = TabletManager::instance()->get_tablet(tablet_id, schema_hash);
        if (tablet.get() != nullptr) {
            tablet->delete_pending_data(transaction_id);
        }
    }
}

OLAPStatus StorageEngine::publish_version(const TPublishVersionRequest& publish_version_req,
                                 vector<TTabletId>* error_tablet_ids) {
    LOG(INFO) << "begin to process publish version. transaction_id="
        << publish_version_req.transaction_id;

    int64_t transaction_id = publish_version_req.transaction_id;
    OLAPStatus res = OLAP_SUCCESS;

    // each partition
    for (const TPartitionVersionInfo& partitionVersionInfo
         : publish_version_req.partition_version_infos) {

        int64_t partition_id = partitionVersionInfo.partition_id;
        vector<TabletInfo> tablet_infos;
        vector<RowsetSharedPtr> rowsets;
        TxnManager::instance()->get_txn_related_tablets(transaction_id, partition_id, &tablet_infos, &rowsets);

        Version version(partitionVersionInfo.version, partitionVersionInfo.version);
        VersionHash version_hash = partitionVersionInfo.version_hash;

        // each tablet
        for (auto& tablet_info : tablet_infos) {
            VLOG(3) << "begin to publish version on tablet. "
                    << "tablet_id=" << tablet_info.tablet_id
                    << ", schema_hash=" << tablet_info.schema_hash
                    << ", version=" << version.first
                    << ", version_hash=" << version_hash
                    << ", transaction_id=" << transaction_id;
            TabletSharedPtr tablet = TabletManager::instance()->get_tablet(tablet_info.tablet_id, tablet_info.schema_hash);

            if (tablet.get() == NULL) {
                OLAP_LOG_WARNING("can't get tablet when publish version. [tablet_id=%ld schema_hash=%d]",
                                 tablet_info.tablet_id, tablet_info.schema_hash);
                error_tablet_ids->push_back(tablet_info.tablet_id);
                res = OLAP_ERR_PUSH_TABLE_NOT_EXIST;
                continue;
            }

            // get rowsets from txn manager according to tablet and txn id
            // TODO(ygl) just add rowset to tablet
            // publish version
            OLAPStatus publish_status = tablet->publish_version(
                transaction_id, version, version_hash);

            // if data existed, delete transaction from engine and tablet
            if (publish_status == OLAP_ERR_PUSH_VERSION_ALREADY_EXIST) {
                OLAP_LOG_WARNING("can't publish version on tablet since data existed. "
                                 "[tablet=%s transaction_id=%ld version=%d]",
                                 tablet->full_name().c_str(), transaction_id, version.first);
                delete_transaction(partition_id, transaction_id,
                                   tablet->tablet_id(), tablet->schema_hash());

            // if publish successfully, delete transaction from engine
            } else if (publish_status == OLAP_SUCCESS) {
                LOG(INFO) << "publish version successfully on tablet. tablet=" << tablet->full_name()
                          << ", transaction_id=" << transaction_id << ", version=" << version.first;
                TxnManager::instance()->delete_txn(partition_id, transaction_id, 
                    tablet_info.tablet_id, tablet_info.schema_hash);
            } else {
                OLAP_LOG_WARNING("fail to publish version on tablet. "
                                 "[tablet=%s transaction_id=%ld version=%d res=%d]",
                                 tablet->full_name().c_str(), transaction_id,
                                 version.first, publish_status);
                error_tablet_ids->push_back(tablet->tablet_id());
                res = publish_status;
            }
        }
    }

    LOG(INFO) << "finish to publish version on transaction."
              << "transaction_id=" << transaction_id
              << ", error_tablet_size=" << error_tablet_ids->size();
    return res;
}

void StorageEngine::clear_transaction_task(const TTransactionId transaction_id,
                                        const vector<TPartitionId> partition_ids) {
    LOG(INFO) << "begin to clear transaction task. transaction_id=" <<  transaction_id;

    for (const TPartitionId& partition_id : partition_ids) {
        vector<TabletInfo> tablet_infos;
        TxnManager::instance()->get_txn_related_tablets(transaction_id, partition_id, &tablet_infos, NULL);

        // each tablet
        for (auto& tablet_info : tablet_infos) {
            delete_transaction(partition_id, transaction_id,
                                tablet_info.tablet_id, tablet_info.schema_hash);
        }
    }
    LOG(INFO) << "finish to clear transaction task. transaction_id=" << transaction_id;
}

TabletSharedPtr StorageEngine::create_tablet(
        const TCreateTabletReq& request, const string* ref_root_path, 
        const bool is_schema_change_tablet, const TabletSharedPtr ref_tablet) {

    // Get all available stores, use ref_root_path if the caller specified
    std::vector<DataDir*> stores;
    if (ref_root_path == nullptr) {
        stores = get_stores_for_create_tablet(request.storage_medium);
        if (stores.empty()) {
            LOG(WARNING) << "there is no available disk that can be used to create tablet.";
            return nullptr;
        }
    } else {
        stores.push_back(ref_tablet->data_dir());
    }

    return TabletManager::instance()->create_tablet(request, ref_root_path, 
        is_schema_change_tablet, ref_tablet, stores);
}

void StorageEngine::start_clean_fd_cache() {
    VLOG(10) << "start clean file descritpor cache";
    FileHandler::get_fd_cache()->prune();
    VLOG(10) << "end clean file descritpor cache";
}

void StorageEngine::perform_cumulative_compaction() {
    TabletSharedPtr best_tablet = TabletManager::instance()->find_best_tablet_to_compaction(CompactionType::CUMULATIVE_COMPACTION);
    if (best_tablet == nullptr) { return; }

    CumulativeCompaction cumulative_compaction;
    OLAPStatus res = cumulative_compaction.init(best_tablet);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to init cumulative compaction."
                     << "tablet=" << best_tablet->full_name();
    }

    res = cumulative_compaction.run();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to do cumulative compaction."
                     << "tablet=" << best_tablet->full_name();
    }
}

void StorageEngine::perform_base_compaction() {
    TabletSharedPtr best_tablet = TabletManager::instance()->find_best_tablet_to_compaction(CompactionType::BASE_COMPACTION);
    if (best_tablet == nullptr) { return; }

    BaseCompaction base_compaction;
    OLAPStatus res = base_compaction.init(best_tablet);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to init base compaction."
                     << "tablet=" << best_tablet->full_name();
        return;
    }

    res = base_compaction.run();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to init base compaction."
                     << "tablet=" << best_tablet->full_name();
    }
}

void StorageEngine::get_cache_status(rapidjson::Document* document) const {
    return _index_stream_lru_cache->get_cache_status(document);
}

OLAPStatus StorageEngine::start_trash_sweep(double* usage) {
    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "start trash and snapshot sweep.";

    const uint32_t snapshot_expire = config::snapshot_expire_time_sec;
    const uint32_t trash_expire = config::trash_file_expire_time_sec;
    const double guard_space = config::disk_capacity_insufficient_percentage / 100.0;
    std::vector<DataDirInfo> data_dir_infos;
    res = get_all_data_dir_info(&data_dir_infos);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to get root path stat info when sweep trash.");
        return res;
    }

    time_t now = time(NULL); //获取UTC时间
    tm local_tm_now;
    if (localtime_r(&now, &local_tm_now) == NULL) {
        OLAP_LOG_WARNING("fail to localtime_r time. [time=%lu]", now);
        return OLAP_ERR_OS_ERROR;
    }
    const time_t local_now = mktime(&local_tm_now); //得到当地日历时间

    for (DataDirInfo& info : data_dir_infos) {
        if (!info.is_used) {
            continue;
        }

        double curr_usage = info.data_used_capacity
                / (double) info.capacity;
        *usage = *usage > curr_usage ? *usage : curr_usage;

        OLAPStatus curr_res = OLAP_SUCCESS;
        string snapshot_path = info.path + SNAPSHOT_PREFIX;
        curr_res = _do_sweep(snapshot_path, local_now, snapshot_expire);
        if (curr_res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to sweep snapshot. [path=%s, err_code=%d]",
                    snapshot_path.c_str(), curr_res);
            res = curr_res;
        }

        string trash_path = info.path + TRASH_PREFIX;
        curr_res = _do_sweep(trash_path, local_now,
                curr_usage > guard_space ? 0 : trash_expire);
        if (curr_res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to sweep trash. [path=%s, err_code=%d]",
                    trash_path.c_str(), curr_res);
            res = curr_res;
        }
    }

    // clear expire incremental segment_group
    TabletManager::instance()->start_trash_sweep();

    return res;
}

OLAPStatus StorageEngine::_do_sweep(
        const string& scan_root, const time_t& local_now, const uint32_t expire) {
    OLAPStatus res = OLAP_SUCCESS;
    if (!check_dir_existed(scan_root)) {
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
            if (difftime(local_now, mktime(&local_tm_create)) >= expire) {
                if (remove_all_dir(path_name) != OLAP_SUCCESS) {
                    OLAP_LOG_WARNING("fail to remove file or directory. [path=%s]",
                            path_name.c_str());
                    res = OLAP_ERR_OS_ERROR;
                    continue;
                }
            }
        }
    } catch (...) {
        OLAP_LOG_WARNING("Exception occur when scan directory. [path=%s]",
                scan_root.c_str());
        res = OLAP_ERR_IO_ERROR;
    }

    return res;
}

void StorageEngine::start_delete_unused_index() {
    _gc_mutex.lock();

    for (auto it = _gc_files.begin(); it != _gc_files.end();) {
        if (it->first->is_in_use()) {
            ++it;
        } else {
            delete it->first;
            vector<string> files = it->second;
            remove_files(files);
            it = _gc_files.erase(it);
        }
    }

    _gc_mutex.unlock();
}

void StorageEngine::add_unused_index(SegmentGroup* segment_group) {
    _gc_mutex.lock();

    auto it = _gc_files.find(segment_group);
    if (it == _gc_files.end()) {
        vector<string> files;
        for (size_t seg_id = 0; seg_id < segment_group->num_segments(); ++seg_id) {
            string index_file = segment_group->construct_index_file_path(seg_id);
            files.push_back(index_file);

            string data_file = segment_group->construct_data_file_path(seg_id);
            files.push_back(data_file);
        }
        _gc_files[segment_group] = files;
    }

    _gc_mutex.unlock();
}

void StorageEngine::start_delete_unused_rowset() {
    _gc_mutex.lock();

    auto it = _unused_rowsets.begin();
    for (; it != _unused_rowsets.end();) { 
        if (it->second->in_use()) {
            ++it;
        } else {
            it->second->delete_files();
            _unused_rowsets.erase(it);
        }
    }

    _gc_mutex.unlock();
}

void StorageEngine::add_unused_rowset(RowsetSharedPtr rowset) {
    _gc_mutex.lock();
    auto it = _unused_rowsets.find(rowset->rowset_id());
    if (it == _unused_rowsets.end()) {
        _unused_rowsets[rowset->rowset_id()] = rowset;
    }
    _gc_mutex.unlock();
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
    return TabletManager::instance()->create_tablet(request, stores);
}

OLAPStatus StorageEngine::recover_tablet_until_specfic_version(
        const TRecoverTabletReq& recover_tablet_req) {
    TabletSharedPtr tablet = TabletManager::instance()->get_tablet(recover_tablet_req.tablet_id,
                                   recover_tablet_req.schema_hash);
    if (tablet == nullptr) { return OLAP_ERR_TABLE_NOT_FOUND; }
    RETURN_NOT_OK(tablet->recover_tablet_until_specfic_version(recover_tablet_req.version,
                                                        recover_tablet_req.version_hash));
    return OLAP_SUCCESS;
}

OLAPStatus StorageEngine::obtain_shard_path(
        TStorageMedium::type storage_medium, std::string* shard_path, DataDir** store) {
    LOG(INFO) << "begin to process obtain root path. storage_medium=" << storage_medium;
    OLAPStatus res = OLAP_SUCCESS;

    if (shard_path == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    auto stores = get_stores_for_create_tablet(storage_medium);
    if (stores.empty()) {
        OLAP_LOG_WARNING("no available disk can be used to create tablet.");
        return OLAP_ERR_NO_AVAILABLE_ROOT_PATH;
    }

    uint64_t shard = 0;
    res = stores[0]->get_shard(&shard);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get root path shard. [res=%d]", res);
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
    res =  TabletManager::instance()->load_one_tablet(
            store,
            request.tablet_id, request.schema_hash,
            schema_hash_path_stream.str(), false);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process load headers. [res=%d]", res);
        return res;
    }

    LOG(INFO) << "success to process load headers.";
    return res;
}

OLAPStatus StorageEngine::load_header(
        DataDir* store,
        const string& shard_path,
        TTabletId tablet_id,
        TSchemaHash schema_hash) {
    LOG(INFO) << "begin to process load headers. tablet_id=" << tablet_id
              << "schema_hash=" << schema_hash;
    OLAPStatus res = OLAP_SUCCESS;

    stringstream schema_hash_path_stream;
    schema_hash_path_stream << shard_path
                            << "/" << tablet_id
                            << "/" << schema_hash;
    res =  TabletManager::instance()->load_one_tablet(
            store,
            tablet_id, schema_hash,
            schema_hash_path_stream.str(), 
            false);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process load headers. [res=%d]", res);
        return res;
    }

    LOG(INFO) << "success to process load headers.";
    return res;
}

}  // namespace doris
