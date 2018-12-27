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

#include "agent/file_downloader.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/lru_cache.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
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
const std::string HTTP_REQUEST_PREFIX = "/api/_tablet/_download?";
const std::string HTTP_REQUEST_TOKEN_PARAM = "token=";
const std::string HTTP_REQUEST_FILE_PARAM = "&file=";

const uint32_t DOWNLOAD_FILE_MAX_RETRY = 3;
const uint32_t LIST_REMOTE_FILE_TIMEOUT = 15;

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
        _snapshot_base_id(0),
        _is_report_disk_state_already(false),
        _is_report_tablet_already(false),
        _txn_mgr(),
        _tablet_mgr() {
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
    if (is_header_converted) {
        LOG(INFO) << "load header from meta";
        auto load_tablet_func = [this, data_dir](long tablet_id,
            long schema_hash, const std::string& value) -> bool {
            OLAPStatus status = this->_tablet_mgr.load_tablet_from_header(data_dir, tablet_id, schema_hash, value);
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
        _tablet_mgr.clear();
        return OLAP_ERR_INIT_FAILED;
    }
    FileHandler::set_fd_cache(cache);

    // 初始化LRUCache
    // cache大小可通过配置文件配置
    _index_stream_lru_cache = new_lru_cache(config::index_stream_cache_capacity);
    if (_index_stream_lru_cache == NULL) {
        OLAP_LOG_WARNING("failed to init index stream LRUCache");
        _tablet_mgr.clear();
        return OLAP_ERR_INIT_FAILED;
    }

    // 初始化CE调度器
    int32_t cumulative_compaction_num_threads = config::cumulative_compaction_num_threads;
    int32_t base_compaction_num_threads = config::base_compaction_num_threads;
    uint32_t file_system_num = get_file_system_count();
    _max_cumulative_compaction_task_per_disk = (cumulative_compaction_num_threads + file_system_num - 1) / file_system_num;
    _max_base_compaction_task_per_disk = (base_compaction_num_threads + file_system_num - 1) / file_system_num;

    auto stores = get_stores();
    load_data_dirs(stores);
    // 取消未完成的SchemaChange任务
    _tablet_mgr.cancel_unfinished_schema_change();

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
    _tablet_mgr.update_storage_medium_type_count(_available_storage_medium_type_count);
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
            }
        }
    }

    // for each tablet, get it's data size, and accumulate the path 'data_used_capacity'
    // which the tablet belongs to.
    _tablet_mgr.update_root_path_info(&path_map, &tablet_counter);

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
    
    _tablet_mgr.drop_tablets_on_error_root_path(tablet_info_vec);
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

TabletSharedPtr StorageEngine::get_tablet(TTabletId tablet_id, SchemaHash schema_hash, bool load_tablet) {
    
    return _tablet_mgr.get_tablet(tablet_id, schema_hash, load_tablet);
}

OLAPStatus StorageEngine::get_tables_by_id(
        TTabletId tablet_id,
        list<TabletSharedPtr>* tablet_list) {
    return _tablet_mgr.get_tablets_by_id(tablet_id, tablet_list);
}

bool StorageEngine::check_tablet_id_exist(TTabletId tablet_id) {

    return _tablet_mgr.check_tablet_id_exist(tablet_id);
}

OLAPStatus StorageEngine::add_transaction(
    TPartitionId partition_id, TTransactionId transaction_id,
    TTabletId tablet_id, SchemaHash schema_hash, const PUniqueId& load_id) {
    
    OLAPStatus status = _txn_mgr.add_txn(partition_id, transaction_id, 
        tablet_id, schema_hash, load_id);
    return status;
}

void StorageEngine::delete_transaction(
    TPartitionId partition_id, TTransactionId transaction_id,
    TTabletId tablet_id, SchemaHash schema_hash, bool delete_from_tablet) {

    // call txn manager to delete txn from memory
    OLAPStatus res = _txn_mgr.delete_txn(partition_id, transaction_id, 
        tablet_id, schema_hash);
    // delete transaction from tablet
    if (res == OLAP_SUCCESS && delete_from_tablet) {
        TabletSharedPtr tablet = get_tablet(tablet_id, schema_hash);
        if (tablet.get() != nullptr) {
            tablet->delete_pending_data(transaction_id);
        }
    }
}

void StorageEngine::get_transactions_by_tablet(TabletSharedPtr tablet, int64_t* partition_id,
                                            set<int64_t>* transaction_ids) {
    _txn_mgr.get_tablet_related_txns(tablet, partition_id, transaction_ids);
}

bool StorageEngine::has_transaction(TPartitionId partition_id, TTransactionId transaction_id,
                                 TTabletId tablet_id, SchemaHash schema_hash) {
    bool found = _txn_mgr.has_txn(partition_id, transaction_id, tablet_id, schema_hash);
    return found;
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
        _txn_mgr.get_txn_related_tablets(transaction_id, partition_id, &tablet_infos);

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
            TabletSharedPtr tablet = get_tablet(tablet_info.tablet_id, tablet_info.schema_hash);

            if (tablet.get() == NULL) {
                OLAP_LOG_WARNING("can't get tablet when publish version. [tablet_id=%ld schema_hash=%d]",
                                 tablet_info.tablet_id, tablet_info.schema_hash);
                error_tablet_ids->push_back(tablet_info.tablet_id);
                res = OLAP_ERR_PUSH_TABLE_NOT_EXIST;
                continue;
            }


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
                _txn_mgr.delete_txn(partition_id, transaction_id, 
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
        _txn_mgr.get_txn_related_tablets(transaction_id, partition_id, &tablet_infos);

        // each tablet
        for (auto& tablet_info : tablet_infos) {
            delete_transaction(partition_id, transaction_id,
                                tablet_info.tablet_id, tablet_info.schema_hash);
        }
    }
    LOG(INFO) << "finish to clear transaction task. transaction_id=" << transaction_id;
}

OLAPStatus StorageEngine::clone_incremental_data(TabletSharedPtr tablet, TabletMeta& clone_header,
                                              int64_t committed_version) {
    LOG(INFO) << "begin to incremental clone. tablet=" << tablet->full_name()
              << ", committed_version=" << committed_version;

    // calculate missing version again
    vector<Version> missing_versions;
    tablet->get_missing_versions_with_header_locked(committed_version, &missing_versions);

    // add least complete version
    // prevent lastest version not replaced (if need to rewrite) when restart
    const PDelta* least_complete_version = tablet->least_complete_version(missing_versions);

    vector<Version> versions_to_delete;
    vector<const PDelta*> versions_to_clone;

    // it's not a merged version in principle
    if (least_complete_version != NULL &&
        least_complete_version->start_version() == least_complete_version->end_version()) {

        Version version(least_complete_version->start_version(), least_complete_version->end_version());
        const PDelta* clone_src_version = clone_header.get_incremental_version(version);

        // if least complete version not found in clone src, return error
        if (clone_src_version == nullptr) {
            OLAP_LOG_WARNING("failed to find least complete version in clone header. "
                    "[clone_header_file=%s least_complete_version=%d-%d]",
                    clone_header.file_name().c_str(),
                    least_complete_version->start_version(), least_complete_version->end_version());
            return OLAP_ERR_VERSION_NOT_EXIST;

        // if least complete version_hash in clone src is different, clone it
        } else if (clone_src_version->version_hash() != least_complete_version->version_hash()) {
            versions_to_clone.push_back(clone_src_version);
            versions_to_delete.push_back(Version(
                least_complete_version->start_version(),
                least_complete_version->end_version()));

            VLOG(3) << "least complete version_hash in clone src is different, replace it. "
                    << "tablet=" << tablet->full_name()
                    << ", least_complete_version=" << least_complete_version->start_version()
                    << "-" << least_complete_version->end_version()
                    << ", local_hash=" << least_complete_version->version_hash()
                    << ", clone_hash=" << clone_src_version->version_hash();
        }
    }

    VLOG(3) << "get missing versions again when incremental clone. "
            << "tablet=" << tablet->full_name() 
            << ", committed_version=" << committed_version
            << ", missing_versions_size=" << missing_versions.size();

    // check missing versions exist in clone src
    for (Version version : missing_versions) {
        const PDelta* clone_src_version = clone_header.get_incremental_version(version);
        if (clone_src_version == NULL) {
           LOG(WARNING) << "missing version not found in clone src."
                        << "clone_header_file=" << clone_header.file_name()
                        << ", missing_version=" << version.first << "-" << version.second;
            return OLAP_ERR_VERSION_NOT_EXIST;
        }

        versions_to_clone.push_back(clone_src_version);
    }

    // clone_data to tablet
    OLAPStatus clone_res = tablet->clone_data(clone_header, versions_to_clone, versions_to_delete);
    LOG(INFO) << "finish to incremental clone. [tablet=" << tablet->full_name() << " res=" << clone_res << "]";
    return clone_res;
}

OLAPStatus StorageEngine::clone_full_data(TabletSharedPtr tablet, TabletMeta& clone_header) {
    Version clone_latest_version = clone_header.get_latest_version();
    LOG(INFO) << "begin to full clone. tablet=" << tablet->full_name() << ","
        << "clone_latest_version=" << clone_latest_version.first << "-" << clone_latest_version.second;
    vector<Version> versions_to_delete;

    // check local versions
    for (int i = 0; i < tablet->file_delta_size(); i++) {
        Version local_version(tablet->get_delta(i)->start_version(),
                              tablet->get_delta(i)->end_version());
        VersionHash local_version_hash = tablet->get_delta(i)->version_hash();
        LOG(INFO) << "check local delta when full clone."
            << "tablet=" << tablet->full_name()
            << ", local_version=" << local_version.first << "-" << local_version.second;

        // if local version cross src latest, clone failed
        if (local_version.first <= clone_latest_version.second
            && local_version.second > clone_latest_version.second) {
            LOG(WARNING) << "stop to full clone, version cross src latest."
                    << "tablet=" << tablet->full_name()
                    << ", local_version=" << local_version.first << "-" << local_version.second;
            return OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR;

        } else if (local_version.second <= clone_latest_version.second) {
            // if local version smaller than src, check if existed in src, will not clone it
            bool existed_in_src = false;

            // if delta labeled with local_version is same with the specified version in clone header,
            // there is no necessity to clone it.
            for (int j = 0; j < clone_header.file_delta_size(); ++j) {
                if (clone_header.get_delta(j)->start_version() == local_version.first
                    && clone_header.get_delta(j)->end_version() == local_version.second
                    && clone_header.get_delta(j)->version_hash() == local_version_hash) {
                    existed_in_src = true;
                    LOG(INFO) << "Delta has already existed in local header, no need to clone."
                        << "tablet=" << tablet->full_name()
                        << ", version='" << local_version.first<< "-" << local_version.second
                        << ", version_hash=" << local_version_hash;

                    OLAPStatus delete_res = clone_header.delete_version(local_version);
                    if (delete_res != OLAP_SUCCESS) {
                        LOG(WARNING) << "failed to delete existed version from clone src when full clone. "
                                  << "clone_header_file=" << clone_header.file_name()
                                  << "version=" << local_version.first << "-" << local_version.second;
                        return delete_res;
                    }
                    break;
                }
            }

            // Delta labeled in local_version is not existed in clone header,
            // some overlapping delta will be cloned to replace it.
            // And also, the specified delta should deleted from local header.
            if (!existed_in_src) {
                versions_to_delete.push_back(local_version);
                LOG(INFO) << "Delete delta not included by the clone header, should delete it from local header."
                          << "tablet=" << tablet->full_name() << ","
                          << ", version=" << local_version.first<< "-" << local_version.second
                          << ", version_hash=" << local_version_hash;
            }
        }
    }
    vector<const PDelta*> clone_deltas;
    for (int i = 0; i < clone_header.file_delta_size(); ++i) {
        clone_deltas.push_back(clone_header.get_delta(i));
        LOG(INFO) << "Delta to clone."
            << "tablet=" << tablet->full_name() << ","
            << ", version=" << clone_header.get_delta(i)->start_version() << "-"
                << clone_header.get_delta(i)->end_version()
            << ", version_hash=" << clone_header.get_delta(i)->version_hash();
    }

    // clone_data to tablet
    OLAPStatus clone_res = tablet->clone_data(clone_header, clone_deltas, versions_to_delete);
    LOG(INFO) << "finish to full clone. [tablet=" << tablet->full_name() << ", res=" << clone_res << "]";
    return clone_res;
}

// Drop tablet specified, the main logical is as follows:
// 1. tablet not in schema change:
//      drop specified tablet directly;
// 2. tablet in schema change:
//      a. schema change not finished && dropped tablet is base :
//          base tablet cannot be dropped;
//      b. other cases:
//          drop specified tablet and clear schema change info.
OLAPStatus StorageEngine::drop_tablet(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    return _tablet_mgr.drop_tablet(tablet_id, schema_hash, keep_files);
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
        stores.push_back(ref_tablet->store());
    }

    return _tablet_mgr.create_tablet(request, ref_root_path, 
        is_schema_change_tablet, ref_tablet, stores);
}

bool StorageEngine::try_schema_change_lock(TTabletId tablet_id) {

    return _tablet_mgr.try_schema_change_lock(tablet_id);
}

void StorageEngine::release_schema_change_lock(TTabletId tablet_id) {
    
    _tablet_mgr.release_schema_change_lock(tablet_id);
}

OLAPStatus StorageEngine::report_tablet_info(TTabletInfo* tablet_info) {
    
    return _tablet_mgr.report_tablet_info(tablet_info);
}

OLAPStatus StorageEngine::report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info) {
    
    return _tablet_mgr.report_all_tablets_info(tablets_info);
}

void StorageEngine::get_tablet_stat(TTabletStatResult& result) {
    _tablet_mgr.get_tablet_stat(result);
}

void StorageEngine::start_clean_fd_cache() {
    VLOG(10) << "start clean file descritpor cache";
    FileHandler::get_fd_cache()->prune();
    VLOG(10) << "end clean file descritpor cache";
}

void StorageEngine::perform_cumulative_compaction() {
    TabletSharedPtr best_tablet = _tablet_mgr.find_best_tablet_to_compaction(CompactionType::CUMULATIVE_COMPACTION);
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
    TabletSharedPtr best_tablet = _tablet_mgr.find_best_tablet_to_compaction(CompactionType::BASE_COMPACTION);
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
    _tablet_mgr.start_trash_sweep();

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
        int32_t segment_group_id = segment_group->segment_group_id();
        for (size_t seg_id = 0; seg_id < segment_group->num_segments(); ++seg_id) {
            string index_file = segment_group->construct_index_file_path(segment_group_id, seg_id);
            files.push_back(index_file);

            string data_file = segment_group->construct_data_file_path(segment_group_id, seg_id);
            files.push_back(data_file);
        }
        _gc_files[segment_group] = files;
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
    return _tablet_mgr.create_tablet(request, stores);
}

OLAPStatus StorageEngine::schema_change(const TAlterTabletReq& request) {
    LOG(INFO) << "begin to schema change. old_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id;

    DorisMetrics::schema_change_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    SchemaChangeHandler handler;
    res = handler.process_alter_tablet(ALTER_TABLET_SCHEMA_CHANGE, request);

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to do schema change. "
                         "[base_tablet=%ld new_tablet=%ld] [res=%d]",
                         request.base_tablet_id, request.new_tablet_req.tablet_id, res);
        DorisMetrics::schema_change_requests_failed.increment(1);
        return res;
    }

    LOG(INFO) << "success to submit schema change."
              << "old_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id;
    return res;
}

OLAPStatus StorageEngine::create_rollup_tablet(const TAlterTabletReq& request) {
    
    return _tablet_mgr.create_rollup_tablet(request);
}

AlterTableStatus StorageEngine::show_alter_tablet_status(
        TTabletId tablet_id,
        TSchemaHash schema_hash) {

    return _tablet_mgr.show_alter_tablet_status(tablet_id, schema_hash);
}

OLAPStatus StorageEngine::compute_checksum(
        TTabletId tablet_id,
        TSchemaHash schema_hash,
        TVersion version,
        TVersionHash version_hash,
        uint32_t* checksum) {
    LOG(INFO) << "begin to process compute checksum."
              << "tablet_id=" << tablet_id
              << ", schema_hash=" << schema_hash
              << ", version=" << version;
    OLAPStatus res = OLAP_SUCCESS;

    if (checksum == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    TabletSharedPtr tablet = get_tablet(tablet_id, schema_hash);
    if (NULL == tablet.get()) {
        OLAP_LOG_WARNING("can't find tablet. [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    {
        ReadLock rdlock(tablet->get_header_lock_ptr());
        const PDelta* message = tablet->lastest_version();
        if (message == NULL) {
            LOG(FATAL) << "fail to get latest version. tablet_id=" << tablet_id;
            return OLAP_ERR_VERSION_NOT_EXIST;
        }

        if (message->end_version() == version
                && message->version_hash() != version_hash) {
            OLAP_LOG_WARNING("fail to check latest version hash. "
                             "[res=%d tablet_id=%ld version_hash=%ld request_version_hash=%ld]",
                             res, tablet_id, message->version_hash(), version_hash);
            return OLAP_ERR_CE_CMD_PARAMS_ERROR;
        }
    }

    Reader reader;
    ReaderParams reader_params;
    reader_params.tablet = tablet;
    reader_params.reader_type = READER_CHECKSUM;
    reader_params.version = Version(0, version);

    // ignore float and double type considering to precision lose
    for (size_t i = 0; i < tablet->tablet_schema().size(); ++i) {
        FieldType type = tablet->get_field_type_by_index(i);
        if (type == OLAP_FIELD_TYPE_FLOAT || type == OLAP_FIELD_TYPE_DOUBLE) {
            continue;
        }

        reader_params.return_columns.push_back(i);
    }

    res = reader.init(reader_params);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("initiate reader fail. [res=%d]", res);
        return res;
    }

    RowCursor row;
    res = row.init(tablet->tablet_schema(), reader_params.return_columns);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to init row cursor. [res=%d]", res);
        return res;
    }
    row.allocate_memory_for_string_type(tablet->tablet_schema());

    bool eof = false;
    uint32_t row_checksum = 0;
    while (true) {
        OLAPStatus res = reader.next_row_with_aggregation(&row, &eof);
        if (res == OLAP_SUCCESS && eof) {
            VLOG(3) << "reader reads to the end.";
            break;
        } else if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to read in reader. [res=%d]", res);
            return res;
        }

        row_checksum = row.hash_code(row_checksum);
    }

    LOG(INFO) << "success to finish compute checksum. checksum=" << row_checksum;
    *checksum = row_checksum;
    return OLAP_SUCCESS;
}

OLAPStatus StorageEngine::cancel_delete(const TCancelDeleteDataReq& request) {
    LOG(INFO) << "begin to process cancel delete."
              << "tablet=" << request.tablet_id
              << ", version=" << request.version;

    DorisMetrics::cancel_delete_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    // 1. Get all tablets with same tablet_id
    list<TabletSharedPtr> table_list;
    res = get_tables_by_id(request.tablet_id, &table_list);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("can't find tablet. [tablet=%ld]", request.tablet_id);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 2. Remove delete conditions from each tablet.
    DeleteConditionHandler cond_handler;
    for (TabletSharedPtr temp_tablet : table_list) {
        temp_tablet->obtain_header_wrlock();
        res = cond_handler.delete_cond(temp_tablet, request.version, false);
        if (res != OLAP_SUCCESS) {
            temp_tablet->release_header_lock();
            OLAP_LOG_WARNING("cancel delete failed. [res=%d tablet=%s]",
                             res, temp_tablet->full_name().c_str());
            break;
        }

        res = temp_tablet->save_tablet_meta();
        if (res != OLAP_SUCCESS) {
            temp_tablet->release_header_lock();
            OLAP_LOG_WARNING("fail to save header. [res=%d tablet=%s]",
                             res, temp_tablet->full_name().c_str());
            break;
        }
        temp_tablet->release_header_lock();
    }

    // Show delete conditions in tablet header.
    for (TabletSharedPtr tablet : table_list) {
        cond_handler.log_conds(tablet);
    }

    LOG(INFO) << "finish to process cancel delete. res=" << res;
    return res;
}

OLAPStatus StorageEngine::delete_data(
        const TPushReq& request,
        vector<TTabletInfo>* tablet_info_vec) {
    LOG(INFO) << "begin to process delete data. request=" << ThriftDebugString(request);
    DorisMetrics::delete_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    if (tablet_info_vec == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    // 1. Get all tablets with same tablet_id
    TabletSharedPtr tablet = get_tablet(request.tablet_id, request.schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("can't find tablet. [tablet=%ld schema_hash=%d]",
                         request.tablet_id, request.schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 2. Process delete data by push interface
    PushHandler push_handler;
    if (request.__isset.transaction_id) {
        res = push_handler.process_realtime_push(tablet, request, PUSH_FOR_DELETE, tablet_info_vec);
    } else {
        res = OLAP_ERR_PUSH_BATCH_PROCESS_REMOVED;
    }

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to push empty version for delete data. "
                         "[res=%d tablet='%s']",
                         res, tablet->full_name().c_str());
        DorisMetrics::delete_requests_failed.increment(1);
        return res;
    }

    LOG(INFO) << "finish to process delete data. res=" << res;
    return res;
}

OLAPStatus StorageEngine::recover_tablet_until_specfic_version(
        const TRecoverTabletReq& recover_tablet_req) {
    TabletSharedPtr tablet = get_tablet(recover_tablet_req.tablet_id,
                                   recover_tablet_req.schema_hash);
    if (tablet == nullptr) { return OLAP_ERR_TABLE_NOT_FOUND; }
    RETURN_NOT_OK(tablet->recover_tablet_until_specfic_version(recover_tablet_req.version,
                                                        recover_tablet_req.version_hash));
    return OLAP_SUCCESS;
}

string StorageEngine::get_info_before_incremental_clone(TabletSharedPtr tablet,
    int64_t committed_version, vector<Version>* missing_versions) {

    // get missing versions
    tablet->obtain_header_rdlock();
    tablet->get_missing_versions_with_header_locked(committed_version, missing_versions);

    // get least complete version
    // prevent lastest version not replaced (if need to rewrite) after node restart
    const PDelta* least_complete_version = tablet->least_complete_version(*missing_versions);
    if (least_complete_version != NULL) {
        // TODO: Used in upgraded. If old Doris version, version can be converted.
        Version version(least_complete_version->start_version(), least_complete_version->end_version()); 
        missing_versions->push_back(version);
        LOG(INFO) << "least complete version for incremental clone. tablet=" << tablet->full_name()
                  << ", least_complete_version=" << least_complete_version->end_version();
    }

    tablet->release_header_lock();
    LOG(INFO) << "finish to calculate missing versions when clone. [tablet=" << tablet->full_name()
              << ", committed_version=" << committed_version << " missing_versions_size=" << missing_versions->size() << "]";

    // get download path
    return tablet->tablet_path() + CLONE_PREFIX;
}

OLAPStatus StorageEngine::finish_clone(TabletSharedPtr tablet, const string& clone_dir,
                                         int64_t committed_version, bool is_incremental_clone) {
    OLAPStatus res = OLAP_SUCCESS;
    vector<string> linked_success_files;

    // clone and compaction operation should be performed sequentially
    tablet->obtain_base_compaction_lock();
    tablet->obtain_cumulative_lock();

    tablet->obtain_push_lock();
    tablet->obtain_header_wrlock();
    do {
        // check clone dir existed
        if (!check_dir_existed(clone_dir)) {
            res = OLAP_ERR_DIR_NOT_EXIST;
            OLAP_LOG_WARNING("clone dir not existed when clone. [clone_dir=%s]",
                             clone_dir.c_str());
            break;
        }

        // load src header
        string clone_header_file = clone_dir + "/" + std::to_string(tablet->tablet_id()) + ".hdr";
        TabletMeta clone_header(clone_header_file);
        if ((res = clone_header.load_and_init()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to load src header when clone. [clone_header_file=%s]",
                             clone_header_file.c_str());
            break;
        }

        // check all files in /clone and /tablet
        set<string> clone_files;
        if ((res = dir_walk(clone_dir, NULL, &clone_files)) != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to dir walk when clone. [clone_dir=" << clone_dir << "]";
            break;
        }

        set<string> local_files;
        string tablet_dir = tablet->tablet_path();
        if ((res = dir_walk(tablet_dir, NULL, &local_files)) != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to dir walk when clone. [tablet_dir=" << tablet_dir << "]";
            break;
        }

        // link files from clone dir, if file exists, skip it
        for (const string& clone_file : clone_files) {
            if (local_files.find(clone_file) != local_files.end()) {
                VLOG(3) << "find same file when clone, skip it. "
                        << "tablet=" << tablet->full_name()
                        << ", clone_file=" << clone_file;
                continue;
            }

            string from = clone_dir + "/" + clone_file;
            string to = tablet_dir + "/" + clone_file;
            LOG(INFO) << "src file:" << from << "dest file:" << to;
            if (link(from.c_str(), to.c_str()) != 0) {
                OLAP_LOG_WARNING("fail to create hard link when clone. [from=%s to=%s]",
                                 from.c_str(), to.c_str());
                res = OLAP_ERR_OS_ERROR;
                break;
            }
            linked_success_files.emplace_back(std::move(to));
        }

        if (res != OLAP_SUCCESS) {
            break;
        }

        if (is_incremental_clone) {
            res = clone_incremental_data(
                                              tablet, clone_header, committed_version);
        } else {
            res = clone_full_data(tablet, clone_header);
        }

        // if full clone success, need to update cumulative layer point
        if (!is_incremental_clone && res == OLAP_SUCCESS) {
            tablet->set_cumulative_layer_point(clone_header.cumulative_layer_point());
        }

    } while (0);

    // clear linked files if errors happen
    if (res != OLAP_SUCCESS) {
        remove_files(linked_success_files);
    }
    tablet->release_header_lock();
    tablet->release_push_lock();

    tablet->release_cumulative_lock();
    tablet->release_base_compaction_lock();

    // clear clone dir
    boost::filesystem::path clone_dir_path(clone_dir);
    boost::filesystem::remove_all(clone_dir_path);
    LOG(INFO) << "finish to clone data, clear downloaded data. res=" << res
              << ", tablet=" << tablet->full_name()
              << ", clone_dir=" << clone_dir;
    return res;
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
    res =  _tablet_mgr.load_one_tablet(
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
    res =  _tablet_mgr.load_one_tablet(
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

OLAPStatus StorageEngine::clear_alter_task(const TTabletId tablet_id,
                                        const TSchemaHash schema_hash) {
    LOG(INFO) << "begin to process clear alter task. tablet_id=" << tablet_id
              << ", schema_hash=" << schema_hash;
    TabletSharedPtr tablet = get_tablet(tablet_id, schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("can't find tablet when process clear alter task. ",
                         "[tablet_id=%ld, schema_hash=%d]", tablet_id, schema_hash);
        return OLAP_SUCCESS;
    }

    // get schema change info
    AlterTabletType type;
    TTabletId related_tablet_id;
    TSchemaHash related_schema_hash;
    vector<Version> schema_change_versions;
    tablet->obtain_header_rdlock();
    bool ret = tablet->get_schema_change_request(
            &related_tablet_id, &related_schema_hash, &schema_change_versions, &type);
    tablet->release_header_lock();
    if (!ret) {
        return OLAP_SUCCESS;
    } else if (!schema_change_versions.empty()) {
        OLAP_LOG_WARNING("find alter task unfinished when process clear alter task. ",
                         "[tablet=%s versions_to_change_size=%d]",
                         tablet->full_name().c_str(), schema_change_versions.size());
        return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
    }

    // clear schema change info
    tablet->obtain_header_wrlock();
    tablet->clear_schema_change_request();
    OLAPStatus res = tablet->save_tablet_meta();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save header. [res=" << res << " tablet='" << tablet->full_name() << "']";
    } else {
        LOG(INFO) << "clear alter task on tablet. [tablet='" << tablet->full_name() << "']";
    }
    tablet->release_header_lock();

    // clear related tablet's schema change info
    TabletSharedPtr related_tablet = get_tablet(related_tablet_id, related_schema_hash);
    if (related_tablet.get() == NULL) {
        OLAP_LOG_WARNING("related tablet not found when process clear alter task. "
                         "[tablet_id=%ld schema_hash=%d "
                         "related_tablet_id=%ld related_schema_hash=%d]",
                         tablet_id, schema_hash, related_tablet_id, related_schema_hash);
    } else {
        related_tablet->obtain_header_wrlock();
        related_tablet->clear_schema_change_request();
        res = related_tablet->save_tablet_meta();
        if (res != OLAP_SUCCESS) {
            LOG(FATAL) << "fail to save header. [res=" << res << " tablet='"
                       << related_tablet->full_name() << "']";
        } else {
            LOG(INFO) << "clear alter task on tablet. [tablet='" << related_tablet->full_name() << "']";
        }
        related_tablet->release_header_lock();
    }

    LOG(INFO) << "finish to process clear alter task."
              << "tablet_id=" << related_tablet_id
              << ", schema_hash=" << related_schema_hash;
    return OLAP_SUCCESS;
}

OLAPStatus StorageEngine::push(
        const TPushReq& request,
        vector<TTabletInfo>* tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "begin to process push. tablet_id=" << request.tablet_id
              << ", version=" << request.version;

    if (tablet_info_vec == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        DorisMetrics::push_requests_fail_total.increment(1);
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    TabletSharedPtr tablet = get_tablet(
            request.tablet_id, request.schema_hash);
    if (NULL == tablet.get()) {
        OLAP_LOG_WARNING("false to find tablet. [tablet=%ld schema_hash=%d]",
                         request.tablet_id, request.schema_hash);
        DorisMetrics::push_requests_fail_total.increment(1);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    PushType type = PUSH_NORMAL;
    if (request.push_type == TPushType::LOAD_DELETE) {
        type = PUSH_FOR_LOAD_DELETE;
    }

    int64_t duration_ns = 0;
    PushHandler push_handler;
    if (request.__isset.transaction_id) {
        {
            SCOPED_RAW_TIMER(&duration_ns);
            res = push_handler.process_realtime_push(tablet, request, type, tablet_info_vec);
        }
    } else {
        {
            SCOPED_RAW_TIMER(&duration_ns);
            res = OLAP_ERR_PUSH_BATCH_PROCESS_REMOVED;
        }
    }

    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to push delta, tablet=" << tablet->full_name().c_str()
            << ",cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
        DorisMetrics::push_requests_fail_total.increment(1);
    } else {
        LOG(INFO) << "success to push delta, tablet=" << tablet->full_name().c_str()
            << ",cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
        DorisMetrics::push_requests_success_total.increment(1);
        DorisMetrics::push_request_duration_us.increment(duration_ns / 1000);
        DorisMetrics::push_request_write_bytes.increment(push_handler.write_bytes());
        DorisMetrics::push_request_write_rows.increment(push_handler.write_rows());
    }
    return res;
}

}  // namespace doris
