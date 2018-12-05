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
#include "olap/olap_header.h"
#include "olap/olap_header_manager.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/schema_change.h"
#include "olap/store.h"
#include "olap/utils.h"
#include "olap/data_writer.h"
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

bool _sort_tablet_by_create_time(const TabletSharedPtr& a, const TabletSharedPtr& b) {
    return a->creation_time() < b->creation_time();
}

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
        _global_tablet_id(0),
        _index_stream_lru_cache(NULL),
        _tablet_stat_cache_update_time_ms(0),
        _snapshot_base_id(0),
        _is_report_disk_state_already(false),
        _is_report_tablet_already(false) {
    if (_s_instance == nullptr) {
        _s_instance = this;
    }
}

StorageEngine::~StorageEngine() {
    clear();
}

OLAPStatus StorageEngine::_load_store(OlapStore* store) {
    std::string store_path = store->path();
    LOG(INFO) <<"start to load tablets from store_path:" << store_path;

    bool is_header_converted = false;
    OLAPStatus res = OlapHeaderManager::get_header_converted(store, is_header_converted);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "get convert flag from meta failed";
        return res;
    }
    if (is_header_converted) {
        LOG(INFO) << "load header from meta";
        OLAPStatus s = store->load_tables(this);
        LOG(INFO) << "load header from meta finished";
        if (s != OLAP_SUCCESS) {
            LOG(WARNING) << "there is failure when loading tablet headers, path:" << store_path;
            return s;
        } else {
            return OLAP_SUCCESS;
        }
    }

    // compatible for old header load method
    // walk all directory to load header file
    LOG(INFO) << "load headers from header files";

    // get all shards
    set<string> shards;
    if (dir_walk(store_path + DATA_PREFIX, &shards, NULL) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to walk dir. [root=" << store_path << "]";
        return OLAP_ERR_INIT_FAILED;
    }

    for (const auto& shard : shards) {
        // get all tablets
        set<string> tablets;
        string one_shard_path = store_path + DATA_PREFIX +  '/' + shard;
        if (dir_walk(one_shard_path, &tablets, NULL) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to walk dir. [root=" << one_shard_path << "]";
            continue;
        }

        for (const auto& tablet : tablets) {
            // 遍历table目录寻找此table的所有indexedRollupTable，注意不是SegmentGroup，而是Tablet
            set<string> schema_hashes;
            string one_tablet_path = one_shard_path + '/' + tablet;
            if (dir_walk(one_tablet_path, &schema_hashes, NULL) != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to walk dir. [root=" << one_tablet_path << "]";
                continue;
            }

            for (const auto& schema_hash : schema_hashes) {
                TTabletId tablet_id = strtoul(tablet.c_str(), NULL, 10);
                TSchemaHash tablet_schema_hash = strtoul(schema_hash.c_str(), NULL, 10);

                // 遍历schema_hash目录寻找此index的所有schema
                // 加载失败依然加载下一个Table
                if (load_one_tablet(
                        store,
                        tablet_id,
                        tablet_schema_hash,
                        one_tablet_path + '/' + schema_hash) != OLAP_SUCCESS) {
                    OLAP_LOG_WARNING("fail to load one tablet, but continue. [path='%s']",
                                     (one_tablet_path + '/' + schema_hash).c_str());
                }
            }
        }
    }
    res = OlapHeaderManager::set_converted_flag(store);
    LOG(INFO) << "load header from header files finished";
    return res;
}

OLAPStatus StorageEngine::load_one_tablet(
        OlapStore* store, TTabletId tablet_id, SchemaHash schema_hash,
        const string& schema_hash_path, bool force) {
    stringstream header_name_stream;
    header_name_stream << schema_hash_path << "/" << tablet_id << ".hdr";
    string header_path = header_name_stream.str();
    path boost_schema_hash_path(schema_hash_path);

    if (access(header_path.c_str(), F_OK) != 0) {
        LOG(WARNING) << "fail to find header file. [header_path=" << header_path << "]";
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_FILE_NOT_EXIST;
    }

    auto tablet = Tablet::create_from_header_file(
            tablet_id, schema_hash, header_path, store);
    if (tablet == NULL) {
        LOG(WARNING) << "fail to load tablet. [header_path=" << header_path << "]";
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    if (tablet->lastest_version() == NULL && !tablet->is_schema_changing()) {
        OLAP_LOG_WARNING("tablet not in schema change state without delta is invalid. "
                         "[header_path=%s]",
                         header_path.c_str());
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    // 这里不需要SAFE_DELETE(tablet),因为tablet指针已经在add_table中托管到smart pointer中
    OLAPStatus res = OLAP_SUCCESS;
    string table_name = tablet->full_name();
    res = add_tablet(tablet_id, schema_hash, tablet, force);
    if (res != OLAP_SUCCESS) {
        // 插入已经存在的table时返回成功
        if (res == OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE) {
            return OLAP_SUCCESS;
        }

        LOG(WARNING) << "failed to add tablet. [tablet=" << table_name << "]";
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    if (register_tablet_into_root_path(tablet.get()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to register tablet into root path. [root_path=%s]",
                         schema_hash_path.c_str());

        if (StorageEngine::get_instance()->drop_tablet(tablet_id, schema_hash) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to drop tablet when create tablet failed. "
                             "[tablet=%ld schema_hash=%d]",
                             tablet_id, schema_hash);
        }

        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    // load pending data (for realtime push), will add transaction relationship into engine
    tablet->load_pending_data();

    VLOG(3) << "succeed to add tablet. tablet=" << tablet->full_name()
            << ", path=" << schema_hash_path;
    return OLAP_SUCCESS;
}

void StorageEngine::check_none_row_oriented_tablet(const std::vector<OlapStore*>& stores) {
    for (auto store : stores) {
        auto res = _check_none_row_oriented_tablet_in_store(store);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "io error when init load tables. res=" << res
                << ", store=" << store->path();
        }
    }
}

OLAPStatus StorageEngine::_check_none_row_oriented_tablet_in_store(OlapStore* store) {
    std::string store_path = store->path();
    LOG(INFO) <<"start to load tablets from store_path:" << store_path;

    bool is_header_converted = false;
    OLAPStatus res = OlapHeaderManager::get_header_converted(store, is_header_converted);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "get convert flag from meta failed";
        return res;
    }
    if (is_header_converted) {
        OLAPStatus s = store->check_none_row_oriented_tablet_in_store(this);
        if (s != OLAP_SUCCESS) {
            LOG(WARNING) << "there is failure when loading tablet headers, path:" << store_path;
            return s;
        } else {
            return OLAP_SUCCESS;
        }
    }

    // compatible for old header load method
    // walk all directory to load header file
    LOG(INFO) << "check has none row-oriented tablet from header files";

    // get all shards
    set<string> shards;
    if (dir_walk(store_path + DATA_PREFIX, &shards, NULL) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to walk dir. [root=" << store_path << "]";
        return OLAP_ERR_INIT_FAILED;
    }

    for (const auto& shard : shards) {
        // get all tablets
        set<string> tablets;
        string one_shard_path = store_path + DATA_PREFIX +  '/' + shard;
        if (dir_walk(one_shard_path, &tablets, NULL) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to walk dir. [root=" << one_shard_path << "]";
            continue;
        }

        for (const auto& tablet : tablets) {
            // 遍历table目录寻找此table的所有indexedRollupTable，注意不是SegmentGroup，而是Tablet
            set<string> schema_hashes;
            string one_tablet_path = one_shard_path + '/' + tablet;
            if (dir_walk(one_tablet_path, &schema_hashes, NULL) != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to walk dir. [root=" << one_tablet_path << "]";
                continue;
            }

            for (const auto& schema_hash : schema_hashes) {
                TTabletId tablet_id = strtoul(tablet.c_str(), NULL, 10);
                TSchemaHash tablet_schema_hash = strtoul(schema_hash.c_str(), NULL, 10);

                // 遍历schema_hash目录寻找此index的所有schema
                // 加载失败依然加载下一个Table
                if (check_none_row_oriented_tablet_in_path(
                        store,
                        tablet_id,
                        tablet_schema_hash,
                        one_tablet_path + '/' + schema_hash) != OLAP_SUCCESS) {
                    OLAP_LOG_WARNING("fail to load one tablet, but continue. [path='%s']",
                                     (one_tablet_path + '/' + schema_hash).c_str());
                }
            }
        }
    }
    return res;
}

OLAPStatus StorageEngine::check_none_row_oriented_tablet_in_path(
        OlapStore* store, TTabletId tablet_id,
        SchemaHash schema_hash, const string& schema_hash_path) {
    stringstream header_name_stream;
    header_name_stream << schema_hash_path << "/" << tablet_id << ".hdr";
    string header_path = header_name_stream.str();
    path boost_schema_hash_path(schema_hash_path);

    if (access(header_path.c_str(), F_OK) != 0) {
        LOG(WARNING) << "fail to find header file. [header_path=" << header_path << "]";
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_FILE_NOT_EXIST;
    }

    auto tablet = Tablet::create_from_header_file_for_check(
            tablet_id, schema_hash, header_path);
    if (tablet == NULL) {
        LOG(WARNING) << "fail to load tablet. [header_path=" << header_path << "]";
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    LOG(INFO) << "data_file_type:" << tablet->data_file_type();
    if (tablet->data_file_type() == OLAP_DATA_FILE) {
        LOG(FATAL) << "Not support row-oriented tablet any more. Please convert it to column-oriented tablet."
                   << "tablet=" << tablet->full_name();
    }

    return OLAP_SUCCESS;
}

void StorageEngine::load_stores(const std::vector<OlapStore*>& stores) {
    std::vector<std::thread> threads;
    for (auto store : stores) {
        threads.emplace_back([this, store] {
            auto res = _load_store(store);
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
        OlapStore* store = new OlapStore(path.path, path.capacity_bytes);
        auto st = store->load();
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
        _tablet_map.clear();
        return OLAP_ERR_INIT_FAILED;
    }
    FileHandler::set_fd_cache(cache);

    // 初始化LRUCache
    // cache大小可通过配置文件配置
    _index_stream_lru_cache = new_lru_cache(config::index_stream_cache_capacity);
    if (_index_stream_lru_cache == NULL) {
        OLAP_LOG_WARNING("failed to init index stream LRUCache");
        _tablet_map.clear();
        return OLAP_ERR_INIT_FAILED;
    }

    // 初始化CE调度器
    int32_t cumulative_compaction_num_threads = config::cumulative_compaction_num_threads;
    int32_t base_compaction_num_threads = config::base_compaction_num_threads;
    uint32_t file_system_num = get_file_system_count();
    _max_cumulative_compaction_task_per_disk = (cumulative_compaction_num_threads + file_system_num - 1) / file_system_num;
    _max_base_compaction_task_per_disk = (base_compaction_num_threads + file_system_num - 1) / file_system_num;

    auto stores = get_stores();
    check_none_row_oriented_tablet(stores);
    load_stores(stores);
    // 取消未完成的SchemaChange任务
    _cancel_unfinished_schema_change();

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
std::vector<OlapStore*> StorageEngine::get_stores() {
    std::vector<OlapStore*> stores;
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

template std::vector<OlapStore*> StorageEngine::get_stores<false>();
template std::vector<OlapStore*> StorageEngine::get_stores<true>();

OLAPStatus StorageEngine::get_all_root_path_info(vector<RootPathInfo>* root_paths_info) {
    OLAPStatus res = OLAP_SUCCESS;
    root_paths_info->clear();

    MonotonicStopWatch timer;
    timer.start();
    int tablet_counter = 0;

    // get all root path info and construct a path map.
    // path -> RootPathInfo
    std::map<std::string, RootPathInfo> path_map;
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& it : _store_map) {
            std::string path = it.first;
            path_map.emplace(path, it.second->to_root_path_info());
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
    _tablet_map_lock.rdlock();
    for (auto& entry : _tablet_map) {
        TableInstances& instance = entry.second;
        for (auto& tablet : instance.table_arr) {
            ++tablet_counter;
            int64_t data_size = tablet->get_data_size();
            auto find = path_map.find(tablet->storage_root_path_name()); 
            if (find == path_map.end()) {
                continue;
            }
            if (find->second.is_used) {
                find->second.data_used_capacity += data_size;
            }
        } 
    }
    _tablet_map_lock.unlock();

    // add path info to root_paths_info
    for (auto& entry : path_map) {
        root_paths_info->emplace_back(entry.second);
    }

    // get available capacity of each path
    for (auto& info: *root_paths_info) {
        if (info.is_used) {
            _get_path_available_capacity(info.path,  &info.available);
        }
    }
    timer.stop();
    LOG(INFO) << "get root path info cost: " << timer.elapsed_time() / 1000000
            << " ms. tablet counter: " << tablet_counter;

    return res;
}

OLAPStatus StorageEngine::register_tablet_into_root_path(Tablet* tablet) {
    return tablet->store()->register_tablet(tablet);
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

std::vector<OlapStore*> StorageEngine::get_stores_for_create_tablet(
        TStorageMedium::type storage_medium) {
    std::vector<OlapStore*> stores;
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

OlapStore* StorageEngine::get_store(const std::string& path) {
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
        for (auto& tablet : it.second->_tablet_set) {
            tablet_info_vec.push_back(tablet);
        }
        it.second->_tablet_set.clear();
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
    
    StorageEngine::get_instance()->drop_tables_on_error_root_path(tablet_info_vec);
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

    _tablet_map.clear();
    _transaction_tablet_map.clear();
    _global_tablet_id = 0;

    return OLAP_SUCCESS;
}

TabletSharedPtr StorageEngine::_get_tablet_with_no_lock(TTabletId tablet_id, SchemaHash schema_hash) {
    VLOG(3) << "begin to get tablet. tablet_id=" << tablet_id;
    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end()) {
        for (TabletSharedPtr tablet : it->second.table_arr) {
            if (tablet->equal(tablet_id, schema_hash)) {
                VLOG(3) << "get tablet success. tablet_id=" << tablet_id;
                return tablet;
            }
        }
    }

    VLOG(3) << "fail to get tablet. tablet_id=" << tablet_id;
    // Return empty tablet if fail
    TabletSharedPtr tablet;
    return tablet;
}

TabletSharedPtr StorageEngine::get_tablet(TTabletId tablet_id, SchemaHash schema_hash, bool load_tablet) {
    _tablet_map_lock.rdlock();
    TabletSharedPtr tablet;
    tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
    _tablet_map_lock.unlock();

    if (tablet.get() != NULL) {
        if (!tablet->is_used()) {
            OLAP_LOG_WARNING("tablet cannot be used. [tablet=%ld]", tablet_id);
            tablet.reset();
        } else if (load_tablet && !tablet->is_loaded()) {
            if (tablet->load() != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to load tablet. [tablet=%ld]", tablet_id);
                tablet.reset();
            }
        }
    }

    return tablet;
}

OLAPStatus StorageEngine::get_tables_by_id(
        TTabletId tablet_id,
        list<TabletSharedPtr>* table_list) {
    OLAPStatus res = OLAP_SUCCESS;
    VLOG(3) << "begin to get tables by id. tablet_id=" << tablet_id;

    _tablet_map_lock.rdlock();
    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end()) {
        for (TabletSharedPtr tablet : it->second.table_arr) {
            table_list->push_back(tablet);
        }
    }
    _tablet_map_lock.unlock();

    if (table_list->size() == 0) {
        OLAP_LOG_WARNING("there is no tablet with specified id. [tablet=%ld]", tablet_id);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    for (std::list<TabletSharedPtr>::iterator it = table_list->begin();
            it != table_list->end();) {
        if (!(*it)->is_loaded()) {
            if ((*it)->load() != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to load tablet. [tablet='%s']",
                                 (*it)->full_name().c_str());
                it = table_list->erase(it);
                continue;
            }
        }
        ++it;
    }

    VLOG(3) << "success to get tables by id. table_num=" << table_list->size();
    return res;
}

bool StorageEngine::check_tablet_id_exist(TTabletId tablet_id) {
    bool is_exist = false;
    _tablet_map_lock.rdlock();

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end() && it->second.table_arr.size() != 0) {
        is_exist = true;
    }

    _tablet_map_lock.unlock();
    return is_exist;
}

OLAPStatus StorageEngine::add_tablet(TTabletId tablet_id, SchemaHash schema_hash,
                                 const TabletSharedPtr& tablet, bool force) {
    OLAPStatus res = OLAP_SUCCESS;
    VLOG(3) << "begin to add tablet to StorageEngine. "
            << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
            << ", force=" << force;
    _tablet_map_lock.wrlock();

    tablet->set_id(_global_tablet_id++);

    TabletSharedPtr table_item;
    for (TabletSharedPtr item : _tablet_map[tablet_id].table_arr) {
        if (item->equal(tablet_id, schema_hash)) {
            table_item = item;
            break;
        }
    }

    if (table_item.get() == NULL) {
        _tablet_map[tablet_id].table_arr.push_back(tablet);
        _tablet_map[tablet_id].table_arr.sort(_sort_tablet_by_create_time);
        _tablet_map_lock.unlock();

        return res;
    }
    _tablet_map_lock.unlock();

    if (!force) {
        if (table_item->tablet_path() == tablet->tablet_path()) {
            LOG(WARNING) << "add the same tablet twice! tablet_id="
                << tablet_id << " schema_hash=" << tablet_id;
            return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
        }
    }

    table_item->obtain_header_rdlock();
    int64_t old_time = table_item->lastest_version()->creation_time();
    int64_t new_time = tablet->lastest_version()->creation_time();
    int32_t old_version = table_item->lastest_version()->end_version();
    int32_t new_version = tablet->lastest_version()->end_version();
    table_item->release_header_lock();

    /*
     * In restore process, we replace all origin files in tablet dir with
     * the downloaded snapshot files. Than we try to reload tablet header.
     * force == true means we forcibly replace the Tablet in _tablet_map
     * with the new one. But if we do so, the files in the tablet dir will be
     * dropped when the origin Tablet deconstruct.
     * So we set keep_files == true to not delete files when the
     * origin Tablet deconstruct.
     */
    bool keep_files = force ? true : false;
    if (force || (new_version > old_version
            || (new_version == old_version && new_time > old_time))) {
        drop_tablet(tablet_id, schema_hash, keep_files);
        _tablet_map_lock.wrlock();
        _tablet_map[tablet_id].table_arr.push_back(tablet);
        _tablet_map[tablet_id].table_arr.sort(_sort_tablet_by_create_time);
        _tablet_map_lock.unlock();
    } else {
        tablet->mark_dropped();
        res = OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
    }
    LOG(WARNING) << "add duplicated tablet. force=" << force << ", res=" << res
            << ", tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
            << ", old_version=" << old_version << ", new_version=" << new_version
            << ", old_time=" << old_time << ", new_time=" << new_time
            << ", old_tablet_path=" << table_item->tablet_path()
            << ", new_tablet_path=" << tablet->tablet_path();

    return res;
}

OLAPStatus StorageEngine::add_transaction(
    TPartitionId partition_id, TTransactionId transaction_id,
    TTabletId tablet_id, SchemaHash schema_hash, const PUniqueId& load_id) {

    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash);
    WriteLock wrlock(&_transaction_tablet_map_lock);
    auto it = _transaction_tablet_map.find(key);
    if (it != _transaction_tablet_map.end()) {
        auto load_info = it->second.find(tablet_info);
        if (load_info != it->second.end()) {
            for (PUniqueId& pid : load_info->second) {
                if (pid.hi() == load_id.hi() && pid.lo() == load_id.lo()) {
                    LOG(WARNING) << "find transaction exists when add to engine."
                        << "partition_id: " << key.first
                        << ", transaction_id: " << key.second
                        << ", tablet: " << tablet_info.to_string();
                    return OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST;
                }
            }
        }
    }

    _transaction_tablet_map[key][tablet_info].push_back(load_id);
    VLOG(3) << "add transaction to engine successfully."
            << "partition_id: " << key.first
            << ", transaction_id: " << key.second
            << ", tablet: " << tablet_info.to_string();
    return OLAP_SUCCESS;
}

void StorageEngine::delete_transaction(
    TPartitionId partition_id, TTransactionId transaction_id,
    TTabletId tablet_id, SchemaHash schema_hash, bool delete_from_tablet) {

    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash);
    WriteLock wrlock(&_transaction_tablet_map_lock);

    auto it = _transaction_tablet_map.find(key);
    if (it != _transaction_tablet_map.end()) {
        VLOG(3) << "delete transaction to engine successfully."
                << ",partition_id: " << key.first
                << ", transaction_id: " << key.second
                << ", tablet: " << tablet_info.to_string();
        it->second.erase(tablet_info);
        if (it->second.empty()) {
            _transaction_tablet_map.erase(it);
        }

        // delete transaction from tablet
        if (delete_from_tablet) {
            TabletSharedPtr tablet = get_tablet(tablet_info.tablet_id, tablet_info.schema_hash);
            if (tablet.get() != nullptr) {
                tablet->delete_pending_data(transaction_id);
            }
        }
    }
}

void StorageEngine::get_transactions_by_tablet(TabletSharedPtr tablet, int64_t* partition_id,
                                            set<int64_t>* transaction_ids) {
    if (tablet.get() == nullptr || partition_id == nullptr || transaction_ids == nullptr) {
        OLAP_LOG_WARNING("parameter is null when get transactions by tablet");
        return;
    }

    TabletInfo tablet_info(tablet->tablet_id(), tablet->schema_hash());
    ReadLock rdlock(&_transaction_tablet_map_lock);
    for (auto& it : _transaction_tablet_map) {
        if (it.second.find(tablet_info) != it.second.end()) {
            *partition_id = it.first.first;
            transaction_ids->insert(it.first.second);
            VLOG(3) << "find transaction on tablet."
                    << "partition_id: " << it.first.first
                    << ", transaction_id: " << it.first.second
                    << ", tablet: " << tablet_info.to_string();
        }
    }
}

bool StorageEngine::has_transaction(TPartitionId partition_id, TTransactionId transaction_id,
                                 TTabletId tablet_id, SchemaHash schema_hash) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash);

    _transaction_tablet_map_lock.rdlock();
    auto it = _transaction_tablet_map.find(key);
    bool found = it != _transaction_tablet_map.end()
                 && it->second.find(tablet_info) != it->second.end();
    _transaction_tablet_map_lock.unlock();

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
        pair<int64_t, int64_t> key(partition_id, transaction_id);

        _transaction_tablet_map_lock.rdlock();
        auto it = _transaction_tablet_map.find(key);
        if (it == _transaction_tablet_map.end()) {
            OLAP_LOG_WARNING("no tablet to publish version. [partition_id=%ld transaction_id=%ld]",
                             partition_id, transaction_id);
            _transaction_tablet_map_lock.unlock();
            continue;
        }
        std::map<TabletInfo, std::vector<PUniqueId>> load_info_map = it->second;
        _transaction_tablet_map_lock.unlock();

        Version version(partitionVersionInfo.version, partitionVersionInfo.version);
        VersionHash version_hash = partitionVersionInfo.version_hash;

        // each tablet
        for (auto& load_info : load_info_map) {
            const TabletInfo& tablet_info = load_info.first;
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
                _transaction_tablet_map_lock.wrlock();
                auto it2 = _transaction_tablet_map.find(key);
                if (it2 != _transaction_tablet_map.end()) {
                    VLOG(3) << "delete transaction from engine. tablet=" << tablet->full_name()
                        << "transaction_id: " << transaction_id;
                    it2->second.erase(tablet_info);
                    if (it2->second.empty()) {
                        _transaction_tablet_map.erase(it2);
                    }
                }
                _transaction_tablet_map_lock.unlock();

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

    // each partition
    for (const TPartitionId& partition_id : partition_ids) {

        // get tablets in this transaction
        pair<int64_t, int64_t> key(partition_id, transaction_id);

        _transaction_tablet_map_lock.rdlock();
        auto it = _transaction_tablet_map.find(key);
        if (it == _transaction_tablet_map.end()) {
            OLAP_LOG_WARNING("no tablet to clear transaction. [partition_id=%ld transaction_id=%ld]",
                             partition_id, transaction_id);
            _transaction_tablet_map_lock.unlock();
            continue;
        }
        std::map<TabletInfo, std::vector<PUniqueId>> load_info_map = it->second;
        _transaction_tablet_map_lock.unlock();

        // each tablet
        for (auto& load_info : load_info_map) {
            const TabletInfo& tablet_info = load_info.first;
            delete_transaction(partition_id, transaction_id,
                               tablet_info.tablet_id, tablet_info.schema_hash);
        }
    }

    LOG(INFO) << "finish to clear transaction task. transaction_id=" << transaction_id;
}

OLAPStatus StorageEngine::clone_incremental_data(TabletSharedPtr tablet, OLAPHeader& clone_header,
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

OLAPStatus StorageEngine::clone_full_data(TabletSharedPtr tablet, OLAPHeader& clone_header) {
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
    LOG(INFO) << "begin to process drop tablet."
        << "tablet=" << tablet_id << ", schema_hash=" << schema_hash;
    DorisMetrics::drop_tablet_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    // Get tablet which need to be droped
    _tablet_map_lock.rdlock();
    TabletSharedPtr dropped_tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
    _tablet_map_lock.unlock();
    if (dropped_tablet.get() == NULL) {
        OLAP_LOG_WARNING("fail to drop not existed tablet. [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // Try to get schema change info
    AlterTabletType type;
    TTabletId related_tablet_id;
    TSchemaHash related_schema_hash;
    vector<Version> schema_change_versions;
    dropped_tablet->obtain_header_rdlock();
    bool ret = dropped_tablet->get_schema_change_request(
            &related_tablet_id, &related_schema_hash, &schema_change_versions, &type);
    dropped_tablet->release_header_lock();

    // Drop tablet directly when not in schema change
    if (!ret) {
        return _drop_tablet_directly(tablet_id, schema_hash, keep_files);
    }

    // Check tablet is in schema change or not, is base tablet or not
    bool is_schema_change_finished = true;
    if (schema_change_versions.size() != 0) {
        is_schema_change_finished = false;
    }

    bool is_drop_base_tablet = false;
    _tablet_map_lock.rdlock();
    TabletSharedPtr related_tablet = _get_tablet_with_no_lock(
            related_tablet_id, related_schema_hash);
    _tablet_map_lock.unlock();
    if (related_tablet.get() == NULL) {
        OLAP_LOG_WARNING("drop tablet directly when related tablet not found. "
                         "[tablet_id=%ld schema_hash=%d]",
                         related_tablet_id, related_schema_hash);
        return _drop_tablet_directly(tablet_id, schema_hash, keep_files);
    }

    if (dropped_tablet->creation_time() < related_tablet->creation_time()) {
        is_drop_base_tablet = true;
    }

    if (is_drop_base_tablet && !is_schema_change_finished) {
        OLAP_LOG_WARNING("base tablet in schema change cannot be droped. [tablet=%s]",
                         dropped_tablet->full_name().c_str());
        return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
    }

    // Drop specified tablet and clear schema change info
    _tablet_map_lock.wrlock();
    related_tablet->obtain_header_wrlock();
    related_tablet->clear_schema_change_request();
    res = related_tablet->save_header();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save tablet header. res=" << res
                   << ", tablet=" << related_tablet->full_name();
    }

    res = _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);
    related_tablet->release_header_lock();
    _tablet_map_lock.unlock();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to drop tablet which in schema change. [tablet=%s]",
                         dropped_tablet->full_name().c_str());
        return res;
    }

    LOG(INFO) << "finish to drop tablet. res=" << res;
    return res;
}

OLAPStatus StorageEngine::_drop_tablet_directly(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    _tablet_map_lock.wrlock();
    OLAPStatus res = _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);
    _tablet_map_lock.unlock();
    return res;
}

OLAPStatus StorageEngine::_drop_tablet_directly_unlocked(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    OLAPStatus res = OLAP_SUCCESS;

    TabletSharedPtr dropped_tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
    if (dropped_tablet.get() == NULL) {
        OLAP_LOG_WARNING("fail to drop not existed tablet. [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    for (list<TabletSharedPtr>::iterator it = _tablet_map[tablet_id].table_arr.begin();
            it != _tablet_map[tablet_id].table_arr.end();) {
        if ((*it)->equal(tablet_id, schema_hash)) {
            if (!keep_files) {
                (*it)->mark_dropped();
            }
            it = _tablet_map[tablet_id].table_arr.erase(it);
        } else {
            ++it;
        }
    }

    if (_tablet_map[tablet_id].table_arr.empty()) {
        _tablet_map.erase(tablet_id);
    }

    res = dropped_tablet->store()->deregister_tablet(dropped_tablet.get());
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to unregister from root path. [res=%d tablet=%ld]",
                         res, tablet_id);
    }

    return res;
}

OLAPStatus StorageEngine::drop_tables_on_error_root_path(
        const vector<TabletInfo>& tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;

    _tablet_map_lock.wrlock();

    for (const TabletInfo& tablet_info : tablet_info_vec) {
        TTabletId tablet_id = tablet_info.tablet_id;
        TSchemaHash schema_hash = tablet_info.schema_hash;
        VLOG(3) << "drop_tablet begin. tablet_id=" << tablet_id
                << ", schema_hash=" << schema_hash;
        TabletSharedPtr dropped_tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
        if (dropped_tablet.get() == NULL) {
            OLAP_LOG_WARNING("dropping tablet not exist. [tablet=%ld schema_hash=%d]",
                             tablet_id, schema_hash);
            continue;
        } else {
            for (list<TabletSharedPtr>::iterator it = _tablet_map[tablet_id].table_arr.begin();
                    it != _tablet_map[tablet_id].table_arr.end();) {
                if ((*it)->equal(tablet_id, schema_hash)) {
                    it = _tablet_map[tablet_id].table_arr.erase(it);
                } else {
                    ++it;
                }
            }

            if (_tablet_map[tablet_id].table_arr.empty()) {
                _tablet_map.erase(tablet_id);
            }
        }
    }

    _tablet_map_lock.unlock();

    return res;
}

TabletSharedPtr StorageEngine::create_tablet(
        const TCreateTabletReq& request, const string* ref_root_path, 
        const bool is_schema_change_tablet, const TabletSharedPtr ref_tablet) {
    // Get all available stores, use ref_root_path if the caller specified
    std::vector<OlapStore*> stores;
    if (ref_root_path == nullptr) {
        stores = get_stores_for_create_tablet(request.storage_medium);
        if (stores.empty()) {
            LOG(WARNING) << "there is no available disk that can be used to create tablet.";
            return nullptr;
        }
    } else {
        stores.push_back(ref_tablet->store());
    }

    TabletSharedPtr tablet;
    // Try to create tablet on each of all_available_root_path, util success
    for (auto& store : stores) {
        OLAPHeader* header = new OLAPHeader();
        OLAPStatus res = _create_new_tablet_header(request, store, is_schema_change_tablet, ref_tablet, header);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to create tablet header. [res=" << res << " root=" << store->path();
            break;
        }

        tablet = Tablet::create_from_header(header, store);
        if (tablet == nullptr) {
            LOG(WARNING) << "fail to load tablet from header. root_path:%s" << store->path();
            break;
        }

        // commit header finally
        res = OlapHeaderManager::save(store, request.tablet_id, request.tablet_schema.schema_hash, header);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to save header. [res=" << res << " root=" << store->path();
            break;
        }
        break;
    }

    return tablet;
}

OLAPStatus StorageEngine::create_init_version(TTabletId tablet_id, SchemaHash schema_hash,
                                           Version version, VersionHash version_hash) {
    VLOG(3) << "begin to create init version. "
            << "begin=" << version.first << ", end=" << version.second;
    TabletSharedPtr tablet;
    ColumnDataWriter* writer = NULL;
    SegmentGroup* new_segment_group = NULL;
    OLAPStatus res = OLAP_SUCCESS;
    std::vector<SegmentGroup*> index_vec;

    do {
        if (version.first > version.second) {
            OLAP_LOG_WARNING("begin should not larger than end. [begin=%d end=%d]",
                             version.first, version.second);
            res = OLAP_ERR_INPUT_PARAMETER_ERROR;
            break;
        }

        // Get tablet and generate new index
        tablet = get_tablet(tablet_id, schema_hash);
        if (tablet.get() == NULL) {
            OLAP_LOG_WARNING("fail to find tablet. [tablet=%ld]", tablet_id);
            res = OLAP_ERR_TABLE_NOT_FOUND;
            break;
        }

        new_segment_group = new(nothrow) SegmentGroup(tablet.get(), version, version_hash, false, 0, 0);
        if (new_segment_group == NULL) {
            LOG(WARNING) << "fail to malloc index. [tablet=" << tablet->full_name() << "]";
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        // Create writer, which write nothing to tablet, to generate empty data file
        writer = ColumnDataWriter::create(tablet, new_segment_group, false);
        if (writer == NULL) {
            LOG(WARNING) << "fail to create writer. [tablet=" << tablet->full_name() << "]";
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        res = writer->finalize();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to finalize writer. [tablet=" << tablet->full_name() << "]";
            break;
        }

        // Load new index and add to tablet
        res = new_segment_group->load();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load new index. [tablet=" << tablet->full_name() << "]";
            break;
        }

        WriteLock wrlock(tablet->get_header_lock_ptr());
        index_vec.push_back(new_segment_group);
        res = tablet->register_data_source(index_vec);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to register index to data sources. [tablet=%s]",
                             tablet->full_name().c_str());
            break;
        }

        res = tablet->save_header();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to save header. [tablet=" << tablet->full_name() << "]";
            break;
        }
    } while (0);

    // Unregister index and delete files(index and data) if failed
    if (res != OLAP_SUCCESS && tablet.get() != NULL) {
        std::vector<SegmentGroup*> unused_index;
        tablet->obtain_header_wrlock();
        tablet->unregister_data_source(version, &unused_index);
        tablet->release_header_lock();

        for (SegmentGroup* index : index_vec) {
            index->delete_all_files();
            SAFE_DELETE(index);
        }
    }

    VLOG(3) << "create init version end. res=" << res;
    SAFE_DELETE(writer);
    return res;
}

bool StorageEngine::try_schema_change_lock(TTabletId tablet_id) {
    bool res = false;
    VLOG(3) << "try_schema_change_lock begin. table_id=" << tablet_id;
    _tablet_map_lock.rdlock();

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it == _tablet_map.end()) {
        OLAP_LOG_WARNING("tablet does not exists. [tablet=%ld]", tablet_id);
    } else {
        res = (it->second.schema_change_lock.trylock() == OLAP_SUCCESS);
    }

    _tablet_map_lock.unlock();
    VLOG(3) << "try_schema_change_lock end. table_id=" <<  tablet_id;
    return res;
}

void StorageEngine::release_schema_change_lock(TTabletId tablet_id) {
    VLOG(3) << "release_schema_change_lock begin. tablet_id=" << tablet_id;
    _tablet_map_lock.rdlock();

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it == _tablet_map.end()) {
        OLAP_LOG_WARNING("tablet does not exists. [tablet=%ld]", tablet_id);
    } else {
        it->second.schema_change_lock.unlock();
    }

    _tablet_map_lock.unlock();
    VLOG(3) << "release_schema_change_lock end. tablet_id=" << tablet_id;
}

void StorageEngine::_build_tablet_info(TabletSharedPtr tablet, TTabletInfo* tablet_info) {
    tablet_info->tablet_id = tablet->tablet_id();
    tablet_info->schema_hash = tablet->schema_hash();

    tablet->obtain_header_rdlock();
    tablet_info->row_count = tablet->get_num_rows();
    tablet_info->data_size = tablet->get_data_size();
    const PDelta* last_file_version = tablet->lastest_version();
    if (last_file_version == NULL) {
        tablet_info->version = -1;
        tablet_info->version_hash = 0;
    } else {
        // report the version before first missing version
        vector<Version> missing_versions;
        tablet->get_missing_versions_with_header_locked(
                last_file_version->end_version(), &missing_versions);
        const PDelta* least_complete_version =
            tablet->least_complete_version(missing_versions);
        if (least_complete_version == NULL) {
            tablet_info->version = -1;
            tablet_info->version_hash = 0;
        } else {
            tablet_info->version = least_complete_version->end_version();
            tablet_info->version_hash = least_complete_version->version_hash();
        }
    }
    tablet->release_header_lock();
}

OLAPStatus StorageEngine::report_tablet_info(TTabletInfo* tablet_info) {
    DorisMetrics::report_tablet_requests_total.increment(1);
    LOG(INFO) << "begin to process report tablet info."
              << "tablet_id=" << tablet_info->tablet_id
              << ", schema_hash=" << tablet_info->schema_hash;

    OLAPStatus res = OLAP_SUCCESS;

    TabletSharedPtr tablet = get_tablet(
            tablet_info->tablet_id, tablet_info->schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("can't find tablet. [tablet=%ld schema_hash=%d]",
                         tablet_info->tablet_id, tablet_info->schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    _build_tablet_info(tablet, tablet_info);
    LOG(INFO) << "success to process report tablet info.";
    return res;
}

OLAPStatus StorageEngine::report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info) {
    LOG(INFO) << "begin to process report all tablets info.";
    DorisMetrics::report_all_tablets_requests_total.increment(1);

    if (tablets_info == NULL) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    _tablet_map_lock.rdlock();
    for (const auto& item : _tablet_map) {
        if (item.second.table_arr.size() == 0) {
            continue;
        }

        TTablet tablet;
        for (TabletSharedPtr tablet_ptr : item.second.table_arr) {
            if (tablet_ptr == NULL) {
                continue;
            }

            TTabletInfo tablet_info;
            _build_tablet_info(tablet_ptr, &tablet_info);

            // report expire transaction
            vector<int64_t> transaction_ids;
            tablet_ptr->get_expire_pending_data(&transaction_ids);
            tablet_info.__set_transaction_ids(transaction_ids);

            if (_available_storage_medium_type_count > 1) {
                tablet_info.__set_storage_medium(tablet_ptr->store()->storage_medium());
            }

            tablet_info.__set_version_count(tablet_ptr->file_delta_size());
            tablet_info.__set_path_hash(tablet_ptr->store()->path_hash());

            tablet.tablet_infos.push_back(tablet_info);
        }

        if (tablet.tablet_infos.size() != 0) {
            tablets_info->insert(pair<TTabletId, TTablet>(tablet.tablet_infos[0].tablet_id, tablet));
        }
    }
    _tablet_map_lock.unlock();

    LOG(INFO) << "success to process report all tablets info. tablet_num=" << tablets_info->size();
    return OLAP_SUCCESS;
}

void StorageEngine::get_tablet_stat(TTabletStatResult& result) {
    VLOG(3) << "begin to get all tablet stat.";

    // get current time
    int64_t current_time = UnixMillis();
    
    _tablet_map_lock.wrlock();
    // update cache if too old
    if (current_time - _tablet_stat_cache_update_time_ms > 
        config::tablet_stat_cache_update_interval_second * 1000) {
        VLOG(3) << "update tablet stat.";
        _build_tablet_stat();
    }

    result.__set_tablets_stats(_tablet_stat_cache);

    _tablet_map_lock.unlock();
}

void StorageEngine::_build_tablet_stat() {
    _tablet_stat_cache.clear();
    for (const auto& item : _tablet_map) {
        if (item.second.table_arr.size() == 0) {
            continue;
        }

        TTabletStat stat;
        stat.tablet_id = item.first;
        for (TabletSharedPtr tablet : item.second.table_arr) {
            if (tablet.get() == NULL) {
                continue;
            }
                
            // we only get base tablet's stat
            stat.__set_data_size(tablet->get_data_size());
            stat.__set_row_num(tablet->get_num_rows());
            VLOG(3) << "tablet_id=" << item.first 
                    << ", data_size=" << tablet->get_data_size()
                    << ", row_num:" << tablet->get_num_rows();
            break;
        }

        _tablet_stat_cache.emplace(item.first, stat);
    }

    _tablet_stat_cache_update_time_ms = UnixMillis();
}

bool StorageEngine::_can_do_compaction(TabletSharedPtr tablet) {
    // 如果table正在做schema change，则通过选路判断数据是否转换完成
    // 如果选路成功，则转换完成，可以进行BE
    // 如果选路失败，则转换未完成，不能进行BE
    tablet->obtain_header_rdlock();
    const PDelta* lastest_version = tablet->lastest_version();
    if (lastest_version == NULL) {
        tablet->release_header_lock();
        return false;
    }

    if (tablet->is_schema_changing()) {
        Version test_version = Version(0, lastest_version->end_version());
        vector<Version> path_versions;
        if (OLAP_SUCCESS != tablet->select_versions_to_span(test_version, &path_versions)) {
            tablet->release_header_lock();
            return false;
        }
    }
    tablet->release_header_lock();

    return true;
}

void StorageEngine::start_clean_fd_cache() {
    VLOG(10) << "start clean file descritpor cache";
    FileHandler::get_fd_cache()->prune();
    VLOG(10) << "end clean file descritpor cache";
}

void StorageEngine::perform_cumulative_compaction() {
    TabletSharedPtr best_tablet = _find_best_tablet_to_compaction(CompactionType::CUMULATIVE_COMPACTION);
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
    TabletSharedPtr best_tablet = _find_best_tablet_to_compaction(CompactionType::BASE_COMPACTION);
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

TabletSharedPtr StorageEngine::_find_best_tablet_to_compaction(CompactionType compaction_type) {
    ReadLock tablet_map_rdlock(&_tablet_map_lock);
    uint32_t highest_score = 0;
    TabletSharedPtr best_tablet;
    for (tablet_map_t::value_type& table_ins : _tablet_map){
        for (TabletSharedPtr& table_ptr : table_ins.second.table_arr) {
            if (!table_ptr->is_loaded() || !_can_do_compaction(table_ptr)) {
                continue;
            }

            ReadLock rdlock(table_ptr->get_header_lock_ptr());
            uint32_t table_score = 0;
            if (compaction_type == CompactionType::BASE_COMPACTION) {
                table_score = table_ptr->get_base_compaction_score();
            } else if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                table_score = table_ptr->get_cumulative_compaction_score();
            }
            if (table_score > highest_score) {
                highest_score = table_score;
                best_tablet = table_ptr;
            }
        }
    }
    return best_tablet;
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
    std::vector<RootPathInfo> root_paths_info;
    res = get_all_root_path_info(&root_paths_info);
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

    for (RootPathInfo& info : root_paths_info) {
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
    _tablet_map_lock.rdlock();
    for (const auto& item : _tablet_map) {
        for (TabletSharedPtr tablet : item.second.table_arr) {
            if (tablet.get() == NULL) {
                continue;
            }
            tablet->delete_expire_incremental_data();
        }
    }
    _tablet_map_lock.unlock();

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

OLAPStatus StorageEngine::_create_new_tablet_header(
        const TCreateTabletReq& request,
        OlapStore* store,
        const bool is_schema_change_tablet,
        const TabletSharedPtr ref_tablet,
        OLAPHeader* header) {
    uint64_t shard = 0;
    OLAPStatus res = store->get_shard(&shard);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to get root path shard. res=" << res;
        return res;
    }
    stringstream schema_hash_dir_stream;
    schema_hash_dir_stream << store->path()
                      << DATA_PREFIX
                      << "/" << shard
                      << "/" << request.tablet_id
                      << "/" << request.tablet_schema.schema_hash;
    string schema_hash_dir = schema_hash_dir_stream.str();
    if (check_dir_existed(schema_hash_dir)) {
        LOG(WARNING) << "failed to create the dir that existed. path=" << schema_hash_dir;
        return OLAP_ERR_CANNOT_CREATE_DIR;
    }
    res = create_dirs(schema_hash_dir);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "create dir fail. [res=" << res << " path:" << schema_hash_dir;
        return res;
    }
    
    // set basic information
    header->set_num_short_key_fields(request.tablet_schema.short_key_column_count);
    header->set_compress_kind(COMPRESS_LZ4);
    if (request.tablet_schema.keys_type == TKeysType::DUP_KEYS) {
        header->set_keys_type(KeysType::DUP_KEYS);
    } else if (request.tablet_schema.keys_type == TKeysType::UNIQUE_KEYS) {
        header->set_keys_type(KeysType::UNIQUE_KEYS);
    } else {
        header->set_keys_type(KeysType::AGG_KEYS);
    }
    DCHECK(request.tablet_schema.storage_type == TStorageType::COLUMN);
    header->set_data_file_type(COLUMN_ORIENTED_FILE);
    header->set_segment_size(OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE);
    header->set_num_rows_per_data_block(config::default_num_rows_per_column_file_block);

    // set column information
    uint32_t i = 0;
    uint32_t key_count = 0;
    bool has_bf_columns = false;
    uint32_t next_unique_id = 0;
    if (true == is_schema_change_tablet) {
        next_unique_id = ref_tablet->next_unique_id();
    }
    for (TColumn column : request.tablet_schema.columns) {
        if (column.column_type.type == TPrimitiveType::VARCHAR
                && i < request.tablet_schema.short_key_column_count - 1) {
            LOG(WARNING) << "varchar type column should be the last short key.";
            return OLAP_ERR_SCHEMA_SCHEMA_INVALID;
        }
        header->add_column();
        if (true == is_schema_change_tablet) {
            /*
             * for schema change, compare old_tablet and new_tablet
             * 1. if column in both new_tablet and old_tablet,
             * assign unique_id of old_tablet to the column of new_tablet
             * 2. if column exists only in new_tablet, assign next_unique_id of old_tablet
             * to the new column
             *
            */
            size_t field_num = ref_tablet->tablet_schema().size();
            size_t field_off = 0;
            for (field_off = 0; field_off < field_num; ++field_off) {
                if (ref_tablet->tablet_schema()[field_off].name == column.column_name) {
                    uint32_t unique_id = ref_tablet->tablet_schema()[field_off].unique_id;
                    header->mutable_column(i)->set_unique_id(unique_id);
                    break;
                }
            }
            if (field_off == field_num) {
                header->mutable_column(i)->set_unique_id(next_unique_id++);
            }
        } else {
            header->mutable_column(i)->set_unique_id(i);
        }
        header->mutable_column(i)->set_name(column.column_name);
        header->mutable_column(i)->set_is_root_column(true);
        string data_type;
        EnumToString(TPrimitiveType, column.column_type.type, data_type);
        header->mutable_column(i)->set_type(data_type);
        if (column.column_type.type == TPrimitiveType::DECIMAL) {
            if (column.column_type.__isset.precision && column.column_type.__isset.scale) {
                header->mutable_column(i)->set_precision(column.column_type.precision);
                header->mutable_column(i)->set_frac(column.column_type.scale);
            } else {
                LOG(WARNING) << "decimal type column should set precision and frac.";
                return OLAP_ERR_SCHEMA_SCHEMA_INVALID;
            }
        }
        if (column.column_type.type == TPrimitiveType::CHAR
                || column.column_type.type == TPrimitiveType::VARCHAR || column.column_type.type == TPrimitiveType::HLL) {
            if (!column.column_type.__isset.len) {
                LOG(WARNING) << "CHAR or VARCHAR should specify length. type=" << column.column_type.type;
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }
        }
        uint32_t length = FieldInfo::get_field_length_by_type(
                column.column_type.type, column.column_type.len);
        header->mutable_column(i)->set_length(length);
        header->mutable_column(i)->set_index_length(length);
        if (column.column_type.type == TPrimitiveType::VARCHAR || column.column_type.type == TPrimitiveType::HLL) {
            if (!column.column_type.__isset.index_len) {
                header->mutable_column(i)->set_index_length(10);
            } else {
                header->mutable_column(i)->set_index_length(column.column_type.index_len);
            }
        }
        if (!column.is_key) {
            header->mutable_column(i)->set_is_key(false);
            string aggregation_type;
            EnumToString(TAggregationType, column.aggregation_type, aggregation_type);
            header->mutable_column(i)->set_aggregation(aggregation_type);
        } else {
            ++key_count;
            header->add_selectivity(1);
            header->mutable_column(i)->set_is_key(true);
            header->mutable_column(i)->set_aggregation("NONE");
        }
        if (column.__isset.default_value) {
            header->mutable_column(i)->set_default_value(column.default_value);
        }
        if (column.__isset.is_allow_null) {
            header->mutable_column(i)->set_is_allow_null(column.is_allow_null);
        } else {
            header->mutable_column(i)->set_is_allow_null(false);
        }
        if (column.__isset.is_bloom_filter_column) {
            header->mutable_column(i)->set_is_bf_column(column.is_bloom_filter_column);
            has_bf_columns = true;
        }
        ++i;
    }
    if (true == is_schema_change_tablet){
        /*
         * for schema change, next_unique_id of new tablet should be greater than
         * next_unique_id of old tablet
         * */
        header->set_next_column_unique_id(next_unique_id);
    } else {
        header->set_next_column_unique_id(i);
    }
    if (has_bf_columns && request.tablet_schema.__isset.bloom_filter_fpp) {
        header->set_bf_fpp(request.tablet_schema.bloom_filter_fpp);
    }
    if (key_count < request.tablet_schema.short_key_column_count) {
        LOG(WARNING) << "short key num should not large than key num. "
                << "key_num=" << key_count << " short_key_num=" << request.tablet_schema.short_key_column_count;
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    header->set_creation_time(time(NULL));
    header->set_cumulative_layer_point(-1);
    header->set_tablet_id(request.tablet_id);
    header->set_schema_hash(request.tablet_schema.schema_hash);
    header->set_shard(shard);
    return OLAP_SUCCESS;
}

OLAPStatus StorageEngine::_check_existed_or_else_create_dir(const string& path) {
    if (check_dir_existed(path)) {
        LOG(WARNING) << "failed to create the dir that existed. [path='" << path << "']";
        return OLAP_ERR_CANNOT_CREATE_DIR;
    }

    return create_dirs(path);
}

void StorageEngine::_cancel_unfinished_schema_change() {
    // Schema Change在引擎退出时schemachange信息还保存在在Header里，
    // 引擎重启后，需清除schemachange信息，上层会重做
    uint64_t canceled_num = 0;
    LOG(INFO) << "begin to cancel unfinished schema change.";

    SchemaChangeHandler schema_change_handler;
    TTabletId tablet_id;
    TSchemaHash schema_hash;
    vector<Version> schema_change_versions;
    AlterTabletType type;

    for (const auto& tablet_instance : _tablet_map) {
        for (TabletSharedPtr tablet : tablet_instance.second.table_arr) {
            if (tablet.get() == NULL) {
                OLAP_LOG_WARNING("get empty TabletSharedPtr. [tablet_id=%ld]", tablet_instance.first);
                continue;
            }

            bool ret = tablet->get_schema_change_request(
                    &tablet_id, &schema_hash, &schema_change_versions, &type);
            if (!ret) {
                continue;
            }

            TabletSharedPtr new_tablet = get_tablet(tablet_id, schema_hash, false);
            if (new_tablet.get() == NULL) {
                OLAP_LOG_WARNING("the tablet referenced by schema change cannot be found. "
                                 "schema change cancelled. [tablet='%s']",
                                 tablet->full_name().c_str());
                continue;
            }

            // DORIS-3741. Upon restart, it should not clear schema change request.
            new_tablet->set_schema_change_status(
                    ALTER_TABLE_FAILED, new_tablet->schema_hash(), -1);
            tablet->set_schema_change_status(
                    ALTER_TABLE_FAILED, tablet->schema_hash(), -1);
            VLOG(3) << "cancel unfinished schema change. tablet=" << tablet->full_name();
            ++canceled_num;
        }
    }

    LOG(INFO) << "finish to cancel unfinished schema change! canceled_num=" << canceled_num;
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

OLAPStatus StorageEngine::_create_init_version(
        TabletSharedPtr tablet, const TCreateTabletReq& request) {
    OLAPStatus res = OLAP_SUCCESS;

    if (request.version < 1) {
        OLAP_LOG_WARNING("init version of tablet should at least 1.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    } else {
        Version init_base_version(0, request.version);
        res = create_init_version(
                request.tablet_id, request.tablet_schema.schema_hash,
                init_base_version, request.version_hash);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create init base version. [res=%d version=%ld]",
                    res, request.version);
            return res;
        }

        Version init_delta_version(request.version + 1, request.version + 1);
        res = create_init_version(
                request.tablet_id, request.tablet_schema.schema_hash,
                init_delta_version, 0);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create init delta version. [res=%d version=%ld]",
                    res, request.version + 1);
            return res;
        }
    }

    tablet->obtain_header_wrlock();
    tablet->set_cumulative_layer_point(request.version + 1);
    res = tablet->save_header();
    tablet->release_header_lock();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to save header. [tablet=" << tablet->full_name() << "]";
    }

    return res;
}

// TODO(zc): refactor this funciton
OLAPStatus StorageEngine::create_tablet(const TCreateTabletReq& request) {
    OLAPStatus res = OLAP_SUCCESS;
    bool is_tablet_added = false;

    LOG(INFO) << "begin to process create tablet. tablet=" << request.tablet_id
              << ", schema_hash=" << request.tablet_schema.schema_hash;

    DorisMetrics::create_tablet_requests_total.increment(1);

    // 1. Make sure create_tablet operation is idempotent:
    //    return success if tablet with same tablet_id and schema_hash exist,
    //           false if tablet with same tablet_id but different schema_hash exist
    if (check_tablet_id_exist(request.tablet_id)) {
        TabletSharedPtr tablet = get_tablet(
                request.tablet_id, request.tablet_schema.schema_hash);
        if (tablet.get() != NULL) {
            LOG(INFO) << "create tablet success for tablet already exist.";
            return OLAP_SUCCESS;
        } else {
            OLAP_LOG_WARNING("tablet with different schema hash already exists.");
            return OLAP_ERR_CE_TABLET_ID_EXIST;
        }
    }

    // 2. Lock to ensure that all create_tablet operation execute in serial
    static Mutex create_tablet_lock;
    MutexLock auto_lock(&create_tablet_lock);

    TabletSharedPtr tablet;
    do {
        // 3. Create tablet with only header, no deltas
        tablet = create_tablet(request, NULL, false, NULL);
        if (tablet == NULL) {
            res = OLAP_ERR_CE_CMD_PARAMS_ERROR;
            OLAP_LOG_WARNING("fail to create tablet. [res=%d]", res);
            break;
        }

        // 4. Add tablet to StorageEngine will make it visiable to user
        res = add_tablet(
                request.tablet_id, request.tablet_schema.schema_hash, tablet);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to add tablet to StorageEngine. [res=%d]", res);
            break;
        }
        is_tablet_added = true;

        TabletSharedPtr tablet_ptr = get_tablet(
                request.tablet_id, request.tablet_schema.schema_hash);
        if (tablet_ptr.get() == NULL) {
            res = OLAP_ERR_TABLE_NOT_FOUND;
            OLAP_LOG_WARNING("fail to get tablet. [res=%d]", res);
            break;
        }

        // 5. Register tablet into StorageEngine, so that we can manage tablet from
        // the perspective of root path.
        // Example: unregister all tables when a bad disk found.
        res = register_tablet_into_root_path(tablet_ptr.get());
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to register tablet into StorageEngine. [res=%d, root_path=%s]",
                    res, tablet_ptr->storage_root_path_name().c_str());
            break;
        }

        // 6. Create init version if this is not a restore mode replica and request.version is set
        // bool in_restore_mode = request.__isset.in_restore_mode && request.in_restore_mode;
        // if (!in_restore_mode && request.__isset.version) {
        res = _create_init_version(tablet_ptr, request);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create initial version for tablet. [res=%d]", res);
        }
        // }
    } while (0);

    // 7. clear environment
    if (res != OLAP_SUCCESS) {
        DorisMetrics::create_tablet_requests_failed.increment(1);
        if (is_tablet_added) {
            OLAPStatus status = drop_tablet(
                    request.tablet_id, request.tablet_schema.schema_hash);
            if (status !=  OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to drop tablet when create tablet failed. [res=%d]", res);
            }
        } else if (NULL != tablet) {
            tablet->delete_all_files();
        }
    }

    LOG(INFO) << "finish to process create tablet. res=" << res;
    return res;
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
    LOG(INFO) << "begin to create rollup tablet. "
              << "old_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id;

    DorisMetrics::create_rollup_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    SchemaChangeHandler handler;
    res = handler.process_alter_tablet(ALTER_TABLET_CREATE_ROLLUP_TABLE, request);

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to do rollup. "
                         "[base_tablet=%ld new_tablet=%ld] [res=%d]",
                         request.base_tablet_id, request.new_tablet_req.tablet_id, res);
        DorisMetrics::create_rollup_requests_failed.increment(1);
        return res;
    }

    LOG(INFO) << "success to create rollup tablet. res=" << res
              << ", old_tablet_id=" << request.base_tablet_id 
              << ", new_tablet_id=" << request.new_tablet_req.tablet_id;
    return res;
}

AlterTableStatus StorageEngine::show_alter_tablet_status(
        TTabletId tablet_id,
        TSchemaHash schema_hash) {
    LOG(INFO) << "begin to process show alter tablet status."
              << "tablet_id" << tablet_id
              << ", schema_hash" << schema_hash;

    AlterTableStatus status = ALTER_TABLE_FINISHED;

    TabletSharedPtr tablet = StorageEngine::get_instance()->get_tablet(tablet_id, schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("fail to get tablet. [tablet=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        status = ALTER_TABLE_FAILED;
    } else {
        status = tablet->schema_change_status().status;
    }

    return status;
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

        res = temp_tablet->save_header();
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
        OLAPHeader clone_header(clone_header_file);
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
            res = StorageEngine::get_instance()->clone_incremental_data(
                                              tablet, clone_header, committed_version);
        } else {
            res = StorageEngine::get_instance()->clone_full_data(tablet, clone_header);
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
        TStorageMedium::type storage_medium, std::string* shard_path, OlapStore** store) {
    LOG(INFO) << "begin to process obtain root path. storage_medium=" << storage_medium;
    OLAPStatus res = OLAP_SUCCESS;

    if (shard_path == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    auto stores = StorageEngine::get_instance()->get_stores_for_create_tablet(storage_medium);
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

    OlapStore* store = nullptr;
    {
        // TODO(zc)
        try {
            auto store_path =
                boost::filesystem::path(shard_path).parent_path().parent_path().string();
            store = StorageEngine::get_instance()->get_store(store_path);
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
    res =  StorageEngine::get_instance()->load_one_tablet(
            store,
            request.tablet_id, request.schema_hash,
            schema_hash_path_stream.str());
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process load headers. [res=%d]", res);
        return res;
    }

    LOG(INFO) << "success to process load headers.";
    return res;
}

OLAPStatus StorageEngine::load_header(
        OlapStore* store,
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
    res =  StorageEngine::get_instance()->load_one_tablet(
            store,
            tablet_id, schema_hash,
            schema_hash_path_stream.str());
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
    OLAPStatus res = tablet->save_header();
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
        res = related_tablet->save_header();
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

    TabletSharedPtr tablet = StorageEngine::get_instance()->get_tablet(
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
