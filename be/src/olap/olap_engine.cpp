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

#include "olap/olap_engine.h"

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

OLAPEngine* OLAPEngine::_s_instance = nullptr;
const std::string HTTP_REQUEST_PREFIX = "/api/_tablet/_download?";
const std::string HTTP_REQUEST_TOKEN_PARAM = "token=";
const std::string HTTP_REQUEST_FILE_PARAM = "&file=";

const uint32_t DOWNLOAD_FILE_MAX_RETRY = 3;
const uint32_t LIST_REMOTE_FILE_TIMEOUT = 15;

bool _sort_table_by_create_time(const OLAPTablePtr& a, const OLAPTablePtr& b) {
    return a->creation_time() < b->creation_time();
}

static Status _validate_options(const EngineOptions& options) {
    if (options.store_paths.empty()) {
        return Status("store paths is empty");;
    }
    return Status::OK;
}

Status OLAPEngine::open(const EngineOptions& options, OLAPEngine** engine_ptr) {
    RETURN_IF_ERROR(_validate_options(options));
    std::unique_ptr<OLAPEngine> engine(new OLAPEngine(options));
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

OLAPEngine::OLAPEngine(const EngineOptions& options)
        : _options(options),
        _available_storage_medium_type_count(0),
        _effective_cluster_id(-1),
        _is_all_cluster_id_exist(true),
        _is_drop_tables(false),
        _global_table_id(0),
        _index_stream_lru_cache(NULL),
        _tablet_stat_cache_update_time_ms(0),
        _snapshot_base_id(0),
        _is_report_disk_state_already(false),
        _is_report_olap_table_already(false) {
    if (_s_instance == nullptr) {
        _s_instance = this;
    }
}

OLAPEngine::~OLAPEngine() {
    clear();
}

OLAPStatus OLAPEngine::_load_store(OlapStore* store) {
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
            LOG(WARNING) << "there is failure when loading table headers, path:" << store_path;
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
            // 遍历table目录寻找此table的所有indexedRollupTable，注意不是Rowset，而是OLAPTable
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
                    OLAP_LOG_WARNING("fail to load one table, but continue. [path='%s']",
                                     (one_tablet_path + '/' + schema_hash).c_str());
                }
            }
        }
    }
    res = OlapHeaderManager::set_converted_flag(store);
    LOG(INFO) << "load header from header files finished";
    return res;
}

OLAPStatus OLAPEngine::load_one_tablet(
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

    auto olap_table = OLAPTable::create_from_header_file(
            tablet_id, schema_hash, header_path, store);
    if (olap_table == NULL) {
        LOG(WARNING) << "fail to load table. [header_path=" << header_path << "]";
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    if (olap_table->lastest_version() == NULL && !olap_table->is_schema_changing()) {
        OLAP_LOG_WARNING("tablet not in schema change state without delta is invalid. "
                         "[header_path=%s]",
                         header_path.c_str());
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    // 这里不需要SAFE_DELETE(olap_table),因为olap_table指针已经在add_table中托管到smart pointer中
    OLAPStatus res = OLAP_SUCCESS;
    string table_name = olap_table->full_name();
    res = add_table(tablet_id, schema_hash, olap_table, force);
    if (res != OLAP_SUCCESS) {
        // 插入已经存在的table时返回成功
        if (res == OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE) {
            return OLAP_SUCCESS;
        }

        LOG(WARNING) << "failed to add table. [table=" << table_name << "]";
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    if (register_table_into_root_path(olap_table.get()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to register table into root path. [root_path=%s]",
                         schema_hash_path.c_str());

        if (OLAPEngine::get_instance()->drop_table(tablet_id, schema_hash) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to drop table when create table failed. "
                             "[tablet=%ld schema_hash=%d]",
                             tablet_id, schema_hash);
        }

        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    // load pending data (for realtime push), will add transaction relationship into engine
    olap_table->load_pending_data();

    OLAP_LOG_DEBUG("succeed to add table. [table=%s, path=%s]",
                   olap_table->full_name().c_str(),
                   schema_hash_path.c_str());
    return OLAP_SUCCESS;
}

void OLAPEngine::check_none_row_oriented_table(const std::vector<OlapStore*>& stores) {
    for (auto store : stores) {
        auto res = _check_none_row_oriented_table_in_store(store);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "io error when init load tables. res=" << res
                << ", store=" << store->path();
        }
    }
}

OLAPStatus OLAPEngine::_check_none_row_oriented_table_in_store(OlapStore* store) {
    std::string store_path = store->path();
    LOG(INFO) <<"start to load tablets from store_path:" << store_path;

    bool is_header_converted = false;
    OLAPStatus res = OlapHeaderManager::get_header_converted(store, is_header_converted);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "get convert flag from meta failed";
        return res;
    }
    if (is_header_converted) {
        OLAPStatus s = store->check_none_row_oriented_table_in_store(this);
        if (s != OLAP_SUCCESS) {
            LOG(WARNING) << "there is failure when loading table headers, path:" << store_path;
            return s;
        } else {
            return OLAP_SUCCESS;
        }
    }

    // compatible for old header load method
    // walk all directory to load header file
    LOG(INFO) << "check has none row-oriented table from header files";

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
            // 遍历table目录寻找此table的所有indexedRollupTable，注意不是Rowset，而是OLAPTable
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
                if (check_none_row_oriented_table_in_path(
                        store,
                        tablet_id,
                        tablet_schema_hash,
                        one_tablet_path + '/' + schema_hash) != OLAP_SUCCESS) {
                    OLAP_LOG_WARNING("fail to load one table, but continue. [path='%s']",
                                     (one_tablet_path + '/' + schema_hash).c_str());
                }
            }
        }
    }
    return res;
}

OLAPStatus OLAPEngine::check_none_row_oriented_table_in_path(
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

    auto olap_table = OLAPTable::create_from_header_file_for_check(
            tablet_id, schema_hash, header_path);
    if (olap_table == NULL) {
        LOG(WARNING) << "fail to load table. [header_path=" << header_path << "]";
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    LOG(INFO) << "data_file_type:" << olap_table->data_file_type();
    if (olap_table->data_file_type() == OLAP_DATA_FILE) {
        LOG(FATAL) << "Not support row-oriented table any more. Please convert it to column-oriented table."
                   << "tablet=" << olap_table->full_name();
    }

    return OLAP_SUCCESS;
}

void OLAPEngine::load_stores(const std::vector<OlapStore*>& stores) {
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

OLAPStatus OLAPEngine::open() {
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
    check_none_row_oriented_table(stores);
    load_stores(stores);
    // 取消未完成的SchemaChange任务
    _cancel_unfinished_schema_change();

    return OLAP_SUCCESS;
}

void OLAPEngine::_update_storage_medium_type_count() {
    set<TStorageMedium::type> available_storage_medium_types;

    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        if (it.second->is_used()) {
            available_storage_medium_types.insert(it.second->storage_medium());
        }
    }

    _available_storage_medium_type_count = available_storage_medium_types.size();
}


OLAPStatus OLAPEngine::_judge_and_update_effective_cluster_id(int32_t cluster_id) {
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

void OLAPEngine::set_store_used_flag(const string& path, bool is_used) {
    std::lock_guard<std::mutex> l(_store_lock);
    auto it = _store_map.find(path);
    if (it == _store_map.end()) {
        LOG(WARNING) << "store not exist, path=" << path;
    }

    it->second->set_is_used(is_used);
    _update_storage_medium_type_count();
}

void OLAPEngine::get_all_available_root_path(std::vector<std::string>* available_paths) {
    available_paths->clear();
    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        if (it.second->is_used()) {
            available_paths->push_back(it.first);
        }
    }
}

template<bool include_unused>
std::vector<OlapStore*> OLAPEngine::get_stores() {
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

template std::vector<OlapStore*> OLAPEngine::get_stores<false>();
template std::vector<OlapStore*> OLAPEngine::get_stores<true>();

OLAPStatus OLAPEngine::get_all_root_path_info(vector<RootPathInfo>* root_paths_info) {
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

OLAPStatus OLAPEngine::register_table_into_root_path(OLAPTable* olap_table) {
    return olap_table->store()->register_table(olap_table);
}

void OLAPEngine::start_disk_stat_monitor() {
    for (auto& it : _store_map) {
        it.second->health_check();
    }
    _update_storage_medium_type_count();
    _delete_tables_on_unused_root_path();
    
    // if drop tables
    // notify disk_state_worker_thread and olap_table_worker_thread until they received
    if (_is_drop_tables) {
        report_notify(true);

        bool is_report_disk_state_expected = true;
        bool is_report_olap_table_expected = true;
        bool is_report_disk_state_exchanged = 
                _is_report_disk_state_already.compare_exchange_strong(is_report_disk_state_expected, false);
        bool is_report_olap_table_exchanged =
                _is_report_olap_table_already.compare_exchange_strong(is_report_olap_table_expected, false);
        if (is_report_disk_state_exchanged && is_report_olap_table_exchanged) {
            _is_drop_tables = false;
        }
    }
}

bool OLAPEngine::_used_disk_not_enough(uint32_t unused_num, uint32_t total_num) {
    return ((total_num == 0) || (unused_num * 100 / total_num > _min_percentage_of_error_disk));
}

OLAPStatus OLAPEngine::check_all_root_path_cluster_id() {
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

Status OLAPEngine::set_cluster_id(int32_t cluster_id) {
    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        RETURN_IF_ERROR(it.second->set_cluster_id(cluster_id));
    }
    _effective_cluster_id = cluster_id;
    _is_all_cluster_id_exist = true;
    return Status::OK;
}

std::vector<OlapStore*> OLAPEngine::get_stores_for_create_table(
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

OlapStore* OLAPEngine::get_store(const std::string& path) {
    std::lock_guard<std::mutex> l(_store_lock);
    auto it = _store_map.find(path);
    if (it == std::end(_store_map)) {
        return nullptr;
    }
    return it->second;
}

void OLAPEngine::_delete_tables_on_unused_root_path() {
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
        OLAP_LOG_FATAL("engine stop running, because more than %d disks error."
                       "[total_disks=%d error_disks=%d]",
                       _min_percentage_of_error_disk,
                       total_root_path_num,
                       unused_root_path_num);
        exit(0);
    }

    if (!tablet_info_vec.empty()) {
        _is_drop_tables = true;
    }
    
    OLAPEngine::get_instance()->drop_tables_on_error_root_path(tablet_info_vec);
}

OLAPStatus OLAPEngine::_get_path_available_capacity(
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

OLAPStatus OLAPEngine::clear() {
    // 删除lru中所有内容,其实进程退出这么做本身意义不大,但对单测和更容易发现问题还是有很大意义的
    delete FileHandler::get_fd_cache();
    FileHandler::set_fd_cache(nullptr);
    SAFE_DELETE(_index_stream_lru_cache);

    _tablet_map.clear();
    _transaction_tablet_map.clear();
    _global_table_id = 0;

    return OLAP_SUCCESS;
}

OLAPTablePtr OLAPEngine::_get_table_with_no_lock(TTabletId tablet_id, SchemaHash schema_hash) {
    OLAP_LOG_DEBUG("begin to get olap table. [table=%ld]", tablet_id);

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end()) {
        for (OLAPTablePtr table : it->second.table_arr) {
            if (table->equal(tablet_id, schema_hash)) {
                OLAP_LOG_DEBUG("get olap table success. [table=%ld]", tablet_id);
                return table;
            }
        }
    }

    OLAP_LOG_DEBUG("fail to get olap table. [table=%ld]", tablet_id);
    // Return empty olap_table if fail
    OLAPTablePtr olap_table;
    return olap_table;
}

OLAPTablePtr OLAPEngine::get_table(TTabletId tablet_id, SchemaHash schema_hash, bool load_table) {
    _tablet_map_lock.rdlock();
    OLAPTablePtr olap_table;
    olap_table = _get_table_with_no_lock(tablet_id, schema_hash);
    _tablet_map_lock.unlock();

    if (olap_table.get() != NULL) {
        if (!olap_table->is_used()) {
            OLAP_LOG_WARNING("olap table cannot be used. [table=%ld]", tablet_id);
            olap_table.reset();
        } else if (load_table && !olap_table->is_loaded()) {
            if (olap_table->load() != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to load olap table. [table=%ld]", tablet_id);
                olap_table.reset();
            }
        }
    }

    return olap_table;
}

OLAPStatus OLAPEngine::get_tables_by_id(
        TTabletId tablet_id,
        list<OLAPTablePtr>* table_list) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAP_LOG_DEBUG("begin to get tables by id. [table=%ld]", tablet_id);

    _tablet_map_lock.rdlock();
    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end()) {
        for (OLAPTablePtr olap_table : it->second.table_arr) {
            table_list->push_back(olap_table);
        }
    }
    _tablet_map_lock.unlock();

    if (table_list->size() == 0) {
        OLAP_LOG_WARNING("there is no tablet with specified id. [table=%ld]", tablet_id);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    for (std::list<OLAPTablePtr>::iterator it = table_list->begin();
            it != table_list->end();) {
        if (!(*it)->is_loaded()) {
            if ((*it)->load() != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to load table. [table='%s']",
                                 (*it)->full_name().c_str());
                it = table_list->erase(it);
                continue;
            }
        }
        ++it;
    }

    OLAP_LOG_DEBUG("success to get tables by id. [table_num=%u]", table_list->size());
    return res;
}

bool OLAPEngine::check_tablet_id_exist(TTabletId tablet_id) {
    bool is_exist = false;
    _tablet_map_lock.rdlock();

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end() && it->second.table_arr.size() != 0) {
        is_exist = true;
    }

    _tablet_map_lock.unlock();
    return is_exist;
}

OLAPStatus OLAPEngine::add_table(TTabletId tablet_id, SchemaHash schema_hash,
                                 const OLAPTablePtr& table, bool force) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAP_LOG_DEBUG("begin to add olap table to OLAPEngine. [tablet_id=%ld schema_hash=%d], force: %d",
                   tablet_id, schema_hash, force);
    _tablet_map_lock.wrlock();

    table->set_id(_global_table_id++);

    OLAPTablePtr table_item;
    for (OLAPTablePtr item : _tablet_map[tablet_id].table_arr) {
        if (item->equal(tablet_id, schema_hash)) {
            table_item = item;
            break;
        }
    }

    if (table_item.get() == NULL) {
        _tablet_map[tablet_id].table_arr.push_back(table);
        _tablet_map[tablet_id].table_arr.sort(_sort_table_by_create_time);
        _tablet_map_lock.unlock();

        return res;
    }
    _tablet_map_lock.unlock();

    if (!force) {
        if (table_item->tablet_path() == table->tablet_path()) {
            LOG(WARNING) << "add the same tablet twice! tablet_id="
                << tablet_id << " schema_hash=" << tablet_id;
            return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
        }
    }

    table_item->obtain_header_rdlock();
    int64_t old_time = table_item->lastest_version()->creation_time();
    int64_t new_time = table->lastest_version()->creation_time();
    int32_t old_version = table_item->lastest_version()->end_version();
    int32_t new_version = table->lastest_version()->end_version();
    table_item->release_header_lock();

    /*
     * In restore process, we replace all origin files in tablet dir with
     * the downloaded snapshot files. Than we try to reload tablet header.
     * force == true means we forcibly replace the OLAPTable in _tablet_map
     * with the new one. But if we do so, the files in the tablet dir will be
     * dropped when the origin OLAPTable deconstruct.
     * So we set keep_files == true to not delete files when the
     * origin OLAPTable deconstruct.
     */
    bool keep_files = force ? true : false;
    if (force || (new_version > old_version
            || (new_version == old_version && new_time > old_time))) {
        drop_table(tablet_id, schema_hash, keep_files);
        _tablet_map_lock.wrlock();
        _tablet_map[tablet_id].table_arr.push_back(table);
        _tablet_map[tablet_id].table_arr.sort(_sort_table_by_create_time);
        _tablet_map_lock.unlock();
    } else {
        table->mark_dropped();
        res = OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
    }
    LOG(WARNING) << "add duplicated table. force=" << force << ", res=" << res
            << ", tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
            << ", old_version=" << old_version << ", new_version=" << new_version
            << ", old_time=" << old_time << ", new_time=" << new_time
            << ", old_tablet_path=" << table_item->tablet_path()
            << ", new_tablet_path=" << table->tablet_path();

    return res;
}

OLAPStatus OLAPEngine::add_transaction(
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
                        << "partition_id: " << key.first << ", "
                        << "transaction_id: " << key.second << ", "
                        << "table: " << tablet_info.to_string();
                    return OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST;
                }
            }
        }
    }

    _transaction_tablet_map[key][tablet_info].push_back(load_id);
    VLOG(3) << "add transaction to engine successfully."
        << "partition_id: " << key.first << ", "
        << "transaction_id: " << key.second << ", "
        << "table: " << tablet_info.to_string();
    return OLAP_SUCCESS;
}

void OLAPEngine::delete_transaction(
    TPartitionId partition_id, TTransactionId transaction_id,
    TTabletId tablet_id, SchemaHash schema_hash, bool delete_from_tablet) {

    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash);
    WriteLock wrlock(&_transaction_tablet_map_lock);

    auto it = _transaction_tablet_map.find(key);
    if (it != _transaction_tablet_map.end()) {
        VLOG(3) << "delete transaction to engine successfully."
            << "partition_id: " << key.first << ", "
            << "transaction_id: " << key.second << ", "
            << "table: " << tablet_info.to_string();
        it->second.erase(tablet_info);
        if (it->second.empty()) {
            _transaction_tablet_map.erase(it);
        }

        // delete transaction from tablet
        if (delete_from_tablet) {
            OLAPTablePtr tablet = get_table(tablet_info.tablet_id, tablet_info.schema_hash);
            if (tablet.get() != nullptr) {
                tablet->delete_pending_data(transaction_id);
            }
        }
    }
}

void OLAPEngine::get_transactions_by_tablet(OLAPTablePtr tablet, int64_t* partition_id,
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
                << "partition_id: " << it.first.first << ", "
                << "transaction_id: " << it.first.second << ", "
                << "table: " << tablet_info.to_string();
        }
    }
}

bool OLAPEngine::has_transaction(TPartitionId partition_id, TTransactionId transaction_id,
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

OLAPStatus OLAPEngine::publish_version(const TPublishVersionRequest& publish_version_req,
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
            OLAP_LOG_DEBUG("begin to publish version on tablet. "
                "[tablet_id=%ld schema_hash=%d version=%d version_hash=%ld transaction_id=%ld]",
                tablet_info.tablet_id, tablet_info.schema_hash,
                version.first, version_hash, transaction_id);

            OLAPTablePtr tablet = get_table(tablet_info.tablet_id, tablet_info.schema_hash);

            if (tablet.get() == NULL) {
                OLAP_LOG_WARNING("can't get table when publish version. [tablet_id=%ld schema_hash=%d]",
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
                                 "[table=%s transaction_id=%ld version=%d]",
                                 tablet->full_name().c_str(), transaction_id, version.first);
                delete_transaction(partition_id, transaction_id,
                                   tablet->tablet_id(), tablet->schema_hash());

            // if publish successfully, delete transaction from engine
            } else if (publish_status == OLAP_SUCCESS) {
                LOG(INFO) << "publish version successfully on tablet. [table=" << tablet->full_name()
                          << " transaction_id=" << transaction_id << " version=" << version.first << "]";
                _transaction_tablet_map_lock.wrlock();
                auto it2 = _transaction_tablet_map.find(key);
                if (it2 != _transaction_tablet_map.end()) {
                    VLOG(3) << "delete transaction from engine. table=" << tablet->full_name() << ", "
                        << "transaction_id: " << transaction_id;
                    it2->second.erase(tablet_info);
                    if (it2->second.empty()) {
                        _transaction_tablet_map.erase(it2);
                    }
                }
                _transaction_tablet_map_lock.unlock();

            } else {
                OLAP_LOG_WARNING("fail to publish version on tablet. "
                                 "[table=%s transaction_id=%ld version=%d res=%d]",
                                 tablet->full_name().c_str(), transaction_id,
                                 version.first, publish_status);
                error_tablet_ids->push_back(tablet->tablet_id());
                res = publish_status;
            }
        }
    }

    OLAP_LOG_INFO("finish to publish version on transaction. "
                  "[transaction_id=%ld, error_tablet_size=%d]",
                  transaction_id, error_tablet_ids->size());
    return res;
}

void OLAPEngine::clear_transaction_task(const TTransactionId transaction_id,
                                        const vector<TPartitionId> partition_ids) {
    OLAP_LOG_INFO("begin to clear transaction task. [transaction_id=%ld]", transaction_id);

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

    OLAP_LOG_INFO("finish to clear transaction task. [transaction_id=%ld]", transaction_id);
}

OLAPStatus OLAPEngine::clone_incremental_data(OLAPTablePtr tablet, OLAPHeader& clone_header,
                                              int64_t committed_version) {
    OLAP_LOG_INFO("begin to incremental clone. [table=%s committed_version=%ld]",
                   tablet->full_name().c_str(), committed_version);

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

            OLAP_LOG_DEBUG("least complete version_hash in clone src is different, replace it. "
                    "[tablet=%s least_complete_version=%d-%d local_hash=%ld clone_hash=%ld]",
                    tablet->full_name().c_str(),
                    least_complete_version->start_version(), least_complete_version->end_version(),
                    least_complete_version->version_hash(), clone_src_version->version_hash());
        }
    }

    OLAP_LOG_DEBUG("get missing versions again when incremental clone. "
                   "[table=%s committed_version=%ld missing_versions_size=%d]",
                   tablet->full_name().c_str(), committed_version, missing_versions.size());

    // check missing versions exist in clone src
    for (Version version : missing_versions) {
        const PDelta* clone_src_version = clone_header.get_incremental_version(version);
        if (clone_src_version == NULL) {
           LOG(WARNING) << "missing version not found in clone src."
                        << "clone_header_file=" << clone_header.file_name() << ", "
                        << "missing_version=" << version.first << "-" << version.second;
            return OLAP_ERR_VERSION_NOT_EXIST;
        }

        versions_to_clone.push_back(clone_src_version);
    }

    // clone_data to tablet
    OLAPStatus clone_res = tablet->clone_data(clone_header, versions_to_clone, versions_to_delete);
    LOG(INFO) << "finish to incremental clone. [table=" << tablet->full_name() << " res=" << clone_res << "]";
    return clone_res;
}

OLAPStatus OLAPEngine::clone_full_data(OLAPTablePtr tablet, OLAPHeader& clone_header) {
    Version clone_latest_version = clone_header.get_latest_version();
    LOG(INFO) << "begin to full clone. table=" << tablet->full_name() << ","
        << "clone_latest_version=" << clone_latest_version.first << "-" << clone_latest_version.second;
    vector<Version> versions_to_delete;

    // check local versions
    for (int i = 0; i < tablet->file_delta_size(); i++) {
        Version local_version(tablet->get_delta(i)->start_version(),
                              tablet->get_delta(i)->end_version());
        VersionHash local_version_hash = tablet->get_delta(i)->version_hash();
        LOG(INFO) << "check local delta when full clone."
            << "table=" << tablet->full_name() << ", "
            << "local_version=" << local_version.first << "-" << local_version.second;

        // if local version cross src latest, clone failed
        if (local_version.first <= clone_latest_version.second
            && local_version.second > clone_latest_version.second) {
            LOG(WARNING) << "stop to full clone, version cross src latest."
                    << "table=" << tablet->full_name() << ", "
                    << "local_version=" << local_version.first << "-" << local_version.second;
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
                        << "table=" << tablet->full_name() << ", "
                        << "version='" << local_version.first<< "-" << local_version.second << ", "
                        << "version_hash=" << local_version_hash;

                    OLAPStatus delete_res = clone_header.delete_version(local_version);
                    if (delete_res != OLAP_SUCCESS) {
                        LOG(WARNING) << "failed to delete existed version from clone src when full clone. "
                                  << "clone_header_file=" << clone_header.file_name() << ", "
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
                          << "table=" << tablet->full_name() << ","
                          << "version=" << local_version.first<< "-" << local_version.second << ", "
                          << "version_hash=" << local_version_hash;
            }
        }
    }
    vector<const PDelta*> clone_deltas;
    for (int i = 0; i < clone_header.file_delta_size(); ++i) {
        clone_deltas.push_back(clone_header.get_delta(i));
        LOG(INFO) << "Delta to clone."
            << "table=" << tablet->full_name() << ","
            << "version=" << clone_header.get_delta(i)->start_version() << "-"
                << clone_header.get_delta(i)->end_version() << ", "
            << "version_hash=" << clone_header.get_delta(i)->version_hash();
    }

    // clone_data to tablet
    OLAPStatus clone_res = tablet->clone_data(clone_header, clone_deltas, versions_to_delete);
    LOG(INFO) << "finish to full clone. [table=" << tablet->full_name() << ", res=" << clone_res << "]";
    return clone_res;
}

// Drop table specified, the main logical is as follows:
// 1. table not in schema change:
//      drop specified table directly;
// 2. table in schema change:
//      a. schema change not finished && dropped table is base :
//          base table cannot be dropped;
//      b. other cases:
//          drop specified table and clear schema change info.
OLAPStatus OLAPEngine::drop_table(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    LOG(INFO) << "begin to process drop table."
        << "table=" << tablet_id << ", schema_hash=" << schema_hash;
    DorisMetrics::drop_tablet_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    // Get table which need to be droped
    _tablet_map_lock.rdlock();
    OLAPTablePtr dropped_table = _get_table_with_no_lock(tablet_id, schema_hash);
    _tablet_map_lock.unlock();
    if (dropped_table.get() == NULL) {
        OLAP_LOG_WARNING("fail to drop not existed table. [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // Try to get schema change info
    AlterTabletType type;
    TTabletId related_tablet_id;
    TSchemaHash related_schema_hash;
    vector<Version> schema_change_versions;
    dropped_table->obtain_header_rdlock();
    bool ret = dropped_table->get_schema_change_request(
            &related_tablet_id, &related_schema_hash, &schema_change_versions, &type);
    dropped_table->release_header_lock();

    // Drop table directly when not in schema change
    if (!ret) {
        return _drop_table_directly(tablet_id, schema_hash, keep_files);
    }

    // Check table is in schema change or not, is base table or not
    bool is_schema_change_finished = true;
    if (schema_change_versions.size() != 0) {
        is_schema_change_finished = false;
    }

    bool is_drop_base_table = false;
    _tablet_map_lock.rdlock();
    OLAPTablePtr related_table = _get_table_with_no_lock(
            related_tablet_id, related_schema_hash);
    _tablet_map_lock.unlock();
    if (related_table.get() == NULL) {
        OLAP_LOG_WARNING("drop table directly when related table not found. "
                         "[tablet_id=%ld schema_hash=%d]",
                         related_tablet_id, related_schema_hash);
        return _drop_table_directly(tablet_id, schema_hash, keep_files);
    }

    if (dropped_table->creation_time() < related_table->creation_time()) {
        is_drop_base_table = true;
    }

    if (is_drop_base_table && !is_schema_change_finished) {
        OLAP_LOG_WARNING("base table in schema change cannot be droped. [table=%s]",
                         dropped_table->full_name().c_str());
        return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
    }

    // Drop specified table and clear schema change info
    _tablet_map_lock.wrlock();
    related_table->obtain_header_wrlock();
    related_table->clear_schema_change_request();
    res = related_table->save_header();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_FATAL("fail to save table header. [res=%d table=%s]",
                       res, related_table->full_name().c_str());
    }

    res = _drop_table_directly_unlocked(tablet_id, schema_hash, keep_files);
    related_table->release_header_lock();
    _tablet_map_lock.unlock();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to drop table which in schema change. [table=%s]",
                         dropped_table->full_name().c_str());
        return res;
    }

    OLAP_LOG_INFO("finish to drop tablet. [res=%d]", res);
    return res;
}

OLAPStatus OLAPEngine::_drop_table_directly(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    _tablet_map_lock.wrlock();
    OLAPStatus res = _drop_table_directly_unlocked(tablet_id, schema_hash, keep_files);
    _tablet_map_lock.unlock();
    return res;
}

OLAPStatus OLAPEngine::_drop_table_directly_unlocked(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    OLAPStatus res = OLAP_SUCCESS;

    OLAPTablePtr dropped_table = _get_table_with_no_lock(tablet_id, schema_hash);
    if (dropped_table.get() == NULL) {
        OLAP_LOG_WARNING("fail to drop not existed table. [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    for (list<OLAPTablePtr>::iterator it = _tablet_map[tablet_id].table_arr.begin();
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

    res = dropped_table->store()->deregister_table(dropped_table.get());
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to unregister from root path. [res=%d table=%ld]",
                         res, tablet_id);
    }

    return res;
}

OLAPStatus OLAPEngine::drop_tables_on_error_root_path(
        const vector<TabletInfo>& tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;

    _tablet_map_lock.wrlock();

    for (const TabletInfo& tablet_info : tablet_info_vec) {
        TTabletId tablet_id = tablet_info.tablet_id;
        TSchemaHash schema_hash = tablet_info.schema_hash;
        OLAP_LOG_DEBUG("drop_table begin. [table=%ld schema_hash=%d]",
                       tablet_id, schema_hash);
        OLAPTablePtr dropped_table = _get_table_with_no_lock(tablet_id, schema_hash);
        if (dropped_table.get() == NULL) {
            OLAP_LOG_WARNING("dropping table not exist. [table=%ld schema_hash=%d]",
                             tablet_id, schema_hash);
            continue;
        } else {
            for (list<OLAPTablePtr>::iterator it = _tablet_map[tablet_id].table_arr.begin();
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

OLAPTablePtr OLAPEngine::create_table(
        const TCreateTabletReq& request, const string* ref_root_path, 
        const bool is_schema_change_table, const OLAPTablePtr ref_olap_table) {
    // Get all available stores, use ref_root_path if the caller specified
    std::vector<OlapStore*> stores;
    if (ref_root_path == nullptr) {
        stores = get_stores_for_create_table(request.storage_medium);
        if (stores.empty()) {
            LOG(WARNING) << "there is no available disk that can be used to create table.";
            return nullptr;
        }
    } else {
        stores.push_back(ref_olap_table->store());
    }

    OLAPTablePtr olap_table;
    // Try to create table on each of all_available_root_path, util success
    for (auto& store : stores) {
        OLAPHeader* header = new OLAPHeader();
        OLAPStatus res = _create_new_table_header(request, store, is_schema_change_table, ref_olap_table, header);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to create table header. [res=" << res << " root=" << store->path();
            break;
        }

        olap_table = OLAPTable::create_from_header(header, store);
        if (olap_table == nullptr) {
            LOG(WARNING) << "fail to load olap table from header. root_path:%s" << store->path();
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

    return olap_table;
}

OLAPStatus OLAPEngine::create_init_version(TTabletId tablet_id, SchemaHash schema_hash,
                                           Version version, VersionHash version_hash) {
    OLAP_LOG_DEBUG("begin to create init version. [begin=%d end=%d]",
                   version.first, version.second);

    OLAPTablePtr table;
    ColumnDataWriter* writer = NULL;
    Rowset* new_rowset = NULL;
    OLAPStatus res = OLAP_SUCCESS;
    std::vector<Rowset*> index_vec;

    do {
        if (version.first > version.second) {
            OLAP_LOG_WARNING("begin should not larger than end. [begin=%d end=%d]",
                             version.first, version.second);
            res = OLAP_ERR_INPUT_PARAMETER_ERROR;
            break;
        }

        // Get olap table and generate new index
        table = get_table(tablet_id, schema_hash);
        if (table.get() == NULL) {
            OLAP_LOG_WARNING("fail to find table. [table=%ld]", tablet_id);
            res = OLAP_ERR_TABLE_NOT_FOUND;
            break;
        }

        new_rowset = new(nothrow) Rowset(table.get(), version, version_hash, false, 0, 0);
        if (new_rowset == NULL) {
            LOG(WARNING) << "fail to malloc index. [table=" << table->full_name() << "]";
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        // Create writer, which write nothing to table, to generate empty data file
        writer = ColumnDataWriter::create(table, new_rowset, false);
        if (writer == NULL) {
            LOG(WARNING) << "fail to create writer. [table=" << table->full_name() << "]";
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        res = writer->finalize();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to finalize writer. [table=" << table->full_name() << "]";
            break;
        }

        // Load new index and add to table
        res = new_rowset->load();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load new index. [table=" << table->full_name() << "]";
            break;
        }

        WriteLock wrlock(table->get_header_lock_ptr());
        index_vec.push_back(new_rowset);
        res = table->register_data_source(index_vec);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to register index to data sources. [table=%s]",
                             table->full_name().c_str());
            break;
        }

        res = table->save_header();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to save header. [table=" << table->full_name() << "]";
            break;
        }
    } while (0);

    // Unregister index and delete files(index and data) if failed
    if (res != OLAP_SUCCESS && table.get() != NULL) {
        std::vector<Rowset*> unused_index;
        table->obtain_header_wrlock();
        table->unregister_data_source(version, &unused_index);
        table->release_header_lock();

        for (Rowset* index : index_vec) {
            index->delete_all_files();
            SAFE_DELETE(index);
        }
    }

    OLAP_LOG_DEBUG("create init version end. [res=%d]", res);
    SAFE_DELETE(writer);
    return res;
}

bool OLAPEngine::try_schema_change_lock(TTabletId tablet_id) {
    bool res = false;
    OLAP_LOG_DEBUG("try_schema_change_lock begin. [table=%ld]", tablet_id);
    _tablet_map_lock.rdlock();

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it == _tablet_map.end()) {
        OLAP_LOG_WARNING("tablet does not exists. [table=%ld]", tablet_id);
    } else {
        res = (it->second.schema_change_lock.trylock() == OLAP_SUCCESS);
    }

    _tablet_map_lock.unlock();
    OLAP_LOG_DEBUG("try_schema_change_lock end. [table=%ld]", tablet_id);
    return res;
}

void OLAPEngine::release_schema_change_lock(TTabletId tablet_id) {
    OLAP_LOG_DEBUG("release_schema_change_lock begin. [table=%ld]", tablet_id);
    _tablet_map_lock.rdlock();

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it == _tablet_map.end()) {
        OLAP_LOG_WARNING("tablet does not exists. [table=%ld]", tablet_id);
    } else {
        it->second.schema_change_lock.unlock();
    }

    _tablet_map_lock.unlock();
    OLAP_LOG_DEBUG("release_schema_change_lock end. [table=%ld]", tablet_id);
}

void OLAPEngine::_build_tablet_info(OLAPTablePtr olap_table, TTabletInfo* tablet_info) {
    tablet_info->tablet_id = olap_table->tablet_id();
    tablet_info->schema_hash = olap_table->schema_hash();

    olap_table->obtain_header_rdlock();
    tablet_info->row_count = olap_table->get_num_rows();
    tablet_info->data_size = olap_table->get_data_size();
    const PDelta* last_file_version = olap_table->lastest_version();
    if (last_file_version == NULL) {
        tablet_info->version = -1;
        tablet_info->version_hash = 0;
    } else {
        // report the version before first missing version
        vector<Version> missing_versions;
        olap_table->get_missing_versions_with_header_locked(
                last_file_version->end_version(), &missing_versions);
        const PDelta* least_complete_version =
            olap_table->least_complete_version(missing_versions);
        if (least_complete_version == NULL) {
            tablet_info->version = -1;
            tablet_info->version_hash = 0;
        } else {
            tablet_info->version = least_complete_version->end_version();
            tablet_info->version_hash = least_complete_version->version_hash();
        }
    }
    olap_table->release_header_lock();
}

OLAPStatus OLAPEngine::report_tablet_info(TTabletInfo* tablet_info) {
    DorisMetrics::report_tablet_requests_total.increment(1);
    OLAP_LOG_INFO("begin to process report tablet info. "
                  "[table=%ld schema_hash=%d]",
                  tablet_info->tablet_id, tablet_info->schema_hash);

    OLAPStatus res = OLAP_SUCCESS;

    OLAPTablePtr olap_table = get_table(
            tablet_info->tablet_id, tablet_info->schema_hash);
    if (olap_table.get() == NULL) {
        OLAP_LOG_WARNING("can't find table. [table=%ld schema_hash=%d]",
                         tablet_info->tablet_id, tablet_info->schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    _build_tablet_info(olap_table, tablet_info);
    OLAP_LOG_INFO("success to process report tablet info.");
    return res;
}

OLAPStatus OLAPEngine::report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info) {
    OLAP_LOG_INFO("begin to process report all tablets info.");
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
        for (OLAPTablePtr olap_table : item.second.table_arr) {
            if (olap_table.get() == NULL) {
                continue;
            }

            TTabletInfo tablet_info;
            _build_tablet_info(olap_table, &tablet_info);

            // report expire transaction
            vector<int64_t> transaction_ids;
            olap_table->get_expire_pending_data(&transaction_ids);
            tablet_info.__set_transaction_ids(transaction_ids);

            if (_available_storage_medium_type_count > 1) {
                tablet_info.__set_storage_medium(olap_table->store()->storage_medium());
            }

            tablet_info.__set_version_count(olap_table->file_delta_size());
            tablet_info.__set_path_hash(olap_table->store()->path_hash());

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

void OLAPEngine::get_tablet_stat(TTabletStatResult& result) {
    OLAP_LOG_DEBUG("begin to get all tablet stat.");

    // get current time
    int64_t current_time = UnixMillis();
    
    _tablet_map_lock.wrlock();
    // update cache if too old
    if (current_time - _tablet_stat_cache_update_time_ms > 
        config::tablet_stat_cache_update_interval_second * 1000) {
        OLAP_LOG_DEBUG("update tablet stat.");
        _build_tablet_stat();
    }

    result.__set_tablets_stats(_tablet_stat_cache);

    _tablet_map_lock.unlock();
}

void OLAPEngine::_build_tablet_stat() {
    _tablet_stat_cache.clear();
    for (const auto& item : _tablet_map) {
        if (item.second.table_arr.size() == 0) {
            continue;
        }

        TTabletStat stat;
        stat.tablet_id = item.first;
        for (OLAPTablePtr olap_table : item.second.table_arr) {
            if (olap_table.get() == NULL) {
                continue;
            }
                
            // we only get base tablet's stat
            stat.__set_data_size(olap_table->get_data_size());
            stat.__set_row_num(olap_table->get_num_rows());
            OLAP_LOG_DEBUG("tablet %d get data size: %d, row num %d",
                    item.first, olap_table->get_data_size(), 
                    olap_table->get_num_rows());
            break;
        }

        _tablet_stat_cache.emplace(item.first, stat);
    }

    _tablet_stat_cache_update_time_ms = UnixMillis();
}

bool OLAPEngine::_can_do_compaction(OLAPTablePtr table) {
    // 如果table正在做schema change，则通过选路判断数据是否转换完成
    // 如果选路成功，则转换完成，可以进行BE
    // 如果选路失败，则转换未完成，不能进行BE
    table->obtain_header_rdlock();
    const PDelta* lastest_version = table->lastest_version();
    if (lastest_version == NULL) {
        table->release_header_lock();
        return false;
    }

    if (table->is_schema_changing()) {
        Version test_version = Version(0, lastest_version->end_version());
        vector<Version> path_versions;
        if (OLAP_SUCCESS != table->select_versions_to_span(test_version, &path_versions)) {
            table->release_header_lock();
            return false;
        }
    }
    table->release_header_lock();

    return true;
}

void OLAPEngine::start_clean_fd_cache() {
    OLAP_LOG_TRACE("start clean file descritpor cache");
    FileHandler::get_fd_cache()->prune();
    OLAP_LOG_TRACE("end clean file descritpor cache");
}

void OLAPEngine::perform_cumulative_compaction() {
    OLAPTablePtr best_table = _find_best_tablet_to_compaction(CompactionType::CUMULATIVE_COMPACTION);
    if (best_table == nullptr) { return; }

    CumulativeCompaction cumulative_compaction;
    OLAPStatus res = cumulative_compaction.init(best_table);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to init cumulative compaction."
                     << "table=" << best_table->full_name();
    }

    res = cumulative_compaction.run();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to do cumulative compaction."
                     << "table=" << best_table->full_name();
    }
}

void OLAPEngine::perform_base_compaction() {
    OLAPTablePtr best_table = _find_best_tablet_to_compaction(CompactionType::BASE_COMPACTION);
    if (best_table == nullptr) { return; }

    BaseCompaction base_compaction;
    OLAPStatus res = base_compaction.init(best_table);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to init base compaction."
                     << "table=" << best_table->full_name();
        return;
    }

    res = base_compaction.run();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to init base compaction."
                     << "table=" << best_table->full_name();
    }
}

OLAPTablePtr OLAPEngine::_find_best_tablet_to_compaction(CompactionType compaction_type) {
    ReadLock tablet_map_rdlock(&_tablet_map_lock);
    uint32_t highest_score = 0;
    OLAPTablePtr best_table;
    for (tablet_map_t::value_type& table_ins : _tablet_map){
        for (OLAPTablePtr& table_ptr : table_ins.second.table_arr) {
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
                best_table = table_ptr;
            }
        }
    }
    return best_table;
}

void OLAPEngine::get_cache_status(rapidjson::Document* document) const {
    return _index_stream_lru_cache->get_cache_status(document);
}

OLAPStatus OLAPEngine::start_trash_sweep(double* usage) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAP_LOG_INFO("start trash and snapshot sweep.");

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

    // clear expire incremental rowset
    _tablet_map_lock.rdlock();
    for (const auto& item : _tablet_map) {
        for (OLAPTablePtr olap_table : item.second.table_arr) {
            if (olap_table.get() == NULL) {
                continue;
            }
            olap_table->delete_expire_incremental_data();
        }
    }
    _tablet_map_lock.unlock();

    return res;
}

OLAPStatus OLAPEngine::_do_sweep(
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

OLAPStatus OLAPEngine::_create_new_table_header(
        const TCreateTabletReq& request,
        OlapStore* store,
        const bool is_schema_change_table,
        const OLAPTablePtr ref_olap_table,
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
    if (true == is_schema_change_table) {
        next_unique_id = ref_olap_table->next_unique_id();
    }
    for (TColumn column : request.tablet_schema.columns) {
        if (column.column_type.type == TPrimitiveType::VARCHAR
                && i < request.tablet_schema.short_key_column_count - 1) {
            LOG(WARNING) << "varchar type column should be the last short key.";
            return OLAP_ERR_SCHEMA_SCHEMA_INVALID;
        }
        header->add_column();
        if (true == is_schema_change_table) {
            /*
             * for schema change, compare old_olap_table and new_olap_table
             * 1. if column in both new_olap_table and old_olap_table,
             * assign unique_id of old_olap_table to the column of new_olap_table
             * 2. if column exists only in new_olap_table, assign next_unique_id of old_olap_table
             * to the new column
             *
            */
            size_t field_num = ref_olap_table->tablet_schema().size();
            size_t field_off = 0;
            for (field_off = 0; field_off < field_num; ++field_off) {
                if (ref_olap_table->tablet_schema()[field_off].name == column.column_name) {
                    uint32_t unique_id = ref_olap_table->tablet_schema()[field_off].unique_id;
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
    if (true == is_schema_change_table){
        /*
         * for schema change, next_unique_id of new olap table should be greater than
         * next_unique_id of old olap table
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

OLAPStatus OLAPEngine::_check_existed_or_else_create_dir(const string& path) {
    if (check_dir_existed(path)) {
        LOG(WARNING) << "failed to create the dir that existed. [path='" << path << "']";
        return OLAP_ERR_CANNOT_CREATE_DIR;
    }

    return create_dirs(path);
}

void OLAPEngine::_cancel_unfinished_schema_change() {
    // Schema Change在引擎退出时schemachange信息还保存在在Header里，
    // 引擎重启后，需清除schemachange信息，上层会重做
    uint64_t canceled_num = 0;
    OLAP_LOG_INFO("begin to cancel unfinished schema change.");

    SchemaChangeHandler schema_change_handler;
    TTabletId tablet_id;
    TSchemaHash schema_hash;
    vector<Version> schema_change_versions;
    AlterTabletType type;

    for (const auto& tablet_instance : _tablet_map) {
        for (OLAPTablePtr olap_table : tablet_instance.second.table_arr) {
            if (olap_table.get() == NULL) {
                OLAP_LOG_WARNING("get empty OLAPTablePtr. [tablet_id=%ld]", tablet_instance.first);
                continue;
            }

            bool ret = olap_table->get_schema_change_request(
                    &tablet_id, &schema_hash, &schema_change_versions, &type);
            if (!ret) {
                continue;
            }

            OLAPTablePtr new_olap_table = get_table(tablet_id, schema_hash, false);
            if (new_olap_table.get() == NULL) {
                OLAP_LOG_WARNING("the table referenced by schema change cannot be found. "
                                 "schema change cancelled. [tablet='%s']",
                                 olap_table->full_name().c_str());
                continue;
            }

            // DORIS-3741. Upon restart, it should not clear schema change request.
            new_olap_table->set_schema_change_status(
                    ALTER_TABLE_FAILED, new_olap_table->schema_hash(), -1);
            olap_table->set_schema_change_status(
                    ALTER_TABLE_FAILED, olap_table->schema_hash(), -1);
            OLAP_LOG_DEBUG("cancel unfinished schema change. [tablet='%s']",
                          olap_table->full_name().c_str());
            ++canceled_num;
        }
    }

    OLAP_LOG_INFO("finish to cancel unfinished schema change! [canceled_num=%lu]", canceled_num);
}

void OLAPEngine::start_delete_unused_index() {
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

void OLAPEngine::add_unused_index(Rowset* olap_index) {
    _gc_mutex.lock();

    auto it = _gc_files.find(olap_index);
    if (it == _gc_files.end()) {
        vector<string> files;
        int32_t rowset_id = olap_index->rowset_id();
        for (size_t seg_id = 0; seg_id < olap_index->num_segments(); ++seg_id) {
            string index_file = olap_index->construct_index_file_path(rowset_id, seg_id);
            files.push_back(index_file);

            string data_file = olap_index->construct_data_file_path(rowset_id, seg_id);
            files.push_back(data_file);
        }
        _gc_files[olap_index] = files;
    }

    _gc_mutex.unlock();
}

OLAPStatus OLAPEngine::_create_init_version(
        OLAPTablePtr olap_table, const TCreateTabletReq& request) {
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

    olap_table->obtain_header_wrlock();
    olap_table->set_cumulative_layer_point(request.version + 1);
    res = olap_table->save_header();
    olap_table->release_header_lock();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to save header. [table=" << olap_table->full_name() << "]";
    }

    return res;
}

// TODO(zc): refactor this funciton
OLAPStatus OLAPEngine::create_table(const TCreateTabletReq& request) {
    OLAPStatus res = OLAP_SUCCESS;
    bool is_table_added = false;

    OLAP_LOG_INFO("begin to process create table. [tablet=%ld, schema_hash=%d]",
                  request.tablet_id, request.tablet_schema.schema_hash);

    DorisMetrics::create_tablet_requests_total.increment(1);

    // 1. Make sure create_table operation is idempotent:
    //    return success if table with same tablet_id and schema_hash exist,
    //           false if table with same tablet_id but different schema_hash exist
    if (check_tablet_id_exist(request.tablet_id)) {
        OLAPTablePtr table = get_table(
                request.tablet_id, request.tablet_schema.schema_hash);
        if (table.get() != NULL) {
            OLAP_LOG_INFO("create table success for table already exist.");
            return OLAP_SUCCESS;
        } else {
            OLAP_LOG_WARNING("table with different schema hash already exists.");
            return OLAP_ERR_CE_TABLET_ID_EXIST;
        }
    }

    // 2. Lock to ensure that all create_table operation execute in serial
    static Mutex create_table_lock;
    MutexLock auto_lock(&create_table_lock);

    OLAPTablePtr olap_table;
    do {
        // 3. Create table with only header, no deltas
        olap_table = create_table(request, NULL, false, NULL);
        if (olap_table == NULL) {
            res = OLAP_ERR_CE_CMD_PARAMS_ERROR;
            OLAP_LOG_WARNING("fail to create olap table. [res=%d]", res);
            break;
        }

        // 4. Add table to OlapEngine will make it visiable to user
        res = add_table(
                request.tablet_id, request.tablet_schema.schema_hash, olap_table);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to add table to OLAPEngine. [res=%d]", res);
            break;
        }
        is_table_added = true;

        OLAPTablePtr olap_table_ptr = get_table(
                request.tablet_id, request.tablet_schema.schema_hash);
        if (olap_table_ptr.get() == NULL) {
            res = OLAP_ERR_TABLE_NOT_FOUND;
            OLAP_LOG_WARNING("fail to get table. [res=%d]", res);
            break;
        }

        // 5. Register table into OLAPEngine, so that we can manage table from
        // the perspective of root path.
        // Example: unregister all tables when a bad disk found.
        res = register_table_into_root_path(olap_table_ptr.get());
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to register table into OLAPEngine. [res=%d, root_path=%s]",
                    res, olap_table_ptr->storage_root_path_name().c_str());
            break;
        }

        // 6. Create init version if this is not a restore mode replica and request.version is set
        // bool in_restore_mode = request.__isset.in_restore_mode && request.in_restore_mode;
        // if (!in_restore_mode && request.__isset.version) {
        res = _create_init_version(olap_table_ptr, request);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create initial version for table. [res=%d]", res);
        }
        // }
    } while (0);

    // 7. clear environment
    if (res != OLAP_SUCCESS) {
        DorisMetrics::create_tablet_requests_failed.increment(1);
        if (is_table_added) {
            OLAPStatus status = drop_table(
                    request.tablet_id, request.tablet_schema.schema_hash);
            if (status !=  OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to drop table when create table failed. [res=%d]", res);
            }
        } else if (NULL != olap_table) {
            olap_table->delete_all_files();
        }
    }

    OLAP_LOG_INFO("finish to process create table. [res=%d]", res);
    return res;
}

OLAPStatus OLAPEngine::schema_change(const TAlterTabletReq& request) {
    OLAP_LOG_INFO("begin to schema change. [base_table=%ld new_table=%ld]",
                  request.base_tablet_id, request.new_tablet_req.tablet_id);

    DorisMetrics::schema_change_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    SchemaChangeHandler handler;
    res = handler.process_alter_table(ALTER_TABLET_SCHEMA_CHANGE, request);

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to do schema change. "
                         "[base_table=%ld new_table=%ld] [res=%d]",
                         request.base_tablet_id, request.new_tablet_req.tablet_id, res);
        DorisMetrics::schema_change_requests_failed.increment(1);
        return res;
    }

    OLAP_LOG_INFO("success to submit schema change. "
                  "[base_table=%ld new_table=%ld]",
                  request.base_tablet_id, request.new_tablet_req.tablet_id);
    return res;
}

OLAPStatus OLAPEngine::create_rollup_table(const TAlterTabletReq& request) {
    OLAP_LOG_INFO("begin to create rollup table. "
                  "[base_table=%ld new_table=%ld]",
                  request.base_tablet_id, request.new_tablet_req.tablet_id);

    DorisMetrics::create_rollup_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    SchemaChangeHandler handler;
    res = handler.process_alter_table(ALTER_TABLET_CREATE_ROLLUP_TABLE, request);

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to do rollup. "
                         "[base_table=%ld new_table=%ld] [res=%d]",
                         request.base_tablet_id, request.new_tablet_req.tablet_id, res);
        DorisMetrics::create_rollup_requests_failed.increment(1);
        return res;
    }

    OLAP_LOG_INFO("success to create rollup table. "
                  "[base_table=%ld new_table=%ld] [res=%d]",
                  request.base_tablet_id, request.new_tablet_req.tablet_id, res);
    return res;
}

AlterTableStatus OLAPEngine::show_alter_table_status(
        TTabletId tablet_id,
        TSchemaHash schema_hash) {
    OLAP_LOG_INFO("begin to process show alter table status. "
                  "[table=%ld schema_hash=%d]",
                  tablet_id, schema_hash);

    AlterTableStatus status = ALTER_TABLE_FINISHED;

    OLAPTablePtr table = OLAPEngine::get_instance()->get_table(tablet_id, schema_hash);
    if (table.get() == NULL) {
        OLAP_LOG_WARNING("fail to get table. [table=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        status = ALTER_TABLE_FAILED;
    } else {
        status = table->schema_change_status().status;
    }

    return status;
}

OLAPStatus OLAPEngine::compute_checksum(
        TTabletId tablet_id,
        TSchemaHash schema_hash,
        TVersion version,
        TVersionHash version_hash,
        uint32_t* checksum) {
    OLAP_LOG_INFO("begin to process compute checksum. "
                  "[tablet_id=%ld schema_hash=%d version=%ld]",
                  tablet_id, schema_hash, version);
    OLAPStatus res = OLAP_SUCCESS;

    if (checksum == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    OLAPTablePtr tablet = get_table(tablet_id, schema_hash);
    if (NULL == tablet.get()) {
        OLAP_LOG_WARNING("can't find tablet. [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    {
        ReadLock rdlock(tablet->get_header_lock_ptr());
        const PDelta* message = tablet->lastest_version();
        if (message == NULL) {
            OLAP_LOG_FATAL("fail to get latest version. [tablet_id=%ld]", tablet_id);
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
    reader_params.olap_table = tablet;
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
            OLAP_LOG_DEBUG("reader reads to the end.");
            break;
        } else if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to read in reader. [res=%d]", res);
            return res;
        }

        row_checksum = row.hash_code(row_checksum);
    }

    OLAP_LOG_INFO("success to finish compute checksum. [checksum=%u]", row_checksum);
    *checksum = row_checksum;
    return OLAP_SUCCESS;
}

OLAPStatus OLAPEngine::cancel_delete(const TCancelDeleteDataReq& request) {
    OLAP_LOG_INFO("begin to process cancel delete. [table=%ld version=%ld]",
                  request.tablet_id, request.version);

    DorisMetrics::cancel_delete_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    // 1. Get all tablets with same tablet_id
    list<OLAPTablePtr> table_list;
    res = get_tables_by_id(request.tablet_id, &table_list);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("can't find table. [table=%ld]", request.tablet_id);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 2. Remove delete conditions from each tablet.
    DeleteConditionHandler cond_handler;
    for (OLAPTablePtr temp_table : table_list) {
        temp_table->obtain_header_wrlock();
        res = cond_handler.delete_cond(temp_table, request.version, false);
        if (res != OLAP_SUCCESS) {
            temp_table->release_header_lock();
            OLAP_LOG_WARNING("cancel delete failed. [res=%d table=%s]",
                             res, temp_table->full_name().c_str());
            break;
        }

        res = temp_table->save_header();
        if (res != OLAP_SUCCESS) {
            temp_table->release_header_lock();
            OLAP_LOG_WARNING("fail to save header. [res=%d table=%s]",
                             res, temp_table->full_name().c_str());
            break;
        }
        temp_table->release_header_lock();
    }

    // Show delete conditions in tablet header.
    for (OLAPTablePtr table : table_list) {
        cond_handler.log_conds(table);
    }

    OLAP_LOG_INFO("finish to process cancel delete. [res=%d]", res);
    return res;
}

OLAPStatus OLAPEngine::delete_data(
        const TPushReq& request,
        vector<TTabletInfo>* tablet_info_vec) {
    OLAP_LOG_INFO("begin to process delete data. [request='%s']",
                  ThriftDebugString(request).c_str());
    DorisMetrics::delete_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    if (tablet_info_vec == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    // 1. Get all tablets with same tablet_id
    OLAPTablePtr table = get_table(request.tablet_id, request.schema_hash);
    if (table.get() == NULL) {
        OLAP_LOG_WARNING("can't find table. [table=%ld schema_hash=%d]",
                         request.tablet_id, request.schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // 2. Process delete data by push interface
    PushHandler push_handler;
    if (request.__isset.transaction_id) {
        res = push_handler.process_realtime_push(table, request, PUSH_FOR_DELETE, tablet_info_vec);
    } else {
        res = push_handler.process(table, request, PUSH_FOR_DELETE, tablet_info_vec);
    }

    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to push empty version for delete data. "
                         "[res=%d table='%s']",
                         res, table->full_name().c_str());
        DorisMetrics::delete_requests_failed.increment(1);
        return res;
    }

    OLAP_LOG_INFO("finish to process delete data. [res=%d]", res);
    return res;
}

OLAPStatus OLAPEngine::recover_tablet_until_specfic_version(
        const TRecoverTabletReq& recover_tablet_req) {
    OLAPTablePtr table = get_table(recover_tablet_req.tablet_id,
                                   recover_tablet_req.schema_hash);
    if (table == nullptr) { return OLAP_ERR_TABLE_NOT_FOUND; }
    RETURN_NOT_OK(table->recover_tablet_until_specfic_version(recover_tablet_req.version,
                                                        recover_tablet_req.version_hash));
    return OLAP_SUCCESS;
}

string OLAPEngine::get_info_before_incremental_clone(OLAPTablePtr tablet,
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
        LOG(INFO) << "least complete version for incremental clone. table=" << tablet->full_name() << ", "
                  << "least_complete_version=" << least_complete_version->end_version();
    }

    tablet->release_header_lock();
    LOG(INFO) << "finish to calculate missing versions when clone. [table=" << tablet->full_name()
              << " committed_version=" << committed_version << " missing_versions_size=" << missing_versions->size() << "]";

    // get download path
    return tablet->tablet_path() + CLONE_PREFIX;
}

OLAPStatus OLAPEngine::finish_clone(OLAPTablePtr tablet, const string& clone_dir,
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
                OLAP_LOG_DEBUG("find same file when clone, skip it. [table=%s clone_file=%s]",
                               tablet->full_name().c_str(), clone_file.c_str());
                continue;
            }

            string from = clone_dir + "/" + clone_file;
            string to = tablet_dir + "/" + clone_file;
            LOG(INFO) << "src file:" << from << ", " << "dest file:" << to;
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
            res = OLAPEngine::get_instance()->clone_incremental_data(
                                              tablet, clone_header, committed_version);
        } else {
            res = OLAPEngine::get_instance()->clone_full_data(tablet, clone_header);
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
    OLAP_LOG_INFO("finish to clone data, clear downloaded data. "
                  "[table=%s clone_dir=%s clone_res=%d]",
                  tablet->full_name().c_str(), clone_dir.c_str(), res);
    return res;
}

OLAPStatus OLAPEngine::obtain_shard_path(
        TStorageMedium::type storage_medium, std::string* shard_path, OlapStore** store) {
    OLAP_LOG_INFO("begin to process obtain root path. [storage_medium=%d]", storage_medium);
    OLAPStatus res = OLAP_SUCCESS;

    if (shard_path == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    auto stores = OLAPEngine::get_instance()->get_stores_for_create_table(storage_medium);
    if (stores.empty()) {
        OLAP_LOG_WARNING("no available disk can be used to create table.");
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

    OLAP_LOG_INFO("success to process obtain root path. [path='%s']",
                  shard_path->c_str());
    return res;
}

OLAPStatus OLAPEngine::load_header(
        const string& shard_path,
        const TCloneReq& request) {
    OLAP_LOG_INFO("begin to process load headers. "
                  "[tablet_id=%ld schema_hash=%d]",
                  request.tablet_id, request.schema_hash);
    OLAPStatus res = OLAP_SUCCESS;

    OlapStore* store = nullptr;
    {
        // TODO(zc)
        try {
            auto store_path =
                boost::filesystem::path(shard_path).parent_path().parent_path().string();
            store = OLAPEngine::get_instance()->get_store(store_path);
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
    res =  OLAPEngine::get_instance()->load_one_tablet(
            store,
            request.tablet_id, request.schema_hash,
            schema_hash_path_stream.str());
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process load headers. [res=%d]", res);
        return res;
    }

    OLAP_LOG_INFO("success to process load headers.");
    return res;
}

OLAPStatus OLAPEngine::load_header(
        OlapStore* store,
        const string& shard_path,
        TTabletId tablet_id,
        TSchemaHash schema_hash) {
    OLAP_LOG_INFO("begin to process load headers. [tablet_id=%ld schema_hash=%d]",
                  tablet_id, schema_hash);
    OLAPStatus res = OLAP_SUCCESS;

    stringstream schema_hash_path_stream;
    schema_hash_path_stream << shard_path
                            << "/" << tablet_id
                            << "/" << schema_hash;
    res =  OLAPEngine::get_instance()->load_one_tablet(
            store,
            tablet_id, schema_hash,
            schema_hash_path_stream.str());
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to process load headers. [res=%d]", res);
        return res;
    }

    OLAP_LOG_INFO("success to process load headers.");
    return res;
}

OLAPStatus OLAPEngine::clear_alter_task(const TTabletId tablet_id,
                                        const TSchemaHash schema_hash) {
    OLAP_LOG_INFO("begin to process clear alter task. [tablet_id=%ld schema_hash=%d]",
                  tablet_id, schema_hash);
    OLAPTablePtr tablet = get_table(tablet_id, schema_hash);
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
    OLAPTablePtr related_table = get_table(related_tablet_id, related_schema_hash);
    if (related_table.get() == NULL) {
        OLAP_LOG_WARNING("related table not found when process clear alter task. "
                         "[tablet_id=%ld schema_hash=%d "
                         "related_tablet_id=%ld related_schema_hash=%d]",
                         tablet_id, schema_hash, related_tablet_id, related_schema_hash);
    } else {
        related_table->obtain_header_wrlock();
        related_table->clear_schema_change_request();
        res = related_table->save_header();
        if (res != OLAP_SUCCESS) {
            LOG(FATAL) << "fail to save header. [res=" << res << " tablet='"
                       << related_table->full_name() << "']";
        } else {
            LOG(INFO) << "clear alter task on tablet. [tablet='" << related_table->full_name() << "']";
        }
        related_table->release_header_lock();
    }

    OLAP_LOG_INFO("finish to process clear alter task. [tablet_id=%ld schema_hash=%d]",
                  related_tablet_id, related_schema_hash);
    return OLAP_SUCCESS;
}

OLAPStatus OLAPEngine::push(
        const TPushReq& request,
        vector<TTabletInfo>* tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAP_LOG_INFO("begin to process push. [tablet_id=%ld version=%ld]",
                  request.tablet_id, request.version);

    if (tablet_info_vec == NULL) {
        OLAP_LOG_WARNING("invalid output parameter which is null pointer.");
        DorisMetrics::push_requests_fail_total.increment(1);
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    OLAPTablePtr olap_table = OLAPEngine::get_instance()->get_table(
            request.tablet_id, request.schema_hash);
    if (NULL == olap_table.get()) {
        OLAP_LOG_WARNING("false to find table. [table=%ld schema_hash=%d]",
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
            res = push_handler.process_realtime_push(olap_table, request, type, tablet_info_vec);
        }
    } else {
        {
            SCOPED_RAW_TIMER(&duration_ns);
            res = push_handler.process(olap_table, request, type, tablet_info_vec);
        }
    }

    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to push delta, table=" << olap_table->full_name().c_str()
            << ",cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
        DorisMetrics::push_requests_fail_total.increment(1);
    } else {
        LOG(INFO) << "success to push delta, table=" << olap_table->full_name().c_str()
            << ",cost=" << PrettyPrinter::print(duration_ns, TUnit::TIME_NS);
        DorisMetrics::push_requests_success_total.increment(1);
        DorisMetrics::push_request_duration_us.increment(duration_ns / 1000);
        DorisMetrics::push_request_write_bytes.increment(push_handler.write_bytes());
        DorisMetrics::push_request_write_rows.increment(push_handler.write_rows());
    }
    return res;
}

}  // namespace doris
