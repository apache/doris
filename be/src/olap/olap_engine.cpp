// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <rapidjson/document.h>

#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/lru_cache.h"
#include "olap/olap_header.h"
#include "olap/olap_rootpath.h"
#include "olap/olap_snapshot.h"
#include "olap/push_handler.h"
#include "olap/schema_change.h"
#include "olap/utils.h"
#include "olap/writer.h"

using boost::filesystem::canonical;
using boost::filesystem::directory_iterator;
using boost::filesystem::path;
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


namespace palo {
// OLAPTable*对象的shared_ptr的析构函数
void OLAPTableDestruction(OLAPTable* olap_table) {
    SAFE_DELETE(olap_table);
}

bool _sort_table_by_create_time(const SmartOLAPTable& a, const SmartOLAPTable& b) {
    return a->creation_time() < b->creation_time();
}

OLAPEngine::OLAPEngine() :
        _global_table_id(0),
        _file_descriptor_lru_cache(NULL),
        _index_stream_lru_cache(NULL) {}

OLAPEngine::~OLAPEngine() {
    clear();
}

OLAPStatus OLAPEngine::_load_tables(const string& tablet_root_path) {
    // 遍历跟目录寻找所有的shard
    set<string> shards;
    if (dir_walk(tablet_root_path + DATA_PREFIX, &shards, NULL) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to walk dir. [root=%s]", tablet_root_path.c_str());
        return OLAP_ERR_INIT_FAILED;
    }

    for (const auto& shard : shards) {
        // 遍历shard目录寻找此shard的所有tablet
        set<string> tablets;
        string one_shard_path = tablet_root_path + DATA_PREFIX +  '/' + shard;
        if (dir_walk(one_shard_path, &tablets, NULL) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to walk dir. [root=%s]", one_shard_path.c_str());
            continue;
        }

        for (const auto& tablet : tablets) {
            // 遍历table目录寻找此table的所有indexedRollupTable，注意不是OLAPIndex，而是OLAPTable
            set<string> schema_hashes;
            string one_tablet_path = one_shard_path + '/' + tablet;
            if (dir_walk(one_tablet_path, &schema_hashes, NULL) != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to walk dir. [root=%s]", one_tablet_path.c_str());
                continue;
            }

            for (const auto& schema_hash : schema_hashes) {
                TTabletId tablet_id = strtoul(tablet.c_str(), NULL, 10);
                TSchemaHash tablet_schema_hash = strtoul(schema_hash.c_str(), NULL, 10);

                // 遍历schema_hash目录寻找此index的所有schema
                // 加载失败依然加载下一个Table
                if (load_one_tablet(
                        tablet_id,
                        tablet_schema_hash,
                        one_tablet_path + '/' + schema_hash) != OLAP_SUCCESS) {
                    OLAP_LOG_WARNING("fail to load one table, but continue. [path='%s']",
                                     (one_tablet_path + '/' + schema_hash).c_str());
                }
            }
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPEngine::load_one_tablet(
        TTabletId tablet_id, SchemaHash schema_hash, const string& schema_hash_path) {
    stringstream header_name_stream;
    header_name_stream << schema_hash_path << "/" << tablet_id << ".hdr";
    string header_path = header_name_stream.str();
    path boost_schema_hash_path(schema_hash_path);

    if (access(header_path.c_str(), F_OK) != 0) {
        OLAP_LOG_WARNING("fail to find header file. [header_path=%s]", header_path.c_str());
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_FILE_NOT_EXIST;
    }

    OLAPTable* olap_table = OLAPTable::create_from_header_file(
            tablet_id, schema_hash, header_path);
    if (olap_table == NULL) {
        OLAP_LOG_WARNING("fail to load table. [header_path=%s]", header_path.c_str());
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    if (olap_table->latest_version() == NULL && !olap_table->is_schema_changing()) {
        OLAP_LOG_WARNING("tablet not in schema change state without delta is invalid. "
                         "[header_path=%s]",
                         header_path.c_str());
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        SAFE_DELETE(olap_table);
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    // 这里不需要SAFE_DELETE(olap_table),因为olap_table指针已经在add_table中托管到smart pointer中
    OLAPStatus res = OLAP_SUCCESS;
    string table_name = olap_table->full_name();
    res = add_table(tablet_id, schema_hash, olap_table);
    if (res != OLAP_SUCCESS) {
        // 插入已经存在的table时返回成功
        if (res == OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE) {
            return OLAP_SUCCESS;
        }

        OLAP_LOG_WARNING("failed to add table. [table=%s]", table_name.c_str());
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    if (OLAPRootPath::get_instance()->register_table_into_root_path(olap_table) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to register table into root path. [root_path=%s]",
                         schema_hash_path.c_str());

        if (OLAPEngine::get_instance()->drop_table(tablet_id, schema_hash) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to drop table when create table failed. "
                             "[tablet=%ld schema_hash=%d]",
                             tablet_id, schema_hash); 
        }

        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    OLAP_LOG_DEBUG("succeed to add table. [table=%s, path=%s]",
                   olap_table->full_name().c_str(),
                   schema_hash_path.c_str());
    return OLAP_SUCCESS;
}

void* load_root_path_thread_callback(void* arg) {
    OLAPStatus res = OLAP_SUCCESS;
    string root_path = (char*)arg;

    if ((res = OLAPEngine::get_instance()->_load_tables(root_path.c_str())) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("io error when init load tables. [res=%d root='%s']",
                         res,
                         root_path.c_str());
    }

    return NULL;
}

// @static
OLAPStatus OLAPEngine::_spawn_load_root_path_thread(pthread_t* thread, const string& root_path) {
    sigset_t mask;
    sigset_t omask;

    sigemptyset(&mask);
    sigaddset(&mask, SIGCHLD);
    sigaddset(&mask, SIGHUP);
    sigaddset(&mask, SIGPIPE);
    pthread_sigmask(SIG_SETMASK, &mask, &omask);

    OLAP_LOG_TRACE("spawn a schema_change thread.");

    int err = 0;
    while ((err = pthread_create(thread,
                                 NULL,
                                 load_root_path_thread_callback,
                                 reinterpret_cast<void*>(const_cast<char*>(
                                         root_path.c_str())))) != 0) {
        OLAP_LOG_WARNING("failed to spawn load root path thread.");
        // Sleep 1s before next try
        usleep(1000000);
    }

    pthread_sigmask(SIG_SETMASK, &omask, NULL);
    return OLAP_SUCCESS;
}

void OLAPEngine::load_root_paths(const OLAPRootPath::RootPathVec& root_paths) {
    pthread_t* load_root_path_thread = new pthread_t [root_paths.size()];

    for (uint32_t i = 0; i < root_paths.size(); i++) {
        _spawn_load_root_path_thread(&load_root_path_thread[i], root_paths[i]);
    }

    for (uint32_t i = 0; i < root_paths.size(); i++) {
        pthread_join(load_root_path_thread[i], NULL);
    }

    delete [] load_root_path_thread;
}

OLAPStatus OLAPEngine::init() {
    OLAPRootPath::RootPathVec all_available_root_path;

    _file_descriptor_lru_cache = new_lru_cache(config::file_descriptor_cache_capacity);
    if (_file_descriptor_lru_cache == NULL) {
        OLAP_LOG_WARNING("failed to init file descriptor LRUCache");
        _tablet_map.clear();
        return OLAP_ERR_INIT_FAILED;
    }

    // 初始化LRUCache
    // cache大小可通过配置文件配置
    _index_stream_lru_cache = new_lru_cache(config::index_stream_cache_capacity);
    if (_index_stream_lru_cache == NULL) {
        OLAP_LOG_WARNING("failed to init index stream LRUCache");
        _tablet_map.clear();
        return OLAP_ERR_INIT_FAILED;
    }

    // 初始化CE调度器
    vector<OLAPRootPathStat> all_root_paths_stat;
    OLAPRootPath::get_instance()->get_all_disk_stat(&all_root_paths_stat);
    _cumulative_compaction_disk_stat.reserve(all_root_paths_stat.size());
    for (uint32_t i = 0; i < all_root_paths_stat.size(); i++) {
        const OLAPRootPathStat& stat = all_root_paths_stat[i];
        _cumulative_compaction_disk_stat.emplace_back(stat.root_path, i, stat.is_used);
        _disk_id_map[stat.root_path] = i;
    }
    int32_t cumulative_compaction_num_threads = config::cumulative_compaction_num_threads;
    int32_t base_compaction_num_threads = config::base_compaction_num_threads;
    uint32_t file_system_num = OLAPRootPath::get_instance()->get_file_system_count();
    _max_cumulative_compaction_task_per_disk = (cumulative_compaction_num_threads + file_system_num - 1) / file_system_num;
    _max_base_compaction_task_per_disk = (base_compaction_num_threads + file_system_num - 1) / file_system_num;

    // 加载所有table
    OLAPRootPath::get_instance()->get_all_available_root_path(&all_available_root_path);
    load_root_paths(all_available_root_path);

    // 取消未完成的SchemaChange任务
    _cancel_unfinished_schema_change();

    return OLAP_SUCCESS;
}

OLAPStatus OLAPEngine::clear() {
    // 删除lru中所有内容,其实进程退出这么做本身意义不大,但对单测和更容易发现问题还是有很大意义的
    SAFE_DELETE(_file_descriptor_lru_cache);
    SAFE_DELETE(_index_stream_lru_cache);

    _tablet_map.clear();
    _global_table_id = 0;

    return OLAP_SUCCESS;
}

SmartOLAPTable OLAPEngine::_get_table_with_no_lock(TTabletId tablet_id, SchemaHash schema_hash) {
    OLAP_LOG_DEBUG("begin to get olap table. [table=%ld]", tablet_id);

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end()) {
        for (SmartOLAPTable table : it->second.table_arr) {
            if (table->equal(tablet_id, schema_hash)) {
                OLAP_LOG_DEBUG("get olap table success. [table=%ld]", tablet_id);
                return table;
            }
        }
    }

    OLAP_LOG_DEBUG("fail to get olap table. [table=%ld]", tablet_id);
    // Return empty olap_table if fail
    SmartOLAPTable olap_table;
    return olap_table;
}

SmartOLAPTable OLAPEngine::get_table(TTabletId tablet_id, SchemaHash schema_hash) {
    _tablet_map_lock.rdlock();
    SmartOLAPTable olap_table;
    olap_table = _get_table_with_no_lock(tablet_id, schema_hash);
    _tablet_map_lock.unlock();

    if (olap_table.get() != NULL) {
        if (!olap_table->is_used()) {
            OLAP_LOG_WARNING("olap table cannot be used. [table=%ld]", tablet_id);
            olap_table.reset();
        } else if (!olap_table->is_loaded()) {
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
        list<SmartOLAPTable>* table_list) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAP_LOG_DEBUG("begin to get tables by id. [table=%ld]", tablet_id);

    _tablet_map_lock.rdlock();
    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end()) {
        for (SmartOLAPTable olap_table : it->second.table_arr) {
            table_list->push_back(olap_table);
        }
    }
    _tablet_map_lock.unlock();

    if (table_list->size() == 0) {
        OLAP_LOG_WARNING("there is no tablet with specified id. [table=%ld]", tablet_id);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    for (std::list<SmartOLAPTable>::iterator it = table_list->begin();
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

OLAPStatus OLAPEngine::add_table(TTabletId tablet_id, SchemaHash schema_hash, OLAPTable* table) {
    OLAPStatus res = OLAP_SUCCESS;
    OLAP_LOG_DEBUG("begin to add olap table to OLAPEngine. [tablet_id=%ld schema_hash=%d]",
                   tablet_id, schema_hash);
    _tablet_map_lock.wrlock();

    SmartOLAPTable smart_table(table, OLAPTableDestruction);
    smart_table->set_id(_global_table_id++);

    SmartOLAPTable table_item;
    for (SmartOLAPTable item : _tablet_map[tablet_id].table_arr) {
        if (item->equal(tablet_id, schema_hash)) {
            table_item = item;
            break;
        }
    }

    if (table_item.get() == NULL) {
        _tablet_map[tablet_id].table_arr.push_back(smart_table);
        _tablet_map[tablet_id].table_arr.sort(_sort_table_by_create_time);
        _tablet_map_lock.unlock();

        return res;
    }
    _tablet_map_lock.unlock();

    if (table_item->header_file_name() == smart_table->header_file_name()) {
        OLAP_LOG_WARNING("add the same tablet twice! [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
    }

    table_item->obtain_header_rdlock();
    int64_t old_time = table_item->latest_version()->creation_time();
    int64_t new_time = smart_table->latest_version()->creation_time();
    int32_t old_version = table_item->latest_version()->end_version();
    int32_t new_version = smart_table->latest_version()->end_version();
    table_item->release_header_lock();

    if (new_version > old_version
            || (new_version == old_version && new_time > old_time)) {
        drop_table(tablet_id, schema_hash);
        _tablet_map_lock.wrlock();
        _tablet_map[tablet_id].table_arr.push_back(smart_table);
        _tablet_map[tablet_id].table_arr.sort(_sort_table_by_create_time);
        _tablet_map_lock.unlock();
    } else {
        smart_table->mark_dropped();
        res = OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
    }
    OLAP_LOG_WARNING("add duplicated table. [res=%d tablet_id=%ld schema_hash=%d "
                     "old_version=%d new_version=%d old_time=%ld new_time=%ld]",
                     res, tablet_id, schema_hash,
                     old_version, new_version, old_time, new_time);

    return res;
}

// Drop table specified, the main logical is as follows:
// 1. table not in schema change:
//      drop specified table directly;
// 2. table in schema change:
//      a. schema change not finished && dropped table is base :
//          base table cannot be dropped;
//      b. other cases:
//          drop specified table and clear schema change info.
OLAPStatus OLAPEngine::drop_table(TTabletId tablet_id, SchemaHash schema_hash) {
    OLAP_LOG_INFO("begin to drop olap table. [tablet_id=%ld]", tablet_id);
    OLAPStatus res = OLAP_SUCCESS;

    // Get table which need to be droped
    _tablet_map_lock.rdlock();
    SmartOLAPTable dropped_table = _get_table_with_no_lock(tablet_id, schema_hash);
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
        return _drop_table_directly(tablet_id, schema_hash);
    }

    // Check table is in schema change or not, is base table or not
    bool is_schema_change_finished = true;
    if (schema_change_versions.size() != 0) {
        is_schema_change_finished = false;
    }

    bool is_drop_base_table = false;
    _tablet_map_lock.rdlock();
    SmartOLAPTable related_table = _get_table_with_no_lock(
            related_tablet_id, related_schema_hash);
    _tablet_map_lock.unlock();
    if (related_table.get() == NULL) {
        OLAP_LOG_WARNING("drop table directly when related table not found. "
                         "[tablet_id=%ld schema_hash=%d]",
                         related_tablet_id, related_schema_hash);
        return _drop_table_directly(tablet_id, schema_hash);
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
    related_table->obtain_header_wrlock();
    related_table->clear_schema_change_request();
    res = related_table->save_header();
    related_table->release_header_lock();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_FATAL("fail to save table header. [res=%d table=%s]",
                       res, related_table->full_name().c_str());
    }

    res = _drop_table_directly(tablet_id, schema_hash);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to drop table which in schema change. [table=%s]",
                         dropped_table->full_name().c_str());
        return res;
    }

    OLAP_LOG_INFO("finish to drop tablet. [res=%d]", res);
    return res;
}

OLAPStatus OLAPEngine::_drop_table_directly(TTabletId tablet_id, SchemaHash schema_hash) {
    OLAPStatus res = OLAP_SUCCESS;
    _tablet_map_lock.wrlock();

    SmartOLAPTable dropped_table = _get_table_with_no_lock(tablet_id, schema_hash);
    if (dropped_table.get() == NULL) {
        OLAP_LOG_WARNING("fail to drop not existed table. [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        _tablet_map_lock.unlock();
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    for (list<SmartOLAPTable>::iterator it = _tablet_map[tablet_id].table_arr.begin();
            it != _tablet_map[tablet_id].table_arr.end();) {
        if ((*it)->equal(tablet_id, schema_hash)) {
            (*it)->mark_dropped();
            it = _tablet_map[tablet_id].table_arr.erase(it);
        } else {
            ++it;
        }
    }

    if (_tablet_map[tablet_id].table_arr.empty()) {
        _tablet_map.erase(tablet_id);
    }

    _tablet_map_lock.unlock();
    res = OLAPRootPath::get_instance()->unregister_table_from_root_path(dropped_table.get());
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to unregister from root path. [res=%d table=%ld]",
                         res, tablet_id);
    }

    return res;
}

OLAPStatus OLAPEngine::drop_tables_on_error_root_path(
        const vector<TableInfo>& table_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;

    _tablet_map_lock.wrlock();

    for (const TableInfo& table_info : table_info_vec) {
        TTabletId tablet_id = table_info.tablet_id;
        TSchemaHash schema_hash = table_info.schema_hash;
        OLAP_LOG_DEBUG("drop_table begin. [table=%ld schema_hash=%d]",
                       tablet_id, schema_hash);
        SmartOLAPTable dropped_table = _get_table_with_no_lock(tablet_id, schema_hash);
        if (dropped_table.get() == NULL) {
            OLAP_LOG_WARNING("dropping table not exist. [table=%ld schema_hash=%d]",
                             tablet_id, schema_hash);
            continue;
        } else {
            for (list<SmartOLAPTable>::iterator it = _tablet_map[tablet_id].table_arr.begin();
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

OLAPTable* OLAPEngine::create_table(
        const TCreateTabletReq& request, const string* ref_root_path, 
        const bool is_schema_change_table, const SmartOLAPTable ref_olap_table) {
    OLAPTable* olap_table = NULL;
    // Get all available root paths, use ref_root_path if the caller specified
    OLAPRootPath::RootPathVec all_available_root_path;
    if (ref_root_path == NULL) {
        OLAPRootPath* olap_root_path = OLAPRootPath::get_instance();
        olap_root_path->get_root_path_for_create_table(
                request.storage_medium, &all_available_root_path);
        if (all_available_root_path.size() == 0) {
            OLAP_LOG_WARNING("there is no available disk that can be used to create table.");
            return olap_table;
        }
    } else {
        all_available_root_path.push_back(*ref_root_path);
    }

    // Try to create table on each of all_available_root_path, util success
    string header_path;
    for (string root_path : all_available_root_path) {
        OLAPStatus res = _create_new_table_header_file(request, root_path, &header_path, is_schema_change_table, ref_olap_table);
        if (res != OLAP_SUCCESS) {
            if (is_io_error(res)) {
                OLAP_LOG_WARNING("io error when creating table header. [res=%d root=%s]",
                                 res, root_path.c_str());
                continue;
            } else {
                OLAP_LOG_WARNING("fail to create table header. [res=%d root=%s]",
                                 res, root_path.c_str());
                break;
            }
        }

        olap_table = OLAPTable::create_from_header_file(
                request.tablet_id, request.tablet_schema.schema_hash, header_path);
        if (olap_table == NULL) {
            OLAP_LOG_WARNING("fail to load olap table from header. [header_path=%s]",
                             header_path.c_str());
            if (remove_parent_dir(header_path) != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to remove header path. [header_path=%s]",
                                 header_path.c_str());
            }
            continue;
        }

        OLAP_LOG_DEBUG("success to create table from header. [header_path=%s]",
                       header_path.c_str());
        break;
    }

    return olap_table;
}

OLAPStatus OLAPEngine::create_init_version(TTabletId tablet_id, SchemaHash schema_hash,
                                           Version version, VersionHash version_hash) {
    OLAP_LOG_DEBUG("begin to create init version. [begin=%d end=%d]",
                   version.first, version.second);

    SmartOLAPTable table;
    IWriter* writer = NULL;
    OLAPIndex* new_index = NULL;
    OLAPStatus res = OLAP_SUCCESS;

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

        new_index = new(nothrow) OLAPIndex(table.get(), version, version_hash, false, 0, 0);
        if (new_index == NULL) {
            OLAP_LOG_WARNING("fail to malloc index. [table=%s]", table->full_name().c_str());
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        // Create writer, which write nothing to table, to generate empty data file
        writer = IWriter::create(table, new_index, false);
        if (writer == NULL) {
            OLAP_LOG_WARNING("fail to create writer. [table=%s]", table->full_name().c_str());
            res = OLAP_ERR_MALLOC_ERROR;
            break;
        }

        res = writer->init();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init writer. [table=%s]", table->full_name().c_str());
            break;
        }

        res = writer->finalize();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to finalize writer. [table=%s]", table->full_name().c_str());
            break;
        }

        // Load new index and add to table
        res = new_index->load();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to load new index. [table=%s]", table->full_name().c_str());
            break;
        }

        AutoRWLock auto_lock(table->get_header_lock_ptr(), false);
        res = table->register_data_source(new_index);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to register index to data sources. [table=%s]",
                             table->full_name().c_str());
            break;
        }

        res = table->save_header();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to save header. [table=%s]", table->full_name().c_str());
            break;
        }
    } while (0);

    // Unregister index and delete files(index and data) if failed
    if (res != OLAP_SUCCESS && table.get() != NULL) {
        OLAPIndex* unused_index = NULL;
        table->obtain_header_wrlock();
        table->unregister_data_source(version, &unused_index);
        table->release_header_lock();

        if (new_index != NULL) {
            new_index->delete_all_files();
            SAFE_DELETE(new_index);
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
        OLAP_LOG_WARNING("table does not exists. [table=%ld]", tablet_id);
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
        OLAP_LOG_WARNING("table does not exists. [table=%ld]", tablet_id);
    } else {
        it->second.schema_change_lock.unlock();
    }

    _tablet_map_lock.unlock();
    OLAP_LOG_DEBUG("release_schema_change_lock end. [table=%ld]", tablet_id);
}

OLAPStatus OLAPEngine::report_tablet_info(TTabletInfo* tablet_info) {
    OLAPStatus res = OLAP_SUCCESS;

    SmartOLAPTable olap_table = get_table(
            tablet_info->tablet_id, tablet_info->schema_hash);
    if (olap_table.get() == NULL) {
        OLAP_LOG_WARNING("can't find table. [table=%ld schema_hash=%d]",
                         tablet_info->tablet_id, tablet_info->schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    tablet_info->tablet_id = olap_table->tablet_id();
    tablet_info->schema_hash = olap_table->schema_hash();

    olap_table->obtain_header_rdlock();
    tablet_info->row_count = olap_table->get_num_rows();
    tablet_info->data_size = olap_table->get_data_size();
    const FileVersionMessage* last_file_version = olap_table->latest_version();
    if (last_file_version == NULL) {
        tablet_info->version = -1;
        tablet_info->version_hash = 0;
    } else {
        tablet_info->version = last_file_version->end_version();
        tablet_info->version_hash = last_file_version->version_hash();
    }
    olap_table->release_header_lock();

    return res;
}

OLAPStatus OLAPEngine::report_all_tablets_info(
        map<TTabletId, TTablet>* tablets_info) {
    OLAP_LOG_DEBUG("begin to get all tablet info.");

    if (tablets_info == NULL) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    uint32_t available_storage_medium_type_count = 
            OLAPRootPath::get_instance()->available_storage_medium_type_count();

    _tablet_map_lock.rdlock();
    for (const auto& item : _tablet_map) {
        if (item.second.table_arr.size() == 0) {
            continue;
        }

        TTablet tablet;
        for (SmartOLAPTable olap_table : item.second.table_arr) {
            if (olap_table.get() == NULL) {
                continue;
            }

            TTabletInfo tablet_info;
            tablet_info.tablet_id = olap_table->tablet_id();
            tablet_info.schema_hash = olap_table->schema_hash();

            olap_table->obtain_header_rdlock();
            tablet_info.row_count = olap_table->get_num_rows();
            tablet_info.data_size = olap_table->get_data_size();
            const FileVersionMessage* last_file_version = olap_table->latest_version();
            if (last_file_version == NULL) {
                tablet_info.version = -1;
                tablet_info.version_hash = 0;
            } else {
                tablet_info.version = last_file_version->end_version();
                tablet_info.version_hash = last_file_version->version_hash();
            }
            olap_table->release_header_lock();

            if (available_storage_medium_type_count > 1) {
                tablet_info.__set_storage_medium(TStorageMedium::HDD);
                if (OLAPRootPath::is_ssd_disk(olap_table->storage_root_path_name())) {
                    tablet_info.__set_storage_medium(TStorageMedium::SSD);
                }
            }

            tablet.tablet_infos.push_back(tablet_info);
        }

        if (tablet.tablet_infos.size() != 0) {
            tablets_info->insert(pair<TTabletId, TTablet>(tablet.tablet_infos[0].tablet_id, tablet));
        }
    }
    _tablet_map_lock.unlock();

    OLAP_LOG_DEBUG("success to get all tablets info. [tablet_num=%u]",
                   tablets_info->size());
    return OLAP_SUCCESS;
}

bool OLAPEngine::_can_do_compaction(SmartOLAPTable table) {
    // 如果table正在做schema change，则通过选路判断数据是否转换完成
    // 如果选路成功，则转换完成，可以进行BE
    // 如果选路失败，则转换未完成，不能进行BE
    table->obtain_header_rdlock();
    const FileVersionMessage* latest_version = table->latest_version();
    if (latest_version == NULL) {
        table->release_header_lock();
        return false;
    }

    if (table->is_schema_changing()) {
        Version test_version = Version(0, latest_version->end_version());
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
    _file_descriptor_lru_cache->prune();
    OLAP_LOG_TRACE("end clean file descritpor cache");
}

void OLAPEngine::start_base_compaction(string* last_base_compaction_fs, TTabletId* last_base_compaction_tablet_id) {
    uint64_t base_compaction_start_hour = config::base_compaction_start_hour;
    uint64_t base_compaction_end_hour = config::base_compaction_end_hour;
    time_t current_time = time(NULL);
    uint64_t current_hour = localtime(&current_time)->tm_hour;
    // 如果执行BE的时间区间设置为类似以下的形式：[1:00, 8:00)
    if (base_compaction_start_hour <= base_compaction_end_hour) {
        if (current_hour < base_compaction_start_hour
                || current_hour >= base_compaction_end_hour) {
            OLAP_LOG_TRACE("don't allow to excute base compaction in this time interval. "
                           "[now_hour=%d; allow_start_time=%d; allow_end_time=%d]",
                           current_hour,
                           base_compaction_start_hour,
                           base_compaction_end_hour);
            return;
        }
    } else { // 如果执行BE的时间区间设置为类似以下的形式：[22:00, 8:00)
        if (current_hour < base_compaction_start_hour
                && current_hour >= base_compaction_end_hour) {
            OLAP_LOG_TRACE("don't allow to excute base compaction in this time interval. "
                           "[now_hour=%d; allow_start_time=%d; allow_end_time=%d]",
                           current_hour,
                           base_compaction_start_hour,
                           base_compaction_end_hour);
            return;
        }
    }

    SmartOLAPTable tablet;
    BaseCompaction base_compaction;

    bool do_base_compaction = false;
    OLAP_LOG_TRACE("start_base_compaction begin.");
    _tablet_map_lock.rdlock();
    _fs_task_mutex.lock();

    if (*last_base_compaction_fs != "") {
        _fs_base_compaction_task_num_map[*last_base_compaction_fs] -= 1;
        last_base_compaction_fs->clear();
    }

    for (const auto& i : _tablet_map) {
        for (SmartOLAPTable j : i.second.table_arr) {
            // 保证从上一次被选中进行BE的表开始轮询
            if (i.first <= *last_base_compaction_tablet_id) {
                continue;
            }

            if (_fs_base_compaction_task_num_map[j->storage_root_path_name()] >= _max_base_compaction_task_per_disk) {
                continue;
            }

            // 跳过正在做schema change的tablet
            if (!_can_do_compaction(j)) {
                OLAP_LOG_DEBUG("skip tablet, it is schema changing. [tablet=%s]",
                               j->full_name().c_str());
                continue;
            }

            if (base_compaction.init(j, false) == OLAP_SUCCESS) {
                tablet = j;
                do_base_compaction = true;
                _fs_base_compaction_task_num_map[tablet->storage_root_path_name()] += 1;
                *last_base_compaction_fs = tablet->storage_root_path_name();
                *last_base_compaction_tablet_id = i.first;
                goto TRY_START_BE_OK;
            }
        }
    }

    // when the loop comes the end, restart from begin
    *last_base_compaction_tablet_id = -1;

TRY_START_BE_OK:
    _fs_task_mutex.unlock();
    _tablet_map_lock.unlock();
    OLAP_LOG_TRACE("start_base_compaction end.");

    if (do_base_compaction) {
        OLAP_LOG_NOTICE_PUSH("request", "START_BASE_COMPACTION");
        OLAPStatus cmd_res = base_compaction.run();
        if (cmd_res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to do base compaction. [tablet='%s']",
                             tablet->full_name().c_str());
        }
    }
}

void OLAPEngine::_select_candidate() {
    // 这是一个小根堆，用于记录nice最大的top k个candidate tablet
    SmartOLAPTable tablet;
    typedef priority_queue<CompactionCandidate, vector<CompactionCandidate>,
            CompactionCandidateComparator> candidate_heap_t;
    vector<candidate_heap_t> candidate_heap_vec(_cumulative_compaction_disk_stat.size());
    for (const auto& i : _tablet_map) {
        uint32_t nice = 0;
        // calc nice
        for (SmartOLAPTable j : i.second.table_arr) {
            if (!j->is_loaded()) {
                continue;
            }

            j->obtain_header_rdlock();
            const uint32_t curr_nice = j->get_compaction_nice_estimate();
            j->release_header_lock();
            nice = curr_nice > nice ? curr_nice : nice;
            tablet = j;
        }

        // save
        if (nice > 0) {
            uint32_t disk_id = _disk_id_map[tablet->storage_root_path_name()];
            candidate_heap_vec[disk_id].emplace(nice, i.first, disk_id);
            if (candidate_heap_vec[disk_id].size() > OLAP_COMPACTION_DEFAULT_CANDIDATE_SIZE) {
                candidate_heap_vec[disk_id].pop();
            }
        }
    }

    _cumulative_compaction_candidate.clear();
    for (auto& stat : _cumulative_compaction_disk_stat) {
        stat.task_remaining = 0;
    }

    for (auto& candidate_heap : candidate_heap_vec) {
        while (!candidate_heap.empty()) {
            _cumulative_compaction_candidate.push_back(candidate_heap.top());
            ++_cumulative_compaction_disk_stat[candidate_heap.top().disk_index].task_remaining;
            candidate_heap.pop();
        }
    }

    // sort small to big
    sort(_cumulative_compaction_candidate.rbegin(), _cumulative_compaction_candidate.rend(), CompactionCandidateComparator());
}

void OLAPEngine::start_cumulative_priority() {
    _tablet_map_lock.rdlock();
    _fs_task_mutex.lock();

    // determine whether to select candidate or not 
    bool is_select = false;
    vector<OLAPRootPathStat> all_root_paths_stat;
    OLAPRootPath::get_instance()->get_all_disk_stat(&all_root_paths_stat);
    for (uint32_t i = 0; i < all_root_paths_stat.size(); i++) {
        uint32_t disk_id = _disk_id_map[all_root_paths_stat[i].root_path];
        _cumulative_compaction_disk_stat[disk_id].is_used = all_root_paths_stat[i].is_used;
    }

    for (auto& disk : _cumulative_compaction_disk_stat) {
        if (!disk.task_remaining && disk.is_used) {
            is_select = true;
        }
    }

    if (is_select) {
        _select_candidate();
    }

    // traverse _cumulative_compaction_candidate to start cumulative compaction
    CumulativeCompaction cumulative_compaction;
    for (auto it_cand = _cumulative_compaction_candidate.rbegin(); it_cand != _cumulative_compaction_candidate.rend(); ++it_cand) {
        CompactionCandidate candidate = *it_cand;
        const auto i = _tablet_map.find(candidate.tablet_id);
        if (i == _tablet_map.end()) {
            // tablet已经不存在
            _cumulative_compaction_candidate.erase(it_cand.base() - 1);
            --_cumulative_compaction_disk_stat[candidate.disk_index].task_remaining;
            continue;
        }

        if (_cumulative_compaction_disk_stat[candidate.disk_index].task_running >= _max_cumulative_compaction_task_per_disk) {
            OLAP_LOG_DEBUG("skip tablet, too much ce task on disk %s",
                    _cumulative_compaction_disk_stat[candidate.disk_index].storage_path.c_str());
            // 某个disk上任务数太多，跳过，candidate中保留这个任务
            continue;
        }

        for (SmartOLAPTable j : i->second.table_arr) {
            if (!_can_do_compaction(j)) {
                OLAP_LOG_DEBUG("skip tablet, it is schema changing. [tablet=%s]",
                               j->full_name().c_str());
                continue;
            }

            if (cumulative_compaction.init(j) == OLAP_SUCCESS) {
                _cumulative_compaction_candidate.erase(it_cand.base() - 1);
                --_cumulative_compaction_disk_stat[candidate.disk_index].task_remaining;
                ++_cumulative_compaction_disk_stat[candidate.disk_index].task_running;
                _fs_task_mutex.unlock();
                _tablet_map_lock.unlock();

                // start cumulative
                if (cumulative_compaction.run() != OLAP_SUCCESS) {
                    OLAP_LOG_WARNING("failed to do cumulative. [tablet='%s']",
                                     j->full_name().c_str());
                }

                _fs_task_mutex.lock();
                --_cumulative_compaction_disk_stat[candidate.disk_index].task_running;
                _fs_task_mutex.unlock();
                return;
            }
        }
        // 这个tablet不适合做ce
        _cumulative_compaction_candidate.erase(it_cand.base() - 1);
        --_cumulative_compaction_disk_stat[candidate.disk_index].task_remaining;
    }
    _fs_task_mutex.unlock();
    _tablet_map_lock.unlock();
    OLAP_LOG_TRACE("no tablet selected to do cumulative compaction this loop.");
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
    std::vector<OLAPRootPathStat> disks_stat;
    res = OLAPRootPath::get_instance()->get_all_disk_stat(&disks_stat);
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

    for (OLAPRootPathStat& stat : disks_stat) {
        if (!stat.is_used) {
            continue;
        }

        double curr_usage = (stat.disk_total_capacity - stat.disk_available_capacity)
                / (double) stat.disk_total_capacity;
        *usage = *usage > curr_usage ? *usage : curr_usage;

        OLAPStatus curr_res = OLAP_SUCCESS;
        string snapshot_path = stat.root_path + SNAPSHOT_PREFIX;
        curr_res = _do_sweep(snapshot_path, local_now, snapshot_expire);
        if (curr_res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to sweep snapshot. [path=%s, err_code=%d]",
                    snapshot_path.c_str(), curr_res);
            res = curr_res;
        }

        string trash_path = stat.root_path + TRASH_PREFIX;
        curr_res = _do_sweep(trash_path, local_now,
                curr_usage > guard_space ? 0 : trash_expire);
        if (curr_res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("failed to sweep trash. [path=%s, err_code=%d]",
                    trash_path.c_str(), curr_res);
            res = curr_res;
        }
    }

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
                OLAP_LOG_WARNING("fail to strptime time. [time=%lu]", str_time.c_str());
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

OLAPStatus OLAPEngine::_create_new_table_header_file(
        const TCreateTabletReq& request, const string& root_path, string* header_path,
        const bool is_schema_change_table, const SmartOLAPTable ref_olap_table) {
    OLAPStatus res = OLAP_SUCCESS;

    uint64_t shard = 0;
    res = OLAPRootPath::get_instance()->get_root_path_shard(root_path, &shard);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get root path shard. [res=%d]", res);
        return res;
    }

    // Generate header path info: header_path = header_dir + "/" + header_file
    stringstream header_dir_stream;
    header_dir_stream << root_path
                      << DATA_PREFIX
                      << "/" << shard
                      << "/" << request.tablet_id
                      << "/" << request.tablet_schema.schema_hash;
    string header_dir = header_dir_stream.str();

    stringstream header_file_stream;
    header_file_stream << request.tablet_id << ".hdr";
    string header_file = header_file_stream.str();

    res = _check_existed_or_else_create_dir(header_dir);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("dir existed or create dir fail. [res=%d]", res);
        return res;
    }

    // Generate and Initialize OLAPHeader
    string header_path_tmp = header_dir + "/" + header_file;
    OLAPHeader header(header_path_tmp);
    
    // set basic information
    header.set_num_short_key_fields(request.tablet_schema.short_key_column_count);
    header.set_compress_kind(COMPRESS_LZ4);

    if (request.tablet_schema.keys_type == TKeysType::DUP_KEYS) {
        header.set_keys_type(KeysType::DUP_KEYS);
    } else if (request.tablet_schema.keys_type == TKeysType::UNIQUE_KEYS) {
        header.set_keys_type(KeysType::UNIQUE_KEYS);
    } else {
        header.set_keys_type(KeysType::AGG_KEYS);
    }

    if (request.tablet_schema.storage_type == TStorageType::COLUMN) {
        header.set_data_file_type(COLUMN_ORIENTED_FILE);
        header.set_segment_size(OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE);
        header.set_num_rows_per_data_block(config::default_num_rows_per_column_file_block);
    } else {
        header.set_data_file_type(OLAP_DATA_FILE);
        header.set_segment_size(OLAP_MAX_SEGMENT_FILE_SIZE);
        header.set_num_rows_per_data_block(config::default_num_rows_per_data_block);
    }
    
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
            OLAP_LOG_WARNING("varchar type column should be the last short key.");
            remove_dir(header_dir);
            return OLAP_ERR_SCHEMA_SCHEMA_INVALID;
        }

        header.add_column();
        if (true == is_schema_change_table) {
            /*
             * schema change的old_olap_table和new_olap_table的schema进行比较
             * 1. 新表的列名在旧表中存在，则新表相应列的unique_id复用旧表列的unique_id 
             * 2. 新表的列名在旧表中不存在，则新表相应列的unique_id设为旧表列的next_unique_id
             *    
            */
            size_t field_num = ref_olap_table->tablet_schema().size();
            size_t field_off = 0;
            for (field_off = 0; field_off < field_num; ++field_off) {
                if (ref_olap_table->tablet_schema()[field_off].name == column.column_name) {
                    uint32_t unique_id = ref_olap_table->tablet_schema()[field_off].unique_id;
                    header.mutable_column(i)->set_unique_id(unique_id);
                    break;
                }
            }
            if (field_off == field_num) {
                header.mutable_column(i)->set_unique_id(next_unique_id++);
            }
        } else {
            header.mutable_column(i)->set_unique_id(i);
        }
        header.mutable_column(i)->set_name(column.column_name);
        header.mutable_column(i)->set_is_root_column(true);
        string data_type;
        EnumToString(TPrimitiveType, column.column_type.type, data_type);
        header.mutable_column(i)->set_type(data_type);

        if (column.column_type.type == TPrimitiveType::DECIMAL) {
            if (column.column_type.__isset.precision && column.column_type.__isset.scale) {
                header.mutable_column(i)->set_precision(column.column_type.precision);
                header.mutable_column(i)->set_frac(column.column_type.scale);
            } else {
                OLAP_LOG_WARNING("decimal type column should set precision and frac.");
                remove_dir(header_dir);
                return OLAP_ERR_SCHEMA_SCHEMA_INVALID;
            }
        } 

        if (column.column_type.type == TPrimitiveType::CHAR
                || column.column_type.type == TPrimitiveType::VARCHAR || column.column_type.type == TPrimitiveType::HLL) {
            if (!column.column_type.__isset.len) {
                remove_dir(header_dir);
                OLAP_LOG_WARNING("CHAR or VARCHAR should specify length. [type=%d]",
                                 column.column_type.type);
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }
        }
        uint32_t length = FieldInfo::get_field_length_by_type(
                column.column_type.type, column.column_type.len);
        header.mutable_column(i)->set_length(length);

        header.mutable_column(i)->set_index_length(length);
        if (column.column_type.type == TPrimitiveType::VARCHAR || column.column_type.type == TPrimitiveType::HLL) {
            if (!column.column_type.__isset.index_len) {
                header.mutable_column(i)->set_index_length(10);
            } else {
                header.mutable_column(i)->set_index_length(column.column_type.index_len);
            }
        }
        
        if (!column.is_key) {
            header.mutable_column(i)->set_is_key(false);
            string aggregation_type;
            EnumToString(TAggregationType, column.aggregation_type, aggregation_type);
            header.mutable_column(i)->set_aggregation(aggregation_type);
        } else {
            ++key_count;
            header.add_selectivity(1);
            header.mutable_column(i)->set_is_key(true);
            header.mutable_column(i)->set_aggregation("NONE");
        }

        if (column.__isset.default_value) {
            header.mutable_column(i)->set_default_value(column.default_value);
        }

        if (column.__isset.is_allow_null) {
            header.mutable_column(i)->set_is_allow_null(column.is_allow_null);
        } else {
            header.mutable_column(i)->set_is_allow_null(false);
        }

        if (column.__isset.is_bloom_filter_column) {
            header.mutable_column(i)->set_is_bf_column(column.is_bloom_filter_column);
            has_bf_columns = true;
        }

        ++i;
    }
    if (true == is_schema_change_table){
        /* 
         * schema change时，新表的next_unique_id应保证大于等于旧表的next_unique_id,
         * 以防止出现先删除列，后加相同列的两次linked schema change的列unique_id出现混淆
         * */
        header.set_next_column_unique_id(next_unique_id);
    } else {
        header.set_next_column_unique_id(i);
    }

    if (has_bf_columns && request.tablet_schema.__isset.bloom_filter_fpp) {
        header.set_bf_fpp(request.tablet_schema.bloom_filter_fpp);
    }

    if (key_count < request.tablet_schema.short_key_column_count) {
        OLAP_LOG_WARNING("short key num should not large than key num. "
                         "[key_num=%d short_key_num=%d]",
                         key_count, request.tablet_schema.short_key_column_count);
        remove_dir(header_dir);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // save header file
    header.set_creation_time(time(NULL));
    header.set_cumulative_layer_point(-1);
    res = header.save();
    if (res != OLAP_SUCCESS) {
        remove_dir(header_dir);
        return res;
    }

    *header_path = header_path_tmp;
    return res;
}

OLAPStatus OLAPEngine::_check_existed_or_else_create_dir(const string& path) {
    if (check_dir_existed(path)) {
        OLAP_LOG_WARNING("failed to create the dir that existed. [path='%s']", path.c_str());
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
        for (SmartOLAPTable olap_table : tablet_instance.second.table_arr) {
            if (olap_table.get() == NULL) {
                OLAP_LOG_WARNING("get empty SmartOLAPTable. [tablet_id=%ld]", tablet_instance.first);
                continue;
            }

            bool ret = olap_table->get_schema_change_request(
                    &tablet_id, &schema_hash, &schema_change_versions, &type);
            if (!ret) {
                continue;
            }

            SmartOLAPTable new_olap_table = get_table(tablet_id, schema_hash);
            if (new_olap_table.get() == NULL) {
                OLAP_LOG_WARNING("the table referenced by schema change cannot be found. "
                                 "schema change cancelled. [tablet='%s']",
                                 olap_table->full_name().c_str());
                continue;
            }

            new_olap_table->set_schema_change_status(
                    ALTER_TABLE_FAILED, olap_table->schema_hash(), -1);
            olap_table->set_schema_change_status(
                    ALTER_TABLE_FAILED, new_olap_table->schema_hash(), -1);
            OLAP_LOG_DEBUG("cancel unfinished schema change. [tablet='%s']",
                          olap_table->full_name().c_str());
            ++canceled_num;
        }
    }

    OLAP_LOG_INFO("finish to cancel unfinished schema change! [canceled_num=%lu]", canceled_num);
}

}  // namespace palo
