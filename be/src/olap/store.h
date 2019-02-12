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

#include <cstdint>
#include <set>
#include <string>
#include <mutex>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "olap/olap_common.h"
#include "olap/olap_engine.h"

namespace doris {

class OLAPRootPath;
class OLAPEngine;

// A OlapStore used to manange data in same path.
// Now, After OlapStore was created, it will never be deleted for easy implementation.
class OlapStore {
public:
    OlapStore(const std::string& path, int64_t capacity_bytes = -1);
    ~OlapStore();

    Status load();

    const std::string& path() const { return _path; }
    const int64_t path_hash() const { return _path_hash; }
    bool is_used() const { return _is_used; }
    void set_is_used(bool is_used) { _is_used = is_used; }
    int32_t cluster_id() const { return _cluster_id; }
    RootPathInfo to_root_path_info() {
        RootPathInfo info;
        info.path = _path;
        info.path_hash = _path_hash;
        info.is_used = _is_used;
        info.capacity = _capacity_bytes;
        return info;
    }

    Status set_cluster_id(int32_t cluster_id);
    void health_check();

    OLAPStatus get_shard(uint64_t* shard);

    OlapMeta* get_meta();

    bool is_ssd_disk() const {
        return _storage_medium == TStorageMedium::SSD;
    }
    TStorageMedium::type storage_medium() const { return _storage_medium; }

    OLAPStatus register_table(OLAPTable* table);
    OLAPStatus deregister_table(OLAPTable* table);

    std::string get_tablet_schema_hash_path_from_header(OLAPHeader* header);

    std::string get_tablet_path_from_header(OLAPHeader* header);

    std::string get_shard_path_from_header(const std::string& shard_string);

    void find_tablet_in_trash(int64_t tablet_id, std::vector<std::string>* paths);

    static std::string get_root_path_from_schema_hash_path_in_trash(const std::string& schema_hash_dir_in_trash);

    OLAPStatus load_tables(OLAPEngine* engine);
    OLAPStatus check_none_row_oriented_table_in_store(OLAPEngine* engine);
    OLAPStatus _check_none_row_oriented_table_in_store(
                        OLAPEngine* engine, TTabletId tablet_id,
                        TSchemaHash schema_hash, const std::string& header);

private:
    std::string _cluster_id_path() const { return _path + CLUSTER_ID_PREFIX; }
    Status _init_cluster_id();
    Status _check_path_exist();
    Status _init_extension_and_capacity();
    Status _init_file_system();
    Status _init_meta();

    Status _check_disk();
    OLAPStatus _read_and_write_test_file();
    Status _read_cluster_id(const std::string& path, int32_t* cluster_id);
    Status _write_cluster_id_to_path(const std::string& path, int32_t cluster_id); 

    OLAPStatus _load_table_from_header(OLAPEngine* engine, TTabletId tablet_id,
                TSchemaHash schema_hash, const std::string& header);

    void _deal_with_header_error(TTabletId tablet_id, TSchemaHash schema_hash, int shard);

private:
    friend class OLAPRootPath;
    friend class OLAPEngine;
    
    std::string _path;
    int64_t _path_hash;
    int32_t _cluster_id;
    uint32_t _rand_seed;

    std::string _file_system;
    int64_t _capacity_bytes;
    int64_t _available_bytes;
    int64_t _used_bytes;
    uint64_t _current_shard;
    bool _is_used;
    // This flag will be set true if this store was not in root path when reloading
    bool _to_be_deleted;

    std::mutex _mutex;
    TStorageMedium::type _storage_medium;  // 存储介质类型：SSD|HDD
    std::set<TabletInfo> _tablet_set;

    static const size_t TEST_FILE_BUF_SIZE = 4096;
    static const size_t DIRECT_IO_ALIGNMENT = 512;
    static const uint32_t MAX_SHARD_NUM = 1024;
    char* _test_file_read_buf;
    char* _test_file_write_buf;
    OlapMeta* _meta;
};

}
