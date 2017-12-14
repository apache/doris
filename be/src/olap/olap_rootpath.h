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

#ifndef BDG_PALO_BE_SRC_OLAP_OLAP_ROOTPATH_H
#define BDG_PALO_BE_SRC_OLAP_OLAP_ROOTPATH_H

#include <list>
#include <memory>
#include <queue>
#include <sstream>
#include <stack>
#include <string>
#include <utility>
#include <vector>

#include "olap/olap_cond.h"
#include "olap/olap_define.h"

namespace palo {

struct OLAPRootPathStat {
    OLAPRootPathStat(): 
          disk_total_capacity(0),
          data_used_capacity(0),
          disk_available_capacity(0),
          is_used(false) {}

    std::string root_path;
    int64_t disk_total_capacity;
    int64_t data_used_capacity;
    int64_t disk_available_capacity;
    bool is_used;
};

/*
 * 目前所谓的RootPath指的是storage_root_path，其目录组织结构如下:
 *
 * Storage_root_path  ==>根目录，由配置指定
 *  |-data            ==>存放有效数据的目录，又称tables_root_path
 *  |-trash           ==>存放已删除的tablet数据（回收站），
 *  `-snapshot        ==>存放snapshot的目录
 */
class OLAPRootPath {
    DECLARE_SINGLETON(OLAPRootPath);
public:
    typedef std::vector<std::string> RootPathVec;
    typedef std::vector<int64_t> CapacityVec;

    // @brief 初始化。
    // 从配置文件中读取storage_root_path信息，重名的path当成一条path，
    // 校验各root_path的目录、磁盘等。
    OLAPStatus init();

    OLAPStatus clear();

    // @brief 查询root_path是否可用
    // @param root_path [in] 被查询的root_path
    // @param is_used [out] 是否使用的状态
    OLAPStatus get_root_path_used_stat(const std::string& root_path, bool* is_used);
    
    // @brief 设置root_path是否可用
    OLAPStatus set_root_path_used_stat(const std::string& root_path, bool is_used);
    
    // @brief 获取当前可用的root_path，并根据可用容量大小排序
    void get_all_available_root_path(RootPathVec* all_available_root_path);
    
    // @brief 获取所有root_path信息
    OLAPStatus get_all_disk_stat(std::vector<OLAPRootPathStat>* disks_stat);
    OLAPStatus get_all_root_path_stat(std::vector<OLAPRootPathStat>* root_paths_stat);

    // @brief 重新加载root_paths信息，全量操作。
    // 对于新增的root_path，同init操作
    // 对于删除的root_path，要同时从内存中删除相关表。
    // 对于未变的，如果之前为不可使用状态，则需要重新进行root_path检测
    //
    // NOTE: be/ce scheduler policy doesn't support
    OLAPStatus reload_root_paths(const char* root_paths);

    OLAPStatus register_table_into_root_path(OLAPTable* olap_table);

    OLAPStatus unregister_table_from_root_path(OLAPTable* olap_table);

    // 磁盘状态监测。监测unused_flag路劲新的对应root_path unused标识位，
    // 当检测到有unused标识时，从内存中删除对应表信息，磁盘数据不动。
    // 当磁盘状态为不可用，但未检测到unused标识时，需要从root_path上
    // 重新加载数据。
    void start_disk_stat_monitor();

    // get root path for creating table. The returned vector of root path should be random, 
    // for avoiding that all the table would be deployed one disk.
    void get_root_path_for_create_table(
            TStorageMedium::type storage_medium, RootPathVec *root_path);
    void get_table_data_path(std::vector<std::string>* data_paths);

    uint32_t available_storage_medium_type_count() {
        return _available_storage_medium_type_count;
    }

    uint32_t total_storage_medium_type_count() {
        return _total_storage_medium_type_count;
    }

    int32_t effective_cluster_id() const {
        return _effective_cluster_id;
    }

    OLAPStatus get_root_path_shard(const std::string& root_path, uint64_t* shard);

    static bool is_ssd_disk(const std::string& file_path);

    uint32_t get_file_system_count() {
        return _root_paths.size();
    }

    virtual OLAPStatus set_cluster_id(int32_t cluster_id);

    OLAPStatus check_all_root_path_cluster_id(
            const std::vector<std::string>& root_path_vec,
            const std::vector<bool>& is_accessable_vec);

private:
    struct RootPathInfo {
        RootPathInfo():
                capacity(0),
                available(0),
                current_shard(0),
                is_used(false),
                to_be_deleted(false) {}

        std::string file_system;            // 目录对应的磁盘分区
        std::string unused_flag_file;       // 不可用标识对应的文件名
        int64_t capacity;                  // 总空间，单位字节
        int64_t available;                 // 可用空间，单位字节
        uint64_t current_shard;             // shard按0,1...方式编号，最大的shard号
        bool is_used;                       // 是否可用标识
        bool to_be_deleted;                 // 删除标识，如在reload时删除某一目录
        TStorageMedium::type storage_medium;  // 存储介质类型：SSD|HDD
        std::set<TableInfo> table_set;
    };

    typedef std::map<std::string, RootPathInfo> RootPathMap;

    // 检测磁盘。主要通过周期地读写4K的测试数据
    void _start_check_disks();

    bool _used_disk_not_enough(uint32_t unused_num, uint32_t total_num);

    OLAPStatus _check_existed_root_path(const std::string& root_path, int64_t* capacity);

    OLAPStatus _check_root_paths(
            RootPathVec& root_path_vec,
            CapacityVec* capacity_vec,
            std::vector<bool>* is_accessable_vec);

    OLAPStatus _parse_root_paths_from_string(
            const char* root_paths,
            RootPathVec* root_path_vec,
            CapacityVec* capacity_vec);

    OLAPStatus _get_root_path_capacity(
            const std::string& root_path,
            int64_t* data_used);

    OLAPStatus _get_disk_capacity(
            const std::string& root_path,
            int64_t* capacity,
            int64_t* available);

    OLAPStatus _get_root_path_file_system(const std::string& root_path, std::string* file_system);

    OLAPStatus _get_root_path_current_shard(const std::string& root_path, uint64_t* shard);

    OLAPStatus _config_root_path_unused_flag_file(
            const std::string& root_path,
            std::string* unused_flag_file);

    OLAPStatus _create_unused_flag_file(std::string& unused_flag_file);

    OLAPStatus _update_root_path_info(const std::string& root_path, RootPathInfo* root_path_info);

    OLAPStatus _read_and_write_test_file(const std::string& root_path);

    void _delete_tables_on_unused_root_path();

    void _detect_unused_flag();

    void _remove_all_unused_flag_file();

    void _update_storage_medium_type_count();

    OLAPStatus _get_cluster_id_path_vec(std::vector<std::string>* cluster_id_path_vec);

    OLAPStatus _get_cluster_id_from_path(const std::string& path, int32_t* cluster_id);
    OLAPStatus _write_cluster_id_to_path(const std::string& path, int32_t cluster_id);

    OLAPStatus _judge_and_update_effective_cluster_id(int32_t cluster_id);

    OLAPStatus _check_recover_root_path_cluster_id(const std::string& root_path);

    bool _check_root_path_exist(const std::string& root_path);

    RootPathMap _root_paths;
    std::string _unused_flag_path;
    char* _test_file_write_buf;
    char* _test_file_read_buf;
    uint32_t _rand_seed;
    uint32_t _total_storage_medium_type_count;
    uint32_t _available_storage_medium_type_count;

    int32_t _effective_cluster_id;
    bool _is_all_cluster_id_exist;

    // 错误磁盘所在百分比，超过设定的值，则engine需要退出运行
    uint32_t _min_percentage_of_error_disk;
    MutexLock _mutex;
    static const size_t TEST_FILE_BUF_SIZE = 4096;
    static const size_t DIRECT_IO_ALIGNMENT = 512;

    DISALLOW_COPY_AND_ASSIGN(OLAPRootPath);
}; // class OLAPRootPath

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_OLAP_ROOTPATH_H

