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

#include "olap/olap_rootpath.h"

#include <ctype.h>
#include <mntent.h>
#include <mntent.h>
#include <stdio.h>
#include <sys/file.h>
#include <sys/statfs.h>
#include <sys/statfs.h>
#include <utime.h>

#include <algorithm>
#include <algorithm>
#include <fstream>
#include <iterator>
#include <map>
#include <random>
#include <set>
#include <sstream>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/filesystem.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <errno.h>

#include "olap/file_helper.h"
#include "olap/olap_engine.h"
#include "olap/utils.h"

using boost::filesystem::canonical;
using boost::filesystem::file_size;
using boost::filesystem::is_directory;
using boost::filesystem::path;
using boost::filesystem::recursive_directory_iterator;
using boost::interprocess::file_lock;
using std::find;
using std::fstream;
using std::make_pair;
using std::nothrow;
using std::pair;
using std::random_device;
using std::random_shuffle;
using std::set;
using std::sort;
using std::string;
using std::stringstream;
using std::unique;
using std::vector;

namespace palo {

static const char* const kMtabPath = "/etc/mtab";
static const char* const kTouchPath = "/.touch_flag";
static const char* const kUnusedFlagFilePrefix = "unused";
static const char* const kTestFilePath = "/.testfile";

OLAPRootPath::OLAPRootPath() :
        is_report_disk_state_already(false),
        is_report_olap_table_already(false),
        _test_file_write_buf(NULL),
        _test_file_read_buf(NULL),
        _total_storage_medium_type_count(0),
        _available_storage_medium_type_count(0),
        _effective_cluster_id(-1),
        _is_all_cluster_id_exist(true),
        _is_drop_tables(false) {}

OLAPRootPath::~OLAPRootPath() {
    clear();
}

OLAPStatus OLAPRootPath::init() {
    OLAPStatus res = OLAP_SUCCESS;
    string& root_paths = config::storage_root_path;
    OLAP_LOG_DEBUG("root_path='%s'. ", root_paths.c_str());
    RootPathVec root_path_vec;
    CapacityVec capacity_vec;

    _rand_seed = static_cast<uint32_t>(time(NULL));
    _root_paths.clear();
    _min_percentage_of_error_disk = config::min_percentage_of_error_disk;

    if (posix_memalign((void**)&_test_file_write_buf,
                       DIRECT_IO_ALIGNMENT,
                       TEST_FILE_BUF_SIZE) != 0) {
        OLAP_LOG_WARNING("fail to malloc _test_file_write_buf. [size=%lu]", TEST_FILE_BUF_SIZE);
        clear();

        return OLAP_ERR_MALLOC_ERROR;
    }

    if (posix_memalign((void**)&_test_file_read_buf,
                       DIRECT_IO_ALIGNMENT,
                       TEST_FILE_BUF_SIZE) != 0) {
        OLAP_LOG_WARNING("fail to malloc _test_file_read_buf. [size=%lu]", TEST_FILE_BUF_SIZE);
        clear();
        
        return OLAP_ERR_MALLOC_ERROR;
    }

    _unused_flag_path = string(getenv("LOG_DIR")) + UNUSED_PREFIX; 
    if (!check_dir_existed(_unused_flag_path)) {
        if ((res = create_dir(_unused_flag_path)) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create unused flag path.[path='%s']",
                             _unused_flag_path.c_str());
            clear();
            
            return res;
        }
    } else {
        _remove_all_unused_flag_file();
    }

    res = parse_root_paths_from_string(root_paths.c_str(), &root_path_vec, &capacity_vec);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("get root path failed. [res=%d root_paths='%s']",
                         res, root_paths.c_str());
        clear();

        return res;
    }

    vector<bool> is_accessable_vec;
    res = _check_root_paths(root_path_vec, &capacity_vec, &is_accessable_vec);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to check root path. [res=%d]", res);
        clear();

        return res;
    }

    _effective_cluster_id = config::cluster_id;
    res = check_all_root_path_cluster_id(root_path_vec, is_accessable_vec);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to check cluster info. [res=%d]", res);
        clear();
        return res;
    }

    for (size_t i = 0; i < root_path_vec.size(); ++i) {
        RootPathInfo root_path_info;
        root_path_info.path = root_path_vec[i];
        root_path_info.is_used = true;
        root_path_info.capacity = capacity_vec[i];
        res = _update_root_path_info(root_path_vec[i], &root_path_info);
        if (res != OLAP_SUCCESS || !is_accessable_vec[i]) {
            OLAP_LOG_WARNING("fail to update root path info[root path='%s']",
                             root_path_vec[i].c_str());
            root_path_info.is_used = false;
            _create_unused_flag_file(root_path_info.unused_flag_file);
        }

        _root_paths.insert(pair<string, RootPathInfo>(root_path_vec[i], root_path_info));
    }

    _update_storage_medium_type_count();

    return res;
}

OLAPStatus OLAPRootPath::clear() {
    _root_paths.clear();

    if (_test_file_read_buf != NULL) {
        free(_test_file_read_buf);
        _test_file_read_buf = NULL;
    }
    if (_test_file_write_buf != NULL) {
        free(_test_file_write_buf);
        _test_file_write_buf = NULL;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPRootPath::get_root_path_used_stat(const string& root_path, bool* is_used) {
    OLAPStatus res = OLAP_SUCCESS;
    *is_used = false;

    _mutex.lock();

    RootPathMap::iterator it = _root_paths.find(root_path);
    if (it != _root_paths.end()) {
        *is_used = it->second.is_used;
    } else {
        res = OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    _mutex.unlock();

    return res;
}

OLAPStatus OLAPRootPath::set_root_path_used_stat(const string& root_path, bool is_used) {
    OLAPStatus res = OLAP_SUCCESS;

    _mutex.lock();
    RootPathMap::iterator it = _root_paths.find(root_path);
    if (it != _root_paths.end()) {
        it->second.is_used = is_used;
        if (!is_used) {
            if ((res = _create_unused_flag_file(it->second.unused_flag_file)) != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to create unused flag file."
                                 "[root_path='%s' unused_flag_file='%s']",
                                 root_path.c_str(),
                                 it->second.unused_flag_file.c_str());
            }
        }
    } else {
        res = OLAP_ERR_INPUT_PARAMETER_ERROR;
    }
    _mutex.unlock();

    _update_storage_medium_type_count();

    return res;
}

void OLAPRootPath::get_all_available_root_path(RootPathVec* all_available_root_path) {
    all_available_root_path->clear();
    _mutex.lock();

    for (RootPathMap::iterator it = _root_paths.begin(); it != _root_paths.end(); ++it) {
        if (it->second.is_used) {
            all_available_root_path->push_back(it->first);
        }
    }

    _mutex.unlock();
}

OLAPStatus OLAPRootPath::get_all_root_path_info(
        vector<RootPathInfo>* root_paths_info,
        bool need_capacity) {

    OLAPStatus res = OLAP_SUCCESS;
    root_paths_info->clear();

    _mutex.lock();
    for (RootPathMap::iterator it = _root_paths.begin(); it != _root_paths.end(); ++it) {
        RootPathInfo info;
        info.path = it->first;
        info.is_used = it->second.is_used;
        info.capacity = it->second.capacity;
        root_paths_info->push_back(info);
    }
    _mutex.unlock();

    if (need_capacity) {
        for (auto& info: *root_paths_info) {
            if (info.is_used) {
                _get_root_path_capacity(info.path, &info.data_used_capacity, &info.available);
            } else {
                info.capacity = 1;
                info.data_used_capacity = 0;
                info.available = 0;
            }
        }
    }

    return res;
}

OLAPStatus OLAPRootPath::reload_root_paths(const char* root_paths) {
    OLAPStatus res = OLAP_SUCCESS;

    RootPathVec root_path_vec;
    CapacityVec capacity_vec;
    res = parse_root_paths_from_string(root_paths, &root_path_vec, &capacity_vec);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("get root path failed when reload root path. [root_paths=%s]", root_paths);
        return res;
    }

    vector<bool> is_accessable_vec;
    res = _check_root_paths(root_path_vec, &capacity_vec, &is_accessable_vec);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to check reload root paths. [res=%d root_paths=%s]",
                         res, root_paths);
        return res;
    }

    res = check_all_root_path_cluster_id(root_path_vec, is_accessable_vec);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to check cluster info. [res=%d]", res);
        return res;
    }

    _mutex.lock();

    _remove_all_unused_flag_file();

    for (RootPathMap::iterator it = _root_paths.begin(); it != _root_paths.end(); ++it) {
        if (root_path_vec.end() == find(root_path_vec.begin(), root_path_vec.end(), it->first)) {
            it->second.to_be_deleted = true;
        }
    }

    OLAPRootPath::RootPathVec root_path_to_be_loaded;
    for (size_t i = 0; i < root_path_vec.size(); ++i) {
        RootPathMap::iterator iter_root_path = _root_paths.find(root_path_vec[i]);
        if (iter_root_path == _root_paths.end()) {
            RootPathInfo root_path_info;
            root_path_info.path = root_path_vec[i];
            root_path_info.is_used = true;
            root_path_info.capacity = capacity_vec[i];
            root_path_to_be_loaded.push_back(root_path_vec[i]);
            res = _update_root_path_info(root_path_vec[i], &root_path_info);
            if (res != OLAP_SUCCESS || !is_accessable_vec[i]) {
                OLAP_LOG_WARNING("fail to update root path info.[root path='%s']",
                                 root_path_vec[i].c_str());
                root_path_info.is_used = false;
                root_path_to_be_loaded.pop_back();
                _create_unused_flag_file(root_path_info.unused_flag_file);
            }

            _root_paths.insert(pair<string, RootPathInfo>(root_path_vec[i], root_path_info));
        } else {
            if (!iter_root_path->second.is_used) {
                iter_root_path->second.is_used = true;
                iter_root_path->second.capacity = capacity_vec[i];
                root_path_to_be_loaded.push_back(root_path_vec[i]);
                res = _update_root_path_info(iter_root_path->first, &iter_root_path->second);
                if (res != OLAP_SUCCESS || !is_accessable_vec[i]) {
                    iter_root_path->second.is_used = false;
                    root_path_to_be_loaded.pop_back();
                    _create_unused_flag_file(iter_root_path->second.unused_flag_file);
                }
            }
        }
    }

    vector<TableInfo> table_info_vec;
    for (RootPathMap::iterator iter_root_path = _root_paths.begin();
            iter_root_path != _root_paths.end();) {
        if (iter_root_path->second.to_be_deleted) {
            for (set<TableInfo>::iterator iter_table = iter_root_path->second.table_set.begin();
                    iter_table != iter_root_path->second.table_set.end();
                    ++iter_table) {
                table_info_vec.push_back(*iter_table);
            }

            _root_paths.erase(iter_root_path++);
        } else {
            ++iter_root_path;
        }
    }

    _mutex.unlock();

    _update_storage_medium_type_count();

    OLAPEngine::get_instance()->drop_tables_on_error_root_path(table_info_vec);
    OLAPEngine::get_instance()->load_root_paths(root_path_to_be_loaded);

    return OLAP_SUCCESS;
}

OLAPStatus OLAPRootPath::register_table_into_root_path(OLAPTable* olap_table) {
    OLAPStatus res = OLAP_SUCCESS;
    _mutex.lock();

    RootPathMap::iterator it = _root_paths.find(olap_table->storage_root_path_name());
    if (it == _root_paths.end()) {
        OLAP_LOG_WARNING("fail to register table into root path.[root_path='%s' table='%s']",
                         olap_table->storage_root_path_name().c_str(),
                         olap_table->full_name().c_str());
        res = OLAP_ERR_INVALID_ROOT_PATH;
    } else {
        TableInfo table_info(olap_table->tablet_id(),
                             olap_table->schema_hash());
        it->second.table_set.insert(table_info);
    }

    _mutex.unlock();

    return res;
}

OLAPStatus OLAPRootPath::unregister_table_from_root_path(OLAPTable* olap_table) {
    _mutex.lock();

    RootPathMap::iterator it = _root_paths.find(olap_table->storage_root_path_name());
    if (it == _root_paths.end()) {
        OLAP_LOG_WARNING("fail to unregister table into root path.[root_path='%s' table='%s']",
                         olap_table->storage_root_path_name().c_str(),
                         olap_table->full_name().c_str());
    } else {
        TableInfo table_info(olap_table->tablet_id(),
                             olap_table->schema_hash());
        it->second.table_set.erase(table_info);
    }

    _mutex.unlock();

    return OLAP_SUCCESS;
}

void OLAPRootPath::start_disk_stat_monitor() {
    _start_check_disks();
    _detect_unused_flag();
    _delete_tables_on_unused_root_path();
    
    // if drop tables
    // notify disk_state_worker_thread and olap_table_worker_thread until they received
    if (_is_drop_tables) {
        disk_broken_cv.notify_all();

        bool is_report_disk_state_expected = true;
        bool is_report_olap_table_expected = true;
        bool is_report_disk_state_exchanged = 
                is_report_disk_state_already.compare_exchange_strong(is_report_disk_state_expected, false);
        bool is_report_olap_table_exchanged =
                is_report_olap_table_already.compare_exchange_strong(is_report_olap_table_expected, false);
        if (is_report_disk_state_exchanged && is_report_olap_table_exchanged) {
            _is_drop_tables = false;
        }
    }
}


void OLAPRootPath::_start_check_disks() {
    OLAPRootPath::RootPathVec all_available_root_path;
    get_all_available_root_path(&all_available_root_path);
    for (OLAPRootPath::RootPathVec::iterator iter = all_available_root_path.begin();
            iter != all_available_root_path.end(); ++iter) {
        OLAPStatus res = OLAP_SUCCESS;
        if ((res = _read_and_write_test_file(*iter)) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("root path occur IO Error. [root path='%s']",
                             iter->c_str());

            if (is_io_error(res)) {
                set_root_path_used_stat(*iter, false);
            }
        }
    }
}

bool OLAPRootPath::_used_disk_not_enough(uint32_t unused_num, uint32_t total_num) {
    return ((total_num == 0) || (unused_num * 100 / total_num > _min_percentage_of_error_disk));
}

OLAPStatus OLAPRootPath::_check_root_paths(
        RootPathVec& root_path_vec,
        CapacityVec* capacity_vec,
        vector<bool>* is_accessable_vec) {
    OLAPStatus res = OLAP_SUCCESS;

    size_t i = 0;
    for (RootPathVec::iterator iter = root_path_vec.begin(); iter != root_path_vec.end();) {
        // if root path exist:
        //     check extension, capacity and create subdir;
        // else:
        //     if root path can be created, return error;
        //     otherwise, the disk may be corrupted, check it when recover.
        if (_check_root_path_exist(*iter)) {
            string align_tag_path = *iter + ALIGN_TAG_PREFIX;
            if (access(align_tag_path.c_str(), F_OK) == 0) {
                OLAP_LOG_WARNING("disk with align tag find. [root_path='%s']", (*iter).c_str());
                root_path_vec.erase(iter);
                continue;
            }

            res = _check_existed_root_path(*iter, &(*capacity_vec)[i]);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to check existed root path. [res=%d]", res);
                return res;
            }

            is_accessable_vec->push_back(true);
            ++i;
            ++iter;
        } else {
            path boost_path = *iter;
            path last_boost_path = boost_path;
            boost_path = boost_path.parent_path();
            while (!check_dir_existed(boost_path.string())) {
                last_boost_path = boost_path;
                boost_path = boost_path.parent_path();
            }

            res = create_dirs(*iter);
            if (res != OLAP_SUCCESS && errno != EACCES) {
                OLAP_LOG_WARNING("root path is unusable! [root_path='%s' err='%m']",
                                 (*iter).c_str());
                is_accessable_vec->push_back(false);
                ++i;
                ++iter;
                continue;
            } else {
                OLAP_LOG_WARNING("root path not exist. [root_path='%s']", (*iter).c_str());
                remove_dir(last_boost_path.string());
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPRootPath::_check_existed_root_path(
        const std::string& root_path,
        int64_t* capacity) {
    path boost_path = root_path;
    string extension = canonical(boost_path).extension().string();
    if (extension != "" && extension != ".SSD" && extension != ".ssd"
            && extension != ".HDD" && extension != ".hdd") {
        OLAP_LOG_WARNING("root path has wrong extension. [root_path='%s']",
                         root_path.c_str());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    int64_t disk_capacity = space(boost_path).capacity;
    if (*capacity == -1) {
        *capacity = disk_capacity;
    } else if (*capacity > disk_capacity) {
        OLAP_LOG_WARNING("root path capacity should not larger than disk capacity. "
                         "[root_path='%s' root_path_capacity=%lu disk_capacity=%lu]",
                         root_path.c_str(), *capacity, disk_capacity);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    } else {
        *capacity = *capacity;
    }

    string data_path = root_path + DATA_PREFIX;
    if (!check_dir_existed(data_path) && create_dir(data_path) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("failed to create data root path. [path='%s']", data_path.c_str());
        return OLAP_ERR_CANNOT_CREATE_DIR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPRootPath::_read_and_write_test_file(const string& root_path) {
    OLAPStatus res = OLAP_SUCCESS;
    string test_file = root_path + kTestFilePath;
    FileHandler file_handler;

    if (access(test_file.c_str(), F_OK) == 0) {
        if (remove(test_file.c_str()) != 0) {
            OLAP_LOG_WARNING("fail to delete test file. [err='%m' path='%s']", test_file.c_str());
            return OLAP_ERR_IO_ERROR;
        }
    } else {
        if (errno != ENOENT) {
            OLAP_LOG_WARNING("fail to access test file. [err='%m' path='%s']", test_file.c_str());
            return OLAP_ERR_IO_ERROR;
        }
    }

    if ((res = file_handler.open_with_mode(test_file.c_str(),
                                           O_RDWR | O_CREAT | O_DIRECT,
                                           S_IRUSR | S_IWUSR)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to create test file. [file_name=%s]", test_file.c_str());
        return res;
    }

    for (size_t i = 0; i < TEST_FILE_BUF_SIZE; ++i) {
        int32_t tmp_value = rand_r(&_rand_seed);
        _test_file_write_buf[i] = static_cast<char>(tmp_value);
    }

    if ((res = file_handler.pwrite(_test_file_write_buf, TEST_FILE_BUF_SIZE, SEEK_SET)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to write test file. [file_name=%s]", test_file.c_str());
        return res;
    }

    if ((res = file_handler.pread(_test_file_read_buf, TEST_FILE_BUF_SIZE, SEEK_SET)) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to read test file. [file_name=%s]", test_file.c_str());
        return res;
    }

    if (memcmp(_test_file_write_buf, _test_file_read_buf, TEST_FILE_BUF_SIZE) != 0) {
        OLAP_LOG_WARNING("the test file write_buf and read_buf not equal.");
        return OLAP_ERR_TEST_FILE_ERROR;
    }

    if ((res = file_handler.close()) != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to close test file. [file_name=%s]", test_file.c_str());
        return res;
    }

    if (remove(test_file.c_str()) != 0) {
        OLAP_LOG_TRACE("fail to delete test file. [err='%m' path='%s']", test_file.c_str());
        return OLAP_ERR_IO_ERROR;
    }

    return res;
}

OLAPStatus OLAPRootPath::_create_unused_flag_file(string& unused_flag_file) {
    OLAPStatus res = OLAP_SUCCESS;
    string unused_flag_file_path = _unused_flag_path + "/" + unused_flag_file;
    if (!check_dir_existed(_unused_flag_path)) {
        if (OLAP_SUCCESS != (res = create_dir(_unused_flag_path))) {
            OLAP_LOG_WARNING("fail to create unused flag path.[path='%s']",
                             _unused_flag_path.c_str());
            return res;
        }
    }

    if (access(unused_flag_file_path.c_str(), F_OK) != 0) {
        int fd = open(unused_flag_file_path.c_str(),
                      O_RDWR | O_CREAT,
                      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if ((fd >= 0) && !close(fd)) {
            OLAP_LOG_TRACE("success to create unused flag file.[path='%s']",
                           unused_flag_file_path.c_str());
        } else {
            OLAP_LOG_WARNING("fail to create unused flag file.[err='%m' path='%s']",
                             _unused_flag_path.c_str());
            res = OLAP_ERR_OTHER_ERROR;
        }
    }

    return res;
}

OLAPStatus OLAPRootPath::parse_root_paths_from_string(
        const char* root_paths,
        RootPathVec* root_path_vec,
        CapacityVec* capacity_vec) {
    root_path_vec->clear();
    capacity_vec->clear();

    try {
        vector<string> item_vec;
        boost::split(item_vec, root_paths, boost::is_any_of(";"), boost::token_compress_on);
        for (string item : item_vec) {
            vector<string> tmp_vec;
            boost::split(tmp_vec, item, boost::is_any_of(","), boost::token_compress_on);

            // parse root path name
            boost::trim(tmp_vec[0]);
            tmp_vec[0].erase(tmp_vec[0].find_last_not_of("/") + 1);
            if (tmp_vec[0].size() == 0 || tmp_vec[0][0] != '/') {
                OLAP_LOG_WARNING("invalid root path name. [root_path='%s']",
                                 tmp_vec[0].c_str());
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }
            root_path_vec->push_back(tmp_vec[0]);

            // parse root path capacity
            if (tmp_vec.size() > 1) {
                if (!valid_signed_number<int64_t>(tmp_vec[1])
                        || strtol(tmp_vec[1].c_str(), NULL, 10) < 0) {
                    OLAP_LOG_WARNING("invalid capacity of root path. [capacity='%s']",
                                     tmp_vec[1].c_str());
                    return OLAP_ERR_INPUT_PARAMETER_ERROR;
                }

                capacity_vec->push_back(strtol(tmp_vec[1].c_str(), NULL, 10) * GB_EXCHANGE_BYTE);
            } else {
                capacity_vec->push_back(-1);
            }
        }
    } catch (...) {
        OLAP_LOG_WARNING("get root path failed. [root_paths: %s]", root_paths);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (root_path_vec->size() == 0) {
        OLAP_LOG_WARNING("there are no valid root path.");
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // verify if root path dumplicated
    vector<string> verify_vec = *root_path_vec;
    sort(verify_vec.begin(), verify_vec.end());
    verify_vec.erase(unique(verify_vec.begin(), verify_vec.end()), verify_vec.end());
    if (verify_vec.size() != root_path_vec->size()) {
        OLAP_LOG_WARNING("there are dumplicated root paths. [root_paths='%s']", root_paths);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPRootPath::_get_root_path_capacity(
        const string& root_path,
        int64_t* data_used,
        int64_t* disk_available) {
    OLAPStatus res = OLAP_SUCCESS;
    int64_t used = 0;

    OlapStopWatch watch;
    try {
        path boost_root_path(root_path + DATA_PREFIX);
        for (recursive_directory_iterator it(boost_root_path);
                it != recursive_directory_iterator(); ++it) {
            if (!is_directory(*it)) {
                used += file_size(*it);
            }
        }
        *data_used = used;

        boost::filesystem::path path_name(root_path);
        boost::filesystem::space_info path_info = boost::filesystem::space(path_name);
        *disk_available = path_info.available;
    } catch (boost::filesystem::filesystem_error& e) {
        OLAP_LOG_WARNING("get space info failed. [path: %s, erro:%s]", root_path.c_str(), e.what());
        return OLAP_ERR_STL_ERROR;
    }

    OLAP_LOG_INFO("get all root path capacity cost: %ld us", watch.get_elapse_time_us());
    return res;
}

OLAPStatus OLAPRootPath::_get_root_path_file_system(const string& root_path, string* file_system) {
    struct stat s;
    if (stat(root_path.c_str(), &s) != 0) {
        OLAP_LOG_WARNING("get path stat failed.[err=%m path='%s']", root_path.c_str());
        return OLAP_ERR_OS_ERROR;
    }

    dev_t mountDevice;
    if ((s.st_mode & S_IFMT) == S_IFBLK) {
        mountDevice = s.st_rdev;
    } else {
        mountDevice = s.st_dev;
    }

    FILE* mountTable = NULL;
    if ((mountTable = setmntent(kMtabPath, "r")) == NULL) {
        OLAP_LOG_WARNING("fail to open the file system description.[file='%s']", kMtabPath);
        return OLAP_ERR_OS_ERROR;
    }

    bool is_find = false;
    struct mntent* mountEntry = NULL;
    while ((mountEntry = getmntent(mountTable)) != NULL) {
        if (strcmp(root_path.c_str(), mountEntry->mnt_dir) == 0
                || strcmp(root_path.c_str(), mountEntry->mnt_fsname) == 0) {
            is_find = true;
            break;
        }

        if (stat(mountEntry->mnt_fsname, &s) == 0 && s.st_rdev == mountDevice) {
            is_find = true;
            break;
        }

        if (stat(mountEntry->mnt_dir, &s) == 0 && s.st_dev == mountDevice) {
            is_find = true;
            break;
        }
    }

    endmntent(mountTable);

    if (!is_find) {
        OLAP_LOG_WARNING("fail to find file system.[path='%s']", root_path.c_str());
        return OLAP_ERR_OS_ERROR;
    }

    file_system->assign(mountEntry->mnt_fsname);

    return OLAP_SUCCESS;
}

OLAPStatus OLAPRootPath::_get_root_path_current_shard(const string& root_path, uint64_t* shard) {
    OLAPStatus res = OLAP_SUCCESS;

    set<string> shards;
    res = dir_walk(root_path + DATA_PREFIX, &shards, NULL);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to dir walk root path.[root path='%s']",
                (root_path + DATA_PREFIX).c_str());
        return res;
    }

    errno = 0;
    char* end_ptr = NULL;
    uint64_t max_shard = 0;
    for (const auto& i : shards) {
        uint64_t j = strtoul(i.c_str(), &end_ptr, 10);
        if (*end_ptr != 0 || errno != 0) {
            OLAP_LOG_WARNING("fail to convert shard string to int. [shard='%s']", i.c_str());
            continue;
        }

        max_shard = j > max_shard ? j : max_shard;
    }

    *shard = max_shard;
    return res;
}

OLAPStatus OLAPRootPath::_config_root_path_unused_flag_file(const string& root_path,
                                                        string* unused_flag_file) {
    vector<string> vector_name_element;

    try {
        boost::split(vector_name_element, root_path,
                     boost::is_any_of("/"),
                     boost::token_compress_on);
    } catch (...) {
        OLAP_LOG_WARNING("get root path unused file name failed.[root_path='%s']",
                         root_path.c_str());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    *unused_flag_file = kUnusedFlagFilePrefix;

    for (vector<string>::iterator it = vector_name_element.begin();
            it != vector_name_element.end(); ++it) {
        if (it->size() == 0) {
            continue;
        }

        *unused_flag_file += "_" + *it;
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPRootPath::_update_root_path_info(
        const string& root_path, RootPathInfo* root_path_info) {
    OLAPStatus res = OLAP_SUCCESS;

    root_path_info->available = 0;
    root_path_info->to_be_deleted = false;
    root_path_info->table_set.clear();
    
    res = _config_root_path_unused_flag_file(root_path, &root_path_info->unused_flag_file);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get unused flag file.[root path='%s']", root_path.c_str());
        return res;
    }
    
    if (!_check_root_path_exist(root_path)) {
        OLAP_LOG_WARNING("root path not exist. [root_path='%s']", root_path.c_str());
        return OLAP_ERR_TEST_FILE_ERROR;
    }

    string align_tag_path = root_path + ALIGN_TAG_PREFIX;
    if (access(align_tag_path.c_str(), F_OK) == 0) {
        OLAP_LOG_WARNING("disk with align tag find. [root_path='%s']", root_path.c_str());
        return OLAP_ERR_INVALID_ROOT_PATH;
    }

    res = _check_existed_root_path(root_path, &root_path_info->capacity);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to check existed root path. [res=%d]", res);
        return res;
    }

    root_path_info->storage_medium = TStorageMedium::HDD;
    if (is_ssd_disk(root_path)) {
        root_path_info->storage_medium = TStorageMedium::SSD;
    }

    res = _get_root_path_file_system(root_path, &root_path_info->file_system);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get file system.[root path='%s']", root_path.c_str());
        return res;
    }

    res = _get_root_path_current_shard(root_path, &root_path_info->current_shard);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get current shard.[root path='%s']", root_path.c_str());
        return res;
    }

    return res;
}

bool OLAPRootPath::is_ssd_disk(const std::string& file_path) {
    path boost_root_path = file_path;
    string extension = canonical(boost_root_path).extension().string();
    if (extension == ".SSD" || extension == ".ssd") {
        return true;
    }
    return false;
}

void OLAPRootPath::_delete_tables_on_unused_root_path() {
    vector<TableInfo> table_info_vec;
    uint32_t unused_root_path_num = 0;
    uint32_t total_root_path_num = 0;
    _mutex.lock();

    for (RootPathMap::iterator iter_root_path = _root_paths.begin();
            iter_root_path != _root_paths.end(); ++iter_root_path) {
        total_root_path_num++;

        if (!iter_root_path->second.is_used) {
            unused_root_path_num++;

            for (set<TableInfo>::iterator iter_table = iter_root_path->second.table_set.begin();
                    iter_table != iter_root_path->second.table_set.end(); ++iter_table) {
                table_info_vec.push_back(*iter_table);
            }

            iter_root_path->second.table_set.clear();
        }
    }

    _mutex.unlock();

    if (_used_disk_not_enough(unused_root_path_num, total_root_path_num)) {
        OLAP_LOG_FATAL("engine stop running, because more than %d disks error."
                       "[total_disks=%d error_disks=%d]",
                       _min_percentage_of_error_disk,
                       total_root_path_num,
                       unused_root_path_num);
        exit(0);
    }

    if (!table_info_vec.empty()) {
        _is_drop_tables = true;
    }
    
    OLAPEngine::get_instance()->drop_tables_on_error_root_path(table_info_vec);
}

void OLAPRootPath::_detect_unused_flag() {
    set<string> unused_falg_files;
    if (!check_dir_existed(_unused_flag_path)) {
        if (create_dir(_unused_flag_path) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create unused flag path.[path='%s']",
                             _unused_flag_path.c_str());
        }

        return;
    }

    dir_walk(_unused_flag_path, NULL, &unused_falg_files);

    OLAPRootPath::RootPathVec root_paths_to_be_loaded;

    _mutex.lock();

    for (RootPathMap::iterator it = _root_paths.begin(); it != _root_paths.end(); ++it) {
        set<string>::iterator jt = unused_falg_files.find(it->second.unused_flag_file);
        if (!it->second.is_used) {
            if (jt == unused_falg_files.end()) {
                it->second.is_used = true;
                root_paths_to_be_loaded.push_back(it->first);
                OLAPStatus res = _update_root_path_info(it->first, &it->second);
                if (res != OLAP_SUCCESS) {
                    OLAP_LOG_WARNING("fail to update the root path info.[root path='%s']",
                                     it->first.c_str());
                    it->second.is_used = false;
                    root_paths_to_be_loaded.pop_back();
                    _create_unused_flag_file(it->second.unused_flag_file);
                } else {
                    res = _check_recover_root_path_cluster_id(it->first);
                    if (res != OLAP_SUCCESS) {
                        OLAP_LOG_WARNING("fail to check cluster id. [res=%d]", res);
                        it->second.is_used = false;
                        root_paths_to_be_loaded.pop_back();
                        _create_unused_flag_file(it->second.unused_flag_file);
                    }
                }
            }
        } else {
            if (jt != unused_falg_files.end()) {
                if (it->second.is_used) {
                    OLAP_LOG_WARNING("detect unused flag, unuse the rootpath."
                                     "[root_path='%s' flag='%s']",
                                     it->first.c_str(),
                                     jt->c_str());
                }

                it->second.is_used = false;
            }
        }
    }

    _mutex.unlock();

    _update_storage_medium_type_count();

    if (root_paths_to_be_loaded.size() > 0) {
        OLAPEngine::get_instance()->load_root_paths(root_paths_to_be_loaded);
    }
}

void OLAPRootPath::_remove_all_unused_flag_file() {
    set<string> unused_falg_files;
    dir_walk(_unused_flag_path, NULL, &unused_falg_files);

    for (set<string>::iterator it = unused_falg_files.begin();
            it != unused_falg_files.end(); ++it) {
        if (it->find(kUnusedFlagFilePrefix) == 0) {
            string unused_flag_file = _unused_flag_path + "/" + it->c_str();
            if (remove(unused_flag_file.c_str()) != 0) {
                OLAP_LOG_WARNING("fail to remove unused flag file.[file='%s']", it->c_str());
            }
        }
    }
}

void OLAPRootPath::get_root_path_for_create_table(
        TStorageMedium::type storage_medium, RootPathVec *root_path) {
    root_path->clear();

    _mutex.lock();
    for (RootPathMap::iterator it = _root_paths.begin(); it != _root_paths.end(); ++it) {
        if (it->second.is_used) {
            if (_available_storage_medium_type_count == 1
                    || it->second.storage_medium == storage_medium) {
                root_path->push_back(it->first);
            }
        }
    }
    _mutex.unlock();

    random_device rd;
    srand(rd());
    random_shuffle(root_path->begin(), root_path->end());
}

void OLAPRootPath::get_table_data_path(std::vector<std::string>* data_paths) {
    _mutex.lock();

    for (RootPathMap::iterator it = _root_paths.begin();
         it != _root_paths.end();
         ++it) {
            data_paths->push_back(it->first);
    }

    _mutex.unlock();
}

OLAPStatus OLAPRootPath::get_root_path_shard(const std::string& root_path, uint64_t* shard) {
    OLAPStatus res = OLAP_SUCCESS;
    AutoMutexLock auto_lock(&_mutex);

    RootPathMap::iterator it = _root_paths.find(root_path);
    if (it == _root_paths.end()) {
        OLAP_LOG_WARNING("fail to find root path. [root_path='%s']", root_path.c_str());
        return OLAP_ERR_NO_AVAILABLE_ROOT_PATH;
    }

    uint64_t current_shard = it->second.current_shard;
    stringstream shard_path_stream;
    shard_path_stream << root_path << DATA_PREFIX << "/" << current_shard;
    string shard_path = shard_path_stream.str();
    if (!check_dir_existed(shard_path)) {
        res = create_dir(shard_path);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create path. [path='%s']", shard_path.c_str());
            return res;
        }
    }

    set<string> tablets;
    res = dir_walk(shard_path, &tablets, NULL);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to dir walk root path. [res=%d root_path='%s']",
                         res, root_path.c_str());
        return res;
    }

    if (tablets.size() > config::max_tablet_num_per_shard) {
        ++current_shard;
        it->second.current_shard = current_shard;
    }

    *shard = current_shard;
    return OLAP_SUCCESS;
}

void OLAPRootPath::_update_storage_medium_type_count() {
    set<TStorageMedium::type> total_storage_medium_types;
    set<TStorageMedium::type> available_storage_medium_types;

    _mutex.lock();
    for (RootPathMap::iterator it = _root_paths.begin(); it != _root_paths.end(); ++it) {
        total_storage_medium_types.insert(it->second.storage_medium);
        if (it->second.is_used) {
            available_storage_medium_types.insert(it->second.storage_medium);
        }
    }
    _mutex.unlock();

    _total_storage_medium_type_count = total_storage_medium_types.size();
    _available_storage_medium_type_count = available_storage_medium_types.size();
}

OLAPStatus OLAPRootPath::_get_cluster_id_path_vec(
        vector<string>* cluster_id_path_vec) {
    OLAPStatus res = OLAP_SUCCESS;

    vector<RootPathInfo> root_path_info_vec;
    res = get_all_root_path_info(&root_path_info_vec, false);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get root path info. [res=%d]", res);
        return res;
    }

    for (const auto& info: root_path_info_vec) {
        if (info.is_used) {
            cluster_id_path_vec->push_back(info.path + CLUSTER_ID_PREFIX);
        }
    }

    return res;
}

OLAPStatus OLAPRootPath::_get_cluster_id_from_path(const string& path, int32_t* cluster_id) {
    OLAPStatus res = OLAP_SUCCESS;
    int32_t tmp_cluster_id = -1;

    fstream fs(path.c_str(), fstream::in);
    if (!fs.is_open()) {
        OLAP_LOG_WARNING("fail to open cluster id path. [path='%s']", path.c_str());
        return OLAP_ERR_IO_ERROR;
    }

    fs >> tmp_cluster_id;
    fs.close();

    if (tmp_cluster_id == -1 && (fs.rdstate() & fstream::eofbit) != 0) {
        *cluster_id = -1;
        return res;
    } else if (tmp_cluster_id >= 0 && (fs.rdstate() & fstream::eofbit) != 0) {
        *cluster_id = tmp_cluster_id;
        return res;
    } else {
        OLAP_LOG_WARNING("fail to read cluster id from file. "
                         "[id=%d eofbit=%d failbit=%d badbit=%d]",
                         tmp_cluster_id,
                         fs.rdstate() & fstream::eofbit,
                         fs.rdstate() & fstream::failbit,
                         fs.rdstate() & fstream::badbit);
        return OLAP_ERR_IO_ERROR;
    }
}

OLAPStatus OLAPRootPath::_write_cluster_id_to_path(const string& path, int32_t cluster_id) {
    OLAPStatus res = OLAP_SUCCESS;

    fstream fs(path.c_str(), fstream::out);
    if (!fs.is_open()) {
        OLAP_LOG_WARNING("fail to open cluster id path. [path='%s']", path.c_str());
        return OLAP_ERR_IO_ERROR;
    }

    fs << cluster_id;
    fs.close();

    return res;
}

OLAPStatus OLAPRootPath::_judge_and_update_effective_cluster_id(int32_t cluster_id) {
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

OLAPStatus OLAPRootPath::_check_recover_root_path_cluster_id(const std::string& root_path) {
    OLAPStatus res = OLAP_SUCCESS;

    // prepare: check cluster id file exist, if not, create it
    string path = root_path + CLUSTER_ID_PREFIX;
    if (access(path.c_str(), F_OK) != 0) {
        int fd = open(path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (fd < 0 || close(fd) < 0) {
            OLAP_LOG_WARNING("fail to create file. [path='%s' err='%m']", path.c_str());
            return OLAP_ERR_OTHER_ERROR;
        }
    }

    // obtain lock of cluster id path
    FILE* fp = NULL;
    fp = fopen(path.c_str(), "r+b");
    if (fp == NULL) {
        OLAP_LOG_WARNING("fail to open cluster id path. [path='%s']", path.c_str());
        return OLAP_ERR_IO_ERROR;
    }

    int lock_res = flock(fp->_fileno, LOCK_EX | LOCK_NB);
    if (lock_res < 0) {
        OLAP_LOG_WARNING("fail to lock file descriptor. [path='%s']", path.c_str());
        fclose(fp);
        fp = NULL;
        return OLAP_ERR_TRY_LOCK_FAILED;
    }

    // obtain cluster id of root path
    int32_t cluster_id = -1;
    res = _get_cluster_id_from_path(path, &cluster_id);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get cluster id from path. [res=%d]", res);
        fclose(fp);
        fp = NULL;
        return res;
    } else if (cluster_id == -1 || _effective_cluster_id == -1) {
        _is_all_cluster_id_exist = false;
    }
    
    // judge and update effective cluster id
    res = _judge_and_update_effective_cluster_id(cluster_id);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to judge and update effective cluster id. [res=%d]", res);
        fclose(fp);
        fp = NULL;
        return res;
    }

    // write cluster id into cluster_id_path if get effective cluster id success
    if (_effective_cluster_id != -1 && !_is_all_cluster_id_exist) {
        res = set_cluster_id(_effective_cluster_id);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to write cluster id to path. [res=%d]", res);
            fclose(fp);
            fp = NULL;
            return res;
        }
        _is_all_cluster_id_exist = true;
    }

    fclose(fp);
    fp = NULL;
    return res;
}

bool OLAPRootPath::_check_root_path_exist(const string& root_path) {
    bool is_exist = true;
    DIR* dirp = opendir(root_path.c_str());
    do {
        if (NULL == dirp) {
            is_exist = false;
            OLAP_LOG_WARNING("can't open root path. [root_path=%s]", root_path.c_str());
            break;
        }

        if (readdir(dirp) == NULL) {
            is_exist = false;
            OLAP_LOG_WARNING("can't read root path. [root_path=%s]", root_path.c_str());
            break;
        }
    } while (0);

    closedir(dirp);
    return is_exist;
}

OLAPStatus OLAPRootPath::check_all_root_path_cluster_id(
        const vector<string>& root_path_vec,
        const vector<bool>& is_accessable_vec) {
    OLAPStatus res = OLAP_SUCCESS;

    // prepare: check cluster id file exist, if not, create it
    vector<string> cluster_id_path_vec;
    for (size_t i = 0; i < root_path_vec.size(); ++i) {
        if (!is_accessable_vec[i]) {
            continue;
        }
        cluster_id_path_vec.push_back(root_path_vec[i] + CLUSTER_ID_PREFIX);
    }

    for (const auto& path : cluster_id_path_vec) {
        if (access(path.c_str(), F_OK) != 0) {
            int fd = open(path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
            if (fd < 0 || close(fd) < 0) {
                OLAP_LOG_WARNING("fail to create file. [path='%s' err='%m']", path.c_str());
                return OLAP_ERR_OTHER_ERROR;
            }
        }
    }

    // obtain lock of all cluster id paths
    for (size_t i = 0; i < root_path_vec.size(); ++i) {
        RootPathMap::iterator it = _root_paths.find(root_path_vec[i]);
        if (is_accessable_vec[i] && it == _root_paths.end()) {
            FILE* fp = NULL;
            string path = root_path_vec[i] + CLUSTER_ID_PREFIX;
            fp = fopen(path.c_str(), "r+b");
            if (fp == NULL) {
                OLAP_LOG_WARNING("fail to open cluster id path. [path='%s']", path.c_str());
                return OLAP_ERR_IO_ERROR;
            }

            int lock_res = flock(fp->_fileno, LOCK_EX | LOCK_NB);
            if (lock_res < 0) {
                OLAP_LOG_WARNING("fail to lock file descriptor. [path='%s']", path.c_str());
                fclose(fp);
                fp = NULL;
                return OLAP_ERR_TRY_LOCK_FAILED;
            }
        }
    }

    // obtain cluster id of all root paths
    int32_t cluster_id = -1;
    for (const auto& path : cluster_id_path_vec) {
        int32_t tmp_cluster_id = -1;
        res = _get_cluster_id_from_path(path, &tmp_cluster_id);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to get cluster id from path. [res=%d]", res);
            return res;
        } else if (tmp_cluster_id == -1) {
            _is_all_cluster_id_exist = false;
        } else if (tmp_cluster_id ==  cluster_id) {
            // both hava right cluster id, do nothing
        } else if (cluster_id == -1) {
            cluster_id = tmp_cluster_id;
        } else {
            OLAP_LOG_WARNING("multiple cluster ids is not equal. [id1=%d id2=%d]",
                             cluster_id, tmp_cluster_id);
            return OLAP_ERR_INVALID_CLUSTER_INFO;
        }
    }

    // judge and get effective cluster id
    res = _judge_and_update_effective_cluster_id(cluster_id);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to judge and update effective cluster id. [res=%d]", res);
        return res;
    }

    // write cluster id into cluster_id_path if get effective cluster id success
    if (_effective_cluster_id != -1 && !_is_all_cluster_id_exist) {
        for (const string& path : cluster_id_path_vec) {
            res = _write_cluster_id_to_path(path, _effective_cluster_id);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to write cluster id to path. [res=%d]", res);
                return res;
            }
        }
        _is_all_cluster_id_exist = true;
    }

    return res;
}

OLAPStatus OLAPRootPath::set_cluster_id(int32_t cluster_id) {
    OLAPStatus res = OLAP_SUCCESS;

    vector<string> cluster_id_path_vec;
    res = _get_cluster_id_path_vec(&cluster_id_path_vec);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get all cluster id path. [res=%d]", res);
        return res;
    }

    for (const string& path : cluster_id_path_vec) {
        res = _write_cluster_id_to_path(path, cluster_id);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to write cluster id to path. [res=%d]", res);
            return res;
        }
    }

    _effective_cluster_id = cluster_id;
    _is_all_cluster_id_exist = true;
    return res;
}

}  // namespace palo
