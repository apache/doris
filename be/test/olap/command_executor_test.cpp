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

#include <stdlib.h>
#include <sys/file.h>
#include <unistd.h>

#include <map>
#include <set>
#include <sstream>
#include <string>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include "olap/command_executor.h"
#include "olap/field.h"
#include "olap/olap_engine.h"
#include "olap/olap_main.cpp"
#include "olap/olap_table.h"
#include "olap/utils.h"
#include "util/logging.h"

using std::nothrow;
using std::stringstream;

namespace palo {

// SQL for generate BASE_TABLE_PUSH_DATA & BASE_TABLE_PUSH_DATA_BIG & ROLLUP_TABLE_PUSH_DATA:
//
// create table delete_test_row (k1 tinyint, k2 bigint, k3 char(64), 
// k6 DECIMAL, v bigint sum) engine=olap distributed by 
// random buckets 1 properties ("storage_type" = "row", "short_key" = "2");
//
// alter table delete_test_row add rollup delete_test_row_rollup(k1, k3, v);
//
// load label label1 (data infile 
// ("hdfs://host:port/dir") 
// into table `delete_test_row` (k1,k2,v,k3,k4,k5,k6));
//
// load label label2 (data infile 
// ("hdfs://host:port/dir") 
// into table `delete_test_row` (k1,k2,v,k3,k4,k5,k6));
static const int64_t BASE_TABLE_PUSH_DATA_ROW_COUNT = 100;
static const char* BASE_TABLE_PUSH_DATA = "./be/test/olap/test_data/all_types_100";
static const int64_t BASE_TABLE_PUSH_DATA_BIG_ROW_COUNT = 100000;
static const char* BASE_TABLE_PUSH_DATA_BIG = "./be/test/olap/test_data/all_types_100000";
static const int64_t ROLLUP_TABLE_PUSH_DATA_ROW_COUNT = 100;
static const char* ROLLUP_TABLE_PUSH_DATA = "./be/test/olap/test_data/all_types_100_rollup";

// checksum for base table push data
static const uint32_t MAX_RETRY_TIMES = 10;
static const uint32_t BASE_TABLE_PUSH_DATA_CHECKSUM = 1401759800;

static const uint32_t MAX_PATH_LEN = 1024;

void set_up() {
    char buffer[MAX_PATH_LEN];
    getcwd(buffer, MAX_PATH_LEN);
    config::storage_root_path = string(buffer) + "/test_run/data_test";
    system("rm -rf ./test_run && mkdir -p ./test_run");
    create_dir(config::storage_root_path);
    touch_all_singleton();
}

void tear_down() {
    system("rm -rf ./test_run");
    remove_all_dir(string(getenv("DORIS_HOME")) + UNUSED_PREFIX);
}

void set_default_create_tablet_request(TCreateTabletReq* request) {
    request->tablet_id = 10003;
    request->__set_version(1);
    request->__set_version_hash(0);
    request->tablet_schema.schema_hash = 270068375;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::AGG_KEYS;
    request->tablet_schema.storage_type = TStorageType::ROW;

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.type = TPrimitiveType::BIGINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    k3.column_type.type = TPrimitiveType::CHAR;
    k3.column_type.__set_len(64);
    request->tablet_schema.columns.push_back(k3);

    TColumn k6;
    k6.column_name = "k6";
    k6.__set_is_key(true);
    k6.column_type.type = TPrimitiveType::DECIMAL;
    k6.column_type.__set_precision(6);
    k6.column_type.__set_scale(3);
    request->tablet_schema.columns.push_back(k6);

    TColumn v;
    v.column_name = "v";
    v.__set_is_key(false);
    v.column_type.type = TPrimitiveType::BIGINT;
    v.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v);
}

void set_bloom_filter_create_tablet_request(TCreateTabletReq* request) {
    request->tablet_id = 10004;
    request->__set_version(1);
    request->__set_version_hash(0);
    request->tablet_schema.schema_hash = 270076533;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::AGG_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.type = TPrimitiveType::BIGINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    k3.column_type.type = TPrimitiveType::CHAR;
    k3.column_type.__set_len(64);
    request->tablet_schema.columns.push_back(k3);

    TColumn k6;
    k6.column_name = "k6";
    k6.__set_is_key(true);
    k6.column_type.type = TPrimitiveType::DECIMAL;
    k6.column_type.__set_precision(6);
    k6.column_type.__set_scale(3);
    request->tablet_schema.columns.push_back(k6);

    TColumn v;
    v.column_name = "v";
    v.__set_is_key(false);
    v.column_type.type = TPrimitiveType::BIGINT;
    v.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v);
}

void set_default_push_request(const TCreateTabletReq& request, TPushReq* push_request) {
    push_request->tablet_id = request.tablet_id;
    push_request->schema_hash = request.tablet_schema.schema_hash;
    push_request->__set_version(request.version + 1);
    push_request->__set_version_hash(request.version_hash + 1);
    push_request->timeout = 86400;
    push_request->push_type = TPushType::LOAD;
    push_request->__set_http_file_path(BASE_TABLE_PUSH_DATA);
}

void set_alter_tablet_request(const TCreateTabletReq& base_tablet, TAlterTabletReq* request) {
    request->base_tablet_id = base_tablet.tablet_id;
    request->base_schema_hash = base_tablet.tablet_schema.schema_hash;
}

class TestCreateTable : public ::testing::Test {
public:
    TestCreateTable() : _command_executor(NULL) {}
    ~TestCreateTable() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_create_table";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestCreateTable, create_tablet) {
    OLAPStatus res = OLAP_SUCCESS;

    // 1. Create table with error param.
    TCreateTabletReq request;
    set_default_create_tablet_request(&request);
    request.tablet_schema.short_key_column_count = 5;

    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_ERR_CE_CMD_PARAMS_ERROR, res);

    // 2. Create table normally.
    request.tablet_schema.short_key_column_count = 2;
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // check create table result
    SmartOLAPTable tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);
    ASSERT_EQ(0, access(tablet->header_file_name().c_str(), F_OK));

    Version base_version(0, request.version);
    string index_name = tablet->construct_index_file_path(base_version, request.version_hash, 0);
    string data_name = tablet->construct_data_file_path(base_version, request.version_hash, 0);
    ASSERT_EQ(0, access(index_name.c_str(), F_OK));
    ASSERT_EQ(0, access(data_name.c_str(), F_OK));

    Version delta_version(request.version + 1, request.version + 1);
    index_name = tablet->construct_index_file_path(delta_version, 0, 0);
    data_name = tablet->construct_data_file_path(delta_version, 0, 0);
    ASSERT_EQ(0, access(index_name.c_str(), F_OK));
    ASSERT_EQ(0, access(data_name.c_str(), F_OK));

    // 3. Create table already existed.
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // 4. Create table with different schema_hash.
    request.tablet_schema.schema_hash = 0;
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_ERR_CE_TABLET_ID_EXIST, res);
}

TEST_F(TestCreateTable, column_create_tablet) {
    OLAPStatus res = OLAP_SUCCESS;
    TCreateTabletReq request;
    set_default_create_tablet_request(&request);
    request.tablet_id += 1;
    request.tablet_schema.schema_hash += 1;
    request.tablet_schema.storage_type = TStorageType::COLUMN;

    res = _command_executor->create_table(request);

    // check create table result
    ASSERT_EQ(OLAP_SUCCESS, res);
    SmartOLAPTable tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);
    ASSERT_EQ(0, access(tablet->header_file_name().c_str(), F_OK));

    Version base_version(0, request.version);
    string index_name = tablet->construct_index_file_path(base_version, request.version_hash, 0);
    string data_name = tablet->construct_data_file_path(base_version, request.version_hash, 0);
    ASSERT_EQ(0, access(index_name.c_str(), F_OK));
    ASSERT_EQ(0, access(data_name.c_str(), F_OK));

    Version delta_version(request.version + 1, request.version + 1);
    index_name = tablet->construct_index_file_path(delta_version, 0, 0);
    data_name = tablet->construct_data_file_path(delta_version, 0, 0);
    ASSERT_EQ(0, access(index_name.c_str(), F_OK));
    ASSERT_EQ(0, access(data_name.c_str(), F_OK));
}

class TestGetTable : public ::testing::Test {
public:
    TestGetTable() : _command_executor(NULL) {}
    ~TestGetTable() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_get_table";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestGetTable, get_table) {
    SmartOLAPTable tablet;
    OLAPStatus res = OLAP_SUCCESS;
    TCreateTabletReq request;
    set_default_create_tablet_request(&request);

    // 1. Get table not existed.
    tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() == NULL);

    // 2. Get table normally.
    // create table first
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // get table
    tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);
}

class TestDropTable : public ::testing::Test {
public:
    TestDropTable() : _command_executor(NULL) {}
    ~TestDropTable() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_drop_table";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestDropTable, drop_table) {
    SmartOLAPTable tablet;
    OLAPStatus res = OLAP_SUCCESS;

    TCreateTabletReq request;
    set_default_create_tablet_request(&request);

    TDropTabletReq drop_request;
    drop_request.tablet_id =  request.tablet_id;
    drop_request.schema_hash = request.tablet_schema.schema_hash;

    // 1. Drop table not existed.
    res = _command_executor->drop_table(drop_request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    double usage = -1;
    ASSERT_EQ(OLAPEngine::get_instance()->start_trash_sweep(&usage), OLAP_SUCCESS);

    // 2. Drop table normally.
    // create table first
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    res = _command_executor->drop_table(drop_request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // 3. Clear trash
    // check dropped table exist after trash scan
    std::set<std::string> dirs;
    ASSERT_EQ(OLAPEngine::get_instance()->start_trash_sweep(&usage), OLAP_SUCCESS);
    dir_walk(config::storage_root_path + TRASH_PREFIX, &dirs, nullptr);
    OLAP_LOG_INFO("max disk usage is: %f", usage);
    const double guard_usage = config::disk_capacity_insufficient_percentage / 100.0;
    ASSERT_TRUE(usage > guard_usage ? dirs.empty() : !dirs.empty());
    // check dirs really removed after timeout
    config::trash_file_expire_time_sec = 1;
    sleep(2); // wait for timeout
    ASSERT_EQ(OLAPEngine::get_instance()->start_trash_sweep(&usage), OLAP_SUCCESS);
    ASSERT_TRUE(0 <= usage && usage <= 100);
    dirs.clear();
    dir_walk(config::storage_root_path + TRASH_PREFIX, &dirs, nullptr);
    ASSERT_TRUE(dirs.empty());
}

class TestReportTablet : public ::testing::Test {
public:
    TestReportTablet() : _command_executor(NULL) {}
    ~TestReportTablet() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_report";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestReportTablet, report_tablet_info) {
    OLAPStatus res = OLAP_SUCCESS;
    TCreateTabletReq request;
    set_default_create_tablet_request(&request);

    TTabletInfo tablet_info;
    tablet_info.tablet_id = request.tablet_id;
    tablet_info.schema_hash = request.tablet_schema.schema_hash;

    // 1. Report tablet info not existed.
    res = _command_executor->report_tablet_info(&tablet_info);
    ASSERT_EQ(OLAP_ERR_TABLE_NOT_FOUND, res);

    // 2. Report tablet info normally.
    // create tablet first
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // report tablet info and check.
    res = _command_executor->report_tablet_info(&tablet_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(request.version + 1, tablet_info.version);
    ASSERT_EQ(request.version_hash, tablet_info.version_hash);
    ASSERT_EQ(0, tablet_info.row_count);
}

class TestReportAllTablets : public ::testing::Test {
public:
    TestReportAllTablets() : _command_executor(NULL) {}
    ~TestReportAllTablets() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_report_all";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestReportAllTablets, report_all_tablets_info) {
    OLAPStatus res = OLAP_SUCCESS;
    std::map<TTabletId, TTablet> tablets_info;

    // 1. Report empty tablet.
    tablets_info.clear();
    res = _command_executor->report_all_tablets_info(&tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(0, tablets_info.size());

    // 2. Report one tablets.
    // create default tablet.
    TCreateTabletReq request;
    set_default_create_tablet_request(&request);
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    tablets_info.clear();
    res = _command_executor->report_all_tablets_info(&tablets_info);

    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());
    ASSERT_EQ(1, tablets_info[request.tablet_id].tablet_infos.size());
    ASSERT_EQ(request.tablet_id, tablets_info[request.tablet_id].tablet_infos[0].tablet_id);

    // 3. Report two tablets.
    // create another tablet.
    request.tablet_id = request.tablet_id + 1;
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
   
    tablets_info.clear();
    res = _command_executor->report_all_tablets_info(&tablets_info);

    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(2, tablets_info.size());
    ASSERT_EQ(1, tablets_info[request.tablet_id].tablet_infos.size());
    ASSERT_EQ(request.tablet_id, tablets_info[request.tablet_id].tablet_infos[0].tablet_id);
}

class TestReloadRootPath : public ::testing::Test {
public:
    TestReloadRootPath() : _command_executor(NULL) {}
    ~TestReloadRootPath() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_reload";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestReloadRootPath, reload_root_path) {
    string root_path;
    OLAPStatus res = OLAP_SUCCESS;

    // create table in current root path
    TCreateTabletReq request;
    set_default_create_tablet_request(&request);
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    SmartOLAPTable tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);

    // 1. Reload empty root path
    root_path = ";";
    res = _command_executor->reload_root_path(root_path);
    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, res);

    // 2. Reload new root path not existed
    char buffer[MAX_PATH_LEN];
    getcwd(buffer, MAX_PATH_LEN);
    root_path = string(buffer) + "/test_run/data_reload_new";
    res = _command_executor->reload_root_path(root_path);
    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, res);
    tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);

    // 3. Reload new root path normally
    remove_all_dir(root_path);
    ASSERT_EQ(create_dir(root_path), OLAP_SUCCESS);
    res = _command_executor->reload_root_path(root_path);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() == NULL);

    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);

    // 3. Reload same root path
    res = _command_executor->reload_root_path(root_path);
    ASSERT_EQ(OLAP_SUCCESS, res);
    tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);

    ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(root_path));   
}

class TestGetRootPathInfo : public ::testing::Test {
public:
    TestGetRootPathInfo() : _command_executor(NULL) {}
    ~TestGetRootPathInfo() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_root_path_stat";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestGetRootPathInfo, get_all_root_path_info) {
    OLAPStatus res = OLAP_SUCCESS;
    std::vector<RootPathInfo> root_paths_info;

    res = _command_executor->get_all_root_path_info(&root_paths_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, root_paths_info.size());
    EXPECT_STREQ(config::storage_root_path.c_str(), root_paths_info[0].path.c_str());
}

class TestPush : public ::testing::Test {
public:
    TestPush() : _command_executor(NULL) {}
    ~TestPush() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_push";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestPush, push) {
    OLAPStatus res = OLAP_SUCCESS;
    TCreateTabletReq request;
    set_default_create_tablet_request(&request);
    TPushReq push_req;
    set_default_push_request(request, &push_req);
    std::vector<TTabletInfo> tablets_info;
    
    // 1. Push before tablet created.
    tablets_info.clear();
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_ERR_TABLE_NOT_FOUND, res);
    ASSERT_EQ(0, tablets_info.size());

    // create tablet first
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    SmartOLAPTable tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);

    // 2. Push with wrong version.
    push_req.version = 0;
    tablets_info.clear();
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_ERR_PUSH_VERSION_INCORRECT, res);
    ASSERT_EQ(0, tablets_info.size());

    // 3. Push next version normally.
    push_req.version = request.version + 1;
    tablets_info.clear();
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());
    ASSERT_EQ(push_req.tablet_id, tablets_info[0].tablet_id);
    ASSERT_EQ(push_req.schema_hash, tablets_info[0].schema_hash);
    ASSERT_EQ(push_req.version, tablets_info[0].version);
    ASSERT_EQ(push_req.version_hash, tablets_info[0].version_hash);
    ASSERT_EQ(BASE_TABLE_PUSH_DATA_ROW_COUNT, tablets_info[0].row_count);

    // 4. Push the same batch.
    tablets_info.clear();
    int64_t row_count = BASE_TABLE_PUSH_DATA_ROW_COUNT;
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());
    ASSERT_EQ(push_req.tablet_id, tablets_info[0].tablet_id);
    ASSERT_EQ(push_req.schema_hash, tablets_info[0].schema_hash);
    ASSERT_EQ(push_req.version, tablets_info[0].version);
    ASSERT_EQ(push_req.version_hash, tablets_info[0].version_hash);
    ASSERT_EQ(row_count, tablets_info[0].row_count);

    // 5. Push the next batch.
    push_req.version += 1;
    push_req.version_hash += 1;
    tablets_info.clear();
    row_count += BASE_TABLE_PUSH_DATA_ROW_COUNT;
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());
    ASSERT_EQ(push_req.tablet_id, tablets_info[0].tablet_id);
    ASSERT_EQ(push_req.schema_hash, tablets_info[0].schema_hash);
    ASSERT_EQ(push_req.version, tablets_info[0].version);
    ASSERT_EQ(push_req.version_hash, tablets_info[0].version_hash);
    ASSERT_EQ(row_count, tablets_info[0].row_count);
}

TEST_F(TestPush, column_push) {
    OLAPStatus res = OLAP_SUCCESS;
    TCreateTabletReq request;
    set_default_create_tablet_request(&request);
    std::vector<TTabletInfo> tablets_info;
    
    // create tablet first
    request.tablet_id += 1;
    request.tablet_schema.schema_hash += 1;
    request.tablet_schema.storage_type = TStorageType::COLUMN;
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    SmartOLAPTable tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);

    tablets_info.clear();
    TPushReq push_req;
    set_default_push_request(request, &push_req);
    push_req.tablet_id = request.tablet_id;
    push_req.schema_hash = request.tablet_schema.schema_hash;
    push_req.__set_http_file_path(BASE_TABLE_PUSH_DATA_BIG);
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());
    ASSERT_EQ(push_req.tablet_id, tablets_info[0].tablet_id);
    ASSERT_EQ(push_req.schema_hash, tablets_info[0].schema_hash);
    ASSERT_EQ(push_req.version, tablets_info[0].version);
    ASSERT_EQ(push_req.version_hash, tablets_info[0].version_hash);
    ASSERT_EQ(BASE_TABLE_PUSH_DATA_BIG_ROW_COUNT, tablets_info[0].row_count);
}

class TestComputeChecksum : public ::testing::Test {
public:
    TestComputeChecksum() : _command_executor(NULL) {}
    ~TestComputeChecksum() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_compute_checksum";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestComputeChecksum, compute_checksum) {
    uint32_t checksum = 0;
    OLAPStatus res = OLAP_SUCCESS;

    TCreateTabletReq request;
    set_default_create_tablet_request(&request);

    TPushReq push_req;
    set_default_push_request(request, &push_req);
    std::vector<TTabletInfo> tablets_info;

    // 1. Compute checksum before tablet created.
    res = _command_executor->compute_checksum(
            request.tablet_id, request.tablet_schema.schema_hash,
            request.version, request.version_hash, &checksum);
    ASSERT_EQ(OLAP_ERR_TABLE_NOT_FOUND, res);

    // 2. Compute checksum for empty tablet.
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    SmartOLAPTable tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);

    res = _command_executor->compute_checksum(
            request.tablet_id, request.tablet_schema.schema_hash,
            request.version, request.version_hash, &checksum);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(0, checksum);

    // 3. Compute checksum normally.
    tablets_info.clear();
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());

    res = _command_executor->compute_checksum(
            push_req.tablet_id, push_req.schema_hash,
            push_req.version, push_req.version_hash, &checksum);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(BASE_TABLE_PUSH_DATA_CHECKSUM, checksum);
}

class TestBaseCompaction : public ::testing::Test {
public:
    TestBaseCompaction() : _command_executor(NULL) {}
    ~TestBaseCompaction() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_base_compaction";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestBaseCompaction, TestBaseCompaction) {
    OLAPStatus res = OLAP_SUCCESS;
    TCreateTabletReq request;
    set_default_create_tablet_request(&request);

    TPushReq push_req;
    set_default_push_request(request, &push_req);
    std::vector<TTabletInfo> tablets_info;

    // 1. Start BE before tablet created.
    res = _command_executor->base_compaction(
            push_req.tablet_id, push_req.schema_hash, push_req.version);
    ASSERT_EQ(OLAP_ERR_TABLE_NOT_FOUND, res);

    // 2. Start BE for error new base version.
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    SmartOLAPTable tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);

    res = _command_executor->base_compaction(
            request.tablet_id, request.tablet_schema.schema_hash, request.version + 1);
    ASSERT_EQ(OLAP_ERR_BE_NO_SUITABLE_VERSION, res);
}

// ######################### ALTER TABLE TEST BEGIN #########################

void set_create_tablet_request_1(const TCreateTabletReq& base_request, TCreateTabletReq* request) {
    //sorting schema change
    request->tablet_id = base_request.tablet_id + 1;
    request->__set_version(base_request.version);
    request->__set_version_hash(base_request.version_hash);
    request->tablet_schema.schema_hash = base_request.tablet_schema.schema_hash + 1;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.storage_type = TStorageType::ROW;

    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[0]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[2]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[4]);
}

void set_create_tablet_request_2(const TCreateTabletReq& base_request, TCreateTabletReq* request) {
    //directly schema change
    request->tablet_id = base_request.tablet_id + 2;
    request->__set_version(base_request.version);
    request->__set_version_hash(base_request.version_hash);
    request->tablet_schema.schema_hash = base_request.tablet_schema.schema_hash + 2;
    request->tablet_schema.short_key_column_count = 1;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[0]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[4]);
}

void set_create_tablet_request_3(const TCreateTabletReq& base_request, TCreateTabletReq* request) {
    //linked schema change, add a value column
    request->tablet_id = base_request.tablet_id + 3;
    request->__set_version(base_request.version);
    request->__set_version_hash(base_request.version_hash);
    request->tablet_schema.schema_hash = base_request.tablet_schema.schema_hash + 3;
    request->tablet_schema.short_key_column_count = 1;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[0]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[4]);

    TColumn v2;
    v2.column_name = "v2";
    v2.column_type.type = TPrimitiveType::BIGINT;
    v2.__set_is_key(false);
    v2.__set_default_value("0");
    v2.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v2);
}

void set_create_tablet_request_4(const TCreateTabletReq& base_request, TCreateTabletReq* request) {
    //directly schema change, modify a key column type
    request->tablet_id = base_request.tablet_id + 4;
    request->__set_version(base_request.version);
    request->__set_version_hash(base_request.version_hash);
    request->tablet_schema.schema_hash = base_request.tablet_schema.schema_hash + 1;
    request->tablet_schema.short_key_column_count = 1;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::LARGEINT;
    request->tablet_schema.columns.push_back(k1);

    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[1]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[2]);
}

void set_create_tablet_request_bloom_filter(
        const TCreateTabletReq& base_request, TCreateTabletReq* request) {
    //sorting schema change
    request->tablet_id = base_request.tablet_id + 5;
    request->__set_version(base_request.version);
    request->__set_version_hash(base_request.version_hash);
    request->tablet_schema.schema_hash = base_request.tablet_schema.schema_hash + 1;
    request->tablet_schema.short_key_column_count = 4;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    k3.column_type.type = TPrimitiveType::CHAR;
    k3.column_type.__set_len(64);
    k3.is_bloom_filter_column = true;
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[0]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[1]);
    request->tablet_schema.columns.push_back(k3);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[3]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[4]);
}

void set_create_tablet_request_char_to_varchar(
        const TCreateTabletReq& base_request, TCreateTabletReq* request) {
    //sorting schema change
    request->tablet_id = base_request.tablet_id + 6;
    request->__set_version(base_request.version);
    request->__set_version_hash(base_request.version_hash);
    request->tablet_schema.schema_hash = base_request.tablet_schema.schema_hash + 1;
    request->tablet_schema.short_key_column_count = 4;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[0]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[1]);
    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    k3.column_type.type = TPrimitiveType::VARCHAR;
    k3.column_type.__set_len(128);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[2]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[3]);
    request->tablet_schema.columns.push_back(base_request.tablet_schema.columns[4]);
}

AlterTableStatus show_alter_table_status(
        CommandExecutor* command_executor,
        const TAlterTabletReq& request) {
    AlterTableStatus status = ALTER_TABLE_RUNNING;

    uint32_t max_retry = MAX_RETRY_TIMES;
    while (max_retry > 0) {
        status = command_executor->show_alter_table_status(
                request.base_tablet_id, request.base_schema_hash);
        if (status != ALTER_TABLE_RUNNING) {
            break;
        }

        OLAP_LOG_INFO("doing alter table......");
        --max_retry;
        sleep(1);
    }
    
    return status;
}

class TestSchemaChange : public ::testing::Test {
public:
    TestSchemaChange() : _command_executor(NULL) {}
    ~TestSchemaChange() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_schema_change";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestSchemaChange, tablet_schema_change_abnormal) {
    OLAPStatus res = OLAP_SUCCESS;

    // check not existed tablet id
    TCreateTabletReq create_base_tablet;
    set_default_create_tablet_request(&create_base_tablet);
    TCreateTabletReq create_new_tablet;
    set_create_tablet_request_1(create_base_tablet, &create_new_tablet);

    TAlterTabletReq request;
    set_alter_tablet_request(create_base_tablet, &request);
    request.__set_new_tablet_req(create_new_tablet);

    res = _command_executor->schema_change(request);
    ASSERT_EQ(OLAP_ERR_TRY_LOCK_FAILED, res);

    AlterTableStatus status = _command_executor->show_alter_table_status(
            request.base_tablet_id, request.base_schema_hash);
    ASSERT_EQ(ALTER_TABLE_FAILED, status);
}

TEST_F(TestSchemaChange, schema_change_bloom_filter) {
    OLAPStatus res = OLAP_SUCCESS;

    TCreateTabletReq create_base_tablet;
    set_bloom_filter_create_tablet_request(&create_base_tablet);
    res = _command_executor->create_table(create_base_tablet);
    ASSERT_EQ(OLAP_SUCCESS, res);

    TCreateTabletReq create_new_tablet;
    set_create_tablet_request_bloom_filter(create_base_tablet, &create_new_tablet);

    TAlterTabletReq request;
    set_alter_tablet_request(create_base_tablet, &request);
    request.__set_new_tablet_req(create_new_tablet);

    res = _command_executor->schema_change(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    AlterTableStatus status = ALTER_TABLE_WAITING;
    status = show_alter_table_status(_command_executor, request);
    ASSERT_EQ(ALTER_TABLE_DONE, status);

    res = OLAPEngine::get_instance()->drop_table(
            request.base_tablet_id, request.base_schema_hash); 
    ASSERT_EQ(OLAP_SUCCESS, res);
}

TEST_F(TestSchemaChange, schema_change_char_to_varchar) {
    OLAPStatus res = OLAP_SUCCESS;
    AlterTableStatus status = ALTER_TABLE_WAITING;

    // 1. Prepare for schema change.
    // create base tablet
    TCreateTabletReq create_base_tablet;
    set_default_create_tablet_request(&create_base_tablet);
    res = _command_executor->create_table(create_base_tablet);
    ASSERT_EQ(OLAP_SUCCESS, res);
    SmartOLAPTable tablet = _command_executor->get_table(
            create_base_tablet.tablet_id, create_base_tablet.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);

    // push data
    TPushReq push_req;
    set_default_push_request(create_base_tablet, &push_req);
    std::vector<TTabletInfo> tablets_info;
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // set schema change request
    TCreateTabletReq create_new_tablet;
    set_create_tablet_request_char_to_varchar(create_base_tablet, &create_new_tablet);
    TAlterTabletReq request;
    set_alter_tablet_request(create_base_tablet, &request);
    request.__set_new_tablet_req(create_new_tablet);

    // 2. Submit schema change.
    request.base_schema_hash = create_base_tablet.tablet_schema.schema_hash;
    res = _command_executor->schema_change(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // 3. Verify schema change result.
    // show schema change status
    status = show_alter_table_status(_command_executor, request);
    ASSERT_EQ(ALTER_TABLE_DONE, status);

    // check new tablet information
    TTabletInfo tablet_info;
    tablet_info.tablet_id = create_new_tablet.tablet_id;
    tablet_info.schema_hash = create_new_tablet.tablet_schema.schema_hash;
    res = _command_executor->report_tablet_info(&tablet_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(push_req.version, tablet_info.version);
    ASSERT_EQ(push_req.version_hash, tablet_info.version_hash);
    ASSERT_EQ(BASE_TABLE_PUSH_DATA_ROW_COUNT, tablet_info.row_count);

    // 4. Retry the same schema change request.
    res = _command_executor->schema_change(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    status = _command_executor->show_alter_table_status(
            request.base_tablet_id, request.base_schema_hash);
    ASSERT_EQ(ALTER_TABLE_DONE, status);

    // 5. Do schema change continuously.
    res = OLAPEngine::get_instance()->drop_table(
            request.base_tablet_id, request.base_schema_hash); 
    ASSERT_EQ(OLAP_SUCCESS, res);
}

TEST_F(TestSchemaChange, schema_change) {
    OLAPStatus res = OLAP_SUCCESS;
    AlterTableStatus status = ALTER_TABLE_WAITING;

    // 1. Prepare for schema change.
    // create base tablet
    TCreateTabletReq create_base_tablet;
    set_default_create_tablet_request(&create_base_tablet);
    res = _command_executor->create_table(create_base_tablet);
    ASSERT_EQ(OLAP_SUCCESS, res);
    SmartOLAPTable tablet = _command_executor->get_table(
            create_base_tablet.tablet_id, create_base_tablet.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);

    // push data
    TPushReq push_req;
    set_default_push_request(create_base_tablet, &push_req);
    std::vector<TTabletInfo> tablets_info;
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());

    // set schema change request
    TCreateTabletReq create_new_tablet;
    set_create_tablet_request_1(create_base_tablet, &create_new_tablet);
    TAlterTabletReq request;
    set_alter_tablet_request(create_base_tablet, &request);
    request.__set_new_tablet_req(create_new_tablet);

    // check schema change for non_existed tablet
    request.base_schema_hash = 0;
    res = _command_executor->schema_change(request);
    ASSERT_EQ(OLAP_ERR_TABLE_NOT_FOUND, res);

    // 2. Submit schema change.
    request.base_schema_hash = create_base_tablet.tablet_schema.schema_hash;
    res = _command_executor->schema_change(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // 3. Verify schema change result.
    // show schema change status
    status = show_alter_table_status(_command_executor, request);
    ASSERT_EQ(ALTER_TABLE_DONE, status);

    // check new tablet information
    TTabletInfo tablet_info;
    tablet_info.tablet_id = create_new_tablet.tablet_id;
    tablet_info.schema_hash = create_new_tablet.tablet_schema.schema_hash;
    res = _command_executor->report_tablet_info(&tablet_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(push_req.version, tablet_info.version);
    ASSERT_EQ(push_req.version_hash, tablet_info.version_hash);
    ASSERT_EQ(BASE_TABLE_PUSH_DATA_ROW_COUNT, tablet_info.row_count);

    // 4. Retry the same schema change request.
    res = _command_executor->schema_change(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    status = _command_executor->show_alter_table_status(
            request.base_tablet_id, request.base_schema_hash);
    ASSERT_EQ(ALTER_TABLE_DONE, status);
    
    // 5. Do schema change continuously.
    res = OLAPEngine::get_instance()->drop_table(
            request.base_tablet_id, request.base_schema_hash); 
    ASSERT_EQ(OLAP_SUCCESS, res);

    TCreateTabletReq create_new_new_tablet;
    set_create_tablet_request_2(create_base_tablet, &create_new_new_tablet);
    TAlterTabletReq new_request;
    set_alter_tablet_request(create_new_tablet, &new_request);
    new_request.__set_new_tablet_req(create_new_new_tablet);

    res = _command_executor->schema_change(new_request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // show alter table status
    status = show_alter_table_status(_command_executor, new_request);
    ASSERT_EQ(ALTER_TABLE_DONE, status);

    res = OLAPEngine::get_instance()->drop_table(
            new_request.base_tablet_id, new_request.base_schema_hash);

    // check new tablet information
    tablet_info.tablet_id = create_new_new_tablet.tablet_id;
    tablet_info.schema_hash = create_new_new_tablet.tablet_schema.schema_hash;
    res = _command_executor->report_tablet_info(&tablet_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(push_req.version, tablet_info.version);
    ASSERT_EQ(push_req.version_hash, tablet_info.version_hash);
    ASSERT_EQ(100, tablet_info.row_count);

    //schema change, add a value column
    TCreateTabletReq create_new_tablet3;
    set_create_tablet_request_3(create_base_tablet, &create_new_tablet3);
    TAlterTabletReq request3;
    set_alter_tablet_request(create_new_new_tablet, &request3);
    request3.__set_new_tablet_req(create_new_tablet3);

    res = _command_executor->schema_change(request3);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // show alter table status
    status = show_alter_table_status(_command_executor, request3);
    ASSERT_EQ(ALTER_TABLE_DONE, status);
    
    res = OLAPEngine::get_instance()->drop_table(
            request3.base_tablet_id, request3.base_schema_hash);
    
    // check new tablet information
    tablet_info.tablet_id = create_new_tablet3.tablet_id;
    tablet_info.schema_hash = create_new_tablet3.tablet_schema.schema_hash;
    res = _command_executor->report_tablet_info(&tablet_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(push_req.version, tablet_info.version);
    ASSERT_EQ(push_req.version_hash, tablet_info.version_hash);
    ASSERT_EQ(100, tablet_info.row_count);
   
    //schema change, modify a key column
    TCreateTabletReq create_new_tablet4;
    set_create_tablet_request_4(create_new_tablet3, &create_new_tablet4);
    TAlterTabletReq request4;
    set_alter_tablet_request(create_new_tablet3, &request4);
    request4.__set_new_tablet_req(create_new_tablet4);

    res = _command_executor->schema_change(request4);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // show alter table status
    status = show_alter_table_status(_command_executor, request4);
    ASSERT_EQ(ALTER_TABLE_DONE, status);
    
    res = OLAPEngine::get_instance()->drop_table(
            request4.base_tablet_id, request4.base_schema_hash);
    
    // check new tablet information
    tablet_info.tablet_id = create_new_tablet4.tablet_id;
    tablet_info.schema_hash = create_new_tablet4.tablet_schema.schema_hash;
    res = _command_executor->report_tablet_info(&tablet_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(push_req.version, tablet_info.version);
    ASSERT_EQ(push_req.version_hash, tablet_info.version_hash);
    ASSERT_EQ(100, tablet_info.row_count);
}

class TestCreateRollupTable : public ::testing::Test {
public:
    TestCreateRollupTable() : _command_executor(NULL) {}
    ~TestCreateRollupTable() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_create_rollup";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);
    }

    void TearDown(){
        // Remove all dir.
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }

    CommandExecutor* _command_executor;
};

TEST_F(TestCreateRollupTable, create_rollup_table) {
    OLAPStatus res = OLAP_SUCCESS;
    AlterTableStatus status = ALTER_TABLE_WAITING;

    // 1. Prepare for schema change.
    // create base tablet
    TCreateTabletReq create_base_tablet;
    set_default_create_tablet_request(&create_base_tablet);
    res = _command_executor->create_table(create_base_tablet);
    ASSERT_EQ(OLAP_SUCCESS, res);
    SmartOLAPTable tablet = _command_executor->get_table(
            create_base_tablet.tablet_id, create_base_tablet.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);

    // push data
    TPushReq push_req;
    set_default_push_request(create_base_tablet, &push_req);
    std::vector<TTabletInfo> tablets_info;
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());

    // set schema change request
    TCreateTabletReq create_new_tablet;
    set_create_tablet_request_1(create_base_tablet, &create_new_tablet);
    TAlterTabletReq request;
    set_alter_tablet_request(create_base_tablet, &request);
    request.__set_new_tablet_req(create_new_tablet);

    // 2. Submit schema change.
    res = _command_executor->create_rollup_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // 3. Verify schema change result.
    // show schema change status
    status = show_alter_table_status(_command_executor, request);
    ASSERT_EQ(ALTER_TABLE_DONE, status);

    // check new tablet information
    int64_t rollup_row_count = BASE_TABLE_PUSH_DATA_ROW_COUNT;
    TTabletInfo tablet_info;
    tablet_info.tablet_id = create_new_tablet.tablet_id;
    tablet_info.schema_hash = create_new_tablet.tablet_schema.schema_hash;
    res = _command_executor->report_tablet_info(&tablet_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(push_req.version, tablet_info.version);
    ASSERT_EQ(push_req.version_hash, tablet_info.version_hash);
    ASSERT_EQ(BASE_TABLE_PUSH_DATA_ROW_COUNT, tablet_info.row_count);
    
    // 4. Push base tablet.
    tablets_info.clear();
    push_req.version += 1;
    push_req.version_hash += 1;
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(2, tablets_info.size());
    ASSERT_TRUE(tablet->is_schema_changing() == true);
    rollup_row_count += BASE_TABLE_PUSH_DATA_ROW_COUNT;

    // 5. Push rollup tablet.
    tablets_info.clear();
    push_req.version += 1;
    push_req.version_hash += 1;
    push_req.tablet_id = create_new_tablet.tablet_id;
    push_req.schema_hash = create_new_tablet.tablet_schema.schema_hash;
    push_req.__set_http_file_path(ROLLUP_TABLE_PUSH_DATA);
    res = _command_executor->push(push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());
    rollup_row_count += ROLLUP_TABLE_PUSH_DATA_ROW_COUNT;
    ASSERT_EQ(rollup_row_count, tablets_info[0].row_count);
    ASSERT_TRUE(tablet->is_schema_changing() == false);
}

// ######################### ALTER TABLE TEST END #########################

// ######################### ALTER CLONE BEGIN #########################

class TestClone : public ::testing::Test {
public:
    TestClone() : _command_executor(NULL) {}
    ~TestClone() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_clone";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);

        // 1. Prepare for query split key.
        // create base tablet
        OLAPStatus res = OLAP_SUCCESS;
        set_default_create_tablet_request(&_create_tablet);
        res = _command_executor->create_table(_create_tablet);
        ASSERT_EQ(OLAP_SUCCESS, res);
        SmartOLAPTable tablet = _command_executor->get_table(
                _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash);
        ASSERT_TRUE(tablet.get() != NULL);
        _header_file_name = tablet->header_file_name();

        // push data
        set_default_push_request(_create_tablet, &_push_req);
        std::vector<TTabletInfo> tablets_info;
        res = _command_executor->push(_push_req, &tablets_info);
        ASSERT_EQ(OLAP_SUCCESS, res);
        ASSERT_EQ(1, tablets_info.size());
    }

    void TearDown(){
        // Remove all dir.
        OLAPEngine::get_instance()->drop_table(
                _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash);
        ASSERT_EQ(access(_header_file_name.c_str(), F_OK), -1);
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }
    
    CommandExecutor* _command_executor;
    std::string _header_file_name;
    TCreateTabletReq _create_tablet;
    TPushReq _push_req;
};

TEST_F(TestClone, make_snapshot_abnormal) {
    OLAPStatus res = OLAP_SUCCESS;
    std::string snapshot_path;
    
    // check tablet not existed
    res = _command_executor->make_snapshot(0, 0, &snapshot_path);
    ASSERT_EQ(OLAP_ERR_TABLE_NOT_FOUND, res);

    // check tablet without delta
    TCreateTabletReq request = _create_tablet;
    request.tablet_id = 0;
    request.__isset.version = false;
    res = _command_executor->create_table(request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    SmartOLAPTable tablet = _command_executor->get_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_TRUE(tablet.get() != NULL);
    std::string header_file_name = tablet->header_file_name();

    res = _command_executor->make_snapshot(
            request.tablet_id, request.tablet_schema.schema_hash, &snapshot_path);
    // ASSERT_EQ(OLAP_ERR_VERSION_NOT_EXIST, res);

    // clear
    tablet.reset();
    OLAPEngine::get_instance()->drop_table(
            request.tablet_id, request.tablet_schema.schema_hash);
    ASSERT_EQ(access(header_file_name.c_str(), F_OK), -1);
}

TEST_F(TestClone, make_snapshot) {
    OLAPStatus res = OLAP_SUCCESS;
    std::string snapshot_path;

    res = _command_executor->make_snapshot(
            _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash, &snapshot_path);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(0, access(snapshot_path.c_str(), F_OK));

    ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(snapshot_path));
}

TEST_F(TestClone, release_snapshot_abnormal) {
    std::string long_path = "/empty_storage_root/snapshot/path";
    std::string short_path = "/s";

    ASSERT_EQ(OLAP_ERR_CE_CMD_PARAMS_ERROR, _command_executor->release_snapshot(long_path));
    ASSERT_EQ(OLAP_ERR_CE_CMD_PARAMS_ERROR, _command_executor->release_snapshot(short_path));
}

TEST_F(TestClone, release_snapshot) {
    OLAPStatus res = OLAP_SUCCESS;
    std::string snapshot_path;

    res = _command_executor->make_snapshot(
            _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash, &snapshot_path);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(0, access(snapshot_path.c_str(), F_OK));

    res = _command_executor->release_snapshot(snapshot_path);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(access(snapshot_path.c_str(), F_OK), -1);
}

TEST_F(TestClone, obtain_root_path_abnormal) {
    OLAPStatus res = OLAP_SUCCESS;
    std::string root_path;

    OLAPRootPath::get_instance()->clear();
    res = _command_executor->obtain_shard_path(TStorageMedium::HDD, &root_path);
    ASSERT_EQ(OLAP_ERR_NO_AVAILABLE_ROOT_PATH, res);
    OLAPRootPath::get_instance()->init();
}

TEST_F(TestClone, obtain_root_path) {
    OLAPStatus res = OLAP_SUCCESS;
    std::string root_path;

    res = _command_executor->obtain_shard_path(TStorageMedium::HDD, &root_path);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(config::storage_root_path + DATA_PREFIX + "/0", root_path);
}

void set_clone_request(const TCreateTabletReq& create_tablet, TCloneReq* request) {
    request->tablet_id = create_tablet.tablet_id;
    request->schema_hash = create_tablet.tablet_schema.schema_hash;
}

TEST_F(TestClone, load_header_abnormal) {
    OLAPStatus res = OLAP_SUCCESS;
    std::string shard_path = config::storage_root_path + DATA_PREFIX + "/0";
    
    TCloneReq request;
    set_clone_request(_create_tablet, &request);
    request.tablet_id = 0;
    res = _command_executor->load_header(shard_path, request);
    ASSERT_EQ(OLAP_ERR_FILE_NOT_EXIST, res);

    request.tablet_id = _create_tablet.tablet_id;
    res = _command_executor->load_header(shard_path, request);
    ASSERT_EQ(OLAP_SUCCESS, res);
}

TEST_F(TestClone, load_header) {
    OLAPStatus res = OLAP_SUCCESS;
    std::string snapshot_path;
    std::string root_path;

    // 1. Make snapshot.
    res = _command_executor->make_snapshot(
            _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash, &snapshot_path);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(0, access(snapshot_path.c_str(), F_OK));

    // 2. Obtain root path.
    res = _command_executor->obtain_shard_path(TStorageMedium::HDD, &root_path);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(config::storage_root_path + DATA_PREFIX + "/0", root_path);

    // 3. Drop the old tablet and copy the snapshot to root_path.
    // to avoid delete tablet has same name: .delete.schema_hash.datetime
    OLAPEngine::get_instance()->drop_table(
            _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash);
    ASSERT_EQ(access(_header_file_name.c_str(), F_OK), -1);
    system(("rm -fr " + root_path + "/[^s]*").c_str());
    system(("cp -r " + snapshot_path + "/* " + root_path).c_str());

    // 4. Load header.
    TCloneReq request;
    set_clone_request(_create_tablet, &request);
    res = _command_executor->load_header(root_path, request);
    ASSERT_EQ(OLAP_SUCCESS, res);

    // 5. Release snapshot.
    res = _command_executor->release_snapshot(snapshot_path);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(access(snapshot_path.c_str(), F_OK), -1);
}

// ######################### ALTER CLONE END #########################

class TestDeleteData : public ::testing::Test {
public:
    TestDeleteData() : _command_executor(NULL) {}
    ~TestDeleteData() {
        SAFE_DELETE(_command_executor);
    }

    void SetUp() {
        // Create local data dir for OLAPEngine.
        char buffer[MAX_PATH_LEN];
        getcwd(buffer, MAX_PATH_LEN);
        config::storage_root_path = string(buffer) + "/test_run/data_delete_data";
        remove_all_dir(config::storage_root_path);
        ASSERT_EQ(create_dir(config::storage_root_path), OLAP_SUCCESS);

        // Initialize all singleton object.
        OLAPRootPath::get_instance()->reload_root_paths(config::storage_root_path.c_str());

        _command_executor = new(nothrow) CommandExecutor();
        ASSERT_TRUE(_command_executor != NULL);

        // 1. Prepare for query split key.
        // create base tablet
        OLAPStatus res = OLAP_SUCCESS;
        set_default_create_tablet_request(&_create_tablet);
        res = _command_executor->create_table(_create_tablet);
        ASSERT_EQ(OLAP_SUCCESS, res);
        SmartOLAPTable tablet = _command_executor->get_table(
                _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash);
        ASSERT_TRUE(tablet.get() != NULL);
        _header_file_name = tablet->header_file_name();

        // push data
        set_default_push_request(_create_tablet, &_push_req);
        std::vector<TTabletInfo> tablets_info;
        res = _command_executor->push(_push_req, &tablets_info);
        ASSERT_EQ(OLAP_SUCCESS, res);
        ASSERT_EQ(1, tablets_info.size());
    }

    void TearDown(){
        // Remove all dir.
        OLAPEngine::get_instance()->drop_table(
                _create_tablet.tablet_id, _create_tablet.tablet_schema.schema_hash);
        while (0 == access(_header_file_name.c_str(), F_OK)) {
            sleep(1);
        }
        ASSERT_EQ(OLAP_SUCCESS, remove_all_dir(config::storage_root_path));
    }
    
    CommandExecutor* _command_executor;
    std::string _header_file_name;
    TCreateTabletReq _create_tablet;
    TPushReq _push_req;
};

void set_delete_data_condition(TPushReq* request) {
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("-120");
    request->delete_conditions.push_back(condition);

    condition.column_name = "k3";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("ccc1aa42-e403-4964-a065-583f77e7ee98");
    request->delete_conditions.push_back(condition);

    condition.column_name = "k6";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("89.779");
    request->delete_conditions.push_back(condition);
}

TEST_F(TestDeleteData, delete_data_abnormal) {
    OLAPStatus res = OLAP_SUCCESS;
    std::vector<TTabletInfo> tablets_info;
    set_delete_data_condition(&_push_req);
    
    // check non_existed tablet
    _push_req.tablet_id = 0;
    res = _command_executor->delete_data(_push_req, &tablets_info);
    ASSERT_EQ(OLAP_ERR_TABLE_NOT_FOUND, res);

    // check invalid delete version
    _push_req.version = 1;
    _push_req.tablet_id = _create_tablet.tablet_id;
    res = _command_executor->delete_data(_push_req, &tablets_info);
    ASSERT_EQ(OLAP_ERR_PUSH_VERSION_INCORRECT, res);

    // check invalid delete condition
    _push_req.version += 2;
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.push_back("128");
    _push_req.delete_conditions.push_back(condition);
    res = _command_executor->delete_data(_push_req, &tablets_info);
    ASSERT_EQ(OLAP_ERR_DELETE_INVALID_CONDITION, res);
}

TEST_F(TestDeleteData, delete_data) {
    OLAPStatus res = OLAP_SUCCESS;
    std::vector<TTabletInfo> tablets_info;

    // 1. Submit delete data normally.
    tablets_info.clear();
    _push_req.version += 1;
    _push_req.version_hash +=  1;
    _push_req.__isset.http_file_path = false;
    set_delete_data_condition(&_push_req);

    res = _command_executor->delete_data(_push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());
    ASSERT_EQ(_push_req.version, tablets_info[0].version);
    ASSERT_EQ(_push_req.version_hash, tablets_info[0].version_hash);
    ASSERT_EQ(BASE_TABLE_PUSH_DATA_ROW_COUNT, tablets_info[0].row_count);

    // 2. Submit the the request.
    tablets_info.clear();
    res = _command_executor->delete_data(_push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());
    ASSERT_EQ(_push_req.version, tablets_info[0].version);
    ASSERT_EQ(_push_req.version_hash, tablets_info[0].version_hash);
    ASSERT_EQ(BASE_TABLE_PUSH_DATA_ROW_COUNT, tablets_info[0].row_count);
}

void set_cancel_delete_data_request(
        const TPushReq& push_req,
        TCancelDeleteDataReq* request) {
    request->tablet_id = push_req.tablet_id;
    request->schema_hash = push_req.schema_hash;
    request->version = push_req.version;
    request->version_hash = push_req.version_hash;
}

TEST_F(TestDeleteData, cancel_delete_abnormal) {
    OLAPStatus res = OLAP_SUCCESS;

    // check non_existed tablet
    TCancelDeleteDataReq request;
    set_cancel_delete_data_request(_push_req, &request);
    request.tablet_id = 0;
    res = _command_executor->cancel_delete(request);
    // ASSERT_EQ(OLAP_ERR_TABLE_NOT_FOUND, res);
    
    // check invalid version
    request.version = -1;
    request.tablet_id = _push_req.tablet_id;
    res = _command_executor->cancel_delete(request);
    ASSERT_EQ(OLAP_ERR_DELETE_INVALID_PARAMETERS, res);
}

TEST_F(TestDeleteData, cancel_delete) {
    OLAPStatus res = OLAP_SUCCESS;
    std::vector<TTabletInfo> tablets_info;

    // 1. Submit delete data first.
    tablets_info.clear();
    _push_req.version += 1;
    _push_req.version_hash += 1;
    _push_req.__isset.http_file_path = false;
    set_delete_data_condition(&_push_req);

    res = _command_executor->delete_data(_push_req, &tablets_info);
    ASSERT_EQ(OLAP_SUCCESS, res);
    ASSERT_EQ(1, tablets_info.size());
    ASSERT_EQ(_push_req.version, tablets_info[0].version);
    
    // 2. Cancel delete data request.
    TCancelDeleteDataReq request;
    set_cancel_delete_data_request(_push_req, &request);
    res = _command_executor->cancel_delete(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
    
    // 3. Cancel the delete data request again.
    res = _command_executor->cancel_delete(request);
    ASSERT_EQ(OLAP_SUCCESS, res);
}

} // namespace palo

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!palo::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    palo::init_glog("be-test");
    int ret = palo::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    palo::CpuInfo::init();

    palo::set_up();
    ret = RUN_ALL_TESTS();
    palo::tear_down();

    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}
