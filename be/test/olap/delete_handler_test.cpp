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

#include "olap/delete_handler.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "gutil/strings/numbers.h"
#include "io/fs/local_file_system.h"
#include "json2pb/json_to_pb.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_tuple.h"
#include "olap/options.h"
#include "olap/row_cursor.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "runtime/exec_env.h"
#include "util/cpu_info.h"

using namespace std;
using namespace doris;
using google::protobuf::RepeatedPtrField;

namespace doris {
using namespace ErrorCode;

static const uint32_t MAX_PATH_LEN = 1024;
static std::unique_ptr<StorageEngine> k_engine;

static void set_up() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    config::storage_root_path = string(buffer) + "/data_test";
    EXPECT_TRUE(io::global_local_filesystem()
                        ->delete_and_create_directory(config::storage_root_path)
                        .ok());
    EXPECT_TRUE(io::global_local_filesystem()
                        ->delete_directory(string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX)
                        .ok());

    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);
    config::min_file_descriptor_number = 1000;
    config::tablet_map_shard_size = 1;
    config::txn_map_shard_size = 1;
    config::txn_shard_size = 1;

    doris::EngineOptions options;
    options.store_paths = paths;
    k_engine = std::make_unique<StorageEngine>(options);
    Status s = k_engine->open();
    EXPECT_TRUE(s.ok()) << s.to_string();
    ExecEnv::GetInstance()->set_storage_engine(k_engine.get());
}

static void tear_down() {
    char buffer[MAX_PATH_LEN];
    EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
    config::storage_root_path = string(buffer) + "/data_test";
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(config::storage_root_path).ok());
    EXPECT_TRUE(io::global_local_filesystem()
                        ->delete_directory(string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX)
                        .ok());
    ExecEnv::GetInstance()->set_storage_engine(nullptr);
    k_engine.reset();
}

static void set_default_create_tablet_request(TCreateTabletReq* request) {
    request->tablet_id = 10003;
    request->__set_version(1);
    request->tablet_schema.schema_hash = 270068375;
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
    k2.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    k3.column_type.type = TPrimitiveType::INT;
    request->tablet_schema.columns.push_back(k3);

    TColumn k4;
    k4.column_name = "k4";
    k4.__set_is_key(true);
    k4.column_type.type = TPrimitiveType::BIGINT;
    request->tablet_schema.columns.push_back(k4);

    TColumn k5;
    k5.column_name = "k5";
    k5.__set_is_key(true);
    k5.column_type.type = TPrimitiveType::LARGEINT;
    request->tablet_schema.columns.push_back(k5);

    TColumn k9;
    k9.column_name = "k9";
    k9.__set_is_key(true);
    k9.column_type.type = TPrimitiveType::DECIMALV2;
    k9.column_type.__set_precision(6);
    k9.column_type.__set_scale(3);
    request->tablet_schema.columns.push_back(k9);

    TColumn k10;
    k10.column_name = "k10";
    k10.__set_is_key(true);
    k10.column_type.type = TPrimitiveType::DATE;
    request->tablet_schema.columns.push_back(k10);

    TColumn k11;
    k11.column_name = "k11";
    k11.__set_is_key(true);
    k11.column_type.type = TPrimitiveType::DATETIME;
    request->tablet_schema.columns.push_back(k11);

    TColumn k12;
    k12.column_name = "k12";
    k12.__set_is_key(true);
    k12.column_type.__set_len(64);
    k12.column_type.type = TPrimitiveType::CHAR;
    request->tablet_schema.columns.push_back(k12);

    TColumn kSpecial;
    kSpecial.column_name = "k$1";
    kSpecial.__set_is_key(true);
    kSpecial.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(kSpecial);

    TColumn k13;
    k13.column_name = "k13";
    k13.__set_is_key(true);
    k13.column_type.__set_len(64);
    k13.column_type.type = TPrimitiveType::VARCHAR;
    request->tablet_schema.columns.push_back(k13);

    TColumn v;
    v.column_name = "v";
    v.__set_is_key(false);
    v.column_type.type = TPrimitiveType::BIGINT;
    v.__set_aggregation_type(TAggregationType::SUM);
    request->tablet_schema.columns.push_back(v);
}

static void set_create_duplicate_tablet_request(TCreateTabletReq* request) {
    request->tablet_id = 10009;
    request->__set_version(1);
    request->tablet_schema.schema_hash = 270068376;
    request->tablet_schema.short_key_column_count = 2;
    request->tablet_schema.keys_type = TKeysType::DUP_KEYS;
    request->tablet_schema.storage_type = TStorageType::COLUMN;

    TColumn k1;
    k1.column_name = "k1";
    k1.__set_is_key(true);
    k1.column_type.type = TPrimitiveType::TINYINT;
    request->tablet_schema.columns.push_back(k1);

    TColumn k2;
    k2.column_name = "k2";
    k2.__set_is_key(true);
    k2.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(k2);

    TColumn k3;
    k3.column_name = "k3";
    k3.__set_is_key(true);
    k3.column_type.type = TPrimitiveType::INT;
    request->tablet_schema.columns.push_back(k3);

    TColumn k4;
    k4.column_name = "k4";
    k4.__set_is_key(true);
    k4.column_type.type = TPrimitiveType::BIGINT;
    request->tablet_schema.columns.push_back(k4);

    TColumn k5;
    k5.column_name = "k5";
    k5.__set_is_key(true);
    k5.column_type.type = TPrimitiveType::LARGEINT;
    request->tablet_schema.columns.push_back(k5);

    TColumn k9;
    k9.column_name = "k9";
    k9.__set_is_key(true);
    k9.column_type.type = TPrimitiveType::DECIMALV2;
    k9.column_type.__set_precision(6);
    k9.column_type.__set_scale(3);
    request->tablet_schema.columns.push_back(k9);

    TColumn k10;
    k10.column_name = "k10";
    k10.__set_is_key(true);
    k10.column_type.type = TPrimitiveType::DATE;
    request->tablet_schema.columns.push_back(k10);

    TColumn k11;
    k11.column_name = "k11";
    k11.__set_is_key(true);
    k11.column_type.type = TPrimitiveType::DATETIME;
    request->tablet_schema.columns.push_back(k11);

    TColumn k12;
    k12.column_name = "k12";
    k12.__set_is_key(true);
    k12.column_type.__set_len(64);
    k12.column_type.type = TPrimitiveType::CHAR;
    request->tablet_schema.columns.push_back(k12);

    TColumn kSpecial;
    kSpecial.column_name = "k$1";
    kSpecial.__set_is_key(true);
    kSpecial.column_type.type = TPrimitiveType::SMALLINT;
    request->tablet_schema.columns.push_back(kSpecial);

    TColumn k13;
    k13.column_name = "k13";
    k13.__set_is_key(true);
    k13.column_type.__set_len(64);
    k13.column_type.type = TPrimitiveType::VARCHAR;
    request->tablet_schema.columns.push_back(k13);

    TColumn v;
    v.column_name = "v";
    v.__set_is_key(false);
    v.column_type.type = TPrimitiveType::BIGINT;
    request->tablet_schema.columns.push_back(v);
}

class TestDeleteConditionHandler : public testing::Test {
public:
    static void SetUpTestSuite() {
        config::min_file_descriptor_number = 100;
        set_up();
    }

    static void TearDownTestSuite() { tear_down(); }

protected:
    void SetUp() {
        // Create local data dir for StorageEngine.
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        config::storage_root_path = string(buffer) + "/data_delete_condition";
        EXPECT_TRUE(io::global_local_filesystem()
                            ->delete_and_create_directory(config::storage_root_path)
                            .ok());

        // 1. Prepare for query split key.
        // create base tablet
        Status res = Status::OK();
        RuntimeProfile profile("CreateTablet");
        set_default_create_tablet_request(&_create_tablet);
        res = k_engine->create_tablet(_create_tablet, &profile);
        EXPECT_EQ(Status::OK(), res);
        tablet = k_engine->tablet_manager()->get_tablet(_create_tablet.tablet_id);
        EXPECT_NE(tablet.get(), nullptr);
        _tablet_path = tablet->tablet_path();

        set_create_duplicate_tablet_request(&_create_dup_tablet);
        res = k_engine->create_tablet(_create_dup_tablet, &profile);
        EXPECT_EQ(Status::OK(), res);
        dup_tablet = k_engine->tablet_manager()->get_tablet(_create_dup_tablet.tablet_id);
        EXPECT_TRUE(dup_tablet.get() != NULL);
        _dup_tablet_path = tablet->tablet_path();
    }

    void TearDown() {
        // Remove all dir.
        tablet.reset();
        dup_tablet.reset();
        static_cast<void>(StorageEngine::instance()->tablet_manager()->drop_tablet(
                _create_tablet.tablet_id, _create_tablet.replica_id, false));
        EXPECT_TRUE(
                io::global_local_filesystem()->delete_directory(config::storage_root_path).ok());
    }

    std::string _tablet_path;
    std::string _dup_tablet_path;
    TabletSharedPtr tablet;
    TabletSharedPtr dup_tablet;
    TCreateTabletReq _create_tablet;
    TCreateTabletReq _create_dup_tablet;
};

TEST_F(TestDeleteConditionHandler, StoreCondSucceed) {
    Status success_res;
    std::vector<TCondition> conditions;

    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = ">";
    condition.condition_values.clear();
    condition.condition_values.push_back("3");
    conditions.push_back(condition);

    condition.column_name = "k3";
    condition.condition_op = "<=";
    condition.condition_values.clear();
    condition.condition_values.push_back("5");
    conditions.push_back(condition);

    condition.column_name = "k4";
    condition.condition_op = "IS";
    condition.condition_values.clear();
    condition.condition_values.push_back("NULL");
    conditions.push_back(condition);

    condition.column_name = "k5";
    condition.condition_op = "*=";
    condition.condition_values.clear();
    condition.condition_values.push_back("7");
    conditions.push_back(condition);

    condition.column_name = "k12";
    condition.condition_op = "!*=";
    condition.condition_values.clear();
    condition.condition_values.push_back("9");
    conditions.push_back(condition);

    condition.column_name = "k$1";
    condition.condition_op = ">";
    condition.condition_values.clear();
    condition.condition_values.push_back("1");
    conditions.push_back(condition);

    condition.column_name = "k13";
    condition.condition_op = "*=";
    condition.condition_values.clear();
    condition.condition_values.push_back("1");
    condition.condition_values.push_back("3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    success_res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                           &del_pred);
    EXPECT_EQ(Status::OK(), success_res);

    // 验证存储在header中的过滤条件正确
    EXPECT_EQ(size_t(7), del_pred.sub_predicates_size());
    EXPECT_STREQ("k1='1'", del_pred.sub_predicates(0).c_str());
    EXPECT_STREQ("k2>>'3'", del_pred.sub_predicates(1).c_str());
    EXPECT_STREQ("k3<='5'", del_pred.sub_predicates(2).c_str());
    EXPECT_STREQ("k4 IS NULL", del_pred.sub_predicates(3).c_str());
    EXPECT_STREQ("k5='7'", del_pred.sub_predicates(4).c_str());
    EXPECT_STREQ("k12!='9'", del_pred.sub_predicates(5).c_str());
    EXPECT_STREQ("k$1>>'1'", del_pred.sub_predicates(6).c_str());

    // check sub predicate v2

    EXPECT_EQ(size_t(7), del_pred.sub_predicates_v2_size());
    EXPECT_STREQ("k1", del_pred.sub_predicates_v2(0).column_name().c_str());
    EXPECT_STREQ("k2", del_pred.sub_predicates_v2(1).column_name().c_str());
    EXPECT_STREQ("k3", del_pred.sub_predicates_v2(2).column_name().c_str());
    EXPECT_STREQ("k4", del_pred.sub_predicates_v2(3).column_name().c_str());
    EXPECT_STREQ("k5", del_pred.sub_predicates_v2(4).column_name().c_str());
    EXPECT_STREQ("k12", del_pred.sub_predicates_v2(5).column_name().c_str());
    EXPECT_STREQ("k$1", del_pred.sub_predicates_v2(6).column_name().c_str());

    EXPECT_STREQ("=", del_pred.sub_predicates_v2(0).op().c_str());
    EXPECT_STREQ(">>", del_pred.sub_predicates_v2(1).op().c_str());
    EXPECT_STREQ("<=", del_pred.sub_predicates_v2(2).op().c_str());
    EXPECT_STREQ("IS", del_pred.sub_predicates_v2(3).op().c_str());
    EXPECT_STREQ("=", del_pred.sub_predicates_v2(4).op().c_str());
    EXPECT_STREQ("!=", del_pred.sub_predicates_v2(5).op().c_str());
    EXPECT_STREQ(">>", del_pred.sub_predicates_v2(6).op().c_str());

    EXPECT_STREQ("1", del_pred.sub_predicates_v2(0).cond_value().c_str());
    EXPECT_STREQ("3", del_pred.sub_predicates_v2(1).cond_value().c_str());
    EXPECT_STREQ("5", del_pred.sub_predicates_v2(2).cond_value().c_str());
    EXPECT_STREQ("NULL", del_pred.sub_predicates_v2(3).cond_value().c_str());
    EXPECT_STREQ("7", del_pred.sub_predicates_v2(4).cond_value().c_str());
    EXPECT_STREQ("9", del_pred.sub_predicates_v2(5).cond_value().c_str());
    EXPECT_STREQ("1", del_pred.sub_predicates_v2(6).cond_value().c_str());

    EXPECT_EQ(size_t(1), del_pred.in_predicates_size());
    EXPECT_FALSE(del_pred.in_predicates(0).is_not_in());
    EXPECT_STREQ("k13", del_pred.in_predicates(0).column_name().c_str());
    EXPECT_EQ(std::size_t(2), del_pred.in_predicates(0).values().size());
}

// 检测参数不正确的情况，包括：空的过滤条件字符串
TEST_F(TestDeleteConditionHandler, StoreCondInvalidParameters) {
    // 空的过滤条件
    std::vector<TCondition> conditions;
    DeletePredicatePB del_pred;
    Status failed_res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(),
                                                                 conditions, &del_pred);
    EXPECT_EQ(Status::Error<DELETE_INVALID_PARAMETERS>(""), failed_res);
}

// 检测过滤条件中指定的列不存在,或者列不符合要求
TEST_F(TestDeleteConditionHandler, StoreCondNonexistentColumn) {
    // 'k100'是一个不存在的列
    std::vector<TCondition> conditions;
    TCondition condition;
    condition.column_name = "k100";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("2");
    conditions.push_back(condition);
    DeletePredicatePB del_pred;
    Status failed_res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(),
                                                                 conditions, &del_pred);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), failed_res);

    // 'v'是value列
    conditions.clear();
    condition.column_name = "v";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("5");
    conditions.push_back(condition);

    Status success_res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(),
                                                                  conditions, &del_pred);
    EXPECT_EQ(Status::OK(), success_res);

    // value column in duplicate model can be deleted;
    conditions.clear();
    condition.column_name = "v";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("5");
    conditions.push_back(condition);

    success_res = DeleteHandler::generate_delete_predicate(*dup_tablet->tablet_schema(), conditions,
                                                           &del_pred);
    EXPECT_EQ(Status::OK(), success_res);
}

// 测试删除条件值不符合类型要求
class TestDeleteConditionHandler2 : public testing::Test {
protected:
    static void SetUpTestSuite() {
        config::min_file_descriptor_number = 100;
        set_up();
    }

    static void TearDownTestSuite() { tear_down(); }
    void SetUp() {
        // Create local data dir for StorageEngine.
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        config::storage_root_path = string(buffer) + "/data_delete_condition";
        EXPECT_TRUE(io::global_local_filesystem()
                            ->delete_and_create_directory(config::storage_root_path)
                            .ok());

        // 1. Prepare for query split key.
        // create base tablet
        RuntimeProfile profile("CreateTablet");
        Status res = Status::OK();
        set_default_create_tablet_request(&_create_tablet);
        res = k_engine->create_tablet(_create_tablet, &profile);
        EXPECT_EQ(Status::OK(), res);
        tablet = k_engine->tablet_manager()->get_tablet(_create_tablet.tablet_id);
        EXPECT_TRUE(tablet.get() != nullptr);
        _tablet_path = tablet->tablet_path();
    }

    void TearDown() {
        // Remove all dir.
        tablet.reset();
        static_cast<void>(k_engine->tablet_manager()->drop_tablet(
                _create_tablet.tablet_id, _create_tablet.replica_id, false));
        EXPECT_TRUE(
                io::global_local_filesystem()->delete_directory(config::storage_root_path).ok());
    }

    std::string _tablet_path;
    TabletSharedPtr tablet;
    TCreateTabletReq _create_tablet;
};

TEST_F(TestDeleteConditionHandler2, ValidConditionValue) {
    Status res;
    std::vector<TCondition> conditions;

    // 测试数据中, k1,k2,k3,k4类型分别为int8, int16, int32, int64
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("-1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("-1");
    conditions.push_back(condition);

    condition.column_name = "k3";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("-1");
    conditions.push_back(condition);

    condition.column_name = "k4";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("-1");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions, &del_pred);
    EXPECT_EQ(Status::OK(), res);

    // k5类型为int128
    conditions.clear();
    condition.column_name = "k5";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("1");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_2;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_2);
    EXPECT_EQ(Status::OK(), res);

    // k9类型为decimal, precision=6, frac=3
    conditions.clear();
    condition.column_name = "k9";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("2.3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_3;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_3);
    EXPECT_EQ(Status::OK(), res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("2");
    DeletePredicatePB del_pred_4;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_4);
    EXPECT_EQ(Status::OK(), res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("-2");
    DeletePredicatePB del_pred_5;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_5);
    EXPECT_EQ(Status::OK(), res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("-2.3");
    DeletePredicatePB del_pred_6;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_6);
    EXPECT_EQ(Status::OK(), res);

    // k10,k11类型分别为date, datetime
    conditions.clear();
    condition.column_name = "k10";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("2014-01-01");
    conditions.push_back(condition);

    condition.column_name = "k10";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("2014-01-01 00:00:00");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_7;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_7);
    EXPECT_EQ(Status::OK(), res);

    // k12,k13类型分别为string(64), varchar(64)
    conditions.clear();
    condition.column_name = "k12";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("YWFh");
    conditions.push_back(condition);

    condition.column_name = "k13";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("YWFhYQ==");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_8;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_8);
    EXPECT_EQ(Status::OK(), res);
}

TEST_F(TestDeleteConditionHandler2, InvalidConditionValue) {
    Status res;
    std::vector<TCondition> conditions;

    // 测试k1的值越上界，k1类型为int8
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("1000");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_1;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_1);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k1的值越下界，k1类型为int8
    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("-1000");
    DeletePredicatePB del_pred_2;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_2);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k2的值越上界，k2类型为int16
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k2";
    conditions[0].condition_values.push_back("32768");
    DeletePredicatePB del_pred_3;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_3);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k2的值越下界，k2类型为int16
    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("-32769");
    DeletePredicatePB del_pred_4;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_4);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k3的值越上界，k3类型为int32
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k3";
    conditions[0].condition_values.push_back("2147483648");
    DeletePredicatePB del_pred_5;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_5);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k3的值越下界，k3类型为int32
    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("-2147483649");
    DeletePredicatePB del_pred_6;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_6);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k4的值越上界，k2类型为int64
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k4";
    conditions[0].condition_values.push_back("9223372036854775808");
    DeletePredicatePB del_pred_7;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_7);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k4的值越下界，k1类型为int64
    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("-9223372036854775809");
    DeletePredicatePB del_pred_8;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_8);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k5的值越上界，k5类型为int128
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k5";
    conditions[0].condition_values.push_back("170141183460469231731687303715884105728");
    DeletePredicatePB del_pred_9;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_9);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k5的值越下界，k5类型为int128
    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("-170141183460469231731687303715884105729");
    DeletePredicatePB del_pred_10;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_10);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k9整数部分长度过长，k9类型为decimal, precision=6, frac=3
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k9";
    conditions[0].condition_values.push_back("12347876.5");
    DeletePredicatePB del_pred_11;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_11);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k9小数部分长度过长，k9类型为decimal, precision=6, frac=3
    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("1.2345678");
    DeletePredicatePB del_pred_12;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_12);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k9没有小数部分，但包含小数点
    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("1.");
    DeletePredicatePB del_pred_13;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_13);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k10类型的过滤值不符合对应格式，k10为date
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k10";
    conditions[0].condition_values.push_back("20130101");
    DeletePredicatePB del_pred_14;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_14);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("2013-64-01");
    DeletePredicatePB del_pred_15;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_15);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("2013-01-40");
    DeletePredicatePB del_pred_16;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_16);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k11类型的过滤值不符合对应格式，k11为datetime
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k11";
    conditions[0].condition_values.push_back("20130101 00:00:00");
    DeletePredicatePB del_pred_17;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_17);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("2013-64-01 00:00:00");
    DeletePredicatePB del_pred_18;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_18);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("2013-01-40 00:00:00");
    DeletePredicatePB del_pred_19;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_19);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("2013-01-01 24:00:00");
    DeletePredicatePB del_pred_20;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_20);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("2013-01-01 00:60:00");
    DeletePredicatePB del_pred_21;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_21);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    conditions[0].condition_values.clear();
    conditions[0].condition_values.push_back("2013-01-01 00:00:60");
    DeletePredicatePB del_pred_22;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_22);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    // 测试k12和k13类型的过滤值过长，k12,k13类型分别为string(64), varchar(64)
    conditions[0].condition_values.clear();
    conditions[0].column_name = "k12";
    conditions[0].condition_values.push_back(
            "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWE=;k13=YWFhYQ==");
    DeletePredicatePB del_pred_23;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_23);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);

    conditions[0].condition_values.clear();
    conditions[0].column_name = "k13";
    conditions[0].condition_values.push_back(
            "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYW"
            "FhYWFhYWFhYWFhYWFhYWFhYWFhYWE=;k13=YWFhYQ==");
    DeletePredicatePB del_pred_24;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_24);
    EXPECT_EQ(Status::Error<DELETE_INVALID_CONDITION>(""), res);
}

class TestDeleteHandler : public testing::Test {
public:
    static void SetUpTestSuite() {
        config::min_file_descriptor_number = 100;
        set_up();
    }

    static void TearDownTestSuite() { tear_down(); }

protected:
    void SetUp() {
        CpuInfo::init();
        // Create local data dir for StorageEngine.
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        config::storage_root_path = string(buffer) + "/data_delete_condition";
        EXPECT_TRUE(io::global_local_filesystem()
                            ->delete_and_create_directory(config::storage_root_path)
                            .ok());

        // 1. Prepare for query split key.
        // create base tablet
        RuntimeProfile profile("CreateTablet");
        Status res = Status::OK();
        set_default_create_tablet_request(&_create_tablet);
        res = k_engine->create_tablet(_create_tablet, &profile);
        EXPECT_EQ(Status::OK(), res);
        tablet = k_engine->tablet_manager()->get_tablet(_create_tablet.tablet_id);
        EXPECT_TRUE(tablet != nullptr);
        _tablet_path = tablet->tablet_path();

        static_cast<void>(_data_row_cursor.init(tablet->tablet_schema()));
        static_cast<void>(
                _data_row_cursor.allocate_memory_for_string_type(tablet->tablet_schema()));
        _json_rowset_meta = R"({
            "rowset_id": 540081,
            "tablet_id": 15673,
            "txn_id": 4042,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "start_version": 2,
            "end_version": 2,
            "num_rows": 3929,
            "total_disk_size": 84699,
            "data_disk_size": 84464,
            "index_disk_size": 235,
            "empty": false,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765670
        })";
    }

    void TearDown() {
        // Remove all dir.
        tablet.reset();
        _delete_handler.finalize();
        static_cast<void>(StorageEngine::instance()->tablet_manager()->drop_tablet(
                _create_tablet.tablet_id, _create_tablet.replica_id, false));
        EXPECT_TRUE(
                io::global_local_filesystem()->delete_directory(config::storage_root_path).ok());
    }

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end) {
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(_json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
    }

    void add_delete_predicate(DeletePredicatePB& del_pred, int64_t version) {
        RowsetMetaSharedPtr rsm(new RowsetMeta());
        init_rs_meta(rsm, version, version);
        RowsetId id;
        id.init(version * 1000);
        rsm->set_rowset_id(id);
        rsm->set_delete_predicate(del_pred);
        rsm->set_tablet_schema(tablet->tablet_schema());
        RowsetSharedPtr rowset = std::make_shared<BetaRowset>(tablet->tablet_schema(), "", rsm);
        static_cast<void>(tablet->add_rowset(rowset));
    }

    std::vector<RowsetMetaSharedPtr> get_delete_predicates() {
        std::vector<RowsetMetaSharedPtr> delete_preds;
        for (auto&& rs_meta : tablet->tablet_meta()->_rs_metas) {
            if (rs_meta->has_delete_predicate()) {
                delete_preds.push_back(rs_meta);
            }
        }
        return delete_preds;
    }

    std::string _tablet_path;
    RowCursor _data_row_cursor;
    TabletSharedPtr tablet;
    TCreateTabletReq _create_tablet;
    DeleteHandler _delete_handler;
    std::string _json_rowset_meta;
};

TEST_F(TestDeleteHandler, ValueWithQuote) {
    DeletePredicatePB del_predicate;
    del_predicate.add_sub_predicates("k1='b'");
    del_predicate.add_sub_predicates("k1='b");
    del_predicate.add_sub_predicates("k1=b'");
    del_predicate.add_sub_predicates("k1=''b'");
    del_predicate.add_sub_predicates("k1='b''");
    del_predicate.add_sub_predicates("k1=''b''");
    del_predicate.set_version(2);

    add_delete_predicate(del_predicate, 2);

    auto res = _delete_handler.init(tablet->tablet_schema(), get_delete_predicates(), 5);
    EXPECT_EQ(Status::OK(), res);
    _delete_handler.finalize();
}

TEST_F(TestDeleteHandler, ValueWithoutQuote) {
    DeletePredicatePB del_predicate;
    del_predicate.add_sub_predicates("k1=b");
    del_predicate.add_sub_predicates("k2=123");
    del_predicate.set_version(2);

    add_delete_predicate(del_predicate, 2);

    auto res = _delete_handler.init(tablet->tablet_schema(), get_delete_predicates(), 5);
    EXPECT_EQ(Status::OK(), res);
    _delete_handler.finalize();
}

TEST_F(TestDeleteHandler, InitSuccess) {
    Status res;
    std::vector<TCondition> conditions;

    // 往头文件中添加过滤条件
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = ">";
    condition.condition_values.clear();
    condition.condition_values.push_back("3");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = "<=";
    condition.condition_values.clear();
    condition.condition_values.push_back("5");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions, &del_pred);
    EXPECT_EQ(Status::OK(), res);
    add_delete_predicate(del_pred, 2);

    conditions.clear();
    condition.column_name = "k1";
    condition.condition_op = "!=";
    condition.condition_values.clear();
    condition.condition_values.push_back("3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_2;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_2);
    EXPECT_EQ(Status::OK(), res);
    add_delete_predicate(del_pred_2, 3);

    conditions.clear();
    condition.column_name = "k2";
    condition.condition_op = ">=";
    condition.condition_values.clear();
    condition.condition_values.push_back("1");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_3;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_3);
    EXPECT_EQ(Status::OK(), res);
    add_delete_predicate(del_pred_3, 4);

    conditions.clear();
    condition.column_name = "k2";
    condition.condition_op = "!=";
    condition.condition_values.clear();
    condition.condition_values.push_back("3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_4;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_4);
    EXPECT_EQ(Status::OK(), res);
    add_delete_predicate(del_pred_4, 5);

    // Get delete conditions which version <= 5
    res = _delete_handler.init(tablet->tablet_schema(), get_delete_predicates(), 5);
    EXPECT_EQ(Status::OK(), res);
    _delete_handler.finalize();
}

// 测试一个过滤条件包含的子条件之间是and关系,
// 即只有满足一条过滤条件包含的所有子条件，这条数据才会被过滤
TEST_F(TestDeleteHandler, FilterDataSubconditions) {
    Status res;
    std::vector<TCondition> conditions;

    // 往Header中添加过滤条件
    // 过滤条件1
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = "!=";
    condition.condition_values.clear();
    condition.condition_values.push_back("4");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions, &del_pred);
    EXPECT_EQ(Status::OK(), res);
    add_delete_predicate(del_pred, 2);

    // 指定版本号为10以载入Header中的所有过滤条件(在这个case中，只有过滤条件1)
    res = _delete_handler.init(tablet->tablet_schema(), get_delete_predicates(), 4);
    EXPECT_EQ(Status::OK(), res);

    // 构造一行测试数据
    std::vector<string> data_str;
    data_str.push_back("1");
    data_str.push_back("6");
    data_str.push_back("8");
    data_str.push_back("-1");
    data_str.push_back("16");
    data_str.push_back("1.2");
    data_str.push_back("2014-01-01");
    data_str.push_back("2014-01-01 00:00:00");
    data_str.push_back("YWFH");
    data_str.push_back("1");
    data_str.push_back("YWFH==");
    data_str.push_back("1");
    OlapTuple tuple1(data_str);
    res = _data_row_cursor.from_tuple(tuple1);
    EXPECT_EQ(Status::OK(), res);

    // 构造一行测试数据
    data_str[1] = "4";
    OlapTuple tuple2(data_str);
    res = _data_row_cursor.from_tuple(tuple2);
    EXPECT_EQ(Status::OK(), res);

    _delete_handler.finalize();
}

// 测试多个过滤条件之间是or关系，
// 即如果存在多个过滤条件，会一次检查数据是否符合这些过滤条件；只要有一个过滤条件符合，则过滤数据
TEST_F(TestDeleteHandler, FilterDataConditions) {
    Status res;
    std::vector<TCondition> conditions;

    // 往Header中添加过滤条件
    // 过滤条件1
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = "!=";
    condition.condition_values.clear();
    condition.condition_values.push_back("4");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions, &del_pred);
    EXPECT_EQ(Status::OK(), res);
    add_delete_predicate(del_pred, 2);

    // 过滤条件2
    conditions.clear();
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_2;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_2);
    EXPECT_EQ(Status::OK(), res);
    add_delete_predicate(del_pred_2, 3);

    // 过滤条件3
    conditions.clear();
    condition.column_name = "k2";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("5");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_3;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_3);
    EXPECT_EQ(Status::OK(), res);
    add_delete_predicate(del_pred_3, 4);

    // 指定版本号为4以载入meta中的所有过滤条件(在这个case中，只有过滤条件1)
    res = _delete_handler.init(tablet->tablet_schema(), get_delete_predicates(), 4);
    EXPECT_EQ(Status::OK(), res);

    std::vector<string> data_str;
    data_str.push_back("4");
    data_str.push_back("5");
    data_str.push_back("8");
    data_str.push_back("-1");
    data_str.push_back("16");
    data_str.push_back("1.2");
    data_str.push_back("2014-01-01");
    data_str.push_back("2014-01-01 00:00:00");
    data_str.push_back("YWFH");
    data_str.push_back("1");
    data_str.push_back("YWFH==");
    data_str.push_back("1");
    OlapTuple tuple(data_str);
    res = _data_row_cursor.from_tuple(tuple);
    EXPECT_EQ(Status::OK(), res);

    _delete_handler.finalize();
}

// 测试在过滤时，版本号小于数据版本的过滤条件将不起作用
TEST_F(TestDeleteHandler, FilterDataVersion) {
    Status res;
    std::vector<TCondition> conditions;

    // 往Header中添加过滤条件
    // 过滤条件1
    TCondition condition;
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("1");
    conditions.push_back(condition);

    condition.column_name = "k2";
    condition.condition_op = "!=";
    condition.condition_values.clear();
    condition.condition_values.push_back("4");
    conditions.push_back(condition);

    DeletePredicatePB del_pred;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions, &del_pred);
    EXPECT_EQ(Status::OK(), res);
    add_delete_predicate(del_pred, 3);

    // 过滤条件2
    conditions.clear();
    condition.column_name = "k1";
    condition.condition_op = "=";
    condition.condition_values.clear();
    condition.condition_values.push_back("3");
    conditions.push_back(condition);

    DeletePredicatePB del_pred_2;
    res = DeleteHandler::generate_delete_predicate(*tablet->tablet_schema(), conditions,
                                                   &del_pred_2);
    EXPECT_EQ(Status::OK(), res);
    add_delete_predicate(del_pred_2, 4);

    // 指定版本号为4以载入meta中的所有过滤条件(过滤条件1，过滤条件2)
    res = _delete_handler.init(tablet->tablet_schema(), get_delete_predicates(), 4);
    EXPECT_EQ(Status::OK(), res);

    // 构造一行测试数据
    std::vector<string> data_str;
    data_str.push_back("1");
    data_str.push_back("6");
    data_str.push_back("8");
    data_str.push_back("-1");
    data_str.push_back("16");
    data_str.push_back("1.2");
    data_str.push_back("2014-01-01");
    data_str.push_back("2014-01-01 00:00:00");
    data_str.push_back("YWFH");
    data_str.push_back("1");
    data_str.push_back("YWFH==");
    data_str.push_back("1");
    OlapTuple tuple(data_str);
    res = _data_row_cursor.from_tuple(tuple);
    EXPECT_EQ(Status::OK(), res);

    _delete_handler.finalize();
}

} // namespace doris
