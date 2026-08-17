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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/config.h"
#include "common/consts.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/compaction/cumulative_compaction_policy.h"
#include "storage/compaction/cumulative_compaction_time_series_policy.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_meta_manager.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

namespace {

TColumn make_row_ttl_test_column(const std::string& name, TPrimitiveType::type type, bool is_key,
                                 bool is_nullable, TAggregationType::type aggregation,
                                 bool visible = true) {
    TColumnType column_type;
    column_type.__set_type(type);
    TColumn column;
    column.__set_column_name(name);
    column.__set_column_type(column_type);
    column.__set_is_key(is_key);
    column.__set_is_allow_null(is_nullable);
    column.__set_aggregation_type(aggregation);
    column.__set_visible(visible);
    return column;
}

TCreateTabletReq make_row_ttl_create_request(int64_t tablet_id, TKeysType::type keys_type,
                                             TPrimitiveType::type ttl_type,
                                             bool set_duration = false) {
    auto key_column =
            make_row_ttl_test_column("k", TPrimitiveType::INT, true, false, TAggregationType::NONE);
    auto ttl_column = make_row_ttl_test_column(
            TTL_COL, ttl_type, false, true,
            keys_type == TKeysType::DUP_KEYS ? TAggregationType::NONE : TAggregationType::REPLACE,
            false);

    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(keys_type);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns({key_column, ttl_column});
    tablet_schema.__set_ttl_col_idx(1);
    if (set_duration) {
        tablet_schema.__set_row_ttl_duration_us(1'000'000);
    }
    if (ttl_type != TPrimitiveType::BIGINT) {
        tablet_schema.__set_row_ttl_time_zone_offset_seconds(0);
    }

    TCreateTabletReq request;
    request.__set_tablet_schema(tablet_schema);
    request.__set_tablet_id(tablet_id);
    request.__set_version(2);
    return request;
}

} // namespace

class TabletMgrTest : public testing::Test {
public:
    virtual void SetUp() {
        _engine_data_path = "./be/test/storage/test_data/converter_test_data/tmp";
        auto st = io::global_local_filesystem()->delete_directory(_engine_data_path);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_engine_data_path);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(
                io::global_local_filesystem()->create_directory(_engine_data_path + "/meta").ok());

        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;
        EngineOptions options;
        // won't open engine, options.path is needless
        options.backend_uid = UniqueId::gen_uid();
        auto engine = std::make_unique<StorageEngine>(options);
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        k_engine = &ExecEnv::GetInstance()->storage_engine().to_local();
        _data_dir = new DataDir(*k_engine, _engine_data_path, 1000000000);
        static_cast<void>(_data_dir->init());
        _tablet_mgr = k_engine->tablet_manager();
    }

    virtual void TearDown() {
        SAFE_DELETE(_data_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_engine_data_path).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _tablet_mgr = nullptr;
        config::compaction_num_per_round = 1;
    }

    TabletSharedPtr create_compaction_tablet(
            int64_t tablet_id, int rowset_size,
            std::string_view compaction_policy = CUMULATIVE_SIZE_BASED_POLICY,
            DataDir* data_dir = nullptr) {
        data_dir = data_dir == nullptr ? _data_dir : data_dir;
        std::vector<TColumn> cols;
        TColumn col1;
        col1.column_type.type = TPrimitiveType::SMALLINT;
        col1.__set_column_name("col1");
        col1.__set_is_key(true);
        cols.push_back(col1);

        TColumn col2;
        col2.column_type.type = TPrimitiveType::INT;
        col2.__set_column_name(SEQUENCE_COL);
        col2.__set_is_key(false);
        col2.__set_aggregation_type(TAggregationType::REPLACE);
        cols.push_back(col2);

        TColumn col3;
        col3.column_type.type = TPrimitiveType::INT;
        col3.__set_column_name("v1");
        col3.__set_is_key(false);
        col3.__set_aggregation_type(TAggregationType::REPLACE);
        cols.push_back(col3);

        RuntimeProfile profile("CreateTablet");
        TTabletSchema tablet_schema;
        tablet_schema.__set_short_key_column_count(1);
        tablet_schema.__set_schema_hash(3333);
        tablet_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
        tablet_schema.__set_storage_type(TStorageType::COLUMN);
        tablet_schema.__set_columns(cols);
        tablet_schema.__set_sequence_col_idx(1);
        TCreateTabletReq create_tablet_req;
        create_tablet_req.__set_tablet_schema(tablet_schema);
        create_tablet_req.__set_tablet_id(tablet_id);
        create_tablet_req.__set_version(1);
        create_tablet_req.__set_replica_id(tablet_id * 10);
        create_tablet_req.__set_compaction_policy(std::string(compaction_policy));
        if (compaction_policy == CUMULATIVE_TIME_SERIES_POLICY) {
            create_tablet_req.__set_time_series_compaction_file_count_threshold(1);
        }
        std::vector<DataDir*> data_dirs;
        data_dirs.push_back(data_dir);
        Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
        if (!create_st.ok()) {
            ADD_FAILURE() << create_st;
            return nullptr;
        }

        TabletSharedPtr tablet = _tablet_mgr->get_tablet(tablet_id);
        if (tablet == nullptr) {
            ADD_FAILURE() << "failed to get tablet " << tablet_id;
            return nullptr;
        }

        auto create_rowset = [=, this](int64_t start, int64_t end) {
            auto rowset_meta = std::make_shared<RowsetMeta>();
            Version version(start, end);
            rowset_meta->set_version(version);
            rowset_meta->set_tablet_id(tablet->tablet_id());
            rowset_meta->set_tablet_uid(tablet->tablet_uid());
            rowset_meta->set_rowset_id(k_engine->next_rowset_id());
            return std::make_shared<BetaRowset>(tablet->tablet_schema(), std::move(rowset_meta),
                                                tablet->tablet_path());
        };
        auto st = tablet->init();
        if (!st.ok()) {
            ADD_FAILURE() << st;
            return nullptr;
        }
        for (int i = 2; i <= rowset_size; ++i) {
            auto rs = create_rowset(i, i);
            st = tablet->add_inc_rowset(rs);
            if (!st.ok()) {
                ADD_FAILURE() << st;
                return nullptr;
            }
        }
        return tablet;
    }

    StorageEngine* k_engine;

private:
    DataDir* _data_dir = nullptr;
    std::string _engine_data_path;
    TabletManager* _tablet_mgr = nullptr;
};

TEST_F(TabletMgrTest, CreateTablet) {
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    RuntimeProfile profile("CreateTablet");
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist).ok());
    EXPECT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    Status check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    EXPECT_TRUE(check_meta_st == Status::OK());

    // retry create should be successfully
    create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());

    Status drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet.reset();
    Status trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
}

TEST_F(TabletMgrTest, CreateTabletWithSequence) {
    std::vector<TColumn> cols;
    TColumn col1;
    col1.column_type.type = TPrimitiveType::SMALLINT;
    col1.__set_column_name("col1");
    col1.__set_is_key(true);
    cols.push_back(col1);

    TColumn col2;
    col2.column_type.type = TPrimitiveType::INT;
    col2.__set_column_name(SEQUENCE_COL);
    col2.__set_is_key(false);
    col2.__set_aggregation_type(TAggregationType::REPLACE);
    cols.push_back(col2);

    TColumn col3;
    col3.column_type.type = TPrimitiveType::INT;
    col3.__set_column_name("v1");
    col3.__set_is_key(false);
    col3.__set_aggregation_type(TAggregationType::REPLACE);
    cols.push_back(col3);

    RuntimeProfile profile("CreateTablet");
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    tablet_schema.__set_sequence_col_idx(1);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());

    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);
    // check dir exist
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist).ok());
    EXPECT_TRUE(dir_exist);
    // check meta has this tablet
    TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
    Status check_meta_st = TabletMetaManager::get_meta(_data_dir, 111, 3333, new_tablet_meta);
    EXPECT_TRUE(check_meta_st == Status::OK());

    Status drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet.reset();
    Status trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
}

TEST_F(TabletMgrTest, ValidateRowTtlSchema) {
    std::vector<DataDir*> data_dirs {_data_dir};
    RuntimeProfile profile("ValidateRowTtlSchema");
    auto create_tablet = [&](const TCreateTabletReq& request) {
        return _tablet_mgr->create_tablet(request, data_dirs, &profile);
    };
    auto expect_invalid = [&](const TCreateTabletReq& request, const std::string& message) {
        Status status = create_tablet(request);
        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
        EXPECT_THAT(status.to_string(), testing::HasSubstr(message));
    };

    auto aggregate_key =
            make_row_ttl_create_request(201, TKeysType::AGG_KEYS, TPrimitiveType::BIGINT);
    aggregate_key.tablet_schema.columns[1].__set_aggregation_type(TAggregationType::REPLACE);
    expect_invalid(aggregate_key, "Row TTL is not supported for AGG KEY tables");

    auto missing_index =
            make_row_ttl_create_request(202, TKeysType::DUP_KEYS, TPrimitiveType::BIGINT);
    missing_index.tablet_schema.__isset.ttl_col_idx = false;
    expect_invalid(missing_index, "Row TTL column index is missing or out of range");

    auto negative_index =
            make_row_ttl_create_request(203, TKeysType::DUP_KEYS, TPrimitiveType::BIGINT);
    negative_index.tablet_schema.__set_ttl_col_idx(-1);
    expect_invalid(negative_index, "Row TTL column index is missing or out of range");

    auto out_of_range =
            make_row_ttl_create_request(204, TKeysType::DUP_KEYS, TPrimitiveType::BIGINT);
    out_of_range.tablet_schema.__set_ttl_col_idx(2);
    expect_invalid(out_of_range, "Row TTL column index is missing or out of range");

    auto expect_invalid_column = [&](int64_t tablet_id,
                                     const std::function<void(TColumn&)>& mutate) {
        auto request =
                make_row_ttl_create_request(tablet_id, TKeysType::DUP_KEYS, TPrimitiveType::BIGINT);
        mutate(request.tablet_schema.columns[1]);
        expect_invalid(request, "Row TTL column must be a hidden nullable temporal or BIGINT");
    };
    expect_invalid_column(205, [](TColumn& column) { column.__set_column_name("not_row_ttl"); });
    expect_invalid_column(
            206, [](TColumn& column) { column.column_type.__set_type(TPrimitiveType::INT); });
    expect_invalid_column(207, [](TColumn& column) { column.__set_is_key(true); });
    expect_invalid_column(208, [](TColumn& column) { column.__set_is_allow_null(false); });
    expect_invalid_column(209, [](TColumn& column) { column.__set_visible(true); });
    expect_invalid_column(
            210, [](TColumn& column) { column.__set_aggregation_type(TAggregationType::REPLACE); });

    auto temporal_without_duration =
            make_row_ttl_create_request(211, TKeysType::DUP_KEYS, TPrimitiveType::TIMESTAMPTZ);
    expect_invalid(temporal_without_duration,
                   "Row TTL duration must be set only for a temporal hidden column");

    auto direct_with_duration =
            make_row_ttl_create_request(212, TKeysType::DUP_KEYS, TPrimitiveType::BIGINT, true);
    expect_invalid(direct_with_duration,
                   "Row TTL duration must be set only for a temporal hidden column");

    auto temporal = make_row_ttl_create_request(213, TKeysType::DUP_KEYS,
                                                TPrimitiveType::TIMESTAMPTZ, true);
    ASSERT_TRUE(create_tablet(temporal).ok());
    auto temporal_tablet = _tablet_mgr->get_tablet(temporal.tablet_id);
    ASSERT_NE(temporal_tablet, nullptr);
    EXPECT_EQ(temporal_tablet->tablet_schema()->ttl_col_idx(), 1);
    EXPECT_EQ(temporal_tablet->tablet_schema()->row_ttl_duration_us(), 1'000'000);
    ASSERT_TRUE(temporal_tablet->tablet_schema()->has_row_ttl_time_zone_offset_seconds());
    EXPECT_EQ(temporal_tablet->tablet_schema()->row_ttl_time_zone_offset_seconds(), 0);
    EXPECT_EQ(temporal_tablet->tablet_schema()->column(1).name(), TTL_COL);
    EXPECT_EQ(temporal_tablet->tablet_schema()->column(1).type(),
              FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ);

    auto direct_without_restore =
            make_row_ttl_create_request(214, TKeysType::UNIQUE_KEYS, TPrimitiveType::BIGINT);
    expect_invalid(direct_without_restore,
                   "Direct-expiration Row TTL tablets may only be created during restore");

    auto direct = make_row_ttl_create_request(215, TKeysType::UNIQUE_KEYS, TPrimitiveType::BIGINT);
    direct.__set_in_restore_mode(true);
    ASSERT_TRUE(create_tablet(direct).ok());
    auto direct_tablet = _tablet_mgr->get_tablet(direct.tablet_id);
    ASSERT_NE(direct_tablet, nullptr);
    EXPECT_EQ(direct_tablet->tablet_schema()->ttl_col_idx(), 1);
    EXPECT_EQ(direct_tablet->tablet_schema()->row_ttl_duration_us(), -1);
    EXPECT_EQ(direct_tablet->tablet_schema()->column(1).type(), FieldType::OLAP_FIELD_TYPE_BIGINT);
    EXPECT_EQ(direct_tablet->tablet_schema()->column(1).aggregation(),
              FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE);

    auto missing_offset =
            make_row_ttl_create_request(216, TKeysType::DUP_KEYS, TPrimitiveType::DATETIMEV2, true);
    missing_offset.tablet_schema.__isset.row_ttl_time_zone_offset_seconds = false;
    expect_invalid(missing_offset,
                   "Row TTL time zone offset is required for a temporal hidden column");

    auto legacy_restore = missing_offset;
    legacy_restore.__set_tablet_id(217);
    legacy_restore.__set_in_restore_mode(true);
    ASSERT_TRUE(create_tablet(legacy_restore).ok());
    auto legacy_restore_tablet = _tablet_mgr->get_tablet(legacy_restore.tablet_id);
    ASSERT_NE(legacy_restore_tablet, nullptr);
    EXPECT_FALSE(legacy_restore_tablet->tablet_schema()->has_row_ttl_time_zone_offset_seconds());

    auto invalid_offset =
            make_row_ttl_create_request(218, TKeysType::DUP_KEYS, TPrimitiveType::DATETIMEV2, true);
    invalid_offset.tablet_schema.__set_row_ttl_time_zone_offset_seconds(8 * 60 * 60 + 1);
    expect_invalid(invalid_offset,
                   "Row TTL time zone offset 28801 must be a whole minute in [-43200, 50400]");

    auto timestamp_non_utc = make_row_ttl_create_request(219, TKeysType::DUP_KEYS,
                                                         TPrimitiveType::TIMESTAMPTZ, true);
    timestamp_non_utc.tablet_schema.__set_row_ttl_time_zone_offset_seconds(60);
    expect_invalid(timestamp_non_utc, "TIMESTAMPTZ Row TTL time zone offset must be 0");

    ASSERT_TRUE(_tablet_mgr->drop_tablet(temporal.tablet_id, temporal.replica_id, false).ok());
    ASSERT_TRUE(_tablet_mgr->drop_tablet(direct.tablet_id, direct.replica_id, false).ok());
    ASSERT_TRUE(_tablet_mgr->drop_tablet(legacy_restore.tablet_id, legacy_restore.replica_id, false)
                        .ok());
    temporal_tablet.reset();
    direct_tablet.reset();
    legacy_restore_tablet.reset();
    ASSERT_TRUE(_tablet_mgr->start_trash_sweep().ok());
}

TEST_F(TabletMgrTest, DropTablet) {
    RuntimeProfile profile("CreateTablet");
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    // drop unexist tablet will be success
    Status drop_st = _tablet_mgr->drop_tablet(1121, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    // drop exist tablet will be success
    drop_st = _tablet_mgr->drop_tablet(111, create_tablet_req.replica_id, false);
    EXPECT_TRUE(drop_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111);
    EXPECT_TRUE(tablet == nullptr);
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet != nullptr);

    // check dir exist
    std::string tablet_path = tablet->tablet_path();
    bool dir_exist = false;
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_TRUE(dir_exist);

    // do trash sweep, tablet will not be garbage collected
    // because tablet ptr referenced it
    Status trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet != nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_TRUE(dir_exist);

    // reset tablet ptr
    tablet.reset();
    trash_st = _tablet_mgr->start_trash_sweep();
    EXPECT_TRUE(trash_st == Status::OK());
    tablet = _tablet_mgr->get_tablet(111, true);
    EXPECT_TRUE(tablet == nullptr);
    EXPECT_TRUE(io::global_local_filesystem()->exists(tablet_path, &dir_exist).ok());
    EXPECT_FALSE(dir_exist);
}

TEST_F(TabletMgrTest, GetRowsetId) {
    // normal case
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);
    }
    {
        std::string path = _engine_data_path + "/data/0/15007/368169781/";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);
    }
    // normal case
    {
        std::string path =
                _engine_data_path +
                "/data/0/15007/368169781/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(368169781, schema_hash);

        RowsetId id;
        EXPECT_TRUE(_tablet_mgr->get_rowset_id_from_path(path, &id));
        EXPECT_EQ(2UL << 56 | 1, id.hi);
        EXPECT_EQ(2, id.mi);
        EXPECT_EQ(3, id.lo);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(0, schema_hash);

        RowsetId id;
        EXPECT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007/";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_TRUE(_tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
        EXPECT_EQ(15007, tid);
        EXPECT_EQ(0, schema_hash);
    }
    // empty tablet directory
    {
        std::string path = _engine_data_path + "/data/0/15007abc";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_FALSE(
                _tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));
    }
    // not match pattern
    {
        std::string path =
                _engine_data_path +
                "/data/0/15007/123abc/020000000000000100000000000000020000000000000003_0_0.dat";
        TTabletId tid;
        TSchemaHash schema_hash;
        EXPECT_FALSE(
                _tablet_mgr->get_tablet_id_and_schema_hash_from_path(path, &tid, &schema_hash));

        RowsetId id;
        EXPECT_FALSE(_tablet_mgr->get_rowset_id_from_path(path, &id));
    }
}

TEST_F(TabletMgrTest, FindTabletWithCompact) {
    auto create_tablet = [this](int64_t tablet_id, int rowset_size,
                                std::string_view compaction_policy = CUMULATIVE_SIZE_BASED_POLICY) {
        std::vector<TColumn> cols;
        TColumn col1;
        col1.column_type.type = TPrimitiveType::SMALLINT;
        col1.__set_column_name("col1");
        col1.__set_is_key(true);
        cols.push_back(col1);

        TColumn col2;
        col2.column_type.type = TPrimitiveType::INT;
        col2.__set_column_name(SEQUENCE_COL);
        col2.__set_is_key(false);
        col2.__set_aggregation_type(TAggregationType::REPLACE);
        cols.push_back(col2);

        TColumn col3;
        col3.column_type.type = TPrimitiveType::INT;
        col3.__set_column_name("v1");
        col3.__set_is_key(false);
        col3.__set_aggregation_type(TAggregationType::REPLACE);
        cols.push_back(col3);

        RuntimeProfile profile("CreateTablet");
        TTabletSchema tablet_schema;
        tablet_schema.__set_short_key_column_count(1);
        tablet_schema.__set_schema_hash(3333);
        tablet_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
        tablet_schema.__set_storage_type(TStorageType::COLUMN);
        tablet_schema.__set_columns(cols);
        tablet_schema.__set_sequence_col_idx(1);
        TCreateTabletReq create_tablet_req;
        create_tablet_req.__set_tablet_schema(tablet_schema);
        create_tablet_req.__set_tablet_id(tablet_id);
        create_tablet_req.__set_version(1);
        create_tablet_req.__set_replica_id(tablet_id * 10);
        create_tablet_req.__set_compaction_policy(std::string(compaction_policy));
        if (compaction_policy == CUMULATIVE_TIME_SERIES_POLICY) {
            create_tablet_req.__set_time_series_compaction_file_count_threshold(1);
        }
        std::vector<DataDir*> data_dirs;
        data_dirs.push_back(_data_dir);
        Status create_st = _tablet_mgr->create_tablet(create_tablet_req, data_dirs, &profile);
        ASSERT_TRUE(create_st.ok()) << create_st;

        TabletSharedPtr tablet = _tablet_mgr->get_tablet(tablet_id);
        ASSERT_TRUE(tablet);
        // check dir exist
        bool dir_exist = false;
        Status exist_st = io::global_local_filesystem()->exists(tablet->tablet_path(), &dir_exist);
        ASSERT_TRUE(exist_st.ok()) << exist_st;
        ASSERT_TRUE(dir_exist);
        // check meta has this tablet
        TabletMetaSharedPtr new_tablet_meta(new TabletMeta());
        Status check_meta_st =
                TabletMetaManager::get_meta(_data_dir, tablet_id, 3333, new_tablet_meta);
        ASSERT_TRUE(check_meta_st.ok()) << check_meta_st;
        // insert into rowset
        auto create_rowset = [=, this](int64_t start, int64_t end) {
            auto rowset_meta = std::make_shared<RowsetMeta>();
            Version version(start, end);
            rowset_meta->set_version(version);
            rowset_meta->set_tablet_id(tablet->tablet_id());
            rowset_meta->set_tablet_uid(tablet->tablet_uid());
            rowset_meta->set_rowset_id(k_engine->next_rowset_id());
            return std::make_shared<BetaRowset>(tablet->tablet_schema(), std::move(rowset_meta),
                                                tablet->tablet_path());
        };
        auto st = tablet->init();
        ASSERT_TRUE(st.ok()) << st;
        for (int i = 2; i <= rowset_size; ++i) {
            auto rs = create_rowset(i, i);
            auto st = tablet->add_inc_rowset(rs);
            ASSERT_TRUE(st.ok()) << st;
        }
    };

    int rowset_size = 5;

    // create 10 tablets
    for (int64_t id = 1; id <= 10; ++id) {
        create_tablet(id, rowset_size++);
    }

    std::unordered_set<TabletSharedPtr> cumu_set;
    std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>
            cumulative_compaction_policies;
    cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_SIZE_BASED_POLICY);
    cumulative_compaction_policies[CUMULATIVE_TIME_SERIES_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_TIME_SERIES_POLICY);
    CompactionScoreStats score_stats;
    auto compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0].tablet->tablet_id(), 10);
    ASSERT_TRUE(score_stats.scanned);
    ASSERT_EQ(score_stats.max_score, 14);
    ASSERT_EQ(score_stats.size_based_max_score, 14);
    ASSERT_EQ(score_stats.time_series_max_score, 0);

    // create 10 more tablets with higher compaction scores
    for (int64_t id = 11; id <= 20; ++id) {
        create_tablet(id, rowset_size++);
    }

    compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0].tablet->tablet_id(), 20);
    ASSERT_EQ(score_stats.max_score, 24);
    ASSERT_EQ(score_stats.size_based_max_score, 24);
    ASSERT_EQ(score_stats.time_series_max_score, 0);

    create_tablet(21, rowset_size++);

    compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
            cumulative_compaction_policies);
    ASSERT_EQ(compact_tablets.size(), 1);
    ASSERT_EQ(compact_tablets[0].tablet->tablet_id(), 21);
    ASSERT_EQ(score_stats.max_score, 25);
    ASSERT_EQ(score_stats.size_based_max_score, 25);
    ASSERT_EQ(score_stats.time_series_max_score, 0);

    // drop all tablets
    for (int64_t id = 1; id <= 21; ++id) {
        Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
        ASSERT_TRUE(drop_st.ok()) << drop_st;
    }

    {
        create_tablet(40001, 8, CUMULATIVE_SIZE_BASED_POLICY);
        create_tablet(40002, 12, CUMULATIVE_TIME_SERIES_POLICY);

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
                cumulative_compaction_policies);
        ASSERT_TRUE(score_stats.scanned);
        ASSERT_EQ(score_stats.max_score, 12);
        ASSERT_EQ(score_stats.size_based_max_score, 8);
        ASSERT_EQ(score_stats.time_series_max_score, 12);
        ASSERT_EQ(compact_tablets.size(), 1);
        ASSERT_EQ(compact_tablets[0].tablet->tablet_id(), 40002);

        Status drop_st = _tablet_mgr->drop_tablet(40001, 400010, false);
        ASSERT_TRUE(drop_st.ok()) << drop_st;
        drop_st = _tablet_mgr->drop_tablet(40002, 400020, false);
        ASSERT_TRUE(drop_st.ok()) << drop_st;
    }

    {
        k_engine->_compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 100; ++i) {
            create_tablet(10000 + i, i);
        }

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 10);
        int index = 0;
        for (auto& t : compact_tablets) {
            ASSERT_EQ(t.tablet->tablet_id(), 10100 - index);
            ASSERT_EQ(t.tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION),
                      100 - index);
            index++;
        }
        k_engine->_compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 10001; id <= 10100; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }
    }

    {
        k_engine->_compaction_num_per_round = 10;
        for (int64_t i = 1; i <= 5; ++i) {
            create_tablet(30000 + i, i + 5);
        }

        compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
                CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
                cumulative_compaction_policies);
        ASSERT_EQ(compact_tablets.size(), 5);
        for (int i = 0; i < 5; ++i) {
            ASSERT_EQ(compact_tablets[i].tablet->tablet_id(), 30000 + 5 - i);
            ASSERT_EQ(compact_tablets[i].tablet->calc_compaction_score(
                              CompactionType::CUMULATIVE_COMPACTION),
                      10 - i);
        }

        k_engine->_compaction_num_per_round = 1;
        // drop all tablets
        for (int64_t id = 30001; id <= 30005; ++id) {
            Status drop_st = _tablet_mgr->drop_tablet(id, id * 10, false);
            ASSERT_TRUE(drop_st.ok()) << drop_st;
        }
    }

    Status trash_st = _tablet_mgr->start_trash_sweep();
    ASSERT_TRUE(trash_st.ok()) << trash_st;
}

TEST_F(TabletMgrTest, FindBestTabletsIgnoresUnsuitablePolicyScore) {
    auto tablet = create_compaction_tablet(50001, 12, CUMULATIVE_TIME_SERIES_POLICY);
    ASSERT_TRUE(tablet != nullptr);
    ASSERT_GT(tablet->calc_compaction_score(CompactionType::CUMULATIVE_COMPACTION), 5);

    bool old_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    Defer restore_debug_points([&] { config::enable_debug_points = old_enable_debug_points; });
    DebugPoints::instance()->add("Tablet._calc_cumulative_compaction_score.return");
    Defer clear_debug_point([] { DebugPoints::instance()->clear(); });

    std::unordered_set<TabletSharedPtr> cumu_set;
    std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>
            cumulative_compaction_policies;
    cumulative_compaction_policies[CUMULATIVE_SIZE_BASED_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_SIZE_BASED_POLICY);
    cumulative_compaction_policies[CUMULATIVE_TIME_SERIES_POLICY] =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    CUMULATIVE_TIME_SERIES_POLICY);

    CompactionScoreStats score_stats;
    auto compact_tablets = _tablet_mgr->find_best_tablets_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, _data_dir, cumu_set, &score_stats,
            cumulative_compaction_policies);
    ASSERT_TRUE(score_stats.scanned);
    ASSERT_EQ(score_stats.max_score, 0);
    ASSERT_EQ(score_stats.size_based_max_score, 0);
    ASSERT_EQ(score_stats.time_series_max_score, 0);
    ASSERT_TRUE(compact_tablets.empty());
}

TEST_F(TabletMgrTest, GenerateCompactionTasksClearsMissingPolicyScoreOnCheck) {
    auto tablet = create_compaction_tablet(51001, 8, CUMULATIVE_SIZE_BASED_POLICY);
    ASSERT_TRUE(tablet != nullptr);
    auto* metrics = DorisMetrics::instance();
    metrics->tablet_cumulative_max_compaction_score->set_value(101);
    metrics->tablet_size_based_max_compaction_score->set_value(102);
    metrics->tablet_time_series_max_compaction_score->set_value(200);

    std::vector<DataDir*> data_dirs {_data_dir};
    auto tasks = k_engine->generate_compaction_tasks_for_test(CompactionType::CUMULATIVE_COMPACTION,
                                                              data_dirs, true);

    ASSERT_EQ(tasks.size(), 1);
    ASSERT_EQ(tasks[0]->tablet_id(), 51001);
    ASSERT_EQ(metrics->tablet_cumulative_max_compaction_score->value(), 8);
    ASSERT_EQ(metrics->tablet_size_based_max_compaction_score->value(), 8);
    ASSERT_EQ(metrics->tablet_time_series_max_compaction_score->value(), 0);
}

TEST_F(TabletMgrTest, GenerateCompactionTasksKeepsMissingPolicyScoreWithoutCheck) {
    auto tablet = create_compaction_tablet(52001, 8, CUMULATIVE_SIZE_BASED_POLICY);
    ASSERT_TRUE(tablet != nullptr);
    auto* metrics = DorisMetrics::instance();
    metrics->tablet_cumulative_max_compaction_score->set_value(101);
    metrics->tablet_size_based_max_compaction_score->set_value(102);
    metrics->tablet_time_series_max_compaction_score->set_value(200);

    std::vector<DataDir*> data_dirs {_data_dir};
    auto tasks = k_engine->generate_compaction_tasks_for_test(CompactionType::CUMULATIVE_COMPACTION,
                                                              data_dirs, false);

    ASSERT_EQ(tasks.size(), 1);
    ASSERT_EQ(tasks[0]->tablet_id(), 52001);
    ASSERT_EQ(metrics->tablet_cumulative_max_compaction_score->value(), 8);
    ASSERT_EQ(metrics->tablet_size_based_max_compaction_score->value(), 8);
    ASSERT_EQ(metrics->tablet_time_series_max_compaction_score->value(), 200);
}

TEST_F(TabletMgrTest, GenerateCompactionTasksDoesNotUpdateMetricWhenNoDirScanned) {
    auto* metrics = DorisMetrics::instance();
    metrics->tablet_cumulative_max_compaction_score->set_value(101);
    metrics->tablet_size_based_max_compaction_score->set_value(102);
    metrics->tablet_time_series_max_compaction_score->set_value(200);

    std::vector<DataDir*> data_dirs;
    auto tasks = k_engine->generate_compaction_tasks_for_test(CompactionType::CUMULATIVE_COMPACTION,
                                                              data_dirs, true);

    ASSERT_TRUE(tasks.empty());
    ASSERT_EQ(metrics->tablet_cumulative_max_compaction_score->value(), 101);
    ASSERT_EQ(metrics->tablet_size_based_max_compaction_score->value(), 102);
    ASSERT_EQ(metrics->tablet_time_series_max_compaction_score->value(), 200);
}

TEST_F(TabletMgrTest, GenerateCompactionTasksAggregatesScoreWhenNoSlot) {
    auto dummy = create_compaction_tablet(53000, 5, CUMULATIVE_SIZE_BASED_POLICY);
    auto size_based = create_compaction_tablet(53001, 8, CUMULATIVE_SIZE_BASED_POLICY);
    auto time_series = create_compaction_tablet(53002, 12, CUMULATIVE_TIME_SERIES_POLICY);
    ASSERT_TRUE(dummy != nullptr);
    ASSERT_TRUE(size_based != nullptr);
    ASSERT_TRUE(time_series != nullptr);

    std::vector<DataDir*> data_dirs {_data_dir};
    auto& registry = k_engine->compaction_submit_registry_for_test();
    registry.reset(data_dirs);
    Defer reset_registry([&] { registry.reset(data_dirs); });
    dummy->compaction_stage = CompactionStage::EXECUTING;
    ASSERT_FALSE(registry.insert(dummy, CompactionType::CUMULATIVE_COMPACTION));

    int32_t old_compaction_task_num_per_disk = config::compaction_task_num_per_disk;
    config::compaction_task_num_per_disk = 1;
    Defer restore_config(
            [&] { config::compaction_task_num_per_disk = old_compaction_task_num_per_disk; });
    bool old_enable_compaction_priority_scheduling = config::enable_compaction_priority_scheduling;
    config::enable_compaction_priority_scheduling = false;
    Defer restore_priority_scheduling([&] {
        config::enable_compaction_priority_scheduling = old_enable_compaction_priority_scheduling;
    });

    auto* metrics = DorisMetrics::instance();
    metrics->tablet_cumulative_max_compaction_score->set_value(0);
    metrics->tablet_size_based_max_compaction_score->set_value(0);
    metrics->tablet_time_series_max_compaction_score->set_value(0);

    auto tasks = k_engine->generate_compaction_tasks_for_test(CompactionType::CUMULATIVE_COMPACTION,
                                                              data_dirs, true);

    ASSERT_TRUE(tasks.empty());
    ASSERT_EQ(metrics->tablet_cumulative_max_compaction_score->value(), 12);
    ASSERT_EQ(metrics->tablet_size_based_max_compaction_score->value(), 8);
    ASSERT_EQ(metrics->tablet_time_series_max_compaction_score->value(), 12);
}

TEST_F(TabletMgrTest, GenerateCompactionTasksDoesNotLowerPolicyScoreWhenDirFull) {
    std::string full_dir_path = "./be/test/storage/test_data/converter_test_data/tmp_full";
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(full_dir_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(full_dir_path).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(full_dir_path + "/meta").ok());
    Defer cleanup_full_dir([&] {
        static_cast<void>(io::global_local_filesystem()->delete_directory(full_dir_path));
    });

    auto full_data_dir = std::make_unique<DataDir>(*k_engine, full_dir_path, 1000000000);
    ASSERT_TRUE(full_data_dir->init().ok());
    auto full_time_series =
            create_compaction_tablet(54001, 12, CUMULATIVE_TIME_SERIES_POLICY, full_data_dir.get());
    auto size_based = create_compaction_tablet(54002, 8, CUMULATIVE_SIZE_BASED_POLICY);
    ASSERT_TRUE(full_time_series != nullptr);
    ASSERT_TRUE(size_based != nullptr);
    Defer drop_full_tablet(
            [&] { static_cast<void>(_tablet_mgr->drop_tablet(54001, 540010, false)); });
    full_data_dir->set_capacity_for_test(100, 0);

    auto* metrics = DorisMetrics::instance();
    metrics->tablet_cumulative_max_compaction_score->set_value(200);
    metrics->tablet_size_based_max_compaction_score->set_value(0);
    metrics->tablet_time_series_max_compaction_score->set_value(200);

    std::vector<DataDir*> data_dirs {_data_dir, full_data_dir.get()};
    auto tasks = k_engine->generate_compaction_tasks_for_test(CompactionType::CUMULATIVE_COMPACTION,
                                                              data_dirs, true);

    ASSERT_EQ(tasks.size(), 1);
    ASSERT_EQ(tasks[0]->tablet_id(), 54002);
    ASSERT_EQ(metrics->tablet_cumulative_max_compaction_score->value(), 200);
    ASSERT_EQ(metrics->tablet_size_based_max_compaction_score->value(), 8);
    ASSERT_EQ(metrics->tablet_time_series_max_compaction_score->value(), 200);
}

TEST_F(TabletMgrTest, LoadTabletFromMeta) {
    TTabletId tablet_id = 111;
    TSchemaHash schema_hash = 3333;
    TColumnType col_type;
    col_type.__set_type(TPrimitiveType::SMALLINT);
    TColumn col1;
    col1.__set_column_name("col1");
    col1.__set_column_type(col_type);
    col1.__set_is_key(true);
    std::vector<TColumn> cols;
    cols.push_back(col1);
    TTabletSchema tablet_schema;
    tablet_schema.__set_short_key_column_count(1);
    tablet_schema.__set_schema_hash(3333);
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);
    tablet_schema.__set_columns(cols);
    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_schema(tablet_schema);
    create_tablet_req.__set_tablet_id(111);
    create_tablet_req.__set_version(2);
    std::vector<DataDir*> data_dirs;
    data_dirs.push_back(_data_dir);
    RuntimeProfile profile("CreateTablet");
    Status create_st =
            k_engine->tablet_manager()->create_tablet(create_tablet_req, data_dirs, &profile);
    EXPECT_TRUE(create_st == Status::OK());
    TabletSharedPtr tablet = k_engine->tablet_manager()->get_tablet(111);
    EXPECT_TRUE(tablet != nullptr);

    std::string serialized_tablet_meta;
    tablet->tablet_meta()->serialize(&serialized_tablet_meta);
    bool update_meta = true;
    bool force = true;
    bool restore = false;
    bool check_path = true;
    Status st = _tablet_mgr->load_tablet_from_meta(_data_dir, tablet_id, schema_hash,
                                                   serialized_tablet_meta, update_meta, force,
                                                   restore, check_path);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // After reload, the original tablet should not be allowed to save meta.
    ASSERT_FALSE(tablet->do_tablet_meta_checkpoint());
}

} // namespace doris
