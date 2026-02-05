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

#include "olap/snapshot_manager.h"

#include <gen_cpp/AgentService_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "runtime/exec_env.h"
#include "util/uid_util.h"

namespace doris {
class SnapshotManagerTest : public testing::Test {
public:
    virtual void SetUp() {
        _engine_data_path = "./be/test/olap/test_data/converter_test_data/tmp";
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
        options.backend_uid = UniqueId::gen_uid();
        auto engine = std::make_unique<StorageEngine>(options);
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _engine = &ExecEnv::GetInstance()->storage_engine().to_local();
        _data_dir = new DataDir(*_engine, _engine_data_path, 1000000000);
        static_cast<void>(_data_dir->init());
    }

    virtual void TearDown() {
        SAFE_DELETE(_data_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_engine_data_path).ok());
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _engine = nullptr;
    }

protected:
    StorageEngine* _engine = nullptr;
    DataDir* _data_dir = nullptr;
    std::string _engine_data_path;
};

TEST_F(SnapshotManagerTest, TestMakeSnapshotInvalidParameters) {
    TSnapshotRequest request;
    request.tablet_id = 10001;
    request.schema_hash = 12345;

    std::string* null_snapshot_path = nullptr;
    bool allow_incremental_clone = false;

    Status status = _engine->snapshot_mgr()->make_snapshot(request, null_snapshot_path,
                                                           &allow_incremental_clone);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
}

TEST_F(SnapshotManagerTest, TestReleaseSnapshotInvalidPath) {
    std::string invalid_path = "/invalid/snapshot/path";

    Status status = _engine->snapshot_mgr()->release_snapshot(invalid_path);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::CE_CMD_PARAMS_ERROR);
}

TEST_F(SnapshotManagerTest, TestConvertRowsetIdsInvalidDir) {
    std::string non_existent_dir = "/non/existent/dir";
    int64_t tablet_id = 10003;
    int64_t replica_id = 1;
    int64_t table_id = 1000;
    int64_t partition_id = 100;
    int32_t schema_hash = 54321;

    auto result = _engine->snapshot_mgr()->convert_rowset_ids(
            non_existent_dir, tablet_id, replica_id, table_id, partition_id, schema_hash);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::DIR_NOT_EXIST);
}

TEST_F(SnapshotManagerTest, TestConvertRowsetIdsNormal) {
    std::string clone_dir = _engine_data_path + "/clone_dir_local";
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(clone_dir).ok());

    TabletMetaPB tablet_meta_pb;
    tablet_meta_pb.set_tablet_id(10006);
    tablet_meta_pb.set_schema_hash(12346);
    tablet_meta_pb.set_replica_id(1);
    tablet_meta_pb.set_table_id(1000);
    tablet_meta_pb.set_partition_id(100);

    TabletSchemaPB* schema_pb = tablet_meta_pb.mutable_schema();
    schema_pb->set_num_short_key_columns(1);
    schema_pb->set_num_rows_per_row_block(1024);
    schema_pb->set_compress_kind(COMPRESS_LZ4);

    // column 0
    ColumnPB* column = schema_pb->add_column();
    column->set_unique_id(0);
    column->set_name("k1");
    column->set_type("INT");
    column->set_is_key(true);
    column->set_is_nullable(false);
    column->set_length(4);
    column->set_index_length(4);
    column->set_precision(0);
    column->set_frac(0);

    // column 1
    column = schema_pb->add_column();
    column->set_unique_id(1);
    column->set_name("v1");
    column->set_type("INT");
    column->set_is_key(false);
    column->set_is_nullable(true);
    column->set_length(4);
    column->set_index_length(4);
    column->set_precision(0);
    column->set_frac(0);

    // index 0
    TabletIndexPB* index_pb = schema_pb->add_index();
    index_pb->set_index_id(1001);
    index_pb->set_index_name("test_index");
    index_pb->set_index_type(IndexType::BITMAP);
    index_pb->add_col_unique_id(0);

    // rowset meta 0
    RowsetMetaPB* rowset_meta = tablet_meta_pb.add_rs_metas();
    rowset_meta->set_rowset_id(10001);
    rowset_meta->set_rowset_id_v2("02000000000000010001");
    rowset_meta->set_tablet_id(10006);
    rowset_meta->set_tablet_schema_hash(12346);
    rowset_meta->set_rowset_type(BETA_ROWSET);
    rowset_meta->set_rowset_state(VISIBLE);
    rowset_meta->set_start_version(0);
    rowset_meta->set_end_version(1);
    rowset_meta->set_num_rows(100);
    rowset_meta->set_total_disk_size(1024);
    rowset_meta->set_data_disk_size(1000);
    rowset_meta->set_index_disk_size(24);
    rowset_meta->set_empty(false);

    TabletSchemaPB* rowset_schema = rowset_meta->mutable_tablet_schema();
    rowset_schema->CopyFrom(*schema_pb);

    // new ids
    int64_t tablet_id = 20006;
    int64_t replica_id = 2;
    int64_t table_id = 2000;
    int64_t partition_id = 200;
    int32_t schema_hash = 65432;

    // save the meta file
    std::string meta_file = clone_dir + "/" + std::to_string(tablet_id) + ".hdr";
    EXPECT_TRUE(TabletMeta::save(meta_file, tablet_meta_pb).ok());

    TCreateTabletReq create_tablet_req;
    create_tablet_req.__set_tablet_id(tablet_id);
    create_tablet_req.__set_replica_id(replica_id);
    create_tablet_req.__set_table_id(table_id);
    create_tablet_req.__set_partition_id(partition_id);
    create_tablet_req.__set_version(1);

    TTabletSchema tablet_schema;
    tablet_schema.__set_schema_hash(schema_hash);
    tablet_schema.__set_short_key_column_count(schema_pb->num_short_key_columns());
    tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
    tablet_schema.__set_storage_type(TStorageType::COLUMN);

    std::vector<TColumn> columns;
    for (int i = 0; i < schema_pb->column_size(); i++) {
        const ColumnPB& column_pb = schema_pb->column(i);
        TColumn tcolumn;
        tcolumn.__set_column_name(column_pb.name());
        tcolumn.__set_is_key(column_pb.is_key());
        TColumnType col_type;
        col_type.__set_type(TPrimitiveType::INT);
        tcolumn.__set_column_type(col_type);
        tcolumn.__set_is_allow_null(column_pb.is_nullable());
        if (!column_pb.is_key()) {
            tcolumn.__set_aggregation_type(TAggregationType::SUM);
        }
        columns.push_back(tcolumn);
    }
    tablet_schema.__set_columns(columns);

    std::vector<TOlapTableIndex> indexes;
    for (int i = 0; i < schema_pb->index_size(); i++) {
        TOlapTableIndex tindex;
        tindex.__set_index_id(2001 + i);
        tindex.__set_index_name("test_index");
        tindex.__set_index_type(TIndexType::BITMAP);
        tindex.__set_columns({"k1"});
        tindex.__set_column_unique_ids({0});
        indexes.push_back(tindex);
    }
    tablet_schema.__set_indexes(indexes);

    create_tablet_req.__set_tablet_schema(tablet_schema);
    std::vector<DataDir*> stores;
    stores.push_back(_data_dir);
    RuntimeProfile profile("CreateTablet");

    Status status = _engine->tablet_manager()->create_tablet(create_tablet_req, stores, &profile);
    EXPECT_TRUE(status.ok()) << "Failed to create tablet: " << status;

    auto result = _engine->snapshot_mgr()->convert_rowset_ids(clone_dir, tablet_id, replica_id,
                                                              table_id, partition_id, schema_hash);
    EXPECT_TRUE(result.has_value());

    TabletMetaPB converted_meta_pb;
    EXPECT_TRUE(TabletMeta::load_from_file(meta_file, &converted_meta_pb).ok());

    EXPECT_EQ(converted_meta_pb.tablet_id(), tablet_id);
    EXPECT_EQ(converted_meta_pb.schema_hash(), schema_hash);
    EXPECT_EQ(converted_meta_pb.replica_id(), replica_id);
    EXPECT_EQ(converted_meta_pb.table_id(), table_id);
    EXPECT_EQ(converted_meta_pb.partition_id(), partition_id);

    // verify schema
    EXPECT_EQ(converted_meta_pb.schema().num_short_key_columns(), 1);
    EXPECT_EQ(converted_meta_pb.schema().column_size(), 2);

    // verify index
    EXPECT_EQ(converted_meta_pb.schema().index_size(), 1);
    const TabletIndexPB& converted_index = converted_meta_pb.schema().index(0);
    EXPECT_EQ(converted_index.index_id(), 2001);
    EXPECT_EQ(converted_index.index_name(), "test_index");
    EXPECT_EQ(converted_index.index_type(), IndexType::BITMAP);
    EXPECT_EQ(converted_index.col_unique_id_size(), 1);
    EXPECT_EQ(converted_index.col_unique_id(0), 0);

    // verify rowset meta
    EXPECT_EQ(converted_meta_pb.rs_metas_size(), 1);
    const RowsetMetaPB& converted_rowset_meta = converted_meta_pb.rs_metas(0);
    EXPECT_NE(converted_rowset_meta.rowset_id(), 10001);
    EXPECT_NE(converted_rowset_meta.rowset_id_v2(), "02000000000000010001");
    EXPECT_EQ(converted_rowset_meta.tablet_id(), tablet_id);
    EXPECT_EQ(converted_rowset_meta.rowset_type(), BETA_ROWSET);
    EXPECT_EQ(converted_rowset_meta.rowset_state(), VISIBLE);
    EXPECT_EQ(converted_rowset_meta.start_version(), 0);
    EXPECT_EQ(converted_rowset_meta.end_version(), 1);
    EXPECT_EQ(converted_rowset_meta.num_rows(), 100);
    EXPECT_EQ(converted_rowset_meta.total_disk_size(), 1024);
    EXPECT_TRUE(converted_rowset_meta.has_tablet_schema());

    // verify rowset schema
    const TabletSchemaPB& converted_rowset_schema = converted_rowset_meta.tablet_schema();
    EXPECT_EQ(converted_rowset_schema.num_short_key_columns(), 1);
    EXPECT_EQ(converted_rowset_schema.column_size(), 2);
    EXPECT_EQ(converted_rowset_schema.index_size(), 1);

    // verify rowset index
    const TabletIndexPB& converted_rowset_index = converted_rowset_schema.index(0);
    EXPECT_EQ(converted_rowset_index.index_id(), 2001);
    EXPECT_EQ(converted_rowset_index.index_name(), "test_index");
    EXPECT_EQ(converted_rowset_index.index_type(), IndexType::BITMAP);
    EXPECT_EQ(converted_rowset_index.col_unique_id_size(), 1);
    EXPECT_EQ(converted_rowset_index.col_unique_id(0), 0);
}
} // namespace doris
