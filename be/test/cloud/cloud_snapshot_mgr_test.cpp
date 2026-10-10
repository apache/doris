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

#include "cloud/cloud_snapshot_mgr.h"

#include <gtest/gtest.h>

#include <optional>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "io/fs/remote_file_system.h"
#include "io/fs/s3_file_system.h"
#include "util/s3_util.h"

namespace doris {
using namespace cloud;
using namespace std::chrono;

S3Conf create_test_s3_conf() {
    S3Conf s3_conf;
    s3_conf.bucket = "test-bucket";
    s3_conf.prefix = "test-prefix";
    s3_conf.client_conf.endpoint = "test-endpoint";
    s3_conf.client_conf.region = "test-region";
    s3_conf.client_conf.ak = "test-ak";
    s3_conf.client_conf.sk = "test-sk";
    return s3_conf;
}

class CloudSnapshotMgrTest : public testing::Test {
protected:
    void SetUp() override {
        _fs = io::S3FileSystem::create(create_test_s3_conf(), io::FileSystem::TMP_FS_ID).value();
        _engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
        _snapshot_mgr = std::make_unique<CloudSnapshotMgr>(*_engine);
    }

    void TearDown() override {}

protected:
    io::RemoteFileSystemSPtr _fs;

public:
    std::unique_ptr<CloudStorageEngine> _engine;
    std::unique_ptr<CloudSnapshotMgr> _snapshot_mgr;
};

TEST_F(CloudSnapshotMgrTest, TestConvertRowsets) {
    TabletMetaPB input_meta_pb;
    input_meta_pb.set_tablet_id(1000);
    input_meta_pb.set_schema_hash(123456);
    *input_meta_pb.mutable_tablet_uid() = TabletUid::gen_uid().to_proto();

    TabletSchemaPB* input_schema = input_meta_pb.mutable_schema();
    input_schema->set_keys_type(KeysType::DUP_KEYS);
    input_schema->set_num_short_key_columns(1);
    input_schema->set_num_rows_per_row_block(1024);
    input_schema->set_compress_kind(COMPRESS_LZ4);

    ColumnPB* col1 = input_schema->add_column();
    col1->set_unique_id(1);
    col1->set_name("col1");
    col1->set_type("INT");
    col1->set_is_key(true);
    col1->set_aggregation("NONE");

    ColumnPB* col2 = input_schema->add_column();
    col2->set_unique_id(2);
    col2->set_name("col2");
    col2->set_type("VARCHAR");
    col2->set_is_key(false);
    col2->set_aggregation("REPLACE");

    doris::TabletIndexPB* index1 = input_schema->add_index();
    index1->set_index_id(1001);
    index1->set_index_name("test_index1");
    index1->set_index_type(IndexType::BITMAP);
    index1->add_col_unique_id(2);

    doris::TabletIndexPB* index2 = input_schema->add_index();
    index2->set_index_id(1002);
    index2->set_index_name("test_index2");
    index2->set_index_type(IndexType::INVERTED);
    index2->add_col_unique_id(2);

    RowsetMetaPB* rowset_meta = input_meta_pb.add_rs_metas();
    RowsetId rowset_id;
    rowset_id.init(10000);
    rowset_meta->set_rowset_id(0);
    rowset_meta->set_rowset_id_v2(rowset_id.to_string());
    rowset_meta->set_tablet_id(1000);
    rowset_meta->set_txn_id(2000);
    rowset_meta->set_num_segments(3);
    rowset_meta->set_segments_overlap_pb(NONOVERLAPPING_WITHIN_GROUP);
    rowset_meta->add_segment_group_sizes(1);
    rowset_meta->add_segment_group_sizes(2);
    rowset_meta->set_num_rows(100);
    rowset_meta->set_start_version(100);
    rowset_meta->set_end_version(101);
    rowset_meta->set_total_disk_size(4096);
    rowset_meta->set_data_disk_size(2048);
    rowset_meta->set_index_disk_size(2048);
    rowset_meta->set_rowset_state(RowsetStatePB::VISIBLE);
    rowset_meta->set_newest_write_timestamp(1678901234567890);
    // Publish-time metadata that the rowset writer context does not carry.
    rowset_meta->mutable_commit_tso()->set_start_tso(466872251335573505L);
    rowset_meta->mutable_commit_tso()->set_end_tso(466872251335573505L);

    TabletSchemaPB* rowset_schema = rowset_meta->mutable_tablet_schema();
    rowset_schema->CopyFrom(*input_schema);

    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->init_from_pb(input_meta_pb);

    TabletSchemaSPtr local_tablet_schema = std::make_shared<TabletSchema>();
    local_tablet_schema->init_from_pb(input_meta_pb.schema());

    // reset the index id
    input_schema->mutable_index(0)->set_index_id(2001);
    input_schema->mutable_index(1)->set_index_id(2002);
    rowset_meta->mutable_tablet_schema()->mutable_index(0)->set_index_id(2001);
    rowset_meta->mutable_tablet_schema()->mutable_index(1)->set_index_id(2002);

    CloudTabletSPtr target_tablet = std::make_shared<CloudTablet>(*_engine, tablet_meta);
    StorageResource storage_resource {_fs};
    std::unordered_map<std::string, std::string> file_mapping;

    TabletMetaPB output_meta_pb;
    Status status = _snapshot_mgr->convert_rowsets(&output_meta_pb, input_meta_pb, 3000,
                                                   target_tablet, storage_resource, file_mapping);
    EXPECT_EQ(output_meta_pb.tablet_id(), 3000);
    EXPECT_EQ(output_meta_pb.rs_metas_size(), 1);
    EXPECT_EQ(output_meta_pb.rs_metas(0).tablet_id(), 3000);
    EXPECT_EQ(output_meta_pb.rs_metas(0).rowset_id(), 0);
    EXPECT_NE(output_meta_pb.rs_metas(0).rowset_id_v2(), rowset_id.to_string());
    EXPECT_TRUE(output_meta_pb.has_schema());
    EXPECT_EQ(output_meta_pb.schema().index_size(), 2);
    EXPECT_EQ(output_meta_pb.schema().index(0).index_id(), 1001);
    EXPECT_EQ(output_meta_pb.schema().index(0).index_name(), "test_index1");
    EXPECT_EQ(output_meta_pb.schema().index(1).index_id(), 1002);
    EXPECT_EQ(output_meta_pb.schema().index(1).index_name(), "test_index2");
    EXPECT_TRUE(output_meta_pb.rs_metas(0).has_tablet_schema());
    EXPECT_EQ(output_meta_pb.rs_metas(0).tablet_schema().index_size(), 2);
    EXPECT_EQ(output_meta_pb.rs_metas(0).tablet_schema().index(0).index_id(), 1001);
    EXPECT_EQ(output_meta_pb.rs_metas(0).tablet_schema().index(1).index_id(), 1002);
    EXPECT_EQ(output_meta_pb.rs_metas(0).num_segments(), 3);
    EXPECT_EQ(output_meta_pb.rs_metas(0).segments_overlap_pb(), NONOVERLAPPING_WITHIN_GROUP);
    ASSERT_EQ(output_meta_pb.rs_metas(0).segment_group_sizes_size(), 2);
    EXPECT_EQ(output_meta_pb.rs_metas(0).segment_group_sizes(0), 1);
    EXPECT_EQ(output_meta_pb.rs_metas(0).segment_group_sizes(1), 2);
    EXPECT_EQ(output_meta_pb.rs_metas(0).num_rows(), 100);
    EXPECT_EQ(output_meta_pb.rs_metas(0).start_version(), 100);
    EXPECT_EQ(output_meta_pb.rs_metas(0).end_version(), 101);
    EXPECT_EQ(output_meta_pb.rs_metas(0).total_disk_size(), 4096);
    EXPECT_EQ(output_meta_pb.rs_metas(0).data_disk_size(), 2048);
    EXPECT_EQ(output_meta_pb.rs_metas(0).index_disk_size(), 2048);
    EXPECT_EQ(output_meta_pb.rs_metas(0).rowset_state(), RowsetStatePB::VISIBLE);
    EXPECT_EQ(output_meta_pb.rs_metas(0).resource_id(), storage_resource.fs->id());
    ASSERT_TRUE(output_meta_pb.rs_metas(0).has_commit_tso());
    EXPECT_EQ(output_meta_pb.rs_metas(0).commit_tso().end_tso(), 466872251335573505L);
    EXPECT_FALSE(file_mapping.empty());
    EXPECT_TRUE(status.ok());
}

TEST_F(CloudSnapshotMgrTest, ConvertRowsetsPreservesCommitTso) {
    const std::vector<std::optional<TsoRange>> cases = {std::nullopt, TsoRange(-1, -1),
                                                        TsoRange(100, 100), TsoRange(100, 200)};
    for (size_t i = 0; i < cases.size(); ++i) {
        SCOPED_TRACE(i);
        TabletMetaPB input;
        input.set_tablet_id(1000);
        input.set_schema_hash(123456);
        *input.mutable_tablet_uid() = TabletUid::gen_uid().to_proto();
        auto* schema = input.mutable_schema();
        schema->set_keys_type(DUP_KEYS);
        schema->set_num_short_key_columns(1);
        schema->set_num_rows_per_row_block(1024);
        auto* key = schema->add_column();
        key->set_unique_id(1);
        key->set_name("k1");
        key->set_type("INT");
        key->set_is_key(true);
        key->set_aggregation("NONE");
        auto* source = input.add_rs_metas();
        source->set_rowset_id(0);
        source->set_rowset_id_v2(_engine->next_rowset_id().to_string());
        source->set_tablet_id(1000);
        source->set_rowset_type(BETA_ROWSET);
        source->set_rowset_state(VISIBLE);
        source->set_start_version(7);
        source->set_end_version(
                cases[i].has_value() && cases[i]->start_tso() != cases[i]->end_tso() ? 8 : 7);
        source->set_num_segments(1);
        source->add_segment_ids(7);
        source->set_num_rows(10);
        source->set_newest_write_timestamp(1000);
        source->mutable_tablet_schema()->CopyFrom(*schema);
        if (cases[i].has_value()) {
            source->mutable_commit_tso()->set_start_tso(cases[i]->start_tso());
            source->mutable_commit_tso()->set_end_tso(cases[i]->end_tso());
        }
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->init_from_pb(input);
        auto target = std::make_shared<CloudTablet>(*_engine, tablet_meta);
        StorageResource resource {_fs};
        std::unordered_map<std::string, std::string> file_mapping;
        TabletMetaPB output;
        auto st = _snapshot_mgr->convert_rowsets(&output, input, 3000, target, resource,
                                                 file_mapping);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(1, output.rs_metas_size());
        const auto& converted = output.rs_metas(0);
        EXPECT_NE(source->rowset_id_v2(), converted.rowset_id_v2());
        EXPECT_EQ(source->has_commit_tso(), converted.has_commit_tso());
        EXPECT_EQ(source->commit_tso().SerializeAsString(),
                  converted.commit_tso().SerializeAsString());
        EXPECT_EQ(source->start_version(), converted.start_version());
        EXPECT_EQ(source->end_version(), converted.end_version());
    }
}

TEST_F(CloudSnapshotMgrTest, TestRenameIndexIds) {
    TabletSchemaPB source_schema_pb;
    source_schema_pb.set_keys_type(KeysType::DUP_KEYS);
    source_schema_pb.set_num_short_key_columns(1);
    source_schema_pb.set_num_rows_per_row_block(1024);
    source_schema_pb.set_compress_kind(COMPRESS_LZ4);

    ColumnPB* col1 = source_schema_pb.add_column();
    col1->set_unique_id(1);
    col1->set_name("col1");
    col1->set_type("INT");
    col1->set_is_key(true);
    col1->set_aggregation("NONE");

    ColumnPB* col2 = source_schema_pb.add_column();
    col2->set_unique_id(2);
    col2->set_name("col2");
    col2->set_type("VARCHAR");
    col2->set_is_key(false);
    col2->set_aggregation("REPLACE");

    doris::TabletIndexPB* index1 = source_schema_pb.add_index();
    index1->set_index_id(1001);
    index1->set_index_name("test_index1");
    index1->set_index_type(IndexType::BITMAP);
    index1->add_col_unique_id(2);

    doris::TabletIndexPB* index2 = source_schema_pb.add_index();
    index2->set_index_id(1002);
    index2->set_index_name("test_index2");
    index2->set_index_type(IndexType::INVERTED);
    index2->add_col_unique_id(2);

    TabletSchemaSPtr local_tablet_schema = std::make_shared<TabletSchema>();
    local_tablet_schema->init_from_pb(source_schema_pb);

    // reset the index id
    source_schema_pb.mutable_index(0)->set_index_id(2001);
    source_schema_pb.mutable_index(1)->set_index_id(2002);

    Status status = _snapshot_mgr->_rename_index_ids(source_schema_pb, local_tablet_schema);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(source_schema_pb.index_size(), 2);
    EXPECT_EQ(source_schema_pb.index(0).index_id(), 1001);
    EXPECT_EQ(source_schema_pb.index(0).index_name(), "test_index1");
    EXPECT_EQ(source_schema_pb.index(0).index_type(), IndexType::BITMAP);
    EXPECT_EQ(source_schema_pb.index(1).index_id(), 1002);
    EXPECT_EQ(source_schema_pb.index(1).index_name(), "test_index2");
    EXPECT_EQ(source_schema_pb.index(1).index_type(), IndexType::INVERTED);
}

} // namespace doris
