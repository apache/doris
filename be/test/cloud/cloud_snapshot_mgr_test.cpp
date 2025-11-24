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

#include "cloud/cloud_storage_engine.h"
#include "io/fs/remote_file_system.h"

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

    RowsetMetaPB* rowset_meta = input_meta_pb.add_rs_metas();
    RowsetId rowset_id;
    rowset_id.init(10000);
    rowset_meta->set_rowset_id(0);
    rowset_meta->set_rowset_id_v2(rowset_id.to_string());
    rowset_meta->set_tablet_id(1000);
    rowset_meta->set_txn_id(2000);
    rowset_meta->set_num_segments(3);
    rowset_meta->set_num_rows(100);
    rowset_meta->set_start_version(100);
    rowset_meta->set_end_version(101);
    rowset_meta->set_total_disk_size(4096);
    rowset_meta->set_data_disk_size(2048);
    rowset_meta->set_index_disk_size(2048);
    rowset_meta->set_rowset_state(RowsetStatePB::VISIBLE);
    rowset_meta->set_newest_write_timestamp(1678901234567890);

    auto tablet_meta = std::make_shared<TabletMeta>();
    tablet_meta->init_from_pb(input_meta_pb);
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
    EXPECT_EQ(output_meta_pb.rs_metas(0).num_segments(), 3);
    EXPECT_EQ(output_meta_pb.rs_metas(0).num_rows(), 100);
    EXPECT_EQ(output_meta_pb.rs_metas(0).start_version(), 100);
    EXPECT_EQ(output_meta_pb.rs_metas(0).end_version(), 101);
    EXPECT_EQ(output_meta_pb.rs_metas(0).total_disk_size(), 4096);
    EXPECT_EQ(output_meta_pb.rs_metas(0).data_disk_size(), 2048);
    EXPECT_EQ(output_meta_pb.rs_metas(0).index_disk_size(), 2048);
    EXPECT_EQ(output_meta_pb.rs_metas(0).rowset_state(), RowsetStatePB::VISIBLE);
    EXPECT_EQ(output_meta_pb.rs_metas(0).resource_id(), storage_resource.fs->id());
    EXPECT_FALSE(file_mapping.empty());
    EXPECT_TRUE(status.ok());
}

} // namespace doris
