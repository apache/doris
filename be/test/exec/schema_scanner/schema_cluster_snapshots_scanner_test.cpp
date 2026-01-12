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

#include "exec/schema_scanner/schema_cluster_snapshots_scanner.h"

#include <gtest/gtest.h>

#include "vec/columns/column_string.h"
#include "vec/core/block.h"

namespace doris {

class SchemaClusterSnapshotsScannerTest : public testing::Test {
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SchemaClusterSnapshotsScannerTest, test_get_next_block_internal) {
    SchemaClusterSnapshotsScanner scanner;
    auto& snapshots = scanner._snapshots;
    {
        doris::cloud::SnapshotInfoPB snapshot;
        snapshots.push_back(snapshot);
    }
    {
        doris::cloud::SnapshotInfoPB snapshot;
        snapshot.set_snapshot_id("232ds");
        snapshot.set_ancestor_id("dnjg6-d");
        snapshot.set_create_at(1758095486);
        snapshot.set_finish_at(1758095486);
        snapshot.set_image_url("image_dadas1");
        snapshot.set_journal_id(21424);
        snapshot.set_status(cloud::SnapshotStatus::SNAPSHOT_PREPARE);
        snapshot.set_auto_snapshot(true);
        snapshot.set_ttl_seconds(3600);
        snapshot.set_snapshot_label("label");
        snapshot.set_reason("reason");
        snapshots.push_back(snapshot);
    }

    auto data_block = vectorized::Block::create_unique();
    scanner._init_block(data_block.get());

    auto st = scanner._fill_block_impl(data_block.get());
    ASSERT_EQ(Status::OK(), st);
    ASSERT_EQ(2, data_block->rows());

    auto col = data_block->safe_get_by_position(0);
    auto v = (*col.column)[1].get<vectorized::String>();
    EXPECT_EQ(v, "232ds");
}

} // namespace doris
