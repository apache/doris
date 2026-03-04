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

#include "exec/schema_scanner/schema_cluster_snapshot_properties_scanner.h"

#include <gtest/gtest.h>

#include "vec/core/block.h"
#include "vec/core/types.h"

namespace doris {

class SchemaClusterSnapshotPropertiesScannerTest : public testing::Test {
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SchemaClusterSnapshotPropertiesScannerTest, test_get_next_block_internal) {
    SchemaClusterSnapshotPropertiesScanner scanner;
    scanner._switch_status = cloud::SnapshotSwitchStatus::SNAPSHOT_SWITCH_ON;
    scanner._max_reserved_snapshots = 30;
    scanner._snapshot_interval_seconds = 3600;

    auto data_block = vectorized::Block::create_unique();
    scanner._init_block(data_block.get());

    auto st = scanner._fill_block_impl(data_block.get());
    ASSERT_EQ(Status::OK(), st);
    ASSERT_EQ(1, data_block->rows());

    auto col = data_block->safe_get_by_position(2);
    auto v = (*col.column)[0].get<TYPE_BIGINT>();
    EXPECT_EQ(v, 30);
}

} // namespace doris
