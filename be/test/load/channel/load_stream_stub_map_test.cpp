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
#include <gtest/gtest.h>

#include "exec/sink/load_stream_map_pool.h"
#include "exec/sink/load_stream_stub.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class LoadStreamMapPoolTest : public testing::Test {
public:
    LoadStreamMapPoolTest() = default;
    virtual ~LoadStreamMapPoolTest() = default;
};

TEST_F(LoadStreamMapPoolTest, test) {
    LoadStreamMapPool pool;
    int64_t src_id = 100;
    PUniqueId load_id;
    load_id.set_lo(1);
    load_id.set_hi(2);
    PUniqueId load_id2;
    load_id2.set_lo(2);
    load_id2.set_hi(1);
    auto streams_for_node1 = pool.get_or_create(load_id, src_id, 5, 2);
    auto streams_for_node2 = pool.get_or_create(load_id, src_id, 5, 2);
    EXPECT_EQ(1, pool.size());
    auto streams_for_node3 = pool.get_or_create(load_id2, src_id, 8, 1);
    EXPECT_EQ(2, pool.size());
    EXPECT_EQ(streams_for_node1, streams_for_node2);
    EXPECT_NE(streams_for_node1, streams_for_node3);

    EXPECT_EQ(5, streams_for_node1->get_or_create(101)->size());
    EXPECT_EQ(5, streams_for_node2->get_or_create(102)->size());
    EXPECT_EQ(8, streams_for_node3->get_or_create(101)->size());

    EXPECT_TRUE(streams_for_node3->release());
    EXPECT_EQ(1, pool.size());
    EXPECT_FALSE(streams_for_node1->release());
    EXPECT_EQ(1, pool.size());
    EXPECT_TRUE(streams_for_node2->release());
    EXPECT_EQ(0, pool.size());
}

TEST_F(LoadStreamMapPoolTest, schema_is_keyed_by_partition_and_index) {
    constexpr int64_t index_id = 100;
    constexpr int64_t v2_partition_id = 101;
    constexpr int64_t v3_partition_id = 102;
    PUniqueId load_id;
    auto schemas = std::make_shared<PartitionIndexToTabletSchema>();
    auto v2_schema = std::make_shared<TabletSchema>();
    auto v3_schema = std::make_shared<TabletSchema>();
    schemas->emplace(PartitionIndexId {v2_partition_id, index_id}, v2_schema);
    schemas->emplace(PartitionIndexId {v3_partition_id, index_id}, v3_schema);

    LoadStreamStub stub(load_id, 1, schemas, std::make_shared<IndexToEnableMoW>());
    EXPECT_EQ(v2_schema, stub.tablet_schema(v2_partition_id, index_id));
    EXPECT_EQ(v3_schema, stub.tablet_schema(v3_partition_id, index_id));
}

} // namespace doris
