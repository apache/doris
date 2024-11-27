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

#include "vec/sink/load_stream_map_pool.h"
#include "vec/sink/load_stream_stub.h"

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

} // namespace doris
