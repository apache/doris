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
#include "vec/sink/delta_writer_v2_pool.h"

#include <gtest/gtest.h>

#include "olap/delta_writer_v2.h"

namespace doris::vectorized {

class DeltaWriterV2PoolTest : public testing::Test {
public:
    DeltaWriterV2PoolTest() = default;
    virtual ~DeltaWriterV2PoolTest() = default;
};

TEST_F(DeltaWriterV2PoolTest, test_pool) {
    DeltaWriterV2Pool pool;
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_hi(2);
    PUniqueId load_id2;
    load_id2.set_hi(1);
    load_id2.set_hi(3);
    auto map = pool.get_or_create(load_id);
    auto map2 = pool.get_or_create(load_id2);
    auto map3 = pool.get_or_create(load_id);
    EXPECT_EQ(2, pool.size());
    EXPECT_EQ(map, map3);
    EXPECT_NE(map, map2);
    EXPECT_TRUE(map->close().ok());
    EXPECT_TRUE(map2->close().ok());
    EXPECT_TRUE(map3->close().ok());
    EXPECT_EQ(0, pool.size());
}

TEST_F(DeltaWriterV2PoolTest, test_map) {
    DeltaWriterV2Pool pool;
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_hi(2);
    auto map = pool.get_or_create(load_id);
    EXPECT_EQ(1, pool.size());
    WriteRequest req;
    RuntimeState state;
    auto writer = map->get_or_create(
            100, [&req, &state]() { return DeltaWriterV2::open(&req, {}, &state); });
    auto writer2 = map->get_or_create(
            101, [&req, &state]() { return DeltaWriterV2::open(&req, {}, &state); });
    auto writer3 = map->get_or_create(
            100, [&req, &state]() { return DeltaWriterV2::open(&req, {}, &state); });
    EXPECT_EQ(2, map->size());
    EXPECT_EQ(writer, writer3);
    EXPECT_NE(writer, writer2);
    static_cast<void>(map->close());
    EXPECT_EQ(0, pool.size());
}

} // namespace doris::vectorized
