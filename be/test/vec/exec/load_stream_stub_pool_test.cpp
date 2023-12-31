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
#include "vec/sink/load_stream_stub_pool.h"

#include <gtest/gtest.h>

#include "vec/sink/load_stream_stub.h"

namespace doris {

class LoadStreamStubPoolTest : public testing::Test {
public:
    LoadStreamStubPoolTest() = default;
    virtual ~LoadStreamStubPoolTest() = default;
};

TEST_F(LoadStreamStubPoolTest, test) {
    LoadStreamStubPool pool;
    RuntimeState state;
    int64_t src_id = 100;
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_hi(2);
    auto streams1 = pool.get_or_create(load_id, src_id, 101, 5, 1, &state);
    auto streams2 = pool.get_or_create(load_id, src_id, 102, 5, 1, &state);
    auto streams3 = pool.get_or_create(load_id, src_id, 101, 5, 1, &state);
    EXPECT_EQ(2, pool.size());
    EXPECT_EQ(1, pool.templates_size());
    EXPECT_EQ(streams1, streams3);
    EXPECT_NE(streams1, streams2);
    streams1->release();
    streams2->release();
    streams3->release();
    EXPECT_EQ(0, pool.size());
    EXPECT_EQ(0, pool.templates_size());
}

} // namespace doris
