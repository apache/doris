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

#include "olap/memory/partial_row_batch.h"

#include <gtest/gtest.h>

#include <vector>

#include "util/hash_util.hpp"

namespace doris {
namespace memory {

TEST(PartialRowbatch, write) {
    scoped_refptr<Schema> sc;
    ASSERT_TRUE(Schema::create("id int,uv int,pv int,city tinyint null", &sc).ok());
    PartialRowWriter writer(sc);
    srand(1);
    const int N = 1000;
    size_t nrow = 0;
    // add insert/update operation
    EXPECT_TRUE(writer.start_batch().ok());
    for (int i = 0; i < N; i++) {
        nrow++;
        writer.start_row();
        int id = i;
        int uv = rand() % 10000;
        int pv = rand() % 10000;
        int8_t city = rand() % 100;
        EXPECT_TRUE(writer.set("id", &id).ok());
        if (i % 3 == 0) {
            EXPECT_TRUE(writer.set("uv", &uv).ok());
            EXPECT_TRUE(writer.set("pv", &pv).ok());
            EXPECT_TRUE(writer.set("city", city % 2 == 0 ? nullptr : &city).ok());
        }
        EXPECT_TRUE(writer.end_row().ok());
    }
    std::vector<uint8_t> buffer;
    writer.finish_batch(&buffer);

    PartialRowBatch rb(&sc);
    EXPECT_TRUE(rb.load(std::move(buffer)).ok());
    EXPECT_EQ(rb.row_size(), nrow);
    // read from rowbatch and check equality
    srand(1);
    for (size_t i = 0; i < nrow; i++) {
        bool has_row = false;
        EXPECT_TRUE(rb.next_row(&has_row).ok());
        EXPECT_TRUE(has_row);
        if (i % 3 == 0) {
            EXPECT_EQ(rb.cur_row_cell_size(), 4);
        } else {
            EXPECT_EQ(rb.cur_row_cell_size(), 1);
        }
        int id = i;
        int uv = rand() % 10000;
        int pv = rand() % 10000;
        int8_t city = rand() % 100;

        const ColumnSchema* cs = nullptr;
        const void* data = nullptr;

        EXPECT_TRUE(rb.cur_row_get_cell(0, &cs, &data).ok());
        EXPECT_EQ(cs->cid(), 1);
        EXPECT_EQ(*(int32_t*)data, id);

        if (i % 3 == 0) {
            EXPECT_TRUE(rb.cur_row_get_cell(1, &cs, &data).ok());
            EXPECT_EQ(cs->cid(), 2);
            EXPECT_EQ(*(int32_t*)data, uv);

            EXPECT_TRUE(rb.cur_row_get_cell(2, &cs, &data).ok());
            EXPECT_EQ(cs->cid(), 3);
            EXPECT_EQ(*(int32_t*)data, pv);

            EXPECT_TRUE(rb.cur_row_get_cell(3, &cs, &data).ok());
            EXPECT_EQ(cs->cid(), 4);
            if (city % 2 == 0) {
                EXPECT_EQ(data, nullptr);
            } else {
                EXPECT_EQ(*(int8_t*)data, city);
            }
        }
    }
    bool has_row = false;
    EXPECT_TRUE(rb.next_row(&has_row).ok());
    EXPECT_FALSE(has_row);
}

} // namespace memory
} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
