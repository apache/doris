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
    PartialRowBatch rb(&sc);
    PartialRowWriter writer(*sc.get());
    srand(1);
    const int N = 1000;
    size_t nrow = 0;
    // add insert/update operation
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
        EXPECT_TRUE(writer.write_row_to_batch(&rb).ok());
    }
    EXPECT_EQ(rb.row_size(), nrow);

    // read from rowbatch and check equality
    PartialRowReader reader(rb);
    srand(1);
    for (size_t i = 0; i < reader.size(); i++) {
        EXPECT_TRUE(reader.read(i).ok());
        if (i % 3 == 0) {
            EXPECT_EQ(reader.cell_size(), 4);
        } else {
            EXPECT_EQ(reader.cell_size(), 1);
        }
        int id = i;
        int uv = rand() % 10000;
        int pv = rand() % 10000;
        int8_t city = rand() % 100;

        const ColumnSchema* cs = nullptr;
        const void* data = nullptr;

        EXPECT_TRUE(reader.get_cell(0, &cs, &data).ok());
        EXPECT_EQ(cs->cid(), 1);
        EXPECT_EQ(*(int32_t*)data, id);

        if (i % 3 == 0) {
            EXPECT_TRUE(reader.get_cell(1, &cs, &data).ok());
            EXPECT_EQ(cs->cid(), 2);
            EXPECT_EQ(*(int32_t*)data, uv);

            EXPECT_TRUE(reader.get_cell(2, &cs, &data).ok());
            EXPECT_EQ(cs->cid(), 3);
            EXPECT_EQ(*(int32_t*)data, pv);

            EXPECT_TRUE(reader.get_cell(3, &cs, &data).ok());
            EXPECT_EQ(cs->cid(), 4);
            if (city % 2 == 0) {
                EXPECT_EQ(data, nullptr);
            } else {
                EXPECT_EQ(*(int8_t*)data, city);
            }
        }
    }
}

} // namespace memory
} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
