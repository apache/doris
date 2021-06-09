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

#include "olap/memory/column_delta.h"

#include <gtest/gtest.h>

#include <map>
#include <vector>

#include "olap/memory/column.h"
#include "test_util/test_util.h"

namespace doris {
namespace memory {

TEST(ColumnDelta, Index) {
    const int BaseSize = LOOP_LESS_OR_MORE(2560, 256001);
    const int NumUpdate = LOOP_LESS_OR_MORE(100, 10000);
    srand(1);
    scoped_refptr<ColumnDelta> delta(new ColumnDelta());
    std::map<uint32_t, uint32_t> updates;
    for (int i = 0; i < NumUpdate; i++) {
        uint32_t idx = rand() % BaseSize;
        updates[idx] = rand();
    }
    size_t nblock = num_block(BaseSize, Column::BLOCK_SIZE);
    ASSERT_TRUE(delta->alloc(nblock, updates.size(), sizeof(uint32_t), false).ok());
    DeltaIndex* index = delta->index();
    std::vector<uint32_t>& block_ends = index->block_ends();
    Buffer& idxdata = index->_data;
    Buffer& data = delta->data();
    uint32_t cidx = 0;
    uint32_t curbid = 0;
    for (auto& e : updates) {
        uint32_t rid = e.first;
        uint32_t bid = rid >> 16;
        while (curbid < bid) {
            block_ends[curbid] = cidx;
            curbid++;
        }
        idxdata.as<uint16_t>()[cidx] = rid & 0xffff;
        data.as<uint32_t>()[cidx] = e.second;
        cidx++;
    }
    while (curbid < nblock) {
        block_ends[curbid] = cidx;
        curbid++;
    }
    for (int i = 0; i < BaseSize; i++) {
        uint32_t idx = delta->find_idx(i);
        auto itr = updates.find(i);
        if (itr == updates.end()) {
            EXPECT_TRUE(idx == DeltaIndex::npos);
        } else {
            uint32_t v = delta->data().as<uint32_t>()[idx];
            EXPECT_EQ(v, itr->second);
        }
    }
}

} // namespace memory
} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
