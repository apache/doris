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

#include "olap/memory/hash_index.h"

#include <gtest/gtest.h>

#include <vector>

#include "util/hash_util.hpp"

namespace doris {
namespace memory {

inline uint64_t HashCode(uint64_t v) {
    return HashUtil::fnv_hash64(&v, 8, 0);
}

TEST(HashIndex, findset) {
    size_t sz = 20;
    scoped_refptr<HashIndex> hi(new HashIndex(sz));
    std::vector<uint32_t> entries;
    entries.reserve(10);
    // add all
    for (size_t i = 0; i < sz * 2; i += 2) {
        uint64_t keyHash = HashCode(i);
        entries.clear();
        uint64_t slot = hi->find(keyHash, &entries);
        bool found = false;
        for (auto& e : entries) {
            uint64_t keyHashVerify = HashCode(e);
            if (keyHashVerify != keyHash) {
                continue;
            }
            if (e == i) {
                found = true;
                break;
            }
        }
        EXPECT_FALSE(found);
        EXPECT_NE(slot, HashIndex::npos);
        hi->set(slot, keyHash, i);
    }
    // search
    for (size_t i = 0; i < sz * 2; i += 2) {
        uint64_t keyHash = HashCode(i);
        entries.clear();
        hi->find(keyHash, &entries);
        uint64_t fslot = HashIndex::npos;
        for (auto& e : entries) {
            uint64_t keyHashVerify = HashCode(e);
            if (keyHashVerify != keyHash) {
                continue;
            }
            if (e == i) {
                fslot = e;
                break;
            }
        }
        EXPECT_NE(fslot, HashIndex::npos);
    }
    LOG(INFO) << hi->dump();
}

TEST(HashIndex, add) {
    srand(1);
    size_t N = 1000;
    scoped_refptr<HashIndex> hi(new HashIndex(N));
    std::vector<int64_t> keys(N);
    for (size_t i = 0; i < N; ++i) {
        keys[i] = rand();
        uint64_t hashcode = HashCode(keys[i]);
        EXPECT_TRUE(hi->add(hashcode, i));
    }
    std::vector<uint32_t> entries;
    entries.reserve(10);
    for (size_t i = 0; i < N; ++i) {
        uint64_t hashcode = HashCode(keys[i]);
        hi->find(hashcode, &entries);
        bool found = false;
        for (size_t ei = 0; ei < entries.size(); ++ei) {
            int64_t v = keys[entries[ei]];
            if (v == keys[i]) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    }
    LOG(INFO) << hi->dump();
}

} // namespace memory
} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
