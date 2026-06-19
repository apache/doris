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

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "storage/index/inverted/analyzer/kuromoji/dict/darts.h"

namespace doris::segment_v2::kuromoji {

// Proves the vendored Darts-clone header builds + queries: surface forms ->
// non-negative values, then common-prefix search (the API the kuromoji lattice
// will use). "東"(E6 9D B1), "京"(E4 BA AC), "東京"(E6 9D B1 E4 BA AC).
TEST(DartsSmokeTest, BuildAndCommonPrefixSearch) {
    std::vector<std::string> keys = {"\xE4\xBA\xAC", "\xE6\x9D\xB1", "\xE6\x9D\xB1\xE4\xBA\xAC"};
    std::sort(keys.begin(), keys.end()); // Darts requires byte-sorted keys

    std::vector<const char*> kptrs;
    std::vector<std::size_t> klens;
    std::vector<int> values;
    for (std::size_t i = 0; i < keys.size(); ++i) {
        kptrs.push_back(keys[i].data());
        klens.push_back(keys[i].size());
        values.push_back(static_cast<int>(i)); // non-negative (MSB reserved by Darts)
    }

    Darts::DoubleArray da;
    ASSERT_EQ(0, da.build(keys.size(), kptrs.data(), klens.data(), values.data()));

    // exact match for "東京"
    const std::string tokyo = "\xE6\x9D\xB1\xE4\xBA\xAC";
    EXPECT_GE(da.exactMatchSearch<int>(tokyo.data(), tokyo.size()), 0);

    // common-prefix search over "東京" must yield "東" (len 3) and "東京" (len 6)
    Darts::DoubleArray::result_pair_type results[8];
    std::size_t n = da.commonPrefixSearch(tokyo.data(), results, 8, tokyo.size());
    ASSERT_GE(n, 2U);
    bool saw_len3 = false;
    bool saw_len6 = false;
    for (std::size_t i = 0; i < n && i < 8; ++i) {
        if (results[i].length == 3) {
            saw_len3 = true;
        }
        if (results[i].length == 6) {
            saw_len6 = true;
        }
    }
    EXPECT_TRUE(saw_len3);
    EXPECT_TRUE(saw_len6);
}

} // namespace doris::segment_v2::kuromoji
