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

#include <cstdint>

#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dict_format.h"

namespace doris::segment_v2::kuromoji {

static_assert(sizeof(KmjFileHeader) == 32, "KmjFileHeader must be 32 bytes");
static_assert(sizeof(WordEntry) == 12, "WordEntry must be 12 bytes");
static_assert(sizeof(WordIdRun) == 8, "WordIdRun must be 8 bytes");
static_assert(sizeof(CategoryDef) == 4, "CategoryDef must be 4 bytes");

TEST(KuromojiDictFormatTest, ConstantsAndCategories) {
    EXPECT_EQ(KMJ_FORMAT_VERSION, 1U);
    EXPECT_EQ(CAT_CLASS_COUNT, 12);
    EXPECT_EQ(KMJ_NO_FEATURE, 0xFFFFFFFFU);
}

TEST(KuromojiDictFormatTest, ConnectionCostIndexing) {
    // forward_size=2, backward_size=3 -> 6 cells, row-major by backward_id.
    int16_t cells[6] = {10, 11, 20, 21, 30, 31};
    // cell index = backward_id * forward_size + forward_id
    EXPECT_EQ(connection_cost(cells, 2, /*forward*/ 0, /*backward*/ 0), 10);
    EXPECT_EQ(connection_cost(cells, 2, /*forward*/ 1, /*backward*/ 0), 11);
    EXPECT_EQ(connection_cost(cells, 2, /*forward*/ 0, /*backward*/ 1), 20);
    EXPECT_EQ(connection_cost(cells, 2, /*forward*/ 0, /*backward*/ 2), 30);
    EXPECT_EQ(connection_cost(cells, 2, /*forward*/ 1, /*backward*/ 2), 31);
}

} // namespace doris::segment_v2::kuromoji
