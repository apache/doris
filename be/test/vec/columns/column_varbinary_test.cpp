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

#include "vec/columns/column_varbinary.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/common/string_view.h"
#include "vec/core/types.h"

namespace doris::vectorized {

class ColumnVarbinaryTest : public ::testing::Test {
protected:
    void SetUp() override {}

    void TearDown() override {}

    static std::string make_bytes(size_t n, uint8_t seed = 0xAB) {
        std::string s;
        s.resize(n);
        for (size_t i = 0; i < n; ++i) {
            s[i] = static_cast<char>(seed + i);
        }
        std::cout << "seed: " << seed << " s: " << s << std::endl;
        if (n >= 3) {
            s[n / 3] = '\0';
            s[(2 * n) / 3] = '\0';
        }
        std::cout << "make_bytes: N: " << n << " , " << s.size() << ", " << s << std::endl;
        return s;
    }
};

TEST_F(ColumnVarbinaryTest, BasicInsertGetPopClear) {
    auto col = ColumnVarbinary::create();

    EXPECT_EQ(col->get_name(), std::string("ColumnVarbinary"));
    EXPECT_EQ(col->size(), 0U);

    const size_t inline_len = std::min<size_t>(doris::StringView::kInlineSize, 8);
    const std::string small = make_bytes(inline_len, 0x11);
    const std::string big = make_bytes(doris::StringView::kInlineSize + 32, 0x22);

    size_t before_bytes = col->byte_size();

    col->insert_data(small.data(), small.size());
    EXPECT_EQ(col->size(), 1U);
    auto r0 = col->get_data_at(0);
    ASSERT_EQ(r0.size, small.size());
    ASSERT_EQ(r0.size, 8U);
    ASSERT_EQ(memcmp(r0.data, small.data(), small.size()), 0);

    size_t after_small_bytes = col->byte_size();
    ASSERT_EQ(after_small_bytes - before_bytes, sizeof(doris::StringView));
    ASSERT_EQ(after_small_bytes - before_bytes, 16);

    col->insert_default();
    EXPECT_EQ(col->size(), 2U);
    auto r1 = col->get_data_at(1);
    ASSERT_EQ(r1.size, 0U);

    col->insert_data(big.data(), big.size());
    EXPECT_EQ(col->size(), 3U);
    auto r2 = col->get_data_at(2);
    ASSERT_EQ(r2.size, big.size());
    ASSERT_EQ(r2.size, 44U);
    ASSERT_EQ(memcmp(r2.data, big.data(), big.size()), 0);

    size_t after_big_bytes = col->byte_size();
    // big insert adds one StringView slot + big payload in arena (Arena may add alignment/overhead)
    size_t diff = after_big_bytes - after_small_bytes;
    std::cout << "after_big_bytes: " << after_big_bytes
              << " after_small_bytes: " << after_small_bytes << " diff: " << diff << std::endl;
    ASSERT_GE(diff, sizeof(doris::StringView) + big.size());

    // pop_back
    col->pop_back(1);
    EXPECT_EQ(col->size(), 2U);
    auto r_back = col->get_data_at(1);
    ASSERT_EQ(r_back.size, 0U);

    // clear resets sizes (arena used_size becomes 0)
    col->clear();
    EXPECT_EQ(col->size(), 0U);
    EXPECT_EQ(col->byte_size(), 0U);
}

TEST_F(ColumnVarbinaryTest, InsertFromAndRanges) {
    auto src = ColumnVarbinary::create();
    std::vector<std::string> vals = {make_bytes(1, 0x01), make_bytes(2, 0x02),
                                     make_bytes(doris::StringView::kInlineSize + 5, 0x03),
                                     make_bytes(0, 0x00), make_bytes(7, 0x05)};
    for (auto& v : vals) {
        src->insert_data(v.data(), v.size());
    }

    // insert_from single rows
    auto dst1 = ColumnVarbinary::create();
    for (size_t i = 0; i < vals.size(); ++i) {
        dst1->insert_from(*src, i);
    }
    ASSERT_EQ(dst1->size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        auto r = dst1->get_data_at(i);
        ASSERT_EQ(r.size, vals[i].size());
        ASSERT_EQ(memcmp(r.data, vals[i].data(), r.size), 0);
    }

    // insert_range_from subset
    auto dst2 = ColumnVarbinary::create();
    dst2->insert_range_from(*src, 1, 3); // expect indices 1,2,3
    ASSERT_EQ(dst2->size(), 3U);
    for (size_t i = 0; i < 3; ++i) {
        auto r = dst2->get_data_at(i);
        ASSERT_EQ(r.size, vals[1 + i].size());
        ASSERT_EQ(memcmp(r.data, vals[1 + i].data(), r.size), 0);
    }

    // insert_indices_from with duplicates and reordering
    std::vector<uint32_t> indices = {4, 2, 2, 0};
    auto dst3 = ColumnVarbinary::create();
    dst3->insert_indices_from(*src, indices.data(), indices.data() + indices.size());
    ASSERT_EQ(dst3->size(), indices.size());
    for (size_t i = 0; i < indices.size(); ++i) {
        auto r = dst3->get_data_at(i);
        const auto& expect = vals[indices[i]];
        ASSERT_EQ(r.size, expect.size());
        ASSERT_EQ(memcmp(r.data, expect.data(), r.size), 0);
    }
}

TEST_F(ColumnVarbinaryTest, FilterBothModes) {
    auto col = ColumnVarbinary::create();
    std::vector<std::string> vals = {make_bytes(1, 0x10), make_bytes(2, 0x11), make_bytes(3, 0x12),
                                     make_bytes(0, 0x00), make_bytes(8, 0x13), make_bytes(5, 0x14)};
    for (auto& v : vals) {
        col->insert_data(v.data(), v.size());
    }

    IColumn::Filter f = {1, 0, 1, 1, 0, 1};
    size_t expected = 4; // number of ones

    // const variant (produces new column)
    const auto& ccol = assert_cast<const ColumnVarbinary&>(*col);
    ColumnPtr filtered = ccol.filter(f, -1);
    const auto& fcol = assert_cast<const ColumnVarbinary&>(*filtered);
    ASSERT_EQ(fcol.size(), expected);
    std::vector<size_t> kept_idx = {0, 2, 3, 5};
    for (size_t i = 0; i < kept_idx.size(); ++i) {
        auto r = fcol.get_data_at(i);
        const auto& expect = vals[kept_idx[i]];
        ASSERT_EQ(r.size, expect.size());
        ASSERT_EQ(memcmp(r.data, expect.data(), r.size), 0);
    }

    // in-place variant
    auto col_inplace = ColumnVarbinary::create();
    for (auto& v : vals) {
        col_inplace->insert_data(v.data(), v.size());
    }
    size_t new_sz = col_inplace->filter(f);
    ASSERT_EQ(new_sz, expected);
    ASSERT_EQ(col_inplace->size(), expected);
    for (size_t i = 0; i < kept_idx.size(); ++i) {
        auto r = col_inplace->get_data_at(i);
        const auto& expect = vals[kept_idx[i]];
        ASSERT_EQ(r.size, expect.size());
        ASSERT_EQ(memcmp(r.data, expect.data(), r.size), 0);
    }
}

TEST_F(ColumnVarbinaryTest, Permute) {
    auto col = ColumnVarbinary::create();
    std::vector<std::string> vals = {make_bytes(1, 0x20), make_bytes(2, 0x21), make_bytes(3, 0x22),
                                     make_bytes(4, 0x23)};
    for (auto& v : vals) {
        col->insert_data(v.data(), v.size());
    }

    IColumn::Permutation perm = {3, 1, 2, 0};

    // limit < size
    ColumnPtr p1 = col->permute(perm, 3);
    const auto& c1 = assert_cast<const ColumnVarbinary&>(*p1);
    ASSERT_EQ(c1.size(), 3U);
    for (size_t i = 0; i < 3; ++i) {
        auto r = c1.get_data_at(i);
        const auto& expect = vals[perm[i]];
        ASSERT_EQ(r.size, expect.size());
        ASSERT_EQ(memcmp(r.data, expect.data(), r.size), 0);
    }

    // limit == 0 means full or follow implementation? Use full size to be explicit
    ColumnPtr p2 = col->permute(perm, vals.size());
    const auto& c2 = assert_cast<const ColumnVarbinary&>(*p2);
    ASSERT_EQ(c2.size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        auto r = c2.get_data_at(i);
        const auto& expect = vals[perm[i]];
        ASSERT_EQ(r.size, expect.size());
        ASSERT_EQ(memcmp(r.data, expect.data(), r.size), 0);
    }
}

TEST_F(ColumnVarbinaryTest, CloneResized) {
    auto col = ColumnVarbinary::create();
    std::vector<std::string> vals = {make_bytes(1, 0x30), make_bytes(0, 0x00),
                                     make_bytes(doris::StringView::kInlineSize + 1, 0x31)};
    for (auto& v : vals) {
        col->insert_data(v.data(), v.size());
    }

    // enlarge
    auto c2 = col->clone_resized(5);
    const auto& cc2 = assert_cast<const ColumnVarbinary&>(*c2);
    ASSERT_EQ(cc2.size(), 5U);
    for (size_t i = 0; i < vals.size(); ++i) {
        auto r = cc2.get_data_at(i);
        ASSERT_EQ(r.size, vals[i].size());
        ASSERT_EQ(memcmp(r.data, vals[i].data(), r.size), 0);
    }
    for (size_t i = vals.size(); i < 5; ++i) {
        auto r = cc2.get_data_at(i);
        ASSERT_EQ(r.size, 0U); // default rows
    }

    // shrink
    auto c3 = col->clone_resized(2);
    const auto& cc3 = assert_cast<const ColumnVarbinary&>(*c3);
    ASSERT_EQ(cc3.size(), 2U);
    for (size_t i = 0; i < 2; ++i) {
        auto r = cc3.get_data_at(i);
        ASSERT_EQ(r.size, vals[i].size());
        ASSERT_EQ(memcmp(r.data, vals[i].data(), r.size), 0);
    }
}

TEST_F(ColumnVarbinaryTest, ReplaceColumnData) {
    auto col = ColumnVarbinary::create();
    std::vector<std::string> vals = {make_bytes(2, 0x40), make_bytes(3, 0x41), make_bytes(4, 0x42)};
    for (auto& v : vals) {
        col->insert_data(v.data(), v.size());
    }

    auto rhs = ColumnVarbinary::create();
    std::vector<std::string> rhs_vals = {make_bytes(5, 0x50), make_bytes(1, 0x51)};
    for (auto& v : rhs_vals) {
        rhs->insert_data(v.data(), v.size());
    }

    // replace row 0 with rhs[1]
    col->replace_column_data(*rhs, /*row=*/1, /*self_row=*/0);
    auto r0 = col->get_data_at(0);
    ASSERT_EQ(r0.size, rhs_vals[1].size());
    ASSERT_EQ(memcmp(r0.data, rhs_vals[1].data(), r0.size), 0);

    // replace row 2 with rhs[0]
    col->replace_column_data(*rhs, /*row=*/0, /*self_row=*/2);
    auto r2 = col->get_data_at(2);
    ASSERT_EQ(r2.size, rhs_vals[0].size());
    ASSERT_EQ(memcmp(r2.data, rhs_vals[0].data(), r2.size), 0);
}

TEST_F(ColumnVarbinaryTest, SerializeDeserializeRoundtripManual) {
    auto col = ColumnVarbinary::create();
    std::string v = make_bytes(doris::StringView::kInlineSize + 17, 0x60);

    std::vector<char> buf;
    auto len = static_cast<uint32_t>(v.size());
    buf.resize(sizeof(uint32_t) + v.size());
    memcpy(buf.data(), &len, sizeof(uint32_t));
    memcpy(buf.data() + sizeof(uint32_t), v.data(), v.size());

    const char* p = buf.data();
    const char* end = col->deserialize_and_insert_from_arena(p);
    ASSERT_EQ(static_cast<size_t>(end - p), sizeof(uint32_t) + v.size());
    ASSERT_EQ(col->size(), 1U);
    auto r = col->get_data_at(0);
    ASSERT_EQ(r.size, v.size());
    ASSERT_EQ(memcmp(r.data, v.data(), r.size), 0);
}

TEST_F(ColumnVarbinaryTest, SerializeSizeAtShouldIncludeLengthHeader) {
    auto col = ColumnVarbinary::create();
    std::string v = make_bytes(9, 0x70);
    col->insert_data(v.data(), v.size());

    size_t sz = col->serialize_size_at(0);
    // Expect payload + 4 bytes length header.
    EXPECT_EQ(sz, v.size() + sizeof(uint32_t));
}

TEST_F(ColumnVarbinaryTest, ConvertToStringColumn) {
    auto col = ColumnVarbinary::create();
    std::vector<std::string> vals = {make_bytes(0, 0x00), make_bytes(3, 0x80),
                                     make_bytes(doris::StringView::kInlineSize + 2, 0x81)};
    for (auto& v : vals) {
        col->insert_data(v.data(), v.size());
    }

    auto str_mut = assert_cast<ColumnVarbinary&>(*col).convert_to_string_column();
    auto str_col_ptr = std::move(str_mut);
    const auto& str_col = assert_cast<const ColumnString&>(*str_col_ptr);

    ASSERT_EQ(str_col.size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        auto r = str_col.get_data_at(i);
        ASSERT_EQ(r.size, vals[i].size());
        ASSERT_EQ(memcmp(r.data, vals[i].data(), r.size), 0);
    }
}

} // namespace doris::vectorized
