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

#include "common/exception.h"
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
        if (n >= 3) {
            s[n / 3] = '\0';
            s[(2 * n) / 3] = '\0';
        }
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
    // Mix inline (small) and non-inline (large > kInlineSize) values
    std::vector<std::string> vals = {
            make_bytes(1, 0x10),                                  // inline
            make_bytes(doris::StringView::kInlineSize + 5, 0x91), // non-inline (dropped)
            make_bytes(3, 0x12),                                  // inline
            make_bytes(doris::StringView::kInlineSize + 7, 0x92), // non-inline
            make_bytes(0, 0x00),                                  // empty (dropped)
            make_bytes(doris::StringView::kInlineSize + 9, 0x93)  // non-inline
    };
    for (auto& v : vals) {
        col->insert_data(v.data(), v.size());
    }

    IColumn::Filter f = {1, 0, 1, 1, 0, 1};
    size_t expected = 4; // number of ones

    const auto& ccol = assert_cast<const ColumnVarbinary&>(*col);
    ColumnPtr filtered = ccol.filter(f, -1);
    const auto& fcol = assert_cast<const ColumnVarbinary&>(*filtered);
    ASSERT_EQ(fcol.size(), expected);
    std::vector<size_t> kept_idx = {0, 2, 3, 5}; // includes both inline and non-inline
    for (size_t i = 0; i < kept_idx.size(); ++i) {
        auto r = fcol.get_data_at(i);
        const auto& expect = vals[kept_idx[i]];
        ASSERT_EQ(r.size, expect.size());
        ASSERT_EQ(memcmp(r.data, expect.data(), r.size), 0);
    }

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
    // Include large (non-inline) entries to exercise arena path
    std::vector<std::string> vals = {
            make_bytes(1, 0x20),                                  // inline
            make_bytes(doris::StringView::kInlineSize + 3, 0xA0), // non-inline
            make_bytes(3, 0x22),                                  // inline
            make_bytes(doris::StringView::kInlineSize + 8, 0xA1)  // non-inline
    };
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

    // full size
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
    // mix inline and non-inline
    std::vector<std::string> vals = {
            make_bytes(2, 0x40),                                  // inline
            make_bytes(doris::StringView::kInlineSize + 4, 0xB0), // non-inline
            make_bytes(4, 0x42)                                   // inline
    };
    for (auto& v : vals) {
        col->insert_data(v.data(), v.size());
    }

    auto rhs = ColumnVarbinary::create();
    std::vector<std::string> rhs_vals = {
            make_bytes(doris::StringView::kInlineSize + 7, 0xC0), // non-inline
            make_bytes(1, 0x51)                                   // inline
    };
    for (auto& v : rhs_vals) {
        rhs->insert_data(v.data(), v.size());
    }

    // replace row 0 (inline) with rhs[1] (inline) -> stays inline
    col->replace_column_data(*rhs, /*row=*/1, /*self_row=*/0);
    auto r0 = col->get_data_at(0);
    ASSERT_EQ(r0.size, rhs_vals[1].size());
    ASSERT_EQ(memcmp(r0.data, rhs_vals[1].data(), r0.size), 0);

    // replace row 2 (inline) with rhs[0] (non-inline)
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

TEST_F(ColumnVarbinaryTest, FieldAccessOperatorAndGet) {
    auto col = ColumnVarbinary::create();
    std::vector<std::string> vals = {
            make_bytes(1, 0x11), make_bytes(0, 0x00),
            make_bytes(doris::StringView::kInlineSize + 6, 0x12)}; // include non-inline
    for (auto& v : vals) {
        col->insert_data(v.data(), v.size());
    }

    for (size_t i = 0; i < vals.size(); ++i) {
        // operator[]
        Field f = (*col)[i];
        auto sv = vectorized::get<const doris::StringView&>(f);
        ASSERT_EQ(sv.size(), vals[i].size());
        ASSERT_EQ(memcmp(sv.data(), vals[i].data(), sv.size()), 0);
        // get(size_t, Field&)
        Field f2;
        col->get(i, f2);
        auto sv2 = vectorized::get<const doris::StringView&>(f2);
        ASSERT_EQ(sv2.size(), vals[i].size());
        ASSERT_EQ(memcmp(sv2.data(), vals[i].data(), sv2.size()), 0);
    }
}

TEST_F(ColumnVarbinaryTest, InsertField) {
    auto col = ColumnVarbinary::create();
    // prepare inline and non-inline fields
    std::string inline_v = make_bytes(2, 0x21);
    std::string big_v = make_bytes(doris::StringView::kInlineSize + 10, 0x22);

    Field f_inline = Field::create_field<TYPE_VARBINARY>(
            doris::StringView(inline_v.data(), static_cast<uint32_t>(inline_v.size())));
    Field f_big = Field::create_field<TYPE_VARBINARY>(
            doris::StringView(big_v.data(), static_cast<uint32_t>(big_v.size())));

    col->insert(f_inline);
    col->insert(f_big);

    ASSERT_EQ(col->size(), 2U);
    auto r0 = col->get_data_at(0);
    auto r1 = col->get_data_at(1);
    ASSERT_EQ(r0.size, inline_v.size());
    ASSERT_EQ(memcmp(r0.data, inline_v.data(), r0.size), 0);
    ASSERT_EQ(r1.size, big_v.size());
    ASSERT_EQ(memcmp(r1.data, big_v.data(), r1.size), 0);
}

TEST_F(ColumnVarbinaryTest, SerializeValueIntoArenaAndImpl) {
    auto col = ColumnVarbinary::create();
    std::string small = make_bytes(3, 0x31);                                 // inline
    std::string big = make_bytes(doris::StringView::kInlineSize + 12, 0x32); // non-inline
    col->insert_data(small.data(), small.size());
    col->insert_data(big.data(), big.size());

    // serialize_value_into_arena (covers serialize_impl indirectly)
    Arena arena;
    const char* begin = nullptr;
    auto sr_inline = col->serialize_value_into_arena(0, arena, begin);
    ASSERT_EQ(sr_inline.size, small.size() + sizeof(uint32_t));
    uint32_t len_inline;
    memcpy(&len_inline, sr_inline.data, sizeof(uint32_t));
    ASSERT_EQ(len_inline, small.size());
    ASSERT_EQ(memcmp(sr_inline.data + sizeof(uint32_t), small.data(), small.size()), 0);

    auto sr_big = col->serialize_value_into_arena(1, arena, begin);
    ASSERT_EQ(sr_big.size, big.size() + sizeof(uint32_t));
    uint32_t len_big;
    memcpy(&len_big, sr_big.data, sizeof(uint32_t));
    ASSERT_EQ(len_big, big.size());
    ASSERT_EQ(memcmp(sr_big.data + sizeof(uint32_t), big.data(), big.size()), 0);

    // direct serialize_impl
    char buf[4096];
    size_t written = col->serialize_impl(buf, 1);
    ASSERT_EQ(written, big.size() + sizeof(uint32_t));
    uint32_t len_big2;
    memcpy(&len_big2, buf, sizeof(uint32_t));
    ASSERT_EQ(len_big2, big.size());
    ASSERT_EQ(memcmp(buf + sizeof(uint32_t), big.data(), big.size()), 0);
}

TEST_F(ColumnVarbinaryTest, AllocatedBytesAndHasEnoughCapacity) {
    auto dest = ColumnVarbinary::create();
    // Grow dest to obtain some spare capacity
    for (int i = 0; i < 64; ++i) {
        std::string v = make_bytes((i % 5) + 1, static_cast<uint8_t>(0x40 + i)); // mostly inline
        dest->insert_data(v.data(), v.size());
    }
    // Force some non-inline values to ensure arena usage
    for (int i = 0; i < 3; ++i) {
        auto big =
                make_bytes(doris::StringView::kInlineSize + 20 + i, static_cast<uint8_t>(0x90 + i));
        dest->insert_data(big.data(), big.size());
    }
    // Capture capacity & size
    size_t cap = dest->get_data().capacity();
    size_t sz = dest->size();
    ASSERT_GT(cap, sz);

    // Ensure allocated_bytes >= byte_size()
    ASSERT_GE(dest->allocated_bytes(), dest->byte_size());

    // Create src_small with size less than free slots (cap - sz)
    size_t free_slots = cap - sz;
    auto src_small = ColumnVarbinary::create();
    for (size_t i = 0; i < free_slots - 1; ++i) { // leave at least 1 slot
        auto v = make_bytes(1, 0x55);
        src_small->insert_data(v.data(), v.size());
    }
    ASSERT_TRUE(dest->has_enough_capacity(*src_small));

    // src_big exactly fills free slots -> expect false (need strictly greater)
    auto src_big = ColumnVarbinary::create();
    for (size_t i = 0; i < free_slots; ++i) {
        auto v = make_bytes(1, 0x66);
        src_big->insert_data(v.data(), v.size());
    }
    ASSERT_FALSE(dest->has_enough_capacity(*src_big));
}

TEST_F(ColumnVarbinaryTest, InsertRangeFromOutOfBoundsThrows) {
    auto src = ColumnVarbinary::create();
    std::vector<std::string> vals = {make_bytes(2, 0x10), make_bytes(3, 0x20)};
    for (auto& v : vals) {
        src->insert_data(v.data(), v.size());
    }

    auto dst = ColumnVarbinary::create();
    EXPECT_THROW(dst->insert_range_from(*src, /*start=*/1, /*length=*/5), doris::Exception);
}

TEST_F(ColumnVarbinaryTest, GetMaxRowByteSizeMix) {
    auto col = ColumnVarbinary::create();
    // empty, inline, contains '\0', non-inline
    std::string empty;
    std::string inline_v = make_bytes(3, 0x01);       // inline (<= kInlineSize)
    std::string with_zero = std::string("AB\0CD", 5); // explicit embedded zero
    std::string big = make_bytes(doris::StringView::kInlineSize + 15, 0x11); // non-inline

    col->insert_data(empty.data(), empty.size());
    col->insert_data(inline_v.data(), inline_v.size());
    col->insert_data(with_zero.data(), with_zero.size());
    col->insert_data(big.data(), big.size());

    size_t expected = std::max({empty.size(), inline_v.size(), with_zero.size(), big.size()}) +
                      sizeof(uint32_t);
    ASSERT_EQ(col->get_max_row_byte_size(), expected);
}

TEST_F(ColumnVarbinaryTest, SerializeDeserializeKeysArray) {
    auto col = ColumnVarbinary::create();
    std::vector<std::string> vals = {
            std::string(),                                        // empty
            std::string("Z", 1),                                  // single char inline
            std::string("A\0B", 3),                               // inline with zero
            make_bytes(doris::StringView::kInlineSize + 2, 0x33), // non-inline small
            make_bytes(doris::StringView::kInlineSize + 25, 0x44) // non-inline larger
    };
    for (auto& v : vals) {
        col->insert_data(v.data(), v.size());
    }
    size_t n = vals.size();
    size_t per_row_cap = col->get_max_row_byte_size();
    std::vector<std::vector<char>> buffers(n); // each row independent buffer
    std::vector<StringRef> keys(n);
    for (size_t i = 0; i < n; ++i) {
        buffers[i].resize(per_row_cap);
        keys[i].data = buffers[i].data();
        keys[i].size = 0; // used bytes starts at 0
    }
    // serialize each row independently
    col->serialize(keys.data(), n);

    for (size_t i = 0; i < n; ++i) {
        size_t expected_sz = vals[i].size() + sizeof(uint32_t);
        ASSERT_EQ(keys[i].size, expected_sz); // used bytes recorded
        // verify header length matches
        uint32_t len;
        memcpy(&len, buffers[i].data(), sizeof(uint32_t));
        ASSERT_EQ(len, vals[i].size());
        ASSERT_EQ(memcmp(buffers[i].data() + sizeof(uint32_t), vals[i].data(), vals[i].size()), 0);
    }

    // Prepare for deserialize into new column
    auto col2 = ColumnVarbinary::create();
    std::vector<StringRef> dkeys(n);
    for (size_t i = 0; i < n; ++i) {
        dkeys[i].data = buffers[i].data();
        dkeys[i].size = keys[i].size; // remaining bytes to consume
    }
    col2->deserialize(dkeys.data(), n);

    ASSERT_EQ(col2->size(), n);
    for (size_t i = 0; i < n; ++i) {
        auto r = col2->get_data_at(i);
        ASSERT_EQ(r.size, vals[i].size());
        ASSERT_EQ(memcmp(r.data, vals[i].data(), r.size), 0);
        // After deserialize pointer advanced & size reduced
        ASSERT_EQ(dkeys[i].size, 0U);
    }
}

TEST_F(ColumnVarbinaryTest, PermuteThrowsOnShortPermutation) {
    auto col = ColumnVarbinary::create();
    std::vector<std::string> vals = {make_bytes(1, 0x31), make_bytes(1, 0x32),
                                     make_bytes(doris::StringView::kInlineSize + 2, 0x33)};
    for (auto& v : vals) {
        col->insert_data(v.data(), v.size());
    }
    IColumn::Permutation perm = {1};
    EXPECT_THROW(col->permute(perm, 2), doris::Exception);
}

TEST_F(ColumnVarbinaryTest, ReplaceColumnDataOnNonInlineTarget) {
    auto col = ColumnVarbinary::create();
    std::string inl = make_bytes(3, 0x41);                                   // inline
    std::string big1 = make_bytes(doris::StringView::kInlineSize + 5, 0xB1); // non-inline
    std::string big2 = make_bytes(doris::StringView::kInlineSize + 7, 0xB2); // non-inline
    col->insert_data(inl.data(), inl.size());
    col->insert_data(big1.data(), big1.size()); // row 1: non-inline target

    auto rhs = ColumnVarbinary::create();
    std::string rhs_inline = make_bytes(2, 0x51);           // inline
    rhs->insert_data(rhs_inline.data(), rhs_inline.size()); // row 0 inline
    rhs->insert_data(big2.data(), big2.size());             // row 1 non-inline

    col->replace_column_data(*rhs, /*row=*/0, /*self_row=*/1);
    auto r1 = col->get_data_at(1);
    ASSERT_EQ(r1.size, rhs_inline.size());
    ASSERT_EQ(memcmp(r1.data, rhs_inline.data(), r1.size), 0);

    col->replace_column_data(*rhs, /*row=*/1, /*self_row=*/1);
    auto r1b = col->get_data_at(1);
    ASSERT_EQ(r1b.size, big2.size());
    ASSERT_EQ(memcmp(r1b.data, big2.data(), r1b.size), 0);
}

TEST_F(ColumnVarbinaryTest, SerializeSizeAtForNonInline) {
    auto col = ColumnVarbinary::create();
    std::string small = make_bytes(4, 0x61);
    std::string big = make_bytes(doris::StringView::kInlineSize + 9, 0x62);
    col->insert_data(small.data(), small.size());
    col->insert_data(big.data(), big.size());

    EXPECT_EQ(col->serialize_size_at(0), small.size() + sizeof(uint32_t));
    EXPECT_EQ(col->serialize_size_at(1), big.size() + sizeof(uint32_t));
}

TEST_F(ColumnVarbinaryTest, CloneResizedZero) {
    auto col = ColumnVarbinary::create();
    col->insert_data("a", 1);
    col->insert_data("", 0);
    auto c0 = col->clone_resized(0);
    const auto& cc0 = assert_cast<const ColumnVarbinary&>(*c0);
    EXPECT_EQ(cc0.size(), 0U);
}

TEST_F(ColumnVarbinaryTest, GetPermutationAscDescIgnoreLimit) {
    auto col = ColumnVarbinary::create();
    // Deliberately craft strings with shared prefixes & embedded zeros
    std::vector<std::string> vals = {
            std::string("aa"),
            std::string("aa\0", 3),
            std::string("aa\0b", 4),
            std::string("aaa"),
            make_bytes(doris::StringView::kInlineSize + 5, 0x50), // non-inline high bytes
            std::string("aab"),
            std::string("aa\0aa", 5)};
    for (auto& v : vals) {
        col->insert_data(v.data(), v.size());
    }

    IColumn::Permutation perm_asc;
    col->get_permutation(/*reverse=*/false, /*limit=*/3, /*nan_hint=*/0,
                         perm_asc); // limit ignored by impl
    ASSERT_EQ(perm_asc.size(), vals.size());
    // check ascending ordering
    for (size_t i = 1; i < perm_asc.size(); ++i) {
        int c = col->compare_at(perm_asc[i - 1], perm_asc[i], *col, 0);
        ASSERT_LE(c, 0) << "Permutation not ascending at position " << i;
    }

    IColumn::Permutation perm_desc;
    col->get_permutation(/*reverse=*/true, /*limit=*/vals.size(), /*nan_hint=*/0, perm_desc);
    ASSERT_EQ(perm_desc.size(), vals.size());
    for (size_t i = 1; i < perm_desc.size(); ++i) {
        int c = col->compare_at(perm_desc[i - 1], perm_desc[i], *col, 0);
        ASSERT_GE(c, 0) << "Permutation not descending at position " << i;
    }
}

TEST_F(ColumnVarbinaryTest, InsertManyStrings) {
    auto col = ColumnVarbinary::create();

    // Test 1: Insert empty array
    {
        std::vector<StringRef> empty_refs;
        col->insert_many_strings(empty_refs.data(), empty_refs.size());
        EXPECT_EQ(col->size(), 0U);
    }

    // Test 2: Insert single string
    {
        std::string s1 = "hello";
        StringRef ref1(s1.data(), s1.size());
        col->insert_many_strings(&ref1, 1);
        EXPECT_EQ(col->size(), 1U);
        auto data = col->get_data_at(0);
        EXPECT_EQ(data.size, 5U);
        EXPECT_EQ(memcmp(data.data, "hello", 5), 0);
    }

    // Test 3: Insert multiple inline strings (size <= kInlineSize)
    {
        std::string s2 = "abc";
        std::string s3 = "def";
        std::string s4 = make_bytes(doris::StringView::kInlineSize, 0xAA);
        std::vector<StringRef> refs = {StringRef(s2.data(), s2.size()),
                                       StringRef(s3.data(), s3.size()),
                                       StringRef(s4.data(), s4.size())};
        col->insert_many_strings(refs.data(), refs.size());
        EXPECT_EQ(col->size(), 4U); // 1 from test 2 + 3 new

        auto data1 = col->get_data_at(1);
        EXPECT_EQ(data1.size, 3U);
        EXPECT_EQ(memcmp(data1.data, "abc", 3), 0);

        auto data2 = col->get_data_at(2);
        EXPECT_EQ(data2.size, 3U);
        EXPECT_EQ(memcmp(data2.data, "def", 3), 0);

        auto data3 = col->get_data_at(3);
        EXPECT_EQ(data3.size, doris::StringView::kInlineSize);
        EXPECT_EQ(memcmp(data3.data, s4.data(), s4.size()), 0);
    }

    // Test 4: Insert multiple large strings (size > kInlineSize)
    {
        std::string large1 = make_bytes(doris::StringView::kInlineSize + 10, 0x11);
        std::string large2 = make_bytes(doris::StringView::kInlineSize + 20, 0x22);
        std::string large3 = make_bytes(doris::StringView::kInlineSize + 30, 0x33);

        std::vector<StringRef> large_refs = {StringRef(large1.data(), large1.size()),
                                             StringRef(large2.data(), large2.size()),
                                             StringRef(large3.data(), large3.size())};
        size_t before_size = col->size();
        col->insert_many_strings(large_refs.data(), large_refs.size());
        EXPECT_EQ(col->size(), before_size + 3);

        auto data_large1 = col->get_data_at(before_size);
        EXPECT_EQ(data_large1.size, large1.size());
        EXPECT_EQ(memcmp(data_large1.data, large1.data(), large1.size()), 0);

        auto data_large2 = col->get_data_at(before_size + 1);
        EXPECT_EQ(data_large2.size, large2.size());
        EXPECT_EQ(memcmp(data_large2.data, large2.data(), large2.size()), 0);

        auto data_large3 = col->get_data_at(before_size + 2);
        EXPECT_EQ(data_large3.size, large3.size());
        EXPECT_EQ(memcmp(data_large3.data, large3.data(), large3.size()), 0);
    }

    // Test 5: Insert strings with null bytes
    {
        std::string null_str1 = std::string("abc\0def", 7);
        std::string null_str2 = std::string("\0\0\0", 3);
        std::vector<StringRef> null_refs = {StringRef(null_str1.data(), null_str1.size()),
                                            StringRef(null_str2.data(), null_str2.size())};
        size_t before_size = col->size();
        col->insert_many_strings(null_refs.data(), null_refs.size());
        EXPECT_EQ(col->size(), before_size + 2);

        auto data_null1 = col->get_data_at(before_size);
        EXPECT_EQ(data_null1.size, 7U);
        EXPECT_EQ(memcmp(data_null1.data, null_str1.data(), 7), 0);

        auto data_null2 = col->get_data_at(before_size + 1);
        EXPECT_EQ(data_null2.size, 3U);
        EXPECT_EQ(memcmp(data_null2.data, null_str2.data(), 3), 0);
    }

    // Test 6: Insert mixed inline and non-inline strings
    {
        std::string small = "xy";
        std::string medium = make_bytes(doris::StringView::kInlineSize, 0xBB);
        std::string large = make_bytes(doris::StringView::kInlineSize + 50, 0xCC);
        std::vector<StringRef> mixed_refs = {StringRef(small.data(), small.size()),
                                             StringRef(medium.data(), medium.size()),
                                             StringRef(large.data(), large.size())};
        size_t before_size = col->size();
        col->insert_many_strings(mixed_refs.data(), mixed_refs.size());
        EXPECT_EQ(col->size(), before_size + 3);

        auto data_small = col->get_data_at(before_size);
        EXPECT_EQ(data_small.size, 2U);
        EXPECT_EQ(memcmp(data_small.data, "xy", 2), 0);

        auto data_medium = col->get_data_at(before_size + 1);
        EXPECT_EQ(data_medium.size, doris::StringView::kInlineSize);

        auto data_large = col->get_data_at(before_size + 2);
        EXPECT_EQ(data_large.size, large.size());
        EXPECT_EQ(memcmp(data_large.data, large.data(), large.size()), 0);
    }

    // Test 7: Insert UUID-like binary data (16 bytes)
    {
        std::string uuid1 = make_bytes(16, 0x55);
        std::string uuid2 = make_bytes(16, 0x12);
        std::vector<StringRef> uuid_refs = {StringRef(uuid1.data(), uuid1.size()),
                                            StringRef(uuid2.data(), uuid2.size())};
        size_t before_size = col->size();
        col->insert_many_strings(uuid_refs.data(), uuid_refs.size());
        EXPECT_EQ(col->size(), before_size + 2);

        auto data_uuid1 = col->get_data_at(before_size);
        EXPECT_EQ(data_uuid1.size, 16U);
        EXPECT_EQ(memcmp(data_uuid1.data, uuid1.data(), 16), 0);

        auto data_uuid2 = col->get_data_at(before_size + 1);
        EXPECT_EQ(data_uuid2.size, 16U);
        EXPECT_EQ(memcmp(data_uuid2.data, uuid2.data(), 16), 0);
    }
}

TEST_F(ColumnVarbinaryTest, InsertManyStringsOverflow) {
    auto col = ColumnVarbinary::create();

    // Test 1: Insert with max_length larger than actual strings (no overflow)
    {
        std::string s1 = "hello";
        std::string s2 = "world";
        std::vector<StringRef> refs = {StringRef(s1.data(), s1.size()),
                                       StringRef(s2.data(), s2.size())};
        col->insert_many_strings_overflow(refs.data(), refs.size(), 100);
        EXPECT_EQ(col->size(), 2U);

        auto data1 = col->get_data_at(0);
        EXPECT_EQ(data1.size, 5U);
        EXPECT_EQ(memcmp(data1.data, "hello", 5), 0);

        auto data2 = col->get_data_at(1);
        EXPECT_EQ(data2.size, 5U);
        EXPECT_EQ(memcmp(data2.data, "world", 5), 0);
    }

    // Test 2: Insert with max_length equal to string length (exact fit)
    {
        std::string s3 = "test123";
        StringRef ref3(s3.data(), s3.size());
        col->insert_many_strings_overflow(&ref3, 1, 7);
        EXPECT_EQ(col->size(), 3U);

        auto data3 = col->get_data_at(2);
        EXPECT_EQ(data3.size, 7U);
        EXPECT_EQ(memcmp(data3.data, "test123", 7), 0);
    }

    // Test 3: Insert large strings with max_length
    // Note: Current implementation doesn't actually truncate, it just calls insert_many_strings
    // This test verifies the current behavior
    {
        std::string large = make_bytes(doris::StringView::kInlineSize + 100, 0xAA);
        StringRef ref_large(large.data(), large.size());
        size_t before_size = col->size();
        col->insert_many_strings_overflow(&ref_large, 1, 50);
        EXPECT_EQ(col->size(), before_size + 1);

        auto data_large = col->get_data_at(before_size);
        // Current implementation doesn't truncate, so full size is preserved
        EXPECT_EQ(data_large.size, large.size());
        EXPECT_EQ(memcmp(data_large.data, large.data(), large.size()), 0);
    }

    // Test 4: Insert multiple strings with overflow parameter
    {
        std::string s4 = make_bytes(20, 0x11);
        std::string s5 = make_bytes(30, 0x22);
        std::string s6 = make_bytes(40, 0x33);
        std::vector<StringRef> refs = {StringRef(s4.data(), s4.size()),
                                       StringRef(s5.data(), s5.size()),
                                       StringRef(s6.data(), s6.size())};
        size_t before_size = col->size();
        col->insert_many_strings_overflow(refs.data(), refs.size(), 100);
        EXPECT_EQ(col->size(), before_size + 3);

        // Verify all strings are inserted correctly
        auto data4 = col->get_data_at(before_size);
        EXPECT_EQ(data4.size, 20U);
        EXPECT_EQ(memcmp(data4.data, s4.data(), 20), 0);

        auto data5 = col->get_data_at(before_size + 1);
        EXPECT_EQ(data5.size, 30U);
        EXPECT_EQ(memcmp(data5.data, s5.data(), 30), 0);

        auto data6 = col->get_data_at(before_size + 2);
        EXPECT_EQ(data6.size, 40U);
        EXPECT_EQ(memcmp(data6.data, s6.data(), 40), 0);
    }

    // Test 5: Insert binary data (like UUID) with overflow
    {
        std::string uuid = make_bytes(16, 0x55);
        StringRef uuid_ref(uuid.data(), uuid.size());
        size_t before_size = col->size();
        col->insert_many_strings_overflow(&uuid_ref, 1, 32);
        EXPECT_EQ(col->size(), before_size + 1);

        auto data_uuid = col->get_data_at(before_size);
        EXPECT_EQ(data_uuid.size, 16U);
        EXPECT_EQ(memcmp(data_uuid.data, uuid.data(), 16), 0);
    }

    // Test 6: Insert empty strings with max_length
    {
        std::string empty1;
        std::string empty2;
        std::vector<StringRef> empty_refs = {StringRef(empty1.data(), empty1.size()),
                                             StringRef(empty2.data(), empty2.size())};
        size_t before_size = col->size();
        col->insert_many_strings_overflow(empty_refs.data(), empty_refs.size(), 10);
        EXPECT_EQ(col->size(), before_size + 2);

        auto data_empty1 = col->get_data_at(before_size);
        EXPECT_EQ(data_empty1.size, 0U);

        auto data_empty2 = col->get_data_at(before_size + 1);
        EXPECT_EQ(data_empty2.size, 0U);
    }

    // Test 7: Insert strings with null bytes and overflow parameter
    {
        std::string null_data = std::string("abc\0\0\0def", 9);
        StringRef null_ref(null_data.data(), null_data.size());
        size_t before_size = col->size();
        col->insert_many_strings_overflow(&null_ref, 1, 20);
        EXPECT_EQ(col->size(), before_size + 1);

        auto data_null = col->get_data_at(before_size);
        EXPECT_EQ(data_null.size, 9U);
        EXPECT_EQ(memcmp(data_null.data, null_data.data(), 9), 0);
    }
}

} // namespace doris::vectorized
