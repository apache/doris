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

#include <string>
#include <string_view>

#include "core/column/column_string.h"
#include "exec/common/join_utils.h"

namespace doris {
namespace {

template <typename StringColumn>
size_t max_key_length(const StringColumn& column) {
    return packed_string_join::max_build_key_length(column.get_chars().data(),
                                                    column.get_offsets().data() - 1, column.size());
}

template <typename Key, typename StringColumn>
void check_packed_join() {
    auto build = StringColumn::create();
    auto probe = StringColumn::create();
    auto append = [](auto& column, std::string_view value) {
        column->insert_data(value.data(), value.size());
    };
    const std::string full(sizeof(Key), 'x');
    const std::string trailing_zero("a\0", 2);
    const std::string internal_zero("a\0b", 3);
    const std::string binary("\xff", 1);
    for (std::string_view value :
         {std::string_view(""), std::string_view(""), std::string_view("a"), std::string_view(full),
          std::string_view(internal_zero), std::string_view(binary), std::string_view("")}) {
        append(build, value);
    }
    const size_t max_length = max_key_length(*build);
    ASSERT_EQ(max_length, sizeof(Key));
    PackedStringJoinContext<Key> context(max_length);
    constexpr uint8_t build_nulls[] {1, 0, 0, 0, 0, 0, 1};
    auto& table = *context.hash_table;
    table.template prepare_build<TJoinOp::INNER_JOIN>(build->size(), 16, true, 0);
    const auto bucket_size = table.get_bucket_size();
    const auto build_rows = static_cast<uint32_t>(build->size());
    context.init_serialized_keys({build.get()}, build_rows, build_nulls, true, true, bucket_size);
    table.build(context.keys, context.bucket_nums.data(), build_rows, true);
    EXPECT_EQ(context.serialized_keys_size(true), build->size() * sizeof(Key));

    const std::string too_long(sizeof(Key) + 1, 'x');
    append(probe, "");
    append(probe, "a");
    append(probe, full);
    append(probe, trailing_zero);
    append(probe, internal_zero);
    append(probe, binary);
    append(probe, too_long);
    append(probe, "");
    constexpr uint8_t probe_nulls[] {0, 0, 0, 0, 0, 0, 0, 1};
    const auto probe_rows = static_cast<uint32_t>(probe->size());
    context.init_serialized_keys({probe.get()}, probe_rows, probe_nulls, true, false, bucket_size);
    EXPECT_EQ(context.bucket_nums[3], table.no_match_bucket(bucket_size));
    EXPECT_EQ(context.bucket_nums[6], table.no_match_bucket(bucket_size));
    EXPECT_EQ(context.bucket_nums[7], bucket_size);
    table.pre_build_idxs(context.bucket_nums);
    EXPECT_EQ(context.bucket_nums[3], 0);
    EXPECT_EQ(context.bucket_nums[6], 0);
    EXPECT_EQ(context.bucket_nums[7], 6);

    uint32_t probe_indexes[16] {};
    uint32_t build_indexes[16] {};
    bool probe_visited = false;
    const auto [probe_pos, build_pos, matched] = table.template find_batch<TJoinOp::INNER_JOIN>(
            context.keys, context.bucket_nums.data(), 0, 0, probe_rows, probe_indexes,
            probe_visited, build_indexes, nullptr, false, false, false);
    EXPECT_EQ(probe_pos, probe->size());
    EXPECT_EQ(build_pos, 0);
    ASSERT_EQ(matched, 6);
    EXPECT_EQ((std::vector<uint32_t>(probe_indexes, probe_indexes + matched)),
              (std::vector<uint32_t> {0, 1, 2, 4, 5, 7}));
    EXPECT_EQ((std::vector<uint32_t>(build_indexes, build_indexes + matched)),
              (std::vector<uint32_t> {1, 2, 3, 4, 5, 6}));

    append(build, trailing_zero);
    EXPECT_EQ(max_key_length(*build), 0);
}

TEST(PackedStringJoinKeyTest, KeyWidthsAndNoMatchBucket) {
    check_packed_join<UInt32, ColumnString>();
    check_packed_join<UInt64, ColumnString>();
    check_packed_join<UInt128, ColumnString64>();
}

TEST(PackedStringJoinKeyTest, BuildWithLongValueKeepsStringKeys) {
    auto build = ColumnString::create();
    build->insert_default();
    build->insert_data("abcdefghijklmnopq", 17);
    EXPECT_EQ(max_key_length(*build), 0);
}

} // namespace
} // namespace doris
