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
#include <vector>

#include "storage/merger.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "util/json/path_in_data.h"

namespace doris {

class MergerSplitColumnsTest : public ::testing::Test {
protected:
    static TabletColumn make_key_column(int32_t uid, const std::string& name) {
        TabletColumn col;
        col.set_unique_id(uid);
        col.set_name(name);
        col.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        col.set_is_key(true);
        return col;
    }

    static TabletColumn make_value_column(int32_t uid, const std::string& name) {
        TabletColumn col;
        col.set_unique_id(uid);
        col.set_name(name);
        col.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        col.set_is_key(false);
        return col;
    }

    static TabletColumn make_variant_root(int32_t uid, const std::string& name) {
        TabletColumn col;
        col.set_unique_id(uid);
        col.set_name(name);
        col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
        col.set_is_key(false);
        return col;
    }

    static TabletColumn make_extracted(int32_t parent_uid, const std::string& full_path) {
        TabletColumn col;
        col.set_unique_id(-1);
        col.set_name(full_path);
        col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        col.set_parent_unique_id(parent_uid);
        col.set_path_info(PathInData(full_path));
        return col;
    }
};

// Baseline: schema with only regular value columns falls back to the existing
// num_columns_per_group split.
TEST_F(MergerSplitColumnsTest, RegularColumnsOnlyHonoursGroupSize) {
    TabletSchema schema;
    schema.append_column(make_key_column(0, "k0"));
    for (int i = 1; i <= 12; ++i) {
        schema.append_column(make_value_column(i, "v" + std::to_string(i)));
    }

    std::vector<std::vector<uint32_t>> groups;
    std::vector<uint32_t> cluster_idxes;
    Merger::vertical_split_columns(schema, &groups, &cluster_idxes, /*num_columns_per_group=*/5);

    // Expect: 1 key group + ceil(12 / 5) = 3 value groups.
    ASSERT_EQ(groups.size(), 4);
    EXPECT_EQ(groups[0], std::vector<uint32_t>({0}));
    EXPECT_EQ(groups[1].size(), 5);
    EXPECT_EQ(groups[2].size(), 5);
    EXPECT_EQ(groups[3].size(), 2);
}

// One variant column with a doc_value bucket, sparse buckets, and 7 sub-columns.
// doc_value → 1/group, sparse → 5/group, sub-columns → 100/group (so a single
// group at this size). Variant groups are emitted after the regular value group.
TEST_F(MergerSplitColumnsTest, SingleVariantSplitsByCategory) {
    TabletSchema schema;
    schema.append_column(make_key_column(0, "k0"));
    schema.append_column(make_value_column(1, "v1"));
    schema.append_column(make_variant_root(100, "var"));

    schema.append_column(make_extracted(100, "var.__DORIS_VARIANT_DOC_VALUE__.b0"));
    for (int b = 0; b < 6; ++b) {
        schema.append_column(
                make_extracted(100, "var.__DORIS_VARIANT_SPARSE__.b" + std::to_string(b)));
    }
    for (int i = 0; i < 7; ++i) {
        schema.append_column(make_extracted(100, "var.path" + std::to_string(i)));
    }

    std::vector<std::vector<uint32_t>> groups;
    std::vector<uint32_t> cluster_idxes;
    Merger::vertical_split_columns(schema, &groups, &cluster_idxes, /*num_columns_per_group=*/5);

    // Expected groups (in order):
    //   key group: [0]
    //   regular value group: [1, 2]  (v1 + variant root, both non-extracted)
    //   doc_value: 1 group of 1 column
    //   sparse: ceil(6/5) = 2 groups (sizes 5 and 1)
    //   sub-columns: 1 group of 7 columns (<= 100)
    ASSERT_EQ(groups.size(), 1 /*key*/ + 1 /*regular*/ + 1 /*doc*/ + 2 /*sparse*/ + 1 /*sub*/);
    EXPECT_EQ(groups[0], std::vector<uint32_t>({0}));
    EXPECT_EQ(groups[1], std::vector<uint32_t>({1, 2}));
    // Doc value: one column per group.
    EXPECT_EQ(groups[2].size(), 1);
    // Sparse: 5 + 1.
    EXPECT_EQ(groups[3].size(), 5);
    EXPECT_EQ(groups[4].size(), 1);
    // Sub columns under 100 stay in one group.
    EXPECT_EQ(groups[5].size(), 7);
}

// Sub-column count that exceeds 100 * 20 = 2000 must widen the per-group size
// rather than emit more than 20 groups.
TEST_F(MergerSplitColumnsTest, WideVariantCapsSubColumnGroupCount) {
    TabletSchema schema;
    schema.append_column(make_key_column(0, "k0"));
    schema.append_column(make_variant_root(100, "var"));

    constexpr int kNumSub = 2500;
    for (int i = 0; i < kNumSub; ++i) {
        schema.append_column(make_extracted(100, "var.p" + std::to_string(i)));
    }

    std::vector<std::vector<uint32_t>> groups;
    std::vector<uint32_t> cluster_idxes;
    Merger::vertical_split_columns(schema, &groups, &cluster_idxes, /*num_columns_per_group=*/5);

    // groups: 1 key + 1 regular (variant root) + N sub-column groups.
    ASSERT_GE(groups.size(), 3u);
    const size_t sub_group_first = 2;
    const size_t sub_group_count = groups.size() - sub_group_first;
    EXPECT_EQ(sub_group_count, 20u) << "must cap at 20 sub-column groups";

    // ceil(2500 / 20) = 125 columns per group; 20 groups of 125 covers 2500.
    size_t total = 0;
    for (size_t i = sub_group_first; i < groups.size(); ++i) {
        EXPECT_LE(groups[i].size(), 125u);
        total += groups[i].size();
    }
    EXPECT_EQ(total, static_cast<size_t>(kNumSub));
}

// Two variants must be grouped independently — one variant's wide sub-column
// list must not pull doc/sparse columns of another variant into its bucket.
TEST_F(MergerSplitColumnsTest, MultipleVariantsAreIsolated) {
    TabletSchema schema;
    schema.append_column(make_key_column(0, "k0"));
    schema.append_column(make_variant_root(100, "var_a"));
    schema.append_column(make_variant_root(200, "var_b"));

    schema.append_column(make_extracted(100, "var_a.__DORIS_VARIANT_DOC_VALUE__.b0"));
    schema.append_column(make_extracted(100, "var_a.path0"));
    schema.append_column(make_extracted(100, "var_a.path1"));

    schema.append_column(make_extracted(200, "var_b.__DORIS_VARIANT_SPARSE__"));
    schema.append_column(make_extracted(200, "var_b.path0"));

    std::vector<std::vector<uint32_t>> groups;
    std::vector<uint32_t> cluster_idxes;
    Merger::vertical_split_columns(schema, &groups, &cluster_idxes, /*num_columns_per_group=*/5);

    // Sanity: at least 5 groups (key, regular, var_a doc_value, var_a sub,
    // var_b sparse, var_b sub). Order: regular-values, then per-variant blocks
    // in unique_id order (std::map iteration).
    ASSERT_EQ(groups.size(), 6u);
    EXPECT_EQ(groups[0], std::vector<uint32_t>({0}));    // key
    EXPECT_EQ(groups[1], std::vector<uint32_t>({1, 2})); // var_a + var_b roots
    EXPECT_EQ(groups[2].size(), 1);                      // var_a doc_value (uid=100 first)
    EXPECT_EQ(groups[3], std::vector<uint32_t>({4, 5})); // var_a sub-cols
    EXPECT_EQ(groups[4].size(), 1);                      // var_b sparse
    EXPECT_EQ(groups[5], std::vector<uint32_t>({7}));    // var_b sub-col
}

} // namespace doris
