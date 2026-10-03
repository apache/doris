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

#include "storage/index/snii/bkd/bkd_types.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <type_traits>

#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/olap_common.h"

using namespace doris::snii::bkd;

// Leaf offsets need 64 bits; leaf counts fit in 32 bits.
static_assert(std::is_same_v<decltype(LeafRef::offset), uint64_t>);
static_assert(std::is_same_v<decltype(LeafRef::count), uint32_t>);

// Readers copy these metadata types by value.
static_assert(std::is_trivially_copyable_v<LeafRef>);
static_assert(std::is_trivially_copyable_v<PointRef>);
static_assert(std::is_trivially_copyable_v<BkdSections>);
static_assert(std::is_trivially_copyable_v<BkdStats>);
static_assert(std::is_trivially_copyable_v<BkdIndexHeader>);

TEST(SniiBkdTypes, BuilderOptionDefaults) {
    BkdBuilderOptions opts;
    EXPECT_EQ(opts.bytes_per_dim, 0U);
    EXPECT_EQ(static_cast<int>(opts.field_type), 0);
    EXPECT_EQ(opts.points_per_leaf, 128U);
    EXPECT_EQ(opts.build_buffer_bytes, 256ULL << 20);
    EXPECT_EQ(opts.points_per_leaf, kDefaultPointsPerLeaf);
    EXPECT_EQ(opts.build_buffer_bytes, kDefaultBuildBufferBytes);
    EXPECT_EQ(opts.reporter, nullptr);
}

TEST(SniiBkdTypes, DefaultHeaderIsEmptyIndex) {
    BkdIndexHeader header;
    EXPECT_EQ(header.format_version, 1U);
    EXPECT_EQ(header.format_version, kFormatVersion);
    EXPECT_EQ(header.flags, 0U);
    EXPECT_EQ(header.bytes_per_dim, 0U);
    EXPECT_EQ(static_cast<int>(header.field_type), 0);
    EXPECT_EQ(header.point_count, 0U);
    EXPECT_EQ(header.doc_count, 0U);
    EXPECT_EQ(header.leaf_count, 0U);
    EXPECT_EQ(header.points_per_leaf, 0U);
}

TEST(SniiBkdTypes, DefaultStatsAreZero) {
    BkdStats stats;
    EXPECT_EQ(stats.point_count, 0U);
    EXPECT_EQ(stats.doc_count, 0U);
    EXPECT_EQ(stats.leaf_count, 0U);
    EXPECT_EQ(stats.index_bytes, 0U);
    EXPECT_EQ(stats.data_bytes, 0U);
    EXPECT_FALSE(stats.built_with_spill);
}
