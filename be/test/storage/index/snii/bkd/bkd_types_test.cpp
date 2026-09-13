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

// ---------------------------------------------------------------------------
// Compile-time contracts
// ---------------------------------------------------------------------------
//
// These belong at file scope, not inside a TEST body. A static_assert fires at
// COMPILE time, so wrapping one in a test case buys nothing: the build already
// failed before any test could run, and the surviving runtime body was
// SUCCEED() -- an assertion that cannot fail and therefore reports nothing.

// Design doc 5.1: the leaf directory is (delta-varint64 offsets, varint32
// counts). Offsets are bkd_data-relative and need the full 64-bit range; counts
// are bounded by points_per_leaf, which open() enforces (bkd_index_block.cpp).
static_assert(std::is_same_v<decltype(LeafRef::offset), uint64_t>);
static_assert(std::is_same_v<decltype(LeafRef::count), uint32_t>);

// Design doc 9: this layer is POD-only -- no virtuals, no ownership -- which is
// what lets a reader hold these by value and share them across concurrent
// queries with no locking.
static_assert(std::is_trivially_copyable_v<LeafRef>);
static_assert(std::is_trivially_copyable_v<PointRef>);
static_assert(std::is_trivially_copyable_v<BkdSections>);
static_assert(std::is_trivially_copyable_v<BkdStats>);
static_assert(std::is_trivially_copyable_v<BkdIndexHeader>);

// ---------------------------------------------------------------------------
// Runtime contracts
// ---------------------------------------------------------------------------
//
// Only defaults that something downstream actually depends on are asserted here.
// A test that assigns a struct field and reads it back verifies the C++
// language, not this module; three such cases were removed rather than left to
// pad the count. That the DEFAULTS ARE HONORED is checked where it is
// observable -- see BkdBuilderTest.UntouchedOptionsUseTheDocumentedLeafCapacity.

// Design doc 6.1: bytes_per_dim / field_type are REQUIRED, so their defaults must
// be the "unset" sentinel (FieldType has no value 0). points_per_leaf and
// build_buffer_bytes carry the documented defaults.
TEST(SniiBkdTypes, BuilderOptionDefaults) {
    BkdBuilderOptions opts;
    EXPECT_EQ(opts.bytes_per_dim, 0U);
    EXPECT_EQ(static_cast<int>(opts.field_type), 0);
    // Asserted as literals as well as against the constants: a caller that never
    // touches these fields must observe the documented 128 / 256MB, however the
    // defaults happen to be spelled.
    EXPECT_EQ(opts.points_per_leaf, 128U);
    EXPECT_EQ(opts.build_buffer_bytes, 256ULL << 20);
    EXPECT_EQ(opts.points_per_leaf, kDefaultPointsPerLeaf);
    EXPECT_EQ(opts.build_buffer_bytes, kDefaultBuildBufferBytes);
    EXPECT_EQ(opts.reporter, nullptr);
}

// Design doc 5.3: an empty index is expressed EXPLICITLY by leaf_count == 0,
// not by the old implementation's implicit indexFP == 0 sentinel. A default
// constructed header/stats must therefore already read as "empty".
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

// BkdStats is filled by finish() and read by the caller. Zero defaults are the
// contract that a field finish() forgot to set reads as "nothing" rather than as
// stack garbage -- which is exactly how the old implementation's docs_seen_ got
// away with being uninitialized for years (design doc 14 #7). That finish()
// actually populates every field is asserted in bkd_builder_test.cpp.
TEST(SniiBkdTypes, DefaultStatsAreZero) {
    BkdStats stats;
    EXPECT_EQ(stats.point_count, 0U);
    EXPECT_EQ(stats.doc_count, 0U);
    EXPECT_EQ(stats.leaf_count, 0U);
    EXPECT_EQ(stats.index_bytes, 0U);
    EXPECT_EQ(stats.data_bytes, 0U);
    EXPECT_FALSE(stats.built_with_spill);
}
