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

// Unit tests for DefaultNestedGroupReadProvider, DefaultNestedGroupWriteProvider,
// NestedGroupReader struct, NestedGroupPathMatch struct, and find_in_nested_groups.
//
// See also:
//   nested_group_path_test.cpp       — nested_group_path.h utilities
//   nested_group_path_filter_test.cpp — NestedGroupPathFilter

#include "olap/rowset/segment_v2/variant/nested_group_provider.h"

#include <gtest/gtest.h>

#include <roaring/roaring.hh>

#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"
#include "olap/rowset/segment_v2/variant/variant_statistics.h"
#include "vec/columns/column_variant.h"

namespace doris::segment_v2 {

// ===========================================================================
// Default provider contract tests
// ===========================================================================

TEST(NestedGroupProviderTest, DefaultReadProviderIsDisabled) {
    auto provider = create_nested_group_read_provider();
    ASSERT_TRUE(provider != nullptr);
    if (provider->should_enable_nested_group_read_path()) {
        GTEST_SKIP() << "EE build: read provider is enabled";
    }
    EXPECT_FALSE(provider->should_enable_nested_group_read_path());
}

TEST(NestedGroupProviderTest, DefaultWriteProviderIsNoOp) {
    auto write_provider = create_nested_group_write_provider();
    ASSERT_TRUE(write_provider != nullptr);

    // In EE, the write provider may have real behavior, skip these checks.
    auto read_provider = create_nested_group_read_provider();
    if (read_provider && read_provider->should_enable_nested_group_read_path()) {
        GTEST_SKIP() << "EE build: write provider has real implementation";
    }

    auto column_variant = vectorized::ColumnVariant::create(0);
    ColumnWriterOptions opts;
    VariantStatistics statistics;

    EXPECT_TRUE(write_provider
                        ->prepare(*column_variant, false, nullptr, opts, nullptr, 0, nullptr,
                                  &statistics)
                        .ok());
    EXPECT_EQ(0, write_provider->estimate_buffer_size());
    EXPECT_TRUE(write_provider->finish().ok());
    EXPECT_TRUE(write_provider->write_data().ok());
    EXPECT_TRUE(write_provider->write_ordinal_index().ok());
    EXPECT_TRUE(write_provider->write_zone_map().ok());
    EXPECT_TRUE(write_provider->write_inverted_index().ok());
    EXPECT_TRUE(write_provider->write_bloom_filter_index().ok());
}

// ===========================================================================
// DefaultNestedGroupReadProvider — detailed method contract
// ===========================================================================

TEST(DefaultNestedGroupReadProviderTest, TryBuildReadPlanReturnsFalse) {
    auto provider = create_nested_group_read_provider();
    ASSERT_NE(nullptr, provider);

    NestedGroupReaders readers;
    TabletColumn col;
    vectorized::PathInData path;

    bool is_whole = true;
    vectorized::DataTypePtr out_type;
    vectorized::PathInData out_path;
    std::string child_path, pruned_path;
    std::vector<const NestedGroupReader*> chain;
    std::optional<NestedGroupPathFilter> path_filter;

    bool result = provider->try_build_read_plan(nullptr, readers, col, nullptr, 0, path, &is_whole,
                                                &out_type, &out_path, &child_path, &pruned_path,
                                                &chain, &path_filter);
    EXPECT_FALSE(result);
}

TEST(DefaultNestedGroupReadProviderTest, CreateNestedGroupIteratorCEBehavior) {
    auto provider = create_nested_group_read_provider();
    ASSERT_NE(nullptr, provider);
    if (provider->should_enable_nested_group_read_path()) {
        GTEST_SKIP() << "EE build: create_nested_group_iterator has real implementation";
    }

    std::vector<const NestedGroupReader*> chain;
    ColumnIteratorUPtr iter;
    vectorized::DataTypePtr out_type;
    std::optional<NestedGroupPathFilter> filter;

    auto status =
            provider->create_nested_group_iterator(false, chain, "", "", filter, &iter, &out_type);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
    EXPECT_NE(status.to_string().find("not available"), std::string::npos);
}

TEST(DefaultNestedGroupReadProviderTest, GetTotalElementsCEBehavior) {
    auto provider = create_nested_group_read_provider();
    ASSERT_NE(nullptr, provider);
    if (provider->should_enable_nested_group_read_path()) {
        GTEST_SKIP() << "EE build: get_total_elements has real implementation";
    }

    ColumnIteratorOptions opts;
    uint64_t total = 0;
    auto status = provider->get_total_elements(opts, nullptr, &total);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
}

TEST(DefaultNestedGroupReadProviderTest, MapElementsToParentOrdsCEBehavior) {
    auto provider = create_nested_group_read_provider();
    ASSERT_NE(nullptr, provider);
    if (provider->should_enable_nested_group_read_path()) {
        GTEST_SKIP() << "EE build: map_elements_to_parent_ords has real implementation";
    }

    std::vector<const NestedGroupReader*> chain;
    ColumnIteratorOptions opts;
    roaring::Roaring element_bitmap;
    roaring::Roaring parent_bitmap;

    auto status =
            provider->map_elements_to_parent_ords(chain, opts, element_bitmap, &parent_bitmap);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
    EXPECT_NE(status.to_string().find("not available"), std::string::npos);
}

TEST(DefaultNestedGroupReadProviderTest, InitReadersCEBehavior) {
    auto provider = create_nested_group_read_provider();
    ASSERT_NE(nullptr, provider);
    if (provider->should_enable_nested_group_read_path()) {
        GTEST_SKIP() << "EE build: init_readers requires real footer";
    }

    ColumnReaderOptions opts;
    NestedGroupReaders out_readers;
    auto status = provider->init_readers(opts, nullptr, nullptr, 0, out_readers);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(out_readers.empty());
}

// ===========================================================================
// NestedGroupReader struct
// ===========================================================================

TEST(NestedGroupReaderTest, IsValidWhenOffsetsReaderIsNull) {
    NestedGroupReader reader;
    EXPECT_FALSE(reader.is_valid());
    EXPECT_TRUE(reader.array_path.empty());
    EXPECT_EQ(1, reader.depth);
}

TEST(NestedGroupReaderTest, DefaultDepthIsOne) {
    NestedGroupReader reader;
    EXPECT_EQ(1, reader.depth);
}

TEST(NestedGroupReaderTest, ChildReadersEmptyByDefault) {
    NestedGroupReader reader;
    EXPECT_TRUE(reader.child_readers.empty());
    EXPECT_TRUE(reader.nested_group_readers.empty());
}

// ===========================================================================
// NestedGroupPathMatch struct
// ===========================================================================

TEST(NestedGroupPathMatchTest, DefaultValues) {
    NestedGroupPathMatch match;
    EXPECT_EQ(nullptr, match.reader);
    EXPECT_TRUE(match.chain.empty());
    EXPECT_TRUE(match.child_path.empty());
    EXPECT_FALSE(match.found);
}

// ===========================================================================
// find_in_nested_groups
// ===========================================================================

TEST(NestedGroupProviderTest, FindInNestedGroupsReturnsEmpty) {
    NestedGroupReaders readers;
    auto result = find_in_nested_groups(readers, "some.path", false);
    EXPECT_FALSE(result.found);
    EXPECT_TRUE(result.chain.empty());
}

TEST(NestedGroupProviderTest, FindInNestedGroupsEmptyPath) {
    NestedGroupReaders readers;
    auto result = find_in_nested_groups(readers, "", false);
    EXPECT_FALSE(result.found);
}

TEST(NestedGroupProviderTest, FindInNestedGroupsEmptyReadersWithChain) {
    NestedGroupReaders readers;
    auto result = find_in_nested_groups(readers, "some.path", true);
    EXPECT_FALSE(result.found);
    EXPECT_TRUE(result.chain.empty());
    EXPECT_TRUE(result.child_path.empty());
}

TEST(NestedGroupProviderTest, FindInNestedGroupsMultiplePaths) {
    NestedGroupReaders readers;
    auto result1 = find_in_nested_groups(readers, "items.msg", false);
    EXPECT_FALSE(result1.found);

    auto result2 = find_in_nested_groups(readers, "items.title", true);
    EXPECT_FALSE(result2.found);
}

} // namespace doris::segment_v2
