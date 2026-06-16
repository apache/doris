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
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "storage/predicate/predicate_creator.h"
#include "testutil/index_storage_test_util.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kVariantUid = 2;

std::shared_ptr<ColumnPredicate> bigint_equals(int32_t column_id, std::string column_name,
                                               int64_t value) {
    return create_comparison_predicate<PredicateType::EQ>(
            column_id, std::move(column_name), std::make_shared<DataTypeInt64>(),
            Field::create_field<TYPE_BIGINT>(value), false);
}

std::shared_ptr<ColumnPredicate> bigint_greater(int32_t column_id, std::string column_name,
                                                int64_t value) {
    return create_comparison_predicate<PredicateType::GT>(
            column_id, std::move(column_name), std::make_shared<DataTypeInt64>(),
            Field::create_field<TYPE_BIGINT>(value), false);
}

VariantColumnSpec dynamic_variant_column(bool is_bf_column = false) {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.is_bf_column = is_bf_column;
    variant.max_subcolumns_count = 4;
    return variant;
}

std::vector<std::string> split_dynamic_variant_rows(size_t low_rows, int64_t low_value,
                                                    size_t high_rows, int64_t high_value) {
    std::vector<std::string> rows;
    rows.reserve(low_rows + high_rows);
    for (size_t i = 0; i < low_rows; ++i) {
        rows.push_back(R"({"dynamic_i": )" + std::to_string(low_value) + "}");
    }
    for (size_t i = 0; i < high_rows; ++i) {
        rows.push_back(R"({"dynamic_i": )" + std::to_string(high_value) + "}");
    }
    return rows;
}

class IndexStorageVariantDynamicPathPruningTest : public IndexStorageTestFixture {
protected:
    void verify_dynamic_path_filter(const std::vector<RowsetSharedPtr>& rowsets,
                                    const std::function<void(const IndexReadResult&)>& verify) {
        auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets);
        ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
        const int32_t path_column_id = column_id_by_path("v.dynamic_i");
        ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
        const auto& path_column = tablet_schema()->column(path_column_id);

        IndexReadOptions read_options;
        read_options.enable_inverted_index_query = false;
        read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
        read_options.target_cast_type_for_variants[path_column.name()] =
                std::make_shared<DataTypeInt64>();
        read_options.predicates.push_back(bigint_greater(path_column_id, path_column.name(), 50));

        auto read_result = read_rowsets(readable_rowsets.value(), read_options);
        ASSERT_TRUE(read_result.has_value()) << read_result.error();
        verify(read_result.value());
        expect_inverted_index_not_attempted(read_result.value());
    }
};

TEST_F(IndexStorageVariantDynamicPathPruningTest,
       DynamicPathSegmentZoneMapPrunesBeforeCompactionWithoutPredefinedTypedPath) {
    const auto index_case =
            IndexStorageCaseBuilder("variant_dynamic_path_segment_zone_map_prune")
                    .tablet_id(110060)
                    .variant_column(dynamic_variant_column())
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"dynamic_i": 1})", R"({"dynamic_i": 2})",
                                     R"({"dynamic_i": 100})", R"({"dynamic_i": 101})"},
                                    0),
                            2)
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    verify_dynamic_path_filter(rowsets.value(), [=](const IndexReadResult& result) {
        EXPECT_EQ(result.rows_read, 2);
        expect_raw_rows_read(result, 2);
        expect_segment_pruned(result, 1);
    });

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();

    verify_dynamic_path_filter(reloaded.value(), [=](const IndexReadResult& result) {
        EXPECT_EQ(result.rows_read, 2);
        EXPECT_GT(result.stats.raw_rows_read, 0);
    });
}

// Expected-red: no-typed-path dynamic Variant path segment pruning works before compaction, but
// the compacted rowset currently reads all rows for the same predicate.
TEST_F(IndexStorageVariantDynamicPathPruningTest,
       DISABLED_DynamicPathSegmentZoneMapPrunesAfterCompactionWithoutPredefinedTypedPath) {
    const auto index_case =
            IndexStorageCaseBuilder("variant_dynamic_path_segment_zone_map_after_compaction")
                    .tablet_id(110063)
                    .variant_column(dynamic_variant_column())
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"dynamic_i": 1})", R"({"dynamic_i": 2})",
                                     R"({"dynamic_i": 100})", R"({"dynamic_i": 101})"},
                                    0),
                            2)
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();

    verify_dynamic_path_filter(reloaded.value(), [=](const IndexReadResult& result) {
        EXPECT_EQ(result.rows_read, 2);
        EXPECT_LT(result.stats.raw_rows_read, 4);
    });
}

// Expected-red: no-typed-path dynamic Variant path page zone-map pruning currently does not filter
// pages before or after compaction.
TEST_F(IndexStorageVariantDynamicPathPruningTest,
       DISABLED_DynamicPathPageZoneMapPrunesWithoutPredefinedTypedPath) {
    constexpr size_t kLowRows = 2048;
    constexpr size_t kHighRows = 2048;

    IndexTabletOptions options;
    options.tablet_id = 110061;
    options.storage_page_size = 4096;
    options.variant_columns.push_back(dynamic_variant_column());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.max_rows_per_segment = static_cast<int64_t>(kLowRows + kHighRows);
    rowset.data_sources.push_back(IndexDataSourceSpec::inline_variant(
            split_dynamic_variant_rows(kLowRows, 1, kHighRows, 100), 0));

    ASSERT_TRUE(create_tablet(options).ok());
    auto rowsets = write_rowsets({rowset});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    verify_dynamic_path_filter(rowsets.value(), [=](const IndexReadResult& result) {
        EXPECT_EQ(result.rows_read, kHighRows);
        expect_segment_pruned(result, 0);
        EXPECT_GT(result.stats.raw_rows_read, 0);
        EXPECT_LT(result.stats.raw_rows_read, static_cast<int64_t>(kLowRows + kHighRows));
        EXPECT_GT(result.stats.rows_stats_filtered, 0);
    });

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();

    verify_dynamic_path_filter(reloaded.value(), [=](const IndexReadResult& result) {
        EXPECT_EQ(result.rows_read, kHighRows);
        EXPECT_GT(result.stats.raw_rows_read, 0);
        EXPECT_LT(result.stats.raw_rows_read, static_cast<int64_t>(kLowRows + kHighRows));
        EXPECT_GT(result.stats.rows_stats_filtered, 0);
    });
}

// Expected-red: no-typed-path dynamic Variant subpaths currently do not use bloom filters before
// or after compaction.
TEST_F(IndexStorageVariantDynamicPathPruningTest,
       DISABLED_DynamicPathBloomFilterPrunesWithoutPredefinedTypedPath) {
    constexpr size_t kLowRows = 2048;
    constexpr size_t kHighRows = 2048;

    IndexTabletOptions options;
    options.tablet_id = 110062;
    options.storage_page_size = 4096;
    options.variant_columns.push_back(dynamic_variant_column(true));

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.max_rows_per_segment = static_cast<int64_t>(kLowRows + kHighRows);
    rowset.data_sources.push_back(IndexDataSourceSpec::inline_variant(
            split_dynamic_variant_rows(kLowRows, 1, kHighRows, 100), 0));

    ASSERT_TRUE(create_tablet(options).ok());
    auto rowsets = write_rowsets({rowset});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto verify_bloom_filter = [&](const std::vector<RowsetSharedPtr>& target_rowsets) {
        auto readable_rowsets = rowsets_with_variant_extended_schema(target_rowsets);
        ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
        const int32_t path_column_id = column_id_by_path("v.dynamic_i");
        ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
        const auto& path_column = tablet_schema()->column(path_column_id);

        IndexReadOptions read_options;
        read_options.enable_inverted_index_query = false;
        read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
        read_options.target_cast_type_for_variants[path_column.name()] =
                std::make_shared<DataTypeInt64>();
        read_options.predicates.push_back(bigint_equals(path_column_id, path_column.name(), 999));

        ScopedDebugPoint must_filter("bloom_filter_must_filter_data");
        auto read_result = read_rowsets(readable_rowsets.value(), read_options);
        ASSERT_TRUE(read_result.has_value()) << read_result.error();
        EXPECT_EQ(read_result->rows_read, 0);
        expect_bloom_filter_filtered(read_result.value(), kLowRows + kHighRows);
        expect_inverted_index_not_attempted(read_result.value());
    };

    verify_bloom_filter(rowsets.value());

    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();

    verify_bloom_filter(reloaded.value());
}

} // namespace
} // namespace doris::index_storage_test
