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

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/value/vdatetime_value.h"
#include "storage/predicate/predicate_creator.h"
#include "testutil/index_storage_test_util.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kVariantUid = 2;
constexpr int64_t kIntPatternIndexId = 210201;
constexpr int64_t kStringPatternIndexId = 210202;
constexpr int64_t kBigIntPatternIndexId = 210203;
constexpr int64_t kDoublePatternIndexId = 210204;
constexpr int64_t kBoolPatternIndexId = 210205;
constexpr int64_t kDatePatternIndexId = 210206;
constexpr int64_t kDateTimePatternIndexId = 210207;
constexpr std::string_view kIntPath = "int_1";

std::shared_ptr<ColumnPredicate> typed_equals(int32_t column_id, std::string column_name,
                                              DataTypePtr data_type, Field value) {
    return create_comparison_predicate<PredicateType::EQ>(column_id, std::move(column_name),
                                                          std::move(data_type), value, false);
}

std::shared_ptr<ColumnPredicate> int_equals(int32_t column_id, std::string column_name,
                                            int32_t value) {
    return create_comparison_predicate<PredicateType::EQ>(
            column_id, std::move(column_name), std::make_shared<DataTypeInt32>(),
            Field::create_field<TYPE_INT>(value), false);
}

std::shared_ptr<ColumnPredicate> int_greater(int32_t column_id, std::string column_name,
                                             int32_t value) {
    return create_comparison_predicate<PredicateType::GT>(
            column_id, std::move(column_name), std::make_shared<DataTypeInt32>(),
            Field::create_field<TYPE_INT>(value), false);
}

DataTypePtr nullable_target_type(const DataTypePtr& type) {
    return make_nullable(type);
}

DataTypePtr nullable_int32_target_type() {
    return make_nullable(std::make_shared<DataTypeInt32>());
}

DataTypePtr nullable_int64_target_type() {
    return make_nullable(std::make_shared<DataTypeInt64>());
}

void disable_bkd_skip_for_index_probe_assertions(IndexReadOptions* read_options) {
    // These cases assert the field-pattern index binding all the way to an APPLIED
    // probe. Tiny three-row segments can hit the default 50% BKD skip threshold first
    // and report a valid INVERTED_INDEX_BYPASS fallback instead of an applied probe.
    read_options->inverted_index_skip_threshold = 100;
}

VariantColumnSpec typed_pattern_variant_column() {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.max_subcolumns_count = 2;
    variant.predefined_paths = {
            VariantPathSpec {.path = "int_*",
                             .type = FieldType::OLAP_FIELD_TYPE_INT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                             .array_item_type = {},
                             .array_item_nullable = true},
            VariantPathSpec {.path = "string_*",
                             .type = FieldType::OLAP_FIELD_TYPE_STRING,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };
    return variant;
}

VariantColumnSpec multi_typed_pattern_variant_column() {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.max_subcolumns_count = 6;
    variant.predefined_paths = {
            VariantPathSpec {.path = "big_*",
                             .type = FieldType::OLAP_FIELD_TYPE_BIGINT,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                             .array_item_type = {},
                             .array_item_nullable = true},
            VariantPathSpec {.path = "double_*",
                             .type = FieldType::OLAP_FIELD_TYPE_DOUBLE,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                             .array_item_type = {},
                             .array_item_nullable = true},
            VariantPathSpec {.path = "bool_*",
                             .type = FieldType::OLAP_FIELD_TYPE_BOOL,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };
    return variant;
}

VariantColumnSpec date_time_pattern_variant_column() {
    VariantColumnSpec variant;
    variant.unique_id = kVariantUid;
    variant.name = "v";
    variant.max_subcolumns_count = 4;
    variant.predefined_paths = {
            VariantPathSpec {.path = "date_*",
                             .type = FieldType::OLAP_FIELD_TYPE_DATEV2,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                             .array_item_type = {},
                             .array_item_nullable = true},
            VariantPathSpec {.path = "datetime_*",
                             .type = FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
                             .nullable = true,
                             .pattern_type = PatternTypePB::MATCH_NAME_GLOB,
                             .array_item_type = {},
                             .array_item_nullable = true},
    };
    return variant;
}

Field date_v2_field(int64_t yyyymmdd) {
    DateV2Value<DateV2ValueType> value;
    value.from_date_int64(yyyymmdd);
    return Field::create_field<TYPE_DATEV2>(value);
}

Field datetime_v2_field(int64_t yyyymmddhhmmss) {
    DateV2Value<DateTimeV2ValueType> value;
    value.from_date_int64(yyyymmddhhmmss);
    return Field::create_field<TYPE_DATETIMEV2>(value);
}

std::vector<std::string> split_int_variant_rows(size_t low_rows, int32_t low_value,
                                                size_t high_rows, int32_t high_value) {
    // Keep both ranges in one data source. The fixture flushes the rowset writer after
    // each data source, so splitting low/high ranges into separate sources creates two
    // segments and lets segment ZoneMap prune before page ZoneMap or BloomFilter can run.
    std::vector<std::string> rows;
    rows.reserve(low_rows + high_rows);
    for (size_t i = 0; i < low_rows; ++i) {
        rows.push_back(R"({"int_1": )" + std::to_string(low_value) + "}");
    }
    for (size_t i = 0; i < high_rows; ++i) {
        rows.push_back(R"({"int_1": )" + std::to_string(high_value) + "}");
    }
    return rows;
}

std::vector<std::string> interleaved_int_variant_rows(size_t pairs, int32_t low_value,
                                                      int32_t high_value) {
    // BF assertions must keep both segment and page ZoneMaps matched. Alternating values keeps each
    // page range wide enough that ZoneMap cannot prune before the BloomFilter reader is exercised.
    std::vector<std::string> rows;
    rows.reserve(pairs * 2);
    for (size_t i = 0; i < pairs; ++i) {
        rows.push_back(R"({"int_1": )" + std::to_string(low_value) + "}");
        rows.push_back(R"({"int_1": )" + std::to_string(high_value) + "}");
    }
    return rows;
}

void expect_int_index_probe_count(const IndexReadResult& result, int64_t expected_count) {
    expect_index_probe_count(result,
                             IndexProbeExpectation {
                                     .source = IndexProbeSource::COLUMN_PREDICATE,
                                     .state = IndexProbeState::APPLIED,
                                     .reason = IndexFallbackReason::NONE,
                                     .column_uid = kVariantUid,
                                     .variant_path = std::string(kIntPath),
                                     .index_id = kIntPatternIndexId,
                                     .counts_toward_filter_stats = true,
                                     .filtered_rows = {},
                             },
                             expected_count);
}

} // namespace

class IndexStorageVariantFieldPatternIndexTest : public IndexStorageTestFixture {
protected:
    void run_typed_int_field_pattern_index_lifecycle(IndexCompactionKind compaction_kind,
                                                     int64_t tablet_id);
};

void IndexStorageVariantFieldPatternIndexTest::run_typed_int_field_pattern_index_lifecycle(
        IndexCompactionKind compaction_kind, int64_t tablet_id) {
    const auto int_index = IndexSpec::field_pattern_index(kIntPatternIndexId, "idx_v_int_glob",
                                                          kVariantUid, "int_*");
    const auto string_index = IndexSpec::field_pattern_index(
            kStringPatternIndexId, "idx_v_string_glob", kVariantUid, "string_*");
    const auto index_case = IndexStorageCaseBuilder("typed_int_field_pattern_index_lifecycle")
                                    .tablet_id(tablet_id)
                                    .variant_column(typed_pattern_variant_column())
                                    .inverted_index(int_index)
                                    .inverted_index(string_index)
                                    .rowset(0, IndexDataSourceSpec::inline_variant(
                                                       {R"({"int_1": 42, "string_1": "sample"})",
                                                        R"({"int_1": 7, "string_1": "other"})"},
                                                       0))
                                    .rowset(1, IndexDataSourceSpec::inline_variant(
                                                       {R"({"int_1": 42, "string_1": "sample"})",
                                                        R"({"int_1": 8, "string_1": "other"})"},
                                                       100))
                                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.int_1");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_int32_target_type();
    read_options.predicates.push_back(int_equals(path_column_id, path_column.name(), 42));

    auto before_compaction = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(before_compaction.has_value()) << before_compaction.error();
    EXPECT_EQ(before_compaction->rows_read, 2);
    expect_applied_variant_path_index(before_compaction.value(), kIntPath, kIntPatternIndexId, 2);
    expect_int_index_probe_count(before_compaction.value(), 2);
    expect_index_not_applied(before_compaction.value(), kStringPatternIndexId);

    IndexReadOptions range_read_options;
    range_read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    range_read_options.target_cast_type_for_variants[path_column.name()] =
            nullable_int32_target_type();
    range_read_options.predicates.push_back(int_greater(path_column_id, path_column.name(), 10));

    auto range_before_compaction = read_rowsets(readable_rowsets.value(), range_read_options);
    ASSERT_TRUE(range_before_compaction.has_value()) << range_before_compaction.error();
    EXPECT_EQ(range_before_compaction->rows_read, 2);
    expect_applied_variant_path_index(range_before_compaction.value(), kIntPath, kIntPatternIndexId,
                                      2);
    expect_int_index_probe_count(range_before_compaction.value(), 2);
    expect_index_not_applied(range_before_compaction.value(), kStringPatternIndexId);

    auto compacted = compact_rowsets(compaction_kind, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    EXPECT_EQ(compacted.value()->num_rows(), 4);

    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();
    auto readable_compacted = rowsets_with_variant_extended_schema(reloaded.value());
    ASSERT_TRUE(readable_compacted.has_value()) << readable_compacted.error();
    const int32_t compacted_path_column_id = column_id_by_path("v.int_1");
    ASSERT_EQ(compacted_path_column_id, path_column_id);

    auto after_compaction = read_rowsets(readable_compacted.value(), read_options);
    ASSERT_TRUE(after_compaction.has_value()) << after_compaction.error();
    EXPECT_EQ(after_compaction->rows_read, 2);
    expect_applied_variant_path_index(after_compaction.value(), kIntPath, kIntPatternIndexId, 2);
    expect_int_index_probe_count(after_compaction.value(), 1);
    expect_index_not_applied(after_compaction.value(), kStringPatternIndexId);

    auto range_after_compaction = read_rowsets(readable_compacted.value(), range_read_options);
    ASSERT_TRUE(range_after_compaction.has_value()) << range_after_compaction.error();
    EXPECT_EQ(range_after_compaction->rows_read, 2);
    expect_applied_variant_path_index(range_after_compaction.value(), kIntPath, kIntPatternIndexId,
                                      2);
    expect_int_index_probe_count(range_after_compaction.value(), 1);
    expect_index_not_applied(range_after_compaction.value(), kStringPatternIndexId);
}

TEST_F(IndexStorageVariantFieldPatternIndexTest, TypedIntIndexAfterCumulativeCompaction) {
    run_typed_int_field_pattern_index_lifecycle(IndexCompactionKind::CUMULATIVE, 110032);
}

TEST_F(IndexStorageVariantFieldPatternIndexTest, TypedIntIndexAfterFullCompaction) {
    run_typed_int_field_pattern_index_lifecycle(IndexCompactionKind::FULL, 110033);
}

// Non-INT typed Variant field-pattern indexes should bind to the matching path and report the
// applied probe metadata for each physical storage type.
TEST_F(IndexStorageVariantFieldPatternIndexTest,
       BigIntDoubleAndBoolFieldPatternIndexesUseExpectedPathAndIndexIds) {
    const auto index_case =
            IndexStorageCaseBuilder("variant_multi_typed_field_pattern_index_matrix")
                    .tablet_id(110040)
                    .variant_column(multi_typed_pattern_variant_column())
                    .inverted_index(IndexSpec::field_pattern_index(
                            kBigIntPatternIndexId, "idx_v_big_glob", kVariantUid, "big_*"))
                    .inverted_index(IndexSpec::field_pattern_index(
                            kDoublePatternIndexId, "idx_v_double_glob", kVariantUid, "double_*"))
                    .inverted_index(IndexSpec::field_pattern_index(
                            kBoolPatternIndexId, "idx_v_bool_glob", kVariantUid, "bool_*"))
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"big_1": 9000000000, "double_1": 3.5, "bool_1": true})",
                                     R"({"big_1": 7, "double_1": 1.25, "bool_1": false})",
                                     R"({"big_1": 9000000000, "double_1": 7.5, "bool_1": true})"},
                                    0))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();

    auto read_and_verify = [&](std::string_view path, FieldType expected_storage_type,
                               DataTypePtr data_type, Field value, int64_t index_id,
                               int64_t expected_rows, int64_t expected_filtered_rows) {
        const int32_t path_column_id = column_id_by_path("v." + std::string(path));
        ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
        const auto& path_column = tablet_schema()->column(path_column_id);
        EXPECT_EQ(path_column.type(), expected_storage_type)
                << "unexpected storage type for " << path_column.name();
        EXPECT_EQ(data_type->get_storage_field_type(), expected_storage_type)
                << "unexpected predicate type for " << path_column.name();

        IndexReadOptions read_options;
        read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
        read_options.target_cast_type_for_variants[path_column.name()] =
                nullable_target_type(data_type);
        disable_bkd_skip_for_index_probe_assertions(&read_options);
        read_options.predicates.push_back(
                typed_equals(path_column_id, path_column.name(), data_type, value));

        auto read_result = read_rowsets(readable_rowsets.value(), read_options);
        ASSERT_TRUE(read_result.has_value()) << read_result.error();
        EXPECT_EQ(read_result->rows_read, expected_rows);
        expect_applied_variant_path_index(read_result.value(), path, index_id,
                                          expected_filtered_rows);
        expect_index_probe_count(read_result.value(),
                                 IndexProbeExpectation {
                                         .source = IndexProbeSource::COLUMN_PREDICATE,
                                         .state = IndexProbeState::APPLIED,
                                         .reason = IndexFallbackReason::NONE,
                                         .column_uid = kVariantUid,
                                         .variant_path = std::string(path),
                                         .index_id = index_id,
                                         .segment_id = 0,
                                         .counts_toward_filter_stats = true,
                                         .input_rows = 3,
                                         .output_rows = expected_rows,
                                         .filtered_rows = expected_filtered_rows,
                                 },
                                 1);
        expect_index_not_applied(read_result.value(), kStringPatternIndexId);
    };

    read_and_verify("big_1", FieldType::OLAP_FIELD_TYPE_BIGINT, std::make_shared<DataTypeInt64>(),
                    Field::create_field<TYPE_BIGINT>(Int64(9000000000LL)), kBigIntPatternIndexId, 2,
                    1);
    read_and_verify("double_1", FieldType::OLAP_FIELD_TYPE_DOUBLE,
                    std::make_shared<DataTypeFloat64>(),
                    Field::create_field<TYPE_DOUBLE>(Float64(3.5)), kDoublePatternIndexId, 1, 2);
    read_and_verify("bool_1", FieldType::OLAP_FIELD_TYPE_BOOL, std::make_shared<DataTypeBool>(),
                    Field::create_field<TYPE_BOOLEAN>(UInt8(1)), kBoolPatternIndexId, 2, 1);
}

// DATEV2/DATETIMEV2 Variant field-pattern indexes should bind to the matching path and report the
// expected applied probe metadata.
TEST_F(IndexStorageVariantFieldPatternIndexTest,
       DateAndDateTimeFieldPatternIndexesUseExpectedPathAndIndexIds) {
    const auto index_case =
            IndexStorageCaseBuilder("variant_date_time_field_pattern_index_matrix")
                    .tablet_id(110044)
                    .variant_column(date_time_pattern_variant_column())
                    .inverted_index(IndexSpec::field_pattern_index(
                            kDatePatternIndexId, "idx_v_date_glob", kVariantUid, "date_*"))
                    .inverted_index(IndexSpec::field_pattern_index(kDateTimePatternIndexId,
                                                                   "idx_v_datetime_glob",
                                                                   kVariantUid, "datetime_*"))
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"date_1": "2024-01-02", "datetime_1": "2024-01-02 03:04:05"})",
                                     R"({"date_1": "2024-01-03", "datetime_1": "2024-01-03 03:04:05"})",
                                     R"({"date_1": "2024-01-02", "datetime_1": "2024-01-02 03:04:05"})"},
                                    0))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();

    auto read_and_verify = [&](std::string_view path, DataTypePtr data_type, Field value,
                               int64_t index_id, int64_t expected_rows,
                               int64_t expected_filtered_rows) {
        const int32_t path_column_id = column_id_by_path("v." + std::string(path));
        ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
        const auto& path_column = tablet_schema()->column(path_column_id);

        IndexReadOptions read_options;
        read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
        read_options.target_cast_type_for_variants[path_column.name()] =
                nullable_target_type(data_type);
        disable_bkd_skip_for_index_probe_assertions(&read_options);
        read_options.predicates.push_back(
                typed_equals(path_column_id, path_column.name(), data_type, value));

        auto read_result = read_rowsets(readable_rowsets.value(), read_options);
        ASSERT_TRUE(read_result.has_value()) << read_result.error();
        EXPECT_EQ(read_result->rows_read, expected_rows);
        expect_applied_variant_path_index(read_result.value(), path, index_id,
                                          expected_filtered_rows);
        expect_index_probe_count(read_result.value(),
                                 IndexProbeExpectation {
                                         .source = IndexProbeSource::COLUMN_PREDICATE,
                                         .state = IndexProbeState::APPLIED,
                                         .reason = IndexFallbackReason::NONE,
                                         .column_uid = kVariantUid,
                                         .variant_path = std::string(path),
                                         .index_id = index_id,
                                         .segment_id = 0,
                                         .counts_toward_filter_stats = true,
                                         .input_rows = 3,
                                         .output_rows = expected_rows,
                                         .filtered_rows = expected_filtered_rows,
                                 },
                                 1);
    };

    read_and_verify("date_1", std::make_shared<DataTypeDateV2>(), date_v2_field(20240102),
                    kDatePatternIndexId, 2, 1);
    read_and_verify("datetime_1", std::make_shared<DataTypeDateTimeV2>(),
                    datetime_v2_field(20240102030405), kDateTimePatternIndexId, 2, 1);
}

TEST_F(IndexStorageVariantFieldPatternIndexTest, TypedVariantPathSegmentZoneMapPrunesWholeSegment) {
    const auto index_case = IndexStorageCaseBuilder("variant_typed_path_segment_zone_map_prune")
                                    .tablet_id(110041)
                                    .variant_column(typed_pattern_variant_column())
                                    .rowset(0,
                                            IndexDataSourceSpec::inline_variant(
                                                    {R"({"int_1": 1})", R"({"int_1": 2})",
                                                     R"({"int_1": 100})", R"({"int_1": 101})"},
                                                    0),
                                            2)
                                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.int_1");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.enable_inverted_index_query = false;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_int32_target_type();
    read_options.predicates.push_back(int_greater(path_column_id, path_column.name(), 50));

    auto read_result = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 2);
    expect_raw_rows_read(read_result.value(), 2);
    expect_segment_pruned(read_result.value(), 1);
    expect_inverted_index_not_attempted(read_result.value());
}

TEST_F(IndexStorageVariantFieldPatternIndexTest, TypedVariantPathPageZoneMapPrunesWithinSegment) {
    constexpr size_t kLowRows = 2048;
    constexpr size_t kHighRows = 2048;
    auto variant = typed_pattern_variant_column();

    IndexTabletOptions options;
    options.tablet_id = 110045;
    options.storage_page_size = 4096;
    options.variant_columns.push_back(std::move(variant));

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.max_rows_per_segment = static_cast<int64_t>(kLowRows + kHighRows);
    rowset.data_sources.push_back(IndexDataSourceSpec::inline_variant(
            split_int_variant_rows(kLowRows, 1, kHighRows, 100), 0));

    ASSERT_TRUE(create_tablet(options).ok());
    auto rowsets = write_rowsets({rowset});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    // The initial flush can pack all 4096 values into a single data page whose ZoneMap is [1, 100].
    // Verify page-level pruning only on the compacted rowset, where low/high values split by page.
    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(reloaded.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.int_1");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.enable_inverted_index_query = false;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_int32_target_type();
    read_options.predicates.push_back(int_greater(path_column_id, path_column.name(), 50));

    auto read_result = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, kHighRows);
    expect_segment_pruned(read_result.value(), 0);
    expect_raw_rows_read(read_result.value(), kHighRows);
    expect_zone_map_filtered(read_result.value(), kLowRows);
    expect_inverted_index_not_attempted(read_result.value());
}

TEST_F(IndexStorageVariantFieldPatternIndexTest,
       DisabledInvertedIndexQueryDoesNotProbeVariantFieldPatternIndex) {
    const auto int_index = IndexSpec::field_pattern_index(kIntPatternIndexId, "idx_v_int_glob",
                                                          kVariantUid, "int_*");
    const auto index_case =
            IndexStorageCaseBuilder("variant_disabled_inverted_index_query_reverse_case")
                    .tablet_id(110046)
                    .variant_column(typed_pattern_variant_column())
                    .inverted_index(int_index)
                    .rowset(0, IndexDataSourceSpec::inline_variant(
                                       {R"({"int_1": 42})", R"({"int_1": 7})", R"({"int_1": 42})",
                                        R"({"int_1": 8})"},
                                       0))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.int_1");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.enable_inverted_index_query = false;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_int32_target_type();
    read_options.predicates.push_back(int_equals(path_column_id, path_column.name(), 42));

    auto read_result = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 2);
    expect_index_filter_stats(read_result.value(), 0);
    expect_inverted_index_not_attempted(read_result.value());
}

TEST_F(IndexStorageVariantFieldPatternIndexTest,
       TargetCastTypeMismatchSkipsVariantPathZoneMapPruning) {
    const auto index_case =
            IndexStorageCaseBuilder("variant_unsafe_target_cast_zone_map_reverse_case")
                    .tablet_id(110047)
                    .variant_column(typed_pattern_variant_column())
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"int_1": 1})", R"({"int_1": 2})", R"({"int_1": 100})",
                                     R"({"int_1": 101})"},
                                    0),
                            2)
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.int_1");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.enable_inverted_index_query = false;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    // Keep nullability aligned with Variant typed paths so this case checks the stored
    // physical type mismatch, not a nullable/non-nullable wrapper mismatch.
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_int64_target_type();
    read_options.predicates.push_back(int_greater(path_column_id, path_column.name(), 50));

    auto read_result = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    // The target cast type intentionally differs from the stored typed path. Storage-level
    // predicate evaluation must be skipped entirely; residual expression filtering happens above
    // this helper in real query execution.
    EXPECT_EQ(read_result->rows_read, 4);
    expect_raw_rows_read(read_result.value(), 4);
    expect_segment_pruned(read_result.value(), 0);
    expect_zone_map_filtered(read_result.value(), 0);
    expect_inverted_index_not_attempted(read_result.value());
}

// Expected-red: this is separate from the DORIS-26471 BF case fix. Segment-level typed-path ZoneMap
// pruning works, but rows_stats_filtered is still not updated on this path.
TEST_F(IndexStorageVariantFieldPatternIndexTest,
       TypedVariantPathZoneMapRowsStatsFilteredCountsPrunedRows) {
    const auto index_case =
            IndexStorageCaseBuilder("variant_typed_path_segment_zone_map_rows_stats")
                    .tablet_id(110042)
                    .variant_column(typed_pattern_variant_column())
                    .rowset(0,
                            IndexDataSourceSpec::inline_variant(
                                    {R"({"int_1": 1})", R"({"int_1": 2})", R"({"int_1": 100})",
                                     R"({"int_1": 101})"},
                                    0),
                            2)
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.int_1");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.enable_inverted_index_query = false;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_int32_target_type();
    read_options.predicates.push_back(int_greater(path_column_id, path_column.name(), 50));

    auto read_result = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 2);
    expect_raw_rows_read(read_result.value(), 2);
    expect_segment_pruned(read_result.value(), 1);
    expect_zone_map_filtered(read_result.value(), 2);
    expect_inverted_index_not_attempted(read_result.value());
}

TEST_F(IndexStorageVariantFieldPatternIndexTest,
       TypedVariantPathPageZoneMapRowsStatsRpFilteredCountsPrunedRows) {
    constexpr size_t kLowRows = 2048;
    constexpr size_t kHighRows = 2048;

    IndexTabletOptions options;
    options.tablet_id = 110048;
    options.storage_page_size = 4096;
    options.variant_columns.push_back(typed_pattern_variant_column());

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.max_rows_per_segment = static_cast<int64_t>(kLowRows + kHighRows);
    rowset.data_sources.push_back(IndexDataSourceSpec::inline_variant(
            split_int_variant_rows(kLowRows, 1, kHighRows, 100), 0));

    ASSERT_TRUE(create_tablet(options).ok());
    auto rowsets = write_rowsets({rowset});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    // The initial flush can keep all 4096 values in a single data page whose ZoneMap is [1, 100].
    // Use a compacted rowset so low/high values are split across pages and page pruning is testable.
    auto compacted = compact_rowsets(IndexCompactionKind::CUMULATIVE, rowsets.value());
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    auto reloaded = reload_rowsets({compacted.value()});
    ASSERT_TRUE(reloaded.has_value()) << reloaded.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(reloaded.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.int_1");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.enable_inverted_index_query = false;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_int32_target_type();
    read_options.predicates.push_back(int_greater(path_column_id, path_column.name(), 50));

    auto read_result = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, kHighRows);
    expect_segment_pruned(read_result.value(), 0);
    expect_zone_map_filtered(read_result.value(), kLowRows);
    EXPECT_EQ(read_result->stats.rows_stats_rp_filtered, kLowRows);
    expect_inverted_index_not_attempted(read_result.value());
}

// Variant typed-path BloomFilter is inherited from the parent Variant column and filters equality
// misses before ZoneMap pruning.
TEST_F(IndexStorageVariantFieldPatternIndexTest, VariantTypedPathBloomFilterFiltersMissingValue) {
    constexpr size_t kLowRows = 2048;
    constexpr size_t kHighRows = 2048;
    constexpr int32_t kMissingValueInsideSegmentZoneMap = 50;
    auto variant = typed_pattern_variant_column();
    variant.is_bf_column = true;

    IndexTabletOptions options;
    options.tablet_id = 110049;
    options.storage_page_size = 4096;
    options.variant_columns.push_back(std::move(variant));

    IndexRowsetSpec rowset;
    rowset.version = 0;
    rowset.max_rows_per_segment = static_cast<int64_t>(kLowRows + kHighRows);
    rowset.data_sources.push_back(
            IndexDataSourceSpec::inline_variant(interleaved_int_variant_rows(kLowRows, 1, 100), 0));

    ASSERT_TRUE(create_tablet(options).ok());
    auto rowsets = write_rowsets({rowset});
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.int_1");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.enable_inverted_index_query = false;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    // Variant extracted paths are nullable in the extended schema. Keep the target cast nullable so
    // can_apply_predicate_safely allows this case to exercise the BloomFilter reader.
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_int32_target_type();
    read_options.predicates.push_back(
            int_equals(path_column_id, path_column.name(), kMissingValueInsideSegmentZoneMap));

    ScopedDebugPoint must_filter("bloom_filter_must_filter_data");
    auto read_result = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 0);
    expect_bloom_filter_filtered(read_result.value(), kLowRows + kHighRows);
    expect_inverted_index_not_attempted(read_result.value());
}

// Expected-red: once a sparse Variant child has sparse statistics but no materialized subcolumn, an
// exact typed-path predicate should still be able to use those stats for pushdown observability.
TEST_F(IndexStorageVariantFieldPatternIndexTest,
       DISABLED_SparseChildPathStatsCanDriveTypedPathPushdown) {
    auto variant = typed_pattern_variant_column();
    variant.max_subcolumns_count = 1;
    variant.max_sparse_column_statistics_size = 1;

    const auto index_case =
            IndexStorageCaseBuilder("variant_sparse_child_path_stats_pushdown")
                    .tablet_id(110050)
                    .variant_column(std::move(variant))
                    .rowset(0, IndexDataSourceSpec::inline_variant(
                                       {R"({"int_1": 1, "int_rare": 999})", R"({"int_1": 2})",
                                        R"({"int_1": 3})", R"({"int_1": 4})"},
                                       0))
                    .build();
    ASSERT_TRUE(create_tablet(index_case.tablet_options).ok());
    auto rowsets = write_rowsets(index_case.rowsets);
    ASSERT_TRUE(rowsets.has_value()) << rowsets.error();

    auto readable_rowsets = rowsets_with_variant_extended_schema(rowsets.value());
    ASSERT_TRUE(readable_rowsets.has_value()) << readable_rowsets.error();
    const int32_t path_column_id = column_id_by_path("v.int_rare");
    ASSERT_GE(path_column_id, 0) << dump_schema_paths(*tablet_schema());
    const auto& path_column = tablet_schema()->column(path_column_id);

    IndexReadOptions read_options;
    read_options.enable_inverted_index_query = false;
    read_options.return_columns = {0, static_cast<uint32_t>(path_column_id)};
    read_options.target_cast_type_for_variants[path_column.name()] = nullable_int32_target_type();
    read_options.predicates.push_back(int_equals(path_column_id, path_column.name(), 999));

    auto read_result = read_rowsets(readable_rowsets.value(), read_options);
    ASSERT_TRUE(read_result.has_value()) << read_result.error();
    EXPECT_EQ(read_result->rows_read, 1);
    EXPECT_GT(read_result->stats.rows_stats_filtered, 0);
    expect_inverted_index_not_attempted(read_result.value());
}

} // namespace doris::index_storage_test
