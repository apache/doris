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

#include "format_v2/parquet/parquet_statistics.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <cctz/time_zone.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/bloom_filter.h>

#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "storage/predicate/accept_null_predicate.h"
#include "storage/predicate/null_predicate.h"
#include "storage/predicate/predicate_creator.h"

namespace doris {
namespace {

format::parquet::ParquetColumnSchema primitive_bloom_schema(const DataTypePtr& type) {
    format::parquet::ParquetColumnSchema schema;
    schema.local_id = 0;
    schema.name = "c0";
    schema.type = type;
    schema.leaf_column_id = 0;
    schema.kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
    return schema;
}

format::FileColumnPredicateFilter bloom_filter_with_predicate(
        const std::shared_ptr<ColumnPredicate>& predicate) {
    format::FileColumnPredicateFilter filter;
    filter.file_column_id = format::LocalColumnId(0);
    filter.predicates.push_back(predicate);
    return filter;
}

std::shared_ptr<arrow::Array> finish_array(arrow::ArrayBuilder* builder) {
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder->Finish(&array).ok());
    return array;
}

std::shared_ptr<arrow::Array> int32_array(const std::vector<std::optional<int32_t>>& values) {
    arrow::Int32Builder builder;
    for (const auto& value : values) {
        if (value.has_value()) {
            EXPECT_TRUE(builder.Append(*value).ok());
        } else {
            EXPECT_TRUE(builder.AppendNull().ok());
        }
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> uint32_array(const std::vector<uint32_t>& values) {
    arrow::UInt32Builder builder;
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> string_array(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    for (const auto& value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> timestamp_array(const std::vector<int64_t>& values) {
    arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                                    arrow::default_memory_pool());
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

std::unique_ptr<::parquet::ParquetFileReader> make_reader(
        const std::shared_ptr<arrow::Table>& table, int64_t row_group_size, bool enable_dictionary,
        bool enable_statistics) {
    auto out_result = arrow::io::BufferOutputStream::Create();
    EXPECT_TRUE(out_result.ok());
    auto out = *out_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    if (enable_dictionary) {
        builder.enable_dictionary();
    } else {
        builder.disable_dictionary();
    }
    if (!enable_statistics) {
        builder.disable_statistics();
    }
    EXPECT_TRUE(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                             row_group_size, builder.build())
                        .ok());
    auto buffer_result = out->Finish();
    EXPECT_TRUE(buffer_result.ok());
    return ::parquet::ParquetFileReader::Open(
            std::make_shared<arrow::io::BufferReader>(*buffer_result));
}

std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> build_file_schema(
        const ::parquet::ParquetFileReader& reader) {
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> file_schema;
    EXPECT_TRUE(
            format::parquet::build_parquet_column_schema(*reader.metadata()->schema(), &file_schema)
                    .ok());
    return file_schema;
}

format::FileScanRequest request_with_filter(format::FileColumnPredicateFilter filter) {
    format::FileScanRequest request;
    request.column_predicate_filters.push_back(std::move(filter));
    return request;
}

::parquet::BlockSplitBloomFilter bloom_filter_for_int32_values(const std::vector<int32_t>& values) {
    ::parquet::BlockSplitBloomFilter bloom_filter;
    bloom_filter.Init(::parquet::BlockSplitBloomFilter::kMinimumBloomFilterBytes);
    for (const auto value : values) {
        bloom_filter.InsertHash(bloom_filter.Hash(value));
    }
    return bloom_filter;
}

TEST(ParquetStatisticsTransformTest, ConvertsMinMaxNullCountUnsignedStringAndTimestamp) {
    auto table = arrow::Table::Make(
            arrow::schema({
                    arrow::field("i", arrow::int32(), true),
                    arrow::field("u", arrow::uint32(), false),
                    arrow::field("s", arrow::utf8(), false),
                    arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), false),
            }),
            {int32_array({1, std::nullopt, 5}), uint32_array({7, 9, 11}),
             string_array({"alpha", "beta", "omega"}), timestamp_array({1000, 2000, 3000})});
    auto reader = make_reader(table, 3, false, true);
    auto schema = build_file_schema(*reader);
    auto row_group = reader->metadata()->RowGroup(0);

    const auto int_stats = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *schema[0], row_group->ColumnChunk(0)->statistics());
    EXPECT_TRUE(int_stats.has_min_max);
    EXPECT_TRUE(int_stats.has_null_count);
    EXPECT_TRUE(int_stats.has_null);
    EXPECT_TRUE(int_stats.has_not_null);
    EXPECT_EQ(int_stats.min_value.get<TYPE_INT>(), 1);
    EXPECT_EQ(int_stats.max_value.get<TYPE_INT>(), 5);

    const auto uint_stats = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *schema[1], row_group->ColumnChunk(1)->statistics());
    EXPECT_TRUE(uint_stats.has_min_max);
    EXPECT_EQ(uint_stats.min_value.get<TYPE_BIGINT>(), 7);
    EXPECT_EQ(uint_stats.max_value.get<TYPE_BIGINT>(), 11);

    const auto string_stats = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *schema[2], row_group->ColumnChunk(2)->statistics());
    EXPECT_TRUE(string_stats.has_min_max);
    EXPECT_EQ(string_stats.min_value.get<TYPE_STRING>(), "alpha");
    EXPECT_EQ(string_stats.max_value.get<TYPE_STRING>(), "omega");

    auto utc = cctz::utc_time_zone();
    const auto timestamp_stats = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *schema[3], row_group->ColumnChunk(3)->statistics(), &utc);
    EXPECT_TRUE(timestamp_stats.has_min_max);
    EXPECT_EQ(timestamp_stats.min_value.get_type(), TYPE_DATETIMEV2);
    EXPECT_EQ(timestamp_stats.max_value.get_type(), TYPE_DATETIMEV2);
    EXPECT_LT(timestamp_stats.min_value, timestamp_stats.max_value);
}

TEST(ParquetStatisticsTransformTest, HandlesMissingStatisticsAndAllNullChunks) {
    auto no_stats_table = arrow::Table::Make(
            arrow::schema({arrow::field("i", arrow::int32(), true)}), {int32_array({1, 2, 3})});
    auto no_stats_reader = make_reader(no_stats_table, 3, false, false);
    auto no_stats_schema = build_file_schema(*no_stats_reader);
    auto no_stats = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *no_stats_schema[0],
            no_stats_reader->metadata()->RowGroup(0)->ColumnChunk(0)->statistics());
    EXPECT_FALSE(no_stats.has_min_max);

    auto all_null_table =
            arrow::Table::Make(arrow::schema({arrow::field("i", arrow::int32(), true)}),
                               {int32_array({std::nullopt, std::nullopt})});
    auto all_null_reader = make_reader(all_null_table, 2, false, true);
    auto all_null_schema = build_file_schema(*all_null_reader);
    auto all_null_stats = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *all_null_schema[0],
            all_null_reader->metadata()->RowGroup(0)->ColumnChunk(0)->statistics());
    EXPECT_TRUE(all_null_stats.has_null_count);
    EXPECT_TRUE(all_null_stats.has_null);
    EXPECT_FALSE(all_null_stats.has_not_null);
    EXPECT_FALSE(all_null_stats.has_min_max);
}

TEST(ParquetStatisticsPruningTest, StatisticsPredicatesAndNullPredicatesPruneRowGroups) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("i", arrow::int32(), true)}),
                                    {int32_array({std::nullopt, std::nullopt, 3, 4, 5, 6})});
    auto reader = make_reader(table, 2, false, true);
    auto schema = build_file_schema(*reader);

    format::FileColumnPredicateFilter ge_filter;
    ge_filter.file_column_id = format::LocalColumnId(0);
    ge_filter.predicates.push_back(create_comparison_predicate<PredicateType::GE>(
            0, "i", schema[0]->type, Field::create_field<TYPE_INT>(5), false));
    std::vector<int> selected;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_statistics(
                        *reader->metadata(), reader.get(), schema, request_with_filter(ge_filter),
                        nullptr, &selected, false, &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({2}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_statistics, 2);

    format::FileColumnPredicateFilter is_not_null_filter;
    is_not_null_filter.file_column_id = format::LocalColumnId(0);
    is_not_null_filter.predicates.push_back(
            std::make_shared<NullPredicate>(0, "i", false, TYPE_INT));
    selected.clear();
    ASSERT_TRUE(format::parquet::select_row_groups_by_statistics(
                        *reader->metadata(), reader.get(), schema,
                        request_with_filter(is_not_null_filter), nullptr, &selected, false,
                        &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({1, 2}));

    format::FileColumnPredicateFilter is_null_filter;
    is_null_filter.file_column_id = format::LocalColumnId(0);
    is_null_filter.predicates.push_back(std::make_shared<NullPredicate>(0, "i", true, TYPE_INT));
    selected.clear();
    ASSERT_TRUE(format::parquet::select_row_groups_by_statistics(
                        *reader->metadata(), reader.get(), schema,
                        request_with_filter(is_null_filter), nullptr, &selected, false,
                        &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({0}));
}

TEST(ParquetStatisticsPruningTest, DictionaryPruningHandlesExcludeIncludeAndUnsupportedPaths) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("s", arrow::utf8(), false)}),
                                    {string_array({"alpha", "beta", "gamma", "omega"})});
    auto reader = make_reader(table, 2, true, false);
    auto schema = build_file_schema(*reader);

    format::FileColumnPredicateFilter absent_filter;
    absent_filter.file_column_id = format::LocalColumnId(0);
    absent_filter.predicates.push_back(create_comparison_predicate<PredicateType::EQ>(
            0, "s", schema[0]->type, Field::create_field<TYPE_STRING>("missing"), false));
    std::vector<int> selected;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_statistics(
                        *reader->metadata(), reader.get(), schema,
                        request_with_filter(absent_filter), nullptr, &selected, false,
                        &pruning_stats)
                        .ok());
    EXPECT_TRUE(selected.empty());
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_dictionary, 2);

    format::FileColumnPredicateFilter present_filter;
    present_filter.file_column_id = format::LocalColumnId(0);
    present_filter.predicates.push_back(create_comparison_predicate<PredicateType::EQ>(
            0, "s", schema[0]->type, Field::create_field<TYPE_STRING>("gamma"), false));
    selected.clear();
    pruning_stats = {};
    ASSERT_TRUE(format::parquet::select_row_groups_by_statistics(
                        *reader->metadata(), reader.get(), schema,
                        request_with_filter(present_filter), nullptr, &selected, false,
                        &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({1}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_dictionary, 1);

    auto plain_reader = make_reader(table, 2, false, false);
    auto plain_schema = build_file_schema(*plain_reader);
    selected.clear();
    pruning_stats = {};
    ASSERT_TRUE(format::parquet::select_row_groups_by_statistics(
                        *plain_reader->metadata(), plain_reader.get(), plain_schema,
                        request_with_filter(absent_filter), nullptr, &selected, false,
                        &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({0, 1}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_dictionary, 0);
}

TEST(ParquetStatisticsPruningTest, StatisticsRunsBeforeDictionaryAndMissingBloomKeepsRows) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("s", arrow::utf8(), false)}),
                                    {string_array({"alpha", "beta", "gamma", "omega"})});
    auto reader = make_reader(table, 2, true, true);
    auto schema = build_file_schema(*reader);

    format::FileColumnPredicateFilter beyond_max_filter;
    beyond_max_filter.file_column_id = format::LocalColumnId(0);
    beyond_max_filter.predicates.push_back(create_comparison_predicate<PredicateType::GT>(
            0, "s", schema[0]->type, Field::create_field<TYPE_STRING>("zzzz"), false));
    std::vector<int> selected;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_statistics(
                        *reader->metadata(), reader.get(), schema,
                        request_with_filter(beyond_max_filter), nullptr, &selected, true,
                        &pruning_stats)
                        .ok());
    EXPECT_TRUE(selected.empty());
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_statistics, 2);
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_dictionary, 0);
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_bloom_filter, 0);

    auto no_stats_reader = make_reader(table, 2, false, false);
    auto no_stats_schema = build_file_schema(*no_stats_reader);
    format::FileColumnPredicateFilter missing_bloom_filter;
    missing_bloom_filter.file_column_id = format::LocalColumnId(0);
    missing_bloom_filter.predicates.push_back(create_comparison_predicate<PredicateType::EQ>(
            0, "s", no_stats_schema[0]->type, Field::create_field<TYPE_STRING>("absent"), false));
    selected.clear();
    pruning_stats = {};
    ASSERT_TRUE(format::parquet::select_row_groups_by_statistics(
                        *no_stats_reader->metadata(), no_stats_reader.get(), no_stats_schema,
                        request_with_filter(missing_bloom_filter), nullptr, &selected, true,
                        &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({0, 1}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_bloom_filter, 0);
}

::parquet::BlockSplitBloomFilter bloom_filter_for_string_values(
        const std::vector<std::string>& values) {
    ::parquet::BlockSplitBloomFilter bloom_filter;
    bloom_filter.Init(::parquet::BlockSplitBloomFilter::kMinimumBloomFilterBytes);
    for (const auto& value : values) {
        ::parquet::ByteArray byte_array(static_cast<uint32_t>(value.size()),
                                        reinterpret_cast<const uint8_t*>(value.data()));
        bloom_filter.InsertHash(bloom_filter.Hash(&byte_array));
    }
    return bloom_filter;
}

TEST(ParquetBloomFilterPruningTest, EqPredicateUsesArrowHashAndPrunesAbsentIntValue) {
    auto schema = primitive_bloom_schema(std::make_shared<DataTypeInt32>());
    auto bloom_filter = bloom_filter_for_int32_values({1, 3});
    auto absent_filter = bloom_filter_with_predicate(create_comparison_predicate<PredicateType::EQ>(
            0, "c0", schema.type, Field::create_field<TYPE_INT>(2), false));
    auto present_filter =
            bloom_filter_with_predicate(create_comparison_predicate<PredicateType::EQ>(
                    0, "c0", schema.type, Field::create_field<TYPE_INT>(3), false));

    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(schema, absent_filter,
                                                                             bloom_filter));
    EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(
            schema, present_filter, bloom_filter));
}

TEST(ParquetBloomFilterPruningTest, InPredicatePrunesOnlyWhenAllValuesAreAbsent) {
    auto schema = primitive_bloom_schema(std::make_shared<DataTypeInt32>());
    auto bloom_filter = bloom_filter_for_int32_values({1, 3});

    auto absent_set = build_set<TYPE_INT>();
    int32_t absent_first = 2;
    int32_t absent_second = 4;
    absent_set->insert(&absent_first);
    absent_set->insert(&absent_second);
    auto absent_filter =
            bloom_filter_with_predicate(create_in_list_predicate<PredicateType::IN_LIST>(
                    0, "c0", schema.type, absent_set, false));

    auto present_set = build_set<TYPE_INT>();
    int32_t present_first = 2;
    int32_t present_second = 3;
    present_set->insert(&present_first);
    present_set->insert(&present_second);
    auto present_filter =
            bloom_filter_with_predicate(create_in_list_predicate<PredicateType::IN_LIST>(
                    0, "c0", schema.type, present_set, false));

    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(schema, absent_filter,
                                                                             bloom_filter));
    EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(
            schema, present_filter, bloom_filter));
}

TEST(ParquetBloomFilterPruningTest, BooleanPredicateHashesAsParquetInt32) {
    auto schema = primitive_bloom_schema(std::make_shared<DataTypeBool>());
    auto bloom_filter = bloom_filter_for_int32_values({1});
    auto false_filter = bloom_filter_with_predicate(create_comparison_predicate<PredicateType::EQ>(
            0, "c0", schema.type, Field::create_field<TYPE_BOOLEAN>(false), false));
    auto true_filter = bloom_filter_with_predicate(create_comparison_predicate<PredicateType::EQ>(
            0, "c0", schema.type, Field::create_field<TYPE_BOOLEAN>(true), false));

    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(schema, false_filter,
                                                                             bloom_filter));
    EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(schema, true_filter,
                                                                              bloom_filter));
}

TEST(ParquetBloomFilterPruningTest, StringPredicateUsesArrowByteArrayHash) {
    auto schema = primitive_bloom_schema(std::make_shared<DataTypeString>());
    auto bloom_filter = bloom_filter_for_string_values({"alpha", "omega"});
    auto absent_filter = bloom_filter_with_predicate(create_comparison_predicate<PredicateType::EQ>(
            0, "c0", schema.type, Field::create_field<TYPE_STRING>("beta"), false));
    auto present_filter =
            bloom_filter_with_predicate(create_comparison_predicate<PredicateType::EQ>(
                    0, "c0", schema.type, Field::create_field<TYPE_STRING>("alpha"), false));

    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(schema, absent_filter,
                                                                             bloom_filter));
    EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(
            schema, present_filter, bloom_filter));
}

TEST(ParquetBloomFilterPruningTest, NullableAcceptingAndUnsupportedPredicatesKeepRowGroup) {
    auto schema = primitive_bloom_schema(std::make_shared<DataTypeInt32>());
    auto bloom_filter = bloom_filter_for_int32_values({1});
    auto nested_predicate = create_comparison_predicate<PredicateType::EQ>(
            0, "c0", schema.type, Field::create_field<TYPE_INT>(2), false);
    auto accept_null_filter =
            bloom_filter_with_predicate(std::make_shared<AcceptNullPredicate>(nested_predicate));
    EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(
            schema, accept_null_filter, bloom_filter));

    auto unsupported_schema = primitive_bloom_schema(std::make_shared<DataTypeInt16>());
    auto unsupported_filter =
            bloom_filter_with_predicate(create_comparison_predicate<PredicateType::EQ>(
                    0, "c0", unsupported_schema.type, Field::create_field<TYPE_SMALLINT>(2),
                    false));
    EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(
            unsupported_schema, unsupported_filter, bloom_filter));
}

} // namespace
} // namespace doris
