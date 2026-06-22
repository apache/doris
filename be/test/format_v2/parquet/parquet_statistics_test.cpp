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

#include <gtest/gtest.h>
#include <parquet/bloom_filter.h>

#include <memory>
#include <string>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "storage/predicate/accept_null_predicate.h"
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
    filter.target = format::FileNestedPredicateTarget(filter.file_column_id);
    filter.predicates.push_back(predicate);
    return filter;
}

::parquet::BlockSplitBloomFilter bloom_filter_for_int32_values(const std::vector<int32_t>& values) {
    ::parquet::BlockSplitBloomFilter bloom_filter;
    bloom_filter.Init(::parquet::BlockSplitBloomFilter::kMinimumBloomFilterBytes);
    for (const auto value : values) {
        bloom_filter.InsertHash(bloom_filter.Hash(value));
    }
    return bloom_filter;
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
