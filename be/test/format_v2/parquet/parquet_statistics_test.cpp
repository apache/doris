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

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/field.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "storage/index/bloom_filter/block_split_bloom_filter.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/index/zone_map/zonemap_filter_result.h"

namespace doris {
namespace {

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

class Int32ZoneMapExpr final : public VExpr {
public:
    enum class Op { GE, GT, IS_NULL, IS_NOT_NULL };

    Int32ZoneMapExpr(int column_id, Op op, int32_t value = 0)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _op(op),
              _value(value) {}

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("Int32ZoneMapExpr is only used by parquet statistics tests");
    }

    bool can_evaluate_zonemap_filter() const override { return true; }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        auto zone_map = ctx.zone_map(_column_id);
        if (zone_map == nullptr) {
            return unsupported_zonemap_filter(ctx);
        }
        if (_op == Op::IS_NULL) {
            return zone_map->has_null ? ZoneMapFilterResult::kMayMatch
                                      : ZoneMapFilterResult::kNoMatch;
        }
        if (_op == Op::IS_NOT_NULL) {
            return zone_map->has_not_null ? ZoneMapFilterResult::kMayMatch
                                          : ZoneMapFilterResult::kNoMatch;
        }
        if (!zone_map->has_not_null) {
            return ZoneMapFilterResult::kNoMatch;
        }
        const auto literal = Field::create_field<TYPE_INT>(_value);
        if (_op == Op::GE) {
            return zone_map->max_value < literal ? ZoneMapFilterResult::kNoMatch
                                                 : ZoneMapFilterResult::kMayMatch;
        }
        return zone_map->max_value <= literal ? ZoneMapFilterResult::kNoMatch
                                              : ZoneMapFilterResult::kMayMatch;
    }

private:
    int _column_id;
    Op _op;
    int32_t _value;
    const std::string _expr_name = "Int32ZoneMapExpr";
};

class StringDictionaryInExpr final : public VExpr {
public:
    StringDictionaryInExpr(int column_id, std::vector<std::string> values)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _slot(VSlotRef::create_shared(0, column_id, -1, std::make_shared<DataTypeString>(),
                                            "c0")) {
        _values.reserve(values.size());
        for (auto& value : values) {
            _values.emplace_back(Field::create_field<TYPE_STRING>(std::move(value)));
        }
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError(
                "StringDictionaryInExpr is only used by parquet statistics tests");
    }

    bool can_evaluate_dictionary_filter() const override { return true; }

    ZoneMapFilterResult evaluate_dictionary_filter(
            const DictionaryEvalContext& ctx) const override {
        return expr_zonemap::eval_in_dictionary(ctx, _slot, false, _values);
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        _slot->collect_slot_column_ids(column_ids);
    }

private:
    VExprSPtr _slot;
    std::vector<Field> _values;
    const std::string _expr_name = "StringDictionaryInExpr";
};

class BloomInExpr final : public VExpr {
public:
    BloomInExpr(int column_id, DataTypePtr data_type, std::vector<Field> values)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _slot(VSlotRef::create_shared(0, column_id, -1, std::move(data_type), "c0")),
              _values(std::move(values)) {}

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("BloomInExpr is only used by parquet statistics tests");
    }

    bool can_evaluate_bloom_filter() const override { return true; }

    ZoneMapFilterResult evaluate_bloom_filter(const BloomFilterEvalContext& ctx) const override {
        return expr_zonemap::eval_in_bloom_filter(ctx, _slot, false, _values);
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        _slot->collect_slot_column_ids(column_ids);
    }

private:
    VExprSPtr _slot;
    std::vector<Field> _values;
    const std::string _expr_name = "BloomInExpr";
};

format::FileScanRequest request_with_zonemap_conjunct(std::shared_ptr<VExpr> expr) {
    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.conjuncts.push_back(VExprContext::create_shared(std::move(expr)));
    return request;
}

format::FileScanRequest request_with_dictionary_conjunct(std::vector<std::string> values) {
    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<StringDictionaryInExpr>(0, std::move(values))));
    return request;
}

VExprContextSPtrs bloom_conjuncts(DataTypePtr data_type, std::vector<Field> values) {
    return {VExprContext::create_shared(
            std::make_shared<BloomInExpr>(0, std::move(data_type), std::move(values)))};
}

void add_bloom_field(segment_v2::BlockSplitBloomFilter* bloom_filter, const Field& value,
                     PrimitiveType type) {
    DORIS_CHECK(bloom_filter != nullptr);
    switch (type) {
    case TYPE_BOOLEAN: {
        const bool typed_value = value.get<TYPE_BOOLEAN>();
        bloom_filter->add_bytes(reinterpret_cast<const char*>(&typed_value), sizeof(typed_value));
        break;
    }
    case TYPE_INT: {
        const int32_t typed_value = value.get<TYPE_INT>();
        bloom_filter->add_bytes(reinterpret_cast<const char*>(&typed_value), sizeof(typed_value));
        break;
    }
    case TYPE_STRING: {
        const auto& typed_value = value.get<TYPE_STRING>();
        bloom_filter->add_bytes(typed_value.data(), typed_value.size());
        break;
    }
    default:
        DORIS_CHECK(false);
    }
}

std::unique_ptr<segment_v2::BlockSplitBloomFilter> bloom_filter_for_fields(
        const std::vector<Field>& values, PrimitiveType type) {
    auto bloom_filter = std::make_unique<segment_v2::BlockSplitBloomFilter>();
    EXPECT_TRUE(bloom_filter->init(segment_v2::BloomFilter::MINIMUM_BYTES).ok());
    for (const auto& value : values) {
        add_bloom_field(bloom_filter.get(), value, type);
    }
    return bloom_filter;
}

BloomFilterEvalContext bloom_context(const DataTypePtr& data_type,
                                     const segment_v2::BloomFilter* bloom_filter) {
    BloomFilterEvalContext ctx;
    ctx.slots.emplace(0, BloomFilterEvalContext::SlotBloomFilter {.data_type = data_type,
                                                                  .bloom_filter = bloom_filter});
    return ctx;
}

std::unique_ptr<::parquet::BlockSplitBloomFilter> parquet_bloom_filter() {
    auto bloom_filter = std::make_unique<::parquet::BlockSplitBloomFilter>();
    bloom_filter->Init(::parquet::BlockSplitBloomFilter::kMinimumBloomFilterBytes);
    return bloom_filter;
}

format::parquet::ParquetColumnSchema uint32_parquet_bloom_schema() {
    format::parquet::ParquetColumnSchema column_schema;
    column_schema.type = std::make_shared<DataTypeInt64>();
    column_schema.type_descriptor.doris_type = column_schema.type;
    column_schema.type_descriptor.physical_type = ::parquet::Type::INT32;
    column_schema.type_descriptor.integer_bit_width = 32;
    column_schema.type_descriptor.is_unsigned_integer = true;
    return column_schema;
}

format::parquet::ParquetColumnSchema fixed_len_string_parquet_bloom_schema(int fixed_length) {
    format::parquet::ParquetColumnSchema column_schema;
    column_schema.type = std::make_shared<DataTypeString>();
    column_schema.type_descriptor.doris_type = column_schema.type;
    column_schema.type_descriptor.physical_type = ::parquet::Type::FIXED_LEN_BYTE_ARRAY;
    column_schema.type_descriptor.fixed_length = fixed_length;
    column_schema.type_descriptor.is_string_like = true;
    return column_schema;
}

format::parquet::ParquetColumnSchema float16_parquet_bloom_schema() {
    format::parquet::ParquetColumnSchema column_schema;
    column_schema.type = std::make_shared<DataTypeFloat32>();
    column_schema.type_descriptor.doris_type = column_schema.type;
    column_schema.type_descriptor.physical_type = ::parquet::Type::FIXED_LEN_BYTE_ARRAY;
    column_schema.type_descriptor.fixed_length = 2;
    column_schema.type_descriptor.extra_type_info = format::parquet::ParquetExtraTypeInfo::FLOAT16;
    return column_schema;
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

TEST(ParquetStatisticsTransformTest, DisablesUtcTimestampMinMaxAcrossDstRollback) {
    constexpr int64_t MICROS_PER_SECOND = 1000000;
    // America/New_York moved from UTC-04:00 to UTC-05:00 at 2021-11-07 06:00:00 UTC.
    // Both UTC endpoints below map to 01:30 local time, while values inside the interval cover
    // 01:00 through 01:59. Endpoint conversion therefore cannot represent the true local range.
    auto table = arrow::Table::Make(
            arrow::schema(
                    {arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), false)}),
            {timestamp_array({1636263000 * MICROS_PER_SECOND, 1636263900 * MICROS_PER_SECOND,
                              1636266600 * MICROS_PER_SECOND})});
    auto reader = make_reader(table, 3, false, true);
    auto schema = build_file_schema(*reader);
    auto statistics = reader->metadata()->RowGroup(0)->ColumnChunk(0)->statistics();

    cctz::time_zone new_york;
    ASSERT_TRUE(cctz::load_time_zone("America/New_York", &new_york));
    const auto local_stats = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *schema[0], statistics, &new_york);
    EXPECT_TRUE(local_stats.has_not_null);
    EXPECT_FALSE(local_stats.has_min_max);

    auto utc = cctz::utc_time_zone();
    const auto utc_stats = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *schema[0], statistics, &utc);
    EXPECT_TRUE(utc_stats.has_min_max);
    EXPECT_LT(utc_stats.min_value, utc_stats.max_value);
}

TEST(ParquetStatisticsTransformTest, KeepsTimestampTzMinMaxAcrossDstRollback) {
    constexpr int64_t MICROS_PER_SECOND = 1000000;
    auto table = arrow::Table::Make(
            arrow::schema(
                    {arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), false)}),
            {timestamp_array({1636263000 * MICROS_PER_SECOND, 1636266600 * MICROS_PER_SECOND})});
    auto reader = make_reader(table, 2, false, true);
    auto schema = build_file_schema(*reader);
    auto statistics = reader->metadata()->RowGroup(0)->ColumnChunk(0)->statistics();

    // This is the effective type produced by enable_mapping_timestamp_tz. The physical timestamp
    // flags intentionally remain adjusted-to-UTC so decoding can preserve the source semantics.
    schema[0]->type = std::make_shared<DataTypeTimeStampTz>(6);
    schema[0]->type_descriptor.doris_type = schema[0]->type;

    cctz::time_zone new_york;
    ASSERT_TRUE(cctz::load_time_zone("America/New_York", &new_york));
    const auto timestamp_tz_stats =
            format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
                    *schema[0], statistics, &new_york);
    EXPECT_TRUE(timestamp_tz_stats.has_min_max);
    EXPECT_EQ(timestamp_tz_stats.min_value.get_type(), TYPE_TIMESTAMPTZ);
    EXPECT_EQ(timestamp_tz_stats.max_value.get_type(), TYPE_TIMESTAMPTZ);
    EXPECT_LT(timestamp_tz_stats.min_value, timestamp_tz_stats.max_value);
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

TEST(ParquetStatisticsPruningTest, ExprZonemapPredicatesAndNullPredicatesPruneRowGroups) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("i", arrow::int32(), true)}),
                                    {int32_array({std::nullopt, std::nullopt, 3, 4, 5, 6})});
    auto reader = make_reader(table, 2, false, true);
    auto schema = build_file_schema(*reader);

    std::vector<int> selected;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        *reader->metadata(), reader.get(), schema,
                        request_with_zonemap_conjunct(
                                std::make_shared<Int32ZoneMapExpr>(0, Int32ZoneMapExpr::Op::GE, 5)),
                        nullptr, &selected, false, &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({2}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_statistics, 2);

    selected.clear();
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        *reader->metadata(), reader.get(), schema,
                        request_with_zonemap_conjunct(std::make_shared<Int32ZoneMapExpr>(
                                0, Int32ZoneMapExpr::Op::IS_NOT_NULL)),
                        nullptr, &selected, false, &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({1, 2}));

    selected.clear();
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        *reader->metadata(), reader.get(), schema,
                        request_with_zonemap_conjunct(std::make_shared<Int32ZoneMapExpr>(
                                0, Int32ZoneMapExpr::Op::IS_NULL)),
                        nullptr, &selected, false, &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({0}));
}

TEST(ParquetStatisticsPruningTest, DictionaryPruningHandlesExcludeIncludeAndUnsupportedPaths) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("s", arrow::utf8(), false)}),
                                    {string_array({"alpha", "beta", "gamma", "omega"})});
    auto reader = make_reader(table, 2, true, false);
    auto schema = build_file_schema(*reader);

    std::vector<int> selected;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        *reader->metadata(), reader.get(), schema,
                        request_with_dictionary_conjunct({"missing"}), nullptr, &selected, false,
                        &pruning_stats)
                        .ok());
    EXPECT_TRUE(selected.empty());
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_dictionary, 2);

    selected.clear();
    pruning_stats = {};
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        *reader->metadata(), reader.get(), schema,
                        request_with_dictionary_conjunct({"gamma"}), nullptr, &selected, false,
                        &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({1}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_dictionary, 1);

    auto plain_reader = make_reader(table, 2, false, false);
    auto plain_schema = build_file_schema(*plain_reader);
    selected.clear();
    pruning_stats = {};
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        *plain_reader->metadata(), plain_reader.get(), plain_schema,
                        request_with_dictionary_conjunct({"missing"}), nullptr, &selected, false,
                        &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({0, 1}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_dictionary, 0);
}

TEST(ParquetStatisticsPruningTest, VExprUsesDictionaryAndMissingBloomKeepsRows) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("s", arrow::utf8(), false)}),
                                    {string_array({"alpha", "beta", "gamma", "omega"})});
    auto reader = make_reader(table, 2, true, true);
    auto schema = build_file_schema(*reader);

    std::vector<int> selected;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        *reader->metadata(), reader.get(), schema,
                        request_with_dictionary_conjunct({"absent"}), nullptr, &selected, true,
                        &pruning_stats)
                        .ok());
    EXPECT_TRUE(selected.empty());
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_statistics, 0);
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_dictionary, 2);
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_bloom_filter, 0);

    auto no_stats_reader = make_reader(table, 2, false, false);
    auto no_stats_schema = build_file_schema(*no_stats_reader);
    selected.clear();
    pruning_stats = {};
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        *no_stats_reader->metadata(), no_stats_reader.get(), no_stats_schema,
                        request_with_dictionary_conjunct({"absent"}), nullptr, &selected, true,
                        &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({0, 1}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_bloom_filter, 0);
}

TEST(ParquetBloomFilterPruningTest, VExprEqPrunesAbsentIntValue) {
    auto data_type = std::make_shared<DataTypeInt32>();
    auto bloom_filter = bloom_filter_for_fields(
            {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(3)}, TYPE_INT);
    auto ctx = bloom_context(data_type, bloom_filter.get());

    EXPECT_EQ(VExprContext::evaluate_bloom_filter(
                      bloom_conjuncts(data_type, {Field::create_field<TYPE_INT>(2)}), ctx),
              ZoneMapFilterResult::kNoMatch);
    EXPECT_EQ(VExprContext::evaluate_bloom_filter(
                      bloom_conjuncts(data_type, {Field::create_field<TYPE_INT>(3)}), ctx),
              ZoneMapFilterResult::kMayMatch);
}

TEST(ParquetBloomFilterPruningTest, VExprInPrunesOnlyWhenAllValuesAreAbsent) {
    auto data_type = std::make_shared<DataTypeInt32>();
    auto bloom_filter = bloom_filter_for_fields(
            {Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(3)}, TYPE_INT);
    auto ctx = bloom_context(data_type, bloom_filter.get());

    EXPECT_EQ(VExprContext::evaluate_bloom_filter(
                      bloom_conjuncts(data_type, {Field::create_field<TYPE_INT>(2),
                                                  Field::create_field<TYPE_INT>(4)}),
                      ctx),
              ZoneMapFilterResult::kNoMatch);
    EXPECT_EQ(VExprContext::evaluate_bloom_filter(
                      bloom_conjuncts(data_type, {Field::create_field<TYPE_INT>(2),
                                                  Field::create_field<TYPE_INT>(3)}),
                      ctx),
              ZoneMapFilterResult::kMayMatch);
}

TEST(ParquetBloomFilterPruningTest, VExprBoolAndStringUseSlotBloomFilter) {
    auto bool_type = std::make_shared<DataTypeBool>();
    auto bool_filter =
            bloom_filter_for_fields({Field::create_field<TYPE_BOOLEAN>(true)}, TYPE_BOOLEAN);
    auto bool_ctx = bloom_context(bool_type, bool_filter.get());
    EXPECT_EQ(VExprContext::evaluate_bloom_filter(
                      bloom_conjuncts(bool_type, {Field::create_field<TYPE_BOOLEAN>(false)}),
                      bool_ctx),
              ZoneMapFilterResult::kNoMatch);
    EXPECT_EQ(VExprContext::evaluate_bloom_filter(
                      bloom_conjuncts(bool_type, {Field::create_field<TYPE_BOOLEAN>(true)}),
                      bool_ctx),
              ZoneMapFilterResult::kMayMatch);

    auto string_type = std::make_shared<DataTypeString>();
    auto string_filter = bloom_filter_for_fields(
            {Field::create_field<TYPE_STRING>("alpha"), Field::create_field<TYPE_STRING>("omega")},
            TYPE_STRING);
    auto string_ctx = bloom_context(string_type, string_filter.get());
    EXPECT_EQ(VExprContext::evaluate_bloom_filter(
                      bloom_conjuncts(string_type, {Field::create_field<TYPE_STRING>("beta")}),
                      string_ctx),
              ZoneMapFilterResult::kNoMatch);
    EXPECT_EQ(VExprContext::evaluate_bloom_filter(
                      bloom_conjuncts(string_type, {Field::create_field<TYPE_STRING>("alpha")}),
                      string_ctx),
              ZoneMapFilterResult::kMayMatch);
}

TEST(ParquetBloomFilterPruningTest, MissingOrUnsupportedBloomContextKeepsRowGroup) {
    auto int_type = std::make_shared<DataTypeInt32>();
    BloomFilterEvalContext missing_ctx;
    EXPECT_EQ(VExprContext::evaluate_bloom_filter(
                      bloom_conjuncts(int_type, {Field::create_field<TYPE_INT>(2)}), missing_ctx),
              ZoneMapFilterResult::kMayMatch);

    auto smallint_type = std::make_shared<DataTypeInt16>();
    auto bloom_filter = bloom_filter_for_fields({Field::create_field<TYPE_INT>(1)}, TYPE_INT);
    auto unsupported_ctx = bloom_context(smallint_type, bloom_filter.get());
    EXPECT_EQ(VExprContext::evaluate_bloom_filter(
                      bloom_conjuncts(smallint_type, {Field::create_field<TYPE_SMALLINT>(2)}),
                      unsupported_ctx),
              ZoneMapFilterResult::kMayMatch);
}

TEST(ParquetBloomFilterPruningTest, ParquetUint32BloomUsesPhysicalInt32Hash) {
    const auto column_schema = uint32_parquet_bloom_schema();
    auto bloom_filter = parquet_bloom_filter();

    const uint32_t present_value = 4000000000U;
    int32_t physical_value;
    memcpy(&physical_value, &present_value, sizeof(physical_value));
    bloom_filter->InsertHash(bloom_filter->Hash(physical_value));

    // UINT32 is exposed to VExpr as Doris BIGINT, but Parquet stores and hashes it as a physical
    // INT32 carrier. A present value above INT32_MAX must therefore be narrowed to the physical
    // bit pattern before probing the file bloom filter.
    EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(
            column_schema, 0,
            bloom_conjuncts(column_schema.type, {Field::create_field<TYPE_BIGINT>(
                                                        static_cast<int64_t>(present_value))}),
            *bloom_filter));

    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(
            column_schema, 0,
            bloom_conjuncts(column_schema.type, {Field::create_field<TYPE_BIGINT>(-1)}),
            *bloom_filter));
    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(
            column_schema, 0,
            bloom_conjuncts(
                    column_schema.type,
                    {Field::create_field<TYPE_BIGINT>(
                            static_cast<int64_t>(std::numeric_limits<uint32_t>::max()) + 1)}),
            *bloom_filter));
}

TEST(ParquetBloomFilterPruningTest, ParquetFixedLenByteArrayBloomUsesFlbaHash) {
    const auto column_schema = fixed_len_string_parquet_bloom_schema(4);
    auto bloom_filter = parquet_bloom_filter();

    const std::string present_value = "abcd";
    ::parquet::FLBA physical_value(reinterpret_cast<const uint8_t*>(present_value.data()));
    bloom_filter->InsertHash(
            bloom_filter->Hash(&physical_value, column_schema.type_descriptor.fixed_length));

    EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(
            column_schema, 0,
            bloom_conjuncts(column_schema.type, {Field::create_field<TYPE_STRING>(present_value)}),
            *bloom_filter));
    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(
            column_schema, 0,
            bloom_conjuncts(column_schema.type, {Field::create_field<TYPE_STRING>("abc")}),
            *bloom_filter));
}

TEST(ParquetBloomFilterPruningTest, ParquetFloat16BloomDoesNotUseFloatHash) {
    const auto column_schema = float16_parquet_bloom_schema();
    auto bloom_filter = parquet_bloom_filter();

    EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::BloomFilterExcludes(
            column_schema, 0,
            bloom_conjuncts(column_schema.type, {Field::create_field<TYPE_FLOAT>(1.0F)}),
            *bloom_filter));
}

} // namespace
} // namespace doris
