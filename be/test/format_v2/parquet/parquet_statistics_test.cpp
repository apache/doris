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
#include <parquet/bloom_filter_reader.h>
#include <parquet/page_index.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/field.h"
#include "exprs/expr_zonemap_filter.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_file_context.h"
#include "format_v2/parquet/reader/native/block_split_bloom_filter.h"
#include "io/fs/file_reader.h"
#include "storage/index/bloom_filter/block_split_bloom_filter.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/index/zone_map/zonemap_filter_result.h"

namespace doris {
namespace {

class StatisticsMemoryFileReader final : public io::FileReader {
public:
    explicit StatisticsMemoryFileReader(std::vector<uint8_t> bytes)
            : _bytes(std::move(bytes)), _path("native-bloom-filter.parquet") {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }
    const io::Path& path() const override { return _path; }
    size_t size() const override { return _bytes.size(); }
    bool closed() const override { return _closed; }
    int64_t mtime() const override { return 1; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext*) override {
        if (offset > _bytes.size() || result.size > _bytes.size() - offset) {
            return Status::IOError("native Bloom test read exceeds memory file");
        }
        memcpy(result.data, _bytes.data() + offset, result.size);
        *bytes_read = result.size;
        return Status::OK();
    }

private:
    std::vector<uint8_t> _bytes;
    io::Path _path;
    bool _closed = false;
};

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

std::shared_ptr<arrow::Array> float_array(const std::vector<float>& values) {
    arrow::FloatBuilder builder;
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> double_array(const std::vector<double>& values) {
    arrow::DoubleBuilder builder;
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

template <typename NativeType>
std::string encoded_value(const NativeType& value) {
    return {reinterpret_cast<const char*>(&value), sizeof(value)};
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

template <typename ParquetDType>
class TestColumnIndex final : public ::parquet::TypedColumnIndex<ParquetDType> {
public:
    using NativeType = typename ParquetDType::c_type;

    TestColumnIndex(NativeType min_value, NativeType max_value)
            : TestColumnIndex(std::vector<NativeType> {min_value},
                              std::vector<NativeType> {max_value}) {}

    TestColumnIndex(std::vector<NativeType> min_values, std::vector<NativeType> max_values)
            : _null_pages(min_values.size(), false),
              _null_counts(min_values.size(), 0),
              _min_values(std::move(min_values)),
              _max_values(std::move(max_values)) {
        EXPECT_EQ(_min_values.size(), _max_values.size());
        for (size_t page_idx = 0; page_idx < _min_values.size(); ++page_idx) {
            _non_null_page_indices.push_back(static_cast<int32_t>(page_idx));
        }
    }

    const std::vector<bool>& null_pages() const override { return _null_pages; }
    const std::vector<std::string>& encoded_min_values() const override { return _encoded_values; }
    const std::vector<std::string>& encoded_max_values() const override { return _encoded_values; }
    ::parquet::BoundaryOrder::type boundary_order() const override {
        return ::parquet::BoundaryOrder::Unordered;
    }
    bool has_null_counts() const override { return true; }
    const std::vector<int64_t>& null_counts() const override { return _null_counts; }
    const std::vector<int32_t>& non_null_page_indices() const override {
        return _non_null_page_indices;
    }
    const std::vector<NativeType>& min_values() const override { return _min_values; }
    const std::vector<NativeType>& max_values() const override { return _max_values; }

private:
    const std::vector<bool> _null_pages;
    const std::vector<std::string> _encoded_values;
    const std::vector<int64_t> _null_counts;
    std::vector<int32_t> _non_null_page_indices;
    const std::vector<NativeType> _min_values;
    const std::vector<NativeType> _max_values;
};

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
    request.predicate_columns.push_back(
            format::LocalColumnIndex::top_level(format::LocalColumnId(0)));
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

format::FileScanRequest request_with_bloom_conjunct(DataTypePtr data_type,
                                                    std::vector<Field> values) {
    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.conjuncts = bloom_conjuncts(std::move(data_type), std::move(values));
    return request;
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

TEST(ParquetStatisticsTransformTest, MissingNullCountConservativelyReportsPossibleNulls) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("i", arrow::int32(), true)}),
                                    {int32_array({1, std::nullopt, 3})});
    auto reader = make_reader(table, 3, false, true);
    auto schema = build_file_schema(*reader);
    auto file_statistics = reader->metadata()->RowGroup(0)->ColumnChunk(0)->statistics();
    auto statistics_without_null_count = ::parquet::MakeStatistics<::parquet::Int32Type>(
            reader->metadata()->schema()->Column(0), file_statistics->EncodeMin(),
            file_statistics->EncodeMax(), file_statistics->num_values(), 0, 0, true, false, false);

    const auto statistics = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *schema[0], statistics_without_null_count);
    EXPECT_FALSE(statistics.has_null_count);
    EXPECT_TRUE(statistics.has_null);
    EXPECT_TRUE(statistics.has_not_null);
    EXPECT_TRUE(statistics.has_min_max);
    EXPECT_EQ(statistics.min_value.get<TYPE_INT>(), 1);
    EXPECT_EQ(statistics.max_value.get<TYPE_INT>(), 3);
}

TEST(ParquetStatisticsTransformTest, InvalidFooterNullCountDisablesPruningStatistics) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("i", arrow::int32(), true)}),
                                    {int32_array({7})});
    auto reader = make_reader(table, 1, false, true);
    auto schema = build_file_schema(*reader);
    tparquet::Statistics malformed;
    malformed.__set_null_count(2);
    malformed.__set_min_value(encoded_value<int32_t>(7));
    malformed.__set_max_value(encoded_value<int32_t>(7));
    const auto statistics = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *schema[0], &malformed, 1);
    EXPECT_FALSE(statistics.has_null_count);
    EXPECT_FALSE(statistics.has_min_max);
}

TEST(ParquetBloomFilterPruningTest, NativeBloomLayoutRejectsSubBlockAndHugePayloads) {
    using format::parquet::detail::validate_native_bloom_filter_layout;
    EXPECT_TRUE(validate_native_bloom_filter_layout(8, 12, 32, 44, 128).ok());
    EXPECT_FALSE(validate_native_bloom_filter_layout(8, 12, 2, 14, 128).ok());
    EXPECT_FALSE(validate_native_bloom_filter_layout(8, 12, 33, 45, 128).ok());
    EXPECT_FALSE(validate_native_bloom_filter_layout(8, 12, std::numeric_limits<int32_t>::max(), -1,
                                                     std::numeric_limits<size_t>::max())
                         .ok());
    EXPECT_FALSE(validate_native_bloom_filter_layout(120, 12, 32, 44, 128).ok());
    EXPECT_FALSE(validate_native_bloom_filter_layout(8, 12, 32, 200, 128).ok());
}

TEST(ParquetStatisticsTransformTest, IgnoresNaNFloatAndDoubleMinMax) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("f", arrow::float32(), false),
                                                   arrow::field("d", arrow::float64(), false)}),
                                    {float_array({1.0F, 2.0F}), double_array({1.0, 2.0})});
    auto reader = make_reader(table, 2, false, true);
    auto schema = build_file_schema(*reader);

    const float float_nan = std::numeric_limits<float>::quiet_NaN();
    const float float_max = 2.0F;
    auto float_stats = ::parquet::MakeStatistics<::parquet::FloatType>(
            schema[0]->descriptor, encoded_value(float_nan), encoded_value(float_max), 2, 0, 0,
            true, true, false);
    const auto converted_float = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *schema[0], float_stats);
    EXPECT_FALSE(converted_float.has_min_max);
    EXPECT_TRUE(converted_float.has_not_null);

    const double double_nan = std::numeric_limits<double>::quiet_NaN();
    const double double_min = 1.0;
    auto double_stats = ::parquet::MakeStatistics<::parquet::DoubleType>(
            schema[1]->descriptor, encoded_value(double_min), encoded_value(double_nan), 2, 0, 0,
            true, true, false);
    const auto converted_double =
            format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(*schema[1],
                                                                               double_stats);
    EXPECT_FALSE(converted_double.has_min_max);
    EXPECT_TRUE(converted_double.has_not_null);

    const double double_max = 2.0;
    auto finite_stats = ::parquet::MakeStatistics<::parquet::DoubleType>(
            schema[1]->descriptor, encoded_value(double_min), encoded_value(double_max), 2, 0, 0,
            true, true, false);
    const auto converted_finite =
            format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(*schema[1],
                                                                               finite_stats);
    EXPECT_TRUE(converted_finite.has_min_max);
}

TEST(ParquetStatisticsTransformTest, IgnoresNaNFloatAndDoubleColumnIndexMinMax) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("f", arrow::float32(), false),
                                                   arrow::field("d", arrow::float64(), false)}),
                                    {float_array({1.0F, 2.0F}), double_array({1.0, 2.0})});
    auto reader = make_reader(table, 2, false, true);
    auto schema = build_file_schema(*reader);

    auto float_index = std::make_shared<TestColumnIndex<::parquet::FloatType>>(
            1.0F, std::numeric_limits<float>::quiet_NaN());
    format::parquet::ParquetColumnStatistics float_page_stats;
    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::TransformColumnIndexStatistics(
            float_index, *schema[0], 0, &float_page_stats));
    EXPECT_FALSE(float_page_stats.has_min_max);
    EXPECT_TRUE(float_page_stats.has_not_null);

    auto double_index = std::make_shared<TestColumnIndex<::parquet::DoubleType>>(
            std::numeric_limits<double>::quiet_NaN(), 2.0);
    format::parquet::ParquetColumnStatistics double_page_stats;
    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::TransformColumnIndexStatistics(
            double_index, *schema[1], 0, &double_page_stats));
    EXPECT_FALSE(double_page_stats.has_min_max);
    EXPECT_TRUE(double_page_stats.has_not_null);

    auto finite_index = std::make_shared<TestColumnIndex<::parquet::DoubleType>>(1.0, 2.0);
    format::parquet::ParquetColumnStatistics finite_page_stats;
    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::TransformColumnIndexStatistics(
            finite_index, *schema[1], 0, &finite_page_stats));
    EXPECT_TRUE(finite_page_stats.has_min_max);

    auto mixed_index = std::make_shared<TestColumnIndex<::parquet::DoubleType>>(
            std::vector<double> {std::numeric_limits<double>::quiet_NaN(), 1.0},
            std::vector<double> {std::numeric_limits<double>::quiet_NaN(), 2.0});
    format::parquet::ParquetColumnStatistics nan_page_stats;
    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::TransformColumnIndexStatistics(
            mixed_index, *schema[1], 0, &nan_page_stats));
    EXPECT_FALSE(nan_page_stats.has_min_max);

    format::parquet::ParquetColumnStatistics following_page_stats;
    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::TransformColumnIndexStatistics(
            mixed_index, *schema[1], 1, &following_page_stats));
    EXPECT_TRUE(following_page_stats.has_min_max);
    EXPECT_EQ(following_page_stats.min_value.get<TYPE_DOUBLE>(), 1.0);
    EXPECT_EQ(following_page_stats.max_value.get<TYPE_DOUBLE>(), 2.0);
}

TEST(ParquetStatisticsTransformTest, IgnoresInvertedFooterAndColumnIndexMinMax) {
    auto table = arrow::Table::Make(
            arrow::schema(
                    {arrow::field("i", arrow::int32(), false),
                     arrow::field("s", arrow::utf8(), false),
                     arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), false)}),
            {int32_array({1, 2}), string_array({"a", "z"}), timestamp_array({1000000, 2000000})});
    auto reader = make_reader(table, 2, false, true);
    auto schema = build_file_schema(*reader);

    const int32_t inverted_min = 10;
    const int32_t inverted_max = 1;
    auto int_stats = ::parquet::MakeStatistics<::parquet::Int32Type>(
            schema[0]->descriptor, encoded_value(inverted_min), encoded_value(inverted_max), 2, 0,
            0, true, true, false);
    const auto converted_int = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            *schema[0], int_stats);
    EXPECT_TRUE(converted_int.has_not_null);
    EXPECT_FALSE(converted_int.has_min_max);

    const std::string inverted_string_min = "z";
    const std::string inverted_string_max = "a";
    auto string_stats = ::parquet::MakeStatistics<::parquet::ByteArrayType>(
            schema[1]->descriptor, inverted_string_min, inverted_string_max, 2, 0, 0, true, true,
            false);
    const auto converted_string =
            format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(*schema[1],
                                                                               string_stats);
    EXPECT_TRUE(converted_string.has_not_null);
    EXPECT_FALSE(converted_string.has_min_max);

    auto int_index =
            std::make_shared<TestColumnIndex<::parquet::Int32Type>>(inverted_min, inverted_max);
    format::parquet::ParquetColumnStatistics page_stats;
    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::TransformColumnIndexStatistics(
            int_index, *schema[0], 0, &page_stats));
    EXPECT_TRUE(page_stats.has_not_null);
    EXPECT_FALSE(page_stats.has_min_max);

    // These endpoints are inverted within one second. Whole-second validation alone would miss
    // the corruption, and TIMESTAMPTZ must reject the same raw inversion before its UTC shortcut.
    constexpr int64_t timestamp_min = 1500000;
    constexpr int64_t timestamp_max = 1000000;
    auto timestamp_stats = ::parquet::MakeStatistics<::parquet::Int64Type>(
            schema[2]->descriptor, encoded_value(timestamp_min), encoded_value(timestamp_max), 2, 0,
            0, true, true, false);
    auto utc = cctz::utc_time_zone();
    const auto converted_timestamp =
            format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
                    *schema[2], timestamp_stats, &utc);
    EXPECT_FALSE(converted_timestamp.has_min_max);

    schema[2]->type = std::make_shared<DataTypeTimeStampTz>(6);
    schema[2]->type_descriptor.doris_type = schema[2]->type;
    const auto converted_timestamp_tz =
            format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
                    *schema[2], timestamp_stats, &utc);
    EXPECT_FALSE(converted_timestamp_tz.has_min_max);
}

TEST(ParquetStatisticsTransformTest, PreservesNullCountWhenNaNInvalidatesMinMax) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("f", arrow::float64(), false)}),
                                    {double_array({1.0, 2.0})});
    auto reader = make_reader(table, 2, false, true);
    auto schema = build_file_schema(*reader);

    const double nan = std::numeric_limits<double>::quiet_NaN();
    const double max_value = 2.0;
    auto footer_stats = ::parquet::MakeStatistics<::parquet::DoubleType>(
            schema[0]->descriptor, encoded_value(nan), encoded_value(max_value), 2, 0, 0, true,
            true, false);
    const auto converted_footer =
            format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(*schema[0],
                                                                               footer_stats);
    auto footer_zone_map = format::parquet::ParquetStatisticsUtils::MakeZoneMap(converted_footer);
    ASSERT_NE(footer_zone_map, nullptr);
    EXPECT_TRUE(footer_zone_map->pass_all);
    EXPECT_FALSE(footer_zone_map->has_null);
    EXPECT_TRUE(footer_zone_map->has_not_null);
    EXPECT_FALSE(expr_zonemap::range_stats_usable_for_zonemap(*footer_zone_map, schema[0]->type));

    ZoneMapEvalContext footer_ctx;
    footer_ctx.slots.emplace(0, ZoneMapEvalContext::SlotZoneMap {.data_type = schema[0]->type,
                                                                 .zone_map = footer_zone_map});
    Int32ZoneMapExpr is_null_expr(0, Int32ZoneMapExpr::Op::IS_NULL);
    EXPECT_EQ(is_null_expr.evaluate_zonemap_filter(footer_ctx), ZoneMapFilterResult::kNoMatch);

    auto column_index = std::make_shared<TestColumnIndex<::parquet::DoubleType>>(nan, max_value);
    format::parquet::ParquetColumnStatistics page_stats;
    ASSERT_TRUE(format::parquet::ParquetStatisticsUtils::TransformColumnIndexStatistics(
            column_index, *schema[0], 0, &page_stats));
    auto page_zone_map = format::parquet::ParquetStatisticsUtils::MakeZoneMap(page_stats);
    ASSERT_NE(page_zone_map, nullptr);
    EXPECT_TRUE(page_zone_map->pass_all);
    EXPECT_FALSE(page_zone_map->has_null);
    EXPECT_TRUE(page_zone_map->has_not_null);
    EXPECT_FALSE(expr_zonemap::range_stats_usable_for_zonemap(*page_zone_map, schema[0]->type));

    ZoneMapEvalContext page_ctx;
    page_ctx.slots.emplace(0, ZoneMapEvalContext::SlotZoneMap {.data_type = schema[0]->type,
                                                               .zone_map = page_zone_map});
    EXPECT_EQ(is_null_expr.evaluate_zonemap_filter(page_ctx), ZoneMapFilterResult::kNoMatch);
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
    EXPECT_GT(pruning_stats.filtered_bytes, 0);

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

TEST(ParquetStatisticsPruningTest, NativeContradictoryNullMetadataFallsBackForNullPredicates) {
    auto table = arrow::Table::Make(arrow::schema({arrow::field("i", arrow::int32(), true)}),
                                    {int32_array({1})});
    auto reader = make_reader(table, 1, false, true);
    auto schema = build_file_schema(*reader);

    auto make_indexes = [](int64_t null_count) {
        format::parquet::NativeParquetPageIndex indexes;
        indexes.column_index.__set_null_pages({true});
        indexes.column_index.__set_null_counts({null_count});
        tparquet::PageLocation location;
        location.__set_offset(0);
        location.__set_compressed_page_size(1);
        location.__set_first_row_index(0);
        indexes.offset_index.__set_page_locations({location});
        return std::unordered_map<int, format::parquet::NativeParquetPageIndex> {
                {0, std::move(indexes)}};
    };

    for (const auto op : {Int32ZoneMapExpr::Op::IS_NULL, Int32ZoneMapExpr::Op::IS_NOT_NULL}) {
        std::vector<format::parquet::RowRange> selected;
        const int64_t null_count = op == Int32ZoneMapExpr::Op::IS_NULL ? 0 : -1;
        ASSERT_TRUE(
                format::parquet::select_row_group_ranges_by_native_page_index(
                        make_indexes(null_count), schema,
                        request_with_zonemap_conjunct(std::make_shared<Int32ZoneMapExpr>(0, op)), 1,
                        &selected, nullptr, nullptr)
                        .ok());
        ASSERT_EQ(selected.size(), 1);
        EXPECT_EQ(selected[0].start, 0);
        EXPECT_EQ(selected[0].length, 1);
    }
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

TEST(ParquetStatisticsPruningTest, BloomFilterCacheIsScopedToRowGroupAndColumn) {
    auto input = arrow::io::ReadableFile::Open(
            "./be/test/exec/test_data/parquet_scanner/multi_row_group_bloom_filter.parquet");
    ASSERT_TRUE(input.ok());
    auto reader = ::parquet::ParquetFileReader::Open(*input);
    ASSERT_EQ(reader->metadata()->num_row_groups(), 2);
    auto& bloom_filter_reader = reader->GetBloomFilterReader();
    for (int row_group_idx = 0; row_group_idx < 2; ++row_group_idx) {
        auto row_group_reader = bloom_filter_reader.RowGroup(row_group_idx);
        ASSERT_NE(row_group_reader, nullptr);
        ASSERT_NE(row_group_reader->GetColumnBloomFilter(0), nullptr);
    }
    auto schema = build_file_schema(*reader);

    std::vector<int> selected;
    format::parquet::ParquetPruningStats pruning_stats;
    auto request = request_with_bloom_conjunct(std::make_shared<DataTypeInt32>(),
                                               {Field::create_field<TYPE_INT>(12345)});
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(*reader->metadata(), reader.get(),
                                                               schema, request, nullptr, &selected,
                                                               false, &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({0, 1}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_bloom_filter, 0);

    selected.clear();
    pruning_stats = {};
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(*reader->metadata(), reader.get(),
                                                               schema, request, nullptr, &selected,
                                                               true, &pruning_stats)
                        .ok());
    EXPECT_EQ(selected, std::vector<int>({1}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_bloom_filter, 1);
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

TEST(ParquetBloomFilterPruningTest, NativeUint32BloomUsesPhysicalInt32Hash) {
    const auto column_schema = uint32_parquet_bloom_schema();
    format::parquet::native::BlockSplitBloomFilter bloom_filter;
    ASSERT_TRUE(bloom_filter
                        .init(segment_v2::BloomFilter::MINIMUM_BYTES,
                              segment_v2::HashStrategyPB::XX_HASH_64)
                        .ok());

    const uint32_t present_value = 4000000000U;
    int32_t physical_value;
    memcpy(&physical_value, &present_value, sizeof(physical_value));
    bloom_filter.add_bytes(reinterpret_cast<const char*>(&physical_value), sizeof(physical_value));

    EXPECT_FALSE(format::parquet::ParquetStatisticsUtils::NativeBloomFilterExcludes(
            column_schema, 0,
            bloom_conjuncts(column_schema.type, {Field::create_field<TYPE_BIGINT>(
                                                        static_cast<int64_t>(present_value))}),
            bloom_filter));
    EXPECT_TRUE(format::parquet::ParquetStatisticsUtils::NativeBloomFilterExcludes(
            column_schema, 0,
            bloom_conjuncts(column_schema.type, {Field::create_field<TYPE_BIGINT>(-1)}),
            bloom_filter));
}

TEST(ParquetBloomFilterPruningTest, NativeRowGroupKeepsPresentUint32AboveInt32Max) {
    auto column_schema =
            std::make_unique<format::parquet::ParquetColumnSchema>(uint32_parquet_bloom_schema());
    column_schema->local_id = 0;
    column_schema->leaf_column_id = 0;

    format::parquet::native::BlockSplitBloomFilter bloom_filter;
    ASSERT_TRUE(bloom_filter
                        .init(segment_v2::BloomFilter::MINIMUM_BYTES,
                              segment_v2::HashStrategyPB::XX_HASH_64)
                        .ok());
    const uint32_t present_value = 4000000000U;
    int32_t physical_value;
    memcpy(&physical_value, &present_value, sizeof(physical_value));
    bloom_filter.add_bytes(reinterpret_cast<const char*>(&physical_value), sizeof(physical_value));

    tparquet::BloomFilterAlgorithm algorithm;
    algorithm.__set_BLOCK(tparquet::SplitBlockAlgorithm());
    tparquet::BloomFilterHash hash;
    hash.__set_XXHASH(tparquet::XxHash());
    tparquet::BloomFilterCompression compression;
    compression.__set_UNCOMPRESSED(tparquet::Uncompressed());
    tparquet::BloomFilterHeader bloom_header;
    bloom_header.__set_numBytes(static_cast<int32_t>(bloom_filter.size()));
    bloom_header.__set_algorithm(algorithm);
    bloom_header.__set_hash(hash);
    bloom_header.__set_compression(compression);
    std::vector<uint8_t> bloom_bytes;
    ThriftSerializer serializer(/*compact=*/true, 64);
    ASSERT_TRUE(serializer.serialize(&bloom_header, &bloom_bytes).ok());
    bloom_bytes.insert(bloom_bytes.end(), bloom_filter.data(),
                       bloom_filter.data() + bloom_filter.size());

    tparquet::ColumnMetaData column_metadata;
    column_metadata.__set_type(tparquet::Type::INT32);
    column_metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    column_metadata.__set_num_values(1);
    column_metadata.__set_total_compressed_size(0);
    column_metadata.__set_data_page_offset(0);
    column_metadata.__set_bloom_filter_offset(0);
    column_metadata.__set_bloom_filter_length(static_cast<int32_t>(bloom_bytes.size()));
    tparquet::ColumnChunk chunk;
    chunk.__set_meta_data(column_metadata);
    tparquet::RowGroup row_group;
    row_group.__set_columns({chunk});
    row_group.__set_total_byte_size(0);
    row_group.__set_num_rows(1);
    tparquet::FileMetaData metadata;
    metadata.__set_version(1);
    metadata.__set_num_rows(1);
    metadata.__set_row_groups({row_group});

    format::parquet::ParquetFileContext file_context;
    file_context.native_file = std::make_shared<StatisticsMemoryFileReader>(std::move(bloom_bytes));
    auto request = request_with_bloom_conjunct(
            column_schema->type,
            {Field::create_field<TYPE_BIGINT>(static_cast<int64_t>(present_value))});
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(column_schema));
    std::vector<int> selected_row_groups;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        metadata, schema, request, nullptr, &selected_row_groups, true,
                        &pruning_stats, nullptr, nullptr, &file_context)
                        .ok());
    EXPECT_EQ(selected_row_groups, std::vector<int>({0}));
    EXPECT_EQ(pruning_stats.filtered_row_groups_by_bloom_filter, 0);
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

TEST(NativeParquetStatisticsTest, LegacyBinaryFooterBoundsRequireComparableOrdering) {
    format::parquet::ParquetTypeDescriptor binary_type;
    binary_type.physical_type = ::parquet::Type::BYTE_ARRAY;

    tparquet::Statistics max_only;
    max_only.__set_max("III");
    EXPECT_FALSE(format::parquet::detail::can_use_native_footer_min_max(binary_type, max_only));

    tparquet::Statistics legacy_different;
    legacy_different.__set_min("III");
    legacy_different.__set_max("\xe6\x98\xaf");
    EXPECT_FALSE(
            format::parquet::detail::can_use_native_footer_min_max(binary_type, legacy_different));

    tparquet::Statistics legacy_equal;
    legacy_equal.__set_min("same");
    legacy_equal.__set_max("same");
    EXPECT_TRUE(format::parquet::detail::can_use_native_footer_min_max(binary_type, legacy_equal));

    tparquet::Statistics type_defined;
    type_defined.__set_min_value("III");
    type_defined.__set_max_value("\xe6\x98\xaf");
    EXPECT_TRUE(format::parquet::detail::can_use_native_footer_min_max(binary_type, type_defined));

    tparquet::Statistics mixed_fields;
    mixed_fields.__set_min_value("III");
    mixed_fields.__set_max("\xe6\x98\xaf");
    EXPECT_FALSE(format::parquet::detail::can_use_native_footer_min_max(binary_type, mixed_fields));
}

} // namespace
} // namespace doris
