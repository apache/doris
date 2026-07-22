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

#include <cstdint>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
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
#include "util/thrift_util.h"
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

class DictionaryStringInExpr final : public VExpr {
public:
    DictionaryStringInExpr() : VExpr(std::make_shared<DataTypeUInt8>(), false) {}

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("DictionaryStringInExpr is metadata-only");
    }

    bool can_evaluate_dictionary_filter() const override { return true; }

    ZoneMapFilterResult evaluate_dictionary_filter(const DictionaryEvalContext&) const override {
        return ZoneMapFilterResult::kNoMatch;
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override { column_ids.insert(0); }

private:
    const std::string _expr_name = "DictionaryStringInExpr";
};

class MetadataInt32GreaterThanExpr final : public VExpr {
public:
    explicit MetadataInt32GreaterThanExpr(int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _value(value) {}

    const std::string& expr_name() const override { return _expr_name; }
    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("MetadataInt32GreaterThanExpr is metadata-only");
    }
    bool can_evaluate_zonemap_filter() const override { return true; }
    void collect_slot_column_ids(std::set<int>& column_ids) const override { column_ids.insert(0); }
    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        const auto zone_map = ctx.zone_map(0);
        if (zone_map == nullptr) {
            return unsupported_zonemap_filter(ctx);
        }
        if (!zone_map->has_not_null) {
            return ZoneMapFilterResult::kNoMatch;
        }
        return zone_map->max_value <= Field::create_field<TYPE_INT>(_value)
                       ? ZoneMapFilterResult::kNoMatch
                       : ZoneMapFilterResult::kMayMatch;
    }

private:
    int32_t _value;
    const std::string _expr_name = "MetadataInt32GreaterThanExpr";
};
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
format::parquet::ParquetColumnSchema uint32_parquet_bloom_schema() {
    format::parquet::ParquetColumnSchema column_schema;
    column_schema.type = std::make_shared<DataTypeInt64>();
    column_schema.type_descriptor.doris_type = column_schema.type;
    column_schema.type_descriptor.physical_type = tparquet::Type::INT32;
    column_schema.type_descriptor.integer_bit_width = 32;
    column_schema.type_descriptor.is_unsigned_integer = true;
    return column_schema;
}

TEST(NativeParquetStatisticsTest, InvalidNullableDateBoundsDisableMinMax) {
    format::parquet::ParquetColumnSchema column_schema;
    column_schema.type = make_nullable(std::make_shared<DataTypeDateV2>());
    column_schema.type_descriptor.doris_type = column_schema.type;
    column_schema.type_descriptor.physical_type = tparquet::Type::INT32;

    const int32_t invalid_date = std::numeric_limits<int32_t>::min();
    tparquet::Statistics statistics;
    statistics.__set_null_count(0);
    statistics.__set_min_value(
            std::string(reinterpret_cast<const char*>(&invalid_date), sizeof(invalid_date)));
    statistics.__set_max_value(
            std::string(reinterpret_cast<const char*>(&invalid_date), sizeof(invalid_date)));

    const auto result = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            column_schema, &statistics, 1, nullptr);
    EXPECT_FALSE(result.has_min_max);
}

TEST(NativeParquetStatisticsTest, InvalidNullableDecimalBoundsDisableMinMax) {
    format::parquet::ParquetColumnSchema column_schema;
    column_schema.type = make_nullable(std::make_shared<DataTypeDecimal32>(2, 0));
    column_schema.type_descriptor.doris_type = column_schema.type;
    column_schema.type_descriptor.physical_type = tparquet::Type::INT32;
    column_schema.type_descriptor.is_decimal = true;
    column_schema.type_descriptor.decimal_precision = 2;
    column_schema.type_descriptor.decimal_scale = 0;

    const int32_t invalid_decimal = 1000;
    tparquet::Statistics statistics;
    statistics.__set_null_count(0);
    statistics.__set_min_value(
            std::string(reinterpret_cast<const char*>(&invalid_decimal), sizeof(invalid_decimal)));
    statistics.__set_max_value(
            std::string(reinterpret_cast<const char*>(&invalid_decimal), sizeof(invalid_decimal)));

    const auto result = format::parquet::ParquetStatisticsUtils::TransformColumnStatistics(
            column_schema, &statistics, 1, nullptr);
    EXPECT_FALSE(result.has_min_max);
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

TEST(NativeParquetStatisticsTest, EmptyDictionaryRowGroupIsSkippedBeforeMetadataProbes) {
    tparquet::SchemaElement root;
    root.__set_name("schema");
    root.__set_num_children(1);
    tparquet::SchemaElement leaf;
    leaf.__set_name("value");
    leaf.__set_type(tparquet::Type::BYTE_ARRAY);
    leaf.__set_repetition_type(tparquet::FieldRepetitionType::REQUIRED);

    tparquet::ColumnMetaData column_metadata;
    column_metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    column_metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
    column_metadata.__set_num_values(0);
    column_metadata.__set_total_compressed_size(0);
    column_metadata.__set_data_page_offset(0);
    column_metadata.__set_dictionary_page_offset(0);
    column_metadata.__set_encodings({tparquet::Encoding::RLE_DICTIONARY});
    tparquet::ColumnChunk chunk;
    chunk.__set_meta_data(column_metadata);
    tparquet::RowGroup row_group;
    row_group.__set_columns({chunk});
    row_group.__set_total_byte_size(0);
    row_group.__set_num_rows(0);
    tparquet::FileMetaData thrift_metadata;
    thrift_metadata.__set_version(1);
    thrift_metadata.__set_schema({root, leaf});
    thrift_metadata.__set_num_rows(0);
    thrift_metadata.__set_row_groups({row_group});

    format::parquet::NativeParquetMetadata native_metadata(thrift_metadata, 0);
    ASSERT_TRUE(native_metadata.init_schema(false, false).ok());
    format::parquet::ParquetFileContext file_context;
    file_context.native_file =
            std::make_shared<StatisticsMemoryFileReader>(std::vector<uint8_t> {});
    file_context.native_metadata = &native_metadata;

    auto column_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    column_schema->local_id = 0;
    column_schema->leaf_column_id = 0;
    column_schema->type = std::make_shared<DataTypeString>();
    column_schema->type_descriptor.doris_type = column_schema->type;
    column_schema->type_descriptor.physical_type = tparquet::Type::BYTE_ARRAY;
    column_schema->type_descriptor.is_string_like = true;
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(column_schema));

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.predicate_columns = {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    request.conjuncts = {VExprContext::create_shared(std::make_shared<DictionaryStringInExpr>())};
    std::vector<int> selected_row_groups;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(
                        thrift_metadata, schema, request, nullptr, &selected_row_groups, true,
                        &pruning_stats, nullptr, nullptr, &file_context)
                        .ok());
    EXPECT_TRUE(selected_row_groups.empty());
}

TEST(NativeParquetStatisticsTest, InvalidCandidateRowGroupReturnsCorruption) {
    tparquet::RowGroup row_group;
    row_group.__set_num_rows(1);
    tparquet::FileMetaData metadata;
    metadata.__set_row_groups({row_group});
    format::FileScanRequest request;
    const std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    const std::vector<int> candidates {1};
    std::vector<int> selected_row_groups;

    const auto status = format::parquet::select_row_groups_by_metadata(
            metadata, schema, request, &candidates, &selected_row_groups, false, nullptr, nullptr,
            nullptr, nullptr);
    EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>()) << status;
}
TEST(NativeParquetStatisticsTest, LegacyBinaryFooterBoundsRequireComparableOrdering) {
    format::parquet::ParquetTypeDescriptor binary_type;
    binary_type.physical_type = tparquet::Type::BYTE_ARRAY;

    tparquet::Statistics max_only;
    max_only.__set_max("III");
    EXPECT_FALSE(
            format::parquet::detail::can_use_native_footer_min_max(binary_type, max_only, false));

    tparquet::Statistics legacy_different;
    legacy_different.__set_min("III");
    legacy_different.__set_max("\xe6\x98\xaf");
    EXPECT_FALSE(format::parquet::detail::can_use_native_footer_min_max(binary_type,
                                                                        legacy_different, false));

    tparquet::Statistics legacy_equal;
    legacy_equal.__set_min("same");
    legacy_equal.__set_max("same");
    EXPECT_TRUE(format::parquet::detail::can_use_native_footer_min_max(binary_type, legacy_equal,
                                                                       false));

    tparquet::Statistics type_defined;
    type_defined.__set_min_value("III");
    type_defined.__set_max_value("\xe6\x98\xaf");
    EXPECT_FALSE(format::parquet::detail::can_use_native_footer_min_max(binary_type, type_defined,
                                                                        false));
    EXPECT_TRUE(format::parquet::detail::can_use_native_footer_min_max(binary_type, type_defined,
                                                                       true));

    tparquet::Statistics mixed_fields;
    mixed_fields.__set_min_value("III");
    mixed_fields.__set_max("\xe6\x98\xaf");
    EXPECT_FALSE(format::parquet::detail::can_use_native_footer_min_max(binary_type, mixed_fields,
                                                                        true));
}

TEST(NativeParquetStatisticsTest, TypeDefinedBoundsRequireSupportedColumnOrder) {
    auto encode_int32 = [](int32_t value) {
        std::string bytes(sizeof(value), '\0');
        memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };

    auto column_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    column_schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
    column_schema->local_id = 0;
    column_schema->leaf_column_id = 0;
    column_schema->type = std::make_shared<DataTypeInt32>();
    column_schema->type_descriptor.doris_type = column_schema->type;
    column_schema->type_descriptor.physical_type = tparquet::Type::INT32;
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(column_schema));

    tparquet::Statistics statistics;
    statistics.__set_min_value(encode_int32(1));
    statistics.__set_max_value(encode_int32(2));
    statistics.__set_null_count(0);
    tparquet::ColumnMetaData column_metadata;
    column_metadata.__set_type(tparquet::Type::INT32);
    column_metadata.__set_num_values(1);
    column_metadata.__set_statistics(statistics);
    tparquet::ColumnChunk chunk;
    chunk.__set_meta_data(column_metadata);
    tparquet::RowGroup row_group;
    row_group.__set_columns({chunk});
    row_group.__set_num_rows(1);
    tparquet::FileMetaData metadata;
    metadata.__set_row_groups({row_group});

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.predicate_columns = {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    request.conjuncts = {
            VExprContext::create_shared(std::make_shared<MetadataInt32GreaterThanExpr>(100))};
    std::vector<int> selected_row_groups;
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(metadata, schema, request, nullptr,
                                                               &selected_row_groups, false, nullptr)
                        .ok());
    EXPECT_EQ(selected_row_groups, std::vector<int>({0}));

    format::parquet::NativeParquetPageIndex page_index;
    page_index.column_index.__set_min_values({encode_int32(1)});
    page_index.column_index.__set_max_values({encode_int32(2)});
    page_index.column_index.__set_null_pages({false});
    page_index.column_index.__set_null_counts({0});
    tparquet::PageLocation location;
    location.__set_offset(0);
    location.__set_compressed_page_size(10);
    location.__set_first_row_index(0);
    page_index.offset_index.__set_page_locations({location});
    std::unordered_map<int, format::parquet::NativeParquetPageIndex> page_indexes;
    page_indexes.emplace(0, page_index);
    std::vector<format::parquet::RowRange> selected_ranges;
    std::map<int, format::parquet::ParquetPageSkipPlan> skip_plans;
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, page_indexes, schema, request, 1, &selected_ranges, &skip_plans,
                        nullptr)
                        .ok());
    EXPECT_EQ(selected_ranges.size(), 1);

    tparquet::ColumnOrder order;
    order.__set_TYPE_ORDER(tparquet::TypeDefinedOrder());
    metadata.__set_column_orders({order});
    selected_row_groups.clear();
    ASSERT_TRUE(format::parquet::select_row_groups_by_metadata(metadata, schema, request, nullptr,
                                                               &selected_row_groups, false, nullptr)
                        .ok());
    EXPECT_TRUE(selected_row_groups.empty());
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                        metadata, page_indexes, schema, request, 1, &selected_ranges, &skip_plans,
                        nullptr)
                        .ok());
    EXPECT_TRUE(selected_ranges.empty());
}

TEST(NativeParquetStatisticsTest, ContradictoryAllNullPageCountsDisablePruning) {
    auto column_schema = std::make_unique<format::parquet::ParquetColumnSchema>();
    column_schema->kind = format::parquet::ParquetColumnSchemaKind::PRIMITIVE;
    column_schema->local_id = 0;
    column_schema->leaf_column_id = 0;
    column_schema->type = std::make_shared<DataTypeInt32>();
    column_schema->type_descriptor.doris_type = column_schema->type;
    column_schema->type_descriptor.physical_type = tparquet::Type::INT32;
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> schema;
    schema.push_back(std::move(column_schema));

    tparquet::ColumnOrder order;
    order.__set_TYPE_ORDER(tparquet::TypeDefinedOrder());
    tparquet::FileMetaData metadata;
    metadata.__set_column_orders({order});
    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.predicate_columns = {format::LocalColumnIndex::top_level(format::LocalColumnId(0))};
    request.conjuncts = {
            VExprContext::create_shared(std::make_shared<MetadataInt32GreaterThanExpr>(0))};

    for (const int64_t contradictory_null_count : {5, 11}) {
        SCOPED_TRACE(contradictory_null_count);
        format::parquet::NativeParquetPageIndex page_index;
        page_index.column_index.__set_null_pages({true});
        page_index.column_index.__set_null_counts({contradictory_null_count});
        tparquet::PageLocation location;
        location.__set_offset(0);
        location.__set_compressed_page_size(10);
        location.__set_first_row_index(0);
        page_index.offset_index.__set_page_locations({location});
        std::unordered_map<int, format::parquet::NativeParquetPageIndex> page_indexes;
        page_indexes.emplace(0, std::move(page_index));
        std::vector<format::parquet::RowRange> selected_ranges;
        std::map<int, format::parquet::ParquetPageSkipPlan> skip_plans;

        ASSERT_TRUE(format::parquet::select_row_group_ranges_by_native_page_index(
                            metadata, page_indexes, schema, request, 10, &selected_ranges,
                            &skip_plans, nullptr)
                            .ok());
        // ColumnIndex is optional. An impossible all-null claim must fall back to reading the
        // ten-row data page instead of proving that no value can satisfy the predicate.
        ASSERT_EQ(selected_ranges.size(), 1);
        EXPECT_EQ(selected_ranges[0].start, 0);
        EXPECT_EQ(selected_ranges[0].length, 10);
    }
}

} // namespace
} // namespace doris
