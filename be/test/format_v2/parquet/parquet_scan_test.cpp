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

#include "format_v2/parquet/parquet_scan.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>

#include <cstring>
#include <filesystem>
#include <memory>
#include <numeric>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/index/zone_map/zonemap_filter_result.h"
#include "storage/utils.h"

namespace doris {
namespace {

format::LocalColumnIndex field_projection(int32_t column_id) {
    return format::LocalColumnIndex {.index = column_id};
}

const ColumnInt32& int32_data_column(const IColumn& column) {
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(&column)) {
        return assert_cast<const ColumnInt32&>(nullable_column->get_nested_column());
    }
    return assert_cast<const ColumnInt32&>(column);
}

const ColumnString& string_data_column(const IColumn& column) {
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(&column)) {
        return assert_cast<const ColumnString&>(nullable_column->get_nested_column());
    }
    return assert_cast<const ColumnString&>(column);
}

class Int32ZoneMapExpr final : public VExpr {
public:
    enum class Op { GE, GT, LT };

    Int32ZoneMapExpr(int column_id, Op op, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _op(op),
              _value(value) {}

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        DORIS_CHECK(selector == nullptr);
        DORIS_CHECK(_column_id >= 0 && _column_id < static_cast<int>(block->columns()));
        const auto& data_column = int32_data_column(*block->get_by_position(_column_id).column);
        DORIS_CHECK(data_column.size() >= count);

        auto result = ColumnUInt8::create(count, 0);
        auto& result_data = result->get_data();
        for (size_t row = 0; row < count; ++row) {
            const auto value = data_column.get_element(row);
            if (_op == Op::GE) {
                result_data[row] = value >= _value;
            } else if (_op == Op::GT) {
                result_data[row] = value > _value;
            } else {
                result_data[row] = value < _value;
            }
        }
        result_column = std::move(result);
        return Status::OK();
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
        if (!zone_map->has_not_null) {
            return ZoneMapFilterResult::kNoMatch;
        }
        const auto literal = Field::create_field<TYPE_INT>(_value);
        if (_op == Op::GE) {
            return zone_map->max_value < literal ? ZoneMapFilterResult::kNoMatch
                                                 : ZoneMapFilterResult::kMayMatch;
        }
        if (_op == Op::GT) {
            return zone_map->max_value <= literal ? ZoneMapFilterResult::kNoMatch
                                                  : ZoneMapFilterResult::kMayMatch;
        }
        return zone_map->min_value >= literal ? ZoneMapFilterResult::kNoMatch
                                              : ZoneMapFilterResult::kMayMatch;
    }

private:
    int _column_id;
    Op _op;
    int32_t _value;
    const std::string _expr_name = "Int32ZoneMapExpr";
};

class Int32PairSumExpr final : public VExpr {
public:
    Int32PairSumExpr(int left_column_id, int right_column_id, int32_t upper_bound)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _left_column_id(left_column_id),
              _right_column_id(right_column_id),
              _upper_bound(upper_bound) {}

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        DORIS_CHECK(selector == nullptr);
        DORIS_CHECK(_left_column_id >= 0 && _left_column_id < static_cast<int>(block->columns()));
        DORIS_CHECK(_right_column_id >= 0 && _right_column_id < static_cast<int>(block->columns()));
        const auto& left_column =
                int32_data_column(*block->get_by_position(_left_column_id).column);
        const auto& right_column =
                int32_data_column(*block->get_by_position(_right_column_id).column);
        DORIS_CHECK(left_column.size() >= count);
        DORIS_CHECK(right_column.size() >= count);

        auto result = ColumnUInt8::create(count, 0);
        auto& result_data = result->get_data();
        for (size_t row = 0; row < count; ++row) {
            result_data[row] =
                    left_column.get_element(row) + right_column.get_element(row) < _upper_bound;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_left_column_id);
        column_ids.insert(_right_column_id);
    }

private:
    int _left_column_id;
    int _right_column_id;
    int32_t _upper_bound;
    const std::string _expr_name = "Int32PairSumExpr";
};

VExprContextSPtr create_int32_zonemap_conjunct(int column_id, Int32ZoneMapExpr::Op op,
                                               int32_t value) {
    return VExprContext::create_shared(std::make_shared<Int32ZoneMapExpr>(column_id, op, value));
}

VExprContextSPtr create_int32_pair_sum_conjunct(int left_column_id, int right_column_id,
                                                int32_t upper_bound) {
    return VExprContext::create_shared(
            std::make_shared<Int32PairSumExpr>(left_column_id, right_column_id, upper_bound));
}

int64_t counter_value(RuntimeProfile& profile, const std::string& name) {
    auto* counter = profile.get_counter(name);
    DORIS_CHECK(counter != nullptr);
    return counter->value();
}

std::shared_ptr<arrow::Array> finish_array(arrow::ArrayBuilder* builder) {
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder->Finish(&array).ok());
    return array;
}

std::shared_ptr<arrow::Array> build_int32_array(const std::vector<int32_t>& values) {
    arrow::Int32Builder builder;
    for (const auto value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_string_array(const std::vector<std::string>& values) {
    arrow::StringBuilder builder;
    for (const auto& value : values) {
        EXPECT_TRUE(builder.Append(value).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_fixed_binary_array(const std::vector<std::string>& values,
                                                       int byte_width) {
    auto type = arrow::fixed_size_binary(byte_width);
    arrow::FixedSizeBinaryBuilder builder(type, arrow::default_memory_pool());
    for (const auto& value : values) {
        EXPECT_EQ(value.size(), byte_width);
        EXPECT_TRUE(builder.Append(reinterpret_cast<const uint8_t*>(value.data())).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_struct_array(const std::vector<int32_t>& ids,
                                                 const std::vector<std::string>& names) {
    auto struct_type = arrow::struct_({arrow::field("id", arrow::int32(), false),
                                       arrow::field("name", arrow::utf8(), false)});
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(
            std::make_unique<arrow::Int32Builder>().release()));
    field_builders.push_back(std::shared_ptr<arrow::ArrayBuilder>(
            std::make_unique<arrow::StringBuilder>().release()));
    arrow::StructBuilder builder(struct_type, arrow::default_memory_pool(),
                                 std::move(field_builders));
    auto* id_builder = assert_cast<arrow::Int32Builder*>(builder.field_builder(0));
    auto* name_builder = assert_cast<arrow::StringBuilder*>(builder.field_builder(1));
    for (size_t row = 0; row < ids.size(); ++row) {
        EXPECT_TRUE(builder.Append().ok());
        EXPECT_TRUE(id_builder->Append(ids[row]).ok());
        EXPECT_TRUE(name_builder->Append(names[row]).ok());
    }
    return finish_array(&builder);
}

std::shared_ptr<arrow::Array> build_list_array() {
    auto value_builder = std::make_unique<arrow::Int32Builder>();
    arrow::ListBuilder builder(arrow::default_memory_pool(), std::move(value_builder));
    auto* int_builder = assert_cast<arrow::Int32Builder*>(builder.value_builder());
    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(int_builder->Append(1).ok());
    EXPECT_TRUE(int_builder->Append(2).ok());
    EXPECT_TRUE(builder.Append().ok());
    EXPECT_TRUE(int_builder->Append(3).ok());
    EXPECT_TRUE(builder.Append().ok());
    return finish_array(&builder);
}

void write_table(const std::string& file_path, const std::shared_ptr<arrow::Table>& table,
                 int64_t row_group_size, bool enable_dictionary = false,
                 bool enable_page_index = false, bool enable_statistics = true) {
    auto file_result = arrow::io::FileOutputStream::Open(file_path);
    ASSERT_TRUE(file_result.ok()) << file_result.status();
    std::shared_ptr<arrow::io::FileOutputStream> out = *file_result;

    ::parquet::WriterProperties::Builder builder;
    builder.version(::parquet::ParquetVersion::PARQUET_2_6);
    builder.data_page_version(::parquet::ParquetDataPageVersion::V2);
    builder.compression(::parquet::Compression::UNCOMPRESSED);
    if (enable_dictionary) {
        builder.enable_dictionary();
    } else {
        builder.disable_dictionary();
    }
    if (enable_page_index) {
        builder.enable_write_page_index();
        builder.write_batch_size(8);
        builder.data_pagesize(10);
    }
    if (!enable_statistics) {
        builder.disable_statistics();
    }
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), out,
                                                      row_group_size, builder.build()));
}

void write_int_pair_parquet_file(const std::string& file_path, int64_t row_group_size = 2,
                                 bool enable_statistics = true) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("score", arrow::int32(), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array({1, 2, 3, 4, 5, 6}),
                                             build_int32_array({10, 20, 30, 40, 50, 60})});
    write_table(file_path, table, row_group_size, false, false, enable_statistics);
}

void write_binary_minmax_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("text", arrow::utf8(), false),
            arrow::field("fixed", arrow::fixed_size_binary(4), false),
    });
    auto table = arrow::Table::Make(schema, {build_string_array({"alpha", "omega"}),
                                             build_fixed_binary_array({"aaaa", "zzzz"}, 4)});
    write_table(file_path, table, 2);
}

void write_struct_parquet_file(const std::string& file_path) {
    auto struct_type = arrow::struct_({arrow::field("id", arrow::int32(), false),
                                       arrow::field("name", arrow::utf8(), false)});
    auto schema = arrow::schema({
            arrow::field("s", struct_type, false),
    });
    auto table = arrow::Table::Make(
            schema, {build_struct_array({1, 2, 10, 11}, {"one", "two", "ten", "eleven"})});
    write_table(file_path, table, 2);
}

void write_list_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("xs", arrow::list(arrow::int32()), false),
    });
    auto table = arrow::Table::Make(schema, {build_list_array()});
    write_table(file_path, table, 2);
}

void write_int_list_parquet_file(const std::string& file_path) {
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("xs", arrow::list(arrow::int32()), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array({1, 2, 3}), build_list_array()});
    write_table(file_path, table, 3);
}

void write_page_index_parquet_file(const std::string& file_path) {
    std::vector<int32_t> ids(128);
    std::iota(ids.begin(), ids.end(), 0);
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array(ids)});
    write_table(file_path, table, ids.size(), false, true);
}

void write_page_index_pair_parquet_file(const std::string& file_path) {
    std::vector<int32_t> ids(128);
    std::iota(ids.begin(), ids.end(), 0);
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32(), false),
            arrow::field("score", arrow::int32(), false),
    });
    auto table = arrow::Table::Make(schema, {build_int32_array(ids), build_int32_array(ids)});
    write_table(file_path, table, ids.size(), false, true);
}

int64_t parquet_column_start_offset(const ::parquet::ColumnChunkMetaData& column_metadata) {
    return column_metadata.has_dictionary_page()
                   ? static_cast<int64_t>(column_metadata.dictionary_page_offset())
                   : static_cast<int64_t>(column_metadata.data_page_offset());
}

std::pair<int64_t, int64_t> row_group_mid_range(const std::string& file_path, int row_group_idx) {
    auto reader = ::parquet::ParquetFileReader::OpenFile(file_path, false);
    auto metadata = reader->metadata();
    auto row_group_metadata = metadata->RowGroup(row_group_idx);
    auto first_column = row_group_metadata->ColumnChunk(0);
    auto last_column = row_group_metadata->ColumnChunk(row_group_metadata->num_columns() - 1);
    const int64_t row_group_start_offset = parquet_column_start_offset(*first_column);
    const int64_t row_group_end_offset =
            parquet_column_start_offset(*last_column) + last_column->total_compressed_size();
    const int64_t row_group_mid_offset =
            row_group_start_offset + (row_group_end_offset - row_group_start_offset) / 2;
    return {row_group_mid_offset, 1};
}

Block build_file_block(const std::vector<format::ColumnDefinition>& schema) {
    Block block;
    for (const auto& field : schema) {
        block.insert({field.type->create_column(), field.type, field.name});
    }
    return block;
}

GlobalRowLoacationV2 decode_rowid(const ColumnString& column, size_t row) {
    const auto ref = column.get_data_at(row);
    EXPECT_EQ(ref.size, sizeof(GlobalRowLoacationV2));
    GlobalRowLoacationV2 location(0, 0, 0, 0);
    std::memcpy(&location, ref.data, sizeof(GlobalRowLoacationV2));
    return location;
}

void use_schema_order_positions(format::FileScanRequest* request,
                                const std::vector<format::ColumnDefinition>& schema) {
    DORIS_CHECK(request != nullptr);
    for (size_t idx = 0; idx < schema.size(); ++idx) {
        request->local_positions.emplace(format::LocalColumnId(schema[idx].local_id),
                                         format::LocalIndex(idx));
    }
}

std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> build_file_schema(
        const ::parquet::ParquetFileReader& reader) {
    std::vector<std::unique_ptr<format::parquet::ParquetColumnSchema>> file_schema;
    auto schema_descriptor = reader.metadata()->schema();
    EXPECT_NE(schema_descriptor, nullptr);
    EXPECT_TRUE(
            format::parquet::build_parquet_column_schema(*schema_descriptor, &file_schema).ok());
    return file_schema;
}

int64_t count_range_rows(const std::vector<format::parquet::RowRange>& ranges) {
    int64_t rows = 0;
    for (const auto& range : ranges) {
        rows += range.length;
    }
    return rows;
}

class ParquetScanTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = std::filesystem::temp_directory_path() / "doris_format_v2_parquet_scan_test";
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
        _file_path = (_test_dir / "scan.parquet").string();
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

    std::unique_ptr<format::parquet::ParquetReader> create_reader(
            int64_t range_start_offset = 0, int64_t range_size = -1,
            RuntimeProfile* profile = nullptr,
            std::optional<format::GlobalRowIdContext> global_rowid_context = std::nullopt,
            std::shared_ptr<io::IOContext> io_ctx = nullptr) const {
        auto system_properties = std::make_shared<io::FileSystemProperties>();
        system_properties->system_type = TFileType::FILE_LOCAL;
        auto file_description = std::make_unique<io::FileDescription>();
        file_description->path = _file_path;
        file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));
        file_description->range_start_offset = range_start_offset;
        file_description->range_size = range_size;
        return std::make_unique<format::parquet::ParquetReader>(system_properties, file_description,
                                                                std::move(io_ctx), profile,
                                                                global_rowid_context);
    }

    std::shared_ptr<format::FileScanRequest> open_all_row_groups(
            format::parquet::ParquetReader* reader) {
        auto request = std::make_shared<format::FileScanRequest>();
        EXPECT_TRUE(reader->open(request).ok());
        return request;
    }

    std::filesystem::path _test_dir;
    std::string _file_path;
};

TEST(ParquetScanSelectionTest, CompactFilterShrinksCurrentSelection) {
    format::parquet::SelectionVector selection(4);
    selection.set_index(0, 0);
    selection.set_index(1, 2);
    selection.set_index(2, 4);
    selection.set_index(3, 5);

    const IColumn::Filter compact_filter {1, 0, 1, 0};
    const auto selected_rows =
            format::parquet::apply_compact_filter_to_selection(compact_filter, &selection, 4);

    ASSERT_EQ(selected_rows, 2);
    EXPECT_EQ(selection.get_index(0), 0);
    EXPECT_EQ(selection.get_index(1), 4);
    EXPECT_TRUE(selection.verify(selected_rows, 6).ok());
}

TEST_F(ParquetScanTest, PlanRowGroupsAppliesScanRangeBeforeStatistics) {
    write_int_pair_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);
    auto file_schema = build_file_schema(*parquet_file_reader);

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GE, 5));

    const auto [range_start_offset, range_size] = row_group_mid_range(_file_path, 1);
    format::parquet::ParquetScanRange scan_range;
    scan_range.start_offset = range_start_offset;
    scan_range.size = range_size;
    scan_range.file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));

    format::parquet::RowGroupScanPlan plan;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                         parquet_file_reader.get(), file_schema,
                                                         request, scan_range, false, &plan)
                        .ok());
    EXPECT_TRUE(plan.row_groups.empty());
    EXPECT_EQ(plan.pruning_stats.total_row_groups, 3);
    EXPECT_EQ(plan.pruning_stats.selected_row_groups, 0);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_statistics, 1);
    EXPECT_EQ(plan.pruning_stats.filtered_group_rows, 2);
}

TEST_F(ParquetScanTest, PlanRowGroupsPreservesFirstFileRowAcrossPrunedRowGroups) {
    write_int_pair_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);
    auto file_schema = build_file_schema(*parquet_file_reader);

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GE, 5));

    format::parquet::RowGroupScanPlan plan;
    format::parquet::ParquetScanRange scan_range;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                         parquet_file_reader.get(), file_schema,
                                                         request, scan_range, false, &plan)
                        .ok());
    ASSERT_EQ(plan.row_groups.size(), 1);
    EXPECT_EQ(plan.row_groups[0].row_group_id, 2);
    EXPECT_EQ(plan.row_groups[0].first_file_row, 4);
    EXPECT_EQ(plan.row_groups[0].row_group_rows, 2);
    ASSERT_EQ(plan.row_groups[0].selected_ranges.size(), 1);
    EXPECT_EQ(plan.row_groups[0].selected_ranges[0].start, 0);
    EXPECT_EQ(plan.row_groups[0].selected_ranges[0].length, 2);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_statistics, 2);
    EXPECT_EQ(plan.pruning_stats.filtered_group_rows, 4);
}

TEST_F(ParquetScanTest, PlanRowGroupsSelectsAllRowGroupsWithoutFilters) {
    write_int_pair_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);
    auto file_schema = build_file_schema(*parquet_file_reader);

    format::FileScanRequest request;
    format::parquet::RowGroupScanPlan plan;
    format::parquet::ParquetScanRange scan_range;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                         parquet_file_reader.get(), file_schema,
                                                         request, scan_range, false, &plan)
                        .ok());

    ASSERT_EQ(plan.row_groups.size(), 3);
    EXPECT_EQ(plan.pruning_stats.total_row_groups, 3);
    EXPECT_EQ(plan.pruning_stats.selected_row_groups, 3);
    for (size_t row_group_idx = 0; row_group_idx < plan.row_groups.size(); ++row_group_idx) {
        EXPECT_EQ(plan.row_groups[row_group_idx].row_group_id, row_group_idx);
        EXPECT_EQ(plan.row_groups[row_group_idx].first_file_row,
                  static_cast<int64_t>(row_group_idx * 2));
        ASSERT_EQ(plan.row_groups[row_group_idx].selected_ranges.size(), 1);
        EXPECT_EQ(plan.row_groups[row_group_idx].selected_ranges[0].start, 0);
        EXPECT_EQ(plan.row_groups[row_group_idx].selected_ranges[0].length, 2);
        EXPECT_TRUE(plan.row_groups[row_group_idx].page_skip_plans.empty());
    }
}

TEST(ParquetScanConditionCacheTest, HitKeepsCachedBaseWhenCurrentPlanStartsLater) {
    format::parquet::RowGroupScanPlan plan;
    plan.row_groups.push_back(
            {.row_group_id = 1,
             .first_file_row = ConditionCacheContext::GRANULE_SIZE,
             .row_group_rows = ConditionCacheContext::GRANULE_SIZE,
             .selected_ranges = {{.start = 0, .length = ConditionCacheContext::GRANULE_SIZE}},
             .page_skip_plans = {}});

    format::parquet::ParquetScanScheduler scheduler;
    scheduler.set_plan(std::move(plan));
    auto ctx = std::make_shared<ConditionCacheContext>();
    ctx->is_hit = true;
    ctx->base_granule = 0;
    ctx->filter_result = std::make_shared<std::vector<bool>>(std::vector<bool> {false});
    scheduler.set_condition_cache_context(ctx);

    EXPECT_FALSE(scheduler.empty());
    EXPECT_EQ(scheduler.condition_cache_filtered_rows(), 0);
    EXPECT_EQ(ctx->base_granule, 0);
}

TEST_F(ParquetScanTest, PageIndexIntersectsMultipleFiltersAndBuildsSkipPlan) {
    write_page_index_pair_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 1);
    auto file_schema = build_file_schema(*parquet_file_reader);

    format::FileScanRequest single_filter_request;
    format::FileScanRequestBuilder single_filter_builder(&single_filter_request);
    ASSERT_TRUE(single_filter_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    single_filter_request.conjuncts.push_back(
            create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GE, 32));
    format::parquet::RowGroupScanPlan single_filter_plan;
    format::parquet::ParquetScanRange scan_range;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(
                        *parquet_file_reader->metadata(), parquet_file_reader.get(), file_schema,
                        single_filter_request, scan_range, false, &single_filter_plan)
                        .ok());
    ASSERT_EQ(single_filter_plan.row_groups.size(), 1);
    const int64_t single_filter_rows =
            count_range_rows(single_filter_plan.row_groups[0].selected_ranges);

    format::FileScanRequest intersect_request;
    format::FileScanRequestBuilder intersect_builder(&intersect_request);
    ASSERT_TRUE(intersect_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(intersect_builder.add_predicate_column(format::LocalColumnId(1)).ok());
    intersect_request.conjuncts.push_back(
            create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GE, 32));
    intersect_request.conjuncts.push_back(
            create_int32_zonemap_conjunct(1, Int32ZoneMapExpr::Op::LT, 96));
    format::parquet::RowGroupScanPlan intersect_plan;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(
                        *parquet_file_reader->metadata(), parquet_file_reader.get(), file_schema,
                        intersect_request, scan_range, false, &intersect_plan)
                        .ok());
    ASSERT_EQ(intersect_plan.row_groups.size(), 1);
    ASSERT_FALSE(intersect_plan.row_groups[0].selected_ranges.empty());
    const int64_t intersect_rows = count_range_rows(intersect_plan.row_groups[0].selected_ranges);
    EXPECT_GT(single_filter_rows, intersect_rows);
    EXPECT_GT(intersect_plan.row_groups[0].selected_ranges.front().start, 0);
    const auto& last_range = intersect_plan.row_groups[0].selected_ranges.back();
    EXPECT_LT(last_range.start + last_range.length, 128);
    EXPECT_GT(intersect_plan.pruning_stats.filtered_page_rows, 0);
    EXPECT_EQ(intersect_plan.pruning_stats.selected_row_ranges,
              intersect_plan.row_groups[0].selected_ranges.size());

    auto id_skip_plan = intersect_plan.row_groups[0].page_skip_plans.find(0);
    ASSERT_NE(id_skip_plan, intersect_plan.row_groups[0].page_skip_plans.end());
    EXPECT_EQ(id_skip_plan->second.leaf_column_id, 0);
    EXPECT_FALSE(id_skip_plan->second.empty());
    auto score_skip_plan = intersect_plan.row_groups[0].page_skip_plans.find(1);
    ASSERT_NE(score_skip_plan, intersect_plan.row_groups[0].page_skip_plans.end());
    EXPECT_EQ(score_skip_plan->second.leaf_column_id, 1);
    EXPECT_FALSE(score_skip_plan->second.empty());
}

TEST_F(ParquetScanTest, PageIndexCanFullyFilterRowGroupAfterRangeIntersection) {
    write_page_index_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 1);
    auto file_schema = build_file_schema(*parquet_file_reader);

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GE, 32));
    request.conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::LT, 32));

    format::parquet::RowGroupScanPlan plan;
    format::parquet::ParquetScanRange scan_range;
    ASSERT_TRUE(format::parquet::plan_parquet_row_groups(*parquet_file_reader->metadata(),
                                                         parquet_file_reader.get(), file_schema,
                                                         request, scan_range, false, &plan)
                        .ok());
    EXPECT_TRUE(plan.row_groups.empty());
    EXPECT_EQ(plan.pruning_stats.total_row_groups, 1);
    EXPECT_EQ(plan.pruning_stats.selected_row_groups, 0);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_statistics, 0);
    EXPECT_EQ(plan.pruning_stats.filtered_row_groups_by_page_index, 1);
    EXPECT_EQ(plan.pruning_stats.filtered_page_rows, 128);
}

TEST_F(ParquetScanTest, PageIndexFullRangeWhenDisabledOrUnavailable) {
    write_page_index_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    auto file_schema = build_file_schema(*parquet_file_reader);

    format::FileScanRequest request;
    request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request.conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 63));

    const bool old_enable_page_index = config::enable_parquet_page_index;
    config::enable_parquet_page_index = false;
    std::vector<format::parquet::RowRange> selected_ranges;
    std::map<int, format::parquet::ParquetPageSkipPlan> page_skip_plans;
    format::parquet::ParquetPruningStats pruning_stats;
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_page_index(
                        parquet_file_reader.get(), file_schema, request, 0, 128, &selected_ranges,
                        &page_skip_plans, &pruning_stats)
                        .ok());
    config::enable_parquet_page_index = old_enable_page_index;
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 0);
    EXPECT_EQ(selected_ranges[0].length, 128);
    EXPECT_TRUE(page_skip_plans.empty());
    EXPECT_EQ(pruning_stats.page_index_read_calls, 0);

    write_int_pair_parquet_file(_file_path, 6);
    auto no_index_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    auto no_index_schema = build_file_schema(*no_index_reader);
    format::FileScanRequest no_index_request;
    no_index_request.local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    no_index_request.conjuncts.push_back(
            create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 3));
    selected_ranges.clear();
    page_skip_plans.clear();
    pruning_stats = {};
    ASSERT_TRUE(format::parquet::select_row_group_ranges_by_page_index(
                        no_index_reader.get(), no_index_schema, no_index_request, 0, 6,
                        &selected_ranges, &page_skip_plans, &pruning_stats)
                        .ok());
    ASSERT_EQ(selected_ranges.size(), 1);
    EXPECT_EQ(selected_ranges[0].start, 0);
    EXPECT_EQ(selected_ranges[0].length, 6);
    EXPECT_TRUE(page_skip_plans.empty());
}

TEST_F(ParquetScanTest, AggregateCountAndMinMaxUseAllSelectedRowGroups) {
    write_int_pair_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    open_all_row_groups(reader.get());

    format::FileAggregateResult count_result;
    format::FileAggregateRequest count_request;
    count_request.agg_type = TPushAggOp::COUNT;
    ASSERT_TRUE(reader->get_aggregate_result(count_request, &count_result).ok());
    EXPECT_EQ(count_result.count, 6);
    EXPECT_TRUE(count_result.columns.empty());

    format::FileAggregateResult minmax_result;
    format::FileAggregateRequest minmax_request;
    minmax_request.agg_type = TPushAggOp::MINMAX;
    minmax_request.columns.push_back({.projection = field_projection(0)});
    minmax_request.columns.push_back({.projection = field_projection(1)});
    ASSERT_TRUE(reader->get_aggregate_result(minmax_request, &minmax_result).ok());
    EXPECT_EQ(minmax_result.count, 6);
    ASSERT_EQ(minmax_result.columns.size(), 2);
    EXPECT_TRUE(minmax_result.columns[0].has_min);
    EXPECT_TRUE(minmax_result.columns[0].has_max);
    EXPECT_EQ(minmax_result.columns[0].min_value.get<TYPE_INT>(), 1);
    EXPECT_EQ(minmax_result.columns[0].max_value.get<TYPE_INT>(), 6);
    EXPECT_EQ(minmax_result.columns[1].min_value.get<TYPE_INT>(), 10);
    EXPECT_EQ(minmax_result.columns[1].max_value.get<TYPE_INT>(), 60);
}

TEST_F(ParquetScanTest, AggregateMinMaxRejectsInexactBinaryStatistics) {
    write_binary_minmax_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    open_all_row_groups(reader.get());

    for (int32_t column_id = 0; column_id < 2; ++column_id) {
        format::FileAggregateRequest request;
        request.agg_type = TPushAggOp::MINMAX;
        request.columns.push_back({.projection = field_projection(column_id)});
        format::FileAggregateResult result;
        const auto status = reader->get_aggregate_result(request, &result);
        EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    }
}

TEST_F(ParquetScanTest, AggregateRespectsStatisticsPrunedRowGroups) {
    write_int_pair_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GE, 5));
    ASSERT_TRUE(reader->open(request).ok());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::MINMAX;
    aggregate_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &result).ok());
    EXPECT_EQ(result.count, 2);
    ASSERT_EQ(result.columns.size(), 1);
    EXPECT_EQ(result.columns[0].min_value.get<TYPE_INT>(), 5);
    EXPECT_EQ(result.columns[0].max_value.get<TYPE_INT>(), 6);
}

TEST_F(ParquetScanTest, AggregateCountKeepsRowGroupRowsAfterPageIndexPruning) {
    write_page_index_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 63));
    ASSERT_TRUE(reader->open(request).ok());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::COUNT;
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &result).ok());
    EXPECT_EQ(result.count, 128);
}

TEST_F(ParquetScanTest, AggregateMinMaxSupportsNestedSingleLeafProjection) {
    write_struct_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    open_all_row_groups(reader.get());

    format::LocalColumnIndex nested_id = format::LocalColumnIndex::partial_local(0);
    nested_id.children.push_back(field_projection(0));
    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::MINMAX;
    aggregate_request.columns.push_back({.projection = nested_id});
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &result).ok());
    EXPECT_EQ(result.count, 4);
    ASSERT_EQ(result.columns.size(), 1);
    EXPECT_EQ(result.columns[0].min_value.get<TYPE_INT>(), 1);
    EXPECT_EQ(result.columns[0].max_value.get<TYPE_INT>(), 11);
}

TEST_F(ParquetScanTest, AggregateCountOnStructRecordsSelectedRowsRead) {
    write_struct_parquet_file(_file_path);
    io::FileReaderStats file_reader_stats;
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = &file_reader_stats;
    auto reader = create_reader(0, -1, nullptr, std::nullopt, io_ctx);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    open_all_row_groups(reader.get());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::COUNT;
    aggregate_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult result;
    ASSERT_TRUE(reader->get_aggregate_result(aggregate_request, &result).ok());
    EXPECT_EQ(result.count, 4);
    EXPECT_EQ(file_reader_stats.read_rows, 4);
}

TEST_F(ParquetScanTest, AggregateCountOnStructReturnsEndOfFileWhenStopped) {
    write_struct_parquet_file(_file_path);
    io::FileReaderStats file_reader_stats;
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = &file_reader_stats;
    auto reader = create_reader(0, -1, nullptr, std::nullopt, io_ctx);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    open_all_row_groups(reader.get());
    io_ctx->should_stop = true;

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::COUNT;
    aggregate_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult result;
    const auto status = reader->get_aggregate_result(aggregate_request, &result);
    EXPECT_TRUE(status.is<ErrorCode::END_OF_FILE>()) << status;
    EXPECT_EQ(file_reader_stats.read_rows, 0);
}

TEST_F(ParquetScanTest, AggregateRejectsRepeatedMissingStatisticsAndInvalidRequests) {
    write_list_parquet_file(_file_path);
    auto repeated_reader = create_reader();
    RuntimeState repeated_state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(repeated_reader->init(&repeated_state).ok());
    open_all_row_groups(repeated_reader.get());

    format::FileAggregateRequest repeated_request;
    repeated_request.agg_type = TPushAggOp::MINMAX;
    repeated_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult repeated_result;
    EXPECT_FALSE(repeated_reader->get_aggregate_result(repeated_request, &repeated_result).ok());

    write_int_pair_parquet_file(_file_path, 2, false);
    auto no_stats_reader = create_reader();
    RuntimeState no_stats_state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(no_stats_reader->init(&no_stats_state).ok());
    open_all_row_groups(no_stats_reader.get());
    format::FileAggregateRequest no_stats_request;
    no_stats_request.agg_type = TPushAggOp::MINMAX;
    no_stats_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult no_stats_result;
    EXPECT_FALSE(no_stats_reader->get_aggregate_result(no_stats_request, &no_stats_result).ok());

    format::FileAggregateRequest invalid_type_request;
    invalid_type_request.agg_type = TPushAggOp::MIX;
    format::FileAggregateResult invalid_type_result;
    EXPECT_FALSE(
            no_stats_reader->get_aggregate_result(invalid_type_request, &invalid_type_result).ok());

    format::FileAggregateRequest invalid_column_request;
    invalid_column_request.agg_type = TPushAggOp::MINMAX;
    invalid_column_request.columns.push_back({.projection = field_projection(100)});
    format::FileAggregateResult invalid_column_result;
    EXPECT_FALSE(
            no_stats_reader->get_aggregate_result(invalid_column_request, &invalid_column_result)
                    .ok());
}

TEST_F(ParquetScanTest, GlobalRowIdUsesFileLocalPositionForScanRange) {
    write_int_pair_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);
    const auto [range_start_offset, range_size] = row_group_mid_range(_file_path, 1);
    format::GlobalRowIdContext context {.version = 7, .backend_id = 123456789, .file_id = 42};
    auto reader = create_reader(range_start_offset, range_size, nullptr, context);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0),
                                      field_projection(format::GLOBAL_ROWID_COLUMN_ID)};
    use_schema_order_positions(request.get(), schema);
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<uint32_t> row_ids;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = int32_data_column(*block.get_by_position(0).column);
        const auto& rowid_column = string_data_column(*block.get_by_position(2).column);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            const auto location = decode_rowid(rowid_column, row);
            EXPECT_EQ(location.version, context.version);
            EXPECT_EQ(location.backend_id, context.backend_id);
            EXPECT_EQ(location.file_id, context.file_id);
            row_ids.push_back(location.row_id);
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({3, 4}));
    EXPECT_EQ(row_ids, std::vector<uint32_t>({2, 3}));
}

TEST_F(ParquetScanTest, EmptyScanPlanReturnsEofWithoutReadingColumns) {
    write_int_pair_parquet_file(_file_path, 2);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GE, 100));
    ASSERT_TRUE(reader->open(request).ok());

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 0);
    EXPECT_TRUE(eof);
}

TEST_F(ParquetScanTest, NoRequestedColumnsReturnsRowsOnlyAcrossRowGroups) {
    write_int_pair_parquet_file(_file_path, 2);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    size_t total_rows = 0;
    bool eof = false;
    while (!eof) {
        Block block;
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        EXPECT_EQ(block.columns(), 0);
        total_rows += rows;
    }
    EXPECT_EQ(total_rows, 6);
}

TEST_F(ParquetScanTest, PredicateColumnsFilterRoundByRound) {
    write_int_pair_parquet_file(_file_path, 6, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 2));
    request->conjuncts.push_back(create_int32_zonemap_conjunct(1, Int32ZoneMapExpr::Op::LT, 50));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<int32_t> scores;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = int32_data_column(*block.get_by_position(0).column);
        const auto& score_column = int32_data_column(*block.get_by_position(1).column);
        ASSERT_EQ(id_column.size(), rows);
        ASSERT_EQ(score_column.size(), rows);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            scores.push_back(score_column.get_element(row));
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({3, 4}));
    EXPECT_EQ(scores, std::vector<int32_t>({30, 40}));
    EXPECT_EQ(counter_value(profile, "RawRowsRead"), 6);
    EXPECT_EQ(counter_value(profile, "SelectedRows"), 2);
    EXPECT_EQ(counter_value(profile, "RowsFilteredByConjunct"), 4);
    EXPECT_EQ(counter_value(profile, "ReaderReadRows"), 10);
    EXPECT_EQ(counter_value(profile, "ReaderSelectRows"), 4);
    EXPECT_EQ(counter_value(profile, "ReaderSkipRows"), 2);
}

TEST_F(ParquetScanTest, PredicateColumnsSkipUnreadColumnsWhenFirstPredicateFiltersAll) {
    write_int_pair_parquet_file(_file_path, 6, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 100));
    request->conjuncts.push_back(create_int32_zonemap_conjunct(1, Int32ZoneMapExpr::Op::LT, 50));
    ASSERT_TRUE(reader->open(request).ok());

    size_t total_rows = 0;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        total_rows += rows;
    }

    EXPECT_EQ(total_rows, 0);
    EXPECT_EQ(counter_value(profile, "RawRowsRead"), 6);
    EXPECT_EQ(counter_value(profile, "SelectedRows"), 0);
    EXPECT_EQ(counter_value(profile, "RowsFilteredByConjunct"), 6);
    EXPECT_EQ(counter_value(profile, "EmptySelectionBatches"), 1);
    EXPECT_EQ(counter_value(profile, "ReaderReadRows"), 6);
    EXPECT_EQ(counter_value(profile, "ReaderSelectRows"), 0);
    EXPECT_EQ(counter_value(profile, "ReaderSkipRows"), 6);
}

// Scenario: every physical batch in every row group is rejected. Predicate readers reach each row
// group boundary, while the lazy score reader remains at row 0. The boundary reset must discard that
// reader and its pending lag instead of issuing SkipRecords for values that can never be observed.
TEST_F(ParquetScanTest, FullyFilteredRowGroupsDropPendingLazyReaders) {
    write_int_pair_parquet_file(_file_path, 2, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(1);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 100));
    ASSERT_TRUE(reader->open(request).ok());

    size_t total_rows = 0;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        total_rows += rows;
    }

    EXPECT_EQ(total_rows, 0);
    EXPECT_EQ(counter_value(profile, "EmptySelectionBatches"), 6);
    EXPECT_EQ(counter_value(profile, "ReaderSkipRows"), 0);
    ASSERT_NE(profile.get_counter("ArrowSkipRecordsTime"), nullptr);
    EXPECT_EQ(profile.get_counter("ArrowSkipRecordsTime")->value(), 0);
}

// Scenario: row group 0 is fully filtered and leaves two pending lazy rows. Reset must discard that
// lag before row group 1 creates fresh readers at its own row 0; otherwise score 30 would be skipped
// as if it belonged to the rejected prefix from the previous row group.
TEST_F(ParquetScanTest, PendingLazySkipDoesNotCrossRowGroupReset) {
    write_int_pair_parquet_file(_file_path, 2, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(1);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 2));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> scores;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& score_column = int32_data_column(*block.get_by_position(1).column);
        for (size_t row = 0; row < rows; ++row) {
            scores.push_back(score_column.get_element(row));
        }
    }

    EXPECT_EQ(scores, std::vector<int32_t>({30, 40, 50, 60}));
    EXPECT_EQ(counter_value(profile, "ReaderSkipRows"), 0);
}

// Scenario: a nested lazy column stays behind while id=1 is rejected. Flushing skip(1) must consume
// the complete repetition/definition-level span for the first list, then materialize the remaining
// two parent rows without corrupting their child boundaries.
TEST_F(ParquetScanTest, PendingLazySkipPreservesNestedRowBoundaries) {
    write_int_list_parquet_file(_file_path);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    reader->set_batch_size(1);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_non_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 1));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<size_t> list_lengths;
    std::vector<int32_t> list_values;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        const IColumn* list_column = block.get_by_position(1).column.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(list_column)) {
            list_column = &nullable->get_nested_column();
        }
        const auto& array_column = assert_cast<const ColumnArray&>(*list_column);
        const auto& value_column = int32_data_column(array_column.get_data());
        for (size_t row = 0; row < rows; ++row) {
            const size_t list_start = array_column.offset_at(row);
            const size_t list_length = array_column.size_at(row);
            list_lengths.push_back(list_length);
            for (size_t element = 0; element < list_length; ++element) {
                list_values.push_back(value_column.get_element(list_start + element));
            }
        }
    }

    EXPECT_EQ(list_lengths, std::vector<size_t>({1, 0}));
    EXPECT_EQ(list_values, std::vector<int32_t>({3}));
    EXPECT_EQ(counter_value(profile, "ReaderSkipRows"), 1);
}

TEST_F(ParquetScanTest, MultiColumnPredicateWaitsForAllPredicateColumns) {
    write_int_pair_parquet_file(_file_path, 6, false);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(1)).ok());
    request->conjuncts.push_back(create_int32_pair_sum_conjunct(0, 1, 45));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    std::vector<int32_t> scores;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = int32_data_column(*block.get_by_position(0).column);
        const auto& score_column = int32_data_column(*block.get_by_position(1).column);
        ASSERT_EQ(id_column.size(), rows);
        ASSERT_EQ(score_column.size(), rows);
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
            scores.push_back(score_column.get_element(row));
        }
    }

    EXPECT_EQ(ids, std::vector<int32_t>({1, 2, 3, 4}));
    EXPECT_EQ(scores, std::vector<int32_t>({10, 20, 30, 40}));
    EXPECT_EQ(counter_value(profile, "RawRowsRead"), 6);
    EXPECT_EQ(counter_value(profile, "SelectedRows"), 4);
    EXPECT_EQ(counter_value(profile, "RowsFilteredByConjunct"), 2);
    EXPECT_EQ(counter_value(profile, "ReaderReadRows"), 12);
    EXPECT_EQ(counter_value(profile, "ReaderSelectRows"), 0);
}

TEST_F(ParquetScanTest, ProfileCountersReflectPageIndexAndRangeGapPruning) {
    write_page_index_parquet_file(_file_path);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(request.get());
    ASSERT_TRUE(request_builder.add_predicate_column(format::LocalColumnId(0)).ok());
    request->conjuncts.push_back(create_int32_zonemap_conjunct(0, Int32ZoneMapExpr::Op::GT, 63));
    ASSERT_TRUE(reader->open(request).ok());

    size_t total_rows = 0;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        total_rows += rows;
    }

    EXPECT_EQ(total_rows, 64);
    ASSERT_NE(profile.get_counter("RowGroupsTotalNum"), nullptr);
    ASSERT_NE(profile.get_counter("RowGroupsReadNum"), nullptr);
    ASSERT_NE(profile.get_counter("FilteredRowsByPage"), nullptr);
    ASSERT_NE(profile.get_counter("SelectedRowRanges"), nullptr);
    ASSERT_NE(profile.get_counter("PageIndexReadCalls"), nullptr);
    ASSERT_NE(profile.get_counter("RawRowsRead"), nullptr);
    ASSERT_NE(profile.get_counter("RangeGapSkippedRows"), nullptr);
    EXPECT_EQ(profile.get_counter("RowGroupsTotalNum")->value(), 1);
    EXPECT_EQ(profile.get_counter("RowGroupsReadNum")->value(), 1);
    EXPECT_GT(profile.get_counter("FilteredRowsByPage")->value(), 0);
    EXPECT_GT(profile.get_counter("SelectedRowRanges")->value(), 0);
    EXPECT_GT(profile.get_counter("PageIndexReadCalls")->value(), 0);
    EXPECT_EQ(profile.get_counter("RawRowsRead")->value(), 64);
    EXPECT_GT(profile.get_counter("RangeGapSkippedRows")->value(), 0);
}

} // namespace
} // namespace doris
