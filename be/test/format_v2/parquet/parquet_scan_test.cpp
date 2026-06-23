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
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "storage/predicate/predicate_creator.h"
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

format::FileColumnPredicateFilter int32_filter(int32_t column_id, std::string column_name,
                                               const DataTypePtr& type,
                                               PredicateType predicate_type, int32_t value) {
    format::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = format::LocalColumnId(column_id);
    switch (predicate_type) {
    case PredicateType::GE:
        column_filter.predicates.push_back(create_comparison_predicate<PredicateType::GE>(
                column_id, column_name, type, Field::create_field<TYPE_INT>(value), false));
        break;
    case PredicateType::GT:
        column_filter.predicates.push_back(create_comparison_predicate<PredicateType::GT>(
                column_id, column_name, type, Field::create_field<TYPE_INT>(value), false));
        break;
    case PredicateType::LT:
        column_filter.predicates.push_back(create_comparison_predicate<PredicateType::LT>(
                column_id, column_name, type, Field::create_field<TYPE_INT>(value), false));
        break;
    default:
        DORIS_CHECK(false);
    }
    return column_filter;
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
            std::optional<format::GlobalRowIdContext> global_rowid_context = std::nullopt) const {
        auto system_properties = std::make_shared<io::FileSystemProperties>();
        system_properties->system_type = TFileType::FILE_LOCAL;
        auto file_description = std::make_unique<io::FileDescription>();
        file_description->path = _file_path;
        file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));
        file_description->range_start_offset = range_start_offset;
        file_description->range_size = range_size;
        return std::make_unique<format::parquet::ParquetReader>(
                system_properties, file_description, nullptr, profile, global_rowid_context);
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

TEST_F(ParquetScanTest, PlanRowGroupsAppliesScanRangeBeforeStatistics) {
    write_int_pair_parquet_file(_file_path, 2);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 3);
    auto file_schema = build_file_schema(*parquet_file_reader);

    format::FileScanRequest request;
    request.column_predicate_filters.push_back(
            int32_filter(0, "id", file_schema[0]->type, PredicateType::GE, 5));

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
    request.column_predicate_filters.push_back(
            int32_filter(0, "id", file_schema[0]->type, PredicateType::GE, 5));

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

TEST_F(ParquetScanTest, PageIndexIntersectsMultipleFiltersAndBuildsSkipPlan) {
    write_page_index_pair_parquet_file(_file_path);
    auto parquet_file_reader = ::parquet::ParquetFileReader::OpenFile(_file_path, false);
    ASSERT_EQ(parquet_file_reader->metadata()->num_row_groups(), 1);
    auto file_schema = build_file_schema(*parquet_file_reader);

    format::FileScanRequest single_filter_request;
    single_filter_request.column_predicate_filters.push_back(
            int32_filter(0, "id", file_schema[0]->type, PredicateType::GE, 32));
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
    intersect_request.column_predicate_filters.push_back(
            int32_filter(0, "id", file_schema[0]->type, PredicateType::GE, 32));
    intersect_request.column_predicate_filters.push_back(
            int32_filter(1, "score", file_schema[1]->type, PredicateType::LT, 96));
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
    request.column_predicate_filters.push_back(
            int32_filter(0, "id", file_schema[0]->type, PredicateType::GE, 32));
    request.column_predicate_filters.push_back(
            int32_filter(0, "id", file_schema[0]->type, PredicateType::LT, 32));

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
    request.column_predicate_filters.push_back(
            int32_filter(0, "id", file_schema[0]->type, PredicateType::GT, 63));

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
    no_index_request.column_predicate_filters.push_back(
            int32_filter(0, "id", no_index_schema[0]->type, PredicateType::GT, 3));
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

TEST_F(ParquetScanTest, AggregateRespectsStatisticsPrunedRowGroups) {
    write_int_pair_parquet_file(_file_path);
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = format::LocalColumnId(0);
    column_filter.predicates.push_back(create_comparison_predicate<PredicateType::GE>(
            0, "id", schema[0].type, Field::create_field<TYPE_INT>(5), false));
    request->column_predicate_filters.push_back(std::move(column_filter));
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

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    format::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = format::LocalColumnId(0);
    column_filter.predicates.push_back(create_comparison_predicate<PredicateType::GT>(
            0, "id", schema[0].type, Field::create_field<TYPE_INT>(63), false));
    request->column_predicate_filters.push_back(std::move(column_filter));
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
    format::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = format::LocalColumnId(0);
    column_filter.predicates.push_back(create_comparison_predicate<PredicateType::GE>(
            0, "id", schema[0].type, Field::create_field<TYPE_INT>(100), false));
    request->column_predicate_filters.push_back(std::move(column_filter));
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

TEST_F(ParquetScanTest, ProfileCountersReflectPageIndexAndRangeGapPruning) {
    write_page_index_parquet_file(_file_path);
    RuntimeProfile profile("profile");
    auto reader = create_reader(0, -1, &profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    use_schema_order_positions(request.get(), schema);
    format::FileColumnPredicateFilter column_filter;
    column_filter.file_column_id = format::LocalColumnId(0);
    column_filter.predicates.push_back(create_comparison_predicate<PredicateType::GT>(
            0, "id", schema[0].type, Field::create_field<TYPE_INT>(63), false));
    request->column_predicate_filters.push_back(std::move(column_filter));
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
