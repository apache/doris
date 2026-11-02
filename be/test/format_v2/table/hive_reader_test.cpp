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

#include "format_v2/table/hive_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exec/scan/access_path_parser.h"
#include "format_v2/column_data.h"
#include "format_v2/column_mapper.h"
#include "format_v2/schema_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris::format::hive {
namespace {

ColumnDefinition table_column(const std::string& name, DataTypePtr type) {
    ColumnDefinition column;
    column.identifier = Field::create_field<TYPE_STRING>(name);
    column.name = name;
    column.type = std::move(type);
    return column;
}

ColumnDefinition file_column(const std::string& name, DataTypePtr type, int32_t local_id) {
    ColumnDefinition column;
    column.local_id = local_id;
    column.name = name;
    column.type = std::move(type);
    return column;
}

Status init_hive_reader(FileFormat format, TFileScanRangeParams* params, RuntimeState* state,
                        RuntimeProfile* profile, HiveReader* reader,
                        std::vector<ColumnDefinition> projected_columns) {
    return reader->init({
            .projected_columns = std::move(projected_columns),
            .conjuncts = {},
            .format = format,
            .scan_params = params,
            .io_ctx = nullptr,
            .runtime_state = state,
            .scanner_profile = profile,
    });
}

Status init_hive_reader(FileFormat format, TFileScanRangeParams* params, RuntimeState* state,
                        RuntimeProfile* profile, HiveReader* reader) {
    return init_hive_reader(format, params, state, profile, reader,
                            {table_column("id", std::make_shared<DataTypeInt32>()),
                             table_column("name", std::make_shared<DataTypeString>())});
}

bool has_name_mapping(const ColumnDefinition& column, const std::string& name) {
    return std::ranges::find(column.name_mapping, name) != column.name_mapping.end();
}

class HiveV2ReaderTest : public testing::Test {
public:
    HiveV2ReaderTest() : state(query_options, query_globals), profile("hive_v2_reader_test") {}

protected:
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    RuntimeState state;
    RuntimeProfile profile;
};

// Scenario: Hive tables using OpenCSVSerde are planned as table_format=hive with CSV file format.
// HiveReader must allow that file format so TableReader can create the v2 CsvReader.
TEST_F(HiveV2ReaderTest, InitSupportsCsvFileFormat) {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    HiveReader reader;

    ASSERT_TRUE(init_hive_reader(FileFormat::CSV, &params, &state, &profile, &reader).ok());
    EXPECT_EQ(reader.mapping_mode(), TableColumnMappingMode::BY_NAME);
}

// Scenario: Hive text files also synthesize a file-local schema from FE slots, so they should use
// name mapping at the table-reader layer while TextReader consumes column_idxs for field ordinals.
TEST_F(HiveV2ReaderTest, InitSupportsTextFileFormat) {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_TEXT);
    HiveReader reader;

    ASSERT_TRUE(init_hive_reader(FileFormat::TEXT, &params, &state, &profile, &reader).ok());
    EXPECT_EQ(reader.mapping_mode(), TableColumnMappingMode::BY_NAME);
}

// Scenario: Hive JSON files also synthesize a file-local schema from FE slots, so they should use
// name mapping at the table-reader layer while JsonReader consumes JSON attributes.
TEST_F(HiveV2ReaderTest, InitSupportsJsonFileFormat) {
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_JSON);
    HiveReader reader;

    ASSERT_TRUE(init_hive_reader(FileFormat::JSON, &params, &state, &profile, &reader).ok());
    EXPECT_EQ(reader.mapping_mode(), TableColumnMappingMode::BY_NAME);
}

TEST_F(HiveV2ReaderTest, MappingModeUsesInitializedFormat) {
    query_options.hive_parquet_use_column_names = false;
    query_options.hive_orc_use_column_names = true;
    state.set_query_options(query_options);

    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    HiveReader reader;

    ASSERT_TRUE(init_hive_reader(FileFormat::PARQUET, &params, &state, &profile, &reader).ok());
    EXPECT_EQ(reader.mapping_mode(), TableColumnMappingMode::BY_INDEX);

    SplitReadOptions parquet_split;
    parquet_split.current_range.__set_path("split.parquet");
    parquet_split.current_split_format = FileFormat::PARQUET;
    ASSERT_TRUE(reader.prepare_split(parquet_split).ok());
    EXPECT_EQ(reader.mapping_mode(), TableColumnMappingMode::BY_INDEX);

    SplitReadOptions orc_split;
    orc_split.current_range.__set_path("split.orc");
    orc_split.current_split_format = FileFormat::ORC;
    EXPECT_FALSE(reader.prepare_split(orc_split).ok());
}

TEST_F(HiveV2ReaderTest, OrcConsumesColumnIdxsAsPositionalSchemaMapping) {
    query_options.hive_orc_use_column_names = false;
    state.set_query_options(query_options);

    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_ORC);
    params.__set_column_idxs({3});
    ProjectedColumnBuildContext context {
            .scan_params = &params,
            .runtime_state = &state,
    };
    HiveReader reader;

    TFileScanSlotInfo slot;
    slot.__set_is_file_slot(true);
    auto column = table_column("value", std::make_shared<DataTypeInt32>());

    ASSERT_TRUE(reader.annotate_projected_column(slot, &context, &column).ok());
    ASSERT_TRUE(column.has_identifier_field_id());
    EXPECT_EQ(column.get_identifier_position(), 3);
    EXPECT_EQ(context.next_file_column_idx, 1);
}

// Scenario: positional mapping is only for Hive Parquet/ORC sessions that disable name mapping.
// CSV keeps the synthesized file-column names and leaves column_idxs for the CsvReader itself.
TEST_F(HiveV2ReaderTest, CsvDoesNotConsumeColumnIdxsAsPositionalSchemaMapping) {
    query_options.hive_parquet_use_column_names = false;
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    params.__set_column_idxs({3});
    ProjectedColumnBuildContext context {
            .scan_params = &params,
            .runtime_state = &state,
    };
    HiveReader reader;

    TFileScanSlotInfo slot;
    slot.__set_is_file_slot(true);
    auto column = table_column("value", std::make_shared<DataTypeInt32>());

    ASSERT_TRUE(reader.annotate_projected_column(slot, &context, &column).ok());
    ASSERT_TRUE(column.has_identifier_name());
    EXPECT_EQ(column.get_identifier_name(), "value");
    EXPECT_EQ(context.next_file_column_idx, 0);
}

TEST_F(HiveV2ReaderTest, OrcHive1ColumnNamesUsePositionAliasesWhenNameMappingEnabled) {
    query_options.hive_orc_use_column_names = true;
    state.set_query_options(query_options);

    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_ORC);
    params.__set_column_idxs({0, 1, 2});
    std::vector<ColumnDefinition> projected_columns {
            table_column("a", std::make_shared<DataTypeUInt8>()),
            table_column("b", std::make_shared<DataTypeInt32>()),
            table_column("c", std::make_shared<DataTypeString>())};
    HiveReader reader;
    ASSERT_TRUE(
            init_hive_reader(FileFormat::ORC, &params, &state, &profile, &reader, projected_columns)
                    .ok());

    std::vector<ColumnDefinition> file_schema {
            file_column("_col0", std::make_shared<DataTypeUInt8>(), 0),
            file_column("_col1", std::make_shared<DataTypeInt32>(), 1),
            file_column("_col2", std::make_shared<DataTypeString>(), 2)};
    ASSERT_TRUE(reader.TEST_annotate_file_schema(&file_schema).ok());

    EXPECT_TRUE(has_name_mapping(file_schema[0], "a"));
    EXPECT_TRUE(has_name_mapping(file_schema[1], "b"));
    EXPECT_TRUE(has_name_mapping(file_schema[2], "c"));

    TableColumnMapper mapper({.mode = reader.mapping_mode()});
    ASSERT_TRUE(mapper.create_mapping(projected_columns, {}, file_schema).ok());
    ASSERT_EQ(mapper.mappings().size(), 3);
    ASSERT_TRUE(mapper.mappings()[0].file_local_id.has_value());
    ASSERT_TRUE(mapper.mappings()[1].file_local_id.has_value());
    ASSERT_TRUE(mapper.mappings()[2].file_local_id.has_value());
    EXPECT_EQ(*mapper.mappings()[0].file_local_id, 0);
    EXPECT_EQ(*mapper.mappings()[1].file_local_id, 1);
    EXPECT_EQ(*mapper.mappings()[2].file_local_id, 2);
}

struct NestedAccessPaths {
    std::string column_name;
    std::vector<std::vector<std::string>> all_paths;
    std::vector<std::vector<std::string>> predicate_paths;
};

std::vector<TColumnAccessPath> thrift_access_paths(
        const std::vector<std::vector<std::string>>& paths) {
    std::vector<TColumnAccessPath> result;
    for (const auto& path : paths) {
        TDataAccessPath data_path;
        data_path.__set_path(path);
        TColumnAccessPath access_path;
        access_path.__set_type(TAccessPathType::DATA);
        access_path.__set_data_access_path(data_path);
        result.push_back(std::move(access_path));
    }
    return result;
}

void collect_selected_ids(const ColumnDefinition& field, const LocalColumnIndex* projection,
                          bool selected, int* next_id, std::set<int>* ids) {
    const int id = ++*next_id;
    if (selected) {
        ids->insert(id);
    }
    for (const auto& child : field.children) {
        const auto* child_projection = find_child_projection(projection, child.local_id);
        collect_selected_ids(child, child_projection,
                             selected && is_child_projected(projection, child.local_id), next_id,
                             ids);
    }
}

std::set<int> selected_ids(const std::vector<ColumnDefinition>& schema,
                           const std::vector<LocalColumnIndex>& projections) {
    // V2 addresses children by sibling ordinal. Number the normalized schema in preorder to keep
    // the original ORC/Parquet regression matrix's IDs (profile=3, address=4, coordinates=7).
    int next_id = 0;
    std::set<int> ids;
    for (const auto& field : schema) {
        const auto it = std::ranges::find_if(projections, [&](const auto& projection) {
            return projection.local_id() == field.local_id;
        });
        const auto* projection = it == projections.end() ? nullptr : &*it;
        collect_selected_ids(field, projection, projection != nullptr, &next_id, &ids);
    }
    return ids;
}

void prune_column_type(ColumnDefinition* column) {
    for (auto& child : column->children) {
        prune_column_type(&child);
    }
    for (auto& child : column->predicate_children) {
        prune_column_type(&child);
    }
    switch (remove_nullable(column->type)->get_primitive_type()) {
    case TYPE_STRUCT: {
        DataTypes types;
        Strings names;
        for (const auto& child : column->children) {
            types.push_back(child.type);
            names.push_back(child.name);
        }
        column->type = make_nullable(std::make_shared<DataTypeStruct>(types, names));
        break;
    }
    case TYPE_ARRAY:
        column->type = make_nullable(std::make_shared<DataTypeArray>(column->children[0].type));
        break;
    case TYPE_MAP:
        column->type = make_nullable(
                std::make_shared<DataTypeMap>(column->children[0].type, column->children[1].type));
        break;
    default:
        break;
    }
}

void expect_selected_values(const ColumnDefinition& field, const LocalColumnIndex* projection,
                            const DataTypePtr& actual_type, const IColumn& actual,
                            const IColumn& expected);

void expect_selected_struct_values(const ColumnDefinition& field,
                                   const LocalColumnIndex* projection,
                                   const DataTypePtr& actual_type, const IColumn& actual,
                                   const IColumn& expected) {
    const auto& actual_struct = assert_cast<const ColumnStruct&>(actual);
    const auto& expected_struct = assert_cast<const ColumnStruct&>(expected);
    const auto& struct_type = assert_cast<const DataTypeStruct&>(*remove_nullable(actual_type));
    for (const auto& child : field.children) {
        if (is_child_projected(projection, child.local_id)) {
            const auto position = struct_type.get_position_by_name(child.name);
            expect_selected_values(child, find_child_projection(projection, child.local_id),
                                   struct_type.get_elements()[position],
                                   actual_struct.get_column(position),
                                   expected_struct.get_column(child.local_id));
        }
    }
}

void expect_selected_map_values(const ColumnDefinition& field, const LocalColumnIndex* projection,
                                const DataTypePtr& actual_type, const IColumn& actual,
                                const IColumn& expected) {
    const auto& actual_map = assert_cast<const ColumnMap&>(actual);
    const auto& expected_map = assert_cast<const ColumnMap&>(expected);
    const auto& map_type = assert_cast<const DataTypeMap&>(*remove_nullable(actual_type));
    EXPECT_EQ(actual_map.get_offsets(), expected_map.get_offsets());
    for (const auto& child : field.children) {
        if (is_child_projected(projection, child.local_id)) {
            expect_selected_values(
                    child, find_child_projection(projection, child.local_id),
                    child.local_id == 0 ? map_type.get_key_type() : map_type.get_value_type(),
                    child.local_id == 0 ? actual_map.get_keys() : actual_map.get_values(),
                    child.local_id == 0 ? expected_map.get_keys() : expected_map.get_values());
        }
    }
}

void expect_selected_values(const ColumnDefinition& field, const LocalColumnIndex* projection,
                            const DataTypePtr& actual_type, const IColumn& actual,
                            const IColumn& expected) {
    SCOPED_TRACE(field.name);
    ASSERT_EQ(actual.size(), expected.size());
    if (is_full_projection(projection)) {
        for (size_t row = 0; row < actual.size(); ++row) {
            EXPECT_EQ(field.type->to_string(actual, row), field.type->to_string(expected, row));
        }
        return;
    }
    ASSERT_TRUE(is_column_nullable(actual));
    ASSERT_TRUE(is_column_nullable(expected));
    const auto& actual_nullable = assert_cast<const ColumnNullable&>(actual);
    const auto& expected_nullable = assert_cast<const ColumnNullable&>(expected);
    EXPECT_EQ(actual_nullable.get_null_map_data(), expected_nullable.get_null_map_data());
    const auto& actual_nested = actual_nullable.get_nested_column();
    const auto& expected_nested = expected_nullable.get_nested_column();
    switch (remove_nullable(field.type)->get_primitive_type()) {
    case TYPE_STRUCT:
        expect_selected_struct_values(field, projection, actual_type, actual_nested,
                                      expected_nested);
        break;
    case TYPE_ARRAY: {
        const auto& actual_array = assert_cast<const ColumnArray&>(actual_nested);
        const auto& expected_array = assert_cast<const ColumnArray&>(expected_nested);
        EXPECT_EQ(actual_array.get_offsets(), expected_array.get_offsets());
        const auto& array_type = assert_cast<const DataTypeArray&>(*remove_nullable(actual_type));
        expect_selected_values(field.children[0], find_child_projection(projection, 0),
                               array_type.get_nested_type(), actual_array.get_data(),
                               expected_array.get_data());
        break;
    }
    case TYPE_MAP:
        expect_selected_map_values(field, projection, actual_type, actual_nested, expected_nested);
        break;
    default:
        FAIL() << "Unexpected partial projection for " << field.name;
    }
}

class HiveV2NestedAccessPathTest
        : public HiveV2ReaderTest,
          public testing::WithParamInterface<std::tuple<FileFormat, bool>> {
protected:
    void SetUp() override {
        const auto [format, use_names] = GetParam();
        query_options.__set_hive_orc_use_column_names(use_names);
        query_options.__set_hive_parquet_use_column_names(use_names);
        state.set_query_options(query_options);
        params.__set_file_type(TFileType::FILE_LOCAL);
        params.__set_format_type(format == FileFormat::ORC ? TFileFormatType::FORMAT_ORC
                                                           : TFileFormatType::FORMAT_PARQUET);
        const std::string path =
                format == FileFormat::ORC
                        ? "./be/test/exec/test_data/nested_user_profiles_orc/"
                          "part-00000-62614f23-05d1-4043-a533-b155ef52b720-c000.snappy.orc"
                        : "./be/test/exec/test_data/nested_user_profiles_parquet/"
                          "part-00000-64a7a390-1a03-4efc-ab51-557e9369a1f9-c000.snappy.parquet";
        ASSERT_TRUE(std::filesystem::exists(path)) << path;
        split.current_range.__set_path(path);
        split.current_range.__set_start_offset(0);
        split.current_range.__set_size(std::filesystem::file_size(path));
        split.current_range.__set_file_size(std::filesystem::file_size(path));
        split.current_split_format = format;
        ASSERT_TRUE(fetch_file_schema(params, split.current_range,
                                      std::make_shared<io::IOContext>(), &profile, &schema)
                            .ok());
        ASSERT_EQ(schema.size(), 8);
    }

    void build_columns(const std::vector<NestedAccessPaths>& configs,
                       std::vector<ColumnDefinition>* columns) {
        std::vector<int> positions {1}; // The unpruned name column accompanies every projection.
        for (const auto& config : configs) {
            const auto it = std::ranges::find_if(
                    schema, [&](const auto& field) { return field.name == config.column_name; });
            ASSERT_NE(it, schema.end());
            positions.push_back(it->local_id);
        }
        params.__set_column_idxs(positions);
        ProjectedColumnBuildContext context {.scan_params = &params, .runtime_state = &state};
        HiveReader annotation_reader;
        for (size_t index = 0; index < positions.size(); ++index) {
            const auto& field = schema[positions[index]];
            TSlotDescriptor thrift_slot;
            thrift_slot.__set_id(index);
            thrift_slot.__set_parent(0);
            thrift_slot.__set_colName(field.name);
            thrift_slot.__set_columnPos(positions[index]);
            thrift_slot.__set_slotType(field.type->to_thrift());
            thrift_slot.__set_nullIndicatorBit(0);
            thrift_slot.__set_isMaterialized(true);
            if (index != 0) {
                thrift_slot.__set_all_access_paths(
                        thrift_access_paths(configs[index - 1].all_paths));
                thrift_slot.__set_predicate_access_paths(
                        thrift_access_paths(configs[index - 1].predicate_paths));
            }
            SlotDescriptor slot(thrift_slot);
            auto column = table_column(field.name, slot.type());
            TFileScanSlotInfo slot_info;
            slot_info.__set_is_file_slot(true);
            context.slot_desc = &slot;
            ASSERT_TRUE(
                    annotation_reader.annotate_projected_column(slot_info, &context, &column).ok());
            ASSERT_TRUE(AccessPathParser::build_nested_children(&column, &slot, nullptr).ok());
            // FE prunes slot types together with access paths. Keep the target type and selected
            // children aligned before TableReader materializes the projected STRUCTs.
            prune_column_type(&column);
            columns->push_back(std::move(column));
        }
        ASSERT_TRUE(annotation_reader.validate_projected_columns(context).ok());
    }

    void read_columns(const std::vector<ColumnDefinition>& columns, Block* result) {
        HiveReader reader;
        ASSERT_TRUE(init_hive_reader(std::get<0>(GetParam()), &params, &state, &profile, &reader,
                                     columns)
                            .ok());
        ASSERT_TRUE(reader.prepare_split(split).ok());
        Block block;
        for (const auto& column : columns) {
            block.insert({column.type->create_column(), column.type, column.name});
        }
        MutableBlock collected(block.clone_empty());
        bool eos = false;
        while (!eos) {
            ASSERT_TRUE(reader.get_block(&block, &eos).ok());
            ASSERT_TRUE(collected.merge(block).ok());
        }
        *result = collected.to_block();
        ASSERT_TRUE(reader.close().ok());
        ASSERT_GT(result->rows(), 0);
    }

    std::vector<ColumnDefinition> build_predicate_columns(
            const std::vector<ColumnDefinition>& columns) {
        std::vector<ColumnDefinition> predicate_columns;
        for (auto column : columns) {
            if (column.has_predicate_access_paths) {
                column.children = column.predicate_children;
                column.has_predicate_access_paths = false;
                column.predicate_children.clear();
                prune_column_type(&column);
                predicate_columns.push_back(std::move(column));
            }
        }
        return predicate_columns;
    }

    void expect_predicate_ids(const std::vector<ColumnDefinition>& columns,
                              const TableColumnMapperOptions& options,
                              const std::set<int>& expected_predicate_ids) {
        // Resolve predicate metadata separately so an all-access-path union cannot hide a lost
        // predicate leaf, even when the physical reader reads a larger union projection.
        const auto predicate_columns = build_predicate_columns(columns);
        TableColumnMapper predicate_mapper(options);
        ASSERT_TRUE(predicate_mapper.create_mapping(predicate_columns, {}, schema).ok());
        FileScanRequest predicate_request;
        ASSERT_TRUE(predicate_mapper.create_scan_request({}, predicate_columns, &predicate_request)
                            .ok());
        EXPECT_EQ(selected_ids(schema, predicate_request.non_predicate_columns),
                  expected_predicate_ids);
    }

    void expect_scan_values(const std::vector<NestedAccessPaths>& configs,
                            const std::vector<ColumnDefinition>& columns,
                            const std::vector<LocalColumnIndex>& projections) {
        Block actual;
        ASSERT_NO_FATAL_FAILURE(read_columns(columns, &actual));
        std::vector<NestedAccessPaths> full_configs;
        for (const auto& config : configs) {
            full_configs.push_back(
                    {.column_name = config.column_name, .all_paths = {}, .predicate_paths = {}});
        }
        std::vector<ColumnDefinition> full_columns;
        ASSERT_NO_FATAL_FAILURE(build_columns(full_configs, &full_columns));
        Block expected;
        ASSERT_NO_FATAL_FAILURE(read_columns(full_columns, &expected));
        ASSERT_EQ(actual.rows(), expected.rows());
        // Selected STRUCTs have fewer child ordinals than the complete scan. Compare by field
        // name, retaining parent null maps and repetition offsets as well as leaf values.
        for (size_t index = 0; index < columns.size(); ++index) {
            const auto& field = schema[params.column_idxs[index]];
            const auto it = std::ranges::find_if(
                    projections, [&](const auto& p) { return p.local_id() == field.local_id; });
            ASSERT_NE(it, projections.end());
            expect_selected_values(field, &*it, columns[index].type,
                                   *actual.get_by_position(index).column,
                                   *expected.get_by_position(index).column);
        }
    }

    void run_case(const std::vector<NestedAccessPaths>& configs, const std::set<int>& expected_ids,
                  const std::set<int>& expected_predicate_ids) {
        std::vector<ColumnDefinition> columns;
        ASSERT_NO_FATAL_FAILURE(build_columns(configs, &columns));
        const auto options = TableColumnMapperOptions {
                .mode = std::get<1>(GetParam()) ? TableColumnMappingMode::BY_NAME
                                                : TableColumnMappingMode::BY_INDEX};
        TableColumnMapper mapper(options);
        ASSERT_TRUE(mapper.create_mapping(columns, {}, schema).ok());
        FileScanRequest request;
        ASSERT_TRUE(mapper.create_scan_request({}, columns, &request).ok());
        EXPECT_EQ(selected_ids(schema, request.non_predicate_columns), expected_ids);

        ASSERT_NO_FATAL_FAILURE(expect_predicate_ids(columns, options, expected_predicate_ids));
        ASSERT_NO_FATAL_FAILURE(
                expect_scan_values(configs, columns, request.non_predicate_columns));
    }

    TFileScanRangeParams params;
    SplitReadOptions split;
    std::vector<ColumnDefinition> schema;
};

TEST_P(HiveV2NestedAccessPathTest, PartialStructAndArrayWildcard) {
    run_case({{.column_name = "profile",
               .all_paths = {{"profile", "address", "coordinates", "lat"},
                             {"profile", "address", "coordinates", "lng"},
                             {"profile", "contact", "email"},
                             {"profile", "hobbies", "*", "level"}},
               .predicate_paths = {{"profile", "address", "coordinates", "lat"},
                                   {"profile", "contact", "email"}}}},
             {2, 3, 4, 7, 8, 9, 10, 11, 15, 16, 18}, {3, 4, 7, 8, 10, 11});
}

TEST_P(HiveV2NestedAccessPathTest, WholeStructWithPartialPredicate) {
    run_case({{.column_name = "profile",
               .all_paths = {{"profile"}},
               .predicate_paths = {{"profile", "address", "coordinates", "lat"},
                                   {"profile", "contact", "email"}}}},
             {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}, {3, 4, 7, 8, 10, 11});
}

TEST_P(HiveV2NestedAccessPathTest, WholeNestedStructsWithPartialPredicate) {
    run_case({{.column_name = "profile",
               .all_paths = {{"profile", "contact"}, {"profile", "address"}},
               .predicate_paths = {{"profile", "address", "coordinates"},
                                   {"profile", "contact", "email"}}}},
             {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, {3, 4, 7, 8, 9, 10, 11});
}

TEST_P(HiveV2NestedAccessPathTest, EmptyPathsReadWholeStruct) {
    run_case({{.column_name = "profile", .all_paths = {}, .predicate_paths = {}}},
             {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}, {});
}

TEST_P(HiveV2NestedAccessPathTest, ArraysOfStructsAndNestedArrays) {
    run_case({{.column_name = "friends",
               .all_paths = {{"friends", "*", "nickname"}, {"friends", "*", "friendship_level"}},
               .predicate_paths = {{"friends", "*", "nickname"}}},
              {.column_name = "recent_activity",
               .all_paths = {{"recent_activity", "*", "action"},
                             {"recent_activity", "*", "details", "*", "value"}},
               .predicate_paths = {{"recent_activity", "*", "action"}}}},
             {2, 21, 22, 24, 25, 26, 27, 28, 29, 30, 32}, {21, 22, 24, 26, 27, 28});
}

TEST_P(HiveV2NestedAccessPathTest, DeepMapsWithKeysValuesAndWildcards) {
    // V2 retains the value leaf (84) for a KEYS-only map so ColumnMap can still be assembled.
    run_case({{.column_name = "complex_attributes",
               .all_paths = {{"complex_attributes", "*", "metadata", "version"},
                             {"complex_attributes", "*", "historical_scores", "*", "components",
                              "*", "sub_scores"},
                             {"complex_attributes", "*", "hierarchical_data", "VALUES"},
                             {"complex_attributes", "VALUES", "validation_rules", "constraints",
                              "*", "parameters", "KEYS"}},
               .predicate_paths = {{"complex_attributes", "*", "metadata", "version"}}}},
             {2,  36, 37, 38, 39, 40, 44, 45, 48, 49, 52, 53, 54, 61, 62, 63, 64, 65,
              66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 79, 80, 82, 83, 84},
             {36, 37, 38, 39, 40});
}

INSTANTIATE_TEST_SUITE_P(
        OrcAndParquet, HiveV2NestedAccessPathTest,
        testing::Combine(testing::Values(FileFormat::ORC, FileFormat::PARQUET), testing::Bool()),
        [](const auto& info) {
            return std::string(std::get<0>(info.param) == FileFormat::ORC ? "Orc" : "Parquet") +
                   (std::get<1>(info.param) ? "ByName" : "ByPosition");
        });

} // namespace
} // namespace doris::format::hive
