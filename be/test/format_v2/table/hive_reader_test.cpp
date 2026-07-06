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

#include <memory>
#include <string>
#include <utility>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "format_v2/column_data.h"
#include "gen_cpp/PlanNodes_types.h"
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

Status init_hive_reader(FileFormat format, TFileScanRangeParams* params, RuntimeState* state,
                        RuntimeProfile* profile, HiveReader* reader) {
    return reader->init({
            .projected_columns = {table_column("id", std::make_shared<DataTypeInt32>()),
                                  table_column("name", std::make_shared<DataTypeString>())},
            .conjuncts = {},
            .format = format,
            .scan_params = params,
            .io_ctx = nullptr,
            .runtime_state = state,
            .scanner_profile = profile,
    });
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

} // namespace
} // namespace doris::format::hive
