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

#include "format_v2/table/iceberg_position_delete_sys_table_reader.h"

#include <gtest/gtest.h>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "format/table/iceberg_scan_semantics.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/parquet_reader.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris::format::iceberg {
namespace {

TFileRangeDesc range_with_delete_file(const TIcebergDeleteFileDesc& delete_file) {
    TIcebergFileDesc iceberg_desc;
    iceberg_desc.__set_delete_files({delete_file});
    TTableFormatFileDesc table_format_desc;
    table_format_desc.__set_iceberg_params(std::move(iceberg_desc));
    TFileRangeDesc range;
    range.__set_table_format_params(std::move(table_format_desc));
    return range;
}

TEST(IcebergPositionDeleteSysTableV2ProfileTest, UsesDistinctProfileForNestedPositionReader) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("position_delete_system_table_profile");
    TFileScanRangeParams params;
    std::vector<SlotDescriptor*> file_slot_descs;
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(1);
    delete_file.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    delete_file.__set_path("/not-opened-during-prepare.parquet");

    IcebergPositionDeleteSysTableV2Reader reader;
    reader._runtime_state = &state;
    reader._scanner_profile = &profile;
    reader._scan_params = &params;
    reader._file_slot_descs = &file_slot_descs;
    reader._current_range = range_with_delete_file(delete_file);
    ASSERT_TRUE(reader._init_split().ok());

    ASSERT_NE(reader._position_reader_profile, nullptr);
    EXPECT_NE(reader._position_reader_profile, &profile);
    EXPECT_EQ(profile.get_child("IcebergPositionDeleteFileReader"),
              reader._position_reader_profile);
}

TEST(IcebergPositionDeleteSysTableV2ProfileTest, ReusesChildCountersAcrossSplitsAndScanners) {
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    RuntimeProfile profile("position_delete_system_table_profile");
    parquet::ParquetProfileCache outer_cache(&profile);
    TFileScanRangeParams params;
    std::vector<SlotDescriptor*> file_slot_descs;
    TIcebergDeleteFileDesc delete_file;
    delete_file.__set_content(1);
    delete_file.__set_file_format(TFileFormatType::FORMAT_PARQUET);
    delete_file.__set_path("/not-opened-during-prepare.parquet");
    parquet::ParquetProfileCache* shared_child = nullptr;
    for (int scanner = 0; scanner < 2; ++scanner) {
        IcebergPositionDeleteSysTableV2Reader reader;
        reader._runtime_state = &state;
        reader._scanner_profile = &profile;
        reader._parquet_profile_cache = &outer_cache;
        reader._scan_params = &params;
        reader._file_slot_descs = &file_slot_descs;
        for (int split = 0; split < 2; ++split) {
            reader._current_range = range_with_delete_file(delete_file);
            ASSERT_TRUE(reader._init_split().ok());
            auto* cache = reader._position_reader->_parquet_profile_cache;
            ASSERT_NE(cache, nullptr);
            ASSERT_NE(cache, &outer_cache);
            if (shared_child == nullptr) {
                shared_child = cache;
            }
            EXPECT_EQ(cache, shared_child);
            std::unique_ptr<format::FileReader> file;
            ASSERT_TRUE(reader._position_reader->create_file_reader(&file).ok());
            auto* parquet_reader = dynamic_cast<parquet::ParquetReader*>(file.get());
            ASSERT_NE(parquet_reader, nullptr);
            EXPECT_EQ(parquet_reader->_profile_cache, shared_child);
            parquet_reader->_init_profile();
            EXPECT_EQ(parquet_reader->_parquet_profile.total_time, shared_child->get().total_time);
        }
    }
    EXPECT_EQ(profile.get_counter("FileFooterReadCalls"), nullptr);
    EXPECT_NE(profile.get_child("IcebergPositionDeleteFileReader")
                      ->get_counter("FileFooterReadCalls"),
              nullptr);
}

TEST(IcebergPositionDeleteSysTableV2ProfileTest, PreparesNestedRowInitialDefaults) {
    IcebergPositionDeleteSysTableV2Reader reader;
    TFileScanRangeParams params;
    params.__set_iceberg_scan_semantics_version(ICEBERG_SCAN_SEMANTICS_VERSION_2);
    reader._scan_params = &params;
    const auto child_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto row_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {child_type}, Strings {"added"}));
    ColumnDefinition row;
    row.name = "row";
    row.type = row_type;
    ColumnDefinition child;
    child.name = "added";
    child.type = child_type;
    child.initial_default_value = "7";
    row.children.push_back(std::move(child));
    reader._projected_columns = {row};
    reader._read_columns = {{"row", row_type}};

    std::vector<ColumnDefinition> columns;
    ASSERT_TRUE(reader._build_delete_file_projected_columns(&columns).ok());
    ASSERT_EQ(columns.size(), 1);
    ASSERT_EQ(columns[0].children.size(), 1);
    EXPECT_NE(columns[0].children[0].default_expr, nullptr);
}

} // namespace
} // namespace doris::format::iceberg
