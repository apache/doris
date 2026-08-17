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

} // namespace
} // namespace doris::format::iceberg
