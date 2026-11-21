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

#include <gtest/gtest.h>

#include "exec/scan/file_scan_range_utils.h"
#include "exec/scan/file_scanner_v2.h"
#include "format_v2/table/deltalake_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace doris {

TEST(FileScanOperatorDeltaLakeTest, DispatchesNativeReaderRegardlessOfDeprecatedScannerOption) {
    for (int option = 0; option < 3; ++option) {
        TQueryOptions options;
        if (option != 0) {
            options.__set_enable_file_scanner_v2(option == 2);
        }
        RuntimeState state(options, TQueryGlobals());
        RuntimeProfile profile("delta_scan");
        TFileScanRangeParams params;
        params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
        FileScannerV2 scanner(&state, &profile, nullptr);
        scanner._params = &params;
        TTableFormatFileDesc table;
        table.__set_table_format_type("deltalake");
        TFileRangeDesc range;
        range.__set_table_format_params(table);
        std::unique_ptr<format::TableReader> reader;
        ASSERT_TRUE(validate_file_scan_support(params, &range).ok());
        ASSERT_TRUE(scanner._create_table_reader_for_format(range, &reader).ok());
        EXPECT_NE(dynamic_cast<format::deltalake::DeltaReader*>(reader.get()), nullptr);
    }
}

TEST(FileScanOperatorDeltaLakeTest, MissingSnapshotFilesFailEvenWhenDirectoryReadsIgnoreNotFound) {
    TTableFormatFileDesc table;
    table.__set_table_format_type("deltalake");
    TFileRangeDesc range;
    range.__set_table_format_params(table);
    EXPECT_FALSE(can_ignore_not_found_file(range, true));
    EXPECT_FALSE(can_ignore_not_found_file(range, false));
}

} // namespace doris
