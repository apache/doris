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

#include "exec/operator/file_scan_operator.h"

namespace doris {

TEST(FileLoadScannerV2Test, LoadUsesV2WithoutQueryOption) {
    TQueryOptions options;
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    // Stream and routine load plans need not carry the external-query session option.
    EXPECT_TRUE(FileScanLocalState::should_use_file_scanner_v2(options, true, params));
    EXPECT_FALSE(FileScanLocalState::should_use_file_scanner_v2(options, false, params));
}

TEST(FileLoadScannerV2Test, LoadSelectionDoesNotDependOnQueryOption) {
    TQueryOptions options;
    TFileScanRangeParams params;
    for (auto format : {TFileFormatType::FORMAT_CSV_PLAIN, TFileFormatType::FORMAT_JSON,
                        TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_ORC,
                        TFileFormatType::FORMAT_PROTO, TFileFormatType::FORMAT_WAL,
                        TFileFormatType::FORMAT_TEXT, TFileFormatType::FORMAT_ARROW}) {
        params.__set_format_type(format);
        options.__set_enable_file_scanner_v2(true);
        EXPECT_TRUE(FileScanLocalState::should_use_file_scanner_v2(options, true, params));
        options.__set_enable_file_scanner_v2(false);
        EXPECT_TRUE(FileScanLocalState::should_use_file_scanner_v2(options, true, params));
    }
    options.__set_enable_file_scanner_v2(true);
    for (auto format : {TFileFormatType::FORMAT_AVRO}) {
        params.__set_format_type(format);
        EXPECT_FALSE(FileScanLocalState::should_use_file_scanner_v2(options, true, params));
    }
}

// A connector marker must not become a file-load path just because its default format is Parquet.
TEST(FileLoadScannerV2Test, QueryOnlyConnectorsAreNotSupportedLoads) {
    for (const auto* table_format : {"adbc", "fluss"}) {
        TFileScanRangeParams params;
        TTableFormatFileDesc table;
        table.__set_table_format_type(table_format);
        params.__set_table_format_params(table);
        TQueryOptions options;
        EXPECT_FALSE(FileScanLocalState::should_use_file_scanner_v2(options, true, params));
        for (bool enabled : {false, true}) {
            options.__set_enable_file_scanner_v2(enabled);
            for (auto format : {TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_ORC,
                                TFileFormatType::FORMAT_CSV_PLAIN, TFileFormatType::FORMAT_JSON,
                                TFileFormatType::FORMAT_WAL, TFileFormatType::FORMAT_JNI,
                                TFileFormatType::FORMAT_ARROW}) {
                params.__set_format_type(format);
                EXPECT_FALSE(FileScanLocalState::should_use_file_scanner_v2(options, true, params))
                        << table_format << ": " << format << ", V2 option=" << enabled;
            }
        }
    }
}

} // namespace doris
