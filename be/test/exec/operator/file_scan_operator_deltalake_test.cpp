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
#include "exec/scan/file_scanner_v2.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::pipeline {

TEST(FileScanOperatorDeltaLakeTest, ForcesScannerV2ForQueriesOnly) {
    TQueryOptions options;
    options.__set_enable_file_scanner_v2(false);
    TTableFormatFileDesc format;
    format.__set_table_format_type("deltalake");
    TFileScanRangeParams params;
    params.__set_table_format_params(format);
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    EXPECT_TRUE(FileScanLocalState::TEST_should_use_file_scanner_v2(options, false, params));
    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(options, true, params));

    TFileRangeDesc range;
    range.__set_table_format_params(format);
    range.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    EXPECT_TRUE(FileScannerV2::is_supported(params, range));
    for (const auto physical_format : {TFileFormatType::FORMAT_ORC, TFileFormatType::FORMAT_JNI,
                                       TFileFormatType::FORMAT_CSV_PLAIN}) {
        range.__set_format_type(physical_format);
        EXPECT_FALSE(FileScannerV2::is_supported(params, range));
    }
}

} // namespace doris::pipeline
