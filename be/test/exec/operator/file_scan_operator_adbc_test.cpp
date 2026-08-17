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

#include <string>

#include "exec/operator/file_scan_operator.h"
#include "exec/scan/file_scanner_v2.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::pipeline {
namespace {

TFileScanRangeParams params_for_table_format(const std::string& table_format) {
    TFileScanRangeParams params;
    if (!table_format.empty()) {
        TTableFormatFileDesc fmt;
        fmt.__set_table_format_type(table_format);
        params.__set_table_format_params(fmt);
    }
    return params;
}

} // namespace

// enable_file_scanner_v2 is fuzzy=true, so the regression harness turns it off at random. ADBC has
// no v1 reader, so honoring the flag would make ADBC queries fail on a coin flip.
TEST(FileScanOperatorAdbcTest, AdbcAlwaysUsesScannerV2EvenWhenDisabled) {
    TQueryOptions opts;
    opts.__set_enable_file_scanner_v2(false);
    const auto params = params_for_table_format("adbc");

    EXPECT_TRUE(
            FileScanLocalState::TEST_should_use_file_scanner_v2(opts, /*is_load=*/false, params));
}

// The escape hatch must not drag any other format onto v2 with it.
TEST(FileScanOperatorAdbcTest, NonAdbcStillHonorsTheSessionVariable) {
    TQueryOptions opts;
    opts.__set_enable_file_scanner_v2(false);

    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(opts, /*is_load=*/false,
                                                                     params_for_table_format("")));
    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(
            opts, /*is_load=*/false, params_for_table_format("hive")));
    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(
            opts, /*is_load=*/false, params_for_table_format("remote_doris")));
}

// Loads have no ADBC path at all; forcing them onto v2 would route them somewhere they cannot run.
TEST(FileScanOperatorAdbcTest, AdbcForcingDoesNotApplyToLoads) {
    TQueryOptions opts;
    opts.__set_enable_file_scanner_v2(false);
    const auto params = params_for_table_format("adbc");

    EXPECT_FALSE(
            FileScanLocalState::TEST_should_use_file_scanner_v2(opts, /*is_load=*/true, params));
}

// is_supported gates which ranges FileScannerV2 accepts; without adbc there it would refuse the
// very ranges the operator forces onto it.
TEST(FileScanOperatorAdbcTest, ScannerV2SupportsAdbcArrowRanges) {
    TFileScanRangeParams params;

    TTableFormatFileDesc fmt;
    fmt.__set_table_format_type("adbc");
    TFileRangeDesc range;
    range.__set_format_type(TFileFormatType::FORMAT_ARROW);
    range.__set_table_format_params(fmt);
    EXPECT_TRUE(FileScannerV2::is_supported(params, range));

    // Still only under FORMAT_ARROW: adbc never produces files in another format.
    TFileRangeDesc parquet_range = range;
    parquet_range.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    EXPECT_FALSE(FileScannerV2::is_supported(params, parquet_range));
}

} // namespace doris::pipeline
