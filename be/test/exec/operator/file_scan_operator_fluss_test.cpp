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

TFileScanRangeParams scan_level_format(const std::string& table_format) {
    TFileScanRangeParams params;
    if (!table_format.empty()) {
        TTableFormatFileDesc fmt;
        fmt.__set_table_format_type(table_format);
        params.__set_table_format_params(fmt);
    }
    return params;
}

TFileRangeDesc jni_range(const std::string& table_format) {
    TTableFormatFileDesc fmt;
    fmt.__set_table_format_type(table_format);
    TFileRangeDesc range;
    range.__set_format_type(TFileFormatType::FORMAT_JNI);
    range.__set_table_format_params(fmt);
    return range;
}

} // namespace

// Every fluss reader lives in FileScannerV2 alone, and enable_file_scanner_v2 is a session variable a
// user may legitimately turn off (it is also fuzzy=true, so the regression harness turns it off at
// random). Honoring it would fail the query with "Not supported create reader for table format" from
// the legacy scanner's JNI dispatch, which has no fluss branch.
TEST(FileScanOperatorFlussTest, FlussAlwaysUsesScannerV2EvenWhenDisabled) {
    TQueryOptions opts;
    opts.__set_enable_file_scanner_v2(false);

    EXPECT_TRUE(FileScanLocalState::TEST_should_use_file_scanner_v2(opts, /*is_load=*/false,
                                                                    scan_level_format("fluss")));
}

// The node is stamped "fluss" whichever ranges the plan produced, so this must not depend on the
// session variable being set at all.
TEST(FileScanOperatorFlussTest, FlussForcingHoldsWhenTheVariableIsUnset) {
    TQueryOptions opts;

    EXPECT_TRUE(FileScanLocalState::TEST_should_use_file_scanner_v2(opts, /*is_load=*/false,
                                                                    scan_level_format("fluss")));
}

// There is no fluss load path, so widening the rule to loads would only route them somewhere they
// still cannot run -- the same reason adbc leaves loads alone.
TEST(FileScanOperatorFlussTest, FlussForcingDoesNotApplyToLoads) {
    TQueryOptions opts;
    opts.__set_enable_file_scanner_v2(false);

    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(opts, /*is_load=*/true,
                                                                     scan_level_format("fluss")));
}

// The escape hatch is keyed on the exact scan-level marker FE writes. A node planned by another
// connector - even the paimon sibling whose splits a fluss scan borrows - is not admitted by it.
TEST(FileScanOperatorFlussTest, OnlyTheScanLevelFlussMarkerForcesV2) {
    TQueryOptions opts;
    opts.__set_enable_file_scanner_v2(false);

    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(opts, /*is_load=*/false,
                                                                     scan_level_format("paimon")));
    EXPECT_FALSE(FileScanLocalState::TEST_should_use_file_scanner_v2(opts, /*is_load=*/false,
                                                                     scan_level_format("")));
}

// Forcing the node onto V2 is only half an answer: V2 has to accept every range kind that node can
// carry, or the failure just moves from the legacy scanner to _validate_scan_range.
TEST(FileScanOperatorFlussTest, ScannerV2SupportsEveryRangeKindOfAForcedNode) {
    const auto params = scan_level_format("fluss");

    TFileRangeDesc log_range = jni_range("fluss");
    log_range.table_format_params.__set_fluss_params({{"fluss.range_type", "LOG"}});
    EXPECT_TRUE(FileScannerV2::is_supported(params, log_range));

    // A lake split arrives in whichever form the paimon sibling planned it: a serialized JNI
    // split, or a native Parquet/ORC range.
    TFileRangeDesc lake_jni = jni_range("fluss");
    lake_jni.table_format_params.__set_fluss_params(
            {{"fluss.range_type", "LAKE_SUPPRESS"}, {"fluss.union.tail", ":0:0:9"}});
    TPaimonFileDesc paimon_params;
    paimon_params.__set_paimon_split("encoded");
    lake_jni.table_format_params.__set_paimon_params(paimon_params);
    EXPECT_TRUE(FileScannerV2::is_supported(params, lake_jni));

    TFileRangeDesc lake_native = jni_range("fluss");
    lake_native.table_format_params.__set_fluss_params({{"fluss.range_type", "LAKE"}});
    lake_native.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    EXPECT_TRUE(FileScannerV2::is_supported(params, lake_native));
}

} // namespace doris::pipeline
