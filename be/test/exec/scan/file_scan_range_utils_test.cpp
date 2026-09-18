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

#include "exec/scan/file_scan_range_utils.h"

#include <gtest/gtest.h>

namespace doris {

TEST(FileScanRangeUtilsTest, IcebergFilesCannotBeIgnored) {
    TFileRangeDesc range;
    range.table_format_params.__set_table_format_type("iceberg");
    range.__isset.table_format_params = true;
    for (auto format : {TFileFormatType::FORMAT_PARQUET, TFileFormatType::FORMAT_ORC,
                        TFileFormatType::FORMAT_JNI}) {
        range.__set_format_type(format);
        EXPECT_FALSE(can_ignore_not_found_file(range, true));
        EXPECT_FALSE(can_ignore_not_found_file(range, false));
    }
}

TEST(FileScanRangeUtilsTest, OtherFilesRespectIgnoreSetting) {
    TFileRangeDesc range;
    EXPECT_TRUE(can_ignore_not_found_file(range, true));
    EXPECT_FALSE(can_ignore_not_found_file(range, false));
    for (const auto* format : {"hive", "hudi", "paimon"}) {
        range.table_format_params.__set_table_format_type(format);
        range.__isset.table_format_params = true;
        EXPECT_TRUE(can_ignore_not_found_file(range, true));
        EXPECT_FALSE(can_ignore_not_found_file(range, false));
    }
}

} // namespace doris
