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

#include "format_v2/jni/iceberg_sys_table_reader.h"

#include <gtest/gtest.h>

#include <map>
#include <string>

#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::iceberg {
namespace {

TFileRangeDesc make_iceberg_sys_table_range() {
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("iceberg");
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_serialized_split("serialized-metadata-task");
    table_format_params.__set_iceberg_params(iceberg_params);
    range.__set_table_format_params(table_format_params);
    return range;
}

Status init_reader(IcebergSysTableJniReader* reader, TFileScanRangeParams* scan_params) {
    return reader->init({
            .projected_columns = {},
            .conjuncts = {},
            .format = FileFormat::JNI,
            .scan_params = scan_params,
            .io_ctx = nullptr,
            .runtime_state = nullptr,
            .scanner_profile = nullptr,
    });
}

Status build_params(IcebergSysTableJniReader* reader, const TFileRangeDesc& range,
                    std::map<std::string, std::string>* params) {
    reader->_current_range = range;
    return reader->build_scanner_params(params);
}

TEST(IcebergSysTableJniReaderTest, ForwardsExpirySeparatelyFromHadoopProperties) {
    auto range = make_iceberg_sys_table_range();
    range.table_format_params.iceberg_params.__set_file_io_expiry_ms(4102444800000LL);
    TFileScanRangeParams scan_params;
    scan_params.__set_properties({
            {"fs.defaultFS", "hdfs://namenode"},
            {"hadoop.security.authentication", "kerberos"},
            {"fs.azure.account.auth.type.onelake.dfs.fabric.microsoft.com", "OAuth"},
    });
    IcebergSysTableJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());
    ASSERT_TRUE(reader.validate_scan_range(range).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params.at("serialized_split"), "serialized-metadata-task");
    EXPECT_EQ(params.at("file_io_expiry_ms"), "4102444800000");
    EXPECT_FALSE(params.contains("hadoop.file_io_expiry_ms"));
    EXPECT_EQ(params.at("hadoop.fs.defaultFS"), "hdfs://namenode");
    EXPECT_EQ(params.at("hadoop.hadoop.security.authentication"), "kerberos");
    EXPECT_EQ(params.at("hadoop.fs.azure.account.auth.type.onelake.dfs.fabric.microsoft.com"),
              "OAuth");
}

TEST(IcebergSysTableJniReaderTest, OmitsExpiryForLegacySplit) {
    const auto range = make_iceberg_sys_table_range();
    IcebergSysTableJniReader reader;
    ASSERT_TRUE(init_reader(&reader, nullptr).ok());
    ASSERT_TRUE(reader.validate_scan_range(range).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params.at("serialized_split"), "serialized-metadata-task");
    EXPECT_FALSE(params.contains("file_io_expiry_ms"));
    EXPECT_FALSE(params.contains("hadoop.file_io_expiry_ms"));
}

TEST(IcebergSysTableJniReaderTest, SwitchingSplitsDoesNotRetainPreviousExpiry) {
    auto expiring_range = make_iceberg_sys_table_range();
    expiring_range.table_format_params.iceberg_params.__set_file_io_expiry_ms(4102444800000LL);
    auto legacy_range = make_iceberg_sys_table_range();
    legacy_range.table_format_params.iceberg_params.__set_serialized_split("next-metadata-task");
    TFileScanRangeParams scan_params;
    scan_params.__set_properties({{"fs.defaultFS", "hdfs://namenode"}});
    IcebergSysTableJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, expiring_range, &params).ok());
    EXPECT_EQ(params.at("file_io_expiry_ms"), "4102444800000");
    ASSERT_TRUE(build_params(&reader, legacy_range, &params).ok());
    EXPECT_EQ(params.at("serialized_split"), "next-metadata-task");
    EXPECT_FALSE(params.contains("file_io_expiry_ms"));
    EXPECT_FALSE(params.contains("hadoop.file_io_expiry_ms"));
    EXPECT_EQ(params.at("hadoop.fs.defaultFS"), "hdfs://namenode");
}

} // namespace
} // namespace doris::format::iceberg
