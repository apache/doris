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

#include "format/table/iceberg_sys_table_jni_reader.h"

#include <gtest/gtest.h>

#include <utility>
#include <vector>

#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace {

TFileRangeDesc make_legacy_iceberg_sys_table_range() {
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("iceberg");
    TIcebergFileDesc iceberg_params;
    iceberg_params.__set_serialized_split("serialized-metadata-task");
    table_format_params.__set_iceberg_params(std::move(iceberg_params));
    range.__set_table_format_params(std::move(table_format_params));
    return range;
}

TEST(LegacyIcebergSysTableJniReaderTest, ForwardsExpirySeparatelyFromHadoopProperties) {
    auto range = make_legacy_iceberg_sys_table_range();
    range.table_format_params.iceberg_params.__set_file_io_expiry_ms(4102444800000LL);
    TFileScanRangeParams scan_params;
    scan_params.__set_properties({
            {"fs.defaultFS", "hdfs://namenode"},
            {"hadoop.security.authentication", "kerberos"},
            {"fs.azure.account.auth.type.onelake.dfs.fabric.microsoft.com", "OAuth"},
    });
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    const std::vector<SlotDescriptor*> file_slot_descs;

    IcebergSysTableJniReader reader(file_slot_descs, &state, nullptr, range, &scan_params);
    const auto& params = reader._scanner_params;
    EXPECT_EQ(params.at("serialized_split"), "serialized-metadata-task");
    EXPECT_EQ(params.at("file_io_expiry_ms"), "4102444800000");
    EXPECT_FALSE(params.contains("hadoop.file_io_expiry_ms"));
    EXPECT_EQ(params.at("hadoop.fs.defaultFS"), "hdfs://namenode");
    EXPECT_EQ(params.at("hadoop.hadoop.security.authentication"), "kerberos");
    EXPECT_EQ(params.at("hadoop.fs.azure.account.auth.type.onelake.dfs.fabric.microsoft.com"),
              "OAuth");
}

TEST(LegacyIcebergSysTableJniReaderTest, OmitsExpiryForLegacySplit) {
    const auto range = make_legacy_iceberg_sys_table_range();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    const std::vector<SlotDescriptor*> file_slot_descs;

    IcebergSysTableJniReader reader(file_slot_descs, &state, nullptr, range, nullptr);
    EXPECT_EQ(reader._scanner_params.at("serialized_split"), "serialized-metadata-task");
    EXPECT_FALSE(reader._scanner_params.contains("file_io_expiry_ms"));
    EXPECT_FALSE(reader._scanner_params.contains("hadoop.file_io_expiry_ms"));
}

} // namespace
} // namespace doris
