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

#include "format_v2/jni/fluss_jni_reader.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::fluss {
namespace {

TFileRangeDesc make_fluss_range(std::map<std::string, std::string> fluss_params) {
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("fluss");
    table_format_params.__set_fluss_params(std::move(fluss_params));
    range.__set_table_format_params(std::move(table_format_params));
    return range;
}

TFileScanRangeParams make_scan_params(std::map<std::string, std::string> fluss_properties) {
    TFileScanRangeParams scan_params;
    scan_params.__set_fluss_properties(std::move(fluss_properties));
    return scan_params;
}

Status init_reader(FlussJniReader* reader, TFileScanRangeParams* scan_params,
                   std::vector<ColumnDefinition> projected_columns = {}) {
    return reader->init({
            .projected_columns = std::move(projected_columns),
            .conjuncts = {},
            .format = FileFormat::JNI,
            .scan_params = scan_params,
            .io_ctx = nullptr,
            .runtime_state = nullptr,
            .scanner_profile = nullptr,
    });
}

Status build_params(FlussJniReader* reader, const TFileRangeDesc& range,
                    std::map<std::string, std::string>* params) {
    reader->_current_range = range;
    return reader->build_scanner_params(params);
}

ColumnDefinition make_column(const std::string& name, DataTypePtr type, bool is_partition_key) {
    ColumnDefinition column;
    column.name = name;
    column.type = std::move(type);
    column.is_partition_key = is_partition_key;
    return column;
}

// Both maps reach the Java scanner as one. Dropping either half is invisible in C++ - the scanner
// would just report a missing key - so the merge itself is what has to be pinned here.
TEST(FlussJniReaderTest, MergesScanLevelAndRangeLevelParams) {
    auto scan_params = make_scan_params({{"fluss.db_name", "db"},
                                         {"fluss.table_name", "log_basic"},
                                         {"fluss.client.bootstrap.servers", "host:9123"}});
    auto range = make_fluss_range({{"fluss.range_type", "LOG"},
                                   {"fluss.bucket_id", "3"},
                                   {"fluss.log_start_offset", "-2"},
                                   {"fluss.log_stop_offset", "17"}});

    FlussJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());
    ASSERT_TRUE(reader.validate_scan_range(range).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params["fluss.db_name"], "db");
    EXPECT_EQ(params["fluss.table_name"], "log_basic");
    EXPECT_EQ(params["fluss.client.bootstrap.servers"], "host:9123");
    EXPECT_EQ(params["fluss.range_type"], "LOG");
    EXPECT_EQ(params["fluss.bucket_id"], "3");
    EXPECT_EQ(params["fluss.log_start_offset"], "-2");
    EXPECT_EQ(params["fluss.log_stop_offset"], "17");
    EXPECT_EQ(params.size(), 7);
}

// The range is the specific half of the pair. If the scan level ever won, every bucket of a scan
// would read the same one - a query that returns wrong rows rather than failing.
TEST(FlussJniReaderTest, RangeLevelParamWinsOverScanLevelParam) {
    auto scan_params = make_scan_params({{"fluss.db_name", "db"}, {"fluss.bucket_id", "0"}});
    auto range = make_fluss_range({{"fluss.range_type", "LOG"}, {"fluss.bucket_id", "7"}});

    FlussJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());

    std::map<std::string, std::string> params;
    ASSERT_TRUE(build_params(&reader, range, &params).ok());
    EXPECT_EQ(params["fluss.bucket_id"], "7");
}

TEST(FlussJniReaderTest, RejectsRangeWithoutFlussParams) {
    auto scan_params = make_scan_params({{"fluss.db_name", "db"}});

    FlussJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params).ok());

    TFileRangeDesc no_table_format;
    EXPECT_FALSE(reader.validate_scan_range(no_table_format).ok());

    TFileRangeDesc no_fluss_params;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("fluss");
    no_fluss_params.__set_table_format_params(std::move(table_format_params));
    EXPECT_FALSE(reader.validate_scan_range(no_fluss_params).ok());

    EXPECT_FALSE(reader.validate_scan_range(make_fluss_range({})).ok());
}

TEST(FlussJniReaderTest, RejectsScanWithoutFlussProperties) {
    auto range = make_fluss_range({{"fluss.range_type", "LOG"}});

    TFileScanRangeParams empty_scan_params;
    FlussJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &empty_scan_params).ok());
    EXPECT_FALSE(reader.validate_scan_range(range).ok());

    auto empty_properties = make_scan_params({});
    FlussJniReader reader_with_empty_properties;
    ASSERT_TRUE(init_reader(&reader_with_empty_properties, &empty_properties).ok());
    EXPECT_FALSE(reader_with_empty_properties.validate_scan_range(range).ok());
}

// A partition column is a constant of the range, not something the scanner reads. Asking for it
// would also disagree with the paimon half of a union read, whose partition columns come from the
// same per-range values - the two halves of one scan node have to lay their blocks out alike.
TEST(FlussJniReaderTest, LeavesPartitionColumnsToTheRangeAndKeepsOutputPositions) {
    auto scan_params = make_scan_params({{"fluss.db_name", "db"}});
    std::vector<ColumnDefinition> projected_columns {
            make_column("id", std::make_shared<DataTypeInt32>(), false),
            make_column("dt", std::make_shared<DataTypeString>(), true),
            make_column("name", std::make_shared<DataTypeString>(), false)};

    FlussJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params, std::move(projected_columns)).ok());
    reader._partition_values.emplace("dt",
                                     Field::create_field<TYPE_STRING>(std::string("20260101")));

    std::vector<format::JniTableReader::JniColumn> columns;
    ASSERT_TRUE(reader.build_jni_columns(&columns).ok());
    ASSERT_EQ(columns.size(), 2);
    EXPECT_EQ(columns[0].java_name, "id");
    EXPECT_EQ(columns[0].output_index, 0);
    EXPECT_EQ(columns[1].java_name, "name");
    // Its position in the output block, not its position among the columns the scanner returns:
    // the two differ by every partition column dropped before it.
    EXPECT_EQ(columns[1].output_index, 2);
}

// A partition column the range has no value for would otherwise leave a hole in the output block.
TEST(FlussJniReaderTest, StillReadsAPartitionColumnTheRangeDidNotCarry) {
    auto scan_params = make_scan_params({{"fluss.db_name", "db"}});
    std::vector<ColumnDefinition> projected_columns {
            make_column("id", std::make_shared<DataTypeInt32>(), false),
            make_column("dt", std::make_shared<DataTypeString>(), true)};

    FlussJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params, std::move(projected_columns)).ok());

    std::vector<format::JniTableReader::JniColumn> columns;
    ASSERT_TRUE(reader.build_jni_columns(&columns).ok());
    ASSERT_EQ(columns.size(), 2);
    EXPECT_EQ(columns[1].java_name, "dt");
    EXPECT_EQ(columns[1].output_index, 1);
}

} // namespace
} // namespace doris::format::fluss
