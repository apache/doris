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
#include "core/data_type/data_type_struct.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "util/url_coding.h"

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

// A reader whose projection nobody planned.
//
// Every projection so far arrived from FE as slot descriptors: the query asked for these columns,
// the engine turned them into ColumnDefinitions, the reader read them. A union read of a primary-key
// table needs one that did not come from a query at all - to suppress the lake rows a newer log
// record has replaced, BE first has to read the key columns of that log tail, and those keys are
// needed whether or not the query happens to project them.
//
// What this pins is that a projection assembled here, from names and types alone, drives the reader
// exactly like a planned one: the scanner is asked for those columns, in that order, with those
// types. Everything downstream (the JNI block template, and the Java scanner's own projection) is
// derived from this pair of strings, so if they are right the rest follows.
TEST(FlussJniReaderTest, ReadsAProjectionBuiltHereRatherThanPlannedByFe) {
    auto scan_params = make_scan_params({{"fluss.db_name", "db"}});
    // No slot descriptors, no tuple, no query: three key columns named and typed on the spot.
    std::vector<ColumnDefinition> synthetic_keys {
            make_column("id", std::make_shared<DataTypeInt32>(), false),
            make_column("name", std::make_shared<DataTypeString>(), false),
            make_column("amount", std::make_shared<DataTypeInt64>(), false)};

    FlussJniReader reader;
    ASSERT_TRUE(init_reader(&reader, &scan_params, std::move(synthetic_keys)).ok());

    ASSERT_TRUE(reader.build_jni_columns(&reader._jni_columns).ok());
    ASSERT_EQ(reader._jni_columns.size(), 3);
    reader._prepare_jni_scanner_schema();

    // The two strings the Java scanner projects by. A synthetic projection that produced anything
    // else here would read the wrong columns, or fail to open, at the first union read of a
    // primary-key table - and nothing between here and there would notice.
    EXPECT_EQ(reader._scanner_params["required_fields"], "id,name,amount");
    EXPECT_EQ(reader._scanner_params["columns_types"], "int#string#bigint");

    // The block C++ receives them into. Its width and types must match the projection, since
    // finalize_jni_block moves these columns into the output block by position.
    ASSERT_EQ(reader._jni_block_template.columns(), 3);
    EXPECT_EQ(reader._jni_block_template.get_by_position(0).name, "id");
    EXPECT_EQ(reader._jni_block_template.get_by_position(2).name, "amount");
    EXPECT_TRUE(reader._jni_block_template.get_by_position(2).type->equals(DataTypeInt64 {}));
}

// The fluss scanner resolves a pruned STRUCT's fields by name while decoding, and the legacy schema
// grammar lowercases those names. Publishing the encoded pair is what keeps a field's exact spelling
// on the way to Java - this pins that the pair is actually sent, and that a STRUCT field's name
// inside it carries the encoded grammar's '$' version marker rather than the legacy grammar's
// lowercased, unmarked one.
TEST(FlussJniReaderTest, PublishesEncodedSchemaForStructFieldNames) {
    FlussJniReader reader;
    DataTypePtr struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeString>()}, Strings {"City"});
    reader._jni_columns = {JniTableReader::JniColumn {
            .java_name = "address",
            // Keep the aggregate complete because BE UT treats omitted JniColumn fields as errors.
            .output_type = struct_type,
            .transfer_type = struct_type,
    }};

    reader._prepare_jni_scanner_schema();

    ASSERT_TRUE(reader._scanner_params.contains("required_fields_base64"));
    ASSERT_TRUE(reader._scanner_params.contains("columns_types_base64"));

    const std::string& encoded_type = reader._scanner_params.at("columns_types_base64");
    ASSERT_FALSE(encoded_type.empty());
    ASSERT_EQ(encoded_type[0], '$');
    std::string decoded_type;
    ASSERT_TRUE(base64_decode(encoded_type.substr(1), &decoded_type));
    // "City" base64-encodes to "Q2l0eQ=="; the leading '$' marks it as a versioned,
    // spelling-preserving field name rather than the legacy grammar's lowercased "city".
    EXPECT_EQ(decoded_type, "struct<$Q2l0eQ==:string>");
}

} // namespace
} // namespace doris::format::fluss
