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

#include "information_schema/schema_processlist_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/define_primitive_type.h"

namespace doris {
namespace {

// A SHOW PROCESSLIST row as an FE of this version sends it: the scanner's columns, in order.
std::vector<std::string> current_row(const std::string& protocol) {
    // clang-format off
    return {"No", "42", "alice", "10.0.0.7:51122", "2026-09-20 10:00:00", "internal", "db1",
            "Query", "3", "OK", "q-1", "trace-1", "select 1", "10.0.0.1", "NULL", protocol};
    // clang-format on
}

} // namespace

TEST(SchemaProcessListScannerTest, protocol_is_the_last_column) {
    auto scanner = SchemaScanner::create(TSchemaTableType::SCH_PROCESSLIST);
    ASSERT_NE(nullptr, scanner);
    EXPECT_EQ(TSchemaTableType::SCH_PROCESSLIST, scanner->type());
    const auto& columns = scanner->get_column_desc();
    ASSERT_EQ(16, columns.size());
    EXPECT_STREQ("TraceId", columns[11].name);
    EXPECT_STREQ("CloudCluster", columns[14].name);
    EXPECT_STREQ("Protocol", columns[15].name);
    EXPECT_EQ(TYPE_VARCHAR, columns[15].type);
    EXPECT_FALSE(columns[15].is_null);
}

TEST(SchemaProcessListScannerTest, fit_keeps_a_row_of_this_version) {
    auto row = current_row("ArrowFlightSQL");
    SchemaProcessListScanner::_fit_row_to_columns(row);
    EXPECT_EQ(current_row("ArrowFlightSQL"), row);
}

// An FE from before the Protocol column ends its row at CloudCluster.
TEST(SchemaProcessListScannerTest, fit_pads_a_row_without_protocol) {
    auto row = current_row("");
    row.pop_back();
    ASSERT_EQ(15, row.size());
    SchemaProcessListScanner::_fit_row_to_columns(row);
    EXPECT_EQ(current_row(""), row);
}

// An FE from before the TraceId column (#51400) sends 14 columns: TraceId is inserted at its
// position and Protocol appended, both empty.
TEST(SchemaProcessListScannerTest, fit_inserts_trace_id_and_pads_a_row_of_an_old_fe) {
    auto expected = current_row("");
    expected[11] = "";
    auto row = expected;
    row.erase(row.begin() + 11);
    row.pop_back();
    ASSERT_EQ(14, row.size());
    SchemaProcessListScanner::_fit_row_to_columns(row);
    EXPECT_EQ(expected, row);
}

// An FE newer than this scanner sends columns it does not know; they are dropped.
TEST(SchemaProcessListScannerTest, fit_cuts_a_row_of_a_newer_fe) {
    auto row = current_row("MySQL");
    row.push_back("a column of a later version");
    SchemaProcessListScanner::_fit_row_to_columns(row);
    EXPECT_EQ(current_row("MySQL"), row);
}

TEST(SchemaProcessListScannerTest, fill_block_reads_protocol_by_position) {
    SchemaProcessListScanner scanner;
    auto flight_row = current_row("ArrowFlightSQL");
    auto mysql_row = current_row("MySQL");
    mysql_row[1] = "43";
    auto old_fe_row = current_row("");
    old_fe_row[1] = "44";
    old_fe_row.pop_back();
    SchemaProcessListScanner::_fit_row_to_columns(old_fe_row);
    scanner._process_list_result.process_list = {flight_row, mysql_row, old_fe_row};

    auto block = Block::create_unique();
    scanner._init_block(block.get());
    ASSERT_TRUE(scanner._fill_block_impl(block.get()).ok());
    ASSERT_EQ(3, block->rows());
    ASSERT_EQ(16, block->columns());

    const auto& id_column = block->get_by_position(1).column;
    EXPECT_EQ(static_cast<__int128_t>(42), (*id_column)[0].get<TYPE_LARGEINT>());
    EXPECT_EQ(static_cast<__int128_t>(43), (*id_column)[1].get<TYPE_LARGEINT>());
    EXPECT_EQ(static_cast<__int128_t>(44), (*id_column)[2].get<TYPE_LARGEINT>());
    const auto& cloud_cluster_column = block->get_by_position(14).column;
    EXPECT_EQ("NULL", (*cloud_cluster_column)[0].get<TYPE_STRING>());
    const auto& protocol_column = block->get_by_position(15).column;
    EXPECT_EQ("ArrowFlightSQL", (*protocol_column)[0].get<TYPE_STRING>());
    EXPECT_EQ("MySQL", (*protocol_column)[1].get<TYPE_STRING>());
    // The row of an FE without the column reads as an empty Protocol, not as an error.
    EXPECT_EQ("", (*protocol_column)[2].get<TYPE_STRING>());
}

// A row that was not fitted (a column count no FE version sends) is still refused, not misread.
TEST(SchemaProcessListScannerTest, fill_block_refuses_a_row_of_the_wrong_width) {
    SchemaProcessListScanner scanner;
    auto row = current_row("MySQL");
    row.pop_back();
    scanner._process_list_result.process_list = {row};

    auto block = Block::create_unique();
    scanner._init_block(block.get());
    auto status = scanner._fill_block_impl(block.get());
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_NE(std::string::npos, status.to_string().find("invalid schema"));
}

} // namespace doris
