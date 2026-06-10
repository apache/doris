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

#include "format_v2/table_reader.h"

namespace doris::format {
namespace {

class TableReaderRequestTestHelper final : public TableReader {
public:
    using TableReader::_append_file_scan_column;
};

TEST(TableReaderRequestTest, AppendPredicateColumnKeepsOtherNonPredicateColumns) {
    TableReaderRequestTestHelper reader;
    FileScanRequest request;

    reader._append_file_scan_column(&request, LocalColumnId(1), &request.non_predicate_columns);
    reader._append_file_scan_column(&request, LocalColumnId(2), &request.non_predicate_columns);
    reader._append_file_scan_column(&request, LocalColumnId(1), &request.predicate_columns);

    ASSERT_EQ(request.local_positions.size(), 2);
    EXPECT_EQ(request.local_positions.at(LocalColumnId(1)).value(), 0);
    EXPECT_EQ(request.local_positions.at(LocalColumnId(2)).value(), 1);

    ASSERT_EQ(request.predicate_columns.size(), 1);
    EXPECT_EQ(request.predicate_columns[0].column_id(), LocalColumnId(1));

    ASSERT_EQ(request.non_predicate_columns.size(), 1);
    EXPECT_EQ(request.non_predicate_columns[0].column_id(), LocalColumnId(2));
}

} // namespace
} // namespace doris::format
