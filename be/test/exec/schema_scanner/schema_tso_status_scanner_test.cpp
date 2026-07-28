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

#include <gen_cpp/Descriptors_types.h>
#include <gtest/gtest.h>

#include "information_schema/schema_scanner.h"

namespace doris {

TEST(SchemaTsoStatusScannerTest, test_create_tso_status_scanner) {
    auto scanner = SchemaScanner::create(TSchemaTableType::SCH_TSO_STATUS);
    ASSERT_NE(nullptr, scanner);
    EXPECT_EQ(TSchemaTableType::SCH_TSO_STATUS, scanner->type());
    ASSERT_EQ(4, scanner->get_column_desc().size());
    EXPECT_STREQ("WINDOW_END_PHYSICAL_TIME", scanner->get_column_desc()[0].name);
    EXPECT_STREQ("CURRENT_TSO", scanner->get_column_desc()[1].name);
    EXPECT_STREQ("CURRENT_TSO_PHYSICAL_TIME", scanner->get_column_desc()[2].name);
    EXPECT_STREQ("CURRENT_TSO_LOGICAL_COUNTER", scanner->get_column_desc()[3].name);
}

} // namespace doris
