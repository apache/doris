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

#include "exec/sink/writer/iceberg/iceberg_partition_path.h"

#include <gtest/gtest.h>

namespace doris {

class IcebergPartitionPathTest : public testing::Test {};

TEST_F(IcebergPartitionPathTest, test_escape_matches_iceberg_partition_spec_path_encoding) {
    EXPECT_EQ("", IcebergPartitionPath::escape(""));
    EXPECT_EQ("abcXYZ012.-*_", IcebergPartitionPath::escape("abcXYZ012.-*_"));
    EXPECT_EQ("with+space", IcebergPartitionPath::escape("with space"));
    EXPECT_EQ("slash%2Fcolon%3Aequals%3Dpercent%25question%3F",
              IcebergPartitionPath::escape("slash/colon:equals=percent%question?"));
    EXPECT_EQ("quote%22hash%23brackets%5B%5Dcaret%5E",
              IcebergPartitionPath::escape("quote\"hash#brackets[]caret^"));
    EXPECT_EQ("tilde%7Ebang%21plus%2B", IcebergPartitionPath::escape("tilde~bang!plus+"));
    EXPECT_EQ(
            "with%CC%81combining+character",
            IcebergPartitionPath::escape(std::string("with") + "\xCC\x81" + "combining character"));
}

} // namespace doris
