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

#include "storage/iterator/binlog_block_reader_utils.h"

#include <gtest/gtest.h>

namespace doris {

class BinlogBlockReaderUtilsTest : public testing::Test {};
TEST_F(BinlogBlockReaderUtilsTest, BuildBeforeColumnName) {
    EXPECT_EQ(binlog::build_before_column_name("v1"), "__BEFORE__v1__");
}

} // namespace doris
