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

#include "format_v2/wal/wal_reader.h"

#include <gtest/gtest.h>

namespace doris::format::wal {

TEST(WalReaderV2Test, ParseColumnIdsPreservesHeaderOrder) {
    std::vector<int32_t> column_ids;
    ASSERT_TRUE(parse_wal_column_ids("17,4,99", &column_ids).ok());
    EXPECT_EQ(column_ids, (std::vector<int32_t> {17, 4, 99}));
}

TEST(WalReaderV2Test, ParseColumnIdsRejectsMalformedOrAmbiguousHeaders) {
    std::vector<int32_t> column_ids;
    EXPECT_FALSE(parse_wal_column_ids("", &column_ids).ok());
    EXPECT_FALSE(parse_wal_column_ids("17,,99", &column_ids).ok());
    EXPECT_FALSE(parse_wal_column_ids("17,nope,99", &column_ids).ok());
    EXPECT_FALSE(parse_wal_column_ids("17,4,17", &column_ids).ok());
}

} // namespace doris::format::wal
