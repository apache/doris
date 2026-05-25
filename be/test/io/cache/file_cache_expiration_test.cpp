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

#include "io/cache/file_cache_expiration.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

#include "olap/rowset/rowset_writer_context.h"

namespace doris::io {

TEST(FileCacheExpirationTest, ReturnsZeroForInvalidOrExpiredBase) {
    EXPECT_EQ(0, calc_file_cache_expiration_time(0, 60));
    EXPECT_EQ(0, calc_file_cache_expiration_time(-1, 60));
    EXPECT_EQ(0, calc_file_cache_expiration_time(UnixSeconds() - 10, 5));
    EXPECT_EQ(0, calc_file_cache_expiration_time(std::numeric_limits<int64_t>::max() - 1, 10));
}

TEST(FileCacheExpirationTest, UsesBaseTimestamp) {
    const int64_t base_timestamp = UnixSeconds();
    const int64_t ttl_seconds = 120;

    EXPECT_EQ(base_timestamp + ttl_seconds,
              calc_file_cache_expiration_time(base_timestamp, ttl_seconds));
}

TEST(FileCacheExpirationTest, RowsetWriterContextUsesFileCacheBaseTimestamp) {
    doris::RowsetWriterContext context;
    context.write_file_cache = true;
    context.file_cache_ttl_sec = 60;
    const int64_t base_timestamp = UnixSeconds();
    context.file_cache_base_timestamp = base_timestamp;
    context.newest_write_timestamp = base_timestamp + 600;

    auto opts = context.get_file_writer_options();
    EXPECT_EQ(static_cast<uint64_t>(base_timestamp + 60), opts.file_cache_expiration);
}

} // namespace doris::io
