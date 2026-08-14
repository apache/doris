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

#include "io/fs/file_handle_cache.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "format/table/iceberg_delete_file_reader_helper.h"

namespace doris::io {

TEST(FileHandleCacheTest, CacheKeyIncludesHdfsFs) {
    auto first_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    auto second_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x2));
    const std::string fname = "/user/hive/warehouse/table/data.parquet";
    constexpr int64_t mtime = 12345;

    EXPECT_TRUE(FileHandleCache::same_cache_key_for_test(first_fs, fname, mtime, first_fs, fname,
                                                         mtime));
    EXPECT_FALSE(FileHandleCache::same_cache_key_for_test(first_fs, fname, mtime, second_fs, fname,
                                                          mtime));
    EXPECT_FALSE(FileHandleCache::same_cache_key_for_test(first_fs, fname, mtime, first_fs,
                                                          fname + ".other", mtime));
    EXPECT_FALSE(FileHandleCache::same_cache_key_for_test(first_fs, fname, mtime, first_fs, fname,
                                                          mtime + 1));
}

// Verify that init() with file_size > 0 does NOT open the file (lazy open).
// The handle should know file_size but file() should be nullptr.
TEST(FileHandleCacheTest, InitWithKnownFileSizeDoesNotOpenFile) {
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    ExclusiveHdfsFileHandle handle(mock_fs, "/nonexistent/file.parquet", 12345);
    auto st = handle.init(4096);
    ASSERT_TRUE(st.ok()) << st;
    // file_size should be set from parameter
    EXPECT_EQ(handle.file_size(), 4096);
    // file() should be nullptr — lazy open means no hdfsOpenFile was called
    EXPECT_EQ(handle.file(), nullptr);
}

// Verify that build_iceberg_delete_file_range treats file_size <= 0 as unknown (-1).
// This covers the case where thrift optional file_size defaults to 0.
TEST(FileHandleCacheTest, BuildDeleteFileRangeTreatsZeroAsUnknown) {
    auto range_known = build_iceberg_delete_file_range("s3://b/f.parquet", 1024);
    EXPECT_EQ(range_known.file_size, 1024);

    auto range_zero = build_iceberg_delete_file_range("s3://b/f.parquet", 0);
    EXPECT_EQ(range_zero.file_size, -1);

    auto range_neg = build_iceberg_delete_file_range("s3://b/f.parquet", -1);
    EXPECT_EQ(range_neg.file_size, -1);
}

} // namespace doris::io
