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

} // namespace doris::io
