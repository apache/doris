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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/tests/gtest_lru_file_cache.cpp
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stddef.h>

#include "runtime/thread_context.h"
#if defined(__APPLE__)
#include <sys/mount.h>
#else
#include <sys/statfs.h>
#endif

// IWYU pragma: no_include <bits/chrono.h>
#include <gtest/gtest.h>

#include <chrono> // IWYU pragma: keep
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <ranges>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/fs/path.h"
#include "olap/options.h"
#include "runtime/exec_env.h"
#include "util/slice.h"
#include "util/time.h"

namespace doris::io {
namespace fs = std::filesystem;

extern int disk_used_percentage(const std::string& path, std::pair<int, int>* percent);
extern fs::path caches_dir;
extern std::string cache_base_path;
extern std::string tmp_file;

constexpr unsigned long long operator""_mb(unsigned long long m) {
    return m * 1024 * 1024;
}

constexpr unsigned long long operator""_kb(unsigned long long m) {
    return m * 1024;
}

extern void assert_range([[maybe_unused]] size_t assert_n, io::FileBlockSPtr file_block,
                         const io::FileBlock::Range& expected_range,
                         io::FileBlock::State expected_state);
extern std::vector<io::FileBlockSPtr> fromHolder(const io::FileBlocksHolder& holder);
extern void download(io::FileBlockSPtr file_block, size_t size = 0);
extern void download_into_memory(io::FileBlockSPtr file_block, size_t size = 0);
extern void complete(const io::FileBlocksHolder& holder);
extern void complete_into_memory(const io::FileBlocksHolder& holder);
extern void test_file_cache(io::FileCacheType cache_type);
extern void test_file_cache_memory_storage(io::FileCacheType cache_type);

class BlockFileCacheTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        config::file_cache_enter_disk_resource_limit_mode_percent = 99;
        config::enable_evict_file_cache_in_advance = false; // disable evict in
                                                            // advance for most
                                                            // cases for simple
                                                            // verification
        bool exists {false};
        ASSERT_TRUE(global_local_filesystem()->exists(caches_dir, &exists).ok());
        if (!exists) {
            ASSERT_TRUE(global_local_filesystem()->create_directory(caches_dir).ok());
        }
        ASSERT_TRUE(global_local_filesystem()->exists(tmp_file, &exists).ok());
        if (!exists) {
            FileWriterPtr writer;
            ASSERT_TRUE(global_local_filesystem()->create_file(tmp_file, &writer).ok());
            for (int i = 0; i < 10; i++) {
                std::string data(1_mb, '0' + i);
                ASSERT_TRUE(writer->append(Slice(data.data(), data.size())).ok());
            }
            std::string data(1, '0');
            ASSERT_TRUE(writer->append(Slice(data.data(), data.size())).ok());
            ASSERT_TRUE(writer->close().ok());
        }
        ExecEnv::GetInstance()->_file_cache_factory = factory.get();
        ExecEnv::GetInstance()->_file_cache_open_fd_cache = std::make_unique<io::FDCache>();
    }
    static void TearDownTestSuite() {
        config::file_cache_enter_disk_resource_limit_mode_percent = 99;
        ExecEnv::GetInstance()->_file_cache_open_fd_cache.reset(nullptr);
    }

private:
    inline static std::unique_ptr<FileCacheFactory> factory = std::make_unique<FileCacheFactory>();
};

} // end of namespace doris::io