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

#include "io/cache/cache_lru_dumper.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"

using ::testing::_;
using ::testing::Return;
using ::testing::NiceMock;

namespace doris::io {
std::mutex _mutex;

class MockBlockFileCache : public BlockFileCache {
public:
    LRUQueue* dst_queue; // Pointer to the destination queue

    MockBlockFileCache(LRUQueue* queue) : BlockFileCache("", {}), dst_queue(queue) {
        _cache_base_path = "./";
    }

    FileBlockCell* add_cell(const UInt128Wrapper& hash, const CacheContext& ctx, size_t offset,
                            size_t size, FileBlock::State state,
                            std::lock_guard<std::mutex>& lock) {
        dst_queue->add(hash, offset, size, lock);
        return nullptr;
    }

    std::mutex& mutex() { return _mutex; }

private:
    std::mutex _mutex;
    struct {
        std::string _cache_base_path;
    } _mgr;
};

class CacheLRUDumperTest : public ::testing::Test {
protected:
    LRUQueue dst_queue; // Member variable for destination queue

    void SetUp() override {
        mock_cache = std::make_unique<NiceMock<MockBlockFileCache>>(&dst_queue);
        recorder = std::make_unique<LRUQueueRecorder>(mock_cache.get());

        dumper = std::make_unique<CacheLRUDumper>(mock_cache.get(), recorder.get());
    }

    void TearDown() override {
        dumper.reset();
        mock_cache.reset();
    }

    std::unique_ptr<NiceMock<MockBlockFileCache>> mock_cache;
    std::unique_ptr<CacheLRUDumper> dumper;
    std::unique_ptr<LRUQueueRecorder> recorder;
};

TEST_F(CacheLRUDumperTest, test_finalize_dump_and_parse_dump_footer) {
    std::ofstream out("test_finalize.bin", std::ios::binary);
    std::string tmp_filename = "test_finalize.bin.tmp";
    std::string final_filename = "test_finalize.bin";
    size_t file_size = 0;
    size_t entry_num = 10;

    // Test finalize dump
    EXPECT_TRUE(
            dumper->finalize_dump(out, entry_num, tmp_filename, final_filename, file_size).ok());

    // Test parse footer
    std::ifstream in("test_finalize.bin", std::ios::binary);
    size_t parsed_entry_num = 0;
    EXPECT_TRUE(dumper->parse_dump_footer(in, final_filename, parsed_entry_num).ok());
    EXPECT_EQ(entry_num, parsed_entry_num);

    out.close();
    in.close();
    std::remove("test_finalize.bin");
}

TEST_F(CacheLRUDumperTest, test_remove_lru_dump_files) {
    // Create test files
    std::vector<std::string> queue_names = {"disposable", "index", "normal", "ttl"};
    for (const auto& name : queue_names) {
        std::ofstream(fmt::format("lru_dump_{}.tail", name));
    }

    // Test remove
    dumper->remove_lru_dump_files();

    // Verify files are removed
    for (const auto& name : queue_names) {
        EXPECT_FALSE(std::filesystem::exists(fmt::format("lru_dump_{}.tail", name)));
    }
}

TEST_F(CacheLRUDumperTest, test_dump_and_restore_queue) {
    LRUQueue src_queue;
    std::string queue_name = "normal";

    // Add test data
    UInt128Wrapper hash(123456789ULL);
    size_t offset = 1024;
    size_t size = 4096;
    std::lock_guard<std::mutex> lock(_mutex);
    src_queue.add(hash, offset, size, lock);

    // Test dump
    dumper->do_dump_queue(src_queue, queue_name);

    // Test restore
    std::lock_guard<std::mutex> cache_lock(mock_cache->mutex());
    dumper->restore_queue(dst_queue, queue_name, cache_lock);

    // Verify queue content and order
    auto src_it = src_queue.begin();
    auto dst_it = dst_queue.begin();
    while (src_it != src_queue.end() && dst_it != dst_queue.end()) {
        EXPECT_EQ(src_it->hash, dst_it->hash);
        EXPECT_EQ(src_it->offset, dst_it->offset);
        EXPECT_EQ(src_it->size, dst_it->size);
        ++src_it;
        ++dst_it;
    }

    // Clean up
    std::remove(fmt::format("lru_dump_{}.tail", queue_name).c_str());
}

} // namespace doris::io