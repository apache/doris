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

#include "io/fs/file_meta_cache.h"

#include <thread>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "io/fs/file_reader.h"

namespace doris {
namespace {

class MockFileReader : public io::FileReader {
public:
    MockFileReader(const std::string& file_name, size_t size)
            : _file_name(file_name), _size(size), _closed(false) {}
    ~MockFileReader() override = default;

    const io::Path& path() const override {
        static io::Path p(_file_name);
        return p;
    }

    size_t size() const override { return _size; }

    bool closed() const override { return _closed; }

    int64_t mtime() const override { return 0; }

    Status close() override {
        _closed = true;
        return Status::OK();
    }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const io::IOContext* io_ctx) override {
        *bytes_read = 0;
        return Status::OK();
    }

private:
    std::string _file_name;
    size_t _size;
    bool _closed;
};
} // anonymous namespace

TEST(FileMetaCacheTest, KeyGenerationFromParams) {
    std::string file_name = "/path/to/file";
    int64_t mtime = 123456789;
    int64_t file_size = 987654321;

    std::string key1 = FileMetaCache::get_key(file_name, mtime, file_size);
    std::string key2 = FileMetaCache::get_key(file_name, mtime, file_size);
    EXPECT_EQ(key1, key2) << "Same parameters should produce same key";

    // Different mtime should produce different key
    std::string key3 = FileMetaCache::get_key(file_name, mtime + 1, file_size);
    EXPECT_NE(key1, key3);

    // mtime == 0, use file_size
    std::string key4 = FileMetaCache::get_key(file_name, 0, file_size);
    std::string key5 = FileMetaCache::get_key(file_name, 0, file_size);
    EXPECT_EQ(key4, key5);
    EXPECT_NE(key1, key4);

    // mtime == 0, different file_size
    std::string key6 = FileMetaCache::get_key(file_name, 0, file_size + 1);
    EXPECT_NE(key4, key6);
}

TEST(FileMetaCacheTest, KeyGenerationFromFileReader) {
    std::string file_name = "/path/to/file";
    int64_t mtime = 123456789;
    int64_t file_size = 100;

    // file_description.file_size != -1, use it as file size
    io::FileDescription desc1;
    desc1.mtime = mtime;
    desc1.file_size = file_size;
    auto reader1 = std::make_shared<MockFileReader>(file_name, 200);

    std::string key1 = FileMetaCache::get_key(reader1, desc1);
    std::string expected_key1 = FileMetaCache::get_key(file_name, mtime, file_size);
    EXPECT_EQ(key1, expected_key1);

    // file_description.file_size == -1, use reader->size()
    io::FileDescription desc2;
    desc2.mtime = 0;
    desc2.file_size = -1;
    auto reader2 = std::make_shared<MockFileReader>(file_name, 300);

    std::string key2 = FileMetaCache::get_key(reader2, desc2);
    std::string expected_key2 = FileMetaCache::get_key(file_name, 0, 300);
    EXPECT_EQ(key2, expected_key2);
}
TEST(FileMetaCacheTest, KeyContentVerification) {
    std::string file_name = "/path/to/file";
    int64_t mtime = 0x0102030405060708;
    int64_t file_size = 0x1112131415161718;

    std::string key_with_mtime = FileMetaCache::get_key(file_name, mtime, file_size);

    ASSERT_EQ(key_with_mtime.size(), file_name.size() + sizeof(int64_t));

    EXPECT_EQ(memcmp(key_with_mtime.data(), file_name.data(), file_name.size()), 0);

    int64_t extracted_mtime = 0;
    memcpy(&extracted_mtime, key_with_mtime.data() + file_name.size(), sizeof(int64_t));
    EXPECT_EQ(extracted_mtime, mtime);

    std::string key_with_filesize = FileMetaCache::get_key(file_name, 0, file_size);
    ASSERT_EQ(key_with_filesize.size(), file_name.size() + sizeof(int64_t));
    EXPECT_EQ(memcmp(key_with_filesize.data(), file_name.data(), file_name.size()), 0);
    int64_t extracted_filesize = 0;
    memcpy(&extracted_filesize, key_with_filesize.data() + file_name.size(), sizeof(int64_t));
    EXPECT_EQ(extracted_filesize, file_size);
}

TEST(FileMetaCacheTest, InsertAndLookupWithIntValue) {
    FileMetaCache cache(1024 * 1024, LRUCacheType::SIZE);

    auto value = std::make_unique<int>(12345);
    ObjLRUCache::CacheHandle handle;

    ASSERT_TRUE(cache.insert("key_int", value, &handle, sizeof(int)));
    EXPECT_EQ(value, nullptr);
    ASSERT_NE(handle._cache, nullptr);

    const int* cached_val = handle.data<int>();
    ASSERT_NE(cached_val, nullptr);
    EXPECT_EQ(*cached_val, 12345);

    ObjLRUCache::CacheHandle handle2;
    cache.lookup("key_int", &handle2);

    ASSERT_NE(handle2._cache, nullptr);

    const int* cached_val2 = handle2.data<int>();
    ASSERT_NE(cached_val2, nullptr);
    EXPECT_EQ(*cached_val2, 12345);
}

TEST(FileMetaCacheTest, MemoryBudgetConfigurationAndLegacyAdmission) {
    std::unique_ptr<FileMetaCache> cache;
    ASSERT_TRUE(FileMetaCache::create("1%", 1000, 4LL << 30, 64LL << 30, &cache).ok());
    EXPECT_EQ(cache->cache().get_capacity(), (4LL << 30) / 100);
    EXPECT_TRUE(cache->should_cache_file_count(15000));
    cache.reset();
    ASSERT_TRUE(FileMetaCache::create("2M", 1000, 4LL << 30, 64LL << 30, &cache).ok());
    EXPECT_EQ(cache->cache().get_capacity(), 2 << 20);
    cache.reset();
    ASSERT_TRUE(FileMetaCache::create("", 300, 4LL << 30, 64LL << 30, &cache).ok());
    EXPECT_EQ(cache->cache().get_capacity(), 300);
    EXPECT_TRUE(cache->should_cache_file_count(99));
    EXPECT_FALSE(cache->should_cache_file_count(100));
    for (const auto& limit : {"0", "1"}) {
        cache.reset();
        ASSERT_TRUE(FileMetaCache::create(limit, 1000, 4LL << 30, 64LL << 30, &cache).ok());
        EXPECT_FALSE(cache->enabled());
        EXPECT_FALSE(cache->should_cache_file_count(0));
    }
    cache.reset();
    ASSERT_TRUE(FileMetaCache::create("1%", 0, 4LL << 30, 64LL << 30, &cache).ok());
    EXPECT_FALSE(cache->enabled());
    cache.reset();
    for (const auto& limit : {"invalid", "-2", "101%", "5G"}) {
        EXPECT_FALSE(FileMetaCache::create(limit, 1000, 4LL << 30, 64LL << 30, &cache).ok());
        EXPECT_EQ(cache, nullptr);
    }
}

TEST(FileMetaCacheTest, ByteChargeEvictionAndPinnedLifetime) {
    FileMetaCache cache(4096, LRUCacheType::SIZE, 1);
    auto first = std::make_unique<std::string>(1500, 'a');
    ObjLRUCache::CacheHandle pinned;
    ASSERT_TRUE(cache.insert("a", first, &pinned, 1533));
    EXPECT_EQ(cache.cache().get_usage(),
              sizeof(LRUHandle) + sizeof(ObjLRUCache::ObjValue<std::string>) + 1533);
    auto second = std::make_unique<std::string>(1500, 'b');
    ObjLRUCache::CacheHandle released;
    ASSERT_TRUE(cache.insert("b", second, &released, 1533));
    released = {};
    auto third = std::make_unique<std::string>(1500, 'c');
    ASSERT_TRUE(cache.insert("c", third, &released, 1533));
    EXPECT_EQ(pinned.data<std::string>()->front(), 'a');
    ObjLRUCache::CacheHandle lookup;
    EXPECT_FALSE(cache.lookup("b", &lookup));
    EXPECT_TRUE(cache.lookup("a", &lookup));
    lookup = {};
    released = {};
    pinned = {};
    EXPECT_LE(cache.cache().get_usage(), cache.cache().get_capacity());
    cache.cache().prune_all(true);
    EXPECT_EQ(cache.cache().get_usage(), 0);
    EXPECT_EQ(cache.cache().mem_consumption(), 0);
}

TEST(FileMetaCacheTest, OversizedAndDisabledEntriesKeepReaderOwnership) {
    FileMetaCache cache(4096, LRUCacheType::SIZE, 1);
    auto value = std::make_unique<std::string>(8192, 'x');
    auto* original = value.get();
    ObjLRUCache::CacheHandle handle;
    EXPECT_FALSE(cache.insert("large", value, &handle, value->capacity() + sizeof(*value)));
    EXPECT_EQ(value.get(), original);
    EXPECT_FALSE(handle.valid());
    EXPECT_EQ(cache.cache().get_usage(), 0);
    FileMetaCache disabled(0, LRUCacheType::SIZE, 1);
    EXPECT_FALSE(disabled.insert("large", value, &handle, value->capacity() + sizeof(*value)));
    EXPECT_EQ(value.get(), original);
}

TEST(FileMetaCacheTest, PinnedEntriesConvergeAfterRelease) {
    FileMetaCache cache(2048, LRUCacheType::SIZE, 1);
    ObjLRUCache::CacheHandle first_handle;
    ObjLRUCache::CacheHandle second_handle;
    auto first = std::make_unique<int>(1);
    auto second = std::make_unique<int>(2);
    ASSERT_TRUE(cache.insert("a", first, &first_handle, 1500));
    ASSERT_TRUE(cache.insert("b", second, &second_handle, 1500));
    EXPECT_GT(cache.cache().get_usage(), cache.cache().get_capacity());
    first_handle = {};
    EXPECT_EQ(*second_handle.data<int>(), 2);
    EXPECT_LE(cache.cache().get_usage(), cache.cache().get_capacity());
    second_handle = {};
    cache.cache().prune_all(true);
    EXPECT_EQ(cache.cache().mem_consumption(), 0);
}

TEST(FileMetaCacheTest, ShardBudgetIncludesKeyAndWrapper) {
    FileMetaCache cache(4096, LRUCacheType::SIZE, 32);
    auto value = std::make_unique<int>(1);
    ObjLRUCache::CacheHandle handle;
    // A 128-byte shard cannot retain a 128-byte value plus its key, handle and wrapper.
    EXPECT_FALSE(cache.insert("key", value, &handle, 128));
    EXPECT_NE(value, nullptr);
    EXPECT_FALSE(handle.valid());
}

TEST(FileMetaCacheTest, ConcurrentReplacementPreservesPinnedValue) {
    FileMetaCache cache(1 << 20, LRUCacheType::SIZE, 1);
    auto first = std::make_unique<int>(1);
    ObjLRUCache::CacheHandle pinned;
    ASSERT_TRUE(cache.insert("same", first, &pinned, sizeof(int)));
    std::thread replace([&] {
        SCOPED_INIT_THREAD_CONTEXT();
        auto next = std::make_unique<int>(2);
        ObjLRUCache::CacheHandle handle;
        EXPECT_TRUE(cache.insert("same", next, &handle, sizeof(int)));
    });
    replace.join();
    EXPECT_EQ(*pinned.data<int>(), 1);
    ObjLRUCache::CacheHandle lookup;
    ASSERT_TRUE(cache.lookup("same", &lookup));
    EXPECT_EQ(*lookup.data<int>(), 2);
    pinned = {};
    lookup = {};
    cache.cache().prune_all(true);
    EXPECT_EQ(cache.cache().mem_consumption(), 0);
}

} // namespace doris