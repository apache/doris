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

#include <memory>

#include "common/config.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "io/cache/block_file_cache.h"
#include "io/fs/file_meta_disk_cache.h"
#include "io/fs/file_reader.h"
#include "util/defer_op.h"

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
    FileMetaCache cache(1024 * 1024);

    int* value = new int(12345);
    ObjLRUCache::CacheHandle handle;

    cache.insert("key_int", value, &handle);
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

TEST(FileMetaCacheTest, ContextLookupReportsMemoryHit) {
    FileMetaCache cache(1024 * 1024);
    const std::string meta_key = FileMetaCache::get_key("s3://bucket/memory.parquet", 123, 456);
    const FileMetaCacheContext context {.format = FileMetaCacheFormat::PARQUET,
                                        .key = meta_key,
                                        .modification_time = 123,
                                        .file_size = 456};

    auto value = std::make_unique<std::string>("serialized footer payload");
    ObjLRUCache::CacheHandle insert_handle;
    ASSERT_TRUE(cache.insert(meta_key, value, &insert_handle));
    ASSERT_TRUE(insert_handle.valid());

    int64_t hit_cache = 0;
    int64_t hit_memory_cache = 0;
    int64_t hit_disk_cache = 0;
    int64_t miss_disk_cache = 0;
    FileMetaCacheProfile profile {.hit_cache = &hit_cache,
                                  .hit_memory_cache = &hit_memory_cache,
                                  .hit_disk_cache = &hit_disk_cache,
                                  .miss_disk_cache = &miss_disk_cache};

    ObjLRUCache::CacheHandle lookup_handle;
    std::string serialized_meta;
    const FileMetaCacheLookupResult result =
            cache.lookup(context, &lookup_handle, &serialized_meta, &profile);

    EXPECT_EQ(result.state, FileMetaCacheLookupState::MEMORY_HIT);
    EXPECT_TRUE(lookup_handle.valid());
    EXPECT_TRUE(serialized_meta.empty());
    EXPECT_EQ(hit_cache, 1);
    EXPECT_EQ(hit_memory_cache, 1);
    EXPECT_EQ(hit_disk_cache, 0);
    EXPECT_EQ(miss_disk_cache, 0);
}

TEST(FileMetaCacheTest, ContextInsertCanSkipMemoryCacheWithoutPersistentStore) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
    }};
    config::enable_external_file_meta_disk_cache = false;

    FileMetaCache cache(1024 * 1024);
    const std::string meta_key =
            FileMetaCache::get_key("s3://bucket/skip-memory.parquet", 123, 456);
    const FileMetaCacheContext context {.format = FileMetaCacheFormat::PARQUET,
                                        .key = meta_key,
                                        .modification_time = 123,
                                        .file_size = 456,
                                        .enable_memory_cache = false};
    auto value = std::make_unique<std::string>("serialized footer payload");
    ObjLRUCache::CacheHandle handle;

    const FileMetaCacheInsertResult result =
            cache.insert(context, value, &handle, std::string_view(*value));

    EXPECT_FALSE(result.persisted_inserted);
    EXPECT_FALSE(result.memory_inserted);
    EXPECT_FALSE(handle.valid());
    EXPECT_NE(value, nullptr);

    ObjLRUCache::CacheHandle lookup_handle;
    EXPECT_FALSE(cache.lookup(meta_key, &lookup_handle));
}

TEST(FileMetaCacheDiskTest, PersistentCacheStoresPayloadBehindFileMetaInterface) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings;
    settings.capacity = 1024 * 1024;
    settings.max_file_block_size = 8;
    settings.index_queue_size = 1024 * 1024;
    settings.index_queue_elements = 1024;
    settings.max_query_cache_size = 1024 * 1024;
    settings.storage = "memory";
    io::BlockFileCache block_cache("file_meta_disk_cache_interface_test", settings);
    ASSERT_TRUE(block_cache.initialize().ok());

    auto disk_cache = std::make_unique<FileMetaDiskCache>(&block_cache);
    FileMetaCache cache(1024 * 1024, std::move(disk_cache));
    const std::string meta_key = FileMetaCache::get_key("s3://bucket/interface.parquet", 123, 456);
    const FileMetaCacheContext context {.format = FileMetaCacheFormat::PARQUET,
                                        .key = meta_key,
                                        .modification_time = 123,
                                        .file_size = 456,
                                        .enable_memory_cache = false};
    const std::string payload = "serialized footer payload";
    auto value = std::make_unique<std::string>(payload);
    ObjLRUCache::CacheHandle handle;

    const FileMetaCacheInsertResult insert_result = cache.insert(context, value, &handle, payload);
    ASSERT_TRUE(insert_result.persisted_inserted);
    ASSERT_FALSE(insert_result.memory_inserted);

    ObjLRUCache::CacheHandle lookup_handle;
    std::string output;
    const FileMetaCacheLookupResult lookup_result = cache.lookup(context, &lookup_handle, &output);
    EXPECT_EQ(lookup_result.state, FileMetaCacheLookupState::PERSISTED_HIT);
    EXPECT_EQ(output, payload);
    EXPECT_FALSE(lookup_handle.valid());

    const FileMetaCacheContext stale_context {.format = FileMetaCacheFormat::PARQUET,
                                              .key = meta_key,
                                              .modification_time = 124,
                                              .file_size = 456,
                                              .enable_memory_cache = false};
    output.clear();
    const FileMetaCacheLookupResult stale_lookup_result =
            cache.lookup(stale_context, &lookup_handle, &output);
    EXPECT_EQ(stale_lookup_result.state, FileMetaCacheLookupState::MISS);
    EXPECT_TRUE(output.empty());
}

} // namespace doris
