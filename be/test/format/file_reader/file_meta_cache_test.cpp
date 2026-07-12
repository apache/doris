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

#include <crc32c/crc32c.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>

#include "common/config.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/file_meta_disk_cache.h"
#include "io/fs/file_reader.h"
#include "util/coding.h"
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

io::FileCacheSettings create_file_meta_disk_cache_test_settings() {
    io::FileCacheSettings settings;
    settings.capacity = 1024 * 1024;
    settings.max_file_block_size = 8;
    settings.index_queue_size = 1024 * 1024;
    settings.index_queue_elements = 1024;
    settings.max_query_cache_size = 1024 * 1024;
    settings.storage = "memory";
    return settings;
}

constexpr size_t FILE_META_DISK_CACHE_TEST_HEADER_SIZE = 36;

std::string file_meta_disk_cache_key(FileMetaCacheFormat format, const std::string& meta_key) {
    std::string key = "file_meta_cache:v1:";
    switch (format) {
    case FileMetaCacheFormat::PARQUET:
        key.append("parquet");
        break;
    case FileMetaCacheFormat::ORC:
        key.append("orc");
        break;
    case FileMetaCacheFormat::PARQUET_V2:
        key.append("parquet_v2");
        break;
    case FileMetaCacheFormat::ORC_V2:
        key.append("orc_v2");
        break;
    }
    key.push_back(':');
    key.append(meta_key);
    return key;
}

std::string build_file_meta_disk_cache_value(FileMetaCacheFormat format, int64_t modification_time,
                                             int64_t file_size, std::string_view payload) {
    std::string value;
    value.append("DFMC", 4);
    value.push_back(1);
    value.push_back(static_cast<char>(format));
    value.push_back(0);
    value.push_back(0);
    put_fixed64_le(&value, static_cast<uint64_t>(file_size));
    put_fixed64_le(&value, static_cast<uint64_t>(modification_time));
    put_fixed64_le(&value, payload.size());
    put_fixed32_le(&value, crc32c::Crc32c(payload.data(), payload.size()));
    value.append(payload);
    return value;
}

io::CacheContext create_file_meta_disk_cache_context(io::ReadStatistics* stats) {
    io::CacheContext context;
    context.cache_type = io::FileCacheType::INDEX;
    context.query_id = TUniqueId();
    context.expiration_time = 0;
    context.is_cold_data = false;
    context.is_warmup = false;
    context.stats = stats;
    return context;
}

bool wait_until_file_cache_ready(io::BlockFileCache* cache) {
    for (int i = 0; i < 1000 && !cache->get_async_open_success(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return cache->get_async_open_success();
}
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

    // Same mtime with different size should produce different key
    std::string key_with_different_size = FileMetaCache::get_key(file_name, mtime, file_size + 1);
    EXPECT_NE(key1, key_with_different_size);

    // mtime == 0, still includes file_size
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

    ASSERT_EQ(key_with_mtime.size(), file_name.size() + 2 * sizeof(int64_t));

    EXPECT_EQ(memcmp(key_with_mtime.data(), file_name.data(), file_name.size()), 0);

    int64_t extracted_mtime = 0;
    memcpy(&extracted_mtime, key_with_mtime.data() + file_name.size(), sizeof(int64_t));
    EXPECT_EQ(extracted_mtime, mtime);
    int64_t extracted_file_size = 0;
    memcpy(&extracted_file_size, key_with_mtime.data() + file_name.size() + sizeof(int64_t),
           sizeof(int64_t));
    EXPECT_EQ(extracted_file_size, file_size);

    std::string key_with_filesize = FileMetaCache::get_key(file_name, 0, file_size);
    ASSERT_EQ(key_with_filesize.size(), file_name.size() + 2 * sizeof(int64_t));
    EXPECT_EQ(memcmp(key_with_filesize.data(), file_name.data(), file_name.size()), 0);
    int64_t extracted_zero_mtime = -1;
    memcpy(&extracted_zero_mtime, key_with_filesize.data() + file_name.size(), sizeof(int64_t));
    EXPECT_EQ(extracted_zero_mtime, 0);
    int64_t extracted_filesize = 0;
    memcpy(&extracted_filesize, key_with_filesize.data() + file_name.size() + sizeof(int64_t),
           sizeof(int64_t));
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
    ASSERT_TRUE(cache.insert(FileMetaCache::get_memory_cache_key(context), value, &insert_handle));
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

TEST(FileMetaCacheTest, ContextMemoryCacheKeyIncludesFormat) {
    FileMetaCache cache(1024 * 1024);
    const std::string meta_key = FileMetaCache::get_key("s3://bucket/same.parquet", 123, 456);
    const FileMetaCacheContext parquet_context {.format = FileMetaCacheFormat::PARQUET,
                                                .key = meta_key,
                                                .modification_time = 123,
                                                .file_size = 456,
                                                .enable_memory_cache = true};
    const FileMetaCacheContext parquet_v2_context {.format = FileMetaCacheFormat::PARQUET_V2,
                                                   .key = meta_key,
                                                   .modification_time = 123,
                                                   .file_size = 456,
                                                   .enable_memory_cache = true};

    auto value = std::make_unique<int>(7);
    ObjLRUCache::CacheHandle insert_handle;
    const FileMetaCacheInsertResult insert_result =
            cache.insert(parquet_context, value, &insert_handle, "payload");
    ASSERT_TRUE(insert_result.memory_inserted);

    ObjLRUCache::CacheHandle lookup_handle;
    std::string serialized_meta;
    EXPECT_EQ(cache.lookup(parquet_v2_context, &lookup_handle, &serialized_meta).state,
              FileMetaCacheLookupState::MISS);
    EXPECT_EQ(cache.lookup(parquet_context, &lookup_handle, &serialized_meta).state,
              FileMetaCacheLookupState::MEMORY_HIT);
}

TEST(FileMetaCacheTest, PersistentCacheConfigurationIsIndependentFromRuntimeSizeGate) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    const int64_t old_external_file_meta_disk_cache_max_entry_bytes =
            config::external_file_meta_disk_cache_max_entry_bytes;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        config::external_file_meta_disk_cache_max_entry_bytes =
                old_external_file_meta_disk_cache_max_entry_bytes;
    }};

    config::enable_external_file_meta_disk_cache = true;
    config::external_file_meta_disk_cache_max_entry_bytes = 0;
    EXPECT_TRUE(FileMetaCache::is_persistent_cache_configured());
    EXPECT_FALSE(FileMetaCache::is_persistent_cache_enabled());

    config::external_file_meta_disk_cache_max_entry_bytes = 1024;
    EXPECT_TRUE(FileMetaCache::is_persistent_cache_configured());
    EXPECT_TRUE(FileMetaCache::is_persistent_cache_enabled());

    config::enable_external_file_meta_disk_cache = false;
    EXPECT_FALSE(FileMetaCache::is_persistent_cache_configured());
    EXPECT_FALSE(FileMetaCache::is_persistent_cache_enabled());
}

TEST(FileMetaCacheDiskTest, OrcFormatsUseIndependentKeys) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings = create_file_meta_disk_cache_test_settings();
    io::BlockFileCache block_cache("file_meta_disk_cache_orc_format_isolation_test", settings);
    ASSERT_TRUE(block_cache.initialize().ok());

    const std::string meta_key = FileMetaCache::get_key("s3://bucket/same.orc", 123, 456);
    FileMetaDiskCache disk_cache(&block_cache);
    ASSERT_TRUE(disk_cache
                        .write(FileMetaCacheFormat::ORC, meta_key, 123, 456,
                               "legacy orc serialized tail")
                        .ok());
    ASSERT_TRUE(disk_cache
                        .write(FileMetaCacheFormat::ORC_V2, meta_key, 123, 456,
                               "format v2 orc serialized tail")
                        .ok());

    std::string legacy_payload;
    ASSERT_TRUE(
            disk_cache.read(FileMetaCacheFormat::ORC, meta_key, 123, 456, &legacy_payload).ok());
    EXPECT_EQ(legacy_payload, "legacy orc serialized tail");

    std::string v2_payload;
    ASSERT_TRUE(disk_cache.read(FileMetaCacheFormat::ORC_V2, meta_key, 123, 456, &v2_payload).ok());
    EXPECT_EQ(v2_payload, "format v2 orc serialized tail");
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

TEST(FileMetaCacheDiskTest, PersistentCacheRequiresStableModificationTime) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings = create_file_meta_disk_cache_test_settings();
    io::BlockFileCache block_cache("file_meta_disk_cache_unstable_identity_test", settings);
    ASSERT_TRUE(block_cache.initialize().ok());

    const std::string meta_key = FileMetaCache::get_key("s3://bucket/mtime-zero.parquet", 0, 456);
    auto disk_cache = std::make_unique<FileMetaDiskCache>(&block_cache);
    FileMetaCache cache(0, std::move(disk_cache));
    const FileMetaCacheContext context {.format = FileMetaCacheFormat::PARQUET,
                                        .key = meta_key,
                                        .modification_time = 0,
                                        .file_size = 456,
                                        .enable_memory_cache = false};

    auto value = std::make_unique<std::string>("serialized footer payload");
    ObjLRUCache::CacheHandle handle;
    const FileMetaCacheInsertResult insert_result =
            cache.insert(context, value, &handle, std::string_view(*value));
    EXPECT_FALSE(insert_result.persisted_inserted);
    EXPECT_FALSE(insert_result.memory_inserted);

    int64_t miss_disk_cache = 0;
    FileMetaCacheProfile profile {.miss_disk_cache = &miss_disk_cache};
    std::string serialized_meta;
    ObjLRUCache::CacheHandle lookup_handle;
    EXPECT_EQ(cache.lookup(context, &lookup_handle, &serialized_meta, &profile).state,
              FileMetaCacheLookupState::MISS);
    EXPECT_EQ(miss_disk_cache, 0);
}

TEST(FileMetaCacheDiskTest, PersistentCacheStoresPayloadBehindFileMetaInterface) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings = create_file_meta_disk_cache_test_settings();
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

TEST(FileMetaCacheDiskTest, CapacityFailureDoesNotLeavePartiallyDownloadedBlocks) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    const int64_t old_external_file_meta_disk_cache_max_entry_bytes =
            config::external_file_meta_disk_cache_max_entry_bytes;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        config::external_file_meta_disk_cache_max_entry_bytes =
                old_external_file_meta_disk_cache_max_entry_bytes;
    }};
    config::enable_external_file_meta_disk_cache = true;
    config::external_file_meta_disk_cache_max_entry_bytes = 1024;

    io::FileCacheSettings settings = create_file_meta_disk_cache_test_settings();
    settings.capacity = 64;
    settings.index_queue_size = 64;
    settings.index_queue_elements = 8;
    settings.max_query_cache_size = 0;
    io::BlockFileCache block_cache("file_meta_disk_cache_capacity_failure_test", settings);
    ASSERT_TRUE(block_cache.initialize().ok());

    const std::string meta_key = FileMetaCache::get_key("s3://bucket/too-large.parquet", 123, 456);
    const auto hash = io::BlockFileCache::hash(
            file_meta_disk_cache_key(FileMetaCacheFormat::PARQUET, meta_key));
    FileMetaDiskCache disk_cache(&block_cache);

    const Status status = disk_cache.write(FileMetaCacheFormat::PARQUET, meta_key, 123, 456,
                                           std::string(80, 'p'));
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(block_cache.get_blocks_by_key(hash).empty());
}

TEST(FileMetaCacheDiskTest, DiskEntrySurvivesBlockFileCacheReinitialization) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    const int64_t old_external_file_meta_disk_cache_max_entry_bytes =
            config::external_file_meta_disk_cache_max_entry_bytes;
    Defer restore_config {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
        config::external_file_meta_disk_cache_max_entry_bytes =
                old_external_file_meta_disk_cache_max_entry_bytes;
    }};
    config::enable_external_file_meta_disk_cache = true;
    config::external_file_meta_disk_cache_max_entry_bytes = 1024;

    const std::filesystem::path cache_path =
            std::filesystem::temp_directory_path() /
            ("doris_file_meta_disk_cache_restart_" + std::to_string(getpid()));
    std::error_code error;
    std::filesystem::remove_all(cache_path, error);
    ASSERT_TRUE(std::filesystem::create_directories(cache_path));
    Defer remove_cache_path {[&] { std::filesystem::remove_all(cache_path, error); }};

    io::FileCacheSettings settings = create_file_meta_disk_cache_test_settings();
    settings.storage = "disk";
    const std::string meta_key = FileMetaCache::get_key("s3://bucket/restart.parquet", 123, 456);
    const std::string payload = "serialized footer persisted across cache initialization";

    {
        io::BlockFileCache block_cache(cache_path.string(), settings);
        ASSERT_TRUE(block_cache.initialize().ok());
        ASSERT_TRUE(wait_until_file_cache_ready(&block_cache));
        FileMetaDiskCache disk_cache(&block_cache);
        ASSERT_TRUE(
                disk_cache.write(FileMetaCacheFormat::PARQUET, meta_key, 123, 456, payload).ok());
    }

    {
        io::BlockFileCache block_cache(cache_path.string(), settings);
        ASSERT_TRUE(block_cache.initialize().ok());
        ASSERT_TRUE(wait_until_file_cache_ready(&block_cache));
        FileMetaDiskCache disk_cache(&block_cache);
        std::string output;
        ASSERT_TRUE(
                disk_cache.read(FileMetaCacheFormat::PARQUET, meta_key, 123, 456, &output).ok());
        EXPECT_EQ(output, payload);
    }
}

TEST(FileMetaCacheDiskTest, IncompleteMultiBlockEntryMissesWithoutInvalidating) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings = create_file_meta_disk_cache_test_settings();
    io::BlockFileCache block_cache("file_meta_disk_cache_incomplete_entry_test", settings);
    ASSERT_TRUE(block_cache.initialize().ok());

    const std::string meta_key = FileMetaCache::get_key("s3://bucket/incomplete.parquet", 123, 456);
    const std::string payload = "serialized footer payload spanning several blocks";
    const std::string value =
            build_file_meta_disk_cache_value(FileMetaCacheFormat::PARQUET, 123, 456, payload);
    const auto hash = io::BlockFileCache::hash(
            file_meta_disk_cache_key(FileMetaCacheFormat::PARQUET, meta_key));

    {
        io::ReadStatistics stats;
        io::CacheContext context = create_file_meta_disk_cache_context(&stats);
        auto holder = block_cache.get_or_set(hash, 0, value.size(), context);
        bool covered_header = false;
        for (const auto& block : holder.file_blocks) {
            if (block->range().left >= FILE_META_DISK_CACHE_TEST_HEADER_SIZE) {
                continue;
            }
            ASSERT_EQ(block->get_or_set_downloader(), io::FileBlock::get_caller_id());
            ASSERT_TRUE(
                    block->append(Slice(value.data() + block->range().left, block->range().size()))
                            .ok());
            ASSERT_TRUE(block->finalize().ok());
            covered_header = block->range().right + 1 >= FILE_META_DISK_CACHE_TEST_HEADER_SIZE;
        }
        ASSERT_TRUE(covered_header);
    }

    FileMetaDiskCache disk_cache(&block_cache);
    std::string output;
    const Status status =
            disk_cache.read(FileMetaCacheFormat::PARQUET, meta_key, 123, 456, &output);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(output.empty());
    EXPECT_FALSE(block_cache.get_blocks_by_key(hash).empty());
}

TEST(FileMetaCacheDiskTest, PayloadMissDoesNotRemoveInFlightWriterBlocks) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings = create_file_meta_disk_cache_test_settings();
    io::BlockFileCache block_cache("file_meta_disk_cache_inflight_writer_test", settings);
    ASSERT_TRUE(block_cache.initialize().ok());

    const std::string meta_key = FileMetaCache::get_key("s3://bucket/inflight.parquet", 123, 456);
    const std::string payload = "serialized footer payload spanning several blocks";
    const std::string value =
            build_file_meta_disk_cache_value(FileMetaCacheFormat::PARQUET, 123, 456, payload);
    const auto hash = io::BlockFileCache::hash(
            file_meta_disk_cache_key(FileMetaCacheFormat::PARQUET, meta_key));

    {
        io::ReadStatistics stats;
        io::CacheContext context = create_file_meta_disk_cache_context(&stats);
        auto partial_holder = block_cache.get_or_set(hash, 0, value.size(), context);
        for (const auto& block : partial_holder.file_blocks) {
            if (block->range().left >= FILE_META_DISK_CACHE_TEST_HEADER_SIZE) {
                continue;
            }
            ASSERT_EQ(block->get_or_set_downloader(), io::FileBlock::get_caller_id());
            ASSERT_TRUE(
                    block->append(Slice(value.data() + block->range().left, block->range().size()))
                            .ok());
            ASSERT_TRUE(block->finalize().ok());
        }
    }

    FileMetaDiskCache disk_cache(&block_cache);
    {
        io::ReadStatistics stats;
        io::CacheContext context = create_file_meta_disk_cache_context(&stats);
        auto writer_holder = block_cache.get_or_set(hash, 0, value.size(), context);
        bool has_inflight_payload_block = false;
        for (const auto& block : writer_holder.file_blocks) {
            if (block->range().left >= FILE_META_DISK_CACHE_TEST_HEADER_SIZE) {
                ASSERT_EQ(block->get_or_set_downloader(), io::FileBlock::get_caller_id());
                has_inflight_payload_block = true;
                break;
            }
        }
        ASSERT_TRUE(has_inflight_payload_block);

        std::string output;
        const Status read_status =
                disk_cache.read(FileMetaCacheFormat::PARQUET, meta_key, 123, 456, &output);
        EXPECT_FALSE(read_status.ok());
        EXPECT_TRUE(output.empty());

        for (const auto& block : writer_holder.file_blocks) {
            auto state = block->state();
            if (state == io::FileBlock::State::DOWNLOADED) {
                continue;
            }
            if (state == io::FileBlock::State::EMPTY) {
                ASSERT_EQ(block->get_or_set_downloader(), io::FileBlock::get_caller_id());
            } else {
                ASSERT_EQ(state, io::FileBlock::State::DOWNLOADING);
                ASSERT_TRUE(block->is_downloader());
            }
            ASSERT_TRUE(
                    block->append(Slice(value.data() + block->range().left, block->range().size()))
                            .ok());
            ASSERT_TRUE(block->finalize().ok());
        }
    }

    std::string output;
    ASSERT_TRUE(disk_cache.read(FileMetaCacheFormat::PARQUET, meta_key, 123, 456, &output).ok());
    EXPECT_EQ(output, payload);
}

TEST(FileMetaCacheDiskTest, EmptyPayloadIsNotPersisted) {
    const bool old_enable_external_file_meta_disk_cache =
            config::enable_external_file_meta_disk_cache;
    Defer defer {[&] {
        config::enable_external_file_meta_disk_cache = old_enable_external_file_meta_disk_cache;
    }};
    config::enable_external_file_meta_disk_cache = true;

    io::FileCacheSettings settings = create_file_meta_disk_cache_test_settings();
    io::BlockFileCache block_cache("file_meta_disk_cache_empty_payload_test", settings);
    ASSERT_TRUE(block_cache.initialize().ok());

    const std::string meta_key = FileMetaCache::get_key("s3://bucket/empty.parquet", 123, 456);
    const auto hash = io::BlockFileCache::hash(
            file_meta_disk_cache_key(FileMetaCacheFormat::PARQUET, meta_key));
    FileMetaDiskCache disk_cache(&block_cache);

    const Status status = disk_cache.write(FileMetaCacheFormat::PARQUET, meta_key, 123, 456, "");
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(block_cache.get_blocks_by_key(hash).empty());
}

} // namespace doris
