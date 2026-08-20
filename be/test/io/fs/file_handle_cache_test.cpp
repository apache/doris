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

#include "cpp/sync_point.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "io/fs/hdfs_file_reader.h"

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

// init(file_size>0) does not open the file.
TEST(FileHandleCacheTest, InitWithKnownFileSizeDoesNotOpenFile) {
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    ExclusiveHdfsFileHandle handle(mock_fs, "/nonexistent/file.parquet", 12345);
    auto st = handle.init(4096);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(handle.file_size(), 4096);
    EXPECT_EQ(handle.file(), nullptr);
}

// Destructor is safe when file was never opened.
TEST(FileHandleCacheTest, DestructorSafeWithoutOpen) {
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    {
        ExclusiveHdfsFileHandle handle(mock_fs, "/nonexistent/file.parquet", 12345);
        ASSERT_TRUE(handle.init(4096).ok());
        EXPECT_EQ(handle.file(), nullptr);
    }
}

// Mocks hdfsOpenFile/hdfsCloseFile/hdfsGetPathInfo/hdfsFreeFileInfo/hdfsUnbufferFile via SyncPoint to avoid JNI.
struct MockHandleGuard {
    SyncPoint::CallbackGuard open_guard;
    SyncPoint::CallbackGuard close_guard;
    SyncPoint::CallbackGuard info_guard;
    SyncPoint::CallbackGuard free_guard;
    SyncPoint::CallbackGuard unbuffer_guard;
    static inline hdfsFileInfo mock_info;
    MockHandleGuard(hdfsFile mock_file, int64_t file_size = 4096) {
        mock_info.mSize = file_size;
        auto* sp = SyncPoint::get_instance();
        sp->enable_processing();
        sp->set_call_back(
                "HdfsFileHandle::ensure_open::hdfsOpenFile",
                [mock_file](auto&& args) {
                    auto* ret = try_any_cast_ret<hdfsFile>(args);
                    ret->first = mock_file;
                    ret->second = true;
                },
                &open_guard);
        sp->set_call_back(
                "HdfsFileHandle::close::hdfsCloseFile",
                [](auto&& args) {
                    auto* ret = try_any_cast_ret<int>(args);
                    ret->first = 0;
                    ret->second = true;
                },
                &close_guard);
        sp->set_call_back(
                "HdfsFileHandle::init::hdfsGetPathInfo",
                [](auto&& args) {
                    auto* ret = try_any_cast_ret<hdfsFileInfo*>(args);
                    ret->first = &mock_info;
                    ret->second = true;
                },
                &info_guard);
        sp->set_call_back(
                "HdfsFileHandle::init::hdfsFreeFileInfo",
                [](auto&& args) {
                    auto* ret = try_any_cast_ret<Status>(args);
                    ret->first = Status::OK();
                    ret->second = true;
                },
                &free_guard);
        sp->set_call_back(
                "HdfsFileHandle::close::hdfsUnbufferFile",
                [](auto&& args) {
                    auto* ret = try_any_cast_ret<int>(args);
                    ret->first = 0;
                    ret->second = true;
                },
                &unbuffer_guard);
    }
};

// ensure_open() succeeds via SyncPoint mock.
TEST(FileHandleCacheTest, EnsureOpenSucceedsWithMock) {
    MockHandleGuard mg(reinterpret_cast<hdfsFile>(static_cast<uintptr_t>(0xdeadbeef)));
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    ExclusiveHdfsFileHandle handle(mock_fs, "/test/file.parquet", 12345);
    ASSERT_TRUE(handle.init(4096).ok());
    EXPECT_EQ(handle.file(), nullptr);

    ASSERT_TRUE(handle.ensure_open().ok());
    EXPECT_NE(handle.file(), nullptr);
}

// ensure_open() fails when mock returns nullptr.
TEST(FileHandleCacheTest, EnsureOpenFailsWithMock) {
    MockHandleGuard mg(nullptr);
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    ExclusiveHdfsFileHandle handle(mock_fs, "/test/file.parquet", 12345);
    ASSERT_TRUE(handle.init(4096).ok());

    auto st = handle.ensure_open();
    ASSERT_FALSE(st.ok());
    EXPECT_EQ(handle.file(), nullptr);
}

// ensure_open() is idempotent via call_once.
TEST(FileHandleCacheTest, EnsureOpenIsIdempotentWithMock) {
    MockHandleGuard mg(reinterpret_cast<hdfsFile>(static_cast<uintptr_t>(0xdeadbeef)));
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    ExclusiveHdfsFileHandle handle(mock_fs, "/test/file.parquet", 12345);
    ASSERT_TRUE(handle.init(4096).ok());

    ASSERT_TRUE(handle.ensure_open().ok());
    ASSERT_TRUE(handle.ensure_open().ok());
}

// init(-1) fetches file_size via mocked hdfsGetPathInfo.
TEST(FileHandleCacheTest, InitWithUnknownFileSizeWithMock) {
    MockHandleGuard mg(nullptr, 8192);
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    ExclusiveHdfsFileHandle handle(mock_fs, "/test/file.parquet", 12345);

    ASSERT_TRUE(handle.init(-1).ok());
    EXPECT_EQ(handle.file_size(), 8192);
    EXPECT_EQ(handle.file(), nullptr);
}

// init(-1) fails when hdfsGetPathInfo returns nullptr.
TEST(FileHandleCacheTest, InitFailsWhenGetPathInfoReturnsNull) {
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    ExclusiveHdfsFileHandle handle(mock_fs, "/test/file.parquet", 12345);

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "HdfsFileHandle::init::hdfsGetPathInfo",
            [](auto&& args) {
                auto* ret = try_any_cast_ret<hdfsFileInfo*>(args);
                ret->first = nullptr;
                ret->second = true;
            },
            &guard);

    auto st = handle.init(-1);
    ASSERT_FALSE(st.ok());
    EXPECT_EQ(st.code(), TStatusCode::INTERNAL_ERROR);
}

// ensure_open() returns NotFound when error contains "No such file or directory".
TEST(FileHandleCacheTest, EnsureOpenReturnsNotFoundForMissingFile) {
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    ExclusiveHdfsFileHandle handle(mock_fs, "/test/missing.parquet", 12345);
    ASSERT_TRUE(handle.init(4096).ok());

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard open_guard;
    sp->set_call_back(
            "HdfsFileHandle::ensure_open::hdfsOpenFile",
            [](auto&& args) {
                auto* ret = try_any_cast_ret<hdfsFile>(args);
                ret->first = nullptr;
                ret->second = true;
            },
            &open_guard);
    SyncPoint::CallbackGuard err_guard;
    sp->set_call_back(
            "HdfsFileHandle::ensure_open::hdfs_error",
            [](auto&& args) {
                auto* ret = try_any_cast_ret<std::string>(args);
                ret->first = "No such file or directory";
                ret->second = true;
            },
            &err_guard);

    auto st = handle.ensure_open();
    ASSERT_FALSE(st.ok());
    EXPECT_EQ(st.code(), TStatusCode::NOT_FOUND);
}

// --- Cache lifecycle tests ---

// Helper: create a FileHandleCache with small capacity for testing.
static std::unique_ptr<FileHandleCache> make_test_cache() {
    return std::make_unique<FileHandleCache>(4, 1, 0);
}

// Helper: get a file handle from cache, asserting success.
static void get_handle(FileHandleCache& cache, const hdfsFS& fs, const std::string& fname,
                       int64_t mtime, FileHandleCache::Accessor* accessor, bool* cache_hit) {
    ASSERT_TRUE(cache.get_file_handle(fs, fname, mtime, 4096, false, accessor, cache_hit).ok());
}

// ensure_open succeeds → ~Accessor releases (unbuffer OK) → next get_file_handle hits cache.
TEST(FileHandleCacheTest, OpenedHandleReleasedBackToCache) {
    MockHandleGuard mg(reinterpret_cast<hdfsFile>(static_cast<uintptr_t>(0xdeadbeef)));
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    auto cache = make_test_cache();
    const std::string fname = "/test/opened_release.parquet";
    constexpr int64_t mtime = 12345;

    bool cache_hit = false;
    {
        FileHandleCache::Accessor accessor;
        get_handle(*cache, mock_fs, fname, mtime, &accessor, &cache_hit);
        EXPECT_FALSE(cache_hit);
        ASSERT_TRUE(accessor.get()->ensure_open().ok());
        EXPECT_NE(accessor.get()->file(), nullptr);
    }

    FileHandleCache::Accessor accessor2;
    get_handle(*cache, mock_fs, fname, mtime, &accessor2, &cache_hit);
    EXPECT_TRUE(cache_hit);
    EXPECT_NE(accessor2.get()->file(), nullptr);
}

// ensure_open fails → ~Accessor destroys → next get_file_handle misses cache.
TEST(FileHandleCacheTest, OpenFailedHandleDestroyedNotCached) {
    MockHandleGuard mg(nullptr);
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    auto cache = make_test_cache();
    const std::string fname = "/test/open_fail.parquet";
    constexpr int64_t mtime = 12345;

    bool cache_hit = false;
    {
        FileHandleCache::Accessor accessor;
        get_handle(*cache, mock_fs, fname, mtime, &accessor, &cache_hit);
        EXPECT_FALSE(cache_hit);
        auto st = accessor.get()->ensure_open();
        ASSERT_FALSE(st.ok());
        EXPECT_EQ(accessor.get()->file(), nullptr);
    }

    FileHandleCache::Accessor accessor2;
    get_handle(*cache, mock_fs, fname, mtime, &accessor2, &cache_hit);
    EXPECT_FALSE(cache_hit);
}

// read_at triggers ensure_open failure → reader destroyed → ~Accessor destroys → cache miss.
TEST(FileHandleCacheTest, ReadAtOpenFailedHandleDestroyedNotCached) {
    MockHandleGuard mg(nullptr);
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    auto cache = make_test_cache();
    const std::string fname = "/test/read_at_fail.parquet";
    constexpr int64_t mtime = 12345;

    bool cache_hit = false;
    {
        FileHandleCache::Accessor accessor;
        get_handle(*cache, mock_fs, fname, mtime, &accessor, &cache_hit);
        EXPECT_FALSE(cache_hit);
        auto reader = std::make_shared<HdfsFileReader>(Path(fname), "hdfs", std::move(accessor),
                                                       nullptr, mtime);
        char buf[16];
        size_t bytes_read = 0;
        auto st = reader->read_at(0, {buf, sizeof(buf)}, &bytes_read, nullptr);
        ASSERT_FALSE(st.ok());
    }

    FileHandleCache::Accessor accessor2;
    get_handle(*cache, mock_fs, fname, mtime, &accessor2, &cache_hit);
    EXPECT_FALSE(cache_hit);
}

} // namespace doris::io
