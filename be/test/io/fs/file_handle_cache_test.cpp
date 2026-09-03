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

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cpp/sync_point.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "gen_cpp/Status_types.h"
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

// Register a SyncPoint callback that returns a fixed value.
template <typename T>
static void set_mock_return(const std::string& point, T value, SyncPoint::CallbackGuard* guard) {
    SyncPoint::get_instance()->set_call_back(
            point,
            [value = std::move(value)](auto&& args) {
                auto* ret = try_any_cast_ret<T>(args);
                ret->first = std::move(value);
                ret->second = true;
            },
            guard);
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
        set_mock_return<hdfsFile>("HdfsFileHandle::ensure_open::hdfsOpenFile", mock_file,
                                  &open_guard);
        set_mock_return<int>("HdfsFileHandle::close::hdfsCloseFile", 0, &close_guard);
        set_mock_return<hdfsFileInfo*>("HdfsFileHandle::init::hdfsGetPathInfo", &mock_info,
                                       &info_guard);
        set_mock_return<Status>("HdfsFileHandle::init::hdfsFreeFileInfo", Status::OK(),
                                &free_guard);
        set_mock_return<int>("HdfsFileHandle::close::hdfsUnbufferFile", 0, &unbuffer_guard);
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
    set_mock_return<hdfsFileInfo*>("HdfsFileHandle::init::hdfsGetPathInfo", nullptr, &guard);

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
    set_mock_return<hdfsFile>("HdfsFileHandle::ensure_open::hdfsOpenFile", nullptr, &open_guard);
    SyncPoint::CallbackGuard err_guard;
    set_mock_return<std::string>("HdfsFileHandle::ensure_open::hdfs_error",
                                 "No such file or directory", &err_guard);

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
        auto reader =
                std::make_shared<HdfsFileReader>(Path(fname), "hdfs", std::move(accessor), mtime);
        char buf[16];
        size_t bytes_read = 0;
        auto st = reader->read_at(0, {buf, sizeof(buf)}, &bytes_read, nullptr);
        ASSERT_FALSE(st.ok());
    }

    FileHandleCache::Accessor accessor2;
    get_handle(*cache, mock_fs, fname, mtime, &accessor2, &cache_hit);
    EXPECT_FALSE(cache_hit);
}

// Serial ensure_open() preserves Status inside call_once.
TEST(FileHandleCacheTest, OpenFailurePreservesStatusInsideCallOnce) {
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    ExclusiveHdfsFileHandle handle(mock_fs, "/test/serial_fail.parquet", 12345);
    ASSERT_TRUE(handle.init(4096).ok());

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard open_guard;
    set_mock_return<hdfsFile>("HdfsFileHandle::ensure_open::hdfsOpenFile", nullptr, &open_guard);
    SyncPoint::CallbackGuard err_guard;
    set_mock_return<std::string>("HdfsFileHandle::ensure_open::hdfs_error",
                                 "No such file or directory", &err_guard);

    // First call opens and fails -> NotFound.
    auto st1 = handle.ensure_open();
    ASSERT_FALSE(st1.ok());
    EXPECT_EQ(st1.code(), TStatusCode::NOT_FOUND);

    // Change hdfs_error mock to a different value; _open_status must be preserved.
    sp->clear_call_back("HdfsFileHandle::ensure_open::hdfs_error");
    set_mock_return<std::string>("HdfsFileHandle::ensure_open::hdfs_error", "Permission denied",
                                 &err_guard);

    auto st2 = handle.ensure_open();
    ASSERT_FALSE(st2.ok());
    EXPECT_EQ(st2.code(), TStatusCode::NOT_FOUND);
}

// Concurrent ensure_open() must return the same Status to all callers.
TEST(FileHandleCacheTest, ConcurrentOpenFailureReturnsSameStatusToAllCallers) {
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    ExclusiveHdfsFileHandle handle(mock_fs, "/test/concurrent_fail.parquet", 12345);
    ASSERT_TRUE(handle.init(4096).ok());

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    SyncPoint::CallbackGuard open_guard;
    set_mock_return<hdfsFile>("HdfsFileHandle::ensure_open::hdfsOpenFile", nullptr, &open_guard);

    // Simulate libhdfs thread-local last-error: only first call returns real message.
    std::atomic<int> err_call_count {0};
    SyncPoint::CallbackGuard err_guard;
    sp->set_call_back(
            "HdfsFileHandle::ensure_open::hdfs_error",
            [&err_call_count](auto&& args) {
                auto* ret = try_any_cast_ret<std::string>(args);
                if (err_call_count.fetch_add(1) == 0) {
                    ret->first = "No such file or directory";
                } else {
                    ret->first = "";
                }
                ret->second = true;
            },
            &err_guard);

    constexpr int kNumThreads = 8;
    std::vector<std::thread> threads;
    std::vector<TStatusCode::type> codes(kNumThreads);
    for (int i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([&handle, &codes, i]() {
            auto st = handle.ensure_open();
            codes[i] = static_cast<TStatusCode::type>(st.code());
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    for (int i = 0; i < kNumThreads; ++i) {
        EXPECT_EQ(codes[i], TStatusCode::NOT_FOUND) << "thread " << i << " got code " << codes[i];
    }
}

// Second read_at after a failed read must not dereference null _handle.
TEST(FileHandleCacheTest, SecondReadAfterFailureDoesNotCrash) {
    MockHandleGuard mg(reinterpret_cast<hdfsFile>(static_cast<uintptr_t>(0xdeadbeef)));
    auto mock_fs = reinterpret_cast<hdfsFS>(static_cast<uintptr_t>(0x1));
    auto cache = make_test_cache();
    const std::string fname = "/test/second_read.parquet";
    constexpr int64_t mtime = 12345;

    bool cache_hit = false;
    FileHandleCache::Accessor accessor;
    get_handle(*cache, mock_fs, fname, mtime, &accessor, &cache_hit);
    auto reader = std::make_shared<HdfsFileReader>(Path(fname), "hdfs", std::move(accessor), mtime);

    char buf[16];
    size_t bytes_read = 0;
    // offset > file_size(4096) -> IOError -> read_at_impl sets _handle=nullptr.
    auto st1 = reader->read_at(5000, {buf, sizeof(buf)}, &bytes_read, nullptr);
    ASSERT_FALSE(st1.ok());
    // Confirm error came from do_read_at_impl's offset guard (sets _handle=nullptr).
    EXPECT_EQ(st1.code(), TStatusCode::IO_ERROR);

    // Second read must not crash; should return InternalError about destroyed handle.
    auto st2 = reader->read_at(0, {buf, sizeof(buf)}, &bytes_read, nullptr);
    ASSERT_FALSE(st2.ok());
    EXPECT_EQ(st2.code(), TStatusCode::INTERNAL_ERROR);
}

} // namespace doris::io
