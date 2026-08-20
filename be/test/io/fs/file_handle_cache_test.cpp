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

// Mocks hdfsOpenFile/hdfsCloseFile/hdfsGetPathInfo/hdfsFreeFileInfo via SyncPoint to avoid JNI.
struct MockHandleGuard {
    SyncPoint::CallbackGuard open_guard;
    SyncPoint::CallbackGuard close_guard;
    SyncPoint::CallbackGuard info_guard;
    SyncPoint::CallbackGuard free_guard;
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

} // namespace doris::io
