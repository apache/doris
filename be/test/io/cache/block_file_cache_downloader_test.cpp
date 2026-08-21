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

#include "io/cache/block_file_cache_downloader.h"

#include <gtest/gtest.h>

#include <utility>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "common/config.h"
#include "cpp/sync_point.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "util/defer_op.h"

namespace doris::io {

class FileCacheBlockDownloaderTest : public testing::Test {
public:
    FileCacheBlockDownloaderTest() : _engine(CloudStorageEngine(EngineOptions {})) {}

    void SetUp() override {
        _old_enable_file_cache = config::enable_file_cache;
        _old_thread_num_min = config::file_cache_downloader_thread_num_min;
        _old_thread_num_max = config::file_cache_downloader_thread_num_max;
        config::enable_file_cache = true;
        config::file_cache_downloader_thread_num_min = 2;
        config::file_cache_downloader_thread_num_max = 4;
    }

    void TearDown() override {
        config::enable_file_cache = _old_enable_file_cache;
        config::file_cache_downloader_thread_num_min = _old_thread_num_min;
        config::file_cache_downloader_thread_num_max = _old_thread_num_max;
    }

protected:
    CloudStorageEngine _engine;

private:
    bool _old_enable_file_cache = false;
    int32_t _old_thread_num_min = 0;
    int32_t _old_thread_num_max = 0;
};

TEST_F(FileCacheBlockDownloaderTest, DownloadSegmentForcesSynchronousCacheWriteForBothDryRunModes) {
    const Path directory = "ut_dir/block_file_cache_downloader_sync_override";
    const Path file = directory / "segment.dat";
    static_cast<void>(global_local_filesystem()->delete_file(file));
    static_cast<void>(global_local_filesystem()->delete_directory(directory));
    ASSERT_TRUE(global_local_filesystem()->create_directory(directory).ok());
    Defer cleanup {[&]() {
        static_cast<void>(global_local_filesystem()->delete_file(file));
        static_cast<void>(global_local_filesystem()->delete_directory(directory));
    }};
    FileWriterPtr writer;
    ASSERT_TRUE(global_local_filesystem()->create_file(file, &writer).ok());
    ASSERT_TRUE(writer->append(Slice("x", 1)).ok());
    ASSERT_TRUE(writer->close().ok());

    const bool old_dryrun_config = config::enable_reader_dryrun_when_download_file_cache;
    const int64_t old_buffer_size = config::s3_write_buffer_size;
    Defer restore_config {[&]() {
        config::enable_reader_dryrun_when_download_file_cache = old_dryrun_config;
        config::s3_write_buffer_size = old_buffer_size;
    }};
    config::s3_write_buffer_size = 1;

    std::vector<std::pair<bool, CacheWriteMode>> observed_contexts;
    auto* sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sync_point->set_call_back(
            "FileCacheBlockDownloader::download_segment_file:before_read",
            [&](auto&& values) {
                auto* context = try_any_cast<IOContext*>(values.back());
                ASSERT_TRUE(context->cache_write_mode_override.has_value());
                observed_contexts.emplace_back(context->is_dryrun,
                                               *context->cache_write_mode_override);
            },
            &guard);
    sync_point->enable_processing();
    Defer clear_sync_point {[&]() {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    }};

    FileCacheBlockDownloader downloader(_engine);
    for (bool is_dryrun : {false, true}) {
        config::enable_reader_dryrun_when_download_file_cache = is_dryrun;
        Status completion_status = Status::InternalError("download completion was not called");
        DownloadFileMeta meta {
                .path = file,
                .file_size = 1,
                .offset = 0,
                .download_size = 1,
                .file_system = global_local_filesystem(),
                .ctx = IOContext {.is_dryrun = is_dryrun},
                .download_done = [&](Status status) { completion_status = std::move(status); },
                .tablet_id = 10086,
        };
        DownloadTask task(std::move(meta));
        downloader.download_blocks(task);
        EXPECT_TRUE(completion_status.ok());
    }

    EXPECT_EQ(observed_contexts, (std::vector<std::pair<bool, CacheWriteMode>> {
                                         {false, CacheWriteMode::SYNC_WRITE},
                                         {true, CacheWriteMode::SYNC_WRITE},
                                 }));
}

} // namespace doris::io
