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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/Cache/FileCacheFactory.h
// and modified by Doris

#pragma once

#include <gen_cpp/internal_service.pb.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <thread>
#include <variant>

#include "cloud/cloud_storage_engine.h"
#include "io/fs/file_system.h"

namespace doris::io {

struct S3FileMeta {
    Path path;
    size_t file_size {0};
    io::FileSystemSPtr file_system;
    uint64_t expiration_time {0};
    bool is_cold_data {false};
    std::function<void(Status)> download_callback;
};

struct DownloadTask {
    std::chrono::steady_clock::time_point atime = std::chrono::steady_clock::now();
    std::variant<std::vector<FileCacheBlockMeta>, S3FileMeta> task_message;
    DownloadTask(std::vector<FileCacheBlockMeta> metas) : task_message(std::move(metas)) {}
    DownloadTask(S3FileMeta meta) : task_message(std::move(meta)) {}
    DownloadTask() = default;
};

class FileCacheBlockDownloader {
public:
    FileCacheBlockDownloader(CloudStorageEngine& engine) : _engine(engine) {
        _download_thread = std::thread(&FileCacheBlockDownloader::polling_download_task, this);
    }

    virtual ~FileCacheBlockDownloader() {
        _closed = true;
        _empty.notify_all();
        if (_download_thread.joinable()) {
            _download_thread.join();
        }
    }

    // dowloan into cache block
    virtual void download_blocks(DownloadTask& task) = 0;

    static FileCacheBlockDownloader* instance();
    // submit the task to download queue
    void submit_download_task(DownloadTask task);
    // polling the queue, get the task to download
    void polling_download_task();
    // check whether the tasks about tables finish or not
    void check_download_task(const std::vector<int64_t>& tablets, std::map<int64_t, bool>* done);

protected:
    std::mutex _mtx;
    std::mutex _inflight_mtx;
    // tablet id -> inflight block num of tablet
    std::unordered_map<int64_t, int64_t> _inflight_tablets;
    CloudStorageEngine& _engine;

private:
    std::thread _download_thread;
    std::condition_variable _empty;
    std::deque<DownloadTask> _task_queue;
    std::atomic_bool _closed {false};
    const size_t _max_size {10240};
};

class FileCacheBlockS3Downloader : public FileCacheBlockDownloader {
public:
    FileCacheBlockS3Downloader(CloudStorageEngine& engine) : FileCacheBlockDownloader(engine) {}
    ~FileCacheBlockS3Downloader() override = default;

    void download_blocks(DownloadTask& task) override;

private:
    void download_file_cache_block(std::vector<FileCacheBlockMeta>&);
    static void download_s3_file(S3FileMeta&);
    std::atomic<size_t> _cur_download_file {0};
};

} // namespace doris::io
