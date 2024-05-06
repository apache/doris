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

struct DownloadFileMeta {
    Path path;
    int64_t file_size {-1};
    int64_t offset {0};
    int64_t download_size {-1};
    io::FileSystemSPtr file_system;
    IOContext ctx;
    std::function<void(Status)> download_done;
};

struct DownloadTask {
    std::chrono::steady_clock::time_point atime = std::chrono::steady_clock::now();

    using FileCacheBlockMetaVec =
            ::google::protobuf::RepeatedPtrField< ::doris::FileCacheBlockMeta>;

    std::variant<FileCacheBlockMetaVec, DownloadFileMeta> task_message;
    DownloadTask(FileCacheBlockMetaVec metas) : task_message(std::move(metas)) {}
    DownloadTask(DownloadFileMeta meta) : task_message(std::move(meta)) {}
    DownloadTask() = default;
};

class FileCacheBlockDownloader {
public:
    explicit FileCacheBlockDownloader(CloudStorageEngine& engine);

    ~FileCacheBlockDownloader();

    // download into cache block
    void download_blocks(DownloadTask& task);

    // submit the task to download queue
    void submit_download_task(DownloadTask task);
    // polling the queue, get the task to download
    void polling_download_task();
    // check whether the tasks about tables finish or not
    void check_download_task(const std::vector<int64_t>& tablets, std::map<int64_t, bool>* done);

private:
    void download_file_cache_block(const DownloadTask::FileCacheBlockMetaVec&);
    void download_segment_file(const DownloadFileMeta&);

    CloudStorageEngine& _engine;

    std::thread _poller;
    std::unique_ptr<ThreadPool> _workers;

    std::mutex _mtx;
    bool _closed {false};
    std::condition_variable _empty;
    std::deque<DownloadTask> _task_queue;

    std::mutex _inflight_mtx;
    // tablet id -> inflight block num of tablet
    std::unordered_map<int64_t, int64_t> _inflight_tablets;

    static inline constexpr size_t _max_size {10240};
};

} // namespace doris::io
