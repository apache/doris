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

#include "io/cache/block_file_cache_downloader.h"

#include <bthread/countdown_event.h>
#include <bvar/bvar.h>
#include <fmt/core.h>
#include <gen_cpp/internal_service.pb.h>

#include <memory>
#include <mutex>
#include <variant>

#include "cloud/cloud_tablet_mgr.h"
#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "olap/rowset/beta_rowset.h"
#include "util/bvar_helper.h"

namespace doris::io {

FileCacheBlockDownloader::FileCacheBlockDownloader(CloudStorageEngine& engine) : _engine(engine) {
    _poller = std::thread(&FileCacheBlockDownloader::polling_download_task, this);
    auto st = ThreadPoolBuilder("FileCacheBlockDownloader")
                      .set_min_threads(4)
                      .set_max_threads(16)
                      .build(&_workers);
    CHECK(st.ok()) << "failed to create FileCacheBlockDownloader";
}

FileCacheBlockDownloader::~FileCacheBlockDownloader() {
    {
        std::lock_guard lock(_mtx);
        _closed = true;
    }
    _empty.notify_all();

    if (_poller.joinable()) {
        _poller.join();
    }

    if (_workers) {
        _workers->shutdown();
    }
}

void FileCacheBlockDownloader::submit_download_task(DownloadTask task) {
    if (!config::enable_file_cache) [[unlikely]] {
        LOG(INFO) << "Skip submit download file task because file cache is not enabled";
        return;
    }

    if (task.task_message.index() == 0) { // download file cache block task
        std::lock_guard lock(_inflight_mtx);
        for (auto& meta : std::get<0>(task.task_message)) {
            ++_inflight_tablets[meta.tablet_id()];
        }
    }

    {
        std::lock_guard lock(_mtx);
        if (_task_queue.size() == _max_size) {
            if (_task_queue.front().task_message.index() == 1) { // download segment file task
                auto& download_file_meta = std::get<1>(_task_queue.front().task_message);
                if (download_file_meta.download_done) {
                    download_file_meta.download_done(
                            Status::InternalError("The downloader queue is full"));
                }
            }
            _task_queue.pop_front(); // Eliminate the earliest task in the queue
        }
        _task_queue.push_back(std::move(task));
        _empty.notify_all();
    }
}

void FileCacheBlockDownloader::polling_download_task() {
    constexpr int64_t hot_interval = 2 * 60 * 60; // 2 hours
    while (!_closed) {
        DownloadTask task;
        {
            std::unique_lock lock(_mtx);
            _empty.wait(lock, [this]() { return !_task_queue.empty() || _closed; });
            if (_closed) {
                break;
            }

            task = std::move(_task_queue.front());
            _task_queue.pop_front();
        }

        if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() -
                                                             task.atime)
                    .count() < hot_interval) {
            auto st = _workers->submit_func(
                    [this, task_ = std::move(task)]() mutable { download_blocks(task_); });
            if (!st.ok()) {
                LOG(WARNING) << "submit download blocks failed: " << st;
            }
        }
    }
}

void FileCacheBlockDownloader::check_download_task(const std::vector<int64_t>& tablets,
                                                   std::map<int64_t, bool>* done) {
    std::lock_guard lock(_inflight_mtx);
    for (int64_t tablet_id : tablets) {
        done->insert({tablet_id, !_inflight_tablets.contains(tablet_id)});
    }
}

std::unordered_map<std::string, RowsetMetaSharedPtr> snapshot_rs_metas(BaseTablet* tablet) {
    std::unordered_map<std::string, RowsetMetaSharedPtr> id_to_rowset_meta_map;
    auto visitor = [&id_to_rowset_meta_map](const RowsetSharedPtr& r) {
        id_to_rowset_meta_map.emplace(r->rowset_meta()->rowset_id().to_string(), r->rowset_meta());
    };
    constexpr bool include_stale = false;
    tablet->traverse_rowsets(visitor, include_stale);
    return id_to_rowset_meta_map;
}

void FileCacheBlockDownloader::download_file_cache_block(
        const DownloadTask::FileCacheBlockMetaVec& metas) {
    std::ranges::for_each(metas, [&](const FileCacheBlockMeta& meta) {
        CloudTabletSPtr tablet;
        if (auto res = _engine.tablet_mgr().get_tablet(meta.tablet_id(), false); !res.has_value()) {
            LOG(INFO) << "failed to find tablet " << meta.tablet_id() << " : " << res.error();
            return;
        } else {
            tablet = std::move(res).value();
        }

        auto id_to_rowset_meta_map = snapshot_rs_metas(tablet.get());
        auto find_it = id_to_rowset_meta_map.find(meta.rowset_id());
        if (find_it == id_to_rowset_meta_map.end()) {
            return;
        }

        auto storage_resource = find_it->second->remote_storage_resource();
        if (!storage_resource) {
            LOG(WARNING) << storage_resource.error();
            return;
        }

        auto download_done = [&, tablet_id = meta.tablet_id()](Status) {
            std::lock_guard lock(_inflight_mtx);
            auto it = _inflight_tablets.find(tablet_id);
            TEST_SYNC_POINT_CALLBACK("FileCacheBlockDownloader::download_file_cache_block");
            if (it == _inflight_tablets.end()) {
                LOG(WARNING) << "inflight ref cnt not exist, tablet id " << tablet_id;
            } else {
                it->second--;
                if (it->second <= 0) {
                    DCHECK_EQ(it->second, 0) << it->first;
                    _inflight_tablets.erase(it);
                }
            }
        };

        DownloadFileMeta download_meta {
                .path = storage_resource.value()->remote_segment_path(*find_it->second,
                                                                      meta.segment_id()),
                .file_size = meta.has_file_size() ? meta.file_size()
                                                  : -1, // To avoid trigger get file size IO
                .offset = meta.offset(),
                .download_size = meta.size(),
                .file_system = storage_resource.value()->fs,
                .ctx =
                        {
                                .is_index_data = meta.cache_type() == ::doris::FileCacheType::INDEX,
                                .expiration_time = meta.expiration_time(),
                        },
                .download_done = std::move(download_done),
        };
        download_segment_file(download_meta);
    });
}

void FileCacheBlockDownloader::download_segment_file(const DownloadFileMeta& meta) {
    FileReaderSPtr file_reader;
    FileReaderOptions opts {
            .cache_type = FileCachePolicy::FILE_BLOCK_CACHE,
            .is_doris_table = true,
            .cache_base_path {},
            .file_size = meta.file_size,
    };
    auto st = meta.file_system->open_file(meta.path, &file_reader, &opts);
    if (!st.ok()) {
        LOG(WARNING) << "failed to download file: " << st;
        if (meta.download_done) {
            meta.download_done(std::move(st));
        }
        return;
    }

    size_t one_single_task_size = config::s3_write_buffer_size;

    int64_t download_size = meta.download_size > 0 ? meta.download_size : file_reader->size();
    size_t task_num = (download_size + one_single_task_size - 1) / one_single_task_size;

    std::unique_ptr<char[]> buffer(new char[one_single_task_size]);

    for (size_t i = 0; i < task_num; i++) {
        size_t offset = meta.offset + i * one_single_task_size;
        size_t size =
                std::min(one_single_task_size, static_cast<size_t>(meta.download_size - offset));
        size_t bytes_read;
        // TODO(plat1ko):
        //  1. Directly append buffer data to file cache
        //  2. Provide `FileReader::async_read()` interface
        auto st = file_reader->read_at(offset, {buffer.get(), size}, &bytes_read, &meta.ctx);
        if (!st.ok()) {
            LOG(WARNING) << "failed to download file: " << st;
            if (meta.download_done) {
                meta.download_done(std::move(st));
            }
            return;
        }
    }

    if (meta.download_done) {
        meta.download_done(Status::OK());
    }
}

void FileCacheBlockDownloader::download_blocks(DownloadTask& task) {
    switch (task.task_message.index()) {
    case 0:
        download_file_cache_block(std::get<0>(task.task_message));
        break;
    case 1:
        download_segment_file(std::get<1>(task.task_message));
        break;
    }
}

} // namespace doris::io
