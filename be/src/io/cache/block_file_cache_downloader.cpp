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

#include <aws/transfer/TransferHandle.h>
#include <aws/transfer/TransferManager.h>
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
#include "common/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/file_block.h"
#include "io/cache/file_cache_common.h"
#include "io/fs/err_utils.h"
#include "io/fs/s3_common.h"
#include "io/fs/s3_file_bufferpool.h"
#include "io/fs/s3_file_system.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/tablet.h"
// #include "runtime/exec_env.h"
#include "util/bvar_helper.h"
#include "util/s3_util.h"

using Aws::S3::Model::GetObjectRequest;

namespace doris::io {

namespace {
bvar::Adder<uint64_t> file_cache_downloader_counter("file_cache_downloader", "size");

std::string s3_path(std::string_view bucket, std::string_view key) {
    return fmt::format("s3://{}/{}", bucket, key);
}

Status _download_part(std::shared_ptr<::Aws::S3::S3Client> client, std::string key_name,
                      std::string bucket, size_t offset, size_t size, Slice& s) {
    GetObjectRequest request;
    request.WithBucket(bucket).WithKey(key_name);
    request.SetRange(fmt::format("bytes={}-{}", offset, offset + size - 1));
    request.SetResponseStreamFactory(AwsWriteableStreamFactory((void*)s.get_data(), size));
    SCOPED_BVAR_LATENCY(s3_bvar::s3_get_latency);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(DO_S3_GET_RATE_LIMIT(client->GetObject(request)),
                                                "io::_download_part", std::cref(request).get(), &s);

    TEST_SYNC_POINT_CALLBACK("io::_download_part::error", &outcome);
    if (!outcome.IsSuccess()) {
        return s3fs_error(outcome.GetError(),
                          fmt::format("failed to read from {}", s3_path(bucket, key_name)));
    }
    auto bytes_read = outcome.GetResult().GetContentLength();
    if (bytes_read != size) {
        return Status::InternalError("failed to read from {}(bytes read: {}, bytes req: {})",
                                     s3_path(bucket, key_name), bytes_read, size);
    }
    s.size = bytes_read;

    return Status::OK();
}

// maybe we should move this logic inside s3 file bufferpool.cpp
void _append_data_to_file_cache(FileBlocksHolderPtr holder, Slice data) {
    size_t offset = 0;
    std::for_each(
            holder->file_blocks.begin(), holder->file_blocks.end(), [&](FileBlockSPtr& file_block) {
                if (file_block->is_downloader() && offset < data.size) {
                    size_t append_size = std::min(data.size - offset, file_block->range().size());
                    Slice append_data(data.data + offset, append_size);
                    Status st;
                    st = file_block->append(append_data);
                    if (st.ok()) {
                        st = file_block->finalize();
                    }
                    if (!st.ok()) {
                        LOG_WARNING("failed to append data to file cache").error(st);
                    }
                }
                offset += file_block->range().size();
            });
}

} // namespace

struct DownloadTaskExecutor {
    DownloadTaskExecutor() = default;
    ~DownloadTaskExecutor() = default;

    void execute(std::shared_ptr<Aws::S3::S3Client> client, std::string key_name, size_t offset,
                 size_t size, std::string bucket,
                 std::function<FileBlocksHolderPtr(size_t, size_t)> alloc_holder,
                 std::function<void(Status)> download_callback, Slice user_slice) {
        if (!user_slice.empty()) {
            DCHECK(user_slice.get_size() >= size)
                    << "request size " << size << " is larger than preserved size "
                    << user_slice.get_size();
        }
        size_t one_single_task_size = config::s3_write_buffer_size;
        size_t task_num = (size + one_single_task_size - 1) / one_single_task_size;
        auto sync_task = [this, task_num, download_callback](Status st) {
            Defer defer {[&] { _countdown_event.signal(); }};
            bool ret = false;
            if (!st.ok()) [[unlikely]] {
                bool expect = false;
                if (_failed.compare_exchange_strong(expect, true)) {
                    _st = std::move(st);
                }
                ret = true;
            }
            _finished_num++;
            if (_finished_num == task_num) {
                if (download_callback) {
                    download_callback(_st);
                }
            }
            return ret;
        };
        _countdown_event.add_count(task_num);
        for (size_t i = 0; i < task_num; i++) {
            size_t cur_task_off = offset + i * one_single_task_size;
            FileBufferBuilder builder;
            size_t cur_task_size = std::min(one_single_task_size, size - cur_task_off);
            auto download = [client, key_name, bucket, cur_task_off,
                             cur_task_size](Slice& s) mutable {
                return _download_part(client, std::move(key_name), std::move(bucket), cur_task_off,
                                      cur_task_size, s);
            };
            auto append_file_cache = [](FileBlocksHolderPtr holder, Slice s) mutable {
                _append_data_to_file_cache(std::move(holder), s);
            };
            if (alloc_holder != nullptr) {
                builder.set_allocate_file_blocks_holder(
                        [cur_task_off, one_single_task_size, alloc_holder]() {
                            return alloc_holder(cur_task_off, one_single_task_size);
                        });
            }
            builder.set_type(BufferType::DOWNLOAD)
                    .set_download_callback(std::move(download))
                    .set_sync_after_complete_task(sync_task)
                    .set_write_to_local_file_cache(std::move(append_file_cache))
                    .set_is_cancelled([this]() { return _failed.load(); });
            if (!user_slice.empty()) {
                auto write_to_use_buffer = [user_slice, cur_task_off](Slice content,
                                                                      size_t /*off*/) {
                    std::memcpy((void*)(user_slice.get_data() + cur_task_off), content.get_data(),
                                content.get_size());
                };
                builder.set_write_to_use_buffer(std::move(write_to_use_buffer));
            }
            std::shared_ptr<io::FileBuffer> buffer;
            if (auto st = builder.build(&buffer); !st.ok()) {
                LOG_WARNING("build download buffer failed due to {}", st);
                sync_task(std::move(st));
                continue;
            }
            Status st = buffer->submit(std::move(buffer));
            if (!st.ok()) {
                LOG_WARNING("").error(st);
            }
        }
        auto timeout_duration = config::s3_file_writer_log_interval_second;
        timespec current_time;
        // We don't need high accuracy here, so we use time(nullptr)
        // since it's the fastest way to get current time(second)
        auto current_time_second = time(nullptr);
        current_time.tv_sec = current_time_second + timeout_duration;
        current_time.tv_nsec = 0;
        // bthread::countdown_event::timed_wait() should use absolute time
        while (0 != _countdown_event.timed_wait(current_time)) {
            current_time.tv_sec += timeout_duration;
            auto cur_finish_num = _finished_num.load();
            LOG_WARNING("Downloading {} {} {} {} already takes {} seconds, progress {}/{}", bucket,
                        key_name, offset, size, timeout_duration, cur_finish_num, task_num);
        }
    }

private:
    std::atomic_bool _failed {false};
    std::atomic_uint64_t _finished_num {0};
    Status _st {Status::OK()};
    // **Attention** call add_count() before submitting buf to async thread pool
    bthread::CountdownEvent _countdown_event {0};
};
extern void download_file(std::shared_ptr<Aws::S3::S3Client> client, std::string key_name,
                          size_t offset, size_t size, std::string bucket,
                          std::function<FileBlocksHolderPtr(size_t, size_t)> alloc_holder = nullptr,
                          std::function<void(Status)> download_callback = nullptr,
                          Slice s = Slice());

void download_file(std::shared_ptr<Aws::S3::S3Client> client, std::string key_name, size_t offset,
                   size_t size, std::string bucket,
                   std::function<FileBlocksHolderPtr(size_t, size_t)> alloc_holder,
                   std::function<void(Status)> download_callback, Slice s) {
    static_cast<void>(
            ExecEnv::GetInstance()->s3_downloader_download_poller_thread_pool()->submit_func(
                    [c = std::move(client), key_name_ = std::move(key_name), offset, size,
                     bucket_ = std::move(bucket), s, holder = std::move(alloc_holder),
                     cb = std::move(download_callback)]() mutable {
                        DownloadTaskExecutor task;
                        task.execute(std::move(c), std::move(key_name_), offset, size,
                                     std::move(bucket_), std::move(holder), std::move(cb), s);
                    }));
}

FileCacheBlockDownloader* FileCacheBlockDownloader::instance() {
    return ExecEnv::GetInstance()->file_cache_block_downloader();
}

void FileCacheBlockDownloader::submit_download_task(DownloadTask task) {
    if (!config::enable_file_cache) [[unlikely]] {
        LOG(INFO) << "Skip submit download file task because file cache is not enabled";
        return;
    }
    if (task.task_message.index() == 0) {
        std::lock_guard lock(_inflight_mtx);
        for (auto& meta : std::get<0>(task.task_message)) {
            auto it = _inflight_tablets.find(meta.tablet_id());
            if (it == _inflight_tablets.end()) {
                _inflight_tablets.insert({meta.tablet_id(), 1});
            } else {
                it->second++;
            }
        }
    }
    {
        std::lock_guard lock(_mtx);
        if (_task_queue.size() == _max_size) {
            if (_task_queue.front().task_message.index() == 1) {
                auto& s3_file_meta = std::get<1>(_task_queue.front().task_message);
                if (s3_file_meta.download_callback) {
                    s3_file_meta.download_callback(
                            Status::InternalError("The downloader queue is full"));
                }
            }
            _task_queue.pop_front();
        }
        _task_queue.push_back(std::move(task));
        _empty.notify_all();
    }
}

void FileCacheBlockDownloader::polling_download_task() {
    static int64_t hot_interval = 2 * 60 * 60; // 2 hours
    while (!_closed) {
        DownloadTask task;
        {
            std::unique_lock lock(_mtx);
            if (_task_queue.empty()) {
                _empty.wait(lock, [this]() { return !_task_queue.empty() || _closed; });
            }
            if (_closed) {
                break;
            }
            task = std::move(_task_queue.front());
            _task_queue.pop_front();
        }

        if (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() -
                                                             task.atime)
                    .count() < hot_interval) {
            download_blocks(task);
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

void FileCacheBlockS3Downloader::download_file_cache_block(std::vector<FileCacheBlockMeta>& metas) {
    std::ranges::for_each(metas, [&](const FileCacheBlockMeta& meta) {
        auto download_callback = [&, tablet_id = meta.tablet_id()](Status) {
            std::lock_guard lock(_inflight_mtx);
            auto it = _inflight_tablets.find(tablet_id);
            TEST_SYNC_POINT_CALLBACK("FileCacheBlockS3Downloader::download_file_cache_block");
            if (it == _inflight_tablets.end()) {
                LOG(WARNING) << "inflight ref cnt not exist, tablet id " << tablet_id;
            } else {
                it->second--;
                if (it->second < 0) {
                    LOG(WARNING) << "reference count is less than 0, tablet id " << tablet_id
                                 << " ref cnt " << it->second;
                }
                if (it->second <= 0) {
                    _inflight_tablets.erase(it);
                }
            }
        };
        auto res = _engine.tablet_mgr().get_tablet(meta.tablet_id(), false);
        if (!res.has_value()) {
            LOG_WARNING("Failed to find tablet {}", meta.tablet_id()).error(res.error());
            return;
        }
        auto id_to_rowset_meta_map = res.value()->tablet_meta()->snapshot_rs_metas();
        if (auto iter = id_to_rowset_meta_map.find(meta.rowset_id());
            iter != id_to_rowset_meta_map.end()) {
            UInt128Wrapper cache_key = BlockFileCache::hash(meta.file_name());
            BlockFileCache* cache = FileCacheFactory::instance()->get_by_path(cache_key);
            CacheContext context;
            switch (meta.cache_type()) {
            case ::doris::FileCacheType::TTL:
                context.cache_type = FileCacheType::TTL;
                break;
            case ::doris::FileCacheType::INDEX:
                context.cache_type = FileCacheType::INDEX;
                break;
            default:
                context.cache_type = FileCacheType::NORMAL;
            }
            context.expiration_time = meta.expiration_time();
            context.is_cold_data = true;
            auto* s3_file_system = dynamic_cast<S3FileSystem*>(iter->second->fs().get());
            DCHECK(s3_file_system != nullptr);
            auto client = s3_file_system->client_holder()->get();
            if (!client) {
                return;
            }
            auto alloc_holder = [k = cache_key, c = cache, ctx = context](size_t off, size_t size) {
                auto h = c->get_or_set(k, off, size, ctx);
                return std::make_unique<FileBlocksHolder>(std::move(h));
            };
            std::string key_name = s3_file_system->prefix() + '/' +
                                   BetaRowset::remote_segment_path(
                                           meta.tablet_id(), meta.rowset_id(), meta.segment_id());
            TEST_SYNC_POINT_CALLBACK("BlockFileCache::mock_key", &key_name);
            download_file(client, key_name, meta.offset(), meta.size(), s3_file_system->bucket(),
                          std::move(alloc_holder), download_callback);
        }
    });
}

void FileCacheBlockS3Downloader::download_s3_file(S3FileMeta& meta) {
    auto* s3_file_system = dynamic_cast<S3FileSystem*>(meta.file_system.get());
    DCHECK(s3_file_system != nullptr);
    auto client = s3_file_system->client_holder()->get();
    int64_t file_size = meta.file_size;
    if (!client) {
        return;
    }
    if (file_size == 0 || file_size == -1) {
        Status st = s3_file_system->file_size(meta.path, &file_size);
        if (!st.ok()) {
            LOG_WARNING("").error(st);
            return;
        }
    }
    UInt128Wrapper cache_key = BlockFileCache::hash(meta.path.filename().native());
    BlockFileCache* cache = FileCacheFactory::instance()->get_by_path(cache_key);
    CacheContext context;
    if (meta.expiration_time == 0) {
        context.cache_type = FileCacheType::NORMAL;
    } else {
        context.cache_type = FileCacheType::TTL;
    }
    context.is_cold_data = meta.is_cold_data;
    context.expiration_time = meta.expiration_time;
    auto alloc_holder = [k = cache_key, c = cache, ctx = context](size_t off, size_t size) {
        auto h = c->get_or_set(k, off, size, ctx);
        return std::make_unique<FileBlocksHolder>(std::move(h));
    };
    std::string key_name = s3_file_system->prefix() + '/' + meta.path.native();
    TEST_SYNC_POINT_CALLBACK("BlockFileCache::remove_prefix", &key_name);
    download_file(client, key_name, 0, file_size, s3_file_system->bucket(), std::move(alloc_holder),
                  std::move(meta.download_callback));
}

void FileCacheBlockS3Downloader::download_blocks(DownloadTask& task) {
    switch (task.task_message.index()) {
    case 0:
        download_file_cache_block(std::get<0>(task.task_message));
        break;
    case 1:
        download_s3_file(std::get<1>(task.task_message));
        break;
    }
}

} // namespace doris::io
