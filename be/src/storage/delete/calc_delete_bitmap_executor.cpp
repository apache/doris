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

#include "storage/delete/calc_delete_bitmap_executor.h"

#include <gen_cpp/olap_file.pb.h>

#include <ostream>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "load/memtable/memtable.h"
#include "load/memtable/memtable_flush_executor.h"
#include "storage/tablet/base_tablet.h"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

void DeleteBitmapCancellation::_register_token(const std::shared_ptr<ThreadPoolToken>& token) {
    {
        std::lock_guard lock(_lock);
        if (_status.ok()) {
            _tokens.emplace_back(token);
            return;
        }
    }
    // A token created after cancellation must reject submissions too.
    token->shutdown();
}

void DeleteBitmapCancellation::cancel(const Status& reason) {
    DCHECK(!reason.ok());
    std::vector<std::shared_ptr<ThreadPoolToken>> tokens;
    {
        std::lock_guard lock(_lock);
        _status.update(reason);
        for (const auto& weak_token : _tokens) {
            if (auto token = weak_token.lock()) {
                tokens.push_back(std::move(token));
            }
        }
    }
    TEST_SYNC_POINT_CALLBACK("DeleteBitmapCancellation::cancel:before_shutdown", this);
    // Publish to all tasks before waiting. Neither registration nor task completion
    // needs to wait for this lock while shutdown waits for running tasks.
    for (const auto& token : tokens) {
        token->shutdown();
    }
}

CalcDeleteBitmapToken::CalcDeleteBitmapToken(
        std::unique_ptr<ThreadPoolToken> thread_token,
        std::shared_ptr<DeleteBitmapCancellation> delete_bitmap_cancellation)
        : _thread_token(std::move(thread_token)),
          _status(Status::OK()),
          _delete_bitmap_cancellation(std::move(delete_bitmap_cancellation)) {
    if (_delete_bitmap_cancellation) {
        _delete_bitmap_cancellation->_register_token(_thread_token);
    }
}

CalcDeleteBitmapToken::~CalcDeleteBitmapToken() {
    // A concurrent cancellation can retain the underlying token. Finish callbacks
    // that capture this before destroying the wrapper's status and lock.
    _thread_token->shutdown();
}

Status CalcDeleteBitmapToken::submit(BaseTabletSPtr tablet, RowsetSharedPtr cur_rowset,
                                     const segment_v2::SegmentSharedPtr& cur_segment,
                                     const std::vector<RowsetSharedPtr>& target_rowsets,
                                     int64_t end_version, DeleteBitmapPtr delete_bitmap,
                                     RowsetWriter* rowset_writer,
                                     DeleteBitmapPtr tablet_delete_bitmap) {
    const auto submit_time_us = MonotonicMicros();
    return submit_func([=]() {
        const auto queue_time_us = MonotonicMicros() - submit_time_us;
        auto st = tablet->calc_segment_delete_bitmap(cur_rowset, cur_segment, target_rowsets,
                                                     delete_bitmap, end_version, rowset_writer,
                                                     tablet_delete_bitmap, queue_time_us);
        if (!st.ok()) {
            LOG(WARNING) << "failed to calc segment delete bitmap, tablet_id: "
                         << tablet->tablet_id() << " rowset: " << cur_rowset->rowset_id()
                         << " seg_id: " << cur_segment->id() << " version: " << end_version
                         << " error: " << st;
        }
        return st;
    });
}

Status CalcDeleteBitmapToken::submit(BaseTabletSPtr tablet, TabletSchemaSPtr schema,
                                     RowsetId rowset_id,
                                     const std::vector<segment_v2::SegmentSharedPtr>& segments,
                                     DeleteBitmapPtr delete_bitmap) {
    const auto submit_time_us = MonotonicMicros();
    // Only test builds use the token pointer; avoid an unused explicit lambda capture.
    [[maybe_unused]] auto* token = this;
    return submit_func([=]() {
        TEST_SYNC_POINT_CALLBACK("CalcDeleteBitmapToken::submit:before_between_segments", token);
        const auto queue_time_us = MonotonicMicros() - submit_time_us;
        auto st = tablet->calc_delete_bitmap_between_segments(schema, rowset_id, segments,
                                                              delete_bitmap, queue_time_us);
        if (!st.ok()) {
            LOG(WARNING) << "failed to calc delete bitmap between segments, tablet_id: "
                         << tablet->tablet_id() << " rowset: " << rowset_id
                         << " segments num: " << segments.size() << " error: " << st;
        }
        return st;
    });
}

Status CalcDeleteBitmapToken::_submit_func(std::function<void()> func) {
    TEST_SYNC_POINT_CALLBACK("CalcDeleteBitmapToken::submit_func:before_submit", this);
    auto st = _thread_token->submit_func(std::move(func));
    if (!st.ok()) {
        // Cancellation may shut down the token after the initial status check.
        // Preserve the published failure instead of reporting a pool shutdown error.
        RETURN_IF_ERROR(_get_status());
    }
    return st;
}

Status CalcDeleteBitmapToken::_get_status() {
    std::shared_lock rlock(_lock);
    RETURN_IF_ERROR(_status);
    return _delete_bitmap_cancellation && !_delete_bitmap_cancellation->ok()
                   ? _delete_bitmap_cancellation->status()
                   : Status::OK();
}

Status CalcDeleteBitmapToken::wait() {
    TEST_SYNC_POINT_CALLBACK("CalcDeleteBitmapToken::wait:before_wait", this);
    _thread_token->wait();
    return _get_status();
}

void CalcDeleteBitmapToken::_set_status(const Status& st) {
    DCHECK(!st.ok());
    // Serialize this token's failure with load-wide cancellation publication.
    // Calculation errors stay local to this token; an already-published load
    // cancellation wins over later callback failures or explicit token cancellation.
    std::unique_lock<std::mutex> cancellation_lock;
    if (_delete_bitmap_cancellation) {
        cancellation_lock = std::unique_lock(_delete_bitmap_cancellation->_lock);
    }
    std::lock_guard wlock(_lock);
    if (_status.ok()) {
        _status = _delete_bitmap_cancellation && !_delete_bitmap_cancellation->ok()
                          ? _delete_bitmap_cancellation->status()
                          : st;
    }
}

void CalcDeleteBitmapToken::cancel(const Status& st) {
    _set_status(st);
    // Do not hold _lock while waiting: running tasks may need it to report an error.
    _thread_token->shutdown();
}

void CalcDeleteBitmapExecutor::init(const std::string& name, int max_threads) {
    int min_threads = 1;
    if (config::enable_adaptive_flush_threads) {
        int num_cpus = std::thread::hardware_concurrency();
        if (num_cpus <= 0) num_cpus = 1;
        // Disk count and per-store count are unused in adaptive mode.
        const auto limits = MemTableFlushExecutor::calc_flush_thread_count(num_cpus, 0, 0);
        min_threads = limits.first;
        max_threads = limits.second;
    }
    static_cast<void>(ThreadPoolBuilder(name)
                              .set_min_threads(min_threads)
                              .set_max_threads(max_threads)
                              .build(&_thread_pool));
}

std::unique_ptr<CalcDeleteBitmapToken> CalcDeleteBitmapExecutor::create_token(
        std::shared_ptr<DeleteBitmapCancellation> delete_bitmap_cancellation) {
    return std::make_unique<CalcDeleteBitmapToken>(
            _thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT),
            std::move(delete_bitmap_cancellation));
}

} // namespace doris
