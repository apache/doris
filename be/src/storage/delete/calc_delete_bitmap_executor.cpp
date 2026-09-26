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

#include "common/check.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "load/memtable/memtable.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_management/resource_context.h"
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
        std::shared_ptr<WorkloadGroup> workload_group, bool help_while_wait,
        std::shared_ptr<DeleteBitmapCancellation> delete_bitmap_cancellation)
        : _workload_group(std::move(workload_group)),
          // Cancellation can retain the token after this wrapper is destroyed.
          // Keep its pool owner alive until the retained token is released too.
          _thread_token(thread_token.release(),
                        [workload_group = _workload_group](ThreadPoolToken* token) mutable {
                            delete token;
                            workload_group.reset();
                        }),
          _help_while_wait(help_while_wait),
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
    ++_submitted_tasks;
    auto task = [this, func = std::move(func)]() {
        func();
        ++_finished_tasks;
    };
    auto st = _thread_token->submit_func(
            [task = std::move(task), resource_ctx = thread_context()->resource_ctx(),
             tracker = thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker_sptr()]() {
                if (ThreadPool::is_helping_load_task()) {
                    // The parent is already attached. AttachTask cannot be nested.
                    DCHECK(thread_context()->is_attach_task());
                    DCHECK(thread_context()->resource_ctx() == resource_ctx);
                    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
                    task();
                } else {
                    SCOPED_ATTACH_TASK(resource_ctx);
                    // A cloud parent switches to a tablet tracker after attaching
                    // the request context. Preserve that tracker on spare workers too.
                    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(tracker);
                    task();
                }
            });
    if (!st.ok()) {
        // Preserve the submission error before wait() checks for callbacks
        // that did not finish; a rejection is not a generic cancellation.
        _set_status(st);
        return _get_status();
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
    if (_help_while_wait) {
        auto st = _thread_token->wait_and_help();
        if (!st.ok()) {
            // Do not return while callbacks can still access this token or its
            // caller's state. Factory-created nested tokens always own leaves.
            _set_status(st);
            _thread_token->shutdown();
        }
    } else {
        _thread_token->wait();
    }
    RETURN_IF_ERROR(_get_status());
    // A workload-group shutdown may remove queued tasks without executing them.
    if (_finished_tasks.load() != _submitted_tasks.load()) {
        return Status::Cancelled("delete bitmap tasks were cancelled before completion");
    }
    return Status::OK();
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

void CalcDeleteBitmapExecutor::init(const std::string& name, int max_threads,
                                    ThreadPool* load_pool) {
    _load_pool = load_pool;
    static_cast<void>(ThreadPoolBuilder(name)
                              .set_min_threads(1)
                              .set_max_threads(max_threads)
                              .build(&_thread_pool));
}

std::unique_ptr<CalcDeleteBitmapToken> CalcDeleteBitmapExecutor::create_token(
        std::shared_ptr<DeleteBitmapCancellation> delete_bitmap_cancellation) {
    return std::make_unique<CalcDeleteBitmapToken>(
            _thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT), nullptr, false,
            std::move(delete_bitmap_cancellation));
}

std::unique_ptr<CalcDeleteBitmapToken> CalcDeleteBitmapExecutor::create_load_token(
        int64_t load_id, LoadTaskPriority priority, LoadTaskType type) {
    return create_load_token(load_id, priority, type,
                             thread_context()->resource_ctx()->workload_group());
}

std::unique_ptr<CalcDeleteBitmapToken> CalcDeleteBitmapExecutor::create_load_token(
        int64_t load_id, LoadTaskPriority priority, LoadTaskType type,
        std::shared_ptr<WorkloadGroup> wg,
        std::shared_ptr<DeleteBitmapCancellation> delete_bitmap_cancellation) {
    // Nested segment calculations belong to the parent's actual pool. Its
    // attached request context may not carry the workload group used to route it.
    if (auto* pool = ThreadPool::current_load_pool()) {
        DORIS_CHECK(type == LoadTaskType::LEAF) << "Nested bitmap tasks must be leaves";
        return std::make_unique<CalcDeleteBitmapToken>(
                pool->new_load_token(load_id, priority, type), std::move(wg), true,
                std::move(delete_bitmap_cancellation));
    }
    // A commit retry can outlive a dropped workload group. Its pool is stopped;
    // use the default domain in that case. A concurrent stop is reported by submit/wait.
    ThreadPool* pool = wg && !wg->can_be_dropped() ? wg->get_memtable_flush_pool() : nullptr;
    if (pool == nullptr) {
        pool = _load_pool;
    }
    DCHECK(pool != nullptr);
    return std::make_unique<CalcDeleteBitmapToken>(pool->new_load_token(load_id, priority, type),
                                                   std::move(wg), false,
                                                   std::move(delete_bitmap_cancellation));
}

} // namespace doris
