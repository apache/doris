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

#include "common/logging.h"
#include "load/memtable/memtable.h"
#include "runtime/thread_context.h"
#include "runtime/workload_group/workload_group.h"
#include "runtime/workload_management/resource_context.h"
#include "storage/tablet/base_tablet.h"
#include "util/time.h"

namespace doris {
using namespace ErrorCode;

Status CalcDeleteBitmapToken::submit(BaseTabletSPtr tablet, RowsetSharedPtr cur_rowset,
                                     const segment_v2::SegmentSharedPtr& cur_segment,
                                     const std::vector<RowsetSharedPtr>& target_rowsets,
                                     int64_t end_version, DeleteBitmapPtr delete_bitmap,
                                     RowsetWriter* rowset_writer,
                                     DeleteBitmapPtr tablet_delete_bitmap) {
    {
        std::shared_lock rlock(_lock);
        RETURN_IF_ERROR(_status);
    }

    const auto submit_time_us = MonotonicMicros();
    return _submit_func([=, this]() {
        const auto queue_time_us = MonotonicMicros() - submit_time_us;
        auto st = tablet->calc_segment_delete_bitmap(cur_rowset, cur_segment, target_rowsets,
                                                     delete_bitmap, end_version, rowset_writer,
                                                     tablet_delete_bitmap, queue_time_us);
        if (!st.ok()) {
            LOG(WARNING) << "failed to calc segment delete bitmap, tablet_id: "
                         << tablet->tablet_id() << " rowset: " << cur_rowset->rowset_id()
                         << " seg_id: " << cur_segment->id() << " version: " << end_version
                         << " error: " << st;
            std::lock_guard wlock(_lock);
            if (_status.ok()) {
                _status = st;
            }
        }
    });
}

Status CalcDeleteBitmapToken::submit(BaseTabletSPtr tablet, TabletSchemaSPtr schema,
                                     RowsetId rowset_id,
                                     const std::vector<segment_v2::SegmentSharedPtr>& segments,
                                     DeleteBitmapPtr delete_bitmap) {
    {
        std::shared_lock rlock(_lock);
        RETURN_IF_ERROR(_status);
    }
    const auto submit_time_us = MonotonicMicros();
    return _submit_func([=, this]() {
        const auto queue_time_us = MonotonicMicros() - submit_time_us;
        auto st = tablet->calc_delete_bitmap_between_segments(schema, rowset_id, segments,
                                                              delete_bitmap, queue_time_us);
        if (!st.ok()) {
            LOG(WARNING) << "failed to calc delete bitmap between segments, tablet_id: "
                         << tablet->tablet_id() << " rowset: " << rowset_id
                         << " segments num: " << segments.size() << " error: " << st;
            std::lock_guard wlock(_lock);
            if (_status.ok()) {
                _status = st;
            }
        }
    });
}

Status CalcDeleteBitmapToken::wait() {
    if (_thread_token) {
        _thread_token->wait();
    }
    std::shared_lock rlock(_lock);
    RETURN_IF_ERROR(_status);
    // A workload-group shutdown may remove queued tasks without executing them.
    if (_finished_tasks.load() != _submitted_tasks.load()) {
        return Status::Cancelled("delete bitmap tasks were cancelled before completion");
    }
    return Status::OK();
}

Status CalcDeleteBitmapToken::_submit_func(std::function<void()> func) {
    ++_submitted_tasks;
    auto task = [this, func = std::move(func)]() {
        func();
        ++_finished_tasks;
    };
    if (_thread_token) {
        return _thread_token->submit_func(
                [task = std::move(task), resource_ctx = thread_context()->resource_ctx()]() {
                    SCOPED_ATTACH_TASK(resource_ctx);
                    task();
                });
    }
    // Inline children already run in the parent's attached context, including
    // any tablet-specific MemTracker scope. AttachTask cannot be nested.
    task();
    return Status::OK();
}

void CalcDeleteBitmapExecutor::init(const std::string& name, int max_threads,
                                    ThreadPool* load_pool) {
    _load_pool = load_pool;
    static_cast<void>(ThreadPoolBuilder(name)
                              .set_min_threads(1)
                              .set_max_threads(max_threads)
                              .build(&_thread_pool));
}

std::unique_ptr<CalcDeleteBitmapToken> CalcDeleteBitmapExecutor::create_token() {
    return std::make_unique<CalcDeleteBitmapToken>(
            _thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT));
}

std::unique_ptr<CalcDeleteBitmapToken> CalcDeleteBitmapExecutor::create_load_token(
        int64_t load_id, LoadTaskPriority priority) {
    return create_load_token(load_id, priority, thread_context()->resource_ctx()->workload_group());
}

std::unique_ptr<CalcDeleteBitmapToken> CalcDeleteBitmapExecutor::create_load_token(
        int64_t load_id, LoadTaskPriority priority, std::shared_ptr<WorkloadGroup> wg) {
    // Publish holds tablet locks while waiting for segment calculations. Running
    // these children inline preserves the lock scope without a same-pool wait.
    if (ThreadPool::is_load_worker()) {
        return std::make_unique<CalcDeleteBitmapToken>(nullptr);
    }
    // A commit retry can outlive a dropped workload group. Its pool is stopped;
    // use the default domain in that case. A concurrent stop is reported by submit/wait.
    ThreadPool* pool = wg && !wg->can_be_dropped() ? wg->get_memtable_flush_pool() : nullptr;
    if (pool == nullptr) {
        pool = _load_pool;
    }
    DCHECK(pool != nullptr);
    return std::make_unique<CalcDeleteBitmapToken>(pool->new_load_token(load_id, priority),
                                                   std::move(wg));
}

} // namespace doris
