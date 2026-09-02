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

#include "runtime/result_buffer_mgr.h"

#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>

#include <cstdint>

// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <condition_variable>
#include <memory>
#include <ostream>
#include <unordered_set>
#include <utility>

#include "arrow/type_fwd.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "common/status.h"
#include "exec/sink/writer/varrow_flight_result_writer.h"
#include "exec/sink/writer/vmysql_result_writer.h"
#include "runtime/exec_env.h"
#include "runtime/result_block_buffer.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "util/uid_util.h"

namespace doris {

class PendingOutfileCleanup {
public:
    explicit PendingOutfileCleanup(std::shared_ptr<ResultBlockBufferBase> buffer)
            : _buffer(std::move(buffer)) {}

    void mark_ready() {
        std::lock_guard lock(_lock);
        _ready = true;
        _ready_condition.notify_all();
    }

    void run() {
        std::unique_lock lock(_lock);
        _ready_condition.wait(lock, [this] { return _ready; });
        if (_buffer == nullptr) {
            return;
        }
        _buffer->release_outfile_cleanup();
        _buffer.reset();
    }

private:
    std::mutex _lock;
    std::condition_variable _ready_condition;
    bool _ready = false;
    std::shared_ptr<ResultBlockBufferBase> _buffer;
};

class PendingOutfileCleanupRegistry {
public:
    std::mutex lock;
    std::unordered_set<std::shared_ptr<PendingOutfileCleanup>> cleanups;
};

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(result_buffer_block_count, MetricUnit::NOUNIT);

ResultBufferMgr::ResultBufferMgr()
        : _stop_background_threads_latch(1),
          _pending_outfile_cleanup_registry(std::make_shared<PendingOutfileCleanupRegistry>()) {
    // Each ResultBlockBufferBase has a limited queue size of 1024, it's not needed to count the
    // actual size of all ResultBlockBufferBase.
    REGISTER_HOOK_METRIC(result_buffer_block_count, [this]() {
        // std::lock_guard<std::mutex> l(_buffer_map_lock);
        return _buffer_map.size();
    });
}

void ResultBufferMgr::stop() {
    DEREGISTER_HOOK_METRIC(result_buffer_block_count);
    _stop_background_threads_latch.count_down();
    if (_clean_thread) {
        _clean_thread->join();
    }
    std::vector<TUniqueId> remaining_ids;
    {
        std::unique_lock<std::shared_mutex> wlock(_buffer_map_lock);
        // Closing the registration gate under the map lock keeps the shutdown snapshot complete.
        _stopping = true;
        remaining_ids.reserve(_buffer_map.size());
        for (const auto& item : _buffer_map) {
            remaining_ids.emplace_back(item.first);
        }
    }
    for (const auto& id : remaining_ids) {
        // Shutdown must retain cleanup ownership until rollback finishes; the lazy-release pool may
        // already be stopping and cannot provide that guarantee.
        cancel(id, Status::Cancelled("ResultBufferMgr is stopping"));
    }

    std::vector<std::shared_ptr<PendingOutfileCleanup>> pending_cleanups;
    {
        std::lock_guard lock(_pending_outfile_cleanup_registry->lock);
        pending_cleanups.assign(_pending_outfile_cleanup_registry->cleanups.begin(),
                                _pending_outfile_cleanup_registry->cleanups.end());
    }
    for (const auto& cleanup : pending_cleanups) {
        cleanup->run();
    }
    {
        std::lock_guard lock(_pending_outfile_cleanup_registry->lock);
        _pending_outfile_cleanup_registry->cleanups.clear();
    }
}

Status ResultBufferMgr::init() {
    RETURN_IF_ERROR(Thread::create(
            "ResultBufferMgr", "cancel_timeout_result", [this]() { this->cancel_thread(); },
            &_clean_thread));
    return Status::OK();
}

Status ResultBufferMgr::create_sender(const TUniqueId& unique_id, int buffer_size,
                                      std::shared_ptr<ResultBlockBufferBase>* sender,
                                      RuntimeState* state, bool arrow_flight,
                                      std::shared_ptr<arrow::Schema> schema) {
    {
        std::shared_lock<std::shared_mutex> rlock(_buffer_map_lock);
        if (_stopping) {
            return Status::Cancelled("ResultBufferMgr is stopping");
        }
        auto iter = _buffer_map.find(unique_id);

        if (_buffer_map.end() != iter) {
            return Status::InternalError("ResultBlockBuffer already exist, id={}",
                                         print_id(unique_id));
        }
    }

    std::shared_ptr<ResultBlockBufferBase> control_block = nullptr;

    if (arrow_flight) {
        control_block = std::make_shared<ArrowFlightResultBlockBuffer>(unique_id, state, schema,
                                                                       buffer_size);
    } else {
        control_block = std::make_shared<MySQLResultBlockBuffer>(unique_id, state, buffer_size);
    }

    {
        std::unique_lock<std::shared_mutex> wlock(_buffer_map_lock);
        if (_stopping) {
            return Status::Cancelled("ResultBufferMgr is stopping");
        }
        _buffer_map.insert(std::make_pair(unique_id, control_block));
        // ResultBlockBufferBase should destroy after max_timeout
        // for exceed max_timeout FE will return timeout to client
        // otherwise in some case may block all fragment handle threads
        // details see issue https://github.com/apache/doris/issues/16203
        // add extra 5s for avoid corner case
        int64_t max_timeout = time(nullptr) + state->execution_timeout() + 5;
        cancel_at_time(max_timeout, unique_id);
    }
    *sender = control_block;
    return Status::OK();
}

template <typename ResultBlockBufferType>
std::shared_ptr<ResultBlockBufferType> ResultBufferMgr::_find_control_block(
        const TUniqueId& unique_id) {
    std::shared_lock<std::shared_mutex> rlock(_buffer_map_lock);
    auto iter = _buffer_map.find(unique_id);

    if (_buffer_map.end() != iter) {
        return std::dynamic_pointer_cast<ResultBlockBufferType>(iter->second);
    }

    return {};
}

template <typename ResultBlockBufferType>
Status ResultBufferMgr::find_buffer(const TUniqueId& finst_id,
                                    std::shared_ptr<ResultBlockBufferType>& buffer) {
    buffer = _find_control_block<ResultBlockBufferType>(finst_id);
    return buffer == nullptr ? Status::InternalError(
                                       "no arrow schema for this query, maybe query has been "
                                       "canceled, finst_id={}",
                                       print_id(finst_id))
                             : Status::OK();
}

bool ResultBufferMgr::cancel(const TUniqueId& unique_id, const Status& reason, bool cleanup_async) {
    std::shared_ptr<ResultBlockBufferBase> buffer;
    std::shared_ptr<PendingOutfileCleanup> pending_cleanup;
    {
        std::unique_lock<std::shared_mutex> wlock(_buffer_map_lock);
        auto iter = _buffer_map.find(unique_id);
        if (iter == _buffer_map.end()) {
            return false;
        }
        buffer = std::move(iter->second);
        cleanup_async = cleanup_async && !_stopping;
        if (cleanup_async) {
            // Register ownership before removing the buffer so shutdown cannot miss a cleanup
            // that has not reached the lazy-release worker yet.
            pending_cleanup = std::make_shared<PendingOutfileCleanup>(buffer);
            std::lock_guard cleanup_lock(_pending_outfile_cleanup_registry->lock);
            _pending_outfile_cleanup_registry->cleanups.emplace(pending_cleanup);
        }
        _buffer_map.erase(iter);
    }
    buffer->cancel(reason, false);
    if (!cleanup_async) {
        buffer->release_outfile_cleanup();
        return true;
    }

    pending_cleanup->mark_ready();
    std::weak_ptr<PendingOutfileCleanupRegistry> weak_registry = _pending_outfile_cleanup_registry;
    auto cleanup = [pending_cleanup, weak_registry] {
        pending_cleanup->run();
        if (auto registry = weak_registry.lock()) {
            std::lock_guard lock(registry->lock);
            registry->cleanups.erase(pending_cleanup);
        }
    };
    ThreadPool* pool = ExecEnv::GetInstance()->lazy_release_obj_pool();
    if (cleanup_async && pool != nullptr) {
        Status submit_status = pool->submit_func(cleanup);
        if (!submit_status.ok()) {
            // submit_func may retain a runnable while returning an error; the guard keeps fallback
            // cleanup single-shot and ensures ownership is not silently discarded.
            cleanup();
        }
    } else {
        cleanup();
    }
    return true;
}

Status ResultBufferMgr::finish_outfile(const TUniqueId& unique_id, OutfileOperation operation) {
    auto buffer = _find_control_block<ResultBlockBufferBase>(unique_id);
    if (buffer == nullptr) {
        return Status::NotFound("outfile result buffer no longer exists");
    }
    return buffer->finish_outfile(operation);
}

void ResultBufferMgr::cancel_at_time(time_t cancel_time, const TUniqueId& unique_id) {
    std::lock_guard<std::mutex> l(_timeout_lock);
    auto iter = _timeout_map.find(cancel_time);

    if (_timeout_map.end() == iter) {
        _timeout_map.insert(
                std::pair<time_t, std::vector<TUniqueId>>(cancel_time, std::vector<TUniqueId>()));
        iter = _timeout_map.find(cancel_time);
    }

    iter->second.push_back(unique_id);
}

void ResultBufferMgr::cancel_thread() {
    LOG(INFO) << "result buffer manager cancel thread begin.";

    do {
        // get query
        std::vector<TUniqueId> query_to_cancel;
        time_t now_time = time(nullptr);
        {
            std::lock_guard<std::mutex> l(_timeout_lock);
            auto end = _timeout_map.upper_bound(now_time + 1);

            for (auto iter = _timeout_map.begin(); iter != end; ++iter) {
                for (const auto& id : iter->second) {
                    query_to_cancel.push_back(id);
                }
            }

            _timeout_map.erase(_timeout_map.begin(), end);
        }

        // cancel query
        for (const auto& id : query_to_cancel) {
            cancel(id,
                   Status::Cancelled("Clean up expired ResultBlockBuffer, queryId: {}",
                                     print_id(id)),
                   true);
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(1)));

    LOG(INFO) << "result buffer manager cancel thread finish.";
}

template Status ResultBufferMgr::find_buffer(
        const TUniqueId& finst_id, std::shared_ptr<doris::ArrowFlightResultBlockBuffer>& buffer);
template Status ResultBufferMgr::find_buffer(
        const TUniqueId& finst_id,
        std::shared_ptr<doris::ResultBlockBuffer<doris::GetResultBatchCtx>>& buffer);

} // namespace doris
