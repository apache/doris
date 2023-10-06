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
#include <stdint.h>

#include <chrono>
#include <thread>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <memory>
#include <ostream>
#include <utility>

#include "arrow/record_batch.h"
#include "arrow/type_fwd.h"
#include "runtime/buffer_control_block.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "util/thread.h"
#include "util/uid_util.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(result_buffer_block_count, MetricUnit::NOUNIT);

ResultBufferMgr::ResultBufferMgr() : _stop_background_threads_latch(1) {
    // Each BufferControlBlock has a limited queue size of 1024, it's not needed to count the
    // actual size of all BufferControlBlock.
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
}

Status ResultBufferMgr::init() {
    RETURN_IF_ERROR(Thread::create(
            "ResultBufferMgr", "cancel_timeout_result", [this]() { this->cancel_thread(); },
            &_clean_thread));
    return Status::OK();
}

Status ResultBufferMgr::create_sender(const TUniqueId& query_id, int buffer_size,
                                      std::shared_ptr<BufferControlBlock>* sender,
                                      bool enable_pipeline, int exec_timout) {
    *sender = find_control_block(query_id);
    if (*sender != nullptr) {
        LOG(WARNING) << "already have buffer control block for this instance " << query_id;
        return Status::OK();
    }

    std::shared_ptr<BufferControlBlock> control_block = nullptr;

    if (enable_pipeline) {
        control_block = std::make_shared<PipBufferControlBlock>(query_id, buffer_size);
    } else {
        control_block = std::make_shared<BufferControlBlock>(query_id, buffer_size);
    }

    {
        std::unique_lock<std::shared_mutex> wlock(_buffer_map_lock);
        _buffer_map.insert(std::make_pair(query_id, control_block));
        // BufferControlBlock should destroy after max_timeout
        // for exceed max_timeout FE will return timeout to client
        // otherwise in some case may block all fragment handle threads
        // details see issue https://github.com/apache/doris/issues/16203
        // add extra 5s for avoid corner case
        int64_t max_timeout = time(nullptr) + exec_timout + 5;
        static_cast<void>(cancel_at_time(max_timeout, query_id));
    }
    *sender = control_block;
    return Status::OK();
}

std::shared_ptr<BufferControlBlock> ResultBufferMgr::find_control_block(const TUniqueId& query_id) {
    std::shared_lock<std::shared_mutex> rlock(_buffer_map_lock);
    BufferMap::iterator iter = _buffer_map.find(query_id);

    if (_buffer_map.end() != iter) {
        return iter->second;
    }

    return std::shared_ptr<BufferControlBlock>();
}

void ResultBufferMgr::register_row_descriptor(const TUniqueId& query_id,
                                              const RowDescriptor& row_desc) {
    std::unique_lock<std::shared_mutex> wlock(_row_descriptor_map_lock);
    _row_descriptor_map.insert(std::make_pair(query_id, row_desc));
}

RowDescriptor ResultBufferMgr::find_row_descriptor(const TUniqueId& query_id) {
    std::shared_lock<std::shared_mutex> rlock(_row_descriptor_map_lock);
    RowDescriptorMap::iterator iter = _row_descriptor_map.find(query_id);

    if (_row_descriptor_map.end() != iter) {
        return iter->second;
    }

    return RowDescriptor();
}

void ResultBufferMgr::fetch_data(const PUniqueId& finst_id, GetResultBatchCtx* ctx) {
    TUniqueId tid;
    tid.__set_hi(finst_id.hi());
    tid.__set_lo(finst_id.lo());
    std::shared_ptr<BufferControlBlock> cb = find_control_block(tid);
    if (cb == nullptr) {
        LOG(WARNING) << "no result for this query, id=" << print_id(tid);
        ctx->on_failure(Status::InternalError("no result for this query"));
        return;
    }
    cb->get_batch(ctx);
}

Status ResultBufferMgr::fetch_arrow_data(const TUniqueId& finst_id,
                                         std::shared_ptr<arrow::RecordBatch>* result) {
    std::shared_ptr<BufferControlBlock> cb = find_control_block(finst_id);
    if (cb == nullptr) {
        LOG(WARNING) << "no result for this query, id=" << print_id(finst_id);
        return Status::InternalError("no result for this query");
    }
    RETURN_IF_ERROR(cb->get_arrow_batch(result));
    return Status::OK();
}

Status ResultBufferMgr::cancel(const TUniqueId& query_id) {
    {
        std::unique_lock<std::shared_mutex> wlock(_buffer_map_lock);
        BufferMap::iterator iter = _buffer_map.find(query_id);

        if (_buffer_map.end() != iter) {
            static_cast<void>(iter->second->cancel());
            _buffer_map.erase(iter);
        }
    }

    {
        std::unique_lock<std::shared_mutex> wlock(_row_descriptor_map_lock);
        RowDescriptorMap::iterator row_desc_iter = _row_descriptor_map.find(query_id);

        if (_row_descriptor_map.end() != row_desc_iter) {
            _row_descriptor_map.erase(row_desc_iter);
        }
    }

    return Status::OK();
}

Status ResultBufferMgr::cancel_at_time(time_t cancel_time, const TUniqueId& query_id) {
    std::lock_guard<std::mutex> l(_timeout_lock);
    TimeoutMap::iterator iter = _timeout_map.find(cancel_time);

    if (_timeout_map.end() == iter) {
        _timeout_map.insert(
                std::pair<time_t, std::vector<TUniqueId>>(cancel_time, std::vector<TUniqueId>()));
        iter = _timeout_map.find(cancel_time);
    }

    iter->second.push_back(query_id);
    return Status::OK();
}

void ResultBufferMgr::cancel_thread() {
    LOG(INFO) << "result buffer manager cancel thread begin.";

    do {
        // get query
        std::vector<TUniqueId> query_to_cancel;
        time_t now_time = time(nullptr);
        {
            std::lock_guard<std::mutex> l(_timeout_lock);
            TimeoutMap::iterator end = _timeout_map.upper_bound(now_time + 1);

            for (TimeoutMap::iterator iter = _timeout_map.begin(); iter != end; ++iter) {
                for (int i = 0; i < iter->second.size(); ++i) {
                    query_to_cancel.push_back(iter->second[i]);
                }
            }

            _timeout_map.erase(_timeout_map.begin(), end);
        }

        // cancel query
        for (int i = 0; i < query_to_cancel.size(); ++i) {
            static_cast<void>(cancel(query_to_cancel[i]));
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(1)));

    LOG(INFO) << "result buffer manager cancel thread finish.";
}

} // namespace doris
