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

#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/types.pb.h"
#include "runtime/buffer_control_block.h"
#include "runtime/raw_value.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(result_buffer_block_count, MetricUnit::NOUNIT);

//std::size_t hash_value(const TUniqueId& fragment_id) {
//    uint32_t value = RawValue::get_hash_value(&fragment_id.lo, TypeDescriptor(TYPE_BIGINT), 0);
//    value = RawValue::get_hash_value(&fragment_id.hi, TypeDescriptor(TYPE_BIGINT), value);
//    return value;
//}

ResultBufferMgr::ResultBufferMgr() : _stop_background_threads_latch(1) {
    // Each BufferControlBlock has a limited queue size of 1024, it's not needed to count the
    // actual size of all BufferControlBlock.
    REGISTER_HOOK_METRIC(result_buffer_block_count, [this]() {
        std::lock_guard<std::mutex> l(_lock);
        return _buffer_map.size();
    });
}

ResultBufferMgr::~ResultBufferMgr() {
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
                                      std::shared_ptr<BufferControlBlock>* sender) {
    *sender = find_control_block(query_id);
    if (*sender != nullptr) {
        LOG(WARNING) << "already have buffer control block for this instance " << query_id;
        return Status::OK();
    }

    std::shared_ptr<BufferControlBlock> control_block(
            new BufferControlBlock(query_id, buffer_size));
    {
        std::lock_guard<std::mutex> l(_lock);
        _buffer_map.insert(std::make_pair(query_id, control_block));
    }
    *sender = control_block;
    return Status::OK();
}

std::shared_ptr<BufferControlBlock> ResultBufferMgr::find_control_block(const TUniqueId& query_id) {
    // TODO(zhaochun): this lock can be bottleneck?
    std::lock_guard<std::mutex> l(_lock);
    BufferMap::iterator iter = _buffer_map.find(query_id);

    if (_buffer_map.end() != iter) {
        return iter->second;
    }

    return std::shared_ptr<BufferControlBlock>();
}

Status ResultBufferMgr::fetch_data(const TUniqueId& query_id, TFetchDataResult* result) {
    std::shared_ptr<BufferControlBlock> cb = find_control_block(query_id);

    if (nullptr == cb) {
        // the sender tear down its buffer block
        return Status::InternalError("no result for this query.");
    }

    return cb->get_batch(result);
}

void ResultBufferMgr::fetch_data(const PUniqueId& finst_id, GetResultBatchCtx* ctx) {
    TUniqueId tid;
    tid.__set_hi(finst_id.hi());
    tid.__set_lo(finst_id.lo());
    std::shared_ptr<BufferControlBlock> cb = find_control_block(tid);
    if (cb == nullptr) {
        LOG(WARNING) << "no result for this query, id=" << tid;
        ctx->on_failure(Status::InternalError("no result for this query"));
        return;
    }
    cb->get_batch(ctx);
}

Status ResultBufferMgr::cancel(const TUniqueId& query_id) {
    std::lock_guard<std::mutex> l(_lock);
    BufferMap::iterator iter = _buffer_map.find(query_id);

    if (_buffer_map.end() != iter) {
        iter->second->cancel();
        _buffer_map.erase(iter);
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
            cancel(query_to_cancel[i]);
        }
    } while (!_stop_background_threads_latch.wait_for(MonoDelta::FromSeconds(1)));

    LOG(INFO) << "result buffer manager cancel thread finish.";
}

} // namespace doris
