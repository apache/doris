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

#include "runtime/thread_mem_tracker_mgr.h"

#include "runtime/mem_tracker_task_pool.h"
#include "service/backend_options.h"

namespace doris {

std::shared_ptr<MemTracker> ThreadMemTrackerMgr::default_mem_tracker() {
    ExecEnv* exec_env = ExecEnv::GetInstance();
    std::shared_ptr<MemTracker> process_tracker = exec_env->new_process_mem_tracker();
    if (process_tracker != nullptr) {
        return process_tracker;
    } else {
        return MemTracker::get_root_tracker();
    }
}

void ThreadMemTrackerMgr::attach_query(const std::string& query_id,
                                       const TUniqueId& fragment_instance_id) {
    DCHECK(query_id != "" && fragment_instance_id != TUniqueId());
    _query_id = query_id;
    _fragment_instance_id = fragment_instance_id;
    _consume_err_call_back = std::make_shared<ConsumeErrCallBackInfo>("Query", true, nullptr);
#ifdef BE_TEST
    if (ExecEnv::GetInstance()->task_pool_mem_tracker_registry() == nullptr) {
        return;
    }
#endif
    update_tracker(ExecEnv::GetInstance()->task_pool_mem_tracker_registry()->get_query_mem_tracker(
            query_id));
}

void ThreadMemTrackerMgr::detach() {
    update_tracker(default_mem_tracker());
    _query_id = "";
    _fragment_instance_id = TUniqueId();
    _consume_err_call_back = std::make_shared<ConsumeErrCallBackInfo>("", true, nullptr);
}

std::weak_ptr<MemTracker> ThreadMemTrackerMgr::update_tracker(
        std::weak_ptr<MemTracker> mem_tracker) {
    if (_untracked_mem != 0) {
        noncache_consume();
        _untracked_mem = 0;
    }
    DCHECK(!_mem_tracker.expired());
    DCHECK(!mem_tracker.expired());
    std::weak_ptr<MemTracker> old_mem_tracker = _mem_tracker.lock();
    _mem_tracker = mem_tracker;
    return old_mem_tracker;
}

std::shared_ptr<ConsumeErrCallBackInfo> ThreadMemTrackerMgr::update_consume_err_call_back(
        const std::string& action_name, bool cancel_task, ERRCALLBACK call_back_func) {
    std::shared_ptr<ConsumeErrCallBackInfo> old_consume_err_call_back = _consume_err_call_back;
    _consume_err_call_back =
            std::make_shared<ConsumeErrCallBackInfo>(action_name, cancel_task, call_back_func);
    return old_consume_err_call_back;
}

std::shared_ptr<ConsumeErrCallBackInfo> ThreadMemTrackerMgr::update_consume_err_call_back(
        std::shared_ptr<ConsumeErrCallBackInfo> consume_err_call_back) {
    std::shared_ptr<ConsumeErrCallBackInfo> old_consume_err_call_back = _consume_err_call_back;
    _consume_err_call_back = consume_err_call_back;
    return old_consume_err_call_back;
}

void ThreadMemTrackerMgr::exceeded_cancel_query() {
    if (_fragment_instance_id != TUniqueId() && ExecEnv::GetInstance()->initialized() &&
        ExecEnv::GetInstance()->fragment_mgr()->is_canceling(_fragment_instance_id).ok()) {
        std::string detail =
                " {} Memory exceed limit in TCMalloc Hook New, Backend: {}, QueryID: {}, "
                "FragmentID: {}, Used: {}, Limit: {}. You can change the limit by session variable "
                "exec_mem_limit.";
        ExecEnv::GetInstance()->fragment_mgr()->cancel(
                _fragment_instance_id, PPlanFragmentCancelReason::MEMORY_LIMIT_EXCEED,
                fmt::format(detail, _consume_err_call_back->action_name,
                            BackendOptions::get_localhost(), _query_id,
                            print_id(_fragment_instance_id),
                            std::to_string(_mem_tracker.lock()->consumption()),
                            std::to_string(_mem_tracker.lock()->limit())));
        _fragment_instance_id = TUniqueId(); // Make sure it will only be canceled once
    }
}

void ThreadMemTrackerMgr::exceeded(Status st, int64_t mem_usage) {
    DCHECK(st.is_mem_limit_exceeded());
    std::string detail = st.to_string() + ", in TCMalloc Hook New.";
    auto rst = _mem_tracker.lock()->mem_limit_exceeded(nullptr, detail, mem_usage);
    if (_consume_err_call_back->call_back_func != nullptr) {
        _consume_err_call_back->call_back_func();
    }
    if (_query_id != "") {
        std::shared_ptr<MemTracker> query_mem_tracker =
                ExecEnv::GetInstance()->task_pool_mem_tracker_registry()->get_query_mem_tracker(
                        _query_id);
        if (_consume_err_call_back->cancel_task == true ||
            (query_mem_tracker != nullptr && query_mem_tracker->limit_exceeded())) {
            exceeded_cancel_query();
        }
    }
    LOG(WARNING) << rst.to_string();
}

void ThreadMemTrackerMgr::noncache_consume() {
    _stop_mem_tracker = true;
    {
        // Ensure thread safety
        auto tracker = _mem_tracker.lock();
        // The first time get_root_tracker is called after the main thread starts, == nullptr.
        if (tracker) {
            Status st = _mem_tracker.lock()->try_consume(_untracked_mem);
            if (!st) {
                // The memory has been allocated, so when TryConsume fails, need to continue to complete
                // the consume to ensure the accuracy of the statistics.
                _mem_tracker.lock()->consume(_untracked_mem);
                exceeded(st, _untracked_mem);
            }
        }
    }
    _stop_mem_tracker = false;
}

void ThreadMemTrackerMgr::cache_consume(int64_t size) {
    if (_stop_mem_tracker == true) {
        return;
    }
    _untracked_mem += size;
    // When some threads `0 < _untracked_mem < _tracker_consume_cache_size`
    // and some threads `_untracked_mem <= -_tracker_consume_cache_size` trigger consumption(),
    // it will cause tracker->consumption to be temporarily less than 0.
    if (_untracked_mem >= _tracker_consume_cache_size ||
        _untracked_mem <= -_tracker_consume_cache_size) {
        noncache_consume();
        _untracked_mem = 0;
    }
}

} // namespace doris
