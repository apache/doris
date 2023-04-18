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

#include "vec/common/allocator.h"

// Allocator is used by too many files. For compilation speed, put dependencies in `.cpp` as much as possible.
#include "runtime/thread_context.h"
#include "util/mem_info.h"

template <bool clear_memory_, bool mmap_populate>
void Allocator<clear_memory_, mmap_populate>::sys_memory_check(size_t size) const {
    if (doris::MemTrackerLimiter::sys_mem_exceed_limit_check(size)) {
        // Only thread attach query, and has not completely waited for thread_wait_gc_max_milliseconds,
        // will wait for gc, asynchronous cancel or throw bad::alloc.
        // Otherwise, if the external catch, directly throw bad::alloc.
        if (doris::thread_context()->thread_mem_tracker_mgr->is_attach_query() &&
            doris::thread_context()->thread_mem_tracker_mgr->wait_gc()) {
            int64_t wait_milliseconds = doris::config::thread_wait_gc_max_milliseconds;
            while (wait_milliseconds > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if (!doris::MemTrackerLimiter::sys_mem_exceed_limit_check(size)) {
                    doris::MemInfo::refresh_interval_memory_growth += size;
                    break;
                }
                wait_milliseconds -= 100;
            }
            if (wait_milliseconds <= 0) {
                // Make sure to completely wait thread_wait_gc_max_milliseconds only once.
                doris::thread_context()->thread_mem_tracker_mgr->disable_wait_gc();
                auto err_msg = fmt::format(
                        "Allocator sys memory check failed: Cannot alloc:{}, consuming "
                        "tracker:<{}>, exec node:<{}>, {}.",
                        size, doris::thread_context()->thread_mem_tracker()->label(),
                        doris::thread_context()->thread_mem_tracker_mgr->last_consumer_tracker(),
                        doris::MemTrackerLimiter::process_limit_exceeded_errmsg_str(size));
                // If the external catch, throw bad::alloc first, let the query actively cancel. Otherwise asynchronous cancel.
                if (!doris::enable_thread_catch_bad_alloc) {
                    doris::thread_context()->thread_mem_tracker_mgr->cancel_fragment(err_msg);
                } else {
                    doris::thread_context()->thread_mem_tracker_mgr->save_exceed_mem_limit_msg(
                            err_msg);
                    throw std::bad_alloc {};
                }
            }
        } else if (doris::enable_thread_catch_bad_alloc) {
            auto err_msg = fmt::format(
                    "Allocator sys memory check failed: Cannot alloc:{}, consuming tracker:<{}>, "
                    "exec node:<{}>, {}.",
                    size, doris::thread_context()->thread_mem_tracker()->label(),
                    doris::thread_context()->thread_mem_tracker_mgr->last_consumer_tracker(),
                    doris::MemTrackerLimiter::process_limit_exceeded_errmsg_str(size));
            doris::thread_context()->thread_mem_tracker_mgr->save_exceed_mem_limit_msg(err_msg);
            throw std::bad_alloc {};
        }
    }
}

template <bool clear_memory_, bool mmap_populate>
void Allocator<clear_memory_, mmap_populate>::memory_tracker_check(size_t size) const {
    auto st = doris::thread_context()->thread_mem_tracker()->check_limit(size);
    if (!st) {
        doris::thread_context()->thread_mem_tracker_mgr->disable_wait_gc();
        auto err_msg =
                doris::thread_context()->thread_mem_tracker()->query_tracker_limit_exceeded_str(
                        st.to_string(),
                        doris::thread_context()->thread_mem_tracker_mgr->last_consumer_tracker(),
                        "Allocator mem tracker check failed");
        // If the external catch, throw bad::alloc first, let the query actively cancel. Otherwise asynchronous cancel.
        if (!doris::enable_thread_catch_bad_alloc) {
            doris::thread_context()->thread_mem_tracker_mgr->cancel_fragment(err_msg);
            doris::thread_context()->thread_mem_tracker()->print_log_usage(err_msg);
        } else {
            doris::thread_context()->thread_mem_tracker_mgr->save_exceed_mem_limit_msg(err_msg);
            throw std::bad_alloc {};
        }
    }
}

template <bool clear_memory_, bool mmap_populate>
void Allocator<clear_memory_, mmap_populate>::memory_check(size_t size) const {
    sys_memory_check(size);
    memory_tracker_check(size);
}

template <bool clear_memory_, bool mmap_populate>
void Allocator<clear_memory_, mmap_populate>::consume_memory(size_t size) const {
    CONSUME_THREAD_MEM_TRACKER(size);
}

template <bool clear_memory_, bool mmap_populate>
void Allocator<clear_memory_, mmap_populate>::release_memory(size_t size) const {
    RELEASE_THREAD_MEM_TRACKER(size);
}

template <bool clear_memory_, bool mmap_populate>
void Allocator<clear_memory_, mmap_populate>::throw_bad_alloc(const std::string& err) const {
    LOG(WARNING) << err;
    if (!doris::enable_thread_catch_bad_alloc)
        doris::MemTrackerLimiter::print_log_process_usage(err);
    throw std::bad_alloc {};
}

template class Allocator<true, true>;
template class Allocator<true, false>;
template class Allocator<false, true>;
template class Allocator<false, false>;
