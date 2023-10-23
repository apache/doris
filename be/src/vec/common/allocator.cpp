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

#include <glog/logging.h>

#include <atomic>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <memory>
#include <new>
#include <thread>

// Allocator is used by too many files. For compilation speed, put dependencies in `.cpp` as much as possible.
#include "runtime/fragment_mgr.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"
#include "util/mem_info.h"
#include "util/stack_util.h"
#include "util/uid_util.h"

template <bool clear_memory_, bool mmap_populate, bool use_mmap>
void Allocator<clear_memory_, mmap_populate, use_mmap>::sys_memory_check(size_t size) const {
    if (doris::thread_context()->skip_memory_check) return;
    if (doris::MemTrackerLimiter::sys_mem_exceed_limit_check(size)) {
        // Only thread attach query, and has not completely waited for thread_wait_gc_max_milliseconds,
        // will wait for gc, asynchronous cancel or throw bad::alloc.
        // Otherwise, if the external catch, directly throw bad::alloc.
        auto err_msg = fmt::format(
                "Allocator sys memory check failed: Cannot alloc:{}, consuming "
                "tracker:<{}>, peak used {}, current used {}, exec node:<{}>, {}.",
                size, doris::thread_context()->thread_mem_tracker()->label(),
                doris::thread_context()->thread_mem_tracker()->peak_consumption(),
                doris::thread_context()->thread_mem_tracker()->consumption(),
                doris::thread_context()->thread_mem_tracker_mgr->last_consumer_tracker(),
                doris::MemTrackerLimiter::process_limit_exceeded_errmsg_str());
        if (size > 1024l * 1024 * 1024 && !doris::enable_thread_catch_bad_alloc &&
            !doris::config::disable_memory_gc) { // 1G
            err_msg += "\nAlloc Stacktrace:\n" + doris::get_stack_trace();
        }

        // TODO, Save the query context in the thread context, instead of finding whether the query id is canceled in fragment_mgr.
        if (doris::ExecEnv::GetInstance()->fragment_mgr()->query_is_canceled(
                    doris::thread_context()->task_id())) {
            if (doris::enable_thread_catch_bad_alloc) {
                throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
            }
            return;
        }
        if (!doris::config::disable_memory_gc &&
            doris::thread_context()->thread_mem_tracker_mgr->is_attach_query() &&
            doris::thread_context()->thread_mem_tracker_mgr->wait_gc()) {
            int64_t wait_milliseconds = 0;
            LOG(INFO) << fmt::format(
                    "Query:{} waiting for enough memory in thread id:{}, maximum {}ms, {}.",
                    print_id(doris::thread_context()->task_id()),
                    doris::thread_context()->get_thread_id(),
                    doris::config::thread_wait_gc_max_milliseconds, err_msg);
            while (wait_milliseconds < doris::config::thread_wait_gc_max_milliseconds) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if (!doris::MemTrackerLimiter::sys_mem_exceed_limit_check(size)) {
                    doris::MemInfo::refresh_interval_memory_growth += size;
                    break;
                }
                if (doris::ExecEnv::GetInstance()->fragment_mgr()->query_is_canceled(
                            doris::thread_context()->task_id())) {
                    if (doris::enable_thread_catch_bad_alloc) {
                        throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
                    }
                    return;
                }
                wait_milliseconds += 100;
            }
            if (wait_milliseconds >= doris::config::thread_wait_gc_max_milliseconds) {
                // Make sure to completely wait thread_wait_gc_max_milliseconds only once.
                doris::thread_context()->thread_mem_tracker_mgr->disable_wait_gc();
                doris::MemTrackerLimiter::print_log_process_usage();
                // If the external catch, throw bad::alloc first, let the query actively cancel. Otherwise asynchronous cancel.
                if (!doris::enable_thread_catch_bad_alloc) {
                    LOG(INFO) << fmt::format(
                            "Query:{} canceled asyn, after waiting for memory {}ms, {}.",
                            print_id(doris::thread_context()->task_id()), wait_milliseconds,
                            err_msg);
                    doris::thread_context()->thread_mem_tracker_mgr->cancel_fragment(err_msg);
                } else {
                    LOG(INFO) << fmt::format(
                            "Query:{} throw exception, after waiting for memory {}ms, {}.",
                            print_id(doris::thread_context()->task_id()), wait_milliseconds,
                            err_msg);
                    throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
                }
            }
            // else, enough memory is available, the query continues execute.
        } else if (doris::enable_thread_catch_bad_alloc) {
            LOG(INFO) << fmt::format("sys memory check failed, throw exception, {}.", err_msg);
            doris::MemTrackerLimiter::print_log_process_usage();
            throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
        } else {
            LOG(INFO) << fmt::format("sys memory check failed, no throw exception, {}.", err_msg);
        }
    }
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap>
void Allocator<clear_memory_, mmap_populate, use_mmap>::memory_tracker_check(size_t size) const {
    if (doris::thread_context()->skip_memory_check) return;
    auto st = doris::thread_context()->thread_mem_tracker()->check_limit(size);
    if (!st) {
        auto err_msg = fmt::format("Allocator mem tracker check failed, {}", st.to_string());
        doris::thread_context()->thread_mem_tracker()->print_log_usage(err_msg);
        // If the external catch, throw bad::alloc first, let the query actively cancel. Otherwise asynchronous cancel.
        if (doris::thread_context()->thread_mem_tracker_mgr->is_attach_query()) {
            doris::thread_context()->thread_mem_tracker_mgr->disable_wait_gc();
            if (!doris::enable_thread_catch_bad_alloc) {
                LOG(INFO) << fmt::format("query/load:{} canceled asyn, {}.",
                                         print_id(doris::thread_context()->task_id()), err_msg);
                doris::thread_context()->thread_mem_tracker_mgr->cancel_fragment(err_msg);
            } else {
                LOG(INFO) << fmt::format("query/load:{} throw exception, {}.",
                                         print_id(doris::thread_context()->task_id()), err_msg);
                throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
            }
        } else if (doris::enable_thread_catch_bad_alloc) {
            LOG(INFO) << fmt::format("memory tracker check failed, throw exception, {}.", err_msg);
            throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
        } else {
            LOG(INFO) << fmt::format("memory tracker check failed, no throw exception, {}.",
                                     err_msg);
        }
    }
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap>
void Allocator<clear_memory_, mmap_populate, use_mmap>::memory_check(size_t size) const {
    sys_memory_check(size);
    memory_tracker_check(size);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap>
void Allocator<clear_memory_, mmap_populate, use_mmap>::consume_memory(size_t size) const {
    CONSUME_THREAD_MEM_TRACKER(size);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap>
void Allocator<clear_memory_, mmap_populate, use_mmap>::release_memory(size_t size) const {
    RELEASE_THREAD_MEM_TRACKER(size);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap>
void Allocator<clear_memory_, mmap_populate, use_mmap>::throw_bad_alloc(
        const std::string& err) const {
    LOG(WARNING) << err
                 << fmt::format(
                            " os physical memory {}. process memory used {}, sys available memory "
                            "{}, Stacktrace: {}",
                            doris::PrettyPrinter::print(doris::MemInfo::physical_mem(),
                                                        doris::TUnit::BYTES),
                            doris::PerfCounters::get_vm_rss_str(),
                            doris::MemInfo::sys_mem_available_str(), doris::get_stack_trace());
    doris::MemTrackerLimiter::print_log_process_usage();
    throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap>
void* Allocator<clear_memory_, mmap_populate, use_mmap>::alloc(size_t size, size_t alignment) {
    return alloc_impl(size, alignment);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap>
void* Allocator<clear_memory_, mmap_populate, use_mmap>::realloc(void* buf, size_t old_size,
                                                                 size_t new_size,
                                                                 size_t alignment) {
    return realloc_impl(buf, old_size, new_size, alignment);
}

template class Allocator<true, true, true>;
template class Allocator<true, true, false>;
template class Allocator<true, false, true>;
template class Allocator<true, false, false>;
template class Allocator<false, true, true>;
template class Allocator<false, true, false>;
template class Allocator<false, false, true>;
template class Allocator<false, false, false>;
