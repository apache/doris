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
#include <random>
#include <thread>

// Allocator is used by too many files. For compilation speed, put dependencies in `.cpp` as much as possible.
#include "common/compiler_util.h"
#include "common/status.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/jemalloc_control.h"
#include "runtime/memory/memory_reclamation.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/process_profile.h"
#include "runtime/thread_context.h"
#include "util/mem_info.h"
#include "util/pretty_printer.h"
#include "util/stack_util.h"
#include "util/uid_util.h"

namespace doris {
std::unordered_map<void*, size_t> RecordSizeMemoryAllocator::_allocated_sizes;
std::mutex RecordSizeMemoryAllocator::_mutex;

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
bool Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::sys_memory_exceed(size_t size,
                                                             std::string* err_msg) const {
#ifdef BE_TEST
    if (!doris::pthread_context_ptr_init) {
        return false;
    }
#endif
    if (UNLIKELY(doris::thread_context()->thread_mem_tracker_mgr->skip_memory_check != 0)) {
        return false;
    }

    if (doris::GlobalMemoryArbitrator::is_exceed_hard_mem_limit(size)) {
        auto* thread_mem_ctx = doris::thread_context()->thread_mem_tracker_mgr.get();
        // Only thread attach task, and has not completely waited for thread_wait_gc_max_milliseconds,
        // will wait for gc. otherwise, if the outside will catch the exception, throwing an exception.
        *err_msg += fmt::format(
                "Allocator sys memory check failed: Cannot alloc:{}, consuming "
                "tracker:<{}>, peak used {}, current used {}, reserved {}, exec node:<{}>, {}.",
                doris::PrettyPrinter::print_bytes(size),
                thread_mem_ctx->limiter_mem_tracker()->label(),
                doris::PrettyPrinter::print_bytes(
                        thread_mem_ctx->limiter_mem_tracker()->peak_consumption()),
                doris::PrettyPrinter::print_bytes(
                        thread_mem_ctx->limiter_mem_tracker()->consumption()),
                doris::PrettyPrinter::print_bytes(
                        thread_mem_ctx->limiter_mem_tracker()->reserved_consumption()),
                thread_mem_ctx->last_consumer_tracker_label(),
                doris::GlobalMemoryArbitrator::process_mem_log_str());

        if (doris::config::stacktrace_in_alloc_large_memory_bytes > 0 &&
            size > doris::config::stacktrace_in_alloc_large_memory_bytes) {
            *err_msg += "\nAlloc Stacktrace:\n" + doris::get_stack_trace();
        }
        return true;
    }
    return false;
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::alloc_fault_probability() const {
#ifdef BE_TEST
    if (!doris::pthread_context_ptr_init) {
        return;
    }
#endif
    if (UNLIKELY(doris::thread_context()->thread_mem_tracker_mgr->skip_memory_check != 0)) {
        return;
    }

    if (UNLIKELY(doris::config::mem_alloc_fault_probability > 0.0)) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::bernoulli_distribution fault(doris::config::mem_alloc_fault_probability);
        if (fault(gen)) {
            const std::string injection_err_msg = fmt::format(
                    "[MemAllocInjectFault] Task {} alloc memory failed due to fault "
                    "injection.",
                    doris::thread_context()
                            ->thread_mem_tracker_mgr->limiter_mem_tracker()
                            ->label());
            // Print stack trace for debug.
            [[maybe_unused]] auto stack_trace_st =
                    doris::Status::Error<doris::ErrorCode::MEM_ALLOC_FAILED, true>(
                            injection_err_msg);
            if (!doris::config::enable_stacktrace) {
                LOG(INFO) << stack_trace_st.to_string();
            }
            if (doris::enable_thread_catch_bad_alloc) {
                throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, injection_err_msg);
            }
        }
    }
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::sys_memory_check(size_t size) const {
    std::string err_msg;
    if (sys_memory_exceed(size, &err_msg)) {
        auto* thread_mem_ctx = doris::thread_context()->thread_mem_tracker_mgr.get();
        if (thread_mem_ctx->wait_gc()) {
            int64_t wait_milliseconds = 0;
            LOG(INFO) << fmt::format(
                    "Task:{} waiting for enough memory in thread id:{}, maximum {}ms, {}.",
                    thread_mem_ctx->limiter_mem_tracker()->label(),
                    doris::ThreadContext::get_thread_id(),
                    doris::config::thread_wait_gc_max_milliseconds, err_msg);

            // only task thread exceeded memory limit for the first time and wait_gc is true.
            // TODO, in the future, try to free memory and waiting for memory to be freed in pipeline scheduling.
            doris::JemallocControl::je_thread_tcache_flush();
            doris::MemoryReclamation::je_purge_dirty_pages();

            if (!doris::config::disable_memory_gc) {
                // Allocator is not aware of whether the query is canceled.
                // even if the query is canceled, it will not end early.
                // this may cause query to be cancelled more slowly, but it is not a major problem.
                while (wait_milliseconds < doris::config::thread_wait_gc_max_milliseconds) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    if (!doris::GlobalMemoryArbitrator::is_exceed_hard_mem_limit(size)) {
                        doris::GlobalMemoryArbitrator::refresh_interval_memory_growth += size;
                        break;
                    }
                    wait_milliseconds += 100;
                }
            } else {
                wait_milliseconds = doris::config::thread_wait_gc_max_milliseconds;
            }

            // If true, it means that after waiting for a while, there is still not enough memory,
            // and try to throw an exception.
            // else, enough memory is available, the task continues execute.
            if (wait_milliseconds >= doris::config::thread_wait_gc_max_milliseconds) {
                // Make sure to completely wait thread_wait_gc_max_milliseconds only once.
                thread_mem_ctx->disable_wait_gc();
                doris::ProcessProfile::instance()->memory_profile()->print_log_process_usage();

                // If the outside will catch the exception, after throwing an exception,
                // the task will actively cancel itself.
                if (doris::enable_thread_catch_bad_alloc) {
                    LOG(INFO) << fmt::format(
                            "Task:{} sys memory check failed, throw exception, after waiting for "
                            "memory {}ms, {}.",
                            thread_mem_ctx->limiter_mem_tracker()->label(), wait_milliseconds,
                            err_msg);
                    throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
                } else {
                    LOG(INFO) << fmt::format(
                            "Task:{} sys memory check failed, will continue to execute, cannot "
                            "throw exception, after waiting for memory {}ms, {}.",
                            thread_mem_ctx->limiter_mem_tracker()->label(), wait_milliseconds,
                            err_msg);
                }
            }
        } else if (doris::enable_thread_catch_bad_alloc) {
            LOG(INFO) << fmt::format("{}, throw exception.", err_msg);
            throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
        } else {
            LOG(INFO) << fmt::format("{}, no throw exception.", err_msg);
        }
    }
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
bool Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::memory_tracker_exceed(size_t size,
                                                                 std::string* err_msg) const {
#ifdef BE_TEST
    if (!doris::pthread_context_ptr_init) {
        return false;
    }
#endif
    if (UNLIKELY(doris::thread_context()->thread_mem_tracker_mgr->skip_memory_check != 0)) {
        return false;
    }

    auto st = doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->check_limit(
            size);
    if (!st) {
        *err_msg += fmt::format("Allocator mem tracker check failed, {}", st.to_string());
        doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->print_log_usage(
                *err_msg);
        return true;
    }
    return false;
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::memory_tracker_check(size_t size) const {
    std::string err_msg;
    if (memory_tracker_exceed(size, &err_msg)) {
        doris::thread_context()->thread_mem_tracker_mgr->disable_wait_gc();
        // If the outside will catch the exception, after throwing an exception,
        // the task will actively cancel itself.
        if (doris::enable_thread_catch_bad_alloc) {
            LOG(INFO) << fmt::format("{}, throw exception.", err_msg);
            throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
        } else {
            LOG(INFO) << fmt::format("{}, will continue to execute, no throw exception.", err_msg);
        }
    }
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::memory_check(size_t size) const {
    if (check_and_tracking_memory) {
        alloc_fault_probability();
        sys_memory_check(size);
        memory_tracker_check(size);
    }
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::consume_memory(size_t size) const {
    if (check_and_tracking_memory) {
        CONSUME_THREAD_MEM_TRACKER(size);
    }
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::release_memory(size_t size) const {
    if (check_and_tracking_memory) {
        RELEASE_THREAD_MEM_TRACKER(size);
    }
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::throw_bad_alloc(const std::string& err) const {
    LOG(WARNING) << err
                 << fmt::format("{}, Stacktrace: {}",
                                doris::GlobalMemoryArbitrator::process_mem_log_str(),
                                doris::get_stack_trace());
    doris::ProcessProfile::instance()->memory_profile()->print_log_process_usage();
    throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::add_address_sanitizers(void* buf, size_t size) const {
    if (!doris::config::crash_in_memory_tracker_inaccurate) {
        return;
    }
    doris::thread_context()->thread_mem_tracker_mgr->limiter_mem_tracker()->add_address_sanitizers(
            buf, size);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::remove_address_sanitizers(void* buf, size_t size) const {
    if (!doris::config::crash_in_memory_tracker_inaccurate) {
        return;
    }
    doris::thread_context()
            ->thread_mem_tracker_mgr->limiter_mem_tracker()
            ->remove_address_sanitizers(buf, size);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void* Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
                check_and_tracking_memory>::alloc(size_t size, size_t alignment) {
    memory_check(size);
    // consume memory in tracker before alloc, similar to early declaration.
    consume_memory(size);
    void* buf;
    size_t record_size = size;

    if (use_mmap && size >= doris::config::mmap_threshold) {
        if (alignment > MMAP_MIN_ALIGNMENT) {
            throw doris::Exception(
                    doris::ErrorCode::INVALID_ARGUMENT,
                    "Too large alignment {}: more than page size when allocating {}.", alignment,
                    size);
        }

        buf = mmap(nullptr, size, PROT_READ | PROT_WRITE, mmap_flags, -1, 0);
        if (MAP_FAILED == buf) {
            release_memory(size);
            throw_bad_alloc(fmt::format("Allocator: Cannot mmap {}.", size));
        }
        if constexpr (MemoryAllocator::need_record_actual_size()) {
            record_size = MemoryAllocator::allocated_size(buf);
        }

        /// No need for zero-fill, because mmap guarantees it.
    } else {
        if (alignment <= MALLOC_MIN_ALIGNMENT) {
            if constexpr (clear_memory) {
                buf = MemoryAllocator::calloc(size, 1);
            } else {
                buf = MemoryAllocator::malloc(size);
            }

            if (nullptr == buf) {
                release_memory(size);
                throw_bad_alloc(fmt::format("Allocator: Cannot malloc {}.", size));
            }
            if constexpr (MemoryAllocator::need_record_actual_size()) {
                record_size = MemoryAllocator::allocated_size(buf);
            }
            add_address_sanitizers(buf, record_size);
        } else {
            buf = nullptr;
            int res = MemoryAllocator::posix_memalign(&buf, alignment, size);

            if (0 != res) {
                release_memory(size);
                throw_bad_alloc(fmt::format("Cannot allocate memory (posix_memalign) {}.", size));
            }

            if constexpr (clear_memory) {
                memset(buf, 0, size);
            }

            if constexpr (MemoryAllocator::need_record_actual_size()) {
                record_size = MemoryAllocator::allocated_size(buf);
            }
            add_address_sanitizers(buf, record_size);
        }
    }
    if constexpr (MemoryAllocator::need_record_actual_size()) {
        consume_memory(record_size - size);
    }
    return buf;
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
               check_and_tracking_memory>::free(void* buf, size_t size) {
    if (use_mmap && size >= doris::config::mmap_threshold) {
        if (0 != munmap(buf, size)) {
            throw_bad_alloc(fmt::format("Allocator: Cannot munmap {}.", size));
        }
    } else {
        remove_address_sanitizers(buf, size);
        MemoryAllocator::free(buf);
    }
    release_memory(size);
}

template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
void* Allocator<clear_memory_, mmap_populate, use_mmap, MemoryAllocator,
                check_and_tracking_memory>::realloc(void* buf, size_t old_size, size_t new_size,
                                                    size_t alignment) {
    if (old_size == new_size) {
        /// nothing to do.
        /// BTW, it's not possible to change alignment while doing realloc.
        return buf;
    }
    memory_check(new_size);
    // Realloc can do 2 possible things:
    // - expand existing memory region
    // - allocate new memory block and free the old one
    // Because we don't know which option will be picked we need to make sure there is enough
    // memory for all options.
    consume_memory(new_size);

    if (!use_mmap ||
        (old_size < doris::config::mmap_threshold && new_size < doris::config::mmap_threshold &&
         alignment <= MALLOC_MIN_ALIGNMENT)) {
        remove_address_sanitizers(buf, old_size);
        /// Resize malloc'd memory region with no special alignment requirement.
        void* new_buf = MemoryAllocator::realloc(buf, new_size);
        if (nullptr == new_buf) {
            release_memory(new_size);
            throw_bad_alloc(
                    fmt::format("Allocator: Cannot realloc from {} to {}.", old_size, new_size));
        }
        // usually, buf addr = new_buf addr, asan maybe not equal.
        add_address_sanitizers(new_buf, new_size);

        buf = new_buf;
        release_memory(old_size);
        if constexpr (clear_memory) {
            if (new_size > old_size) {
                memset(reinterpret_cast<char*>(buf) + old_size, 0, new_size - old_size);
            }
        }
    } else if (old_size >= doris::config::mmap_threshold &&
               new_size >= doris::config::mmap_threshold) {
        /// Resize mmap'd memory region.
        // On apple and freebsd self-implemented mremap used (common/mremap.h)
        buf = clickhouse_mremap(buf, old_size, new_size, MREMAP_MAYMOVE, PROT_READ | PROT_WRITE,
                                mmap_flags, -1, 0);
        if (MAP_FAILED == buf) {
            release_memory(new_size);
            throw_bad_alloc(fmt::format("Allocator: Cannot mremap memory chunk from {} to {}.",
                                        old_size, new_size));
        }
        release_memory(old_size);

        /// No need for zero-fill, because mmap guarantees it.

        if constexpr (mmap_populate) {
            // MAP_POPULATE seems have no effect for mremap as for mmap,
            // Clear enlarged memory range explicitly to pre-fault the pages
            if (new_size > old_size) {
                memset(reinterpret_cast<char*>(buf) + old_size, 0, new_size - old_size);
            }
        }
    } else {
        // Big allocs that requires a copy.
        void* new_buf = alloc(new_size, alignment);
        memcpy(new_buf, buf, std::min(old_size, new_size));
        add_address_sanitizers(new_buf, new_size);
        remove_address_sanitizers(buf, old_size);
        free(buf, old_size);
        buf = new_buf;
        release_memory(old_size);
    }

    return buf;
}

template class Allocator<true, true, true, DefaultMemoryAllocator, true>;
template class Allocator<true, true, false, DefaultMemoryAllocator, true>;
template class Allocator<true, false, true, DefaultMemoryAllocator, true>;
template class Allocator<true, false, false, DefaultMemoryAllocator, true>;
template class Allocator<false, true, true, DefaultMemoryAllocator, true>;
template class Allocator<false, true, false, DefaultMemoryAllocator, true>;
template class Allocator<false, false, true, DefaultMemoryAllocator, true>;
template class Allocator<false, false, false, DefaultMemoryAllocator, true>;
template class Allocator<true, true, true, DefaultMemoryAllocator, false>;
template class Allocator<true, true, false, DefaultMemoryAllocator, false>;
template class Allocator<true, false, true, DefaultMemoryAllocator, false>;
template class Allocator<true, false, false, DefaultMemoryAllocator, false>;
template class Allocator<false, true, true, DefaultMemoryAllocator, false>;
template class Allocator<false, true, false, DefaultMemoryAllocator, false>;
template class Allocator<false, false, true, DefaultMemoryAllocator, false>;
template class Allocator<false, false, false, DefaultMemoryAllocator, false>;

/** It would be better to put these Memory Allocators where they are used, such as in the orc memory pool and arrow memory pool.
  * But currently allocators use templates in .cpp instead of all in .h, so they can only be placed here.
  */
template class Allocator<true, true, true, ORCMemoryAllocator, true>;
template class Allocator<true, true, false, ORCMemoryAllocator, true>;
template class Allocator<true, false, true, ORCMemoryAllocator, true>;
template class Allocator<true, false, false, ORCMemoryAllocator, true>;
template class Allocator<false, true, true, ORCMemoryAllocator, true>;
template class Allocator<false, true, false, ORCMemoryAllocator, true>;
template class Allocator<false, false, true, ORCMemoryAllocator, true>;
template class Allocator<false, false, false, ORCMemoryAllocator, true>;
template class Allocator<true, true, true, ORCMemoryAllocator, false>;
template class Allocator<true, true, false, ORCMemoryAllocator, false>;
template class Allocator<true, false, true, ORCMemoryAllocator, false>;
template class Allocator<true, false, false, ORCMemoryAllocator, false>;
template class Allocator<false, true, true, ORCMemoryAllocator, false>;
template class Allocator<false, true, false, ORCMemoryAllocator, false>;
template class Allocator<false, false, true, ORCMemoryAllocator, false>;
template class Allocator<false, false, false, ORCMemoryAllocator, false>;

template class Allocator<true, true, true, RecordSizeMemoryAllocator, true>;
template class Allocator<true, true, false, RecordSizeMemoryAllocator, true>;
template class Allocator<true, false, true, RecordSizeMemoryAllocator, true>;
template class Allocator<true, false, false, RecordSizeMemoryAllocator, true>;
template class Allocator<false, true, true, RecordSizeMemoryAllocator, true>;
template class Allocator<false, true, false, RecordSizeMemoryAllocator, true>;
template class Allocator<false, false, true, RecordSizeMemoryAllocator, true>;
template class Allocator<false, false, false, RecordSizeMemoryAllocator, true>;
template class Allocator<true, true, true, RecordSizeMemoryAllocator, false>;
template class Allocator<true, true, false, RecordSizeMemoryAllocator, false>;
template class Allocator<true, false, true, RecordSizeMemoryAllocator, false>;
template class Allocator<true, false, false, RecordSizeMemoryAllocator, false>;
template class Allocator<false, true, true, RecordSizeMemoryAllocator, false>;
template class Allocator<false, true, false, RecordSizeMemoryAllocator, false>;
template class Allocator<false, false, true, RecordSizeMemoryAllocator, false>;
template class Allocator<false, false, false, RecordSizeMemoryAllocator, false>;

} // namespace doris