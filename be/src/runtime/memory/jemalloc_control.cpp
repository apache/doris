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

#include "runtime/memory/jemalloc_control.h"

#include <atomic>
#include <condition_variable>

namespace doris {
#include "common/compile_check_begin.h"

std::mutex JemallocControl::je_purge_dirty_pages_lock;
std::atomic<bool> JemallocControl::je_purge_dirty_pages_notify {false};
std::mutex JemallocControl::je_reset_dirty_decay_lock;
std::atomic<bool> JemallocControl::je_enable_dirty_page {true};
std::condition_variable JemallocControl::je_reset_dirty_decay_cv;
std::atomic<bool> JemallocControl::je_reset_dirty_decay_notify {false};

std::atomic<int64_t> JemallocControl::je_cache_bytes_ = 0;
std::atomic<int64_t> JemallocControl::je_tcache_mem_ = 0;
std::atomic<int64_t> JemallocControl::je_metadata_mem_ = 0;
std::atomic<int64_t> JemallocControl::je_dirty_pages_mem_ = std::numeric_limits<int64_t>::min();
std::atomic<int64_t> JemallocControl::je_virtual_memory_used_ = 0;

void JemallocControl::refresh_allocator_mem() {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
#elif defined(USE_JEMALLOC)
    // jemalloc mallctl refer to : https://jemalloc.net/jemalloc.3.html
    // https://www.bookstack.cn/read/aliyun-rds-core/4a0cdf677f62feb3.md
    //  Check the Doris BE web page `http://ip:webserver_port/memory` to get the Jemalloc Profile.

    // 'epoch' is a special mallctl -- it updates the statistics. Without it, all
    // the following calls will return stale values. It increments and returns
    // the current epoch number, which might be useful to log as a sanity check.
    uint64_t epoch = 0;
    size_t sz = sizeof(epoch);
    jemallctl("epoch", &epoch, &sz, &epoch, sz);

    // Number of extents of the given type in this arena in the bucket corresponding to page size index.
    // Large size class starts at 16384, the extents have three sizes before 16384: 4096, 8192, and 12288, so + 3
    int64_t dirty_pages_bytes = 0;
    for (unsigned i = 0; i < get_jemallctl_value<unsigned>("arenas.nlextents") + 3; i++) {
        dirty_pages_bytes += get_je_all_arena_extents_metrics(i, "dirty_bytes");
    }
    je_dirty_pages_mem_.store(dirty_pages_bytes, std::memory_order_relaxed);
    je_tcache_mem_.store(get_je_all_arena_metrics("tcache_bytes"));

    // Doris uses Jemalloc as default Allocator, Jemalloc Cache consists of two parts:
    // - Thread Cache, cache a specified number of Pages in Thread Cache.
    // - Dirty Page, memory Page that can be reused in all Arenas.
    je_cache_bytes_.store(je_tcache_mem_ + dirty_pages_bytes, std::memory_order_relaxed);
    // Total number of bytes dedicated to metadata, which comprise base allocations used
    // for bootstrap-sensitive allocator metadata structures.
    je_metadata_mem_.store(get_jemallctl_value<int64_t>("stats.metadata"),
                           std::memory_order_relaxed);
    je_virtual_memory_used_.store(get_jemallctl_value<int64_t>("stats.mapped"),
                                  std::memory_order_relaxed);
#else
    je_cache_bytes_.store(get_tc_metrics("tcmalloc.pageheap_free_bytes") +
                                  get_tc_metrics("tcmalloc.central_cache_free_bytes") +
                                  get_tc_metrics("tcmalloc.transfer_cache_free_bytes") +
                                  get_tc_metrics("tcmalloc.thread_cache_free_bytes"),
                          std::memory_order_relaxed);
    je_virtual_memory_used_.store(get_tc_metrics("generic.total_physical_bytes") +
                                          get_tc_metrics("tcmalloc.pageheap_unmapped_bytes"),
                                  std::memory_order_relaxed);
#endif
}

#ifdef USE_JEMALLOC
void JemallocControl::action_jemallctl(const std::string& name) {
    try {
        int err = jemallctl(name.c_str(), nullptr, nullptr, nullptr, 0);
        if (err) {
            LOG(WARNING) << fmt::format("Failed, jemallctl action {}", name);
        }
    } catch (...) {
        LOG(WARNING) << fmt::format("Exception, jemallctl action {}", name);
    }
}

int64_t JemallocControl::get_je_all_arena_metrics(const std::string& name) {
    return get_jemallctl_value<int64_t>(
            fmt::format("stats.arenas.{}.{}", MALLCTL_ARENAS_ALL, name));
}

int64_t JemallocControl::get_je_all_arena_extents_metrics(int64_t page_size_index,
                                                          const std::string& extent_type) {
    return get_jemallctl_value<int64_t>(fmt::format(
            "stats.arenas.{}.extents.{}.{}", MALLCTL_ARENAS_ALL, page_size_index, extent_type));
}

void JemallocControl::je_purge_all_arena_dirty_pages() {
    // https://github.com/jemalloc/jemalloc/issues/2470
    // If there is a core dump here, it may cover up the real stack, if stack trace indicates heap corruption
    // (which led to invalid jemalloc metadata), like double free or use-after-free in the application.
    // Try sanitizers such as ASAN, or build jemalloc with --enable-debug to investigate further.
    action_jemallctl(fmt::format("arena.{}.purge", MALLCTL_ARENAS_ALL));
}

void JemallocControl::je_reset_all_arena_dirty_decay_ms(ssize_t dirty_decay_ms) {
    // Each time this interface is set, all currently unused dirty pages are considered
    // to have fully decayed, which causes immediate purging of all unused dirty pages unless
    // the decay time is set to -1
    //
    // NOTE: Using "arena.MALLCTL_ARENAS_ALL.dirty_decay_ms" to modify all arenas will fail or even crash,
    // which may be a bug.
    for (unsigned i = 0; i < get_jemallctl_value<unsigned>("arenas.narenas"); i++) {
        set_jemallctl_value<ssize_t>(fmt::format("arena.{}.dirty_decay_ms", i), dirty_decay_ms);
    }
}

void JemallocControl::je_decay_all_arena_dirty_pages() {
    // Trigger decay-based purging of unused dirty/muzzy pages for arena <i>, or for all arenas if <i> equals
    // MALLCTL_ARENAS_ALL. The proportion of unused dirty/muzzy pages to be purged depends on the
    // current time; see opt.dirty_decay_ms and opt.muzy_decay_ms for details.
    action_jemallctl(fmt::format("arena.{}.decay", MALLCTL_ARENAS_ALL));
}

void JemallocControl::je_thread_tcache_flush() {
    constexpr size_t TCACHE_LIMIT = (1ULL << 30); // 1G
    if (je_tcache_mem() > TCACHE_LIMIT) {
        action_jemallctl("thread.tcache.flush");
    }
}

#else
void JemallocControl::action_jemallctl(const std::string& name) {}
int64_t JemallocControl::get_je_all_arena_metrics(const std::string& name) {
    return 0;
}
int64_t JemallocControl::get_je_all_arena_extents_metrics(int64_t page_size_index,
                                                          const std::string& extent_type) {
    return 0;
}
void JemallocControl::je_purge_all_arena_dirty_pages() {}
void JemallocControl::je_reset_all_arena_dirty_decay_ms(ssize_t dirty_decay_ms) {}
void JemallocControl::je_decay_all_arena_dirty_pages() {}
void JemallocControl::je_thread_tcache_flush() {}
#endif

#include "common/compile_check_end.h"
} // namespace doris
