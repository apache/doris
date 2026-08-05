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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/base/phdr_cache.cpp
// and modified by Doris

#include "common/phdr_cache.h"

#if defined(__clang__)
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

/// This code was based on the code by Fedor Korotkiy https://www.linkedin.com/in/fedor-korotkiy-659a1838/

#if defined(__linux__) && !defined(THREAD_SANITIZER) && !defined(USE_MUSL)
#define USE_PHDR_CACHE 1
#endif

/// Thread Sanitizer uses dl_iterate_phdr function on initialization and fails if we provide our own.
#ifdef USE_PHDR_CACHE

#if defined(__clang__)
#pragma clang diagnostic ignored "-Wreserved-id-macro"
#pragma clang diagnostic ignored "-Wunused-macros"
#endif

#define __msan_unpoison(X, Y) // NOLINT
#if defined(__clang__) && defined(__has_feature)
#if __has_feature(memory_sanitizer)
#undef __msan_unpoison
#include <sanitizer/msan_interface.h>
#endif
#endif

#include <dlfcn.h>
#include <link.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#if defined(USE_UNWIND) && USE_UNWIND
#ifndef UNW_LOCAL_ONLY
#define UNW_LOCAL_ONLY
#endif
#include <libunwind.h>
#endif

namespace {

// This is adapted from
// https://github.com/scylladb/seastar/blob/master/core/exception_hacks.hh
// https://github.com/scylladb/seastar/blob/master/core/exception_hacks.cc

constexpr size_t MAX_PHDR_CACHE_LOADED_OBJECTS = 4096;
constexpr size_t MAX_PHDR_CACHE_PROGRAM_HEADERS = 128;
constexpr size_t MAX_PHDR_CACHE_OBJECT_NAME = 4096;

using DLIterateFunction = int (*)(int (*callback)(dl_phdr_info* info, size_t size, void* data),
                                  void* data);

DLIterateFunction getOriginalDLIteratePHDR() {
    void* func = dlsym(RTLD_NEXT, "dl_iterate_phdr");
    if (!func) {
        throw std::runtime_error("Cannot find dl_iterate_phdr function with dlsym");
    }
    return reinterpret_cast<DLIterateFunction>(func);
}

struct RawPHDRCacheEntry {
    dl_phdr_info info {};
    std::array<ElfW(Phdr), MAX_PHDR_CACHE_PROGRAM_HEADERS> phdrs {};
    size_t phdr_count = 0;
    std::array<char, MAX_PHDR_CACHE_OBJECT_NAME> name {};
    uintptr_t address_begin = std::numeric_limits<uintptr_t>::max();
    uintptr_t address_end = 0;
};

struct RawPHDRCacheSnapshot {
    std::array<RawPHDRCacheEntry, MAX_PHDR_CACHE_LOADED_OBJECTS> entries {};
    size_t size = 0;
    bool overflow = false;
    bool phdr_truncated = false;
    bool name_truncated = false;
};

struct PHDRCacheEntry {
    dl_phdr_info info {};
    std::vector<ElfW(Phdr)> phdrs;
    std::string name;
    uintptr_t address_begin = std::numeric_limits<uintptr_t>::max();
    uintptr_t address_end = 0;

    bool contains(uintptr_t ip) const {
        return ip == 0 || (address_begin <= ip && ip < address_end);
    }
};

using PHDRCache = std::vector<PHDRCacheEntry>;
std::atomic<PHDRCache*> phdr_cache {};
// This flag is flipped inside the stack-trace signal handler. Force a static TLS access model so
// reading it from our dl_iterate_phdr interposer does not call into the dynamic loader's TLS path.
__thread bool use_phdr_cache __attribute__((tls_model("initial-exec"))) = false;

uintptr_t saturated_segment_end(uintptr_t begin, uintptr_t size) {
    const uintptr_t max_address = std::numeric_limits<uintptr_t>::max();
    return begin > max_address - size ? max_address : begin + size;
}

void copy_object_name(const char* source, RawPHDRCacheEntry* entry,
                      RawPHDRCacheSnapshot* snapshot) {
    if (source == nullptr) {
        entry->name[0] = '\0';
        return;
    }

    size_t length = 0;
    while (length + 1 < entry->name.size() && source[length] != '\0') {
        entry->name[length] = source[length];
        ++length;
    }
    entry->name[length] = '\0';
    if (source[length] != '\0') {
        snapshot->name_truncated = true;
    }
}

int collectPHDRCacheEntry(dl_phdr_info* info, size_t /*size*/, void* data) {
    auto* snapshot = reinterpret_cast<RawPHDRCacheSnapshot*>(data);
    if (snapshot->size >= snapshot->entries.size()) {
        snapshot->overflow = true;
        return 0;
    }

    auto& entry = snapshot->entries[snapshot->size++];
    entry.info = *info;
    copy_object_name(info->dlpi_name, &entry, snapshot);

    const size_t phdr_count = std::min<size_t>(info->dlpi_phnum, entry.phdrs.size());
    if (phdr_count < info->dlpi_phnum) {
        snapshot->phdr_truncated = true;
    }
    entry.phdr_count = phdr_count;
    entry.info.dlpi_phnum = static_cast<ElfW(Half)>(phdr_count);
    entry.info.dlpi_name = nullptr;
    entry.info.dlpi_phdr = nullptr;

    if (info->dlpi_phdr == nullptr) {
        return 0;
    }
    std::memcpy(entry.phdrs.data(), info->dlpi_phdr, phdr_count * sizeof(ElfW(Phdr)));

    for (size_t i = 0; i < phdr_count; ++i) {
        const auto& phdr = entry.phdrs[i];
        if (phdr.p_type != PT_LOAD || phdr.p_memsz == 0) {
            continue;
        }
        const auto begin = static_cast<uintptr_t>(info->dlpi_addr + phdr.p_vaddr);
        const auto end = saturated_segment_end(begin, static_cast<uintptr_t>(phdr.p_memsz));
        entry.address_begin = std::min(entry.address_begin, begin);
        entry.address_end = std::max(entry.address_end, end);
    }
    return 0;
}

PHDRCache* buildPHDRCache(const RawPHDRCacheSnapshot& snapshot) {
    auto* cache = new PHDRCache;
    cache->reserve(snapshot.size);
    for (size_t i = 0; i < snapshot.size; ++i) {
        const auto& raw_entry = snapshot.entries[i];
        PHDRCacheEntry entry;
        entry.info = raw_entry.info;
        entry.phdrs.assign(raw_entry.phdrs.begin(), raw_entry.phdrs.begin() + raw_entry.phdr_count);
        entry.name = raw_entry.name.data();
        entry.address_begin = raw_entry.address_begin;
        entry.address_end = raw_entry.address_end;
        cache->emplace_back(std::move(entry));
    }
    for (auto& entry : *cache) {
        entry.info.dlpi_phdr = entry.phdrs.data();
        entry.info.dlpi_name = entry.name.c_str();
    }
    return cache;
}

int iteratePHDRCache(int (*callback)(dl_phdr_info* info, size_t size, void* data), void* data,
                     uintptr_t ip) {
    auto* current_phdr_cache = phdr_cache.load(std::memory_order_acquire);
    if (current_phdr_cache == nullptr) {
        return 0;
    }

    int result = 0;
    for (auto& entry : *current_phdr_cache) {
        if (!entry.contains(ip)) {
            continue;
        }
        result = callback(&entry.info, sizeof(dl_phdr_info), data);
        if (result != 0) {
            break;
        }
    }
    return result;
}

} // namespace

extern "C"
#ifndef __clang__
        [[gnu::visibility("default")]] [[gnu::externally_visible]]
#endif
        int
        dl_iterate_phdr(int (*callback)(dl_phdr_info* info, size_t size, void* data), void* data) {
    if (!use_phdr_cache) {
        return getOriginalDLIteratePHDR()(callback, data);
    }

    return iteratePHDRCache(callback, data, 0);
}

extern "C"
#ifndef __clang__
        [[gnu::visibility("default")]] [[gnu::externally_visible]]
#endif
        int
        doris_unwind_iterate_phdr(int (*callback)(dl_phdr_info* info, size_t size, void* data),
                                  void* data, uintptr_t ip) {
    return iteratePHDRCache(callback, data, ip);
}

#include "util/debug/leak_annotations.h"

void updatePHDRCache() {
    // Fill out ELF header cache for access without locking.
    // Old snapshots are intentionally kept alive because another thread may already be unwinding
    // through the previous cache when a Doris-controlled dlopen/dlclose refreshes this one.

    auto raw_snapshot = std::make_unique<RawPHDRCacheSnapshot>();
    getOriginalDLIteratePHDR()(
            [](dl_phdr_info* info, size_t size, void* data) {
                // `info` is created by dl_iterate_phdr, which is a non-instrumented
                // libc function, so we have to unpoison it manually.
                __msan_unpoison(info, sizeof(*info));

                return collectPHDRCacheEntry(info, size, data);
            },
            raw_snapshot.get());

    PHDRCache* new_phdr_cache = buildPHDRCache(*raw_snapshot);
    phdr_cache.store(new_phdr_cache, std::memory_order_release);

    /// Memory is intentionally leaked.
    ANNOTATE_LEAKING_OBJECT_PTR(new_phdr_cache);
}

bool hasPHDRCache() {
    return phdr_cache.load(std::memory_order_acquire) != nullptr;
}

void configureLibunwindPHDRCache() {
#if defined(USE_UNWIND) && USE_UNWIND
    static std::once_flag once;
    std::call_once(once, [] {
        unw_context_t context;
        unw_cursor_t cursor;
        (void)unw_getcontext(&context);
        (void)unw_init_local(&cursor, &context);
        // Doris-patched libunwind gets FDEs from the PHDR snapshot. Disable the global DWARF
        // register-state cache so a signal handler cannot self-deadlock on libunwind's cache mutex
        // after interrupting a thread that was already unwinding.
        (void)unw_set_caching_policy(unw_local_addr_space, UNW_CACHE_NONE);
    });
#endif
}

ScopedPHDRCacheRead::ScopedPHDRCacheRead() : _previous(use_phdr_cache) {
    use_phdr_cache = true;
}

ScopedPHDRCacheRead::~ScopedPHDRCacheRead() {
    use_phdr_cache = _previous;
}

#else

void updatePHDRCache() {}

void configureLibunwindPHDRCache() {}

#if defined(USE_MUSL)
/// With statically linked with musl, dl_iterate_phdr is immutable.
bool hasPHDRCache() {
    return true;
}
#else
bool hasPHDRCache() {
    return false;
}
#endif

ScopedPHDRCacheRead::ScopedPHDRCacheRead() = default;

ScopedPHDRCacheRead::~ScopedPHDRCacheRead() = default;

#endif
