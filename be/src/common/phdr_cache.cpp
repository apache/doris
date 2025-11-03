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

#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <vector>

namespace {

// This is adapted from
// https://github.com/scylladb/seastar/blob/master/core/exception_hacks.hh
// https://github.com/scylladb/seastar/blob/master/core/exception_hacks.cc

using DLIterateFunction = int (*)(int (*callback)(dl_phdr_info* info, size_t size, void* data),
                                  void* data);

DLIterateFunction getOriginalDLIteratePHDR() {
    void* func = dlsym(RTLD_NEXT, "dl_iterate_phdr");
    if (!func) {
        throw std::runtime_error("Cannot find dl_iterate_phdr function with dlsym");
    }
    return reinterpret_cast<DLIterateFunction>(func);
}

using PHDRCache = std::vector<dl_phdr_info>;
std::atomic<PHDRCache*> phdr_cache {};

} // namespace

// Jemalloc heap profile follows libgcc's way of backtracing by default.
// rewrites dl_iterate_phdr will cause Jemalloc to fail to run after enable profile.

// TODO, two solutions:
// 1. Jemalloc specifies GNU libunwind as the prof backtracing way, but my test failed,
//    `--enable-prof-libunwind` not work: https://github.com/jemalloc/jemalloc/issues/2504
// 2. ClickHouse/libunwind solves Jemalloc profile backtracing, but the branch of ClickHouse/libunwind
//    has been out of touch with GNU libunwind and LLVM libunwind, which will leave the fate to others.
/*
extern "C"
#ifndef __clang__
        [[gnu::visibility("default")]] [[gnu::externally_visible]]
#endif
        int
        dl_iterate_phdr(int (*callback)(dl_phdr_info* info, size_t size, void* data), void* data) {
    auto* current_phdr_cache = phdr_cache.load();
    if (!current_phdr_cache) {
        // Cache is not yet populated, pass through to the original function.
        return getOriginalDLIteratePHDR()(callback, data);
    }

    int result = 0;
    for (auto& entry : *current_phdr_cache) {
        result = callback(&entry, offsetof(dl_phdr_info, dlpi_adds), data);
        if (result != 0) {
            break;
        }
    }
    return result;
}
*/

extern "C" {
#ifdef ADDRESS_SANITIZER
void __lsan_ignore_object(const void*);
#else
void __lsan_ignore_object(const void*) {} // NOLINT
#endif
}

void updatePHDRCache() {
    // Fill out ELF header cache for access without locking.
    // This assumes no dynamic object loading/unloading after this point

    PHDRCache* new_phdr_cache = new PHDRCache;
    getOriginalDLIteratePHDR()(
            [](dl_phdr_info* info, size_t /*size*/, void* data) {
                // `info` is created by dl_iterate_phdr, which is a non-instrumented
                // libc function, so we have to unpoison it manually.
                __msan_unpoison(info, sizeof(*info));

                reinterpret_cast<PHDRCache*>(data)->push_back(*info);
                return 0;
            },
            new_phdr_cache);
    phdr_cache.store(new_phdr_cache);

    /// Memory is intentionally leaked.
    __lsan_ignore_object(new_phdr_cache);
}

bool hasPHDRCache() {
    return phdr_cache.load() != nullptr;
}

#else

void updatePHDRCache() {}

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

#endif
