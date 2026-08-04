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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/base/phdr_cache.h
// and modified by Doris

#pragma once

#if defined(__linux__) && !defined(THREAD_SANITIZER) && !defined(USE_MUSL)
#include <link.h>

#include <cstddef>
#include <cstdint>
#endif

/// This code was based on the code by Fedor Korotkiy https://www.linkedin.com/in/fedor-korotkiy-659a1838/

/** Collects loaded-object PHDR metadata into a lock-free snapshot.
  * Doris uses this snapshot in two narrow places where entering glibc's loader lock can deadlock:
  * the BE stack-trace signal handler and Doris-patched GNU libunwind's FDE lookup. Both paths may
  * run while another thread is inside dlopen/dlclose or jemalloc profiling.
  *
  * Normal code paths must keep using the original glibc dl_iterate_phdr. Process-wide use of a
  * snapshot can hide libraries loaded after the last refresh from sanitizers, JVM/Jindo native
  * code, and C++ exception handling. Use ScopedPHDRCacheRead only around the minimal
  * signal-handler unwind section; GNU libunwind reaches this cache through
  * doris_unwind_iterate_phdr without changing ordinary dl_iterate_phdr callers.
  *
  * Old cache snapshots are intentionally leaked and remain readable by concurrent signal-handler
  * unwinders.
  *
  * NOTE: It is disabled with Thread Sanitizer because TSan can only use original "dl_iterate_phdr" function.
  */
void updatePHDRCache();

/** Configure GNU libunwind to use the PHDR snapshot without its global register-state cache. */
void configureLibunwindPHDRCache();

/** Check if a PHDR snapshot is available for ScopedPHDRCacheRead. */
bool hasPHDRCache();

#if defined(__linux__) && !defined(THREAD_SANITIZER) && !defined(USE_MUSL)
/**
 * GNU libunwind calls this weak hook from Doris-patched thirdparty libunwind instead of calling
 * glibc dl_iterate_phdr directly. The hook is intentionally narrower than the process-wide
 * dl_iterate_phdr interposer: ordinary loader users still see glibc's live loader list, while
 * libunwind reads Doris' lock-free PHDR snapshot to avoid loader-lock inversions during profiling
 * and signal-context stack capture.
 */
extern "C" int doris_unwind_iterate_phdr(int (*callback)(dl_phdr_info* info, size_t size,
                                                         void* data),
                                         void* data, uintptr_t ip);
#endif

class ScopedPHDRCacheRead {
public:
    ScopedPHDRCacheRead();
    ~ScopedPHDRCacheRead();

    ScopedPHDRCacheRead(const ScopedPHDRCacheRead&) = delete;
    ScopedPHDRCacheRead& operator=(const ScopedPHDRCacheRead&) = delete;

private:
#if defined(__linux__) && !defined(THREAD_SANITIZER) && !defined(USE_MUSL)
    bool _previous = false;
#endif
};
