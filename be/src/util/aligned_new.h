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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/aligned-new.h
// and modified by Doris

#ifndef DORIS_BE_SRC_UTIL_ALIGNED_NEW_H_
#define DORIS_BE_SRC_UTIL_ALIGNED_NEW_H_

#include <memory>

#include "common/compiler_util.h"
#include "common/logging.h"

namespace doris {

// Objects that should be allocated, for performance or correctness reasons, at alignment
// greater than that promised by the global new (16) can inherit publicly from AlignedNew.
template <size_t ALIGNMENT>
struct alignas(ALIGNMENT) AlignedNew {
    static_assert(ALIGNMENT > 0, "ALIGNMENT must be positive");
    static_assert((ALIGNMENT & (ALIGNMENT - 1)) == 0, "ALIGNMENT must be a power of 2");
    static_assert((ALIGNMENT % sizeof(void*)) == 0,
                  "ALIGNMENT must be a multiple of sizeof(void *)");
    static void* operator new(std::size_t count) { return Allocate(count); }
    static void* operator new[](std::size_t count) { return Allocate(count); }
    static void operator delete(void* ptr) { free(ptr); }
    static void operator delete[](void* ptr) { free(ptr); }

private:
    static void* Allocate(std::size_t count) {
        void* result = nullptr;
        const auto alloc_failed = posix_memalign(&result, ALIGNMENT, count);
        if (alloc_failed) {
            LOG(ERROR) << "Failed to allocate aligned memory; return code " << alloc_failed;
            throw std::bad_alloc();
        }
        DCHECK(result != nullptr);
        return result;
    }
};

using CacheLineAligned = AlignedNew<CACHE_LINE_SIZE>;
} // namespace doris

#endif
