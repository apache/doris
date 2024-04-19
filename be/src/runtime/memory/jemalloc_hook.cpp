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

#include <jemalloc/jemalloc.h>

#include "common/status.h"
#include "runtime/thread_context.h"

namespace {

extern "C" {
#define HOOK_MAX 4

enum hook_alloc_e {
    hook_alloc_malloc,
    hook_alloc_posix_memalign,
    hook_alloc_aligned_alloc,
    hook_alloc_calloc,
    hook_alloc_memalign,
    hook_alloc_valloc,
    hook_alloc_mallocx,

    /* The reallocating functions have both alloc and dalloc variants */
    hook_alloc_realloc,
    hook_alloc_rallocx,
};
/*
 * We put the enum typedef after the enum, since this file may get included by
 * jemalloc_cpp.cpp, and C++ disallows enum forward declarations.
 */
using hook_alloc_t = enum hook_alloc_e;

enum hook_dalloc_e {
    hook_dalloc_free,
    hook_dalloc_dallocx,
    hook_dalloc_sdallocx,

    /*
	 * The dalloc halves of reallocation (not called if in-place expansion
	 * happens).
	 */
    hook_dalloc_realloc,
    hook_dalloc_rallocx,
};
using hook_dalloc_t = enum hook_dalloc_e;

enum hook_expand_e {
    hook_expand_realloc,
    hook_expand_rallocx,
    hook_expand_xallocx,
};
using hook_expand_t = enum hook_expand_e;

using hook_alloc = void (*)(void* extra, hook_alloc_t type, void* result, uintptr_t result_raw,
                            uintptr_t args_raw[3]);

using hook_dalloc = void (*)(void* extra, hook_dalloc_t type, void* address, uintptr_t args_raw[3]);

using hook_expand = void (*)(void* extra, hook_expand_t type, void* address, size_t old_usize,
                             size_t new_usize, uintptr_t result_raw, uintptr_t args_raw[4]);

using hooks_t = struct hooks_s;
struct hooks_s {
    hook_alloc alloc_hook;
    hook_dalloc dalloc_hook;
    hook_expand expand_hook;
    void* extra;
};

void doris_hook_alloc(void* extra, hook_alloc_t type, void* result, uintptr_t result_raw,
                      uintptr_t args_raw[3]) {
    LOG(INFO) << "####### doris_hook_alloc invoked! (" << type << ")";
    if (result == nullptr) {
        return;
    }
    switch (type) {
    case hook_alloc_malloc: {
        [[maybe_unused]] size_t size = args_raw[0];
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN([](size_t size) { return nallocx(size, 0); },
                                                   size);
        break;
    }
    case hook_alloc_posix_memalign: {
        [[maybe_unused]] size_t size = args_raw[2];
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK(size);
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(
                [](void* ptr, size_t size) { return malloc_usable_size(ptr) - size; }, result,
                size);
        break;
    }
    case hook_alloc_aligned_alloc: {
        [[maybe_unused]] size_t size = args_raw[1];
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK(size);
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(
                [](void* ptr, size_t size) { return malloc_usable_size(ptr) - size; }, result,
                size);
        break;
    }
    case hook_alloc_calloc: {
        [[maybe_unused]] size_t num = args_raw[0];
        [[maybe_unused]] size_t size = args_raw[1];
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK(num * size);
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(
                [](void* ptr, size_t size) { return malloc_usable_size(ptr) - size; }, result,
                num * size);
        break;
    }
    case hook_alloc_memalign: {
        [[maybe_unused]] size_t size = args_raw[1];
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK(size);
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(
                [](void* ptr, size_t size) { return malloc_usable_size(ptr) - size; }, result,
                size);
        break;
    }
    case hook_alloc_valloc: {
        [[maybe_unused]] size_t size = args_raw[0];
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK(size);
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(
                [](void* ptr, size_t size) { return malloc_usable_size(ptr) - size; }, result,
                size);
        break;
    }
    case hook_alloc_mallocx: {
        [[maybe_unused]] size_t size = args_raw[0];
        [[maybe_unused]] int flag = args_raw[1];
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN([](size_t size) { return nallocx(size, 0); },
                                                   size);
        if (flag) {
            CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN(
                    [](void* ptr, size_t size) { return malloc_usable_size(ptr) - size; }, result,
                    size);
        }
        break;
    }
    case hook_alloc_realloc: {
        [[maybe_unused]] size_t size = args_raw[1];
        CONSUME_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN([](size_t size) { return nallocx(size, 0); },
                                                   size);
        break;
    }
    default:
        break;
    }
}

void doris_hook_dalloc(void* extra, hook_dalloc_t type, void* address, uintptr_t args_raw[3]) {
    LOG(INFO) << "####### doris_hook_dalloc invoked! (" << type << ")";
    switch (type) {
    case hook_dalloc_free: {
        [[maybe_unused]] void* ptr = reinterpret_cast<void*>(args_raw[0]);
        RELEASE_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN([](void* p) { return malloc_usable_size(p); },
                                                   ptr);
        break;
    }
    case hook_dalloc_realloc: {
        [[maybe_unused]] void* ptr = reinterpret_cast<void*>(args_raw[0]);
        RELEASE_THREAD_MEM_TRACKER_BY_HOOK_WITH_FN([](void* p) { return malloc_usable_size(p); },
                                                   ptr);
        break;
    }
    default:
        break;
    }
}

void doris_hook_expand(void* extra, hook_expand_t type, void* address, size_t old_usize,
                       size_t new_usize, uintptr_t result_raw, uintptr_t args_raw[4]) {}
}

} // namespace

namespace doris {

Status init_jemalloc_hooks() {
    hooks_t hooks = {
            .alloc_hook = doris_hook_alloc,
            .dalloc_hook = doris_hook_dalloc,
            .expand_hook = doris_hook_expand,
            .extra = nullptr,
    };
    void* handle = nullptr;
    size_t size = sizeof(handle);
    int err = mallctl("experimental.hooks.install", &handle, &size, &hooks, sizeof(hooks));
    if (err) {
        return Status::InternalError("Failed to initialize jemalloc hooks");
    }
    return Status::OK();
}

} // namespace doris
