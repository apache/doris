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

#include "jemalloc/jemalloc.h"
#include "runtime/thread_context.h"

extern "C" {
void* doris_malloc(size_t size) __THROW {
    MEM_MALLOC_HOOK(je_nallocx(size, 0));
    void* ptr = je_malloc(size);
    if (UNLIKELY(ptr == nullptr)) {
        MEM_FREE_HOOK(je_nallocx(size, 0));
    }
    return ptr;
}

void doris_free(void* p) __THROW {
    MEM_FREE_HOOK(je_malloc_usable_size(p));
    je_free(p);
}

void* doris_realloc(void* p, size_t size) __THROW {
    if (UNLIKELY(size == 0)) {
        return nullptr;
    }
    int64_t old_size = je_malloc_usable_size(p);
    MEM_MALLOC_HOOK(je_nallocx(size, 0) - old_size);
    void* ptr = je_realloc(p, size);
    if (UNLIKELY(ptr == nullptr)) {
        MEM_FREE_HOOK(je_nallocx(size, 0) - old_size);
    }
    return ptr;
}

void* doris_calloc(size_t n, size_t size) __THROW {
    if (UNLIKELY(size == 0)) {
        return nullptr;
    }

    MEM_MALLOC_HOOK(n * size);
    void* ptr = je_calloc(n, size);
    if (UNLIKELY(ptr == nullptr)) {
        MEM_FREE_HOOK(n * size);
    } else {
        MEM_FREE_HOOK(je_malloc_usable_size(ptr) - n * size);
    }
    return ptr;
}

void doris_cfree(void* ptr) __THROW {
    MEM_FREE_HOOK(je_malloc_usable_size(ptr));
    je_free(ptr);
}

void* doris_memalign(size_t align, size_t size) __THROW {
    MEM_MALLOC_HOOK(size);
    void* ptr = je_aligned_alloc(align, size);
    if (UNLIKELY(ptr == nullptr)) {
        MEM_FREE_HOOK(size);
    } else {
        MEM_MALLOC_HOOK(je_malloc_usable_size(ptr) - size);
    }
    return ptr;
}

void* doris_aligned_alloc(size_t align, size_t size) __THROW {
    MEM_MALLOC_HOOK(size);
    void* ptr = je_aligned_alloc(align, size);
    if (UNLIKELY(ptr == nullptr)) {
        MEM_FREE_HOOK(size);
    } else {
        MEM_MALLOC_HOOK(je_malloc_usable_size(ptr) - size);
    }
    return ptr;
}

void* doris_valloc(size_t size) __THROW {
    MEM_MALLOC_HOOK(size);
    void* ptr = je_valloc(size);
    if (UNLIKELY(ptr == nullptr)) {
        MEM_FREE_HOOK(size);
    } else {
        MEM_MALLOC_HOOK(je_malloc_usable_size(ptr) - size);
    }
    return ptr;
}

void* doris_pvalloc(size_t size) __THROW {
    MEM_MALLOC_HOOK(size);
    void* ptr = je_valloc(size);
    if (UNLIKELY(ptr == nullptr)) {
        MEM_FREE_HOOK(size);
    } else {
        MEM_MALLOC_HOOK(je_malloc_usable_size(ptr) - size);
    }
    return ptr;
}

int doris_posix_memalign(void** r, size_t align, size_t size) __THROW {
    MEM_MALLOC_HOOK(size);
    int ret = je_posix_memalign(r, align, size);
    if (UNLIKELY(ret != 0)) {
        MEM_FREE_HOOK(size);
    } else {
        MEM_MALLOC_HOOK(je_malloc_usable_size(*r) - size);
    }
    return ret;
}

size_t doris_malloc_usable_size(void* ptr) __THROW {
    size_t ret = je_malloc_usable_size(ptr);
    return ret;
}

#define ALIAS(doris_fn) __attribute__((alias(#doris_fn), used))
void* malloc(size_t size) __THROW ALIAS(doris_malloc);
void free(void* p) __THROW ALIAS(doris_free);
void* realloc(void* p, size_t size) __THROW ALIAS(doris_realloc);
void* calloc(size_t n, size_t size) __THROW ALIAS(doris_calloc);
void cfree(void* ptr) __THROW ALIAS(doris_cfree);
void* memalign(size_t align, size_t size) __THROW ALIAS(doris_memalign);
void* aligned_alloc(size_t align, size_t size) __THROW ALIAS(doris_aligned_alloc);
void* valloc(size_t size) __THROW ALIAS(doris_valloc);
void* pvalloc(size_t size) __THROW ALIAS(doris_pvalloc);
int posix_memalign(void** r, size_t a, size_t s) __THROW ALIAS(doris_posix_memalign);
size_t malloc_usable_size(void* ptr) __THROW ALIAS(doris_malloc_usable_size);
}
