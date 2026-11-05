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

#include <unistd.h>

#include <cerrno>
#include <limits>

#include "jemalloc/jemalloc.h"

#ifndef __THROW
#if __cplusplus
#define __THROW noexcept
#else
#define __THROW
#endif
#endif

extern "C" {
void* doris_malloc(size_t size) __THROW {
    return jemalloc(size);
}

void doris_free(void* p) __THROW {
    jefree(p);
}

void* doris_realloc(void* p, size_t size) __THROW {
    void* ptr = jerealloc(p, size);
    // jemalloc 5.3.0 can reject an oversized request without setting errno.
    // realloc(p, 0) may free p and return nullptr, so only nonzero sizes fail.
    if (ptr == nullptr && size != 0) {
        errno = ENOMEM;
    }
    return ptr;
}

void* doris_calloc(size_t n, size_t size) __THROW {
    return jecalloc(n, size);
}

void doris_cfree(void* ptr) __THROW {
    jefree(ptr);
}

void* doris_memalign(size_t align, size_t size) __THROW {
    return jealigned_alloc(align, size);
}

void* doris_aligned_alloc(size_t align, size_t size) __THROW {
    return jealigned_alloc(align, size);
}

void* doris_valloc(size_t size) __THROW {
    // Unlike jemalloc's legacy valloc, aligned_alloc keeps zero-size results
    // page-aligned and sets errno when an allocation fails.
    return jealigned_alloc(getpagesize(), size);
}

void* doris_pvalloc(size_t size) __THROW {
    const size_t page_size = getpagesize();
    // pvalloc rounds up to whole pages; SIZE_MAX must fail rather than wrap
    // to a small allocation when adding the page-size adjustment.
    if (size > std::numeric_limits<size_t>::max() - (page_size - 1)) {
        errno = ENOMEM;
        return nullptr;
    }
    const size_t rounded_size = (size + page_size - 1) / page_size * page_size;
    return jealigned_alloc(page_size, rounded_size);
}

int doris_posix_memalign(void** r, size_t align, size_t size) __THROW {
    return jeposix_memalign(r, align, size);
}

size_t doris_malloc_usable_size(void* ptr) __THROW {
    return jemalloc_usable_size(ptr);
}

#define ALIAS(doris_fn) __attribute__((alias(#doris_fn), used))
void* malloc(size_t size) __THROW ALIAS(doris_malloc);
void free(void* p) __THROW ALIAS(doris_free);
void* realloc(void* p, size_t size) __THROW ALIAS(doris_realloc);
void* calloc(size_t n, size_t size) __THROW ALIAS(doris_calloc);
void cfree(void* ptr) __THROW ALIAS(doris_cfree);
void* memalign(size_t align, size_t size) __THROW ALIAS(doris_memalign);
#ifdef __GLIBC__
// Keep glibc's exported allocation entry points paired with the public jemalloc
// hooks. For example, before glibc 2.25, dlopened modules allocate dynamic TLS
// with __libc_memalign and release it through free.
void* __libc_malloc(size_t size) __THROW ALIAS(doris_malloc);
void __libc_free(void* ptr) __THROW ALIAS(doris_free);
void* __libc_calloc(size_t n, size_t size) __THROW ALIAS(doris_calloc);
void* __libc_realloc(void* ptr, size_t size) __THROW ALIAS(doris_realloc);
void* __libc_memalign(size_t align, size_t size) __THROW ALIAS(doris_memalign);
void* __libc_valloc(size_t size) __THROW ALIAS(doris_valloc);
void* __libc_pvalloc(size_t size) __THROW ALIAS(doris_pvalloc);
#endif
void* aligned_alloc(size_t align, size_t size) __THROW ALIAS(doris_aligned_alloc);
void* valloc(size_t size) __THROW ALIAS(doris_valloc);
void* pvalloc(size_t size) __THROW ALIAS(doris_pvalloc);
int posix_memalign(void** r, size_t a, size_t s) __THROW ALIAS(doris_posix_memalign);
size_t malloc_usable_size(void* ptr) __THROW ALIAS(doris_malloc_usable_size);
}
