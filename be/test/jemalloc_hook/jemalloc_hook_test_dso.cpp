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

#include <cstdlib>

extern "C" void* __libc_memalign(size_t alignment, size_t size) noexcept;
extern "C" void* __libc_malloc(size_t size) noexcept;
extern "C" void __libc_free(void* ptr) noexcept;
extern "C" void* __libc_calloc(size_t count, size_t size) noexcept;
extern "C" void* __libc_realloc(void* ptr, size_t size) noexcept;
extern "C" void* __libc_valloc(size_t size) noexcept;
extern "C" void* __libc_pvalloc(size_t size) noexcept;

extern "C" void jemalloc_hook_test_allocator_symbols(void** symbols) {
    symbols[0] = reinterpret_cast<void*>(&__libc_memalign);
    symbols[1] = reinterpret_cast<void*>(&std::malloc);
    symbols[2] = reinterpret_cast<void*>(&std::free);
    symbols[3] = reinterpret_cast<void*>(&__libc_malloc);
    symbols[4] = reinterpret_cast<void*>(&__libc_free);
    symbols[5] = reinterpret_cast<void*>(&__libc_calloc);
    symbols[6] = reinterpret_cast<void*>(&__libc_realloc);
    symbols[7] = reinterpret_cast<void*>(&__libc_valloc);
    symbols[8] = reinterpret_cast<void*>(&__libc_pvalloc);
}

// Access TLS from a late-loaded library, as happens with JNI native libraries.
extern "C" int jemalloc_hook_test_touch_tls() {
    static thread_local int tls_values[1024] = {};
    return ++tls_values[1023];
}
