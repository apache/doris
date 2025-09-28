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

#pragma once

#include <sanitizer/asan_interface.h>

class AsanPoisonGuard {
#ifdef ADDRESS_SANITIZER
public:
    // Poison the memory region to prevent accidental access during the lifetime of this object.
    AsanPoisonGuard(const void* start, size_t len) : start(start), len(len) {
        //FIXME: now it may cause some false-positive, need to find a way to fix it. maybe ASAN_UNPOISON_MEMORY_REGION
        // didn't clean the same memory as ASAN_POISON_MEMORY_REGION
        // ASAN_POISON_MEMORY_REGION(start, len);
    }
    // Unpoison the memory region when this object goes out of scope.
    ~AsanPoisonGuard() {
        // ASAN_UNPOISON_MEMORY_REGION(start, len);
    }

private:
    const void* start [[maybe_unused]];
    size_t len [[maybe_unused]];
#else
public:
    // No-op for platforms without ASAN_DEFINE_REGION_MACROS
    AsanPoisonGuard(const void*, size_t) {}
    ~AsanPoisonGuard() = default;
#endif
};
