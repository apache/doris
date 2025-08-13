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

class AsanPoisonDefer {
#ifdef ADDRESS_SANITIZER
public:
    // Poison the memory region to prevent accidental access
    // during the lifetime of this object.
    AsanPoisonDefer(const void* start, size_t len) : start(start), len(len) {
        ASAN_POISON_MEMORY_REGION(start, len);
    }
    // Unpoison the memory region when this object goes out of scope.
    ~AsanPoisonDefer() { ASAN_UNPOISON_MEMORY_REGION(start, len); }

private:
    const void* start;
    size_t len;
#else
public:
    // No-op for platforms without ASAN_DEFINE_REGION_MACROS
    AsanPoisonDefer(const void*, size_t) {}
    ~AsanPoisonDefer() = default;
#endif
};
