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

#include "orc/MemoryPool.hh"
#include "vec/common/allocator.h"

#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
using ORC_MEMORY_ALLOCATOR = RecordSizeMemoryAllocator;
#else
using ORC_MEMORY_ALLOCATOR = ORCMemoryAllocator;
#endif

namespace doris::vectorized {

class ORCMemoryPool : public orc::MemoryPool {
public:
    char* malloc(uint64_t size) override {
        char* p = reinterpret_cast<char*>(_allocator.alloc(size));
        return p;
    }

    void free(char* p) override {
        if (p == nullptr) {
            return;
        }
        size_t size = ORC_MEMORY_ALLOCATOR::allocated_size(p);
        _allocator.free(p, size);
    }

    ORCMemoryPool() = default;
    ~ORCMemoryPool() override = default;

private:
    Allocator<false, false, false, ORC_MEMORY_ALLOCATOR> _allocator;
};

} // namespace doris::vectorized
