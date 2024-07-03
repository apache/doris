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

#if defined(USE_JEMALLOC) && defined(USE_MEM_TRACKER)
extern "C" {
void* doris_malloc(size_t size) __THROW;
void doris_free(void* p) __THROW;
}
#endif // #if defined(USE_JEMALLOC) && defined(USE_MEM_TRACKER)

namespace doris::vectorized {

class ORCMemoryPool : public orc::MemoryPool {
public:
    char* malloc(uint64_t size) override {
#if defined(USE_JEMALLOC) && defined(USE_MEM_TRACKER)
        return reinterpret_cast<char*>(doris_malloc(size));
#else
        return reinterpret_cast<char*>(std::malloc(size));
#endif // #if defined(USE_JEMALLOC) && defined(USE_MEM_TRACKER)
    }

    void free(char* p) override {
#if defined(USE_JEMALLOC) && defined(USE_MEM_TRACKER)
        doris_free(p);
#else
        std::free(p);
#endif // #if defined(USE_JEMALLOC) && defined(USE_MEM_TRACKER)
    }

    ORCMemoryPool() = default;
    ~ORCMemoryPool() override = default;
};

} // namespace doris::vectorized
