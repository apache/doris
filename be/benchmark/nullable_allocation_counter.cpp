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

#include "nullable_allocation_counter.h"

#include <benchmark/benchmark.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>

namespace doris::nullable_aggregation_benchmark {
namespace {
std::atomic<bool> measuring {false};
std::atomic<int64_t> allocations {0};
std::atomic<int64_t> allocated_bytes {0};
std::atomic<int64_t> operations {0};
std::string benchmark_name;
thread_local bool in_operation = false;
thread_local bool in_allocator = false;

class MemoryCounter final : public benchmark::MemoryManager {
public:
    void Start() override {
        allocations = 0;
        allocated_bytes = 0;
        operations = 0;
        measuring = true;
    }
    void Stop(Result& result) override {
        measuring = false;
        result.num_allocs = allocations;
        result.total_allocated_bytes = allocated_bytes;
        // Peak live bytes require free tracking. Use query profiles for that measurement;
        // never present requested allocation bytes as a memory peak.
        result.max_bytes_used = TombstoneValue;
        std::cout << "NULLABLE_ALLOCATIONS {\"benchmark\":\"" << benchmark_name
                  << "\",\"operations\":" << operations.load()
                  << ",\"requests\":" << allocations.load()
                  << ",\"requested_bytes\":" << allocated_bytes.load() << "}\n";
    }
};
} // namespace

bool begin_allocation_counting() {
    bool previous = in_operation;
    in_operation = true;
    if (!previous && measuring.load(std::memory_order_relaxed)) {
        operations.fetch_add(1, std::memory_order_relaxed);
    }
    return previous;
}
void end_allocation_counting(bool previous) {
    in_operation = previous;
}
void set_allocation_benchmark(const std::string& name) {
    benchmark_name = name;
}
void register_allocation_counter() {
    static MemoryCounter counter;
    benchmark::RegisterMemoryManager(&counter);
    benchmark::AddCustomContext(
            "allocation_metric",
            "successful allocation requests inside the operation; setup excluded");
    benchmark::AddCustomContext(
            "max_bytes_used",
            "unavailable (MemoryManager::TombstoneValue); use query profile peaks");
}

// Allocation counting is built separately from timing runs. Link wrapping is restricted
// to this executable; it never changes doris_be or the normal benchmark binary.
class RequestScope {
public:
    RequestScope() : _previous(in_allocator), _outer(in_operation && !in_allocator) {
        in_allocator = true;
    }
    ~RequestScope() { in_allocator = _previous; }
    void record(const void* pointer, size_t size) const {
        if (_outer && pointer) {
            allocations.fetch_add(1, std::memory_order_relaxed);
            allocated_bytes.fetch_add(size, std::memory_order_relaxed);
        }
    }

private:
    bool _previous;
    bool _outer;
};
} // namespace doris::nullable_aggregation_benchmark

using doris::nullable_aggregation_benchmark::RequestScope;
using doris::nullable_aggregation_benchmark::measuring;

extern "C" {
void* __real_malloc(size_t size);
void* __real_calloc(size_t count, size_t size);
void* __real_realloc(void* pointer, size_t size);
void* __real__Znwm(size_t size);
void* __real__Znam(size_t size);

void* __wrap_malloc(size_t size) {
    if (!measuring.load(std::memory_order_relaxed)) {
        return __real_malloc(size);
    }
    RequestScope scope;
    void* pointer = __real_malloc(size);
    scope.record(pointer, size);
    return pointer;
}
void* __wrap_calloc(size_t count, size_t size) {
    if (!measuring.load(std::memory_order_relaxed)) {
        return __real_calloc(count, size);
    }
    RequestScope scope;
    void* pointer = __real_calloc(count, size);
    scope.record(pointer, count * size);
    return pointer;
}
void* __wrap_realloc(void* previous, size_t size) {
    if (!measuring.load(std::memory_order_relaxed)) {
        return __real_realloc(previous, size);
    }
    RequestScope scope;
    void* pointer = __real_realloc(previous, size);
    scope.record(pointer, size);
    return pointer;
}
void* __wrap__Znwm(size_t size) {
    if (!measuring.load(std::memory_order_relaxed)) {
        return __real__Znwm(size);
    }
    RequestScope scope;
    void* pointer = __real__Znwm(size);
    scope.record(pointer, size);
    return pointer;
}
void* __wrap__Znam(size_t size) {
    if (!measuring.load(std::memory_order_relaxed)) {
        return __real__Znam(size);
    }
    RequestScope scope;
    void* pointer = __real__Znam(size);
    scope.record(pointer, size);
    return pointer;
}
}
