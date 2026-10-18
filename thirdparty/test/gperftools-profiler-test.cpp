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

#include <dlfcn.h>
#include <gperftools/profiler.h>
#include <unwind.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

static std::atomic<unsigned> unwind_calls {0};
static_assert(std::atomic<unsigned>::is_always_lock_free);

extern "C" _Unwind_Reason_Code __real__Unwind_Backtrace(_Unwind_Trace_Fn, void*);
extern "C" _Unwind_Reason_Code __wrap__Unwind_Backtrace(_Unwind_Trace_Fn callback, void* arg) {
    unwind_calls.fetch_add(1, std::memory_order_relaxed);
    return __real__Unwind_Backtrace(callback, arg);
}

extern "C" __attribute__((noinline)) void profiler_test_leaf() {
    volatile unsigned long value = 1;
    auto end = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (std::chrono::steady_clock::now() < end) {
        for (int i = 0; i < 10000; ++i) {
            value = value * 1664525UL + 1013904223UL;
        }
    }
}

extern "C" __attribute__((noinline)) void profiler_test_parent() {
    profiler_test_leaf();
    asm volatile("" ::: "memory"); // Keep the parent frame instead of a tail call.
}

static bool has_parent_frame(const char* path) {
    FILE* profile = std::fopen(path, "rb");
    if (profile == nullptr) {
        return false;
    }
    uintptr_t header[5];
    bool found = false;
    if (std::fread(header, sizeof(header), 1, profile) == 1 && header[0] == 0 && header[1] == 3 &&
        header[2] == 0) {
        uintptr_t entry[2];
        while (std::fread(entry, sizeof(entry), 1, profile) == 1) {
            if (entry[0] == 0 || entry[1] > 1024) {
                break;
            }
            std::vector<uintptr_t> stack(entry[1]);
            if (std::fread(stack.data(), sizeof(uintptr_t), stack.size(), profile) !=
                stack.size()) {
                break;
            }
            // Slot zero is the interrupted PC; a parent must come from unwinding.
            for (size_t i = 1; i < stack.size(); ++i) {
                Dl_info info {};
                if (dladdr(reinterpret_cast<void*>(stack[i]), &info) && info.dli_sname != nullptr &&
                    std::strcmp(info.dli_sname, "profiler_test_parent") == 0) {
                    found = true;
                }
            }
        }
    }
    std::fclose(profile);
    return found;
}

int main(int argc, char** argv) {
    if (argc != 2 || !ProfilerStart(argv[1])) {
        std::fputs("FAIL: cannot start CPU profiler\n", stderr);
        return 1;
    }
    // Exclude initialization, which may legitimately use libgcc outside SIGPROF.
    unwind_calls.store(0, std::memory_order_relaxed);
    std::thread worker(profiler_test_parent);
    profiler_test_parent();
    worker.join();
    ProfilerState state {};
    ProfilerGetCurrentState(&state);
    ProfilerStop();
    unsigned calls = unwind_calls.load(std::memory_order_relaxed);
    std::printf("CPU samples=%d libgcc unwind calls=%u\n", state.samples_gathered, calls);
    if (state.samples_gathered == 0 || calls != 0) {
        std::fputs("FAIL: CPU profiling must sample without libgcc unwinding\n", stderr);
        return 1;
    }
    if (!has_parent_frame(argv[1])) {
        std::fputs("FAIL: CPU profile does not contain the known parent frame\n", stderr);
        return 1;
    }
    std::puts("PASS: CPU profiler samples parent frames without libgcc unwinding");
}
