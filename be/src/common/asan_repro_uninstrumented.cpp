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

// This translation unit is compiled WITHOUT sanitizer instrumentation
// (-fno-sanitize=all -fno-builtin, no PCH; see common/CMakeLists.txt).
// It plays the role of an un-instrumented third-party static library such as
// libthrift in the production crashes:
//   * it throws a C++ exception from un-instrumented code (like TSocket
//     throwing TTransportException when the FE restarts), and
//   * it re-enters the just-unwound stack region without rewriting ASAN
//     shadow memory (like thrift's reopen -> close -> flush ->
//     TSocket::getSocketInfo() path), where its innocent stack locals land
//     on stale poison left behind by the unwind.
// See asan-glog-rca.md at the repository root for the full analysis.

#include "common/asan_repro.h"

#ifdef ADDRESS_SANITIZER

#include <cstdint>
#include <cstdio>
#include <cstring>

// Provided by the statically linked ASAN runtime. Query-only, never reports.
extern "C" void* __asan_region_is_poisoned(void* beg, size_t size);

#if defined(__linux__)
// Real symbols vs. their ASAN interceptors. If the resolved addresses differ,
// calls to the real symbol are NOT intercepted, so __asan_handle_no_return()
// never runs for exceptions thrown by un-instrumented code and the unwound
// stack region keeps its shadow poison. The interceptor symbols are declared
// weak: they exist in ASAN builds, but this keeps the file link-safe
// everywhere else.
extern "C" void __cxa_throw(void*, void*, void (*)(void*));
extern "C" int _Unwind_RaiseException(void*);
extern "C" __attribute__((weak)) void __interceptor___cxa_throw(void*, void*, void (*)(void*));
extern "C" __attribute__((weak)) int __interceptor__Unwind_RaiseException(void*);
// Defined in asan_cxa_throw_wrap.cpp and linked with -Wl,--wrap=<symbol>
// when the fix is active: then the references above resolve to these
// wrappers, which re-insert the __asan_handle_no_return() shadow cleanup.
// Weak, so the diagnosis still links in builds without the fix.
extern "C" __attribute__((weak)) void __wrap___cxa_throw(void*, void*, void (*)(void*));
extern "C" __attribute__((weak)) int __wrap__Unwind_RaiseException(void*);
#endif

namespace doris::asan_repro {

namespace {
// Forces the compiler to keep the buffers and stores.
volatile uint8_t g_sink = 0;
} // namespace

__attribute__((noinline)) void uninstrumented_throw() {
    // No compiler-emitted __asan_handle_no_return() here (this TU is not
    // instrumented), and per the RCA the __cxa_throw/_Unwind_RaiseException
    // interceptors are bypassed by the fully static link. So unwinding this
    // exception leaves the instrumented frames below the catch point with
    // their stack shadow poison still in place.
    throw 42;
}

__attribute__((noinline)) void probe_and_scrub(int level, int max_depth, ProbeResult* out) {
    char buf[256];
    void* poisoned = __asan_region_is_poisoned(buf, sizeof(buf));
    if (poisoned != nullptr) {
        ++out->poisoned_levels;
        if (out->first_poisoned_level < 0) {
            out->first_poisoned_level = level;
            out->first_poisoned_addr = poisoned;
        }
    }
    // Plain stores: un-instrumented and not intercepted, so they change the
    // memory but NOT the shadow. This overwrites the dead instrumented
    // frames' on-stack descriptors (kCurrentStackFrameMagic) with zeroes,
    // which is what turns the later false positive into the historical
    // "CHECK failed ... (0x0, 0x0)" abort instead of a regular ASAN report.
    for (size_t i = 0; i < sizeof(buf); ++i) {
        buf[i] = 0;
    }
    g_sink = static_cast<uint8_t>(g_sink + buf[0]);
    if (level + 1 < max_depth) {
        probe_and_scrub(level + 1, max_depth, out);
    }
    asm volatile("" : : "r"(buf) : "memory");
}

__attribute__((noinline)) void descend_and_boom(int level, int max_depth) {
    char buf[256];
    if (__asan_region_is_poisoned(buf, sizeof(buf)) != nullptr) {
        // memset IS intercepted by the ASAN runtime and validates the shadow
        // of the destination range: writing to this perfectly valid local
        // buffer raises a false-positive report, and while describing the
        // address ASAN walks the (scrubbed) frame descriptor and dies with
        // the kCurrentStackFrameMagic CHECK. This mirrors thrift's
        // getSocketInfo()/TOutput::perror() locals being written through
        // intercepted memcpy/memset/localtime_r in the production stacks.
        memset(buf, 0, sizeof(buf));
        g_sink = static_cast<uint8_t>(g_sink + buf[0]); // not reached if the bug is present
    }
    if (level + 1 < max_depth) {
        descend_and_boom(level + 1, max_depth);
    }
    asm volatile("" : : "r"(buf) : "memory");
}

void describe_repro_symbols(char* out, size_t out_size) {
#if defined(__linux__)
    void* real_throw = reinterpret_cast<void*>(&__cxa_throw);
    void* icept_throw = reinterpret_cast<void*>(&__interceptor___cxa_throw);
    void* wrap_throw = reinterpret_cast<void*>(&__wrap___cxa_throw);
    void* real_raise = reinterpret_cast<void*>(&_Unwind_RaiseException);
    void* icept_raise = reinterpret_cast<void*>(&__interceptor__Unwind_RaiseException);
    const bool wrap_active = wrap_throw != nullptr && real_throw == wrap_throw;
    const char* throw_state;
    if (icept_throw != nullptr && real_throw == icept_throw) {
        throw_state = "INTERCEPTED";
    } else if (wrap_active) {
        // --wrap fix active: every __cxa_throw reference (this one included)
        // resolves to __wrap___cxa_throw, which restores the shadow cleanup.
        throw_state = "WRAPPED by __wrap___cxa_throw (fix active, shadow cleanup restored)";
    } else {
        throw_state = "NOT intercepted (bypassed by static link)";
    }
    void* wrap_raise = reinterpret_cast<void*>(&__wrap__Unwind_RaiseException);
    const char* raise_state;
    if (icept_raise != nullptr && real_raise == icept_raise) {
        raise_state = "INTERCEPTED";
    } else if (wrap_raise != nullptr && real_raise == wrap_raise) {
        raise_state =
                "WRAPPED by __wrap__Unwind_RaiseException (fix active, shadow cleanup restored)";
    } else {
        raise_state = "NOT intercepted (bypassed by static link)";
    }
    snprintf(out, out_size,
             "[ASAN-REPRO] __cxa_throw=%p, __interceptor___cxa_throw=%p => %s\n"
             "[ASAN-REPRO] _Unwind_RaiseException=%p, __interceptor__Unwind_RaiseException=%p "
             "=> %s\n",
             real_throw, icept_throw, throw_state, real_raise, icept_raise, raise_state);
#else
    snprintf(out, out_size, "[ASAN-REPRO] symbol diagnosis is only implemented on linux\n");
#endif
}

} // namespace doris::asan_repro

#endif // ADDRESS_SANITIZER
