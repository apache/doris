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

// Fix for the ASAN stale-stack-poison crashes on the thrift retry/reopen
// path (internal QA-202 / DORIS-1073 / DORIS-15154, google/glog#978,
// google/sanitizers#1010). See asan-glog-rca.md at the repository root.
//
// The BE's fully static link (-static-libstdc++/-static-libgcc plus a static
// sanitizer runtime) resolves __cxa_throw/__cxa_rethrow to libstdc++.a's
// strong definitions, silently discarding the ASAN runtime's weak interceptor
// versions. So when un-instrumented code (libthrift and the other third-party
// static libs) throws, __asan_handle_no_return() never runs, the unwound
// instrumented frames keep their stack-shadow poison, and the next deep call
// chain over the same stack range dies with a false positive that ASAN cannot
// even report:
//     AddressSanitizer: CHECK failed: asan_thread.cpp:...
//         "((ptr[0] == kCurrentStackFrameMagic)) != (0)" (0x0, 0x0)
//
// ASAN builds therefore link with
//     -Wl,--wrap=__cxa_throw -Wl,--wrap=__cxa_rethrow
//     -Wl,--wrap=_Unwind_RaiseException
// (see the ASAN branch in be/CMakeLists.txt). --wrap redirects every
// reference from every input object -- including the members of the
// un-instrumented third-party archives and of libstdc++.a itself -- to the
// wrappers below, which restore exactly what the official interceptors do:
// wipe the stack shadow above the throw point, then forward to the real
// implementation (__real___cxa_throw resolves to libstdc++'s definition).
//
// _Unwind_RaiseException is wrapped as well because not every raise funnels
// through __cxa_throw: std::rethrow_exception (libstdc++'s eh_ptr.cc) and
// foreign-language unwinders call it directly. This matches the official
// ASAN interceptor set for the unwind entry points. Throws via __cxa_throw
// run the shadow wipe twice (once per wrapper); that is idempotent and is
// also what happens with the stock interceptors.

#if defined(ADDRESS_SANITIZER) && defined(__linux__)

extern "C" {

void __asan_handle_no_return();
void __real___cxa_throw(void* thrown_exception, void* tinfo, void (*dest)(void*));
void __real___cxa_rethrow();
int __real__Unwind_RaiseException(void* exception_object);

void __wrap___cxa_throw(void* thrown_exception, void* tinfo, void (*dest)(void*));
void __wrap___cxa_rethrow();
int __wrap__Unwind_RaiseException(void* exception_object);

void __wrap___cxa_throw(void* thrown_exception, void* tinfo, void (*dest)(void*)) {
    __asan_handle_no_return();
    __real___cxa_throw(thrown_exception, tinfo, dest);
}

void __wrap___cxa_rethrow() {
    __asan_handle_no_return();
    __real___cxa_rethrow();
}

// Unlike the two above this one RETURNS when no handler is found (the caller
// then calls std::terminate), so the return value must be forwarded.
int __wrap__Unwind_RaiseException(void* exception_object) {
    __asan_handle_no_return();
    return __real__Unwind_RaiseException(exception_object);
}

} // extern "C"

#endif // ADDRESS_SANITIZER && __linux__
