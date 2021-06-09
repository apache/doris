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
#ifndef DORIS_UTIL_DEBUG_LEAK_ANNOTATIONS_H_
#define DORIS_UTIL_DEBUG_LEAK_ANNOTATIONS_H_

// Ignore a single leaked object, given its pointer.
// Does nothing if LeakSanitizer is not enabled.
#define ANNOTATE_LEAKING_OBJECT_PTR(p)

#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#if defined(__linux__)

#undef ANNOTATE_LEAKING_OBJECT_PTR
#define ANNOTATE_LEAKING_OBJECT_PTR(p) __lsan_ignore_object(p);

#endif
#endif
#endif

// API definitions from LLVM lsan_interface.h

extern "C" {
// Allocations made between calls to __lsan_disable() and __lsan_enable() will
// be treated as non-leaks. Disable/enable pairs may be nested.
void __lsan_disable();
void __lsan_enable();

// The heap object into which p points will be treated as a non-leak.
void __lsan_ignore_object(const void* p);

// The user may optionally provide this function to disallow leak checking
// for the program it is linked into (if the return value is non-zero). This
// function must be defined as returning a constant value; any behavior beyond
// that is unsupported.
int __lsan_is_turned_off();

// Check for leaks now. This function behaves identically to the default
// end-of-process leak check. In particular, it will terminate the process if
// leaks are found and the exitcode runtime flag is non-zero.
// Subsequent calls to this function will have no effect and end-of-process
// leak check will not run. Effectively, end-of-process leak check is moved to
// the time of first invocation of this function.
// By calling this function early during process shutdown, you can instruct
// LSan to ignore shutdown-only leaks which happen later on.
void __lsan_do_leak_check();

// Check for leaks now. Returns zero if no leaks have been found or if leak
// detection is disabled, non-zero otherwise.
// This function may be called repeatedly, e.g. to periodically check a
// long-running process. It prints a leak report if appropriate, but does not
// terminate the process. It does not affect the behavior of
// __lsan_do_leak_check() or the end-of-process leak check, and is not
// affected by them.
int __lsan_do_recoverable_leak_check();
} // extern "C"

namespace doris {
namespace debug {

class ScopedLSANDisabler {
public:
    ScopedLSANDisabler() { __lsan_disable(); }
    ~ScopedLSANDisabler() { __lsan_enable(); }
};

} // namespace debug
} // namespace doris

#endif // DORIS_UTIL_DEBUG_LEAK_ANNOTATIONS_H_
