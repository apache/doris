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

#include <cstddef>

// Minimal, self-contained reproducer for the historical ASAN-only BE crash
// around thrift RPC retry (reopen) / glog LOG(WARNING) paths:
//
//   AddressSanitizer: CHECK failed: asan_thread.cpp:36x
//       "((ptr[0] == kCurrentStackFrameMagic)) != (0)" (0x0, 0x0)
//
// (internal Jira QA-202 / DORIS-1073 / DORIS-15154, google/glog#978,
// google/sanitizers#1010). Root cause analysis: asan-glog-rca.md at the
// repository root.
//
// The whole reproducer is a no-op unless the BE is compiled with
// BUILD_TYPE=ASAN (-DADDRESS_SANITIZER).

namespace doris {

// Called once at BE startup (doris_main.cpp), runs on a dedicated thread.
// If the bug is present it crashes the process with the historical ASAN
// CHECK signature, preferably at the exact Jira stack site (a real
// FrontendServiceClient RPC against an in-process dead peer, then
// ThriftClientImpl::close() -> TBufferedTransport::close/flush ->
// TSocket::write_partial -> getSocketInfo()). Otherwise it prints
// "[ASAN-REPRO] RESULT: NEGATIVE" and returns so the BE starts normally.
// Set DORIS_DISABLE_ASAN_REPRO=1 to skip it.
void run_asan_stale_poison_repro();

namespace asan_repro {

struct ProbeResult {
    int poisoned_levels = 0;
    int first_poisoned_level = -1;
    const void* first_poisoned_addr = nullptr;
};

#ifdef ADDRESS_SANITIZER
// The functions below are implemented in asan_repro_uninstrumented.cpp,
// which is compiled WITHOUT sanitizer instrumentation (see
// common/CMakeLists.txt) to emulate an un-instrumented third-party static
// library such as libthrift.

// Throws int(42) from un-instrumented code. Deliberately NOT declared
// [[noreturn]]: otherwise the instrumented caller would emit
// __asan_handle_no_return() before the call and mask the bug.
void uninstrumented_throw();

// Descends max_depth un-instrumented frames over the region that was just
// unwound, records which local buffers sit on stale shadow poison, and
// overwrites the region with plain (unchecked) stores so the dead frames'
// on-stack descriptors lose their kCurrentStackFrameMagic.
void probe_and_scrub(int level, int max_depth, ProbeResult* out);

// Descends again and calls memset() (intercepted by ASAN) on the first local
// buffer that overlaps stale poison -> reproduces the historical crash.
void descend_and_boom(int level, int max_depth);

// Prints whether __cxa_throw / _Unwind_RaiseException resolve to the ASAN
// interceptors or to the real (statically linked) implementations.
void describe_repro_symbols(char* out, size_t out_size);
#endif

} // namespace asan_repro
} // namespace doris
