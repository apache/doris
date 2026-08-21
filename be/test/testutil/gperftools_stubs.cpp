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

// doris_be_test on macOS/arm64 links against the system allocator instead of
// tcmalloc: the Debug test binary's .text exceeds arm64's +/-128MB direct
// branch reach and Apple's linker emits no branch islands for the prebuilt
// gperftools archive. These no-op definitions satisfy the few gperftools
// symbols still referenced unconditionally (HeapAction's HTTP handler, which
// no unit test invokes, and brpc's periodic MallocExtension release hint).
// Everywhere else the real libtcmalloc.a provides them and this TU is empty.

#if defined(__APPLE__) && defined(__aarch64__)

#include <cstddef>

extern "C" {

char* GetHeapProfile() {
    return nullptr;
}

void HeapProfilerStart(const char* /*prefix*/) {}

void HeapProfilerStop() {}

void MallocExtension_ReleaseFreeMemory() {}

} // extern "C"

#endif
