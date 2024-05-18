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

#include "runtime/memory/global_memory_arbitrator.h"

#include <bvar/bvar.h>

namespace doris {

bvar::PassiveStatus<int64_t> g_proc_mem_no_allocator_cache(
        "meminfo_proc_mem_no_allocator_cache",
        [](void*) { return GlobalMemoryArbitrator::proc_mem_no_allocator_cache(); }, nullptr);
bvar::PassiveStatus<int64_t> g_proc_mem_corrected(
        "meminfo_proc_mem_corrected",
        [](void*) { return GlobalMemoryArbitrator::proc_mem_corrected(); }, nullptr);

std::atomic<int64_t> GlobalMemoryArbitrator::_s_proc_mem_no_allocator_cache = -1;
std::atomic<int64_t> GlobalMemoryArbitrator::_s_proc_reserved_mem = 0;

} // namespace doris
