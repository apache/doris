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

#include "runtime/memory/system_allocator.h"

#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>

#include "common/config.h"
#include "common/logging.h"
#include "runtime/thread_context.h"

namespace doris {

#define PAGE_SIZE (4 * 1024) // 4K

uint8_t* SystemAllocator::allocate(size_t length) {
    return allocate_via_malloc(length);
}

void SystemAllocator::free(uint8_t* ptr, size_t length) {
    ::free(ptr);
}

uint8_t* SystemAllocator::allocate_via_malloc(size_t length) {
    void* ptr = nullptr;
    // try to use a whole page instead of parts of one page
    int res = posix_memalign(&ptr, PAGE_SIZE, length);
    if (res != 0) {
        char buf[64];
        auto err = fmt::format("fail to allocate mem via posix_memalign, res={}, errmsg={}.", res,
                               strerror_r(res, buf, 64));
        MemTrackerLimiter::print_log_process_usage(err);
        LOG(ERROR) << err;
        return nullptr;
    }
    return (uint8_t*)ptr;
}

} // namespace doris
