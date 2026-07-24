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

#ifdef WITH_PAIMON_CPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "core/allocator.h"
#include "core/allocator_fwd.h"
#include "paimon/memory/memory_pool.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"

namespace doris {

class PaimonDorisMemoryPool final : public ::paimon::MemoryPool {
public:
    explicit PaimonDorisMemoryPool(
            const std::shared_ptr<::doris::MemTrackerLimiter>& query_mem_tracker);
    ~PaimonDorisMemoryPool() override = default;

    void* Malloc(uint64_t size, uint64_t alignment) override;
    void* Realloc(void* p, size_t old_size, size_t new_size, uint64_t alignment) override;
    void Free(void* p, uint64_t size) override;

    uint64_t CurrentUsage() const override;
    uint64_t MaxMemoryUsage() const override;

private:
    static void* _zero_size_ptr();
    void _ensure_thread_context(std::unique_ptr<::doris::SwitchThreadMemTrackerLimiter>* switcher,
                                std::unique_ptr<::doris::AttachTask>* attacher);

    std::atomic<uint64_t> _bytes_allocated {0};
    std::atomic<uint64_t> _max_bytes_allocated {0};

    Allocator<false, false, false, DefaultMemoryAllocator> _allocator;
    std::shared_ptr<::doris::MemTrackerLimiter> _query_mem_tracker;
};

} // namespace doris

#endif
