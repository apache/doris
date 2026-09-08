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

#include "exec/sink/writer/paimon/cpp_paimon_write_backend.h"

#ifdef USE_PAIMON_CPP
#include <gtest/gtest.h>

#include <cstring>
#include <thread>

#include "common/config.h"
#include "runtime/query_context.h"
#include "util/defer_op.h"

namespace doris {

TEST(PaimonQueryMemoryPoolTest, AllocatorOomRollsBackAndAllowsNextAllocation) {
    TUniqueId query_id;
    query_id.hi = 7;
    query_id.lo = 9;
    TQueryOptions options;
    options.__set_mem_limit(64L * 1024 * 1024);
    options.__set_query_type(TQueryType::SELECT);
    TNetworkAddress address;
    address.hostname = "127.0.0.1";
    address.port = 9030;
    auto query = QueryContext::create(query_id, ExecEnv::GetInstance(), options, address, true,
                                      address, QuerySource::INTERNAL_FRONTEND);
    auto pool = make_paimon_query_memory_pool(query->resource_ctx(), 1024);
    const double old_probability = config::mem_alloc_fault_probability;
    Defer restore {[&] { config::mem_alloc_fault_probability = old_probability; }};
    config::mem_alloc_fault_probability = 0;
    auto* ptr = static_cast<unsigned char*>(pool->Malloc(64, 64));
    memset(ptr, 0x5a, 64);
    EXPECT_EQ(64, pool->CurrentUsage());
    EXPECT_THROW(pool->Malloc(2048), std::bad_alloc);
    EXPECT_THROW(pool->Realloc(ptr, 64, 2048), std::bad_alloc);
    EXPECT_EQ(64, pool->CurrentUsage());

    // Exercise Doris Allocator itself (MEM_ALLOC_FAILED), not a mocked SDK Status.
    config::mem_alloc_fault_probability = 1.0;
    EXPECT_THROW(pool->Malloc(64), std::bad_alloc);
    EXPECT_THROW(pool->Realloc(ptr, 64, 128), std::bad_alloc);
    config::mem_alloc_fault_probability = 0;
    EXPECT_EQ(64, pool->CurrentUsage());
    for (size_t i = 0; i < 64; ++i) EXPECT_EQ(0x5a, ptr[i]);
    ptr = static_cast<unsigned char*>(pool->Realloc(ptr, 64, 128));
    for (size_t i = 0; i < 64; ++i) EXPECT_EQ(0x5a, ptr[i]);
    EXPECT_EQ(128, pool->CurrentUsage());
    EXPECT_EQ(192, pool->MaxMemoryUsage()); // old + new coexist during realloc

    // SDK workers may perform the final release on a different thread.
    std::thread worker([pool, ptr] { pool->Free(ptr, 128); });
    worker.join();
    EXPECT_EQ(0, pool->CurrentUsage());
    auto next_writer_pool = make_paimon_query_memory_pool(query->resource_ctx(), 1024);
    void* next = next_writer_pool->Malloc(64);
    next_writer_pool->Free(next, 64);
    EXPECT_EQ(0, next_writer_pool->CurrentUsage());
}

} // namespace doris
#endif
