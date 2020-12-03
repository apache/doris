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

#include "runtime/memory/chunk_allocator.h"

#include <gtest/gtest.h>

#include "common/config.h"
#include "runtime/memory/chunk.h"
#include "util/cpu_info.h"
#include "util/doris_metrics.h"

namespace doris {

TEST(ChunkAllocatorTest, Normal) {
    config::use_mmap_allocate_chunk = true;
    for (size_t size = 4096; size <= 1024 * 1024; size <<= 1) {
        Chunk chunk;
        ASSERT_TRUE(ChunkAllocator::instance()->allocate(size, &chunk));
        ASSERT_NE(nullptr, chunk.data);
        ASSERT_EQ(size, chunk.size);
        ChunkAllocator::instance()->free(chunk);
    }
}
} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    doris::ChunkAllocator::init_instance(1024 * 1024);
    return RUN_ALL_TESTS();
}
