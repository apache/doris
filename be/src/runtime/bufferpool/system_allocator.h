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

#ifndef IMPALA_RUNTIME_SYSTEM_ALLOCATOR_H
#define IMPALA_RUNTIME_SYSTEM_ALLOCATOR_H

#include "common/status.h"
#include "runtime/bufferpool/buffer_pool.h"

namespace doris {

/// The underlying memory allocator for the buffer pool that allocates buffer memory from
/// the operating system using mmap(). All buffers are allocated through the BufferPool's
/// SystemAllocator. The allocator only handles allocating buffers that are power-of-two
/// multiples of the minimum buffer length.
class SystemAllocator {
public:
    SystemAllocator(int64_t min_buffer_len);

    /// Allocate memory for a buffer of 'len' bytes. 'len' must be a power-of-two multiple
    /// of the minimum buffer length.
    Status Allocate(int64_t len, BufferPool::BufferHandle* buffer) WARN_UNUSED_RESULT;

    /// Free the memory for a previously-allocated buffer.
    void Free(BufferPool::BufferHandle&& buffer);

private:
    /// Allocate 'len' bytes of memory for a buffer via mmap().
    Status AllocateViaMMap(int64_t len, uint8_t** buffer_mem);

    /// Allocate 'len' bytes of memory for a buffer via our malloc implementation.
    Status AllocateViaMalloc(int64_t len, uint8_t** buffer_mem);

    const int64_t min_buffer_len_;
};
} // namespace doris

#endif
