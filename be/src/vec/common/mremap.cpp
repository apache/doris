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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/base/base/mremap.cpp
// and modified by Doris

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <errno.h>

#include "vec/common/mremap.h"

void* mremap_fallback(void* old_address, size_t old_size, size_t new_size, int flags, int mmap_prot,
                      int mmap_flags, int mmap_fd, off_t mmap_offset) {
    /// No actual shrink
    if (new_size < old_size) return old_address;

    if (!(flags & MREMAP_MAYMOVE)) {
        errno = ENOMEM;
        return MAP_FAILED;
    }

    void* new_address = mmap(nullptr, new_size, mmap_prot, mmap_flags, mmap_fd, mmap_offset);
    if (MAP_FAILED == new_address) return MAP_FAILED;

    memcpy(new_address, old_address, old_size);

    if (munmap(old_address, old_size)) abort();

    return new_address;
}
