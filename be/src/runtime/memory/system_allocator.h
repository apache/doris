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
#include <cstdint>

namespace doris {

// Allocate memory from system allocator, this allocator can be configured
// to allocate memory via mmap or malloc.
class SystemAllocator {
public:
    static uint8_t* allocate(size_t length);

    static void free(uint8_t* ptr, size_t length);

private:
    static uint8_t* allocate_via_mmap(size_t length);
    static uint8_t* allocate_via_malloc(size_t length);
};

} // namespace doris
