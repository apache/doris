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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ArenaWithFreeLists.h
// and modified by Doris

#pragma once

#include "vec/common/arena.h"
#if __has_include(<sanitizer/asan_interface.h>)
#include <sanitizer/asan_interface.h>
#endif
#include "vec/common/bit_helpers.h"

namespace doris::vectorized {

class ArenaWithFreeLists : private Allocator<false>, private boost::noncopyable {
private:
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wzero-length-array"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wzero-length-bounds"
#endif

    union Block {
        Block* next;
        char data[0];
    };

#if defined(__clang__)
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

    static constexpr size_t max_fixed_block_size = 65536;

    static size_t find_free_list_index(const size_t size) {
        return size <= 8 ? 2 : bit_scan_reverse(size - 1);
    }

    Arena pool;

    Block* free_lists[16] {};

public:
    explicit ArenaWithFreeLists(const size_t initial_size = 4096, const size_t growth_factor = 2,
                                const size_t linear_growth_threshold = 128 * 1024 * 1024)
            : pool {initial_size, growth_factor, linear_growth_threshold} {}

    char* alloc(const size_t size) {
        if (size > max_fixed_block_size) {
            return static_cast<char*>(Allocator<false>::alloc(size));
        }

        const auto list_idx = find_free_list_index(size);

        if (auto& free_block_ptr = free_lists[list_idx]) {
            ASAN_UNPOISON_MEMORY_REGION(free_block_ptr, std::max(size, sizeof(Block)));

            auto* const res = free_block_ptr->data;
            free_block_ptr = free_block_ptr->next;
            return res;
        }

        return pool.alloc(1ULL << (list_idx + 1));
    }

    void free(char* ptr, const size_t size) {
        if (size > max_fixed_block_size) {
            Allocator<false>::free(ptr, size);
            return;
        }

        const auto list_idx = find_free_list_index(size);

        auto& free_block_ptr = free_lists[list_idx];
        auto* const old_head = free_block_ptr;
        free_block_ptr = reinterpret_cast<Block*>(ptr);
        free_block_ptr->next = old_head;

        ASAN_POISON_MEMORY_REGION(ptr, 1ULL << (list_idx + 1));
    }

    size_t allocated_bytes() const { return pool.size(); }
};

} // namespace doris::vectorized