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

/**
  * This file contains some using-declarations that define various kinds of
  * PODArray.
  */
#pragma once

#include "vec/common/allocator_fwd.h"

namespace doris::vectorized {

inline constexpr size_t integerRoundUp(size_t value, size_t dividend) {
    return ((value + dividend - 1) / dividend) * dividend;
}

template <typename T, size_t initial_bytes = 4096, typename TAllocator = Allocator<false>,
          size_t pad_right_ = 0, size_t pad_left_ = 0>
class PODArray;

/** For columns. Padding is enough to read and write xmm-register at the address of the last element. */
template <typename T, size_t initial_bytes = 4096, typename TAllocator = Allocator<false>>
using PaddedPODArray = PODArray<T, initial_bytes, TAllocator, 15, 16>;

/** A helper for declaring PODArray that uses inline memory.
  * The initial size is set to use all the inline bytes, since using less would
  * only add some extra allocation calls.
  */
template <typename T, size_t inline_bytes,
          size_t rounded_bytes = integerRoundUp(inline_bytes, sizeof(T))>
using PODArrayWithStackMemory =
        PODArray<T, rounded_bytes,
                 AllocatorWithStackMemory<Allocator<false>, rounded_bytes, alignof(T)>>;

} // namespace doris::vectorized
