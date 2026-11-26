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

#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>

#include "core/custom_allocator.h"

namespace doris {

// Sorts [first, last) with a comparator that cannot be trusted to be a strict weak ordering,
// for example one evaluated from user SQL. Such a comparator may report `less(a, a)`, may
// report both `less(a, b)` and `less(b, a)`, and may even answer differently when asked the
// same question twice.
//
// Standard library sorts (std::sort, std::make_heap/std::sort_heap, ...) require a strict weak
// ordering and are free to read outside the range, loop forever or abort when it is violated.
// This routine is a bottom-up merge sort in which every loop bound and every access is derived
// from the range length alone; the comparator only decides which of two in-range elements is
// copied next. Therefore, for ANY comparator:
//   - it makes at most n * ceil(log2(n)) comparator calls,
//   - it never accesses memory outside [first, last) and `scratch`,
//   - the result is a permutation of the input.
// When the comparator is a strict weak ordering the result is sorted, and the sort is stable.
//
// `scratch` is caller-owned so that repeated calls can reuse its allocation. If the comparator
// throws, the exception propagates and the contents of [first, last) are unspecified.
template <typename T, typename Less>
void sort_with_untrusted_comparator(T* first, T* last, DorisVector<T>& scratch, Less&& less) {
    const size_t n = last - first;
    scratch.resize(n);

    T* src = first;
    T* dst = scratch.data();
    for (size_t width = 1; width < n; width *= 2) {
        for (size_t lo = 0; lo < n; lo += 2 * width) {
            const size_t mid = std::min(lo + width, n);
            const size_t hi = std::min(lo + 2 * width, n);
            // Runs that are already in order are copied without merging. This is what makes an
            // already sorted input cost O(n) comparator calls instead of O(n log n).
            if (mid == hi || !less(src[mid], src[mid - 1])) {
                std::copy(src + lo, src + hi, dst + lo);
                continue;
            }
            size_t i = lo;
            size_t j = mid;
            size_t k = lo;
            while (i < mid && j < hi) {
                // The right run wins only when strictly less, so equal elements keep their
                // relative order.
                if (less(src[j], src[i])) {
                    dst[k++] = src[j++];
                } else {
                    dst[k++] = src[i++];
                }
            }
            std::copy(src + i, src + mid, dst + k);
            k += mid - i;
            std::copy(src + j, src + hi, dst + k);
        }
        std::swap(src, dst);
    }
    if (src != first) {
        std::copy(src, src + n, first);
    }
}

} // namespace doris
