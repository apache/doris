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

#include "storage/index/snii/bkd/point_sorter.h"

#include <algorithm>
#include <array>
#include <cstring>

#include "common/check.h"

namespace doris::snii::bkd::point_sorter {
namespace {

// Buckets at or below this size go to the comparison fallback: under it a radix
// pass spends more time zeroing and walking a 256-entry histogram than a
// comparison sort spends on the whole bucket. Purely a tuning knob -- the sorted
// output is byte-identical either way and nothing on disk depends on it.
constexpr size_t kIntroThreshold = 64;

// One bucket per byte value. There is no extra "key exhausted" bucket the way a
// variable-length radix sort needs: every record is exactly record_size bytes
// (INV-2), so at any level every record still has a byte.
constexpr size_t kBucketCount = 256;

uint8_t* record_at(uint8_t* records, size_t index, uint32_t record_size) {
    return records + index * static_cast<size_t>(record_size);
}

// Comparison fallback for one small bucket. Everything in the bucket already
// shares bytes [0, level) -- that is what put it in the same bucket -- so the
// compare starts at `level` instead of re-reading the common prefix per compare.
void sort_small(uint8_t* records, size_t count, uint32_t record_size, uint32_t level) {
    DCHECK_LE(count, kIntroThreshold);
    // std::sort needs a movable element, which a fixed-width slice of a byte
    // array is not, so what gets sorted is the bucket's index permutation. It is
    // bounded by the threshold, hence a stack array rather than an allocation.
    std::array<uint32_t, kIntroThreshold> order;
    for (size_t i = 0; i < count; ++i) {
        order[i] = static_cast<uint32_t>(i);
    }
    const size_t width = record_size - level;
    std::sort(order.begin(), order.begin() + count, [&](uint32_t lhs, uint32_t rhs) {
        return std::memcmp(record_at(records, lhs, record_size) + level,
                           record_at(records, rhs, record_size) + level, width) < 0;
    });

    // Apply the permutation in place -- no scratch copy of the bucket, per the
    // in-place requirement in the header. order[target] is where the record that
    // belongs at `target` STARTED. Positions below `target` are already final, so
    // a source below it has since been overwritten; following the chain through
    // those settled positions lands on wherever that record was displaced to.
    // Records that compare equal are byte-identical (the whole record is the key),
    // so which of them the chain picks cannot be observed.
    for (size_t target = 0; target < count; ++target) {
        size_t source = order[target];
        while (source < target) {
            source = order[source];
        }
        if (source != target) {
            std::swap_ranges(record_at(records, target, record_size),
                             record_at(records, target + 1, record_size),
                             record_at(records, source, record_size));
        }
    }
}

// MSB radix sort of one bucket, distinguishing records from byte `level` on.
//
// The single-bucket case loops instead of recursing, so a long common prefix (all
// eight value bytes equal, which is the norm for a leaf of one repeated value)
// descends without a stack frame per byte. Only a real split recurses, which caps
// the live frame count at record_size.
void radix_sort(uint8_t* records, size_t count, uint32_t record_size, uint32_t level) {
    while (true) {
        if (count <= 1) {
            return;
        }
        if (level == record_size) {
            // Every byte of every record here compared equal, so they are
            // byte-identical and the arrangement they are in already IS sorted.
            return;
        }
        if (count <= kIntroThreshold) {
            sort_small(records, count, record_size, level);
            return;
        }

        // bucket_start[b] .. bucket_start[b + 1] is where bucket b ends up.
        // Counting one slot to the right lets the prefix sum run in place, so the
        // per-level stack cost is this array plus the cursors and nothing else.
        size_t bucket_start[kBucketCount + 1] = {};
        for (size_t i = 0; i < count; ++i) {
            ++bucket_start[record_at(records, i, record_size)[level] + 1];
        }
        const uint8_t first_byte = record_at(records, 0, record_size)[level];
        if (bucket_start[first_byte + 1] == count) {
            ++level;
            continue;
        }
        for (size_t bucket = 0; bucket < kBucketCount; ++bucket) {
            bucket_start[bucket + 1] += bucket_start[bucket];
        }

        // American flag sort. cursor[b] is the next unfilled slot of bucket b, so
        // every swap puts one record where it will stay: the pass costs at most
        // `count` swaps and, unlike a counting sort, no second copy of the array.
        size_t cursor[kBucketCount];
        for (size_t bucket = 0; bucket < kBucketCount; ++bucket) {
            cursor[bucket] = bucket_start[bucket];
        }
        for (size_t bucket = 0; bucket < kBucketCount; ++bucket) {
            const size_t bucket_end = bucket_start[bucket + 1];
            while (cursor[bucket] < bucket_end) {
                uint8_t* record = record_at(records, cursor[bucket], record_size);
                const uint8_t target = record[level];
                if (target == bucket) {
                    ++cursor[bucket];
                    continue;
                }
                // This record belongs to `target` yet is sitting outside it, so
                // bucket `target` cannot be full: the swap always has a slot.
                DCHECK_LT(cursor[target], bucket_start[target + 1]);
                std::swap_ranges(record, record + record_size,
                                 record_at(records, cursor[target], record_size));
                ++cursor[target];
                // cursor[bucket] deliberately does not advance: the record just
                // swapped in is unplaced and gets classified on the next turn.
            }
        }

        for (size_t bucket = 0; bucket < kBucketCount; ++bucket) {
            const size_t bucket_size = bucket_start[bucket + 1] - bucket_start[bucket];
            if (bucket_size > 1) {
                radix_sort(record_at(records, bucket_start[bucket], record_size), bucket_size,
                           record_size, level + 1);
            }
        }
        return;
    }
}

} // namespace

void sort(uint8_t* records, size_t count, uint32_t record_size) {
    DORIS_CHECK_GT(record_size, 0U);
    if (count <= 1) {
        // Nothing to permute. This precedes the pointer assertion because an
        // empty run legitimately has no buffer to point at.
        return;
    }
    DORIS_CHECK(records != nullptr);
    radix_sort(records, count, record_size, 0);
}

} // namespace doris::snii::bkd::point_sorter
