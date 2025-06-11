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

// This file is based on code available under the Apache license here:
//   https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/Volnitsky.h

#pragma once

#include <algorithm>
#include <functional>

namespace doris {

/** Search for a substring in a string by Volnitsky's algorithm
  * http://volnitsky.com/project/str_search/
  *
  * `haystack` and `needle` can contain zero bytes.
  *
  * Algorithm:
  * - if the `needle` is too small or too large, or too small `haystack`, use std::search or memchr;
  * - when initializing, fill in an open-addressing linear probing hash table of the form
  *    hash from the bigram of needle -> the position of this bigram in needle + 1.
  *    (one is added only to distinguish zero offset from an empty cell)
  * - the keys are not stored in the hash table, only the values are stored;
  * - bigrams can be inserted several times if they occur in the needle several times;
  * - when searching, take from haystack bigram, which should correspond to the last bigram of needle (comparing from the end);
  * - look for it in the hash table, if found - get the offset from the hash table and compare the string bytewise;
  * - if it did not match, we check the next cell of the hash table from the collision resolution chain;
  * - if not found, skip to haystack almost the size of the needle bytes;
  *
  * MultiVolnitsky - search for multiple substrings in a string:
  * - Add bigrams to hash table with string index. Then the usual Volnitsky search is used.
  * - We are adding while searching, limiting the number of fallback searchers and the total number of added bigrams
  */

namespace VolnitskyTraits {
using Offset =
        uint8_t; /// Offset in the needle. For the basic algorithm, the length of the needle must not be greater than 255.
using Id =
        uint8_t; /// Index of the string (within the array of multiple needles), must not be greater than 255.
using Ngram = uint16_t; /// n-gram (2 bytes).

/** Fits into the L2 cache (of common Intel CPUs).
     * This number is extremely good for compilers as it is numeric_limits<Uint16>::max() and there are optimizations with movzwl and other instructions with 2 bytes
     */
static constexpr size_t hash_size = 64 * 1024;

/// min haystack size to use main algorithm instead of fallback
static constexpr size_t min_haystack_size_for_algorithm = 20000;

static inline bool isFallbackNeedle(const size_t needle_size, size_t haystack_size_hint = 0) {
    return needle_size < 2 * sizeof(Ngram) || needle_size >= std::numeric_limits<Offset>::max() ||
           (haystack_size_hint && haystack_size_hint < min_haystack_size_for_algorithm);
}

static inline Ngram toNGram(const uint8_t* const pos) {
    Ngram res;
    memcpy(&res, pos, sizeof(res));
    return res;
}

template <typename Callback>
static inline void putNGram(const uint8_t* const pos, const int offset,
                            const Callback& putNGramBase) {
    putNGramBase(toNGram(pos), offset);
}
} // namespace VolnitskyTraits

/// @todo store lowercase needle to speed up in case there are numerous occurrences of bigrams from needle in haystack
template <typename FallbackSearcher>
class VolnitskyBase {
protected:
    const uint8_t* const needle;
    const size_t needle_size;
    const uint8_t* const needle_end = needle + needle_size;
    /// For how long we move, if the n-gram from haystack is not found in the hash table.
    const size_t step = needle_size - sizeof(VolnitskyTraits::Ngram) + 1;

    /** max needle length is 255, max distinct ngrams for case-sensitive is (255 - 1), case-insensitive is 4 * (255 - 1)
      *  storage of 64K ngrams (n = 2, 128 KB) should be large enough for both cases */
    std::unique_ptr<VolnitskyTraits::Offset[]> hash; /// Hash table.

    const bool fallback; /// Do we need to use the fallback algorithm.

    FallbackSearcher fallback_searcher;

public:
    using Searcher = FallbackSearcher;

    /** haystack_size_hint - the expected total size of the haystack for `search` calls. Optional (zero means unspecified).
      * If you specify it small enough, the fallback algorithm will be used,
      *  since it is considered that it's useless to waste time initializing the hash table.
      */
    VolnitskyBase(const char* const needle_, const size_t needle_size_,
                  size_t haystack_size_hint = 0)
            : needle {reinterpret_cast<const uint8_t*>(needle_)},
              needle_size {needle_size_},
              fallback {VolnitskyTraits::isFallbackNeedle(needle_size, haystack_size_hint)},
              fallback_searcher {needle_, needle_size} {
        //std::cout << "fallback: " << fallback << std::endl;
        if (fallback) return;

        hash = std::unique_ptr<VolnitskyTraits::Offset[]>(
                new VolnitskyTraits::Offset[VolnitskyTraits::hash_size] {});

        auto callback = [this](const VolnitskyTraits::Ngram ngram, const int offset) {
            return this->putNGramBase(ngram, offset);
        };
        /// ssize_t is used here because unsigned can't be used with condition like `i >= 0`, unsigned always >= 0
        /// And also adding from the end guarantees that we will find first occurrence because we will lookup bigger offsets first.
        for (auto i = static_cast<ssize_t>(needle_size - sizeof(VolnitskyTraits::Ngram)); i >= 0;
             --i)
            VolnitskyTraits::putNGram(this->needle + i, i + 1, callback);
    }

    /// If not found, the end of the haystack is returned.
    const uint8_t* search(const uint8_t* const haystack, const size_t haystack_size) const {
        if (needle_size == 0) return haystack;

        const auto haystack_end = haystack + haystack_size;

        if (fallback || haystack_size <= needle_size)
            return fallback_searcher.search(haystack, haystack_end);

        /// Let's "apply" the needle to the haystack and compare the n-gram from the end of the needle.
        const auto* pos = haystack + needle_size - sizeof(VolnitskyTraits::Ngram);
        for (; pos <= haystack_end - needle_size; pos += step) {
            /// We look at all the cells of the hash table that can correspond to the n-gram from haystack.
            for (size_t cell_num = VolnitskyTraits::toNGram(pos) % VolnitskyTraits::hash_size;
                 hash[cell_num]; cell_num = (cell_num + 1) % VolnitskyTraits::hash_size) {
                /// When found - compare bytewise, using the offset from the hash table.
                const auto res = pos - (hash[cell_num] - 1);

                /// pointer in the code is always padded array so we can use pagesafe semantics
                if (fallback_searcher.compare(haystack, haystack_end, (uint8_t*)res)) return res;
            }
        }

        return fallback_searcher.search(pos - step + 1, haystack_end);
    }

    const char* search(const char* haystack, size_t haystack_size) const {
        return reinterpret_cast<const char*>(
                search(reinterpret_cast<const uint8_t*>(haystack), haystack_size));
    }

    const uint8_t* search(const uint8_t* haystack, const uint8_t* haystack_end) const {
        return search(haystack, haystack_end - haystack);
    }

protected:
    void putNGramBase(const VolnitskyTraits::Ngram ngram, const int offset) {
        /// Put the offset for the n-gram in the corresponding cell or the nearest free cell.
        size_t cell_num = ngram % VolnitskyTraits::hash_size;

        while (hash[cell_num]) {
            cell_num =
                    (cell_num + 1) % VolnitskyTraits::hash_size; /// Search for the next free cell.
        }

        hash[cell_num] = offset;
    }
};
} // namespace doris
