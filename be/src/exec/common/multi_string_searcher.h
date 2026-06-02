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
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include "core/string_ref.h"
#include "core/types.h"
#include "exec/common/string_searcher.h"

namespace doris {

// Searches many literal needles in one haystack. The table stores 2-byte ngrams from a
// batch of needles, so scanning the haystack can find candidates for many needles at once.
class MultiStringSearcher {
    using Offset = uint8_t;
    using Id = uint8_t;
    using Ngram = uint16_t;

    // One table entry is one candidate ngram occurrence in a needle. It stores the global
    // needle id and the 1-based offset of this ngram inside that needle. off == 0 means
    // the slot is empty, so needle offsets are stored as 1-based values.
    struct OffsetId {
        Id id = 0;
        Offset off = 0;
    };

public:
    explicit MultiStringSearcher(const std::vector<StringRef>& needles)
            : _needles(needles), _hash(new OffsetId[HASH_SIZE]) {
        _searchers.reserve(_needles.size());
        for (const auto& needle : _needles) {
            _searchers.emplace_back(needle.data, needle.size);
        }
    }

    bool has_more_to_search() {
        if (_last == _needles.size()) {
            return false;
        }

        std::memset(_hash.get(), 0, HASH_SIZE * sizeof(OffsetId));
        _fallback_needles.clear();
        _step = std::numeric_limits<size_t>::max();

        // Not all needles are necessarily inserted in one table. Keeping the load factor
        // low makes the linear-probing chains short; if the current batch reaches
        // SMALL_LIMIT, the caller will scan all rows with this batch and call this method
        // again for the next batch. _last is the global needle index of the next batch.
        size_t ngrams = 0;
        for (; _last < _needles.size(); ++_last) {
            const auto& needle = _needles[_last];
            if (is_fallback_needle(needle.size)) {
                _fallback_needles.push_back(_last);
            } else {
                const size_t needle_ngrams = needle.size - sizeof(Ngram) + 1;
                if (ngrams + needle_ngrams > SMALL_LIMIT && ngrams != 0) {
                    break;
                }

                ngrams += needle_ngrams;
                _step = std::min(_step, needle_ngrams);
                // Insert ngrams from the end to the beginning, matching the way the scan
                // position is later converted back to the candidate needle start.
                for (auto i = static_cast<int>(needle.size - sizeof(Ngram)); i >= 0; --i) {
                    put_ngram(to_ngram(reinterpret_cast<const uint8_t*>(needle.data + i)), i + 1,
                              _last);
                }
            }
        }

        return true;
    }

    void search_one_all(const uint8_t* haystack, const uint8_t* haystack_end, Int32* answer) const {
        // answer points to the beginning of one result array row. Entries are indexed by
        // global needle id, so different batches write into the same full-size row result.
        for (const size_t needle_index : _fallback_needles) {
            const auto* match = _searchers[needle_index].search(haystack, haystack_end);
            if (match != haystack_end) {
                answer[needle_index] = static_cast<Int32>(match - haystack + 1);
            }
        }

        if (_step == std::numeric_limits<size_t>::max() ||
            haystack_end - haystack < static_cast<ptrdiff_t>(sizeof(Ngram))) {
            // The batch may contain only fallback needles, or the haystack may be too short
            // to read a 2-byte ngram. Fallback needles have already been handled above.
            return;
        }

        for (const uint8_t* pos = haystack + _step - sizeof(Ngram);
             pos <= haystack_end - sizeof(Ngram); pos += _step) {
            // A 2-byte ngram has exactly 65536 possible values. HASH_SIZE is also 65536,
            // so ngram % HASH_SIZE is effectively direct addressing. Collisions and
            // duplicate ngrams are kept by linear probing and checked until an empty slot.
            for (size_t cell = to_ngram(pos) % HASH_SIZE; _hash[cell].off;
                 cell = (cell + 1) % HASH_SIZE) {
                if (pos < haystack + _hash[cell].off - 1) {
                    continue;
                }

                const size_t needle_index = _hash[cell].id;
                // Convert the matched ngram position in the haystack back to the candidate
                // start position of the whole needle, then verify the full needle below.
                const uint8_t* match = pos - (_hash[cell].off - 1);
                const auto candidate_position = static_cast<Int32>(match - haystack + 1);
                if (answer[needle_index] != 0 && candidate_position >= answer[needle_index]) {
                    continue;
                }

                const auto& needle = _needles[needle_index];
                // The hash table only proves that one 2-byte ngram matched. Full memcmp is
                // required to discard false positives from duplicate ngrams and collisions.
                if (match + needle.size <= haystack_end &&
                    std::memcmp(match, needle.data, needle.size) == 0) {
                    answer[needle_index] = candidate_position;
                }
            }
        }
    }

private:
    static constexpr size_t HASH_SIZE = 64 * 1024;
    static constexpr size_t SMALL_LIMIT = HASH_SIZE / 8;

    static bool is_fallback_needle(size_t needle_size) {
        // Very short needles do not have enough 2-byte ngrams to be selective. Very long
        // needles cannot store their ngram offset in the uint8_t Offset field.
        return needle_size < 2 * sizeof(Ngram) || needle_size >= std::numeric_limits<Offset>::max();
    }

    static Ngram to_ngram(const uint8_t* pos) {
        // Read two bytes as a uint16_t. On little-endian machines, for example, "ab"
        // becomes 97 + 98 * 256. This numeric value is used as the initial hash slot.
        Ngram ngram;
        std::memcpy(&ngram, pos, sizeof(ngram));
        return ngram;
    }

    void put_ngram(Ngram ngram, int offset, size_t needle_index) {
        size_t cell = ngram % HASH_SIZE;
        // Linear probing keeps all duplicate ngrams instead of overwriting them. Search
        // uses the same probe sequence and validates every candidate with full memcmp.
        while (_hash[cell].off) {
            cell = (cell + 1) % HASH_SIZE;
        }
        _hash[cell] = {.id = static_cast<Id>(needle_index), .off = static_cast<Offset>(offset)};
    }

    const std::vector<StringRef>& _needles;
    std::vector<size_t> _fallback_needles;
    std::vector<ASCIICaseSensitiveStringSearcher> _searchers;
    std::unique_ptr<OffsetId[]> _hash;
    size_t _step = 0;
    size_t _last = 0;
};

} // namespace doris
