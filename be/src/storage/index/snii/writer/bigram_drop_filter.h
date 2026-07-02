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
#include <string_view>
#include <vector>

namespace doris::snii::writer {

// EVER-DROPPED bloom over evicted phrase-bigram terms (G04 "bigram diet"
// phase 2). One instance lives per SpimiTermBuffer; every bigram term the
// vocab-cap eviction drops mid-build is inserted here, and at flush the
// writer's process_term drops any bigram term the filter says it MAY have
// evicted -- IN ADDITION to the G01 df threshold. The direction of error is
// one-sided by design:
//   * a FALSE POSITIVE only OVER-drops a never-evicted bigram, which is safe
//     under the G01 reader contract (a bigram dict miss on a segment whose
//     meta declares pruning falls back to the generic positions phrase path,
//     same results, just slower);
//   * a false NEGATIVE is impossible (a bloom never forgets an insertion), so
//     an evicted term -- whose surviving postings would be INCOMPLETE (they
//     miss the docids dropped with the eviction) -- can never materialize.
//
// SIZING: ~kBitsPerEntry (10) bits per expected eviction, kNumHashes (4)
// probes -> ~1.2% per-partition false-positive rate at design fill. The total
// eviction count is not knowable up front (it tracks the corpus' unique-pair
// tail, not any config), so the filter is SCALABLE: partitions of
// geometrically doubling capacity are chained as insertions accumulate; a
// query probes every partition (compound FPP ~= partition count * 1.2%, and
// every extra hit is still only a safe over-drop). The first partition is
// sized by the caller from the vocab cap (roughly the live-term count the cap
// can hold), so the common case is one partition.
//
// Keys are the RAW SYNTHETIC TERM BYTES (make_phrase_bigram_term's
// marker+varint+left+right layout) hashed with the same FNV-1a 64 the SPIMI
// intern table uses, so insert (at eviction, from the owned vocab string) and
// probe (at flush, from TermPostings::term) agree byte-for-byte.
//
// insert() is CHECK-THEN-INSERT: a key the filter already reports as present
// consumes no new bits and does not advance the partition fill count, so a
// pair that is evicted, re-interned, and evicted again (each eviction
// re-inserting the same bytes) never double-counts toward partition growth.
//
// Single-threaded (one writer build path), like the rest of the SPIMI writer.
class BigramDropFilter {
public:
    static constexpr uint32_t kBitsPerEntry = 10;
    static constexpr uint32_t kNumHashes = 4;

    // `initial_capacity` is the first partition's design insertion count (the
    // caller derives it from the vocab cap); clamped to a small floor so a
    // degenerate cap still yields a usable filter.
    explicit BigramDropFilter(uint64_t initial_capacity)
            : next_capacity_(initial_capacity < kMinPartitionCapacity ? kMinPartitionCapacity
                                                                      : initial_capacity) {}

    BigramDropFilter(const BigramDropFilter&) = delete;
    BigramDropFilter& operator=(const BigramDropFilter&) = delete;

    // Records `term` as ever-dropped. No-op (and no bit writes) when the filter
    // already reports the key as maybe-present.
    void insert(std::string_view term) {
        if (maybe_contains(term)) {
            return;
        }
        if (partitions_.empty() || partitions_.back().inserted >= partitions_.back().capacity) {
            add_partition();
        }
        Partition& p = partitions_.back();
        const uint64_t h = fnv1a64(term);
        const uint64_t h2 = (h >> 32) | 1ULL; // odd step (Kirsch-Mitzenmacher)
        for (uint32_t i = 0; i < kNumHashes; ++i) {
            const uint64_t bit = (h + static_cast<uint64_t>(i) * h2) % p.n_bits;
            p.bits[bit >> 6] |= (1ULL << (bit & 63));
        }
        ++p.inserted;
        ++insertions_;
    }

    // May this term have been evicted? False negatives never occur; a false
    // positive only over-drops (safe, see header comment).
    bool maybe_contains(std::string_view term) const {
        if (partitions_.empty()) {
            return false;
        }
        const uint64_t h = fnv1a64(term);
        const uint64_t h2 = (h >> 32) | 1ULL;
        for (const Partition& p : partitions_) {
            bool all = true;
            for (uint32_t i = 0; i < kNumHashes; ++i) {
                const uint64_t bit = (h + static_cast<uint64_t>(i) * h2) % p.n_bits;
                if ((p.bits[bit >> 6] & (1ULL << (bit & 63))) == 0) {
                    all = false;
                    break;
                }
            }
            if (all) {
                return true;
            }
        }
        return false;
    }

    // Distinct-ish insertions accepted (check-then-insert dedups repeats).
    uint64_t insertions() const { return insertions_; }
    bool empty() const { return insertions_ == 0; }
    size_t partition_count() const { return partitions_.size(); }

    // Resident filter bytes (the bitsets); observability / RSS accounting.
    uint64_t allocated_bytes() const {
        uint64_t total = 0;
        for (const Partition& p : partitions_) {
            total += p.bits.size() * sizeof(uint64_t);
        }
        return total;
    }

private:
    static constexpr uint64_t kMinPartitionCapacity = 4096;

    struct Partition {
        std::vector<uint64_t> bits;
        uint64_t n_bits = 0;
        uint64_t capacity = 0; // design insertion count for this partition
        uint64_t inserted = 0;
    };

    // Same FNV-1a 64 constants/byte order as the SPIMI intern hash, so the
    // composed-string key hashed here equals the piecewise intern hash of the
    // same pair (identical byte sequence through the identical update).
    static uint64_t fnv1a64(std::string_view bytes) noexcept {
        uint64_t h = 1469598103934665603ULL;
        for (const char c : bytes) {
            h ^= static_cast<unsigned char>(c);
            h *= 1099511628211ULL;
        }
        return h;
    }

    void add_partition() {
        Partition p;
        p.capacity = next_capacity_;
        next_capacity_ *= 2; // geometric growth: few partitions even at huge tails
        p.n_bits = p.capacity * kBitsPerEntry;
        // Round up to whole 64-bit words; n_bits is the modulus so keep it exact
        // to the allocated word count (no phantom always-zero tail bits).
        const uint64_t words = (p.n_bits + 63) / 64;
        p.n_bits = words * 64;
        p.bits.assign(static_cast<size_t>(words), 0);
        partitions_.push_back(std::move(p));
    }

    std::vector<Partition> partitions_;
    uint64_t next_capacity_ = kMinPartitionCapacity;
    uint64_t insertions_ = 0;
};

} // namespace doris::snii::writer
