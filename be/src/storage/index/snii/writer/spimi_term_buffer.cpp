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

#include "storage/index/snii/writer/spimi_term_buffer.h"

#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include "common/exception.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_word_set.h"
#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/writer/global_memory_limiter.h"
#include "storage/index/snii/writer/spill_run_codec.h"
#include "storage/index/snii/writer/temp_dir.h"

#if defined(__GLIBC__)
#include <malloc.h>
#endif

namespace doris::snii::writer {

namespace {

constexpr size_t kCommonGramPairKeySize = 10;
constexpr char kCommonGramPairKeyTag = 'P';

std::array<char, kCommonGramPairKeySize> EncodeCommonGramPairKey(PlainTermId left,
                                                                 PlainTermId right) {
    std::array<char, kCommonGramPairKeySize> key {};
    key[0] = segment_v2::inverted_index::INTERNAL_TERM_NAMESPACE_BEGIN.front();
    key[1] = kCommonGramPairKeyTag;
    for (size_t byte = 0; byte < sizeof(uint32_t); ++byte) {
        const size_t shift = (sizeof(uint32_t) - byte - 1) * 8;
        key[2 + byte] = static_cast<char>((left.value >> shift) & 0xffU);
        key[6 + byte] = static_cast<char>((right.value >> shift) & 0xffU);
    }
    return key;
}

bool is_common_gram_pair_key(std::string_view key) {
    return key.size() == kCommonGramPairKeySize &&
           key.front() == segment_v2::inverted_index::INTERNAL_TERM_NAMESPACE_BEGIN.front() &&
           key[1] == kCommonGramPairKeyTag;
}

struct CommonGramPairIds {
    PlainTermId left;
    PlainTermId right;
};

uint32_t decode_big_endian_uint32_unchecked(const char* bytes) {
    uint32_t value = 0;
    for (size_t byte = 0; byte < sizeof(uint32_t); ++byte) {
        value = (value << 8) | static_cast<uint8_t>(bytes[byte]);
    }
    return value;
}

#ifdef BE_TEST
std::atomic<uint64_t> g_common_gram_pair_unchecked_decodes {0};
std::atomic<uint64_t> g_common_gram_trusted_plain_decodes {0};
std::atomic<uint64_t> g_common_gram_pair_cache_probes {0};
std::atomic<uint64_t> g_common_gram_pair_cache_pair_hits {0};
std::atomic<uint64_t> g_common_gram_pair_cache_same_doc_hits {0};
std::atomic<uint64_t> g_common_gram_native_pair_probes {0};
std::atomic<uint64_t> g_common_gram_native_pair_hits {0};
std::atomic<uint64_t> g_common_gram_native_pair_inserts {0};
std::atomic<uint64_t> g_common_gram_logical_validations {0};
std::atomic<uint64_t> g_common_gram_plain_cache_probes {0};
std::atomic<uint64_t> g_common_gram_plain_cache_hits {0};
std::atomic<uint64_t> g_common_gram_plain_intern_table_probes {0};
std::atomic<uint64_t> g_owned_term_full_byte_comparisons {0};
std::atomic<bool> g_fail_next_owned_term_reserve {false};
std::atomic<bool> g_fail_next_owned_term_emplace {false};
std::atomic<uint64_t> g_spill_gate_checks {0};
std::atomic<uint64_t> g_compact_chain_varint_decodes {0};
#endif

CommonGramPairIds decode_common_gram_pair_key_unchecked(std::string_view key) {
    DCHECK(is_common_gram_pair_key(key));
#ifdef BE_TEST
    g_common_gram_pair_unchecked_decodes.fetch_add(1, std::memory_order_relaxed);
#endif
    return {
            .left = PlainTermId {.value = decode_big_endian_uint32_unchecked(key.data() + 2)},
            .right = PlainTermId {.value = decode_big_endian_uint32_unchecked(key.data() + 6)},
    };
}

class LogicalPlainKeyView {
public:
    explicit LogicalPlainKeyView(std::string_view physical) : physical_(physical) {
        escaped_ = !physical.empty() &&
                   physical.front() == segment_v2::inverted_index::PLAIN_ESCAPE_PREFIX;
        if (escaped_) {
            DCHECK_GE(physical.size(), 2U);
            DCHECK(physical[1] == 'E' || physical[1] == 'G');
        }
    }

    size_t size() const { return physical_.size() - (escaped_ ? 1 : 0); }

    uint8_t operator[](size_t index) const {
        DCHECK_LT(index, size());
        if (!escaped_) {
            return static_cast<uint8_t>(physical_[index]);
        }
        if (index == 0) {
            return static_cast<uint8_t>(
                    physical_[1] == 'E' ? segment_v2::inverted_index::PLAIN_ESCAPE_PREFIX : '\x1f');
        }
        return static_cast<uint8_t>(physical_[index + 1]);
    }

private:
    std::string_view physical_;
    bool escaped_ = false;
};

std::string_view decode_logical_plain_term_trusted(std::string_view physical,
                                                   std::string* scratch) {
    DCHECK(scratch != nullptr);
#ifdef BE_TEST
    g_common_gram_trusted_plain_decodes.fetch_add(1, std::memory_order_relaxed);
#endif
    scratch->clear();
    if (physical.empty() || physical.front() != segment_v2::inverted_index::PLAIN_ESCAPE_PREFIX) {
        DCHECK(!segment_v2::inverted_index::is_internal_term_key(physical));
        return physical;
    }

    DCHECK_GE(physical.size(), 2U);
    DCHECK(physical[1] == 'E' || physical[1] == 'G');
    scratch->reserve(physical.size() - 1);
    scratch->push_back(physical[1] == 'E'
                               ? segment_v2::inverted_index::PLAIN_ESCAPE_PREFIX
                               : segment_v2::inverted_index::INTERNAL_TERM_NAMESPACE_BEGIN.front());
    scratch->append(physical.substr(2));
    return std::string_view(*scratch);
}

int compare_logical_plain_keys(const LogicalPlainKeyView& left, const LogicalPlainKeyView& right) {
    const size_t common = std::min(left.size(), right.size());
    for (size_t i = 0; i < common; ++i) {
        if (left[i] != right[i]) {
            return left[i] < right[i] ? -1 : 1;
        }
    }
    if (left.size() == right.size()) {
        return 0;
    }
    return left.size() < right.size() ? -1 : 1;
}

} // namespace

struct SpimiTermBuffer::CommonGramPairCache {
    struct Entry {
        uint64_t pair = 0;
        uint32_t term_id = std::numeric_limits<uint32_t>::max();
        uint32_t last_docid = 0;
    };
    static_assert(sizeof(Entry) == 16);

    static constexpr size_t kEntryCount = 1024;
    static constexpr uint64_t kHashMultiplier = 11400714819323198485ULL;
    static constexpr uint32_t kInvalidTermId = std::numeric_limits<uint32_t>::max();

    static size_t index(uint64_t pair) {
        return static_cast<size_t>((pair * kHashMultiplier) >> 54);
    }

    std::array<Entry, kEntryCount> entries;
    static_assert(sizeof(std::array<Entry, kEntryCount>) == 16 * 1024);
};

struct SpimiTermBuffer::CommonGramPlainTermCache {
    struct Entry {
        uint32_t fingerprint = 0;
        uint32_t term_id = std::numeric_limits<uint32_t>::max();
    };
    static_assert(sizeof(Entry) == 8);

    static constexpr size_t kSetCount = 1024;
    static constexpr uint32_t kInvalidTermId = std::numeric_limits<uint32_t>::max();
    using Set = std::array<Entry, 2>;
    static_assert((kSetCount & (kSetCount - 1)) == 0);

    static size_t index(size_t term_hash) { return term_hash & (kSetCount - 1); }

    static uint32_t fingerprint(size_t term_hash) {
        const auto hash = static_cast<uint64_t>(term_hash);
        return static_cast<uint32_t>(hash ^ (hash >> 32));
    }

    static bool matches(const Entry& entry, uint32_t expected_fingerprint, std::string_view term,
                        const std::vector<std::string>& vocab) {
        if (entry.term_id == kInvalidTermId || entry.fingerprint != expected_fingerprint) {
            return false;
        }
        DCHECK_LT(entry.term_id, vocab.size());
        return std::string_view(vocab[entry.term_id]) == term;
    }

    uint32_t find(size_t term_hash, std::string_view term, const std::vector<std::string>& vocab) {
        Set& set = sets[index(term_hash)];
        const uint32_t expected_fingerprint = fingerprint(term_hash);
        if (matches(set[0], expected_fingerprint, term, vocab)) {
            return set[0].term_id;
        }
        if (matches(set[1], expected_fingerprint, term, vocab)) {
            std::swap(set[0], set[1]);
            return set[0].term_id;
        }
        return kInvalidTermId;
    }

    void remember(size_t term_hash, uint32_t term_id) {
        Set& set = sets[index(term_hash)];
        set[1] = set[0];
        set[0] = Entry {.fingerprint = fingerprint(term_hash), .term_id = term_id};
    }

    std::array<Set, kSetCount> sets {};
    static_assert(sizeof(std::array<Set, kSetCount>) == 16 * 1024);
};

bool SpimiTermBuffer::OwnedVocabEq::operator()(uint32_t stored,
                                               std::string_view probe) const noexcept {
#ifdef BE_TEST
    g_owned_term_full_byte_comparisons.fetch_add(1, std::memory_order_relaxed);
#endif
    DCHECK_LT(stored, vocab->size());
    return std::string_view((*vocab)[stored]) == probe;
}

bool SpimiTermBuffer::OwnedVocabEq::operator()(std::string_view probe,
                                               uint32_t stored) const noexcept {
    return (*this)(stored, probe);
}

#ifdef BE_TEST
size_t SpimiTermBuffer::owned_term_key_size_for_test() {
    return sizeof(decltype(intern_)::key_type);
}

void SpimiTermBuffer::set_owned_term_hash_mask_for_test(size_t mask) {
    DORIS_CHECK(intern_.empty());
    intern_ = decltype(intern_)(0, OwnedVocabHash {.vocab = &owned_vocab_, .hash_mask = mask},
                                OwnedVocabEq {&owned_vocab_});
}
#endif

namespace {

// Returns freed heap arenas to the OS (glibc only). The spill encode churns many
// small allocations whose freed chunks glibc retains in its arenas; trimming
// before the peak-RSS-defining merge phase recovers that retention. No-op (and
// harmless) on non-glibc libcs.
void trim_malloc() {
#if defined(__GLIBC__)
    ::malloc_trim(0);
#endif
}

// Process-unique temp path for a spill run under `dir` (pid + monotonic counter so
// parallel builds / multiple buffers never collide).
std::string make_run_path(const std::string& dir) {
    static std::atomic<uint64_t> counter {0};
    const uint64_t n = counter.fetch_add(1);
    return dir + "/snii_spill_" + std::to_string(::getpid()) + "_" + std::to_string(n) + ".run";
}

// TEST-ONLY seam backing testing::vocab_string_materialization_count(). Bumped once
// per DISTINCT interned term (owned_vocab_.emplace_back), never per token. Relaxed:
// the build path is single-threaded, so only the COUNT matters, not ordering.
#ifdef BE_TEST
std::atomic<uint64_t> g_vocab_materializations {0};
#endif

// G09 seam: spills that consumed a pending process-wide forced-spill request
// (the limiter flagged this buffer as one of the largest registered consumers
// while the global total exceeded the budget). Incremented under BE_TEST only
// (per-token path shared by concurrent writers).
std::atomic<uint64_t> g_global_forced_spills {0};

// G09 run-file cap seam: merge-compactions of a buffer's run list (always-on:
// at most one per cap-many spills, contention-free).
std::atomic<uint64_t> g_run_compactions {0};

// Test seam for complete-vocabulary rank rebuilds. The increment is compiled
// out of production because ensure_string_rank() may run on the import path.
#ifdef BE_TEST
std::atomic<uint64_t> g_string_rank_rebuilds {0};
std::atomic<uint64_t> g_dense_rank_inversions {0};
std::atomic<uint64_t> g_rank_comparison_sorts {0};
#endif

// G11 bench seam: when set (BE_TEST paths only), the add-path prefetch hints
// are skipped so the locality bench can A/B them in one process. Production
// builds never read it (the hint compiles in unconditionally there).
std::atomic<bool> g_bench_disable_g11_prefetch {false};

// G11 add-path prefetch gate: always-on in production; toggleable under
// BE_TEST for the in-process A/B bench. The branch is perfectly predicted, so
// the bench's OFF arm measures the pre-G11 code path faithfully.
inline bool g11_prefetch_enabled() {
#ifdef BE_TEST
    return !g_bench_disable_g11_prefetch.load(std::memory_order_relaxed);
#else
    return true;
#endif
}

// G08: heap payload of one owned-vocab string -- 0 while it fits the SSO buffer
// (those bytes live inside the 32 B header owned_vocab_.capacity() charges), else
// the allocated buffer (capacity + NUL). The SSO capacity is probed from the
// running stdlib so the classification is exact, not hardcoded.
uint64_t string_heap_bytes(const std::string& s) {
    static const size_t kSsoCapacity = std::string().capacity();
    return s.capacity() > kSsoCapacity ? static_cast<uint64_t>(s.capacity()) + 1 : 0;
}

void order_ids_by_dense_rank(std::vector<uint32_t>* ids, const std::vector<uint32_t>& rank) {
    if (ids->size() == rank.size()) {
        // Touched ids are unique. Equal cardinality therefore means the run covers
        // the complete vocabulary, so invert the dense rank in linear time.
        for (uint32_t term_id = 0; term_id < rank.size(); ++term_id) {
            (*ids)[rank[term_id]] = term_id;
        }
#ifdef BE_TEST
        g_dense_rank_inversions.fetch_add(1, std::memory_order_relaxed);
#endif
        return;
    }

    std::ranges::sort(*ids, [&](uint32_t a, uint32_t b) { return rank[a] < rank[b]; });
#ifdef BE_TEST
    g_rank_comparison_sorts.fetch_add(1, std::memory_order_relaxed);
#endif
}

} // namespace

namespace testing {
void set_bench_disable_g11_prefetch(bool disabled) {
    g_bench_disable_g11_prefetch.store(disabled, std::memory_order_relaxed);
}
uint64_t vocab_string_materialization_count() {
#ifdef BE_TEST
    return g_vocab_materializations.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_vocab_string_materialization_count() {
#ifdef BE_TEST
    g_vocab_materializations.store(0, std::memory_order_relaxed);
#endif
}
uint64_t global_forced_spills() {
    return g_global_forced_spills.load(std::memory_order_relaxed);
}
void reset_global_forced_spills() {
    g_global_forced_spills.store(0, std::memory_order_relaxed);
}
uint64_t run_compactions() {
    return g_run_compactions.load(std::memory_order_relaxed);
}
void reset_run_compactions() {
    g_run_compactions.store(0, std::memory_order_relaxed);
}
uint64_t string_rank_rebuilds() {
#ifdef BE_TEST
    return g_string_rank_rebuilds.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_string_rank_rebuilds() {
#ifdef BE_TEST
    g_string_rank_rebuilds.store(0, std::memory_order_relaxed);
#endif
}
uint64_t dense_rank_inversions() {
#ifdef BE_TEST
    return g_dense_rank_inversions.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
uint64_t rank_comparison_sorts() {
#ifdef BE_TEST
    return g_rank_comparison_sorts.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_rank_ordering_counts() {
#ifdef BE_TEST
    g_dense_rank_inversions.store(0, std::memory_order_relaxed);
    g_rank_comparison_sorts.store(0, std::memory_order_relaxed);
#endif
}
uint64_t common_gram_pair_unchecked_decode_count() {
#ifdef BE_TEST
    return g_common_gram_pair_unchecked_decodes.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
uint64_t common_gram_trusted_plain_decode_count() {
#ifdef BE_TEST
    return g_common_gram_trusted_plain_decodes.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_common_gram_pair_fast_path_counts() {
#ifdef BE_TEST
    g_common_gram_pair_unchecked_decodes.store(0, std::memory_order_relaxed);
    g_common_gram_trusted_plain_decodes.store(0, std::memory_order_relaxed);
#endif
}
uint64_t common_gram_pair_cache_probes() {
#ifdef BE_TEST
    return g_common_gram_pair_cache_probes.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
uint64_t common_gram_pair_cache_pair_hits() {
#ifdef BE_TEST
    return g_common_gram_pair_cache_pair_hits.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
uint64_t common_gram_pair_cache_same_doc_hits() {
#ifdef BE_TEST
    return g_common_gram_pair_cache_same_doc_hits.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_common_gram_pair_cache_counts() {
#ifdef BE_TEST
    g_common_gram_pair_cache_probes.store(0, std::memory_order_relaxed);
    g_common_gram_pair_cache_pair_hits.store(0, std::memory_order_relaxed);
    g_common_gram_pair_cache_same_doc_hits.store(0, std::memory_order_relaxed);
#endif
}
uint64_t common_gram_native_pair_probes() {
#ifdef BE_TEST
    return g_common_gram_native_pair_probes.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
uint64_t common_gram_native_pair_hits() {
#ifdef BE_TEST
    return g_common_gram_native_pair_hits.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
uint64_t common_gram_native_pair_inserts() {
#ifdef BE_TEST
    return g_common_gram_native_pair_inserts.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_common_gram_native_pair_intern_counts() {
#ifdef BE_TEST
    g_common_gram_native_pair_probes.store(0, std::memory_order_relaxed);
    g_common_gram_native_pair_hits.store(0, std::memory_order_relaxed);
    g_common_gram_native_pair_inserts.store(0, std::memory_order_relaxed);
#endif
}
uint64_t common_gram_logical_validation_count() {
#ifdef BE_TEST
    return g_common_gram_logical_validations.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_common_gram_logical_validation_count() {
#ifdef BE_TEST
    g_common_gram_logical_validations.store(0, std::memory_order_relaxed);
#endif
}
uint64_t common_gram_plain_cache_probes() {
#ifdef BE_TEST
    return g_common_gram_plain_cache_probes.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
uint64_t common_gram_plain_cache_hits() {
#ifdef BE_TEST
    return g_common_gram_plain_cache_hits.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
uint64_t common_gram_plain_intern_table_probes() {
#ifdef BE_TEST
    return g_common_gram_plain_intern_table_probes.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_common_gram_plain_cache_counts() {
#ifdef BE_TEST
    g_common_gram_plain_cache_probes.store(0, std::memory_order_relaxed);
    g_common_gram_plain_cache_hits.store(0, std::memory_order_relaxed);
    g_common_gram_plain_intern_table_probes.store(0, std::memory_order_relaxed);
#endif
}
uint64_t owned_term_full_byte_comparison_count() {
#ifdef BE_TEST
    return g_owned_term_full_byte_comparisons.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_owned_term_full_byte_comparison_count() {
#ifdef BE_TEST
    g_owned_term_full_byte_comparisons.store(0, std::memory_order_relaxed);
#endif
}
void fail_next_owned_term_reserve() {
#ifdef BE_TEST
    g_fail_next_owned_term_reserve.store(true, std::memory_order_relaxed);
#endif
}
void fail_next_owned_term_emplace() {
#ifdef BE_TEST
    g_fail_next_owned_term_emplace.store(true, std::memory_order_relaxed);
#endif
}
uint64_t spill_gate_check_count() {
#ifdef BE_TEST
    return g_spill_gate_checks.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_spill_gate_check_count() {
#ifdef BE_TEST
    g_spill_gate_checks.store(0, std::memory_order_relaxed);
#endif
}
uint64_t compact_chain_varint_decode_count() {
#ifdef BE_TEST
    return g_compact_chain_varint_decodes.load(std::memory_order_relaxed);
#else
    return 0;
#endif
}
void reset_compact_chain_varint_decode_count() {
#ifdef BE_TEST
    g_compact_chain_varint_decodes.store(0, std::memory_order_relaxed);
#endif
}
} // namespace testing

SpimiTermBuffer::SpimiTermBuffer(const std::vector<std::string>* vocab, bool has_positions,
                                 size_t spill_threshold_bytes, MemoryReporter* reporter)
        : vocab_(vocab),
          // Bind the equality functor to &owned_vocab_ even in borrowed mode:
          // add_token(string_view) rejects before the functor can dereference it,
          // and binding unconditionally keeps both constructors symmetric.
          // Initialized in the member-init list (NOT the body): the functors are
          // NESTED types, whose default-constructibility is not yet established at
          // the point the flat set's default ctor would be needed. The
          // (bucket_count, hash, equal) constructor sidesteps that entirely.
          // owned_vocab_ is constructed before intern_ (declaration order) and the
          // buffer is non-movable, so &owned_vocab_ is stable for the buffer's life.
          intern_(0, OwnedVocabHash {.vocab = &owned_vocab_}, OwnedVocabEq {&owned_vocab_}),
          has_positions_(has_positions),
          spill_threshold_bytes_(spill_threshold_bytes),
          mem_reporter_(reporter) {
    // Borrowed-vocab mode: only the 4 B/id slot-index array is sized to the
    // vocabulary; the Term pool (slots_) grows with the LIVE touched count, so an
    // all-but-empty vocabulary costs ~4 B/id instead of ~80 B/id.
    slot_of_.assign(vocab_->size(), 0);
    // The vocab-sized slot index is resident immediately and survives spills; report
    // its initial positive delta now.
    report_arena_delta();
}

SpimiTermBuffer::SpimiTermBuffer(bool has_positions, size_t spill_threshold_bytes,
                                 MemoryReporter* reporter)
        : vocab_(&owned_vocab_),
          // Owned-vocab mode: bind both functors to the sole vocabulary so stored
          // ids can rehash and string probes resolve full term equality.
          // Initialized in the member-init list (NOT the body):
          // the functors are NESTED types whose default-constructibility is not yet
          // established where the flat set's default ctor (whose noexcept spec inspects
          // the functors) would be needed for a body assignment, so the
          // (bucket_count, hash, equal) constructor is used instead. owned_vocab_ is
          // constructed before intern_ (declaration order) and the buffer is
          // non-movable, so &owned_vocab_ is stable for the buffer's life.
          intern_(0, OwnedVocabHash {.vocab = &owned_vocab_}, OwnedVocabEq {&owned_vocab_}),
          has_positions_(has_positions),
          spill_threshold_bytes_(spill_threshold_bytes),
          mem_reporter_(reporter) {
    report_arena_delta();
}

SpimiTermBuffer::~SpimiTermBuffer() {
    // G09: leave the process-wide registry FIRST. unregister_buffer removes the
    // entry (and its bytes) under the registry mutex -- the same mutex every
    // flag store is made under -- so once it returns, no other thread can touch
    // global_spill_requested_ while this buffer dies.
    if (global_limiter_ != nullptr) {
        global_limiter_->unregister_buffer(&global_spill_requested_);
        global_limiter_ = nullptr;
    }
    // Balance the writer-level / Doris tracker on the error path: if the buffer is
    // destroyed while resident bytes were reported but not yet freed-and-reported
    // (e.g. a build aborts before draining), return them here so nothing leaks.
    if (mem_reporter_ != nullptr && reported_resident_ != 0) {
        mem_reporter_->report(-reported_resident_);
        reported_resident_ = 0;
    }
    cleanup_runs();
}

void SpimiTermBuffer::attach_global_limiter(GlobalMemoryLimiter* limiter) {
    // At-most-once: a re-attach would leave a stale registry entry behind (the
    // dtor un-registers only the current limiter).
    if (limiter == nullptr || global_limiter_ != nullptr) {
        return;
    }
    global_limiter_ = limiter;
    // Race-safe vs report: registration and every report run on the OWNER's
    // thread, strictly ordered; the registry serializes them against other
    // buffers' calls internally. Register with the CURRENT resident total AND
    // the current spillable arena bytes (the victim-selection key) so the
    // registry is exact from the first moment (a borrowed-vocab buffer
    // already holds its vocab-sized slot index here).
    global_limiter_->register_buffer(&global_spill_requested_,
                                     static_cast<int64_t>(resident_bytes()),
                                     static_cast<int64_t>(pool_.arena_bytes()));
}

void SpimiTermBuffer::report_arena_delta() {
    if (mem_reporter_ == nullptr && global_limiter_ == nullptr) {
        return;
    }
    // Diff the REAL resident bytes (resident_bytes()) against the last reported
    // total; emit the signed delta exactly once.
    const auto now = static_cast<int64_t>(resident_bytes());
    // Per-token zero-delta debounce: skip the locked fetch_add when resident is
    // unchanged (the common case -- arena_bytes() grows only ~every 32 KiB block and
    // the other charged structures grow by geometric capacity steps / per new term
    // only, so most tokens see delta==0). A
    // delta==0 report() is a no-op (current_.fetch_add(0) plus a mirrored
    // consume_release(0)) and leaves reported_resident_ == now, so current_bytes(),
    // every over_cap() result, and the gate-2 spill timing stay bit-for-bit identical.
    // The spill gate still evaluates the writer-level UNIFIED total whenever the
    // arena is large enough to reclaim, even if this buffer's local delta is 0:
    // the shared dict buffer may have crossed the cap independently.
    if (now == reported_resident_) {
        return;
    }
    if (mem_reporter_ != nullptr) {
        mem_reporter_->report(now - reported_resident_);
    }
    // G09: forward the same debounced total -- as an ABSOLUTE, self-healing
    // value -- to the process-wide registry, together with the current
    // SPILLABLE arena bytes (the victim-selection key: only the arena is
    // reclaimable by a forced spill; the persistent vocab/pair structures are
    // not). This is the limiter's decision point: report() flags the
    // largest-arena eligible buffers (possibly this one) while the global sum
    // exceeds the budget. It only ever takes the registry mutex and flips
    // advisory atomics; no lock is held here while spilling (any spill this
    // buffer performs happens AFTER this returns, back in
    // maybe_spill_after_token, on this thread).
    if (global_limiter_ != nullptr) {
        global_limiter_->report(&global_spill_requested_, now,
                                static_cast<int64_t>(pool_.arena_bytes()));
    }
    reported_resident_ = now;
}

size_t SpimiTermBuffer::unique_terms() const {
    return live_term_count_;
}

uint64_t SpimiTermBuffer::resident_bytes() const {
    // Everything live is charged by CAPACITY (the reserved tail is resident RSS
    // and survives spills). All reads are O(1), since this runs once per token.
    uint64_t b = pool_.arena_bytes(); // posting chains: docs + prx payload
    b += static_cast<uint64_t>(slot_of_.capacity()) * sizeof(uint32_t); // vocab-sized slot index
    b += static_cast<uint64_t>(slots_.capacity()) * sizeof(Term);       // live Term pool
    b += static_cast<uint64_t>(free_slots_.capacity()) * sizeof(uint32_t);
    b += static_cast<uint64_t>(touched_ids_.capacity()) * sizeof(uint32_t);
    // Owned-vocab machinery (all zero in borrowed mode): string headers by vector
    // capacity, heap payloads via the incrementally-maintained counter, and the
    // intern set's entries at a fixed per-entry estimate (kept at the
    // pre-G10 node-set value so the gate-2 spill points are unchanged; see the
    // constant's comment).
    b += static_cast<uint64_t>(owned_vocab_.capacity()) * sizeof(std::string);
    b += owned_vocab_heap_bytes_;
    b += static_cast<uint64_t>(common_word_classification_.capacity()) *
         sizeof(CommonWordClassification);
    b += static_cast<uint64_t>(intern_.size() + common_gram_pair_intern_.size()) *
         kInternEntryEstimateBytes;
    b += common_gram_pair_cache_bytes_;
    b += common_gram_plain_term_cache_bytes_;
    // Cached lexicographic ranks survive spills and are included by capacity.
    b += static_cast<uint64_t>(string_rank_.capacity()) * sizeof(uint32_t);
    return b;
}

// Returns the live Term for `term_id`, claiming a pool slot on first touch (1 ==
// new). Reuses a freed slot from free_slots_ when available; otherwise appends a
// fresh Term to slots_. slot_of_[term_id] holds (slot index + 1); 0 means empty.
SpimiTermBuffer::Term& SpimiTermBuffer::term_slot(uint32_t term_id, bool* new_term) {
    uint32_t enc = slot_of_[term_id];
    if (enc != 0) {
        *new_term = false;
        return slots_[enc - 1];
    }
    *new_term = true;
    uint32_t slot;
    if (!free_slots_.empty()) {
        slot = free_slots_.back();
        free_slots_.pop_back();
    } else {
        slot = static_cast<uint32_t>(slots_.size());
        slots_.emplace_back();
    }
    slot_of_[term_id] = slot + 1;
    return slots_[slot];
}

void SpimiTermBuffer::put_varint(Term* t, uint64_t v) {
    if (t->head == kNoChain) {
        t->head = pool_.start_chain(&t->w, &t->level);
    }
    if (v < 0x80U) {
        pool_.append_byte(&t->w, &t->level, static_cast<uint8_t>(v));
        return;
    }
    pool_.append_varint(&t->w, &t->level, v);
}

void SpimiTermBuffer::accumulate_without_spill_gate(uint32_t term_id, uint32_t docid, uint32_t pos,
                                                    PostingChainShape shape) {
    const bool retain_positions = shape == PostingChainShape::kTaggedPositioned;
    const bool statless_common_gram = shape == PostingChainShape::kStatlessDocsOnly;
    DCHECK(!retain_positions || has_positions_);
    bool new_term = false;
    Term& t = term_slot(term_id, &new_term);
    if (new_term) {
        t.shape = shape;
        touched_ids_.push_back(term_id);
        ++live_term_count_;
    } else {
        DCHECK(t.shape == shape);
    }
    // Docs-only accelerator postings are sets. Tokens for one input document are
    // contiguous on the writer path, so discard repeated occurrences before they
    // allocate arena bytes or enter spill/sort/posting encoding.
    if (!retain_positions && t.started && t.cur_docid == docid) {
        ++total_tokens_;
        return;
    }
    // A token starts a new doc unless it continues the most-recent doc for this term.
    const bool first_token = !t.started;
    const bool new_doc = first_token || t.cur_docid != docid;
    // A statless CommonGram singleton owns no chain. On its second distinct doc,
    // backfill the first absolute docid before appending the current delta.
    if (statless_common_gram && !first_token && t.head == kNoChain) {
        DCHECK_EQ(t.ntok, 1U);
        DCHECK_EQ(t.ndocs, 1U);
        put_varint(&t, zigzag_encode(static_cast<int64_t>(t.cur_docid)));
    }

    // Positioned and ordinary docs-only terms retain the tagged token stream used
    // to reconstruct frequency. A statless CommonGram is already deduplicated per
    // document and has no frequency, so its new_doc tag would be the constant 1;
    // omit it and store only the document delta.
    if (!statless_common_gram) {
        // Widen to 64-bit so a full 32-bit position survives the shift.
        const uint64_t tagged = retain_positions
                                        ? ((static_cast<uint64_t>(pos) << 1) | (new_doc ? 1U : 0U))
                                        : (new_doc ? 1U : 0U);
        put_varint(&t, tagged);
    } else {
        DCHECK(new_doc);
    }
    if (new_doc) {
        // Out-of-order docids are tolerated (zigzag delta is signed) and reordered at
        // finalize; flag them so to_postings sorts. The delta base is the previous
        // distinct doc (cur_docid), which is 0 for the very first doc (started==false).
        const int64_t base = t.started ? static_cast<int64_t>(t.cur_docid) : 0;
        if (t.started && docid < t.cur_docid) {
            t.sorted = false;
        }
        const int64_t delta = static_cast<int64_t>(docid) - base;
        if (!first_token || !statless_common_gram) {
            put_varint(&t, zigzag_encode(delta));
        }
        t.cur_docid = docid;
        t.started = true;
        // Exact new-doc group count; out-of-order coalescing can only shrink it.
        ++t.ndocs;
    }
    ++t.ntok;
    ++total_tokens_;
}

void SpimiTermBuffer::accumulate(uint32_t term_id, uint32_t docid, uint32_t pos,
                                 bool retain_positions) {
    accumulate_without_spill_gate(term_id, docid, pos,
                                  retain_positions ? PostingChainShape::kTaggedPositioned
                                                   : PostingChainShape::kTaggedDocsOnly);
    maybe_spill_after_token();
}

// Per-input-token gate-2 tail. Ordinary adds invoke it after one posting; the
// fused CommonGrams path invokes it after its gram and right plain posting. It
// reports the token's REAL resident growth FIRST so the writer's unified total
// (reporter_->current_bytes()) reflects it before the gate check (single-source
// diff; cheap: a subtraction + relaxed atomic add), then evaluates the spill triggers:
//   * Gate-2 (UNIFIED): with a reporter attached, trigger on the writer's TOTAL
//     build RAM (arena + vocab structures + dict) crossing the one
//     configured cap -- the same total and cap every buffer of this writer
//     shares, not a per-buffer threshold. Off Doris (no reporter) fall back to
//     the local spill_threshold_bytes_ against resident_bytes().
//   * G08 anti-churn floor: a gate-2 spill reclaims ONLY the posting arena
//     (pool_.reset()); the vocab / slot structures resident_bytes()
//     now also charges SURVIVE it. Once those persistent bytes alone exceed the
//     cap, an unconditioned
//     trigger would spill EVERY subsequent token -- one-block runs, k-way-merge
//     and spill-fixed-cost blowup. Honor the cap only when at least a quarter of
//     it is reclaimable arena: peak stays bounded at persistent + cap/4 and no
//     run is smaller than cap/4, while the one-block minimum keeps small caps
//     (tests, tiny configs) spilling on the first block exactly as before.
//   * Hard arena safety stop, active even in unlimited mode and BYPASSING the
//     floor: when the arena nears the 4 GiB uint32-offset limit, spill now --
//     without it a single >4 GiB in-memory segment wraps alloc_run and silently
//     corrupts data. A forced spill + final k-way merge stays byte-identical
//     regardless of when it fires.
// spill_to_run() resets the arena and reports its negative internally, so the
// unified total drops (and the trigger self-rearms) after each spill.
void SpimiTermBuffer::maybe_spill_after_token() {
#ifdef BE_TEST
    g_spill_gate_checks.fetch_add(1, std::memory_order_relaxed);
#endif
    constexpr uint64_t kArenaSpillCap = 0xE0000000ULL; // 3.5 GiB, < UINT32_MAX margin
    const bool global_requested = global_spill_requested_.load(std::memory_order_relaxed);
    const bool arena_near_limit = pool_.arena_bytes() >= kArenaSpillCap;
    report_arena_delta();
    const uint64_t gate_cap =
            mem_reporter_ != nullptr ? mem_reporter_->cap_bytes() : spill_threshold_bytes_;
    const bool arena_worth_spilling =
            pool_.arena_bytes() >= std::max<uint64_t>(CompactPostingPool::kBlockSize, gate_cap / 4);
    // G09: the process-wide limiter flagged this buffer (one of the
    // largest-ARENA eligible consumers while the global total exceeded the
    // budget). Honored HERE, on the owner's own thread -- never on the
    // reporting thread that set the flag. The G08 anti-churn floor (cap/4) is
    // deliberately BYPASSED (each victim's arena is below cap/4 by
    // construction: it never reached its per-writer gate -- that is exactly
    // why the global sum grew), but the FORCED-SPILL FLOOR
    // (snii_forced_spill_min_arena_bytes, >= one arena block so a run is
    // writable) still applies: a forced spill reclaims ONLY the arena, so
    // honoring below the floor would cut a tiny run for near-zero relief.
    // Below the floor the request is a NO-OP that stays PENDING -- it is NOT
    // retried as a spill each token -- and is honored once the arena regrows
    // past the floor (the limiter's victim selection applies the same floor,
    // so a below-floor flag only arises from a floor/config race or a test
    // seam). A request that finds the owner already drained is never observed
    // again -- an advisory no-op (the dtor un-registers) -- and a stale
    // re-request after a spill costs at most one extra floor-sized run
    // (double-spill is harmless, byte-identical output).
    const bool global_spill_now =
            global_requested &&
            pool_.arena_bytes() >= std::max<uint64_t>(CompactPostingPool::kBlockSize,
                                                      forced_spill_min_arena_bytes_);
    const bool over_cap = !global_spill_now && !arena_near_limit && arena_worth_spilling &&
                          (mem_reporter_ != nullptr ? mem_reporter_->over_cap()
                                                    : (spill_threshold_bytes_ != 0 &&
                                                       resident_bytes() >= spill_threshold_bytes_));
    if ((over_cap || global_spill_now || arena_near_limit) && spill_status_.ok()) {
        if (global_requested) {
            // Consume the request BEFORE spilling: this spill releases exactly
            // the arena a forced spill would, so it satisfies the request no
            // matter which trigger won the OR above.
            global_spill_requested_.store(false, std::memory_order_relaxed);
#ifdef BE_TEST
            // Seam under BE_TEST only: per-token path shared by every
            // concurrent writer.
            g_global_forced_spills.fetch_add(1, std::memory_order_relaxed);
#endif
        }
        spill_status_ = spill_to_run();
    }
}

void SpimiTermBuffer::add_token(uint32_t term_id, uint32_t docid, uint32_t pos) {
    add_token(term_id, docid, pos, has_positions_);
}

void SpimiTermBuffer::add_token(uint32_t term_id, uint32_t docid, uint32_t pos,
                                bool retain_positions) {
    // Hot path: a pooled slot lookup + a couple of pushes. No hashing, no string
    // construction per token. Reject (and latch) an out-of-range id.
    if (term_id >= slot_of_.size()) {
        if (spill_status_.ok()) {
            spill_status_ = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spimi: term_id out of vocab range");
        }
        return;
    }
    accumulate(term_id, docid, pos, retain_positions);
}

void SpimiTermBuffer::add_token(std::string_view term, uint32_t docid, uint32_t pos) {
    add_token(term, docid, pos, has_positions_);
}

void SpimiTermBuffer::add_token(std::string_view term, uint32_t docid, uint32_t pos,
                                bool retain_positions) {
    // Compatibility path: intern the term into the owned vocabulary on first
    // occurrence, then accumulate by its id. ONLY valid in OWNED-vocab mode. In
    // BORROWED-vocab mode vocab_ points at the caller's vector, NOT &owned_vocab_:
    // interning here would grow owned_vocab_ / intern_ / slot_of_ out of step with
    // the active (borrowed) vocab, so the new id indexes the WRONG string and writes
    // a slot_of_ entry the borrowed-vocab build never reconciles -- silent
    // corruption. Reject (and latch) instead of forwarding by a bogus id.
    if (vocab_ != &owned_vocab_) {
        if (spill_status_.ok()) {
            spill_status_ = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spimi: add_token(string_view) requires owned-vocab mode");
        }
        return;
    }
    DCHECK(!common_gram_pair_keys_);
    const uint32_t term_id = find_or_intern_owned_term(term);
    accumulate(term_id, docid, pos, retain_positions);
}

PlainTermId SpimiTermBuffer::intern_plain_term(std::string_view physical_plain_term) {
    DCHECK(common_gram_pair_keys_);
    DCHECK(vocab_ == &owned_vocab_);
    DCHECK(!physical_plain_term.empty());
    DCHECK(!segment_v2::inverted_index::is_internal_term_key(physical_plain_term));
    const size_t term_hash = intern_.hash(physical_plain_term);
    uint32_t term_id = find_interned_plain_term(physical_plain_term, term_hash);
    if (term_id == CommonGramPlainTermCache::kInvalidTermId) {
        term_id = intern_owned_term(std::string(physical_plain_term), term_hash);
        remember_plain_term(term_hash, term_id);
    }
    return PlainTermId {.value = term_id};
}

PlainTermId SpimiTermBuffer::intern_plain_term(std::string_view physical_plain_term,
                                               std::string_view logical_plain_term) {
    DCHECK(common_gram_pair_keys_);
    DCHECK(vocab_ == &owned_vocab_);
    DCHECK(!physical_plain_term.empty());
    DCHECK(!logical_plain_term.empty());
    DCHECK(!segment_v2::inverted_index::is_internal_term_key(physical_plain_term));

    const size_t term_hash = intern_.hash(physical_plain_term);
    uint32_t term_id = find_interned_plain_term(physical_plain_term, term_hash);
    if (term_id != CommonGramPlainTermCache::kInvalidTermId) {
        return PlainTermId {.value = term_id};
    }

#ifdef BE_TEST
    g_common_gram_logical_validations.fetch_add(1, std::memory_order_relaxed);
#endif
    auto validation = segment_v2::inverted_index::validate_common_grams_logical_term(
            logical_plain_term, "input token");
    if (!validation.ok()) {
        throw Exception(validation);
    }
    if (physical_plain_term.size() > segment_v2::inverted_index::COMMON_GRAM_MAX_ENCODED_BYTES) {
        throw Exception(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "CommonGrams escaped plain term would exceed the 16383-byte key limit; "
                "set enable_common_grams_index_build=false and retry the import in a new "
                "transaction"));
    }
    term_id = intern_owned_term(std::string(physical_plain_term), term_hash);
    remember_plain_term(term_hash, term_id);
    return PlainTermId {.value = term_id};
}

ClassifiedPlainTerm SpimiTermBuffer::intern_classified_plain_term(
        std::string_view physical_plain_term, std::string_view logical_plain_term,
        const segment_v2::inverted_index::CommonWordSet& common_words) {
    DCHECK(common_gram_pair_keys_);
    const PlainTermId id = intern_plain_term(physical_plain_term, logical_plain_term);
    DCHECK_EQ(common_word_classification_.size(), owned_vocab_.size());
    DCHECK_LT(id.value, common_word_classification_.size());
    CommonWordClassification& classification = common_word_classification_[id.value];
    if (classification == CommonWordClassification::kUnknown) {
        classification = common_words.contains(logical_plain_term)
                                 ? CommonWordClassification::kCommon
                                 : CommonWordClassification::kNotCommon;
    }
    return ClassifiedPlainTerm {
            .id = id,
            .is_common = classification == CommonWordClassification::kCommon,
    };
}

void SpimiTermBuffer::add_plain_token(PlainTermId term_id, uint32_t docid, uint32_t pos) {
    DCHECK(common_gram_pair_keys_);
    DCHECK_LT(term_id.value, owned_vocab_.size());
    DCHECK(!segment_v2::inverted_index::is_internal_term_key(owned_vocab_[term_id.value]));
    accumulate(term_id.value, docid, pos, has_positions_);
}

void SpimiTermBuffer::add_common_gram_without_spill_gate(PlainTermId left, PlainTermId right,
                                                         uint32_t docid, uint32_t pos,
                                                         bool retain_positions) {
    DCHECK(common_gram_pair_keys_);
    DCHECK_LT(left.value, owned_vocab_.size());
    DCHECK_LT(right.value, owned_vocab_.size());
    DCHECK(!segment_v2::inverted_index::is_internal_term_key(owned_vocab_[left.value]));
    DCHECK(!segment_v2::inverted_index::is_internal_term_key(owned_vocab_[right.value]));
    DCHECK(common_gram_pair_cache_ != nullptr);
    const PostingChainShape shape = retain_positions ? PostingChainShape::kTaggedPositioned
                                                     : PostingChainShape::kStatlessDocsOnly;
    const uint64_t pair = (static_cast<uint64_t>(left.value) << 32) | right.value;
    CommonGramPairCache::Entry& entry =
            common_gram_pair_cache_->entries[CommonGramPairCache::index(pair)];
#ifdef BE_TEST
    g_common_gram_pair_cache_probes.fetch_add(1, std::memory_order_relaxed);
#endif
    if (entry.term_id != CommonGramPairCache::kInvalidTermId && entry.pair == pair) {
#ifdef BE_TEST
        g_common_gram_pair_cache_pair_hits.fetch_add(1, std::memory_order_relaxed);
#endif
        DCHECK_LT(entry.term_id, slot_of_.size());
        if (!retain_positions && entry.last_docid == docid) {
#ifdef BE_TEST
            g_common_gram_pair_cache_same_doc_hits.fetch_add(1, std::memory_order_relaxed);
#endif
            ++total_tokens_;
            return;
        }
        entry.last_docid = docid;
        accumulate_without_spill_gate(entry.term_id, docid, pos, shape);
        return;
    }

    const uint32_t term_id = find_or_intern_common_gram_pair(left, right, pair);
    DCHECK_NE(term_id, CommonGramPairCache::kInvalidTermId);
    entry = CommonGramPairCache::Entry {.pair = pair, .term_id = term_id, .last_docid = docid};
    accumulate_without_spill_gate(term_id, docid, pos, shape);
}

void SpimiTermBuffer::add_common_gram(PlainTermId left, PlainTermId right, uint32_t docid,
                                      uint32_t pos, bool retain_positions) {
    add_common_gram_without_spill_gate(left, right, docid, pos, retain_positions);
    maybe_spill_after_token();
}

void SpimiTermBuffer::add_common_gram_and_plain(PlainTermId left, PlainTermId right, uint32_t docid,
                                                uint32_t gram_pos, uint32_t plain_pos,
                                                bool retain_gram_positions) {
    add_common_gram_without_spill_gate(left, right, docid, gram_pos, retain_gram_positions);
    DCHECK_LT(right.value, owned_vocab_.size());
    DCHECK(!segment_v2::inverted_index::is_internal_term_key(owned_vocab_[right.value]));
    accumulate_without_spill_gate(right.value, docid, plain_pos,
                                  has_positions_ ? PostingChainShape::kTaggedPositioned
                                                 : PostingChainShape::kTaggedDocsOnly);
    maybe_spill_after_token();
}

void SpimiTermBuffer::enable_common_gram_pair_keys() {
    DORIS_CHECK(vocab_ == &owned_vocab_);
    DORIS_CHECK_EQ(total_tokens_, 0);
    DORIS_CHECK(owned_vocab_.empty());
    DORIS_CHECK(!common_gram_pair_keys_);
    DORIS_CHECK(common_word_classification_.empty());
    auto pair_cache = std::make_unique<CommonGramPairCache>();
    auto plain_term_cache = std::make_unique<CommonGramPlainTermCache>();
    common_gram_pair_keys_ = true;
    common_gram_pair_cache_ = std::move(pair_cache);
    common_gram_pair_cache_bytes_ = sizeof(CommonGramPairCache);
    common_gram_plain_term_cache_ = std::move(plain_term_cache);
    common_gram_plain_term_cache_bytes_ = sizeof(CommonGramPlainTermCache);
    report_arena_delta();
}

uint32_t SpimiTermBuffer::find_or_intern_owned_term(std::string_view term) {
    static_assert(std::is_same_v<decltype(intern_)::key_type, uint32_t>);
    DCHECK_LE(term.size(), std::numeric_limits<uint32_t>::max());
    const size_t term_hash = intern_.hash(term);
    const auto found = intern_.find(term, term_hash);
    if (found != intern_.end()) {
        const uint32_t term_id = *found;
        if (g11_prefetch_enabled()) {
            __builtin_prefetch(slot_of_.data() + term_id);
        }
        return term_id;
    }
    return intern_owned_term(std::string(term), term_hash);
}

uint32_t SpimiTermBuffer::find_or_intern_common_gram_pair(PlainTermId left, PlainTermId right,
                                                          uint64_t pair) {
    DCHECK(common_gram_pair_keys_);
    DCHECK_LT(left.value, owned_vocab_.size());
    DCHECK_LT(right.value, owned_vocab_.size());
    DCHECK(!segment_v2::inverted_index::is_internal_term_key(owned_vocab_[left.value]));
    DCHECK(!segment_v2::inverted_index::is_internal_term_key(owned_vocab_[right.value]));
#ifdef BE_TEST
    g_common_gram_native_pair_probes.fetch_add(1, std::memory_order_relaxed);
#endif
    const auto found = common_gram_pair_intern_.find(pair);
    if (found != common_gram_pair_intern_.end()) {
#ifdef BE_TEST
        g_common_gram_native_pair_hits.fetch_add(1, std::memory_order_relaxed);
#endif
        return found->second;
    }

    const auto key = EncodeCommonGramPairKey(left, right);
    const uint32_t term_id = append_owned_vocab_term(std::string(key.data(), key.size()));
    const auto [inserted_it, inserted] = common_gram_pair_intern_.try_emplace(pair, term_id);
    DCHECK(inserted);
    DCHECK_EQ(inserted_it->second, term_id);
#ifdef BE_TEST
    g_common_gram_native_pair_inserts.fetch_add(1, std::memory_order_relaxed);
#endif
    return term_id;
}

uint32_t SpimiTermBuffer::find_interned_plain_term(std::string_view term, size_t term_hash) {
    DCHECK(common_gram_pair_keys_);
    DCHECK(common_gram_plain_term_cache_ != nullptr);
#ifdef BE_TEST
    g_common_gram_plain_cache_probes.fetch_add(1, std::memory_order_relaxed);
#endif
    const uint32_t cached_term_id =
            common_gram_plain_term_cache_->find(term_hash, term, owned_vocab_);
    if (cached_term_id != CommonGramPlainTermCache::kInvalidTermId) {
#ifdef BE_TEST
        g_common_gram_plain_cache_hits.fetch_add(1, std::memory_order_relaxed);
#endif
        if (g11_prefetch_enabled()) {
            __builtin_prefetch(slot_of_.data() + cached_term_id);
        }
        return cached_term_id;
    }

#ifdef BE_TEST
    g_common_gram_plain_intern_table_probes.fetch_add(1, std::memory_order_relaxed);
#endif
    const auto found = intern_.find(term, term_hash);
    if (found == intern_.end()) {
        return CommonGramPlainTermCache::kInvalidTermId;
    }
    const uint32_t term_id = *found;
    remember_plain_term(term_hash, term_id);
    if (g11_prefetch_enabled()) {
        __builtin_prefetch(slot_of_.data() + term_id);
    }
    return term_id;
}

void SpimiTermBuffer::remember_plain_term(size_t term_hash, uint32_t term_id) {
    DCHECK(common_gram_pair_keys_);
    DCHECK(common_gram_plain_term_cache_ != nullptr);
    DCHECK_LT(term_id, owned_vocab_.size());
    common_gram_plain_term_cache_->remember(term_hash, term_id);
}

bool SpimiTermBuffer::transient_term_less(uint32_t left_id, uint32_t right_id) const {
    const std::vector<std::string>& v = vocab();
    const std::string_view left = v[left_id];
    const std::string_view right = v[right_id];
    if (!common_gram_pair_keys_) {
        return left < right;
    }

    const bool left_is_pair = is_common_gram_pair_key(left);
    const bool right_is_pair = is_common_gram_pair_key(right);
    if (left_is_pair != right_is_pair) {
        const std::string_view plain = left_is_pair ? right : left;
        DCHECK(!plain.empty());
        DCHECK(!segment_v2::inverted_index::is_internal_term_key(plain));
        const bool pair_sorts_first =
                static_cast<uint8_t>(
                        segment_v2::inverted_index::INTERNAL_TERM_NAMESPACE_BEGIN.front()) <
                static_cast<uint8_t>(plain.front());
        return left_is_pair ? pair_sorts_first : !pair_sorts_first;
    }
    if (!left_is_pair) {
        return left < right;
    }

    const CommonGramPairIds left_ids = decode_common_gram_pair_key_unchecked(left);
    const CommonGramPairIds right_ids = decode_common_gram_pair_key_unchecked(right);
    DCHECK_LT(left_ids.left.value, v.size());
    DCHECK_LT(left_ids.right.value, v.size());
    DCHECK_LT(right_ids.left.value, v.size());
    DCHECK_LT(right_ids.right.value, v.size());

    const LogicalPlainKeyView left_left_key(v[left_ids.left.value]);
    const LogicalPlainKeyView left_right_key(v[left_ids.right.value]);
    const LogicalPlainKeyView right_left_key(v[right_ids.left.value]);
    const LogicalPlainKeyView right_right_key(v[right_ids.right.value]);
    if (left_left_key.size() != right_left_key.size()) {
        return left_left_key.size() < right_left_key.size();
    }
    const int left_component_order = compare_logical_plain_keys(left_left_key, right_left_key);
    if (left_component_order != 0) {
        return left_component_order < 0;
    }
    return compare_logical_plain_keys(left_right_key, right_right_key) < 0;
}

std::string SpimiTermBuffer::materialize_transient_term(std::string_view term) const {
    if (!is_common_gram_pair_key(term)) {
        return std::string(term);
    }

    DCHECK(common_gram_pair_keys_);
    const CommonGramPairIds ids = decode_common_gram_pair_key_unchecked(term);
    DCHECK_LT(ids.left.value, owned_vocab_.size());
    DCHECK_LT(ids.right.value, owned_vocab_.size());
    DCHECK(!is_common_gram_pair_key(owned_vocab_[ids.left.value]));
    DCHECK(!is_common_gram_pair_key(owned_vocab_[ids.right.value]));

    std::string left_scratch;
    std::string right_scratch;
    const std::string_view left =
            decode_logical_plain_term_trusted(owned_vocab_[ids.left.value], &left_scratch);
    const std::string_view right =
            decode_logical_plain_term_trusted(owned_vocab_[ids.right.value], &right_scratch);
    std::string output;
    [[maybe_unused]] const bool encoded =
            segment_v2::inverted_index::try_encode_common_gram_prevalidated(left, right, output);
    DCHECK(encoded);
    return output;
}

// Prepared first-time insertion stores the string before emplace so every
// stored id remains resolvable during later growth rehashes.
uint32_t SpimiTermBuffer::intern_owned_term(std::string&& term_str, size_t term_hash) {
    const size_t next_vocab_size = owned_vocab_.size() + 1;
    DCHECK_LE(next_vocab_size, std::numeric_limits<uint32_t>::max());

    size_t target_capacity = owned_vocab_.capacity();
    if (target_capacity < next_vocab_size) {
        target_capacity = target_capacity <= std::numeric_limits<size_t>::max() / 2
                                  ? std::max(next_vocab_size, target_capacity * 2)
                                  : next_vocab_size;
    }

    // Prepare append-only vectors geometrically before publishing a vocabulary
    // id. A later reserve may throw after an earlier vector already changed
    // capacity, so the catch path must settle that resident delta before
    // propagating the failure.
    try {
        owned_vocab_.reserve(target_capacity);
#ifdef BE_TEST
        if (g_fail_next_owned_term_reserve.exchange(false, std::memory_order_relaxed)) {
            throw std::bad_alloc();
        }
#endif
        slot_of_.reserve(target_capacity);
        if (common_gram_pair_keys_) {
            common_word_classification_.reserve(target_capacity);
        }
    } catch (...) {
        report_arena_delta();
        throw;
    }
    report_arena_delta();

    const uint32_t term_id = append_owned_vocab_term(std::move(term_str));
    static_assert(std::is_nothrow_copy_constructible_v<uint32_t>);

    const auto rollback_append = [&]() {
#ifdef BE_TEST
        g_vocab_materializations.fetch_sub(1, std::memory_order_relaxed);
#endif
        owned_vocab_heap_bytes_ -= string_heap_bytes(owned_vocab_.back());
        slot_of_.pop_back();
        if (common_gram_pair_keys_) {
            common_word_classification_.pop_back();
        }
        owned_vocab_.pop_back();
        report_arena_delta();
    };

    // phmap allocates a growth table before publishing the prepared slot, and
    // constructing this trivial key cannot throw. If allocation fails, the old
    // table is intact and only the preceding vocabulary append needs rollback.
    const auto [it, inserted] = [&]() {
        try {
#ifdef BE_TEST
            if (g_fail_next_owned_term_emplace.exchange(false, std::memory_order_relaxed)) {
                throw std::bad_alloc();
            }
#endif
            return intern_.emplace_with_hash(term_hash, term_id);
        } catch (...) {
            rollback_append();
            throw;
        }
    }();
    if (!inserted) {
        rollback_append();
    }
    DCHECK(inserted);
    DCHECK_EQ(*it, term_id);
    return term_id;
}

uint32_t SpimiTermBuffer::append_owned_vocab_term(std::string&& term_str) {
    const uint32_t term_id = static_cast<uint32_t>(owned_vocab_.size());
    owned_vocab_.emplace_back(std::move(term_str));
    if (common_gram_pair_keys_) {
        common_word_classification_.push_back(CommonWordClassification::kUnknown);
        DCHECK_EQ(common_word_classification_.size(), owned_vocab_.size());
    }
    slot_of_.push_back(0); // vocab grows: new id starts with no live slot
    // G08: credit the stored string's heap payload (0 for SSO); the header is
    // charged via owned_vocab_.capacity().
    owned_vocab_heap_bytes_ += string_heap_bytes(owned_vocab_[term_id]);
#ifdef BE_TEST
    g_vocab_materializations.fetch_add(1, std::memory_order_relaxed);
#endif
    return term_id;
}

namespace {

// Reorders a term's flat arrays into ascending-docid order, COALESCING any
// same-docid groups so the result has exactly one entry per docid -- matching the
// k-way-merge path's boundary-doc coalescing and the writer's strictly-ascending
// precondition. Only invoked for the rare term that received out-of-order docids
// (the common ascending path leaves t.sorted true and skips it).
//
// A docid may REVISIT (e.g. feed 5,1,5): the chain holds two separate doc-groups
// for doc 5. A STABLE sort keeps equal-docid groups in arrival order, then the
// coalesce pass sums their freqs and concatenates their positions in that same
// (document/arrival) order -- so the merged positions stay consistent with the
// merged freqs, exactly as the run-order merge would have produced.
template <typename T>
Status reserve_tracked_vector(std::vector<T>* values, size_t target,
                              MemoryReporter* memory_reporter,
                              MemoryReporter::Reservation* reservation) {
    if (target <= values->capacity()) {
        return Status::OK();
    }
    if (target > std::numeric_limits<uint64_t>::max() / sizeof(T)) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "spimi materialization: vector byte capacity overflow");
    }
    if (memory_reporter == nullptr) {
        values->reserve(target);
        return Status::OK();
    }
    MemoryReporter::Reservation replacement;
    RETURN_IF_ERROR(reservation->prepare_replacement(static_cast<uint64_t>(target) * sizeof(T),
                                                     &replacement));
    values->reserve(target);
    DCHECK_EQ(values->capacity(), target);
    *reservation = std::move(replacement);
    return Status::OK();
}

Status sort_by_docid(std::vector<uint32_t>* docids, std::vector<uint32_t>* freqs,
                     std::vector<uint32_t>* positions_flat, bool has_positions,
                     MemoryReporter* memory_reporter,
                     MemoryReporter::Reservation* docids_reservation,
                     MemoryReporter::Reservation* freqs_reservation,
                     MemoryReporter::Reservation* positions_reservation) {
    const size_t n = docids->size();
    MemoryReporter::Reservation order_reservation = memory_reporter == nullptr
                                                            ? MemoryReporter::Reservation()
                                                            : memory_reporter->make_reservation();
    MemoryReporter::Reservation pos_off_reservation = memory_reporter == nullptr
                                                              ? MemoryReporter::Reservation()
                                                              : memory_reporter->make_reservation();
    MemoryReporter::Reservation sorted_docids_reservation =
            memory_reporter == nullptr ? MemoryReporter::Reservation()
                                       : memory_reporter->make_reservation();
    MemoryReporter::Reservation sorted_freqs_reservation =
            memory_reporter == nullptr ? MemoryReporter::Reservation()
                                       : memory_reporter->make_reservation();
    MemoryReporter::Reservation sorted_positions_reservation =
            memory_reporter == nullptr ? MemoryReporter::Reservation()
                                       : memory_reporter->make_reservation();
    std::vector<size_t> order;
    RETURN_IF_ERROR(reserve_tracked_vector(&order, n, memory_reporter, &order_reservation));
    order.resize(n);
    std::iota(order.begin(), order.end(), 0);
    // The original index breaks equal-doc ties, preserving arrival order without
    // stable_sort's implementation-owned allocation.
    std::ranges::sort(order, [&](size_t a, size_t b) {
        if ((*docids)[a] != (*docids)[b]) {
            return (*docids)[a] < (*docids)[b];
        }
        return a < b;
    });

    std::vector<uint32_t> pos_off;
    if (has_positions) {
        RETURN_IF_ERROR(reserve_tracked_vector(&pos_off, n, memory_reporter, &pos_off_reservation));
        pos_off.resize(n);
        uint32_t running = 0;
        for (size_t i = 0; i < n; ++i) {
            pos_off[i] = running;
            running += (*freqs)[i];
        }
    }
    std::vector<uint32_t> nd, nf, np;
    RETURN_IF_ERROR(reserve_tracked_vector(&nd, n, memory_reporter, &sorted_docids_reservation));
    RETURN_IF_ERROR(reserve_tracked_vector(&nf, n, memory_reporter, &sorted_freqs_reservation));
    if (has_positions) {
        RETURN_IF_ERROR(reserve_tracked_vector(&np, positions_flat->size(), memory_reporter,
                                               &sorted_positions_reservation));
    }
    for (size_t k : order) {
        // Coalesce a revisited docid into the previous entry (it sorts adjacent now):
        // sum freqs and append this group's positions right after the prior group's,
        // so flat doc order stays partitioned by the merged freqs.
        if (!nd.empty() && nd.back() == (*docids)[k]) {
            if (has_positions) {
                nf.back() += (*freqs)[k];
            }
        } else {
            nd.push_back((*docids)[k]);
            nf.push_back((*freqs)[k]);
        }
        if (has_positions) {
            np.insert(np.end(), positions_flat->begin() + pos_off[k],
                      positions_flat->begin() + pos_off[k] + (*freqs)[k]);
        }
    }
    docids->swap(nd);
    freqs->swap(nf);
    std::swap(*docids_reservation, sorted_docids_reservation);
    std::swap(*freqs_reservation, sorted_freqs_reservation);
    if (has_positions) {
        positions_flat->swap(np);
        std::swap(*positions_reservation, sorted_positions_reservation);
    }
    return Status::OK();
}

} // namespace

namespace {

// Decodes one varint from a pool chain cursor. The chain was written by
// encode_varint*, so the same LEB128 continuation-bit loop reconstructs it.
uint64_t decode_chain_varint(CompactPostingPool::Cursor* c) {
#ifdef BE_TEST
    g_compact_chain_varint_decodes.fetch_add(1, std::memory_order_relaxed);
#endif
    return c->read_varint();
}

} // namespace

// Decodes the compact tagged chain directly into caller-owned posting windows.
class SpimiTermBuffer::ArenaTermPostingSource final : public TermPostingSource {
public:
    ArenaTermPostingSource(const CompactPostingPool* pool, const Term& term)
            : shape_(term.shape),
              remaining_docs_(term.ndocs),
              remaining_tokens_(term.ntok),
              inline_docid_(term.cur_docid) {
        if (term.head != kNoChain) {
            doc_cursor_.emplace(pool->cursor(term.head, term.w.cur));
        }
    }

    Status fill(uint32_t target_docs, TermPostingBuffer* out, bool* exhausted) override {
        if (out == nullptr || exhausted == nullptr || target_docs == 0 || !out->empty()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spimi arena source: invalid fill arguments");
        }
        const uint32_t count = std::min(target_docs, remaining_docs_);
        if (count == 0) {
            *exhausted = true;
            return Status::OK();
        }

        if (shape_ == PostingChainShape::kStatlessDocsOnly) {
            MutableTermPostingSpan destination;
            RETURN_IF_ERROR(out->grow_uninitialized(count, /*has_freqs=*/false,
                                                    /*position_count=*/0, &destination));
            for (uint32_t i = 0; i < count; ++i) {
                if (!doc_cursor_) {
                    DCHECK_EQ(remaining_docs_, 1U);
                    destination.docids[i] = inline_docid_;
                } else {
                    absolute_docid_ += zigzag_decode(decode_chain_varint(&*doc_cursor_));
                    destination.docids[i] = static_cast<uint32_t>(absolute_docid_);
                }
            }
            remaining_tokens_ -= count;
        } else {
            RETURN_IF_ERROR(fill_tagged(count, out));
        }

        remaining_docs_ -= count;
        *exhausted = remaining_docs_ == 0;
        if (*exhausted) {
            DCHECK_EQ(remaining_tokens_, 0U);
            DCHECK(!pending_new_doc_);
        }
        return Status::OK();
    }

    bool exhausted() const { return remaining_docs_ == 0; }

private:
    Status fill_tagged(uint32_t count, TermPostingBuffer* out) {
        const bool has_positions = shape_ == PostingChainShape::kTaggedPositioned;
        const bool terminal_fill = count == remaining_docs_;
        const size_t position_count = has_positions && terminal_fill ? remaining_tokens_ : 0;
        MutableTermPostingSpan documents;
        RETURN_IF_ERROR(
                out->grow_uninitialized(count, /*has_freqs=*/true, position_count, &documents));
        size_t position_index = 0;
        for (uint32_t i = 0; i < count; ++i) {
            uint64_t tagged = 0;
            if (pending_new_doc_) {
                tagged = pending_tagged_;
                pending_new_doc_ = false;
            } else {
                DCHECK_GT(remaining_tokens_, 0U);
                tagged = decode_chain_varint(&*doc_cursor_);
            }
            DCHECK_NE(tagged & 1U, 0U);
            absolute_docid_ += zigzag_decode(decode_chain_varint(&*doc_cursor_));
            documents.docids[i] = static_cast<uint32_t>(absolute_docid_);
            uint32_t frequency = 0;
            while (true) {
                if (has_positions) {
                    if (terminal_fill) {
                        documents.positions_flat[position_index++] =
                                static_cast<uint32_t>(tagged >> 1);
                    } else {
                        RETURN_IF_ERROR(out->append_position(static_cast<uint32_t>(tagged >> 1)));
                    }
                }
                ++frequency;
                --remaining_tokens_;
                if (remaining_tokens_ == 0) {
                    break;
                }
                tagged = decode_chain_varint(&*doc_cursor_);
                if ((tagged & 1U) != 0) {
                    pending_tagged_ = tagged;
                    pending_new_doc_ = true;
                    break;
                }
            }
            documents.freqs[i] = frequency;
        }
        DCHECK_EQ(position_index, documents.positions_flat.size());
        return Status::OK();
    }

    PostingChainShape shape_;
    std::optional<CompactPostingPool::Cursor> doc_cursor_;
    uint32_t remaining_docs_ = 0;
    uint32_t remaining_tokens_ = 0;
    uint32_t inline_docid_ = 0;
    int64_t absolute_docid_ = 0;
    uint64_t pending_tagged_ = 0;
    bool pending_new_doc_ = false;
};

Status SpimiTermBuffer::to_postings(std::string term, Term&& t,
                                    TrackedTermPostings* tracked) const {
    DCHECK(tracked != nullptr);
    TermPostings& postings = tracked->postings;
    DCHECK(postings.docids.empty());
    DCHECK(postings.freqs.empty());
    DCHECK(postings.positions_flat.empty());
    postings.term = std::move(term);
    postings.retain_positions = t.shape == PostingChainShape::kTaggedPositioned;
    if (t.ntok == 0) {
        return Status::OK();
    }

    RETURN_IF_ERROR(reserve_tracked_vector(&postings.docids, t.ndocs, mem_reporter_,
                                           &tracked->docids_reservation));
    if (t.shape != PostingChainShape::kStatlessDocsOnly) {
        RETURN_IF_ERROR(reserve_tracked_vector(&postings.freqs, t.ndocs, mem_reporter_,
                                               &tracked->freqs_reservation));
    }
    if (t.shape == PostingChainShape::kTaggedPositioned) {
        RETURN_IF_ERROR(reserve_tracked_vector(&postings.positions_flat, t.ntok, mem_reporter_,
                                               &tracked->positions_reservation));
    }

    ArenaTermPostingSource source(&pool_, t);
    TermPostingBuffer buffer(mem_reporter_);
    bool exhausted = false;
    while (!exhausted) {
        buffer.clear_reuse();
        RETURN_IF_ERROR(source.fill(format::kAdaptiveWindowDocs, &buffer, &exhausted));
        postings.docids.insert(postings.docids.end(), buffer.docids().begin(),
                               buffer.docids().end());
        postings.freqs.insert(postings.freqs.end(), buffer.freqs().begin(), buffer.freqs().end());
        postings.positions_flat.insert(postings.positions_flat.end(),
                                       buffer.positions_flat().begin(),
                                       buffer.positions_flat().end());
    }
    if (!t.sorted && t.shape == PostingChainShape::kStatlessDocsOnly) {
        std::ranges::sort(postings.docids);
        postings.docids.erase(std::unique(postings.docids.begin(), postings.docids.end()),
                              postings.docids.end());
    } else if (!t.sorted) {
        RETURN_IF_ERROR(sort_by_docid(&postings.docids, &postings.freqs, &postings.positions_flat,
                                      postings.retain_positions, mem_reporter_,
                                      &tracked->docids_reservation, &tracked->freqs_reservation,
                                      &tracked->positions_reservation));
    }
    return Status::OK();
}

void SpimiTermBuffer::ensure_string_rank() const {
    const std::vector<std::string>& v = vocab();
    if (string_rank_.size() == v.size()) {
        return; // already built for the current append-only vocabulary
    }
    // Build the complete rank required by the first spill and by k-way merge
    // paths. Ordinary spills with a stale rank deliberately do not call here.
    if (!common_gram_pair_keys_) {
        std::vector<uint32_t> order(v.size());
        std::iota(order.begin(), order.end(), 0U);
        std::ranges::sort(order, [&](uint32_t a, uint32_t b) { return transient_term_less(a, b); });
        string_rank_.assign(v.size(), 0U);
        for (uint32_t rank = 0; rank < order.size(); ++rank) {
            string_rank_[order[rank]] = rank;
        }
    } else {
        size_t pair_count = 0;
        for (const std::string& term : v) {
            pair_count += is_common_gram_pair_key(term);
        }

        std::vector<uint32_t> plain_order;
        std::vector<uint64_t> pair_order;
        plain_order.reserve(v.size() - pair_count);
        pair_order.reserve(pair_count);
        for (uint32_t term_id = 0; term_id < v.size(); ++term_id) {
            if (is_common_gram_pair_key(v[term_id])) {
                pair_order.push_back(term_id);
            } else {
                plain_order.push_back(term_id);
            }
        }

        // EscapedV1 preserves logical byte order: 0x1e maps to 0x1eE and 0x1f
        // maps to 0x1eG. One physical sort therefore supplies both the final plain
        // order and the logical component rank used by a gram's right term.
        std::ranges::sort(plain_order,
                          [&](uint32_t left, uint32_t right) { return v[left] < v[right]; });
        string_rank_.assign(v.size(), 0U);
        for (uint32_t rank = 0; rank < plain_order.size(); ++rank) {
            string_rank_[plain_order[rank]] = rank;
        }

        // Decode each transient pair exactly once. The low word remains its term id;
        // the high word temporarily carries the left plain id, while the pair's
        // unused rank slot carries its right component's logical rank.
        for (uint64_t& decorated_pair : pair_order) {
            const uint32_t pair_term_id = static_cast<uint32_t>(decorated_pair);
            const CommonGramPairIds ids = decode_common_gram_pair_key_unchecked(v[pair_term_id]);
            DCHECK_LT(ids.left.value, v.size());
            DCHECK_LT(ids.right.value, v.size());
            DCHECK(!is_common_gram_pair_key(v[ids.left.value]));
            DCHECK(!is_common_gram_pair_key(v[ids.right.value]));
            string_rank_[pair_term_id] = string_rank_[ids.right.value];
            decorated_pair = (static_cast<uint64_t>(ids.left.value) << 32) | pair_term_id;
        }

        // The physical gram key orders its left component by fixed-width encoded
        // length, then by logical bytes. Stable per-length offsets convert the
        // already-logically-sorted plain ids into that compound dense rank without
        // another comparison sort.
        std::vector<uint32_t> next_length_rank(
                segment_v2::inverted_index::COMMON_GRAM_MAX_ENCODED_BYTES + 1, 0U);
        for (uint32_t plain_id : plain_order) {
            const size_t logical_size = LogicalPlainKeyView(v[plain_id]).size();
            DCHECK_LT(logical_size, next_length_rank.size());
            ++next_length_rank[logical_size];
        }
        uint32_t next_rank = 0;
        for (uint32_t& length_count : next_length_rank) {
            const uint32_t count = length_count;
            length_count = next_rank;
            next_rank += count;
        }
        DCHECK_EQ(next_rank, plain_order.size());
        for (uint32_t plain_id : plain_order) {
            const size_t logical_size = LogicalPlainKeyView(v[plain_id]).size();
            string_rank_[plain_id] = next_length_rank[logical_size]++;
        }
        for (uint64_t& decorated_pair : pair_order) {
            const uint32_t left_plain_id = static_cast<uint32_t>(decorated_pair >> 32);
            const uint32_t pair_term_id = static_cast<uint32_t>(decorated_pair);
            decorated_pair =
                    (static_cast<uint64_t>(string_rank_[left_plain_id]) << 32) | pair_term_id;
        }

        std::ranges::sort(pair_order, [&](uint64_t left, uint64_t right) {
            const uint32_t left_component_rank = static_cast<uint32_t>(left >> 32);
            const uint32_t right_component_rank = static_cast<uint32_t>(right >> 32);
            if (left_component_rank != right_component_rank) {
                return left_component_rank < right_component_rank;
            }
            return string_rank_[static_cast<uint32_t>(left)] <
                   string_rank_[static_cast<uint32_t>(right)];
        });

        // No EscapedV1 plain key enters 0x1f, so every materialized gram forms one
        // contiguous namespace group between the two physical-plain ranges.
        const auto pair_position = std::lower_bound(
                plain_order.begin(), plain_order.end(), segment_v2::inverted_index::CG_V1_MARKER,
                [&](uint32_t plain_id, std::string_view marker) { return v[plain_id] < marker; });
        uint32_t final_rank = 0;
        for (auto it = plain_order.begin(); it != pair_position; ++it) {
            string_rank_[*it] = final_rank++;
        }
        for (uint64_t decorated_pair : pair_order) {
            string_rank_[static_cast<uint32_t>(decorated_pair)] = final_rank++;
        }
        for (auto it = pair_position; it != plain_order.end(); ++it) {
            string_rank_[*it] = final_rank++;
        }
        DCHECK_EQ(final_rank, v.size());
    }
#ifdef BE_TEST
    g_string_rank_rebuilds.fetch_add(1, std::memory_order_relaxed);
#endif
}

std::vector<uint32_t> SpimiTermBuffer::sorted_ids() const {
    std::vector<uint32_t> ids = touched_ids_;
    const std::vector<std::string>& v = vocab();
    if (string_rank_.empty()) {
        // Preserve the fixed-vocabulary fast path: the first spill pays once for
        // a complete rank, then every later spill is integer-only until vocab grows.
        ensure_string_rank();
    }
    if (string_rank_.size() == v.size()) {
        order_ids_by_dense_rank(&ids, string_rank_);
    } else {
        // Vocabulary grew after the last complete rank. A run needs only its touched
        // terms in lexical order; defer the O(vocab log vocab) rebuild until a k-way
        // merge needs rank lookups for arbitrary ids. Reserve the same persistent
        // rank capacity the old rebuild allocated so resident accounting and later
        // spill-trigger timing remain unchanged.
        string_rank_.reserve(v.size());
        std::ranges::sort(ids, [&](uint32_t a, uint32_t b) { return transient_term_less(a, b); });
    }
    return ids;
}

void SpimiTermBuffer::release_term(uint32_t term_id) {
    const uint32_t enc = slot_of_[term_id];
    DCHECK_NE(enc, 0U);
    const uint32_t slot = enc - 1;
    slots_[slot] = Term(); // free this term's arrays; the empty Term slot is reusable
    free_slots_.push_back(slot);
    slot_of_[term_id] = 0;
    --live_term_count_;
}

Status SpimiTermBuffer::drain_sorted_streamed(const StreamedTermConsumer& fn) {
    const std::vector<std::string>& v = vocab();
    ensure_string_rank();
    report_arena_delta();
    order_ids_by_dense_rank(&touched_ids_, string_rank_);
    intern_ = decltype(intern_)(0, OwnedVocabHash {.vocab = &owned_vocab_},
                                OwnedVocabEq {&owned_vocab_});
    common_gram_pair_intern_ = decltype(common_gram_pair_intern_)();
    std::vector<CommonWordClassification>().swap(common_word_classification_);
    std::vector<uint32_t>().swap(string_rank_);
    report_arena_delta();

    constexpr size_t kSlotIndexPrefetchDistance = 32;
    constexpr size_t kTermPrefetchDistance = 16;
    Status callback_status = Status::OK();
    for (size_t ordinal = 0; ordinal < touched_ids_.size(); ++ordinal) {
        if (ordinal + kSlotIndexPrefetchDistance < touched_ids_.size()) {
            const uint32_t future_id = touched_ids_[ordinal + kSlotIndexPrefetchDistance];
            __builtin_prefetch(slot_of_.data() + future_id);
        }
        if (ordinal + kTermPrefetchDistance < touched_ids_.size()) {
            const uint32_t future_id = touched_ids_[ordinal + kTermPrefetchDistance];
            const uint32_t future_enc = slot_of_[future_id];
            DCHECK_NE(future_enc, 0U);
            __builtin_prefetch(slots_.data() + future_enc - 1);
            __builtin_prefetch(v.data() + future_id);
        }
        const uint32_t id = touched_ids_[ordinal];
        const uint32_t enc = slot_of_[id];
        DCHECK_NE(enc, 0U);
        Term term = slots_[enc - 1];
        slots_[enc - 1] = Term();
        slot_of_[id] = 0;
        --live_term_count_;

        std::string output_term = materialize_transient_term(v[id]);
        if (term.sorted) {
            ArenaTermPostingSource source(&pool_, term);
            StreamedTermPostings postings {
                    .term = std::move(output_term),
                    .retain_positions = term.shape == PostingChainShape::kTaggedPositioned,
                    .source = &source};
            callback_status = fn(std::move(postings));
            if (callback_status.ok() && !source.exhausted()) {
                callback_status = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                        "spimi arena source: consumer returned before term exhaustion");
            }
        } else {
            TrackedTermPostings materialized(mem_reporter_);
            callback_status = to_postings(std::move(output_term), std::move(term), &materialized);
            if (callback_status.ok()) {
                SpanTermPostingSource source(materialized.postings.docids,
                                             materialized.postings.freqs,
                                             materialized.postings.positions_flat);
                StreamedTermPostings postings {
                        .term = std::move(materialized.postings.term),
                        .retain_positions = materialized.postings.retain_positions,
                        .source = &source};
                callback_status = fn(std::move(postings));
                if (callback_status.ok() && !source.exhausted()) {
                    callback_status = Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                            "spimi span source: consumer returned before term exhaustion");
                }
            }
        }
        if (!callback_status.ok()) {
            break;
        }
    }

    pool_.reset();
    std::vector<Term>().swap(slots_);
    std::vector<uint32_t>().swap(free_slots_);
    std::vector<uint32_t>().swap(slot_of_);
    std::vector<uint32_t>().swap(touched_ids_);
    live_term_count_ = 0;
    std::vector<std::string>().swap(owned_vocab_);
    owned_vocab_heap_bytes_ = 0;
    common_gram_pair_cache_.reset();
    common_gram_pair_cache_bytes_ = 0;
    common_gram_plain_term_cache_.reset();
    common_gram_plain_term_cache_bytes_ = 0;
    trim_malloc();
    report_arena_delta();
    return callback_status;
}

Status SpimiTermBuffer::drain_to_writer(RunWriter* w) {
    Status st = Status::OK();
    const std::vector<std::string>& v = vocab();
    // Spill writes by term-id (no string IO). Iterate touched ids in vocab-string
    // order so each run is sorted; the k-way merge re-orders runs by the same key.
    for (uint32_t id : sorted_ids()) {
        const uint32_t enc = slot_of_[id];
        DCHECK_NE(enc, 0U);
        Term term = slots_[enc - 1];
        release_term(id);
        if (st.ok()) {
            TrackedTermPostings materialized(mem_reporter_);
            st = to_postings(v[id], std::move(term), &materialized);
            if (st.ok()) {
                st = w->write_term(id, materialized.postings);
            }
        }
    }
    touched_ids_.clear();
    pool_.reset(); // all chains decoded into the run; free the arena for the refill
    // The spill returns the arena to 0; slot_of_ keeps its capacity (survives
    // the spill). Report the arena-drop negative now so the gate-2 spill is balanced
    // immediately, not deferred to the next token.
    report_arena_delta();
    return st;
}

Status SpimiTermBuffer::compact_runs() {
    if (run_paths_.size() < 2) {
        return Status::OK();
    }
    // The compaction heap can encounter any id held by an earlier run, so it
    // requires a complete rank for the current vocabulary. New append-only ids
    // can shift existing lexicographic ranks, hence the explicit refresh here.
    ensure_string_rank();
    const std::string out_path = make_run_path(resolve_temp_dir());
    Status s =
            writer::compact_runs(run_paths_, string_rank_, has_positions_, out_path, mem_reporter_);
    if (!s.ok()) {
        std::remove(out_path.c_str()); // drop the partial output; inputs intact
        return s;
    }
    // The compacted run REPLACES its inputs at the FRONT of the run order:
    // it holds exactly runs [0..n) merged in run order, and any later run only
    // covers strictly-later docids, so per-term run-order concatenation (the
    // k-way merge invariant) is preserved.
    for (const std::string& p : run_paths_) {
        std::remove(p.c_str());
    }
    run_paths_.clear();
    run_paths_.push_back(out_path);
    g_run_compactions.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
}

Status SpimiTermBuffer::spill_to_run() {
    // G09 run-file cap: a buffer must never accumulate unbounded run files --
    // the final k-way merge (re)opens ALL of them simultaneously and holds
    // the fds for its whole duration, so unbounded runs across ~100
    // concurrent writers exhausted the BE nofile rlimit ('Too many open
    // files' at run reopen). At the cap, merge-compact the existing runs into
    // one before cutting the new run: the merge fan-in (and its fd count) is
    // bounded by cap + 1 per buffer.
    if (max_run_files_ != 0 && run_paths_.size() >= max_run_files_) {
        RETURN_IF_ERROR(compact_runs());
    }
    const std::string dir = resolve_temp_dir();
    // Best-effort space pre-check: fail with a clear, early error rather than a
    // mid-write IoError that leaves a half-written run. Best-effort only (TOCTOU; on
    // tmpfs this reports RAM). The ARENA -- not full resident_bytes(), which since
    // G08 also charges vocabulary structures a run never contains -- is what the
    // run re-encodes, and its block slack makes it a conservative over-estimate of
    // the run's on-disk size.
    const uint64_t arena = pool_.arena_bytes();
    const uint64_t avail = temp_dir_available_bytes(dir);
    if (avail < arena) {
        return Status::Error<ErrorCode::IO_ERROR, false>(
                "spimi: insufficient temp space in '" + dir + "' to spill ~" +
                std::to_string(arena) + " B (~" + std::to_string(avail) +
                " B free); set SNII_TEMP_DIR/TMPDIR to a larger disk");
    }
    const std::string path = make_run_path(dir);
    RunWriter w(mem_reporter_);
    RETURN_IF_ERROR(w.open(path));
    run_paths_.push_back(path); // tracked for cleanup even if a later step fails
    RETURN_IF_ERROR(drain_to_writer(&w));
    // The drain emptied touched_ids_ and released every live slot while retaining
    // capacity for the next fill.
    return w.close();
}

Status SpimiTermBuffer::prepare_run_merge(TermKeyMaterializer* materializer) {
    if (!touched_ids_.empty()) {
        Status status = spill_to_run();
        if (!status.ok() && spill_status_.ok()) {
            spill_status_ = status;
        }
    }
    if (!spill_status_.ok()) {
        return spill_status_;
    }

    std::vector<Term>().swap(slots_);
    std::vector<uint32_t>().swap(free_slots_);
    std::vector<uint32_t>().swap(slot_of_);
    std::vector<uint32_t>().swap(touched_ids_);
    common_gram_pair_cache_.reset();
    common_gram_pair_cache_bytes_ = 0;
    common_gram_plain_term_cache_.reset();
    common_gram_plain_term_cache_bytes_ = 0;
    common_gram_pair_intern_ = decltype(common_gram_pair_intern_)();
    std::vector<CommonWordClassification>().swap(common_word_classification_);
    trim_malloc();
    report_arena_delta();

    ensure_string_rank();
    report_arena_delta();
    intern_ = decltype(intern_)(0, OwnedVocabHash {.vocab = &owned_vocab_},
                                OwnedVocabEq {&owned_vocab_});
    report_arena_delta();
    if (common_gram_pair_keys_) {
        *materializer = [this](std::string_view term) { return materialize_transient_term(term); };
    }
    return Status::OK();
}

void SpimiTermBuffer::finish_run_merge() {
    std::vector<std::string>().swap(owned_vocab_);
    owned_vocab_heap_bytes_ = 0;
    std::vector<uint32_t>().swap(string_rank_);
    report_arena_delta();
    trim_malloc();
}

Status SpimiTermBuffer::merge_runs_streamed(const StreamedTermConsumer& fn) {
    TermKeyMaterializer materializer;
    RETURN_IF_ERROR(prepare_run_merge(&materializer));
    Status status = merge_run_sources(run_paths_, vocab(), string_rank_, has_positions_, fn,
                                      std::move(materializer), mem_reporter_);
    finish_run_merge();
    return status;
}

Status SpimiTermBuffer::for_each_term_sorted(const StreamedTermConsumer& fn) {
    // Single-drain contract: a second call would re-merge the (still-present) run
    // files and re-emit every term, or emit nothing in the in-memory path. Return
    // an error and emit NOTHING rather than produce a wrong second stream.
    if (drained_) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>(
                "spimi: already drained (single-drain contract)");
    }
    drained_ = true;
    if (run_paths_.empty() && spill_status_.ok()) {
        return drain_sorted_streamed(fn);
    }
    return merge_runs_streamed(fn);
}

std::vector<TermPostings> SpimiTermBuffer::finalize_sorted() {
    std::vector<TermPostings> out;
    out.reserve(touched_ids_.size());
    Status status = for_each_term_sorted([&out](StreamedTermPostings&& streamed) {
        TermPostings materialized;
        materialized.term = std::move(streamed.term);
        materialized.retain_positions = streamed.retain_positions;
        TermPostingBuffer buffer(nullptr);
        bool exhausted = false;
        while (!exhausted) {
            buffer.clear_reuse();
            RETURN_IF_ERROR(
                    streamed.source->fill(format::kAdaptiveWindowDocs, &buffer, &exhausted));
            materialized.docids.insert(materialized.docids.end(), buffer.docids().begin(),
                                       buffer.docids().end());
            materialized.freqs.insert(materialized.freqs.end(), buffer.freqs().begin(),
                                      buffer.freqs().end());
            materialized.positions_flat.insert(materialized.positions_flat.end(),
                                               buffer.positions_flat().begin(),
                                               buffer.positions_flat().end());
        }
        out.push_back(std::move(materialized));
        return Status::OK();
    });
    if (!status.ok() && spill_status_.ok()) {
        spill_status_ = status;
        std::vector<TermPostings>().swap(out);
    }
    return out;
}

void SpimiTermBuffer::cleanup_runs() {
    for (const std::string& p : run_paths_) {
        std::remove(p.c_str());
    }
    run_paths_.clear();
}

} // namespace doris::snii::writer
