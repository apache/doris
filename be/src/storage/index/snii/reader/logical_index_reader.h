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
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/format/bsbf.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/metadata_blob.h"
#include "storage/index/snii/format/sampled_term_index.h"
#include "storage/index/snii/format/stats_block.h"
#include "storage/index/snii/io/file_reader.h"

// LogicalIndexReader -- read-side counterpart of LogicalIndexWriter for one
// logical index. It owns decoded Core, SampledTermIndex, and DICT block-directory
// state from one adjacent metadata group, and resolves a query term to its
// DictEntry through the documented lookup flow:
//   XFilter (reject absent) -> SampledTermIndex (candidate block ordinal) ->
//   DICT block directory (block range) -> resident small-DICT block or one
//   range read of the DICT block -> DictBlockReader::find_term.
//
// lookup() also returns the block's frq_base/prx_base (captured by the
// DictBlockReader) so callers can resolve a pod_ref entry's absolute .frq/.prx
// offsets via the writer's contract. Both deltas index into the SAME
// interleaved posting region (prx_base == frq_base; the prx span precedes the
// frq span):
//   abs_frq = posting_region.offset + frq_base + entry.frq_off_delta
//   abs_prx = posting_region.offset + prx_base + entry.prx_off_delta
//
// The reader retains no raw metadata-group bytes after open.
namespace doris::snii::format {
class NormsPodReader;
}

namespace doris::snii::reader {

// Forward-declared: this widely-included header only names DictBlockCache* and
// shared_ptr<const DecodedDictBlock>*; the full definitions are pulled into the
// .cpp and into tests that construct a cache. Keeps the request-scoped cache
// header out of the ~500 TUs that transitively include this one.
struct DecodedDictBlock;
class DictBlockCache;

enum class LogicalIndexOpenMode : uint8_t {
    kQuery,
    kCompaction,
};

struct DictBlockScanMemory {
    uint64_t decode_bytes = 0;
    uint64_t entries_bytes = 0;
};

struct NullDocidsScanMemory {
    uint64_t frame_bytes = 0;
    uint64_t output_bytes = 0;
};

class LogicalIndexReader {
public:
    LogicalIndexReader() = default;

    // Parses one mandatory Core/STI/DBD metadata group and binds the reader to
    // file_reader. The reader retains decoded state, not the input byte slices.
    static Status open(io::FileReader* file_reader, Slice core_frame, Slice sti_blob,
                       Slice dbd_blob, LogicalIndexReader* out,
                       LogicalIndexOpenMode open_mode = LogicalIndexOpenMode::kQuery);

    // Resolves term to a DictEntry. *found=false when the term is absent (XFilter
    // rejection, out-of-range sample, or DICT-block miss). On a hit, *entry is
    // filled and *frq_base / *prx_base carry the candidate block's bases.
    //
    // `cache` is an optional REQUEST-SCOPED DictBlockCache: when a single query
    // threads one cache through its per-term lookups, an on-demand DICT block hit
    // by several terms is decoded once instead of once per term. nullptr keeps the
    // pre-existing behavior (each lookup materializes its own block). The cache is
    // caller-owned, single-threaded, and never mutates this (const) reader.
    Status lookup(std::string_view term, bool* found, format::DictEntry* entry, uint64_t* frq_base,
                  uint64_t* prx_base, DictBlockCache* cache = nullptr) const;

    struct BatchLookupResult {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
    };

    // Resolves one sorted, duplicate-free term batch. Terms are first mapped to
    // candidate DICT ordinals through the same XFilter/STI path as lookup(), then
    // distinct on-demand blocks are fetched concurrently in bounded waves.
    // Results stay aligned with `terms`; absent terms have found=false.
    Status lookup_batch(const std::vector<std::string>& terms,
                        std::vector<BatchLookupResult>* results) const;

    // One enumerated term whose key has the requested prefix, with its DictEntry
    // and the owning DICT block's frq/prx bases (for posting resolution).
    struct PrefixHit {
        std::string term;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
    };

    using PrefixHitVisitor = std::function<Status(PrefixHit&& hit, bool* stop)>;

    // Ordered term enumeration: every term with `prefix`, in lexicographic order,
    // by seeking the start DICT block via the SampledTermIndex and scanning
    // forward across contiguous blocks until the terms pass the prefix range.
    // Empty prefix enumerates all terms. This is the contiguous-DICT-block design
    // the term-anchor layout was built for (MATCH_PHRASE_PREFIX / prefix / range
    // queries). The visitor form avoids materializing all hits when callers only
    // need a bounded expansion.
    Status visit_prefix_terms(std::string_view prefix, const PrefixHitVisitor& visitor,
                              DictBlockCache* cache = nullptr) const;
    Status visit_term_range(std::string_view lower_inclusive,
                            std::optional<std::string_view> upper_exclusive,
                            const PrefixHitVisitor& visitor, DictBlockCache* cache = nullptr) const;
    Status prefix_terms(std::string_view prefix, std::vector<PrefixHit>* const out,
                        int32_t max_terms = 0, DictBlockCache* cache = nullptr) const;

    // ---- Sequential whole-dictionary access (T2.3, compaction index merge) ----
    // Number of DICT blocks in this index (0 for an empty dictionary).
    uint32_t n_dict_blocks() const { return dbd_.n_blocks(); }
    // Decodes EVERY entry of DICT block `ordinal` in lexicographic order into
    // *entries (each self-contained, owning its term and any inline posting
    // bytes) and returns the block's frq/prx bases. One block is materialized
    // at a time so a full-dictionary scan (SniiSegmentTermCursor) holds a single
    // block's entries, never the whole vocabulary. ordinal must be
    // < n_dict_blocks().
    Status decode_dict_block(uint32_t ordinal, std::vector<format::DictEntry>* entries,
                             uint64_t* frq_base, uint64_t* prx_base) const;
    // Returns conservative pre-allocation charges for the on-demand block decode
    // and its fully materialized DictEntry vector. Compaction cursors reserve both
    // before decoding so MEM_LIMIT_EXCEEDED is returned before the normal block
    // allocations whenever the shared merge cap cannot admit them.
    Status dict_block_scan_memory(uint32_t ordinal, DictBlockScanMemory* out) const;

    // Resolves a pod_ref entry's absolute .frq / .prx window byte range,
    // validating the locator against the posting_region length (defends against
    // corrupt entries: prelude_len > frq_len underflow, or off_delta+len past the
    // region). Both windows resolve against the single posting_region. *abs_off
    // is the absolute file offset of the window (after prelude); *len its byte
    // length.
    Status resolve_frq_window(const format::DictEntry& entry, uint64_t frq_base, uint64_t* abs_off,
                              uint64_t* len) const;
    Status resolve_prx_window(const format::DictEntry& entry, uint64_t prx_base, uint64_t* abs_off,
                              uint64_t* len) const;

    const format::SectionRefs& section_refs() const { return core_.section_refs; }
    const format::StatsBlock& stats() const { return core_.stats; }
    format::IndexTier tier() const { return tier_; }
    bool has_positions() const { return has_positions_; }
    LogicalIndexOpenMode open_mode() const { return open_mode_; }
    const segment_v2::inverted_index::CommonGramsSegmentMetadata* common_grams_metadata() const {
        return core_.common_grams_metadata ? &*core_.common_grams_metadata : nullptr;
    }
    format::CommonGramsPostingPolicy common_grams_posting_policy() const {
        return core_.common_grams_posting_policy;
    }
    io::FileReader* reader() const { return reader_; }

    // Returns a reader over the validated norms section. The first call reads
    // and validates the section; later calls share the immutable reader-owned
    // bytes. The full on-disk section is reserved in memory_usage() before this
    // LogicalIndexReader enters the searcher cache, so lazy loading cannot make
    // the cache under-report its eventual resident size.
    Status open_norms(format::NormsPodReader* out) const;
    // Compaction scans one source norm vector at a time. This charge matches the
    // reader's full cache accounting; release_compaction_norms() drops the loaded
    // frame after its values have been scattered into destination vectors.
    size_t compaction_norms_cache_charge() const { return norms_reserved_charge_; }
    void release_compaction_norms() const;

    // Reads and validates the sparse null-bitmap side POD, then returns its
    // docids in ascending order. Work is O(null_count), not O(doc_count), which
    // lets compaction remap NULL rows without scanning the complete document
    // domain. A missing section is valid only when StatsBlock::null_count is 0.
    using NullDocidsDecodeReservation = std::function<Status(uint64_t bytes)>;
    Status read_null_docids(std::vector<uint32_t>* out,
                            const NullDocidsDecodeReservation& reserve_decode =
                                    NullDocidsDecodeReservation()) const;
    Status null_docids_scan_memory(NullDocidsScanMemory* out) const;
    size_t memory_usage() const;

private:
    struct NormsCacheState;
    struct BatchLookupCandidate {
        size_t term_index = 0;
        uint32_t ordinal = 0;
    };
    struct BatchLookupGroup {
        uint32_t ordinal = 0;
        size_t begin = 0;
        size_t end = 0;
    };
    struct PendingBatchLookupBlock {
        size_t group_index = 0;
        format::BlockRef ref;
        size_t handle = 0;
    };
    io::FileReader* reader_ = nullptr;
    format::IndexTier tier_ = format::IndexTier::kT1;
    bool has_positions_ = false;
    LogicalIndexOpenMode open_mode_ = LogicalIndexOpenMode::kQuery;
    format::CoreMetadata core_;
    format::SampledTermIndexReader sti_;
    format::DictBlockDirectoryReader dbd_;
    format::BsbfHeader bsbf_header_; // resident header (from section ref)
    bool has_bsbf_ = false;
    // L0 tiering: when the bsbf section is small (<= kBsbfResidentMaxBytes) its
    // whole bitset is loaded here at open -> in-memory probe, no per-lookup
    // round. Larger filters keep only the parsed header here, so the small
    // header enters Doris searcher cache and lookup reads just one 32-byte body
    // block for an L1 probe.
    bool bsbf_resident_ = false;
    std::vector<uint8_t> bsbf_resident_bitset_;

    // Small DICT blocks are opened once with the index so exact lookups avoid an
    // otherwise serial S3 round for the term dictionary. Empty means the
    // dictionary exceeded the resident threshold and lookup/prefix enumeration
    // read blocks on demand. Each DictBlockReader holds a Slice into the owning
    // bytes.
    struct ResidentDictBlock {
        std::vector<uint8_t> bytes;
        format::DictBlockReader reader;
    };
    Status load_resident_dict_blocks();
    Status load_resident_bsbf();
    // Resolves the DictBlockReader for `ordinal`. Resident blocks return a pointer
    // into the reader-owned resident set with *pin left null (stable for the reader
    // lifetime). On-demand blocks are decoded (optionally via the request-scoped
    // `cache`) into a heap-allocated DecodedDictBlock; *pin holds it alive so *out
    // never dangles under a later cache eviction. Callers must keep *pin alive for
    // as long as they use *out.
    Status dict_block_reader_for_ordinal(uint32_t ordinal, DictBlockCache* cache,
                                         std::shared_ptr<const DecodedDictBlock>* pin,
                                         const format::DictBlockReader** out) const;
    Status locate_candidate_dict_block(std::string_view term, bool* maybe_present,
                                       uint32_t* ordinal) const;
    Status collect_batch_lookup_groups(const std::vector<std::string>& terms,
                                       std::vector<BatchLookupCandidate>* candidates,
                                       std::vector<BatchLookupGroup>* groups) const;
    static Status resolve_batch_lookup_group(const std::vector<std::string>& terms,
                                             const std::vector<BatchLookupCandidate>& candidates,
                                             const BatchLookupGroup& group,
                                             const format::DictBlockReader& block_reader,
                                             std::vector<BatchLookupResult>* results);
    Status lookup_batch_on_demand(const std::vector<std::string>& terms,
                                  const std::vector<BatchLookupCandidate>& candidates,
                                  const std::vector<BatchLookupGroup>& groups,
                                  std::vector<BatchLookupResult>* results) const;
    std::vector<ResidentDictBlock> resident_dict_blocks_;
    std::shared_ptr<NormsCacheState> norms_cache_;
    size_t norms_reserved_charge_ = 0;
};

} // namespace doris::snii::reader
