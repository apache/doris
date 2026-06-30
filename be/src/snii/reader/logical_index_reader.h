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
#include <string_view>
#include <vector>

#include "common/status.h"
#include "snii/common/slice.h"
#include "snii/format/bsbf.h"
#include "snii/format/dict_block.h"
#include "snii/format/dict_block_directory.h"
#include "snii/format/dict_entry.h"
#include "snii/format/format_constants.h"
#include "snii/format/per_index_meta.h"
#include "snii/format/sampled_term_index.h"
#include "snii/format/stats_block.h"
#include "snii/io/file_reader.h"

// LogicalIndexReader -- read-side counterpart of LogicalIndexWriter for one
// logical index. It owns the resident per-index meta sub-readers (XFilter,
// SampledTermIndex, DICT block directory, StatsBlock, SectionRefs) parsed from
// the per-index meta block, and resolves a query term to its DictEntry through
// the documented lookup flow:
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
// The reader copies the meta block bytes at open so every parsed sub-reader has
// stable backing storage for the reader lifetime.
namespace snii::reader {

// Forward-declared: this widely-included header only names DictBlockCache* and
// shared_ptr<const DecodedDictBlock>*; the full definitions are pulled into the
// .cpp and into tests that construct a cache. Keeps the request-scoped cache
// header out of the ~500 TUs that transitively include this one.
struct DecodedDictBlock;
class DictBlockCache;

class LogicalIndexReader {
public:
    LogicalIndexReader() = default;

    // Parses the per-index meta block and binds the reader to file_reader.
    // file_reader / meta_block must outlive this reader.
    static doris::Status open(snii::io::FileReader* file_reader, snii::format::IndexTier tier,
                              bool has_positions, Slice meta_block, LogicalIndexReader* out);

    // Resolves term to a DictEntry. *found=false when the term is absent (XFilter
    // rejection, out-of-range sample, or DICT-block miss). On a hit, *entry is
    // filled and *frq_base / *prx_base carry the candidate block's bases.
    //
    // `cache` is an optional REQUEST-SCOPED DictBlockCache: when a single query
    // threads one cache through its per-term lookups, an on-demand DICT block hit
    // by several terms is decoded once instead of once per term. nullptr keeps the
    // pre-existing behavior (each lookup materializes its own block). The cache is
    // caller-owned, single-threaded, and never mutates this (const) reader.
    doris::Status lookup(std::string_view term, bool* found, snii::format::DictEntry* entry,
                         uint64_t* frq_base, uint64_t* prx_base,
                         DictBlockCache* cache = nullptr) const;

    // One enumerated term whose key has the requested prefix, with its DictEntry
    // and the owning DICT block's frq/prx bases (for posting resolution).
    struct PrefixHit {
        std::string term;
        snii::format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
    };

    using PrefixHitVisitor = std::function<doris::Status(PrefixHit&& hit, bool* stop)>;

    // Ordered term enumeration: every term with `prefix`, in lexicographic order,
    // by seeking the start DICT block via the SampledTermIndex and scanning
    // forward across contiguous blocks until the terms pass the prefix range.
    // Empty prefix enumerates all terms. This is the contiguous-DICT-block design
    // the term-anchor layout was built for (MATCH_PHRASE_PREFIX / prefix / range
    // queries). The visitor form avoids materializing all hits when callers only
    // need a bounded expansion.
    doris::Status visit_prefix_terms(std::string_view prefix, const PrefixHitVisitor& visitor,
                                     DictBlockCache* cache = nullptr) const;
    doris::Status prefix_terms(std::string_view prefix, std::vector<PrefixHit>* const out,
                               int32_t max_terms = 0, DictBlockCache* cache = nullptr) const;

    // Resolves a pod_ref entry's absolute .frq / .prx window byte range,
    // validating the locator against the posting_region length (defends against
    // corrupt entries: prelude_len > frq_len underflow, or off_delta+len past the
    // region). Both windows resolve against the single posting_region. *abs_off
    // is the absolute file offset of the window (after prelude); *len its byte
    // length.
    doris::Status resolve_frq_window(const snii::format::DictEntry& entry, uint64_t frq_base,
                                     uint64_t* abs_off, uint64_t* len) const;
    doris::Status resolve_prx_window(const snii::format::DictEntry& entry, uint64_t prx_base,
                                     uint64_t* abs_off, uint64_t* len) const;

    const snii::format::SectionRefs& section_refs() const { return meta_.section_refs(); }
    const snii::format::StatsBlock& stats() const { return meta_.stats(); }
    snii::format::IndexTier tier() const { return tier_; }
    bool has_positions() const { return has_positions_; }
    snii::io::FileReader* reader() const { return reader_; }
    size_t memory_usage() const;

private:
    snii::io::FileReader* reader_ = nullptr;
    snii::format::IndexTier tier_ = snii::format::IndexTier::kT1;
    bool has_positions_ = false;
    std::vector<uint8_t> meta_block_;
    snii::format::PerIndexMetaReader meta_;
    snii::format::SampledTermIndexReader sti_;
    snii::format::DictBlockDirectoryReader dbd_;
    snii::format::BsbfHeader bsbf_header_; // resident header (from section ref)
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
        snii::format::DictBlockReader reader;
    };
    doris::Status load_resident_dict_blocks();
    // Resolves the DictBlockReader for `ordinal`. Resident blocks return a pointer
    // into the reader-owned resident set with *pin left null (stable for the reader
    // lifetime). On-demand blocks are decoded (optionally via the request-scoped
    // `cache`) into a heap-allocated DecodedDictBlock; *pin holds it alive so *out
    // never dangles under a later cache eviction. Callers must keep *pin alive for
    // as long as they use *out.
    doris::Status dict_block_reader_for_ordinal(uint32_t ordinal, DictBlockCache* cache,
                                                std::shared_ptr<const DecodedDictBlock>* pin,
                                                const snii::format::DictBlockReader** out) const;
    std::vector<ResidentDictBlock> resident_dict_blocks_;
};

} // namespace snii::reader
