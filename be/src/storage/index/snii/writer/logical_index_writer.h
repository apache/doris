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

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/metadata_blob.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/format/sampled_term_index.h"
#include "storage/index/snii/format/stats_block.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/spillable_byte_buffer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii/writer/term_posting_source.h"

// LogicalIndexWriter -- builds the per-logical-index section bytes (interleaved
// posting region + DICT block region) plus the SampledTermIndex and DICT block
// directory metadata for ONE logical index. It owns the in-memory section bytes,
// runtime statistics, and references needed by the container orchestrator
// (SniiCompoundWriter) to resolve absolute offsets and emit the Core/STI/DBD
// metadata group.
//
// This module deliberately produces ONLY relative bytes/structures: it has no
// knowledge of the absolute file position where the sections will land. The
// orchestrator stitches the absolute offsets in afterward (append-only, no
// seek-back). See snii_compound_writer.h for the precise offset contract.
//
// POSTING REGION (single interleaved sink): the former separate .frq POD and .prx
// POD are merged into ONE posting region. For each pod_ref term, in term order, the
// writer appends its prx span FIRST then its frq span, contiguously:
//   posting region = concat over pod_ref terms of [prx span][frq span].
// The prx span is empty when !has_prx (docs-only / keyword tier). INLINE terms
// append NOTHING to the posting region.
//
// Per-term encoding policy (v1):
//   df >= kSlimDfThreshold (512), or a lower-df term whose positions cannot fit
//     one configured reader-safe PRX window: WINDOWED pod_ref. The term's [prx
//     windows] are appended to the posting region first, then its
//     [prelude][dd-block][freq-block] frq span. The DictEntry records frq/prx
//     off_delta+len relative to frq_base/prx_base (see below).
//   Other df < kSlimDfThreshold terms: SLIM. The postings are encoded as a
//     single .frq window (and .prx window). If the encoded .frq bytes are small
//     (<= kDefaultInlineThreshold), they are stored INLINE inside the DictEntry
//     (kind=inline); otherwise the term's [prx][frq] spans are appended to the
//     posting region as a slim pod_ref (kind=pod_ref, enc=slim, no prelude).
//
// frq_base / prx_base convention (DOCUMENTED CONTRACT):
//   For each DICT block, frq_base == prx_base == the running byte offset into THIS
//   index's posting region at the moment the block opens (the posting-region size
//   when the block's first POD-backed entry is appended). A windowed/slim pod_ref
//   entry then sets frq_off_delta = (offset of its frq span within the posting
//   region) - frq_base, so the reader computes the absolute file offset as
//     section_refs.posting_region.offset + frq_base + frq_off_delta.
//   prx_base / prx_off_delta follow the identical rule against the SAME region.
//   Because [prx][frq] are written contiguously per term, a writer-side property
//   holds when has_prx: frq_off_delta == prx_off_delta + prx_len. The reader does
//   NOT rely on it -- each delta is resolved independently.
//   Inline entries carry no off_delta (bytes live in the entry).
namespace doris::snii::writer {

class SniiStreamedIndexSession;
class StreamingTermEncoder;

struct SerializedMetadataGroup {
    std::vector<uint8_t> core;
    std::vector<uint8_t> sampled_term_index;
    std::vector<uint8_t> dict_block_directory;
};

// Inputs describing one logical index to be written.
struct SniiIndexInput {
    uint64_t index_id = 0;
    std::string index_suffix;
    format::IndexConfig config = format::IndexConfig::kDocsPositions;
    uint32_t doc_count = 0;
    std::vector<uint32_t> null_docids;
    // Per-doc 1-byte encoded norm (length doc_count); only consumed when the
    // config has scoring. May be empty otherwise.
    std::vector<uint8_t> encoded_norms;
    // G16-h: zstd levels for the dict-block whole-block compression and the
    // .prx window auto mode (both default 3 == the historical constants).
    // Higher levels trade import CPU for size; decode speed is unaffected.
    int dict_block_zstd_level = 3;
    int prx_zstd_level = 3;
    // Internal writer policy. Production callers keep the reader limits; unit
    // tests may only tighten them to exercise extreme-window behavior without
    // allocating hundreds of MiB.
    format::PrxWindowLimits prx_window_limits = format::kReaderPrxWindowLimits;
    // G16-c: whether freq-capable (tier>=T2) postings lay out freq regions at
    // all. Freq bytes serve ONLY BM25 scoring (want_freq=true lives solely in
    // scoring_query), so the CALLER resolves the policy -- the Doris adapter
    // passes has_scoring(config) || config::snii_positions_index_write_freq,
    // i.e. plain kDocsPositions indexes drop freq unless the escape hatch is
    // set. Defaults to true so the core library and existing callers keep the
    // full T2 layout unless they opt out. The drop is value-driven on disk
    // (windowed prelude flags bit0; slim/inline zero-length freq regions), so
    // readers need no index-level flag. Ignored for docs-only configs.
    bool write_freq = true;
    // Lexicographically sorted terms with ascending-docid postings. Used when
    // `term_source` is null (callers that already hold a materialized vector,
    // e.g. unit tests). The writer reads but does not retain these.
    std::vector<TermPostings> terms;
    // Optional streaming term source. When non-null, the writer DRAINS it via
    // SpimiTermBuffer::for_each_term_sorted so that only one term's postings is
    // materialized at a time (avoiding the full TermPostings vector and its
    // second-copy peak). `terms` is ignored when this is set. The buffer is
    // consumed (emptied) by build(); the caller must keep it alive until build()
    // returns and must not reuse it afterwards.
    SpimiTermBuffer* term_source = nullptr;
    // Target DICT block size in bytes; a block is cut once its estimate reaches
    // this. 0 uses kDefaultTargetDictBlockBytes. Smaller values yield more blocks
    // (and a finer-grained sampled-term index).
    uint32_t target_dict_block_bytes = 0;
    // Maximum resident capacity of the staged DICT region before it spills.
    // Ordinary builds keep the default unlimited local cap and use their shared
    // spill-threshold reporter. Streamed compaction sets a bounded watermark so
    // reclaimable DICT blocks cannot consume the hard-capped term workspace.
    uint64_t dict_resident_cap_bytes = std::numeric_limits<uint64_t>::max();
    // Optional writer-level build-RAM reporter (one per SniiCompoundWriter = one
    // segment inverted index). When non-null, the dict buffer reports its REAL
    // resident-byte deltas (positive on grow, negative on spill). The SPIMI side
    // (arena + slot index) reports through the SAME reporter, injected directly at
    // the term_source's construction by the caller. null in bench / unit tests -> no
    // reporting. NEVER report live_bytes_ (a gated estimate); report
    // arena_bytes()+slot_of_+dict ram_bytes_.
    MemoryReporter* mem_reporter = nullptr;
    // Optional persisted CommonGrams capability and semantic scoring stats.
    // Missing metadata preserves the legacy SNII image and cannot be treated as
    // compatibility proof by readers.
    std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata> common_grams_metadata;
    // Optional per-term CommonGrams postings shape. HybridV1 requires Mixed
    // coverage metadata and a positions-capable logical index.
    format::CommonGramsPostingPolicy common_grams_posting_policy =
            format::CommonGramsPostingPolicy::kNone;
};

// Move-only ownership of a NULL-docid allocation and its precharged bytes.
// Reservation is declared first so destruction always frees the vector before
// returning its charge. Move assignment is intentionally forbidden because its
// default member order would release the destination charge before its vector.
class TrackedNullDocids {
public:
    explicit TrackedNullDocids(std::vector<uint32_t>&& docids) : docids_(std::move(docids)) {}
    TrackedNullDocids(MemoryReporter::Reservation&& reservation, std::vector<uint32_t>&& docids)
            : reservation_(std::move(reservation)), docids_(std::move(docids)) {}

    TrackedNullDocids(const TrackedNullDocids&) = delete;
    TrackedNullDocids& operator=(const TrackedNullDocids&) = delete;
    TrackedNullDocids(TrackedNullDocids&&) noexcept = default;
    TrackedNullDocids& operator=(TrackedNullDocids&&) = delete;

    bool empty() const { return docids_.empty(); }
    size_t size() const { return docids_.size(); }
    const uint32_t* data() const { return docids_.data(); }
    uint32_t operator[](size_t index) const { return docids_[index]; }
    auto begin() const { return docids_.begin(); }
    auto end() const { return docids_.end(); }

    void release() {
        std::vector<uint32_t>().swap(docids_);
        reservation_.reset();
    }

private:
    MemoryReporter::Reservation reservation_;
    std::vector<uint32_t> docids_;
};

// Move-only ownership of a destination norms allocation and its precharged
// bytes. Streamed sessions adopt both together so the vector remains accounted
// for until the writer has materialized the norms section.
class TrackedEncodedNorms {
public:
    explicit TrackedEncodedNorms(std::vector<uint8_t>&& norms) : norms_(std::move(norms)) {}
    TrackedEncodedNorms(MemoryReporter::Reservation&& reservation, std::vector<uint8_t>&& norms)
            : reservation_(std::move(reservation)), norms_(std::move(norms)) {}

    TrackedEncodedNorms(const TrackedEncodedNorms&) = delete;
    TrackedEncodedNorms& operator=(const TrackedEncodedNorms&) = delete;
    TrackedEncodedNorms(TrackedEncodedNorms&&) noexcept = default;
    TrackedEncodedNorms& operator=(TrackedEncodedNorms&&) = delete;

    bool empty() const { return norms_.empty(); }
    size_t size() const { return norms_.size(); }
    uint8_t operator[](size_t index) const { return norms_[index]; }
    auto begin() const { return norms_.begin(); }
    auto end() const { return norms_.end(); }

    void release() {
        std::vector<uint8_t>().swap(norms_);
        reservation_.reset();
    }

private:
    friend class SniiStreamedIndexSession;
    MemoryReporter::Reservation reservation_;
    std::vector<uint8_t> norms_;
};

// Term-level frequency statistics. Ordinary terms compute sum(freqs) and
// max(freqs) in one fused scan. Complete CommonGrams entries derive total_freq
// from their required PRX position count and leave max_freq at 0 because their
// statless DICT block does not serialize it.
struct FreqStats {
    uint64_t total_freq = 0;
    uint32_t max_freq = 0;
};

// Builds and holds the section bytes + meta sub-sections for one logical index.
class LogicalIndexWriter {
public:
    explicit LogicalIndexWriter(const SniiIndexInput& in);
    // Out-of-line: stream_state_ points at the private nested BlockState, which
    // is incomplete here (unique_ptr needs the complete type at destruction).
    ~LogicalIndexWriter();

    // Builds DICT blocks, the interleaved posting region, sampled-term index, dict
    // directory, stats and bsbf. The posting region is written STRAIGHT into
    // `posting_out` as terms are produced (no temp round-trip for the bulk); the
    // orchestrator captures its absolute offset/length from posting_out->bytes_written()
    // around this call. Must be called once before the accessors below. Returns
    // InvalidArgument on a null sink or inconsistent input (e.g. norms/doc_count
    // mismatch when scoring is enabled, or non-ascending docids).
    Status build(io::FileWriter* posting_out);

    // Streamed three-phase alternative to build() (T2.1, the compaction index
    // merge fast path): the CALLER produces terms one at a time (k-way merge
    // over source segments) instead of handing the writer a term source.
    //   begin_streamed(sink) -> push_term(tp) x N -> finish_streamed()
    // push_term funnels through the SAME process_term choke point build()
    // drains through (bigram prune gates, shape validation, encode), and the
    // setup/finalize steps are shared with build() -- so the produced bytes are
    // IDENTICAL to a build() fed the same terms in the same order (the T2
    // byte-golden invariant). A writer instance runs EXACTLY ONE session:
    // build() and begin_streamed are mutually exclusive, push_term after
    // finish_streamed and a second finish_streamed are errors -- a half-fed
    // session can never masquerade as a sealed index (crash-safety invariant 6).
    // Failure poisons the session: any push_term/finish_streamed/build failure
    // past entry validation (e.g. a posting-sink append error mid-encode) moves
    // the writer to a terminal failed state where every subsequent
    // push_term/finish_streamed/build/begin_streamed is rejected, so a caller
    // that swallows an error can never seal (or re-claim) a corrupt index.
    // Entry rejections themselves (term-order / postings-shape / phase checks)
    // do not poison an active session EXCEPT postings-shape violations, which
    // fail inside the shared process_term and conservatively poison too; only
    // the term-order guard is explicitly recoverable (see the UT contract).
    Status begin_streamed(io::FileWriter* posting_out);
    // Consumes tp synchronously. Entry validation: terms must arrive in STRICTLY
    // increasing lexicographic order (the one invariant process_term cannot see
    // -- DICT blocks, the sampled term index and the reader's binary search all
    // assume it; equal terms are rejected too, the upstream merge must have
    // combined duplicates). The streaming encoder rejects invalid per-term
    // posting shapes. Returns InvalidArgument on any violation.
    Status push_term(StreamedTermPostings&& tp);
    Status finish_streamed();

    // DICT region byte length (relative; orchestrator decides its absolute offset). The
    // DICT region (zstd-compressed blocks) is built into a tiered buffer during build()
    // -- it must land contiguously AFTER the posting region (streamed concurrently), so
    // it cannot stream directly. The buffer stays in RAM while small (spill-only build)
    // and spills to a temp once it crosses the RAM cap (bounded peak RSS for a huge
    // dict). Its bytes are emitted via stream_dict_region_into below. The posting region
    // went straight to the output during build(), so it has no length accessor here --
    // the orchestrator measures it directly. norms stays in RAM (1 byte/doc).
    uint64_t dict_region_size() const { return dict_buf_.size(); }
    const std::vector<uint8_t>& norms_bytes() const { return norms_section_; }
    const std::vector<uint8_t>& null_bitmap_bytes() const { return null_bitmap_section_; }
    // Block-split bloom XFilter blob ([28B header][bitset]); empty when no terms.
    const std::vector<uint8_t>& bsbf_bytes() const { return bsbf_bytes_; }
    bool has_bsbf() const { return bsbf_built_; }
    void release_bsbf_bytes();
    void release_null_bitmap_bytes();
    void release_norms_bytes();
    bool has_null_bitmap() const { return !null_bitmap_section_.empty(); }

    // Streams the DICT region (RAM or spilled temp) into the append-only container
    // after its posting region.
    Status stream_dict_region_into(io::FileWriter* out) {
        return dict_buf_.stream_into_and_release(out);
    }

    bool has_prx() const { return has_prx_; }
    bool has_norms() const { return has_norms_; }
    format::IndexTier tier() const { return tier_; }
    uint64_t index_id() const { return index_id_; }
    const std::string& index_suffix() const { return index_suffix_; }

    // Builds the three mandatory v1 metadata blobs. The orchestrator writes them
    // contiguously as Core -> STI -> DBD and publishes their absolute references
    // only after all three appends succeed.
    Status finish_metadata(const format::SectionRefs& abs_refs, uint64_t dict_region_offset,
                           SerializedMetadataGroup* out) const;

private:
    friend class SniiStreamedIndexSession;
    LogicalIndexWriter(const SniiIndexInput& in, TrackedNullDocids null_docids);

    // One DICT block's directory record. The block's serialized bytes are appended to
    // the in-RAM dict buffer as soon as the block is cut; only this compact summary
    // (offset within the dict region + length + entry count + checksum) is kept to
    // build the DICT block directory at finish_metadata time. The absolute file offset is
    // computed as dict_region_offset + rel_offset.
    struct BlockRecord {
        uint64_t rel_offset = 0; // byte offset of this block within the dict region
        uint64_t length = 0;     // ON-DISK block length (compressed when flags&kZstd)
        uint32_t n_entries = 0;
        uint32_t checksum = 0;   // crc32c of the UNCOMPRESSED block bytes
        uint8_t flags = 0;       // block_ref_flags::* (kZstd when block is compressed)
        uint64_t uncomp_len = 0; // uncompressed block length (when flags&kZstd)
        std::string first_term;
    };

    // Shared entry/exit of build() and the streamed session, extracted so the
    // two paths are byte-identical BY CONSTRUCTION (not by parallel-maintained
    // copies). prepare_build validates the sink/norms/CommonGrams identity and
    // anchors the posting region; finalize_build re-checks the stats-dependent
    // CommonGrams invariants, seals the dict buffer and materializes
    // stats/norms/null-bitmap/BSBF.
    Status prepare_build(io::FileWriter* posting_out);
    Status finalize_build();
    // Iterates terms (from the streaming source or the materialized vector),
    // splitting DICT blocks by target size and filling PODs + blocks_.
    Status build_blocks();
    // Per-term driver shared by every producer. It validates the term, opens a
    // block if needed, encodes it, and cuts the block at the target size.
    struct BlockState;
    Status process_term(StreamedTermPostings& tp, BlockState* st);
    Status reserve_term_hash_for_append();
    // Region-relative byte count of the posting bytes written so far (the offset basis
    // for frq_base/prx_base + frq_off_delta/prx_off_delta). During build() the only
    // writes to posting_out_ are this index's posting region, so the count is the
    // output offset advanced since the region began.
    uint64_t posting_size() const { return posting_out_->bytes_written() - posting_off0_; }
    // Serializes the current open block, streams its bytes into the dict scratch
    // file, and records a compact directory entry (no block bytes retained).
    Status flush_block(format::DictBlockBuilder* block, std::string first_term);

    uint64_t index_id_;
    std::string index_suffix_;
    format::IndexConfig index_config_;
    format::IndexTier tier_;
    bool has_prx_;
    bool has_freq_; // tier >= T2: a freq region is encoded per window
    bool has_norms_;
    uint32_t doc_count_;
    TrackedNullDocids null_docids_;
    const std::vector<TermPostings>& terms_; // materialized fallback (may be empty)
    SpimiTermBuffer* term_source_;           // streaming source (null => use terms_)
    uint64_t term_count_ = 0;                // distinct terms actually consumed
    const std::vector<uint8_t>& encoded_norms_;
    std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata> common_grams_metadata_;
    format::CommonGramsPostingPolicy common_grams_posting_policy_ =
            format::CommonGramsPostingPolicy::kNone;

    uint32_t target_dict_block_bytes_;
    // G16-h: zstd levels (dict whole-block / prx auto mode), from SniiIndexInput.
    int dict_block_zstd_level_ = 3;
    int prx_zstd_level_ = 3;
    format::PrxWindowLimits prx_window_limits_ = format::kReaderPrxWindowLimits;
    // The DICT region (zstd-compressed blocks) is staged here as blocks flush. It must
    // land contiguously AFTER the posting region (which streams concurrently to the
    // output), so it cannot stream directly; the orchestrator streams it into the
    // container right after the posting region. It has NO independent local cap -- it
    // spills to a temp via the writer's shared tracked-memory budget (the
    // MemoryReporter from SniiIndexInput, null off-Doris). This reporter covers
    // explicitly reserved structures; codec-local scratch remains governed by
    // its format window limits rather than being represented as a whole-writer
    // RSS hard cap.
    MemoryReporter* memory_reporter_ = nullptr;
    SpillableByteBuffer dict_buf_;
    // The interleaved [prx][frq] posting region streams STRAIGHT into the container
    // output during build() -- no temp. posting_out_ is the container writer (borrowed
    // for the duration of build); posting_off0_ is its absolute offset when this index's
    // region began, so posting_size() = bytes_written() - posting_off0_.
    io::FileWriter* posting_out_ = nullptr;
    uint64_t posting_off0_ = 0;
    MemoryReporter::Reservation norms_section_reservation_;
    std::vector<uint8_t> norms_section_;
    MemoryReporter::Reservation null_bitmap_section_reservation_;
    std::vector<uint8_t> null_bitmap_section_;

    std::vector<BlockRecord> blocks_;
    MemoryReporter::Reservation term_hashes_reservation_;
    MemoryReporter::Reservation bsbf_bytes_reservation_;
    // One 8-byte XXH64 (seed 0) filter key per term, collected during the build pass
    // so the whole-vocabulary string copy is never retained.
    std::vector<uint64_t> term_hashes_;
    format::StatsBlock stats_;
    std::vector<uint8_t> bsbf_bytes_; // serialized block-split bloom XFilter section
    bool bsbf_built_ = false;

    // Streamed-session state (T2.1). kIdle until build()/begin_streamed claims
    // the writer; build() jumps straight to kFinished on completion so the two
    // entry points can never interleave on one instance (single-session
    // invariant). kFailed is the poison state: any failure PAST entry
    // validation (process_term inside push_term, build_blocks inside build(),
    // flush/finalize inside either finish path) may have left partial posting
    // bytes in the sink or partial term state (term_hashes_/stats_), so the
    // writer transitions to kFailed and every subsequent
    // push/finish/build/begin is rejected -- a half-fed session can never
    // masquerade as a sealed index (crash-safety invariant 6), and a dirty
    // writer can never be re-claimed for a second session. Pure entry
    // rejections (phase check, term-order guard, prepare_build validation)
    // mutate nothing and therefore do NOT poison.
    enum class StreamPhase : uint8_t { kIdle, kActive, kFinished, kFailed };
    StreamPhase stream_phase_ = StreamPhase::kIdle;
    std::unique_ptr<BlockState> stream_state_; // live only while kActive
    std::string last_pushed_term_;             // strict term-order entry guard
    bool has_pushed_term_ = false;

    friend class StreamingTermEncoder;
};

// TEST-ONLY observability seam (mirrors the reader-side decode-counter and the
// SPIMI vocab-materialization patterns). term_freq_scans() returns a
// process-global count of term-level fused freqs scans. Ordinary terms call
// fuse_freq_stats exactly once; complete CommonGrams entries call it zero times.
// note_term_freq_scan() bumps the counter; reset_term_freq_scans() zeroes it
// between tests; fuse_freq_stats_for_test() exposes the real fused helper so
// pure boundary tests exercise production code. Process-global; reset between
// tests. Not part of the production API.
namespace testing {
void note_term_freq_scan();
uint64_t term_freq_scans();
void reset_term_freq_scans();
FreqStats fuse_freq_stats_for_test(const std::vector<uint32_t>& freqs);
} // namespace testing

} // namespace doris::snii::writer
