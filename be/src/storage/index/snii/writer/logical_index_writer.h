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
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/per_index_meta.h"
#include "storage/index/snii/format/sampled_term_index.h"
#include "storage/index/snii/format/stats_block.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/spillable_byte_buffer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

// LogicalIndexWriter -- builds the per-logical-index section bytes (interleaved
// posting region + DICT block region) and the meta sub-sections (SampledTermIndex,
// DICT block directory, StatsBlock, XFilter) for ONE logical index. It owns the
// in-memory section bytes and the metadata needed by the container orchestrator
// (SniiCompoundWriter) to resolve absolute offsets and emit the per-index meta
// block.
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
//   df >= kSlimDfThreshold (512): WINDOWED pod_ref. The term's [prx windows] are
//     appended to the posting region first, then its [prelude][dd-block][freq-block]
//     frq span. The DictEntry records frq/prx off_delta+len relative to
//     frq_base/prx_base (see below).
//   df < kSlimDfThreshold: SLIM. The postings are encoded as a single .frq
//     window (and .prx window). If the encoded .frq bytes are small
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
    // EFFECTIVE phrase-bigram df-prune threshold (G01). 0 (default) == legacy:
    // every term materializes as before. > 0 (positions-capable configs only;
    // ignored otherwise): a hidden phrase-bigram term (marker prefix, sentinel
    // excluded) whose FINAL df < this is skipped entirely at materialization (no
    // dict entry, no postings, no bloom membership, no stats), and every
    // SURVIVING bigram term writes its posting docs+freq-only (no .prx) even in
    // kDocsPositions config. The applied value is recorded in the per-index meta
    // (kBigramPruneInfo) so the reader's 2-term phrase path knows a bigram dict
    // miss must fall back to positions verification. The caller (Doris adapter)
    // resolves config::snii_bigram_prune_min_df + the doc-count formula into
    // this field; the core writer just applies it.
    uint32_t bigram_prune_min_df = 0;
    // EFFECTIVE phrase-bigram df-prune UPPER bound (G15), an ABSOLUTE doc
    // count. 0 (default) == no upper gate. > 0 (positions-capable configs only;
    // ignored otherwise): a hidden phrase-bigram term (sentinel excluded) whose
    // FINAL df EXCEEDS this is skipped at materialization exactly like a
    // min-df-pruned term -- near-ubiquitous pairs pay dict + posting bytes for
    // almost no selectivity. Recorded in kBigramPruneInfo alongside the min
    // threshold, so the reader falls back on a dict miss when EITHER gate is
    // declared. The caller (Doris adapter) resolves
    // config::snii_bigram_prune_max_df_ratio * doc_count (floored at
    // 2 * bigram_prune_min_df when the min gate is active) into this field; the
    // core writer just applies it. Unlike the min gate it is NEVER sunk into
    // the mid-feed drain: a partial df above a threshold derived from the
    // still-growing doc count proves nothing about the final df/threshold pair.
    uint64_t bigram_prune_max_df = 0;
    // G04: EVER-DROPPED bloom over bigram terms the SPIMI vocab-cap eviction
    // dropped mid-build (SpimiTermBuffer::bigram_dropped_filter(); the caller
    // wires it). When non-null AND pruning is active (bigram_prune_min_df > 0),
    // process_term drops any non-sentinel bigram term the filter may contain IN
    // ADDITION to the df threshold: an evicted-then-reappearing pair's
    // re-accumulated postings are INCOMPLETE (they miss the docids dropped at
    // eviction), so materializing them would silently lose phrase hits; dropping
    // them instead routes the pair to the G01 dict-miss fallback (correct, just
    // slower). Bloom false positives only over-drop -- safe under the same
    // contract. Ignored (with pruning forced off) when bigram_prune_min_df == 0:
    // a bloom drop on a segment whose meta declares NO pruning would break the
    // legacy miss==empty reader semantics. Borrowed; null when no eviction fired.
    const BigramDropFilter* bigram_ever_dropped = nullptr;
    // Optional writer-level build-RAM reporter (one per SniiCompoundWriter = one
    // segment inverted index). When non-null, the dict buffer reports its REAL
    // resident-byte deltas (positive on grow, negative on spill). The SPIMI side
    // (arena + slot index) reports through the SAME reporter, injected directly at
    // the term_source's construction by the caller. null in bench / unit tests -> no
    // reporting. NEVER report live_bytes_ (a gated estimate); report
    // arena_bytes()+slot_of_+dict ram_bytes_.
    MemoryReporter* mem_reporter = nullptr;
};

// Fused single-pass term-level frequency statistics for ONE term: total_freq is
// sum(freqs) and max_freq is max(freqs), computed in a SINGLE scan (see
// fuse_freq_stats) and reused for the has_prx position-count check
// (validate_term), stats_.sum_total_term_freq, and the DictEntry
// ttf_delta/max_freq. Byte-identical to the former separate sum/max scans (same
// accumulation order; max initialized to 0 so a freq of 0 never pollutes it).
struct FreqStats {
    uint64_t total_freq = 0;
    uint32_t max_freq = 0;
};

// Builds and holds the section bytes + meta sub-sections for one logical index.
class LogicalIndexWriter {
public:
    explicit LogicalIndexWriter(const SniiIndexInput& in);

    // Builds DICT blocks, the interleaved posting region, sampled-term index, dict
    // directory, stats and bsbf. The posting region is written STRAIGHT into
    // `posting_out` as terms are produced (no temp round-trip for the bulk); the
    // orchestrator captures its absolute offset/length from posting_out->bytes_written()
    // around this call. Must be called once before the accessors below. Returns
    // InvalidArgument on a null sink or inconsistent input (e.g. norms/doc_count
    // mismatch when scoring is enabled, or non-ascending docids).
    Status build(io::FileWriter* posting_out);

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
    bool has_bsbf() const { return !bsbf_bytes_.empty(); }
    bool has_null_bitmap() const { return !null_bitmap_section_.empty(); }

    // Streams the DICT region (RAM or spilled temp) into the append-only container
    // after its posting region.
    Status stream_dict_region_into(io::FileWriter* out) const { return dict_buf_.stream_into(out); }

    bool has_prx() const { return has_prx_; }
    bool has_norms() const { return has_norms_; }
    format::IndexTier tier() const { return tier_; }
    uint64_t index_id() const { return index_id_; }
    const std::string& index_suffix() const { return index_suffix_; }

    // Builds the per-index meta block bytes given the resolved ABSOLUTE section
    // refs (filled by the orchestrator), appending them to out. The DICT block
    // directory entries are rebased to absolute offsets using dict_region_offset.
    Status finish_meta(const format::SectionRefs& abs_refs, uint64_t dict_region_offset,
                       ByteSink* out) const;

private:
    // One DICT block's directory record. The block's serialized bytes are appended to
    // the in-RAM dict buffer as soon as the block is cut; only this compact summary
    // (offset within the dict region + length + entry count + checksum) is kept to
    // build the DICT block directory at finish_meta time. The absolute file offset is
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

    // Validates one term's shape (parallel lengths, strictly ascending docids).
    // `total_freq` is the fused sum(freqs) the caller computes once via
    // fuse_freq_stats; the position-count check compares against it instead of
    // re-summing freqs internally. `expect_positions` is the PER-TERM positions
    // switch (the caller's write_prx): a G04 position-suppressed bigram term
    // legitimately arrives with an empty positions_flat while freqs sum > 0, and
    // its .prx is never written -- so the count check is skipped exactly when the
    // term writes no positions. For every other term (and all terms in legacy
    // mode) expect_positions == has_prx_, preserving the strict check.
    Status validate_term(const TermPostings& tp, uint64_t total_freq, bool expect_positions) const;
    // Iterates terms (from the streaming source or the materialized vector),
    // splitting DICT blocks by target size and filling PODs + blocks_.
    Status build_blocks();
    // Per-term driver shared by both the streaming and materialized paths:
    // validates the term, opens a block if needed, builds its DictEntry, and cuts
    // the block once it reaches the target size. Mutates the running block state.
    struct BlockState;
    // `tp` is taken by mutable reference: the encode FREES the term's large flat
    // arrays (docids/freqs/positions_flat) as soon as they are consumed, so the
    // widest term's source does not co-exist with its encoded output at peak RSS.
    Status process_term(TermPostings& tp, BlockState* st);
    // Region-relative byte count of the posting bytes written so far (the offset basis
    // for frq_base/prx_base + frq_off_delta/prx_off_delta). During build() the only
    // writes to posting_out_ are this index's posting region, so the count is the
    // output offset advanced since the region began.
    uint64_t posting_size() const { return posting_out_->bytes_written() - posting_off0_; }
    // Builds one DictEntry (inline or pod_ref), growing the posting region as
    // needed. `fs` is the fused term-level freq stats (reused for ttf_delta /
    // max_freq, so no second sum/max scan). `write_prx` is the PER-TERM positions
    // switch: has_prx_ for normal terms, false for phrase-bigram terms in
    // prune mode (G01 docs-only bigrams) -- the entry then carries no .prx span
    // (prx_len == 0) while the dd/freq regions stay identical. `write_freq` is
    // the PER-TERM freq switch (G16): has_freq_ for normal terms, false for
    // prune-mode bigrams, whose freq bytes no query path ever reads. Honored on
    // the WINDOWED path only (the prelude flags self-describe freq presence);
    // slim/inline entries keep freq because their DictEntry region metadata is
    // tier-conditioned, not per-entry.
    Status build_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base, const FreqStats& fs,
                       bool write_prx, bool write_freq, format::DictEntry* e);
    // Builds a windowed (df >= kSlimDfThreshold) entry: multi-window + two-level
    // prelude. The term's [prx span][frq span] is appended to the posting region
    // (prx span empty when !write_prx, freq-block empty when !write_freq; the
    // per-term prelude is self-describing).
    Status build_windowed_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base,
                                bool write_prx, bool write_freq, format::DictEntry* e);
    // Builds a slim (df < kSlimDfThreshold) entry: single window, inline or
    // pod_ref, no prelude.
    Status build_slim_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base, bool write_prx,
                            format::DictEntry* e);
    // Serializes the current open block, streams its bytes into the dict scratch
    // file, and records a compact directory entry (no block bytes retained).
    Status flush_block(format::DictBlockBuilder* block, std::string first_term);

    uint64_t index_id_;
    std::string index_suffix_;
    format::IndexTier tier_;
    bool has_prx_;
    bool has_freq_; // tier >= T2: a freq region is encoded per window
    bool has_norms_;
    // G01 effective bigram df-prune threshold (0 == off/legacy). Forced to 0 for
    // non-positional configs (no bigrams are ever emitted there). Recorded into
    // the per-index meta by finish_meta when non-zero.
    uint32_t bigram_prune_min_df_;
    // G15 effective bigram df-prune UPPER bound, absolute (0 == no upper gate).
    // Forced to 0 for non-positional configs like the min threshold. Recorded
    // into the per-index meta by finish_meta alongside it.
    uint64_t bigram_prune_max_df_;
    // G04 ever-dropped bloom (borrowed from SniiIndexInput). Non-null ONLY when
    // pruning is active (see the SniiIndexInput field contract): probed once per
    // df-surviving bigram term in process_term.
    const BigramDropFilter* bigram_ever_dropped_;
    uint32_t doc_count_;
    std::vector<uint32_t> null_docids_;
    const std::vector<TermPostings>& terms_; // materialized fallback (may be empty)
    SpimiTermBuffer* term_source_;           // streaming source (null => use terms_)
    uint64_t term_count_ = 0;                // distinct terms actually consumed
    const std::vector<uint8_t>& encoded_norms_;

    uint32_t target_dict_block_bytes_;
    // The DICT region (zstd-compressed blocks) is staged here as blocks flush. It must
    // land contiguously AFTER the posting region (which streams concurrently to the
    // output), so it cannot stream directly; the orchestrator streams it into the
    // container right after the posting region. It has NO independent local cap -- it
    // spills to a temp via the writer's UNIFIED gate-2 cap (the MemoryReporter from
    // SniiIndexInput, null off-Doris), the same single cap the SPIMI arena uses, so one
    // threshold bounds the writer's total build RAM. The dict self-reports its ram_bytes_
    // deltas; the SPIMI term_source self-reports its arena+slot deltas (its reporter is
    // injected at the source's own construction by the caller).
    SpillableByteBuffer dict_buf_;
    // The interleaved [prx][frq] posting region streams STRAIGHT into the container
    // output during build() -- no temp. posting_out_ is the container writer (borrowed
    // for the duration of build); posting_off0_ is its absolute offset when this index's
    // region began, so posting_size() = bytes_written() - posting_off0_.
    io::FileWriter* posting_out_ = nullptr;
    uint64_t posting_off0_ = 0;
    std::vector<uint8_t> norms_section_;
    std::vector<uint8_t> null_bitmap_section_;

    std::vector<BlockRecord> blocks_;
    // One 8-byte XXH64 (seed 0) filter key per term, collected during the build pass
    // so the whole-vocabulary string copy is never retained.
    std::vector<uint64_t> term_hashes_;
    format::StatsBlock stats_;
    std::vector<uint8_t> bsbf_bytes_; // serialized block-split bloom XFilter section
};

// TEST-ONLY observability seam (mirrors the reader-side decode-counter and the
// SPIMI vocab-materialization patterns). term_freq_scans() returns a
// process-global count of term-level fused freqs scans -- fuse_freq_stats is
// called EXACTLY ONCE per term, so a build of N terms yields N (it was 3N for a
// docs-only build, or 4N with positions, across the former separate
// validate-sum / stats-sum / ttf-sum / max scans). note_term_freq_scan() bumps
// the counter (called only from fuse_freq_stats); reset_term_freq_scans() zeroes
// it between tests; fuse_freq_stats_for_test() exposes the real fused helper so
// pure boundary tests exercise production code. Process-global; reset between
// tests. Not part of the production API.
namespace testing {
void note_term_freq_scan();
uint64_t term_freq_scans();
void reset_term_freq_scans();
FreqStats fuse_freq_stats_for_test(const std::vector<uint32_t>& freqs);

// G01 bigram-diet observability seam (same always-on relaxed-atomic pattern as
// term_freq_scans). Counts the writer's per-bigram materialization decisions --
// bumped ONCE per non-sentinel phrase-bigram term reaching process_term:
//   bigram_terms_materialized : the term got a dict entry (df >= threshold, or
//                               pruning disabled).
//   bigram_terms_pruned       : the term was skipped entirely (pruning active
//                               and final df < threshold): no dict entry, no
//                               postings, no bloom membership, no stats.
//   bigram_terms_max_pruned   : the term was skipped by the G15 UPPER gate
//                               (final df > bigram_prune_max_df): same total
//                               skip, counted separately from the min gate.
// The sentinel term counts toward NONE of these (it is never prunable).
// Process-global; reset between tests. Not part of the production API.
void note_bigram_term_materialized();
void note_bigram_term_pruned();
void note_bigram_term_max_pruned();
uint64_t bigram_terms_materialized();
uint64_t bigram_terms_pruned();
uint64_t bigram_terms_max_pruned();
// G16 seam: bumped ONCE per WINDOWED entry built with its freq region elided
// (write_freq == false on a freq-capable index, i.e. a prune-mode bigram whose
// df crossed the windowed threshold). Slim/inline bigrams keep freq and never
// count here. Reset together with the prune counters.
void note_bigram_freq_elided();
uint64_t bigram_freqs_elided();
void reset_bigram_prune_counters();

// G04 flush-side seam: bumped ONCE per bigram term that SURVIVED the df
// threshold but was dropped because the ever-dropped bloom may contain it
// (i.e. the drop the bloom -- not the threshold -- caused). df-dropped terms
// count toward bigram_terms_pruned as before, never here. Reset together with
// the prune counters by reset_bigram_prune_counters().
void note_bigram_bloom_dropped();
uint64_t bigram_bloom_dropped();
} // namespace testing

} // namespace doris::snii::writer
