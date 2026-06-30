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
    // Optional writer-level build-RAM reporter (one per SniiCompoundWriter = one
    // segment inverted index). When non-null, the dict buffer reports its REAL
    // resident-byte deltas (positive on grow, negative on spill). The SPIMI side
    // (arena + slot index) reports through the SAME reporter, injected directly at
    // the term_source's construction by the caller. null in bench / unit tests -> no
    // reporting. NEVER report live_bytes_ (a gated estimate); report
    // arena_bytes()+slot_of_+dict ram_bytes_.
    MemoryReporter* mem_reporter = nullptr;
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
    Status validate_term(const TermPostings& tp) const;
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
    // Builds one DictEntry (inline or pod_ref), growing the posting region as needed.
    Status build_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base,
                       format::DictEntry* e);
    // Builds a windowed (df >= kSlimDfThreshold) entry: multi-window + two-level
    // prelude. The term's [prx span][frq span] is appended to the posting region.
    Status build_windowed_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base,
                                format::DictEntry* e);
    // Builds a slim (df < kSlimDfThreshold) entry: single window, inline or
    // pod_ref, no prelude.
    Status build_slim_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base,
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

} // namespace doris::snii::writer
