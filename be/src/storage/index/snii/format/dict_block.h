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
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"

// DICT block —— a positioning unit mapping term → postings read plan, and also
// the unit for remote on-demand fetching, caching, and CRC checksum
// verification (see docs/design/SNII-design-spec.source.md "DICT block" and
// "dict lookup flow summary" sections).
//
// Byte layout (strictly implemented; multi-byte fixed-width fields are
// little-endian, variable-length integers use LEB128):
//   header:
//     n_entries        varint
//     entry_format_ver u8        # = kDictBlockFormatVer
//     block_flags      u8        # bit0 = has_positions (consistency check
//     against the value passed to reader) frq_base         varint64 prx_base
//     varint64  # present only when has_positions is set
//   entries[n_entries]           # variable-length DictEntry, front-coded in
//   lexicographic order anchor_offsets[n_anchors]    # u32 * n_anchors, byte
//   offset of each anchor entry within the block n_anchors        u32 crc32c
//   u32         # covers [header .. n_anchors], detects corruption (sole CRC
//   layer)
//
// Anchor rule: every anchor_interval entries, one "term anchor" is forced —
// that entry is encoded with prev_term="" (prefix_len=0, storing the full
// term), and its byte offset is recorded in anchor_offsets; non-anchor entries
// use the preceding entry's term as prev_term for front coding. The reader can
// start from any anchor and scan independently without needing earlier terms,
// enabling anchor binary search + local scan for exact term lookup.
namespace doris::snii::format {

// DICT block entry_format_ver: self-describing version of the DictEntry
// encoding. Reader rejects a mismatch so a query-only run cannot silently read
// an older dict-entry layout as the current one.
inline constexpr uint8_t kDictBlockFormatVer = 2;

// block_flags bit definitions.
namespace dict_block_flags {
inline constexpr uint8_t kHasPositions = 1U << 0; // whether to write prx_base / .prx fields
// bit1-7 reserved
} // namespace dict_block_flags

// DICT block writer: entries are added in lexicographic order via add_entry;
// internally determines anchors and accumulates size estimates, and on finish
// serializes header + entries + anchor table + CRC in one pass. The front-coding
// base is rebuilt from a local prev inside finish(), so no prev_term is retained
// as builder state.
class DictBlockBuilder {
public:
    DictBlockBuilder(IndexTier tier, bool has_positions, uint64_t frq_base, uint64_t prx_base,
                     uint32_t anchor_interval = 16);

    // Append one entry (caller must guarantee lexicographic term order).
    // Internally decides whether it becomes an anchor. The copy overload is kept
    // for callers that must retain their entry afterwards (materialized fallback,
    // tests); the move overload avoids the per-term DictEntry copy -- which for an
    // inline entry is two std::vector<uint8_t> heap allocations plus the term
    // copy -- on the SPIMI build path.
    void add_entry(const DictEntry& entry);
    void add_entry(DictEntry&& entry);

    // Upper-bound estimate of the serialized size of the current block (including
    // header + entries + anchor table + CRC footer), used by the upper layer to
    // decide when to cut a new block based on target_dict_block_bytes.
    size_t estimated_bytes() const;

    // Number of entries.
    uint32_t n_entries() const { return n_entries_; }

    // Serialize the entire block and append it to sink.
    void finish(ByteSink* sink) const;

    // G16 byte accounting, valid AFTER finish(): serialized entry-body bytes
    // (uncompressed, prefix-coded, anchors included) split by term class.
    // Measurement-only -- the writer aggregates these into the per-index
    // section-stats log line; nothing on disk depends on them.
    uint64_t entry_bytes_total() const { return entry_bytes_total_; }
    uint64_t entry_bytes_bigram() const { return entry_bytes_bigram_; }

private:
    bool is_anchor(uint32_t index) const { return index % anchor_interval_ == 0; }

    IndexTier tier_;
    bool has_positions_;
    uint64_t frq_base_;
    uint64_t prx_base_;
    uint32_t anchor_interval_;

    uint32_t n_entries_ = 0;
    std::vector<DictEntry> entries_;
    size_t entries_est_ = 0; // accumulated byte estimate for the entries section
    size_t n_anchors_ = 0;   // number of anchors
    // G16 accounting, filled by finish() (which stays logically const: these
    // never affect serialization).
    mutable uint64_t entry_bytes_total_ = 0;
    mutable uint64_t entry_bytes_bigram_ = 0;
};

// DICT block reader: on open, verifies the CRC and parses the header / anchor
// table; find_term uses anchor binary search + local scan to locate a
// DictEntry. Holds a byte view of the block (non-owning); lifetime is managed
// by the caller.
class DictBlockReader {
public:
    DictBlockReader() = default;

    // Parse and verify the entire block. CRC mismatch / truncation / invalid
    // structure → Corruption; has_positions in the header inconsistent with the
    // supplied argument → InvalidArgument.
    static Status open(Slice block, IndexTier tier, bool has_positions, DictBlockReader* out);

    // Anchor binary search + local scan to locate target. Hit → *found=true and
    // *out is filled; miss (including out-of-range, gap) → *found=false.
    // Structural error → non-OK Status.
    Status find_term(std::string_view target, bool* found, DictEntry* out) const;

    // Decodes EVERY entry in the block in lexicographic order into *out (each a
    // self-contained DictEntry, owning its term). Used for ordered term
    // enumeration (prefix / range scans). Resets the front-coding base at each
    // anchor segment. Retained as the golden reference; the prefix path now
    // streams via visit_prefix_range.
    Status decode_all(std::vector<DictEntry>* out) const;

    // Streams the entries of this block whose term lies in [prefix, prefix+) in
    // lexicographic order, materializing only the bodies a caller keeps (T07):
    //   1) anchor-jump to the anchor segment containing prefix (segments whose
    //      terms are all < prefix are skipped without any decode); an empty
    //      prefix or one before the first anchor starts at anchor 0;
    //   2) within range, decode each entry's term key only; term < prefix skips
    //      the body and continues; a term that leaves the prefix range sets
    //      *prefix_exhausted=true and ends the scan (sorted order guarantees no
    //      further matches here or in later blocks);
    //   3) accept_key(term)==false skips the body (lets callers push a key-only
    //      predicate down so a non-match never pays for its body);
    //   4) only accepted entries have their body decoded and are handed to
    //      on_hit, which may request an early stop via *stop.
    // accept_key may be empty (treated as accept-all). prefix_exhausted is set
    // false on entry and true only when a term past the range is seen.
    Status visit_prefix_range(std::string_view prefix,
                              const std::function<bool(std::string_view term)>& accept_key,
                              const std::function<Status(DictEntry&& entry, bool* stop)>& on_hit,
                              bool* prefix_exhausted) const;

    uint64_t frq_base() const { return frq_base_; }
    uint64_t prx_base() const { return prx_base_; }
    uint32_t n_entries() const { return n_entries_; }

    // Resident heap held beyond sizeof(*this): the anchor_offsets_ / anchor_terms_
    // vector buffers plus each non-SSO anchor term's heap allocation. block_ is a
    // NON-owning view and is deliberately NOT counted here -- its owning buffer is
    // charged by the caller (the resident block's `bytes` vector, or a
    // request-scoped decoded block). Summed into
    // LogicalIndexReader::memory_usage() per resident block.
    size_t heap_bytes() const;

private:
    // Sequentially scan from anchor anchor_idx to the end of that anchor segment,
    // searching for target.
    Status scan_from_anchor(size_t anchor_idx, std::string_view target, bool* found,
                            DictEntry* out) const;

    // Find the last anchor index where first_term(anchor) <= target; return false
    // if none exists.
    bool locate_anchor(std::string_view target, size_t* anchor_idx) const;

    Slice block_; // [header .. crc) full block view
    IndexTier tier_ = IndexTier::kT1;
    bool has_positions_ = false;
    uint64_t frq_base_ = 0;
    uint64_t prx_base_ = 0;
    uint32_t n_entries_ = 0;

    size_t entries_begin_ = 0;             // absolute offset of the start of the entries section
    std::vector<uint32_t> anchor_offsets_; // byte offset within the block for each anchor entry
    std::vector<std::string>
            anchor_terms_; // full term of each anchor entry (used for binary search)
};

} // namespace doris::snii::format

// Test-only instrumentation seam. dict_decode_counter() returns a process-global
// count of DICT block decodes performed by DictBlockReader::open -- i.e. the
// optional zstd decompress + CRC verify + anchor parse that turns on-disk block
// bytes into a usable reader. This is precisely the unit a dict-block cache
// eliminates on repeat, so tests assert dict_decode_counter() == unique_blocks.
// In production DICT blocks are zstd-compressed, so this equals the zstd
// decompress count. Counters use relaxed atomics; reset between tests.
namespace doris::snii::testing {

uint64_t dict_decode_counter();
void reset_dict_decode_counter();
void note_dict_block_decode();

} // namespace doris::snii::testing
