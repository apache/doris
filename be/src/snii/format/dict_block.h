#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/encoding/byte_sink.h"
#include "snii/format/dict_entry.h"
#include "snii/format/format_constants.h"

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
namespace snii::format {

// DICT block entry_format_ver: self-describing version of the DictEntry
// encoding. Reader rejects a mismatch so a query-only run cannot silently read
// an older dict-entry layout as the current one.
inline constexpr uint8_t kDictBlockFormatVer = 2;

// block_flags bit definitions.
namespace dict_block_flags {
inline constexpr uint8_t kHasPositions = 1u << 0; // whether to write prx_base / .prx fields
// bit1-7 reserved
} // namespace dict_block_flags

// DICT block writer: entries are added in lexicographic order via add_entry;
// internally maintains prev_term, determines anchors, accumulates size
// estimates, and on finish serializes header + entries + anchor table + CRC in
// one pass.
class DictBlockBuilder {
public:
    DictBlockBuilder(IndexTier tier, bool has_positions, uint64_t frq_base, uint64_t prx_base,
                     uint32_t anchor_interval = 16);

    // Append one entry (caller must guarantee lexicographic term order).
    // Internally decides whether it becomes an anchor.
    void add_entry(const DictEntry& entry);

    // Upper-bound estimate of the serialized size of the current block (including
    // header + entries + anchor table + CRC footer), used by the upper layer to
    // decide when to cut a new block based on target_dict_block_bytes.
    size_t estimated_bytes() const;

    // Number of entries.
    uint32_t n_entries() const { return n_entries_; }

    // Serialize the entire block and append it to sink.
    void finish(ByteSink* sink) const;

private:
    bool is_anchor(uint32_t index) const { return index % anchor_interval_ == 0; }

    IndexTier tier_;
    bool has_positions_;
    uint64_t frq_base_;
    uint64_t prx_base_;
    uint32_t anchor_interval_;

    uint32_t n_entries_ = 0;
    std::vector<DictEntry> entries_;
    std::string prev_term_;  // term of the previous entry (front coding base)
    size_t entries_est_ = 0; // accumulated byte estimate for the entries section
    size_t n_anchors_ = 0;   // number of anchors
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
    // anchor segment.
    Status decode_all(std::vector<DictEntry>* out) const;

    uint64_t frq_base() const { return frq_base_; }
    uint64_t prx_base() const { return prx_base_; }
    uint32_t n_entries() const { return n_entries_; }

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

} // namespace snii::format
