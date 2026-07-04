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

#include "storage/index/snii/format/dict_block.h"

#include <algorithm>
#include <atomic>

#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/format/phrase_bigram.h"      // is_phrase_bigram_term (G16 accounting)
#include "storage/index/snii/format/sampled_term_index.h" // std_string_heap_bytes

namespace doris::snii::format {

namespace {

constexpr size_t kFooterBytes = sizeof(uint32_t);    // trailing crc32c
constexpr size_t kNAnchorsBytes = sizeof(uint32_t);  // n_anchors u32
constexpr size_t kAnchorOffBytes = sizeof(uint32_t); // per-anchor offset u32

// Estimate the encoded upper-bound byte size of one entry (no actual encoding; used by estimated_bytes).
// Take the maximum varint width of each variable-length field plus payload bytes to guarantee an upper bound.
size_t estimate_entry_bytes(const DictEntry& e) {
    size_t body = 0;
    body += varint_len(static_cast<uint32_t>(e.term.size())); // prefix_len upper bound
    body += varint_len(static_cast<uint32_t>(e.term.size())); // suffix_len upper bound
    body += e.term.size();                                    // suffix bytes upper bound
    body += 1;                                                // flags
    body += 10;                                               // df + ttf + max_freq upper bound
    body += 10;                                               // ttf_delta
    body += 10;                                               // max_freq
    if (e.kind == DictEntryKind::kInline) {
        body += 10 + e.frq_bytes.size();
        body += 10 + e.prx_bytes.size();
    } else {
        body += 10 * 5; // frq_off/frq_len/prelude/prx_off/prx_len upper bound
    }
    return varint_len(static_cast<uint64_t>(body)) + body; // entry_len + body
}

} // namespace

// ---- DictBlockBuilder ----

DictBlockBuilder::DictBlockBuilder(IndexTier tier, bool has_positions, uint64_t frq_base,
                                   uint64_t prx_base, uint32_t anchor_interval, bool term_stats)
        : tier_(tier),
          has_positions_(has_positions),
          term_stats_(term_stats),
          frq_base_(frq_base),
          prx_base_(prx_base),
          anchor_interval_(anchor_interval == 0 ? 1 : anchor_interval) {}

void DictBlockBuilder::add_entry(const DictEntry& entry) {
    if (is_anchor(n_entries_)) {
        ++n_anchors_;
    }
    entries_est_ += estimate_entry_bytes(entry);
    entries_.push_back(entry);
    ++n_entries_;
}

void DictBlockBuilder::add_entry(DictEntry&& entry) {
    if (is_anchor(n_entries_)) {
        ++n_anchors_;
    }
    // estimate_entry_bytes reads `entry`, so it MUST run before the move below:
    // sizing a moved-from (empty) entry would undercount entries_est_ and split
    // blocks incorrectly. finish() output is unaffected either way -- it depends
    // only on the entries actually queued, not on how they were appended.
    entries_est_ += estimate_entry_bytes(entry);
    entries_.push_back(std::move(entry));
    ++n_entries_;
}

size_t DictBlockBuilder::estimated_bytes() const {
    size_t header = varint_len(static_cast<uint64_t>(n_entries_)) + 2; // +ver +flags
    header += varint_len(frq_base_);
    if (has_positions_) {
        header += varint_len(prx_base_);
    }
    const size_t anchors = n_anchors_ * kAnchorOffBytes + kNAnchorsBytes;
    return header + entries_est_ + anchors + kFooterBytes;
}

void DictBlockBuilder::finish(ByteSink* sink) const {
    ByteSink body; // header + entries + anchor_offsets + n_anchors (crc covered region)

    // header.
    body.put_varint64(static_cast<uint64_t>(n_entries_));
    body.put_u8(kDictBlockFormatVer);
    body.put_u8(static_cast<uint8_t>((has_positions_ ? dict_block_flags::kHasPositions : 0U) |
                                     (term_stats_ ? 0U : dict_block_flags::kNoTermStats)));
    body.put_varint64(frq_base_);
    if (has_positions_) {
        body.put_varint64(prx_base_);
    }

    // entries: anchor entries use prev_term="" and record their byte offset within the block.
    std::vector<uint32_t> anchor_offsets;
    anchor_offsets.reserve(n_anchors_);
    std::string prev;
    entry_bytes_total_ = 0;
    entry_bytes_bigram_ = 0;
    for (uint32_t i = 0; i < n_entries_; ++i) {
        const bool anchor = is_anchor(i);
        if (anchor) {
            anchor_offsets.push_back(static_cast<uint32_t>(body.size()));
        }
        const std::string_view prev_term = anchor ? std::string_view {} : std::string_view(prev);
        // finish() is void and entry encoding into an in-memory ByteSink cannot fail;
        // explicitly discard the (now [[nodiscard]] Status) return.
        const uint64_t before = body.size();
        static_cast<void>(encode_dict_entry(entries_[i], prev_term, tier_, &body, term_stats_));
        const uint64_t encoded = body.size() - before;
        entry_bytes_total_ += encoded;
        if (is_phrase_bigram_term(entries_[i].term)) {
            entry_bytes_bigram_ += encoded;
        }
        prev = entries_[i].term;
    }

    // anchor_offsets[] + n_anchors.
    for (uint32_t off : anchor_offsets) {
        body.put_fixed32(off);
    }
    body.put_fixed32(static_cast<uint32_t>(anchor_offsets.size()));

    // Write the entire block (including crc footer) to sink.
    sink->put_bytes(body.view());
    sink->put_fixed32(crc32c(body.view()));
}

// ---- DictBlockReader ----

namespace {

// Verify the block length is sufficient and validate the trailing crc; return a Slice of the covered region (excluding crc footer).
Status verify_crc(Slice block, Slice* covered) {
    if (block.size() < kFooterBytes + kNAnchorsBytes) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_block: block too short to contain footer");
    }
    const size_t covered_len = block.size() - kFooterBytes;
    *covered = block.subslice(0, covered_len);

    ByteSource crc_src(block.subslice(covered_len, kFooterBytes));
    uint32_t stored = 0;
    RETURN_IF_ERROR(crc_src.get_fixed32(&stored));
    if (crc32c(*covered) != stored) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_block: crc32c checksum mismatch");
    }
    return Status::OK();
}

// Read and verify that block_flags is consistent with has_positions.
Status check_flags(uint8_t flags, bool has_positions) {
    const bool flag_pos = (flags & dict_block_flags::kHasPositions) != 0;
    if (flag_pos != has_positions) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "dict_block: has_positions inconsistent with block_flags");
    }
    return Status::OK();
}

} // namespace

Status DictBlockReader::open(Slice block, IndexTier tier, bool has_positions,
                             DictBlockReader* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("dict_block: out is null");
    }
    *out = DictBlockReader {};

    // Decode instrumentation seam: one increment per block materialization (the
    // CRC verify + anchor parse below, preceded by a zstd decompress for a
    // compressed block). A dict-block cache eliminates repeats of exactly this.
    testing::note_dict_block_decode();

    Slice covered;
    RETURN_IF_ERROR(verify_crc(block, &covered));
    out->block_ = covered;
    out->tier_ = tier;
    out->has_positions_ = has_positions;

    // header.
    ByteSource src(covered);
    uint64_t n_entries = 0;
    RETURN_IF_ERROR(src.get_varint64(&n_entries));
    uint8_t ver = 0;
    uint8_t flags = 0;
    RETURN_IF_ERROR(src.get_u8(&ver));
    RETURN_IF_ERROR(src.get_u8(&flags));
    if (ver != kDictBlockFormatVer) {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "dict_block: unsupported entry_format_ver");
    }
    RETURN_IF_ERROR(check_flags(flags, has_positions));
    out->term_stats_ = (flags & dict_block_flags::kNoTermStats) == 0;
    RETURN_IF_ERROR(src.get_varint64(&out->frq_base_));
    if (has_positions) {
        RETURN_IF_ERROR(src.get_varint64(&out->prx_base_));
    }

    out->n_entries_ = static_cast<uint32_t>(n_entries);
    out->entries_begin_ = src.position();

    // The anchor table is at the tail of covered: [... anchor_offsets[n] n_anchors(u32)].
    if (covered.size() < kNAnchorsBytes) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_block: missing n_anchors");
    }
    ByteSource na_src(covered.subslice(covered.size() - kNAnchorsBytes, kNAnchorsBytes));
    uint32_t n_anchors = 0;
    RETURN_IF_ERROR(na_src.get_fixed32(&n_anchors));

    const size_t anchor_table_bytes = static_cast<size_t>(n_anchors) * kAnchorOffBytes;
    if (covered.size() < kNAnchorsBytes + anchor_table_bytes ||
        out->entries_begin_ + anchor_table_bytes + kNAnchorsBytes > covered.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_block: anchor table out of range");
    }
    const size_t anchor_table_begin = covered.size() - kNAnchorsBytes - anchor_table_bytes;

    ByteSource at_src(covered.subslice(anchor_table_begin, anchor_table_bytes));
    out->anchor_offsets_.resize(n_anchors);
    out->anchor_terms_.resize(n_anchors);
    for (uint32_t i = 0; i < n_anchors; ++i) {
        uint32_t off = 0;
        RETURN_IF_ERROR(at_src.get_fixed32(&off));
        if (off >= anchor_table_begin) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "dict_block: anchor offset out of range");
        }
        // Anchor offsets must be strictly monotonically increasing, and the first anchor must be exactly the start of the entries region (entry 0 is always an anchor).
        // Otherwise scan_from_anchor's segment-length computation seg_end-seg_begin would underflow as size_t and cause an out-of-range read,
        // guarding against non-monotonic offset tables with a re-stamped crc (remote on-demand read / cache misalignment scenarios).
        if (i == 0) {
            if (off != out->entries_begin_) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "dict_block: first anchor offset is not the start of entries");
            }
        } else if (off <= out->anchor_offsets_[i - 1]) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "dict_block: anchor offsets are not strictly increasing");
        }
        out->anchor_offsets_[i] = off;
        // Anchor entries are encoded with prev_term="" and can be decoded independently to retrieve their term.
        ByteSource e_src(covered.subslice(off, anchor_table_begin - off));
        DictEntry probe;
        RETURN_IF_ERROR(decode_dict_entry(&e_src, std::string_view {}, tier, &probe,
                                          (flags & dict_block_flags::kNoTermStats) == 0));
        out->anchor_terms_[i] = std::move(probe.term);
    }
    return Status::OK();
}

size_t DictBlockReader::heap_bytes() const {
    size_t bytes = anchor_offsets_.capacity() * sizeof(uint32_t) +
                   anchor_terms_.capacity() * sizeof(std::string);
    for (const auto& term : anchor_terms_) {
        bytes += std_string_heap_bytes(term);
    }
    return bytes;
}

bool DictBlockReader::locate_anchor(std::string_view target, size_t* anchor_idx) const {
    if (anchor_terms_.empty()) {
        return false;
    }
    if (target < std::string_view(anchor_terms_.front())) {
        return false;
    }
    // The last anchor_term <= target.
    size_t lo = 0;
    size_t hi = anchor_terms_.size(); // open interval
    while (lo + 1 < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        if (std::string_view(anchor_terms_[mid]) <= target) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    *anchor_idx = lo;
    return true;
}

Status DictBlockReader::decode_all(std::vector<DictEntry>* out) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("dict_block: out is null");
    }
    out->clear();
    out->reserve(n_entries_);
    for (size_t a = 0; a < anchor_offsets_.size(); ++a) {
        const size_t seg_begin = anchor_offsets_[a];
        const bool is_last = a + 1 == anchor_offsets_.size();
        const size_t seg_end = is_last ? (block_.size() - kNAnchorsBytes -
                                          anchor_offsets_.size() * kAnchorOffBytes)
                                       : anchor_offsets_[a + 1];
        if (seg_end < seg_begin || seg_end > block_.size()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "dict_block: anchor segment range invalid");
        }
        ByteSource src(block_.subslice(seg_begin, seg_end - seg_begin));
        std::string prev; // first entry of a segment is an anchor (prev_term="")
        while (!src.eof()) {
            DictEntry e;
            RETURN_IF_ERROR(decode_dict_entry(&src, std::string_view(prev), tier_, &e,
                                              term_stats_));
            prev = e.term;
            out->push_back(std::move(e));
        }
    }
    if (out->size() != n_entries_) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_block: decoded entry count mismatch");
    }
    return Status::OK();
}

Status DictBlockReader::scan_from_anchor(size_t anchor_idx, std::string_view target, bool* found,
                                         DictEntry* out) const {
    // Byte range of this anchor segment: [anchor_offset, next anchor offset or anchor table start).
    const size_t seg_begin = anchor_offsets_[anchor_idx];
    const bool is_last = anchor_idx + 1 == anchor_offsets_.size();
    const size_t seg_end =
            is_last ? (block_.size() - kNAnchorsBytes - anchor_offsets_.size() * kAnchorOffBytes)
                    : anchor_offsets_[anchor_idx + 1];

    // Fallback: open() has already verified anchor monotonicity; this additionally guards against seg_end<seg_begin underflow/out-of-range read.
    if (seg_end < seg_begin || seg_end > block_.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_block: anchor segment range invalid");
    }
    ByteSource src(block_.subslice(seg_begin, seg_end - seg_begin));
    std::string prev; // the first entry in the segment is an anchor, prev_term=""
    while (!src.eof()) {
        // Key-first: decode only the (front-coded) term key, then decide whether
        // the body is worth materializing. Non-matching entries skip their body
        // entirely -- with anchor_interval=16 that turns ~16 body decodes per
        // lookup into 1 (the matched entry) or 0 (a miss).
        DictEntry e;
        size_t body_start = 0;
        uint64_t entry_total = 0;
        RETURN_IF_ERROR(
                decode_dict_entry_key(&src, std::string_view(prev), &e, &body_start, &entry_total));
        if (e.term == target) {
            RETURN_IF_ERROR(
                    decode_dict_entry_rest(&src, tier_, body_start, entry_total, &e, term_stats_));
            *found = true;
            *out = std::move(e);
            return Status::OK();
        }
        if (std::string_view(e.term) > target) {
            *found = false; // already past target; entries are sorted so it does not exist
            return Status::OK();
        }
        // Before target: skip the body but keep the key as the front-coding base.
        RETURN_IF_ERROR(skip_dict_entry_body(&src, body_start, entry_total));
        prev = std::move(e.term);
    }
    *found = false;
    return Status::OK();
}

Status DictBlockReader::find_term(std::string_view target, bool* found, DictEntry* out) const {
    if (found == nullptr || out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("dict_block: found / out is null");
    }
    *found = false;
    size_t anchor_idx = 0;
    if (!locate_anchor(target, &anchor_idx)) {
        return Status::OK();
    }
    return scan_from_anchor(anchor_idx, target, found, out);
}

Status DictBlockReader::visit_prefix_range(std::string_view prefix,
                                           const std::function<bool(std::string_view)>& accept_key,
                                           const std::function<Status(DictEntry&&, bool*)>& on_hit,
                                           bool* prefix_exhausted) const {
    if (!on_hit || prefix_exhausted == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "dict_block: null visit_prefix_range args");
    }
    *prefix_exhausted = false;
    if (anchor_offsets_.empty()) {
        return Status::OK(); // empty block: nothing to enumerate
    }

    // Anchor-jump: start at the anchor segment that may contain prefix. Earlier
    // segments hold only terms < prefix (every term is < the next anchor term
    // <= prefix), so they are skipped without any decode. An empty prefix or one
    // sorting before the first anchor starts at anchor 0.
    size_t anchor_idx = 0;
    if (!prefix.empty()) {
        locate_anchor(prefix, &anchor_idx); // false leaves anchor_idx at 0
    }

    // Scan from the chosen anchor to the end of the entries region: the prefix
    // range may span several anchor segments. Anchor entries are encoded with
    // prefix_len=0, so a single running `prev` reconstructs every term correctly
    // even as the scan crosses segment boundaries.
    const size_t seg_begin = anchor_offsets_[anchor_idx];
    const size_t entries_end =
            block_.size() - kNAnchorsBytes - anchor_offsets_.size() * kAnchorOffBytes;
    if (entries_end < seg_begin || entries_end > block_.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "dict_block: entries region range invalid");
    }

    ByteSource src(block_.subslice(seg_begin, entries_end - seg_begin));
    std::string prev; // the chosen anchor segment starts at an anchor (prev="")
    while (!src.eof()) {
        DictEntry e;
        size_t body_start = 0;
        uint64_t entry_total = 0;
        RETURN_IF_ERROR(
                decode_dict_entry_key(&src, std::string_view(prev), &e, &body_start, &entry_total));
        const std::string_view t(e.term);
        if (t < prefix) {
            // Still before the range (only reachable inside the anchor segment
            // that straddles prefix): skip the body, keep the front-coding base.
            RETURN_IF_ERROR(skip_dict_entry_body(&src, body_start, entry_total));
            prev = std::move(e.term);
            continue;
        }
        const bool has_prefix = t.size() >= prefix.size() && t.starts_with(prefix);
        if (!has_prefix) {
            *prefix_exhausted = true; // sorted: no further matches here or later
            return Status::OK();
        }
        if (accept_key && !accept_key(t)) {
            // Key-only rejection: never pay for this entry's body.
            RETURN_IF_ERROR(skip_dict_entry_body(&src, body_start, entry_total));
            prev = std::move(e.term);
            continue;
        }
        // Accepted: materialize the body and hand the entry to the visitor.
        RETURN_IF_ERROR(
                decode_dict_entry_rest(&src, tier_, body_start, entry_total, &e, term_stats_));
        prev = e.term; // copy the key before the entry is moved into on_hit
        bool stop = false;
        RETURN_IF_ERROR(on_hit(std::move(e), &stop));
        if (stop) {
            return Status::OK();
        }
    }
    return Status::OK();
}

} // namespace doris::snii::format

namespace doris::snii::testing {
namespace {
std::atomic<uint64_t>& dict_decode_atomic() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}
} // namespace

uint64_t dict_decode_counter() {
    return dict_decode_atomic().load(std::memory_order_relaxed);
}

void reset_dict_decode_counter() {
    dict_decode_atomic().store(0, std::memory_order_relaxed);
}

void note_dict_block_decode() {
    dict_decode_atomic().fetch_add(1, std::memory_order_relaxed);
}

} // namespace doris::snii::testing
