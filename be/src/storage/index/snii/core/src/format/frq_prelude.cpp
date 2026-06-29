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

#include "snii/format/frq_prelude.h"

#include <algorithm>
#include <cstddef>
#include <limits>

#include "snii/encoding/byte_source.h"
#include "snii/encoding/crc32c.h"

namespace snii::format {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

namespace {

// Anti-DoS: a segment holds at most ~15M docs (>=1 doc/window), so 1<<24
// windows is a generous ceiling that still prevents multi-GB allocations from a
// crafted N. (crc32c is not a MAC and cannot defend a re-stamped inflated count.)
constexpr uint64_t kMaxWindows = 1ull << 24;

uint64_t ceil_div(uint64_t a, uint64_t b) {
    return (a + b - 1) / b;
}

uint8_t make_flags(const FrqPreludeColumns& cols) {
    uint8_t flags = 0;
    if (cols.has_freq) flags |= frq_prelude_flags::kHasFreq;
    if (cols.has_prx) flags |= frq_prelude_flags::kHasPrx;
    return flags;
}

uint8_t make_win_mode(const WindowMeta& m, bool has_freq) {
    uint8_t mode = 0;
    if (m.dd_zstd) mode |= frq_win_mode::kDdZstd;
    if (has_freq && m.freq_zstd) mode |= frq_win_mode::kFreqZstd;
    return mode;
}

doris::Status checked_add_u64(uint64_t lhs, uint64_t rhs, const char* message, uint64_t* out) {
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                message);
    }
    *out = lhs + rhs;
    return doris::Status::OK();
}

doris::Status checked_u32(uint64_t value, const char* message, uint32_t* out) {
    if (value > std::numeric_limits<uint32_t>::max()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                message);
    }
    *out = static_cast<uint32_t>(value);
    return doris::Status::OK();
}

doris::Status validate_window_doc_count(bool first_window, uint64_t win_base, uint64_t last_docid,
                                        uint64_t doc_count) {
    uint64_t first_docid = 0;
    if (!first_window) {
        RETURN_IF_ERROR(checked_add_u64(win_base, 1, "frq_prelude: window base exceeds docid range",
                                        &first_docid));
    }
    if (last_docid < first_docid) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: invalid window docid range");
    }
    const uint64_t width = last_docid - first_docid + 1;
    if (doc_count > width) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: doc_count exceeds window width");
    }
    return doris::Status::OK();
}

// Validates builder input: non-null sink, group_size>=1, sane count, and
// non-decreasing absolute last_docid across windows.
doris::Status validate_input(const FrqPreludeColumns& cols, ByteSink* out) {
    if (out == nullptr)
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq_prelude: null sink");
    if (cols.group_size == 0) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq_prelude: group_size must be >= 1");
    }
    if (cols.windows.size() > kMaxWindows) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq_prelude: window count exceeds cap");
    }
    for (size_t w = 1; w < cols.windows.size(); ++w) {
        if (cols.windows[w].last_docid < cols.windows[w - 1].last_docid) {
            return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                    "frq_prelude: last_docid not monotonic");
        }
    }
    return doris::Status::OK();
}

// Encodes one window row into a per-block sink. last_docid_delta is the row's
// absolute last_docid minus prev_last (the previous window's absolute last).
void encode_window_row(const WindowMeta& m, bool has_freq, bool has_prx, uint64_t prev_last,
                       ByteSink* block) {
    block->put_varint64(static_cast<uint64_t>(m.last_docid) - prev_last);
    block->put_varint64(m.doc_count);
    block->put_u8(make_win_mode(m, has_freq));
    block->put_varint64(m.dd_off);
    block->put_varint64(m.dd_disk_len);
    block->put_varint64(m.dd_uncomp_len);
    block->put_fixed32(m.crc_dd);
    if (has_freq) {
        block->put_varint64(m.freq_off);
        block->put_varint64(m.freq_disk_len);
        block->put_varint64(m.freq_uncomp_len);
        block->put_fixed32(m.crc_freq);
    }
    if (has_prx) {
        block->put_varint64(m.prx_off);
        block->put_varint64(m.prx_len);
    }
    block->put_varint64(m.max_freq);
    block->put_u8(m.max_norm);
}

// One super-block's serialized window block plus its directory fields.
struct SuperBlock {
    ByteSink block;
    uint64_t last_docid = 0; // absolute last docid of this super-block's last window
};

// Builds every super-block's window block (row-encoded) and records the running
// absolute last docid at each super-block boundary.
std::vector<SuperBlock> encode_super_blocks(const FrqPreludeColumns& cols) {
    const uint32_t g = cols.group_size;
    const size_t n = cols.windows.size();
    std::vector<SuperBlock> blocks;
    blocks.reserve(static_cast<size_t>(ceil_div(n, g)));
    uint64_t prev_last = 0; // previous window's absolute last docid (chains across blocks)
    for (size_t start = 0; start < n; start += g) {
        const size_t end = std::min(n, start + g);
        SuperBlock sb;
        for (size_t w = start; w < end; ++w) {
            encode_window_row(cols.windows[w], cols.has_freq, cols.has_prx, prev_last, &sb.block);
            prev_last = cols.windows[w].last_docid;
        }
        sb.last_docid = prev_last;
        blocks.push_back(std::move(sb));
    }
    return blocks;
}

// Serializes the super_block_dir (one row per super-block) into dir_sink, using
// each block's byte length to compute its offset within the window_dir region.
void encode_super_block_dir(const std::vector<SuperBlock>& blocks, ByteSink* dir_sink) {
    uint64_t prev_last = 0;
    uint64_t block_off = 0;
    for (const SuperBlock& sb : blocks) {
        dir_sink->put_varint64(sb.last_docid - prev_last);
        dir_sink->put_varint64(block_off);
        dir_sink->put_varint64(sb.block.size());
        prev_last = sb.last_docid;
        block_off += sb.block.size();
    }
}

} // namespace

doris::Status build_frq_prelude(const FrqPreludeColumns& cols, ByteSink* out) {
    RETURN_IF_ERROR(validate_input(cols, out));

    const std::vector<SuperBlock> blocks = encode_super_blocks(cols);
    ByteSink dir_sink;
    encode_super_block_dir(blocks, &dir_sink);

    // covered = header + super_block_dir (the crc covers exactly this region).
    ByteSink covered;
    covered.put_u8(make_flags(cols));
    covered.put_varint64(cols.windows.size());
    covered.put_varint64(cols.group_size);
    covered.put_varint64(blocks.size());
    covered.put_varint64(dir_sink.size());
    covered.put_bytes(dir_sink.view());

    out->put_bytes(covered.view());
    out->put_fixed32(crc32c(covered.view()));
    for (const SuperBlock& sb : blocks) out->put_bytes(sb.block.view());
    return doris::Status::OK();
}

namespace {

// Decoded header fields shared between parse phases.
struct Header {
    bool has_freq = false;
    bool has_prx = false;
    uint64_t n = 0;
    uint64_t group_size = 0;
    uint64_t n_super = 0;
    uint64_t sbdir_len = 0;
};

// Verifies the trailing crc covers [start of buffer .. end of super_block_dir].
// covered_len = header bytes (up to and including sbdir_len) + sbdir_len.
doris::Status verify_covered_crc(Slice prelude, size_t header_end, uint64_t sbdir_len) {
    const size_t covered = header_end + static_cast<size_t>(sbdir_len);
    if (covered + sizeof(uint32_t) > prelude.size()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: buffer too short for crc region");
    }
    uint32_t stored = 0;
    ByteSource crc_src(prelude.subslice(covered, sizeof(uint32_t)));
    RETURN_IF_ERROR(crc_src.get_fixed32(&stored));
    if (crc32c(prelude.subslice(0, covered)) != stored) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: crc32c mismatch");
    }
    return doris::Status::OK();
}

// Parses + validates the header (counts capped before any later reserve).
doris::Status parse_header(ByteSource* src, Header* h) {
    uint8_t flags = 0;
    RETURN_IF_ERROR(src->get_u8(&flags));
    h->has_freq = (flags & frq_prelude_flags::kHasFreq) != 0;
    h->has_prx = (flags & frq_prelude_flags::kHasPrx) != 0;
    RETURN_IF_ERROR(src->get_varint64(&h->n));
    RETURN_IF_ERROR(src->get_varint64(&h->group_size));
    RETURN_IF_ERROR(src->get_varint64(&h->n_super));
    RETURN_IF_ERROR(src->get_varint64(&h->sbdir_len));
    if (h->n > kMaxWindows || h->n_super > kMaxWindows) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: window count exceeds sane cap");
    }
    if (h->group_size == 0) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: group_size is zero");
    }
    if (h->n_super != ceil_div(h->n, h->group_size)) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: n_super inconsistent with N/G");
    }
    return doris::Status::OK();
}

// One super-block directory row.
struct SbDirRow {
    uint64_t last_docid = 0;
    uint64_t block_off = 0;
    uint64_t block_len = 0;
};

// Decodes the super_block_dir region into absolute-last-docid rows, validating
// monotonic last docids and contiguous, in-bounds block offsets.
doris::Status decode_super_block_dir(Slice dir, const Header& h, std::vector<SbDirRow>* rows,
                                     uint64_t* window_region_len) {
    ByteSource src(dir);
    rows->clear();
    rows->reserve(static_cast<size_t>(h.n_super));
    uint64_t prev_last = 0;
    uint64_t expect_off = 0;
    for (uint64_t s = 0; s < h.n_super; ++s) {
        SbDirRow r;
        uint64_t ldd = 0;
        RETURN_IF_ERROR(src.get_varint64(&ldd));
        RETURN_IF_ERROR(src.get_varint64(&r.block_off));
        RETURN_IF_ERROR(src.get_varint64(&r.block_len));
        RETURN_IF_ERROR(checked_add_u64(
                prev_last, ldd, "frq_prelude: super-block last_docid overflow", &r.last_docid));
        uint32_t checked_last = 0;
        RETURN_IF_ERROR(checked_u32(r.last_docid, "frq_prelude: super-block last_docid exceeds u32",
                                    &checked_last));
        if (r.last_docid < prev_last || r.block_off != expect_off) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "frq_prelude: super-block dir inconsistent");
        }
        expect_off += r.block_len;
        prev_last = r.last_docid;
        rows->push_back(r);
    }
    if (!src.eof()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: super-block dir has trailing bytes");
    }
    *window_region_len = expect_off;
    return doris::Status::OK();
}

// Validates a per-window codec mode byte against the known bits.
doris::Status check_win_mode(uint8_t mode, bool has_freq) {
    if ((mode & ~frq_win_mode::kKnownBits) != 0) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: unknown win_mode bits");
    }
    if (!has_freq && (mode & frq_win_mode::kFreqZstd) != 0) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: freq mode set without has_freq");
    }
    return doris::Status::OK();
}

// Decodes one window row, advancing prev_last to this window's absolute last.
doris::Status decode_window_row(ByteSource* src, bool has_freq, bool has_prx, bool first_window,
                                uint64_t* prev_last, WindowMeta* m) {
    uint64_t ldd = 0, doc_count = 0;
    RETURN_IF_ERROR(src->get_varint64(&ldd));
    RETURN_IF_ERROR(src->get_varint64(&doc_count));
    uint8_t mode = 0;
    RETURN_IF_ERROR(src->get_u8(&mode));
    RETURN_IF_ERROR(check_win_mode(mode, has_freq));
    m->dd_zstd = (mode & frq_win_mode::kDdZstd) != 0;
    m->freq_zstd = has_freq && (mode & frq_win_mode::kFreqZstd) != 0;
    RETURN_IF_ERROR(src->get_varint64(&m->dd_off));
    RETURN_IF_ERROR(src->get_varint64(&m->dd_disk_len));
    RETURN_IF_ERROR(src->get_varint64(&m->dd_uncomp_len));
    RETURN_IF_ERROR(src->get_fixed32(&m->crc_dd));
    if (has_freq) {
        RETURN_IF_ERROR(src->get_varint64(&m->freq_off));
        RETURN_IF_ERROR(src->get_varint64(&m->freq_disk_len));
        RETURN_IF_ERROR(src->get_varint64(&m->freq_uncomp_len));
        RETURN_IF_ERROR(src->get_fixed32(&m->crc_freq));
    }
    if (has_prx) {
        RETURN_IF_ERROR(src->get_varint64(&m->prx_off));
        RETURN_IF_ERROR(src->get_varint64(&m->prx_len));
    }
    uint64_t max_freq = 0;
    RETURN_IF_ERROR(src->get_varint64(&max_freq));
    RETURN_IF_ERROR(src->get_u8(&m->max_norm));
    uint64_t last_docid = 0;
    RETURN_IF_ERROR(checked_add_u64(*prev_last, ldd, "frq_prelude: window last_docid overflow",
                                    &last_docid));
    RETURN_IF_ERROR(validate_window_doc_count(first_window, *prev_last, last_docid, doc_count));
    m->win_base = *prev_last;
    RETURN_IF_ERROR(
            checked_u32(last_docid, "frq_prelude: window last_docid exceeds u32", &m->last_docid));
    RETURN_IF_ERROR(
            checked_u32(doc_count, "frq_prelude: window doc_count exceeds u32", &m->doc_count));
    RETURN_IF_ERROR(
            checked_u32(max_freq, "frq_prelude: window max_freq exceeds u32", &m->max_freq));
    *prev_last = last_docid;
    return doris::Status::OK();
}

// Decodes one super-block's window block (<=G rows) into the global window list,
// seeding win_base from prev_last and re-checking the recorded sb last docid.
doris::Status decode_one_block(Slice block, const Header& h, uint64_t sb_last_docid,
                               size_t row_count, uint64_t* prev_last,
                               std::vector<WindowMeta>* windows) {
    ByteSource src(block);
    for (size_t i = 0; i < row_count; ++i) {
        WindowMeta m;
        RETURN_IF_ERROR(
                decode_window_row(&src, h.has_freq, h.has_prx, windows->empty(), prev_last, &m));
        windows->push_back(m);
    }
    if (!src.eof()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: window block has trailing bytes");
    }
    if (*prev_last != sb_last_docid) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: window block last docid mismatch");
    }
    return doris::Status::OK();
}

// Decodes all window blocks pointed to by the super_block_dir.
doris::Status decode_all_blocks(Slice window_region, const Header& h,
                                const std::vector<SbDirRow>& dir,
                                std::vector<WindowMeta>* windows) {
    windows->clear();
    windows->reserve(static_cast<size_t>(h.n));
    uint64_t prev_last = 0;
    for (size_t s = 0; s < dir.size(); ++s) {
        const SbDirRow& r = dir[s];
        if (r.block_off + r.block_len > window_region.size() ||
            r.block_off + r.block_len < r.block_off) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "frq_prelude: window block out of region");
        }
        const uint64_t already = static_cast<uint64_t>(windows->size());
        const uint64_t rows = std::min<uint64_t>(h.group_size, h.n - already);
        Slice block = window_region.subslice(static_cast<size_t>(r.block_off),
                                             static_cast<size_t>(r.block_len));
        RETURN_IF_ERROR(decode_one_block(block, h, r.last_docid, static_cast<size_t>(rows),
                                         &prev_last, windows));
    }
    if (windows->size() != h.n) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: decoded window count mismatch");
    }
    return doris::Status::OK();
}

// Validates the dd/freq region locators tile the dd-block / freq-block contiguously
// (each region starts where the previous one ended) and returns the block lengths.
// Contiguity makes the docs-only prefix one solid run and bounds the read range.
doris::Status validate_region_layout(const Header& h, const std::vector<WindowMeta>& windows,
                                     uint64_t* dd_block_len, uint64_t* freq_block_len) {
    uint64_t dd_expect = 0;
    uint64_t freq_expect = 0;
    for (const WindowMeta& m : windows) {
        if (m.dd_off != dd_expect) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "frq_prelude: dd region not contiguous");
        }
        if (m.dd_disk_len > m.dd_uncomp_len && !m.dd_zstd) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "frq_prelude: raw dd region length inconsistent");
        }
        if (dd_expect + m.dd_disk_len < dd_expect) {
            return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "frq_prelude: dd block length overflow");
        }
        dd_expect += m.dd_disk_len;
        if (h.has_freq) {
            if (m.freq_off != freq_expect) {
                return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "frq_prelude: freq region not contiguous");
            }
            if (freq_expect + m.freq_disk_len < freq_expect) {
                return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "frq_prelude: freq block length overflow");
            }
            freq_expect += m.freq_disk_len;
        }
    }
    *dd_block_len = dd_expect;
    *freq_block_len = freq_expect;
    return doris::Status::OK();
}

} // namespace

doris::Status FrqPreludeReader::open(Slice prelude, FrqPreludeReader* out) {
    ByteSource src(prelude);
    Header h;
    RETURN_IF_ERROR(parse_header(&src, &h));
    const size_t header_end = src.position();
    RETURN_IF_ERROR(verify_covered_crc(prelude, header_end, h.sbdir_len));

    if (header_end + static_cast<size_t>(h.sbdir_len) > prelude.size()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: sbdir_len past buffer");
    }
    Slice dir = prelude.subslice(header_end, static_cast<size_t>(h.sbdir_len));
    std::vector<SbDirRow> rows;
    uint64_t window_region_len = 0;
    RETURN_IF_ERROR(decode_super_block_dir(dir, h, &rows, &window_region_len));

    const size_t region_start = header_end + static_cast<size_t>(h.sbdir_len) + sizeof(uint32_t);
    if (region_start + static_cast<size_t>(window_region_len) > prelude.size()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "frq_prelude: window region past buffer");
    }
    Slice window_region = prelude.subslice(region_start, static_cast<size_t>(window_region_len));

    out->has_freq_ = h.has_freq;
    out->has_prx_ = h.has_prx;
    out->group_size_ = static_cast<uint32_t>(h.group_size);
    out->n_super_ = static_cast<uint32_t>(h.n_super);
    out->sb_last_docid_.clear();
    out->sb_last_docid_.reserve(rows.size());
    for (const SbDirRow& r : rows) out->sb_last_docid_.push_back(r.last_docid);
    RETURN_IF_ERROR(decode_all_blocks(window_region, h, rows, &out->windows_));
    return validate_region_layout(h, out->windows_, &out->dd_block_len_, &out->freq_block_len_);
}

doris::Status FrqPreludeReader::window(uint32_t w, WindowMeta* out) const {
    if (out == nullptr)
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq_prelude: null window out");
    if (w >= windows_.size()) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq_prelude: window index out of range");
    }
    *out = windows_[w];
    return doris::Status::OK();
}

doris::Status FrqPreludeReader::locate_window(uint32_t docid, bool* found, uint32_t* w) const {
    if (found == nullptr || w == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>(
                "frq_prelude: null locate out");
    }
    *found = false;
    if (windows_.empty()) return doris::Status::OK();
    if (docid > windows_.back().last_docid) return doris::Status::OK();

    // Level 1: first super-block whose absolute last docid >= docid.
    const auto sb_it = std::lower_bound(sb_last_docid_.begin(), sb_last_docid_.end(),
                                        static_cast<uint64_t>(docid));
    const size_t sb = static_cast<size_t>(sb_it - sb_last_docid_.begin());
    // Level 2: window binary search within [sb*G, min((sb+1)*G, N)).
    const size_t lo = sb * group_size_;
    const size_t hi = std::min<size_t>(lo + group_size_, windows_.size());
    for (size_t i = lo; i < hi; ++i) {
        if (docid <= windows_[i].last_docid) {
            *found = true;
            *w = static_cast<uint32_t>(i);
            return doris::Status::OK();
        }
    }
    return doris::Status::OK(); // unreachable when invariants hold; defensive miss.
}

} // namespace snii::format
