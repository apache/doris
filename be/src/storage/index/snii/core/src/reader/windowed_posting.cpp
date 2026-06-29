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

#include "snii/reader/windowed_posting.h"

#include <cstddef>
#include <vector>

#include "snii/common/slice.h"
#include "snii/encoding/byte_source.h"
#include "snii/format/frq_pod.h"
#include "snii/format/frq_prelude.h"
#include "snii/format/prx_pod.h"
#include "snii/io/batch_range_fetcher.h"

namespace snii::reader {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

using snii::format::DictEntry;
using snii::format::FrqPreludeReader;
using snii::format::FrqRegionMeta;
using snii::format::WindowMeta;

namespace {

// Resolves the absolute file offset of the prelude bytes for a windowed entry.
// The frq span lives in the interleaved posting region (after the term's prx span).
uint64_t PreludeAbs(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t frq_base) {
    const auto& region = idx.section_refs().posting_region;
    return region.offset + frq_base + entry.frq_off_delta;
}

// Validates that [off, off+len) fits within [0, total).
doris::Status InBounds(uint64_t off, uint64_t len, uint64_t total) {
    if (off > total || len > total - off) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("windowed_posting: range out of section");
    }
    return doris::Status::OK();
}

// Block geometry of a windowed entry's grouped .frq payload (all offsets absolute).
struct BlockGeometry {
    uint64_t dd_block_off = 0; // absolute start of the dd-block
    uint64_t dd_block_len = 0;
    uint64_t freq_block_off = 0; // absolute start of the freq-block
    uint64_t freq_block_len = 0;
    uint64_t frq_region_len = 0; // entry.frq_len - prelude_len (dd-block + freq-block)
};

// Derives the dd-block / freq-block absolute ranges from the entry + prelude,
// validating they tile the post-prelude .frq region exactly.
doris::Status ResolveBlocks(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t frq_base,
                     const FrqPreludeReader& prelude, BlockGeometry* g) {
    if (entry.prelude_len > entry.frq_len) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("windowed_posting: prelude_len exceeds frq_len");
    }
    const uint64_t frq_window_start = PreludeAbs(idx, entry, frq_base) + entry.prelude_len;
    g->frq_region_len = entry.frq_len - entry.prelude_len;
    g->dd_block_len = prelude.dd_block_len();
    g->freq_block_len = prelude.freq_block_len();
    // dd-block + freq-block must fit exactly within the post-prelude region.
    if (g->dd_block_len > g->frq_region_len ||
        g->freq_block_len > g->frq_region_len - g->dd_block_len) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("windowed_posting: blocks exceed frq region");
    }
    g->dd_block_off = frq_window_start;
    g->freq_block_off = frq_window_start + g->dd_block_len;
    return doris::Status::OK();
}

// Per-window decode state for the full-posting path.
struct WindowSlices {
    WindowMeta meta;
    Slice dd_region;
    Slice freq_region;
    Slice prx_window;
};

// Carves window w's dd (and freq when want_freq) sub-slices out of the fetched
// blocks, validating each locator against its block length.
doris::Status CarveRegionSlices(const WindowMeta& m, Slice dd_block, Slice freq_block, bool want_freq,
                         WindowSlices* out) {
    RETURN_IF_ERROR(InBounds(m.dd_off, m.dd_disk_len, dd_block.size()));
    out->dd_region =
            dd_block.subslice(static_cast<size_t>(m.dd_off), static_cast<size_t>(m.dd_disk_len));
    if (!want_freq) return doris::Status::OK();
    RETURN_IF_ERROR(InBounds(m.freq_off, m.freq_disk_len, freq_block.size()));
    out->freq_region = freq_block.subslice(static_cast<size_t>(m.freq_off),
                                           static_cast<size_t>(m.freq_disk_len));
    return doris::Status::OK();
}

// Decodes window w from the fetched blocks (+ optional prx slice) and appends to out.
doris::Status AppendWindow(const WindowSlices& ws, bool want_positions, bool want_freq,
                    DecodedPosting* out) {
    std::vector<uint32_t> docids, freqs;
    std::vector<std::vector<uint32_t>> pos;
    RETURN_IF_ERROR(decode_window_slices(ws.meta, ws.dd_region, ws.freq_region, ws.prx_window,
                                              want_positions, want_freq, &docids, &freqs, &pos));
    out->docids.insert(out->docids.end(), docids.begin(), docids.end());
    out->freqs.insert(out->freqs.end(), freqs.begin(), freqs.end());
    if (want_positions) {
        for (auto& v : pos) out->positions.push_back(std::move(v));
    }
    return doris::Status::OK();
}

} // namespace

doris::Status fetch_windowed_prelude(const LogicalIndexReader& idx, const DictEntry& entry,
                              uint64_t frq_base, FrqPreludeReader* prelude) {
    if (entry.prelude_len == 0) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("windowed_posting: windowed entry has no prelude");
    }
    if (entry.prelude_len > entry.frq_len) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("windowed_posting: prelude_len exceeds frq_len");
    }
    const uint64_t prelude_abs = PreludeAbs(idx, entry, frq_base);
    snii::io::BatchRangeFetcher fetcher(idx.reader());
    const size_t h = fetcher.add(prelude_abs, entry.prelude_len);
    RETURN_IF_ERROR(fetcher.fetch());
    return FrqPreludeReader::open(fetcher.get(h), prelude);
}

doris::Status windowed_window_range(const LogicalIndexReader& idx, const DictEntry& entry,
                             uint64_t frq_base, uint64_t prx_base, const FrqPreludeReader& prelude,
                             uint32_t w, bool want_positions, bool want_freq, WindowAbsRange* out) {
    if (out == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("windowed_posting: null range");
    *out = WindowAbsRange {};
    BlockGeometry g;
    RETURN_IF_ERROR(ResolveBlocks(idx, entry, frq_base, prelude, &g));
    WindowMeta meta;
    RETURN_IF_ERROR(prelude.window(w, &meta));

    // dd sub-range within the dd-block.
    RETURN_IF_ERROR(InBounds(meta.dd_off, meta.dd_disk_len, g.dd_block_len));
    out->dd_off = g.dd_block_off + meta.dd_off;
    out->dd_len = meta.dd_disk_len;

    if (want_freq) {
        RETURN_IF_ERROR(InBounds(meta.freq_off, meta.freq_disk_len, g.freq_block_len));
        out->freq_off = g.freq_block_off + meta.freq_off;
        out->freq_len = meta.freq_disk_len;
    }

    if (!want_positions) return doris::Status::OK();
    if (!prelude.has_prx()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("windowed_posting: positions requested but prelude has none");
    }
    const uint64_t prx_region_start =
            idx.section_refs().posting_region.offset + prx_base + entry.prx_off_delta;
    RETURN_IF_ERROR(InBounds(meta.prx_off, meta.prx_len, entry.prx_len));
    out->prx_off = prx_region_start + meta.prx_off;
    out->prx_len = meta.prx_len;
    return doris::Status::OK();
}

doris::Status decode_window_slices(const WindowMeta& meta, Slice dd_region, Slice freq_region,
                            Slice prx_window, bool want_positions, bool want_freq,
                            std::vector<uint32_t>* docids, std::vector<uint32_t>* freqs,
                            std::vector<std::vector<uint32_t>>* positions) {
    FrqRegionMeta dd_meta;
    dd_meta.zstd = meta.dd_zstd;
    dd_meta.uncomp_len = meta.dd_uncomp_len;
    dd_meta.disk_len = meta.dd_disk_len;
    dd_meta.crc = meta.crc_dd;
    dd_meta.verify_crc = meta.verify_crc;
    RETURN_IF_ERROR(snii::format::decode_dd_region(dd_region, dd_meta, meta.win_base, docids));
    if (docids->size() != meta.doc_count) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("windowed_posting: frq doc_count mismatch");
    }
    if (want_freq) {
        FrqRegionMeta freq_meta;
        freq_meta.zstd = meta.freq_zstd;
        freq_meta.uncomp_len = meta.freq_uncomp_len;
        freq_meta.disk_len = meta.freq_disk_len;
        freq_meta.crc = meta.crc_freq;
        freq_meta.verify_crc = meta.verify_crc;
        RETURN_IF_ERROR(
                snii::format::decode_freq_region(freq_region, freq_meta, meta.doc_count, freqs));
    } else {
        freqs->clear();
    }
    if (!want_positions) return doris::Status::OK();

    ByteSource psrc(prx_window);
    RETURN_IF_ERROR(snii::format::read_prx_window(&psrc, positions));
    if (positions->size() != docids->size()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("windowed_posting: prx/frq doc-count mismatch");
    }
    return doris::Status::OK();
}

namespace {

// Fetches the dd-block (always), the freq-block (when want_freq) and the whole .prx
// region (when want_positions) of a windowed entry in ONE batch and returns the
// in-memory block slices. The dd-block is a single contiguous range -> the
// docid-only / phrase path reads it as one Range GET (the byte-saving core).
doris::Status FetchBlocks(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t prx_base,
                   const BlockGeometry& g, bool want_positions, bool want_freq,
                   snii::io::BatchRangeFetcher* fetcher, size_t* dd_h, size_t* freq_h,
                   size_t* prx_h) {
    *dd_h = fetcher->add(g.dd_block_off, g.dd_block_len);
    if (want_freq) {
        *freq_h = fetcher->add(g.freq_block_off, g.freq_block_len);
    }
    if (want_positions) {
        const uint64_t prx_region_start =
                idx.section_refs().posting_region.offset + prx_base + entry.prx_off_delta;
        *prx_h = fetcher->add(prx_region_start, entry.prx_len);
    }
    return fetcher->fetch();
}

} // namespace

doris::Status read_windowed_posting(const LogicalIndexReader& idx, const DictEntry& entry,
                             uint64_t frq_base, uint64_t prx_base, bool want_positions,
                             bool want_freq, DecodedPosting* out) {
    if (out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("windowed_posting: null out");
    }
    *out = DecodedPosting {};

    FrqPreludeReader prelude;
    RETURN_IF_ERROR(fetch_windowed_prelude(idx, entry, frq_base, &prelude));
    if (want_positions && !prelude.has_prx()) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("windowed_posting: positions requested but prelude has none");
    }
    BlockGeometry g;
    RETURN_IF_ERROR(ResolveBlocks(idx, entry, frq_base, prelude, &g));

    snii::io::BatchRangeFetcher fetcher(idx.reader());
    size_t dd_h = 0, freq_h = 0, prx_h = 0;
    RETURN_IF_ERROR(FetchBlocks(idx, entry, prx_base, g, want_positions, want_freq, &fetcher,
                                     &dd_h, &freq_h, &prx_h));
    const Slice dd_block = fetcher.get(dd_h);
    const Slice freq_block = want_freq ? fetcher.get(freq_h) : Slice();
    const Slice prx_region = want_positions ? fetcher.get(prx_h) : Slice();

    const uint32_t n = prelude.n_windows();
    for (uint32_t w = 0; w < n; ++w) {
        WindowSlices ws;
        RETURN_IF_ERROR(prelude.window(w, &ws.meta));
        RETURN_IF_ERROR(CarveRegionSlices(ws.meta, dd_block, freq_block, want_freq, &ws));
        if (want_positions) {
            RETURN_IF_ERROR(InBounds(ws.meta.prx_off, ws.meta.prx_len, prx_region.size()));
            ws.prx_window = prx_region.subslice(static_cast<size_t>(ws.meta.prx_off),
                                                static_cast<size_t>(ws.meta.prx_len));
        }
        RETURN_IF_ERROR(AppendWindow(ws, want_positions, want_freq, out));
    }
    return doris::Status::OK();
}

} // namespace snii::reader
