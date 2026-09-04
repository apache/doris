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

// T18 (FP-01..FP-09): FrqPrelude window-row trim. The trimmed on-disk row drops
// the three running-sum offsets (dd_off/freq_off/prx_off) and the two raw-region
// uncomp_len fields; the reader DERIVES them. These tests pin round-trip
// equivalence of the derived WindowMeta, the exact/shrunken byte size, the
// cross-super-block accumulation, the conditional zstd uncomp_len, corruption on
// truncation / sum-overflow, and end-to-end query equivalence with a smaller
// prelude fetch.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <numeric>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii_query_test_util.h"

using doris::Status;
using doris::snii::ByteSink;
using doris::snii::Slice;
using doris::snii::varint_len;
using doris::snii::format::build_frq_prelude;
using doris::snii::format::DictEntry;
using doris::snii::format::FrqPreludeColumns;
using doris::snii::format::FrqPreludeReader;
using doris::snii::format::WindowMeta;
using doris::snii::query::term_query;
using doris::snii::reader::LogicalIndexReader;
using doris::snii::reader::SniiSegmentReader;
using doris::snii::snii_test::assert_ok;
using doris::snii::snii_test::build_reader;
using doris::snii::snii_test::MemoryFile;

namespace {

// Writer constant (logical_index_writer.cpp kPreludeGroupSize): windows per
// super-block. Used both by the make_windows fixture and to reconstruct the E2E
// term's columns for the size model.
constexpr uint32_t kPreludeGroupSize = 64;

// ---------------------------------------------------------------------------
// Exact byte-size model of the prelude. row_bytes/prelude_size mirror
// encode_window_row / build_frq_prelude field-for-field so prelude_size(kNew)
// reproduces the real build output exactly (cross-checked by EXPECT_EQ against
// sink.size() in every round-trip test), while prelude_size(kOld) models the
// pre-T18 layout (three extra offset varints + unconditional uncomp_len).
// ---------------------------------------------------------------------------
enum class Layout { kNew, kOld };

size_t row_bytes(const WindowMeta& m, uint64_t last_docid_delta, bool has_freq, bool has_prx,
                 Layout layout) {
    const bool old_layout = layout == Layout::kOld;
    size_t n = 0;
    n += varint_len(last_docid_delta);
    n += varint_len(m.doc_count);
    n += 1; // win_mode (u8)
    if (old_layout) {
        n += varint_len(m.dd_off);
    }
    n += varint_len(m.dd_disk_len);
    if (old_layout || m.dd_zstd) {
        n += varint_len(m.dd_uncomp_len);
    }
    n += sizeof(uint32_t); // crc_dd
    if (has_freq) {
        if (old_layout) {
            n += varint_len(m.freq_off);
        }
        n += varint_len(m.freq_disk_len);
        if (old_layout || m.freq_zstd) {
            n += varint_len(m.freq_uncomp_len);
        }
        n += sizeof(uint32_t); // crc_freq
    }
    if (has_prx) {
        if (old_layout) {
            n += varint_len(m.prx_off);
        }
        n += varint_len(m.prx_len);
    }
    n += varint_len(m.max_freq);
    n += 1; // max_norm (u8)
    return n;
}

size_t prelude_size(const FrqPreludeColumns& cols, Layout layout) {
    const uint64_t g = cols.group_size;
    const size_t n = cols.windows.size();
    const size_t n_super = (g == 0) ? 0 : (n + static_cast<size_t>(g) - 1) / static_cast<size_t>(g);

    // Per-super-block window-block byte length + absolute last docid.
    std::vector<size_t> block_len(n_super, 0);
    std::vector<uint64_t> block_last(n_super, 0);
    uint64_t prev_last = 0;
    size_t s = 0;
    for (size_t start = 0; start < n; start += static_cast<size_t>(g), ++s) {
        const size_t end = std::min(n, start + static_cast<size_t>(g));
        size_t bl = 0;
        for (size_t w = start; w < end; ++w) {
            const uint64_t delta = cols.windows[w].last_docid - prev_last;
            bl += row_bytes(cols.windows[w], delta, cols.has_freq, cols.has_prx, layout);
            prev_last = cols.windows[w].last_docid;
        }
        block_len[s] = bl;
        block_last[s] = prev_last;
    }

    // super_block_dir: VInt last_docid_delta + VInt block_off + VInt block_len.
    size_t sbdir_len = 0;
    uint64_t dir_prev_last = 0;
    uint64_t block_off = 0;
    for (size_t i = 0; i < n_super; ++i) {
        sbdir_len += varint_len(block_last[i] - dir_prev_last);
        sbdir_len += varint_len(block_off);
        sbdir_len += varint_len(block_len[i]);
        dir_prev_last = block_last[i];
        block_off += block_len[i];
    }

    size_t window_region = 0;
    for (size_t i = 0; i < n_super; ++i) {
        window_region += block_len[i];
    }

    // covered = u8 flags + VInt N + VInt G + VInt n_super + VInt sbdir_len + sbdir.
    const size_t covered = 1 + varint_len(n) + varint_len(g) + varint_len(n_super) +
                           varint_len(sbdir_len) + sbdir_len;
    return covered + sizeof(uint32_t) /*crc*/ + window_region;
}

// Builds n contiguous stride-100 windows with deterministic, all-raw metadata.
// dd/freq/prx offsets are the CONTIGUOUS running prefix sums the reader derives;
// raw uncomp_len == disk_len. Region disk lengths are constant so the NEW row is a
// fixed 16 B (docs+positions) / 10 B (docs-only), while the OLD row's extra offset
// varints grow past one byte for most windows (~24 B / ~13 B).
FrqPreludeColumns make_windows(uint32_t n, uint32_t group_size, bool has_freq, bool has_prx) {
    FrqPreludeColumns cols;
    cols.has_freq = has_freq;
    cols.has_prx = has_prx;
    cols.group_size = group_size;
    uint64_t dd_run = 0;
    uint64_t freq_run = 0;
    uint64_t prx_run = 0;
    for (uint32_t w = 0; w < n; ++w) {
        WindowMeta m;
        m.last_docid = (w + 1) * 100 - 1; // window w covers docids [w*100, w*100+99]
        m.doc_count = 10;
        m.dd_zstd = false;
        m.dd_off = dd_run;
        m.dd_disk_len = 64;
        m.dd_uncomp_len = 64; // raw => == disk_len
        m.crc_dd = 0xDD000000U + w;
        dd_run += m.dd_disk_len;
        if (has_freq) {
            m.freq_zstd = false;
            m.freq_off = freq_run;
            m.freq_disk_len = 48;
            m.freq_uncomp_len = 48;
            m.crc_freq = 0xEE000000U + w;
            freq_run += m.freq_disk_len;
        }
        if (has_prx) {
            m.prx_off = prx_run;
            m.prx_len = 96;
            prx_run += m.prx_len;
        }
        m.max_freq = 5;
        m.max_norm = 42;
        cols.windows.push_back(m);
    }
    return cols;
}

// Round-trips columns through build + open, asserting every decoded WindowMeta
// field (incl. the DERIVED dd_off/freq_off/prx_off/uncomp_len) matches the input,
// and that the real build size equals the exact NEW model.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
void expect_round_trip(const FrqPreludeColumns& cols) {
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());

    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());
    ASSERT_EQ(reader.n_windows(), cols.windows.size());
    EXPECT_EQ(reader.has_freq(), cols.has_freq);
    EXPECT_EQ(reader.has_prx(), cols.has_prx);

    uint64_t expect_win_base = 0;
    uint64_t dd_run = 0;
    uint64_t freq_run = 0;
    uint64_t prx_run = 0;
    for (uint32_t w = 0; w < reader.n_windows(); ++w) {
        WindowMeta got;
        ASSERT_TRUE(reader.window(w, &got).ok()) << "w=" << w;
        const WindowMeta& exp = cols.windows[w];
        EXPECT_EQ(got.last_docid, exp.last_docid) << "w=" << w;
        EXPECT_EQ(got.win_base, expect_win_base) << "w=" << w;
        EXPECT_EQ(got.doc_count, exp.doc_count) << "w=" << w;
        EXPECT_EQ(got.dd_zstd, exp.dd_zstd) << "w=" << w;
        EXPECT_EQ(got.dd_off, exp.dd_off) << "w=" << w; // derived == explicit sum
        EXPECT_EQ(got.dd_off, dd_run) << "w=" << w;     // ...and == our running sum
        EXPECT_EQ(got.dd_disk_len, exp.dd_disk_len) << "w=" << w;
        EXPECT_EQ(got.dd_uncomp_len, exp.dd_uncomp_len) << "w=" << w;
        EXPECT_EQ(got.crc_dd, exp.crc_dd) << "w=" << w;
        dd_run += got.dd_disk_len;
        if (cols.has_freq) {
            EXPECT_EQ(got.freq_zstd, exp.freq_zstd) << "w=" << w;
            EXPECT_EQ(got.freq_off, exp.freq_off) << "w=" << w;
            EXPECT_EQ(got.freq_off, freq_run) << "w=" << w;
            EXPECT_EQ(got.freq_disk_len, exp.freq_disk_len) << "w=" << w;
            EXPECT_EQ(got.freq_uncomp_len, exp.freq_uncomp_len) << "w=" << w;
            EXPECT_EQ(got.crc_freq, exp.crc_freq) << "w=" << w;
            freq_run += got.freq_disk_len;
        }
        if (cols.has_prx) {
            EXPECT_EQ(got.prx_off, exp.prx_off) << "w=" << w;
            EXPECT_EQ(got.prx_off, prx_run) << "w=" << w;
            EXPECT_EQ(got.prx_len, exp.prx_len) << "w=" << w;
            prx_run += got.prx_len;
        }
        EXPECT_EQ(got.max_freq, exp.max_freq) << "w=" << w;
        EXPECT_EQ(got.max_norm, exp.max_norm) << "w=" << w;
        expect_win_base = exp.last_docid;
    }
    EXPECT_EQ(reader.dd_block_len(), dd_run);
    if (cols.has_freq) {
        EXPECT_EQ(reader.freq_block_len(), freq_run);
    }
    EXPECT_EQ(sink.size(), prelude_size(cols, Layout::kNew)); // model tracks the real encoder
}

} // namespace

// FP-01: N=200 windows (has_freq+has_prx, all raw) round-trip; the derived
// offsets/uncomp_lens equal the explicit running sums the columns carried.
TEST(SniiFrqPreludeTest, FP01DerivedOffsetsMatchExplicit) {
    expect_round_trip(make_windows(/*n=*/200, kPreludeGroupSize, /*has_freq=*/true,
                                   /*has_prx=*/true));
}

// FP-02: the trimmed prelude hits an EXACT byte size and is strictly smaller than
// the pre-T18 layout, for both docs+positions and docs-only rows.
TEST(SniiFrqPreludeTest, FP02RowTrimShrinksPreludeBytes) {
    // docs+positions: drops 5 varints/row (dd_off, dd_uncomp_len, freq_off,
    // freq_uncomp_len, prx_off).
    const FrqPreludeColumns dp = make_windows(200, kPreludeGroupSize, /*has_freq=*/true,
                                              /*has_prx=*/true);
    ByteSink dp_sink;
    ASSERT_TRUE(build_frq_prelude(dp, &dp_sink).ok());
    const size_t dp_new = prelude_size(dp, Layout::kNew);
    const size_t dp_old = prelude_size(dp, Layout::kOld);
    EXPECT_EQ(dp_sink.size(), dp_new);                  // exact new size
    EXPECT_LT(dp_new, dp_old);                          // strictly < pre-T18
    EXPECT_GE(dp_old - dp_new, 5U * dp.windows.size()); // >= 5 bytes/row removed
    EXPECT_GT((dp_old - dp_new) * 100, dp_old * 25);    // ~1/3 saved (30-40% band)

    // docs-only: no freq / no positions -> drops only dd_off + dd_uncomp_len.
    const FrqPreludeColumns doc = make_windows(200, kPreludeGroupSize, /*has_freq=*/false,
                                               /*has_prx=*/false);
    ByteSink doc_sink;
    ASSERT_TRUE(build_frq_prelude(doc, &doc_sink).ok());
    const size_t doc_new = prelude_size(doc, Layout::kNew);
    const size_t doc_old = prelude_size(doc, Layout::kOld);
    EXPECT_EQ(doc_sink.size(), doc_new);
    EXPECT_LT(doc_new, doc_old);
    EXPECT_GE(doc_old - doc_new, 2U * doc.windows.size()); // >= 2 bytes/row removed
}

// FP-03: degenerate empty prelude (N=0) builds + opens with zero windows.
TEST(SniiFrqPreludeTest, FP03EmptyWindows) {
    FrqPreludeColumns cols;
    cols.has_freq = true;
    cols.has_prx = true;
    cols.group_size = kPreludeGroupSize;
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());
    EXPECT_EQ(reader.n_windows(), 0U);
    EXPECT_EQ(reader.n_super_blocks(), 0U);
    EXPECT_EQ(reader.dd_block_len(), 0U);
    EXPECT_EQ(reader.freq_block_len(), 0U);
    EXPECT_EQ(sink.size(), prelude_size(cols, Layout::kNew));
}

// FP-04: single-window round-trip (win_base=0, one super-block).
TEST(SniiFrqPreludeTest, FP04SingleWindow) {
    expect_round_trip(
            make_windows(/*n=*/1, kPreludeGroupSize, /*has_freq=*/true, /*has_prx=*/true));
}

// FP-05: a df~20000 term (adaptive unit=1024 -> ~20 windows) split into multiple
// super-blocks; the derived dd/freq/prx offsets must accumulate ACROSS block
// boundaries (not reset per block).
TEST(SniiFrqPreludeTest, FP05CrossSuperBlockAccumulation) {
    constexpr uint32_t kN = 20; // ceil(20000 / kAdaptiveWindowDocs=1024)
    constexpr uint32_t kG = 8;  // force 3 super-blocks (8, 8, 4)
    const FrqPreludeColumns cols = make_windows(kN, kG, /*has_freq=*/true, /*has_prx=*/true);
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());
    ASSERT_EQ(reader.n_windows(), kN);
    ASSERT_GT(reader.n_super_blocks(), 1U); // genuinely multi-block

    uint64_t dd_run = 0;
    uint64_t freq_run = 0;
    uint64_t prx_run = 0;
    for (uint32_t w = 0; w < kN; ++w) {
        WindowMeta got;
        ASSERT_TRUE(reader.window(w, &got).ok()) << "w=" << w;
        EXPECT_EQ(got.dd_off, dd_run) << "w=" << w; // continuous over all blocks
        EXPECT_EQ(got.freq_off, freq_run) << "w=" << w;
        EXPECT_EQ(got.prx_off, prx_run) << "w=" << w;
        dd_run += got.dd_disk_len;
        freq_run += got.freq_disk_len;
        prx_run += got.prx_len;
    }
    EXPECT_EQ(reader.dd_block_len(), dd_run);
    EXPECT_EQ(reader.freq_block_len(), freq_run);
}

// FP-06: a zstd window stores its uncomp_len (!= disk_len) and reads it back, while
// a raw window in the same prelude derives uncomp_len == disk_len (no stored field).
TEST(SniiFrqPreludeTest, FP06ZstdWindowStoresConditionalUncompLen) {
    FrqPreludeColumns cols;
    cols.has_freq = true;
    cols.has_prx = false;
    cols.group_size = kPreludeGroupSize;

    WindowMeta z; // window 0: zstd dd + zstd freq, uncomp != disk
    z.last_docid = 99;
    z.doc_count = 10;
    z.dd_zstd = true;
    z.dd_off = 0;
    z.dd_disk_len = 40;
    z.dd_uncomp_len = 137; // meaningful only because dd_zstd
    z.crc_dd = 0x0000ABCDU;
    z.freq_zstd = true;
    z.freq_off = 0;
    z.freq_disk_len = 20;
    z.freq_uncomp_len = 71;
    z.crc_freq = 0x00001234U;
    z.max_freq = 3;
    z.max_norm = 7;

    WindowMeta r; // window 1: raw dd + raw freq, uncomp == disk
    r.last_docid = 199;
    r.doc_count = 10;
    r.dd_zstd = false;
    r.dd_off = z.dd_disk_len;
    r.dd_disk_len = 55;
    r.dd_uncomp_len = 55;
    r.crc_dd = 0x0000BEEFU;
    r.freq_zstd = false;
    r.freq_off = z.freq_disk_len;
    r.freq_disk_len = 25;
    r.freq_uncomp_len = 25;
    r.crc_freq = 0x00005678U;
    r.max_freq = 4;
    r.max_norm = 9;
    cols.windows = {z, r};

    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());

    WindowMeta g0;
    WindowMeta g1;
    ASSERT_TRUE(reader.window(0, &g0).ok());
    ASSERT_TRUE(reader.window(1, &g1).ok());
    EXPECT_TRUE(g0.dd_zstd);
    EXPECT_EQ(g0.dd_disk_len, 40U);
    EXPECT_EQ(g0.dd_uncomp_len, 137U); // stored + read back
    EXPECT_NE(g0.dd_uncomp_len, g0.dd_disk_len);
    EXPECT_TRUE(g0.freq_zstd);
    EXPECT_EQ(g0.freq_uncomp_len, 71U);
    EXPECT_NE(g0.freq_uncomp_len, g0.freq_disk_len);
    EXPECT_FALSE(g1.dd_zstd);
    EXPECT_EQ(g1.dd_uncomp_len, g1.dd_disk_len); // raw: derived, not stored
    EXPECT_EQ(g1.freq_uncomp_len, g1.freq_disk_len);
    EXPECT_EQ(sink.size(), prelude_size(cols, Layout::kNew));
}

// FP-07: a prelude truncated into its window blocks fails to open.
TEST(SniiFrqPreludeTest, FP07TruncatedPreludeIsCorruption) {
    const FrqPreludeColumns cols = make_windows(8, 4, /*has_freq=*/true, /*has_prx=*/true);
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    std::vector<uint8_t> bytes = sink.buffer();
    ASSERT_GT(bytes.size(), 6U);
    bytes.resize(bytes.size() - 6); // chop into the last window block
    FrqPreludeReader reader;
    const Status s = FrqPreludeReader::open(Slice(bytes), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << s.to_string();
}

// FP-08: two raw windows whose dd_disk_len values overflow the derived dd-block
// offset accumulator (checked_add_u64) are rejected on open.
TEST(SniiFrqPreludeTest, FP08DdDiskLenSumOverflowIsCorruption) {
    FrqPreludeColumns cols;
    cols.has_freq = false;
    cols.has_prx = false;
    cols.group_size = kPreludeGroupSize;

    WindowMeta a;
    a.last_docid = 10;
    a.doc_count = 1;
    a.dd_disk_len = std::numeric_limits<uint64_t>::max() / 2 + 100;
    a.dd_uncomp_len = a.dd_disk_len; // raw
    a.crc_dd = 1;
    a.max_freq = 1;
    a.max_norm = 0;
    WindowMeta b = a;
    b.last_docid = 20; // second window's derivation overflows: sum > UINT64_MAX
    b.crc_dd = 2;
    cols.windows = {a, b};

    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok()); // builder does not sum-check
    FrqPreludeReader reader;
    const Status s = FrqPreludeReader::open(sink.view(), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << s.to_string();
}

// FP-09 (E2E): a high-df windowed term ("failed", 9000 docs) queried through the
// real reader returns the full docid set (new path == old semantics), the docid
// path fetches exactly the (now-smaller) prelude range once, and the on-disk
// prelude_len matches the NEW model and is strictly smaller than the pre-T18 one.
TEST(SniiFrqPreludeTest, FP09WindowedEntryPreludeShrinksAndQueryEquivalent) {
    MemoryFile file;
    SniiSegmentReader segment_reader;
    LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    bool found = false;
    DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index_reader.lookup("failed", &found, &entry, &frq_base, &prx_base));
    ASSERT_TRUE(found);
    ASSERT_GT(entry.prelude_len, 0U) << "expected a windowed entry with a prelude";

    const uint64_t prelude_abs =
            index_reader.section_refs().posting_region.offset + frq_base + entry.frq_off_delta;

    // Reconstruct the term's windows from the (new) prelude and model both layouts:
    // the on-disk length must equal the NEW model and be strictly below the OLD one.
    std::vector<uint8_t> prelude_bytes;
    assert_ok(file.read_at(prelude_abs, entry.prelude_len, &prelude_bytes));
    FrqPreludeReader prelude;
    assert_ok(FrqPreludeReader::open(Slice(prelude_bytes), &prelude));
    ASSERT_GT(prelude.n_windows(), 1U); // genuinely windowed

    FrqPreludeColumns cols;
    cols.has_freq = prelude.has_freq();
    cols.has_prx = prelude.has_prx();
    cols.group_size = kPreludeGroupSize;
    for (uint32_t w = 0; w < prelude.n_windows(); ++w) {
        WindowMeta m;
        assert_ok(prelude.window(w, &m));
        cols.windows.push_back(m);
    }
    EXPECT_EQ(entry.prelude_len, prelude_size(cols, Layout::kNew)); // on-disk == new model
    EXPECT_LT(entry.prelude_len, prelude_size(cols, Layout::kOld)); // shrinks vs pre-T18

    // Functional equivalence + deterministic IO on the docid path.
    file.clear_reads();
    std::vector<uint32_t> docids;
    assert_ok(term_query(index_reader, "failed", &docids));

    std::vector<uint32_t> expected(9000);
    std::iota(expected.begin(), expected.end(), 0U);
    EXPECT_EQ(docids, expected);

    // The docid path issues ONE coalesced read starting at the prelude and spanning the
    // prelude + dd-block (docid-only: the freq-block is not fetched), i.e. a single
    // round-trip whose length lies within [prelude_len, frq_len]. The prelude — now the
    // shrunk new-model size (prelude_len == new < old, asserted above) — is the head of
    // that read, so the end-to-end docid fetch is strictly smaller than pre-T18.
    const auto frq_region_reads =
            std::ranges::count_if(file.reads(), [&](const MemoryFile::Read& r) {
                return r.offset == prelude_abs && r.len >= entry.prelude_len &&
                       r.len <= entry.frq_len;
            });
    EXPECT_EQ(frq_region_reads, 1)
            << "docid path must fetch the frq region in a single round-trip at the prelude; "
            << "prelude_abs=" << prelude_abs << " frq_len=" << entry.frq_len
            << " prelude_len=" << entry.prelude_len;
}
