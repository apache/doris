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

#include "storage/index/snii/format/frq_prelude.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <iterator>
#include <limits>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/crc32c.h"

using doris::snii::ByteSink;
using doris::snii::Slice;
using doris::snii::format::build_frq_prelude;
using doris::snii::format::FrqPreludeColumns;
using doris::snii::format::FrqPreludeReader;
using doris::snii::format::WindowMeta;
using doris::Status;

namespace {

// Builds N windows that tile a docid space in fixed strides, with deterministic
// per-window metadata. doc_count is `stride`; absolute last docid of window w is
// (w+1)*stride - 1 so windows are contiguous and strictly ascending. dd and freq
// region offsets are CONTIGUOUS prefix sums (the prelude validates this).
FrqPreludeColumns MakeColumns(uint32_t n, uint32_t group_size, uint32_t stride, bool has_prx) {
    FrqPreludeColumns cols;
    cols.has_freq = true;
    cols.has_prx = has_prx;
    cols.group_size = group_size;
    uint64_t dd_running = 0, freq_running = 0, prx_running = 0;
    for (uint32_t w = 0; w < n; ++w) {
        WindowMeta m;
        m.last_docid = (w + 1) * stride - 1;
        m.doc_count = stride;
        m.dd_zstd = (w % 2) == 0;
        m.dd_off = dd_running;
        m.dd_disk_len = 8 + w;
        // T18: a raw region's uncomp_len == disk_len (the row stores uncomp_len only
        // for zstd regions; the reader derives it otherwise). Keep the test columns
        // on that invariant so the round-trip's derived uncomp_len matches.
        m.dd_uncomp_len = m.dd_zstd ? m.dd_disk_len + 2 : m.dd_disk_len;
        m.crc_dd = 0xDD000000U + w;
        dd_running += m.dd_disk_len;
        m.freq_zstd = (w % 3) == 0;
        m.freq_off = freq_running;
        m.freq_disk_len = 5 + (w % 4);
        m.freq_uncomp_len = m.freq_zstd ? m.freq_disk_len + 1 : m.freq_disk_len;
        m.crc_freq = 0xEE000000U + w;
        freq_running += m.freq_disk_len;
        if (has_prx) {
            m.prx_off = prx_running;
            m.prx_len = 7 + (w % 5);
            prx_running += m.prx_len;
        }
        m.max_freq = w * 3 + 1;
        m.max_norm = static_cast<uint8_t>((w * 13 + 3) & 0xFF);
        cols.windows.push_back(m);
    }
    return cols;
}

ByteSink MakeSingleWindowPrelude(uint64_t last_docid_delta, uint64_t doc_count, uint64_t max_freq) {
    ByteSink block;
    block.put_varint64(last_docid_delta);
    block.put_varint64(doc_count);
    block.put_u8(0);          // win_mode: raw dd/freq (no zstd bit => no uncomp_len fields)
    block.put_varint64(4);    // dd_disk_len (dd_uncomp_len omitted: raw region)
    block.put_fixed32(0xDDU); // crc_dd
    block.put_varint64(0);    // freq_disk_len (freq_uncomp_len omitted: raw region)
    block.put_fixed32(0xEEU); // crc_freq
    block.put_varint64(max_freq);
    block.put_u8(0); // max_norm

    ByteSink dir;
    dir.put_varint64(last_docid_delta);
    dir.put_varint64(0); // first block starts at the window region
    dir.put_varint64(block.size());

    ByteSink covered;
    covered.put_u8(doris::snii::format::frq_prelude_flags::kHasFreq);
    covered.put_varint64(1); // N
    covered.put_varint64(1); // G
    covered.put_varint64(1); // n_super
    covered.put_varint64(dir.size());
    covered.put_bytes(dir.view());

    ByteSink frame;
    frame.put_bytes(covered.view());
    frame.put_fixed32(doris::snii::crc32c(covered.view()));
    frame.put_bytes(block.view());
    return frame;
}

// Round-trips columns through build + open, asserting every window's absolute
// fields match (and win_base chains correctly).
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
void ExpectRoundTrip(const FrqPreludeColumns& cols) {
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());

    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());

    ASSERT_EQ(reader.n_windows(), cols.windows.size());
    EXPECT_EQ(reader.has_freq(), cols.has_freq);
    EXPECT_EQ(reader.has_prx(), cols.has_prx);

    uint64_t expect_win_base = 0;
    for (uint32_t w = 0; w < reader.n_windows(); ++w) {
        WindowMeta got;
        ASSERT_TRUE(reader.window(w, &got).ok()) << "window w=" << w;
        const WindowMeta& exp = cols.windows[w];
        EXPECT_EQ(got.last_docid, exp.last_docid) << "ldd w=" << w;
        EXPECT_EQ(got.win_base, expect_win_base) << "win_base w=" << w;
        EXPECT_EQ(got.doc_count, exp.doc_count) << "doc_count w=" << w;
        EXPECT_EQ(got.dd_zstd, exp.dd_zstd) << "dd_zstd w=" << w;
        EXPECT_EQ(got.dd_off, exp.dd_off) << "dd_off w=" << w;
        EXPECT_EQ(got.dd_disk_len, exp.dd_disk_len) << "dd_disk_len w=" << w;
        EXPECT_EQ(got.dd_uncomp_len, exp.dd_uncomp_len) << "dd_uncomp_len w=" << w;
        EXPECT_EQ(got.crc_dd, exp.crc_dd) << "crc_dd w=" << w;
        EXPECT_EQ(got.freq_zstd, exp.freq_zstd) << "freq_zstd w=" << w;
        EXPECT_EQ(got.freq_off, exp.freq_off) << "freq_off w=" << w;
        EXPECT_EQ(got.freq_disk_len, exp.freq_disk_len) << "freq_disk_len w=" << w;
        EXPECT_EQ(got.freq_uncomp_len, exp.freq_uncomp_len) << "freq_uncomp_len w=" << w;
        EXPECT_EQ(got.crc_freq, exp.crc_freq) << "crc_freq w=" << w;
        EXPECT_EQ(got.max_freq, exp.max_freq) << "max_freq w=" << w;
        EXPECT_EQ(got.max_norm, exp.max_norm) << "max_norm w=" << w;
        if (cols.has_prx) {
            EXPECT_EQ(got.prx_off, exp.prx_off) << "prx_off w=" << w;
            EXPECT_EQ(got.prx_len, exp.prx_len) << "prx_len w=" << w;
        }
        expect_win_base = exp.last_docid;
    }
}

} // namespace

// Many windows across multiple super-blocks, no prx.
TEST(SniiFrqPrelude, RoundTripManyWindowsNoPrx) {
    ExpectRoundTrip(MakeColumns(/*n=*/20, /*group_size=*/4, /*stride=*/256, false));
}

// Many windows across multiple super-blocks, with prx.
TEST(SniiFrqPrelude, RoundTripManyWindowsWithPrx) {
    ExpectRoundTrip(MakeColumns(/*n=*/17, /*group_size=*/4, /*stride=*/256, true));
}

// Single window: N=1 collapses to a single super-block.
TEST(SniiFrqPrelude, SingleWindow) {
    FrqPreludeColumns cols = MakeColumns(/*n=*/1, /*group_size=*/64, /*stride=*/300, true);
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());
    EXPECT_EQ(reader.n_windows(), 1U);
    EXPECT_EQ(reader.n_super_blocks(), 1U);
    WindowMeta m;
    ASSERT_TRUE(reader.window(0, &m).ok());
    EXPECT_EQ(m.win_base, 0U);
    EXPECT_EQ(m.last_docid, 299U);
}

// N exactly == group_size -> exactly one super-block.
TEST(SniiFrqPrelude, ExactlyOneSuperBlock) {
    FrqPreludeColumns cols = MakeColumns(/*n=*/4, /*group_size=*/4, /*stride=*/256, true);
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());
    EXPECT_EQ(reader.n_super_blocks(), 1U);
}

// Large N exercises many super-blocks and varint streams.
TEST(SniiFrqPrelude, LargeN) {
    ExpectRoundTrip(MakeColumns(/*n=*/1000, /*group_size=*/64, /*stride=*/256, true));
}

// locate_window finds the covering window at boundaries, inside, and past-end.
TEST(SniiFrqPrelude, LocateWindowBoundaries) {
    // 10 windows, stride 100 -> window w covers docids [w*100, w*100+99].
    FrqPreludeColumns cols = MakeColumns(/*n=*/10, /*group_size=*/3, /*stride=*/100, true);
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());

    struct Case {
        uint32_t docid;
        bool found;
        uint32_t w;
    };
    const Case cases[] = {
            {.docid = 0, .found = true, .w = 0},   // first docid of window 0
            {.docid = 99, .found = true, .w = 0},  // last docid of window 0
            {.docid = 100, .found = true, .w = 1}, // first docid of window 1 (boundary)
            {.docid = 250, .found = true, .w = 2}, // inside window 2
            {.docid = 299, .found = true, .w = 2}, // last docid of window 2
            {.docid = 300, .found = true, .w = 3}, // boundary into window 3
            {.docid = 999, .found = true, .w = 9}, // last docid overall (window 9 covers [900,999])
            {.docid = 1000, .found = false, .w = 0}, // past the term's last docid
            {.docid = 5000, .found = false, .w = 0}, // far past end
    };
    for (const auto& c : cases) {
        bool found = false;
        uint32_t w = 999;
        ASSERT_TRUE(reader.locate_window(c.docid, &found, &w).ok()) << "docid=" << c.docid;
        EXPECT_EQ(found, c.found) << "docid=" << c.docid;
        if (c.found) {
            EXPECT_EQ(w, c.w) << "docid=" << c.docid;
        }
    }
}

// locate_window must agree with a linear scan over windows for every docid in a
// gappy docid layout (windows not contiguous: gaps between windows).
TEST(SniiFrqPrelude, LocateWindowAgainstLinearScan) {
    FrqPreludeColumns cols;
    cols.has_prx = true;
    cols.group_size = 4;
    // Non-contiguous absolute last docids with gaps: 50, 130, 131, 400, 900.
    const uint32_t lasts[] = {50, 130, 131, 400, 900};
    uint64_t dd_running = 0, freq_running = 0, prx_running = 0;
    for (uint32_t v : lasts) {
        WindowMeta m;
        m.last_docid = v;
        m.doc_count = 1;
        m.dd_off = dd_running;
        m.dd_disk_len = 12;
        m.dd_uncomp_len = 12;
        m.crc_dd = v;
        dd_running += 12;
        m.freq_off = freq_running;
        m.freq_disk_len = 6;
        m.freq_uncomp_len = 6;
        m.crc_freq = v + 1;
        freq_running += 6;
        m.prx_off = prx_running;
        m.prx_len = 6;
        prx_running += 6;
        cols.windows.push_back(m);
    }
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());

    for (uint32_t docid = 0; docid <= 950; ++docid) {
        // Linear oracle: first window whose absolute last_docid >= docid.
        bool exp_found = false;
        uint32_t exp_w = 0;
        for (uint32_t w = 0; w < std::size(lasts); ++w) {
            if (docid <= lasts[w]) {
                exp_found = true;
                exp_w = w;
                break;
            }
        }
        bool found = false;
        uint32_t w = 999;
        ASSERT_TRUE(reader.locate_window(docid, &found, &w).ok());
        EXPECT_EQ(found, exp_found) << "docid=" << docid;
        if (exp_found) {
            EXPECT_EQ(w, exp_w) << "docid=" << docid;
        }
    }
}

// Builder rejects a null sink.
TEST(SniiFrqPrelude, BuildNullSinkRejected) {
    FrqPreludeColumns cols = MakeColumns(3, 4, 100, false);
    EXPECT_TRUE(build_frq_prelude(cols, nullptr).is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// Builder rejects group_size == 0.
TEST(SniiFrqPrelude, BuildZeroGroupSizeRejected) {
    FrqPreludeColumns cols = MakeColumns(3, 4, 100, false);
    cols.group_size = 0;
    ByteSink sink;
    EXPECT_TRUE(build_frq_prelude(cols, &sink).is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// Builder rejects non-monotonic last_docid ordering across windows.
TEST(SniiFrqPrelude, BuildNonMonotonicRejected) {
    FrqPreludeColumns cols = MakeColumns(3, 4, 100, false);
    cols.windows[2].last_docid = cols.windows[1].last_docid - 1; // goes backwards
    ByteSink sink;
    EXPECT_TRUE(build_frq_prelude(cols, &sink).is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// T18: dd_off is DERIVED (running prefix sum), not serialized. Tampering a
// column's dd_off no longer changes the on-disk bytes, and the reader
// reconstructs the contiguous offset regardless -- so the offset is always
// contiguous by construction (pre-T18 this stored + cross-checked an explicit
// dd_off, and a gap was rejected on open).
TEST(SniiFrqPrelude, DdOffsetIsDerivedNotStored) {
    FrqPreludeColumns cols = MakeColumns(/*n=*/5, /*group_size=*/4, /*stride=*/256, false);
    const uint64_t expect_dd_off_2 =
            cols.windows[0].dd_disk_len + cols.windows[1].dd_disk_len; // running sum
    cols.windows[2].dd_off += 100; // in-memory tamper has no on-disk effect now
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());
    WindowMeta m;
    ASSERT_TRUE(reader.window(2, &m).ok());
    EXPECT_EQ(m.dd_off, expect_dd_off_2); // derived, not the tampered column value
}

TEST(SniiFrqPrelude, RejectsDocCountBeyondWindowWidth) {
    FrqPreludeColumns cols = MakeColumns(/*n=*/1, /*group_size=*/1,
                                         /*stride=*/10, false);
    cols.windows[0].doc_count = 11;
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    FrqPreludeReader reader;
    Status s = FrqPreludeReader::open(sink.view(), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiFrqPrelude, RejectsWindowDocCountThatDoesNotFitU32) {
    ByteSink sink = MakeSingleWindowPrelude(
            /*last_docid_delta=*/0, static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1,
            /*max_freq=*/1);
    FrqPreludeReader reader;
    Status s = FrqPreludeReader::open(sink.view(), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

TEST(SniiFrqPrelude, RejectsWindowMaxFreqThatDoesNotFitU32) {
    ByteSink sink = MakeSingleWindowPrelude(
            /*last_docid_delta=*/0, /*doc_count=*/1,
            static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1);
    FrqPreludeReader reader;
    Status s = FrqPreludeReader::open(sink.view(), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// The reader reports dd_block_len / freq_block_len as the sums of per-window region
// disk lengths (the contiguous block sizes a reader range-fetches).
TEST(SniiFrqPrelude, BlockLengthsAreRegionSums) {
    FrqPreludeColumns cols = MakeColumns(/*n=*/6, /*group_size=*/4, /*stride=*/256, true);
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());
    uint64_t dd_sum = 0, freq_sum = 0;
    for (const auto& m : cols.windows) {
        dd_sum += m.dd_disk_len;
        freq_sum += m.freq_disk_len;
    }
    EXPECT_EQ(reader.dd_block_len(), dd_sum);
    EXPECT_EQ(reader.freq_block_len(), freq_sum);
}

// CRC corruption inside the header / super_block_dir is detected by open().
TEST(SniiFrqPrelude, CrcCorruptionDetected) {
    FrqPreludeColumns cols = MakeColumns(8, 4, 256, true);
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    std::vector<uint8_t> bytes = sink.buffer();
    ASSERT_GE(bytes.size(), 4U);
    bytes[1] ^= 0xFF; // flip a flags/header byte
    FrqPreludeReader reader;
    Status s = FrqPreludeReader::open(Slice(bytes), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}

// Truncated buffer (chopping into a window block) is rejected.
TEST(SniiFrqPrelude, TruncatedRejected) {
    FrqPreludeColumns cols = MakeColumns(8, 4, 256, true);
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    std::vector<uint8_t> bytes = sink.buffer();
    bytes.resize(bytes.size() - 5); // drop tail bytes of the last window block
    FrqPreludeReader reader;
    Status s = FrqPreludeReader::open(Slice(bytes), &reader);
    EXPECT_FALSE(s.ok());
}

// Out-of-range window() returns InvalidArgument (no OOB read).
TEST(SniiFrqPrelude, WindowOutOfRangeRejected) {
    FrqPreludeColumns cols = MakeColumns(3, 4, 100, true);
    ByteSink sink;
    ASSERT_TRUE(build_frq_prelude(cols, &sink).ok());
    FrqPreludeReader reader;
    ASSERT_TRUE(FrqPreludeReader::open(sink.view(), &reader).ok());
    WindowMeta m;
    EXPECT_TRUE(reader.window(99, &m).is<doris::ErrorCode::INVALID_ARGUMENT>());
}

// Anti-DoS: a crc-valid header declaring an absurd window count N must be
// rejected before any reserve(N). We craft header + super_block_dir bytes by
// hand with a matching crc, so verify_crc passes but the count is rejected.
TEST(SniiFrqPrelude, RejectsOversizedWindowCount) {
    ByteSink covered;
    covered.put_u8(doris::snii::format::frq_prelude_flags::kHasFreq);
    covered.put_varint64(0xFFFFFFFFULL); // N: absurd window count
    covered.put_varint64(64);            // G
    covered.put_varint64(1);             // n_super (bogus but small)
    covered.put_varint64(0);             // sbdir_len = 0
    ByteSink frame;
    frame.put_bytes(covered.view());
    frame.put_fixed32(doris::snii::crc32c(covered.view()));
    FrqPreludeReader reader;
    Status s = FrqPreludeReader::open(frame.view(), &reader);
    EXPECT_TRUE(s.is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
}
