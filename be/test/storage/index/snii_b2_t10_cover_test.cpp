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

// T10 -- select_covering_windows as a monotonic two-pointer cursor (O(C + N)).
// Covers: (a) the isolated cursor core select_covering_windows_cursor and the
// FrqPreludeReader::select_covering_windows member produce results element-for-element
// identical to the legacy per-candidate locate_window + run-collapse oracle; (b) the
// window_probe_count() seam shows the cursor is O(C + N) (not O(C * N)) and bounded by
// C + N regardless of group_size, while the legacy in-block rescan grows with G; (c) the
// packed win_last_docid_ catalogue is byte-identical to WindowMeta.last_docid; and (d) the
// wired phrase query path is unchanged.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <random>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii {
namespace {

using namespace snii_test; // assert_ok, build_reader, MemoryFile, ...

using format::build_frq_prelude;
using format::FrqPreludeColumns;
using format::FrqPreludeReader;
using format::select_covering_windows_cursor;
using format::WindowMeta;

// Trusted oracle: the legacy locate_window-per-candidate + run-collapse semantics,
// computed with a dumb per-candidate linear scan (first window whose last_docid >= d).
std::vector<uint32_t> oracle_select(const std::vector<uint32_t>& win_last_docid,
                                    const std::vector<uint32_t>& candidates) {
    std::vector<uint32_t> out;
    uint32_t last = UINT32_MAX;
    for (uint32_t d : candidates) {
        bool found = false;
        uint32_t hit = 0;
        for (uint32_t w = 0; w < win_last_docid.size(); ++w) {
            if (d <= win_last_docid[w]) {
                found = true;
                hit = w;
                break;
            }
        }
        if (!found) continue;
        if (hit != last) {
            out.push_back(hit);
            last = hit;
        }
    }
    return out;
}

// Derives sb_last_docid exactly as FrqPreludeReader::open does: the absolute last docid
// of each super-block's last window.
std::vector<uint64_t> derive_sb_last_docid(const std::vector<uint32_t>& win_last_docid,
                                           uint32_t group_size) {
    std::vector<uint64_t> sb;
    const uint32_t n = static_cast<uint32_t>(win_last_docid.size());
    if (n == 0) return sb;
    const uint32_t n_super = (n + group_size - 1) / group_size;
    sb.reserve(n_super);
    for (uint32_t s = 0; s < n_super; ++s) {
        const uint32_t last_win = std::min((s + 1) * group_size, n) - 1;
        sb.push_back(win_last_docid[last_win]);
    }
    return sb;
}

// Runs the pure cursor core over directory arrays derived from win_last_docid.
std::vector<uint32_t> run_cursor(const std::vector<uint32_t>& win_last_docid, uint32_t group_size,
                                 const std::vector<uint32_t>& candidates) {
    const std::vector<uint64_t> sb = derive_sb_last_docid(win_last_docid, group_size);
    std::vector<uint32_t> out;
    select_covering_windows_cursor(win_last_docid.data(),
                                   static_cast<uint32_t>(win_last_docid.size()), sb.data(),
                                   static_cast<uint32_t>(sb.size()), group_size, candidates, &out);
    return out;
}

// Builds a real prelude from strictly-increasing absolute last docids (doc_count=1 and
// contiguous 1-byte dd/freq regions satisfy the prelude width / layout validators).
void make_test_prelude(const std::vector<uint32_t>& last_docids, uint32_t group_size,
                       FrqPreludeReader* reader) {
    FrqPreludeColumns cols;
    cols.has_freq = true;
    cols.has_prx = false;
    cols.group_size = group_size;
    uint64_t dd_running = 0;
    uint64_t freq_running = 0;
    for (uint32_t v : last_docids) {
        WindowMeta m;
        m.last_docid = v;
        m.doc_count = 1;
        m.dd_off = dd_running;
        m.dd_disk_len = 1;
        m.dd_uncomp_len = 1;
        m.crc_dd = v;
        dd_running += 1;
        m.freq_off = freq_running;
        m.freq_disk_len = 1;
        m.freq_uncomp_len = 1;
        m.crc_freq = v + 1;
        freq_running += 1;
        cols.windows.push_back(m);
    }
    ByteSink sink;
    assert_ok(build_frq_prelude(cols, &sink));
    assert_ok(FrqPreludeReader::open(sink.view(), reader));
}

// Production oracle: per-candidate locate_window + run-collapse over a real prelude.
std::vector<uint32_t> locate_window_select(const FrqPreludeReader& prelude,
                                           const std::vector<uint32_t>& candidates) {
    std::vector<uint32_t> out;
    uint32_t last = UINT32_MAX;
    for (uint32_t d : candidates) {
        bool found = false;
        uint32_t w = 0;
        assert_ok(prelude.locate_window(d, &found, &w));
        if (!found) continue;
        if (w != last) {
            out.push_back(w);
            last = w;
        }
    }
    return out;
}

// FV-01: randomized equivalence of the cursor with the oracle across N and group_size.
TEST(SniiB2T10CoverTest, CursorMatchesOracleOnRandomAscendingSets) {
    std::mt19937 rng(0x7710u);
    const uint32_t kNs[] = {1, 2, 8, 64, 200, 4000};
    const uint32_t kGs[] = {1, 8, 64};
    std::uniform_int_distribution<uint32_t> step(1, 5); // strictly increasing last_docid
    std::uniform_int_distribution<uint32_t> keep(0, 3); // ~1/4 of docids become candidates
    for (uint32_t n : kNs) {
        std::vector<uint32_t> win_last(n);
        uint32_t acc = 0;
        for (uint32_t w = 0; w < n; ++w) {
            acc += step(rng);
            win_last[w] = acc;
        }
        const uint32_t max_docid = acc + 3; // a few candidates land past the last window
        std::vector<uint32_t> cands;
        for (uint32_t d = 0; d <= max_docid; ++d) {
            if (keep(rng) == 0) cands.push_back(d); // ascending, distinct, some same-window runs
        }
        const std::vector<uint32_t> expected = oracle_select(win_last, cands);
        for (uint32_t g : kGs) {
            EXPECT_EQ(run_cursor(win_last, g, cands), expected) << "n=" << n << " g=" << g;
        }
    }
}

// FV-02: the reader member (real prelude, production path) agrees with per-candidate
// locate_window over the same prelude.
TEST(SniiB2T10CoverTest, MemberMatchesRealLocateWindow) {
    std::vector<uint32_t> last_docids;
    uint32_t acc = 0;
    for (uint32_t w = 0; w < 300; ++w) {
        acc += 1 + (w % 7);
        last_docids.push_back(acc);
    }
    FrqPreludeReader prelude;
    make_test_prelude(last_docids, /*group_size=*/64, &prelude);

    std::vector<uint32_t> cands;
    for (uint32_t d = 0; d <= acc + 5; d += 3) cands.push_back(d);

    std::vector<uint32_t> got;
    prelude.select_covering_windows(cands, &got);
    EXPECT_EQ(got, locate_window_select(prelude, cands));
    EXPECT_EQ(got, oracle_select(last_docids, cands));
}

// FV-03: single window -- candidates inside emit {0}, candidates past it emit {}.
TEST(SniiB2T10CoverTest, SingleWindow) {
    const std::vector<uint32_t> win_last = {100};
    EXPECT_EQ(run_cursor(win_last, 64, {0, 50, 100}), (std::vector<uint32_t> {0}));
    EXPECT_EQ(run_cursor(win_last, 64, {0, 50, 100}), oracle_select(win_last, {0, 50, 100}));
    EXPECT_TRUE(run_cursor(win_last, 64, {101, 200}).empty());
}

// FV-04: a candidate at docid 0 / at the first window's last docid still emits window 0.
TEST(SniiB2T10CoverTest, CandidateInFirstWindow) {
    const std::vector<uint32_t> win_last = {10, 20, 30};
    EXPECT_EQ(run_cursor(win_last, 2, {0, 10}), (std::vector<uint32_t> {0}));
    EXPECT_EQ(run_cursor(win_last, 2, {0, 10}), oracle_select(win_last, {0, 10}));
}

// FV-05: candidates past the last window are dropped (early break == locate_window miss).
TEST(SniiB2T10CoverTest, CandidatesPastLastWindowAreDropped) {
    const std::vector<uint32_t> win_last = {10, 20, 30};
    const std::vector<uint32_t> cands = {5, 25, 31, 40, 100}; // 5->w0, 25->w2, rest miss
    EXPECT_EQ(run_cursor(win_last, 2, cands), (std::vector<uint32_t> {0, 2}));
    EXPECT_EQ(run_cursor(win_last, 2, cands), oracle_select(win_last, cands));
}

// FV-06: empty candidate list -> empty result (output is cleared first).
TEST(SniiB2T10CoverTest, EmptyCandidates) {
    const std::vector<uint32_t> win_last = {10, 20};
    const std::vector<uint64_t> sb = derive_sb_last_docid(win_last, 64);
    std::vector<uint32_t> out = {99, 98, 97}; // pre-filled: must be cleared
    select_covering_windows_cursor(win_last.data(), 2, sb.data(), static_cast<uint32_t>(sb.size()),
                                   64, {}, &out);
    EXPECT_TRUE(out.empty());
}

// FV-07: zero windows -> empty result, no crash (mirrors locate_window's empty early-out).
TEST(SniiB2T10CoverTest, EmptyWindows) {
    const std::vector<uint32_t> win_last;
    EXPECT_TRUE(run_cursor(win_last, 64, {0, 5, 100}).empty());
}

// FV-08: sparse candidates crossing several super-block boundaries; cursor, member, and
// real locate_window all agree (boundary jump correctness).
TEST(SniiB2T10CoverTest, SuperBlockBoundaryCrossingSparse) {
    std::vector<uint32_t> last_docids; // 10 windows, stride 100 -> window w covers [w*100, ..+99]
    for (uint32_t w = 0; w < 10; ++w) last_docids.push_back((w + 1) * 100 - 1);
    FrqPreludeReader prelude;
    make_test_prelude(last_docids, /*group_size=*/4, &prelude); // 3 super-blocks
    const std::vector<uint32_t> cands = {50, 450, 850, 999};    // windows 0, 4, 8, 9
    std::vector<uint32_t> got;
    prelude.select_covering_windows(cands, &got);
    EXPECT_EQ(got, (std::vector<uint32_t> {0, 4, 8, 9}));
    EXPECT_EQ(got, locate_window_select(prelude, cands));
    EXPECT_EQ(run_cursor(last_docids, 4, cands), got);
}

// FV-09: every window is covered -> the full {0..N-1} set, de-duplicated.
TEST(SniiB2T10CoverTest, DenseEveryWindowCovered) {
    std::vector<uint32_t> win_last;
    std::vector<uint32_t> cands;
    std::vector<uint32_t> expected;
    for (uint32_t w = 0; w < 16; ++w) {
        win_last.push_back(w); // window w covers exactly docid w
        cands.push_back(w);
        expected.push_back(w);
    }
    EXPECT_EQ(run_cursor(win_last, 4, cands), expected);
    EXPECT_EQ(run_cursor(win_last, 4, cands), oracle_select(win_last, cands));
}

// FV-10: the packed win_last_docid_ catalogue is byte-identical to WindowMeta.last_docid.
TEST(SniiB2T10CoverTest, PackedLastDocidMatchesWindowMeta) {
    std::vector<uint32_t> last_docids;
    uint32_t acc = 0;
    for (uint32_t w = 0; w < 130; ++w) { // spans >1 super-block at G=64
        acc += 1 + (w % 3);
        last_docids.push_back(acc);
    }
    FrqPreludeReader prelude;
    make_test_prelude(last_docids, /*group_size=*/64, &prelude);
    ASSERT_EQ(prelude.n_windows(), 130u);
    for (uint32_t w = 0; w < prelude.n_windows(); ++w) {
        WindowMeta m;
        assert_ok(prelude.window(w, &m));
        EXPECT_EQ(prelude.window_last_docid(w), m.last_docid) << "w=" << w;
    }
}

// Deterministic complexity invariant: probe_count <= C + N for both dense and sparse
// candidate sets (i.e. O(C + N), far below O(C * N)); the sparse case additionally stays
// below N, proving the super-block jump avoids a full linear scan.
TEST(SniiB2T10CoverTest, CursorProbeCountStaysLinearInCandidatesPlusWindows) {
    {
        const uint32_t n = 256; // dense: C == N * stride
        std::vector<uint32_t> win_last(n);
        for (uint32_t w = 0; w < n; ++w) win_last[w] = w * 4 + 3;
        std::vector<uint32_t> cands;
        for (uint32_t d = 0; d < n * 4; ++d) cands.push_back(d);
        format::testing::reset_window_probe_count();
        const std::vector<uint32_t> got = run_cursor(win_last, 64, cands);
        const uint64_t probes = format::testing::window_probe_count();
        EXPECT_LE(probes, static_cast<uint64_t>(cands.size()) + n);
        EXPECT_EQ(got, oracle_select(win_last, cands));
    }
    {
        const uint32_t n = 4000; // sparse: C << N
        std::vector<uint32_t> win_last(n);
        for (uint32_t w = 0; w < n; ++w) win_last[w] = w;
        const std::vector<uint32_t> cands = {0, 1000, 2000, 3000, 3999};
        format::testing::reset_window_probe_count();
        const std::vector<uint32_t> got = run_cursor(win_last, 64, cands);
        const uint64_t probes = format::testing::window_probe_count();
        EXPECT_LE(probes, static_cast<uint64_t>(cands.size()) + n);
        EXPECT_LT(probes, n); // super-block jump prevents an O(N) linear walk
        EXPECT_EQ(got, oracle_select(win_last, cands));
    }
}

// Deterministic before/after contrast: the legacy per-candidate locate_window probe count
// grows with group_size and exceeds C + N, whereas the cursor stays <= C + N for every G
// and yields the same covering-window set regardless of G.
TEST(SniiB2T10CoverTest, LegacyProbeGrowsWithGroupSizeButCursorStaysLinear) {
    const uint32_t n = 200;
    std::vector<uint32_t> last_docids(n);
    std::vector<uint32_t> cands(n);
    for (uint32_t w = 0; w < n; ++w) {
        last_docids[w] = w; // window w covers exactly docid w
        cands[w] = w;       // one candidate per window
    }
    const uint64_t cn = static_cast<uint64_t>(cands.size()) + n; // C + N

    FrqPreludeReader prelude_g8;
    FrqPreludeReader prelude_g64;
    make_test_prelude(last_docids, 8, &prelude_g8);
    make_test_prelude(last_docids, 64, &prelude_g64);

    format::testing::reset_window_probe_count();
    (void)locate_window_select(prelude_g8, cands);
    const uint64_t legacy_g8 = format::testing::window_probe_count();

    format::testing::reset_window_probe_count();
    (void)locate_window_select(prelude_g64, cands);
    const uint64_t legacy_g64 = format::testing::window_probe_count();

    EXPECT_GT(legacy_g64, legacy_g8); // deeper in-block rescans with larger G
    EXPECT_GT(legacy_g8, cn);         // even G=8 already exceeds C + N
    EXPECT_GT(legacy_g64, cn);

    format::testing::reset_window_probe_count();
    const std::vector<uint32_t> cur_g8 = run_cursor(last_docids, 8, cands);
    const uint64_t cursor_g8 = format::testing::window_probe_count();

    format::testing::reset_window_probe_count();
    const std::vector<uint32_t> cur_g64 = run_cursor(last_docids, 64, cands);
    const uint64_t cursor_g64 = format::testing::window_probe_count();

    EXPECT_LE(cursor_g8, cn); // bounded by C + N regardless of G
    EXPECT_LE(cursor_g64, cn);
    EXPECT_LT(cursor_g64, legacy_g64); // cursor crushes the legacy probe count
    EXPECT_EQ(cur_g8, cur_g64);        // identical covering-window set across G
    EXPECT_EQ(cur_g8, oracle_select(last_docids, cands));
}

// FV-11: the wired phrase query result is unchanged (no regression through the windowed
// docid-conjunction path that now uses the cursor member).
TEST(SniiB2T10CoverTest, WiredPhraseQueryResultUnchanged) {
    MemoryFile file;
    reader::SniiSegmentReader segment_reader;
    reader::LogicalIndexReader index_reader;
    assert_ok(build_reader(&file, &segment_reader, &index_reader));

    std::vector<uint32_t> docids;
    assert_ok(query::phrase_query(index_reader, {"failed", "order"}, &docids));
    EXPECT_EQ(docids, (std::vector<uint32_t> {5000, 7000, 8000}));
}

} // namespace
} // namespace doris::snii
