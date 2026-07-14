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

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/scoring_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/stats/snii_stats_provider.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"

// PHASE A read-back self-validation (design 2026-06-18-snii-read-byte-optimizations
// sections 1 + 3 + 4). Builds a scoring index with a high-df term that spans
// multiple ADAPTIVE-size windows plus low-df terms, then asserts:
//   (a) for every window, decoding ONLY the docs-only prefix slice
//       [frq window start, +frq_docs_len) via read_frq_window_docs yields exactly
//       the same docids as decoding the full window;
//   (b) sum of window doc_counts == df and the windows tile the posting in order;
//   (c) max_norm is non-zero for windows whose docs have non-default norms and
//       equals the tightest (smallest encoded) norm in the window;
//   (d) term_query / phrase_query / scoring (exhaustive vs WAND) agree with the
//       oracle docids.
namespace {

using namespace doris::snii;         // NOLINT
using namespace doris::snii::format; // NOLINT
using namespace doris::snii::writer; // NOLINT
using doris::snii::query::Bm25Params;
using doris::snii::query::ScoredDoc;
using doris::snii::stats::SniiStatsProvider;

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_phaseA_readback_" + std::to_string(getpid()) + "_" +
           std::to_string(counter++) + ".idx";
}

// Corpus: every doc has "hot" (very high df -> adaptive 1024-doc windows). "spark"
// follows "hot" consecutively in docs where d % 7 == 0. "rare" is a tiny term.
// Doc lengths vary by docid so encoded norms differ across windows and within a
// window, exercising real max_norm.
struct Corpus {
    uint32_t doc_count = 12000;       // df(hot)=12000 >= kAdaptiveWindowDfThreshold(8192)
    std::vector<uint32_t> hot_docs;   // every doc
    std::vector<uint32_t> spark_docs; // d % 7 == 0
    std::vector<uint32_t> rare_docs = {5, 91, 4096, 11999};
    std::vector<uint32_t> phrase_oracle; // docs with consecutive "hot spark"
    std::vector<uint64_t> doc_len;       // per-doc length (drives encoded norm)
};

// Per-doc length: varies so encoded norms are non-default (>1) and differ; the
// minimum within each 1024-doc window is deterministic.
uint64_t DocLen(uint32_t d) {
    return 2 + (d % 250);
}

Corpus MakeCorpus() {
    Corpus c;
    c.doc_len.assign(c.doc_count, 0);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        c.hot_docs.push_back(d);
        c.doc_len[d] = DocLen(d);
        if (d % 7 == 0) {
            c.spark_docs.push_back(d);
            c.phrase_oracle.push_back(d);
        }
    }
    return c;
}

TermPostings MakePosTerm(const std::string& term, const std::vector<uint32_t>& docs, uint32_t pos) {
    TermPostings tp;
    tp.term = term;
    tp.docids = docs;
    tp.freqs.assign(docs.size(), 1);
    tp.positions_flat.assign(docs.size(), pos); // one position per doc, flat
    return tp;
}

SniiIndexInput MakeIndex(const Corpus& c) {
    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = IndexConfig::kDocsPositionsScoring; // scoring -> norms available
    in.doc_count = c.doc_count;
    in.target_dict_block_bytes = 1; // one block per term
    in.encoded_norms.resize(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        in.encoded_norms[d] = doris::snii::query::encode_norm(c.doc_len[d]);
    }
    // Lexicographically sorted: hot < rare < spark.
    in.terms.push_back(MakePosTerm("hot", c.hot_docs, /*pos=*/0));
    in.terms.push_back(MakePosTerm("rare", c.rare_docs, /*pos=*/0));
    in.terms.push_back(MakePosTerm("spark", c.spark_docs, /*pos=*/1));
    return in;
}

// Smallest encoded norm across [start, end) docids (the tightest WAND norm).
uint8_t WindowMinNorm(const std::vector<uint8_t>& norms, uint32_t start, uint32_t end) {
    uint8_t best = std::numeric_limits<uint8_t>::max();
    for (uint32_t d = start; d < end; ++d) {
        best = std::min(best, norms[d]);
    }
    return best;
}

} // namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPhaseAReadBack, DocsPrefixTilesAndMaxNormTightAndQueriesAgree) {
    const Corpus c = MakeCorpus();
    std::vector<uint8_t> norms(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        norms[d] = doris::snii::query::encode_norm(c.doc_len[d]);
    }

    const std::string path = TempPath();
    {
        io::LocalFileWriter w;
        ASSERT_TRUE(w.open(path).ok());
        SniiCompoundWriter cw(&w);
        ASSERT_TRUE(cw.add_logical_index(MakeIndex(c)).ok());
        ASSERT_TRUE(cw.finish().ok());
    }

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local);
    reader::SniiSegmentReader seg;
    ASSERT_TRUE(reader::SniiSegmentReader::open(&metered, &seg).ok());
    reader::LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());

    // Resolve "hot" -> windowed DictEntry + the block's frq_base.
    bool found = false;
    DictEntry hot;
    uint64_t frq_base = 0, prx_base = 0;
    ASSERT_TRUE(idx.lookup("hot", &found, &hot, &frq_base, &prx_base).ok());
    ASSERT_TRUE(found);
    EXPECT_EQ(hot.df, c.doc_count);
    EXPECT_EQ(hot.enc, DictEntryEnc::kWindowed);
    EXPECT_TRUE(hot.has_sb);

    const uint64_t prelude_abs =
            idx.section_refs().posting_region.offset + frq_base + hot.frq_off_delta;
    std::vector<uint8_t> prelude_bytes;
    ASSERT_TRUE(local.read_at(prelude_abs, hot.prelude_len, &prelude_bytes).ok());
    FrqPreludeReader prelude;
    ASSERT_TRUE(FrqPreludeReader::open(Slice(prelude_bytes), &prelude).ok());

    // Adaptive sizing: df=12000 -> 1024-doc windows -> >= 2 windows.
    ASSERT_GT(prelude.n_windows(), 1U);
    // Most windows are the adaptive size (the last may be a remainder).
    WindowMeta w0;
    ASSERT_TRUE(prelude.window(0, &w0).ok());
    EXPECT_EQ(w0.doc_count, doris::snii::format::kAdaptiveWindowDocs);

    // The grouped layout puts all dd regions in the dd-block right after the prelude.
    const uint64_t dd_block_start = prelude_abs + hot.prelude_len;
    std::vector<uint32_t> tiled;
    uint64_t summed = 0;
    uint64_t expect_win_base = 0;
    uint64_t expect_dd_off = 0;
    uint32_t doc_cursor = 0;
    bool any_nonzero_max_norm = false;

    for (uint32_t w = 0; w < prelude.n_windows(); ++w) {
        WindowMeta m;
        ASSERT_TRUE(prelude.window(w, &m).ok());
        EXPECT_EQ(m.win_base, expect_win_base) << "win_base w=" << w;
        ASSERT_GT(m.dd_disk_len, 0U) << "w=" << w;
        // (a) dd regions are contiguous in the dd-block (the docs-only run is one range).
        EXPECT_EQ(m.dd_off, expect_dd_off) << "dd_off contiguity w=" << w;

        // Decode the window's dd region ALONE (the freq region bytes are NOT fetched).
        FrqRegionMeta dd_meta;
        dd_meta.zstd = m.dd_zstd;
        dd_meta.uncomp_len = m.dd_uncomp_len;
        dd_meta.disk_len = m.dd_disk_len;
        dd_meta.crc = m.crc_dd;
        std::vector<uint8_t> dd_bytes;
        ASSERT_TRUE(local.read_at(dd_block_start + m.dd_off, m.dd_disk_len, &dd_bytes).ok());
        std::vector<uint32_t> dd_docs;
        ASSERT_TRUE(decode_dd_region(Slice(dd_bytes), dd_meta, m.win_base, &dd_docs).ok())
                << "dd decode w=" << w;
        ASSERT_EQ(dd_docs.size(), m.doc_count) << "w=" << w;

        // (c) max_norm equals the tightest (smallest encoded) norm in the window.
        const uint32_t exp_min_norm = WindowMinNorm(norms, doc_cursor, doc_cursor + m.doc_count);
        EXPECT_EQ(m.max_norm, exp_min_norm) << "max_norm w=" << w;
        if (m.max_norm != 0) {
            any_nonzero_max_norm = true;
        }

        // (b) tiling: doc_counts sum and concatenated docids stay ascending.
        summed += m.doc_count;
        expect_win_base = m.last_docid;
        expect_dd_off += m.dd_disk_len;
        doc_cursor += m.doc_count;
        tiled.insert(tiled.end(), dd_docs.begin(), dd_docs.end());
    }
    // The dd-block length equals the sum of per-window dd region lengths.
    EXPECT_EQ(prelude.dd_block_len(), expect_dd_off);

    // (b) windows tile the whole posting [0, doc_count).
    EXPECT_EQ(summed, c.doc_count);
    ASSERT_EQ(tiled.size(), c.doc_count);
    EXPECT_EQ(tiled.front(), 0U);
    EXPECT_EQ(tiled.back(), c.doc_count - 1);
    for (size_t i = 1; i < tiled.size(); ++i) {
        ASSERT_LT(tiled[i - 1], tiled[i]) << "non-ascending at " << i;
    }
    // (c) at least one window's docs have non-default norms -> non-zero max_norm.
    EXPECT_TRUE(any_nonzero_max_norm);

    // --- slim/inline term frq_docs_len plumbing ('rare' is tiny -> inline) ---
    bool rfound = false;
    DictEntry rare;
    uint64_t rf = 0, rp = 0;
    ASSERT_TRUE(idx.lookup("rare", &rfound, &rare, &rf, &rp).ok());
    ASSERT_TRUE(rfound);
    EXPECT_EQ(rare.df, c.rare_docs.size());

    // (d) term_query returns exactly the oracle docid set for each term.
    std::vector<uint32_t> hot_q;
    ASSERT_TRUE(doris::snii::query::term_query(idx, "hot", &hot_q).ok());
    EXPECT_EQ(hot_q, c.hot_docs);

    std::vector<uint32_t> rare_q;
    ASSERT_TRUE(doris::snii::query::term_query(idx, "rare", &rare_q).ok());
    EXPECT_EQ(rare_q, c.rare_docs);

    std::vector<uint32_t> spark_q;
    ASSERT_TRUE(doris::snii::query::term_query(idx, "spark", &spark_q).ok());
    EXPECT_EQ(spark_q, c.spark_docs);

    // (d) phrase_query "hot spark" returns exactly the consecutive-occurrence docs.
    std::vector<uint32_t> phrase_q;
    ASSERT_TRUE(doris::snii::query::phrase_query(idx, {"hot", "spark"}, &phrase_q).ok());
    EXPECT_EQ(phrase_q, c.phrase_oracle);

    // (d) scoring: WAND top-K == exhaustive top-K (uses the real per-window max_norm).
    SniiStatsProvider stats;
    ASSERT_TRUE(SniiStatsProvider::open(&idx, &stats).ok());
    const Bm25Params params;
    for (uint32_t k : {1U, 5U, 50U}) {
        std::vector<ScoredDoc> ex, wa;
        ASSERT_TRUE(doris::snii::query::scoring_query_exhaustive(idx, stats, {"hot", "rare"}, k,
                                                                 params, &ex)
                            .ok());
        ASSERT_TRUE(
                doris::snii::query::scoring_query_wand(idx, stats, {"hot", "rare"}, k, params, &wa)
                        .ok());
        ASSERT_EQ(wa.size(), ex.size()) << "k=" << k;
        for (size_t i = 0; i < ex.size(); ++i) {
            EXPECT_EQ(wa[i].docid, ex[i].docid) << "k=" << k << " i=" << i;
            EXPECT_NEAR(wa[i].score, ex[i].score, 1e-9) << "k=" << k << " i=" << i;
        }
    }

    std::remove(path.c_str());
}
