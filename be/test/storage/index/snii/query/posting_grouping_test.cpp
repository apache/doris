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
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/scoring_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/reader/windowed_posting.h"
#include "storage/index/snii/stats/snii_stats_provider.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

// PHASE D differential + contiguity test (design 1.6: posting-level dd/freq
// grouping). The windowed .frq payload is laid out
// [prelude][dd-block][freq-block] so a docid-only / phrase reader fetches the
// docs-only data ([prelude][dd-block]) as ONE CONTIGUOUS run. Over a ~5000-doc
// kDocsPositionsScoring index with a high-df term spanning MANY adaptive
// windows plus mid/low terms and a planted 5-term phrase, this asserts:
//   (a) term_query + phrase_query (incl the 5-term phrase) docids == a
//   brute-force
//       ORACLE; scoring top-K (exhaustive == wand == selective) unchanged.
//   (b) Through a MeteredFileReader the docid-only posting reader for the
//   high-df
//       term reads the dd-block as a CONTIGUOUS region: read_at is SMALL (a
//       couple of ranges, NOT one-per-window / not thousands), range_gets is
//       small, AND request_bytes is strictly LESS than fetching the full
//       posting (the freq-block is skipped). All three (read_at, range_gets,
//       request_bytes) drop vs the full-posting read, with identical docids.
using namespace doris::snii;
using namespace doris::snii::format;
using namespace doris::snii::reader;
using namespace doris::snii::writer;
using doris::snii::query::Bm25Params;
using doris::snii::query::ScoredDoc;
using doris::snii::stats::SniiStatsProvider;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_posting_grouping_" + std::to_string(getpid()) + "_" +
           std::to_string(counter++) + ".idx";
}

constexpr uint32_t kDocCount = 5000;
constexpr uint32_t kHiCount = 4800;   // df of aa_hi (>> 256 -> many windows)
constexpr uint32_t kPhraseSpan = 150; // 5-term phrase planted in early aa_hi docs

// aa_hi appears with varied, large frequency so its per-window freq region is
// big in absolute terms (the freq-block the docs-only path never fetches).
uint32_t HiFreq(uint32_t occ) {
    return 50U + (occ * 17U + 11U) % 240U;
}

struct Corpus {
    uint32_t doc_count = 0;
    std::vector<std::vector<std::string>> docs;
    std::vector<uint64_t> doc_len;

    bool phrase_in_doc(uint32_t d, const std::vector<std::string>& phrase) const {
        if (phrase.empty()) {
            return false;
        }
        const auto& toks = docs[d];
        if (toks.size() < phrase.size()) {
            return false;
        }
        for (size_t i = 0; i + phrase.size() <= toks.size(); ++i) {
            bool match = true;
            for (size_t k = 0; k < phrase.size(); ++k) {
                if (toks[i + k] != phrase[k]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return true;
            }
        }
        return false;
    }

    std::vector<uint32_t> term_oracle(const std::string& term) const {
        std::vector<uint32_t> out;
        for (uint32_t d = 0; d < doc_count; ++d) {
            const auto& toks = docs[d];
            if (std::ranges::find(toks, term) != toks.end()) {
                out.push_back(d);
            }
        }
        return out;
    }

    std::vector<uint32_t> phrase_oracle(const std::vector<std::string>& phrase) const {
        std::vector<uint32_t> out;
        for (uint32_t d = 0; d < doc_count; ++d) {
            if (phrase_in_doc(d, phrase)) {
                out.push_back(d);
            }
        }
        return out;
    }
};

Corpus BuildCorpus() {
    Corpus c;
    c.doc_count = kDocCount;
    c.docs.resize(c.doc_count);
    c.doc_len.assign(c.doc_count, 0);
    const std::vector<std::string> vocab = {"alpha", "bravo", "charlie", "delta"};

    for (uint32_t d = 0; d < c.doc_count; ++d) {
        std::vector<std::string>& toks = c.docs[d];
        const bool is_hi = d < kHiCount;
        if (is_hi) {
            toks.emplace_back("aa_hi"); // high-df, position 0
        }
        const uint32_t occ = d;

        if (is_hi && occ < kPhraseSpan) {
            toks.emplace_back("aa_quick");
            toks.emplace_back("aa_brown");
            toks.emplace_back("aa_fox");
            toks.emplace_back("aa_jumps");
        } else if (d % 13 == 0) {
            toks.emplace_back("aa_lazy");
            toks.emplace_back("aa_dog");
        }
        if (d % 5 == 0) {
            toks.emplace_back("aa_mid");
            if (d % 15 == 0) {
                toks.emplace_back("aa_mid");
            }
        }
        for (uint32_t k = 0; k < 2; ++k) {
            toks.push_back(vocab[(d + k) % vocab.size()]);
        }
        if (d % 101 == 0) {
            toks.emplace_back("aa_rare");
        }
        char nm[16];
        std::snprintf(nm, sizeof(nm), "zz_%03u", d % 400);
        toks.emplace_back(nm);
        if (is_hi) {
            const uint32_t extra = HiFreq(occ) - 1U; // total freq = HiFreq(occ)
            for (uint32_t k = 0; k < extra; ++k) {
                toks.emplace_back("aa_hi");
            }
        }
        c.doc_len[d] = toks.size();
    }
    return c;
}

void WriteCorpus(const Corpus& c, const std::string& path) {
    SpimiTermBuffer buf(/*has_positions=*/true);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        const auto& toks = c.docs[d];
        for (uint32_t pos = 0; pos < toks.size(); ++pos) {
            buf.add_token(toks[pos], d, pos);
        }
    }
    std::vector<TermPostings> terms = buf.finalize_sorted();

    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = IndexConfig::kDocsPositionsScoring;
    in.doc_count = c.doc_count;
    in.terms = std::move(terms);
    in.target_dict_block_bytes = 256;
    in.encoded_norms.resize(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        in.encoded_norms[d] = doris::snii::query::encode_norm(c.doc_len[d]);
    }

    io::LocalFileWriter w;
    ASSERT_TRUE(w.open(path).ok());
    SniiCompoundWriter cw(&w);
    ASSERT_TRUE(cw.add_logical_index(in).ok());
    ASSERT_TRUE(cw.finish().ok());
}

// dd-block / freq-block byte sizes of a windowed term (the contiguous runs).
struct Blocks {
    uint64_t prelude_len = 0;
    uint64_t dd_block = 0;
    uint64_t freq_block = 0;
    uint32_t windows = 0;
};

Blocks MeasureBlocks(const LogicalIndexReader& idx, const std::string& term) {
    Blocks b;
    DictEntry entry;
    uint64_t frq_base = 0, prx_base = 0;
    bool found = false;
    EXPECT_TRUE(idx.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    EXPECT_TRUE(found);
    EXPECT_EQ(entry.enc, DictEntryEnc::kWindowed);
    b.prelude_len = entry.prelude_len;
    FrqPreludeReader prelude;
    EXPECT_TRUE(fetch_windowed_prelude(idx, entry, frq_base, &prelude).ok());
    b.windows = prelude.n_windows();
    b.dd_block = prelude.dd_block_len();
    b.freq_block = prelude.freq_block_len();
    return b;
}

std::vector<ScoredDoc> ReferenceRanking(const Corpus& c, const std::vector<std::string>& terms,
                                        uint32_t k, const Bm25Params& params) {
    uint64_t sum_ttf = 0;
    for (uint64_t dl : c.doc_len) {
        sum_ttf += dl;
    }
    const double avgdl = static_cast<double>(sum_ttf) / std::max<uint64_t>(1, c.doc_count);

    std::unordered_map<uint32_t, double> scores;
    for (const auto& term : terms) {
        std::map<uint32_t, uint32_t> plist;
        for (uint32_t d = 0; d < c.doc_count; ++d) {
            uint32_t f = 0;
            for (const auto& t : c.docs[d]) {
                if (t == term) {
                    ++f;
                }
            }
            if (f > 0) {
                plist[d] = f;
            }
        }
        if (plist.empty()) {
            continue;
        }
        const uint64_t df = plist.size();
        const double idf =
                std::log(1.0 + (static_cast<double>(c.doc_count) - df + 0.5) / (df + 0.5));
        for (const auto& [docid, freq] : plist) {
            const double dl = doris::snii::query::decode_norm(
                    doris::snii::query::encode_norm(c.doc_len[docid]));
            const double denom = freq + params.k1 * (1.0 - params.b + params.b * dl / avgdl);
            scores[docid] += idf * (freq * (params.k1 + 1.0)) / denom;
        }
    }

    std::vector<ScoredDoc> all;
    all.reserve(scores.size());
    for (const auto& [docid, s] : scores) {
        all.push_back({docid, s});
    }
    std::ranges::sort(all, [](const ScoredDoc& a, const ScoredDoc& b) {
        if (a.score != b.score) {
            return a.score > b.score;
        }
        return a.docid < b.docid;
    });
    if (all.size() > k) {
        all.resize(k);
    }
    return all;
}

void ExpectRankingEqual(const std::vector<ScoredDoc>& got, const std::vector<ScoredDoc>& ref,
                        const char* label) {
    ASSERT_EQ(got.size(), ref.size()) << label;
    for (size_t i = 0; i < ref.size(); ++i) {
        EXPECT_EQ(got[i].docid, ref[i].docid) << label << " docid i=" << i;
        EXPECT_NEAR(got[i].score, ref[i].score, 1e-9) << label << " score i=" << i;
    }
}

// PRE-GROUPING SIMULATION: decode a windowed term's docids by fetching EACH
// window's dd region in its OWN read round (one BatchRangeFetcher per window),
// as the prior per-window [dd][freq] layout forced -- each window's docs prefix
// had to be fetched on its own because the next window's dd was separated by
// the current window's (skipped) freq region. This reproduces the read_at /
// range_get explosion (one per window) that the grouped, contiguous dd-block
// eliminates. Same docids.
std::vector<uint32_t> DecodePerWindow(const LogicalIndexReader& idx, const DictEntry& entry,
                                      uint64_t frq_base, uint64_t prx_base) {
    FrqPreludeReader prelude;
    EXPECT_TRUE(fetch_windowed_prelude(idx, entry, frq_base, &prelude).ok());
    std::vector<uint32_t> docids;
    for (uint32_t w = 0; w < prelude.n_windows(); ++w) {
        WindowAbsRange r;
        EXPECT_TRUE(windowed_window_range(idx, entry, frq_base, prx_base, prelude, w,
                                          /*want_positions=*/false, /*want_freq=*/false, &r)
                            .ok());
        WindowMeta m;
        EXPECT_TRUE(prelude.window(w, &m).ok());
        // One read round per window (the un-grouped reader could not coalesce
        // across the interleaved freq regions of the old layout).
        doris::snii::io::BatchRangeFetcher fetcher(idx.reader(), /*coalesce_gap=*/0);
        const size_t h = fetcher.add(r.dd_off, r.dd_len);
        EXPECT_TRUE(fetcher.fetch().ok());
        std::vector<uint32_t> wd, wf;
        std::vector<std::vector<uint32_t>> wp;
        EXPECT_TRUE(decode_window_slices(m, fetcher.get(h), Slice(), Slice(),
                                         /*want_positions=*/false,
                                         /*want_freq=*/false, &wd, &wf, &wp)
                            .ok());
        docids.insert(docids.end(), wd.begin(), wd.end());
    }
    return docids;
}

} // namespace

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPostingGrouping, ContiguousDdBlockSavesAllThreeMetrics) {
    Corpus c = BuildCorpus();
    const std::string path = TempPath();
    WriteCorpus(c, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    // Fine-grained FileCache block (< a window's freq region) so the grouped
    // docs-only run occupies strictly fewer cache blocks than (i) the full
    // posting and (ii) a per-window dd fetch whose gaps span the skipped freq
    // regions.
    io::MeteredFileReader metered(&local, /*block_size=*/64);

    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&metered, &seg).ok());
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());
    ASSERT_EQ(idx.stats().doc_count, c.doc_count);

    // The high-df term must span many windows (the contiguity win is meaningful).
    const Blocks hi = MeasureBlocks(idx, "aa_hi");
    ASSERT_GE(hi.windows, 8U) << "high-df term should span many windows";
    ASSERT_GT(hi.freq_block, 0U) << "freq-block must carry real bytes";

    // ---- (a) term_query / phrase_query docids == ORACLE -----------------------
    const std::vector<std::string> terms_to_check = {"aa_hi", "aa_mid", "aa_quick", "aa_lazy",
                                                     "aa_rare"};
    for (const auto& term : terms_to_check) {
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::term_query(idx, term, &got).ok()) << term;
        EXPECT_TRUE(std::ranges::is_sorted(got)) << term;
        EXPECT_EQ(got, c.term_oracle(term)) << "term_query oracle mismatch: " << term;
    }

    const std::vector<std::vector<std::string>> phrases = {
            {"aa_hi", "aa_quick", "aa_brown", "aa_fox", "aa_jumps"}, // 5-term, hi lead
            {"aa_hi", "aa_quick"},                                   // hi + mid
            {"aa_quick", "aa_brown", "aa_fox", "aa_jumps"},          // no high-df term
            {"aa_lazy", "aa_dog"},                                   // scattered phrase
            {"aa_brown", "aa_fox"},                                  // sub-phrase
            {"aa_fox", "aa_brown"},                                  // reversed -> absent
            {"aa_hi", "aa_lazy"},                                    // present terms, absent phrase
    };
    for (const auto& p : phrases) {
        std::string label;
        for (const auto& w : p) {
            label += w + " ";
        }
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::phrase_query(idx, p, &got).ok()) << label;
        EXPECT_TRUE(std::ranges::is_sorted(got)) << label;
        EXPECT_EQ(got, c.phrase_oracle(p)) << "phrase oracle mismatch: [" << label << "]";
    }

    // ---- scoring: exhaustive == wand == selective == reference ----------------
    SniiStatsProvider stats;
    ASSERT_TRUE(SniiStatsProvider::open(&idx, &stats).ok());
    const Bm25Params params;
    const std::vector<std::string> score_terms = {"aa_hi", "aa_mid", "aa_rare"};
    for (uint32_t k : {1U, 10U, 100U}) {
        std::vector<ScoredDoc> ex, wa, sel;
        ASSERT_TRUE(query::scoring_query_exhaustive(idx, stats, score_terms, k, params, &ex).ok());
        ASSERT_TRUE(query::scoring_query_wand(idx, stats, score_terms, k, params, &wa).ok());
        ASSERT_TRUE(
                query::scoring_query_wand_selective(idx, stats, score_terms, k, params, &sel).ok());
        const std::vector<ScoredDoc> ref = ReferenceRanking(c, score_terms, k, params);
        ExpectRankingEqual(ex, ref, "exhaustive");
        ExpectRankingEqual(wa, ex, "wand");
        ExpectRankingEqual(sel, ex, "selective");
    }

    DictEntry hi_entry;
    uint64_t hi_frq_base = 0, hi_prx_base = 0;
    bool hi_found = false;
    ASSERT_TRUE(idx.lookup("aa_hi", &hi_found, &hi_entry, &hi_frq_base, &hi_prx_base).ok());
    ASSERT_TRUE(hi_found);
    ASSERT_EQ(hi_entry.frq_docs_len, hi.prelude_len + hi.dd_block);
    ASSERT_LT(hi_entry.frq_docs_len, hi_entry.frq_len);

    DictEntry oversized_docs_prefix = hi_entry;
    ++oversized_docs_prefix.frq_docs_len;
    std::vector<uint32_t> corrupt_docs;
    // Integrated SNII reports posting corruption with the inverted-index-file
    // corruption code (the "docs prefix length mismatch" guard), not the generic
    // CORRUPTION code the standalone test used.
    EXPECT_TRUE(query::internal::read_docid_posting(idx, oversized_docs_prefix, hi_frq_base,
                                                    hi_prx_base, &corrupt_docs)
                        .is<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());

    // ---- (b) docid-only posting read fetches the dd-block CONTIGUOUSLY --------
    // GROUPED = the Phase-D grouped docid-only path: read [prelude][dd-block] as
    // one contiguous prefix; the freq-block is skipped on the wire. The lookup is
    // deliberately outside this metrics window so the assertion covers posting
    // I/O.
    metered.reset_metrics();
    std::vector<uint32_t> grouped_docs;
    ASSERT_TRUE(query::internal::read_docid_posting(idx, hi_entry, hi_frq_base, hi_prx_base,
                                                    &grouped_docs)
                        .ok());
    const io::IoMetrics grouped = metered.metrics();
    ASSERT_EQ(grouped_docs, c.term_oracle("aa_hi"));
    EXPECT_EQ(grouped.serial_rounds, 1U)
            << "docid-only windowed term_query should fetch [prelude][dd-block] in "
               "one "
               "batched round";
    EXPECT_EQ(grouped.range_gets, 1U) << "docid-only windowed term_query should "
                                         "issue one contiguous prefix range";

    // PER-WINDOW = the pre-grouping layout: fetch EACH window's dd region as its
    // own physical range (gaps = the skipped freq regions). Same resolved term.
    metered.reset_metrics();
    const std::vector<uint32_t> perwin_docs =
            DecodePerWindow(idx, hi_entry, hi_frq_base, hi_prx_base);
    const io::IoMetrics perwin = metered.metrics();
    ASSERT_EQ(perwin_docs, c.term_oracle("aa_hi"));

    // FULL = fetch the whole posting (prelude + dd-block + freq-block). Same
    // resolved term.
    metered.reset_metrics();
    {
        DecodedPosting full_posting;
        ASSERT_TRUE(read_windowed_posting(idx, hi_entry, hi_frq_base, hi_prx_base,
                                          /*want_positions=*/false,
                                          /*want_freq=*/true, &full_posting)
                            .ok());
        EXPECT_EQ(full_posting.docids, c.term_oracle("aa_hi"));
        EXPECT_EQ(full_posting.freqs.size(), full_posting.docids.size());
    }
    const io::IoMetrics full = metered.metrics();

    // The grouped docid-only path issues only a HANDFUL of logical reads (lookup
    // + prelude + ONE dd-block range), NOT one-per-window and not thousands.
    EXPECT_LE(grouped.read_at_calls, 4U)
            << "grouped docid-only read_at not contiguous-small: " << grouped.read_at_calls;
    EXPECT_LT(grouped.read_at_calls, hi.windows)
            << "grouped must not read per-window: read_at=" << grouped.read_at_calls
            << " windows=" << hi.windows;

    // (1) vs PER-WINDOW: all three drop because the dd-block is ONE contiguous
    // range instead of one fetch per window separated by the skipped freq
    // regions.
    EXPECT_LT(grouped.read_at_calls, perwin.read_at_calls)
            << "read_at vs per-window did not drop: grouped=" << grouped.read_at_calls
            << " perwin=" << perwin.read_at_calls;
    EXPECT_LT(grouped.range_gets, perwin.range_gets)
            << "range_gets vs per-window did not drop: grouped=" << grouped.range_gets
            << " perwin=" << perwin.range_gets;
    EXPECT_LE(grouped.total_request_bytes, perwin.total_request_bytes)
            << "request_bytes vs per-window grew: grouped=" << grouped.total_request_bytes
            << " perwin=" << perwin.total_request_bytes;

    // (2) vs FULL posting: request_bytes and remote_bytes strictly drop
    // (freq-block skipped on the wire and its cache blocks never touched).
    EXPECT_LT(grouped.total_request_bytes, full.total_request_bytes)
            << "request_bytes vs full did not drop: grouped=" << grouped.total_request_bytes
            << " full=" << full.total_request_bytes;
    EXPECT_EQ(full.total_request_bytes - grouped.total_request_bytes, hi.freq_block)
            << "skipped bytes != freq-block size";
    EXPECT_LT(grouped.remote_bytes, full.remote_bytes)
            << "remote_bytes vs full did not drop: grouped=" << grouped.remote_bytes
            << " full=" << full.remote_bytes;

    std::remove(path.c_str());
}
