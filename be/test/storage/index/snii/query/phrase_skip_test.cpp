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
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/reader/windowed_posting.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

// Differential test for phrase_query WINDOW SKIPPING (design spec section 6.2).
//
// Builds a corpus with a VERY high-df term ("aa_hi", df=2500) whose .frq
// posting spans MANY 256-doc windows, plus low/mid-df terms, and plants known
// phrases (including a 5-term phrase whose FIRST term is the high-df term). For
// every query the skipping phrase_query result must equal:
//   (a) an in-memory brute-force ORACLE, and
//   (b) an independent FULL-READ reference (decode every term's whole posting,
//       intersect, positional check) -- the pre-skipping behavior.
// Finally it asserts the byte/round reduction: a phrase whose low-df lead term
// concentrates the candidates in a few windows reads FAR fewer bytes than the
// full-read path, and the high-df windows touched are NOT proportional to the
// term's total window count.
//
// NOTE on term naming: all real terms use an "aa_" prefix and a "zz_NNN" filler
// vocabulary fills the lexicographic tail, so every real term sorts within the
// SampledTermIndex's candidate range (the index samples per-block first terms).
using namespace doris::snii;
using namespace doris::snii::format;
using namespace doris::snii::reader;
using namespace doris::snii::writer;
using doris::Status;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_phrase_skip_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

constexpr uint32_t kDocCount = 60000;
constexpr uint32_t kHiGap = 24;     // aa_hi at 0, 24, 48 ... -> 2500 occurrences
constexpr uint32_t kHiCount = 2500; // df of aa_hi (>512 -> windowed, 10 windows)
// The 5-term phrase is planted only in the FIRST kPhraseSpan occurrences of
// aa_hi, concentrating its candidate docids into the first window(s).
constexpr uint32_t kPhraseSpan = 150;

struct Corpus {
    uint32_t doc_count = 0;
    std::vector<std::vector<std::string>> docs; // docs[d] = ordered tokens

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

    std::vector<uint32_t> oracle(const std::vector<std::string>& phrase) const {
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
    const std::vector<std::string> vocab = {"alpha", "bravo", "charlie", "delta"};

    for (uint32_t d = 0; d < c.doc_count; ++d) {
        std::vector<std::string>& toks = c.docs[d];
        const bool is_hi = (d % kHiGap == 0) && (d / kHiGap < kHiCount);
        if (is_hi) {
            toks.emplace_back("aa_hi"); // high-df, position 0
        }
        const uint32_t occ = d / kHiGap; // aa_hi occurrence ordinal

        if (is_hi && occ < kPhraseSpan) {
            // 5-term phrase led by the high-df term, concentrated in the first
            // windows.
            toks.emplace_back("aa_quick");
            toks.emplace_back("aa_brown");
            toks.emplace_back("aa_fox");
            toks.emplace_back("aa_jumps");
        } else if (d % 13 == 0) {
            // A phrase NOT containing the high-df term, scattered across the corpus.
            toks.emplace_back("aa_lazy");
            toks.emplace_back("aa_dog");
        }
        for (uint32_t k = 0; k < 2; ++k) {
            toks.push_back(vocab[(d + k) % vocab.size()]);
        }
        if (d % 101 == 0) {
            toks.emplace_back("aa_rare");
        }
        if (d % 997 == 0) {
            toks.emplace_back("aa_echo");
            toks.emplace_back("aa_echo");
        }
        // Larger filler vocabulary to occupy the lexicographic tail blocks.
        char nm[16];
        std::snprintf(nm, sizeof(nm), "zz_%03u", d % 500);
        toks.emplace_back(nm);
    }
    return c;
}

Corpus BuildDensePhraseCorpus() {
    Corpus c;
    c.doc_count = 4096;
    c.docs.resize(c.doc_count);
    for (uint32_t d = 0; d < c.doc_count; ++d) {
        std::vector<std::string>& toks = c.docs[d];
        toks.emplace_back("aa_dense");
        if (d >= 16 && d < 96) {
            toks.emplace_back("aa_rare_driver");
        } else {
            toks.emplace_back("aa_gap");
        }
        char nm[16];
        std::snprintf(nm, sizeof(nm), "zz_%03u", d % 257);
        toks.emplace_back(nm);
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
    in.config = IndexConfig::kDocsPositions;
    in.doc_count = c.doc_count;
    in.terms = std::move(terms);
    in.target_dict_block_bytes = 256;

    io::LocalFileWriter w;
    ASSERT_TRUE(w.open(path).ok());
    SniiCompoundWriter cw(&w);
    ASSERT_TRUE(cw.add_logical_index(in).ok());
    ASSERT_TRUE(cw.finish().ok());
}

// Decodes one term's FULL posting (every window for windowed; the single window
// for slim/inline) -> sorted docids + aligned per-doc positions. This mirrors
// the pre-skipping reference path used to cross-check the skipping result.
struct FullPosting {
    std::vector<uint32_t> docids;
    std::vector<std::vector<uint32_t>> positions;
};

bool DecodeFullTerm(const LogicalIndexReader& idx, const std::string& term, FullPosting* out) {
    DictEntry entry;
    uint64_t frq_base = 0, prx_base = 0;
    bool found = false;
    EXPECT_TRUE(idx.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    if (!found) {
        return false;
    }

    if (entry.kind == DictEntryKind::kPodRef && entry.enc == DictEntryEnc::kWindowed) {
        DecodedPosting dp;
        EXPECT_TRUE(read_windowed_posting(idx, entry, frq_base, prx_base,
                                          /*want_positions=*/true,
                                          /*want_freq=*/false, &dp)
                            .ok());
        out->docids = std::move(dp.docids);
        out->positions = std::move(dp.positions);
        return true;
    }
    std::vector<uint8_t> frq, prx;
    if (entry.kind == DictEntryKind::kPodRef) {
        uint64_t foff = 0, flen = 0, poff = 0, plen = 0;
        EXPECT_TRUE(idx.resolve_frq_window(entry, frq_base, &foff, &flen).ok());
        EXPECT_TRUE(idx.resolve_prx_window(entry, prx_base, &poff, &plen).ok());
        EXPECT_TRUE(idx.reader()->read_at(foff, flen, &frq).ok());
        EXPECT_TRUE(idx.reader()->read_at(poff, plen, &prx).ok());
    } else {
        frq = entry.frq_bytes;
        prx = entry.prx_bytes;
    }
    // Slim/inline window = [dd_region][freq_region]; the dd region is the docs
    // prefix.
    WindowMeta meta;
    meta.win_base = 0;
    meta.doc_count = entry.df;
    meta.dd_zstd = entry.dd_meta.zstd;
    meta.dd_uncomp_len = entry.dd_meta.uncomp_len;
    meta.dd_disk_len = entry.dd_meta.disk_len;
    meta.crc_dd = entry.dd_meta.crc;
    // INLINE entries (format v2) carry no per-region crc -- their bytes are
    // covered by the dict block crc32c -- so decode must skip the region crc
    // check.
    meta.verify_crc = entry.dd_meta.verify_crc;
    EXPECT_LE(entry.dd_meta.disk_len, frq.size());
    Slice dd_region(frq.data(), static_cast<size_t>(entry.dd_meta.disk_len));
    std::vector<uint32_t> freqs;
    Status st = decode_window_slices(meta, dd_region, Slice(), Slice(prx),
                                     /*want_positions=*/true, /*want_freq=*/false, &out->docids,
                                     &freqs, &out->positions);
    EXPECT_TRUE(st.ok()) << st.msg();
    return true;
}

// Independent FULL-READ phrase reference: intersect full postings, positional
// check. Mirrors the pre-skipping algorithm.
std::vector<uint32_t> FullReadPhrase(const LogicalIndexReader& idx,
                                     const std::vector<std::string>& phrase) {
    std::vector<FullPosting> posts(phrase.size());
    for (size_t t = 0; t < phrase.size(); ++t) {
        if (!DecodeFullTerm(idx, phrase[t], &posts[t])) {
            return {};
        }
    }
    std::vector<uint32_t> cand = posts[0].docids;
    for (size_t t = 1; t < posts.size(); ++t) {
        std::vector<uint32_t> next;
        std::set_intersection(cand.begin(), cand.end(), posts[t].docids.begin(),
                              posts[t].docids.end(), std::back_inserter(next));
        cand.swap(next);
    }
    auto positions_for = [](const FullPosting& p, uint32_t d) -> const std::vector<uint32_t>& {
        const auto it = std::ranges::lower_bound(p.docids, d);
        return p.positions[static_cast<size_t>(it - p.docids.begin())];
    };
    std::vector<uint32_t> out;
    for (uint32_t d : cand) {
        const auto& first = positions_for(posts[0], d);
        bool hit = false;
        for (uint32_t start : first) {
            bool ok = true;
            for (size_t t = 1; t < posts.size(); ++t) {
                const auto& ps = positions_for(posts[t], d);
                if (!std::ranges::binary_search(ps, start + static_cast<uint32_t>(t))) {
                    ok = false;
                    break;
                }
            }
            if (ok) {
                hit = true;
                break;
            }
        }
        if (hit) {
            out.push_back(d);
        }
    }
    return out;
}

// Looks up the high-df term and reports its full .frq/.prx length + window
// count.
void HighDfStats(const LogicalIndexReader& idx, const std::string& term, uint64_t* frq_len,
                 uint64_t* prx_len, uint32_t* windows) {
    DictEntry entry;
    uint64_t frq_base = 0, prx_base = 0;
    bool found = false;
    ASSERT_TRUE(idx.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    ASSERT_TRUE(found);
    ASSERT_EQ(entry.enc, DictEntryEnc::kWindowed);
    FrqPreludeReader prelude;
    ASSERT_TRUE(fetch_windowed_prelude(idx, entry, frq_base, &prelude).ok());
    *windows = prelude.n_windows();
    *frq_len = entry.frq_len;
    *prx_len = entry.prx_len;
}

} // namespace

// boolean_and (MATCH all-terms) must equal the intersection of the per-term
// docid sets (term_query is the trusted oracle). Covers high+low df mix,
// all-present, an absent term (-> empty), and single-term (-> that term's
// docs).
TEST(SniiBooleanAnd, EqualsTermIntersection) {
    Corpus c = BuildCorpus();
    const std::string path = TempPath();
    WriteCorpus(c, path);
    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/4096);
    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&metered, &seg).ok());
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());

    auto intersect_terms = [&](const std::vector<std::string>& terms) {
        std::vector<uint32_t> acc;
        bool first = true;
        for (const auto& t : terms) {
            std::vector<uint32_t> d;
            EXPECT_TRUE(query::term_query(idx, t, &d).ok());
            if (first) {
                acc = d;
                first = false;
            } else {
                std::vector<uint32_t> out;
                std::ranges::set_intersection(acc, d, std::back_inserter(out));
                acc = std::move(out);
            }
        }
        return acc;
    };

    const std::vector<std::vector<std::string>> cases = {
            {"aa_hi", "aa_quick"}, // high + low df
            {"aa_quick", "aa_brown", "aa_fox"},
            {"aa_hi", "nope_absent"}, // absent term -> empty
            {"aa_hi"},                // single term -> its docs
    };
    for (const auto& terms : cases) {
        std::string label;
        for (const auto& t : terms) {
            label += t + " ";
        }
        std::vector<uint32_t> got;
        ASSERT_TRUE(query::boolean_and(idx, terms, &got).ok()) << label;
        EXPECT_TRUE(std::ranges::is_sorted(got)) << label;
        EXPECT_EQ(got, intersect_terms(terms))
                << "boolean_and != term-intersection: [" << label << "]";
    }
    std::remove(path.c_str());
}

// prefix_terms ordered enumeration: the full enumeration (empty prefix) must be
// strictly sorted; any prefix scan must equal the full enumeration filtered by
// that prefix; and every enumerated term must lookup() to the SAME DictEntry
// (df).
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPrefixTerms, OrderedEnumerationMatchesFilterAndLookup) {
    Corpus c = BuildCorpus();
    const std::string path = TempPath();
    WriteCorpus(c, path);
    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/4096);
    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&metered, &seg).ok());
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());

    std::vector<LogicalIndexReader::PrefixHit> all;
    ASSERT_TRUE(idx.prefix_terms("", &all).ok());
    ASSERT_FALSE(all.empty());
    for (size_t i = 1; i < all.size(); ++i) {
        EXPECT_LT(all[i - 1].term, all[i].term) << "at " << i;
    }
    for (const auto& h : all) {
        bool found = false;
        doris::snii::format::DictEntry e;
        uint64_t fb = 0, pb = 0;
        ASSERT_TRUE(idx.lookup(h.term, &found, &e, &fb, &pb).ok());
        EXPECT_TRUE(found) << h.term;
        EXPECT_EQ(e.df, h.entry.df) << h.term;
    }
    auto filtered = [&](const std::string& pfx) {
        std::vector<std::string> v;
        for (const auto& h : all) {
            if (h.term.size() >= pfx.size() && h.term.starts_with(pfx)) {
                v.push_back(h.term);
            }
        }
        return v;
    };
    auto scanned = [&](const std::string& pfx) {
        std::vector<LogicalIndexReader::PrefixHit> hits;
        EXPECT_TRUE(idx.prefix_terms(pfx, &hits).ok());
        std::vector<std::string> v;
        for (const auto& h : hits) {
            v.push_back(h.term);
        }
        return v;
    };
    for (const char* pfx : {"aa_", "aa_h", "zz_", "zz_0", "alpha"}) {
        EXPECT_EQ(scanned(pfx), filtered(pfx)) << "prefix=" << pfx;
    }
    std::vector<LogicalIndexReader::PrefixHit> none;
    ASSERT_TRUE(idx.prefix_terms("zzzznope", &none).ok());
    EXPECT_TRUE(none.empty());
    // Null output is rejected, not dereferenced.
    EXPECT_FALSE(idx.prefix_terms("", nullptr).ok());
    std::remove(path.c_str());
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiPhraseSkip, SkippingEqualsOracleAndFullRead) {
    Corpus c = BuildCorpus();
    const std::string path = TempPath();
    WriteCorpus(c, path);

    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    // Small cache block so window-level skipping shows up in remote_bytes / GETs.
    io::MeteredFileReader metered(&local, /*block_size=*/4096);

    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&metered, &seg).ok());
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());
    ASSERT_EQ(idx.stats().doc_count, c.doc_count);

    uint64_t hi_frq_len = 0, hi_prx_len = 0;
    uint32_t hi_windows = 0;
    HighDfStats(idx, "aa_hi", &hi_frq_len, &hi_prx_len, &hi_windows);
    ASSERT_GE(hi_windows, 8U) << "high-df term should span many windows";

    // Phrases: present (incl. 5-term led by high-df term), absent, sub-phrases.
    const std::vector<std::vector<std::string>> phrases = {
            {"aa_hi", "aa_quick", "aa_brown", "aa_fox", "aa_jumps"}, // 5-term, hi lead
            {"aa_hi", "aa_quick"},                                   // hi + mid
            {"aa_quick", "aa_brown", "aa_fox", "aa_jumps"},          // no high-df term
            {"aa_lazy", "aa_dog"},                                   // no high-df term
            {"aa_brown", "aa_fox"},                                  // sub-phrase
            {"aa_fox", "aa_brown"},                                  // reversed -> absent
            {"aa_hi", "aa_lazy"},                                    // present terms, absent phrase
            {"aa_quick", "aa_jumps"},                                // non-consecutive -> absent
            {"aa_echo", "aa_echo"}, // repeated term -> unique-term mapping
            {"aa_hi"},              // single high-df term
            {"nope", "missing"},    // absent terms -> empty
    };

    for (const auto& p : phrases) {
        std::string label;
        for (const auto& w : p) {
            label += w + " ";
        }

        std::vector<uint32_t> got;
        ASSERT_TRUE(query::phrase_query(idx, p, &got).ok()) << label;
        EXPECT_TRUE(std::ranges::is_sorted(got)) << label;

        const std::vector<uint32_t> want = c.oracle(p);
        EXPECT_EQ(got, want) << "oracle mismatch: [" << label << "]";

        const std::vector<uint32_t> full = FullReadPhrase(idx, p);
        EXPECT_EQ(got, full) << "full-read mismatch: [" << label << "]";
    }

    // ---- Byte/round reduction for a phrase CONTAINING the high-df term. ----
    // The low-df lead "aa_quick" concentrates candidates into the first window(s)
    // of "aa_hi", so the skip path reads only those windows + the prelude, NOT
    // the whole posting. Compare against a full-read of the same phrase.
    const std::vector<std::string> hi_phrase = {"aa_hi", "aa_quick", "aa_brown", "aa_fox",
                                                "aa_jumps"};
    metered.reset_metrics();
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_query(idx, hi_phrase, &got).ok());
    const io::IoMetrics skip = metered.metrics();

    metered.reset_metrics();
    (void)FullReadPhrase(idx, hi_phrase);
    const io::IoMetrics full = metered.metrics();

    // 1. Skipping requests SUBSTANTIALLY fewer bytes than the full-read path: the
    //    high-df term contributes only its prelude + the candidate-covering
    //    windows (dd + prx sub-ranges), never its whole posting, so the 5-term
    //    skip query reads comfortably under 80% of the full-read bytes (a clear
    //    reduction even with the richer Phase-D prelude and 4 companion terms).
    EXPECT_LT(skip.total_request_bytes * 10, full.total_request_bytes * 8)
            << "skip=" << skip.total_request_bytes << " full=" << full.total_request_bytes;
    // 2. Skipping requests strictly fewer bytes than the full-read path.
    EXPECT_LT(skip.total_request_bytes, full.total_request_bytes)
            << "skip=" << skip.total_request_bytes << " full=" << full.total_request_bytes;
    // 3. Range GETs stay small and are NOT proportional to the term's window
    // count
    //    (the high-df term contributes only its prelude + the covering windows).
    EXPECT_LT(skip.range_gets, hi_windows)
            << "range_gets=" << skip.range_gets << " windows=" << hi_windows;
    // 4. Skipping issues no more range GETs than the full-read path, and stays in
    //    a small number of serial rounds (preludes + the selected-window batch).
    EXPECT_LE(skip.range_gets, full.range_gets)
            << "skip_gets=" << skip.range_gets << " full_gets=" << full.range_gets;
    EXPECT_GE(skip.serial_rounds, 1U);
    EXPECT_LE(skip.serial_rounds, 4U) << "skip did not stay in few serial rounds";

    std::remove(path.c_str());
}

TEST(SniiPhraseSkip, DenseLeadingTermWithRareAnchorSkipsDocsButKeepsPositions) {
    Corpus c = BuildDensePhraseCorpus();
    const std::string path = TempPath();
    WriteCorpus(c, path);
    io::LocalFileReader local;
    ASSERT_TRUE(local.open(path).ok());
    io::MeteredFileReader metered(&local, /*block_size=*/4096);
    SniiSegmentReader seg;
    ASSERT_TRUE(SniiSegmentReader::open(&metered, &seg).ok());
    LogicalIndexReader idx;
    ASSERT_TRUE(seg.open_index(1, "body", &idx).ok());

    uint64_t dense_frq = 0;
    uint64_t dense_prx = 0;
    uint32_t dense_windows = 0;
    HighDfStats(idx, "aa_dense", &dense_frq, &dense_prx, &dense_windows);
    ASSERT_GE(dense_windows, 8U);

    const std::vector<std::string> phrase = {"aa_dense", "aa_rare_driver"};
    metered.reset_metrics();
    std::vector<uint32_t> got;
    ASSERT_TRUE(query::phrase_query(idx, phrase, &got).ok());
    const uint64_t skipped_bytes = metered.metrics().total_request_bytes;

    metered.reset_metrics();
    const std::vector<uint32_t> full = FullReadPhrase(idx, phrase);
    const uint64_t full_bytes = metered.metrics().total_request_bytes;

    EXPECT_EQ(got, c.oracle(phrase));
    EXPECT_EQ(got, full);
    EXPECT_LT(skipped_bytes, full_bytes)
            << "dense leading phrase should keep the rare-position anchor but avoid "
               "reading dense docid regions";

    std::remove(path.c_str());
}
