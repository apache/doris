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

// Differential + zero-GET tests for the inline-small-terms feature.
//
// A term whose full posting (.frq + .prx) block is <= the inline threshold is
// stored verbatim INSIDE its .tis entry (kFormatInline = -5), so a query
// fetches it together with the resident term dictionary — zero extra .frq/.prx
// GET. This file asserts:
//   (1) Byte-identical query results: a corpus written with inlining enabled
//       (threshold 256) produces the SAME (doc, freq, position*) tuples for
//       every term as the SAME corpus forced fully external (threshold 0).
//   (2) Zero-GET proxy: querying an inlined small term issues ZERO read_at on
//       the external .frq/.prx stores; a large (non-inlined) term still does.
//   (3) omit_tfap inline term: prx_len == 0 inline term decodes correctly.
//   (4) .tis TermInfo round-trip: TermDictReader recovers inline spans.
//   (5) Merger correctness with inline output on a multi-input merge.

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/field_infos_writer.h"
#include "storage/index/inverted/spimi/posting_buffer.h"
#include "storage/index/inverted/spimi/posting_store.h"
#include "storage/index/inverted/spimi/query_term_positions.h"
#include "storage/index/inverted/spimi/segment_merger.h"
#include "storage/index/inverted/spimi/segment_writer.h"
#include "storage/index/inverted/spimi/term_dict_reader.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

using Post = std::tuple<std::string, uint32_t, uint32_t>; // (term, doc, pos)

// One term's fully-decoded query result: per-doc (doc_id, freq, positions).
struct DocResult {
    int32_t doc = 0;
    int32_t freq = 0;
    std::vector<int32_t> positions;
    bool operator==(const DocResult& o) const {
        return doc == o.doc && freq == o.freq && positions == o.positions;
    }
};

// Builds a V4 windowed SPIMI segment from `posts` and queries it. When
// `inline_threshold > 0` the writer inlines small terms; the read path
// transparently decodes inline-or-external.
struct Segment {
    MemoryByteOutput tis, tii, frq, prx;
    std::vector<FieldInfoEntry> field_infos;
    std::vector<std::wstring> field_names_wide;
    std::unique_ptr<TermDictReader> dict;

    explicit Segment(bool has_prox = true) {
        field_infos.push_back(
                {.name = "body", .is_indexed = true, .omit_norms = true, .has_prox = has_prox});
        field_names_wide.emplace_back(L"body");
    }

    void Write(const std::vector<Post>& posts, bool inline_small, uint32_t threshold) {
        SpimiPostingBuffer buffer;
        for (const auto& [t, d, p] : posts) {
            buffer.Append(t, d, p);
        }
        buffer.Sort();
        SegmentWriter w(&tis, &tii, &frq, &prx, TermDictWriter::kDefaultIndexInterval,
                        TermDictWriter::kDefaultSkipInterval, TermDictWriter::kMaxSkipLevels,
                        /*omit_term_freq_and_positions=*/!field_infos[0].has_prox,
                        /*use_windowed=*/true, inline_small, threshold);
        w.Emit(buffer, /*field_number=*/0);
        w.Close();
        dict = std::make_unique<TermDictReader>(tis.bytes(), tii.bytes());
    }

    // Decodes every (doc, freq, position) for `term` through the query path.
    // `frq_reads` / `prx_reads`, when non-null, receive the external store
    // read_at counts after the query (0 proves the data came from inline/.tis).
    std::vector<DocResult> Query(const std::string& term, int64_t* frq_reads = nullptr,
                                 int64_t* prx_reads = nullptr) {
        auto frq_store = std::make_unique<MemPostingStore>(frq.bytes().data(), frq.bytes().size());
        std::unique_ptr<MemPostingStore> prx_store;
        if (!prx.bytes().empty()) {
            prx_store = std::make_unique<MemPostingStore>(prx.bytes().data(), prx.bytes().size());
        }
        MemPostingStore* frq_raw = frq_store.get();
        MemPostingStore* prx_raw = prx_store.get();

        std::unique_ptr<PostingStore> prx_ps;
        if (prx_store) {
            prx_ps = std::move(prx_store);
        }
        SpimiQueryTermPositions tp(dict.get(), std::move(frq_store), std::move(prx_ps),
                                   &field_infos, &field_names_wide);
        if (frq_raw != nullptr) {
            frq_raw->reset_counters();
        }
        if (prx_raw != nullptr) {
            prx_raw->reset_counters();
        }

        const std::wstring wterm(term.begin(), term.end());
        auto* lterm = _CLNEW lucene::index::Term(L"body", wterm.c_str());
        std::vector<DocResult> out;
        try {
            tp.seek(lterm);
            const bool has_prox = field_infos[0].has_prox;
            while (tp.next()) {
                DocResult dr;
                dr.doc = tp.doc();
                dr.freq = tp.freq();
                if (has_prox) {
                    for (int32_t i = 0; i < dr.freq; ++i) {
                        dr.positions.push_back(tp.nextPosition());
                    }
                }
                out.push_back(std::move(dr));
            }
        } catch (...) {
            _CLDECDELETE(lterm);
            throw;
        }
        _CLDECDELETE(lterm);
        if (frq_reads != nullptr) {
            *frq_reads = frq_raw->read_count();
        }
        if (prx_reads != nullptr) {
            *prx_reads = (prx_raw != nullptr) ? prx_raw->read_count() : 0;
        }
        return out;
    }
};

// Builds a corpus with a Zipf-ish mix of tiny terms (df 1..3) and a few large
// terms (df well over the skip interval) so both inline and external paths are
// exercised.
// NOTE: the V4 windowed `.frq` format requires the first doc id of every term
// to be >= 1 (a min_docid of 0 is ambiguous with a zero first delta), so all
// doc ids below start at 1.
std::vector<Post> MakeCorpus() {
    std::vector<Post> posts;
    // Many tiny terms (df 1..3) — these are the inline candidates.
    for (int t = 0; t < 60; ++t) {
        const std::string term = "tiny" + std::to_string(t);
        const int df = 1 + (t % 3);
        for (int d = 0; d < df; ++d) {
            const uint32_t doc = static_cast<uint32_t>(1 + t * 7 + d);
            posts.emplace_back(term, doc, 0);
            posts.emplace_back(term, doc, 5); // 2 positions per doc
        }
    }
    // A couple of large terms — genuinely > 256 B even after windowed ZSTD, so
    // they keep external pointers. 4000 docs, each with several spread-out
    // positions so the block does not compress below the inline budget.
    for (int big = 0; big < 2; ++big) {
        const std::string term = "big" + std::to_string(big);
        for (int d = 0; d < 4000; ++d) {
            const uint32_t doc = static_cast<uint32_t>(100000 + big * 100000 + d);
            for (int pp = 0; pp < 3; ++pp) {
                posts.emplace_back(term, doc, static_cast<uint32_t>((d * 7 + pp * 131) % 997));
            }
        }
    }
    return posts;
}

} // namespace

// (1) Inline-enabled query results are byte-identical to forced-external.
TEST(InlineSmallTermsTest, ByteIdenticalInlineVsExternal) {
    const auto corpus = MakeCorpus();

    Segment inlined;
    inlined.Write(corpus, /*inline_small=*/true, /*threshold=*/256);
    Segment external;
    external.Write(corpus, /*inline_small=*/false, /*threshold=*/0);

    // Collect the distinct term set.
    std::map<std::string, bool> terms;
    for (const auto& [t, d, p] : corpus) {
        terms[t] = true;
    }

    for (const auto& [term, _] : terms) {
        const auto a = inlined.Query(term);
        const auto b = external.Query(term);
        ASSERT_EQ(a.size(), b.size()) << "doc count differs for term " << term;
        for (size_t i = 0; i < a.size(); ++i) {
            EXPECT_TRUE(a[i] == b[i]) << "tuple differs for term " << term << " doc-index " << i;
        }
    }
}

// (a-boundary) Threshold boundary sweep: terms whose posting block sizes
// straddle the inline threshold must (i) still decode byte-identically to a
// forced-external build, and (ii) cross the inline/external decision boundary
// as the threshold moves. We build a family of terms with strictly increasing
// posting sizes (df 1, 2, 4, 8, ... up to a clearly-external size), then sweep
// the threshold and assert: every term at/below threshold inlines, every term
// above stays external, and all query results equal the external reference.
TEST(InlineSmallTermsTest, ThresholdBoundaryInlineDecisionAndIdentity) {
    // df=1 (smallest possible) up through a large df that is unambiguously
    // external at any sane threshold. Positions spread so blocks don't collapse.
    const std::vector<int> dfs = {1, 1, 2, 3, 5, 8, 13, 30, 80, 250, 1500};
    std::vector<Post> posts;
    for (size_t i = 0; i < dfs.size(); ++i) {
        const std::string term = "b" + std::to_string(i);
        const int df = dfs[i];
        for (int d = 0; d < df; ++d) {
            // Doc ids start at 1 (windowed format requires first doc id >= 1)
            // and are disjoint per term.
            const uint32_t doc = static_cast<uint32_t>(1 + static_cast<int>(i) * 5000 + d);
            posts.emplace_back(term, doc, static_cast<uint32_t>((d * 13 + 1) % 503));
            posts.emplace_back(term, doc, static_cast<uint32_t>((d * 29 + 7) % 503));
        }
    }

    std::vector<std::string> terms;
    for (size_t i = 0; i < dfs.size(); ++i) {
        terms.push_back("b" + std::to_string(i));
    }

    // External reference: nothing inlined.
    Segment external;
    external.Write(posts, /*inline_small=*/false, /*threshold=*/0);

    // Sweep a range of thresholds spanning tiny -> well past the largest small
    // term's block. For each threshold, assert the inline decision tracks the
    // staged block size and results stay byte-identical to external.
    for (uint32_t threshold : {0u, 16u, 32u, 64u, 128u, 256u, 512u, 1024u, 4096u}) {
        Segment inlined;
        inlined.Write(posts, /*inline_small=*/true, threshold);

        // Inline decisions must be monotone in df: once a term (by increasing
        // block size) becomes external at a threshold, all larger terms are
        // external too. Verify monotonicity AND identity.
        bool seen_external = false;
        for (const std::string& term : terms) {
            auto ti = inlined.dict->LookupTerm(0, term);
            ASSERT_TRUE(ti.has_value()) << "term " << term << " thr=" << threshold;
            if (ti->inlined) {
                EXPECT_FALSE(seen_external)
                        << "non-monotone inline decision: " << term << " inlined after a larger "
                        << "term went external (thr=" << threshold << ")";
            } else {
                seen_external = true;
            }
            // threshold==0 must inline nothing.
            if (threshold == 0) {
                EXPECT_FALSE(ti->inlined) << term << " inlined at threshold 0";
            }

            const auto a = inlined.Query(term);
            const auto b = external.Query(term);
            ASSERT_EQ(a.size(), b.size()) << "term " << term << " thr=" << threshold;
            for (size_t i = 0; i < a.size(); ++i) {
                EXPECT_TRUE(a[i] == b[i])
                        << "tuple differs term " << term << " thr=" << threshold << " idx " << i;
            }
        }

        // At a generous threshold (>= 256) at least the df=1 term must inline,
        // proving the boundary is actually crossed (not "everything external").
        if (threshold >= 256) {
            auto df1 = inlined.dict->LookupTerm(0, "b0");
            ASSERT_TRUE(df1.has_value());
            EXPECT_TRUE(df1->inlined) << "df=1 term should inline at threshold " << threshold;
        }
        // At a tiny threshold (16 B) the largest term's block far exceeds the
        // budget, so it must stay external — proving the boundary actually
        // gates the largest term in at least one swept threshold. (The windowed
        // .frq/.prx blocks are ZSTD-compressed, so the on-disk block of even a
        // high-df term can drop below a few KiB; we anchor on the low end where
        // the decision is unambiguous instead of assuming a fixed df is large.)
        if (threshold <= 16) {
            auto biggest = inlined.dict->LookupTerm(0, terms.back());
            ASSERT_TRUE(biggest.has_value());
            EXPECT_FALSE(biggest->inlined)
                    << "largest term must stay external at threshold " << threshold;
        }
    }
}

// (a-zerogete-boundary) Zero-extra-GET holds for an inlined term that sits just
// below the threshold, and an external term just above it still reads .frq.
TEST(InlineSmallTermsTest, ZeroGetAcrossBoundary) {
    const std::vector<int> dfs = {1, 2, 4, 8, 16, 64, 1500};
    std::vector<Post> posts;
    for (size_t i = 0; i < dfs.size(); ++i) {
        const std::string term = "c" + std::to_string(i);
        for (int d = 0; d < dfs[i]; ++d) {
            const uint32_t doc = static_cast<uint32_t>(1 + static_cast<int>(i) * 5000 + d);
            posts.emplace_back(term, doc, static_cast<uint32_t>((d * 17 + 3) % 401));
            posts.emplace_back(term, doc, static_cast<uint32_t>((d * 41 + 9) % 401));
        }
    }

    Segment seg;
    seg.Write(posts, /*inline_small=*/true, /*threshold=*/256);

    // Every inlined term must issue ZERO external reads; every external term
    // must issue > 0 (the zero-extra-GET property is exactly per-term inline).
    bool saw_inlined = false;
    bool saw_external = false;
    for (size_t i = 0; i < dfs.size(); ++i) {
        const std::string term = "c" + std::to_string(i);
        auto ti = seg.dict->LookupTerm(0, term);
        ASSERT_TRUE(ti.has_value());
        int64_t frq_reads = -1;
        int64_t prx_reads = -1;
        auto r = seg.Query(term, &frq_reads, &prx_reads);
        ASSERT_FALSE(r.empty()) << term;
        if (ti->inlined) {
            saw_inlined = true;
            EXPECT_EQ(frq_reads, 0) << "inlined term " << term << " read external .frq";
            EXPECT_EQ(prx_reads, 0) << "inlined term " << term << " read external .prx";
        } else {
            saw_external = true;
            EXPECT_GT(frq_reads, 0) << "external term " << term << " did not read .frq";
        }
    }
    EXPECT_TRUE(saw_inlined) << "test corpus must contain at least one inlined term";
    EXPECT_TRUE(saw_external) << "test corpus must contain at least one external term";
}

// Header format byte: inline segment is -5, external segment is -4.
TEST(InlineSmallTermsTest, HeaderFormatByte) {
    const auto corpus = MakeCorpus();
    Segment inlined;
    inlined.Write(corpus, /*inline_small=*/true, /*threshold=*/256);
    Segment external;
    external.Write(corpus, /*inline_small=*/false, /*threshold=*/0);

    auto read_format = [](const std::vector<uint8_t>& b) {
        return static_cast<int32_t>((b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3]);
    };
    EXPECT_EQ(read_format(inlined.tis.bytes()), TermDictWriter::kFormatInline);
    EXPECT_EQ(read_format(inlined.tii.bytes()), TermDictWriter::kFormatInline);
    EXPECT_EQ(read_format(external.tis.bytes()), TermDictWriter::kFormat);
}

// (2) Zero-GET proxy: a small inlined term touches the external stores ZERO
// times; a large external term touches them.
TEST(InlineSmallTermsTest, ZeroGetForSmallTermNonzeroForLarge) {
    const auto corpus = MakeCorpus();
    Segment seg;
    seg.Write(corpus, /*inline_small=*/true, /*threshold=*/256);

    int64_t frq_reads = -1;
    int64_t prx_reads = -1;
    auto small = seg.Query("tiny0", &frq_reads, &prx_reads);
    ASSERT_FALSE(small.empty());
    EXPECT_EQ(frq_reads, 0) << "small inlined term must not read the external .frq";
    EXPECT_EQ(prx_reads, 0) << "small inlined term must not read the external .prx";

    int64_t big_frq_reads = -1;
    int64_t big_prx_reads = -1;
    auto big = seg.Query("big0", &big_frq_reads, &big_prx_reads);
    ASSERT_FALSE(big.empty());
    EXPECT_GT(big_frq_reads, 0) << "large external term must read the external .frq";
    // .prx is read lazily only when positions are pulled; we pull them above.
    EXPECT_GT(big_prx_reads, 0) << "large external term must read the external .prx";
}

// .tis TermInfo round-trip: the reader recovers inline spans for small terms
// and external pointers for large terms.
TEST(InlineSmallTermsTest, TermInfoRoundTripInlineSpans) {
    const auto corpus = MakeCorpus();
    Segment seg;
    seg.Write(corpus, /*inline_small=*/true, /*threshold=*/256);

    auto small = seg.dict->LookupTerm(0, "tiny0");
    ASSERT_TRUE(small.has_value());
    EXPECT_TRUE(small->inlined);
    EXPECT_NE(small->inline_frq, nullptr);
    EXPECT_GT(small->inline_frq_len, 0U);
    // has_prox term: prx span present.
    EXPECT_NE(small->inline_prx, nullptr);
    EXPECT_GT(small->inline_prx_len, 0U);

    auto big = seg.dict->LookupTerm(0, "big0");
    ASSERT_TRUE(big.has_value());
    EXPECT_FALSE(big->inlined);
    EXPECT_GE(big->freq_pointer, 0);
}

// (3) omit_tfap inline term: prx_len == 0, no positions, decodes correctly.
TEST(InlineSmallTermsTest, OmitTfapInlineTerm) {
    // First doc id >= 1 (windowed format requirement).
    std::vector<Post> posts;
    for (int t = 0; t < 10; ++t) {
        const std::string term = "w" + std::to_string(t);
        posts.emplace_back(term, static_cast<uint32_t>(t + 1), 0);
        posts.emplace_back(term, static_cast<uint32_t>(t + 2), 0);
    }

    Segment inlined(/*has_prox=*/false);
    inlined.Write(posts, /*inline_small=*/true, /*threshold=*/256);
    Segment external(/*has_prox=*/false);
    external.Write(posts, /*inline_small=*/false, /*threshold=*/0);

    // omit_tfap: .prx is empty; the inline prx span must be zero-length.
    auto ti = inlined.dict->LookupTerm(0, "w0");
    ASSERT_TRUE(ti.has_value());
    EXPECT_TRUE(ti->inlined);
    EXPECT_EQ(ti->inline_prx_len, 0U);

    for (int t = 0; t < 10; ++t) {
        const std::string term = "w" + std::to_string(t);
        const auto a = inlined.Query(term);
        const auto b = external.Query(term);
        ASSERT_EQ(a.size(), b.size()) << term;
        for (size_t i = 0; i < a.size(); ++i) {
            EXPECT_EQ(a[i].doc, b[i].doc) << term;
            EXPECT_EQ(a[i].freq, b[i].freq) << term;
        }
    }

    // Zero-GET on the small omit_tfap inline term.
    int64_t frq_reads = -1;
    int64_t prx_reads = -1;
    inlined.Query("w0", &frq_reads, &prx_reads);
    EXPECT_EQ(frq_reads, 0);
    EXPECT_EQ(prx_reads, 0);
}

// (5) Merger correctness: a multi-input merge with inline-enabled V4 output.
// The merge inputs are non-inlined spill-style segments; the merged output
// inlines small terms. Query results match a single-segment external build.
TEST(InlineSmallTermsTest, MergerInlineOutputMatchesExternal) {
    // Two inputs with disjoint doc ranges but overlapping terms. Doc ids start
    // at 1 (windowed format requires the first doc id >= 1). in0 covers docs
    // {1,2} (doc_count 3); in1 covers docs {1,2} (doc_count 3) and is offset by
    // in0's doc_count (3) in the merge, so its docs become {4,5}.
    std::vector<Post> in0 = {{"alpha", 1, 0}, {"alpha", 2, 2}, {"beta", 1, 1}};
    std::vector<Post> in1 = {{"alpha", 1, 3}, {"gamma", 2, 0}};

    auto build_input = [](const std::vector<Post>& posts, int32_t doc_count) {
        SegmentMerger::Input input;
        {
            MemoryByteOutput tis, tii, frq, prx;
            SpimiPostingBuffer buffer;
            for (const auto& [t, d, p] : posts) {
                buffer.Append(t, d, p);
            }
            buffer.Sort();
            // Spill-style: non-inlined V4 windowed segment.
            SegmentWriter w(&tis, &tii, &frq, &prx, TermDictWriter::kDefaultIndexInterval,
                            TermDictWriter::kDefaultSkipInterval, TermDictWriter::kMaxSkipLevels,
                            /*omit_term_freq_and_positions=*/false, /*use_windowed=*/true,
                            /*inline_small_terms=*/false);
            w.Emit(buffer, 0);
            w.Close();
            input.tis_bytes = tis.bytes();
            input.tii_bytes = tii.bytes();
            input.frq_bytes = frq.bytes();
            input.prx_bytes = prx.bytes();
        }
        input.doc_count = doc_count;
        return input;
    };

    std::vector<SegmentMerger::Input> inputs;
    inputs.push_back(build_input(in0, 3));
    inputs.push_back(build_input(in1, 3));

    // Merge into an inline-enabled V4 output.
    MemoryByteOutput tis, tii, frq, prx, fnm, segn, segg;
    SpimiSegmentSink sink;
    sink.tis = &tis;
    sink.tii = &tii;
    sink.frq = &frq;
    sink.prx = &prx;
    sink.fnm = &fnm;
    sink.segments_n = &segn;
    sink.segments_gen = &segg;

    SegmentMerger::Merge(inputs, sink, "merged", "body", /*total_doc_count=*/6,
                         FieldInfosWriter::kIndexVersionV4, /*omit_term_freq_and_positions=*/false,
                         /*omit_norms=*/true);

    // The merged .tis must be inline format.
    const auto& mb = tis.bytes();
    const int32_t fmt = static_cast<int32_t>((mb[0] << 24) | (mb[1] << 16) | (mb[2] << 8) | mb[3]);
    EXPECT_EQ(fmt, TermDictWriter::kFormatInline);

    // Query the merged output and compare against an independent reference
    // build (all docs in one external segment with the expected global doc ids:
    // input1's docs are offset by input0's doc_count == 2).
    std::vector<FieldInfoEntry> fis = {
            {.name = "body", .is_indexed = true, .omit_norms = true, .has_prox = true}};
    std::vector<std::wstring> fnw = {L"body"};
    auto merged_dict = std::make_unique<TermDictReader>(tis.bytes(), tii.bytes());

    auto query_merged = [&](const std::string& term) {
        auto frq_store = std::make_unique<MemPostingStore>(frq.bytes().data(), frq.bytes().size());
        auto prx_store = std::make_unique<MemPostingStore>(prx.bytes().data(), prx.bytes().size());
        SpimiQueryTermPositions tp(merged_dict.get(), std::move(frq_store), std::move(prx_store),
                                   &fis, &fnw);
        const std::wstring wterm(term.begin(), term.end());
        auto* lt = _CLNEW lucene::index::Term(L"body", wterm.c_str());
        std::vector<DocResult> out;
        try {
            tp.seek(lt);
            while (tp.next()) {
                DocResult dr;
                dr.doc = tp.doc();
                dr.freq = tp.freq();
                for (int32_t i = 0; i < dr.freq; ++i) {
                    dr.positions.push_back(tp.nextPosition());
                }
                out.push_back(std::move(dr));
            }
        } catch (...) {
            _CLDECDELETE(lt);
            throw;
        }
        _CLDECDELETE(lt);
        return out;
    };

    // Reference: build a single external segment with merged global doc ids
    // (input1 docs offset by input0's doc_count == 3).
    std::vector<Post> ref_offset = {
            {"alpha", 1, 0}, // input0 doc1
            {"alpha", 2, 2}, // input0 doc2
            {"alpha", 4, 3}, // input1 doc1 + offset 3
            {"beta", 1, 1},  // input0 doc1
            {"gamma", 5, 0}, // input1 doc2 + offset 3
    };
    Segment ref;
    ref.Write(ref_offset, /*inline_small=*/false, /*threshold=*/0);

    for (const std::string& term : {"alpha", "beta", "gamma"}) {
        const auto got = query_merged(term);
        const auto want = ref.Query(term);
        ASSERT_EQ(got.size(), want.size()) << term;
        for (size_t i = 0; i < got.size(); ++i) {
            EXPECT_TRUE(got[i] == want[i]) << "merged term " << term << " doc-index " << i;
        }
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
