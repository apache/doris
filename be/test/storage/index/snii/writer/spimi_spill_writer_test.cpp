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

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"

using namespace doris::snii;
using namespace doris::snii::format;
using namespace doris::snii::writer;
using doris::Status;

namespace {

std::string TempPath() {
    static int counter = 0;
    return "/tmp/snii_spill_test_" + std::to_string(getpid()) + "_" + std::to_string(counter++) +
           ".idx";
}

std::vector<uint8_t> ReadAll(const std::string& path) {
    io::LocalFileReader r;
    EXPECT_TRUE(r.open(path).ok());
    std::vector<uint8_t> out;
    EXPECT_TRUE(r.read_at(0, r.size(), &out).ok());
    return out;
}

// Deterministic (term, doc, pos) stream with globally ascending docids. Mixes
// high-df ("alpha", every doc), mid-df, multi-token docs, and a term whose docs
// straddle arbitrary spill boundaries -- so the spill path exercises a term in
// one run, a term in every run, and a spill boundary mid-term.
void Feed(SpimiTermBuffer* buf, uint32_t doc_count) {
    for (uint32_t d = 0; d < doc_count; ++d) {
        buf->add_token("alpha", d, 0); // every doc (spans all runs)
        buf->add_token("alpha", d, 7); // freq 2 in every doc
        if (d % 2 == 0) {
            buf->add_token("beta", d, 1);
        }
        if (d % 3 == 0) {
            buf->add_token("gamma", d, 2);
        }
        if (d % 5 == 0) {
            buf->add_token("delta", d, 3);
            buf->add_token("delta", d, 9);
        }
        if (d == 1) {
            buf->add_token("singleton", d, 4); // only one doc, one run
        }
    }
}

SniiIndexInput BaseInput(uint32_t doc_count) {
    SniiIndexInput in;
    in.index_id = 1;
    in.index_suffix = "body";
    in.config = IndexConfig::kDocsPositions;
    in.doc_count = doc_count;
    in.target_dict_block_bytes = 512; // force several DICT blocks
    return in;
}

std::vector<uint8_t> WriteContainer(const SniiIndexInput& in) {
    const std::string path = TempPath();
    io::LocalFileWriter writer;
    EXPECT_TRUE(writer.open(path).ok());
    SniiCompoundWriter compound(&writer);
    EXPECT_TRUE(compound.add_logical_index(in).ok());
    EXPECT_TRUE(compound.finish().ok());
    std::vector<uint8_t> bytes = ReadAll(path);
    std::remove(path.c_str());
    return bytes;
}

// Builds a container by STREAMING a freshly-fed buffer at the given spill
// threshold (0 == unlimited), returning the container bytes.
std::vector<uint8_t> BuildStreamed(uint32_t docs, size_t spill_bytes) {
    SpimiTermBuffer buf(/*has_positions=*/true, spill_bytes);
    Feed(&buf, docs);
    SniiIndexInput in = BaseInput(docs);
    in.term_source = &buf;
    return WriteContainer(in);
}

// Opens a container from bytes and runs a couple of queries for a sanity match.
struct OpenedIndex {
    std::string path;
    io::LocalFileReader local;
    std::unique_ptr<io::MeteredFileReader> metered;
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;

    ~OpenedIndex() {
        if (!path.empty()) {
            std::remove(path.c_str());
        }
    }
};

void OpenFromBytes(const std::vector<uint8_t>& bytes, OpenedIndex* out) {
    out->path = TempPath();
    io::LocalFileWriter w;
    ASSERT_TRUE(w.open(out->path).ok());
    ASSERT_TRUE(w.append(Slice(bytes)).ok());
    ASSERT_TRUE(w.finalize().ok());
    ASSERT_TRUE(out->local.open(out->path).ok());
    out->metered = std::make_unique<io::MeteredFileReader>(&out->local);
    ASSERT_TRUE(reader::SniiSegmentReader::open(out->metered.get(), &out->segment).ok());
    ASSERT_TRUE(out->segment.open_index(1, "body", &out->index).ok());
}

std::vector<uint32_t> TermDocs(OpenedIndex* idx, const std::string& term) {
    std::vector<uint32_t> docs;
    const Status s = query::term_query(idx->index, term, &docs);
    EXPECT_TRUE(s.ok()) << "term=" << term << " err=" << s.to_string();
    return docs;
}

} // namespace

// CORE GUARANTEE: building the SAME corpus with a tiny spill threshold (forcing
// many spills + a final k-way merge) yields a container that is BYTE-FOR-BYTE
// identical to the unlimited in-memory build.
TEST(SniiSpimiSpillWriter, SpilledMatchesUnlimitedBytes) {
    constexpr uint32_t kDocs = 400;
    const std::vector<uint8_t> unlimited = BuildStreamed(kDocs, /*spill=*/0);
    // ~4 KiB threshold forces dozens of spills across this corpus.
    const std::vector<uint8_t> spilled = BuildStreamed(kDocs, /*spill=*/4096);

    ASSERT_EQ(unlimited.size(), spilled.size());
    EXPECT_EQ(unlimited, spilled);
}

// Regression: the wide term "alpha" (every doc, freq 2) can have a doc split across
// a spill seam, so its PRE-coalesce total_docs reaches >= kSlimDfThreshold (512)
// while its POST-coalesce df stays below it. The merge gate keyed on total_docs
// would then STREAM positions (leaving positions_flat empty) into the slim writer
// path, which reads positions_flat directly -> segfault. Sweeping docs around the
// 512 boundary x several spill thresholds exercises the band; byte-identity vs the
// unlimited build proves both no-crash and identical output.
TEST(SniiSpimiSpillWriter, WideTermDfNearThresholdAcrossSeamMatchesUnlimited) {
    for (uint32_t docs = 505; docs <= 515; ++docs) {
        const std::vector<uint8_t> unlimited = BuildStreamed(docs, /*spill=*/0);
        for (size_t spill : {size_t {1024}, size_t {2048}, size_t {4096}}) {
            const std::vector<uint8_t> spilled = BuildStreamed(docs, spill);
            ASSERT_EQ(unlimited.size(), spilled.size()) << "docs=" << docs << " spill=" << spill;
            EXPECT_EQ(unlimited, spilled) << "docs=" << docs << " spill=" << spill;
        }
    }
}

// Identical bytes regardless of threshold size: a mid threshold (one or two
// spills) must also match the unlimited build exactly.
TEST(SniiSpimiSpillWriter, MidThresholdAlsoIdentical) {
    constexpr uint32_t kDocs = 400;
    const std::vector<uint8_t> unlimited = BuildStreamed(kDocs, /*spill=*/0);
    const std::vector<uint8_t> mid = BuildStreamed(kDocs, /*spill=*/32 * 1024);
    EXPECT_EQ(unlimited, mid);
}

// An extremely small threshold (spill almost every token) still produces the
// identical container -- stresses many single-term runs and the merge.
TEST(SniiSpimiSpillWriter, ExtremeSpillIdentical) {
    constexpr uint32_t kDocs = 200;
    const std::vector<uint8_t> unlimited = BuildStreamed(kDocs, /*spill=*/0);
    const std::vector<uint8_t> tiny = BuildStreamed(kDocs, /*spill=*/1);
    EXPECT_EQ(unlimited, tiny);
}

// No-positions config with spilling still matches the in-memory build.
TEST(SniiSpimiSpillWriter, NoPositionsSpillIdentical) {
    constexpr uint32_t kDocs = 300;
    auto build = [&](size_t spill) {
        SpimiTermBuffer buf(/*has_positions=*/false, spill);
        for (uint32_t d = 0; d < kDocs; ++d) {
            buf.add_token("alpha", d, 0);
            if (d % 2 == 0) {
                buf.add_token("beta", d, 0);
            }
            if (d % 4 == 0) {
                buf.add_token("gamma", d, 0);
            }
        }
        SniiIndexInput in = BaseInput(kDocs);
        in.config = IndexConfig::kDocsOnly; // docids only (no positions section)
        in.term_source = &buf;
        return WriteContainer(in);
    };
    EXPECT_EQ(build(0), build(2048));
}

// Queries against the spilled and unlimited containers return identical results.
// Uses >= kSlimDfThreshold docs so the high-df term is windowed (populating the
// .prx POD) -- the segment reader infers has_positions from a non-empty prx POD.
TEST(SniiSpimiSpillWriter, QueriesMatchAcrossSpill) {
    constexpr uint32_t kDocs = 700;
    OpenedIndex un, sp;
    OpenFromBytes(BuildStreamed(kDocs, 0), &un);
    OpenFromBytes(BuildStreamed(kDocs, 4096), &sp);

    for (const char* term : {"alpha", "beta", "gamma", "delta", "singleton", "absent"}) {
        EXPECT_EQ(TermDocs(&un, term), TermDocs(&sp, term)) << "term=" << term;
    }
    std::vector<uint32_t> un_phrase, sp_phrase;
    ASSERT_TRUE(query::phrase_query(un.index, {"alpha", "alpha"}, &un_phrase).ok());
    ASSERT_TRUE(query::phrase_query(sp.index, {"alpha", "alpha"}, &sp_phrase).ok());
    EXPECT_EQ(un_phrase, sp_phrase);
}

// WIDE-TERM STREAMED MERGE byte-identity: with enough docs that the high-df term
// "alpha" exceeds kSlimDfThreshold, the spilled k-way merge takes the WIDE-term
// position-STREAMING path (pos_pump pulled across runs) instead of materializing
// the term's full positions_flat. The produced container MUST stay byte-for-byte
// identical to the unlimited in-memory build (which materializes positions). This
// is the direct regression guard for the merge-phase peak-RSS streaming change.
TEST(SniiSpimiSpillWriter, WideTermStreamedMergeMatchesUnlimitedBytes) {
    // 1200 docs -> alpha df=1200 (>= kSlimDfThreshold 512) -> windowed + streamed.
    constexpr uint32_t kDocs = 1200;
    static_assert(kDocs >= doris::snii::format::kSlimDfThreshold, "alpha must be a wide term");
    const std::vector<uint8_t> unlimited = BuildStreamed(kDocs, /*spill=*/0);
    // Small threshold forces many spills, so alpha lands in EVERY run and the merge
    // coalesces it across runs via the streamed pump.
    const std::vector<uint8_t> spilled = BuildStreamed(kDocs, /*spill=*/8192);
    ASSERT_EQ(unlimited.size(), spilled.size());
    EXPECT_EQ(unlimited, spilled);
}

// Single-doc corpus with spilling: an edge corpus must not crash or diverge.
TEST(SniiSpimiSpillWriter, SingleDocCorpus) {
    SpimiTermBuffer un(/*has_positions=*/true, 0);
    SpimiTermBuffer sp(/*has_positions=*/true, 1);
    for (auto* b : {&un, &sp}) {
        b->add_token("only", 0, 0);
        b->add_token("only", 0, 1);
        b->add_token("word", 0, 2);
    }
    SniiIndexInput un_in = BaseInput(1);
    un_in.term_source = &un;
    SniiIndexInput sp_in = BaseInput(1);
    sp_in.term_source = &sp;
    EXPECT_EQ(WriteContainer(un_in), WriteContainer(sp_in));
}

// finalize_sorted (the materialized accessor) also reflects spilled runs and is
// byte-identical to the in-memory result at the postings level.
TEST(SniiSpimiSpillWriter, FinalizeSortedMatchesAcrossSpill) {
    constexpr uint32_t kDocs = 150;
    SpimiTermBuffer un(/*has_positions=*/true, 0);
    SpimiTermBuffer sp(/*has_positions=*/true, 256);
    Feed(&un, kDocs);
    Feed(&sp, kDocs);
    const std::vector<TermPostings> a = un.finalize_sorted();
    const std::vector<TermPostings> b = sp.finalize_sorted();
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        EXPECT_EQ(a[i].term, b[i].term);
        EXPECT_EQ(a[i].docids, b[i].docids);
        EXPECT_EQ(a[i].freqs, b[i].freqs);
        EXPECT_EQ(a[i].positions_flat, b[i].positions_flat);
    }
    EXPECT_TRUE(un.status().ok());
    EXPECT_TRUE(sp.status().ok());
}

// Gate-2 trigger now compares REAL resident bytes (pool_.arena_bytes() +
// slot_of_.capacity()*4) against the configured cap, NOT the old per-token
// estimate. With a small cap, the first 32 KiB arena block immediately exceeds it,
// so at least one spill fires; the unlimited (cap=0) build never spills.
TEST(SniiSpimiSpillWriter, ArenaByteCapTriggersSpill) {
    constexpr uint32_t kDocs = 400;

    SpimiTermBuffer capped(/*has_positions=*/true, /*spill=*/4096);
    Feed(&capped, kDocs);
    SniiIndexInput capped_in = BaseInput(kDocs);
    capped_in.term_source = &capped;
    const std::vector<uint8_t> capped_bytes = WriteContainer(capped_in);
    EXPECT_TRUE(capped.status().ok());
    // A spill ran: real resident (>= one 32 KiB block) crossed the 4 KiB cap.
    EXPECT_GE(capped.run_count_for_test(), 1U);

    SpimiTermBuffer unlimited(/*has_positions=*/true, /*spill=*/0);
    Feed(&unlimited, kDocs);
    SniiIndexInput unlimited_in = BaseInput(kDocs);
    unlimited_in.term_source = &unlimited;
    const std::vector<uint8_t> unlimited_bytes = WriteContainer(unlimited_in);
    EXPECT_TRUE(unlimited.status().ok());
    // Unlimited never spills (corpus fits well under the arena hard-stop).
    EXPECT_EQ(unlimited.run_count_for_test(), 0U);

    // And the metric switch did not change the output: byte-for-byte identical.
    ASSERT_EQ(capped_bytes.size(), unlimited_bytes.size());
    EXPECT_EQ(capped_bytes, unlimited_bytes);
}
