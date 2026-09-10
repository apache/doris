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

// What does building the high-df digest cost the writer?
//
// The digest earns its keep on the query side -- it lets the cost gate bound a node without
// the remote dictionary reads that were measured at 40 seconds on a cold segment. That is
// only a good trade if writing it is close to free, and "close to free" has to be a number
// rather than an assertion.
//
// The two builds below index the identical corpus with the identical settings and differ in
// exactly one field: whether a gram scheme is present, which is what decides that a digest
// is built at all. Everything else -- postings, dictionary blocks, term hashes, statistics
// -- is byte-for-byte the same work, so the difference in elapsed time is the digest.
//
// The shape of the corpus matters to what is measured. Term frequencies follow a Zipf-like
// curve, as a real vocabulary does, so most terms fall below the digest's floor and cost
// only the one comparison that rejects them, while a small head clears it and pays for a
// bounded-heap insertion. Measuring on a flat corpus would either exercise no heap at all
// or exercise nothing but the heap; neither resembles a real index.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <string>
#include <vector>

#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/logical_index_writer.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii::writer {
namespace {

using snii_test::assert_ok;
using snii_test::make_term;
using snii_test::MemoryFile;
using snii_test::PostingDoc;

constexpr uint32_t kDocCount = 200000;
constexpr uint32_t kTermCount = 40000;
constexpr int kRounds = 5;

// A Zipf-like vocabulary over kDocCount documents: term i appears in roughly
// kTermCount / (i + 1) documents, so a few hundred terms clear the digest floor
// (kDocCount / kHighDfDigestDivisor = 100) and the rest sit far below it.
std::vector<TermPostings> BuildCorpus() {
    std::vector<TermPostings> terms;
    terms.reserve(kTermCount);
    for (uint32_t i = 0; i < kTermCount; i++) {
        const uint32_t df = std::max<uint32_t>(1, kTermCount / (i + 1));
        const uint32_t stride = std::max<uint32_t>(1, kDocCount / df);
        std::vector<PostingDoc> docs;
        docs.reserve(df);
        for (uint32_t docid = 0; docid < kDocCount && docs.size() < df; docid += stride) {
            docs.push_back(PostingDoc {.docid = docid, .positions = {}});
        }
        // Zero-padded so the terms are already in lexicographic order.
        char name[32];
        snprintf(name, sizeof(name), "term_%06u", i);
        terms.push_back(make_term(name, std::move(docs)));
    }
    return terms;
}

// One build of the whole index. with_digest selects it by supplying a gram scheme, which is
// the only thing that differs between the two arms.
double BuildOnceMs(const std::vector<TermPostings>& corpus, bool with_digest,
                   uint64_t* digest_entries) {
    SniiIndexInput input;
    input.index_id = 31;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = kDocCount;
    input.terms = corpus;
    if (with_digest) {
        segment_v2::gram::GramScheme scheme;
        scheme.mode = segment_v2::gram::GramMode::SPARSE;
        scheme.min_len = 3;
        scheme.max_len = 4;
        scheme.density_permille = 250;
        input.gram_scheme = scheme;
    }

    MemoryFile file;
    SniiCompoundWriter writer(&file);
    const auto start = std::chrono::steady_clock::now();
    assert_ok(writer.add_logical_index(input));
    assert_ok(writer.finish());
    const auto end = std::chrono::steady_clock::now();

    if (digest_entries != nullptr) {
        reader::SniiSegmentReader segment;
        assert_ok(reader::SniiSegmentReader::open(&file, &segment));
        reader::LogicalIndexReader index;
        assert_ok(segment.open_index(31, "body", &index));
        *digest_entries = index.high_df_terms().term_hash.size();
    }
    return std::chrono::duration<double, std::milli>(end - start).count();
}

double Median(std::vector<double> values) {
    std::ranges::sort(values);
    return values[values.size() / 2];
}

} // namespace

// Reports the cost; asserts only that it is not a large fraction of the build, so the case
// records a number without becoming a tripwire for ordinary timing noise.
TEST(SniiHighDfDigestCost, BuildingTheDigestIsANegligibleShareOfTheWrite) {
    const std::vector<TermPostings> corpus = BuildCorpus();

    uint64_t entries = 0;
    std::vector<double> with_digest;
    std::vector<double> without;
    for (int round = 0; round < kRounds; round++) {
        // Alternate the order so a warming effect cannot favour one arm systematically.
        if (round % 2 == 0) {
            with_digest.push_back(BuildOnceMs(corpus, true, &entries));
            without.push_back(BuildOnceMs(corpus, false, nullptr));
        } else {
            without.push_back(BuildOnceMs(corpus, false, nullptr));
            with_digest.push_back(BuildOnceMs(corpus, true, &entries));
        }
    }

    const double on = Median(with_digest);
    const double off = Median(without);
    const double overhead_pct = 100.0 * (on - off) / off;
    // Spread of the two arms, so a reader can see whether the difference above is signal.
    const double on_spread =
            *std::ranges::max_element(with_digest) - *std::ranges::min_element(with_digest);
    const double off_spread =
            *std::ranges::max_element(without) - *std::ranges::min_element(without);

    printf("\n%u docs, %u terms, digest holds %llu entries\n", kDocCount, kTermCount,
           static_cast<unsigned long long>(entries));
    printf("build with digest    : %8.1f ms (median of %d, spread %.1f ms)\n", on, kRounds,
           on_spread);
    printf("build without digest : %8.1f ms (median of %d, spread %.1f ms)\n", off, kRounds,
           off_spread);
    printf("difference           : %+8.1f ms  (%+.2f%%)\n", on - off, overhead_pct);
    printf("per term             : %+8.3f us\n", 1000.0 * (on - off) / kTermCount);
    // The work itself, which is deterministic and does not depend on the machine: one
    // comparison for every term, and a bounded-heap insertion only for those above the
    // floor. Reported alongside the timing because it is the number that stays true.
    printf("work                 : %u comparisons + at most %llu heap ops (log2 K <= %.1f)\n",
           kTermCount, static_cast<unsigned long long>(entries),
           entries > 0 ? std::log2(static_cast<double>(entries)) : 0.0);

    EXPECT_GT(entries, 0U) << "the corpus must actually populate the digest";
    EXPECT_LT(overhead_pct, 5.0)
            << "digest build cost " << overhead_pct << "% of the write; it is meant to be "
            << "one comparison per term plus a bounded heap insertion for the few that pass";
}

} // namespace doris::snii::writer
