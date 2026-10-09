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

#include "storage/index/snii/query/gram_boolean_query.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "storage/index/inverted/gram/gram_query.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii/writer/spimi_term_buffer.h"
#include "storage/index/snii_query_test_util.h"

using namespace doris::snii;
using doris::Status;
using doris::segment_v2::gram::GramQuery;

// R8/R24 (unity build): named rather than anonymous so these helpers cannot clash with the
// identically purposed helpers of the other query tests.
namespace gram_boolean_query_test_detail {

// A fake GramPostingSource: a map standing in for the "gram -> sorted docid list" dictionary, so
// the AND/OR/ALL/NONE evaluation logic of gram_boolean_query can be covered without building a
// real SNII index file. The counters record how the evaluator drives the source: dictionary
// rounds (df_batches) and posting rounds (posting_batches) are what a cold object-store read pays
// for, and `lookups` still counts individual grams so the "a missing gram costs a df lookup and
// no posting read" property stays asserted.
class MapPostingSource final : public query::GramPostingSource {
public:
    std::map<std::string, std::vector<uint32_t>> lists;
    // Grams present in the dictionary whose posting list the writer dropped (stop-gram).
    // The value is the df the entry still carries; there is no list to serve, and asking
    // for one is a bug the evaluator must not commit -- hence the fatal counter below.
    std::map<std::string, uint64_t> dropped;
    int lookups = 0;
    int df_batches = 0;
    int posting_batches = 0;
    int dropped_posting_reads = 0;

    Status dfs(const std::vector<std::string>& grams, std::vector<query::GramDf>* out) override {
        ++df_batches;
        out->assign(grams.size(), query::GramDf {});
        for (size_t i = 0; i < grams.size(); ++i) {
            ++lookups;
            const auto d = dropped.find(grams[i]);
            if (d != dropped.end()) {
                (*out)[i].found = true;
                (*out)[i].df = d->second;
                (*out)[i].matches_all = true;
                continue;
            }
            auto it = lists.find(grams[i]);
            if (it == lists.end()) {
                continue;
            }
            (*out)[i].found = true;
            (*out)[i].df = it->second.size();
        }
        return Status::OK();
    }

    // The no-IO arm of the gate. `budget` of 0 means the fake supplies none and the
    // evaluator falls back to the configured ratio, which is the pre-digest behaviour every
    // other case in this file exercises. `bounded` stands in for the segment's high-df
    // digest: a gram listed here has a df the source can state without reading anything.
    uint64_t budget = 0;
    std::map<std::string, uint64_t> bounded;

    uint64_t candidate_budget() const override { return budget; }

    void known_dfs(const std::vector<std::string>& grams, std::vector<uint64_t>* out,
                   std::vector<uint8_t>* known) const override {
        out->assign(grams.size(), 0);
        known->assign(grams.size(), 0);
        for (size_t i = 0; i < grams.size(); ++i) {
            const auto it = bounded.find(grams[i]);
            if (it != bounded.end()) {
                (*out)[i] = it->second;
                (*known)[i] = 1;
            }
        }
    }

    // Records an attempt to read a posting that does not exist. In production this is not
    // a silent wrong answer but a refusal: the planner rejects such a term outright.
    void note_if_dropped(const std::vector<std::string>& grams) {
        for (const std::string& gram : grams) {
            if (dropped.contains(gram)) {
                ++dropped_posting_reads;
            }
        }
    }

    Status and_postings(const std::vector<std::string>& grams, roaring::Roaring* out) override {
        ++posting_batches;
        note_if_dropped(grams);
        bool seeded = false;
        roaring::Roaring acc;
        for (const std::string& gram : grams) {
            auto it = lists.find(gram);
            if (it == lists.end()) {
                return Status::OK();
            }
            roaring::Roaring cur;
            cur.addMany(it->second.size(), it->second.data());
            if (!seeded) {
                acc = std::move(cur);
                seeded = true;
            } else {
                acc &= cur;
            }
        }
        *out |= acc;
        return Status::OK();
    }

    Status or_postings(const std::vector<std::string>& grams, roaring::Roaring* out) override {
        ++posting_batches;
        note_if_dropped(grams);
        for (const std::string& gram : grams) {
            auto it = lists.find(gram);
            if (it == lists.end()) {
                continue;
            }
            out->addMany(it->second.size(), it->second.data());
        }
        return Status::OK();
    }
};

std::vector<uint32_t> ToVec(const roaring::Roaring& r) {
    return {r.begin(), r.end()};
}

roaring::Roaring FullRange(uint32_t num_docs) {
    roaring::Roaring full;
    full.addRange(0, num_docs);
    return full;
}

// Pins BOTH knobs of the cost gate for one test: the ratio and the row floor below which the
// ratio is not applied at all. Tests that want the gate to fire have to lower the floor under
// their own segment size on purpose -- the production default (65536 rows) is far above every
// segment built here, so nothing in this file can trip the gate by accident. a bp value of 0 disables
// the gate outright, which is exactly the behaviour the evaluator had before it existed.
class ScopedCandidateRatio {
public:
    ScopedCandidateRatio(int32_t bp, int32_t min_rows)
            : _saved_bp(doris::config::gram_index_max_candidate_ratio_bp),
              _saved_min_rows(doris::config::gram_index_candidate_ratio_min_rows) {
        doris::config::gram_index_max_candidate_ratio_bp = bp;
        doris::config::gram_index_candidate_ratio_min_rows = min_rows;
    }
    ScopedCandidateRatio(const ScopedCandidateRatio&) = delete;
    ScopedCandidateRatio& operator=(const ScopedCandidateRatio&) = delete;
    ScopedCandidateRatio(ScopedCandidateRatio&&) = delete;
    ScopedCandidateRatio& operator=(ScopedCandidateRatio&&) = delete;
    ~ScopedCandidateRatio() {
        doris::config::gram_index_max_candidate_ratio_bp = _saved_bp;
        doris::config::gram_index_candidate_ratio_min_rows = _saved_min_rows;
    }

private:
    int32_t _saved_bp;
    int32_t _saved_min_rows;
};

// Row floor for tests that want the gate to fire: apply the ratio at any segment size.
constexpr int32_t kGateEverySize = 1;

GramQuery AndOf(const std::vector<std::string>& grams) {
    GramQuery q = GramQuery::of_gram(grams.front());
    for (size_t i = 1; i < grams.size(); ++i) {
        q = GramQuery::and_(std::move(q), GramQuery::of_gram(grams[i]));
    }
    return q;
}

GramQuery OrOf(const std::vector<std::string>& grams) {
    GramQuery q = GramQuery::of_gram(grams.front());
    for (size_t i = 1; i < grams.size(); ++i) {
        q = GramQuery::or_(std::move(q), GramQuery::of_gram(grams[i]));
    }
    return q;
}

// ---------------------------------------------------------------------------
// A real SNII segment for the cold-read I/O regression tests below. Every knob is set so that a
// read really costs a round trip -- SNII_DICT_RESIDENT_MAX=0 keeps the dictionary off-heap,
// target_dict_block_bytes=1 puts every gram in its own DICT block, and a 1-byte metered block
// makes any not-yet-read byte range a miss. Without that, a per-gram loop and a batched read are
// indistinguishable on a warm local file and the assertions would prove nothing.
// ---------------------------------------------------------------------------

constexpr uint32_t kIoDocCount = 8192;
constexpr uint64_t kIoIndexId = 1;
constexpr const char* kIoIndexSuffix = "body";

// Six dense grams: gram i covers every doc whose bit i is clear, so each has df 4096 while their
// intersection is only the multiples of 64 -- the shape a gram AND actually meets.
std::vector<std::string> DenseGrams() {
    return {"gaa", "gab", "gac", "gad", "gae", "gaf"};
}

// Six mid-frequency grams for the OR side.
std::vector<std::string> OrGrams() {
    return {"rba", "rbb", "rbc", "rbd", "rbe", "rbf"};
}

bool DenseGramHasDoc(size_t gram_index, uint32_t docid) {
    return ((docid >> gram_index) & 1U) == 0U;
}

bool OrGramHasDoc(size_t gram_index, uint32_t docid) {
    return docid % (13U + static_cast<uint32_t>(gram_index)) == 0U;
}

void WriteIoSegment(snii_test::MemoryFile* file) {
    writer::SpimiTermBuffer buf(/*has_positions=*/false);
    const std::vector<std::string> dense = DenseGrams();
    const std::vector<std::string> or_grams = OrGrams();
    for (uint32_t d = 0; d < kIoDocCount; ++d) {
        uint32_t pos = 0;
        for (size_t g = 0; g < dense.size(); ++g) {
            if (DenseGramHasDoc(g, d)) {
                buf.add_token(dense[g], d, pos++);
            }
        }
        for (size_t g = 0; g < or_grams.size(); ++g) {
            if (OrGramHasDoc(g, d)) {
                buf.add_token(or_grams[g], d, pos++);
            }
        }
    }

    writer::SniiIndexInput in;
    in.index_id = kIoIndexId;
    in.index_suffix = kIoIndexSuffix;
    in.config = format::IndexConfig::kDocsOnly;
    in.doc_count = kIoDocCount;
    in.terms = buf.finalize_sorted();
    // One DICT block per term, so resolving k grams touches k independent cold blocks.
    in.target_dict_block_bytes = 1;

    writer::SniiCompoundWriter compound(file);
    snii_test::assert_ok(compound.add_logical_index(in));
    snii_test::assert_ok(compound.finish());
}

roaring::Roaring DenseAndTruth() {
    roaring::Roaring truth;
    const size_t gram_count = DenseGrams().size();
    for (uint32_t d = 0; d < kIoDocCount; ++d) {
        bool all = true;
        for (size_t g = 0; g < gram_count && all; ++g) {
            all = DenseGramHasDoc(g, d);
        }
        if (all) {
            truth.add(d);
        }
    }
    return truth;
}

roaring::Roaring OrTruth() {
    roaring::Roaring truth;
    const size_t gram_count = OrGrams().size();
    for (uint32_t d = 0; d < kIoDocCount; ++d) {
        for (size_t g = 0; g < gram_count; ++g) {
            if (OrGramHasDoc(g, d)) {
                truth.add(d);
                break;
            }
        }
    }
    return truth;
}

// Serial rounds spent evaluating `q` from a cold cache. reset_metrics() clears both the counters
// and the modelled resident block set, so every measurement starts from the same cold state.
uint64_t ColdSerialRounds(io::MeteredFileReader* metered, const reader::LogicalIndexReader& idx,
                          const GramQuery& q, roaring::Roaring* out) {
    metered->reset_metrics();
    query::LogicalIndexPostingSource source(idx);
    EXPECT_TRUE(query::gram_boolean_query(source, q, kIoDocCount, out).ok());
    return metered->metrics().serial_rounds;
}

// The reference shape: resolve and read every gram on its own, the way a one-gram-at-a-time
// posting source does. The batched evaluator must stay well below this and, more importantly,
// must not grow with the gram count.
uint64_t ColdSerialRoundsPerGram(io::MeteredFileReader* metered,
                                 const reader::LogicalIndexReader& idx,
                                 const std::vector<std::string>& grams, bool intersect) {
    metered->reset_metrics();
    for (const std::string& gram : grams) {
        // A fresh source per gram models the old per-gram interface: no shared memo, so each gram
        // pays its own dictionary lookup plus its own posting read.
        query::LogicalIndexPostingSource source(idx);
        const std::vector<std::string> single = {gram};
        std::vector<query::GramDf> dfs;
        EXPECT_TRUE(source.dfs(single, &dfs).ok());
        EXPECT_EQ(dfs.size(), single.size());
        roaring::Roaring cur;
        EXPECT_TRUE(intersect ? source.and_postings(single, &cur).ok()
                              : source.or_postings(single, &cur).ok());
    }
    return metered->metrics().serial_rounds;
}

} // namespace gram_boolean_query_test_detail

using gram_boolean_query_test_detail::AndOf;
using gram_boolean_query_test_detail::ColdSerialRounds;
using gram_boolean_query_test_detail::ColdSerialRoundsPerGram;
using gram_boolean_query_test_detail::DenseAndTruth;
using gram_boolean_query_test_detail::DenseGrams;
using gram_boolean_query_test_detail::FullRange;
using gram_boolean_query_test_detail::kGateEverySize;
using gram_boolean_query_test_detail::kIoDocCount;
using gram_boolean_query_test_detail::kIoIndexId;
using gram_boolean_query_test_detail::kIoIndexSuffix;
using gram_boolean_query_test_detail::MapPostingSource;
using gram_boolean_query_test_detail::OrGrams;
using gram_boolean_query_test_detail::OrOf;
using gram_boolean_query_test_detail::OrTruth;
using gram_boolean_query_test_detail::ScopedCandidateRatio;
using gram_boolean_query_test_detail::ToVec;
using gram_boolean_query_test_detail::WriteIoSegment;

// The evaluation tests below run on the PRODUCTION defaults on purpose: a ten-doc query is
// orders of magnitude under config::gram_index_candidate_ratio_min_rows, so the cost gate is
// structurally unable to fire and these assertions cover the default configuration path. Only
// the gate's own tests lower the row floor to make it fire.
TEST(GramBooleanQueryTest, AndOrAllNone) {
    MapPostingSource src;
    src.lists["abc"] = {1, 2, 3, 7};
    src.lists["bcd"] = {2, 3, 9};
    src.lists["xyz"] = {7};

    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(
                        src, GramQuery::and_(GramQuery::of_gram("abc"), GramQuery::of_gram("bcd")),
                        10, &out)
                        .ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {2, 3}));

    out = roaring::Roaring();
    ASSERT_TRUE(query::gram_boolean_query(
                        src, GramQuery::or_(GramQuery::of_gram("bcd"), GramQuery::of_gram("xyz")),
                        10, &out)
                        .ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {2, 3, 7, 9}));

    out = roaring::Roaring();
    ASSERT_TRUE(query::gram_boolean_query(src, GramQuery::all(), 4, &out).ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {0, 1, 2, 3}));

    out = roaring::Roaring();
    ASSERT_TRUE(query::gram_boolean_query(src, GramQuery::none(), 4, &out).ok());
    EXPECT_TRUE(out.isEmpty());
}

TEST(GramBooleanQueryTest, MissingGramIsNoneAndEarlyExit) {
    MapPostingSource src;
    src.lists["abc"] = {1, 2, 3};

    roaring::Roaring out;
    auto q = GramQuery::and_(GramQuery::of_gram("abc"), GramQuery::of_gram("nope"));
    ASSERT_TRUE(query::gram_boolean_query(src, q, 10, &out).ok());
    EXPECT_TRUE(out.isEmpty());
    // A missing gram makes the whole AND empty after a df lookup alone, so no posting should be
    // read: one df lookup per gram, resolved in a single dictionary round, and no posting round.
    EXPECT_EQ(src.lookups, 2);
    EXPECT_EQ(src.df_batches, 1);
    EXPECT_EQ(src.posting_batches, 0);
}

TEST(GramBooleanQueryTest, NestedAndOfOr) {
    MapPostingSource src;
    src.lists["a"] = {1, 2, 3, 4};
    src.lists["b"] = {2};
    src.lists["c"] = {4, 5};

    roaring::Roaring out;
    auto q = GramQuery::and_(GramQuery::of_gram("a"),
                             GramQuery::or_(GramQuery::of_gram("b"), GramQuery::of_gram("c")));
    ASSERT_TRUE(query::gram_boolean_query(src, q, 10, &out).ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {2, 4}));
}

// Each query node must cost ONE dictionary round and ONE posting round no matter how many grams
// it holds. On object storage that is the difference between a constant and a linear number of
// remote round trips, so it is asserted on the fake source too, where it is exact.
TEST(GramBooleanQueryTest, EachNodeCostsOneDictAndOnePostingRound) {
    MapPostingSource src;
    src.lists["a"] = {1, 2, 3, 4, 5};
    src.lists["b"] = {2, 3, 4, 5};
    src.lists["c"] = {3, 4, 5};
    src.lists["d"] = {4, 5};

    roaring::Roaring out;
    auto q = GramQuery::and_(GramQuery::and_(GramQuery::of_gram("a"), GramQuery::of_gram("b")),
                             GramQuery::and_(GramQuery::of_gram("c"), GramQuery::of_gram("d")));
    ASSERT_TRUE(query::gram_boolean_query(src, q, 10, &out).ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {4, 5}));
    // and_() flattens the nested ANDs into a single four-gram node.
    EXPECT_EQ(src.df_batches, 1);
    EXPECT_EQ(src.posting_batches, 1);
    EXPECT_EQ(src.lookups, 4);

    src.df_batches = 0;
    src.posting_batches = 0;
    src.lookups = 0;
    out = roaring::Roaring();
    auto or_q = GramQuery::or_(GramQuery::or_(GramQuery::of_gram("a"), GramQuery::of_gram("b")),
                               GramQuery::or_(GramQuery::of_gram("c"), GramQuery::of_gram("d")));
    ASSERT_TRUE(query::gram_boolean_query(src, or_q, 10, &out).ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {1, 2, 3, 4, 5}));
    EXPECT_EQ(src.df_batches, 1);
    EXPECT_EQ(src.posting_batches, 1);
    EXPECT_EQ(src.lookups, 4);
}

// A single-gram AND has an exact candidate count before any posting read, so the gate fires on
// df alone and the posting read is skipped entirely.
TEST(GramBooleanQueryTest, CandidateRatioGateSkipsPostingReadForSingleGram) {
    MapPostingSource src;
    src.lists["hot"] = {0, 1, 2, 3, 4, 5, 6, 7}; // df 8 of 10 docs

    {
        ScopedCandidateRatio ratio(3000, kGateEverySize);
        roaring::Roaring out;
        ASSERT_TRUE(query::gram_boolean_query(src, GramQuery::of_gram("hot"), 10, &out).ok());
        // Giving up means "prune nothing", i.e. the whole docid space, never a smaller set.
        EXPECT_TRUE(out == FullRange(10));
        EXPECT_EQ(src.df_batches, 1);
        EXPECT_EQ(src.posting_batches, 0) << "the df bound alone must decide, before any IO";
    }

    src.df_batches = 0;
    src.posting_batches = 0;
    {
        // Gate disabled: exactly the pre-gate behaviour.
        ScopedCandidateRatio ratio(0, kGateEverySize);
        roaring::Roaring out;
        ASSERT_TRUE(query::gram_boolean_query(src, GramQuery::of_gram("hot"), 10, &out).ok());
        EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {0, 1, 2, 3, 4, 5, 6, 7}));
        EXPECT_EQ(src.posting_batches, 1);
    }
}

// A union is at least as large as its largest member, so max(df) gates an OR node before any
// posting -- or any sub-query -- is touched.
TEST(GramBooleanQueryTest, CandidateRatioGateSkipsPostingReadForOr) {
    MapPostingSource src;
    src.lists["hot"] = {0, 1, 2, 3, 4, 5}; // df 6 of 10 docs
    src.lists["cold"] = {9};

    {
        ScopedCandidateRatio ratio(3000, kGateEverySize);
        roaring::Roaring out;
        ASSERT_TRUE(query::gram_boolean_query(
                            src,
                            GramQuery::or_(GramQuery::of_gram("hot"), GramQuery::of_gram("cold")),
                            10, &out)
                            .ok());
        EXPECT_TRUE(out == FullRange(10));
        EXPECT_EQ(src.df_batches, 1);
        EXPECT_EQ(src.posting_batches, 0);
    }

    src.df_batches = 0;
    src.posting_batches = 0;
    {
        ScopedCandidateRatio ratio(7000, kGateEverySize);
        roaring::Roaring out;
        ASSERT_TRUE(query::gram_boolean_query(
                            src,
                            GramQuery::or_(GramQuery::of_gram("hot"), GramQuery::of_gram("cold")),
                            10, &out)
                            .ok());
        // 7 of 10 candidates is not above 70%, so the gate stays out of the way.
        EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {0, 1, 2, 3, 4, 5, 9}));
        EXPECT_EQ(src.posting_batches, 1);
    }
}

// A multi-gram AND is gated on min(df), before a single posting is read. min(df) bounds the node
// from above -- an intersection can never hold more docs than its rarest gram -- so a min(df)
// inside the budget lets the node through, and one above it means the node is only worth reading
// if the intersection collapses far below its rarest gram. Giving up there is deliberately
// pessimistic: measured against remote object storage, a node whose candidates stay above the
// budget cannot skip a single page of the column, so its posting reads are pure overhead.
//
// Whichever arm fires, giving up yields the full range, which is a superset of what the ungated
// evaluation returns -- not one matching row can be lost.
TEST(GramBooleanQueryTest, CandidateRatioGatePrejudgesMultiGramAndOnMinDf) {
    MapPostingSource src;
    src.lists["a"] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    src.lists["b"] = {0, 1, 2, 3, 4, 5, 6, 7, 9};

    roaring::Roaring gated;
    {
        // Both grams are far above the ratio, so min(df) is too, and the node is abandoned on the
        // df batch alone.
        ScopedCandidateRatio ratio(3000, kGateEverySize);
        ASSERT_TRUE(query::gram_boolean_query(
                            src, GramQuery::and_(GramQuery::of_gram("a"), GramQuery::of_gram("b")),
                            10, &gated)
                            .ok());
        EXPECT_EQ(src.posting_batches, 0)
                << "the gate must fire on the df batch, before paying for any posting read";
        EXPECT_TRUE(gated == FullRange(10)) << "giving up widens, it never narrows";
    }

    src.posting_batches = 0;
    roaring::Roaring ungated;
    {
        // Same node, gate off: the true candidate set. Every row of it is inside the gated result
        // above, which is what "giving up never drops a row" means.
        ScopedCandidateRatio ratio(0, kGateEverySize);
        ASSERT_TRUE(query::gram_boolean_query(
                            src, GramQuery::and_(GramQuery::of_gram("a"), GramQuery::of_gram("b")),
                            10, &ungated)
                            .ok());
        EXPECT_EQ(src.posting_batches, 1);
        EXPECT_EQ(ToVec(ungated), (std::vector<uint32_t> {0, 1, 2, 3, 4, 5, 6, 7}));
    }
    EXPECT_TRUE((ungated - gated).isEmpty())
            << "the gated result must contain every true candidate";
}

// The safe direction of the same rule: one rare gram is enough to carry a node through, however
// common its neighbours are. This is what keeps the gate from throwing away the selective patterns
// the index exists for -- a pattern worth an index is selective through its rarest gram.
TEST(GramBooleanQueryTest, CandidateRatioGateLetsARareGramCarryTheNode) {
    MapPostingSource src;
    src.lists["common"] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    src.lists["rare"] = {3};

    ScopedCandidateRatio ratio(3000, kGateEverySize);
    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(
                        src,
                        GramQuery::and_(GramQuery::of_gram("common"), GramQuery::of_gram("rare")),
                        10, &out)
                        .ok());
    EXPECT_EQ(src.posting_batches, 1) << "min(df) is inside the budget, so the node must proceed";
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {3}));
}

// The shape min(df) cannot see, and the one that made a CJK column's index worthless.
//
// min(df) bounds an AND from above, and for grams cut out of one ASCII literal that bound is
// close to the truth: the grams overlap almost completely, so the intersection really is about
// as big as the rarest of them. Nothing forces that. A CJK column indexes one term per code
// point, so a four-character literal is an AND of four ordinary characters -- each common on
// its own, sharing almost no documents beyond the rows that spell the literal. Measured on
// weibo: one four-character literal has 18 matching rows and its AND returns 28 candidates,
// while another has 5 and was abandoned outright, because its rarest character is still a
// common one.
//
// Below, four grams each cover 20% of the segment and their intersection is a single document.
// A gate reading min(df) sees 20%, exceeds a 15% budget and gives up on a node that returns one
// row.
TEST(GramBooleanQueryTest, TheGateReadsANodeWhoseCommonGramsIntersectRarely) {
    constexpr uint32_t kDocs = 2000;
    constexpr uint32_t kBlock = 399;
    MapPostingSource src;
    const char* names[4] = {"g0", "g1", "g2", "g3"};
    for (uint32_t g = 0; g < 4; g++) {
        std::vector<uint32_t> docs {0}; // the one document every gram shares
        for (uint32_t d = 0; d < kBlock; d++) {
            docs.push_back(1 + g * kBlock + d); // disjoint from every other gram's block
        }
        src.lists[names[g]] = docs;
    }

    ScopedCandidateRatio ratio(1500, kGateEverySize); // 15%; each gram alone is 20%
    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(
                        src,
                        GramQuery::and_(GramQuery::and_(GramQuery::and_(GramQuery::of_gram("g0"),
                                                                        GramQuery::of_gram("g1")),
                                                        GramQuery::of_gram("g2")),
                                        GramQuery::of_gram("g3")),
                        kDocs, &out)
                        .ok());
    EXPECT_EQ(src.posting_batches, 1)
            << "grams that are common individually can still intersect rarely; the gate must "
               "judge the intersection, not its largest possible size";
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {0}));
}

// The same mistake one level up. `(A|B)C` compiles to an AND whose direct gram leaves are the
// grams of C and whose sub-query is the alternation, and the gate judged such a node on the
// leaves alone. Measured on weibo with a four-character CJK literal `XY` (a two-character
// brand X followed by a two-character noun Y): `XY` returns 148 candidates for its 18 rows,
// while `(X|X')Y` -- 23 rows, a strictly smaller answer -- was abandoned with the whole
// segment as candidates, because the two characters of Y are common and the alternation
// that made the node selective was not looked at.
//
// A sub-query can only narrow an AND, and once evaluated its cardinality is an exact upper
// bound on the whole node, so the node below must be read however common its own leaves are.
TEST(GramBooleanQueryTest, TheGateCountsWhatASubQueryAlreadyNarrowedTo) {
    constexpr uint32_t kDocs = 2000;
    MapPostingSource src;
    // Two leaves covering half the segment each: on their own, far over the budget.
    std::vector<uint32_t> half;
    for (uint32_t d = 0; d < kDocs / 2; ++d) {
        half.push_back(d);
    }
    src.lists["common_x"] = half;
    src.lists["common_y"] = half;
    // The alternation is what makes the node selective: three documents in total, one of them
    // outside the common leaves so the leaves are shown to still take part in the answer.
    src.lists["rare_a"] = {1, 2};
    src.lists["rare_b"] = {1500};

    ScopedCandidateRatio ratio(1500, kGateEverySize); // 15%; each leaf alone is 50%
    roaring::Roaring out;
    const GramQuery node = GramQuery::and_(
            AndOf({"common_x", "common_y"}),
            GramQuery::or_(GramQuery::of_gram("rare_a"), GramQuery::of_gram("rare_b")));
    ASSERT_TRUE(query::gram_boolean_query(src, node, kDocs, &out).ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {1, 2}))
            << "the sub-query narrows the node to three documents, two of which hold the common "
               "leaves as well; abandoning it on the leaves alone throws that away";
}

// Every defect this gate has had looked the same from outside: the index was consulted and
// eliminated nothing. A profile could only show that indirectly -- a candidate count equal to
// the segment with zero rows filtered -- which a reader has to already suspect to go looking
// for. Reporting the decision makes "the index gave up here" a fact rather than an inference.
TEST(GramBooleanQuery, TheGateReportsWhenItGivesUp) {
    MapPostingSource src;
    src.lists["a"] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    src.lists["b"] = {0, 1, 2, 3, 4, 5, 6, 7, 9};
    src.lists["rare"] = {3};

    ScopedCandidateRatio ratio(3000, kGateEverySize);
    {
        query::GramGateStats stats;
        roaring::Roaring out;
        ASSERT_TRUE(query::gram_boolean_query(src, AndOf({"a", "b"}), 10, &out, &stats).ok());
        EXPECT_TRUE(out == FullRange(10));
        EXPECT_EQ(stats.nodes_given_up, 1U) << "the node the gate abandoned must be reported";
    }
    {
        // One rare gram carries the node, so nothing is abandoned and nothing is reported.
        query::GramGateStats stats;
        roaring::Roaring out;
        ASSERT_TRUE(query::gram_boolean_query(src, AndOf({"a", "rare"}), 10, &out, &stats).ok());
        EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {3}));
        EXPECT_EQ(stats.nodes_given_up, 0U) << "a node that was read must not be reported";
    }
}

// ---------------------------------------------------------------------------
// I/O regression: the number of serial (cold) read rounds a gram node costs must be a small
// constant, not a function of the gram count. Without these assertions the batching added here
// can silently regress back into a per-gram loop, which is invisible on a warm local file and
// fatal on object storage.
// ---------------------------------------------------------------------------

TEST(GramBooleanQueryIoTest, AndBatchesDictionaryAndPostingReads) {
    // Runs on the production gate defaults. This segment is below the configured row floor, so
    // the gate cannot fire and the full read path really is measured; assert that rather than
    // assume it, so lowering the default floor cannot silently turn this into a no-op.
    ASSERT_LT(kIoDocCount,
              static_cast<uint32_t>(doris::config::gram_index_candidate_ratio_min_rows));
    snii_test::ScopedEnv on_demand_dict("SNII_DICT_RESIDENT_MAX", "0");

    snii_test::MemoryFile file;
    WriteIoSegment(&file);
    if (::testing::Test::HasFatalFailure()) {
        return;
    }

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    reader::SniiSegmentReader segment;
    snii_test::assert_ok(reader::SniiSegmentReader::open(&metered, &segment));
    reader::LogicalIndexReader idx;
    snii_test::assert_ok(segment.open_index(kIoIndexId, kIoIndexSuffix, &idx));

    const std::vector<std::string> grams = DenseGrams();
    roaring::Roaring got;
    const uint64_t batched = ColdSerialRounds(&metered, idx, AndOf(grams), &got);
    EXPECT_TRUE(got == DenseAndTruth());

    // The same node with half the grams: what the extra three grams cost tells batching apart
    // from a per-gram loop far more reliably than any absolute number.
    const std::vector<std::string> half(grams.begin(), grams.begin() + 3);
    roaring::Roaring got_half;
    const uint64_t batched_half = ColdSerialRounds(&metered, idx, AndOf(half), &got_half);

    const uint64_t per_gram = ColdSerialRoundsPerGram(&metered, idx, grams, /*intersect=*/true);
    EXPECT_GT(per_gram, batched) << "batching must beat the one-gram-at-a-time shape";
    // Round budget for a k-gram AND: ONE batched dictionary wave + ONE batched prelude round +
    // at most one docid round per gram. That last term is the leapfrog conjunction shared with
    // boolean_and(): it is deliberately sequential, because each gram is only read over the
    // windows the grams before it left alive. The two fixed stages are what must never become
    // per-gram again -- a per-gram loop measures three rounds per gram (see per_gram).
    EXPECT_LE(batched, grams.size() + 2)
            << "a gram AND must pay ONE dictionary round and ONE prelude round for all grams "
               "together (per-gram reference was "
            << per_gram << ")";
    EXPECT_LE(batched - batched_half, grams.size() - half.size())
            << "each extra gram may add at most the one conjunction round it is read in, never "
               "its own dictionary and prelude rounds too";
}

TEST(GramBooleanQueryIoTest, OrBatchesDictionaryAndPostingReads) {
    // Production gate defaults; see AndBatchesDictionaryAndPostingReads for why the floor is
    // asserted rather than assumed.
    ASSERT_LT(kIoDocCount,
              static_cast<uint32_t>(doris::config::gram_index_candidate_ratio_min_rows));
    snii_test::ScopedEnv on_demand_dict("SNII_DICT_RESIDENT_MAX", "0");

    snii_test::MemoryFile file;
    WriteIoSegment(&file);
    if (::testing::Test::HasFatalFailure()) {
        return;
    }

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    reader::SniiSegmentReader segment;
    snii_test::assert_ok(reader::SniiSegmentReader::open(&metered, &segment));
    reader::LogicalIndexReader idx;
    snii_test::assert_ok(segment.open_index(kIoIndexId, kIoIndexSuffix, &idx));

    const std::vector<std::string> grams = OrGrams();
    roaring::Roaring got;
    const uint64_t batched = ColdSerialRounds(&metered, idx, OrOf(grams), &got);
    EXPECT_TRUE(got == OrTruth());

    const std::vector<std::string> half(grams.begin(), grams.begin() + 3);
    roaring::Roaring got_half;
    const uint64_t batched_half = ColdSerialRounds(&metered, idx, OrOf(half), &got_half);

    const uint64_t per_gram = ColdSerialRoundsPerGram(&metered, idx, grams, /*intersect=*/false);
    EXPECT_GT(per_gram, batched) << "batching must beat the one-gram-at-a-time shape";
    // A union has nothing to leapfrog, so it is fully batched: ONE dictionary round and ONE
    // posting round, whatever the gram count.
    EXPECT_EQ(batched, batched_half)
            << "a gram OR must not cost more rounds for six grams than for three (per-gram "
               "reference was "
            << per_gram << ")";
    EXPECT_LE(batched, 3U) << "a gram OR must cost a constant, small number of serial rounds";
}

// The gate exists to save exactly this: when the df bound already says the node cannot prune, the
// posting read never happens, and the result is the whole segment. Both knobs are pinned: this
// 8192-row segment is under the production row floor, so the gate is made to fire on purpose
// rather than by accident.
TEST(GramBooleanQueryIoTest, CandidateRatioGateSkipsPostingIo) {
    snii_test::ScopedEnv on_demand_dict("SNII_DICT_RESIDENT_MAX", "0");

    snii_test::MemoryFile file;
    WriteIoSegment(&file);
    if (::testing::Test::HasFatalFailure()) {
        return;
    }

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    reader::SniiSegmentReader segment;
    snii_test::assert_ok(reader::SniiSegmentReader::open(&metered, &segment));
    reader::LogicalIndexReader idx;
    snii_test::assert_ok(segment.open_index(kIoIndexId, kIoIndexSuffix, &idx));

    // One dense gram: df is 50% of the segment, so a 30% gate must give up on the df alone.
    const GramQuery q = GramQuery::of_gram(DenseGrams().front());

    roaring::Roaring ungated;
    uint64_t ungated_rounds = 0;
    {
        ScopedCandidateRatio ratio(0, kGateEverySize);
        ungated_rounds = ColdSerialRounds(&metered, idx, q, &ungated);
    }
    ASSERT_FALSE(ungated.isEmpty());

    roaring::Roaring gated;
    uint64_t gated_rounds = 0;
    {
        ScopedCandidateRatio ratio(3000, kGateEverySize);
        gated_rounds = ColdSerialRounds(&metered, idx, q, &gated);
    }
    EXPECT_TRUE(gated == FullRange(kIoDocCount)) << "giving up must widen to the whole segment";
    EXPECT_TRUE((ungated - gated).isEmpty()) << "giving up must never drop a candidate row";
    EXPECT_LT(gated_rounds, ungated_rounds) << "the gate must actually save the posting read";
}

// ---------------------------------------------------------------------------
// stop-gram: a gram present in the dictionary whose posting list the writer dropped.
//
// It matches every document, so it constrains nothing. The whole risk of the feature lives
// in one confusion: a dropped gram and an absent gram are both "no posting list to read",
// and they mean opposite things. Absent empties an AND; dropped leaves it untouched. These
// cases pin both directions, and every one of them also asserts that the evaluator never
// asks for the posting list that does not exist.
// ---------------------------------------------------------------------------

TEST(GramBooleanQuery, StopGramDropsOutOfAnAndWithoutChangingIt) {
    MapPostingSource src;
    src.lists["abc"] = {3, 7, 11, 40};
    src.lists["bcd"] = {7, 11, 90};
    src.dropped["the"] = 900; // in the dictionary, no posting list

    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(src, AndOf({"abc", "the", "bcd"}), 1000, &out).ok());
    // Identical to the same AND without the stop-gram: X AND ALL == X.
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {7, 11}));
    EXPECT_EQ(src.dropped_posting_reads, 0) << "a dropped posting must never be read";
}

TEST(GramBooleanQuery, AnAndOfOnlyStopGramsIsAll) {
    MapPostingSource src;
    src.dropped["the"] = 900;
    src.dropped["and"] = 850;

    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(src, AndOf({"the", "and"}), 1000, &out).ok());
    EXPECT_TRUE(out == FullRange(1000)) << "grams that constrain nothing leave the node at ALL";
    EXPECT_EQ(src.posting_batches, 0) << "nothing to read: no posting round at all";
    EXPECT_EQ(src.dropped_posting_reads, 0);
}

// The direction that matters most: dropped must not be treated as absent, which would
// empty the node and lose every matching row.
TEST(GramBooleanQuery, StopGramIsNotTreatedAsAMissingGram) {
    MapPostingSource present;
    present.lists["abc"] = {1, 2, 3};
    present.dropped["the"] = 900;
    roaring::Roaring dropped_out;
    ASSERT_TRUE(query::gram_boolean_query(present, AndOf({"abc", "the"}), 1000, &dropped_out).ok());
    EXPECT_EQ(ToVec(dropped_out), (std::vector<uint32_t> {1, 2, 3}));

    // Same query shape, but the second gram is genuinely absent: that IS empty.
    MapPostingSource absent;
    absent.lists["abc"] = {1, 2, 3};
    roaring::Roaring absent_out;
    ASSERT_TRUE(query::gram_boolean_query(absent, AndOf({"abc", "zzz"}), 1000, &absent_out).ok());
    EXPECT_TRUE(absent_out.isEmpty()) << "an absent gram still empties the AND";
}

TEST(GramBooleanQuery, AnOrHoldingAStopGramIsAll) {
    MapPostingSource src;
    src.lists["abc"] = {1, 2};
    src.dropped["the"] = 900;

    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(src, OrOf({"abc", "the"}), 1000, &out).ok());
    EXPECT_TRUE(out == FullRange(1000)) << "a union with a match-all member is everything";
    EXPECT_EQ(src.posting_batches, 0) << "the answer is known from the df batch alone";
    EXPECT_EQ(src.dropped_posting_reads, 0);
}

// A stop-gram inside an AND must not suppress the sub-queries, which still narrow the node.
TEST(GramBooleanQuery, StopGramLeavesSubQueriesInAnAndIntact) {
    MapPostingSource src;
    src.lists["abc"] = {1, 2, 3, 4};
    src.lists["bcd"] = {3, 4, 5};
    src.dropped["the"] = 900;

    GramQuery q = GramQuery::and_(GramQuery::of_gram("the"), OrOf({"abc", "bcd"}));
    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(src, q, 1000, &out).ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {1, 2, 3, 4, 5}));
    EXPECT_EQ(src.dropped_posting_reads, 0);
}

// The gate and the stop-gram meet on the same node: the surviving grams decide it, because
// a term that matches everything cannot be the rarest one and must not stand in for it.
TEST(GramBooleanQuery, TheGateJudgesAnAndOnItsConstrainingGramsOnly) {
    ScopedCandidateRatio ratio(/*bp=*/1000, kGateEverySize); // give up above 10% of the segment
    MapPostingSource src;
    src.lists["abc"] = {1, 2, 3}; // 0.3% of 1000 docs: well inside the budget
    src.dropped["the"] = 999;     // would blow any budget if it were counted

    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(src, AndOf({"abc", "the"}), 1000, &out).ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {1, 2, 3}))
            << "the rare gram still carries the node";
    EXPECT_EQ(src.dropped_posting_reads, 0);
}

// ---------------------------------------------------------------------------
// The no-IO arm of the cost gate.
//
// Reading a df costs a remote dictionary round trip -- measured at 40 seconds across a
// node's grams on a segment whose cache holds nothing -- and the gate often spends it only
// to conclude the node is not worth pruning. When the segment can bound those df values
// from its own resident digest, that conclusion is free. The direction of the bound is the
// whole correctness argument, and it differs between AND and OR.
// ---------------------------------------------------------------------------

TEST(GramBooleanQuery, AnAndOverBudgetOnKnownDfsGivesUpWithoutReadingAnything) {
    MapPostingSource src;
    src.lists["abc"] = {1, 2, 3};
    src.lists["bcd"] = {2, 3, 4};
    src.budget = 100;
    src.bounded["abc"] = 500; // both known, both far over budget
    src.bounded["bcd"] = 900;

    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(src, AndOf({"abc", "bcd"}), 10000, &out).ok());
    EXPECT_TRUE(out == FullRange(10000)) << "giving up must widen to the whole segment";
    EXPECT_EQ(src.df_batches, 0) << "the point is that no dictionary read happens at all";
    EXPECT_EQ(src.posting_batches, 0);
}

// The asymmetry that makes it correct: an AND is estimated from all its dfs, and the digest
// holds only the segment's common terms, so a gram it cannot state is a rare one -- possibly
// the one carrying the node. It must be read rather than assumed common.
TEST(GramBooleanQuery, AnAndWithOneUnknownDfStillReadsTheDictionary) {
    MapPostingSource src;
    src.lists["abc"] = {1, 2, 3};
    src.lists["rare"] = {7};
    src.budget = 100;
    src.bounded["abc"] = 5000; // known and over budget
    // "rare" is absent from the digest: unknown, and in fact rare enough to carry the node.

    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(src, AndOf({"abc", "rare"}), 10000, &out).ok());
    EXPECT_EQ(src.df_batches, 1) << "an unknown gram must be looked up, not assumed common";
    EXPECT_TRUE(out.isEmpty() || out.cardinality() <= 3)
            << "the node is decided by the rare gram, not widened to ALL";
}

// An OR is the mirror: it is at least as large as any one member, so a single known gram
// over budget settles it without the others being known at all.
TEST(GramBooleanQuery, AnOrGivesUpOnASingleKnownDfOverBudget) {
    MapPostingSource src;
    src.lists["abc"] = {1, 2};
    src.lists["unknown_one"] = {5};
    src.budget = 100;
    src.bounded["abc"] = 4000; // only this one is known

    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(src, OrOf({"abc", "unknown_one"}), 10000, &out).ok());
    EXPECT_TRUE(out == FullRange(10000));
    EXPECT_EQ(src.df_batches, 0) << "one known member over budget is enough";
}

// Known and under budget must not trigger anything: the node proceeds normally.
TEST(GramBooleanQuery, KnownDfsUnderBudgetDoNotGiveUp) {
    MapPostingSource src;
    src.lists["abc"] = {1, 2, 3};
    src.lists["bcd"] = {2, 3, 9};
    src.budget = 1000;
    src.bounded["abc"] = 3;
    src.bounded["bcd"] = 3;

    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(src, AndOf({"abc", "bcd"}), 10000, &out).ok());
    EXPECT_EQ(ToVec(out), (std::vector<uint32_t> {2, 3}));
    EXPECT_EQ(src.df_batches, 1) << "the node was worth reading, so it was read";
}

// A source with no digest (a segment written before it existed) must behave exactly as
// before: the configured ratio decides, and nothing is skipped on bounds that do not exist.
TEST(GramBooleanQuery, WithoutABudgetTheConfiguredRatioStillGoverns) {
    ScopedCandidateRatio ratio(/*bp=*/1000, kGateEverySize);
    MapPostingSource src;
    src.lists["abc"] = std::vector<uint32_t>(5000);
    for (uint32_t i = 0; i < 5000; ++i) {
        src.lists["abc"][i] = i;
    }
    src.budget = 0; // no digest

    roaring::Roaring out;
    ASSERT_TRUE(query::gram_boolean_query(src, AndOf({"abc"}), 10000, &out).ok());
    EXPECT_TRUE(out == FullRange(10000)) << "50% of the segment is far above a 10% ratio";
    EXPECT_EQ(src.df_batches, 1) << "without a digest the df must be read to decide";
}
