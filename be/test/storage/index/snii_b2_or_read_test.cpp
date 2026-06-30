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

// SNII Batch 2 -- multi-term OR read-amplification + streaming docid-union (T09).
//
// Covers three reader-only, byte-identical changes:
//   (1) emit_docid_union streams each posting into a dedup-capable sink (Roaring)
//       over ONE shared fetch round -- dense-full windows stay runs via
//       append_range -- instead of materializing a per-term vector + K-way merge.
//   (2) union_sorted_many reserves by summed input size (single allocation),
//       capped by reserve_cap so heavily-overlapping inputs do not over-reserve.
//   (3) the OR resolve path threads one request-scoped DictBlockCache through its
//       per-term lookups, so terms sharing a DICT block read+decode it once.
//
// All assertions are deterministic (op-counts, capacities, set equality, I/O round
// counts through MeteredFileReader / MemoryFile). No wall-clock gates.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/format/per_index_meta.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/docid_sink.h"
#include "storage/index/snii/query/internal/docid_set_ops.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii_query_test_util.h"

using namespace doris::snii;
using namespace doris::snii::snii_test;
using doris::Status;

namespace qi = doris::snii::query::internal;

namespace {

// A dedup-capable sink (dedups()==true) that records how each posting was handed
// off: append_range for run-preserving dense windows, append_sorted otherwise. The
// collected docids land in a std::set so equality checks are order-independent
// (a real Roaring sink also dedups/orders internally).
class CountingDedupSink final : public query::DocIdSink {
public:
    Status append_sorted(std::span<const uint32_t> docids) override {
        ++sorted_calls;
        for (uint32_t docid : docids) {
            ids.insert(docid);
        }
        return Status::OK();
    }

    Status append_range(uint32_t first, uint64_t last_exclusive) override {
        ++range_calls;
        for (uint64_t docid = first; docid < last_exclusive; ++docid) {
            ids.insert(static_cast<uint32_t>(docid));
        }
        return Status::OK();
    }

    bool dedups() const override { return true; }

    std::set<uint32_t> ids;
    size_t sorted_calls = 0;
    size_t range_calls = 0;
};

std::vector<uint32_t> closed_range(uint32_t begin, uint32_t end_exclusive) {
    std::vector<uint32_t> out;
    out.reserve(end_exclusive - begin);
    for (uint32_t docid = begin; docid < end_exclusive; ++docid) {
        out.push_back(docid);
    }
    return out;
}

// Opens the standard 9000-doc fixture over a MeteredFileReader so I/O rounds and
// remote GETs can be measured. build_reader() writes the index bytes into `file`;
// a fresh segment/index is then re-opened over a metered wrapper of those bytes.
struct MeteredIndex {
    MemoryFile file;
    std::unique_ptr<io::MeteredFileReader> metered;
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader idx;
};

void OpenMeteredFixture(MeteredIndex* fx, size_t block_size) {
    reader::SniiSegmentReader seg0;
    reader::LogicalIndexReader idx0;
    assert_ok(build_reader(&fx->file, &seg0, &idx0));
    fx->metered = std::make_unique<io::MeteredFileReader>(&fx->file, block_size);
    assert_ok(reader::SniiSegmentReader::open(fx->metered.get(), &fx->segment));
    assert_ok(fx->segment.open_index(7, "Body", &fx->idx));
}

} // namespace

// ---------------------------------------------------------------------------
// T09 F16 -- union_sorted_many reserves by total, capped by reserve_cap.
// ---------------------------------------------------------------------------

TEST(SniiB2OrRead, UnionReservesByTotalForDisjointLists) {
    // Three disjoint sorted lists (sum == 12); union == sum.
    const std::vector<std::vector<uint32_t>> lists = {{0, 3, 6, 9}, {1, 4, 7, 10}, {2, 5, 8, 11}};
    const size_t total = 12;

    const std::vector<uint32_t> got = qi::union_sorted_many(lists);

    EXPECT_EQ(got.size(), total);
    // reserve(total) + exactly `total` pushes -> a single allocation, so capacity is
    // exactly total (was geometric growth off reserve(largest)).
    EXPECT_EQ(got.capacity(), total);
    EXPECT_TRUE(std::ranges::is_sorted(got));
}

TEST(SniiB2OrRead, UnionRespectsReserveCapOnHeavyOverlap) {
    // >8 identical lists -> heap path; total == 40 but the union is only 4 elements.
    const std::vector<std::vector<uint32_t>> lists(10, std::vector<uint32_t> {1, 2, 3, 4});
    const size_t union_size = 4;

    const std::vector<uint32_t> got = qi::union_sorted_many(lists, /*reserve_cap=*/union_size);

    EXPECT_EQ(got.size(), union_size);
    // Capped at reserve_cap rather than over-reserving 10x (= 40) on overlap.
    EXPECT_EQ(got.capacity(), union_size);
    EXPECT_EQ(got, (std::vector<uint32_t> {1, 2, 3, 4}));
}

TEST(SniiB2OrRead, UnionContentUnchangedByReserveFix) {
    const std::vector<std::vector<uint32_t>> lists = {{0, 2, 4, 6, 8}, {1, 3, 5}, {4, 5, 6, 7}};
    std::set<uint32_t> expected_set;
    for (const std::vector<uint32_t>& list : lists) {
        expected_set.insert(list.begin(), list.end());
    }
    const std::vector<uint32_t> want(expected_set.begin(), expected_set.end());

    EXPECT_EQ(qi::union_sorted_many(lists), want);
}

// ---------------------------------------------------------------------------
// T09 F15 -- dedups() capability gate + streaming OR.
// ---------------------------------------------------------------------------

TEST(SniiB2OrRead, DedupCapabilityGate) {
    std::vector<uint32_t> backing;
    query::VectorDocIdSink vector_sink(backing);
    EXPECT_FALSE(vector_sink.dedups()) << "plain vector sink needs materialize+merge";

    CountingDedupSink dedup_sink;
    EXPECT_TRUE(dedup_sink.dedups()) << "Roaring-style sink dedups/orders natively";
}

TEST(SniiB2OrRead, MultiTermOrPreservesDenseRangeToDedupSink) {
    MemoryFile file;
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader idx;
    assert_ok(build_reader(&file, &segment, &idx));

    CountingDedupSink sink;
    assert_ok(query::boolean_or(idx, {"failed", "sparse_left"}, &sink));

    // "failed" is a dense full posting (docids 0..8999): its dense-full windows must
    // stream in via append_range (run-preserving), not be expanded element-by-element
    // through a merge accumulator -- the old path issued only append_sorted(acc).
    EXPECT_GE(sink.range_calls, 1u);

    // failed covers every doc, so the union is the full doc range.
    const std::vector<uint32_t> got(sink.ids.begin(), sink.ids.end());
    EXPECT_EQ(got, closed_range(0, 9000));
}

TEST(SniiB2OrRead, MultiTermOrStreamingMatchesMergePath) {
    MemoryFile file;
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader idx;
    assert_ok(build_reader(&file, &segment, &idx));

    const std::vector<std::string> terms = {"needle", "sparse_left", "driver"};

    // Streaming path: dedup-capable sink -> emit_docid_postings_streamed.
    CountingDedupSink sink;
    assert_ok(query::boolean_or(idx, terms, &sink));
    const std::vector<uint32_t> streamed(sink.ids.begin(), sink.ids.end());

    // Merge path: vector out -> build_docid_union + union_sorted_many.
    std::vector<uint32_t> merged;
    assert_ok(query::boolean_or(idx, terms, &merged));

    EXPECT_TRUE(std::ranges::is_sorted(merged));
    EXPECT_FALSE(merged.empty());
    EXPECT_EQ(streamed, merged) << "streaming OR must yield the identical docid set";
}

TEST(SniiB2OrRead, NonDedupSinkFallsBackToMergeContract) {
    MemoryFile file;
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader idx;
    assert_ok(build_reader(&file, &segment, &idx));

    const std::vector<std::string> terms = {"needle", "sparse_left"};

    // A non-dedup sink (VectorDocIdSink) routed through the sink overload must keep
    // the merge path so its single globally-sorted-deduplicated span contract holds.
    std::vector<uint32_t> via_sink;
    query::VectorDocIdSink vector_sink(via_sink);
    assert_ok(query::boolean_or(idx, terms, &vector_sink));

    std::vector<uint32_t> via_vector;
    assert_ok(query::boolean_or(idx, terms, &via_vector));

    EXPECT_TRUE(std::ranges::is_sorted(via_sink));
    EXPECT_EQ(via_sink, via_vector);
}

TEST(SniiB2OrRead, SingleTermAndBoundaryInputs) {
    MemoryFile file;
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader idx;
    assert_ok(build_reader(&file, &segment, &idx));

    // Single resolved term -> exact known docids.
    CountingDedupSink single;
    assert_ok(query::boolean_or(idx, {"needle"}, &single));
    EXPECT_EQ((std::vector<uint32_t>(single.ids.begin(), single.ids.end())),
              (std::vector<uint32_t> {100, 101, 102, 6000}));

    // Empty terms -> OK, sink untouched.
    CountingDedupSink empty;
    assert_ok(query::boolean_or(idx, std::vector<std::string> {}, &empty));
    EXPECT_TRUE(empty.ids.empty());

    // All-miss term -> OK, sink untouched (resolve skips absent terms).
    CountingDedupSink miss;
    assert_ok(query::boolean_or(idx, {"zzz_absent_term"}, &miss));
    EXPECT_TRUE(miss.ids.empty());

    // Null sink -> InvalidArgument.
    query::DocIdSink* null_sink = nullptr;
    const Status st = query::boolean_or(idx, {"failed"}, null_sink);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<doris::ErrorCode::INVALID_ARGUMENT>()) << st.to_string();
}

// ---------------------------------------------------------------------------
// Read amplification -- one shared fetch round for the multi-term OR postings.
// ---------------------------------------------------------------------------

TEST(SniiB2OrRead, MultiTermOrIssuesSingleSerialRound) {
    MeteredIndex fx;
    OpenMeteredFixture(&fx, /*block_size=*/256);
    const std::vector<std::string> terms = {"failed", "driver", "sparse_left"};

    // Per-term baseline: each term resolved + read on its own -> its own fetch round.
    fx.metered->reset_metrics();
    for (const std::string& term : terms) {
        std::vector<uint32_t> docs;
        assert_ok(query::term_query(fx.idx, term, &docs));
    }
    const io::IoMetrics per_term = fx.metered->metrics();

    // Batched OR: one shared fetch round for all postings' docid reads.
    fx.metered->reset_metrics();
    std::vector<uint32_t> got;
    assert_ok(query::boolean_or(fx.idx, terms, &got));
    const io::IoMetrics batched = fx.metered->metrics();

    EXPECT_EQ(batched.serial_rounds, 1u) << "multi-term OR must read all postings in one round";
    EXPECT_GE(per_term.serial_rounds, 2u);
    EXPECT_GT(per_term.serial_rounds, batched.serial_rounds);
    // Coalescing never increases physical GETs or bytes vs the per-term path.
    EXPECT_LE(batched.range_gets, per_term.range_gets);
    EXPECT_LE(batched.remote_bytes, per_term.remote_bytes);

    // Result set is unchanged: failed(0..8999) covers driver(0..7999) and sparse_left.
    EXPECT_EQ(got, closed_range(0, 9000));
}

TEST(SniiB2OrRead, MultiTermOrDedupsSharedDictBlockReads) {
    // Force on-demand DICT blocks so each lookup reads its block from the file
    // (resident DICT blocks would serve lookups from memory and hide the effect).
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");

    MemoryFile file;
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader idx;
    assert_ok(build_reader(&file, &segment, &idx));

    const format::RegionRef dict = idx.section_refs().dict_region;
    const auto dict_reads = [&]() {
        size_t count = 0;
        for (const MemoryFile::Read& r : file.reads()) {
            if (r.offset >= dict.offset && r.offset < dict.offset + dict.length) {
                ++count;
            }
        }
        return count;
    };

    const std::vector<std::string> terms = {"123",         "almost",       "driver",  "failed",
                                            "needle",      "order",        "ordinal", "repeat",
                                            "sparse_left", "sparse_right", "trace"};

    // Per-term baseline: term_query resolves each DICT block on its own (cache=null)
    // -> one DICT-region read per term.
    file.clear_reads();
    for (const std::string& term : terms) {
        std::vector<uint32_t> docs;
        assert_ok(query::term_query(idx, term, &docs));
    }
    const size_t per_term_dict_reads = dict_reads();

    // Multi-term OR threads one request-scoped DictBlockCache through resolve, so each
    // unique DICT block is read + decoded once for the whole OR.
    file.clear_reads();
    std::vector<uint32_t> got;
    assert_ok(query::boolean_or(idx, terms, &got));
    const size_t or_dict_reads = dict_reads();

    EXPECT_EQ(per_term_dict_reads, terms.size());
    EXPECT_GE(or_dict_reads, 1u);
    EXPECT_LT(or_dict_reads, per_term_dict_reads)
            << "OR must not re-read a DICT block already decoded for another term";
    EXPECT_FALSE(got.empty());
}
