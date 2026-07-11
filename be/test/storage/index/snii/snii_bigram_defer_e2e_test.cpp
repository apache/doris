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

// SNII bigram-defer END-TO-END wiring tests (see
// config::snii_bigram_defer_build_to_compaction): real tablet, real rowset
// writers, real compaction -- the full `_opts.write_type == TYPE_DIRECT` ->
// `opts.is_direct_load` -> `set_direct_load(true)` chain that the writer-level
// tests cannot see. Written segments are probed with the raw SNII segment
// reader (sentinel / pair / unigram dict lookups), so the assertions pin the
// WRITE-side wiring without depending on the query read path:
//   1. Direct-load rowsets defer: no bigram sentinel, no pair terms -- through
//      BOTH production segment writers (VerticalSegmentWriter, the
//      enable_vertical_segment_writer default, AND the horizontal
//      SegmentWriter), covering both `write_type == TYPE_DIRECT` assignments.
//   2. Compaction rebuilds: the FULL-compaction output segment (written with
//      DataWriteType::TYPE_COMPACTION) carries the sentinel AND materializes
//      the high-df pair again -- the "compaction re-feeds every token" premise
//      the whole deferral rests on. An inverted wiring (`!=`, deferring
//      compaction forever) fails here.
//   3. With the config OFF a direct load still full-builds (guards an
//      accidentally-always-on deferral and proves the probes see sentinels on
//      exactly this path).
//   4. Variant subcolumn index writers inherit the marking through
//      `opts.is_direct_load` propagation in variant_column_writer_impl.cpp:
//      a direct-load variant rowset's subcolumn index defers, a config-off one
//      full-builds.

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset.h"
#include "testutil/index_storage_test_util.h"

namespace doris::index_storage_test {
namespace {

constexpr int32_t kTextColumnUid = 2;
constexpr int64_t kTextIndexId = 230001;
constexpr int32_t kVariantColumnUid = 2;
constexpr int64_t kVariantIndexId = 230002;

// What the raw SNII dict of one segment index says about the bigram build,
// plus the resident kPhraseBigramsDeferred capability flag: the flag and the
// postings must stay COHERENT across the segment lifecycle (deferred load:
// flag set + no sentinel; compacted / full build: flag clear + sentinel). A
// stale flag on a compacted segment would silently disable the bigram fast
// path forever with correct results -- only this assertion catches it.
struct SniiBigramProbe {
    bool sentinel = false;
    bool pair = false;
    bool hello = false;
    bool deferred_flag = false;
};

std::map<std::string, std::string> phrase_index_properties() {
    return {{"parser", "english"}, {"lower_case", "true"}, {"support_phrase", "true"}};
}

// 40 rows with the adjacent (hello, world) pair + 40 without: after compacting
// two such rowsets the pair's df is 80, inside the default prune survival
// window [auto floor 64, 2 x floor 128] for the 160-doc segment -- so the
// compacted full build PROVABLY materializes the pair (each 80-doc load
// segment alone stays under the floor, which is fine: deferred segments feed
// no pairs at all and the control test only asserts the sentinel).
std::vector<std::string> half_pair_rows(const std::string& tag) {
    std::vector<std::string> rows;
    rows.reserve(80);
    for (uint32_t i = 0; i < 40; ++i) {
        rows.push_back("hello world " + tag + std::to_string(i));
    }
    for (uint32_t i = 0; i < 40; ++i) {
        rows.push_back("alpha beta " + tag + std::to_string(i));
    }
    return rows;
}

class SniiBigramDeferE2eTest : public IndexStorageTestFixture {
protected:
    void SetUp() override {
        IndexStorageTestFixture::SetUp();
        _saved_defer = config::snii_bigram_defer_build_to_compaction;
        _saved_vertical_writer = config::enable_vertical_segment_writer;
    }

    void TearDown() override {
        config::snii_bigram_defer_build_to_compaction = _saved_defer;
        config::enable_vertical_segment_writer = _saved_vertical_writer;
        IndexStorageTestFixture::TearDown();
    }

    // The single-file SNII index sits next to the segment file:
    // {tablet_path}/{rowset_id}_{seg_id}.idx -- the same path derivation the
    // production writers used (get_index_file_path_prefix + _v2).
    std::string segment_index_path(const RowsetSharedPtr& rowset, int seg_id) const {
        const std::string seg_path = local_segment_path(tablet()->tablet_path(),
                                                        rowset->rowset_id().to_string(), seg_id);
        return segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(seg_path));
    }

    // Opens segment 0's logical index (index_id, suffix) with the raw SNII
    // reader and probes the dict for the bigram sentinel, the (hello, world)
    // pair, and the "hello" unigram. On any open failure the all-false probe is
    // returned with the failure recorded -- callers assert probe.hello == true
    // on every path, so a missing file / wrong suffix can never pass as "the
    // sentinel is absent".
    SniiBigramProbe probe_segment_index(const RowsetSharedPtr& rowset, int64_t index_id,
                                        const std::string& suffix) const {
        SniiBigramProbe probe;
        ::doris::snii::io::LocalFileReader file;
        ::doris::snii::reader::SniiSegmentReader segment;
        ::doris::snii::reader::LogicalIndexReader idx;
        const std::string path = segment_index_path(rowset, 0);
        if (Status st = file.open(path); !st.ok()) {
            ADD_FAILURE() << "open " << path << ": " << st.to_string();
            return probe;
        }
        if (Status st = ::doris::snii::reader::SniiSegmentReader::open(&file, &segment); !st.ok()) {
            ADD_FAILURE() << "segment open " << path << ": " << st.to_string();
            return probe;
        }
        if (Status st = segment.open_index(static_cast<uint64_t>(index_id), suffix, &idx);
            !st.ok()) {
            ADD_FAILURE() << "index (" << index_id << ", '" << suffix << "') in " << path << ": "
                          << st.to_string();
            return probe;
        }
        probe.sentinel =
                lookup_found(idx, ::doris::snii::format::make_phrase_bigram_sentinel_term());
        probe.pair =
                lookup_found(idx, ::doris::snii::format::make_phrase_bigram_term("hello", "world"));
        probe.hello = lookup_found(idx, "hello");
        probe.deferred_flag = idx.phrase_bigrams_deferred();
        return probe;
    }

private:
    static bool lookup_found(const ::doris::snii::reader::LogicalIndexReader& idx,
                             const std::string& term) {
        bool found = false;
        ::doris::snii::format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        EXPECT_TRUE(idx.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
        return found;
    }

    bool _saved_defer = false;
    bool _saved_vertical_writer = true;
};

// The core wiring round trip: two direct-load rowsets (one through the default
// VerticalSegmentWriter, one through the horizontal SegmentWriter) defer the
// bigram build; FULL compaction of the pair -- whose output segment is written
// under DataWriteType::TYPE_COMPACTION -- rebuilds sentinel and pairs.
TEST_F(SniiBigramDeferE2eTest, DirectLoadDefersBigramAndFullCompactionRebuilds) {
    config::snii_bigram_defer_build_to_compaction = true;

    IndexTabletOptions options;
    options.tablet_id = 130001;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.text_columns = {TextColumnSpec {.unique_id = kTextColumnUid, .name = "title"}};
    options.inverted_indexes.push_back(IndexSpec::column_index(
            kTextIndexId, "idx_title", kTextColumnUid, phrase_index_properties()));
    ASSERT_TRUE(create_tablet(options).ok());

    // Rowset v0: FORCE the vertical writer (do not trust the ambient default:
    // if the environment started with it false, both rowsets would take the
    // horizontal path and the VerticalSegmentWriter assignment went untested).
    config::enable_vertical_segment_writer = true;
    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_text(half_pair_rows("wiki"), 0));
    auto rowset0_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset0_result.has_value()) << rowset0_result.error();

    // Rowset v1: the horizontal SegmentWriter path (segment_writer.cpp
    // assignment) -- some deployments run with the vertical writer disabled.
    config::enable_vertical_segment_writer = false;
    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_text(half_pair_rows("doc"), 100));
    auto rowset1_result = write_rowset(rowset1);
    config::enable_vertical_segment_writer = true;
    ASSERT_TRUE(rowset1_result.has_value()) << rowset1_result.error();

    // Both load segments deferred: no sentinel, no pair, unigrams intact, and
    // the resident capability flag SET (flag/postings coherence, load side).
    for (const auto& rowset : {rowset0_result.value(), rowset1_result.value()}) {
        ASSERT_EQ(rowset->num_segments(), 1);
        const SniiBigramProbe probe = probe_segment_index(rowset, kTextIndexId, "");
        EXPECT_FALSE(probe.sentinel) << rowset->rowset_id().to_string();
        EXPECT_FALSE(probe.pair) << rowset->rowset_id().to_string();
        EXPECT_TRUE(probe.hello) << rowset->rowset_id().to_string();
        EXPECT_TRUE(probe.deferred_flag) << rowset->rowset_id().to_string();
    }

    // FULL compaction re-feeds every token under TYPE_COMPACTION: the output
    // segment must carry the sentinel again AND materialize the now-df-80
    // pair -- the deferred work actually got done, not just re-declared.
    auto compacted = compact_rowsets(IndexCompactionKind::FULL,
                                     {rowset0_result.value(), rowset1_result.value()});
    ASSERT_TRUE(compacted.has_value()) << compacted.error();
    ASSERT_NE(compacted.value(), nullptr);
    ASSERT_EQ(compacted.value()->num_rows(), 160);
    ASSERT_EQ(compacted.value()->num_segments(), 1);
    const SniiBigramProbe compacted_probe =
            probe_segment_index(compacted.value(), kTextIndexId, "");
    EXPECT_TRUE(compacted_probe.sentinel);
    EXPECT_TRUE(compacted_probe.pair);
    EXPECT_TRUE(compacted_probe.hello);
    // Flag/postings coherence, compaction side: a stale deferred flag next to
    // rebuilt pairs would permanently disable the bigram fast path (correct
    // results, invisible perf regression) -- it must be CLEARED here.
    EXPECT_FALSE(compacted_probe.deferred_flag);
}

// Config off: a direct load keeps the full bigram build (sentinel present).
// Guards an accidentally-always-on deferral and proves the probe machinery
// sees sentinels on exactly the load path the test above asserts them absent.
TEST_F(SniiBigramDeferE2eTest, ConfigOffDirectLoadKeepsFullBigramBuild) {
    config::snii_bigram_defer_build_to_compaction = false;

    IndexTabletOptions options;
    options.tablet_id = 130002;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.text_columns = {TextColumnSpec {.unique_id = kTextColumnUid, .name = "title"}};
    options.inverted_indexes.push_back(IndexSpec::column_index(
            kTextIndexId, "idx_title", kTextColumnUid, phrase_index_properties()));
    ASSERT_TRUE(create_tablet(options).ok());

    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_text(half_pair_rows("base"), 0));
    auto rowset_result = write_rowset(rowset0);
    ASSERT_TRUE(rowset_result.has_value()) << rowset_result.error();
    ASSERT_EQ(rowset_result.value()->num_segments(), 1);

    const SniiBigramProbe probe = probe_segment_index(rowset_result.value(), kTextIndexId, "");
    EXPECT_TRUE(probe.sentinel);
    EXPECT_TRUE(probe.hello);
    EXPECT_FALSE(probe.deferred_flag);
}

// Variant subcolumn wiring: the direct-load marking reaches subcolumn index
// writers only through the `opts.is_direct_load` propagation in
// variant_column_writer_impl.cpp -- a dropped copy there loses the deferral
// for every variant load while nothing else fails. The predefined path
// "content" carries a field-pattern phrase index; its logical index is keyed
// by the escaped full path ("v.content" with '.' as %2E).
TEST_F(SniiBigramDeferE2eTest, VariantSubcolumnIndexFollowsDirectLoadDeferral) {
    config::snii_bigram_defer_build_to_compaction = true;

    VariantColumnSpec variant;
    variant.unique_id = kVariantColumnUid;
    variant.name = "v";
    variant.predefined_paths = {VariantPathSpec {.path = "content",
                                                 .type = FieldType::OLAP_FIELD_TYPE_STRING,
                                                 .nullable = true,
                                                 .pattern_type = PatternTypePB::MATCH_NAME,
                                                 .array_item_type = {},
                                                 .array_item_nullable = true}};
    IndexSpec index_spec = IndexSpec::field_pattern_index(kVariantIndexId, "idx_v_content",
                                                          kVariantColumnUid, "content");
    for (const auto& [key, value] : phrase_index_properties()) {
        index_spec.properties[key] = value;
    }

    IndexTabletOptions options;
    options.tablet_id = 130003;
    options.index_storage_format = InvertedIndexStorageFormatPB::SNII;
    options.variant_columns = {variant};
    options.inverted_indexes.push_back(index_spec);
    ASSERT_TRUE(create_tablet(options).ok());

    std::vector<std::string> jsons;
    jsons.reserve(80);
    for (uint32_t i = 0; i < 80; ++i) {
        jsons.push_back(R"({"content": "hello world item)" + std::to_string(i) + R"("})");
    }

    // Escaped-by-production-rules suffix of the subcolumn's logical index:
    // escape_for_path_name("v.content") -- '.' escapes to %2E.
    const std::string suffix = "v%2Econtent";

    // Direct load with the config on: the subcolumn index defers.
    IndexRowsetSpec rowset0;
    rowset0.version = 0;
    rowset0.batches.push_back(IndexBatch::single_variant(jsons, 0));
    auto defer_rowset = write_rowset(rowset0);
    ASSERT_TRUE(defer_rowset.has_value()) << defer_rowset.error();
    ASSERT_EQ(defer_rowset.value()->num_segments(), 1);
    const SniiBigramProbe defer_probe =
            probe_segment_index(defer_rowset.value(), kVariantIndexId, suffix);
    EXPECT_FALSE(defer_probe.sentinel);
    EXPECT_FALSE(defer_probe.pair);
    EXPECT_TRUE(defer_probe.hello);
    EXPECT_TRUE(defer_probe.deferred_flag);

    // Config off, same data: the subcolumn index full-builds (positive control
    // for the probe and for the propagation's false direction).
    config::snii_bigram_defer_build_to_compaction = false;
    IndexRowsetSpec rowset1;
    rowset1.version = 1;
    rowset1.batches.push_back(IndexBatch::single_variant(jsons, 100));
    auto full_rowset = write_rowset(rowset1);
    ASSERT_TRUE(full_rowset.has_value()) << full_rowset.error();
    ASSERT_EQ(full_rowset.value()->num_segments(), 1);
    const SniiBigramProbe full_probe =
            probe_segment_index(full_rowset.value(), kVariantIndexId, suffix);
    EXPECT_TRUE(full_probe.sentinel);
    EXPECT_TRUE(full_probe.hello);
    EXPECT_FALSE(full_probe.deferred_flag);
}

} // namespace
} // namespace doris::index_storage_test
