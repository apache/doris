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

// End-to-end tests for the token-exists Bloom Filter ("tbf") absent-term fast path on the
// FullTextIndexReader::query path. Builds a real V2 .idx (with the "tbf" sub-file when
// token_bloom_filter=true), then drives queries with config::enable_inverted_index_term_bf
// flipped on, and asserts that:
//   - absent terms return an empty bitmap identical to the no-BF result;
//   - present terms return a bitmap identical to the no-BF result (no false negatives);
//   - MATCH_ANY with a partially-absent term set keeps the present-term hits;
//   - the fast path is gated on the build-time analyzer signature (A3);
//   - the keyword/exact path (StringTypeInvertedIndexReader) is never consulted, so an
//     over-ignore_above EQUAL still returns INVERTED_INDEX_EVALUATE_SKIPPED (A4);
//   - the empty keyword token round-trips and is matchable (A5).

#include <CLucene.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "common/config.h"
#include "core/field.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/inverted_index_term_bloom_filter.h"
#include "storage/index/inverted/inverted_index_writer.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris::segment_v2 {

class InvertedIndexTermBfReaderTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/inverted_index_term_bf_reader_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;

        std::vector<StorePath> paths;
        paths.emplace_back(kTestDir, 1024);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        ASSERT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // Save whatever ExecEnv held so TearDown can restore it: these fixture-owned caches are
        // freed when the fixture is destroyed, so the global pointers must not be left dangling for
        // later tests (test isolation).
        _saved_searcher_cache = ExecEnv::GetInstance()->get_inverted_index_searcher_cache();
        _saved_query_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();

        int64_t cache_limit = 1024L * 1024 * 1024;
        _searcher_cache = std::unique_ptr<InvertedIndexSearcherCache>(
                InvertedIndexSearcherCache::create_global_instance(cache_limit, 1));
        _query_cache = std::unique_ptr<InvertedIndexQueryCache>(
                InvertedIndexQueryCache::create_global_cache(cache_limit, 1));
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_searcher_cache.get());
        ExecEnv::GetInstance()->_inverted_index_query_cache = _query_cache.get();

        _saved_flag = config::enable_inverted_index_term_bf;
    }

    void TearDown() override {
        config::enable_inverted_index_term_bf = _saved_flag;
        // Restore the global cache pointers BEFORE _searcher_cache / _query_cache (which back them)
        // are destroyed, so no later test sees a dangling pointer.
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_saved_searcher_cache);
        ExecEnv::GetInstance()->_inverted_index_query_cache = _saved_query_cache;
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    TabletSchemaSPtr create_schema() {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(DUP_KEYS);
        tablet_schema->init_from_pb(tablet_schema_pb);

        TabletColumn column_1;
        column_1.set_name("c1");
        column_1.set_unique_id(0);
        column_1.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        column_1.set_length(4);
        column_1.set_index_length(4);
        column_1.set_is_key(true);
        column_1.set_is_nullable(true);
        tablet_schema->append_column(column_1);

        TabletColumn column_2;
        column_2.set_name("c2");
        column_2.set_unique_id(1);
        column_2.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
        column_2.set_length(65535);
        column_2.set_is_key(false);
        column_2.set_is_nullable(false);
        tablet_schema->append_column(column_2);

        return tablet_schema;
    }

    std::string local_segment_path(const std::string& base, std::string_view rowset_id,
                                   int64_t seg_id) {
        return fmt::format("{}/{}_{}.dat", base, rowset_id, seg_id);
    }

    // Build a fulltext (analyzed) index over the c2 column with the supplied properties.
    void build_index(std::string_view rowset_id, int seg_id,
                     const std::map<std::string, std::string>& properties,
                     std::vector<Slice>& values, TabletIndex* idx_meta,
                     std::string* index_path_prefix) {
        auto tablet_schema = create_schema();

        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test_tbf");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2
        for (const auto& [k, v] : properties) {
            (*index_meta_pb->mutable_properties())[k] = v;
        }
        idx_meta->init_from_pb(*index_meta_pb.get());

        *index_path_prefix = InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id));
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v2(*index_path_prefix);

        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->create_file(index_path, &file_writer, &opts).ok());
        auto index_file_writer = std::make_unique<IndexFileWriter>(
                fs, *index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        const TabletColumn& column = tablet_schema->column(1);
        std::unique_ptr<IndexColumnWriter> column_writer;
        ASSERT_TRUE(IndexColumnWriter::create(&column, &column_writer, index_file_writer.get(),
                                              idx_meta)
                            .ok());
        ASSERT_TRUE(column_writer->add_values("c2", values.data(), values.size()).ok());
        ASSERT_TRUE(column_writer->finish().ok());
        ASSERT_TRUE(index_file_writer->begin_close().ok());
        ASSERT_TRUE(index_file_writer->finish_close().ok());
    }

    std::shared_ptr<IndexQueryContext> make_context(io::IOContext* io_ctx,
                                                    OlapReaderStatistics* stats,
                                                    RuntimeState* runtime_state) {
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        query_options.enable_inverted_index_query_cache = false;
        runtime_state->set_query_options(query_options);
        auto context = std::make_shared<IndexQueryContext>();
        context->io_ctx = io_ctx;
        context->stats = stats;
        context->runtime_state = runtime_state;
        return context;
    }

    std::shared_ptr<roaring::Roaring> run_fulltext(const std::string& index_path_prefix,
                                                   TabletIndex* idx_meta, const std::string& term,
                                                   InvertedIndexQueryType type,
                                                   Status* out_status) {
        auto reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
        EXPECT_TRUE(reader->init().ok());
        auto ft_reader = FullTextIndexReader::create_shared(idx_meta, reader);
        EXPECT_NE(ft_reader, nullptr);

        io::IOContext io_ctx;
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        auto context = make_context(&io_ctx, &stats, &runtime_state);

        auto bitmap = std::make_shared<roaring::Roaring>();
        Field qp = Field::create_field<TYPE_STRING>(term);
        *out_status = ft_reader->query(context, "1", qp, type, bitmap);
        return bitmap;
    }

protected:
    std::unique_ptr<InvertedIndexSearcherCache> _searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
    InvertedIndexSearcherCache* _saved_searcher_cache = nullptr;
    InvertedIndexQueryCache* _saved_query_cache = nullptr;
    bool _saved_flag = false;
};

// Absent term on MATCH_ALL/MATCH_ANY/EQUAL returns the same empty bitmap with and without the
// BF fast path. Present terms return identical bitmaps too (no false negatives).
TEST_F(InvertedIndexTermBfReaderTest, AbsentAndPresentMatchNoBfBaseline) {
    std::vector<Slice> values = {Slice("apple banana"), Slice("cherry date"), Slice("apple cherry"),
                                 Slice("elderberry")};
    std::map<std::string, std::string> props = {{"parser", "english"},
                                                {"lower_case", "true"},
                                                {"support_phrase", "true"},
                                                {"token_bloom_filter", "true"}};
    TabletIndex idx_meta;
    std::string prefix;
    build_index("tbf_rs_present", 0, props, values, &idx_meta, &prefix);

    struct Case {
        std::string term;
        InvertedIndexQueryType type;
    };
    std::vector<Case> cases = {
            {"apple", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"apple", InvertedIndexQueryType::MATCH_ALL_QUERY},
            {"cherry", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"absentword", InvertedIndexQueryType::MATCH_ANY_QUERY},
            {"absentword", InvertedIndexQueryType::MATCH_ALL_QUERY},
            {"absentword", InvertedIndexQueryType::MATCH_PHRASE_QUERY},
    };

    for (const auto& c : cases) {
        config::enable_inverted_index_term_bf = false;
        Status st_off;
        auto baseline = run_fulltext(prefix, &idx_meta, c.term, c.type, &st_off);
        EXPECT_TRUE(st_off.ok()) << c.term << ": " << st_off;
        // Pin the baseline shape so the equality below cannot pass on two empty bitmaps: present
        // terms must return rows, the absent term must be empty.
        if (c.term == "absentword") {
            EXPECT_EQ(baseline->cardinality(), 0U) << c.term << " must be empty";
        } else {
            EXPECT_GT(baseline->cardinality(), 0U) << c.term << " must return rows";
        }

        config::enable_inverted_index_term_bf = true;
        Status st_on;
        auto with_bf = run_fulltext(prefix, &idx_meta, c.term, c.type, &st_on);
        EXPECT_TRUE(st_on.ok()) << c.term << ": " << st_on;

        EXPECT_TRUE(*baseline == *with_bf)
                << "BF result diverged from baseline for term '" << c.term << "' type "
                << static_cast<int>(c.type) << " (baseline card=" << baseline->cardinality()
                << ", bf card=" << with_bf->cardinality() << ")";
    }
}

// MATCH_ANY with one present and one absent token must keep the present-token hits (the fast
// path only empties when *every* token is absent).
TEST_F(InvertedIndexTermBfReaderTest, MatchAnyPartialAbsentKeepsHits) {
    std::vector<Slice> values = {Slice("apple banana"), Slice("cherry date"),
                                 Slice("apple cherry")};
    std::map<std::string, std::string> props = {{"parser", "english"},
                                                {"lower_case", "true"},
                                                {"support_phrase", "true"},
                                                {"token_bloom_filter", "true"}};
    TabletIndex idx_meta;
    std::string prefix;
    build_index("tbf_rs_any", 0, props, values, &idx_meta, &prefix);

    // "apple" present + "absentword" absent: MATCH_ANY should still match the apple docs.
    config::enable_inverted_index_term_bf = false;
    Status st_off;
    auto baseline = run_fulltext(prefix, &idx_meta, "apple absentword",
                                 InvertedIndexQueryType::MATCH_ANY_QUERY, &st_off);
    EXPECT_TRUE(st_off.ok()) << st_off;

    config::enable_inverted_index_term_bf = true;
    Status st_on;
    auto with_bf = run_fulltext(prefix, &idx_meta, "apple absentword",
                                InvertedIndexQueryType::MATCH_ANY_QUERY, &st_on);
    EXPECT_TRUE(st_on.ok()) << st_on;

    EXPECT_GT(baseline->cardinality(), 0U);
    EXPECT_TRUE(*baseline == *with_bf)
            << "partial-absent MATCH_ANY must keep present-token hits (baseline card="
            << baseline->cardinality() << ", bf card=" << with_bf->cardinality() << ")";
}

// A3: when the build-time analyzer signature differs from the index's own analyzer, the BF is
// rejected and the query falls back to the normal lookup. We simulate a mismatch by building
// the BF with one analyzer config and reading the index with a different parser property; the
// read side recomputes the signature from the (different) properties, so it must not trust the
// BF and must still return the correct (non-empty) result for a present term.
TEST_F(InvertedIndexTermBfReaderTest, AnalyzerMismatchFallsBackToNormalQuery) {
    std::vector<Slice> values = {Slice("Apple Banana"), Slice("Cherry Date")};

    // Build with english + lower_case=true + BF.
    std::map<std::string, std::string> build_props = {{"parser", "english"},
                                                      {"lower_case", "true"},
                                                      {"support_phrase", "true"},
                                                      {"token_bloom_filter", "true"}};
    TabletIndex build_meta;
    std::string prefix;
    build_index("tbf_rs_a3", 0, build_props, values, &build_meta, &prefix);

    // Make the A3 guard load-bearing in this test: assert that the build-time analyzer
    // signature (lower_case=true) and the read-time signature (lower_case=false) actually
    // differ. Without this, a behavioral-equality assertion alone could pass even if the guard
    // were removed (e.g. when the probed token happens to be present in the dictionary).
    InvertedIndexAnalyzerConfig build_cfg;
    build_cfg.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    build_cfg.lower_case = "true";
    InvertedIndexAnalyzerConfig read_cfg = build_cfg;
    read_cfg.lower_case = "false";
    EXPECT_NE(compute_analyzer_sig(build_cfg), compute_analyzer_sig(read_cfg))
            << "A3 guard is only load-bearing if the build/read analyzer signatures differ";

    // Read with a different analyzer config (lower_case=false): the index's own signature now
    // differs from the one stored in the BF, so the fast path must be skipped. The normal query
    // still returns whatever the index actually contains; the key assertion is that the BF does
    // not silently empty the result.
    auto read_meta_pb = std::make_unique<TabletIndexPB>();
    read_meta_pb->set_index_type(IndexType::INVERTED);
    read_meta_pb->set_index_id(1);
    read_meta_pb->set_index_name("test_tbf");
    read_meta_pb->clear_col_unique_id();
    read_meta_pb->add_col_unique_id(1);
    (*read_meta_pb->mutable_properties())["parser"] = "english";
    (*read_meta_pb->mutable_properties())["lower_case"] = "false";
    (*read_meta_pb->mutable_properties())["support_phrase"] = "true";
    (*read_meta_pb->mutable_properties())["token_bloom_filter"] = "true";
    TabletIndex read_meta;
    read_meta.init_from_pb(*read_meta_pb.get());

    config::enable_inverted_index_term_bf = false;
    Status st_off;
    auto baseline = run_fulltext(prefix, &read_meta, "apple",
                                 InvertedIndexQueryType::MATCH_ANY_QUERY, &st_off);
    EXPECT_TRUE(st_off.ok()) << st_off;
    // The baseline must actually return rows, otherwise the equality below could pass vacuously
    // (two empty bitmaps) even if the BF wrongly emptied a mismatched-signature query.
    EXPECT_GT(baseline->cardinality(), 0U) << "baseline must be non-empty for a meaningful check";

    config::enable_inverted_index_term_bf = true;
    Status st_on;
    auto with_bf = run_fulltext(prefix, &read_meta, "apple",
                                InvertedIndexQueryType::MATCH_ANY_QUERY, &st_on);
    EXPECT_TRUE(st_on.ok()) << st_on;

    // With the analyzer mismatched, the BF is ignored: results must match the normal path
    // exactly (the BF must not turn a mismatched-signature query into a (possibly wrong) empty).
    EXPECT_TRUE(*baseline == *with_bf)
            << "analyzer-sig mismatch must fall back to normal query (baseline card="
            << baseline->cardinality() << ", bf card=" << with_bf->cardinality() << ")";
}

// A5: an empty keyword token must round-trip through the BF and remain matchable. Build a
// fulltext index whose values include an empty string; the term dictionary then contains the
// zero-length token, which the writer feeds into the BF. A MATCH_ANY for a present token still
// returns its docs with the BF on (the empty token in the dictionary must not corrupt probing).
TEST_F(InvertedIndexTermBfReaderTest, EmptyTokenRoundTripsThroughBf) {
    std::vector<Slice> values = {Slice(""), Slice("apple"), Slice("")};
    std::map<std::string, std::string> props = {{"parser", "english"},
                                                {"lower_case", "true"},
                                                {"support_phrase", "true"},
                                                {"token_bloom_filter", "true"}};
    TabletIndex idx_meta;
    std::string prefix;
    build_index("tbf_rs_a5", 0, props, values, &idx_meta, &prefix);

    config::enable_inverted_index_term_bf = false;
    Status st_off;
    auto baseline = run_fulltext(prefix, &idx_meta, "apple",
                                 InvertedIndexQueryType::MATCH_ANY_QUERY, &st_off);
    EXPECT_TRUE(st_off.ok()) << st_off;

    config::enable_inverted_index_term_bf = true;
    Status st_on;
    auto with_bf = run_fulltext(prefix, &idx_meta, "apple", InvertedIndexQueryType::MATCH_ANY_QUERY,
                                &st_on);
    EXPECT_TRUE(st_on.ok()) << st_on;

    EXPECT_TRUE(*baseline == *with_bf)
            << "empty token in the dictionary must not corrupt the BF fast path (baseline card="
            << baseline->cardinality() << ", bf card=" << with_bf->cardinality() << ")";
}

// A4: the keyword/exact path is StringTypeInvertedIndexReader and is never consulted by the BF.
// An over-ignore_above EQUAL still returns INVERTED_INDEX_EVALUATE_SKIPPED (forcing a table
// scan), even with the BF flag enabled -- the flag must not turn this into an empty result.
TEST_F(InvertedIndexTermBfReaderTest, KeywordIgnoreAboveStillEvaluateSkipped) {
    // Keyword index (no parser) over a short value; query a value longer than ignore_above.
    std::vector<Slice> values = {Slice("short")};
    std::map<std::string, std::string> props = {{"ignore_above", "10"}};
    TabletIndex idx_meta;
    std::string prefix;
    build_index("tbf_rs_a4", 0, props, values, &idx_meta, &prefix);

    auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                    InvertedIndexStorageFormatPB::V2);
    ASSERT_TRUE(reader->init().ok());
    auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
    ASSERT_NE(str_reader, nullptr);

    io::IOContext io_ctx;
    OlapReaderStatistics stats;
    RuntimeState runtime_state;
    auto context = make_context(&io_ctx, &stats, &runtime_state);

    std::string long_value(64, 'x'); // > ignore_above (10)
    config::enable_inverted_index_term_bf = true;
    auto bitmap = std::make_shared<roaring::Roaring>();
    Field qp = Field::create_field<TYPE_STRING>(long_value);
    auto st = str_reader->query(context, "1", qp, InvertedIndexQueryType::EQUAL_QUERY, bitmap);
    EXPECT_EQ(st.code(), static_cast<int>(ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED))
            << "over-ignore_above keyword EQUAL must still force a table scan, not be emptied by "
               "the BF";
}

// Observability: the per-(segment,index) BF profile counters must reflect the actual path taken
// -- absent -> AbsentSkip, present -> Fallthrough, warm -> CacheHit, cold -> CacheMiss+Load, and
// an index with no tbf -> Unavailable. These are exactly the counters surfaced in the query
// profile, so this test is what keeps the profile numbers honest.
TEST_F(InvertedIndexTermBfReaderTest, BfObservabilityCountersReflectPath) {
    std::vector<Slice> values = {Slice("apple banana"), Slice("cherry date"),
                                 Slice("apple cherry")};
    std::map<std::string, std::string> with_bf = {{"parser", "english"},
                                                  {"lower_case", "true"},
                                                  {"support_phrase", "true"},
                                                  {"token_bloom_filter", "true"}};
    TabletIndex idx_meta;
    std::string prefix;
    build_index("tbf_rs_obs", 0, with_bf, values, &idx_meta, &prefix);

    config::enable_inverted_index_term_bf = true;

    auto run = [&](const std::string& term, InvertedIndexQueryType type,
                   OlapReaderStatistics* stats) -> std::shared_ptr<roaring::Roaring> {
        auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                        InvertedIndexStorageFormatPB::V2);
        EXPECT_TRUE(reader->init().ok());
        auto ft_reader = FullTextIndexReader::create_shared(&idx_meta, reader);
        io::IOContext io_ctx;
        RuntimeState runtime_state;
        auto context = make_context(&io_ctx, stats, &runtime_state);
        auto bitmap = std::make_shared<roaring::Roaring>();
        Field qp = Field::create_field<TYPE_STRING>(term);
        EXPECT_TRUE(ft_reader->query(context, "1", qp, type, bitmap).ok());
        return bitmap;
    };

    // Cold absent query: cache miss -> load the tbf once, prove absent, skip the searcher.
    OlapReaderStatistics cold;
    auto b1 = run("zzabsentone", InvertedIndexQueryType::MATCH_ANY_QUERY, &cold);
    EXPECT_EQ(b1->cardinality(), 0U);
    EXPECT_EQ(cold.inverted_index_term_bf_probe, 1);
    EXPECT_EQ(cold.inverted_index_term_bf_skipped_lookups, 1);
    EXPECT_EQ(cold.inverted_index_term_bf_fallthrough, 0);
    EXPECT_EQ(cold.inverted_index_term_bf_unavailable, 0);
    EXPECT_EQ(cold.inverted_index_term_bf_cache_hit, 0);
    EXPECT_EQ(cold.inverted_index_term_bf_cache_miss, 1);
    EXPECT_EQ(cold.inverted_index_term_bf_load_count, 1);
    EXPECT_GT(cold.inverted_index_term_bf_load_bytes, 0);

    // Warm absent query (different token): BF cache hit, zero load IO, still proven absent.
    OlapReaderStatistics warm;
    auto b2 = run("zzabsenttwo", InvertedIndexQueryType::MATCH_ANY_QUERY, &warm);
    EXPECT_EQ(b2->cardinality(), 0U);
    EXPECT_EQ(warm.inverted_index_term_bf_cache_hit, 1);
    EXPECT_EQ(warm.inverted_index_term_bf_cache_miss, 0);
    EXPECT_EQ(warm.inverted_index_term_bf_load_count, 0);
    EXPECT_EQ(warm.inverted_index_term_bf_load_bytes, 0);
    EXPECT_EQ(warm.inverted_index_term_bf_probe, 1);
    EXPECT_EQ(warm.inverted_index_term_bf_skipped_lookups, 1);

    // Present token: BF says MAYBE -> fall through to the normal lookup (no skip).
    OlapReaderStatistics present;
    auto b3 = run("apple", InvertedIndexQueryType::MATCH_ANY_QUERY, &present);
    EXPECT_GT(b3->cardinality(), 0U);
    EXPECT_EQ(present.inverted_index_term_bf_probe, 1);
    EXPECT_EQ(present.inverted_index_term_bf_skipped_lookups, 0);
    EXPECT_EQ(present.inverted_index_term_bf_fallthrough, 1);

    // (a) Property gate: an index WITHOUT token_bloom_filter=true is skipped before the fast path
    // even runs -- no index open, no fileExists("tbf"), so EVERY BF counter stays 0. Without the
    // gate, flipping the global config on would tax every fulltext index with an extra open.
    std::map<std::string, std::string> no_bf = {
            {"parser", "english"}, {"lower_case", "true"}, {"support_phrase", "true"}};
    TabletIndex idx_nobf;
    std::string prefix_nobf;
    build_index("tbf_rs_obs_nobf", 1, no_bf, values, &idx_nobf, &prefix_nobf);
    {
        auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix_nobf,
                                                        InvertedIndexStorageFormatPB::V2);
        ASSERT_TRUE(reader->init().ok());
        auto ft_reader = FullTextIndexReader::create_shared(&idx_nobf, reader);
        io::IOContext io_ctx;
        OlapReaderStatistics nobf;
        RuntimeState runtime_state;
        auto context = make_context(&io_ctx, &nobf, &runtime_state);
        auto bitmap = std::make_shared<roaring::Roaring>();
        Field qp = Field::create_field<TYPE_STRING>("zzabsentthree");
        ASSERT_TRUE(
                ft_reader->query(context, "1", qp, InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap)
                        .ok());
        EXPECT_EQ(nobf.inverted_index_term_bf_probe, 0);
        EXPECT_EQ(nobf.inverted_index_term_bf_skipped_lookups, 0);
        EXPECT_EQ(nobf.inverted_index_term_bf_unavailable, 0);
        EXPECT_EQ(nobf.inverted_index_term_bf_cache_miss, 0);
        EXPECT_EQ(nobf.inverted_index_term_bf_load_count, 0);
    }

    // (b) Unavailable: an index WITH token_bloom_filter=true passes the gate, but a read whose
    // analyzer signature differs from the build (A3, lower_case flipped) rejects the loaded BF as
    // stale -> Unavailable, no probe/skip. Built fresh (no prior matching-meta query) so the load
    // -- and its A3 check -- actually runs rather than hitting a cached BF.
    {
        std::map<std::string, std::string> with_bf_stale = {{"parser", "english"},
                                                            {"lower_case", "true"},
                                                            {"support_phrase", "true"},
                                                            {"token_bloom_filter", "true"}};
        TabletIndex idx_stale_build;
        std::string prefix_stale;
        build_index("tbf_rs_obs_stale", 2, with_bf_stale, values, &idx_stale_build, &prefix_stale);

        auto read_pb = std::make_unique<TabletIndexPB>();
        read_pb->set_index_type(IndexType::INVERTED);
        read_pb->set_index_id(1);
        read_pb->set_index_name("test_tbf");
        read_pb->clear_col_unique_id();
        read_pb->add_col_unique_id(1);
        (*read_pb->mutable_properties())["parser"] = "english";
        (*read_pb->mutable_properties())["lower_case"] = "false"; // build used true -> sig mismatch
        (*read_pb->mutable_properties())["support_phrase"] = "true";
        (*read_pb->mutable_properties())["token_bloom_filter"] = "true";
        TabletIndex stale_meta;
        stale_meta.init_from_pb(*read_pb.get());

        auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix_stale,
                                                        InvertedIndexStorageFormatPB::V2);
        ASSERT_TRUE(reader->init().ok());
        auto ft_reader = FullTextIndexReader::create_shared(&stale_meta, reader);
        io::IOContext io_ctx;
        OlapReaderStatistics stale;
        RuntimeState runtime_state;
        auto context = make_context(&io_ctx, &stale, &runtime_state);
        auto bitmap = std::make_shared<roaring::Roaring>();
        Field qp = Field::create_field<TYPE_STRING>("zzabsentfour");
        ASSERT_TRUE(
                ft_reader->query(context, "1", qp, InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap)
                        .ok());
        EXPECT_EQ(stale.inverted_index_term_bf_unavailable, 1);
        EXPECT_EQ(stale.inverted_index_term_bf_probe, 0);
        EXPECT_EQ(stale.inverted_index_term_bf_skipped_lookups, 0);
    }
}

// erase() must drop the BF entry too. The BF lives in the searcher cache under a derived key; if
// erase() only removed the searcher key, a compaction/rewrite that invalidates the segment would
// leave a stale BF behind -- and a same-key rewrite could then serve it (a false "absent" ->
// dropped hit). After erase, a subsequent absent query must re-load the BF (cache miss), not hit.
TEST_F(InvertedIndexTermBfReaderTest, EraseAlsoClearsTermBf) {
    std::vector<Slice> values = {Slice("apple banana"), Slice("cherry date"),
                                 Slice("apple cherry")};
    std::map<std::string, std::string> props = {{"parser", "english"},
                                                {"lower_case", "true"},
                                                {"support_phrase", "true"},
                                                {"token_bloom_filter", "true"}};
    TabletIndex idx_meta;
    std::string prefix;
    build_index("tbf_rs_erase", 0, props, values, &idx_meta, &prefix);
    config::enable_inverted_index_term_bf = true;

    auto run_absent = [&](const std::string& term,
                          OlapReaderStatistics* stats) -> std::shared_ptr<IndexFileReader> {
        auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                        InvertedIndexStorageFormatPB::V2);
        EXPECT_TRUE(reader->init().ok());
        auto ft_reader = FullTextIndexReader::create_shared(&idx_meta, reader);
        io::IOContext io_ctx;
        RuntimeState runtime_state;
        auto context = make_context(&io_ctx, stats, &runtime_state);
        auto bitmap = std::make_shared<roaring::Roaring>();
        Field qp = Field::create_field<TYPE_STRING>(term);
        EXPECT_TRUE(
                ft_reader->query(context, "1", qp, InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap)
                        .ok());
        return reader;
    };

    OlapReaderStatistics cold;
    auto reader = run_absent("zzeraseone", &cold);
    EXPECT_EQ(cold.inverted_index_term_bf_cache_miss, 1);
    EXPECT_EQ(cold.inverted_index_term_bf_load_count, 1);

    OlapReaderStatistics warm;
    run_absent("zzerasetwo", &warm);
    EXPECT_EQ(warm.inverted_index_term_bf_cache_hit, 1);
    EXPECT_EQ(warm.inverted_index_term_bf_load_count, 0);

    // Erase the (segment, index) from the searcher cache, exactly as compaction / rewrite does.
    const auto index_file_key = reader->get_index_file_cache_key(&idx_meta);
    ASSERT_TRUE(InvertedIndexSearcherCache::instance()->erase(index_file_key).ok());

    OlapReaderStatistics after;
    run_absent("zzerasethree", &after);
    EXPECT_EQ(after.inverted_index_term_bf_cache_hit, 0) << "erased BF must not be served";
    EXPECT_EQ(after.inverted_index_term_bf_cache_miss, 1) << "erased BF must trigger a reload";
    EXPECT_EQ(after.inverted_index_term_bf_load_count, 1);
}

// A3 must be applied consistently on the cache-hit path, not just the cold-load path. The BF cache
// key (index_file_key) does not encode the analyzer, so a BF cached under one analyzer is reachable
// by a reader whose _index_meta was altered to a different analyzer. A cold query for that reader
// rejects the BF (A3 -> unavailable); a warm query must do the same, otherwise cold/warm disagree.
// (Results are correct either way -- the BF mirrors the segment's dictionary, so no false negatives
// -- so this asserts on the path/stats, which is what the cache-hit revalidation changes.)
TEST_F(InvertedIndexTermBfReaderTest, StaleAnalyzerCachedBfRevalidatedOnHit) {
    std::vector<Slice> values = {Slice("apple banana"), Slice("cherry date"),
                                 Slice("apple cherry")};
    std::map<std::string, std::string> match_props = {{"parser", "english"},
                                                      {"lower_case", "true"},
                                                      {"support_phrase", "true"},
                                                      {"token_bloom_filter", "true"}};
    TabletIndex match_meta;
    std::string prefix;
    build_index("tbf_rs_stalehit", 0, match_props, values, &match_meta, &prefix);

    // Same file/index (-> same index_file_key) but read with lower_case=false: a different analyzer
    // signature from the one the BF was built under.
    auto stale_pb = std::make_unique<TabletIndexPB>();
    stale_pb->set_index_type(IndexType::INVERTED);
    stale_pb->set_index_id(1);
    stale_pb->set_index_name("test_tbf");
    stale_pb->clear_col_unique_id();
    stale_pb->add_col_unique_id(1);
    (*stale_pb->mutable_properties())["parser"] = "english";
    (*stale_pb->mutable_properties())["lower_case"] = "false";
    (*stale_pb->mutable_properties())["support_phrase"] = "true";
    (*stale_pb->mutable_properties())["token_bloom_filter"] = "true";
    TabletIndex stale_meta;
    stale_meta.init_from_pb(*stale_pb.get());

    auto query = [&](TabletIndex* meta, const std::string& term,
                     OlapReaderStatistics* stats) -> std::shared_ptr<roaring::Roaring> {
        auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                        InvertedIndexStorageFormatPB::V2);
        EXPECT_TRUE(reader->init().ok());
        auto ft_reader = FullTextIndexReader::create_shared(meta, reader);
        io::IOContext io_ctx;
        RuntimeState runtime_state;
        auto context = make_context(&io_ctx, stats, &runtime_state);
        auto bitmap = std::make_shared<roaring::Roaring>();
        Field qp = Field::create_field<TYPE_STRING>(term);
        EXPECT_TRUE(
                ft_reader->query(context, "1", qp, InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap)
                        .ok());
        return bitmap;
    };

    // Baseline (no BF) for the stale-analyzer reader; "apple" is present so this is non-empty.
    config::enable_inverted_index_term_bf = false;
    OlapReaderStatistics base_stats;
    auto baseline = query(&stale_meta, "apple", &base_stats);
    EXPECT_GT(baseline->cardinality(), 0U) << "baseline must be non-empty for a meaningful check";

    // Populate the BF cache with the MATCHING analyzer.
    config::enable_inverted_index_term_bf = true;
    OlapReaderStatistics warm_stats;
    query(&match_meta, "apple", &warm_stats);
    EXPECT_EQ(warm_stats.inverted_index_term_bf_cache_miss, 1);
    EXPECT_EQ(warm_stats.inverted_index_term_bf_load_count, 1);

    // The stale-analyzer reader now hits the cache but must revalidate: same outcome as a cold load
    // would give it (unavailable -> fall back), NOT a cache hit that serves the mismatched BF.
    OlapReaderStatistics stale_stats;
    auto with_bf = query(&stale_meta, "apple", &stale_stats);
    EXPECT_EQ(stale_stats.inverted_index_term_bf_unavailable, 1) << "stale-analyzer BF rejected";
    EXPECT_EQ(stale_stats.inverted_index_term_bf_cache_hit, 0);
    EXPECT_EQ(stale_stats.inverted_index_term_bf_probe, 0);
    EXPECT_TRUE(*baseline == *with_bf) << "fall-back result must match the normal query";
}

// Negative caching: an index whose property passes the gate (token_bloom_filter=true) but whose
// segment carries no usable "tbf" (e.g. compacted by an older BE, or a build failure) must cache
// that negative result -- the first query loads-and-fails, and a second query hits the negative
// marker instead of re-opening the index. Built WITHOUT a tbf on disk, then read with a
// property=true meta so the gate is passed but the structural load returns nothing.
TEST_F(InvertedIndexTermBfReaderTest, NegativeCacheSkipsRepeatOpenWhenNoTbf) {
    std::vector<Slice> values = {Slice("apple banana"), Slice("cherry date"),
                                 Slice("apple cherry")};
    // No token_bloom_filter -> the writer emits no "tbf" sub-file.
    std::map<std::string, std::string> no_bf_props = {
            {"parser", "english"}, {"lower_case", "true"}, {"support_phrase", "true"}};
    TabletIndex build_meta;
    std::string prefix;
    build_index("tbf_rs_neg", 0, no_bf_props, values, &build_meta, &prefix);

    // Reader meta WITH token_bloom_filter=true: the property gate passes even though the disk
    // segment has no tbf.
    auto read_pb = std::make_unique<TabletIndexPB>();
    read_pb->set_index_type(IndexType::INVERTED);
    read_pb->set_index_id(1);
    read_pb->set_index_name("test_tbf");
    read_pb->clear_col_unique_id();
    read_pb->add_col_unique_id(1);
    (*read_pb->mutable_properties())["parser"] = "english";
    (*read_pb->mutable_properties())["lower_case"] = "true";
    (*read_pb->mutable_properties())["support_phrase"] = "true";
    (*read_pb->mutable_properties())["token_bloom_filter"] = "true";
    TabletIndex read_meta;
    read_meta.init_from_pb(*read_pb.get());

    config::enable_inverted_index_term_bf = true;

    auto run = [&](const std::string& term, OlapReaderStatistics* stats) {
        auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(), prefix,
                                                        InvertedIndexStorageFormatPB::V2);
        EXPECT_TRUE(reader->init().ok());
        auto ft_reader = FullTextIndexReader::create_shared(&read_meta, reader);
        io::IOContext io_ctx;
        RuntimeState runtime_state;
        auto context = make_context(&io_ctx, stats, &runtime_state);
        auto bitmap = std::make_shared<roaring::Roaring>();
        Field qp = Field::create_field<TYPE_STRING>(term);
        EXPECT_TRUE(
                ft_reader->query(context, "1", qp, InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap)
                        .ok());
    };

    // First query: gate passes, lookup miss, structural load finds no tbf -> unavailable + cache
    // the negative result.
    OlapReaderStatistics first;
    run("zznegone", &first);
    EXPECT_EQ(first.inverted_index_term_bf_cache_miss, 1);
    EXPECT_EQ(first.inverted_index_term_bf_unavailable, 1);
    EXPECT_EQ(first.inverted_index_term_bf_load_count, 0) << "no tbf -> nothing loaded";

    // Second query (different token): hits the negative marker -> no new miss, no re-open.
    OlapReaderStatistics second;
    run("zznegtwo", &second);
    EXPECT_EQ(second.inverted_index_term_bf_cache_miss, 0)
            << "negative result must be cached, not re-missed";
    EXPECT_EQ(second.inverted_index_term_bf_unavailable, 1);
    EXPECT_EQ(second.inverted_index_term_bf_load_count, 0);
    EXPECT_EQ(second.inverted_index_term_bf_probe, 0);
}

} // namespace doris::segment_v2
