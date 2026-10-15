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

// How SEARCH keeps the rows a NULL field leaves UNKNOWN, read through fake text indexes.

#include <gen_cpp/Exprs_types.h>
#include <gtest/gtest.h>

#include <deque>
#include <initializer_list>
#include <map>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/search/function_search.h"
#include "runtime/exec_env.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/common/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/query/scoring/collection_similarity.h"
#include "storage/index/inverted/query/spi/fake_index_source.h"
#include "storage/index/inverted/reader/collection_statistics.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
namespace {

using segment_v2::InvertedIndexQueryCache;
using segment_v2::InvertedIndexQueryCacheHandle;
using segment_v2::InvertedIndexQueryType;

roaring::Roaring rows(std::initializer_list<uint32_t> docs) {
    roaring::Roaring bitmap;
    for (const uint32_t doc : docs) {
        bitmap.add(doc);
    }
    return bitmap;
}

// An index file the engine never opens: the reader below answers from memory.
class UnopenedIndexFileReader final : public segment_v2::IndexFileReader {
public:
    explicit UnopenedIndexFileReader(const std::string& index_path)
            : segment_v2::IndexFileReader(nullptr, index_path, InvertedIndexStorageFormatPB::SNII) {
    }

    Status init(int32_t /*read_buffer_size*/, const io::IOContext* /*io_ctx*/) override {
        return Status::OK();
    }

    Result<std::unique_ptr<segment_v2::DorisCompoundReader, segment_v2::DirectoryDeleter>> open(
            const TabletIndex* /*index_meta*/, const io::IOContext* /*io_ctx*/) const override {
        return ResultError(Status::InternalError("the test index is never opened"));
    }
};

// A text index whose postings come from a fake source and whose NULL rows come from memory.
// A non-OK `null_status` fails every read of the NULL rows.
class FakeTextIndexReader final : public segment_v2::InvertedIndexReader {
public:
    FakeTextIndexReader(const TabletIndex* index_meta,
                        std::shared_ptr<segment_v2::IndexFileReader> index_file_reader)
            : segment_v2::InvertedIndexReader(index_meta, std::move(index_file_reader)),
              _null_cache(1024 * 1024, 1),
              _null_cache_key {"/tmp/search_null_test", "", InvertedIndexQueryType::UNKNOWN_QUERY,
                               std::to_string(index_meta->index_id())} {}

    Status new_iterator(std::unique_ptr<segment_v2::IndexIterator>* /*iterator*/) override {
        return Status::OK();
    }

    Status open_source(const segment_v2::IndexQueryContextPtr& /*context*/,
                       const std::wstring& /*field*/,
                       std::unique_ptr<segment_v2::OpenedIndex>* opened,
                       index_query::IndexSourcePtr* index_source) override {
        opened->reset();
        *index_source = source;
        return Status::OK();
    }

    Status query(const segment_v2::IndexQueryContextPtr& /*context*/,
                 const std::string& /*column_name*/, const Field& /*query_value*/,
                 InvertedIndexQueryType /*query_type*/,
                 std::shared_ptr<roaring::Roaring>& /*bitmap*/,
                 const InvertedIndexAnalyzerCtx* /*analyzer_ctx*/ = nullptr) override {
        return Status::InternalError("SEARCH reads the index through its source");
    }

    Status try_query(const segment_v2::IndexQueryContextPtr& /*context*/,
                     const std::string& /*column_name*/, const Field& /*query_value*/,
                     InvertedIndexQueryType /*query_type*/, size_t* /*count*/) override {
        return Status::OK();
    }

    Status read_null_bitmap(const segment_v2::IndexQueryContextPtr& /*context*/,
                            InvertedIndexQueryCacheHandle* cache_handle) override {
        RETURN_IF_ERROR(null_status);
        _null_cache.insert(_null_cache_key, std::make_shared<roaring::Roaring>(nulls),
                           cache_handle);
        return Status::OK();
    }

    segment_v2::InvertedIndexReaderType type() override {
        return segment_v2::InvertedIndexReaderType::FULLTEXT;
    }

    std::shared_ptr<index_query::testing::FakeIndexSource> source =
            std::make_shared<index_query::testing::FakeIndexSource>();
    roaring::Roaring nulls;
    Status null_status = Status::OK();

private:
    InvertedIndexQueryCache _null_cache;
    InvertedIndexQueryCache::CacheKey _null_cache_key;
};

class FixedStatistics final : public CollectionStatistics {
public:
    float get_or_calculate_idf(const std::wstring& /*field*/,
                               const std::wstring& /*term*/) override {
        return 1.5F;
    }

    float get_or_calculate_avg_dl(const std::wstring& /*field*/) override { return 4.0F; }
};

struct EvaluateOptions {
    bool scoring = false;
    // Collect the best rows only, as `ORDER BY score() DESC LIMIT top_k` does.
    size_t top_k = 0;
    bool cache = false;
    std::string default_operator;
};

// Options that score when `scoring` is set and keep the best `top_k` rows when it is non-zero.
EvaluateOptions scored_if(bool scoring, size_t top_k = 0) {
    EvaluateOptions options;
    options.scoring = scoring;
    options.top_k = top_k;
    return options;
}

// Analyzed text fields of one segment, searched the way VSearchExpr searches them.
class NullSearchFixture {
public:
    explicit NullSearchFixture(uint32_t row_count) : _row_count(row_count) {}

    // Adds a field; an array field is an ARRAY<STRING> column.
    FakeTextIndexReader& add_field(const std::string& name, bool array = false) {
        auto& field = _fields.emplace_back();
        field.meta = make_index(static_cast<int64_t>(_fields.size()));
        field.reader = std::make_shared<FakeTextIndexReader>(
                &field.meta, std::make_shared<UnopenedIndexFileReader>("/tmp/search_null_idx"));
        field.iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, field.reader);
        DataTypePtr type = std::make_shared<DataTypeString>();
        if (array) {
            type = make_nullable(std::make_shared<DataTypeArray>(make_nullable(type)));
        }
        _types.emplace(name, IndexFieldNameAndTypePair {name, type});
        _iterators[name] = &field.iterator;
        TSearchFieldBinding binding;
        binding.field_name = name;
        binding.index_properties = properties();
        binding.__isset.index_properties = true;
        _bindings.push_back(binding);
        return *field.reader;
    }

    TSearchParam param(const TSearchClause& root, const EvaluateOptions& options = {}) const {
        TSearchParam param;
        param.original_dsl = "search null test";
        param.root = root;
        param.field_bindings = _bindings;
        if (!options.default_operator.empty()) {
            param.default_operator = options.default_operator;
            param.__isset.default_operator = true;
        }
        return param;
    }

    Status evaluate(const TSearchClause& root, const EvaluateOptions& options,
                    InvertedIndexResultBitmap* result) {
        auto context = std::make_shared<segment_v2::IndexQueryContext>();
        context->stats = &_stats;
        if (options.scoring) {
            context->collection_similarity = std::make_shared<CollectionSimilarity>();
            context->collection_statistics = std::make_shared<FixedStatistics>();
            context->query_limit = options.top_k;
        }
        FunctionSearch search;
        return search.evaluate_inverted_index_with_search_param(
                param(root, options), _types, _iterators, _row_count, *result, options.cache,
                nullptr, nullptr, context);
    }

    // The TRUE and UNKNOWN rows of `root`.
    void expect(const TSearchClause& root, const EvaluateOptions& options,
                const roaring::Roaring& true_rows, const roaring::Roaring& null_rows) {
        InvertedIndexResultBitmap result;
        const Status status = evaluate(root, options, &result);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_NE(nullptr, result.get_data_bitmap());
        EXPECT_EQ(true_rows.toString(), result.get_data_bitmap()->toString());
        const roaring::Roaring no_rows;
        const auto* nulls =
                result.get_null_bitmap() != nullptr ? result.get_null_bitmap().get() : &no_rows;
        EXPECT_EQ(null_rows.toString(), nulls->toString());
    }

    static std::map<std::string, std::string> properties() {
        return {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD},
                {INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                 INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES}};
    }

private:
    struct FieldIndex {
        TabletIndex meta;
        std::shared_ptr<FakeTextIndexReader> reader;
        segment_v2::InvertedIndexIterator iterator;
    };

    static TabletIndex make_index(int64_t index_id) {
        TabletIndexPB pb;
        pb.set_index_type(IndexType::INVERTED);
        pb.set_index_id(index_id);
        pb.set_index_name("search_null_index_" + std::to_string(index_id));
        pb.add_col_unique_id(static_cast<int32_t>(index_id));
        for (const auto& [key, value] : properties()) {
            (*pb.mutable_properties())[key] = value;
        }
        TabletIndex index;
        index.init_from_pb(pb);
        return index;
    }

    uint32_t _row_count;
    std::deque<FieldIndex> _fields;
    std::unordered_map<std::string, IndexFieldNameAndTypePair> _types;
    std::unordered_map<std::string, IndexIterator*> _iterators;
    std::vector<TSearchFieldBinding> _bindings;
    OlapReaderStatistics _stats;
};

TSearchClause leaf(const std::string& clause_type, const std::string& field,
                   const std::string& value) {
    TSearchClause clause;
    clause.clause_type = clause_type;
    clause.field_name = field;
    clause.value = value;
    clause.__isset.field_name = true;
    clause.__isset.value = true;
    return clause;
}

TSearchClause compound(const std::string& clause_type, std::vector<TSearchClause> children) {
    TSearchClause clause;
    clause.clause_type = clause_type;
    clause.children = std::move(children);
    clause.__isset.children = true;
    return clause;
}

TSearchClause occur(TSearchClause clause, TSearchOccur::type value) {
    clause.occur = value;
    clause.__isset.occur = true;
    return clause;
}

class ScopedQueryCache final {
public:
    ScopedQueryCache()
            : _previous(ExecEnv::GetInstance()->get_inverted_index_query_cache()),
              _cache(InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1)) {
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_cache.get());
    }
    ~ScopedQueryCache() { ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous); }

    InvertedIndexQueryCache* get() const { return _cache.get(); }

private:
    InvertedIndexQueryCache* _previous;
    std::unique_ptr<InvertedIndexQueryCache> _cache;
};

} // namespace

// A term the dictionary lacks matches no row, and the rows where its field is NULL stay
// UNKNOWN, alone or listed with other terms of the field.
TEST(FunctionSearchNullTest, AbsentTermKeepsTheFieldNullRows) {
    for (const bool batches : {false, true}) {
        for (const bool scoring : {false, true}) {
            SCOPED_TRACE(::testing::Message() << "batches=" << batches << " scoring=" << scoring);
            NullSearchFixture fixture(4);
            auto& body = fixture.add_field("body");
            body.source->batches = batches;
            body.source->add("foo", {0});
            body.source->add("bar", {1});
            body.nulls = rows({3});
            const EvaluateOptions options = scored_if(scoring);

            fixture.expect(compound("NOT", {leaf("TERM", "body", "missing")}), options,
                           rows({0, 1, 2}), rows({3}));
            fixture.expect(compound("NOT", {compound("AND", {leaf("TERM", "body", "missing"),
                                                             leaf("TERM", "body", "foo")})}),
                           options, rows({0, 1, 2}), rows({3}));
            fixture.expect(compound("NOT", {compound("OR", {leaf("TERM", "body", "missing"),
                                                            leaf("TERM", "body", "absent")})}),
                           options, rows({0, 1, 2}), rows({3}));
        }
    }
}

// A scored conjunction whose terms share no row still leaves the field's NULL rows UNKNOWN, as
// the unscored one does.
TEST(FunctionSearchNullTest, EmptyScoredConjunctionKeepsTheFieldNullRows) {
    for (const bool scoring : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "scoring=" << scoring);
        NullSearchFixture fixture(3);
        auto& body = fixture.add_field("body");
        body.source->batches = true;
        body.source->add("foo", {0});
        body.source->add("bar", {1});
        body.nulls = rows({2});

        EvaluateOptions options = scored_if(scoring);
        options.default_operator = "and";
        fixture.expect(compound("NOT", {leaf("TERM", "body", "foo/bar")}), options, rows({0, 1}),
                       rows({2}));
    }
}

// A value the analyzer turns into no token matches no row, and the rows where the field is NULL
// stay UNKNOWN.
TEST(FunctionSearchNullTest, AnalyzerEmptyLeafKeepsTheFieldNullRows) {
    for (const std::string clause_type : {"TERM", "PHRASE", "MATCH", "ANY", "ALL"}) {
        for (const bool scoring : {false, true}) {
            SCOPED_TRACE(::testing::Message() << clause_type << " scoring=" << scoring);
            NullSearchFixture fixture(3);
            auto& body = fixture.add_field("body");
            body.source->add("foo", {0});
            body.nulls = rows({2});

            fixture.expect(compound("NOT", {leaf(clause_type, "body", "!!!")}), scored_if(scoring),
                           rows({0, 1}), rows({2}));
        }
    }
}

// An array column can keep values in a row whose array is NULL, and its index lists that row
// under them. The row is UNKNOWN in every Boolean, and FALSE in a two-valued one.
TEST(FunctionSearchNullTest, ArrayNullRowWithValuesIsUnknown) {
    for (const bool scoring : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "scoring=" << scoring);
        NullSearchFixture fixture(4);
        auto& arr = fixture.add_field("arr", /*array=*/true);
        arr.source->add("alpha", {0, 1});
        arr.nulls = rows({1});
        auto& other = fixture.add_field("other");
        other.source->add("omega", {2});
        other.source->add("x", {1});
        const EvaluateOptions options = scored_if(scoring);

        fixture.expect(leaf("TERM", "arr", "alpha"), options, rows({0}), rows({1}));
        fixture.expect(
                compound("OR", {leaf("TERM", "arr", "alpha"), leaf("TERM", "other", "omega")}),
                options, rows({0, 2}), rows({1}));
        fixture.expect(compound("AND", {leaf("TERM", "arr", "alpha"), leaf("TERM", "other", "x")}),
                       options, rows({}), rows({1}));
        fixture.expect(compound("NOT", {leaf("TERM", "arr", "alpha")}), options, rows({2, 3}),
                       rows({1}));
        fixture.expect(compound("OCCUR_BOOLEAN",
                                {occur(leaf("TERM", "arr", "alpha"), TSearchOccur::SHOULD),
                                 occur(leaf("TERM", "other", "omega"), TSearchOccur::SHOULD)}),
                       options, rows({0, 2}), rows({}));
        if (scoring) {
            fixture.expect(
                    compound("OR", {leaf("TERM", "arr", "alpha"), leaf("TERM", "other", "omega")}),
                    scored_if(true, 2), rows({0, 2}), rows({1}));
        }
    }
}

// A failed read of a field's NULL rows fails the search instead of leaving them out.
TEST(FunctionSearchNullTest, FailedNullBitmapReadFailsTheSearch) {
    for (const bool scoring : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "scoring=" << scoring);
        NullSearchFixture fixture(3);
        auto& body = fixture.add_field("body");
        body.source->add("foo", {0});
        body.nulls = rows({2});
        body.null_status = Status::IOError("the NULL section failed its checksum");

        InvertedIndexResultBitmap result;
        const Status status = fixture.evaluate(compound("NOT", {leaf("TERM", "body", "foo")}),
                                               scored_if(scoring), &result);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(std::string::npos, status.to_string().find("checksum")) << status.to_string();
    }
}

// The cache keeps a result's TRUE and UNKNOWN rows as two entries; one found without the other
// is computed again rather than taken as having no UNKNOWN rows.
TEST(FunctionSearchNullTest, DslCacheEntryWithoutItsNullRowsIsAMiss) {
    ScopedQueryCache cache;
    NullSearchFixture fixture(4);
    auto& body = fixture.add_field("body");
    body.source->add("foo", {0});
    body.nulls = rows({3});
    const auto root = compound("NOT", {leaf("TERM", "body", "foo")});

    EvaluateOptions options;
    options.cache = true;
    InvertedIndexQueryCache::CacheKey key {
            body.get_index_file_reader()->get_index_path_prefix(), "__search_dsl__",
            InvertedIndexQueryType::SEARCH_DSL_QUERY,
            FunctionSearch::dsl_cache_signature(fixture.param(root, options), nullptr)};
    InvertedIndexQueryCacheHandle handle;
    cache.get()->insert(key, std::make_shared<roaring::Roaring>(rows({1, 2})), &handle);

    fixture.expect(root, options, rows({1, 2}), rows({3}));
    EXPECT_FALSE(body.source->opened.empty());

    // The computed result is cached with its UNKNOWN rows.
    body.source->opened.clear();
    fixture.expect(root, options, rows({1, 2}), rows({3}));
    EXPECT_TRUE(body.source->opened.empty());
}

// A TopK takes the UNKNOWN rows from the scorer it ranks with instead of building and reading the
// query again.
TEST(FunctionSearchNullTest, TopKReadsTheQueryOnce) {
    NullSearchFixture fixture(4);
    auto& body = fixture.add_field("body");
    body.source->add("foo", {0, 1});
    body.nulls = rows({3});
    auto& other = fixture.add_field("other");
    other.source->add("bar", {1, 2});
    other.nulls = rows({0});

    fixture.expect(compound("OR", {leaf("TERM", "body", "foo"), leaf("TERM", "other", "bar")}),
                   scored_if(true, 3), rows({0, 1, 2}), rows({3}));
    EXPECT_EQ(std::vector<std::string> {"foo"}, body.source->opened);
    EXPECT_EQ(std::vector<std::string> {"bar"}, other.source->opened);
}

} // namespace doris
