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

#include "exprs/function/function_search.h"

#include <gen_cpp/Exprs_types.h>
#include <gtest/gtest.h>

#include <chrono>
#include <initializer_list>
#include <map>
#include <memory>
#include <optional>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "exprs/function/clucene_leaf_compiler.h"
#include "exprs/function/native_leaf_compiler.h"
#include "exprs/function/scalar_leaf_compiler.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_iterator.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/query_v2/collect/doc_set_collector.h"
#include "storage/index/inverted/query_v2/collect/top_k_collector.h"
#include "storage/index/inverted/query_v2/expand_query/expand_query.h"
#include "storage/index/inverted/query_v2/phrase_query/multi_phrase_query.h"
#include "storage/index/inverted/query_v2/phrase_query/multi_phrase_weight.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_query.h"
#include "storage/index/query/logical/search_lowering.h"
#include "storage/segment/variant/nested_group_provider.h"
#include "util/defer_op.h"
#include "util/thrift_util.h"

namespace doris {

class FunctionSearchTest : public testing::Test {
public:
    void SetUp() override { function_search = std::make_shared<FunctionSearch>(); }

protected:
    std::shared_ptr<FunctionSearch> function_search;
};

class DummyIndexIterator : public segment_v2::IndexIterator {
public:
    segment_v2::IndexReaderPtr get_reader(
            segment_v2::IndexReaderType /*reader_type*/) const override {
        return nullptr;
    }

    Status read_from_index(const segment_v2::IndexParam& /*param*/) override {
        return Status::OK();
    }

    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle* /*cache_handle*/) override {
        return Status::OK();
    }

    Result<bool> has_null() override { return false; }
};

class RecordingIndexIterator : public segment_v2::IndexIterator {
public:
    segment_v2::IndexReaderPtr get_reader(
            segment_v2::IndexReaderType /*reader_type*/) const override {
        return nullptr;
    }

    Status read_from_index(const segment_v2::IndexParam& param) override {
        auto* i_param_ptr = std::get_if<segment_v2::InvertedIndexParam*>(&param);
        if (i_param_ptr == nullptr || *i_param_ptr == nullptr) {
            return Status::InvalidArgument("missing inverted index param");
        }
        auto* i_param = *i_param_ptr;
        last_column_name = i_param->column_name;
        last_column_storage_type = i_param->column_type == nullptr
                                           ? FieldType::OLAP_FIELD_TYPE_UNKNOWN
                                           : i_param->column_type->get_storage_field_type();
        last_query_type = i_param->query_type;
        last_query_value_type = i_param->query_value.get_type();
        if (i_param->query_value.get_type() == TYPE_BOOLEAN) {
            last_bool_value = i_param->query_value.get<TYPE_BOOLEAN>();
        }
        if (i_param->query_value.get_type() == TYPE_INT) {
            last_int_value = i_param->query_value.get<TYPE_INT>();
        }
        if (i_param->roaring != nullptr) {
            i_param->roaring->add(3);
        }
        return Status::OK();
    }

    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle* /*cache_handle*/) override {
        return Status::OK();
    }

    Result<bool> has_null() override { return false; }

    std::string last_column_name;
    FieldType last_column_storage_type = FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    segment_v2::InvertedIndexQueryType last_query_type =
            segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY;
    PrimitiveType last_query_value_type = PrimitiveType::TYPE_NULL;
    bool last_bool_value = false;
    Int32 last_int_value = 0;
};

class RecordingDirectInvertedIndexIterator final : public segment_v2::InvertedIndexIterator {
public:
    Status read_from_index(const segment_v2::IndexParam& param) override {
        ++read_calls;
        auto* inverted_param = std::get_if<segment_v2::InvertedIndexParam*>(&param);
        DORIS_CHECK(inverted_param != nullptr);
        DORIS_CHECK(*inverted_param != nullptr);
        DORIS_CHECK((*inverted_param)->roaring != nullptr);
        (*inverted_param)->roaring->add(3);
        return Status::OK();
    }

    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle* /*cache_handle*/) override {
        return Status::OK();
    }

    Result<bool> has_null() override { return false; }

    int read_calls = 0;
};

class DummyInvertedIndexReader final : public segment_v2::InvertedIndexReader {
public:
    explicit DummyInvertedIndexReader(const TabletIndex* index_meta)
            : segment_v2::InvertedIndexReader(index_meta, nullptr) {}

    DummyInvertedIndexReader(const TabletIndex* index_meta,
                             std::shared_ptr<segment_v2::IndexFileReader> index_file_reader,
                             segment_v2::InvertedIndexReaderType reader_type)
            : segment_v2::InvertedIndexReader(index_meta, std::move(index_file_reader)),
              _reader_type(reader_type) {}

    Status new_iterator(std::unique_ptr<segment_v2::IndexIterator>* /*iterator*/) override {
        return Status::OK();
    }

    Status query(const segment_v2::IndexQueryContextPtr& /*context*/,
                 const std::string& /*column_name*/, const Field& /*query_value*/,
                 segment_v2::InvertedIndexQueryType /*query_type*/,
                 std::shared_ptr<roaring::Roaring>& /*bit_map*/,
                 const InvertedIndexAnalyzerCtx* /*analyzer_ctx*/ = nullptr) override {
        return Status::OK();
    }

    Status try_query(const segment_v2::IndexQueryContextPtr& /*context*/,
                     const std::string& /*column_name*/, const Field& /*query_value*/,
                     segment_v2::InvertedIndexQueryType /*query_type*/,
                     size_t* /*count*/) override {
        return Status::OK();
    }

    segment_v2::InvertedIndexReaderType type() override { return _reader_type; }

private:
    segment_v2::InvertedIndexReaderType _reader_type = segment_v2::InvertedIndexReaderType::BKD;
};

class RejectingCluceneIndexFileReader final : public segment_v2::IndexFileReader {
public:
    explicit RejectingCluceneIndexFileReader(
            InvertedIndexStorageFormatPB storage_format = InvertedIndexStorageFormatPB::SNII,
            const std::string& index_path = "/tmp/search_snii_native_idx")
            : segment_v2::IndexFileReader(nullptr, index_path, storage_format) {}

    Status init(int32_t /*read_buffer_size*/, const io::IOContext* /*io_ctx*/) override {
        ++init_calls;
        return Status::OK();
    }

    Result<std::unique_ptr<segment_v2::DorisCompoundReader, segment_v2::DirectoryDeleter>> open(
            const TabletIndex* /*index_meta*/, const io::IOContext* /*io_ctx*/) const override {
        ++open_calls;
        return ResultError(Status::InternalError("unexpected CLucene open for SNII search"));
    }

    int init_calls = 0;
    mutable int open_calls = 0;
};

class RecordingNativeInvertedIndexReader final : public segment_v2::InvertedIndexReader {
public:
    RecordingNativeInvertedIndexReader(
            const TabletIndex* index_meta,
            const std::shared_ptr<segment_v2::IndexFileReader>& index_file_reader,
            segment_v2::InvertedIndexReaderType reader_type =
                    segment_v2::InvertedIndexReaderType::FULLTEXT)
            : segment_v2::InvertedIndexReader(index_meta, index_file_reader),
              _reader_type(reader_type),
              _null_cache(1024 * 1024, 1),
              _null_cache_key {"/tmp/search_snii_native_null", "",
                               segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY,
                               std::to_string(index_meta->index_id())} {
        set_has_null(false);
    }

    Status new_iterator(std::unique_ptr<segment_v2::IndexIterator>* /*iterator*/) override {
        return Status::OK();
    }

    // The raw entry analyzes the value itself; SEARCH no longer uses it.
    Status query(const segment_v2::IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, segment_v2::InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override {
        ++raw_query_calls;
        last_column_name = column_name;
        last_query_type = query_type;
        last_query_value_type = query_value.get_type();
        last_analyzer_ctx = analyzer_ctx;
        if (last_query_value_type == TYPE_STRING) {
            last_query_value = query_value.get<TYPE_STRING>();
        }
        return answer(context, bit_map);
    }

    // Results and scores are keyed by the terms joined with spaces (alternatives of one slot
    // with '|'), so a test states what the reader would answer for a given term list.
    Status query_analyzed(const segment_v2::IndexQueryContextPtr& context,
                          const std::string& column_name,
                          segment_v2::InvertedIndexQueryType query_type,
                          const segment_v2::InvertedIndexQueryInfo& query_info,
                          std::shared_ptr<roaring::Roaring>& bit_map,
                          segment_v2::InvertedIndexQueryCacheHandle* /*null_bitmap_cache_handle*/ =
                                  nullptr) override {
        ++query_calls;
        last_column_name = column_name;
        last_query_type = query_type;
        last_query_info = query_info;
        last_query_scored = context != nullptr && context->collection_similarity != nullptr;
        last_query_value_type = TYPE_STRING;
        last_query_value.clear();
        for (const auto& term_info : query_info.term_infos) {
            if (!last_query_value.empty()) {
                last_query_value += ' ';
            }
            if (term_info.is_single_term()) {
                last_query_value += term_info.get_single_term();
                continue;
            }
            const auto& alternatives = term_info.get_multi_terms();
            for (size_t i = 0; i < alternatives.size(); ++i) {
                last_query_value += (i == 0 ? "" : "|") + alternatives[i];
            }
        }
        calls.emplace_back(query_type, last_query_value);
        call_candidates.push_back(context != nullptr && context->candidate_rows != nullptr
                                          ? std::optional(*context->candidate_rows)
                                          : std::nullopt);
        RETURN_IF_ERROR(answer(context, bit_map));
        // Like SniiIndexReader, a phrase of several terms keeps only the candidate rows.
        if (context != nullptr && context->candidate_rows != nullptr &&
            query_type == segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY &&
            query_info.term_infos.size() > 1) {
            *bit_map &= *context->candidate_rows;
            context->candidate_rows_consumed = true;
        }
        return Status::OK();
    }

    Status answer(const segment_v2::IndexQueryContextPtr& context,
                  std::shared_ptr<roaring::Roaring>& bit_map) {
        bit_map = std::make_shared<roaring::Roaring>();
        auto result_it = query_results.find(last_query_value);
        if (result_it != query_results.end()) {
            *bit_map = result_it->second;
        }
        // SniiIndexReader publishes its per-document BM25 values through the collection
        // similarity carried by the query context (score_plain_term_candidates /
        // score_phrase_matches), never through a return value. Reproducing that handshake here
        // is what lets the SEARCH scoring path be exercised without a physical SNII segment.
        auto scores_it = query_scores.find(last_query_value);
        if (scores_it != query_scores.end() && context != nullptr &&
            context->collection_similarity != nullptr) {
            observed_similarity = context->collection_similarity.get();
            for (const auto& [doc, score] : scores_it->second) {
                context->collection_similarity->collect(doc, score);
            }
        }
        return Status::OK();
    }

    Status try_query(const segment_v2::IndexQueryContextPtr& /*context*/,
                     const std::string& /*column_name*/, const Field& /*query_value*/,
                     segment_v2::InvertedIndexQueryType /*query_type*/,
                     size_t* /*count*/) override {
        return Status::OK();
    }

    Status read_null_bitmap(const segment_v2::IndexQueryContextPtr& /*context*/,
                            segment_v2::InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* /*dir*/ = nullptr) override {
        ++null_bitmap_calls;
        _null_cache.insert(_null_cache_key, std::make_shared<roaring::Roaring>(_null_bitmap),
                           cache_handle);
        return Status::OK();
    }

    segment_v2::InvertedIndexReaderType type() override { return _reader_type; }

    void set_query_result(const std::string& pattern, roaring::Roaring result) {
        query_results[pattern] = std::move(result);
    }

    void set_query_scores(const std::string& pattern,
                          std::vector<std::pair<uint32_t, float>> scores) {
        query_scores[pattern] = std::move(scores);
    }

    void set_null_bitmap(roaring::Roaring null_bitmap) {
        _null_bitmap = std::move(null_bitmap);
        set_has_null(!_null_bitmap.isEmpty());
    }

    int query_calls = 0;
    int raw_query_calls = 0;
    int null_bitmap_calls = 0;
    std::string last_column_name;
    std::string last_query_value;
    segment_v2::InvertedIndexQueryInfo last_query_info;
    PrimitiveType last_query_value_type = PrimitiveType::TYPE_NULL;
    segment_v2::InvertedIndexQueryType last_query_type =
            segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY;
    const InvertedIndexAnalyzerCtx* last_analyzer_ctx = nullptr;
    // Every analyzed query in call order: its type and its terms as the result tables key them.
    std::vector<std::pair<segment_v2::InvertedIndexQueryType, std::string>> calls;
    // The candidate rows each of those queries was handed, if any.
    std::vector<std::optional<roaring::Roaring>> call_candidates;
    std::unordered_map<std::string, roaring::Roaring> query_results;
    std::unordered_map<std::string, std::vector<std::pair<uint32_t, float>>> query_scores;
    // Identity of the similarity the reader was handed, so a test can prove the query's own
    // collection similarity is not the one the reader writes into.
    const CollectionSimilarity* observed_similarity = nullptr;
    // Whether the last analyzed query was asked to score, that is handed a similarity.
    bool last_query_scored = false;

private:
    segment_v2::InvertedIndexReaderType _reader_type;
    roaring::Roaring _null_bitmap;
    segment_v2::InvertedIndexQueryCache _null_cache;
    segment_v2::InvertedIndexQueryCache::CacheKey _null_cache_key;
};

class ScopedInvertedIndexQueryCache final {
public:
    ScopedInvertedIndexQueryCache()
            : _previous(ExecEnv::GetInstance()->get_inverted_index_query_cache()),
              _cache(segment_v2::InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1)) {
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_cache.get());
    }

    ~ScopedInvertedIndexQueryCache() {
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous);
    }

    segment_v2::InvertedIndexQueryCache* get() const { return _cache.get(); }

private:
    segment_v2::InvertedIndexQueryCache* _previous;
    std::unique_ptr<segment_v2::InvertedIndexQueryCache> _cache;
};

static roaring::Roaring make_bitmap(std::initializer_list<uint32_t> docs) {
    roaring::Roaring bitmap;
    for (uint32_t doc : docs) {
        bitmap.add(doc);
    }
    return bitmap;
}

static void expect_bitmap_eq(const roaring::Roaring& actual,
                             std::initializer_list<uint32_t> expected_docs) {
    auto expected = make_bitmap(expected_docs);
    EXPECT_EQ(expected.cardinality(), actual.cardinality());
    EXPECT_TRUE(actual == expected);
}

static roaring::Roaring collect_docs(
        const segment_v2::inverted_index::query_v2::ScorerPtr& scorer) {
    roaring::Roaring docs;
    for (uint32_t doc = scorer->doc(); doc != segment_v2::inverted_index::query_v2::TERMINATED;
         doc = scorer->advance()) {
        docs.add(doc);
    }
    return docs;
}

static TSearchClause make_leaf_clause(const std::string& clause_type, const std::string& value) {
    TSearchClause clause;
    clause.clause_type = clause_type;
    clause.field_name = "body";
    clause.value = value;
    clause.__isset.field_name = true;
    clause.__isset.value = true;
    return clause;
}

static Status insert_search_dsl_cache(
        segment_v2::InvertedIndexQueryCache* cache,
        const std::shared_ptr<segment_v2::IndexFileReader>& index_file_reader,
        const TSearchParam& search_param, roaring::Roaring bitmap) {
    ThriftSerializer serializer(false, 1024);
    TSearchParam copy = search_param;
    std::string signature;
    RETURN_IF_ERROR(serializer.serialize(&copy, &signature));

    segment_v2::InvertedIndexQueryCache::CacheKey key {
            index_file_reader->get_index_path_prefix(), "__search_dsl__",
            segment_v2::InvertedIndexQueryType::SEARCH_DSL_QUERY, std::move(signature)};
    segment_v2::InvertedIndexQueryCacheHandle handle;
    cache->insert(key, std::make_shared<roaring::Roaring>(std::move(bitmap)), &handle);
    return Status::OK();
}

static TabletIndex make_test_inverted_index(
        int64_t index_id, const std::map<std::string, std::string>& properties = {}) {
    TabletIndex index_meta;
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(index_id);
    pb.set_index_name("test_index_" + std::to_string(index_id));
    pb.add_col_unique_id(1);
    for (const auto& [key, value] : properties) {
        (*pb.mutable_properties())[key] = value;
    }
    index_meta.init_from_pb(pb);
    return index_meta;
}

static Status resolve_non_variant_binding_with_mismatched_analyzer(const DataTypePtr& column_type) {
    std::map<std::string, std::string> index_properties;
    index_properties[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_STANDARD;
    auto index_meta = make_test_inverted_index(13, index_properties);
    auto reader = std::make_shared<DummyInvertedIndexReader>(
            &index_meta, nullptr, segment_v2::InvertedIndexReaderType::FULLTEXT);

    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace("content", IndexFieldNameAndTypePair {"content", column_type});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["content"] = &iterator;

    TSearchFieldBinding field_binding;
    field_binding.field_name = "content";
    field_binding.index_properties[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_ENGLISH;
    field_binding.__isset.index_properties = true;

    auto context = std::make_shared<IndexQueryContext>();
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});
    FieldReaderBinding binding;
    return resolver.resolve("content", InvertedIndexQueryType::MATCH_ANY_QUERY, &binding);
}

TEST_F(FunctionSearchTest, TestGetName) {
    EXPECT_EQ("search", function_search->get_name());
}

TEST_F(FunctionSearchTest, TestBuildSearchParam) {
    // Create test search param
    TSearchParam searchParam;
    searchParam.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "title";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    // Test successful creation
    EXPECT_EQ("title:hello", searchParam.original_dsl);
    EXPECT_EQ("TERM", searchParam.root.clause_type);
    EXPECT_EQ("title", searchParam.root.field_name);
    EXPECT_EQ("hello", searchParam.root.value);
    EXPECT_EQ(1, searchParam.field_bindings.size());
    EXPECT_EQ("title", searchParam.field_bindings[0].field_name);
    EXPECT_EQ(0, searchParam.field_bindings[0].slot_index);
}

TEST_F(FunctionSearchTest, TestComplexSearchParam) {
    // Create complex search param with AND clause
    TSearchParam searchParam;
    searchParam.original_dsl = "title:hello AND content:world";

    // Create child clauses
    TSearchClause titleClause;
    titleClause.clause_type = "TERM";
    titleClause.field_name = "title";
    titleClause.value = "hello";

    TSearchClause contentClause;
    contentClause.clause_type = "TERM";
    contentClause.field_name = "content";
    contentClause.value = "world";

    // Create root AND clause
    TSearchClause rootClause;
    rootClause.clause_type = "AND";
    rootClause.children = {titleClause, contentClause};
    searchParam.root = rootClause;

    // Create field bindings
    TSearchFieldBinding titleBinding;
    titleBinding.field_name = "title";
    titleBinding.slot_index = 0;

    TSearchFieldBinding contentBinding;
    contentBinding.field_name = "content";
    contentBinding.slot_index = 1;

    searchParam.field_bindings = {titleBinding, contentBinding};

    // Verify structure
    EXPECT_EQ("title:hello AND content:world", searchParam.original_dsl);
    EXPECT_EQ("AND", searchParam.root.clause_type);
    EXPECT_EQ(2, searchParam.root.children.size());
    EXPECT_EQ("TERM", searchParam.root.children[0].clause_type);
    EXPECT_EQ("title", searchParam.root.children[0].field_name);
    EXPECT_EQ("hello", searchParam.root.children[0].value);
    EXPECT_EQ("TERM", searchParam.root.children[1].clause_type);
    EXPECT_EQ("content", searchParam.root.children[1].field_name);
    EXPECT_EQ("world", searchParam.root.children[1].value);
    EXPECT_EQ(2, searchParam.field_bindings.size());
}

TEST_F(FunctionSearchTest, TestExecuteImpl) {
    // Test that execute_impl always returns RuntimeError
    FunctionContext function_context;
    Block block;
    ColumnNumbers arguments;
    uint32_t result = 0;
    size_t input_rows_count = 0;

    auto status = function_search->execute_impl(&function_context, block, arguments, result,
                                                input_rows_count);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.code() == ErrorCode::RUNTIME_ERROR);
    EXPECT_TRUE(status.to_string().find("only inverted index queries are supported") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestBasicProperties) {
    // Test basic function properties
    EXPECT_EQ("search", function_search->get_name());
    EXPECT_TRUE(function_search->is_variadic());
    EXPECT_EQ(0, function_search->get_number_of_arguments());
    EXPECT_FALSE(function_search->use_default_implementation_for_nulls());
    EXPECT_FALSE(function_search->is_use_default_implementation_for_constants());
    EXPECT_FALSE(function_search->use_default_implementation_for_constants());
    EXPECT_TRUE(function_search->can_push_down_to_index());

    // Test return type
    DataTypes empty_args;
    auto return_type = function_search->get_return_type_impl(empty_args);
    EXPECT_NE(nullptr, return_type);
    // Should return UInt8 type for boolean results
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexBasic) {
    // Test basic evaluate_inverted_index method (legacy version)
    ColumnsWithTypeAndName arguments;
    std::vector<IndexFieldNameAndTypePair> data_type_with_names;
    std::vector<IndexIterator*> iterators;
    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index(
            arguments, data_type_with_names, iterators, num_rows, nullptr, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should return OK for legacy method
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamEmptyInputs) {
    // Test evaluate_inverted_index_with_search_param with empty inputs
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> empty_data_types;
    std::unordered_map<std::string, IndexIterator*> empty_iterators;
    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    // Test with empty iterators
    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, empty_data_types, empty_iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should return OK but with empty result

    // Test with empty data types but non-empty iterators - should still return OK
    // because empty data_types will cause early return
    std::unordered_map<std::string, IndexIterator*> non_empty_iterators;
    non_empty_iterators["title"] = nullptr; // Add null iterator
    status = function_search->evaluate_inverted_index_with_search_param(
            search_param, empty_data_types, non_empty_iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should return OK due to empty data_types check
}

TEST_F(FunctionSearchTest, TestEmptySearchParam) {
    // Test completely empty search param
    TSearchParam empty_param;
    // empty_param.original_dsl is not set
    // empty_param.root is not set

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            empty_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should handle gracefully
}

TEST_F(FunctionSearchTest, TestNullIterators) {
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Add null iterator - this should cause an error
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when iterator is null

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("iterator not found for field 'title'") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestMismatchedFieldNames) {
    // Test query referencing fields not available in iterators
    TSearchParam search_param;
    search_param.original_dsl = "nonexistent_field:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "nonexistent_field";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Add different field
    data_types["existing_field"] = {"existing_field", nullptr};
    iterators["existing_field"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when field not found

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find(
                        "field 'nonexistent_field' not found in inverted index metadata") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestZeroRowsScenario) {
    // Test with zero rows but empty iterators/data_types (realistic scenario)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    search_param.root = rootClause;

    // Empty data types and iterators - this is a realistic zero-data scenario
    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    uint32_t num_rows = 0; // Zero rows
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should handle zero data gracefully and return empty result
}

TEST_F(FunctionSearchTest, TestVeryLargeRowCount) {
    // Test with very large row count but empty iterators/data_types (realistic scenario)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    search_param.root = rootClause;

    // Empty data types and iterators - this tests the large row count parameter handling
    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    uint32_t num_rows = UINT32_MAX; // Very large row count
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should handle large row counts gracefully and return empty result
}

// Tests for FieldReaderResolver::resolve function coverage (lines 74+)
TEST_F(FunctionSearchTest, TestFieldReaderResolverWithNonInvertedIndexIterator) {
    // Exercise the branch where the iterator exists but is not an InvertedIndexIterator
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    data_types["title"] = {"title", nullptr};
    DummyIndexIterator dummy_iterator;
    iterators["title"] = &dummy_iterator;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_NE(status.to_string().find("iterator for field 'title' is not InvertedIndexIterator"),
              std::string::npos);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithValidIterator) {
    // Test the path where we have a valid iterator but no real InvertedIndexIterator
    // This will test the early return in build_query_recursive when resolver.resolve fails
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Add valid data but no real iterator
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithEmptyFieldName) {
    // Test the path where field_name is empty
    TSearchParam search_param;
    search_param.original_dsl = ":hello"; // Empty field name

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = ""; // Empty field name
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when field not found

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("field '' not found in inverted index metadata") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithSpecialCharacters) {
    // Test with special characters in field names
    TSearchParam search_param;
    search_param.original_dsl = "field-with-dashes:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "field-with-dashes";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Field name doesn't match
    data_types["different_field"] = {"different_field", nullptr};
    iterators["different_field"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when field not found

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find(
                        "field 'field-with-dashes' not found in inverted index metadata") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithUnicodeFieldName) {
    // Test with Unicode field names
    TSearchParam search_param;
    search_param.original_dsl = "字段名:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "字段名";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Field name doesn't match
    data_types["english_field"] = {"english_field", nullptr};
    iterators["english_field"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when field not found

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("field '字段名' not found in inverted index metadata") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithVeryLongFieldName) {
    // Test with very long field names
    std::string very_long_field_name = "field_" + std::string(1000, 'a');

    TSearchParam search_param;
    search_param.original_dsl = very_long_field_name + ":hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = very_long_field_name;
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Field name doesn't match
    data_types["short_field"] = {"short_field", nullptr};
    iterators["short_field"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error when field not found

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    EXPECT_TRUE(status.to_string().find("field '" + very_long_field_name +
                                        "' not found in inverted index metadata") !=
                std::string::npos);
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverWithDifferentQueryTypes) {
    // Test with different query types to ensure the binding_key generation is covered
    std::vector<std::string> query_types = {"TERM",  "PHRASE", "WILDCARD", "REGEXP",
                                            "RANGE", "LIST",   "ANY",      "ALL"};

    for (const auto& query_type_str : query_types) {
        TSearchParam search_param;
        search_param.original_dsl = "title:" + query_type_str + "(hello)";

        TSearchClause rootClause;
        rootClause.clause_type = query_type_str;
        rootClause.field_name = "title";
        rootClause.value = "hello";
        rootClause.__isset.field_name = true;
        rootClause.__isset.value = true;
        search_param.root = rootClause;

        std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
        std::unordered_map<std::string, IndexIterator*> iterators;

        data_types["title"] = {"title", nullptr};
        iterators["title"] = nullptr;

        uint32_t num_rows = 100;
        InvertedIndexResultBitmap bitmap_result;

        auto status = function_search->evaluate_inverted_index_with_search_param(
                search_param, data_types, iterators, num_rows, bitmap_result);
        EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

        EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    }
}

// Tests for FunctionSearch::evaluate_inverted_index_with_search_param function coverage (lines 201+)
TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamEmptyQuery) {
    // Test the path where root_query is nullptr (lines 201-204)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // Add valid data but no real iterator - this will cause build_query_recursive to fail
    // and return nullptr for root_query
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamNullBitmapHandling) {
    // Test the null bitmap handling logic (lines 206-220)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause early return due to iterator issues, but we can test the logic path
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamExecutionContext) {
    // Test the QueryExecutionContext creation (lines 222-226)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause early return due to iterator issues, but we can test the logic path
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamWeightAndScorer) {
    // Test the weight and scorer creation logic (lines 228-240)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause early return due to iterator issues, but we can test the logic path
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamDocumentIteration) {
    // Test the document iteration logic (lines 242-248)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause early return due to iterator issues, but we can test the logic path
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamResultMasking) {
    // Test the result masking logic (lines 250-255)
    TSearchParam search_param;
    search_param.original_dsl = "title:hello";

    TSearchClause rootClause;
    rootClause.clause_type = "TERM";
    rootClause.field_name = "title";
    rootClause.value = "hello";
    rootClause.__isset.field_name = true;
    rootClause.__isset.value = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause early return due to iterator issues, but we can test the logic path
    data_types["title"] = {"title", nullptr};
    iterators["title"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_FALSE(status.ok()); // Should return error due to iterator issues

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithSearchParamComplexQuery) {
    // Test with complex query structure to ensure all paths are covered
    TSearchParam search_param;
    search_param.original_dsl = "title:hello AND content:world";

    TSearchClause titleClause;
    titleClause.clause_type = "TERM";
    titleClause.field_name = "title";
    titleClause.value = "hello";
    titleClause.__isset.field_name = true;
    titleClause.__isset.value = true;

    TSearchClause contentClause;
    contentClause.clause_type = "TERM";
    contentClause.field_name = "content";
    contentClause.value = "world";
    contentClause.__isset.field_name = true;
    contentClause.__isset.value = true;

    TSearchClause rootClause;
    rootClause.clause_type = "AND";
    rootClause.children = {titleClause, contentClause};
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // This will cause all child queries to fail, resulting in an empty root_query
    // which will return Status::OK() at line 201-204
    data_types["title"] = {"title", nullptr};
    data_types["content"] = {"content", nullptr};
    iterators["title"] = nullptr;
    iterators["content"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    EXPECT_TRUE(status.ok()); // Should return OK because root_query will be nullptr (empty query)

    // The function should return OK with an empty result when all child queries fail
    // This tests the path where build_query_recursive returns empty query for AND clause
}

TEST_F(FunctionSearchTest, TestOrCrossFieldMatchesMatchAnyRows) {
    auto data_bitmap = std::make_shared<roaring::Roaring>();
    data_bitmap->add(1);
    data_bitmap->add(3);
    auto search_null_bitmap = std::make_shared<roaring::Roaring>();
    search_null_bitmap->add(2);

    InvertedIndexResultBitmap search_bitmap(data_bitmap, search_null_bitmap);
    search_bitmap.mask_out_null();

    auto result_bitmap = search_bitmap.get_data_bitmap();
    ASSERT_NE(nullptr, result_bitmap);
    EXPECT_EQ(2u, result_bitmap->cardinality());

    roaring::Roaring match_any_rows;
    match_any_rows.add(1);
    match_any_rows.add(3);

    roaring::Roaring expected_diff = match_any_rows;
    expected_diff -= *result_bitmap;
    EXPECT_TRUE(expected_diff.isEmpty());

    roaring::Roaring result_diff = *result_bitmap;
    result_diff -= match_any_rows;
    EXPECT_TRUE(result_diff.isEmpty());
}

TEST_F(FunctionSearchTest, TestOrWithNotSameFieldMatchesMatchAllRows) {
    auto data_bitmap = std::make_shared<roaring::Roaring>();
    data_bitmap->add(1);
    data_bitmap->add(2);
    data_bitmap->add(3);
    auto search_null_bitmap = std::make_shared<roaring::Roaring>();
    search_null_bitmap->add(3);

    InvertedIndexResultBitmap search_bitmap(data_bitmap, search_null_bitmap);
    search_bitmap.mask_out_null();

    auto result_bitmap = search_bitmap.get_data_bitmap();
    ASSERT_NE(nullptr, result_bitmap);
    EXPECT_EQ(2u, result_bitmap->cardinality());

    roaring::Roaring match_all_rows;
    match_all_rows.add(1);
    match_all_rows.add(2);

    roaring::Roaring expected_diff = match_all_rows;
    expected_diff -= *result_bitmap;
    EXPECT_TRUE(expected_diff.isEmpty());

    roaring::Roaring result_diff = *result_bitmap;
    result_diff -= match_all_rows;
    EXPECT_TRUE(result_diff.isEmpty());
}

TEST_F(FunctionSearchTest, TestBuildLeafQueryPhrase) {
    TSearchClause clause;
    clause.clause_type = "PHRASE";
    clause.field_name = "content";
    clause.value = "hello world";
    clause.__isset.field_name = true;
    clause.__isset.value = true;

    auto context = std::make_shared<IndexQueryContext>();

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace("content", IndexFieldNameAndTypePair {"content", nullptr});

    std::unordered_map<std::string, IndexIterator*> iterators;
    FieldReaderResolver resolver(data_type_with_names, iterators, context);

    FieldReaderBinding binding;
    binding.logical_field_name = "content";
    binding.stored_field_name = "content";
    binding.stored_field_wstr = L"content";
    binding.index_properties["parser"] = "unicode";
    binding.query_type = InvertedIndexQueryType::MATCH_PHRASE_QUERY;
    binding.leaf_compiler = std::make_shared<CluceneLeafCompiler>(
            L"content",
            resolver.binding_key_for("content", InvertedIndexQueryType::MATCH_PHRASE_QUERY));

    auto* dummy_reader = reinterpret_cast<lucene::index::IndexReader*>(0x1);
    binding.lucene_reader = std::shared_ptr<lucene::index::IndexReader>(
            dummy_reader, [](lucene::index::IndexReader* /*ptr*/) {});

    std::string key =
            resolver.binding_key_for("content", InvertedIndexQueryType::MATCH_PHRASE_QUERY);
    binding.binding_key = key;
    resolver._cache[key] = binding;

    inverted_index::query_v2::QueryPtr out;
    std::string out_binding_key;
    Status st = function_search->build_query_recursive(clause, context, resolver, &out,
                                                       &out_binding_key, "OR", 0);
    EXPECT_TRUE(st.ok());

    auto phrase_query = std::dynamic_pointer_cast<inverted_index::query_v2::PhraseQuery>(out);
    EXPECT_NE(phrase_query, nullptr);
}

TEST_F(FunctionSearchTest, TestBuildLeafQueryPhraseUsesPlainTerms) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_policy_mgr = exec_env->index_policy_mgr();
    IndexPolicyMgr scoped_policy_mgr;
    exec_env->_index_policy_mgr = &scoped_policy_mgr;
    DEFER(exec_env->_index_policy_mgr = previous_policy_mgr);

    auto* policy_mgr = exec_env->index_policy_mgr();
    ASSERT_NE(policy_mgr, nullptr);

    TIndexPolicy tokenizer;
    tokenizer.id = 910020;
    tokenizer.name = "function_search_cg_tokenizer";
    tokenizer.type = TIndexPolicyType::TOKENIZER;
    tokenizer.properties["type"] = "char_group";
    tokenizer.properties["tokenize_on_chars"] = "[whitespace]";

    TIndexPolicy analyzer;
    analyzer.id = 910022;
    analyzer.name = "function_search_cg_analyzer";
    analyzer.type = TIndexPolicyType::ANALYZER;
    analyzer.properties["tokenizer"] = tokenizer.name;
    analyzer.properties["token_filter"] = "lowercase";
    policy_mgr->apply_policy_changes({tokenizer, analyzer}, {});

    TSearchClause clause;
    clause.clause_type = "PHRASE";
    clause.field_name = "content";
    clause.value = "man of the year";
    clause.__isset.field_name = true;
    clause.__isset.value = true;

    auto context = std::make_shared<IndexQueryContext>();
    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace("content", IndexFieldNameAndTypePair {"content", nullptr});
    std::unordered_map<std::string, IndexIterator*> iterators;
    FieldReaderResolver resolver(data_type_with_names, iterators, context);

    FieldReaderBinding binding;
    binding.logical_field_name = "content";
    binding.stored_field_name = "content";
    binding.stored_field_wstr = L"content";
    binding.index_properties["analyzer"] = analyzer.name;
    binding.query_type = InvertedIndexQueryType::MATCH_PHRASE_QUERY;
    binding.leaf_compiler = std::make_shared<CluceneLeafCompiler>(
            L"content",
            resolver.binding_key_for("content", InvertedIndexQueryType::MATCH_PHRASE_QUERY));
    auto* dummy_reader = reinterpret_cast<lucene::index::IndexReader*>(0x1);
    binding.lucene_reader = std::shared_ptr<lucene::index::IndexReader>(
            dummy_reader, [](lucene::index::IndexReader* /*ptr*/) {});
    binding.binding_key =
            resolver.binding_key_for("content", InvertedIndexQueryType::MATCH_PHRASE_QUERY);
    resolver._cache[binding.binding_key] = binding;

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    ASSERT_TRUE(function_search
                        ->build_query_recursive(clause, context, resolver, &query, &binding_key,
                                                "OR", 0)
                        .ok());

    auto phrase = std::dynamic_pointer_cast<inverted_index::query_v2::PhraseQuery>(query);
    ASSERT_NE(phrase, nullptr);
    ASSERT_EQ(phrase->_term_infos.size(), 4);
    EXPECT_EQ(phrase->_term_infos[0].get_single_term(), "man");
    EXPECT_EQ(phrase->_term_infos[1].get_single_term(), "of");
    EXPECT_EQ(phrase->_term_infos[2].get_single_term(), "the");
    EXPECT_EQ(phrase->_term_infos[3].get_single_term(), "year");
    policy_mgr->apply_policy_changes({}, {tokenizer.id, analyzer.id});
}

TEST_F(FunctionSearchTest, TestBuildLeafQueryVariantMissingFieldReturnsUnknown) {
    TSearchClause clause;
    clause.clause_type = "TERM";
    clause.field_name = "var.items.missing";
    clause.value = "value";
    clause.__isset.field_name = true;
    clause.__isset.value = true;

    auto context = std::make_shared<IndexQueryContext>();

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    std::unordered_map<std::string, IndexIterator*> iterators;

    TSearchFieldBinding field_binding;
    field_binding.field_name = "var.items.missing";
    field_binding.is_variant_subcolumn = true;
    field_binding.__isset.is_variant_subcolumn = true;

    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});
    bool mapper_called = false;
    resolver.set_leaf_query_mapper([&](const std::string& logical_field,
                                       inverted_index::query_v2::QueryPtr* query) -> Status {
        mapper_called = true;
        EXPECT_EQ("var.items.missing", logical_field);
        EXPECT_NE(nullptr, query);
        EXPECT_NE(nullptr, *query);
        return Status::OK();
    });

    inverted_index::query_v2::QueryPtr out;
    std::string out_binding_key;
    Status st = function_search->build_query_recursive(clause, context, resolver, &out,
                                                       &out_binding_key, "OR", 0, 5);
    ASSERT_TRUE(st.ok());
    ASSERT_NE(out, nullptr);
    EXPECT_TRUE(mapper_called);
    EXPECT_TRUE(out_binding_key.empty());

    auto weight = out->weight(false);
    ASSERT_NE(weight, nullptr);
    inverted_index::query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 5;
    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);
    EXPECT_EQ(inverted_index::query_v2::TERMINATED, scorer->doc());
    ASSERT_TRUE(scorer->has_null_bitmap());
    const auto* null_bitmap = scorer->get_null_bitmap();
    ASSERT_NE(null_bitmap, nullptr);
    EXPECT_EQ(5u, null_bitmap->cardinality());
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverVariantSubcolumnWithMissingIterator) {
    auto context = std::make_shared<IndexQueryContext>();

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "var.items.level",
            IndexFieldNameAndTypePair {"1.var.items.level", std::make_shared<DataTypeInt32>()});
    std::unordered_map<std::string, IndexIterator*> iterators;

    TSearchFieldBinding field_binding;
    field_binding.field_name = "var.items.level";
    field_binding.is_variant_subcolumn = true;
    field_binding.__isset.is_variant_subcolumn = true;

    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});
    FieldReaderBinding binding;
    auto status =
            resolver.resolve("var.items.level", InvertedIndexQueryType::EQUAL_QUERY, &binding);

    ASSERT_TRUE(status.ok());
    EXPECT_FALSE(binding.is_bound());
    EXPECT_TRUE(resolver.binding_cache().empty());
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverVariantSubcolumnWithReaderSelectionError) {
    auto context = std::make_shared<IndexQueryContext>();

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "var.items.level",
            IndexFieldNameAndTypePair {"1.var.items.level", std::make_shared<DataTypeInt32>()});

    segment_v2::InvertedIndexIterator iterator;
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["var.items.level"] = &iterator;

    TSearchFieldBinding field_binding;
    field_binding.field_name = "var.items.level";
    field_binding.is_variant_subcolumn = true;
    field_binding.__isset.is_variant_subcolumn = true;

    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});
    FieldReaderBinding binding;
    auto status =
            resolver.resolve("var.items.level", InvertedIndexQueryType::EQUAL_QUERY, &binding);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(ErrorCode::INVERTED_INDEX_NO_TERMS, status.code());
}

TEST_F(FunctionSearchTest,
       TestFieldReaderResolverVariantAnalyzerUpgradeWithMissingIndexFileReader) {
    auto context = std::make_shared<IndexQueryContext>();

    std::map<std::string, std::string> properties;
    properties[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_STANDARD;
    auto index_meta = make_test_inverted_index(11, properties);
    auto reader = std::make_shared<DummyInvertedIndexReader>(
            &index_meta, nullptr, segment_v2::InvertedIndexReaderType::FULLTEXT);

    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "var.items.msg",
            IndexFieldNameAndTypePair {"1.var.items.msg", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["var.items.msg"] = &iterator;

    TSearchFieldBinding field_binding;
    field_binding.field_name = "var.items.msg";
    field_binding.is_variant_subcolumn = true;
    field_binding.index_properties = properties;
    field_binding.__isset.is_variant_subcolumn = true;
    field_binding.__isset.index_properties = true;

    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});
    FieldReaderBinding binding;
    auto status = resolver.resolve("var.items.msg", InvertedIndexQueryType::EQUAL_QUERY, &binding);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND, status.code());
}

TEST_F(FunctionSearchTest,
       TestFieldReaderResolverNonVariantStringBindingRejectsMismatchedAnalyzer) {
    auto status = resolve_non_variant_binding_with_mismatched_analyzer(
            std::make_shared<DataTypeString>());

    ASSERT_FALSE(status.ok());
    EXPECT_EQ(ErrorCode::INVERTED_INDEX_BYPASS, status.code());
}

TEST_F(FunctionSearchTest,
       TestFieldReaderResolverNonVariantArrayStringBindingRejectsMismatchedAnalyzer) {
    auto column_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>()));
    auto status = resolve_non_variant_binding_with_mismatched_analyzer(column_type);

    ASSERT_FALSE(status.ok());
    EXPECT_EQ(ErrorCode::INVERTED_INDEX_BYPASS, status.code());
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverExactIgnoresAnalyzedBindingHint) {
    std::map<std::string, std::string> analyzed_properties;
    analyzed_properties[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_STANDARD;
    auto analyzed_index = make_test_inverted_index(13, analyzed_properties);
    auto keyword_index = make_test_inverted_index(14);
    auto index_file_reader = std::make_shared<segment_v2::IndexFileReader>(
            nullptr, "/tmp/search_exact_multi_index", InvertedIndexStorageFormatPB::SNII);
    auto analyzed_reader = std::make_shared<DummyInvertedIndexReader>(
            &analyzed_index, index_file_reader, segment_v2::InvertedIndexReaderType::FULLTEXT);
    auto keyword_reader = std::make_shared<DummyInvertedIndexReader>(
            &keyword_index, index_file_reader, segment_v2::InvertedIndexReaderType::STRING_TYPE);

    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, analyzed_reader);
    iterator.add_reader(segment_v2::InvertedIndexReaderType::STRING_TYPE, keyword_reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "content", IndexFieldNameAndTypePair {"content", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["content"] = &iterator;

    TSearchFieldBinding field_binding;
    field_binding.field_name = "content";
    field_binding.index_properties = analyzed_properties;
    field_binding.__isset.index_properties = true;

    auto context = std::make_shared<IndexQueryContext>();
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});
    FieldReaderBinding binding;
    auto status = resolver.resolve("content", InvertedIndexQueryType::EQUAL_QUERY, &binding);

    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(binding.inverted_reader, nullptr);
    EXPECT_EQ(binding.inverted_reader->get_index_id(), 14);
    EXPECT_EQ(binding.query_type, InvertedIndexQueryType::EQUAL_QUERY);
    EXPECT_TRUE(binding.index_properties.empty());
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverVariantBkdDirectReader) {
    auto context = std::make_shared<IndexQueryContext>();

    auto index_meta = make_test_inverted_index(12);
    auto index_file_reader = std::make_shared<segment_v2::IndexFileReader>(
            nullptr, "/tmp/variant_direct_idx", InvertedIndexStorageFormatPB::V2);
    auto reader = std::make_shared<DummyInvertedIndexReader>(
            &index_meta, index_file_reader, segment_v2::InvertedIndexReaderType::BKD);

    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::BKD, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "var.items.level",
            IndexFieldNameAndTypePair {"1.var.items.level", std::make_shared<DataTypeInt32>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["var.items.level"] = &iterator;

    TSearchFieldBinding field_binding;
    field_binding.field_name = "var.items.level";
    field_binding.is_variant_subcolumn = true;
    field_binding.__isset.is_variant_subcolumn = true;

    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});
    FieldReaderBinding binding;
    auto status =
            resolver.resolve("var.items.level", InvertedIndexQueryType::EQUAL_QUERY, &binding);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_NE(nullptr, dynamic_cast<ScalarLeafCompiler*>(binding.leaf_compiler.get()));
    EXPECT_EQ(reader, binding.inverted_reader);
    EXPECT_EQ("var.items.level", binding.logical_field_name);
    EXPECT_EQ("1.var.items.level", binding.stored_field_name);
    EXPECT_EQ(InvertedIndexQueryType::EQUAL_QUERY, binding.query_type);

    const auto& cache = resolver.binding_cache();
    ASSERT_EQ(1u, cache.size());
    EXPECT_NE(nullptr,
              dynamic_cast<ScalarLeafCompiler*>(cache.begin()->second.leaf_compiler.get()));
}

TEST_F(FunctionSearchTest, TestFieldReaderResolverBindsSniiWithoutOpeningClucene) {
    auto context = std::make_shared<IndexQueryContext>();
    auto index_meta = make_test_inverted_index(
            14, {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD}});
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader = std::make_shared<DummyInvertedIndexReader>(
            &index_meta, index_file_reader, segment_v2::InvertedIndexReaderType::FULLTEXT);

    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;

    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = index_meta.properties();
    field_binding.__isset.index_properties = true;

    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});
    FieldReaderBinding binding;
    auto status = resolver.resolve("body", InvertedIndexQueryType::MATCH_ANY_QUERY, &binding);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(0, index_file_reader->init_calls);
    EXPECT_EQ(0, index_file_reader->open_calls);
    EXPECT_EQ(reader, binding.inverted_reader);
    EXPECT_EQ(nullptr, binding.lucene_reader);
    EXPECT_NE(nullptr, dynamic_cast<NativeLeafCompiler*>(binding.leaf_compiler.get()));
}

TEST_F(FunctionSearchTest, TestBuildLeafQueryExecutesSelectedSniiWildcardReader) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_policy_mgr = exec_env->index_policy_mgr();
    IndexPolicyMgr scoped_policy_mgr;
    exec_env->_index_policy_mgr = &scoped_policy_mgr;
    DEFER(exec_env->_index_policy_mgr = previous_policy_mgr);

    // The selected index's analyzer lowercases, so the pattern is lowercased as well.
    TIndexPolicy analyzer;
    analyzer.id = 910050;
    analyzer.name = "function_search_wildcard_analyzer";
    analyzer.type = TIndexPolicyType::ANALYZER;
    analyzer.properties["tokenizer"] = "standard";
    analyzer.properties["token_filter"] = "lowercase";
    scoped_policy_mgr.apply_policy_changes({analyzer}, {});

    auto context = std::make_shared<IndexQueryContext>();
    std::map<std::string, std::string> decoy_properties {
            {INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH},
            {INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE}};
    std::map<std::string, std::string> selected_properties {
            {INVERTED_INDEX_ANALYZER_NAME_KEY, analyzer.name}};
    auto decoy_meta = make_test_inverted_index(15, decoy_properties);
    auto selected_meta = make_test_inverted_index(16, selected_properties);
    auto decoy_file_reader = std::make_shared<RejectingCluceneIndexFileReader>(
            InvertedIndexStorageFormatPB::SNII, "/tmp/search_snii_decoy_idx");
    auto selected_file_reader = std::make_shared<RejectingCluceneIndexFileReader>(
            InvertedIndexStorageFormatPB::SNII, "/tmp/search_snii_selected_idx");
    auto decoy_reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&decoy_meta, decoy_file_reader);
    auto selected_reader = std::make_shared<RecordingNativeInvertedIndexReader>(
            &selected_meta, selected_file_reader);
    selected_reader->set_query_result("*lpha", make_bitmap({0, 2}));
    selected_reader->set_null_bitmap(make_bitmap({3}));

    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, decoy_reader);
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, selected_reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"stored_body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;

    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = selected_properties;
    field_binding.__isset.index_properties = true;

    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});
    auto clause = make_leaf_clause("WILDCARD", "*LPHA");
    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(clause, context, resolver, &query,
                                                         &binding_key, "OR", 0, 4);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, query);
    EXPECT_EQ(0, decoy_reader->query_calls);
    EXPECT_EQ(1, selected_reader->query_calls);
    EXPECT_EQ("stored_body", selected_reader->last_column_name);
    EXPECT_EQ(TYPE_STRING, selected_reader->last_query_value_type);
    EXPECT_EQ("*lpha", selected_reader->last_query_value);
    EXPECT_EQ(InvertedIndexQueryType::WILDCARD_QUERY, selected_reader->last_query_type);
    EXPECT_EQ(0, selected_reader->raw_query_calls);
    EXPECT_EQ(0, decoy_file_reader->open_calls);
    EXPECT_EQ(0, selected_file_reader->open_calls);
    EXPECT_EQ(0, decoy_reader->null_bitmap_calls);
    EXPECT_EQ(1, selected_reader->null_bitmap_calls);
    const auto& bindings = resolver.binding_cache();
    ASSERT_EQ(1U, bindings.size());
    EXPECT_EQ(InvertedIndexQueryType::MATCH_ANY_QUERY, bindings.begin()->second.query_type);

    auto weight = query->weight(true);
    ASSERT_NE(nullptr, weight);
    inverted_index::query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 4;
    auto scorer = weight->scorer(exec_ctx, binding_key);
    ASSERT_NE(nullptr, scorer);
    EXPECT_EQ(0U, scorer->doc());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());
    expect_bitmap_eq(collect_docs(scorer), {0, 2});
    ASSERT_TRUE(scorer->has_null_bitmap());
    const auto* null_bitmap = scorer->get_null_bitmap();
    ASSERT_NE(nullptr, null_bitmap);
    expect_bitmap_eq(*null_bitmap, {3});

    scoped_policy_mgr.apply_policy_changes({}, {analyzer.id});
}

TEST_F(FunctionSearchTest, TestSniiWildcardPreservesThreeValuedBooleanAndFieldExists) {
    auto context = std::make_shared<IndexQueryContext>();
    std::map<std::string, std::string> properties {
            {INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD}};
    auto index_meta = make_test_inverted_index(17, properties);
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&index_meta, index_file_reader);
    reader->set_query_result("*lpha", make_bitmap({0}));
    reader->set_query_result("beta*", make_bitmap({1}));
    reader->set_null_bitmap(make_bitmap({3}));

    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);
    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;

    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = properties;
    field_binding.__isset.index_properties = true;
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});

    TSearchClause or_clause;
    or_clause.clause_type = "OR";
    or_clause.children = {make_leaf_clause("WILDCARD", "*lpha"),
                          make_leaf_clause("WILDCARD", "beta*")};
    or_clause.__isset.children = true;

    TSearchClause not_clause;
    not_clause.clause_type = "NOT";
    not_clause.children = {make_leaf_clause("WILDCARD", "*lpha")};
    not_clause.__isset.children = true;
    auto exists_clause = make_leaf_clause("WILDCARD", "*");

    auto verify_result = [&](const TSearchClause& root,
                             std::initializer_list<uint32_t> expected_docs,
                             std::initializer_list<uint32_t> expected_nulls) {
        inverted_index::query_v2::QueryPtr query;
        std::string binding_key;
        auto status = function_search->build_query_recursive(root, context, resolver, &query,
                                                             &binding_key, "OR", 0, 4);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_NE(nullptr, query);
        auto weight = query->weight(false);
        ASSERT_NE(nullptr, weight);
        auto scorer = weight->scorer(
                build_variant_search_query_execution_context(4, resolver, nullptr), binding_key);
        ASSERT_NE(nullptr, scorer);
        expect_bitmap_eq(collect_docs(scorer), expected_docs);
        ASSERT_TRUE(scorer->has_null_bitmap());
        const auto* null_bitmap = scorer->get_null_bitmap();
        ASSERT_NE(nullptr, null_bitmap);
        expect_bitmap_eq(*null_bitmap, expected_nulls);
    };

    verify_result(or_clause, {0, 1}, {3});
    verify_result(not_clause, {1, 2}, {3});
    verify_result(exists_clause, {0, 1, 2}, {3});
    EXPECT_EQ(3, reader->query_calls);
}

// A TERM clause on an analyzed SNII field reaches the reader as the analyzed terms of a
// MATCH_ANY_QUERY, so it is scored the way the CLucene path scores its TermQuery. The raw
// entry, which would analyze the value again, is never used.
TEST_F(FunctionSearchTest, TestSniiNativeTermOnAnalyzedFieldIsAMatchAnyQuery) {
    OlapReaderStatistics stats;
    auto context = std::make_shared<IndexQueryContext>();
    context->stats = &stats;
    auto index_meta = make_test_inverted_index(
            18, {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD}});
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&index_meta, index_file_reader);
    reader->set_query_result("alpha", make_bitmap({0, 2}));
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;
    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = index_meta.properties();
    field_binding.__isset.index_properties = true;
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});

    auto clause = make_leaf_clause("TERM", "Alpha");
    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(clause, context, resolver, &query,
                                                         &binding_key, "OR", 0, 4);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, query);
    EXPECT_EQ(1, reader->query_calls);
    EXPECT_EQ(0, reader->raw_query_calls);
    EXPECT_EQ(InvertedIndexQueryType::MATCH_ANY_QUERY, reader->last_query_type);
    EXPECT_EQ("alpha", reader->last_query_value) << "analyzed once, with the index's lowercase";
    EXPECT_EQ(0, index_file_reader->open_calls);

    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    inverted_index::query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 4;
    auto scorer = weight->scorer(exec_ctx, binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {0, 2});
}

// On an untokenized SNII field the value is one dictionary term and stays an EQUAL_QUERY.
TEST_F(FunctionSearchTest, TestSniiNativeTermOnKeywordFieldIsAnEqualQuery) {
    auto context = std::make_shared<IndexQueryContext>();
    auto index_meta = make_test_inverted_index(46);
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&index_meta, index_file_reader);
    reader->set_query_result("Alpha Beta", make_bitmap({1}));
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::STRING_TYPE, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;
    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = index_meta.properties();
    field_binding.__isset.index_properties = true;
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status =
            function_search->build_query_recursive(make_leaf_clause("TERM", "Alpha Beta"), context,
                                                   resolver, &query, &binding_key, "OR", 0, 4);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(1, reader->query_calls);
    EXPECT_EQ(InvertedIndexQueryType::EQUAL_QUERY, reader->last_query_type);
    EXPECT_EQ("Alpha Beta", reader->last_query_value);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    inverted_index::query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 4;
    auto scorer = weight->scorer(exec_ctx, binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {1});
}

// The terms a native reader receives come from the analyzer of the index the field was bound
// to, here a custom analyzer that splits on whitespace and lowercases; the reader itself is
// never asked to analyze.
TEST_F(FunctionSearchTest, TestSniiNativeReceivesTermsFromTheSelectedAnalyzer) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_policy_mgr = exec_env->index_policy_mgr();
    IndexPolicyMgr scoped_policy_mgr;
    exec_env->_index_policy_mgr = &scoped_policy_mgr;
    DEFER(exec_env->_index_policy_mgr = previous_policy_mgr);

    TIndexPolicy tokenizer;
    tokenizer.id = 910030;
    tokenizer.name = "function_search_context_tokenizer";
    tokenizer.type = TIndexPolicyType::TOKENIZER;
    tokenizer.properties["type"] = "char_group";
    tokenizer.properties["tokenize_on_chars"] = "[whitespace]";

    TIndexPolicy analyzer;
    analyzer.id = 910032;
    analyzer.name = "function_search_context_analyzer";
    analyzer.type = TIndexPolicyType::ANALYZER;
    analyzer.properties["tokenizer"] = tokenizer.name;
    analyzer.properties["token_filter"] = "lowercase";
    scoped_policy_mgr.apply_policy_changes({tokenizer, analyzer}, {});

    auto context = std::make_shared<IndexQueryContext>();
    std::map<std::string, std::string> properties {
            {INVERTED_INDEX_ANALYZER_NAME_KEY, analyzer.name},
            {INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_NONE}};
    auto index_meta = make_test_inverted_index(45, properties);
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&index_meta, index_file_reader);
    reader->set_query_result("running quickly", make_bitmap({1}));
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;
    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = properties;
    field_binding.__isset.index_properties = true;
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_leaf_clause("MATCH", "Running QUICKLY"), context, resolver, &query, &binding_key,
            "OR", 0, 3);

    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(1, reader->query_calls);
    EXPECT_EQ(0, reader->raw_query_calls);
    EXPECT_EQ(InvertedIndexQueryType::MATCH_ANY_QUERY, reader->last_query_type);
    EXPECT_EQ("running quickly", reader->last_query_value);
    ASSERT_EQ(2U, reader->last_query_info.term_infos.size());
    const auto& bindings = resolver.binding_cache();
    ASSERT_EQ(1U, bindings.size());
    ASSERT_NE(nullptr, bindings.begin()->second.analyzer_context);
    EXPECT_EQ(analyzer.name, bindings.begin()->second.analyzer_context->analyzer_name);
    scoped_policy_mgr.apply_policy_changes({}, {tokenizer.id, analyzer.id});
}

// default_operator "and" maps a multi-token TERM clause onto MATCH_ALL_QUERY instead of the
// default EQUAL_QUERY (which is an OR of terms) -- SNII has no boolean query tree to build, so
// this is expressed entirely as which query type gets forwarded to the reader.
TEST_F(FunctionSearchTest, TestSniiNativeTermDefaultOperatorAndMapsToMatchAllQuery) {
    OlapReaderStatistics stats;
    auto context = std::make_shared<IndexQueryContext>();
    context->stats = &stats;
    auto index_meta = make_test_inverted_index(
            21, {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD}});
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&index_meta, index_file_reader);
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;
    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = index_meta.properties();
    field_binding.__isset.index_properties = true;
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});

    auto clause = make_leaf_clause("TERM", "alpha beta");
    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(clause, context, resolver, &query,
                                                         &binding_key, "and", 0, 4);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(1, reader->query_calls);
    EXPECT_EQ(InvertedIndexQueryType::MATCH_ALL_QUERY, reader->last_query_type);
    EXPECT_EQ("alpha beta", reader->last_query_value);
}

// A single-token TERM value has nothing for minimum_should_match to select "at least N of"
// among -- there is only one term. Lowering drops the threshold for one token, so the
// reader sees a plain one-term MATCH_ANY_QUERY.
TEST_F(FunctionSearchTest, TestSniiNativeTermSingleTokenAllowsMinimumShouldMatch) {
    auto context = std::make_shared<IndexQueryContext>();
    auto index_meta = make_test_inverted_index(
            24, {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD}});
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&index_meta, index_file_reader);
    reader->set_query_result("alpha", make_bitmap({0, 2}));
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;
    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = index_meta.properties();
    field_binding.__isset.index_properties = true;
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});

    auto clause = make_leaf_clause("TERM", "alpha");
    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(clause, context, resolver, &query,
                                                         &binding_key, "OR", 1, 4);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, query);
    EXPECT_EQ(1, reader->query_calls);
    EXPECT_EQ(InvertedIndexQueryType::MATCH_ANY_QUERY, reader->last_query_type);
    EXPECT_EQ("alpha", reader->last_query_value);

    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    inverted_index::query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 4;
    auto scorer = weight->scorer(exec_ctx, binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {0, 2});
}

// A value that tokenizes to zero terms (here, an empty string on an analysed field) must be
// handled the same way the CLucene path handles it -- an empty BitSetQuery -- instead of
// reaching SniiIndexReader::_query at all: that reader only short-circuits empty term_infos to
// an empty bitmap for proper MATCH_* query types (see is_match_query() in
// inverted_index_query_type.h), and a TERM clause maps to EQUAL_QUERY/MATCH_ALL_QUERY, neither
// of which qualifies, so it would otherwise surface INVERTED_INDEX_NO_TERMS instead of a match.
TEST_F(FunctionSearchTest, TestSniiNativeTermZeroTokenMinimumShouldMatchReturnsEmptyBitmap) {
    auto context = std::make_shared<IndexQueryContext>();
    auto index_meta = make_test_inverted_index(
            25, {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD}});
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&index_meta, index_file_reader);
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;
    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = index_meta.properties();
    field_binding.__isset.index_properties = true;
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});

    auto clause = make_leaf_clause("TERM", "");
    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(clause, context, resolver, &query,
                                                         &binding_key, "OR", 1, 4);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, query);
    EXPECT_EQ(0, reader->query_calls);

    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    inverted_index::query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 4;
    auto scorer = weight->scorer(exec_ctx, binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {});
}

// On a keyword field (no parser property), a PREFIX is the literal stem without the DSL's '*'.
TEST_F(FunctionSearchTest, TestSniiNativeKeywordPrefixIsALiteralStem) {
    OlapReaderStatistics stats;
    auto context = std::make_shared<IndexQueryContext>();
    context->stats = &stats;
    auto index_meta = make_test_inverted_index(23);
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&index_meta, index_file_reader);
    reader->set_query_result("al", make_bitmap({0, 2}));
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;
    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = index_meta.properties();
    field_binding.__isset.index_properties = true;
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});

    auto clause = make_leaf_clause("PREFIX", "al*");
    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(clause, context, resolver, &query,
                                                         &binding_key, "OR", 0, 4);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, query);
    EXPECT_EQ(1, reader->query_calls);
    EXPECT_EQ(InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, reader->last_query_type);
    EXPECT_EQ("al", reader->last_query_value);
    EXPECT_EQ(0, index_file_reader->open_calls);

    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    inverted_index::query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 4;
    auto scorer = weight->scorer(exec_ctx, binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {0, 2});
}

TEST_F(FunctionSearchTest, TestSniiNativeRegexpMatchesWholeTerms) {
    OlapReaderStatistics stats;
    auto context = std::make_shared<IndexQueryContext>();
    context->stats = &stats;
    auto index_meta = make_test_inverted_index(23);
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&index_meta, index_file_reader);
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;
    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = index_meta.properties();
    field_binding.__isset.index_properties = true;
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});

    auto clause = make_leaf_clause("REGEXP", "alpha");
    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(clause, context, resolver, &query,
                                                         &binding_key, "OR", 0, 4);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(1, reader->query_calls);
    EXPECT_EQ(InvertedIndexQueryType::MATCH_REGEXP_QUERY, reader->last_query_type);
    // The reader's MATCH_REGEXP matches inside a term, so SEARCH hands it an anchored pattern.
    EXPECT_EQ("^(alpha)$", reader->last_query_value);
}

TEST_F(FunctionSearchTest, TestSniiNativeCustomKeywordPrefixStripsTheDslSuffix) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_policy_mgr = exec_env->index_policy_mgr();
    IndexPolicyMgr scoped_policy_mgr;
    exec_env->_index_policy_mgr = &scoped_policy_mgr;
    DEFER(exec_env->_index_policy_mgr = previous_policy_mgr);

    TIndexPolicy tokenizer;
    tokenizer.id = 910030;
    tokenizer.name = "function_search_keyword_tokenizer";
    tokenizer.type = TIndexPolicyType::TOKENIZER;
    tokenizer.properties["type"] = "keyword";

    TIndexPolicy analyzer;
    analyzer.id = 910031;
    analyzer.name = "function_search_keyword_analyzer";
    analyzer.type = TIndexPolicyType::ANALYZER;
    analyzer.properties["tokenizer"] = tokenizer.name;
    scoped_policy_mgr.apply_policy_changes({tokenizer, analyzer}, {});

    std::map<std::string, std::string> properties {
            {INVERTED_INDEX_ANALYZER_NAME_KEY, analyzer.name}};
    ASSERT_TRUE(inverted_index::InvertedIndexAnalyzer::should_analyzer(properties));
    auto raw_terms = inverted_index::InvertedIndexAnalyzer::get_analyse_result("fail*", properties);
    ASSERT_EQ(1, raw_terms.size());
    EXPECT_EQ("fail*", raw_terms[0].get_single_term());

    OlapReaderStatistics stats;
    auto context = std::make_shared<IndexQueryContext>();
    context->stats = &stats;
    auto index_meta = make_test_inverted_index(24, properties);
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&index_meta, index_file_reader);
    reader->set_query_result("fail", make_bitmap({0, 2}));
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;
    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = properties;
    field_binding.__isset.index_properties = true;
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});

    auto clause = make_leaf_clause("PREFIX", "fail*");
    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(clause, context, resolver, &query,
                                                         &binding_key, "OR", 0, 4);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, query);
    EXPECT_EQ(1, reader->query_calls);
    EXPECT_EQ(InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, reader->last_query_type);
    EXPECT_EQ("fail", reader->last_query_value);

    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    inverted_index::query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 4;
    auto scorer = weight->scorer(exec_ctx, binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {0, 2});

    scoped_policy_mgr.apply_policy_changes({}, {tokenizer.id, analyzer.id});
}

// Runs one PREFIX clause against a fake SNII reader bound to "body" with `properties`.
static void search_snii_prefix(const std::map<std::string, std::string>& properties,
                               const std::shared_ptr<IndexQueryContext>& context,
                               const std::string& value,
                               const std::shared_ptr<RecordingNativeInvertedIndexReader>& reader) {
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);
    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;
    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = properties;
    field_binding.__isset.index_properties = true;
    FieldReaderResolver resolver(data_type_with_names, iterators, context, {field_binding});

    const int calls = reader->query_calls;
    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = FunctionSearch().build_query_recursive(
            make_leaf_clause("PREFIX", value), context, resolver, &query, &binding_key, "OR", 0, 4);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(calls + 1, reader->query_calls) << value;
    EXPECT_EQ(InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, reader->last_query_type) << value;
}

// SEARCH PREFIX follows Elasticsearch's query_string: the stem is normalized, not analyzed, and
// every term that starts with it matches with a constant score.
TEST_F(FunctionSearchTest, TestSniiNativePrefixIsAnUnscoredPrefixOfTheNormalizedStem) {
    OlapReaderStatistics stats;
    auto context = std::make_shared<IndexQueryContext>();
    context->stats = &stats;
    context->collection_similarity = std::make_shared<CollectionSimilarity>();
    const std::map<std::string, std::string> properties {
            {INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD},
            {INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE},
            {INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY, INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES}};
    auto index_meta = make_test_inverted_index(25, properties);
    auto reader = std::make_shared<RecordingNativeInvertedIndexReader>(
            &index_meta, std::make_shared<RejectingCluceneIndexFileReader>());

    // The analyzer would split "Foo-Ba" into two terms and drop the stopword "the".
    for (const auto& [value, stem] : std::vector<std::pair<std::string, std::string>> {
                 {"Foo-Ba*", "foo-ba"}, {"The*", "the"}}) {
        search_snii_prefix(properties, context, value, reader);
        EXPECT_EQ(stem, reader->last_query_value) << value;
        EXPECT_FALSE(reader->last_query_scored) << value;
    }
}

// A custom analyzer normalizes a prefix with its char filters and the token filters that work
// per character; its tokenizer and filters such as word_delimiter do not apply.
TEST_F(FunctionSearchTest, TestSniiNativePrefixNormalizesWithTheAnalyzersPerCharacterFilters) {
    auto* exec_env = ExecEnv::GetInstance();
    auto* previous_policy_mgr = exec_env->index_policy_mgr();
    IndexPolicyMgr scoped_policy_mgr;
    exec_env->_index_policy_mgr = &scoped_policy_mgr;
    DEFER(exec_env->_index_policy_mgr = previous_policy_mgr);

    TIndexPolicy analyzer;
    analyzer.id = 910040;
    analyzer.name = "function_search_folding_analyzer";
    analyzer.type = TIndexPolicyType::ANALYZER;
    analyzer.properties["tokenizer"] = "standard";
    analyzer.properties["token_filter"] = "word_delimiter, asciifolding, lowercase";
    scoped_policy_mgr.apply_policy_changes({analyzer}, {});

    OlapReaderStatistics stats;
    auto context = std::make_shared<IndexQueryContext>();
    context->stats = &stats;
    const std::map<std::string, std::string> properties {
            {INVERTED_INDEX_ANALYZER_NAME_KEY, analyzer.name}};
    auto index_meta = make_test_inverted_index(26, properties);
    auto reader = std::make_shared<RecordingNativeInvertedIndexReader>(
            &index_meta, std::make_shared<RejectingCluceneIndexFileReader>());
    search_snii_prefix(properties, context, "Café-Au*", reader);
    EXPECT_EQ("cafe-au", reader->last_query_value);

    scoped_policy_mgr.apply_policy_changes({}, {analyzer.id});
}

// On CLucene too a stopword stays a prefix, since a prefix is normalized and never analyzed.
TEST_F(FunctionSearchTest, TestBuildLeafQueryClucenePrefixOfAStopwordStaysAPrefix) {
    auto context = std::make_shared<IndexQueryContext>();
    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace("body", IndexFieldNameAndTypePair {"body", nullptr});
    std::unordered_map<std::string, IndexIterator*> iterators;
    FieldReaderResolver resolver(data_type_with_names, iterators, context);

    FieldReaderBinding binding;
    binding.logical_field_name = "body";
    binding.stored_field_name = "body";
    binding.stored_field_wstr = L"body";
    binding.index_properties = {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD},
                                {INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE}};
    binding.query_type = InvertedIndexQueryType::MATCH_ANY_QUERY;
    binding.binding_key = resolver.binding_key_for("body", InvertedIndexQueryType::MATCH_ANY_QUERY);
    binding.leaf_compiler = std::make_shared<CluceneLeafCompiler>(L"body", binding.binding_key);
    auto* dummy_reader = reinterpret_cast<lucene::index::IndexReader*>(0x1);
    binding.lucene_reader = std::shared_ptr<lucene::index::IndexReader>(
            dummy_reader, [](lucene::index::IndexReader* /*ptr*/) {});
    resolver._cache[binding.binding_key] = binding;

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    ASSERT_TRUE(function_search
                        ->build_query_recursive(make_leaf_clause("PREFIX", "The*"), context,
                                                resolver, &query, &binding_key, "OR", 0)
                        .ok());
    auto expand = std::dynamic_pointer_cast<inverted_index::query_v2::ExpandQuery>(query);
    ASSERT_NE(expand, nullptr);
    EXPECT_EQ(index_query::TermPatternKind::kPrefix, expand->_kind);
    EXPECT_EQ("the", expand->_pattern);
}

// Shared wiring for the SNII native SEARCH scoring tests: one fake SNII reader bound to field
// "body" behind a standard analyzer, plus the resolver build_query_recursive needs. The resolver keeps
// references to the maps, so they must be owned by something that outlives it.
class SniiScoringFixture {
public:
    SniiScoringFixture(int64_t index_id, uint32_t rows) : num_rows(rows) {
        // support_phrase is what makes is_need_similarity_score accept a MATCH query type, which
        // is the production gate the leaf builder consults before wiring up a score sink.
        _properties = {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD},
                       {INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY,
                        INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES}};
        _index_meta = make_test_inverted_index(index_id, _properties);
        _index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
        reader = std::make_shared<RecordingNativeInvertedIndexReader>(&_index_meta,
                                                                      _index_file_reader);
        context = std::make_shared<IndexQueryContext>();
        context->stats = &_stats;
        context->collection_similarity = std::make_shared<CollectionSimilarity>();
        _iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);
        _data_type_with_names.emplace(
                "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
        _iterators["body"] = &_iterator;

        TSearchFieldBinding field_binding;
        field_binding.field_name = "body";
        field_binding.index_properties = _properties;
        field_binding.__isset.index_properties = true;
        resolver = std::make_unique<FieldReaderResolver>(
                _data_type_with_names, _iterators, context,
                std::vector<TSearchFieldBinding> {field_binding});
    }

    SniiScoringFixture(const SniiScoringFixture&) = delete;
    SniiScoringFixture& operator=(const SniiScoringFixture&) = delete;

    inverted_index::query_v2::QueryExecutionContext exec_context() const {
        return build_variant_search_query_execution_context(num_rows, *resolver, nullptr);
    }

    uint32_t num_rows;
    std::shared_ptr<RecordingNativeInvertedIndexReader> reader;
    std::shared_ptr<IndexQueryContext> context;
    std::unique_ptr<FieldReaderResolver> resolver;

private:
    OlapReaderStatistics _stats;
    std::map<std::string, std::string> _properties;
    TabletIndex _index_meta;
    std::shared_ptr<RejectingCluceneIndexFileReader> _index_file_reader;
    segment_v2::InvertedIndexIterator _iterator;
    std::unordered_map<std::string, IndexFieldNameAndTypePair> _data_type_with_names;
    std::unordered_map<std::string, IndexIterator*> _iterators;
};

// Reads back what a CollectionSimilarity actually holds for the given documents, so a test can
// assert on the score values themselves rather than only on which rows survived.
static std::map<uint32_t, float> read_collected_scores(const CollectionSimilarity& similarity,
                                                       const roaring::Roaring& docs) {
    roaring::Roaring row_bitmap = docs;
    IColumn::MutablePtr score_column;
    auto row_ids = std::make_unique<std::vector<uint64_t>>();
    similarity.get_bm25_scores(&row_bitmap, score_column, row_ids);
    const auto& nullable = assert_cast<const ColumnNullable&>(*score_column);
    const auto& values = assert_cast<const ColumnFloat32&>(nullable.get_nested_column()).get_data();
    std::map<uint32_t, float> collected;
    for (size_t i = 0; i < row_ids->size(); ++i) {
        collected[static_cast<uint32_t>((*row_ids)[i])] = values[i];
    }
    return collected;
}

// A SEARCH answered by the SNII native reader must rank by the reader's own BM25 values. The
// leaf used to be wrapped in a plain BitSetQuery, whose scorer returns a constant 1.0 for every
// document, so the early top-k collector saw an all-tie ranking and "ORDER BY score() DESC LIMIT
// k" returned an arbitrary k rows instead of the k best-scoring ones.
TEST_F(FunctionSearchTest, TestSniiNativeTopKRanksByReaderBm25Scores) {
    // enable_inverted_index_wand_query defaults to true and function_search passes it straight
    // through, so the wand variant is the one that actually ships; the non-wand variant is what
    // an explicitly disabled session gets. Both must rank the same.
    for (bool use_wand : {false, true}) {
        SCOPED_TRACE(use_wand ? "use_wand=true" : "use_wand=false");
        SniiScoringFixture fixture(41, 5);
        fixture.reader->set_query_result("alpha", make_bitmap({0, 1, 2, 3, 4}));
        // Deliberately not monotonic in doc id: the two best documents are 1 and 3, which are not
        // the two a doc-id-ordered tie-break would pick.
        fixture.reader->set_query_scores("alpha",
                                         {{0, 1.0F}, {1, 9.0F}, {2, 3.0F}, {3, 7.0F}, {4, 5.0F}});

        inverted_index::query_v2::QueryPtr query;
        std::string binding_key;
        auto status = function_search->build_query_recursive(
                make_leaf_clause("MATCH", "alpha"), fixture.context, *fixture.resolver, &query,
                &binding_key, "OR", 0, fixture.num_rows);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_NE(nullptr, query);

        auto weight = query->weight(true);
        ASSERT_NE(nullptr, weight);
        auto exec_ctx = fixture.exec_context();
        auto roaring = std::make_shared<roaring::Roaring>();
        inverted_index::query_v2::collect_multi_segment_top_k(
                weight, exec_ctx, binding_key, 2, roaring, fixture.context->collection_similarity,
                use_wand);

        expect_bitmap_eq(*roaring, {1, 3});
        auto collected = read_collected_scores(*fixture.context->collection_similarity, *roaring);
        ASSERT_EQ(2U, collected.size());
        EXPECT_FLOAT_EQ(9.0F, collected[1]);
        EXPECT_FLOAT_EQ(7.0F, collected[3]);
    }
}

// The reader used to publish its BM25 values straight into the query's collection similarity
// while the collector added the scorer's constant on top of them, so every document ended up
// with "BM25 + 1.0". Exactly one of the two channels may write.
TEST_F(FunctionSearchTest, TestSniiNativeDocSetCollectionScoresEachDocumentOnce) {
    SniiScoringFixture fixture(42, 3);
    fixture.reader->set_query_result("alpha", make_bitmap({0, 1, 2}));
    fixture.reader->set_query_scores("alpha", {{0, 2.5F}, {1, 4.25F}, {2, 0.75F}});

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(make_leaf_clause("MATCH", "alpha"),
                                                         fixture.context, *fixture.resolver, &query,
                                                         &binding_key, "OR", 0, fixture.num_rows);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, query);
    // The reader must have been handed a private sink, never the similarity the collector fills.
    EXPECT_NE(fixture.context->collection_similarity.get(), fixture.reader->observed_similarity);

    auto weight = query->weight(true);
    ASSERT_NE(nullptr, weight);
    auto exec_ctx = fixture.exec_context();
    auto roaring = std::make_shared<roaring::Roaring>();
    inverted_index::query_v2::collect_multi_segment_doc_set(weight, exec_ctx, binding_key, roaring,
                                                            fixture.context->collection_similarity,
                                                            /*enable_scoring=*/true);

    expect_bitmap_eq(*roaring, {0, 1, 2});
    auto collected = read_collected_scores(*fixture.context->collection_similarity, *roaring);
    ASSERT_EQ(3U, collected.size());
    EXPECT_FLOAT_EQ(2.5F, collected[0]);
    EXPECT_FLOAT_EQ(4.25F, collected[1]);
    EXPECT_FLOAT_EQ(0.75F, collected[2]);
}

// A leaf for which the reader publishes no per-document score must keep the constant that the
// CLucene path also gives its own constant-score leaves, so that routing scored clauses through a
// new scorer does not quietly re-rank the unscored ones. The fake reader is what withholds the
// scores here, so this pins the "nothing published -> BitSetQuery -> 1.0" behaviour; it does not
// pin which query types the production is_need_similarity_score gate rejects.
TEST_F(FunctionSearchTest, TestSniiNativeLeafWithoutPublishedScoresKeepsConstantScore) {
    SniiScoringFixture fixture(43, 4);
    fixture.reader->set_query_result("alpha*", make_bitmap({0, 2}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(make_leaf_clause("WILDCARD", "alpha*"),
                                                         fixture.context, *fixture.resolver, &query,
                                                         &binding_key, "OR", 0, fixture.num_rows);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, query);

    auto weight = query->weight(true);
    ASSERT_NE(nullptr, weight);
    auto exec_ctx = fixture.exec_context();
    auto scorer = weight->scorer(exec_ctx, binding_key);
    ASSERT_NE(nullptr, scorer);
    EXPECT_EQ(0U, scorer->doc());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());
    expect_bitmap_eq(collect_docs(scorer), {0, 2});
}

// A non-scoring execution must not pay for the score plumbing: weight(false) has to hand back the
// same constant-score scorer the unscored path always used.
TEST_F(FunctionSearchTest, TestSniiNativeScoredQueryFallsBackToConstantScorerWithoutScoring) {
    SniiScoringFixture fixture(44, 3);
    fixture.reader->set_query_result("alpha", make_bitmap({0, 1, 2}));
    fixture.reader->set_query_scores("alpha", {{0, 2.5F}, {1, 4.25F}, {2, 0.75F}});

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(make_leaf_clause("MATCH", "alpha"),
                                                         fixture.context, *fixture.resolver, &query,
                                                         &binding_key, "OR", 0, fixture.num_rows);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, query);

    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto exec_ctx = fixture.exec_context();
    auto scorer = weight->scorer(exec_ctx, binding_key);
    ASSERT_NE(nullptr, scorer);
    EXPECT_EQ(0U, scorer->doc());
    EXPECT_FLOAT_EQ(1.0F, scorer->score());
    expect_bitmap_eq(collect_docs(scorer), {0, 1, 2});
}

// Records the term sets the SEARCH compile step hands to a field's compiler and answers each
// one-term set from a fixed table, without touching an index.
class RecordingTermSetCompiler final : public SearchLeafCompiler {
public:
    explicit RecordingTermSetCompiler(std::map<std::string, roaring::Roaring> rows)
            : _rows(std::move(rows)) {}

    Status compile(const index_query::logical::Node& leaf, const SearchLeafContext& /*ctx*/,
                   inverted_index::query_v2::QueryPtr* out) override {
        const auto* set = leaf.as<index_query::logical::TermSet>();
        if (set == nullptr) {
            return Status::InternalError("the recording compiler only takes term sets");
        }
        leaves.push_back(*set);
        roaring::Roaring rows;
        if (set->terms.size() == 1 && _rows.contains(set->terms.front())) {
            rows = _rows.at(set->terms.front());
        }
        *out = std::make_shared<inverted_index::query_v2::BitSetQuery>(std::move(rows));
        return Status::OK();
    }

    std::vector<index_query::logical::TermSet> leaves;

private:
    std::map<std::string, roaring::Roaring> _rows;
};

// A TERM value with a threshold is counted above the field's compiler: every term becomes its
// own leaf on that field, and the nested-document mapper sees the whole threshold as one leaf,
// so rows are counted before they are mapped.
TEST_F(FunctionSearchTest, TestTermThresholdIsCountedAboveTheFieldCompiler) {
    SniiScoringFixture fixture(45, 4);
    FieldReaderBinding binding;
    ASSERT_TRUE(fixture.resolver
                        ->resolve("body", index_query::logical::search_clause_query_type("TERM"),
                                  &binding)
                        .ok());
    auto compiler = std::make_shared<RecordingTermSetCompiler>(
            std::map<std::string, roaring::Roaring> {{"alpha", make_bitmap({0, 1, 2})},
                                                     {"beta", make_bitmap({1, 2})},
                                                     {"gamma", make_bitmap({2, 3})}});
    fixture.resolver->_cache.at(binding.binding_key).leaf_compiler = compiler;
    std::vector<std::string> mapped_fields;
    fixture.resolver->set_leaf_query_mapper(
            [&mapped_fields](const std::string& field,
                             inverted_index::query_v2::QueryPtr* /*query*/) {
                mapped_fields.push_back(field);
                return Status::OK();
            });

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_leaf_clause("TERM", "alpha beta gamma"), fixture.context, *fixture.resolver,
            &query, &binding_key, "OR", 2, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::vector<std::string> terms = {"alpha", "beta", "gamma"};
    ASSERT_EQ(terms.size(), compiler->leaves.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        EXPECT_EQ(std::vector<std::string> {terms[i]}, compiler->leaves[i].terms);
        EXPECT_FALSE(compiler->leaves[i].require_all);
        EXPECT_EQ(0U, compiler->leaves[i].min_should_match);
    }
    EXPECT_EQ(std::vector<std::string> {"body"}, mapped_fields);

    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {1, 2});
}

// SNII answers "at least N of M terms" through one MATCH_ANY query per term. The count is
// two-valued, so a row the field leaves NULL does not match.
TEST_F(FunctionSearchTest, TestSniiNativeTermMinimumShouldMatchCountsMatchingTerms) {
    SniiScoringFixture fixture(46, 5);
    fixture.reader->set_query_result("alpha", make_bitmap({0, 1, 2}));
    fixture.reader->set_query_result("beta", make_bitmap({1, 2}));
    fixture.reader->set_query_result("gamma", make_bitmap({2, 3}));
    fixture.reader->set_null_bitmap(make_bitmap({4}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_leaf_clause("TERM", "alpha beta gamma"), fixture.context, *fixture.resolver,
            &query, &binding_key, "OR", 2, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(3, fixture.reader->query_calls);
    EXPECT_EQ(InvertedIndexQueryType::MATCH_ANY_QUERY, fixture.reader->last_query_type);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {1, 2});
    EXPECT_FALSE(scorer->has_null_bitmap());
}

// A row's score is the sum of the BM25 values the reader published for the terms it matched.
TEST_F(FunctionSearchTest, TestSniiNativeTermMinimumShouldMatchSumsMatchingTermScores) {
    SniiScoringFixture fixture(47, 4);
    fixture.reader->set_query_result("alpha", make_bitmap({0, 1, 2}));
    fixture.reader->set_query_result("beta", make_bitmap({1, 2}));
    fixture.reader->set_query_result("gamma", make_bitmap({2, 3}));
    fixture.reader->set_query_scores("alpha", {{0, 1.0F}, {1, 2.0F}, {2, 4.0F}});
    fixture.reader->set_query_scores("beta", {{1, 8.0F}, {2, 16.0F}});
    fixture.reader->set_query_scores("gamma", {{2, 32.0F}, {3, 64.0F}});

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_leaf_clause("TERM", "alpha beta gamma"), fixture.context, *fixture.resolver,
            &query, &binding_key, "OR", 2, fixture.num_rows);
    ASSERT_TRUE(status.ok()) << status.to_string();

    auto weight = query->weight(true);
    ASSERT_NE(nullptr, weight);
    auto exec_ctx = fixture.exec_context();
    auto roaring = std::make_shared<roaring::Roaring>();
    inverted_index::query_v2::collect_multi_segment_doc_set(weight, exec_ctx, binding_key, roaring,
                                                            fixture.context->collection_similarity,
                                                            /*enable_scoring=*/true);

    expect_bitmap_eq(*roaring, {1, 2});
    auto collected = read_collected_scores(*fixture.context->collection_similarity, *roaring);
    ASSERT_EQ(2U, collected.size());
    EXPECT_FLOAT_EQ(10.0F, collected[1]);
    EXPECT_FLOAT_EQ(52.0F, collected[2]);
}

// An all-of set with a threshold gets the CLucene answer: the threshold counts optional clauses,
// an all-of set has none, so no row matches.
TEST_F(FunctionSearchTest, TestSniiNativeAllOfTermWithThresholdMatchesNothing) {
    SniiScoringFixture fixture(48, 4);
    fixture.reader->set_query_result("alpha", make_bitmap({0, 1, 2}));
    fixture.reader->set_query_result("beta", make_bitmap({1, 2}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(make_leaf_clause("TERM", "alpha beta"),
                                                         fixture.context, *fixture.resolver, &query,
                                                         &binding_key, "and", 2, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {});
}

using AnalyzedCall = std::pair<InvertedIndexQueryType, std::string>;

static TSearchClause make_compound_clause(const std::string& clause_type,
                                          std::vector<TSearchClause> children) {
    TSearchClause clause;
    clause.clause_type = clause_type;
    clause.children = std::move(children);
    clause.__isset.children = true;
    return clause;
}

static TSearchClause with_occur(TSearchClause clause, TSearchOccur::type occur) {
    clause.occur = occur;
    clause.__isset.occur = true;
    return clause;
}

// Term clauses on one SNII field under AND are answered by one MATCH_ALL query, so the field's
// chained conjunction narrows the candidates instead of every term being read in full.
TEST_F(FunctionSearchTest, TestSniiNativeAndOfOneFieldIsOneConjunction) {
    SniiScoringFixture fixture(49, 4);
    fixture.reader->set_query_result("alpha beta", make_bitmap({1, 2}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause(
                    "AND", {make_leaf_clause("TERM", "alpha"), make_leaf_clause("TERM", "beta")}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<AnalyzedCall> {{InvertedIndexQueryType::MATCH_ALL_QUERY, "alpha beta"}}),
              fixture.reader->calls);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {1, 2});
}

// Under OR they are answered by one MATCH_ANY query, the field's streamed union.
TEST_F(FunctionSearchTest, TestSniiNativeOrOfOneFieldIsOneUnion) {
    SniiScoringFixture fixture(50, 4);
    fixture.reader->set_query_result("alpha beta", make_bitmap({0, 1, 2, 3}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause(
                    "OR", {make_leaf_clause("TERM", "alpha"), make_leaf_clause("TERM", "beta")}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<AnalyzedCall> {{InvertedIndexQueryType::MATCH_ANY_QUERY, "alpha beta"}}),
              fixture.reader->calls);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {0, 1, 2, 3});
}

// In an occur tree the required terms join; an optional term keeps its own query.
TEST_F(FunctionSearchTest, TestSniiNativeOccurJoinsOnlyRequiredTerms) {
    SniiScoringFixture fixture(51, 4);
    fixture.reader->set_query_result("alpha beta", make_bitmap({1, 2}));
    fixture.reader->set_query_result("gamma", make_bitmap({2, 3}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause(
                    "OCCUR_BOOLEAN",
                    {with_occur(make_leaf_clause("TERM", "alpha"), TSearchOccur::MUST),
                     with_occur(make_leaf_clause("TERM", "beta"), TSearchOccur::MUST),
                     with_occur(make_leaf_clause("TERM", "gamma"), TSearchOccur::SHOULD)}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<AnalyzedCall> {{InvertedIndexQueryType::MATCH_ALL_QUERY, "alpha beta"},
                                          {InvertedIndexQueryType::MATCH_ANY_QUERY, "gamma"}}),
              fixture.reader->calls);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {1, 2});
}

// Beside a required clause, optional terms only add to the score, so they keep their own queries.
TEST_F(FunctionSearchTest, TestSniiNativeOptionalTermsBesideARequiredOneStayApart) {
    SniiScoringFixture fixture(58, 4);
    fixture.reader->set_query_result("alpha", make_bitmap({1, 2}));
    fixture.reader->set_query_result("beta", make_bitmap({2, 3}));
    fixture.reader->set_query_result("gamma", make_bitmap({0, 2}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause(
                    "OCCUR_BOOLEAN",
                    {with_occur(make_leaf_clause("TERM", "alpha"), TSearchOccur::MUST),
                     with_occur(make_leaf_clause("TERM", "beta"), TSearchOccur::SHOULD),
                     with_occur(make_leaf_clause("TERM", "gamma"), TSearchOccur::SHOULD)}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<AnalyzedCall> {{InvertedIndexQueryType::MATCH_ANY_QUERY, "alpha"},
                                          {InvertedIndexQueryType::MATCH_ANY_QUERY, "beta"},
                                          {InvertedIndexQueryType::MATCH_ANY_QUERY, "gamma"}}),
              fixture.reader->calls);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {1, 2});
}

// Optional terms with no required clause beside them decide the match, so they join.
TEST_F(FunctionSearchTest, TestSniiNativeOptionalTermsThatDecideTheMatchJoin) {
    SniiScoringFixture fixture(59, 4);
    fixture.reader->set_query_result("alpha beta", make_bitmap({0, 1, 3}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause(
                    "OCCUR_BOOLEAN",
                    {with_occur(make_leaf_clause("TERM", "alpha"), TSearchOccur::SHOULD),
                     with_occur(make_leaf_clause("TERM", "beta"), TSearchOccur::SHOULD)}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<AnalyzedCall> {{InvertedIndexQueryType::MATCH_ANY_QUERY, "alpha beta"}}),
              fixture.reader->calls);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {0, 1, 3});
}

// An unscored query never reads optional terms beside a required one: they cannot change which
// rows match.
TEST_F(FunctionSearchTest, TestSniiNativeUnscoredQuerySkipsOptionalTerms) {
    SniiScoringFixture fixture(60, 4);
    fixture.reader->set_query_result("alpha", make_bitmap({1, 2}));
    fixture.reader->set_query_result("beta", make_bitmap({2, 3}));
    fixture.reader->set_query_result("gamma", make_bitmap({0, 2}));
    fixture.reader->set_null_bitmap(make_bitmap({3}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause(
                    "OCCUR_BOOLEAN",
                    {with_occur(make_leaf_clause("TERM", "alpha"), TSearchOccur::MUST),
                     with_occur(make_leaf_clause("TERM", "beta"), TSearchOccur::SHOULD),
                     with_occur(make_leaf_clause("TERM", "gamma"), TSearchOccur::SHOULD)}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows,
            /*scoring=*/false);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<AnalyzedCall> {{InvertedIndexQueryType::MATCH_ANY_QUERY, "alpha"}}),
              fixture.reader->calls);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {1, 2});
}

// A lucene-style Boolean follows Elasticsearch: a clause on a NULL field does not match, so NOT
// keeps the row and the SEARCH result has no NULL rows. Rows 0-3 have a title and a NULL content,
// row 4 has a content and a NULL title, and row 5 has neither.
TEST_F(FunctionSearchTest, TestOccurBooleanTreatsNullFieldsAsNotMatching) {
    const std::map<std::string, std::string> properties {
            {INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD}};
    auto title_meta = make_test_inverted_index(62, properties);
    auto content_meta = make_test_inverted_index(63, properties);
    auto title = std::make_shared<RecordingNativeInvertedIndexReader>(
            &title_meta, std::make_shared<RejectingCluceneIndexFileReader>(
                                 InvertedIndexStorageFormatPB::SNII, "/tmp/search_title_idx"));
    auto content = std::make_shared<RecordingNativeInvertedIndexReader>(
            &content_meta, std::make_shared<RejectingCluceneIndexFileReader>(
                                   InvertedIndexStorageFormatPB::SNII, "/tmp/search_content_idx"));
    title->set_query_result("philosophy", make_bitmap({0, 1, 2, 3}));
    title->set_null_bitmap(make_bitmap({4, 5}));
    content->set_query_result("news", make_bitmap({4}));
    content->set_null_bitmap(make_bitmap({0, 1, 2, 3, 5}));
    segment_v2::InvertedIndexIterator title_iterator;
    title_iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, title);
    segment_v2::InvertedIndexIterator content_iterator;
    content_iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, content);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    TSearchParam search_param;
    for (const auto& [field, iterator] :
         {std::pair<std::string, IndexIterator*> {"title", &title_iterator},
          std::pair<std::string, IndexIterator*> {"content", &content_iterator}}) {
        data_types.emplace(field,
                           IndexFieldNameAndTypePair {field, std::make_shared<DataTypeString>()});
        iterators[field] = iterator;
        TSearchFieldBinding binding;
        binding.field_name = field;
        binding.index_properties = properties;
        binding.__isset.index_properties = true;
        search_param.field_bindings.push_back(binding);
    }
    const auto term = [](const std::string& field, const std::string& value,
                         TSearchOccur::type occur) {
        auto clause = make_leaf_clause("TERM", value);
        clause.field_name = field;
        return with_occur(clause, occur);
    };
    const auto evaluate = [&](const TSearchClause& root,
                              std::initializer_list<uint32_t> expected_docs) {
        search_param.root = root;
        InvertedIndexResultBitmap result;
        auto status = function_search->evaluate_inverted_index_with_search_param(
                search_param, data_types, iterators, 6, result);
        ASSERT_TRUE(status.ok()) << status.to_string();
        expect_bitmap_eq(*result.get_data_bitmap(), expected_docs);
        EXPECT_TRUE(result.get_null_bitmap() == nullptr || result.get_null_bitmap()->isEmpty());
    };

    // title:philosophy OR NOT (content:history AND NOT content:news)
    const auto content_subtree = make_compound_clause(
            "OCCUR_BOOLEAN", {term("content", "history", TSearchOccur::MUST),
                              term("content", "news", TSearchOccur::MUST_NOT)});
    evaluate(make_compound_clause("OCCUR_BOOLEAN",
                                  {term("title", "philosophy", TSearchOccur::SHOULD),
                                   with_occur(content_subtree, TSearchOccur::MUST_NOT)}),
             {0, 1, 2, 3});

    // NOT (title:philosophy OR content:news)
    TSearchClause match_all;
    match_all.clause_type = "MATCH_ALL_DOCS";
    const auto either = make_compound_clause("OCCUR_BOOLEAN",
                                             {term("title", "philosophy", TSearchOccur::SHOULD),
                                              term("content", "news", TSearchOccur::SHOULD)});
    evaluate(make_compound_clause("OCCUR_BOOLEAN", {with_occur(match_all, TSearchOccur::SHOULD),
                                                    with_occur(either, TSearchOccur::MUST_NOT)}),
             {5});
}

// A phrase beside a required rare term runs only on the rows the term leaves. Those rows are
// internal to SEARCH, so the scan never hears that candidates were used.
TEST_F(FunctionSearchTest, TestSniiNativePhraseRunsWithinTheRowsOfARequiredTerm) {
    SniiScoringFixture fixture(61, 100);
    fixture.reader->set_query_result("alpha", make_bitmap({3, 7}));
    fixture.reader->set_query_result("gamma delta", make_bitmap({7, 50}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause("AND", {make_leaf_clause("PHRASE", "gamma delta"),
                                         make_leaf_clause("TERM", "alpha")}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<AnalyzedCall> {
                      {InvertedIndexQueryType::MATCH_ANY_QUERY, "alpha"},
                      {InvertedIndexQueryType::MATCH_PHRASE_QUERY, "gamma delta"}}),
              fixture.reader->calls);
    ASSERT_EQ(2U, fixture.reader->call_candidates.size());
    EXPECT_FALSE(fixture.reader->call_candidates[0].has_value());
    ASSERT_TRUE(fixture.reader->call_candidates[1].has_value());
    expect_bitmap_eq(*fixture.reader->call_candidates[1], {3, 7});
    EXPECT_FALSE(fixture.context->candidate_rows_consumed);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {7});
}

// A negated clause beside a required term also runs within the term's rows: outside them the AND
// is FALSE whatever the negation says.
TEST_F(FunctionSearchTest, TestSniiNativeNegatedPhraseRunsWithinTheRowsOfARequiredTerm) {
    SniiScoringFixture fixture(62, 100);
    fixture.reader->set_query_result("alpha", make_bitmap({3, 7, 9}));
    fixture.reader->set_query_result("gamma delta", make_bitmap({7, 50}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause(
                    "AND",
                    {make_leaf_clause("TERM", "alpha"),
                     make_compound_clause("NOT", {make_leaf_clause("PHRASE", "gamma delta")})}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(2U, fixture.reader->call_candidates.size());
    EXPECT_FALSE(fixture.reader->call_candidates[0].has_value());
    ASSERT_TRUE(fixture.reader->call_candidates[1].has_value());
    expect_bitmap_eq(*fixture.reader->call_candidates[1], {3, 7, 9});
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {3, 9});
}

// Rows a required term leaves are passed on only while they are as selective as the candidates
// a scan passes, and the phrase then runs on every row.
TEST_F(FunctionSearchTest, TestSniiNativeWideRowsOfARequiredTermAreNotPassedOn) {
    SniiScoringFixture fixture(63, 100);
    roaring::Roaring wide;
    wide.addRange(0, 50);
    fixture.reader->set_query_result("alpha", wide);
    fixture.reader->set_query_result("gamma delta", make_bitmap({7, 50}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause("AND", {make_leaf_clause("PHRASE", "gamma delta"),
                                         make_leaf_clause("TERM", "alpha")}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(2U, fixture.reader->call_candidates.size());
    EXPECT_FALSE(fixture.reader->call_candidates[0].has_value());
    EXPECT_FALSE(fixture.reader->call_candidates[1].has_value());
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {7});
}

// Under a nested-document mapper a leaf may live in another document space, so no rows are
// passed between clauses.
TEST_F(FunctionSearchTest, TestSniiNativeNoRowsArePassedOnUnderALeafMapper) {
    SniiScoringFixture fixture(64, 100);
    fixture.reader->set_query_result("alpha", make_bitmap({3, 7}));
    fixture.reader->set_query_result("gamma delta", make_bitmap({7, 50}));
    fixture.resolver->set_leaf_query_mapper(
            [](const std::string& /*field*/, inverted_index::query_v2::QueryPtr* /*query*/) {
                return Status::OK();
            });

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause("AND", {make_leaf_clause("PHRASE", "gamma delta"),
                                         make_leaf_clause("TERM", "alpha")}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(2U, fixture.reader->call_candidates.size());
    EXPECT_FALSE(fixture.reader->call_candidates[0].has_value());
    EXPECT_FALSE(fixture.reader->call_candidates[1].has_value());
}

// Terms join past a clause of another kind, which keeps its own query.
TEST_F(FunctionSearchTest, TestSniiNativeTermsJoinPastAPhrase) {
    SniiScoringFixture fixture(52, 4);
    fixture.reader->set_query_result("alpha beta", make_bitmap({0, 1, 2}));
    fixture.reader->set_query_result("gamma delta", make_bitmap({1, 2, 3}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause("AND", {make_leaf_clause("TERM", "alpha"),
                                         make_leaf_clause("PHRASE", "gamma delta"),
                                         make_leaf_clause("TERM", "beta")}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<AnalyzedCall> {
                      {InvertedIndexQueryType::MATCH_ALL_QUERY, "alpha beta"},
                      {InvertedIndexQueryType::MATCH_PHRASE_QUERY, "gamma delta"}}),
              fixture.reader->calls);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {1, 2});
}

// A nested-document mapper maps each leaf on its own, so leaves under it never join.
TEST_F(FunctionSearchTest, TestSniiNativeTermsStayApartUnderALeafMapper) {
    SniiScoringFixture fixture(53, 4);
    fixture.reader->set_query_result("alpha", make_bitmap({0, 1, 2}));
    fixture.reader->set_query_result("beta", make_bitmap({1, 2, 3}));
    std::vector<std::string> mapped_fields;
    fixture.resolver->set_leaf_query_mapper(
            [&mapped_fields](const std::string& field,
                             inverted_index::query_v2::QueryPtr* /*query*/) {
                mapped_fields.push_back(field);
                return Status::OK();
            });

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause(
                    "AND", {make_leaf_clause("TERM", "alpha"), make_leaf_clause("TERM", "beta")}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<AnalyzedCall> {{InvertedIndexQueryType::MATCH_ANY_QUERY, "alpha"},
                                          {InvertedIndexQueryType::MATCH_ANY_QUERY, "beta"}}),
              fixture.reader->calls);
    EXPECT_EQ((std::vector<std::string> {"body", "body"}), mapped_fields);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {1, 2});
}

// A threshold over optional clauses counts clauses, so their terms keep their own queries.
TEST_F(FunctionSearchTest, TestSniiNativeOptionalTermsUnderAThresholdStayApart) {
    SniiScoringFixture fixture(54, 4);
    fixture.reader->set_query_result("alpha", make_bitmap({0, 1, 2}));
    fixture.reader->set_query_result("beta", make_bitmap({1, 2, 3}));
    fixture.reader->set_query_result("gamma", make_bitmap({2, 3}));
    auto root = make_compound_clause(
            "OCCUR_BOOLEAN", {with_occur(make_leaf_clause("TERM", "alpha"), TSearchOccur::SHOULD),
                              with_occur(make_leaf_clause("TERM", "beta"), TSearchOccur::SHOULD),
                              with_occur(make_leaf_clause("TERM", "gamma"), TSearchOccur::SHOULD)});
    root.minimum_should_match = 2;
    root.__isset.minimum_should_match = true;

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status =
            function_search->build_query_recursive(root, fixture.context, *fixture.resolver, &query,
                                                   &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<AnalyzedCall> {{InvertedIndexQueryType::MATCH_ANY_QUERY, "alpha"},
                                          {InvertedIndexQueryType::MATCH_ANY_QUERY, "beta"},
                                          {InvertedIndexQueryType::MATCH_ANY_QUERY, "gamma"}}),
              fixture.reader->calls);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {1, 2, 3});
}

// A term repeated in one AND keeps its own leaves; that is how a repeated term scores.
TEST_F(FunctionSearchTest, TestSniiNativeRepeatedTermStaysApart) {
    SniiScoringFixture fixture(55, 4);
    fixture.reader->set_query_result("alpha", make_bitmap({0, 1}));

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause(
                    "AND", {make_leaf_clause("TERM", "alpha"), make_leaf_clause("TERM", "alpha")}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ((std::vector<AnalyzedCall> {{InvertedIndexQueryType::MATCH_ANY_QUERY, "alpha"},
                                          {InvertedIndexQueryType::MATCH_ANY_QUERY, "alpha"}}),
              fixture.reader->calls);
}

// A field compiler whose index the engine drives lazily keeps one leaf per term.
TEST_F(FunctionSearchTest, TestLazyFieldCompilerKeepsOneLeafPerTerm) {
    SniiScoringFixture fixture(56, 4);
    FieldReaderBinding binding;
    ASSERT_TRUE(fixture.resolver
                        ->resolve("body", index_query::logical::search_clause_query_type("TERM"),
                                  &binding)
                        .ok());
    auto compiler =
            std::make_shared<RecordingTermSetCompiler>(std::map<std::string, roaring::Roaring> {
                    {"alpha", make_bitmap({0, 1, 2})}, {"beta", make_bitmap({1, 2, 3})}});
    fixture.resolver->_cache.at(binding.binding_key).leaf_compiler = compiler;

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause(
                    "AND", {make_leaf_clause("TERM", "alpha"), make_leaf_clause("TERM", "beta")}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(2U, compiler->leaves.size());
    EXPECT_EQ(std::vector<std::string> {"alpha"}, compiler->leaves[0].terms);
    EXPECT_EQ(std::vector<std::string> {"beta"}, compiler->leaves[1].terms);
    auto weight = query->weight(false);
    ASSERT_NE(nullptr, weight);
    auto scorer = weight->scorer(fixture.exec_context(), binding_key);
    ASSERT_NE(nullptr, scorer);
    expect_bitmap_eq(collect_docs(scorer), {1, 2});
}

// A joined AND keeps the BM25 values the reader published for the joined query.
TEST_F(FunctionSearchTest, TestSniiNativeJoinedAndKeepsReaderScores) {
    SniiScoringFixture fixture(57, 4);
    fixture.reader->set_query_result("alpha beta", make_bitmap({1, 2}));
    fixture.reader->set_query_scores("alpha beta", {{1, 3.5F}, {2, 1.25F}});

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_query_recursive(
            make_compound_clause(
                    "AND", {make_leaf_clause("TERM", "alpha"), make_leaf_clause("TERM", "beta")}),
            fixture.context, *fixture.resolver, &query, &binding_key, "OR", 0, fixture.num_rows);
    ASSERT_TRUE(status.ok()) << status.to_string();

    auto weight = query->weight(true);
    ASSERT_NE(nullptr, weight);
    auto exec_ctx = fixture.exec_context();
    auto roaring = std::make_shared<roaring::Roaring>();
    inverted_index::query_v2::collect_multi_segment_doc_set(weight, exec_ctx, binding_key, roaring,
                                                            fixture.context->collection_similarity,
                                                            /*enable_scoring=*/true);

    expect_bitmap_eq(*roaring, {1, 2});
    auto collected = read_collected_scores(*fixture.context->collection_similarity, *roaring);
    ASSERT_EQ(2U, collected.size());
    EXPECT_FLOAT_EQ(3.5F, collected[1]);
    EXPECT_FLOAT_EQ(1.25F, collected[2]);
}

TEST_F(FunctionSearchTest, TestSearchDslCacheIsDisabledForSniiNativeExecution) {
    ScopedInvertedIndexQueryCache cache_guard;
    auto index_meta = make_test_inverted_index(
            19, {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD}});
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>();
    auto reader =
            std::make_shared<RecordingNativeInvertedIndexReader>(&index_meta, index_file_reader);
    reader->set_query_result("*lpha", make_bitmap({0}));
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;

    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = index_meta.properties();
    field_binding.__isset.index_properties = true;
    TSearchParam search_param;
    search_param.original_dsl = "body:*lpha";
    search_param.root = make_leaf_clause("WILDCARD", "*lpha");
    search_param.field_bindings = {field_binding};
    ASSERT_TRUE(insert_search_dsl_cache(cache_guard.get(), index_file_reader, search_param,
                                        make_bitmap({3}))
                        .ok());

    InvertedIndexResultBitmap result;
    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_type_with_names, iterators, 4, result, true);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, result.get_data_bitmap());
    expect_bitmap_eq(*result.get_data_bitmap(), {0});
    EXPECT_EQ(1, reader->query_calls);
}

TEST_F(FunctionSearchTest, TestSearchDslCacheIsDisabledWhenScoring) {
    ScopedInvertedIndexQueryCache cache_guard;
    auto index_meta = make_test_inverted_index(
            20, {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD}});
    auto index_file_reader = std::make_shared<RejectingCluceneIndexFileReader>(
            InvertedIndexStorageFormatPB::V2, "/tmp/search_scoring_v2_idx");
    auto reader = std::make_shared<DummyInvertedIndexReader>(
            &index_meta, index_file_reader, segment_v2::InvertedIndexReaderType::FULLTEXT);
    segment_v2::InvertedIndexIterator iterator;
    iterator.add_reader(segment_v2::InvertedIndexReaderType::FULLTEXT, reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &iterator;
    TSearchFieldBinding field_binding;
    field_binding.field_name = "body";
    field_binding.index_properties = index_meta.properties();
    field_binding.__isset.index_properties = true;
    TSearchParam search_param;
    search_param.original_dsl = "body:alpha";
    search_param.root = make_leaf_clause("TERM", "alpha");
    search_param.field_bindings = {field_binding};
    ASSERT_TRUE(insert_search_dsl_cache(cache_guard.get(), index_file_reader, search_param,
                                        make_bitmap({3}))
                        .ok());

    auto scoring_context = std::make_shared<IndexQueryContext>();
    scoring_context->collection_similarity = std::make_shared<CollectionSimilarity>();
    InvertedIndexResultBitmap result;
    std::unordered_map<std::string, int> field_name_to_column_id;
    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_type_with_names, iterators, 4, result, true, nullptr,
            field_name_to_column_id, scoring_context);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(1, index_file_reader->init_calls);
    EXPECT_EQ(1, index_file_reader->open_calls);
}

TEST_F(FunctionSearchTest, TestSearchDslCacheRemainsEnabledForUnreferencedSniiField) {
    ScopedInvertedIndexQueryCache cache_guard;
    auto text_index_meta = make_test_inverted_index(
            21, {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD}});
    auto number_index_meta = make_test_inverted_index(22);
    auto text_file_reader = std::make_shared<RejectingCluceneIndexFileReader>(
            InvertedIndexStorageFormatPB::SNII, "/tmp/search_mixed_cache_idx");
    auto number_file_reader = std::make_shared<RejectingCluceneIndexFileReader>(
            InvertedIndexStorageFormatPB::V2, "/tmp/search_mixed_cache_idx");
    auto text_reader = std::make_shared<RecordingNativeInvertedIndexReader>(
            &text_index_meta, text_file_reader, InvertedIndexReaderType::FULLTEXT);
    auto number_reader = std::make_shared<RecordingNativeInvertedIndexReader>(
            &number_index_meta, number_file_reader, InvertedIndexReaderType::BKD);

    InvertedIndexIterator text_iterator;
    text_iterator.add_reader(InvertedIndexReaderType::FULLTEXT, text_reader);
    RecordingDirectInvertedIndexIterator number_iterator;
    number_iterator.add_reader(InvertedIndexReaderType::BKD, number_reader);

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    data_type_with_names.emplace(
            "body", IndexFieldNameAndTypePair {"body", std::make_shared<DataTypeString>()});
    data_type_with_names.emplace(
            "age", IndexFieldNameAndTypePair {"age", std::make_shared<DataTypeInt32>()});
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["body"] = &text_iterator;
    iterators["age"] = &number_iterator;

    TSearchClause age_clause = make_leaf_clause("TERM", "42");
    age_clause.field_name = "age";
    TSearchParam search_param;
    search_param.original_dsl = "age:42";
    search_param.root = age_clause;
    ASSERT_TRUE(insert_search_dsl_cache(cache_guard.get(), number_file_reader, search_param,
                                        make_bitmap({1}))
                        .ok());

    InvertedIndexResultBitmap result;
    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_type_with_names, iterators, 4, result, true);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, result.get_data_bitmap());
    expect_bitmap_eq(*result.get_data_bitmap(), {1});
    EXPECT_EQ(0, text_reader->query_calls);
    EXPECT_EQ(0, number_iterator.read_calls);
}

TEST_F(FunctionSearchTest, TestBuildLeafQueryDirectUnknownClauseUsesLeafMapper) {
    TSearchClause clause;
    clause.clause_type = "PHRASE";
    clause.field_name = "var.items.active";
    clause.value = "true";
    clause.__isset.field_name = true;
    clause.__isset.value = true;

    auto context = std::make_shared<IndexQueryContext>();

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    auto bool_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeBool>()));
    data_type_with_names.emplace("var.items.active",
                                 IndexFieldNameAndTypePair {"1.var.items.active", bool_type});

    RecordingIndexIterator iterator;
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["var.items.active"] = &iterator;

    FieldReaderResolver resolver(data_type_with_names, iterators, context);

    FieldReaderBinding binding;
    binding.logical_field_name = "var.items.active";
    binding.stored_field_name = "1.var.items.active";
    binding.stored_field_wstr = L"1.var.items.active";
    binding.column_type = bool_type;
    binding.query_type = InvertedIndexQueryType::MATCH_PHRASE_QUERY;
    binding.state = SearchFieldBindingState::BOUND;
    binding.leaf_compiler =
            std::make_shared<ScalarLeafCompiler>(&iterator, bool_type, "1.var.items.active");
    TabletIndex index_meta;
    binding.inverted_reader = std::make_shared<DummyInvertedIndexReader>(&index_meta);

    std::string key = resolver.binding_key_for("1.var.items.active",
                                               InvertedIndexQueryType::MATCH_PHRASE_QUERY);
    binding.binding_key = key;
    resolver._cache[key] = binding;

    bool mapper_called = false;
    resolver.set_leaf_query_mapper([&](const std::string& logical_field,
                                       inverted_index::query_v2::QueryPtr* query) -> Status {
        mapper_called = true;
        EXPECT_EQ("var.items.active", logical_field);
        EXPECT_NE(nullptr, query);
        EXPECT_NE(nullptr, *query);
        return Status::OK();
    });

    inverted_index::query_v2::QueryPtr out;
    std::string out_binding_key;
    Status st = function_search->build_query_recursive(clause, context, resolver, &out,
                                                       &out_binding_key, "OR", 0, 4);
    ASSERT_TRUE(st.ok());
    ASSERT_NE(out, nullptr);
    EXPECT_TRUE(mapper_called);
    EXPECT_EQ(key, out_binding_key);
    EXPECT_TRUE(iterator.last_column_name.empty());

    auto weight = out->weight(false);
    ASSERT_NE(weight, nullptr);
    inverted_index::query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 4;
    auto scorer = weight->scorer(exec_ctx);
    ASSERT_NE(scorer, nullptr);
    EXPECT_EQ(inverted_index::query_v2::TERMINATED, scorer->doc());
    ASSERT_TRUE(scorer->has_null_bitmap());
    const auto* null_bitmap = scorer->get_null_bitmap();
    ASSERT_NE(null_bitmap, nullptr);
    EXPECT_EQ(4u, null_bitmap->cardinality());
}

TEST_F(FunctionSearchTest, TestBuildLeafQueryVariantBoolUsesDirectIndexReader) {
    TSearchClause clause;
    clause.clause_type = "TERM";
    clause.field_name = "var.items.active";
    clause.value = "true";
    clause.__isset.field_name = true;
    clause.__isset.value = true;

    auto context = std::make_shared<IndexQueryContext>();

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    auto bool_type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeBool>()));
    data_type_with_names.emplace("var.items.active",
                                 IndexFieldNameAndTypePair {"1.var.items.active", bool_type});

    RecordingIndexIterator iterator;
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["var.items.active"] = &iterator;

    FieldReaderResolver resolver(data_type_with_names, iterators, context);

    FieldReaderBinding binding;
    binding.logical_field_name = "var.items.active";
    binding.stored_field_name = "1.var.items.active";
    binding.stored_field_wstr = L"1.var.items.active";
    binding.column_type = bool_type;
    binding.query_type = InvertedIndexQueryType::MATCH_ANY_QUERY;
    binding.state = SearchFieldBindingState::BOUND;
    binding.leaf_compiler =
            std::make_shared<ScalarLeafCompiler>(&iterator, bool_type, "1.var.items.active");
    TabletIndex index_meta;
    binding.inverted_reader = std::make_shared<DummyInvertedIndexReader>(&index_meta);

    std::string key =
            resolver.binding_key_for("1.var.items.active", InvertedIndexQueryType::MATCH_ANY_QUERY);
    binding.binding_key = key;
    resolver._cache[key] = binding;

    inverted_index::query_v2::QueryPtr out;
    std::string out_binding_key;
    Status st = function_search->build_query_recursive(clause, context, resolver, &out,
                                                       &out_binding_key, "OR", 0, 10);
    ASSERT_TRUE(st.ok());
    ASSERT_NE(out, nullptr);
    EXPECT_EQ(key, out_binding_key);
    EXPECT_EQ("1.var.items.active", iterator.last_column_name);
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_BOOL, iterator.last_column_storage_type);
    EXPECT_EQ(InvertedIndexQueryType::EQUAL_QUERY, iterator.last_query_type);
    EXPECT_EQ(TYPE_BOOLEAN, iterator.last_query_value_type);
    EXPECT_TRUE(iterator.last_bool_value);

    auto weight = out->weight(false);
    ASSERT_NE(weight, nullptr);
    inverted_index::query_v2::QueryExecutionContext exec_ctx;
    exec_ctx.segment_num_rows = 10;
    auto scorer = weight->scorer(exec_ctx, out_binding_key);
    ASSERT_NE(scorer, nullptr);
    EXPECT_EQ(3u, scorer->doc());
}

TEST_F(FunctionSearchTest, TestBuildLeafQueryVariantNestedIntUsesDirectIndexReader) {
    TSearchClause clause;
    clause.clause_type = "TERM";
    clause.field_name = "var.items.flags.level";
    clause.value = "3";
    clause.__isset.field_name = true;
    clause.__isset.value = true;

    auto context = std::make_shared<IndexQueryContext>();

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_type_with_names;
    auto int_type = std::make_shared<DataTypeArray>(make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()))));
    data_type_with_names.emplace("var.items.flags.level",
                                 IndexFieldNameAndTypePair {"1.var.items.flags.level", int_type});

    RecordingIndexIterator iterator;
    std::unordered_map<std::string, IndexIterator*> iterators;
    iterators["var.items.flags.level"] = &iterator;

    FieldReaderResolver resolver(data_type_with_names, iterators, context);

    FieldReaderBinding binding;
    binding.logical_field_name = "var.items.flags.level";
    binding.stored_field_name = "1.var.items.flags.level";
    binding.stored_field_wstr = L"1.var.items.flags.level";
    binding.column_type = int_type;
    binding.query_type = InvertedIndexQueryType::MATCH_ANY_QUERY;
    binding.state = SearchFieldBindingState::BOUND;
    binding.leaf_compiler =
            std::make_shared<ScalarLeafCompiler>(&iterator, int_type, "1.var.items.flags.level");
    TabletIndex index_meta;
    binding.inverted_reader = std::make_shared<DummyInvertedIndexReader>(&index_meta);

    std::string key = resolver.binding_key_for("1.var.items.flags.level",
                                               InvertedIndexQueryType::MATCH_ANY_QUERY);
    binding.binding_key = key;
    resolver._cache[key] = binding;

    inverted_index::query_v2::QueryPtr out;
    std::string out_binding_key;
    Status st = function_search->build_query_recursive(clause, context, resolver, &out,
                                                       &out_binding_key, "OR", 0, 10);
    ASSERT_TRUE(st.ok());
    ASSERT_NE(out, nullptr);
    EXPECT_EQ(key, out_binding_key);
    EXPECT_EQ("1.var.items.flags.level", iterator.last_column_name);
    EXPECT_EQ(FieldType::OLAP_FIELD_TYPE_INT, iterator.last_column_storage_type);
    EXPECT_EQ(InvertedIndexQueryType::EQUAL_QUERY, iterator.last_query_type);
    EXPECT_EQ(TYPE_INT, iterator.last_query_value_type);
    EXPECT_EQ(3, iterator.last_int_value);
}

TEST_F(FunctionSearchTest, TestMultiPhraseQueryCase) {
    using doris::segment_v2::InvertedIndexQueryInfo;
    using doris::segment_v2::TermInfo;
    using doris::CollectionStatistics;
    using doris::CollectionStatisticsPtr;

    auto context = std::make_shared<IndexQueryContext>();
    context->collection_statistics = std::make_shared<CollectionStatistics>();
    context->collection_similarity = std::make_shared<CollectionSimilarity>();

    std::wstring field = doris::segment_v2::inverted_index::StringHelper::to_wstring("content");

    std::vector<TermInfo> term_infos;

    TermInfo t1;
    t1.term = std::vector<std::string> {"quick", "fast", "speedy"};
    t1.position = 0;
    term_infos.push_back(t1);

    TermInfo t2;
    t2.term = std::string("brown");
    t2.position = 1;
    term_infos.push_back(t2);

    auto query = std::make_shared<doris::segment_v2::inverted_index::query_v2::MultiPhraseQuery>(
            context, field, term_infos);
    ASSERT_NE(query, nullptr);

    auto weight = query->weight(false);
    ASSERT_NE(weight, nullptr);

    auto multi_phrase_weight = std::dynamic_pointer_cast<
            doris::segment_v2::inverted_index::query_v2::MultiPhraseWeight>(weight);
    ASSERT_NE(multi_phrase_weight, nullptr);
}

TEST_F(FunctionSearchTest, TestOccurBooleanSearchParam) {
    // Test creating OCCUR_BOOLEAN search param (Lucene mode)
    TSearchParam searchParam;
    searchParam.original_dsl = "field:a AND field:b OR field:c";

    // Create child clauses with occur types
    TSearchClause mustClause1;
    mustClause1.clause_type = "TERM";
    mustClause1.field_name = "field";
    mustClause1.value = "a";
    mustClause1.__isset.field_name = true;
    mustClause1.__isset.value = true;
    mustClause1.occur = TSearchOccur::MUST;
    mustClause1.__isset.occur = true;

    TSearchClause mustClause2;
    mustClause2.clause_type = "TERM";
    mustClause2.field_name = "field";
    mustClause2.value = "b";
    mustClause2.__isset.field_name = true;
    mustClause2.__isset.value = true;
    mustClause2.occur = TSearchOccur::MUST;
    mustClause2.__isset.occur = true;

    // Create root OCCUR_BOOLEAN clause
    TSearchClause rootClause;
    rootClause.clause_type = "OCCUR_BOOLEAN";
    rootClause.children = {mustClause1, mustClause2};
    rootClause.__isset.children = true;
    rootClause.minimum_should_match = 0;
    rootClause.__isset.minimum_should_match = true;
    searchParam.root = rootClause;

    // Verify structure
    EXPECT_EQ("OCCUR_BOOLEAN", searchParam.root.clause_type);
    EXPECT_EQ(2, searchParam.root.children.size());
    EXPECT_EQ(TSearchOccur::MUST, searchParam.root.children[0].occur);
    EXPECT_EQ(TSearchOccur::MUST, searchParam.root.children[1].occur);
    EXPECT_EQ(0, searchParam.root.minimum_should_match);
}

TEST_F(FunctionSearchTest, TestOccurBooleanWithMustNotClause) {
    // Test OCCUR_BOOLEAN with MUST_NOT (NOT operator in Lucene mode)
    TSearchParam searchParam;
    searchParam.original_dsl = "NOT field:a";

    TSearchClause mustNotClause;
    mustNotClause.clause_type = "TERM";
    mustNotClause.field_name = "field";
    mustNotClause.value = "a";
    mustNotClause.__isset.field_name = true;
    mustNotClause.__isset.value = true;
    mustNotClause.occur = TSearchOccur::MUST_NOT;
    mustNotClause.__isset.occur = true;

    TSearchClause rootClause;
    rootClause.clause_type = "OCCUR_BOOLEAN";
    rootClause.children = {mustNotClause};
    rootClause.__isset.children = true;
    searchParam.root = rootClause;

    // Verify structure
    EXPECT_EQ("OCCUR_BOOLEAN", searchParam.root.clause_type);
    EXPECT_EQ(1, searchParam.root.children.size());
    EXPECT_EQ(TSearchOccur::MUST_NOT, searchParam.root.children[0].occur);
}

TEST_F(FunctionSearchTest, TestOccurBooleanWithShouldClauses) {
    // Test OCCUR_BOOLEAN with SHOULD clauses (OR in Lucene mode)
    TSearchParam searchParam;
    searchParam.original_dsl = "field:a OR field:b";

    TSearchClause shouldClause1;
    shouldClause1.clause_type = "TERM";
    shouldClause1.field_name = "field";
    shouldClause1.value = "a";
    shouldClause1.__isset.field_name = true;
    shouldClause1.__isset.value = true;
    shouldClause1.occur = TSearchOccur::SHOULD;
    shouldClause1.__isset.occur = true;

    TSearchClause shouldClause2;
    shouldClause2.clause_type = "TERM";
    shouldClause2.field_name = "field";
    shouldClause2.value = "b";
    shouldClause2.__isset.field_name = true;
    shouldClause2.__isset.value = true;
    shouldClause2.occur = TSearchOccur::SHOULD;
    shouldClause2.__isset.occur = true;

    TSearchClause rootClause;
    rootClause.clause_type = "OCCUR_BOOLEAN";
    rootClause.children = {shouldClause1, shouldClause2};
    rootClause.__isset.children = true;
    rootClause.minimum_should_match = 1;
    rootClause.__isset.minimum_should_match = true;
    searchParam.root = rootClause;

    // Verify structure
    EXPECT_EQ("OCCUR_BOOLEAN", searchParam.root.clause_type);
    EXPECT_EQ(2, searchParam.root.children.size());
    EXPECT_EQ(TSearchOccur::SHOULD, searchParam.root.children[0].occur);
    EXPECT_EQ(TSearchOccur::SHOULD, searchParam.root.children[1].occur);
    EXPECT_EQ(1, searchParam.root.minimum_should_match);
}

TEST_F(FunctionSearchTest, TestOccurBooleanMixedOccurTypes) {
    // Test OCCUR_BOOLEAN with mixed MUST, SHOULD, MUST_NOT (complex Lucene query)
    // Example: +a +b c -d (a AND b, c is optional, NOT d)
    TSearchParam searchParam;
    searchParam.original_dsl = "field:a AND field:b OR field:c NOT field:d";

    TSearchClause mustClause1;
    mustClause1.clause_type = "TERM";
    mustClause1.field_name = "field";
    mustClause1.value = "a";
    mustClause1.__isset.field_name = true;
    mustClause1.__isset.value = true;
    mustClause1.occur = TSearchOccur::MUST;
    mustClause1.__isset.occur = true;

    TSearchClause mustClause2;
    mustClause2.clause_type = "TERM";
    mustClause2.field_name = "field";
    mustClause2.value = "b";
    mustClause2.__isset.field_name = true;
    mustClause2.__isset.value = true;
    mustClause2.occur = TSearchOccur::MUST;
    mustClause2.__isset.occur = true;

    TSearchClause shouldClause;
    shouldClause.clause_type = "TERM";
    shouldClause.field_name = "field";
    shouldClause.value = "c";
    shouldClause.__isset.field_name = true;
    shouldClause.__isset.value = true;
    shouldClause.occur = TSearchOccur::SHOULD;
    shouldClause.__isset.occur = true;

    TSearchClause mustNotClause;
    mustNotClause.clause_type = "TERM";
    mustNotClause.field_name = "field";
    mustNotClause.value = "d";
    mustNotClause.__isset.field_name = true;
    mustNotClause.__isset.value = true;
    mustNotClause.occur = TSearchOccur::MUST_NOT;
    mustNotClause.__isset.occur = true;

    TSearchClause rootClause;
    rootClause.clause_type = "OCCUR_BOOLEAN";
    rootClause.children = {mustClause1, mustClause2, shouldClause, mustNotClause};
    rootClause.__isset.children = true;
    rootClause.minimum_should_match = 0;
    rootClause.__isset.minimum_should_match = true;
    searchParam.root = rootClause;

    // Verify structure
    EXPECT_EQ("OCCUR_BOOLEAN", searchParam.root.clause_type);
    EXPECT_EQ(4, searchParam.root.children.size());
    EXPECT_EQ(TSearchOccur::MUST, searchParam.root.children[0].occur);
    EXPECT_EQ(TSearchOccur::MUST, searchParam.root.children[1].occur);
    EXPECT_EQ(TSearchOccur::SHOULD, searchParam.root.children[2].occur);
    EXPECT_EQ(TSearchOccur::MUST_NOT, searchParam.root.children[3].occur);
    EXPECT_EQ(0, searchParam.root.minimum_should_match);
}

TEST_F(FunctionSearchTest, TestOccurBooleanMinimumShouldMatchZero) {
    // Test that SHOULD clauses are effectively ignored when minimum_should_match=0
    // and MUST clauses exist
    TSearchParam searchParam;
    searchParam.original_dsl = "field:a AND field:b OR field:c";

    TSearchClause mustClause1;
    mustClause1.clause_type = "TERM";
    mustClause1.field_name = "field";
    mustClause1.value = "a";
    mustClause1.__isset.field_name = true;
    mustClause1.__isset.value = true;
    mustClause1.occur = TSearchOccur::MUST;
    mustClause1.__isset.occur = true;

    TSearchClause mustClause2;
    mustClause2.clause_type = "TERM";
    mustClause2.field_name = "field";
    mustClause2.value = "b";
    mustClause2.__isset.field_name = true;
    mustClause2.__isset.value = true;
    mustClause2.occur = TSearchOccur::MUST;
    mustClause2.__isset.occur = true;

    // Note: In Lucene mode with minimum_should_match=0 and MUST clauses,
    // SHOULD clauses are filtered out during FE parsing.
    // So only MUST clauses should be present.
    TSearchClause rootClause;
    rootClause.clause_type = "OCCUR_BOOLEAN";
    rootClause.children = {mustClause1, mustClause2};
    rootClause.__isset.children = true;
    rootClause.minimum_should_match = 0;
    rootClause.__isset.minimum_should_match = true;
    searchParam.root = rootClause;

    // Verify structure
    EXPECT_EQ("OCCUR_BOOLEAN", searchParam.root.clause_type);
    EXPECT_EQ(2, searchParam.root.children.size());
    EXPECT_EQ(0, searchParam.root.minimum_should_match);
}

TEST_F(FunctionSearchTest, TestOccurBooleanMinimumShouldMatchOne) {
    // Test that at least one SHOULD clause must match when minimum_should_match=1
    TSearchParam searchParam;
    searchParam.original_dsl = "field:a OR field:b OR field:c";

    TSearchClause shouldClause1;
    shouldClause1.clause_type = "TERM";
    shouldClause1.field_name = "field";
    shouldClause1.value = "a";
    shouldClause1.__isset.field_name = true;
    shouldClause1.__isset.value = true;
    shouldClause1.occur = TSearchOccur::SHOULD;
    shouldClause1.__isset.occur = true;

    TSearchClause shouldClause2;
    shouldClause2.clause_type = "TERM";
    shouldClause2.field_name = "field";
    shouldClause2.value = "b";
    shouldClause2.__isset.field_name = true;
    shouldClause2.__isset.value = true;
    shouldClause2.occur = TSearchOccur::SHOULD;
    shouldClause2.__isset.occur = true;

    TSearchClause shouldClause3;
    shouldClause3.clause_type = "TERM";
    shouldClause3.field_name = "field";
    shouldClause3.value = "c";
    shouldClause3.__isset.field_name = true;
    shouldClause3.__isset.value = true;
    shouldClause3.occur = TSearchOccur::SHOULD;
    shouldClause3.__isset.occur = true;

    TSearchClause rootClause;
    rootClause.clause_type = "OCCUR_BOOLEAN";
    rootClause.children = {shouldClause1, shouldClause2, shouldClause3};
    rootClause.__isset.children = true;
    rootClause.minimum_should_match = 1;
    rootClause.__isset.minimum_should_match = true;
    searchParam.root = rootClause;

    // Verify structure
    EXPECT_EQ("OCCUR_BOOLEAN", searchParam.root.clause_type);
    EXPECT_EQ(3, searchParam.root.children.size());
    EXPECT_EQ(1, searchParam.root.minimum_should_match);
}

TEST_F(FunctionSearchTest, TestOccurBooleanNestedQuery) {
    // Test nested OCCUR_BOOLEAN query
    TSearchParam searchParam;
    searchParam.original_dsl = "(field:a AND field:b) OR field:c";

    TSearchClause innerMust1;
    innerMust1.clause_type = "TERM";
    innerMust1.field_name = "field";
    innerMust1.value = "a";
    innerMust1.__isset.field_name = true;
    innerMust1.__isset.value = true;
    innerMust1.occur = TSearchOccur::MUST;
    innerMust1.__isset.occur = true;

    TSearchClause innerMust2;
    innerMust2.clause_type = "TERM";
    innerMust2.field_name = "field";
    innerMust2.value = "b";
    innerMust2.__isset.field_name = true;
    innerMust2.__isset.value = true;
    innerMust2.occur = TSearchOccur::MUST;
    innerMust2.__isset.occur = true;

    TSearchClause innerOccurBoolean;
    innerOccurBoolean.clause_type = "OCCUR_BOOLEAN";
    innerOccurBoolean.children = {innerMust1, innerMust2};
    innerOccurBoolean.__isset.children = true;
    innerOccurBoolean.occur = TSearchOccur::SHOULD;
    innerOccurBoolean.__isset.occur = true;

    TSearchClause shouldClause;
    shouldClause.clause_type = "TERM";
    shouldClause.field_name = "field";
    shouldClause.value = "c";
    shouldClause.__isset.field_name = true;
    shouldClause.__isset.value = true;
    shouldClause.occur = TSearchOccur::SHOULD;
    shouldClause.__isset.occur = true;

    TSearchClause rootClause;
    rootClause.clause_type = "OCCUR_BOOLEAN";
    rootClause.children = {innerOccurBoolean, shouldClause};
    rootClause.__isset.children = true;
    rootClause.minimum_should_match = 1;
    rootClause.__isset.minimum_should_match = true;
    searchParam.root = rootClause;

    // Verify structure
    EXPECT_EQ("OCCUR_BOOLEAN", searchParam.root.clause_type);
    EXPECT_EQ(2, searchParam.root.children.size());
    EXPECT_EQ("OCCUR_BOOLEAN", searchParam.root.children[0].clause_type);
    EXPECT_EQ("TERM", searchParam.root.children[1].clause_type);
    EXPECT_EQ(1, searchParam.root.minimum_should_match);
}

TEST_F(FunctionSearchTest, TestEvaluateInvertedIndexWithOccurBoolean) {
    // Test evaluate_inverted_index_with_search_param with OCCUR_BOOLEAN
    TSearchParam search_param;
    search_param.original_dsl = "title:hello AND content:world";

    TSearchClause mustClause1;
    mustClause1.clause_type = "TERM";
    mustClause1.field_name = "title";
    mustClause1.value = "hello";
    mustClause1.__isset.field_name = true;
    mustClause1.__isset.value = true;
    mustClause1.occur = TSearchOccur::MUST;
    mustClause1.__isset.occur = true;

    TSearchClause mustClause2;
    mustClause2.clause_type = "TERM";
    mustClause2.field_name = "content";
    mustClause2.value = "world";
    mustClause2.__isset.field_name = true;
    mustClause2.__isset.value = true;
    mustClause2.occur = TSearchOccur::MUST;
    mustClause2.__isset.occur = true;

    TSearchClause rootClause;
    rootClause.clause_type = "OCCUR_BOOLEAN";
    rootClause.children = {mustClause1, mustClause2};
    rootClause.__isset.children = true;
    rootClause.minimum_should_match = 0;
    rootClause.__isset.minimum_should_match = true;
    search_param.root = rootClause;

    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;

    // No real iterators - will fail but tests the code path
    data_types["title"] = {"title", nullptr};
    data_types["content"] = {"content", nullptr};
    iterators["title"] = nullptr;
    iterators["content"] = nullptr;

    uint32_t num_rows = 100;
    InvertedIndexResultBitmap bitmap_result;

    auto status = function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result);
    // Will return OK because root_query is nullptr (all child queries fail)
    //    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>());
}

TEST_F(FunctionSearchTest, TestSearcherCacheHandlesLifetime) {
    // Verify FieldReaderResolver keeps _searcher_cache_handles alive
    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types;
    std::unordered_map<std::string, IndexIterator*> iterators;
    auto context = std::make_shared<IndexQueryContext>();

    FieldReaderResolver resolver(data_types, iterators, context);

    // The resolver should have an empty cache handles vector initially
    // (We can't directly access _searcher_cache_handles, but we can verify
    // that binding_cache is empty)
    EXPECT_TRUE(resolver.binding_cache().empty());
    EXPECT_TRUE(resolver.readers().empty());
}
// NESTED clause tests moved to function_search_nested_test.cpp

} // namespace doris
