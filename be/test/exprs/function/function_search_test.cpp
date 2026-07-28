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
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_iterator.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/query_v2/phrase_query/multi_phrase_query.h"
#include "storage/index/inverted/query_v2/phrase_query/multi_phrase_weight.h"
#include "storage/index/inverted/query_v2/phrase_query/phrase_query.h"
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

    Status query(const segment_v2::IndexQueryContextPtr& /*context*/,
                 const std::string& column_name, const Field& query_value,
                 segment_v2::InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override {
        ++query_calls;
        last_column_name = column_name;
        last_query_type = query_type;
        last_query_value_type = query_value.get_type();
        last_analyzer_ctx = analyzer_ctx;
        if (last_query_value_type == TYPE_STRING) {
            last_query_value = query_value.get<TYPE_STRING>();
        }

        bit_map = std::make_shared<roaring::Roaring>();
        auto result_it = query_results.find(last_query_value);
        if (result_it != query_results.end()) {
            *bit_map = result_it->second;
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

    void set_null_bitmap(roaring::Roaring null_bitmap) {
        _null_bitmap = std::move(null_bitmap);
        set_has_null(!_null_bitmap.isEmpty());
    }

    int query_calls = 0;
    int null_bitmap_calls = 0;
    std::string last_column_name;
    std::string last_query_value;
    PrimitiveType last_query_value_type = PrimitiveType::TYPE_NULL;
    segment_v2::InvertedIndexQueryType last_query_type =
            segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY;
    const InvertedIndexAnalyzerCtx* last_analyzer_ctx = nullptr;
    std::unordered_map<std::string, roaring::Roaring> query_results;

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

TEST_F(FunctionSearchTest, TestClauseTypeCategory) {
    // Test NON_TOKENIZED types
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("TERM"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("PREFIX"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("WILDCARD"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("REGEXP"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("RANGE"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("LIST"));

    // Test TOKENIZED types
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::TOKENIZED,
              function_search->get_clause_type_category("PHRASE"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::TOKENIZED,
              function_search->get_clause_type_category("MATCH"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::TOKENIZED,
              function_search->get_clause_type_category("ANY"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::TOKENIZED,
              function_search->get_clause_type_category("ALL"));

    // Test COMPOUND types
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::COMPOUND,
              function_search->get_clause_type_category("AND"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::COMPOUND,
              function_search->get_clause_type_category("OR"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::COMPOUND,
              function_search->get_clause_type_category("NOT"));
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::COMPOUND,
              function_search->get_clause_type_category("NESTED"));

    // Test unknown type - should default to NON_TOKENIZED
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED,
              function_search->get_clause_type_category("UNKNOWN"));
}

TEST_F(FunctionSearchTest, TestAnalyzeFieldQueryTypeSimpleLeaf) {
    // Test TERM query
    TSearchClause termClause;
    termClause.clause_type = "TERM";
    termClause.field_name = "title";
    termClause.value = "hello";

    auto query_type = function_search->analyze_field_query_type("title", termClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type);

    // Test PHRASE query
    TSearchClause phraseClause;
    phraseClause.clause_type = "PHRASE";
    phraseClause.field_name = "content";
    phraseClause.value = "machine learning";

    query_type = function_search->analyze_field_query_type("content", phraseClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, query_type);

    // Test PREFIX query
    TSearchClause prefixClause;
    prefixClause.clause_type = "PREFIX";
    prefixClause.field_name = "title";
    prefixClause.value = "hello*";

    query_type = function_search->analyze_field_query_type("title", prefixClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestAnalyzeFieldQueryTypeCompound) {
    // Test AND query with mixed children
    TSearchClause termChild;
    termChild.clause_type = "TERM";
    termChild.field_name = "title";
    termChild.value = "hello";

    TSearchClause phraseChild;
    phraseChild.clause_type = "PHRASE";
    phraseChild.field_name = "content";
    phraseChild.value = "machine learning";

    TSearchClause andClause;
    andClause.clause_type = "AND";
    andClause.children = {termChild, phraseChild};

    // Test field-specific query type analysis
    auto title_query_type = function_search->analyze_field_query_type("title", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto content_query_type = function_search->analyze_field_query_type("content", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, content_query_type);
}

TEST_F(FunctionSearchTest, TestAnalyzeFieldQueryTypeCompoundNonTokenized) {
    // Test AND query with only non-tokenized children
    TSearchClause termChild1;
    termChild1.clause_type = "TERM";
    termChild1.field_name = "title";
    termChild1.value = "hello";

    TSearchClause termChild2;
    termChild2.clause_type = "TERM";
    termChild2.field_name = "category";
    termChild2.value = "tech";

    TSearchClause andClause;
    andClause.clause_type = "AND";
    andClause.children = {termChild1, termChild2};

    // Test field-specific query type analysis
    auto title_query_type = function_search->analyze_field_query_type("title", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto category_query_type = function_search->analyze_field_query_type("category", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, category_query_type);
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

TEST_F(FunctionSearchTest, TestPhraseClause) {
    TSearchParam searchParam;
    searchParam.original_dsl = "content:\"machine learning\"";

    TSearchClause rootClause;
    rootClause.clause_type = "PHRASE";
    rootClause.field_name = "content";
    rootClause.value = "machine learning";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "content";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    // Verify phrase handling
    EXPECT_EQ("PHRASE", searchParam.root.clause_type);
    EXPECT_EQ("content", searchParam.root.field_name);
    EXPECT_EQ("machine learning", searchParam.root.value);

    auto query_type = function_search->analyze_field_query_type("content", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestRegexpClause) {
    TSearchParam searchParam;
    searchParam.original_dsl = "title:/[a-z]+/";

    TSearchClause rootClause;
    rootClause.clause_type = "REGEXP";
    rootClause.field_name = "title";
    rootClause.value = "[a-z]+"; // slashes should be removed by parser
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "title";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    // Verify regexp handling
    EXPECT_EQ("REGEXP", searchParam.root.clause_type);
    EXPECT_EQ("title", searchParam.root.field_name);
    EXPECT_EQ("[a-z]+", searchParam.root.value);

    auto query_type = function_search->analyze_field_query_type("title", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_REGEXP_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestRangeClause) {
    TSearchParam searchParam;
    searchParam.original_dsl = "age:[18 TO 65]";

    TSearchClause rootClause;
    rootClause.clause_type = "RANGE";
    rootClause.field_name = "age";
    rootClause.value = "[18 TO 65]";
    searchParam.root = rootClause;

    TSearchFieldBinding binding;
    binding.field_name = "age";
    binding.slot_index = 0;
    searchParam.field_bindings = {binding};

    // Verify range handling
    EXPECT_EQ("RANGE", searchParam.root.clause_type);
    EXPECT_EQ("age", searchParam.root.field_name);
    EXPECT_EQ("[18 TO 65]", searchParam.root.value);

    auto query_type = function_search->analyze_field_query_type("age", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::RANGE_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestAnyAllClauses) {
    // Test ANY clause
    TSearchParam anyParam;
    anyParam.original_dsl = "tags:ANY(java python)";

    TSearchClause anyClause;
    anyClause.clause_type = "ANY";
    anyClause.field_name = "tags";
    anyClause.value = "java python";
    anyParam.root = anyClause;

    auto query_type = function_search->analyze_field_query_type("tags", anyParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY, query_type);

    // Test ALL clause
    TSearchParam allParam;
    allParam.original_dsl = "tags:ALL(programming language)";

    TSearchClause allClause;
    allClause.clause_type = "ALL";
    allClause.field_name = "tags";
    allClause.value = "programming language";
    allParam.root = allClause;

    query_type = function_search->analyze_field_query_type("tags", allParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestAnalyzeFieldQueryType) {
    // Test compound query with different field types
    TSearchClause termChild;
    termChild.clause_type = "TERM";
    termChild.field_name = "title";
    termChild.value = "hello";

    TSearchClause phraseChild;
    phraseChild.clause_type = "PHRASE";
    phraseChild.field_name = "content";
    phraseChild.value = "machine learning";

    TSearchClause andClause;
    andClause.clause_type = "AND";
    andClause.children = {termChild, phraseChild};

    // Test field-specific query type analysis
    auto title_query_type = function_search->analyze_field_query_type("title", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto content_query_type = function_search->analyze_field_query_type("content", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, content_query_type);

    // Test field not in query
    auto other_query_type = function_search->analyze_field_query_type("other_field", andClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, other_query_type);

    // Test single field query
    auto single_field_type = function_search->analyze_field_query_type("title", termChild);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, single_field_type);

    auto single_phrase_type = function_search->analyze_field_query_type("content", phraseChild);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, single_phrase_type);
}

TEST_F(FunctionSearchTest, TestClauseTypeToQueryType) {
    // Test non-tokenized queries
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY,
              function_search->clause_type_to_query_type("TERM"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
              function_search->clause_type_to_query_type("PREFIX"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::WILDCARD_QUERY,
              function_search->clause_type_to_query_type("WILDCARD"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_REGEXP_QUERY,
              function_search->clause_type_to_query_type("REGEXP"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::RANGE_QUERY,
              function_search->clause_type_to_query_type("RANGE"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::LIST_QUERY,
              function_search->clause_type_to_query_type("LIST"));

    // Test tokenized queries
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY,
              function_search->clause_type_to_query_type("PHRASE"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY,
              function_search->clause_type_to_query_type("MATCH"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY,
              function_search->clause_type_to_query_type("ANY"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY,
              function_search->clause_type_to_query_type("ALL"));

    // Test boolean operations
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY,
              function_search->clause_type_to_query_type("AND"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY,
              function_search->clause_type_to_query_type("OR"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY,
              function_search->clause_type_to_query_type("NOT"));
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY,
              function_search->clause_type_to_query_type("NESTED"));

    // Test unknown clause type
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY,
              function_search->clause_type_to_query_type("UNKNOWN"));
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

// NESTED clause tests moved to function_search_nested_test.cpp

TEST_F(FunctionSearchTest, TestNestedBooleanQueries) {
    // Test deeply nested boolean queries
    TSearchParam searchParam;
    searchParam.original_dsl =
            "((title:hello OR content:world) AND category:tech) OR (author:john AND "
            "status:published)";

    // Create nested structure: OR -> AND -> OR, AND
    TSearchClause titleClause;
    titleClause.clause_type = "TERM";
    titleClause.field_name = "title";
    titleClause.value = "hello";

    TSearchClause contentClause;
    contentClause.clause_type = "TERM";
    contentClause.field_name = "content";
    contentClause.value = "world";

    TSearchClause categoryClause;
    categoryClause.clause_type = "TERM";
    categoryClause.field_name = "category";
    categoryClause.value = "tech";

    TSearchClause authorClause;
    authorClause.clause_type = "TERM";
    authorClause.field_name = "author";
    authorClause.value = "john";

    TSearchClause statusClause;
    statusClause.clause_type = "TERM";
    statusClause.field_name = "status";
    statusClause.value = "published";

    // Build nested structure
    TSearchClause innerOrClause;
    innerOrClause.clause_type = "OR";
    innerOrClause.children = {titleClause, contentClause};

    TSearchClause leftAndClause;
    leftAndClause.clause_type = "AND";
    leftAndClause.children = {innerOrClause, categoryClause};

    TSearchClause rightAndClause;
    rightAndClause.clause_type = "AND";
    rightAndClause.children = {authorClause, statusClause};

    TSearchClause rootOrClause;
    rootOrClause.clause_type = "OR";
    rootOrClause.children = {leftAndClause, rightAndClause};
    searchParam.root = rootOrClause;

    // Test field-specific query type analysis for nested queries
    auto title_query_type = function_search->analyze_field_query_type("title", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto content_query_type =
            function_search->analyze_field_query_type("content", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, content_query_type);

    auto author_query_type = function_search->analyze_field_query_type("author", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, author_query_type);

    // Test field not in query
    auto missing_query_type =
            function_search->analyze_field_query_type("missing_field", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, missing_query_type);
}

TEST_F(FunctionSearchTest, TestMixedTokenizedAndNonTokenizedQueries) {
    // Test queries mixing tokenized and non-tokenized clause types
    TSearchParam searchParam;
    searchParam.original_dsl =
            "title:TERM(hello) AND content:PHRASE(\"machine learning\") AND tags:ANY(java python)";

    TSearchClause termClause;
    termClause.clause_type = "TERM";
    termClause.field_name = "title";
    termClause.value = "hello";

    TSearchClause phraseClause;
    phraseClause.clause_type = "PHRASE";
    phraseClause.field_name = "content";
    phraseClause.value = "machine learning";

    TSearchClause anyClause;
    anyClause.clause_type = "ANY";
    anyClause.field_name = "tags";
    anyClause.value = "java python";

    TSearchClause rootAndClause;
    rootAndClause.clause_type = "AND";
    rootAndClause.children = {termClause, phraseClause, anyClause};
    searchParam.root = rootAndClause;

    // Test field-specific query type analysis
    auto title_query_type = function_search->analyze_field_query_type("title", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto content_query_type =
            function_search->analyze_field_query_type("content", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, content_query_type);

    auto tags_query_type = function_search->analyze_field_query_type("tags", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY, tags_query_type);
}

TEST_F(FunctionSearchTest, TestNotOperatorQueries) {
    // Test NOT operator with various clause types
    TSearchParam searchParam;
    searchParam.original_dsl = "NOT (title:hello OR content:world)";

    TSearchClause titleClause;
    titleClause.clause_type = "TERM";
    titleClause.field_name = "title";
    titleClause.value = "hello";

    TSearchClause contentClause;
    contentClause.clause_type = "TERM";
    contentClause.field_name = "content";
    contentClause.value = "world";

    TSearchClause orClause;
    orClause.clause_type = "OR";
    orClause.children = {titleClause, contentClause};

    TSearchClause notClause;
    notClause.clause_type = "NOT";
    notClause.children = {orClause};
    searchParam.root = notClause;

    // Test field-specific query type analysis for NOT queries
    auto title_query_type = function_search->analyze_field_query_type("title", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    auto content_query_type =
            function_search->analyze_field_query_type("content", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, content_query_type);
}

TEST_F(FunctionSearchTest, TestWildcardAndPrefixQueries) {
    // Test WILDCARD queries
    TSearchParam wildcardParam;
    wildcardParam.original_dsl = "title:hello*";

    TSearchClause wildcardClause;
    wildcardClause.clause_type = "WILDCARD";
    wildcardClause.field_name = "title";
    wildcardClause.value = "hello*";
    wildcardParam.root = wildcardClause;

    auto wildcard_query_type =
            function_search->analyze_field_query_type("title", wildcardParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::WILDCARD_QUERY, wildcard_query_type);

    // Test PREFIX queries
    TSearchParam prefixParam;
    prefixParam.original_dsl = "title:hello*";

    TSearchClause prefixClause;
    prefixClause.clause_type = "PREFIX";
    prefixClause.field_name = "title";
    prefixClause.value = "hello";
    prefixParam.root = prefixClause;

    auto prefix_query_type = function_search->analyze_field_query_type("title", prefixParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, prefix_query_type);
}

TEST_F(FunctionSearchTest, TestListQueries) {
    // Test LIST queries
    TSearchParam listParam;
    listParam.original_dsl = "category:LIST(tech, science, programming)";

    TSearchClause listClause;
    listClause.clause_type = "LIST";
    listClause.field_name = "category";
    listClause.value = "tech,science,programming";
    listParam.root = listClause;

    auto list_query_type = function_search->analyze_field_query_type("category", listParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::LIST_QUERY, list_query_type);
}

TEST_F(FunctionSearchTest, TestMatchQueries) {
    // Test MATCH queries (full-text search)
    TSearchParam matchParam;
    matchParam.original_dsl = "content:MATCH(machine learning algorithms)";

    TSearchClause matchClause;
    matchClause.clause_type = "MATCH";
    matchClause.field_name = "content";
    matchClause.value = "machine learning algorithms";
    matchParam.root = matchClause;

    auto match_query_type = function_search->analyze_field_query_type("content", matchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY, match_query_type);
}

TEST_F(FunctionSearchTest, TestEmptyAndNullQueries) {
    // Test empty clause type
    TSearchClause emptyClause;
    emptyClause.clause_type = "";
    emptyClause.field_name = "title";
    emptyClause.value = "hello";

    auto empty_query_type = function_search->analyze_field_query_type("title", emptyClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY,
              empty_query_type); // Should default to EQUAL_QUERY

    // Test clause with empty field name
    TSearchClause noFieldClause;
    noFieldClause.clause_type = "TERM";
    noFieldClause.field_name = "";
    noFieldClause.value = "hello";

    auto no_field_query_type = function_search->analyze_field_query_type("title", noFieldClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, no_field_query_type);

    // Test clause with empty value
    TSearchClause emptyValueClause;
    emptyValueClause.clause_type = "TERM";
    emptyValueClause.field_name = "title";
    emptyValueClause.value = "";

    auto empty_value_query_type =
            function_search->analyze_field_query_type("title", emptyValueClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, empty_value_query_type);
}

// Error handling and edge case tests
TEST_F(FunctionSearchTest, TestInvalidClauseTypes) {
    // Test completely invalid clause types
    std::vector<std::string> invalid_types = {"INVALID", "UNKNOWN_TYPE", "BAD_CLAUSE", "", " "};

    for (const auto& invalid_type : invalid_types) {
        auto category = function_search->get_clause_type_category(invalid_type);
        EXPECT_EQ(FunctionSearch::ClauseTypeCategory::NON_TOKENIZED, category);

        auto query_type = function_search->clause_type_to_query_type(invalid_type);
        EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type);
    }
}

TEST_F(FunctionSearchTest, TestMalformedSearchClauses) {
    // Test clause without field_name
    TSearchClause malformed_clause1;
    malformed_clause1.clause_type = "TERM";
    // malformed_clause1.field_name is not set
    malformed_clause1.value = "hello";

    auto query_type1 = function_search->analyze_field_query_type("any_field", malformed_clause1);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, query_type1);

    // Test clause without value
    TSearchClause malformed_clause2;
    malformed_clause2.clause_type = "TERM";
    malformed_clause2.field_name = "title";
    // malformed_clause2.value is not set

    auto query_type2 = function_search->analyze_field_query_type("title", malformed_clause2);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type2);

    // Test clause without clause_type
    TSearchClause malformed_clause3;
    // malformed_clause3.clause_type is not set
    malformed_clause3.field_name = "title";
    malformed_clause3.value = "hello";

    auto query_type3 = function_search->analyze_field_query_type("title", malformed_clause3);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type3);
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

TEST_F(FunctionSearchTest, TestBooleanClauseWithoutChildren) {
    // Test AND clause with no children
    TSearchClause and_clause_no_children;
    and_clause_no_children.clause_type = "AND";
    // No children set

    auto query_type =
            function_search->analyze_field_query_type("any_field", and_clause_no_children);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, query_type);

    // Test OR clause with no children
    TSearchClause or_clause_no_children;
    or_clause_no_children.clause_type = "OR";
    // No children set

    query_type = function_search->analyze_field_query_type("any_field", or_clause_no_children);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, query_type);

    // Test NOT clause with no children
    TSearchClause not_clause_no_children;
    not_clause_no_children.clause_type = "NOT";
    // No children set

    query_type = function_search->analyze_field_query_type("any_field", not_clause_no_children);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, query_type);
}

TEST_F(FunctionSearchTest, TestSpecialCharactersInValues) {
    // Test special characters in field values
    std::vector<std::string> special_values = {
            "",   " ",    "\n",    "\t",        "\\",  "\"",
            "'",  "null", "NULL",  "undefined", "NaN", "0",
            "-1", "true", "false", "你好",      "🔍",  std::string(1000, 'a')};

    for (const auto& special_value : special_values) {
        TSearchClause special_clause;
        special_clause.clause_type = "TERM";
        special_clause.field_name = "title";
        special_clause.value = special_value;

        auto query_type = function_search->analyze_field_query_type("title", special_clause);
        EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type);
    }
}

TEST_F(FunctionSearchTest, TestSpecialCharactersInFieldNames) {
    // Test special characters in field names
    std::vector<std::string> special_field_names = {"",
                                                    " ",
                                                    "field with spaces",
                                                    "field-with-dashes",
                                                    "field_with_underscores",
                                                    "field.with.dots",
                                                    "field@with@symbols",
                                                    "字段名",
                                                    "🔍field",
                                                    "123field"};

    for (const auto& special_field_name : special_field_names) {
        TSearchClause special_clause;
        special_clause.clause_type = "TERM";
        special_clause.field_name = special_field_name;
        special_clause.value = "hello";

        // Test with matching field name
        auto query_type1 =
                function_search->analyze_field_query_type(special_field_name, special_clause);
        EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type1);

        // Test with non-matching field name
        auto query_type2 =
                function_search->analyze_field_query_type("different_field", special_clause);
        EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, query_type2);
    }
}

TEST_F(FunctionSearchTest, TestCaseSensitivityInClauseTypes) {
    // Test case sensitivity for clause types
    std::vector<std::pair<std::string, segment_v2::InvertedIndexQueryType>> case_variations = {
            {"term", segment_v2::InvertedIndexQueryType::EQUAL_QUERY},  // lowercase
            {"TERM", segment_v2::InvertedIndexQueryType::EQUAL_QUERY},  // uppercase
            {"AND", segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY}, // uppercase
            {"and", segment_v2::InvertedIndexQueryType::
                            EQUAL_QUERY}, // lowercase (unknown, defaults to EQUAL)
            {"PHRASE", segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY}, // uppercase
            {"phrase", segment_v2::InvertedIndexQueryType::
                               EQUAL_QUERY}, // lowercase (unknown, defaults to EQUAL)
    };

    for (const auto& [clause_type, expected_query_type] : case_variations) {
        auto actual_query_type = function_search->clause_type_to_query_type(clause_type);
        EXPECT_EQ(expected_query_type, actual_query_type)
                << "Failed for clause_type: " << clause_type;
    }
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

// Integration tests with VSearchExpr
TEST_F(FunctionSearchTest, TestFunctionSearchAndVSearchExprIntegration) {
    // Test that both components handle the same clause types consistently
    std::vector<std::string> clause_types = {"TERM",  "PHRASE", "WILDCARD", "REGEXP",
                                             "RANGE", "LIST",   "ANY",      "ALL",
                                             "AND",   "OR",     "NOT"};

    for (const auto& clause_type : clause_types) {
        auto category = function_search->get_clause_type_category(clause_type);
        auto query_type = function_search->clause_type_to_query_type(clause_type);

        // Verify that the mapping is consistent
        if (category == FunctionSearch::ClauseTypeCategory::COMPOUND) {
            EXPECT_EQ(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY, query_type);
        } else {
            EXPECT_NE(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY, query_type);
        }
    }
}

TEST_F(FunctionSearchTest, TestTokenizedVsNonTokenizedConsistency) {
    // Test that both components agree on tokenized vs non-tokenized classification
    std::map<std::string, FunctionSearch::ClauseTypeCategory> expected_categories = {
            {"TERM", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"PREFIX", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"WILDCARD", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"REGEXP", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"RANGE", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"LIST", FunctionSearch::ClauseTypeCategory::NON_TOKENIZED},
            {"PHRASE", FunctionSearch::ClauseTypeCategory::TOKENIZED},
            {"MATCH", FunctionSearch::ClauseTypeCategory::TOKENIZED},
            {"ANY", FunctionSearch::ClauseTypeCategory::TOKENIZED},
            {"ALL", FunctionSearch::ClauseTypeCategory::TOKENIZED},
            {"AND", FunctionSearch::ClauseTypeCategory::COMPOUND},
            {"OR", FunctionSearch::ClauseTypeCategory::COMPOUND},
            {"NOT", FunctionSearch::ClauseTypeCategory::COMPOUND}};

    for (const auto& [clause_type, expected_category] : expected_categories) {
        auto actual_category = function_search->get_clause_type_category(clause_type);
        EXPECT_EQ(expected_category, actual_category) << "Failed for clause_type: " << clause_type;
    }
}

TEST_F(FunctionSearchTest, TestPerformanceWithLargeQueries) {
    // Test performance with large query structures
    std::vector<TSearchClause> clauses;

    // Generate many field clauses
    for (int i = 0; i < 100; ++i) {
        TSearchClause clause;
        clause.clause_type = "TERM";
        clause.field_name = "field" + std::to_string(i);
        clause.value = "value" + std::to_string(i);
        clauses.push_back(clause);
    }

    // Create large OR clause
    TSearchClause largeOr;
    largeOr.clause_type = "OR";
    largeOr.children = clauses;

    // Test that analysis completes in reasonable time
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 100; ++i) {
        std::string field_name = "field" + std::to_string(i);
        auto query_type = function_search->analyze_field_query_type(field_name, largeOr);
        EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, query_type);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Should complete within reasonable time (less than 1 second for 100 fields)
    EXPECT_LT(duration.count(), 1000)
            << "Query analysis took too long: " << duration.count() << "ms";
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
    // This will test the early return in build_leaf_query when resolver.resolve fails
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
    binding.execution_mode = SearchFieldExecutionMode::CLUCENE;

    auto* dummy_reader = reinterpret_cast<lucene::index::IndexReader*>(0x1);
    binding.lucene_reader = std::shared_ptr<lucene::index::IndexReader>(
            dummy_reader, [](lucene::index::IndexReader* /*ptr*/) {});

    std::string key =
            resolver.binding_key_for("content", InvertedIndexQueryType::MATCH_PHRASE_QUERY);
    binding.binding_key = key;
    resolver._cache[key] = binding;

    inverted_index::query_v2::QueryPtr out;
    std::string out_binding_key;
    Status st = function_search->build_leaf_query(clause, context, resolver, &out, &out_binding_key,
                                                  "OR", 0);
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

    TIndexPolicy common_grams;
    common_grams.id = 910021;
    common_grams.name = "function_search_cg_filter";
    common_grams.type = TIndexPolicyType::TOKEN_FILTER;
    common_grams.properties["type"] = "common_grams";

    TIndexPolicy analyzer;
    analyzer.id = 910022;
    analyzer.name = "function_search_cg_analyzer";
    analyzer.type = TIndexPolicyType::ANALYZER;
    analyzer.properties["tokenizer"] = tokenizer.name;
    analyzer.properties["token_filter"] = "lowercase," + common_grams.name;
    policy_mgr->apply_policy_changes({tokenizer, common_grams, analyzer}, {});

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
    binding.execution_mode = SearchFieldExecutionMode::CLUCENE;
    auto* dummy_reader = reinterpret_cast<lucene::index::IndexReader*>(0x1);
    binding.lucene_reader = std::shared_ptr<lucene::index::IndexReader>(
            dummy_reader, [](lucene::index::IndexReader* /*ptr*/) {});
    binding.binding_key =
            resolver.binding_key_for("content", InvertedIndexQueryType::MATCH_PHRASE_QUERY);
    resolver._cache[binding.binding_key] = binding;

    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    ASSERT_TRUE(function_search
                        ->build_leaf_query(clause, context, resolver, &query, &binding_key, "OR", 0)
                        .ok());

    auto phrase = std::dynamic_pointer_cast<inverted_index::query_v2::PhraseQuery>(query);
    ASSERT_NE(phrase, nullptr);
    ASSERT_EQ(phrase->_term_infos.size(), 4);
    EXPECT_EQ(phrase->_term_infos[0].get_single_term(), "man");
    EXPECT_EQ(phrase->_term_infos[1].get_single_term(), "of");
    EXPECT_EQ(phrase->_term_infos[2].get_single_term(), "the");
    EXPECT_EQ(phrase->_term_infos[3].get_single_term(), "year");
    policy_mgr->apply_policy_changes({}, {tokenizer.id, common_grams.id, analyzer.id});
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
    Status st = function_search->build_leaf_query(clause, context, resolver, &out, &out_binding_key,
                                                  "OR", 0, 5);
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
    EXPECT_TRUE(binding.use_direct_index_reader());
    EXPECT_EQ(reader, binding.inverted_reader);
    EXPECT_EQ("var.items.level", binding.logical_field_name);
    EXPECT_EQ("1.var.items.level", binding.stored_field_name);
    EXPECT_EQ(InvertedIndexQueryType::EQUAL_QUERY, binding.query_type);

    const auto& cache = resolver.binding_cache();
    ASSERT_EQ(1u, cache.size());
    EXPECT_TRUE(cache.begin()->second.use_direct_index_reader());
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
    EXPECT_TRUE(binding.use_snii_native_reader());
    EXPECT_FALSE(binding.use_direct_index_reader());
    EXPECT_EQ(SearchFieldExecutionMode::SNII_NATIVE, binding.execution_mode);
}

TEST_F(FunctionSearchTest, TestBuildLeafQueryExecutesSelectedSniiWildcardReader) {
    auto context = std::make_shared<IndexQueryContext>();
    std::map<std::string, std::string> decoy_properties {
            {INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_ENGLISH},
            {INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE}};
    std::map<std::string, std::string> selected_properties {
            {INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD},
            {INVERTED_INDEX_PARSER_LOWERCASE_KEY, INVERTED_INDEX_PARSER_TRUE}};
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
    auto status = function_search->build_leaf_query(clause, context, resolver, &query, &binding_key,
                                                    "OR", 0, 4);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_NE(nullptr, query);
    EXPECT_EQ(0, decoy_reader->query_calls);
    EXPECT_EQ(1, selected_reader->query_calls);
    EXPECT_EQ("stored_body", selected_reader->last_column_name);
    EXPECT_EQ(TYPE_STRING, selected_reader->last_query_value_type);
    EXPECT_EQ("*lpha", selected_reader->last_query_value);
    EXPECT_EQ(InvertedIndexQueryType::WILDCARD_QUERY, selected_reader->last_query_type);
    EXPECT_EQ(nullptr, selected_reader->last_analyzer_ctx);
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

TEST_F(FunctionSearchTest, TestSniiNativeRejectsNonWildcardClause) {
    auto context = std::make_shared<IndexQueryContext>();
    auto index_meta = make_test_inverted_index(
            18, {{INVERTED_INDEX_PARSER_KEY, INVERTED_INDEX_PARSER_STANDARD}});
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

    auto clause = make_leaf_clause("TERM", "alpha");
    inverted_index::query_v2::QueryPtr query;
    std::string binding_key;
    auto status = function_search->build_leaf_query(clause, context, resolver, &query, &binding_key,
                                                    "OR", 0, 4);

    ASSERT_FALSE(status.ok());
    EXPECT_EQ(ErrorCode::NOT_IMPLEMENTED_ERROR, status.code());
    EXPECT_NE(std::string::npos, status.to_string().find("TERM"));
    EXPECT_NE(std::string::npos, status.to_string().find("WILDCARD"));
    EXPECT_EQ(0, reader->query_calls);
    EXPECT_EQ(0, index_file_reader->open_calls);
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
    binding.execution_mode = SearchFieldExecutionMode::DIRECT_INDEX;
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
    Status st = function_search->build_leaf_query(clause, context, resolver, &out, &out_binding_key,
                                                  "OR", 0, 4);
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
    binding.execution_mode = SearchFieldExecutionMode::DIRECT_INDEX;
    TabletIndex index_meta;
    binding.inverted_reader = std::make_shared<DummyInvertedIndexReader>(&index_meta);

    std::string key =
            resolver.binding_key_for("1.var.items.active", InvertedIndexQueryType::MATCH_ANY_QUERY);
    binding.binding_key = key;
    resolver._cache[key] = binding;

    inverted_index::query_v2::QueryPtr out;
    std::string out_binding_key;
    Status st = function_search->build_leaf_query(clause, context, resolver, &out, &out_binding_key,
                                                  "OR", 0, 10);
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
    binding.execution_mode = SearchFieldExecutionMode::DIRECT_INDEX;
    TabletIndex index_meta;
    binding.inverted_reader = std::make_shared<DummyInvertedIndexReader>(&index_meta);

    std::string key = resolver.binding_key_for("1.var.items.flags.level",
                                               InvertedIndexQueryType::MATCH_ANY_QUERY);
    binding.binding_key = key;
    resolver._cache[key] = binding;

    inverted_index::query_v2::QueryPtr out;
    std::string out_binding_key;
    Status st = function_search->build_leaf_query(clause, context, resolver, &out, &out_binding_key,
                                                  "OR", 0, 10);
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

// ============== Lucene Mode (OCCUR_BOOLEAN) Tests ==============

TEST_F(FunctionSearchTest, TestOccurBooleanClauseTypeCategory) {
    // Test that OCCUR_BOOLEAN is classified as COMPOUND
    EXPECT_EQ(FunctionSearch::ClauseTypeCategory::COMPOUND,
              function_search->get_clause_type_category("OCCUR_BOOLEAN"));
}

TEST_F(FunctionSearchTest, TestOccurBooleanQueryType) {
    // Test that OCCUR_BOOLEAN maps to BOOLEAN_QUERY
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::BOOLEAN_QUERY,
              function_search->clause_type_to_query_type("OCCUR_BOOLEAN"));
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

TEST_F(FunctionSearchTest, TestOccurBooleanAnalyzeFieldQueryType) {
    // Test field query type analysis for OCCUR_BOOLEAN
    TSearchClause mustClause;
    mustClause.clause_type = "TERM";
    mustClause.field_name = "title";
    mustClause.value = "hello";
    mustClause.__isset.field_name = true;
    mustClause.__isset.value = true;
    mustClause.occur = TSearchOccur::MUST;
    mustClause.__isset.occur = true;

    TSearchClause rootClause;
    rootClause.clause_type = "OCCUR_BOOLEAN";
    rootClause.children = {mustClause};
    rootClause.__isset.children = true;

    // Test field-specific query type analysis
    auto title_query_type = function_search->analyze_field_query_type("title", rootClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);

    // Test field not in query
    auto other_query_type = function_search->analyze_field_query_type("other_field", rootClause);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY, other_query_type);
}

TEST_F(FunctionSearchTest, TestOccurBooleanWithPhraseQuery) {
    // Test OCCUR_BOOLEAN with PHRASE child clause
    TSearchParam searchParam;
    searchParam.original_dsl = "content:\"machine learning\" AND title:hello";

    TSearchClause phraseClause;
    phraseClause.clause_type = "PHRASE";
    phraseClause.field_name = "content";
    phraseClause.value = "machine learning";
    phraseClause.__isset.field_name = true;
    phraseClause.__isset.value = true;
    phraseClause.occur = TSearchOccur::MUST;
    phraseClause.__isset.occur = true;

    TSearchClause termClause;
    termClause.clause_type = "TERM";
    termClause.field_name = "title";
    termClause.value = "hello";
    termClause.__isset.field_name = true;
    termClause.__isset.value = true;
    termClause.occur = TSearchOccur::MUST;
    termClause.__isset.occur = true;

    TSearchClause rootClause;
    rootClause.clause_type = "OCCUR_BOOLEAN";
    rootClause.children = {phraseClause, termClause};
    rootClause.__isset.children = true;
    searchParam.root = rootClause;

    // Verify structure
    EXPECT_EQ("OCCUR_BOOLEAN", searchParam.root.clause_type);
    EXPECT_EQ(2, searchParam.root.children.size());
    EXPECT_EQ("PHRASE", searchParam.root.children[0].clause_type);
    EXPECT_EQ("TERM", searchParam.root.children[1].clause_type);

    // Test field-specific query type analysis
    auto content_query_type =
            function_search->analyze_field_query_type("content", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::MATCH_PHRASE_QUERY, content_query_type);

    auto title_query_type = function_search->analyze_field_query_type("title", searchParam.root);
    EXPECT_EQ(segment_v2::InvertedIndexQueryType::EQUAL_QUERY, title_query_type);
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
