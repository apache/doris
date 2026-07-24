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

#include <CLucene.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/data_type/data_type_string.h"
#include "exprs/function/function_search.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/compaction/collection_similarity.h"
#include "storage/compaction/collection_statistics.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/analyzer/custom_analyzer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/inverted/query_v2/collect/multi_segment_util.h"
#include "storage/olap_common.h"
#include "util/faststring.h"

namespace doris {

using doris::segment_v2::FullTextIndexReader;
using doris::segment_v2::IndexFileReader;
using doris::segment_v2::IndexFileWriter;
using doris::segment_v2::IndexIterator;
using doris::segment_v2::IndexQueryContext;
using doris::segment_v2::IndexQueryContextPtr;
using doris::segment_v2::InvertedIndexIterator;
using doris::segment_v2::InvertedIndexQueryCache;
using doris::segment_v2::InvertedIndexReaderType;
using doris::segment_v2::InvertedIndexResultBitmap;
using doris::segment_v2::InvertedIndexSearcherCache;
using doris::segment_v2::TmpFileDirs;
using doris::segment_v2::inverted_index::query_v2::sub_readers;

class FixedCollectionStatistics final : public CollectionStatistics {
public:
    float get_or_calculate_idf(const std::wstring&, const std::wstring&) { return 1.0F; }
    float get_or_calculate_avg_dl(const std::wstring&) { return 1.0F; }
};

class FunctionSearchMultiSegmentReadTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/function_search_multi_segment_read_test";
    const std::string kTmpDir = "./ut_dir/function_search_multi_segment_read_tmp";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;

        st = io::global_local_filesystem()->delete_directory(kTmpDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTmpDir);
        ASSERT_TRUE(st.ok()) << st;

        std::vector<StorePath> paths;
        paths.emplace_back(kTmpDir, -1);
        auto tmp_file_dirs = std::make_unique<TmpFileDirs>(paths);
        st = tmp_file_dirs->init();
        ASSERT_TRUE(st.ok()) << st;
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        _searcher_cache.reset(
                InvertedIndexSearcherCache::create_global_instance(1024 * 1024 * 1024, 1));
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_searcher_cache.get());

        _query_cache.reset(InvertedIndexQueryCache::create_global_cache(1024 * 1024 * 1024, 1));
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_query_cache.get());

        _function_search = std::make_shared<FunctionSearch>();

        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        query_options.enable_inverted_index_query_cache = false;
        _runtime_state.set_query_options(query_options);

        _index_query_context = std::make_shared<IndexQueryContext>();
        _index_query_context->io_ctx = &_io_ctx;
        _index_query_context->stats = &_stats;
        _index_query_context->runtime_state = &_runtime_state;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTmpDir).ok());
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(nullptr);
        ExecEnv::GetInstance()->set_inverted_index_query_cache(nullptr);
    }

protected:
    TabletIndex create_fulltext_index_meta(int64_t index_id, int column_uid,
                                           const std::string& index_name) {
        TabletIndexPB pb;
        pb.set_index_type(IndexType::INVERTED);
        pb.set_index_id(index_id);
        pb.set_index_name(index_name);
        pb.add_col_unique_id(column_uid);
        pb.mutable_properties()->insert({"parser", "english"});
        pb.mutable_properties()->insert({"lower_case", "true"});

        TabletIndex idx_meta;
        idx_meta.init_from_pb(pb);
        return idx_meta;
    }

    std::string local_segment_path(const std::string& base, const std::string& rowset_id,
                                   int64_t seg_id) {
        return fmt::format("{}/{}_{}.dat", base, rowset_id, seg_id);
    }

    std::string prepare_fulltext_index(const std::string& rowset_id, int64_t seg_id,
                                       const std::string& stored_field_name,
                                       const std::vector<std::string>& docs,
                                       const TabletIndex& idx_meta, int32_t max_buffered_docs,
                                       const std::vector<uint32_t>& null_docs = {}) {
        std::string index_path_prefix {
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(
                        local_segment_path(kTestDir, rowset_id, seg_id))};
        const std::string index_path =
                segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);

        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto st = io::global_local_filesystem()->create_file(index_path, &file_writer, &opts);
        EXPECT_TRUE(st.ok()) << st;

        auto index_file_writer = std::make_unique<IndexFileWriter>(
                io::global_local_filesystem(), index_path_prefix, rowset_id, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));
        auto dir_result = index_file_writer->open(&idx_meta);
        EXPECT_TRUE(dir_result.has_value()) << dir_result.error();
        auto dir = dir_result.value();

        segment_v2::inverted_index::CustomAnalyzerConfig::Builder builder;
        builder.with_tokenizer_config("standard", {});
        auto analyzer =
                segment_v2::inverted_index::CustomAnalyzer::build_custom_analyzer(builder.build());

        auto* index_writer = _CLNEW lucene::index::IndexWriter(dir.get(), analyzer.get(), true);
        index_writer->setMaxBufferedDocs(max_buffered_docs);
        index_writer->setRAMBufferSizeMB(-1);
        index_writer->setMaxFieldLength(0x7FFFFFFFL);
        index_writer->setMergeFactor(1000000000);
        index_writer->setUseCompoundFile(false);

        auto char_reader = std::make_shared<lucene::util::SStringReader<char>>();
        auto* doc = _CLNEW lucene::document::Document();
        int32_t field_config = lucene::document::Field::STORE_NO;
        field_config |= lucene::document::Field::INDEX_NONORMS;
        field_config |= lucene::document::Field::INDEX_TOKENIZED;
        auto field_name_w = segment_v2::inverted_index::StringHelper::to_wstring(stored_field_name);
        auto* field = _CLNEW lucene::document::Field(field_name_w.c_str(), field_config);
        field->setOmitTermFreqAndPositions(false);
        doc->add(*field);

        for (const auto& value : docs) {
            char_reader->init(value.data(), value.size(), false);
            auto* stream = analyzer->reusableTokenStream(field->name(), char_reader);
            field->setValue(stream);
            index_writer->addDocument(doc);
        }

        index_writer->close();
        _CLLDELETE(index_writer);
        _CLLDELETE(doc);

        if (!null_docs.empty()) {
            roaring::Roaring null_bitmap;
            for (uint32_t doc_id : null_docs) {
                null_bitmap.add(doc_id);
            }
            null_bitmap.runOptimize();
            const size_t null_bitmap_size = null_bitmap.getSizeInBytes(false);
            faststring buffer;
            buffer.resize(null_bitmap_size);
            null_bitmap.write(reinterpret_cast<char*>(buffer.data()), false);

            std::unique_ptr<lucene::store::IndexOutput> null_bitmap_out(dir->createOutput(
                    segment_v2::InvertedIndexDescriptor::get_temporary_null_bitmap_file_name()));
            null_bitmap_out->writeBytes(buffer.data(), static_cast<int32_t>(null_bitmap_size));
            null_bitmap_out->close();
        }
        dir->close();

        st = index_file_writer->begin_close();
        EXPECT_TRUE(st.ok()) << st;
        st = index_file_writer->finish_close();
        EXPECT_TRUE(st.ok()) << st;
        return index_path_prefix;
    }

    std::unique_ptr<InvertedIndexIterator> create_iterator(const std::string& index_path_prefix,
                                                           const TabletIndex& idx_meta,
                                                           const IndexQueryContextPtr& context) {
        auto file_reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
        auto st = file_reader->init();
        EXPECT_TRUE(st.ok()) << st;

        auto iterator = std::make_unique<InvertedIndexIterator>();
        auto reader = FullTextIndexReader::create_shared(&idx_meta, file_reader);
        iterator->add_reader(InvertedIndexReaderType::FULLTEXT, reader);
        iterator->set_context(context);
        return iterator;
    }

    void assert_multi_segment_reader(const std::string& index_path_prefix,
                                     const TabletIndex& idx_meta) {
        auto file_reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
        auto st = file_reader->init();
        ASSERT_TRUE(st.ok()) << st;
        auto dir_result = file_reader->open(&idx_meta, &_io_ctx);
        ASSERT_TRUE(dir_result.has_value()) << dir_result.error();
        auto dir = std::move(dir_result.value());

        auto* raw_reader = lucene::index::IndexReader::open(dir.get());
        ASSERT_NE(raw_reader, nullptr);
        std::shared_ptr<lucene::index::IndexReader> reader(raw_reader,
                                                           [](lucene::index::IndexReader* r) {
                                                               if (r != nullptr) {
                                                                   r->close();
                                                                   _CLDELETE(r);
                                                               }
                                                           });
        const auto* segments = sub_readers(reader.get());
        ASSERT_NE(segments, nullptr);
        ASSERT_GT(segments->length, 1);
    }

    std::shared_ptr<FunctionSearch> _function_search;

    io::IOContext _io_ctx;
    OlapReaderStatistics _stats;
    RuntimeState _runtime_state;
    IndexQueryContextPtr _index_query_context;

private:
    std::unique_ptr<InvertedIndexSearcherCache> _searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _query_cache;
};

TEST_F(FunctionSearchMultiSegmentReadTest, SearchReadsMultipleMultiSegmentReaders) {
    const std::string title_field = "1";
    const std::string body_field = "2";
    const uint32_t num_rows = 4;

    TabletIndex title_meta = create_fulltext_index_meta(101, 1, "title_index");
    TabletIndex body_meta = create_fulltext_index_meta(102, 2, "body_index");

    const std::string title_index_prefix = prepare_fulltext_index(
            "title_rowset", 0, title_field,
            {"apple intro", "plain title", "history text", "apple ending"}, title_meta, 2);
    const std::string body_index_prefix = prepare_fulltext_index(
            "body_rowset", 0, body_field,
            {"nothing here", "fleabag season", "history text", "fleabag finale"}, body_meta, 2);
    assert_multi_segment_reader(body_index_prefix, body_meta);

    auto title_iterator = create_iterator(title_index_prefix, title_meta, _index_query_context);
    auto body_iterator = create_iterator(body_index_prefix, body_meta, _index_query_context);

    DataTypePtr string_type = std::make_shared<DataTypeString>();
    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types {
            {"title", {title_field, string_type}}, {"body", {body_field, string_type}}};
    std::unordered_map<std::string, IndexIterator*> iterators {{"title", title_iterator.get()},
                                                               {"body", body_iterator.get()}};

    TSearchClause title_clause;
    title_clause.clause_type = "TERM";
    title_clause.field_name = "title";
    title_clause.value = "apple";
    title_clause.__isset.field_name = true;
    title_clause.__isset.value = true;

    TSearchClause body_clause;
    body_clause.clause_type = "TERM";
    body_clause.field_name = "body";
    body_clause.value = "fleabag";
    body_clause.__isset.field_name = true;
    body_clause.__isset.value = true;

    TSearchClause root_clause;
    root_clause.clause_type = "AND";
    root_clause.children = {title_clause, body_clause};
    root_clause.__isset.children = true;

    TSearchParam search_param;
    search_param.original_dsl = "title:apple AND body:fleabag";
    search_param.root = root_clause;
    TSearchFieldBinding title_binding;
    title_binding.field_name = "title";
    title_binding.slot_index = 0;
    TSearchFieldBinding body_binding;
    body_binding.field_name = "body";
    body_binding.slot_index = 1;
    search_param.field_bindings = {title_binding, body_binding};

    InvertedIndexResultBitmap bitmap_result;
    auto status = _function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result, false, nullptr,
            std::unordered_map<std::string, int> {}, _index_query_context);
    ASSERT_TRUE(status.ok()) << status;

    const auto& bitmap = bitmap_result.get_data_bitmap();
    ASSERT_NE(bitmap, nullptr);
    EXPECT_EQ(bitmap->cardinality(), 1);
    EXPECT_TRUE(bitmap->contains(3));
    EXPECT_TRUE(bitmap_result.get_null_bitmap()->isEmpty());
}

TEST_F(FunctionSearchMultiSegmentReadTest, SearchReadsNullBitmapFromMultiSegmentReader) {
    const std::string field = "1";
    const uint32_t num_rows = 4;

    TabletIndex index_meta = create_fulltext_index_meta(103, 1, "title_index");
    const std::string index_path_prefix = prepare_fulltext_index(
            "null_bitmap_rowset", 0, field,
            {"fleabag premiere", "null title", "another null title", "fleabag finale"}, index_meta,
            2, {1, 2});
    assert_multi_segment_reader(index_path_prefix, index_meta);

    auto iterator = create_iterator(index_path_prefix, index_meta, _index_query_context);
    DataTypePtr string_type = std::make_shared<DataTypeString>();
    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types {
            {"title", {field, string_type}}};
    std::unordered_map<std::string, IndexIterator*> iterators {{"title", iterator.get()}};

    TSearchClause root_clause;
    root_clause.clause_type = "TERM";
    root_clause.field_name = "title";
    root_clause.value = "fleabag";
    root_clause.__isset.field_name = true;
    root_clause.__isset.value = true;

    TSearchParam search_param;
    search_param.original_dsl = "title:fleabag";
    search_param.root = root_clause;
    TSearchFieldBinding title_binding;
    title_binding.field_name = "title";
    title_binding.slot_index = 0;
    search_param.field_bindings = {title_binding};

    InvertedIndexResultBitmap bitmap_result;
    auto status = _function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result, false, nullptr,
            std::unordered_map<std::string, int> {}, _index_query_context);
    ASSERT_TRUE(status.ok()) << status;

    const auto& bitmap = bitmap_result.get_data_bitmap();
    ASSERT_NE(bitmap, nullptr);
    EXPECT_EQ(bitmap->cardinality(), 2);
    EXPECT_TRUE(bitmap->contains(0));
    EXPECT_TRUE(bitmap->contains(3));

    const auto& null_bitmap = bitmap_result.get_null_bitmap();
    ASSERT_NE(null_bitmap, nullptr);
    EXPECT_EQ(null_bitmap->cardinality(), 2);
    EXPECT_TRUE(null_bitmap->contains(1));
    EXPECT_TRUE(null_bitmap->contains(2));
}

TEST_F(FunctionSearchMultiSegmentReadTest, SearchDocSetDoesNotUseGlobalNullBitmapInSubReader) {
    const std::string field = "1";
    const uint32_t num_rows = 4;

    TabletIndex index_meta = create_fulltext_index_meta(104, 1, "title_index");
    const std::string index_path_prefix = prepare_fulltext_index(
            "doc_set_null_bitmap_rowset", 0, field,
            {"available title", "available title", "available title", "available title"},
            index_meta, 2, {0});
    assert_multi_segment_reader(index_path_prefix, index_meta);

    auto iterator = create_iterator(index_path_prefix, index_meta, _index_query_context);
    DataTypePtr string_type = std::make_shared<DataTypeString>();
    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types {
            {"title", {field, string_type}}};
    std::unordered_map<std::string, IndexIterator*> iterators {{"title", iterator.get()}};

    TSearchClause missing_clause;
    missing_clause.clause_type = "PREFIX";
    missing_clause.field_name = "title";
    missing_clause.value = "missing";
    missing_clause.__isset.field_name = true;
    missing_clause.__isset.value = true;

    TSearchClause root_clause;
    root_clause.clause_type = "NOT";
    root_clause.children = {missing_clause};
    root_clause.__isset.children = true;

    TSearchParam search_param;
    search_param.original_dsl = "NOT title:missing*";
    search_param.root = root_clause;
    TSearchFieldBinding title_binding;
    title_binding.field_name = "title";
    title_binding.slot_index = 0;
    search_param.field_bindings = {title_binding};

    // Keep this on the non-scoring path, which uses collect_multi_segment_doc_set().
    _index_query_context->collection_similarity.reset();
    _index_query_context->query_limit = 0;

    InvertedIndexResultBitmap bitmap_result;
    auto status = _function_search->evaluate_inverted_index_with_search_param(
            search_param, data_types, iterators, num_rows, bitmap_result, false, nullptr,
            std::unordered_map<std::string, int> {}, _index_query_context);
    ASSERT_TRUE(status.ok()) << status;

    const auto& bitmap = bitmap_result.get_data_bitmap();
    ASSERT_NE(bitmap, nullptr);
    EXPECT_EQ(bitmap->cardinality(), 3);
    EXPECT_TRUE(bitmap->contains(1));
    EXPECT_TRUE(bitmap->contains(2));
    EXPECT_TRUE(bitmap->contains(3));
    EXPECT_FALSE(bitmap->contains(0));

    const auto& null_bitmap = bitmap_result.get_null_bitmap();
    ASSERT_NE(null_bitmap, nullptr);
    EXPECT_EQ(null_bitmap->cardinality(), 1);
    EXPECT_TRUE(null_bitmap->contains(0));
}

TEST_F(FunctionSearchMultiSegmentReadTest, SearchTopKFiltersNullBeforeTruncation) {
    const std::string field = "1";
    const uint32_t num_rows = 4;

    TabletIndex index_meta = create_fulltext_index_meta(104, 1, "title_index");
    const std::string index_path_prefix = prepare_fulltext_index(
            "top_k_null_bitmap_rowset", 0, field,
            {"alpha alpha alpha alpha", "alpha", "alpha alpha", "alpha alpha alpha"}, index_meta, 2,
            {0});
    assert_multi_segment_reader(index_path_prefix, index_meta);

    auto iterator = create_iterator(index_path_prefix, index_meta, _index_query_context);
    DataTypePtr string_type = std::make_shared<DataTypeString>();
    std::unordered_map<std::string, IndexFieldNameAndTypePair> data_types {
            {"title", {field, string_type}}};
    std::unordered_map<std::string, IndexIterator*> iterators {{"title", iterator.get()}};

    TSearchClause alpha_clause;
    alpha_clause.clause_type = "TERM";
    alpha_clause.field_name = "title";
    alpha_clause.value = "alpha";
    alpha_clause.__isset.field_name = true;
    alpha_clause.__isset.value = true;

    TSearchClause missing_clause;
    missing_clause.clause_type = "PREFIX";
    missing_clause.field_name = "title";
    missing_clause.value = "missing";
    missing_clause.__isset.field_name = true;
    missing_clause.__isset.value = true;

    TSearchClause not_missing_clause;
    not_missing_clause.clause_type = "NOT";
    not_missing_clause.children = {missing_clause};
    not_missing_clause.__isset.children = true;

    TSearchClause root_clause;
    root_clause.clause_type = "AND";
    root_clause.children = {alpha_clause, not_missing_clause};
    root_clause.__isset.children = true;

    TSearchParam search_param;
    search_param.original_dsl = "title:alpha AND NOT title:missing*";
    search_param.root = root_clause;
    TSearchFieldBinding title_binding;
    title_binding.field_name = "title";
    title_binding.slot_index = 0;
    search_param.field_bindings = {title_binding};

    _index_query_context->collection_similarity = std::make_shared<CollectionSimilarity>();
    _index_query_context->collection_statistics = std::make_shared<FixedCollectionStatistics>();
    _index_query_context->query_limit = 3;

    const auto evaluate_and_expect = [&] {
        InvertedIndexResultBitmap bitmap_result;
        auto status = _function_search->evaluate_inverted_index_with_search_param(
                search_param, data_types, iterators, num_rows, bitmap_result, false, nullptr,
                std::unordered_map<std::string, int> {}, _index_query_context);
        ASSERT_TRUE(status.ok()) << status;

        const auto& bitmap = bitmap_result.get_data_bitmap();
        ASSERT_NE(bitmap, nullptr);
        EXPECT_EQ(bitmap->cardinality(), 3);
        EXPECT_TRUE(bitmap->contains(1));
        EXPECT_TRUE(bitmap->contains(2));
        EXPECT_TRUE(bitmap->contains(3));
        EXPECT_FALSE(bitmap->contains(0));

        const auto& null_bitmap = bitmap_result.get_null_bitmap();
        ASSERT_NE(null_bitmap, nullptr);
        EXPECT_EQ(null_bitmap->cardinality(), 1);
        EXPECT_TRUE(null_bitmap->contains(0));
    };

    evaluate_and_expect();

    TQueryOptions query_options;
    query_options.enable_inverted_index_searcher_cache = false;
    query_options.enable_inverted_index_query_cache = false;
    query_options.enable_inverted_index_wand_query = true;
    _runtime_state.set_query_options(query_options);
    _index_query_context->collection_similarity = std::make_shared<CollectionSimilarity>();
    evaluate_and_expect();
}

} // namespace doris
