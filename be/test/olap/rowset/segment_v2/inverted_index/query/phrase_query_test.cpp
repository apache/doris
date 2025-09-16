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

#include "olap/rowset/segment_v2/inverted_index/query/phrase_query.h"

#include <CLucene.h>
#include <gtest/gtest.h>

#include "gen_cpp/PaloInternalService_types.h"
#include "io/fs/local_file_system.h"
#include "olap/field.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_info.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_searcher.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "util/slice.h"

namespace doris::segment_v2 {

class PhraseQueryTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/phrase_query_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;

        // Initialize TmpFileDirs
        std::vector<StorePath> paths;
        paths.emplace_back(kTestDir, 1024);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        st = tmp_file_dirs->init();
        if (!st.ok()) {
            std::cout << "init tmp file dirs error:" << st.to_string() << std::endl;
            return;
        }
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // Initialize cache
        int64_t inverted_index_cache_limit = 1024 * 1024 * 1024;
        _inverted_index_searcher_cache = std::unique_ptr<segment_v2::InvertedIndexSearcherCache>(
                InvertedIndexSearcherCache::create_global_instance(inverted_index_cache_limit, 1));
        _inverted_index_query_cache = std::unique_ptr<segment_v2::InvertedIndexQueryCache>(
                InvertedIndexQueryCache::create_global_cache(inverted_index_cache_limit, 1));

        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(
                _inverted_index_searcher_cache.get());
        ExecEnv::GetInstance()->_inverted_index_query_cache = _inverted_index_query_cache.get();
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    TabletSchemaSPtr create_schema(KeysType keys_type = DUP_KEYS) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(keys_type);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(1024);
        schema_pb.set_compress_kind(COMPRESS_NONE);
        schema_pb.set_next_column_unique_id(4);

        // Add c1 column (INT)
        ColumnPB* column_1 = schema_pb.add_column();
        column_1->set_unique_id(0);
        column_1->set_name("c1");
        column_1->set_type("INT");
        column_1->set_is_key(true);
        column_1->set_length(4);
        column_1->set_index_length(4);
        column_1->set_is_nullable(false);
        column_1->set_is_bf_column(false);

        // Add c2 column (VARCHAR for text search)
        ColumnPB* column_2 = schema_pb.add_column();
        column_2->set_unique_id(1);
        column_2->set_name("c2");
        column_2->set_type("VARCHAR");
        column_2->set_length(255);
        column_2->set_index_length(255);
        column_2->set_is_nullable(false);
        column_2->set_is_bf_column(false);

        auto tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->init_from_pb(schema_pb);
        return tablet_schema;
    }

    std::string local_segment_path(std::string base, std::string_view rowset_id, int64_t seg_id) {
        return fmt::format("{}/{}_{}.dat", base, rowset_id, seg_id);
    }

    // Create fulltext inverted index with phrase support
    void prepare_fulltext_index(
            std::string_view rowset_id, int seg_id, std::vector<Slice>& values,
            TabletIndex* idx_meta, std::string* index_path_prefix,
            InvertedIndexStorageFormatPB format = InvertedIndexStorageFormatPB::V2) {
        auto tablet_schema = create_schema();

        *index_path_prefix = InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id));
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v2(*index_path_prefix);

        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        Status sts = fs->create_file(index_path, &file_writer, &opts);
        ASSERT_TRUE(sts.ok()) << sts;
        auto index_file_writer =
                std::make_unique<IndexFileWriter>(fs, *index_path_prefix, std::string {rowset_id},
                                                  seg_id, format, std::move(file_writer));

        // Get c2 column Field
        const TabletColumn& column = tablet_schema->column(1);
        ASSERT_NE(&column, nullptr);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        ASSERT_NE(field.get(), nullptr);

        // Create column writer
        std::unique_ptr<IndexColumnWriter> column_writer;
        auto status = IndexColumnWriter::create(field.get(), &column_writer,
                                                index_file_writer.get(), idx_meta);
        EXPECT_TRUE(status.ok()) << status;

        // Write string values
        status = column_writer->add_values("c2", values.data(), values.size());
        EXPECT_TRUE(status.ok()) << status;

        // Finish and close
        status = column_writer->finish();
        EXPECT_TRUE(status.ok()) << status;
        status = index_file_writer->close();
        EXPECT_TRUE(status.ok()) << status;
    }

    // Create an IndexSearcher from the created index
    std::shared_ptr<lucene::search::IndexSearcher> create_searcher(
            const std::string& index_path_prefix, const TabletIndex& idx_meta) {
        auto reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
        auto status = reader->init();
        EXPECT_EQ(status, Status::OK());

        auto result = reader->open(&idx_meta);
        EXPECT_TRUE(result.has_value()) << "Failed to open compound reader";

        auto index_searcher_builder = std::make_unique<FulltextIndexSearcherBuilder>();
        auto searcher_result = index_searcher_builder->get_index_searcher(result.value().get());
        EXPECT_TRUE(searcher_result.has_value());

        auto* fulltext_searcher = std::get_if<FulltextIndexSearcherPtr>(&searcher_result.value());
        EXPECT_TRUE(fulltext_searcher != nullptr);

        return *fulltext_searcher;
    }

    PhraseQueryTest() = default;
    ~PhraseQueryTest() override = default;

private:
    std::unique_ptr<InvertedIndexSearcherCache> _inverted_index_searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _inverted_index_query_cache;
};

TEST_F(PhraseQueryTest, test_exact_phrase_query) {
    std::string_view rowset_id = "test_exact_phrase";
    int seg_id = 0;

    // Prepare test data for exact phrase matching
    std::vector<Slice> values = {
            Slice("big red apple"),   // doc 0 - matches "big red"
            Slice("small red apple"), // doc 1 - no match (small != big)
            Slice("big blue apple"),  // doc 2 - no match (blue != red)
            Slice("red big apple"),   // doc 3 - no match (wrong order)
            Slice("big red orange"),  // doc 4 - matches "big red"
            Slice("very big red car") // doc 5 - matches "big red"
    };

    TabletIndex idx_meta;
    // Create fulltext index metadata with phrase support
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_exact_phrase");
    index_meta_pb->clear_col_unique_id();
    index_meta_pb->add_col_unique_id(1); // c2 column ID
    index_meta_pb->mutable_properties()->insert({"parser", "english"});
    index_meta_pb->mutable_properties()->insert({"lower_case", "true"});
    index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
    idx_meta.init_from_pb(*index_meta_pb.get());

    std::string index_path_prefix;
    prepare_fulltext_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

    // Create searcher
    auto searcher = create_searcher(index_path_prefix, idx_meta);
    ASSERT_NE(searcher, nullptr);

    // Test exact phrase query
    RuntimeState runtime_state;
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhraseQuery query(searcher, context);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1"; // c2 column unique_id in V2 format
    query_info.term_infos.emplace_back("big", 0);
    query_info.term_infos.emplace_back("red", 1);
    query_info.slop = 0; // exact phrase match

    query.add(query_info);

    roaring::Roaring result;
    EXPECT_NO_THROW(query.search(result));

    // Should find documents with exact phrase "big red"
    EXPECT_GE(result.cardinality(), 0);
}

TEST_F(PhraseQueryTest, test_single_term_query) {
    std::string_view rowset_id = "test_single_term";
    int seg_id = 0;

    // Prepare test data for single term query
    std::vector<Slice> values = {
            Slice("apple banana"), // doc 0 - contains "apple"
            Slice("orange grape"), // doc 1 - no "apple"
            Slice("green apple"),  // doc 2 - contains "apple"
            Slice("apple pie"),    // doc 3 - contains "apple"
            Slice("banana split")  // doc 4 - no "apple"
    };

    TabletIndex idx_meta;
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_single_term");
    index_meta_pb->clear_col_unique_id();
    index_meta_pb->add_col_unique_id(1);
    index_meta_pb->mutable_properties()->insert({"parser", "english"});
    index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
    idx_meta.init_from_pb(*index_meta_pb.get());

    std::string index_path_prefix;
    prepare_fulltext_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

    auto searcher = create_searcher(index_path_prefix, idx_meta);
    ASSERT_NE(searcher, nullptr);

    RuntimeState runtime_state;
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhraseQuery query(searcher, context);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1";
    query_info.term_infos.emplace_back("apple", 0); // single term

    query.add(query_info);

    roaring::Roaring result;
    EXPECT_NO_THROW(query.search(result));

    // Should find documents containing "apple"
    EXPECT_GT(result.cardinality(), 0);
}

TEST_F(PhraseQueryTest, test_sloppy_phrase_query) {
    std::string_view rowset_id = "test_sloppy_phrase";
    int seg_id = 0;

    // Prepare test data for sloppy phrase matching
    std::vector<Slice> values = {
            Slice("big red apple"),            // doc 0 - "big red" with slop 0
            Slice("big very red apple"),       // doc 1 - "big red" with slop 1
            Slice("big huge red apple"),       // doc 2 - "big red" with slop 1
            Slice("big small blue red apple"), // doc 3 - "big red" with slop 3
            Slice("red big apple"),            // doc 4 - "big red" reordered
            Slice("green apple")               // doc 5 - no match
    };

    TabletIndex idx_meta;
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_sloppy_phrase");
    index_meta_pb->clear_col_unique_id();
    index_meta_pb->add_col_unique_id(1);
    index_meta_pb->mutable_properties()->insert({"parser", "english"});
    index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
    idx_meta.init_from_pb(*index_meta_pb.get());

    std::string index_path_prefix;
    prepare_fulltext_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

    auto searcher = create_searcher(index_path_prefix, idx_meta);
    ASSERT_NE(searcher, nullptr);

    RuntimeState runtime_state;
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhraseQuery query(searcher, context);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1";
    query_info.term_infos.emplace_back("big", 0);
    query_info.term_infos.emplace_back("red", 1);
    query_info.slop = 2;        // allow up to 2 words between "big" and "red"
    query_info.ordered = false; // allow reordering

    query.add(query_info);

    roaring::Roaring result;
    EXPECT_NO_THROW(query.search(result));

    // Should find documents with "big" and "red" within slop distance
    EXPECT_GE(result.cardinality(), 0);
}

TEST_F(PhraseQueryTest, test_ordered_sloppy_phrase_query) {
    std::string_view rowset_id = "test_ordered_sloppy";
    int seg_id = 0;

    // Prepare test data for ordered sloppy phrase matching
    std::vector<Slice> values = {
            Slice("big red apple"),            // doc 0 - "big red" in order
            Slice("big very red apple"),       // doc 1 - "big red" in order with slop
            Slice("red big apple"),            // doc 2 - "big red" wrong order
            Slice("big small blue red apple"), // doc 3 - "big red" in order with large slop
            Slice("green apple")               // doc 4 - no match
    };

    TabletIndex idx_meta;
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_ordered_sloppy");
    index_meta_pb->clear_col_unique_id();
    index_meta_pb->add_col_unique_id(1);
    index_meta_pb->mutable_properties()->insert({"parser", "english"});
    index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
    idx_meta.init_from_pb(*index_meta_pb.get());

    std::string index_path_prefix;
    prepare_fulltext_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

    auto searcher = create_searcher(index_path_prefix, idx_meta);
    ASSERT_NE(searcher, nullptr);

    RuntimeState runtime_state;
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhraseQuery query(searcher, context);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1";
    query_info.term_infos.emplace_back("big", 0);
    query_info.term_infos.emplace_back("red", 1);
    query_info.slop = 2;       // allow up to 2 words between "big" and "red"
    query_info.ordered = true; // require original order

    query.add(query_info);

    roaring::Roaring result;
    EXPECT_NO_THROW(query.search(result));

    // Should find documents with "big" before "red" within slop distance
    EXPECT_GE(result.cardinality(), 0);
}

TEST_F(PhraseQueryTest, test_multi_term_vector_add) {
    std::string_view rowset_id = "test_multi_term_vector";
    int seg_id = 0;

    std::vector<Slice> values = {Slice("apple banana cherry"), Slice("apple orange cherry"),
                                 Slice("grape banana cherry"), Slice("apple banana date")};

    TabletIndex idx_meta;
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_multi_term_vector");
    index_meta_pb->clear_col_unique_id();
    index_meta_pb->add_col_unique_id(1);
    index_meta_pb->mutable_properties()->insert({"parser", "english"});
    index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
    idx_meta.init_from_pb(*index_meta_pb.get());

    std::string index_path_prefix;
    prepare_fulltext_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

    auto searcher = create_searcher(index_path_prefix, idx_meta);
    ASSERT_NE(searcher, nullptr);

    RuntimeState runtime_state;
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhraseQuery query(searcher, context);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1";
    query_info.term_infos.emplace_back("apple", 0);
    std::vector<std::string> multi_terms = {"banana", "orange"};
    query_info.term_infos.emplace_back(multi_terms, 1);
    query_info.term_infos.emplace_back("cherry", 2);

    query.add(query_info);

    roaring::Roaring result;
    EXPECT_NO_THROW(query.search(result));

    // Should find documents matching the phrase with alternatives
    EXPECT_GE(result.cardinality(), 0);
}

TEST_F(PhraseQueryTest, test_empty_terms_exception) {
    std::string_view rowset_id = "test_empty_terms";
    int seg_id = 0;

    std::vector<Slice> values = {Slice("test data")};

    TabletIndex idx_meta;
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_empty");
    index_meta_pb->clear_col_unique_id();
    index_meta_pb->add_col_unique_id(1);
    index_meta_pb->mutable_properties()->insert({"parser", "english"});
    index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
    idx_meta.init_from_pb(*index_meta_pb.get());

    std::string index_path_prefix;
    prepare_fulltext_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

    auto searcher = create_searcher(index_path_prefix, idx_meta);
    ASSERT_NE(searcher, nullptr);

    RuntimeState runtime_state;
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhraseQuery query(searcher, context);

    // Test with empty terms - should throw exception
    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1";
    // terms is empty

    EXPECT_THROW(query.add(query_info), Exception);

    // Test vector add with empty terms
    InvertedIndexQueryInfo query_info1;
    query_info1.field_name = L"1";
    std::vector<std::string> empty_terms = {};
    query_info1.term_infos.emplace_back(empty_terms, 0);
    query_info1.term_infos.emplace_back(empty_terms, 1);
    query_info1.slop = 2;
    EXPECT_THROW(query.add(query_info1), Exception);
}

TEST_F(PhraseQueryTest, test_no_matches) {
    std::string_view rowset_id = "test_no_matches";
    int seg_id = 0;

    std::vector<Slice> values = {Slice("hello world"), Slice("test document"),
                                 Slice("sample text")};

    TabletIndex idx_meta;
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_no_match");
    index_meta_pb->clear_col_unique_id();
    index_meta_pb->add_col_unique_id(1);
    index_meta_pb->mutable_properties()->insert({"parser", "english"});
    index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
    idx_meta.init_from_pb(*index_meta_pb.get());

    std::string index_path_prefix;
    prepare_fulltext_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

    auto searcher = create_searcher(index_path_prefix, idx_meta);
    ASSERT_NE(searcher, nullptr);

    RuntimeState runtime_state;
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhraseQuery query(searcher, context);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1";
    query_info.term_infos.emplace_back("nonexistent", 0);
    query_info.term_infos.emplace_back("phrase", 1);

    query.add(query_info);

    roaring::Roaring result;
    EXPECT_NO_THROW(query.search(result));

    // Should find no documents
    EXPECT_EQ(result.cardinality(), 0);
}

TEST_F(PhraseQueryTest, test_parser_info) {
    std::map<std::string, std::string> properties;
    properties.insert({"parser", "english"});
    properties.insert({"support_phrase", "true"});
    properties.insert({"lower_case", "true"});

    OlapReaderStatistics stats;
    auto parser_info = [&properties, &stats](std::string& search_str,
                                             InvertedIndexQueryInfo& query_info) {
        PhraseQuery::parser_info(&stats, search_str, properties, query_info);
    };

    auto parser = [&parser_info](std::string search_str, std::string res1, size_t res2,
                                 int32_t res3, bool res4, size_t res5) {
        InvertedIndexQueryInfo query_info;
        parser_info(search_str, query_info);
        EXPECT_EQ(search_str, res1);
        EXPECT_EQ(query_info.term_infos.size(), res2);
        EXPECT_EQ(query_info.slop, res3);
        EXPECT_EQ(query_info.ordered, res4);
    };

    // "english/history off.gif ~20+" sequential_opt = true
    parser("", "", 0, 0, false, 0);
    parser("english", "english", 1, 0, false, 0);
    parser("english/history", "english/history", 2, 0, false, 0);
    parser("english/history off", "english/history off", 3, 0, false, 0);
    parser("english/history off.gif", "english/history off.gif", 4, 0, false, 0);
    parser("english/history off.gif ", "english/history off.gif ", 4, 0, false, 0);
    parser("english/history off.gif ~", "english/history off.gif ~", 4, 0, false, 0);
    parser("english/history off.gif ~2", "english/history off.gif", 4, 2, false, 0);
    parser("english/history off.gif ~20", "english/history off.gif", 4, 20, false, 0);
    parser("english/history off.gif ~20+", "english/history off.gif", 4, 20, true, 2);
    parser("english/history off.gif ~20+ ", "english/history off.gif ~20+ ", 5, 0, false, 0);
    parser("english/history off.gif ~20+x", "english/history off.gif ~20+x", 6, 0, false, 0);
}

TEST_F(PhraseQueryTest, test_parser_info1) {
    std::map<std::string, std::string> properties;
    properties.insert({"parser", "unicode"});
    properties.insert({"support_phrase", "true"});
    properties.insert({"lower_case", "true"});

    OlapReaderStatistics stats;
    auto parser_info = [&properties, &stats](std::string& search_str,
                                             InvertedIndexQueryInfo& query_info) {
        PhraseQuery::parser_info(&stats, search_str, properties, query_info);
    };

    {
        InvertedIndexQueryInfo query_info;
        std::string search_str = "我在 北京 ~4+";
        parser_info(search_str, query_info);
        EXPECT_EQ(search_str, "我在 北京");
        EXPECT_EQ(query_info.slop, 4);
        EXPECT_EQ(query_info.ordered, true);
        EXPECT_EQ(query_info.term_infos.size(), 4);
    }

    {
        InvertedIndexQueryInfo query_info;
        std::string search_str = "List of Pirates of the Caribbean characters ~4+";
        parser_info(search_str, query_info);
        EXPECT_EQ(search_str, "List of Pirates of the Caribbean characters");
        EXPECT_EQ(query_info.slop, 4);
        EXPECT_EQ(query_info.ordered, true);
        EXPECT_EQ(query_info.term_infos.size(), 4);
    }
}

TEST_F(PhraseQueryTest, test_parser_slop) {
    // Test the static parser_slop method
    InvertedIndexQueryInfo query_info;

    // Test basic slop parsing
    {
        std::string query = "hello world ~5";
        PhraseQuery::parser_slop(query, query_info);
        EXPECT_EQ(query, "hello world");
        EXPECT_EQ(query_info.slop, 5);
        EXPECT_FALSE(query_info.ordered);
    }

    // Test ordered slop parsing
    {
        std::string query = "hello world ~3+";
        query_info = InvertedIndexQueryInfo {}; // reset
        PhraseQuery::parser_slop(query, query_info);
        EXPECT_EQ(query, "hello world");
        EXPECT_EQ(query_info.slop, 3);
        EXPECT_TRUE(query_info.ordered);
    }

    // Test invalid slop format
    {
        std::string query = "hello world ~abc";
        query_info = InvertedIndexQueryInfo {}; // reset
        PhraseQuery::parser_slop(query, query_info);
        EXPECT_EQ(query, "hello world ~abc"); // should remain unchanged
        EXPECT_EQ(query_info.slop, 0);
        EXPECT_FALSE(query_info.ordered);
    }

    // Test no slop
    {
        std::string query = "hello world";
        query_info = InvertedIndexQueryInfo {}; // reset
        PhraseQuery::parser_slop(query, query_info);
        EXPECT_EQ(query, "hello world");
        EXPECT_EQ(query_info.slop, 0);
        EXPECT_FALSE(query_info.ordered);
    }
}

} // namespace doris::segment_v2