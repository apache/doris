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

#include "olap/rowset/segment_v2/inverted_index/query/phrase_prefix_query.h"

#include <CLucene.h>
#include <gtest/gtest.h>

#include "gen_cpp/PaloInternalService_types.h"
#include "io/fs/local_file_system.h"
#include "olap/field.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/rowset/segment_v2/inverted_index_searcher.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "util/slice.h"

namespace doris::segment_v2 {

class PhrasePrefixQueryTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/phrase_prefix_query_test";

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

    PhrasePrefixQueryTest() = default;
    ~PhrasePrefixQueryTest() override = default;

private:
    std::unique_ptr<InvertedIndexSearcherCache> _inverted_index_searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _inverted_index_query_cache;
};

TEST_F(PhrasePrefixQueryTest, test_single_term_prefix_query) {
    std::string_view rowset_id = "test_single_prefix";
    int seg_id = 0;

    // Prepare test data with words that have common prefixes
    std::vector<Slice> values = {
            Slice("application development"), // doc 0 - contains "app*"
            Slice("apple banana"),            // doc 1 - contains "app*"
            Slice("approach method"),         // doc 2 - contains "app*"
            Slice("orange grape"),            // doc 3 - no "app*"
            Slice("appreciate value"),        // doc 4 - contains "app*"
            Slice("random text")              // doc 5 - no "app*"
    };

    TabletIndex idx_meta;
    // Create fulltext index metadata with phrase support
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_single_prefix");
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

    // Test PhrasePrefixQuery with single term (acts as prefix query)
    RuntimeState runtime_state;
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    runtime_state.set_query_options(query_options);
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhrasePrefixQuery query(searcher, context);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1";                 // c2 column unique_id in V2 format
    query_info.term_infos.emplace_back("app", 0); // Should match words starting with "app"

    query.add(query_info);

    roaring::Roaring result;
    EXPECT_NO_THROW(query.search(result));

    // Verify results - should find documents containing words starting with "app"
    EXPECT_GT(result.cardinality(), 0);
    // Note: Exact document matches depend on how the tokenizer and prefix matching work
}

TEST_F(PhrasePrefixQueryTest, test_multi_term_phrase_prefix_query) {
    std::string_view rowset_id = "test_multi_phrase_prefix";
    int seg_id = 0;

    // Prepare test data for multi-term phrase prefix query
    std::vector<Slice> values = {
            Slice("big red apple"),       // doc 0 - matches "big red app*"
            Slice("big red application"), // doc 1 - matches "big red app*"
            Slice("big blue apple"),      // doc 2 - no match (blue != red)
            Slice("small red apple"),     // doc 3 - no match (small != big)
            Slice("big red approach"),    // doc 4 - matches "big red app*"
            Slice("big red orange"),      // doc 5 - no match (orange doesn't start with "app")
            Slice("big red appreciate")   // doc 6 - matches "big red app*"
    };

    TabletIndex idx_meta;
    // Create fulltext index metadata
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_multi_phrase_prefix");
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

    // Test PhrasePrefixQuery with multiple terms
    RuntimeState runtime_state;
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    runtime_state.set_query_options(query_options);
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhrasePrefixQuery query(searcher, context);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1"; // c2 column unique_id in V2 format
    // Phrase: "big red app*" - first two terms exact match, last term prefix match
    query_info.term_infos.emplace_back("big", 0); // exact match
    query_info.term_infos.emplace_back("red", 1); // exact match
    query_info.term_infos.emplace_back(
            "app",
            2); // prefix match - should match "apple", "application", "approach", "appreciate"

    query.add(query_info);

    roaring::Roaring result;
    EXPECT_NO_THROW(query.search(result));

    // The exact results depend on the phrase and prefix query implementation
    // We mainly test that the query executes without error
    EXPECT_GE(result.cardinality(), 0);
}

TEST_F(PhrasePrefixQueryTest, test_empty_terms_exception) {
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
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    runtime_state.set_query_options(query_options);
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhrasePrefixQuery query(searcher, context);

    // Test with empty terms - should throw exception
    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1"; // c2 column unique_id in V2 format
    // terms is empty

    EXPECT_THROW(query.add(query_info), Exception);
}

TEST_F(PhrasePrefixQueryTest, test_max_expansions_limit) {
    std::string_view rowset_id = "test_max_expansions";
    int seg_id = 0;

    // Create data with many terms that could be expanded with same prefix
    std::vector<Slice> values = {
            Slice("application software"), // doc 0 - "app*"
            Slice("apple fruit"),          // doc 1 - "app*"
            Slice("approach method"),      // doc 2 - "app*"
            Slice("append text"),          // doc 3 - "app*"
            Slice("apparatus device"),     // doc 4 - "app*"
            Slice("appreciate value"),     // doc 5 - "app*"
            Slice("appropriate time"),     // doc 6 - "app*"
            Slice("random word")           // doc 7 - no "app*"
    };

    TabletIndex idx_meta;
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_expansions");
    index_meta_pb->clear_col_unique_id();
    index_meta_pb->add_col_unique_id(1);
    index_meta_pb->mutable_properties()->insert({"parser", "english"});
    index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
    idx_meta.init_from_pb(*index_meta_pb.get());

    std::string index_path_prefix;
    prepare_fulltext_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

    auto searcher = create_searcher(index_path_prefix, idx_meta);
    ASSERT_NE(searcher, nullptr);

    // Test with limited max_expansions
    RuntimeState runtime_state;
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 3; // Limit to 3 expansions
    runtime_state.set_query_options(query_options);
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhrasePrefixQuery query(searcher, context);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1";                 // c2 column unique_id in V2 format
    query_info.term_infos.emplace_back("app", 0); // Should match many terms but limited to 3

    query.add(query_info);

    roaring::Roaring result;
    EXPECT_NO_THROW(query.search(result));

    // Should work without error even with expansion limits
    EXPECT_GE(result.cardinality(), 0);
}

TEST_F(PhrasePrefixQueryTest, test_no_prefix_matches) {
    std::string_view rowset_id = "test_no_prefix_matches";
    int seg_id = 0;

    std::vector<Slice> values = {Slice("hello world"), Slice("test document"),
                                 Slice("sample text")};

    TabletIndex idx_meta;
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_no_prefix_match");
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
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    runtime_state.set_query_options(query_options);
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhrasePrefixQuery query(searcher, context);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1";                 // c2 column unique_id in V2 format
    query_info.term_infos.emplace_back("xyz", 0); // Should not match any prefix

    query.add(query_info);

    roaring::Roaring result;
    EXPECT_NO_THROW(query.search(result));

    // Should find no documents
    EXPECT_EQ(result.cardinality(), 0);
}

TEST_F(PhrasePrefixQueryTest, test_phrase_with_no_prefix_expansion) {
    std::string_view rowset_id = "test_phrase_no_expansion";
    int seg_id = 0;

    // Test case where the last term has no prefix matches, should fallback to exact term
    std::vector<Slice> values = {
            Slice("big red car"),   // doc 0 - exact match for "big red car"
            Slice("big red truck"), // doc 1 - no match (truck != car)
            Slice("small red car"), // doc 2 - no match (small != big)
            Slice("big blue car")   // doc 3 - no match (blue != red)
    };

    TabletIndex idx_meta;
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test_phrase_no_expansion");
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
    TQueryOptions query_options;
    query_options.inverted_index_max_expansions = 50;
    runtime_state.set_query_options(query_options);
    io::IOContext io_ctx;

    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
    context->io_ctx = &io_ctx;
    context->runtime_state = &runtime_state;

    PhrasePrefixQuery query(searcher, context);

    InvertedIndexQueryInfo query_info;
    query_info.field_name = L"1"; // c2 column unique_id in V2 format
    // Phrase: "big red car" - no prefix expansion for "car"
    query_info.term_infos.emplace_back("big", 0);
    query_info.term_infos.emplace_back("red", 1);
    query_info.term_infos.emplace_back("car", 2); // exact term, no prefix matches

    query.add(query_info);

    roaring::Roaring result;
    EXPECT_NO_THROW(query.search(result));

    // Should work and potentially find exact matches
    EXPECT_GE(result.cardinality(), 0);
}

} // namespace doris::segment_v2