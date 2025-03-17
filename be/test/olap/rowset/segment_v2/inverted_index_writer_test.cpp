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

#include "olap/rowset/segment_v2/inverted_index_writer.h"

#include <CLucene.h>
#include <CLucene/config/repl_wchar.h>
#include <CLucene/index/IndexReader.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/field.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "runtime/runtime_state.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/olap/olap_data_convertor.h"

using namespace lucene::index;
using doris::segment_v2::InvertedIndexFileWriter;

namespace doris::segment_v2 {

// Define InvertedIndexDirectoryMap
using InvertedIndexDirectoryMap =
        std::map<std::pair<int64_t, std::string>, std::shared_ptr<lucene::store::Directory>>;

class InvertedIndexWriterTest : public testing::Test {
    using ExpectedDocMap = std::map<std::string, std::vector<int>>;

public:
    const std::string kTestDir = "./ut_dir/inverted_index_writer_test";

    void check_terms_stats(std::string index_prefix, ExpectedDocMap* expected,
                           std::vector<int> expected_null_bitmap = {},
                           InvertedIndexStorageFormatPB format = InvertedIndexStorageFormatPB::V1,
                           const TabletIndex* index_meta = nullptr) {
        std::string file_str;
        if (format == InvertedIndexStorageFormatPB::V1) {
            file_str = InvertedIndexDescriptor::get_index_file_path_v1(index_prefix,
                                                                       index_meta->index_id(), "");
        } else if (format == InvertedIndexStorageFormatPB::V2) {
            file_str = InvertedIndexDescriptor::get_index_file_path_v2(index_prefix);
        }
        std::unique_ptr<InvertedIndexFileReader> reader = std::make_unique<InvertedIndexFileReader>(
                io::global_local_filesystem(), index_prefix, format);
        auto st = reader->init();
        EXPECT_EQ(st, Status::OK());
        auto result = reader->open(index_meta);
        EXPECT_TRUE(result.has_value()) << "Failed to open compound reader" << result.error();
        auto compound_reader = std::move(result.value());
        try {
            CLuceneError err;
            CL_NS(store)::IndexInput* index_input = nullptr;
            auto ok = DorisFSDirectory::FSIndexInput::open(
                    io::global_local_filesystem(), file_str.c_str(), index_input, err, 4096);
            if (!ok) {
                throw err;
            }

            std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
            const char* null_bitmap_file_name =
                    InvertedIndexDescriptor::get_temporary_null_bitmap_file_name();
            if (compound_reader->fileExists(null_bitmap_file_name)) {
                std::unique_ptr<lucene::store::IndexInput> null_bitmap_in;
                assert(compound_reader->openInput(null_bitmap_file_name, null_bitmap_in, err,
                                                  4096));
                size_t null_bitmap_size = null_bitmap_in->length();
                doris::faststring buf;
                buf.resize(null_bitmap_size);
                null_bitmap_in->readBytes(reinterpret_cast<uint8_t*>(buf.data()), null_bitmap_size);
                *null_bitmap = roaring::Roaring::read(reinterpret_cast<char*>(buf.data()), false);
                EXPECT_TRUE(expected_null_bitmap.size() == null_bitmap->cardinality());
                for (int i : expected_null_bitmap) {
                    EXPECT_TRUE(null_bitmap->contains(i));
                }
            }
            index_input->close();
            _CLLDELETE(index_input);
        } catch (const CLuceneError& e) {
            EXPECT_TRUE(false) << "CLuceneError: " << e.what();
        }

        std::cout << "Term statistics for " << file_str << std::endl;
        std::cout << "==================================" << std::endl;
        lucene::store::Directory* dir = compound_reader.get();

        IndexReader* r = IndexReader::open(dir);

        printf("Max Docs: %d\n", r->maxDoc());
        printf("Num Docs: %d\n", r->numDocs());

        TermEnum* te = r->terms();
        int32_t nterms;
        for (nterms = 0; te->next(); nterms++) {
            /* empty */
            std::string token =
                    lucene_wcstoutf8string(te->term(false)->text(), te->term(false)->textLength());

            printf("Term: %s ", token.c_str());
            if (expected) {
                auto it = expected->find(token);
                if (it != expected->end()) {
                    TermDocs* td = r->termDocs(te->term(false));
                    std::vector<int> actual_docs;
                    while (td->next()) {
                        actual_docs.push_back(td->doc());
                    }
                    td->close();
                    _CLLDELETE(td);
                    EXPECT_EQ(actual_docs, it->second) << "Term: " << token;
                }
            }
            printf("Freq: %d\n", te->docFreq());
        }
        printf("Term count: %d\n\n", nterms);

        te->close();
        _CLLDELETE(te);
        r->close();
        _CLLDELETE(r);
    }

    void check_bkd_index(std::string index_prefix, const TabletIndex* index_meta,
                         const std::vector<int32_t>& values, const std::vector<int>& doc_ids) {
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_query = false;
        runtime_state.set_query_options(query_options);
        // Create a BkdIndexReader to verify the index
        auto reader = std::make_shared<InvertedIndexFileReader>(
                io::global_local_filesystem(), index_prefix, InvertedIndexStorageFormatPB::V2);
        auto st = reader->init();
        EXPECT_EQ(st, Status::OK());
        auto result = reader->open(index_meta);
        EXPECT_TRUE(result.has_value()) << "Failed to open compound reader" << result.error();

        auto bkd_reader = doris::segment_v2::BkdIndexReader::create_shared(index_meta, reader);
        EXPECT_NE(bkd_reader, nullptr);

        // Test EQUAL query for each value
        for (size_t i = 0; i < values.size(); i++) {
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status = bkd_reader->query(nullptr, &stats, &runtime_state, "c1", &values[i],
                                            doris::segment_v2::InvertedIndexQueryType::EQUAL_QUERY,
                                            bitmap);
            EXPECT_TRUE(status.ok()) << status;

            // Verify the result contains the expected document ID
            EXPECT_TRUE(bitmap->contains(doc_ids[i]))
                    << "Value " << values[i] << " should match document " << doc_ids[i];

            // For duplicate values, verify all matching documents are found
            for (size_t j = 0; j < values.size(); j++) {
                if (values[j] == values[i]) {
                    EXPECT_TRUE(bitmap->contains(doc_ids[j]))
                            << "Value " << values[i] << " should match document " << doc_ids[j];
                }
            }
        }

        // Test range queries
        // Test LESS_THAN query
        std::shared_ptr<roaring::Roaring> less_than_bitmap = std::make_shared<roaring::Roaring>();
        int32_t test_value = 200;
        auto status = bkd_reader->query(nullptr, &stats, &runtime_state, "c1", &test_value,
                                        doris::segment_v2::InvertedIndexQueryType::LESS_THAN_QUERY,
                                        less_than_bitmap);
        EXPECT_TRUE(status.ok()) << status;

        // Verify documents with values less than test_value are in the result
        for (size_t i = 0; i < values.size(); i++) {
            if (values[i] < test_value) {
                EXPECT_TRUE(less_than_bitmap->contains(doc_ids[i]))
                        << "Value " << values[i] << " should be less than " << test_value;
            } else {
                EXPECT_FALSE(less_than_bitmap->contains(doc_ids[i]))
                        << "Value " << values[i] << " should not be less than " << test_value;
            }
        }

        // Test GREATER_THAN query
        std::shared_ptr<roaring::Roaring> greater_than_bitmap =
                std::make_shared<roaring::Roaring>();
        status = bkd_reader->query(nullptr, &stats, &runtime_state, "c1", &test_value,
                                   doris::segment_v2::InvertedIndexQueryType::GREATER_THAN_QUERY,
                                   greater_than_bitmap);
        EXPECT_TRUE(status.ok()) << status;

        // Verify documents with values greater than test_value are in the result
        for (size_t i = 0; i < values.size(); i++) {
            if (values[i] > test_value) {
                EXPECT_TRUE(greater_than_bitmap->contains(doc_ids[i]))
                        << "Value " << values[i] << " should be greater than " << test_value;
            } else {
                EXPECT_FALSE(greater_than_bitmap->contains(doc_ids[i]))
                        << "Value " << values[i] << " should not be greater than " << test_value;
            }
        }
    }

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        std::vector<StorePath> paths;
        paths.emplace_back(kTestDir, 1024);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        st = tmp_file_dirs->init();
        if (!st.ok()) {
            std::cout << "init tmp file dirs error:" << st.to_string() << std::endl;
            return;
        }
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // use memory limit
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
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
    }

    TabletSchemaSPtr create_schema(KeysType keys_type = DUP_KEYS) {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(keys_type);
        tablet_schema_pb.set_num_short_key_columns(1);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(4);

        tablet_schema->init_from_pb(tablet_schema_pb);

        // Add key column (INT)
        TabletColumn column_1;
        column_1.set_name("c1");
        column_1.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        column_1.set_length(4);
        column_1.set_index_length(4);
        column_1.set_is_key(true);
        column_1.set_is_nullable(true);
        tablet_schema->append_column(column_1);

        // Add value column (VARCHAR)
        TabletColumn column_2;
        column_2.set_name("c2");
        column_2.set_type(FieldType::OLAP_FIELD_TYPE_VARCHAR);
        column_2.set_length(65535);
        column_2.set_is_key(false);
        column_2.set_is_nullable(false);
        tablet_schema->append_column(column_2);

        return tablet_schema;
    }

    std::string local_segment_path(std::string base, std::string_view rowset_id, int64_t seg_id) {
        return fmt::format("{}/{}_{}.dat", base, rowset_id, seg_id);
    }

    void test_string_write(std::string_view rowset_id, int seg_id) {
        auto tablet_schema = create_schema();

        // Create index meta
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column id

        TabletIndex idx_meta;
        idx_meta.init_from_pb(*index_meta_pb.get());

        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);

        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        Status sts = fs->create_file(index_path, &file_writer, &opts);
        ASSERT_TRUE(sts.ok()) << sts;
        auto index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                fs, index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        // Get field for column c2
        const TabletColumn& column = tablet_schema->column(1); // c2 is the second column
        ASSERT_NE(&column, nullptr);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        ASSERT_NE(field.get(), nullptr);

        // Create column writer
        std::unique_ptr<InvertedIndexColumnWriter> column_writer;
        auto status = InvertedIndexColumnWriter::create(field.get(), &column_writer,
                                                        index_file_writer.get(), &idx_meta);
        EXPECT_TRUE(status.ok()) << status;

        // Add string values
        std::vector<Slice> values = {Slice("apple"), Slice("banana"), Slice("cherry"),
                                     Slice("apple"), // Duplicate to test frequency
                                     Slice("date")};

        status = column_writer->add_values("c2", values.data(), values.size());
        EXPECT_TRUE(status.ok()) << status;

        // Finish and close
        status = column_writer->finish();
        EXPECT_TRUE(status.ok()) << status;

        status = index_file_writer->write_v2();
        EXPECT_TRUE(status.ok()) << status;

        // Verify the terms stats
        ExpectedDocMap expected {
                {"apple", {0, 3}}, {"banana", {1}}, {"cherry", {2}}, {"date", {4}}};

        check_terms_stats(index_path_prefix, &expected, {}, InvertedIndexStorageFormatPB::V2,
                          &idx_meta);
    }

    void test_nulls_write(std::string_view rowset_id, int seg_id) {
        auto tablet_schema = create_schema();

        // Create index meta
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column id

        TabletIndex idx_meta;
        idx_meta.init_from_pb(*index_meta_pb.get());

        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);

        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        Status sts = fs->create_file(index_path, &file_writer, &opts);
        ASSERT_TRUE(sts.ok()) << sts;
        auto index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                fs, index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        // Get field for column c2
        const TabletColumn& column = tablet_schema->column(1); // c2 is the second column
        ASSERT_NE(&column, nullptr);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        ASSERT_NE(field.get(), nullptr);

        // Create column writer
        std::unique_ptr<InvertedIndexColumnWriter> column_writer;
        auto status = InvertedIndexColumnWriter::create(field.get(), &column_writer,
                                                        index_file_writer.get(), &idx_meta);
        EXPECT_TRUE(status.ok()) << status;

        // Add null values
        status = column_writer->add_nulls(3);
        EXPECT_TRUE(status.ok()) << status;

        // Add some regular values
        std::vector<Slice> values = {Slice("apple"), Slice("banana")};

        status = column_writer->add_values("c2", values.data(), values.size());
        EXPECT_TRUE(status.ok()) << status;

        // Add more nulls
        status = column_writer->add_nulls(2);
        EXPECT_TRUE(status.ok()) << status;

        // Finish and close
        status = column_writer->finish();
        EXPECT_TRUE(status.ok()) << status;

        status = index_file_writer->write_v2();
        EXPECT_TRUE(status.ok()) << status;

        // Verify the terms stats
        ExpectedDocMap expected {{"apple", {3}}, {"banana", {4}}};

        // Null values should be at positions 0, 1, 2, 5, 6
        std::vector<int> expected_nulls = {0, 1, 2, 5, 6};

        check_terms_stats(index_path_prefix, &expected, expected_nulls,
                          InvertedIndexStorageFormatPB::V2, &idx_meta);
    }

    void test_numeric_write(std::string_view rowset_id, int seg_id) {
        auto tablet_schema = create_schema();

        // Create index meta
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(0); // c1 column id

        // Set index properties for BKD index
        auto* properties = index_meta_pb->mutable_properties();
        (*properties)["type"] = "bkd";

        TabletIndex idx_meta;
        idx_meta.init_from_pb(*index_meta_pb.get());

        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);

        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        Status sts = fs->create_file(index_path, &file_writer, &opts);
        ASSERT_TRUE(sts.ok()) << sts;
        auto index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                fs, index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        // Get field for column c1
        const TabletColumn& column = tablet_schema->column(0);
        ASSERT_NE(&column, nullptr);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        ASSERT_NE(field.get(), nullptr);

        // Create column writer
        std::unique_ptr<InvertedIndexColumnWriter> column_writer;
        auto status = InvertedIndexColumnWriter::create(field.get(), &column_writer,
                                                        index_file_writer.get(), &idx_meta);
        EXPECT_TRUE(status.ok()) << status;

        // Add integer values
        std::vector<int32_t> values = {42, 100, 42, 200, 300};

        status = column_writer->add_values("c1", values.data(), values.size());
        EXPECT_TRUE(status.ok()) << status;

        // Finish and close
        status = column_writer->finish();
        EXPECT_TRUE(status.ok()) << status;

        status = index_file_writer->write_v2();
        EXPECT_TRUE(status.ok()) << status;

        // For BKD index, we need to verify using BkdIndexReader instead of check_terms_stats
        // Create a vector of document IDs corresponding to the values
        std::vector<int> doc_ids = {0, 1, 2, 3, 4};

        // Verify the BKD index using the appropriate method
        check_bkd_index(index_path_prefix, &idx_meta, values, doc_ids);
    }

    void test_unicode_string_write(std::string_view rowset_id, int seg_id,
                                   bool enable_correct_term_write) {
        auto tablet_schema = create_schema();

        // Create index meta
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column id

        TabletIndex idx_meta;
        idx_meta.init_from_pb(*index_meta_pb.get());

        std::string index_path_prefix {InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id))};
        std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);

        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        Status sts = fs->create_file(index_path, &file_writer, &opts);
        ASSERT_TRUE(sts.ok()) << sts;
        auto index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                fs, index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        // Get field for column c2
        const TabletColumn& column = tablet_schema->column(1); // c2 is the second column
        ASSERT_NE(&column, nullptr);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        ASSERT_NE(field.get(), nullptr);

        // Save original config value
        bool original_config_value = config::enable_inverted_index_correct_term_write;

        // Set the config value for this test
        config::enable_inverted_index_correct_term_write = enable_correct_term_write;

        // Create column writer
        std::unique_ptr<InvertedIndexColumnWriter> column_writer;
        auto status = InvertedIndexColumnWriter::create(field.get(), &column_writer,
                                                        index_file_writer.get(), &idx_meta);
        EXPECT_TRUE(status.ok()) << status;

        // Add string values with Unicode characters above 0xFFFF
        // U+10000 (𐀀) LINEAR B SYLLABLE B008 A
        // U+1F600 (😀) GRINNING FACE
        // U+1F914 (🤔) THINKING FACE
        std::vector<Slice> values = {
                Slice("regular"),
                Slice("unicode_𐀀"),   // Contains U+10000
                Slice("emoji_😀"),    // Contains U+1F600
                Slice("thinking_🤔"), // Contains U+1F914
                Slice("regular")      // Duplicate to test frequency
        };

        status = column_writer->add_values("c2", values.data(), values.size());
        EXPECT_TRUE(status.ok()) << status;

        // Finish and close
        status = column_writer->finish();
        EXPECT_TRUE(status.ok()) << status;

        status = index_file_writer->write_v2();
        EXPECT_TRUE(status.ok()) << status;

        // Restore original config value
        config::enable_inverted_index_correct_term_write = original_config_value;

        // Verify the terms stats
        ExpectedDocMap expected {
                {"regular", {0, 4}}, {"unicode_𐀀", {1}}, {"emoji_😀", {2}}, {"thinking_🤔", {3}}};

        check_terms_stats(index_path_prefix, &expected, {}, InvertedIndexStorageFormatPB::V2,
                          &idx_meta);

        // Now query the index to verify the results
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_query = false;
        runtime_state.set_query_options(query_options);

        // Create a reader to verify the index
        auto reader = std::make_shared<InvertedIndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
        status = reader->init();
        EXPECT_EQ(status, Status::OK());
        auto result = reader->open(&idx_meta);
        EXPECT_TRUE(result.has_value()) << "Failed to open compound reader" << result.error();

        // Create an inverted index reader
        auto inverted_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(inverted_reader, nullptr);

        // Create IO context for query
        io::IOContext io_ctx;
        std::string field_name = std::to_string(field->unique_id());

        // Test querying for each value
        for (size_t i = 0; i < values.size(); i++) {
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            StringRef str_ref(values[i].data, values[i].size);
            auto query_status =
                    inverted_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref,
                                           InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(query_status.ok()) << query_status;
            // For regular strings, both should work the same
            if (i == 0 || i == 4) {
                EXPECT_TRUE(bitmap->contains(0)) << "Value 'regular' should match document 0";
                EXPECT_TRUE(bitmap->contains(4)) << "Value 'regular' should match document 4";
            }
            // For Unicode strings, the behavior might differ based on enable_correct_term_write
            else {
                // Check if the document is found
                if (enable_correct_term_write) {
                    EXPECT_TRUE(bitmap->contains(i))
                            << "Value '" << values[i].to_string() << "' should match document " << i
                            << " with enable_correct_term_write=" << enable_correct_term_write;
                } else {
                    EXPECT_FALSE(bitmap->contains(i))
                            << "Value '" << values[i].to_string() << "' should not match document "
                            << i << " with enable_correct_term_write=" << enable_correct_term_write;
                }
            }
        }
    }

private:
    std::string _current_dir;
    std::string _absolute_dir;
    std::unique_ptr<InvertedIndexSearcherCache> _inverted_index_searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _inverted_index_query_cache;
};

// Test case for writing string values
TEST_F(InvertedIndexWriterTest, StringWrite) {
    test_string_write("test_rowset_1", 0);
}

// Test case for nulls writing
TEST_F(InvertedIndexWriterTest, NullsWrite) {
    test_nulls_write("test_rowset_2", 0);
}

// Test case for numeric values
TEST_F(InvertedIndexWriterTest, NumericWrite) {
    test_numeric_write("test_rowset_3", 0);
}

// Test case for Unicode string values with enable_correct_term_write=true
TEST_F(InvertedIndexWriterTest, UnicodeStringWriteEnabled) {
    test_unicode_string_write("test_rowset_4", 0, true);
}

// Test case for Unicode string values with enable_correct_term_write=false
TEST_F(InvertedIndexWriterTest, UnicodeStringWriteDisabled) {
    test_unicode_string_write("test_rowset_5", 0, false);
}

// Test case to compare the difference between enabled and disabled correct term write
TEST_F(InvertedIndexWriterTest, CompareUnicodeStringWriteResults) {
    // First write with enabled correct term write
    auto tablet_schema = create_schema();

    // Create index meta
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(1);
    index_meta_pb->set_index_name("test");
    index_meta_pb->clear_col_unique_id();
    index_meta_pb->add_col_unique_id(1); // c2 column id

    TabletIndex idx_meta;
    idx_meta.init_from_pb(*index_meta_pb.get());

    // Create two indexes with different settings
    std::string index_path_prefix_enabled {InvertedIndexDescriptor::get_index_file_path_prefix(
            local_segment_path(kTestDir, "test_rowset_compare", 1))};
    std::string index_path_enabled =
            InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix_enabled);

    std::string index_path_prefix_disabled {InvertedIndexDescriptor::get_index_file_path_prefix(
            local_segment_path(kTestDir, "test_rowset_compare", 2))};
    std::string index_path_disabled =
            InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix_disabled);

    // Create file writers
    io::FileWriterPtr file_writer_enabled, file_writer_disabled;
    io::FileWriterOptions opts;
    auto fs = io::global_local_filesystem();
    Status sts = fs->create_file(index_path_enabled, &file_writer_enabled, &opts);
    ASSERT_TRUE(sts.ok()) << sts;
    sts = fs->create_file(index_path_disabled, &file_writer_disabled, &opts);
    ASSERT_TRUE(sts.ok()) << sts;

    auto index_file_writer_enabled = std::make_unique<InvertedIndexFileWriter>(
            fs, index_path_prefix_enabled, "test_rowset_compare", 1,
            InvertedIndexStorageFormatPB::V2, std::move(file_writer_enabled));

    auto index_file_writer_disabled = std::make_unique<InvertedIndexFileWriter>(
            fs, index_path_prefix_disabled, "test_rowset_compare", 2,
            InvertedIndexStorageFormatPB::V2, std::move(file_writer_disabled));

    // Get field for column c2
    const TabletColumn& column = tablet_schema->column(1); // c2 is the second column
    ASSERT_NE(&column, nullptr);
    std::unique_ptr<Field> field(FieldFactory::create(column));
    ASSERT_NE(field.get(), nullptr);

    // Save original config value
    bool original_config_value = config::enable_inverted_index_correct_term_write;

    // Create column writers with different settings
    std::unique_ptr<InvertedIndexColumnWriter> column_writer_enabled, column_writer_disabled;

    // Set config to enabled for first writer
    config::enable_inverted_index_correct_term_write = true;
    auto status = InvertedIndexColumnWriter::create(field.get(), &column_writer_enabled,
                                                    index_file_writer_enabled.get(), &idx_meta);
    EXPECT_TRUE(status.ok()) << status;

    // Set config to disabled for second writer
    config::enable_inverted_index_correct_term_write = false;
    status = InvertedIndexColumnWriter::create(field.get(), &column_writer_disabled,
                                               index_file_writer_disabled.get(), &idx_meta);
    EXPECT_TRUE(status.ok()) << status;

    // Add string values with Unicode characters above 0xFFFF
    // U+10000 (𐀀) LINEAR B SYLLABLE B008 A
    // U+1F600 (😀) GRINNING FACE
    // U+1F914 (🤔) THINKING FACE
    std::vector<Slice> values = {
            Slice("unicode_𐀀"),   // Contains U+10000
            Slice("emoji_😀"),    // Contains U+1F600
            Slice("thinking_🤔"), // Contains U+1F914
    };

    // Add values to both writers
    status = column_writer_enabled->add_values("c2", values.data(), values.size());
    EXPECT_TRUE(status.ok()) << status;

    status = column_writer_disabled->add_values("c2", values.data(), values.size());
    EXPECT_TRUE(status.ok()) << status;

    // Finish and close both writers
    status = column_writer_enabled->finish();
    EXPECT_TRUE(status.ok()) << status;
    status = index_file_writer_enabled->write_v2();
    EXPECT_TRUE(status.ok()) << status;

    status = column_writer_disabled->finish();
    EXPECT_TRUE(status.ok()) << status;
    status = index_file_writer_disabled->write_v2();
    EXPECT_TRUE(status.ok()) << status;

    // Restore original config value
    config::enable_inverted_index_correct_term_write = original_config_value;

    // Now query both indexes and compare results
    OlapReaderStatistics stats;
    RuntimeState runtime_state;
    TQueryOptions query_options;
    query_options.enable_inverted_index_query = false;
    runtime_state.set_query_options(query_options);
    io::IOContext io_ctx;

    // Create readers for both indexes
    auto reader_enabled = std::make_shared<InvertedIndexFileReader>(
            io::global_local_filesystem(), index_path_prefix_enabled,
            InvertedIndexStorageFormatPB::V2);
    status = reader_enabled->init();
    EXPECT_EQ(status, Status::OK());
    auto result_enabled = reader_enabled->open(&idx_meta);
    EXPECT_TRUE(result_enabled.has_value())
            << "Failed to open compound reader" << result_enabled.error();

    auto reader_disabled = std::make_shared<InvertedIndexFileReader>(
            io::global_local_filesystem(), index_path_prefix_disabled,
            InvertedIndexStorageFormatPB::V2);
    status = reader_disabled->init();
    EXPECT_EQ(status, Status::OK());
    auto result_disabled = reader_disabled->open(&idx_meta);
    EXPECT_TRUE(result_disabled.has_value())
            << "Failed to open compound reader" << result_disabled.error();

    // Create inverted index readers
    auto inverted_reader_enabled =
            StringTypeInvertedIndexReader::create_shared(&idx_meta, reader_enabled);
    EXPECT_NE(inverted_reader_enabled, nullptr);

    auto inverted_reader_disabled =
            StringTypeInvertedIndexReader::create_shared(&idx_meta, reader_disabled);
    EXPECT_NE(inverted_reader_disabled, nullptr);
    std::string field_name = std::to_string(field->unique_id());

    // Test querying for each value and compare results
    for (size_t i = 0; i < values.size(); i++) {
        std::shared_ptr<roaring::Roaring> bitmap_enabled = std::make_shared<roaring::Roaring>();
        std::shared_ptr<roaring::Roaring> bitmap_disabled = std::make_shared<roaring::Roaring>();

        auto query_status_enabled = inverted_reader_enabled->query(
                &io_ctx, &stats, &runtime_state, field_name, &values[i],
                InvertedIndexQueryType::EQUAL_QUERY, bitmap_enabled);

        auto query_status_disabled = inverted_reader_disabled->query(
                &io_ctx, &stats, &runtime_state, field_name, &values[i],
                InvertedIndexQueryType::EQUAL_QUERY, bitmap_disabled);

        EXPECT_TRUE(query_status_enabled.ok()) << query_status_enabled;
        EXPECT_TRUE(query_status_disabled.ok()) << query_status_disabled;

        // Check if both queries found the document
        EXPECT_TRUE(bitmap_enabled->contains(i))
                << "Value '" << values[i].to_string() << "' should match document " << i
                << " with enable_correct_term_write=true";

        EXPECT_FALSE(bitmap_disabled->contains(i))
                << "Value '" << values[i].to_string() << "' should match document " << i
                << " with enable_correct_term_write=false";

        // Print the cardinality of both bitmaps to see if there's any difference
        std::cout << "Value: " << values[i].to_string()
                  << ", enabled cardinality: " << bitmap_enabled->cardinality()
                  << ", disabled cardinality: " << bitmap_disabled->cardinality() << std::endl;
    }
}

} // namespace doris::segment_v2