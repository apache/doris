// be/test/olap/rowset/segment_v2/inverted_index_reader_test.cpp
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

#include "olap/rowset/segment_v2/inverted_index_reader.h"

#include <CLucene.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <cstring>
#include <map>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "olap/field.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/rowset/segment_v2/inverted_index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "runtime/runtime_state.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris::segment_v2 {

class InvertedIndexReaderTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/inverted_index_reader_test";

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
        //        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    // Create table schema
    TabletSchemaSPtr create_schema(KeysType keys_type = DUP_KEYS) {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(keys_type);

        tablet_schema->init_from_pb(tablet_schema_pb);

        // Add INT type key column
        TabletColumn column_1;
        column_1.set_name("c1");
        column_1.set_unique_id(0);
        column_1.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        column_1.set_length(4);
        column_1.set_index_length(4);
        column_1.set_is_key(true);
        column_1.set_is_nullable(true);
        tablet_schema->append_column(column_1);

        // Add VARCHAR type value column
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

    std::string local_segment_path(std::string base, std::string_view rowset_id, int64_t seg_id) {
        return fmt::format("{}/{}_{}.dat", base, rowset_id, seg_id);
    }

    // Create string inverted index and write data
    void prepare_string_index(std::string_view rowset_id, int seg_id, std::vector<Slice>& values,
                              TabletIndex* idx_meta, std::string* index_path_prefix) {
        auto tablet_schema = create_schema();

        // Create index metadata
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column ID

        idx_meta->init_from_pb(*index_meta_pb.get());

        *index_path_prefix = InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id));
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v2(*index_path_prefix);

        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        Status sts = fs->create_file(index_path, &file_writer, &opts);
        ASSERT_TRUE(sts.ok()) << sts;
        auto index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                fs, *index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        // Get c2 column Field
        const TabletColumn& column = tablet_schema->column(1);
        ASSERT_NE(&column, nullptr);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        ASSERT_NE(field.get(), nullptr);

        // Create column writer
        std::unique_ptr<InvertedIndexColumnWriter> column_writer;
        auto status = InvertedIndexColumnWriter::create(field.get(), &column_writer,
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

    // Create inverted index with NULL values
    void prepare_null_index(std::string_view rowset_id, int seg_id, TabletIndex* idx_meta,
                            std::string* index_path_prefix) {
        auto tablet_schema = create_schema();

        // Create index metadata
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column ID

        idx_meta->init_from_pb(*index_meta_pb.get());

        *index_path_prefix = InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id));
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v2(*index_path_prefix);

        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        Status sts = fs->create_file(index_path, &file_writer, &opts);
        ASSERT_TRUE(sts.ok()) << sts;
        auto index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                fs, *index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        // Get c2 column Field
        const TabletColumn& column = tablet_schema->column(1);
        ASSERT_NE(&column, nullptr);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        ASSERT_NE(field.get(), nullptr);

        // Create column writer
        std::unique_ptr<InvertedIndexColumnWriter> column_writer;
        auto status = InvertedIndexColumnWriter::create(field.get(), &column_writer,
                                                        index_file_writer.get(), idx_meta);
        EXPECT_TRUE(status.ok()) << status;

        // Add NULL values
        status = column_writer->add_nulls(3);
        EXPECT_TRUE(status.ok()) << status;

        // Add some regular values
        std::vector<Slice> values = {Slice("apple"), Slice("banana")};
        status = column_writer->add_values("c2", values.data(), values.size());
        EXPECT_TRUE(status.ok()) << status;

        // Add more NULL values
        status = column_writer->add_nulls(2);
        EXPECT_TRUE(status.ok()) << status;

        // Finish and close
        status = column_writer->finish();
        EXPECT_TRUE(status.ok()) << status;

        status = index_file_writer->close();
        EXPECT_TRUE(status.ok()) << status;
    }

    // Create BKD index
    void prepare_bkd_index(std::string_view rowset_id, int seg_id, std::vector<int32_t>& values,
                           TabletIndex* idx_meta, std::string* index_path_prefix) {
        auto tablet_schema = create_schema();

        // Create index metadata
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(0); // c1 column ID

        // Set BKD index properties
        auto* properties = index_meta_pb->mutable_properties();
        (*properties)["type"] = "bkd";

        idx_meta->init_from_pb(*index_meta_pb.get());

        *index_path_prefix = InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id));
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v2(*index_path_prefix);

        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        Status sts = fs->create_file(index_path, &file_writer, &opts);
        ASSERT_TRUE(sts.ok()) << sts;
        auto index_file_writer = std::make_unique<InvertedIndexFileWriter>(
                fs, *index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        // Get c1 column Field
        const TabletColumn& column = tablet_schema->column(0);
        ASSERT_NE(&column, nullptr);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        ASSERT_NE(field.get(), nullptr);

        // Create column writer
        std::unique_ptr<InvertedIndexColumnWriter> column_writer;
        auto status = InvertedIndexColumnWriter::create(field.get(), &column_writer,
                                                        index_file_writer.get(), idx_meta);
        EXPECT_TRUE(status.ok()) << status;

        // Add integer values
        status = column_writer->add_values("c1", values.data(), values.size());
        EXPECT_TRUE(status.ok()) << status;

        // Finish and close
        status = column_writer->finish();
        EXPECT_TRUE(status.ok()) << status;

        status = index_file_writer->close();
        EXPECT_TRUE(status.ok()) << status;
    }

    // Test string inverted index reading
    void test_string_index_read() {
        std::string_view rowset_id = "test_read_rowset_1";
        int seg_id = 0;

        // Prepare data
        std::vector<Slice> values = {Slice("apple"), Slice("banana"), Slice("cherry"),
                                     Slice("apple"), // Duplicate value to test frequency
                                     Slice("date")};

        TabletIndex idx_meta;
        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);
        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<InvertedIndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);

        auto status = reader->init();
        EXPECT_EQ(status, Status::OK());

        auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(str_reader, nullptr);

        // Test query
        io::IOContext io_ctx;
        std::string field_name = "1"; // c2 column unique_id

        // Test EQUAL query
        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        StringRef str_ref(values[0].data, values[0].size); // "apple"

        auto query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref,
                                              InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 2) << "Should find 2 documents matching 'apple'";
        EXPECT_TRUE(bitmap->contains(0)) << "Document 0 should match 'apple'";
        EXPECT_TRUE(bitmap->contains(3)) << "Document 3 should match 'apple'";

        // Test non-existent value
        bitmap = std::make_shared<roaring::Roaring>();
        std::string not_exist = "orange";
        StringRef not_exist_ref(not_exist.c_str(), not_exist.length());

        query_status =
                str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &not_exist_ref,
                                  InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 0) << "Should not find any document matching 'orange'";
    }

    // Test NULL value handling
    void test_null_bitmap_read() {
        std::string_view rowset_id = "test_read_rowset_2";
        int seg_id = 0;

        TabletIndex idx_meta;
        std::string index_path_prefix;
        prepare_null_index(rowset_id, seg_id, &idx_meta, &index_path_prefix);

        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        io::IOContext io_ctx;

        auto reader = std::make_shared<InvertedIndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);

        auto status = reader->init();
        EXPECT_EQ(status, Status::OK());

        auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(str_reader, nullptr);

        // Read NULL bitmap
        InvertedIndexQueryCacheHandle cache_handle;
        status = str_reader->read_null_bitmap(&io_ctx, &stats, &cache_handle, nullptr);
        EXPECT_TRUE(status.ok()) << status;

        // Get NULL bitmap
        std::shared_ptr<roaring::Roaring> null_bitmap = cache_handle.get_bitmap();
        EXPECT_NE(null_bitmap, nullptr);

        // Verify expected values in NULL bitmap
        EXPECT_EQ(null_bitmap->cardinality(), 5) << "Should have 5 NULL documents";
        std::vector<int> expected_nulls = {0, 1, 2, 5, 6};
        for (int doc_id : expected_nulls) {
            EXPECT_TRUE(null_bitmap->contains(doc_id))
                    << "Document " << doc_id << " should be NULL";
        }
    }

    // Test BKD index query
    void test_bkd_index_read() {
        std::string_view rowset_id = "test_read_rowset_3";
        int seg_id = 0;

        // Prepare data
        std::vector<int32_t> values = {42, 100, 42, 200, 300};

        TabletIndex idx_meta;
        std::string index_path_prefix;
        prepare_bkd_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<InvertedIndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);

        auto status = reader->init();
        EXPECT_EQ(status, Status::OK());

        auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(bkd_reader, nullptr);

        io::IOContext io_ctx;
        std::string field_name = "0"; // c1 column unique_id

        // Test EQUAL query
        int32_t query_value = 42;
        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();

        auto query_status =
                bkd_reader->query(&io_ctx, &stats, &runtime_state, field_name, &query_value,
                                  InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 2) << "Should find 2 documents matching value 42";
        EXPECT_TRUE(bitmap->contains(0)) << "Document 0 should match value 42";
        EXPECT_TRUE(bitmap->contains(2)) << "Document 2 should match value 42";

        // Test LESS_THAN query
        bitmap = std::make_shared<roaring::Roaring>();
        int32_t less_than_value = 100;

        query_status =
                bkd_reader->query(&io_ctx, &stats, &runtime_state, field_name, &less_than_value,
                                  InvertedIndexQueryType::LESS_THAN_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 2) << "Should find 2 documents with values less than 100";
        EXPECT_TRUE(bitmap->contains(0)) << "Document 0 should have value less than 100";
        EXPECT_TRUE(bitmap->contains(2)) << "Document 2 should have value less than 100";

        // Test GREATER_THAN query
        bitmap = std::make_shared<roaring::Roaring>();
        int32_t greater_than_value = 100;

        query_status =
                bkd_reader->query(&io_ctx, &stats, &runtime_state, field_name, &greater_than_value,
                                  InvertedIndexQueryType::GREATER_THAN_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 2)
                << "Should find 2 documents with values greater than 100";
        EXPECT_TRUE(bitmap->contains(3)) << "Document 3 should have value greater than 100";
        EXPECT_TRUE(bitmap->contains(4)) << "Document 4 should have value greater than 100";
    }

    // Test query cache
    void test_query_cache() {
        std::string_view rowset_id = "test_read_rowset_4";
        int seg_id = 0;

        // Prepare data
        std::vector<Slice> values = {Slice("apple"), Slice("banana"), Slice("cherry")};

        TabletIndex idx_meta;
        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_query_cache = true;
        query_options.enable_inverted_index_searcher_cache = true;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<InvertedIndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);

        auto status = reader->init();
        EXPECT_EQ(status, Status::OK());

        auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(str_reader, nullptr);

        io::IOContext io_ctx;
        std::string field_name = "1"; // c2 column unique_id

        // First query, should be cache miss
        std::shared_ptr<roaring::Roaring> bitmap1 = std::make_shared<roaring::Roaring>();
        StringRef str_ref(values[0].data, values[0].size); // "apple"

        auto query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref,
                                              InvertedIndexQueryType::EQUAL_QUERY, bitmap1);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(stats.inverted_index_query_cache_miss, 1) << "First query should be cache miss";
        EXPECT_EQ(bitmap1->cardinality(), 1) << "Should find 1 document matching 'apple'";

        // Second query with same value, should be cache hit
        std::shared_ptr<roaring::Roaring> bitmap2 = std::make_shared<roaring::Roaring>();

        query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref,
                                         InvertedIndexQueryType::EQUAL_QUERY, bitmap2);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(stats.inverted_index_query_cache_hit, 1) << "Second query should be cache hit";
        EXPECT_EQ(bitmap2->cardinality(), 1) << "Should find 1 document matching 'apple'";
    }

    // Test searcher cache
    void test_searcher_cache() {
        std::string_view rowset_id = "test_read_rowset_5";
        int seg_id = 0;

        // Prepare data
        std::vector<Slice> values = {Slice("apple"), Slice("banana"), Slice("cherry")};

        TabletIndex idx_meta;
        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_query_cache = false;
        query_options.enable_inverted_index_searcher_cache = true;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<InvertedIndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);

        auto status = reader->init();
        EXPECT_EQ(status, Status::OK());

        auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(str_reader, nullptr);

        io::IOContext io_ctx;
        std::string field_name = "1"; // c2 column unique_id

        // First query, should be searcher cache miss
        std::shared_ptr<roaring::Roaring> bitmap1 = std::make_shared<roaring::Roaring>();
        StringRef str_ref(values[0].data, values[0].size); // "apple"

        auto query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref,
                                              InvertedIndexQueryType::EQUAL_QUERY, bitmap1);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(stats.inverted_index_searcher_cache_miss, 1)
                << "First query should be searcher cache miss";

        // Query with different value, should be searcher cache hit
        std::shared_ptr<roaring::Roaring> bitmap2 = std::make_shared<roaring::Roaring>();
        StringRef str_ref2(values[1].data, values[1].size); // "banana"

        query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref2,
                                         InvertedIndexQueryType::EQUAL_QUERY, bitmap2);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(stats.inverted_index_searcher_cache_hit, 1)
                << "Second query should be searcher cache hit";
    }

    // Test string index with large document set (>512 docs)
    void test_string_index_large_docset() {
        std::string_view rowset_id = "test_read_rowset_6";
        int seg_id = 0;

        // Prepare data with 1000 documents
        std::vector<Slice> values;
        values.reserve(1000);

        // Add 600 documents with term "common_term"
        for (int i = 0; i < 600; i++) {
            values.emplace_back("common_term");
        }

        // Add 200 documents with term "term_a"
        for (int i = 0; i < 200; i++) {
            values.emplace_back("term_a");
        }

        // Add 200 documents with term "term_b"
        for (int i = 0; i < 200; i++) {
            values.emplace_back("term_b");
        }

        TabletIndex idx_meta;
        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<InvertedIndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);

        auto status = reader->init();
        EXPECT_EQ(status, Status::OK());

        auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(str_reader, nullptr);

        io::IOContext io_ctx;
        std::string field_name = "1"; // c2 column unique_id

        // Test query for "common_term" which has >512 documents
        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        std::string query_term = "common_term";
        StringRef str_ref(query_term.c_str(), query_term.length());

        auto query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref,
                                              InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 600) << "Should find 600 documents matching 'common_term'";

        // Verify first and last document IDs
        EXPECT_TRUE(bitmap->contains(0)) << "First document should match 'common_term'";
        EXPECT_TRUE(bitmap->contains(599)) << "Last document should match 'common_term'";

        // Test query for "term_a"
        bitmap = std::make_shared<roaring::Roaring>();
        query_term = "term_a";
        StringRef str_ref_a(query_term.c_str(), query_term.length());

        query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref_a,
                                         InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 200) << "Should find 200 documents matching 'term_a'";
        EXPECT_TRUE(bitmap->contains(600)) << "First document of term_a should be at position 600";
        EXPECT_TRUE(bitmap->contains(799)) << "Last document of term_a should be at position 799";
    }

    // Test reading existing large document set index file
    void test_read_existing_large_docset_old_version() {
        std::string data_dir = "./be/test/olap/test_data/";
        std::string_view rowset_id =
                "test_read_rowset_6_old128"; // Changed from string_view to string
        int seg_id = 0;

        // Create index metadata (same as in prepare_string_index)
        auto tablet_schema = create_schema();
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column ID

        TabletIndex idx_meta;
        idx_meta.init_from_pb(*index_meta_pb.get());
        std::string index_path_prefix;
        // Get the index file path
        index_path_prefix = InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(data_dir, rowset_id, seg_id));

        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        query_options.inverted_index_compatible_read = true;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<InvertedIndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);

        auto status = reader->init();
        EXPECT_EQ(status, Status::OK());

        auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(str_reader, nullptr);

        io::IOContext io_ctx;
        std::string field_name = "1"; // c2 column unique_id

        // Test queries on the existing index file

        // 1. Query for "common_term" (should have 600 docs)
        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        std::string query_term = "common_term";
        StringRef str_ref(query_term.c_str(), query_term.length());

        auto query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref,
                                              InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 600)
                << "Should find 600 documents matching 'common_term' in existing index";

        // Verify the document range for common_term
        EXPECT_TRUE(bitmap->contains(0)) << "First document should match 'common_term'";
        EXPECT_TRUE(bitmap->contains(599)) << "Last document should match 'common_term'";

        // 2. Query for "term_a" (should have 200 docs)
        bitmap = std::make_shared<roaring::Roaring>();
        query_term = "term_a";
        StringRef str_ref_a(query_term.c_str(), query_term.length());

        query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref_a,
                                         InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 200)
                << "Should find 200 documents matching 'term_a' in existing index";
        EXPECT_TRUE(bitmap->contains(600)) << "First document of term_a should be at position 600";
        EXPECT_TRUE(bitmap->contains(799)) << "Last document of term_a should be at position 799";

        // 3. Query for "term_b" (should have 200 docs)
        bitmap = std::make_shared<roaring::Roaring>();
        query_term = "term_b";
        StringRef str_ref_b(query_term.c_str(), query_term.length());

        query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref_b,
                                         InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 200)
                << "Should find 200 documents matching 'term_b' in existing index";
        EXPECT_TRUE(bitmap->contains(800)) << "First document of term_b should be at position 800";
        EXPECT_TRUE(bitmap->contains(999)) << "Last document of term_b should be at position 999";

        // 4. Query for non-existent term
        bitmap = std::make_shared<roaring::Roaring>();
        query_term = "non_existent_term";
        StringRef str_ref_non(query_term.c_str(), query_term.length());

        query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref_non,
                                         InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 0) << "Should find 0 documents for non-existent term";
    }
    // Test reading existing large document set index file
    void test_read_existing_large_docset_new_version() {
        std::string data_dir = "./be/test/olap/test_data/";
        std::string_view rowset_id =
                "test_read_rowset_6_new128"; // Changed from string_view to string
        int seg_id = 0;

        // Create index metadata (same as in prepare_string_index)
        auto tablet_schema = create_schema();
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column ID

        TabletIndex idx_meta;
        idx_meta.init_from_pb(*index_meta_pb.get());
        std::string index_path_prefix;
        // Get the index file path
        index_path_prefix = InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(data_dir, rowset_id, seg_id));

        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        query_options.inverted_index_compatible_read = false;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<InvertedIndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);

        auto status = reader->init();
        EXPECT_EQ(status, Status::OK());

        auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(str_reader, nullptr);

        io::IOContext io_ctx;
        std::string field_name = "1"; // c2 column unique_id

        // Test queries on the existing index file

        // 1. Query for "common_term" (should have 600 docs)
        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        std::string query_term = "common_term";
        StringRef str_ref(query_term.c_str(), query_term.length());

        auto query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref,
                                              InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 600)
                << "Should find 600 documents matching 'common_term' in existing index";

        // Verify the document range for common_term
        EXPECT_TRUE(bitmap->contains(0)) << "First document should match 'common_term'";
        EXPECT_TRUE(bitmap->contains(599)) << "Last document should match 'common_term'";

        // 2. Query for "term_a" (should have 200 docs)
        bitmap = std::make_shared<roaring::Roaring>();
        query_term = "term_a";
        StringRef str_ref_a(query_term.c_str(), query_term.length());

        query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref_a,
                                         InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 200)
                << "Should find 200 documents matching 'term_a' in existing index";
        EXPECT_TRUE(bitmap->contains(600)) << "First document of term_a should be at position 600";
        EXPECT_TRUE(bitmap->contains(799)) << "Last document of term_a should be at position 799";

        // 3. Query for "term_b" (should have 200 docs)
        bitmap = std::make_shared<roaring::Roaring>();
        query_term = "term_b";
        StringRef str_ref_b(query_term.c_str(), query_term.length());

        query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref_b,
                                         InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 200)
                << "Should find 200 documents matching 'term_b' in existing index";
        EXPECT_TRUE(bitmap->contains(800)) << "First document of term_b should be at position 800";
        EXPECT_TRUE(bitmap->contains(999)) << "Last document of term_b should be at position 999";

        // 4. Query for non-existent term
        bitmap = std::make_shared<roaring::Roaring>();
        query_term = "non_existent_term";
        StringRef str_ref_non(query_term.c_str(), query_term.length());

        query_status = str_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref_non,
                                         InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 0) << "Should find 0 documents for non-existent term";
    }

private:
    std::unique_ptr<InvertedIndexSearcherCache> _inverted_index_searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _inverted_index_query_cache;
};

// String index reading test
TEST_F(InvertedIndexReaderTest, StringIndexRead) {
    test_string_index_read();
}

// NULL value bitmap test
TEST_F(InvertedIndexReaderTest, NullBitmapRead) {
    test_null_bitmap_read();
}

// BKD index reading test
TEST_F(InvertedIndexReaderTest, BkdIndexRead) {
    test_bkd_index_read();
}

// Query cache test
TEST_F(InvertedIndexReaderTest, QueryCache) {
    test_query_cache();
}

// Searcher cache test
TEST_F(InvertedIndexReaderTest, SearcherCache) {
    test_searcher_cache();
}

// Test string index with large document set (>512 docs)
TEST_F(InvertedIndexReaderTest, StringIndexLargeDocset) {
    test_string_index_large_docset();
}

// Test reading existing large document set index file
TEST_F(InvertedIndexReaderTest, ReadExistingLargeDocset) {
    test_read_existing_large_docset_old_version();
    test_read_existing_large_docset_new_version();
}

} // namespace doris::segment_v2