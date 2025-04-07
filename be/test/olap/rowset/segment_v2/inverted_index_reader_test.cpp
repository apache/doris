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
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
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

    // Helper function to check CPU architecture
    bool is_arm_architecture() {
#if defined(__aarch64__)
        return true;
#else
        return false;
#endif
    }

    // Helper function to check AVX2 support
    bool has_avx2_support() {
#if defined(USE_AVX2) && defined(__x86_64__)
        unsigned int eax, ebx, ecx, edx;
        __asm__("cpuid" : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx) : "0"(7), "c"(0));
        return (ebx & (1 << 5)) != 0; // Check AVX2 bit
#else
        return false;
#endif
    }

    // Helper function to test index reading with inline validation
    void test_read_index_file(
            const TabletIndex& idx_meta, const std::string& data_dir, const std::string& index_file,
            const std::string& field_name, const std::string& query_term,
            InvertedIndexStorageFormatPB storage_format, bool enable_compatible_read,
            uint64_t expected_cardinality,                   // Added expected cardinality parameter
            const std::vector<uint32_t>& expected_doc_ids) { // Added expected doc IDs parameter
        // Get the index file path
        std::string index_path_prefix = data_dir + "/" + index_file;

        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        query_options.inverted_index_compatible_read = enable_compatible_read;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<InvertedIndexFileReader>(io::global_local_filesystem(),
                                                                index_path_prefix, storage_format);

        auto status = reader->init();
        ASSERT_TRUE(status.ok()) << "Failed to initialize InvertedIndexFileReader for "
                                 << index_file << ": " << status.to_string();

        auto index_reader = FullTextIndexReader::create_shared(&idx_meta, reader);
        ASSERT_NE(index_reader, nullptr)
                << "Failed to create FullTextIndexReader for " << index_file;

        io::IOContext io_ctx;

        // Test queries
        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        StringRef str_ref(query_term.c_str(), query_term.length());

        auto query_status =
                index_reader->query(&io_ctx, &stats, &runtime_state, field_name, &str_ref,
                                    InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);

        ASSERT_TRUE(query_status.ok()) << "Query failed for term '" << query_term << "' in file "
                                       << index_file << ": " << query_status.to_string();

        // Perform validation inline
        ASSERT_NE(bitmap, nullptr)
                << "Bitmap is null after successful query for file: " << index_file;
        EXPECT_EQ(bitmap->cardinality(), expected_cardinality)
                << "File: " << index_file << " - Incorrect cardinality for term '" << query_term
                << "'";

        for (uint32_t doc_id : expected_doc_ids) {
            EXPECT_TRUE(bitmap->contains(doc_id))
                    << "File: " << index_file << " - Bitmap should contain doc ID " << doc_id
                    << " for term '" << query_term << "'";
        }
    }

    // Test reading existing large document set index file
    void test_read_existing_large_docset() {
        std::string data_dir = "./be/test/olap/test_data";

        // Helper lambda to create TabletIndex easily
        auto create_test_index_meta = [](int64_t index_id, const std::string& index_name,
                                         int64_t col_unique_id) {
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(index_id);
            index_meta_pb->set_index_name(index_name);
            index_meta_pb->clear_col_unique_id();
            index_meta_pb->add_col_unique_id(col_unique_id);
            index_meta_pb->mutable_properties()->insert({"parser", "english"});
            index_meta_pb->mutable_properties()->insert({"lower_case", "true"});
            index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
            TabletIndex idx_meta;
            idx_meta.init_from_pb(*index_meta_pb.get());
            return idx_meta;
        };

        // Default metadata, query parameters, and expected results
        int64_t default_index_id = 10071;
        std::string default_index_name = "test";
        int64_t default_col_unique_id = 1;
        std::string default_field_name = "request";
        std::string default_query_term = "gif";
        uint64_t expected_gif_cardinality = 27296;
        std::vector<uint32_t> expected_gif_doc_ids = {0, 19, 22, 23, 24, 26, 1000, 10278, 44702};

        if (is_arm_architecture()) {
            // Test ARM architecture cases
            std::cout << "Testing on ARM architecture" << std::endl;
            {
                TabletIndex meta_arm_old_v2 =
                        create_test_index_meta(1744016478578, "request_idx", 2);
                std::string field_name_avx2 = "2";
                default_query_term = "gif";
                expected_gif_cardinality = 37284;
                expected_gif_doc_ids = {0, 19, 110, 1000, 2581, 7197, 9091, 16711, 29676, 44702};
                test_read_index_file(meta_arm_old_v2, data_dir, "arm_old_v2", field_name_avx2,
                                     default_query_term, InvertedIndexStorageFormatPB::V2, false,
                                     expected_gif_cardinality, expected_gif_doc_ids);
                default_query_term = "/english/index.html";
                expected_gif_cardinality = 359;
                expected_gif_doc_ids = {
                        25,    63,    66,    135,   214,   276,   287,   321,   653,   819,   968,
                        1038,  1115,  1210,  1305,  1394,  1650,  1690,  1761,  1934,  1935,  2101,
                        2114,  2544,  2815,  2912,  3028,  3104,  3475,  3953,  3991,  4052,  4097,
                        4424,  4430,  4458,  4504,  4571,  4629,  4704,  4711,  4838,  5021,  5322,
                        5362,  5414,  5461,  5524,  5681,  5828,  5877,  6031,  6123,  6249,  6298,
                        6575,  6626,  6637,  6692,  6708,  6765,  6926,  6953,  7061,  7089,  7144,
                        7147,  7184,  7342,  7461,  7615,  7703,  7818,  8002,  8014,  8280,  8369,
                        8398,  8440,  8554,  8675,  8682,  8780,  9064,  9379,  9448,  9455,  9639,
                        10036, 10124, 10164, 10224, 10246, 10568, 10736, 10750, 10914, 10930, 10944,
                        10970, 11149, 11434, 11435, 11534, 11862, 11961, 12187, 12247, 12344, 12479,
                        12632, 12923, 13015, 13018, 13122, 13277, 13357, 13459, 13466, 13597, 13792,
                        13857, 13952, 14096, 14127, 14211, 14221, 14344, 14563, 14567, 14588, 14606,
                        14692, 14868, 14880, 14990, 15085, 15101, 15211, 15218, 15439, 15530, 15564,
                        15676, 15695, 15727, 15754, 15846, 15895, 15904, 15983, 16004, 16299, 16423,
                        16476, 16530, 16954, 17045, 17202, 17393, 17592, 17693, 17829, 17852, 18018,
                        18224, 18335, 18881, 18942, 19162, 19387, 19401, 19418, 19434, 19525, 19710,
                        19805, 20054, 20126, 20127, 20407, 20572, 20742, 20929, 21023, 21024, 21248,
                        21267, 21354, 21452, 21704, 21810, 21831, 21847, 21900, 22202, 22328, 22599,
                        22629, 22671, 22761, 22762, 22824, 23139, 23478, 23784, 23797, 23884, 23886,
                        23983, 24128, 24137, 24176, 24253, 24434, 24484, 24518, 24538, 24655, 24849,
                        24853, 24865, 24888, 25163, 25256, 25274, 25307, 25613, 25816, 26225, 26323,
                        26459, 26461, 26476, 26580, 26598, 26800, 26932, 26962, 27202, 27499, 27506,
                        27768, 27923, 28049, 28133, 28305, 28468, 28535, 28670, 28717, 28782, 29154,
                        29692, 29742, 30112, 30125, 30289, 30353, 30437, 30734, 30741, 30848, 30933,
                        31332, 31399, 31581, 31841, 31867, 32025, 32446, 32463, 32712, 32947, 33038,
                        33210, 33325, 33563, 33572, 33757, 33947, 33975, 34016, 34041, 34210, 34627,
                        34684, 34732, 35064, 35684, 35787, 35809, 35811, 35996, 36272, 36389, 36418,
                        36420, 36568, 36847, 36956, 37022, 37189, 37200, 37401, 37484, 37581, 37852,
                        37939, 38156, 38269, 38785, 38874, 39072, 39081, 39094, 39157, 39187, 39308,
                        39562, 39676, 39690, 39814, 39848, 40134, 40350, 40352, 40684, 41143, 41249,
                        41416, 41463, 41738, 41840, 41875, 42028, 42077, 42104, 42439, 42467, 42528,
                        42784, 42793, 42970, 43020, 43418, 43430, 43571, 43809, 43811, 44040, 44057,
                        44081, 44168, 44288, 44329, 44608, 44624, 44690};
                test_read_index_file(meta_arm_old_v2, data_dir, "arm_old_v2", field_name_avx2,
                                     default_query_term, InvertedIndexStorageFormatPB::V2, false,
                                     expected_gif_cardinality, expected_gif_doc_ids);
            }
            {
                TabletIndex meta_arm_old_v1 =
                        create_test_index_meta(1744016478651, "request_idx", 2);
                std::string field_name_avx2 = "request";
                default_query_term = "gif";
                expected_gif_cardinality = 40962;
                expected_gif_doc_ids = {0, 21, 110, 1000, 2581, 7196, 9091, 16712, 26132, 44702};
                test_read_index_file(meta_arm_old_v1, data_dir, "arm_old", field_name_avx2,
                                     default_query_term, InvertedIndexStorageFormatPB::V1, false,
                                     expected_gif_cardinality, expected_gif_doc_ids);
                default_query_term = "/english/index.html";
                expected_gif_cardinality = 402;
                expected_gif_doc_ids = {
                        36,    41,    242,   628,   716,   741,   884,   902,   1025,  1129,  1349,
                        1401,  1871,  1873,  2074,  2184,  2420,  2815,  3138,  3164,  3189,  3302,
                        3308,  3347,  3430,  3475,  3645,  3772,  3803,  3921,  4036,  4080,  4127,
                        4419,  4424,  4450,  4526,  4546,  4608,  4668,  4701,  5223,  5274,  5366,
                        5438,  5670,  6109,  6176,  6386,  6412,  6466,  6554,  6594,  6761,  6941,
                        6957,  7076,  7173,  7178,  7208,  7263,  7370,  7489,  7726,  7800,  8293,
                        8309,  8469,  8588,  8759,  8914,  9242,  9254,  9334,  9354,  9422,  9476,
                        9515,  9545,  9709,  9714,  9741,  9982,  9995,  10145, 10284, 10384, 10464,
                        10508, 10641, 10720, 10771, 10810, 10935, 11097, 11367, 11525, 11554, 11574,
                        11660, 11857, 11930, 12025, 12078, 12203, 12237, 12245, 12297, 12432, 12466,
                        12601, 12745, 12893, 12928, 13127, 13157, 13173, 13336, 13458, 13517, 13553,
                        13681, 13747, 13893, 13935, 14108, 14191, 14265, 14408, 14439, 14468, 14528,
                        14565, 14587, 14618, 14642, 14993, 15010, 15260, 15358, 15453, 15539, 15557,
                        15586, 15594, 15728, 15893, 15904, 16156, 16304, 16408, 16532, 16789, 16974,
                        17015, 17294, 17330, 17347, 17733, 17773, 17981, 17992, 18015, 18209, 18211,
                        18278, 18566, 18603, 18643, 18912, 19327, 19419, 19538, 19700, 19714, 19872,
                        19873, 19895, 19971, 20118, 20379, 20515, 20526, 20781, 20967, 21108, 21163,
                        21179, 21431, 21474, 21595, 21749, 21822, 21848, 21999, 22314, 22476, 22539,
                        22677, 23070, 23071, 23491, 23841, 23986, 24017, 24109, 24139, 24196, 24301,
                        24355, 24742, 24965, 24970, 24987, 25254, 25268, 25287, 25331, 26050, 26133,
                        26238, 26364, 26388, 26435, 26804, 26844, 26849, 26934, 27190, 27294, 27441,
                        27467, 27679, 27702, 27762, 27772, 27821, 27844, 27860, 27912, 28068, 28115,
                        28301, 28304, 28379, 28440, 28816, 28885, 28948, 28966, 29348, 29484, 29509,
                        29902, 29908, 29917, 29951, 30127, 30181, 30693, 30779, 30861, 30903, 31061,
                        31358, 31646, 31658, 31713, 31782, 31815, 31905, 31967, 32019, 32333, 32376,
                        32394, 32452, 32635, 32709, 32973, 33505, 33506, 33602, 33693, 33751, 33793,
                        33942, 33993, 34106, 34413, 34508, 34526, 34798, 34974, 34999, 35033, 35106,
                        35159, 35200, 35288, 35305, 35355, 35373, 35522, 35583, 35602, 35716, 35956,
                        36022, 36035, 36264, 36315, 36359, 36525, 36601, 36616, 36627, 36677, 36939,
                        36970, 37050, 37139, 37218, 37287, 37445, 37467, 37502, 37521, 37552, 37635,
                        37705, 37737, 37786, 37855, 38242, 38410, 38790, 38881, 39036, 39051, 39103,
                        39123, 39165, 39195, 39373, 39425, 39464, 39476, 39499, 39627, 39657, 39754,
                        39804, 40029, 40510, 40651, 40660, 40745, 40974, 41163, 41275, 41515, 41847,
                        41931, 42030, 42174, 42385, 42448, 42462, 43183, 43243, 43279, 43417, 43645,
                        43698, 44144, 44425, 44430, 44625, 44739, 44849, 44993, 45335, 45343, 45561,
                        45594, 45734, 45978, 46070, 46162, 46378, 46449, 46704, 46833, 47257, 47268,
                        47548, 47984, 47990, 48101, 48545, 48661};
                test_read_index_file(meta_arm_old_v1, data_dir, "arm_old", field_name_avx2,
                                     default_query_term, InvertedIndexStorageFormatPB::V1, false,
                                     expected_gif_cardinality, expected_gif_doc_ids);
                {
                    TabletIndex meta_x86_old_v2 = create_test_index_meta(10083, "request_idx", 2);
                    std::string field_name_avx2 = "2";
                    default_query_term = "gif";
                    expected_gif_cardinality = 37343;
                    expected_gif_doc_ids = {0,    19,   110,   1000,  2581,
                                            7196, 9090, 16711, 10278, 44702};
                    test_read_index_file(meta_x86_old_v2, data_dir, "x86_old_v2", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V2, true,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 346;
                    expected_gif_doc_ids = {
                            3,     222,   502,   649,   671,   814,   1101,  1110,  1286,  1329,
                            1350,  1409,  1478,  1598,  1621,  1627,  1686,  1895,  2218,  2304,
                            2429,  2654,  2735,  2798,  2799,  2828,  2966,  3050,  3083,  3261,
                            3296,  3574,  3625,  3653,  4053,  4128,  4192,  4200,  4594,  4623,
                            4747,  5284,  5371,  5379,  5467,  5567,  5694,  5714,  5723,  5903,
                            5954,  6120,  6187,  6226,  6451,  6664,  6723,  6748,  6958,  7319,
                            7933,  7947,  8041,  8156,  8203,  8205,  8568,  8626,  8777,  8923,
                            8999,  9088,  9193,  9239,  9282,  9358,  9386,  9531,  9589,  9599,
                            9864,  10006, 10229, 10370, 10523, 10751, 10854, 10864, 10883, 11045,
                            11077, 11134, 11149, 11252, 11258, 11260, 11432, 11488, 11578, 11599,
                            11765, 11826, 11929, 12124, 12154, 12277, 12339, 12410, 12432, 12500,
                            12612, 12618, 12654, 12872, 12929, 12987, 13173, 13293, 13306, 13397,
                            13559, 13800, 14017, 14180, 14195, 14283, 14385, 14481, 14659, 14728,
                            14738, 15150, 15574, 15586, 15774, 15914, 15968, 16093, 16131, 16155,
                            16337, 16340, 16391, 16420, 16577, 16632, 16836, 16874, 16883, 16896,
                            16954, 17060, 17241, 17302, 17359, 17601, 17985, 18017, 18043, 18084,
                            18334, 18539, 18637, 18831, 18864, 19068, 19075, 19140, 19445, 19487,
                            19495, 19559, 19648, 19656, 19770, 19880, 20284, 20311, 20358, 20439,
                            21103, 21252, 21382, 21429, 21678, 21765, 21773, 21779, 21877, 22067,
                            22318, 22607, 22713, 22719, 22929, 23074, 23148, 23209, 23500, 23611,
                            23614, 23709, 23761, 23952, 23999, 24120, 24217, 24503, 24656, 24675,
                            24842, 24924, 24970, 25144, 25582, 25767, 25923, 26184, 26206, 26344,
                            26376, 26529, 26682, 26686, 26803, 26896, 26921, 26951, 26982, 27033,
                            27075, 27163, 27166, 27299, 27567, 27682, 28010, 28173, 28368, 28423,
                            28440, 28590, 28801, 28990, 28994, 29138, 29256, 29300, 29657, 29769,
                            30018, 30086, 30154, 30189, 30382, 30385, 30445, 30456, 30489, 30545,
                            30908, 30931, 31009, 31267, 31297, 31336, 31696, 31728, 31735, 31943,
                            32155, 32244, 32342, 32431, 32569, 32733, 32799, 32817, 32903, 33078,
                            33552, 34064, 34604, 34705, 35186, 35256, 35284, 35295, 35494, 35745,
                            35943, 36051, 36343, 36430, 36452, 36666, 36697, 36763, 36822, 36890,
                            37511, 37547, 37706, 38256, 38581, 38911, 38931, 38955, 38998, 39131,
                            39135, 39255, 39312, 39394, 39459, 39635, 39707, 40190, 40215, 40708,
                            41063, 41264, 41361, 41593, 41699, 41864, 42190, 42363, 42444, 42873,
                            42983, 43314, 43587, 43693, 43880, 43908, 43909, 43925, 43978, 43986,
                            44071, 44183, 44340, 44398, 44466, 44498};
                    test_read_index_file(meta_x86_old_v2, data_dir, "x86_old_v2", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V2, true,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                }
                {
                    TabletIndex meta_x86_old_v1 = create_test_index_meta(10248, "request_idx", 2);
                    std::string field_name_avx2 = "request";
                    default_query_term = "gif";
                    expected_gif_cardinality = 40893;
                    expected_gif_doc_ids = {0,    19,   110,   1001,  2581,
                                            7196, 9090, 16711, 10278, 44701};
                    test_read_index_file(meta_x86_old_v1, data_dir, "x86_old", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V1, true,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 356;
                    expected_gif_doc_ids = {
                            622,   754,   1021,  1186,  1403,  1506,  1655,  1661,  1833,  2287,
                            2356,  2425,  2849,  3198,  3350,  3365,  3416,  3423,  3499,  3541,
                            3609,  3682,  3936,  4117,  4198,  4589,  4591,  4808,  4959,  5282,
                            5332,  5495,  5560,  5624,  5773,  5831,  6138,  6180,  6361,  6372,
                            6621,  6777,  6878,  6911,  6983,  7048,  7148,  7207,  7273,  7274,
                            7385,  7545,  7735,  7904,  7912,  8150,  8215,  8238,  8363,  8598,
                            8672,  8765,  8877,  9188,  9264,  9761,  9864,  9866,  9946,  10022,
                            10139, 10143, 10146, 10184, 10291, 10304, 10308, 10332, 10371, 10695,
                            10707, 11056, 11095, 11111, 11505, 11752, 11860, 11989, 12119, 12156,
                            12655, 12764, 12792, 13055, 13636, 13824, 13902, 13912, 14061, 14152,
                            14315, 14355, 14618, 14712, 14788, 15050, 15057, 15110, 15122, 15249,
                            15267, 15281, 15735, 15848, 15939, 16117, 16327, 16331, 16597, 16739,
                            16868, 17092, 17458, 17553, 17602, 17664, 17781, 18061, 18353, 18397,
                            18468, 18717, 18726, 19131, 19209, 19402, 19551, 19812, 20128, 20146,
                            20232, 20322, 20407, 20431, 20436, 20466, 20757, 20960, 20994, 21197,
                            21254, 21487, 21561, 21602, 21662, 21710, 21754, 21826, 21965, 22091,
                            22200, 22203, 22291, 22317, 22561, 22584, 22606, 22950, 23140, 23315,
                            23442, 23858, 24026, 24322, 24581, 24617, 24655, 24756, 24974, 25191,
                            25246, 25287, 25406, 25599, 25830, 26020, 26109, 26149, 26402, 26431,
                            26451, 26458, 26495, 26766, 26777, 26848, 26966, 27053, 27089, 27177,
                            27519, 27595, 27693, 28294, 28719, 28755, 29073, 29323, 29472, 29496,
                            29604, 29761, 29772, 29953, 30030, 30083, 30139, 30210, 30719, 30774,
                            30868, 30897, 31200, 31347, 31811, 31880, 31903, 32040, 32048, 32225,
                            32335, 32357, 32517, 32579, 32679, 32821, 33294, 33393, 33509, 33675,
                            33802, 34390, 34441, 34474, 34547, 34557, 35057, 35262, 35327, 35348,
                            35455, 35482, 35668, 35811, 35845, 35953, 36098, 36151, 36602, 36711,
                            36946, 37036, 37220, 37291, 37436, 37721, 37747, 37864, 37890, 37923,
                            38045, 38588, 38654, 38730, 38930, 39169, 39814, 40401, 40689, 40762,
                            40822, 41249, 41399, 41419, 41572, 41736, 41768, 41946, 41989, 42077,
                            42079, 42225, 42360, 42524, 42576, 42595, 42691, 42784, 42892, 42930,
                            43210, 43299, 43348, 43468, 43510, 43622, 43795, 43824, 43893, 43972,
                            43975, 43998, 44008, 44023, 44031, 44049, 44139, 44518, 44555, 44597,
                            44815, 44879, 45014, 45020, 45054, 45084, 45100, 45464, 45471, 45505,
                            45580, 45593, 45686, 45991, 46019, 46021, 46107, 46138, 46197, 46209,
                            46551, 46658, 46988, 47027, 47046, 47071, 47106, 47190, 47225, 47439,
                            47465, 47531, 47602, 47660, 48453, 48575};
                    test_read_index_file(meta_x86_old_v1, data_dir, "x86_old", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V1, true,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                }
                {
                    TabletIndex meta_x86_new_v1 =
                            create_test_index_meta(1744025019684, "request_idx", 2);
                    std::string field_name_avx2 = "request";
                    default_query_term = "gif";
                    expected_gif_cardinality = 37337;
                    expected_gif_doc_ids = {0,    19,   110,   1001,  2581,
                                            7196, 9090, 16711, 10279, 44701};
                    test_read_index_file(meta_x86_new_v1, data_dir, "x86_new", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V1,
                                         false, expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 368;
                    expected_gif_doc_ids = {
                            294,   678,   835,   852,   998,   1204,  1237,  1553,  1648,  1674,
                            1859,  1944,  2024,  2043,  2319,  2383,  2476,  2955,  3064,  3281,
                            3292,  3324,  3341,  3389,  3424,  3713,  3715,  3731,  3794,  3801,
                            3824,  3892,  4089,  4174,  4302,  4436,  4459,  4509,  4697,  4726,
                            4891,  4931,  4975,  5008,  5020,  5154,  5200,  5288,  5375,  5458,
                            5624,  5728,  5844,  5864,  6306,  6452,  6461,  6619,  6632,  6755,
                            7021,  7093,  7400,  7414,  7422,  7484,  7752,  7758,  7785,  7900,
                            7992,  8013,  8019,  8075,  8076,  8118,  8119,  8123,  8126,  8248,
                            8345,  8557,  8772,  8900,  8946,  8966,  8974,  9583,  9597,  9613,
                            9782,  9869,  10033, 10162, 10271, 10297, 10439, 10520, 10558, 10591,
                            10651, 10807, 10810, 10864, 10906, 11293, 11499, 11511, 11572, 11574,
                            11665, 11697, 11722, 11729, 11801, 11845, 11868, 12031, 12251, 12289,
                            12323, 12337, 12576, 12804, 12938, 13349, 13459, 13509, 13558, 13938,
                            13951, 13989, 14006, 14237, 14362, 14365, 14508, 14560, 14658, 14666,
                            14954, 15155, 15216, 15314, 15430, 15532, 15567, 15689, 15848, 15978,
                            15983, 15985, 16119, 16174, 16193, 16506, 16543, 17048, 17078, 17190,
                            17351, 17412, 17444, 17475, 17761, 17950, 17996, 18195, 18275, 18405,
                            18637, 18780, 19245, 19445, 19604, 19744, 19763, 20284, 20308, 20530,
                            20762, 20782, 20792, 20818, 20867, 20959, 21104, 21207, 21255, 21280,
                            21339, 21514, 21870, 21966, 22231, 22275, 22391, 22478, 22509, 22637,
                            22942, 22984, 23121, 23269, 23362, 23572, 23589, 23832, 23919, 24043,
                            24078, 24126, 24244, 24364, 24405, 24454, 24782, 24794, 24833, 24949,
                            24980, 24989, 25034, 25166, 25358, 25443, 25553, 25600, 25634, 25900,
                            26054, 26105, 26196, 26218, 26241, 26532, 26637, 26918, 27179, 27207,
                            27258, 27463, 27604, 27614, 27624, 27669, 27837, 27841, 28025, 28172,
                            28181, 28214, 28391, 28554, 28785, 28812, 28893, 29063, 29665, 29810,
                            29900, 30236, 30256, 30313, 30357, 30447, 30945, 30965, 30997, 31012,
                            31033, 31302, 31309, 31806, 31821, 31904, 32080, 32128, 32330, 32359,
                            32426, 32430, 32507, 32580, 32588, 32711, 32767, 32835, 32841, 32903,
                            33094, 33226, 33313, 33371, 33615, 33742, 33808, 34480, 34571, 34874,
                            34989, 35189, 35234, 35241, 35258, 35742, 35793, 36207, 36208, 36214,
                            36735, 36915, 37041, 37286, 37391, 37433, 37454, 37480, 37493, 37504,
                            37695, 37761, 37769, 38027, 38038, 38113, 38285, 38343, 38596, 38625,
                            38758, 38874, 38944, 39045, 39346, 39390, 39432, 39670, 40012, 40068,
                            40342, 40826, 41087, 41206, 41502, 41700, 42215, 42251, 42373, 42413,
                            42475, 42482, 42490, 42506, 42594, 42656, 42665, 43075, 43147, 43366,
                            43488, 43499, 43609, 43889, 43925, 44040, 44180, 44568};
                    test_read_index_file(meta_x86_new_v1, data_dir, "x86_new", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V1,
                                         false, expected_gif_cardinality, expected_gif_doc_ids);
                }
                {
                    TabletIndex meta_x86_new_v2 =
                            create_test_index_meta(1744025019611, "request_idx", 2);
                    std::string field_name_avx2 = "2";
                    default_query_term = "gif";
                    expected_gif_cardinality = 37170;
                    expected_gif_doc_ids = {0,    18,   110,   1000,  2581,
                                            7196, 9090, 16711, 10276, 44702};
                    test_read_index_file(meta_x86_new_v2, data_dir, "x86_new_v2", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V2, true,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 356;
                    expected_gif_doc_ids = {
                            260,   329,   334,   347,   459,   471,   568,   676,   689,   718,
                            760,   1267,  1421,  1477,  2363,  2523,  2571,  2725,  2941,  3125,
                            3148,  3306,  3459,  3808,  3856,  3933,  4022,  4076,  4386,  4815,
                            4818,  4898,  4938,  4970,  4975,  5192,  5302,  5320,  5417,  5470,
                            5752,  5875,  6007,  6143,  6425,  6597,  6639,  6761,  6961,  6977,
                            6983,  7045,  7179,  7214,  7350,  7393,  7436,  7485,  7518,  7592,
                            7739,  7856,  7921,  7957,  8006,  8116,  8411,  8664,  8716,  8728,
                            8747,  8809,  8883,  8907,  8931,  8995,  9089,  9393,  9611,  9746,
                            9787,  9963,  10080, 10230, 10348, 10464, 10494, 10547, 10552, 10666,
                            10813, 10847, 10989, 11134, 11298, 11531, 11605, 11654, 11720, 11791,
                            11835, 11994, 12012, 12068, 12232, 12272, 12336, 12438, 12537, 12646,
                            12738, 12768, 12923, 12925, 13173, 13186, 13187, 13251, 13503, 13830,
                            13973, 14121, 14291, 14378, 14380, 14389, 14453, 14495, 14508, 14620,
                            14686, 14872, 15241, 15275, 15491, 15564, 15652, 15951, 15966, 16287,
                            16289, 16531, 16681, 16914, 16919, 17079, 17382, 17393, 17860, 17961,
                            18158, 18191, 18578, 18692, 18741, 18987, 19038, 19117, 19271, 19641,
                            19723, 20253, 20259, 20473, 20766, 20863, 21419, 21424, 21908, 22325,
                            22327, 22449, 22701, 22852, 22867, 22906, 22912, 22958, 23175, 23203,
                            23332, 23461, 23493, 23746, 23921, 24257, 24328, 24411, 24479, 24747,
                            24816, 25462, 25492, 25528, 25872, 25944, 26164, 26414, 26463, 26688,
                            26779, 27033, 27283, 27303, 27858, 27948, 28248, 28372, 28402, 28460,
                            28478, 28897, 29019, 29053, 29140, 29216, 29299, 29393, 29414, 29575,
                            29789, 29803, 29805, 29934, 30270, 30278, 30291, 30301, 30433, 30493,
                            30698, 30723, 30737, 30751, 31015, 31167, 31447, 32136, 32138, 32296,
                            32318, 32374, 32585, 32747, 32815, 32964, 33060, 33144, 33159, 33315,
                            33342, 33543, 33753, 33767, 33990, 34176, 34375, 34422, 34455, 34538,
                            34563, 34708, 34738, 35050, 35130, 35137, 35220, 35422, 35484, 35487,
                            35603, 35697, 35717, 35986, 36114, 36116, 36230, 36288, 36332, 36469,
                            36520, 36572, 36727, 36959, 37099, 37152, 37400, 37473, 37712, 37838,
                            37920, 38264, 38354, 38431, 38646, 38692, 38757, 38888, 38909, 38945,
                            39078, 39103, 39125, 39138, 39155, 39274, 39412, 39553, 39577, 39583,
                            39653, 39706, 39895, 39934, 39978, 40023, 40154, 40250, 40259, 40310,
                            40357, 40376, 40457, 40643, 40665, 40881, 40990, 41368, 41379, 41519,
                            41578, 41641, 41680, 42260, 42357, 42391, 42461, 42561, 42575, 42781,
                            42810, 42844, 43026, 43028, 43046, 43145, 43386, 43388, 43576, 43667,
                            43798, 43983, 44280, 44453, 44591, 44634};
                    test_read_index_file(meta_x86_new_v2, data_dir, "x86_new_v2", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V2, true,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                }
            }
        } else {
            // Test x86 architecture cases
            std::cout << "Testing on x86 architecture" << std::endl;

            if (has_avx2_support()) {
                std::cout << "Testing with AVX2 support" << std::endl;
                {
                    TabletIndex meta_arm_old_v2 =
                            create_test_index_meta(1744016478578, "request_idx", 2);
                    std::string field_name_avx2 = "2";
                    default_query_term = "gif";
                    expected_gif_cardinality = 37284;
                    expected_gif_doc_ids = {0,    19,   110,   1000,  2581,
                                            7197, 9091, 16711, 29676, 44702};
                    test_read_index_file(meta_arm_old_v2, data_dir, "arm_old_v2", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V2, true,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 359;
                    expected_gif_doc_ids = {
                            25,    63,    66,    135,   214,   276,   287,   321,   653,   819,
                            968,   1038,  1115,  1210,  1305,  1394,  1650,  1690,  1761,  1934,
                            1935,  2101,  2114,  2544,  2815,  2912,  3028,  3104,  3475,  3953,
                            3991,  4052,  4097,  4424,  4430,  4458,  4504,  4571,  4629,  4704,
                            4711,  4838,  5021,  5322,  5362,  5414,  5461,  5524,  5681,  5828,
                            5877,  6031,  6123,  6249,  6298,  6575,  6626,  6637,  6692,  6708,
                            6765,  6926,  6953,  7061,  7089,  7144,  7147,  7184,  7342,  7461,
                            7615,  7703,  7818,  8002,  8014,  8280,  8369,  8398,  8440,  8554,
                            8675,  8682,  8780,  9064,  9379,  9448,  9455,  9639,  10036, 10124,
                            10164, 10224, 10246, 10568, 10736, 10750, 10914, 10930, 10944, 10970,
                            11149, 11434, 11435, 11534, 11862, 11961, 12187, 12247, 12344, 12479,
                            12632, 12923, 13015, 13018, 13122, 13277, 13357, 13459, 13466, 13597,
                            13792, 13857, 13952, 14096, 14127, 14211, 14221, 14344, 14563, 14567,
                            14588, 14606, 14692, 14868, 14880, 14990, 15085, 15101, 15211, 15218,
                            15439, 15530, 15564, 15676, 15695, 15727, 15754, 15846, 15895, 15904,
                            15983, 16004, 16299, 16423, 16476, 16530, 16954, 17045, 17202, 17393,
                            17592, 17693, 17829, 17852, 18018, 18224, 18335, 18881, 18942, 19162,
                            19387, 19401, 19418, 19434, 19525, 19710, 19805, 20054, 20126, 20127,
                            20407, 20572, 20742, 20929, 21023, 21024, 21248, 21267, 21354, 21452,
                            21704, 21810, 21831, 21847, 21900, 22202, 22328, 22599, 22629, 22671,
                            22761, 22762, 22824, 23139, 23478, 23784, 23797, 23884, 23886, 23983,
                            24128, 24137, 24176, 24253, 24434, 24484, 24518, 24538, 24655, 24849,
                            24853, 24865, 24888, 25163, 25256, 25274, 25307, 25613, 25816, 26225,
                            26323, 26459, 26461, 26476, 26580, 26598, 26800, 26932, 26962, 27202,
                            27499, 27506, 27768, 27923, 28049, 28133, 28305, 28468, 28535, 28670,
                            28717, 28782, 29154, 29692, 29742, 30112, 30125, 30289, 30353, 30437,
                            30734, 30741, 30848, 30933, 31332, 31399, 31581, 31841, 31867, 32025,
                            32446, 32463, 32712, 32947, 33038, 33210, 33325, 33563, 33572, 33757,
                            33947, 33975, 34016, 34041, 34210, 34627, 34684, 34732, 35064, 35684,
                            35787, 35809, 35811, 35996, 36272, 36389, 36418, 36420, 36568, 36847,
                            36956, 37022, 37189, 37200, 37401, 37484, 37581, 37852, 37939, 38156,
                            38269, 38785, 38874, 39072, 39081, 39094, 39157, 39187, 39308, 39562,
                            39676, 39690, 39814, 39848, 40134, 40350, 40352, 40684, 41143, 41249,
                            41416, 41463, 41738, 41840, 41875, 42028, 42077, 42104, 42439, 42467,
                            42528, 42784, 42793, 42970, 43020, 43418, 43430, 43571, 43809, 43811,
                            44040, 44057, 44081, 44168, 44288, 44329, 44608, 44624, 44690};
                    test_read_index_file(meta_arm_old_v2, data_dir, "arm_old_v2", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V2, true,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                }
                {
                    TabletIndex meta_arm_old_v1 =
                            create_test_index_meta(1744016478651, "request_idx", 2);
                    std::string field_name_avx2 = "request";
                    default_query_term = "gif";
                    expected_gif_cardinality = 40962;
                    expected_gif_doc_ids = {0,    21,   110,   1000,  2581,
                                            7196, 9091, 16712, 26132, 44702};
                    test_read_index_file(meta_arm_old_v1, data_dir, "arm_old", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V1, true,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 402;
                    expected_gif_doc_ids = {
                            36,    41,    242,   628,   716,   741,   884,   902,   1025,  1129,
                            1349,  1401,  1871,  1873,  2074,  2184,  2420,  2815,  3138,  3164,
                            3189,  3302,  3308,  3347,  3430,  3475,  3645,  3772,  3803,  3921,
                            4036,  4080,  4127,  4419,  4424,  4450,  4526,  4546,  4608,  4668,
                            4701,  5223,  5274,  5366,  5438,  5670,  6109,  6176,  6386,  6412,
                            6466,  6554,  6594,  6761,  6941,  6957,  7076,  7173,  7178,  7208,
                            7263,  7370,  7489,  7726,  7800,  8293,  8309,  8469,  8588,  8759,
                            8914,  9242,  9254,  9334,  9354,  9422,  9476,  9515,  9545,  9709,
                            9714,  9741,  9982,  9995,  10145, 10284, 10384, 10464, 10508, 10641,
                            10720, 10771, 10810, 10935, 11097, 11367, 11525, 11554, 11574, 11660,
                            11857, 11930, 12025, 12078, 12203, 12237, 12245, 12297, 12432, 12466,
                            12601, 12745, 12893, 12928, 13127, 13157, 13173, 13336, 13458, 13517,
                            13553, 13681, 13747, 13893, 13935, 14108, 14191, 14265, 14408, 14439,
                            14468, 14528, 14565, 14587, 14618, 14642, 14993, 15010, 15260, 15358,
                            15453, 15539, 15557, 15586, 15594, 15728, 15893, 15904, 16156, 16304,
                            16408, 16532, 16789, 16974, 17015, 17294, 17330, 17347, 17733, 17773,
                            17981, 17992, 18015, 18209, 18211, 18278, 18566, 18603, 18643, 18912,
                            19327, 19419, 19538, 19700, 19714, 19872, 19873, 19895, 19971, 20118,
                            20379, 20515, 20526, 20781, 20967, 21108, 21163, 21179, 21431, 21474,
                            21595, 21749, 21822, 21848, 21999, 22314, 22476, 22539, 22677, 23070,
                            23071, 23491, 23841, 23986, 24017, 24109, 24139, 24196, 24301, 24355,
                            24742, 24965, 24970, 24987, 25254, 25268, 25287, 25331, 26050, 26133,
                            26238, 26364, 26388, 26435, 26804, 26844, 26849, 26934, 27190, 27294,
                            27441, 27467, 27679, 27702, 27762, 27772, 27821, 27844, 27860, 27912,
                            28068, 28115, 28301, 28304, 28379, 28440, 28816, 28885, 28948, 28966,
                            29348, 29484, 29509, 29902, 29908, 29917, 29951, 30127, 30181, 30693,
                            30779, 30861, 30903, 31061, 31358, 31646, 31658, 31713, 31782, 31815,
                            31905, 31967, 32019, 32333, 32376, 32394, 32452, 32635, 32709, 32973,
                            33505, 33506, 33602, 33693, 33751, 33793, 33942, 33993, 34106, 34413,
                            34508, 34526, 34798, 34974, 34999, 35033, 35106, 35159, 35200, 35288,
                            35305, 35355, 35373, 35522, 35583, 35602, 35716, 35956, 36022, 36035,
                            36264, 36315, 36359, 36525, 36601, 36616, 36627, 36677, 36939, 36970,
                            37050, 37139, 37218, 37287, 37445, 37467, 37502, 37521, 37552, 37635,
                            37705, 37737, 37786, 37855, 38242, 38410, 38790, 38881, 39036, 39051,
                            39103, 39123, 39165, 39195, 39373, 39425, 39464, 39476, 39499, 39627,
                            39657, 39754, 39804, 40029, 40510, 40651, 40660, 40745, 40974, 41163,
                            41275, 41515, 41847, 41931, 42030, 42174, 42385, 42448, 42462, 43183,
                            43243, 43279, 43417, 43645, 43698, 44144, 44425, 44430, 44625, 44739,
                            44849, 44993, 45335, 45343, 45561, 45594, 45734, 45978, 46070, 46162,
                            46378, 46449, 46704, 46833, 47257, 47268, 47548, 47984, 47990, 48101,
                            48545, 48661};
                    test_read_index_file(meta_arm_old_v1, data_dir, "arm_old", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V1, true,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                }
                {
                    TabletIndex meta_arm_new_v2 =
                            create_test_index_meta(1744017919311, "request_idx", 2);
                    std::string field_name_avx2 = "2";
                    default_query_term = "gif";
                    expected_gif_cardinality = 37170;
                    expected_gif_doc_ids = {0,    18,   110,   1000,  2581,
                                            7196, 9090, 16711, 10276, 44702};
                    test_read_index_file(meta_arm_new_v2, data_dir, "arm_new_v2", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V2,
                                         false, expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 356;
                    expected_gif_doc_ids = {
                            260,   329,   334,   347,   459,   471,   568,   676,   689,   718,
                            760,   1267,  1421,  1477,  2363,  2523,  2571,  2725,  2941,  3125,
                            3148,  3306,  3459,  3808,  3856,  3933,  4022,  4076,  4386,  4815,
                            4818,  4898,  4938,  4970,  4975,  5192,  5302,  5320,  5417,  5470,
                            5752,  5875,  6007,  6143,  6425,  6597,  6639,  6761,  6961,  6977,
                            6983,  7045,  7179,  7214,  7350,  7393,  7436,  7485,  7518,  7592,
                            7739,  7856,  7921,  7957,  8006,  8116,  8411,  8664,  8716,  8728,
                            8747,  8809,  8883,  8907,  8931,  8995,  9089,  9393,  9611,  9746,
                            9787,  9963,  10080, 10230, 10348, 10464, 10494, 10547, 10552, 10666,
                            10813, 10847, 10989, 11134, 11298, 11531, 11605, 11654, 11720, 11791,
                            11835, 11994, 12012, 12068, 12232, 12272, 12336, 12438, 12537, 12646,
                            12738, 12768, 12923, 12925, 13173, 13186, 13187, 13251, 13503, 13830,
                            13973, 14121, 14291, 14378, 14380, 14389, 14453, 14495, 14508, 14620,
                            14686, 14872, 15241, 15275, 15491, 15564, 15652, 15951, 15966, 16287,
                            16289, 16531, 16681, 16914, 16919, 17079, 17382, 17393, 17860, 17961,
                            18158, 18191, 18578, 18692, 18741, 18987, 19038, 19117, 19271, 19641,
                            19723, 20253, 20259, 20473, 20766, 20863, 21419, 21424, 21908, 22325,
                            22327, 22449, 22701, 22852, 22867, 22906, 22912, 22958, 23175, 23203,
                            23332, 23461, 23493, 23746, 23921, 24257, 24328, 24411, 24479, 24747,
                            24816, 25462, 25492, 25528, 25872, 25944, 26164, 26414, 26463, 26688,
                            26779, 27033, 27283, 27303, 27858, 27948, 28248, 28372, 28402, 28460,
                            28478, 28897, 29019, 29053, 29140, 29216, 29299, 29393, 29414, 29575,
                            29789, 29803, 29805, 29934, 30270, 30278, 30291, 30301, 30433, 30493,
                            30698, 30723, 30737, 30751, 31015, 31167, 31447, 32136, 32138, 32296,
                            32318, 32374, 32585, 32747, 32815, 32964, 33060, 33144, 33159, 33315,
                            33342, 33543, 33753, 33767, 33990, 34176, 34375, 34422, 34455, 34538,
                            34563, 34708, 34738, 35050, 35130, 35137, 35220, 35422, 35484, 35487,
                            35603, 35697, 35717, 35986, 36114, 36116, 36230, 36288, 36332, 36469,
                            36520, 36572, 36727, 36959, 37099, 37152, 37400, 37473, 37712, 37838,
                            37920, 38264, 38354, 38431, 38646, 38692, 38757, 38888, 38909, 38945,
                            39078, 39103, 39125, 39138, 39155, 39274, 39412, 39553, 39577, 39583,
                            39653, 39706, 39895, 39934, 39978, 40023, 40154, 40250, 40259, 40310,
                            40357, 40376, 40457, 40643, 40665, 40881, 40990, 41368, 41379, 41519,
                            41578, 41641, 41680, 42260, 42357, 42391, 42461, 42561, 42575, 42781,
                            42810, 42844, 43026, 43028, 43046, 43145, 43386, 43388, 43576, 43667,
                            43798, 43983, 44280, 44453, 44591, 44634};
                    test_read_index_file(meta_arm_new_v2, data_dir, "arm_new_v2", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V2,
                                         false, expected_gif_cardinality, expected_gif_doc_ids);
                }
                {
                    TabletIndex meta_arm_new_v1 =
                            create_test_index_meta(1744017919441, "request_idx", 2);
                    std::string field_name_avx2 = "request";
                    default_query_term = "gif";
                    expected_gif_cardinality = 37343;
                    expected_gif_doc_ids = {0,    21,   110,   1000,  2580,
                                            7195, 9091, 16711, 26131, 44702};
                    test_read_index_file(meta_arm_new_v1, data_dir, "arm_new", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V1,
                                         false, expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 346;
                    expected_gif_doc_ids = {
                            3,     222,   502,   649,   671,   814,   1101,  1110,  1286,  1329,
                            1350,  1409,  1478,  1598,  1621,  1627,  1686,  1895,  2218,  2304,
                            2429,  2654,  2735,  2798,  2799,  2828,  2966,  3050,  3083,  3261,
                            3296,  3574,  3625,  3653,  4053,  4128,  4192,  4200,  4594,  4623,
                            4747,  5284,  5371,  5379,  5467,  5567,  5694,  5714,  5723,  5903,
                            5954,  6120,  6187,  6226,  6451,  6664,  6723,  6748,  6958,  7319,
                            7933,  7947,  8041,  8156,  8203,  8205,  8568,  8626,  8777,  8923,
                            8999,  9088,  9193,  9239,  9282,  9358,  9386,  9531,  9589,  9599,
                            9864,  10006, 10229, 10370, 10523, 10751, 10854, 10864, 10883, 11045,
                            11077, 11134, 11149, 11252, 11258, 11260, 11432, 11488, 11578, 11599,
                            11765, 11826, 11929, 12124, 12154, 12277, 12339, 12410, 12432, 12500,
                            12612, 12618, 12654, 12872, 12929, 12987, 13173, 13293, 13306, 13397,
                            13559, 13800, 14017, 14180, 14195, 14283, 14385, 14481, 14659, 14728,
                            14738, 15150, 15574, 15586, 15774, 15914, 15968, 16093, 16131, 16155,
                            16337, 16340, 16391, 16420, 16577, 16632, 16836, 16874, 16883, 16896,
                            16954, 17060, 17241, 17302, 17359, 17601, 17985, 18017, 18043, 18084,
                            18334, 18539, 18637, 18831, 18864, 19068, 19075, 19140, 19445, 19487,
                            19495, 19559, 19648, 19656, 19770, 19880, 20284, 20311, 20358, 20439,
                            21103, 21252, 21382, 21429, 21678, 21765, 21773, 21779, 21877, 22067,
                            22318, 22607, 22713, 22719, 22929, 23074, 23148, 23209, 23500, 23611,
                            23614, 23709, 23761, 23952, 23999, 24120, 24217, 24503, 24656, 24675,
                            24842, 24924, 24970, 25144, 25582, 25767, 25923, 26184, 26206, 26344,
                            26376, 26529, 26682, 26686, 26803, 26896, 26921, 26951, 26982, 27033,
                            27075, 27163, 27166, 27299, 27567, 27682, 28010, 28173, 28368, 28423,
                            28440, 28590, 28801, 28990, 28994, 29138, 29256, 29300, 29657, 29769,
                            30018, 30086, 30154, 30189, 30382, 30385, 30445, 30456, 30489, 30545,
                            30908, 30931, 31009, 31267, 31297, 31336, 31696, 31728, 31735, 31943,
                            32155, 32244, 32342, 32431, 32569, 32733, 32799, 32817, 32903, 33078,
                            33552, 34064, 34604, 34705, 35186, 35256, 35284, 35295, 35494, 35745,
                            35943, 36051, 36343, 36430, 36452, 36666, 36697, 36763, 36822, 36890,
                            37511, 37547, 37706, 38256, 38581, 38911, 38931, 38955, 38998, 39131,
                            39135, 39255, 39312, 39394, 39459, 39635, 39707, 40190, 40215, 40708,
                            41063, 41264, 41361, 41593, 41699, 41864, 42190, 42363, 42444, 42873,
                            42983, 43314, 43587, 43693, 43880, 43908, 43909, 43925, 43978, 43986,
                            44071, 44183, 44340, 44398, 44466, 44498};
                    test_read_index_file(meta_arm_new_v1, data_dir, "arm_new", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V1,
                                         false, expected_gif_cardinality, expected_gif_doc_ids);
                }
                {
                    TabletIndex meta_x86_old_v2 = create_test_index_meta(10083, "request_idx", 2);
                    std::string field_name_avx2 = "2";
                    default_query_term = "gif";
                    expected_gif_cardinality = 37343;
                    expected_gif_doc_ids = {0,    19,   110,   1000,  2581,
                                            7196, 9090, 16711, 10278, 44702};
                    test_read_index_file(meta_x86_old_v2, data_dir, "x86_old_v2", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V2,
                                         false, expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 346;
                    expected_gif_doc_ids = {
                            3,     222,   502,   649,   671,   814,   1101,  1110,  1286,  1329,
                            1350,  1409,  1478,  1598,  1621,  1627,  1686,  1895,  2218,  2304,
                            2429,  2654,  2735,  2798,  2799,  2828,  2966,  3050,  3083,  3261,
                            3296,  3574,  3625,  3653,  4053,  4128,  4192,  4200,  4594,  4623,
                            4747,  5284,  5371,  5379,  5467,  5567,  5694,  5714,  5723,  5903,
                            5954,  6120,  6187,  6226,  6451,  6664,  6723,  6748,  6958,  7319,
                            7933,  7947,  8041,  8156,  8203,  8205,  8568,  8626,  8777,  8923,
                            8999,  9088,  9193,  9239,  9282,  9358,  9386,  9531,  9589,  9599,
                            9864,  10006, 10229, 10370, 10523, 10751, 10854, 10864, 10883, 11045,
                            11077, 11134, 11149, 11252, 11258, 11260, 11432, 11488, 11578, 11599,
                            11765, 11826, 11929, 12124, 12154, 12277, 12339, 12410, 12432, 12500,
                            12612, 12618, 12654, 12872, 12929, 12987, 13173, 13293, 13306, 13397,
                            13559, 13800, 14017, 14180, 14195, 14283, 14385, 14481, 14659, 14728,
                            14738, 15150, 15574, 15586, 15774, 15914, 15968, 16093, 16131, 16155,
                            16337, 16340, 16391, 16420, 16577, 16632, 16836, 16874, 16883, 16896,
                            16954, 17060, 17241, 17302, 17359, 17601, 17985, 18017, 18043, 18084,
                            18334, 18539, 18637, 18831, 18864, 19068, 19075, 19140, 19445, 19487,
                            19495, 19559, 19648, 19656, 19770, 19880, 20284, 20311, 20358, 20439,
                            21103, 21252, 21382, 21429, 21678, 21765, 21773, 21779, 21877, 22067,
                            22318, 22607, 22713, 22719, 22929, 23074, 23148, 23209, 23500, 23611,
                            23614, 23709, 23761, 23952, 23999, 24120, 24217, 24503, 24656, 24675,
                            24842, 24924, 24970, 25144, 25582, 25767, 25923, 26184, 26206, 26344,
                            26376, 26529, 26682, 26686, 26803, 26896, 26921, 26951, 26982, 27033,
                            27075, 27163, 27166, 27299, 27567, 27682, 28010, 28173, 28368, 28423,
                            28440, 28590, 28801, 28990, 28994, 29138, 29256, 29300, 29657, 29769,
                            30018, 30086, 30154, 30189, 30382, 30385, 30445, 30456, 30489, 30545,
                            30908, 30931, 31009, 31267, 31297, 31336, 31696, 31728, 31735, 31943,
                            32155, 32244, 32342, 32431, 32569, 32733, 32799, 32817, 32903, 33078,
                            33552, 34064, 34604, 34705, 35186, 35256, 35284, 35295, 35494, 35745,
                            35943, 36051, 36343, 36430, 36452, 36666, 36697, 36763, 36822, 36890,
                            37511, 37547, 37706, 38256, 38581, 38911, 38931, 38955, 38998, 39131,
                            39135, 39255, 39312, 39394, 39459, 39635, 39707, 40190, 40215, 40708,
                            41063, 41264, 41361, 41593, 41699, 41864, 42190, 42363, 42444, 42873,
                            42983, 43314, 43587, 43693, 43880, 43908, 43909, 43925, 43978, 43986,
                            44071, 44183, 44340, 44398, 44466, 44498};
                    test_read_index_file(meta_x86_old_v2, data_dir, "x86_old_v2", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V2,
                                         false, expected_gif_cardinality, expected_gif_doc_ids);
                }
                {
                    TabletIndex meta_x86_old_v1 = create_test_index_meta(10248, "request_idx", 2);
                    std::string field_name_avx2 = "request";
                    default_query_term = "gif";
                    expected_gif_cardinality = 40893;
                    expected_gif_doc_ids = {0,    19,   110,   1001,  2581,
                                            7196, 9090, 16711, 10278, 44701};
                    test_read_index_file(meta_x86_old_v1, data_dir, "x86_old", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V1,
                                         false, expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 356;
                    expected_gif_doc_ids = {
                            622,   754,   1021,  1186,  1403,  1506,  1655,  1661,  1833,  2287,
                            2356,  2425,  2849,  3198,  3350,  3365,  3416,  3423,  3499,  3541,
                            3609,  3682,  3936,  4117,  4198,  4589,  4591,  4808,  4959,  5282,
                            5332,  5495,  5560,  5624,  5773,  5831,  6138,  6180,  6361,  6372,
                            6621,  6777,  6878,  6911,  6983,  7048,  7148,  7207,  7273,  7274,
                            7385,  7545,  7735,  7904,  7912,  8150,  8215,  8238,  8363,  8598,
                            8672,  8765,  8877,  9188,  9264,  9761,  9864,  9866,  9946,  10022,
                            10139, 10143, 10146, 10184, 10291, 10304, 10308, 10332, 10371, 10695,
                            10707, 11056, 11095, 11111, 11505, 11752, 11860, 11989, 12119, 12156,
                            12655, 12764, 12792, 13055, 13636, 13824, 13902, 13912, 14061, 14152,
                            14315, 14355, 14618, 14712, 14788, 15050, 15057, 15110, 15122, 15249,
                            15267, 15281, 15735, 15848, 15939, 16117, 16327, 16331, 16597, 16739,
                            16868, 17092, 17458, 17553, 17602, 17664, 17781, 18061, 18353, 18397,
                            18468, 18717, 18726, 19131, 19209, 19402, 19551, 19812, 20128, 20146,
                            20232, 20322, 20407, 20431, 20436, 20466, 20757, 20960, 20994, 21197,
                            21254, 21487, 21561, 21602, 21662, 21710, 21754, 21826, 21965, 22091,
                            22200, 22203, 22291, 22317, 22561, 22584, 22606, 22950, 23140, 23315,
                            23442, 23858, 24026, 24322, 24581, 24617, 24655, 24756, 24974, 25191,
                            25246, 25287, 25406, 25599, 25830, 26020, 26109, 26149, 26402, 26431,
                            26451, 26458, 26495, 26766, 26777, 26848, 26966, 27053, 27089, 27177,
                            27519, 27595, 27693, 28294, 28719, 28755, 29073, 29323, 29472, 29496,
                            29604, 29761, 29772, 29953, 30030, 30083, 30139, 30210, 30719, 30774,
                            30868, 30897, 31200, 31347, 31811, 31880, 31903, 32040, 32048, 32225,
                            32335, 32357, 32517, 32579, 32679, 32821, 33294, 33393, 33509, 33675,
                            33802, 34390, 34441, 34474, 34547, 34557, 35057, 35262, 35327, 35348,
                            35455, 35482, 35668, 35811, 35845, 35953, 36098, 36151, 36602, 36711,
                            36946, 37036, 37220, 37291, 37436, 37721, 37747, 37864, 37890, 37923,
                            38045, 38588, 38654, 38730, 38930, 39169, 39814, 40401, 40689, 40762,
                            40822, 41249, 41399, 41419, 41572, 41736, 41768, 41946, 41989, 42077,
                            42079, 42225, 42360, 42524, 42576, 42595, 42691, 42784, 42892, 42930,
                            43210, 43299, 43348, 43468, 43510, 43622, 43795, 43824, 43893, 43972,
                            43975, 43998, 44008, 44023, 44031, 44049, 44139, 44518, 44555, 44597,
                            44815, 44879, 45014, 45020, 45054, 45084, 45100, 45464, 45471, 45505,
                            45580, 45593, 45686, 45991, 46019, 46021, 46107, 46138, 46197, 46209,
                            46551, 46658, 46988, 47027, 47046, 47071, 47106, 47190, 47225, 47439,
                            47465, 47531, 47602, 47660, 48453, 48575};
                    test_read_index_file(meta_x86_old_v1, data_dir, "x86_old", field_name_avx2,
                                         default_query_term, InvertedIndexStorageFormatPB::V1,
                                         false, expected_gif_cardinality, expected_gif_doc_ids);
                }
            } else {
                std::cout << "Testing with SSE support" << std::endl;
                TabletIndex meta_x86_new_sse = create_test_index_meta(
                        default_index_id, default_index_name, default_col_unique_id);
                test_read_index_file(meta_x86_new_sse, data_dir, "x86_256_new", default_field_name,
                                     default_query_term, InvertedIndexStorageFormatPB::V2, false,
                                     expected_gif_cardinality, expected_gif_doc_ids);

                TabletIndex meta_x86_old_sse =
                        create_test_index_meta(1743693997395, "request_idx", 2);
                test_read_index_file(meta_x86_old_sse, data_dir, "x86_256_old", "2",
                                     default_query_term, InvertedIndexStorageFormatPB::V1, true,
                                     expected_gif_cardinality, expected_gif_doc_ids);

                TabletIndex meta_arm_new_sse = create_test_index_meta(
                        default_index_id, default_index_name, default_col_unique_id);
                test_read_index_file(meta_arm_new_sse, data_dir, "arm_new", default_field_name,
                                     default_query_term, InvertedIndexStorageFormatPB::V2, false,
                                     expected_gif_cardinality, expected_gif_doc_ids);

                TabletIndex meta_arm_old_sse = create_test_index_meta(
                        default_index_id, default_index_name, default_col_unique_id);
                test_read_index_file(meta_arm_old_sse, data_dir, "arm_old", default_field_name,
                                     default_query_term, InvertedIndexStorageFormatPB::V1, true,
                                     expected_gif_cardinality, expected_gif_doc_ids);
            }
        }
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
    test_read_existing_large_docset();
}

} // namespace doris::segment_v2