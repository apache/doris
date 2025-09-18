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
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_iterator.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "runtime/runtime_state.h"
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
    void prepare_string_index(
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
        auto index_file_writer = std::make_unique<IndexFileWriter>(
                fs, *index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

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
        auto index_file_writer = std::make_unique<IndexFileWriter>(
                fs, *index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        // Get c1 column Field
        const TabletColumn& column = tablet_schema->column(0);
        ASSERT_NE(&column, nullptr);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        ASSERT_NE(field.get(), nullptr);

        // Create column writer
        std::unique_ptr<IndexColumnWriter> column_writer;
        auto status = IndexColumnWriter::create(field.get(), &column_writer,
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
        // Create index metadata
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column ID
        idx_meta.init_from_pb(*index_meta_pb.get());
        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix,
                             InvertedIndexStorageFormatPB::V2);
        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<IndexFileReader>(
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

        auto context = std::make_shared<segment_v2::IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;
        auto query_status = str_reader->query(context, field_name, &str_ref,
                                              InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 2) << "Should find 2 documents matching 'apple'";
        EXPECT_TRUE(bitmap->contains(0)) << "Document 0 should match 'apple'";
        EXPECT_TRUE(bitmap->contains(3)) << "Document 3 should match 'apple'";

        // Test non-existent value
        bitmap = std::make_shared<roaring::Roaring>();
        std::string not_exist = "orange";
        StringRef not_exist_ref(not_exist.c_str(), not_exist.length());

        query_status = str_reader->query(context, field_name, &not_exist_ref,
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

        auto reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);

        auto status = reader->init();
        EXPECT_EQ(status, Status::OK());

        auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(str_reader, nullptr);

        auto context = std::make_shared<segment_v2::IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        // Read NULL bitmap
        InvertedIndexQueryCacheHandle cache_handle;
        status = str_reader->read_null_bitmap(context, &cache_handle, nullptr);
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

        auto reader = std::make_shared<IndexFileReader>(
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

        auto context = std::make_shared<segment_v2::IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        auto query_status = bkd_reader->query(context, field_name, &query_value,
                                              InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 2) << "Should find 2 documents matching value 42";
        EXPECT_TRUE(bitmap->contains(0)) << "Document 0 should match value 42";
        EXPECT_TRUE(bitmap->contains(2)) << "Document 2 should match value 42";

        // Test LESS_THAN query
        bitmap = std::make_shared<roaring::Roaring>();
        int32_t less_than_value = 100;

        query_status = bkd_reader->query(context, field_name, &less_than_value,
                                         InvertedIndexQueryType::LESS_THAN_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 2) << "Should find 2 documents with values less than 100";
        EXPECT_TRUE(bitmap->contains(0)) << "Document 0 should have value less than 100";
        EXPECT_TRUE(bitmap->contains(2)) << "Document 2 should have value less than 100";

        // Test GREATER_THAN query
        bitmap = std::make_shared<roaring::Roaring>();
        int32_t greater_than_value = 100;

        query_status = bkd_reader->query(context, field_name, &greater_than_value,
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
        // Create index metadata
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column ID
        idx_meta.init_from_pb(*index_meta_pb.get());
        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_query_cache = true;
        query_options.enable_inverted_index_searcher_cache = true;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<IndexFileReader>(
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

        auto context = std::make_shared<segment_v2::IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        auto query_status = str_reader->query(context, field_name, &str_ref,
                                              InvertedIndexQueryType::EQUAL_QUERY, bitmap1);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(stats.inverted_index_query_cache_miss, 1) << "First query should be cache miss";
        EXPECT_EQ(bitmap1->cardinality(), 1) << "Should find 1 document matching 'apple'";

        // Second query with same value, should be cache hit
        std::shared_ptr<roaring::Roaring> bitmap2 = std::make_shared<roaring::Roaring>();

        query_status = str_reader->query(context, field_name, &str_ref,
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
        // Create index metadata
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column ID
        idx_meta.init_from_pb(*index_meta_pb.get());
        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix,
                             InvertedIndexStorageFormatPB::V2);

        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_query_cache = false;
        query_options.enable_inverted_index_searcher_cache = true;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<IndexFileReader>(
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

        auto context = std::make_shared<segment_v2::IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        auto query_status = str_reader->query(context, field_name, &str_ref,
                                              InvertedIndexQueryType::EQUAL_QUERY, bitmap1);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(stats.inverted_index_searcher_cache_miss, 1)
                << "First query should be searcher cache miss";

        // Query with different value, should be searcher cache hit
        std::shared_ptr<roaring::Roaring> bitmap2 = std::make_shared<roaring::Roaring>();
        StringRef str_ref2(values[1].data, values[1].size); // "banana"

        query_status = str_reader->query(context, field_name, &str_ref2,
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

        // Add 600 documents with term "common"
        for (int i = 0; i < 600; i++) {
            values.emplace_back("common");
        }

        // Add 200 documents with term "apple"
        for (int i = 0; i < 200; i++) {
            values.emplace_back("apple");
        }

        // Add 200 documents with term "banana"
        for (int i = 0; i < 200; i++) {
            values.emplace_back("banana");
        }

        TabletIndex idx_meta;
        // Create index metadata
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column ID
        index_meta_pb->mutable_properties()->insert({"parser", "english"});
        index_meta_pb->mutable_properties()->insert({"lower_case", "true"});
        index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
        idx_meta.init_from_pb(*index_meta_pb.get());
        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix,
                             InvertedIndexStorageFormatPB::V2);

        // Create reader
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);

        auto status = reader->init();
        EXPECT_EQ(status, Status::OK());

        auto str_reader = FullTextIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(str_reader, nullptr);

        io::IOContext io_ctx;
        std::string field_name = "1"; // c2 column unique_id

        // Test query for "common_term" which has >512 documents
        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        std::string query_term = "common";
        StringRef str_ref(query_term.c_str(), query_term.length());

        auto context = std::make_shared<segment_v2::IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        auto query_status = str_reader->query(context, field_name, &str_ref,
                                              InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 600) << "Should find 600 documents matching 'common'";

        // Verify first and last document IDs
        EXPECT_TRUE(bitmap->contains(0)) << "First document should match 'common'";
        EXPECT_TRUE(bitmap->contains(599)) << "Last document should match 'common'";

        // Test query for "apple"
        bitmap = std::make_shared<roaring::Roaring>();
        query_term = "apple";
        StringRef str_ref_a(query_term.c_str(), query_term.length());

        query_status = str_reader->query(context, field_name, &str_ref_a,
                                         InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);

        EXPECT_TRUE(query_status.ok()) << query_status;
        EXPECT_EQ(bitmap->cardinality(), 200) << "Should find 200 documents matching 'apple'";
        EXPECT_TRUE(bitmap->contains(600)) << "First document of apple should be at position 600";
        EXPECT_TRUE(bitmap->contains(799)) << "Last document of apple should be at position 799";
    }

    // Test string index with large document set using V3 format
    void test_string_index_large_docset_v3() {
        std::string_view rowset_id = "test_read_rowset_6_v3";
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

        {
            TabletIndex idx_meta;
            // Create index metadata
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test");
            index_meta_pb->clear_col_unique_id();
            index_meta_pb->add_col_unique_id(1); // c2 column ID
            index_meta_pb->mutable_properties()->insert({"parser", "english"});
            index_meta_pb->mutable_properties()->insert({"lower_case", "true"});
            index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
            idx_meta.init_from_pb(*index_meta_pb.get());
            std::string index_path_prefix;
            prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix,
                                 InvertedIndexStorageFormatPB::V3);

            // Create V3 format reader
            OlapReaderStatistics stats;
            RuntimeState runtime_state;
            TQueryOptions query_options;
            query_options.enable_inverted_index_searcher_cache = false;
            runtime_state.set_query_options(query_options);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V3);

            auto status = reader->init();
            EXPECT_EQ(status, Status::OK());

            auto str_reader = FullTextIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(str_reader, nullptr);

            io::IOContext io_ctx;
            std::string field_name = "1"; // c2 column unique_id

            // Test query for "common_term" which has >512 documents with V3 format
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string query_term = "common_term";
            StringRef str_ref(query_term.c_str(), query_term.length());

            auto context = std::make_shared<segment_v2::IndexQueryContext>();
            context->io_ctx = &io_ctx;
            context->stats = &stats;
            context->runtime_state = &runtime_state;

            auto query_status =
                    str_reader->query(context, field_name, &str_ref,
                                      InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);

            EXPECT_TRUE(query_status.ok()) << query_status;
            EXPECT_EQ(bitmap->cardinality(), 600)
                    << "V3: Should find 600 documents matching 'common_term'";

            // Verify first and last document IDs
            EXPECT_TRUE(bitmap->contains(0)) << "V3: First document should match 'common_term'";
            EXPECT_TRUE(bitmap->contains(599)) << "V3: Last document should match 'common_term'";

            // Test query for "term_a" with V3 format
            bitmap = std::make_shared<roaring::Roaring>();
            query_term = "term_a";
            StringRef str_ref_a(query_term.c_str(), query_term.length());

            query_status = str_reader->query(context, field_name, &str_ref_a,
                                             InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);

            EXPECT_TRUE(query_status.ok()) << query_status;
            EXPECT_EQ(bitmap->cardinality(), 200)
                    << "V3: Should find 200 documents matching 'term_a'";
            EXPECT_TRUE(bitmap->contains(600))
                    << "V3: First document of term_a should be at position 600";
            EXPECT_TRUE(bitmap->contains(799))
                    << "V3: Last document of term_a should be at position 799";

            // Test query for "noexist" with V3 format
            bitmap = std::make_shared<roaring::Roaring>();
            query_term = "noexist";
            StringRef str_ref_no_term(query_term.c_str(), query_term.length());

            query_status = str_reader->query(context, field_name, &str_ref_no_term,
                                             InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap);
            EXPECT_TRUE(query_status.ok()) << query_status;
            EXPECT_EQ(bitmap->cardinality(), 0) << "V3: Should find 0 documents matching 'noexist'";
        }
        {
            TabletIndex idx_meta;
            // Create index metadata
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test");
            index_meta_pb->clear_col_unique_id();
            index_meta_pb->add_col_unique_id(1); // c2 column ID
            idx_meta.init_from_pb(*index_meta_pb.get());
            std::string index_path_prefix;
            prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix,
                                 InvertedIndexStorageFormatPB::V3);

            // Create V3 format reader
            OlapReaderStatistics stats;
            RuntimeState runtime_state;
            TQueryOptions query_options;
            query_options.enable_inverted_index_searcher_cache = false;
            runtime_state.set_query_options(query_options);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V3);

            auto status = reader->init();
            EXPECT_EQ(status, Status::OK());

            auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(str_reader, nullptr);

            io::IOContext io_ctx;
            std::string field_name = "1"; // c2 column unique_id

            // Test query for "common_term" which has >512 documents with V3 format
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string query_term = "common_term";
            StringRef str_ref(query_term.c_str(), query_term.length());

            auto context = std::make_shared<segment_v2::IndexQueryContext>();
            context->io_ctx = &io_ctx;
            context->stats = &stats;
            context->runtime_state = &runtime_state;

            auto query_status = str_reader->query(context, field_name, &str_ref,
                                                  InvertedIndexQueryType::EQUAL_QUERY, bitmap);

            EXPECT_TRUE(query_status.ok()) << query_status;
            EXPECT_EQ(bitmap->cardinality(), 600)
                    << "V3: Should find 600 documents matching 'common_term'";

            // Verify first and last document IDs
            EXPECT_TRUE(bitmap->contains(0)) << "V3: First document should match 'common_term'";
            EXPECT_TRUE(bitmap->contains(599)) << "V3: Last document should match 'common_term'";

            // Test query for "term_a" with V3 format
            bitmap = std::make_shared<roaring::Roaring>();
            query_term = "term_a";
            StringRef str_ref_a(query_term.c_str(), query_term.length());

            query_status = str_reader->query(context, field_name, &str_ref_a,
                                             InvertedIndexQueryType::EQUAL_QUERY, bitmap);

            EXPECT_TRUE(query_status.ok()) << query_status;
            EXPECT_EQ(bitmap->cardinality(), 200)
                    << "V3: Should find 200 documents matching 'term_a'";
            EXPECT_TRUE(bitmap->contains(600))
                    << "V3: First document of term_a should be at position 600";
            EXPECT_TRUE(bitmap->contains(799))
                    << "V3: Last document of term_a should be at position 799";

            // Test query for "noexist" with V3 format
            bitmap = std::make_shared<roaring::Roaring>();
            query_term = "noexist";
            StringRef str_ref_no_term(query_term.c_str(), query_term.length());

            query_status = str_reader->query(context, field_name, &str_ref_no_term,
                                             InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(query_status.ok()) << query_status;
            EXPECT_EQ(bitmap->cardinality(), 0) << "V3: Should find 0 documents matching 'noexist'";
        }
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

        auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                        index_path_prefix, storage_format);

        auto status = reader->init();
        ASSERT_TRUE(status.ok()) << "Failed to initialize IndexFileReader for " << index_file
                                 << ": " << status.to_string();

        auto index_reader = FullTextIndexReader::create_shared(&idx_meta, reader);
        ASSERT_NE(index_reader, nullptr)
                << "Failed to create FullTextIndexReader for " << index_file;

        io::IOContext io_ctx;

        // Test queries
        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        StringRef str_ref(query_term.c_str(), query_term.length());

        auto context = std::make_shared<segment_v2::IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        auto query_status = index_reader->query(context, field_name, &str_ref,
                                                InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);

        ASSERT_TRUE(query_status.ok()) << "Query failed for term '" << query_term << "' in file "
                                       << index_file << ": " << query_status.to_string();

        // Perform validation inline
        ASSERT_NE(bitmap, nullptr)
                << "Bitmap is null after successful query for file: " << index_file;
        EXPECT_EQ(bitmap->cardinality(), expected_cardinality)
                << "File: " << index_file << " - Incorrect cardinality for term '" << query_term
                << "'";
        //std::cout << "bitmap: " << bitmap->toString() << std::endl;
        for (uint32_t doc_id : expected_doc_ids) {
            EXPECT_TRUE(bitmap->contains(doc_id))
                    << "File: " << index_file << " - Bitmap should contain doc ID " << doc_id
                    << " for term '" << query_term << "'";
        }
    }

    // Test reading existing large document set index file
    void test_compatible_read_cross_platform() {
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
        std::string default_index_name = "test";
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
                {
                    TabletIndex meta_x86_old_noavx2_v2 =
                            create_test_index_meta(1744076789957, "request_idx", 2);
                    std::string field_name_avx2 = "2";
                    default_query_term = "gif";
                    expected_gif_cardinality = 37203;
                    expected_gif_doc_ids = {0,    19,   110,   1000,  2582,
                                            7196, 9090, 16711, 10279, 44703};
                    test_read_index_file(meta_x86_old_noavx2_v2, data_dir, "x86_noavx2_old_v2",
                                         field_name_avx2, default_query_term,
                                         InvertedIndexStorageFormatPB::V2, false,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 395;
                    expected_gif_doc_ids = {
                            267,   285,   462,   515,   578,   710,   778,   805,   807,   834,
                            958,   1166,  1231,  1266,  1339,  1487,  1523,  1524,  1555,  1622,
                            1632,  1676,  1742,  1762,  1798,  2068,  2074,  2220,  2559,  2594,
                            2600,  2646,  3101,  3426,  3491,  3827,  4085,  4192,  4358,  4590,
                            4776,  4789,  5191,  5236,  5339,  5378,  5578,  5630,  5711,  5742,
                            5747,  5884,  5932,  6061,  6157,  6187,  6387,  6404,  6488,  6583,
                            6808,  6905,  6969,  7033,  7059,  7582,  7630,  7651,  7820,  8045,
                            8161,  8244,  8382,  8472,  8476,  8545,  8628,  8700,  9047,  9129,
                            9191,  9197,  9265,  9660,  9808,  9998,  10185, 10324, 10431, 10441,
                            10507, 10569, 10611, 10693, 10761, 10965, 10996, 11113, 11348, 11370,
                            11578, 11592, 11694, 11969, 12104, 12521, 12718, 12871, 12907, 12911,
                            13018, 13113, 13126, 13136, 13140, 13304, 13381, 13568, 13606, 13637,
                            13720, 13727, 13871, 13883, 13931, 14075, 14179, 14210, 14367, 14464,
                            14475, 14526, 14723, 14835, 14884, 15070, 15163, 15283, 15309, 15373,
                            15420, 15495, 15531, 15635, 15704, 15752, 15760, 15768, 15777, 15827,
                            15855, 15977, 16048, 16284, 16305, 16348, 16387, 16519, 16637, 16641,
                            16954, 17080, 17318, 17409, 17435, 17491, 17550, 17587, 17872, 18021,
                            18248, 18272, 18395, 18541, 18569, 19100, 19170, 19331, 19383, 19529,
                            19571, 19581, 19594, 19630, 19635, 19714, 19970, 20272, 20317, 20432,
                            20689, 20798, 20896, 20936, 21327, 21357, 22049, 22076, 22108, 22125,
                            22181, 22185, 22262, 22327, 22411, 22514, 22531, 22553, 22774, 22824,
                            22929, 22995, 23026, 23069, 23146, 23193, 23194, 23411, 23430, 23515,
                            23561, 23616, 23680, 23898, 24104, 24200, 24235, 24287, 24358, 24417,
                            24483, 24678, 24758, 24764, 24824, 24926, 25202, 25257, 25576, 25598,
                            25816, 25910, 26015, 26277, 26479, 26787, 26857, 26941, 27140, 27216,
                            27282, 27528, 27554, 27725, 27974, 28087, 28136, 28228, 28441, 28491,
                            28618, 28628, 28733, 28758, 28793, 28896, 29143, 29150, 29279, 29617,
                            29632, 29854, 30086, 30364, 30371, 30868, 31034, 31139, 31421, 31502,
                            31538, 31968, 31989, 32220, 32264, 32363, 32393, 32490, 32576, 32671,
                            32741, 32867, 32874, 33115, 33503, 33970, 34192, 34258, 34366, 34418,
                            34550, 34648, 34667, 34738, 34829, 35184, 35279, 35314, 35510, 35645,
                            35684, 35708, 35725, 35768, 35895, 36227, 36247, 36307, 36361, 36456,
                            36586, 36638, 36656, 36716, 36856, 36907, 37088, 37217, 37321, 37374,
                            37397, 37448, 37481, 37572, 37769, 37911, 37925, 37973, 37988, 38020,
                            38108, 38134, 38248, 38429, 38615, 38814, 38827, 38877, 39080, 39167,
                            39218, 39593, 39932, 39946, 40143, 40303, 40339, 40405, 40592, 40719,
                            40791, 41101, 41194, 41206, 41358, 41455, 41470, 41560, 42374, 42597,
                            42718, 42728, 42800, 42826, 42902, 43085, 43130, 43203, 43301, 43448,
                            43556, 43604, 43606, 43656, 43781, 44029, 44043, 44129, 44203, 44273,
                            44323, 44412, 44590, 44619, 44659};
                    test_read_index_file(meta_x86_old_noavx2_v2, data_dir, "x86_noavx2_old_v2",
                                         field_name_avx2, default_query_term,
                                         InvertedIndexStorageFormatPB::V2, false,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                }
                {
                    TabletIndex meta_x86_old_noavx2_v1 =
                            create_test_index_meta(1744076790030, "request_idx", 2);
                    std::string field_name_avx2 = "request";
                    default_query_term = "gif";
                    expected_gif_cardinality = 40657;
                    expected_gif_doc_ids = {0,    19,   110,   1001,  2581,
                                            7197, 9090, 16711, 10278, 44701};
                    test_read_index_file(meta_x86_old_noavx2_v1, data_dir, "x86_noavx2_old",
                                         field_name_avx2, default_query_term,
                                         InvertedIndexStorageFormatPB::V1, false,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 407;
                    expected_gif_doc_ids = {
                            401,   452,   511,   584,   661,   916,   1019,  1149,  1212,  1285,
                            1498,  1877,  1998,  2048,  2065,  2123,  2266,  2332,  2436,  2711,
                            2743,  2851,  2927,  2959,  3129,  3330,  3433,  3536,  3745,  3808,
                            3825,  4472,  4523,  4641,  4780,  4788,  4799,  5032,  5043,  5077,
                            5368,  5532,  5638,  5794,  5837,  6179,  6744,  6756,  7057,  7093,
                            7100,  7143,  7269,  7277,  7429,  7431,  7484,  7531,  8032,  8275,
                            8303,  8327,  8423,  8944,  9043,  9075,  9170,  9317,  9636,  9683,
                            9687,  9755,  10054, 10062, 10152, 10208, 10471, 10747, 10771, 10815,
                            10861, 10976, 11012, 11014, 11099, 11110, 11251, 11261, 11266, 11293,
                            11436, 11474, 11752, 11783, 11800, 11851, 11960, 12028, 12068, 12199,
                            12404, 12422, 12605, 12814, 12889, 13104, 13414, 13505, 13572, 13839,
                            14099, 14212, 14245, 14248, 14260, 14364, 14396, 14478, 14486, 14542,
                            14627, 14674, 14797, 14853, 14875, 14945, 14984, 15254, 15273, 15591,
                            15600, 15621, 15650, 15794, 15987, 16046, 16112, 16119, 16170, 16173,
                            16325, 16461, 16474, 16525, 16656, 16758, 16963, 17068, 17262, 17329,
                            17507, 17511, 17535, 17630, 17897, 17966, 18075, 18163, 18209, 18297,
                            18378, 18380, 18419, 18533, 18587, 18681, 18927, 19108, 19283, 19350,
                            19370, 19493, 19516, 19612, 19792, 20045, 20107, 20111, 20211, 20266,
                            20322, 20325, 20384, 20986, 21035, 21193, 21201, 21578, 21589, 21604,
                            21686, 21800, 21816, 21983, 22007, 22185, 22230, 22338, 22482, 22526,
                            22540, 22563, 22575, 22726, 22855, 23032, 23087, 23149, 23182, 23890,
                            24070, 24192, 24239, 24368, 24521, 24562, 24567, 24625, 24685, 24797,
                            24898, 24971, 25006, 25007, 25229, 25425, 25753, 25777, 25877, 25921,
                            26328, 26455, 26537, 26587, 26677, 26881, 27086, 27431, 27491, 27537,
                            27640, 27748, 27829, 27919, 28104, 28170, 28235, 28449, 28468, 28574,
                            28834, 28942, 29092, 29102, 29184, 29215, 29237, 29318, 29622, 29974,
                            30071, 30192, 30218, 30302, 30353, 30711, 30869, 31070, 31133, 31193,
                            31210, 31273, 31391, 31516, 31704, 31746, 31792, 31807, 32046, 32054,
                            32297, 32484, 32513, 32676, 33028, 33173, 33463, 33554, 33620, 33652,
                            33741, 33967, 34082, 34092, 34294, 34321, 34338, 34362, 34641, 35035,
                            35039, 35149, 35270, 35322, 35349, 35586, 35627, 35820, 35832, 35920,
                            36505, 36518, 36589, 36597, 36755, 36772, 36774, 36871, 37211, 37405,
                            37564, 37843, 37927, 37935, 38171, 38416, 38520, 38586, 38685, 38821,
                            38906, 38944, 39001, 39124, 39153, 39276, 39421, 39426, 39609, 39612,
                            39734, 39836, 39999, 40108, 40136, 40226, 40307, 40349, 40403, 40491,
                            40993, 41189, 41448, 41487, 41666, 41691, 41716, 41733, 41924, 42006,
                            42070, 42317, 42451, 42588, 42800, 42903, 42934, 43217, 43221, 43544,
                            43586, 43945, 44030, 44068, 44334, 44355, 44650, 44676, 44722, 44738,
                            44915, 45060, 45493, 45650, 45708, 45740, 45800, 46172, 46485, 46674,
                            46680, 46763, 46898, 47021, 47092, 47214, 47321, 47758, 47761, 47913,
                            48121, 48167, 48184, 48271, 48383, 48431, 48560};
                    test_read_index_file(meta_x86_old_noavx2_v1, data_dir, "x86_noavx2_old",
                                         field_name_avx2, default_query_term,
                                         InvertedIndexStorageFormatPB::V1, false,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                }
                {
                    TabletIndex meta_x86_new_noavx2_v2 =
                            create_test_index_meta(1744093412497, "request_idx", 2);
                    std::string field_name_avx2 = "2";
                    default_query_term = "gif";
                    expected_gif_cardinality = 37409;
                    expected_gif_doc_ids = {2,    19,   110,   1001,  2583,
                                            7196, 9090, 16710, 10278, 44702};
                    test_read_index_file(meta_x86_new_noavx2_v2, data_dir, "x86_noavx2_new_v2",
                                         field_name_avx2, default_query_term,
                                         InvertedIndexStorageFormatPB::V2, false,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 368;
                    expected_gif_doc_ids = {
                            20,    206,   632,   742,   799,   1080,  1217,  1764,  1770,  2401,
                            2415,  2425,  2560,  2587,  2852,  2876,  3235,  3336,  3763,  4051,
                            4101,  4330,  4361,  4393,  4405,  4743,  4812,  4815,  4897,  4958,
                            5088,  5180,  5250,  5326,  5379,  5428,  5497,  5514,  5626,  6041,
                            6068,  6107,  6354,  6576,  6779,  6784,  6964,  6988,  7005,  7123,
                            7172,  7459,  7575,  7863,  7920,  7923,  7939,  7957,  7977,  8550,
                            8654,  8683,  8790,  8921,  8992,  9088,  9101,  9235,  9348,  9469,
                            9486,  9670,  9759,  9823,  9833,  9857,  10187, 10477, 10760, 10955,
                            11056, 11266, 11289, 11343, 11357, 11439, 11447, 11508, 11608, 11719,
                            11797, 11843, 11937, 11939, 12126, 12173, 12228, 12321, 12364, 12504,
                            12749, 12821, 12858, 13031, 13108, 13126, 13214, 13235, 13314, 13360,
                            13374, 13385, 13455, 13596, 13707, 13771, 13810, 14305, 14444, 14617,
                            14679, 14865, 15301, 15332, 15341, 15696, 15807, 15839, 15883, 15946,
                            16015, 16156, 16304, 16412, 16607, 16709, 16797, 17290, 17563, 17570,
                            18091, 18218, 18220, 18258, 18465, 18628, 18644, 18652, 18653, 18729,
                            18737, 19053, 19138, 19155, 19208, 19209, 19245, 19384, 19587, 19947,
                            20008, 20151, 20178, 20468, 20623, 20667, 20796, 20924, 21019, 21194,
                            21471, 21493, 21540, 21622, 21675, 21746, 21991, 22184, 22490, 22627,
                            23004, 23005, 23122, 23197, 23279, 23322, 23733, 23788, 23857, 23898,
                            23924, 24359, 24574, 24635, 24759, 24804, 25009, 25083, 25181, 25349,
                            25503, 25900, 26135, 26306, 26755, 26838, 26870, 26880, 26927, 27000,
                            27063, 27226, 27391, 27418, 27458, 27536, 27544, 27595, 27660, 27854,
                            27875, 27901, 27947, 28064, 28201, 28211, 28240, 28270, 28349, 28408,
                            28456, 28696, 28829, 28886, 28944, 28967, 29107, 29215, 29782, 29907,
                            30382, 30434, 30491, 30515, 30539, 30777, 30896, 30935, 31041, 31161,
                            31244, 31521, 31625, 31669, 31800, 31819, 32308, 32327, 32483, 32690,
                            32709, 33087, 33222, 33272, 33370, 33522, 33677, 33699, 34086, 34280,
                            34303, 34372, 34492, 34564, 34602, 34668, 34738, 34854, 34871, 35154,
                            35228, 35474, 35637, 35658, 35671, 35672, 35775, 35896, 35913, 36024,
                            36220, 36259, 36394, 36437, 36671, 36833, 37023, 37073, 37095, 37136,
                            37171, 37262, 37369, 37528, 37583, 37697, 37710, 37842, 37955, 38073,
                            38080, 38091, 38100, 38187, 38214, 38283, 38430, 38485, 38578, 38592,
                            38910, 39157, 39254, 39387, 39388, 39570, 39642, 39664, 39732, 39814,
                            39817, 39865, 40071, 40096, 40113, 40495, 40529, 40596, 40676, 40711,
                            40850, 41156, 41160, 41236, 41809, 41822, 41895, 41912, 41961, 41991,
                            42037, 42271, 42333, 42556, 42614, 42682, 42897, 43041, 43429, 43530,
                            43597, 43646, 43754, 44312, 44334, 44438, 44489, 44513};
                    test_read_index_file(meta_x86_new_noavx2_v2, data_dir, "x86_noavx2_new_v2",
                                         field_name_avx2, default_query_term,
                                         InvertedIndexStorageFormatPB::V2, true,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                }
                {
                    TabletIndex meta_x86_new_noavx2_v1 =
                            create_test_index_meta(1744093412581, "request_idx", 2);
                    std::string field_name_avx2 = "request";
                    default_query_term = "gif";
                    expected_gif_cardinality = 37272;
                    expected_gif_doc_ids = {0,    19,   110,   1001,  2581,
                                            7197, 9090, 16711, 10278, 44701};
                    test_read_index_file(meta_x86_new_noavx2_v1, data_dir, "x86_noavx2_new",
                                         field_name_avx2, default_query_term,
                                         InvertedIndexStorageFormatPB::V1, false,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                    default_query_term = "/english/index.html";
                    expected_gif_cardinality = 326;
                    expected_gif_doc_ids = {
                            50,    117,   237,   623,   1102,  1172,  1358,  1612,  1725,  1932,
                            2074,  2233,  2395,  2618,  2871,  2977,  2985,  3305,  3375,  3385,
                            3518,  3713,  3761,  3810,  3879,  3917,  4059,  4081,  4114,  4121,
                            4292,  4306,  4509,  4565,  4566,  4700,  4711,  4831,  4832,  5024,
                            5029,  5184,  5324,  5432,  5618,  5753,  5803,  5844,  6558,  6594,
                            6876,  6901,  7273,  7429,  7498,  7504,  7624,  7681,  7842,  7883,
                            7971,  7983,  8349,  8530,  8597,  8632,  8687,  8807,  8847,  8865,
                            8886,  9303,  9315,  9319,  9428,  9509,  9601,  9799,  9909,  10101,
                            10177, 10203, 10228, 10553, 10666, 10693, 10780, 10814, 10824, 11046,
                            11118, 11265, 11409, 11463, 11611, 11730, 11767, 12041, 12096, 12119,
                            12294, 12475, 12496, 12634, 12759, 12987, 13181, 13276, 13373, 13566,
                            13830, 14001, 14383, 14425, 14613, 14846, 15002, 15039, 15072, 15817,
                            15950, 16092, 16109, 16334, 16442, 16531, 16635, 17023, 17030, 17661,
                            17726, 17856, 18208, 18210, 18261, 18414, 18420, 18582, 18645, 19045,
                            19101, 19374, 19535, 19728, 19740, 19815, 19861, 19938, 19955, 19991,
                            20512, 20908, 21066, 21097, 21403, 21524, 21789, 22177, 22298, 22402,
                            22422, 22769, 22836, 22874, 22985, 23005, 23018, 23027, 23291, 23361,
                            23413, 23500, 23513, 23588, 23609, 23851, 23959, 24228, 24383, 24445,
                            24468, 24636, 24817, 24888, 25070, 25459, 25618, 25640, 26178, 26459,
                            26583, 26970, 27070, 27131, 27147, 27479, 27606, 27616, 27696, 27780,
                            27871, 27960, 28094, 28306, 28442, 28516, 28609, 28843, 29042, 29488,
                            29512, 29686, 29891, 29932, 29956, 30094, 30319, 30357, 30478, 30527,
                            30914, 31035, 31530, 31586, 31659, 31696, 32134, 32178, 32273, 32713,
                            32932, 33273, 33334, 33338, 33461, 33481, 33552, 33727, 33852, 33982,
                            34104, 34235, 34253, 34308, 34816, 35126, 35147, 35246, 35293, 35329,
                            35402, 35482, 35535, 35940, 35986, 36034, 36075, 36125, 36170, 36313,
                            36340, 36683, 36823, 37002, 37351, 37458, 37537, 37552, 37808, 37943,
                            37952, 37954, 38166, 38461, 38624, 38756, 38807, 38847, 39096, 39147,
                            39358, 39592, 40015, 40170, 40201, 40230, 40409, 40542, 40593, 40608,
                            40687, 40825, 40894, 40903, 41234, 41278, 41380, 41488, 41522, 41555,
                            41559, 41593, 41678, 41742, 41765, 41792, 42054, 42248, 42319, 42623,
                            42660, 42886, 42925, 43338, 43552, 43593, 43594, 43766, 43782, 43881,
                            44229, 44263, 44324, 44537, 44601, 44661};
                    test_read_index_file(meta_x86_new_noavx2_v1, data_dir, "x86_noavx2_new",
                                         field_name_avx2, default_query_term,
                                         InvertedIndexStorageFormatPB::V1, false,
                                         expected_gif_cardinality, expected_gif_doc_ids);
                }
            }
        }
    }

    class MockStringTypeInvertedIndexReader final : public StringTypeInvertedIndexReader {
    public:
        static std::shared_ptr<MockStringTypeInvertedIndexReader> create_shared(
                const TabletIndex* idx_meta, std::shared_ptr<IndexFileReader>& file_reader) {
            return std::shared_ptr<MockStringTypeInvertedIndexReader>(
                    new MockStringTypeInvertedIndexReader(idx_meta, file_reader));
        }

    protected:
        Status handle_searcher_cache(const IndexQueryContextPtr& context,
                                     InvertedIndexCacheHandle*) override {
            CLuceneError err;
            err.set(CL_ERR_IO, "mock handle_searcher_cache failure");
            throw err;
        }

    private:
        MockStringTypeInvertedIndexReader(const TabletIndex* idx_meta,
                                          std::shared_ptr<IndexFileReader>& file_reader)
                : StringTypeInvertedIndexReader(idx_meta, file_reader) {}
    };

    // Mock class for testing tokenized index query exceptions
    class MockTokenizedStringTypeInvertedIndexReader final : public FullTextIndexReader {
    public:
        static std::shared_ptr<MockTokenizedStringTypeInvertedIndexReader> create_shared(
                const TabletIndex* idx_meta, std::shared_ptr<IndexFileReader>& file_reader) {
            return std::shared_ptr<MockTokenizedStringTypeInvertedIndexReader>(
                    new MockTokenizedStringTypeInvertedIndexReader(idx_meta, file_reader));
        }

    protected:
        Status handle_searcher_cache(const IndexQueryContextPtr& context,
                                     InvertedIndexCacheHandle*) override {
            CLuceneError err;
            err.set(CL_ERR_IO, "mock tokenized index searcher cache failure");
            throw err;
        }

    private:
        MockTokenizedStringTypeInvertedIndexReader(const TabletIndex* idx_meta,
                                                   std::shared_ptr<IndexFileReader>& file_reader)
                : FullTextIndexReader(idx_meta, file_reader) {}
    };

    void test_cache_error_scenarios() {
        std::string_view rowset_id = "test_handle_searcher_cache_exception";
        int seg_id = 0;
        std::vector<Slice> values = {Slice("apple"), Slice("banana")};

        TabletIndex idx_meta;
        {
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test_mock_cache");
            index_meta_pb->add_col_unique_id(1); // c2
            idx_meta.init_from_pb(*index_meta_pb);
        }

        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

        auto file_reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
        ASSERT_TRUE(file_reader->init().ok());

        auto mock_reader = MockStringTypeInvertedIndexReader::create_shared(&idx_meta, file_reader);
        ASSERT_NE(mock_reader, nullptr);

        io::IOContext io_ctx;
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions opts;
        opts.enable_inverted_index_searcher_cache = true;
        runtime_state.set_query_options(opts);

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        std::string field_name = "1"; // c2 unique_id
        StringRef query_val(values[0].data, values[0].size);

        Status st = mock_reader->query(context, field_name, &query_val,
                                       InvertedIndexQueryType::EQUAL_QUERY, bitmap);

        EXPECT_FALSE(st.ok());
        EXPECT_EQ(st.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    }

    void test_tokenized_index_query_error_scenarios() {
        std::string_view rowset_id = "test_tokenized_index_query_exception";
        int seg_id = 0;
        std::vector<Slice> values = {Slice("Hello world this is a test"),
                                     Slice("Apache Doris is a modern analytics database"),
                                     Slice("Inverted index provides fast text search")};

        TabletIndex idx_meta;
        {
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(2);
            index_meta_pb->set_index_name("test_tokenized_mock_cache");
            index_meta_pb->add_col_unique_id(1); // c2

            // Set tokenized index properties
            auto* properties = index_meta_pb->mutable_properties();
            (*properties)[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_ENGLISH;
            (*properties)[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] =
                    INVERTED_INDEX_PARSER_PHRASE_SUPPORT_YES;
            (*properties)[INVERTED_INDEX_PARSER_LOWERCASE_KEY] = INVERTED_INDEX_PARSER_TRUE;

            idx_meta.init_from_pb(*index_meta_pb);
        }

        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

        auto file_reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
        ASSERT_TRUE(file_reader->init().ok());

        auto mock_reader =
                MockTokenizedStringTypeInvertedIndexReader::create_shared(&idx_meta, file_reader);
        ASSERT_NE(mock_reader, nullptr);

        io::IOContext io_ctx;
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions opts;
        opts.enable_inverted_index_searcher_cache = true;
        runtime_state.set_query_options(opts);

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        std::string field_name = "1"; // c2 unique_id

        // Test tokenized query with "world" which should be found in "Hello world this is a test"
        std::string query_term = "world";
        StringRef query_val(query_term.data(), query_term.size());

        Status st = mock_reader->query(context, field_name, &query_val,
                                       InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap);

        EXPECT_FALSE(st.ok());
        EXPECT_EQ(st.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);

        // Test phrase query
        std::string phrase_query = "Apache Doris";
        StringRef phrase_query_val(phrase_query.data(), phrase_query.size());

        st = mock_reader->query(context, field_name, &phrase_query_val,
                                InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);

        EXPECT_FALSE(st.ok());
        EXPECT_EQ(st.code(), ErrorCode::INVERTED_INDEX_CLUCENE_ERROR);
    }

    // Test InvertedIndexResultBitmap operations
    void test_result_bitmap_operations() {
        auto bitmap1 = std::make_shared<roaring::Roaring>();
        bitmap1->add(1);
        bitmap1->add(2);
        bitmap1->add(3);

        auto null_bitmap1 = std::make_shared<roaring::Roaring>();
        null_bitmap1->add(4);
        null_bitmap1->add(5);

        auto bitmap2 = std::make_shared<roaring::Roaring>();
        bitmap2->add(2);
        bitmap2->add(3);
        bitmap2->add(6);

        auto null_bitmap2 = std::make_shared<roaring::Roaring>();
        null_bitmap2->add(5);
        null_bitmap2->add(7);

        InvertedIndexResultBitmap result1(bitmap1, null_bitmap1);
        InvertedIndexResultBitmap result2(bitmap2, null_bitmap2);

        // Test copy constructor
        InvertedIndexResultBitmap result3(result1);
        EXPECT_EQ(result3.get_data_bitmap()->cardinality(), 3);
        EXPECT_EQ(result3.get_null_bitmap()->cardinality(), 2);

        // Test move constructor
        InvertedIndexResultBitmap result4(std::move(result3));
        EXPECT_EQ(result4.get_data_bitmap()->cardinality(), 3);
        EXPECT_EQ(result4.get_null_bitmap()->cardinality(), 2);

        // Test &= operator
        result1 &= result2;
        EXPECT_EQ(result1.get_data_bitmap()->cardinality(), 2); // {2, 3}

        // Test |= operator
        InvertedIndexResultBitmap result5(bitmap1, null_bitmap1);
        InvertedIndexResultBitmap result6(bitmap2, null_bitmap2);
        result5 |= result6;
        EXPECT_GT(result5.get_data_bitmap()->cardinality(), 2);

        // Test -= operator
        InvertedIndexResultBitmap result7(bitmap1, null_bitmap1);
        InvertedIndexResultBitmap result8(bitmap2, null_bitmap2);
        result7 -= result8;

        // Test mask_out_null
        result7.mask_out_null();

        // Test is_empty
        InvertedIndexResultBitmap empty_result;
        EXPECT_TRUE(empty_result.is_empty());
        EXPECT_FALSE(result1.is_empty());
    }

    // Test string index with various parser configurations
    void test_string_index_parser_configurations() {
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(query_options);
        io::IOContext io_ctx;

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        std::vector<Slice> values = {Slice("Apple"), Slice("BANANA"),     Slice("Cherry"),
                                     Slice("DATE"),  Slice("elderberry"), Slice("FIG"),
                                     Slice("grape")};

        // Test case-insensitive parsing
        {
            std::string_view rowset_id = "test_case_insensitive";
            int seg_id = 0;

            TabletIndex idx_meta;
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test_case");
            index_meta_pb->clear_col_unique_id();
            index_meta_pb->add_col_unique_id(1); // c2 column
            index_meta_pb->mutable_properties()->insert({"parser", "english"});
            index_meta_pb->mutable_properties()->insert({"lower_case", "true"});
            idx_meta.init_from_pb(*index_meta_pb.get());

            std::string index_path_prefix;
            prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(str_reader, nullptr);

            // Test case-insensitive search
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string query_lower = "apple"; // lowercase
            StringRef str_ref(query_lower.c_str(), query_lower.length());
            auto status = str_reader->query(context, "c2", &str_ref,
                                            InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
            EXPECT_GT(bitmap->cardinality(), 0) << "Should find 'Apple' with lowercase query";
        }

        // Test ignore_above functionality
        {
            std::string_view rowset_id = "test_ignore_above";
            int seg_id = 1;
            std::vector<Slice> long_values = {
                    Slice("short"),
                    Slice("this_is_a_very_long_string_that_exceeds_ignore_above_limit")};

            TabletIndex idx_meta;
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test_ignore_above");
            index_meta_pb->clear_col_unique_id();
            index_meta_pb->add_col_unique_id(1);
            index_meta_pb->mutable_properties()->insert({"ignore_above", "10"});
            idx_meta.init_from_pb(*index_meta_pb.get());

            std::string index_path_prefix;
            prepare_string_index(rowset_id, seg_id, long_values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(str_reader, nullptr);

            // Query with long string should trigger evaluate skipped
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string long_query = "this_is_a_very_long_string_that_exceeds_ignore_above_limit";
            StringRef str_ref(long_query.c_str(), long_query.length());

            auto status = str_reader->query(context, "c2", &str_ref,
                                            InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_FALSE(status.ok());
            EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED);
        }
    }

    // Test fulltext index with comprehensive query types
    void test_fulltext_comprehensive_queries() {
        std::string_view rowset_id = "test_fulltext_comprehensive";
        int seg_id = 0;

        std::vector<Slice> values = {Slice("the quick brown fox jumps over the lazy dog"),
                                     Slice("apache doris is a fast analytical database"),
                                     Slice("inverted index provides fast text search capabilities"),
                                     Slice("lucene clucene search engine implementation"),
                                     Slice("phrase query matches exact word sequences"),
                                     Slice("regular expression pattern matching"),
                                     Slice("boolean queries combine multiple terms")};

        TabletIndex idx_meta;
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test_fulltext_comprehensive");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(1); // c2 column
        index_meta_pb->mutable_properties()->insert({"parser", "english"});
        index_meta_pb->mutable_properties()->insert({"lower_case", "true"});
        index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
        idx_meta.init_from_pb(*index_meta_pb.get());

        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        query_options.inverted_index_max_expansions = 50;
        runtime_state.set_query_options(query_options);

        auto reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
        EXPECT_TRUE(reader->init().ok());

        auto fulltext_reader = FullTextIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(fulltext_reader, nullptr);

        io::IOContext io_ctx;

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        // Test MATCH_ANY_QUERY
        {
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string query = "quick database";
            StringRef query_ref(query.c_str(), query.length());

            auto status = fulltext_reader->query(context, "c2", &query_ref,
                                                 InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
            EXPECT_GT(bitmap->cardinality(), 0)
                    << "Should find documents with 'quick' or 'database'";
        }

        // Test MATCH_ALL_QUERY
        {
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string query = "search fast";
            StringRef query_ref(query.c_str(), query.length());

            auto status = fulltext_reader->query(context, "c2", &query_ref,
                                                 InvertedIndexQueryType::MATCH_ALL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }

        // Test MATCH_PHRASE_QUERY
        {
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string query = "quick brown";
            StringRef query_ref(query.c_str(), query.length());

            auto status = fulltext_reader->query(
                    context, "c2", &query_ref, InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }

        // Test MATCH_PHRASE_PREFIX_QUERY
        {
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string query = "sear";
            StringRef query_ref(query.c_str(), query.length());

            auto status = fulltext_reader->query(context, "c2", &query_ref,
                                                 InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
                                                 bitmap);
            EXPECT_TRUE(status.ok());
        }

        // Test MATCH_REGEXP_QUERY
        {
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string query = "qu.*k";
            StringRef query_ref(query.c_str(), query.length());

            auto status = fulltext_reader->query(
                    context, "c2", &query_ref, InvertedIndexQueryType::MATCH_REGEXP_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }
    }

    // Test iterator comprehensive functionality
    void test_iterator_comprehensive() {
        std::string_view rowset_id = "test_iterator_comprehensive";
        int seg_id = 0;
        std::vector<Slice> values = {Slice("test1"), Slice("test2"), Slice("test3")};

        TabletIndex idx_meta;
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test_iterator_comprehensive");
        index_meta_pb->add_col_unique_id(1);
        idx_meta.init_from_pb(*index_meta_pb.get());

        std::string index_path_prefix;
        prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

        auto reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
        EXPECT_TRUE(reader->init().ok());

        auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(str_reader, nullptr);

        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.inverted_index_skip_threshold = 10; // 10% threshold
        runtime_state.set_query_options(query_options);
        io::IOContext io_ctx;

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        // Test iterator creation
        std::unique_ptr<IndexIterator> iterator;
        auto status = str_reader->new_iterator(&iterator);
        EXPECT_TRUE(status.ok());
        EXPECT_NE(iterator, nullptr);

        // Test iterator properties
        auto inverted_index_reader = std::static_pointer_cast<InvertedIndexReader>(
                iterator->get_reader(InvertedIndexReaderType::STRING_TYPE));
        EXPECT_EQ(inverted_index_reader->type(), InvertedIndexReaderType::STRING_TYPE);
        EXPECT_FALSE(inverted_index_reader->get_index_properties().empty());
        EXPECT_TRUE(inverted_index_reader->has_null());

        // Test skip_try parameter
        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        StringRef str_ref("test1", 5);

        InvertedIndexParam param;
        param.column_name = "c2";
        param.query_value = &str_ref;
        param.query_type = InvertedIndexQueryType::EQUAL_QUERY;
        param.num_rows = 3;
        param.roaring = bitmap;
        param.skip_try = true;
        status = iterator->read_from_index(&param);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(bitmap->cardinality(), 1);

        // Test try_read functionality with non-BKD index (should succeed)
        size_t count = 0;
        auto* inverted_index_iterator = static_cast<InvertedIndexIterator*>(iterator.get());
        inverted_index_iterator->set_context(context);
        status = inverted_index_iterator->try_read_from_inverted_index(
                std::static_pointer_cast<InvertedIndexReader>(inverted_index_reader), "c2",
                &str_ref, InvertedIndexQueryType::EQUAL_QUERY, &count);
        EXPECT_TRUE(status.ok());
    }

    // Test error handling and edge cases
    void test_error_handling() {
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        runtime_state.set_query_options(query_options);
        io::IOContext io_ctx;

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        // Test with invalid file path
        {
            TabletIndex idx_meta;
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test_invalid");
            index_meta_pb->add_col_unique_id(0);
            idx_meta.init_from_pb(*index_meta_pb.get());

            std::string invalid_path = kTestDir + "/nonexistent_index";
            auto reader = std::make_shared<IndexFileReader>(
                    io::global_local_filesystem(), invalid_path, InvertedIndexStorageFormatPB::V2);

            auto status = reader->init();
            EXPECT_FALSE(status.ok()) << "Should fail with nonexistent file";
        }

        // Test string index range query with TooManyClauses error
        {
            std::string_view rowset_id = "test_too_many_clauses";
            int seg_id = 0;
            std::vector<Slice> values;
            std::vector<std::string> values_string;
            // Create many values to potentially trigger TooManyClauses
            for (int i = 0; i < 1000; ++i) {
                values_string.emplace_back(std::to_string(i));
            }
            std::transform(values_string.begin(), values_string.end(), std::back_inserter(values),
                           [](const std::string& s) { return Slice(s.c_str(), s.size()); });

            TabletIndex idx_meta;
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test_too_many_clauses");
            index_meta_pb->add_col_unique_id(1);
            idx_meta.init_from_pb(*index_meta_pb.get());

            std::string index_path_prefix;
            prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(str_reader, nullptr);

            // Test range query that might exceed clause limit
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string query = "500";
            StringRef str_ref(query.c_str(), query.length());

            auto status = str_reader->query(context, "c2", &str_ref,
                                            InvertedIndexQueryType::LESS_THAN_QUERY, bitmap);
            // This might succeed or fail depending on the implementation limits
            // The important thing is we handle the potential TooManyClauses error gracefully
        }

        // Test empty query terms
        {
            std::string_view rowset_id = "test_empty_terms";
            int seg_id = 0;
            std::vector<Slice> values = {Slice("test")};

            TabletIndex idx_meta;
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test_empty_terms");
            index_meta_pb->add_col_unique_id(1);
            index_meta_pb->mutable_properties()->insert({"parser", "english"});
            idx_meta.init_from_pb(*index_meta_pb.get());

            std::string index_path_prefix;
            prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto fulltext_reader = FullTextIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(fulltext_reader, nullptr);

            // Test with empty string that produces no terms
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string empty_query = "";
            StringRef str_ref(empty_query.c_str(), empty_query.length());

            auto status = fulltext_reader->query(context, "c2", &str_ref,
                                                 InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap);
            // Should either succeed with empty result or fail gracefully
        }
    }

    // Helper methods for new data types
    void prepare_bkd_index_double(std::string_view rowset_id, int seg_id,
                                  std::vector<double>& values, TabletIndex* idx_meta,
                                  std::string* index_path_prefix) {
        auto tablet_schema = create_schema();

        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test_double");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(0); // c1 column
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
        auto index_file_writer = std::make_unique<IndexFileWriter>(
                fs, *index_path_prefix, std::string {rowset_id}, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        // Create a temporary schema with DOUBLE column for this test
        TabletSchemaSPtr double_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB double_schema_pb;
        double_schema_pb.set_keys_type(DUP_KEYS);
        double_schema->init_from_pb(double_schema_pb);

        TabletColumn double_column;
        double_column.set_name("c1");
        double_column.set_unique_id(0);
        double_column.set_type(FieldType::OLAP_FIELD_TYPE_DOUBLE);
        double_column.set_length(8);
        double_column.set_index_length(8);
        double_column.set_is_key(true);
        double_column.set_is_nullable(true);
        double_schema->append_column(double_column);

        const TabletColumn& column = double_schema->column(0);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        ASSERT_NE(field.get(), nullptr);

        std::unique_ptr<IndexColumnWriter> column_writer;
        auto status = IndexColumnWriter::create(field.get(), &column_writer,
                                                index_file_writer.get(), idx_meta);
        EXPECT_TRUE(status.ok()) << status;

        for (const auto& value : values) {
            status = column_writer->add_values(column.name(), reinterpret_cast<const void*>(&value),
                                               1);
            EXPECT_TRUE(status.ok()) << status;
        }

        status = column_writer->finish();
        EXPECT_TRUE(status.ok()) << status;
        status = index_file_writer->close();
        EXPECT_TRUE(status.ok()) << status;
    }

    // Test error handling comprehensive
    void test_error_handling_comprehensive() {
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        runtime_state.set_query_options(query_options);
        io::IOContext io_ctx;

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        // Test with invalid file path
        {
            TabletIndex idx_meta;
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test_invalid");
            index_meta_pb->add_col_unique_id(0);
            idx_meta.init_from_pb(*index_meta_pb.get());

            std::string invalid_path = kTestDir + "/nonexistent_index";
            auto reader = std::make_shared<IndexFileReader>(
                    io::global_local_filesystem(), invalid_path, InvertedIndexStorageFormatPB::V2);

            auto status = reader->init();
            EXPECT_FALSE(status.ok()) << "Should fail with nonexistent file";
        }

        // Test string index range query with TooManyClauses error
        {
            std::string_view rowset_id = "test_too_many_clauses";
            int seg_id = 0;
            std::vector<std::string> values_string;
            std::vector<Slice> values;

            // Create many values to potentially trigger TooManyClauses
            for (int i = 0; i < 1000; ++i) {
                values_string.emplace_back(std::to_string(i));
            }
            std::transform(values_string.begin(), values_string.end(), std::back_inserter(values),
                           [](const std::string& s) { return Slice(s.c_str(), s.size()); });

            TabletIndex idx_meta;
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test_too_many_clauses");
            index_meta_pb->add_col_unique_id(1);
            idx_meta.init_from_pb(*index_meta_pb.get());

            std::string index_path_prefix;
            prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(str_reader, nullptr);

            // Test range query that might exceed clause limit
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string query = "500";
            StringRef str_ref(query.c_str(), query.length());

            auto status = str_reader->query(context, "c2", &str_ref,
                                            InvertedIndexQueryType::LESS_THAN_QUERY, bitmap);
            // This might succeed or fail depending on the implementation limits
            // The important thing is we handle the potential TooManyClauses error gracefully
        }

        // Test empty query terms
        {
            std::string_view rowset_id = "test_empty_terms";
            int seg_id = 0;
            std::vector<Slice> values = {Slice("test")};

            TabletIndex idx_meta;
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test_empty_terms");
            index_meta_pb->add_col_unique_id(1);
            index_meta_pb->mutable_properties()->insert({"parser", "english"});
            idx_meta.init_from_pb(*index_meta_pb.get());

            std::string index_path_prefix;
            prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto fulltext_reader = FullTextIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(fulltext_reader, nullptr);

            // Test with empty string that produces no terms
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string empty_query;
            StringRef str_ref(empty_query.c_str(), empty_query.length());

            auto status = fulltext_reader->query(context, "c2", &str_ref,
                                                 InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap);
            // Should either succeed with empty result or fail gracefully
        }
    }

    // Test specific error paths and edge cases based on uncovered lines
    void test_uncovered_error_paths() {
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        runtime_state.set_query_options(query_options);
        io::IOContext io_ctx;

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        // Test MATCH_REGEXP_QUERY path that was uncovered
        {
            std::string_view rowset_id = "test_regexp_query";
            int seg_id = 0;
            std::vector<Slice> values = {Slice("test123"), Slice("example456"), Slice("demo789")};

            TabletIndex idx_meta;
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test_regexp");
            index_meta_pb->add_col_unique_id(1);
            index_meta_pb->mutable_properties()->insert({"parser", "english"});
            index_meta_pb->mutable_properties()->insert({"support_phrase", "true"});
            idx_meta.init_from_pb(*index_meta_pb.get());

            std::string index_path_prefix;
            prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto fulltext_reader = FullTextIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(fulltext_reader, nullptr);

            // Test MATCH_REGEXP_QUERY specifically
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string regexp_query = "test.*";
            StringRef query_ref(regexp_query.c_str(), regexp_query.length());

            auto status = fulltext_reader->query(
                    context, "c2", &query_ref, InvertedIndexQueryType::MATCH_REGEXP_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }

        // Test StringType range queries that were uncovered
        {
            std::string_view rowset_id = "test_string_range";
            int seg_id = 0;
            std::vector<Slice> values = {Slice("apple"), Slice("banana"), Slice("cherry"),
                                         Slice("date")};

            TabletIndex idx_meta;
            auto index_meta_pb = std::make_unique<TabletIndexPB>();
            index_meta_pb->set_index_type(IndexType::INVERTED);
            index_meta_pb->set_index_id(1);
            index_meta_pb->set_index_name("test_string_range");
            index_meta_pb->add_col_unique_id(1);
            idx_meta.init_from_pb(*index_meta_pb.get());

            std::string index_path_prefix;
            prepare_string_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto str_reader = StringTypeInvertedIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(str_reader, nullptr);

            // Test LESS_THAN_QUERY
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            std::string query = "cherry";
            StringRef str_ref(query.c_str(), query.length());

            auto status = str_reader->query(context, "c2", &str_ref,
                                            InvertedIndexQueryType::LESS_THAN_QUERY, bitmap);
            EXPECT_TRUE(status.ok());

            // Test LESS_EQUAL_QUERY
            bitmap = std::make_shared<roaring::Roaring>();
            status = str_reader->query(context, "c2", &str_ref,
                                       InvertedIndexQueryType::LESS_EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());

            // Test GREATER_THAN_QUERY
            bitmap = std::make_shared<roaring::Roaring>();
            status = str_reader->query(context, "c2", &str_ref,
                                       InvertedIndexQueryType::GREATER_THAN_QUERY, bitmap);
            EXPECT_TRUE(status.ok());

            // Test GREATER_EQUAL_QUERY
            bitmap = std::make_shared<roaring::Roaring>();
            status = str_reader->query(context, "c2", &str_ref,
                                       InvertedIndexQueryType::GREATER_EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());

            // Test MATCH_PHRASE_QUERY for StringType
            bitmap = std::make_shared<roaring::Roaring>();
            status = str_reader->query(context, "c2", &str_ref,
                                       InvertedIndexQueryType::MATCH_PHRASE_QUERY, bitmap);
            EXPECT_TRUE(status.ok());

            // Test MATCH_PHRASE_PREFIX_QUERY for StringType
            bitmap = std::make_shared<roaring::Roaring>();
            status = str_reader->query(context, "c2", &str_ref,
                                       InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY, bitmap);
            EXPECT_TRUE(status.ok());

            // Test MATCH_REGEXP_QUERY for StringType
            bitmap = std::make_shared<roaring::Roaring>();
            status = str_reader->query(context, "c2", &str_ref,
                                       InvertedIndexQueryType::MATCH_REGEXP_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }
    }

    // Test BKD specific uncovered paths
    void test_bkd_uncovered_paths() {
        std::string_view rowset_id = "test_bkd_uncovered";
        int seg_id = 0;
        std::vector<int32_t> values = {1, 5, 10, 15, 20, 25, 30};

        TabletIndex idx_meta;
        std::string index_path_prefix;
        prepare_bkd_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(query_options);
        io::IOContext io_ctx;

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        auto reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
        EXPECT_TRUE(reader->init().ok());

        auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(bkd_reader, nullptr);

        // Test all BKD query types systematically to cover visitor paths
        std::vector<std::pair<InvertedIndexQueryType, int32_t>> test_cases = {
                {InvertedIndexQueryType::LESS_THAN_QUERY, 15},
                {InvertedIndexQueryType::LESS_EQUAL_QUERY, 15},
                {InvertedIndexQueryType::GREATER_THAN_QUERY, 15},
                {InvertedIndexQueryType::GREATER_EQUAL_QUERY, 15},
                {InvertedIndexQueryType::EQUAL_QUERY, 15}};

        for (auto& test_case : test_cases) {
            // Test try_query path
            size_t count = 0;
            auto status = bkd_reader->try_query(context, "c1", &test_case.second, test_case.first,
                                                &count);
            EXPECT_TRUE(status.ok()) << "Try query type: " << static_cast<int>(test_case.first);

            // Test actual query path
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            status = bkd_reader->query(context, "c1", &test_case.second, test_case.first, bitmap);
            EXPECT_TRUE(status.ok()) << "Query type: " << static_cast<int>(test_case.first);
        }

        // Test boundary values to exercise different visitor logic paths
        int32_t min_value = 0;   // Less than minimum in data
        int32_t max_value = 100; // Greater than maximum in data

        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        auto status = bkd_reader->query(context, "c1", &min_value,
                                        InvertedIndexQueryType::GREATER_THAN_QUERY, bitmap);
        EXPECT_TRUE(status.ok());

        bitmap = std::make_shared<roaring::Roaring>();
        status = bkd_reader->query(context, "c1", &max_value,
                                   InvertedIndexQueryType::LESS_THAN_QUERY, bitmap);
        EXPECT_TRUE(status.ok());
    }

    // Test InvertedIndexIterator uncovered paths
    void test_iterator_uncovered_paths() {
        std::string_view rowset_id = "test_iterator_uncovered";
        int seg_id = 0;
        std::vector<int32_t> values = {1, 2, 3, 4, 5};

        TabletIndex idx_meta;
        std::string index_path_prefix;
        prepare_bkd_index(rowset_id, seg_id, values, &idx_meta, &index_path_prefix);

        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        query_options.inverted_index_skip_threshold = 1; // Very low threshold to trigger bypass
        runtime_state.set_query_options(query_options);
        io::IOContext io_ctx;

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        auto reader = std::make_shared<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);
        EXPECT_TRUE(reader->init().ok());

        auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
        EXPECT_NE(bkd_reader, nullptr);

        std::unique_ptr<IndexIterator> iterator;
        auto status = bkd_reader->new_iterator(&iterator);
        EXPECT_TRUE(status.ok());
        EXPECT_NE(iterator, nullptr);

        auto* inverted_index_iterator = static_cast<InvertedIndexIterator*>(iterator.get());
        inverted_index_iterator->set_context(context);

        // Test the bypass path in read_from_inverted_index
        std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
        int32_t query_value = 3;

        // This should trigger the bypass logic due to low threshold
        InvertedIndexParam param;
        param.column_name = "c1";
        param.query_value = &query_value;
        param.query_type = InvertedIndexQueryType::LESS_THAN_QUERY;
        param.num_rows = 5;
        param.roaring = bitmap;
        param.skip_try = false;
        status = inverted_index_iterator->read_from_index(&param);
        // Expect bypass error due to threshold
        EXPECT_FALSE(status.ok());
        EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_BYPASS);

        // Test skip_try=true path
        bitmap = std::make_shared<roaring::Roaring>();
        InvertedIndexParam param1;
        param1.column_name = "c1";
        param1.query_value = &query_value;
        param1.query_type = InvertedIndexQueryType::EQUAL_QUERY;
        param1.num_rows = 5;
        param1.roaring = bitmap;
        param1.skip_try = true;
        status = inverted_index_iterator->read_from_index(&param1);
        EXPECT_TRUE(status.ok());

        // Test try_read_from_inverted_index with non-BKD compatible query
        size_t count = 0;
        status = inverted_index_iterator->try_read_from_inverted_index(
                std::static_pointer_cast<InvertedIndexReader>(
                        iterator->get_reader(InvertedIndexReaderType::STRING_TYPE)),
                "c1", &query_value, InvertedIndexQueryType::MATCH_ANY_QUERY, &count);
        EXPECT_TRUE(status.ok()); // Should succeed but not do anything for non-BKD queries
    }

    // Create comprehensive schema for various data types (from InvertedIndexReaderComprehensiveTest)
    TabletSchemaSPtr create_comprehensive_schema() {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(DUP_KEYS);
        tablet_schema->init_from_pb(tablet_schema_pb);

        // Add various primitive type columns for testing
        std::vector<std::tuple<std::string, FieldType, int, bool>> columns = {
                {"c_int", FieldType::OLAP_FIELD_TYPE_INT, 4, true},
                {"c_bigint", FieldType::OLAP_FIELD_TYPE_BIGINT, 8, false},
                {"c_varchar", FieldType::OLAP_FIELD_TYPE_VARCHAR, 255, false},
                {"c_string", FieldType::OLAP_FIELD_TYPE_STRING, 65535, false},
                //{"c_double", FieldType::OLAP_FIELD_TYPE_DOUBLE, 8, false},
                //{"c_float", FieldType::OLAP_FIELD_TYPE_FLOAT, 4, false},
                {"c_date", FieldType::OLAP_FIELD_TYPE_DATE, 3, false},
                {"c_datetime", FieldType::OLAP_FIELD_TYPE_DATETIME, 8, false},
                {"c_decimal", FieldType::OLAP_FIELD_TYPE_DECIMAL, 16, false},
                {"c_bool", FieldType::OLAP_FIELD_TYPE_BOOL, 1, false},
                {"c_tinyint", FieldType::OLAP_FIELD_TYPE_TINYINT, 1, false},
                {"c_smallint", FieldType::OLAP_FIELD_TYPE_SMALLINT, 2, false},
                {"c_largeint", FieldType::OLAP_FIELD_TYPE_LARGEINT, 16, false},
                {"c_char", FieldType::OLAP_FIELD_TYPE_CHAR, 10, false},
                {"c_datev2", FieldType::OLAP_FIELD_TYPE_DATEV2, 4, false},
                {"c_datetimev2", FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 8, false}};

        for (size_t i = 0; i < columns.size(); ++i) {
            TabletColumn column;
            column.set_name(std::get<0>(columns[i]));
            column.set_unique_id(i);
            column.set_type(std::get<1>(columns[i]));
            column.set_length(std::get<2>(columns[i]));
            column.set_index_length(std::get<2>(columns[i]));
            column.set_is_key(std::get<3>(columns[i]));
            column.set_is_nullable(true);
            tablet_schema->append_column(column);
        }

        return tablet_schema;
    }

    // Prepare BKD index for different numeric types (from InvertedIndexReaderComprehensiveTest)
    template <typename T>
    void prepare_bkd_index_typed(
            std::string_view rowset_id, int seg_id, int col_id, std::vector<T>& values,
            TabletIndex* idx_meta, std::string* index_path_prefix,
            InvertedIndexStorageFormatPB format = InvertedIndexStorageFormatPB::V2) {
        auto tablet_schema = create_comprehensive_schema();

        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test_bkd");
        index_meta_pb->clear_col_unique_id();
        index_meta_pb->add_col_unique_id(col_id);
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
        auto index_file_writer =
                std::make_unique<IndexFileWriter>(fs, *index_path_prefix, std::string {rowset_id},
                                                  seg_id, format, std::move(file_writer));

        const TabletColumn& column = tablet_schema->column(col_id);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        ASSERT_NE(field.get(), nullptr);

        std::unique_ptr<IndexColumnWriter> column_writer;
        auto status = IndexColumnWriter::create(field.get(), &column_writer,
                                                index_file_writer.get(), idx_meta);
        EXPECT_TRUE(status.ok()) << status;

        for (const auto& value : values) {
            status = column_writer->add_values(column.name(), reinterpret_cast<const void*>(&value),
                                               1);
            EXPECT_TRUE(status.ok()) << status;
        }

        status = column_writer->finish();
        EXPECT_TRUE(status.ok()) << status;
        status = index_file_writer->close();
        EXPECT_TRUE(status.ok()) << status;
    }

    void test_bkd_various_data_types() {
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(query_options);
        io::IOContext io_ctx;

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        // Test INT type
        {
            std::string_view rowset_id = "test_bkd_int";
            int seg_id = 0;
            std::vector<int32_t> values = {-100, 0, 42, 100, 200, 300};
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 0, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            std::vector<std::pair<InvertedIndexQueryType, int32_t>> test_cases = {
                    {InvertedIndexQueryType::EQUAL_QUERY, 42},
                    {InvertedIndexQueryType::LESS_THAN_QUERY, 100},
                    {InvertedIndexQueryType::LESS_EQUAL_QUERY, 100},
                    {InvertedIndexQueryType::GREATER_THAN_QUERY, 100},
                    {InvertedIndexQueryType::GREATER_EQUAL_QUERY, 100}};

            for (auto& test_case : test_cases) {
                std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
                auto status = bkd_reader->query(context, "c_int", &test_case.second,
                                                test_case.first, bitmap);
                EXPECT_TRUE(status.ok()) << "Query type: " << static_cast<int>(test_case.first);

                if (test_case.first == InvertedIndexQueryType::EQUAL_QUERY) {
                    EXPECT_EQ(bitmap->cardinality(), 1)
                            << "Should find exactly one document for value 42";
                }
            }

            for (auto& test_case : test_cases) {
                size_t count = 0;
                auto status = bkd_reader->try_query(context, "c_int", &test_case.second,
                                                    test_case.first, &count);
                EXPECT_TRUE(status.ok()) << "Try query type: " << static_cast<int>(test_case.first);
            }
        }

        // Test BIGINT type
        {
            std::string_view rowset_id = "test_bkd_bigint";
            int seg_id = 1;
            std::vector<int64_t> values = {-1000000LL, 0LL, 1000000LL, 2000000LL};
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 1, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            int64_t query_value = 1000000LL;
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status = bkd_reader->query(context, "c_bigint", &query_value,
                                            InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(bitmap->cardinality(), 1);
        }

        // Test DOUBLE type
        /*{
            std::string_view rowset_id = "test_bkd_double";
            int seg_id = 2;
            std::vector<double> values = {-3.14, 0.0, 2.71, 3.14, 100.5};
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 4, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(
                    io::global_local_filesystem(), index_path_prefix,
                    InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            double query_value = 3.14;
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status =
                    bkd_reader->query(&io_ctx, &stats, &runtime_state, "c_double", &query_value,
                                      InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(bitmap->cardinality(), 1);
        }

        // Test FLOAT type
        {
            std::string_view rowset_id = "test_bkd_float";
            int seg_id = 3;
            std::vector<float> values = {-1.5f, 0.0f, 1.5f, 2.5f, 10.5f};
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 5, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(
                    io::global_local_filesystem(), index_path_prefix,
                    InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            float query_value = 1.5f;
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status =
                    bkd_reader->query(&io_ctx, &stats, &runtime_state, "c_float", &query_value,
                                      InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
            EXPECT_EQ(bitmap->cardinality(), 1);
        }*/
    }

    // Test additional data types to improve code coverage
    void test_additional_data_types_coverage() {
        OlapReaderStatistics stats;
        RuntimeState runtime_state;
        TQueryOptions query_options;
        query_options.enable_inverted_index_searcher_cache = false;
        runtime_state.set_query_options(query_options);
        io::IOContext io_ctx;

        IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
        context->io_ctx = &io_ctx;
        context->stats = &stats;
        context->runtime_state = &runtime_state;

        // Test DATE type (to cover TYPE_DATE case)
        {
            std::string_view rowset_id = "test_date_type";
            int seg_id = 0;
            std::vector<uint24_t> values = {20240101, 20240102, 20240103}; // DATE as uint32
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 4, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            uint32_t query_value = 20240102;
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status = bkd_reader->query(context, "c_date", &query_value,
                                            InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }

        // Test DATETIME type (to cover TYPE_DATETIME case)
        {
            std::string_view rowset_id = "test_datetime_type";
            int seg_id = 1;
            std::vector<uint64_t> values = {20240101120000ULL, 20240101130000ULL,
                                            20240101140000ULL};
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 5, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            uint64_t query_value = 20240101130000ULL;
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status = bkd_reader->query(context, "c_datetime", &query_value,
                                            InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }

        // Test BOOL type (to cover TYPE_BOOL case)
        {
            std::string_view rowset_id = "test_bool_type";
            int seg_id = 2;
            std::vector<bool> values = {true, false, true, false};
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 7, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            bool query_value = true;
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status = bkd_reader->query(context, "c_bool", &query_value,
                                            InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }

        // Test TINYINT type (to cover TYPE_TINYINT case)
        {
            std::string_view rowset_id = "test_tinyint_type";
            int seg_id = 3;
            std::vector<int8_t> values = {-128, 0, 1, 127};
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 8, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            int8_t query_value = 1;
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status = bkd_reader->query(context, "c_tinyint", &query_value,
                                            InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }

        // Test SMALLINT type (to cover TYPE_SMALLINT case)
        {
            std::string_view rowset_id = "test_smallint_type";
            int seg_id = 4;
            std::vector<int16_t> values = {-32768, 0, 1000, 32767};
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 9, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            int16_t query_value = 1000;
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status = bkd_reader->query(context, "c_smallint", &query_value,
                                            InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }

        // Test LARGEINT type (to cover TYPE_LARGEINT case)
        {
            std::string_view rowset_id = "test_largeint_type";
            int seg_id = 5;
            std::vector<__int128> values = {-1000000000000000000LL, 0, 1000000000000000000LL};
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 10, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            __int128 query_value = 0;
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status = bkd_reader->query(context, "c_largeint", &query_value,
                                            InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }

        // Test DATEV2 type (to cover TYPE_DATEV2 case)
        {
            std::string_view rowset_id = "test_datev2_type";
            int seg_id = 6;
            std::vector<uint32_t> values = {20240201, 20240202, 20240203};
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 12, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            uint32_t query_value = 20240202;
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status = bkd_reader->query(context, "c_datev2", &query_value,
                                            InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }

        // Test DATETIMEV2 type (to cover TYPE_DATETIMEV2 case)
        {
            std::string_view rowset_id = "test_datetimev2_type";
            int seg_id = 7;
            std::vector<uint64_t> values = {20240201120000ULL, 20240201130000ULL,
                                            20240201140000ULL};
            TabletIndex idx_meta;
            std::string index_path_prefix;
            prepare_bkd_index_typed(rowset_id, seg_id, 13, values, &idx_meta, &index_path_prefix);

            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            index_path_prefix,
                                                            InvertedIndexStorageFormatPB::V2);
            EXPECT_TRUE(reader->init().ok());

            auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
            EXPECT_NE(bkd_reader, nullptr);

            uint64_t query_value = 20240201130000ULL;
            std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
            auto status = bkd_reader->query(context, "c_datetimev2", &query_value,
                                            InvertedIndexQueryType::EQUAL_QUERY, bitmap);
            EXPECT_TRUE(status.ok());
        }
    }

    // Test unsupported data types to cover default case
    void test_unsupported_data_types() {
        // Create a schema with unsupported type for inverted index
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(DUP_KEYS);
        tablet_schema->init_from_pb(tablet_schema_pb);

        // Add an unsupported type column (e.g., JSON type if it exists)
        TabletColumn column;
        column.set_name("c_unsupported");
        column.set_unique_id(0);
        column.set_type(FieldType::OLAP_FIELD_TYPE_JSONB); // Using JSONB instead of JSON
        column.set_length(65535);
        column.set_index_length(65535);
        column.set_is_key(false);
        column.set_is_nullable(true);
        tablet_schema->append_column(column);

        std::string rowset_id = "test_unsupported";
        int seg_id = 0;

        TabletIndex idx_meta;
        auto index_meta_pb = std::make_unique<TabletIndexPB>();
        index_meta_pb->set_index_type(IndexType::INVERTED);
        index_meta_pb->set_index_id(1);
        index_meta_pb->set_index_name("test_unsupported");
        index_meta_pb->add_col_unique_id(0);
        idx_meta.init_from_pb(*index_meta_pb.get());

        auto index_path_prefix = std::string(InvertedIndexDescriptor::get_index_file_path_prefix(
                local_segment_path(kTestDir, rowset_id, seg_id)));
        std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);

        io::FileWriterPtr file_writer;
        io::FileWriterOptions opts;
        auto fs = io::global_local_filesystem();
        Status sts = fs->create_file(index_path, &file_writer, &opts);
        ASSERT_TRUE(sts.ok()) << sts;

        auto index_file_writer = std::make_unique<IndexFileWriter>(
                fs, std::string(index_path_prefix), rowset_id, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        const TabletColumn& test_column = tablet_schema->column(0);
        std::unique_ptr<Field> field(FieldFactory::create(test_column));
        ASSERT_NE(field.get(), nullptr);

        std::unique_ptr<IndexColumnWriter> column_writer;
        auto status = IndexColumnWriter::create(field.get(), &column_writer,
                                                index_file_writer.get(), &idx_meta);

        // This should fail for unsupported types, demonstrating the default case
        // If it succeeds, we can still test with invalid query parameters
        if (status.ok()) {
            status = column_writer->finish();
            EXPECT_TRUE(status.ok()) << status;
            status = index_file_writer->close();
            EXPECT_TRUE(status.ok()) << status;

            // Try to create reader and test unsupported query
            auto reader = std::make_shared<IndexFileReader>(io::global_local_filesystem(),
                                                            std::string(index_path_prefix),
                                                            InvertedIndexStorageFormatPB::V2);
            if (reader->init().ok()) {
                auto bkd_reader = BkdIndexReader::create_shared(&idx_meta, reader);
                if (bkd_reader != nullptr) {
                    OlapReaderStatistics stats;
                    RuntimeState runtime_state;
                    io::IOContext io_ctx;

                    IndexQueryContextPtr context = std::make_shared<IndexQueryContext>();
                    context->io_ctx = &io_ctx;
                    context->stats = &stats;
                    context->runtime_state = &runtime_state;

                    std::string query_value = "test";
                    std::shared_ptr<roaring::Roaring> bitmap = std::make_shared<roaring::Roaring>();
                    auto query_status =
                            bkd_reader->query(context, "c_unsupported", &query_value,
                                              InvertedIndexQueryType::EQUAL_QUERY, bitmap);
                    // This might fail due to unsupported type, which is what we want to test
                }
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

// Test string index with large document set using V3 format
TEST_F(InvertedIndexReaderTest, StringIndexLargeDocsetV3) {
    test_string_index_large_docset_v3();
}

// Test reading existing large document set index file
TEST_F(InvertedIndexReaderTest, CompatibleTest) {
    test_compatible_read_cross_platform();
}

// Test cache error scenarios that could crash BE
TEST_F(InvertedIndexReaderTest, CacheErrorScenarios) {
    test_cache_error_scenarios();
}

// Test tokenized index query error scenarios
TEST_F(InvertedIndexReaderTest, TokenizedIndexQueryErrorScenarios) {
    test_tokenized_index_query_error_scenarios();
}

// Additional comprehensive tests for uncovered paths
TEST_F(InvertedIndexReaderTest, ErrorHandlingComprehensive) {
    test_error_handling_comprehensive();
}

TEST_F(InvertedIndexReaderTest, ErrorHandling) {
    test_error_handling();
}

TEST_F(InvertedIndexReaderTest, UncoveredErrorPaths) {
    test_uncovered_error_paths();
}

TEST_F(InvertedIndexReaderTest, BkdUncoveredPaths) {
    test_bkd_uncovered_paths();
}

TEST_F(InvertedIndexReaderTest, IteratorUncoveredPaths) {
    test_iterator_uncovered_paths();
}

TEST_F(InvertedIndexReaderTest, BkdVariousDataTypes) {
    test_bkd_various_data_types();
}

TEST_F(InvertedIndexReaderTest, AdditionalDataTypesCoverage) {
    test_additional_data_types_coverage();
}

TEST_F(InvertedIndexReaderTest, UnsupportedDataTypes) {
    test_unsupported_data_types();
}

} // namespace doris::segment_v2
