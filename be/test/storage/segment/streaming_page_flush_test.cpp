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

#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>

#include "common/config.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/tablet_schema_helper.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"

using std::string;

namespace doris {
namespace segment_v2 {

static const std::string TEST_DIR = "./ut_dir/streaming_page_flush_test";

class StreamingPageFlushTest : public testing::Test {
public:
    StreamingPageFlushTest() = default;
    ~StreamingPageFlushTest() override = default;

protected:
    void SetUp() override {
        config::disable_storage_page_cache = true;
        auto st = io::global_local_filesystem()->delete_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
    }
};

// Helper: write INT column with BIT_SHUFFLE encoding, read back and verify.
// Returns file size.
static uint64_t write_and_verify_int_column(bool streaming_enabled, const std::string& suffix,
                                            int num_rows) {
    bool old_val = config::enable_streaming_page_flush;
    config::enable_streaming_page_flush = streaming_enabled;

    ColumnMetaPB meta;
    std::string fname = TEST_DIR + "/int_col_" + suffix;
    auto fs = io::global_local_filesystem();

    TabletColumn tablet_col(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                            FieldType::OLAP_FIELD_TYPE_INT);

    // Write
    {
        io::FileWriterPtr file_writer;
        Status st = fs->create_file(fname, &file_writer);
        EXPECT_TRUE(st.ok()) << st;

        ColumnWriterOptions writer_opts;
        writer_opts.meta = &meta;
        writer_opts.meta->set_column_id(0);
        writer_opts.meta->set_unique_id(0);
        writer_opts.meta->set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
        writer_opts.meta->set_length(0);
        writer_opts.meta->set_encoding(BIT_SHUFFLE);
        writer_opts.meta->set_compression(segment_v2::CompressionTypePB::LZ4F);
        writer_opts.meta->set_is_nullable(true);
        writer_opts.need_zone_map = true;

        std::unique_ptr<ColumnWriter> writer;
        EXPECT_TRUE(
                ColumnWriter::create(writer_opts, &tablet_col, file_writer.get(), &writer).ok());
        st = writer->init();
        EXPECT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < num_rows; ++i) {
            int32_t val = i;
            bool is_null = (i % 7 == 0);
            st = writer->append(is_null, &val);
            EXPECT_TRUE(st.ok());
        }

        EXPECT_TRUE(writer->finish().ok());
        EXPECT_TRUE(writer->write_data().ok());
        EXPECT_TRUE(writer->write_ordinal_index().ok());
        EXPECT_TRUE(writer->write_zone_map().ok());
        EXPECT_TRUE(file_writer->close().ok());
    }

    config::enable_streaming_page_flush = old_val;

    // Read and verify using vectorized API
    io::FileReaderSPtr file_reader;
    EXPECT_EQ(fs->open_file(fname, &file_reader), Status::OK());

    int64_t file_size = 0;
    EXPECT_TRUE(fs->file_size(fname, &file_size).ok());

    // Sequential read
    {
        ColumnReaderOptions reader_opts;
        std::shared_ptr<ColumnReader> reader;
        auto st = ColumnReader::create(reader_opts, meta, num_rows, file_reader, &reader);
        EXPECT_TRUE(st.ok()) << st;

        ColumnIteratorUPtr iter;
        st = reader->new_iterator(&iter, &tablet_col);
        EXPECT_TRUE(st.ok());

        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        iter_opts.file_reader = file_reader.get();
        st = iter->init(iter_opts);
        EXPECT_TRUE(st.ok());

        int idx = 0;
        st = iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok());
        while (idx < num_rows) {
            vectorized::MutableColumnPtr dst = vectorized::ColumnNullable::create(
                    vectorized::ColumnInt32::create(), vectorized::ColumnUInt8::create());
            size_t rows_read = 1024;
            bool has_null = false;
            st = iter->next_batch(&rows_read, dst, &has_null);
            EXPECT_TRUE(st.ok());

            auto& nullable_col = assert_cast<vectorized::ColumnNullable&>(*dst);
            auto& null_map =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable_col.get_null_map_column())
                            .get_data();
            auto& data_col =
                    assert_cast<const vectorized::ColumnInt32&>(nullable_col.get_nested_column());

            for (size_t j = 0; j < rows_read; ++j) {
                bool expected_null = (idx % 7 == 0);
                EXPECT_EQ(expected_null, null_map[j] != 0) << "row " << idx;
                if (!expected_null) {
                    EXPECT_EQ(static_cast<int32_t>(idx), data_col.get_element(j)) << "row " << idx;
                }
                idx++;
            }
            if (rows_read < 1024) break;
        }
        EXPECT_EQ(idx, num_rows);
    }

    // Seek read (every 4025 rows)
    {
        ColumnReaderOptions reader_opts;
        std::shared_ptr<ColumnReader> reader;
        auto st = ColumnReader::create(reader_opts, meta, num_rows, file_reader, &reader);
        EXPECT_TRUE(st.ok());

        ColumnIteratorUPtr iter;
        st = reader->new_iterator(&iter, &tablet_col);
        EXPECT_TRUE(st.ok());

        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        iter_opts.file_reader = file_reader.get();
        st = iter->init(iter_opts);
        EXPECT_TRUE(st.ok());

        for (int rowid = 0; rowid < num_rows; rowid += 4025) {
            st = iter->seek_to_ordinal(rowid);
            EXPECT_TRUE(st.ok());

            vectorized::MutableColumnPtr dst = vectorized::ColumnNullable::create(
                    vectorized::ColumnInt32::create(), vectorized::ColumnUInt8::create());
            size_t rows_read = 1024;
            bool has_null = false;
            st = iter->next_batch(&rows_read, dst, &has_null);
            EXPECT_TRUE(st.ok());

            auto& nullable_col = assert_cast<vectorized::ColumnNullable&>(*dst);
            auto& null_map =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable_col.get_null_map_column())
                            .get_data();
            auto& data_col =
                    assert_cast<const vectorized::ColumnInt32&>(nullable_col.get_nested_column());

            for (size_t j = 0; j < rows_read; ++j) {
                int idx = rowid + static_cast<int>(j);
                if (idx >= num_rows) break;
                bool expected_null = (idx % 7 == 0);
                EXPECT_EQ(expected_null, null_map[j] != 0) << "row " << idx;
                if (!expected_null) {
                    EXPECT_EQ(idx, data_col.get_element(j)) << "row " << idx;
                }
            }
        }
    }
    return file_size;
}

// Helper: write VARCHAR column with DICT_ENCODING, read back and verify.
static void write_and_verify_varchar_column(bool streaming_enabled, const std::string& suffix,
                                            int num_rows) {
    bool old_val = config::enable_streaming_page_flush;
    config::enable_streaming_page_flush = streaming_enabled;

    ColumnMetaPB meta;
    std::string fname = TEST_DIR + "/varchar_col_" + suffix;
    auto fs = io::global_local_filesystem();

    auto tablet_col_ptr = create_varchar_key(1);
    TabletColumn& tablet_col = *tablet_col_ptr;

    // Prepare data
    std::vector<Slice> values(num_rows);
    std::vector<std::string> str_storage(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        str_storage[i] = "value_" + std::to_string(i % 100);
        values[i] = Slice(str_storage[i]);
    }

    // Write
    {
        io::FileWriterPtr file_writer;
        Status st = fs->create_file(fname, &file_writer);
        EXPECT_TRUE(st.ok()) << st;

        ColumnWriterOptions writer_opts;
        writer_opts.meta = &meta;
        writer_opts.meta->set_column_id(0);
        writer_opts.meta->set_unique_id(0);
        writer_opts.meta->set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_VARCHAR));
        writer_opts.meta->set_length(128);
        writer_opts.meta->set_encoding(DICT_ENCODING);
        writer_opts.meta->set_compression(segment_v2::CompressionTypePB::LZ4F);
        writer_opts.meta->set_is_nullable(true);
        writer_opts.need_zone_map = false;

        std::unique_ptr<ColumnWriter> writer;
        EXPECT_TRUE(
                ColumnWriter::create(writer_opts, &tablet_col, file_writer.get(), &writer).ok());
        st = writer->init();
        EXPECT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < num_rows; ++i) {
            bool is_null = (i % 11 == 0);
            st = writer->append(is_null, &values[i]);
            EXPECT_TRUE(st.ok());
        }

        EXPECT_TRUE(writer->finish().ok());
        EXPECT_TRUE(writer->write_data().ok());
        EXPECT_TRUE(writer->write_ordinal_index().ok());
        EXPECT_TRUE(file_writer->close().ok());
    }

    config::enable_streaming_page_flush = old_val;

    // Read and verify using vectorized API
    io::FileReaderSPtr file_reader;
    EXPECT_EQ(fs->open_file(fname, &file_reader), Status::OK());

    {
        ColumnReaderOptions reader_opts;
        std::shared_ptr<ColumnReader> reader;
        auto st = ColumnReader::create(reader_opts, meta, num_rows, file_reader, &reader);
        EXPECT_TRUE(st.ok()) << st;

        ColumnIteratorUPtr iter;
        st = reader->new_iterator(&iter, &tablet_col);
        EXPECT_TRUE(st.ok());

        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        iter_opts.file_reader = file_reader.get();
        st = iter->init(iter_opts);
        EXPECT_TRUE(st.ok());

        int idx = 0;
        st = iter->seek_to_ordinal(0);
        EXPECT_TRUE(st.ok());
        while (idx < num_rows) {
            vectorized::MutableColumnPtr dst = vectorized::ColumnNullable::create(
                    vectorized::ColumnString::create(), vectorized::ColumnUInt8::create());
            size_t rows_read = 1024;
            bool has_null = false;
            st = iter->next_batch(&rows_read, dst, &has_null);
            EXPECT_TRUE(st.ok());

            auto& nullable_col = assert_cast<vectorized::ColumnNullable&>(*dst);
            auto& null_map =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable_col.get_null_map_column())
                            .get_data();
            auto& data_col =
                    assert_cast<const vectorized::ColumnString&>(nullable_col.get_nested_column());

            for (size_t j = 0; j < rows_read; ++j) {
                bool expected_null = (idx % 11 == 0);
                EXPECT_EQ(expected_null, null_map[j] != 0) << "row " << idx;
                if (!expected_null) {
                    std::string expected = "value_" + std::to_string(idx % 100);
                    auto actual = data_col.get_data_at(j);
                    EXPECT_EQ(expected, std::string(actual.data, actual.size)) << "row " << idx;
                }
                idx++;
            }
            if (rows_read < 1024) break;
        }
        EXPECT_EQ(idx, num_rows);
    }
}

// Test 1: streaming=false (traditional mode) with INT BIT_SHUFFLE
TEST_F(StreamingPageFlushTest, test_int_traditional_mode) {
    write_and_verify_int_column(false, "traditional", 10000);
}

// Test 2: streaming=true with INT BIT_SHUFFLE
TEST_F(StreamingPageFlushTest, test_int_streaming_mode) {
    write_and_verify_int_column(true, "streaming", 10000);
}

// Test 3: streaming=false (traditional mode) with VARCHAR DICT_ENCODING
TEST_F(StreamingPageFlushTest, test_varchar_traditional_mode) {
    write_and_verify_varchar_column(false, "traditional", 10000);
}

// Test 4: streaming=true with VARCHAR DICT_ENCODING
TEST_F(StreamingPageFlushTest, test_varchar_streaming_mode) {
    write_and_verify_varchar_column(true, "streaming", 10000);
}

// Test 5: Both modes produce the same file size (INT) — single column write
TEST_F(StreamingPageFlushTest, test_int_both_modes_same_file_size) {
    int num_rows = 5000;
    auto file_size_traditional = write_and_verify_int_column(false, "cmp_traditional", num_rows);
    auto file_size_streaming = write_and_verify_int_column(true, "cmp_streaming", num_rows);
    EXPECT_EQ(file_size_traditional, file_size_streaming);
}

// Test 6: Both modes work for VARCHAR/DICT
TEST_F(StreamingPageFlushTest, test_varchar_both_modes) {
    write_and_verify_varchar_column(false, "dict_cmp_traditional", 5000);
    write_and_verify_varchar_column(true, "dict_cmp_streaming", 5000);
}

// Test 7: Small data set (fits in single page) — streaming mode
TEST_F(StreamingPageFlushTest, test_small_data_streaming) {
    write_and_verify_int_column(true, "small_streaming", 10);
}

// Test 8: Small data set (fits in single page) — traditional mode
TEST_F(StreamingPageFlushTest, test_small_data_traditional) {
    write_and_verify_int_column(false, "small_traditional", 10);
}

} // namespace segment_v2
} // namespace doris
