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

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

static const std::string TEST_DIR = "./ut_dir/column_compression_roundtrip_test";

class ColumnCompressionRoundtripTest : public ::testing::Test {
protected:
    void SetUp() override {
        _old_disable_storage_page_cache = config::disable_storage_page_cache;
        config::disable_storage_page_cache = true;
        auto st = io::global_local_filesystem()->delete_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = io::global_local_filesystem()->create_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st.to_string();
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
        config::disable_storage_page_cache = _old_disable_storage_page_cache;
    }

private:
    bool _old_disable_storage_page_cache = false;
};

// Write `num_rows` INT values through a segment column configured with the given
// compression codec + level, reopen the file, read every value back, and assert
// the round-trip is lossless. This is an integration smoke test of the full
// per-column compression plumbing: the writer picks a level-aware codec, the
// codec/level is persisted into ColumnMetaPB, and the reader decompresses.
//
// NOTE: a lossless round-trip is level-INDEPENDENT -- decompression never needs
// the level, so this test cannot by itself prove the configured level reached
// the codec. The observable level effect (different level => different output)
// is guarded by BlockCompressionTest.DifferentLevelsProduceDifferentOutput at
// the codec layer and by SegmentBytesDifferWithLevel below at the segment layer.
//
// A tiny data_page_size forces many data pages so the codec is invoked once per
// page.
static void test_int_roundtrip(CompressionTypePB compression, int compression_level,
                               const std::string& test_name) {
    const int32_t num_rows = 4096;
    std::vector<int32_t> src(num_rows);
    for (int32_t i = 0; i < num_rows; ++i) {
        // Repetitive-but-varying data so compression actually kicks in.
        src[i] = (i % 97) * 31 + (i / 97);
    }

    ColumnMetaPB meta;
    std::string fname = TEST_DIR + "/" + test_name;
    auto fs = io::global_local_filesystem();

    // ---- write ----
    {
        io::FileWriterPtr file_writer;
        Status st = fs->create_file(fname, &file_writer);
        ASSERT_TRUE(st.ok()) << st.to_string();

        ColumnWriterOptions writer_opts;
        writer_opts.meta = &meta;
        writer_opts.meta->set_column_id(0);
        writer_opts.meta->set_unique_id(0);
        writer_opts.meta->set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
        writer_opts.meta->set_length(0);
        writer_opts.meta->set_encoding(PLAIN_ENCODING);
        writer_opts.meta->set_compression(compression);
        if (compression_level > 0) {
            writer_opts.meta->set_compression_level(compression_level);
        }
        writer_opts.meta->set_is_nullable(false);
        writer_opts.data_page_size = 128;

        TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                            FieldType::OLAP_FIELD_TYPE_INT);
        std::unique_ptr<ColumnWriter> writer;
        st = ColumnWriter::create(writer_opts, &column, file_writer.get(), &writer);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = writer->init();
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int32_t i = 0; i < num_rows; ++i) {
            st = writer->append(false, &src[i]);
            ASSERT_TRUE(st.ok()) << st.to_string();
        }

        ASSERT_TRUE(writer->finish().ok());
        ASSERT_TRUE(writer->write_data().ok());
        ASSERT_TRUE(writer->write_ordinal_index().ok());
        ASSERT_TRUE(file_writer->close().ok());
    }

    // Codec must be persisted so the reader can reconstruct the decompressor.
    // (compression_level is round-tripped through ColumnMetaPB by the caller and
    // consumed by the writer only; asserting it here would be tautological.)
    ASSERT_EQ(meta.compression(), compression);

    // ---- read back ----
    io::FileReaderSPtr file_reader;
    ASSERT_TRUE(fs->open_file(fname, &file_reader).ok());

    ColumnReaderOptions reader_opts;
    std::shared_ptr<ColumnReader> reader;
    ASSERT_TRUE(ColumnReader::create(reader_opts, meta, num_rows, file_reader, &reader).ok());

    TabletColumn read_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                             FieldType::OLAP_FIELD_TYPE_INT);
    ColumnIteratorUPtr iter;
    ASSERT_TRUE(reader->new_iterator(&iter, &read_column).ok());

    ColumnIteratorOptions iter_opts;
    OlapReaderStatistics stats;
    iter_opts.stats = &stats;
    iter_opts.file_reader = file_reader.get();
    ASSERT_TRUE(iter->init(iter_opts).ok());
    ASSERT_TRUE(iter->seek_to_ordinal(0).ok());

    MutableColumnPtr dst = ColumnInt32::create();
    size_t total_read = 0;
    while (total_read < static_cast<size_t>(num_rows)) {
        size_t rows_read = 1024;
        bool has_null = false;
        ASSERT_TRUE(iter->next_batch(&rows_read, dst, &has_null).ok());
        if (rows_read == 0) {
            break;
        }
        total_read += rows_read;
    }
    ASSERT_EQ(total_read, static_cast<size_t>(num_rows));

    const auto& int_col = assert_cast<const ColumnInt32&>(*dst);
    ASSERT_EQ(int_col.size(), static_cast<size_t>(num_rows));
    for (int32_t i = 0; i < num_rows; ++i) {
        ASSERT_EQ(src[i], int_col.get_element(i))
                << "codec=" << compression << " level=" << compression_level << " idx=" << i;
    }
}

TEST_F(ColumnCompressionRoundtripTest, Lz4fNoLevel) {
    test_int_roundtrip(CompressionTypePB::LZ4F, 0, "int_lz4f");
}

TEST_F(ColumnCompressionRoundtripTest, ZstdWithLevel) {
    test_int_roundtrip(CompressionTypePB::ZSTD, 9, "int_zstd_l9");
}

TEST_F(ColumnCompressionRoundtripTest, ZstdMaxLevel) {
    test_int_roundtrip(CompressionTypePB::ZSTD, 22, "int_zstd_l22");
}

TEST_F(ColumnCompressionRoundtripTest, Lz4hcWithLevel) {
    test_int_roundtrip(CompressionTypePB::LZ4HC, 12, "int_lz4hc_l12");
}

TEST_F(ColumnCompressionRoundtripTest, ZstdDefaultLevelFallback) {
    // level == 0 means "use codec default"; must still round-trip via the singleton.
    test_int_roundtrip(CompressionTypePB::ZSTD, 0, "int_zstd_default");
}

TEST_F(ColumnCompressionRoundtripTest, SnappyNoLevel) {
    test_int_roundtrip(CompressionTypePB::SNAPPY, 0, "int_snappy");
}

TEST_F(ColumnCompressionRoundtripTest, ZlibNoLevel) {
    test_int_roundtrip(CompressionTypePB::ZLIB, 0, "int_zlib");
}

// Segment-layer level oracle. Write the SAME data through a real segment column
// at two ZSTD levels using a realistic page size, then compare the resulting
// on-disk bytes. This is the end-to-end guard that the writer forwards
// meta.compression_level() into the codec (column_writer.cpp): if the level were
// dropped, both writes would use the codec default and the files would be
// byte-identical. (We compare bytes, not sizes: ZSTD level is search effort, not
// a monotonic size bound, so a higher level is not guaranteed to be smaller.)
static std::string write_int_segment_bytes(int compression_level, int32_t num_rows,
                                           const std::string& test_name) {
    std::vector<int32_t> src(num_rows);
    for (int32_t i = 0; i < num_rows; ++i) {
        src[i] = ((i / 64) % 41) * 100 + (i % 7);
    }

    ColumnMetaPB meta;
    std::string fname = TEST_DIR + "/" + test_name;
    auto fs = io::global_local_filesystem();

    io::FileWriterPtr file_writer;
    EXPECT_TRUE(fs->create_file(fname, &file_writer).ok());

    ColumnWriterOptions writer_opts;
    writer_opts.meta = &meta;
    writer_opts.meta->set_column_id(0);
    writer_opts.meta->set_unique_id(0);
    writer_opts.meta->set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    writer_opts.meta->set_length(0);
    writer_opts.meta->set_encoding(PLAIN_ENCODING);
    writer_opts.meta->set_compression(CompressionTypePB::ZSTD);
    if (compression_level > 0) {
        writer_opts.meta->set_compression_level(compression_level);
    }
    writer_opts.meta->set_is_nullable(false);
    writer_opts.data_page_size = 1024 * 1024;

    TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                        FieldType::OLAP_FIELD_TYPE_INT);
    std::unique_ptr<ColumnWriter> writer;
    EXPECT_TRUE(ColumnWriter::create(writer_opts, &column, file_writer.get(), &writer).ok());
    EXPECT_TRUE(writer->init().ok());
    for (int32_t i = 0; i < num_rows; ++i) {
        EXPECT_TRUE(writer->append(false, &src[i]).ok());
    }
    EXPECT_TRUE(writer->finish().ok());
    EXPECT_TRUE(writer->write_data().ok());
    EXPECT_TRUE(writer->write_ordinal_index().ok());
    EXPECT_TRUE(file_writer->close().ok());

    int64_t size = 0;
    EXPECT_TRUE(fs->file_size(fname, &size).ok());
    EXPECT_GT(size, 0);

    io::FileReaderSPtr file_reader;
    EXPECT_TRUE(fs->open_file(fname, &file_reader).ok());
    std::string bytes(static_cast<size_t>(size), '\0');
    size_t bytes_read = 0;
    EXPECT_TRUE(file_reader->read_at(0, Slice(bytes.data(), bytes.size()), &bytes_read).ok());
    EXPECT_EQ(bytes_read, static_cast<size_t>(size));
    return bytes;
}

TEST_F(ColumnCompressionRoundtripTest, SegmentBytesDifferWithLevel) {
    const int32_t num_rows = 512 * 1024; // multi-page column
    std::string low = write_int_segment_bytes(1, num_rows, "int_seg_zstd_l1");
    std::string high = write_int_segment_bytes(22, num_rows, "int_seg_zstd_l22");
    EXPECT_NE(low, high) << "ZSTD level 1 and level 22 produced byte-identical segments; "
                         << "the writer likely dropped the per-column compression level";
}

} // namespace doris::segment_v2
