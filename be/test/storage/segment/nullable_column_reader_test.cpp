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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {

namespace {
class NullMapOnlyTestFileColumnIterator final : public FileColumnIterator {
public:
    explicit NullMapOnlyTestFileColumnIterator(std::shared_ptr<ColumnReader> reader)
            : FileColumnIterator(std::move(reader)) {}

    void force_null_map_only() { _meta_read_mode = MetaReadMode::NULL_MAP_ONLY; }
};
} // namespace

class NullableColumnReaderTest : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(TEST_DIR).ok());
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
    }

    static constexpr std::string_view TEST_DIR = "./ut_dir/nullable_column_reader_test";
};

TEST_F(NullableColumnReaderTest, ReadByRowidsKeepsNullAndDataOrdinalsAligned) {
    constexpr size_t num_rows = 160;
    std::vector<int32_t> values(num_rows);
    std::vector<uint8_t> null_map(num_rows, 0);
    for (size_t i = 0; i < num_rows; ++i) {
        values[i] = 10000 + static_cast<int32_t>(i) * 37;
        const bool is_null = (i < 23 && i % 3 == 0) || (i >= 23 && i < 55) ||
                             (i >= 79 && i < 111 && (i / 2) % 2 == 0) || (i >= 111 && i < 140) ||
                             (i >= 140 && i % 2 == 0);
        null_map[i] = is_null;
    }

    auto fs = io::global_local_filesystem();
    const std::string file_path = std::string(TEST_DIR) + "/nullable_int";
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(fs->create_file(file_path, &file_writer).ok());

    ColumnMetaPB meta;
    meta.set_column_id(0);
    meta.set_unique_id(0);
    meta.set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_INT));
    meta.set_length(sizeof(int32_t));
    meta.set_encoding(EncodingTypePB::BIT_SHUFFLE);
    meta.set_compression(CompressionTypePB::LZ4F);
    meta.set_is_nullable(true);

    ColumnWriterOptions writer_options;
    writer_options.meta = &meta;
    TabletColumn tablet_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                               FieldType::OLAP_FIELD_TYPE_INT, true);
    std::unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(
            ColumnWriter::create(writer_options, &tablet_column, file_writer.get(), &writer).ok());
    ASSERT_TRUE(writer->init().ok());
    ASSERT_TRUE(writer->append(null_map.data(), values.data(), num_rows).ok());
    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(file_writer->close().ok());

    io::FileReaderSPtr file_reader;
    ASSERT_TRUE(fs->open_file(file_path, &file_reader).ok());
    ColumnReaderOptions reader_options;
    std::shared_ptr<ColumnReader> reader;
    ASSERT_TRUE(ColumnReader::create(reader_options, meta, num_rows, file_reader, &reader).ok());

    std::vector<rowid_t> sparse_rowids = {1, 6, 17, 24, 31, 48, 63, 64, 79, 96, 111, 127, 145, 159};
    std::vector<rowid_t> dense_rowids;
    for (rowid_t rowid = 18; rowid <= 120; ++rowid) {
        dense_rowids.push_back(rowid);
    }

    for (const auto& rowids : {sparse_rowids, dense_rowids}) {
        ColumnIteratorUPtr iterator;
        ASSERT_TRUE(reader->new_iterator(&iterator, &tablet_column).ok());
        ASSERT_NE(dynamic_cast<FileColumnIterator*>(iterator.get()), nullptr);

        ColumnIteratorOptions iterator_options;
        OlapReaderStatistics stats;
        iterator_options.file_reader = file_reader.get();
        iterator_options.stats = &stats;
        ASSERT_TRUE(iterator->init(iterator_options).ok());

        MutableColumnPtr result =
                ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
        ASSERT_TRUE(iterator->read_by_rowids(rowids.data(), rowids.size(), result).ok());
        ASSERT_EQ(result->size(), rowids.size());

        const auto& nullable = assert_cast<const ColumnNullable&>(*result);
        const auto& nested = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
        for (size_t i = 0; i < rowids.size(); ++i) {
            const rowid_t rowid = rowids[i];
            EXPECT_EQ(nullable.is_null_at(i), null_map[rowid] != 0) << "rowid=" << rowid;
            if (!nullable.is_null_at(i)) {
                EXPECT_EQ(nested.get_data()[i], values[rowid]) << "rowid=" << rowid;
            }
        }
    }

    NullMapOnlyTestFileColumnIterator null_map_iterator(reader);
    ColumnIteratorOptions iterator_options;
    OlapReaderStatistics stats;
    iterator_options.file_reader = file_reader.get();
    iterator_options.stats = &stats;
    ASSERT_TRUE(null_map_iterator.init(iterator_options).ok());
    null_map_iterator.force_null_map_only();

    auto read_and_check_null_map = [&](const std::vector<rowid_t>& rowids) {
        MutableColumnPtr result =
                ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
        ASSERT_TRUE(null_map_iterator.read_by_rowids(rowids.data(), rowids.size(), result).ok());
        ASSERT_EQ(result->size(), rowids.size());

        const auto& nullable = assert_cast<const ColumnNullable&>(*result);
        for (size_t i = 0; i < rowids.size(); ++i) {
            EXPECT_EQ(nullable.is_null_at(i), null_map[rowids[i]] != 0) << "rowid=" << rowids[i];
        }
    };

    read_and_check_null_map({1, 96, 145});
    ASSERT_TRUE(null_map_iterator.get_current_page()->contains(159));
    read_and_check_null_map({159});
    read_and_check_null_map({6, 24});
}

} // namespace doris::segment_v2
