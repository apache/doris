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

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <stdlib.h>

#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <utility>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bitmap_index_reader.h"
#include "olap/rowset/segment_v2/bitmap_index_writer.h"
#include "olap/types.h"
#include "testutil/test_util.h"

namespace doris {

using FileSystemSPtr = std::shared_ptr<io::FileSystem>;

namespace segment_v2 {
using roaring::Roaring;

class BitmapIndexTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/bitmap_index_test";

    void SetUp() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_and_create_directory(kTestDir).ok());
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }
};

template <FieldType type>
void write_index_file(const std::string& filename, FileSystemSPtr fs, const void* values,
                      size_t value_count, size_t null_count, ColumnIndexMetaPB* meta) {
    const auto* type_info = get_scalar_type_info<type>();
    {
        io::FileWriterPtr file_writer;
        EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());

        std::unique_ptr<BitmapIndexWriter> writer;
        static_cast<void>(BitmapIndexWriter::create(type_info, &writer));
        writer->add_values(values, value_count);
        writer->add_nulls(null_count);
        EXPECT_TRUE(writer->finish(file_writer.get(), meta).ok());
        EXPECT_EQ(BITMAP_INDEX, meta->type());
        EXPECT_TRUE(file_writer->close().ok());
    }
}

template <FieldType type>
void get_bitmap_reader_iter(const std::string& file_name, const ColumnIndexMetaPB& meta,
                            BitmapIndexReader** reader, BitmapIndexIterator** iter) {
    io::FileReaderSPtr file_reader;
    ASSERT_EQ(io::global_local_filesystem()->open_file(file_name, &file_reader), Status::OK());
    *reader = new BitmapIndexReader(std::move(file_reader), meta.bitmap_index());
    auto st = (*reader)->load(true, false);
    EXPECT_TRUE(st.ok());

    st = (*reader)->new_iterator(iter);
    EXPECT_TRUE(st.ok());
}

TEST_F(BitmapIndexTest, test_invert) {
    size_t num_uint8_rows = 1024 * 10;
    int* val = new int[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = i;
    }

    std::string file_name = kTestDir + "/invert";
    ColumnIndexMetaPB meta;
    write_index_file<FieldType::OLAP_FIELD_TYPE_INT>(file_name, io::global_local_filesystem(), val,
                                                     num_uint8_rows, 0, &meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<FieldType::OLAP_FIELD_TYPE_INT>(file_name, meta, &reader, &iter);
        EXPECT_FALSE(iter == nullptr);

        int value = 2;
        bool exact_match;
        Status st = iter->seek_dictionary(&value, &exact_match);
        EXPECT_TRUE(st.ok());
        EXPECT_TRUE(exact_match);
        EXPECT_EQ(2, iter->current_ordinal());

        Roaring bitmap;
        static_cast<void>(iter->read_bitmap(iter->current_ordinal(), &bitmap));
        EXPECT_TRUE(Roaring::bitmapOf(1, 2) == bitmap);

        int value2 = 1024 * 9;
        st = iter->seek_dictionary(&value2, &exact_match);
        EXPECT_TRUE(st.ok());
        EXPECT_TRUE(exact_match);
        EXPECT_EQ(1024 * 9, iter->current_ordinal());

        static_cast<void>(
                iter->read_union_bitmap(iter->current_ordinal(), iter->bitmap_nums(), &bitmap));
        EXPECT_EQ(1025, bitmap.cardinality());

        int value3 = 1024;
        static_cast<void>(iter->seek_dictionary(&value3, &exact_match));
        EXPECT_EQ(1024, iter->current_ordinal());

        Roaring bitmap2;
        static_cast<void>(iter->read_union_bitmap(0, iter->current_ordinal(), &bitmap2));
        EXPECT_EQ(1024, bitmap2.cardinality());

        delete reader;
        delete iter;
    }
    delete[] val;
}

TEST_F(BitmapIndexTest, test_invert_2) {
    size_t num_uint8_rows = 1024 * 10;
    int* val = new int[num_uint8_rows];
    for (int i = 0; i < 1024; ++i) {
        val[i] = i;
    }

    for (int i = 1024; i < num_uint8_rows; ++i) {
        val[i] = i * 10;
    }

    std::string file_name = kTestDir + "/invert2";
    ColumnIndexMetaPB meta;
    write_index_file<FieldType::OLAP_FIELD_TYPE_INT>(file_name, io::global_local_filesystem(), val,
                                                     num_uint8_rows, 0, &meta);

    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<FieldType::OLAP_FIELD_TYPE_INT>(file_name, meta, &reader, &iter);

        int value = 1026;
        bool exact_match;
        auto st = iter->seek_dictionary(&value, &exact_match);
        EXPECT_TRUE(st.ok());
        EXPECT_TRUE(!exact_match);

        EXPECT_EQ(1024, iter->current_ordinal());

        Roaring bitmap;
        static_cast<void>(iter->read_union_bitmap(0, iter->current_ordinal(), &bitmap));
        EXPECT_EQ(1024, bitmap.cardinality());

        delete reader;
        delete iter;
    }
    delete[] val;
}

TEST_F(BitmapIndexTest, test_multi_pages) {
    size_t times = LOOP_LESS_OR_MORE(1, 1024);
    size_t num_uint8_rows = times * 1024;
    int64_t* val = new int64_t[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = random() + 10000;
    }
    val[times * 510] = 2019;

    std::string file_name = kTestDir + "/mul";
    ColumnIndexMetaPB meta;
    write_index_file<FieldType::OLAP_FIELD_TYPE_BIGINT>(file_name, io::global_local_filesystem(),
                                                        val, num_uint8_rows, 0, &meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<FieldType::OLAP_FIELD_TYPE_BIGINT>(file_name, meta, &reader, &iter);

        int64_t value = 2019;
        bool exact_match;
        auto st = iter->seek_dictionary(&value, &exact_match);
        EXPECT_TRUE(st.ok()) << "status:" << st.to_string();
        EXPECT_EQ(0, iter->current_ordinal());

        Roaring bitmap;
        static_cast<void>(iter->read_bitmap(iter->current_ordinal(), &bitmap));
        EXPECT_EQ(1, bitmap.cardinality());

        delete reader;
        delete iter;
    }
    delete[] val;
}

TEST_F(BitmapIndexTest, test_null) {
    size_t num_uint8_rows = 1024;
    int64_t* val = new int64_t[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = i;
    }

    std::string file_name = kTestDir + "/null";
    ColumnIndexMetaPB meta;
    write_index_file<FieldType::OLAP_FIELD_TYPE_BIGINT>(file_name, io::global_local_filesystem(),
                                                        val, num_uint8_rows, 30, &meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<FieldType::OLAP_FIELD_TYPE_BIGINT>(file_name, meta, &reader, &iter);

        Roaring bitmap;
        static_cast<void>(iter->read_null_bitmap(&bitmap));
        EXPECT_EQ(30, bitmap.cardinality());

        delete reader;
        delete iter;
    }
    delete[] val;
}

} // namespace segment_v2
} // namespace doris
