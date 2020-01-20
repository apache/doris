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

#include "olap/rowset/segment_v2/bitmap_index_reader.h"
#include "olap/rowset/segment_v2/bitmap_index_writer.h"
#include "olap/key_coder.h"

#include <gtest/gtest.h>

#include "common/logging.h"
#include "env/env.h"
#include "olap/olap_common.h"
#include "olap/types.h"
#include "olap/column_block.h"
#include "util/file_utils.h"
#include "runtime/mem_tracker.h"
#include "runtime/mem_pool.h"

namespace doris {
namespace segment_v2 {

class IndexColumnReaderWriterTest : public testing::Test {
   public:
    IndexColumnReaderWriterTest() : _pool(&_tracker) { }
    virtual ~IndexColumnReaderWriterTest() {
    }
   private:
    MemTracker _tracker;
    MemPool _pool;
};

const std::string dname = "./ut_dir/index_column_reader_writer_test";

template<FieldType type>
void wirte_index_file(std::string& file_name, const void* values,
                      size_t value_count, size_t null_count,
                      BitmapIndexColumnPB* bitmap_index_meta) {
    const TypeInfo* type_info = get_type_info(type);
    FileUtils::create_dir(dname);
    std::string fname = dname + "/" + file_name;
    {
        std::unique_ptr<WritableFile> wfile;
        auto st = Env::Default()->new_writable_file(fname, &wfile);
        ASSERT_TRUE(st.ok());
        std::unique_ptr<BitmapIndexWriter> _bitmap_index_builder;
        BitmapIndexWriter::create(type_info, &_bitmap_index_builder);
        _bitmap_index_builder->add_values(values, value_count);
        _bitmap_index_builder->add_nulls(null_count);
        st = _bitmap_index_builder->finish(wfile.get(), bitmap_index_meta);
        ASSERT_TRUE(st.ok()) << "writer finish status:" << st.to_string();
        wfile.reset();
    }
}

template<FieldType type>
void get_bitmap_reader_iter(std::string& file_name, BitmapIndexColumnPB& bitmap_index_meta,
                            BitmapIndexReader** reader,
                            BitmapIndexIterator** iter) {
    file_name = dname + "/" + file_name;
    *reader = new BitmapIndexReader(file_name, bitmap_index_meta);
    auto st = (*reader)->load();
    ASSERT_TRUE(st.ok());

    st = (*reader)->new_iterator(iter);
    ASSERT_TRUE(st.ok());
}

TEST_F(IndexColumnReaderWriterTest, test_invert) {
    size_t num_uint8_rows = 1024 * 10;
    int* val = new int[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = i;
    }

    std::string file_name = "invert";
    BitmapIndexColumnPB bitmap_index_meta;
    wirte_index_file<OLAP_FIELD_TYPE_INT>(file_name, val, num_uint8_rows, 0,
                                          &bitmap_index_meta);
    {
        std::unique_ptr<RandomAccessFile> rfile;
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<OLAP_FIELD_TYPE_INT>(file_name, bitmap_index_meta, &reader, &iter);

        int value = 2;
        bool exact_match;
        Status st = iter->seek_dictionary(&value, &exact_match);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(exact_match);
        ASSERT_EQ(2, iter->current_ordinal());

        Roaring bitmap;
        iter->read_bitmap(iter->current_ordinal(), &bitmap);
        ASSERT_TRUE(Roaring::bitmapOf(1, 2) == bitmap);

        int value2 = 1024 * 9;
        st = iter->seek_dictionary(&value2, &exact_match);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(exact_match);
        ASSERT_EQ(1024 * 9, iter->current_ordinal());

        iter->read_union_bitmap(iter->current_ordinal(), iter->bitmap_nums(), &bitmap);
        ASSERT_EQ(1025, bitmap.cardinality());

        int value3 = 1024;
        iter->seek_dictionary(&value3, &exact_match);
        ASSERT_EQ(1024, iter->current_ordinal());

        Roaring bitmap2;
        iter->read_union_bitmap(0, iter->current_ordinal(), &bitmap2);
        ASSERT_EQ(1024, bitmap2.cardinality());

        delete reader;
        delete iter;
    }
}

TEST_F(IndexColumnReaderWriterTest, test_invert_2) {
    size_t num_uint8_rows = 1024 * 10;
    int* val = new int[num_uint8_rows];
    for (int i = 0; i < 1024; ++i) {
        val[i] = i;
    }

    for (int i = 1024; i < num_uint8_rows; ++i) {
        val[i] = i * 10;
    }

    std::string file_name = "invert2";
    BitmapIndexColumnPB bitmap_index_meta;
    wirte_index_file<OLAP_FIELD_TYPE_INT>(file_name, val, num_uint8_rows, 0,
                                          &bitmap_index_meta);

    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<OLAP_FIELD_TYPE_INT>(file_name, bitmap_index_meta, &reader, &iter);

        int value = 1026;
        bool exact_match;
        auto st = iter->seek_dictionary(&value, &exact_match);
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(!exact_match);

        ASSERT_EQ(1024, iter->current_ordinal());

        Roaring bitmap;
        iter->read_union_bitmap(0, iter->current_ordinal(), &bitmap);
        ASSERT_EQ(1024, bitmap.cardinality());

        delete reader;
        delete iter;
    }
}

TEST_F(IndexColumnReaderWriterTest, test_multi_pages) {
    size_t num_uint8_rows = 1024 * 1024;
    int64_t* val = new int64_t[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = random() + 10000;
    }
    val[1024 * 510] = 2019;

    std::string file_name = "mul";
    BitmapIndexColumnPB bitmap_index_meta;
    wirte_index_file<OLAP_FIELD_TYPE_BIGINT>(file_name, val, num_uint8_rows, 0,
                                             &bitmap_index_meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<OLAP_FIELD_TYPE_BIGINT>(file_name, bitmap_index_meta, &reader, &iter);

        int64_t value = 2019;
        bool exact_match;
        auto st = iter->seek_dictionary(&value, &exact_match);
        ASSERT_TRUE(st.ok()) << "status:" << st.to_string();
        ASSERT_EQ(0, iter->current_ordinal());

        Roaring bitmap;
        iter->read_bitmap(iter->current_ordinal(), &bitmap);
        ASSERT_EQ(1,bitmap.cardinality());

        delete reader;
        delete iter;
    }
}

TEST_F(IndexColumnReaderWriterTest, test_null) {
    size_t num_uint8_rows = 1024;
    int64_t* val = new int64_t[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = i;
    }

    std::string file_name = "null";
    BitmapIndexColumnPB bitmap_index_meta;
    wirte_index_file<OLAP_FIELD_TYPE_BIGINT>(file_name, val, num_uint8_rows, 30,
                                             &bitmap_index_meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<OLAP_FIELD_TYPE_BIGINT>(file_name, bitmap_index_meta, &reader, &iter);

        Roaring bitmap;
        iter->read_null_bitmap(&bitmap);
        ASSERT_EQ(30,bitmap.cardinality());

        delete reader;
        delete iter;
    }
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}