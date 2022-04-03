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

#include <string>

#include "common/logging.h"
#include "env/env.h"
#include "olap/fs/block_manager.h"
#include "olap/fs/fs_util.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/bitmap_index_reader.h"
#include "olap/rowset/segment_v2/bitmap_index_writer.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "test_util/test_util.h"
#include "util/file_utils.h"

namespace doris {
namespace segment_v2 {
using roaring::Roaring;

class BitmapIndexTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/bitmap_index_test";
    BitmapIndexTest() : _tracker(new MemTracker()), _pool(_tracker.get()) {}

    void SetUp() override {
        if (FileUtils::check_exist(kTestDir)) {
            ASSERT_TRUE(FileUtils::remove_all(kTestDir).ok());
        }
        ASSERT_TRUE(FileUtils::create_dir(kTestDir).ok());
    }
    void TearDown() override {
        if (FileUtils::check_exist(kTestDir)) {
            ASSERT_TRUE(FileUtils::remove_all(kTestDir).ok());
        }
    }

private:
    std::shared_ptr<MemTracker> _tracker;
    MemPool _pool;
};

template <FieldType type>
void write_index_file(std::string& filename, const void* values, size_t value_count,
                      size_t null_count, ColumnIndexMetaPB* meta) {
    const auto* type_info = get_scalar_type_info(type);
    {
        std::unique_ptr<fs::WritableBlock> wblock;
        fs::CreateBlockOptions opts(filename);
        ASSERT_TRUE(
                fs::fs_util::block_manager(TStorageMedium::HDD)->create_block(opts, &wblock).ok());

        std::unique_ptr<BitmapIndexWriter> writer;
        BitmapIndexWriter::create(type_info, &writer);
        writer->add_values(values, value_count);
        writer->add_nulls(null_count);
        ASSERT_TRUE(writer->finish(wblock.get(), meta).ok());
        ASSERT_EQ(BITMAP_INDEX, meta->type());
        ASSERT_TRUE(wblock->close().ok());
    }
}

template <FieldType type>
void get_bitmap_reader_iter(std::string& file_name, const ColumnIndexMetaPB& meta,
                            BitmapIndexReader** reader, BitmapIndexIterator** iter) {
    *reader = new BitmapIndexReader(file_name, &meta.bitmap_index());
    auto st = (*reader)->load(true, false);
    ASSERT_TRUE(st.ok());

    st = (*reader)->new_iterator(iter);
    ASSERT_TRUE(st.ok());
}

TEST_F(BitmapIndexTest, test_invert) {
    size_t num_uint8_rows = 1024 * 10;
    int* val = new int[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = i;
    }

    std::string file_name = kTestDir + "/invert";
    ColumnIndexMetaPB meta;
    write_index_file<OLAP_FIELD_TYPE_INT>(file_name, val, num_uint8_rows, 0, &meta);
    {
        std::unique_ptr<RandomAccessFile> rfile;
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<OLAP_FIELD_TYPE_INT>(file_name, meta, &reader, &iter);

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
    write_index_file<OLAP_FIELD_TYPE_INT>(file_name, val, num_uint8_rows, 0, &meta);

    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<OLAP_FIELD_TYPE_INT>(file_name, meta, &reader, &iter);

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
    write_index_file<OLAP_FIELD_TYPE_BIGINT>(file_name, val, num_uint8_rows, 0, &meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<OLAP_FIELD_TYPE_BIGINT>(file_name, meta, &reader, &iter);

        int64_t value = 2019;
        bool exact_match;
        auto st = iter->seek_dictionary(&value, &exact_match);
        ASSERT_TRUE(st.ok()) << "status:" << st.to_string();
        ASSERT_EQ(0, iter->current_ordinal());

        Roaring bitmap;
        iter->read_bitmap(iter->current_ordinal(), &bitmap);
        ASSERT_EQ(1, bitmap.cardinality());

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
    write_index_file<OLAP_FIELD_TYPE_BIGINT>(file_name, val, num_uint8_rows, 30, &meta);
    {
        BitmapIndexReader* reader = nullptr;
        BitmapIndexIterator* iter = nullptr;
        get_bitmap_reader_iter<OLAP_FIELD_TYPE_BIGINT>(file_name, meta, &reader, &iter);

        Roaring bitmap;
        iter->read_null_bitmap(&bitmap);
        ASSERT_EQ(30, bitmap.cardinality());

        delete reader;
        delete iter;
    }
    delete[] val;
}

} // namespace segment_v2
} // namespace doris

int main(int argc, char** argv) {
    doris::StoragePageCache::create_global_cache(1 << 30, 10);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
