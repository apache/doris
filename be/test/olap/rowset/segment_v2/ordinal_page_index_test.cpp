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

#include "olap/rowset/segment_v2/ordinal_page_index.h"

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <iostream>
#include <memory>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"

namespace doris {
namespace segment_v2 {

class OrdinalPageIndexTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/ordinal_page_index_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }
};

TEST_F(OrdinalPageIndexTest, normal) {
    std::string filename = kTestDir + "/normal.idx";
    auto fs = io::global_local_filesystem();

    OrdinalIndexWriter builder;
    // generate ordinal index for 16K data pages,
    // each data page is 16KB in size and contains 4096 values,
    // ordinal starts at 1 instead of 0
    for (uint64_t i = 0; i < 16 * 1024; ++i) {
        builder.append_entry(1 + 4096 * i, {16 * 1024 * i, 16 * 1024});
    }
    ColumnIndexMetaPB index_meta;
    {
        io::FileWriterPtr file_writer;
        EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());

        EXPECT_TRUE(builder.finish(file_writer.get(), &index_meta).ok());
        EXPECT_EQ(ORDINAL_INDEX, index_meta.type());
        EXPECT_FALSE(index_meta.ordinal_index().root_page().is_root_data_page());
        EXPECT_TRUE(file_writer->close().ok());
        LOG(INFO) << "index page size="
                  << index_meta.ordinal_index().root_page().root_page().size();
    }

    io::FileReaderSPtr file_reader;
    EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());
    OrdinalIndexReader index(file_reader, 16 * 1024 * 4096 + 1, index_meta.ordinal_index());
    EXPECT_TRUE(index.load(true, false).ok());
    EXPECT_EQ(16 * 1024, index.num_data_pages());
    EXPECT_EQ(1, index.get_first_ordinal(0));
    EXPECT_EQ(4096, index.get_last_ordinal(0));
    EXPECT_EQ((16 * 1024 - 1) * 4096 + 1, index.get_first_ordinal(16 * 1024 - 1));
    EXPECT_EQ(16 * 1024 * 4096, index.get_last_ordinal(16 * 1024 - 1));

    {
        auto iter = index.seek_at_or_before(1);
        EXPECT_TRUE(iter.valid());
        EXPECT_EQ(1, iter.first_ordinal());
        EXPECT_EQ(PagePointer(0, 16 * 1024), iter.page());
    }
    {
        auto iter = index.seek_at_or_before(4095);
        EXPECT_TRUE(iter.valid());
        EXPECT_EQ(1, iter.first_ordinal());
        EXPECT_EQ(PagePointer(0, 16 * 1024), iter.page());
    }
    {
        auto iter = index.seek_at_or_before(4098);
        EXPECT_TRUE(iter.valid());
        EXPECT_EQ(4097, iter.first_ordinal());
        EXPECT_EQ(PagePointer(1 * 16 * 1024, 16 * 1024), iter.page());

        iter.next();
        EXPECT_TRUE(iter.valid());
        EXPECT_EQ(4097 + 4096, iter.first_ordinal());
        EXPECT_EQ(PagePointer(2 * 16 * 1024, 16 * 1024), iter.page());
    }
    {
        auto iter = index.seek_at_or_before(0);
        EXPECT_FALSE(iter.valid());
    }
}

TEST_F(OrdinalPageIndexTest, one_data_page) {
    // index one data page with 1024 values
    int num_values = 1024;
    PagePointer data_page_pointer(0, 4096);

    OrdinalIndexWriter builder;
    builder.append_entry(0, data_page_pointer); // add only one entry
    ColumnIndexMetaPB index_meta;
    {
        // in this case, no index page is written, thus file could be null
        EXPECT_TRUE(builder.finish(nullptr, &index_meta).ok());
        EXPECT_EQ(ORDINAL_INDEX, index_meta.type());
        EXPECT_TRUE(index_meta.ordinal_index().root_page().is_root_data_page());
        PagePointer root_page_pointer(index_meta.ordinal_index().root_page().root_page());
        EXPECT_EQ(data_page_pointer, root_page_pointer);
    }

    OrdinalIndexReader index(nullptr, num_values, index_meta.ordinal_index());
    EXPECT_TRUE(index.load(true, false).ok());
    EXPECT_EQ(1, index.num_data_pages());
    EXPECT_EQ(0, index.get_first_ordinal(0));
    EXPECT_EQ(num_values - 1, index.get_last_ordinal(0));

    {
        auto iter = index.seek_at_or_before(0);
        EXPECT_TRUE(iter.valid());
        EXPECT_EQ(0, iter.first_ordinal());
        EXPECT_EQ(num_values - 1, iter.last_ordinal());
        EXPECT_EQ(data_page_pointer, iter.page());
    }
    {
        auto iter = index.seek_at_or_before(num_values - 1);
        EXPECT_TRUE(iter.valid());
        EXPECT_EQ(0, iter.first_ordinal());
        EXPECT_EQ(data_page_pointer, iter.page());
    }
    {
        auto iter = index.seek_at_or_before(num_values);
        EXPECT_TRUE(iter.valid());
        EXPECT_EQ(0, iter.first_ordinal());
        EXPECT_EQ(data_page_pointer, iter.page());
    }
}

} // namespace segment_v2
} // namespace doris
