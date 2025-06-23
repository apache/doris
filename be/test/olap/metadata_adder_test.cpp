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

#include "olap/metadata_adder.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/zone_map_index.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"

namespace doris {

class MetadataAdderTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/metadata_adder_test";

    void SetUp() override {
        std::string filter_str = ::testing::GTEST_FLAG(filter);
        std::cout << "GTEST_FILTER: " << filter_str << std::endl;

        if (filter_str != "MetadataAdderTest.*") {
            GTEST_SKIP() << "Skipping this test when executing concurrently.";
        }

        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    MetadataAdderTest() = default;
    ~MetadataAdderTest() override = default;
};

template <typename T>
void test_construct_new_obj() {
    int classSize = sizeof(T);

    std::cout << "Class Name: " << typeid(T).name() << std::endl;

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);
    // 1 default construct
    {
        T t1;
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize);
        T t2;
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize * 2);
    }

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);
    // 2 copy construct
    {
        T t1;
        T t2 = t1;
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize * 2);
    }

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);

    // 3 move construct
    {
        T t1;
        T t2 = std::move(t1);
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize * 2);
    }

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);

    // 4 copy assignment
    {
        T t1;
        T t2;
        int before_size = MetadataAdder<T>::get_all_tablets_size();
        t1 = t2;
        ASSERT_TRUE(before_size == MetadataAdder<T>::get_all_tablets_size());
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize * 2);
    }

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);

    // 5 move assignment
    {
        T t1;
        T t2;
        int before_size = MetadataAdder<T>::get_all_tablets_size();
        t1 = std::move(t2);
        ASSERT_TRUE(before_size == MetadataAdder<T>::get_all_tablets_size());
        ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == classSize * 2);
    }

    ASSERT_TRUE(MetadataAdder<T>::get_all_tablets_size() == 0);
}

TEST_F(MetadataAdderTest, metadata_adder_test) {
    test_construct_new_obj<TabletSchema>();
    test_construct_new_obj<TabletColumn>();
    test_construct_new_obj<TabletIndex>();
}

TEST_F(MetadataAdderTest, meta_load_with_pb_test) {
    {
        auto fs = io::global_local_filesystem();
        TabletColumnPtr int_column = create_int_key(0);
        Field* int_field = FieldFactory::create(*int_column);

        // 1 load first column
        segment_v2::ColumnIndexMetaPB index_meta1;
        std::string file1 = kTestDir + "/copy_obj1";
        {
            std::unique_ptr<segment_v2::ZoneMapIndexWriter> builder(nullptr);
            static_cast<void>(segment_v2::ZoneMapIndexWriter::create(int_field, builder));
            for (int i = 0; i < 100; i++) {
                builder->add_values((const uint8_t*)&i, 1);
            }
            static_cast<void>(builder->flush());
            {
                io::FileWriterPtr file_writer;
                EXPECT_TRUE(fs->create_file(file1, &file_writer).ok());
                EXPECT_TRUE(builder->finish(file_writer.get(), &index_meta1).ok());
                EXPECT_TRUE(file_writer->close().ok());
            }
        }

        ASSERT_TRUE(MetadataAdder<segment_v2::ZoneMapIndexReader>::get_all_segments_size() == 0);

        io::FileReaderSPtr file_reader;
        EXPECT_TRUE(fs->open_file(file1, &file_reader).ok());
        segment_v2::ZoneMapIndexReader zonemap_col_reader(
                file_reader, index_meta1.zone_map_index().page_zone_maps());
        Status status = zonemap_col_reader.load(true, false);

        int mem_size = zonemap_col_reader.get_metadata_size();

        ASSERT_TRUE(MetadataAdder<segment_v2::ZoneMapIndexReader>::get_all_segments_size() ==
                    mem_size);

        // load second column
        segment_v2::ColumnIndexMetaPB index_meta2;
        TabletColumnPtr varchar_column = create_varchar_key(0);
        Field* str_field = FieldFactory::create(*varchar_column);

        std::string file2 = kTestDir + "/copy_obj2";
        {
            std::unique_ptr<segment_v2::ZoneMapIndexWriter> builder(nullptr);
            static_cast<void>(segment_v2::ZoneMapIndexWriter::create(str_field, builder));
            std::vector<std::string> values1 = {"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"};
            for (auto& value : values1) {
                Slice slice(value);
                builder->add_values((const uint8_t*)&slice, 1);
            }
            static_cast<void>(builder->flush());
            {
                io::FileWriterPtr file_writer;
                EXPECT_TRUE(fs->create_file(file2, &file_writer).ok());
                EXPECT_TRUE(builder->finish(file_writer.get(), &index_meta2).ok());
                EXPECT_TRUE(file_writer->close().ok());
            }
        }

        io::FileReaderSPtr file_reader2;
        EXPECT_TRUE(fs->open_file(file2, &file_reader2).ok());
        segment_v2::ZoneMapIndexReader zonemap_col_reader2(
                file_reader2, index_meta2.zone_map_index().page_zone_maps());
        Status status2 = zonemap_col_reader2.load(true, false);

        int mem_size2 = zonemap_col_reader2.get_metadata_size();

        ASSERT_TRUE(MetadataAdder<segment_v2::ZoneMapIndexReader>::get_all_segments_size() ==
                    mem_size2 + mem_size);

        delete int_field;
        delete str_field;
    }

    ASSERT_TRUE(MetadataAdder<segment_v2::ZoneMapIndexReader>::get_all_segments_size() == 0);
}

}; // namespace doris