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

#include "olap/rowset/segment_v2/zone_map_index.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/config.h"
#include "env/env.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/page_cache.h"
#include "olap/tablet_schema_helper.h"
#include "util/file_utils.h"

namespace doris {
namespace segment_v2 {

class ColumnZoneMapTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/zone_map_index_test";

    void SetUp() override {
        if (FileUtils::check_exist(kTestDir)) {
            EXPECT_TRUE(FileUtils::remove_all(kTestDir).ok());
        }
        EXPECT_TRUE(FileUtils::create_dir(kTestDir).ok());
    }
    void TearDown() override {
        if (FileUtils::check_exist(kTestDir)) {
            EXPECT_TRUE(FileUtils::remove_all(kTestDir).ok());
        }
    }

    void test_string(std::string testname, Field* field) {
        std::string filename = kTestDir + "/" + testname;
        auto fs = io::global_local_filesystem();

        std::unique_ptr<ZoneMapIndexWriter> builder(nullptr);
        ZoneMapIndexWriter::create(field, builder);
        std::vector<std::string> values1 = {"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"};
        for (auto& value : values1) {
            Slice slice(value);
            builder->add_values((const uint8_t*)&slice, 1);
        }
        builder->flush();
        std::vector<std::string> values2 = {"aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee", "fffff"};
        for (auto& value : values2) {
            Slice slice(value);
            builder->add_values((const uint8_t*)&slice, 1);
        }
        builder->add_nulls(1);
        builder->flush();
        for (int i = 0; i < 6; ++i) {
            builder->add_nulls(1);
        }
        builder->flush();
        // write out zone map index
        ColumnIndexMetaPB index_meta;
        {
            io::FileWriterPtr file_writer;
            EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());
            EXPECT_TRUE(builder->finish(file_writer.get(), &index_meta).ok());
            EXPECT_EQ(ZONE_MAP_INDEX, index_meta.type());
            EXPECT_TRUE(file_writer->close().ok());
        }

        io::FileReaderSPtr file_reader;
        EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());
        ZoneMapIndexReader column_zone_map(file_reader, &index_meta.zone_map_index());
        Status status = column_zone_map.load(true, false);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(3, column_zone_map.num_pages());
        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
        EXPECT_EQ(3, zone_maps.size());
        EXPECT_EQ("aaaa", zone_maps[0].min());
        EXPECT_EQ("ffff", zone_maps[0].max());
        EXPECT_EQ(false, zone_maps[0].has_null());
        EXPECT_EQ(true, zone_maps[0].has_not_null());

        EXPECT_EQ("aaaaa", zone_maps[1].min());
        EXPECT_EQ("fffff", zone_maps[1].max());
        EXPECT_EQ(true, zone_maps[1].has_null());
        EXPECT_EQ(true, zone_maps[1].has_not_null());

        EXPECT_EQ(true, zone_maps[2].has_null());
        EXPECT_EQ(false, zone_maps[2].has_not_null());
    }

    void test_cut_zone_map(std::string testname, Field* field) {
        std::string filename = kTestDir + "/" + testname;
        auto fs = io::global_local_filesystem();

        std::unique_ptr<ZoneMapIndexWriter> builder(nullptr);
        ZoneMapIndexWriter::create(field, builder);
        char ch = 'a';
        char buf[1024];
        for (int i = 0; i < 5; i++) {
            memset(buf, ch + i, 1024);
            Slice slice(buf, 1024);
            builder->add_values((const uint8_t*)&slice, 1);
        }
        builder->flush();

        // write out zone map index
        ColumnIndexMetaPB index_meta;
        {
            io::FileWriterPtr file_writer;
            EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());
            EXPECT_TRUE(builder->finish(file_writer.get(), &index_meta).ok());
            EXPECT_EQ(ZONE_MAP_INDEX, index_meta.type());
            EXPECT_TRUE(file_writer->close().ok());
        }

        io::FileReaderSPtr file_reader;
        EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());
        ZoneMapIndexReader column_zone_map(file_reader, &index_meta.zone_map_index());
        Status status = column_zone_map.load(true, false);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(1, column_zone_map.num_pages());
        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
        EXPECT_EQ(1, zone_maps.size());

        char value[512];
        memset(value, 'a', 512);
        EXPECT_EQ(value, zone_maps[0].min());
        memset(value, 'f', 512);
        value[511] += 1;
        EXPECT_EQ(value, zone_maps[0].max());
        EXPECT_EQ(false, zone_maps[0].has_null());
        EXPECT_EQ(true, zone_maps[0].has_not_null());
    }
};

// Test for int
TEST_F(ColumnZoneMapTest, NormalTestIntPage) {
    std::string filename = kTestDir + "/NormalTestIntPage";
    auto fs = io::global_local_filesystem();

    TabletColumn int_column = create_int_key(0);
    Field* field = FieldFactory::create(int_column);

    std::unique_ptr<ZoneMapIndexWriter> builder(nullptr);
    ZoneMapIndexWriter::create(field, builder);
    std::vector<int> values1 = {1, 10, 11, 20, 21, 22};
    for (auto value : values1) {
        builder->add_values((const uint8_t*)&value, 1);
    }
    builder->flush();
    std::vector<int> values2 = {2, 12, 31, 23, 21, 22};
    for (auto value : values2) {
        builder->add_values((const uint8_t*)&value, 1);
    }
    builder->add_nulls(1);
    builder->flush();
    builder->add_nulls(6);
    builder->flush();
    // write out zone map index
    ColumnIndexMetaPB index_meta;
    {
        io::FileWriterPtr file_writer;
        EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());
        EXPECT_TRUE(builder->finish(file_writer.get(), &index_meta).ok());
        EXPECT_EQ(ZONE_MAP_INDEX, index_meta.type());
        EXPECT_TRUE(file_writer->close().ok());
    }

    io::FileReaderSPtr file_reader;
    EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());
    ZoneMapIndexReader column_zone_map(file_reader, &index_meta.zone_map_index());
    Status status = column_zone_map.load(true, false);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(3, column_zone_map.num_pages());
    const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
    EXPECT_EQ(3, zone_maps.size());

    EXPECT_EQ(std::to_string(1), zone_maps[0].min());
    EXPECT_EQ(std::to_string(22), zone_maps[0].max());
    EXPECT_EQ(false, zone_maps[0].has_null());
    EXPECT_EQ(true, zone_maps[0].has_not_null());

    EXPECT_EQ(std::to_string(2), zone_maps[1].min());
    EXPECT_EQ(std::to_string(31), zone_maps[1].max());
    EXPECT_EQ(true, zone_maps[1].has_null());
    EXPECT_EQ(true, zone_maps[1].has_not_null());

    EXPECT_EQ(true, zone_maps[2].has_null());
    EXPECT_EQ(false, zone_maps[2].has_not_null());
    delete field;
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestVarcharPage) {
    TabletColumn varchar_column = create_varchar_key(0);
    Field* field = FieldFactory::create(varchar_column);
    test_string("NormalTestVarcharPage", field);
    delete field;
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestCharPage) {
    TabletColumn char_column = create_char_key(0);
    Field* field = FieldFactory::create(char_column);
    test_string("NormalTestCharPage", field);
    delete field;
}

// Test for zone map limit
TEST_F(ColumnZoneMapTest, ZoneMapCut) {
    TabletColumn varchar_column = create_varchar_key(0);
    varchar_column.set_index_length(1024);
    Field* field = FieldFactory::create(varchar_column);
    test_string("ZoneMapCut", field);
    delete field;
}

} // namespace segment_v2
} // namespace doris
