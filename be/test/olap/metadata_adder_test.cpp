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

#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/segment_v2/inverted_index/compaction/util/index_compaction_utils.cpp"
#include "olap/tablet_meta.h"

namespace doris {

class MetadataAdderTest : public testing::Test {
public:
    void SetUp() override {}

    void TearDown() override {}

    MetadataAdderTest() = default;
    ~MetadataAdderTest() override = default;
};

void call_for_move(RowsetMeta t1) {}

TEST_F(MetadataAdderTest, metadata_adder_test) {
    ASSERT_TRUE(MetadataAdder<TabletMeta>::get_all_tablets_size() == 0);

    int tablet_meta_size = sizeof(TabletMeta);
    int tablet_schema_size = sizeof(TabletSchema);
    int tablet_col_size = sizeof(TabletColumn);

    std::cout << "tablet_meta_size size=" << tablet_meta_size << std::endl;
    std::cout << "tablet_schema_size size=" << tablet_schema_size << std::endl;
    std::cout << "tablet_col_size size=" << tablet_col_size << std::endl;

    ASSERT_TRUE(MetadataAdder<TabletMeta>::get_all_tablets_size() == 0);

    {
        // test basic
        TabletSchema t1;
        ASSERT_TRUE(MetadataAdder<TabletSchema>::get_all_tablets_size() == tablet_schema_size);
        TabletSchema t2;
        ASSERT_TRUE(MetadataAdder<TabletSchema>::get_all_tablets_size() == tablet_schema_size * 2);
    }

    ASSERT_TRUE(MetadataAdder<TabletMeta>::get_all_tablets_size() == 0);

    {
        // test operator =
        TabletSchemaPB schema_pb;
        std::string colname = "key";
        IndexCompactionUtils::construct_column(schema_pb.add_column(), 0, "INT", colname);
        int col_memory_size = sizeof(StringRef) + sizeof(char) * colname.size() + sizeof(size_t) +
                              sizeof(int32_t) * 2;
        std::cout << "col mem size=" << col_memory_size << std::endl;
        TabletSchema t1;
        t1.init_from_pb(schema_pb);
        int t1_mem_size = tablet_schema_size + col_memory_size + tablet_col_size;
        ASSERT_TRUE(MetadataAdder<TabletMeta>::get_all_tablets_size() == t1_mem_size);
        std::cout << "t1 size=" << t1_mem_size << std::endl;

        TabletSchema t2;
        std::cout << "t1 + t2 = " << MetadataAdder<TabletMeta>::get_all_tablets_size() << std::endl;
        ASSERT_TRUE(MetadataAdder<TabletMeta>::get_all_tablets_size() ==
                    (t1_mem_size + tablet_schema_size));

        t2 = t1;
        std::cout << "final=" << MetadataAdder<TabletMeta>::get_all_tablets_size()
                  << std::endl;
        ASSERT_TRUE(MetadataAdder<TabletMeta>::get_all_tablets_size() == (t1_mem_size * 2));
    }

    ASSERT_TRUE(MetadataAdder<TabletMeta>::get_all_tablets_size() == 0);

    {
        // test copy
        TabletSchema t1;
        ASSERT_TRUE(MetadataAdder<TabletMeta>::get_all_tablets_size() == tablet_schema_size);

        TabletSchema t2(t1);
        ASSERT_TRUE(MetadataAdder<TabletMeta>::get_all_tablets_size() == (tablet_schema_size * 2));
    }

    ASSERT_TRUE(MetadataAdder<TabletMeta>::get_all_tablets_size() == 0);
}

}; // namespace doris