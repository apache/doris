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

#include "olap/rowset/unique_rowset_id_generator.h"

#include <gtest/gtest.h>
#include <iostream>

namespace doris {
class UniqueRowsetIdGeneratorTest : public testing::Test {
public:
    UniqueRowsetIdGeneratorTest() { }
    virtual ~UniqueRowsetIdGeneratorTest() {
    }
};

TEST_F(UniqueRowsetIdGeneratorTest, RowsetIdFormatTest) {
    int64_t max_id = 1;
    max_id = max_id << 56;
    {
        RowsetId rowset_id;
        rowset_id.init(123);
        ASSERT_TRUE(rowset_id.version == 1);
        ASSERT_TRUE(rowset_id.lo == (123 + max_id));
        ASSERT_TRUE(rowset_id.mi == 0);
        ASSERT_TRUE(rowset_id.hi == 0);
        ASSERT_STREQ("123", rowset_id.to_string().c_str());
    }
    {
        RowsetId rowset_id;
        rowset_id.init("123");
        ASSERT_TRUE(rowset_id.version == 1);
        ASSERT_TRUE(rowset_id.lo == (123 + max_id));
        ASSERT_TRUE(rowset_id.mi == 0);
        ASSERT_TRUE(rowset_id.hi == 0);
        ASSERT_STREQ("123", rowset_id.to_string().c_str());
    }
    
    {
        RowsetId rowset_id;
        rowset_id.init("c04f58d989cab2f2efd45faa204491890200000000000003");
        ASSERT_TRUE(rowset_id.version == 2);
        ASSERT_TRUE(rowset_id.lo == (3 + max_id));
        ASSERT_STREQ("c04f58d989cab2f2efd45faa204491890200000000000003", rowset_id.to_string().c_str());
    }
}


TEST_F(UniqueRowsetIdGeneratorTest, GenerateIdTest) {
    UniqueId backend_uid;
    UniqueRowsetIdGenerator id_generator(backend_uid);
    {
        int64_t max_id = 2;
        max_id = max_id << 56;
        RowsetId rowset_id;
        id_generator.next_id(&rowset_id);
        ASSERT_TRUE(rowset_id.lo == (1 + max_id));
        ASSERT_TRUE(rowset_id.version == 2);
        ASSERT_TRUE(backend_uid.lo == rowset_id.mi);
        ASSERT_TRUE(backend_uid.hi == rowset_id.hi);
        ASSERT_TRUE(rowset_id.hi != 0);
        bool in_use = id_generator.id_in_use(rowset_id);
        ASSERT_TRUE(in_use == true);
        id_generator.release_id(rowset_id);
        in_use = id_generator.id_in_use(rowset_id);
        ASSERT_TRUE(in_use == false);

        int64_t low = rowset_id.lo + 1;
        id_generator.next_id(&rowset_id);
        ASSERT_TRUE(rowset_id.lo == low);
        in_use = id_generator.id_in_use(rowset_id);
        ASSERT_TRUE(in_use == true);

        std::string rowset_mid_str = rowset_id.to_string().substr(16,16);
        std::string backend_mid_str = backend_uid.to_string().substr(17, 16);
        ASSERT_STREQ(rowset_mid_str.c_str(), backend_mid_str.c_str());
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}

