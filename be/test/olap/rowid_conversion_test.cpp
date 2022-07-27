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

#include "olap/rowid_conversion.h"

#include <gtest/gtest.h>

#include <iostream>

#include "common/logging.h"

namespace doris {

class TestRowIdConversion : public testing::Test {
public:
    TestRowIdConversion() {}
    virtual ~TestRowIdConversion() {}
};

TEST_F(TestRowIdConversion, TestConversion) {
    // rowset_id, segment_id, row_id
    int input_data[11][3] = {{0, 0, 0}, {0, 0, 1}, {0, 0, 2}, {0, 0, 3}, {0, 1, 0}, {0, 1, 1},
                             {0, 1, 2}, {1, 0, 0}, {1, 0, 1}, {1, 0, 2}, {1, 0, 3}};

    RowsetId src_rowset;
    RowsetId dst_rowset;
    dst_rowset.init(3);

    std::vector<RowLocation> rss_row_ids;
    for (auto i = 0; i < 11; i++) {
        src_rowset.init(input_data[i][0]);
        RowLocation rss_row_id(src_rowset, input_data[i][1], input_data[i][2]);
        rss_row_ids.push_back(rss_row_id);
    }
    RowIdConversion rowid_conversion;
    src_rowset.init(0);
    std::vector<uint32_t> rs0_segment_num_rows = {4, 3};
    rowid_conversion.init_segment_map(src_rowset, rs0_segment_num_rows);
    src_rowset.init(1);
    std::vector<uint32_t> rs1_segment_num_rows = {4};
    rowid_conversion.init_segment_map(src_rowset, rs1_segment_num_rows);

    rowid_conversion.add(rss_row_ids);
    std::vector<uint32_t> dst_segment_num_rows = {4, 3, 4};
    rowid_conversion.set_dst_segment_num_rows(dst_rowset, dst_segment_num_rows);

    int res = 0;
    src_rowset.init(0);
    RowLocation src0(src_rowset, 0, 0);
    RowLocation dst0;
    res = rowid_conversion.get(src0, &dst0);

    EXPECT_EQ(dst0.rowset_id, dst_rowset);
    EXPECT_EQ(dst0.segment_id, 0);
    EXPECT_EQ(dst0.row_id, 0);
    EXPECT_EQ(res, 0);

    src_rowset.init(0);
    RowLocation src1(src_rowset, 1, 2);
    RowLocation dst1;
    res = rowid_conversion.get(src1, &dst1);

    EXPECT_EQ(dst1.rowset_id, dst_rowset);
    EXPECT_EQ(dst1.segment_id, 1);
    EXPECT_EQ(dst1.row_id, 2);
    EXPECT_EQ(res, 0);

    src_rowset.init(1);
    RowLocation src2(src_rowset, 0, 3);
    RowLocation dst2;
    res = rowid_conversion.get(src2, &dst2);

    EXPECT_EQ(dst2.rowset_id, dst_rowset);
    EXPECT_EQ(dst2.segment_id, 2);
    EXPECT_EQ(dst2.row_id, 3);
    EXPECT_EQ(res, 0);

    src_rowset.init(1);
    RowLocation src3(src_rowset, 0, 4);
    RowLocation dst3;
    res = rowid_conversion.get(src3, &dst3);
    EXPECT_EQ(res, -1);

    src_rowset.init(100);
    RowLocation src4(src_rowset, 5, 4);
    RowLocation dst4;
    res = rowid_conversion.get(src4, &dst4);
    EXPECT_EQ(res, -1);
}

} // namespace doris
