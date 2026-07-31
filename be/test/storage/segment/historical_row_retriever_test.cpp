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

#include "storage/segment/historical_row_retriever.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <vector>

#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "storage/binlog.h"
#include "storage/olap_common.h"
#include "storage/utils.h"

namespace doris::segment_v2 {

TEST(HistoricalRowRetrieverTest, ReviseOperatorWithOldDeleteSign) {
    auto delete_sign_column = ColumnInt8::create();
    delete_sign_column->insert_value(1);
    delete_sign_column->insert_value(0);
    delete_sign_column->insert_value(1);

    Block old_value_block;
    old_value_block.insert(
            {std::move(delete_sign_column), std::make_shared<DataTypeInt8>(), DELETE_SIGN});

    std::map<uint32_t, uint32_t> read_index {
            {0, 0},
            {1, 1},
            {3, 2},
    };

    PrimaryKeyModelRowRetriever retriever;
    ASSERT_TRUE(retriever._fill_old_delete_signs(old_value_block, read_index, 4).ok());
    ASSERT_EQ((std::vector<signed char> {1, 0, 0, 1}), retriever._old_delete_signs);

    retriever._operators = {ROW_BINLOG_UPDATE, ROW_BINLOG_UPDATE, ROW_BINLOG_UPDATE,
                            ROW_BINLOG_DELETE};
    RowsetId rowset_id;
    rowset_id.init(1);
    retriever._rssid_to_rid.prepare_to_read(RowLocation(rowset_id, 0, 0), 0);
    ASSERT_TRUE(retriever.revise_operators_by_old_delete_sign(4).ok());

    EXPECT_EQ(ROW_BINLOG_APPEND, retriever._operators[0]);
    EXPECT_EQ(ROW_BINLOG_UPDATE, retriever._operators[1]);
    EXPECT_EQ(ROW_BINLOG_UPDATE, retriever._operators[2]);
    EXPECT_EQ(ROW_BINLOG_DELETE, retriever._operators[3]);
}

} // namespace doris::segment_v2
