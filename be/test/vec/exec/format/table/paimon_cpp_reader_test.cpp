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

#include "vec/exec/format/table/paimon_cpp_reader.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris::vectorized {

class PaimonCppReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _query_options.__set_batch_size(3);
        _runtime_state = std::make_unique<RuntimeState>(_query_options, _query_globals);
    }

    TFileRangeDesc _build_range_with_table_level_row_count(int64_t row_count) {
        TFileRangeDesc range;
        range.__isset.table_format_params = true;
        range.table_format_params.__isset.table_level_row_count = true;
        range.table_format_params.table_level_row_count = row_count;
        return range;
    }

    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile _profile {"paimon_cpp_reader_test"};
    std::vector<SlotDescriptor*> _file_slot_descs;
};

TEST_F(PaimonCppReaderTest, CountPushDownUsesTableLevelRowCount) {
    auto range = _build_range_with_table_level_row_count(5);
    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    reader.set_push_down_agg_type(TPushAggOp::type::COUNT);

    auto init_status = reader.init_reader();
    ASSERT_TRUE(init_status.ok()) << init_status;

    Block block;
    size_t read_rows = 0;
    bool eof = false;

    auto first_status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(first_status.ok()) << first_status;
    EXPECT_EQ(3, read_rows);
    EXPECT_FALSE(eof);

    auto second_status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(second_status.ok()) << second_status;
    EXPECT_EQ(2, read_rows);
    EXPECT_TRUE(eof);

    auto third_status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(third_status.ok()) << third_status;
    EXPECT_EQ(0, read_rows);
    EXPECT_TRUE(eof);
}

TEST_F(PaimonCppReaderTest, InitReaderFailsWithoutPaimonSplit) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__isset.paimon_table = true;
    range.table_format_params.paimon_params.paimon_table = "s3://bucket/db.tbl";

    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto status = reader.init_reader();

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_split"), std::string::npos);
}

} // namespace doris::vectorized
