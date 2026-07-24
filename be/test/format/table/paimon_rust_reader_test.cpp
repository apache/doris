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

#include "format/table/paimon_rust_reader.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "core/block/block.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/url_coding.h"

namespace doris {

class PaimonRustReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _query_options.__set_batch_size(3);
        _runtime_state = RuntimeState::create_unique(_query_options, _query_globals);
    }

    TFileRangeDesc _build_range_with_table_level_row_count(int64_t row_count) {
        TFileRangeDesc range;
        range.__isset.table_format_params = true;
        range.table_format_params.__isset.table_level_row_count = true;
        range.table_format_params.table_level_row_count = row_count;
        return range;
    }

    // Build a scan range with a non-empty (but arbitrary) base64-encoded split
    // so _decode_split_bytes() succeeds. The bytes are ignored by the mock
    // plan-from-split helper, but they must be present and decodable so the
    // reader gets past the fail-fast validation.
    TFileRangeDesc _build_range_with_split() {
        TFileRangeDesc range;
        range.__isset.table_format_params = true;
        range.table_format_params.__isset.paimon_params = true;
        range.table_format_params.paimon_params.__isset.paimon_split = true;
        std::string dummy = "dummy-split-bytes";
        std::string encoded;
        base64_encode(dummy, &encoded);
        range.table_format_params.paimon_params.paimon_split = encoded;
        return range;
    }

    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile _profile {"paimon_rust_reader_test"};
    std::vector<SlotDescriptor*> _file_slot_descs;
};

TEST_F(PaimonRustReaderTest, CountPushDownUsesTableLevelRowCount) {
    auto range = _build_range_with_table_level_row_count(5);
    PaimonRustReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    reader.set_push_down_agg_type(TPushAggOp::type::COUNT);

    auto init_status = reader.init_reader();
    ASSERT_TRUE(init_status.ok()) << init_status;

    Block block;
    size_t read_rows = 0;
    bool eof = false;

    auto first_status = reader._do_get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(first_status.ok()) << first_status;
    EXPECT_EQ(3, read_rows);
    EXPECT_FALSE(eof);

    auto second_status = reader._do_get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(second_status.ok()) << second_status;
    EXPECT_EQ(2, read_rows);
    EXPECT_TRUE(eof);

    auto third_status = reader._do_get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(third_status.ok()) << third_status;
    EXPECT_EQ(0, read_rows);
    EXPECT_TRUE(eof);
}

TEST_F(PaimonRustReaderTest, InitReaderFailsWithoutSplit) {
    // No paimon_split set on the range: _decode_split_bytes must reject it
    // before any filesystem IO happens.
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__isset.paimon_table = true;
    range.table_format_params.paimon_params.paimon_table =
            "file:///tmp/doris_paimon_rust_ut_missing/db.db/tbl";
    // paimon_split intentionally left unset.

    PaimonRustReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto status = reader.init_reader();

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_split"), std::string::npos) << status;
}

TEST_F(PaimonRustReaderTest, InitReaderFailsWithoutTablePath) {
    auto range = _build_range_with_split();
    // paimon_table intentionally left unset.

    PaimonRustReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto status = reader.init_reader();

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_table"), std::string::npos) << status;
}

TEST_F(PaimonRustReaderTest, InitReaderFailsWithoutDbName) {
    auto range = _build_range_with_split();
    range.table_format_params.paimon_params.__isset.paimon_table = true;
    range.table_format_params.paimon_params.paimon_table =
            "file:///tmp/doris_paimon_rust_ut_missing/db.db/tbl";
    // db_name / table_name intentionally left unset.

    PaimonRustReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto status = reader.init_reader();

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing db_name"), std::string::npos) << status;
}

TEST_F(PaimonRustReaderTest, InitReaderFailsWithoutTableName) {
    auto range = _build_range_with_split();
    range.table_format_params.paimon_params.__isset.paimon_table = true;
    range.table_format_params.paimon_params.paimon_table =
            "file:///tmp/doris_paimon_rust_ut_missing/db.db/tbl";
    range.table_format_params.paimon_params.__isset.db_name = true;
    range.table_format_params.paimon_params.db_name = "db";
    // table_name intentionally left unset.

    PaimonRustReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto status = reader.init_reader();

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing table_name"), std::string::npos) << status;
}

} // namespace doris
