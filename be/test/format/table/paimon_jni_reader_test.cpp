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

#include "format/table/paimon_jni_reader.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "format/generic_reader.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace {

TFileRangeDesc make_paimon_jni_range(std::string split) {
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("paimon");
    TPaimonFileDesc paimon_params;
    paimon_params.__set_paimon_split(std::move(split));
    table_format_params.__set_paimon_params(std::move(paimon_params));
    range.__set_table_format_params(std::move(table_format_params));
    return range;
}

TFileScanRangeParams make_scan_params() {
    TFileScanRangeParams params;
    params.__set_serialized_table("serialized-paimon-table");
    params.__set_paimon_predicate("serialized-paimon-predicate");
    return params;
}

class LegacyPaimonJniReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _query_options.__set_batch_size(2);
        _runtime_state = std::make_unique<RuntimeState>(_query_options, _query_globals);
    }

    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile _profile {"paimon_jni_reader_test"};
    std::vector<SlotDescriptor*> _file_slot_descs;
};

TEST_F(LegacyPaimonJniReaderTest, ReusesScannerWhenOnlySplitChanges) {
    auto first_range = make_paimon_jni_range("split-a");
    auto second_range = make_paimon_jni_range("split-b");
    const auto scan_params = make_scan_params();

    PaimonJniReader reader(_file_slot_descs, _runtime_state.get(), &_profile, first_range,
                           &scan_params);

    EXPECT_TRUE(reader.can_reuse_for(second_range));
    EXPECT_EQ(reader.TEST_scanner_params_signature().at("paimon_predicate"),
              "serialized-paimon-predicate");
    EXPECT_EQ(reader.TEST_scanner_params_signature().at("serialized_table"),
              "serialized-paimon-table");
}

TEST_F(LegacyPaimonJniReaderTest, RecreatesScannerWhenScannerParametersChange) {
    auto first_range = make_paimon_jni_range("split-a");
    first_range.table_format_params.paimon_params.__set_paimon_options(
            std::map<std::string, std::string> {{"warehouse", "a"}});
    auto second_range = make_paimon_jni_range("split-b");
    second_range.table_format_params.paimon_params.__set_paimon_options(
            std::map<std::string, std::string> {{"warehouse", "b"}});
    const auto scan_params = make_scan_params();

    PaimonJniReader reader(_file_slot_descs, _runtime_state.get(), &_profile, first_range,
                           &scan_params);

    EXPECT_FALSE(reader.can_reuse_for(second_range));
}

TEST_F(LegacyPaimonJniReaderTest, ReturnsTableLevelCountInBatches) {
    auto range = make_paimon_jni_range("count-split");
    range.table_format_params.__set_table_level_row_count(5);
    const auto scan_params = make_scan_params();
    PaimonJniReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, &scan_params);
    reader.set_push_down_agg_type(TPushAggOp::type::COUNT);

    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "count"});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader._do_get_next_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 2);
    EXPECT_EQ(block.rows(), 2);
    EXPECT_FALSE(eof);

    ASSERT_TRUE(reader._do_get_next_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 2);
    EXPECT_FALSE(eof);

    ASSERT_TRUE(reader._do_get_next_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 1);
    EXPECT_EQ(block.rows(), 1);
    EXPECT_TRUE(eof);
}

TEST_F(LegacyPaimonJniReaderTest, PreparesCountSplitWithoutOpeningAnotherScanner) {
    auto initial_range = make_paimon_jni_range("first-split");
    auto count_range = make_paimon_jni_range("second-split");
    count_range.table_format_params.__set_table_level_row_count(3);
    const auto scan_params = make_scan_params();
    PaimonJniReader reader(_file_slot_descs, _runtime_state.get(), &_profile, initial_range,
                           &scan_params);
    reader.set_push_down_agg_type(TPushAggOp::type::COUNT);

    ReaderInitContext init_ctx;
    ASSERT_TRUE(reader.prepare_split(count_range, &init_ctx).ok());
    ASSERT_TRUE(reader.finish_split().ok());

    Block block;
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader._do_get_next_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 2);
    EXPECT_FALSE(eof);
}

} // namespace
} // namespace doris
