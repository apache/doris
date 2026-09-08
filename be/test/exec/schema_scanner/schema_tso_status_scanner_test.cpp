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

#include "information_schema/schema_tso_status_scanner.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/Status_types.h>
#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

TRow create_tso_status_row(const std::array<int64_t, 4>& values) {
    std::vector<TCell> cells;
    cells.reserve(values.size());
    for (int64_t value : values) {
        TCell cell;
        cell.__set_longVal(value);
        cells.emplace_back(std::move(cell));
    }

    TRow row;
    row.__set_column_value(cells);
    return row;
}

TFetchSchemaTableDataResult create_tso_status_result(const std::vector<TRow>& rows) {
    TFetchSchemaTableDataResult result;
    result.status.__set_status_code(TStatusCode::OK);
    result.__set_data_batch(rows);
    return result;
}

std::unique_ptr<Block> create_output_block(SchemaTsoStatusScanner* scanner) {
    auto block = Block::create_unique();
    scanner->_init_block(block.get());
    return block;
}

void expect_tso_status_row(const Block& block, size_t row_idx,
                           const std::array<int64_t, 4>& expected) {
    ASSERT_EQ(expected.size(), block.columns());
    for (size_t column_idx = 0; column_idx < expected.size(); ++column_idx) {
        const auto& column = block.get_by_position(column_idx).column;
        EXPECT_FALSE(column->is_null_at(row_idx));
        EXPECT_EQ(expected[column_idx], (*column)[row_idx].get<TYPE_BIGINT>());
    }
}

} // namespace

TEST(SchemaTsoStatusScannerTest, test_create_tso_status_scanner) {
    auto scanner = SchemaScanner::create(TSchemaTableType::SCH_TSO_STATUS);
    ASSERT_NE(nullptr, scanner);
    EXPECT_EQ(TSchemaTableType::SCH_TSO_STATUS, scanner->type());
    ASSERT_EQ(4, scanner->get_column_desc().size());
    EXPECT_STREQ("WINDOW_END_PHYSICAL_TIME", scanner->get_column_desc()[0].name);
    EXPECT_STREQ("CURRENT_TSO", scanner->get_column_desc()[1].name);
    EXPECT_STREQ("CURRENT_TSO_PHYSICAL_TIME", scanner->get_column_desc()[2].name);
    EXPECT_STREQ("CURRENT_TSO_LOGICAL_COUNTER", scanner->get_column_desc()[3].name);
    for (const auto& column : scanner->get_column_desc()) {
        EXPECT_EQ(TYPE_BIGINT, column.type);
        EXPECT_TRUE(column.is_null);
    }
}

TEST(SchemaTsoStatusScannerTest, test_start) {
    MockRuntimeState state;
    state._batch_size = 2;
    state._query_options.__set_execution_timeout(7);
    SchemaScannerParam param;
    ObjectPool pool;

    SchemaTsoStatusScanner scanner;
    ASSERT_TRUE(scanner.init(&state, &param, &pool).ok());
    ASSERT_TRUE(scanner.start(&state).ok());

    EXPECT_EQ(2, scanner._block_rows_limit);
    EXPECT_EQ(7000, scanner._rpc_timeout_ms);
}

TEST(SchemaTsoStatusScannerTest, test_get_next_block_invalid_input) {
    SchemaTsoStatusScanner scanner;
    auto block = Block::create_unique();
    bool eos = false;

    auto status = scanner.get_next_block_internal(block.get(), &eos);
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_FALSE(eos);

    MockRuntimeState state;
    SchemaScannerParam param;
    ObjectPool pool;
    ASSERT_TRUE(scanner.init(&state, &param, &pool).ok());

    status = scanner.get_next_block_internal(nullptr, &eos);
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>());
    status = scanner.get_next_block_internal(block.get(), nullptr);
    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>());
}

TEST(SchemaTsoStatusScannerTest, test_process_tso_status_result_error) {
    TFetchSchemaTableDataResult result;
    result.status.__set_status_code(TStatusCode::INTERNAL_ERROR);
    result.status.__set_error_msgs({"fetch failed"});

    SchemaTsoStatusScanner scanner;
    auto status = scanner._process_tso_status_result(result);

    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_NE(std::string::npos, status.to_string().find("fetch failed"));
    EXPECT_EQ(nullptr, scanner._tso_status_block);
}

TEST(SchemaTsoStatusScannerTest, test_process_tso_status_result) {
    const std::array<int64_t, 4> first_row = {1000, 2000, 3000, 4000};
    const std::array<int64_t, 4> second_row = {1001, 2001, 3001, 4001};
    auto result = create_tso_status_result(
            {create_tso_status_row(first_row), create_tso_status_row(second_row)});

    SchemaTsoStatusScanner scanner;
    ASSERT_TRUE(scanner._process_tso_status_result(result).ok());

    ASSERT_NE(nullptr, scanner._tso_status_block);
    EXPECT_EQ(4, scanner._tso_status_block->columns());
    EXPECT_EQ(2, scanner._tso_status_block->rows());
    EXPECT_EQ(2, scanner._total_rows);
    expect_tso_status_row(*scanner._tso_status_block, 0, first_row);
    expect_tso_status_row(*scanner._tso_status_block, 1, second_row);
}

TEST(SchemaTsoStatusScannerTest, test_process_tso_status_result_schema_mismatch) {
    TRow invalid_row = create_tso_status_row({1000, 2000, 3000, 4000});
    invalid_row.column_value.pop_back();
    auto result = create_tso_status_result({invalid_row});

    SchemaTsoStatusScanner scanner;
    auto status = scanner._process_tso_status_result(result);

    EXPECT_TRUE(status.is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_NE(std::string::npos,
              status.to_string().find("TSO status schema does not match between FE and BE"));
    EXPECT_EQ(0, scanner._total_rows);
}

TEST(SchemaTsoStatusScannerTest, test_get_next_block_empty_result) {
    MockRuntimeState state;
    SchemaScannerParam param;
    ObjectPool pool;
    SchemaTsoStatusScanner scanner;
    ASSERT_TRUE(scanner.init(&state, &param, &pool).ok());
    ASSERT_TRUE(scanner.start(&state).ok());
    ASSERT_TRUE(scanner._process_tso_status_result(create_tso_status_result({})).ok());

    auto block = create_output_block(&scanner);
    bool eos = false;
    ASSERT_TRUE(scanner.get_next_block_internal(block.get(), &eos).ok());

    EXPECT_TRUE(eos);
    EXPECT_EQ(0, block->rows());
}

TEST(SchemaTsoStatusScannerTest, test_get_next_block_in_batches) {
    const std::array<int64_t, 4> first_row = {1000, 2000, 3000, 4000};
    const std::array<int64_t, 4> second_row = {1001, 2001, 3001, 4001};
    const std::array<int64_t, 4> third_row = {1002, 2002, 3002, 4002};

    MockRuntimeState state;
    state._batch_size = 2;
    SchemaScannerParam param;
    ObjectPool pool;
    SchemaTsoStatusScanner scanner;
    ASSERT_TRUE(scanner.init(&state, &param, &pool).ok());
    ASSERT_TRUE(scanner.start(&state).ok());
    ASSERT_TRUE(scanner._process_tso_status_result(
                               create_tso_status_result({create_tso_status_row(first_row),
                                                         create_tso_status_row(second_row),
                                                         create_tso_status_row(third_row)}))
                        .ok());

    auto first_block = create_output_block(&scanner);
    bool eos = true;
    ASSERT_TRUE(scanner.get_next_block_internal(first_block.get(), &eos).ok());
    EXPECT_FALSE(eos);
    ASSERT_EQ(2, first_block->rows());
    expect_tso_status_row(*first_block, 0, first_row);
    expect_tso_status_row(*first_block, 1, second_row);

    auto second_block = create_output_block(&scanner);
    ASSERT_TRUE(scanner.get_next_block_internal(second_block.get(), &eos).ok());
    EXPECT_TRUE(eos);
    ASSERT_EQ(1, second_block->rows());
    expect_tso_status_row(*second_block, 0, third_row);

    auto exhausted_block = create_output_block(&scanner);
    eos = false;
    ASSERT_TRUE(scanner.get_next_block_internal(exhausted_block.get(), &eos).ok());
    EXPECT_TRUE(eos);
    EXPECT_EQ(0, exhausted_block->rows());
}

} // namespace doris
