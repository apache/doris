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

#include "information_schema/schema_extensions_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/data_type/define_primitive_type.h"
#include "information_schema/schema_scanner.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

// The RPC data path (_get_extensions_block_from_fe) requires a live FE and is
// covered by the FE-side regression test; ThriftRpcHelper has no result-injection
// hook, so these unit tests exercise the FE-independent surface only: the factory
// registration, the column schema, and the pre-/post-init guard branches of
// start() and get_next_block_internal().
class SchemaExtensionsScannerTest : public testing::Test {
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SchemaExtensionsScannerTest, test_create_extensions_scanner) {
    auto scanner = SchemaScanner::create(TSchemaTableType::SCH_EXTENSIONS);
    ASSERT_NE(nullptr, scanner);
    EXPECT_EQ(TSchemaTableType::SCH_EXTENSIONS, scanner->type());

    const auto& columns = scanner->get_column_desc();
    ASSERT_EQ(5, columns.size());
    EXPECT_STREQ("EXTENSION_NAME", columns[0].name);
    EXPECT_STREQ("EXTENSION_TYPE", columns[1].name);
    EXPECT_STREQ("EXTENSION_VERSION", columns[2].name);
    EXPECT_STREQ("SOURCE", columns[3].name);
    EXPECT_STREQ("DESCRIPTION", columns[4].name);

    // All extension columns are nullable strings (e.g. unknown EXTENSION_VERSION
    // is surfaced as SQL NULL rather than an empty string).
    for (const auto& column : columns) {
        EXPECT_EQ(TYPE_STRING, column.type);
        EXPECT_TRUE(column.is_null);
    }
}

TEST_F(SchemaExtensionsScannerTest, test_start_before_init) {
    SchemaExtensionsScanner scanner;
    // start() must reject use before init() regardless of the RuntimeState.
    auto st = scanner.start(nullptr);
    EXPECT_FALSE(st.ok());
}

TEST_F(SchemaExtensionsScannerTest, test_get_next_block_before_init) {
    SchemaExtensionsScanner scanner;
    auto block = Block::create_unique();
    bool eos = false;
    auto st = scanner.get_next_block_internal(block.get(), &eos);
    EXPECT_FALSE(st.ok());
    EXPECT_FALSE(eos);
}

// After init(), start() still fails when the session has no query context, since
// extensions are fetched from the FE recorded on the query context.
TEST_F(SchemaExtensionsScannerTest, test_start_without_query_context) {
    // A default RuntimeState has a null query context.
    RuntimeState state;
    SchemaScannerParam param;
    ObjectPool pool;

    SchemaExtensionsScanner scanner;
    ASSERT_TRUE(scanner.init(&state, &param, &pool).ok());

    auto st = scanner.start(&state);
    EXPECT_FALSE(st.ok());
}

// With a query context present, start() succeeds and stashes the per-session
// state (batch size, rpc timeout, connected FE) used by the later RPC.
TEST_F(SchemaExtensionsScannerTest, test_start_with_query_context) {
    MockRuntimeState state;
    SchemaScannerParam param;
    ObjectPool pool;

    SchemaExtensionsScanner scanner;
    ASSERT_TRUE(scanner.init(&state, &param, &pool).ok());

    auto st = scanner.start(&state);
    EXPECT_TRUE(st.ok());
}

// After init(), get_next_block_internal() rejects null output pointers before
// attempting any FE RPC.
TEST_F(SchemaExtensionsScannerTest, test_get_next_block_null_output) {
    RuntimeState state;
    SchemaScannerParam param;
    ObjectPool pool;

    SchemaExtensionsScanner scanner;
    ASSERT_TRUE(scanner.init(&state, &param, &pool).ok());

    bool eos = false;
    EXPECT_FALSE(scanner.get_next_block_internal(nullptr, &eos).ok());

    auto block = Block::create_unique();
    EXPECT_FALSE(scanner.get_next_block_internal(block.get(), nullptr).ok());
}

} // namespace doris
