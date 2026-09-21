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

#include "information_schema/schema_table_stream_consumption_scanner.h"

#include <gen_cpp/FrontendService_types.h>
#include <gtest/gtest.h>

#include <string>

#include "common/object_pool.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

TEST(SchemaTableStreamConsumptionScannerTest, forwards_frontend_conjuncts_in_fetch_request) {
    const std::string frontend_conjuncts = R"([{"column":"UNIT","value":"p1"}])";
    MockRuntimeState state;
    SchemaScannerParam param;
    param.common_param->frontend_conjuncts = &frontend_conjuncts;
    ObjectPool pool;

    SchemaTableStreamConsumptionScanner scanner;
    ASSERT_TRUE(scanner.init(&state, &param, &pool).ok());

    TFetchSchemaTableDataRequest request = scanner._build_fetch_request();
    EXPECT_EQ(TSchemaTableName::TABLE_STREAM_CONSUMPTION, request.schema_table_name);
    ASSERT_TRUE(request.__isset.schema_table_params);
    ASSERT_TRUE(request.schema_table_params.__isset.frontend_conjuncts);
    EXPECT_EQ(frontend_conjuncts, request.schema_table_params.frontend_conjuncts);
}

} // namespace doris
