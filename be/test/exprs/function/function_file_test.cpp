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

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_file.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/file_schema_descriptor.h"
#include "exprs/function/simple_function_factory.h"
#include "testutil/column_helper.h"
#include "util/jsonb_utils.h"

namespace doris {

TEST(FunctionFileTest, toFileBuildsExpectedJsonbPayload) {
    const std::string object_url = "s3://bench-dataset/ssb/sf1/customer/customer.tbl";
    const std::string endpoint = "http://oss-cn-beijing.aliyuncs.com";
    const std::string role_arn = "arn:aws:iam::543815668950:role/test-role";

    auto string_type = std::make_shared<DataTypeString>();
    ColumnsWithTypeAndName arguments {
            ColumnWithTypeAndName {ColumnHelper::create_column<DataTypeString>({object_url}),
                                   string_type, "object_url"},
            ColumnWithTypeAndName {ColumnHelper::create_column<DataTypeString>({endpoint}),
                                   string_type, "endpoint"},
            ColumnWithTypeAndName {ColumnHelper::create_column<DataTypeString>({role_arn}),
                                   string_type, "role_arn"}};

    auto result_type = std::make_shared<DataTypeFile>();
    auto function =
            SimpleFunctionFactory::instance().get_function("to_file", arguments, result_type);
    ASSERT_TRUE(function);

    Block block(arguments);
    block.insert(ColumnWithTypeAndName {nullptr, result_type, "result"});

    Status status = function->execute(nullptr, block, {0, 1, 2}, 3, 1);
    ASSERT_TRUE(status.ok()) << status;

    const auto& result_column = assert_cast<const ColumnFile&>(*block.get_by_position(3).column);
    const auto& jsonb_column = assert_cast<const ColumnString&>(result_column.get_jsonb_column());
    ASSERT_EQ(result_column.size(), 1);
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(jsonb_column.get_data_at(0).data,
                                                jsonb_column.get_data_at(0).size),
              "{\"object_uri\":\"" + object_url +
                      "\",\"file_name\":\"customer.tbl\",\"content_type\":\"text/plain\","
                      "\"size\":-1,\"etag\":null,"
                      "\"last_modified_at\":\"1970-01-01 00:00:00.000\","
                      "\"region\":null,\"endpoint\":\"" +
                      endpoint + "\",\"role_arn\":\"" + role_arn + "\"}");
    EXPECT_TRUE(result_column.check_schema(FileSchemaDescriptor::instance()).ok());

    const auto& file_type = assert_cast<const DataTypeFile&>(*result_type);
    EXPECT_EQ(file_type.schema().field(0).type->get_name(), "String");
    EXPECT_EQ(file_type.schema().field(1).type->get_name(), "String");
    EXPECT_EQ(file_type.schema().field(2).type->get_name(), "String");
    EXPECT_EQ(file_type.schema().field(3).type->get_name(), "BIGINT");
    EXPECT_EQ(file_type.schema().field(4).type->get_name(), "Nullable(String)");
    EXPECT_EQ(file_type.schema().field(5).type->get_name(), "DateTimeV2(3)");
    EXPECT_EQ(file_type.schema().field(6).type->get_name(), "Nullable(String)");
    EXPECT_EQ(file_type.schema().field(7).type->get_name(), "Nullable(String)");
    EXPECT_EQ(file_type.schema().field(8).type->get_name(), "Nullable(String)");
}

} // namespace doris
