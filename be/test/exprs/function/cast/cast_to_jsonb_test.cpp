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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/cast_test.h"

namespace doris {

namespace {
Status cast_string_to_jsonb(FunctionContext* ctx, const std::string& str) {
    auto from_type = std::make_shared<DataTypeString>();
    auto to_type = make_nullable(std::make_shared<DataTypeJsonb>());
    auto fn = get_cast_wrapper(ctx, from_type, to_type);
    EXPECT_TRUE(fn != nullptr);

    auto from_column = ColumnString::create();
    from_column->insert_data(str.data(), str.size());
    Block block = {
            {std::move(from_column), from_type, "from"},
            {nullptr, to_type, "to"},
    };
    return fn(ctx, block, {0}, 1, block.rows(), nullptr);
}
} // namespace

TEST_F(FunctionCastTest, test_strict_cast_string_to_jsonb_quotes_short_source) {
    auto ctx = create_context(true);
    Status st = cast_string_to_jsonb(ctx.get(), "[1.x]");
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.msg().find("Failed to parse json string: [1.x], error: "), std::string::npos)
            << st.msg();
}

TEST_F(FunctionCastTest, test_strict_cast_string_to_jsonb_error_message_is_bounded) {
    // The CAST error quotes the source string; a malformed multi-megabyte value must not be
    // copied into the message in full.
    const std::string json = "[1." + std::string(1 << 20, '0') + "x]";
    auto ctx = create_context(true);
    Status st = cast_string_to_jsonb(ctx.get(), json);
    EXPECT_FALSE(st.ok());
    EXPECT_LT(st.msg().size(), 512) << st.msg().substr(0, 512);
    EXPECT_NE(st.msg().find(fmt::format("Failed to parse json string: {}... (truncated, {} bytes)",
                                        json.substr(0, 64), json.size())),
              std::string::npos)
            << st.msg();
}

} // namespace doris
