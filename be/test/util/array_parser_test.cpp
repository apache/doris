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
#include <util/array_parser.hpp>

#include "gutil/casts.h"
#include "olap/types.h"
#include "runtime/free_pool.hpp"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/string_value.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace doris {

using TypeDesc = FunctionContext::TypeDesc;

template <typename... Ts>
TypeDesc create_function_type_desc(FunctionContext::Type type, Ts... sub_types) {
    TypeDesc type_desc = {.type = type,
                          .len = (type == FunctionContext::TYPE_ARRAY) ? OLAP_ARRAY_MAX_BYTES : 0};
    if constexpr (sizeof...(sub_types)) {
        type_desc.children.push_back(create_function_type_desc(sub_types...));
    }
    return type_desc;
}

ColumnPB create_column_pb(const TypeDesc& function_type_desc) {
    ColumnPB column_pb;
    column_pb.set_length(function_type_desc.len);
    switch (function_type_desc.type) {
    case FunctionContext::TYPE_ARRAY:
        column_pb.set_type("ARRAY");
        break;
    case FunctionContext::TYPE_INT:
        column_pb.set_type("INT");
        break;
    case FunctionContext::TYPE_VARCHAR:
        column_pb.set_type("VARCHAR");
        break;
    default:
        break;
    }
    for (auto child_type_desc : function_type_desc.children) {
        auto sub_column_pb = create_column_pb(child_type_desc);
        column_pb.add_children_columns()->Swap(&sub_column_pb);
    }
    return column_pb;
}

std::shared_ptr<const TypeInfo> get_type_info(const TypeDesc& function_type_desc) {
    auto column_pb = create_column_pb(function_type_desc);
    TabletColumn tablet_column;
    tablet_column.init_from_pb(column_pb);
    return get_type_info(&tablet_column);
}

void test_array_parser(const TypeDesc& function_type_desc, const std::string& json,
                       const CollectionValue& expect) {
    MemTracker tracker(1024 * 1024, "ArrayParserTest");
    MemPool mem_pool(&tracker);
    std::unique_ptr<FunctionContext> function_context(new FunctionContext());
    function_context->impl()->_return_type = function_type_desc;
    function_context->impl()->_pool = new FreePool(&mem_pool);
    CollectionVal collection_val;
    auto status =
            ArrayParser::parse(collection_val, function_context.get(), StringVal(json.c_str()));
    EXPECT_TRUE(status.ok());
    auto actual = CollectionValue::from_collection_val(collection_val);
    EXPECT_TRUE(get_type_info(function_type_desc)->equal(&expect, &actual));
}

TEST(ArrayParserTest, TestParseIntArray) {
    auto function_type_desc =
            create_function_type_desc(FunctionContext::TYPE_ARRAY, FunctionContext::TYPE_INT);
    test_array_parser(function_type_desc, "[]", CollectionValue(0));

    int num_items = 3;
    std::unique_ptr<int32_t[]> data(new int32_t[num_items] {1, 2, 3});
    CollectionValue value(data.get(), num_items, false, nullptr);
    test_array_parser(function_type_desc, "[1, 2, 3]", value);

    std::unique_ptr<bool[]> null_signs(new bool[num_items] {false, true, false});
    value.set_has_null(true);
    value.set_null_signs(null_signs.get());
    test_array_parser(function_type_desc, "[1, null, 3]", value);
}

TEST(ArrayParserTest, TestParseVarcharArray) {
    auto function_type_desc =
            create_function_type_desc(FunctionContext::TYPE_ARRAY, FunctionContext::TYPE_VARCHAR);
    test_array_parser(function_type_desc, "[]", CollectionValue(0));

    int num_items = 3;
    std::unique_ptr<char[]> data(new char[num_items] {'a', 'b', 'c'});
    std::unique_ptr<StringValue[]> string_values(new StringValue[num_items] {
            {&data[0], 1},
            {&data[1], 1},
            {&data[2], 1},
    });
    CollectionValue value(string_values.get(), num_items, false, nullptr);
    test_array_parser(function_type_desc, "[\"a\", \"b\", \"c\"]", value);

    std::unique_ptr<bool[]> null_signs(new bool[num_items] {false, true, false});
    value.set_has_null(true);
    value.set_null_signs(null_signs.get());
    test_array_parser(function_type_desc, "[\"a\", null, \"c\"]", value);
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
