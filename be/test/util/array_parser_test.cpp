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

#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/mem_tracker.h"
#include "runtime/string_value.h"
#include "testutil/array_utils.h"

namespace doris {

template <typename... Ts>
ColumnPB create_column_pb(const std::string& type, const Ts&... sub_column_types) {
    ColumnPB column;
    column.set_type(type);
    column.set_aggregation("NONE");
    column.set_is_nullable(true);
    if (type == "ARRAY") {
        column.set_length(OLAP_ARRAY_MAX_BYTES);
    }
    if constexpr (sizeof...(sub_column_types) > 0) {
        auto sub_column = create_column_pb(sub_column_types...);
        column.add_children_columns()->Swap(&sub_column);
    }
    return column;
}

static TypeInfoPtr get_type_info(const ColumnPB& column_pb) {
    TabletColumn tablet_column;
    tablet_column.init_from_pb(column_pb);
    return get_type_info(&tablet_column);
}

static void test_array_parser(const ColumnPB& column_pb, const std::string& json,
                              const CollectionValue& expect) {
    MemTracker tracker(1024 * 1024, "ArrayParserTest");
    MemPool mem_pool(&tracker);
    FunctionContext context;
    ArrayUtils::prepare_context(context, mem_pool, column_pb);
    CollectionValue actual;
    auto status = ArrayUtils::create_collection_value(&actual, &context, json);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(get_type_info(column_pb)->equal(&expect, &actual));
}

TEST(ArrayParserTest, TestParseIntArray) {
    auto column_pb = create_column_pb("ARRAY", "INT");
    test_array_parser(column_pb, "[]", CollectionValue(0));

    int32_t data[] = {1, 2, 3};
    int num_items = sizeof(data) / sizeof(data[0]);
    CollectionValue value(data, num_items, false, nullptr);
    test_array_parser(column_pb, "[1, 2, 3]", value);

    bool null_signs[] = {false, true, false};
    value.set_has_null(true);
    value.set_null_signs(null_signs);
    test_array_parser(column_pb, "[1, null, 3]", value);
}

TEST(ArrayParserTest, TestParseVarcharArray) {
    auto column_pb = create_column_pb("ARRAY", "VARCHAR");
    test_array_parser(column_pb, "[]", CollectionValue(0));

    char data[] = {'a', 'b', 'c'};
    int num_items = sizeof(data) / sizeof(data[0]);
    StringValue string_values[] = {
            {&data[0], 1},
            {&data[1], 1},
            {&data[2], 1},
    };
    CollectionValue value(string_values, num_items, false, nullptr);
    test_array_parser(column_pb, "[\"a\", \"b\", \"c\"]", value);

    bool null_signs[] = {false, true, false};
    value.set_has_null(true);
    value.set_null_signs(null_signs);
    test_array_parser(column_pb, "[\"a\", null, \"c\"]", value);
}

TEST(ArrayParserTest, TestNestedArray) {
    auto column_pb = create_column_pb("ARRAY", "ARRAY", "INT");
    test_array_parser(column_pb, "[]", CollectionValue(0));

    CollectionValue empty_array(0);
    test_array_parser(column_pb, "[[]]", {&empty_array, 1, false, nullptr});

    int data[] = {1, 0, 3};
    uint32_t num_items = sizeof(data) / sizeof(data[0]);
    bool null_signs[] = {false, true, false};
    CollectionValue array = {data, num_items, true, null_signs};

    CollectionValue array_data[] = {empty_array, array, empty_array, array};
    uint32_t num_arrays = sizeof(array_data) / sizeof(array_data[0]);
    test_array_parser(column_pb, "[[], [1, null, 3], [], [1, null, 3]]",
                      {array_data, num_arrays, false, nullptr});
    bool array_null_signs[] = {false, true, true, false};
    test_array_parser(column_pb, "[[], null, null, [1, null, 3]]",
                      {array_data, num_arrays, true, array_null_signs});
}

} // namespace doris
