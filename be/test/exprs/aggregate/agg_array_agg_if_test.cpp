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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/aggregate/agg_function_test.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {

struct AggregateFunctionArrayAggIfTest : public AggregateFunctiontest {};

namespace {

DataTypePtr bool_type() {
    return std::make_shared<DataTypeBool>();
}

DataTypePtr nullable_bool_type() {
    return make_nullable(std::make_shared<DataTypeBool>());
}

DataTypePtr nullable_int64_type() {
    return make_nullable(std::make_shared<DataTypeInt64>());
}

DataTypePtr nullable_string_type() {
    return make_nullable(std::make_shared<DataTypeString>());
}

MutableColumnPtr make_cond_column(std::initializer_list<int> conds) {
    auto cond = ColumnUInt8::create();
    for (int c : conds) {
        cond->insert_value(c != 0);
    }
    return cond;
}

MutableColumnPtr make_nullable_cond_column(std::initializer_list<const char*> conds) {
    auto values = ColumnUInt8::create();
    auto null_map = ColumnUInt8::create();
    for (const char* c : conds) {
        if (*c == 'n') {
            values->insert_value(0);
            null_map->insert_value(1);
        } else {
            values->insert_value(*c == 't');
            null_map->insert_value(0);
        }
    }
    return ColumnNullable::create(std::move(values), std::move(null_map));
}

/** Runs array_agg_if over the given cond/elem block and checks the aggregated array row. */
void check_array_agg_if(AggregateFunctionArrayAggIfTest* test, Block block,
                        DataTypePtr cond_type, DataTypePtr elem_type, Array expected) {
    test->create_agg("array_agg_if", false, {cond_type, elem_type}, elem_type);
    auto array_type = std::make_shared<DataTypeArray>(make_nullable(elem_type));
    auto expected_column = array_type->create_column();
    expected_column->insert(Field::create_field<TYPE_ARRAY>(std::move(expected)));
    test->execute(std::move(block),
                  ColumnWithTypeAndName(std::move(expected_column), array_type, "column"));
}

Block make_block(MutableColumnPtr cond, MutableColumnPtr elem, DataTypePtr cond_type,
                 DataTypePtr elem_type) {
    return Block({ColumnWithTypeAndName(std::move(cond), cond_type, "cond"),
                  ColumnWithTypeAndName(std::move(elem), elem_type, "elem")});
}

} // namespace

TEST_F(AggregateFunctionArrayAggIfTest, test_int64_skip_cond_false_rows) {
    auto elem_type = nullable_int64_type();
    auto elem = elem_type->create_column();
    elem->insert(Field::create_field<TYPE_BIGINT>(1));
    elem->insert(Field::create_field<TYPE_BIGINT>(2));
    elem->insert(Field::create_field<TYPE_BIGINT>(3));
    elem->insert(Field::create_field<TYPE_BIGINT>(4));
    auto block = make_block(make_cond_column({1, 0, 1, 1}), std::move(elem), bool_type(),
                            elem_type);
    check_array_agg_if(this, std::move(block), bool_type(), elem_type,
                       Array {Field::create_field<TYPE_BIGINT>(1),
                              Field::create_field<TYPE_BIGINT>(3),
                              Field::create_field<TYPE_BIGINT>(4)});
}

TEST_F(AggregateFunctionArrayAggIfTest, test_int64_keeps_null_elements) {
    auto elem_type = nullable_int64_type();
    auto elem = elem_type->create_column();
    elem->insert(Field::create_field<TYPE_BIGINT>(5));
    elem->insert(Field());
    elem->insert(Field::create_field<TYPE_BIGINT>(7));
    auto block = make_block(make_cond_column({1, 1, 1}), std::move(elem), bool_type(),
                            elem_type);
    check_array_agg_if(this, std::move(block), bool_type(), elem_type,
                       Array {Field::create_field<TYPE_BIGINT>(5), Field(),
                              Field::create_field<TYPE_BIGINT>(7)});
}

TEST_F(AggregateFunctionArrayAggIfTest, test_int64_null_elem_skipped_by_cond) {
    auto elem_type = nullable_int64_type();
    auto elem = elem_type->create_column();
    elem->insert(Field::create_field<TYPE_BIGINT>(5));
    elem->insert(Field());
    elem->insert(Field::create_field<TYPE_BIGINT>(7));
    auto block = make_block(make_cond_column({1, 0, 1}), std::move(elem), bool_type(),
                            elem_type);
    check_array_agg_if(this, std::move(block), bool_type(), elem_type,
                       Array {Field::create_field<TYPE_BIGINT>(5),
                              Field::create_field<TYPE_BIGINT>(7)});
}

TEST_F(AggregateFunctionArrayAggIfTest, test_int64_all_cond_false_returns_empty_array) {
    auto elem_type = nullable_int64_type();
    auto elem = elem_type->create_column();
    elem->insert(Field::create_field<TYPE_BIGINT>(1));
    elem->insert(Field::create_field<TYPE_BIGINT>(2));
    auto block = make_block(make_cond_column({0, 0}), std::move(elem), bool_type(), elem_type);
    check_array_agg_if(this, std::move(block), bool_type(), elem_type, Array {});
}

TEST_F(AggregateFunctionArrayAggIfTest, test_int64_null_cond_treated_as_false) {
    auto elem_type = nullable_int64_type();
    auto elem = elem_type->create_column();
    elem->insert(Field::create_field<TYPE_BIGINT>(1));
    elem->insert(Field::create_field<TYPE_BIGINT>(2));
    elem->insert(Field::create_field<TYPE_BIGINT>(3));
    auto block = make_block(make_nullable_cond_column({"t", "n", "f"}), std::move(elem),
                            nullable_bool_type(), elem_type);
    check_array_agg_if(this, std::move(block), nullable_bool_type(), elem_type,
                       Array {Field::create_field<TYPE_BIGINT>(1)});
}

TEST_F(AggregateFunctionArrayAggIfTest, test_string_keeps_null_elements) {
    auto elem_type = nullable_string_type();
    auto elem = elem_type->create_column();
    elem->insert(Field::create_field<TYPE_STRING>(String("a")));
    elem->insert(Field());
    elem->insert(Field::create_field<TYPE_STRING>(String("c")));
    auto block = make_block(make_cond_column({1, 1, 1}), std::move(elem), bool_type(),
                            elem_type);
    check_array_agg_if(this, std::move(block), bool_type(), elem_type,
                       Array {Field::create_field<TYPE_STRING>(String("a")), Field(),
                              Field::create_field<TYPE_STRING>(String("c"))});
}

namespace {

Field int_array_field(std::initializer_list<int> values) {
    Array fields;
    for (int v : values) {
        fields.push_back(Field::create_field<TYPE_INT>(v));
    }
    return Field::create_field<TYPE_ARRAY>(std::move(fields));
}

} // namespace

TEST_F(AggregateFunctionArrayAggIfTest, test_complex_elem_not_nullable) {
    // Outer element type is a non-nullable ARRAY: exercises the raw native-serde state path.
    auto inner_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto elem_type = std::make_shared<DataTypeArray>(inner_type);
    auto elem = elem_type->create_column();
    elem->insert(int_array_field({1, 2}));
    elem->insert(int_array_field({3}));
    auto block = make_block(make_cond_column({1, 0}), std::move(elem), bool_type(), elem_type);
    check_array_agg_if(this, std::move(block), bool_type(), elem_type,
                       Array {int_array_field({1, 2})});
}

TEST_F(AggregateFunctionArrayAggIfTest, test_complex_elem_nullable) {
    auto inner_type = make_nullable(std::make_shared<DataTypeInt32>());
    auto elem_type = make_nullable(std::make_shared<DataTypeArray>(inner_type));
    auto elem = elem_type->create_column();
    elem->insert(int_array_field({1, 2}));
    elem->insert(Field());
    elem->insert(int_array_field({3}));
    auto block = make_block(make_cond_column({1, 0, 1}), std::move(elem), bool_type(), elem_type);
    check_array_agg_if(this, std::move(block), bool_type(), elem_type,
                       Array {int_array_field({1, 2}), int_array_field({3})});
}

TEST_F(AggregateFunctionArrayAggIfTest, test_string_skip_cond_false_rows) {
    auto elem_type = nullable_string_type();
    auto elem = elem_type->create_column();
    elem->insert(Field::create_field<TYPE_STRING>(String("a")));
    elem->insert(Field::create_field<TYPE_STRING>(String("b")));
    elem->insert(Field::create_field<TYPE_STRING>(String("c")));
    auto block = make_block(make_cond_column({1, 0, 1}), std::move(elem), bool_type(),
                            elem_type);
    check_array_agg_if(this, std::move(block), bool_type(), elem_type,
                       Array {Field::create_field<TYPE_STRING>(String("a")),
                              Field::create_field<TYPE_STRING>(String("c"))});
}

} // namespace doris
