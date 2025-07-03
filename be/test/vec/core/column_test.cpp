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

#include "runtime/primitive_type.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"

namespace doris::vectorized {

class ColumnTest : public ::testing::Test {
protected:
    void SetUp() override {
        col_str = ColumnString::create();
        col_str->insert_data("aaa", 3);
        col_str->insert_data("bb", 2);
        col_str->insert_data("cccc", 4);

        col_int = ColumnInt64::create();
        col_int->insert_value(1);
        col_int->insert_value(2);
        col_int->insert_value(3);

        col_dcm = ColumnDecimal64::create(0, 3);
        col_dcm->insert_value(1.23);
        col_dcm->insert_value(4.56);
        col_dcm->insert_value(7.89);

        col_arr = ColumnArray::create(ColumnInt64::create(), ColumnArray::ColumnOffsets::create());
        Array array1 = {Field::create_field<TYPE_BIGINT>(1), Field::create_field<TYPE_BIGINT>(2),
                        Field::create_field<TYPE_BIGINT>(3)};
        Array array2 = {Field::create_field<TYPE_BIGINT>(4)};
        col_arr->insert(Field::create_field<TYPE_ARRAY>(array1));
        col_arr->insert(Field::create_field<TYPE_ARRAY>(Array()));
        col_arr->insert(Field::create_field<TYPE_ARRAY>(array2));

        col_map = ColumnMap::create(ColumnString::create(), ColumnInt64::create(),
                                    ColumnArray::ColumnOffsets::create());
        Array k1 = {Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("b"),
                    Field::create_field<TYPE_STRING>("c")};
        Array v1 = {Field::create_field<TYPE_BIGINT>(1), Field::create_field<TYPE_BIGINT>(2),
                    Field::create_field<TYPE_BIGINT>(3)};
        Array k2 = {Field::create_field<TYPE_STRING>("d")};
        Array v2 = {Field::create_field<TYPE_BIGINT>(4)};
        Array a = Array();
        Map map1, map2, map3;
        map1.push_back(Field::create_field<TYPE_ARRAY>(k1));
        map1.push_back(Field::create_field<TYPE_ARRAY>(v1));
        col_map->insert(Field::create_field<TYPE_MAP>(map1));
        map3.push_back(Field::create_field<TYPE_ARRAY>(a));
        map3.push_back(Field::create_field<TYPE_ARRAY>(a));
        col_map->insert(Field::create_field<TYPE_MAP>(map3));
        map2.push_back(Field::create_field<TYPE_ARRAY>(k2));
        map2.push_back(Field::create_field<TYPE_ARRAY>(v2));
        col_map->insert(Field::create_field<TYPE_MAP>(map2));
    }

    ColumnString::MutablePtr col_str;
    ColumnInt64::MutablePtr col_int;
    ColumnDecimal64::MutablePtr col_dcm;
    ColumnArray::MutablePtr col_arr;
    ColumnMap::MutablePtr col_map;
};

TEST_F(ColumnTest, CutColumnString) {
    auto cut_col = col_str->cut(0, 2);
    EXPECT_EQ(cut_col->size(), 2);
    EXPECT_EQ(cut_col->get_data_at(0), StringRef("aaa"));
    EXPECT_EQ(cut_col->get_data_at(1), StringRef("bb"));
    EXPECT_THROW({ col_str->cut(0, 10); }, doris::Exception);
}

TEST_F(ColumnTest, CutColumnInt64) {
    auto cut_col = col_int->cut(0, 2);
    EXPECT_EQ(cut_col->size(), 2);
    EXPECT_EQ(static_cast<const ColumnInt64*>(cut_col.get())->get_element(0), 1);
    EXPECT_EQ(static_cast<const ColumnInt64*>(cut_col.get())->get_element(1), 2);
    EXPECT_THROW({ col_int->cut(0, 10); }, doris::Exception);
}

TEST_F(ColumnTest, CutColumnDecimal64) {
    auto cut_col = col_dcm->cut(0, 2);
    EXPECT_EQ(cut_col->size(), 2);
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(cut_col.get())->get_element(0), Decimal64(1.23));
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(cut_col.get())->get_element(1), Decimal64(4.56));
    EXPECT_THROW({ col_dcm->cut(0, 10); }, doris::Exception);
}

TEST_F(ColumnTest, ShrinkColumnString) {
    auto shrunk_col = col_str->shrink(2);
    EXPECT_EQ(shrunk_col->size(), 2);
    EXPECT_EQ(shrunk_col->get_data_at(0), StringRef("aaa"));
    EXPECT_EQ(shrunk_col->get_data_at(1), StringRef("bb"));

    shrunk_col = shrunk_col->shrink(10);
    EXPECT_EQ(shrunk_col->size(), 10);
    EXPECT_EQ(shrunk_col->use_count(), 1);
    EXPECT_EQ(shrunk_col->get_data_at(0), StringRef("aaa"));
    EXPECT_EQ(shrunk_col->get_data_at(1), StringRef("bb"));
    // column string scale in will not clear
}

TEST_F(ColumnTest, ShrinkColumnInt64) {
    auto shrunk_col = col_int->shrink(2);
    EXPECT_EQ(shrunk_col->size(), 2);
    EXPECT_EQ(static_cast<const ColumnInt64*>(shrunk_col.get())->get_element(0), 1);
    EXPECT_EQ(static_cast<const ColumnInt64*>(shrunk_col.get())->get_element(1), 2);

    shrunk_col = col_int->shrink(10);
    EXPECT_EQ(shrunk_col->size(), 10);
    EXPECT_EQ(shrunk_col->use_count(), 1);
    EXPECT_EQ(static_cast<const ColumnInt64*>(shrunk_col.get())->get_element(0), 1);
    EXPECT_EQ(static_cast<const ColumnInt64*>(shrunk_col.get())->get_element(1), 2);
    // column vector scale out will not empty init
}

TEST_F(ColumnTest, ShrinkColumnDecimal64) {
    auto shrunk_col = col_dcm->shrink(2);
    EXPECT_EQ(shrunk_col->size(), 2);
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(shrunk_col.get())->get_element(0),
              Decimal64(1.23));
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(shrunk_col.get())->get_element(1),
              Decimal64(4.56));

    shrunk_col = col_dcm->shrink(10);
    EXPECT_EQ(shrunk_col->size(), 10);
    EXPECT_EQ(shrunk_col->use_count(), 1);
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(shrunk_col.get())->get_element(0),
              Decimal64(1.23));
    EXPECT_EQ(static_cast<const ColumnDecimal64*>(shrunk_col.get())->get_element(1),
              Decimal64(4.56));
    // column decimal scale out will not empty init
}

TEST_F(ColumnTest, ShrinkColumnArray) {
    // check column array result
    // array : [[1,2,3],[],[4]]
    auto shrunk_col = col_arr->shrink(2);
    EXPECT_EQ(shrunk_col->size(), 2);
    auto data_col = assert_cast<const ColumnArray&>(*shrunk_col).get_data_ptr();
    EXPECT_EQ(data_col->size(), 3);
    auto v = get<Array>(shrunk_col->operator[](0));
    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(get<int32_t>(v[0]), 1);
    EXPECT_EQ(get<int32_t>(v[1]), 2);
    EXPECT_EQ(get<int32_t>(v[2]), 3);
    v = get<Array>(shrunk_col->operator[](1));
    EXPECT_EQ(v.size(), 0);
    EXPECT_EQ(get<int32_t>(data_col->operator[](0)), 1);
    EXPECT_EQ(get<int32_t>(data_col->operator[](1)), 2);
    EXPECT_EQ(get<int32_t>(data_col->operator[](2)), 3);

    // expand will not make data expand
    EXPECT_EQ(col_arr->size(), 2);
    shrunk_col = col_arr->shrink(10);
    EXPECT_EQ(shrunk_col->size(), 10);
    data_col = assert_cast<const ColumnArray&>(*shrunk_col).get_data_ptr();
    EXPECT_EQ(data_col->size(), 3);
    v = get<Array>(shrunk_col->operator[](0));
    EXPECT_EQ(v.size(), 3);
    EXPECT_EQ(get<int32_t>(v[0]), 1);
    EXPECT_EQ(get<int32_t>(v[1]), 2);
    EXPECT_EQ(get<int32_t>(v[2]), 3);
    v = get<Array>(shrunk_col->operator[](1));
    EXPECT_EQ(v.size(), 0);
    v = get<Array>(shrunk_col->operator[](2));
    EXPECT_EQ(v.size(), 0);
}

TEST_F(ColumnTest, ShrinkColumnMap) {
    // check column map result
    // map : {"a":1,"b":2,"c":3},{:},{"d":4}
    auto shrunk_col = col_map->shrink(2);
    EXPECT_EQ(shrunk_col->size(), 2);
    auto data_col = assert_cast<const ColumnMap&>(*shrunk_col).get_values_ptr();
    EXPECT_EQ(data_col->size(), 3);
    auto v = get<Map>(shrunk_col->operator[](0));
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(get<Array>(v[0]),
              Array({Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("b"),
                     Field::create_field<TYPE_STRING>("c")}));
    EXPECT_EQ(get<Array>(v[1]),
              Array({Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2),
                     Field::create_field<TYPE_INT>(3)}));
    v = get<Map>(shrunk_col->operator[](1));
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(get<Array>(v[0]), Array());
    EXPECT_EQ(get<Array>(v[1]), Array());
    EXPECT_EQ(get<int32_t>(data_col->operator[](0)), 1);
    EXPECT_EQ(get<int32_t>(data_col->operator[](1)), 2);
    EXPECT_EQ(get<int32_t>(data_col->operator[](2)), 3);

    // expand will not make data expand
    EXPECT_EQ(col_map->size(), 2);
    shrunk_col = col_map->shrink(10);
    EXPECT_EQ(shrunk_col->size(), 10);
    data_col = assert_cast<const ColumnMap&>(*shrunk_col).get_values_ptr();
    EXPECT_EQ(data_col->size(), 3);
    v = get<Map>(shrunk_col->operator[](0));
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(get<Array>(v[0]),
              Array({Field::create_field<TYPE_STRING>("a"), Field::create_field<TYPE_STRING>("b"),
                     Field::create_field<TYPE_STRING>("c")}));
    EXPECT_EQ(get<Array>(v[1]),
              Array({Field::create_field<TYPE_INT>(1), Field::create_field<TYPE_INT>(2),
                     Field::create_field<TYPE_INT>(3)}));
    v = get<Map>(shrunk_col->operator[](1));
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(get<Array>(v[0]), Array());
    EXPECT_EQ(get<Array>(v[1]), Array());
    v = get<Map>(shrunk_col->operator[](2));
    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(get<Array>(v[0]), Array());
    EXPECT_EQ(get<Array>(v[1]), Array());
}

} // namespace doris::vectorized
