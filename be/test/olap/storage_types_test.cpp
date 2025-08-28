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
#include <stdint.h>

#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "olap/decimal12.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "olap/uint24.h"
#include "runtime/collection_value.h"
#include "util/slice.h"
#include "vec/common/arena.h"

namespace doris {

class TypesTest : public testing::Test {
public:
    TypesTest() {}
    virtual ~TypesTest() {}
};

template <FieldType field_type>
void common_test(typename TypeTraits<field_type>::CppType src_val) {
    const auto* type = get_scalar_type_info<field_type>();

    EXPECT_EQ(field_type, type->type());
    EXPECT_EQ(sizeof(src_val), type->size());
    {
        typename TypeTraits<field_type>::CppType dst_val;
        vectorized::Arena pool;
        type->deep_copy((char*)&dst_val, (char*)&src_val, pool);
        EXPECT_EQ(0, type->cmp((char*)&src_val, (char*)&dst_val));
    }
    {
        typename TypeTraits<field_type>::CppType dst_val;
        type->direct_copy((char*)&dst_val, (char*)&src_val);
        EXPECT_EQ(0, type->cmp((char*)&src_val, (char*)&dst_val));
    }
    // test min
    {
        typename TypeTraits<field_type>::CppType dst_val;
        type->set_to_min((char*)&dst_val);

        EXPECT_TRUE(type->cmp((char*)&src_val, (char*)&dst_val) > 0);
    }
    // test max
    {
        typename TypeTraits<field_type>::CppType dst_val;
        type->set_to_max((char*)&dst_val);
        // NOTE: bool input is true, this will return 0
        EXPECT_TRUE(type->cmp((char*)&src_val, (char*)&dst_val) <= 0);
    }
}

template <FieldType fieldType>
void test_char(Slice src_val) {
    Field* field = FieldFactory::create_by_type(fieldType);
    field->_length = src_val.size;
    const auto* type = field->type_info();

    EXPECT_EQ(field->type(), fieldType);
    EXPECT_EQ(sizeof(src_val), type->size());
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        vectorized::Arena pool;
        type->deep_copy((char*)&dst_val, (char*)&src_val, pool);
        EXPECT_EQ(0, type->cmp((char*)&src_val, (char*)&dst_val));
    }
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        type->direct_copy((char*)&dst_val, (char*)&src_val);
        EXPECT_EQ(0, type->cmp((char*)&src_val, (char*)&dst_val));
    }
    // test min
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        field->set_to_min((char*)&dst_val);

        EXPECT_TRUE(type->cmp((char*)&src_val, (char*)&dst_val) > 0);
    }
    // test max
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        field->set_to_max((char*)&dst_val);

        EXPECT_TRUE(type->cmp((char*)&src_val, (char*)&dst_val) < 0);
    }
    delete field;
}

template <>
void common_test<FieldType::OLAP_FIELD_TYPE_CHAR>(Slice src_val) {
    test_char<FieldType::OLAP_FIELD_TYPE_VARCHAR>(src_val);
}

template <>
void common_test<FieldType::OLAP_FIELD_TYPE_VARCHAR>(Slice src_val) {
    test_char<FieldType::OLAP_FIELD_TYPE_VARCHAR>(src_val);
}

TEST(TypesTest, copy_and_equal) {
    common_test<FieldType::OLAP_FIELD_TYPE_BOOL>(true);
    common_test<FieldType::OLAP_FIELD_TYPE_TINYINT>(112);
    common_test<FieldType::OLAP_FIELD_TYPE_SMALLINT>(static_cast<short>(54321));
    common_test<FieldType::OLAP_FIELD_TYPE_INT>(-123454321);
    common_test<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>(1234543212L);
    common_test<FieldType::OLAP_FIELD_TYPE_BIGINT>(123454321123456789L);
    __int128 int128_val = 1234567899L;
    common_test<FieldType::OLAP_FIELD_TYPE_LARGEINT>(int128_val);
    common_test<FieldType::OLAP_FIELD_TYPE_FLOAT>(1.11);
    common_test<FieldType::OLAP_FIELD_TYPE_DOUBLE>(12221.11);
    decimal12_t decimal_val = {123, 2345};
    common_test<FieldType::OLAP_FIELD_TYPE_DECIMAL>(decimal_val);

    common_test<FieldType::OLAP_FIELD_TYPE_DATE>((1988 << 9) | (2 << 5) | 1);
    common_test<FieldType::OLAP_FIELD_TYPE_DATETIME>(19880201010203L);

    common_test<FieldType::OLAP_FIELD_TYPE_DATEV2>((1988 << 9) | (2 << 5) | 1);

    Slice slice("12345abcde");
    common_test<FieldType::OLAP_FIELD_TYPE_CHAR>(slice);
    common_test<FieldType::OLAP_FIELD_TYPE_VARCHAR>(slice);
}

template <FieldType item_type>
void common_test_array(CollectionValue src_val) {
    TabletColumn list_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                             FieldType::OLAP_FIELD_TYPE_ARRAY);
    int32_t item_length = 0;
    if (item_type == FieldType::OLAP_FIELD_TYPE_CHAR ||
        item_type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
        item_length = 10;
    }
    TabletColumn item_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, item_type, true,
                             0, item_length);
    list_column.add_sub_column(item_column);

    auto array_type = get_type_info(&list_column);
    ASSERT_EQ(item_type,
              dynamic_cast<const ArrayTypeInfo*>(array_type.get())->item_type_info()->type());

    { // test deep copy
        CollectionValue dst_val;
        vectorized::Arena pool;
        array_type->deep_copy((char*)&dst_val, (char*)&src_val, pool);
        EXPECT_EQ(0, array_type->cmp((char*)&src_val, (char*)&dst_val));
    }
    { // test direct copy
        bool null_signs[50];
        uint8_t data[50];
        CollectionValue dst_val(data, sizeof(null_signs), null_signs);
        array_type->direct_copy((char*)&dst_val, (char*)&src_val);
        EXPECT_EQ(0, array_type->cmp((char*)&src_val, (char*)&dst_val));
    }
}

TEST(ArrayTypeTest, copy_and_equal) {
    bool bool_array[3] = {true, false, true};
    bool null_signs[3] = {true, true, true};
    common_test_array<FieldType::OLAP_FIELD_TYPE_BOOL>(CollectionValue(bool_array, 3, null_signs));

    uint8_t tiny_int_array[3] = {3, 4, 5};
    common_test_array<FieldType::OLAP_FIELD_TYPE_TINYINT>(
            CollectionValue(tiny_int_array, 3, null_signs));

    int16_t small_int_array[3] = {123, 234, 345};
    common_test_array<FieldType::OLAP_FIELD_TYPE_SMALLINT>(
            CollectionValue(small_int_array, 3, null_signs));

    int32_t int_array[3] = {-123454321, 123454321, 323412343};
    common_test_array<FieldType::OLAP_FIELD_TYPE_INT>(CollectionValue(int_array, 3, null_signs));

    uint32_t uint_array[3] = {123454321, 2342341, 52435234};
    common_test_array<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>(
            CollectionValue(uint_array, 3, null_signs));

    int64_t bigint_array[3] = {123454321123456789L, 23534543234L, -123454321123456789L};
    common_test_array<FieldType::OLAP_FIELD_TYPE_BIGINT>(
            CollectionValue(bigint_array, 3, null_signs));

    __int128 large_int_array[3] = {1234567899L, 1234567899L, -12345631899L};
    common_test_array<FieldType::OLAP_FIELD_TYPE_LARGEINT>(
            CollectionValue(large_int_array, 3, null_signs));

    float float_array[3] = {1.11, 2.22, -3.33};
    common_test_array<FieldType::OLAP_FIELD_TYPE_FLOAT>(
            CollectionValue(float_array, 3, null_signs));

    double double_array[3] = {12221.11, 12221.11, -12221.11};
    common_test_array<FieldType::OLAP_FIELD_TYPE_DOUBLE>(
            CollectionValue(double_array, 3, null_signs));

    decimal12_t decimal_array[3] = {{123, 234}, {345, 453}, {4524, 2123}};
    common_test_array<FieldType::OLAP_FIELD_TYPE_DECIMAL>(
            CollectionValue(decimal_array, 3, null_signs));

    uint24_t date_array[3] = {(1988 << 9) | (2 << 5) | 1, (1998 << 9) | (2 << 5) | 1,
                              (2008 << 9) | (2 << 5) | 1};
    common_test_array<FieldType::OLAP_FIELD_TYPE_DATE>(CollectionValue(date_array, 3, null_signs));

    uint32_t date_v2_array[3] = {(1988 << 9) | (2 << 5) | 1, (1998 << 9) | (2 << 5) | 1,
                                 (2008 << 9) | (2 << 5) | 1};
    common_test_array<FieldType::OLAP_FIELD_TYPE_DATEV2>(
            CollectionValue(date_v2_array, 3, null_signs));

    int64_t datetime_array[3] = {19880201010203L, 19980201010203L, 20080204010203L};
    common_test_array<FieldType::OLAP_FIELD_TYPE_DATETIME>(
            CollectionValue(datetime_array, 3, null_signs));

    Slice char_array[3] = {"12345abcde", "12345abcde", "asdf322"};
    common_test_array<FieldType::OLAP_FIELD_TYPE_CHAR>(CollectionValue(char_array, 3, null_signs));
    common_test_array<FieldType::OLAP_FIELD_TYPE_VARCHAR>(
            CollectionValue(char_array, 3, null_signs));
}

TEST(TypesTest, has_char_type) {
    // Test basic types
    TabletColumn char_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                             FieldType::OLAP_FIELD_TYPE_CHAR);
    EXPECT_TRUE(char_column.has_char_type());

    TabletColumn int_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                            FieldType::OLAP_FIELD_TYPE_INT);
    EXPECT_FALSE(int_column.has_char_type());

    // Test array type with char element
    TabletColumn array_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                              FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn array_element(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                               FieldType::OLAP_FIELD_TYPE_CHAR);
    array_column.add_sub_column(array_element);
    EXPECT_TRUE(array_column.has_char_type());

    // Test array type with non-char element
    TabletColumn array_column2(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                               FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn array_element2(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                FieldType::OLAP_FIELD_TYPE_INT);
    array_column2.add_sub_column(array_element2);
    EXPECT_FALSE(array_column2.has_char_type());

    // Test nested array with char element
    TabletColumn nested_array(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                              FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn inner_array(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                             FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn char_element(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                              FieldType::OLAP_FIELD_TYPE_CHAR);
    inner_array.add_sub_column(char_element);
    nested_array.add_sub_column(inner_array);
    EXPECT_TRUE(nested_array.has_char_type());
}

} // namespace doris
