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

#include "core/decimal12.h"
#include "core/uint24.h"
#include "gtest/gtest_pred_impl.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "util/slice.h"

namespace doris {

class TypesTest : public testing::Test {
public:
    TypesTest() {}
    virtual ~TypesTest() {}
};

template <FieldType field_type>
void common_test(typename TypeTraits<field_type>::CppType src_val) {
    EXPECT_EQ(sizeof(src_val), field_type_size(field_type));
}

template <FieldType fieldType>
void test_char(Slice src_val) {
    auto field = std::make_unique<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                                fieldType, false, 0, src_val.size);
    EXPECT_EQ(field->type(), fieldType);
    EXPECT_EQ(sizeof(src_val), field_type_size(field->type()));
}

template <>
void common_test<FieldType::OLAP_FIELD_TYPE_CHAR>(Slice src_val) {
    test_char<FieldType::OLAP_FIELD_TYPE_VARCHAR>(src_val);
}

template <>
void common_test<FieldType::OLAP_FIELD_TYPE_VARCHAR>(Slice src_val) {
    test_char<FieldType::OLAP_FIELD_TYPE_VARCHAR>(src_val);
}

TEST(TypesTest, field_type_size_matches_cpp_type) {
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
