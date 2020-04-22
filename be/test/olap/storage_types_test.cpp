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

#include "olap/types.h"

#include <gtest/gtest.h>

#include "runtime/mem_tracker.h"
#include "runtime/mem_pool.h"
#include "util/slice.h"
#include "olap/field.h"

namespace doris {

class TypesTest : public testing::Test {
public:
    TypesTest() { }
    virtual ~TypesTest() {
    }
};

template<FieldType field_type>
void common_test(typename TypeTraits<field_type>::CppType src_val) {
    TypeInfo* type = get_type_info(field_type);

    ASSERT_EQ(field_type, type->type());
    ASSERT_EQ(sizeof(src_val), type->size());
    {
        typename TypeTraits<field_type>::CppType dst_val;
        MemTracker tracker;
        MemPool pool(&tracker);
        type->deep_copy((char*)&dst_val, (char*)&src_val, &pool);
        ASSERT_TRUE(type->equal((char*)&src_val, (char*)&dst_val));
        ASSERT_EQ(0, type->cmp((char*)&src_val, (char*)&dst_val));
    }
    {
        typename TypeTraits<field_type>::CppType dst_val;
        type->direct_copy((char*)&dst_val, (char*)&src_val);
        ASSERT_TRUE(type->equal((char*)&src_val, (char*)&dst_val));
        ASSERT_EQ(0, type->cmp((char*)&src_val, (char*)&dst_val));
    }
    // test min
    {
        typename TypeTraits<field_type>::CppType dst_val;
        type->set_to_min((char*)&dst_val);


        ASSERT_FALSE(type->equal((char*)&src_val, (char*)&dst_val));
        ASSERT_TRUE(type->cmp((char*)&src_val, (char*)&dst_val) > 0);
    }
    // test max
    {
        typename TypeTraits<field_type>::CppType dst_val;
        type->set_to_max((char*)&dst_val);
        // NOTE: bool input is true, this will return 0
        ASSERT_TRUE(type->cmp((char*)&src_val, (char*)&dst_val) <= 0);
    }
}

template<FieldType fieldType>
void test_char(Slice src_val) {
    Field* field = FieldFactory::create_by_type(fieldType);
    field->_length = src_val.size;
    const TypeInfo* type = field->type_info();

    ASSERT_EQ(field->type(), fieldType);
    ASSERT_EQ(sizeof(src_val), type->size());
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        MemTracker tracker;
        MemPool pool(&tracker);
        type->deep_copy((char*)&dst_val, (char*)&src_val, &pool);
        ASSERT_TRUE(type->equal((char*)&src_val, (char*)&dst_val));
        ASSERT_EQ(0, type->cmp((char*)&src_val, (char*)&dst_val));
    }
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        type->direct_copy((char*)&dst_val, (char*)&src_val);
        ASSERT_TRUE(type->equal((char*)&src_val, (char*)&dst_val));
        ASSERT_EQ(0, type->cmp((char*)&src_val, (char*)&dst_val));
    }
    // test min
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        field->set_to_min((char*)&dst_val);

        ASSERT_FALSE(type->equal((char*)&src_val, (char*)&dst_val));
        ASSERT_TRUE(type->cmp((char*)&src_val, (char*)&dst_val) > 0);
    }
    // test max
    {
        char buf[64];
        Slice dst_val(buf, sizeof(buf));
        field->set_to_max((char*)&dst_val);

        ASSERT_FALSE(type->equal((char*)&src_val, (char*)&dst_val));
        ASSERT_TRUE(type->cmp((char*)&src_val, (char*)&dst_val) < 0);
    }
    delete field;
}

template<>
void common_test<OLAP_FIELD_TYPE_CHAR>(Slice src_val) {
    test_char<OLAP_FIELD_TYPE_VARCHAR>(src_val);
}

template<>
void common_test<OLAP_FIELD_TYPE_VARCHAR>(Slice src_val) {
    test_char<OLAP_FIELD_TYPE_VARCHAR>(src_val);
}

TEST(TypesTest, copy_and_equal) {
    common_test<OLAP_FIELD_TYPE_BOOL>(true);
    common_test<OLAP_FIELD_TYPE_TINYINT>(112);
    common_test<OLAP_FIELD_TYPE_SMALLINT>(54321);
    common_test<OLAP_FIELD_TYPE_INT>(-123454321);
    common_test<OLAP_FIELD_TYPE_UNSIGNED_INT>(1234543212L);
    common_test<OLAP_FIELD_TYPE_BIGINT>(123454321123456789L);
    __int128 int128_val = 1234567899L;
    common_test<OLAP_FIELD_TYPE_LARGEINT>(int128_val);
    common_test<OLAP_FIELD_TYPE_FLOAT>(1.11);
    common_test<OLAP_FIELD_TYPE_DOUBLE>(12221.11);
    decimal12_t decimal_val(123, 2345);
    common_test<OLAP_FIELD_TYPE_DECIMAL>(decimal_val);

    common_test<OLAP_FIELD_TYPE_DATE>((1988 << 9) | (2 << 5) | 1);
    common_test<OLAP_FIELD_TYPE_DATETIME>(19880201010203L);

    Slice slice("12345abcde");
    common_test<OLAP_FIELD_TYPE_CHAR>(slice);
    common_test<OLAP_FIELD_TYPE_VARCHAR>(slice);
}

} // namespace doris

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv); 
    return RUN_ALL_TESTS();
}
