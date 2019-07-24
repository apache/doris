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

#include "olap/aggregate_func.h"

#include <gtest/gtest.h>

#include "olap/decimal12.h"
#include "olap/uint24.h"

namespace doris {

class AggregateFuncTest : public testing::Test {
public:
    AggregateFuncTest() { }
    virtual ~AggregateFuncTest() {
    }
};

template<FieldType field_type>
void test_min() {
    using CppType = typename CppTypeTraits<field_type>::CppType;
    char buf[64];

    Arena arena;
    const AggregateInfo* agg = get_aggregate_info(OLAP_FIELD_AGGREGATION_MIN, field_type);
    agg->init(buf, &arena);

    // null
    {
        char val_buf[16];
        *(bool*)val_buf = true;
        agg->update(buf, val_buf, &arena);
        ASSERT_TRUE(*(bool*)(buf));
    }
    // 100
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        CppType val = 100;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(100, val);
    }
    // 200
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        CppType val = 200;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(100, val);
    }
    // 50
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        CppType val = 50;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(50, val);
    }
    // null
    {
        char val_buf[16];
        *(bool*)val_buf = true;
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        CppType val;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(50, val);
    }
    agg->finalize(buf, &arena);
    ASSERT_FALSE(*(bool*)(buf));
    CppType val;
    memcpy(&val, buf + 1, sizeof(CppType));
    ASSERT_EQ(50, val);
}

TEST_F(AggregateFuncTest, min) {
    test_min<OLAP_FIELD_TYPE_INT>();
    test_min<OLAP_FIELD_TYPE_LARGEINT>();
}

template<FieldType field_type>
void test_max() {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    char buf[64];

    Arena arena;
    const AggregateInfo* agg = get_aggregate_info(OLAP_FIELD_AGGREGATION_MAX, field_type);
    agg->init(buf, &arena);

    // null
    {
        char val_buf[16];
        *(bool*)val_buf = true;
        agg->update(buf, val_buf, &arena);
        ASSERT_TRUE(*(bool*)(buf));
    }
    // 100
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        CppType val = 100;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(100, val);
    }
    // 200
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        CppType val = 200;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(200, val);
    }
    // 50
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        CppType val = 50;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(200, val);
    }
    // null
    {
        char val_buf[16];
        *(bool*)val_buf = true;
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        CppType val;
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(200, val);
    }
    agg->finalize(buf, &arena);
    ASSERT_FALSE(*(bool*)(buf));
    CppType val;
    memcpy(&val, buf + 1, sizeof(CppType));
    ASSERT_EQ(200, val);
}

TEST_F(AggregateFuncTest, max) {
    test_max<OLAP_FIELD_TYPE_INT>();
    test_max<OLAP_FIELD_TYPE_LARGEINT>();
}

template<FieldType field_type>
void test_sum() {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    char buf[64];

    Arena arena;
    const AggregateInfo* agg = get_aggregate_info(OLAP_FIELD_AGGREGATION_SUM, field_type);
    agg->init(buf, &arena);

    // null
    {
        char val_buf[16];
        *(bool*)val_buf = true;
        agg->update(buf, val_buf, &arena);
        ASSERT_TRUE(*(bool*)(buf));
    }
    // 100
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        CppType val = 100;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(100, val);
    }
    // 200
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        CppType val = 200;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(300, val);
    }
    // 50
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        CppType val = 50;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(350, val);
    }
    // null
    {
        char val_buf[16];
        *(bool*)val_buf = true;
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        CppType val;
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(350, val);
    }
    agg->finalize(buf, &arena);
    ASSERT_FALSE(*(bool*)(buf));
    CppType val;
    memcpy(&val, buf + 1, sizeof(CppType));
    ASSERT_EQ(350, val);
}

TEST_F(AggregateFuncTest, sum) {
    test_sum<OLAP_FIELD_TYPE_INT>();
    test_sum<OLAP_FIELD_TYPE_LARGEINT>();
}

template<FieldType field_type>
void test_replace() {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    char buf[64];

    Arena arena;
    const AggregateInfo* agg = get_aggregate_info(OLAP_FIELD_AGGREGATION_REPLACE, field_type);
    agg->init(buf, &arena);

    // null
    {
        char val_buf[16];
        *(bool*)val_buf = true;
        agg->update(buf, val_buf, &arena);
        ASSERT_TRUE(*(bool*)(buf));
    }
    // 100
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        CppType val = 100;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(100, val);
    }
    // null
    {
        char val_buf[16];
        *(bool*)val_buf = true;
        agg->update(buf, val_buf, &arena);
        ASSERT_TRUE(*(bool*)(buf));
    }
    // 50
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        CppType val = 50;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(50, val);
    }
    agg->finalize(buf, &arena);
    ASSERT_FALSE(*(bool*)(buf));
    CppType val;
    memcpy(&val, buf + 1, sizeof(CppType));
    ASSERT_EQ(50, val);
}

template<FieldType field_type>
void test_replace_string() {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    char buf[64];

    Arena arena;
    const AggregateInfo* agg = get_aggregate_info(OLAP_FIELD_AGGREGATION_REPLACE, field_type);
    agg->init(buf, &arena);

    // null
    {
        char val_buf[16];
        *(bool*)val_buf = true;
        agg->update(buf, val_buf, &arena);
        ASSERT_TRUE(*(bool*)(buf));
    }
    // "12345"
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        Slice* slice = (Slice*)(val_buf + 1);
        slice->data = (char*)"1234567890";
        slice->size = 10;
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        slice = (Slice*)(buf + 1);
        ASSERT_EQ(10, slice->size);
        ASSERT_STREQ("1234567890", slice->to_string().c_str());
    }
    // abc
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        Slice* slice = (Slice*)(val_buf + 1);
        slice->data = (char*)"abc";
        slice->size = 3;
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        slice = (Slice*)(buf + 1);
        ASSERT_EQ(3, slice->size);
        ASSERT_STREQ("abc", slice->to_string().c_str());
    }
    // null
    {
        char val_buf[16];
        *(bool*)val_buf = true;
        agg->update(buf, val_buf, &arena);
        ASSERT_TRUE(*(bool*)(buf));
    }
    // "12345"
    {
        char val_buf[16];
        *(bool*)val_buf = false;
        Slice* slice = (Slice*)(val_buf + 1);
        slice->data = (char*)"12345";
        slice->size = 5;
        agg->update(buf, val_buf, &arena);
        ASSERT_FALSE(*(bool*)(buf));
        slice = (Slice*)(buf + 1);
        ASSERT_EQ(5, slice->size);
        ASSERT_STREQ("12345", slice->to_string().c_str());
    }
    agg->finalize(buf, &arena);
    ASSERT_FALSE(*(bool*)(buf));
    Slice* slice = (Slice*)(buf + 1);
    ASSERT_EQ(5, slice->size);
    ASSERT_STREQ("12345", slice->to_string().c_str());
}

TEST_F(AggregateFuncTest, replace) {
    test_replace<OLAP_FIELD_TYPE_INT>();
    test_replace<OLAP_FIELD_TYPE_LARGEINT>();
    test_replace_string<OLAP_FIELD_TYPE_CHAR>();
    test_replace_string<OLAP_FIELD_TYPE_VARCHAR>();
}

}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv); 
    return RUN_ALL_TESTS();
}
