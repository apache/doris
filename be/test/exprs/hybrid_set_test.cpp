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

#include "exprs/hybrid_set.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/config.h"
#include "exprs/create_predicate_function.h"
#include "gtest/internal/gtest-internal.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

// mock
class HybridSetTest : public testing::Test {
public:
    HybridSetTest() {}

protected:
};

TEST_F(HybridSetTest, bool) {
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_BOOLEAN, false));
    bool a = true;
    set->insert(&a);
    a = false;
    set->insert(&a);
    a = true;
    set->insert(&a);
    a = false;
    set->insert(&a);

    EXPECT_EQ(2, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(bool*)base->get_value());
        base->next();
    }

    a = true;
    EXPECT_TRUE(set->find(&a));
    a = false;
    EXPECT_TRUE(set->find(&a));
}

#define TEST_NUMERIC(primitive_type)                                               \
    do {                                                                           \
        using NumericType = PrimitiveTypeTraits<primitive_type>::CppType;          \
        std::unique_ptr<HybridSetBase> set(create_set(primitive_type, false));     \
        NumericType min = type_limit<NumericType>::min();                          \
        NumericType max = type_limit<NumericType>::max();                          \
        NumericType mid = NumericType(NumericType(min + max) / NumericType(2));    \
        EXPECT_NE(min, mid);                                                       \
        EXPECT_NE(max, mid);                                                       \
        EXPECT_FALSE(set->find(&min));                                             \
        set->insert(&min);                                                         \
        EXPECT_FALSE(set->find(&max));                                             \
        set->insert(&max);                                                         \
        EXPECT_FALSE(set->find(&mid));                                             \
        set->insert(&mid);                                                         \
        EXPECT_EQ(3, set->size());                                                 \
                                                                                   \
        HybridSetBase::IteratorBase* base = set->begin();                          \
                                                                                   \
        while (base->has_next()) {                                                 \
            base->next();                                                          \
        }                                                                          \
                                                                                   \
        EXPECT_TRUE(set->find(&min));                                              \
        EXPECT_TRUE(set->find(&max));                                              \
        EXPECT_TRUE(set->find(&mid));                                              \
                                                                                   \
        std::unique_ptr<HybridSetBase> set2(create_set<3>(primitive_type, false)); \
        set2->insert(&min);                                                        \
        set2->insert(&max);                                                        \
        set2->insert(&mid);                                                        \
        EXPECT_EQ(3, set2->size());                                                \
                                                                                   \
        base = set->begin();                                                       \
                                                                                   \
        while (base->has_next()) {                                                 \
            base->next();                                                          \
        }                                                                          \
                                                                                   \
        EXPECT_TRUE(set2->find(&min));                                             \
        EXPECT_TRUE(set2->find(&max));                                             \
        EXPECT_TRUE(set2->find(&mid));                                             \
    } while (0)

TEST_F(HybridSetTest, Numeric) {
    TEST_NUMERIC(PrimitiveType::TYPE_TINYINT);
    TEST_NUMERIC(PrimitiveType::TYPE_SMALLINT);
    TEST_NUMERIC(PrimitiveType::TYPE_INT);
    TEST_NUMERIC(PrimitiveType::TYPE_BIGINT);
    TEST_NUMERIC(PrimitiveType::TYPE_LARGEINT);
    TEST_NUMERIC(PrimitiveType::TYPE_FLOAT);
    TEST_NUMERIC(PrimitiveType::TYPE_DOUBLE);
    TEST_NUMERIC(PrimitiveType::TYPE_IPV4);
    TEST_NUMERIC(PrimitiveType::TYPE_IPV6);
    TEST_NUMERIC(PrimitiveType::TYPE_DECIMAL256);
    TEST_NUMERIC(PrimitiveType::TYPE_DECIMALV2);
    TEST_NUMERIC(PrimitiveType::TYPE_DECIMAL32);
    TEST_NUMERIC(PrimitiveType::TYPE_DECIMAL64);
    TEST_NUMERIC(PrimitiveType::TYPE_DECIMAL128I);
}

#define TEST_DATE(primitive_type)                                                  \
    do {                                                                           \
        using NumericType = PrimitiveTypeTraits<primitive_type>::CppType;          \
        std::unique_ptr<HybridSetBase> set(create_set(primitive_type, false));     \
        NumericType min = type_limit<NumericType>::min();                          \
        NumericType max = type_limit<NumericType>::max();                          \
        NumericType def = NumericType {};                                          \
        EXPECT_NE(min, def);                                                       \
        EXPECT_NE(max, def);                                                       \
        EXPECT_FALSE(set->find(&min));                                             \
        set->insert(&min);                                                         \
        EXPECT_FALSE(set->find(&max));                                             \
        set->insert(&max);                                                         \
        EXPECT_FALSE(set->find(&def));                                             \
        set->insert(&def);                                                         \
        EXPECT_EQ(3, set->size());                                                 \
                                                                                   \
        HybridSetBase::IteratorBase* base = set->begin();                          \
                                                                                   \
        while (base->has_next()) {                                                 \
            base->next();                                                          \
        }                                                                          \
                                                                                   \
        EXPECT_TRUE(set->find(&min));                                              \
        EXPECT_TRUE(set->find(&max));                                              \
        EXPECT_TRUE(set->find(&def));                                              \
                                                                                   \
        std::unique_ptr<HybridSetBase> set2(create_set<3>(primitive_type, false)); \
        set2->insert(&min);                                                        \
        set2->insert(&max);                                                        \
        set2->insert(&def);                                                        \
        EXPECT_EQ(3, set2->size());                                                \
                                                                                   \
        base = set2->begin();                                                      \
                                                                                   \
        while (base->has_next()) {                                                 \
            base->next();                                                          \
        }                                                                          \
                                                                                   \
        EXPECT_TRUE(set2->find(&min));                                             \
        EXPECT_TRUE(set2->find(&max));                                             \
        EXPECT_TRUE(set2->find(&def));                                             \
    } while (0)

TEST_F(HybridSetTest, Date) {
    TEST_DATE(PrimitiveType::TYPE_DATE);
    TEST_DATE(PrimitiveType::TYPE_DATEV2);
    TEST_DATE(PrimitiveType::TYPE_DATETIME);
    TEST_DATE(PrimitiveType::TYPE_DATETIMEV2);
}

TEST_F(HybridSetTest, tinyint) {
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_TINYINT, false));
    int8_t a = 0;
    set->insert(&a);
    a = 1;
    set->insert(&a);
    a = 2;
    set->insert(&a);
    a = 3;
    set->insert(&a);
    a = 4;
    set->insert(&a);
    a = 4;
    set->insert(&a);

    EXPECT_EQ(5, set->size());

    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int8_t*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1;
    EXPECT_TRUE(set->find(&a));
    a = 2;
    EXPECT_TRUE(set->find(&a));
    a = 3;
    EXPECT_TRUE(set->find(&a));
    a = 4;
    EXPECT_TRUE(set->find(&a));
    a = 5;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, smallint) {
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_SMALLINT, false));
    int16_t a = 0;
    set->insert(&a);
    a = 1;
    set->insert(&a);
    a = 2;
    set->insert(&a);
    a = 3;
    set->insert(&a);
    a = 4;
    set->insert(&a);
    a = 4;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int16_t*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1;
    EXPECT_TRUE(set->find(&a));
    a = 2;
    EXPECT_TRUE(set->find(&a));
    a = 3;
    EXPECT_TRUE(set->find(&a));
    a = 4;
    EXPECT_TRUE(set->find(&a));
    a = 5;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, int) {
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_INT, false));
    int32_t a = 0;
    set->insert(&a);
    a = 1;
    set->insert(&a);
    a = 2;
    set->insert(&a);
    a = 3;
    set->insert(&a);
    a = 4;
    set->insert(&a);
    a = 4;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int32_t*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1;
    EXPECT_TRUE(set->find(&a));
    a = 2;
    EXPECT_TRUE(set->find(&a));
    a = 3;
    EXPECT_TRUE(set->find(&a));
    a = 4;
    EXPECT_TRUE(set->find(&a));
    a = 5;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, bigint) {
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_BIGINT, false));
    int64_t a = 0;
    set->insert(&a);
    a = 1;
    set->insert(&a);
    a = 2;
    set->insert(&a);
    a = 3;
    set->insert(&a);
    a = 4;
    set->insert(&a);
    a = 4;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(int64_t*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1;
    EXPECT_TRUE(set->find(&a));
    a = 2;
    EXPECT_TRUE(set->find(&a));
    a = 3;
    EXPECT_TRUE(set->find(&a));
    a = 4;
    EXPECT_TRUE(set->find(&a));
    a = 5;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, float) {
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_FLOAT, false));
    float a = 0;
    set->insert(&a);
    a = 1.1;
    set->insert(&a);
    a = 2.1;
    set->insert(&a);
    a = 3.1;
    set->insert(&a);
    a = 4.1;
    set->insert(&a);
    a = 4.1;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(float*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1.1;
    EXPECT_TRUE(set->find(&a));
    a = 2.1;
    EXPECT_TRUE(set->find(&a));
    a = 3.1;
    EXPECT_TRUE(set->find(&a));
    a = 4.1;
    EXPECT_TRUE(set->find(&a));
    a = 5.1;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, double) {
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_DOUBLE, false));
    double a = 0;
    set->insert(&a);
    a = 1.1;
    set->insert(&a);
    a = 2.1;
    set->insert(&a);
    a = 3.1;
    set->insert(&a);
    a = 4.1;
    set->insert(&a);
    a = 4.1;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << (*(double*)base->get_value());
        base->next();
    }

    a = 0;
    EXPECT_TRUE(set->find(&a));
    a = 1.1;
    EXPECT_TRUE(set->find(&a));
    a = 2.1;
    EXPECT_TRUE(set->find(&a));
    a = 3.1;
    EXPECT_TRUE(set->find(&a));
    a = 4.1;
    EXPECT_TRUE(set->find(&a));
    a = 5.1;
    EXPECT_FALSE(set->find(&a));
}
TEST_F(HybridSetTest, string) {
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_VARCHAR, false));
    StringRef a;

    char buf[100];

    snprintf(buf, 100, "abcdefghigk");
    a.data = buf;

    a.size = 0;
    set->insert(&a);
    a.size = 1;
    set->insert(&a);
    a.size = 2;
    set->insert(&a);
    a.size = 3;
    set->insert(&a);
    a.size = 4;
    set->insert(&a);
    a.size = 4;
    set->insert(&a);

    EXPECT_EQ(5, set->size());
    HybridSetBase::IteratorBase* base = set->begin();

    while (base->has_next()) {
        LOG(INFO) << ((StringRef*)base->get_value())->data;
        base->next();
    }

    StringRef b;

    char buf1[100];

    snprintf(buf1, 100, "abcdefghigk");
    b.data = buf1;

    b.size = 0;
    EXPECT_TRUE(set->find(&b));
    b.size = 1;
    EXPECT_TRUE(set->find(&b));
    b.size = 2;
    EXPECT_TRUE(set->find(&b));
    b.size = 3;
    EXPECT_TRUE(set->find(&b));
    b.size = 4;
    EXPECT_TRUE(set->find(&b));
    b.size = 5;
    EXPECT_FALSE(set->find(&b));
}

#define TEST_FIXED_CONTAINER(N)                                                             \
    {                                                                                       \
        std::unique_ptr<HybridSetBase> set(create_set<N>(PrimitiveType::TYPE_INT, false));  \
                                                                                            \
        auto column = vectorized::ColumnHelper::create_column<vectorized::DataTypeInt32>(   \
                {1, 2, 3, 4, 5, 6, 7, 8});                                                  \
        auto result_column = vectorized::ColumnUInt8::create(N, 0);                         \
        try {                                                                               \
            set->find_batch(*column, N, result_column->get_data());                         \
            ASSERT_TRUE(false) << "should not be here";                                     \
        } catch (...) {                                                                     \
        }                                                                                   \
                                                                                            \
        for (size_t i = 0; i != N; ++i) {                                                   \
            set->insert(&i);                                                                \
        }                                                                                   \
                                                                                            \
        for (size_t i = 0; i != N; ++i) {                                                   \
            ASSERT_TRUE(set->find(&i));                                                     \
        }                                                                                   \
                                                                                            \
        for (size_t i = N; i != 1024; ++i) {                                                \
            ASSERT_FALSE(set->find(&i));                                                    \
        }                                                                                   \
                                                                                            \
        std::unique_ptr<HybridSetBase> set2(create_set<N>(PrimitiveType::TYPE_INT, false)); \
        set2->insert(set.get());                                                            \
                                                                                            \
        for (size_t i = 0; i != N; ++i) {                                                   \
            ASSERT_TRUE(set2->find(&i));                                                    \
        }                                                                                   \
                                                                                            \
        for (size_t i = N; i != 1024; ++i) {                                                \
            ASSERT_FALSE(set2->find(&i));                                                   \
        }                                                                                   \
                                                                                            \
        auto it = set->begin();                                                             \
        while (it->has_next()) {                                                            \
            auto value = *(int*)it->get_value();                                            \
            ASSERT_TRUE(set2->find(&value)) << "cannot find: " << value;                    \
            it->next();                                                                     \
        }                                                                                   \
        PInFilter in_filter;                                                                \
        set->to_pb(&in_filter);                                                             \
        set->clear();                                                                       \
        ASSERT_EQ(set->size(), 0);                                                          \
    }

TEST_F(HybridSetTest, FixedContainer) {
    TEST_FIXED_CONTAINER(1);
    TEST_FIXED_CONTAINER(2);
    TEST_FIXED_CONTAINER(3);
    TEST_FIXED_CONTAINER(4);
    TEST_FIXED_CONTAINER(5);
    TEST_FIXED_CONTAINER(6);
    TEST_FIXED_CONTAINER(7);
    TEST_FIXED_CONTAINER(8);

    std::unique_ptr<HybridSetBase> set(create_set<8>(PrimitiveType::TYPE_INT, false));
    auto column = vectorized::ColumnHelper::create_column<vectorized::DataTypeInt32>(
            {1, 2, 3, 4, 5, 6, 7, 8});
}

TEST_F(HybridSetTest, FindBatch) {
    std::unique_ptr<HybridSetBase> string_set(create_set(PrimitiveType::TYPE_VARCHAR, true));
    auto string_column = vectorized::ColumnHelper::create_column<vectorized::DataTypeString>(
            {"ab", "cd", "ef", "gh", "ij", "kl", "mn", "op"});
    auto nullmap_column = vectorized::ColumnUInt8::create(8, 0);

    auto nullable_column =
            vectorized::ColumnNullable::create(string_column->clone(), nullmap_column->clone());

    string_set->insert_fixed_len(nullable_column->clone(), 0);
    ASSERT_EQ(string_set->size(), nullable_column->size());

    nullmap_column->get_data()[1] = 1;
    nullmap_column->get_data()[3] = 1;
    nullmap_column->get_data()[6] = 1;
    auto nullable_column2 =
            vectorized::ColumnNullable::create(string_column->clone(), nullmap_column->clone());

    std::unique_ptr<HybridSetBase> string_set2(create_set(PrimitiveType::TYPE_VARCHAR, true));
    string_set2->insert_fixed_len(nullable_column2->clone(), 0);
    ASSERT_EQ(string_set2->size(), nullable_column2->size() - 3);
    ASSERT_TRUE(string_set2->contain_null());

    auto result_column = vectorized::ColumnUInt8::create(nullable_column2->size(), 0);
    string_set->find_batch(*string_column, string_column->size(), result_column->get_data());

    ASSERT_EQ(result_column->get_data()[0], 1);
    ASSERT_EQ(result_column->get_data()[1], 1);
    ASSERT_EQ(result_column->get_data()[2], 1);
    ASSERT_EQ(result_column->get_data()[3], 1);
    ASSERT_EQ(result_column->get_data()[4], 1);
    ASSERT_EQ(result_column->get_data()[5], 1);
    ASSERT_EQ(result_column->get_data()[6], 1);
    ASSERT_EQ(result_column->get_data()[7], 1);

    string_set->find_batch_negative(*string_column, string_column->size(),
                                    result_column->get_data());
    ASSERT_EQ(result_column->get_data()[0], 0);
    ASSERT_EQ(result_column->get_data()[1], 0);
    ASSERT_EQ(result_column->get_data()[2], 0);
    ASSERT_EQ(result_column->get_data()[3], 0);
    ASSERT_EQ(result_column->get_data()[4], 0);
    ASSERT_EQ(result_column->get_data()[5], 0);
    ASSERT_EQ(result_column->get_data()[6], 0);
    ASSERT_EQ(result_column->get_data()[7], 0);

    // Only bloom fitler need to handle nullaware(VRuntimeFilterWrapper::execute),
    // So HybridSet will return false when find null value.
    string_set2->find_batch_nullable(*string_column, string_column->size(),
                                     nullmap_column->get_data(), result_column->get_data());
    ASSERT_EQ(result_column->get_data()[0], 1);
    // null value always return false, no metter nullaware or not.
    ASSERT_EQ(result_column->get_data()[1], 0);
    ASSERT_EQ(result_column->get_data()[2], 1);
    ASSERT_EQ(result_column->get_data()[3], 0);
    ASSERT_EQ(result_column->get_data()[4], 1);
    ASSERT_EQ(result_column->get_data()[5], 1);
    ASSERT_EQ(result_column->get_data()[6], 0);
    ASSERT_EQ(result_column->get_data()[7], 1);

    string_set2->find_batch_nullable_negative(*string_column, string_column->size(),
                                              nullmap_column->get_data(),
                                              result_column->get_data());
    ASSERT_EQ(result_column->get_data()[0], 0);
    ASSERT_EQ(result_column->get_data()[1], 1);
    ASSERT_EQ(result_column->get_data()[2], 0);
    ASSERT_EQ(result_column->get_data()[3], 1);
    ASSERT_EQ(result_column->get_data()[4], 0);
    ASSERT_EQ(result_column->get_data()[5], 0);
    ASSERT_EQ(result_column->get_data()[6], 1);
    ASSERT_EQ(result_column->get_data()[7], 0);

    PInFilter in_filter;
    string_set2->to_pb(&in_filter);
    string_set2->clear();
}

TEST_F(HybridSetTest, StringValueSet) {
    auto test_string_value_set = [](size_t n) {
        std::unique_ptr<HybridSetBase> string_value_set(create_string_value_set(n, true));

        string_value_set->insert((const void*)(nullptr));
        ASSERT_TRUE(string_value_set->contain_null());

        StringRef refs[] = {StringRef("ab"), StringRef("cd"), StringRef("ef"), StringRef("gh"),
                            StringRef("ij"), StringRef("kl"), StringRef("mn"), StringRef("op"),
                            StringRef("qr"), StringRef("st"), StringRef("uv"), StringRef("wx")};
        for (size_t i = 0; i != n; ++i) {
            string_value_set->insert((const void*)&refs[i]);
        }

        for (size_t i = 0; i != 12; ++i) {
            ASSERT_EQ(string_value_set->find((const void*)&refs[i]), i < n);
        }

        StringRef tmp("abc");
        ASSERT_FALSE(string_value_set->find((const void*)&tmp));

        string_value_set->clear();

        const char* strings[] = {"ab", "cd", "ef", "gh", "ij", "kl",
                                 "mn", "op", "qr", "st", "uv", "wx"};
        for (size_t i = 0; i != n; ++i) {
            string_value_set->insert((void*)strings[i], strlen(strings[i]));
        }

        for (size_t i = 0; i != 12; ++i) {
            ASSERT_EQ(string_value_set->find((const void*)&refs[i]), i < n);
            ASSERT_EQ(string_value_set->find((const void*)strings[i], strlen(strings[i])), i < n);
        }
    };

    for (size_t i = 1; i != 12; ++i) {
        test_string_value_set(i);
    }

    vectorized::ColumnPtr string_column =
            vectorized::ColumnHelper::create_column<vectorized::DataTypeString>(
                    {"ab", "cd", "ef", "gh", "ij", "kl", "mn", "op", "qr", "st", "uv", "wx"});
    auto nullmap_column = vectorized::ColumnUInt8::create(12, 0);

    vectorized::ColumnPtr nullable_column =
            vectorized::ColumnNullable::create(string_column->clone(), nullmap_column->clone());

    std::unique_ptr<HybridSetBase> string_value_set(create_string_value_set(0, true));
    string_value_set->insert_fixed_len(nullable_column, 0);

    ASSERT_EQ(string_value_set->size(), nullable_column->size());

    auto results = vectorized::ColumnUInt8::create(string_column->size(), 0);
    string_value_set->find_batch(*string_column, string_column->size(), results->get_data());
    for (size_t i = 0; i != string_column->size(); ++i) {
        ASSERT_TRUE(results->get_data()[i]);
    }

    string_value_set->clear();
    ASSERT_EQ(string_value_set->size(), 0);

    nullmap_column->get_data()[1] = 1;
    nullmap_column->get_data()[3] = 1;
    nullmap_column->get_data()[6] = 1;
    auto nullable_column2 =
            vectorized::ColumnNullable::create(string_column, nullmap_column->clone());

    string_value_set->insert_fixed_len(nullable_column2->clone(), 0);
    ASSERT_EQ(string_value_set->size(), nullable_column2->size() - 3);

    string_value_set->find_batch(*string_column, string_column->size(), results->get_data());
    for (size_t i = 0; i != string_column->size(); ++i) {
        ASSERT_EQ(results->get_data()[i], i != 1 && i != 3 && i != 6);
    }

    // insert duplicated strings
    string_value_set->insert_fixed_len(nullable_column2->clone(), 0);
    ASSERT_EQ(string_value_set->size(), nullable_column2->size() - 3);

    string_value_set->find_batch(*string_column, string_column->size(), results->get_data());
    for (size_t i = 0; i != string_column->size(); ++i) {
        ASSERT_EQ(results->get_data()[i], i != 1 && i != 3 && i != 6);
    }

    // test ColumnStr64
    auto string_overflow_size = config::string_overflow_size;
    config::string_overflow_size = 10;
    Defer defer([string_overflow_size]() { config::string_overflow_size = string_overflow_size; });

    vectorized::ColumnPtr string64_column = string_column->clone()->convert_column_if_overflow();
    ASSERT_TRUE(string64_column->is_column_string64());

    string_value_set->clear();
    ASSERT_EQ(string_value_set->size(), 0);

    string_value_set->insert_fixed_len(string64_column, 0);
    ASSERT_EQ(string_value_set->size(), string64_column->size());

    string_value_set->find_batch(*string_column, string_column->size(), results->get_data());
    for (size_t i = 0; i != string_column->size(); ++i) {
        ASSERT_TRUE(results->get_data()[i]);
    }

    string_value_set->clear();
    ASSERT_EQ(string_value_set->size(), 0);

    vectorized::ColumnNullable::Ptr nullable_column3 =
            vectorized::ColumnNullable::create(string64_column->clone(), nullmap_column->clone());

    string_value_set->insert_fixed_len(nullable_column3, 0);
    ASSERT_EQ(string_value_set->size(), string64_column->size() - 3);

    string_value_set->find_batch(*string_column, string_column->size(), results->get_data());
    for (size_t i = 0; i != string_column->size(); ++i) {
        ASSERT_EQ(results->get_data()[i], i != 1 && i != 3 && i != 6);
    }

    string_value_set->find_batch_negative(*string_column, string_column->size(),
                                          results->get_data());
    for (size_t i = 0; i != string_column->size(); ++i) {
        ASSERT_EQ(results->get_data()[i], !(i != 1 && i != 3 && i != 6));
    }

    string_value_set->find_batch_nullable(*string_column, string_column->size(),
                                          nullable_column2->get_null_map_data(),
                                          results->get_data());
    for (size_t i = 0; i != string_column->size(); ++i) {
        ASSERT_EQ(results->get_data()[i], (i != 1 && i != 3 && i != 6));
    }

    string_value_set->find_batch_nullable_negative(*string_column, string_column->size(),
                                                   nullable_column2->get_null_map_data(),
                                                   results->get_data());
    for (size_t i = 0; i != string_column->size(); ++i) {
        ASSERT_EQ(results->get_data()[i], !(i != 1 && i != 3 && i != 6));
    }

    try {
        PInFilter in_filter;
        string_value_set->to_pb(&in_filter);
    } catch (...) {
    }
}

// Test BitSetContainer
TEST_F(HybridSetTest, BitSetContainerBasic) {
    BitSetContainer<int32_t> container;

    // Test init_bitset
    container.init_bitset(0, 100);

    // Test insert and find
    container.insert(0);
    container.insert(50);
    container.insert(100);

    EXPECT_EQ(container.size(), 3);

    EXPECT_TRUE(container.find(0));
    EXPECT_TRUE(container.find(50));
    EXPECT_TRUE(container.find(100));
    EXPECT_FALSE(container.find(1));
    EXPECT_FALSE(container.find(49));
    EXPECT_FALSE(container.find(99));

    // Test out of range find
    EXPECT_FALSE(container.find(-1));
    EXPECT_FALSE(container.find(101));

    // Test duplicate insert
    container.insert(50);
    EXPECT_EQ(container.size(), 3);

    // Test clear
    container.clear();
    EXPECT_EQ(container.size(), 0);
    EXPECT_FALSE(container.find(50));
}

TEST_F(HybridSetTest, BitSetContainerNegativeValues) {
    BitSetContainer<int32_t> container;

    // Test with negative range
    container.init_bitset(-100, 100);

    container.insert(-100);
    container.insert(-50);
    container.insert(0);
    container.insert(50);
    container.insert(100);

    EXPECT_EQ(container.size(), 5);

    EXPECT_TRUE(container.find(-100));
    EXPECT_TRUE(container.find(-50));
    EXPECT_TRUE(container.find(0));
    EXPECT_TRUE(container.find(50));
    EXPECT_TRUE(container.find(100));

    EXPECT_FALSE(container.find(-101));
    EXPECT_FALSE(container.find(101));
    EXPECT_FALSE(container.find(-1));
    EXPECT_FALSE(container.find(1));
}

TEST_F(HybridSetTest, BitSetContainerTypes) {
    // Test with int8_t
    {
        BitSetContainer<int8_t> container;
        container.init_bitset(-10, 10);
        container.insert(-10);
        container.insert(0);
        container.insert(10);
        EXPECT_EQ(container.size(), 3);
        EXPECT_TRUE(container.find(-10));
        EXPECT_TRUE(container.find(0));
        EXPECT_TRUE(container.find(10));
    }

    // Test with int16_t
    {
        BitSetContainer<int16_t> container;
        container.init_bitset(-1000, 1000);
        container.insert(-1000);
        container.insert(0);
        container.insert(1000);
        EXPECT_EQ(container.size(), 3);
        EXPECT_TRUE(container.find(-1000));
        EXPECT_TRUE(container.find(0));
        EXPECT_TRUE(container.find(1000));
    }

    // Test with int64_t
    {
        BitSetContainer<int64_t> container;
        container.init_bitset(-10000, 10000);
        container.insert(-10000);
        container.insert(0);
        container.insert(10000);
        EXPECT_EQ(container.size(), 3);
        EXPECT_TRUE(container.find(-10000));
        EXPECT_TRUE(container.find(0));
        EXPECT_TRUE(container.find(10000));
    }
}

// Test try_convert_to_bitset
TEST_F(HybridSetTest, TryConvertToBitsetNullState) {
    // Test with null state - should return nullptr
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_INT, false));

    int32_t values[] = {1, 2, 3, 4, 5};
    for (auto v : values) {
        set->insert(&v);
    }

    auto result = set->try_convert_to_bitset(nullptr);
    EXPECT_EQ(result, nullptr);
}

TEST_F(HybridSetTest, TryConvertToBitsetSuccess) {
    // Test successful conversion
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_INT, false));

    int32_t values[] = {1, 5, 10, 15, 20};
    for (auto v : values) {
        set->insert(&v);
    }

    // Create MockRuntimeState with appropriate settings
    MockRuntimeState state;
    TQueryOptions query_options;
    query_options.__set_in_set_to_bitset_max_size(100);
    query_options.__set_in_set_to_bitset_max_range(100);
    state.set_query_options(query_options);

    auto bitset_set = set->try_convert_to_bitset(&state);
    ASSERT_NE(bitset_set, nullptr);

    // Verify the converted set has the same elements
    EXPECT_EQ(bitset_set->size(), 5);
    for (auto v : values) {
        EXPECT_TRUE(bitset_set->find(&v));
    }

    // Verify elements not in the set
    int32_t not_in_set[] = {0, 2, 3, 4, 6, 7, 8, 9, 11, 100};
    for (auto v : not_in_set) {
        EXPECT_FALSE(bitset_set->find(&v));
    }
}

TEST_F(HybridSetTest, TryConvertToBitsetWithNullAware) {
    // Test conversion preserves null awareness
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_INT, true));

    int32_t values[] = {1, 2, 3};
    for (auto v : values) {
        set->insert(&v);
    }
    set->insert(static_cast<const void*>(nullptr)); // insert null

    MockRuntimeState state;
    TQueryOptions query_options;
    query_options.__set_in_set_to_bitset_max_size(100);
    query_options.__set_in_set_to_bitset_max_range(100);
    state.set_query_options(query_options);

    auto bitset_set = set->try_convert_to_bitset(&state);
    ASSERT_NE(bitset_set, nullptr);

    EXPECT_EQ(bitset_set->size(), 3);
    EXPECT_TRUE(bitset_set->contain_null());

    for (auto v : values) {
        EXPECT_TRUE(bitset_set->find(&v));
    }
}

TEST_F(HybridSetTest, TryConvertToBitsetTooLargeSize) {
    // Test when set size exceeds max_bitset_size
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_INT, false));

    for (int32_t i = 0; i < 20; ++i) {
        set->insert(&i);
    }

    MockRuntimeState state;
    TQueryOptions query_options;
    query_options.__set_in_set_to_bitset_max_size(10); // max size is 10, but set has 20 elements
    query_options.__set_in_set_to_bitset_max_range(100);
    state.set_query_options(query_options);

    auto result = set->try_convert_to_bitset(&state);
    EXPECT_EQ(result, nullptr);
}

TEST_F(HybridSetTest, TryConvertToBitsetTooLargeRange) {
    // Test when value range exceeds max_range
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_INT, false));

    int32_t values[] = {0, 1000}; // range is 1000
    for (auto v : values) {
        set->insert(&v);
    }

    MockRuntimeState state;
    TQueryOptions query_options;
    query_options.__set_in_set_to_bitset_max_size(100);
    query_options.__set_in_set_to_bitset_max_range(
            500); // max range is 500, but actual range is 1000
    state.set_query_options(query_options);

    auto result = set->try_convert_to_bitset(&state);
    EXPECT_EQ(result, nullptr);
}

TEST_F(HybridSetTest, TryConvertToBitsetEmptySet) {
    // Test with empty set
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_INT, false));

    MockRuntimeState state;
    TQueryOptions query_options;
    query_options.__set_in_set_to_bitset_max_size(100);
    query_options.__set_in_set_to_bitset_max_range(100);
    state.set_query_options(query_options);

    auto result = set->try_convert_to_bitset(&state);
    EXPECT_EQ(result, nullptr);
}

TEST_F(HybridSetTest, TryConvertToBitsetDifferentIntTypes) {
    // Test with TINYINT
    {
        std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_TINYINT, false));

        int8_t values[] = {-10, 0, 10, 20};
        for (auto v : values) {
            set->insert(&v);
        }

        MockRuntimeState state;
        TQueryOptions query_options;
        query_options.__set_in_set_to_bitset_max_size(100);
        query_options.__set_in_set_to_bitset_max_range(100);
        state.set_query_options(query_options);

        auto bitset_set = set->try_convert_to_bitset(&state);
        ASSERT_NE(bitset_set, nullptr);
        EXPECT_EQ(bitset_set->size(), 4);

        for (auto v : values) {
            EXPECT_TRUE(bitset_set->find(&v));
        }
    }

    // Test with SMALLINT
    {
        std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_SMALLINT, false));

        int16_t values[] = {-100, 0, 100, 200};
        for (auto v : values) {
            set->insert(&v);
        }

        MockRuntimeState state;
        TQueryOptions query_options;
        query_options.__set_in_set_to_bitset_max_size(100);
        query_options.__set_in_set_to_bitset_max_range(500);
        state.set_query_options(query_options);

        auto bitset_set = set->try_convert_to_bitset(&state);
        ASSERT_NE(bitset_set, nullptr);
        EXPECT_EQ(bitset_set->size(), 4);

        for (auto v : values) {
            EXPECT_TRUE(bitset_set->find(&v));
        }
    }

    // Test with BIGINT
    {
        std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_BIGINT, false));

        int64_t values[] = {-1000, 0, 1000, 2000};
        for (auto v : values) {
            set->insert(&v);
        }

        MockRuntimeState state;
        TQueryOptions query_options;
        query_options.__set_in_set_to_bitset_max_size(100);
        query_options.__set_in_set_to_bitset_max_range(5000);
        state.set_query_options(query_options);

        auto bitset_set = set->try_convert_to_bitset(&state);
        ASSERT_NE(bitset_set, nullptr);
        EXPECT_EQ(bitset_set->size(), 4);

        for (auto v : values) {
            EXPECT_TRUE(bitset_set->find(&v));
        }
    }
}

TEST_F(HybridSetTest, TryConvertToBitsetNonIntegerTypes) {
    // Test with STRING - should return nullptr
    {
        std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_STRING, false));

        std::string s1 = "test1";
        std::string s2 = "test2";
        set->insert(&s1);
        set->insert(&s2);

        MockRuntimeState state;
        TQueryOptions query_options;
        query_options.__set_in_set_to_bitset_max_size(100);
        query_options.__set_in_set_to_bitset_max_range(100);
        state.set_query_options(query_options);

        auto result = set->try_convert_to_bitset(&state);
        EXPECT_EQ(result, nullptr);
    }

    // Test with FLOAT - should return nullptr
    {
        std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_FLOAT, false));

        float values[] = {1.0F, 2.0F, 3.0F};
        for (auto v : values) {
            set->insert(&v);
        }

        MockRuntimeState state;
        TQueryOptions query_options;
        query_options.__set_in_set_to_bitset_max_size(100);
        query_options.__set_in_set_to_bitset_max_range(100);
        state.set_query_options(query_options);

        auto result = set->try_convert_to_bitset(&state);
        EXPECT_EQ(result, nullptr);
    }

    // Test with DOUBLE - should return nullptr
    {
        std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_DOUBLE, false));

        double values[] = {1.0, 2.0, 3.0};
        for (auto v : values) {
            set->insert(&v);
        }

        MockRuntimeState state;
        TQueryOptions query_options;
        query_options.__set_in_set_to_bitset_max_size(100);
        query_options.__set_in_set_to_bitset_max_range(100);
        state.set_query_options(query_options);

        auto result = set->try_convert_to_bitset(&state);
        EXPECT_EQ(result, nullptr);
    }
}

TEST_F(HybridSetTest, TryConvertToBitsetFindBatch) {
    // Test find_batch on converted bitset
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_INT, false));

    int32_t values[] = {1, 3, 5, 7, 9};
    for (auto v : values) {
        set->insert(&v);
    }

    MockRuntimeState state;
    TQueryOptions query_options;
    query_options.__set_in_set_to_bitset_max_size(100);
    query_options.__set_in_set_to_bitset_max_range(100);
    state.set_query_options(query_options);

    auto bitset_set = set->try_convert_to_bitset(&state);
    ASSERT_NE(bitset_set, nullptr);

    // Create a column with test data
    auto column = vectorized::ColumnInt32::create();
    column->insert_value(0);
    column->insert_value(1);
    column->insert_value(2);
    column->insert_value(3);
    column->insert_value(4);
    column->insert_value(5);
    column->insert_value(6);
    column->insert_value(7);
    column->insert_value(8);
    column->insert_value(9);

    vectorized::ColumnUInt8::Container results(10, 0);
    bitset_set->find_batch(*column, 10, results);

    // Expected: 0->false, 1->true, 2->false, 3->true, 4->false, 5->true, 6->false, 7->true, 8->false, 9->true
    EXPECT_EQ(results[0], 0);
    EXPECT_EQ(results[1], 1);
    EXPECT_EQ(results[2], 0);
    EXPECT_EQ(results[3], 1);
    EXPECT_EQ(results[4], 0);
    EXPECT_EQ(results[5], 1);
    EXPECT_EQ(results[6], 0);
    EXPECT_EQ(results[7], 1);
    EXPECT_EQ(results[8], 0);
    EXPECT_EQ(results[9], 1);
}

TEST_F(HybridSetTest, TryConvertToBitsetFindBatchNegative) {
    // Test find_batch_negative on converted bitset
    std::unique_ptr<HybridSetBase> set(create_set(PrimitiveType::TYPE_INT, false));

    int32_t values[] = {1, 3, 5};
    for (auto v : values) {
        set->insert(&v);
    }

    MockRuntimeState state;
    TQueryOptions query_options;
    query_options.__set_in_set_to_bitset_max_size(100);
    query_options.__set_in_set_to_bitset_max_range(100);
    state.set_query_options(query_options);

    auto bitset_set = set->try_convert_to_bitset(&state);
    ASSERT_NE(bitset_set, nullptr);

    auto column = vectorized::ColumnInt32::create();
    column->insert_value(0);
    column->insert_value(1);
    column->insert_value(2);
    column->insert_value(3);
    column->insert_value(4);
    column->insert_value(5);

    vectorized::ColumnUInt8::Container results(6, 0);
    bitset_set->find_batch_negative(*column, 6, results);

    // Expected: NOT IN results
    // 0->true (not in set), 1->false (in set), 2->true, 3->false, 4->true, 5->false
    EXPECT_EQ(results[0], 1);
    EXPECT_EQ(results[1], 0);
    EXPECT_EQ(results[2], 1);
    EXPECT_EQ(results[3], 0);
    EXPECT_EQ(results[4], 1);
    EXPECT_EQ(results[5], 0);
}

TEST_F(HybridSetTest, BitSetContainerIteratorThrows) {
    BitSetContainer<int32_t> container;
    container.init_bitset(0, 10);

    // Test that begin() throws
    EXPECT_THROW(container.begin(), doris::Exception);

    // Test that end() throws
    EXPECT_THROW(container.end(), doris::Exception);

    // Test that insert by iterator throws
    std::vector<int32_t> values = {1, 2, 3};
    EXPECT_THROW(container.insert(values.begin(), values.end()), doris::Exception);
}

TEST_F(HybridSetTest, TryConvertToBitsetFixedContainer) {
    // Test with FixedContainer - should return nullptr
    std::unique_ptr<HybridSetBase> set(create_set<3>(PrimitiveType::TYPE_INT, false));

    int32_t values[] = {1, 2, 3};
    for (auto v : values) {
        set->insert(&v);
    }

    MockRuntimeState state;
    TQueryOptions query_options;
    query_options.__set_in_set_to_bitset_max_size(100);
    query_options.__set_in_set_to_bitset_max_range(100);
    state.set_query_options(query_options);

    auto result = set->try_convert_to_bitset(&state);
    EXPECT_EQ(result, nullptr);
}

} // namespace doris
