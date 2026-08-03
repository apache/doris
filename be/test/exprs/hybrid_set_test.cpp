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

#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "core/field.h"
#include "exprs/create_predicate_function.h"
#include "gtest/internal/gtest-internal.h"
#include "testutil/column_helper.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris {

static constexpr auto CONVERT_COLUMN_IF_OVERFLOW_DEBUG_POINT =
        "ColumnStr.convert_column_if_overflow.max_string_size";

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

TEST_F(HybridSetTest, IntegerMinMaxAndRangeLookup) {
    const auto field = [](int32_t value) { return Field::create_field<TYPE_INT>(value); };
    const auto verify = [&](HybridSetBase& set) {
        for (int32_t value : {1, 5, 9}) {
            set.insert(&value);
        }

        Field min_value;
        Field max_value;
        set.get_min_max(min_value, max_value);
        EXPECT_EQ(min_value.get<TYPE_INT>(), 1);
        EXPECT_EQ(max_value.get<TYPE_INT>(), 9);

        EXPECT_TRUE(set.contains_any_in_range(field(1), field(1)));
        EXPECT_TRUE(set.contains_any_in_range(field(4), field(5)));
        EXPECT_TRUE(set.contains_any_in_range(field(9), field(10)));
        EXPECT_FALSE(set.contains_any_in_range(field(2), field(4)));
        EXPECT_FALSE(set.contains_any_in_range(field(6), field(8)));

        set.clear();
        set.get_min_max(min_value, max_value);
        EXPECT_TRUE(min_value.is_null());
        EXPECT_TRUE(max_value.is_null());
    };

    HybridSet<TYPE_INT> dynamic_set(false);
    EXPECT_TRUE(dynamic_set.supports_fast_range_lookup());
    verify(dynamic_set);

    HybridSet<TYPE_INT, FixedContainer<int32_t, 3>> fixed_set(false);
    EXPECT_TRUE(fixed_set.supports_fast_range_lookup());
    verify(fixed_set);
}

TEST_F(HybridSetTest, SignedBitSetRangeLookup) {
    const auto tinyint_field = [](int8_t value) {
        return Field::create_field<TYPE_TINYINT>(value);
    };
    HybridSet<TYPE_TINYINT, BitSetContainer<int8_t>> tinyint_set(false);
    EXPECT_FALSE(tinyint_set.supports_fast_range_lookup());
    for (int8_t value : {int8_t {-100}, int8_t {-1}, int8_t {0}, int8_t {100}}) {
        tinyint_set.insert(&value);
    }
    EXPECT_TRUE(tinyint_set.contains_any_in_range(tinyint_field(-2), tinyint_field(1)));
    EXPECT_TRUE(tinyint_set.contains_any_in_range(tinyint_field(-100), tinyint_field(-100)));
    EXPECT_FALSE(tinyint_set.contains_any_in_range(tinyint_field(-99), tinyint_field(-2)));
    EXPECT_FALSE(tinyint_set.contains_any_in_range(tinyint_field(1), tinyint_field(99)));

    const auto smallint_field = [](int16_t value) {
        return Field::create_field<TYPE_SMALLINT>(value);
    };
    HybridSet<TYPE_SMALLINT, BitSetContainer<int16_t>> edge_set(false);
    int16_t min_value = std::numeric_limits<int16_t>::min();
    int16_t max_value = std::numeric_limits<int16_t>::max();
    edge_set.insert(&min_value);
    edge_set.insert(&max_value);

    Field min_field;
    Field max_field;
    edge_set.get_min_max(min_field, max_field);
    EXPECT_EQ(min_field.get<TYPE_SMALLINT>(), min_value);
    EXPECT_EQ(max_field.get<TYPE_SMALLINT>(), max_value);
    EXPECT_TRUE(
            edge_set.contains_any_in_range(smallint_field(min_value), smallint_field(min_value)));
    EXPECT_TRUE(
            edge_set.contains_any_in_range(smallint_field(max_value), smallint_field(max_value)));
    EXPECT_FALSE(
            edge_set.contains_any_in_range(smallint_field(static_cast<int16_t>(min_value + 1)),
                                           smallint_field(static_cast<int16_t>(max_value - 1))));

    HybridSet<TYPE_SMALLINT, BitSetContainer<int16_t>> crossing_set(false);
    for (int16_t value : {int16_t {-30000}, int16_t {-1}, int16_t {0}, int16_t {30000}}) {
        crossing_set.insert(&value);
    }
    EXPECT_TRUE(crossing_set.contains_any_in_range(smallint_field(-2), smallint_field(1)));
    EXPECT_TRUE(crossing_set.contains_any_in_range(smallint_field(-1), smallint_field(-1)));
    EXPECT_TRUE(crossing_set.contains_any_in_range(smallint_field(0), smallint_field(0)));
    EXPECT_FALSE(crossing_set.contains_any_in_range(smallint_field(-29999), smallint_field(-2)));
    EXPECT_FALSE(crossing_set.contains_any_in_range(smallint_field(1), smallint_field(29999)));
}

TEST_F(HybridSetTest, StringRangeLookupPreservesEmbeddedNull) {
    const std::array<std::string, 3> values = {std::string("a\0a", 3), std::string("a\0c", 3),
                                               std::string("b\0b", 3)};
    const std::string missing("a\0b", 3);
    const std::string upper_hole("b\0a", 3);
    const auto field = [](const std::string& value) {
        return Field::create_field<TYPE_STRING>(String(value.data(), value.size()));
    };
    const auto verify = [&](HybridSetBase& set) {
        Field min_value;
        Field max_value;
        set.get_min_max(min_value, max_value);
        EXPECT_EQ(min_value.get<TYPE_STRING>(), values.front());
        EXPECT_EQ(max_value.get<TYPE_STRING>(), values.back());

        EXPECT_TRUE(set.contains_any_in_range(field(values.front()), field(values.front())));
        EXPECT_TRUE(set.contains_any_in_range(field(missing), field(values[1])));
        EXPECT_FALSE(set.contains_any_in_range(field(missing), field(missing)));
        EXPECT_FALSE(set.contains_any_in_range(field(std::string("a\0d", 3)), field(upper_hole)));
    };

    StringSet<> owning_set(false);
    for (const auto& value : values) {
        StringRef ref(value);
        owning_set.insert(&ref);
    }
    verify(owning_set);

    StringSet<FixedContainer<std::string, 3>> fixed_owning_set(false);
    for (const auto& value : values) {
        StringRef ref(value);
        fixed_owning_set.insert(&ref);
    }
    verify(fixed_owning_set);

    StringValueSet<> borrowed_set(false);
    for (const auto& value : values) {
        StringRef ref(value);
        borrowed_set.insert(&ref);
    }
    verify(borrowed_set);
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

TEST_F(HybridSetTest, DynamicFloatingSetFindsDorisEqualNanPayload) {
    const auto check_type = []<PrimitiveType Type, typename UInt>(UInt stored_bits,
                                                                  UInt probe_bits) {
        using T = typename PrimitiveTypeTraits<Type>::CppType;
        std::unique_ptr<HybridSetBase> set(create_set(Type, false));
        for (int value = 0; value < FIXED_CONTAINER_MAX_SIZE; ++value) {
            T finite = static_cast<T>(value);
            set->insert(&finite);
        }
        const T stored_nan = std::bit_cast<T>(stored_bits);
        set->insert(&stored_nan);
        ASSERT_EQ(FIXED_CONTAINER_MAX_SIZE + 1, set->size());
        EXPECT_TRUE(set->contains_nan());

        Field min_value;
        Field max_value;
        set->get_min_max(min_value, max_value);
        EXPECT_EQ(T {0}, min_value.get<Type>());
        EXPECT_EQ(T {FIXED_CONTAINER_MAX_SIZE - 1}, max_value.get<Type>());

        const T probe_nan = std::bit_cast<T>(probe_bits);
        EXPECT_TRUE(set->find(&probe_nan));
        uint8_t match = 1;
        set->find_batch_raw_fixed(reinterpret_cast<const uint8_t*>(&probe_nan), 1, sizeof(T),
                                  &match);
        EXPECT_EQ(1, match);
    };

    check_type.template operator()<TYPE_FLOAT>(uint32_t {0x7fc00001U}, uint32_t {0x7fc00002U});
    check_type.template operator()<TYPE_DOUBLE>(uint64_t {0x7ff8000000000001ULL},
                                                uint64_t {0x7ff8000000000002ULL});
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
        auto column = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5, 6, 7, 8}); \
        auto result_column = ColumnUInt8::create(N, 0);                                     \
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
    auto column = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3, 4, 5, 6, 7, 8});
}

TEST_F(HybridSetTest, FindBatch) {
    std::unique_ptr<HybridSetBase> string_set(create_set(PrimitiveType::TYPE_VARCHAR, true));
    auto string_column = ColumnHelper::create_column<DataTypeString>(
            {"ab", "cd", "ef", "gh", "ij", "kl", "mn", "op"});
    auto nullmap_column = ColumnUInt8::create(8, 0);

    auto nullable_column = ColumnNullable::create(string_column->clone(), nullmap_column->clone());

    string_set->insert_fixed_len(nullable_column->clone(), 0);
    ASSERT_EQ(string_set->size(), nullable_column->size());

    nullmap_column->get_data()[1] = 1;
    nullmap_column->get_data()[3] = 1;
    nullmap_column->get_data()[6] = 1;
    auto nullable_column2 = ColumnNullable::create(string_column->clone(), nullmap_column->clone());

    std::unique_ptr<HybridSetBase> string_set2(create_set(PrimitiveType::TYPE_VARCHAR, true));
    string_set2->insert_fixed_len(nullable_column2->clone(), 0);
    ASSERT_EQ(string_set2->size(), nullable_column2->size() - 3);
    ASSERT_TRUE(string_set2->contain_null());

    auto result_column = ColumnUInt8::create(nullable_column2->size(), 0);
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

    // Only bloom fitler need to handle nullaware(RuntimeFilterExpr::execute),
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

    ColumnPtr string_column = ColumnHelper::create_column<DataTypeString>(
            {"ab", "cd", "ef", "gh", "ij", "kl", "mn", "op", "qr", "st", "uv", "wx"});
    auto nullmap_column = ColumnUInt8::create(12, 0);

    ColumnPtr nullable_column =
            ColumnNullable::create(string_column->clone(), nullmap_column->clone());

    std::unique_ptr<HybridSetBase> string_value_set(create_string_value_set(0, true));
    string_value_set->insert_fixed_len(nullable_column, 0);

    ASSERT_EQ(string_value_set->size(), nullable_column->size());

    auto results = ColumnUInt8::create(string_column->size(), 0);
    string_value_set->find_batch(*string_column, string_column->size(), results->get_data());
    for (size_t i = 0; i != string_column->size(); ++i) {
        ASSERT_TRUE(results->get_data()[i]);
    }

    string_value_set->clear();
    ASSERT_EQ(string_value_set->size(), 0);

    nullmap_column->get_data()[1] = 1;
    nullmap_column->get_data()[3] = 1;
    nullmap_column->get_data()[6] = 1;
    auto nullable_column2 = ColumnNullable::create(string_column, nullmap_column->clone());

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
    auto origin_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;
    DebugPoints::instance()->add_with_params(CONVERT_COLUMN_IF_OVERFLOW_DEBUG_POINT,
                                             {{"max_string_size", "10"}});
    Defer defer([origin_enable_debug_points]() {
        DebugPoints::instance()->remove(CONVERT_COLUMN_IF_OVERFLOW_DEBUG_POINT);
        config::enable_debug_points = origin_enable_debug_points;
    });

    ColumnPtr string64_column = string_column->clone()->convert_column_if_overflow();
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

    ColumnNullable::Ptr nullable_column3 =
            ColumnNullable::create(string64_column->clone(), nullmap_column->clone());

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

} // namespace doris
