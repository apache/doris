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

#include <algorithm>
#include <initializer_list>
#include <limits>
#include <memory>
#include <string>

#include "common/config.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/bitset_container.h"
#include "storage/predicate/predicate_creator.h"

namespace doris {
namespace {

template <PrimitiveType Type>
segment_v2::ZoneMap make_zone_map(const typename PrimitiveTypeTraits<Type>::CppType& min,
                                  const typename PrimitiveTypeTraits<Type>::CppType& max) {
    segment_v2::ZoneMap zone_map;
    zone_map.has_not_null = true;
    zone_map.min_value = Field::create_field<Type>(min);
    zone_map.max_value = Field::create_field<Type>(max);
    return zone_map;
}

template <PrimitiveType Type>
std::shared_ptr<ColumnPredicate> make_in_predicate(
        std::initializer_list<typename PrimitiveTypeTraits<Type>::CppType> values,
        const DataTypePtr& data_type) {
    auto set = build_set<Type>();
    for (const auto& value : values) {
        if constexpr (is_string_type(Type)) {
            const StringRef ref(value);
            set->insert(&ref);
        } else {
            set->insert(&value);
        }
    }
    return create_in_list_predicate<PredicateType::IN_LIST>(0, "c", data_type, set, false);
}

} // namespace

class InListPredicateZoneMapTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_threshold = config::in_zonemap_point_check_threshold;
        config::in_zonemap_point_check_threshold = 8192;
    }

    void TearDown() override { config::in_zonemap_point_check_threshold = _saved_threshold; }

private:
    int32_t _saved_threshold = 0;
};

TEST_F(InListPredicateZoneMapTest, SparseIntegerSet) {
    auto predicate = make_in_predicate<TYPE_INT>({10, 30}, std::make_shared<DataTypeInt32>());
    EXPECT_FALSE(predicate->evaluate_and(make_zone_map<TYPE_INT>(11, 12)));
    EXPECT_FALSE(predicate->evaluate_and(make_zone_map<TYPE_INT>(11, 29)));
    EXPECT_FALSE(predicate->evaluate_and(make_zone_map<TYPE_INT>(0, 9)));
    EXPECT_FALSE(predicate->evaluate_and(make_zone_map<TYPE_INT>(31, 40)));
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(10, 10)));
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(30, 30)));
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(0, 10)));
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(29, 40)));
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(0, 40)));
}

TEST_F(InListPredicateZoneMapTest, FixedIntegerSet) {
    auto set = std::make_shared<HybridSet<TYPE_INT, FixedContainer<int32_t, 2>>>(false);
    for (int32_t value : {10, 30}) {
        set->insert(&value);
    }
    auto predicate = create_in_list_predicate<PredicateType::IN_LIST>(
            0, "c", std::make_shared<DataTypeInt32>(), set, false);
    EXPECT_FALSE(predicate->evaluate_and(make_zone_map<TYPE_INT>(11, 29)));
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(10, 10)));
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(30, 30)));
}

TEST_F(InListPredicateZoneMapTest, IntegerRangesMatchSetIntersection) {
    const std::initializer_list<int32_t> values {-5, 0, 9, 30};
    auto predicate = make_in_predicate<TYPE_INT>(values, std::make_shared<DataTypeInt32>());
    for (int32_t min = -6; min <= 31; ++min) {
        for (int32_t max = min; max <= 31; ++max) {
            const bool intersects = std::ranges::any_of(
                    values, [&](int32_t value) { return min <= value && value <= max; });
            EXPECT_EQ(intersects, predicate->evaluate_and(make_zone_map<TYPE_INT>(min, max)))
                    << min << ", " << max;
        }
    }
}

TEST_F(InListPredicateZoneMapTest, NullAndEmptySets) {
    auto predicate = make_in_predicate<TYPE_INT>({10, 30}, std::make_shared<DataTypeInt32>());
    segment_v2::ZoneMap zone_map;
    EXPECT_FALSE(predicate->evaluate_and(zone_map));
    zone_map.has_null = true;
    EXPECT_FALSE(predicate->evaluate_and(zone_map));
    zone_map = make_zone_map<TYPE_INT>(11, 29);
    zone_map.has_null = true;
    EXPECT_FALSE(predicate->evaluate_and(zone_map));
    zone_map.max_value = Field::create_field<TYPE_INT>(30);
    EXPECT_TRUE(predicate->evaluate_and(zone_map));

    auto set = build_set<TYPE_INT>();
    for (bool contains_null : {false, true}) {
        if (contains_null) {
            set->insert(static_cast<const void*>(nullptr));
        }
        auto empty_predicate = create_in_list_predicate<PredicateType::IN_LIST>(
                0, "c", std::make_shared<DataTypeInt32>(), set, false);
        EXPECT_FALSE(empty_predicate->evaluate_and(make_zone_map<TYPE_INT>(0, 100)));
    }
}

TEST_F(InListPredicateZoneMapTest, NegativePredicatesRemainConservative) {
    auto set = build_set<TYPE_INT>();
    for (int32_t value : {10, 30}) {
        set->insert(&value);
    }
    auto not_in = create_in_list_predicate<PredicateType::NOT_IN_LIST>(
            0, "c", std::make_shared<DataTypeInt32>(), set, false);
    auto opposite_in = create_in_list_predicate<PredicateType::IN_LIST>(
            0, "c", std::make_shared<DataTypeInt32>(), set, true);
    for (const auto& predicate : {not_in, opposite_in}) {
        EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(11, 29)));
        EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(31, 40)));
        EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(10, 10)));
    }
}

TEST_F(InListPredicateZoneMapTest, ExactCheckThreshold) {
    auto predicate = make_in_predicate<TYPE_INT>({10, 30}, std::make_shared<DataTypeInt32>());
    config::in_zonemap_point_check_threshold = 2;
    EXPECT_FALSE(predicate->evaluate_and(make_zone_map<TYPE_INT>(11, 29)));
    config::in_zonemap_point_check_threshold = 1;
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(11, 29)));
    EXPECT_FALSE(predicate->evaluate_and(make_zone_map<TYPE_INT>(31, 40)));
    config::in_zonemap_point_check_threshold = 0;
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_INT>(11, 29)));
    EXPECT_FALSE(predicate->evaluate_and(make_zone_map<TYPE_INT>(0, 9)));
}

TEST_F(InListPredicateZoneMapTest, DenseDomainSetUsesBoundsOnly) {
    auto set = std::make_shared<HybridSet<TYPE_SMALLINT, BitSetContainer<int16_t>>>(false);
    for (int16_t value : {10, 30}) {
        set->insert(&value);
    }
    auto predicate = create_in_list_predicate<PredicateType::IN_LIST>(
            0, "c", std::make_shared<DataTypeInt16>(), set, false);
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_SMALLINT>(11, 29)));
    EXPECT_FALSE(predicate->evaluate_and(make_zone_map<TYPE_SMALLINT>(31, 40)));
}

TEST_F(InListPredicateZoneMapTest, StringSetsPreserveBinaryOrdering) {
    auto predicate = make_in_predicate<TYPE_STRING>(
            {std::string("a\0a", 3), std::string("a\0z", 3)}, std::make_shared<DataTypeString>());
    EXPECT_FALSE(predicate->evaluate_and(
            make_zone_map<TYPE_STRING>(std::string("a\0b", 3), std::string("a\0y", 3))));
    EXPECT_TRUE(predicate->evaluate_and(
            make_zone_map<TYPE_STRING>(std::string("a\0a", 3), std::string("a\0a", 3))));
    auto clone = predicate->clone(1);
    predicate.reset();
    EXPECT_FALSE(clone->evaluate_and(
            make_zone_map<TYPE_STRING>(std::string("a\0b", 3), std::string("a\0y", 3))));
    EXPECT_TRUE(clone->evaluate_and(
            make_zone_map<TYPE_STRING>(std::string("a\0y", 3), std::string("a\0z", 3))));

    auto dynamic = make_in_predicate<TYPE_STRING>({"", "a", "c", "e", "g", "i", "k", "m", "o"},
                                                  std::make_shared<DataTypeString>());
    EXPECT_FALSE(dynamic->evaluate_and(make_zone_map<TYPE_STRING>("b", "b")));
    EXPECT_TRUE(dynamic->evaluate_and(make_zone_map<TYPE_STRING>("", "")));
    EXPECT_TRUE(dynamic->evaluate_and(make_zone_map<TYPE_STRING>("n", "o")));
}

TEST_F(InListPredicateZoneMapTest, DecimalSet) {
    auto predicate = make_in_predicate<TYPE_DECIMAL64>({Decimal64(-1234), Decimal64(5678)},
                                                       std::make_shared<DataTypeDecimal64>(18, 2));
    EXPECT_FALSE(predicate->evaluate_and(
            make_zone_map<TYPE_DECIMAL64>(Decimal64(-1000), Decimal64(5000))));
    EXPECT_TRUE(predicate->evaluate_and(
            make_zone_map<TYPE_DECIMAL64>(Decimal64(-1234), Decimal64(-1234))));
    EXPECT_TRUE(predicate->evaluate_and(
            make_zone_map<TYPE_DECIMAL64>(Decimal64(5000), Decimal64(5678))));
}

TEST_F(InListPredicateZoneMapTest, DatetimeSet) {
    DateV2Value<DateTimeV2ValueType> low;
    low.unchecked_set_time(2024, 1, 1, 0, 0, 0, 123456);
    DateV2Value<DateTimeV2ValueType> high;
    high.unchecked_set_time(2024, 1, 1, 0, 0, 0, 654321);
    DateV2Value<DateTimeV2ValueType> middle;
    middle.unchecked_set_time(2024, 1, 1, 0, 0, 0, 500000);
    auto predicate = make_in_predicate<TYPE_DATETIMEV2>({low, high},
                                                        std::make_shared<DataTypeDateTimeV2>(6));
    EXPECT_FALSE(predicate->evaluate_and(make_zone_map<TYPE_DATETIMEV2>(middle, middle)));
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_DATETIMEV2>(low, low)));
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_DATETIMEV2>(middle, high)));
}

TEST_F(InListPredicateZoneMapTest, FloatingPointSet) {
    auto predicate =
            make_in_predicate<TYPE_DOUBLE>({10.0, 30.0}, std::make_shared<DataTypeFloat64>());
    EXPECT_FALSE(predicate->evaluate_and(make_zone_map<TYPE_DOUBLE>(11.0, 29.0)));
    EXPECT_TRUE(predicate->evaluate_and(make_zone_map<TYPE_DOUBLE>(30.0, 30.0)));

    auto with_nan =
            make_in_predicate<TYPE_DOUBLE>({10.0, 30.0, std::numeric_limits<double>::quiet_NaN()},
                                           std::make_shared<DataTypeFloat64>());
    auto zone_map = make_zone_map<TYPE_DOUBLE>(11.0, 29.0);
    zone_map.has_nan = true;
    EXPECT_TRUE(with_nan->evaluate_and(zone_map));
    const auto nan = std::numeric_limits<double>::quiet_NaN();
    auto only_nan = make_in_predicate<TYPE_DOUBLE>({nan}, std::make_shared<DataTypeFloat64>());
    EXPECT_TRUE(only_nan->evaluate_and(make_zone_map<TYPE_DOUBLE>(nan, nan)));

    auto zero = make_in_predicate<TYPE_FLOAT>({-0.0F, 1.0F}, std::make_shared<DataTypeFloat32>());
    EXPECT_TRUE(zero->evaluate_and(make_zone_map<TYPE_FLOAT>(0.0F, 0.0F)));
    const auto inf = std::numeric_limits<double>::infinity();
    auto infinities =
            make_in_predicate<TYPE_DOUBLE>({-inf, inf}, std::make_shared<DataTypeFloat64>());
    EXPECT_FALSE(infinities->evaluate_and(make_zone_map<TYPE_DOUBLE>(0.0, 0.0)));
    EXPECT_TRUE(infinities->evaluate_and(make_zone_map<TYPE_DOUBLE>(inf, inf)));
}

} // namespace doris
