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

#include "storage/predicate/predicate_creator.h"

#include <gtest/gtest.h>

#include <initializer_list>
#include <memory>
#include <numeric>
#include <string_view>
#include <utility>
#include <vector>

#include "core/column/column_vector.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/value/timestamp_ns_value.h"
#include "exec/runtime_filter/runtime_filter_definitions.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/create_predicate_function.h"
#include "storage/predicate/bloom_filter_predicate.h"
#include "storage/predicate/comparison_predicate.h"

namespace doris {
namespace {

constexpr uint32_t kColumnId = 7;
constexpr std::string_view kColumnName = "timestamp_ns_column";

ColumnPtr timestamp_ns_column(std::initializer_list<int64_t> epoch_nanos) {
    auto column = ColumnTimeStampNs::create();
    for (const int64_t value : epoch_nanos) {
        column->insert_value(TimeStampNsValue(value));
    }
    return std::move(column);
}

std::vector<uint16_t> evaluate(const ColumnPredicate& predicate, const IColumn& column) {
    std::vector<uint16_t> selector(column.size());
    std::iota(selector.begin(), selector.end(), 0);
    const uint16_t selected_size =
            predicate.evaluate(column, selector.data(), static_cast<uint16_t>(selector.size()));
    selector.resize(selected_size);
    return selector;
}

template <PredicateType PT>
void expect_comparison_result(const std::vector<uint16_t>& expected) {
    auto data_type = std::make_shared<DataTypeTimeStampNs>();
    auto predicate = create_comparison_predicate<PT>(
            kColumnId, std::string(kColumnName), data_type,
            Field::create_field<TYPE_TIMESTAMP_NS>(TimeStampNsValue(0)), false);

    ASSERT_NE(predicate, nullptr);
    auto* typed_predicate =
            dynamic_cast<ComparisonPredicateBase<TYPE_TIMESTAMP_NS, PT>*>(predicate.get());
    EXPECT_NE(typed_predicate, nullptr);
    EXPECT_EQ(predicate->type(), PT);
    EXPECT_EQ(predicate->primitive_type(), TYPE_TIMESTAMP_NS);
    EXPECT_EQ(predicate->column_id(), kColumnId);
    EXPECT_EQ(predicate->col_name(), kColumnName);

    auto column = timestamp_ns_column({-2, -1, 0, 1, 2});
    EXPECT_EQ(evaluate(*predicate, *column), expected);
}

TEST(PredicateCreatorTest, TimestampNsComparisonFactories) {
    expect_comparison_result<PredicateType::EQ>({2});
    expect_comparison_result<PredicateType::NE>({0, 1, 3, 4});
    expect_comparison_result<PredicateType::LT>({0, 1});
    expect_comparison_result<PredicateType::LE>({0, 1, 2});
    expect_comparison_result<PredicateType::GT>({3, 4});
    expect_comparison_result<PredicateType::GE>({2, 3, 4});
}

TEST(PredicateCreatorTest, TimestampNsInAndNotInFactories) {
    auto data_type = std::make_shared<DataTypeTimeStampNs>();
    auto set = build_set<TYPE_TIMESTAMP_NS>();
    const TimeStampNsValue before_epoch(-1);
    const TimeStampNsValue after_epoch(1);
    set->insert(&before_epoch);
    set->insert(&after_epoch);
    auto column = timestamp_ns_column({-2, -1, 0, 1, 2});

    auto in_predicate = create_in_list_predicate<PredicateType::IN_LIST>(
            kColumnId, std::string(kColumnName), data_type, set, false);
    ASSERT_NE(in_predicate, nullptr);
    EXPECT_EQ(in_predicate->type(), PredicateType::IN_LIST);
    EXPECT_EQ(in_predicate->primitive_type(), TYPE_TIMESTAMP_NS);
    EXPECT_EQ(evaluate(*in_predicate, *column), (std::vector<uint16_t> {1, 3}));

    auto not_in_predicate = create_in_list_predicate<PredicateType::NOT_IN_LIST>(
            kColumnId, std::string(kColumnName), data_type, set, false);
    ASSERT_NE(not_in_predicate, nullptr);
    EXPECT_EQ(not_in_predicate->type(), PredicateType::NOT_IN_LIST);
    EXPECT_EQ(not_in_predicate->primitive_type(), TYPE_TIMESTAMP_NS);
    EXPECT_EQ(evaluate(*not_in_predicate, *column), (std::vector<uint16_t> {0, 2, 4}));
}

TEST(PredicateCreatorTest, TimestampNsBloomFilterFactory) {
    auto filter =
            std::shared_ptr<BloomFilterFuncBase>(create_bloom_filter(TYPE_TIMESTAMP_NS, false));
    RuntimeFilterParams params {.column_return_type = TYPE_TIMESTAMP_NS, .bloom_filter_size = 64};
    filter->init_params(&params);
    ASSERT_TRUE(filter->init_with_fixed_length(3).ok());

    auto members = timestamp_ns_column({-1, 0, 1});
    filter->insert_fixed_len(members, 0);

    auto predicate = create_bloom_filter_predicate(kColumnId, std::string(kColumnName),
                                                   std::make_shared<DataTypeTimeStampNs>(), filter);
    ASSERT_NE(predicate, nullptr);
    EXPECT_NE(dynamic_cast<BloomFilterColumnPredicate<TYPE_TIMESTAMP_NS>*>(predicate.get()),
              nullptr);
    EXPECT_EQ(predicate->type(), PredicateType::BF);
    EXPECT_EQ(predicate->primitive_type(), TYPE_TIMESTAMP_NS);
    EXPECT_EQ(evaluate(*predicate, *members), (std::vector<uint16_t> {0, 1, 2}));
}

} // namespace
} // namespace doris
