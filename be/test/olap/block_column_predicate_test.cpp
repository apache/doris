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

#include "olap/block_column_predicate.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <boost/iterator/iterator_facade.hpp>
#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exprs/hybrid_set.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/column_predicate.h"
#include "olap/comparison_predicate.h"
#include "olap/in_list_predicate.h"
#include "olap/null_predicate.h"
#include "olap/tablet_schema.h"
#include "runtime/define_primitive_type.h"
#include "runtime/type_limit.h"
#include "vec/columns/column.h"
#include "vec/columns/predicate_column.h"
#include "vec/core/field.h"
#include "vec/exec/format/parquet/parquet_block_split_bloom_filter.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/runtime/timestamptz_value.h"

namespace doris {

class BlockColumnPredicateTest : public testing::Test {
public:
    BlockColumnPredicateTest() = default;

    ~BlockColumnPredicateTest() = default;

    void SetTabletSchema(std::string name, const std::string& type, const std::string& aggregation,
                         uint32_t length, bool is_allow_null, bool is_key,
                         TabletSchemaSPtr tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        static int id = 0;
        ColumnPB* column = tablet_schema_pb.add_column();
        column->set_unique_id(++id);
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(is_key);
        column->set_is_nullable(is_allow_null);
        column->set_length(length);
        column->set_aggregation(aggregation);
        column->set_precision(1000);
        column->set_frac(1000);
        column->set_is_bf_column(false);
        tablet_schema->init_from_pb(tablet_schema_pb);
    }
};

TEST_F(BlockColumnPredicateTest, SINGLE_COLUMN_VEC) {
    vectorized::MutableColumns block;
    block.push_back(vectorized::PredicateColumnType<TYPE_INT>::create());

    auto value = vectorized::Field::create_field<TYPE_INT>(5);
    int rows = 10;
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, "", value));
    SingleColumnBlockPredicate single_column_block_pred(pred);

    std::vector<uint16_t> sel_idx(rows);
    uint16_t selected_size = rows;
    block[col_idx]->reserve(rows);
    for (int i = 0; i < rows; i++) {
        int* int_ptr = &i;
        block[col_idx]->insert_data((char*)int_ptr, 0);
        sel_idx[i] = i;
    }

    selected_size = single_column_block_pred.evaluate(block, sel_idx.data(), selected_size);
    EXPECT_EQ(selected_size, 1);
    auto* pred_col =
            reinterpret_cast<vectorized::PredicateColumnType<TYPE_INT>*>(block[col_idx].get());
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], value.template get<TYPE_INT>());
}

TEST_F(BlockColumnPredicateTest, AND_MUTI_COLUMN_VEC) {
    vectorized::MutableColumns block;
    block.push_back(vectorized::PredicateColumnType<TYPE_INT>::create());

    auto less_value = vectorized::Field::create_field<TYPE_INT>(5);
    auto great_value = vectorized::Field::create_field<TYPE_INT>(3);
    int rows = 10;
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> less_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(col_idx, "", less_value));
    std::shared_ptr<ColumnPredicate> great_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::GT>(col_idx, "", great_value));
    auto single_less_pred = SingleColumnBlockPredicate::create_unique(less_pred);
    auto single_great_pred = SingleColumnBlockPredicate::create_unique(great_pred);

    AndBlockColumnPredicate and_block_column_pred;
    and_block_column_pred.add_column_predicate(std::move(single_less_pred));
    and_block_column_pred.add_column_predicate(std::move(single_great_pred));

    std::vector<uint16_t> sel_idx(rows);
    uint16_t selected_size = rows;
    block[col_idx]->reserve(rows);
    for (int i = 0; i < rows; i++) {
        int* int_ptr = &i;
        block[col_idx]->insert_data((char*)int_ptr, 0);
        sel_idx[i] = i;
    }

    selected_size = and_block_column_pred.evaluate(block, sel_idx.data(), selected_size);
    EXPECT_EQ(selected_size, 1);
    auto* pred_col =
            reinterpret_cast<vectorized::PredicateColumnType<TYPE_INT>*>(block[col_idx].get());
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 4);
}

TEST_F(BlockColumnPredicateTest, OR_MUTI_COLUMN_VEC) {
    vectorized::MutableColumns block;
    block.push_back(vectorized::PredicateColumnType<TYPE_INT>::create());

    auto less_value = vectorized::Field::create_field<TYPE_INT>(5);
    auto great_value = vectorized::Field::create_field<TYPE_INT>(3);
    int rows = 10;
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> less_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(col_idx, "", less_value));
    std::shared_ptr<ColumnPredicate> great_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::GT>(col_idx, "", great_value));
    auto single_less_pred = SingleColumnBlockPredicate::create_unique(less_pred);
    auto single_great_pred = SingleColumnBlockPredicate::create_unique(great_pred);

    OrBlockColumnPredicate or_block_column_pred;
    or_block_column_pred.add_column_predicate(std::move(single_less_pred));
    or_block_column_pred.add_column_predicate(std::move(single_great_pred));

    std::vector<uint16_t> sel_idx(rows);
    uint16_t selected_size = rows;
    block[col_idx]->reserve(rows);
    for (int i = 0; i < rows; i++) {
        int* int_ptr = &i;
        block[col_idx]->insert_data((char*)int_ptr, 0);
        sel_idx[i] = i;
    }

    selected_size = or_block_column_pred.evaluate(block, sel_idx.data(), selected_size);
    EXPECT_EQ(selected_size, 10);
    auto* pred_col =
            reinterpret_cast<vectorized::PredicateColumnType<TYPE_INT>*>(block[col_idx].get());
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 0);
}

TEST_F(BlockColumnPredicateTest, OR_AND_MUTI_COLUMN_VEC) {
    vectorized::MutableColumns block;
    block.push_back(vectorized::PredicateColumnType<TYPE_INT>::create());

    auto less_value = vectorized::Field::create_field<TYPE_INT>(5);
    auto great_value = vectorized::Field::create_field<TYPE_INT>(3);
    int rows = 10;
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> less_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(0, "", less_value));
    std::shared_ptr<ColumnPredicate> great_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::GT>(0, "", great_value));
    std::shared_ptr<ColumnPredicate> less_pred1(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(0, "", great_value));

    // Test for and or single
    // (column < 5 and column > 3) or column < 3
    auto and_block_column_pred = AndBlockColumnPredicate::create_unique();
    and_block_column_pred->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred));
    and_block_column_pred->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(great_pred));

    OrBlockColumnPredicate or_block_column_pred;
    or_block_column_pred.add_column_predicate(std::move(and_block_column_pred));
    or_block_column_pred.add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred1));

    std::vector<uint16_t> sel_idx(rows);
    uint16_t selected_size = rows;
    block[col_idx]->reserve(rows);
    for (int i = 0; i < rows; i++) {
        int* int_ptr = &i;
        block[col_idx]->insert_data((char*)int_ptr, 0);
        sel_idx[i] = i;
    }

    selected_size = or_block_column_pred.evaluate(block, sel_idx.data(), selected_size);
    EXPECT_EQ(selected_size, 4);
    auto* pred_col =
            reinterpret_cast<vectorized::PredicateColumnType<TYPE_INT>*>(block[col_idx].get());
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 0);
    EXPECT_EQ(pred_col->get_data()[sel_idx[1]], 1);
    EXPECT_EQ(pred_col->get_data()[sel_idx[2]], 2);
    EXPECT_EQ(pred_col->get_data()[sel_idx[3]], 4);

    // Test for single or and
    //  column < 3 or (column < 5 and column > 3)
    auto and_block_column_pred1 = AndBlockColumnPredicate::create_unique();
    and_block_column_pred1->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred));
    and_block_column_pred1->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(great_pred));

    OrBlockColumnPredicate or_block_column_pred1;
    or_block_column_pred1.add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred1));
    or_block_column_pred1.add_column_predicate(std::move(and_block_column_pred1));

    selected_size = or_block_column_pred1.evaluate(block, sel_idx.data(), selected_size);
    EXPECT_EQ(selected_size, 4);
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 0);
    EXPECT_EQ(pred_col->get_data()[sel_idx[1]], 1);
    EXPECT_EQ(pred_col->get_data()[sel_idx[2]], 2);
    EXPECT_EQ(pred_col->get_data()[sel_idx[3]], 4);
}

TEST_F(BlockColumnPredicateTest, AND_OR_MUTI_COLUMN_VEC) {
    vectorized::MutableColumns block;
    block.push_back(vectorized::PredicateColumnType<TYPE_INT>::create());

    auto less_value = vectorized::Field::create_field<TYPE_INT>(5);
    auto great_value = vectorized::Field::create_field<TYPE_INT>(3);
    int rows = 10;
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> less_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(0, "", less_value));
    std::shared_ptr<ColumnPredicate> great_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::GT>(0, "", great_value));
    std::shared_ptr<ColumnPredicate> less_pred1(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(0, "", great_value));

    // Test for and or single
    // (column < 5 or column < 3) and column > 3
    auto or_block_column_pred = OrBlockColumnPredicate::create_unique();
    or_block_column_pred->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred));
    or_block_column_pred->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred1));

    AndBlockColumnPredicate and_block_column_pred;
    and_block_column_pred.add_column_predicate(std::move(or_block_column_pred));
    and_block_column_pred.add_column_predicate(
            SingleColumnBlockPredicate::create_unique(great_pred));

    std::vector<uint16_t> sel_idx(rows);
    uint16_t selected_size = rows;
    block[col_idx]->reserve(rows);
    for (int i = 0; i < rows; i++) {
        int* int_ptr = &i;
        block[col_idx]->insert_data((char*)int_ptr, 0);
        sel_idx[i] = i;
    }

    selected_size = and_block_column_pred.evaluate(block, sel_idx.data(), selected_size);

    auto* pred_col =
            reinterpret_cast<vectorized::PredicateColumnType<TYPE_INT>*>(block[col_idx].get());
    EXPECT_EQ(selected_size, 1);
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 4);

    // Test for single or and
    // column > 3 and (column < 5 or column < 3)
    auto or_block_column_pred1 = OrBlockColumnPredicate::create_unique();
    or_block_column_pred1->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred));
    or_block_column_pred1->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred1));

    AndBlockColumnPredicate and_block_column_pred1;
    and_block_column_pred1.add_column_predicate(
            SingleColumnBlockPredicate::create_unique(great_pred));
    and_block_column_pred1.add_column_predicate(std::move(or_block_column_pred1));

    EXPECT_EQ(selected_size, 1);
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 4);
}

template <PrimitiveType T, PredicateType PT>
void single_column_predicate_test_func(const segment_v2::ZoneMap& zone_map_info,
                                       vectorized::Field& check_value, bool expect_match) {
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<T, PT>(col_idx, "", check_value));
    SingleColumnBlockPredicate single_column_block_pred(pred);

    bool matched = single_column_block_pred.evaluate_and(zone_map_info);
    EXPECT_EQ(matched, expect_match);
}

template <PrimitiveType T, PredicateType PT>
void single_column_predicate_test_func(const segment_v2::ZoneMap& zone_map_info,
                                       vectorized::Field&& check_value, bool expect_match) {
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<T, PT>(col_idx, "", check_value));
    SingleColumnBlockPredicate single_column_block_pred(pred);

    bool matched = single_column_block_pred.evaluate_and(zone_map_info);
    EXPECT_EQ(matched, expect_match);
}

// test zonemap index
TEST_F(BlockColumnPredicateTest, test_double_single_column_predicate) {
    auto nan =
            vectorized::Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN());
    auto neg_inf =
            vectorized::Field::create_field<TYPE_DOUBLE>(-std::numeric_limits<double>::infinity());
    auto pos_inf =
            vectorized::Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::infinity());
    auto min = vectorized::Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::lowest());
    auto max = vectorized::Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::max());

    // test normal value min max:
    {
        std::cout << "========test normal value min max\n";
        double zonemap_min_v = std::numeric_limits<double>::lowest();
        double zonemap_max_v = std::numeric_limits<double>::max();
        segment_v2::ZoneMap zone_map_info {
                .min_value = vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_min_v),
                .max_value = vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_max_v),
                .has_null = false,
                .has_not_null = true};

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, pos_inf,
                                                                          true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, neg_inf,
                                                                          false);

        std::vector<vectorized::Field> test_values_in_range = {
                vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_min_v),
                vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_max_v),
                vectorized::Field::create_field<TYPE_DOUBLE>(-123456.789012345),
                vectorized::Field::create_field<TYPE_DOUBLE>(-0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(123456.789012345)};
        for (auto v : test_values_in_range) {
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              true);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    zone_map_info, v, v.template get<TYPE_DOUBLE>() != zonemap_min_v);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    zone_map_info, v, v.template get<TYPE_DOUBLE>() != zonemap_max_v);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              true);
        }

        // test values out of zonemap range
        {
            auto v = vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_min_v * 2);
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              false);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, v,
                                                                              false);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              false);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, v,
                                                                              true);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              true);
        }
        {
            auto v = vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_max_v * 2);
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              false);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, v,
                                                                              true);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, v,
                                                                              false);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              false);
        }
    }
    // test special range: [normal, +Infinity]
    {
        std::cout << "========test special range: [normal, +Infinity]\n";
        double zonemap_min_v = std::numeric_limits<double>::lowest();
        segment_v2::ZoneMap zone_map_info {
                .min_value = vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_min_v),
                .max_value = pos_inf,
                .has_null = false,
                .has_not_null = true};

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, pos_inf,
                                                                          true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, neg_inf,
                                                                          false);

        std::vector<vectorized::Field> test_values_in_range = {
                vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_min_v),
                max,
                pos_inf,
                vectorized::Field::create_field<TYPE_DOUBLE>(-123456.789012345),
                vectorized::Field::create_field<TYPE_DOUBLE>(-0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(123456.789012345),
        };
        for (auto v : test_values_in_range) {
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              true);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    zone_map_info, v, v.template get<TYPE_DOUBLE>() != zonemap_min_v);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    zone_map_info, v,
                    v.template get<TYPE_DOUBLE>() != pos_inf.template get<TYPE_DOUBLE>());

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              true);
        }

        // test values out of zonemap range
        {
            auto v = vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_min_v * 2);
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              false);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, v,
                                                                              false);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              false);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, v,
                                                                              true);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              true);
        }
    }
    // test special range: [-Infinity, normal]
    {
        std::cout << "========test special range: [-Infinity, normal]\n";
        double zonemap_max_v = std::numeric_limits<double>::max();
        segment_v2::ZoneMap zone_map_info {
                .min_value = neg_inf,
                .max_value = vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_max_v),
                .has_null = false,
                .has_not_null = true};

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, pos_inf,
                                                                          true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, neg_inf,
                                                                          true);

        std::vector<vectorized::Field> test_values_in_range = {
                neg_inf,
                min,
                vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_max_v),
                vectorized::Field::create_field<TYPE_DOUBLE>(-123456.789012345),
                vectorized::Field::create_field<TYPE_DOUBLE>(-0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(123456.789012345),
        };
        for (auto v : test_values_in_range) {
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              true);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, v,
                                                                              v != neg_inf);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    zone_map_info, v, v.template get<TYPE_DOUBLE>() != zonemap_max_v);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              true);
        }
        // test values out of zonemap range
        {
            auto v = vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_max_v * 2);
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              false);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, v,
                                                                              true);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, v,
                                                                              false);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              false);
        }
    }
    // test special range: [normal, NaN]
    {
        std::cout << "========test special range: [normal, NaN]\n";
        double zonemap_min_v = std::numeric_limits<float>::lowest();
        segment_v2::ZoneMap zone_map_info {
                .min_value = vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_min_v),
                .max_value = nan,
                .has_null = false,
                .has_not_null = true};

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, pos_inf,
                                                                          true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, neg_inf,
                                                                          false);

        std::vector<vectorized::Field> test_values_in_range = {
                vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_min_v),
                max,
                pos_inf,
                vectorized::Field::create_field<TYPE_DOUBLE>(-123456.789012345),
                vectorized::Field::create_field<TYPE_DOUBLE>(-0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(123456.789012345),
        };
        for (auto v : test_values_in_range) {
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              true);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    zone_map_info, v, v.template get<TYPE_DOUBLE>() != zonemap_min_v);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    zone_map_info, v, !std::isnan(v.template get<TYPE_DOUBLE>()));

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              true);
        }

        // test values out of zonemap range
        {
            auto v = vectorized::Field::create_field<TYPE_DOUBLE>(zonemap_min_v * 2);
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              false);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, v,
                                                                              false);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              false);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, v,
                                                                              true);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              true);
        }
    }
    // test special range: [-Infinity, +Infinity]
    {
        std::cout << "========test special range: [-Infinity, +Infinity]\n";
        segment_v2::ZoneMap zone_map_info {.min_value = neg_inf,
                                           .max_value = pos_inf,
                                           .has_null = false,
                                           .has_not_null = true};

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, pos_inf,
                                                                          true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, neg_inf,
                                                                          true);

        std::vector<vectorized::Field> test_values_in_range = {
                min,
                max,
                vectorized::Field::create_field<TYPE_DOUBLE>(-123456.789012345),
                vectorized::Field::create_field<TYPE_DOUBLE>(-0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(123456.789012345),
        };
        for (auto v : test_values_in_range) {
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              true);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, v,
                                                                              v != neg_inf);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, v,
                                                                              v != pos_inf);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              true);
        }
    }
    // test special range: [-Infinity, NaN]
    {
        std::cout << "========test special range: [-Infinity, NaN]\n";
        segment_v2::ZoneMap zone_map_info {
                .min_value = neg_inf, .max_value = nan, .has_null = false, .has_not_null = true};

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, pos_inf,
                                                                          true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, neg_inf,
                                                                          true);

        std::vector<vectorized::Field> test_values_in_range = {
                min,
                max,
                vectorized::Field::create_field<TYPE_DOUBLE>(-123456.789012345),
                vectorized::Field::create_field<TYPE_DOUBLE>(-0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(123456.789012345),
        };
        for (auto v : test_values_in_range) {
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              true);
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, v,
                                                                              true);

            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              true);

            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, v,
                                                                              true);

            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              true);
        }
    }
    // test special range: [-Infinity, -Infinity]
    {
        std::cout << "========test special range: [-Infinity, -Infinity]\n";
        segment_v2::ZoneMap zone_map_info {.min_value = neg_inf,
                                           .max_value = neg_inf,
                                           .has_null = false,
                                           .has_not_null = true};

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, pos_inf,
                                                                          true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, neg_inf,
                                                                          true);

        std::vector<vectorized::Field> test_values_not_in_range = {
                min,
                max,
                vectorized::Field::create_field<TYPE_DOUBLE>(-123456.789012345),
                vectorized::Field::create_field<TYPE_DOUBLE>(-0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(123456.789012345),
        };
        for (auto v : test_values_not_in_range) {
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              false);
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, v,
                                                                              true);

            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              true);

            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, v,
                                                                              false);

            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              false);
        }
    }
    // test special range: [+Infinity, +Infinity]
    {
        std::cout << "========test special range: [+Infinity, +Infinity]\n";
        segment_v2::ZoneMap zone_map_info {.min_value = pos_inf,
                                           .max_value = pos_inf,
                                           .has_null = false,
                                           .has_not_null = true};

        // test NaN
        std::cout << "========test NaN\n";
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, nan, true);

        // test +Infinity
        std::cout << "========test +Infinity\n";
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, pos_inf,
                                                                          true);

        // test -Infinity
        std::cout << "========test -Infinity\n";
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, neg_inf,
                                                                          false);

        std::cout << "========test values not in range\n";
        std::vector<vectorized::Field> test_values_not_in_range = {
                min,
                max,
                vectorized::Field::create_field<TYPE_DOUBLE>(-123456.789012345),
                vectorized::Field::create_field<TYPE_DOUBLE>(-0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(123456.789012345),
        };
        for (auto v : test_values_not_in_range) {
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              false);
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, v,
                                                                              false);

            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              false);

            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, v,
                                                                              true);

            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              true);
        }
    }
    // test special range: [NaN, NaN]
    {
        std::cout << "========test special range: [NaN, NaN]\n";
        segment_v2::ZoneMap zone_map_info {
                .min_value = nan, .max_value = nan, .has_null = false, .has_not_null = true};

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, nan,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, pos_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, pos_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, pos_inf,
                                                                          false);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, neg_inf,
                                                                          true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, neg_inf,
                                                                          false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, neg_inf,
                                                                          false);

        std::vector<vectorized::Field> test_values_not_in_range = {
                min,
                max,
                vectorized::Field::create_field<TYPE_DOUBLE>(-123456.789012345),
                vectorized::Field::create_field<TYPE_DOUBLE>(-0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(0.0),
                vectorized::Field::create_field<TYPE_DOUBLE>(123456.789012345),
        };
        for (auto v : test_values_not_in_range) {
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(zone_map_info, v,
                                                                              false);
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(zone_map_info, v,
                                                                              true);

            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(zone_map_info, v,
                                                                              false);

            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(zone_map_info, v,
                                                                              false);

            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(zone_map_info, v,
                                                                              true);

            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(zone_map_info, v,
                                                                              true);
        }
    }
}

// test timestamptz zonemap index
TEST_F(BlockColumnPredicateTest, test_timestamptz_zonemap_index) {
    cctz::time_zone time_zone = cctz::fixed_time_zone(std::chrono::hours(0));
    TimezoneUtils::load_offsets_to_cache();
    vectorized::CastParameters params;
    params.is_strict = true;

    // test normal value min max:
    {
        std::cout << "========test normal value min max\n";
        // auto zonemap_min_v = type_limit<TimestampTzValue>::min();
        // auto zonemap_max_v = type_limit<TimestampTzValue>::max();
        TimestampTzValue zonemap_min_v;
        TimestampTzValue zonemap_max_v;
        EXPECT_TRUE(zonemap_min_v.from_string(StringRef {"0001-01-01 00:00:00"}, &time_zone, params,
                                              0));
        EXPECT_TRUE(zonemap_max_v.from_string(StringRef {"8999-12-31 23:59:59"}, &time_zone, params,
                                              0));
        segment_v2::ZoneMap zone_map_info {
                .min_value = vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(zonemap_min_v),
                .max_value = vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(zonemap_max_v),
                .has_null = false,
                .has_not_null = true};

        // test values within zonemap range
        std::vector<std::string> values = {"0001-01-01 00:00:00", "2023-01-01 15:00:00",
                                           "8999-12-31 23:59:59"};
        for (auto str : values) {
            TimestampTzValue tz {};
            EXPECT_TRUE(tz.from_string(StringRef {str}, &time_zone, params, 0));
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::EQ>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz), true);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::NE>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz), true);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::LT>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz),
                    tz != zonemap_min_v);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::LE>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz), true);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::GT>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz),
                    tz != zonemap_max_v);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::GE>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz), true);
        }
        // test values out of zonemap range
        {
            auto v = vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(
                    type_limit<TimestampTzValue>::min());
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::EQ>(zone_map_info, v,
                                                                                   false);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::NE>(zone_map_info, v,
                                                                                   true);

            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::LT>(zone_map_info, v,
                                                                                   false);

            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::LE>(zone_map_info, v,
                                                                                   false);

            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::GT>(zone_map_info, v,
                                                                                   true);

            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::GE>(zone_map_info, v,
                                                                                   true);
        }
        // test values out of zonemap range
        {
            auto v = vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(
                    type_limit<TimestampTzValue>::max());
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::EQ>(zone_map_info, v,
                                                                                   false);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::NE>(zone_map_info, v,
                                                                                   true);

            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::LT>(zone_map_info, v,
                                                                                   true);

            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::LE>(zone_map_info, v,
                                                                                   true);

            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::GT>(zone_map_info, v,
                                                                                   false);

            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::GE>(zone_map_info, v,
                                                                                   false);
        }
    }

    // test range [min, max]:
    {
        std::cout << "========test range [min, max]\n";
        auto zonemap_min_v = type_limit<TimestampTzValue>::min();
        auto zonemap_max_v = type_limit<TimestampTzValue>::max();
        segment_v2::ZoneMap zone_map_info {
                .min_value = vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(zonemap_min_v),
                .max_value = vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(zonemap_max_v),
                .has_null = false,
                .has_not_null = true};

        // test values within zonemap range
        std::vector<std::string> values = {"0000-01-01 00:00:00", "2023-01-01 15:00:00",
                                           "9999-12-31 23:59:59.999999"};
        for (auto str : values) {
            TimestampTzValue tz {};
            EXPECT_TRUE(tz.from_string(StringRef {str}, &time_zone, params, 6));
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::EQ>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz), true);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::NE>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz), true);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::LT>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz),
                    tz != zonemap_min_v);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::LE>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz), true);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::GT>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz),
                    tz != zonemap_max_v);
            single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::GE>(
                    zone_map_info, vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz), true);
        }
    }
}

template <PrimitiveType T, PredicateType PT>
void single_column_predicate_test_func(const segment_v2::BloomFilter* bf,
                                       vectorized::Field&& check_value, bool expect_match) {
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<T, PT>(col_idx, "", check_value));
    SingleColumnBlockPredicate single_column_block_pred(pred);

    bool matched = single_column_block_pred.evaluate_and(bf);
    EXPECT_EQ(matched, expect_match);
}
// test timestamptz bloom filter
TEST_F(BlockColumnPredicateTest, test_timestamptz_bloom_filter) {
    cctz::time_zone time_zone = cctz::fixed_time_zone(std::chrono::hours(0));
    TimezoneUtils::load_offsets_to_cache();
    vectorized::CastParameters params;
    params.is_strict = true;

    std::vector<std::string> str_values = {"0001-01-01 00:00:00", "2023-01-01 15:00:00",
                                           "1111-01-01 01:01:01", "5555-05-05 05:05:05",
                                           "6666-06-06 06:06:06", "7777-07-07 07:07:07",
                                           "6666-12-01 23:00:00", "8999-12-31 23:59:59"};

    std::unique_ptr<BloomFilter> bf;
    auto st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(nullptr, bf);
    st = bf->init(1024, 0.05, HASH_MURMUR3_X64_64);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(bf->size() > 0);

    std::vector<TimestampTzValue> values;
    for (const auto& str : str_values) {
        TimestampTzValue tz {};
        EXPECT_TRUE(tz.from_string(StringRef {str}, &time_zone, params, 0));
        bf->add_bytes((char*)&tz, sizeof(TimestampTzValue));
        values.push_back(tz);
    }

    for (const auto& v : values) {
        single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::EQ>(
                bf.get(), vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(v), true);
    }
    {
        auto str = "0000-01-01 00:00:00";
        TimestampTzValue tz {};
        EXPECT_TRUE(tz.from_string(StringRef {str}, &time_zone, params, 0));
        single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::EQ>(
                bf.get(), vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz), false);
    }
    {
        auto str = "9999-12-31 23:59:59.999999";
        TimestampTzValue tz {};
        EXPECT_TRUE(tz.from_string(StringRef {str}, &time_zone, params, 6));
        single_column_predicate_test_func<TYPE_TIMESTAMPTZ, PredicateType::EQ>(
                bf.get(), vectorized::Field::create_field<TYPE_TIMESTAMPTZ>(tz), false);
    }
}

TEST_F(BlockColumnPredicateTest, PARQUET_COMPARISON_PREDICATE) {
    { // INT
     {// EQ
      auto value = vectorized::Field::create_field<TYPE_INT>(5);
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, "", value));
    SingleColumnBlockPredicate single_column_block_pred(pred);
    std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
            std::make_unique<vectorized::FieldSchema>();
    parquet_field_col1->name = "col1";
    parquet_field_col1->data_type =
            vectorized::DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    parquet_field_col1->field_id = -1;
    parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

    vectorized::ParquetPredicate::ColumnStat stat;
    cctz::time_zone tmp_ctz;
    stat.ctz = &tmp_ctz;

    std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
    {
        // 5 belongs to [5, 5]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
            stat->encoded_min_value = tmp;
            stat->encoded_max_value = tmp;
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
    }
    {
        // 5 not belongs to [6, 7]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            int lower = 6;
            int upper = 7;
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            stat->encoded_min_value =
                    std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
            stat->encoded_max_value =
                    std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
    }
    {
        // 5 not belongs to [1, 4]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            int lower = 1;
            int upper = 4;
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            stat->encoded_min_value =
                    std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
            stat->encoded_max_value =
                    std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
    }
    {
        // get stat failed
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            return false;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
    }
}
{
    // NE
    auto value = vectorized::Field::create_field<TYPE_INT>(5);
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, "", value));
    SingleColumnBlockPredicate single_column_block_pred(pred);
    std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
            std::make_unique<vectorized::FieldSchema>();
    parquet_field_col1->name = "col1";
    parquet_field_col1->data_type =
            vectorized::DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    parquet_field_col1->field_id = -1;
    parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

    vectorized::ParquetPredicate::ColumnStat stat;
    cctz::time_zone tmp_ctz;
    stat.ctz = &tmp_ctz;

    std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
    {
        // 5 belongs to [5, 5]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
            stat->encoded_min_value = tmp;
            stat->encoded_max_value = tmp;
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
    }
    {
        // 5 not belongs to [6, 7]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            int lower = 6;
            int upper = 7;
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            stat->encoded_min_value =
                    std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
            stat->encoded_max_value =
                    std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
    }
    {
        // 5 not belongs to [1, 4]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            int lower = 1;
            int upper = 4;
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            stat->encoded_min_value =
                    std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
            stat->encoded_max_value =
                    std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
    }
}
{
    // GE
    auto value = vectorized::Field::create_field<TYPE_INT>(5);
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::GE>(col_idx, "", value));
    SingleColumnBlockPredicate single_column_block_pred(pred);
    std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
            std::make_unique<vectorized::FieldSchema>();
    parquet_field_col1->name = "col1";
    parquet_field_col1->data_type =
            vectorized::DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    parquet_field_col1->field_id = -1;
    parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

    vectorized::ParquetPredicate::ColumnStat stat;
    cctz::time_zone tmp_ctz;
    stat.ctz = &tmp_ctz;

    std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
    {
        // 5 belongs to [5, 5]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
            stat->encoded_min_value = tmp;
            stat->encoded_max_value = tmp;
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
    }
    {
        // 5 not belongs to [6, 7]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            int lower = 6;
            int upper = 7;
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            stat->encoded_min_value =
                    std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
            stat->encoded_max_value =
                    std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
    }
    {
        // 5 not belongs to [1, 4]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            int lower = 1;
            int upper = 4;
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            stat->encoded_min_value =
                    std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
            stat->encoded_max_value =
                    std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
    }
}
{
    // LE
    auto value = vectorized::Field::create_field<TYPE_INT>(5);
    int col_idx = 0;
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LE>(col_idx, "", value));
    SingleColumnBlockPredicate single_column_block_pred(pred);
    std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
            std::make_unique<vectorized::FieldSchema>();
    parquet_field_col1->name = "col1";
    parquet_field_col1->data_type =
            vectorized::DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    parquet_field_col1->field_id = -1;
    parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

    vectorized::ParquetPredicate::ColumnStat stat;
    cctz::time_zone tmp_ctz;
    stat.ctz = &tmp_ctz;

    std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
    {
        // 5 belongs to [5, 5]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
            stat->encoded_min_value = tmp;
            stat->encoded_max_value = tmp;
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
    }
    {
        // 5 not belongs to [6, 7]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            int lower = 6;
            int upper = 7;
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            stat->encoded_min_value =
                    std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
            stat->encoded_max_value =
                    std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
    }
    {
        // 5 not belongs to [1, 4]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            int lower = 1;
            int upper = 4;
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            stat->encoded_min_value =
                    std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
            stat->encoded_max_value =
                    std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
    }
}
} // namespace doris
{
    // FLOAT
    {
        // EQ
        auto value = vectorized::Field::create_field<TYPE_FLOAT>(5.0);
        int col_idx = 0;
        std::shared_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_FLOAT, PredicateType::EQ>(col_idx, "", value));
        SingleColumnBlockPredicate single_column_block_pred(pred);
        std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                std::make_unique<vectorized::FieldSchema>();
        parquet_field_col1->name = "col1";
        parquet_field_col1->data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_FLOAT, true);
        parquet_field_col1->field_id = -1;
        parquet_field_col1->parquet_schema.type = tparquet::Type::type::FLOAT;

        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
        {
            // 5 belongs to [5, 5]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
                stat->encoded_min_value = tmp;
                stat->encoded_max_value = tmp;
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // 5 not belongs to [6, 7]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                float lower = 6.0;
                float upper = 7.0;
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                stat->encoded_min_value =
                        std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
                stat->encoded_max_value =
                        std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // 5 not belongs to [1, 4]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                float lower = 1.0;
                float upper = 4.0;
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                stat->encoded_min_value =
                        std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
                stat->encoded_max_value =
                        std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // get stat failed
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                return false;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // get min max failed
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                float lower = nanf("");
                float upper = 4.0;
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                stat->encoded_min_value =
                        std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
                stat->encoded_max_value =
                        std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
    }
    {
        // NE
        auto value = vectorized::Field::create_field<TYPE_FLOAT>(5.0);
        int col_idx = 0;
        std::shared_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_FLOAT, PredicateType::NE>(col_idx, "", value));
        SingleColumnBlockPredicate single_column_block_pred(pred);
        std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                std::make_unique<vectorized::FieldSchema>();
        parquet_field_col1->name = "col1";
        parquet_field_col1->data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_FLOAT, true);
        parquet_field_col1->field_id = -1;
        parquet_field_col1->parquet_schema.type = tparquet::Type::type::FLOAT;

        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
        {
            // 5 belongs to [5, 5]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
                stat->encoded_min_value = tmp;
                stat->encoded_max_value = tmp;
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // 5 not belongs to [6, 7]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                float lower = 6.0;
                float upper = 7.0;
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                stat->encoded_min_value =
                        std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
                stat->encoded_max_value =
                        std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // 5 not belongs to [1, 4]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                float lower = 1.0;
                float upper = 4.0;
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                stat->encoded_min_value =
                        std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
                stat->encoded_max_value =
                        std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
    }
    {
        // GE
        auto value = vectorized::Field::create_field<TYPE_FLOAT>(5.0);
        int col_idx = 0;
        std::shared_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_FLOAT, PredicateType::GE>(col_idx, "", value));
        SingleColumnBlockPredicate single_column_block_pred(pred);
        std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                std::make_unique<vectorized::FieldSchema>();
        parquet_field_col1->name = "col1";
        parquet_field_col1->data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, true);
        parquet_field_col1->field_id = -1;
        parquet_field_col1->parquet_schema.type = tparquet::Type::type::FLOAT;

        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
        {
            // 5 belongs to [5, 5]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
                stat->encoded_min_value = tmp;
                stat->encoded_max_value = tmp;
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // 5 not belongs to [6, 7]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                float lower = 6.0;
                float upper = 7.0;
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                stat->encoded_min_value =
                        std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
                stat->encoded_max_value =
                        std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // 5 not belongs to [1, 4]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                float lower = 1.0;
                float upper = 4.0;
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                stat->encoded_min_value =
                        std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
                stat->encoded_max_value =
                        std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
        }
    }
    {
        // LE
        auto value = vectorized::Field::create_field<TYPE_FLOAT>(5.0);
        int col_idx = 0;
        std::shared_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_FLOAT, PredicateType::LE>(col_idx, "", value));
        SingleColumnBlockPredicate single_column_block_pred(pred);
        std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                std::make_unique<vectorized::FieldSchema>();
        parquet_field_col1->name = "col1";
        parquet_field_col1->data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_FLOAT, true);
        parquet_field_col1->field_id = -1;
        parquet_field_col1->parquet_schema.type = tparquet::Type::type::FLOAT;

        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
        {
            // 5 belongs to [5, 5]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
                stat->encoded_min_value = tmp;
                stat->encoded_max_value = tmp;
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // 5 not belongs to [6, 7]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                float lower = 6.0;
                float upper = 7.0;
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                stat->encoded_min_value =
                        std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
                stat->encoded_max_value =
                        std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // 5 not belongs to [1, 4]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                float lower = 1.0;
                float upper = 4.0;
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                stat->encoded_min_value =
                        std::string(reinterpret_cast<const char*>(&lower), sizeof(lower));
                stat->encoded_max_value =
                        std::string(reinterpret_cast<const char*>(&upper), sizeof(upper));
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
    }
}
}

TEST_F(BlockColumnPredicateTest, PARQUET_IN_PREDICATE) {
    { // INT
        {
            int value = 5;
            int col_idx = 0;
            auto hybrid_set = std::make_shared<HybridSet<PrimitiveType::TYPE_INT>>(false);
            hybrid_set->insert(&value);
            std::shared_ptr<ColumnPredicate> pred(
                    new InListPredicateBase<TYPE_INT, PredicateType::IN_LIST, 1>(
                            col_idx, "", hybrid_set, false));
            SingleColumnBlockPredicate single_column_block_pred(pred);
            std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                    std::make_unique<vectorized::FieldSchema>();
            parquet_field_col1->name = "col1";
            parquet_field_col1->data_type =
                    vectorized::DataTypeFactory::instance().create_data_type(
                            PrimitiveType::TYPE_INT, true);
            parquet_field_col1->field_id = -1;
            parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

            vectorized::ParquetPredicate::ColumnStat stat;
            cctz::time_zone tmp_ctz;
            stat.ctz = &tmp_ctz;

            std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
            {
                // 5 belongs to [5, 5]
                get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                    stat->col_schema = parquet_field_col1.get();
                    stat->is_all_null = false;
                    stat->has_null = false;
                    auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
                    stat->encoded_min_value = tmp;
                    stat->encoded_max_value = tmp;
                    return true;
                };
                stat.get_stat_func = &get_stat_func;
                EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
            }
            {
                // get stat failed
                get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                    return false;
                };
                stat.get_stat_func = &get_stat_func;
                EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
            }
        }
        {
            int value = 5;
            int col_idx = 0;
            auto hybrid_set = std::make_shared<HybridSet<PrimitiveType::TYPE_INT>>(false);
            hybrid_set->insert(&value);
            std::shared_ptr<ColumnPredicate> pred(
                    new InListPredicateBase<TYPE_INT, PredicateType::IN_LIST, 1>(
                            col_idx, "", hybrid_set, false));
            SingleColumnBlockPredicate single_column_block_pred(pred);
            std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                    std::make_unique<vectorized::FieldSchema>();
            parquet_field_col1->name = "col1";
            parquet_field_col1->data_type =
                    vectorized::DataTypeFactory::instance().create_data_type(
                            PrimitiveType::TYPE_INT, true);
            parquet_field_col1->field_id = -1;
            parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

            vectorized::ParquetPredicate::ColumnStat stat;
            cctz::time_zone tmp_ctz;
            stat.ctz = &tmp_ctz;

            std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
            {
                // 5 belongs to [5, 5]
                get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                    stat->col_schema = parquet_field_col1.get();
                    stat->is_all_null = false;
                    stat->has_null = false;
                    int tmp_v = 6;
                    auto tmp = std::string(reinterpret_cast<const char*>(&tmp_v), sizeof(tmp_v));
                    stat->encoded_min_value = tmp;
                    stat->encoded_max_value = tmp;
                    return true;
                };
                stat.get_stat_func = &get_stat_func;
                EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
            }
            {
                // get stat failed
                get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                    return false;
                };
                stat.get_stat_func = &get_stat_func;
                EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
            }
        }
    }
}

TEST_F(BlockColumnPredicateTest, PARQUET_COMPARISON_PREDICATE_BLOOM_FILTER) {
    auto value = vectorized::Field::create_field<TYPE_INT>(42);
    const int col_idx = 0;
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, "", value));
    SingleColumnBlockPredicate single_column_block_pred(pred);

    auto parquet_field = std::make_unique<vectorized::FieldSchema>();
    parquet_field->name = "col1";
    parquet_field->data_type =
            vectorized::DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    parquet_field->field_id = -1;
    parquet_field->parquet_schema.type = tparquet::Type::type::INT32;

    auto encode_value = [](int v) {
        return std::string(reinterpret_cast<const char*>(&v), sizeof(v));
    };

    {
        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    current_stat->col_schema = parquet_field.get();
                    current_stat->is_all_null = false;
                    current_stat->has_null = false;
                    current_stat->encoded_min_value = encode_value(value.template get<TYPE_INT>());
                    current_stat->encoded_max_value = encode_value(value.template get<TYPE_INT>());
                    return true;
                };
        stat.get_stat_func = &get_stat_func;

        int loader_calls = 0;
        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_bloom_filter_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    loader_calls++;
                    if (!current_stat->bloom_filter) {
                        current_stat->bloom_filter =
                                std::make_unique<vectorized::ParquetBlockSplitBloomFilter>();
                        auto* bloom = static_cast<vectorized::ParquetBlockSplitBloomFilter*>(
                                current_stat->bloom_filter.get());
                        Status st = bloom->init(256, segment_v2::HashStrategyPB::XX_HASH_64);
                        EXPECT_TRUE(st.ok());
                        bloom->add_bytes(
                                reinterpret_cast<const char*>(&value.template get<TYPE_INT>()),
                                sizeof(value.template get<TYPE_INT>()));
                    }
                    return true;
                };
        stat.get_bloom_filter_func = &get_bloom_filter_func;

        EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        EXPECT_EQ(1, loader_calls);
    }

    {
        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    current_stat->col_schema = parquet_field.get();
                    current_stat->is_all_null = false;
                    current_stat->has_null = false;
                    current_stat->encoded_min_value = encode_value(value.template get<TYPE_INT>());
                    current_stat->encoded_max_value = encode_value(value.template get<TYPE_INT>());
                    return true;
                };
        stat.get_stat_func = &get_stat_func;

        int loader_calls = 0;
        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_bloom_filter_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    loader_calls++;
                    if (!current_stat->bloom_filter) {
                        current_stat->bloom_filter =
                                std::make_unique<vectorized::ParquetBlockSplitBloomFilter>();
                        auto* bloom = static_cast<vectorized::ParquetBlockSplitBloomFilter*>(
                                current_stat->bloom_filter.get());
                        Status st = bloom->init(256, segment_v2::HashStrategyPB::XX_HASH_64);
                        EXPECT_TRUE(st.ok());
                        int other_value = value.template get<TYPE_INT>() + 10;
                        bloom->add_bytes(reinterpret_cast<const char*>(&other_value),
                                         sizeof(other_value));
                    }
                    return true;
                };
        stat.get_bloom_filter_func = &get_bloom_filter_func;

        EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
        EXPECT_EQ(1, loader_calls);
    }

    {
        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    current_stat->col_schema = parquet_field.get();
                    current_stat->is_all_null = false;
                    current_stat->has_null = false;
                    current_stat->encoded_min_value = encode_value(value.template get<TYPE_INT>());
                    current_stat->encoded_max_value = encode_value(value.template get<TYPE_INT>());
                    return true;
                };
        stat.get_stat_func = &get_stat_func;

        bool loader_invoked = false;
        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_bloom_filter_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    loader_invoked = true;
                    return false;
                };
        stat.get_bloom_filter_func = &get_bloom_filter_func;

        EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        EXPECT_TRUE(loader_invoked);
    }

    {
        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    current_stat->col_schema = parquet_field.get();
                    current_stat->is_all_null = false;
                    current_stat->has_null = false;
                    int min_value = value.template get<TYPE_INT>() + 5;
                    int max_value = value.template get<TYPE_INT>() + 10;
                    current_stat->encoded_min_value = encode_value(min_value);
                    current_stat->encoded_max_value = encode_value(max_value);
                    return true;
                };
        stat.get_stat_func = &get_stat_func;

        int loader_calls = 0;
        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_bloom_filter_func =
                [&](vectorized::ParquetPredicate::ColumnStat*, int) {
                    loader_calls++;
                    return true;
                };
        stat.get_bloom_filter_func = &get_bloom_filter_func;

        EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
        EXPECT_EQ(0, loader_calls);
    }
}

TEST_F(BlockColumnPredicateTest, PARQUET_IN_PREDICATE_BLOOM_FILTER) {
    const int col_idx = 0;
    auto hybrid_set = std::make_shared<HybridSet<PrimitiveType::TYPE_INT>>(false);
    const int included_value = 7;
    hybrid_set->insert(&included_value);
    std::shared_ptr<ColumnPredicate> pred(
            new InListPredicateBase<TYPE_INT, PredicateType::IN_LIST, 1>(col_idx, "", hybrid_set,
                                                                         false));
    SingleColumnBlockPredicate single_column_block_pred(pred);

    auto parquet_field = std::make_unique<vectorized::FieldSchema>();
    parquet_field->name = "col1";
    parquet_field->data_type =
            vectorized::DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, true);
    parquet_field->field_id = -1;
    parquet_field->parquet_schema.type = tparquet::Type::type::INT32;

    auto encode_value = [](int v) {
        return std::string(reinterpret_cast<const char*>(&v), sizeof(v));
    };

    {
        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    current_stat->col_schema = parquet_field.get();
                    current_stat->is_all_null = false;
                    current_stat->has_null = false;
                    current_stat->encoded_min_value = encode_value(included_value);
                    current_stat->encoded_max_value = encode_value(included_value);
                    return true;
                };
        stat.get_stat_func = &get_stat_func;

        int loader_calls = 0;
        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_bloom_filter_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    loader_calls++;
                    if (!current_stat->bloom_filter) {
                        current_stat->bloom_filter =
                                std::make_unique<vectorized::ParquetBlockSplitBloomFilter>();
                        auto* bloom = static_cast<vectorized::ParquetBlockSplitBloomFilter*>(
                                current_stat->bloom_filter.get());
                        Status st = bloom->init(256, segment_v2::HashStrategyPB::XX_HASH_64);
                        EXPECT_TRUE(st.ok());
                        bloom->add_bytes(reinterpret_cast<const char*>(&included_value),
                                         sizeof(included_value));
                    }
                    return true;
                };
        stat.get_bloom_filter_func = &get_bloom_filter_func;

        EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        EXPECT_EQ(1, loader_calls);
    }

    {
        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    current_stat->col_schema = parquet_field.get();
                    current_stat->is_all_null = false;
                    current_stat->has_null = false;
                    current_stat->encoded_min_value = encode_value(included_value);
                    current_stat->encoded_max_value = encode_value(included_value);
                    return true;
                };
        stat.get_stat_func = &get_stat_func;

        int loader_calls = 0;
        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_bloom_filter_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    loader_calls++;
                    if (!current_stat->bloom_filter) {
                        current_stat->bloom_filter =
                                std::make_unique<vectorized::ParquetBlockSplitBloomFilter>();
                        auto* bloom = static_cast<vectorized::ParquetBlockSplitBloomFilter*>(
                                current_stat->bloom_filter.get());
                        Status st = bloom->init(256, segment_v2::HashStrategyPB::XX_HASH_64);
                        EXPECT_TRUE(st.ok());
                        int excluded_value = included_value + 1;
                        bloom->add_bytes(reinterpret_cast<const char*>(&excluded_value),
                                         sizeof(excluded_value));
                    }
                    return true;
                };
        stat.get_bloom_filter_func = &get_bloom_filter_func;

        EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
        EXPECT_EQ(1, loader_calls);
    }

    {
        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func =
                [&](vectorized::ParquetPredicate::ColumnStat* current_stat, int cid) {
                    EXPECT_EQ(col_idx, cid);
                    current_stat->col_schema = parquet_field.get();
                    current_stat->is_all_null = false;
                    current_stat->has_null = false;
                    int min_value = included_value + 5;
                    int max_value = included_value + 10;
                    current_stat->encoded_min_value = encode_value(min_value);
                    current_stat->encoded_max_value = encode_value(max_value);
                    return true;
                };
        stat.get_stat_func = &get_stat_func;

        int loader_calls = 0;
        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_bloom_filter_func =
                [&](vectorized::ParquetPredicate::ColumnStat*, int) {
                    loader_calls++;
                    return true;
                };
        stat.get_bloom_filter_func = &get_bloom_filter_func;

        EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
        EXPECT_EQ(0, loader_calls);
    }
}

TEST_F(BlockColumnPredicateTest, NULL_PREDICATE) {
    {
        int col_idx = 0;
        std::shared_ptr<ColumnPredicate> pred(
                new NullPredicate(col_idx, "", true, PrimitiveType::TYPE_INT));
        SingleColumnBlockPredicate single_column_block_pred(pred);
        std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                std::make_unique<vectorized::FieldSchema>();
        parquet_field_col1->name = "col1";
        parquet_field_col1->data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, true);
        parquet_field_col1->field_id = -1;
        parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
        {
            // 5 belongs to [5, 5]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // get stat failed
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                return false;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
    }
    {
        int col_idx = 0;
        std::shared_ptr<ColumnPredicate> pred(
                new NullPredicate(col_idx, "", false, PrimitiveType::TYPE_INT));
        SingleColumnBlockPredicate single_column_block_pred(pred);
        std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                std::make_unique<vectorized::FieldSchema>();
        parquet_field_col1->name = "col1";
        parquet_field_col1->data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, true);
        parquet_field_col1->field_id = -1;
        parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
        {
            // 5 belongs to [5, 5]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = false;
                stat->has_null = false;
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // 5 belongs to [5, 5]
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                stat->col_schema = parquet_field_col1.get();
                stat->is_all_null = true;
                stat->has_null = false;
                return true;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_FALSE(single_column_block_pred.evaluate_and(&stat));
        }
        {
            // get stat failed
            get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
                return false;
            };
            stat.get_stat_func = &get_stat_func;
            EXPECT_TRUE(single_column_block_pred.evaluate_and(&stat));
        }
    }
}

TEST_F(BlockColumnPredicateTest, COMBINED_PREDICATE) {
    {
        AndBlockColumnPredicate and_block_column_pred;

        std::unique_ptr<SingleColumnBlockPredicate> true_predicate;
        int col_idx = 0;
        auto value = vectorized::Field::create_field<TYPE_INT>(5);
        std::shared_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, "", value));
        true_predicate = std::make_unique<SingleColumnBlockPredicate>(pred);

        std::unique_ptr<SingleColumnBlockPredicate> false_predicate;
        std::shared_ptr<ColumnPredicate> pred2(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, "", value));
        false_predicate = std::make_unique<SingleColumnBlockPredicate>(pred2);

        std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                std::make_unique<vectorized::FieldSchema>();
        parquet_field_col1->name = "col1";
        parquet_field_col1->data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, true);
        parquet_field_col1->field_id = -1;
        parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
        // 5 belongs to [5, 5]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
            stat->encoded_min_value = tmp;
            stat->encoded_max_value = tmp;
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_TRUE(true_predicate->evaluate_and(&stat));
        EXPECT_FALSE(false_predicate->evaluate_and(&stat));
        and_block_column_pred.add_column_predicate(std::move(true_predicate));
        and_block_column_pred.add_column_predicate(std::move(false_predicate));
        EXPECT_FALSE(and_block_column_pred.evaluate_and(&stat));
    }
    {
        AndBlockColumnPredicate and_block_column_pred;

        std::unique_ptr<SingleColumnBlockPredicate> true_predicate;
        int col_idx = 0;
        auto value = vectorized::Field::create_field<TYPE_INT>(5);
        std::shared_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, "", value));
        true_predicate = std::make_unique<SingleColumnBlockPredicate>(pred);

        std::unique_ptr<SingleColumnBlockPredicate> true_predicate2;
        std::shared_ptr<ColumnPredicate> pred2(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, "", value));
        true_predicate2 = std::make_unique<SingleColumnBlockPredicate>(pred2);

        std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                std::make_unique<vectorized::FieldSchema>();
        parquet_field_col1->name = "col1";
        parquet_field_col1->data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, true);
        parquet_field_col1->field_id = -1;
        parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
        // 5 belongs to [5, 5]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
            stat->encoded_min_value = tmp;
            stat->encoded_max_value = tmp;
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_TRUE(true_predicate->evaluate_and(&stat));
        EXPECT_TRUE(true_predicate2->evaluate_and(&stat));
        and_block_column_pred.add_column_predicate(std::move(true_predicate));
        and_block_column_pred.add_column_predicate(std::move(true_predicate2));
        EXPECT_TRUE(and_block_column_pred.evaluate_and(&stat));
    }
    {
        OrBlockColumnPredicate or_block_column_pred;

        std::unique_ptr<SingleColumnBlockPredicate> true_predicate;
        int col_idx = 0;
        auto value = vectorized::Field::create_field<TYPE_INT>(5);
        std::shared_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, "", value));
        true_predicate = std::make_unique<SingleColumnBlockPredicate>(pred);

        std::unique_ptr<SingleColumnBlockPredicate> false_predicate;
        std::shared_ptr<ColumnPredicate> pred2(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, "", value));
        false_predicate = std::make_unique<SingleColumnBlockPredicate>(pred2);

        std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                std::make_unique<vectorized::FieldSchema>();
        parquet_field_col1->name = "col1";
        parquet_field_col1->data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, true);
        parquet_field_col1->field_id = -1;
        parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
        // 5 belongs to [5, 5]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
            stat->encoded_min_value = tmp;
            stat->encoded_max_value = tmp;
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_TRUE(true_predicate->evaluate_and(&stat));
        EXPECT_FALSE(false_predicate->evaluate_and(&stat));
        or_block_column_pred.add_column_predicate(std::move(true_predicate));
        or_block_column_pred.add_column_predicate(std::move(false_predicate));
        EXPECT_TRUE(or_block_column_pred.evaluate_and(&stat));
    }
    {
        OrBlockColumnPredicate or_block_column_pred;

        std::unique_ptr<SingleColumnBlockPredicate> false_predicate2;
        int col_idx = 0;
        auto value = vectorized::Field::create_field<TYPE_INT>(5);
        std::shared_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, "", value));
        false_predicate2 = std::make_unique<SingleColumnBlockPredicate>(pred);

        std::unique_ptr<SingleColumnBlockPredicate> false_predicate;
        std::shared_ptr<ColumnPredicate> pred2(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, "", value));
        false_predicate = std::make_unique<SingleColumnBlockPredicate>(pred2);

        std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                std::make_unique<vectorized::FieldSchema>();
        parquet_field_col1->name = "col1";
        parquet_field_col1->data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, true);
        parquet_field_col1->field_id = -1;
        parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
        // 5 belongs to [5, 5]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
            stat->encoded_min_value = tmp;
            stat->encoded_max_value = tmp;
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_FALSE(false_predicate2->evaluate_and(&stat));
        EXPECT_FALSE(false_predicate->evaluate_and(&stat));
        or_block_column_pred.add_column_predicate(std::move(false_predicate2));
        or_block_column_pred.add_column_predicate(std::move(false_predicate));
        EXPECT_FALSE(or_block_column_pred.evaluate_and(&stat));
    }
    {
        OrBlockColumnPredicate or_block_column_pred;

        int col_idx = 0;
        auto value = vectorized::Field::create_field<TYPE_INT>(5);
        std::unique_ptr<SingleColumnBlockPredicate> false_predicate;
        std::shared_ptr<ColumnPredicate> pred2(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, "", value));
        false_predicate = std::make_unique<SingleColumnBlockPredicate>(pred2);

        std::unique_ptr<vectorized::FieldSchema> parquet_field_col1 =
                std::make_unique<vectorized::FieldSchema>();
        parquet_field_col1->name = "col1";
        parquet_field_col1->data_type = vectorized::DataTypeFactory::instance().create_data_type(
                PrimitiveType::TYPE_INT, true);
        parquet_field_col1->field_id = -1;
        parquet_field_col1->parquet_schema.type = tparquet::Type::type::INT32;

        vectorized::ParquetPredicate::ColumnStat stat;
        cctz::time_zone tmp_ctz;
        stat.ctz = &tmp_ctz;

        std::function<bool(vectorized::ParquetPredicate::ColumnStat*, int)> get_stat_func;
        // 5 belongs to [5, 5]
        get_stat_func = [&](vectorized::ParquetPredicate::ColumnStat* stat, const int cid) {
            stat->col_schema = parquet_field_col1.get();
            stat->is_all_null = false;
            stat->has_null = false;
            auto tmp = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
            stat->encoded_min_value = tmp;
            stat->encoded_max_value = tmp;
            return true;
        };
        stat.get_stat_func = &get_stat_func;
        EXPECT_FALSE(false_predicate->evaluate_and(&stat));
        or_block_column_pred.add_column_predicate(std::move(false_predicate));
        EXPECT_FALSE(or_block_column_pred.evaluate_and(&stat));
    }
}

} // namespace doris
