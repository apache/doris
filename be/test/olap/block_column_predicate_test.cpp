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
#include "vec/columns/column.h"
#include "vec/columns/predicate_column.h"
#include "vec/core/field.h"
#include "vec/exec/format/parquet/parquet_block_split_bloom_filter.h"
#include "vec/exec/format/parquet/vparquet_reader.h"

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

    int value = 5;
    int rows = 10;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, value));
    SingleColumnBlockPredicate single_column_block_pred(pred.get());

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
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], value);
}

TEST_F(BlockColumnPredicateTest, AND_MUTI_COLUMN_VEC) {
    vectorized::MutableColumns block;
    block.push_back(vectorized::PredicateColumnType<TYPE_INT>::create());

    int less_value = 5;
    int great_value = 3;
    int rows = 10;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> less_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(col_idx, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::GT>(col_idx, great_value));
    auto single_less_pred = SingleColumnBlockPredicate::create_unique(less_pred.get());
    auto single_great_pred = SingleColumnBlockPredicate::create_unique(great_pred.get());

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

    int less_value = 5;
    int great_value = 3;
    int rows = 10;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> less_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(col_idx, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::GT>(col_idx, great_value));
    auto single_less_pred = SingleColumnBlockPredicate::create_unique(less_pred.get());
    auto single_great_pred = SingleColumnBlockPredicate::create_unique(great_pred.get());

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

    int less_value = 5;
    int great_value = 3;
    int rows = 10;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> less_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(0, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::GT>(0, great_value));
    std::unique_ptr<ColumnPredicate> less_pred1(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(0, great_value));

    // Test for and or single
    // (column < 5 and column > 3) or column < 3
    auto and_block_column_pred = AndBlockColumnPredicate::create_unique();
    and_block_column_pred->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred.get()));
    and_block_column_pred->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(great_pred.get()));

    OrBlockColumnPredicate or_block_column_pred;
    or_block_column_pred.add_column_predicate(std::move(and_block_column_pred));
    or_block_column_pred.add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred1.get()));

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
            SingleColumnBlockPredicate::create_unique(less_pred.get()));
    and_block_column_pred1->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(great_pred.get()));

    OrBlockColumnPredicate or_block_column_pred1;
    or_block_column_pred1.add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred1.get()));
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

    int less_value = 5;
    int great_value = 3;
    int rows = 10;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> less_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(0, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::GT>(0, great_value));
    std::unique_ptr<ColumnPredicate> less_pred1(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LT>(0, great_value));

    // Test for and or single
    // (column < 5 or column < 3) and column > 3
    auto or_block_column_pred = OrBlockColumnPredicate::create_unique();
    or_block_column_pred->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred.get()));
    or_block_column_pred->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred1.get()));

    AndBlockColumnPredicate and_block_column_pred;
    and_block_column_pred.add_column_predicate(std::move(or_block_column_pred));
    and_block_column_pred.add_column_predicate(
            SingleColumnBlockPredicate::create_unique(great_pred.get()));

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
            SingleColumnBlockPredicate::create_unique(less_pred.get()));
    or_block_column_pred1->add_column_predicate(
            SingleColumnBlockPredicate::create_unique(less_pred1.get()));

    AndBlockColumnPredicate and_block_column_pred1;
    and_block_column_pred1.add_column_predicate(
            SingleColumnBlockPredicate::create_unique(great_pred.get()));
    and_block_column_pred1.add_column_predicate(std::move(or_block_column_pred1));

    EXPECT_EQ(selected_size, 1);
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 4);
}

template <PrimitiveType T, PredicateType PT>
void single_column_predicate_test_func(const std::pair<WrapperField*, WrapperField*>& statistic,
                                       typename PrimitiveTypeTraits<T>::CppType check_value,
                                       bool expect_match) {
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> pred(new ComparisonPredicateBase<T, PT>(col_idx, check_value));
    SingleColumnBlockPredicate single_column_block_pred(pred.get());

    bool matched = single_column_block_pred.evaluate_and(statistic);
    EXPECT_EQ(matched, expect_match);
}

// test zonemap index
TEST_F(BlockColumnPredicateTest, test_double_single_column_predicate) {
    FieldType type = FieldType::OLAP_FIELD_TYPE_DOUBLE;
    std::unique_ptr<WrapperField> min_field(WrapperField::create_by_type(type, 0));
    std::unique_ptr<WrapperField> max_field(WrapperField::create_by_type(type, 0));
    static auto constexpr nan = std::numeric_limits<double>::quiet_NaN();
    static auto constexpr neg_inf = -std::numeric_limits<double>::infinity();
    static auto constexpr pos_inf = std::numeric_limits<double>::infinity();
    static auto constexpr min = std::numeric_limits<double>::lowest();
    static auto constexpr max = std::numeric_limits<double>::max();

    // test normal value min max:
    {
        std::cout << "========test normal value min max\n";
        double zonemap_min_v = std::numeric_limits<float>::lowest();
        double zonemap_max_v = std::numeric_limits<float>::max();
        min_field->set_raw_value(&zonemap_min_v, sizeof(zonemap_min_v));
        max_field->set_raw_value(&zonemap_max_v, sizeof(zonemap_max_v));

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, pos_inf, true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, neg_inf, false);

        std::vector<double> test_values_in_range = {
                zonemap_min_v, zonemap_max_v, -123456.789012345, -0.0, 0.0, 123456.789012345,
        };
        for (auto v : test_values_in_range) {
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, true);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, v != zonemap_min_v);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, v != zonemap_max_v);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, true);
        }

        // test values out of zonemap range
        {
            double v = zonemap_min_v * 2;
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, false);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, false);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, false);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, true);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, true);
        }
        {
            double v = zonemap_max_v * 2;
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, false);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, false);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, false);
        }
    }
    // test special range: [normal, +Infinity]
    {
        std::cout << "========test special range: [normal, +Infinity]\n";
        double zonemap_min_v = std::numeric_limits<float>::lowest();
        min_field->set_raw_value(&zonemap_min_v, sizeof(zonemap_min_v));
        max_field->set_raw_value(&pos_inf, sizeof(pos_inf));

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, pos_inf, true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, neg_inf, false);

        std::vector<double> test_values_in_range = {
                zonemap_min_v, max, pos_inf, -123456.789012345, -0.0, 0.0, 123456.789012345,
        };
        for (auto v : test_values_in_range) {
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, true);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, v != zonemap_min_v);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, v != pos_inf);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, true);
        }

        // test values out of zonemap range
        {
            double v = zonemap_min_v * 2;
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, false);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, false);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, false);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, true);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, true);
        }
    }
    // test special range: [-Infinity, normal]
    {
        std::cout << "========test special range: [-Infinity, normal]\n";
        double zonemap_max_v = std::numeric_limits<float>::max();
        min_field->set_raw_value(&neg_inf, sizeof(neg_inf));
        max_field->set_raw_value(&zonemap_max_v, sizeof(zonemap_max_v));

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, pos_inf, true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, neg_inf, true);

        std::vector<double> test_values_in_range = {
                neg_inf, min, zonemap_max_v, -123456.789012345, -0.0, 0.0, 123456.789012345,
        };
        for (auto v : test_values_in_range) {
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, true);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, v != neg_inf);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, v != zonemap_max_v);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, true);
        }
        // test values out of zonemap range
        {
            double v = zonemap_max_v * 2;
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, false);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, false);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, false);
        }
    }
    // test special range: [normal, NaN]
    {
        std::cout << "========test special range: [normal, NaN]\n";
        double zonemap_min_v = std::numeric_limits<float>::lowest();
        min_field->set_raw_value(&zonemap_min_v, sizeof(zonemap_min_v));
        max_field->set_raw_value(&nan, sizeof(nan));

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, pos_inf, true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, neg_inf, false);

        std::vector<double> test_values_in_range = {
                zonemap_min_v, max, pos_inf, -123456.789012345, -0.0, 0.0, 123456.789012345,
        };
        for (auto v : test_values_in_range) {
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, true);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, v != zonemap_min_v);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, !std::isnan(v));

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, true);
        }

        // test values out of zonemap range
        {
            double v = zonemap_min_v * 2;
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, false);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, false);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, false);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, true);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, true);
        }
    }
    // test special range: [-Infinity, +Infinity]
    {
        std::cout << "========test special range: [-Infinity, +Infinity]\n";
        min_field->set_raw_value(&neg_inf, sizeof(neg_inf));
        max_field->set_raw_value(&pos_inf, sizeof(pos_inf));

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, pos_inf, true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, neg_inf, true);

        std::vector<double> test_values_in_range = {
                min, max, -123456.789012345, -0.0, 0.0, 123456.789012345,
        };
        for (auto v : test_values_in_range) {
            // test EQ
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, true);
            // test NE
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test LT
            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, v != neg_inf);

            // test LE
            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, true);

            // test GT
            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, v != pos_inf);

            // test GE
            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, true);
        }
    }
    // test special range: [-Infinity, NaN]
    {
        std::cout << "========test special range: [-Infinity, NaN]\n";
        min_field->set_raw_value(&neg_inf, sizeof(neg_inf));
        max_field->set_raw_value(&nan, sizeof(nan));

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, pos_inf, true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, neg_inf, true);

        std::vector<double> test_values_in_range = {
                min, max, -123456.789012345, -0.0, 0.0, 123456.789012345,
        };
        for (auto v : test_values_in_range) {
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, true);
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, true);

            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, true);

            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, true);

            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, true);
        }
    }
    // test special range: [-Infinity, -Infinity]
    {
        std::cout << "========test special range: [-Infinity, -Infinity]\n";
        min_field->set_raw_value(&neg_inf, sizeof(neg_inf));
        max_field->set_raw_value(&neg_inf, sizeof(neg_inf));

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, pos_inf, true);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, neg_inf, true);

        std::vector<double> test_values_not_in_range = {
                min, max, -123456.789012345, -0.0, 0.0, 123456.789012345,
        };
        for (auto v : test_values_not_in_range) {
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, false);
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, true);

            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, true);

            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, false);

            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, false);
        }
    }
    // test special range: [+Infinity, +Infinity]
    {
        std::cout << "========test special range: [+Infinity, +Infinity]\n";
        min_field->set_raw_value(&pos_inf, sizeof(pos_inf));
        max_field->set_raw_value(&pos_inf, sizeof(pos_inf));

        // test NaN
        std::cout << "========test NaN\n";
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, nan, true);

        // test +Infinity
        std::cout << "========test +Infinity\n";
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, pos_inf, true);

        // test -Infinity
        std::cout << "========test -Infinity\n";
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, neg_inf, false);

        std::cout << "========test values not in range\n";
        std::vector<double> test_values_not_in_range = {
                min, max, -123456.789012345, -0.0, 0.0, 123456.789012345,
        };
        for (auto v : test_values_not_in_range) {
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, false);
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, false);

            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, false);

            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, true);

            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, true);
        }
    }
    // test special range: [NaN, NaN]
    {
        std::cout << "========test special range: [NaN, NaN]\n";
        min_field->set_raw_value(&nan, sizeof(nan));
        max_field->set_raw_value(&nan, sizeof(nan));

        // test NaN
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, nan, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, nan, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, nan, true);

        // test +Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, pos_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, pos_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, pos_inf, false);

        // test -Infinity
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                {min_field.get(), max_field.get()}, neg_inf, true);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                {min_field.get(), max_field.get()}, neg_inf, false);
        single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                {min_field.get(), max_field.get()}, neg_inf, false);

        std::vector<double> test_values_not_in_range = {
                min, max, -123456.789012345, -0.0, 0.0, 123456.789012345,
        };
        for (auto v : test_values_not_in_range) {
            // std::cout << "test double EQ value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::EQ>(
                    {min_field.get(), max_field.get()}, v, false);
            // std::cout << "test double NE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::NE>(
                    {min_field.get(), max_field.get()}, v, true);

            // std::cout << "test double LT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LT>(
                    {min_field.get(), max_field.get()}, v, false);

            // std::cout << "test double LE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::LE>(
                    {min_field.get(), max_field.get()}, v, false);

            // std::cout << "test double GT value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GT>(
                    {min_field.get(), max_field.get()}, v, true);

            // std::cout << "test double GE value: " << v << std::endl;
            single_column_predicate_test_func<TYPE_DOUBLE, PredicateType::GE>(
                    {min_field.get(), max_field.get()}, v, true);
        }
    }
}

TEST_F(BlockColumnPredicateTest, PARQUET_COMPARISON_PREDICATE) {
    { // INT
     {// EQ
      int value = 5;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, value));
    SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
    int value = 5;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, value));
    SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
    int value = 5;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::GE>(col_idx, value));
    SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
    int value = 5;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::LE>(col_idx, value));
    SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
        float value = 5.0;
        int col_idx = 0;
        std::unique_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_FLOAT, PredicateType::EQ>(col_idx, value));
        SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
        float value = 5;
        int col_idx = 0;
        std::unique_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_FLOAT, PredicateType::NE>(col_idx, value));
        SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
        float value = 5.0;
        int col_idx = 0;
        std::unique_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_FLOAT, PredicateType::GE>(col_idx, value));
        SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
        float value = 5.0;
        int col_idx = 0;
        std::unique_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_FLOAT, PredicateType::LE>(col_idx, value));
        SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
            std::unique_ptr<ColumnPredicate> pred(
                    new InListPredicateBase<TYPE_INT, PredicateType::IN_LIST,
                                            HybridSet<PrimitiveType::TYPE_INT>>(col_idx,
                                                                                hybrid_set));
            SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
            std::unique_ptr<ColumnPredicate> pred(
                    new InListPredicateBase<TYPE_INT, PredicateType::IN_LIST,
                                            HybridSet<PrimitiveType::TYPE_INT>>(col_idx,
                                                                                hybrid_set));
            SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
    const int value = 42;
    const int col_idx = 0;
    std::unique_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, value));
    SingleColumnBlockPredicate single_column_block_pred(pred.get());

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
                    current_stat->encoded_min_value = encode_value(value);
                    current_stat->encoded_max_value = encode_value(value);
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
                        bloom->add_bytes(reinterpret_cast<const char*>(&value), sizeof(value));
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
                    current_stat->encoded_min_value = encode_value(value);
                    current_stat->encoded_max_value = encode_value(value);
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
                        int other_value = value + 10;
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
                    current_stat->encoded_min_value = encode_value(value);
                    current_stat->encoded_max_value = encode_value(value);
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
                    int min_value = value + 5;
                    int max_value = value + 10;
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
    std::unique_ptr<ColumnPredicate> pred(
            new InListPredicateBase<TYPE_INT, PredicateType::IN_LIST,
                                    HybridSet<PrimitiveType::TYPE_INT>>(col_idx, hybrid_set));
    SingleColumnBlockPredicate single_column_block_pred(pred.get());

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
        std::unique_ptr<ColumnPredicate> pred(new NullPredicate(col_idx, true));
        SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
        std::unique_ptr<ColumnPredicate> pred(new NullPredicate(col_idx, false));
        SingleColumnBlockPredicate single_column_block_pred(pred.get());
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
        int value = 5;
        std::unique_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, value));
        true_predicate = std::make_unique<SingleColumnBlockPredicate>(pred.get());

        std::unique_ptr<SingleColumnBlockPredicate> false_predicate;
        std::unique_ptr<ColumnPredicate> pred2(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, value));
        false_predicate = std::make_unique<SingleColumnBlockPredicate>(pred2.get());

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
        int value = 5;
        std::unique_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, value));
        true_predicate = std::make_unique<SingleColumnBlockPredicate>(pred.get());

        std::unique_ptr<SingleColumnBlockPredicate> true_predicate2;
        std::unique_ptr<ColumnPredicate> pred2(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, value));
        true_predicate2 = std::make_unique<SingleColumnBlockPredicate>(pred2.get());

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
        int value = 5;
        std::unique_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::EQ>(col_idx, value));
        true_predicate = std::make_unique<SingleColumnBlockPredicate>(pred.get());

        std::unique_ptr<SingleColumnBlockPredicate> false_predicate;
        std::unique_ptr<ColumnPredicate> pred2(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, value));
        false_predicate = std::make_unique<SingleColumnBlockPredicate>(pred2.get());

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
        int value = 5;
        std::unique_ptr<ColumnPredicate> pred(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, value));
        false_predicate2 = std::make_unique<SingleColumnBlockPredicate>(pred.get());

        std::unique_ptr<SingleColumnBlockPredicate> false_predicate;
        std::unique_ptr<ColumnPredicate> pred2(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, value));
        false_predicate = std::make_unique<SingleColumnBlockPredicate>(pred2.get());

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
        int value = 5;
        std::unique_ptr<SingleColumnBlockPredicate> false_predicate;
        std::unique_ptr<ColumnPredicate> pred2(
                new ComparisonPredicateBase<TYPE_INT, PredicateType::NE>(col_idx, value));
        false_predicate = std::make_unique<SingleColumnBlockPredicate>(pred2.get());

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
