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
#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "olap/column_predicate.h"
#include "olap/comparison_predicate.h"
#include "olap/tablet_schema.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/predicate_column.h"

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

} // namespace doris
