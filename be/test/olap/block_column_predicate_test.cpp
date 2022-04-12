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

#include <google/protobuf/stubs/common.h>
#include <gtest/gtest.h>

#include "olap/column_predicate.h"
#include "olap/comparison_predicate.h"
#include "olap/field.h"
#include "olap/row_block2.h"
#include "olap/wrapper_field.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"
#include "util/logging.h"
#include "vec/columns/predicate_column.h"

namespace doris {

class BlockColumnPredicateTest : public testing::Test {
public:
    BlockColumnPredicateTest() {
        _mem_tracker.reset(new MemTracker(-1));
        _mem_pool.reset(new MemPool(_mem_tracker.get()));
    }

    ~BlockColumnPredicateTest() = default;

    void SetTabletSchema(std::string name, const std::string& type, const std::string& aggregation,
                         uint32_t length, bool is_allow_null, bool is_key,
                         TabletSchema* tablet_schema) {
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

    void init_row_block(const TabletSchema* tablet_schema, int size) {
        Schema schema(*tablet_schema);
        _row_block.reset(new RowBlockV2(schema, size));
    }

    std::shared_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<RowBlockV2> _row_block;
};

TEST_F(BlockColumnPredicateTest, SINGLE_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("FLOAT_COLUMN"), "FLOAT", "REPLACE", 1, true, true, &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    float value = 5.0;

    std::unique_ptr<ColumnPredicate> pred(new EqualPredicate<float>(0, value));
    SingleColumnBlockPredicate single_column_block_pred(pred.get());

    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<float*>(col_block_view.data()) = i;
    }
    single_column_block_pred.evaluate(_row_block.get(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_FLOAT_EQ(*(float*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 5.0);
}

TEST_F(BlockColumnPredicateTest, SINGLE_COLUMN_VEC) {
    vectorized::MutableColumns block;
    block.push_back(vectorized::PredicateColumnType<int>::create());

    int value = 5;
    int rows = 10;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> pred(new EqualPredicate<int>(col_idx, value));
    SingleColumnBlockPredicate single_column_block_pred(pred.get());

    uint16_t sel_idx[rows];
    uint16_t selected_size = rows;
    block[col_idx]->reserve(rows);
    for (int i = 0; i < rows; i++) {
        int* int_ptr = &i;
        block[col_idx]->insert_data((char*)int_ptr, 0);
        sel_idx[i] = i;
    }

    single_column_block_pred.evaluate(block, sel_idx, &selected_size);
    EXPECT_EQ(selected_size, 1);
    auto* pred_col = reinterpret_cast<vectorized::PredicateColumnType<int>*>(block[col_idx].get());
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], value);
}

TEST_F(BlockColumnPredicateTest, AND_MUTI_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DOUBLE_COLUMN"), "DOUBLE", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    double less_value = 5.0;
    double great_value = 3.0;
    std::unique_ptr<ColumnPredicate> less_pred(new LessPredicate<double>(0, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(new GreaterPredicate<double>(0, great_value));
    auto single_less_pred = new SingleColumnBlockPredicate(less_pred.get());
    auto single_great_pred = new SingleColumnBlockPredicate(great_pred.get());

    AndBlockColumnPredicate and_block_column_pred;
    and_block_column_pred.add_column_predicate(single_less_pred);
    and_block_column_pred.add_column_predicate(single_great_pred);

    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<double*>(col_block_view.data()) = i;
    }
    and_block_column_pred.evaluate(_row_block.get(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 4.0);
}

TEST_F(BlockColumnPredicateTest, AND_MUTI_COLUMN_VEC) {
    vectorized::MutableColumns block;
    block.push_back(vectorized::PredicateColumnType<int>::create());

    int less_value = 5;
    int great_value = 3;
    int rows = 10;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> less_pred(new LessPredicate<int>(col_idx, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(new GreaterPredicate<int>(col_idx, great_value));
    auto single_less_pred = new SingleColumnBlockPredicate(less_pred.get());
    auto single_great_pred = new SingleColumnBlockPredicate(great_pred.get());

    AndBlockColumnPredicate and_block_column_pred;
    and_block_column_pred.add_column_predicate(single_less_pred);
    and_block_column_pred.add_column_predicate(single_great_pred);

    uint16_t sel_idx[rows];
    uint16_t selected_size = rows;
    block[col_idx]->reserve(rows);
    for (int i = 0; i < rows; i++) {
        int* int_ptr = &i;
        block[col_idx]->insert_data((char*)int_ptr, 0);
        sel_idx[i] = i;
    }

    and_block_column_pred.evaluate(block, sel_idx, &selected_size);
    EXPECT_EQ(selected_size, 1);
    auto* pred_col = reinterpret_cast<vectorized::PredicateColumnType<int>*>(block[col_idx].get());
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 4);
}

TEST_F(BlockColumnPredicateTest, OR_MUTI_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DOUBLE_COLUMN"), "DOUBLE", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    double less_value = 5.0;
    double great_value = 3.0;
    std::unique_ptr<ColumnPredicate> less_pred(new LessPredicate<double>(0, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(new GreaterPredicate<double>(0, great_value));
    auto single_less_pred = new SingleColumnBlockPredicate(less_pred.get());
    auto single_great_pred = new SingleColumnBlockPredicate(great_pred.get());

    OrBlockColumnPredicate or_block_column_pred;
    or_block_column_pred.add_column_predicate(single_less_pred);
    or_block_column_pred.add_column_predicate(single_great_pred);

    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<double*>(col_block_view.data()) = i;
    }
    or_block_column_pred.evaluate(_row_block.get(), &select_size);
    EXPECT_EQ(select_size, 10);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 0.0);
}

TEST_F(BlockColumnPredicateTest, OR_MUTI_COLUMN_VEC) {
    vectorized::MutableColumns block;
    block.push_back(vectorized::PredicateColumnType<int>::create());

    int less_value = 5;
    int great_value = 3;
    int rows = 10;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> less_pred(new LessPredicate<int>(col_idx, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(new GreaterPredicate<int>(col_idx, great_value));
    auto single_less_pred = new SingleColumnBlockPredicate(less_pred.get());
    auto single_great_pred = new SingleColumnBlockPredicate(great_pred.get());

    OrBlockColumnPredicate or_block_column_pred;
    or_block_column_pred.add_column_predicate(single_less_pred);
    or_block_column_pred.add_column_predicate(single_great_pred);

    uint16_t sel_idx[rows];
    uint16_t selected_size = rows;
    block[col_idx]->reserve(rows);
    for (int i = 0; i < rows; i++) {
        int* int_ptr = &i;
        block[col_idx]->insert_data((char*)int_ptr, 0);
        sel_idx[i] = i;
    }

    or_block_column_pred.evaluate(block, sel_idx, &selected_size);
    EXPECT_EQ(selected_size, 10);
    auto* pred_col = reinterpret_cast<vectorized::PredicateColumnType<int>*>(block[col_idx].get());
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 0);
}

TEST_F(BlockColumnPredicateTest, OR_AND_MUTI_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DOUBLE_COLUMN"), "DOUBLE", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    double less_value = 5.0;
    double great_value = 3.0;
    std::unique_ptr<ColumnPredicate> less_pred(new LessPredicate<double>(0, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(new GreaterPredicate<double>(0, great_value));
    std::unique_ptr<ColumnPredicate> less_pred1(new LessPredicate<double>(0, great_value));

    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<double*>(col_block_view.data()) = i;
    }

    // Test for and or single
    //  (column < 5 and column > 3) or column < 3
    auto and_block_column_pred = new AndBlockColumnPredicate();
    and_block_column_pred->add_column_predicate(new SingleColumnBlockPredicate(less_pred.get()));
    and_block_column_pred->add_column_predicate(new SingleColumnBlockPredicate(great_pred.get()));

    OrBlockColumnPredicate or_block_column_pred;
    or_block_column_pred.add_column_predicate(and_block_column_pred);
    or_block_column_pred.add_column_predicate(new SingleColumnBlockPredicate(less_pred1.get()));

    or_block_column_pred.evaluate(_row_block.get(), &select_size);
    EXPECT_EQ(select_size, 4);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 0.0);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[1]).cell_ptr(), 1.0);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[2]).cell_ptr(), 2.0);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[3]).cell_ptr(), 4.0);

    _row_block->clear();
    select_size = _row_block->selected_size();
    // Test for single or and
    //   column < 3 or (column < 5 and column > 3)
    auto and_block_column_pred1 = new AndBlockColumnPredicate();
    and_block_column_pred1->add_column_predicate(new SingleColumnBlockPredicate(less_pred.get()));
    and_block_column_pred1->add_column_predicate(new SingleColumnBlockPredicate(great_pred.get()));

    OrBlockColumnPredicate or_block_column_pred1;
    or_block_column_pred1.add_column_predicate(new SingleColumnBlockPredicate(less_pred1.get()));
    or_block_column_pred1.add_column_predicate(and_block_column_pred1);

    or_block_column_pred1.evaluate(_row_block.get(), &select_size);
    EXPECT_EQ(select_size, 4);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 0.0);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[1]).cell_ptr(), 1.0);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[2]).cell_ptr(), 2.0);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[3]).cell_ptr(), 4.0);
}

TEST_F(BlockColumnPredicateTest, OR_AND_MUTI_COLUMN_VEC) {
    vectorized::MutableColumns block;
    block.push_back(vectorized::PredicateColumnType<int>::create());

    int less_value = 5;
    int great_value = 3;
    int rows = 10;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> less_pred(new LessPredicate<int>(0, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(new GreaterPredicate<int>(0, great_value));
    std::unique_ptr<ColumnPredicate> less_pred1(new LessPredicate<int>(0, great_value));

    // Test for and or single
    // (column < 5 and column > 3) or column < 3
    auto and_block_column_pred = new AndBlockColumnPredicate();
    and_block_column_pred->add_column_predicate(new SingleColumnBlockPredicate(less_pred.get()));
    and_block_column_pred->add_column_predicate(new SingleColumnBlockPredicate(great_pred.get()));

    OrBlockColumnPredicate or_block_column_pred;
    or_block_column_pred.add_column_predicate(and_block_column_pred);
    or_block_column_pred.add_column_predicate(new SingleColumnBlockPredicate(less_pred1.get()));

    uint16_t sel_idx[rows];
    uint16_t selected_size = rows;
    block[col_idx]->reserve(rows);
    for (int i = 0; i < rows; i++) {
        int* int_ptr = &i;
        block[col_idx]->insert_data((char*)int_ptr, 0);
        sel_idx[i] = i;
    }

    or_block_column_pred.evaluate(block, sel_idx, &selected_size);
    EXPECT_EQ(selected_size, 4);
    auto* pred_col = reinterpret_cast<vectorized::PredicateColumnType<int>*>(block[col_idx].get());
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 0);
    EXPECT_EQ(pred_col->get_data()[sel_idx[1]], 1);
    EXPECT_EQ(pred_col->get_data()[sel_idx[2]], 2);
    EXPECT_EQ(pred_col->get_data()[sel_idx[3]], 4);

    // Test for single or and
    //  column < 3 or (column < 5 and column > 3)
    auto and_block_column_pred1 = new AndBlockColumnPredicate();
    and_block_column_pred1->add_column_predicate(new SingleColumnBlockPredicate(less_pred.get()));
    and_block_column_pred1->add_column_predicate(new SingleColumnBlockPredicate(great_pred.get()));

    OrBlockColumnPredicate or_block_column_pred1;
    or_block_column_pred1.add_column_predicate(new SingleColumnBlockPredicate(less_pred1.get()));
    or_block_column_pred1.add_column_predicate(and_block_column_pred1);

    or_block_column_pred1.evaluate(block, sel_idx, &selected_size);
    EXPECT_EQ(selected_size, 4);
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 0);
    EXPECT_EQ(pred_col->get_data()[sel_idx[1]], 1);
    EXPECT_EQ(pred_col->get_data()[sel_idx[2]], 2);
    EXPECT_EQ(pred_col->get_data()[sel_idx[3]], 4);
}

TEST_F(BlockColumnPredicateTest, AND_OR_MUTI_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DOUBLE_COLUMN"), "DOUBLE", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    double less_value = 5.0;
    double great_value = 3.0;
    std::unique_ptr<ColumnPredicate> less_pred(new LessPredicate<double>(0, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(new GreaterPredicate<double>(0, great_value));
    std::unique_ptr<ColumnPredicate> less_pred1(new LessPredicate<double>(0, great_value));

    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<double*>(col_block_view.data()) = i;
    }

    // Test for and or single
    // (column < 5 or column < 3) and column > 3
    auto or_block_column_pred = new OrBlockColumnPredicate();
    or_block_column_pred->add_column_predicate(new SingleColumnBlockPredicate(less_pred.get()));
    or_block_column_pred->add_column_predicate(new SingleColumnBlockPredicate(less_pred1.get()));

    AndBlockColumnPredicate and_block_column_pred;
    and_block_column_pred.add_column_predicate(or_block_column_pred);
    and_block_column_pred.add_column_predicate(new SingleColumnBlockPredicate(great_pred.get()));

    and_block_column_pred.evaluate(_row_block.get(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 4.0);

    _row_block->clear();
    select_size = _row_block->selected_size();
    // Test for single or and
    // column > 3 and (column < 5 or column < 3)
    auto or_block_column_pred1 = new OrBlockColumnPredicate();
    or_block_column_pred1->add_column_predicate(new SingleColumnBlockPredicate(less_pred.get()));
    or_block_column_pred1->add_column_predicate(new SingleColumnBlockPredicate(less_pred1.get()));

    AndBlockColumnPredicate and_block_column_pred1;
    and_block_column_pred1.add_column_predicate(new SingleColumnBlockPredicate(great_pred.get()));
    and_block_column_pred1.add_column_predicate(or_block_column_pred1);

    and_block_column_pred1.evaluate(_row_block.get(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 4.0);
}

TEST_F(BlockColumnPredicateTest, AND_OR_MUTI_COLUMN_VEC) {
    vectorized::MutableColumns block;
    block.push_back(vectorized::PredicateColumnType<int>::create());

    int less_value = 5;
    int great_value = 3;
    int rows = 10;
    int col_idx = 0;
    std::unique_ptr<ColumnPredicate> less_pred(new LessPredicate<int>(0, less_value));
    std::unique_ptr<ColumnPredicate> great_pred(new GreaterPredicate<int>(0, great_value));
    std::unique_ptr<ColumnPredicate> less_pred1(new LessPredicate<int>(0, great_value));

    // Test for and or single
    // (column < 5 or column < 3) and column > 3
    auto or_block_column_pred = new OrBlockColumnPredicate();
    or_block_column_pred->add_column_predicate(new SingleColumnBlockPredicate(less_pred.get()));
    or_block_column_pred->add_column_predicate(new SingleColumnBlockPredicate(less_pred1.get()));

    AndBlockColumnPredicate and_block_column_pred;
    and_block_column_pred.add_column_predicate(or_block_column_pred);
    and_block_column_pred.add_column_predicate(new SingleColumnBlockPredicate(great_pred.get()));

    uint16_t sel_idx[rows];
    uint16_t selected_size = rows;
    block[col_idx]->reserve(rows);
    for (int i = 0; i < rows; i++) {
        int* int_ptr = &i;
        block[col_idx]->insert_data((char*)int_ptr, 0);
        sel_idx[i] = i;
    }

    and_block_column_pred.evaluate(block, sel_idx, &selected_size);

    auto* pred_col = reinterpret_cast<vectorized::PredicateColumnType<int>*>(block[col_idx].get());
    EXPECT_EQ(selected_size, 1);
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 4);

    // Test for single or and
    // column > 3 and (column < 5 or column < 3)
    auto or_block_column_pred1 = new OrBlockColumnPredicate();
    or_block_column_pred1->add_column_predicate(new SingleColumnBlockPredicate(less_pred.get()));
    or_block_column_pred1->add_column_predicate(new SingleColumnBlockPredicate(less_pred1.get()));

    AndBlockColumnPredicate and_block_column_pred1;
    and_block_column_pred1.add_column_predicate(new SingleColumnBlockPredicate(great_pred.get()));
    and_block_column_pred1.add_column_predicate(or_block_column_pred1);

    EXPECT_EQ(selected_size, 1);
    EXPECT_EQ(pred_col->get_data()[sel_idx[0]], 4);
}

} // namespace doris
