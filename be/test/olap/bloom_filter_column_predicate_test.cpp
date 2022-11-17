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

#include <google/protobuf/stubs/common.h>
#include <gtest/gtest.h>
#include <time.h>

#include "agent/be_exec_version_manager.h"
#include "exprs/create_predicate_function.h"
#include "olap/column_predicate.h"
#include "olap/predicate_creator.h"
#include "olap/row_block2.h"
#include "runtime/mem_pool.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/predicate_column.h"

using namespace doris::vectorized;

namespace doris {

class TestBloomFilterColumnPredicate : public testing::Test {
public:
    TestBloomFilterColumnPredicate() : _row_block(nullptr) { _mem_pool.reset(new MemPool()); }

    ~TestBloomFilterColumnPredicate() {}

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

    void init_row_block(TabletSchemaSPtr tablet_schema, int size) {
        Schema schema(tablet_schema);
        _row_block.reset(new RowBlockV2(schema, size));
    }

    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<RowBlockV2> _row_block;
};

TEST_F(TestBloomFilterColumnPredicate, FLOAT_COLUMN) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("FLOAT_COLUMN"), "FLOAT", "REPLACE", 1, true, true, tablet_schema);
    const int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }

    std::shared_ptr<BloomFilterFuncBase> bloom_filter(
            create_bloom_filter(PrimitiveType::TYPE_FLOAT));

    bloom_filter->init(4096, 0.05);
    float value = 4.1;
    bloom_filter->insert(reinterpret_cast<void*>(&value));
    value = 5.1;
    bloom_filter->insert(reinterpret_cast<void*>(&value));
    value = 6.1;
    bloom_filter->insert(reinterpret_cast<void*>(&value));
    ColumnPredicate* pred = create_column_predicate(0, bloom_filter, OLAP_FIELD_TYPE_FLOAT,
                                                    BeExecVersionManager::get_newest_version());

    // for ColumnBlock no null
    init_row_block(tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<float*>(col_block_view.data()) = i + 0.1f;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 3);
    EXPECT_FLOAT_EQ(*(float*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 4.1);
    EXPECT_FLOAT_EQ(*(float*)col_block.cell(_row_block->selection_vector()[1]).cell_ptr(), 5.1);
    EXPECT_FLOAT_EQ(*(float*)col_block.cell(_row_block->selection_vector()[2]).cell_ptr(), 6.1);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            *reinterpret_cast<float*>(col_block_view.data()) = i + 0.1;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_FLOAT_EQ(*(float*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 5.1);

    delete pred;
}

TEST_F(TestBloomFilterColumnPredicate, FLOAT_COLUMN_VEC) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("FLOAT_COLUMN"), "FLOAT", "REPLACE", 1, true, true, tablet_schema);
    const int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }

    std::shared_ptr<BloomFilterFuncBase> bloom_filter(
            create_bloom_filter(PrimitiveType::TYPE_FLOAT));

    bloom_filter->init(4096, 0.05);
    auto column_data = ColumnFloat32::create();
    float values[3] = {4.1, 5.1, 6.1};
    int offsets[3] = {0, 1, 2};

    bloom_filter->insert_fixed_len((char*)values, offsets, 3);
    ColumnPredicate* pred = create_column_predicate(0, bloom_filter, OLAP_FIELD_TYPE_FLOAT,
                                                    BeExecVersionManager::get_newest_version());
    auto* col_data = reinterpret_cast<float*>(_mem_pool->allocate(size * sizeof(float)));

    // for vectorized::Block no null
    auto pred_col = PredicateColumnType<TYPE_FLOAT>::create();
    pred_col->reserve(size);
    for (int i = 0; i < size; ++i) {
        *(col_data + i) = i + 0.1f;
        pred_col->insert_data(reinterpret_cast<const char*>(col_data + i), 0);
    }
    init_row_block(tablet_schema, size);
    _row_block->clear();
    auto select_size = _row_block->selected_size();
    select_size = pred->evaluate(*pred_col, _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 3);
    EXPECT_FLOAT_EQ((float)pred_col->get_data()[_row_block->selection_vector()[0]], 4.1);
    EXPECT_FLOAT_EQ((float)pred_col->get_data()[_row_block->selection_vector()[1]], 5.1);
    EXPECT_FLOAT_EQ((float)pred_col->get_data()[_row_block->selection_vector()[2]], 6.1);

    // for vectorized::Block has nulls
    auto null_map = ColumnUInt8::create(size, 0);
    auto& null_map_data = null_map->get_data();
    for (int i = 0; i < size; ++i) {
        null_map_data[i] = (i % 2 == 0);
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    auto nullable_col =
            vectorized::ColumnNullable::create(std::move(pred_col), std::move(null_map));
    select_size = pred->evaluate(*nullable_col, _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 1);
    auto nested_col = check_and_get_column<PredicateColumnType<TYPE_FLOAT>>(
            nullable_col->get_nested_column());
    EXPECT_FLOAT_EQ((float)nested_col->get_data()[_row_block->selection_vector()[0]], 5.1);

    delete pred;
}
} // namespace doris
