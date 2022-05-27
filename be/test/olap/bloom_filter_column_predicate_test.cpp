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

#include "exprs/create_predicate_function.h"
#include "olap/bloom_filter_predicate.h"
#include "olap/column_predicate.h"
#include "olap/field.h"
#include "olap/row_block2.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"
#include "util/logging.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/predicate_column.h"
#include "vec/core/block.h"

using namespace doris::vectorized;

namespace doris {

class TestBloomFilterColumnPredicate : public testing::Test {
public:
    TestBloomFilterColumnPredicate() : _vectorized_batch(nullptr), _row_block(nullptr) {
        _mem_tracker.reset(new MemTracker(-1));
        _mem_pool.reset(new MemPool(_mem_tracker.get()));
    }

    ~TestBloomFilterColumnPredicate() {
        if (_vectorized_batch != nullptr) {
            delete _vectorized_batch;
        }
    }

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

    void InitVectorizedBatch(const TabletSchema* tablet_schema, const std::vector<uint32_t>& ids,
                             int size) {
        _vectorized_batch = new VectorizedRowBatch(tablet_schema, ids, size);
        _vectorized_batch->set_size(size);
    }

    void init_row_block(const TabletSchema* tablet_schema, int size) {
        Schema schema(*tablet_schema);
        _row_block.reset(new RowBlockV2(schema, size));
    }

    std::shared_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    VectorizedRowBatch* _vectorized_batch;
    std::unique_ptr<RowBlockV2> _row_block;
};

TEST_F(TestBloomFilterColumnPredicate, FLOAT_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("FLOAT_COLUMN"), "FLOAT", "REPLACE", 1, true, true, &tablet_schema);
    const int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }

    std::shared_ptr<IBloomFilterFuncBase> bloom_filter(
            create_bloom_filter(PrimitiveType::TYPE_FLOAT));

    bloom_filter->init(4096, 0.05);
    float value = 4.1;
    bloom_filter->insert(reinterpret_cast<void*>(&value));
    value = 5.1;
    bloom_filter->insert(reinterpret_cast<void*>(&value));
    value = 6.1;
    bloom_filter->insert(reinterpret_cast<void*>(&value));
    ColumnPredicate* pred = BloomFilterColumnPredicateFactory::create_column_predicate(
            0, bloom_filter, OLAP_FIELD_TYPE_FLOAT);

    // for VectorizedBatch no null
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    auto* col_data = reinterpret_cast<float*>(_mem_pool->allocate(size * sizeof(float)));
    col_vector->set_col_data(col_data);
    for (int i = 0; i < size; ++i) {
        *(col_data + i) = i + 0.1f;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 10);
    uint16_t* sel = _vectorized_batch->selected();
    EXPECT_FLOAT_EQ(*(col_data + sel[0]), 0.1);
    EXPECT_FLOAT_EQ(*(col_data + sel[1]), 1.1);
    EXPECT_FLOAT_EQ(*(col_data + sel[2]), 2.1);

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
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

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            *(col_data + i) = i + 0.1;
        }
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 10);
    sel = _vectorized_batch->selected();
    EXPECT_FLOAT_EQ(*(col_data + sel[0]), 0.1);
    EXPECT_FLOAT_EQ(*(col_data + sel[1]), 1.1);
    EXPECT_FLOAT_EQ(*(col_data + sel[2]), 2.1);

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

    // for vectorized::Block no null
    auto pred_col = PredicateColumnType<vectorized::Float32>::create();
    pred_col->reserve(size);
    for (int i = 0; i < size; ++i) {
        *(col_data + i) = i + 0.1f;
        pred_col->insert_data(reinterpret_cast<const char*>(col_data + i), 0);
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(*pred_col, _row_block->selection_vector(), &select_size);
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
    pred->evaluate(*nullable_col, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    auto nested_col = check_and_get_column<PredicateColumnType<vectorized::Float32>>(
            nullable_col->get_nested_column());
    EXPECT_FLOAT_EQ((float)nested_col->get_data()[_row_block->selection_vector()[0]], 5.1);

    delete pred;
}

} // namespace doris
