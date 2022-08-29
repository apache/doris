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

#include "olap/null_predicate.h"

#include <google/protobuf/stubs/common.h>
#include <gtest/gtest.h>
#include <time.h>

#include "olap/column_predicate.h"
#include "olap/field.h"
#include "olap/row_block2.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.hpp"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"

using namespace doris::vectorized;

namespace doris {

namespace datetime {

static uint24_t to_date_timestamp(const char* date_string) {
    tm time_tm;
    strptime(date_string, "%Y-%m-%d", &time_tm);

    int value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    return uint24_t(value);
}

static uint32_t to_date_v2_timestamp(const char* date_string) {
    tm time_tm;
    strptime(date_string, "%Y-%m-%d", &time_tm);

    return ((time_tm.tm_year + 1900) << 9) | ((time_tm.tm_mon + 1) << 5) | time_tm.tm_mday;
}

}; // namespace datetime

class TestNullPredicate : public testing::Test {
public:
    TestNullPredicate() : _row_block(nullptr) { _mem_pool.reset(new MemPool()); }

    ~TestNullPredicate() {}

    void SetTabletSchema(std::string name, std::string type, std::string aggregation,
                         uint32_t length, bool is_allow_null, bool is_key,
                         TabletSchemaSPtr tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        static int id = 0;
        ColumnPB* column = tablet_schema_pb.add_column();
        ;
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
        _schema = std::make_unique<Schema>(tablet_schema);
        _row_block.reset(new RowBlockV2(*_schema, size));
    }

    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<RowBlockV2> _row_block;
    std::unique_ptr<Schema> _schema;
};

#define TEST_IN_LIST_PREDICATE(TYPE, TYPE_NAME, FIELD_TYPE)                                      \
    TEST_F(TestNullPredicate, TYPE_NAME##_COLUMN) {                                              \
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();                       \
        SetTabletSchema(std::string("TYPE_NAME##_COLUMN"), FIELD_TYPE, "REPLACE", 1, true, true, \
                        tablet_schema);                                                          \
        int size = 10;                                                                           \
        std::vector<uint32_t> return_columns;                                                    \
        for (int i = 0; i < tablet_schema->num_columns(); ++i) {                                 \
            return_columns.push_back(i);                                                         \
        }                                                                                        \
        std::unique_ptr<ColumnPredicate> pred(new NullPredicate(0, true));                       \
                                                                                                 \
        /* for ColumnBlock nulls */                                                              \
        init_row_block(tablet_schema, size);                                                     \
        ColumnBlock col_block = _row_block->column_block(0);                                     \
        auto select_size = _row_block->selected_size();                                          \
        ColumnBlockView col_block_view(&col_block);                                              \
        for (int i = 0; i < size; ++i, col_block_view.advance(1)) {                              \
            col_block_view.set_null_bits(1, false);                                              \
            *reinterpret_cast<TYPE*>(col_block_view.data()) = i;                                 \
        }                                                                                        \
        pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);                \
        EXPECT_EQ(select_size, 0);                                                               \
                                                                                                 \
        /* for vectorized::Block no null */                                                      \
        _row_block->clear();                                                                     \
        select_size = _row_block->selected_size();                                               \
        vectorized::Block vec_block = tablet_schema->create_block(return_columns);               \
        _row_block->convert_to_vec_block(&vec_block);                                            \
        ColumnPtr vec_col = vec_block.get_columns()[0];                                          \
        select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),          \
                                     _row_block->selection_vector(), select_size);               \
        EXPECT_EQ(select_size, 0);                                                               \
                                                                                                 \
        /* for ColumnBlock has nulls */                                                          \
        col_block_view = ColumnBlockView(&col_block);                                            \
        for (int i = 0; i < size; ++i, col_block_view.advance(1)) {                              \
            if (i % 2 == 0) {                                                                    \
                col_block_view.set_null_bits(1, true);                                           \
            } else {                                                                             \
                col_block_view.set_null_bits(1, false);                                          \
                *reinterpret_cast<TYPE*>(col_block_view.data()) = i;                             \
            }                                                                                    \
        }                                                                                        \
        _row_block->clear();                                                                     \
        select_size = _row_block->selected_size();                                               \
        pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);                \
        EXPECT_EQ(select_size, 5);                                                               \
                                                                                                 \
        /* for vectorized::Block has nulls */                                                    \
        _row_block->clear();                                                                     \
        select_size = _row_block->selected_size();                                               \
        vec_block = tablet_schema->create_block(return_columns);                                 \
        _row_block->convert_to_vec_block(&vec_block);                                            \
        vec_col = vec_block.get_columns()[0];                                                    \
        select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),          \
                                     _row_block->selection_vector(), select_size);               \
        EXPECT_EQ(select_size, 5);                                                               \
        pred.reset();                                                                            \
    }

TEST_IN_LIST_PREDICATE(int8_t, TINYINT, "TINYINT")
TEST_IN_LIST_PREDICATE(int16_t, SMALLINT, "SMALLINT")
TEST_IN_LIST_PREDICATE(int32_t, INT, "INT")
TEST_IN_LIST_PREDICATE(int64_t, BIGINT, "BIGINT")
TEST_IN_LIST_PREDICATE(int128_t, LARGEINT, "LARGEINT")

TEST_F(TestNullPredicate, FLOAT_COLUMN) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("FLOAT_COLUMN"), "FLOAT", "REPLACE", 1, true, true, tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    std::unique_ptr<ColumnPredicate> pred(new NullPredicate(0, true));

    // for ColumnBlock no nulls
    init_row_block(tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<float*>(col_block_view.data()) = i + 0.1;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 0);

    // for vectorized::Block no null
    _row_block->clear();
    select_size = _row_block->selected_size();
    vectorized::Block vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    ColumnPtr vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 0);

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
    EXPECT_EQ(select_size, 5);

    // for vectorized::Block has nulls
    _row_block->clear();
    select_size = _row_block->selected_size();
    vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 5);
}

TEST_F(TestNullPredicate, DOUBLE_COLUMN) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DOUBLE_COLUMN"), "DOUBLE", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    std::unique_ptr<ColumnPredicate> pred(new NullPredicate(0, true));

    // for ColumnBlock no nulls
    init_row_block(tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<double*>(col_block_view.data()) = i + 0.1;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 0);

    // for vectorized::Block no null
    _row_block->clear();
    select_size = _row_block->selected_size();
    vectorized::Block vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    ColumnPtr vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 0);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            *reinterpret_cast<double*>(col_block_view.data()) = i + 0.1;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 5);

    // for vectorized::Block has nulls
    _row_block->clear();
    select_size = _row_block->selected_size();
    vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 5);
}

TEST_F(TestNullPredicate, DECIMAL_COLUMN) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DECIMAL_COLUMN"), "DECIMAL", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    std::unique_ptr<ColumnPredicate> pred(new NullPredicate(0, true));

    // for ColumnBlock no nulls
    init_row_block(tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        reinterpret_cast<decimal12_t*>(col_block_view.data())->integer = i;
        reinterpret_cast<decimal12_t*>(col_block_view.data())->fraction = i;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 0);

    // for vectorized::Block no null
    _row_block->clear();
    select_size = _row_block->selected_size();
    vectorized::Block vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    ColumnPtr vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 0);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 3 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            reinterpret_cast<decimal12_t*>(col_block_view.data())->integer = i;
            reinterpret_cast<decimal12_t*>(col_block_view.data())->fraction = i;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 4);

    // for vectorized::Block has nulls
    _row_block->clear();
    select_size = _row_block->selected_size();
    vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 4);
}

TEST_F(TestNullPredicate, STRING_COLUMN) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("STRING_COLUMN"), "VARCHAR", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    std::unique_ptr<ColumnPredicate> pred(new NullPredicate(0, true));

    // for ColumnBlock no nulls
    init_row_block(tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);

    char* string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(55));
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        for (int j = 0; j <= i; ++j) {
            string_buffer[j] = 'a' + i;
        }
        reinterpret_cast<StringValue*>(col_block_view.data())->len = i + 1;
        reinterpret_cast<StringValue*>(col_block_view.data())->ptr = string_buffer;
        string_buffer += i + 1;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 0);

    // for vectorized::Block no null
    _row_block->clear();
    select_size = _row_block->selected_size();
    vectorized::Block vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    ColumnPtr vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 0);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(55));
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 3 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            for (int j = 0; j <= i; ++j) {
                string_buffer[j] = 'a' + i;
            }
            reinterpret_cast<StringValue*>(col_block_view.data())->len = i + 1;
            reinterpret_cast<StringValue*>(col_block_view.data())->ptr = string_buffer;
            string_buffer += i + 1;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 4);

    // for vectorized::Block has nulls
    _row_block->clear();
    select_size = _row_block->selected_size();
    vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 4);
}

TEST_F(TestNullPredicate, DATE_COLUMN) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DATE_COLUMN"), "DATE", "REPLACE", 1, true, true, tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    std::unique_ptr<ColumnPredicate> pred(new NullPredicate(0, true));

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07");
    date_array.push_back("2017-09-08");
    date_array.push_back("2017-09-09");
    date_array.push_back("2017-09-10");
    date_array.push_back("2017-09-11");
    date_array.push_back("2017-09-12");

    // for ColumnBlock no nulls
    init_row_block(tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        uint24_t timestamp = datetime::to_date_timestamp(date_array[i].c_str());
        *reinterpret_cast<uint24_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 0);

    // for vectorized::Block no null
    _row_block->clear();
    select_size = _row_block->selected_size();
    vectorized::Block vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    ColumnPtr vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 0);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 3 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            uint24_t timestamp = datetime::to_date_timestamp(date_array[i].c_str());
            *reinterpret_cast<uint24_t*>(col_block_view.data()) = timestamp;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 2);

    // for vectorized::Block has nulls
    _row_block->clear();
    select_size = _row_block->selected_size();
    vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 2);
}

TEST_F(TestNullPredicate, DATETIME_COLUMN) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DATETIME_COLUMN"), "DATETIME", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    std::unique_ptr<ColumnPredicate> pred(new NullPredicate(0, true));

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07");
    date_array.push_back("2017-09-08");
    date_array.push_back("2017-09-09");
    date_array.push_back("2017-09-10");
    date_array.push_back("2017-09-11");
    date_array.push_back("2017-09-12");

    // for ColumnBlock no nulls
    init_row_block(tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        uint64_t timestamp = datetime::to_date_timestamp(date_array[i].c_str());
        *reinterpret_cast<uint64_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 0);

    // for vectorized::Block no null
    _row_block->clear();
    select_size = _row_block->selected_size();
    vectorized::Block vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    ColumnPtr vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 0);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 3 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            uint64_t timestamp = datetime::to_date_timestamp(date_array[i].c_str());
            *reinterpret_cast<uint64_t*>(col_block_view.data()) = timestamp;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 2);

    // for vectorized::Block has nulls
    _row_block->clear();
    select_size = _row_block->selected_size();
    vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 2);
}

TEST_F(TestNullPredicate, DATEV2_COLUMN) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DATEV2_COLUMN"), "DATEV2", "REPLACE", 4, true, true,
                    tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    std::unique_ptr<ColumnPredicate> pred(new NullPredicate(0, true));

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07");
    date_array.push_back("2017-09-08");
    date_array.push_back("2017-09-09");
    date_array.push_back("2017-09-10");
    date_array.push_back("2017-09-11");
    date_array.push_back("2017-09-12");

    // for ColumnBlock no nulls
    init_row_block(tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        uint32_t timestamp = datetime::to_date_v2_timestamp(date_array[i].c_str());
        *reinterpret_cast<uint32_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 0);

    // for vectorized::Block no null
    _row_block->clear();
    select_size = _row_block->selected_size();
    vectorized::Block vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    ColumnPtr vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 0);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 3 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            uint32_t timestamp = datetime::to_date_v2_timestamp(date_array[i].c_str());
            *reinterpret_cast<uint32_t*>(col_block_view.data()) = timestamp;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 2);

    // for vectorized::Block has nulls
    _row_block->clear();
    select_size = _row_block->selected_size();
    vec_block = tablet_schema->create_block(return_columns);
    _row_block->convert_to_vec_block(&vec_block);
    vec_col = vec_block.get_columns()[0];
    select_size = pred->evaluate(const_cast<doris::vectorized::IColumn&>(*vec_col),
                                 _row_block->selection_vector(), select_size);
    EXPECT_EQ(select_size, 2);
}

} // namespace doris
