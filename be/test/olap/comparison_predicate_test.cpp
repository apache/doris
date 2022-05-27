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

#include "olap/comparison_predicate.h"

#include <google/protobuf/stubs/common.h>
#include <gtest/gtest.h>
#include <time.h>

#include "olap/column_predicate.h"
#include "olap/field.h"
#include "olap/row_block2.h"
#include "olap/wrapper_field.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"
#include "util/logging.h"

namespace doris {

namespace datetime {

static uint24_t to_date_timestamp(const char* date_string) {
    tm time_tm;
    strptime(date_string, "%Y-%m-%d", &time_tm);

    int value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    return uint24_t(value);
}

static uint64_t to_datetime_timestamp(const std::string& value_string) {
    tm time_tm;
    strptime(value_string.c_str(), "%Y-%m-%d %H:%M:%S", &time_tm);

    uint64_t value =
            ((time_tm.tm_year + 1900) * 10000L + (time_tm.tm_mon + 1) * 100L + time_tm.tm_mday) *
                    1000000L +
            time_tm.tm_hour * 10000L + time_tm.tm_min * 100L + time_tm.tm_sec;

    return value;
}

static std::string to_date_string(uint24_t& date_value) {
    tm time_tm;
    int value = date_value;
    memset(&time_tm, 0, sizeof(time_tm));
    time_tm.tm_mday = static_cast<int>(value & 31);
    time_tm.tm_mon = static_cast<int>(value >> 5 & 15) - 1;
    time_tm.tm_year = static_cast<int>(value >> 9) - 1900;
    char buf[20] = {'\0'};
    strftime(buf, sizeof(buf), "%Y-%m-%d", &time_tm);
    return std::string(buf);
}

static std::string to_datetime_string(uint64_t& datetime_value) {
    tm time_tm;
    int64_t part1 = (datetime_value / 1000000L);
    int64_t part2 = (datetime_value - part1 * 1000000L);

    time_tm.tm_year = static_cast<int>((part1 / 10000L) % 10000) - 1900;
    time_tm.tm_mon = static_cast<int>((part1 / 100) % 100) - 1;
    time_tm.tm_mday = static_cast<int>(part1 % 100);

    time_tm.tm_hour = static_cast<int>((part2 / 10000L) % 10000);
    time_tm.tm_min = static_cast<int>((part2 / 100) % 100);
    time_tm.tm_sec = static_cast<int>(part2 % 100);

    char buf[20] = {'\0'};
    strftime(buf, 20, "%Y-%m-%d %H:%M:%S", &time_tm);
    return std::string(buf);
}

}; // namespace datetime

#define TEST_PREDICATE_DEFINITION(CLASS_NAME)                                                     \
    class CLASS_NAME : public testing::Test {                                                     \
    public:                                                                                       \
        CLASS_NAME() : _vectorized_batch(nullptr) {                                               \
            _mem_tracker.reset(new MemTracker(-1));                                               \
            _mem_pool.reset(new MemPool(_mem_tracker.get()));                                     \
        }                                                                                         \
        ~CLASS_NAME() {                                                                           \
            if (_vectorized_batch != nullptr) {                                                   \
                delete _vectorized_batch;                                                         \
            }                                                                                     \
        }                                                                                         \
        void SetTabletSchema(std::string name, const std::string& type,                           \
                             const std::string& aggregation, uint32_t length, bool is_allow_null, \
                             bool is_key, TabletSchema* tablet_schema) {                          \
            TabletSchemaPB tablet_schema_pb;                                                      \
            static int id = 0;                                                                    \
            ColumnPB* column = tablet_schema_pb.add_column();                                     \
            column->set_unique_id(++id);                                                          \
            column->set_name(name);                                                               \
            column->set_type(type);                                                               \
            column->set_is_key(is_key);                                                           \
            column->set_is_nullable(is_allow_null);                                               \
            column->set_length(length);                                                           \
            column->set_aggregation(aggregation);                                                 \
            column->set_precision(1000);                                                          \
            column->set_frac(1000);                                                               \
            column->set_is_bf_column(false);                                                      \
            tablet_schema->init_from_pb(tablet_schema_pb);                                        \
        }                                                                                         \
        void InitVectorizedBatch(const TabletSchema* tablet_schema,                               \
                                 const std::vector<uint32_t>& ids, int size) {                    \
            _vectorized_batch = new VectorizedRowBatch(tablet_schema, ids, size);                 \
            _vectorized_batch->set_size(size);                                                    \
        }                                                                                         \
                                                                                                  \
        void init_row_block(const TabletSchema* tablet_schema, int size) {                        \
            Schema schema(*tablet_schema);                                                        \
            _row_block.reset(new RowBlockV2(schema, size));                                       \
        }                                                                                         \
        std::shared_ptr<MemTracker> _mem_tracker;                                                 \
        std::unique_ptr<MemPool> _mem_pool;                                                       \
        VectorizedRowBatch* _vectorized_batch;                                                    \
        std::unique_ptr<RowBlockV2> _row_block;                                                   \
    };

TEST_PREDICATE_DEFINITION(TestEqualPredicate)
TEST_PREDICATE_DEFINITION(TestLessPredicate)

#define TEST_EQUAL_PREDICATE(TYPE, TYPE_NAME, FIELD_TYPE)                                         \
    TEST_F(TestEqualPredicate, TYPE_NAME##_COLUMN) {                                              \
        TabletSchema tablet_schema;                                                               \
        SetTabletSchema(std::string("TYPE_NAME##_COLUMN"), FIELD_TYPE, "REPLACE", 1, false, true, \
                        &tablet_schema);                                                          \
        int size = 10;                                                                            \
        std::vector<uint32_t> return_columns;                                                     \
        for (int i = 0; i < tablet_schema.num_columns(); ++i) {                                   \
            return_columns.push_back(i);                                                          \
        }                                                                                         \
        InitVectorizedBatch(&tablet_schema, return_columns, size);                                \
        ColumnVector* col_vector = _vectorized_batch->column(0);                                  \
                                                                                                  \
        /* for no nulls */                                                                        \
        col_vector->set_no_nulls(true);                                                           \
        TYPE* col_data = reinterpret_cast<TYPE*>(_mem_pool->allocate(size * sizeof(TYPE)));       \
        col_vector->set_col_data(col_data);                                                       \
        for (int i = 0; i < size; ++i) {                                                          \
            *(col_data + i) = i;                                                                  \
        }                                                                                         \
        TYPE value = 5;                                                                           \
        ColumnPredicate* pred = new EqualPredicate<TYPE>(0, value);                               \
        pred->evaluate(_vectorized_batch);                                                        \
        EXPECT_EQ(_vectorized_batch->size(), 1);                                                  \
        uint16_t* sel = _vectorized_batch->selected();                                            \
        EXPECT_EQ(*(col_data + sel[0]), 5);                                                       \
                                                                                                  \
        /* for has nulls */                                                                       \
        col_vector->set_no_nulls(false);                                                          \
        bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));                       \
        memset(is_null, 0, size);                                                                 \
        col_vector->set_is_null(is_null);                                                         \
        for (int i = 0; i < size; ++i) {                                                          \
            if (i % 2 == 0) {                                                                     \
                is_null[i] = true;                                                                \
            } else {                                                                              \
                *(col_data + i) = i;                                                              \
            }                                                                                     \
        }                                                                                         \
        _vectorized_batch->set_size(size);                                                        \
        _vectorized_batch->set_selected_in_use(false);                                            \
        pred->evaluate(_vectorized_batch);                                                        \
        EXPECT_EQ(_vectorized_batch->size(), 1);                                                  \
        sel = _vectorized_batch->selected();                                                      \
        EXPECT_EQ(*(col_data + sel[0]), 5);                                                       \
        delete pred;                                                                              \
    }

TEST_EQUAL_PREDICATE(int8_t, TINYINT, "TINYINT")
TEST_EQUAL_PREDICATE(int16_t, SMALLINT, "SMALLINT")
TEST_EQUAL_PREDICATE(int32_t, INT, "INT")
TEST_EQUAL_PREDICATE(int64_t, BIGINT, "BIGINT")
TEST_EQUAL_PREDICATE(int128_t, LARGEINT, "LARGEINT")

TEST_F(TestEqualPredicate, FLOAT_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("FLOAT_COLUMN"), "FLOAT", "REPLACE", 1, true, true, &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    float value = 5.0;
    ColumnPredicate* pred = new EqualPredicate<float>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    float* col_data = reinterpret_cast<float*>(_mem_pool->allocate(size * sizeof(float)));
    col_vector->set_col_data(col_data);
    for (int i = 0; i < size; ++i) {
        *(col_data + i) = i;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    uint16_t* sel = _vectorized_batch->selected();
    EXPECT_FLOAT_EQ(*(col_data + sel[0]), 5.0);

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<float*>(col_block_view.data()) = i;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_FLOAT_EQ(*(float*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 5.0);

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            *(col_data + i) = i;
        }
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    sel = _vectorized_batch->selected();
    EXPECT_FLOAT_EQ(*(col_data + sel[0]), 5.0);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            *reinterpret_cast<float*>(col_block_view.data()) = i;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_FLOAT_EQ(*(float*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 5.0);

    delete pred;
}

TEST_F(TestEqualPredicate, DOUBLE_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DOUBLE_COLUMN"), "DOUBLE", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    double value = 5.0;
    ColumnPredicate* pred = new EqualPredicate<double>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    double* col_data = reinterpret_cast<double*>(_mem_pool->allocate(size * sizeof(double)));
    col_vector->set_col_data(col_data);
    for (int i = 0; i < size; ++i) {
        *(col_data + i) = i;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    uint16_t* sel = _vectorized_batch->selected();
    EXPECT_DOUBLE_EQ(*(col_data + sel[0]), 5.0);

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<double*>(col_block_view.data()) = i;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 5.0);

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            *(col_data + i) = i;
        }
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    sel = _vectorized_batch->selected();
    EXPECT_DOUBLE_EQ(*(col_data + sel[0]), 5.0);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            *reinterpret_cast<double*>(col_block_view.data()) = i;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 5.0);

    delete pred;
}

TEST_F(TestEqualPredicate, DECIMAL_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DECIMAL_COLUMN"), "DECIMAL", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    decimal12_t value = {5, 5};
    ColumnPredicate* pred = new EqualPredicate<decimal12_t>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    decimal12_t* col_data =
            reinterpret_cast<decimal12_t*>(_mem_pool->allocate(size * sizeof(decimal12_t)));
    col_vector->set_col_data(col_data);
    for (int i = 0; i < size; ++i) {
        (*(col_data + i)).integer = i;
        (*(col_data + i)).fraction = i;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    uint16_t* sel = _vectorized_batch->selected();
    EXPECT_EQ(*(col_data + sel[0]), value);

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        reinterpret_cast<decimal12_t*>(col_block_view.data())->integer = i;
        reinterpret_cast<decimal12_t*>(col_block_view.data())->fraction = i;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(*(decimal12_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value);

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            (*(col_data + i)).integer = i;
            (*(col_data + i)).fraction = i;
        }
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    sel = _vectorized_batch->selected();
    EXPECT_EQ(*(col_data + sel[0]), value);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
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
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(*(decimal12_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value);

    delete pred;
}

TEST_F(TestEqualPredicate, STRING_COLUMN) {
    TabletSchema char_tablet_schema;
    SetTabletSchema(std::string("STRING_COLUMN"), "CHAR", "REPLACE", 5, true, true,
                    &char_tablet_schema);
    // test WrapperField.from_string() for char type
    WrapperField* field = WrapperField::create(char_tablet_schema.column(0));
    EXPECT_EQ(Status::OK(), field->from_string("true"));
    const std::string tmp = field->to_string();
    EXPECT_EQ(5, tmp.size());
    EXPECT_EQ('t', tmp[0]);
    EXPECT_EQ('r', tmp[1]);
    EXPECT_EQ('u', tmp[2]);
    EXPECT_EQ('e', tmp[3]);
    EXPECT_EQ(0, tmp[4]);

    TabletSchema tablet_schema;
    SetTabletSchema(std::string("STRING_COLUMN"), "VARCHAR", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }

    StringValue value;
    const char* value_buffer = "dddd";
    value.len = 4;
    value.ptr = const_cast<char*>(value_buffer);

    ColumnPredicate* pred = new EqualPredicate<StringValue>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    StringValue* col_data =
            reinterpret_cast<StringValue*>(_mem_pool->allocate(size * sizeof(StringValue)));
    col_vector->set_col_data(col_data);
    char* string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(55));
    for (int i = 0; i < size; ++i) {
        for (int j = 0; j <= i; ++j) {
            string_buffer[j] = 'a' + i;
        }
        (*(col_data + i)).len = i + 1;
        (*(col_data + i)).ptr = string_buffer;
        string_buffer += i + 1;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    uint16_t* sel = _vectorized_batch->selected();
    EXPECT_EQ(sel[0], 3);
    EXPECT_EQ(*(col_data + sel[0]), value);

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(60));
    memset(string_buffer, 0, 60);
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
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(*(StringValue*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value);

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(55));
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            for (int j = 0; j <= i; ++j) {
                string_buffer[j] = 'a' + i;
            }
            (*(col_data + i)).len = i + 1;
            (*(col_data + i)).ptr = string_buffer;
        }
        string_buffer += i + 1;
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    sel = _vectorized_batch->selected();
    EXPECT_EQ(*(col_data + sel[0]), value);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(55));
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
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
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(*(StringValue*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value);

    delete field;
    delete pred;
}

TEST_F(TestEqualPredicate, DATE_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DATE_COLUMN"), "DATE", "REPLACE", 1, true, true, &tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    uint24_t value = datetime::to_date_timestamp("2017-09-10");
    ColumnPredicate* pred = new EqualPredicate<uint24_t>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    uint24_t* col_data = reinterpret_cast<uint24_t*>(_mem_pool->allocate(size * sizeof(uint24_t)));
    col_vector->set_col_data(col_data);

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07");
    date_array.push_back("2017-09-08");
    date_array.push_back("2017-09-09");
    date_array.push_back("2017-09-10");
    date_array.push_back("2017-09-11");
    date_array.push_back("2017-09-12");
    for (int i = 0; i < size; ++i) {
        uint24_t timestamp = datetime::to_date_timestamp(date_array[i].c_str());
        *(col_data + i) = timestamp;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    uint16_t* sel = _vectorized_batch->selected();
    EXPECT_EQ(sel[0], 3);
    EXPECT_EQ(*(col_data + sel[0]), value);
    EXPECT_EQ(datetime::to_date_string(*(col_data + sel[0])), "2017-09-10");

    // for ColumnBlock no nulls
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        uint24_t timestamp = datetime::to_date_timestamp(date_array[i].c_str());
        *reinterpret_cast<uint24_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(datetime::to_date_string(
                      *(uint24_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-10");

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            uint24_t timestamp = datetime::to_date_timestamp(date_array[i].c_str());
            *(col_data + i) = timestamp;
        }
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    sel = _vectorized_batch->selected();
    EXPECT_EQ(*(col_data + sel[0]), value);
    EXPECT_EQ(datetime::to_date_string(*(col_data + sel[0])), "2017-09-10");

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
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
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(datetime::to_date_string(
                      *(uint24_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-10");

    delete pred;
}

TEST_F(TestEqualPredicate, DATETIME_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DATETIME_COLUMN"), "DATETIME", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    uint64_t value = datetime::to_datetime_timestamp("2017-09-10 01:00:00");
    ColumnPredicate* pred = new EqualPredicate<uint64_t>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    uint64_t* col_data = reinterpret_cast<uint64_t*>(_mem_pool->allocate(size * sizeof(uint64_t)));
    col_vector->set_col_data(col_data);

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07 00:00:00");
    date_array.push_back("2017-09-08 00:01:00");
    date_array.push_back("2017-09-09 00:00:01");
    date_array.push_back("2017-09-10 01:00:00");
    date_array.push_back("2017-09-11 01:01:00");
    date_array.push_back("2017-09-12 01:01:01");
    for (int i = 0; i < size; ++i) {
        uint64_t timestamp = datetime::to_datetime_timestamp(date_array[i].c_str());
        *(col_data + i) = timestamp;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    uint16_t* sel = _vectorized_batch->selected();
    EXPECT_EQ(sel[0], 3);
    EXPECT_EQ(*(col_data + sel[0]), value);
    EXPECT_EQ(datetime::to_datetime_string(*(col_data + sel[0])), "2017-09-10 01:00:00");

    // for ColumnBlock no nulls
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        uint64_t timestamp = datetime::to_datetime_timestamp(date_array[i].c_str());
        *reinterpret_cast<uint64_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(datetime::to_datetime_string(
                      *(uint64_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-10 01:00:00");

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            uint64_t timestamp = datetime::to_datetime_timestamp(date_array[i].c_str());
            *(col_data + i) = timestamp;
        }
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    sel = _vectorized_batch->selected();
    EXPECT_EQ(*(col_data + sel[0]), value);
    EXPECT_EQ(datetime::to_datetime_string(*(col_data + sel[0])), "2017-09-10 01:00:00");

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            uint64_t timestamp = datetime::to_datetime_timestamp(date_array[i].c_str());
            *reinterpret_cast<uint64_t*>(col_block_view.data()) = timestamp;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(datetime::to_datetime_string(
                      *(uint64_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-10 01:00:00");

    delete pred;
}

#define TEST_LESS_PREDICATE(TYPE, TYPE_NAME, FIELD_TYPE)                                        \
    TEST_F(TestLessPredicate, TYPE_NAME##_COLUMN) {                                             \
        TabletSchema tablet_schema;                                                             \
        SetTabletSchema(std::string("TYPE_NAME_COLUMN"), FIELD_TYPE, "REPLACE", 1, false, true, \
                        &tablet_schema);                                                        \
        int size = 10;                                                                          \
        std::vector<uint32_t> return_columns;                                                   \
        for (int i = 0; i < tablet_schema.num_columns(); ++i) {                                 \
            return_columns.push_back(i);                                                        \
        }                                                                                       \
        InitVectorizedBatch(&tablet_schema, return_columns, size);                              \
        ColumnVector* col_vector = _vectorized_batch->column(0);                                \
                                                                                                \
        /* for no nulls */                                                                      \
        col_vector->set_no_nulls(true);                                                         \
        TYPE* col_data = reinterpret_cast<TYPE*>(_mem_pool->allocate(size * sizeof(TYPE)));     \
        col_vector->set_col_data(col_data);                                                     \
        for (int i = 0; i < size; ++i) {                                                        \
            *(col_data + i) = i;                                                                \
        }                                                                                       \
        TYPE value = 5;                                                                         \
        ColumnPredicate* pred = new LessPredicate<TYPE>(0, value);                              \
        pred->evaluate(_vectorized_batch);                                                      \
        EXPECT_EQ(_vectorized_batch->size(), 5);                                                \
        uint16_t* sel = _vectorized_batch->selected();                                          \
        TYPE sum = 0;                                                                           \
        for (int i = 0; i < _vectorized_batch->size(); ++i) {                                   \
            sum += *(col_data + sel[i]);                                                        \
        }                                                                                       \
        EXPECT_EQ(sum, 10);                                                                     \
                                                                                                \
        /* for has nulls */                                                                     \
        col_vector->set_no_nulls(false);                                                        \
        bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));                     \
        memset(is_null, 0, size);                                                               \
        col_vector->set_is_null(is_null);                                                       \
        for (int i = 0; i < size; ++i) {                                                        \
            if (i % 2 == 0) {                                                                   \
                is_null[i] = true;                                                              \
            } else {                                                                            \
                *(col_data + i) = i;                                                            \
            }                                                                                   \
        }                                                                                       \
        _vectorized_batch->set_size(size);                                                      \
        _vectorized_batch->set_selected_in_use(false);                                          \
        pred->evaluate(_vectorized_batch);                                                      \
        EXPECT_EQ(_vectorized_batch->size(), 2);                                                \
        sel = _vectorized_batch->selected();                                                    \
        sum = 0;                                                                                \
        for (int i = 0; i < _vectorized_batch->size(); ++i) {                                   \
            sum += *(col_data + sel[i]);                                                        \
        }                                                                                       \
        EXPECT_EQ(sum, 4);                                                                      \
        delete pred;                                                                            \
    }

TEST_LESS_PREDICATE(int8_t, TINYINT, "TINYINT")
TEST_LESS_PREDICATE(int16_t, SMALLINT, "SMALLINT")
TEST_LESS_PREDICATE(int32_t, INT, "INT")
TEST_LESS_PREDICATE(int64_t, BIGINT, "BIGINT")
TEST_LESS_PREDICATE(int128_t, LARGEINT, "LARGEINT")

TEST_F(TestLessPredicate, FLOAT_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("FLOAT_COLUMN"), "FLOAT", "REPLACE", 1, true, true, &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    float value = 5.0;
    ColumnPredicate* pred = new LessPredicate<float>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    float* col_data = reinterpret_cast<float*>(_mem_pool->allocate(size * sizeof(float)));
    col_vector->set_col_data(col_data);
    for (int i = 0; i < size; ++i) {
        *(col_data + i) = i;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 5);
    uint16_t* sel = _vectorized_batch->selected();
    float sum = 0;
    for (int i = 0; i < _vectorized_batch->size(); ++i) {
        sum += *(col_data + sel[i]);
    }
    EXPECT_FLOAT_EQ(sum, 10.0);

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<float*>(col_block_view.data()) = i;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 5);
    sum = 0;
    for (int i = 0; i < 5; ++i) {
        sum += *(float*)col_block.cell(_row_block->selection_vector()[i]).cell_ptr();
    }
    EXPECT_FLOAT_EQ(sum, 10.0);

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            *(col_data + i) = i;
        }
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 2);
    sel = _vectorized_batch->selected();
    sum = 0;
    for (int i = 0; i < _vectorized_batch->size(); ++i) {
        sum += *(col_data + sel[i]);
    }
    EXPECT_FLOAT_EQ(sum, 4.0);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            *reinterpret_cast<float*>(col_block_view.data()) = i;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 2);
    sum = 0;
    for (int i = 0; i < 2; ++i) {
        sum += *(float*)col_block.cell(_row_block->selection_vector()[i]).cell_ptr();
    }
    EXPECT_FLOAT_EQ(sum, 4.0);

    delete pred;
}

TEST_F(TestLessPredicate, DOUBLE_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DOUBLE_COLUMN"), "DOUBLE", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    double value = 5.0;
    ColumnPredicate* pred = new LessPredicate<double>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    double* col_data = reinterpret_cast<double*>(_mem_pool->allocate(size * sizeof(double)));
    col_vector->set_col_data(col_data);
    for (int i = 0; i < size; ++i) {
        *(col_data + i) = i;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 5);
    uint16_t* sel = _vectorized_batch->selected();
    double sum = 0;
    for (int i = 0; i < _vectorized_batch->size(); ++i) {
        sum += *(col_data + sel[i]);
    }
    EXPECT_DOUBLE_EQ(sum, 10.0);

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<double*>(col_block_view.data()) = i;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 5);
    sum = 0;
    for (int i = 0; i < 5; ++i) {
        sum += *(double*)col_block.cell(_row_block->selection_vector()[i]).cell_ptr();
    }
    EXPECT_DOUBLE_EQ(sum, 10.0);

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            *(col_data + i) = i;
        }
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 2);
    sel = _vectorized_batch->selected();
    sum = 0;
    for (int i = 0; i < _vectorized_batch->size(); ++i) {
        sum += *(col_data + sel[i]);
    }
    EXPECT_DOUBLE_EQ(sum, 4.0);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            *reinterpret_cast<double*>(col_block_view.data()) = i;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 2);
    sum = 0;
    for (int i = 0; i < 2; ++i) {
        sum += *(double*)col_block.cell(_row_block->selection_vector()[i]).cell_ptr();
    }
    EXPECT_DOUBLE_EQ(sum, 4.0);

    delete pred;
}

TEST_F(TestLessPredicate, DECIMAL_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DECIMAL_COLUMN"), "DECIMAL", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    decimal12_t value = {5, 5};
    ColumnPredicate* pred = new LessPredicate<decimal12_t>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    decimal12_t* col_data =
            reinterpret_cast<decimal12_t*>(_mem_pool->allocate(size * sizeof(decimal12_t)));
    col_vector->set_col_data(col_data);
    for (int i = 0; i < size; ++i) {
        (*(col_data + i)).integer = i;
        (*(col_data + i)).fraction = i;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 5);
    uint16_t* sel = _vectorized_batch->selected();
    decimal12_t sum = {0, 0};
    for (int i = 0; i < _vectorized_batch->size(); ++i) {
        sum += *(col_data + sel[i]);
    }
    EXPECT_EQ(sum.integer, 10);
    EXPECT_EQ(sum.fraction, 10);

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        reinterpret_cast<decimal12_t*>(col_block_view.data())->integer = i;
        reinterpret_cast<decimal12_t*>(col_block_view.data())->fraction = i;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 5);
    sum.integer = 0;
    sum.fraction = 0;
    for (int i = 0; i < _vectorized_batch->size(); ++i) {
        sum += *(col_data + sel[i]);
    }
    EXPECT_EQ(sum.integer, 10);
    EXPECT_EQ(sum.fraction, 10);

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            (*(col_data + i)).integer = i;
            (*(col_data + i)).fraction = i;
        }
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 2);
    sum.integer = 0;
    sum.fraction = 0;
    for (int i = 0; i < _vectorized_batch->size(); ++i) {
        sum += *(col_data + sel[i]);
    }
    EXPECT_EQ(sum.integer, 4);
    EXPECT_EQ(sum.fraction, 4);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
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
    EXPECT_EQ(select_size, 2);
    sum.integer = 0;
    sum.fraction = 0;
    for (int i = 0; i < _vectorized_batch->size(); ++i) {
        sum += *(col_data + sel[i]);
    }
    EXPECT_EQ(sum.integer, 4);
    EXPECT_EQ(sum.fraction, 4);

    delete pred;
}

TEST_F(TestLessPredicate, STRING_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("STRING_COLUMN"), "VARCHAR", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }

    StringValue value;
    const char* value_buffer = "dddd";
    value.len = 4;
    value.ptr = const_cast<char*>(value_buffer);
    ColumnPredicate* pred = new LessPredicate<StringValue>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    StringValue* col_data =
            reinterpret_cast<StringValue*>(_mem_pool->allocate(size * sizeof(StringValue)));
    col_vector->set_col_data(col_data);
    char* string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(55));
    for (int i = 0; i < size; ++i) {
        for (int j = 0; j <= i; ++j) {
            string_buffer[j] = 'a' + i;
        }
        (*(col_data + i)).len = i + 1;
        (*(col_data + i)).ptr = string_buffer;
        string_buffer += i + 1;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 3);
    uint16_t* sel = _vectorized_batch->selected();
    EXPECT_TRUE(strncmp((*(col_data + sel[0])).ptr, "a", 1) == 0);

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(60));
    memset(string_buffer, 0, 60);
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
    EXPECT_EQ(select_size, 3);
    EXPECT_TRUE(
            strncmp((*(StringValue*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr())
                            .ptr,
                    "a", 1) == 0);

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(55));
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            for (int j = 0; j <= i; ++j) {
                string_buffer[j] = 'a' + i;
            }
            (*(col_data + i)).len = i + 1;
            (*(col_data + i)).ptr = string_buffer;
        }
        string_buffer += i + 1;
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    sel = _vectorized_batch->selected();
    EXPECT_TRUE(strncmp((*(col_data + sel[0])).ptr, "bb", 2) == 0);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(55));
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
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
    EXPECT_EQ(select_size, 1);
    EXPECT_TRUE(
            strncmp((*(StringValue*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr())
                            .ptr,
                    "bb", 2) == 0);

    delete pred;
}

TEST_F(TestLessPredicate, DATE_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DATE_COLUMN"), "DATE", "REPLACE", 1, true, true, &tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    uint24_t value = datetime::to_date_timestamp("2017-09-10");
    ColumnPredicate* pred = new LessPredicate<uint24_t>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    uint24_t* col_data = reinterpret_cast<uint24_t*>(_mem_pool->allocate(size * sizeof(uint24_t)));
    col_vector->set_col_data(col_data);

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07");
    date_array.push_back("2017-09-08");
    date_array.push_back("2017-09-09");
    date_array.push_back("2017-09-10");
    date_array.push_back("2017-09-11");
    date_array.push_back("2017-09-12");
    for (int i = 0; i < size; ++i) {
        uint24_t timestamp = datetime::to_date_timestamp(date_array[i].c_str());
        *(col_data + i) = timestamp;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 3);
    uint16_t* sel = _vectorized_batch->selected();
    EXPECT_EQ(datetime::to_date_string(*(col_data + sel[0])), "2017-09-07");

    // for ColumnBlock no nulls
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        uint24_t timestamp = datetime::to_date_timestamp(date_array[i].c_str());
        *reinterpret_cast<uint24_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 3);
    EXPECT_EQ(datetime::to_date_string(
                      *(uint24_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-07");

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            uint24_t timestamp = datetime::to_date_timestamp(date_array[i].c_str());
            *(col_data + i) = timestamp;
        }
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    sel = _vectorized_batch->selected();
    EXPECT_EQ(datetime::to_date_string(*(col_data + sel[0])), "2017-09-08");

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
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
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(datetime::to_date_string(
                      *(uint24_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-08");

    delete pred;
}

TEST_F(TestLessPredicate, DATETIME_COLUMN) {
    TabletSchema tablet_schema;
    TabletColumn tablet_column;
    SetTabletSchema(std::string("DATETIME_COLUMN"), "DATETIME", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }

    uint64_t value = datetime::to_datetime_timestamp("2017-09-10 01:00:00");
    ColumnPredicate* pred = new LessPredicate<uint64_t>(0, value);

    // for VectorizedBatch no nulls
    InitVectorizedBatch(&tablet_schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);
    col_vector->set_no_nulls(true);
    uint64_t* col_data = reinterpret_cast<uint64_t*>(_mem_pool->allocate(size * sizeof(uint64_t)));
    col_vector->set_col_data(col_data);

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07 00:00:00");
    date_array.push_back("2017-09-08 00:01:00");
    date_array.push_back("2017-09-09 00:00:01");
    date_array.push_back("2017-09-10 01:00:00");
    date_array.push_back("2017-09-11 01:01:00");
    date_array.push_back("2017-09-12 01:01:01");
    for (int i = 0; i < size; ++i) {
        uint64_t timestamp = datetime::to_datetime_timestamp(date_array[i].c_str());
        *(col_data + i) = timestamp;
    }
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 3);
    uint16_t* sel = _vectorized_batch->selected();
    EXPECT_EQ(datetime::to_datetime_string(*(col_data + sel[0])), "2017-09-07 00:00:00");

    // for ColumnBlock no nulls
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        uint64_t timestamp = datetime::to_datetime_timestamp(date_array[i].c_str());
        *reinterpret_cast<uint64_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 3);
    EXPECT_EQ(datetime::to_datetime_string(
                      *(uint64_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-07 00:00:00");

    // for VectorizedBatch has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 2 == 0) {
            is_null[i] = true;
        } else {
            uint64_t timestamp = datetime::to_datetime_timestamp(date_array[i].c_str());
            *(col_data + i) = timestamp;
        }
    }
    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    EXPECT_EQ(_vectorized_batch->size(), 1);
    sel = _vectorized_batch->selected();
    EXPECT_EQ(datetime::to_datetime_string(*(col_data + sel[0])), "2017-09-08 00:01:00");

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            uint64_t timestamp = datetime::to_datetime_timestamp(date_array[i].c_str());
            *reinterpret_cast<uint64_t*>(col_block_view.data()) = timestamp;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(datetime::to_datetime_string(
                      *(uint64_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-08 00:01:00");

    delete pred;
}

} // namespace doris
