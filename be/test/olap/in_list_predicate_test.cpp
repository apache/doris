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

#include "olap/in_list_predicate.h"

#include <google/protobuf/stubs/common.h>
#include <gtest/gtest.h>
#include <time.h>

#include "olap/column_predicate.h"
#include "olap/field.h"
#include "olap/row_block2.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.hpp"
#include "util/logging.h"

namespace doris {

namespace datetime {

static uint24_t timestamp_from_date(const char* date_string) {
    tm time_tm;
    strptime(date_string, "%Y-%m-%d", &time_tm);

    int value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    return uint24_t(value);
}

static uint32_t timestamp_from_date_v2(const char* date_string) {
    tm time_tm;
    strptime(date_string, "%Y-%m-%d", &time_tm);

    doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType> value;
    value.set_time(time_tm.tm_year + 1900, time_tm.tm_mon + 1, time_tm.tm_mday, 0, 0, 0, 0);
    return binary_cast<doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>,
                       uint32_t>(value);
}

static uint64_t timestamp_from_datetime(const std::string& value_string) {
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

static std::string to_date_v2_string(uint32_t& date_value) {
    auto val = binary_cast<uint32_t, vectorized::DateV2Value<doris::vectorized::DateV2ValueType>>(
            date_value);
    std::stringstream ss;
    ss << val;
    return ss.str();
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

class TestInListPredicate : public testing::Test {
public:
    TestInListPredicate() : _row_block(nullptr) { _mem_pool.reset(new MemPool()); }

    ~TestInListPredicate() {}

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
        _schema = std::make_unique<Schema>(*tablet_schema);
        _row_block.reset(new RowBlockV2(*_schema, size));
    }

    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<RowBlockV2> _row_block;
    std::unique_ptr<Schema> _schema;
};

#define TEST_IN_LIST_PREDICATE_V2(TYPE, TYPE_NAME, FIELD_TYPE)                                    \
    TEST_F(TestInListPredicate, TYPE_NAME##_COLUMN_V2) {                                          \
        TabletSchema tablet_schema;                                                               \
        SetTabletSchema(std::string("TYPE_NAME##_COLUMN"), FIELD_TYPE, "REPLACE", 1, false, true, \
                        &tablet_schema);                                                          \
        int size = 10;                                                                            \
        Schema schema(tablet_schema);                                                             \
        RowBlockV2 block(schema, size);                                                           \
        phmap::flat_hash_set<TYPE> values;                                                        \
        values.insert(4);                                                                         \
        values.insert(5);                                                                         \
        values.insert(6);                                                                         \
        ColumnPredicate* pred = new InListPredicate<TYPE>(0, std::move(values));                  \
        uint16_t sel[10];                                                                         \
        for (int i = 0; i < 10; ++i) {                                                            \
            sel[i] = i;                                                                           \
        }                                                                                         \
        uint16_t selected_size = 10;                                                              \
        ColumnBlock column = block.column_block(0);                                               \
        /* for non nulls */                                                                       \
        for (int i = 0; i < size; ++i) {                                                          \
            column.set_is_null(i, false);                                                         \
            uint8_t* value = column.mutable_cell_ptr(i);                                          \
            *((TYPE*)value) = i;                                                                  \
        }                                                                                         \
                                                                                                  \
        pred->evaluate(&column, sel, &selected_size);                                             \
        EXPECT_EQ(selected_size, 3);                                                              \
        EXPECT_EQ(*((TYPE*)column.cell_ptr(sel[0])), 4);                                          \
        EXPECT_EQ(*((TYPE*)column.cell_ptr(sel[1])), 5);                                          \
        EXPECT_EQ(*((TYPE*)column.cell_ptr(sel[2])), 6);                                          \
                                                                                                  \
        /* for has nulls */                                                                       \
        TabletSchema tablet_schema2;                                                              \
        SetTabletSchema(std::string("TYPE_NAME##_COLUMN"), FIELD_TYPE, "REPLACE", 1, true, true,  \
                        &tablet_schema2);                                                         \
        Schema schema2(tablet_schema2);                                                           \
        RowBlockV2 block2(schema2, size);                                                         \
        ColumnBlock column2 = block2.column_block(0);                                             \
        for (int i = 0; i < size; ++i) {                                                          \
            if (i % 2 == 0) {                                                                     \
                column2.set_is_null(i, true);                                                     \
            } else {                                                                              \
                column2.set_is_null(i, false);                                                    \
                uint8_t* value = column2.mutable_cell_ptr(i);                                     \
                *((TYPE*)value) = i;                                                              \
            }                                                                                     \
        }                                                                                         \
        for (int i = 0; i < 10; ++i) {                                                            \
            sel[i] = i;                                                                           \
        }                                                                                         \
        selected_size = 10;                                                                       \
                                                                                                  \
        pred->evaluate(&column2, sel, &selected_size);                                            \
        EXPECT_EQ(selected_size, 1);                                                              \
        EXPECT_EQ(*((TYPE*)column2.cell_ptr(sel[0])), 5);                                         \
        delete pred;                                                                              \
    }

TEST_IN_LIST_PREDICATE_V2(int8_t, TINYINT, "TINYINT")
TEST_IN_LIST_PREDICATE_V2(int16_t, SMALLINT, "SMALLINT")
TEST_IN_LIST_PREDICATE_V2(int32_t, INT, "INT")
TEST_IN_LIST_PREDICATE_V2(int64_t, BIGINT, "BIGINT")
TEST_IN_LIST_PREDICATE_V2(int128_t, LARGEINT, "LARGEINT")

TEST_F(TestInListPredicate, FLOAT_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("FLOAT_COLUMN"), "FLOAT", "REPLACE", 1, true, true, &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    phmap::flat_hash_set<float> values;
    values.insert(4.1);
    values.insert(5.1);
    values.insert(6.1);
    ColumnPredicate* pred = new InListPredicate<float>(0, std::move(values));

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

TEST_F(TestInListPredicate, DOUBLE_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DOUBLE_COLUMN"), "DOUBLE", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    phmap::flat_hash_set<double> values;
    values.insert(4.1);
    values.insert(5.1);
    values.insert(6.1);

    ColumnPredicate* pred = new InListPredicate<double>(0, std::move(values));

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<double*>(col_block_view.data()) = i + 0.1;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 3);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 4.1);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[1]).cell_ptr(), 5.1);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[2]).cell_ptr(), 6.1);

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
    EXPECT_EQ(select_size, 1);
    EXPECT_DOUBLE_EQ(*(double*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), 5.1);

    delete pred;
}

TEST_F(TestInListPredicate, DECIMAL_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DECIMAL_COLUMN"), "DECIMAL", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    phmap::flat_hash_set<decimal12_t> values;

    decimal12_t value1 = {4, 4};
    values.insert(value1);
    decimal12_t value2 = {5, 5};
    values.insert(value2);
    decimal12_t value3 = {6, 6};
    values.insert(value3);

    ColumnPredicate* pred = new InListPredicate<decimal12_t>(0, std::move(values));

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
    EXPECT_EQ(select_size, 3);
    EXPECT_EQ(*(decimal12_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value1);
    EXPECT_EQ(*(decimal12_t*)col_block.cell(_row_block->selection_vector()[1]).cell_ptr(), value2);
    EXPECT_EQ(*(decimal12_t*)col_block.cell(_row_block->selection_vector()[2]).cell_ptr(), value3);

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
    EXPECT_EQ(*(decimal12_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value2);

    delete pred;
}

TEST_F(TestInListPredicate, CHAR_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("STRING_COLUMN"), "CHAR", "REPLACE", 1, true, true, &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    phmap::flat_hash_set<StringValue> values;
    StringValue value1;
    const char* value1_buffer = "aaaaa";
    value1.ptr = const_cast<char*>(value1_buffer);
    value1.len = 5;
    values.insert(value1);

    StringValue value2;
    const char* value2_buffer = "bbbbb";
    value2.ptr = const_cast<char*>(value2_buffer);
    value2.len = 5;
    values.insert(value2);

    StringValue value3;
    const char* value3_buffer = "ccccc";
    value3.ptr = const_cast<char*>(value3_buffer);
    value3.len = 5;
    values.insert(value3);

    ColumnPredicate* pred = new InListPredicate<StringValue>(0, std::move(values));

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    char* string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(60));
    memset(string_buffer, 0, 60);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        for (int j = 0; j <= 5; ++j) {
            string_buffer[j] = 'a' + i;
        }
        reinterpret_cast<StringValue*>(col_block_view.data())->len = 5;
        reinterpret_cast<StringValue*>(col_block_view.data())->ptr = string_buffer;
        string_buffer += 5;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 3);
    EXPECT_EQ(*(StringValue*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value1);
    EXPECT_EQ(*(StringValue*)col_block.cell(_row_block->selection_vector()[1]).cell_ptr(), value2);
    EXPECT_EQ(*(StringValue*)col_block.cell(_row_block->selection_vector()[2]).cell_ptr(), value3);

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(55));
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            for (int j = 0; j <= 5; ++j) {
                string_buffer[j] = 'a' + i;
            }
            reinterpret_cast<StringValue*>(col_block_view.data())->len = 5;
            reinterpret_cast<StringValue*>(col_block_view.data())->ptr = string_buffer;
            string_buffer += 5;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(*(StringValue*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value2);

    delete pred;
}

TEST_F(TestInListPredicate, VARCHAR_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("STRING_COLUMN"), "VARCHAR", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    phmap::flat_hash_set<StringValue> values;
    StringValue value1;
    const char* value1_buffer = "a";
    value1.ptr = const_cast<char*>(value1_buffer);
    value1.len = 1;
    values.insert(value1);

    StringValue value2;
    const char* value2_buffer = "bb";
    value2.ptr = const_cast<char*>(value2_buffer);
    value2.len = 2;
    values.insert(value2);

    StringValue value3;
    const char* value3_buffer = "ccc";
    value3.ptr = const_cast<char*>(value3_buffer);
    value3.len = 3;
    values.insert(value3);

    ColumnPredicate* pred = new InListPredicate<StringValue>(0, std::move(values));

    // for ColumnBlock no null
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    char* string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(60));
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
    EXPECT_EQ(*(StringValue*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value1);
    EXPECT_EQ(*(StringValue*)col_block.cell(_row_block->selection_vector()[1]).cell_ptr(), value2);
    EXPECT_EQ(*(StringValue*)col_block.cell(_row_block->selection_vector()[2]).cell_ptr(), value3);

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
    EXPECT_EQ(*(StringValue*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value2);

    delete pred;
}

TEST_F(TestInListPredicate, DATE_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DATE_COLUMN"), "DATE", "REPLACE", 1, true, true, &tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    phmap::flat_hash_set<uint24_t> values;
    uint24_t value1 = datetime::timestamp_from_date("2017-09-09");
    values.insert(value1);

    uint24_t value2 = datetime::timestamp_from_date("2017-09-10");
    values.insert(value2);

    uint24_t value3 = datetime::timestamp_from_date("2017-09-11");
    values.insert(value3);
    ColumnPredicate* pred = new InListPredicate<uint24_t>(0, std::move(values));

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07");
    date_array.push_back("2017-09-08");
    date_array.push_back("2017-09-09");
    date_array.push_back("2017-09-10");
    date_array.push_back("2017-09-11");
    date_array.push_back("2017-09-12");

    // for ColumnBlock no nulls
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        uint24_t timestamp = datetime::timestamp_from_date(date_array[i].c_str());
        *reinterpret_cast<uint24_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 3);
    EXPECT_EQ(datetime::to_date_string(
                      *(uint24_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-09");
    EXPECT_EQ(datetime::to_date_string(
                      *(uint24_t*)col_block.cell(_row_block->selection_vector()[1]).cell_ptr()),
              "2017-09-10");
    EXPECT_EQ(datetime::to_date_string(
                      *(uint24_t*)col_block.cell(_row_block->selection_vector()[2]).cell_ptr()),
              "2017-09-11");

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            uint24_t timestamp = datetime::timestamp_from_date(date_array[i].c_str());
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

TEST_F(TestInListPredicate, DATE_V2_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DATE_V2_COLUMN"), "DATEV2", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    phmap::flat_hash_set<uint32_t> values;
    uint32_t value1 = datetime::timestamp_from_date_v2("2017-09-09");
    values.insert(value1);

    uint32_t value2 = datetime::timestamp_from_date_v2("2017-09-10");
    values.insert(value2);

    uint32_t value3 = datetime::timestamp_from_date_v2("2017-09-11");
    values.insert(value3);
    ColumnPredicate* pred = new InListPredicate<uint32_t>(0, std::move(values));

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07");
    date_array.push_back("2017-09-08");
    date_array.push_back("2017-09-09");
    date_array.push_back("2017-09-10");
    date_array.push_back("2017-09-11");
    date_array.push_back("2017-09-12");

    // for ColumnBlock no nulls
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        uint32_t timestamp = datetime::timestamp_from_date_v2(date_array[i].c_str());
        *reinterpret_cast<uint32_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 3);
    EXPECT_EQ(datetime::to_date_v2_string(
                      *(uint32_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-09");
    EXPECT_EQ(datetime::to_date_v2_string(
                      *(uint32_t*)col_block.cell(_row_block->selection_vector()[1]).cell_ptr()),
              "2017-09-10");
    EXPECT_EQ(datetime::to_date_v2_string(
                      *(uint32_t*)col_block.cell(_row_block->selection_vector()[2]).cell_ptr()),
              "2017-09-11");

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            uint32_t timestamp = datetime::timestamp_from_date_v2(date_array[i].c_str());
            *reinterpret_cast<uint32_t*>(col_block_view.data()) = timestamp;
        }
    }
    _row_block->clear();
    select_size = _row_block->selected_size();
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(datetime::to_date_v2_string(
                      *(uint32_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-10");

    delete pred;
}

TEST_F(TestInListPredicate, DATETIME_COLUMN) {
    TabletSchema tablet_schema;
    SetTabletSchema(std::string("DATETIME_COLUMN"), "DATETIME", "REPLACE", 1, true, true,
                    &tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema.num_columns(); ++i) {
        return_columns.push_back(i);
    }
    phmap::flat_hash_set<uint64_t> values;
    uint64_t value1 = datetime::timestamp_from_datetime("2017-09-09 00:00:01");
    values.insert(value1);

    uint64_t value2 = datetime::timestamp_from_datetime("2017-09-10 01:00:00");
    values.insert(value2);

    uint64_t value3 = datetime::timestamp_from_datetime("2017-09-11 01:01:00");
    values.insert(value3);

    ColumnPredicate* pred = new InListPredicate<uint64_t>(0, std::move(values));

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07 00:00:00");
    date_array.push_back("2017-09-08 00:01:00");
    date_array.push_back("2017-09-09 00:00:01");
    date_array.push_back("2017-09-10 01:00:00");
    date_array.push_back("2017-09-11 01:01:00");
    date_array.push_back("2017-09-12 01:01:01");

    // for ColumnBlock no nulls
    init_row_block(&tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        uint64_t timestamp = datetime::timestamp_from_datetime(date_array[i].c_str());
        *reinterpret_cast<uint64_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 3);
    EXPECT_EQ(datetime::to_datetime_string(
                      *(uint64_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-09 00:00:01");
    EXPECT_EQ(datetime::to_datetime_string(
                      *(uint64_t*)col_block.cell(_row_block->selection_vector()[1]).cell_ptr()),
              "2017-09-10 01:00:00");
    EXPECT_EQ(datetime::to_datetime_string(
                      *(uint64_t*)col_block.cell(_row_block->selection_vector()[2]).cell_ptr()),
              "2017-09-11 01:01:00");

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            uint64_t timestamp = datetime::timestamp_from_datetime(date_array[i].c_str());
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

} // namespace doris
