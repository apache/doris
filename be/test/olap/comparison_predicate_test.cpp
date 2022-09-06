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
#include "runtime/primitive_type.h"
#include "runtime/string_value.hpp"

namespace doris {

namespace datetime {

static uint32_t to_date_timestamp(const char* date_string) {
    tm time_tm;
    strptime(date_string, "%Y-%m-%d", &time_tm);

    int value = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
    return uint32_t(value);
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
        CLASS_NAME() { _mem_pool.reset(new MemPool()); }                                          \
        ~CLASS_NAME() {}                                                                          \
        void SetTabletSchema(std::string name, const std::string& type,                           \
                             const std::string& aggregation, uint32_t length, bool is_allow_null, \
                             bool is_key, TabletSchemaSPtr tablet_schema) {                       \
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
                                                                                                  \
        void init_row_block(TabletSchemaSPtr tablet_schema, int size) {                           \
            Schema schema(tablet_schema);                                                         \
            _row_block.reset(new RowBlockV2(schema, size));                                       \
        }                                                                                         \
        std::unique_ptr<MemPool> _mem_pool;                                                       \
        std::unique_ptr<RowBlockV2> _row_block;                                                   \
    };

TEST_PREDICATE_DEFINITION(TestEqualPredicate)
TEST_PREDICATE_DEFINITION(TestLessPredicate)

TEST_F(TestEqualPredicate, FLOAT_COLUMN) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("FLOAT_COLUMN"), "FLOAT", "REPLACE", 1, true, true, tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    float value = 5.0;
    ColumnPredicate* pred = new ComparisonPredicateBase<TYPE_FLOAT, PredicateType::EQ>(0, value);

    // for ColumnBlock no null
    init_row_block(tablet_schema, size);
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
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DOUBLE_COLUMN"), "DOUBLE", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    double value = 5.0;
    ColumnPredicate* pred = new ComparisonPredicateBase<TYPE_DOUBLE, PredicateType::EQ>(0, value);

    // for ColumnBlock no null
    init_row_block(tablet_schema, size);
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
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DECIMAL_COLUMN"), "DECIMAL", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    decimal12_t value = {5, 5};
    ColumnPredicate* pred =
            new ComparisonPredicateBase<TYPE_DECIMALV2, PredicateType::EQ>(0, value);

    // for ColumnBlock no null
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
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(*(decimal12_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value);

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
    TabletSchemaSPtr char_tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("STRING_COLUMN"), "CHAR", "REPLACE", 5, true, true,
                    char_tablet_schema);
    // test WrapperField.from_string() for char type
    WrapperField* field = WrapperField::create(char_tablet_schema->column(0));
    EXPECT_EQ(Status::OK(), field->from_string("true"));
    const std::string tmp = field->to_string();
    EXPECT_EQ(5, tmp.size());
    EXPECT_EQ('t', tmp[0]);
    EXPECT_EQ('r', tmp[1]);
    EXPECT_EQ('u', tmp[2]);
    EXPECT_EQ('e', tmp[3]);
    EXPECT_EQ(0, tmp[4]);

    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("STRING_COLUMN"), "VARCHAR", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }

    StringValue value;
    const char* value_buffer = "dddd";
    value.len = 4;
    value.ptr = const_cast<char*>(value_buffer);

    ColumnPredicate* pred = new ComparisonPredicateBase<TYPE_STRING, PredicateType::EQ>(0, value);

    // for ColumnBlock no null
    init_row_block(tablet_schema, size);
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
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(*(StringValue*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr(), value);

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
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DATE_COLUMN"), "DATE", "REPLACE", 1, true, true, tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    uint32_t value = datetime::to_date_timestamp("2017-09-10");
    ColumnPredicate* pred = new ComparisonPredicateBase<TYPE_DATE, PredicateType::EQ>(0, value);

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
        const uint32_t tmp = datetime::to_date_timestamp(date_array[i].c_str());
        uint24_t timestamp = 0;
        memcpy(reinterpret_cast<void*>(&timestamp), reinterpret_cast<const void*>(&tmp),
               sizeof(uint24_t));
        *reinterpret_cast<uint24_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 1);
    EXPECT_EQ(datetime::to_date_string(
                      *(uint24_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-10");

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            const uint32_t tmp = datetime::to_date_timestamp(date_array[i].c_str());
            uint24_t timestamp = 0;
            memcpy(reinterpret_cast<void*>(&timestamp), reinterpret_cast<const void*>(&tmp),
                   sizeof(uint24_t));
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
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DATETIME_COLUMN"), "DATETIME", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    uint64_t value = datetime::to_datetime_timestamp("2017-09-10 01:00:00");
    ColumnPredicate* pred = new ComparisonPredicateBase<TYPE_DATETIME, PredicateType::EQ>(0, value);

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07 00:00:00");
    date_array.push_back("2017-09-08 00:01:00");
    date_array.push_back("2017-09-09 00:00:01");
    date_array.push_back("2017-09-10 01:00:00");
    date_array.push_back("2017-09-11 01:01:00");
    date_array.push_back("2017-09-12 01:01:01");

    // for ColumnBlock no nulls
    init_row_block(tablet_schema, size);
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

TEST_F(TestLessPredicate, FLOAT_COLUMN) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("FLOAT_COLUMN"), "FLOAT", "REPLACE", 1, true, true, tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    float value = 5.0;
    ColumnPredicate* pred = new ComparisonPredicateBase<TYPE_FLOAT, PredicateType::LT>(0, value);

    // for ColumnBlock no null
    init_row_block(tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<float*>(col_block_view.data()) = i;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 5);
    float sum = 0;
    for (int i = 0; i < 5; ++i) {
        sum += *(float*)col_block.cell(_row_block->selection_vector()[i]).cell_ptr();
    }
    EXPECT_FLOAT_EQ(sum, 10.0);

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
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DOUBLE_COLUMN"), "DOUBLE", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    double value = 5.0;
    ColumnPredicate* pred = new ComparisonPredicateBase<TYPE_DOUBLE, PredicateType::LT>(0, value);

    // for ColumnBlock no null
    init_row_block(tablet_schema, size);
    ColumnBlock col_block = _row_block->column_block(0);
    auto select_size = _row_block->selected_size();
    ColumnBlockView col_block_view(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        col_block_view.set_null_bits(1, false);
        *reinterpret_cast<double*>(col_block_view.data()) = i;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 5);
    double sum = 0;
    for (int i = 0; i < 5; ++i) {
        sum += *(double*)col_block.cell(_row_block->selection_vector()[i]).cell_ptr();
    }
    EXPECT_DOUBLE_EQ(sum, 10.0);

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
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DECIMAL_COLUMN"), "DECIMAL", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    decimal12_t value = {5, 5};
    ColumnPredicate* pred =
            new ComparisonPredicateBase<TYPE_DECIMALV2, PredicateType::LT>(0, value);

    // for ColumnBlock no null
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
    EXPECT_EQ(select_size, 5);

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

    delete pred;
}

TEST_F(TestLessPredicate, STRING_COLUMN) {
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("STRING_COLUMN"), "VARCHAR", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }

    StringValue value;
    const char* value_buffer = "dddd";
    value.len = 4;
    value.ptr = const_cast<char*>(value_buffer);
    ColumnPredicate* pred = new ComparisonPredicateBase<TYPE_STRING, PredicateType::LT>(0, value);

    // for ColumnBlock no null
    init_row_block(tablet_schema, size);
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
    EXPECT_TRUE(
            strncmp((*(StringValue*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr())
                            .ptr,
                    "a", 1) == 0);

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
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    SetTabletSchema(std::string("DATE_COLUMN"), "DATE", "REPLACE", 1, true, true, tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }
    uint32_t value = datetime::to_date_timestamp("2017-09-10");
    ColumnPredicate* pred = new ComparisonPredicateBase<TYPE_DATE, PredicateType::LT>(0, value);

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
        const uint32_t tmp = datetime::to_date_timestamp(date_array[i].c_str());
        uint24_t timestamp = 0;
        memcpy(reinterpret_cast<void*>(&timestamp), reinterpret_cast<const void*>(&tmp),
               sizeof(uint24_t));
        *reinterpret_cast<uint24_t*>(col_block_view.data()) = timestamp;
    }
    pred->evaluate(&col_block, _row_block->selection_vector(), &select_size);
    EXPECT_EQ(select_size, 3);
    EXPECT_EQ(datetime::to_date_string(
                      *(uint24_t*)col_block.cell(_row_block->selection_vector()[0]).cell_ptr()),
              "2017-09-07");

    // for ColumnBlock has nulls
    col_block_view = ColumnBlockView(&col_block);
    for (int i = 0; i < size; ++i, col_block_view.advance(1)) {
        if (i % 2 == 0) {
            col_block_view.set_null_bits(1, true);
        } else {
            col_block_view.set_null_bits(1, false);
            const uint32_t tmp = datetime::to_date_timestamp(date_array[i].c_str());
            uint24_t timestamp = 0;
            memcpy(reinterpret_cast<void*>(&timestamp), reinterpret_cast<const void*>(&tmp),
                   sizeof(uint24_t));
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
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    TabletColumn tablet_column;
    SetTabletSchema(std::string("DATETIME_COLUMN"), "DATETIME", "REPLACE", 1, true, true,
                    tablet_schema);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < tablet_schema->num_columns(); ++i) {
        return_columns.push_back(i);
    }

    uint64_t value = datetime::to_datetime_timestamp("2017-09-10 01:00:00");
    ColumnPredicate* pred = new ComparisonPredicateBase<TYPE_DATETIME, PredicateType::LT>(0, value);

    std::vector<std::string> date_array;
    date_array.push_back("2017-09-07 00:00:00");
    date_array.push_back("2017-09-08 00:01:00");
    date_array.push_back("2017-09-09 00:00:01");
    date_array.push_back("2017-09-10 01:00:00");
    date_array.push_back("2017-09-11 01:01:00");
    date_array.push_back("2017-09-12 01:01:01");

    // for ColumnBlock no nulls
    init_row_block(tablet_schema, size);
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
