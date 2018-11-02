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

#include <time.h>
#include <gtest/gtest.h>
#include <google/protobuf/stubs/common.h>

#include "olap/field.h"
#include "olap/column_predicate.h"
#include "olap/null_predicate.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"
#include "util/logging.h"

namespace doris {

namespace datetime {

static uint24_t to_date_timestamp(const char* date_string) {
    tm time_tm;
    strptime(date_string, "%Y-%m-%d", &time_tm);

    int value = (time_tm.tm_year + 1900) * 16 * 32
        + (time_tm.tm_mon + 1) * 32
        + time_tm.tm_mday;
    return uint24_t(value); 
}

static uint64_t to_datetime_timestamp(const std::string& value_string) {
    tm time_tm;
    strptime(value_string.c_str(), "%Y-%m-%d %H:%M:%S", &time_tm);

    uint64_t value = ((time_tm.tm_year + 1900) * 10000L
            + (time_tm.tm_mon + 1) * 100L
            + time_tm.tm_mday) * 1000000L
        + time_tm.tm_hour * 10000L
        + time_tm.tm_min * 100L
        + time_tm.tm_sec;

    return value;
}

};

class TestNullPredicate : public testing::Test {
public:
    TestNullPredicate() : _vectorized_batch(NULL) {
        _mem_tracker.reset(new MemTracker(-1));
        _mem_pool.reset(new MemPool(_mem_tracker.get()));
    }

    ~TestNullPredicate() {
        if (_vectorized_batch != NULL) {
            delete _vectorized_batch;
        }
    }

    void SetFieldInfo(FieldInfo &field_info, std::string name,
            FieldType type, FieldAggregationMethod aggregation,
            uint32_t length, bool is_allow_null, bool is_key) {
        field_info.name = name;
        field_info.type = type;
        field_info.aggregation = aggregation;
        field_info.length = length;
        field_info.is_allow_null = is_allow_null;
        field_info.is_key = is_key;
        field_info.precision = 1000;
        field_info.frac = 10000;
        field_info.unique_id = 0;
        field_info.is_bf_column = false;
    }

    void InitVectorizedBatch(const std::vector<FieldInfo>& schema,
                             const std::vector<uint32_t>&ids,
                             int size) {
        _vectorized_batch = new VectorizedRowBatch(schema, ids, size);
        _vectorized_batch->set_size(size);
    }
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    VectorizedRowBatch* _vectorized_batch;
};

#define TEST_IN_LIST_PREDICATE(TYPE, TYPE_NAME, FIELD_TYPE) \
TEST_F(TestNullPredicate, TYPE_NAME##_COLUMN) { \
    std::vector<FieldInfo> schema; \
    FieldInfo field_info; \
    SetFieldInfo(field_info, std::string("TYPE_NAME##_COLUMN"), FIELD_TYPE, \
                 OLAP_FIELD_AGGREGATION_REPLACE, 1, false, true); \
    schema.push_back(field_info); \
    int size = 10; \
    std::vector<uint32_t> return_columns; \
    for (int i = 0; i < schema.size(); ++i) { \
        return_columns.push_back(i); \
    } \
    InitVectorizedBatch(schema, return_columns, size); \
    ColumnVector* col_vector = _vectorized_batch->column(0); \
     \
    /* for no nulls */ \
    col_vector->set_no_nulls(true); \
    TYPE* col_data = reinterpret_cast<TYPE*>(_mem_pool->allocate(size * sizeof(TYPE))); \
    col_vector->set_col_data(col_data); \
    for (int i = 0; i < size; ++i) { \
        *(col_data + i) = i; \
    } \
    \
    ColumnPredicate* pred = new NullPredicate(0, true); \
    pred->evaluate(_vectorized_batch); \
    ASSERT_EQ(_vectorized_batch->size(), 0); \
    \
    /* for has nulls */ \
    col_vector->set_no_nulls(false); \
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size)); \
    memset(is_null, 0, size); \
    col_vector->set_is_null(is_null); \
    for (int i = 0; i < size; ++i) { \
        if (i % 2 == 0) { \
            is_null[i] = true; \
        } else { \
            *(col_data + i) = i; \
        } \
    } \
    _vectorized_batch->set_size(size); \
    _vectorized_batch->set_selected_in_use(false); \
    pred->evaluate(_vectorized_batch); \
    ASSERT_EQ(_vectorized_batch->size(), 5); \
} \

TEST_IN_LIST_PREDICATE(int8_t, TINYINT, OLAP_FIELD_TYPE_TINYINT)
TEST_IN_LIST_PREDICATE(int16_t, SMALLINT, OLAP_FIELD_TYPE_SMALLINT)
TEST_IN_LIST_PREDICATE(int32_t, INT, OLAP_FIELD_TYPE_INT)
TEST_IN_LIST_PREDICATE(int64_t, BIGINT, OLAP_FIELD_TYPE_BIGINT)
TEST_IN_LIST_PREDICATE(int128_t, LARGEINT, OLAP_FIELD_TYPE_LARGEINT)

TEST_F(TestNullPredicate, FLOAT_COLUMN) {
    std::vector<FieldInfo> schema;
    FieldInfo field_info;
    SetFieldInfo(field_info, std::string("FLOAT_COLUMN"), OLAP_FIELD_TYPE_FLOAT,
                 OLAP_FIELD_AGGREGATION_REPLACE, 1, false, true);
    schema.push_back(field_info);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < schema.size(); ++i) {
        return_columns.push_back(i);
    }
    InitVectorizedBatch(schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);

    // for no nulls
    col_vector->set_no_nulls(true);
    float* col_data = reinterpret_cast<float*>(_mem_pool->allocate(size * sizeof(float)));
    col_vector->set_col_data(col_data);
    for (int i = 0; i < size; ++i) {
        *(col_data + i) = i + 0.1;
    }
    ColumnPredicate* pred = new NullPredicate(0, true);
    pred->evaluate(_vectorized_batch);
    ASSERT_EQ(_vectorized_batch->size(), 0);

    // for has nulls
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
    ASSERT_EQ(_vectorized_batch->size(), 5);
}

TEST_F(TestNullPredicate, DOUBLE_COLUMN) {
    std::vector<FieldInfo> schema;
    FieldInfo field_info;
    SetFieldInfo(field_info, std::string("DOUBLE_COLUMN"), OLAP_FIELD_TYPE_DOUBLE,
                 OLAP_FIELD_AGGREGATION_REPLACE, 1, false, true);
    schema.push_back(field_info);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < schema.size(); ++i) {
        return_columns.push_back(i);
    }
    InitVectorizedBatch(schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);

    // for no nulls
    col_vector->set_no_nulls(true);
    double* col_data = reinterpret_cast<double*>(_mem_pool->allocate(size * sizeof(double)));
    col_vector->set_col_data(col_data);
    for (int i = 0; i < size; ++i) {
        *(col_data + i) = i + 0.1;
    }

    ColumnPredicate* pred = new NullPredicate(0, true);
    pred->evaluate(_vectorized_batch);
    ASSERT_EQ(_vectorized_batch->size(), 0);

    // for has nulls
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
    ASSERT_EQ(_vectorized_batch->size(), 5);
}

TEST_F(TestNullPredicate, DECIMAL_COLUMN) {
    std::vector<FieldInfo> schema;
    FieldInfo field_info;
    SetFieldInfo(field_info, std::string("DECIMAL_COLUMN"), OLAP_FIELD_TYPE_DECIMAL,
                 OLAP_FIELD_AGGREGATION_REPLACE, 1, false, true);
    schema.push_back(field_info);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < schema.size(); ++i) {
        return_columns.push_back(i);
    }
    InitVectorizedBatch(schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);

    // for no nulls
    col_vector->set_no_nulls(true);
    decimal12_t* col_data = reinterpret_cast<decimal12_t*>(_mem_pool->allocate(size * sizeof(decimal12_t)));
    col_vector->set_col_data(col_data);
    for (int i = 0; i < size; ++i) {
        (*(col_data + i)).integer = i;
        (*(col_data + i)).fraction = i;
    }

    ColumnPredicate* pred = new NullPredicate(0, true);
    pred->evaluate(_vectorized_batch);
    ASSERT_EQ(_vectorized_batch->size(), 0);

    // for has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));  
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 3 == 0) {
            is_null[i] = true;
        } else {
            (*(col_data + i)).integer = i;
            (*(col_data + i)).fraction = i;
        }
    }

    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    ASSERT_EQ(_vectorized_batch->size(), 4);
}

TEST_F(TestNullPredicate, STRING_COLUMN) {
    std::vector<FieldInfo> schema;
    FieldInfo field_info;
    SetFieldInfo(field_info, std::string("STRING_COLUMN"), OLAP_FIELD_TYPE_VARCHAR,
                 OLAP_FIELD_AGGREGATION_REPLACE, 1, false, true);
    schema.push_back(field_info);
    int size = 10;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < schema.size(); ++i) {
        return_columns.push_back(i);
    }
    InitVectorizedBatch(schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);

    // for no nulls
    col_vector->set_no_nulls(true);
    StringValue* col_data = reinterpret_cast<StringValue*>(_mem_pool->allocate(size * sizeof(StringValue)));
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

    ColumnPredicate* pred = new NullPredicate(0, true);
    ASSERT_EQ(_vectorized_batch->size(), 10);

    // for has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));  
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    string_buffer = reinterpret_cast<char*>(_mem_pool->allocate(55));
    for (int i = 0; i < size; ++i) {
        if (i % 3 == 0) {
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
    ASSERT_EQ(_vectorized_batch->size(), 4);
}

TEST_F(TestNullPredicate, DATE_COLUMN) {
    std::vector<FieldInfo> schema;
    FieldInfo field_info;
    SetFieldInfo(field_info, std::string("DATE_COLUMN"), OLAP_FIELD_TYPE_DATE,
                 OLAP_FIELD_AGGREGATION_REPLACE, 1, false, true);
    schema.push_back(field_info);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < schema.size(); ++i) {
        return_columns.push_back(i);
    }
    InitVectorizedBatch(schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);

    // for no nulls
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

    ColumnPredicate* pred = new NullPredicate(0, true);
    pred->evaluate(_vectorized_batch);
    ASSERT_EQ(_vectorized_batch->size(), 0);

    // for has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));  
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 3 == 0) {
            is_null[i] = true;
        } else {
            uint24_t timestamp = datetime::to_date_timestamp(date_array[i].c_str());
            *(col_data + i) = timestamp;
        }
    }

    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    ASSERT_EQ(_vectorized_batch->size(), 2);
}

TEST_F(TestNullPredicate, DATETIME_COLUMN) {
    std::vector<FieldInfo> schema;
    FieldInfo field_info;
    SetFieldInfo(field_info, std::string("DATETIME_COLUMN"), OLAP_FIELD_TYPE_DATETIME,
                 OLAP_FIELD_AGGREGATION_REPLACE, 1, false, true);
    schema.push_back(field_info);
    int size = 6;
    std::vector<uint32_t> return_columns;
    for (int i = 0; i < schema.size(); ++i) {
        return_columns.push_back(i);
    }
    InitVectorizedBatch(schema, return_columns, size);
    ColumnVector* col_vector = _vectorized_batch->column(0);

    // for no nulls
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

    ColumnPredicate* pred = new NullPredicate(0, true);
    pred->evaluate(_vectorized_batch);
    ASSERT_EQ(_vectorized_batch->size(), 0);

    // for has nulls
    col_vector->set_no_nulls(false);
    bool* is_null = reinterpret_cast<bool*>(_mem_pool->allocate(size));  
    memset(is_null, 0, size);
    col_vector->set_is_null(is_null);
    for (int i = 0; i < size; ++i) {
        if (i % 3 == 0) {
            is_null[i] = true;
        } else {
            uint64_t timestamp = datetime::to_datetime_timestamp(date_array[i].c_str());
            *(col_data + i) = timestamp;
        }
    }

    _vectorized_batch->set_size(size);
    _vectorized_batch->set_selected_in_use(false);
    pred->evaluate(_vectorized_batch);
    ASSERT_EQ(_vectorized_batch->size(), 2);
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    int ret = doris::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    ret = RUN_ALL_TESTS();
    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}
