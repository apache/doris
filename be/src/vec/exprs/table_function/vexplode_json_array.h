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

#pragma once

#include <glog/logging.h>
#include <rapidjson/document.h>

#include "common/status.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/table_function/table_function.h"

namespace doris {
struct ArrayVal;
namespace vectorized {
#include "common/compile_check_begin.h"

template <typename T>
struct ParsedData {
    ParsedData() = default;
    virtual ~ParsedData() = default;
    virtual void reset() {
        _backup_data.clear();
        _values_null_flag.clear();
    }
    virtual int set_output(rapidjson::Document& document, int value_size) = 0;
    virtual int set_output(const ArrayVal& array_doc, int value_size) = 0;
    virtual void insert_result_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                                int max_step) = 0;
    virtual void insert_many_same_value_from_parsed_data(MutableColumnPtr& column,
                                                         int64_t cur_offset, int length) = 0;
    const char* get_null_flag_address(size_t cur_offset) {
        return reinterpret_cast<const char*>(_values_null_flag.data() + cur_offset);
    }
    std::vector<UInt8> _values_null_flag;
    std::vector<T> _backup_data;
};

struct ParsedDataInt : public ParsedData<int64_t> {
    static constexpr auto MAX_VALUE = std::numeric_limits<int64_t>::max(); //9223372036854775807
    static constexpr auto MIN_VALUE = std::numeric_limits<int64_t>::min(); //-9223372036854775808
    int set_output(rapidjson::Document& document, int value_size) override;
    int set_output(const ArrayVal& array_doc, int value_size) override;
    void insert_result_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                        int max_step) override;
    void insert_many_same_value_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                                 int length) override;
};
struct ParsedDataDouble : public ParsedData<double> {
    int set_output(rapidjson::Document& document, int value_size) override;

    int set_output(const ArrayVal& array_doc, int value_size) override;

    void insert_result_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                        int max_step) override;

    void insert_many_same_value_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                                 int length) override;
};
struct ParsedDataStringBase : public ParsedData<std::string> {
    void insert_result_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                        int max_step) override;
    void insert_many_same_value_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                                 int length) override;

    void reset() override;

    static constexpr const char* TRUE_VALUE = "true";
    static constexpr const char* FALSE_VALUE = "false";
    std::vector<StringRef> _data_string_ref;
    char tmp_buf[128] = {0};
};
struct ParsedDataString : public ParsedDataStringBase {
    int set_output(rapidjson::Document& document, int value_size) override;

    int set_output(const ArrayVal& array_doc, int value_size) override;
};

struct ParsedDataJSON : public ParsedDataStringBase {
    int set_output(rapidjson::Document& document, int value_size) override;

    int set_output(const ArrayVal& array_doc, int value_size) override;
};

template <typename DataImpl>
class VExplodeJsonArrayTableFunction final : public TableFunction {
    ENABLE_FACTORY_CREATOR(VExplodeJsonArrayTableFunction<DataImpl>);

public:
    VExplodeJsonArrayTableFunction();
    ~VExplodeJsonArrayTableFunction() override = default;

    Status process_init(Block* block, RuntimeState* state) override;
    void process_row(size_t row_idx) override;
    void process_close() override;
    void get_same_many_values(MutableColumnPtr& column, int length) override;
    int get_value(MutableColumnPtr& column, int max_step) override;

private:
    void _insert_same_many_values_into_column(MutableColumnPtr& column, int max_step);
    void _insert_values_into_column(MutableColumnPtr& column, int max_step);
    DataImpl _parsed_data;
    ColumnPtr _text_column;
    DataTypePtr _text_datatype;
};

#include "common/compile_check_end.h"
} // namespace vectorized
} // namespace doris