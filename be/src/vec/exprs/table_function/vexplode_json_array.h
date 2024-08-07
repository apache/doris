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

#include <ostream>
#include <string>
#include <vector>

#include "common/status.h"
#include "gutil/integral_types.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/functions/function_string.h"

namespace doris::vectorized {

template <typename T>
struct ParsedData {
    ParsedData() = default;
    virtual ~ParsedData() = default;
    virtual void reset() {
        _backup_data.clear();
        _values_null_flag.clear();
    }
    virtual int set_output(rapidjson::Document& document, int value_size) = 0;
    virtual int set_output(ArrayVal& array_doc, int value_size) = 0;
    virtual void insert_result_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                                int max_step) = 0;
    virtual void insert_many_same_value_from_parsed_data(MutableColumnPtr& column,
                                                         int64_t cur_offset, int length) = 0;
    const char* get_null_flag_address(int cur_offset) {
        return reinterpret_cast<const char*>(_values_null_flag.data() + cur_offset);
    }
    std::vector<UInt8> _values_null_flag;
    std::vector<T> _backup_data;
};

struct ParsedDataInt : public ParsedData<int64_t> {
    static constexpr auto MAX_VALUE = std::numeric_limits<int64_t>::max(); //9223372036854775807
    static constexpr auto MIN_VALUE = std::numeric_limits<int64_t>::min(); //-9223372036854775808

    int set_output(rapidjson::Document& document, int value_size) override {
        _values_null_flag.resize(value_size, 0);
        _backup_data.resize(value_size);
        int i = 0;
        for (auto& v : document.GetArray()) {
            if (v.IsInt64()) {
                _backup_data[i] = v.GetInt64();
            } else if (v.IsUint64()) {
                auto value = v.GetUint64();
                if (value > MAX_VALUE) {
                    _backup_data[i] = MAX_VALUE;
                } else {
                    _backup_data[i] = value;
                }
            } else if (v.IsDouble()) {
                auto value = v.GetDouble();
                if (value > MAX_VALUE) {
                    _backup_data[i] = MAX_VALUE;
                } else if (value < MIN_VALUE) {
                    _backup_data[i] = MIN_VALUE;
                } else {
                    _backup_data[i] = long(value);
                }
            } else {
                _values_null_flag[i] = 1;
                _backup_data[i] = 0;
            }
            ++i;
        }
        return value_size;
    }
    int set_output(ArrayVal& array_doc, int value_size) override {
        _values_null_flag.resize(value_size, 0);
        _backup_data.resize(value_size);
        int i = 0;
        for (auto& val : array_doc) {
            if (val.isInt8()) {
                _backup_data[i] = static_cast<const JsonbInt8Val&>(val).val();
            } else if (val.isInt16()) {
                _backup_data[i] = static_cast<const JsonbInt16Val&>(val).val();
            } else if (val.isInt32()) {
                _backup_data[i] = static_cast<const JsonbInt32Val&>(val).val();
            } else if (val.isInt64()) {
                _backup_data[i] = static_cast<const JsonbInt64Val&>(val).val();
            } else if (val.isDouble()) {
                auto value = static_cast<const JsonbDoubleVal&>(val).val();
                if (value > MAX_VALUE) {
                    _backup_data[i] = MAX_VALUE;
                } else if (value < MIN_VALUE) {
                    _backup_data[i] = MIN_VALUE;
                } else {
                    _backup_data[i] = long(value);
                }
            } else {
                _values_null_flag[i] = 1;
                _backup_data[i] = 0;
            }
            ++i;
        }
        return value_size;
    }

    void insert_result_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                        int max_step) override {
        assert_cast<ColumnInt64*>(column.get())
                ->insert_many_raw_data(
                        reinterpret_cast<const char*>(_backup_data.data() + cur_offset), max_step);
    }

    void insert_many_same_value_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                                 int length) override {
        assert_cast<ColumnInt64*>(column.get())->insert_many_vals(_backup_data[cur_offset], length);
    }
};

struct ParsedDataDouble : public ParsedData<double> {
    int set_output(rapidjson::Document& document, int value_size) override {
        _values_null_flag.resize(value_size, 0);
        _backup_data.resize(value_size);
        int i = 0;
        for (auto& v : document.GetArray()) {
            if (v.IsDouble()) {
                _backup_data[i] = v.GetDouble();
            } else {
                _backup_data[i] = 0;
                _values_null_flag[i] = 1;
            }
            ++i;
        }
        return value_size;
    }

    int set_output(ArrayVal& array_doc, int value_size) override {
        _values_null_flag.resize(value_size, 0);
        _backup_data.resize(value_size);
        int i = 0;
        for (auto& val : array_doc) {
            if (val.isDouble()) {
                _backup_data[i] = static_cast<const JsonbDoubleVal&>(val).val();
            } else {
                _backup_data[i] = 0;
                _values_null_flag[i] = 1;
            }
            ++i;
        }
        return value_size;
    }

    void insert_result_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                        int max_step) override {
        assert_cast<ColumnFloat64*>(column.get())
                ->insert_many_raw_data(
                        reinterpret_cast<const char*>(_backup_data.data() + cur_offset), max_step);
    }

    void insert_many_same_value_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                                 int length) override {
        assert_cast<ColumnFloat64*>(column.get())
                ->insert_many_vals(_backup_data[cur_offset], length);
    }
};

struct ParsedDataStringBase : public ParsedData<std::string> {
    void insert_result_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                        int max_step) override {
        assert_cast<ColumnString*>(column.get())
                ->insert_many_strings(_data_string_ref.data() + cur_offset, max_step);
    }

    void insert_many_same_value_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                                 int length) override {
        assert_cast<ColumnString*>(column.get())
                ->insert_many_data(_data_string_ref[cur_offset].data,
                                   _data_string_ref[cur_offset].size, length);
    }

    void reset() override {
        ParsedData<std::string>::reset();
        _data_string_ref.clear();
    }

    static constexpr const char* TRUE_VALUE = "true";
    static constexpr const char* FALSE_VALUE = "false";
    std::vector<StringRef> _data_string_ref;
    char tmp_buf[128] = {0};
};

struct ParsedDataString : public ParsedDataStringBase {
    int set_output(rapidjson::Document& document, int value_size) override {
        _data_string_ref.clear();
        _backup_data.clear();
        _values_null_flag.clear();
        int32_t wbytes = 0;
        for (auto& v : document.GetArray()) {
            switch (v.GetType()) {
            case rapidjson::Type::kStringType: {
                _backup_data.emplace_back(v.GetString(), v.GetStringLength());
                _values_null_flag.emplace_back(false);
                break;
                // do not set _data_string here.
                // Because the address of the string stored in `_backup_data` may
                // change each time `emplace_back()` is called.
            }
            case rapidjson::Type::kNumberType: {
                if (v.IsUint()) {
                    wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%u", v.GetUint());
                } else if (v.IsInt()) {
                    wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%d", v.GetInt());
                } else if (v.IsUint64()) {
                    wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%" PRIu64, v.GetUint64());
                } else if (v.IsInt64()) {
                    wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%" PRId64, v.GetInt64());
                } else {
                    wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%f", v.GetDouble());
                }
                _backup_data.emplace_back(tmp_buf, wbytes);
                _values_null_flag.emplace_back(false);
                // do not set _data_string here.
                // Because the address of the string stored in `_backup_data` may
                // change each time `emplace_back()` is called.
                break;
            }
            case rapidjson::Type::kFalseType:
                _backup_data.emplace_back(TRUE_VALUE);
                _values_null_flag.emplace_back(false);
                break;
            case rapidjson::Type::kTrueType:
                _backup_data.emplace_back(FALSE_VALUE);
                _values_null_flag.emplace_back(false);
                break;
            case rapidjson::Type::kNullType:
                _backup_data.emplace_back();
                _values_null_flag.emplace_back(true);
                break;
            default:
                _backup_data.emplace_back();
                _values_null_flag.emplace_back(true);
                break;
            }
        }
        // Must set _data_string at the end, so that we can
        // save the real addr of string in `_backup_data` to `_data_string`.
        for (auto& str : _backup_data) {
            _data_string_ref.emplace_back(str.data(), str.length());
        }
        return value_size;
    }

    int set_output(ArrayVal& array_doc, int value_size) override {
        _data_string_ref.clear();
        _backup_data.clear();
        _values_null_flag.clear();
        int32_t wbytes = 0;
        for (auto& val : array_doc) {
            switch (val.type()) {
            case JsonbType::T_String: {
                _backup_data.emplace_back(static_cast<const JsonbStringVal&>(val).getBlob(),
                                          static_cast<const JsonbStringVal&>(val).getBlobLen());
                _values_null_flag.emplace_back(false);
                break;
                // do not set _data_string here.
                // Because the address of the string stored in `_backup_data` may
                // change each time `emplace_back()` is called.
            }
            case JsonbType::T_Int8: {
                wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%d",
                                  static_cast<const JsonbInt8Val&>(val).val());
                _backup_data.emplace_back(tmp_buf, wbytes);
                _values_null_flag.emplace_back(false);
                break;
            }
            case JsonbType::T_Int16: {
                wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%d",
                                  static_cast<const JsonbInt16Val&>(val).val());
                _backup_data.emplace_back(tmp_buf, wbytes);
                _values_null_flag.emplace_back(false);
                break;
            }
            case JsonbType::T_Int64: {
                wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%" PRId64,
                                  static_cast<const JsonbInt64Val&>(val).val());
                _backup_data.emplace_back(tmp_buf, wbytes);
                _values_null_flag.emplace_back(false);
                break;
            }
            case JsonbType::T_Double: {
                wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%f",
                                  static_cast<const JsonbDoubleVal&>(val).val());
                _backup_data.emplace_back(tmp_buf, wbytes);
                _values_null_flag.emplace_back(false);
                break;
            }
            case JsonbType::T_Int32: {
                wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%d",
                                  static_cast<const JsonbInt32Val&>(val).val());
                _backup_data.emplace_back(tmp_buf, wbytes);
                _values_null_flag.emplace_back(false);
                break;
            }
            case JsonbType::T_True:
                _backup_data.emplace_back(TRUE_VALUE);
                _values_null_flag.emplace_back(false);
                break;
            case JsonbType::T_False:
                _backup_data.emplace_back(FALSE_VALUE);
                _values_null_flag.emplace_back(false);
                break;
            case JsonbType::T_Null:
                _backup_data.emplace_back();
                _values_null_flag.emplace_back(true);
                break;
            default:
                _backup_data.emplace_back();
                _values_null_flag.emplace_back(true);
                break;
            }
        }
        // Must set _data_string at the end, so that we can
        // save the real addr of string in `_backup_data` to `_data_string`.
        for (auto& str : _backup_data) {
            _data_string_ref.emplace_back(str.data(), str.length());
        }
        return value_size;
    }
};

struct ParsedDataJSON : public ParsedDataStringBase {
    int set_output(rapidjson::Document& document, int value_size) override {
        _data_string_ref.clear();
        _backup_data.clear();
        _values_null_flag.clear();
        for (auto& v : document.GetArray()) {
            if (v.IsObject()) {
                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                v.Accept(writer);
                _backup_data.emplace_back(buffer.GetString(), buffer.GetSize());
                _values_null_flag.emplace_back(false);
            } else {
                _backup_data.emplace_back();
                _values_null_flag.emplace_back(true);
            }
        }
        // Must set _data_string at the end, so that we can
        // save the real addr of string in `_backup_data` to `_data_string`.
        for (auto& str : _backup_data) {
            _data_string_ref.emplace_back(str);
        }
        return value_size;
    }

    int set_output(ArrayVal& array_doc, int value_size) override {
        _data_string_ref.clear();
        _backup_data.clear();
        _values_null_flag.clear();
        auto writer = std::make_unique<JsonbWriter>();
        for (auto& v : array_doc) {
            if (v.isObject()) {
                writer->reset();
                writer->writeValue(&v);
                _backup_data.emplace_back(writer->getOutput()->getBuffer(),
                                          writer->getOutput()->getSize());
                _values_null_flag.emplace_back(false);
            } else {
                _backup_data.emplace_back();
                _values_null_flag.emplace_back(true);
            }
        }
        // Must set _data_string at the end, so that we can
        // save the real addr of string in `_backup_data` to `_data_string`.
        for (auto& str : _backup_data) {
            _data_string_ref.emplace_back(str);
        }
        return value_size;
    }
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

} // namespace doris::vectorized