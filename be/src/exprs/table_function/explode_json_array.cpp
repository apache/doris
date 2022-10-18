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

#include "exprs/table_function/explode_json_array.h"

#include "exprs/expr_context.h"

namespace doris {

std::string ParsedData::true_value = "true";
std::string ParsedData::false_value = "false";

int ParsedData::set_output(ExplodeJsonArrayType type, rapidjson::Document& document) {
    int size = document.GetArray().Size();
    switch (type) {
    case ExplodeJsonArrayType::INT: {
        _data.resize(size);
        _backup_int.resize(size);
        int i = 0;
        for (auto& v : document.GetArray()) {
            if (v.IsInt64()) {
                _backup_int[i] = v.GetInt64();
                _data[i] = &_backup_int[i];
            } else {
                _data[i] = nullptr;
            }
            ++i;
        }
        break;
    }
    case ExplodeJsonArrayType::DOUBLE: {
        _data.resize(size);
        _backup_double.resize(size);
        int i = 0;
        for (auto& v : document.GetArray()) {
            if (v.IsDouble()) {
                _backup_double[i] = v.GetDouble();
                _data[i] = &_backup_double[i];
            } else {
                _data[i] = nullptr;
            }
            ++i;
        }
        break;
    }
    case ExplodeJsonArrayType::STRING: {
        _data_string.clear();
        _backup_string.clear();
        _string_nulls.clear();
        int32_t wbytes = 0;
        for (auto& v : document.GetArray()) {
            switch (v.GetType()) {
            case rapidjson::Type::kStringType:
                _backup_string.emplace_back(v.GetString(), v.GetStringLength());
                _string_nulls.push_back(false);
                // do not set _data_string here.
                // Because the address of the string stored in `_backup_string` may
                // change each time `emplace_back()` is called.
                break;
            case rapidjson::Type::kNumberType:
                if (v.IsUint()) {
                    wbytes = sprintf(tmp_buf, "%u", v.GetUint());
                } else if (v.IsInt()) {
                    wbytes = sprintf(tmp_buf, "%d", v.GetInt());
                } else if (v.IsUint64()) {
                    wbytes = sprintf(tmp_buf, "%" PRIu64, v.GetUint64());
                } else if (v.IsInt64()) {
                    wbytes = sprintf(tmp_buf, "%" PRId64, v.GetInt64());
                } else {
                    wbytes = sprintf(tmp_buf, "%f", v.GetDouble());
                }
                _backup_string.emplace_back(tmp_buf, wbytes);
                _string_nulls.push_back(false);
                // do not set _data_string here.
                // Because the address of the string stored in `_backup_string` may
                // change each time `emplace_back()` is called.
                break;
            case rapidjson::Type::kFalseType:
                _data_string.emplace_back(true_value);
                _string_nulls.push_back(false);
                break;
            case rapidjson::Type::kTrueType:
                _data_string.emplace_back(false_value);
                _string_nulls.push_back(false);
                break;
            case rapidjson::Type::kNullType:
                _data_string.push_back({});
                _string_nulls.push_back(true);
                break;
            default:
                _data_string.push_back({});
                _string_nulls.push_back(true);
                break;
            }
        }
        // Must set _data_string at the end, so that we can
        // save the real addr of string in `_backup_string` to `_data_string`.
        for (auto& str : _backup_string) {
            _data_string.emplace_back(str);
        }
        break;
    }
    default:
        CHECK(false) << type;
        break;
    }
    return size;
}

/////////////////////////
ExplodeJsonArrayTableFunction::ExplodeJsonArrayTableFunction(ExplodeJsonArrayType type)
        : _type(type) {
    switch (type) {
    case ExplodeJsonArrayType::INT:
        _fn_name = "explode_json_array_int";
        break;
    case ExplodeJsonArrayType::DOUBLE:
        _fn_name = "explode_json_array_double";
        break;
    case ExplodeJsonArrayType::STRING:
        _fn_name = "explode_json_array_string";
        break;
    default:
        _fn_name = "unknown";
        break;
    }
}

ExplodeJsonArrayTableFunction::~ExplodeJsonArrayTableFunction() {}

Status ExplodeJsonArrayTableFunction::process(TupleRow* tuple_row) {
    CHECK(1 == _expr_context->root()->get_num_children())
            << _expr_context->root()->get_num_children();
    _is_current_empty = false;
    _eos = false;

    StringVal text = _expr_context->root()->get_child(0)->get_string_val(_expr_context, tuple_row);
    if (text.is_null || text.len == 0) {
        _is_current_empty = true;
    } else {
        rapidjson::Document document;
        document.Parse((char*)text.ptr, text.len);
        if (UNLIKELY(document.HasParseError()) || !document.IsArray() ||
            document.GetArray().Size() == 0) {
            _is_current_empty = true;
        } else {
            _cur_size = _parsed_data.set_output(_type, document);
            _cur_offset = 0;
        }
    }
    return Status::OK();
}

void ExplodeJsonArrayTableFunction::_set_null_output() {
    _parsed_data.set_null_output(_type);
    _cur_size = 1;
    _cur_offset = 0;
    _eos = false;
}

Status ExplodeJsonArrayTableFunction::reset() {
    _eos = false;
    _cur_offset = 0;
    return Status::OK();
}

Status ExplodeJsonArrayTableFunction::get_value(void** output) {
    if (_is_current_empty) {
        *output = nullptr;
    } else {
        _parsed_data.get_value(_type, _cur_offset, output);
    }
    return Status::OK();
}
} // namespace doris
