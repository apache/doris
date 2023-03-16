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

#include "vec/exprs/table_function/vexplode_json_array.h"

#include "common/status.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

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

VExplodeJsonArrayTableFunction::VExplodeJsonArrayTableFunction(ExplodeJsonArrayType type)
        : _type(type) {
    _fn_name = "vexplode_json_array";
}

Status VExplodeJsonArrayTableFunction::process_init(vectorized::Block* block) {
    CHECK(_vexpr_context->root()->children().size() == 1)
            << _vexpr_context->root()->children().size();

    int text_column_idx = -1;
    RETURN_IF_ERROR(_vexpr_context->root()->children()[0]->execute(_vexpr_context, block,
                                                                   &text_column_idx));
    _text_column = block->get_by_position(text_column_idx).column;

    return Status::OK();
}

Status VExplodeJsonArrayTableFunction::reset() {
    _eos = false;
    _cur_offset = 0;
    return Status::OK();
}

Status VExplodeJsonArrayTableFunction::process_row(size_t row_idx) {
    _is_current_empty = false;
    _eos = false;

    StringRef text = _text_column->get_data_at(row_idx);
    if (text.data == nullptr) {
        _is_current_empty = true;
    } else {
        rapidjson::Document document;
        document.Parse(text.data, text.size);
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

Status VExplodeJsonArrayTableFunction::process_close() {
    _text_column = nullptr;
    return Status::OK();
}

Status VExplodeJsonArrayTableFunction::get_value_length(int64_t* length) {
    if (_is_current_empty) {
        *length = -1;
    } else {
        _parsed_data.get_value_length(_type, _cur_offset, length);
    }
    return Status::OK();
}

Status VExplodeJsonArrayTableFunction::get_value(void** output) {
    if (_is_current_empty) {
        *output = nullptr;
    } else {
        _parsed_data.get_value(_type, _cur_offset, output, true);
    }
    return Status::OK();
}

} // namespace doris::vectorized