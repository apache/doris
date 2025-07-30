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

#include <glog/logging.h>
#include <rapidjson/rapidjson.h>

#include <algorithm>
#include <cstdio>
#include <string>
#include <vector>

#include "common/status.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

int ParsedDataInt::set_output(rapidjson::Document& document, int value_size) {
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
            // target slot is int64(cast double to int64). so compare with int64_max
            if (static_cast<int64_t>(value) > MAX_VALUE) {
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
int ParsedDataInt::set_output(const ArrayVal& array_doc, int value_size) {
    _values_null_flag.resize(value_size, 0);
    _backup_data.resize(value_size);
    int i = 0;
    for (const auto& val : array_doc) {
        if (val.isInt8()) {
            _backup_data[i] = val.unpack<JsonbInt8Val>()->val();
        } else if (val.isInt16()) {
            _backup_data[i] = val.unpack<JsonbInt16Val>()->val();
        } else if (val.isInt32()) {
            _backup_data[i] = val.unpack<JsonbInt32Val>()->val();
        } else if (val.isInt64()) {
            _backup_data[i] = val.unpack<JsonbInt64Val>()->val();
        } else if (val.isDouble()) {
            auto value = val.unpack<JsonbDoubleVal>()->val();
            // target slot is int64(cast double to int64). so compare with int64_max
            if (static_cast<int64_t>(value) > MAX_VALUE) {
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

void ParsedDataInt::insert_result_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                                   int max_step) {
    assert_cast<ColumnInt64*>(column.get())
            ->insert_many_raw_data(reinterpret_cast<const char*>(_backup_data.data() + cur_offset),
                                   max_step);
}

void ParsedDataInt::insert_many_same_value_from_parsed_data(MutableColumnPtr& column,
                                                            int64_t cur_offset, int length) {
    assert_cast<ColumnInt64*>(column.get())->insert_many_vals(_backup_data[cur_offset], length);
}

int ParsedDataDouble::set_output(rapidjson::Document& document, int value_size) {
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

int ParsedDataDouble::set_output(const ArrayVal& array_doc, int value_size) {
    _values_null_flag.resize(value_size, 0);
    _backup_data.resize(value_size);
    int i = 0;
    for (const auto& val : array_doc) {
        if (val.isDouble()) {
            _backup_data[i] = val.unpack<JsonbDoubleVal>()->val();
        } else {
            _backup_data[i] = 0;
            _values_null_flag[i] = 1;
        }
        ++i;
    }
    return value_size;
}

void ParsedDataDouble::insert_result_from_parsed_data(MutableColumnPtr& column, int64_t cur_offset,
                                                      int max_step) {
    assert_cast<ColumnFloat64*>(column.get())
            ->insert_many_raw_data(reinterpret_cast<const char*>(_backup_data.data() + cur_offset),
                                   max_step);
}

void ParsedDataDouble::insert_many_same_value_from_parsed_data(MutableColumnPtr& column,
                                                               int64_t cur_offset, int length) {
    assert_cast<ColumnFloat64*>(column.get())->insert_many_vals(_backup_data[cur_offset], length);
}

void ParsedDataStringBase::insert_result_from_parsed_data(MutableColumnPtr& column,
                                                          int64_t cur_offset, int max_step) {
    assert_cast<ColumnString*>(column.get())
            ->insert_many_strings(_data_string_ref.data() + cur_offset, max_step);
}

void ParsedDataStringBase::insert_many_same_value_from_parsed_data(MutableColumnPtr& column,
                                                                   int64_t cur_offset, int length) {
    assert_cast<ColumnString*>(column.get())
            ->insert_data_repeatedly(_data_string_ref[cur_offset].data,
                                     _data_string_ref[cur_offset].size, length);
}

void ParsedDataStringBase::reset() {
    ParsedData<std::string>::reset();
    _data_string_ref.clear();
}

int ParsedDataString::set_output(rapidjson::Document& document, int value_size) {
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

int ParsedDataString::set_output(const ArrayVal& array_doc, int value_size) {
    _data_string_ref.clear();
    _backup_data.clear();
    _values_null_flag.clear();
    int32_t wbytes = 0;
    for (const auto& val : array_doc) {
        switch (val.type) {
        case JsonbType::T_String: {
            _backup_data.emplace_back(val.unpack<JsonbStringVal>()->getBlob(),
                                      val.unpack<JsonbStringVal>()->getBlobLen());
            _values_null_flag.emplace_back(false);
            break;
            // do not set _data_string here.
            // Because the address of the string stored in `_backup_data` may
            // change each time `emplace_back()` is called.
        }
        case JsonbType::T_Int8: {
            wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%d", val.unpack<JsonbInt8Val>()->val());
            _backup_data.emplace_back(tmp_buf, wbytes);
            _values_null_flag.emplace_back(false);
            break;
        }
        case JsonbType::T_Int16: {
            wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%d", val.unpack<JsonbInt16Val>()->val());
            _backup_data.emplace_back(tmp_buf, wbytes);
            _values_null_flag.emplace_back(false);
            break;
        }
        case JsonbType::T_Int64: {
            wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%" PRId64,
                              val.unpack<JsonbInt64Val>()->val());
            _backup_data.emplace_back(tmp_buf, wbytes);
            _values_null_flag.emplace_back(false);
            break;
        }
        case JsonbType::T_Double: {
            wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%f", val.unpack<JsonbDoubleVal>()->val());
            _backup_data.emplace_back(tmp_buf, wbytes);
            _values_null_flag.emplace_back(false);
            break;
        }
        case JsonbType::T_Int32: {
            wbytes = snprintf(tmp_buf, sizeof(tmp_buf), "%d", val.unpack<JsonbInt32Val>()->val());
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

int ParsedDataJSON::set_output(rapidjson::Document& document, int value_size) {
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

int ParsedDataJSON::set_output(const ArrayVal& array_doc, int value_size) {
    _data_string_ref.clear();
    _backup_data.clear();
    _values_null_flag.clear();
    auto writer = std::make_unique<JsonbWriter>();
    for (const auto& v : array_doc) {
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

template <typename DataImpl>
VExplodeJsonArrayTableFunction<DataImpl>::VExplodeJsonArrayTableFunction() : TableFunction() {
    _fn_name = "vexplode_json_array";
}

template <typename DataImpl>
Status VExplodeJsonArrayTableFunction<DataImpl>::process_init(Block* block, RuntimeState* state) {
    CHECK(_expr_context->root()->children().size() == 1)
            << _expr_context->root()->children().size();

    int text_column_idx = -1;
    RETURN_IF_ERROR(_expr_context->root()->children()[0]->execute(_expr_context.get(), block,
                                                                  &text_column_idx));
    _text_column = block->get_by_position(text_column_idx).column;
    _text_datatype = remove_nullable(block->get_by_position(text_column_idx).type);
    return Status::OK();
}

template <typename DataImpl>
void VExplodeJsonArrayTableFunction<DataImpl>::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);

    StringRef text = _text_column->get_data_at(row_idx);
    if (text.data != nullptr) {
        if (_text_datatype->get_primitive_type() == TYPE_JSONB) {
            JsonbDocument* doc = nullptr;
            auto st = JsonbDocument::checkAndCreateDocument(text.data, text.size, &doc);
            if (st.ok() && doc && doc->getValue() && doc->getValue()->isArray()) {
                const auto* a = doc->getValue()->unpack<ArrayVal>();
                if (a->numElem() > 0) {
                    _cur_size = _parsed_data.set_output(*a, a->numElem());
                }
            }
        } else {
            rapidjson::Document document;
            document.Parse(text.data, text.size);
            if (!document.HasParseError() && document.IsArray() && document.GetArray().Size()) {
                _cur_size = _parsed_data.set_output(document, document.GetArray().Size());
            }
        }
    }
}

template <typename DataImpl>
void VExplodeJsonArrayTableFunction<DataImpl>::process_close() {
    _text_column = nullptr;
    _text_datatype = nullptr;
    _parsed_data.reset();
}

template <typename DataImpl>
void VExplodeJsonArrayTableFunction<DataImpl>::get_same_many_values(MutableColumnPtr& column,
                                                                    int length) {
    if (current_empty()) {
        column->insert_many_defaults(length);
    } else {
        _insert_same_many_values_into_column(column, length);
    }
}

template <typename DataImpl>
int VExplodeJsonArrayTableFunction<DataImpl>::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        _insert_values_into_column(column, max_step);
    }
    forward(max_step);
    return max_step;
}

template <typename DataImpl>
void VExplodeJsonArrayTableFunction<DataImpl>::_insert_same_many_values_into_column(
        MutableColumnPtr& column, int length) {
    if (_is_nullable) {
        auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
        auto nested_column = nullable_column->get_nested_column_ptr();

        _parsed_data.insert_many_same_value_from_parsed_data(nested_column, _cur_offset, length);

        auto* nullmap_column =
                assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
        size_t old_size = nullmap_column->size();
        nullmap_column->resize(old_size + length);
        memset(nullmap_column->get_data().data() + old_size,
               *(_parsed_data.get_null_flag_address(_cur_offset)), length * sizeof(UInt8));
    } else {
        _parsed_data.insert_many_same_value_from_parsed_data(column, _cur_offset, length);
    }
}

template <typename DataImpl>
void VExplodeJsonArrayTableFunction<DataImpl>::_insert_values_into_column(MutableColumnPtr& column,
                                                                          int max_step) {
    if (_is_nullable) {
        auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
        auto nested_column = nullable_column->get_nested_column_ptr();

        _parsed_data.insert_result_from_parsed_data(nested_column, _cur_offset, max_step);

        auto* nullmap_column =
                assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
        size_t old_size = nullmap_column->size();
        nullmap_column->resize(old_size + max_step);
        memcpy(nullmap_column->get_data().data() + old_size,
               _parsed_data.get_null_flag_address(_cur_offset), max_step * sizeof(UInt8));
    } else {
        _parsed_data.insert_result_from_parsed_data(column, _cur_offset, max_step);
    }
}

template class VExplodeJsonArrayTableFunction<ParsedDataInt>;
template class VExplodeJsonArrayTableFunction<ParsedDataDouble>;
template class VExplodeJsonArrayTableFunction<ParsedDataString>;
template class VExplodeJsonArrayTableFunction<ParsedDataJSON>;

#include "common/compile_check_end.h"
} // namespace doris::vectorized
