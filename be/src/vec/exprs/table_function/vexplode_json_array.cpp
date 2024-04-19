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
#include <inttypes.h>
#include <rapidjson/rapidjson.h>
#include <stdio.h>

#include <algorithm>
#include <limits>

#include "common/status.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "vec/columns/column.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

std::string ParsedData::true_value = "true";
std::string ParsedData::false_value = "false";
auto max_value = std::numeric_limits<int64_t>::max(); //9223372036854775807
auto min_value = std::numeric_limits<int64_t>::min(); //-9223372036854775808

int ParsedData::set_output(rapidjson::Document& document) {
    int size = document.GetArray().Size();
    _values_null_flag.resize(size, 0);
    switch (_data_type) {
    case ExplodeJsonArrayType::INT: {
        _backup_int.resize(size);
        int i = 0;
        for (auto& v : document.GetArray()) {
            if (v.IsInt64()) {
                _backup_int[i] = v.GetInt64();
            } else if (v.IsUint64()) {
                auto value = v.GetUint64();
                if (value > max_value) {
                    _backup_int[i] = max_value;
                } else {
                    _backup_int[i] = value;
                }
            } else if (v.IsDouble()) {
                auto value = v.GetDouble();
                if (value > max_value) {
                    _backup_int[i] = max_value;
                } else if (value < min_value) {
                    _backup_int[i] = min_value;
                } else {
                    _backup_int[i] = long(value);
                }
            } else {
                _values_null_flag[i] = 1;
                _backup_int[i] = 0;
            }
            ++i;
        }
        break;
    }
    case ExplodeJsonArrayType::DOUBLE: {
        _backup_double.resize(size);
        int i = 0;
        for (auto& v : document.GetArray()) {
            if (v.IsDouble()) {
                _backup_double[i] = v.GetDouble();
            } else {
                _backup_double[i] = 0;
                _values_null_flag[i] = 1;
            }
            ++i;
        }
        break;
    }
    case ExplodeJsonArrayType::STRING: {
        int32_t wbytes = 0;
        int i = 0;
        for (auto& v : document.GetArray()) {
            switch (v.GetType()) {
            case rapidjson::Type::kStringType:
                _backup_string.emplace_back(v.GetString(), v.GetStringLength());
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
                break;
            case rapidjson::Type::kFalseType:
                _backup_string.emplace_back(true_value);
                break;
            case rapidjson::Type::kTrueType:
                _backup_string.emplace_back(false_value);
                break;
            case rapidjson::Type::kNullType:
                _backup_string.emplace_back("", 0);
                _values_null_flag[i] = 1;
                break;
            default:
                _backup_string.emplace_back("", 0);
                _values_null_flag[i] = 1;
                break;
            }
            ++i;
        }
        break;
    }
    case ExplodeJsonArrayType::JSON: {
        int i = 0;
        for (auto& v : document.GetArray()) {
            if (v.IsObject()) {
                rapidjson::StringBuffer buffer;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
                v.Accept(writer);
                _backup_string.emplace_back(buffer.GetString(), buffer.GetSize());
            } else {
                _backup_string.emplace_back("", 0);
                _values_null_flag[i] = 1;
            }
            ++i;
        }
        break;
    }
    default:
        CHECK(false) << _data_type;
        break;
    }
    return size;
}

Status ParsedData::insert_result_from_parsed_data(MutableColumnPtr& column, int max_step,
                                                  int64_t cur_offset) {
    switch (_data_type) {
    case ExplodeJsonArrayType::INT: {
        assert_cast<ColumnInt32*>(column.get())
                ->insert_many_raw_data(
                        reinterpret_cast<const char*>(_backup_int.data() + cur_offset), max_step);
    }
    case ExplodeJsonArrayType::DOUBLE: {
        assert_cast<ColumnFloat64*>(column.get())
                ->insert_many_raw_data(
                        reinterpret_cast<const char*>(_backup_double.data() + cur_offset),
                        max_step);
    }
    case ExplodeJsonArrayType::JSON:
    case ExplodeJsonArrayType::STRING: {
        assert_cast<ColumnString*>(column.get())
                ->insert_many_strings(_backup_string.data() + cur_offset, max_step);
    }
    default:
        auto error_msg = fmt::format(
                "Type not implemented:{} need check it in function explode array insert into "
                "result",
                _data_type);
        return Status::InternalError(error_msg);
    }
    return Status::OK();
}

Status ParsedData::set_type(ExplodeJsonArrayType type) {
    switch (type) {
    case ExplodeJsonArrayType::INT:
    case ExplodeJsonArrayType::DOUBLE:
    case ExplodeJsonArrayType::JSON:
    case ExplodeJsonArrayType::STRING:
        _data_type = type;
    default:
        auto error_msg = fmt::format(
                "Type not implemented:{} need check it in function explode array", type);
        return Status::InternalError(error_msg);
    }
    return Status::OK();
}

VExplodeJsonArrayTableFunction::VExplodeJsonArrayTableFunction(ExplodeJsonArrayType type)
        : _type(type) {
    _fn_name = "vexplode_json_array";
}

Status VExplodeJsonArrayTableFunction::process_init(Block* block, RuntimeState* state) {
    CHECK(_expr_context->root()->children().size() == 1)
            << _expr_context->root()->children().size();

    int text_column_idx = -1;
    RETURN_IF_ERROR(_expr_context->root()->children()[0]->execute(_expr_context.get(), block,
                                                                  &text_column_idx));
    _text_column = block->get_by_position(text_column_idx).column;
    RETURN_IF_ERROR(_parsed_data.set_type(_type));
    return Status::OK();
}

void VExplodeJsonArrayTableFunction::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);

    StringRef text = _text_column->get_data_at(row_idx);
    if (text.data != nullptr) {
        rapidjson::Document document;
        document.Parse(text.data, text.size);
        if (!document.HasParseError() && document.IsArray() && document.GetArray().Size()) {
            _cur_size = _parsed_data.set_output(document);
        }
    }
}

void VExplodeJsonArrayTableFunction::process_close() {
    _text_column = nullptr;
    _parsed_data.reset();
}

void VExplodeJsonArrayTableFunction::get_value(MutableColumnPtr& column) {
    DCHECK(false) << " should not run into VExplodeJsonArrayTableFunction::get_value";
}

int VExplodeJsonArrayTableFunction::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, (int)(_cur_size - _cur_offset));
    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        if (_is_nullable) {
            auto* nullable_column = assert_cast<ColumnNullable*>(column.get());
            auto nested_column = nullable_column->get_nested_column_ptr();
            RETURN_IF_ERROR(
                    _parsed_data.insert_result_from_parsed_data(column, max_step, _cur_offset));

            auto* nullmap_column =
                    assert_cast<ColumnUInt8*>(nullable_column->get_null_map_column_ptr().get());
            size_t old_size = nullmap_column->size();
            nullmap_column->resize(old_size + max_step);
            memcpy(nullmap_column->get_data().data() + old_size,
                   _parsed_data.get_null_flag_address(_cur_offset), max_step * sizeof(UInt8));
        } else {
            RETURN_IF_ERROR(
                    _parsed_data.insert_result_from_parsed_data(column, max_step, _cur_offset));
        }
    }
    forward(max_step);
    return max_step;
}

} // namespace doris::vectorized