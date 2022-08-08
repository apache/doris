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

#include "vec/exec/vjson_scanner.h"

#include <fmt/format.h>

#include <algorithm>

#include "exec/line_reader.h"
#include "exprs/json_functions.h"
#include "runtime/runtime_state.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

VJsonScanner::VJsonScanner(RuntimeState* state, RuntimeProfile* profile,
                           const TBrokerScanRangeParams& params,
                           const std::vector<TBrokerRangeDesc>& ranges,
                           const std::vector<TNetworkAddress>& broker_addresses,
                           const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : JsonScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs, counter),
          _cur_vjson_reader(nullptr) {}

Status VJsonScanner::get_next(vectorized::Block* output_block, bool* eof) {
    SCOPED_TIMER(_read_timer);
    RETURN_IF_ERROR(_init_src_block());
    const int batch_size = _state->batch_size();

    auto columns = _src_block.mutate_columns();
    // Get one line
    while (columns[0]->size() < batch_size && !_scanner_eof) {
        if (_cur_file_reader == nullptr || _cur_reader_eof) {
            RETURN_IF_ERROR(open_next_reader());
            // If there isn't any more reader, break this
            if (_scanner_eof) {
                break;
            }
        }

        if (_read_json_by_line && _skip_next_line) {
            size_t size = 0;
            const uint8_t* line_ptr = nullptr;
            RETURN_IF_ERROR(_cur_line_reader->read_line(&line_ptr, &size, &_cur_reader_eof));
            _skip_next_line = false;
            continue;
        }

        bool is_empty_row = false;
        RETURN_IF_ERROR(_cur_vjson_reader->read_json_column(columns, _src_slot_descs, &is_empty_row,
                                                            &_cur_reader_eof));
        if (is_empty_row) {
            // Read empty row, just continue
            continue;
        }
    }

    COUNTER_UPDATE(_rows_read_counter, columns[0]->size());
    SCOPED_TIMER(_materialize_timer);

    return _fill_dest_block(output_block, eof);
}

Status VJsonScanner::open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(JsonScanner::open_based_reader());
    RETURN_IF_ERROR(open_vjson_reader());
    _next_range++;
    return Status::OK();
}

Status VJsonScanner::open_vjson_reader() {
    if (_cur_vjson_reader != nullptr) {
        _cur_vjson_reader.reset();
    }
    std::string json_root = "";
    std::string jsonpath = "";
    bool strip_outer_array = false;
    bool num_as_string = false;
    bool fuzzy_parse = false;

    RETURN_IF_ERROR(JsonScanner::get_range_params(jsonpath, json_root, strip_outer_array,
                                                  num_as_string, fuzzy_parse));
    if (_read_json_by_line) {
        _cur_vjson_reader.reset(new VJsonReader(_state, _counter, _profile, strip_outer_array,
                                                num_as_string, fuzzy_parse, &_scanner_eof, nullptr,
                                                _cur_line_reader));
    } else {
        _cur_vjson_reader.reset(new VJsonReader(_state, _counter, _profile, strip_outer_array,
                                                num_as_string, fuzzy_parse, &_scanner_eof,
                                                _cur_file_reader.get()));
    }

    RETURN_IF_ERROR(_cur_vjson_reader->init(jsonpath, json_root));
    return Status::OK();
}

VJsonReader::VJsonReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                         bool strip_outer_array, bool num_as_string, bool fuzzy_parse,
                         bool* scanner_eof, FileReader* file_reader, LineReader* line_reader)
        : JsonReader(state, counter, profile, strip_outer_array, num_as_string, fuzzy_parse,
                     scanner_eof, file_reader, line_reader),
          _vhandle_json_callback(nullptr) {}

VJsonReader::~VJsonReader() {}

Status VJsonReader::init(const std::string& jsonpath, const std::string& json_root) {
    // generate _parsed_jsonpaths and _parsed_json_root
    RETURN_IF_ERROR(JsonReader::_parse_jsonpath_and_json_root(jsonpath, json_root));

    //improve performance
    if (_parsed_jsonpaths.empty()) { // input is a simple json-string
        _vhandle_json_callback = &VJsonReader::_vhandle_simple_json;
    } else { // input is a complex json-string and a json-path
        if (_strip_outer_array) {
            _vhandle_json_callback = &VJsonReader::_vhandle_flat_array_complex_json;
        } else {
            _vhandle_json_callback = &VJsonReader::_vhandle_nested_complex_json;
        }
    }

    return Status::OK();
}

Status VJsonReader::read_json_column(std::vector<MutableColumnPtr>& columns,
                                     const std::vector<SlotDescriptor*>& slot_descs,
                                     bool* is_empty_row, bool* eof) {
    return (this->*_vhandle_json_callback)(columns, slot_descs, is_empty_row, eof);
}

Status VJsonReader::_vhandle_simple_json(std::vector<MutableColumnPtr>& columns,
                                         const std::vector<SlotDescriptor*>& slot_descs,
                                         bool* is_empty_row, bool* eof) {
    do {
        bool valid = false;
        if (_next_line >= _total_lines) { // parse json and generic document
            Status st = _parse_json(is_empty_row, eof);
            if (st.is_data_quality_error()) {
                continue; // continue to read next
            }
            RETURN_IF_ERROR(st);
            if (*is_empty_row == true) {
                return Status::OK();
            }
            _name_map.clear();
            rapidjson::Value* objectValue = nullptr;
            if (_json_doc->IsArray()) {
                _total_lines = _json_doc->Size();
                if (_total_lines == 0) {
                    // may be passing an empty json, such as "[]"
                    RETURN_IF_ERROR(_append_error_msg(*_json_doc, "Empty json line", "", nullptr));
                    if (*_scanner_eof) {
                        *is_empty_row = true;
                        return Status::OK();
                    }
                    continue;
                }
                objectValue = &(*_json_doc)[0];
            } else {
                _total_lines = 1; // only one row
                objectValue = _json_doc;
            }
            _next_line = 0;
            if (_fuzzy_parse) {
                for (auto v : slot_descs) {
                    for (int i = 0; i < objectValue->MemberCount(); ++i) {
                        auto it = objectValue->MemberBegin() + i;
                        if (v->col_name() == it->name.GetString()) {
                            _name_map[v->col_name()] = i;
                            break;
                        }
                    }
                }
            }
        }

        if (_json_doc->IsArray()) {                                   // handle case 1
            rapidjson::Value& objectValue = (*_json_doc)[_next_line]; // json object
            RETURN_IF_ERROR(_set_column_value(objectValue, columns, slot_descs, &valid));
        } else { // handle case 2
            RETURN_IF_ERROR(_set_column_value(*_json_doc, columns, slot_descs, &valid));
        }
        _next_line++;
        if (!valid) {
            if (*_scanner_eof) {
                // When _scanner_eof is true and valid is false, it means that we have encountered
                // unqualified data and decided to stop the scan.
                *is_empty_row = true;
                return Status::OK();
            }
            continue;
        }
        *is_empty_row = false;
        break; // get a valid row, then break
    } while (_next_line <= _total_lines);
    return Status::OK();
}

// for simple format json
// set valid to true and return OK if succeed.
// set valid to false and return OK if we met an invalid row.
// return other status if encounter other problmes.
Status VJsonReader::_set_column_value(rapidjson::Value& objectValue,
                                      std::vector<MutableColumnPtr>& columns,
                                      const std::vector<SlotDescriptor*>& slot_descs, bool* valid) {
    if (!objectValue.IsObject()) {
        // Here we expect the incoming `objectValue` to be a Json Object, such as {"key" : "value"},
        // not other type of Json format.
        RETURN_IF_ERROR(_append_error_msg(objectValue, "Expect json object value", "", valid));
        return Status::OK();
    }

    int nullcount = 0;
    int ctx_idx = 0;
    for (auto slot_desc : slot_descs) {
        if (!slot_desc->is_materialized()) {
            continue;
        }

        int dest_index = ctx_idx++;
        auto* column_ptr = columns[dest_index].get();
        rapidjson::Value::ConstMemberIterator it = objectValue.MemberEnd();

        if (_fuzzy_parse) {
            auto idx_it = _name_map.find(slot_desc->col_name());
            if (idx_it != _name_map.end() && idx_it->second < objectValue.MemberCount()) {
                it = objectValue.MemberBegin() + idx_it->second;
            }
        } else {
            it = objectValue.FindMember(
                    rapidjson::Value(slot_desc->col_name().c_str(), slot_desc->col_name().size()));
        }

        if (it != objectValue.MemberEnd()) {
            const rapidjson::Value& value = it->value;
            RETURN_IF_ERROR(_write_data_to_column(&value, slot_desc, column_ptr, valid));
            if (!(*valid)) {
                return Status::OK();
            }
        } else { // not found
            if (slot_desc->is_nullable()) {
                auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
                nullable_column->insert_default();
                nullcount++;
            } else {
                RETURN_IF_ERROR(_append_error_msg(
                        objectValue,
                        "The column `{}` is not nullable, but it's not found in jsondata.",
                        slot_desc->col_name(), valid));
                break;
            }
        }
    }

    if (nullcount == slot_descs.size()) {
        RETURN_IF_ERROR(_append_error_msg(objectValue, "All fields is null, this is a invalid row.",
                                          "", valid));
        return Status::OK();
    }
    *valid = true;
    return Status::OK();
}

Status VJsonReader::_write_data_to_column(rapidjson::Value::ConstValueIterator value,
                                          SlotDescriptor* slot_desc,
                                          vectorized::IColumn* column_ptr, bool* valid) {
    const char* str_value = nullptr;
    char tmp_buf[128] = {0};
    int32_t wbytes = 0;
    std::string json_str;

    vectorized::ColumnNullable* nullable_column = nullptr;
    if (slot_desc->is_nullable()) {
        nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
        nullable_column->get_null_map_data().push_back(0);
        column_ptr = &nullable_column->get_nested_column();
    }

    switch (value->GetType()) {
    case rapidjson::Type::kStringType:
        str_value = value->GetString();
        wbytes = strlen(str_value);
        break;
    case rapidjson::Type::kNumberType:
        if (value->IsUint()) {
            wbytes = sprintf(tmp_buf, "%u", value->GetUint());
        } else if (value->IsInt()) {
            wbytes = sprintf(tmp_buf, "%d", value->GetInt());
        } else if (value->IsUint64()) {
            wbytes = sprintf(tmp_buf, "%lu", value->GetUint64());
        } else if (value->IsInt64()) {
            wbytes = sprintf(tmp_buf, "%ld", value->GetInt64());
        } else {
            wbytes = sprintf(tmp_buf, "%f", value->GetDouble());
        }
        str_value = tmp_buf;
        break;
    case rapidjson::Type::kFalseType:
        wbytes = 1;
        str_value = (char*)"0";
        break;
    case rapidjson::Type::kTrueType:
        wbytes = 1;
        str_value = (char*)"1";
        break;
    case rapidjson::Type::kNullType:
        if (slot_desc->is_nullable()) {
            nullable_column->insert_default();
        } else {
            RETURN_IF_ERROR(_append_error_msg(
                    *value, "Json value is null, but the column `{}` is not nullable.",
                    slot_desc->col_name(), valid));
            return Status::OK();
        }
        // return immediately to prevent from repeatedly insert_data
        *valid = true;
        return Status::OK();
    default:
        // for other type like array or object. we convert it to string to save
        json_str = JsonReader::_print_json_value(*value);
        wbytes = json_str.size();
        str_value = json_str.c_str();
        break;
    }

    // TODO: if the vexpr can support another 'slot_desc type' than 'TYPE_VARCHAR',
    // we need use a function to support these types to insert data in columns.
    DCHECK(slot_desc->type().type == TYPE_VARCHAR);
    assert_cast<ColumnString*>(column_ptr)->insert_data(str_value, wbytes);

    *valid = true;
    return Status::OK();
}

Status VJsonReader::_vhandle_flat_array_complex_json(std::vector<MutableColumnPtr>& columns,
                                                     const std::vector<SlotDescriptor*>& slot_descs,
                                                     bool* is_empty_row, bool* eof) {
    do {
        if (_next_line >= _total_lines) {
            Status st = _parse_json(is_empty_row, eof);
            if (st.is_data_quality_error()) {
                continue; // continue to read next
            }
            RETURN_IF_ERROR(st);
            if (*is_empty_row == true) {
                if (st == Status::OK()) {
                    return Status::OK();
                }
                if (_total_lines == 0) {
                    continue;
                }
            }
        }
        rapidjson::Value& objectValue = (*_json_doc)[_next_line++];
        bool valid = true;
        RETURN_IF_ERROR(_write_columns_by_jsonpath(objectValue, slot_descs, columns, &valid));
        if (!valid) {
            continue; // process next line
        }
        *is_empty_row = false;
        break; // get a valid row, then break
    } while (_next_line <= _total_lines);
    return Status::OK();
}

Status VJsonReader::_vhandle_nested_complex_json(std::vector<MutableColumnPtr>& columns,
                                                 const std::vector<SlotDescriptor*>& slot_descs,
                                                 bool* is_empty_row, bool* eof) {
    while (true) {
        Status st = _parse_json(is_empty_row, eof);
        if (st.is_data_quality_error()) {
            continue; // continue to read next
        }
        RETURN_IF_ERROR(st);
        if (*is_empty_row == true) {
            return Status::OK();
        }
        *is_empty_row = false;
        break; // read a valid row
    }
    bool valid = true;
    RETURN_IF_ERROR(_write_columns_by_jsonpath(*_json_doc, slot_descs, columns, &valid));
    if (!valid) {
        // there is only one line in this case, so if it return false, just set is_empty_row true
        // so that the caller will continue reading next line.
        *is_empty_row = true;
    }
    return Status::OK();
}

Status VJsonReader::_write_columns_by_jsonpath(rapidjson::Value& objectValue,
                                               const std::vector<SlotDescriptor*>& slot_descs,
                                               std::vector<MutableColumnPtr>& columns,
                                               bool* valid) {
    int nullcount = 0;
    int ctx_idx = 0;
    size_t column_num = slot_descs.size();
    for (size_t i = 0; i < column_num; i++) {
        int dest_index = ctx_idx++;
        auto* column_ptr = columns[dest_index].get();
        rapidjson::Value* json_values = nullptr;
        bool wrap_explicitly = false;
        if (LIKELY(i < _parsed_jsonpaths.size())) {
            json_values = JsonFunctions::get_json_array_from_parsed_json(
                    _parsed_jsonpaths[i], &objectValue, _origin_json_doc.GetAllocator(),
                    &wrap_explicitly);
        }

        if (json_values == nullptr) {
            // not match in jsondata.
            if (slot_descs[i]->is_nullable()) {
                auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
                nullable_column->insert_default();
                nullcount++;
            } else {
                RETURN_IF_ERROR(_append_error_msg(
                        objectValue,
                        "The column `{}` is not nullable, but it's not found in jsondata.",
                        slot_descs[i]->col_name(), valid));
                break;
            }
        } else {
            CHECK(json_values->IsArray());
            if (json_values->Size() == 1 && wrap_explicitly) {
                // NOTICE1: JsonFunctions::get_json_array_from_parsed_json() will wrap the single json object with an array.
                // so here we unwrap the array to get the real element.
                // if json_values' size > 1, it means we just match an array, not a wrapped one, so no need to unwrap.
                json_values = &((*json_values)[0]);
            }
            RETURN_IF_ERROR(_write_data_to_column(json_values, slot_descs[i], column_ptr, valid));
            if (!(*valid)) {
                break;
            }
        }
    }

    if (nullcount == column_num) {
        RETURN_IF_ERROR(_append_error_msg(
                objectValue, "All fields is null or not matched, this is a invalid row.", "",
                valid));
    }
    return Status::OK();
}

Status VJsonReader::_parse_json(bool* is_empty_row, bool* eof) {
    size_t size = 0;
    Status st = JsonReader::_parse_json_doc(&size, eof);
    // terminate if encounter other errors
    RETURN_IF_ERROR(st);

    // read all data, then return
    if (size == 0 || *eof) {
        *is_empty_row = true;
        return Status::OK();
    }

    if (!_parsed_jsonpaths.empty() && _strip_outer_array) {
        _total_lines = _json_doc->Size();
        _next_line = 0;

        if (_total_lines == 0) {
            // meet an empty json array.
            *is_empty_row = true;
        }
    }
    return Status::OK();
}

Status VJsonReader::_append_error_msg(const rapidjson::Value& objectValue, std::string error_msg,
                                      std::string col_name, bool* valid) {
    std::string err_msg;
    if (!col_name.empty()) {
        fmt::memory_buffer error_buf;
        fmt::format_to(error_buf, error_msg, col_name);
        err_msg = fmt::to_string(error_buf);
    } else {
        err_msg = error_msg;
    }

    RETURN_IF_ERROR(_state->append_error_msg_to_file(
            [&]() -> std::string { return JsonReader::_print_json_value(objectValue); },
            [&]() -> std::string { return err_msg; }, _scanner_eof));

    _counter->num_rows_filtered++;
    if (valid != nullptr) {
        // current row is invalid
        *valid = false;
    }
    return Status::OK();
}

} // namespace doris::vectorized
