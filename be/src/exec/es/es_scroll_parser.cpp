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

#include "exec/es/es_scroll_parser.h"

#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>
#include <string>
#include "common/logging.h"
#include "common/status.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/string_parser.hpp"

namespace doris {

static const char* FIELD_SCROLL_ID = "_scroll_id";
static const char* FIELD_HITS = "hits";
static const char* FIELD_INNER_HITS = "hits";
static const char* FIELD_SOURCE = "_source";
static const char* FIELD_TOTAL = "total";

static const string ERROR_INVALID_COL_DATA = "Data source returned inconsistent column data. "
    "Expected value of type $0 based on column metadata. This likely indicates a "
    "problem with the data source library.";
static const string ERROR_MEM_LIMIT_EXCEEDED = "DataSourceScanNode::$0() failed to allocate "
    "$1 bytes for $2.";
static const string ERROR_COL_DATA_IS_ARRAY = "Data source returned an array for the type $0"
    "based on column metadata.";

ScrollParser::ScrollParser(const std::string& scroll_result) :
    _scroll_id(""),
    _total(0),
    _size(0),
    _line_index(0) {
        parsing(scroll_result);
}

ScrollParser::~ScrollParser() {
}

void ScrollParser::parsing(const std::string& scroll_result) {
    _document_node.Parse(scroll_result.c_str());

    if (!_document_node.HasMember(FIELD_SCROLL_ID)) {
        LOG(ERROR) << "maybe not a scroll request";
        return;
    }

    const rapidjson::Value &scroll_node = _document_node[FIELD_SCROLL_ID];
    _scroll_id = scroll_node.GetString();
    // { hits: { total : 2, "hits" : [ {}, {}, {} ]}}
    const rapidjson::Value &outer_hits_node = _document_node[FIELD_HITS];
    const rapidjson::Value &field_total = outer_hits_node[FIELD_TOTAL];
    _total = field_total.GetInt();
    if (_total == 0) {
        return;
    }

    VLOG(1) << "es_scan_reader total hits: " << _total << " documents";
    const rapidjson::Value &inner_hits_node = outer_hits_node[FIELD_INNER_HITS];
    if (!inner_hits_node.IsArray()) {
        LOG(ERROR) << "maybe not a scroll request";
        return;
    }

    rapidjson::Document::AllocatorType& a = _document_node.GetAllocator();
    _inner_hits_node.CopyFrom(inner_hits_node, a);
    _size = _inner_hits_node.Size();
}

int ScrollParser::get_size() {
    return _size;
}

const std::string& ScrollParser::get_scroll_id() {
    return _scroll_id;
}

int ScrollParser::get_total() {
    return _total;
}

Status ScrollParser::fill_tuple(const TupleDescriptor* tuple_desc, 
            Tuple* tuple, MemPool* tuple_pool, bool* line_eof) {
    *line_eof = true;
    if (_size <= 0 || _line_index >= _size) {
        return Status::OK;
    }

    const rapidjson::Value& obj = _inner_hits_node[_line_index++];
    const rapidjson::Value& line = obj[FIELD_SOURCE];
    if (!line.IsObject()) {
        return Status("Parse inner hits failed");
    }

    tuple->init(tuple_desc->byte_size());
    for (int i = 0; i < tuple_desc->slots().size(); ++i) {
        const SlotDescriptor* slot_desc = tuple_desc->slots()[i];

        if (!slot_desc->is_materialized()) {
            continue;
        }

        std::string s(slot_desc->col_name());
        const char* col_name = s.c_str();
        rapidjson::Value::ConstMemberIterator itr = line.FindMember(col_name);
        if (itr == line.MemberEnd()) {
            tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }

        tuple->set_not_null(slot_desc->null_indicator_offset());
        const rapidjson::Value &col = line[col_name];

        void* slot = tuple->get_slot(slot_desc->tuple_offset());
        switch (slot_desc->type().type) {
            case TYPE_CHAR:
            case TYPE_VARCHAR: {
                if (col.IsArray()) {
                    return Status(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, "STRING"));
                }
                if (!col.IsString()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "STRING"));
                }
                const std::string& val = col.GetString();
                size_t val_size = col.GetStringLength();
                char* buffer = reinterpret_cast<char*>(tuple_pool->try_allocate_unaligned(val_size));
                if (UNLIKELY(buffer == NULL)) {
                    string details = strings::Substitute(ERROR_MEM_LIMIT_EXCEEDED, "MaterializeNextRow",
                                val_size, "string slot");
                    return tuple_pool->mem_tracker()->MemLimitExceeded(NULL, details, val_size);
                }
                memcpy(buffer, val.data(), val_size);
                reinterpret_cast<StringValue*>(slot)->ptr = buffer;
                reinterpret_cast<StringValue*>(slot)->len = val_size;
                break;
            }

            case TYPE_TINYINT: {
                if (col.IsNumber()) {
                    *reinterpret_cast<int8_t*>(slot) = (int8_t)col.GetInt();
                    break;
                }

                if (col.IsArray()) {
                    return Status(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, "TINYINT"));
                }

                if (!col.IsString()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TINYINT"));
                } 

                const std::string& val = col.GetString();
                const char* data = val.c_str();
                size_t len = col.GetStringLength();
                StringParser::ParseResult result;
                int8_t v = StringParser::string_to_int<int8_t>(data, len, &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TINYINT"));
                }
                *reinterpret_cast<int8_t*>(slot) = v;
                break;
            }

            case TYPE_SMALLINT: {
                if (col.IsNumber()) {
                    *reinterpret_cast<int16_t*>(slot) = (int16_t)col.GetInt();
                    break;
                }

                if (col.IsArray()) {
                    return Status(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, "SMALLINT"));
                }

                if (!col.IsString()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "SMALLINT"));
                } 
                    
                const std::string& val = col.GetString();
                const char* data = val.c_str();
                size_t len = col.GetStringLength();
                StringParser::ParseResult result;
                int16_t v = StringParser::string_to_int<int16_t>(data, len, &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "SMALLINT"));
                }
                *reinterpret_cast<int16_t*>(slot) = v;
                break;
            }

            case TYPE_INT: {
                if (col.IsNumber()) {
                    *reinterpret_cast<int32_t*>(slot) = (int32_t)col.GetInt();
                    break;
                }

                if (col.IsArray()) {
                    return Status(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, "INT"));
                }

                if (!col.IsString()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "INT"));
                } 

                const std::string& val = col.GetString();
                const char* data = val.c_str();
                size_t len = col.GetStringLength();
                StringParser::ParseResult result;
                int32_t v = StringParser::string_to_int<int32_t>(data, len, &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "INT"));
                }
                *reinterpret_cast<int32_t*>(slot) = v;
                break;
            }

            case TYPE_BIGINT: {
                if (col.IsNumber()) {
                    *reinterpret_cast<int64_t*>(slot) = col.GetInt64();
                    break;
                }

                if (col.IsArray()) {
                    return Status(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, "BIGINT"));
                }

                if (!col.IsString()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "BIGINT"));
                } 

                const std::string& val = col.GetString();
                const char* data = val.c_str();
                size_t len = col.GetStringLength();
                StringParser::ParseResult result;
                int64_t v = StringParser::string_to_int<int64_t>(data, len, &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "BIGINT"));
                }
                *reinterpret_cast<int64_t*>(slot) = v;
                break;
            }

            case TYPE_LARGEINT: {
                if (col.IsNumber()) {
                    *reinterpret_cast<int128_t*>(slot) = col.GetInt64();
                    break;
                }

                if (col.IsArray()) {
                    return Status(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, "LARGEINT"));
                }

                if (!col.IsString()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "LARGEINT"));
                } 

                const std::string& val = col.GetString();
                const char* data = val.c_str();
                size_t len = col.GetStringLength();
                StringParser::ParseResult result;
                __int128 v = StringParser::string_to_int<__int128>(data, len, &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "LARGEINT"));
                }
                memcpy(slot, &v, sizeof(v));
                break;
            }

            case TYPE_DOUBLE: {
                if (col.IsNumber()) {
                    *reinterpret_cast<double*>(slot) = col.GetDouble();
                    break;
                }

                if (col.IsArray()) {
                    return Status(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, "DOUBLE"));
                }

                if (!col.IsString()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "DOUBLE"));
                } 

                const std::string& val = col.GetString();
                size_t val_size = col.GetStringLength();
                StringParser::ParseResult result;
                double d = StringParser::string_to_float<double>(val.c_str(), 
                            val_size, &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "DOUBLE"));
                }
                *reinterpret_cast<double*>(slot) = d;
                break;
            }

            case TYPE_FLOAT: {
                if (col.IsNumber()) {
                    *reinterpret_cast<float*>(slot) = col.GetFloat();
                    break;
                }

                if (col.IsArray()) {
                    return Status(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, "FLOAT"));
                }

                if (!col.IsString()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "FLOAT"));
                } 

                const std::string& val = col.GetString();
                size_t val_size = col.GetStringLength();
                StringParser::ParseResult result;
                float f = StringParser::string_to_float<float>(val.c_str(), val_size, &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "FLOAT"));
                }
                *reinterpret_cast<float*>(slot) = f;
                break;
            }

            case TYPE_BOOLEAN: {
                if (col.IsBool()) {
                    *reinterpret_cast<int8_t*>(slot) = col.GetBool();
                    break;
                }

                if (col.IsNumber()) {
                    *reinterpret_cast<int8_t*>(slot) = col.GetInt();
                    break;
                }

                if (col.IsArray()) {
                    return Status(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, "BOOLEAN"));
                }

                if (!col.IsString()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "BOOLEAN"));
                } 

                const std::string& val = col.GetString();
                size_t val_size = col.GetStringLength();
                StringParser::ParseResult result;
                bool b = StringParser::string_to_bool(val.c_str(), val_size, &result);
                if (result != StringParser::PARSE_SUCCESS) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "BOOLEAN"));
                }
                *reinterpret_cast<int8_t*>(slot) = b;
                break;
            }

            case TYPE_DATE: {
                if (col.IsNumber()) {
                    if (!reinterpret_cast<DateTimeValue*>(slot)->from_unixtime(col.GetInt64())) {
                        return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATE"));
                    }
                    reinterpret_cast<DateTimeValue*>(slot)->cast_to_date();
                    break;
                }

                if (col.IsArray()) {
                    return Status(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, "TYPE_DATE"));
                }

                if (!col.IsString()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATE"));
                } 

                DateTimeValue* ts_slot = reinterpret_cast<DateTimeValue*>(slot);
                const std::string& val = col.GetString();
                size_t val_size = col.GetStringLength();
                if (!ts_slot->from_date_str(val.c_str(), val_size)) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATE"));
                }

                if (ts_slot->year() < 1900) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATE"));
                }

                ts_slot->cast_to_date();
                break;
            }

            case TYPE_DATETIME: {
                if (col.IsNumber()) {
                    if (!reinterpret_cast<DateTimeValue*>(slot)->from_unixtime(col.GetInt64())) {
                        return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATETIME"));
                    }
                    reinterpret_cast<DateTimeValue*>(slot)->set_type(TIME_DATETIME);
                    break;
                }

                if (col.IsArray()) {
                    return Status(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, "TYPE_DATETIME"));
                }

                if (!col.IsString()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATETIME"));
                } 

                DateTimeValue* ts_slot = reinterpret_cast<DateTimeValue*>(slot);
                const std::string& val = col.GetString();
                size_t val_size = col.GetStringLength();
                if (!ts_slot->from_date_str(val.c_str(), val_size)) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATETIME"));
                }

                if (ts_slot->year() < 1900) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATETIME"));
                }

                ts_slot->to_datetime();
                break;
            }

            default: {
                DCHECK(false);
                break;
            }
        }
    }

    *line_eof = false;
    return Status::OK;
}
}
