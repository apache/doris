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

#define RETURN_ERROR_IF_COL_IS_ARRAY(col, type) \
    do { \
        if (col.IsArray()) { \
            return Status::InternalError(strings::Substitute(ERROR_COL_DATA_IS_ARRAY, type_to_string(type))); \
        } \
    } while (false)


#define RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type) \
    do { \
        if (!col.IsString()) { \
            return Status::InternalError(strings::Substitute(ERROR_INVALID_COL_DATA, type_to_string(type))); \
        } \
    } while (false)


#define RETURN_ERROR_IF_PARSING_FAILED(result, type) \
    do { \
        if (result != StringParser::PARSE_SUCCESS) { \
            return Status::InternalError(strings::Substitute(ERROR_INVALID_COL_DATA, type_to_string(type))); \
        } \
    } while (false)

template <typename T>
static Status get_int_value(const rapidjson::Value &col, PrimitiveType type, void* slot) {
    if (col.IsNumber()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) < 8 ? col.GetInt() : col.GetInt64());
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string& val = col.GetString();
    size_t len = col.GetStringLength();
    T v = StringParser::string_to_int<T>(val.c_str(), len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, type);

    if (sizeof(T) < 16) {
        *reinterpret_cast<T*>(slot) = v;
    } else {
        DCHECK(sizeof(T) == 16);
        memcpy(slot, &v, sizeof(v));
    }

    return Status::OK();
}

template <typename T>
static Status get_float_value(const rapidjson::Value &col, PrimitiveType type, void* slot) {
    DCHECK(sizeof(T) == 4 || sizeof(T) == 8);
    if (col.IsNumber()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) == 4 ? col.GetFloat() : col.GetDouble());
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string& val = col.GetString();
    size_t len = col.GetStringLength();
    T v = StringParser::string_to_float<T>(val.c_str(), len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, type);
    *reinterpret_cast<T*>(slot) = v;

    return Status::OK();
}

ScrollParser::ScrollParser() :
    _scroll_id(""),
    _total(0),
    _size(0),
    _line_index(0) {
}

ScrollParser::~ScrollParser() {
}

Status ScrollParser::parse(const std::string& scroll_result) {
    _document_node.Parse(scroll_result.c_str());
    if (_document_node.HasParseError()) {
        std::stringstream ss;
        ss << "Parsing json error, json is: " << scroll_result;
        return Status::InternalError(ss.str());
    }

    if (!_document_node.HasMember(FIELD_SCROLL_ID)) {
        return Status::InternalError("Document has not a scroll id field");
    }

    const rapidjson::Value &scroll_node = _document_node[FIELD_SCROLL_ID];
    _scroll_id = scroll_node.GetString();
    // { hits: { total : 2, "hits" : [ {}, {}, {} ]}}
    const rapidjson::Value &outer_hits_node = _document_node[FIELD_HITS];
    const rapidjson::Value &field_total = outer_hits_node[FIELD_TOTAL];
    _total = field_total.GetInt();
    if (_total == 0) {
        return Status::OK();
    }

    VLOG(1) << "es_scan_reader total hits: " << _total << " documents";
    const rapidjson::Value &inner_hits_node = outer_hits_node[FIELD_INNER_HITS];
    if (!inner_hits_node.IsArray()) {
        return Status::InternalError("inner hits node is not an array");
    }

    rapidjson::Document::AllocatorType& a = _document_node.GetAllocator();
    _inner_hits_node.CopyFrom(inner_hits_node, a);
    _size = _inner_hits_node.Size();

    return Status::OK();
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
        return Status::OK();
    }

    const rapidjson::Value& obj = _inner_hits_node[_line_index++];
    const rapidjson::Value& line = obj[FIELD_SOURCE];
    if (!line.IsObject()) {
        return Status::InternalError("Parse inner hits failed");
    }

    tuple->init(tuple_desc->byte_size());
    for (int i = 0; i < tuple_desc->slots().size(); ++i) {
        const SlotDescriptor* slot_desc = tuple_desc->slots()[i];

        if (!slot_desc->is_materialized()) {
            continue;
        }

        const char* col_name = slot_desc->col_name().c_str();
        rapidjson::Value::ConstMemberIterator itr = line.FindMember(col_name);
        if (itr == line.MemberEnd()) {
            tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }

        tuple->set_not_null(slot_desc->null_indicator_offset());
        const rapidjson::Value &col = line[col_name];

        void* slot = tuple->get_slot(slot_desc->tuple_offset());
        PrimitiveType type = slot_desc->type().type;
        switch (type) {
            case TYPE_CHAR:
            case TYPE_VARCHAR: {
                RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
                RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

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
                Status status = get_int_value<int8_t>(col, type, slot);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_SMALLINT: {
                Status status = get_int_value<int16_t>(col, type, slot);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_INT: {
                Status status = get_int_value<int32_t>(col, type, slot);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_BIGINT: {
                Status status = get_int_value<int64_t>(col, type, slot);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_LARGEINT: {
                Status status = get_int_value<__int128>(col, type, slot);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_DOUBLE: {
                Status status = get_float_value<double>(col, type, slot);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_FLOAT: {
                Status status = get_float_value<float>(col, type, slot);
                if (!status.ok()) {
                    return status;
                }
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

                RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
                RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

                const std::string& val = col.GetString();
                size_t val_size = col.GetStringLength();
                StringParser::ParseResult result;
                bool b = 
                    StringParser::string_to_bool(val.c_str(), val_size, &result);
                RETURN_ERROR_IF_PARSING_FAILED(result, type);
                *reinterpret_cast<int8_t*>(slot) = b;
                break;
            }

            case TYPE_DATE:
            case TYPE_DATETIME: {
                if (col.IsNumber()) {
                    if (!reinterpret_cast<DateTimeValue*>(slot)->from_unixtime(col.GetInt64())) {
                        return Status::InternalError(strings::Substitute(ERROR_INVALID_COL_DATA, type_to_string(type)));
                    }

                    if (type == TYPE_DATE) {
                        reinterpret_cast<DateTimeValue*>(slot)->cast_to_date();
                    } else {
                        reinterpret_cast<DateTimeValue*>(slot)->set_type(TIME_DATETIME);
                    }
                    break;
                }

                RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
                RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

                DateTimeValue* ts_slot = reinterpret_cast<DateTimeValue*>(slot);
                const std::string& val = col.GetString();
                size_t val_size = col.GetStringLength();
                if (!ts_slot->from_date_str(val.c_str(), val_size)) {
                    return Status::InternalError(strings::Substitute(ERROR_INVALID_COL_DATA, type_to_string(type)));
                }

                if (ts_slot->year() < 1900) {
                    return Status::InternalError(strings::Substitute(ERROR_INVALID_COL_DATA, type_to_string(type)));
                }

                if (type == TYPE_DATE) {
                    ts_slot->cast_to_date();
                } else {
                    ts_slot->to_datetime();
                }
                break;
            }

            default: {
                DCHECK(false);
                break;
            }
        }
    }

    *line_eof = false;
    return Status::OK();
}
}
