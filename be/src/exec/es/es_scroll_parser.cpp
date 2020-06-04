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
#include "rapidjson/rapidjson.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "util/string_parser.hpp"



namespace doris {

static const char* FIELD_SCROLL_ID = "_scroll_id";
static const char* FIELD_HITS = "hits";
static const char* FIELD_INNER_HITS = "hits";
static const char* FIELD_SOURCE = "_source";
static const char* FIELD_TOTAL = "total";


// get the original json data type
std::string json_type_to_string(rapidjson::Type type) {
    switch (type) {
        case rapidjson::kNumberType:
            return "Number";
        case rapidjson::kStringType:
            return "Varchar/Char";
        case rapidjson::kArrayType:
            return "Array";
        case rapidjson::kObjectType:
            return "Object";
        case rapidjson::kNullType:
            return "Null Type";
        case rapidjson::kFalseType:
        case rapidjson::kTrueType:
            return "True/False";
        default:
            return "Unknown Type";
    }
}

// transfer rapidjson::Value to string representation
std::string json_value_to_string(const rapidjson::Value& value) {
    rapidjson::StringBuffer scratch_buffer;
    rapidjson::Writer<rapidjson::StringBuffer> temp_writer(scratch_buffer);
    value.Accept(temp_writer);
    return scratch_buffer.GetString();
}

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
            std::stringstream ss;    \
            ss << "Expected value of type: " \
               << type_to_string(type)  \
               << "; but found type: " << json_type_to_string(col.GetType()) \
               << "; Docuemnt slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str()); \
        } \
    } while (false)


#define RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type) \
    do { \
        if (!col.IsString()) { \
            std::stringstream ss;    \
            ss << "Expected value of type: " \
               << type_to_string(type)  \
               << "; but found type: " << json_type_to_string(col.GetType()) \
               << "; Docuemnt source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str()); \
        } \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_NOT_NUMBER(col, type) \
    do { \
        if (!col.IsNumber()) { \
            std::stringstream ss; \
            ss << "Expected value of type: " \
               << type_to_string(type) \
               << "; but found type: " <<json_type_to_string(col.GetType()) \
               << "; Document value is: " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str()); \
        } \
     } while (false)

#define RETURN_ERROR_IF_PARSING_FAILED(result, col, type) \
    do { \
        if (result != StringParser::PARSE_SUCCESS) { \
            std::stringstream ss;    \
            ss << "Expected value of type: " \
               << type_to_string(type)  \
               << "; but found type: " << json_type_to_string(col.GetType()) \
               << "; Docuemnt source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str()); \
        } \
    } while (false)

#define RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type) \
    do { \
        std::stringstream ss;    \
        ss << "Expected value of type: " \
            << type_to_string(type)  \
            << "; but found type: " << json_type_to_string(col.GetType()) \
            << "; Docuemnt slice is : " << json_value_to_string(col); \
        return Status::RuntimeError(ss.str()); \
    } while (false)

template <typename T>
static Status get_int_value(const rapidjson::Value &col, PrimitiveType type, void* slot, bool pure_doc_value) {
    if (col.IsNumber()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) < 8 ? col.GetInt() : col.GetInt64());
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray()) {
        RETURN_ERROR_IF_COL_IS_NOT_NUMBER(col[0], type);
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) < 8 ? col[0].GetInt() : col[0].GetInt64());
        return Status::OK();
    }


    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);


    StringParser::ParseResult result;
    const std::string& val = col.GetString();
    size_t len = col.GetStringLength();
    T v = StringParser::string_to_int<T>(val.c_str(), len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);

    if (sizeof(T) < 16) {
        *reinterpret_cast<T*>(slot) = v;
    } else {
        DCHECK(sizeof(T) == 16);
        memcpy(slot, &v, sizeof(v));
    }

    return Status::OK();
}

template <typename T>
static Status get_float_value(const rapidjson::Value &col, PrimitiveType type, void* slot, bool pure_doc_value) {
    DCHECK(sizeof(T) == 4 || sizeof(T) == 8);
    if (col.IsNumber()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) == 4 ? col.GetFloat() : col.GetDouble());
        return Status::OK();
    }

    if (pure_doc_value && col.IsArray()) {
        *reinterpret_cast<T*>(slot) = (T)(sizeof(T) == 4 ? col[0].GetFloat() : col[0].GetDouble());
        return Status::OK();
    }

    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
    RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

    StringParser::ParseResult result;
    const std::string& val = col.GetString();
    size_t len = col.GetStringLength();
    T v = StringParser::string_to_float<T>(val.c_str(), len, &result);
    RETURN_ERROR_IF_PARSING_FAILED(result, col, type);
    *reinterpret_cast<T*>(slot) = v;

    return Status::OK();
}

ScrollParser::ScrollParser(bool doc_value_mode) :
    _scroll_id(""),
    _total(0),
    _size(0),
    _line_index(0),
    _doc_value_mode(doc_value_mode) {
}

ScrollParser::~ScrollParser() {
}

Status ScrollParser::parse(const std::string& scroll_result, bool exactly_once) {
    _document_node.Parse(scroll_result.c_str());
    if (_document_node.HasParseError()) {
        std::stringstream ss;
        ss << "Parsing json error, json is: " << scroll_result;
        return Status::InternalError(ss.str());
    }

    if (!exactly_once && !_document_node.HasMember(FIELD_SCROLL_ID)) {
        LOG(WARNING) << "Document has not a scroll id field scroll reponse:" << scroll_result;
        return Status::InternalError("Document has not a scroll id field");
    }

    if (!exactly_once) {
        const rapidjson::Value &scroll_node = _document_node[FIELD_SCROLL_ID];
        _scroll_id = scroll_node.GetString();
    }
    // { hits: { total : 2, "hits" : [ {}, {}, {} ]}}
    const rapidjson::Value &outer_hits_node = _document_node[FIELD_HITS];
    const rapidjson::Value &field_total = outer_hits_node[FIELD_TOTAL];
    // after es 7.x "total": { "value": 1, "relation": "eq" }
    // it is not necessary to parse `total`, this logic would be removed the another pr.
    if (field_total.IsObject()) {
        const rapidjson::Value &field_relation_value = field_total["relation"];
        std::string relation = field_relation_value.GetString();
        // maybe not happend on scoll sort mode, for logically rigorous
        if ("eq" != relation) {
            return Status::InternalError("Could not identify exact hit count for search response");
        }
        const rapidjson::Value &field_total_value = field_total["value"];
        _total = field_total_value.GetInt();
    } else {
        _total = field_total.GetInt();
    }
    // just used for the first scroll, maybe we should remove this logic from the `get_next`
    if (_total == 0) {
        return Status::OK();
    }

    VLOG(1) << "es_scan_reader parse scroll result: " << scroll_result;
    const rapidjson::Value &inner_hits_node = outer_hits_node[FIELD_INNER_HITS];
    // this happened just the end of scrolling
    if (!inner_hits_node.IsArray()) {
        return Status::OK();
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
            Tuple* tuple, MemPool* tuple_pool, bool* line_eof, const std::map<std::string, std::string>& docvalue_context) {
    *line_eof = true;

    if (_size <= 0 || _line_index >= _size) {
        return Status::OK();
    }

    const rapidjson::Value& obj = _inner_hits_node[_line_index++];
    bool pure_doc_value = false;
    if (obj.HasMember("fields")) {
        pure_doc_value = true;
    }
    const rapidjson::Value& line = obj.HasMember(FIELD_SOURCE) ? obj[FIELD_SOURCE] : obj["fields"];

    tuple->init(tuple_desc->byte_size());
    for (int i = 0; i < tuple_desc->slots().size(); ++i) {
        const SlotDescriptor* slot_desc = tuple_desc->slots()[i];

        if (!slot_desc->is_materialized()) {
            continue;
        }

        // if pure_doc_value enabled, docvalue_context must contains the key
        // todo: need move all `pure_docvalue` for every tuple outside fill_tuple
        //  should check pure_docvalue for one table scan not every tuple
        const char* col_name = pure_doc_value ? docvalue_context.at(slot_desc->col_name()).c_str() : slot_desc->col_name().c_str();

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
                // sometimes elasticsearch user post some not-string value to Elasticsearch Index.
                // because of reading value from _source, we can not process all json type and then just transfer the value to original string representation 
                // this may be a tricky, but we can workaround this issue
                std::string val;
                if (pure_doc_value) {
                    if (!col[0].IsString()) {
                        val = json_value_to_string(col[0]);
                    } else {
                        val = col[0].GetString();
                    }
                } else {
                    RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
                    if (!col.IsString()) {
                        val = json_value_to_string(col);
                    } else {
                        val = col.GetString();
                    }
                }
                size_t val_size = val.length();
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
                Status status = get_int_value<int8_t>(col, type, slot, pure_doc_value);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_SMALLINT: {
                Status status = get_int_value<int16_t>(col, type, slot, pure_doc_value);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_INT: {
                Status status = get_int_value<int32_t>(col, type, slot, pure_doc_value);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_BIGINT: {
                Status status = get_int_value<int64_t>(col, type, slot, pure_doc_value);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_LARGEINT: {
                Status status = get_int_value<__int128>(col, type, slot, pure_doc_value);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_DOUBLE: {
                Status status = get_float_value<double>(col, type, slot, pure_doc_value);
                if (!status.ok()) {
                    return status;
                }
                break;
            }

            case TYPE_FLOAT: {
                Status status = get_float_value<float>(col, type, slot, pure_doc_value);
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
                if (pure_doc_value && col.IsArray()) {
                    *reinterpret_cast<int8_t*>(slot) = col[0].GetBool();
                    break;
                }

                RETURN_ERROR_IF_COL_IS_ARRAY(col, type);
                RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type);

                const std::string& val = col.GetString();
                size_t val_size = col.GetStringLength();
                StringParser::ParseResult result;
                bool b = 
                    StringParser::string_to_bool(val.c_str(), val_size, &result);
                RETURN_ERROR_IF_PARSING_FAILED(result, col, type);
                *reinterpret_cast<int8_t*>(slot) = b;
                break;
            }

            case TYPE_DATE:
            case TYPE_DATETIME: {
                if (col.IsNumber()) {
                    if (!reinterpret_cast<DateTimeValue*>(slot)->from_unixtime(col.GetInt64(), "+08:00")) {
                        RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type);
                    }

                    if (type == TYPE_DATE) {
                        reinterpret_cast<DateTimeValue*>(slot)->cast_to_date();
                    } else {
                        reinterpret_cast<DateTimeValue*>(slot)->set_type(TIME_DATETIME);
                    }
                    break;
                }
                if (pure_doc_value && col.IsArray()) {
                    if (!reinterpret_cast<DateTimeValue*>(slot)->from_unixtime(col[0].GetInt64(), "+08:00")) {
                        RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type);
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
                    RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type);
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
