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

#include <gutil/strings/substitute.h>

#include <boost/algorithm/string.hpp>
#include <string>

#include "common/logging.h"
#include "common/status.h"
#include "exprs/agg_fn_evaluator.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/string_parser.hpp"

namespace doris {

static const char* FIELD_SCROLL_ID = "_scroll_id";
static const char* FIELD_HITS = "hits";
static const char* FIELD_INNER_HITS = "hits";
static const char* FIELD_SOURCE = "_source";
static const char* FIELD_ID = "_id";
static const char* FILED_AGG = "aggregations";
static const char* FILED_VALUE = "value";
static const char* FILED_VALUE_AS_STRING = "value_as_string";
static const char* FILED_GROUPBY = "groupby";
static const char* FILED_BUCKETS = "buckets";
static const char* FILED_AFTER_KEY = "after_key";

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

static const std::string ERROR_INVALID_COL_DATA =
        "Data source returned inconsistent column data. "
        "Expected value of type $0 based on column metadata. This likely indicates a "
        "problem with the data source library.";
static const std::string ERROR_MEM_LIMIT_EXCEEDED =
        "DataSourceScanNode::$0() failed to allocate "
        "$1 bytes for $2.";
static const std::string ERROR_COL_DATA_IS_ARRAY =
        "Data source returned an array for the type $0"
        "based on column metadata.";

#define RETURN_ERROR_IF_COL_IS_ARRAY(col, type)                              \
    do {                                                                     \
        if (col.IsArray()) {                                                 \
            std::stringstream ss;                                            \
            ss << "Expected value of type: " << type_to_string(type)         \
               << "; but found type: " << json_type_to_string(col.GetType()) \
               << "; Document slice is : " << json_value_to_string(col);     \
            return Status::RuntimeError(ss.str());                           \
        }                                                                    \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_NOT_STRING(col, type)                            \
    do {                                                                        \
        if (!col.IsString()) {                                                  \
            std::stringstream ss;                                               \
            ss << "Expected value of type: " << type_to_string(type)            \
               << "; but found type: " << json_type_to_string(col.GetType())    \
               << "; Document source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str());                              \
        }                                                                       \
    } while (false)

#define RETURN_ERROR_IF_COL_IS_NOT_NUMBER(col, type)                         \
    do {                                                                     \
        if (!col.IsNumber()) {                                               \
            std::stringstream ss;                                            \
            ss << "Expected value of type: " << type_to_string(type)         \
               << "; but found type: " << json_type_to_string(col.GetType()) \
               << "; Document value is: " << json_value_to_string(col);      \
            return Status::RuntimeError(ss.str());                           \
        }                                                                    \
    } while (false)

#define RETURN_ERROR_IF_PARSING_FAILED(result, col, type)                       \
    do {                                                                        \
        if (result != StringParser::PARSE_SUCCESS) {                            \
            std::stringstream ss;                                               \
            ss << "Expected value of type: " << type_to_string(type)            \
               << "; but found type: " << json_type_to_string(col.GetType())    \
               << "; Document source slice is : " << json_value_to_string(col); \
            return Status::RuntimeError(ss.str());                              \
        }                                                                       \
    } while (false)

#define RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type)                     \
    do {                                                                 \
        std::stringstream ss;                                            \
        ss << "Expected value of type: " << type_to_string(type)         \
           << "; but found type: " << json_type_to_string(col.GetType()) \
           << "; Document slice is : " << json_value_to_string(col);     \
        return Status::RuntimeError(ss.str());                           \
    } while (false)

template <typename T>
static Status get_int_value(const rapidjson::Value& col, PrimitiveType type, void* slot,
                                bool pure_doc_value) {
    // for check double -> int is right or not
    const double eps = 1e-10;

    if (col.IsNumber()) {
        if (col.IsDouble()) {
            // ES will convert <int> to <double> when do aggregate(int)
            // so the aggregation result from ES will be double
            // eg. type: long, aggregation: sum(xx), result: 32.0
            // we convert double to int and check there's any deviation(>= 1e-10)
            double temp = col.GetDouble();
            T converted_temp = (T) temp;
            if (abs( ((double) (converted_temp)) - temp) < eps) {
                *reinterpret_cast<T *>(slot) = converted_temp;
                return Status::OK();
            }

            std::stringstream ss;
            ss << "Expected value of type: " << type_to_string(type)
            << "; but found type: " << json_type_to_string(col.GetType())
            << "; Document slice is : " << json_value_to_string(col);
            return Status::RuntimeError(ss.str());
        } else {
            *reinterpret_cast<T *>(slot) = (T) (sizeof(T) < 8 ? col.GetInt() : col.GetInt64());
        }
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
static Status get_float_value(const rapidjson::Value& col, PrimitiveType type, void* slot,
                              bool pure_doc_value) {
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

ScrollParser::ScrollParser(bool doc_value_mode)
        : _scroll_id(""), _size(0), _line_index(0), _doc_value_mode(doc_value_mode) {}

ScrollParser::~ScrollParser() {}

Status ScrollParser::parse(const std::string& scroll_result, bool exactly_once) {
    // when do es scan, rely on `_size !=0 ` to determine whether scroll ends
    // when do es aggregate, there is two cases:
    // 1. has group by: rely on `_size !=0 ` to determine whether aggregation result ends
    // 2. no group by : _size should be 1, if _size = 0, maybe aggregate function is only <count>
    _size = 0;
    _document_node.Parse(scroll_result.c_str(), scroll_result.length());
    if (_document_node.HasParseError()) {
        std::stringstream ss;
        ss << "Parsing json error, json is: " << scroll_result;
        return Status::InternalError(ss.str());
    }
    if (!_document_node.HasMember(FILED_AGG)) {
        // if it's scan and not exactly_once, should be scroll read
        if (!exactly_once && !_document_node.HasMember(FIELD_SCROLL_ID)) {
            LOG(WARNING) << "Document has not a scroll id field scroll response:" << scroll_result;
            return Status::InternalError("Document has not a scroll id field");
        }

        if (!exactly_once) {
            const rapidjson::Value &scroll_node = _document_node[FIELD_SCROLL_ID];
            _scroll_id = scroll_node.GetString();
        }
    }

    // { hits: { total : 2, "hits" : [ {}, {}, {} ]}}
    const rapidjson::Value& outer_hits_node = _document_node[FIELD_HITS];

    if (outer_hits_node.HasMember("total")) {
        const rapidjson::Value& hits_total = outer_hits_node["total"];
        if (!hits_total.IsObject()) {
            // {"hits":{"total":33204320}}
            _count_with_null.CopyFrom(hits_total, _document_node.GetAllocator());
        } else if (hits_total.HasMember("value")) {
            // {"hits":{"total":{"value":8,"relation":"eq"}}}
            const rapidjson::Value& hits_total_value = hits_total["value"];

            if (hits_total.HasMember("relation")) {
                const rapidjson::Value &hits_total_relation = hits_total["relation"];
                string relation = hits_total_relation.GetString();
                if (relation != "eq") {
                    return Status::RuntimeError("get approximate total count from ES wrongly: " + relation);
                }
            }
            _count_with_null.CopyFrom(hits_total_value, _document_node.GetAllocator());
            /*
             Note that, an integer value may be obtained in various ways without conversion.
             For example, A value x containing 123 will make x.IsInt() == x.IsUint() == x.IsInt64() == x.IsUint64() == true.
             But a value y containing -3000000000 will only make x.IsInt64() == true.
             */
            VLOG_DEBUG << "get total count (including null) from ES, value is: " << _count_with_null.GetInt64();
        } else {
            return Status::RuntimeError("Document hasn't <hits.total.value>, can't get total count");
        }
    }

    if (_document_node.HasMember(FILED_AGG) && _document_node[FILED_AGG].IsObject()) {
        // there is two cases : 1. group by 2. no group by
        if (_document_node[FILED_AGG].HasMember(FILED_GROUPBY)) { // case 1: agg + group by.
            if (_document_node[FILED_AGG][FILED_GROUPBY].IsObject() && _document_node[FILED_AGG][FILED_GROUPBY].HasMember(FILED_BUCKETS)) {
                const rapidjson::Value& agg_groupby_buckets = _document_node[FILED_AGG][FILED_GROUPBY][FILED_BUCKETS];
                /**
                   _inner_agg_values = 
                   [
                      {"group_hold1": null, "group_hold2": null, "place_hold1": 0, "place_hold2": 1, "doc_count": 1},    --> buckets[0]
                      {"group_hold1": A,    "group_hold2": null, "place_hold1": 0, "place_hold2": 1, "doc_count": 1},    --> buckets[1]
                        ...
                   ]
                 */
                _inner_agg_values.SetArray();     

                for(auto i = 0; i < agg_groupby_buckets.Size(); i++) {
                    rapidjson::Value line(rapidjson::kObjectType);
                    line.SetObject();

                    // 1. add group by clauses
                    if (agg_groupby_buckets[i].IsObject() && agg_groupby_buckets[i].HasMember("key") && agg_groupby_buckets[i]["key"].IsObject()) {
                        for (rapidjson::Value::ConstMemberIterator itr = agg_groupby_buckets[i]["key"].MemberBegin(); 
                                itr != agg_groupby_buckets[i]["key"].MemberEnd(); ++itr) {
                            rapidjson::Value jKey;
                            rapidjson::Value jValue;
                            jKey.CopyFrom(itr->name, _document_node.GetAllocator());
                            jValue.CopyFrom(itr->value, _document_node.GetAllocator());
                            line.AddMember(jKey, jValue, _document_node.GetAllocator());
                        }
                    } else {
                        return Status::RuntimeError("Document hasn't <buckets[i].key>, can't get group_by value");
                    }

                    // 2. add result
                    int j = 1;
                    std::string str_ph = "place_hold1";
                    while (agg_groupby_buckets[i].HasMember(str_ph.c_str())) {
                        rapidjson::Value jKey(str_ph.c_str(), _document_node.GetAllocator());
                        rapidjson::Value jValue;

                        if (!agg_groupby_buckets[i][str_ph.c_str()].IsObject() || !agg_groupby_buckets[i][str_ph.c_str()].HasMember(FILED_VALUE)) {
                            return Status::RuntimeError("Document hasn't <aggregate_field.value> field, can't get aggregation result");
                        }

                        // when value is scientific notation or eg.., result is '{value:1.634118932e12, value_as_string:2021-10-13 17:55:32}'
                        // the scientific notation need doris using <getDouble> to read, it maybe make deviation
                        // <value_as_string> is a good choice, the case usually happen when the field is date type
                        if (agg_groupby_buckets[i][str_ph.c_str()].HasMember(FILED_VALUE_AS_STRING)) {
                            jValue.CopyFrom(agg_groupby_buckets[i][str_ph.c_str()][FILED_VALUE_AS_STRING], _document_node.GetAllocator());
                        } else if (agg_groupby_buckets[i][str_ph.c_str()].HasMember(FILED_VALUE)) {
                            jValue.CopyFrom(agg_groupby_buckets[i][str_ph.c_str()][FILED_VALUE], _document_node.GetAllocator());
                        }
                        line.AddMember(jKey, jValue, _document_node.GetAllocator());

                        str_ph = "place_hold" + std::to_string(++j);
                    }

                    // 3. add count
                    if (agg_groupby_buckets[i].HasMember("doc_count")) {
                        rapidjson::Value v;
                        v.CopyFrom(agg_groupby_buckets[i]["doc_count"], _document_node.GetAllocator());
                        line.AddMember("doc_count", v, _document_node.GetAllocator());
                    } else {
                        return Status::RuntimeError("Document hasn't <buckets[i].key>, can't get group_by value");
                    }

                    // 4. add to _inner_agg_values
                    _inner_agg_values.PushBack(line, _document_node.GetAllocator()); 
                }

                VLOG_DEBUG << "Get result from ES queryDSL " << json_value_to_string(_inner_agg_values);
            } else {
                return Status::RuntimeError("Document hasn't <groupBy.bucket>, can't get aggregation result");
            }

            if (_document_node[FILED_AGG][FILED_GROUPBY].HasMember(FILED_AFTER_KEY)) {
                _after_key = json_value_to_string(_document_node[FILED_AGG][FILED_GROUPBY][FILED_AFTER_KEY]);
            }
        } else { // case 2: "aggregations":{"place_hold1":{"value":500.0},"place_hold2":{"value":10.0}}
            _inner_agg_values.SetArray();
            rapidjson::Value place_hold_map;
            place_hold_map.SetObject();

            for (rapidjson::Value::ConstMemberIterator itr = _document_node[FILED_AGG].MemberBegin(); itr != _document_node[FILED_AGG].MemberEnd(); ++itr) {
                rapidjson::Value jKey;
                rapidjson::Value jValue;

                jKey.CopyFrom(itr->name, _document_node.GetAllocator());
                if (!itr->value.IsObject() || !itr->value.HasMember(FILED_VALUE)) {
                    return Status::RuntimeError("Document hasn't <aggregate_field.value> field, can't get aggregation result");
                }

                if (itr->value.HasMember(FILED_VALUE_AS_STRING)) {
                    jValue.CopyFrom(itr->value[FILED_VALUE_AS_STRING], _document_node.GetAllocator());
                } else if (itr->value.HasMember(FILED_VALUE)) {
                    jValue.CopyFrom(itr->value[FILED_VALUE], _document_node.GetAllocator());
                }
                place_hold_map.AddMember(jKey, jValue, _document_node.GetAllocator());
            }
            _inner_agg_values.PushBack(place_hold_map, _document_node.GetAllocator());
        }
        // how many documents contains in this batch
        _size = _inner_agg_values.Size();
        return Status::OK();
    }

    // if has no inner hits, there has no data in this index
    if (!outer_hits_node.HasMember(FIELD_INNER_HITS)) {
        return Status::OK();
    }
    const rapidjson::Value& inner_hits_node = outer_hits_node[FIELD_INNER_HITS];
    // this happened just the end of scrolling
    if (!inner_hits_node.IsArray()) {
        return Status::OK();
    }
    _inner_hits_node.CopyFrom(inner_hits_node, _document_node.GetAllocator());
    // how many documents contains in this batch
    _size = _inner_hits_node.Size();
    return Status::OK();
}

int ScrollParser::get_size() {
    return _size;
}

const std::string& ScrollParser::get_scroll_id() {
    return _scroll_id;
}

const string& ScrollParser::get_after_key() {
    return _after_key;
}

Status ScrollParser::fill_tuple(const TupleDescriptor* tuple_desc, Tuple* tuple,
                                MemPool* tuple_pool, bool* line_eof,
                                const std::map<std::string, std::string>& docvalue_context) {
    *line_eof = true;

    if (_size <= 0 || _line_index >= _size) {
        return Status::OK();
    }

    const rapidjson::Value& obj = _inner_hits_node[_line_index++];
    const rapidjson::Value null_val;
    bool pure_doc_value = false;
    bool all_field_is_null = false;
    if (obj.HasMember("fields")) {
        pure_doc_value = true;
    }
    const rapidjson::Value& line = obj.HasMember(FIELD_SOURCE) ? obj[FIELD_SOURCE] :
            (pure_doc_value ? obj["fields"] : (all_field_is_null = true, null_val));

    tuple->init(tuple_desc->byte_size());
    for (int i = 0; i < tuple_desc->slots().size(); ++i) {
        const SlotDescriptor* slot_desc = tuple_desc->slots()[i];

        if (!slot_desc->is_materialized()) {
            continue;
        }
        // _id field must exists in every document, this is guaranteed by ES
        // if _id was found in tuple, we would get `_id` value from inner-hit node
        // json-format response would like below:
        //    "hits": {
        //            "hits": [
        //                {
        //                    "_id": "UhHNc3IB8XwmcbhBk1ES",
        //                    "_source": {
        //                          "k": 201,
        //                    }
        //                }
        //            ]
        //        }
        if (all_field_is_null) {
            tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }

        if (slot_desc->col_name() == FIELD_ID) {
            // actually this branch will not be reached, this is guaranteed by Doris FE.
            if (pure_doc_value) {
                std::stringstream ss;
                ss << "obtain `_id` is not supported in doc_values mode";
                return Status::RuntimeError(ss.str());
            }
            tuple->set_not_null(slot_desc->null_indicator_offset());
            void* slot = tuple->get_slot(slot_desc->tuple_offset());
            // obj[FIELD_ID] must not be NULL
            std::string _id = obj[FIELD_ID].GetString();
            size_t len = _id.length();
            char* buffer = reinterpret_cast<char*>(tuple_pool->try_allocate_unaligned(len));
            if (UNLIKELY(buffer == NULL)) {
                std::string details = strings::Substitute(ERROR_MEM_LIMIT_EXCEEDED,
                                                          "MaterializeNextRow", len, "string slot");
                return tuple_pool->mem_tracker()->MemLimitExceeded(NULL, details, len);
            }
            memcpy(buffer, _id.data(), len);
            reinterpret_cast<StringValue*>(slot)->ptr = buffer;
            reinterpret_cast<StringValue*>(slot)->len = len;
            continue;
        }

        // if pure_doc_value enabled, docvalue_context must contains the key
        // todo: need move all `pure_docvalue` for every tuple outside fill_tuple
        //  should check pure_docvalue for one table scan not every tuple
        const char* col_name = pure_doc_value ? docvalue_context.at(slot_desc->col_name()).c_str()
                                              : slot_desc->col_name().c_str();

        rapidjson::Value::ConstMemberIterator itr = line.FindMember(col_name);
        if (itr == line.MemberEnd()) {
            tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }

        tuple->set_not_null(slot_desc->null_indicator_offset());
        const rapidjson::Value& col = line[col_name];

        void* slot = tuple->get_slot(slot_desc->tuple_offset());
        PrimitiveType type = slot_desc->type().type;

        // when the column value is null, the subsequent type casting will report an error
        if (col.IsNull()) {
            slot = nullptr;
            continue;
        }

        set_any_val_from_json(type, tuple_pool, col, slot, pure_doc_value);
    }

    *line_eof = false;
    return Status::OK();
}


Status ScrollParser::fill_agg_tuple(const TupleDescriptor* intermediate_tuple_desc, Tuple* tuple, MemPool* mem_pool,
                                    bool* line_eof, const std::map<std::string, std::string>& docvalue_context,
                                    const int group_by_size, const std::vector<EsAggregationOp>& aggregate_functions) {
    *line_eof = true;

    if (group_by_size == 0 && _first_read_tuple) {
        // if has group by : _size > 0, do nothing
        // if has no group by : may_be _size = 0, so we must allow to read the first tuple for the batch
        // batch will be only one and has only one tuple (we will set exactly_once = true)
        _first_read_tuple = false;
    } else {
        if (_size <= 0 || _line_index >= _size) {
            return Status::OK();
        }
    }

    // if has no agg field or just a count (no group by), _inner_agg_values.size() will be 0
    bool has_agg_field_read = _size > 0;
    bool prepare_check_fail = false;

    // select count(1) from xx, now _size = 0, obj/doc_count/xxx need a value to point
    rapidjson::Value null_val;
    bool pure_doc_value = false;
    // get one aggregate tuple
    const rapidjson::Value& obj = has_agg_field_read ? _inner_agg_values[_line_index] : null_val;
    const rapidjson::Value& line = obj;
    std::string agg_field_name;

    // check rapidjson value is ok
    if (has_agg_field_read) {
        // obj should be a agg tuple but not
        prepare_check_fail |= !obj.IsObject();
    }
    if (prepare_check_fail) return Status::RuntimeError("parse aggregate tuple failed : aggregate tuple isn't a object");
    if (group_by_size > 0){
        // group by should have doc_count but not
        prepare_check_fail |= !obj.HasMember("doc_count");
    }
    if (prepare_check_fail) return Status::RuntimeError("parse aggregate tuple failed : group by field has no doc_count");

    const rapidjson::Value& doc_count = group_by_size > 0 ? obj["doc_count"] : null_val;

    if (has_agg_field_read) {
        ++_line_index;
    }

    tuple->init(intermediate_tuple_desc->byte_size());
    for (int i = 0, j = 0, k; i < intermediate_tuple_desc->slots().size(); i++) {
        const SlotDescriptor* slot_desc = intermediate_tuple_desc->slots()[i];
        PrimitiveType type = slot_desc->type().type;

        if (!slot_desc->is_materialized()) {
            continue;
        }

        if (i < group_by_size) {
            // handle group by
            VLOG_DEBUG << "Get result from ES fill agg tuple queryDSL " << json_value_to_string(line) << " ";

            // after agg, we use "group_hold1", "group_hold2", ..., to replace every group by column.
            agg_field_name = "group_hold" + std::to_string(i + 1);

            rapidjson::Value::ConstMemberIterator itr = line.FindMember(agg_field_name.c_str());
            if (itr == line.MemberEnd()) {
                tuple->set_null(slot_desc->null_indicator_offset());
                continue;
            }

            const rapidjson::Value &val = line[agg_field_name.c_str()];

            if (val.IsNull()) {
                tuple->set_null(slot_desc->null_indicator_offset());
                continue;
            } else {
                tuple->set_not_null(slot_desc->null_indicator_offset());
            }

            void* slot = tuple->get_slot(slot_desc->tuple_offset());

            Status status = set_any_val_from_json(type, mem_pool, val, slot, pure_doc_value);
            if (!status.ok()) {
                return status;
            }
            continue;
        }

        // handle aggregate
        // j is agg field index for es read(count(1/*) not included)
        // k is agg field index for get evaluator(all are included)
        k = i - group_by_size;

        if (aggregate_functions[k] != EsAggregationOp::COUNT_ONE_OR_STAR) {
            // count is special, count(1/*) don't need to set agg field
            // after agg, we use "place_hold1", "place_hold2", ..., to replace every aggregate column.
            agg_field_name = "place_hold" + std::to_string(++j);
        }

        switch (aggregate_functions[k]) {
            case EsAggregationOp::AVG: {
                string& sum_field_name = agg_field_name;
                rapidjson::Value::ConstMemberIterator itr_sum = line.FindMember(sum_field_name.c_str());
                string count_field_name = "place_hold" + std::to_string(++j);
                rapidjson::Value::ConstMemberIterator itr_count = line.FindMember(count_field_name.c_str());

                if (itr_sum == line.MemberEnd() || itr_count == line.MemberEnd()) {
                    tuple->set_null(slot_desc->null_indicator_offset());
                    break;
                }
                const rapidjson::Value &sum = line[sum_field_name.c_str()];
                const rapidjson::Value &count = line[count_field_name.c_str()];

                if (sum.IsNull() || count.IsNull()) {
                    tuple->set_null(slot_desc->null_indicator_offset());
                    break;
                } else {
                    tuple->set_not_null(slot_desc->null_indicator_offset());
                }

                void *slot = tuple->get_slot(slot_desc->tuple_offset());

                if (type != PrimitiveType::TYPE_VARCHAR && type != PrimitiveType::TYPE_CHAR) {
                    return Status::RuntimeError("avg intermediate slot desc type should be string");
                }

                StringVal dst;
                // StringVal <=> AvgState
                // can convert to each other
                dst.is_null = false;
                dst.len = sizeof(AvgState);
                // the avgState's mem will free when batch free
                dst.ptr = mem_pool->try_allocate_unaligned(dst.len);
                new(dst.ptr) AvgState;
                AvgState *avgState = reinterpret_cast<AvgState *>(dst.ptr);

                Status status1, status2;
                string error_msg = "set avg error: ";
                status1 = set_any_val_from_json(PrimitiveType::TYPE_DOUBLE, mem_pool, sum, &avgState->sum,
                                              pure_doc_value);
                status2 = set_any_val_from_json(PrimitiveType::TYPE_BIGINT, mem_pool, count, &avgState->count,
                                              pure_doc_value);

                if (!status1.ok() || !status2.ok()) {
                    if (!status1.ok()) {
                        error_msg += status1.get_error_msg();
                    }
                    if (!status2.ok()) {
                        if (!status1.ok()) error_msg += ",";
                        error_msg += status2.get_error_msg();
                    }
                    return Status::RuntimeError(error_msg);
                }

                *reinterpret_cast<StringValue *>(slot) = StringValue::from_string_val(dst);

                break;
            }
            case EsAggregationOp::COUNT_ONE_OR_STAR: {
                const rapidjson::Value &count = group_by_size > 0 ? doc_count : _count_with_null;

                if (count.IsNull()) {
                    tuple->set_null(slot_desc->null_indicator_offset());
                    break;
                } else {
                    tuple->set_not_null(slot_desc->null_indicator_offset());
                }

                void* slot = tuple->get_slot(slot_desc->tuple_offset());

                Status status = set_any_val_from_json(type, mem_pool, count, slot, pure_doc_value);
                if (!status.ok()) {
                    return status;
                }
                break;
            }
            case EsAggregationOp::COUNT:
            case EsAggregationOp::SUM:
            case EsAggregationOp::MIN:
            case EsAggregationOp::MAX: {
                rapidjson::Value::ConstMemberIterator itr = line.FindMember(agg_field_name.c_str());
                if (itr == line.MemberEnd()) {
                    tuple->set_null(slot_desc->null_indicator_offset());
                    break;
                }

                const rapidjson::Value &val = line[agg_field_name.c_str()];

                if (val.IsNull()) {
                    tuple->set_null(slot_desc->null_indicator_offset());
                    break;
                } else {
                    tuple->set_not_null(slot_desc->null_indicator_offset());
                }

                void* slot = tuple->get_slot(slot_desc->tuple_offset());

                Status status = set_any_val_from_json(type, mem_pool, val, slot, pure_doc_value);
                if (!status.ok()) {
                    return status;
                }
                break;
            }
            default:
                DCHECK(false);
                return Status::RuntimeError("Only support min/max/sum/count/avg, please check the aggregation type, "
                                            "if need to use other aggregation type, just run "
                                            "(\"set enable_pushdown_agg_to_es\"=\"false\");"
                                            " to ban pushing aggregation operator down the Es");
        }
    }

    *line_eof = false;
    return Status::OK();
}

Status ScrollParser::fill_date_slot_with_strval(void* slot, const rapidjson::Value& col,
                                                PrimitiveType type) {
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
    return Status::OK();
}

Status ScrollParser::fill_date_slot_with_timestamp(void* slot, const rapidjson::Value& col,
                                                   PrimitiveType type) {
    int64_t timestamp;
    if (!col.IsInt64()) {
        // for date, we will use value_as_string as string date
        // the code won't be run in the normal case, and it ensure the be won't crash.
        return Status::RuntimeError("get double timestamp from ES, it maybe scientific notation");
    } else {
        timestamp = col.GetInt64();
    }
    if (!reinterpret_cast<DateTimeValue*>(slot)->from_unixtime(timestamp / 1000, "+00:00")) {
        RETURN_ERROR_IF_CAST_FORMAT_ERROR(col, type);
    }
    if (type == TYPE_DATE) {
        reinterpret_cast<DateTimeValue*>(slot)->cast_to_date();
    } else {
        reinterpret_cast<DateTimeValue*>(slot)->set_type(TIME_DATETIME);
    }
    return Status::OK();
}

Status ScrollParser::set_any_val_from_json(PrimitiveType type, MemPool* tuple_pool, const rapidjson::Value& val, void* slot, bool pure_doc_value) {
    switch (type) {
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
            // sometimes elasticsearch user post some not-string value to Elasticsearch Index.
            // because of reading value from _source, we can not process all json type and then just transfer the value to original string representation
            // this may be a tricky, but we can workaround this issue
            std::string string_val;
            if (pure_doc_value) {
                if (!val[0].IsString()) {
                    string_val = json_value_to_string(val[0]);
                } else {
                    string_val = val[0].GetString();
                }
            } else {
                RETURN_ERROR_IF_COL_IS_ARRAY(val, type);
                if (!val.IsString()) {
                    string_val = json_value_to_string(val);
                } else {
                    string_val = val.GetString();
                }
            }
            size_t string_val_size = string_val.length();
            char* buffer = reinterpret_cast<char*>(tuple_pool->try_allocate_unaligned(string_val_size));
            if (UNLIKELY(buffer == NULL)) {
                std::string details = strings::Substitute(
                        ERROR_MEM_LIMIT_EXCEEDED, "MaterializeNextRow", string_val_size, "string slot");
                return tuple_pool->mem_tracker()->MemLimitExceeded(NULL, details, string_val_size);
            }
            memcpy(buffer, string_val.data(), string_val_size);
            reinterpret_cast<StringValue*>(slot)->ptr = buffer;
            reinterpret_cast<StringValue*>(slot)->len = string_val_size;
            break;
        }

        case TYPE_TINYINT: {
            Status status = get_int_value<int8_t>(val, type, slot, pure_doc_value);
            if (!status.ok()) {
                return status;
            }
            break;
        }

        case TYPE_SMALLINT: {
            Status status = get_int_value<int16_t>(val, type, slot, pure_doc_value);
            if (!status.ok()) {
                return status;
            }
            break;
        }

        case TYPE_INT: {
            Status status = get_int_value<int32_t>(val, type, slot, pure_doc_value);
            if (!status.ok()) {
                return status;
            }
            break;
        }

        case TYPE_BIGINT: {
            Status status = get_int_value<int64_t>(val, type, slot, pure_doc_value);
            if (!status.ok()) {
                return status;
            }
            break;
        }

        case TYPE_LARGEINT: {
            Status status = get_int_value<__int128>(val, type, slot, pure_doc_value);
            if (!status.ok()) {
                return status;
            }
            break;
        }

        case TYPE_DOUBLE: {
            Status status = get_float_value<double>(val, type, slot, pure_doc_value);
            if (!status.ok()) {
                return status;
            }
            break;
        }

        case TYPE_FLOAT: {
            Status status = get_float_value<float>(val, type, slot, pure_doc_value);
            if (!status.ok()) {
                return status;
            }
            break;
        }

        case TYPE_BOOLEAN: {
            if (val.IsBool()) {
                *reinterpret_cast<int8_t*>(slot) = val.GetBool();
                break;
            }

            if (val.IsNumber()) {
                *reinterpret_cast<int8_t*>(slot) = val.GetInt();
                break;
            }
            if (pure_doc_value && val.IsArray()) {
                *reinterpret_cast<int8_t*>(slot) = val[0].GetBool();
                break;
            }

            RETURN_ERROR_IF_COL_IS_ARRAY(val, type);
            RETURN_ERROR_IF_COL_IS_NOT_STRING(val, type);

            const std::string& bool_val = val.GetString();
            size_t bool_val_size = val.GetStringLength();
            StringParser::ParseResult result;
            bool b = StringParser::string_to_bool(bool_val.c_str(), bool_val_size, &result);
            RETURN_ERROR_IF_PARSING_FAILED(result, val, type);
            *reinterpret_cast<int8_t*>(slot) = b;
            break;
        }

        case TYPE_DATE:
        case TYPE_DATETIME: {
            if (val.IsNumber()) {
                // ES process date/datetime field would use millisecond timestamp for index or docvalue
                // processing date type field, if a number is encountered, Doris On ES will force it to be processed according to ms
                // Doris On ES needs to be consistent with ES, so just divided by 1000 because the unit for from_unixtime is seconds
                RETURN_IF_ERROR(fill_date_slot_with_timestamp(slot, val, type));
            } else if (val.IsArray() && pure_doc_value) {
                // this would happened just only when `enable_docvalue_scan = true`
                // ES add default format for all field after ES 6.4, if we not provided format for `date` field ES would impose
                // a standard date-format for date field as `2020-06-16T00:00:00.000Z`
                // At present, we just process this string format date. After some PR were merged into Doris, we would impose `epoch_mills` for
                // date field's docvalue
                if (val[0].IsString()) {
                    RETURN_IF_ERROR(fill_date_slot_with_strval(slot, val[0], type));
                    break;
                }
                // ES would return millisecond timestamp for date field, divided by 1000 because the unit for from_unixtime is seconds
                RETURN_IF_ERROR(fill_date_slot_with_timestamp(slot, val[0], type));
            } else {
                RETURN_ERROR_IF_COL_IS_ARRAY(val, type);
                RETURN_ERROR_IF_COL_IS_NOT_STRING(val, type);
                RETURN_IF_ERROR(fill_date_slot_with_strval(slot, val, type));
            }
            break;
        }
        default: {
            DCHECK(false);
            return Status::RuntimeError("Excepted Type is in (int/date/double/varchar...), but found type\n"
                                        "is" + type_to_string(type) + ", please check the Es table type or Aggregation column type");
        }
    }

    return Status::OK();
}

} // namespace doris
