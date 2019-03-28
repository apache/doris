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

#include "es_scroll_parser.h"

#include <string>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "common/status.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

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

ScrollParser::ScrollParser(std::string scroll_id, int total, int size) :
    _scroll_id(scroll_id),
    _total(total),
    _size(size),
    _line_index(0) {
}

ScrollParser::~ScrollParser() {
}


ScrollParser* ScrollParser::parse_from_string(const std::string& scroll_result) {
    ScrollParser* scroll_parser = nullptr;
    rapidjson::Document document_node;
    document_node.Parse<0>(scroll_result.c_str());

    if (!document_node.HasMember(FIELD_SCROLL_ID)) {
        LOG(ERROR) << "maybe not a scroll request";
        return nullptr;
    }

    rapidjson::Value &scroll_node = document_node[FIELD_SCROLL_ID];
    std::string scroll_id = scroll_node.GetString();
    // { hits: { total : 2, "hits" : [ {}, {}, {} ]}}
    rapidjson::Value &outer_hits_node = document_node[FIELD_HITS];
    rapidjson::Value &field_total = outer_hits_node[FIELD_TOTAL];
    int total = field_total.GetInt();
    if (total == 0) {
        scroll_parser = new ScrollParser(scroll_id, total);
        return scroll_parser;
    }

    VLOG(1) << "es_scan_reader total hits: " << total << " documents";
    rapidjson::Value &inner_hits_node = outer_hits_node[FIELD_INNER_HITS];
    if (!inner_hits_node.IsArray()) {
        LOG(ERROR) << "maybe not a scroll request";
        return nullptr;
    }

    int size = inner_hits_node.Size();
    scroll_parser = new ScrollParser(scroll_id, total, size);
    scroll_parser->set_inner_hits_node(inner_hits_node);
    return scroll_parser;
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

    rapidjson::Value& obj = _inner_hits_node[_line_index++];
    rapidjson::Value& line = obj[FIELD_SOURCE];
    if (!line.IsObject()) {
        return Status("Parse inner hits failed");
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
        rapidjson::Value &col = line[col_name];

        void* slot = tuple->get_slot(slot_desc->tuple_offset());
        switch (slot_desc->type().type) {
            case TYPE_CHAR:
            case TYPE_VARCHAR: {
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
                if (!col.IsNumber()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TINYINT"));
                }
                *reinterpret_cast<int8_t*>(slot) = (int8_t)col.GetInt();
                break;
            }

            case TYPE_SMALLINT: {
                if (!col.IsNumber()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "SMALLINT"));
                }
                *reinterpret_cast<int16_t*>(slot) = (int16_t)col.GetInt();
                break;
            }

            case TYPE_INT: {
                if (!col.IsNumber()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "INT"));
                }
                *reinterpret_cast<int32_t*>(slot) = (int32_t)col.GetInt();
                break;
            }

            case TYPE_BIGINT: {
                if (!col.IsNumber()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "BIGINT"));
                }
                *reinterpret_cast<int64_t*>(slot) = col.GetInt64();
                break;
            }

            case TYPE_LARGEINT: {
                if (!col.IsNumber()) {
                   return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "LARGEINT"));
                }
                *reinterpret_cast<int128_t*>(slot) = col.GetInt64();
                break;
            }

            case TYPE_DOUBLE: {
                if (!col.IsNumber()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "DOUBLE"));
                }
                *reinterpret_cast<double*>(slot) = col.GetDouble();
                break;
            }

            case TYPE_FLOAT: {
                if (!col.IsNumber()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "FLOAT"));
                }
                *reinterpret_cast<float*>(slot) = col.GetDouble();
                break;
            }

            case TYPE_BOOLEAN: {
                if (!col.IsBool()) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "BOOLEAN"));
                }
                *reinterpret_cast<int8_t*>(slot) = col.GetBool();
                break;
            }

            case TYPE_DATE: {
                if (!col.IsNumber() || 
                  !reinterpret_cast<DateTimeValue*>(slot)->from_unixtime(col.GetInt64())) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATE"));
                }
                reinterpret_cast<DateTimeValue*>(slot)->cast_to_date();
                break;
            }

            case TYPE_DATETIME: {
                if (!col.IsNumber() || 
                  !reinterpret_cast<DateTimeValue*>(slot)->from_unixtime(col.GetInt64())) {
                    return Status(strings::Substitute(ERROR_INVALID_COL_DATA, "TYPE_DATETIME"));
                }
                reinterpret_cast<DateTimeValue*>(slot)->set_type(TIME_DATETIME);
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
