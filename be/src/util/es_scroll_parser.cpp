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
#include "rapidjson/document.h"
#include "common/logging.h"
#include "common/status.h"

namespace doris {

const char* FIELD_SCROLL_ID = "_scroll_id";
const char* FIELD_HITS = "hits";
const char* FIELD_INNER_HITS = "hits";
const char* FIELD_SOURCE = "_source";
const char* FIELD_TOTAL = "total";

ScrollParser::ScrollParser() {
    _eos = false;
    _total = 0;
}

ScrollParser::~ScrollParser() {
}


Status ScrollParser::parse(const std::string& scroll_result) {
    rapidjson::Document document_node;
    document_node.Parse<0>(scroll_result.c_str());
    if (!document_node.HasMember(FIELD_SCROLL_ID)) {
        return Status("maybe not a scroll request");
    }
    rapidjson::Value &scroll_node = document_node[FIELD_SCROLL_ID];
    _scroll_id = scroll_node.GetString();
    // { hits: { total : 2, "hits" : [ {}, {}, {} ]}}
    rapidjson::Value &outer_hits_node = document_node[FIELD_HITS];
    rapidjson::Value &total = document_node[FIELD_TOTAL];
    _total = total.GetInt();
    if (_total == 0) {
        _eos = true;
        return Status::OK;
    }
    VLOG(1) << "es_scan_reader total hits: " << _total << " documents";
    rapidjson::Value &inner_hits_node = outer_hits_node[FIELD_INNER_HITS];
    if (!inner_hits_node.IsArray()) {
        return Status("invalid response from elasticsearch");
    }
    _size = inner_hits_node.Size();
    if (_size < _batch_size) {
        _eos = true;
    }
    return Status::OK;
}

bool ScrollParser::has_next() {
    return _eos;
}

bool ScrollParser::count() {
    return _size;
}

std::string ScrollParser::get_scroll_id() {
    return _scroll_id;
}
}
