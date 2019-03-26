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

#include "common/logging.h"
#include "common/status.h"

namespace doris {

const char* FIELD_SCROLL_ID = "_scroll_id";
const char* FIELD_HITS = "hits";
const char* FIELD_INNER_HITS = "hits";
const char* FIELD_SOURCE = "_source";
const char* FIELD_TOTAL = "total";

ScrollParser::ScrollParser(const std::string& scroll_id, int total, int size) :
    _scroll_id(scroll_id),
    _total(total),
    _size(size) {
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
    rapidjson::Value &field_total = document_node[FIELD_TOTAL];
    int total = field_total.GetInt();
    if (total == 0) {
        scroll_parser = new ScrollParser(scroll_id, total, 0);
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


Status ScrollParser::read_next_line(const char** ptr, size_t* size, bool* line_eof) {
    *line_eof = true;
    return Status::OK;
}

}
