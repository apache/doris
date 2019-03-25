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

#include "es_search_builder.h"
#include<sstream>
#include <boost/algorithm/string/join.hpp>
#include "common/logging.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace doris {

SearchRequestBuilder::SearchRequestBuilder() {

}

SearchRequestBuilder::~SearchRequestBuilder() {
    
}

std::string SearchRequestBuilder::build() {
    rapidjson::Document es_query_dsl;
    rapidjson::Document::AllocatorType &allocator = es_query_dsl.GetAllocator();
    es_query_dsl.SetObject();
    if (_fields.size() > 0) {
        rapidjson::Value source_node(rapidjson::kArrayType);
        for (auto iter = _fields.cbegin(); iter != _fields.cend(); iter++) {
            rapidjson::Value field(iter->c_str(), allocator);
            source_node.PushBack(field, allocator);
        }
        es_query_dsl.AddMember("_source", source_node, allocator);
    }
    
    rapidjson::Value sort_node(rapidjson::kArrayType);
    rapidjson::Value field("_doc", allocator);
    sort_node.PushBack(field, allocator);
    es_query_dsl.AddMember("sort", sort_node, allocator);

    es_query_dsl.AddMember("size", _size, allocator);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    es_query_dsl.Accept(writer);
    std::string es_query_dsl_json = buffer.GetString();
    return es_query_dsl_json;
}

}
