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

#include "exec/es/es_scroll_query.h"

#include <boost/algorithm/string/join.hpp>
#include <sstream>

#include "common/logging.h"
#include "exec/es/es_query_builder.h"
#include "exec/es/es_scan_reader.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace doris {

ESScrollQueryBuilder::ESScrollQueryBuilder() {

}

ESScrollQueryBuilder::~ESScrollQueryBuilder() {
    
}

std::string ESScrollQueryBuilder::build_next_scroll_body(const std::string& scroll_id, const std::string& scroll) {
    rapidjson::Document scroll_dsl;
    rapidjson::Document::AllocatorType &allocator = scroll_dsl.GetAllocator();
    scroll_dsl.SetObject();
    rapidjson::Value scroll_id_value(scroll_id.c_str(), allocator);
    scroll_dsl.AddMember("scroll_id", scroll_id_value, allocator);
    rapidjson::Value scroll_value(scroll.c_str(), allocator);
    scroll_dsl.AddMember("scroll", scroll_value, allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    scroll_dsl.Accept(writer);
    return buffer.GetString();
}
std::string ESScrollQueryBuilder::build_clear_scroll_body(const std::string& scroll_id) {
    rapidjson::Document delete_scroll_dsl;
    rapidjson::Document::AllocatorType &allocator = delete_scroll_dsl.GetAllocator();
    delete_scroll_dsl.SetObject();
    rapidjson::Value scroll_id_value(scroll_id.c_str(), allocator);
    delete_scroll_dsl.AddMember("scroll_id", scroll_id_value, allocator);
    rapidjson::StringBuffer buffer;  
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    delete_scroll_dsl.Accept(writer);
    return buffer.GetString();
}

std::string ESScrollQueryBuilder::build(const std::map<std::string, std::string>& properties,
                const std::vector<std::string>& fields,
                std::vector<EsPredicate*>& predicates) {
    rapidjson::Document es_query_dsl;
    rapidjson::Document::AllocatorType &allocator = es_query_dsl.GetAllocator();
    es_query_dsl.SetObject();
    // generate the filter caluse
    rapidjson::Document scratch_document;
    rapidjson::Value query_node(rapidjson::kObjectType);
    query_node.SetObject();
    BooleanQueryBuilder::to_query(predicates, &scratch_document, &query_node);
    // note: add `query` for this value....
    es_query_dsl.AddMember("query", query_node, allocator);
    // just filter the selected fields for reducing the network cost
    if (fields.size() > 0) {
        rapidjson::Value source_node(rapidjson::kArrayType);
        for (auto iter = fields.begin(); iter != fields.end(); iter++) {
            rapidjson::Value field(iter->c_str(), allocator);
            source_node.PushBack(field, allocator);
        }
        es_query_dsl.AddMember("_source", source_node, allocator);
    }
    int size = atoi(properties.at(ESScanReader::KEY_BATCH_SIZE).c_str());
    rapidjson::Value sort_node(rapidjson::kArrayType);
    // use the scroll-scan mode for scan index documents
    rapidjson::Value field("_doc", allocator);
    sort_node.PushBack(field, allocator);
    es_query_dsl.AddMember("sort", sort_node, allocator);
    // number of docuements returned
    es_query_dsl.AddMember("size", size, allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    es_query_dsl.Accept(writer);
    std::string es_query_dsl_json = buffer.GetString();
    LOG(INFO) << "Generated ES queryDSL [ " << es_query_dsl_json << " ]";
    return es_query_dsl_json;                

}
}
