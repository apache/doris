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

#include <glog/logging.h>
#include <rapidjson/encodings.h>
#include <rapidjson/rapidjson.h>
#include <stdlib.h>

#include <sstream>

#include "exec/es/es_scan_reader.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace doris {

ESScrollQueryBuilder::ESScrollQueryBuilder() {}

ESScrollQueryBuilder::~ESScrollQueryBuilder() {}

std::string ESScrollQueryBuilder::build_next_scroll_body(const std::string& scroll_id,
                                                         const std::string& scroll) {
    rapidjson::Document scroll_dsl;
    rapidjson::Document::AllocatorType& allocator = scroll_dsl.GetAllocator();
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
    rapidjson::Document::AllocatorType& allocator = delete_scroll_dsl.GetAllocator();
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
                                        const std::map<std::string, std::string>& docvalue_context,
                                        bool* doc_value_mode) {
    rapidjson::Document es_query_dsl;
    rapidjson::Document::AllocatorType& allocator = es_query_dsl.GetAllocator();
    es_query_dsl.SetObject();
    // generate the filter clause
    rapidjson::Document scratch_document;
    rapidjson::Value query_node(rapidjson::kObjectType);
    // use fe generate dsl, it must be placed outside the if, otherwise it will cause problems in AddMember
    rapidjson::Document fe_query_dsl;
    DCHECK(properties.find(ESScanReader::KEY_QUERY_DSL) != properties.end());
    auto query_dsl = properties.at(ESScanReader::KEY_QUERY_DSL);
    es_query_dsl.AddMember("query", fe_query_dsl.Parse(query_dsl.c_str(), query_dsl.length()),
                           allocator);

    // Doris FE already has checked docvalue-scan optimization
    bool pure_docvalue = true;
    if (properties.find(ESScanReader::KEY_DOC_VALUES_MODE) != properties.end()) {
        pure_docvalue = atoi(properties.at(ESScanReader::KEY_DOC_VALUES_MODE).c_str());
    } else {
        // check docvalue scan optimization, used for compatibility
        if (docvalue_context.size() == 0 || docvalue_context.size() < fields.size()) {
            pure_docvalue = false;
        } else {
            for (auto& select_field : fields) {
                if (docvalue_context.find(select_field) == docvalue_context.end()) {
                    pure_docvalue = false;
                    break;
                }
            }
        }
    }

    *doc_value_mode = pure_docvalue;

    rapidjson::Value source_node(rapidjson::kArrayType);
    if (pure_docvalue) {
        for (auto& select_field : fields) {
            rapidjson::Value field(docvalue_context.at(select_field).c_str(), allocator);
            source_node.PushBack(field, allocator);
        }
    } else {
        for (auto& select_field : fields) {
            rapidjson::Value field(select_field.c_str(), allocator);
            source_node.PushBack(field, allocator);
        }
    }

    // just filter the selected fields for reducing the network cost
    if (pure_docvalue) {
        es_query_dsl.AddMember("stored_fields", "_none_", allocator);
        es_query_dsl.AddMember("docvalue_fields", source_node, allocator);
    } else {
        es_query_dsl.AddMember("_source", source_node, allocator);
    }

    int size;
    if (properties.find(ESScanReader::KEY_TERMINATE_AFTER) != properties.end()) {
        size = atoi(properties.at(ESScanReader::KEY_TERMINATE_AFTER).c_str());
    } else {
        size = atoi(properties.at(ESScanReader::KEY_BATCH_SIZE).c_str());
    }

    std::string shard_id;
    if (properties.find(ESScanReader::KEY_SHARD) != properties.end()) {
        shard_id = properties.at(ESScanReader::KEY_SHARD);
    }
    // To maintain consistency with the query, when shard_id is negative, do not add sort node in scroll request body.
    if (!shard_id.empty() && std::stoi(shard_id) >= 0) {
        rapidjson::Value sort_node(rapidjson::kArrayType);
        // use the scroll-scan mode for scan index documents
        rapidjson::Value field("_doc", allocator);
        sort_node.PushBack(field, allocator);
        es_query_dsl.AddMember("sort", sort_node, allocator);
    }
    // number of documents returned
    es_query_dsl.AddMember("size", size, allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    es_query_dsl.Accept(writer);
    std::string es_query_dsl_json = buffer.GetString();
    LOG(INFO) << "Generated ES queryDSL [ " << es_query_dsl_json << " ]";
    return es_query_dsl_json;
}
} // namespace doris
