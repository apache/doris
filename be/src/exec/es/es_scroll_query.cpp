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

#include <sstream>

#include "common/logging.h"
#include "exec/es/es_query_builder.h"
#include "exec/es/es_scan_reader.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "gutil/strings/join.h"

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

Status ESScrollQueryBuilder::build_composite_page_body(const std::string& es_first_composite_query, const std::string& after_key, std::string& result) {
    rapidjson::Document es_composite_query_dsl_after_key;
    rapidjson::Document es_result_dsl_after_key;
    rapidjson::Document::AllocatorType& allocator = es_composite_query_dsl_after_key.GetAllocator();

    es_composite_query_dsl_after_key.Parse(es_first_composite_query.c_str());
    es_result_dsl_after_key.Parse(after_key.c_str());

    if (es_composite_query_dsl_after_key.HasParseError()) {
        std::stringstream ss;
        ss << "Parsing json error, json is: " << es_first_composite_query;
        return Status::RuntimeError(ss.str());
    }
    if (es_result_dsl_after_key.HasParseError()) {
        std::stringstream ss;
        ss << "Parsing json error, json is: " << after_key;
        return Status::RuntimeError(ss.str());
    }

    if (es_composite_query_dsl_after_key.IsObject() && es_composite_query_dsl_after_key.HasMember("aggs")) {
        rapidjson::Value& aggregations_node = es_composite_query_dsl_after_key["aggs"];
        if (aggregations_node.IsObject() && aggregations_node.HasMember("groupby")) {
            rapidjson::Value& group_by_node = aggregations_node["groupby"];
            if (group_by_node.IsObject() && group_by_node.HasMember("composite")) {
                rapidjson::Value& composite_node = group_by_node["composite"];
                if (es_result_dsl_after_key.IsObject()) {
                    rapidjson::Value after;
                    after.SetObject();
                    for (rapidjson::Value::ConstMemberIterator itr = es_result_dsl_after_key.MemberBegin(); itr != es_result_dsl_after_key.MemberEnd(); ++itr) {
                        rapidjson::Value jKey;
                        rapidjson::Value jValue;
                        jKey.CopyFrom(itr->name, allocator);
                        jValue.CopyFrom(itr->value, allocator);
                        after.AddMember(jKey, jValue, allocator);
                    }
                    composite_node.AddMember("after", after, allocator);
                } else return Status::RuntimeError("Can't get correct composite query dsl, dsl is: " + after_key);
            } else return Status::RuntimeError("Can't get correct composite query dsl, dsl is: " + es_first_composite_query);
        } else return Status::RuntimeError("Can't get correct composite query dsl, dsl is: " + es_first_composite_query);
    } else return Status::RuntimeError("Can't get correct composite query dsl, dsl is: " + es_first_composite_query);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    es_composite_query_dsl_after_key.Accept(writer);
    result = buffer.GetString();
    VLOG_DEBUG << "Es paged composite query dsl is: " + result;
    return Status::OK();
}

std::string ESScrollQueryBuilder::build(const std::map<std::string, std::string>& properties,
                                        const std::vector<std::string>& fields,
                                        std::vector<EsPredicate*>& predicates,
                                        const std::map<std::string, std::string>& docvalue_context,
                                        bool* doc_value_mode,
                                        bool is_agg,
                                        const std::vector<std::string>& group_by_fields,
                                        const std::vector<std::string>& aggregate_fields,
                                        const std::vector<std::string>& aggregate_functions) {
    rapidjson::Document es_query_dsl;
    rapidjson::Document::AllocatorType& allocator = es_query_dsl.GetAllocator();
    es_query_dsl.SetObject();

    // generate the filter clause
    rapidjson::Document scratch_document;
    rapidjson::Value query_node(rapidjson::kObjectType);
    query_node.SetObject();
    BooleanQueryBuilder::to_query(predicates, &scratch_document, &query_node);
    // note: add `query` for this value....
    es_query_dsl.AddMember("query", query_node, allocator);
    bool pure_docvalue = true;

    // Doris FE already has checked docvalue-scan optimization
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

    // when read too many data rows, the <hits.total.value> maybe get approximate count value
    // eg. result is {value:10000, relation:eq}
    // track_total_hits=true can make ES return the accurate result
    es_query_dsl.AddMember("track_total_hits", true, allocator);

    int size;
    if (properties.find(ESScanReader::KEY_TERMINATE_AFTER) != properties.end()) {
        size = atoi(properties.at(ESScanReader::KEY_TERMINATE_AFTER).c_str());
    } else {
        size = atoi(properties.at(ESScanReader::KEY_BATCH_SIZE).c_str());
    }
    rapidjson::Value sort_node(rapidjson::kArrayType);
    // use the scroll-scan mode for scan index documents
    rapidjson::Value field("_doc", allocator);
    sort_node.PushBack(field, allocator);
    es_query_dsl.AddMember("sort", sort_node, allocator);

    if (is_agg) {
        /*
        for ES agg pushdown scenario, we need to build following json.
        {
            "size":0,
            "aggs":{
                "place_hold1":{
                    "max":{
                        "field":"k1"
                    }
                },
                "place_hold2":{
                    "min":{
                        "field":"k5"
                    }
                }
            }
        }
        */

        /*
         * By default, searches containing an aggregation return 
         * both search hits and aggregation results. 
         * To return only aggregation results, set size to 0.
         */
        es_query_dsl.AddMember("size", 0, allocator);

        rapidjson::Value aggs(rapidjson::kObjectType);
        aggs.SetObject();

        int i = 1;
        // specify multiple aggregations in the same request.
        for (auto& aggregate_field : aggregate_fields) {
            rapidjson::Value field;
            field.SetObject();

            string actual_select_field = aggregate_field;
            if (docvalue_context.count(aggregate_field)) {
                // FE ensure if es aggregation, docvalue_context must have aggregate_fields
                actual_select_field = docvalue_context.at(aggregate_field);
            }
            rapidjson::Value field_name(actual_select_field.c_str(), allocator);
            field.AddMember("field", field_name, allocator);    // {"field": "coloum_name"}

            rapidjson::Value agg_kind(rapidjson::kObjectType);
            agg_kind.SetObject();
            rapidjson::Value agg_name(aggregate_functions[i - 1].c_str(), allocator);   // "max" or "min" or "sum" or "avg"(->"sum"). etc
            agg_kind.AddMember(agg_name, field, allocator);    // {"min":{"field": "coloum_name"}}

            rapidjson::Value place_hold(("place_hold" + std::to_string(i++)).c_str(), allocator);
            aggs.AddMember(place_hold, agg_kind, allocator);    // {"place_hold1":{"min":{"field":"coloum_name"}}}
        }

        if (group_by_fields.size() > 0) {
            /* DSL AIM:
                {
                "aggs": {
                    "groupby": {
                        "composite": {"size":1024, "sources": []},
                        "aggs":{"place_hold1":{"min":{"field":"price"}}}
                    }
                }
            */

            /*
             * The sources parameter defines the source fields to use when building composite buckets.
             * The order that the sources are defined controls the order that the keys are returned.
            */
            rapidjson::Value sources(rapidjson::kArrayType);
            int j = 1;
            for (auto& group_filed : group_by_fields) {
                std::string place_hold = "group_hold" + std::to_string(j++);

                rapidjson::Value term(rapidjson::kObjectType);
                term.SetObject();
                string actual_select_field = group_filed;
                if (docvalue_context.count(group_filed)) {
                    // FE ensure if es aggregation, docvalue_context must have group_fields
                    actual_select_field = docvalue_context.at(group_filed);
                }
                rapidjson::Value field_name(actual_select_field.c_str(), allocator);
                term.AddMember("field", field_name, allocator);
                term.AddMember("missing_bucket", true, allocator);

                rapidjson::Value terms(rapidjson::kObjectType);
                terms.SetObject();
                terms.AddMember("terms", term, allocator);

                rapidjson::Value group_name(place_hold.c_str(), allocator);
                rapidjson::Value group;  group.SetObject(); 
                group.AddMember(group_name, terms, allocator);

                sources.PushBack(group, allocator);
            }

            rapidjson::Value composite;
            composite.SetObject();  
            composite.AddMember("sources", sources, allocator);
            // group by need paging
            composite.AddMember("size", size, allocator);

            rapidjson::Value groupby;    
            groupby.SetObject();   
            groupby.AddMember("composite", composite, allocator);  
            groupby.AddMember("aggs", aggs, allocator);

            rapidjson::Value agg_outer;  
            agg_outer.SetObject(); 
            agg_outer.AddMember("groupby", groupby, allocator);
            es_query_dsl.AddMember("aggs", agg_outer, allocator);
        } else {
            // agg without group by
            es_query_dsl.AddMember("aggs", aggs, allocator); // {"aggs":{"place_hold1":{"min":{"field":"coloum_name"}}}
        }
    } else {
        // number of documents returned
        es_query_dsl.AddMember("size", size, allocator);
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    es_query_dsl.Accept(writer);
    std::string es_query_dsl_json = buffer.GetString();
    LOG(INFO) << "Generated ES queryDSL [ " << es_query_dsl_json << " ]";
    return es_query_dsl_json;
}
} // namespace doris
