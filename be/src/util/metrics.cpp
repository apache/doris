// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#include "util/metrics.h"
#include <sstream>
#include <memory>
#include <functional>

#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "util/palo_metrics.h"
#include "http/web_page_handler.h"

namespace palo {

template <>
void ToJsonValue<std::string>(const std::string& value, const TUnit::type unit,
        rapidjson::Document* document, rapidjson::Value* out_val) {
    rapidjson::Value val(value.c_str(), document->GetAllocator());
    *out_val = val;
}

void Metric::AddStandardFields(rapidjson::Document* document, rapidjson::Value* val) {
    rapidjson::Value name(_key.c_str(), document->GetAllocator());
    val->AddMember("name", name, document->GetAllocator());
    rapidjson::Value desc(_description.c_str(), document->GetAllocator());
    val->AddMember("description", desc, document->GetAllocator());
    rapidjson::Value metric_value(ToHumanReadable().c_str(), document->GetAllocator());
    val->AddMember("human_readable", metric_value, document->GetAllocator());
}

MetricDefs* MetricDefs::GetInstance() {
    // Note that this is not thread-safe in C++03 (but will be in C++11 see
    // http://stackoverflow.com/a/19907903/132034). We don't bother with the double-check
    // locking pattern because it introduces complexity whereas a race is very unlikely
    // and it doesn't matter if we construct two instances since MetricDefsConstants is
    // just a constant map.
    static MetricDefs instance;
    return &instance;
}

TMetricDef MetricDefs::Get(const std::string& key, const std::string& arg) {
    MetricDefs* inst = GetInstance();
    std::map<std::string, TMetricDef>::iterator it = inst->_metric_defs.TMetricDefs.find(key);
    if (it == inst->_metric_defs.TMetricDefs.end()) {
        DCHECK(false) << "Could not find metric definition for key=" << key << " arg=" << arg;
        return TMetricDef();
    }
    TMetricDef md = it->second;
    md.__set_key(strings::Substitute(md.key, arg));
    md.__set_description(strings::Substitute(md.description, arg));
    return md;
}

MetricGroup::MetricGroup(const std::string& name)
    : _obj_pool(new ObjectPool()), _name(name) { }

Status MetricGroup::init(WebPageHandler* webserver) {
    if (webserver != NULL) {
        WebPageHandler::PageHandlerCallback default_callback =
            boost::bind<void>(boost::mem_fn(&MetricGroup::text_callback), this, _1, _2);
        webserver->register_page("/metrics", default_callback);

        WebPageHandler::PageHandlerCallback json_callback =
            boost::bind<void>(boost::mem_fn(&MetricGroup::json_callback), this, _1, _2);
        webserver->register_page("/jsonmetrics", json_callback);
    }

    return Status::OK;
}

/// TODO: init, CMCompatibleCallback, TemplateCallback are for new webserver
/*
Status MetricGroup::init(Webserver* webserver) {
    if (webserver != NULL) {
        Webserver::UrlCallback default_callback =
            bind<void>(mem_fn(&MetricGroup::CMCompatibleCallback), this, _1, _2);
        webserver->RegisterUrlCallback("/jsonmetrics", "legacy-metrics.tmpl",
                default_callback, false);

        Webserver::UrlCallback json_callback =
            bind<void>(mem_fn(&MetricGroup::TemplateCallback), this, _1, _2);
        webserver->RegisterUrlCallback("/metrics", "metrics.tmpl", json_callback);
    }

    return Status::OK();
}

void MetricGroup::CMCompatibleCallback(const Webserver::ArgumentMap& args,
                                       Document* document) {
    // If the request has a 'metric' argument, search all top-level metrics for that metric
    // only. Otherwise, return document with list of all metrics at the top level.
    Webserver::ArgumentMap::const_iterator metric_name = args.find("metric");

    lock_guard<SpinLock> l(_lock);
    if (metric_name != args.end()) {
        MetricMap::const_iterator metric = _metric_map.find(metric_name->second);
        if (metric != _metric_map.end()) {
            metric->second->ToLegacyJson(document);
        }
        return;
    }

    stack<MetricGroup*> groups;
    groups.push(this);
    do {
        // Depth-first traversal of children to flatten all metrics, which is what was
        // expected by CM before we introduced metric groups.
        MetricGroup* group = groups.top();
        for (const ChildGroupMap::value_type& child: group->_children) {
            groups.push(child.second);
        }
        for (const MetricMap::value_type& m: group->_metric_map) {
            m.second->ToLegacyJson(document);
        }
    } while (!groups.empty());
}

void MetricGroup::TemplateCallback(const Webserver::ArgumentMap& args,
                                   Document* document) {
    Webserver::ArgumentMap::const_iterator metric_group = args.find("metric_group");

    lock_guard<SpinLock> l(_lock);
    // If no particular metric group is requested, render this metric group (and all its
    // children).
    if (metric_group == args.end()) {
        Value container;
        ToJson(true, document, &container);
        document->AddMember("metric_group", container, document->GetAllocator());
        return;
    }

    // Search all metric groups to find the one we're looking for. In the future, we'll
    // change this to support path-based resolution of metric groups.
    MetricGroup* found_group = NULL;
    stack<MetricGroup*> groups;
    groups.push(this);
    while (!groups.empty() && found_group == NULL) {
        // Depth-first traversal of children to flatten all metrics, which is what was
        // expected by CM before we introduced metric groups.
        MetricGroup* group = groups.top();
        groups.pop();
        for (const ChildGroupMap::value_type& child: group->_children) {
            if (child.first == metric_group->second) {
                found_group = child.second;
                break;
            }
            groups.push(child.second);
        }
    }
    if (found_group != NULL) {
        Value container;
        found_group->ToJson(false, document, &container);
        document->AddMember("metric_group", container, document->GetAllocator());
    } else {
        Value error(Substitute("Metric group $0 not found", metric_group->second).c_str(),
                    document->GetAllocator());
        document->AddMember("error", error, document->GetAllocator());
    }
}
*/

void MetricGroup::ToJson(bool include_children, rapidjson::Document* document, rapidjson::Value* out_val) {
    rapidjson::Value metric_list(rapidjson::kArrayType);
    for (const MetricMap::value_type& m: _metric_map) {
        rapidjson::Value metric_value;
        m.second->ToJson(document, &metric_value);
        metric_list.PushBack(metric_value, document->GetAllocator());
    }

    rapidjson::Value container(rapidjson::kObjectType);
    container.AddMember("metrics", metric_list, document->GetAllocator());
    container.AddMember("name", 
        rapidjson::Value(_name.c_str(), document->GetAllocator()).Move(), document->GetAllocator());
    if (include_children) {
        rapidjson::Value child_groups(rapidjson::kArrayType);
        for (const ChildGroupMap::value_type& child: _children) {
            rapidjson::Value child_value;
            child.second->ToJson(true, document, &child_value);
            child_groups.PushBack(child_value, document->GetAllocator());
        }
        container.AddMember("child_groups", child_groups, document->GetAllocator());
    }

    *out_val = container;
}

MetricGroup* MetricGroup::GetOrCreateChildGroup(const std::string& name) {
    std::lock_guard<SpinLock> l(_lock);
    ChildGroupMap::iterator it = _children.find(name);
    if (it != _children.end()) return it->second;
    MetricGroup* group = _obj_pool->add(new MetricGroup(name));
    _children[name] = group;
    return group;
}

MetricGroup* MetricGroup::FindChildGroup(const std::string& name) {
    std::lock_guard<SpinLock> l(_lock);
    ChildGroupMap::iterator it = _children.find(name);
    if (it != _children.end()) return it->second;
    return NULL;
}

///TODO: debug string is for new web server
/*
std::string MetricGroup::debug_string() {
    Webserver::ArgumentMap empty_map;
    rapidjson::Document document;
    document.SetObject();
    TemplateCallback(empty_map, &document);
    StringBuffer strbuf;
    PrettyWriter<StringBuffer> writer(strbuf);
    document.Accept(writer);
    return strbuf.GetString();
}
*/

TMetricDef MakeTMetricDef(const std::string& key, TMetricKind::type kind,
                                  TUnit::type unit) {
    TMetricDef ret;
    ret.__set_key(key);
    ret.__set_kind(kind);
    ret.__set_units(unit);
    return ret;
}

void MetricGroup::print_metric_map(std::stringstream* output) {
    std::lock_guard<SpinLock> l(_lock);
    BOOST_FOREACH(const MetricMap::value_type & m, _metric_map) {
        m.second->print(output);
        (*output) << std::endl;
    }
}

void MetricGroup::print_metric_map_as_json(std::vector<std::string>* metrics) {
    std::lock_guard<SpinLock> l(_lock);
    BOOST_FOREACH(const MetricMap::value_type & m, _metric_map) {
        std::stringstream ss;
        m.second->print_json(&ss);
        metrics->push_back(ss.str());
    }
}

std::string MetricGroup::debug_string() {
    std::stringstream ss;
    WebPageHandler::ArgumentMap empty_map;
    text_callback(empty_map, &ss);
    return ss.str();
}

std::string MetricGroup::debug_string_json() {
    std::stringstream ss;
    WebPageHandler::ArgumentMap empty_map;
    json_callback(empty_map, &ss);
    return ss.str();
}

void MetricGroup::text_callback(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<pre>";
    print_metric_map(output);
    (*output) << "</pre>";
}

void MetricGroup::json_callback(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "{";
    std::vector<std::string> metrics;
    print_metric_map_as_json(&metrics);
    (*output) << boost::join(metrics, ",\n");
    (*output) << "}";
}

template<> void print_primitive_as_json<std::string>(const std::string& v,
                                                     std::stringstream* out) {
    (*out) << "\"" << v << "\"";
}

}
