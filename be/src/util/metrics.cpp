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

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace doris {

std::ostream& operator<<(std::ostream& os, MetricType type) {
    switch (type) {
    case MetricType::COUNTER:
        os << "counter";
        break;
    case MetricType::GAUGE:
        os << "gauge";
        break;
    case MetricType::HISTOGRAM:
        os << "histogram";
        break;
    case MetricType::SUMMARY:
        os << "summary";
        break;
    case MetricType::UNTYPED:
        os << "untyped";
        break;
    default:
        os << "unknown";
        break;
    }
    return os;
}

const char* unit_name(MetricUnit unit) {
    switch (unit) {
    case MetricUnit::NANOSECONDS:
        return "nanoseconds";
    case MetricUnit::MICROSECONDS:
        return "microseconds";
    case MetricUnit::MILLISECONDS:
        return "milliseconds";
    case MetricUnit::SECONDS:
        return "seconds";
    case MetricUnit::BYTES:
        return "bytes";
    case MetricUnit::ROWS:
        return "rows";
    case MetricUnit::PERCENT:
        return "percent";
    case MetricUnit::REQUESTS:
        return "requests";
    case MetricUnit::OPERATIONS:
        return "operations";
    case MetricUnit::BLOCKS:
        return "blocks";
    case MetricUnit::ROWSETS:
        return "rowsets";
    case MetricUnit::CONNECTIONS:
        return "rowsets";
    default:
        return "nounit";
    }
}

std::string labels_to_string(const Labels& entity_labels, const Labels& metric_labels) {
    if (entity_labels.empty() && metric_labels.empty()) {
        return std::string();
    }

    std::stringstream ss;
    ss << "{";
    int i = 0;
    for (const auto& label : entity_labels) {
        if (i++ > 0) {
            ss << ",";
        }
        ss << label.first << "=\"" << label.second << "\"";
    }
    for (const auto& label : metric_labels) {
        if (i++ > 0) {
            ss << ",";
        }
        ss << label.first << "=\"" << label.second << "\"";
    }
    ss << "}";

    return ss.str();
}

std::string MetricPrototype::simple_name() const {
    return group_name.empty() ? name : group_name;
}

std::string MetricPrototype::combine_name(const std::string& registry_name) const {
    return (registry_name.empty() ? std::string() : registry_name  + "_") + simple_name();
}

void MetricEntity::register_metric(const MetricPrototype* metric_type, Metric* metric) {
    std::lock_guard<SpinLock> l(_lock);
    DCHECK(_metrics.find(metric_type) == _metrics.end()) << "metric is already exist! " << _name << ":" << metric_type->name;
    _metrics.emplace(metric_type, metric);
}

void MetricEntity::deregister_metric(const MetricPrototype* metric_type) {
    std::lock_guard<SpinLock> l(_lock);
    _metrics.erase(metric_type);
}

Metric* MetricEntity::get_metric(const std::string& name, const std::string& group_name) const {
    MetricPrototype dummy(MetricType::UNTYPED, MetricUnit::NOUNIT, name, "", group_name);
    std::lock_guard<SpinLock> l(_lock);
    auto it = _metrics.find(&dummy);
    if (it == _metrics.end()) {
        return nullptr;
    }
    return it->second;
}

void MetricEntity::register_hook(const std::string& name, const std::function<void()>& hook) {
    std::lock_guard<SpinLock> l(_lock);
    DCHECK(_hooks.find(name) == _hooks.end()) << "hook is already exist! " << _name << ":" << name;
    _hooks.emplace(name, hook);
}

void MetricEntity::deregister_hook(const std::string& name) {
    std::lock_guard<SpinLock> l(_lock);
    _hooks.erase(name);
}

void MetricEntity::trigger_hook_unlocked(bool force) const {
    // When 'enable_metric_calculator' is true, hooks will be triggered by a background thread,
    // see 'calculate_metrics' in daemon.cpp for more details.
    if (!force && config::enable_metric_calculator) {
        return;
    }
    for (const auto& hook : _hooks) {
        hook.second();
    }
}

MetricRegistry::~MetricRegistry() {
}

MetricEntity* MetricRegistry::register_entity(const std::string& name, const Labels& labels, MetricEntityType type) {
    std::shared_ptr<MetricEntity> entity = std::make_shared<MetricEntity>(type, name, labels);

    std::lock_guard<SpinLock> l(_lock);
    DCHECK(_entities.find(name) == _entities.end()) << name;
    _entities.insert(std::make_pair(name, entity));
    return entity.get();
}

void MetricRegistry::deregister_entity(const std::string& name) {
    std::lock_guard<SpinLock> l(_lock);
    _entities.erase(name);
}

std::shared_ptr<MetricEntity> MetricRegistry::get_entity(const std::string& name) {
    std::lock_guard<SpinLock> l(_lock);
    auto entity = _entities.find(name);
    if (entity == _entities.end()) {
        return std::shared_ptr<MetricEntity>();
    }
    return entity->second;
}

void MetricRegistry::trigger_all_hooks(bool force) const {
    std::lock_guard<SpinLock> l(_lock);
    for (const auto& entity : _entities) {
        std::lock_guard<SpinLock> l(entity.second->_lock);
        entity.second->trigger_hook_unlocked(force);
    }
}

std::string MetricRegistry::to_prometheus(bool with_tablet_metrics) const {
    std::stringstream ss;
    // Reorder by MetricPrototype
    EntityMetricsByType entity_metrics_by_types;
    std::lock_guard<SpinLock> l(_lock);
    for (const auto& entity : _entities) {
        if (entity.second->_type == MetricEntityType::kTablet && !with_tablet_metrics) {
            continue;
        }
        std::lock_guard<SpinLock> l(entity.second->_lock);
        entity.second->trigger_hook_unlocked(false);
        for (const auto& metric : entity.second->_metrics) {
            std::pair<MetricEntity*, Metric*> new_elem = std::make_pair(entity.second.get(), metric.second);
            auto found = entity_metrics_by_types.find(metric.first);
            if (found == entity_metrics_by_types.end()) {
                entity_metrics_by_types.emplace(metric.first, std::vector<std::pair<MetricEntity*, Metric*>>({new_elem}));
            } else {
                found->second.emplace_back(new_elem);
            }
        }
    }
    // Output
    std::string last_group_name;
    for (const auto& entity_metrics_by_type : entity_metrics_by_types) {
        if (last_group_name.empty() || last_group_name != entity_metrics_by_type.first->group_name) {
            ss << "# TYPE " << entity_metrics_by_type.first->combine_name(_name) << " "
               << entity_metrics_by_type.first->type << "\n"; // metric TYPE line
        }
        last_group_name = entity_metrics_by_type.first->group_name;
        std::string display_name = entity_metrics_by_type.first->combine_name(_name);
        for (const auto& entity_metric : entity_metrics_by_type.second) {
            ss << display_name                                                                           // metric name
               << labels_to_string(entity_metric.first->_labels, entity_metrics_by_type.first->labels)   // metric labels
               << " " << entity_metric.second->to_string() << "\n";                                      // metric value
        }
    }

    return ss.str();
}

std::string MetricRegistry::to_json(bool with_tablet_metrics) const {
    rj::Document doc{rj::kArrayType};
    rj::Document::AllocatorType& allocator = doc.GetAllocator();
    std::lock_guard<SpinLock> l(_lock);
    for (const auto& entity : _entities) {
        if (entity.second->_type == MetricEntityType::kTablet && !with_tablet_metrics) {
            continue;
        }
        std::lock_guard<SpinLock> l(entity.second->_lock);
        entity.second->trigger_hook_unlocked(false);
        for (const auto& metric : entity.second->_metrics) {
            rj::Value metric_obj(rj::kObjectType);
            // tags
            rj::Value tag_obj(rj::kObjectType);
            tag_obj.AddMember("metric", rj::Value(metric.first->simple_name().c_str(), allocator), allocator);
            // MetricPrototype's labels
            for (auto& label : metric.first->labels) {
                tag_obj.AddMember(
                        rj::Value(label.first.c_str(), allocator),
                        rj::Value(label.second.c_str(), allocator),
                        allocator);
            }
            // MetricEntity's labels
            for (auto& label : entity.second->_labels) {
                tag_obj.AddMember(
                        rj::Value(label.first.c_str(), allocator),
                        rj::Value(label.second.c_str(), allocator),
                        allocator);
            }
            metric_obj.AddMember("tags", tag_obj, allocator);
            // unit
            rj::Value unit_val(unit_name(metric.first->unit), allocator);
            metric_obj.AddMember("unit", unit_val, allocator);
            // value
            metric_obj.AddMember("value", metric.second->to_json_value(), allocator);
            doc.PushBack(metric_obj, allocator);
        }
    }

    rj::StringBuffer strBuf;
    rj::Writer<rj::StringBuffer> writer(strBuf);
    doc.Accept(writer);
    return strBuf.GetString();
}

std::string MetricRegistry::to_core_string() const {
    std::stringstream ss;
    std::lock_guard<SpinLock> l(_lock);
    for (const auto& entity : _entities) {
        std::lock_guard<SpinLock> l(entity.second->_lock);
        entity.second->trigger_hook_unlocked(false);
        for (const auto &metric : entity.second->_metrics) {
            if (metric.first->is_core_metric) {
                ss << metric.first->combine_name(_name) << " LONG " << metric.second->to_string() << "\n";
            }
        }
    }

    return ss.str();
}

}
