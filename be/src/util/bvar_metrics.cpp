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

#include "util/bvar_metrics.h"

namespace doris {

std::ostream& operator<<(std::ostream& os, BvarMetricType type) {
    switch (type) {
    case BvarMetricType::COUNTER:
        os << "counter";
        break;
    case BvarMetricType::GAUGE:
        os << "gauge";
        break;
    case BvarMetricType::HISTOGRAM:
        os << "histogram";
        break;
    case BvarMetricType::SUMMARY:
        os << "summary";
        break;
    case BvarMetricType::UNTYPED:
        os << "untyped";
        break;
    default:
        os << "unknown";
        break;
    }
    return os;
}

const char* unit_name(BvarMetricUnit unit) {
    switch (unit) {
    case BvarMetricUnit::NANOSECONDS:
        return "nanoseconds";
    case BvarMetricUnit::MICROSECONDS:
        return "microseconds";
    case BvarMetricUnit::MILLISECONDS:
        return "milliseconds";
    case BvarMetricUnit::SECONDS:
        return "seconds";
    case BvarMetricUnit::BYTES:
        return "bytes";
    case BvarMetricUnit::ROWS:
        return "rows";
    case BvarMetricUnit::PERCENT:
        return "percent";
    case BvarMetricUnit::REQUESTS:
        return "requests";
    case BvarMetricUnit::OPERATIONS:
        return "operations";
    case BvarMetricUnit::BLOCKS:
        return "blocks";
    case BvarMetricUnit::ROWSETS:
        return "rowsets";
    case BvarMetricUnit::CONNECTIONS:
        return "rowsets";
    default:
        return "nounit";
    }
}

std::string BvarMetric::simple_name() const {
    return group_name_.empty() ? name_ : group_name_;
}

std::string BvarMetric::combine_name(const std::string& registry_name) const {
    return (registry_name.empty() ? std::string() : registry_name + "_") + simple_name();
}

template <typename T>
T BvarAdderMetric<T>::get_value() const {
    return adder_->get_value();
}

template <typename T>
void BvarAdderMetric<T>::increment(T value) {
    (*adder_) << value;
}

template <typename T>
void BvarAdderMetric<T>::set_value(T value) {
    adder_->reset();
    (*adder_) << value;
}

std::string labels_string(
        std::initializer_list<const std::unordered_map<std::string, std::string>*> multi_labels) {
    bool all_empty = true;
    for (const auto& labels : multi_labels) {
        if (!labels->empty()) {
            all_empty = false;
            break;
        }
    }
    if (all_empty) {
        return std::string();
    }

    std::stringstream ss;
    ss << "{";
    int i = 0;
    for (auto labels : multi_labels) {
        for (const auto& label : *labels) {
            if (i++ > 0) {
                ss << ",";
            }
            ss << label.first << "=\"" << label.second << "\"";
        }
    }
    ss << "}";

    return ss.str();
}

template <typename T>
std::string BvarAdderMetric<T>::to_prometheus(const std::string& registry_name,
                                              const Labels& entity_labels) const {
    std::stringstream ss;
    ss << combine_name(registry_name)                      // metrics name
       << " " << labels_string({&entity_labels, &labels_}) // metrics lables
       << " " << value_string() << "\n";                   // metrics value
    return ss.str();
}

template <typename T>
std::string BvarAdderMetric<T>::value_string() const {
    return std::to_string(adder_->get_value());
}

template <typename T>
void BvarMetricEntity::register_metric(const std::string& name, T metric) {
    {
        std::lock_guard<bthread::Mutex> l(mutex_);
        auto it = metrics_.find(name);
        if (it == metrics_.end()) {
            metrics_[name] = std::make_shared<T>(metric);
        }
    }
}

void BvarMetricEntity::deregister_metric(const std::string& name) {
    {
        std::lock_guard<bthread::Mutex> l(mutex_);
        auto it = metrics_.find(name);
        if (it != metrics_.end()) {
            metrics_.erase(it);
        }
    }
}

std::shared_ptr<BvarMetric> BvarMetricEntity::get_metric(const std::string& name) {
    {
        std::lock_guard<bthread::Mutex> l(mutex_);
        auto it = metrics_.find(name);
        if (it == metrics_.end()) {
            return nullptr;
        }
        return it->second;
    }
}

void BvarMetricEntity::register_hook(const std::string& name, const std::function<void()>& hook) {
    std::lock_guard<bthread::Mutex> l(mutex_);
#ifndef BE_TEST
    DCHECK(hooks_.find(name) == hooks_.end()) << "hook is already exist! " << name_ << ":" << name;
#endif
    hooks_.emplace(name, hook);
}

void BvarMetricEntity::deregister_hook(const std::string& name) {
    std::lock_guard<bthread::Mutex> l(mutex_);
    hooks_.erase(name);
}

void BvarMetricEntity::trigger_hook_unlocked(bool force) const {
    // When 'enable_metric_calculator' is true, hooks will be triggered by a background thread,
    // see 'calculate_metrics' in daemon.cpp for more details.
    if (!force && config::enable_metric_calculator) {
        return;
    }
    for (const auto& hook : hooks_) {
        hook.second();
    }
}

std::shared_ptr<BvarMetricEntity> BvarMetricRegistry::register_entity(const std::string& name,
                                                                      const Labels& labels,
                                                                      BvarMetricEntityType type) {
    std::shared_ptr<BvarMetricEntity> entity =
            std::make_shared<BvarMetricEntity>(name, type, labels);
    std::lock_guard<bthread::Mutex> l(mutex_);
    auto inserted_entity = entities_.insert(std::make_pair(entity, 1));
    if (!inserted_entity.second) {
        // If exist, increase the registered count
        inserted_entity.first->second++;
    }
    return inserted_entity.first->first;
}

void BvarMetricRegistry::deregister_entity(const std::shared_ptr<BvarMetricEntity>& entity) {
    std::lock_guard<bthread::Mutex> l(mutex_);
    auto found_entity = entities_.find(entity);
    if (found_entity != entities_.end()) {
        // Decrease the registered count
        --found_entity->second;
        if (found_entity->second == 0) {
            // Only erase it when registered count is zero
            entities_.erase(found_entity);
        }
    }
}

void BvarMetricRegistry::trigger_all_hooks(bool force) {
    std::lock_guard<bthread::Mutex> l(mutex_);
    for (const auto& entity : entities_) {
        std::lock_guard<bthread::Mutex> l(entity.first->mutex_);
        entity.first->trigger_hook_unlocked(force);
    }
}

const std::string BvarMetricRegistry::to_prometheus(bool with_tablet_metrics) {
    BvarEntityMetricsByType entity_metrics_by_types;
    std::lock_guard<bthread::Mutex> l(mutex_);

    for (auto& entity : entities_) {
        if (entity.first->type_ == BvarMetricEntityType::kTablet && !with_tablet_metrics) {
            continue;
        }
        std::lock_guard<bthread::Mutex> l(entity.first->mutex_);
        entity.first->trigger_hook_unlocked(false);
        for (const auto& metric : entity.first->metrics_) {
            std::pair<BvarMetricEntity*, BvarMetric*> new_elem =
                    std::make_pair(entity.first.get(), metric.second.get());
            auto found = entity_metrics_by_types.find(metric.second.get());
            if (found == entity_metrics_by_types.end()) {
                entity_metrics_by_types.emplace(
                        metric.second.get(),
                        std::vector<std::pair<BvarMetricEntity*, BvarMetric*>>({new_elem}));
            } else {
                found->second.emplace_back(new_elem);
            }
        }
    }

    // Output
    std::stringstream ss;
    std::string last_group_name;
    for (const auto& entity_metrics_by_type : entity_metrics_by_types) {
        const auto metric = entity_metrics_by_type.first;
        std::string display_name = metric->combine_name(name_);

        if (last_group_name.empty() || last_group_name != metric->group_name_) {
            ss << "# TYPE " << display_name << " " << metric->type_ << "\n"; // metric TYPE line
        }

        last_group_name = metric->group_name_;

        for (const auto& entity_metric : entity_metrics_by_type.second) {
            ss << entity_metric.second->to_prometheus(name_, // metric key-value line
                                                      entity_metric.first->lables_);
        }
    }

    return ss.str();
}

const std::string BvarMetricRegistry::to_json(bool with_tablet_metrics) {
    rj::Document doc {rj::kArrayType};
    rj::Document::AllocatorType& allocator = doc.GetAllocator();
    std::lock_guard<bthread::Mutex> l(mutex_);
    for (const auto& entity : entities_) {
        if (entity.first->type_ == BvarMetricEntityType::kTablet && !with_tablet_metrics) {
            continue;
        }
        std::lock_guard<bthread::Mutex> l(entity.first->mutex_);
        entity.first->trigger_hook_unlocked(false);
        for (const auto& metric : entity.first->metrics_) {
            rj::Value metric_obj(rj::kObjectType);
            // tags
            rj::Value tag_obj(rj::kObjectType);
            tag_obj.AddMember("metric", rj::Value(metric.second->simple_name().c_str(), allocator),
                              allocator);
            // MetricPrototype's labels
            for (auto& label : metric.second->labels_) {
                tag_obj.AddMember(rj::Value(label.first.c_str(), allocator),
                                  rj::Value(label.second.c_str(), allocator), allocator);
            }
            // MetricEntity's labels
            for (auto& label : entity.first->lables_) {
                tag_obj.AddMember(rj::Value(label.first.c_str(), allocator),
                                  rj::Value(label.second.c_str(), allocator), allocator);
            }
            metric_obj.AddMember("tags", tag_obj, allocator);
            // unit
            rj::Value unit_val(unit_name(metric.second->unit_), allocator);
            metric_obj.AddMember("unit", unit_val, allocator);
            // value
            metric_obj.AddMember("value", metric.second->to_json_value(allocator), allocator);
            doc.PushBack(metric_obj, allocator);
        }
    }

    rj::StringBuffer strBuf;
    rj::Writer<rj::StringBuffer> writer(strBuf);
    doc.Accept(writer);
    return strBuf.GetString();
}

const std::string BvarMetricRegistry::to_core_string() {
    std::stringstream ss;
    std::lock_guard<bthread::Mutex> l(mutex_);
    for (auto& entity : entities_) {
        std::lock_guard<bthread::Mutex> l(entity.first->mutex_);
        entity.first->trigger_hook_unlocked(false);
        for (const auto& metric : entity.first->metrics_) {
            if (metric.second->is_core_metric_) {
                ss << metric.second->combine_name(name_) << " LONG "
                   << metric.second->value_string() << "\n";
            }
        }
    }
    return ss.str();
}

template class BvarAdderMetric<int64_t>;
template class BvarAdderMetric<uint64_t>;
template class BvarAdderMetric<double>;
template void BvarMetricEntity::register_metric(const std::string& name,
                                                BvarAdderMetric<int64_t> metric);
template void BvarMetricEntity::register_metric(const std::string& name,
                                                BvarAdderMetric<uint64_t> metric);
template void BvarMetricEntity::register_metric(const std::string& name,
                                                BvarAdderMetric<double> metric);
// template void BvarMetricEntity::register_metric(const std::string& name, T metric)
} // namespace doris