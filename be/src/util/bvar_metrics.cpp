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

template <typename T>
T BvarAdderMetric<T>::get_value() {
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

template <typename T>
const std::string BvarAdderMetric<T>::to_prometheus(const std::string& registry_name) {
    return registry_name + "_" + name_ + label_string() + " " + value_string() + "\n";
}

template <typename T>
std::string BvarAdderMetric<T>::to_core_string(const std::string& registry_name) const {
    return registry_name + "_" + name_ + " " + "LONG " + value_string() + "\n";
}

template <typename T>
std::string BvarAdderMetric<T>::label_string() const {
    if (labels_.empty()) {
        return "";
    }

    std::stringstream ss;
    ss << "{";
    int i = 0;
    for (auto label : labels_) {
        if (i++ > 0) {
            ss << ",";
        }
        ss << label.first << "=\"" << label.second << "\"";
    }
    ss << "}";
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

// void BvarMetricEntity::deregister_metric(const std::string& name) {
//     {
//         std::lock_guard<bthread::Mutex> l(mutex_);
//         auto it = metrics_.find(name);
//         if (it != metrics_.end()) {
//             metrics_.erase(it);
//         }
//     }
// }

// std::shared_ptr<BvarMetric> BvarMetricEntity::get_metric(const std::string& name) {
//     {
//         std::lock_guard<bthread::Mutex> l(mutex_);
//         auto it = metrics_.find(name);
//         if (it == metrics_.end()) {
//             return nullptr;
//         }
//         return it->second;
//     }
// }

const std::string BvarMetricEntity::to_prometheus(const std::string& registry_name) {
    std::lock_guard<bthread::Mutex> l(mutex_);
    std::stringstream ss;
    // ss << "# TYPE " << registry_name << "_" << entity_name_ << " " << type_ << "\n";
    for (auto metric_pair : metrics_) {
        ss << metric_pair.second->to_prometheus(registry_name);
    }
    return ss.str();
}

const std::string BvarMetricEntity::to_core_string(const std::string& registry_name) {
    std::lock_guard<bthread::Mutex> l(mutex_);
    std::stringstream ss;
    for(auto metrics_pair : metrics_) {
        if(metrics_pair.second->is_core()) {
            ss << metrics_pair.second->to_core_string(registry_name);
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