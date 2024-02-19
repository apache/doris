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

#pragma once

#include <bthread/mutex.h>
#include <bvar/latency_recorder.h>
#include <bvar/reducer.h>
#include <bvar/status.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "util/core_local.h"

namespace doris {

namespace rj = RAPIDJSON_NAMESPACE;
enum class BvarMetricType { COUNTER, GAUGE, HISTOGRAM, SUMMARY, UNTYPED };
enum class BvarMetricUnit {
    NANOSECONDS,
    MICROSECONDS,
    MILLISECONDS,
    SECONDS,
    BYTES,
    ROWS,
    PERCENT,
    REQUESTS,
    OPERATIONS,
    BLOCKS,
    ROWSETS,
    CONNECTIONS,
    PACKETS,
    NOUNIT,
    FILESYSTEM
};

std::ostream& operator<<(std::ostream& os, BvarMetricType type);
const char* unit_name(BvarMetricUnit unit);


class BvarMetric {
public:
    using Labels = std::unordered_map<std::string, std::string>;
    BvarMetric() = default;
    virtual ~BvarMetric() = default;
    BvarMetric(BvarMetric&) = default;
    BvarMetric(BvarMetricType type, BvarMetricUnit unit, std::string name,
               std::string description = "", std::string group_name = "", Labels labels = Labels(),
               bool is_core_metric = false)
            : is_core_metric_(is_core_metric),
              type_(type),
              unit_(unit),
              group_name_(group_name),
              name_(name),
              description_(description),
              labels_(labels) {}
    virtual const std::string to_prometheus(const std::string& registry_name) = 0;
    virtual std::string to_core_string(const std::string& reigstry_name) const = 0;
    virtual rj::Value to_json_value(rj::Document::AllocatorType& allocator) const = 0;
    bool is_core() { return is_core_metric_; }

protected:
    friend class DorisBvarMetrics;
    friend class SystemBvarMetrics;
    bool is_core_metric_;

    BvarMetricType type_;
    BvarMetricUnit unit_;

    // use for expose
    std::string group_name_; // prefix
    std::string name_;
    std::string description_;

    Labels labels_;
};

// bvar::Adder which support the operation of commutative and associative laws
template <typename T>
class BvarAdderMetric : public BvarMetric {
public:
    BvarAdderMetric(BvarMetricType type, BvarMetricUnit unit, std::string name,
                    std::string description = "", std::string group_name = "",
                    Labels labels = Labels(), bool is_core_metric = false)
            : BvarMetric(type, unit, name, description, group_name, labels, is_core_metric) {
        //construct without expose information, just use bvar
        adder_ = std::make_shared<bvar::Adder<T>>();
    }
    ~BvarAdderMetric() override = default;

    T get_value() const;
    void increment(T value);
    void set_value(T value);
    void reset() { adder_->reset(); }

    const std::string to_prometheus(const std::string& registry_name) override;
    std::string to_core_string(const std::string& reigstry_name) const override;
    rj::Value to_json_value(rj::Document::AllocatorType& allocator) const override {
        return rj::Value(get_value());
    }
    std::string label_string() const;
    std::string value_string() const;

private:
    std::shared_ptr<bvar::Adder<T>> adder_;
};

enum class BvarMetricEntityType { kServer, kTablet };

class BvarMetricEntity {
public:
    BvarMetricEntity() = default;
    BvarMetricEntity(std::string entity_name, BvarMetricType type)
            : entity_name_(entity_name), metrics_type_(type) {}
    BvarMetricEntity(const BvarMetricEntity& entity)
            : entity_name_(entity.entity_name_), metrics_type_(entity.metrics_type_), metrics_(entity.metrics_) {}

    template <typename T>
    void register_metric(const std::string& name, T metric);

    void deregister_metric(const std::string& name);

    const std::string to_prometheus(const std::string& registry_name);
    const std::string to_core_string(const std::string& registry_name);
    // std::shared_ptr<BvarMetric> get_metric(const std::string& name);

    // Register a hook, this hook will called before get_metric is called
    void register_hook(const std::string& name, const std::function<void()>& hook);
    void deregister_hook(const std::string& name);
    void trigger_hook_unlocked(bool force) const;

private:
    friend class DorisBvarMetrics;
    friend class SystemBvarMetrics;
    
    std::string entity_name_;

    BvarMetricType metrics_type_;

    BvarMetricEntityType entity_type_ = BvarMetricEntityType::kServer;

    std::unordered_map<std::string, std::shared_ptr<BvarMetric>> metrics_;

    std::map<std::string, std::function<void()>> hooks_;
    
    bthread::Mutex mutex_;
};

} // namespace doris