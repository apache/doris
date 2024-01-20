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

#include <memory>
#include <string>
#include <unordered_map>
#include <map>

namespace doris {

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
// const char* unit_name(BvarMetricUnit unit);

using Labels = std::unordered_map<std::string, std::string>;

class BvarMetric {
public:
    BvarMetric() = default;
    virtual ~BvarMetric() = default;
    BvarMetric(BvarMetric&) = default;
    BvarMetric(BvarMetricType type, BvarMetricUnit unit, std::string name,
                std::string description = "", std::string group_name = "", 
                Labels labels = Labels(), bool is_core_metric = false)
            : is_core_metric_(is_core_metric),
              type_(type),
              unit_(unit),
              group_name_(group_name),
              name_(name),
              description_(description),
              labels_(labels) {}
    virtual std::string to_prometheus(const std::string& registry_name) const = 0;
    // std::string to_json(bool with_tablet_metrics = false) const;
    // std::string to_core_string() const;
protected:
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
        // addr::expose_as
        adder_ = std::make_shared<bvar::Adder<T>>(group_name, name + '_' + description);
    }
    ~BvarAdderMetric() override = default;
    
    T get_value();
    void increment(T value);
    void set_value(T value);
    
    std::string to_prometheus(const std::string& registry_name) const override;
    std::string label_string() const;
    std::string value_string() const;

private:
    std::shared_ptr<bvar::Adder<T>> adder_;
};


class BvarMetricEntity {
public:
    BvarMetricEntity() = default;
    BvarMetricEntity(std::string entity_name, BvarMetricType type)
            : entity_name_(entity_name), type_(type) {}
    BvarMetricEntity(const BvarMetricEntity& entity)
            : entity_name_(entity.entity_name_), type_(entity.type_), metrics_(entity.metrics_) {}
    
    template <typename T>
    void register_metric(const std::string& name, T metric);

    void deregister_metric(const std::string& name);

    std::string to_prometheus(const std::string& registry_name);    
    
    std::shared_ptr<BvarMetric> get_metric(const std::string& name);

    // Register a hook, this hook will called before get_metric is called
    void register_hook(const std::string& name, const std::function<void()>& hook);
    void deregister_hook(const std::string& name);
    void trigger_hook_unlocked(bool force) const;

private:
    std::string entity_name_;
    
    BvarMetricType type_;
    
    std::unordered_map<std::string, std::shared_ptr<BvarMetric>> metrics_;

    std::map<std::string, std::function<void()>> hooks_;
    
    bthread::Mutex mutex_;
};

class BvarMetricRegistry {
public:
    BvarMetricRegistry() = default;
    BvarMetricRegistry(const std::string& registry_name) : registry_name_(registry_name) {}
    BvarMetricRegistry(const BvarMetricRegistry& registry)
            : registry_name_(registry.registry_name_), entities_(registry.entities_) {}

    std::shared_ptr<BvarMetricEntity> register_entity(const std::string& name, BvarMetricType type = BvarMetricType::COUNTER);
    void deregister_entity(const std::string& name);

    std::string to_prometheus() const;
    // std::string to_json(bool with_tablet_metrics = false) const;
    // std::string to_core_string() const;

private:
    const std::string registry_name_;
    std::unordered_map<std::string, std::shared_ptr<BvarMetricEntity>> entities_;
    bthread::Mutex mutex_;
};

} // namespace doris