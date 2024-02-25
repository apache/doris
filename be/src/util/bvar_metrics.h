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
std::string labels_string(
        std::initializer_list<const std::unordered_map<std::string, std::string>*> multi_labels);

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

    std::string simple_name() const;
    std::string combine_name(const std::string& registry_name) const;
    virtual std::string to_prometheus(const std::string& registry_name,
                                      const Labels& entity_labels) const = 0;
    virtual rj::Value to_json_value(rj::Document::AllocatorType& allocator) const = 0;
    virtual std::string value_string() const = 0;

protected:
    friend class BvarMetricRegistry;
    friend struct BvarMetircHash;
    friend struct BvarMetricEqualTo;

    bool is_core_metric_;

    BvarMetricType type_;
    BvarMetricUnit unit_;

    // use for expose
    std::string group_name_; // prefix
    std::string name_;
    std::string description_;

    Labels labels_;
};

// For 'bvar_metrics' in BvarMetricEntity.
struct BvarMetircHash {
    size_t operator()(const BvarMetric* metric) const {
        return std::hash<std::string>()(metric->group_name_.empty() ? metric->name_
                                                                    : metric->group_name_);
    }
};

struct BvarMetricEqualTo {
    bool operator()(const BvarMetric* first, const BvarMetric* second) const {
        return first->group_name_ == second->group_name_ && first->name_ == second->name_;
    }
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

    std::string to_prometheus(const std::string& registry_name,
                              const Labels& entity_labels) const override;
    rj::Value to_json_value(rj::Document::AllocatorType& allocator) const override {
        return rj::Value(get_value());
    }
    std::string value_string() const override;

private:
    std::shared_ptr<bvar::Adder<T>> adder_;
};

enum class BvarMetricEntityType { kServer, kTablet };

class BvarMetricEntity {
public:
    using Labels = std::unordered_map<std::string, std::string>;
    BvarMetricEntity() = default;
    BvarMetricEntity(std::string entity_name, BvarMetricEntityType type, const Labels& labels)
            : name_(entity_name), type_(type), lables_(labels) {}
    BvarMetricEntity(const BvarMetricEntity& entity)
            : name_(entity.name_),
              type_(entity.type_),
              lables_(entity.lables_),
              metrics_(entity.metrics_) {}

    template <typename T>
    void register_metric(const std::string& name, T metric);

    void deregister_metric(const std::string& name);

    std::shared_ptr<BvarMetric> get_metric(const std::string& name);

    // Register a hook, this hook will called before get_metric is called
    void register_hook(const std::string& name, const std::function<void()>& hook);
    void deregister_hook(const std::string& name);
    void trigger_hook_unlocked(bool force) const;

private:
    friend class BvarMetricRegistry;
    friend struct BvarMetricEntityHash;
    friend struct BvarMetricEntityEqualTo;

    std::string name_;

    BvarMetricEntityType type_;

    Labels lables_;

    std::unordered_map<std::string, std::shared_ptr<BvarMetric>> metrics_;

    std::map<std::string, std::function<void()>> hooks_;

    bthread::Mutex mutex_;
};

struct BvarMetricEntityHash {
    size_t operator()(const std::shared_ptr<BvarMetricEntity> metric_entity) const {
        return std::hash<std::string>()(metric_entity->name_);
    }
};

struct BvarMetricEntityEqualTo {
    bool operator()(const std::shared_ptr<BvarMetricEntity> first,
                    const std::shared_ptr<BvarMetricEntity> second) const {
        return first->type_ == second->type_ && first->name_ == second->name_ &&
               first->lables_ == second->lables_;
    }
};

using BvarEntityMetricsByType =
        std::unordered_map<const BvarMetric*,
                           std::vector<std::pair<BvarMetricEntity*, BvarMetric*>>, BvarMetircHash,
                           BvarMetricEqualTo>;

class BvarMetricRegistry {
public:
    using Labels = std::unordered_map<std::string, std::string>;
    BvarMetricRegistry(const std::string name) : name_(name) {}
    ~BvarMetricRegistry() {}

    std::shared_ptr<BvarMetricEntity> register_entity(
            const std::string& name, const Labels& labels = {},
            BvarMetricEntityType type = BvarMetricEntityType::kServer);
    void deregister_entity(const std::shared_ptr<BvarMetricEntity>& entity);

    void trigger_all_hooks(bool force);

    const std::string to_prometheus(bool with_tablet_metrics);
    const std::string to_json(bool with_tablet_metrics);
    const std::string to_core_string();

private:
    const std::string name_;

    // BvarMetricEntity -> register count
    std::unordered_map<std::shared_ptr<BvarMetricEntity>, int32_t, BvarMetricEntityHash,
                       BvarMetricEntityEqualTo>
            entities_;

    bthread::Mutex mutex_;
};

} // namespace doris