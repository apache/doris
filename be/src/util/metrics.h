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

#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "util/histogram.h"

namespace doris {

namespace rj = RAPIDJSON_NAMESPACE;

enum class MetricType { COUNTER, GAUGE, HISTOGRAM, SUMMARY, UNTYPED };

enum class MetricUnit {
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

std::ostream& operator<<(std::ostream& os, MetricType type);
const char* unit_name(MetricUnit unit);

using Labels = std::unordered_map<std::string, std::string>;

class Metric {
public:
    Metric() = default;
    virtual ~Metric() = default;
    virtual std::string to_string() const = 0;
    virtual std::string to_prometheus(const std::string& display_name, const Labels& entity_labels,
                                      const Labels& metric_labels) const;
    virtual rj::Value to_json_value(rj::Document::AllocatorType& allocator) const = 0;

private:
    friend class MetricRegistry;
};

// Metric that only can increment
template <typename T>
class AtomicMetric : public Metric {
public:
    AtomicMetric() : _value(T()) {}
    virtual ~AtomicMetric() = default;

    std::string to_string() const override { return std::to_string(value()); }

    T value() const { return _value.load(); }

    void increment(const T& delta) { _value.fetch_add(delta); }

    void set_value(const T& value) { _value.store(value); }

    rj::Value to_json_value(rj::Document::AllocatorType& allocator) const override {
        return rj::Value(value());
    }

protected:
    std::atomic<T> _value;
};

class HistogramMetric : public Metric {
public:
    HistogramMetric() = default;
    virtual ~HistogramMetric() = default;

    HistogramMetric(const HistogramMetric&) = delete;
    HistogramMetric& operator=(const HistogramMetric&) = delete;

    void clear();
    bool is_empty() const;
    void add(const uint64_t& value);
    void merge(const HistogramMetric& other);
    void set_histogram(const HistogramStat& stats);

    uint64_t min() const { return _stats.min(); }
    uint64_t max() const { return _stats.max(); }
    uint64_t num() const { return _stats.num(); }
    uint64_t sum() const { return _stats.sum(); }
    double median() const;
    double percentile(double p) const;
    double average() const;
    double standard_deviation() const;
    std::string to_string() const override;
    std::string to_prometheus(const std::string& display_name, const Labels& entity_labels,
                              const Labels& metric_labels) const override;
    rj::Value to_json_value(rj::Document::AllocatorType& allocator) const override;

protected:
    static std::map<std::string, double> _s_output_percentiles;
    mutable std::mutex _lock;
    HistogramStat _stats;
};

template <typename T>
class AtomicCounter : public AtomicMetric<T> {
public:
    AtomicCounter() = default;
    virtual ~AtomicCounter() = default;
};

template <typename T>
class AtomicGauge : public AtomicMetric<T> {
public:
    AtomicGauge() : AtomicMetric<T>() {}
    virtual ~AtomicGauge() = default;
};

using IntCounter = AtomicCounter<int64_t>;
using UIntCounter = AtomicCounter<uint64_t>;
using DoubleCounter = AtomicCounter<double>;
using IntGauge = AtomicGauge<int64_t>;
using UIntGauge = AtomicGauge<uint64_t>;
using DoubleGauge = AtomicGauge<double>;
using Labels = std::unordered_map<std::string, std::string>;

struct MetricPrototype {
public:
    MetricPrototype(MetricType type_, MetricUnit unit_, std::string name_,
                    std::string description_ = "", std::string group_name_ = "",
                    Labels labels_ = Labels(), bool is_core_metric_ = false)
            : is_core_metric(is_core_metric_),
              type(type_),
              unit(unit_),
              name(std::move(name_)),
              description(std::move(description_)),
              group_name(std::move(group_name_)),
              labels(std::move(labels_)) {}

    std::string simple_name() const;
    std::string combine_name(const std::string& registry_name) const;
    std::string to_prometheus(const std::string& registry_name) const;

    bool is_core_metric;
    MetricType type;
    MetricUnit unit;
    std::string name;
    std::string description;
    std::string group_name;
    Labels labels;
};

#define DEFINE_METRIC_PROTOTYPE(name, type, unit, desc, group, labels, core) \
    ::doris::MetricPrototype METRIC_##name(type, unit, #name, desc, group, labels, core)

#define DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(name, unit) \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::COUNTER, unit, "", "", Labels(), false)

#define DEFINE_COUNTER_METRIC_PROTOTYPE_3ARG(name, unit, desc) \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::COUNTER, unit, desc, "", Labels(), false)

#define DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(name, unit, desc, group, labels) \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::COUNTER, unit, desc, #group, labels, false)

#define DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(name, unit) \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::GAUGE, unit, "", "", Labels(), false)

#define DEFINE_GAUGE_CORE_METRIC_PROTOTYPE_2ARG(name, unit) \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::GAUGE, unit, "", "", Labels(), true)

#define DEFINE_GAUGE_METRIC_PROTOTYPE_3ARG(name, unit, desc) \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::GAUGE, unit, desc, "", Labels(), false)

#define DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(name, unit, desc, group, labels) \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::GAUGE, unit, desc, #group, labels, false)

#define DEFINE_HISTOGRAM_METRIC_PROTOTYPE_2ARG(name, unit) \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::HISTOGRAM, unit, "", "", Labels(), false)

#define INT_COUNTER_METRIC_REGISTER(entity, metric) \
    metric = (IntCounter*)(entity->register_metric<IntCounter>(&METRIC_##metric))

#define INT_GAUGE_METRIC_REGISTER(entity, metric) \
    metric = (IntGauge*)(entity->register_metric<IntGauge>(&METRIC_##metric))

#define DOUBLE_GAUGE_METRIC_REGISTER(entity, metric) \
    metric = (DoubleGauge*)(entity->register_metric<DoubleGauge>(&METRIC_##metric))

#define INT_UGAUGE_METRIC_REGISTER(entity, metric) \
    metric = (UIntGauge*)(entity->register_metric<UIntGauge>(&METRIC_##metric))

#define HISTOGRAM_METRIC_REGISTER(entity, metric) \
    metric = (HistogramMetric*)(entity->register_metric<HistogramMetric>(&METRIC_##metric))

#define METRIC_DEREGISTER(entity, metric) entity->deregister_metric(&METRIC_##metric)

// For 'metrics' in MetricEntity.
struct MetricPrototypeHash {
    size_t operator()(const MetricPrototype* metric_prototype) const {
        return std::hash<std::string>()(metric_prototype->group_name.empty()
                                                ? metric_prototype->name
                                                : metric_prototype->group_name);
    }
};

struct MetricPrototypeEqualTo {
    bool operator()(const MetricPrototype* first, const MetricPrototype* second) const {
        return first->group_name == second->group_name && first->name == second->name;
    }
};

using MetricMap = std::unordered_map<const MetricPrototype*, Metric*, MetricPrototypeHash,
                                     MetricPrototypeEqualTo>;

enum class MetricEntityType { kServer, kTablet };

class MetricEntity {
public:
    MetricEntity(MetricEntityType type, std::string name, Labels labels)
            : _type(type), _name(std::move(name)), _labels(std::move(labels)) {}
    ~MetricEntity() {
        for (auto& metric : _metrics) {
            delete metric.second;
        }
    }

    const std::string& name() const { return _name; }

    template <typename T>
    Metric* register_metric(const MetricPrototype* metric_type) {
        std::lock_guard<std::mutex> l(_lock);
        auto inserted_metric = _metrics.insert(std::make_pair(metric_type, nullptr));
        if (inserted_metric.second) {
            // If not exist, make a new metric pointer
            inserted_metric.first->second = new T();
        }
        return inserted_metric.first->second;
    }

    void deregister_metric(const MetricPrototype* metric_type);
    Metric* get_metric(const std::string& name, const std::string& group_name = "") const;

    // Register a hook, this hook will called before get_metric is called
    void register_hook(const std::string& name, const std::function<void()>& hook);
    void deregister_hook(const std::string& name);
    void trigger_hook_unlocked(bool force) const;

private:
    friend class MetricRegistry;
    friend struct MetricEntityHash;
    friend struct MetricEntityEqualTo;

    MetricEntityType _type;
    std::string _name;
    Labels _labels;

    mutable std::mutex _lock;
    MetricMap _metrics;
    std::map<std::string, std::function<void()>> _hooks;
};

struct MetricEntityHash {
    size_t operator()(const std::shared_ptr<MetricEntity> metric_entity) const {
        return std::hash<std::string>()(metric_entity->name());
    }
};

struct MetricEntityEqualTo {
    bool operator()(const std::shared_ptr<MetricEntity> first,
                    const std::shared_ptr<MetricEntity> second) const {
        return first->_type == second->_type && first->_name == second->_name &&
               first->_labels == second->_labels;
    }
};

using EntityMetricsByType =
        std::unordered_map<const MetricPrototype*, std::vector<std::pair<MetricEntity*, Metric*>>,
                           MetricPrototypeHash, MetricPrototypeEqualTo>;

class MetricRegistry {
public:
    MetricRegistry(std::string name) : _name(std::move(name)) {}
    ~MetricRegistry();

    std::shared_ptr<MetricEntity> register_entity(
            const std::string& name, const Labels& labels = {},
            MetricEntityType type = MetricEntityType::kServer);
    void deregister_entity(const std::shared_ptr<MetricEntity>& entity);
    std::shared_ptr<MetricEntity> get_entity(const std::string& name, const Labels& labels = {},
                                             MetricEntityType type = MetricEntityType::kServer);

    void trigger_all_hooks(bool force) const;

    std::string to_prometheus(bool with_tablet_metrics = false) const;
    std::string to_json(bool with_tablet_metrics = false) const;
    std::string to_core_string() const;

private:
    const std::string _name;

    mutable std::mutex _lock;
    // MetricEntity -> register count
    std::unordered_map<std::shared_ptr<MetricEntity>, int32_t, MetricEntityHash,
                       MetricEntityEqualTo>
            _entities;
};

} // namespace doris
