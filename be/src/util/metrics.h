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

#include <atomic>
#include <functional>
#include <ostream>
#include <set>
#include <sstream>
#include <string>
#include <mutex>
#include <iomanip>
#include <unordered_map>

#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>

#include "common/config.h"
#include "util/spinlock.h"
#include "util/core_local.h"

namespace doris {

namespace rj = RAPIDJSON_NAMESPACE;

class MetricRegistry;

enum class MetricType {
    COUNTER,
    GAUGE,
    HISTOGRAM,
    SUMMARY,
    UNTYPED
};

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
    NOUNIT
};

std::ostream& operator<<(std::ostream& os, MetricType type);
const char* unit_name(MetricUnit unit);

class Metric {
public:
    Metric() {}
    virtual ~Metric() {}
    virtual std::string to_string() const = 0;
    virtual rj::Value to_json_value() const = 0;

private:
    friend class MetricRegistry;
};

// Metric that only can increment
template<typename T>
class AtomicMetric : public Metric {
public:
    AtomicMetric() : _value(T()) {}
    virtual ~AtomicMetric() {}

    std::string to_string() const override {
        return std::to_string(value());
    }

    T value() const {
        return _value.load();
    }

    void increment(const T& delta) {
        _value.fetch_add(delta);
    }

    void set_value(const T& value) {
        _value.store(value);
    }

    rj::Value to_json_value() const override {
        return rj::Value(value());
    }

protected:
    std::atomic<T> _value;
};

template<typename T>
class LockSimpleMetric : public Metric {
public:
    LockSimpleMetric() : _value(T()) {}
    virtual ~LockSimpleMetric() {}

    std::string to_string() const override {
        return std::to_string(value());
    }
    
    T value() const {
        std::lock_guard<SpinLock> l(_lock);
        return _value;
    }

    void increment(const T& delta) {
        std::lock_guard<SpinLock> l(this->_lock);
        _value += delta;
    }

    void set_value(const T& value) {
        std::lock_guard<SpinLock> l(this->_lock);
        _value = value;
    }

    rj::Value to_json_value() const override {
        return rj::Value(value());
    }

protected:
    // We use spinlock instead of std::atomic is because atomic don't support
    // double's fetch_add
    // TODO(zc): If this is atomic is bottleneck, we change to thread local.
    // performance: on Intel(R) Xeon(R) CPU E5-2450 int64_t
    //  original type: 2ns/op
    //  single thread spinlock: 26ns/op
    //  multiple thread(8) spinlock: 2500ns/op
    mutable SpinLock _lock;
    T _value;
};

template<typename T>
class CoreLocalCounter : public Metric {
public:
    CoreLocalCounter() {}
    virtual ~CoreLocalCounter() {}

    std::string to_string() const override {
        std::stringstream ss;
        ss << value();
        return ss.str();
    }
    
    T value() const {
        T sum = 0;
        for (int i = 0; i < _value.size(); ++i) {
            sum += *_value.access_at_core(i);
        }
        return sum;
    }

    void increment(const T& delta) {
        __sync_fetch_and_add(_value.access(), delta);
    }

    rj::Value to_json_value() const override {
        return rj::Value(value());
    }

protected:
    CoreLocalValue<T> _value;
};

template<typename T>
class AtomicCounter : public AtomicMetric<T> {
public:
    AtomicCounter() {}
    virtual ~AtomicCounter() {}
};

template<typename T>
class AtomicGauge : public AtomicMetric<T> {
public:
    AtomicGauge() : AtomicMetric<T>() {}
    virtual ~AtomicGauge() {}
};

template<typename T>
class LockCounter : public LockSimpleMetric<T> {
public:
    LockCounter() : LockSimpleMetric<T>() {}
    virtual ~LockCounter() {}
};

// This can only used for trival type
template<typename T>
class LockGauge : public LockSimpleMetric<T> {
public:
    LockGauge() : LockSimpleMetric<T>() {}
    virtual ~LockGauge() {}
};

using Labels = std::unordered_map<std::string, std::string>;
struct MetricPrototype {
public:
    MetricPrototype(MetricType type_,
                    MetricUnit unit_,
                    std::string name_,
                    std::string description_ = "",
                    std::string group_name_ = "",
                    Labels labels_ = Labels(),
                    bool is_core_metric_ = false)
        : is_core_metric(is_core_metric_),
          type(type_),
          unit(unit_),
          name(std::move(name_)),
          description(std::move(description_)),
          group_name(std::move(group_name_)),
          labels(std::move(labels_)) {}

    std::string simple_name() const;
    std::string combine_name(const std::string& registry_name) const;

    bool is_core_metric;
    MetricType type;
    MetricUnit unit;
    std::string name;
    std::string description;
    std::string group_name;
    Labels labels;
};

#define DEFINE_METRIC_PROTOTYPE(name, type, unit, desc, group, labels, core)      \
    ::doris::MetricPrototype METRIC_##name(type, unit, #name, desc, group, labels, core)

#define DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(name, unit)                          \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::COUNTER, unit, "", "", Labels(), false)

#define DEFINE_COUNTER_METRIC_PROTOTYPE_3ARG(name, unit, desc)                    \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::COUNTER, unit, desc, "", Labels(), false)

#define DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(name, unit, desc, group, labels)     \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::COUNTER, unit, desc, #group, labels, false)

#define DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(name, unit)                            \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::GAUGE, unit, "", "", Labels(), false)

#define DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(name, unit)                            \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::GAUGE, unit, "", "", Labels(), true)

#define DEFINE_GAUGE_METRIC_PROTOTYPE_3ARG(name, unit, desc)                      \
    DEFINE_METRIC_PROTOTYPE(name, MetricType::GAUGE, unit, desc, "", Labels(), false)

#define METRIC_REGISTER(entity, metric)                                           \
    entity->register_metric(&METRIC_##metric, &metric)

#define METRIC_DEREGISTER(entity, metric)                                         \
    entity->deregister_metric(&METRIC_##metric)

// For 'metrics' in MetricEntity.
struct MetricPrototypeHash {
    size_t operator()(const MetricPrototype* metric_prototype) const {
        return std::hash<std::string>()(metric_prototype->group_name.empty() ? metric_prototype->name : metric_prototype->group_name);
    }
};

struct MetricPrototypeEqualTo {
    bool operator()(const MetricPrototype* first, const MetricPrototype* second) const {
        return first->group_name == second->group_name && first->name == second->name;
    }
};

using MetricMap = std::unordered_map<const MetricPrototype*, Metric*, MetricPrototypeHash, MetricPrototypeEqualTo>;

class MetricEntity {
public:
    MetricEntity(const std::string& name, const Labels& labels)
        : _name(name), _labels(labels) {}

    void register_metric(const MetricPrototype* metric_type, Metric* metric);
    void deregister_metric(const MetricPrototype* metric_type);
    Metric* get_metric(const std::string& name, const std::string& group_name = "") const;

    // Register a hook, this hook will called before get_metric is called
    void register_hook(const std::string& name, const std::function<void()>& hook);
    void deregister_hook(const std::string& name);
    void trigger_hook_unlocked(bool force) const;

private:
    friend class MetricRegistry;

    std::string _name;
    Labels _labels;

    mutable SpinLock _lock;
    MetricMap _metrics;
    std::map<std::string, std::function<void()>> _hooks;
};

using EntityMetricsByType = std::unordered_map<const MetricPrototype*, std::vector<std::pair<MetricEntity*, Metric*>>, MetricPrototypeHash, MetricPrototypeEqualTo>;

class MetricRegistry {
public:
    MetricRegistry(const std::string& name) : _name(name) {}
    ~MetricRegistry();

    MetricEntity* register_entity(const std::string& name, const Labels& labels);
    void deregister_entity(const std::string& name);
    std::shared_ptr<MetricEntity> get_entity(const std::string& name);

    void trigger_all_hooks(bool force) const;

    std::string to_prometheus() const;
    std::string to_json() const;
    std::string to_core_string() const;

private:
    const std::string _name;

    mutable SpinLock _lock;
    std::unordered_map<std::string, std::shared_ptr<MetricEntity>> _entities;
};

using IntCounter = CoreLocalCounter<int64_t>;
using IntAtomicCounter = AtomicCounter<int64_t>;
using UIntCounter = CoreLocalCounter<uint64_t>;
using DoubleCounter = LockCounter<double>;
using IntGauge = AtomicGauge<int64_t>;
using UIntGauge = AtomicGauge<uint64_t>;
using DoubleGauge = LockGauge<double>;

} // namespace doris
