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
    Metric(MetricType type, MetricUnit unit)
      : _type(type),
        _unit(unit),
        _registry(nullptr) {}
    virtual ~Metric() { hide(); }
    virtual std::string to_string() const = 0;
    MetricType type() const { return _type; }
    MetricUnit unit() const { return _unit; }
    void hide();
    virtual void write_value(rj::Value& metric_obj,
                             rj::Document::AllocatorType& allocator) = 0;
private:
    friend class MetricRegistry;

    MetricType _type = MetricType::UNTYPED;
    MetricUnit _unit = MetricUnit::NOUNIT;
    MetricRegistry* _registry;
};

// Metric that only can increment
template<typename T>
class LockSimpleMetric : public Metric {
public:
    LockSimpleMetric(MetricType type, MetricUnit unit)
      : Metric(type, unit),
        _value(T()) {}
    virtual ~LockSimpleMetric() { }

    std::string to_string() const override {
        std::stringstream ss;
        ss << value();
        return ss.str();
    }

    void write_value(rj::Value& metric_obj,
                     rj::Document::AllocatorType& allocator) override {
        metric_obj.AddMember("value", rj::Value(value()), allocator);
    }
    
    T value() const {
        std::lock_guard<SpinLock> l(_lock);
        return _value;
    }

    void increment(const T& delta) {
        std::lock_guard<SpinLock> l(this->_lock);
        this->_value += delta;
    }
    void set_value(const T& value) {
        std::lock_guard<SpinLock> l(this->_lock);
        this->_value = value;
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
    CoreLocalCounter(MetricUnit unit)
      : Metric(MetricType::COUNTER, unit), 
        _value() {}

    virtual ~CoreLocalCounter() { }

    std::string to_string() const override {
        std::stringstream ss;
        ss << value();
        return ss.str();
    }

    void write_value(rj::Value& metric_obj,
                     rj::Document::AllocatorType& allocator) override {
        metric_obj.AddMember("value", rj::Value(value()), allocator);
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
protected:
    CoreLocalValue<T> _value;
};

template<typename T>
class LockCounter : public LockSimpleMetric<T> {
public:
    LockCounter(MetricUnit unit)
      : LockSimpleMetric<T>(MetricType::COUNTER, unit) {}
    virtual ~LockCounter() { }
};

// This can only used for trival type
template<typename T>
class LockGauge : public LockSimpleMetric<T> {
public:
    LockGauge(MetricUnit unit)
      : LockSimpleMetric<T>(MetricType::GAUGE, unit) {}
    virtual ~LockGauge() { }
};

// one key-value pair used to
struct MetricLabel {
    std::string name;
    std::string value;

    MetricLabel() { }

    template<typename T, typename P>
    MetricLabel(const T& name_, const P& value_) :name(name_), value(value_) {
    }

    bool operator==(const MetricLabel& other) const {
        return name == other.name && value == other.value;
    }
    bool operator!=(const MetricLabel& other) const {
        return !(*this == other);
    }
    bool operator<(const MetricLabel& other) const {
        auto res = name.compare(other.name);
        if (res == 0) {
            return value < other.value;
        }
        return res < 0;
    }
    int compare(const MetricLabel& other) const {
        auto res = name.compare(other.name);
        if (res == 0) {
            return value.compare(other.value);
        }
        return res;
    }
    std::string to_string() const {
        return name + "=" + value;
    }
};

struct MetricLabels {
    static MetricLabels EmptyLabels;
    // used std::set to sort MetricLabel so that we can get compare two MetricLabels
    std::set<MetricLabel> labels;

    MetricLabels& add(const std::string& name, const std::string& value) {
        labels.emplace(name, value);
        return *this;
    }

    bool operator==(const MetricLabels& other) const {
        if (labels.size() != other.labels.size()) {
            return false;
        }
        auto it = std::begin(labels);
        auto other_it = std::begin(other.labels);
        while (it != std::end(labels)) {
            if (*it != *other_it) {
                return false;
            }
            ++it;
            ++other_it;
        }
        return true;
    }
    bool operator<(const MetricLabels& other) const {
        auto it = std::begin(labels);
        auto other_it = std::begin(other.labels);
        while (it != std::end(labels) && other_it != std::end(other.labels)) {
            auto res = it->compare(*other_it);
            if (res < 0) {
                return true;
            } else if (res > 0) {
                return false;
            }
            ++it;
            ++other_it;
        }
        if (it == std::end(labels)) {
            if (other_it == std::end(other.labels)) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }
    bool empty() const {
        return labels.empty();
    }

    std::string to_string() const {
        std::stringstream ss;
        int i = 0; 
        for (auto& label : labels) {
            if (i++ > 0) {
                ss << ",";
            }
            ss << label.to_string();
        }
        return ss.str();
    }
};

class MetricCollector;

class MetricsVisitor {
public:
    virtual ~MetricsVisitor() { }

    // visit a collector, you can implement collector visitor, or only implement
    // metric visitor
    virtual void visit(const std::string& prefix, const std::string& name,
                       MetricCollector* collector) = 0;
};

class MetricCollector {
public:
    bool add_metic(const MetricLabels& labels, Metric* metric);
    void remove_metric(Metric* metric);
    void collect(const std::string& prefix, const std::string& name, MetricsVisitor* visitor) {
        visitor->visit(prefix, name, this);
    }
    bool empty() const {
        return _metrics.empty();
    }
    Metric* get_metric(const MetricLabels& labels) const;
    // get all metrics belong to this collector
    void get_metrics(std::vector<Metric*>* metrics);

    const std::map<MetricLabels, Metric*>& metrics() const {
        return _metrics;
    }
    MetricType type() const { return _type; }
private:
    MetricType _type = MetricType::UNTYPED;
    std::map<MetricLabels, Metric*> _metrics;
};

class MetricRegistry {
public:
    MetricRegistry(const std::string& name) : _name(name) { }
    ~MetricRegistry();
    bool register_metric(const std::string& name, Metric* metric) {
        return register_metric(name, MetricLabels::EmptyLabels, metric);
    }
    bool register_metric(const std::string& name, const MetricLabels& labels, Metric* metric);
    // Now this function is not used frequently, so this is a little time consuming
    void deregister_metric(Metric* metric) {
        std::lock_guard<SpinLock> l(_lock);
        _deregister_locked(metric);
    }
    Metric* get_metric(const std::string& name) const {
        return get_metric(name, MetricLabels::EmptyLabels);
    }
    Metric* get_metric(const std::string& name, const MetricLabels& labels) const;

    // Register a hook, this hook will called before collect is called
    bool register_hook(const std::string& name, const std::function<void()>& hook);
    void deregister_hook(const std::string& name);

    void collect(MetricsVisitor* visitor) {
        std::lock_guard<SpinLock> l(_lock);
        if (!config::enable_metric_calculator) {
            // Before we collect, need to call hooks
            unprotected_trigger_hook();
        }

        for (auto& it : _collectors) {
            it.second->collect(_name, it.first, visitor);
        }
    }

    void trigger_hook() {
        std::lock_guard<SpinLock> l(_lock);
        unprotected_trigger_hook();
    }

private:
    void unprotected_trigger_hook() {
        for (auto& it : _hooks) {
            it.second();
        }
    }

private:
    void _deregister_locked(Metric* metric);

    const std::string _name;

    mutable SpinLock _lock;
    std::map<std::string, MetricCollector*> _collectors;
    std::map<std::string, std::function<void()>> _hooks;
};

using IntCounter = CoreLocalCounter<int64_t>;
using IntLockCounter = LockCounter<int64_t>;
using UIntCounter = CoreLocalCounter<uint64_t>;
using DoubleCounter = LockCounter<double>;
using IntGauge = LockGauge<int64_t>;
using UIntGauge = LockGauge<uint64_t>;
using DoubleGauge = LockGauge<double>;

} // namespace doris

// Convenience macros to metric
#define METRIC_DEFINE_INT_COUNTER(metric_name, unit)        \
    doris::IntCounter metric_name{unit}

#define METRIC_DEFINE_INT_LOCK_COUNTER(metric_name, unit)   \
    doris::IntLockCounter metric_name{unit}

#define METRIC_DEFINE_UINT_COUNTER(metric_name, unit)       \
    doris::UIntCounter metric_name{unit}

#define METRIC_DEFINE_DOUBLE_COUNTER(metric_name, unit)     \
    doris::DoubleCounter metric_name{unit}

#define METRIC_DEFINE_INT_GAUGE(metric_name, unit)          \
    doris::IntGauge metric_name{unit}

#define METRIC_DEFINE_UINT_GAUGE(metric_name, unit)         \
    doris::UIntGauge metric_name{unit}

#define METRIC_DEFINE_DOUBLE_GAUGE(metric_name, unit)       \
    doris::DoubleGauge metric_name{unit}
