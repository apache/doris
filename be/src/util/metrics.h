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

#ifndef BDG_PALO_BE_SRC_COMMON_UTIL_METRICS_H
#define BDG_PALO_BE_SRC_COMMON_UTIL_METRICS_H

#include <map>
#include <memory>
#include <mutex>
#include <stack>
#include <string>
#include <sstream>

#include <boost/scoped_ptr.hpp>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include "common/logging.h"
#include "common/status.h"
#include "common/object_pool.h"
#include "gen_cpp/MetricDefs_types.h"
#include "gen_cpp/MetricDefs_constants.h"
#include "http/web_page_handler.h"
#include "util/debug_util.h"
#include "util/json_util.h"
#include "util/pretty_printer.h"

namespace palo {

// Helper method to print a single primitive value as a Json atom
template<typename T> void print_primitive_as_json(const T& v, std::stringstream* out) {
    (*out) << v;
}

// Specialisation to print string values inside quotes when writing to Json
template<> void print_primitive_as_json<std::string>(const std::string& v,
        std::stringstream* out);

/// Singleton that provides metric definitions. Metrics are defined in metrics.json
/// and generate_metrics.py produces MetricDefs.thrift. This singleton wraps an instance
/// of the thrift definitions.
class MetricDefs {
public:
    /// Gets the TMetricDef for the metric key. 'arg' is an optional argument to the
    /// TMetricDef for metrics defined by a format string. The key must exist or a DCHECK
    /// will fail.
    /// TODO: Support multiple arguments.
    static TMetricDef Get(const std::string& key, const std::string& arg = "");

private:
    friend class MetricsTest;

    /// Gets the MetricDefs singleton.
    static MetricDefs* GetInstance();

    /// Contains the map of all TMetricDefs, non-const for testing
    //typedef std::map<std::string, TMetricDefs> MetricDefsConstants;
    MetricDefsConstants _metric_defs;

    MetricDefs() { }
    DISALLOW_COPY_AND_ASSIGN(MetricDefs);
};

/// A metric is a container for some value, identified by a string key. Most metrics are
/// numeric, but this metric base-class is general enough such that metrics may be lists,
/// maps, histograms or other arbitrary structures.
//
/// Metrics must be able to convert themselves to JSON (for integration with our monitoring
/// tools, and for rendering in webpages). See ToJson(), and also ToLegacyJson() which
/// ensures backwards compatibility with older versions of CM.
//
/// Metrics should be supplied with a description, which is included in JSON output for
/// display by monitoring systems / Impala's webpages.
//
/// TODO: Add ToThrift() for conversion to an RPC-friendly format.
//template<typename T>
//class Metric : public GenericMetric {
class Metric {
public:
    /// Empty virtual destructor
    virtual ~Metric() {}

    /// Builds a new Value into 'val', using (if required) the allocator from
    /// 'document'. Should set the following fields where appropriate:
    //
    /// name, value, human_readable, description
    virtual void ToJson(rapidjson::Document* document, rapidjson::Value* val) = 0;

    /// Adds a new json value directly to 'document' of the form:
    /// "name" : "human-readable-string"
    //
    /// This method is kept for backwards-compatibility with CM5.0.
    virtual void ToLegacyJson(rapidjson::Document* document) = 0;

    /// Writes a human-readable representation of this metric to 'out'. This is the
    /// representation that is often displayed in webpages etc.
    virtual std::string ToHumanReadable() = 0;

    const std::string& key() const { return _key; }
    const std::string& description() const { return _description; }

    virtual void print(std::stringstream* out) {
        std::lock_guard<std::mutex> l(_lock);
        (*out) << _key << ":";
        print_value(out);
    }

    virtual void print_json(std::stringstream* out) {
        std::lock_guard<std::mutex> l(_lock);
        (*out) << "\"" << _key << "\": ";
        print_value_json(out);
    }

protected:
    // Subclasses are required to implement this to print a string
    // representation of the metric to the supplied stringstream.
    // Both methods are always called with _lock taken, so implementations must
    // not try and take _lock themselves..
    virtual void print_value(std::stringstream* out) = 0;
    virtual void print_value_json(std::stringstream* out) = 0;

    // Unique key identifying this metric
    const std::string _key;

    /// Description of this metric.
    /// TODO: share one copy amongst metrics with the same description.
    const std::string _description;

    friend class MetricGroup;

    Metric(const TMetricDef& def) : _key(def.key), _description(def.description) { }

    /// Convenience method to add standard fields (name, description, human readable string)
    /// to 'val'.
    void AddStandardFields(rapidjson::Document* document, rapidjson::Value* val);

    // Guards access to value
    std::mutex _lock;
};

/// A SimpleMetric has a value which is a simple primitive type: e.g. integers, strings and
/// floats. It is parameterised not only by the type of its value, but by both the unit
/// (e.g. bytes/s), drawn from TUnit and the 'kind' of the metric itself. The kind
/// can be one of: 'gauge', which may increase or decrease over time, a 'counter' which is
/// increasing only over time, or a 'property' which is not numeric.
//
/// SimpleMetrics return their current value through the value() method. Access to value()
/// is thread-safe.
//
/// TODO: We can use type traits to select a more efficient lock-free implementation of
/// value() etc. where it is safe to do so.
/// TODO: CalculateValue() can be returning a value, its current interface is not clean.
//template<typename T>
//class PrimitiveMetric : public Metric<T> {

template<typename T, TMetricKind::type metric_kind=TMetricKind::GAUGE>
class SimpleMetric : public Metric {
public:
    SimpleMetric(const TMetricDef& metric_def, const T& initial_value)
        : Metric(metric_def), _unit(metric_def.units), _value(initial_value) {
            DCHECK_EQ(metric_kind, metric_def.kind) << "Metric kind does not match definition: "
                << metric_def.key;
        }

    virtual ~SimpleMetric() { }

    /// Returns the current value, updating it if necessary. Thread-safe.
    T value() {
        std::lock_guard<SpinLock> l(_lock);
        CalculateValue();
        return _value;
    }

    /// Sets the current value. Thread-safe.
    void set_value(const T& value) {
        std::lock_guard<SpinLock> l(_lock);
        _value = value;
    }

    /// Adds 'delta' to the current value atomically.
    void increment(const T& delta) {
        DCHECK(kind() != TMetricKind::PROPERTY)
            << "Can't change value of PROPERTY metric: " << key();
        DCHECK(kind() != TMetricKind::COUNTER || delta >= 0)
            << "Can't decrement value of COUNTER metric: " << key();
        if (delta == 0) return;
        std::lock_guard<SpinLock> l(_lock);
        _value += delta;
    }

    // Sets current metric value to parameter
    virtual void update(const T& value) {
        std::lock_guard<SpinLock> l(_lock);
        _value = value;
    }

    virtual void ToJson(rapidjson::Document* document, rapidjson::Value* val) {
        rapidjson::Value container(rapidjson::kObjectType);
        AddStandardFields(document, &container);

        rapidjson::Value metric_value;
        ToJsonValue(value(), TUnit::NONE, document, &metric_value);
        container.AddMember("value", metric_value, document->GetAllocator());

        rapidjson::Value type_value(PrintTMetricKind(kind()).c_str(),
                document->GetAllocator());
        container.AddMember("kind", type_value, document->GetAllocator());
        rapidjson::Value units(PrintTUnit(unit()).c_str(), document->GetAllocator());
        container.AddMember("units", units, document->GetAllocator());
        *val = container;
    }

    virtual std::string ToHumanReadable() {
        return PrettyPrinter::print(value(), unit());
    }

    virtual void ToLegacyJson(rapidjson::Document* document) {
        rapidjson::Value val;
        ToJsonValue(value(), TUnit::NONE, document, &val);
        rapidjson::Value temp(_key.c_str(), document->GetAllocator());
        document->AddMember(temp, val, document->GetAllocator());
    }

    TUnit::type unit() const { return _unit; }
    TMetricKind::type kind() const { return metric_kind; }

protected:
    /// Called to compute value_ if necessary during calls to value(). The more natural
    /// approach would be to have virtual T value(), but that's not possible in C++.
    //
    /// TODO: Should be cheap to have a blank implementation, but if required we can cause
    /// the compiler to avoid calling this entirely through a compile-time constant.
    virtual void CalculateValue() { }

    /// Units of this metric.
    const TUnit::type _unit;

    /// Guards access to value_.
    SpinLock _lock;

    /// The current value of the metric
    T _value;

    virtual void print_value(std::stringstream* out)  {
        (*out) << this->_value;
    }

    virtual void print_value_json(std::stringstream* out)  {
        print_primitive_as_json(this->_value, out);
    }
};

// Gauge metric that computes the sum of several gauges.
template <typename T>
class SumGauge : public SimpleMetric<T, TMetricKind::GAUGE> {
public:
    SumGauge(const TMetricDef& metric_def,
            const std::vector<SimpleMetric<T, TMetricKind::GAUGE>*>& metrics)
        : SimpleMetric<T, TMetricKind::GAUGE>(metric_def, 0), _metrics(metrics) {}
    virtual ~SumGauge() {}

private:
    virtual void CalculateValue() override {
        T sum = 0;
        for (SimpleMetric<T, TMetricKind::GAUGE>* metric : _metrics) sum += metric->value();
        this->_value = sum;
    }

    /// The metrics to be summed.
    std::vector<SimpleMetric<T, TMetricKind::GAUGE>*> _metrics;
};

/// Container for a set of metrics. A MetricGroup owns the memory for every metric
/// contained within it (see Add*() to create commonly used metric
/// types). Metrics are 'registered' with a MetricGroup, once registered they cannot be
/// deleted.
//
/// MetricGroups may be organised hierarchically as a tree.
//
/// Typically a metric object is cached by its creator after registration. If a metric
/// must be retrieved without an available pointer, FindMetricForTesting() will search the
/// MetricGroup and all its descendent MetricGroups in turn.
//
/// TODO: Hierarchical naming: that is, resolve "group1.group2.metric-name" to a path
/// through the metric tree.
class MetricGroup {
public:
    MetricGroup(const std::string& name);

    // Registers a new metric. Ownership of the metric will be transferred to this
    // Metrics object, so callers should take care not to destroy the Metric they
    // pass in.
    // If a metric already exists with the supplied metric's key, it is replaced.
    // The template parameter M must be a subclass of Metric.
    template <typename M>
    M* register_metric(M* metric) {
        DCHECK(!metric->_key.empty());
        M* mt = _obj_pool->add(metric);

        std::lock_guard<SpinLock> l(_lock);
        DCHECK(_metric_map.find(metric->_key) == _metric_map.end());
        _metric_map[metric->_key] = mt;
        return mt;
    }

    /// Create a gauge metric object with given key and initial value (owned by this object)
    template<typename T>
    SimpleMetric<T>* AddGauge(const std::string& key, const T& value,
          const std::string& metric_def_arg = "") {
        return register_metric(new SimpleMetric<T, TMetricKind::GAUGE>(
            MetricDefs::Get(key, metric_def_arg), value));
    }

    template<typename T>
    SimpleMetric<T, TMetricKind::PROPERTY>* AddProperty(const std::string& key,
          const T& value, const std::string& metric_def_arg = "") {
        return register_metric(new SimpleMetric<T, TMetricKind::PROPERTY>(
            MetricDefs::Get(key, metric_def_arg), value));
    }

    template<typename T>
    SimpleMetric<T, TMetricKind::COUNTER>* AddCounter(const std::string& key,
          const T& value, const std::string& metric_def_arg = "") {
        return register_metric(new SimpleMetric<T, TMetricKind::COUNTER>(
            MetricDefs::Get(key, metric_def_arg), value));
    }

    /// Returns a metric by key. All MetricGroups reachable from this group are searched in
    /// depth-first order, starting with the root group.  Returns NULL if there is no metric
    /// with that key. This is not a very cheap operation; the result should be cached where
    /// possible.
    //
    /// Used for testing only.
    template <typename M>
    M* FindMetricForTesting(const std::string& key) {
        std::stack<MetricGroup*> groups;
        groups.push(this);
        std::lock_guard<SpinLock> l(_lock);
        do {
            MetricGroup* group = groups.top();
            groups.pop();
            MetricMap::const_iterator it = group->_metric_map.find(key);
            if (it != group->_metric_map.end()) return reinterpret_cast<M*>(it->second);
            for (const ChildGroupMap::value_type& child: group->_children) {
                groups.push(child.second);
            }
        } while (!groups.empty());
        return NULL;
    }

    // Returns a metric by key.  Returns NULL if there is no metric with that
    // key.  This is not a very cheap operation and should not be called in a loop.
    // If the metric needs to be updated in a loop, the returned metric should be cached.
    template <typename M>
    M* get_metric(const std::string& key) {
        std::lock_guard<SpinLock> l(_lock);
        MetricMap::iterator it = _metric_map.find(key);

        if (it == _metric_map.end()) {
            return NULL;
        }

        return reinterpret_cast<M*>(it->second);
    }

    // Register page callbacks with the webserver
    Status init(WebPageHandler* webserver);

    /// Converts this metric group (and optionally all of its children recursively) to JSON.
    void ToJson(bool include_children, rapidjson::Document* document,
            rapidjson::Value* out_val);

    /// Creates or returns an already existing child metric group.
    MetricGroup* GetOrCreateChildGroup(const std::string& name);

    /// Returns a child metric group with name 'name', or NULL if that group doesn't exist
    MetricGroup* FindChildGroup(const std::string& name);

    // Useful for debuggers, returns the output of text_callback
    std::string debug_string();

    // Same as above, but for Json output
    std::string debug_string_json();

    const std::string& name() const { return _name; }

private:
    // Pool containing all metric objects
    boost::scoped_ptr<ObjectPool> _obj_pool;

    /// Name of this metric group.
    std::string _name;

    // Guards _metric_map
    //std::mutex _lock;
    SpinLock _lock;

    // Contains all Metric objects, indexed by key
    typedef std::map<std::string, Metric*> MetricMap;
    MetricMap _metric_map;

    /// All child metric groups
    typedef std::map<std::string, MetricGroup*> ChildGroupMap;
    ChildGroupMap _children;

    /// Webserver callback for /metrics. Produces a tree of JSON values, each representing a
    /// metric group, and each including a list of metrics, and a list of immediate
    /// children.  If args contains a paramater 'metric', only the json for that metric is
    /// returned.
    /// TODO: new webserver for runtime profile
    //void TemplateCallback(const Webserver::ArgumentMap& args,
    //        rapidjson::Document* document);

    /// Legacy webpage callback for CM 5.0 and earlier. Produces a flattened map of (key,
    /// value) pairs for all metrics in this hierarchy.
    /// If args contains a paramater 'metric', only the json for that metric is returned.
    /// TODO: new webserver for runtime profile
    //void CMCompatibleCallback(const Webserver::ArgumentMap& args,
    //        rapidjson::Document* document);

    // Writes _metric_map as a list of key : value pairs
    void print_metric_map(std::stringstream* output);

    // Builds a list of metrics as Json-style "key": "value" pairs
    void print_metric_map_as_json(std::vector<std::string>* metrics);

    // Webserver callback (on /metrics), renders metrics as single text page
    void text_callback(const WebPageHandler::ArgumentMap& args, std::stringstream* output);

    // Webserver callback (on /jsonmetrics), renders metrics as a single json document
    void json_callback(const WebPageHandler::ArgumentMap& args, std::stringstream* output);

};

typedef class SimpleMetric<int64_t, TMetricKind::GAUGE> IntGauge;
typedef class SimpleMetric<uint64_t, TMetricKind::GAUGE> UIntGauge;
typedef class SimpleMetric<double, TMetricKind::GAUGE> DoubleGauge;
typedef class SimpleMetric<int64_t, TMetricKind::COUNTER> IntCounter;

typedef class SimpleMetric<bool, TMetricKind::PROPERTY> BooleanProperty;
typedef class SimpleMetric<std::string, TMetricKind::PROPERTY> StringProperty;

TMetricDef MakeTMetricDef(const std::string& key, TMetricKind::type kind,
    TUnit::type unit);

} //namespace palo

#endif // BDG_PALO_BE_SRC_COMMON_UTIL_METRICS_H
