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

#ifndef BDG_PALO_BE_SRC_UTIL_COLLECTION_METRICS_H
#define BDG_PALO_BE_SRC_UTIL_COLLECTION_METRICS_H

#include "util/metrics.h"

#include <string>
#include <vector>
#include <set>
#include <boost/algorithm/string/join.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/count.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/variance.hpp>

//#include "util/pretty-printer.h"

namespace palo {

/// Collection metrics are those whose values have more structure than simple
/// scalar types. Therefore they need specialised ToJson() methods, and
/// typically a specialised API for updating the values they contain.

/// Metric whose value is a set of items
template <typename T>
class SetMetric : public Metric {
public:
    static SetMetric* CreateAndRegister(MetricGroup* metrics, const std::string& key,
            const std::set<T>& value) {
        return metrics->register_metric(new SetMetric(MetricDefs::Get(key), value));
    }

    SetMetric(const TMetricDef& def, const std::set<T>& value)
        : Metric(def), _value(value) {
            DCHECK_EQ(def.kind, TMetricKind::SET);
        }

    /// Put an item in this set.
    void add(const T& item) {
        boost::lock_guard<boost::mutex> l(_lock);
        _value.insert(item);
    }

    /// Remove an item from this set by value.
    void remove(const T& item) {
        boost::lock_guard<boost::mutex> l(_lock);
        _value.erase(item);
    }

    /// Copy out value.
    std::set<T> value() {
        boost::lock_guard<boost::mutex> l(_lock);
        return _value;
    }

    void reset() { _value.clear(); }

    virtual void ToJson(rapidjson::Document* document, rapidjson::Value* value) {
        rapidjson::Value container(rapidjson::kObjectType);
        AddStandardFields(document, &container);
        rapidjson::Value metric_list(rapidjson::kArrayType);
        for (const T& s: _value) {
            rapidjson::Value entry_value;
            ToJsonValue(s, TUnit::NONE, document, &entry_value);
            metric_list.PushBack(entry_value, document->GetAllocator());
        }
        container.AddMember("items", metric_list, document->GetAllocator());
        *value = container;
    }

    virtual void ToLegacyJson(rapidjson::Document* document) {
        rapidjson::Value metric_list(rapidjson::kArrayType);
        for (const T& s: _value) {
            rapidjson::Value entry_value;
            ToJsonValue(s, TUnit::NONE, document, &entry_value);
            metric_list.PushBack(entry_value, document->GetAllocator());
        }
        document->AddMember(rapidjson::Value(_key.c_str(), document->GetAllocator()), 
            metric_list, document->GetAllocator());
    }

    virtual std::string ToHumanReadable() {
        std::stringstream out;
        //PrettyPrinter::printStringList<std::set<T>>(
        //    _value, TUnit::NONE, &out);
        return out.str();
    }

    virtual void print_value(std::stringstream* out) {}
    virtual void print_value_json(std::stringstream* out) {}

private:
    /// Lock protecting the set
    boost::mutex _lock;

    /// The set of items
    std::set<T> _value;
};

/// Enum to define which statistic types are available in the StatsMetric
struct StatsType {
    enum type {
        MIN = 1,
        MAX = 2,
        MEAN = 4,
        STDDEV = 8,
        COUNT = 16,
        ALL = 31
    };
};

/// Metric which accumulates min, max and mean of all values, plus a count of samples
/// seen. The output can be controlled by passing a bitmask as a template parameter to
/// indicate which values should be printed or returned as JSON.
///
/// Printed output looks like: name: count:
/// 4, last: 0.0141, min: 4.546e-06, max: 0.0243, mean: 0.0336, stddev: 0.0336
///
/// After construction, all statistics are ill-defined, but count will be 0. The first call
/// to Update() will initialise all stats.
template <typename T, int StatsSelection=StatsType::ALL>
class StatsMetric : public Metric {
public:
    static StatsMetric* CreateAndRegister(MetricGroup* metrics, const std::string& key,
            const std::string& arg = "") {
        return metrics->register_metric(new StatsMetric(MetricDefs::Get(key, arg)));
    }

    StatsMetric(const TMetricDef& def) : Metric(def), _unit(def.units) {
        DCHECK_EQ(def.kind, TMetricKind::STATS);
    }

    void Update(const T& value) {
        boost::lock_guard<boost::mutex> l(_lock);
        _value = value;
        _acc(value);
    }

    void Reset() {
        boost::lock_guard<boost::mutex> l(_lock);
        _acc = Accumulator();
    }

    virtual void ToJson(rapidjson::Document* document, rapidjson::Value* val) {
        boost::lock_guard<boost::mutex> l(_lock);
        rapidjson::Value container(rapidjson::kObjectType);
        AddStandardFields(document, &container);
        rapidjson::Value units(PrintTUnit(_unit).c_str(), document->GetAllocator());
        container.AddMember("units", units, document->GetAllocator());

        if (StatsSelection & StatsType::COUNT) {
            container.AddMember("count",
                    static_cast<uint64_t>(boost::accumulators::count(_acc)),
                    document->GetAllocator());
        }

        if (boost::accumulators::count(_acc) > 0) {
            container.AddMember("last", _value, document->GetAllocator());

            if (StatsSelection & StatsType::MIN) {
                container.AddMember("min",
                        static_cast<uint64_t>(boost::accumulators::min(_acc)),
                        document->GetAllocator());
            }

            if (StatsSelection & StatsType::MAX) {
                container.AddMember("max", boost::accumulators::max(_acc),
                        document->GetAllocator());
            }

            if (StatsSelection & StatsType::MEAN) {
                container.AddMember("mean", boost::accumulators::mean(_acc),
                        document->GetAllocator());
            }

            if (StatsSelection & StatsType::STDDEV) {
                container.AddMember("stddev", sqrt(boost::accumulators::variance(_acc)),
                        document->GetAllocator());
            }
        }
        *val = container;
    }

    virtual void ToLegacyJson(rapidjson::Document* document) {
        std::stringstream ss;
        boost::lock_guard<boost::mutex> l(_lock);
        rapidjson::Value container(rapidjson::kObjectType);

        if (StatsSelection & StatsType::COUNT) {
            container.AddMember("count", boost::accumulators::count(_acc),
                    document->GetAllocator());
        }

        if (boost::accumulators::count(_acc) > 0) {
            container.AddMember("last", _value, document->GetAllocator());
            if (StatsSelection & StatsType::MIN) {
                container.AddMember("min", boost::accumulators::min(_acc),
                        document->GetAllocator());
            }

            if (StatsSelection & StatsType::MAX) {
                container.AddMember("max", boost::accumulators::max(_acc),
                        document->GetAllocator());
            }

            if (StatsSelection & StatsType::MEAN) {
                container.AddMember("mean", boost::accumulators::mean(_acc),
                        document->GetAllocator());
            }

            if (StatsSelection & StatsType::STDDEV) {
                container.AddMember("stddev", sqrt(boost::accumulators::variance(_acc)),
                        document->GetAllocator());
            }
        }
        rapidjson::Value temp(_key.c_str(), document->GetAllocator());
        document->AddMember(temp, container, document->GetAllocator());
    }

    virtual std::string ToHumanReadable() {
        std::stringstream out;
        if (StatsSelection & StatsType::COUNT) {
            out << "count: " << boost::accumulators::count(_acc);
            if (boost::accumulators::count(_acc) > 0) out << ", ";
        }
        if (boost::accumulators::count(_acc) > 0) {
            out << "last: " << PrettyPrinter::print(_value, _unit);
            if (StatsSelection & StatsType::MIN) {
                out << ", min: " << PrettyPrinter::print(boost::accumulators::min(_acc), _unit);
            }

            if (StatsSelection & StatsType::MAX) {
                out << ", max: " << PrettyPrinter::print(boost::accumulators::max(_acc), _unit);
            }

            if (StatsSelection & StatsType::MEAN) {
                out << ", mean: " << PrettyPrinter::print(boost::accumulators::mean(_acc), _unit);
            }

            if (StatsSelection & StatsType::STDDEV) {
                out << ", stddev: " << PrettyPrinter::print(
                    sqrt(boost::accumulators::variance(_acc)), _unit);
            }
        }
        return out.str();
    }
    virtual void print_value(std::stringstream* out) {}
    virtual void print_value_json(std::stringstream* out) {}

private:
    /// The units of the values captured in this metric, used when pretty-printing.
    TUnit::type _unit;

    /// Lock protecting the value and the accumulator_set
    boost::mutex _lock;

    /// The last value
    T _value;

    /// The set of accumulators that update the statistics on each Update()
    typedef boost::accumulators::accumulator_set<T,
            boost::accumulators::features<boost::accumulators::tag::mean,
            boost::accumulators::tag::count,
            boost::accumulators::tag::min,
            boost::accumulators::tag::max,
            boost::accumulators::tag::variance>> Accumulator;
    Accumulator _acc;

};

};

#endif
