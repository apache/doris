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

#include "util/be_lru_cache_metrics.h"

#include <bvar/bvar.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(cache_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(cache_usage, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(cache_element_count, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(cache_usage_ratio, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(cache_lookup_count, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(cache_hit_count, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(cache_miss_count, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(cache_stampede_count, MetricUnit::OPERATIONS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(cache_hit_ratio, MetricUnit::NOUNIT);

class BELRUCacheMetricsRecorder final : public ShardedLRUCache::MetricsRecorder {
public:
    BELRUCacheMetricsRecorder(std::string name, const ShardedLRUCache* cache)
            : _name(std::move(name)), _cache(cache) {
        _entity = DorisMetrics::instance()->metric_registry()->register_entity(
                std::string("lru_cache:") + _name, {{"name", _name}});
        _entity->register_hook(_name, std::bind(&BELRUCacheMetricsRecorder::_update_metrics, this));
        INT_GAUGE_METRIC_REGISTER(_entity, cache_capacity);
        INT_GAUGE_METRIC_REGISTER(_entity, cache_usage);
        INT_GAUGE_METRIC_REGISTER(_entity, cache_element_count);
        DOUBLE_GAUGE_METRIC_REGISTER(_entity, cache_usage_ratio);
        INT_COUNTER_METRIC_REGISTER(_entity, cache_lookup_count);
        INT_COUNTER_METRIC_REGISTER(_entity, cache_hit_count);
        INT_COUNTER_METRIC_REGISTER(_entity, cache_stampede_count);
        INT_COUNTER_METRIC_REGISTER(_entity, cache_miss_count);
        DOUBLE_GAUGE_METRIC_REGISTER(_entity, cache_hit_ratio);

        _hit_count_bvar = std::make_unique<bvar::Adder<uint64_t>>("doris_cache", _name);
        _hit_count_per_second = std::make_unique<bvar::PerSecond<bvar::Adder<uint64_t>>>(
                "doris_cache", _name + "_persecond", _hit_count_bvar.get(), 60);
        _lookup_count_bvar = std::make_unique<bvar::Adder<uint64_t>>("doris_cache", _name);
        _lookup_count_per_second = std::make_unique<bvar::PerSecond<bvar::Adder<uint64_t>>>(
                "doris_cache", _name + "_persecond", _lookup_count_bvar.get(), 60);
    }

    ~BELRUCacheMetricsRecorder() override {
        _entity->deregister_hook(_name);
        DorisMetrics::instance()->metric_registry()->deregister_entity(_entity);
    }

private:
    void _update_metrics() const {
        ShardedLRUCache::MetricsSnapshot snapshot = _cache->get_metrics_snapshot();
        cache_capacity->set_value(snapshot.capacity);
        cache_usage->set_value(snapshot.usage);
        cache_element_count->set_value(snapshot.element_count);
        cache_lookup_count->set_value(snapshot.lookup_count);
        cache_hit_count->set_value(snapshot.hit_count);
        cache_miss_count->set_value(snapshot.miss_count);
        cache_stampede_count->set_value(snapshot.stampede_count);
        cache_usage_ratio->set_value(snapshot.capacity == 0
                                             ? 0
                                             : static_cast<double>(snapshot.usage) /
                                                       static_cast<double>(snapshot.capacity));
        cache_hit_ratio->set_value(snapshot.lookup_count == 0
                                           ? 0
                                           : static_cast<double>(snapshot.hit_count) /
                                                     static_cast<double>(snapshot.lookup_count));
    }

    const std::string _name;
    const ShardedLRUCache* _cache;
    std::shared_ptr<MetricEntity> _entity;
    IntGauge* cache_capacity = nullptr;
    IntGauge* cache_usage = nullptr;
    IntGauge* cache_element_count = nullptr;
    DoubleGauge* cache_usage_ratio = nullptr;
    IntCounter* cache_lookup_count = nullptr;
    IntCounter* cache_hit_count = nullptr;
    IntCounter* cache_miss_count = nullptr;
    IntCounter* cache_stampede_count = nullptr;
    DoubleGauge* cache_hit_ratio = nullptr;
    std::unique_ptr<bvar::Adder<uint64_t>> _hit_count_bvar;
    std::unique_ptr<bvar::PerSecond<bvar::Adder<uint64_t>>> _hit_count_per_second;
    std::unique_ptr<bvar::Adder<uint64_t>> _lookup_count_bvar;
    std::unique_ptr<bvar::PerSecond<bvar::Adder<uint64_t>>> _lookup_count_per_second;
};

std::unique_ptr<ShardedLRUCache::MetricsRecorder> create_be_lru_cache_metrics_recorder(
        std::string name, const ShardedLRUCache* cache) {
    return std::make_unique<BELRUCacheMetricsRecorder>(std::move(name), cache);
}

} // namespace doris
