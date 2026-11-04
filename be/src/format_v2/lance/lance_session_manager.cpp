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

#include "format_v2/lance/lance_session_manager.h"

#include <lance/lance.h>

#include <algorithm>
#include <limits>
#include <string>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "common/metrics/metrics.h"
#include "format_v2/lance/lance_file_system.h"
#include "format_v2/lance/lance_host_runtime.h"
#include "format_v2/lance/lance_reader_helper.h"

namespace doris::format::lance {
namespace {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(lance_session_index_cache_capacity_bytes, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(lance_session_index_cache_usage_bytes, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(lance_session_index_cache_entries, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(lance_session_index_cache_hits_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(lance_session_index_cache_misses_total,
                                     MetricUnit::OPERATIONS);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(lance_session_metadata_cache_capacity_bytes, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(lance_session_metadata_cache_usage_bytes, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(lance_session_metadata_cache_entries, MetricUnit::NOUNIT);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(lance_session_metadata_cache_hits_total,
                                     MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(lance_session_metadata_cache_misses_total,
                                     MetricUnit::OPERATIONS);

constexpr std::string_view LANCE_SESSION_CACHE_METRICS_HOOK = "lance_session_cache";

int64_t metric_value(uint64_t value) {
    return static_cast<int64_t>(
            std::min(value, static_cast<uint64_t>(std::numeric_limits<int64_t>::max())));
}

LanceSessionManager::Config load_lance_session_config() {
    return {.lance_index_cache_size_bytes = config::lance_index_cache_size_bytes,
            .lance_metadata_cache_size_bytes = config::lance_metadata_cache_size_bytes};
}

} // namespace

class LanceSessionMetrics final {
public:
    LanceSessionMetrics(LanceSession* session, const LanceSessionManager::Config& config)
            : _session(session), _entity(DorisMetrics::instance()->server_entity()) {
        INT_GAUGE_METRIC_REGISTER(_entity, lance_session_index_cache_capacity_bytes);
        INT_GAUGE_METRIC_REGISTER(_entity, lance_session_index_cache_usage_bytes);
        INT_GAUGE_METRIC_REGISTER(_entity, lance_session_index_cache_entries);
        INT_COUNTER_METRIC_REGISTER(_entity, lance_session_index_cache_hits_total);
        INT_COUNTER_METRIC_REGISTER(_entity, lance_session_index_cache_misses_total);
        INT_GAUGE_METRIC_REGISTER(_entity, lance_session_metadata_cache_capacity_bytes);
        INT_GAUGE_METRIC_REGISTER(_entity, lance_session_metadata_cache_usage_bytes);
        INT_GAUGE_METRIC_REGISTER(_entity, lance_session_metadata_cache_entries);
        INT_COUNTER_METRIC_REGISTER(_entity, lance_session_metadata_cache_hits_total);
        INT_COUNTER_METRIC_REGISTER(_entity, lance_session_metadata_cache_misses_total);

        lance_session_index_cache_capacity_bytes->set_value(config.lance_index_cache_size_bytes);
        lance_session_metadata_cache_capacity_bytes->set_value(
                config.lance_metadata_cache_size_bytes);
        _entity->register_hook(std::string(LANCE_SESSION_CACHE_METRICS_HOOK),
                               [this]() { update(); });
        update();
    }

    ~LanceSessionMetrics() {
        _entity->deregister_hook(std::string(LANCE_SESSION_CACHE_METRICS_HOOK));
        METRIC_DEREGISTER(_entity, lance_session_index_cache_capacity_bytes);
        METRIC_DEREGISTER(_entity, lance_session_index_cache_usage_bytes);
        METRIC_DEREGISTER(_entity, lance_session_index_cache_entries);
        METRIC_DEREGISTER(_entity, lance_session_index_cache_hits_total);
        METRIC_DEREGISTER(_entity, lance_session_index_cache_misses_total);
        METRIC_DEREGISTER(_entity, lance_session_metadata_cache_capacity_bytes);
        METRIC_DEREGISTER(_entity, lance_session_metadata_cache_usage_bytes);
        METRIC_DEREGISTER(_entity, lance_session_metadata_cache_entries);
        METRIC_DEREGISTER(_entity, lance_session_metadata_cache_hits_total);
        METRIC_DEREGISTER(_entity, lance_session_metadata_cache_misses_total);
    }

private:
    void update() {
        // Session caches are shared across queries, so publish one process-wide snapshot instead
        // of attributing concurrent cache activity to an individual query profile.
        LanceSessionCacheStats stats {};
        if (lance_session_get_cache_stats(_session, &stats) != 0) {
            LOG_EVERY_N(WARNING, 100)
                    << lance_error("collect Lance session cache statistics").to_string();
            return;
        }
        lance_session_index_cache_usage_bytes->set_value(
                metric_value(stats.index_cache_size_bytes));
        lance_session_index_cache_entries->set_value(metric_value(stats.index_cache_entries));
        lance_session_index_cache_hits_total->set_value(metric_value(stats.index_cache_hits));
        lance_session_index_cache_misses_total->set_value(metric_value(stats.index_cache_misses));
        lance_session_metadata_cache_usage_bytes->set_value(
                metric_value(stats.metadata_cache_size_bytes));
        lance_session_metadata_cache_entries->set_value(metric_value(stats.metadata_cache_entries));
        lance_session_metadata_cache_hits_total->set_value(metric_value(stats.metadata_cache_hits));
        lance_session_metadata_cache_misses_total->set_value(
                metric_value(stats.metadata_cache_misses));
    }

    LanceSession* _session;
    MetricEntity* _entity;
    IntGauge* lance_session_index_cache_capacity_bytes = nullptr;
    IntGauge* lance_session_index_cache_usage_bytes = nullptr;
    IntGauge* lance_session_index_cache_entries = nullptr;
    IntCounter* lance_session_index_cache_hits_total = nullptr;
    IntCounter* lance_session_index_cache_misses_total = nullptr;
    IntGauge* lance_session_metadata_cache_capacity_bytes = nullptr;
    IntGauge* lance_session_metadata_cache_usage_bytes = nullptr;
    IntGauge* lance_session_metadata_cache_entries = nullptr;
    IntCounter* lance_session_metadata_cache_hits_total = nullptr;
    IntCounter* lance_session_metadata_cache_misses_total = nullptr;
};

LanceSessionManager& LanceSessionManager::instance() {
    // Function-local static initialization is thread safe. Cache configuration is process scoped,
    // so changing it requires a BE restart.
    // Register the runtime owner before this session so its reactor outlives cache cleanup.
    if (config::enable_lance_host_executor) static_cast<void>(LanceHostRuntime::instance());
    static LanceSessionManager manager(load_lance_session_config());
    return manager;
}

LanceSessionManager::LanceSessionManager(Config config) : _config(std::move(config)) {}

LanceSessionManager::~LanceSessionManager() {
    shutdown();
}

void LanceSessionManager::shutdown() {
    _metrics.reset();
    lance_session_close(std::exchange(_session, nullptr));
}

Status LanceSessionManager::_initialize() {
    if (config::enable_lance_host_executor) {
        _host_runtime = LanceHostRuntime::instance();
        RETURN_IF_ERROR(_host_runtime->ensure_started());
    }
    if (_config.lance_index_cache_size_bytes < 0 || _config.lance_metadata_cache_size_bytes < 0) {
        return Status::InvalidArgument("Lance session cache capacities must be non-negative");
    }
    _session = lance_session_new(static_cast<uint64_t>(_config.lance_index_cache_size_bytes),
                                 static_cast<uint64_t>(_config.lance_metadata_cache_size_bytes));
    if (_session == nullptr) return lance_error("create shared Lance session");
    _metrics = std::make_unique<LanceSessionMetrics>(_session, _config);
    return Status::OK();
}

Status LanceSessionManager::open_dataset(const char* uri, const char* const* storage_options,
                                         uint64_t version, LanceDataset** dataset,
                                         RuntimeState* state,
                                         std::shared_ptr<LanceFileSystem>* storage) {
    if (uri == nullptr || dataset == nullptr) {
        return Status::InvalidArgument("Lance dataset URI and output must not be null");
    }
    *dataset = nullptr;
    RETURN_IF_ERROR(_ensure_initialized());
    auto result = LanceFileSystem::create(uri, storage_options, state);
    if (!result.has_value()) return std::move(result).error();
    auto filesystem = std::move(result).value();
    auto options = filesystem->options();
    *dataset =
            lance_dataset_open_with_storage_v1(uri, storage_options, version, _session, &options);
    if (*dataset == nullptr) return lance_error("open Lance dataset through Doris storage");
    if (storage) *storage = std::move(filesystem);
    return Status::OK();
}

Status LanceSessionManager::_ensure_initialized() {
    // Initialization and its result are process scoped; a failed session requires a BE restart.
    std::call_once(_initialize_once, [this] { _initialize_status = _initialize(); });
    return _initialize_status;
}

Status LanceSessionManager::get_cache_statistics(CacheStatistics* statistics) {
    if (statistics == nullptr) {
        return Status::InvalidArgument("Lance cache statistics output must not be null");
    }
    RETURN_IF_ERROR(_ensure_initialized());
    LanceSessionCacheStats stats {};
    if (lance_session_get_cache_stats(_session, &stats) != 0) {
        return lance_error("collect Lance session cache statistics");
    }
    statistics->index_cache_misses = stats.index_cache_misses;
    statistics->metadata_cache_misses = stats.metadata_cache_misses;
    return Status::OK();
}

} // namespace doris::format::lance
