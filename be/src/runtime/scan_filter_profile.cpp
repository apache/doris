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

#include "runtime/scan_filter_profile.h"

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <glog/logging.h>

#include <algorithm>
#include <unordered_set>

#include "runtime/runtime_profile.h"

namespace doris {

namespace {

constexpr const char* SCAN_FILTER_INFO = "ScanFilterInfo";
constexpr const char* KEY_RANGE_INFO = "KeyRangeInfo";
constexpr const char* RUNTIME_FILTER_PARTITION_PRUNING = "RuntimeFilterPartitionPruning";
constexpr int NOT_APPLIED_PROFILE_ORDER = static_cast<int>(ScanFilterStage::NUM_STAGES);

bool is_index_stage(ScanFilterStage stage) {
    return stage == ScanFilterStage::INDEX_INVERTED || stage == ScanFilterStage::INDEX_ANN ||
           stage == ScanFilterStage::INDEX_DICT || stage == ScanFilterStage::INDEX_BLOOM_FILTER ||
           stage == ScanFilterStage::INDEX_ZONE_MAP;
}

bool is_exec_stage(ScanFilterStage stage) {
    return stage == ScanFilterStage::EXEC_VECTOR || stage == ScanFilterStage::EXEC_SHORT_CIRCUIT ||
           stage == ScanFilterStage::EXEC_COMMON_EXPR || stage == ScanFilterStage::EXEC_RESIDUAL;
}

void set_counter(RuntimeProfile* profile, const std::string& name, TUnit::type type,
                 const std::string& parent, int64_t level, int64_t value) {
    auto* counter = profile->add_counter(name, type, parent, level);
    counter->set(value);
}

void set_root_counter(RuntimeProfile* profile, const std::string& name, TUnit::type type,
                      int64_t level, int64_t value) {
    set_counter(profile, name, type, RuntimeProfile::ROOT_COUNTER, level, value);
}

RuntimeProfile* get_or_create_child(RuntimeProfile* profile, const std::string& name) {
    auto* child = profile->get_child(name);
    if (child != nullptr) {
        return child;
    }
    return profile->create_child(name, true, false);
}

void add_info_string_if_not_empty(RuntimeProfile* profile, const std::string& key,
                                  const std::string& value) {
    if (!value.empty()) {
        profile->add_info_string(key, value);
    }
}

const char* scan_filter_source_name(ScanFilterKind kind) {
    switch (kind) {
    case ScanFilterKind::NORMAL:
        return "Conjunct";
    case ScanFilterKind::RUNTIME_FILTER:
        return "RuntimeFilter";
    case ScanFilterKind::TOPN_FILTER:
        return "TopNFilter";
    case ScanFilterKind::UNKNOWN:
        return "Unknown";
    }
    return "Unknown";
}

struct SummaryStats {
    bool participated = false;
    bool has_filtering_stage = false;
    bool has_time = false;
    int first_stage = NOT_APPLIED_PROFILE_ORDER;
    int last_stage = -1;
    int64_t input_rows = 0;
    int64_t output_rows = 0;
    int64_t filtered_rows = 0;
    int64_t time_ns = 0;
};

void update_summary(SummaryStats* summary, ScanFilterStage stage,
                    const ScanFilterStageStatsSnapshot& stats) {
    if (!stats.participated()) {
        return;
    }
    const auto order = static_cast<int>(stage);
    if (stats.filtered_rows > 0) {
        if (!summary->has_filtering_stage || order < summary->first_stage) {
            summary->first_stage = order;
            summary->input_rows = stats.input_rows;
        }
        if (!summary->has_filtering_stage || order > summary->last_stage) {
            summary->last_stage = order;
            summary->output_rows = stats.output_rows;
        }
        summary->has_filtering_stage = true;
    } else if (!summary->has_filtering_stage &&
               (!summary->participated || order > summary->last_stage)) {
        summary->first_stage = order;
        summary->last_stage = order;
        summary->input_rows = stats.input_rows;
        summary->output_rows = stats.output_rows;
    }
    summary->participated = true;
    summary->filtered_rows += stats.filtered_rows;
    if (stats.has_time) {
        summary->has_time = true;
        summary->time_ns += stats.time_ns;
    }
}

struct MaterializedFilterSnapshot {
    ScanFilterDesc desc;
    std::optional<ScanRuntimeFilterProfileStats> runtime_filter_stats;
    std::array<ScanFilterStageStatsSnapshot, static_cast<size_t>(ScanFilterStage::NUM_STAGES)>
            stage_snapshots;
    SummaryStats total;
    SummaryStats index;
    SummaryStats exec;
};

void materialize_filter_stage(RuntimeProfile* filter_profile, ScanFilterStage stage,
                              const ScanFilterStageStatsSnapshot& stats) {
    auto* stage_profile = get_or_create_child(filter_profile, scan_filter_stage_name(stage));
    set_root_counter(stage_profile, "InputRows", TUnit::UNIT, 2, stats.input_rows);
    set_root_counter(stage_profile, "FilteredRows", TUnit::UNIT, 2, stats.filtered_rows);
    if (stats.has_time) {
        set_root_counter(stage_profile, "Time", TUnit::TIME_NS, 2, stats.time_ns);
    }
}

std::string scan_filter_stages_string(const MaterializedFilterSnapshot& snapshot,
                                      bool is_key_range_source) {
    std::vector<std::string> stages;
    for (int i = 0; i < static_cast<int>(ScanFilterStage::NUM_STAGES); ++i) {
        const auto stage = static_cast<ScanFilterStage>(i);
        if (snapshot.stage_snapshots[static_cast<size_t>(stage)].participated()) {
            stages.emplace_back(scan_filter_stage_name(stage));
        }
    }
    if (stages.empty()) {
        return is_key_range_source ? "KeyRangeInfo" : "NotApplied";
    }
    return fmt::format("{}", fmt::join(stages, " -> "));
}

std::string target_string(const ScanFilterDesc& desc) {
    std::vector<std::string> parts;
    if (desc.column_id >= 0) {
        parts.emplace_back(fmt::format("column_id={}", desc.column_id));
    }
    if (!desc.column_name.empty()) {
        parts.emplace_back(fmt::format("column={}", desc.column_name));
    }
    return fmt::format("{}", fmt::join(parts, ", "));
}

std::string source_string(const ScanFilterDesc& desc) {
    if (desc.kind == ScanFilterKind::RUNTIME_FILTER) {
        return fmt::format("{} rf_id={}", scan_filter_source_name(desc.kind),
                           desc.runtime_filter_id);
    }
    if (desc.kind == ScanFilterKind::TOPN_FILTER) {
        return fmt::format("{} source_node_id={}", scan_filter_source_name(desc.kind),
                           desc.topn_filter_source_node_id);
    }
    return scan_filter_source_name(desc.kind);
}

void materialize_filter_counters(RuntimeProfile* filter_profile,
                                 const MaterializedFilterSnapshot& snapshot, int profile_level,
                                 bool is_key_range_source) {
    const auto* runtime_filter_stats =
            snapshot.runtime_filter_stats.has_value() ? &*snapshot.runtime_filter_stats : nullptr;
    filter_profile->add_info_string("Source", source_string(snapshot.desc));
    add_info_string_if_not_empty(filter_profile, "Target", target_string(snapshot.desc));
    filter_profile->add_info_string("Stages",
                                    scan_filter_stages_string(snapshot, is_key_range_source));
    add_info_string_if_not_empty(filter_profile, "Expr", snapshot.desc.expr_debug_string);
    if (profile_level >= 2 && runtime_filter_stats != nullptr &&
        !runtime_filter_stats->debug_string.empty()) {
        filter_profile->add_info_string("RuntimeFilterInfo", runtime_filter_stats->debug_string);
    }

    if (snapshot.total.participated) {
        set_root_counter(filter_profile, "InputRows", TUnit::UNIT, 1, snapshot.total.input_rows);
        set_root_counter(filter_profile, "FilteredRows", TUnit::UNIT, 1,
                         snapshot.total.filtered_rows);
        if (snapshot.total.has_time) {
            set_root_counter(filter_profile, "FilterTime", TUnit::TIME_NS, 1,
                             snapshot.total.time_ns);
        }
    }
    if (runtime_filter_stats != nullptr) {
        if (!snapshot.total.participated && runtime_filter_stats->input_rows > 0) {
            DCHECK_GE(runtime_filter_stats->input_rows, runtime_filter_stats->filtered_rows);
            set_root_counter(filter_profile, "InputRows", TUnit::UNIT, 1,
                             runtime_filter_stats->input_rows);
            set_root_counter(filter_profile, "FilteredRows", TUnit::UNIT, 1,
                             runtime_filter_stats->filtered_rows);
        }
        set_root_counter(filter_profile, "WaitTime", TUnit::TIME_NS, 1,
                         runtime_filter_stats->wait_time_ns);
        set_root_counter(filter_profile, "AlwaysTrueFilterRows", TUnit::UNIT, 1,
                         runtime_filter_stats->always_true_filter_rows);
    }

    if (profile_level < 2) {
        return;
    }
    for (int i = 0; i < static_cast<int>(ScanFilterStage::NUM_STAGES); ++i) {
        const auto stage = static_cast<ScanFilterStage>(i);
        const auto& stage_stats = snapshot.stage_snapshots[static_cast<size_t>(stage)];
        if (!stage_stats.participated()) {
            continue;
        }
        materialize_filter_stage(filter_profile, stage, stage_stats);
    }
}

void materialize_runtime_filter_partition_pruning(
        RuntimeProfile* scan_filter_profile, const ScanRuntimeFilterPartitionPruningStats& stats) {
    auto* pruning_profile =
            get_or_create_child(scan_filter_profile, RUNTIME_FILTER_PARTITION_PRUNING);
    if (stats.total_partitions > 0) {
        set_root_counter(pruning_profile, "TotalPartitions", TUnit::UNIT, 1,
                         stats.total_partitions);
    }
    if (stats.pruned_partitions > 0) {
        set_root_counter(pruning_profile, "PrunedPartitions", TUnit::UNIT, 1,
                         stats.pruned_partitions);
    }
    if (stats.pruned_tablets > 0) {
        set_root_counter(pruning_profile, "PrunedTablets", TUnit::UNIT, 1, stats.pruned_tablets);
    }
}

} // namespace

const char* scan_filter_kind_name(ScanFilterKind kind) {
    switch (kind) {
    case ScanFilterKind::NORMAL:
        return "NORMAL";
    case ScanFilterKind::RUNTIME_FILTER:
        return "RUNTIME_FILTER";
    case ScanFilterKind::TOPN_FILTER:
        return "TOPN_FILTER";
    case ScanFilterKind::UNKNOWN:
        return "UNKNOWN";
    }
    return "UNKNOWN";
}

const char* scan_filter_stage_name(ScanFilterStage stage) {
    switch (stage) {
    case ScanFilterStage::KEY_RANGE:
        return "KeyRange";
    case ScanFilterStage::INDEX_INVERTED:
        return "IndexInverted";
    case ScanFilterStage::INDEX_ANN:
        return "IndexAnn";
    case ScanFilterStage::INDEX_DICT:
        return "IndexDict";
    case ScanFilterStage::INDEX_BLOOM_FILTER:
        return "IndexBloomFilter";
    case ScanFilterStage::INDEX_ZONE_MAP:
        return "IndexZoneMap";
    case ScanFilterStage::EXEC_VECTOR:
        return "ExecuteVector";
    case ScanFilterStage::EXEC_SHORT_CIRCUIT:
        return "ExecuteShortCircuit";
    case ScanFilterStage::EXEC_COMMON_EXPR:
        return "ExecuteCommonExpr";
    case ScanFilterStage::EXEC_RESIDUAL:
        return "ExecuteResidual";
    case ScanFilterStage::NUM_STAGES:
        break;
    }
    return "Unknown";
}

void ScanFilterStats::record(ScanFilterStage stage, int64_t input_rows, int64_t output_rows,
                             std::optional<int64_t> time_ns) {
    DCHECK_GE(input_rows, output_rows);
    const auto stage_index = static_cast<size_t>(stage);
    DCHECK_LT(stage_index, _stage_stats.size());
    auto& stats = _stage_stats[stage_index];
    stats.calls.fetch_add(1, std::memory_order_relaxed);
    stats.input_rows.fetch_add(input_rows, std::memory_order_relaxed);
    stats.output_rows.fetch_add(output_rows, std::memory_order_relaxed);
    stats.filtered_rows.fetch_add(input_rows - output_rows, std::memory_order_relaxed);
    if (time_ns.has_value()) {
        stats.has_time.store(true, std::memory_order_relaxed);
        stats.time_ns.fetch_add(*time_ns, std::memory_order_relaxed);
    }
}

ScanFilterStageStatsSnapshot ScanFilterStats::snapshot(ScanFilterStage stage) const {
    const auto stage_index = static_cast<size_t>(stage);
    DCHECK_LT(stage_index, _stage_stats.size());
    const auto& stats = _stage_stats[stage_index];
    return {.has_time = stats.has_time.load(std::memory_order_relaxed),
            .calls = stats.calls.load(std::memory_order_relaxed),
            .input_rows = stats.input_rows.load(std::memory_order_relaxed),
            .output_rows = stats.output_rows.load(std::memory_order_relaxed),
            .filtered_rows = stats.filtered_rows.load(std::memory_order_relaxed),
            .time_ns = stats.time_ns.load(std::memory_order_relaxed)};
}

ScanFilterHandle ScanFilterProfile::register_filter(ScanFilterDesc desc) {
    auto stats = std::make_shared<ScanFilterStats>();
    std::lock_guard lock(_lock);
    const auto filter_id = static_cast<int32_t>(_filters.size());
    desc.filter_id = filter_id;
    _filters.emplace_back(FilterEntry {
            .desc = std::move(desc),
            .stats = stats,
            .runtime_filter_stats = std::nullopt,
    });
    return {.filter_id = filter_id, .stats = std::move(stats)};
}

ScanFilterHandle ScanFilterProfile::register_key_range(ScanKeyRangeInfo key_range) {
    auto stats = std::make_shared<ScanFilterStats>();
    std::lock_guard lock(_lock);
    DCHECK(!_key_range.has_value());
    key_range.handle = {.stats = stats};
    _key_range = std::move(key_range);
    return _key_range->handle;
}

std::vector<ScanFilterProfile::FilterSnapshot> ScanFilterProfile::_snapshots() const {
    std::lock_guard lock(_lock);
    std::vector<FilterSnapshot> snapshots;
    snapshots.reserve(_filters.size());
    for (const auto& filter : _filters) {
        snapshots.push_back({.desc = filter.desc,
                             .stats = filter.stats,
                             .runtime_filter_stats = filter.runtime_filter_stats});
    }
    return snapshots;
}

std::optional<ScanKeyRangeInfo> ScanFilterProfile::_key_range_snapshot() const {
    std::lock_guard lock(_lock);
    return _key_range;
}

int64_t ScanFilterProfile::_runtime_filter_acquire_time_snapshot() const {
    std::lock_guard lock(_lock);
    return _runtime_filter_acquire_time_ns;
}

ScanRuntimeFilterPartitionPruningStats
ScanFilterProfile::_runtime_filter_partition_pruning_stats_snapshot() const {
    std::lock_guard lock(_lock);
    return _runtime_filter_partition_pruning_stats;
}

void ScanFilterProfile::set_runtime_filter_acquire_time(int64_t acquire_time_ns) {
    std::lock_guard lock(_lock);
    _runtime_filter_acquire_time_ns = acquire_time_ns;
}

void ScanFilterProfile::set_runtime_filter_profile_stats(ScanRuntimeFilterProfileStats stats) {
    std::lock_guard lock(_lock);
    DCHECK_GE(stats.runtime_filter_id, 0);
    for (auto& filter : _filters) {
        if (filter.desc.kind == ScanFilterKind::RUNTIME_FILTER &&
            filter.desc.runtime_filter_id == stats.runtime_filter_id) {
            filter.runtime_filter_stats = std::move(stats);
            return;
        }
    }

    ScanFilterDesc desc;
    desc.filter_id = static_cast<int32_t>(_filters.size());
    desc.kind = ScanFilterKind::RUNTIME_FILTER;
    desc.runtime_filter_id = stats.runtime_filter_id;
    _filters.emplace_back(FilterEntry {.desc = std::move(desc),
                                       .stats = std::make_shared<ScanFilterStats>(),
                                       .runtime_filter_stats = std::move(stats)});
}

void ScanFilterProfile::set_runtime_filter_partition_pruning_stats(
        ScanRuntimeFilterPartitionPruningStats stats) {
    std::lock_guard lock(_lock);
    _runtime_filter_partition_pruning_stats = stats;
}

void ScanFilterProfile::materialize(RuntimeProfile* profile, int profile_level) const {
    DCHECK(profile != nullptr);
    DCHECK_GT(profile_level, 0);

    const auto snapshots = _snapshots();
    const auto key_range_snapshot = _key_range_snapshot();
    const auto runtime_filter_acquire_time_ns = _runtime_filter_acquire_time_snapshot();
    const auto runtime_filter_partition_pruning_stats =
            _runtime_filter_partition_pruning_stats_snapshot();
    std::vector<MaterializedFilterSnapshot> scan_filter_snapshots;
    scan_filter_snapshots.reserve(snapshots.size());

    for (const auto& snapshot : snapshots) {
        MaterializedFilterSnapshot materialized;
        materialized.desc = snapshot.desc;
        materialized.runtime_filter_stats = snapshot.runtime_filter_stats;
        for (int i = 0; i < static_cast<int>(ScanFilterStage::NUM_STAGES); ++i) {
            const auto stage = static_cast<ScanFilterStage>(i);
            materialized.stage_snapshots[i] = snapshot.stats->snapshot(stage);
            update_summary(&materialized.total, stage, materialized.stage_snapshots[i]);
            if (is_index_stage(stage)) {
                update_summary(&materialized.index, stage, materialized.stage_snapshots[i]);
            } else if (is_exec_stage(stage)) {
                update_summary(&materialized.exec, stage, materialized.stage_snapshots[i]);
            }
        }
        scan_filter_snapshots.emplace_back(std::move(materialized));
    }

    if (key_range_snapshot.has_value()) {
        auto* key_range_profile = get_or_create_child(profile, KEY_RANGE_INFO);
        const auto& key_range = *key_range_snapshot;
        const auto key_range_stats = key_range.handle.stats->snapshot(ScanFilterStage::KEY_RANGE);
        if (!key_range.source_filter_ids.empty()) {
            key_range_profile->add_info_string(
                    "SourceFilterIds",
                    fmt::format("{}", fmt::join(key_range.source_filter_ids, ",")));
        }
        if (profile_level >= 2) {
            add_info_string_if_not_empty(key_range_profile, "ScanKeys", key_range.scan_keys);
        }

        set_root_counter(key_range_profile, "RangeNum", TUnit::UNIT, 1, key_range.range_count);
        set_root_counter(key_range_profile, "InputRows", TUnit::UNIT, 1,
                         key_range_stats.input_rows);
        set_root_counter(key_range_profile, "FilteredRows", TUnit::UNIT, 1,
                         key_range_stats.filtered_rows);
    }

    const bool has_partition_pruning_stats =
            runtime_filter_partition_pruning_stats.total_partitions > 0;
    if (scan_filter_snapshots.empty() && !has_partition_pruning_stats &&
        runtime_filter_acquire_time_ns <= 0) {
        return;
    }

    std::unordered_set<int32_t> key_range_source_filter_ids;
    if (key_range_snapshot.has_value()) {
        key_range_source_filter_ids.insert(key_range_snapshot->source_filter_ids.begin(),
                                           key_range_snapshot->source_filter_ids.end());
    }

    std::ranges::sort(scan_filter_snapshots, [](const auto& left, const auto& right) {
        return left.desc.filter_id < right.desc.filter_id;
    });

    auto* scan_filter_profile = get_or_create_child(profile, SCAN_FILTER_INFO);
    if (runtime_filter_acquire_time_ns > 0) {
        set_root_counter(scan_filter_profile, "RuntimeFilterAcquireTime", TUnit::TIME_NS, 2,
                         runtime_filter_acquire_time_ns);
    }
    if (has_partition_pruning_stats) {
        materialize_runtime_filter_partition_pruning(scan_filter_profile,
                                                     runtime_filter_partition_pruning_stats);
    }

    if (scan_filter_snapshots.empty()) {
        return;
    }

    for (const auto& snapshot : scan_filter_snapshots) {
        auto* filter_profile = get_or_create_child(
                scan_filter_profile, fmt::format("ScanFilter {}", snapshot.desc.filter_id));
        materialize_filter_counters(filter_profile, snapshot, profile_level,
                                    key_range_source_filter_ids.contains(snapshot.desc.filter_id));
    }
}

} // namespace doris
