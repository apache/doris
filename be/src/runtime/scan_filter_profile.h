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

#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace doris {

class RuntimeProfile;

enum class ScanFilterKind {
    NORMAL,
    RUNTIME_FILTER,
    TOPN_FILTER,
    KEY_RANGE,
    UNKNOWN,
};

enum class ScanFilterStage {
    KEY_RANGE = 0,
    INDEX_INVERTED,
    INDEX_BLOOM_FILTER,
    INDEX_ZONE_MAP,
    INDEX_DICT,
    INDEX_ANN,
    EXEC_VECTOR,
    EXEC_SHORT_CIRCUIT,
    EXEC_COMMON_EXPR,
    EXEC_RESIDUAL,
    NUM_STAGES,
};

struct ScanFilterDesc {
    int32_t filter_id = -1;
    ScanFilterKind kind = ScanFilterKind::UNKNOWN;
    int32_t runtime_filter_id = -1;
    int32_t topn_filter_source_node_id = -1;
    int32_t slot_id = -1;
    int32_t column_id = -1;
    std::string column_name;
    std::string compact_info;
    std::string debug_string;
    std::vector<int32_t> source_filter_ids;
    int64_t range_count = 0;
};

struct ScanRuntimeFilterProfileStats {
    int32_t runtime_filter_id = -1;
    int64_t input_rows = 0;
    int64_t filtered_rows = 0;
    int64_t wait_time_ns = 0;
    int64_t always_true_filter_rows = 0;
    std::string debug_string;
};

struct ScanRuntimeFilterPartitionPruningStats {
    int64_t total_partitions = 0;
    int64_t pruned_partitions = 0;
    int64_t pruned_tablets = 0;
};

struct ScanFilterStageStatsSnapshot {
    bool participated = false;
    bool has_time = false;
    int64_t calls = 0;
    int64_t input_rows = 0;
    int64_t output_rows = 0;
    int64_t filtered_rows = 0;
    int64_t time_ns = 0;
};

class ScanFilterStats {
public:
    void record(ScanFilterStage stage, int64_t input_rows, int64_t output_rows,
                std::optional<int64_t> time_ns = std::nullopt);

    ScanFilterStageStatsSnapshot snapshot(ScanFilterStage stage) const;

private:
    struct StageStats {
        std::atomic<int64_t> calls = 0;
        std::atomic<int64_t> input_rows = 0;
        std::atomic<int64_t> output_rows = 0;
        std::atomic<int64_t> filtered_rows = 0;
        std::atomic<int64_t> time_ns = 0;
        std::atomic<bool> has_time = false;
        std::atomic<bool> participated = false;
    };

    static constexpr size_t STAGE_NUM = static_cast<size_t>(ScanFilterStage::NUM_STAGES);
    std::array<StageStats, STAGE_NUM> _stage_stats;
};

struct ScanFilterHandle {
    int32_t filter_id = -1;
    std::shared_ptr<ScanFilterStats> stats;

    explicit operator bool() const { return filter_id >= 0 && stats != nullptr; }
};

class ScanFilterProfile {
public:
    ScanFilterHandle register_filter(ScanFilterDesc desc);
    void add_source_filter(int32_t filter_id, int32_t source_filter_id);
    void set_runtime_filter_acquire_time(int64_t acquire_time_ns);
    void set_runtime_filter_profile_stats(ScanRuntimeFilterProfileStats stats);
    void set_runtime_filter_partition_pruning_stats(ScanRuntimeFilterPartitionPruningStats stats);
    void materialize(RuntimeProfile* profile, int profile_level) const;

private:
    struct FilterSnapshot {
        ScanFilterDesc desc;
        std::shared_ptr<ScanFilterStats> stats;
    };

    std::vector<FilterSnapshot> _snapshots() const;
    std::vector<ScanRuntimeFilterProfileStats> _runtime_filter_stats_snapshots() const;
    int64_t _runtime_filter_acquire_time_snapshot() const;
    ScanRuntimeFilterPartitionPruningStats _runtime_filter_partition_pruning_stats_snapshot() const;

    mutable std::mutex _lock;
    std::vector<ScanFilterDesc> _descs;
    std::vector<std::shared_ptr<ScanFilterStats>> _stats;
    std::vector<ScanRuntimeFilterProfileStats> _runtime_filter_stats;
    int64_t _runtime_filter_acquire_time_ns = 0;
    ScanRuntimeFilterPartitionPruningStats _runtime_filter_partition_pruning_stats;
};

const char* scan_filter_kind_name(ScanFilterKind kind);
const char* scan_filter_stage_name(ScanFilterStage stage);

} // namespace doris
