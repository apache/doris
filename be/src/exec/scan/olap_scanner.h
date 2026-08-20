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

#include <gen_cpp/PaloInternalService_types.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "exec/scan/scanner.h"
#include "runtime/runtime_state.h"
#include "storage/data_dir.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_reader.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

struct OlapScanRange;
class FunctionFilter;
class RuntimeProfile;
class RuntimeState;
class TPaloScanRange;
class ScanLocalStateBase;
struct FilterPredicates;
#ifndef NDEBUG
struct OlapReaderStatistics;
#endif

namespace io {
struct FileCacheStatistics;
struct IOContext;
} // namespace io

class Block;

io::IOContext build_score_runtime_collection_io_context(RuntimeState* state, ReaderType reader_type,
                                                        int64_t expiration_time,
                                                        io::FileCacheStatistics* file_cache_stats);

class OlapScanner : public Scanner {
    ENABLE_FACTORY_CREATOR(OlapScanner);

public:
    struct Params {
        RuntimeState* state = nullptr;
        RuntimeProfile* profile = nullptr;
        std::vector<OlapScanRange*> key_ranges;
        BaseTabletSPtr tablet;
        int64_t version;
        TabletReadSource read_source;
        io::FileCacheStatistics initial_file_cache_stats;
        int64_t limit;
        bool aggregation;
        bool read_row_binlog = false;
        TBinlogScanType::type binlog_scan_type = TBinlogScanType::NONE;
        std::optional<int64_t> start_tso;
        std::optional<int64_t> end_tso;
    };

    OlapScanner(ScanLocalStateBase* parent, Params&& params);

    Status _prepare_impl() override;

    Status _open_impl(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    doris::TabletStorageType get_storage_type() override;

    bool check_partition_pruned() const override;

    void update_realtime_counters() override;

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;
    void _collect_profile_before_close() override;

private:
    Status _init_tablet_reader_params(
            const phmap::flat_hash_map<int, SlotDescriptor*>& slot_id_to_slot_desc,
            const std::vector<OlapScanRange*>& key_ranges,
            const phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>>&
                    predicates,
            const std::vector<FunctionFilter>& function_filters);

    [[nodiscard]] Status _init_tso_pushdown();
    [[nodiscard]] Status _init_return_columns();
    [[nodiscard]] Status _init_variant_columns();
#ifndef NDEBUG
    Status _check_ann_cache_hit_debug_points(const OlapReaderStatistics& stats);
#endif

    using CandidateKeyMap = std::unordered_map<std::string, OlapTuple>;

    struct CandidateScanCostLimit {
        int64_t full_scan_rows = 0;
        size_t point_probe_cost_per_key = 0;
        bool enabled = false;

        [[nodiscard]] bool exceeded(int64_t previous_candidate_scan_rows,
                                    int64_t current_candidate_scan_rows,
                                    size_t candidate_key_count) const;
    };

    struct CandidateMemoryBudget {
        size_t reservation_bytes = 0;
        size_t key_bytes = 0;
        size_t workspace_bytes = 0;
    };

    enum class CandidateKeyInsertResult {
        OK,
        KEY_BYTES_LIMIT,
        RESERVATION_LIMIT,
    };

    [[nodiscard]] Status _prepare_seq_map_candidate_keys();
    [[nodiscard]] Status _build_seq_map_candidate_keys(
            const std::vector<std::shared_ptr<ColumnPredicate>>& key_predicates,
            const std::map<uint32_t, std::vector<std::shared_ptr<ColumnPredicate>>>& group_drivers,
            int64_t max_candidate_keys, const CandidateMemoryBudget& memory_budget,
            const CandidateScanCostLimit& cost_limit);
    [[nodiscard]] Status _collect_seq_map_candidate_keys(
            const std::vector<std::shared_ptr<ColumnPredicate>>& driver_predicates,
            const std::vector<std::shared_ptr<ColumnPredicate>>& key_predicates,
            int64_t previous_candidate_scan_rows, bool price_point_lookups,
            int64_t max_candidate_keys, size_t max_candidate_bytes,
            size_t candidate_workspace_bytes, const CandidateScanCostLimit& cost_limit,
            CandidateKeyMap* candidate_keys, size_t* candidate_bytes, bool* limit_exceeded,
            bool* bytes_exceeded, bool* reservation_exceeded, bool* cost_exceeded);
    [[nodiscard]] Status _materialize_seq_map_point_keys(CandidateKeyMap* candidate_keys,
                                                         size_t retained_bytes,
                                                         PointKeySetSPtr* point_keys);
    [[nodiscard]] std::vector<RowSetSplits> _clone_rowset_splits() const;
    [[nodiscard]] static std::string _encode_candidate_key(const OlapTuple& key);
    [[nodiscard]] static CandidateKeyInsertResult _try_add_seq_map_candidate_key(
            std::string encoded_key, OlapTuple&& key, size_t key_column_count,
            size_t max_candidate_bytes, size_t reservation_headroom_bytes,
            CandidateKeyMap* candidate_keys, size_t* candidate_bytes);
    static void _add_seq_map_candidate_cost(uint64_t row_count, size_t segment_count,
                                            CandidateScanCostLimit* cost_limit);
    static void _merge_seq_map_candidate_stats(const OlapReaderStatistics& candidate_stats,
                                               OlapReaderStatistics* total_stats);
    [[nodiscard]] CandidateMemoryBudget _candidate_memory_budget() const;
    [[nodiscard]] static CandidateMemoryBudget _split_candidate_memory_budget(
            size_t reservation_bytes);
    [[nodiscard]] static bool _is_candidate_memory_failure(const Status& status);
    static void _record_seq_map_candidate_fallback_reason(RuntimeProfile* profile,
                                                          const std::string& fallback_reason);
    [[nodiscard]] static size_t _estimate_candidate_key_bytes(const std::string& encoded_key,
                                                              size_t key_column_count);
    [[nodiscard]] size_t _estimate_candidate_map_bytes(const CandidateKeyMap& candidate_keys) const;

    std::vector<OlapScanRange*> _key_ranges;

    TabletReader::ReaderParams _tablet_reader_params;
    std::unique_ptr<TabletReader> _tablet_reader;
    std::optional<int64_t> _start_tso;
    std::optional<int64_t> _end_tso;
    std::string _seq_map_candidate_fallback_reason;

public:
    std::vector<ColumnId> _return_columns;

    std::unordered_set<uint32_t> _tablet_columns_convert_to_null_set;
    io::FileCacheStatistics _initial_file_cache_stats;

    // This field is copied from OlapScanLocalState.
    std::map<SlotId, VExprContextSPtr> _slot_id_to_virtual_column_expr;

    // ColumnId of virtual column to its expr context
    std::map<ColumnId, VExprContextSPtr> _virtual_column_exprs;
    std::shared_ptr<ScoreRuntime> _score_runtime;

    std::shared_ptr<segment_v2::AnnTopNRuntime> _ann_topn_runtime;

    VectorSearchUserParams _vector_search_params;
};
} // namespace doris
