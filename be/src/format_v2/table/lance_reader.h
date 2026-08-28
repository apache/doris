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

#include <cctz/time_zone.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "format_v2/table_reader.h"
#include "runtime/runtime_profile.h"

struct LanceBatch;
struct LanceDataset;
struct LanceScanner;

namespace doris {
class ShardedKVCache;
}

namespace arrow {
class Array;
class RecordBatch;
} // namespace arrow

namespace doris::format::lance {

// A FORMAT_LANCE table reader. Unlike file formats such as Parquet, a Lance split is not a
// physical-file range. It either selects fragments from a fixed snapshot or scans the whole
// latest snapshot, so the dataset is owned by this table reader and each split owns its scanner.
class LanceTableReader final : public TableReader {
public:
    ~LanceTableReader() override;

    // Fetch the schema of a Lance dataset without initializing the scan path. Version zero opens
    // the latest snapshot, which is used by backend-local TVF schema discovery.
    Status fetch_schema(const TFileRangeDesc& range, const TFileScanRangeParams& scan_params,
                        std::vector<std::string>* column_names,
                        std::vector<DataTypePtr>* column_types) const;

    Status init(TableReadOptions&& options) override;
    Status prepare_split(const SplitReadOptions& options) override;
    Status get_block(Block* block, bool* eos) override;
    // Fetch top-level projected columns by native Lance row IDs from one fixed dataset snapshot.
    // Input order and duplicates are preserved by lance-c. Missing rows are rejected because row
    // IDs produced by phase one must still exist in the same snapshot during materialization.
    Status read_by_row_ids(const TFileRangeDesc& range, const std::vector<uint64_t>& row_ids,
                           Block* block);
    Status abort_split() override;
    Status close() override;

private:
    struct DatasetKey {
        std::string uri;
        int64_t version = 0;
        std::vector<std::string> storage_options;
        bool operator==(const DatasetKey&) const = default;
    };

    Status _validate_external_search_request() const;
    Status _ensure_dataset_open(const TFileRangeDesc& range);
    Status _open_dataset(const DatasetKey& key);
    Status _open_scanner(const TFileRangeDesc& range);
    Status _configure_vector_search(LanceScanner* scanner) const;
    // Keep lance-c's anonymous statistics typedef out of this header. _open_scanner installs the
    // strongly typed C callback adapter before forwarding the borrowed value here.
    static void _collect_scan_statistics(void* callback_ctx, const void* opaque_statistics);
    void _close_scanner();
    void _close_dataset();
    Status _fill_block_from_lance_batch(LanceBatch* batch, Block* block, size_t* rows);
    Status _fill_block_from_record_batch(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                                         Block* block, size_t* rows);
    Status _append_global_row_ids(const std::shared_ptr<arrow::Array>& row_ids,
                                  MutableColumnPtr& output_column) const;
    Status _dataset_key(const TFileRangeDesc& range, DatasetKey* key) const;

    LanceDataset* _dataset = nullptr;
    LanceScanner* _scanner = nullptr;
    ShardedKVCache* _runtime_filter_cache = nullptr;
    std::optional<DatasetKey> _opened_dataset_key;
    std::unordered_map<std::string, size_t> _output_name_to_idx;
    std::optional<size_t> _global_rowid_output_idx;
    cctz::time_zone _ctz;
    size_t _scanner_batch_size = 0;
    RuntimeProfile::Counter* _planned_index_segment_count = nullptr;
    RuntimeProfile::Counter* _planned_indexed_fragment_count = nullptr;
    RuntimeProfile::Counter* _planned_flat_search_fragment_count = nullptr;
    RuntimeProfile::Counter* _dataset_open_time = nullptr;
    RuntimeProfile::Counter* _scanner_configure_time = nullptr;
    RuntimeProfile::Counter* _scanner_read_time = nullptr;
    RuntimeProfile::Counter* _arrow_to_doris_block_time = nullptr;
    RuntimeProfile::Counter* _row_id_take_read_time = nullptr;
    RuntimeProfile::Counter* _row_id_fetch_total_time = nullptr;
    RuntimeProfile::Counter* _execution_iops = nullptr;
    RuntimeProfile::Counter* _execution_requests = nullptr;
    RuntimeProfile::Counter* _execution_bytes_read = nullptr;
    RuntimeProfile::Counter* _index_partition_cache_miss_loads = nullptr;
    RuntimeProfile::Counter* _index_comparisons = nullptr;
    std::unordered_map<std::string_view, RuntimeProfile::Counter*> _lance_count_metrics;
    std::unordered_map<std::string_view, RuntimeProfile::Counter*> _lance_time_metrics;
    bool _vector_search = false;
    bool _eof = false;
};

} // namespace doris::format::lance
