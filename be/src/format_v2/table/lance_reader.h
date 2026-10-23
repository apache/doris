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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "format_v2/lance/lance_record_batch_converter.h"
#include "format_v2/table_reader.h"
#include "runtime/runtime_profile.h"

struct LanceBatch;
struct LanceDataset;
struct LanceFtsQueryContext;
struct LanceScanner;

namespace doris {
class ShardedKVCache;
}

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

    Status _resolve_search_kind();
    Status _validate_external_search_request() const;
    Status _ensure_dataset_open(const TFileRangeDesc& range, bool prepare_fts_context = true);
    // Closes whatever dataset is open and opens the one this key names. One scanner serves several
    // splits and the splits of one scan may belong to different datasets; every split carries the
    // URI and version of the dataset it reads, so which dataset to open is decided by the split and
    // needs no context carried over from the previous one. Everything the previous open owned is
    // released here, so nothing dataset-scoped survives into the next dataset.
    Status _switch_dataset(const DatasetKey& key);
    Status _open_dataset(const DatasetKey& key);
    // Rejects FE fragment ids the opened snapshot lacks; lance-c would silently skip them.
    Status _check_fragment_ids(const std::vector<uint64_t>& fragment_ids) const;
    // Names the opened snapshot in execution errors, since several share one URI or version.
    std::string _snapshot_description() const;
    Status _prepare_fts_query_context();
    Status _open_scanner(const TFileRangeDesc& range);
    void _init_scanner_profile();
    Status _configure_scan_options(LanceScanner* scanner) const;
    Status _configure_normal_scan(LanceScanner* scanner, const TLanceFileDesc& lance_params) const;
    Status _configure_vector_search(LanceScanner* scanner,
                                    const TLanceFileDesc& lance_params) const;
    Status _configure_full_text_search(LanceScanner* scanner,
                                       const TLanceFileDesc& lance_params) const;
    // Add the statistics of the dataset handle currently open to the profile. The Lance-C API
    // returns an absolute snapshot of that one handle, so the value is accumulated into a running
    // total before the counter is written: a reader that opens several datasets would otherwise
    // end up reporting only the last one's numbers.
    void _collect_data_cache_statistics();
    // Keep lance-c's anonymous statistics typedef out of this header. _open_scanner installs the
    // strongly typed C callback adapter before forwarding the borrowed value here.
    static void _collect_scan_statistics(void* callback_ctx, const void* opaque_statistics);
    void _close_scanner();
    void _close_dataset();
    Status _fill_block_from_lance_batch(LanceBatch* batch, Block* block, size_t* rows);
    Status _dataset_key(const TFileRangeDesc& range, DatasetKey* key) const;

    LanceDataset* _dataset = nullptr;
    std::shared_ptr<arrow::Schema> _dataset_schema;
    LanceScanner* _scanner = nullptr;
    ShardedKVCache* _runtime_filter_cache = nullptr;
    std::optional<DatasetKey> _opened_dataset_key;
    // Sorted fragment ids of the opened snapshot, listed on the first split that names any.
    mutable std::optional<std::vector<uint64_t>> _dataset_fragment_ids;
    // The version the open dataset resolved to, which differs from the key's for version 0.
    int64_t _opened_version = 0;
    LanceRecordBatchConverter _record_batch_converter;
    size_t _scanner_batch_size = 0;
    RuntimeProfile::Counter* _planned_index_segment_count = nullptr;
    RuntimeProfile::Counter* _planned_indexed_fragment_count = nullptr;
    RuntimeProfile::Counter* _planned_flat_search_fragment_count = nullptr;
    RuntimeProfile::Counter* _dataset_open_time = nullptr;
    RuntimeProfile::Counter* _scanner_configure_time = nullptr;
    RuntimeProfile::Counter* _runtime_filter_sql_translation_time = nullptr;
    RuntimeProfile::Counter* _scanner_next_time = nullptr;
    RuntimeProfile::Counter* _arrow_to_doris_block_time = nullptr;
    RuntimeProfile::Counter* _row_id_take_read_time = nullptr;
    RuntimeProfile::Counter* _row_id_fetch_total_time = nullptr;
    RuntimeProfile::Counter* _execution_iops = nullptr;
    RuntimeProfile::Counter* _execution_requests = nullptr;
    RuntimeProfile::Counter* _execution_bytes_read = nullptr;
    // Pre-registered before installing Lance's callback so it only updates existing counters.
    std::vector<RuntimeProfile::Counter*> _scan_metric_counters;
    RuntimeProfile::Counter* _data_cache_bytes_read_from_cache = nullptr;
    RuntimeProfile::Counter* _data_cache_bytes_read_from_remote = nullptr;
    // Totals of the two counters above over every dataset this reader opens: a dataset handle
    // reports its own snapshot, and the profile reports the scan as a whole.
    uint64_t _data_cache_bytes_read_from_cache_total = 0;
    uint64_t _data_cache_bytes_read_from_remote_total = 0;
    // The URI and version of every dataset this reader has opened, in order, for the profile.
    std::string _opened_dataset_uris;
    std::string _opened_dataset_versions;
    RuntimeProfile::Counter* _index_object_loads = nullptr;
    RuntimeProfile::Counter* _index_components_loaded = nullptr;
    RuntimeProfile::Counter* _index_comparisons = nullptr;
    LanceFtsQueryContext* _fts_query_context = nullptr;
    SearchKind _search_kind = SearchKind::NORMAL;
    bool _eof = false;
};

} // namespace doris::format::lance
