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

namespace arrow {
class Array;
class RecordBatch;
class Schema;
} // namespace arrow

namespace doris::format::lance {

// Convert every top-level field without discarding unsupported columns. Malformed schemas still
// return an error and leave both output vectors unchanged. DataTypeNothing is the local sentinel
// for a valid Arrow field whose logical type Doris does not support.
Status convert_arrow_schema_to_doris(const std::shared_ptr<arrow::Schema>& arrow_schema,
                                     std::vector<std::string>* column_names,
                                     std::vector<DataTypePtr>* column_types);

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
    void _close_scanner();
    void _close_dataset();
    Status _fill_block_from_lance_batch(LanceBatch* batch, Block* block, size_t* rows);
    Status _fill_block_from_record_batch(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                                         Block* block, size_t* rows);
    Status _append_global_row_ids(const std::shared_ptr<arrow::Array>& row_ids,
                                  MutableColumnPtr& output_column) const;
    static std::vector<std::string> _storage_options(const TFileScanRangeParams* scan_params);
    DatasetKey _dataset_key(const TFileRangeDesc& range) const;
    static Status _lance_error(std::string_view operation);

    LanceDataset* _dataset = nullptr;
    LanceScanner* _scanner = nullptr;
    std::optional<DatasetKey> _opened_dataset_key;
    std::unordered_map<std::string, size_t> _output_name_to_idx;
    std::optional<size_t> _global_rowid_output_idx;
    cctz::time_zone _ctz;
    size_t _scanner_batch_size = 0;
    RuntimeProfile::Counter* _fragment_count = nullptr;
    bool _vector_search = false;
    bool _eof = false;
};

} // namespace doris::format::lance
