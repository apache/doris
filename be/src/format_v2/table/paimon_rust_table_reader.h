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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cctz/time_zone.h"
#include "common/status.h"
#include "format_v2/table_reader.h"
#include "runtime/runtime_profile.h"

namespace arrow {
class RecordBatch;
}

namespace doris::format::paimon {

// Reads one FE-planned Paimon DataSplit per split through the paimon-rust C
// bindings (libpaimon_c), using the schema-json pipeline:
//
//   paimon_table_from_schema_json(table_path, schema_json, db, table, branch, options)
//     -> read_builder (case-insensitive, projection, filter)
//     -> paimon_plan_from_split_bytes(base64-decoded FE split)
//     -> paimon_read_builder_new_read -> paimon_table_read_to_arrow
//     -> per-batch paimon_record_batch_reader_next (Arrow C Data Interface).
//
// Leaf reader inside PaimonHybridReader: a DataSplit is a logical multi-file
// split, not a file range, so this reader does not go through
// FileReader/TableColumnMapper and fills the table-schema output block directly
// (same shape as LanceTableReader). Partition columns that paimon-rust does not
// emit are materialized from the partition values captured by the TableReader
// base class; columns with default expressions are materialized from those
// expressions.
class PaimonRustTableReader final : public format::TableReader {
public:
    // Out-of-line (in the .cpp) so TUs that construct or destroy the reader do not
    // need the complete PaimonHandles pimpl type.
    PaimonRustTableReader();
    ~PaimonRustTableReader() override;

    Status init(format::TableReadOptions&& options) override;
    Status prepare_split(const format::SplitReadOptions& options) override;
    Status get_block(Block* block, bool* eos) override;
    Status abort_split() override;
    Status close() override;
    // paimon_table_read_to_arrow has no batch-size control and the arrow reader
    // emits batches at its own granularity, so the base behavior (store the
    // value, no reader to forward to) is the intended one.

#ifdef BE_TEST
    Status TEST_validate_rust_split(const TFileRangeDesc& range) const {
        return _validate_rust_split(range);
    }
    Status TEST_fill_non_arrow_columns(
            Block* block, size_t rows,
            const std::unordered_set<size_t>& materialized_indices = {}) {
        return _fill_non_arrow_columns(block, rows, materialized_indices);
    }
    void TEST_set_projected_columns(std::vector<format::ColumnDefinition> columns) {
        _projected_columns = std::move(columns);
    }
    void TEST_set_partition_values(std::map<std::string, Field> values) {
        _partition_values = std::move(values);
    }
#endif

private:
    // Opaque holder for the paimon-rust C handles (defined in the .cpp so that
    // paimon_rust/paimon.h does not leak into other translation units).
    struct PaimonHandles;

    // Fail fast (without filesystem IO) when the schema-json pipeline fields are
    // missing from the split; the error text points at an FE/BE protocol mismatch.
    Status _validate_rust_split(const TFileRangeDesc& range) const;
    // Open (or reuse the cached) paimon_table and build the per-split read
    // pipeline: read_builder -> projection -> filter -> plan -> arrow reader.
    Status _open_split_reader(const TFileRangeDesc& range);
    // Drop the per-split pipeline (read_builder .. record batch reader), keeping
    // the cached table handle for the next split of the same table.
    void _close_split_reader();
    // Drop the cached paimon_table handle.
    void _close_table();
    // Convert _conjuncts (global-index VSlotRefs) into a paimon-rust filter and
    // apply it to the read builder. Best effort: non-convertible conjuncts are
    // dropped silently and re-applied by the scanner row-level filter.
    Status _apply_predicate();
    // Fill the output block from one arrow record batch: arrow columns are
    // matched by name (exact then lower-case, v1 semantics) onto fixed projected
    // positions, then _fill_non_arrow_columns back-fills the rest.
    Status _fill_block_from_record_batch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                         Block* block, size_t rows);
    // Materialize projected columns the arrow batch did not contain: partition
    // constants, default expressions, or a default/NULL fill as a last resort.
    Status _fill_non_arrow_columns(Block* block, size_t rows,
                                   const std::unordered_set<size_t>& materialized_indices);
    // Evaluate a constant expression (VLiteral or default_expr) on a one-row
    // block and broadcast the result to `rows` as a ColumnConst.
    Status _materialize_constant_column(const VExprContextSPtr& expr, const DataTypePtr& type,
                                        const std::string& name, size_t rows, ColumnPtr* column);

    std::optional<std::string> _resolve_table_path(const TFileRangeDesc& range) const;
    std::optional<std::string> _resolve_db_name(const TFileRangeDesc& range) const;
    std::optional<std::string> _resolve_table_name(const TFileRangeDesc& range) const;
    std::optional<std::string> _resolve_table_schema_json(const TFileRangeDesc& range) const;
    // Non-default branch name (unset means main branch, matching upstream
    // paimon commit 742da63: null-if-DEFAULT_MAIN_BRANCH).
    std::optional<std::string> _resolve_branch(const TFileRangeDesc& range) const;
    // Base64-decode the FE-planned split into the raw serialized DataSplit bytes.
    Status _decode_split_bytes(std::string* out) const;
    // Projected column names the rust reader actually reads (partition keys are
    // excluded; they are materialized from split metadata, not from arrow).
    std::vector<std::string> _build_read_columns() const;
    // Storage options: FE paimon_options + hadoop conf/properties, with the
    // OSS/S3 -> AWS_* key remap (v1 semantics).
    std::map<std::string, std::string> _build_options() const;

    std::unique_ptr<PaimonHandles> _handles; // cached table + per-split pipeline
    // Identity of the opened table so consecutive splits reuse the handle.
    std::optional<std::tuple<std::string /*table_path*/, std::string /*schema_json*/,
                             std::string /*db*/, std::string /*table*/,
                             std::optional<std::string> /*branch*/,
                             std::map<std::string, std::string> /*options*/>>
            _opened_table_key;
    // Projected column name (exact and lower-case) -> output block position.
    std::unordered_map<std::string, size_t> _output_name_to_idx;
    TFileRangeDesc _current_range;
    cctz::time_zone _ctz;
    // EOF belongs to the current split; reset when a new split is prepared.
    bool _split_eof = false;

    // Profile counters under the "PaimonRustReader" profile group
    // (file_scan_profile::TABLE_READER parent, mirroring JniTableReader).
    RuntimeProfile::Counter* _rust_total_time = nullptr;
    RuntimeProfile::Counter* _rust_open_split_time = nullptr;
    RuntimeProfile::Counter* _rust_read_batch_time = nullptr;
    RuntimeProfile::Counter* _rust_arrow_to_block_time = nullptr;
};

} // namespace doris::format::paimon
