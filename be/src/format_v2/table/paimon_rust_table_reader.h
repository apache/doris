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
#include "gen_cpp/PlanNodes_types.h" // TVectorMetric
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

    // How paimon-rust encoded the distance into its __paimon_search_score field, and
    // hence which transform the reader has to undo before the value lands in the
    // __paimon_search_score_to_dis block column. One enumerator per metric Doris can
    // push down (v1: l2 / inner_product; cosine is rejected by the FE rule and by
    // parse_score_transform, so it deliberately has no enumerator here).
    //
    // Adding a metric is meant to be compiler-guided: add the enumerator, and the
    // switch in _convert_score_to_distance stops compiling (-Wswitch is an error in
    // be/src) until its inverse is written. Do not add a `default:` to that switch
    // or that guarantee is lost.
    enum class ScoreTransform {
        // score = 1/(1 + squaredL2). Inverse: sqrt(1/score - 1).
        L2,
        // score = the inner product itself. Inverse: identity.
        InnerProduct,
    };

    // Map an Arrow field name onto the block column name that receives its values.
    //
    // paimon-rust resolves projections case-insensitively, so the Arrow batch carries
    // the table's canonical field casing (e.g. `is_QT`) while block columns are
    // FE-normalized to lowercase (`is_qt`). Fold the Rust-side name down so the two match.
    //
    // The vector-search field is additionally renamed: paimon-rust calls it
    // kPaimonRustScoreField ("__paimon_search_score") and puts a score in it, but the
    // reader converts that to a distance in place, so FE asks for the column as
    // kPaimonSearchDistanceColumn ("__paimon_search_score_to_dis"). The fill loop
    // skips Arrow fields it cannot match (they may legitimately be absent from the
    // block), so without this alias the rename would not fail loudly -- the distance
    // column would just come back empty.
    static std::string arrow_field_to_block_column(const std::string& arrow_name);

    // Map a thrift TVectorMetric onto its score transform. Unknown or unsupported
    // metrics (including COSINE) are an error rather than a silent fallback: guessing
    // would return wrong distances instead of failing the query. Takes the thrift enum
    // directly so there is no string round-trip between FE and the ScoreTransform enum.
    static Status parse_score_transform(TVectorMetric::type metric, ScoreTransform* transform);

    // Rewrite the reader-produced __paimon_search_score_to_dis column in place,
    // turning paimon-rust's score back into the distance the Doris function is
    // defined to return (see ScoreTransform for the per-metric formulas).
    //
    // Only rewrites [row_offset, row_offset + num_rows): block columns are
    // append-only and a block is reused across get_block calls, so starting at 0
    // would square-root earlier rows again. Depends on nothing but its arguments,
    // hence static and public (also what makes it unit-testable without the
    // paimon-rust FFI handles).
    //
    // A score outside l2's legal (0, 1] range is index corruption, and computing
    // the inverse anyway would yield a non-finite distance (see the two sentinels
    // below). Such rows are emitted as a NEGATIVE distance instead: negative is
    // unreachable for a genuine Euclidean distance, so it cannot be confused with
    // a real result, it is totally ordered (unlike NaN), and it stays finite, so
    // the coordinator's Top-N comparisons remain well-defined. Rows land at the
    // very front of an ASC Top-K -- deliberately: a corrupt index should be
    // visible in the result, not averaged into it.
    static constexpr float kCorruptScoreAboveOne = -1.0F;
    static constexpr float kCorruptScoreNotPositive = -2.0F;
    // paimon-rust's lumina kernel computes the squared L2 distance as
    // ||a||^2 + ||b||^2 - 2<a,b> (vectorized dot), which suffers catastrophic
    // cancellation when a ~= b: the three terms nearly cancel and the residual
    // can be a small NEGATIVE number, so score = 1/(1+d^2) lands slightly ABOVE 1
    // (e.g. 1.00294). That is a numerical artifact for an ~exact hit, not index
    // corruption -- clamp it to distance 0. Only a score clearly past 1 (beyond
    // this tolerance) is treated as a genuine corrupt row and sentineled.
    static constexpr float kScoreAboveOneTolerance = 1e-2F;
    static void _convert_score_to_distance(Block* block, uint32_t block_idx, size_t row_offset,
                                           size_t num_rows, ScoreTransform transform);

    Status init(format::TableReadOptions&& options) override;
    // Each split's Rust read builder applies the batch size stored by the base class
    // before opening the Arrow reader.
    Status prepare_split(const format::SplitReadOptions& options) override;
    Status get_block(Block* block, bool* eos) override;
    Status abort_split() override;
    Status close() override;

#ifdef BE_TEST
    Status TEST_validate_rust_split(const TFileRangeDesc& range) const {
        return _validate_rust_split(range);
    }
    Status TEST_fill_non_arrow_columns(
            Block* block, size_t rows,
            const std::unordered_set<size_t>& materialized_indices = {}) {
        return _fill_non_arrow_columns(block, rows, materialized_indices);
    }
    // The direct-path bounded-string truncation, exposed for unit tests.
    Status TEST_truncate_char_or_varchar_columns(Block* block) {
        return _truncate_char_or_varchar_columns(block);
    }
    void TEST_set_projected_columns(std::vector<format::ColumnDefinition> columns) {
        _projected_columns = std::move(columns);
    }
    void TEST_set_partition_values(std::map<std::string, Field> values) {
        _partition_values = std::move(values);
    }
    // The session timezone the reader materializes TIMESTAMP_LTZ with.
    const cctz::time_zone& TEST_ctz() const { return _ctz; }
    // Exposed for unit tests so the keys-only option-logging contract can be
    // checked without a live table open.
    static std::string TEST_format_options(const std::map<std::string, std::string>& options);
    // Storage options the reader would build for the given FE-planned scan
    // params and split (S3 key remapping, provider modes, scheme dispatch);
    // both must outlive the call. Exposed for unit tests.
    std::map<std::string, std::string> TEST_build_options(TFileScanRangeParams* scan_params,
                                                          const TFileRangeDesc& range);
#endif

private:
    // Opaque holder for the paimon-rust C handles (defined in the .cpp so that
    // paimon_rust/paimon.h does not leak into other translation units).
    struct PaimonHandles;

    // Fail fast (without filesystem IO) when the schema-json pipeline fields are
    // missing from the split; the error text points at an FE/BE protocol mismatch.
    Status _validate_rust_split(const TFileRangeDesc& range) const;
    // Validate the external_search_request thrift fields before any use (mirrors
    // LanceTableReader::_validate_external_search_request): one pass over __isset
    // and value-range checks so the vector build path can use the fields unchecked.
    Status _validate_external_search_request() const;
    // True when this split carries a PK-vector search payload and the reader must
    // open the vector-search read path instead of the normal split read.
    static bool _is_vector_mode(const TFileRangeDesc& range);
    // Resolve the table root path + schema json and open (or reuse the cached)
    // paimon_table handle from them. Shared by the normal-read and vector-search
    // build paths; fills _handles->table and _opened_table_key.
    Status _open_paimon_table(const TFileRangeDesc& range);
    // Open (or reuse the cached) paimon_table and build the per-split read
    // pipeline: read_builder -> projection -> filter -> plan -> arrow reader.
    Status _open_split_reader(const TFileRangeDesc& range);
    // Open the table and build a primary-key vector (ANN) search reader over the
    // FE-planned bucket split. Used when the scan range carries a
    // TPaimonVectorPayload: the returned Arrow stream is best-first and carries the
    // reader-produced distance column (__paimon_search_score_to_dis, converted
    // from paimon-rust's __paimon_search_score field).
    Status _open_vector_split_reader(const TFileRangeDesc& range);
    // Fill the output block from one arrow record batch: arrow columns are
    // matched by name (exact then lower-case, v1 semantics) onto fixed projected
    // positions, then _fill_non_arrow_columns back-fills the rest.
    Status _fill_block_from_record_batch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                         Block* block, size_t rows);
    // Drop the per-split pipeline (read_builder .. record batch reader), keeping
    // the cached table handle for the next split of the same table.
    void _close_split_reader();
    // Drop the cached paimon_table handle.
    void _close_table();
    // Convert _conjuncts (global-index VSlotRefs) into a paimon-rust filter and
    // apply it to the read builder. Best effort: non-convertible conjuncts are
    // dropped silently and re-applied by the scanner row-level filter.
    Status _apply_predicate();
    // Materialize projected columns the arrow batch did not contain: partition
    // constants, default expressions, or a default/NULL fill as a last resort.
    Status _fill_non_arrow_columns(Block* block, size_t rows,
                                   const std::unordered_set<size_t>& materialized_indices);
    // Enforce truncate_char_or_varchar_columns on the direct Arrow path: the
    // pinned rust reader maps paimon CHAR(n)/VARCHAR(n) to lengthless Arrow
    // Utf8, so a column narrowed by schema evolution would return untruncated
    // historical values here while the normal (finalize_chunk) path truncates.
    // The block's own column types carry the table-side bounds (the reader
    // builds them from the FE slot types); the arrow side is always boundless,
    // so every bounded CHAR/VARCHAR column truncates to its declared length.
    Status _truncate_char_or_varchar_columns(Block* block);
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
    std::optional<
            std::tuple<std::string /*table_path*/, std::string /*schema_json*/, std::string /*db*/,
                       std::string /*table*/, std::optional<std::string> /*branch*/,
                       std::map<std::string, std::string> /*options*/>>
            _opened_table_key;
    // Projected column name (exact and lower-case) -> output block position.
    std::unordered_map<std::string, size_t> _output_name_to_idx;
    TFileRangeDesc _current_range;
    cctz::time_zone _ctz;
    // Which score transform the incoming __paimon_search_score field needs undone,
    // derived from the FE-supplied metric when the vector reader is built. Empty
    // outside vector mode (and only then): an unset or unknown metric fails the
    // scan rather than defaulting, since guessing l2 would wrongly sqrt
    // inner_product results.
    std::optional<ScoreTransform> _score_transform;
    // EOF belongs to the current split; reset when a new split is prepared.
    bool _split_eof = false;

    // Profile counters under the "PaimonRustReader" profile group
    // (file_scan_profile::TABLE_READER parent, mirroring JniTableReader).
    RuntimeProfile::Counter* _rust_predicates_input = nullptr;
    RuntimeProfile::Counter* _rust_predicates_converted = nullptr;
    RuntimeProfile::Counter* _rust_predicates_applied = nullptr;
    RuntimeProfile::Counter* _rust_runtime_filters_input = nullptr;
    RuntimeProfile::Counter* _rust_runtime_filters_applied = nullptr;
    RuntimeProfile::Counter* _rust_total_time = nullptr;
    RuntimeProfile::Counter* _rust_open_split_time = nullptr;
    RuntimeProfile::Counter* _rust_read_batch_time = nullptr;
    RuntimeProfile::Counter* _rust_arrow_to_block_time = nullptr;
};

} // namespace doris::format::paimon
