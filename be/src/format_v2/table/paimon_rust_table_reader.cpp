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

#include "format_v2/table/paimon_rust_table_reader.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <string_view>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "common/compiler_util.h"
#include "common/logging.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "exec/common/endian.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "format_v2/column_mapper.h"
#include "format_v2/table/paimon_rust_predicate_converter.h"
#include "runtime/descriptors.h"
#include "runtime/file_scan_profile.h"
#include "runtime/runtime_state.h"
#include "util/string_util.h"
#include "util/timezone_utils.h"
#include "util/url_coding.h"

extern "C" {
#include "paimon_rust/paimon.h"
}

namespace doris::format::paimon {

namespace {
constexpr const char* VALUE_KIND_FIELD = "_VALUE_KIND";

// ---------------------------------------------------------------------------
// RAII wrappers over the paimon-rust C handles. Each handle is an opaque
// pointer owned by Rust and released by a matching paimon_*_free function.
// ---------------------------------------------------------------------------
#define PAIMON_OWNED(type, freefn)                \
    struct type##_deleter {                       \
        void operator()(paimon_##type* p) const { \
            if (p) {                              \
                freefn(p);                        \
            }                                     \
        }                                         \
    };                                            \
    using type##_ptr = std::unique_ptr<paimon_##type, type##_deleter>

PAIMON_OWNED(table, paimon_table_free);
PAIMON_OWNED(read_builder, paimon_read_builder_free);
PAIMON_OWNED(plan, paimon_plan_free);
PAIMON_OWNED(table_read, paimon_table_read_free);
PAIMON_OWNED(record_batch_reader, paimon_record_batch_reader_free);
PAIMON_OWNED(vector_search_builder, paimon_vector_search_builder_free);
// Local handles of the eager Top-K vector-search chain (paimon-rust #822): they only
// need to live until the arrow reader is created, so unlike the table/reader handles
// above they are function locals of _open_vector_split_reader rather than members
// of PaimonHandles.
PAIMON_OWNED(bucket_vector_search_split, paimon_bucket_vector_search_split_free);
PAIMON_OWNED(vector_scan, paimon_vector_scan_free);
PAIMON_OWNED(vector_plan, paimon_vector_plan_free);
PAIMON_OWNED(vector_read, paimon_vector_read_free);
PAIMON_OWNED(error, paimon_error_free);

#undef PAIMON_OWNED

// One Arrow batch (schema + array containers). Owning it requires a two-step
// teardown that the unique_ptr deleters above can't express: first invoke the
// Arrow C Data Interface `release` callback on each struct (hands buffers back
// to the producer), then free the container structs via paimon_arrow_batch_free.
class ArrowBatch {
public:
    explicit ArrowBatch(paimon_arrow_batch batch) : batch_(batch) {}
    ~ArrowBatch() {
        auto* schema = static_cast<ArrowSchema*>(batch_.schema);
        auto* array = static_cast<ArrowArray*>(batch_.array);
        if (array && array->release) {
            array->release(array);
        }
        if (schema && schema->release) {
            schema->release(schema);
        }
        paimon_arrow_batch_free(batch_);
    }

    ArrowBatch(const ArrowBatch&) = delete;
    ArrowBatch& operator=(const ArrowBatch&) = delete;

    ArrowSchema* schema() const { return static_cast<ArrowSchema*>(batch_.schema); }
    ArrowArray* array() const { return static_cast<ArrowArray*>(batch_.array); }

private:
    paimon_arrow_batch batch_;
};

// Render a paimon_error into a string. Takes ownership of `err` via RAII so it
// is freed on every return path. Safe to call with nullptr.
std::string consume_error(paimon_error* err) {
    error_ptr owned(err);
    if (!owned) {
        return "unknown error";
    }
    std::string msg;
    if (owned->message.data != nullptr && owned->message.len > 0) {
        msg.assign(reinterpret_cast<const char*>(owned->message.data), owned->message.len);
    }
    return "code=" + std::to_string(owned->code) + ", msg=" + msg;
}

// Render storage option KEYS for diagnostics. Values are never rendered:
// credential keys arrive under many spellings and cases (AWS_SECRET_KEY,
// AWS_TOKEN, fs.oss.accessKeySecret, s3.secret-key, ...), and a key-name
// blocklist that misses one alias leaks the value into the INFO log, so
// only the key names are printed at all.
std::string format_options(const std::map<std::string, std::string>& options) {
    std::string out;
    for (const auto& kv : options) {
        if (!out.empty()) {
            out += ", ";
        }
        out += kv.first;
    }
    return out;
}

} // namespace

// Paimon-rust handles. Order of members matters: destruction runs in reverse
// declaration order, and the read_builder depends on the table while the arrow
// reader depends on the whole pipeline above it. So the table MUST be declared
// first (destroyed last) and the record batch reader last.
struct PaimonRustTableReader::PaimonHandles {
    table_ptr table;
    read_builder_ptr read_builder;
    plan_ptr plan;
    table_read_ptr table_read;
    record_batch_reader_ptr reader;
};

PaimonRustTableReader::PaimonRustTableReader() = default;

PaimonRustTableReader::~PaimonRustTableReader() = default;

Status PaimonRustTableReader::init(format::TableReadOptions&& options) {
    RETURN_IF_ERROR(format::TableReader::init(std::move(options)));
    {
        // Base and derived scopes must not overlap on the same counter: RuntimeProfile timers
        // add deltas, so nested use would double-count instead of extending lifecycle coverage.
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.init_timer);
        // Materialize TIMESTAMP_LTZ in the session timezone — the same
        // convention as the JNI reader (PaimonJniScanner reads time_zone from
        // its scan params) and lance_reader. Timezone-naive (paimon TIMESTAMP)
        // arrow values are decoded in UTC by the DateTimeV2 serde regardless
        // of _ctz, so NTZ wall-clock semantics are preserved.
        DORIS_CHECK(_runtime_state != nullptr);
        _ctz = _runtime_state->timezone_obj();
        if (_scanner_profile != nullptr) {
            file_scan_profile::ensure_hierarchy(_scanner_profile);
            _rust_total_time = ADD_CHILD_TIMER(_scanner_profile, "PaimonRustReader",
                                               file_scan_profile::TABLE_READER);
            _rust_open_split_time =
                    ADD_CHILD_TIMER(_scanner_profile, "OpenSplitTime", "PaimonRustReader");
            _rust_read_batch_time =
                    ADD_CHILD_TIMER(_scanner_profile, "ReadBatchTime", "PaimonRustReader");
            _rust_arrow_to_block_time =
                    ADD_CHILD_TIMER(_scanner_profile, "ArrowToBlockTime", "PaimonRustReader");
            _rust_predicates_input = ADD_CHILD_COUNTER(_scanner_profile, "RustPredicatesInput",
                                                       TUnit::UNIT, "PaimonRustReader");
            _rust_predicates_converted = ADD_CHILD_COUNTER(
                    _scanner_profile, "RustPredicatesConverted", TUnit::UNIT, "PaimonRustReader");
            _rust_predicates_applied = ADD_CHILD_COUNTER(_scanner_profile, "RustPredicatesApplied",
                                                         TUnit::UNIT, "PaimonRustReader");
            _rust_runtime_filters_input = ADD_CHILD_COUNTER(
                    _scanner_profile, "RustRuntimeFiltersInput", TUnit::UNIT, "PaimonRustReader");
            _rust_runtime_filters_applied = ADD_CHILD_COUNTER(
                    _scanner_profile, "RustRuntimeFiltersApplied", TUnit::UNIT, "PaimonRustReader");
        }
        // Projected column name -> fixed output position, registered with both the exact and
        // the lower-case spelling so mixed-case Rust schema output still resolves (v1
        // semantics: exact match first, lower-case fallback on lookup).
        _output_name_to_idx.reserve(_projected_columns.size() * 2);
        for (size_t idx = 0; idx < _projected_columns.size(); ++idx) {
            _output_name_to_idx.emplace(_projected_columns[idx].name, idx);
            _output_name_to_idx.emplace(to_lower(_projected_columns[idx].name), idx);
        }
    }
    return Status::OK();
}

Status PaimonRustTableReader::prepare_split(const format::SplitReadOptions& options) {
    // EOF belongs to the previous split. Keep it set after closing that split so repeated reads
    // are idempotent, and clear it only when a new split is explicitly prepared.
    _close_split_reader();
    _split_eof = false;
    _current_range = options.current_range;
    RETURN_IF_ERROR(format::TableReader::prepare_split(options));
    if (current_split_pruned()) {
        return Status::OK();
    }
    if (_is_table_level_count_active()) {
        // No rust pipeline is opened; get_block emits the synthetic count rows.
        return Status::OK();
    }
    RETURN_IF_ERROR(_validate_rust_split(options.current_range));
    {
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.prepare_split_timer);
        // Open may dominate the scan or fail before get_block; include it in the parent timer.
        SCOPED_TIMER(_rust_total_time);
        SCOPED_TIMER(_rust_open_split_time);
        RETURN_IF_ERROR(_open_split_reader(options.current_range));
    }
    return Status::OK();
}

Status PaimonRustTableReader::get_block(Block* block, bool* eos) {
    SCOPED_TIMER(_profile.total_timer);
    SCOPED_TIMER(_profile.exec_timer);
    SCOPED_TIMER(_rust_total_time);
    DORIS_CHECK(block != nullptr);
    DORIS_CHECK(eos != nullptr);
    DORIS_CHECK(block->columns() == _projected_columns.size());
    block->clear_column_data(_projected_columns.size());
    *eos = false;

    if (_is_table_level_count_active()) {
        return _read_table_level_count(block, eos);
    }

    // num_splits == 0 yields an empty (but valid) stream: report EOF.
    if (_split_eof) {
        *eos = true;
        return Status::OK();
    }
    if (!_handles || !_handles->reader) {
        return Status::InternalError("paimon-rust reader is not initialized");
    }

    while (true) {
        // Mirror the base TableReader cancellation contract so a cancelled query does not
        // drain the whole split.
        if (_io_ctx != nullptr && _io_ctx->should_stop) {
            _split_eof = true;
            _close_split_reader();
            *eos = true;
            return Status::OK();
        }

        paimon_result_next_batch next;
        {
            SCOPED_TIMER(_rust_read_batch_time);
            next = paimon_record_batch_reader_next(_handles->reader.get());
        }
        if (next.error != nullptr) {
            return Status::InternalError("paimon-rust read batch failed: {}",
                                         consume_error(next.error));
        }
        // End of stream: both pointers are null.
        if (next.batch.array == nullptr && next.batch.schema == nullptr) {
            _split_eof = true;
            _close_split_reader();
            *eos = true;
            return Status::OK();
        }

        // RAII: the batch's Arrow release callbacks + container free run when
        // `batch` leaves this scope, including on any early return.
        ArrowBatch batch(next.batch);

        auto* c_array = batch.array();
        auto* c_schema = batch.schema();
        arrow::Result<std::shared_ptr<arrow::RecordBatch>> import_result =
                arrow::ImportRecordBatch(c_array, c_schema);
        if (!import_result.ok()) {
            return Status::InternalError("failed to import paimon-rust arrow batch: {}",
                                         import_result.status().message());
        }

        auto record_batch = std::move(import_result).ValueUnsafe();
        const auto rows = static_cast<size_t>(record_batch->num_rows());
        if (rows == 0) {
            // Skip empty batches and keep draining the stream.
            continue;
        }
        RETURN_IF_ERROR(_fill_block_from_record_batch(record_batch, block, rows));
        _record_scan_rows(rows);
        *eos = false;
        return Status::OK();
    }
}

Status PaimonRustTableReader::abort_split() {
    {
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.close_timer);
        _close_split_reader();
        _split_eof = false;
    }
    return format::TableReader::abort_split();
}

#ifdef BE_TEST
std::string PaimonRustTableReader::TEST_format_options(
        const std::map<std::string, std::string>& options) {
    return format_options(options);
}

std::map<std::string, std::string> PaimonRustTableReader::TEST_build_options(
        TFileScanRangeParams* scan_params, const TFileRangeDesc& range) {
    TFileScanRangeParams* previous_params = _scan_params;
    TFileRangeDesc previous_range = _current_range;
    _scan_params = scan_params;
    _current_range = range;
    std::map<std::string, std::string> options = _build_options();
    _scan_params = previous_params;
    _current_range = std::move(previous_range);
    return options;
}
#endif

Status PaimonRustTableReader::close() {
    {
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.close_timer);
        _close_split_reader();
        _close_table();
    }
    return format::TableReader::close();
}

Status PaimonRustTableReader::_validate_rust_split(const TFileRangeDesc& range) const {
    if (!range.__isset.table_format_params || !range.table_format_params.__isset.paimon_params) {
        return Status::InternalError(
                "missing paimon_params for paimon rust reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    const auto& params = range.table_format_params.paimon_params;
    if (params.__isset.reader_type && params.reader_type != TPaimonReaderType::PAIMON_RUST) {
        return Status::InternalError(
                "invalid reader_type for paimon rust reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    // Vector (ANN) mode: the payload is the serialized BucketVectorSearchSplit, and
    // paimon_split is deliberately an empty string (FE avoids serializing the same
    // DataSplit twice on the wire). An empty-but-set paimon_split therefore also
    // implies vector mode: reporting "missing paimon_split" for such a split would
    // point the reader at the wrong field when the real gap is the vector payload.
    if (params.__isset.vector_payload ||
        (params.__isset.paimon_split && params.paimon_split.empty())) {
        if (!params.__isset.vector_payload || params.vector_payload.bytes.empty()) {
            return Status::InternalError(
                    "missing vector_payload for paimon rust vector reader, possibly caused by "
                    "FE/BE protocol mismatch");
        }
    } else if (!params.__isset.paimon_split || params.paimon_split.empty()) {
        return Status::InternalError(
                "missing paimon_split for paimon rust reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    if (!_resolve_table_path(range).has_value()) {
        return Status::InternalError(
                "paimon-rust missing paimon_table; cannot resolve paimon table location");
    }
    if (!_resolve_db_name(range).has_value()) {
        return Status::InternalError(
                "paimon-rust missing db_name; cannot open paimon table via schema json");
    }
    if (!_resolve_table_name(range).has_value()) {
        return Status::InternalError(
                "paimon-rust missing table_name; cannot open paimon table via schema json");
    }
    if (!_resolve_table_schema_json(range).has_value()) {
        return Status::InternalError(
                "paimon-rust missing paimon_table_schema_json; cannot open paimon table via "
                "schema json");
    }
    return Status::OK();
}

bool PaimonRustTableReader::_is_vector_mode(const TFileRangeDesc& range) {
    return range.__isset.table_format_params && range.table_format_params.__isset.paimon_params &&
           range.table_format_params.paimon_params.__isset.vector_payload;
}

Status PaimonRustTableReader::_validate_external_search_request() const {
    if (!_scan_params || !_scan_params->__isset.external_search_request) {
        return Status::InvalidArgument(
                "paimon-rust vector search missing external_search_request in scan params");
    }
    const auto& request = _scan_params->external_search_request;
    if (request.__isset.schema_version && request.schema_version != 1) {
        return Status::NotSupported("unsupported external search schema version: {}",
                                    request.schema_version);
    }
    if (!request.__isset.search_query || !request.search_query.__isset.vector_search) {
        return Status::InvalidArgument(
                "paimon-rust vector search requires a vector query in external_search_request");
    }

    const auto& vector = request.search_query.vector_search;
    // column
    if (!vector.__isset.column || vector.column.empty() ||
        vector.column.find('\0') != std::string::npos) {
        return Status::InvalidArgument("paimon-rust vector search requires a non-empty column");
    }
    // query_vector (TSearchVector)
    if (!vector.__isset.query_vector) {
        return Status::InvalidArgument("paimon-rust vector search requires a query vector");
    }
    const auto& query_vector = vector.query_vector;
    if (!query_vector.__isset.element_type || !query_vector.__isset.dimension ||
        !query_vector.__isset.values) {
        return Status::InvalidArgument(
                "paimon-rust query vector requires element_type, dimension, and values");
    }
    if (query_vector.dimension <= 0) {
        return Status::InvalidArgument("paimon-rust query vector dimension must be positive: {}",
                                       query_vector.dimension);
    }
    if (query_vector.element_type != TVectorElementType::FLOAT32) {
        return Status::NotSupported(
                "paimon-rust vector search only supports FLOAT32 query vector, got {}",
                static_cast<int>(query_vector.element_type));
    }
    const auto dimension = static_cast<size_t>(query_vector.dimension);
    if (query_vector.values.size() != dimension * sizeof(float)) {
        return Status::InvalidArgument(
                "paimon-rust query vector byte size {} does not match dimension {} * {}",
                query_vector.values.size(), dimension, sizeof(float));
    }
    // top_k / offset
    if (!vector.__isset.top_k || vector.top_k < 0) {
        return Status::InvalidArgument("paimon-rust vector search top_k must be non-negative");
    }
    if (!vector.__isset.offset || vector.offset < 0) {
        return Status::InvalidArgument("paimon-rust vector search offset must be non-negative");
    }
    // metric is required (NOT advisory): BE uses it to undo the score transform.
    // paimon-rust only supports L2 and DOT_PRODUCT; parse_score_transform rejects
    // anything else, so validate __isset here and let parse_score_transform handle
    // the value.
    if (!vector.__isset.metric) {
        return Status::InvalidArgument("paimon-rust vector search requires a metric");
    }
    // paimon_options (optional): only validate the map itself is present if set.
    if (request.__isset.paimon_options && request.paimon_options.__isset.options) {
        for (const auto& kv : request.paimon_options.options) {
            if (kv.first.find('\0') != std::string::npos ||
                kv.second.find('\0') != std::string::npos) {
                return Status::InvalidArgument(
                        "paimon-rust vector search options contain an embedded NUL byte");
            }
        }
    }
    return Status::OK();
}

Status PaimonRustTableReader::_open_paimon_table(const TFileRangeDesc& range) {
    // Resolve identifier + table_path + FE-supplied TableSchema JSON.
    auto table_path = _resolve_table_path(range).value();
    auto db_name = _resolve_db_name(range).value();
    auto table_name = _resolve_table_name(range).value();
    auto schema_json = _resolve_table_schema_json(range).value();
    auto branch_opt = _resolve_branch(range);

    // Assemble storage options: FE-supplied paimon options + hadoop_conf +
    // OSS/S3 → AWS_* translations. These feed FileIO only (per
    // paimon_table_from_schema_json contract); they are NOT merged into the
    // supplied table schema.
    auto options = _build_options();

    auto opened_table_key =
            std::make_tuple(table_path, schema_json, db_name, table_name, branch_opt, options);
    if (_handles && _handles->table && _opened_table_key == opened_table_key) {
        // A paimon scan reads one table, so the handle is opened at most once per
        // distinct identity; splits of the same table reuse it and only rebuild
        // the read pipeline.
        return Status::OK();
    }
    _close_table();
    _handles = std::make_unique<PaimonHandles>();

    std::vector<paimon_option> c_options;
    c_options.reserve(options.size());
    for (const auto& kv : options) {
        c_options.push_back(paimon_option {kv.first.c_str(), kv.second.c_str()});
    }

    LOG(INFO) << "paimon-rust opening table via schema json: db=" << db_name
              << " table=" << table_name << " path=" << table_path
              << " branch=" << (branch_opt.has_value() ? branch_opt.value() : "main")
              << " storage_options=[" << format_options(options) << "]";

    // Build the table directly from the FE-supplied schema JSON. The Rust
    // side rejects null / empty branch, so we default to paimon's canonical
    // "main" sentinel when FE did not set paimon_branch (i.e. the table is
    // on the main branch — matches upstream Identifier.DEFAULT_MAIN_BRANCH).
    const std::string& branch_str = branch_opt.has_value() ? branch_opt.value() : "main";
    paimon_result_get_table tbl_res = paimon_table_from_schema_json(
            table_path.c_str(), schema_json.c_str(), db_name.c_str(), table_name.c_str(),
            branch_str.c_str(), c_options.empty() ? nullptr : c_options.data(), c_options.size());
    if (tbl_res.error != nullptr) {
        return Status::InternalError(
                "paimon-rust table_from_schema_json failed: db={} table={} err={}", db_name,
                table_name, consume_error(tbl_res.error));
    }
    _handles->table.reset(tbl_res.table);
    _opened_table_key = std::move(opened_table_key);
    return Status::OK();
}

Status PaimonRustTableReader::_open_split_reader(const TFileRangeDesc& range) {
    // Primary-key vector (ANN) search: route to the vector-search read path.
    if (_is_vector_mode(range)) {
        return _open_vector_split_reader(range);
    }
    // 1. Decode the FE-planned split first so we fail fast (and without any
    // filesystem IO) when it is missing or malformed.
    std::string split_bytes;
    RETURN_IF_ERROR(_decode_split_bytes(&split_bytes));

    // 2. Resolve and open (or reuse) the table.
    RETURN_IF_ERROR(_open_paimon_table(range));

    // 4. Build the read pipeline: read_builder -> case-insensitive -> projection.
    // Bound the Rust Arrow allocation itself: wide rows must respect the scanner's
    // adaptive probe size before they are materialized into a Doris block.
    const auto batch_size = std::to_string(
            _batch_size > 0 ? _batch_size : std::max(1, _runtime_state->batch_size()));
    const paimon_option batch_option {"read.batch-size", batch_size.c_str()};
    paimon_result_read_builder rb_res =
            paimon_table_new_read_builder_with_options(_handles->table.get(), &batch_option, 1);
    if (rb_res.error != nullptr) {
        return Status::InternalError("paimon-rust new read builder failed: {}",
                                     consume_error(rb_res.error));
    }
    _handles->read_builder.reset(rb_res.read_builder);

    // Fold column casing on the Rust side so FE-normalized lowercase names
    // resolve against tables with mixed-case column definitions.
    if (paimon_error* case_err =
                paimon_read_builder_with_case_sensitive(_handles->read_builder.get(), false)) {
        return Status::InternalError("paimon-rust set case_sensitive failed: {}",
                                     consume_error(case_err));
    }

    // Partition keys are excluded: they are materialized from split metadata
    // (see _fill_non_arrow_columns), and paimon-rust does not emit them.
    auto read_columns = _build_read_columns();
    std::vector<const char*> projection;
    projection.reserve(read_columns.size() + 1);
    for (const auto& col : read_columns) {
        projection.push_back(col.c_str());
    }
    projection.push_back(nullptr);
    if (paimon_error* proj_err = paimon_read_builder_with_projection(_handles->read_builder.get(),
                                                                     projection.data())) {
        return Status::InternalError("paimon-rust set projection failed: {}",
                                     consume_error(proj_err));
    }

    // Convert the scanner conjuncts into a paimon-rust filter and apply it.
    RETURN_IF_ERROR(_apply_predicate());

    // 5. Deserialize the FE-planned split into a one-split plan, so this
    // scanner reads exactly the split it was assigned rather than replanning
    // the whole table. The wire form is identical to what paimon-cpp consumes
    // (`paimon::table::DataSplit::serialize`).
    paimon_result_plan plan_res = paimon_plan_from_split_bytes(
            reinterpret_cast<const uint8_t*>(split_bytes.data()), split_bytes.size());
    if (plan_res.error != nullptr) {
        return Status::InternalError("paimon-rust build plan failed: {}",
                                     consume_error(plan_res.error));
    }
    _handles->plan.reset(plan_res.plan);

    size_t num_splits = paimon_plan_num_splits(_handles->plan.get());
    if (num_splits == 0) {
        _split_eof = true;
        return Status::OK();
    }

    // 6. Open the arrow stream over the plan.
    paimon_result_new_read read_res = paimon_read_builder_new_read(_handles->read_builder.get());
    if (read_res.error != nullptr) {
        return Status::InternalError("paimon-rust new read failed: {}",
                                     consume_error(read_res.error));
    }
    _handles->table_read.reset(read_res.read);

    paimon_result_record_batch_reader rdr_res = paimon_table_read_to_arrow(
            _handles->table_read.get(), _handles->plan.get(), /*offset=*/0, /*length=*/num_splits);
    if (rdr_res.error != nullptr) {
        return Status::InternalError("paimon-rust open arrow reader failed: {}",
                                     consume_error(rdr_res.error));
    }
    _handles->reader.reset(rdr_res.reader);
    return Status::OK();
}

Status PaimonRustTableReader::_open_vector_split_reader(const TFileRangeDesc& range) {
    // Validate every __isset / value-range up front (mirrors
    // LanceTableReader::_validate_external_search_request). The build path below
    // then uses the fields unchecked, matching lance's split of validate-once /
    // build-without-recheck.
    RETURN_IF_ERROR(_validate_external_search_request());
    const auto& request = _scan_params->external_search_request;
    const auto& vector = request.search_query.vector_search;

    // 0. Distance metric. It decides which score transform the reader has to undo
    // (see _convert_score_to_distance), so defaulting would silently return wrong
    // distances. __isset was checked by _validate_external_search_request; the
    // value is rejected here if unsupported.
    ScoreTransform transform;
    RETURN_IF_ERROR(parse_score_transform(vector.metric, &transform));
    _score_transform = transform;

    // 1. The per-split payload is the serialized BucketVectorSearchSplit (raw
    // thrift binary, NOT base64; magic "PKVSPLIT", v1). Grab it first so we fail
    // fast before any filesystem IO.
    const auto& payload = _current_range.table_format_params.paimon_params.vector_payload;
    if (payload.payload_type != TPaimonVectorPayloadType::BUCKET_SPLIT_BYTES) {
        return Status::InternalError("paimon-rust vector search unsupported payload type: {}",
                                     static_cast<int>(payload.payload_type));
    }
    const std::string& split_bytes = payload.bytes;
    if (split_bytes.empty()) {
        return Status::InternalError("paimon-rust vector search split payload is empty");
    }

    // 2. Resolve and open (or reuse) the table (same as the normal read path);
    // options carry storage credentials to the FileIO.
    RETURN_IF_ERROR(_open_paimon_table(range));

    // 3. Build and configure the vector-search builder.
    paimon_result_vector_search_builder vb_res =
            paimon_table_new_vector_search_builder(_handles->table.get());
    if (vb_res.error != nullptr) {
        return Status::InternalError("paimon-rust new vector search builder failed: {}",
                                     consume_error(vb_res.error));
    }
    vector_search_builder_ptr vb(vb_res.builder);

    // 3a. Target vector column.
    if (paimon_error* err =
                paimon_vector_search_builder_with_vector_column(vb.get(), vector.column.c_str())) {
        return Status::InternalError("paimon-rust set vector column failed: {}",
                                     consume_error(err));
    }
    // 3b. Query vector. The wire form is a TSearchVector carrying a FLOAT32,
    // little-endian binary blob; decode it into the f32 array the C API wants.
    // v1 only supports FLOAT32 (paimon-rust's index backends operate on f32); any
    // other element type was already rejected by validate. Dimension and
    // byte-size were validated there too, so decode unchecked here.
    const auto& search_vec = vector.query_vector;
    const auto dimension = static_cast<size_t>(search_vec.dimension);
    std::vector<float> query_vector(dimension);
    const auto* bytes = search_vec.values.data();
    for (size_t i = 0; i < dimension; ++i) {
        query_vector[i] = std::bit_cast<float>(LittleEndian::Load32(
                reinterpret_cast<const uint8_t*>(bytes + i * sizeof(uint32_t))));
    }
    if (paimon_error* err = paimon_vector_search_builder_with_query_vector(
                vb.get(), query_vector.data(), query_vector.size())) {
        return Status::InternalError("paimon-rust set query vector failed: {}", consume_error(err));
    }
    // 3c. Retrieval limit (user_limit + user_offset, folded on FE). A negative
    // limit would wrap when cast to uintptr_t, so guard it.
    if (vector.top_k < 0) {
        return Status::InternalError("paimon-rust vector search limit is negative: {}",
                                     vector.top_k);
    }
    if (paimon_error* err = paimon_vector_search_builder_with_limit(
                vb.get(), static_cast<uintptr_t>(vector.top_k))) {
        return Status::InternalError("paimon-rust set limit failed: {}", consume_error(err));
    }
    // 3d. Index/search options (refine_factor, ivf.nprobe, "<algo>.metric", ...).
    if (request.__isset.paimon_options && request.paimon_options.__isset.options &&
        !request.paimon_options.options.empty()) {
        std::vector<paimon_option> c_search_options;
        c_search_options.reserve(request.paimon_options.options.size());
        for (const auto& kv : request.paimon_options.options) {
            c_search_options.emplace_back(kv.first.c_str(), kv.second.c_str());
        }
        if (paimon_error* err = paimon_vector_search_builder_with_options(
                    vb.get(), c_search_options.data(), c_search_options.size())) {
            return Status::InternalError("paimon-rust set search options failed: {}",
                                         consume_error(err));
        }
    }
    // 3e. Materialized user columns. The score field is always appended by
    // paimon-rust itself and must NOT be listed here. Empty projection = only the
    // score column (index-only scan). Log the user columns: an empty list is
    // legitimate, so it is logged as such rather than skipped.
    auto read_columns = _build_read_columns();
    std::vector<const char*> projection;
    projection.reserve(read_columns.size() + 1);
    for (const auto& col : read_columns) {
        projection.push_back(col.c_str());
    }
    projection.push_back(nullptr);
    LOG(INFO) << "paimon-rust vector search: projection=[" << join(read_columns, ", ") << "] ("
              << read_columns.size() << " column(s), distance column '"
              << kPaimonSearchDistanceColumn << "' produced by the reader)";
    if (paimon_error* err =
                paimon_vector_search_builder_with_projection(vb.get(), projection.data())) {
        return Status::InternalError("paimon-rust set vector projection failed: {}",
                                     consume_error(err));
    }

    // 3f. Optional residual scalar filter from the FE data-half push-down. The
    // FE-side DV/merge-on-read interception is intentionally not done (see
    // PushDownVectorTopNIntoPaimonScan): the Rust candidate search stage applies
    // the filter together with deletion vectors, and a table shape Rust cannot
    // handle correctly is Rust's responsibility to reject.
    if (!_conjuncts.empty()) {
        std::vector<std::string> names;
        std::vector<DataTypePtr> types;
        names.reserve(_projected_columns.size());
        types.reserve(_projected_columns.size());
        for (const auto& col : _projected_columns) {
            if (col.is_partition_key || col.name == kPaimonSearchDistanceColumn) {
                continue;
            }
            names.push_back(col.name);
            types.push_back(col.type);
        }
        PaimonRustPredicateConverter converter(names, types, _handles->table.get());
        paimon_predicate* predicate = converter.build(_conjuncts);
        if (predicate != nullptr) {
            // with_filter consumes the predicate on every path.
            if (paimon_error* err = paimon_vector_search_builder_with_filter(vb.get(), predicate)) {
                return Status::InternalError("paimon-rust set vector filter failed: {}",
                                             consume_error(err));
            }
            LOG(INFO) << "paimon-rust vector search: applied residual filter";
        }
    }

    // 4. Execute the search over the single FE-planned bucket split, via the
    // eager Top-K chain (paimon-rust #822). Search, refine, local Top-K and
    // materialization all happen in paimon_vector_read_read; the returned arrow
    // stream is best-first and carries the __paimon_search_score column. The
    // intermediate handles are locals: the migration contract (see
    // doris-vector-search-api-migration.md) guarantees the record batch reader
    // is independent once paimon_vector_read_read returns, so they are released
    // when this function exits.
    //
    //   split:   the bytes only need to live until deserialize returns.
    //   scan:    only needs to live until the plan is constructed.
    //   builder: only needs to live until scan and read are created.
    //   read/plan: only need to live until paimon_vector_read_read returns.
    paimon_result_bucket_vector_search_split split_result =
            paimon_bucket_vector_search_split_deserialize(
                    reinterpret_cast<const uint8_t*>(split_bytes.data()), split_bytes.size());
    if (split_result.error != nullptr) {
        return Status::InternalError("paimon-rust deserialize vector split failed: {}",
                                     consume_error(split_result.error));
    }
    bucket_vector_search_split_ptr split(split_result.split);

    paimon_result_vector_scan scan_result = paimon_vector_search_builder_new_scan(vb.get());
    if (scan_result.error != nullptr) {
        return Status::InternalError("paimon-rust new vector scan failed: {}",
                                     consume_error(scan_result.error));
    }
    vector_scan_ptr scan(scan_result.scan);

    paimon_result_vector_read read_result = paimon_vector_search_builder_new_read(vb.get());
    if (read_result.error != nullptr) {
        return Status::InternalError("paimon-rust new vector read failed: {}",
                                     consume_error(read_result.error));
    }
    vector_read_ptr read(read_result.read);

    // Must use plan_from_bucket_splits: it plans from the FE-assigned split,
    // whereas paimon_vector_scan_plan / execute_read replan from the table and
    // would ignore the split this scanner was given.
    const paimon_bucket_vector_search_split* splits[] = {split.get()};
    paimon_result_vector_plan plan_result =
            paimon_vector_scan_plan_from_bucket_splits(scan.get(), splits, 1);
    if (plan_result.error != nullptr) {
        return Status::InternalError("paimon-rust build vector plan failed: {}",
                                     consume_error(plan_result.error));
    }
    vector_plan_ptr plan(plan_result.plan);

    paimon_result_record_batch_reader rdr_res = paimon_vector_read_read(read.get(), plan.get());
    if (rdr_res.error != nullptr) {
        return Status::InternalError("paimon-rust vector search read failed: {}",
                                     consume_error(rdr_res.error));
    }
    _handles->reader.reset(rdr_res.reader);
    return Status::OK();
}

std::string PaimonRustTableReader::arrow_field_to_block_column(const std::string& arrow_name) {
    std::string name = to_lower(arrow_name);
    if (name == kPaimonRustScoreField) {
        return kPaimonSearchDistanceColumn;
    }
    return name;
}

Status PaimonRustTableReader::parse_score_transform(TVectorMetric::type metric,
                                                    ScoreTransform* transform) {
    switch (metric) {
    case TVectorMetric::L2:
        *transform = ScoreTransform::L2;
        return Status::OK();
    case TVectorMetric::DOT_PRODUCT:
        *transform = ScoreTransform::InnerProduct;
        return Status::OK();
    default:
        // Includes COSINE: paimon-rust does define a score for it
        // (score = 1 - distance), but v1 does not push cosine down, so reaching here
        // means FE and BE disagree. Fail instead of inventing a transform.
        return Status::InternalError(
                "paimon-rust vector search unsupported distance metric: {} "
                "(expected L2 or DOT_PRODUCT)",
                static_cast<int>(metric));
    }
}

void PaimonRustTableReader::_convert_score_to_distance(Block* block, uint32_t block_idx,
                                                       size_t row_offset, size_t num_rows,
                                                       ScoreTransform transform) {
    // Undo what paimon-rust's convert_distance_to_score did, so the column holds
    // what the Doris distance function is defined to return.
    switch (transform) {
    case ScoreTransform::InnerProduct:
        // score IS the inner product. Nothing to undo, and no clamping to do
        // either: the value is unconstrained by definition (it may be negative).
        return;
    case ScoreTransform::L2:
        break;
    }

    auto& column_with_type = block->get_by_position(block_idx);
    auto column = IColumn::mutate(std::move(column_with_type.column));
    PaddedPODArray<float>* data = nullptr;
    const NullMap* null_map = nullptr;
    if (auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
        data = &assert_cast<ColumnFloat32&>(nullable->get_nested_column()).get_data();
        null_map = &nullable->get_null_map_data();
    } else {
        data = &assert_cast<ColumnFloat32&>(*column).get_data();
    }

    // score = 1/(1 + d) where d is the SQUARED L2 distance, and
    // l2_distance_approximate returns the true Euclidean one, so invert both steps:
    // d_euclidean = sqrt(1/score - 1).
    const size_t end = std::min(row_offset + num_rows, data->size());
    for (size_t i = row_offset; i < end; ++i) {
        if (null_map != nullptr && (*null_map)[i] != 0) {
            continue;
        }
        const float score = (*data)[i];
        // score is in (0, 1] whenever d is finite and non-negative, and within that
        // range the radicand is provably non-negative: a real value above 1 cannot
        // round down to below 1.0F in f32, so 1/score >= 1. The useful range
        // therefore never reaches either guard below -- they exist because the
        // distances come straight out of the closed-source lumina kernel with no
        // clamp, and computing the inverse on an illegal score would produce a
        // non-finite distance (score <= 0 -> 1/score is inf; score > 1 -> negative
        // radicand -> NaN), which makes the coordinator's Top-N comparisons
        // ill-defined. Report a distinct negative sentinel per case instead: both
        // are unreachable for a genuine Euclidean distance, so they surface as
        // visibly bogus rows that also say WHICH way the index is broken.
        //
        // A score slightly above 1 is NOT corruption: the kernel computes squared
        // L2 as ||a||^2 + ||b||^2 - 2<a,b>, which can yield a small negative
        // residual (catastrophic cancellation) when a ~= b -- a near-exact hit.
        // Clamp those to distance 0; only scores past kScoreAboveOneTolerance are
        // treated as genuinely corrupt.
        if (UNLIKELY(score <= 0.0F)) {
            LOG(WARNING) << "paimon-rust L2 score_to_distance: corrupt score " << score << " (row "
                         << i << ", expect score in (0, 1]), "
                         << "sentinel " << kCorruptScoreNotPositive;
            (*data)[i] = kCorruptScoreNotPositive;
        } else if (UNLIKELY(score > 1.0F + kScoreAboveOneTolerance)) {
            LOG(WARNING) << "paimon-rust L2 score_to_distance: corrupt score " << score << " (row "
                         << i << ", expect score in (0, 1]), "
                         << "sentinel " << kCorruptScoreAboveOne;
            (*data)[i] = kCorruptScoreAboveOne;
        } else if (score > 1.0F) {
            // Numerical artifact of a near-exact hit (see kScoreAboveOneTolerance):
            // clamp to distance 0 instead of sentineling a legitimate best match.
            (*data)[i] = 0.0F;
        } else {
            // score == 1.0F gives exactly 0 here -- a legal exact hit, which is why
            // this must not share a branch with the negative-radicand case.
            (*data)[i] = std::sqrt(1.0F / score - 1.0F);
        }
    }
    // mutate() may have unshared a reference-counted column; put the (possibly
    // new) mutable instance back into the block.
    column_with_type.column = std::move(column);
}

void PaimonRustTableReader::_close_split_reader() {
    if (!_handles) {
        return;
    }
    // Reverse of the declaration order in PaimonHandles.
    _handles->reader.reset();
    _handles->table_read.reset();
    _handles->plan.reset();
    _handles->read_builder.reset();
}

void PaimonRustTableReader::_close_table() {
    if (!_handles) {
        return;
    }
    _close_split_reader();
    _handles->table.reset();
    _opened_table_key.reset();
}

Status PaimonRustTableReader::_apply_predicate() {
    if (_conjuncts.empty() || !_handles || !_handles->table || !_handles->read_builder) {
        return Status::OK();
    }
    if (_scanner_profile != nullptr) {
        COUNTER_UPDATE(_rust_predicates_input, _conjuncts.size());
        for (const auto& conjunct : _conjuncts) {
            if (conjunct && conjunct->root() && conjunct->root()->is_rf_wrapper()) {
                COUNTER_UPDATE(_rust_runtime_filters_input, 1);
            }
        }
    }
    LOG(INFO) << "paimon-rust predicate pushdown: " << _conjuncts.size() << " conjunct(s) input";
    // The conjunct VSlotRefs carry table global indices (positions), so the v2
    // converter mode resolves fields by the projected column names; partition
    // keys are excluded because the rust reader does not read them.
    std::vector<std::string> names;
    std::vector<DataTypePtr> types;
    names.reserve(_projected_columns.size());
    types.reserve(_projected_columns.size());
    for (const auto& col : _projected_columns) {
        if (col.is_partition_key || col.name == kPaimonSearchDistanceColumn) {
            continue;
        }
        names.push_back(col.name);
        types.push_back(col.type);
    }
    PaimonRustPredicateConverter converter(names, types, _handles->table.get());
    paimon_predicate* predicate = converter.build(_conjuncts);
    if (_scanner_profile != nullptr) {
        COUNTER_UPDATE(_rust_predicates_converted, converter.converted_conjuncts());
    }
    if (predicate == nullptr) {
        LOG(INFO) << "paimon-rust predicate pushdown: nothing convertible, no filter applied";
        return Status::OK();
    }
    // paimon_read_builder_with_filter consumes the predicate (ownership moves to
    // the builder) on every path, so we must not free it here.
    if (paimon_error* err =
                paimon_read_builder_with_filter(_handles->read_builder.get(), predicate)) {
        return Status::InternalError("paimon-rust apply filter failed: {}", consume_error(err));
    }
    // Count application only after the C API accepts the filter; reader timers
    // alone cannot distinguish pushdown from Doris residual-only execution.
    if (_scanner_profile != nullptr) {
        COUNTER_UPDATE(_rust_predicates_applied, converter.converted_conjuncts());
        COUNTER_UPDATE(_rust_runtime_filters_applied, converter.converted_runtime_filters());
    }
    LOG(INFO) << "paimon-rust predicate pushdown: applied";
    return Status::OK();
}

Status PaimonRustTableReader::_fill_block_from_record_batch(
        const std::shared_ptr<arrow::RecordBatch>& batch, Block* block, size_t rows) {
    SCOPED_TIMER(_rust_arrow_to_block_time);
    DORIS_CHECK(batch != nullptr);
    DORIS_CHECK(block != nullptr);
    std::unordered_set<size_t> materialized_indices;
    materialized_indices.reserve(_projected_columns.size());

    // PK-vector (ANN) search mode: paimon-rust appends the score field
    // (__paimon_search_score) to every batch, and the reader must convert it into
    // the distance FE asked for (__paimon_search_score_to_dis) in place, after the
    // fill loop. Locate the output position up front: FE only adds the distance
    // column when it rewrote the query into a vector search (see
    // PushDownVectorTopNIntoPaimonScan), so _score_transform being set implies the
    // column is projected. Failing loudly here beats the alternative: the fill loop
    // would silently skip the lookup, leaving the column empty while every other
    // column holds `rows` -- an unattributable "sizes of columns don't match"
    // downstream.
    std::optional<size_t> distance_output_idx;
    size_t distance_row_offset = 0;
    if (_score_transform.has_value()) {
        auto it = _output_name_to_idx.find(kPaimonSearchDistanceColumn);
        if (it == _output_name_to_idx.end()) {
            return Status::InternalError(
                    "paimon-rust vector search: block has no '{}' column (FE/BE column name "
                    "contract broken)",
                    kPaimonSearchDistanceColumn);
        }
        distance_output_idx = it->second;
        // Must be read BEFORE the fill loop: read_column_from_arrow appends, and the
        // column may already hold converted rows from a previous batch of this split.
        distance_row_offset = block->get_by_position(it->second).column->size();
    }

    {
        auto columns_guard = block->mutate_columns_scoped();
        auto& columns = columns_guard.mutable_columns();
        for (int c = 0; c < batch->num_columns(); ++c) {
            const auto& field = batch->schema()->field(c);
            if (field->name() == VALUE_KIND_FIELD) {
                continue;
            }
            // Projected column names are FE-normalized to lowercase.
            // paimon-rust's case_sensitive=false setting also case-folds column
            // names in the schema output, so exact match works — but tolerate
            // mixed-case Rust output by folding here as well. The score field is
            // additionally renamed onto the distance column it feeds (see
            // arrow_field_to_block_column).
            const std::string block_name = arrow_field_to_block_column(field->name());
            auto it = _output_name_to_idx.find(field->name());
            if (it == _output_name_to_idx.end()) {
                it = _output_name_to_idx.find(block_name);
            }
            if (it == _output_name_to_idx.end()) {
                // Skip columns that are not in the block (e.g. columns dropped by
                // slot pruning).
                continue;
            }
            const auto output_idx = it->second;
            if (distance_output_idx.has_value() && output_idx == *distance_output_idx) {
                // Check the Arrow width before the serde reinterprets the buffer. For
                // numerics read_column_from_arrow does NOT validate the source type: it
                // casts buffers[1] to the *destination* CppType and inserts `rows` of
                // them. The destination is pinned to FLOAT by FE
                // (PaimonVectorSearch.createDistanceColumn), so
                //   - a wider field (Float64) yields `rows` garbage f32 values, each half
                //     of a double's bits. The column still grows by exactly `rows`, so
                //     the guard below passes and the query returns a wrongly-ordered
                //     Top-K with no error anywhere.
                //   - a narrower field (HALF_FLOAT) reads rows*4 bytes out of a
                //     rows*2 byte buffer, i.e. past its end.
                // paimon-rust builds this field as Float32 unconditionally
                // (materialize_search_result), so this only fires if that changes --
                // which is exactly when a loud failure beats either outcome above.
                if (batch->column(c)->type_id() != arrow::Type::FLOAT) {
                    return Status::InternalError(
                            "paimon-rust vector search: arrow field '{}' has type {}, expected "
                            "float32 -- the block's '{}' column is FLOAT and the arrow buffer "
                            "is reinterpreted as f32 without conversion",
                            field->name(), batch->column(c)->type()->ToString(),
                            kPaimonSearchDistanceColumn);
                }
            }
            if (!materialized_indices.emplace(output_idx).second) {
                return Status::InternalError("paimon-rust returned duplicate column '{}'",
                                             field->name());
            }
            try {
                RETURN_IF_ERROR(columns_guard.get_datatype_by_position(output_idx)
                                        ->get_serde()
                                        ->read_column_from_arrow(*columns[output_idx],
                                                                 batch->column(c).get(), 0, rows,
                                                                 _ctz));
            } catch (Exception& e) {
                return Status::InternalError("Failed to convert from arrow to block: {}", e.what());
            }
        }
    }

    if (distance_output_idx.has_value()) {
        // Guard the two assumptions _convert_score_to_distance depends on, both of
        // which fail SILENTLY otherwise -- the block stays well-formed and the query
        // returns a wrongly-ordered Top-K with no error anywhere:
        //
        //  - distance_row_offset was captured BEFORE the fill loop. If a refactor
        //    moves that capture below the loop, the offset already includes this
        //    batch, so `end = min(offset + rows, size)` collapses to `offset` and
        //    the conversion loop runs zero times, leaving raw scores in the column.
        //    Since a score is higher-is-better, an l2 ORDER BY then returns the LEAST
        //    similar rows.
        //  - the Arrow batch actually carried the score field. If paimon-rust renames
        //    it, the fill loop's lookup misses and the column is never appended to.
        const size_t size_after = block->get_by_position(*distance_output_idx).column->size();
        if (size_after != distance_row_offset + rows) {
            return Status::InternalError(
                    "paimon-rust vector search: '{}' column grew from {} to {}, expected {} "
                    "(batch has {} rows) -- either the row offset was captured after the fill "
                    "loop, or the arrow batch is missing the '{}' field",
                    kPaimonSearchDistanceColumn, distance_row_offset, size_after,
                    distance_row_offset + rows, rows, kPaimonRustScoreField);
        }
        _convert_score_to_distance(block, static_cast<uint32_t>(*distance_output_idx),
                                   distance_row_offset, rows, *_score_transform);
    }
    // Partition columns and other projected columns absent from the arrow batch
    // are back-filled from split metadata / defaults.
    RETURN_IF_ERROR(_fill_non_arrow_columns(block, rows, materialized_indices));
    // This direct Arrow path bypasses TableReader::finalize_chunk, whose last
    // step enforces truncate_char_or_varchar_columns — without it, a column
    // narrowed by schema evolution returns untruncated historical values.
    RETURN_IF_ERROR(_truncate_char_or_varchar_columns(block));
    return Status::OK();
}

Status PaimonRustTableReader::_truncate_char_or_varchar_columns(Block* block) {
    if (_runtime_state == nullptr ||
        !_runtime_state->query_options().truncate_char_or_varchar_columns) {
        return Status::OK();
    }
    for (size_t idx = 0; idx < block->columns(); ++idx) {
        const auto& column_type = block->get_by_position(idx).type;
        if (column_type == nullptr) {
            continue;
        }
        const auto type = remove_nullable(column_type);
        const auto primitive = type->get_primitive_type();
        if (primitive != TYPE_VARCHAR && primitive != TYPE_CHAR) {
            continue;
        }
        const auto target_len = assert_cast<const DataTypeString*>(type.get())->len();
        if (target_len <= 0) {
            continue;
        }
        // Reuses TableReader's vectorized truncation (substring(column, 1,
        // len)); the base variant maps through column_mapper metadata, which
        // the direct rust path does not populate, so iterate the block's own
        // slot-derived types here — the arrow side is always lengthless Utf8,
        // so any bounded CHAR/VARCHAR target truncates to its declared length.
        // Partition/default expressions can produce ColumnConst(ColumnNullable). The shared
        // truncation helper expects a materialized nullable column, as in finalize_chunk.
        auto& column = block->get_by_position(idx).column;
        column = column->convert_to_full_column_if_const();
        _truncate_char_or_varchar_column(block, idx, target_len);
    }
    return Status::OK();
}

Status PaimonRustTableReader::_fill_non_arrow_columns(
        Block* block, size_t rows, const std::unordered_set<size_t>& materialized_indices) {
    for (size_t idx = 0; idx < _projected_columns.size(); ++idx) {
        if (materialized_indices.count(idx) != 0) {
            continue;
        }
        const auto& column = _projected_columns[idx];
        VExprContextSPtr constant_expr;
        if (const Field* value = find_partition_value(column, _partition_values);
            column.is_partition_key && value != nullptr) {
            // Partition values are split constants (same materialization the
            // TableColumnMapper builds for native readers).
            constant_expr =
                    VExprContext::create_shared(VLiteral::create_shared(column.type, *value));
        } else if (column.default_expr != nullptr) {
            constant_expr = column.default_expr;
        } else {
            // The column is genuinely absent from the arrow batch. Schema
            // evolution is handled by paimon-rust itself, so reaching here means
            // an unexpected schema drift: fill defaults so the scan remains
            // well-defined instead of failing the query.
            LOG(WARNING) << "paimon-rust did not return projected column '" << column.name
                         << "'; filling with defaults";
            auto data = column.type->create_column();
            data->insert_many_defaults(rows);
            block->replace_by_position(idx, std::move(data));
            continue;
        }
        ColumnPtr constant_column;
        RETURN_IF_ERROR(_materialize_constant_column(constant_expr, column.type, column.name, rows,
                                                     &constant_column));
        block->replace_by_position(idx, std::move(constant_column));
    }
    return Status::OK();
}

Status PaimonRustTableReader::_materialize_constant_column(const VExprContextSPtr& expr,
                                                           const DataTypePtr& type,
                                                           const std::string& name, size_t rows,
                                                           ColumnPtr* column) {
    DORIS_CHECK(expr != nullptr);
    DORIS_CHECK(column != nullptr);
    RowDescriptor row_desc;
    RETURN_IF_ERROR(expr->prepare(_runtime_state, row_desc));
    RETURN_IF_ERROR(expr->open(_runtime_state));
    // Constants evaluate per input row, so a rows-sized synthetic block yields a
    // rows-sized result for both plain literals and default expressions.
    Block eval_block;
    eval_block.insert({type->create_column_const_with_default_value(rows), type, name});
    int result_column_id = -1;
    RETURN_IF_ERROR(expr->execute(&eval_block, &result_column_id));
    DORIS_CHECK(result_column_id >= 0);
    ColumnPtr result_column = eval_block.get_by_position(result_column_id).column;
    if (result_column->size() == 1 && rows > 1) {
        result_column = ColumnConst::create(std::move(result_column), rows);
    }
    *column = std::move(result_column);
    return Status::OK();
}

Status PaimonRustTableReader::_decode_split_bytes(std::string* out) const {
    if (!_current_range.__isset.table_format_params ||
        !_current_range.table_format_params.__isset.paimon_params ||
        !_current_range.table_format_params.paimon_params.__isset.paimon_split) {
        return Status::InternalError("paimon-rust missing paimon_split in scan range");
    }
    const auto& encoded_split = _current_range.table_format_params.paimon_params.paimon_split;
    if (!base64_decode(encoded_split, out)) {
        return Status::InternalError("paimon-rust base64 decode paimon_split failed");
    }
    if (out->empty()) {
        return Status::InternalError("paimon-rust decoded paimon_split is empty");
    }
    return Status::OK();
}

std::optional<std::string> PaimonRustTableReader::_resolve_table_path(
        const TFileRangeDesc& range) const {
    if (range.__isset.table_format_params && range.table_format_params.__isset.paimon_params &&
        range.table_format_params.paimon_params.__isset.paimon_table &&
        !range.table_format_params.paimon_params.paimon_table.empty()) {
        return range.table_format_params.paimon_params.paimon_table;
    }
    return std::nullopt;
}

std::optional<std::string> PaimonRustTableReader::_resolve_db_name(
        const TFileRangeDesc& range) const {
    if (range.__isset.table_format_params && range.table_format_params.__isset.paimon_params &&
        range.table_format_params.paimon_params.__isset.db_name &&
        !range.table_format_params.paimon_params.db_name.empty()) {
        return range.table_format_params.paimon_params.db_name;
    }
    return std::nullopt;
}

std::optional<std::string> PaimonRustTableReader::_resolve_table_name(
        const TFileRangeDesc& range) const {
    if (range.__isset.table_format_params && range.table_format_params.__isset.paimon_params &&
        range.table_format_params.paimon_params.__isset.table_name &&
        !range.table_format_params.paimon_params.table_name.empty()) {
        return range.table_format_params.paimon_params.table_name;
    }
    return std::nullopt;
}

std::optional<std::string> PaimonRustTableReader::_resolve_table_schema_json(
        const TFileRangeDesc& range) const {
    if (range.__isset.table_format_params && range.table_format_params.__isset.paimon_params &&
        range.table_format_params.paimon_params.__isset.paimon_table_schema_json &&
        !range.table_format_params.paimon_params.paimon_table_schema_json.empty()) {
        return range.table_format_params.paimon_params.paimon_table_schema_json;
    }
    return std::nullopt;
}

std::optional<std::string> PaimonRustTableReader::_resolve_branch(
        const TFileRangeDesc& range) const {
    // FE only sets paimon_branch when the branch is not `main` (matches
    // upstream paimon commit 742da63: null-if-DEFAULT_MAIN_BRANCH). Unset here
    // means main-branch semantics.
    if (range.__isset.table_format_params && range.table_format_params.__isset.paimon_params &&
        range.table_format_params.paimon_params.__isset.paimon_branch &&
        !range.table_format_params.paimon_params.paimon_branch.empty()) {
        return range.table_format_params.paimon_params.paimon_branch;
    }
    return std::nullopt;
}

std::vector<std::string> PaimonRustTableReader::_build_read_columns() const {
    std::vector<std::string> columns;
    columns.reserve(_projected_columns.size());
    for (const auto& column : _projected_columns) {
        if (column.is_partition_key) {
            continue;
        }
        // The PK-vector search distance is reader-produced and auto-appended by
        // paimon-rust under its own field name; it is never a projectable table
        // column, so exclude it from the projection in every mode (in non-vector
        // mode it is simply absent).
        if (column.name == kPaimonSearchDistanceColumn) {
            continue;
        }
        columns.emplace_back(column.name);
    }
    return columns;
}

std::map<std::string, std::string> PaimonRustTableReader::_build_options() const {
    std::map<std::string, std::string> options;
    if (_scan_params && _scan_params->__isset.paimon_options &&
        !_scan_params->paimon_options.empty()) {
        options.insert(_scan_params->paimon_options.begin(), _scan_params->paimon_options.end());
    } else if (_current_range.__isset.table_format_params &&
               _current_range.table_format_params.__isset.paimon_params &&
               _current_range.table_format_params.paimon_params.__isset.paimon_options) {
        options.insert(_current_range.table_format_params.paimon_params.paimon_options.begin(),
                       _current_range.table_format_params.paimon_params.paimon_options.end());
    }

    if (_scan_params && _scan_params->__isset.properties && !_scan_params->properties.empty()) {
        for (const auto& kv : _scan_params->properties) {
            options[kv.first] = kv.second;
        }
    } else if (_current_range.__isset.table_format_params &&
               _current_range.table_format_params.__isset.paimon_params &&
               _current_range.table_format_params.paimon_params.__isset.hadoop_conf) {
        for (const auto& kv : _current_range.table_format_params.paimon_params.hadoop_conf) {
            options[kv.first] = kv.second;
        }
    }

    auto copy_if_missing = [&](const char* from_key, const char* to_key) {
        if (options.find(to_key) != options.end()) {
            return;
        }
        auto it = options.find(from_key);
        if (it != options.end() && !it->second.empty()) {
            options[to_key] = it->second;
        }
    };

    // The pinned paimon-rust storage dispatcher (io/storage.rs) selects the
    // FileIO parser from the table path's URI scheme, and libpaimon_c.a
    // compiles in separate COS, OBS, GCS and Azdls parsers besides the OSS
    // and S3 ones. Doris's FE normalizes every object store's credentials
    // into the AWS_* / use_path_style aliases, which this bridge can only
    // translate into the two key families those two parsers read:
    //   oss://        -> fs.oss.endpoint / fs.oss.accessKeyId /
    //                    fs.oss.accessKeySecret (+ fs.oss.securityToken STS)
    //   s3:// / s3a:// -> paimon-java's s3.* family (access-key, secret-key,
    //                    session.token, endpoint, region, path-style-access,
    //                    normalized from the fs.s3a. / s3a. / s3. prefixes)
    // The FE therefore gates the rust reader to exactly these schemes
    // (PaimonScanNode: cosn:// / obs:// / gs:// / abfs:// warehouses fall
    // back to JNI before the split is encoded — their parsers read the
    // fs.cosn.userinfo.* / fs.obs.* / gcs.* / azure.* families the AWS_*
    // aliases cannot express). The mapping here is scoped the same way so a
    // version-skewed FE cannot smuggle AWS_* aliases into another parser's
    // property map: a cosn:// table reaching this bridge with synthesized
    // s3.* keys would hit the COS parser without
    // fs.cosn.userinfo.secretId / secretKey and fail the open with a
    // misleading auth error instead of the JNI fallback.
    const std::string table_path = _resolve_table_path(_current_range).value_or("");
    std::string scheme;
    if (const auto sep = table_path.find("://"); sep != std::string::npos) {
        scheme = to_lower(table_path.substr(0, sep));
    }
    if (scheme == "oss") {
        // The OSS parser reads only the four fs.oss.* keys (and retry
        // settings); native fs.oss.* options pass through untouched. It has
        // no region / anonymous / assume-role handling, so nothing else is
        // mapped for this scheme.
        copy_if_missing("AWS_ENDPOINT", "fs.oss.endpoint");
        copy_if_missing("AWS_ACCESS_KEY", "fs.oss.accessKeyId");
        copy_if_missing("AWS_SECRET_KEY", "fs.oss.accessKeySecret");
        copy_if_missing("AWS_TOKEN", "fs.oss.securityToken");
        return options;
    }
    if (scheme == "s3" || scheme == "s3a") {
        copy_if_missing("AWS_ACCESS_KEY", "s3.access-key");
        copy_if_missing("AWS_SECRET_KEY", "s3.secret-key");
        copy_if_missing("AWS_TOKEN", "s3.session.token");
        copy_if_missing("AWS_ENDPOINT", "s3.endpoint");
        copy_if_missing("AWS_REGION", "s3.region");
        copy_if_missing("use_path_style", "s3.path-style-access");
        // Authentication modes: the FE storage-properties channel marks anonymous
        // access with AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS (emitted when no
        // static credentials are configured) and assume-role with
        // AWS_ROLE_ARN / AWS_EXTERNAL_ID (from the s3.role_arn / s3.external_id
        // catalog properties). The crate reads s3.anonymous (skip_signature) and
        // the s3.assumed.role.* family, so map both; without these, anonymous
        // catalogs would consult the ambient credential chain and role-only
        // catalogs would never assume the requested role. The remaining provider
        // modes are ambient JVM credential chains (ENV, SYSTEM_PROPERTIES,
        // WEB_IDENTITY, CONTAINER, INSTANCE_PROFILE) with no paimon-rust
        // equivalent — the FE gates those away from the rust reader before the
        // split is encoded.
        if (options.contains("AWS_CREDENTIALS_PROVIDER_TYPE") &&
            options.at("AWS_CREDENTIALS_PROVIDER_TYPE") == "ANONYMOUS") {
            options["s3.anonymous"] = "true";
        }
        copy_if_missing("AWS_ROLE_ARN", "s3.assumed.role.arn");
        copy_if_missing("AWS_EXTERNAL_ID", "s3.assumed.role.externalId");
        // OSS-shaped options on an S3 warehouse (cross-protocol alias): map them
        // to the s3.* family as well.
        copy_if_missing("fs.oss.accessKeyId", "s3.access-key");
        copy_if_missing("fs.oss.accessKeySecret", "s3.secret-key");
        copy_if_missing("fs.oss.sessionToken", "s3.session.token");
        copy_if_missing("fs.oss.endpoint", "s3.endpoint");
        copy_if_missing("fs.oss.region", "s3.region");
    }
    // Every other scheme passes through untouched: hdfs:// tables read their
    // hadoop conf as delivered, scheme-less / file:// paths need no
    // credentials, and the COS / OBS / GCS / Azdls families are gated to JNI
    // on the FE — synthesizing s3.* keys for any of them would be dead
    // weight at best and a silent misconfiguration at worst.
    return options;
}

} // namespace doris::format::paimon
