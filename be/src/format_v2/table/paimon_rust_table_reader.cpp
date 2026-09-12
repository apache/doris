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
#include <string_view>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_const.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "format/table/paimon_rust_predicate_converter.h"
#include "format_v2/column_mapper.h"
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
#define PAIMON_OWNED(type, freefn)                                  \
    struct type##_deleter {                                         \
        void operator()(paimon_##type* p) const {                   \
            if (p) {                                                \
                freefn(p);                                          \
            }                                                       \
        }                                                           \
    };                                                              \
    using type##_ptr = std::unique_ptr<paimon_##type, type##_deleter>

PAIMON_OWNED(table, paimon_table_free);
PAIMON_OWNED(read_builder, paimon_read_builder_free);
PAIMON_OWNED(plan, paimon_plan_free);
PAIMON_OWNED(table_read, paimon_table_read_free);
PAIMON_OWNED(record_batch_reader, paimon_record_batch_reader_free);
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

// Render storage options for diagnostics. Values of sensitive keys (secret /
// password / token / access key) are masked so credentials never hit the log.
std::string format_options(const std::map<std::string, std::string>& options) {
    std::string out;
    for (const auto& kv : options) {
        if (!out.empty()) {
            out += ", ";
        }
        std::string_view key = kv.first;
        const bool sensitive = key.find("secret") != std::string_view::npos ||
                               key.find("password") != std::string_view::npos ||
                               key.find("token") != std::string_view::npos ||
                               key.find("access.key") != std::string_view::npos ||
                               key.find("access-key") != std::string_view::npos;
        out += kv.first;
        out += '=';
        out += sensitive ? "***" : kv.second;
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
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctz);
        if (_scanner_profile != nullptr) {
            file_scan_profile::ensure_hierarchy(_scanner_profile);
            _rust_total_time = ADD_CHILD_TIMER(_scanner_profile, "PaimonRustReader",
                                               file_scan_profile::TABLE_READER);
            _rust_open_split_time = ADD_CHILD_TIMER(_scanner_profile, "OpenSplitTime",
                                                    "PaimonRustReader");
            _rust_read_batch_time =
                    ADD_CHILD_TIMER(_scanner_profile, "ReadBatchTime", "PaimonRustReader");
            _rust_arrow_to_block_time =
                    ADD_CHILD_TIMER(_scanner_profile, "ArrowToBlockTime", "PaimonRustReader");
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
    if (!params.__isset.paimon_split || params.paimon_split.empty()) {
        return Status::InternalError(
                "missing paimon_split for paimon rust reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    if (params.__isset.reader_type && params.reader_type != TPaimonReaderType::PAIMON_RUST) {
        return Status::InternalError(
                "invalid reader_type for paimon rust reader, possibly caused by FE/BE protocol "
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

Status PaimonRustTableReader::_open_split_reader(const TFileRangeDesc& range) {
    // 1. Decode the FE-planned split first so we fail fast (and without any
    // filesystem IO) when it is missing or malformed.
    std::string split_bytes;
    RETURN_IF_ERROR(_decode_split_bytes(&split_bytes));

    // 2. Resolve identifier + table_path + FE-supplied TableSchema JSON.
    auto table_path = _resolve_table_path(range).value();
    auto db_name = _resolve_db_name(range).value();
    auto table_name = _resolve_table_name(range).value();
    auto schema_json = _resolve_table_schema_json(range).value();
    auto branch_opt = _resolve_branch(range);

    // 3. Assemble storage options: FE-supplied paimon options + hadoop_conf +
    // OSS/S3 → AWS_* translations. These feed FileIO only (per
    // paimon_table_from_schema_json contract); they are NOT merged into the
    // supplied table schema.
    auto options = _build_options();

    auto opened_table_key = std::make_tuple(table_path, schema_json, db_name, table_name,
                                            branch_opt, options);
    if (!_handles || !_handles->table || _opened_table_key != opened_table_key) {
        // A paimon scan reads one table, so the handle is opened at most once per
        // distinct identity (e.g. re-created after a close); splits of the same
        // table reuse it and only rebuild the read pipeline below.
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
                branch_str.c_str(), c_options.empty() ? nullptr : c_options.data(),
                c_options.size());
        if (tbl_res.error != nullptr) {
            return Status::InternalError(
                    "paimon-rust table_from_schema_json failed: db={} table={} err={}", db_name,
                    table_name, consume_error(tbl_res.error));
        }
        _handles->table.reset(tbl_res.table);
        _opened_table_key = std::move(opened_table_key);
    }

    // 4. Build the read pipeline: read_builder -> case-insensitive -> projection.
    paimon_result_read_builder rb_res = paimon_table_new_read_builder(_handles->table.get());
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
    LOG(INFO) << "paimon-rust predicate pushdown: " << _conjuncts.size() << " conjunct(s) input";
    // The conjunct VSlotRefs carry table global indices (positions), so the v2
    // converter mode resolves fields by the projected column names; partition
    // keys are excluded because the rust reader does not read them.
    std::vector<std::string> names;
    std::vector<DataTypePtr> types;
    names.reserve(_projected_columns.size());
    types.reserve(_projected_columns.size());
    for (const auto& col : _projected_columns) {
        if (col.is_partition_key) {
            continue;
        }
        names.push_back(col.name);
        types.push_back(col.type);
    }
    PaimonRustPredicateConverter converter(names, types, _handles->table.get());
    paimon_predicate* predicate = converter.build(_conjuncts);
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
            // mixed-case Rust output by folding here as well.
            auto it = _output_name_to_idx.find(field->name());
            if (it == _output_name_to_idx.end()) {
                it = _output_name_to_idx.find(to_lower(field->name()));
            }
            if (it == _output_name_to_idx.end()) {
                // Skip columns that are not in the block (e.g. columns dropped by
                // slot pruning).
                continue;
            }
            const auto output_idx = it->second;
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
                return Status::InternalError("Failed to convert from arrow to block: {}",
                                             e.what());
            }
        }
    }
    // Partition columns and other projected columns absent from the arrow batch
    // are back-filled from split metadata / defaults.
    return _fill_non_arrow_columns(block, rows, materialized_indices);
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
            constant_expr = VExprContext::create_shared(
                    VLiteral::create_shared(column.type, *value));
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
        options.insert(
                _current_range.table_format_params.paimon_params.paimon_options.begin(),
                _current_range.table_format_params.paimon_params.paimon_options.end());
    }

    if (_scan_params && _scan_params->__isset.properties && !_scan_params->properties.empty()) {
        for (const auto& kv : _scan_params->properties) {
            options[kv.first] = kv.second;
        }
    } else if (_current_range.__isset.table_format_params &&
               _current_range.table_format_params.__isset.paimon_params &&
               _current_range.table_format_params.paimon_params.__isset.hadoop_conf) {
        for (const auto& kv :
             _current_range.table_format_params.paimon_params.hadoop_conf) {
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

    // Map common OSS/S3 Hadoop configs to Doris/paimon-native S3 property keys
    // that the paimon-rust FileIO recognizes.
    copy_if_missing("fs.oss.accessKeyId", "AWS_ACCESS_KEY");
    copy_if_missing("fs.oss.accessKeySecret", "AWS_SECRET_KEY");
    copy_if_missing("fs.oss.sessionToken", "AWS_TOKEN");
    copy_if_missing("fs.oss.endpoint", "AWS_ENDPOINT");
    copy_if_missing("fs.oss.region", "AWS_REGION");
    copy_if_missing("fs.s3a.access.key", "AWS_ACCESS_KEY");
    copy_if_missing("fs.s3a.secret.key", "AWS_SECRET_KEY");
    copy_if_missing("fs.s3a.session.token", "AWS_TOKEN");
    copy_if_missing("fs.s3a.endpoint", "AWS_ENDPOINT");
    copy_if_missing("fs.s3a.region", "AWS_REGION");
    copy_if_missing("fs.s3a.path.style.access", "use_path_style");

    return options;
}

} // namespace doris::format::paimon
