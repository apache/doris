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

#include "format/table/paimon_rust_reader.h"

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
#include "exprs/vexpr.h"
#include "format/table/paimon_rust_predicate_converter.h"
#include "format/table/partition_column_filler.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/string_util.h"
#include "util/url_coding.h"

extern "C" {
#include "paimon_rust/paimon.h"
}

namespace doris {

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

PAIMON_OWNED(catalog, paimon_catalog_free);
PAIMON_OWNED(identifier, paimon_identifier_free);
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

// Render catalog options for diagnostics. Values of sensitive keys (secret /
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

// Long-lived paimon-rust handles. Order of members matters: destruction runs
// in reverse declaration order, and the table depends on the catalog, the
// read_builder on the table, and so on down to the record batch reader. So the
// catalog MUST be declared first (destroyed last) and the reader last.
struct PaimonRustReader::PaimonHandles {
    catalog_ptr catalog;
    table_ptr table;
    read_builder_ptr read_builder;
    plan_ptr plan;
    table_read_ptr table_read;
    record_batch_reader_ptr reader;
};

PaimonRustReader::PaimonRustReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                   RuntimeState* state, RuntimeProfile* profile,
                                   const TFileRangeDesc& range,
                                   const TFileScanRangeParams* range_params)
        : _file_slot_descs(file_slot_descs),
          _state(state),
          _profile(profile),
          _range(range),
          _range_params(range_params) {
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctzz);
    if (range.__isset.table_format_params &&
        range.table_format_params.__isset.table_level_row_count) {
        _remaining_table_level_row_count = range.table_format_params.table_level_row_count;
    } else {
        _remaining_table_level_row_count = -1;
    }
}

PaimonRustReader::~PaimonRustReader() = default;

Status PaimonRustReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _partition_values.clear();
    _partition_value_is_null.clear();
    if (ctx->range == nullptr || ctx->tuple_descriptor == nullptr ||
        !ctx->range->__isset.columns_from_path_keys) {
        return Status::OK();
    }

    DORIS_CHECK(ctx->range->__isset.columns_from_path);
    DORIS_CHECK(ctx->range->columns_from_path.size() == ctx->range->columns_from_path_keys.size());
    const bool has_null_flags = ctx->range->__isset.columns_from_path_is_null;
    if (has_null_flags) {
        DORIS_CHECK(ctx->range->columns_from_path_is_null.size() ==
                    ctx->range->columns_from_path_keys.size());
    }

    std::unordered_map<std::string, const SlotDescriptor*> name_to_slot;
    for (auto* slot : ctx->tuple_descriptor->slots()) {
        name_to_slot.emplace(slot->col_name(), slot);
    }
    for (size_t i = 0; i < ctx->range->columns_from_path_keys.size(); ++i) {
        const auto& key = ctx->range->columns_from_path_keys[i];
        auto slot_it = name_to_slot.find(key);
        if (slot_it == name_to_slot.end()) {
            continue;
        }
        _partition_values.emplace(
                key, std::make_tuple(ctx->range->columns_from_path[i], slot_it->second));
        _partition_value_is_null.emplace(
                key, has_null_flags ? ctx->range->columns_from_path_is_null[i] : false);
    }
    return Status::OK();
}

Status PaimonRustReader::on_after_read_block(Block* block, size_t* read_rows) {
    if (_column_descs == nullptr || _partition_values.empty() || *read_rows == 0 ||
        _push_down_agg_type == TPushAggOp::type::COUNT) {
        return Status::OK();
    }
    return _fill_partition_columns(block, *read_rows);
}

Status PaimonRustReader::_fill_partition_columns(Block* block, size_t num_rows) {
    if (_col_name_to_block_idx.empty()) {
        _col_name_to_block_idx = block->get_name_to_pos_map();
    }

    for (const auto& desc : *_column_descs) {
        if (desc.category != ColumnCategory::PARTITION_KEY) {
            continue;
        }
        auto value_it = _partition_values.find(desc.name);
        if (value_it == _partition_values.end()) {
            continue;
        }
        auto col_it = _col_name_to_block_idx.find(desc.name);
        if (col_it == _col_name_to_block_idx.end()) {
            return Status::InternalError("Missing partition column {} in block {}", desc.name,
                                         block->dump_structure());
        }

        auto& column_with_type_and_name = block->get_by_position(col_it->second);
        auto mutable_column = std::move(*column_with_type_and_name.column).mutate();
        const auto& [value, slot_desc] = value_it->second;
        auto null_it = _partition_value_is_null.find(desc.name);
        DORIS_CHECK(null_it != _partition_value_is_null.end());
        RETURN_IF_ERROR(fill_partition_column_from_path_value(*mutable_column, *slot_desc, value,
                                                              num_rows, null_it->second));
        column_with_type_and_name.column = std::move(mutable_column);
    }
    return Status::OK();
}

Status PaimonRustReader::init_reader() {
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _remaining_table_level_row_count >= 0) {
        return Status::OK();
    }
    return _init_paimon_reader();
}

Status PaimonRustReader::_do_get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _remaining_table_level_row_count >= 0) {
        auto rows = std::min(_remaining_table_level_row_count,
                             (int64_t)_state->query_options().batch_size);
        _remaining_table_level_row_count -= rows;
        auto mutable_columns_guard = block->mutate_columns_scoped();
        auto& mutate_columns = mutable_columns_guard.mutable_columns();
        for (auto& col : mutate_columns) {
            col->resize(rows);
        }
        *read_rows = rows;
        *eof = false;
        if (_remaining_table_level_row_count == 0) {
            *eof = true;
        }
        return Status::OK();
    }

    // num_splits == 0 yields an empty (but valid) stream: report EOF.
    if (_reader_eof) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    if (!_handles || !_handles->reader) {
        return Status::InternalError("paimon-rust reader is not initialized");
    }

    if (_col_name_to_block_idx.empty()) {
        _col_name_to_block_idx = block->get_name_to_pos_map();
    }

    paimon_result_next_batch next = paimon_record_batch_reader_next(_handles->reader.get());
    if (next.error != nullptr) {
        return Status::InternalError("paimon-rust read batch failed: {}",
                                     consume_error(next.error));
    }
    // End of stream: both pointers are null.
    if (next.batch.array == nullptr && next.batch.schema == nullptr) {
        _reader_eof = true;
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }

    // RAII: the batch's Arrow release callbacks + container free run when
    // `batch` leaves this scope, including on any early break/continue.
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
    const auto num_rows = static_cast<size_t>(record_batch->num_rows());
    const auto num_columns = record_batch->num_columns();
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (int c = 0; c < num_columns; ++c) {
        const auto& field = record_batch->schema()->field(c);
        if (field->name() == VALUE_KIND_FIELD) {
            continue;
        }
        // Block column names are FE-normalized to lowercase. paimon-rust's
        // case_sensitive=false setting also case-folds column names in the
        // schema output, so exact match works — but we tolerate mixed-case
        // Rust output by folding here as well.
        auto it = _col_name_to_block_idx.find(field->name());
        if (it == _col_name_to_block_idx.end()) {
            it = _col_name_to_block_idx.find(to_lower(field->name()));
        }
        if (it == _col_name_to_block_idx.end()) {
            // Skip columns that are not in the block (e.g., partition columns).
            continue;
        }
        const auto block_pos = it->second;
        try {
            RETURN_IF_ERROR(columns_guard.get_datatype_by_position(block_pos)
                                    ->get_serde()
                                    ->read_column_from_arrow(*columns[block_pos],
                                                             record_batch->column(c).get(), 0,
                                                             num_rows, _ctzz));
        } catch (Exception& e) {
            return Status::InternalError("Failed to convert from arrow to block: {}", e.what());
        }
    }

    *read_rows = num_rows;
    *eof = false;
    return Status::OK();
}

Status PaimonRustReader::_get_columns_impl(
        std::unordered_map<std::string, DataTypePtr>* name_to_type) {
    for (const auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

Status PaimonRustReader::close() {
    _handles.reset();
    return Status::OK();
}

Status PaimonRustReader::_init_paimon_reader() {
    _handles = std::make_unique<PaimonHandles>();
    return _open_table_and_build_reader();
}

Status PaimonRustReader::_open_table_and_build_reader() {
    // 1. Decode the FE-planned split first so we fail fast (and without any
    // filesystem IO) when it is missing or malformed. The bytes themselves are
    // not consumed yet by the community API — see
    // _paimon_plan_from_split_bytes_mock — but the fail-fast check still
    // guards against a caller that "forgot" to attach a split.
    std::string split_bytes;
    RETURN_IF_ERROR(_decode_split_bytes(&split_bytes));

    // 2. Resolve identifier and warehouse (paimon catalog convention: the
    // table root is `<warehouse>/<db>.db/<table>`).
    auto table_path_opt = _resolve_table_path();
    if (!table_path_opt.has_value()) {
        return Status::InternalError(
                "paimon-rust missing paimon_table; cannot resolve paimon table location");
    }
    auto db_name_opt = _resolve_db_name();
    if (!db_name_opt.has_value()) {
        return Status::InternalError(
                "paimon-rust missing db_name; cannot open paimon table via catalog");
    }
    auto table_name_opt = _resolve_table_name();
    if (!table_name_opt.has_value()) {
        return Status::InternalError(
                "paimon-rust missing table_name; cannot open paimon table via catalog");
    }
    const std::string& table_path = table_path_opt.value();
    const std::string& db_name = db_name_opt.value();
    const std::string& table_name = table_name_opt.value();

    auto warehouse_opt = _derive_warehouse(table_path, db_name, table_name);
    if (!warehouse_opt.has_value()) {
        return Status::InternalError(
                "paimon-rust cannot derive warehouse from table_path='{}' (expected suffix "
                "'/{}.db/{}')",
                table_path, db_name, table_name);
    }

    // 3. Assemble catalog options: warehouse + optional storage credentials
    // from properties/paimon_options/hadoop_conf. Default to metastore=filesystem
    // for path-based paimon layouts.
    auto options = _build_options();
    options["warehouse"] = warehouse_opt.value();
    if (options.find("metastore") == options.end()) {
        options["metastore"] = "filesystem";
    }

    std::vector<paimon_option> c_options;
    c_options.reserve(options.size());
    for (const auto& kv : options) {
        c_options.push_back(paimon_option {kv.first.c_str(), kv.second.c_str()});
    }

    LOG(INFO) << "paimon-rust opening table via catalog: db=" << db_name
              << " table=" << table_name << " warehouse=" << options["warehouse"]
              << " options=[" << format_options(options) << "]";

    // 4. Create catalog.
    paimon_result_catalog_new cat_res =
            paimon_catalog_create(c_options.data(), c_options.size());
    if (cat_res.error != nullptr) {
        return Status::InternalError("paimon-rust catalog_create failed: {}; options=[{}]",
                                     consume_error(cat_res.error), format_options(options));
    }
    _handles->catalog.reset(cat_res.catalog);

    // 5. Build identifier and look up the table.
    paimon_result_identifier_new id_res =
            paimon_identifier_new(db_name.c_str(), table_name.c_str());
    if (id_res.error != nullptr) {
        return Status::InternalError("paimon-rust identifier_new failed: {}",
                                     consume_error(id_res.error));
    }
    identifier_ptr identifier(id_res.identifier);

    paimon_result_get_table tbl_res =
            paimon_catalog_get_table(_handles->catalog.get(), identifier.get());
    if (tbl_res.error != nullptr) {
        return Status::InternalError(
                "paimon-rust get_table failed: db={} table={} err={}", db_name, table_name,
                consume_error(tbl_res.error));
    }
    _handles->table.reset(tbl_res.table);

    // 6. Build the read pipeline: read_builder -> case-insensitive -> projection.
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

    // Convert the FE push-down conjuncts into a paimon-rust filter and apply it.
    RETURN_IF_ERROR(_apply_predicate());

    // 7. Deserialize the FE-planned split into a one-split plan, so this
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
        _reader_eof = true;
        return Status::OK();
    }

    // 8. Open the arrow stream over the plan.
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

Status PaimonRustReader::_apply_predicate() {
    if (_push_down_conjuncts.empty() || !_handles || !_handles->table || !_handles->read_builder) {
        return Status::OK();
    }
    LOG(INFO) << "paimon-rust predicate pushdown: " << _push_down_conjuncts.size()
              << " conjunct(s) input";
    PaimonRustPredicateConverter converter(_file_slot_descs, _state, _handles->table.get());
    paimon_predicate* predicate = converter.build(_push_down_conjuncts);
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

Status PaimonRustReader::_decode_split_bytes(std::string* out) const {
    if (!_range.__isset.table_format_params || !_range.table_format_params.__isset.paimon_params ||
        !_range.table_format_params.paimon_params.__isset.paimon_split) {
        return Status::InternalError("paimon-rust missing paimon_split in scan range");
    }
    const auto& encoded_split = _range.table_format_params.paimon_params.paimon_split;
    if (!base64_decode(encoded_split, out)) {
        return Status::InternalError("paimon-rust base64 decode paimon_split failed");
    }
    if (out->empty()) {
        return Status::InternalError("paimon-rust decoded paimon_split is empty");
    }
    return Status::OK();
}

std::optional<std::string> PaimonRustReader::_resolve_table_path() const {
    if (_range.__isset.table_format_params && _range.table_format_params.__isset.paimon_params &&
        _range.table_format_params.paimon_params.__isset.paimon_table &&
        !_range.table_format_params.paimon_params.paimon_table.empty()) {
        return _range.table_format_params.paimon_params.paimon_table;
    }
    return std::nullopt;
}

std::optional<std::string> PaimonRustReader::_resolve_db_name() const {
    if (_range.__isset.table_format_params && _range.table_format_params.__isset.paimon_params &&
        _range.table_format_params.paimon_params.__isset.db_name &&
        !_range.table_format_params.paimon_params.db_name.empty()) {
        return _range.table_format_params.paimon_params.db_name;
    }
    return std::nullopt;
}

std::optional<std::string> PaimonRustReader::_resolve_table_name() const {
    if (_range.__isset.table_format_params && _range.table_format_params.__isset.paimon_params &&
        _range.table_format_params.paimon_params.__isset.table_name &&
        !_range.table_format_params.paimon_params.table_name.empty()) {
        return _range.table_format_params.paimon_params.table_name;
    }
    return std::nullopt;
}

std::optional<std::string> PaimonRustReader::_derive_warehouse(std::string_view table_path,
                                                               std::string_view db_name,
                                                               std::string_view table_name) {
    // paimon convention: <warehouse>/<db>.db/<table>[/]
    // Strip a trailing slash first so both forms work.
    while (!table_path.empty() && table_path.back() == '/') {
        table_path.remove_suffix(1);
    }
    // Suffix should end with `/<db>.db/<table>`.
    std::string suffix;
    suffix.reserve(db_name.size() + 4 + table_name.size() + 1);
    suffix.append("/");
    suffix.append(db_name);
    suffix.append(".db/");
    suffix.append(table_name);
    if (table_path.size() <= suffix.size()) {
        return std::nullopt;
    }
    if (table_path.substr(table_path.size() - suffix.size()) != suffix) {
        return std::nullopt;
    }
    return std::string(table_path.substr(0, table_path.size() - suffix.size()));
}

std::vector<std::string> PaimonRustReader::_build_read_columns() const {
    std::vector<std::string> columns;
    columns.reserve(_file_slot_descs.size());
    for (const auto& slot : _file_slot_descs) {
        columns.emplace_back(slot->col_name());
    }
    return columns;
}

std::map<std::string, std::string> PaimonRustReader::_build_options() const {
    std::map<std::string, std::string> options;
    if (_range_params && _range_params->__isset.paimon_options &&
        !_range_params->paimon_options.empty()) {
        options.insert(_range_params->paimon_options.begin(), _range_params->paimon_options.end());
    } else if (_range.__isset.table_format_params &&
               _range.table_format_params.__isset.paimon_params &&
               _range.table_format_params.paimon_params.__isset.paimon_options) {
        options.insert(_range.table_format_params.paimon_params.paimon_options.begin(),
                       _range.table_format_params.paimon_params.paimon_options.end());
    }

    if (_range_params && _range_params->__isset.properties && !_range_params->properties.empty()) {
        for (const auto& kv : _range_params->properties) {
            options[kv.first] = kv.second;
        }
    } else if (_range.__isset.table_format_params &&
               _range.table_format_params.__isset.paimon_params &&
               _range.table_format_params.paimon_params.__isset.hadoop_conf) {
        for (const auto& kv : _range.table_format_params.paimon_params.hadoop_conf) {
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

} // namespace doris
