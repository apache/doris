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

#include "exec/scan/file_scanner_v2.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/string_ref.h"
#include "exec/common/util.hpp"
#include "exec/operator/scan_operator.h"
#include "exec/scan/access_path_parser.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format/format_common.h"
#include "format_v2/column_mapper.h"
#include "format_v2/jni/iceberg_sys_table_reader.h"
#include "format_v2/jni/jdbc_reader.h"
#include "format_v2/table/hive_reader.h"
#include "format_v2/table/hudi_reader.h"
#include "format_v2/table/iceberg_reader.h"
#include "format_v2/table/paimon_reader.h"
#include "format_v2/table_reader.h"
#include "io/fs/file_meta_cache.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "storage/id_manager.h"

namespace doris {
namespace {

std::string table_format_name(const TFileRangeDesc& range) {
    return range.__isset.table_format_params ? range.table_format_params.table_format_type
                                             : "NotSet";
}

TFileFormatType::type get_range_format_type(const TFileScanRangeParams& params,
                                            const TFileRangeDesc& range) {
    return range.__isset.format_type ? range.format_type : params.format_type;
}

bool is_supported_table_format(const TFileRangeDesc& range) {
    const auto table_format = table_format_name(range);
    if (table_format == "hudi" && range.__isset.table_format_params &&
        range.table_format_params.__isset.hudi_params &&
        range.table_format_params.hudi_params.__isset.delta_logs &&
        !range.table_format_params.hudi_params.delta_logs.empty()) {
        // Hudi MOR splits need log-file merge semantics and must stay on the existing JNI path.
        // FileScannerV2 currently supports native Parquet data files only.
        return false;
    }
    return table_format == "NotSet" || table_format == "tvf" || table_format == "hive" ||
           table_format == "iceberg" || table_format == "paimon" || table_format == "hudi";
}

bool is_supported_jni_table_format(const TFileRangeDesc& range) {
    const auto table_format = table_format_name(range);
    if (table_format == "paimon") {
        return range.__isset.table_format_params &&
               range.table_format_params.__isset.paimon_params &&
               range.table_format_params.paimon_params.__isset.reader_type &&
               range.table_format_params.paimon_params.reader_type == TPaimonReaderType::PAIMON_JNI;
    }
    return table_format == "jdbc" || table_format == "iceberg";
}

bool is_csv_format(TFileFormatType::type format_type) {
    switch (format_type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZ4BLOCK:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
    case TFileFormatType::FORMAT_CSV_SNAPPYBLOCK:
    case TFileFormatType::FORMAT_PROTO:
        return true;
    default:
        return false;
    }
}

bool is_text_format(TFileFormatType::type format_type) {
    return format_type == TFileFormatType::FORMAT_TEXT;
}

bool is_partition_slot(const TFileScanSlotInfo& slot_info, const std::string& column_name) {
    if (column_name.starts_with(BeConsts::GLOBAL_ROWID_COL) ||
        column_name == BeConsts::ICEBERG_ROWID_COL) {
        return false;
    }
    return slot_info.__isset.category ? slot_info.category == TColumnCategory::PARTITION_KEY
                                      : !slot_info.is_file_slot;
}

bool is_data_file_slot(const TFileScanSlotInfo& slot_info, const std::string& column_name) {
    if (column_name.starts_with(BeConsts::GLOBAL_ROWID_COL) ||
        column_name == BeConsts::ICEBERG_ROWID_COL) {
        return false;
    }
    // CSV and other non-self-describing formats need FE slot descriptors for only the columns that
    // are physically read from the file. Partition/default/virtual columns stay in TableReader's
    // mapping layer and are materialized after the file-local block is read. New FE provides an
    // explicit category; old FE falls back to `is_file_slot`.
    if (slot_info.__isset.category) {
        return slot_info.category == TColumnCategory::REGULAR ||
               slot_info.category == TColumnCategory::GENERATED;
    }
    return slot_info.is_file_slot;
}

Status rewrite_slot_refs_to_global_index(
        VExprSPtr* expr,
        const std::unordered_map<int32_t, format::GlobalIndex>& slot_id_to_global_index) {
    DORIS_CHECK(expr != nullptr);
    if (*expr == nullptr) {
        return Status::OK();
    }
    if ((*expr)->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(expr->get());
        const auto global_index_it = slot_id_to_global_index.find(slot_ref->slot_id());
        if (global_index_it == slot_id_to_global_index.end()) {
            DORIS_CHECK(slot_ref->slot_id() >= 0);
            const auto global_index = format::GlobalIndex(cast_set<size_t>(slot_ref->slot_id()));
            *expr = VSlotRef::create_shared(cast_set<int>(global_index.value()),
                                            cast_set<int>(global_index.value()), -1,
                                            slot_ref->data_type(), slot_ref->column_name());
            RETURN_IF_ERROR(expr->get()->prepare(nullptr, RowDescriptor(), nullptr));
            return Status::OK();
        }
        const auto global_index = global_index_it->second;
        *expr = VSlotRef::create_shared(cast_set<int>(global_index.value()),
                                        cast_set<int>(global_index.value()), -1,
                                        slot_ref->data_type(), slot_ref->column_name());
        RETURN_IF_ERROR(expr->get()->prepare(nullptr, RowDescriptor(), nullptr));
        return Status::OK();
    }
    auto children = (*expr)->children();
    for (auto& child : children) {
        if (child == nullptr) {
            continue;
        }
        RETURN_IF_ERROR(rewrite_slot_refs_to_global_index(&child, slot_id_to_global_index));
    }
    (*expr)->set_children(std::move(children));
    return Status::OK();
}

} // namespace

#ifdef BE_TEST
Status FileScannerV2::TEST_to_file_format(TFileFormatType::type format_type,
                                          format::FileFormat* file_format) {
    return _to_file_format(format_type, file_format);
}

bool FileScannerV2::TEST_is_partition_slot(const TFileScanSlotInfo& slot_info,
                                           const std::string& column_name) {
    return is_partition_slot(slot_info, column_name);
}

bool FileScannerV2::TEST_is_data_file_slot(const TFileScanSlotInfo& slot_info,
                                           const std::string& column_name) {
    return is_data_file_slot(slot_info, column_name);
}

Status FileScannerV2::TEST_rewrite_slot_refs_to_global_index(
        VExprSPtr* expr,
        const std::unordered_map<int32_t, format::GlobalIndex>& slot_id_to_global_index) {
    return rewrite_slot_refs_to_global_index(expr, slot_id_to_global_index);
}
#endif

bool FileScannerV2::is_supported(const TFileScanRangeParams& params, const TFileRangeDesc& range) {
    const auto format_type = get_range_format_type(params, range);
    if (format_type == TFileFormatType::FORMAT_PARQUET) {
        return is_supported_table_format(range);
    } else if (format_type == TFileFormatType::FORMAT_JNI) {
        return is_supported_jni_table_format(range);
    } else if (is_csv_format(format_type) || is_text_format(format_type)) {
        return is_supported_table_format(range);
    } else {
        LOG(WARNING) << "Unsupported file format type " << format_type << " for file scanner v2";
        return false;
    }
}

FileScannerV2::FileScannerV2(RuntimeState* state, FileScanLocalState* local_state, int64_t limit,
                             std::shared_ptr<SplitSourceConnector> split_source,
                             RuntimeProfile* profile, ShardedKVCache* kv_cache,
                             const std::unordered_map<std::string, int>* colname_to_slot_id)
        : Scanner(state, local_state, limit, profile),
          _split_source(std::move(split_source)),
          _kv_cache(kv_cache) {
    (void)colname_to_slot_id;
    if (state->get_query_ctx() != nullptr &&
        state->get_query_ctx()->file_scan_range_params_map.count(local_state->parent_id()) > 0) {
        _params = &(state->get_query_ctx()->file_scan_range_params_map[local_state->parent_id()]);
    } else {
        _params = _split_source->get_params();
    }
}

Status FileScannerV2::init(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    RETURN_IF_ERROR(Scanner::init(state, conjuncts));
    _get_block_timer =
            ADD_TIMER_WITH_LEVEL(_local_state->scanner_profile(), "FileScannerV2GetBlockTime", 1);
    _file_counter =
            ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(), "FileNumber", TUnit::UNIT, 1);
    _file_read_bytes_counter = ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(),
                                                      "FileReadBytes", TUnit::BYTES, 1);
    _file_read_calls_counter = ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(),
                                                      "FileReadCalls", TUnit::UNIT, 1);
    _file_read_time_counter =
            ADD_TIMER_WITH_LEVEL(_local_state->scanner_profile(), "FileReadTime", 1);
    _file_cache_statistics = std::make_unique<io::FileCacheStatistics>();
    _file_reader_stats = std::make_unique<io::FileReaderStats>();
    RETURN_IF_ERROR(_init_io_ctx());
    _io_ctx->file_cache_stats = _file_cache_statistics.get();
    _io_ctx->file_reader_stats = _file_reader_stats.get();
    _io_ctx->is_disposable = _state->query_options().disable_file_cache;
    return Status::OK();
}

Status FileScannerV2::_open_impl(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(Scanner::_open_impl(state));
    RETURN_IF_ERROR(_split_source->get_next(&_first_scan_range, &_current_range));
    if (_first_scan_range) {
        RETURN_IF_ERROR(_create_table_reader_for_format(_current_range, &_table_reader));
        DORIS_CHECK(_table_reader != nullptr);
        RETURN_IF_ERROR(_init_expr_ctxes());
        RETURN_IF_ERROR(_init_table_reader(_current_range));
    }
    return Status::OK();
}

Status FileScannerV2::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    while (true) {
        RETURN_IF_CANCELLED(state);
        if (!_has_prepared_split) {
            RETURN_IF_ERROR(_prepare_next_split(eof));
            if (*eof) {
                return Status::OK();
            }
        }

        {
            SCOPED_TIMER(_get_block_timer);
            RETURN_IF_ERROR(_table_reader->get_block(block, eof));
        }
        if (*eof) {
            _state->update_num_finished_scan_range(1);
            _has_prepared_split = false;
            *eof = false;
            continue;
        }
        return Status::OK();
    }
}

Status FileScannerV2::_prepare_next_split(bool* eos) {
    bool has_next = _first_scan_range;
    if (!_first_scan_range) {
        RETURN_IF_ERROR(_split_source->get_next(&has_next, &_current_range));
    }
    _first_scan_range = false;
    if (!has_next || _should_stop) {
        *eos = true;
        return Status::OK();
    }
    DORIS_CHECK(_table_reader != nullptr);
    _current_range_path = _current_range.path;
    RETURN_IF_ERROR(_prepare_table_reader_split(_current_range));
    COUNTER_UPDATE(_file_counter, 1);
    _has_prepared_split = true;
    *eos = false;
    return Status::OK();
}

Status FileScannerV2::_init_table_reader(const TFileRangeDesc& range) {
    const auto format_type = get_range_format_type(*_params, range);
    format::FileFormat file_format;
    RETURN_IF_ERROR(_to_file_format(format_type, &file_format));
    DORIS_CHECK(_table_reader != nullptr);

    format::TableColumnPredicates table_column_predicates;
    RETURN_IF_ERROR(_build_table_column_predicates(&table_column_predicates));
    VExprContextSPtrs table_conjuncts;
    RETURN_IF_ERROR(_build_table_conjuncts(&table_conjuncts));
    RETURN_IF_ERROR(_table_reader->init({
            .projected_columns = _projected_columns,
            .column_predicates = std::move(table_column_predicates),
            .conjuncts = std::move(table_conjuncts),
            .format = file_format,
            .scan_params = const_cast<TFileScanRangeParams*>(_params),
            .io_ctx = _io_ctx,
            .runtime_state = _state,
            .scanner_profile = _local_state->scanner_profile(),
            .file_slot_descs = &_file_slot_descs,
            .push_down_agg_type = _local_state->get_push_down_agg_type(),
            .condition_cache_digest = _local_state->get_condition_cache_digest(),
    }));
    return Status::OK();
}

Status FileScannerV2::_create_table_reader_for_format(
        const TFileRangeDesc& range, std::unique_ptr<format::TableReader>* reader) const {
    DORIS_CHECK(reader != nullptr);
    const auto table_format = table_format_name(range);
    if (table_format == "NotSet" || table_format == "tvf") {
        *reader = std::make_unique<format::TableReader>();
    } else if (table_format == "hive") {
        *reader = format::hive::HiveReader::create_unique();
    } else if (table_format == "iceberg") {
        if (get_range_format_type(*_params, range) == TFileFormatType::FORMAT_JNI) {
            *reader = std::make_unique<format::iceberg::IcebergSysTableJniReader>();
        } else {
            *reader = std::make_unique<format::iceberg::IcebergTableReader>();
        }
    } else if (table_format == "paimon") {
        *reader = std::make_unique<format::paimon::PaimonHybridReader>();
    } else if (table_format == "hudi") {
        *reader = format::hudi::HudiReader::create_unique();
    } else if (table_format == "jdbc") {
        *reader = std::make_unique<format::jdbc::JdbcJniReader>();
    } else {
        return Status::NotSupported("FileScannerV2 does not support table format {}", table_format);
    }
    return Status::OK();
}

Status FileScannerV2::_prepare_table_reader_split(const TFileRangeDesc& range) {
    std::map<std::string, Field> partition_values;
    RETURN_IF_ERROR(_generate_partition_values(range, &partition_values));
    RETURN_IF_ERROR(_table_reader->prepare_split({
            .partition_values = std::move(partition_values),
            .cache = _kv_cache,
            .current_range = range,
            .global_rowid_context = _create_global_rowid_context(range),
    }));
    return Status::OK();
}

bool FileScannerV2::_should_enable_file_meta_cache() const {
    return ExecEnv::GetInstance()->file_meta_cache()->enabled() &&
           _split_source->num_scan_ranges() < config::max_external_file_meta_cache_num / 3;
}

std::optional<format::GlobalRowIdContext> FileScannerV2::_create_global_rowid_context(
        const TFileRangeDesc& range) const {
    if (!_need_global_rowid_column) {
        return std::nullopt;
    }
    auto& id_file_map = _state->get_id_file_map();
    DORIS_CHECK(id_file_map != nullptr);
    const auto file_id = id_file_map->get_file_mapping_id(
            std::make_shared<FileMapping>(_local_state->cast<FileScanLocalState>().parent_id(),
                                          range, _should_enable_file_meta_cache()));
    return format::GlobalRowIdContext {
            .version = IdManager::ID_VERSION,
            .backend_id = BackendOptions::get_backend_id(),
            .file_id = file_id,
    };
}

Status FileScannerV2::_generate_partition_values(
        const TFileRangeDesc& range, std::map<std::string, Field>* partition_values) const {
    DORIS_CHECK(partition_values != nullptr);
    partition_values->clear();
    if (!range.__isset.columns_from_path_keys || !range.__isset.columns_from_path) {
        return Status::OK();
    }
    DORIS_CHECK(range.columns_from_path_keys.size() == range.columns_from_path.size());
    for (size_t idx = 0; idx < range.columns_from_path_keys.size(); ++idx) {
        const auto& key = range.columns_from_path_keys[idx];
        const auto it = _partition_slot_descs.find(key);
        if (it == _partition_slot_descs.end()) {
            continue;
        }
        const auto& value = range.columns_from_path[idx];
        const bool is_null = range.__isset.columns_from_path_is_null &&
                             idx < range.columns_from_path_is_null.size() &&
                             range.columns_from_path_is_null[idx];
        Field field;
        DORIS_CHECK(it->second.slot_desc != nullptr);
        RETURN_IF_ERROR(_parse_partition_value(it->second.slot_desc, value, is_null, &field));
        partition_values->emplace(it->second.canonical_name, std::move(field));
    }
    return Status::OK();
}

Status FileScannerV2::_parse_partition_value(const SlotDescriptor* slot_desc,
                                             const std::string& value, bool is_null,
                                             Field* field) const {
    DORIS_CHECK(slot_desc != nullptr);
    DORIS_CHECK(field != nullptr);
    if (is_null) {
        *field = Field::create_field<TYPE_NULL>(Null());
        return Status::OK();
    }
    const auto data_type = remove_nullable(slot_desc->get_data_type_ptr());
    auto column = data_type->create_column();
    auto serde = data_type->get_serde();
    DataTypeSerDe::FormatOptions options;
    options.converted_from_string = true;
    StringRef ref(value.data(), value.size());
    RETURN_IF_ERROR(serde->from_string(ref, *column, options));
    DORIS_CHECK(column->size() == 1);
    *field = (*column)[0];
    return Status::OK();
}

Status FileScannerV2::_init_expr_ctxes() {
    _slot_id_to_desc.clear();
    _slot_id_to_global_index.clear();
    _partition_slot_descs.clear();
    _file_slot_descs.clear();
    for (const auto* slot_desc : _output_tuple_desc->slots()) {
        _slot_id_to_desc.emplace(slot_desc->id(), slot_desc);
    }
    DORIS_CHECK(_table_reader != nullptr);
    RETURN_IF_ERROR(_build_projected_columns(*_table_reader));
    return Status::OK();
}

Status FileScannerV2::_build_projected_columns(const format::TableReader& table_reader) {
    _projected_columns.clear();
    _projected_columns.reserve(_params->required_slots.size());
    _need_global_rowid_column = false;
    format::ProjectedColumnBuildContext build_context {
            .scan_params = _params,
            .range = &_current_range,
            .runtime_state = _state,
    };

    for (size_t slot_idx = 0; slot_idx < _params->required_slots.size(); ++slot_idx) {
        const auto& slot_info = _params->required_slots[slot_idx];
        const auto it = _slot_id_to_desc.find(slot_info.slot_id);
        if (it == _slot_id_to_desc.end()) {
            return Status::InternalError("Unknown source slot descriptor, slot_id={}",
                                         slot_info.slot_id);
        }
        auto column = _build_table_column(it->second);
        if (column.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            _need_global_rowid_column = true;
        }
        RETURN_IF_ERROR(_build_default_expr(slot_info, &column.default_expr));
        build_context.schema_column.reset();
        RETURN_IF_ERROR(table_reader.annotate_projected_column(slot_info, &build_context, &column));
        // Build nested children from access paths generated by the slot's access-path
        // expressions. A projected column can therefore contain only a subset of the schema
        // column's nested children.
        RETURN_IF_ERROR(AccessPathParser::build_nested_children(
                &column, it->second,
                build_context.schema_column.has_value() ? &*build_context.schema_column : nullptr));
        if (is_partition_slot(slot_info, column.name)) {
            column.is_partition_key = true;
            _partition_slot_descs.emplace(
                    column.name,
                    PartitionSlotInfo {.slot_desc = it->second, .canonical_name = column.name});
            for (const auto& alias : column.name_mapping) {
                _partition_slot_descs.emplace(
                        alias,
                        PartitionSlotInfo {.slot_desc = it->second, .canonical_name = column.name});
            }
        } else if (is_data_file_slot(slot_info, column.name)) {
            _file_slot_descs.push_back(const_cast<SlotDescriptor*>(it->second));
        }
        const auto global_index = format::GlobalIndex(slot_idx);
        _slot_id_to_global_index.emplace(slot_info.slot_id, global_index);
        _projected_columns.push_back(std::move(column));
    }
    RETURN_IF_ERROR(table_reader.validate_projected_columns(build_context));
    return Status::OK();
}

Status FileScannerV2::_build_default_expr(const TFileScanSlotInfo& slot_info,
                                          VExprContextSPtr* ctx) const {
    DORIS_CHECK(ctx != nullptr);
    if (slot_info.__isset.default_value_expr && !slot_info.default_value_expr.nodes.empty()) {
        return VExpr::create_expr_tree(slot_info.default_value_expr, *ctx);
    }

    if (_params->__isset.default_value_of_src_slot) {
        const auto it = _params->default_value_of_src_slot.find(slot_info.slot_id);
        if (it != _params->default_value_of_src_slot.end() && !it->second.nodes.empty()) {
            return VExpr::create_expr_tree(it->second, *ctx);
        }
    }
    return Status::OK();
}

format::ColumnDefinition FileScannerV2::_build_table_column(const SlotDescriptor* slot_desc) {
    DORIS_CHECK(slot_desc != nullptr);
    format::ColumnDefinition column;
    // TODO(gabriel): why always BY_NAME here?
    column.identifier = Field::create_field<TYPE_STRING>(slot_desc->col_name());
    column.name = slot_desc->col_name();
    column.type = slot_desc->get_data_type_ptr();
    return column;
}

Status FileScannerV2::_build_table_column_predicates(
        format::TableColumnPredicates* predicates) const {
    DORIS_CHECK(predicates != nullptr);
    predicates->clear();
    const auto& slot_predicates = _local_state->cast<FileScanLocalState>()._slot_id_to_predicates;
    for (const auto& [slot_id, slot_predicate_list] : slot_predicates) {
        const auto it = _slot_id_to_desc.find(slot_id);
        if (it == _slot_id_to_desc.end()) {
            continue;
        }
        const auto global_index_it = _slot_id_to_global_index.find(slot_id);
        if (global_index_it == _slot_id_to_global_index.end()) {
            continue;
        }
        (*predicates)[global_index_it->second] = slot_predicate_list;
    }
    return Status::OK();
}

Status FileScannerV2::_build_table_conjuncts(VExprContextSPtrs* conjuncts) const {
    DORIS_CHECK(conjuncts != nullptr);
    conjuncts->clear();
    conjuncts->reserve(_conjuncts.size());
    for (const auto& conjunct : _conjuncts) {
        VExprSPtr root;
        RETURN_IF_ERROR(format::clone_table_expr_tree(conjunct->root(), &root));
        RETURN_IF_ERROR(rewrite_slot_refs_to_global_index(&root, _slot_id_to_global_index));
        conjuncts->push_back(VExprContext::create_shared(std::move(root)));
    }
    return Status::OK();
}

TFileFormatType::type FileScannerV2::_get_current_format_type() const {
    return get_range_format_type(*_params, _current_range);
}

Status FileScannerV2::_to_file_format(TFileFormatType::type format_type,
                                      format::FileFormat* file_format) {
    DORIS_CHECK(file_format != nullptr);
    switch (format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        *file_format = format::FileFormat::PARQUET;
        return Status::OK();
    case TFileFormatType::FORMAT_JNI:
        *file_format = format::FileFormat::JNI;
        return Status::OK();
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZ4BLOCK:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
    case TFileFormatType::FORMAT_CSV_SNAPPYBLOCK:
    case TFileFormatType::FORMAT_PROTO:
        *file_format = format::FileFormat::CSV;
        return Status::OK();
    case TFileFormatType::FORMAT_TEXT:
        *file_format = format::FileFormat::TEXT;
        return Status::OK();
    default:
        return Status::NotSupported("FileScannerV2 does not support file format {}",
                                    to_string(format_type));
    }
}

Status FileScannerV2::_init_io_ctx() {
    _io_ctx = std::make_shared<io::IOContext>();
    _io_ctx->query_id = &_state->query_id();
    return Status::OK();
}

Status FileScannerV2::close(RuntimeState* state) {
    if (!_try_close()) {
        return Status::OK();
    }
    if (_table_reader != nullptr) {
        RETURN_IF_ERROR(_table_reader->close());
        _report_condition_cache_profile();
        _table_reader.reset();
    }
    return Scanner::close(state);
}

void FileScannerV2::try_stop() {
    Scanner::try_stop();
    if (_io_ctx) {
        _io_ctx->should_stop = true;
    }
}

void FileScannerV2::update_realtime_counters() {
    if (_file_reader_stats == nullptr) {
        return;
    }
    const int64_t bytes_read = _file_reader_stats->read_bytes;
    COUNTER_SET(_file_read_bytes_counter, bytes_read);
    COUNTER_SET(_file_read_calls_counter, cast_set<int64_t>(_file_reader_stats->read_calls));
    COUNTER_SET(_file_read_time_counter, cast_set<int64_t>(_file_reader_stats->read_time_ns));
}

void FileScannerV2::_collect_profile_before_close() {
    _report_file_reader_predicate_filtered_rows();
    Scanner::_collect_profile_before_close();
    if (_file_reader_stats != nullptr) {
        COUNTER_SET(_file_read_bytes_counter, cast_set<int64_t>(_file_reader_stats->read_bytes));
        COUNTER_SET(_file_read_calls_counter, cast_set<int64_t>(_file_reader_stats->read_calls));
        COUNTER_SET(_file_read_time_counter, cast_set<int64_t>(_file_reader_stats->read_time_ns));
    }
    // Query profiles can be collected before Scanner::close() runs. Publish condition-cache
    // counters here as well, using deltas so this method and close() cannot double count.
    _report_condition_cache_profile();
}

void FileScannerV2::_report_file_reader_predicate_filtered_rows() {
    const int64_t filtered_rows = _io_ctx != nullptr ? _io_ctx->predicate_filtered_rows : 0;
    const int64_t filtered_delta = filtered_rows - _reported_predicate_filtered_rows;
    if (filtered_delta > 0) {
        // File readers can evaluate localized conjuncts before a block reaches Scanner. Count
        // those rows as scanner-level unselected rows so load statistics stay identical no matter
        // whether a predicate is pushed down or evaluated by Scanner::_filter_output_block().
        _counter.num_rows_unselected += filtered_delta;
        _reported_predicate_filtered_rows = filtered_rows;
    }
}

void FileScannerV2::_report_condition_cache_profile() {
    auto* local_state = static_cast<FileScanLocalState*>(_local_state);
    const int64_t hit_count =
            _table_reader != nullptr ? _table_reader->condition_cache_hit_count() : 0;
    const int64_t hit_delta = hit_count - _reported_condition_cache_hit_count;
    if (hit_delta > 0) {
        COUNTER_UPDATE(local_state->_condition_cache_hit_counter, hit_delta);
        _reported_condition_cache_hit_count = hit_count;
    }
    const int64_t filtered_rows = _io_ctx != nullptr ? _io_ctx->condition_cache_filtered_rows : 0;
    const int64_t filtered_delta = filtered_rows - _reported_condition_cache_filtered_rows;
    if (filtered_delta > 0) {
        COUNTER_UPDATE(local_state->_condition_cache_filtered_rows_counter, filtered_delta);
        _reported_condition_cache_filtered_rows = filtered_rows;
    }
}

} // namespace doris
