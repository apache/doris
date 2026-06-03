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
#include <string>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/status.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/string_ref.h"
#include "exec/common/util.hpp"
#include "exec/operator/scan_operator.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vexpr_context.h"
#include "format/format_common.h"
#include "format/reader/table/hive_reader.h"
#include "format/reader/table/paimon_reader.h"
#include "format/reader/table_reader.h"
#include "format/table/iceberg_reader_v2.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

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
    return table_format == "NotSet" || table_format == "tvf" || table_format == "hive" ||
           table_format == "iceberg" || table_format == "paimon";
}

bool is_partition_slot(const TFileScanSlotInfo& slot_info) {
    return slot_info.__isset.category ? slot_info.category == TColumnCategory::PARTITION_KEY
                                      : !slot_info.is_file_slot;
}

} // namespace

bool FileScannerV2::is_supported(const TFileScanRangeParams& params,
                                 const TFileRangeDesc& range) {
    return get_range_format_type(params, range) == TFileFormatType::FORMAT_PARQUET &&
           is_supported_table_format(range);
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
        RETURN_IF_ERROR(_init_expr_ctxes());
    }
    return Status::OK();
}

Status FileScannerV2::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    while (true) {
        RETURN_IF_CANCELLED(state);
        if (_table_reader == nullptr) {
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
            RETURN_IF_ERROR(_table_reader->close());
            _table_reader.reset();
            _state->update_num_finished_scan_range(1);
            *eof = false;
            continue;
        }
        return Status::OK();
    }
}

Status FileScannerV2::_prepare_next_split(bool* eos) {
    if (_table_reader != nullptr) {
        RETURN_IF_ERROR(_table_reader->close());
        _table_reader.reset();
        _state->update_num_finished_scan_range(1);
    }

    bool has_next = _first_scan_range;
    if (!_first_scan_range) {
        RETURN_IF_ERROR(_split_source->get_next(&has_next, &_current_range));
    }
    _first_scan_range = false;
    if (!has_next || _should_stop) {
        *eos = true;
        return Status::OK();
    }
    _current_range_path = _current_range.path;
    RETURN_IF_ERROR(_create_table_reader(_current_range));
    RETURN_IF_ERROR(_prepare_table_reader_split(_current_range));
    COUNTER_UPDATE(_file_counter, 1);
    *eos = false;
    return Status::OK();
}

Status FileScannerV2::_create_table_reader(const TFileRangeDesc& range) {
    const auto format_type = _get_current_format_type();
    reader::FileFormat format;
    RETURN_IF_ERROR(_to_file_format(format_type, &format));
    RETURN_IF_ERROR(_create_table_reader_for_format(range));
    DORIS_CHECK(_table_reader != nullptr);

    reader::TableColumnPredicates table_column_predicates;
    RETURN_IF_ERROR(_build_table_column_predicates(&table_column_predicates));
    RETURN_IF_ERROR(_table_reader->init({
            .projected_columns = _projected_columns,
            .column_predicates = std::move(table_column_predicates),
            .conjuncts = VExprContext(_build_conjunct_root()),
            .format = format,
            .scan_params = const_cast<TFileScanRangeParams*>(_params),
            .io_ctx = _io_ctx,
            .runtime_state = _state,
            .scanner_profile = _local_state->scanner_profile(),
            .allow_missing_columns = true,
            .push_down_agg_type = _local_state->get_push_down_agg_type(),
            .profile = nullptr,
    }));
    return Status::OK();
}

Status FileScannerV2::_create_table_reader_for_format(const TFileRangeDesc& range) {
    const auto table_format = table_format_name(range);
    if (table_format == "NotSet" || table_format == "tvf") {
        _table_reader = std::make_unique<reader::TableReader>();
    } else if (table_format == "hive") {
        _table_reader = hive::HiveReader::create_unique();
    } else if (table_format == "iceberg") {
        _table_reader = std::make_unique<iceberg::IcebergTableReader>();
    } else if (table_format == "paimon") {
        _table_reader = paimon::PaimonReader::create_unique();
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
    }));
    return Status::OK();
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
        RETURN_IF_ERROR(_parse_partition_value(it->second, value, is_null, &field));
        partition_values->emplace(key, std::move(field));
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
    _partition_slot_descs.clear();
    for (const auto* slot_desc : _output_tuple_desc->slots()) {
        _slot_id_to_desc.emplace(slot_desc->id(), slot_desc);
    }
    RETURN_IF_ERROR(_build_projected_columns());
    return Status::OK();
}

Status FileScannerV2::_build_projected_columns() {
    _projected_columns.clear();
    _projected_columns.reserve(_params->required_slots.size());

    for (const auto& slot_info : _params->required_slots) {
        const auto it = _slot_id_to_desc.find(slot_info.slot_id);
        if (it == _slot_id_to_desc.end()) {
            return Status::InternalError("Unknown source slot descriptor, slot_id={}",
                                         slot_info.slot_id);
        }
        auto column = _build_table_column(it->second);
        if (is_partition_slot(slot_info)) {
            column.is_partition_key = true;
            _partition_slot_descs.emplace(column.name, it->second);
        }
        _projected_columns.push_back(std::move(column));
    }
    return Status::OK();
}

reader::TableColumn FileScannerV2::_build_table_column(const SlotDescriptor* slot_desc) {
    DORIS_CHECK(slot_desc != nullptr);
    reader::TableColumn column;
    column.id = slot_desc->col_unique_id();
    column.name = slot_desc->col_name();
    column.type = slot_desc->get_data_type_ptr();
    return column;
}

Status FileScannerV2::_build_table_column_predicates(
        reader::TableColumnPredicates* predicates) const {
    DORIS_CHECK(predicates != nullptr);
    predicates->clear();
    const auto& slot_predicates = _local_state->cast<FileScanLocalState>()._slot_id_to_predicates;
    for (const auto& [slot_id, slot_predicate_list] : slot_predicates) {
        const auto it = _slot_id_to_desc.find(slot_id);
        if (it == _slot_id_to_desc.end()) {
            continue;
        }
        (*predicates)[it->second->col_unique_id()] = slot_predicate_list;
    }
    return Status::OK();
}

VExprSPtr FileScannerV2::_build_conjunct_root() const {
    if (_conjuncts.empty()) {
        return nullptr;
    }
    if (_conjuncts.size() == 1) {
        return _conjuncts.front()->root();
    }
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::COMPOUND_PRED);
    node.__set_opcode(TExprOpcode::COMPOUND_AND);
    node.__set_num_children(cast_set<int>(_conjuncts.size()));
    auto compound = VCompoundPred::create_shared(node);
    for (const auto& conjunct : _conjuncts) {
        compound->add_child(conjunct->root());
    }
    return compound;
}

TFileFormatType::type FileScannerV2::_get_current_format_type() const {
    return get_range_format_type(*_params, _current_range);
}

Status FileScannerV2::_to_file_format(TFileFormatType::type format_type,
                                      reader::FileFormat* format) {
    DORIS_CHECK(format != nullptr);
    switch (format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        *format = reader::FileFormat::PARQUET;
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
        _table_reader.reset();
    }
    return Scanner::close(state);
}

void FileScannerV2::try_stop() {
    _should_stop = true;
    if (_table_reader != nullptr) {
        static_cast<void>(_table_reader->close());
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

} // namespace doris
