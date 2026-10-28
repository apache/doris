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

#include "format_v2/table/load_reader.h"

#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type_serde/data_type_serde.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "format_v2/arrow/arrow_stream_reader.h"
#include "format_v2/wal/wal_reader.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris::format {

LoadTableReader::LoadTableReader(TupleDescriptor* source, const TupleDescriptor* destination)
        : _source_tuple(source), _destination_tuple(destination), _source_row(source, false) {}

LoadTableReader::~LoadTableReader() = default;

Status LoadTableReader::init(TableReadOptions&& options) {
    _scan_params = options.scan_params;
    _runtime_state = options.runtime_state;
    _format = options.format;
    RETURN_IF_ERROR(_init_source_columns());
    if (_scan_params->__isset.pre_filter_exprs_list) {
        RETURN_IF_ERROR(
                VExpr::create_expr_trees(_scan_params->pre_filter_exprs_list, _pre_filters));
    } else if (_scan_params->__isset.pre_filter_exprs) {
        VExprContextSPtr expr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(_scan_params->pre_filter_exprs, expr));
        _pre_filters.push_back(std::move(expr));
    }
    RETURN_IF_ERROR(VExpr::prepare(_pre_filters, _runtime_state, _source_row));
    RETURN_IF_ERROR(VExpr::open(_pre_filters, _runtime_state));
    for (auto* slot : _destination_tuple->slots()) {
        const auto it = _scan_params->expr_of_dest_slot.find(slot->id());
        if (it == _scan_params->expr_of_dest_slot.end() || it->second.nodes.empty()) {
            return Status::InvalidArgument("Missing load expression for destination slot {}",
                                           slot->id());
        }
        VExprContextSPtr expr;
        RETURN_IF_ERROR(VExpr::create_expr_tree(it->second, expr));
        RETURN_IF_ERROR(expr->prepare(_runtime_state, _source_row));
        RETURN_IF_ERROR(expr->open(_runtime_state));
        _destination_exprs.push_back(std::move(expr));
        int source_position = -1;
        const auto direct = _scan_params->dest_sid_to_src_sid_without_trans.find(slot->id());
        if (direct != _scan_params->dest_sid_to_src_sid_without_trans.end()) {
            for (size_t i = 0; i < _source_tuple->slots().size(); ++i) {
                if (_source_tuple->slots()[i]->id() == direct->second) {
                    source_position = cast_set<int>(i);
                    break;
                }
            }
            if (source_position < 0) {
                return Status::InvalidArgument("Unknown load source slot {}", direct->second);
            }
        }
        _direct_source_positions.push_back(source_position);
        if (slot->is_sequence_col()) {
            _sequence_uid = slot->col_unique_id();
        }
    }
    // Destination predicates must run after conversions and error accounting. Passing them to
    // the source reader would both bind the wrong slots and change rejected-row statistics.
    return TableReader::init({.projected_columns = std::move(_source_columns),
                              .conjuncts = {},
                              .format = options.format,
                              .scan_params = options.scan_params,
                              .io_ctx = options.io_ctx,
                              .runtime_state = options.runtime_state,
                              .scanner_profile = options.scanner_profile,
                              .parquet_profile_cache = options.parquet_profile_cache,
                              .file_slot_descs = &_source_file_slots,
                              .push_down_agg_type = TPushAggOp::NONE,
                              .condition_cache_digest = 0});
}

Status LoadTableReader::_init_source_columns() {
    const auto& slots = _source_tuple->slots();
    for (size_t i = 0; i < slots.size(); ++i) {
        auto* slot = slots[i];
        ColumnDefinition column;
        column.identifier = Field::create_field<TYPE_STRING>(slot->col_name());
        column.name = slot->col_name();
        column.type = slot->get_data_type_ptr();
        const auto required =
                std::ranges::find_if(_scan_params->required_slots,
                                     [&](const auto& info) { return info.slot_id == slot->id(); });
        if (required == _scan_params->required_slots.end()) {
            return Status::InvalidArgument("Missing load source slot {} in required slots",
                                           slot->id());
        }
        if (required->is_file_slot) {
            _source_file_slots.push_back(slot);
        } else {
            column.is_partition_key = true;
            _partition_positions.push_back(cast_set<int>(i));
        }
        const auto default_it = _scan_params->default_value_of_src_slot.find(slot->id());
        if (default_it != _scan_params->default_value_of_src_slot.end() &&
            !default_it->second.nodes.empty()) {
            RETURN_IF_ERROR(VExpr::create_expr_tree(default_it->second, column.default_expr));
            VExprContextSPtr expr;
            RETURN_IF_ERROR(VExpr::create_expr_tree(default_it->second, expr));
            RETURN_IF_ERROR(expr->prepare(_runtime_state, _source_row));
            RETURN_IF_ERROR(expr->open(_runtime_state));
            _source_defaults.emplace(i, std::move(expr));
        }
        if (slot->is_skip_bitmap_col()) {
            _skip_bitmap_position = cast_set<int>(i);
        }
        if (_scan_params->__isset.sequence_map_col &&
            slot->col_name() == _scan_params->sequence_map_col) {
            _sequence_map_uid = slot->col_unique_id();
        }
        ProjectedColumnBuildContext context {
                .scan_params = _scan_params, .runtime_state = _runtime_state, .slot_desc = slot};
        if (_format == FileFormat::WAL) {
            if (slot->col_unique_id() < 0) {
                return Status::InternalError("WAL projected column {} has no valid unique id",
                                             column.name);
            }
            // WAL headers identify columns by stable unique id, including after a rename.
            column.identifier = Field::create_field<TYPE_INT>(slot->col_unique_id());
        } else {
            RETURN_IF_ERROR(annotate_projected_column(*required, &context, &column));
        }
        _source_columns.push_back(std::move(column));
    }
    return Status::OK();
}

Status LoadTableReader::prepare_split(const SplitReadOptions& options) {
    auto source_options = options;
    source_options.conjuncts = VExprContextSPtrs {};
    source_options.partition_prune_conjuncts.clear();
    source_options.condition_cache_digest = 0;
    source_options.condition_cache_split_context.reset();
    source_options.all_runtime_filters_applied = true;
    const auto& range = options.current_range;
    if (_partition_positions.size() > range.columns_from_path.size()) {
        return Status::InvalidArgument("Load range has {} partition values for {} source slots",
                                       range.columns_from_path.size(), _partition_positions.size());
    }
    for (size_t i = 0; i < _partition_positions.size(); ++i) {
        const auto* slot = _source_tuple->slots()[_partition_positions[i]];
        auto column = slot->get_data_type_ptr()->create_column();
        const bool is_null = range.__isset.columns_from_path_is_null &&
                             i < range.columns_from_path_is_null.size() &&
                             range.columns_from_path_is_null[i];
        if (is_null) {
            column->insert_default();
        } else {
            Slice value(range.columns_from_path[i]);
            DataTypeSerDe::FormatOptions serde_options;
            RETURN_IF_ERROR(
                    slot->get_data_type_ptr()->get_serde()->deserialize_one_cell_from_hive_text(
                            *column, value, serde_options));
        }
        Field field;
        column->get(0, field);
        source_options.partition_values.insert_or_assign(slot->col_name(), std::move(field));
    }
    return TableReader::prepare_split(source_options);
}

Status LoadTableReader::refresh_conjuncts(VExprContextSPtrs, std::optional<uint64_t>, bool) {
    return Status::OK();
}

Status LoadTableReader::build_physical_splits(const FileScanSplit&, std::vector<FileScanSplit>*,
                                              bool* was_split) {
    // Keep ingestion records and their error accounting in the original FE source range.
    *was_split = false;
    return Status::OK();
}

Status LoadTableReader::_reject_row(size_t row, const std::string& reason) {
    RETURN_IF_ERROR(_runtime_state->append_error_msg_to_file(
            [&] { return _source.dump_one_line(row, _scan_params->num_of_columns_from_file); },
            [&] { return reason; }));
    ++_io_ctx->load_filtered_rows;
    return Status::OK();
}

Status LoadTableReader::_materialize_missing_values() {
    const size_t rows = _source.rows();
    std::map<size_t, const std::vector<uint8_t>*> missing;
    // Translate presence metadata only through the mapper, while the physical reader is alive.
    const auto& fields = _data_reader.reader->missing_fields();
    for (const auto& mapping : _data_reader.column_mapper->mappings()) {
        if (mapping.file_local_id.has_value()) {
            const auto it = fields.find(LocalColumnId(*mapping.file_local_id));
            if (it != fields.end()) {
                missing.emplace(mapping.global_index.value(), &it->second);
            }
        }
    }
    if (_skip_bitmap_position >= 0) {
        auto bitmap =
                _source_tuple->slots()[_skip_bitmap_position]->get_data_type_ptr()->create_column();
        bitmap->insert_many_defaults(rows);
        auto& nullable = assert_cast<ColumnNullable&>(*bitmap);
        std::fill(nullable.get_null_map_data().begin(), nullable.get_null_map_data().end(), 0);
        auto& values = assert_cast<ColumnBitmap&>(nullable.get_nested_column()).get_data();
        auto keep = ColumnUInt8::create(rows, 1);
        for (const auto& [position, mask] : missing) {
            const auto* slot = _source_tuple->slots()[position];
            if (slot->is_skip_bitmap_col()) {
                continue;
            }
            for (size_t row = 0; row < rows; ++row) {
                if (!(*mask)[row]) {
                    continue;
                }
                if (slot->is_key() && !slot->is_auto_increment()) {
                    if (keep->get_data()[row]) {
                        RETURN_IF_ERROR(_reject_row(
                                row, fmt::format("Missing key column {} in flexible partial update",
                                                 slot->col_name())));
                        keep->get_data()[row] = 0;
                    }
                } else {
                    values[row].add(slot->col_unique_id());
                    if (_sequence_map_uid == slot->col_unique_id() && _sequence_uid >= 0) {
                        values[row].add(_sequence_uid);
                    }
                }
            }
        }
        _source.replace_by_position(_skip_bitmap_position, std::move(bitmap));
        const auto size = _source.columns();
        _source.insert({std::move(keep), std::make_shared<DataTypeUInt8>(), "load_valid"});
        RETURN_IF_ERROR(Block::filter_block(&_source, size, size));
    } else {
        for (const auto& [position, mask] : missing) {
            const auto expr = _source_defaults.find(position);
            if (expr == _source_defaults.end()) {
                continue;
            }
            ColumnPtr default_values;
            RETURN_IF_ERROR(expr->second->execute(&_source, default_values));
            default_values = default_values->convert_to_full_column_if_const();
            auto& input = _source.get_by_position(position);
            default_values = input.type->is_nullable() ? make_nullable(default_values)
                                                       : remove_nullable(default_values);
            auto result = input.type->create_column();
            for (size_t row = 0; row < rows; ++row) {
                result->insert_from((*mask)[row] ? *default_values : *input.column, row);
            }
            _source.replace_by_position(position, std::move(result));
        }
    }
    return Status::OK();
}

Status LoadTableReader::_convert(Block* output) {
    const size_t rows = _source.rows();
    auto keep = ColumnUInt8::create(rows, 1);
    const std::vector<BitmapValue>* skip = nullptr;
    if (_skip_bitmap_position >= 0) {
        const auto& nullable = assert_cast<const ColumnNullable&>(
                *_source.get_by_position(_skip_bitmap_position).column);
        skip = &assert_cast<const ColumnBitmap&>(nullable.get_nested_column()).get_data();
    }
    for (size_t i = 0; i < _destination_exprs.size(); ++i) {
        const auto* slot = _destination_tuple->slots()[i];
        ColumnPtr column;
        RETURN_IF_ERROR(_destination_exprs[i]->execute(&_source, column));
        column = column->convert_to_full_column_if_const();
        for (size_t row = 0; row < rows; ++row) {
            if (!keep->get_data()[row] || !column->is_null_at(row) ||
                (skip != nullptr && (*skip)[row].contains(slot->col_unique_id()))) {
                continue;
            }
            const int source_position = _direct_source_positions[i];
            const bool invalid_cast =
                    _scan_params->strict_mode && source_position >= 0 &&
                    !_source.get_by_position(source_position).column->is_null_at(row);
            if (invalid_cast || !slot->is_nullable()) {
                RETURN_IF_ERROR(_reject_row(
                        row, fmt::format("Column {} {}", slot->col_name(),
                                         invalid_cast ? "cannot be converted in strict mode"
                                                      : "is NULL but is not nullable")));
                keep->get_data()[row] = 0;
            }
        }
        column = slot->is_nullable() ? make_nullable(column) : remove_nullable(column);
        output->replace_by_position(i, std::move(column));
    }
    const auto size = output->columns();
    output->insert({std::move(keep), std::make_shared<DataTypeUInt8>(), "load_valid"});
    RETURN_IF_ERROR(Block::filter_block(output, size, size));
    return Status::OK();
}

Status LoadTableReader::validate_file_mapping(const TableColumnMapper& mapper) const {
    RETURN_IF_ERROR(TableReader::validate_file_mapping(mapper));
    for (const auto& mapping : mapper.mappings()) {
        const auto position = mapping.global_index.value();
        if (mapping.file_local_id.has_value() || _projected_columns[position].is_partition_key) {
            continue;
        }
        const auto* slot = _source_tuple->slots()[position];
        // FE uses an empty expression for an explicit NULL default. An absent map entry instead
        // means the file source alias has no fallback and must not silently materialize as NULL.
        if (!_scan_params->default_value_of_src_slot.contains(slot->id())) {
            return Status::InternalError("failed to find default value expr for slot: {}",
                                         slot->col_name());
        }
    }
    return Status::OK();
}

Status LoadTableReader::finalize_chunk(Block* block, size_t physical_rows) {
    _source = Block(_source_tuple->slots(), 0);
    RETURN_IF_ERROR(TableReader::finalize_chunk(&_source, physical_rows));
    RETURN_IF_ERROR(_materialize_missing_values());
    const auto rows = _source.rows();
    RETURN_IF_ERROR(
            VExprContext::filter_block(_pre_filters, &_source, _source_tuple->slots().size()));
    _io_ctx->load_unselected_rows += cast_set<int64_t>(rows - _source.rows());
    if (_source.rows() > 0) {
        RETURN_IF_ERROR(_convert(block));
    }
    _source.clear();
    return Status::OK();
}

Status LoadTableReader::abort_split() {
    _source.clear();
    return TableReader::abort_split();
}

Status LoadTableReader::customize_file_scan_request(FileScanRequest* request) {
    // Decoders must reject malformed load records before source expressions run.
    request->reject_invalid_rows = true;
    return Status::OK();
}

TableColumnMappingMode LoadTableReader::mapping_mode() const {
    return _format == FileFormat::WAL ? TableColumnMappingMode::BY_FIELD_ID
                                      : TableColumnMappingMode::BY_NAME;
}

Status LoadTableReader::create_file_reader(std::unique_ptr<FileReader>* reader) {
    if (_format == FileFormat::ARROW) {
        *reader = std::make_unique<arrow_stream::ArrowStreamReader>(
                _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
                _source_file_slots, _current_range_load_id);
        return Status::OK();
    }
    if (_format == FileFormat::WAL) {
        *reader = std::make_unique<wal::WalReader>(_system_properties, _current_task->data_file,
                                                   _io_ctx, _scanner_profile, _projected_columns);
        return Status::OK();
    }
    return TableReader::create_file_reader(reader);
}

Status LoadTableReader::close() {
    if (_closed) {
        return Status::OK();
    }
    RETURN_IF_ERROR(TableReader::close());
    _pre_filters.clear();
    _destination_exprs.clear();
    _source_defaults.clear();
    _source.clear();
    _closed = true;
    return Status::OK();
}

} // namespace doris::format
