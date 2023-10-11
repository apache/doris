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

#include "vparquet_reader.h"

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <functional>
#include <ostream>
#include <utility>

#include "common/status.h"
#include "io/file_factory.h"
#include "parquet_pred_cmp.h"
#include "parquet_thrift_util.h"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "util/slice.h"
#include "vec/common/typeid_cast.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/parquet/schema_desc.h"
#include "vec/exec/format/parquet/vparquet_file_metadata.h"
#include "vec/exec/format/parquet/vparquet_group_reader.h"
#include "vec/exec/format/parquet/vparquet_page_index.h"
#include "vec/exprs/vbloom_predicate.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/exprs/vslot_ref.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
class RowDescriptor;
class RuntimeState;
class SlotDescriptor;
class TupleDescriptor;
namespace io {
struct IOContext;
enum class FileCachePolicy : uint8_t;
} // namespace io
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

ParquetReader::ParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                             const TFileRangeDesc& range, size_t batch_size, cctz::time_zone* ctz,
                             io::IOContext* io_ctx, RuntimeState* state, FileMetaCache* meta_cache,
                             bool enable_lazy_mat)
        : _profile(profile),
          _scan_params(params),
          _scan_range(range),
          _batch_size(std::max(batch_size, _MIN_BATCH_SIZE)),
          _range_start_offset(range.start_offset),
          _range_size(range.size),
          _ctz(ctz),
          _io_ctx(io_ctx),
          _state(state),
          _meta_cache(meta_cache),
          _enable_lazy_mat(enable_lazy_mat) {
    _init_profile();
    _init_system_properties();
    _init_file_description();
}

ParquetReader::ParquetReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                             io::IOContext* io_ctx, RuntimeState* state, bool enable_lazy_mat)
        : _profile(nullptr),
          _scan_params(params),
          _scan_range(range),
          _io_ctx(io_ctx),
          _state(state),
          _enable_lazy_mat(enable_lazy_mat) {
    _init_system_properties();
    _init_file_description();
}

ParquetReader::~ParquetReader() {
    _close_internal();
}

void ParquetReader::_init_profile() {
    if (_profile != nullptr) {
        static const char* parquet_profile = "ParquetReader";
        ADD_TIMER(_profile, parquet_profile);

        _parquet_profile.filtered_row_groups =
                ADD_CHILD_COUNTER(_profile, "FilteredGroups", TUnit::UNIT, parquet_profile);
        _parquet_profile.to_read_row_groups =
                ADD_CHILD_COUNTER(_profile, "ReadGroups", TUnit::UNIT, parquet_profile);
        _parquet_profile.filtered_group_rows =
                ADD_CHILD_COUNTER(_profile, "FilteredRowsByGroup", TUnit::UNIT, parquet_profile);
        _parquet_profile.filtered_page_rows =
                ADD_CHILD_COUNTER(_profile, "FilteredRowsByPage", TUnit::UNIT, parquet_profile);
        _parquet_profile.lazy_read_filtered_rows =
                ADD_CHILD_COUNTER(_profile, "FilteredRowsByLazyRead", TUnit::UNIT, parquet_profile);
        _parquet_profile.filtered_bytes =
                ADD_CHILD_COUNTER(_profile, "FilteredBytes", TUnit::BYTES, parquet_profile);
        _parquet_profile.raw_rows_read =
                ADD_CHILD_COUNTER(_profile, "RawRowsRead", TUnit::UNIT, parquet_profile);
        _parquet_profile.to_read_bytes =
                ADD_CHILD_COUNTER(_profile, "ReadBytes", TUnit::BYTES, parquet_profile);
        _parquet_profile.column_read_time =
                ADD_CHILD_TIMER(_profile, "ColumnReadTime", parquet_profile);
        _parquet_profile.parse_meta_time =
                ADD_CHILD_TIMER(_profile, "ParseMetaTime", parquet_profile);
        _parquet_profile.parse_footer_time =
                ADD_CHILD_TIMER(_profile, "ParseFooterTime", parquet_profile);
        _parquet_profile.open_file_time =
                ADD_CHILD_TIMER(_profile, "FileOpenTime", parquet_profile);
        _parquet_profile.open_file_num =
                ADD_CHILD_COUNTER(_profile, "FileNum", TUnit::UNIT, parquet_profile);
        _parquet_profile.page_index_filter_time =
                ADD_CHILD_TIMER(_profile, "PageIndexFilterTime", parquet_profile);
        _parquet_profile.row_group_filter_time =
                ADD_CHILD_TIMER(_profile, "RowGroupFilterTime", parquet_profile);

        _parquet_profile.file_read_time = ADD_TIMER(_profile, "FileReadTime");
        _parquet_profile.file_read_calls = ADD_COUNTER(_profile, "FileReadCalls", TUnit::UNIT);
        _parquet_profile.file_meta_read_calls =
                ADD_COUNTER(_profile, "FileMetaReadCalls", TUnit::UNIT);
        _parquet_profile.file_read_bytes = ADD_COUNTER(_profile, "FileReadBytes", TUnit::BYTES);
        _parquet_profile.decompress_time =
                ADD_CHILD_TIMER(_profile, "DecompressTime", parquet_profile);
        _parquet_profile.decompress_cnt =
                ADD_CHILD_COUNTER(_profile, "DecompressCount", TUnit::UNIT, parquet_profile);
        _parquet_profile.decode_header_time =
                ADD_CHILD_TIMER(_profile, "DecodeHeaderTime", parquet_profile);
        _parquet_profile.decode_value_time =
                ADD_CHILD_TIMER(_profile, "DecodeValueTime", parquet_profile);
        _parquet_profile.decode_dict_time =
                ADD_CHILD_TIMER(_profile, "DecodeDictTime", parquet_profile);
        _parquet_profile.decode_level_time =
                ADD_CHILD_TIMER(_profile, "DecodeLevelTime", parquet_profile);
        _parquet_profile.decode_null_map_time =
                ADD_CHILD_TIMER(_profile, "DecodeNullMapTime", parquet_profile);
    }
}

void ParquetReader::close() {
    _close_internal();
}

void ParquetReader::_close_internal() {
    if (!_closed) {
        if (_profile != nullptr) {
            COUNTER_UPDATE(_parquet_profile.filtered_row_groups, _statistics.filtered_row_groups);
            COUNTER_UPDATE(_parquet_profile.to_read_row_groups, _statistics.read_row_groups);
            COUNTER_UPDATE(_parquet_profile.filtered_group_rows, _statistics.filtered_group_rows);
            COUNTER_UPDATE(_parquet_profile.filtered_page_rows, _statistics.filtered_page_rows);
            COUNTER_UPDATE(_parquet_profile.lazy_read_filtered_rows,
                           _statistics.lazy_read_filtered_rows);
            COUNTER_UPDATE(_parquet_profile.filtered_bytes, _statistics.filtered_bytes);
            COUNTER_UPDATE(_parquet_profile.raw_rows_read, _statistics.read_rows);
            COUNTER_UPDATE(_parquet_profile.to_read_bytes, _statistics.read_bytes);
            COUNTER_UPDATE(_parquet_profile.column_read_time, _statistics.column_read_time);
            COUNTER_UPDATE(_parquet_profile.parse_meta_time, _statistics.parse_meta_time);
            COUNTER_UPDATE(_parquet_profile.parse_footer_time, _statistics.parse_footer_time);
            COUNTER_UPDATE(_parquet_profile.open_file_time, _statistics.open_file_time);
            COUNTER_UPDATE(_parquet_profile.open_file_num, _statistics.open_file_num);
            COUNTER_UPDATE(_parquet_profile.page_index_filter_time,
                           _statistics.page_index_filter_time);
            COUNTER_UPDATE(_parquet_profile.row_group_filter_time,
                           _statistics.row_group_filter_time);

            COUNTER_UPDATE(_parquet_profile.file_read_time, _column_statistics.read_time);
            COUNTER_UPDATE(_parquet_profile.file_read_calls, _column_statistics.read_calls);
            COUNTER_UPDATE(_parquet_profile.file_meta_read_calls,
                           _column_statistics.meta_read_calls);
            COUNTER_UPDATE(_parquet_profile.file_read_bytes, _column_statistics.read_bytes);
            COUNTER_UPDATE(_parquet_profile.decompress_time, _column_statistics.decompress_time);
            COUNTER_UPDATE(_parquet_profile.decompress_cnt, _column_statistics.decompress_cnt);
            COUNTER_UPDATE(_parquet_profile.decode_header_time,
                           _column_statistics.decode_header_time);
            COUNTER_UPDATE(_parquet_profile.decode_value_time,
                           _column_statistics.decode_value_time);
            COUNTER_UPDATE(_parquet_profile.decode_dict_time, _column_statistics.decode_dict_time);
            COUNTER_UPDATE(_parquet_profile.decode_level_time,
                           _column_statistics.decode_level_time);
            COUNTER_UPDATE(_parquet_profile.decode_null_map_time,
                           _column_statistics.decode_null_map_time);
        }
        _closed = true;
    }
}

Status ParquetReader::_open_file() {
    if (_file_reader == nullptr) {
        SCOPED_RAW_TIMER(&_statistics.open_file_time);
        ++_statistics.open_file_num;
        _file_description.mtime =
                _scan_range.__isset.modification_time ? _scan_range.modification_time : 0;
        io::FileReaderOptions reader_options =
                FileFactory::get_reader_options(_state, _file_description);
        RETURN_IF_ERROR(io::DelegateReader::create_file_reader(
                _profile, _system_properties, _file_description, reader_options, &_file_system,
                &_file_reader, io::DelegateReader::AccessMode::RANDOM, _io_ctx));
    }
    if (_file_metadata == nullptr) {
        SCOPED_RAW_TIMER(&_statistics.parse_footer_time);
        if (_file_reader->size() == 0) {
            return Status::EndOfFile("open file failed, empty parquet file: " + _scan_range.path);
        }
        size_t meta_size = 0;
        if (_meta_cache == nullptr) {
            RETURN_IF_ERROR(
                    parse_thrift_footer(_file_reader, &_file_metadata, &meta_size, _io_ctx));
            // wrap it with unique ptr, so that it can be released finally.
            _file_metadata_ptr.reset(_file_metadata);
            _file_metadata = _file_metadata_ptr.get();

            _column_statistics.read_bytes += meta_size;
            // parse magic number & parse meta data
            _column_statistics.meta_read_calls += 1;
        } else {
            RETURN_IF_ERROR(_meta_cache->get_parquet_footer(_file_reader, _io_ctx,
                                                            _file_description.mtime, &meta_size,
                                                            &_meta_cache_handle));
            _column_statistics.read_bytes += meta_size;
            if (meta_size > 0) {
                _column_statistics.meta_read_calls += 1;
            }

            _file_metadata = (FileMetaData*)_meta_cache_handle.data();
        }

        if (_file_metadata == nullptr) {
            return Status::InternalError("failed to get file meta data: {}",
                                         _file_description.path);
        }
        _column_statistics.read_bytes += meta_size;
        // parse magic number & parse meta data
        _column_statistics.read_calls += 1;
    }
    return Status::OK();
}

// Get iceberg col id to col name map stored in parquet metadata key values.
// This is for iceberg schema evolution.
std::vector<tparquet::KeyValue> ParquetReader::get_metadata_key_values() {
    return _t_metadata->key_value_metadata;
}

Status ParquetReader::open() {
    RETURN_IF_ERROR(_open_file());
    _t_metadata = &(_file_metadata->to_thrift());
    return Status::OK();
}

void ParquetReader::_init_system_properties() {
    if (_scan_range.__isset.file_type) {
        // for compatibility
        _system_properties.system_type = _scan_range.file_type;
    } else {
        _system_properties.system_type = _scan_params.file_type;
    }
    _system_properties.properties = _scan_params.properties;
    _system_properties.hdfs_params = _scan_params.hdfs_params;
    if (_scan_params.__isset.broker_addresses) {
        _system_properties.broker_addresses.assign(_scan_params.broker_addresses.begin(),
                                                   _scan_params.broker_addresses.end());
    }
}

void ParquetReader::_init_file_description() {
    _file_description.path = _scan_range.path;
    _file_description.file_size = _scan_range.__isset.file_size ? _scan_range.file_size : -1;
    if (_scan_range.__isset.fs_name) {
        _file_description.fs_name = _scan_range.fs_name;
    }
}

Status ParquetReader::init_reader(
        const std::vector<std::string>& all_column_names,
        const std::vector<std::string>& missing_column_names,
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
        const VExprContextSPtrs& conjuncts, const TupleDescriptor* tuple_descriptor,
        const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts,
        bool filter_groups) {
    _tuple_descriptor = tuple_descriptor;
    _row_descriptor = row_descriptor;
    _colname_to_slot_id = colname_to_slot_id;
    _not_single_slot_filter_conjuncts = not_single_slot_filter_conjuncts;
    _slot_id_to_filter_conjuncts = slot_id_to_filter_conjuncts;
    if (_file_metadata == nullptr) {
        return Status::InternalError("failed to init parquet reader, please open reader first");
    }

    SCOPED_RAW_TIMER(&_statistics.parse_meta_time);
    _total_groups = _t_metadata->row_groups.size();
    if (_total_groups == 0) {
        return Status::EndOfFile("init reader failed, empty parquet file: " + _scan_range.path);
    }
    // all_column_names are all the columns required by user sql.
    // missing_column_names are the columns required by user sql but not in the parquet file,
    // e.g. table added a column after this parquet file was written.
    _column_names = &all_column_names;
    auto schema_desc = _file_metadata->schema();
    std::set<std::string> required_columns(all_column_names.begin(), all_column_names.end());
    // Currently only used in iceberg, the columns are dropped but added back
    std::set<std::string> dropped_columns(missing_column_names.begin(), missing_column_names.end());
    // Make the order of read columns the same as physical order in parquet file
    for (int i = 0; i < schema_desc.size(); ++i) {
        auto name = schema_desc.get_column(i)->name;
        // If the column in parquet file is included in all_column_names and not in missing_column_names,
        // add it to _map_column, which means the reader should read the data of this column.
        // Here to check against missing_column_names is for the 'Add a column back to the table
        // with the same column name' case. (drop column a then add column a).
        // Shouldn't read this column data in this case.
        if (required_columns.find(name) != required_columns.end() &&
            dropped_columns.find(name) == dropped_columns.end()) {
            required_columns.erase(name);
            _read_columns.emplace_back(name);
        }
    }
    for (const std::string& name : required_columns) {
        _missing_cols.emplace_back(name);
    }

    _colname_to_value_range = colname_to_value_range;
    // build column predicates for column lazy read
    _lazy_read_ctx.conjuncts = conjuncts;
    RETURN_IF_ERROR(_init_row_groups(filter_groups));
    return Status::OK();
}

Status ParquetReader::set_fill_columns(
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns,
        const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) {
    SCOPED_RAW_TIMER(&_statistics.parse_meta_time);
    // std::unordered_map<column_name, std::pair<col_id, slot_id>>
    std::unordered_map<std::string, std::pair<uint32_t, int>> predicate_columns;
    std::function<void(VExpr * expr)> visit_slot = [&](VExpr* expr) {
        if (VSlotRef* slot_ref = typeid_cast<VSlotRef*>(expr)) {
            auto expr_name = slot_ref->expr_name();
            auto iter = _table_col_to_file_col.find(expr_name);
            if (iter != _table_col_to_file_col.end()) {
                expr_name = iter->second;
            }
            predicate_columns.emplace(expr_name,
                                      std::make_pair(slot_ref->column_id(), slot_ref->slot_id()));
            if (slot_ref->column_id() == 0) {
                _lazy_read_ctx.resize_first_column = false;
            }
            return;
        } else if (VRuntimeFilterWrapper* runtime_filter =
                           typeid_cast<VRuntimeFilterWrapper*>(expr)) {
            VExpr* filter_impl = const_cast<VExpr*>(runtime_filter->get_impl().get());
            if (VBloomPredicate* bloom_predicate = typeid_cast<VBloomPredicate*>(filter_impl)) {
                for (auto& child : bloom_predicate->children()) {
                    visit_slot(child.get());
                }
            } else if (VInPredicate* in_predicate = typeid_cast<VInPredicate*>(filter_impl)) {
                if (in_predicate->children().size() > 0) {
                    visit_slot(in_predicate->children()[0].get());
                }
            } else {
                for (auto& child : filter_impl->children()) {
                    visit_slot(child.get());
                }
            }
        } else {
            for (auto& child : expr->children()) {
                visit_slot(child.get());
            }
        }
    };
    if (!_lazy_read_ctx.conjuncts.empty()) {
        for (auto& conjunct : _lazy_read_ctx.conjuncts) {
            visit_slot(conjunct->root().get());
        }
    }

    const FieldDescriptor& schema = _file_metadata->schema();
    for (auto& read_col : _read_columns) {
        _lazy_read_ctx.all_read_columns.emplace_back(read_col);
        PrimitiveType column_type = schema.get_column(read_col)->type.type;
        if (column_type == TYPE_ARRAY || column_type == TYPE_MAP || column_type == TYPE_STRUCT) {
            _has_complex_type = true;
        }
        if (predicate_columns.size() > 0) {
            auto iter = predicate_columns.find(read_col);
            if (iter == predicate_columns.end()) {
                _lazy_read_ctx.lazy_read_columns.emplace_back(read_col);
            } else {
                _lazy_read_ctx.predicate_columns.first.emplace_back(iter->first);
                _lazy_read_ctx.predicate_columns.second.emplace_back(iter->second.second);
                _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
            }
        }
    }

    for (auto& kv : partition_columns) {
        auto iter = predicate_columns.find(kv.first);
        if (iter == predicate_columns.end()) {
            _lazy_read_ctx.partition_columns.emplace(kv.first, kv.second);
        } else {
            _lazy_read_ctx.predicate_partition_columns.emplace(kv.first, kv.second);
            _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
        }
    }

    for (auto& kv : missing_columns) {
        auto iter = predicate_columns.find(kv.first);
        if (iter == predicate_columns.end()) {
            _lazy_read_ctx.missing_columns.emplace(kv.first, kv.second);
        } else {
            _lazy_read_ctx.predicate_missing_columns.emplace(kv.first, kv.second);
            _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
        }
    }

    if (!_has_complex_type && _enable_lazy_mat &&
        _lazy_read_ctx.predicate_columns.first.size() > 0 &&
        _lazy_read_ctx.lazy_read_columns.size() > 0) {
        _lazy_read_ctx.can_lazy_read = true;
    }

    if (!_lazy_read_ctx.can_lazy_read) {
        for (auto& kv : _lazy_read_ctx.predicate_partition_columns) {
            _lazy_read_ctx.partition_columns.emplace(kv.first, kv.second);
        }
        for (auto& kv : _lazy_read_ctx.predicate_missing_columns) {
            _lazy_read_ctx.missing_columns.emplace(kv.first, kv.second);
        }
    }

    _fill_all_columns = true;
    return Status::OK();
}

std::unordered_map<std::string, TypeDescriptor> ParquetReader::get_name_to_type() {
    std::unordered_map<std::string, TypeDescriptor> map;
    const auto& schema_desc = _file_metadata->schema();
    std::unordered_set<std::string> column_names;
    schema_desc.get_column_names(&column_names);
    for (auto& name : column_names) {
        auto field = schema_desc.get_column(name);
        map.emplace(name, field->type);
    }
    return map;
}

Status ParquetReader::get_parsed_schema(std::vector<std::string>* col_names,
                                        std::vector<TypeDescriptor>* col_types) {
    RETURN_IF_ERROR(_open_file());
    _t_metadata = &_file_metadata->to_thrift();

    _total_groups = _t_metadata->row_groups.size();
    auto schema_desc = _file_metadata->schema();
    for (int i = 0; i < schema_desc.size(); ++i) {
        // Get the Column Reader for the boolean column
        col_names->emplace_back(schema_desc.get_column(i)->name);
        col_types->emplace_back(schema_desc.get_column(i)->type);
    }
    return Status::OK();
}

Status ParquetReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    const auto& schema_desc = _file_metadata->schema();
    std::unordered_set<std::string> column_names;
    schema_desc.get_column_names(&column_names);
    for (auto& name : column_names) {
        auto field = schema_desc.get_column(name);
        name_to_type->emplace(name, field->type);
    }
    for (auto& col : _missing_cols) {
        missing_cols->insert(col);
    }
    return Status::OK();
}

Status ParquetReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_current_group_reader == nullptr || _row_group_eof) {
        if (_read_row_groups.size() > 0) {
            RETURN_IF_ERROR(_next_row_group_reader());
        } else {
            _current_group_reader.reset(nullptr);
            _row_group_eof = true;
            *read_rows = 0;
            *eof = true;
            return Status::OK();
        }
    }
    DCHECK(_current_group_reader != nullptr);
    if (_push_down_agg_type == TPushAggOp::type::COUNT) {
        auto rows = std::min(_current_group_reader->get_remaining_rows(), (int64_t)_batch_size);

        _current_group_reader->set_remaining_rows(_current_group_reader->get_remaining_rows() -
                                                  rows);

        for (auto& col : block->mutate_columns()) {
            col->resize(rows);
        }

        *read_rows = rows;
        if (_current_group_reader->get_remaining_rows() == 0) {
            _current_group_reader.reset(nullptr);
        }

        return Status::OK();
    }

    {
        SCOPED_RAW_TIMER(&_statistics.column_read_time);
        Status batch_st =
                _current_group_reader->next_batch(block, _batch_size, read_rows, &_row_group_eof);
        if (!batch_st.ok()) {
            return Status::InternalError("Read parquet file {} failed, reason = {}",
                                         _scan_range.path, batch_st.to_string());
        }
    }
    if (_row_group_eof) {
        auto column_st = _current_group_reader->statistics();
        _column_statistics.merge(column_st);
        _statistics.lazy_read_filtered_rows += _current_group_reader->lazy_read_filtered_rows();
        if (_read_row_groups.size() == 0) {
            *eof = true;
        } else {
            *eof = false;
        }
    }
    return Status::OK();
}

RowGroupReader::PositionDeleteContext ParquetReader::_get_position_delete_ctx(
        const tparquet::RowGroup& row_group, const RowGroupReader::RowGroupIndex& row_group_index) {
    if (_delete_rows == nullptr) {
        return RowGroupReader::PositionDeleteContext(row_group.num_rows, row_group_index.first_row);
    }
    int64_t* delete_rows = const_cast<int64_t*>(&(*_delete_rows)[0]);
    int64_t* delete_rows_end = delete_rows + _delete_rows->size();
    int64_t* start_pos = std::lower_bound(delete_rows + _delete_rows_index, delete_rows_end,
                                          row_group_index.first_row);
    int64_t start_index = start_pos - delete_rows;
    int64_t* end_pos = std::lower_bound(start_pos, delete_rows_end, row_group_index.last_row);
    int64_t end_index = end_pos - delete_rows;
    _delete_rows_index = end_index;
    return RowGroupReader::PositionDeleteContext(*_delete_rows, row_group.num_rows,
                                                 row_group_index.first_row, start_index, end_index);
}

Status ParquetReader::_next_row_group_reader() {
    if (_read_row_groups.empty()) {
        _row_group_eof = true;
        _current_group_reader.reset(nullptr);
        return Status::EndOfFile("No next RowGroupReader");
    }
    RowGroupReader::RowGroupIndex row_group_index = _read_row_groups.front();
    _read_row_groups.pop_front();

    // process page index and generate the ranges to read
    auto& row_group = _t_metadata->row_groups[row_group_index.row_group_id];
    std::vector<RowRange> candidate_row_ranges;
    RETURN_IF_ERROR(_process_page_index(row_group, candidate_row_ranges));

    RowGroupReader::PositionDeleteContext position_delete_ctx =
            _get_position_delete_ctx(row_group, row_group_index);
    io::FileReaderSPtr group_file_reader;
    if (typeid_cast<io::InMemoryFileReader*>(_file_reader.get())) {
        // InMemoryFileReader has the ability to merge small IO
        group_file_reader = _file_reader;
    } else {
        size_t avg_io_size = 0;
        const std::vector<io::PrefetchRange> io_ranges =
                _generate_random_access_ranges(row_group_index, &avg_io_size);
        // The underlying page reader will prefetch data in column.
        // Using both MergeRangeFileReader and BufferedStreamReader simultaneously would waste a lot of memory.
        group_file_reader = avg_io_size < io::MergeRangeFileReader::SMALL_IO
                                    ? std::make_shared<io::MergeRangeFileReader>(
                                              _profile, _file_reader, io_ranges)
                                    : _file_reader;
    }
    _current_group_reader.reset(new RowGroupReader(
            group_file_reader, _read_columns, row_group_index.row_group_id, row_group, _ctz,
            _io_ctx, position_delete_ctx, _lazy_read_ctx, _state));
    _row_group_eof = false;
    return _current_group_reader->init(_file_metadata->schema(), candidate_row_ranges, _col_offsets,
                                       _tuple_descriptor, _row_descriptor, _colname_to_slot_id,
                                       _not_single_slot_filter_conjuncts,
                                       _slot_id_to_filter_conjuncts);
}

Status ParquetReader::_init_row_groups(const bool& is_filter_groups) {
    SCOPED_RAW_TIMER(&_statistics.row_group_filter_time);
    if (is_filter_groups && (_total_groups == 0 || _t_metadata->num_rows == 0 || _range_size < 0)) {
        return Status::EndOfFile("No row group to read");
    }
    int64_t row_index = 0;
    for (int32_t row_group_idx = 0; row_group_idx < _total_groups; row_group_idx++) {
        const tparquet::RowGroup& row_group = _t_metadata->row_groups[row_group_idx];
        if (is_filter_groups && _is_misaligned_range_group(row_group)) {
            row_index += row_group.num_rows;
            continue;
        }
        bool filter_group = false;
        if (is_filter_groups) {
            RETURN_IF_ERROR(_process_row_group_filter(row_group, &filter_group));
        }
        int64_t group_size = 0; // only calculate the needed columns
        std::function<int64_t(const FieldSchema*)> column_compressed_size =
                [&row_group, &column_compressed_size](const FieldSchema* field) -> int64_t {
            if (field->physical_column_index >= 0) {
                int parquet_col_id = field->physical_column_index;
                if (row_group.columns[parquet_col_id].__isset.meta_data) {
                    return row_group.columns[parquet_col_id].meta_data.total_compressed_size;
                }
                return 0;
            }
            int64_t size = 0;
            for (const FieldSchema& child : field->children) {
                size += column_compressed_size(&child);
            }
            return size;
        };
        for (auto& read_col : _read_columns) {
            const FieldSchema* field = _file_metadata->schema().get_column(read_col);
            group_size += column_compressed_size(field);
        }
        if (!filter_group) {
            _read_row_groups.emplace_back(row_group_idx, row_index, row_index + row_group.num_rows);
            if (_statistics.read_row_groups == 0) {
                _whole_range.first_row = row_index;
            }
            _whole_range.last_row = row_index + row_group.num_rows;
            _statistics.read_row_groups++;
            _statistics.read_bytes += group_size;
        } else {
            _statistics.filtered_row_groups++;
            _statistics.filtered_bytes += group_size;
            _statistics.filtered_group_rows += row_group.num_rows;
        }
        row_index += row_group.num_rows;
    }

    if (_read_row_groups.empty()) {
        return Status::EndOfFile("No row group to read");
    }
    return Status::OK();
}

std::vector<io::PrefetchRange> ParquetReader::_generate_random_access_ranges(
        const RowGroupReader::RowGroupIndex& group, size_t* avg_io_size) {
    std::vector<io::PrefetchRange> result;
    int64_t last_chunk_end = -1;
    size_t total_io_size = 0;
    std::function<void(const FieldSchema*, const tparquet::RowGroup&)> scalar_range =
            [&](const FieldSchema* field, const tparquet::RowGroup& row_group) {
                if (field->type.type == TYPE_ARRAY) {
                    scalar_range(&field->children[0], row_group);
                } else if (field->type.type == TYPE_MAP) {
                    scalar_range(&field->children[0].children[0], row_group);
                    scalar_range(&field->children[0].children[1], row_group);
                } else if (field->type.type == TYPE_STRUCT) {
                    for (int i = 0; i < field->children.size(); ++i) {
                        scalar_range(&field->children[i], row_group);
                    }
                } else {
                    const tparquet::ColumnChunk& chunk =
                            row_group.columns[field->physical_column_index];
                    auto& chunk_meta = chunk.meta_data;
                    int64_t chunk_start = chunk_meta.__isset.dictionary_page_offset
                                                  ? chunk_meta.dictionary_page_offset
                                                  : chunk_meta.data_page_offset;
                    int64_t chunk_end = chunk_start + chunk_meta.total_compressed_size;
                    DCHECK_GE(chunk_start, last_chunk_end);
                    result.emplace_back(chunk_start, chunk_end);
                    total_io_size += chunk_meta.total_compressed_size;
                    last_chunk_end = chunk_end;
                }
            };
    const tparquet::RowGroup& row_group = _t_metadata->row_groups[group.row_group_id];
    for (const auto& read_col : _read_columns) {
        const FieldSchema* field = _file_metadata->schema().get_column(read_col);
        scalar_range(field, row_group);
    }
    if (!result.empty()) {
        *avg_io_size = total_io_size / result.size();
    }
    return result;
}

bool ParquetReader::_is_misaligned_range_group(const tparquet::RowGroup& row_group) {
    int64_t start_offset = _get_column_start_offset(row_group.columns[0].meta_data);

    auto& last_column = row_group.columns[row_group.columns.size() - 1].meta_data;
    int64_t end_offset = _get_column_start_offset(last_column) + last_column.total_compressed_size;

    int64_t row_group_mid = start_offset + (end_offset - start_offset) / 2;
    if (!(row_group_mid >= _range_start_offset &&
          row_group_mid < _range_start_offset + _range_size)) {
        return true;
    }
    return false;
}

bool ParquetReader::_has_page_index(const std::vector<tparquet::ColumnChunk>& columns,
                                    PageIndex& page_index) {
    return page_index.check_and_get_page_index_ranges(columns);
}

Status ParquetReader::_process_page_index(const tparquet::RowGroup& row_group,
                                          std::vector<RowRange>& candidate_row_ranges) {
    SCOPED_RAW_TIMER(&_statistics.page_index_filter_time);

    std::function<void()> read_whole_row_group = [&]() {
        candidate_row_ranges.emplace_back(0, row_group.num_rows);
        _statistics.read_rows += row_group.num_rows;
    };

    if (_has_complex_type || _lazy_read_ctx.conjuncts.empty() ||
        _colname_to_value_range == nullptr || _colname_to_value_range->empty()) {
        read_whole_row_group();
        return Status::OK();
    }
    PageIndex page_index;
    if (!_has_page_index(row_group.columns, page_index)) {
        read_whole_row_group();
        return Status::OK();
    }
    uint8_t col_index_buff[page_index._column_index_size];
    size_t bytes_read = 0;
    Slice result(col_index_buff, page_index._column_index_size);
    RETURN_IF_ERROR(
            _file_reader->read_at(page_index._column_index_start, result, &bytes_read, _io_ctx));
    _column_statistics.read_bytes += bytes_read;
    auto& schema_desc = _file_metadata->schema();
    std::vector<RowRange> skipped_row_ranges;
    uint8_t off_index_buff[page_index._offset_index_size];
    Slice res(off_index_buff, page_index._offset_index_size);
    RETURN_IF_ERROR(
            _file_reader->read_at(page_index._offset_index_start, res, &bytes_read, _io_ctx));
    _column_statistics.read_bytes += bytes_read;
    // read twice: parse column index & parse offset index
    _column_statistics.meta_read_calls += 2;
    for (auto& read_col : _read_columns) {
        auto conjunct_iter = _colname_to_value_range->find(read_col);
        if (_colname_to_value_range->end() == conjunct_iter) {
            continue;
        }
        int parquet_col_id = _file_metadata->schema().get_column(read_col)->physical_column_index;
        if (parquet_col_id < 0) {
            // complex type, not support page index yet.
            continue;
        }
        auto& chunk = row_group.columns[parquet_col_id];
        if (chunk.column_index_offset == 0 && chunk.column_index_length == 0) {
            continue;
        }
        tparquet::ColumnIndex column_index;
        RETURN_IF_ERROR(page_index.parse_column_index(chunk, col_index_buff, &column_index));
        const int num_of_pages = column_index.null_pages.size();
        if (num_of_pages <= 0) {
            continue;
        }
        auto& conjuncts = conjunct_iter->second;
        std::vector<int> skipped_page_range;
        const FieldSchema* col_schema = schema_desc.get_column(read_col);
        static_cast<void>(page_index.collect_skipped_page_range(
                &column_index, conjuncts, col_schema, skipped_page_range, *_ctz));
        if (skipped_page_range.empty()) {
            continue;
        }
        tparquet::OffsetIndex offset_index;
        RETURN_IF_ERROR(page_index.parse_offset_index(chunk, off_index_buff, &offset_index));
        for (int page_id : skipped_page_range) {
            RowRange skipped_row_range;
            static_cast<void>(page_index.create_skipped_row_range(offset_index, row_group.num_rows,
                                                                  page_id, &skipped_row_range));
            // use the union row range
            skipped_row_ranges.emplace_back(skipped_row_range);
        }
        _col_offsets.emplace(parquet_col_id, offset_index);
    }
    if (skipped_row_ranges.empty()) {
        read_whole_row_group();
        return Status::OK();
    }

    std::sort(skipped_row_ranges.begin(), skipped_row_ranges.end(),
              [](const RowRange& lhs, const RowRange& rhs) {
                  return std::tie(lhs.first_row, lhs.last_row) <
                         std::tie(rhs.first_row, rhs.last_row);
              });
    int skip_end = 0;
    int64_t read_rows = 0;
    for (auto& skip_range : skipped_row_ranges) {
        if (skip_end >= skip_range.first_row) {
            if (skip_end < skip_range.last_row) {
                skip_end = skip_range.last_row;
            }
        } else {
            // read row with candidate ranges rather than skipped ranges
            candidate_row_ranges.emplace_back(skip_end, skip_range.first_row);
            read_rows += skip_range.first_row - skip_end;
            skip_end = skip_range.last_row;
        }
    }
    DCHECK_LE(skip_end, row_group.num_rows);
    if (skip_end != row_group.num_rows) {
        candidate_row_ranges.emplace_back(skip_end, row_group.num_rows);
        read_rows += row_group.num_rows - skip_end;
    }
    _statistics.read_rows += read_rows;
    _statistics.filtered_page_rows += row_group.num_rows - read_rows;
    return Status::OK();
}

Status ParquetReader::_process_row_group_filter(const tparquet::RowGroup& row_group,
                                                bool* filter_group) {
    static_cast<void>(_process_column_stat_filter(row_group.columns, filter_group));
    _init_chunk_dicts();
    RETURN_IF_ERROR(_process_dict_filter(filter_group));
    _init_bloom_filter();
    RETURN_IF_ERROR(_process_bloom_filter(filter_group));
    return Status::OK();
}

Status ParquetReader::_process_column_stat_filter(const std::vector<tparquet::ColumnChunk>& columns,
                                                  bool* filter_group) {
    if (_colname_to_value_range == nullptr || _colname_to_value_range->empty()) {
        return Status::OK();
    }
    auto& schema_desc = _file_metadata->schema();
    for (auto& col_name : _read_columns) {
        auto slot_iter = _colname_to_value_range->find(col_name);
        if (slot_iter == _colname_to_value_range->end()) {
            continue;
        }
        int parquet_col_id = _file_metadata->schema().get_column(col_name)->physical_column_index;
        if (parquet_col_id < 0) {
            // complex type, not support filter yet.
            continue;
        }
        auto& meta_data = columns[parquet_col_id].meta_data;
        auto& statistic = meta_data.statistics;
        bool is_all_null =
                (statistic.__isset.null_count && statistic.null_count == meta_data.num_values);
        bool is_set_min_max = (statistic.__isset.max && statistic.__isset.min) ||
                              (statistic.__isset.max_value && statistic.__isset.min_value);
        if ((!is_set_min_max) && (!is_all_null)) {
            continue;
        }
        const FieldSchema* col_schema = schema_desc.get_column(col_name);
        // Min-max of statistic is plain-encoded value
        if (statistic.__isset.min_value) {
            *filter_group = ParquetPredicate::filter_by_stats(
                    slot_iter->second, col_schema, is_set_min_max, statistic.min_value,
                    statistic.max_value, is_all_null, *_ctz, true);
        } else {
            *filter_group = ParquetPredicate::filter_by_stats(
                    slot_iter->second, col_schema, is_set_min_max, statistic.min, statistic.max,
                    is_all_null, *_ctz, false);
        }
        if (*filter_group) {
            break;
        }
    }
    return Status::OK();
}

void ParquetReader::_init_chunk_dicts() {}

Status ParquetReader::_process_dict_filter(bool* filter_group) {
    return Status::OK();
}

void ParquetReader::_init_bloom_filter() {}

Status ParquetReader::_process_bloom_filter(bool* filter_group) {
    return Status::OK();
}

int64_t ParquetReader::_get_column_start_offset(const tparquet::ColumnMetaData& column) {
    if (column.__isset.dictionary_page_offset) {
        DCHECK_LT(column.dictionary_page_offset, column.data_page_offset);
        return column.dictionary_page_offset;
    }
    return column.data_page_offset;
}
} // namespace doris::vectorized
