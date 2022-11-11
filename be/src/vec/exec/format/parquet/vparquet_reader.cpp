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

#include <algorithm>

#include "io/file_factory.h"
#include "parquet_pred_cmp.h"
#include "parquet_thrift_util.h"
#include "vec/exprs/vbloom_predicate.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/exprs/vslot_ref.h"

namespace doris::vectorized {

ParquetReader::ParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                             const TFileRangeDesc& range,
                             const std::vector<std::string>& column_names, size_t batch_size,
                             cctz::time_zone* ctz)
        : _profile(profile),
          _scan_params(params),
          _scan_range(range),
          _range_start_offset(range.start_offset),
          _range_size(range.size),
          _ctz(ctz),
          _column_names(column_names) {
    // ColumnSelectVector use uint16_t to save row index
    _batch_size = std::min(batch_size, (size_t)USHRT_MAX);
    _init_profile();
}

ParquetReader::ParquetReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                             const std::vector<std::string>& column_names)
        : _profile(nullptr),
          _scan_params(params),
          _scan_range(range),
          _column_names(column_names) {}

ParquetReader::~ParquetReader() {
    close();
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
        _parquet_profile.to_read_bytes =
                ADD_CHILD_COUNTER(_profile, "ReadBytes", TUnit::BYTES, parquet_profile);
        _parquet_profile.column_read_time =
                ADD_CHILD_TIMER(_profile, "ColumnReadTime", parquet_profile);
        _parquet_profile.parse_meta_time =
                ADD_CHILD_TIMER(_profile, "ParseMetaTime", parquet_profile);

        _parquet_profile.file_read_time = ADD_TIMER(_profile, "FileReadTime");
        _parquet_profile.file_read_calls = ADD_COUNTER(_profile, "FileReadCalls", TUnit::UNIT);
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
    if (!_closed) {
        if (_profile != nullptr) {
            COUNTER_UPDATE(_parquet_profile.filtered_row_groups, _statistics.filtered_row_groups);
            COUNTER_UPDATE(_parquet_profile.to_read_row_groups, _statistics.read_row_groups);
            COUNTER_UPDATE(_parquet_profile.filtered_group_rows, _statistics.filtered_group_rows);
            COUNTER_UPDATE(_parquet_profile.filtered_page_rows, _statistics.filtered_page_rows);
            COUNTER_UPDATE(_parquet_profile.lazy_read_filtered_rows,
                           _statistics.lazy_read_filtered_rows);
            COUNTER_UPDATE(_parquet_profile.filtered_bytes, _statistics.filtered_bytes);
            COUNTER_UPDATE(_parquet_profile.to_read_bytes, _statistics.read_bytes);
            COUNTER_UPDATE(_parquet_profile.column_read_time, _statistics.column_read_time);
            COUNTER_UPDATE(_parquet_profile.parse_meta_time, _statistics.parse_meta_time);

            COUNTER_UPDATE(_parquet_profile.file_read_time, _column_statistics.read_time);
            COUNTER_UPDATE(_parquet_profile.file_read_calls, _column_statistics.read_calls);
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

Status ParquetReader::init_reader(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
        VExprContext* vconjunct_ctx) {
    SCOPED_RAW_TIMER(&_statistics.parse_meta_time);
    if (_file_reader == nullptr) {
        RETURN_IF_ERROR(FileFactory::create_file_reader(_profile, _scan_params, _scan_range.path,
                                                        _scan_range.start_offset,
                                                        _scan_range.file_size, 0, _file_reader));
    }
    RETURN_IF_ERROR(_file_reader->open());
    if (_file_reader->size() == 0) {
        return Status::EndOfFile("Empty Parquet File");
    }
    RETURN_IF_ERROR(parse_thrift_footer(_file_reader.get(), _file_metadata));
    _t_metadata = &_file_metadata->to_thrift();
    _total_groups = _t_metadata->row_groups.size();
    if (_total_groups == 0) {
        return Status::EndOfFile("Empty Parquet File");
    }
    auto schema_desc = _file_metadata->schema();
    for (int i = 0; i < schema_desc.size(); ++i) {
        _map_column.emplace(schema_desc.get_column(i)->name, i);
    }
    _colname_to_value_range = colname_to_value_range;
    RETURN_IF_ERROR(_init_read_columns());
    // build column predicates for column lazy read
    _lazy_read_ctx.vconjunct_ctx = vconjunct_ctx;
    _init_lazy_read();
    RETURN_IF_ERROR(_init_row_group_readers());

    return Status::OK();
}

void ParquetReader::_init_lazy_read() {
    std::unordered_map<std::string, uint32_t> predicate_columns;
    std::function<void(VExpr * expr)> visit_slot = [&](VExpr* expr) {
        if (VSlotRef* slot_ref = typeid_cast<VSlotRef*>(expr)) {
            predicate_columns.emplace(slot_ref->expr_name(), slot_ref->column_id());
            if (slot_ref->column_id() == 0) {
                _lazy_read_ctx.resize_first_column = false;
            }
            return;
        } else if (VRuntimeFilterWrapper* runtime_filter =
                           typeid_cast<VRuntimeFilterWrapper*>(expr)) {
            VExpr* filter_impl = const_cast<VExpr*>(runtime_filter->get_impl());
            if (VBloomPredicate* bloom_predicate = typeid_cast<VBloomPredicate*>(filter_impl)) {
                for (VExpr* child : bloom_predicate->children()) {
                    visit_slot(child);
                }
            } else if (VInPredicate* in_predicate = typeid_cast<VInPredicate*>(filter_impl)) {
                visit_slot(in_predicate->children()[0]);
            } else {
                for (VExpr* child : filter_impl->children()) {
                    visit_slot(child);
                }
            }
        } else {
            for (VExpr* child : expr->children()) {
                visit_slot(child);
            }
        }
    };
    if (_lazy_read_ctx.vconjunct_ctx != nullptr) {
        visit_slot(_lazy_read_ctx.vconjunct_ctx->root());
    }
    for (auto& read_col : _read_columns) {
        _lazy_read_ctx.all_read_columns.emplace_back(read_col._file_slot_name);
        if (predicate_columns.size() > 0) {
            auto iter = predicate_columns.find(read_col._file_slot_name);
            if (iter == predicate_columns.end()) {
                _lazy_read_ctx.lazy_read_columns.emplace_back(read_col._file_slot_name);
            } else {
                _lazy_read_ctx.predicate_columns.emplace_back(iter->first);
                _lazy_read_ctx.predicate_col_ids.emplace_back(iter->second);
            }
        }
    }
    if (_lazy_read_ctx.predicate_columns.size() > 0 &&
        _lazy_read_ctx.lazy_read_columns.size() > 0) {
        if (predicate_columns.size() == _lazy_read_ctx.predicate_columns.size()) {
            // TODO: support partition columns
            // _vconjunct_ctx has partition columns, and will push down to row group reader.
            // However, row group reader can't get partition column values now.
            _lazy_read_ctx.can_lazy_read = true;
        }
    }
}

Status ParquetReader::_init_read_columns() {
    std::vector<int> include_column_ids;
    for (auto& file_col_name : _column_names) {
        auto iter = _map_column.find(file_col_name);
        if (iter != _map_column.end()) {
            include_column_ids.emplace_back(iter->second);
        } else {
            _missing_cols.push_back(file_col_name);
        }
    }
    // It is legal to get empty include_column_ids in query task.
    if (include_column_ids.empty()) {
        return Status::OK();
    }
    // The same order as physical columns
    std::sort(include_column_ids.begin(), include_column_ids.end());
    for (int& parquet_col_id : include_column_ids) {
        _read_columns.emplace_back(parquet_col_id,
                                   _file_metadata->schema().get_column(parquet_col_id)->name);
    }
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

Status ParquetReader::get_parsered_schema(std::vector<std::string>* col_names,
                                          std::vector<TypeDescriptor>* col_types) {
    if (_file_reader == nullptr) {
        RETURN_IF_ERROR(FileFactory::create_file_reader(_profile, _scan_params, _scan_range.path,
                                                        _scan_range.start_offset,
                                                        _scan_range.file_size, 0, _file_reader));
    }
    RETURN_IF_ERROR(_file_reader->open());
    if (_file_reader->size() == 0) {
        return Status::EndOfFile("Empty Parquet File");
    }
    RETURN_IF_ERROR(parse_thrift_footer(_file_reader.get(), _file_metadata));
    _t_metadata = &_file_metadata->to_thrift();

    _total_groups = _t_metadata->row_groups.size();
    if (_total_groups == 0) {
        return Status::EndOfFile("Empty Parquet File");
    }

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
    int32_t num_of_readers = _row_group_readers.size();
    DCHECK(num_of_readers <= _read_row_groups.size());
    if (_read_row_groups.empty()) {
        *eof = true;
        return Status::OK();
    }
    bool _batch_eof = false;
    {
        SCOPED_RAW_TIMER(&_statistics.column_read_time);
        RETURN_IF_ERROR(
                _current_group_reader->next_batch(block, _batch_size, read_rows, &_batch_eof));
    }
    if (_batch_eof) {
        auto column_st = _current_group_reader->statistics();
        _column_statistics.merge(column_st);
        _statistics.lazy_read_filtered_rows += _current_group_reader->lazy_read_filtered_rows();
        if (!_next_row_group_reader()) {
            *eof = true;
        }
    }
    VLOG_DEBUG << "ParquetReader::get_next_block: " << block->rows();
    return Status::OK();
}

bool ParquetReader::_next_row_group_reader() {
    if (_row_group_readers.empty()) {
        return false;
    }
    _current_group_reader = _row_group_readers.front();
    _row_group_readers.pop_front();
    return true;
}

Status ParquetReader::_init_row_group_readers() {
    RETURN_IF_ERROR(_filter_row_groups());
    for (auto row_group_id : _read_row_groups) {
        auto& row_group = _t_metadata->row_groups[row_group_id];
        std::shared_ptr<RowGroupReader> row_group_reader;
        row_group_reader.reset(new RowGroupReader(_file_reader.get(), _read_columns, row_group_id,
                                                  row_group, _ctz, _lazy_read_ctx));
        std::vector<RowRange> candidate_row_ranges;
        RETURN_IF_ERROR(_process_page_index(row_group, candidate_row_ranges));
        if (candidate_row_ranges.empty()) {
            _statistics.read_rows += row_group.num_rows;
        }
        RETURN_IF_ERROR(row_group_reader->init(_file_metadata->schema(), candidate_row_ranges,
                                               _col_offsets));
        _row_group_readers.emplace_back(row_group_reader);
    }
    if (!_next_row_group_reader()) {
        return Status::EndOfFile("No next reader");
    }
    return Status::OK();
}

Status ParquetReader::_filter_row_groups() {
    if (_total_groups == 0 || _t_metadata->num_rows == 0 || _range_size < 0) {
        return Status::EndOfFile("No row group need read");
    }
    for (int32_t row_group_idx = 0; row_group_idx < _total_groups; row_group_idx++) {
        const tparquet::RowGroup& row_group = _t_metadata->row_groups[row_group_idx];
        if (_is_misaligned_range_group(row_group)) {
            continue;
        }
        bool filter_group = false;
        RETURN_IF_ERROR(_process_row_group_filter(row_group, &filter_group));
        int64_t group_size = 0; // only calculate the needed columns
        for (auto& read_col : _read_columns) {
            auto& parquet_col_id = read_col._parquet_col_id;
            if (row_group.columns[parquet_col_id].__isset.meta_data) {
                group_size += row_group.columns[parquet_col_id].meta_data.total_compressed_size;
            }
        }
        if (!filter_group) {
            _read_row_groups.emplace_back(row_group_idx);
            _statistics.read_row_groups++;
            _statistics.read_bytes += group_size;
        } else {
            _statistics.filtered_row_groups++;
            _statistics.filtered_bytes += group_size;
            _statistics.filtered_group_rows += row_group.num_rows;
        }
    }
    return Status::OK();
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
    PageIndex page_index;
    if (!_has_page_index(row_group.columns, page_index)) {
        return Status::OK();
    }
    int64_t buffer_size = page_index._column_index_size + page_index._offset_index_size;
    uint8_t buff[buffer_size];
    int64_t bytes_read = 0;
    RETURN_IF_ERROR(
            _file_reader->readat(page_index._column_index_start, buffer_size, &bytes_read, buff));

    auto& schema_desc = _file_metadata->schema();
    std::vector<RowRange> skipped_row_ranges;
    for (auto& read_col : _read_columns) {
        auto conjunct_iter = _colname_to_value_range->find(read_col._file_slot_name);
        if (_colname_to_value_range->end() == conjunct_iter) {
            continue;
        }
        auto& chunk = row_group.columns[read_col._parquet_col_id];
        tparquet::ColumnIndex column_index;
        if (chunk.column_index_offset == 0 && chunk.column_index_length == 0) {
            return Status::OK();
        }
        RETURN_IF_ERROR(page_index.parse_column_index(chunk, buff, &column_index));
        const int num_of_pages = column_index.null_pages.size();
        if (num_of_pages <= 0) {
            break;
        }
        auto& conjuncts = conjunct_iter->second;
        std::vector<int> skipped_page_range;
        const FieldSchema* col_schema = schema_desc.get_column(read_col._file_slot_name);
        page_index.collect_skipped_page_range(&column_index, conjuncts, col_schema,
                                              skipped_page_range);
        if (skipped_page_range.empty()) {
            break;
        }
        tparquet::OffsetIndex offset_index;
        RETURN_IF_ERROR(page_index.parse_offset_index(chunk, buff, buffer_size, &offset_index));
        for (int page_id : skipped_page_range) {
            RowRange skipped_row_range;
            page_index.create_skipped_row_range(offset_index, row_group.num_rows, page_id,
                                                &skipped_row_range);
            // use the union row range
            skipped_row_ranges.emplace_back(skipped_row_range);
        }
        _col_offsets.emplace(read_col._parquet_col_id, offset_index);
    }
    if (skipped_row_ranges.empty()) {
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
    _process_column_stat_filter(row_group.columns, filter_group);
    _init_chunk_dicts();
    RETURN_IF_ERROR(_process_dict_filter(filter_group));
    _init_bloom_filter();
    RETURN_IF_ERROR(_process_bloom_filter(filter_group));
    return Status::OK();
}

Status ParquetReader::_process_column_stat_filter(const std::vector<tparquet::ColumnChunk>& columns,
                                                  bool* filter_group) {
    auto& schema_desc = _file_metadata->schema();
    for (auto& col_name : _column_names) {
        auto col_iter = _map_column.find(col_name);
        if (col_iter == _map_column.end()) {
            continue;
        }
        auto slot_iter = _colname_to_value_range->find(col_name);
        if (slot_iter == _colname_to_value_range->end()) {
            continue;
        }
        int parquet_col_id = col_iter->second;
        auto& statistic = columns[parquet_col_id].meta_data.statistics;
        if (!statistic.__isset.max || !statistic.__isset.min) {
            continue;
        }
        const FieldSchema* col_schema = schema_desc.get_column(col_name);
        // Min-max of statistic is plain-encoded value
        *filter_group = determine_filter_min_max(slot_iter->second, col_schema, statistic.min,
                                                 statistic.max);
        if (*filter_group) {
            break;
        }
    }
    return Status::OK();
}

void ParquetReader::_init_chunk_dicts() {}

Status ParquetReader::_process_dict_filter(bool* filter_group) {
    return Status();
}

void ParquetReader::_init_bloom_filter() {}

Status ParquetReader::_process_bloom_filter(bool* filter_group) {
    return Status();
}

int64_t ParquetReader::_get_column_start_offset(const tparquet::ColumnMetaData& column) {
    if (column.__isset.dictionary_page_offset) {
        DCHECK_LT(column.dictionary_page_offset, column.data_page_offset);
        return column.dictionary_page_offset;
    }
    return column.data_page_offset;
}
} // namespace doris::vectorized
