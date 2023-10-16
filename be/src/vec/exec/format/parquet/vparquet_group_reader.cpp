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

#include "vparquet_group_reader.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/parquet_types.h>
#include <string.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "gutil/stringprintf.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "schema_desc.h"
#include "util/simd/bits.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vdirect_in_predicate.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"
#include "vparquet_column_reader.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
class RuntimeState;

namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::vectorized {

const std::vector<int64_t> RowGroupReader::NO_DELETE = {};

RowGroupReader::RowGroupReader(io::FileReaderSPtr file_reader,
                               const std::vector<std::string>& read_columns,
                               const int32_t row_group_id, const tparquet::RowGroup& row_group,
                               cctz::time_zone* ctz, io::IOContext* io_ctx,
                               const PositionDeleteContext& position_delete_ctx,
                               const LazyReadContext& lazy_read_ctx, RuntimeState* state)
        : _file_reader(file_reader),
          _read_columns(read_columns),
          _row_group_id(row_group_id),
          _row_group_meta(row_group),
          _remaining_rows(row_group.num_rows),
          _ctz(ctz),
          _io_ctx(io_ctx),
          _position_delete_ctx(position_delete_ctx),
          _lazy_read_ctx(lazy_read_ctx),
          _state(state),
          _obj_pool(new ObjectPool()) {}

RowGroupReader::~RowGroupReader() {
    _column_readers.clear();
    _obj_pool->clear();
}

Status RowGroupReader::init(
        const FieldDescriptor& schema, std::vector<RowRange>& row_ranges,
        std::unordered_map<int, tparquet::OffsetIndex>& col_offsets,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts) {
    _tuple_descriptor = tuple_descriptor;
    _row_descriptor = row_descriptor;
    _col_name_to_slot_id = colname_to_slot_id;
    if (not_single_slot_filter_conjuncts != nullptr && !not_single_slot_filter_conjuncts->empty()) {
        _not_single_slot_filter_conjuncts.insert(_not_single_slot_filter_conjuncts.end(),
                                                 not_single_slot_filter_conjuncts->begin(),
                                                 not_single_slot_filter_conjuncts->end());
    }
    _slot_id_to_filter_conjuncts = slot_id_to_filter_conjuncts;
    _merge_read_ranges(row_ranges);
    if (_read_columns.empty()) {
        // Query task that only select columns in path.
        return Status::OK();
    }
    const size_t MAX_GROUP_BUF_SIZE = config::parquet_rowgroup_max_buffer_mb << 20;
    const size_t MAX_COLUMN_BUF_SIZE = config::parquet_column_max_buffer_mb << 20;
    size_t max_buf_size = std::min(MAX_COLUMN_BUF_SIZE, MAX_GROUP_BUF_SIZE / _read_columns.size());
    for (auto& read_col : _read_columns) {
        auto field = const_cast<FieldSchema*>(schema.get_column(read_col));
        std::unique_ptr<ParquetColumnReader> reader;
        RETURN_IF_ERROR(ParquetColumnReader::create(_file_reader, field, _row_group_meta,
                                                    _read_ranges, _ctz, _io_ctx, reader,
                                                    max_buf_size));
        if (reader == nullptr) {
            VLOG_DEBUG << "Init row group(" << _row_group_id << ") reader failed";
            return Status::Corruption("Init row group reader failed");
        }
        _column_readers[read_col] = std::move(reader);
    }
    // Check if single slot can be filtered by dict.
    if (!_slot_id_to_filter_conjuncts) {
        return Status::OK();
    }
    const std::vector<string>& predicate_col_names = _lazy_read_ctx.predicate_columns.first;
    const std::vector<int>& predicate_col_slot_ids = _lazy_read_ctx.predicate_columns.second;
    for (size_t i = 0; i < predicate_col_names.size(); ++i) {
        const string& predicate_col_name = predicate_col_names[i];
        int slot_id = predicate_col_slot_ids[i];
        auto field = const_cast<FieldSchema*>(schema.get_column(predicate_col_name));
        if (_can_filter_by_dict(slot_id,
                                _row_group_meta.columns[field->physical_column_index].meta_data)) {
            _dict_filter_cols.emplace_back(std::make_pair(predicate_col_name, slot_id));
        } else {
            if (_slot_id_to_filter_conjuncts->find(slot_id) !=
                _slot_id_to_filter_conjuncts->end()) {
                for (auto& ctx : _slot_id_to_filter_conjuncts->at(slot_id)) {
                    _filter_conjuncts.push_back(ctx);
                }
            }
        }
    }
    // Add predicate_partition_columns in _slot_id_to_filter_conjuncts(single slot conjuncts)
    // to _filter_conjuncts, others should be added from not_single_slot_filter_conjuncts.
    for (auto& kv : _lazy_read_ctx.predicate_partition_columns) {
        auto& [value, slot_desc] = kv.second;
        auto iter = _slot_id_to_filter_conjuncts->find(slot_desc->id());
        if (iter != _slot_id_to_filter_conjuncts->end()) {
            for (auto& ctx : iter->second) {
                _filter_conjuncts.push_back(ctx);
            }
        }
    }
    RETURN_IF_ERROR(_rewrite_dict_predicates());
    return Status::OK();
}

bool RowGroupReader::_can_filter_by_dict(int slot_id,
                                         const tparquet::ColumnMetaData& column_metadata) {
    if (column_metadata.encodings[0] != tparquet::Encoding::RLE_DICTIONARY ||
        column_metadata.type != tparquet::Type::BYTE_ARRAY) {
        return false;
    }

    if (_slot_id_to_filter_conjuncts->find(slot_id) == _slot_id_to_filter_conjuncts->end()) {
        return false;
    }

    if (!is_dictionary_encoded(column_metadata)) {
        return false;
    }

    // TODO：check expr like 'a > 10 is null', 'a > 10' should can be filter by dict.
    std::function<bool(const VExpr* expr)> visit_function_call = [&](const VExpr* expr) {
        if (expr->node_type() == TExprNodeType::FUNCTION_CALL) {
            std::string is_null_str;
            std::string function_name = expr->fn().name.function_name;
            if (function_name.compare("is_null_pred") == 0 ||
                function_name.compare("is_not_null_pred") == 0) {
                return false;
            }
        } else {
            for (auto& child : expr->children()) {
                if (!visit_function_call(child.get())) {
                    return false;
                }
            }
        }
        return true;
    };
    for (auto& ctx : _slot_id_to_filter_conjuncts->at(slot_id)) {
        if (!visit_function_call(ctx->root().get())) {
            return false;
        }
    }
    return true;
}

// This function is copied from
// https://github.com/apache/impala/blob/master/be/src/exec/parquet/hdfs-parquet-scanner.cc#L1717
bool RowGroupReader::is_dictionary_encoded(const tparquet::ColumnMetaData& column_metadata) {
    // The Parquet spec allows for column chunks to have mixed encodings
    // where some data pages are dictionary-encoded and others are plain
    // encoded. For example, a Parquet file writer might start writing
    // a column chunk as dictionary encoded, but it will switch to plain
    // encoding if the dictionary grows too large.
    //
    // In order for dictionary filters to skip the entire row group,
    // the conjuncts must be evaluated on column chunks that are entirely
    // encoded with the dictionary encoding. There are two checks
    // available to verify this:
    // 1. The encoding_stats field on the column chunk metadata provides
    //    information about the number of data pages written in each
    //    format. This allows for a specific check of whether all the
    //    data pages are dictionary encoded.
    // 2. The encodings field on the column chunk metadata lists the
    //    encodings used. If this list contains the dictionary encoding
    //    and does not include unexpected encodings (i.e. encodings not
    //    associated with definition/repetition levels), then it is entirely
    //    dictionary encoded.
    if (column_metadata.__isset.encoding_stats) {
        // Condition #1 above
        for (const tparquet::PageEncodingStats& enc_stat : column_metadata.encoding_stats) {
            if (enc_stat.page_type == tparquet::PageType::DATA_PAGE &&
                (enc_stat.encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
                 enc_stat.encoding != tparquet::Encoding::RLE_DICTIONARY) &&
                enc_stat.count > 0) {
                return false;
            }
        }
    } else {
        // Condition #2 above
        bool has_dict_encoding = false;
        bool has_nondict_encoding = false;
        for (const tparquet::Encoding::type& encoding : column_metadata.encodings) {
            if (encoding == tparquet::Encoding::PLAIN_DICTIONARY ||
                encoding == tparquet::Encoding::RLE_DICTIONARY) {
                has_dict_encoding = true;
            }

            // RLE and BIT_PACKED are used for repetition/definition levels
            if (encoding != tparquet::Encoding::PLAIN_DICTIONARY &&
                encoding != tparquet::Encoding::RLE_DICTIONARY &&
                encoding != tparquet::Encoding::RLE && encoding != tparquet::Encoding::BIT_PACKED) {
                has_nondict_encoding = true;
                break;
            }
        }
        // Not entirely dictionary encoded if:
        // 1. No dictionary encoding listed
        // OR
        // 2. Some non-dictionary encoding is listed
        if (!has_dict_encoding || has_nondict_encoding) {
            return false;
        }
    }

    return true;
}

Status RowGroupReader::next_batch(Block* block, size_t batch_size, size_t* read_rows,
                                  bool* batch_eof) {
    if (_is_row_group_filtered) {
        *read_rows = 0;
        *batch_eof = true;
        return Status::OK();
    }

    // Process external table query task that select columns are all from path.
    if (_read_columns.empty()) {
        RETURN_IF_ERROR(_read_empty_batch(batch_size, read_rows, batch_eof));
        RETURN_IF_ERROR(
                _fill_partition_columns(block, *read_rows, _lazy_read_ctx.partition_columns));
        RETURN_IF_ERROR(_fill_missing_columns(block, *read_rows, _lazy_read_ctx.missing_columns));

        Status st = VExprContext::filter_block(_lazy_read_ctx.conjuncts, block, block->columns());
        *read_rows = block->rows();
        return st;
    }
    if (_lazy_read_ctx.can_lazy_read) {
        // call _do_lazy_read recursively when current batch is skipped
        return _do_lazy_read(block, batch_size, read_rows, batch_eof);
    } else {
        ColumnSelectVector run_length_vector;
        RETURN_IF_ERROR(_read_column_data(block, _lazy_read_ctx.all_read_columns, batch_size,
                                          read_rows, batch_eof, run_length_vector));
        RETURN_IF_ERROR(
                _fill_partition_columns(block, *read_rows, _lazy_read_ctx.partition_columns));
        RETURN_IF_ERROR(_fill_missing_columns(block, *read_rows, _lazy_read_ctx.missing_columns));

        if (block->rows() == 0) {
            _convert_dict_cols_to_string_cols(block);
            *read_rows = block->rows();
            return Status::OK();
        }

        RETURN_IF_ERROR(_build_pos_delete_filter(*read_rows));

        std::vector<uint32_t> columns_to_filter;
        int column_to_keep = block->columns();
        columns_to_filter.resize(column_to_keep);
        for (uint32_t i = 0; i < column_to_keep; ++i) {
            columns_to_filter[i] = i;
        }
        if (!_lazy_read_ctx.conjuncts.empty()) {
            std::vector<IColumn::Filter*> filters;
            if (_position_delete_ctx.has_filter) {
                filters.push_back(_pos_delete_filter_ptr.get());
            }
            IColumn::Filter result_filter(block->rows(), 1);
            bool can_filter_all = false;
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(VExprContext::execute_conjuncts(
                    _filter_conjuncts, &filters, block, &result_filter, &can_filter_all));

            if (can_filter_all) {
                for (auto& col : columns_to_filter) {
                    std::move(*block->get_by_position(col).column).assume_mutable()->clear();
                }
                Block::erase_useless_column(block, column_to_keep);
                _convert_dict_cols_to_string_cols(block);
                return Status::OK();
            }

            if (!_not_single_slot_filter_conjuncts.empty()) {
                _convert_dict_cols_to_string_cols(block);
                std::vector<IColumn::Filter*> merged_filters;
                merged_filters.push_back(&result_filter);
                RETURN_IF_CATCH_EXCEPTION(
                        RETURN_IF_ERROR(VExprContext::execute_conjuncts_and_filter_block(
                                _not_single_slot_filter_conjuncts, &merged_filters, block,
                                columns_to_filter, column_to_keep)));
            } else {
                RETURN_IF_CATCH_EXCEPTION(
                        Block::filter_block_internal(block, columns_to_filter, result_filter));
                Block::erase_useless_column(block, column_to_keep);
                _convert_dict_cols_to_string_cols(block);
            }
        } else {
            RETURN_IF_CATCH_EXCEPTION(
                    RETURN_IF_ERROR(_filter_block(block, column_to_keep, columns_to_filter)));
        }
        *read_rows = block->rows();
        return Status::OK();
    }
}

void RowGroupReader::_merge_read_ranges(std::vector<RowRange>& row_ranges) {
    _read_ranges = row_ranges;
}

Status RowGroupReader::_read_column_data(Block* block, const std::vector<std::string>& columns,
                                         size_t batch_size, size_t* read_rows, bool* batch_eof,
                                         ColumnSelectVector& select_vector) {
    size_t batch_read_rows = 0;
    bool has_eof = false;
    for (auto& read_col_name : columns) {
        auto& column_with_type_and_name = block->get_by_name(read_col_name);
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        bool is_dict_filter = false;
        for (auto& _dict_filter_col : _dict_filter_cols) {
            if (_dict_filter_col.first == read_col_name) {
                MutableColumnPtr dict_column = ColumnVector<Int32>::create();
                size_t pos = block->get_position_by_name(read_col_name);
                if (column_type->is_nullable()) {
                    block->get_by_position(pos).type =
                            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
                    block->replace_by_position(
                            pos,
                            ColumnNullable::create(std::move(dict_column),
                                                   ColumnUInt8::create(dict_column->size(), 0)));
                } else {
                    block->get_by_position(pos).type = std::make_shared<DataTypeInt32>();
                    block->replace_by_position(pos, std::move(dict_column));
                }
                is_dict_filter = true;
                break;
            }
        }

        size_t col_read_rows = 0;
        bool col_eof = false;
        // Should reset _filter_map_index to 0 when reading next column.
        select_vector.reset();
        while (!col_eof && col_read_rows < batch_size) {
            size_t loop_rows = 0;
            RETURN_IF_ERROR(_column_readers[read_col_name]->read_column_data(
                    column_ptr, column_type, select_vector, batch_size - col_read_rows, &loop_rows,
                    &col_eof, is_dict_filter));
            col_read_rows += loop_rows;
        }
        if (batch_read_rows > 0 && batch_read_rows != col_read_rows) {
            return Status::Corruption("Can't read the same number of rows among parquet columns");
        }
        batch_read_rows = col_read_rows;
        if (col_eof) {
            has_eof = true;
        }
    }

    *read_rows = batch_read_rows;
    *batch_eof = has_eof;

    return Status::OK();
}

Status RowGroupReader::_do_lazy_read(Block* block, size_t batch_size, size_t* read_rows,
                                     bool* batch_eof) {
    std::unique_ptr<ColumnSelectVector> select_vector_ptr = nullptr;
    size_t pre_read_rows;
    bool pre_eof;
    std::vector<uint32_t> columns_to_filter;
    size_t origin_column_num = block->columns();
    columns_to_filter.resize(origin_column_num);
    for (uint32_t i = 0; i < origin_column_num; ++i) {
        columns_to_filter[i] = i;
    }
    IColumn::Filter result_filter;
    while (true) {
        // read predicate columns
        pre_read_rows = 0;
        pre_eof = false;
        ColumnSelectVector run_length_vector;
        RETURN_IF_ERROR(_read_column_data(block, _lazy_read_ctx.predicate_columns.first, batch_size,
                                          &pre_read_rows, &pre_eof, run_length_vector));
        if (pre_read_rows == 0) {
            DCHECK_EQ(pre_eof, true);
            break;
        }
        RETURN_IF_ERROR(_fill_partition_columns(block, pre_read_rows,
                                                _lazy_read_ctx.predicate_partition_columns));
        RETURN_IF_ERROR(_fill_missing_columns(block, pre_read_rows,
                                              _lazy_read_ctx.predicate_missing_columns));

        RETURN_IF_ERROR(_build_pos_delete_filter(pre_read_rows));

        // generate filter vector
        if (_lazy_read_ctx.resize_first_column) {
            // VExprContext.execute has an optimization, the filtering is executed when block->rows() > 0
            // The following process may be tricky and time-consuming, but we have no other way.
            block->get_by_position(0).column->assume_mutable()->resize(pre_read_rows);
        }
        result_filter.assign(pre_read_rows, static_cast<unsigned char>(1));
        bool can_filter_all = false;
        std::vector<IColumn::Filter*> filters;
        if (_position_delete_ctx.has_filter) {
            filters.push_back(_pos_delete_filter_ptr.get());
        }

        VExprContextSPtrs filter_contexts;
        for (auto& conjunct : _filter_conjuncts) {
            filter_contexts.emplace_back(conjunct);
        }
        RETURN_IF_ERROR(VExprContext::execute_conjuncts(filter_contexts, &filters, block,
                                                        &result_filter, &can_filter_all));

        if (_lazy_read_ctx.resize_first_column) {
            // We have to clean the first column to insert right data.
            block->get_by_position(0).column->assume_mutable()->clear();
        }

        const uint8_t* __restrict filter_map = result_filter.data();
        select_vector_ptr.reset(new ColumnSelectVector(filter_map, pre_read_rows, can_filter_all));
        if (select_vector_ptr->filter_all()) {
            for (auto& col : _lazy_read_ctx.predicate_columns.first) {
                // clean block to read predicate columns
                block->get_by_name(col).column->assume_mutable()->clear();
            }
            for (auto& col : _lazy_read_ctx.predicate_partition_columns) {
                block->get_by_name(col.first).column->assume_mutable()->clear();
            }
            for (auto& col : _lazy_read_ctx.predicate_missing_columns) {
                block->get_by_name(col.first).column->assume_mutable()->clear();
            }
            Block::erase_useless_column(block, origin_column_num);

            if (!pre_eof) {
                // If continuous batches are skipped, we can cache them to skip a whole page
                _cached_filtered_rows += pre_read_rows;
            } else { // pre_eof
                // If select_vector_ptr->filter_all() and pre_eof, we can skip whole row group.
                *read_rows = 0;
                *batch_eof = true;
                _lazy_read_filtered_rows += pre_read_rows;
                _convert_dict_cols_to_string_cols(block);
                return Status::OK();
            }
        } else {
            break;
        }
    }
    if (select_vector_ptr == nullptr) {
        DCHECK_EQ(pre_read_rows + _cached_filtered_rows, 0);
        *read_rows = 0;
        *batch_eof = true;
        return Status::OK();
    }

    ColumnSelectVector& select_vector = *select_vector_ptr;
    std::unique_ptr<uint8_t[]> rebuild_filter_map = nullptr;
    if (_cached_filtered_rows != 0) {
        _rebuild_select_vector(select_vector, rebuild_filter_map, pre_read_rows);
        pre_read_rows += _cached_filtered_rows;
        _cached_filtered_rows = 0;
    }

    // lazy read columns
    size_t lazy_read_rows;
    bool lazy_eof;
    RETURN_IF_ERROR(_read_column_data(block, _lazy_read_ctx.lazy_read_columns, pre_read_rows,
                                      &lazy_read_rows, &lazy_eof, select_vector));
    if (pre_read_rows != lazy_read_rows) {
        return Status::Corruption("Can't read the same number of rows when doing lazy read");
    }
    // pre_eof ^ lazy_eof
    // we set pre_read_rows as batch_size for lazy read columns, so pre_eof != lazy_eof

    // filter data in predicate columns, and remove filter column
    if (select_vector.has_filter()) {
        if (block->columns() == origin_column_num) {
            // the whole row group has been filtered by _lazy_read_ctx.vconjunct_ctx, and batch_eof is
            // generated from next batch, so the filter column is removed ahead.
            DCHECK_EQ(block->rows(), 0);
        } else {
            RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(
                    block, _lazy_read_ctx.all_predicate_col_ids, result_filter));
            Block::erase_useless_column(block, origin_column_num);
        }
    } else {
        Block::erase_useless_column(block, origin_column_num);
    }

    _convert_dict_cols_to_string_cols(block);

    size_t column_num = block->columns();
    size_t column_size = 0;
    for (int i = 0; i < column_num; ++i) {
        size_t cz = block->get_by_position(i).column->size();
        if (column_size != 0 && cz != 0) {
            DCHECK_EQ(column_size, cz);
        }
        if (cz != 0) {
            column_size = cz;
        }
    }
    _lazy_read_filtered_rows += pre_read_rows - column_size;
    *read_rows = column_size;

    *batch_eof = pre_eof;
    RETURN_IF_ERROR(_fill_partition_columns(block, column_size, _lazy_read_ctx.partition_columns));
    RETURN_IF_ERROR(_fill_missing_columns(block, column_size, _lazy_read_ctx.missing_columns));
    if (!_not_single_slot_filter_conjuncts.empty()) {
        std::vector<IColumn::Filter*> filters;
        filters.push_back(&result_filter);
        RETURN_IF_CATCH_EXCEPTION(RETURN_IF_ERROR(VExprContext::execute_conjuncts_and_filter_block(
                _not_single_slot_filter_conjuncts, nullptr, block, columns_to_filter,
                origin_column_num)));
    }
    return Status::OK();
}

void RowGroupReader::_rebuild_select_vector(ColumnSelectVector& select_vector,
                                            std::unique_ptr<uint8_t[]>& filter_map,
                                            size_t pre_read_rows) const {
    if (_cached_filtered_rows == 0) {
        return;
    }
    size_t total_rows = _cached_filtered_rows + pre_read_rows;
    if (select_vector.filter_all()) {
        select_vector.build(nullptr, total_rows, true);
        return;
    }

    uint8_t* map = new uint8_t[total_rows];
    filter_map.reset(map);
    for (size_t i = 0; i < _cached_filtered_rows; ++i) {
        map[i] = 0;
    }
    const uint8_t* old_map = select_vector.filter_map();
    if (old_map == nullptr) {
        // select_vector.filter_all() == true is already built.
        for (size_t i = _cached_filtered_rows; i < total_rows; ++i) {
            map[i] = 1;
        }
    } else {
        memcpy(map + _cached_filtered_rows, old_map, pre_read_rows);
    }
    select_vector.build(map, total_rows, false);
}

Status RowGroupReader::_fill_partition_columns(
        Block* block, size_t rows,
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns) {
    DataTypeSerDe::FormatOptions _text_formatOptions;
    for (auto& kv : partition_columns) {
        auto doris_column = block->get_by_name(kv.first).column;
        IColumn* col_ptr = const_cast<IColumn*>(doris_column.get());
        auto& [value, slot_desc] = kv.second;
        auto _text_serde = slot_desc->get_data_type_ptr()->get_serde();
        Slice slice(value.data(), value.size());
        vector<Slice> slices(rows);
        for (int i = 0; i < rows; i++) {
            slices[i] = {value.data(), value.size()};
        }
        int num_deserialized = 0;
        if (_text_serde->deserialize_column_from_json_vector(*col_ptr, slices, &num_deserialized,
                                                             _text_formatOptions) != Status::OK()) {
            return Status::InternalError("Failed to fill partition column: {}={}",
                                         slot_desc->col_name(), value);
        }
        if (num_deserialized != rows) {
            return Status::InternalError(
                    "Failed to fill partition column: {}={} ."
                    "Number of rows expected to be written : {}, number of rows actually written : "
                    "{}",
                    slot_desc->col_name(), value, num_deserialized, rows);
        }
    }
    return Status::OK();
}

Status RowGroupReader::_fill_missing_columns(
        Block* block, size_t rows,
        const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) {
    for (auto& kv : missing_columns) {
        if (kv.second == nullptr) {
            // no default column, fill with null
            auto nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
                    (*std::move(block->get_by_name(kv.first).column)).mutate().get());
            nullable_column->insert_many_defaults(rows);
        } else {
            // fill with default value
            auto& ctx = kv.second;
            auto origin_column_num = block->columns();
            int result_column_id = -1;
            // PT1 => dest primitive type
            RETURN_IF_ERROR(ctx->execute(block, &result_column_id));
            bool is_origin_column = result_column_id < origin_column_num;
            if (!is_origin_column) {
                // call resize because the first column of _src_block_ptr may not be filled by reader,
                // so _src_block_ptr->rows() may return wrong result, cause the column created by `ctx->execute()`
                // has only one row.
                std::move(*block->get_by_position(result_column_id).column).mutate()->resize(rows);
                auto result_column_ptr = block->get_by_position(result_column_id).column;
                // result_column_ptr maybe a ColumnConst, convert it to a normal column
                result_column_ptr = result_column_ptr->convert_to_full_column_if_const();
                auto origin_column_type = block->get_by_name(kv.first).type;
                bool is_nullable = origin_column_type->is_nullable();
                block->replace_by_position(
                        block->get_position_by_name(kv.first),
                        is_nullable ? make_nullable(result_column_ptr) : result_column_ptr);
                block->erase(result_column_id);
            }
        }
    }
    return Status::OK();
}

Status RowGroupReader::_read_empty_batch(size_t batch_size, size_t* read_rows, bool* batch_eof) {
    if (_position_delete_ctx.has_filter) {
        int64_t start_row_id = _position_delete_ctx.current_row_id;
        int64_t end_row_id = std::min(_position_delete_ctx.current_row_id + (int64_t)batch_size,
                                      _position_delete_ctx.last_row_id);
        int64_t num_delete_rows = 0;
        while (_position_delete_ctx.index < _position_delete_ctx.end_index) {
            const int64_t& delete_row_id =
                    _position_delete_ctx.delete_rows[_position_delete_ctx.index];
            if (delete_row_id < start_row_id) {
                _position_delete_ctx.index++;
            } else if (delete_row_id < end_row_id) {
                num_delete_rows++;
                _position_delete_ctx.index++;
            } else { // delete_row_id >= end_row_id
                break;
            }
        }
        *read_rows = end_row_id - start_row_id - num_delete_rows;
        _position_delete_ctx.current_row_id = end_row_id;
        *batch_eof = _position_delete_ctx.current_row_id == _position_delete_ctx.last_row_id;
    } else {
        if (batch_size < _remaining_rows) {
            *read_rows = batch_size;
            _remaining_rows -= batch_size;
            *batch_eof = false;
        } else {
            *read_rows = _remaining_rows;
            _remaining_rows = 0;
            *batch_eof = true;
        }
    }
    return Status::OK();
}

Status RowGroupReader::_build_pos_delete_filter(size_t read_rows) {
    if (!_position_delete_ctx.has_filter) {
        _pos_delete_filter_ptr.reset(nullptr);
        _total_read_rows += read_rows;
        return Status::OK();
    }
    _pos_delete_filter_ptr.reset(new IColumn::Filter(read_rows, 1));
    auto* __restrict _pos_delete_filter_data = _pos_delete_filter_ptr->data();
    while (_position_delete_ctx.index < _position_delete_ctx.end_index) {
        const int64_t delete_row_index_in_row_group =
                _position_delete_ctx.delete_rows[_position_delete_ctx.index] -
                _position_delete_ctx.first_row_id;
        int64_t read_range_rows = 0;
        size_t remaining_read_rows = _total_read_rows + read_rows;
        for (auto& range : _read_ranges) {
            if (delete_row_index_in_row_group < range.first_row) {
                ++_position_delete_ctx.index;
                break;
            } else if (delete_row_index_in_row_group < range.last_row) {
                int64_t index = (delete_row_index_in_row_group - range.first_row) +
                                read_range_rows - _total_read_rows;
                if (index > read_rows - 1) {
                    _total_read_rows += read_rows;
                    return Status::OK();
                }
                _pos_delete_filter_data[index] = 0;
                ++_position_delete_ctx.index;
                break;
            } else { // delete_row >= range.last_row
            }

            int64_t range_size = range.last_row - range.first_row;
            // Don't search next range when there is no remaining_read_rows.
            if (remaining_read_rows <= range_size) {
                _total_read_rows += read_rows;
                return Status::OK();
            } else {
                remaining_read_rows -= range_size;
                read_range_rows += range_size;
            }
        }
    }
    _total_read_rows += read_rows;
    return Status::OK();
}

// need exception safety
Status RowGroupReader::_filter_block(Block* block, int column_to_keep,
                                     const std::vector<uint32_t>& columns_to_filter) {
    if (_pos_delete_filter_ptr) {
        RETURN_IF_CATCH_EXCEPTION(
                Block::filter_block_internal(block, columns_to_filter, (*_pos_delete_filter_ptr)));
    }
    Block::erase_useless_column(block, column_to_keep);

    return Status::OK();
}

Status RowGroupReader::_rewrite_dict_predicates() {
    for (auto it = _dict_filter_cols.begin(); it != _dict_filter_cols.end();) {
        std::string& dict_filter_col_name = it->first;
        int slot_id = it->second;
        // 1. Get dictionary values to a string column.
        MutableColumnPtr dict_value_column = ColumnString::create();
        bool has_dict = false;
        RETURN_IF_ERROR(_column_readers[dict_filter_col_name]->read_dict_values_to_column(
                dict_value_column, &has_dict));
        size_t dict_value_column_size = dict_value_column->size();
        DCHECK(has_dict);
        // 2. Build a temp block from the dict string column, then execute conjuncts and filter block.
        // 2.1 Build a temp block from the dict string column to match the conjuncts executing.
        Block temp_block;
        int dict_pos = -1;
        int index = 0;
        for (const auto slot_desc : _tuple_descriptor->slots()) {
            if (!slot_desc->need_materialize()) {
                // should be ignored from reading
                continue;
            }
            if (slot_desc->id() == slot_id) {
                auto data_type = slot_desc->get_data_type_ptr();
                if (data_type->is_nullable()) {
                    temp_block.insert(
                            {ColumnNullable::create(std::move(dict_value_column),
                                                    ColumnUInt8::create(dict_value_column_size, 0)),
                             std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()),
                             ""});
                } else {
                    temp_block.insert(
                            {std::move(dict_value_column), std::make_shared<DataTypeString>(), ""});
                }
                dict_pos = index;

            } else {
                temp_block.insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                        slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name()));
            }
            ++index;
        }

        // 2.2 Execute conjuncts and filter block.
        VExprContextSPtrs ctxs;
        auto iter = _slot_id_to_filter_conjuncts->find(slot_id);
        if (iter != _slot_id_to_filter_conjuncts->end()) {
            for (auto& ctx : iter->second) {
                ctxs.push_back(ctx);
            }
        } else {
            std::stringstream msg;
            msg << "_slot_id_to_filter_conjuncts: slot_id [" << slot_id << "] not found";
            return Status::NotFound(msg.str());
        }

        std::vector<uint32_t> columns_to_filter(1, dict_pos);
        int column_to_keep = temp_block.columns();
        if (dict_pos != 0) {
            // VExprContext.execute has an optimization, the filtering is executed when block->rows() > 0
            // The following process may be tricky and time-consuming, but we have no other way.
            temp_block.get_by_position(0).column->assume_mutable()->resize(dict_value_column_size);
        }
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(VExprContext::execute_conjuncts_and_filter_block(
                ctxs, nullptr, &temp_block, columns_to_filter, column_to_keep));
        if (dict_pos != 0) {
            // We have to clean the first column to insert right data.
            temp_block.get_by_position(0).column->assume_mutable()->clear();
        }

        // Check some conditions.
        ColumnPtr& dict_column = temp_block.get_by_position(dict_pos).column;
        // If dict_column->size() == 0, can filter this row group.
        if (dict_column->size() == 0) {
            _is_row_group_filtered = true;
            return Status::OK();
        }

        // About Performance: if dict_column size is too large, it will generate a large IN filter.
        if (dict_column->size() > MAX_DICT_CODE_PREDICATE_TO_REWRITE) {
            it = _dict_filter_cols.erase(it);
            for (auto& ctx : ctxs) {
                _filter_conjuncts.push_back(ctx);
            }
            continue;
        }

        // 3. Get dict codes.
        std::vector<int32_t> dict_codes;
        if (dict_column->is_nullable()) {
            const ColumnNullable* nullable_column =
                    static_cast<const ColumnNullable*>(dict_column.get());
            const ColumnString* nested_column = static_cast<const ColumnString*>(
                    nullable_column->get_nested_column_ptr().get());
            RETURN_IF_ERROR(_column_readers[dict_filter_col_name]->get_dict_codes(
                    assert_cast<const ColumnString*>(nested_column), &dict_codes));
        } else {
            RETURN_IF_ERROR(_column_readers[dict_filter_col_name]->get_dict_codes(
                    assert_cast<const ColumnString*>(dict_column.get()), &dict_codes));
        }

        // 4. Rewrite conjuncts.
        static_cast<void>(_rewrite_dict_conjuncts(dict_codes, slot_id, dict_column->is_nullable()));
        ++it;
    }
    return Status::OK();
}

Status RowGroupReader::_rewrite_dict_conjuncts(std::vector<int32_t>& dict_codes, int slot_id,
                                               bool is_nullable) {
    VExprSPtr root;
    if (dict_codes.size() == 1) {
        {
            TFunction fn;
            TFunctionName fn_name;
            fn_name.__set_db_name("");
            fn_name.__set_function_name("eq");
            fn.__set_name(fn_name);
            fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
            std::vector<TTypeDesc> arg_types;
            arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
            arg_types.push_back(create_type_desc(PrimitiveType::TYPE_INT));
            fn.__set_arg_types(arg_types);
            fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            fn.__set_has_var_args(false);

            TExprNode texpr_node;
            texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            texpr_node.__set_node_type(TExprNodeType::BINARY_PRED);
            texpr_node.__set_opcode(TExprOpcode::EQ);
            texpr_node.__set_fn(fn);
            texpr_node.__set_child_type(TPrimitiveType::INT);
            texpr_node.__set_num_children(2);
            texpr_node.__set_is_nullable(is_nullable);
            root = VectorizedFnCall::create_shared(texpr_node);
        }
        {
            SlotDescriptor* slot = nullptr;
            const std::vector<SlotDescriptor*>& slots = _tuple_descriptor->slots();
            for (auto each : slots) {
                if (each->id() == slot_id) {
                    slot = each;
                    break;
                }
            }
            root->add_child(VSlotRef::create_shared(slot));
        }
        {
            TExprNode texpr_node;
            texpr_node.__set_node_type(TExprNodeType::INT_LITERAL);
            texpr_node.__set_type(create_type_desc(TYPE_INT));
            TIntLiteral int_literal;
            int_literal.__set_value(dict_codes[0]);
            texpr_node.__set_int_literal(int_literal);
            texpr_node.__set_is_nullable(is_nullable);
            root->add_child(VLiteral::create_shared(texpr_node));
        }
    } else {
        {
            TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
            TExprNode node;
            node.__set_type(type_desc);
            node.__set_node_type(TExprNodeType::IN_PRED);
            node.in_predicate.__set_is_not_in(false);
            node.__set_opcode(TExprOpcode::FILTER_IN);
            // VdirectInPredicate assume is_nullable = false.
            node.__set_is_nullable(false);

            root = vectorized::VDirectInPredicate::create_shared(node);
            std::shared_ptr<HybridSetBase> hybrid_set(
                    create_set(PrimitiveType::TYPE_INT, dict_codes.size()));
            for (int j = 0; j < dict_codes.size(); ++j) {
                hybrid_set->insert(&dict_codes[j]);
            }
            static_cast<vectorized::VDirectInPredicate*>(root.get())->set_filter(hybrid_set);
        }
        {
            SlotDescriptor* slot = nullptr;
            const std::vector<SlotDescriptor*>& slots = _tuple_descriptor->slots();
            for (auto each : slots) {
                if (each->id() == slot_id) {
                    slot = each;
                    break;
                }
            }
            root->add_child(VSlotRef::create_shared(slot));
        }
    }
    VExprContextSPtr rewritten_conjunct_ctx = VExprContext::create_shared(root);
    RETURN_IF_ERROR(rewritten_conjunct_ctx->prepare(_state, *_row_descriptor));
    RETURN_IF_ERROR(rewritten_conjunct_ctx->open(_state));
    _dict_filter_conjuncts.push_back(rewritten_conjunct_ctx);
    _filter_conjuncts.push_back(rewritten_conjunct_ctx);
    return Status::OK();
}

void RowGroupReader::_convert_dict_cols_to_string_cols(Block* block) {
    for (auto& dict_filter_cols : _dict_filter_cols) {
        size_t pos = block->get_position_by_name(dict_filter_cols.first);
        ColumnWithTypeAndName& column_with_type_and_name = block->get_by_position(pos);
        const ColumnPtr& column = column_with_type_and_name.column;
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
            const ColumnPtr& nested_column = nullable_column->get_nested_column_ptr();
            const ColumnInt32* dict_column = assert_cast<const ColumnInt32*>(nested_column.get());
            DCHECK(dict_column);

            MutableColumnPtr string_column =
                    _column_readers[dict_filter_cols.first]->convert_dict_column_to_string_column(
                            dict_column);

            column_with_type_and_name.type =
                    std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
            block->replace_by_position(
                    pos, ColumnNullable::create(std::move(string_column),
                                                nullable_column->get_null_map_column_ptr()));
        } else {
            const ColumnInt32* dict_column = assert_cast<const ColumnInt32*>(column.get());
            MutableColumnPtr string_column =
                    _column_readers[dict_filter_cols.first]->convert_dict_column_to_string_column(
                            dict_column);

            column_with_type_and_name.type = std::make_shared<DataTypeString>();
            block->replace_by_position(pos, std::move(string_column));
        }
    }
}

ParquetColumnReader::Statistics RowGroupReader::statistics() {
    ParquetColumnReader::Statistics st;
    for (auto& reader : _column_readers) {
        auto ost = reader.second->statistics();
        st.merge(ost);
    }
    return st;
}

} // namespace doris::vectorized