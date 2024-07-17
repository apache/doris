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
#include <common/status.h>
#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "io/fs/file_reader_writer_fwd.h"
#include "vec/columns/column.h"
#include "vec/exec/format/parquet/parquet_common.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vparquet_column_reader.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class SlotDescriptor;
class TupleDescriptor;

namespace io {
struct IOContext;
} // namespace io
namespace vectorized {
class Block;
class FieldDescriptor;
} // namespace vectorized
} // namespace doris
namespace tparquet {
class ColumnMetaData;
class OffsetIndex;
class RowGroup;
} // namespace tparquet

namespace doris::vectorized {

// TODO: we need to determine it by test.
static constexpr uint32_t MAX_DICT_CODE_PREDICATE_TO_REWRITE = std::numeric_limits<uint32_t>::max();

class RowGroupReader : public ProfileCollector {
public:
    static const std::vector<int64_t> NO_DELETE;

    struct RowGroupIndex {
        int32_t row_group_id;
        int64_t first_row;
        int64_t last_row;
        RowGroupIndex(int32_t id, int64_t first, int64_t last)
                : row_group_id(id), first_row(first), last_row(last) {}
    };

    struct LazyReadContext {
        VExprContextSPtrs conjuncts;
        bool can_lazy_read = false;
        // block->rows() returns the number of rows of the first column,
        // so we should check and resize the first column
        bool resize_first_column = true;
        std::vector<std::string> all_read_columns;
        // include predicate_partition_columns & predicate_missing_columns
        std::vector<uint32_t> all_predicate_col_ids;
        // save slot_id to find dict filter column name, because expr column name may
        // be different with parquet column name
        // std::pair<std::vector<col_name>, std::vector<slot_id>>
        std::pair<std::vector<std::string>, std::vector<int>> predicate_columns;
        std::vector<std::string> lazy_read_columns;
        std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
                predicate_partition_columns;
        // lazy read partition columns or all partition columns
        std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>
                partition_columns;
        std::unordered_map<std::string, VExprContextSPtr> predicate_missing_columns;
        VExprContextSPtrs missing_columns_conjuncts;
        // lazy read missing columns or all missing columns
        std::unordered_map<std::string, VExprContextSPtr> missing_columns;
        // should turn off filtering by page index, lazy read and dict filter if having complex type
        bool has_complex_type = false;
    };

    /**
     * Support row-level delete in iceberg:
     * https://iceberg.apache.org/spec/#position-delete-files
     */
    struct PositionDeleteContext {
        // the filtered rows in current row group
        const std::vector<int64_t>& delete_rows;
        // the first row id of current row group in parquet file
        const int64_t first_row_id;
        // the number of rows in current row group
        const int64_t num_rows;
        const int64_t last_row_id;
        // current row id to read in the row group
        int64_t current_row_id;
        // start index in delete_rows
        const int64_t start_index;
        // end index in delete_rows
        const int64_t end_index;
        // current index in delete_rows
        int64_t index;
        const bool has_filter;

        PositionDeleteContext(const std::vector<int64_t>& delete_rows, const int64_t num_rows,
                              const int64_t first_row_id, const int64_t start_index,
                              const int64_t end_index)
                : delete_rows(delete_rows),
                  first_row_id(first_row_id),
                  num_rows(num_rows),
                  last_row_id(first_row_id + num_rows),
                  current_row_id(first_row_id),
                  start_index(start_index),
                  end_index(end_index),
                  index(start_index),
                  has_filter(end_index > start_index) {}

        PositionDeleteContext(const int64_t num_rows, const int64_t first_row)
                : PositionDeleteContext(NO_DELETE, num_rows, first_row, 0, 0) {}

        PositionDeleteContext(const PositionDeleteContext& filter) = default;
    };

    RowGroupReader(io::FileReaderSPtr file_reader, const std::vector<std::string>& read_columns,
                   const int32_t row_group_id, const tparquet::RowGroup& row_group,
                   cctz::time_zone* ctz, io::IOContext* io_ctx,
                   const PositionDeleteContext& position_delete_ctx,
                   const LazyReadContext& lazy_read_ctx, RuntimeState* state);

    ~RowGroupReader();
    Status init(const FieldDescriptor& schema, std::vector<RowRange>& row_ranges,
                std::unordered_map<int, tparquet::OffsetIndex>& col_offsets,
                const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
                const std::unordered_map<std::string, int>* colname_to_slot_id,
                const VExprContextSPtrs* not_single_slot_filter_conjuncts,
                const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts);
    Status next_batch(Block* block, size_t batch_size, size_t* read_rows, bool* batch_eof);
    int64_t lazy_read_filtered_rows() const { return _lazy_read_filtered_rows; }

    ParquetColumnReader::Statistics statistics();
    void set_remaining_rows(int64_t rows) { _remaining_rows = rows; }
    int64_t get_remaining_rows() { return _remaining_rows; }

protected:
    void _collect_profile_before_close() override {
        if (_file_reader != nullptr) {
            _file_reader->collect_profile_before_close();
        }
    }

private:
    void _merge_read_ranges(std::vector<RowRange>& row_ranges);
    Status _read_empty_batch(size_t batch_size, size_t* read_rows, bool* batch_eof);
    Status _read_column_data(Block* block, const std::vector<std::string>& columns,
                             size_t batch_size, size_t* read_rows, bool* batch_eof,
                             ColumnSelectVector& select_vector);
    Status _do_lazy_read(Block* block, size_t batch_size, size_t* read_rows, bool* batch_eof);
    void _rebuild_select_vector(ColumnSelectVector& select_vector,
                                std::unique_ptr<uint8_t[]>& filter_map, size_t pre_read_rows) const;
    Status _fill_partition_columns(
            Block* block, size_t rows,
            const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                    partition_columns);
    Status _fill_missing_columns(
            Block* block, size_t rows,
            const std::unordered_map<std::string, VExprContextSPtr>& missing_columns);
    Status _build_pos_delete_filter(size_t read_rows);
    Status _filter_block(Block* block, int column_to_keep,
                         const std::vector<uint32_t>& columns_to_filter);
    Status _filter_block_internal(Block* block, const std::vector<uint32_t>& columns_to_filter,
                                  const IColumn::Filter& filter);

    bool _can_filter_by_dict(int slot_id, const tparquet::ColumnMetaData& column_metadata);
    bool is_dictionary_encoded(const tparquet::ColumnMetaData& column_metadata);
    Status _rewrite_dict_predicates();
    Status _rewrite_dict_conjuncts(std::vector<int32_t>& dict_codes, int slot_id, bool is_nullable);
    void _convert_dict_cols_to_string_cols(Block* block);

    io::FileReaderSPtr _file_reader;
    std::unordered_map<std::string, std::unique_ptr<ParquetColumnReader>> _column_readers;
    const std::vector<std::string>& _read_columns;
    const int32_t _row_group_id;
    const tparquet::RowGroup& _row_group_meta;
    int64_t _remaining_rows;
    cctz::time_zone* _ctz = nullptr;
    io::IOContext* _io_ctx = nullptr;
    PositionDeleteContext _position_delete_ctx;
    // merge the row ranges generated from page index and position delete.
    std::vector<RowRange> _read_ranges;

    const LazyReadContext& _lazy_read_ctx;
    int64_t _lazy_read_filtered_rows = 0;
    // If continuous batches are skipped, we can cache them to skip a whole page
    size_t _cached_filtered_rows = 0;
    std::unique_ptr<IColumn::Filter> _pos_delete_filter_ptr;
    int64_t _total_read_rows = 0;
    const TupleDescriptor* _tuple_descriptor = nullptr;
    const RowDescriptor* _row_descriptor = nullptr;
    const std::unordered_map<std::string, int>* _col_name_to_slot_id = nullptr;
    VExprContextSPtrs _not_single_slot_filter_conjuncts;
    const std::unordered_map<int, VExprContextSPtrs>* _slot_id_to_filter_conjuncts = nullptr;
    VExprContextSPtrs _dict_filter_conjuncts;
    VExprContextSPtrs _filter_conjuncts;
    // std::pair<col_name, slot_id>
    std::vector<std::pair<std::string, int>> _dict_filter_cols;
    RuntimeState* _state = nullptr;
    std::shared_ptr<ObjectPool> _obj_pool;
    bool _is_row_group_filtered = false;
};
} // namespace doris::vectorized
