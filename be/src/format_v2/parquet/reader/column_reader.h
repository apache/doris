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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "format_v2/column_data.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/parquet_type.h"
#include "format_v2/parquet/selection_vector.h"
#include "runtime/runtime_profile.h"

namespace parquet {
class ColumnDescriptor;
class RowGroupReader;

namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris {
class IColumn;

namespace parquet {
struct ParquetColumnSchema;

// Doris 的 Parquet column reader 抽象。
// 该类包装 Arrow Parquet RecordReader，负责将 file-local Parquet leaf column 读取成
// Doris-owned column。它不理解 Iceberg/global schema，也不处理 table-level
// cast/default/generated/partition 语义。
class ParquetColumnReader {
public:
    virtual ~ParquetColumnReader() = default;

    // Reader-local schema id. Top-level readers return the root column ordinal; nested readers
    // return the child ordinal under their parent.
    virtual int file_column_id() const { return _field_id; }

    // Parquet leaf column id. This is the column id of the leaf column in the Parquet file schema, and can be used to access ColumnDescriptor, RecordReader, column chunk metadata and statistics.
    // For example, for a map column as `a: map<int, string>`, `a` is the top-level column with `parquet_leaf_column_id` = `file_column_id`. `a.key` is the first child of `a` so `parquet_leaf_column_id` = 0.
    virtual int parquet_leaf_column_id() const { return _leaf_column_id; }
    int16_t nullable_definition_level() const { return _nullable_definition_level; }
    int16_t repeated_repetition_level() const { return _repeated_repetition_level; }

    virtual const DataTypePtr& type() const { return _type; }
    virtual const std::string& name() const { return _name; }
    const ParquetColumnReaderProfile& profile() const { return _profile; }

    // 读取一个 file-local column batch。
    virtual Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) = 0;

    // 跳过指定行数。这里必须使用 row-level skip，不能退回到 value-level Skip。
    virtual Status skip(int64_t rows);

    // 按 selection 读取当前 batch 中需要输出的行，并在末尾跳过 batch 内剩余行。
    // 该方法只允许通过 skip + read 推进 reader 游标，不允许退化为整批 read + filter。
    virtual Status select(const SelectionVector& sel, uint16_t selected_rows, int64_t batch_rows,
                          MutableColumnPtr& column);

    virtual Status load_nested_batch(int64_t rows);
    virtual Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                       int64_t* values_read);
    virtual Status skip_nested_column(int64_t rows);
    virtual const std::vector<int16_t>& nested_definition_levels() const;
    virtual const std::vector<int16_t>& nested_repetition_levels() const;
    virtual int64_t nested_levels_written() const;
    virtual bool is_or_has_repeated_child() const;

    int64_t nested_build_level_cursor() const { return _nested_build_level_cursor; }
    void set_nested_build_level_cursor(int64_t cursor) {
        DORIS_CHECK(cursor >= 0);
        _nested_build_level_cursor = cursor;
    }
    void reset_nested_build_level_cursor() { _nested_build_level_cursor = 0; }

protected:
    ParquetColumnReader(const ParquetColumnSchema& schema, const DataTypePtr type,
                        ParquetColumnReaderProfile profile = {});
    ParquetColumnReader() = default;
    void update_reader_read_rows(int64_t rows) const;
    void update_reader_skip_rows(int64_t rows) const;

    ParquetColumnReaderProfile _profile;
    const int _field_id = -1;
    const int _leaf_column_id = -1;
    const int16_t _nullable_definition_level = 0;
    const int16_t _repeated_repetition_level = 0;
    const int16_t _definition_level = 0;
    const int16_t _repetition_level = 0;
    const int16_t _repeated_ancestor_definition_level = 0;
    const DataTypePtr _type;
    const std::string _name;
    int64_t _nested_build_level_cursor = 0;
};

// Creates Doris column readers for one Parquet row group.
//
// The factory owns row-group-local state that must be shared by all readers created for
// the same row group:
// - Arrow RecordReader instances, cached by file leaf column id.
// - Page skip plans and page skip profile counters.
// - Scalar materialization options such as timezone and strict mode.
//
// Public callers only ask for a top-level file column reader or for synthetic scan
// columns. Recursive construction of nested children stays private so physical Parquet
// schema details do not leak into ParquetScanScheduler or ParquetReader.
class ParquetColumnReaderFactory {
public:
    ParquetColumnReaderFactory(std::shared_ptr<::parquet::RowGroupReader> row_group,
                               int num_leaf_columns,
                               const std::map<int, ParquetPageSkipPlan>* page_skip_plans = nullptr,
                               ParquetPageSkipProfile page_skip_profile = {},
                               const cctz::time_zone* timezone = nullptr,
                               bool enable_strict_mode = false,
                               ParquetColumnReaderProfile column_reader_profile = {});

    // Creates a reader for a top-level file column schema. The optional projection uses
    // file-local child ids from ParquetColumnSchema and may select only part of a nested
    // subtree. Table/global schema mapping has already happened before this layer.
    Status create(const ParquetColumnSchema& column_schema,
                  const format::LocalColumnIndex* projection,
                  std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create(const ParquetColumnSchema& column_schema,
                  std::unique_ptr<ParquetColumnReader>* reader) const {
        return create(column_schema, nullptr, reader);
    }

    std::unique_ptr<ParquetColumnReader> create_row_position_column_reader(
            int64_t row_group_first_row) const;
    std::unique_ptr<ParquetColumnReader> create_global_rowid_column_reader(
            const format::GlobalRowIdContext& context, int64_t row_group_first_row) const;

private:
    // Creates a primitive leaf reader. Top-level primitive columns are restricted to flat
    // scalar layouts, while nested primitive leaves are allowed to carry def/rep levels
    // that will be consumed by their parent LIST/MAP/STRUCT readers.
    Status create_scalar_column_reader(const ParquetColumnSchema& column_schema, bool is_nested,
                                       std::unique_ptr<ParquetColumnReader>* reader) const;

    // Creates a STRUCT reader and recursively creates readers only for projected
    // children. For partial projections it rebuilds the output DataTypeStruct so the
    // materialized column contains only projected child fields.
    Status create_struct_column_reader(const ParquetColumnSchema& column_schema,
                                       const format::LocalColumnIndex* projection,
                                       std::unique_ptr<ParquetColumnReader>* reader) const;

    // Creates a LIST reader around the single element reader. If the element itself is
    // partially projected, this rebuilds the output DataTypeArray with the projected
    // element type.
    Status create_list_column_reader(const ParquetColumnSchema& column_schema,
                                     const format::LocalColumnIndex* projection,
                                     std::unique_ptr<ParquetColumnReader>* reader) const;

    // Creates a MAP reader around key and value readers. The schema builder folds the Parquet
    // key_value/entry wrapper into the MAP node, so children are direct key/value fields here.
    // Partial MAP projection means value-subtree pruning only. The key stream is always read in
    // full because it owns entry existence, offsets and key equality semantics; key child
    // projection is intentionally rejected.
    Status create_map_column_reader(const ParquetColumnSchema& column_schema,
                                    const format::LocalColumnIndex* projection,
                                    std::unique_ptr<ParquetColumnReader>* reader) const;

    // Private recursive dispatcher. is_nested is true for children of complex readers
    // and controls primitive-leaf validation; complex readers are always created from
    // normalized ParquetColumnSchema subtrees.
    Status create_column_reader(const ParquetColumnSchema& column_schema,
                                const format::LocalColumnIndex* projection, bool is_nested,
                                std::unique_ptr<ParquetColumnReader>* reader) const;

    // Lazily creates and caches the Arrow RecordReader for a file leaf column. Multiple
    // Doris readers may need the same leaf stream through different nested parents, so
    // RecordReader lifetime is tied to this row-group factory.
    Status get_record_reader(int leaf_column_id, const ::parquet::ColumnDescriptor* descriptor,
                             const std::string& name,
                             std::shared_ptr<::parquet::internal::RecordReader>* reader) const;

    // Final ScalarColumnReader construction after schema validation and RecordReader
    // lookup have already completed.
    Status make_scalar_column_reader(
            const ParquetColumnSchema& column_schema,
            std::shared_ptr<::parquet::internal::RecordReader> record_reader,
            std::unique_ptr<ParquetColumnReader>* reader) const;

    std::shared_ptr<::parquet::RowGroupReader> _row_group;
    mutable std::vector<std::shared_ptr<::parquet::internal::RecordReader>> _record_readers;
    const std::map<int, ParquetPageSkipPlan>* _page_skip_plans = nullptr;
    ParquetPageSkipProfile _page_skip_profile;
    const cctz::time_zone* _timezone = nullptr;
    bool _enable_strict_mode = false;
    ParquetColumnReaderProfile _column_reader_profile;
};

} // namespace parquet
} // namespace doris
