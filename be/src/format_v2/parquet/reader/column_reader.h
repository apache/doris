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

struct ParquetNullShapeSink {
    int16_t nullable_definition_level = 0;
    NullMap* null_map = nullptr;
};

struct ParquetPageSkipProfile {
    RuntimeProfile::Counter* skipped_pages = nullptr;
    RuntimeProfile::Counter* skipped_bytes = nullptr;
};

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

    // Read and materialize this subtree while exposing the null shape of an ancestor complex node.
    //
    // This is the transitional Phase-1 shape channel used by STRUCT when its projected children
    // are all complex. The child reader still owns value materialization, but it must derive one
    // ancestor null bit per parent row from the same Dremel level stream that drives its own
    // LIST/MAP/STRUCT shape. This keeps shape semantics explicit without registering nested
    // children as hidden block slots.
    virtual Status read_with_ancestor_shape(int64_t rows,
                                            int16_t ancestor_nullable_definition_level,
                                            MutableColumnPtr& column, int64_t* rows_read,
                                            NullMap* ancestor_nulls);

    // Same as read_with_ancestor_shape(), but exposes multiple ancestor null shapes from one pass
    // over the same Dremel stream. This is needed when a nested STRUCT with only complex children
    // must build its own parent validity and also provide an outer STRUCT validity to its parent.
    virtual Status read_with_ancestor_shapes(
            int64_t rows, const std::vector<ParquetNullShapeSink>& ancestor_shapes,
            MutableColumnPtr& column, int64_t* rows_read);

    // 跳过指定行数。这里必须使用 row-level skip，不能退回到 value-level Skip。
    virtual Status skip(int64_t rows);

    // 按 selection 读取当前 batch 中需要输出的行，并在末尾跳过 batch 内剩余行。
    // 该方法只允许通过 skip + read 推进 reader 游标，不允许退化为整批 read + filter。
    virtual Status select(const SelectionVector& sel, uint16_t selected_rows, int64_t batch_rows,
                          MutableColumnPtr& column);

    virtual Status load_nested_batch(int64_t rows);
    virtual Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                       int64_t* values_read);
    virtual const std::vector<int16_t>& nested_definition_levels() const;
    virtual const std::vector<int16_t>& nested_repetition_levels() const;
    virtual int64_t nested_levels_written() const;
    virtual bool is_or_has_repeated_child() const;

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
};

// Parquet column reader 工厂。
// 工厂绑定当前 row group，并根据 file-local schema tree 创建 Doris 自己的 column
// reader。Arrow internal RecordReader 的创建和缓存必须封装在这里，避免泄露到
// ParquetReader 主流程。后续 reader options、Dremel assembler、延时物化 cache/skip
// 策略都应挂在该工厂上下文里，而不是继续扩展自由函数参数。
class ParquetColumnReaderFactory {
public:
    ParquetColumnReaderFactory(std::shared_ptr<::parquet::RowGroupReader> row_group,
                               int num_leaf_columns,
                               const std::map<int, ParquetPageSkipPlan>* page_skip_plans = nullptr,
                               ParquetPageSkipProfile page_skip_profile = {},
                               const cctz::time_zone* timezone = nullptr,
                               bool enable_strict_mode = false,
                               ParquetColumnReaderProfile column_reader_profile = {});

    // 根据 file-local schema tree 创建 column reader。复杂类型会在这里递归创建
    // children。该入口只理解 Parquet file schema，不处理 table/global schema。
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
    Status create_scalar_column_reader(const ParquetColumnSchema& column_schema,
                                       std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create_nested_scalar_column_reader(const ParquetColumnSchema& column_schema,
                                              std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create_struct_column_reader(const ParquetColumnSchema& column_schema,
                                       const format::LocalColumnIndex* projection,
                                       std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create_list_column_reader(const ParquetColumnSchema& column_schema,
                                     const format::LocalColumnIndex* projection,
                                     std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create_map_column_reader(const ParquetColumnSchema& column_schema,
                                    const format::LocalColumnIndex* projection,
                                    std::unique_ptr<ParquetColumnReader>* reader) const;

    Status get_record_reader(int leaf_column_id, const ::parquet::ColumnDescriptor* descriptor,
                             const std::string& name,
                             std::shared_ptr<::parquet::internal::RecordReader>* reader) const;

    Status create_scalar_reader(const ParquetColumnSchema& column_schema,
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
