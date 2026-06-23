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

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/consts.h"
#include "core/column/column.h"
#include "exprs/aggregate/aggregate_function.h"
#include "io/io_common.h"
#include "runtime/thread_context.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

// The class is used to represent row's format in memory.  Each row contains
// multiple columns, some of which are key-columns (the rest are value-columns).
// NOTE: If both key-columns and value-columns exist, then the key-columns
// must be placed before value-columns.
//
// To compare two rows whose schemas are different, but they are from the same origin
// we store all column schema maybe accessed here. And default access through column id
class Schema;
using SchemaSPtr = std::shared_ptr<const Schema>;
class Schema {
public:
    // All the columns of one table may exist in the columns param, but col_ids is only a subset.
    Schema(const std::vector<TabletColumnPtr>& columns, const std::vector<ColumnId>& col_ids) {
        size_t num_key_columns = 0;
        for (int i = 0; i < columns.size(); ++i) {
            if (columns[i]->is_key()) {
                ++num_key_columns;
            }
            if (columns[i]->name() == DELETE_SIGN) {
                _delete_sign_idx = i;
            }
            if (columns[i]->name() == BeConsts::ROWID_COL ||
                columns[i]->name().starts_with(BeConsts::GLOBAL_ROWID_COL)) {
                _rowid_col_idx = i;
            }
            if (columns[i]->name() == VERSION_COL) {
                _version_col_idx = i;
            }
            if (columns[i]->name() == BINLOG_LSN_COL) {
                _lsn_col_idx = i;
            }
            if (columns[i]->name() == BINLOG_OP_COL) {
                _op_col_idx = i;
            }
            if (columns[i]->name() == BINLOG_TSO_COL) {
                _tso_col_idx = i;
            }
            if (columns[i]->name() == COMMIT_TSO_COL) {
                _commit_tso_col_idx = i;
            }
        }
        _init(columns, col_ids, num_key_columns);
    }

    Schema(const Schema&);
    Schema& operator=(const Schema& other);

    ~Schema();

    static DataTypePtr get_data_type_ptr(const TabletColumn& column);

    static IColumn::MutablePtr get_predicate_column_ptr(const DataTypePtr& data_type,
                                                        const ReaderType reader_type);

    const std::vector<TabletColumnPtr>& columns() const { return _cols; }

    const TabletColumn* column(ColumnId cid) const { return _cols[cid].get(); }

    size_t num_key_columns() const { return _num_key_columns; }

    size_t num_columns() const { return _cols.size(); }
    size_t num_column_ids() const { return _col_ids.size(); }
    const std::vector<ColumnId>& column_ids() const { return _col_ids; }
    ColumnId column_id(size_t index) const { return _col_ids[index]; }
    int column_index(ColumnId cid) const { return _column_id_to_index[cid]; }
    const std::vector<int>& column_id_to_index() const { return _column_id_to_index; }
    int32_t delete_sign_idx() const { return _delete_sign_idx; }
    bool has_sequence_col() const { return _has_sequence_col; }
    int32_t rowid_col_idx() const { return _rowid_col_idx; }
    int32_t version_col_idx() const { return _version_col_idx; }
    int32_t lsn_col_idx() const { return _lsn_col_idx; }
    int32_t op_col_idx() const { return _op_col_idx; }
    int32_t tso_col_idx() const { return _tso_col_idx; }
    int32_t commit_tso_col_idx() const { return _commit_tso_col_idx; }
    // Don't use.
    // TODO: memory size of Schema cannot be accurately tracked.
    // In some places, temporarily use num_columns() as Schema size.
    int64_t mem_size() const { return _mem_size; }

private:
    void _init(const std::vector<TabletColumnPtr>& cols, const std::vector<ColumnId>& col_ids,
               size_t num_key_columns);

    void _copy_from(const Schema& other);

    // NOTE: The ColumnId here represents the sequential index number (starting from 0) of
    // a column in current row, not the unique id-identifier of each column
    std::vector<ColumnId> _col_ids;
    // NOTE: _cols[cid] can only be accessed when the cid is
    // contained in _col_ids
    std::vector<TabletColumnPtr> _cols;
    // Tablet column id -> slot index in this Schema.
    std::vector<int> _column_id_to_index;

    size_t _num_key_columns;
    int32_t _delete_sign_idx = -1;
    bool _has_sequence_col = false;
    int32_t _rowid_col_idx = -1;
    int32_t _version_col_idx = -1;
    int32_t _lsn_col_idx = -1;
    int32_t _op_col_idx = -1;
    int32_t _tso_col_idx = -1;
    int32_t _commit_tso_col_idx = -1;
    int64_t _mem_size = 0;
};

} // namespace doris
