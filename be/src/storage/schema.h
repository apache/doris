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
#include <unordered_map>
#include <vector>

#include "common/consts.h"
#include "common/status.h"
#include "core/column/column.h"
#include "exprs/aggregate/aggregate_function.h"
#include "io/io_common.h"
#include "runtime/thread_context.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

namespace doris {

class ReadSchema;
class Block;
using ReadSchemaSPtr = std::shared_ptr<const ReadSchema>;
class ReadSchema {
public:
    using SequenceMap = std::unordered_map<ColumnId, std::vector<ColumnId>>;

    explicit ReadSchema(std::vector<TabletColumnPtr> columns);

    explicit ReadSchema(const std::vector<TabletColumnPtr>& columns,
                        const std::vector<ColumnId>& cids);

    // Initially every column is a caller-visible FE slot. Storage-only columns
    // may be appended later without changing `num_block_columns()`.
    explicit ReadSchema(std::vector<TabletColumnPtr> columns, std::vector<DataTypePtr> read_types);

    static IColumn::MutablePtr get_predicate_column_ptr(const DataTypePtr& data_type,
                                                        const ReaderType reader_type);

    const std::vector<TabletColumnPtr>& columns() const { return _read_columns; }

    DataTypePtr data_type(size_t ordinal) const {
        DCHECK_LT(ordinal, _read_types.size());
        return _read_types[ordinal];
    }

    // Append a storage-only column without extending the caller-visible slot prefix.
    ColumnId append_column(TabletColumnPtr column) {
        auto data_type = column->get_vec_type();
        auto ordinal = cast_set<ColumnId>(_read_columns.size());
        if (column->unique_id() >= 0) {
            _uid_to_ordinal.emplace(column->unique_id(), ordinal);
        }
        _read_columns.emplace_back(std::move(column));
        _read_types.emplace_back(std::move(data_type));
        return ordinal;
    }

    // Create caller-visible Blocks from the FE-slot prefix.
    Block create_read_block() const;

    Status init_sequence_map(const TabletSchema& tablet_schema);

    const SequenceMap& sequence_map() const { return _sequence_map; }

    ColumnId before_column_ordinal(ColumnId ordinal) const {
        DCHECK_LT(ordinal, _before_column_ordinals.size());
        return _before_column_ordinals[ordinal];
    }

    const TabletColumn* column(size_t ordinal) const { return _read_columns[ordinal].get(); }

    // Resolve by unique id when one exists. Name identity is only for legacy
    // columns without a unique id.
    int32_t ordinal_by_column(const TabletColumn& column) const {
        if (column.unique_id() >= 0) {
            return ordinal_by_uid(column.unique_id());
        }
        for (uint32_t ordinal = 0; ordinal < _read_columns.size(); ++ordinal) {
            if (_read_columns[ordinal]->name() == column.name()) {
                return static_cast<int32_t>(ordinal);
            }
        }
        return -1;
    }

    // Total columns used inside storage, including appended storage-only columns.
    // Use this for per-column state and iteration over the complete ReadSchema.
    size_t num_read_columns() const { return _read_columns.size(); }

    // Columns materialized in caller Blocks. They are the ReadSchema prefix before
    // appended storage-only columns; use this for Block layout and position bounds.
    size_t num_block_columns() const { return _num_block_columns; }

    size_t num_key_columns() const { return _num_key_columns; }

    int32_t delete_sign_ordinal() const { return _delete_sign_ordinal; }
    int32_t sequence_ordinal() const { return _sequence_ordinal; }
    int32_t rowid_ordinal() const { return _rowid_ordinal; }
    int32_t version_ordinal() const { return _version_ordinal; }
    int32_t tso_ordinal() const { return _tso_ordinal; }
    int32_t lsn_ordinal() const { return _lsn_ordinal; }
    int32_t op_ordinal() const { return _op_ordinal; }
    int32_t commit_tso_ordinal() const { return _commit_tso_ordinal; }

    // -1 if no column with this unique id is present. Columns without a valid
    // unique id (e.g. variant extracted subcolumns) are not in this map;
    // resolve those with ordinal_by_column().
    int32_t ordinal_by_uid(int32_t unique_id) const {
        auto it = _uid_to_ordinal.find(unique_id);
        return it == _uid_to_ordinal.end() ? -1 : it->second;
    }

private:
    void _init_read_types();
    void _init_before_column_ordinals();

    void _init_descriptors() {
        DORIS_CHECK_LE(_num_block_columns, _read_columns.size());
        DORIS_CHECK_EQ(_read_columns.size(), _read_types.size());
        _num_key_columns = 0;
        _delete_sign_ordinal = -1;
        _sequence_ordinal = -1;
        _rowid_ordinal = -1;
        _version_ordinal = -1;
        _tso_ordinal = -1;
        _lsn_ordinal = -1;
        _op_ordinal = -1;
        _commit_tso_ordinal = -1;
        _before_column_ordinals.clear();
        _uid_to_ordinal.clear();
        for (uint32_t i = 0; i < _read_columns.size(); ++i) {
            const auto& col = *_read_columns[i];
            if (col.unique_id() >= 0) {
                _uid_to_ordinal.emplace(col.unique_id(), i);
            }
        }
        for (uint32_t i = 0; i < _num_block_columns; ++i) {
            const auto& col = *_read_columns[i];
            if (col.is_key()) {
                ++_num_key_columns;
            }
            if (col.name() == DELETE_SIGN) {
                _delete_sign_ordinal = i;
            }
            if (col.name() == SEQUENCE_COL) {
                _sequence_ordinal = i;
            }
            if (col.name().starts_with(BeConsts::GLOBAL_ROWID_COL)) {
                _rowid_ordinal = i;
            }
            if (col.name() == VERSION_COL) {
                _version_ordinal = i;
            }
            if (col.name() == BINLOG_TSO_COL) {
                _tso_ordinal = i;
            }
            if (col.name() == BINLOG_LSN_COL) {
                _lsn_ordinal = i;
            }
            if (col.name() == BINLOG_OP_COL) {
                _op_ordinal = i;
            }
            if (col.name() == COMMIT_TSO_COL) {
                _commit_tso_ordinal = i;
            }
        }
        if (_op_ordinal >= 0) {
            _init_before_column_ordinals();
        }
    }

    // The first `_num_block_columns` entries are the requested read columns. Any
    // remaining entries are dropped columns appended for historical delete predicates.
    std::vector<TabletColumnPtr> _read_columns;
    // Types aligned by ordinal with `_read_columns`.
    std::vector<DataTypePtr> _read_types;
    // Boundary between the columns materialized in Blocks and the appended dropped columns.
    size_t _num_block_columns = 0;

    size_t _num_key_columns = 0;
    int32_t _delete_sign_ordinal = -1;
    int32_t _sequence_ordinal = -1;
    int32_t _rowid_ordinal = -1;
    int32_t _version_ordinal = -1;
    int32_t _tso_ordinal = -1;
    int32_t _lsn_ordinal = -1;
    int32_t _op_ordinal = -1;
    int32_t _commit_tso_ordinal = -1;
    std::unordered_map<int32_t, int32_t> _uid_to_ordinal;
    SequenceMap _sequence_map;
    std::vector<ColumnId> _before_column_ordinals;
};

} // namespace doris
