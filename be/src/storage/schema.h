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
#include <utility>
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
using ReadSchemaSPtr = std::shared_ptr<ReadSchema>;

// Select columns by their ordinal in `columns`, preserving the requested order and duplicates.
// Keeping this source-layout operation outside ReadSchema prevents those ordinals from being
// confused with the dense ordinals of the resulting ReadSchema.
std::vector<TabletColumnPtr> project_columns_by_ordinal(
        const std::vector<TabletColumnPtr>& columns,
        const std::vector<ColumnId>& source_column_ordinals);

// Resolve a physical TabletSchema column id to its dense ReadSchema ordinal. Returns -1 when the
// physical column is absent or not projected.
int32_t read_ordinal_by_tablet_cid(const ReadSchema& read_schema, const TabletSchema& tablet_schema,
                                   int32_t tablet_cid);

// The dense, ordered column layout consumed by one storage reader. For example, if the caller's
// Block is [k1, v1] and a historical delete predicate needs dropped_v2, the layout is
// [k1, v1, dropped_v2]: num_block_columns() is 2, num_read_columns() is 3, and every reader-side
// ColumnId is an ordinal in this list. TabletColumn keeps physical storage metadata, while
// data_type() records the expected materialized type, such as a pruned STRUCT type.
class ReadSchema {
public:
    using SequenceMap = std::unordered_map<ColumnId, std::vector<ColumnId>>;
    using RowBinlogValueColumnPairs = std::vector<std::pair<ColumnId, ColumnId>>;

    explicit ReadSchema(std::vector<TabletColumnPtr> columns);

    // Every column is initially caller-visible. Historical delete-predicate columns may be
    // appended later without changing num_block_columns().
    explicit ReadSchema(std::vector<TabletColumnPtr> columns, std::vector<DataTypePtr> read_types);

    static IColumn::MutablePtr get_predicate_column_ptr(const DataTypePtr& data_type,
                                                        const ReaderType reader_type);

    const std::vector<TabletColumnPtr>& columns() const { return _read_columns; }

    DataTypePtr data_type(size_t ordinal) const {
        DCHECK_LT(ordinal, _read_types.size());
        return _read_types[ordinal];
    }

    // Append private historical delete-predicate columns without extending the Block prefix.
    void append_dropped_columns(std::vector<TabletColumn> columns);

    // Create caller-visible Blocks from the FE-slot prefix.
    Block create_read_block() const;

    std::string read_columns_to_string() const;

    Status init_sequence_map(const TabletSchema& tablet_schema);

    const SequenceMap& sequence_map() const { return _sequence_map; }

    // Initialize row-binlog relationships using dense ordinals in this ReadSchema. Special
    // ordinals are -1 when absent.
    Status init_row_binlog_column_mappings(RowBinlogValueColumnPairs value_pairs,
                                           int32_t tso_ordinal, int32_t lsn_ordinal,
                                           int32_t op_ordinal);

    // Return the matching before-image ordinal for a Row Binlog value column. For example, in
    // [v1, v2, __DORIS_BEFORE__v1__, __DORIS_BEFORE__v2__], 0 maps to 2 and 1 maps to 3.
    // Columns without a before image, including TSO/LSN/OP, map to themselves.
    ColumnId before_column_ordinal(ColumnId ordinal) const {
        DCHECK_LT(ordinal, _before_column_ordinals.size());
        return _before_column_ordinals[ordinal];
    }

    const RowBinlogValueColumnPairs& row_binlog_value_column_pairs() const {
        return _row_binlog_value_column_pairs;
    }

    bool row_binlog_value_pairs_complete() const { return _row_binlog_value_pairs_complete; }

    const TabletColumn* column(size_t ordinal) const { return _read_columns[ordinal].get(); }

    // Total columns used inside storage, including appended storage-only columns.
    // Use this for per-column state and iteration over the complete ReadSchema.
    size_t num_read_columns() const { return _read_columns.size(); }

    // Columns materialized in caller Blocks. They are the ReadSchema prefix before
    // appended storage-only columns; use this for Block layout and position bounds.
    size_t num_block_columns() const { return _num_block_columns; }

    // Number of key columns present in the caller-visible ReadSchema. This is deliberately not the
    // full TabletSchema key count. Merge readers additionally require these columns to be the full,
    // leading storage-key prefix; direct/projected readers may contain fewer key columns.
    size_t num_key_columns() const { return _num_key_columns; }

    // All special-column ordinals below address the caller-visible Block prefix and are -1 when
    // absent. A Row Binlog layout may be [k1, v1, __DORIS_BEFORE__v1__, TSO, LSN, OP]; a snapshot layout
    // may instead contain COMMIT_TSO.
    // Logical-delete marker used by unique-key reads.
    int32_t delete_sign_ordinal() const { return _delete_sign_ordinal; }
    // Sequence column used to choose the winning row during merge.
    int32_t sequence_ordinal() const { return _sequence_ordinal; }
    // Synthetic row identifier returned by rowid-producing scans.
    int32_t rowid_ordinal() const { return _rowid_ordinal; }
    // Rowset version synthesized for single-version reads.
    int32_t version_ordinal() const { return _version_ordinal; }
    // Row Binlog transaction timestamp.
    int32_t tso_ordinal() const { return _tso_ordinal; }
    // Row Binlog log-sequence number.
    int32_t lsn_ordinal() const { return _lsn_ordinal; }
    // Row Binlog operation kind, such as INSERT, UPDATE, or DELETE.
    int32_t op_ordinal() const { return _op_ordinal; }
    // Snapshot commit timestamp.
    int32_t commit_tso_ordinal() const { return _commit_tso_ordinal; }

    // -1 if no column with this unique id is present.
    int32_t ordinal_by_uid(int32_t unique_id) const {
        auto it = _uid_to_ordinal.find(unique_id);
        return it == _uid_to_ordinal.end() ? -1 : it->second;
    }

private:
    void _init_read_types();

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
        _before_column_ordinals.resize(_num_block_columns);
        _uid_to_ordinal.clear();
        for (uint32_t i = 0; i < _read_columns.size(); ++i) {
            const auto& col = *_read_columns[i];
            if (col.unique_id() >= 0) {
                _uid_to_ordinal.emplace(col.unique_id(), i);
            }
        }
        for (uint32_t i = 0; i < _num_block_columns; ++i) {
            const auto& col = *_read_columns[i];
            _before_column_ordinals[i] = i;
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
            if (col.name() == COMMIT_TSO_COL) {
                _commit_tso_ordinal = i;
            }
        }
    }

    // Example: storage has k(uid=1, INT) and
    // s(uid=2, STRUCT<a:INT,b:STRING,c:BIGINT>). The scan needs k and only s.a/s.c, while
    // a historical delete predicate `old_v = 0` needs the dropped column old_v(uid=3, INT):
    //   [0] _read_columns: k:INT (uid=1)
    //       _read_types:   INT
    //   [1] _read_columns: s:STRUCT<a:INT,b:STRING,c:BIGINT> (uid=2)
    //       _read_types:   STRUCT<a:INT,c:BIGINT>
    //   [2] _read_columns: old_v:INT (uid=3, dropped)
    //       _read_types:   INT
    // `_num_block_columns` is 2, so create_read_block() materializes ordinals [0, 2). Ordinal 2
    // is read only for delete filtering and never appears in the caller Block.
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
    RowBinlogValueColumnPairs _row_binlog_value_column_pairs;
    bool _row_binlog_value_pairs_complete = false;
};

} // namespace doris
