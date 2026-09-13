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

#include "storage/schema.h"

#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "core/column/column_dictionary.h"
#include "core/column/column_nothing.h"
#include "core/column/column_nullable.h"
#include "storage/binlog.h"

namespace doris {

namespace {

bool row_binlog_value_columns_have_same_type(const TabletColumn& lhs, const TabletColumn& rhs) {
    if (lhs.type() != rhs.type() || lhs.is_nullable() != rhs.is_nullable() ||
        lhs.length() != rhs.length() || lhs.precision() != rhs.precision() ||
        lhs.frac() != rhs.frac() || lhs.get_subtype_count() != rhs.get_subtype_count()) {
        return false;
    }
    for (uint32_t i = 0; i < lhs.get_subtype_count(); ++i) {
        if (!row_binlog_value_columns_have_same_type(lhs.get_sub_column(i),
                                                     rhs.get_sub_column(i))) {
            return false;
        }
    }
    return true;
}

} // namespace

std::vector<TabletColumnPtr> project_columns_by_ordinal(
        const std::vector<TabletColumnPtr>& columns,
        const std::vector<ColumnId>& source_column_ordinals) {
    std::vector<TabletColumnPtr> projected_columns;
    projected_columns.reserve(source_column_ordinals.size());
    for (auto ordinal : source_column_ordinals) {
        projected_columns.emplace_back(columns[ordinal]);
    }
    return projected_columns;
}

ReadSchema::ReadSchema(std::vector<TabletColumnPtr> columns)
        : _read_columns(std::move(columns)), _num_block_columns(_read_columns.size()) {
    _init_read_types();
    _init_descriptors();
}

ReadSchema::ReadSchema(std::vector<TabletColumnPtr> columns, std::vector<DataTypePtr> read_types)
        : _read_columns(std::move(columns)),
          _read_types(std::move(read_types)),
          _num_block_columns(_read_types.size()) {
    _init_descriptors();
}

void ReadSchema::_init_read_types() {
    _read_types.reserve(_read_columns.size());
    for (const auto& column : _read_columns) {
        auto data_type = column->get_vec_type();
        DORIS_CHECK(data_type != nullptr);
        _read_types.emplace_back(std::move(data_type));
    }
}

void ReadSchema::append_dropped_columns(std::vector<TabletColumn> columns) {
    _read_columns.reserve(_read_columns.size() + columns.size());
    _read_types.reserve(_read_types.size() + columns.size());
    for (auto& column : columns) {
        auto column_ptr = std::make_shared<TabletColumn>(std::move(column));
        auto data_type = column_ptr->get_vec_type();
        auto ordinal = cast_set<ColumnId>(_read_columns.size());
        if (column_ptr->unique_id() >= 0) {
            _uid_to_ordinal.emplace(column_ptr->unique_id(), ordinal);
        }
        _read_columns.emplace_back(std::move(column_ptr));
        _read_types.emplace_back(std::move(data_type));
    }
}

void ReadSchema::_init_before_column_ordinals() {
    std::unordered_map<std::string_view, ColumnId> name_to_ordinal;
    name_to_ordinal.reserve(_num_block_columns);
    for (ColumnId ordinal = 0; ordinal < _num_block_columns; ++ordinal) {
        name_to_ordinal.emplace(_read_columns[ordinal]->name(), ordinal);
    }

    _before_column_ordinals.resize(_num_block_columns);
    for (ColumnId ordinal = 0; ordinal < _num_block_columns; ++ordinal) {
        auto read_ordinal = static_cast<int32_t>(ordinal);
        if (read_ordinal == _tso_ordinal || read_ordinal == _lsn_ordinal ||
            read_ordinal == _op_ordinal) {
            _before_column_ordinals[ordinal] = ordinal;
            continue;
        }
        auto before_name = binlog::build_before_column_name(_read_columns[ordinal]->name());
        auto before = name_to_ordinal.find(before_name);
        _before_column_ordinals[ordinal] =
                before == name_to_ordinal.end() ? ordinal : before->second;
    }
}

void ReadSchema::init_row_binlog_column_mappings(const TabletSchema& tablet_schema) {
    DORIS_CHECK_GE(_op_ordinal, 0);
    DORIS_CHECK_EQ(_before_column_ordinals.size(), _num_block_columns);

    _row_binlog_value_column_pairs.clear();
    _row_binlog_value_pairs_complete = false;

    std::vector<ColumnId> value_column_ids;
    value_column_ids.reserve(tablet_schema.num_columns() - tablet_schema.num_key_columns());
    for (ColumnId cid = 0; cid < tablet_schema.num_columns(); ++cid) {
        if (static_cast<int32_t>(cid) == tablet_schema.binlog_tso_col_idx() ||
            static_cast<int32_t>(cid) == tablet_schema.binlog_lsn_col_idx() ||
            static_cast<int32_t>(cid) == tablet_schema.binlog_op_col_idx() ||
            tablet_schema.column(cid).is_key()) {
            continue;
        }
        value_column_ids.push_back(cid);
    }

    if (value_column_ids.empty() || value_column_ids.size() % 2 != 0) {
        return;
    }

    const size_t value_column_count = value_column_ids.size() / 2;
    for (size_t i = 0; i < value_column_count; ++i) {
        const auto& after = tablet_schema.column(value_column_ids[i]);
        const auto& before = tablet_schema.column(value_column_ids[i + value_column_count]);
        if (before.name() != binlog::build_before_column_name(after.name()) ||
            !row_binlog_value_columns_have_same_type(after, before)) {
            return;
        }
    }

    // A valid physical row-binlog layout starts from an identity mapping. Only AFTER value
    // columns map to their BEFORE companions; keys, metadata and BEFORE columns map to themselves.
    for (ColumnId ordinal = 0; ordinal < _num_block_columns; ++ordinal) {
        _before_column_ordinals[ordinal] = ordinal;
    }

    bool complete = true;
    _row_binlog_value_column_pairs.reserve(value_column_count);
    for (size_t i = 0; i < value_column_count; ++i) {
        const auto after_cid = value_column_ids[i];
        const auto before_cid = value_column_ids[i + value_column_count];
        const int32_t after_ordinal = ordinal_by_uid(tablet_schema.column(after_cid).unique_id());
        const int32_t before_ordinal = ordinal_by_uid(tablet_schema.column(before_cid).unique_id());
        if (after_ordinal < 0 || before_ordinal < 0 ||
            static_cast<size_t>(after_ordinal) >= _num_block_columns ||
            static_cast<size_t>(before_ordinal) >= _num_block_columns) {
            complete = false;
            continue;
        }

        const auto after = cast_set<ColumnId>(after_ordinal);
        const auto before = cast_set<ColumnId>(before_ordinal);
        _before_column_ordinals[after] = before;
        if (!_read_types[after]->equals(*_read_types[before])) {
            complete = false;
            continue;
        }
        _row_binlog_value_column_pairs.emplace_back(after, before);
    }
    _row_binlog_value_pairs_complete =
            complete && _row_binlog_value_column_pairs.size() == value_column_count;
}

Block ReadSchema::create_read_block() const {
    Block block;
    for (size_t ordinal = 0; ordinal < _num_block_columns; ++ordinal) {
        const auto& data_type = _read_types[ordinal];
        DORIS_CHECK(data_type != nullptr);
        MutableColumnPtr column;
        if (_read_columns[ordinal]->name().starts_with(BeConsts::VIRTUAL_COLUMN_PREFIX)) {
            column = ColumnNothing::create(0);
        } else {
            column = data_type->create_column();
        }
        block.insert({std::move(column), data_type, _read_columns[ordinal]->name()});
    }
    return block;
}

std::string ReadSchema::read_columns_to_string() const {
    // Avoid lines that are too long to display in SHOW PROFILE.
    constexpr int columns_per_line = 10;
    int column_index = 0;
    std::string result = "[";
    for (auto it = _read_columns.cbegin(); it != _read_columns.cend(); ++it) {
        if (it != _read_columns.cbegin()) {
            result += ", ";
        }
        result += (*it)->name();
        if (column_index >= columns_per_line) {
            result += "\n";
            column_index = 0;
        } else {
            ++column_index;
        }
    }
    result += "]";
    return result;
}

Status ReadSchema::init_sequence_map(const TabletSchema& tablet_schema) {
    if (tablet_schema.has_sequence_col()) {
        auto msg = "sequence columns conflict, both seq_col and seq_map are true!";
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    _sequence_map.clear();
    for (const auto& [sequence_cid, value_cids] : tablet_schema.seq_col_idx_to_value_cols_idx()) {
        std::vector<ColumnId> value_ordinals;
        for (auto value_cid : value_cids) {
            int32_t value_ordinal = ordinal_by_uid(tablet_schema.column(value_cid).unique_id());
            if (value_ordinal >= 0 && static_cast<size_t>(value_ordinal) < num_block_columns()) {
                value_ordinals.emplace_back(value_ordinal);
            }
        }

        int32_t sequence_ordinal = ordinal_by_uid(tablet_schema.column(sequence_cid).unique_id());
        if (sequence_ordinal < 0 || static_cast<size_t>(sequence_ordinal) >= num_block_columns()) {
            if (value_ordinals.empty()) {
                continue;
            }
            return Status::InvalidArgument(
                    "Sequence column {} must be present in the read Block schema",
                    tablet_schema.column(sequence_cid).name());
        }
        _sequence_map.emplace(sequence_ordinal, std::move(value_ordinals));
    }
    return Status::OK();
}

IColumn::MutablePtr ReadSchema::get_predicate_column_ptr(const DataTypePtr& data_type,
                                                         const ReaderType reader_type) {
    // Low-cardinality dictionary optimization substitutes a ColumnDictI32 for the
    // canonical string column during query reads. Every other case just materializes
    // the data type's own canonical column (which already wraps nullable for us).
    if (config::enable_low_cardinality_optimize && reader_type == ReaderType::READER_QUERY &&
        is_string_type(data_type->get_primitive_type())) {
        IColumn::MutablePtr ptr = doris::ColumnDictI32::create();
        if (data_type->is_nullable()) {
            return doris::ColumnNullable::create(std::move(ptr), doris::ColumnUInt8::create());
        }
        return ptr;
    }
    return data_type->create_column();
}

} // namespace doris
