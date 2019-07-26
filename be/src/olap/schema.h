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

#include <vector>

#include "olap/aggregate_func.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/descriptors.h"

namespace doris {

class RowBlockRow;

class ColumnSchema {
public:
    ColumnSchema(const FieldAggregationMethod& agg, const FieldType& type, bool is_nullable)
        : _type_info(get_type_info(type)),
        _agg_info(get_aggregate_info(agg, type)),
        _is_nullable(is_nullable) {
    }

    int compare(const void* left, const void* right) const {
        return _type_info->cmp(left, right);
    }

    void aggregate(void* left, const void* right, Arena* arena) const {
        _agg_info->update(left, right, arena);
    }

    // data of Hyperloglog type will call this function.
    void finalize(void* data) const {
        // NOTE(zc): Currently skip null byte, this is different with update interface
        // we should unify this
        _agg_info->finalize((char*)data + 1, nullptr);
    }

    int size() const { return _type_info->size(); }
    bool is_nullable() const { return _is_nullable; }

    FieldType type() const { return _type_info->type(); }
    const TypeInfo* type_info() const { return _type_info; }
private:
    const TypeInfo* _type_info;
    const AggregateInfo* _agg_info;
    bool _is_nullable;
};

// The class is used to represent row's format in memory.
// One row contains some columns, within these columns there may be key columns which
// must be the first few columns.
class Schema {
public:
    Schema(const TabletSchema& schema) {
        std::vector<ColumnSchema> cols;
        size_t num_key_columns = 0;
        for (int i = 0; i < schema.num_columns(); ++i) {
            const TabletColumn& column = schema.column(i);
            cols.emplace_back(column.aggregation(), column.type(), column.is_nullable());
            if (column.is_key()) {
                num_key_columns++;
            }
        }

        reset(cols, num_key_columns);
    }

    Schema(const std::vector<ColumnSchema>& cols, size_t num_key_columns) {
        reset(cols, num_key_columns);
    }

    void reset(const std::vector<ColumnSchema>& cols, size_t num_key_columns);

    const std::vector<ColumnSchema>& columns() const { return _cols; }
    const ColumnSchema& column(int idx) const { return _cols[idx]; }

    int compare(const RowBlockRow& lhs, const RowBlockRow& rhs) const;
    int compare(const char* left , const char* right) const {
        for (size_t i = 0; i < _num_key_columns; ++i) {
            auto col_offset = _col_offsets[i];

            bool l_null = *reinterpret_cast<const bool*>(left + col_offset);
            bool r_null = *reinterpret_cast<const bool*>(right + col_offset);
            if (l_null != r_null) {
                return l_null ? -1 : 1;
            } else if (l_null) {
                continue;
            }

            // skip null byte
            col_offset += 1;
            int cmp = _cols[i].compare(left + col_offset, right + col_offset);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    void aggregate(char* left, const char* right, Arena* arena) const {
        for (size_t i = _num_key_columns; i < _cols.size(); ++i) {
            auto col_offset = _col_offsets[i];
            _cols[i].aggregate(left + col_offset, right + col_offset, arena);
        }
    }

    void finalize(char* data) const {
        for (int col_id : _hll_col_ids) {
            auto col_offset = _col_offsets[col_id];
            _cols[col_id].finalize(data + col_offset);
        }
    }

    int get_col_offset(int index) const {
        return _col_offsets[index];
    }

    size_t get_col_size(int index) const {
        return _cols[index].size();
    }

    bool is_null(int index, const char* row) const {
        return *reinterpret_cast<const bool*>(row + _col_offsets[index]);
    }

    void set_null(int index, char* row) const {
        *reinterpret_cast<bool*>(row + _col_offsets[index]) = true;
    }

    void set_not_null(int index, char*row) const {
        *reinterpret_cast<bool*>(row + _col_offsets[index]) = false;
    }

    size_t schema_size() const {
        size_t size = _cols.size();
        for (auto col : _cols) {
            size += col.size();
        }
        return size;
    }

    size_t num_columns() const { return _cols.size(); }
private:
    std::vector<ColumnSchema> _cols;
    size_t _num_key_columns;
    std::vector<int> _col_offsets;
    std::vector<int> _hll_col_ids;
};

} // namespace doris
