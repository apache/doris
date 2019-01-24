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

#ifndef DORIS_BE_SRC_OLAP_SCHEMA_H
#define DORIS_BE_SRC_OLAP_SCHEMA_H

#include <vector>

#include "olap/aggregate_func.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/descriptors.h"

namespace doris {

class ColumnSchema {
public:
    ColumnSchema(const FieldAggregationMethod& agg, const FieldType& type) {
        _type_info = get_type_info(type);
        _aggregate_func = get_aggregate_func(agg, type);
        _finalize_func = get_finalize_func(agg, type);
        _size = _type_info->size();
        _col_offset = 0;
    }

    bool is_null(const char* row) const {
        bool is_null = *reinterpret_cast<const bool*>(row + _col_offset);
        return is_null;
    }

    void set_null(char* row) const {
        *reinterpret_cast<bool*>(row + _col_offset) = true;
    }

    void set_not_null(char* row) const {
        *reinterpret_cast<bool*>(row + _col_offset) = false;
    }

    void set_col_offset(int offset) {
        _col_offset = offset;
    }

    int get_col_offset() const {
        return _col_offset;
    }

    int compare(const char* left, const char* right) const {
        bool l_null = *reinterpret_cast<const bool*>(left + _col_offset);
        bool r_null = *reinterpret_cast<const bool*>(right + _col_offset);
        if (l_null != r_null) {
            return l_null ? -1 : 1;
        } else {
            return l_null ? 0 : (_type_info->cmp(left + _col_offset + 1, right + _col_offset + 1));
        }
    }

    void aggregate(char* left, const char* right, Arena* arena) const {
        _aggregate_func(left + _col_offset, right + _col_offset, arena);
    }

    void finalize(char* data) const {
        // data of Hyperloglog type will call this function.
        _finalize_func(data + _col_offset + 1);
    }

    int size() const {
        return _size;
    }
private:
    FieldType _type;
    TypeInfo* _type_info;
    AggregateFunc _aggregate_func;
    FinalizeFunc _finalize_func;
    int _size;
    int _col_offset;
};

class Schema {
public:
    Schema(const TabletSchema& schema) {
        int offset = 0;
        _num_key_columns = 0;
        for (int i = 0; i < schema.num_columns(); ++i) {
            const TabletColumn& column = schema.column(i);
            ColumnSchema col_schema(column.aggregation(), column.type());
            col_schema.set_col_offset(offset);
            offset += col_schema.size() + 1; // 1 for null byte
            if (column.is_key()) {
                _num_key_columns++;
            }
            if (column.type() == OLAP_FIELD_TYPE_HLL) {
                _hll_col_ids.push_back(i);
            }
            _cols.push_back(col_schema);
        }
    }

    int compare(const char* left , const char* right) const {
        for (size_t i = 0; i < _num_key_columns; ++i) {
            int comp = _cols[i].compare(left, right);
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }

    void aggregate(const char* left, const char* right, Arena* arena) const {
        for (size_t i = _num_key_columns; i < _cols.size(); ++i) {
            _cols[i].aggregate(const_cast<char*>(left), right, arena);
        }
    }

    void finalize(const char* data) const {
        for (int col_id : _hll_col_ids) {
            _cols[col_id].finalize(const_cast<char*>(data));
        }
    }

    int get_col_offset(int index) const {
        return _cols[index].get_col_offset();
    }
    size_t get_col_size(int index) const {
        return _cols[index].size();
    }

    bool is_null(int index, const char* row) const {
        return _cols[index].is_null(row);
    }

    void set_null(int index, char*row) {
        _cols[index].set_null(row);
    }

    void set_not_null(int index, char*row) {
        _cols[index].set_not_null(row);
    }

    size_t schema_size() {
        size_t size = _cols.size();
        for (auto col : _cols) {
            size += col.size();
        }
        return size;
    }
private:
    std::vector<ColumnSchema> _cols;
    size_t _num_key_columns;
    std::vector<int> _hll_col_ids;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_SCHEMA_H
