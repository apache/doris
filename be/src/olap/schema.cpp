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

#include "olap/schema.h"

#include "olap/row_block2.h"

namespace doris {

Schema::Schema(const Schema& other) {
    copy_from(other);
}

Schema& Schema::operator=(const Schema& other) {
    if (this != &other) {
        copy_from(other);
    }
    return *this;
}

void Schema::copy_from(const Schema& other) {
    _num_key_columns = other._num_key_columns;
    _col_ids = other._col_ids;
    _col_offsets = other._col_offsets;
    _cols.resize(other._cols.size(), nullptr);
    for (auto cid : _col_ids) {
        _cols[cid] =  other._cols[cid]->clone();
    }
}

void Schema::init_field(const std::vector<TabletColumn>& columns,
                        const std::vector<ColumnId>& col_ids) {
    _cols.resize(columns.size(), nullptr);
    // we must make sure that the offset is the same with RowBlock's
    std::unordered_set<uint32_t> column_set(col_ids.begin(), col_ids.end());
    for (int cid = 0; cid < columns.size(); ++cid) {
        if (column_set.find(cid) == column_set.end()) {
            continue;
        }
        _cols[cid] = FieldFactory::create(columns[cid]);
    }
}

void Schema::init_field(const std::vector<const Field*>& cols,
                const std::vector<ColumnId>& col_ids) {
    _cols.resize(cols.size(), nullptr);
    // we must make sure that the offset is the same with RowBlock's
    std::unordered_set<uint32_t> column_set(col_ids.begin(), col_ids.end());
    for (int cid = 0; cid < cols.size(); ++cid) {
        if (column_set.find(cid) == column_set.end()) {
            continue;
        }
        _cols[cid] = cols[cid]->clone();
    }
}

void Schema::init(const std::vector<ColumnId>& col_ids,
                   size_t num_key_columns) {
    _num_key_columns = num_key_columns;
    _col_ids = col_ids;
    _col_offsets.resize(_cols.size(), -1);

    // we must make sure that the offset is the same with RowBlock's
    std::unordered_set<uint32_t> column_set(_col_ids.begin(), _col_ids.end());
    size_t offset = 0;
    for (int cid = 0; cid < _cols.size(); ++cid) {
        if (column_set.find(cid) == column_set.end()) {
            continue;
        }

        _col_offsets[cid] = offset;
        // 1 for null byte
        offset += _cols[cid]->size() + 1;
    }
    _schema_size = offset;
}

Schema::~Schema() {
    for (auto col : _cols) {
        delete col;
    }
}

}
