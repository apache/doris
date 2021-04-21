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
    _copy_from(other);
}

Schema& Schema::operator=(const Schema& other) {
    if (this != &other) {
        _copy_from(other);
    }
    return *this;
}

void Schema::_copy_from(const Schema& other) {
    _col_ids = other._col_ids;
    _col_offsets = other._col_offsets;

    _num_key_columns = other._num_key_columns;
    _schema_size = other._schema_size;

    // Deep copy _cols
    // TODO(lingbin): really need clone?
    _cols.resize(other._cols.size(), nullptr);
    for (auto cid : _col_ids) {
        _cols[cid] = other._cols[cid]->clone();
    }
}

void Schema::_init(const std::vector<TabletColumn>& cols, const std::vector<ColumnId>& col_ids,
                   size_t num_key_columns) {
    _col_ids = col_ids;
    _num_key_columns = num_key_columns;

    _cols.resize(cols.size(), nullptr);
    _col_offsets.resize(_cols.size(), -1);

    size_t offset = 0;
    std::unordered_set<uint32_t> col_id_set(col_ids.begin(), col_ids.end());
    for (int cid = 0; cid < cols.size(); ++cid) {
        if (col_id_set.find(cid) == col_id_set.end()) {
            continue;
        }
        _cols[cid] = FieldFactory::create(cols[cid]);

        _col_offsets[cid] = offset;
        // Plus 1 byte for null byte
        offset += _cols[cid]->size() + 1;
    }

    _schema_size = offset;
}

void Schema::_init(const std::vector<const Field*>& cols, const std::vector<ColumnId>& col_ids,
                   size_t num_key_columns) {
    _col_ids = col_ids;
    _num_key_columns = num_key_columns;

    _cols.resize(cols.size(), nullptr);
    _col_offsets.resize(_cols.size(), -1);

    size_t offset = 0;
    std::unordered_set<uint32_t> col_id_set(col_ids.begin(), col_ids.end());
    for (int cid = 0; cid < cols.size(); ++cid) {
        if (col_id_set.find(cid) == col_id_set.end()) {
            continue;
        }
        // TODO(lingbin): is it necessary to clone Field? each SegmentIterator will
        // use this func, can we avoid clone?
        _cols[cid] = cols[cid]->clone();

        _col_offsets[cid] = offset;
        // Plus 1 byte for null byte
        offset += _cols[cid]->size() + 1;
    }

    _schema_size = offset;
}

Schema::~Schema() {
    for (auto col : _cols) {
        delete col;
    }
}

} // namespace doris
