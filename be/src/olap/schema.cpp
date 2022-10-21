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
#include "olap/uint24.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/predicate_column.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_factory.hpp"

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

vectorized::DataTypePtr Schema::get_data_type_ptr(const Field& field) {
    return vectorized::DataTypeFactory::instance().create_data_type(field);
}

vectorized::IColumn::MutablePtr Schema::get_predicate_column_nullable_ptr(const Field& field) {
    if (UNLIKELY(field.type() == OLAP_FIELD_TYPE_ARRAY)) {
        return get_data_type_ptr(field)->create_column();
    }

    vectorized::IColumn::MutablePtr ptr = Schema::get_predicate_column_ptr(field.type());
    if (field.is_nullable()) {
        return doris::vectorized::ColumnNullable::create(std::move(ptr),
                                                         doris::vectorized::ColumnUInt8::create());
    }
    return ptr;
}

vectorized::IColumn::MutablePtr Schema::get_predicate_column_ptr(FieldType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_BOOL:
        return doris::vectorized::PredicateColumnType<TYPE_BOOLEAN>::create();

    case OLAP_FIELD_TYPE_TINYINT:
        return doris::vectorized::PredicateColumnType<TYPE_TINYINT>::create();

    case OLAP_FIELD_TYPE_SMALLINT:
        return doris::vectorized::PredicateColumnType<TYPE_SMALLINT>::create();

    case OLAP_FIELD_TYPE_INT:
        return doris::vectorized::PredicateColumnType<TYPE_INT>::create();

    case OLAP_FIELD_TYPE_FLOAT:
        return doris::vectorized::PredicateColumnType<TYPE_FLOAT>::create();

    case OLAP_FIELD_TYPE_DOUBLE:
        return doris::vectorized::PredicateColumnType<TYPE_DOUBLE>::create();

    case OLAP_FIELD_TYPE_BIGINT:
        return doris::vectorized::PredicateColumnType<TYPE_BIGINT>::create();

    case OLAP_FIELD_TYPE_LARGEINT:
        return doris::vectorized::PredicateColumnType<TYPE_LARGEINT>::create();

    case OLAP_FIELD_TYPE_DATE:
        return doris::vectorized::PredicateColumnType<TYPE_DATE>::create();

    case OLAP_FIELD_TYPE_DATEV2:
        return doris::vectorized::PredicateColumnType<TYPE_DATEV2>::create();

    case OLAP_FIELD_TYPE_DATETIMEV2:
        return doris::vectorized::PredicateColumnType<TYPE_DATETIMEV2>::create();

    case OLAP_FIELD_TYPE_DATETIME:
        return doris::vectorized::PredicateColumnType<TYPE_DATETIME>::create();

    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_STRING:
        if (config::enable_low_cardinality_optimize) {
            return doris::vectorized::ColumnDictionary<doris::vectorized::Int32>::create(type);
        }
        return doris::vectorized::PredicateColumnType<TYPE_STRING>::create();

    case OLAP_FIELD_TYPE_DECIMAL:
        return doris::vectorized::PredicateColumnType<TYPE_DECIMALV2>::create();
    case OLAP_FIELD_TYPE_DECIMAL32:
        return doris::vectorized::PredicateColumnType<TYPE_DECIMAL32>::create();
    case OLAP_FIELD_TYPE_DECIMAL64:
        return doris::vectorized::PredicateColumnType<TYPE_DECIMAL64>::create();
    case OLAP_FIELD_TYPE_DECIMAL128:
        return doris::vectorized::PredicateColumnType<TYPE_DECIMAL128>::create();

    default:
        LOG(FATAL) << "Unexpected type when choosing predicate column, type=" << type;
    }
}

} // namespace doris
