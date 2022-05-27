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

vectorized::DataTypePtr Schema::get_data_type_ptr(FieldType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_BOOL:
        return std::make_shared<vectorized::DataTypeUInt8>();

    case OLAP_FIELD_TYPE_TINYINT:
        return std::make_shared<vectorized::DataTypeInt8>();

    case OLAP_FIELD_TYPE_SMALLINT:
        return std::make_shared<vectorized::DataTypeInt16>();

    case OLAP_FIELD_TYPE_INT:
        return std::make_shared<vectorized::DataTypeInt32>();

    case OLAP_FIELD_TYPE_FLOAT:
        return std::make_shared<vectorized::DataTypeFloat32>();

    case OLAP_FIELD_TYPE_BIGINT:
        return std::make_shared<vectorized::DataTypeInt64>();

    case OLAP_FIELD_TYPE_LARGEINT:
        return std::make_shared<vectorized::DataTypeInt128>();

    case OLAP_FIELD_TYPE_DATE:
        return std::make_shared<vectorized::DataTypeDate>();

    case OLAP_FIELD_TYPE_DATETIME:
        return std::make_shared<vectorized::DataTypeDateTime>();

    case OLAP_FIELD_TYPE_DOUBLE:
        return std::make_shared<vectorized::DataTypeFloat64>();

    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_STRING:
        return std::make_shared<vectorized::DataTypeString>();
    case OLAP_FIELD_TYPE_HLL:
        return std::make_shared<vectorized::DataTypeHLL>();
    case OLAP_FIELD_TYPE_OBJECT:
        return std::make_shared<vectorized::DataTypeBitMap>();

    case OLAP_FIELD_TYPE_DECIMAL:
        return std::make_shared<vectorized::DataTypeDecimal<vectorized::Decimal128>>(27, 9);

    default:
        DCHECK(false);
        return nullptr;
    }
}

vectorized::IColumn::MutablePtr Schema::get_predicate_column_nullable_ptr(FieldType type,
                                                                          bool is_null) {
    vectorized::IColumn::MutablePtr ptr = Schema::get_predicate_column_ptr(type);
    if (is_null) {
        return doris::vectorized::ColumnNullable::create(std::move(ptr),
                                                         doris::vectorized::ColumnUInt8::create());
    }
    return ptr;
}

vectorized::IColumn::MutablePtr Schema::get_predicate_column_ptr(FieldType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_BOOL:
        return doris::vectorized::PredicateColumnType<bool>::create();

    case OLAP_FIELD_TYPE_TINYINT:
        return doris::vectorized::PredicateColumnType<doris::vectorized::Int8>::create();

    case OLAP_FIELD_TYPE_SMALLINT:
        return doris::vectorized::PredicateColumnType<doris::vectorized::Int16>::create();

    case OLAP_FIELD_TYPE_INT:
        return doris::vectorized::PredicateColumnType<doris::vectorized::Int32>::create();

    case OLAP_FIELD_TYPE_FLOAT:
        return doris::vectorized::PredicateColumnType<doris::vectorized::Float32>::create();

    case OLAP_FIELD_TYPE_DOUBLE:
        return doris::vectorized::PredicateColumnType<doris::vectorized::Float64>::create();

    case OLAP_FIELD_TYPE_BIGINT:
        return doris::vectorized::PredicateColumnType<doris::vectorized::Int64>::create();

    case OLAP_FIELD_TYPE_LARGEINT:
        return doris::vectorized::PredicateColumnType<doris::vectorized::Int128>::create();

    case OLAP_FIELD_TYPE_DATE:
        return doris::vectorized::PredicateColumnType<uint24_t>::create();

    case OLAP_FIELD_TYPE_DATETIME:
        return doris::vectorized::PredicateColumnType<uint64_t>::create();

    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_STRING:
        if (config::enable_low_cardinality_optimize) {
            return doris::vectorized::ColumnDictionary<doris::vectorized::Int32>::create(type);
        }
        return doris::vectorized::PredicateColumnType<StringValue>::create();

    case OLAP_FIELD_TYPE_DECIMAL:
        return doris::vectorized::PredicateColumnType<decimal12_t>::create();

    default:
        LOG(FATAL) << "Unexpected type when choosing predicate column, type=" << type;
    }
}

} // namespace doris
