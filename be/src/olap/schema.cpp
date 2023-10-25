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

#include <glog/logging.h>

#include <boost/iterator/iterator_facade.hpp>
#include <ostream>
#include <unordered_set>
#include <utility>

#include "common/config.h"
#include "runtime/define_primitive_type.h"
#include "util/trace.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_dictionary.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/columns_number.h"
#include "vec/columns/predicate_column.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
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

vectorized::IColumn::MutablePtr Schema::get_column_by_field(const Field& field) {
    return get_data_type_ptr(field)->create_column();
}

vectorized::IColumn::MutablePtr Schema::get_predicate_column_ptr(const Field& field,
                                                                 const ReaderType reader_type) {
    vectorized::IColumn::MutablePtr ptr = nullptr;
    switch (field.type()) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        ptr = doris::vectorized::PredicateColumnType<TYPE_BOOLEAN>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        ptr = doris::vectorized::PredicateColumnType<TYPE_TINYINT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        ptr = doris::vectorized::PredicateColumnType<TYPE_SMALLINT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_INT:
        ptr = doris::vectorized::PredicateColumnType<TYPE_INT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        ptr = doris::vectorized::PredicateColumnType<TYPE_FLOAT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        ptr = doris::vectorized::PredicateColumnType<TYPE_DOUBLE>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        ptr = doris::vectorized::PredicateColumnType<TYPE_BIGINT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        ptr = doris::vectorized::PredicateColumnType<TYPE_LARGEINT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATE:
        ptr = doris::vectorized::PredicateColumnType<TYPE_DATE>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        ptr = doris::vectorized::PredicateColumnType<TYPE_DATEV2>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        ptr = doris::vectorized::PredicateColumnType<TYPE_DATETIMEV2>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        ptr = doris::vectorized::PredicateColumnType<TYPE_DATETIME>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_CHAR:
        if (config::enable_low_cardinality_optimize && reader_type == ReaderType::READER_QUERY) {
            ptr = doris::vectorized::ColumnDictionary<doris::vectorized::Int32>::create(
                    field.type());
        } else {
            ptr = doris::vectorized::PredicateColumnType<TYPE_CHAR>::create();
        }
        break;
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING:
        if (config::enable_low_cardinality_optimize && reader_type == ReaderType::READER_QUERY) {
            ptr = doris::vectorized::ColumnDictionary<doris::vectorized::Int32>::create(
                    field.type());
        } else {
            ptr = doris::vectorized::PredicateColumnType<TYPE_STRING>::create();
        }
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        ptr = doris::vectorized::PredicateColumnType<TYPE_DECIMALV2>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        ptr = doris::vectorized::PredicateColumnType<TYPE_DECIMAL32>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        ptr = doris::vectorized::PredicateColumnType<TYPE_DECIMAL64>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        ptr = doris::vectorized::PredicateColumnType<TYPE_DECIMAL128I>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        ptr = doris::vectorized::PredicateColumnType<TYPE_DECIMAL256>::create();
        break;
    default:
        LOG(FATAL) << "Unexpected type when choosing predicate column, type=" << int(field.type());
    }

    if (field.is_nullable()) {
        return doris::vectorized::ColumnNullable::create(std::move(ptr),
                                                         doris::vectorized::ColumnUInt8::create());
    }
    return ptr;
}

} // namespace doris
