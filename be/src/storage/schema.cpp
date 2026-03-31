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

#include <glog/logging.h>

#include <boost/iterator/iterator_facade.hpp>
#include <ostream>
#include <unordered_set>
#include <utility>

#include "common/config.h"
#include "core/column/column_array.h"
#include "core/column/column_dictionary.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/predicate_column.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/define_primitive_type.h"
#include "core/types.h"
#include "storage/olap_common.h"
#include "util/trace.h"

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
    _num_key_columns = other._num_key_columns;

    // Deep copy _cols
    // TODO(lingbin): really need clone?
    _cols.resize(other._cols.size(), nullptr);
    for (auto cid : _col_ids) {
        _cols[cid] = other._cols[cid]->clone();
    }
}

void Schema::_init(const std::vector<TabletColumnPtr>& cols, const std::vector<ColumnId>& col_ids,
                   size_t num_key_columns) {
    _col_ids = col_ids;
    _num_key_columns = num_key_columns;

    _cols.resize(cols.size(), nullptr);

    std::unordered_set<uint32_t> col_id_set(col_ids.begin(), col_ids.end());
    for (int cid = 0; cid < cols.size(); ++cid) {
        if (col_id_set.find(cid) == col_id_set.end()) {
            continue;
        }
        _cols[cid] = StorageFieldFactory::create(*cols[cid]);
    }
}

Schema::~Schema() {
    for (auto col : _cols) {
        delete col;
    }
}

DataTypePtr Schema::get_data_type_ptr(const StorageField& field) {
    return DataTypeFactory::instance().create_data_type(field);
}

IColumn::MutablePtr Schema::get_column_by_field(const StorageField& field) {
    return get_data_type_ptr(field)->create_column();
}

IColumn::MutablePtr Schema::get_predicate_column_ptr(const FieldType& type, bool is_nullable,
                                                     const ReaderType reader_type) {
    IColumn::MutablePtr ptr = nullptr;
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        ptr = doris::PredicateColumnType<TYPE_BOOLEAN>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        ptr = doris::PredicateColumnType<TYPE_TINYINT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        ptr = doris::PredicateColumnType<TYPE_SMALLINT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_INT:
        ptr = doris::PredicateColumnType<TYPE_INT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        ptr = doris::PredicateColumnType<TYPE_FLOAT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        ptr = doris::PredicateColumnType<TYPE_DOUBLE>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        ptr = doris::PredicateColumnType<TYPE_BIGINT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        ptr = doris::PredicateColumnType<TYPE_LARGEINT>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATE:
        ptr = doris::PredicateColumnType<TYPE_DATE>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        ptr = doris::PredicateColumnType<TYPE_DATEV2>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        ptr = doris::PredicateColumnType<TYPE_DATETIMEV2>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        ptr = doris::PredicateColumnType<TYPE_DATETIME>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        ptr = doris::PredicateColumnType<TYPE_TIMESTAMPTZ>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_CHAR:
        if (config::enable_low_cardinality_optimize && reader_type == ReaderType::READER_QUERY) {
            ptr = doris::ColumnDictI32::create(type);
        } else {
            ptr = doris::PredicateColumnType<TYPE_CHAR>::create();
        }
        break;
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING:
    case FieldType::OLAP_FIELD_TYPE_JSONB:
        if (config::enable_low_cardinality_optimize && reader_type == ReaderType::READER_QUERY) {
            ptr = doris::ColumnDictI32::create(type);
        } else {
            ptr = doris::PredicateColumnType<TYPE_STRING>::create();
        }
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        ptr = doris::PredicateColumnType<TYPE_DECIMALV2>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        ptr = doris::PredicateColumnType<TYPE_DECIMAL32>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        ptr = doris::PredicateColumnType<TYPE_DECIMAL64>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        ptr = doris::PredicateColumnType<TYPE_DECIMAL128I>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        ptr = doris::PredicateColumnType<TYPE_DECIMAL256>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        ptr = doris::PredicateColumnType<TYPE_IPV4>::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        ptr = doris::PredicateColumnType<TYPE_IPV6>::create();
        break;
    default:
        throw Exception(
                ErrorCode::SCHEMA_SCHEMA_FIELD_INVALID,
                fmt::format("Unexpected type when choosing predicate column, type={}", int(type)));
    }

    if (is_nullable) {
        return doris::ColumnNullable::create(std::move(ptr), doris::ColumnUInt8::create());
    }
    return ptr;
}

} // namespace doris
