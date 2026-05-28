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
#include "core/column/column_decimal.h"
#include "core/column/column_dictionary.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
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
    _unique_ids = other._unique_ids;
    _num_key_columns = other._num_key_columns;
    _delete_sign_idx = other._delete_sign_idx;
    _has_sequence_col = other._has_sequence_col;
    _rowid_col_idx = other._rowid_col_idx;
    _version_col_idx = other._version_col_idx;
    _lsn_col_idx = other._lsn_col_idx;
    _tso_col_idx = other._tso_col_idx;
    _mem_size = other._mem_size;

    _cols.resize(other._cols.size());
    for (auto cid : _col_ids) {
        _cols[cid] = other._cols[cid];
    }
}

void Schema::_init(const std::vector<TabletColumnPtr>& cols, const std::vector<ColumnId>& col_ids,
                   size_t num_key_columns) {
    _col_ids = col_ids;
    _num_key_columns = num_key_columns;

    _cols.resize(cols.size());

    std::unordered_set<uint32_t> col_id_set(col_ids.begin(), col_ids.end());
    for (int cid = 0; cid < cols.size(); ++cid) {
        if (col_id_set.find(cid) == col_id_set.end()) {
            continue;
        }
        _cols[cid] = cols[cid];
    }
}

Schema::~Schema() = default;

DataTypePtr Schema::get_data_type_ptr(const TabletColumn& column) {
    return DataTypeFactory::instance().create_data_type(column);
}

IColumn::MutablePtr Schema::get_predicate_column_ptr(const TabletColumn& column,
                                                     const FieldType& type, bool is_nullable,
                                                     const ReaderType reader_type) {
    IColumn::MutablePtr ptr = nullptr;
    auto decimal_scale = static_cast<UInt32>(column.frac());
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        ptr = doris::ColumnUInt8::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        ptr = doris::ColumnInt8::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        ptr = doris::ColumnInt16::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_INT:
        ptr = doris::ColumnInt32::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        ptr = doris::ColumnFloat32::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        ptr = doris::ColumnFloat64::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        ptr = doris::ColumnInt64::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        ptr = doris::ColumnInt128::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATE:
        ptr = doris::ColumnDate::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        ptr = doris::ColumnDateV2::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        ptr = doris::ColumnDateTimeV2::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        ptr = doris::ColumnDateTime::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        ptr = doris::ColumnTimeStampTz::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_CHAR:
        if (config::enable_low_cardinality_optimize && reader_type == ReaderType::READER_QUERY) {
            ptr = doris::ColumnDictI32::create(type);
        } else {
            ptr = doris::ColumnString::create();
        }
        break;
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING:
    case FieldType::OLAP_FIELD_TYPE_JSONB:
        if (config::enable_low_cardinality_optimize && reader_type == ReaderType::READER_QUERY) {
            ptr = doris::ColumnDictI32::create(type);
        } else {
            ptr = doris::ColumnString::create();
        }
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        ptr = doris::ColumnDecimal128V2::create(0, decimal_scale);
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        ptr = doris::ColumnDecimal32::create(0, decimal_scale);
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        ptr = doris::ColumnDecimal64::create(0, decimal_scale);
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        ptr = doris::ColumnDecimal128V3::create(0, decimal_scale);
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        ptr = doris::ColumnDecimal256::create(0, decimal_scale);
        break;
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        ptr = doris::ColumnIPv4::create();
        break;
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        ptr = doris::ColumnIPv6::create();
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
