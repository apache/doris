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

#ifndef DORIS_BE_SRC_OLAP_TABLET_SCHEMA_H
#define DORIS_BE_SRC_OLAP_TABLET_SCHEMA_H

#include <vector>

#include "runtime/tablet_schema.h"

namespace doris {

OLAPStatus TabletColumnSchema::init_from_pb(const TabletColumnSchema& column) {
    _column_id = column.column_id(); 
    switch (column.type()) {
        case TINYINT:
            _type = OLAP_FIELD_TYPE_TINYINT;
            break;
        case SMALLINT:
            _type = OLAP_FIELD_TYPE_SMALLINT;
            break;
        case INT:
            _type = OLAP_FIELD_TYPE_INT;
            break;
        case BIGINT:
            _type = OLAP_FIELD_TYPE_BIGINT;
            break;
        case LARGEINT:
            _type = OLAP_FIELD_TYPE_LARGEINT;
            break;
        case FLOAT:
            _type = OLAP_FIELD_TYPE_FLOAT;
            break;
        case DOUBLE:
            _type = OLAP_FIELD_TYPE_DOUBLE;
            break;
        case DECIMAL:
            _type = OLAP_FIELD_TYPE_DECIMAL;
            break;
        case CHAR:
            _type = OLAP_FIELD_TYPE_CHAR;
            break;
        case VARCHAR:
            _type = OLAP_FIELD_TYPE_VARCHAR;
            break;
        case HLL:
            _type = OLAP_FIELD_TYPE_HLL;
            break;
        case DATE:
            _type = OLAP_FIELD_TYPE_DATE;
            break;
        case DATETIME:
            _type = OLAP_FIELD_TYPE_DATETIME;
        default:
            _type = OLAP_FIELD_TYPE_UNKNOWN;
            break;
    }
    _type_info = get_type_info(type);
    _is_key = column.is_key();
    _is_nullable = column.is_nullable();
    _is_bf_column = column.is_bf_column();
}

TabletSchema::TabletSchema()
    : _num_columns(0),
      _num_key_columns(0),
      _num_null_columns(0),
      _num_short_key_columns(0) { }

OLAPStatus TabletSchema::init_from_pb(const TabletSchemaPB& schema) {
    for (auto& column : schema.columns()) {
        TabletColumnSchema column_schema;
        column_schema.init_from_pb(column);
        _cols.push_back(column_schema);
        _num_columns++;
        if (column_schema.is_key()) {
            _num_key_columns++;
        }
        if (column_schema.is_nullable()) {
            _num_null_columns++;
        }
    }
}

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_SCHEMA_H
