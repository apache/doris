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
    _num_key_columns = other._num_key_columns;
    _delete_sign_idx = other._delete_sign_idx;
    _has_sequence_col = other._has_sequence_col;
    _rowid_col_idx = other._rowid_col_idx;
    _version_col_idx = other._version_col_idx;
    _lsn_col_idx = other._lsn_col_idx;
    _tso_col_idx = other._tso_col_idx;

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

IColumn::MutablePtr Schema::get_predicate_column_ptr(const DataTypePtr& data_type,
                                                     const ReaderType reader_type) {
    // Low-cardinality dictionary optimization substitutes a ColumnDictI32 for the
    // canonical string column during query reads. Every other case just materializes
    // the data type's own canonical column (which already wraps nullable for us).
    if (config::enable_low_cardinality_optimize && reader_type == ReaderType::READER_QUERY &&
        is_string_type(data_type->get_primitive_type())) {
        IColumn::MutablePtr ptr = doris::ColumnDictI32::create();
        if (data_type->is_nullable()) {
            return doris::ColumnNullable::create(std::move(ptr), doris::ColumnUInt8::create());
        }
        return ptr;
    }
    return data_type->create_column();
}

} // namespace doris
