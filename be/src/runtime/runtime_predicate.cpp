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

#include "runtime/runtime_predicate.h"

#include <stdint.h>

#include <memory>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "olap/accept_null_predicate.h"
#include "olap/column_predicate.h"
#include "olap/predicate_creator.h"

namespace doris {

namespace vectorized {

Status RuntimePredicate::init(const PrimitiveType type, const bool nulls_first) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);

    if (_inited) {
        return Status::OK();
    }

    _nulls_first = nulls_first;

    _predicate_arena.reset(new Arena());

    // set get value function
    switch (type) {
    case PrimitiveType::TYPE_BOOLEAN: {
        _get_value_fn = get_bool_value;
        break;
    }
    case PrimitiveType::TYPE_TINYINT: {
        _get_value_fn = get_tinyint_value;
        break;
    }
    case PrimitiveType::TYPE_SMALLINT: {
        _get_value_fn = get_smallint_value;
        break;
    }
    case PrimitiveType::TYPE_INT: {
        _get_value_fn = get_int_value;
        break;
    }
    case PrimitiveType::TYPE_BIGINT: {
        _get_value_fn = get_bigint_value;
        break;
    }
    case PrimitiveType::TYPE_LARGEINT: {
        _get_value_fn = get_largeint_value;
        break;
    }
    case PrimitiveType::TYPE_FLOAT: {
        _get_value_fn = get_float_value;
        break;
    }
    case PrimitiveType::TYPE_DOUBLE: {
        _get_value_fn = get_double_value;
        break;
    }
    case PrimitiveType::TYPE_STRING: {
        _get_value_fn = get_string_value;
        break;
    }
    case PrimitiveType::TYPE_DATEV2: {
        _get_value_fn = get_datev2_value;
        break;
    }
    case PrimitiveType::TYPE_DATETIMEV2: {
        _get_value_fn = get_datetimev2_value;
        break;
    }
    case PrimitiveType::TYPE_DATE: {
        _get_value_fn = get_date_value;
        break;
    }
    case PrimitiveType::TYPE_DATETIME: {
        _get_value_fn = get_datetime_value;
        break;
    }
    case PrimitiveType::TYPE_DECIMAL32: {
        _get_value_fn = get_decimal32_value;
        break;
    }
    case PrimitiveType::TYPE_DECIMAL64: {
        _get_value_fn = get_decimal64_value;
        break;
    }
    case PrimitiveType::TYPE_DECIMALV2: {
        _get_value_fn = get_decimalv2_value;
        break;
    }
    case PrimitiveType::TYPE_DECIMAL128I: {
        _get_value_fn = get_decimal128_value;
        break;
    }
    case PrimitiveType::TYPE_DECIMAL256: {
        _get_value_fn = get_decimal256_value;
        break;
    }
    default:
        return Status::InvalidArgument("unsupported runtime predicate type {}", type);
    }

    _inited = true;
    return Status::OK();
}

Status RuntimePredicate::update(const Field& value, const String& col_name, bool is_reverse) {
    // skip null value
    if (value.is_null()) {
        return Status::OK();
    }

    if (!_inited) {
        return Status::OK();
    }

    std::unique_lock<std::shared_mutex> wlock(_rwlock);

    // TODO why null
    if (!_tablet_schema) {
        return Status::OK();
    }

    bool updated = false;

    if (UNLIKELY(_orderby_extrem.is_null())) {
        _orderby_extrem = value;
        updated = true;
    } else if (is_reverse) {
        if (value > _orderby_extrem) {
            _orderby_extrem = value;
            updated = true;
        }
    } else {
        if (value < _orderby_extrem) {
            _orderby_extrem = value;
            updated = true;
        }
    }

    if (!updated) {
        return Status::OK();
    }

    // update _predictate
    int32_t col_unique_id = _tablet_schema->column(col_name).unique_id();
    const TabletColumn& column = _tablet_schema->column_by_uid(col_unique_id);
    uint32_t index = _tablet_schema->field_index(col_unique_id);
    auto val = _get_value_fn(_orderby_extrem);
    std::unique_ptr<ColumnPredicate> pred {nullptr};
    if (is_reverse) {
        // For DESC sort, create runtime predicate col_name >= min_top_value
        // since values that < min_top_value are less than any value in current topn values
        pred.reset(create_comparison_predicate<PredicateType::GE>(column, index, val, false,
                                                                  _predicate_arena.get()));
    } else {
        // For ASC  sort, create runtime predicate col_name <= max_top_value
        // since values that > min_top_value are large than any value in current topn values
        pred.reset(create_comparison_predicate<PredicateType::LE>(column, index, val, false,
                                                                  _predicate_arena.get()));
    }

    // For NULLS FIRST, wrap a AcceptNullPredicate to return true for NULL
    // since ORDER BY ASC/DESC should get NULL first but pred returns NULL
    // and NULL in where predicate will be treated as FALSE
    if (_nulls_first) {
        pred = AcceptNullPredicate::create_unique(pred.release());
    }
    _predictate.reset(pred.release());

    return Status::OK();
}

} // namespace vectorized
} // namespace doris
