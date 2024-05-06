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

#include "common/compiler_util.h" // IWYU pragma: keep
#include "olap/accept_null_predicate.h"
#include "olap/column_predicate.h"
#include "olap/predicate_creator.h"

namespace doris::vectorized {

Status RuntimePredicate::init(PrimitiveType type, bool nulls_first, bool is_asc,
                              const std::string& col_name) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);

    if (_inited) {
        return Status::OK();
    }

    _nulls_first = nulls_first;
    _is_asc = is_asc;
    // For ASC  sort, create runtime predicate col_name <= max_top_value
    // since values that > min_top_value are large than any value in current topn values
    // For DESC sort, create runtime predicate col_name >= min_top_value
    // since values that < min_top_value are less than any value in current topn values
    _pred_constructor = is_asc ? create_comparison_predicate<PredicateType::LE>
                               : create_comparison_predicate<PredicateType::GE>;
    _col_name = col_name;

    // set get value function
    switch (type) {
    case PrimitiveType::TYPE_BOOLEAN: {
        _get_value_fn = get_normal_value<TYPE_BOOLEAN>;
        break;
    }
    case PrimitiveType::TYPE_TINYINT: {
        _get_value_fn = get_normal_value<TYPE_TINYINT>;
        break;
    }
    case PrimitiveType::TYPE_SMALLINT: {
        _get_value_fn = get_normal_value<TYPE_SMALLINT>;
        break;
    }
    case PrimitiveType::TYPE_INT: {
        _get_value_fn = get_normal_value<TYPE_INT>;
        break;
    }
    case PrimitiveType::TYPE_BIGINT: {
        _get_value_fn = get_normal_value<TYPE_BIGINT>;
        break;
    }
    case PrimitiveType::TYPE_LARGEINT: {
        _get_value_fn = get_normal_value<TYPE_LARGEINT>;
        break;
    }
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING: {
        _get_value_fn = [](const Field& field) { return field.get<String>(); };
        break;
    }
    case PrimitiveType::TYPE_DATEV2: {
        _get_value_fn = get_normal_value<TYPE_DATEV2>;
        break;
    }
    case PrimitiveType::TYPE_DATETIMEV2: {
        _get_value_fn = get_normal_value<TYPE_DATETIMEV2>;
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
        _get_value_fn = get_decimal_value<TYPE_DECIMAL32>;
        break;
    }
    case PrimitiveType::TYPE_DECIMAL64: {
        _get_value_fn = get_decimal_value<TYPE_DECIMAL64>;
        break;
    }
    case PrimitiveType::TYPE_DECIMALV2: {
        _get_value_fn = get_decimalv2_value;
        break;
    }
    case PrimitiveType::TYPE_DECIMAL128I: {
        _get_value_fn = get_decimal_value<TYPE_DECIMAL128I>;
        break;
    }
    case PrimitiveType::TYPE_DECIMAL256: {
        _get_value_fn = get_decimal_value<TYPE_DECIMAL256>;
        break;
    }
    case PrimitiveType::TYPE_IPV4: {
        _get_value_fn = get_normal_value<TYPE_IPV4>;
        break;
    }
    case PrimitiveType::TYPE_IPV6: {
        _get_value_fn = get_normal_value<TYPE_IPV6>;
        break;
    }
    default:
        return Status::InvalidArgument("unsupported runtime predicate type {}", type);
    }

    _inited = true;
    return Status::OK();
}

Status RuntimePredicate::update(const Field& value) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    // skip null value
    if (value.is_null() || !_inited) {
        return Status::OK();
    }

    bool updated = false;

    if (UNLIKELY(_orderby_extrem.is_null())) {
        _orderby_extrem = value;
        updated = true;
    } else {
        if ((_is_asc && value < _orderby_extrem) || (!_is_asc && value > _orderby_extrem)) {
            _orderby_extrem = value;
            updated = true;
        }
    }

    _has_value = true;

    if (!updated || !_tablet_schema) {
        return Status::OK();
    }

    std::unique_ptr<ColumnPredicate> pred {
            _pred_constructor(_tablet_schema->column(_col_name), _predicate->column_id(),
                              _get_value_fn(_orderby_extrem), false, &_predicate_arena)};
    // For NULLS FIRST, wrap a AcceptNullPredicate to return true for NULL
    // since ORDER BY ASC/DESC should get NULL first but pred returns NULL
    // and NULL in where predicate will be treated as FALSE
    if (_nulls_first) {
        pred = AcceptNullPredicate::create_unique(pred.release());
    }

    ((SharedPredicate*)_predicate.get())->set_nested(pred.release());

    return Status::OK();
}

} // namespace doris::vectorized
