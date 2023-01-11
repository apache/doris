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

#include "olap/predicate_creator.h"

namespace doris {

namespace vectorized {

Status RuntimePredicate::init(const PrimitiveType type) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);

    if (_inited) {
        return Status::OK();
    }

    _predicate_mem_pool.reset(new MemPool());

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
    default:
        return Status::InvalidArgument("unsupported runtime predicate type {}", type);
    }

    _inited = true;
    return Status::OK();
}

Status RuntimePredicate::update(const Field& value, const String& col_name, bool is_reverse) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);

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

    TCondition condition;
    condition.__set_column_name(col_name);
    condition.__set_column_unique_id(_tablet_schema->column(col_name).unique_id());
    condition.__set_condition_op(is_reverse ? ">=" : "<=");

    // get value string from _orderby_extrem and push back to condition_values
    condition.condition_values.push_back(_get_value_fn(_orderby_extrem));

    // update _predictate
    _predictate.reset(
            parse_to_predicate(_tablet_schema, condition, _predicate_mem_pool.get(), false));

    return Status::OK();
}

} // namespace vectorized
} // namespace doris
