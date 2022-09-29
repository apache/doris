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


namespace doris {

namespace vectorized {

Status RuntimePredicate::update(std::vector<vectorized::Field>& values, const String& col_name,
                const PrimitiveType type, bool is_reverse) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);

    if (!_predicate_mem_pool) {
        _predicate_mem_pool.reset(new MemPool());
    }

    bool updated = false;

    if (UNLIKELY(_orderby_extrems.size() == 0)) {
        _orderby_extrems.push_back(values[0]);
        updated = true;
    } else if (is_reverse) {
        if (values[0] > _orderby_extrems[0]) {
            _orderby_extrems[0] = values[0];
            updated = true;
        }
    } else {
        if (values[0] < _orderby_extrems[0]) {
            _orderby_extrems[0] = values[0];
            updated = true;
        }
    }

    if (!updated)
        return Status::OK();

    TCondition condition;
    condition.__set_column_name(col_name);
    condition.__set_column_unique_id(_tablet_schema->column(col_name).unique_id());
    condition.__set_condition_op(is_reverse ? ">=" : "<=");

    int scale = 0;
    String s;

    switch (type) {
    case TYPE_DATEV2: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATEV2>::CppType;
        // ValueType value;
        // value.from_olap_date(_orderby_extrems[0].get<UInt32>());
        ValueType value =
            binary_cast<UInt32, ValueType>(_orderby_extrems[0].get<UInt32>());
        condition.condition_values.push_back(
            cast_to_string<TYPE_DATEV2, ValueType>(value, scale));
        break;
    }
    case TYPE_DATETIMEV2: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType;
        // ValueType value;
        // value.from_olap_datetime(_orderby_extrems[0].get<UInt64>());
        ValueType value =
            binary_cast<UInt64, ValueType>(_orderby_extrems[0].get<UInt64>());
        condition.condition_values.push_back(
            cast_to_string<TYPE_DATETIMEV2, ValueType>(value, scale));
        break;
    }
    case TYPE_DATE: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATE>::CppType;
        ValueType value;
        Int64 v = _orderby_extrems[0].get<Int64>();
        VecDateTimeValue* p = (VecDateTimeValue*)&v;
        value.from_olap_date(p->to_olap_date());
        value.cast_to_date();
        condition.condition_values.push_back(
            cast_to_string<TYPE_DATE, ValueType>(value, scale));
        break;
    }
    case TYPE_DATETIME: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType;
        ValueType value;
        Int64 v = _orderby_extrems[0].get<Int64>();
        VecDateTimeValue* p = (VecDateTimeValue*)&v;
        value.from_olap_datetime(p->to_olap_datetime());
        value.to_datetime();
        condition.condition_values.push_back(
            cast_to_string<TYPE_DATETIME, ValueType>(value, scale));
        break;
    }
    case TYPE_DECIMALV2: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType;
        ValueType value;
        auto v = _orderby_extrems[0].get<DecimalField<Decimal128>>();
        value.from_olap_decimal(v.get_value(), v.get_scale());
        scale = v.get_scale();
        condition.condition_values.push_back(
            cast_to_string<TYPE_DECIMALV2, ValueType>(value, scale));
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        // using ValueType = typename PrimitiveTypeTraits<TYPE_STRING>::CppType;
        // ValueType value;
        // s = _orderby_extrems[0].get<String>();
        // value.replace((char*)s.c_str(), s.length());
        // condition.condition_values.push_back(
        //     cast_to_string<TYPE_STRING, ValueType>(value, scale));
        condition.condition_values.push_back(_orderby_extrems[0].get<String>());
        break;
    }
    default:
        // value = _orderby_extrems[0].get<ValueType>();
        break;
    }

    _predictate.reset(parse_to_predicate(_tablet_schema, condition, _predicate_mem_pool.get(), false));
    
    LOG(INFO) << "xk debug reset predictate type=" << (_predictate->type() == PredicateType::GE)
              << " " << (_predictate->type() == PredicateType::LE)
              << " column_id=" << _predictate->column_id() << " condition=" << condition;

    return Status::OK();
}

} // namespace vectorized
} // namespace doris
