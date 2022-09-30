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

Status RuntimePredicate::update(std::vector<Field>& values, const String& col_name,
                const TypeIndex type, bool is_reverse) {
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

    Field& field = _orderby_extrems[0];
    String str_value;
    int scale = 0;

    switch (type) {
    case TypeIndex::UInt8: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_BOOLEAN>::CppType;
        str_value = cast_to_string<TYPE_BOOLEAN, ValueType>(field.get<ValueType>(), scale);
        break;
    }
    case TypeIndex::Int8: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_TINYINT>::CppType;
        str_value = cast_to_string<TYPE_TINYINT, ValueType>(field.get<ValueType>(), scale);
        break;
    }
    case TypeIndex::Int16: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_SMALLINT>::CppType;
        str_value = cast_to_string<TYPE_SMALLINT, ValueType>(field.get<ValueType>(), scale);
        break;
    }
    case TypeIndex::Int32: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_INT>::CppType;
        str_value = cast_to_string<TYPE_INT, ValueType>(field.get<ValueType>(), scale);
        break;
    }
    case TypeIndex::Int64: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_BIGINT>::CppType;
        str_value = cast_to_string<TYPE_BIGINT, ValueType>(field.get<ValueType>(), scale);
        break;
    }
    case TypeIndex::Int128: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_LARGEINT>::CppType;
        str_value = cast_to_string<TYPE_LARGEINT, ValueType>(field.get<ValueType>(), scale);
        break;
    }
    case TypeIndex::Float32: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_FLOAT>::CppType;
        str_value = cast_to_string<TYPE_FLOAT, ValueType>(field.get<ValueType>(), scale);
        break;
    }
    case TypeIndex::Float64: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DOUBLE>::CppType;
        str_value = cast_to_string<TYPE_DOUBLE, ValueType>(field.get<ValueType>(), scale);
        break;
    }
    case TypeIndex::String: {
        str_value = field.get<String>();
        break;
    }
    case TypeIndex::DateV2: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATEV2>::CppType;
        str_value = cast_to_string<TYPE_DATEV2, ValueType>(
            binary_cast<UInt32, ValueType>(field.get<UInt32>()), scale);
        break;
    }
    case TypeIndex::DateTimeV2: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType;
        str_value = cast_to_string<TYPE_DATETIMEV2, ValueType>(
            binary_cast<UInt64, ValueType>(field.get<UInt64>()), scale);
        break;
    }
    case TypeIndex::Date: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATE>::CppType;
        ValueType value;
        Int64 v = field.get<Int64>();
        VecDateTimeValue* p = (VecDateTimeValue*)&v;
        value.from_olap_date(p->to_olap_date());
        value.cast_to_date();
        str_value = cast_to_string<TYPE_DATE, ValueType>(value, scale);
        break;
    }
    case TypeIndex::DateTime: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType;
        ValueType value;
        Int64 v = field.get<Int64>();
        VecDateTimeValue* p = (VecDateTimeValue*)&v;
        value.from_olap_datetime(p->to_olap_datetime());
        value.to_datetime();
        str_value = cast_to_string<TYPE_DATETIME, ValueType>(value, scale);
        break;
    }
    case TypeIndex::Decimal128: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType;
        ValueType value;
        auto v = field.get<DecimalField<Decimal128>>();
        value.from_olap_decimal(v.get_value(), v.get_scale());
        scale = v.get_scale();
        str_value = cast_to_string<TYPE_DECIMALV2, ValueType>(value, scale);
        break;
    }
    case TypeIndex::Decimal32: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DECIMAL32>::CppType;
        ValueType value = field.get<ValueType>();
        str_value = cast_to_string<TYPE_DECIMAL32, ValueType>(value, scale);
        break;
    }
    case TypeIndex::Decimal64: {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DECIMAL64>::CppType;
        ValueType value = field.get<ValueType>();
        str_value = cast_to_string<TYPE_DECIMAL64, ValueType>(value, scale);
        break;
    }
    default:
        return Status::InvalidArgument("unsupported type {}", type);
    }

    condition.condition_values.push_back(str_value);

    _predictate.reset(parse_to_predicate(_tablet_schema, condition, _predicate_mem_pool.get(), false));

    return Status::OK();
}

} // namespace vectorized
} // namespace doris
