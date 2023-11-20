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

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

#include "common/status.h"
#include "exec/olap_common.h"
#include "olap/tablet_schema.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "util/binary_cast.hpp"
#include "vec/common/arena.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
class ColumnPredicate;

namespace vectorized {

class RuntimePredicate {
public:
    RuntimePredicate() = default;

    Status init(const PrimitiveType type, const bool nulls_first);

    bool inited() {
        std::unique_lock<std::shared_mutex> wlock(_rwlock);
        return _inited;
    }

    void set_tablet_schema(TabletSchemaSPtr tablet_schema) {
        std::unique_lock<std::shared_mutex> wlock(_rwlock);
        _tablet_schema = tablet_schema;
    }

    std::shared_ptr<ColumnPredicate> get_predictate() {
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        return _predictate;
    }

    Status update(const Field& value, const String& col_name, bool is_reverse);

private:
    mutable std::shared_mutex _rwlock;
    Field _orderby_extrem {Field::Types::Null};
    std::shared_ptr<ColumnPredicate> _predictate {nullptr};
    TabletSchemaSPtr _tablet_schema;
    std::unique_ptr<Arena> _predicate_arena;
    std::function<std::string(const Field&)> _get_value_fn;
    bool _nulls_first = true;
    bool _inited = false;

    static std::string get_bool_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_BOOLEAN>::CppType;
        return cast_to_string<TYPE_BOOLEAN, ValueType>(field.get<ValueType>(), 0);
    }

    static std::string get_tinyint_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_TINYINT>::CppType;
        return cast_to_string<TYPE_TINYINT, ValueType>(field.get<ValueType>(), 0);
    }

    static std::string get_smallint_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_SMALLINT>::CppType;
        return cast_to_string<TYPE_SMALLINT, ValueType>(field.get<ValueType>(), 0);
    }

    static std::string get_int_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_INT>::CppType;
        return cast_to_string<TYPE_INT, ValueType>(field.get<ValueType>(), 0);
    }

    static std::string get_bigint_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_BIGINT>::CppType;
        return cast_to_string<TYPE_BIGINT, ValueType>(field.get<ValueType>(), 0);
    }

    static std::string get_largeint_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_LARGEINT>::CppType;
        return cast_to_string<TYPE_LARGEINT, ValueType>(field.get<ValueType>(), 0);
    }

    static std::string get_float_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_FLOAT>::CppType;
        return cast_to_string<TYPE_FLOAT, ValueType>(field.get<ValueType>(), 0);
    }

    static std::string get_double_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DOUBLE>::CppType;
        return cast_to_string<TYPE_DOUBLE, ValueType>(field.get<ValueType>(), 0);
    }

    static std::string get_string_value(const Field& field) { return field.get<String>(); }

    static std::string get_date_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATE>::CppType;
        ValueType value;
        Int64 v = field.get<Int64>();
        VecDateTimeValue* p = (VecDateTimeValue*)&v;
        value.from_olap_date(p->to_olap_date());
        value.cast_to_date();
        return cast_to_string<TYPE_DATE, ValueType>(value, 0);
    }

    static std::string get_datetime_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType;
        ValueType value;
        Int64 v = field.get<Int64>();
        VecDateTimeValue* p = (VecDateTimeValue*)&v;
        value.from_olap_datetime(p->to_olap_datetime());
        value.to_datetime();
        return cast_to_string<TYPE_DATETIME, ValueType>(value, 0);
    }

    static std::string get_datev2_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATEV2>::CppType;
        return cast_to_string<TYPE_DATEV2, ValueType>(
                binary_cast<UInt32, ValueType>(field.get<UInt32>()), 0);
    }

    static std::string get_datetimev2_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType;
        return cast_to_string<TYPE_DATETIMEV2, ValueType>(
                binary_cast<UInt64, ValueType>(field.get<UInt64>()), 0);
    }

    static std::string get_decimalv2_value(const Field& field) {
        // can NOT use PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType since
        //   it is DecimalV2Value and Decimal128 can not convert to it implicitly
        using ValueType = Decimal128::NativeType;
        auto v = field.get<DecimalField<Decimal128>>();
        // use TYPE_DECIMAL128I instead of TYPE_DECIMALV2 since v.get_scale()
        //   is always 9 for DECIMALV2
        return cast_to_string<TYPE_DECIMAL128I, ValueType>(v.get_value(), v.get_scale());
    }

    static std::string get_decimal32_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DECIMAL32>::CppType;
        auto v = field.get<DecimalField<Decimal32>>();
        return cast_to_string<TYPE_DECIMAL32, ValueType>(v.get_value(), v.get_scale());
    }

    static std::string get_decimal64_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DECIMAL64>::CppType;
        auto v = field.get<DecimalField<Decimal64>>();
        return cast_to_string<TYPE_DECIMAL64, ValueType>(v.get_value(), v.get_scale());
    }

    static std::string get_decimal128_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DECIMAL128I>::CppType;
        auto v = field.get<DecimalField<Decimal128I>>();
        return cast_to_string<TYPE_DECIMAL128I, ValueType>(v.get_value(), v.get_scale());
    }

    static std::string get_decimal256_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DECIMAL256>::CppType;
        auto v = field.get<DecimalField<Decimal256>>();
        return cast_to_string<TYPE_DECIMAL256, ValueType>(v.get_value(), v.get_scale());
    }
};

} // namespace vectorized
} // namespace doris
