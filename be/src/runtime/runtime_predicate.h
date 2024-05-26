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
#include "olap/shared_predicate.h"
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

    Status init(PrimitiveType type, bool nulls_first, bool is_asc, const std::string& col_name);

    bool inited() const {
        // when sort node and scan node are not in the same fragment, predicate will not be initialized
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        return _inited;
    }

    bool need_update() const {
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        return _inited && _tablet_schema;
    }

    Status set_tablet_schema(TabletSchemaSPtr tablet_schema) {
        std::unique_lock<std::shared_mutex> wlock(_rwlock);
        if (_tablet_schema || !_inited) {
            return Status::OK();
        }
        RETURN_IF_ERROR(tablet_schema->have_column(_col_name));
        _tablet_schema = tablet_schema;
        _predicate = SharedPredicate::create_shared(
                _tablet_schema->field_index(_tablet_schema->column(_col_name).unique_id()));
        return Status::OK();
    }

    std::shared_ptr<ColumnPredicate> get_predicate() {
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        return _predicate;
    }

    Status update(const Field& value);

    bool has_value() const {
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        return _has_value;
    }

    Field get_value() const {
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        return _orderby_extrem;
    }

    std::string get_col_name() const { return _col_name; }

    bool is_asc() const { return _is_asc; }

    bool nulls_first() const { return _nulls_first; }

    bool target_is_slot() const { return true; }

private:
    mutable std::shared_mutex _rwlock;
    Field _orderby_extrem {Field::Types::Null};
    std::shared_ptr<ColumnPredicate> _predicate;
    TabletSchemaSPtr _tablet_schema = nullptr;
    Arena _predicate_arena;
    std::function<std::string(const Field&)> _get_value_fn;
    bool _nulls_first = true;
    bool _is_asc;
    std::function<ColumnPredicate*(const TabletColumn&, int, const std::string&, bool,
                                   vectorized::Arena*)>
            _pred_constructor;
    bool _inited = false;
    std::string _col_name;
    bool _has_value = false;

    template <PrimitiveType type>
    static std::string get_normal_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<type>::CppType;
        return cast_to_string<type, ValueType>(field.get<ValueType>(), 0);
    }

    static std::string get_date_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATE>::CppType;
        ValueType value;
        Int64 v = field.get<Int64>();
        auto* p = (VecDateTimeValue*)&v;
        value.from_olap_date(p->to_olap_date());
        value.cast_to_date();
        return cast_to_string<TYPE_DATE, ValueType>(value, 0);
    }

    static std::string get_datetime_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType;
        ValueType value;
        Int64 v = field.get<Int64>();
        auto* p = (VecDateTimeValue*)&v;
        value.from_olap_datetime(p->to_olap_datetime());
        value.to_datetime();
        return cast_to_string<TYPE_DATETIME, ValueType>(value, 0);
    }

    static std::string get_decimalv2_value(const Field& field) {
        // can NOT use PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType since
        //   it is DecimalV2Value and Decimal128V2 can not convert to it implicitly
        using ValueType = Decimal128V2::NativeType;
        auto v = field.get<DecimalField<Decimal128V2>>();
        // use TYPE_DECIMAL128I instead of TYPE_DECIMALV2 since v.get_scale()
        //   is always 9 for DECIMALV2
        return cast_to_string<TYPE_DECIMAL128I, ValueType>(v.get_value(), v.get_scale());
    }

    template <PrimitiveType type>
    static std::string get_decimal_value(const Field& field) {
        using ValueType = typename PrimitiveTypeTraits<type>::CppType;
        auto v = field.get<DecimalField<ValueType>>();
        return cast_to_string<type, ValueType>(v.get_value(), v.get_scale());
    }
};

} // namespace vectorized
} // namespace doris
