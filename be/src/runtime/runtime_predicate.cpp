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

#include <memory>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/exception.h"
#include "common/status.h"
#include "olap/accept_null_predicate.h"
#include "olap/column_predicate.h"
#include "olap/predicate_creator.h"

namespace doris::vectorized {

RuntimePredicate::RuntimePredicate(const TTopnFilterDesc& desc)
        : _nulls_first(desc.null_first), _is_asc(desc.is_asc) {
    DCHECK(!desc.target_node_id_to_target_expr.empty());
    for (auto p : desc.target_node_id_to_target_expr) {
        _contexts[p.first].expr = p.second;
    }

    PrimitiveType type = thrift_to_type(desc.target_node_id_to_target_expr.begin()
                                                ->second.nodes[0]
                                                .type.types[0]
                                                .scalar_type.type);
    if (!_init(type)) {
        std::stringstream ss;
        desc.target_node_id_to_target_expr.begin()->second.nodes[0].printTo(ss);
        throw Exception(ErrorCode::INTERNAL_ERROR, "meet invalid type, type={}, expr={}", int(type),
                        ss.str());
    }

    // For ASC  sort, create runtime predicate col_name <= max_top_value
    // since values that > min_top_value are large than any value in current topn values
    // For DESC sort, create runtime predicate col_name >= min_top_value
    // since values that < min_top_value are less than any value in current topn values
    _pred_constructor = _is_asc ? create_comparison_predicate<PredicateType::LE>
                                : create_comparison_predicate<PredicateType::GE>;
}

void RuntimePredicate::init_target(
        int32_t target_node_id, phmap::flat_hash_map<int, SlotDescriptor*> slot_id_to_slot_desc) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    check_target_node_id(target_node_id);
    if (target_is_slot(target_node_id)) {
        _contexts[target_node_id].col_name =
                slot_id_to_slot_desc[get_texpr(target_node_id).nodes[0].slot_ref.slot_id]
                        ->col_name();
    }
    _detected_target = true;
}

template <PrimitiveType type>
std::string get_normal_value(const Field& field) {
    using ValueType = typename PrimitiveTypeTraits<type>::CppType;
    return cast_to_string<type, ValueType>(field.get<ValueType>(), 0);
}

std::string get_date_value(const Field& field) {
    using ValueType = typename PrimitiveTypeTraits<TYPE_DATE>::CppType;
    ValueType value;
    Int64 v = field.get<Int64>();
    auto* p = (VecDateTimeValue*)&v;
    value.from_olap_date(p->to_olap_date());
    value.cast_to_date();
    return cast_to_string<TYPE_DATE, ValueType>(value, 0);
}

std::string get_datetime_value(const Field& field) {
    using ValueType = typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType;
    ValueType value;
    Int64 v = field.get<Int64>();
    auto* p = (VecDateTimeValue*)&v;
    value.from_olap_datetime(p->to_olap_datetime());
    value.to_datetime();
    return cast_to_string<TYPE_DATETIME, ValueType>(value, 0);
}

std::string get_decimalv2_value(const Field& field) {
    // can NOT use PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType since
    //   it is DecimalV2Value and Decimal128V2 can not convert to it implicitly
    using ValueType = Decimal128V2::NativeType;
    auto v = field.get<DecimalField<Decimal128V2>>();
    // use TYPE_DECIMAL128I instead of TYPE_DECIMALV2 since v.get_scale()
    //   is always 9 for DECIMALV2
    return cast_to_string<TYPE_DECIMAL128I, ValueType>(v.get_value(), v.get_scale());
}

template <PrimitiveType type>
std::string get_decimal_value(const Field& field) {
    using ValueType = typename PrimitiveTypeTraits<type>::CppType;
    auto v = field.get<DecimalField<ValueType>>();
    return cast_to_string<type, ValueType>(v.get_value(), v.get_scale());
}

bool RuntimePredicate::_init(PrimitiveType type) {
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
        return false;
    }

    return true;
}

Status RuntimePredicate::update(const Field& value) {
    std::unique_lock<std::shared_mutex> wlock(_rwlock);
    // skip null value
    if (value.is_null()) {
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

    if (!updated) {
        return Status::OK();
    }
    for (auto p : _contexts) {
        auto ctx = p.second;
        if (!ctx.tablet_schema) {
            continue;
        }
        const auto& column = *DORIS_TRY(ctx.tablet_schema->column(ctx.col_name));
        std::unique_ptr<ColumnPredicate> pred {_pred_constructor(column, ctx.predicate->column_id(),
                                                                 _get_value_fn(_orderby_extrem),
                                                                 false, &_predicate_arena)};

        // For NULLS FIRST, wrap a AcceptNullPredicate to return true for NULL
        // since ORDER BY ASC/DESC should get NULL first but pred returns NULL
        // and NULL in where predicate will be treated as FALSE
        if (_nulls_first) {
            pred = AcceptNullPredicate::create_unique(pred.release());
        }

        ((SharedPredicate*)ctx.predicate.get())->set_nested(pred.release());
    }
    return Status::OK();
}

} // namespace doris::vectorized
