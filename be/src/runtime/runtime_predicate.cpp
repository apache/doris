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
#include "runtime/define_primitive_type.h"

namespace doris::vectorized {

RuntimePredicate::RuntimePredicate(const TTopnFilterDesc& desc)
        : _nulls_first(desc.null_first), _is_asc(desc.is_asc) {
    DCHECK(!desc.target_node_id_to_target_expr.empty());
    for (auto p : desc.target_node_id_to_target_expr) {
        _contexts[p.first].expr = p.second;
    }

    _type = thrift_to_type(desc.target_node_id_to_target_expr.begin()
                                   ->second.nodes[0]
                                   .type.types[0]
                                   .scalar_type.type);
    if (!_init(_type)) {
        std::stringstream ss;
        desc.target_node_id_to_target_expr.begin()->second.nodes[0].printTo(ss);
        throw Exception(ErrorCode::INTERNAL_ERROR, "meet invalid type, type={}, expr={}",
                        type_to_string(_type), ss.str());
    }

    // For ASC  sort, create runtime predicate col_name <= max_top_value
    // since values that > min_top_value are large than any value in current topn values
    // For DESC sort, create runtime predicate col_name >= min_top_value
    // since values that < min_top_value are less than any value in current topn values
    _pred_constructor = _is_asc ? create_comparison_predicate0<PredicateType::LE>
                                : create_comparison_predicate0<PredicateType::GE>;
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

StringRef RuntimePredicate::_get_string_ref(const Field& field, const PrimitiveType type) {
    switch (type) {
    case PrimitiveType::TYPE_BOOLEAN: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_BOOLEAN>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_TINYINT: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_TINYINT>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_SMALLINT: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_SMALLINT>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_INT: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_INT>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_BIGINT: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_BIGINT>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_LARGEINT: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_LARGEINT>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING: {
        const auto& v = field.get<String>();
        auto length = v.size();
        char* buffer = _predicate_arena.alloc(length);
        memset(buffer, 0, length);
        memcpy(buffer, v.data(), v.length());

        return {buffer, length};
    }
    case PrimitiveType::TYPE_DATEV2: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_DATEV2>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_DATETIMEV2: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_TIMESTAMPTZ: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_TIMESTAMPTZ>::CppType>();
        return StringRef((char*)&v, sizeof(v));
        break;
    }
    case PrimitiveType::TYPE_DATE: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_DATE>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_DATETIME: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_DATETIME>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_TIMEV2: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_TIMEV2>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_DECIMAL32: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_DECIMAL32>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_DECIMAL64: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_DECIMAL64>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_DECIMALV2: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_DECIMAL128I: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_DECIMAL128I>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_DECIMAL256: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_DECIMAL256>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_IPV4: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_IPV4>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case PrimitiveType::TYPE_IPV6: {
        const auto& v = field.get<typename PrimitiveTypeTraits<TYPE_IPV6>::CppType>();
        return StringRef((char*)&v, sizeof(v));
    }
    case doris::PrimitiveType::TYPE_VARBINARY: {
        _get_value_fn = [](const Field& field) {
            return field.get<StringViewField>().get_string();
        };
        break;
    }
    default:
        break;
    }

    throw Exception(ErrorCode::INTERNAL_ERROR, "meet invalid type, type={}", type_to_string(type));
    return StringRef();
}

bool RuntimePredicate::_init(PrimitiveType type) {
    return is_int_or_bool(type) || is_decimal(type) || is_string_type(type) || is_date_type(type) ||
           is_time_type(type) || is_ip(type);
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
        auto str_ref = _get_string_ref(_orderby_extrem, _type);
        std::shared_ptr<ColumnPredicate> pred =
                _pred_constructor(ctx.predicate->column_id(), column.get_vec_type(), str_ref, false,
                                  _predicate_arena);

        // For NULLS FIRST, wrap a AcceptNullPredicate to return true for NULL
        // since ORDER BY ASC/DESC should get NULL first but pred returns NULL
        // and NULL in where predicate will be treated as FALSE
        if (_nulls_first) {
            pred = AcceptNullPredicate::create_shared(pred);
        }

        ((SharedPredicate*)ctx.predicate.get())->set_nested(pred);
    }
    return Status::OK();
}

} // namespace doris::vectorized
