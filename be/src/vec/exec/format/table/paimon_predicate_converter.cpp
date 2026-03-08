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

#include "vec/exec/format/table/paimon_predicate_converter.h"

#include <algorithm>
#include <cctype>
#include <utility>

#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/predicate/predicate_builder.h"
#include "runtime/decimalv2_value.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vcompound_pred.h"
#include "vec/exprs/vdirect_in_predicate.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/runtime/timestamptz_value.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

PaimonPredicateConverter::PaimonPredicateConverter(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state)
        : _state(state) {
    _field_index_by_name.reserve(file_slot_descs.size());
    for (size_t i = 0; i < file_slot_descs.size(); ++i) {
        const auto& name = file_slot_descs[i]->col_name();
        auto normalized = _normalize_name(name);
        if (_field_index_by_name.find(normalized) == _field_index_by_name.end()) {
            _field_index_by_name.emplace(std::move(normalized), static_cast<int32_t>(i));
        }
    }

    if (!TimezoneUtils::find_cctz_time_zone("GMT", _gmt_tz)) {
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _gmt_tz);
    }
}

std::shared_ptr<paimon::Predicate> PaimonPredicateConverter::build(
        const VExprContextSPtrs& conjuncts) {
    std::vector<std::shared_ptr<paimon::Predicate>> predicates;
    predicates.reserve(conjuncts.size());
    for (const auto& conjunct : conjuncts) {
        if (!conjunct || !conjunct->root()) {
            continue;
        }
        auto root = conjunct->root();
        if (root->is_rf_wrapper()) {
            if (auto impl = root->get_impl()) {
                root = impl;
            }
        }
        auto predicate = _convert_expr(root);
        if (predicate) {
            predicates.emplace_back(std::move(predicate));
        }
    }

    if (predicates.empty()) {
        return nullptr;
    }
    if (predicates.size() == 1) {
        return predicates.front();
    }
    auto and_result = paimon::PredicateBuilder::And(predicates);
    if (!and_result.ok()) {
        return nullptr;
    }
    return std::move(and_result).value();
}

std::shared_ptr<paimon::Predicate> PaimonPredicateConverter::_convert_expr(const VExprSPtr& expr) {
    if (!expr) {
        return nullptr;
    }

    auto uncast = VExpr::expr_without_cast(expr);

    if (auto* direct_in = dynamic_cast<VDirectInPredicate*>(uncast.get())) {
        VExprSPtr in_expr;
        if (direct_in->get_slot_in_expr(in_expr)) {
            return _convert_in(in_expr);
        }
        return nullptr;
    }

    if (dynamic_cast<VInPredicate*>(uncast.get()) != nullptr) {
        return _convert_in(uncast);
    }

    switch (uncast->op()) {
    case TExprOpcode::COMPOUND_AND:
    case TExprOpcode::COMPOUND_OR:
        return _convert_compound(uncast);
    case TExprOpcode::COMPOUND_NOT:
        return nullptr;
    case TExprOpcode::EQ:
    case TExprOpcode::EQ_FOR_NULL:
    case TExprOpcode::NE:
    case TExprOpcode::GE:
    case TExprOpcode::GT:
    case TExprOpcode::LE:
    case TExprOpcode::LT:
        return _convert_binary(uncast);
    default:
        break;
    }

    if (auto* fn = dynamic_cast<VectorizedFnCall*>(uncast.get())) {
        auto fn_name = _normalize_name(fn->function_name());
        if (fn_name == "is_null_pred" || fn_name == "is_not_null_pred") {
            return _convert_is_null(uncast, fn_name);
        }
        if (fn_name == "like") {
            return _convert_like_prefix(uncast);
        }
    }

    return nullptr;
}

std::shared_ptr<paimon::Predicate> PaimonPredicateConverter::_convert_compound(
        const VExprSPtr& expr) {
    if (!expr || expr->get_num_children() != 2) {
        return nullptr;
    }
    auto left = _convert_expr(expr->get_child(0));
    if (!left) {
        return nullptr;
    }
    auto right = _convert_expr(expr->get_child(1));
    if (!right) {
        return nullptr;
    }

    if (expr->op() == TExprOpcode::COMPOUND_AND) {
        auto and_result = paimon::PredicateBuilder::And({left, right});
        return and_result.ok() ? std::move(and_result).value() : nullptr;
    }
    if (expr->op() == TExprOpcode::COMPOUND_OR) {
        auto or_result = paimon::PredicateBuilder::Or({left, right});
        return or_result.ok() ? std::move(or_result).value() : nullptr;
    }
    return nullptr;
}

std::shared_ptr<paimon::Predicate> PaimonPredicateConverter::_convert_in(const VExprSPtr& expr) {
    auto* in_pred = dynamic_cast<VInPredicate*>(expr.get());
    if (!in_pred || expr->get_num_children() < 2) {
        return nullptr;
    }
    auto field_meta = _resolve_field(expr->get_child(0));
    if (!field_meta) {
        return nullptr;
    }

    std::vector<paimon::Literal> literals;
    literals.reserve(expr->get_num_children() - 1);
    for (uint16_t i = 1; i < expr->get_num_children(); ++i) {
        auto literal = _convert_literal(expr->get_child(i), *field_meta->slot_desc,
                                        field_meta->field_type);
        if (!literal) {
            return nullptr;
        }
        literals.emplace_back(std::move(*literal));
    }

    if (literals.empty()) {
        return nullptr;
    }
    if (in_pred->is_not_in()) {
        return paimon::PredicateBuilder::NotIn(field_meta->index, field_meta->slot_desc->col_name(),
                                               field_meta->field_type, literals);
    }
    return paimon::PredicateBuilder::In(field_meta->index, field_meta->slot_desc->col_name(),
                                        field_meta->field_type, literals);
}

std::shared_ptr<paimon::Predicate> PaimonPredicateConverter::_convert_binary(
        const VExprSPtr& expr) {
    if (!expr || expr->get_num_children() != 2) {
        return nullptr;
    }
    auto field_meta = _resolve_field(expr->get_child(0));
    if (!field_meta) {
        return nullptr;
    }

    if (expr->op() == TExprOpcode::EQ_FOR_NULL) {
        return paimon::PredicateBuilder::IsNull(
                field_meta->index, field_meta->slot_desc->col_name(), field_meta->field_type);
    }

    auto literal =
            _convert_literal(expr->get_child(1), *field_meta->slot_desc, field_meta->field_type);
    if (!literal) {
        return nullptr;
    }

    switch (expr->op()) {
    case TExprOpcode::EQ:
        return paimon::PredicateBuilder::Equal(field_meta->index, field_meta->slot_desc->col_name(),
                                               field_meta->field_type, *literal);
    case TExprOpcode::NE:
        return paimon::PredicateBuilder::NotEqual(field_meta->index,
                                                  field_meta->slot_desc->col_name(),
                                                  field_meta->field_type, *literal);
    case TExprOpcode::GE:
        return paimon::PredicateBuilder::GreaterOrEqual(field_meta->index,
                                                        field_meta->slot_desc->col_name(),
                                                        field_meta->field_type, *literal);
    case TExprOpcode::GT:
        return paimon::PredicateBuilder::GreaterThan(field_meta->index,
                                                     field_meta->slot_desc->col_name(),
                                                     field_meta->field_type, *literal);
    case TExprOpcode::LE:
        return paimon::PredicateBuilder::LessOrEqual(field_meta->index,
                                                     field_meta->slot_desc->col_name(),
                                                     field_meta->field_type, *literal);
    case TExprOpcode::LT:
        return paimon::PredicateBuilder::LessThan(field_meta->index,
                                                  field_meta->slot_desc->col_name(),
                                                  field_meta->field_type, *literal);
    default:
        break;
    }
    return nullptr;
}

std::shared_ptr<paimon::Predicate> PaimonPredicateConverter::_convert_is_null(
        const VExprSPtr& expr, const std::string& fn_name) {
    if (!expr || expr->get_num_children() != 1) {
        return nullptr;
    }
    auto field_meta = _resolve_field(expr->get_child(0));
    if (!field_meta) {
        return nullptr;
    }
    if (fn_name == "is_not_null_pred") {
        return paimon::PredicateBuilder::IsNotNull(
                field_meta->index, field_meta->slot_desc->col_name(), field_meta->field_type);
    }
    return paimon::PredicateBuilder::IsNull(field_meta->index, field_meta->slot_desc->col_name(),
                                            field_meta->field_type);
}

std::shared_ptr<paimon::Predicate> PaimonPredicateConverter::_convert_like_prefix(
        const VExprSPtr& expr) {
    if (!expr || expr->get_num_children() != 2) {
        return nullptr;
    }
    auto field_meta = _resolve_field(expr->get_child(0));
    if (!field_meta || field_meta->field_type != paimon::FieldType::STRING) {
        return nullptr;
    }

    auto pattern_opt = _extract_string_literal(expr->get_child(1));
    if (!pattern_opt) {
        return nullptr;
    }
    const std::string& pattern = *pattern_opt;
    if (!pattern.empty() && pattern.front() == '%') {
        return nullptr;
    }
    if (pattern.empty() || pattern.back() != '%') {
        return nullptr;
    }

    std::string prefix = pattern.substr(0, pattern.size() - 1);
    paimon::Literal lower_literal(paimon::FieldType::STRING, prefix.data(), prefix.size());
    auto lower_pred = paimon::PredicateBuilder::GreaterOrEqual(
            field_meta->index, field_meta->slot_desc->col_name(), field_meta->field_type,
            lower_literal);

    auto upper_prefix = _next_prefix(prefix);
    if (!upper_prefix) {
        return lower_pred;
    }

    paimon::Literal upper_literal(paimon::FieldType::STRING, upper_prefix->data(),
                                  upper_prefix->size());
    auto upper_pred =
            paimon::PredicateBuilder::LessThan(field_meta->index, field_meta->slot_desc->col_name(),
                                               field_meta->field_type, upper_literal);
    auto and_result = paimon::PredicateBuilder::And({lower_pred, upper_pred});
    return and_result.ok() ? std::move(and_result).value() : nullptr;
}

std::optional<PaimonPredicateConverter::FieldMeta> PaimonPredicateConverter::_resolve_field(
        const VExprSPtr& expr) const {
    if (!_state || !expr) {
        return std::nullopt;
    }
    auto slot_expr = VExpr::expr_without_cast(expr);
    auto* slot_ref = dynamic_cast<VSlotRef*>(slot_expr.get());
    if (!slot_ref) {
        return std::nullopt;
    }
    auto* slot_desc = _state->desc_tbl().get_slot_descriptor(slot_ref->slot_id());
    if (!slot_desc) {
        return std::nullopt;
    }
    auto normalized = _normalize_name(slot_desc->col_name());
    auto it = _field_index_by_name.find(normalized);
    if (it == _field_index_by_name.end()) {
        return std::nullopt;
    }
    auto slot_type = slot_desc->type();
    auto field_type =
            _to_paimon_field_type(slot_type->get_primitive_type(), slot_type->get_precision());
    if (!field_type) {
        return std::nullopt;
    }
    return FieldMeta {it->second, *field_type, slot_desc};
}

std::optional<paimon::Literal> PaimonPredicateConverter::_convert_literal(
        const VExprSPtr& expr, const SlotDescriptor& slot_desc,
        paimon::FieldType field_type) const {
    auto literal_expr = VExpr::expr_without_cast(expr);
    auto* literal = dynamic_cast<VLiteral*>(literal_expr.get());
    if (!literal) {
        return std::nullopt;
    }

    auto literal_type = remove_nullable(literal->get_data_type());
    PrimitiveType literal_primitive = literal_type->get_primitive_type();
    PrimitiveType slot_primitive = slot_desc.type()->get_primitive_type();

    ColumnPtr col = literal->get_column_ptr()->convert_to_full_column_if_const();
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*col)) {
        if (nullable->is_null_at(0)) {
            return std::nullopt;
        }
        col = nullable->get_nested_column_ptr();
    }

    Field field;
    col->get(0, field);

    switch (slot_primitive) {
    case TYPE_BOOLEAN: {
        if (literal_primitive != TYPE_BOOLEAN) {
            return std::nullopt;
        }
        return paimon::Literal(static_cast<bool>(field.get<TYPE_BOOLEAN>()));
    }
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT: {
        if (!_is_integer_type(literal_primitive)) {
            return std::nullopt;
        }
        int64_t value = 0;
        switch (literal_primitive) {
        case TYPE_TINYINT:
            value = field.get<TYPE_TINYINT>();
            break;
        case TYPE_SMALLINT:
            value = field.get<TYPE_SMALLINT>();
            break;
        case TYPE_INT:
            value = field.get<TYPE_INT>();
            break;
        case TYPE_BIGINT:
            value = field.get<TYPE_BIGINT>();
            break;
        default:
            return std::nullopt;
        }
        if (slot_primitive == TYPE_TINYINT) {
            return paimon::Literal(static_cast<int8_t>(value));
        }
        if (slot_primitive == TYPE_SMALLINT) {
            return paimon::Literal(static_cast<int16_t>(value));
        }
        if (slot_primitive == TYPE_INT) {
            return paimon::Literal(static_cast<int32_t>(value));
        }
        return paimon::Literal(static_cast<int64_t>(value));
    }
    case TYPE_DOUBLE: {
        if (literal_primitive != TYPE_DOUBLE && literal_primitive != TYPE_FLOAT) {
            return std::nullopt;
        }
        double value = 0;
        if (literal_primitive == TYPE_FLOAT) {
            value = static_cast<double>(field.get<TYPE_FLOAT>());
        } else {
            value = field.get<TYPE_DOUBLE>();
        }
        return paimon::Literal(value);
    }
    case TYPE_DATE:
    case TYPE_DATEV2: {
        if (!_is_date_type(literal_primitive)) {
            return std::nullopt;
        }
        int64_t seconds = 0;
        if (literal_primitive == TYPE_DATE) {
            const auto& dt = field.get<TYPE_DATE>();
            if (!dt.is_valid_date()) {
                return std::nullopt;
            }
            dt.unix_timestamp(&seconds, _gmt_tz);
        } else if (literal_primitive == TYPE_DATEV2) {
            const auto& dt = field.get<TYPE_DATEV2>();
            if (!dt.is_valid_date()) {
                return std::nullopt;
            }
            dt.unix_timestamp(&seconds, _gmt_tz);
        }
        int32_t days = _seconds_to_days(seconds);
        return paimon::Literal(paimon::FieldType::DATE, days);
    }
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2: {
        if (!_is_datetime_type(literal_primitive)) {
            return std::nullopt;
        }
        if (literal_primitive == TYPE_DATETIME) {
            const auto& dt = field.get<TYPE_DATETIME>();
            if (!dt.is_valid_date()) {
                return std::nullopt;
            }
            int64_t seconds = 0;
            dt.unix_timestamp(&seconds, _gmt_tz);
            return paimon::Literal(paimon::Timestamp::FromEpochMillis(seconds * 1000));
        }
        std::pair<int64_t, int64_t> ts;
        const auto& dt = field.get<TYPE_DATETIMEV2>();
        if (!dt.is_valid_date()) {
            return std::nullopt;
        }
        dt.unix_timestamp(&ts, _gmt_tz);
        int64_t millis = ts.first * 1000 + ts.second / 1000;
        return paimon::Literal(paimon::Timestamp::FromEpochMillis(millis));
    }
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        if (!_is_string_type(literal_primitive)) {
            return std::nullopt;
        }
        const auto& value = field.get<TYPE_STRING>();
        return paimon::Literal(field_type, value.data(), value.size());
    }
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256: {
        if (!_is_decimal_type(literal_primitive)) {
            return std::nullopt;
        }
        int32_t precision = static_cast<int32_t>(literal_type->get_precision());
        int32_t scale = static_cast<int32_t>(literal_type->get_scale());
        if (precision <= 0 || precision > paimon::Decimal::MAX_PRECISION) {
            return std::nullopt;
        }

        paimon::Decimal::int128_t value = 0;
        switch (literal_primitive) {
        case TYPE_DECIMALV2: {
            const auto& dec = field.get<TYPE_DECIMALV2>();
            value = dec.value();
            break;
        }
        case TYPE_DECIMAL32: {
            const auto& dec = field.get<TYPE_DECIMAL32>();
            value = dec.value;
            break;
        }
        case TYPE_DECIMAL64: {
            const auto& dec = field.get<TYPE_DECIMAL64>();
            value = dec.value;
            break;
        }
        case TYPE_DECIMAL128I: {
            const auto& dec = field.get<TYPE_DECIMAL128I>();
            value = dec.value;
            break;
        }
        default:
            return std::nullopt;
        }
        return paimon::Literal(paimon::Decimal(precision, scale, value));
    }
    default:
        break;
    }
    return std::nullopt;
}

std::optional<std::string> PaimonPredicateConverter::_extract_string_literal(
        const VExprSPtr& expr) const {
    auto literal_expr = VExpr::expr_without_cast(expr);
    auto* literal = dynamic_cast<VLiteral*>(literal_expr.get());
    if (!literal) {
        return std::nullopt;
    }
    auto literal_type = remove_nullable(literal->get_data_type());
    PrimitiveType literal_primitive = literal_type->get_primitive_type();
    if (!_is_string_type(literal_primitive)) {
        return std::nullopt;
    }

    ColumnPtr col = literal->get_column_ptr()->convert_to_full_column_if_const();
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*col)) {
        if (nullable->is_null_at(0)) {
            return std::nullopt;
        }
        col = nullable->get_nested_column_ptr();
    }
    Field field;
    col->get(0, field);
    const auto& value = field.get<TYPE_STRING>();
    return value;
}

std::string PaimonPredicateConverter::_normalize_name(std::string_view name) {
    std::string out(name);
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

std::optional<std::string> PaimonPredicateConverter::_next_prefix(const std::string& prefix) {
    if (prefix.empty()) {
        return std::nullopt;
    }
    std::string upper = prefix;
    for (int i = static_cast<int>(upper.size()) - 1; i >= 0; --i) {
        auto c = static_cast<unsigned char>(upper[i]);
        if (c != 0xFF) {
            upper[i] = static_cast<char>(c + 1);
            upper.resize(i + 1);
            return upper;
        }
    }
    return std::nullopt;
}

int32_t PaimonPredicateConverter::_seconds_to_days(int64_t seconds) {
    static constexpr int64_t kSecondsPerDay = 24 * 60 * 60;
    int64_t days = seconds / kSecondsPerDay;
    if (seconds < 0 && seconds % kSecondsPerDay != 0) {
        --days;
    }
    return static_cast<int32_t>(days);
}

bool PaimonPredicateConverter::_is_integer_type(PrimitiveType type) {
    switch (type) {
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return true;
    default:
        return false;
    }
}

bool PaimonPredicateConverter::_is_string_type(PrimitiveType type) {
    return type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_STRING;
}

bool PaimonPredicateConverter::_is_decimal_type(PrimitiveType type) {
    switch (type) {
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        return true;
    default:
        return false;
    }
}

bool PaimonPredicateConverter::_is_date_type(PrimitiveType type) {
    return type == TYPE_DATE || type == TYPE_DATEV2;
}

bool PaimonPredicateConverter::_is_datetime_type(PrimitiveType type) {
    return type == TYPE_DATETIME || type == TYPE_DATETIMEV2;
}

std::optional<paimon::FieldType> PaimonPredicateConverter::_to_paimon_field_type(
        PrimitiveType type, uint32_t precision) {
    switch (type) {
    case TYPE_BOOLEAN:
        return paimon::FieldType::BOOLEAN;
    case TYPE_TINYINT:
        return paimon::FieldType::TINYINT;
    case TYPE_SMALLINT:
        return paimon::FieldType::SMALLINT;
    case TYPE_INT:
        return paimon::FieldType::INT;
    case TYPE_BIGINT:
        return paimon::FieldType::BIGINT;
    case TYPE_DOUBLE:
        return paimon::FieldType::DOUBLE;
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return paimon::FieldType::STRING;
    case TYPE_DATE:
    case TYPE_DATEV2:
        return paimon::FieldType::DATE;
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2:
        return paimon::FieldType::TIMESTAMP;
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        if (precision > 0 && precision > paimon::Decimal::MAX_PRECISION) {
            return std::nullopt;
        }
        return paimon::FieldType::DECIMAL;
    case TYPE_FLOAT:
    case TYPE_CHAR:
    default:
        return std::nullopt;
    }
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
