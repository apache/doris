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

#include "format_v2/orc/orc_search_argument.h"

#include <cctz/time_zone.h>
#include <gen_cpp/Exprs_types.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <orc/Type.hh>
#include <orc/sargs/Literal.hh>
#include <orc/sargs/SearchArgument.hh>
#include <string>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "core/column/column.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "exprs/vtopn_pred.h"

namespace doris::format::orc {
namespace {

// SARG conversion is intentionally conservative: only direct slots,
// struct_element chains, and whitelisted schema-evolution casts become ORC
// SearchArgument predicates. Everything else stays as row-level filtering.
// Supported ORC predicate domains are LONG, FLOAT, STRING, DATE, DECIMAL,
// TIMESTAMP, and BOOLEAN.
std::optional<::orc::PredicateDataType> predicate_type_for_orc_type(const ::orc::Type& type) {
    switch (type.getKind()) {
    case ::orc::TypeKind::BYTE:
    case ::orc::TypeKind::SHORT:
    case ::orc::TypeKind::INT:
    case ::orc::TypeKind::LONG:
        return ::orc::PredicateDataType::LONG;
    case ::orc::TypeKind::FLOAT:
    case ::orc::TypeKind::DOUBLE:
        return ::orc::PredicateDataType::FLOAT;
    case ::orc::TypeKind::STRING:
    case ::orc::TypeKind::BINARY:
    case ::orc::TypeKind::VARCHAR:
        return ::orc::PredicateDataType::STRING;
    case ::orc::TypeKind::DATE:
        return ::orc::PredicateDataType::DATE;
    case ::orc::TypeKind::DECIMAL:
        return ::orc::PredicateDataType::DECIMAL;
    case ::orc::TypeKind::TIMESTAMP:
    case ::orc::TypeKind::TIMESTAMP_INSTANT:
        return ::orc::PredicateDataType::TIMESTAMP;
    case ::orc::TypeKind::BOOLEAN:
        return ::orc::PredicateDataType::BOOLEAN;
    default:
        return std::nullopt;
    }
}

struct OrcSargColumn {
    uint64_t column_id = 0;
    ::orc::PredicateDataType predicate_type = ::orc::PredicateDataType::LONG;
    const ::orc::Type* orc_type = nullptr;
};

struct OrcSargComparison {
    OrcSargColumn column;
    ::orc::Literal literal;
    TExprOpcode::type normalized_op = TExprOpcode::INVALID_OPCODE;
};

std::optional<format::LocalColumnId> file_column_id_for_slot_position(
        const format::FileScanRequest& request, int slot_column_id) {
    if (slot_column_id < 0) {
        return std::nullopt;
    }
    // Localized VSlotRef::column_id() points to the file block position, not the file schema id.
    const auto slot_position = cast_set<size_t>(slot_column_id);
    for (const auto& [file_column_id, local_position] : request.local_positions) {
        if (local_position.value() == slot_position) {
            return file_column_id;
        }
    }
    return std::nullopt;
}

const ::orc::Type* orc_type_for_slot(const format::FileScanRequest& request,
                                     const ::orc::Type& root_type, const VExprSPtr& slot_expr) {
    if (slot_expr == nullptr || !slot_expr->is_slot_ref()) {
        return nullptr;
    }
    const auto* slot_ref = dynamic_cast<const VSlotRef*>(slot_expr.get());
    if (slot_ref == nullptr || slot_ref->column_id() < 0) {
        return nullptr;
    }
    const auto file_column_id = file_column_id_for_slot_position(request, slot_ref->column_id());
    if (!file_column_id.has_value() || !file_column_id->is_valid()) {
        return nullptr;
    }
    const auto file_column_position = cast_set<uint64_t>(file_column_id->value());
    if (file_column_position >= root_type.getSubtypeCount()) {
        return nullptr;
    }
    return root_type.getSubtype(file_column_position);
}

std::optional<OrcSargColumn> sarg_column_for_orc_type(const ::orc::Type* orc_type) {
    if (orc_type == nullptr) {
        return std::nullopt;
    }
    const auto predicate_type = predicate_type_for_orc_type(*orc_type);
    if (!predicate_type.has_value()) {
        return std::nullopt;
    }
    return OrcSargColumn {
            .column_id = orc_type->getColumnId(),
            .predicate_type = *predicate_type,
            .orc_type = orc_type,
    };
}

const ::orc::Type* orc_type_for_sarg_source_expr(const format::FileScanRequest& request,
                                                 const ::orc::Type& root_type,
                                                 const VExprSPtr& expr);

std::optional<Field> literal_field_for_sarg(const VExprSPtr& literal_expr) {
    if (literal_expr == nullptr || !literal_expr->is_literal()) {
        return std::nullopt;
    }
    const auto* literal = dynamic_cast<const VLiteral*>(literal_expr.get());
    if (literal == nullptr || literal->get_column_ptr().get() == nullptr ||
        literal->get_column_ptr()->is_null_at(0)) {
        return std::nullopt;
    }
    Field field;
    literal->get_column_ptr()->get(0, field);
    return field;
}

bool is_struct_element_expr(const VExprSPtr& expr) {
    return expr != nullptr && expr->children().size() == 2 &&
           expr->fn().name.function_name == "struct_element";
}

// struct_element can address a child by field name or 1-based ordinal.
std::optional<uint64_t> struct_child_index(const ::orc::Type& struct_type,
                                           const VExprSPtr& selector_expr) {
    if (struct_type.getKind() != ::orc::TypeKind::STRUCT) {
        return std::nullopt;
    }
    const auto field = literal_field_for_sarg(selector_expr);
    if (!field.has_value()) {
        return std::nullopt;
    }
    switch (field->get_type()) {
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        const auto child_name = std::string(field->as_string_view());
        for (uint64_t child_idx = 0; child_idx < struct_type.getSubtypeCount(); ++child_idx) {
            if (struct_type.getFieldName(child_idx) == child_name) {
                return child_idx;
            }
        }
        return std::nullopt;
    }
    case TYPE_TINYINT:
        if (field->get<TYPE_TINYINT>() <= 0) {
            return std::nullopt;
        }
        return cast_set<uint64_t>(field->get<TYPE_TINYINT>() - 1);
    case TYPE_SMALLINT:
        if (field->get<TYPE_SMALLINT>() <= 0) {
            return std::nullopt;
        }
        return cast_set<uint64_t>(field->get<TYPE_SMALLINT>() - 1);
    case TYPE_INT:
        if (field->get<TYPE_INT>() <= 0) {
            return std::nullopt;
        }
        return cast_set<uint64_t>(field->get<TYPE_INT>() - 1);
    case TYPE_BIGINT:
        if (field->get<TYPE_BIGINT>() <= 0) {
            return std::nullopt;
        }
        return cast_set<uint64_t>(field->get<TYPE_BIGINT>() - 1);
    default:
        return std::nullopt;
    }
}

const ::orc::Type* orc_type_for_struct_element(const format::FileScanRequest& request,
                                               const ::orc::Type& root_type,
                                               const VExprSPtr& expr) {
    if (!is_struct_element_expr(expr)) {
        return nullptr;
    }
    const auto* parent_type =
            orc_type_for_sarg_source_expr(request, root_type, expr->children()[0]);
    if (parent_type == nullptr || parent_type->getKind() != ::orc::TypeKind::STRUCT) {
        return nullptr;
    }
    const auto child_idx = struct_child_index(*parent_type, expr->children()[1]);
    if (!child_idx.has_value() || *child_idx >= parent_type->getSubtypeCount()) {
        return nullptr;
    }
    return parent_type->getSubtype(*child_idx);
}

const ::orc::Type* orc_type_for_sarg_source_expr(const format::FileScanRequest& request,
                                                 const ::orc::Type& root_type,
                                                 const VExprSPtr& expr) {
    if (is_struct_element_expr(expr)) {
        return orc_type_for_struct_element(request, root_type, expr);
    }
    return orc_type_for_slot(request, root_type, expr);
}

std::optional<OrcSargColumn> sarg_column_for_source_expr(const format::FileScanRequest& request,
                                                         const ::orc::Type& root_type,
                                                         const VExprSPtr& expr) {
    return sarg_column_for_orc_type(orc_type_for_sarg_source_expr(request, root_type, expr));
}

// Safe cast checks protect pruning correctness. A cast is SARGable only when the
// comparison in ORC's domain is equivalent to the original Doris expression.
std::optional<uint8_t> orc_integer_width(const ::orc::Type& type) {
    switch (type.getKind()) {
    case ::orc::TypeKind::BYTE:
        return 8;
    case ::orc::TypeKind::SHORT:
        return 16;
    case ::orc::TypeKind::INT:
        return 32;
    case ::orc::TypeKind::LONG:
        return 64;
    default:
        return std::nullopt;
    }
}

std::optional<uint8_t> signed_integer_width(PrimitiveType type) {
    switch (type) {
    case TYPE_TINYINT:
        return 8;
    case TYPE_SMALLINT:
        return 16;
    case TYPE_INT:
        return 32;
    case TYPE_BIGINT:
        return 64;
    default:
        return std::nullopt;
    }
}

std::optional<uint8_t> orc_floating_width(const ::orc::Type& type) {
    switch (type.getKind()) {
    case ::orc::TypeKind::FLOAT:
        return 32;
    case ::orc::TypeKind::DOUBLE:
        return 64;
    default:
        return std::nullopt;
    }
}

std::optional<uint8_t> floating_width(PrimitiveType type) {
    switch (type) {
    case TYPE_FLOAT:
        return 32;
    case TYPE_DOUBLE:
        return 64;
    default:
        return std::nullopt;
    }
}

std::optional<uint8_t> floating_exact_integer_width(PrimitiveType type) {
    switch (type) {
    case TYPE_FLOAT:
        return 24;
    case TYPE_DOUBLE:
        return 53;
    default:
        return std::nullopt;
    }
}

struct DecimalPrecisionScale {
    UInt32 precision = 0;
    UInt32 scale = 0;
};

std::optional<DecimalPrecisionScale> decimal_precision_scale(const DataTypePtr& type) {
    if (type == nullptr || !is_decimal(type->get_primitive_type())) {
        return std::nullopt;
    }
    const auto precision = type->get_precision();
    const auto scale = type->get_scale();
    if (precision == 0 || scale > precision) {
        return std::nullopt;
    }
    return DecimalPrecisionScale {.precision = precision, .scale = scale};
}

bool is_safe_widening_cast(const OrcSargColumn& column, const VExprSPtr& cast_expr) {
    if (column.orc_type == nullptr || cast_expr == nullptr || cast_expr->data_type() == nullptr) {
        return false;
    }
    const auto target_type = remove_nullable(cast_expr->data_type());
    if (target_type == nullptr) {
        return false;
    }
    const auto target_primitive_type = target_type->get_primitive_type();
    if (const auto source_width = orc_integer_width(*column.orc_type)) {
        const auto target_width = signed_integer_width(target_primitive_type);
        return target_width.has_value() && *target_width >= *source_width;
    }
    if (const auto source_width = orc_floating_width(*column.orc_type)) {
        const auto target_width = floating_width(target_primitive_type);
        return target_width.has_value() && *target_width >= *source_width;
    }
    return false;
}

bool is_safe_date_schema_evolution_cast(const OrcSargColumn& column, const VExprSPtr& cast_expr) {
    if (column.orc_type == nullptr || cast_expr == nullptr || cast_expr->data_type() == nullptr ||
        column.orc_type->getKind() != ::orc::TypeKind::DATE) {
        return false;
    }
    const auto target_type = remove_nullable(cast_expr->data_type());
    if (target_type == nullptr) {
        return false;
    }
    const auto target_primitive_type = target_type->get_primitive_type();
    return target_primitive_type == TYPE_DATE || target_primitive_type == TYPE_DATEV2 ||
           target_primitive_type == TYPE_DATETIME || target_primitive_type == TYPE_DATETIMEV2;
}

bool is_safe_string_schema_evolution_cast(const OrcSargColumn& column, const VExprSPtr& cast_expr) {
    if (column.orc_type == nullptr || cast_expr == nullptr || cast_expr->data_type() == nullptr) {
        return false;
    }
    const auto orc_kind = column.orc_type->getKind();
    switch (column.orc_type->getKind()) {
    case ::orc::TypeKind::STRING:
    case ::orc::TypeKind::BINARY:
    case ::orc::TypeKind::VARCHAR:
        break;
    default:
        return false;
    }
    if (cast_expr->children().size() != 1 || cast_expr->children()[0] == nullptr ||
        cast_expr->children()[0]->data_type() == nullptr) {
        return false;
    }
    const auto source_type = remove_nullable(cast_expr->children()[0]->data_type());
    if (source_type == nullptr) {
        return false;
    }
    const auto source_primitive_type = source_type->get_primitive_type();
    const bool source_is_string_like =
            source_primitive_type == TYPE_STRING || source_primitive_type == TYPE_VARCHAR ||
            (orc_kind == ::orc::TypeKind::BINARY && source_primitive_type == TYPE_VARBINARY);
    if (!source_is_string_like) {
        return false;
    }
    const auto target_type = remove_nullable(cast_expr->data_type());
    if (target_type == nullptr) {
        return false;
    }
    return target_type->get_primitive_type() == TYPE_STRING;
}

bool is_safe_decimal_schema_evolution_cast(const OrcSargColumn& column,
                                           const VExprSPtr& cast_expr) {
    if (column.orc_type == nullptr || column.orc_type->getKind() != ::orc::TypeKind::DECIMAL ||
        cast_expr == nullptr || cast_expr->data_type() == nullptr ||
        cast_expr->children().size() != 1 || cast_expr->children()[0] == nullptr ||
        cast_expr->children()[0]->data_type() == nullptr) {
        return false;
    }

    const auto source_type = remove_nullable(cast_expr->children()[0]->data_type());
    const auto target_type = remove_nullable(cast_expr->data_type());
    const auto source_decimal = decimal_precision_scale(source_type);
    const auto target_decimal = decimal_precision_scale(target_type);
    if (!source_decimal.has_value() || !target_decimal.has_value()) {
        return false;
    }
    if (source_decimal->precision != cast_set<UInt32>(column.orc_type->getPrecision()) ||
        source_decimal->scale != cast_set<UInt32>(column.orc_type->getScale())) {
        return false;
    }

    const auto source_integer_digits = source_decimal->precision - source_decimal->scale;
    const auto target_integer_digits = target_decimal->precision - target_decimal->scale;
    return target_decimal->scale >= source_decimal->scale &&
           target_integer_digits >= source_integer_digits;
}

bool is_safe_integer_to_floating_cast(const OrcSargColumn& column, const VExprSPtr& cast_expr) {
    if (column.orc_type == nullptr || cast_expr == nullptr || cast_expr->data_type() == nullptr) {
        return false;
    }
    const auto source_width = orc_integer_width(*column.orc_type);
    if (!source_width.has_value()) {
        return false;
    }
    const auto target_type = remove_nullable(cast_expr->data_type());
    if (target_type == nullptr) {
        return false;
    }
    const auto target_width = floating_exact_integer_width(target_type->get_primitive_type());
    return target_width.has_value() && *source_width <= *target_width;
}

bool is_safe_cast_for_sarg(const OrcSargColumn& column, const VExprSPtr& cast_expr) {
    return is_safe_widening_cast(column, cast_expr) ||
           is_safe_date_schema_evolution_cast(column, cast_expr) ||
           is_safe_string_schema_evolution_cast(column, cast_expr) ||
           is_safe_decimal_schema_evolution_cast(column, cast_expr) ||
           is_safe_integer_to_floating_cast(column, cast_expr);
}

std::optional<OrcSargColumn> sarg_column_for_slot_or_safe_cast(
        const format::FileScanRequest& request, const ::orc::Type& root_type,
        const VExprSPtr& expr) {
    auto column = sarg_column_for_source_expr(request, root_type, expr);
    if (column.has_value()) {
        return column;
    }
    if (expr == nullptr || expr->node_type() != TExprNodeType::CAST_EXPR ||
        expr->children().size() != 1) {
        return std::nullopt;
    }
    column = sarg_column_for_source_expr(request, root_type, expr->children()[0]);
    if (!column.has_value() || !is_safe_cast_for_sarg(*column, expr)) {
        return std::nullopt;
    }
    return column;
}

// Literal conversion preserves ORC PredicateDataType rules. Unsupported
// literal/type pairs simply make the surrounding predicate non-SARGable.
std::optional<::orc::Literal> make_long_literal(const Field& field) {
    switch (field.get_type()) {
    case TYPE_TINYINT:
        return ::orc::Literal(static_cast<int64_t>(field.get<TYPE_TINYINT>()));
    case TYPE_SMALLINT:
        return ::orc::Literal(static_cast<int64_t>(field.get<TYPE_SMALLINT>()));
    case TYPE_INT:
        return ::orc::Literal(static_cast<int64_t>(field.get<TYPE_INT>()));
    case TYPE_BIGINT:
        return ::orc::Literal(static_cast<int64_t>(field.get<TYPE_BIGINT>()));
    default:
        return std::nullopt;
    }
}

std::optional<::orc::Literal> make_float_literal(const Field& field) {
    switch (field.get_type()) {
    case TYPE_FLOAT:
        return ::orc::Literal(static_cast<double>(field.get<TYPE_FLOAT>()));
    case TYPE_DOUBLE:
        return ::orc::Literal(field.get<TYPE_DOUBLE>());
    default:
        return std::nullopt;
    }
}

std::optional<::orc::Literal> make_string_literal(const Field& field) {
    if (!is_string_type(field.get_type()) && !is_varbinary(field.get_type())) {
        return std::nullopt;
    }
    const auto value = field.as_string_view();
    return ::orc::Literal(value.data(), value.size());
}

std::optional<::orc::Literal> make_bool_literal(const Field& field) {
    if (field.get_type() != TYPE_BOOLEAN) {
        return std::nullopt;
    }
    return ::orc::Literal(field.get<TYPE_BOOLEAN>() != 0);
}

std::optional<::orc::Literal> make_date_literal(const Field& field) {
    static const cctz::time_zone utc0 = cctz::utc_time_zone();
    switch (field.get_type()) {
    case TYPE_DATE: {
        const auto& date = field.get<TYPE_DATE>();
        const cctz::civil_day civil_date(date.year(), date.month(), date.day());
        const auto day_offset =
                cctz::convert(civil_date, utc0).time_since_epoch().count() / (24 * 60 * 60);
        return ::orc::Literal(::orc::PredicateDataType::DATE, day_offset);
    }
    case TYPE_DATEV2: {
        const auto& date = field.get<TYPE_DATEV2>();
        const cctz::civil_day civil_date(date.year(), date.month(), date.day());
        const auto day_offset =
                cctz::convert(civil_date, utc0).time_since_epoch().count() / (24 * 60 * 60);
        return ::orc::Literal(::orc::PredicateDataType::DATE, day_offset);
    }
    case TYPE_DATETIME: {
        const auto& datetime = field.get<TYPE_DATETIME>();
        if (datetime.hour() != 0 || datetime.minute() != 0 || datetime.second() != 0) {
            return std::nullopt;
        }
        const cctz::civil_day civil_date(datetime.year(), datetime.month(), datetime.day());
        const auto day_offset =
                cctz::convert(civil_date, utc0).time_since_epoch().count() / (24 * 60 * 60);
        return ::orc::Literal(::orc::PredicateDataType::DATE, day_offset);
    }
    case TYPE_DATETIMEV2: {
        const auto& datetime = field.get<TYPE_DATETIMEV2>();
        if (datetime.hour() != 0 || datetime.minute() != 0 || datetime.second() != 0 ||
            datetime.microsecond() != 0) {
            return std::nullopt;
        }
        const cctz::civil_day civil_date(datetime.year(), datetime.month(), datetime.day());
        const auto day_offset =
                cctz::convert(civil_date, utc0).time_since_epoch().count() / (24 * 60 * 60);
        return ::orc::Literal(::orc::PredicateDataType::DATE, day_offset);
    }
    default:
        return std::nullopt;
    }
}

std::optional<::orc::Literal> make_date_literal(int year, int month, int day) {
    static const cctz::time_zone utc0 = cctz::utc_time_zone();
    const cctz::civil_day civil_date(year, month, day);
    const auto day_offset =
            cctz::convert(civil_date, utc0).time_since_epoch().count() / (24 * 60 * 60);
    return ::orc::Literal(::orc::PredicateDataType::DATE, day_offset);
}

struct DateTimeLiteralParts {
    int year = 0;
    int month = 0;
    int day = 0;
    bool has_time = false;
};

std::optional<DateTimeLiteralParts> date_time_literal_parts(const Field& field) {
    switch (field.get_type()) {
    case TYPE_DATETIME: {
        const auto& datetime = field.get<TYPE_DATETIME>();
        return DateTimeLiteralParts {
                .year = datetime.year(),
                .month = datetime.month(),
                .day = datetime.day(),
                .has_time =
                        datetime.hour() != 0 || datetime.minute() != 0 || datetime.second() != 0,
        };
    }
    case TYPE_DATETIMEV2: {
        const auto& datetime = field.get<TYPE_DATETIMEV2>();
        return DateTimeLiteralParts {
                .year = datetime.year(),
                .month = datetime.month(),
                .day = datetime.day(),
                .has_time = datetime.hour() != 0 || datetime.minute() != 0 ||
                            datetime.second() != 0 || datetime.microsecond() != 0,
        };
    }
    default:
        return std::nullopt;
    }
}

std::optional<::orc::Literal> make_timestamp_literal(const Field& field,
                                                     const cctz::time_zone& timezone) {
    switch (field.get_type()) {
    case TYPE_DATETIME: {
        const auto& datetime = field.get<TYPE_DATETIME>();
        const cctz::civil_second civil_seconds(datetime.year(), datetime.month(), datetime.day(),
                                               datetime.hour(), datetime.minute(),
                                               datetime.second());
        return ::orc::Literal(cctz::convert(civil_seconds, timezone).time_since_epoch().count(), 0);
    }
    case TYPE_DATETIMEV2: {
        const auto& datetime = field.get<TYPE_DATETIMEV2>();
        const cctz::civil_second civil_seconds(datetime.year(), datetime.month(), datetime.day(),
                                               datetime.hour(), datetime.minute(),
                                               datetime.second());
        const auto seconds = cctz::convert(civil_seconds, timezone).time_since_epoch().count();
        const auto nanos = cast_set<int32_t>(datetime.microsecond() * 1000);
        return ::orc::Literal(seconds, nanos);
    }
    default:
        return std::nullopt;
    }
}

std::optional<::orc::Literal> make_decimal_literal(const ::orc::Type& orc_type,
                                                   const VLiteral& literal, const Field& field) {
    if (orc_type.getKind() != ::orc::TypeKind::DECIMAL) {
        return std::nullopt;
    }
    const auto& literal_type = literal.get_data_type();
    if (literal_type == nullptr) {
        return std::nullopt;
    }

    Int128 decimal_value = 0;
    switch (field.get_type()) {
    case TYPE_DECIMALV2:
        decimal_value = binary_cast<DecimalV2Value, Int128>(field.get<TYPE_DECIMALV2>());
        break;
    case TYPE_DECIMAL32:
        decimal_value = static_cast<Int128>(field.get<TYPE_DECIMAL32>().value);
        break;
    case TYPE_DECIMAL64:
        decimal_value = static_cast<Int128>(field.get<TYPE_DECIMAL64>().value);
        break;
    case TYPE_DECIMAL128I:
        decimal_value = field.get<TYPE_DECIMAL128I>().value;
        break;
    default:
        return std::nullopt;
    }

    const auto precision = literal_type->get_precision() == 0
                                   ? cast_set<UInt32>(orc_type.getPrecision())
                                   : literal_type->get_precision();
    const auto scale = literal_type->get_scale();
    return ::orc::Literal(::orc::Int128(static_cast<uint64_t>(decimal_value >> 64),
                                        static_cast<uint64_t>(decimal_value)),
                          cast_set<int>(precision), cast_set<int>(scale));
}

std::optional<::orc::Literal> make_orc_literal(const OrcSargColumn& sarg_column, const Field& field,
                                               const cctz::time_zone& timezone) {
    switch (sarg_column.predicate_type) {
    case ::orc::PredicateDataType::LONG:
        return make_long_literal(field);
    case ::orc::PredicateDataType::FLOAT:
        return make_float_literal(field);
    case ::orc::PredicateDataType::STRING:
        return make_string_literal(field);
    case ::orc::PredicateDataType::BOOLEAN:
        return make_bool_literal(field);
    case ::orc::PredicateDataType::DATE:
        return make_date_literal(field);
    case ::orc::PredicateDataType::TIMESTAMP: {
        DORIS_CHECK(sarg_column.orc_type != nullptr);
        static const cctz::time_zone utc0 = cctz::utc_time_zone();
        const auto& literal_timezone =
                sarg_column.orc_type->getKind() == ::orc::TypeKind::TIMESTAMP_INSTANT ? timezone
                                                                                      : utc0;
        return make_timestamp_literal(field, literal_timezone);
    }
    case ::orc::PredicateDataType::DECIMAL:
        return std::nullopt;
    }
    return std::nullopt;
}

std::optional<::orc::Literal> make_orc_literal(const OrcSargColumn& sarg_column,
                                               const VExprSPtr& literal_expr,
                                               const cctz::time_zone& timezone) {
    if (literal_expr == nullptr || !literal_expr->is_literal()) {
        return std::nullopt;
    }
    const auto* literal = dynamic_cast<const VLiteral*>(literal_expr.get());
    if (literal == nullptr || literal->get_column_ptr().get() == nullptr ||
        literal->get_column_ptr()->is_null_at(0)) {
        return std::nullopt;
    }

    Field field;
    literal->get_column_ptr()->get(0, field);
    if (sarg_column.predicate_type == ::orc::PredicateDataType::DECIMAL) {
        if (sarg_column.orc_type == nullptr) {
            return std::nullopt;
        }
        return make_decimal_literal(*sarg_column.orc_type, *literal, field);
    }
    return make_orc_literal(sarg_column, field, timezone);
}

bool is_null_literal(const VExprSPtr& literal_expr) {
    if (literal_expr == nullptr || !literal_expr->is_literal()) {
        return false;
    }
    const auto* literal = dynamic_cast<const VLiteral*>(literal_expr.get());
    return literal != nullptr && literal->get_column_ptr().get() != nullptr &&
           literal->get_column_ptr()->is_null_at(0);
}

// Buildability is checked before emission so the builder path can assume the
// expression shape was validated and use DORIS_CHECK for impossible branches.
bool can_build_search_argument(const format::FileScanRequest& request, const ::orc::Type& root_type,
                               const cctz::time_zone& timezone, const VExprSPtr& expr);

std::optional<VExprSPtr> expression_for_search_argument(const VExprSPtr& expr) {
    if (expr == nullptr) {
        return std::nullopt;
    }

    // Match legacy ORC reader behavior: lower wrapper predicates to the concrete predicate before
    // checking SARG support. This keeps runtime-filter and top-N pruning from regressing in format v2.
    if (expr->is_rf_wrapper()) {
        const auto impl = expr->get_impl();
        if (impl == nullptr) {
            return expr;
        }
        if (const auto* direct_in = dynamic_cast<const VDirectInPredicate*>(impl.get());
            direct_in != nullptr) {
            VExprSPtr in_expr;
            if (!direct_in->get_slot_in_expr(in_expr)) {
                return std::nullopt;
            }
            return in_expr;
        }
        return impl;
    }

    if (expr->is_topn_filter()) {
        const auto* topn_pred = dynamic_cast<const VTopNPred*>(expr.get());
        if (topn_pred == nullptr) {
            return std::nullopt;
        }
        VExprSPtr binary_expr;
        if (!topn_pred->get_binary_expr(binary_expr)) {
            return std::nullopt;
        }
        return binary_expr;
    }

    return expr;
}

std::optional<TExprOpcode::type> reverse_comparison_op(TExprOpcode::type op) {
    switch (op) {
    case TExprOpcode::GE:
        return TExprOpcode::LE;
    case TExprOpcode::GT:
        return TExprOpcode::LT;
    case TExprOpcode::LE:
        return TExprOpcode::GE;
    case TExprOpcode::LT:
        return TExprOpcode::GT;
    case TExprOpcode::EQ:
    case TExprOpcode::NE:
        return op;
    default:
        return std::nullopt;
    }
}

bool is_date_to_datetime_cast_for_sarg(const OrcSargColumn& column, const VExprSPtr& expr) {
    if (column.orc_type == nullptr || column.orc_type->getKind() != ::orc::TypeKind::DATE ||
        expr == nullptr || expr->node_type() != TExprNodeType::CAST_EXPR ||
        expr->data_type() == nullptr) {
        return false;
    }
    const auto target_type = remove_nullable(expr->data_type());
    if (target_type == nullptr) {
        return false;
    }
    const auto target_type_id = target_type->get_primitive_type();
    return target_type_id == TYPE_DATETIME || target_type_id == TYPE_DATETIMEV2;
}

bool is_integer_to_floating_cast_for_sarg(const OrcSargColumn& column, const VExprSPtr& expr) {
    return expr != nullptr && expr->node_type() == TExprNodeType::CAST_EXPR &&
           is_safe_integer_to_floating_cast(column, expr);
}

std::optional<TExprOpcode::type> normalize_date_to_datetime_comparison_op(TExprOpcode::type op,
                                                                          bool literal_has_time) {
    if (!literal_has_time) {
        return op;
    }
    switch (op) {
    case TExprOpcode::GT:
    case TExprOpcode::GE:
        return TExprOpcode::GT;
    case TExprOpcode::LT:
    case TExprOpcode::LE:
        return TExprOpcode::LE;
    default:
        return std::nullopt;
    }
}

std::optional<double> floating_literal_value_for_sarg(const VExprSPtr& literal_expr) {
    const auto field = literal_field_for_sarg(literal_expr);
    if (!field.has_value()) {
        return std::nullopt;
    }
    switch (field->get_type()) {
    case TYPE_FLOAT:
        return static_cast<double>(field->get<TYPE_FLOAT>());
    case TYPE_DOUBLE:
        return field->get<TYPE_DOUBLE>();
    default:
        return std::nullopt;
    }
}

std::optional<int64_t> double_to_int64_boundary(double value, double (*round_func)(double)) {
    if (!std::isfinite(value)) {
        return std::nullopt;
    }
    const auto rounded = round_func(value);
    const auto int64_min = static_cast<double>(std::numeric_limits<int64_t>::min());
    // INT64_MAX rounds to 2^63 as double, so keep the upper bound exclusive.
    const auto int64_max_exclusive = std::ldexp(1.0, 63);
    if (rounded < int64_min || rounded >= int64_max_exclusive) {
        return std::nullopt;
    }
    return static_cast<int64_t>(rounded);
}

std::optional<int64_t> double_to_integral_int64(double value) {
    if (!std::isfinite(value) || std::trunc(value) != value) {
        return std::nullopt;
    }
    return double_to_int64_boundary(value, std::trunc);
}

struct OrcSargComparisonLiteral {
    ::orc::Literal literal;
    TExprOpcode::type normalized_op = TExprOpcode::INVALID_OPCODE;
};

std::optional<OrcSargComparisonLiteral> make_integer_to_floating_comparison_literal(
        TExprOpcode::type normalized_op, double literal_value) {
    std::optional<int64_t> integer_literal;
    TExprOpcode::type integer_op = normalized_op;
    switch (normalized_op) {
    case TExprOpcode::GT:
        integer_literal = double_to_int64_boundary(literal_value, std::floor);
        break;
    case TExprOpcode::GE:
        integer_literal = double_to_int64_boundary(literal_value, std::ceil);
        break;
    case TExprOpcode::LT:
        integer_literal = double_to_int64_boundary(literal_value, std::ceil);
        break;
    case TExprOpcode::LE:
        integer_literal = double_to_int64_boundary(literal_value, std::floor);
        break;
    case TExprOpcode::EQ:
    case TExprOpcode::NE:
        integer_literal = double_to_integral_int64(literal_value);
        break;
    default:
        return std::nullopt;
    }
    if (!integer_literal.has_value()) {
        return std::nullopt;
    }
    return OrcSargComparisonLiteral {
            .literal = ::orc::Literal(*integer_literal),
            .normalized_op = integer_op,
    };
}

std::optional<OrcSargComparisonLiteral> make_comparison_literal_for_sarg(
        const OrcSargColumn& column, const VExprSPtr& source_expr, const VExprSPtr& literal_expr,
        TExprOpcode::type normalized_op, const cctz::time_zone& timezone) {
    if (is_date_to_datetime_cast_for_sarg(column, source_expr)) {
        const auto field = literal_field_for_sarg(literal_expr);
        if (!field.has_value()) {
            return std::nullopt;
        }
        const auto parts = date_time_literal_parts(*field);
        if (!parts.has_value()) {
            return std::nullopt;
        }
        const auto adjusted_op =
                normalize_date_to_datetime_comparison_op(normalized_op, parts->has_time);
        if (!adjusted_op.has_value()) {
            return std::nullopt;
        }
        const auto literal = make_date_literal(parts->year, parts->month, parts->day);
        if (!literal.has_value()) {
            return std::nullopt;
        }
        return OrcSargComparisonLiteral {
                .literal = *literal,
                .normalized_op = *adjusted_op,
        };
    }
    if (is_integer_to_floating_cast_for_sarg(column, source_expr)) {
        const auto literal_value = floating_literal_value_for_sarg(literal_expr);
        if (!literal_value.has_value()) {
            return std::nullopt;
        }
        return make_integer_to_floating_comparison_literal(normalized_op, *literal_value);
    }

    const auto literal = make_orc_literal(column, literal_expr, timezone);
    if (!literal.has_value()) {
        return std::nullopt;
    }
    return OrcSargComparisonLiteral {
            .literal = *literal,
            .normalized_op = normalized_op,
    };
}

std::optional<std::vector<::orc::Literal>> make_in_literals_for_sarg(
        const OrcSargColumn& column, const VExprSPtr& source_expr,
        const std::vector<VExprSPtr>& children, const cctz::time_zone& timezone) {
    if (children.size() < 2) {
        return std::nullopt;
    }

    std::vector<::orc::Literal> literals;
    literals.reserve(children.size() - 1);
    if (is_date_to_datetime_cast_for_sarg(column, source_expr)) {
        for (auto child_it = children.begin() + 1; child_it != children.end(); ++child_it) {
            if (is_null_literal(*child_it)) {
                continue;
            }
            const auto field = literal_field_for_sarg(*child_it);
            if (!field.has_value()) {
                return std::nullopt;
            }
            const auto parts = date_time_literal_parts(*field);
            if (!parts.has_value()) {
                return std::nullopt;
            }
            if (parts->has_time) {
                continue;
            }
            const auto literal = make_date_literal(parts->year, parts->month, parts->day);
            if (!literal.has_value()) {
                return std::nullopt;
            }
            literals.push_back(*literal);
        }
        if (literals.empty()) {
            return std::nullopt;
        }
        return literals;
    }
    if (is_integer_to_floating_cast_for_sarg(column, source_expr)) {
        for (auto child_it = children.begin() + 1; child_it != children.end(); ++child_it) {
            if (is_null_literal(*child_it)) {
                continue;
            }
            const auto literal_value = floating_literal_value_for_sarg(*child_it);
            if (!literal_value.has_value()) {
                return std::nullopt;
            }
            const auto integer_literal = double_to_integral_int64(*literal_value);
            if (!integer_literal.has_value()) {
                continue;
            }
            literals.emplace_back(*integer_literal);
        }
        if (literals.empty()) {
            return std::nullopt;
        }
        return literals;
    }

    for (auto child_it = children.begin() + 1; child_it != children.end(); ++child_it) {
        if (is_null_literal(*child_it)) {
            continue;
        }
        auto literal = make_orc_literal(column, *child_it, timezone);
        if (!literal.has_value()) {
            return std::nullopt;
        }
        literals.push_back(*literal);
    }
    if (literals.empty()) {
        return std::nullopt;
    }
    return literals;
}

std::optional<OrcSargComparison> sarg_comparison_for_expr(const format::FileScanRequest& request,
                                                          const ::orc::Type& root_type,
                                                          const cctz::time_zone& timezone,
                                                          const VExprSPtr& expr) {
    if (expr == nullptr || expr->children().size() != 2) {
        return std::nullopt;
    }

    const VExprSPtr* slot_expr = nullptr;
    const VExprSPtr* literal_expr = nullptr;
    auto normalized_op = expr->op();
    if (sarg_column_for_slot_or_safe_cast(request, root_type, expr->children()[0]).has_value() &&
        expr->children()[1]->is_literal()) {
        slot_expr = &expr->children()[0];
        literal_expr = &expr->children()[1];
    } else if (expr->children()[0]->is_literal() &&
               sarg_column_for_slot_or_safe_cast(request, root_type, expr->children()[1])
                       .has_value()) {
        const auto reversed_op = reverse_comparison_op(expr->op());
        if (!reversed_op.has_value()) {
            return std::nullopt;
        }
        slot_expr = &expr->children()[1];
        literal_expr = &expr->children()[0];
        normalized_op = *reversed_op;
    } else {
        return std::nullopt;
    }

    auto sarg_column = sarg_column_for_slot_or_safe_cast(request, root_type, *slot_expr);
    if (!sarg_column.has_value()) {
        return std::nullopt;
    }
    const auto comparison_literal = make_comparison_literal_for_sarg(
            *sarg_column, *slot_expr, *literal_expr, normalized_op, timezone);
    if (!comparison_literal.has_value()) {
        return std::nullopt;
    }
    return OrcSargComparison {
            .column = *sarg_column,
            .literal = comparison_literal->literal,
            .normalized_op = comparison_literal->normalized_op,
    };
}

bool can_build_slot_literal_predicate(const format::FileScanRequest& request,
                                      const ::orc::Type& root_type, const cctz::time_zone& timezone,
                                      const VExprSPtr& expr) {
    if (expr == nullptr || expr->children().size() != 2) {
        return false;
    }
    return sarg_comparison_for_expr(request, root_type, timezone, expr).has_value();
}

bool can_build_in_predicate(const format::FileScanRequest& request, const ::orc::Type& root_type,
                            const cctz::time_zone& timezone, const VExprSPtr& expr) {
    if (expr == nullptr || expr->children().size() < 2) {
        return false;
    }
    const auto sarg_column =
            sarg_column_for_slot_or_safe_cast(request, root_type, expr->children()[0]);
    if (!sarg_column.has_value()) {
        return false;
    }
    return make_in_literals_for_sarg(*sarg_column, expr->children()[0], expr->children(), timezone)
            .has_value();
}

bool can_build_is_null_predicate(const format::FileScanRequest& request,
                                 const ::orc::Type& root_type, const VExprSPtr& expr) {
    return expr != nullptr && expr->children().size() == 1 &&
           sarg_column_for_slot_or_safe_cast(request, root_type, expr->children()[0]).has_value();
}

std::optional<OrcSargColumn> sarg_column_for_null_safe_equal_null(
        const format::FileScanRequest& request, const ::orc::Type& root_type,
        const VExprSPtr& expr) {
    if (expr == nullptr || expr->node_type() != TExprNodeType::NULL_AWARE_BINARY_PRED ||
        expr->op() != TExprOpcode::EQ_FOR_NULL || expr->children().size() != 2) {
        return std::nullopt;
    }

    auto column_if_other_child_is_null_literal =
            [&](size_t slot_idx, size_t literal_idx) -> std::optional<OrcSargColumn> {
        if (!is_null_literal(expr->children()[literal_idx])) {
            return std::nullopt;
        }
        return sarg_column_for_slot_or_safe_cast(request, root_type, expr->children()[slot_idx]);
    };
    auto column = column_if_other_child_is_null_literal(0, 1);
    if (column.has_value()) {
        return column;
    }
    return column_if_other_child_is_null_literal(1, 0);
}

std::optional<OrcSargComparison> sarg_comparison_for_null_safe_equal_literal(
        const format::FileScanRequest& request, const ::orc::Type& root_type,
        const cctz::time_zone& timezone, const VExprSPtr& expr) {
    if (expr == nullptr || expr->node_type() != TExprNodeType::NULL_AWARE_BINARY_PRED ||
        expr->op() != TExprOpcode::EQ_FOR_NULL || expr->children().size() != 2) {
        return std::nullopt;
    }

    auto comparison_if_other_child_is_non_null_literal =
            [&](size_t slot_idx, size_t literal_idx) -> std::optional<OrcSargComparison> {
        const auto& literal_expr = expr->children()[literal_idx];
        if (literal_expr == nullptr || !literal_expr->is_literal() ||
            is_null_literal(literal_expr)) {
            return std::nullopt;
        }
        const auto& source_expr = expr->children()[slot_idx];
        auto column = sarg_column_for_slot_or_safe_cast(request, root_type, source_expr);
        if (!column.has_value()) {
            return std::nullopt;
        }
        const auto comparison_literal = make_comparison_literal_for_sarg(
                *column, source_expr, literal_expr, TExprOpcode::EQ, timezone);
        if (!comparison_literal.has_value() ||
            comparison_literal->normalized_op != TExprOpcode::EQ) {
            return std::nullopt;
        }
        return OrcSargComparison {
                .column = *column,
                .literal = comparison_literal->literal,
                .normalized_op = TExprOpcode::EQ,
        };
    };
    auto comparison = comparison_if_other_child_is_non_null_literal(0, 1);
    if (comparison.has_value()) {
        return comparison;
    }
    return comparison_if_other_child_is_non_null_literal(1, 0);
}

bool can_build_null_safe_equal_predicate(const format::FileScanRequest& request,
                                         const ::orc::Type& root_type,
                                         const cctz::time_zone& timezone, const VExprSPtr& expr) {
    return sarg_column_for_null_safe_equal_null(request, root_type, expr).has_value() ||
           sarg_comparison_for_null_safe_equal_literal(request, root_type, timezone, expr)
                   .has_value();
}

bool contains_null_safe_equal(const VExprSPtr& expr) {
    const auto sarg_expr = expression_for_search_argument(expr);
    if (!sarg_expr.has_value() || *sarg_expr == nullptr) {
        return false;
    }
    if (sarg_expr->get() != expr.get()) {
        return contains_null_safe_equal(*sarg_expr);
    }
    if ((*sarg_expr)->op() == TExprOpcode::EQ_FOR_NULL) {
        return true;
    }
    return std::ranges::any_of((*sarg_expr)->children(), contains_null_safe_equal);
}

bool can_build_search_argument(const format::FileScanRequest& request, const ::orc::Type& root_type,
                               const cctz::time_zone& timezone, const VExprSPtr& expr) {
    const auto sarg_expr = expression_for_search_argument(expr);
    if (!sarg_expr.has_value()) {
        return false;
    }
    if (*sarg_expr == nullptr) {
        return false;
    }
    if (sarg_expr->get() != expr.get()) {
        return can_build_search_argument(request, root_type, timezone, *sarg_expr);
    }

    switch ((*sarg_expr)->op()) {
    case TExprOpcode::COMPOUND_AND:
        return std::ranges::any_of((*sarg_expr)->children(), [&](const auto& child) {
            return can_build_search_argument(request, root_type, timezone, child);
        });
    case TExprOpcode::COMPOUND_OR:
        if (contains_null_safe_equal(*sarg_expr)) {
            return false;
        }
        return !(*sarg_expr)->children().empty() &&
               std::ranges::all_of((*sarg_expr)->children(), [&](const auto& child) {
                   return can_build_search_argument(request, root_type, timezone, child);
               });
    case TExprOpcode::COMPOUND_NOT:
        if (contains_null_safe_equal(*sarg_expr)) {
            return false;
        }
        return (*sarg_expr)->children().size() == 1 &&
               can_build_search_argument(request, root_type, timezone, (*sarg_expr)->children()[0]);
    case TExprOpcode::GE:
    case TExprOpcode::GT:
    case TExprOpcode::LE:
    case TExprOpcode::LT:
    case TExprOpcode::EQ:
    case TExprOpcode::NE:
        return (*sarg_expr)->node_type() != TExprNodeType::NULL_AWARE_BINARY_PRED &&
               can_build_slot_literal_predicate(request, root_type, timezone, *sarg_expr);
    case TExprOpcode::EQ_FOR_NULL:
        return can_build_null_safe_equal_predicate(request, root_type, timezone, *sarg_expr);
    case TExprOpcode::FILTER_IN:
    case TExprOpcode::FILTER_NOT_IN:
        return (*sarg_expr)->node_type() != TExprNodeType::NULL_AWARE_IN_PRED &&
               can_build_in_predicate(request, root_type, timezone, *sarg_expr);
    case TExprOpcode::INVALID_OPCODE:
        return (*sarg_expr)->node_type() == TExprNodeType::FUNCTION_CALL &&
               ((*sarg_expr)->fn().name.function_name == "is_null_pred" ||
                (*sarg_expr)->fn().name.function_name == "is_not_null_pred") &&
               can_build_is_null_predicate(request, root_type, *sarg_expr);
    default:
        return false;
    }
}

void build_less_than(const OrcSargColumn& column, const ::orc::Literal& literal,
                     std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    builder->lessThan(column.column_id, column.predicate_type, literal);
}

void build_less_than(const OrcSargComparison& comparison,
                     std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    build_less_than(comparison.column, comparison.literal, builder);
}

void build_less_than_equals(const OrcSargColumn& column, const ::orc::Literal& literal,
                            std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    builder->lessThanEquals(column.column_id, column.predicate_type, literal);
}

void build_less_than_equals(const OrcSargComparison& comparison,
                            std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    build_less_than_equals(comparison.column, comparison.literal, builder);
}

void build_equals(const OrcSargColumn& column, const ::orc::Literal& literal,
                  std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    builder->equals(column.column_id, column.predicate_type, literal);
}

void build_equals(const OrcSargComparison& comparison,
                  std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    build_equals(comparison.column, comparison.literal, builder);
}

void build_comparison_predicate(const format::FileScanRequest& request,
                                const ::orc::Type& root_type, const VExprSPtr& expr,
                                const cctz::time_zone& timezone,
                                std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    const auto comparison = *sarg_comparison_for_expr(request, root_type, timezone, expr);
    switch (comparison.normalized_op) {
    case TExprOpcode::GE:
        builder->startNot();
        build_less_than(comparison, builder);
        builder->end();
        return;
    case TExprOpcode::GT:
        builder->startNot();
        build_less_than_equals(comparison, builder);
        builder->end();
        return;
    case TExprOpcode::LE:
        build_less_than_equals(comparison, builder);
        return;
    case TExprOpcode::LT:
        build_less_than(comparison, builder);
        return;
    case TExprOpcode::EQ:
        build_equals(comparison, builder);
        return;
    case TExprOpcode::NE:
        builder->startNot();
        build_equals(comparison, builder);
        builder->end();
        return;
    default:
        DORIS_CHECK(false) << "Unsupported normalized ORC SARG comparison op "
                           << comparison.normalized_op;
    }
}

void build_in_predicate(const format::FileScanRequest& request, const ::orc::Type& root_type,
                        const VExprSPtr& expr, const cctz::time_zone& timezone,
                        std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    const auto sarg_column =
            *sarg_column_for_slot_or_safe_cast(request, root_type, expr->children()[0]);
    auto literals = *make_in_literals_for_sarg(sarg_column, expr->children()[0], expr->children(),
                                               timezone);
    DORIS_CHECK(!literals.empty());
    if (literals.size() == 1) {
        builder->equals(sarg_column.column_id, sarg_column.predicate_type, literals.front());
        return;
    }
    builder->in(sarg_column.column_id, sarg_column.predicate_type, literals);
}

void build_is_null(const format::FileScanRequest& request, const ::orc::Type& root_type,
                   const VExprSPtr& expr, std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    const auto sarg_column =
            *sarg_column_for_slot_or_safe_cast(request, root_type, expr->children()[0]);
    builder->isNull(sarg_column.column_id, sarg_column.predicate_type);
}

void build_null_safe_equal(const format::FileScanRequest& request, const ::orc::Type& root_type,
                           const VExprSPtr& expr, const cctz::time_zone& timezone,
                           std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    const auto sarg_column = sarg_column_for_null_safe_equal_null(request, root_type, expr);
    if (sarg_column.has_value()) {
        builder->isNull(sarg_column->column_id, sarg_column->predicate_type);
        return;
    }
    const auto comparison =
            *sarg_comparison_for_null_safe_equal_literal(request, root_type, timezone, expr);
    build_equals(comparison, builder);
}

bool build_search_argument(const format::FileScanRequest& request, const ::orc::Type& root_type,
                           const cctz::time_zone& timezone, const VExprSPtr& expr,
                           std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    const auto sarg_expr = expression_for_search_argument(expr);
    if (!sarg_expr.has_value() || *sarg_expr == nullptr) {
        return false;
    }
    if (sarg_expr->get() != expr.get()) {
        return build_search_argument(request, root_type, timezone, *sarg_expr, builder);
    }
    if (!can_build_search_argument(request, root_type, timezone, *sarg_expr)) {
        return false;
    }

    switch ((*sarg_expr)->op()) {
    case TExprOpcode::COMPOUND_AND:
        builder->startAnd();
        for (const auto& child : (*sarg_expr)->children()) {
            static_cast<void>(build_search_argument(request, root_type, timezone, child, builder));
        }
        builder->end();
        return true;
    case TExprOpcode::COMPOUND_OR:
        builder->startOr();
        for (const auto& child : (*sarg_expr)->children()) {
            const auto built = build_search_argument(request, root_type, timezone, child, builder);
            DORIS_CHECK(built);
        }
        builder->end();
        return true;
    case TExprOpcode::COMPOUND_NOT:
        builder->startNot();
        DORIS_CHECK(build_search_argument(request, root_type, timezone, (*sarg_expr)->children()[0],
                                          builder));
        builder->end();
        return true;
    case TExprOpcode::GE:
    case TExprOpcode::GT:
    case TExprOpcode::LE:
    case TExprOpcode::LT:
    case TExprOpcode::EQ:
    case TExprOpcode::NE:
        build_comparison_predicate(request, root_type, *sarg_expr, timezone, builder);
        return true;
    case TExprOpcode::EQ_FOR_NULL:
        build_null_safe_equal(request, root_type, *sarg_expr, timezone, builder);
        return true;
    case TExprOpcode::FILTER_IN:
        build_in_predicate(request, root_type, *sarg_expr, timezone, builder);
        return true;
    case TExprOpcode::FILTER_NOT_IN:
        builder->startNot();
        build_in_predicate(request, root_type, *sarg_expr, timezone, builder);
        builder->end();
        return true;
    case TExprOpcode::INVALID_OPCODE:
        if ((*sarg_expr)->fn().name.function_name == "is_null_pred") {
            build_is_null(request, root_type, *sarg_expr, builder);
            return true;
        }
        if ((*sarg_expr)->fn().name.function_name == "is_not_null_pred") {
            builder->startNot();
            build_is_null(request, root_type, *sarg_expr, builder);
            builder->end();
            return true;
        }
        return false;
    default:
        return false;
    }
}

} // namespace

bool build_orc_search_argument(const format::FileScanRequest& request, const ::orc::Type& root_type,
                               const cctz::time_zone& timezone, const VExprSPtr& expr,
                               std::unique_ptr<::orc::SearchArgumentBuilder>& builder) {
    return build_search_argument(request, root_type, timezone, expr, builder);
}

} // namespace doris::format::orc
