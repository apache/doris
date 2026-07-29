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

#include "format/table/paimon_rust_predicate_converter.h"

#include <algorithm>
#include <cctype>
#include <memory>
#include <utility>

#include "common/logging.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/decimalv2_value.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr.h"
#include "exprs/vin_predicate.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"

namespace doris {

namespace {
// paimon_datum tags (see paimon.h / bindings/c/src/table.rs::datum_from_c).
constexpr int32_t kTagBool = 0;
constexpr int32_t kTagTinyInt = 1;
constexpr int32_t kTagSmallInt = 2;
constexpr int32_t kTagInt = 3;
constexpr int32_t kTagLong = 4;
constexpr int32_t kTagDouble = 6;
constexpr int32_t kTagString = 7;
constexpr int32_t kTagDate = 8;
constexpr int32_t kTagTimestamp = 10;
constexpr int32_t kTagDecimal = 12;
constexpr int32_t kTagBytes = 13;

// paimon decimal precision ceiling (paimon::Decimal::MAX_PRECISION).
constexpr int32_t kPaimonDecimalMaxPrecision = 38;

// RAII for an owned paimon_predicate*. and/or/not consume their inputs, so we
// release() before handing pointers to them.
struct predicate_deleter {
    void operator()(paimon_predicate* p) const {
        if (p) {
            paimon_predicate_free(p);
        }
    }
};
using predicate_ptr = std::unique_ptr<paimon_predicate, predicate_deleter>;

// RAII for an owned paimon_error*.
struct error_deleter {
    void operator()(paimon_error* p) const {
        if (p) {
            paimon_error_free(p);
        }
    }
};
using error_ptr = std::unique_ptr<paimon_error, error_deleter>;

// Render a paimon_error into a string. Takes ownership of `err` via RAII so it
// is freed on every return path. Safe to call with nullptr.
std::string consume_predicate_error(paimon_error* err) {
    error_ptr owned(err);
    if (!owned) {
        return "unknown error";
    }
    std::string msg;
    if (owned->message.data != nullptr && owned->message.len > 0) {
        msg.assign(reinterpret_cast<const char*>(owned->message.data), owned->message.len);
    }
    return "code=" + std::to_string(owned->code) + ", msg=" + msg;
}
} // namespace

PaimonRustPredicateConverter::PaimonRustPredicateConverter(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
        const paimon_table* table)
        : _state(state), _table(table) {
    _file_columns.reserve(file_slot_descs.size());
    for (const auto& slot : file_slot_descs) {
        _file_columns.insert(_normalize_name(slot->col_name()));
    }
    if (!TimezoneUtils::find_cctz_time_zone("GMT", _gmt_tz)) {
        TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _gmt_tz);
    }
}

paimon_predicate* PaimonRustPredicateConverter::build(const VExprContextSPtrs& conjuncts) {
    if (_table == nullptr) {
        return nullptr;
    }
    predicate_ptr result;
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
        predicate_ptr pred(_convert_expr(root));
        if (!pred) {
            continue;
        }
        if (!result) {
            result = std::move(pred);
        } else {
            // and consumes both inputs regardless of success.
            result.reset(paimon_predicate_and(result.release(), pred.release()));
            if (!result) {
                return nullptr;
            }
        }
    }
    return result.release();
}

paimon_predicate* PaimonRustPredicateConverter::_convert_expr(const VExprSPtr& expr) {
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

paimon_predicate* PaimonRustPredicateConverter::_convert_compound(const VExprSPtr& expr) {
    if (!expr || expr->get_num_children() != 2) {
        return nullptr;
    }
    predicate_ptr left(_convert_expr(expr->get_child(0)));
    if (!left) {
        return nullptr;
    }
    predicate_ptr right(_convert_expr(expr->get_child(1)));
    if (!right) {
        return nullptr;
    }

    if (expr->op() == TExprOpcode::COMPOUND_AND) {
        return paimon_predicate_and(left.release(), right.release());
    }
    if (expr->op() == TExprOpcode::COMPOUND_OR) {
        return paimon_predicate_or(left.release(), right.release());
    }
    return nullptr;
}

paimon_predicate* PaimonRustPredicateConverter::_convert_in(const VExprSPtr& expr) {
    auto* in_pred = dynamic_cast<VInPredicate*>(expr.get());
    if (!in_pred || expr->get_num_children() < 2) {
        return nullptr;
    }
    auto field_meta = _resolve_field(expr->get_child(0));
    if (!field_meta) {
        return nullptr;
    }

    const auto num_values = expr->get_num_children() - 1;
    // Reserve up front so the backing strings never reallocate: each datum's
    // str_data points into storages[i], which must stay stable.
    std::vector<std::string> storages;
    std::vector<paimon_datum> datums;
    storages.reserve(num_values);
    datums.reserve(num_values);
    for (uint16_t i = 1; i < expr->get_num_children(); ++i) {
        auto holder = _convert_literal(expr->get_child(i), *field_meta->slot_desc);
        if (!holder) {
            return nullptr;
        }
        storages.emplace_back(std::move(holder->storage));
        paimon_datum datum = holder->datum;
        _bind_datum_storage(&datum, storages.back());
        datums.emplace_back(datum);
    }

    if (datums.empty()) {
        return nullptr;
    }
    if (in_pred->is_not_in()) {
        return _take(paimon_predicate_is_not_in(_table, field_meta->column.c_str(), datums.data(),
                                                datums.size()));
    }
    return _take(paimon_predicate_is_in(_table, field_meta->column.c_str(), datums.data(),
                                        datums.size()));
}

paimon_predicate* PaimonRustPredicateConverter::_convert_binary(const VExprSPtr& expr) {
    if (!expr || expr->get_num_children() != 2) {
        return nullptr;
    }
    auto field_meta = _resolve_field(expr->get_child(0));
    if (!field_meta) {
        return nullptr;
    }
    const char* column = field_meta->column.c_str();

    if (expr->op() == TExprOpcode::EQ_FOR_NULL) {
        return _take(paimon_predicate_is_null(_table, column));
    }

    auto holder = _convert_literal(expr->get_child(1), *field_meta->slot_desc);
    if (!holder) {
        return nullptr;
    }
    // `holder` is a local, so its storage stays put for the duration of the call.
    _bind_datum_storage(&holder->datum, holder->storage);
    const paimon_datum& datum = holder->datum;

    switch (expr->op()) {
    case TExprOpcode::EQ:
        return _take(paimon_predicate_equal(_table, column, datum));
    case TExprOpcode::NE:
        return _take(paimon_predicate_not_equal(_table, column, datum));
    case TExprOpcode::GE:
        return _take(paimon_predicate_greater_or_equal(_table, column, datum));
    case TExprOpcode::GT:
        return _take(paimon_predicate_greater_than(_table, column, datum));
    case TExprOpcode::LE:
        return _take(paimon_predicate_less_or_equal(_table, column, datum));
    case TExprOpcode::LT:
        return _take(paimon_predicate_less_than(_table, column, datum));
    default:
        break;
    }
    return nullptr;
}

paimon_predicate* PaimonRustPredicateConverter::_convert_is_null(const VExprSPtr& expr,
                                                                 const std::string& fn_name) {
    if (!expr || expr->get_num_children() != 1) {
        return nullptr;
    }
    auto field_meta = _resolve_field(expr->get_child(0));
    if (!field_meta) {
        return nullptr;
    }
    if (fn_name == "is_not_null_pred") {
        return _take(paimon_predicate_is_not_null(_table, field_meta->column.c_str()));
    }
    return _take(paimon_predicate_is_null(_table, field_meta->column.c_str()));
}

paimon_predicate* PaimonRustPredicateConverter::_convert_like_prefix(const VExprSPtr& expr) {
    if (!expr || expr->get_num_children() != 2) {
        return nullptr;
    }
    auto field_meta = _resolve_field(expr->get_child(0));
    if (!field_meta ||
        !_is_string_type(field_meta->slot_desc->type()->get_primitive_type())) {
        return nullptr;
    }

    auto pattern_opt = _extract_string_literal(expr->get_child(1));
    if (!pattern_opt) {
        return nullptr;
    }
    const std::string& pattern = *pattern_opt;
    // Only prefix matches (`abc%`) are convertible to a range scan.
    if (!pattern.empty() && pattern.front() == '%') {
        return nullptr;
    }
    if (pattern.empty() || pattern.back() != '%') {
        return nullptr;
    }

    const char* column = field_meta->column.c_str();
    std::string prefix = pattern.substr(0, pattern.size() - 1);

    // lower bound: column >= prefix
    paimon_datum lower {};
    lower.tag = kTagString;
    _bind_datum_storage(&lower, prefix);
    predicate_ptr lower_pred(_take(paimon_predicate_greater_or_equal(_table, column, lower)));
    if (!lower_pred) {
        return nullptr;
    }

    auto upper_prefix = _next_prefix(prefix);
    if (!upper_prefix) {
        return lower_pred.release();
    }

    // upper bound: column < next_prefix
    paimon_datum upper {};
    upper.tag = kTagString;
    _bind_datum_storage(&upper, *upper_prefix);
    predicate_ptr upper_pred(_take(paimon_predicate_less_than(_table, column, upper)));
    if (!upper_pred) {
        // No usable upper bound: fall back to the (still correct) lower bound.
        return lower_pred.release();
    }
    return paimon_predicate_and(lower_pred.release(), upper_pred.release());
}

std::optional<PaimonRustPredicateConverter::FieldMeta> PaimonRustPredicateConverter::_resolve_field(
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
    if (_file_columns.find(_normalize_name(slot_desc->col_name())) == _file_columns.end()) {
        return std::nullopt;
    }
    auto slot_type = slot_desc->type();
    if (!_is_supported_slot_type(slot_type->get_primitive_type(), slot_type->get_precision())) {
        return std::nullopt;
    }
    return FieldMeta {slot_desc->col_name(), slot_desc};
}

std::optional<PaimonRustPredicateConverter::DatumHolder>
PaimonRustPredicateConverter::_convert_literal(const VExprSPtr& expr,
                                               const SlotDescriptor& slot_desc) const {
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

    DatumHolder holder;
    paimon_datum& datum = holder.datum;

    switch (slot_primitive) {
    case TYPE_BOOLEAN: {
        if (literal_primitive != TYPE_BOOLEAN) {
            return std::nullopt;
        }
        datum.tag = kTagBool;
        datum.int_val = static_cast<bool>(field.get<TYPE_BOOLEAN>()) ? 1 : 0;
        return holder;
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
        datum.int_val = value;
        switch (slot_primitive) {
        case TYPE_TINYINT:
            datum.tag = kTagTinyInt;
            break;
        case TYPE_SMALLINT:
            datum.tag = kTagSmallInt;
            break;
        case TYPE_INT:
            datum.tag = kTagInt;
            break;
        default:
            datum.tag = kTagLong;
            break;
        }
        return holder;
    }
    case TYPE_DOUBLE: {
        if (literal_primitive != TYPE_DOUBLE && literal_primitive != TYPE_FLOAT) {
            return std::nullopt;
        }
        datum.tag = kTagDouble;
        datum.double_val = literal_primitive == TYPE_FLOAT
                                   ? static_cast<double>(field.get<TYPE_FLOAT>())
                                   : field.get<TYPE_DOUBLE>();
        return holder;
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
        } else {
            const auto& dt = field.get<TYPE_DATEV2>();
            if (!dt.is_valid_date()) {
                return std::nullopt;
            }
            dt.unix_timestamp(&seconds, _gmt_tz);
        }
        datum.tag = kTagDate;
        datum.int_val = _seconds_to_days(seconds);
        return holder;
    }
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2: {
        if (!_is_datetime_type(literal_primitive)) {
            return std::nullopt;
        }
        datum.tag = kTagTimestamp;
        // nanos is left at 0 to match paimon-cpp's millisecond-granularity
        // Timestamp::FromEpochMillis behaviour.
        if (literal_primitive == TYPE_DATETIME) {
            const auto& dt = field.get<TYPE_DATETIME>();
            if (!dt.is_valid_date()) {
                return std::nullopt;
            }
            int64_t seconds = 0;
            dt.unix_timestamp(&seconds, _gmt_tz);
            datum.int_val = seconds * 1000;
        } else {
            const auto& dt = field.get<TYPE_DATETIMEV2>();
            if (!dt.is_valid_date()) {
                return std::nullopt;
            }
            std::pair<int64_t, int64_t> ts;
            dt.unix_timestamp(&ts, _gmt_tz);
            datum.int_val = ts.first * 1000 + ts.second / 1000;
        }
        return holder;
    }
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        if (!_is_string_type(literal_primitive)) {
            return std::nullopt;
        }
        const auto& value = field.get<TYPE_STRING>();
        datum.tag = kTagString;
        holder.storage.assign(value.data(), value.size());
        return holder;
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
        if (precision <= 0 || precision > kPaimonDecimalMaxPrecision) {
            return std::nullopt;
        }

        __int128 value = 0;
        switch (literal_primitive) {
        case TYPE_DECIMALV2:
            value = field.get<TYPE_DECIMALV2>().value();
            break;
        case TYPE_DECIMAL32:
            value = field.get<TYPE_DECIMAL32>().value;
            break;
        case TYPE_DECIMAL64:
            value = field.get<TYPE_DECIMAL64>().value;
            break;
        case TYPE_DECIMAL128I:
            value = field.get<TYPE_DECIMAL128I>().value;
            break;
        default:
            return std::nullopt;
        }
        datum.tag = kTagDecimal;
        // rust reassembles as ((int_val2 as i128) << 64) | (int_val as u64 as i128):
        // int_val holds the low 64 bits, int_val2 the (sign-extended) high 64 bits.
        datum.int_val = static_cast<int64_t>(static_cast<uint64_t>(value));
        datum.int_val2 = static_cast<int64_t>(value >> 64);
        datum.uint_val = static_cast<uint32_t>(precision);
        datum.uint_val2 = static_cast<uint32_t>(scale);
        return holder;
    }
    default:
        break;
    }
    return std::nullopt;
}

std::optional<std::string> PaimonRustPredicateConverter::_extract_string_literal(
        const VExprSPtr& expr) const {
    auto literal_expr = VExpr::expr_without_cast(expr);
    auto* literal = dynamic_cast<VLiteral*>(literal_expr.get());
    if (!literal) {
        return std::nullopt;
    }
    auto literal_type = remove_nullable(literal->get_data_type());
    if (!_is_string_type(literal_type->get_primitive_type())) {
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
    return std::string(value.data(), value.size());
}

paimon_predicate* PaimonRustPredicateConverter::_take(paimon_result_predicate result) {
    if (result.error != nullptr) {
        // A single leaf failing to build is not fatal: that conjunct is simply
        // dropped from the pushed-down filter and the engine still re-applies it.
        // Log at WARNING so it is visible without verbose logging enabled.
        LOG(WARNING) << "paimon-rust build predicate failed: "
                     << consume_predicate_error(result.error);
        return nullptr;
    }
    return result.predicate;
}

void PaimonRustPredicateConverter::_bind_datum_storage(paimon_datum* datum,
                                                       const std::string& storage) {
    if (datum->tag == kTagString || datum->tag == kTagBytes) {
        datum->str_data = reinterpret_cast<const uint8_t*>(storage.data());
        datum->str_len = storage.size();
    }
}

std::string PaimonRustPredicateConverter::_normalize_name(std::string_view name) {
    std::string out(name);
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

std::optional<std::string> PaimonRustPredicateConverter::_next_prefix(const std::string& prefix) {
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

int32_t PaimonRustPredicateConverter::_seconds_to_days(int64_t seconds) {
    static constexpr int64_t kSecondsPerDay = 24 * 60 * 60;
    int64_t days = seconds / kSecondsPerDay;
    if (seconds < 0 && seconds % kSecondsPerDay != 0) {
        --days;
    }
    return static_cast<int32_t>(days);
}

bool PaimonRustPredicateConverter::_is_integer_type(PrimitiveType type) {
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

bool PaimonRustPredicateConverter::_is_string_type(PrimitiveType type) {
    return type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_STRING;
}

bool PaimonRustPredicateConverter::_is_decimal_type(PrimitiveType type) {
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

bool PaimonRustPredicateConverter::_is_date_type(PrimitiveType type) {
    return type == TYPE_DATE || type == TYPE_DATEV2;
}

bool PaimonRustPredicateConverter::_is_datetime_type(PrimitiveType type) {
    return type == TYPE_DATETIME || type == TYPE_DATETIMEV2;
}

bool PaimonRustPredicateConverter::_is_supported_slot_type(PrimitiveType type, uint32_t precision) {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_DOUBLE:
    case TYPE_VARCHAR:
    case TYPE_STRING:
    case TYPE_DATE:
    case TYPE_DATEV2:
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2:
        return true;
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        // precision == 0 means "unset"; only a positive precision above the
        // paimon ceiling is unrepresentable.
        return precision <= static_cast<uint32_t>(kPaimonDecimalMaxPrecision);
    case TYPE_FLOAT:
    case TYPE_CHAR:
    default:
        return false;
    }
}

} // namespace doris
