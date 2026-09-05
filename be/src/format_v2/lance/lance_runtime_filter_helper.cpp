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

#include "format_v2/lance/lance_runtime_filter_helper.h"

#include <arrow/type.h>
#include <cctz/time_zone.h>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <limits>
#include <optional>
#include <set>
#include <string_view>
#include <system_error>

#include "common/logging.h"
#include "core/data_type/data_type_nullable.h"
#include "core/field.h"
#include "exprs/hybrid_set.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format/format_common.h"
#include "runtime/runtime_profile.h"
#include "util/utf8_check.h"

namespace doris::format::lance {
namespace {

constexpr std::string_view LANCE_RUNTIME_FILTER_CACHE_KEY_PREFIX = "lance-runtime-filter-sql:";

std::string format_filter_ids(const std::vector<int>& filter_ids) {
    std::string result;
    for (const auto filter_id : filter_ids) {
        if (!result.empty()) {
            result.append(",");
        }
        result.append(std::to_string(filter_id));
    }
    return result;
}

std::string quote_sql_identifier(std::string_view identifier) {
    // Lance SQL uses backticks for delimited identifiers. Escape an embedded backtick by doubling
    // it, matching the SQL parser's quoted-identifier syntax.
    std::string quoted("`");
    quoted.reserve(identifier.size() + 2);
    for (const char ch : identifier) {
        if (ch == '`') {
            quoted.append("``");
        } else {
            quoted.push_back(ch);
        }
    }
    quoted.push_back('`');
    return quoted;
}

const RuntimeFilterExpr* get_runtime_filter(const VExprContextSPtr& conjunct) {
    if (conjunct == nullptr || conjunct->root() == nullptr) {
        return nullptr;
    }
    return dynamic_cast<const RuntimeFilterExpr*>(conjunct->root().get());
}

std::string lowercase_ascii(std::string value) {
    std::ranges::transform(value, value.begin(), [](const unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

std::string quote_string_value(std::string_view value) {
    std::string quoted("'");
    quoted.reserve(value.size() + 2);
    for (const char ch : value) {
        if (ch == '\'') {
            quoted.append("''");
        } else {
            quoted.push_back(ch);
        }
    }
    quoted.push_back('\'');
    return quoted;
}

std::shared_ptr<arrow::DataType> find_unique_physical_type(const arrow::Schema& schema,
                                                           std::string_view column_name) {
    const auto fields = schema.GetAllFieldsByName(column_name);
    return fields.size() == 1 ? fields.front()->type() : nullptr;
}

std::optional<int64_t> parse_int64(std::string_view value) {
    int64_t parsed = 0;
    const auto* begin = value.data();
    const auto* end = begin + value.size();
    const auto result = std::from_chars(begin, end, parsed);
    if (result.ec != std::errc() || result.ptr != end) {
        return std::nullopt;
    }
    return parsed;
}

bool integer_literal_fits_decimal_precision(std::string_view value, int32_t precision) {
    if (!value.empty() && (value.front() == '-' || value.front() == '+')) {
        value.remove_prefix(1);
    }
    const auto first_nonzero = value.find_first_not_of('0');
    if (first_nonzero == std::string_view::npos) {
        return true;
    }
    return value.size() - first_nonzero <= static_cast<size_t>(precision);
}

// This checks Lance planner coercion, not whether stored Lance values fit their own Arrow type.
// Unsigned Arrow columns map to wider signed Doris types (for example UInt8 -> SMALLINT), so an RF
// produced from another input may contain a valid Doris value that cannot be coerced back to the
// narrower physical type.
bool integer_literal_fits_physical_type(std::string_view value,
                                        const arrow::DataType& physical_type) {
    const auto parsed = parse_int64(value);
    if (!parsed.has_value()) {
        return false;
    }
    const auto literal = *parsed;
    switch (physical_type.id()) {
    case arrow::Type::INT8:
        return literal >= std::numeric_limits<int8_t>::min() &&
               literal <= std::numeric_limits<int8_t>::max();
    case arrow::Type::UINT8:
        return literal >= 0 && literal <= std::numeric_limits<uint8_t>::max();
    case arrow::Type::INT16:
        return literal >= std::numeric_limits<int16_t>::min() &&
               literal <= std::numeric_limits<int16_t>::max();
    case arrow::Type::UINT16:
        return literal >= 0 && literal <= std::numeric_limits<uint16_t>::max();
    case arrow::Type::INT32:
        return literal >= std::numeric_limits<int32_t>::min() &&
               literal <= std::numeric_limits<int32_t>::max();
    case arrow::Type::UINT32:
        return literal >= 0 && literal <= std::numeric_limits<uint32_t>::max();
    case arrow::Type::INT64:
        return true;
    case arrow::Type::UINT64:
        // The pinned Lance SQL parser first parses a bare integer as i64. Values above INT64_MAX
        // become Float64, which cannot be safely coerced to UInt64.
        return literal >= 0;
    default:
        return false;
    }
}

bool physical_type_supports_literal(PrimitiveType logical_type, std::string_view value,
                                    const arrow::DataType& physical_type) {
    switch (logical_type) {
    case TYPE_BOOLEAN:
        return physical_type.id() == arrow::Type::BOOL;
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
        return integer_literal_fits_physical_type(value, physical_type);
    case TYPE_FLOAT:
        // Arrow HALF_FLOAT and FLOAT both materialize as Doris FLOAT, but the pinned Lance planner
        // cannot coerce an SQL Int64/Float64 literal to Float16.
        return physical_type.id() == arrow::Type::FLOAT;
    case TYPE_DOUBLE:
        return physical_type.id() == arrow::Type::DOUBLE;
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256: {
        if (physical_type.id() != arrow::Type::DECIMAL128 &&
            physical_type.id() != arrow::Type::DECIMAL256) {
            return false;
        }
        const auto& decimal = static_cast<const arrow::DecimalType&>(physical_type);
        // A scale-bearing bare SQL token is parsed as Float64, which the pinned planner cannot
        // coerce to Decimal. Scale-zero values are safe only while they parse exactly as i64.
        return decimal.scale() == 0 && parse_int64(value).has_value() &&
               integer_literal_fits_decimal_precision(value, decimal.precision());
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return (physical_type.id() == arrow::Type::STRING ||
                physical_type.id() == arrow::Type::LARGE_STRING) &&
               value.find('\0') == std::string_view::npos &&
               validate_utf8(value.data(), value.size());
    case TYPE_DATE:
    case TYPE_DATEV2:
        // Date64 is only specified to contain a millisecond count that should be day-aligned.
        // Doris materialization discards any sub-day remainder, so only Date32 is unconditionally
        // comparison-equivalent.
        return physical_type.id() == arrow::Type::DATE32;
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2: {
        if (physical_type.id() != arrow::Type::TIMESTAMP) {
            return false;
        }
        const auto& timestamp = static_cast<const arrow::TimestampType&>(physical_type);
        // Doris DATETIMEV2 stores at most microseconds, so a nanosecond predicate is not equivalent
        // to evaluating the residual after Arrow-to-Doris materialization.
        return timestamp.timezone().empty() && timestamp.unit() != arrow::TimeUnit::NANO;
    }
    default:
        return false;
    }
}

std::optional<std::string> to_lance_sql_literal(const VLiteral& literal,
                                                const arrow::DataType& physical_type) {
    const auto type = remove_nullable(literal.get_data_type());
    auto options = DataTypeSerDe::get_default_format_options();
    auto timezone = cctz::utc_time_zone();
    options.timezone = &timezone;
    const auto value = literal.value(options);
    if (!physical_type_supports_literal(type->get_primitive_type(), value, physical_type)) {
        // The SQL literal would either fail Lance planning or compare different physical values
        // from the Doris residual. Keep the RF residual-only in either case.
        return std::nullopt;
    }
    switch (type->get_primitive_type()) {
    case TYPE_BOOLEAN: {
        const auto normalized = lowercase_ascii(value);
        if (normalized == "0" || normalized == "false") {
            return "FALSE";
        }
        if (normalized == "1" || normalized == "true") {
            return "TRUE";
        }
        // Do not pass an implementation-specific boolean spelling through to Lance SQL.
        return std::nullopt;
    }
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_LARGEINT:
        return value;
    case TYPE_FLOAT:
    case TYPE_DOUBLE: {
        const auto normalized = lowercase_ascii(value);
        if (normalized.find("nan") != std::string::npos ||
            normalized.find("inf") != std::string::npos) {
            // Lance SQL has no portable scalar spelling with Doris-equivalent NaN/Inf semantics.
            return std::nullopt;
        }
        return value;
    }
    case TYPE_DECIMALV2:
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128I:
    case TYPE_DECIMAL256:
        return value;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return quote_string_value(value);
    case TYPE_DATE:
    case TYPE_DATEV2:
        return "DATE " + quote_string_value(value);
    case TYPE_DATETIME:
    case TYPE_DATETIMEV2:
        return "TIMESTAMP " + quote_string_value(value);
    default:
        return std::nullopt;
    }
}

template <PrimitiveType PT>
std::optional<std::string> in_value_to_lance_sql_literal(const void* raw_value,
                                                         const DataTypePtr& data_type,
                                                         const arrow::DataType& physical_type) {
    if (raw_value == nullptr || data_type == nullptr) {
        // A null set entry needs SQL three-valued semantics and cannot be serialized as a scalar.
        return std::nullopt;
    }
    Field field;
    if constexpr (is_string_type(PT)) {
        const auto* value = static_cast<const StringRef*>(raw_value);
        using CppType = typename PrimitiveTypeTraits<PT>::CppType;
        field = Field::create_field<PT>(CppType(value->data, value->size));
    } else {
        using CppType = typename PrimitiveTypeTraits<PT>::CppType;
        field = Field::create_field<PT>(*static_cast<const CppType*>(raw_value));
    }
    return to_lance_sql_literal(VLiteral(data_type, field), physical_type);
}

std::optional<std::string> in_value_to_lance_sql_literal(PrimitiveType primitive_type,
                                                         const void* raw_value,
                                                         const DataTypePtr& data_type,
                                                         const arrow::DataType& physical_type) {
#define DISPATCH_IN_VALUE(TYPE) \
    case TYPE:                  \
        return in_value_to_lance_sql_literal<TYPE>(raw_value, data_type, physical_type)
    switch (primitive_type) {
        DISPATCH_IN_VALUE(TYPE_BOOLEAN);
        DISPATCH_IN_VALUE(TYPE_TINYINT);
        DISPATCH_IN_VALUE(TYPE_SMALLINT);
        DISPATCH_IN_VALUE(TYPE_INT);
        DISPATCH_IN_VALUE(TYPE_BIGINT);
        DISPATCH_IN_VALUE(TYPE_LARGEINT);
        DISPATCH_IN_VALUE(TYPE_FLOAT);
        DISPATCH_IN_VALUE(TYPE_DOUBLE);
        DISPATCH_IN_VALUE(TYPE_DATE);
        DISPATCH_IN_VALUE(TYPE_DATETIME);
        DISPATCH_IN_VALUE(TYPE_DATEV2);
        DISPATCH_IN_VALUE(TYPE_DATETIMEV2);
        DISPATCH_IN_VALUE(TYPE_CHAR);
        DISPATCH_IN_VALUE(TYPE_VARCHAR);
        DISPATCH_IN_VALUE(TYPE_STRING);
        DISPATCH_IN_VALUE(TYPE_DECIMALV2);
        DISPATCH_IN_VALUE(TYPE_DECIMAL32);
        DISPATCH_IN_VALUE(TYPE_DECIMAL64);
        DISPATCH_IN_VALUE(TYPE_DECIMAL128I);
        DISPATCH_IN_VALUE(TYPE_DECIMAL256);
    default:
        return std::nullopt;
    }
#undef DISPATCH_IN_VALUE
}

std::optional<std::string> build_in_filter_sql(const VDirectInPredicate& predicate,
                                               const arrow::Schema& physical_schema) {
    if (predicate.get_num_children() != 1) {
        return std::nullopt;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(predicate.get_child(0));
    const auto values = predicate.get_set_func();
    if (slot == nullptr || slot->data_type() == nullptr || values == nullptr) {
        // Only a direct slot-backed IN set has a stable Lance column/literal representation.
        return std::nullopt;
    }
    if (values->contain_null()) {
        // Dropping NULL would change the Doris IN-filter semantics.
        return std::nullopt;
    }

    const auto data_type = remove_nullable(slot->data_type());
    const auto physical_type = find_unique_physical_type(physical_schema, slot->column_name());
    if (physical_type == nullptr) {
        // A missing or ambiguous top-level field cannot be bound safely by column name.
        return std::nullopt;
    }
    std::string expression("(" + quote_sql_identifier(slot->column_name()) + " IN (");
    auto* iterator = values->begin();
    bool first_value = true;
    while (iterator != nullptr && iterator->has_next()) {
        auto value = in_value_to_lance_sql_literal(
                data_type->get_primitive_type(), iterator->get_value(), data_type, *physical_type);
        if (!value.has_value()) {
            // Partially pushing an IN set would introduce false negatives, so reject it as a unit.
            return std::nullopt;
        }
        if (!first_value) {
            expression.append(", ");
        }
        expression.append(*value);
        first_value = false;
        iterator->next();
    }
    if (first_value) {
        // An empty IN set has no direct Lance SQL representation.
        return std::nullopt;
    }
    expression.append("))");
    return expression;
}

std::optional<std::string> build_range_filter_sql(const VExpr& predicate,
                                                  const arrow::Schema& physical_schema) {
    if ((predicate.op() != TExprOpcode::GE && predicate.op() != TExprOpcode::LE) ||
        predicate.get_num_children() != 2) {
        return std::nullopt;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(predicate.get_child(0));
    const auto literal = std::dynamic_pointer_cast<VLiteral>(predicate.get_child(1));
    if (slot == nullptr || literal == nullptr) {
        // Computed operands are intentionally left to Doris instead of reconstructing expression
        // SQL.
        return std::nullopt;
    }
    const auto physical_type = find_unique_physical_type(physical_schema, slot->column_name());
    if (physical_type == nullptr) {
        // A missing or ambiguous top-level field cannot be bound safely by column name.
        return std::nullopt;
    }
    const auto sql_literal = to_lance_sql_literal(*literal, *physical_type);
    if (!sql_literal.has_value()) {
        // Preserve the Doris residual when its bound has no proven-equivalent physical literal.
        return std::nullopt;
    }
    const auto* sql_operator = predicate.op() == TExprOpcode::GE ? ">=" : "<=";
    return "(" + quote_sql_identifier(slot->column_name()) + " " + sql_operator + " " +
           *sql_literal + ")";
}

std::optional<std::string> runtime_filter_to_lance_sql(const RuntimeFilterExpr& runtime_filter,
                                                       const arrow::Schema& physical_schema) {
    if (runtime_filter.is_null_aware()) {
        // Doris restores probe NULLs to true for null-aware wrappers; ordinary Lance comparisons
        // would discard those rows before the residual can restore them.
        return std::nullopt;
    }
    const auto impl = runtime_filter.get_impl();
    if (impl == nullptr) {
        // A published wrapper without an implementation has nothing safe to translate.
        return std::nullopt;
    }
    if (const auto* in_predicate = dynamic_cast<const VDirectInPredicate*>(impl.get());
        in_predicate != nullptr) {
        return build_in_filter_sql(*in_predicate, physical_schema);
    }
    return build_range_filter_sql(*impl, physical_schema);
}

std::shared_ptr<const LanceRuntimeFilterSql> build_runtime_filter_sql(
        const VExprContextSPtrs& conjuncts, const arrow::Schema& physical_schema) {
    auto result = std::make_shared<LanceRuntimeFilterSql>();
    std::set<int> seen_filter_ids;
    std::set<int> pushed_filter_ids;
    for (const auto& conjunct : conjuncts) {
        const auto* runtime_filter = get_runtime_filter(conjunct);
        if (runtime_filter == nullptr) {
            continue;
        }
        const auto filter_id = runtime_filter->filter_id();
        seen_filter_ids.emplace(filter_id);

        const auto expression = runtime_filter_to_lance_sql(*runtime_filter, physical_schema);
        if (!expression.has_value()) {
            continue;
        }
        if (!result->expression.empty()) {
            result->expression.append(" AND ");
        }
        result->expression.append(*expression);
        pushed_filter_ids.emplace(filter_id);
    }

    result->pushable_filter_ids.assign(pushed_filter_ids.begin(), pushed_filter_ids.end());
    for (const auto filter_id : seen_filter_ids) {
        if (!pushed_filter_ids.contains(filter_id)) {
            result->skipped_filter_ids.emplace_back(filter_id);
        }
    }
    return result;
}

std::optional<std::string> build_cache_key(const VExprContextSPtrs& conjuncts) {
    // This cache is scoped to one FileScanLocalState. An RF is immutable after it is published, so
    // its sorted IDs identify the snapshot shared by parallel readers.
    std::set<int> filter_ids;
    for (const auto& conjunct : conjuncts) {
        if (const auto* runtime_filter = get_runtime_filter(conjunct); runtime_filter != nullptr) {
            filter_ids.emplace(runtime_filter->filter_id());
        }
    }
    if (filter_ids.empty()) {
        // Avoid allocating a cache entry for conjunct snapshots that contain no runtime filters.
        return std::nullopt;
    }

    std::string key(LANCE_RUNTIME_FILTER_CACHE_KEY_PREFIX);
    for (const auto filter_id : filter_ids) {
        key.append(std::to_string(filter_id)).append(",");
    }
    return key;
}

} // namespace

std::shared_ptr<const LanceRuntimeFilterSql> get_or_create_lance_runtime_filter_sql(
        const VExprContextSPtrs& conjuncts, const arrow::Schema& physical_schema,
        ShardedKVCache* cache) {
    const auto cache_key = build_cache_key(conjuncts);
    if (!cache_key.has_value()) {
        return nullptr;
    }
    if (cache == nullptr) {
        return build_runtime_filter_sql(conjuncts, physical_schema);
    }

    auto* cached = cache->get<std::shared_ptr<const LanceRuntimeFilterSql>>(
            *cache_key, [&]() -> std::shared_ptr<const LanceRuntimeFilterSql>* {
                return new std::shared_ptr<const LanceRuntimeFilterSql>(
                        build_runtime_filter_sql(conjuncts, physical_schema));
            });
    return cached == nullptr ? nullptr : *cached;
}

void record_lance_runtime_filter_pushdown(RuntimeProfile* profile,
                                          const LanceRuntimeFilterSql& runtime_filter_sql) {
    DORIS_CHECK(profile != nullptr);
    const auto pushed_ids = format_filter_ids(runtime_filter_sql.pushable_filter_ids);
    const auto skipped_ids = format_filter_ids(runtime_filter_sql.skipped_filter_ids);

    if (!pushed_ids.empty()) {
        profile->add_info_string("LanceRuntimeFilterPushedIds", pushed_ids);
    }
    if (!skipped_ids.empty()) {
        profile->add_info_string("LanceRuntimeFilterSkippedIds", skipped_ids);
    }
    VLOG_DEBUG << "Lance runtime filter pushdown: pushed_ids=[" << pushed_ids << "], skipped_ids=["
               << skipped_ids << "]";
}

} // namespace doris::format::lance
