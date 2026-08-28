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

#include <cctz/time_zone.h>

#include <algorithm>
#include <cctype>
#include <optional>
#include <set>
#include <string_view>

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

void append_sql_conjunct(std::string_view conjunct, std::string* expression) {
    if (!expression->empty()) {
        expression->append(" AND ");
    }
    expression->append(conjunct);
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

std::optional<std::string> to_lance_sql_literal(const VLiteral& literal) {
    const auto type = remove_nullable(literal.get_data_type());
    auto options = DataTypeSerDe::get_default_format_options();
    auto timezone = cctz::utc_time_zone();
    options.timezone = &timezone;
    const auto value = literal.value(options);
    switch (type->get_primitive_type()) {
    case TYPE_BOOLEAN: {
        const auto normalized = lowercase_ascii(value);
        if (normalized == "0" || normalized == "false") {
            return "FALSE";
        }
        if (normalized == "1" || normalized == "true") {
            return "TRUE";
        }
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
                                                         const DataTypePtr& data_type) {
    if (raw_value == nullptr || data_type == nullptr) {
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
    return to_lance_sql_literal(VLiteral(data_type, field));
}

std::optional<std::string> in_value_to_lance_sql_literal(PrimitiveType primitive_type,
                                                         const void* raw_value,
                                                         const DataTypePtr& data_type) {
#define DISPATCH_IN_VALUE(TYPE) \
    case TYPE:                  \
        return in_value_to_lance_sql_literal<TYPE>(raw_value, data_type)
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

std::optional<std::string> build_in_filter_sql(const VDirectInPredicate& predicate) {
    if (predicate.get_num_children() != 1) {
        return std::nullopt;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(predicate.get_child(0));
    const auto values = predicate.get_set_func();
    if (slot == nullptr || slot->data_type() == nullptr || values == nullptr ||
        values->contain_null() || values->size() == 0) {
        return std::nullopt;
    }

    const auto data_type = remove_nullable(slot->data_type());
    std::string expression("(" + quote_sql_identifier(slot->column_name()) + " IN (");
    auto* iterator = values->begin();
    bool first_value = true;
    while (iterator != nullptr && iterator->has_next()) {
        auto value = in_value_to_lance_sql_literal(data_type->get_primitive_type(),
                                                   iterator->get_value(), data_type);
        if (!value.has_value()) {
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
        return std::nullopt;
    }
    expression.append("))");
    return expression;
}

std::optional<std::string> build_range_filter_sql(const VExpr& predicate) {
    if ((predicate.op() != TExprOpcode::GE && predicate.op() != TExprOpcode::LE) ||
        predicate.get_num_children() != 2) {
        return std::nullopt;
    }
    const auto slot = std::dynamic_pointer_cast<VSlotRef>(predicate.get_child(0));
    const auto literal = std::dynamic_pointer_cast<VLiteral>(predicate.get_child(1));
    if (slot == nullptr || literal == nullptr) {
        return std::nullopt;
    }
    const auto sql_literal = to_lance_sql_literal(*literal);
    if (!sql_literal.has_value()) {
        return std::nullopt;
    }
    const auto* sql_operator = predicate.op() == TExprOpcode::GE ? ">=" : "<=";
    return "(" + quote_sql_identifier(slot->column_name()) + " " + sql_operator + " " +
           *sql_literal + ")";
}

std::optional<std::string> runtime_filter_to_lance_sql(const RuntimeFilterExpr& runtime_filter) {
    const auto impl = runtime_filter.get_impl();
    if (impl == nullptr) {
        return std::nullopt;
    }
    if (const auto* in_predicate = dynamic_cast<const VDirectInPredicate*>(impl.get());
        in_predicate != nullptr) {
        return build_in_filter_sql(*in_predicate);
    }
    return build_range_filter_sql(*impl);
}

std::shared_ptr<const LanceRuntimeFilterSql> build_runtime_filter_sql(
        const VExprContextSPtrs& conjuncts) {
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

        const auto expression = runtime_filter_to_lance_sql(*runtime_filter);
        if (!expression.has_value()) {
            continue;
        }
        append_sql_conjunct(*expression, &result->expression);
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
    // the sorted RF IDs uniquely identify the snapshot shared by its parallel scanners.
    std::set<int> filter_ids;
    for (const auto& conjunct : conjuncts) {
        if (const auto* runtime_filter = get_runtime_filter(conjunct); runtime_filter != nullptr) {
            filter_ids.emplace(runtime_filter->filter_id());
        }
    }
    if (filter_ids.empty()) {
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
        const VExprContextSPtrs& conjuncts, ShardedKVCache* cache) {
    const auto cache_key = build_cache_key(conjuncts);
    if (!cache_key.has_value()) {
        return nullptr;
    }
    if (cache == nullptr) {
        return build_runtime_filter_sql(conjuncts);
    }

    auto* cached = cache->get<std::shared_ptr<const LanceRuntimeFilterSql>>(
            *cache_key, [&]() -> std::shared_ptr<const LanceRuntimeFilterSql>* {
                return new std::shared_ptr<const LanceRuntimeFilterSql>(
                        build_runtime_filter_sql(conjuncts));
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
