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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/sink/tablet_column_validator.h"

#include <algorithm>
#include <string>
#include <type_traits>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/binary_cast.hpp"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/wide_integer_to_string.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"
#include "util/defer_op.h"

namespace doris {

Status TabletValidateContext::reject(size_t row, const fmt::memory_buffer& message) const {
    filter_map[row] = true;
    return state->append_error_msg_to_file(
            []() -> std::string { return ""; },
            [&]() { return fmt::to_string(error_prefix) + fmt::to_string(message); });
}

namespace {

template <bool is_min>
DecimalV2Value get_decimalv2_min_or_max(TabletValidateContext& context, const DataTypePtr& type) {
    auto& cache = is_min ? context.min_decimalv2_val : context.max_decimalv2_val;
    const auto precision = type->get_precision();
    const auto scale = type->get_scale();
    const auto key = std::pair {precision, scale};
    auto iter = cache.find(key);
    if (iter != cache.end()) {
        return iter->second;
    }
    DecimalV2Value value;
    if constexpr (is_min) {
        value.to_min_decimal(precision, scale);
    } else {
        value.to_max_decimal(precision, scale);
    }
    cache.emplace(key, value);
    return value;
}

template <typename DecimalType, bool IsMin>
DecimalType get_decimalv3_min_or_max(TabletValidateContext& context, const DataTypePtr& type) {
    std::map<int, typename DecimalType::NativeType>* pmap;
    if constexpr (std::is_same_v<DecimalType, Decimal32>) {
        pmap = IsMin ? &context.min_decimal32_val : &context.max_decimal32_val;
    } else if constexpr (std::is_same_v<DecimalType, Decimal64>) {
        pmap = IsMin ? &context.min_decimal64_val : &context.max_decimal64_val;
    } else if constexpr (std::is_same_v<DecimalType, Decimal128V3>) {
        pmap = IsMin ? &context.min_decimal128_val : &context.max_decimal128_val;
    } else {
        pmap = IsMin ? &context.min_decimal256_val : &context.max_decimal256_val;
    }

    // found
    auto iter = pmap->find(type->get_precision());
    if (iter != pmap->end()) {
        return DecimalType(iter->second);
    }

    DecimalType value;
    if constexpr (IsMin) {
        value = min_decimal_value<DecimalType::PType>(type->get_precision());
    } else {
        value = max_decimal_value<DecimalType::PType>(type->get_precision());
    }
    pmap->emplace(type->get_precision(), value.value);
    return value;
}

// Borrowed views: local positions map to rows in the original output block.
// null_map includes NULLs inherited from enclosing nullable containers.
struct ValidationRows {
    const IColumn::Permutation* rows = nullptr;
    const UInt8* null_map = nullptr;

    size_t original_row(size_t i) const { return rows ? (*rows)[i] : i; }

    bool should_validate(size_t i, const TabletValidateContext& context) const {
        return !context.filter_map[original_row(i)] && (!null_map || !null_map[i]);
    }
};

Status validate_column(MutableColumnPtr& column, const DataTypePtr& type,
                       TabletValidateContext& context, const ValidationRows& rows);

// The public entry has already detached the tree. Transfer an existing unique
// owner across a ColumnPtr storage boundary without running COW again.
MutableColumnPtr take_writable_column(ColumnPtr& column) {
    auto writable = column->assert_mutable();
    column.reset();
    return writable;
}

Status validate_child(ColumnPtr& slot, const DataTypePtr& type, TabletValidateContext& context,
                      const ValidationRows& rows) {
    auto child = take_writable_column(slot);
    // Keep the parent's structure intact on non-OK Status and exception paths too.
    Defer restore_child {[&] { slot = std::move(child); }};
    return validate_column(child, type, context, rows);
}

Status validate_string(MutableColumnPtr& column, const DataTypePtr& type,
                       TabletValidateContext& context, const ValidationRows& rows) {
    int limit = config::string_type_length_soft_limit_bytes;
    int len = -1;
    const auto* type_str = check_and_get_data_type<DataTypeString>(type.get());
    if (type_str && type_str->len() >= 0) {
        len = type_str->len();
        limit = std::min(limit, len);
    }

    const auto& strings = assert_cast<const ColumnString&>(*column);
    const auto& offsets = strings.get_offsets();
    size_t invalid_count = 0;
    for (size_t i = 0; i < column->size(); ++i) {
        invalid_count += rows.should_validate(i, context) && offsets[i] - offsets[i - 1] > limit;
    }
    if (!invalid_count) {
        return Status::OK();
    }

    // Non-strict loads truncate in characters, but the tablet limit is in bytes.
    // For example, two Chinese characters can still exceed a VARCHAR(2) byte limit.
    if (type_str && !context.state->enable_insert_strict()) {
        auto int_type =
                DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_INT, 0, 0);
        ColumnsWithTypeAndName arguments {{nullptr, type, "string column"},
                                          {nullptr, int_type, "pos"},
                                          {nullptr, int_type, "len"}};
        auto function = SimpleFunctionFactory::instance().get_function(
                "substring", arguments, type, {}, context.state->be_exec_version());
        if (!function) {
            return Status::InternalError("get function substring failed");
        }
        ColumnPtr result;
        {
            const auto size = column->size();
            auto pos = int_type->create_column_const(size, to_field<TYPE_INT>(1));
            auto length = int_type->create_column_const(size, to_field<TYPE_INT>(limit));
            Block block({{column->get_ptr(), type, "string column"},
                         {pos, int_type, "pos"},
                         {length, int_type, "len"},
                         {nullptr, type, "result"}});
            RETURN_IF_ERROR(function->execute(nullptr, block, {0, 1, 2}, 3, size));
            result = std::move(block.get_by_position(3).column);
        }
        // Drop the function's borrowed input owners before using the replacement.
        column = take_writable_column(result);
    }

    const auto& checked = assert_cast<const ColumnString&>(*column);
    for (size_t i = 0; i < column->size(); ++i) {
        if (!rows.should_validate(i, context)) {
            continue;
        }
        auto value = checked.get_data_at(i);
        if (std::cmp_greater(value.size, static_cast<size_t>(limit))) {
            fmt::memory_buffer message;
            if (std::cmp_greater(value.size, static_cast<size_t>(len))) {
                fmt::format_to(message, "the length of input is too long than schema. ");
                fmt::format_to(message, "first 32 bytes of input str: [{}] ", value.to_prefix(32));
                fmt::format_to(message, "schema length: {}; actual length: {}; ", len, value.size);
            } else {
                fmt::format_to(message, "the length of input string is too long than vec schema. ");
                fmt::format_to(message, "first 32 bytes of input str: [{}] ", value.to_prefix(32));
                fmt::format_to(message, "schema length: {}; limit length: {}; actual length: {}; ",
                               len, limit, value.size);
            }
            RETURN_IF_ERROR(context.reject(rows.original_row(i), message));
        }
    }
    return Status::OK();
}

Status validate_jsonb(const ColumnString& column, TabletValidateContext& context,
                      const ValidationRows& rows) {
    for (size_t i = 0; i < column.size(); ++i) {
        if (rows.should_validate(i, context) && column.get_data_at(i).size == 0) {
            fmt::memory_buffer message;
            fmt::format_to(message, "jsonb with size 0 is invalid");
            RETURN_IF_ERROR(context.reject(rows.original_row(i), message));
        }
    }
    return Status::OK();
}

Status validate_decimal_v2(ColumnDecimal128V2& column, const DataTypePtr& type,
                           TabletValidateContext& context, const ValidationRows& rows) {
    // DecimalV2 is deprecated. Preserve its historical validation using execution
    // precision/scale (27, 9), rather than the original schema parameters. For
    // example, DECIMAL(9, 2) still validates 1.235 at scale 9 here; enforcing the
    // declared scale and range is outside this COW refactoring.
    const auto precision = type->get_precision();
    const auto scale = type->get_scale();
    const auto max = get_decimalv2_min_or_max<false>(context, type);
    const auto min = get_decimalv2_min_or_max<true>(context, type);
    for (size_t i = 0; i < column.size(); ++i) {
        if (!rows.should_validate(i, context)) {
            continue;
        }
        auto value = binary_cast<Int128, DecimalV2Value>(column.get_data()[i]);
        fmt::memory_buffer message;
        if (value.greater_than_scale(scale)) {
            auto code = value.round(&value, scale, HALF_UP);
            column.get_data()[i] = value;
            if (code != E_DEC_OK) {
                fmt::format_to(message, "round one decimal failed.value={}; ", value.to_string());
            }
        }
        if (value > max || value < min) {
            fmt::format_to(message,
                           "decimal value is not valid for definition, value={}, "
                           "precision={}, scale={}, min={}, max={}; ",
                           value.to_string(), precision, scale, min.to_string(), max.to_string());
        }
        if (message.size()) {
            RETURN_IF_ERROR(context.reject(rows.original_row(i), message));
        }
    }
    return Status::OK();
}

// DecimalV3 needs only a range check; use one template instead of four macro bodies.
template <typename DecimalType>
Status validate_decimal_v3(const ColumnDecimal<DecimalType::PType>& column, const DataTypePtr& type,
                           TabletValidateContext& context, const ValidationRows& rows) {
    const auto max = get_decimalv3_min_or_max<DecimalType, false>(context, type);
    const auto min = get_decimalv3_min_or_max<DecimalType, true>(context, type);
    size_t invalid_count = 0;
    for (const auto value : column.get_data()) {
        invalid_count += value > max || value < min;
    }
    if (!invalid_count) {
        return Status::OK();
    }
    for (size_t i = 0; i < column.size(); ++i) {
        if (!rows.should_validate(i, context)) {
            continue;
        }
        const auto value = column.get_data()[i];
        if (value > max || value < min) {
            fmt::memory_buffer message;
            fmt::format_to(message,
                           "decimal value is not valid for definition, value={}, "
                           "precision={}, scale={}, min={}, max={}; ",
                           value.value, type->get_precision(), type->get_scale(), min.value,
                           max.value);
            RETURN_IF_ERROR(context.reject(rows.original_row(i), message));
        }
    }
    return Status::OK();
}

// For [NULL, [x, y]], both x and y map to outer row 1; elements underneath a
// NULL parent are skipped even if their physical payload happens to be invalid.
struct NestedRows {
    IColumn::Permutation rows;
    NullMap null_map;

    NestedRows(const ColumnArray::Offsets64& offsets, const ValidationRows& parent)
            : rows(offsets.back()) {
        if (parent.null_map) {
            null_map.resize(rows.size());
        }
        for (size_t i = 0; i < offsets.size(); ++i) {
            for (size_t j = offsets[i - 1]; j < offsets[i]; ++j) {
                rows[j] = parent.original_row(i);
                if (parent.null_map) {
                    null_map[j] = parent.null_map[i];
                }
            }
        }
    }

    ValidationRows view() const {
        return {.rows = &rows, .null_map = null_map.empty() ? nullptr : null_map.data()};
    }
};

Status validate_array(ColumnArray& column, const DataTypeArray& type,
                      TabletValidateContext& context, const ValidationRows& rows) {
    NestedRows nested(column.get_offsets(), rows);
    const auto prefix_size = context.error_prefix.size();
    Defer restore_prefix {[&] { context.error_prefix.resize(prefix_size); }};
    fmt::format_to(context.error_prefix, "ARRAY type failed: ");
    return validate_child(column.get_data_ptr(), type.get_nested_type(), context, nested.view());
}

Status validate_map(ColumnMap& column, const DataTypeMap& type, TabletValidateContext& context,
                    const ValidationRows& rows) {
    // The dispatcher visits every nested map itself. Recursive deduplication here
    // would revisit those maps and acquire mutable owners a second time.
    RETURN_IF_ERROR(column.deduplicate_keys(false));
    NestedRows nested(column.get_offsets(), rows);
    const auto prefix_size = context.error_prefix.size();
    Defer restore_prefix {[&] { context.error_prefix.resize(prefix_size); }};
    fmt::format_to(context.error_prefix, "MAP type failed: ");
    RETURN_IF_ERROR(
            validate_child(column.get_keys_ptr(), type.get_key_type(), context, nested.view()));
    return validate_child(column.get_values_ptr(), type.get_value_type(), context, nested.view());
}

Status validate_struct(ColumnStruct& column, const DataTypeStruct& type,
                       TabletValidateContext& context, const ValidationRows& rows) {
    DCHECK_EQ(type.get_elements().size(), column.tuple_size());
    const auto prefix_size = context.error_prefix.size();
    Defer restore_prefix {[&] { context.error_prefix.resize(prefix_size); }};
    fmt::format_to(context.error_prefix, "STRUCT type failed: ");
    for (size_t i = 0; i < column.tuple_size(); ++i) {
        RETURN_IF_ERROR(
                validate_child(column.get_column_ptr(i), type.get_element(i), context, rows));
    }
    return Status::OK();
}

Status validate_nullable(ColumnNullable& column, const DataTypePtr& type,
                         TabletValidateContext& context, const ValidationRows& rows) {
    const auto& null_map = column.get_null_map_data();
    // A physically nullable column can target NOT NULL (including BITMAP). NULLs
    // beneath a NULL ancestor do not represent values being written to that target.
    if (!type->is_nullable() || type->get_primitive_type() == TYPE_BITMAP) {
        for (size_t i = 0; i < column.size(); ++i) {
            if (null_map[i] && rows.should_validate(i, context)) {
                fmt::memory_buffer message;
                fmt::format_to(message, "null value for not null column, type={}",
                               type->get_name());
                RETURN_IF_ERROR(context.reject(rows.original_row(i), message));
            }
        }
    }
    NullMap combined;
    const UInt8* effective_null_map = null_map.data();
    if (rows.null_map) {
        combined.resize(column.size());
        for (size_t i = 0; i < column.size(); ++i) {
            combined[i] = null_map[i] | rows.null_map[i];
        }
        effective_null_map = combined.data();
    }
    return validate_child(column.get_nested_column_ptr_ref(), remove_nullable(type), context,
                          {.rows = rows.rows, .null_map = effective_null_map});
}

Status validate_column(MutableColumnPtr& column, const DataTypePtr& type,
                       TabletValidateContext& context, const ValidationRows& rows) {
    DCHECK(!rows.rows || rows.rows->size() == column->size());
    auto& writable = *column;
    if (auto* nullable = check_and_get_column<ColumnNullable>(writable)) {
        return validate_nullable(*nullable, type, context, rows);
    }
    // A non-nullable input can target Nullable(T); physical nullability is
    // handled above, while dispatch must always use the unwrapped target type.
    const auto nested_type = remove_nullable(type);
    switch (nested_type->get_primitive_type()) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return validate_string(column, nested_type, context, rows);
    case TYPE_JSONB:
        return validate_jsonb(assert_cast<const ColumnString&>(writable), context, rows);
    case TYPE_DECIMALV2:
        return validate_decimal_v2(assert_cast<ColumnDecimal128V2&>(writable), nested_type, context,
                                   rows);
    case TYPE_DECIMAL32:
        return validate_decimal_v3<Decimal32>(assert_cast<const ColumnDecimal32&>(writable),
                                              nested_type, context, rows);
    case TYPE_DECIMAL64:
        return validate_decimal_v3<Decimal64>(assert_cast<const ColumnDecimal64&>(writable),
                                              nested_type, context, rows);
    case TYPE_DECIMAL128I:
        return validate_decimal_v3<Decimal128V3>(assert_cast<const ColumnDecimal128V3&>(writable),
                                                 nested_type, context, rows);
    case TYPE_DECIMAL256:
        return validate_decimal_v3<Decimal256>(assert_cast<const ColumnDecimal256&>(writable),
                                               nested_type, context, rows);
    case TYPE_ARRAY:
        return validate_array(assert_cast<ColumnArray&>(writable),
                              assert_cast<const DataTypeArray&>(*nested_type), context, rows);
    case TYPE_MAP:
        return validate_map(assert_cast<ColumnMap&>(writable),
                            assert_cast<const DataTypeMap&>(*nested_type), context, rows);
    case TYPE_STRUCT:
        return validate_struct(assert_cast<ColumnStruct&>(writable),
                               assert_cast<const DataTypeStruct&>(*nested_type), context, rows);
    case TYPE_AGG_STATE:
        // Some aggregate states are serialized into a ColumnString.
        if (check_and_get_column<ColumnString>(writable)) {
            return validate_string(column, nested_type, context, rows);
        }
        return Status::OK();
    default:
        return Status::OK();
    }
}

} // namespace

Result<ColumnPtr> tablet_validate_column(ColumnPtr column, const DataTypePtr& type,
                                         TabletValidateContext& context) {
    MutableColumnPtr result;
    auto validate = [&]() -> Status {
        RETURN_IF_CATCH_EXCEPTION({
            // Only this boundary performs COW. Recursion borrows the resulting
            // writable slots, so a unique Struct and its unique fields stay unique.
            result = IColumn::mutate(std::move(column));
            return validate_column(result, type, context, {});
        });
    };
    RETURN_IF_ERROR_RESULT(validate());
    return ColumnPtr(std::move(result));
}

} // namespace doris
