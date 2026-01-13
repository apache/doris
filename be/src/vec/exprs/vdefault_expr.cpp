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

#include "vec/exprs/vdefault_expr.h"

#include <cctz/time_zone.h>
#include <fmt/format.h>

#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/binary_cast.hpp"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/runtime/timestamptz_value.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

Status VDefaultExpr::prepare(RuntimeState* state, const RowDescriptor& desc,
                             VExprContext* context) {
    RETURN_IF_ERROR(VExpr::prepare(state, desc, context));
    DCHECK_EQ(_children.size(), 1);

    _expr_name = fmt::format("default({})", _children[0]->expr_name());

    auto* slot_ref = dynamic_cast<VSlotRef*>(_children[0].get());
    if (slot_ref != nullptr && slot_ref->slot_id() != -1) {
        const auto* slot_desc = state->desc_tbl().get_slot_descriptor(slot_ref->slot_id());
        if (slot_desc) {
            _slot_id = slot_ref->slot_id();
            _is_nullable = slot_desc->is_nullable();
            _has_default_value = slot_desc->has_default_value();
            if (_has_default_value) {
                _default_value = slot_desc->col_default_value();
            }
        }
    }
    _prepare_finished = true;
    return Status::OK();
}

Status VDefaultExpr::open(RuntimeState* state, VExprContext* context,
                          FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    int64_t timestamp_ms = state->timestamp_ms();
    int32_t nano_seconds = state->nano_seconds();
    cctz::time_zone timezone_obj = state->timezone_obj();

    auto res_nested_type = remove_nullable(_data_type);
    PrimitiveType res_primitive_type = res_nested_type->get_primitive_type();

    // Agg state(HLL, BITMAPA, QUANTILE_STATE) error only
    if (is_var_len_object(res_primitive_type)) {
        return Status::InvalidArgument(
                "Agg type(HLL, BITMAP, QUANTILE_STATE) cannot be used for the DEFAULT "
                "function");
    }

    // 1. specified default value when creating table -> default_valueni
    // 2. no specified default value && column is NULLABLE -> NULL
    // 3. no specified default value && column is NOT NULL -> error
    if (_has_default_value) {
        if ((is_date_type(res_primitive_type) || res_primitive_type == TYPE_TIMESTAMPTZ) &&
            (_default_value.starts_with("CURRENT_TIMESTAMP") || _default_value == "CURRENT_DATE")) {
            int precision = -1;
            if (_default_value.size() > 17) {
                precision = std::atoi(_default_value.substr(18, 1).c_str());
            } else {
                precision = res_nested_type->get_scale();
            }

            switch (res_primitive_type) {
            case TYPE_DATEV2:
                RETURN_IF_ERROR(write_current_time_value<TYPE_DATEV2>(timestamp_ms, nano_seconds,
                                                                      timezone_obj, precision));
                break;
            case TYPE_DATETIMEV2:
                RETURN_IF_ERROR(write_current_time_value<TYPE_DATETIMEV2>(
                        timestamp_ms, nano_seconds, timezone_obj, precision));
                break;
            case TYPE_TIMESTAMPTZ:
                RETURN_IF_ERROR(write_current_time_value<TYPE_TIMESTAMPTZ>(
                        timestamp_ms, nano_seconds, timezone_obj, precision));
                break;
            default:
                return Status::FatalError(
                        "Unknown date type in DefaultExpr for CURRENT_DATE/TIMESTAMP");
            }
        } else {
            auto temp_column = res_nested_type->create_column();
            auto serde = res_nested_type->get_serde();
            StringRef default_str_ref(_default_value.data(), _default_value.size());
            DataTypeSerDe::FormatOptions options;
            Status parse_status = serde->from_string(default_str_ref, *temp_column, options);

            if (parse_status.ok() && temp_column->size() > 0) {
                Field default_field;
                temp_column->get(0, default_field);
                _cached_column = _data_type->create_column_const(1, default_field);
            } else [[unlikely]] {
                return Status::FatalError("Failed to parse default value for column '{}'",
                                          _children[0]->expr_name());
            }
        }
    } else if (_is_nullable) {
        _cached_column = _data_type->create_column_const(1, Field());
    } else {
        return Status::InvalidArgument("Column '{}' is NOT NULL but has no default value",
                                       _children[0]->expr_name());
    }

    _open_finished = true;
    return Status::OK();
}

const std::string& VDefaultExpr::expr_name() const {
    return _expr_name;
}

std::string VDefaultExpr::debug_string() const {
    std::stringstream out;
    out << "DefaultExpr(slot_id=" << _slot_id << ", is_nullable=" << _is_nullable
        << ", has_default=" << _has_default_value << ")";
    return out.str();
}

Status VDefaultExpr::execute_column(VExprContext* context, const Block* block, size_t count,
                                    ColumnPtr& result_column) const {
    if (_cached_column) {
        result_column = _cached_column->clone_resized(count);
        return Status::OK();
    } else [[unlikely]] {
        return Status::FatalError("Failed to parse default value of column: {}",
                                  _children[0]->expr_name());
    }
}

template <PrimitiveType PType>
Status VDefaultExpr::write_current_time_value(const int64_t timestamp_ms,
                                              const int32_t nano_seconds,
                                              const cctz::time_zone& timezone_obj,
                                              const int precision) {
    using InputValueType = typename PrimitiveTypeTraits<PType>::CppType;
    using InputNativeType = typename PrimitiveTypeTraits<PType>::CppNativeType;
    using ResColType = typename PrimitiveTypeTraits<PType>::ColumnType;

    auto res_col = ResColType::create();
    InputValueType val;

    if constexpr (PType == TYPE_DATEV2) {
        val.from_unixtime(timestamp_ms / 1000, timezone_obj);
    } else {
        val.from_unixtime(timestamp_ms / 1000, nano_seconds, timezone_obj, precision);
    }

    res_col->insert_value(binary_cast<InputValueType, InputNativeType>(val));
    auto null_map = ColumnUInt8::create(1, 0);
    _cached_column =
            ColumnConst::create(ColumnNullable::create(std::move(res_col), std::move(null_map)), 1);
    return Status::OK();
}

} // namespace doris::vectorized
